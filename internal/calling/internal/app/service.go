// Package app orchestrates Calling's use cases: joining a conversation's call, answering
// the node's offers, and leaving.
//
// It owns the clock, identifiers, transaction boundaries and publication. It owns no
// rules: whether a call can be joined is the aggregate's, and whether this account may
// join is Messaging's, asked through a port (CL-1).
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"comms/internal/calling/internal/domain"
)

// Clock is injected so timing can be tested without sleeping.
type Clock func() time.Time

// Service carries out Calling's use cases.
type Service struct {
	calls         domain.CallRepository
	conversations domain.Conversations
	nodes         domain.MediaNodes
	notifier      domain.Notifier
	events        domain.EventPublisher
	transactor    domain.Transactor
	ids           domain.IDs
	now           Clock
	logger        *slog.Logger
}

// NewService wires a Service.
func NewService(
	calls domain.CallRepository,
	conversations domain.Conversations,
	nodes domain.MediaNodes,
	notifier domain.Notifier,
	events domain.EventPublisher,
	transactor domain.Transactor,
	ids domain.IDs,
	now Clock,
	logger *slog.Logger,
) *Service {
	if now == nil {
		now = time.Now
	}
	return &Service{calls, conversations, nodes, notifier, events, transactor, ids, now, logger}
}

// Joined is what a client needs to be in a call.
type Joined struct {
	Call *domain.Call
	// Answer is the node's SDP answer to the offer the client sent.
	Answer string
}

// Join puts a device into the conversation's call, creating it if there is none.
//
// One use case rather than a start and a join, and that is the design rather than a
// shortcut: a call exists *because* somebody joined it, so CL-2 falls out — a second
// person pressing call in the same conversation joins the first's call because that is the
// only thing this method can do. There is no operation that could create a second.
//
// The media exchange happens after the state is committed. A node that answers an offer
// for a call nobody recorded would forward media for a call that does not exist, and the
// participant would be invisible to everyone asking who is present.
func (s *Service) Join(
	ctx context.Context,
	conversationID domain.ConversationID,
	actor domain.AccountID,
	device domain.DeviceID,
	offer string,
) (Joined, error) {
	if offer == "" {
		return Joined{}, domain.ValidationError{Field: "sdp", Reason: "an offer is required to join"}
	}

	// CL-1: entitlement derives solely from membership of the conversation, and this is
	// the only place it is checked because it is the only way in.
	permitted, err := s.conversations.MayJoin(ctx, string(conversationID), string(actor))
	if err != nil {
		return Joined{}, fmt.Errorf("check permission to join: %w", err)
	}
	if !permitted {
		return Joined{}, domain.ErrNotPermitted
	}

	call, err := s.enter(ctx, conversationID, actor, device)
	if err != nil {
		return Joined{}, err
	}

	answer, err := s.nodes.Join(ctx, call.Node(), call.ID(), device, offer)
	if err != nil {
		// The participant is recorded and has no transport. Left rather than rolled back:
		// the client's remedy is to retry the join, which is idempotent on the device, and
		// a rollback would race with whatever the node did manage to set up.
		s.logger.Error("media node refused a join",
			slog.String("call", string(call.ID())), slog.Any("error", err))
		return Joined{}, fmt.Errorf("join media node: %w", err)
	}

	// Told after the fact and allowed to fail: a missed ring is recovered by the callee
	// asking whether there is a call when it next looks (ADR-0005).
	s.announce(ctx, call)

	return Joined{Call: call, Answer: answer}, nil
}

// enter finds or creates the conversation's call and records the presence.
//
// The retry is what makes CL-2 hold under a race. Two people pressing call at once both
// find no live call, both try to create one, and the database refuses the second — whose
// answer is to look again and join the one that won. Not a loop: exactly one retry, because
// after a conflict there is definitely a live call to find.
func (s *Service) enter(
	ctx context.Context,
	conversationID domain.ConversationID,
	actor domain.AccountID,
	device domain.DeviceID,
) (*domain.Call, error) {
	for attempt := range 2 {
		existing, err := s.calls.ActiveIn(ctx, conversationID)
		switch {
		case err == nil:
			return s.join(ctx, existing, actor, device)
		case !errors.Is(err, domain.ErrCallNotFound):
			return nil, fmt.Errorf("look up the conversation's call: %w", err)
		case attempt == 1:
			// The conflict said there was a live call and the lookup says there is not,
			// which means it ended in between. Refused rather than retried forever.
			return nil, domain.ErrCallEnded
		}

		call, err := s.start(ctx, conversationID, actor, device)
		if errors.Is(err, domain.ErrCallInProgress) {
			continue
		}
		if err != nil {
			return nil, err
		}
		return call, nil
	}
	return nil, domain.ErrCallInProgress
}

func (s *Service) start(
	ctx context.Context,
	conversationID domain.ConversationID,
	actor domain.AccountID,
	device domain.DeviceID,
) (*domain.Call, error) {
	node, err := s.nodes.Allocate(ctx)
	if err != nil {
		return nil, err //nolint:wrapcheck // ErrNoMediaNode is what the caller switches on.
	}

	call, err := domain.Start(s.ids.NewCallID(), conversationID, node, actor, device, s.now())
	if err != nil {
		return nil, err
	}

	if err := s.atomically(ctx, func(ctx context.Context) error {
		if err := s.calls.Save(ctx, call); err != nil {
			return err //nolint:wrapcheck // ErrCallInProgress is what the caller switches on.
		}
		return s.publish(ctx, call.TakeEvents())
	}); err != nil {
		return nil, err
	}
	return call, nil
}

func (s *Service) join(
	ctx context.Context,
	call *domain.Call,
	actor domain.AccountID,
	device domain.DeviceID,
) (*domain.Call, error) {
	if err := call.Join(actor, device, s.now()); err != nil {
		return nil, err
	}

	if err := s.atomically(ctx, func(ctx context.Context) error {
		if err := s.calls.Save(ctx, call); err != nil {
			return fmt.Errorf("save call: %w", err)
		}
		return s.publish(ctx, call.TakeEvents())
	}); err != nil {
		return nil, err
	}
	return call, nil
}

// Answer carries a client's answer to an offer the node sent.
//
// No authorisation, deliberately: the participant is identified by the device on their
// authenticated socket, and a device answering for a call it is not in is refused by the
// node, which has no transport under that name.
func (s *Service) Answer(
	ctx context.Context,
	callID domain.CallID,
	device domain.DeviceID,
	answer string,
) error {
	call, err := s.calls.Find(ctx, callID)
	if err != nil {
		return err //nolint:wrapcheck // ErrCallNotFound is what the caller switches on.
	}
	if !call.Includes(device) {
		return domain.ErrNotPermitted
	}

	if err := s.nodes.Answer(ctx, call.Node(), callID, device, answer); err != nil {
		return fmt.Errorf("answer to media node: %w", err)
	}
	return nil
}

// Leave takes a device out of a call, ending it if it was the last (CL-3).
func (s *Service) Leave(ctx context.Context, callID domain.CallID, device domain.DeviceID) error {
	call, err := s.calls.Find(ctx, callID)
	if err != nil {
		return err //nolint:wrapcheck // ErrCallNotFound is what the caller switches on.
	}

	// The node first. A participant whose state says they left while their transport is
	// still forwarding is the worse of the two orderings: everyone keeps receiving
	// somebody the interface says has gone.
	if err := s.nodes.Leave(ctx, call.Node(), callID, device); err != nil {
		s.logger.Warn("media node refused a leave",
			slog.String("call", string(callID)), slog.Any("error", err))
	}

	if err := call.Leave(device, s.now()); err != nil {
		return err
	}

	if err := s.atomically(ctx, func(ctx context.Context) error {
		if err := s.calls.Save(ctx, call); err != nil {
			return fmt.Errorf("save call: %w", err)
		}
		return s.publish(ctx, call.TakeEvents())
	}); err != nil {
		return err
	}

	s.announce(ctx, call)
	return nil
}

// Active returns the conversation's live call, or ErrCallNotFound.
//
// What a client asks on connecting, and what recovers a missed ring: the notification is
// ephemeral, and this is the durable answer to the same question.
func (s *Service) Active(
	ctx context.Context,
	conversationID domain.ConversationID,
	actor domain.AccountID,
) (*domain.Call, error) {
	permitted, err := s.conversations.MayJoin(ctx, string(conversationID), string(actor))
	if err != nil {
		return nil, fmt.Errorf("check permission: %w", err)
	}
	if !permitted {
		// Absence rather than refusal, so that asking about calls cannot be used to
		// discover which conversations exist.
		return nil, domain.ErrCallNotFound
	}

	call, err := s.calls.ActiveIn(ctx, conversationID)
	if err != nil {
		return nil, err //nolint:wrapcheck // ErrCallNotFound is what the caller switches on.
	}
	return call, nil
}

// LeaveAll takes a device out of every call it is in.
//
// Called when a socket closes. A participant whose client vanished is gone whether or not it
// said so, and without this a crashed tab keeps a call alive forever — CL-3 would never
// fire, and everyone else would keep a tile for somebody who is not there.
//
// Errors are logged rather than returned: nobody is waiting, the socket has already gone,
// and a failure here must not stop the other calls being cleaned up.
func (s *Service) LeaveAll(ctx context.Context, _ domain.AccountID, device domain.DeviceID) {
	calls, err := s.calls.LiveFor(ctx, device)
	if err != nil {
		s.logger.Warn("look up calls to clean up after",
			slog.String("device", string(device)), slog.Any("error", err))
		return
	}

	for _, call := range calls {
		if err := s.Leave(ctx, call.ID(), device); err != nil {
			s.logger.Warn("clean up after a closed socket",
				slog.String("call", string(call.ID())), slog.Any("error", err))
		}
	}
}

// announce tells everyone in the conversation to look at the call again.
func (s *Service) announce(ctx context.Context, call *domain.Call) {
	if err := s.notifier.CallChanged(
		ctx, string(call.ConversationID()), string(call.ID())); err != nil {
		s.logger.Warn("announce call", slog.Any("error", err))
	}
}

func (s *Service) atomically(ctx context.Context, work func(context.Context) error) error {
	return s.transactor.InTransaction(ctx, work) //nolint:wrapcheck // the closure's error is the caller's own.
}

func (s *Service) publish(ctx context.Context, events []domain.Event) error {
	if len(events) == 0 {
		return nil
	}
	if err := s.events.Publish(ctx, events); err != nil {
		return fmt.Errorf("publish events: %w", err)
	}
	return nil
}
