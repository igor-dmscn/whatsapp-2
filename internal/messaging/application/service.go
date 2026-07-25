// Package application orchestrates messaging use cases.
//
// It owns the clock, identifier generation, transaction boundaries, and publishing
// recorded events. It owns no rules: what a membership may do, where a new
// member's visibility starts, and which position an entry gets are all decided by
// aggregates.
package application

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"comms/internal/messaging/domain"
	"comms/internal/platform/logging"
)

// Clock is injected so ordering can be tested without sleeping.
type Clock func() time.Time

// Service carries out messaging use cases.
type Service struct {
	conversations domain.ConversationRepository
	memberships   domain.MembershipRepository
	entries       domain.EntryRepository
	broadcaster   domain.Broadcaster
	events        domain.EventPublisher
	ids           domain.IDs
	now           Clock
	logger        *slog.Logger
}

// NewService wires a Service.
func NewService(
	conversations domain.ConversationRepository,
	memberships domain.MembershipRepository,
	entries domain.EntryRepository,
	broadcaster domain.Broadcaster,
	events domain.EventPublisher,
	ids domain.IDs,
	now Clock,
	logger *slog.Logger,
) *Service {
	if now == nil {
		now = time.Now
	}
	return &Service{conversations, memberships, entries, broadcaster, events, ids, now, logger}
}

// maxRangeLimit caps how many entries one fetch returns.
//
// A client syncing a long absence pages through rather than asking for everything;
// a single unbounded response is how a large gap becomes a memory incident on both
// ends.
const maxRangeLimit = 500

// StartDirect returns the direct conversation between two accounts, creating it if
// they have none.
//
// Idempotent on the pair, so a client that starts a conversation twice — or two
// clients that start it simultaneously — end up in the same one. Without that,
// each would hold half the history and neither would know.
func (s *Service) StartDirect(ctx context.Context, initiator, other domain.AccountID) (*domain.Conversation, error) {
	if initiator == other {
		return nil, domain.ErrCannotMessageSelf
	}

	existing, err := s.conversations.DirectBetween(ctx, initiator, other)
	if err == nil {
		return existing, nil
	}
	if !errors.Is(err, domain.ErrConversationNotFound) {
		return nil, fmt.Errorf("look up direct conversation: %w", err)
	}

	now := s.now()
	conversation, err := domain.StartDirect(s.ids.NewConversationID(), now)
	if err != nil {
		return nil, err
	}

	// Both memberships see the whole log: a direct conversation has no history
	// that predates either participant.
	memberships := make([]*domain.Membership, 0, domain.DirectMemberCount)
	for _, accountID := range []domain.AccountID{initiator, other} {
		membership, err := domain.Join(conversation.ID(), accountID, domain.RoleMember, domain.FirstSequence, now)
		if err != nil {
			return nil, err
		}
		memberships = append(memberships, membership)
	}

	if err := s.conversations.Start(ctx, conversation, memberships); err != nil {
		// Lost a race to create the same pair's conversation. The other writer's
		// conversation is just as good, so use it rather than failing.
		if errors.Is(err, domain.ErrAlreadyAMember) {
			existing, lookupErr := s.conversations.DirectBetween(ctx, initiator, other)
			if lookupErr != nil {
				return nil, fmt.Errorf("look up direct conversation after conflict: %w", lookupErr)
			}
			return existing, nil
		}
		return nil, fmt.Errorf("start conversation: %w", err)
	}

	s.publish(ctx, conversation.TakeEvents())
	for _, membership := range memberships {
		s.publish(ctx, membership.TakeEvents())
	}

	// Tell the other account's node to start listening, or nothing this
	// conversation produces will arrive live until they reconnect.
	if err := s.broadcaster.NotifyConversationStarted(ctx, other, conversation.ID()); err != nil {
		logging.With(ctx, s.logger).Warn("notify conversation started",
			slog.String("conversation_id", string(conversation.ID())),
			slog.Any("error", err),
		)
	}

	return conversation, nil
}

// Send appends an entry to a conversation.
//
// Idempotent on the client-supplied identifier: a retry returns the entry that
// already exists, with its original position, rather than creating a second one
// (MS-2).
func (s *Service) Send(
	ctx context.Context,
	conversationID domain.ConversationID,
	author domain.AccountID,
	clientEntryID string,
	contentType string,
	body []byte,
) (*domain.Entry, error) {
	parsedClientID, err := domain.ParseClientEntryID(clientEntryID)
	if err != nil {
		return nil, err
	}
	payload, err := domain.NewPayload(contentType, body)
	if err != nil {
		return nil, err
	}

	membership, err := s.memberships.Of(ctx, conversationID, author)
	if err != nil {
		if errors.Is(err, domain.ErrNotAMember) {
			// Reported as absence rather than forbidden, so that a caller cannot
			// use send failures to discover which conversations exist.
			return nil, domain.ErrNotAMember
		}
		return nil, fmt.Errorf("look up membership: %w", err)
	}

	// Checked before the transaction so the common retry costs one indexed read
	// rather than a write that fails on a unique index.
	if existing, err := s.entries.ByClientEntryID(ctx, conversationID, author, parsedClientID); err == nil {
		return existing, nil
	} else if !errors.Is(err, domain.ErrEntryNotFound) {
		return nil, fmt.Errorf("look up existing entry: %w", err)
	}

	entry, events, err := s.conversations.AppendEntry(ctx, conversationID,
		func(conversation *domain.Conversation) (*domain.Entry, error) {
			return conversation.Append(s.ids.NewEntryID(), membership, parsedClientID, payload, s.now())
		},
	)
	if err != nil {
		if errors.Is(err, domain.ErrEntryAlreadySent) {
			// An identical send won the race. Returning it makes the retry a lookup
			// rather than a failure, which is the whole point of MS-2.
			existing, lookupErr := s.entries.ByClientEntryID(ctx, conversationID, author, parsedClientID)
			if lookupErr != nil {
				return nil, fmt.Errorf("look up entry after conflict: %w", lookupErr)
			}
			return existing, nil
		}
		return nil, fmt.Errorf("append entry: %w", err)
	}
	s.publish(ctx, events)

	// The ephemeral path, and it is allowed to fail: the entry is committed, and a
	// listener that misses this will notice the sequence gap and refetch. Failing
	// the send here would turn a delivery hiccup into lost work.
	if err := s.broadcaster.BroadcastEntry(ctx, entry); err != nil {
		logging.With(ctx, s.logger).Warn("broadcast entry",
			slog.String("conversation_id", string(conversationID)),
			slog.Int64("sequence", int64(entry.Sequence())),
			slog.Any("error", err),
		)
	}

	return entry, nil
}

// Fetch returns a range of entries a caller is entitled to see.
//
// Visibility is applied from the caller's membership rather than trusted from the
// request, so asking for entry 1 in a group joined at 40 returns nothing rather
// than history that predates the caller.
func (s *Service) Fetch(
	ctx context.Context,
	conversationID domain.ConversationID,
	reader domain.AccountID,
	after domain.Sequence,
	limit int,
) ([]*domain.Entry, error) {
	membership, err := s.memberships.Of(ctx, conversationID, reader)
	if err != nil {
		return nil, err
	}
	conversation, err := s.conversations.ByID(ctx, conversationID)
	if err != nil {
		return nil, fmt.Errorf("load conversation: %w", err)
	}

	gap, hasGap := domain.GapFor(membership, conversation.Head(), after)
	if !hasGap {
		return nil, nil
	}

	if limit <= 0 || limit > maxRangeLimit {
		limit = maxRangeLimit
	}

	entries, err := s.entries.Range(ctx, conversationID, gap.From, gap.To, limit)
	if err != nil {
		return nil, fmt.Errorf("read entries: %w", err)
	}
	return entries, nil
}

// Resume reports what a client is missing, given what it already holds.
//
// The heart of the sync protocol (MS-3). Conversations the client is current on
// produce no gap and are omitted, so a client that missed nothing is told nothing
// — and a client added to a conversation while offline learns about it here,
// without a separate mechanism.
func (s *Service) Resume(
	ctx context.Context,
	reader domain.AccountID,
	clientHas map[domain.ConversationID]domain.Sequence,
) ([]domain.Gap, error) {
	memberships, err := s.memberships.ForAccount(ctx, reader)
	if err != nil {
		return nil, fmt.Errorf("list memberships: %w", err)
	}

	gaps := make([]domain.Gap, 0, len(memberships))
	for _, membership := range memberships {
		if !membership.Active() {
			continue
		}

		conversation, err := s.conversations.ByID(ctx, membership.ConversationID())
		if err != nil {
			return nil, fmt.Errorf("load conversation: %w", err)
		}

		// A conversation absent from the client's map yields zero, which means
		// "nothing yet" — so a new membership is reported as a gap from its
		// visibility start without needing a separate case.
		if gap, hasGap := domain.GapFor(membership, conversation.Head(), clientHas[membership.ConversationID()]); hasGap {
			gaps = append(gaps, gap)
		}
	}
	return gaps, nil
}

// Conversations lists the conversations an account belongs to.
func (s *Service) Conversations(ctx context.Context, accountID domain.AccountID) ([]*domain.Membership, error) {
	memberships, err := s.memberships.ForAccount(ctx, accountID)
	if err != nil {
		return nil, fmt.Errorf("list memberships: %w", err)
	}
	return memberships, nil
}

// Conversation returns a conversation a caller belongs to.
func (s *Service) Conversation(ctx context.Context, conversationID domain.ConversationID, reader domain.AccountID) (*domain.Conversation, error) {
	if _, err := s.memberships.Of(ctx, conversationID, reader); err != nil {
		return nil, err
	}
	conversation, err := s.conversations.ByID(ctx, conversationID)
	if err != nil {
		return nil, fmt.Errorf("load conversation: %w", err)
	}
	return conversation, nil
}

// Members returns a conversation's memberships, for a caller who belongs to it.
func (s *Service) Members(ctx context.Context, conversationID domain.ConversationID, reader domain.AccountID) ([]*domain.Membership, error) {
	if _, err := s.memberships.Of(ctx, conversationID, reader); err != nil {
		return nil, err
	}
	members, err := s.memberships.In(ctx, conversationID)
	if err != nil {
		return nil, fmt.Errorf("list members: %w", err)
	}
	return members, nil
}

// publish hands recorded events to the publisher.
//
// A publish failure does not fail the use case: the state change is committed, and
// refusing an accepted message because an event could not be published would be
// worse than the missing event. Phase 3 removes the choice by writing events in
// the same transaction.
func (s *Service) publish(ctx context.Context, events []domain.Event) {
	if len(events) == 0 {
		return
	}
	if err := s.events.Publish(ctx, events); err != nil {
		logging.With(ctx, s.logger).Warn("publish events", slog.Any("error", err))
	}
}
