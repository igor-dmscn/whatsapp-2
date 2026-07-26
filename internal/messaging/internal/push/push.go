// Package push decides who should be woken up about an entry.
//
// A Kafka consumer rather than something the send path does, and that is the whole point: a
// person waiting for their message to be accepted must not also wait for a push provider on the
// other side of the internet. The entry commits, the request is answered, and this happens
// afterwards from the durable log — so a provider being slow or down delays a notification and
// costs nothing else (ADR-0003).
//
// What this package contains is the *decision*, which is where all the judgement is. Delivering
// to a provider is one port with one method, and the reason there is no APNs or FCM behind it is
// recorded in [ADR-0014](../../../../docs/adr/0014-push-decides-here-delivers-elsewhere.md):
// credentials for either cannot exist in a system that runs entirely on one machine, and a
// half-written integration nobody can run is worse than a seam somebody can fill.
//
// Three rules, and each one is somebody's complaint if it is missing:
//
//   - **Not the author.** Being notified about your own message is the most obvious possible bug.
//   - **Not somebody who is already looking.** Presence is why phase 10 built presence: a
//     notification on the phone in your hand while you read the message on it is worse than
//     silence, because it trains people to ignore notifications.
//   - **Not twice.** Kafka is at-least-once, so the same entry will be seen again. A duplicated
//     unread badge is invisible; a duplicated buzz is not.
package push

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"comms/internal/messaging/internal/domain"
	"comms/internal/platform/kafka"
	"comms/internal/platform/logging"
	"comms/internal/platform/outbox"
)

// Notification is what a provider would be asked to deliver.
//
// Deliberately without the message body. The server holds payloads it does not read (ADR-0001),
// and handing one to a third-party push provider would put message text on somebody else's
// infrastructure — which is a decision about privacy, not about notifications. A provider is told
// who and where, and the client fetches what.
type Notification struct {
	AccountID      string
	ConversationID string
	Sequence       int64
	// AuthorID is who wrote it, so a client can render "from X" without a fetch.
	AuthorID string
}

// Sender delivers a notification to whatever a deployment uses.
//
// One method, because the interesting part of push is deciding whether to send at all. A real
// implementation resolves the account's device tokens and calls a provider; the default one logs,
// which is what a system with no provider credentials can honestly do.
type Sender interface {
	Send(ctx context.Context, notification Notification) error
}

// Presence is the part of phase 10's presence store this needs.
//
// Declared here rather than imported as a concrete type, so that the rule "do not notify somebody
// who is already looking" is visible in this package's dependencies. A consumer that silently did
// not care about presence would be indistinguishable from one that forgot.
type Presence interface {
	OnlineAmong(ctx context.Context, accountIDs []string, now time.Time) (map[string]bool, error)
}

// Suppressor remembers which notifications have already gone out.
//
// In Redis with expiry, like everything else ephemeral here: the question is only ever "did this
// go out just now", and a permanent record of every notification ever sent would be a table
// nobody reads and a retention problem nobody wants.
type Suppressor interface {
	// FirstTime reports whether this is the first time a notification has been claimed, and
	// claims it. False means somebody — this process or another — already sent it.
	FirstTime(ctx context.Context, key string) (bool, error)
}

// Notifier is the consumer.
type Notifier struct {
	memberships domain.MembershipRepository
	presence    Presence
	suppressor  Suppressor
	sender      Sender
	deadLetters *kafka.DeadLetters
	logger      *slog.Logger
}

// NewNotifier wires the consumer.
func NewNotifier(
	memberships domain.MembershipRepository,
	presence Presence,
	suppressor Suppressor,
	sender Sender,
	deadLetters *kafka.DeadLetters,
	logger *slog.Logger,
) *Notifier {
	return &Notifier{
		memberships: memberships,
		presence:    presence,
		suppressor:  suppressor,
		sender:      sender,
		deadLetters: deadLetters,
		logger:      logger,
	}
}

// Topics is what this consumer reads.
func Topics() []string { return []string{"messaging.entries"} }

// errUnprocessable marks a record that will never succeed, so the offset commits and the
// partition keeps moving.
//
// The same distinction the projector draws, and for the same reason: retrying a malformed record
// forever blocks every record behind it on that partition. Here the consequence of skipping is
// milder — somebody misses one notification — which is exactly why it must not be allowed to
// stop the ones behind it.
var errUnprocessable = errors.New("record cannot be applied")

// Apply handles one record.
func (n *Notifier) Apply(ctx context.Context, record kafka.Record) error {
	name := record.Name
	if name == "" {
		var envelope outbox.Envelope
		if err := json.Unmarshal(record.Value, &envelope); err != nil {
			return fmt.Errorf("decode envelope: %w", err)
		}
		name = envelope.Name
	}

	// Only new entries. Revisions arrive as entries too and are deliberately included: an
	// edit to a message somebody has not read yet is the message they have not read.
	if name != "messaging.entry_appended" {
		return nil
	}

	err := n.notify(ctx, record.Value)
	if errors.Is(err, errUnprocessable) {
		n.deadLetters.Record(ctx, record, err)
		return nil
	}
	return err
}

// appended is the part of the event this needs.
type appended struct {
	Payload struct {
		ConversationID string `json:"conversation_id"`
		AuthorID       string `json:"author_id"`
		Sequence       int64  `json:"sequence"`
	} `json:"payload"`
}

func (n *Notifier) notify(ctx context.Context, value []byte) error {
	var event appended
	if err := json.Unmarshal(value, &event); err != nil {
		return fmt.Errorf("%w: %w", errUnprocessable, err)
	}

	conversationID := event.Payload.ConversationID
	if conversationID == "" || event.Payload.Sequence < 1 {
		return fmt.Errorf("%w: an entry event with no conversation or position", errUnprocessable)
	}

	members, err := n.memberships.In(ctx, domain.ConversationID(conversationID))
	if err != nil {
		// Transient: Postgres is unreachable or slow. Returned so the offset stays
		// uncommitted and this is tried again, which is what at-least-once is for.
		return fmt.Errorf("list members: %w", err)
	}

	candidates := make([]string, 0, len(members))
	for _, member := range members {
		accountID := string(member.AccountID())
		// Not the author, and not somebody who has left. A conversation somebody left is
		// not a conversation they should be woken up about.
		if !member.Active() || accountID == event.Payload.AuthorID {
			continue
		}
		candidates = append(candidates, accountID)
	}
	if len(candidates) == 0 {
		return nil
	}

	online, err := n.presence.OnlineAmong(ctx, candidates, time.Now())
	if err != nil {
		// Not fatal, and not a reason to retry. Presence is a cache: if it is unavailable
		// the honest fallback is to notify, because a notification somebody did not need is
		// a smaller failure than silence about a message they did.
		logging.With(ctx, n.logger).Warn("presence unavailable; notifying everybody",
			slog.String("conversation_id", conversationID), slog.Any("error", err))
		online = nil
	}

	for _, accountID := range candidates {
		if online[accountID] {
			// Already looking. This is the rule presence was built for.
			continue
		}

		// Claimed before sending rather than after. A crash between the two loses one
		// notification; the other order sends one twice on every redelivery, and a
		// duplicated buzz is the failure people actually notice.
		key := fmt.Sprintf("%s:%d:%s", conversationID, event.Payload.Sequence, accountID)
		first, err := n.suppressor.FirstTime(ctx, key)
		if err != nil {
			logging.With(ctx, n.logger).Warn("cannot check for a duplicate; sending anyway",
				slog.String("account_id", accountID), slog.Any("error", err))
		} else if !first {
			continue
		}

		if err := n.sender.Send(ctx, Notification{
			AccountID:      accountID,
			ConversationID: conversationID,
			Sequence:       event.Payload.Sequence,
			AuthorID:       event.Payload.AuthorID,
		}); err != nil {
			// One recipient's provider failing must not stop the others, and must not
			// re-deliver the whole record — which would notify everybody who already
			// succeeded a second time.
			logging.With(ctx, n.logger).Warn("send notification",
				slog.String("account_id", accountID), slog.Any("error", err))
		}
	}
	return nil
}

// LoggingSender is the default: it writes what would have been sent.
//
// Not a placeholder to be ashamed of. Without provider credentials — which cannot exist in a
// system that runs on one machine with no cloud dependencies (NF-15) — this is the honest
// implementation, and it makes the decision logic above testable by asserting on what it chose.
type LoggingSender struct {
	logger *slog.Logger
}

func NewLoggingSender(logger *slog.Logger) *LoggingSender {
	return &LoggingSender{logger: logger}
}

func (s *LoggingSender) Send(ctx context.Context, notification Notification) error {
	logging.With(ctx, s.logger).Info("push notification",
		slog.String("account_id", notification.AccountID),
		slog.String("conversation_id", notification.ConversationID),
		slog.Int64("sequence", notification.Sequence),
		slog.String("author_id", notification.AuthorID),
	)
	return nil
}
