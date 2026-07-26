// Package projection builds Messaging's read models from published events.
//
// This is the consuming half of ADR-0002. A send writes one row to the log; this
// catches up afterwards with the per-member state that ADR-0002 refused to write
// synchronously — unread counts, read and delivery marks.
//
// Everything here must be idempotent (NF-8). The relay is at-least-once, a consumer
// group redelivers whatever was not committed, and the whole topic can be replayed
// from the beginning. None of those may move a badge twice. The guarantee is not
// achieved by remembering which events were seen; it is achieved by every write being
// one that can be applied any number of times — GREATEST for a high-water mark, a
// guard on the projected sequence for a count, a recount from the log for the rest.
package projection

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"comms/internal/messaging/internal/domain"
	"comms/internal/platform/kafka"
	"comms/internal/platform/logging"
	"comms/internal/platform/outbox"
)

// Wire structs, declared here rather than decoding into the domain's event types.
//
// Two reasons. A consumer is a separate deployment from the producer and may be
// older, so it must decode what it understands and ignore the rest — which a shared
// type makes impossible to express. And the domain's events embed an unexported type
// for their timestamp, which the envelope carries anyway; decoding into them would
// tie the wire format to the domain's Go layout, exactly what ADR-0010 means by event
// names being wire names.
type (
	entryAppended struct {
		ConversationID string `json:"ConversationID"`
		Sequence       int64  `json:"Sequence"`
		AuthorID       string `json:"AuthorID"`
	}

	memberJoined struct {
		ConversationID string `json:"ConversationID"`
		AccountID      string `json:"AccountID"`
	}

	cursorAdvanced struct {
		ConversationID string `json:"ConversationID"`
		AccountID      string `json:"AccountID"`
		Through        int64  `json:"Through"`
	}
)

// Projector applies events to the read models.
type Projector struct {
	state  domain.MemberStateStore
	logger *slog.Logger
}

// NewProjector returns a projector writing to state.
func NewProjector(state domain.MemberStateStore, logger *slog.Logger) *Projector {
	return &Projector{state: state, logger: logger}
}

// Topics is what a projector needs to consume.
func Topics() []string {
	return []string{kafka.TopicMessagingEntries, kafka.TopicMessagingReceipts}
}

// errUnprocessable marks a record that will never succeed.
//
// The distinction this draws is the one that keeps a projection alive. A failure
// because Postgres is unreachable is transient: returning it leaves the offset
// uncommitted and the record is retried, which is correct. A failure because the
// record itself is malformed is permanent, and retrying it forever blocks every
// record behind it on that partition — one bad message stopping every unread count
// in the system, indefinitely.
//
// So a record that cannot possibly be applied is logged loudly and skipped. Found by
// running the worker against a topic that had a test's incomplete record on it, and
// watching the projection wedge on offset 0 rather than fall behind and recover.
//
// ponytail: skipped records are logged and gone. A dead-letter topic is the phase-10
// version, when there is somewhere for an operator to look.
var errUnprocessable = errors.New("record cannot be applied")

// Apply handles one record.
//
// An unrecognised event name is not an error. A consumer that failed on events it
// does not know would stop the whole partition the first time a newer producer
// published something new — turning a forward-compatible change into an outage.
func (p *Projector) Apply(ctx context.Context, record kafka.Record) error {
	name := record.Name
	if name == "" {
		// The header was lost somewhere in the middle. The envelope carries the name
		// too, which is why it does.
		var envelope outbox.Envelope
		if err := json.Unmarshal(record.Value, &envelope); err != nil {
			return fmt.Errorf("decode envelope: %w", err)
		}
		name = envelope.Name
	}

	var err error
	switch name {
	case "messaging.entry_appended":
		err = p.entryAppended(ctx, record.Value)
	case "messaging.member_joined":
		err = p.memberJoined(ctx, record.Value)
	case "messaging.cursor_advanced":
		err = p.cursorAdvanced(ctx, record.Value)
	case "messaging.entries_delivered":
		err = p.entriesDelivered(ctx, record.Value)
	default:
		logging.With(ctx, p.logger).Debug("ignoring event", slog.String("event", name))
		return nil
	}

	if errors.Is(err, errUnprocessable) {
		// Swallowed so the offset commits and the partition keeps moving. Logged at
		// error because a skipped event means a projection is now permanently a
		// little wrong, and that must be visible.
		logging.With(ctx, p.logger).Error("skipping unprocessable record",
			slog.String("event", name),
			slog.String("topic", record.Topic),
			slog.Int64("offset", record.Offset),
			slog.Any("error", err),
		)
		return nil
	}
	return err
}

// identified checks the fields every projection needs before it can do anything.
//
// A record missing them is not a database problem and will not be fixed by trying
// again, so it is reported as unprocessable rather than retried.
func identified(conversationID, accountID string) error {
	if conversationID == "" {
		return fmt.Errorf("%w: no conversation id", errUnprocessable)
	}
	if accountID == "" {
		return fmt.Errorf("%w: no account id", errUnprocessable)
	}
	return nil
}

func (p *Projector) entryAppended(ctx context.Context, payload []byte) error {
	event, occurredAt, err := outbox.Open[entryAppended](payload)
	if err != nil {
		return fmt.Errorf("%w: %w", errUnprocessable, err)
	}

	if err := identified(event.ConversationID, event.AuthorID); err != nil {
		return err
	}
	if event.Sequence < 1 {
		return fmt.Errorf("%w: sequence %d is not a position in the log", errUnprocessable, event.Sequence)
	}

	conversationID := domain.ConversationID(event.ConversationID)
	sequence := domain.Sequence(event.Sequence)
	author := domain.AccountID(event.AuthorID)

	// The author first. If the count below were applied and this were not — the
	// process dying between them — the entry would be redelivered and the count is
	// guarded, so it would be skipped, leaving the author's own marks permanently
	// behind. Doing the unguarded write first means the redelivery repairs it.
	if err := p.state.TouchAuthor(ctx, conversationID, sequence, author, occurredAt); err != nil {
		return err
	}
	return p.state.CountEntry(ctx, conversationID, sequence, author, occurredAt)
}

func (p *Projector) memberJoined(ctx context.Context, payload []byte) error {
	event, occurredAt, err := outbox.Open[memberJoined](payload)
	if err != nil {
		return fmt.Errorf("%w: %w", errUnprocessable, err)
	}
	if err := identified(event.ConversationID, event.AccountID); err != nil {
		return err
	}
	return p.state.EnsureMember(ctx,
		domain.ConversationID(event.ConversationID),
		domain.AccountID(event.AccountID),
		occurredAt,
	)
}

func (p *Projector) cursorAdvanced(ctx context.Context, payload []byte) error {
	event, occurredAt, err := outbox.Open[cursorAdvanced](payload)
	if err != nil {
		return fmt.Errorf("%w: %w", errUnprocessable, err)
	}
	if err := identified(event.ConversationID, event.AccountID); err != nil {
		return err
	}
	return p.state.MarkRead(ctx,
		domain.ConversationID(event.ConversationID),
		domain.AccountID(event.AccountID),
		domain.Sequence(event.Through),
		occurredAt,
	)
}

func (p *Projector) entriesDelivered(ctx context.Context, payload []byte) error {
	// Same shape as a cursor advance; the difference is which mark moves.
	event, occurredAt, err := outbox.Open[cursorAdvanced](payload)
	if err != nil {
		return fmt.Errorf("%w: %w", errUnprocessable, err)
	}
	if err := identified(event.ConversationID, event.AccountID); err != nil {
		return err
	}
	return p.state.MarkDelivered(ctx,
		domain.ConversationID(event.ConversationID),
		domain.AccountID(event.AccountID),
		domain.Sequence(event.Through),
		occurredAt,
	)
}

// DeliveryOf reports the state of an entry the caller wrote, per MS-13.
//
// Derived from the other members' marks rather than stored per entry per recipient:
// two integers per membership answer the question for every entry in the conversation,
// where a row per entry per recipient would be the fan-out-on-write that ADR-0002
// rejected, wearing a different hat.
func DeliveryOf(sequence, othersRead, othersDelivered domain.Sequence) domain.DeliveryState {
	switch {
	case sequence <= othersRead:
		return domain.DeliveryRead
	case sequence <= othersDelivered:
		return domain.DeliveryDelivered
	default:
		return domain.DeliverySent
	}
}
