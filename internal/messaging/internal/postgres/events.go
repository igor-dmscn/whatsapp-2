package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"comms/internal/messaging/internal/domain"
	"comms/internal/platform/kafka"
	"comms/internal/platform/outbox"
)

// OutboxPublisher writes recorded events to the outbox, in the caller's transaction.
//
// Replaces the interim publisher that wrote them to the log. What changes is not the
// destination but the atomicity: the event row commits with the entry or not at all,
// so an unread badge cannot be missing because the process died in the millisecond
// between the insert and the publish.
type OutboxPublisher struct {
	writer *outbox.Writer
}

// NewOutboxPublisher returns a publisher over db.
func NewOutboxPublisher(db *sql.DB) *OutboxPublisher {
	return &OutboxPublisher{writer: outbox.NewWriter(db)}
}

var _ domain.EventPublisher = (*OutboxPublisher)(nil)

// Publish encodes events and appends them to the outbox.
func (p *OutboxPublisher) Publish(ctx context.Context, events []domain.Event) error {
	records := make([]outbox.Record, 0, len(events))
	for _, event := range events {
		topic, key, err := route(event)
		if err != nil {
			return err
		}

		record, err := outbox.Encode(topic, key, event.EventName(), event.OccurredAt(), event)
		if err != nil {
			return err
		}
		records = append(records, record)
	}

	if err := p.writer.Write(ctx, records); err != nil {
		return fmt.Errorf("publish messaging events: %w", err)
	}
	return nil
}

// route decides which topic an event belongs on and what orders it.
//
// Every messaging event is keyed by conversation, which is the whole reason
// per-conversation ordering holds: entry 41 cannot be projected before entry 40,
// because they are on the same partition in the order they were written. Keying by
// account instead would order one person's messages across unrelated conversations
// and order nothing within a conversation, which is precisely backwards.
//
// Receipts go on their own topic. A member scrolling through a year of history
// produces a burst of cursor advances, and behind entries on one topic that burst
// would delay the projection of new messages — the badge that matters most being
// held up by the badges being cleared.
func route(event domain.Event) (topic string, key string, err error) {
	switch typed := event.(type) {
	case domain.ConversationStarted:
		return kafka.TopicMessagingEntries, string(typed.ConversationID), nil
	case domain.EntryAppended:
		return kafka.TopicMessagingEntries, string(typed.ConversationID), nil
	case domain.EntryAmended:
		return kafka.TopicMessagingEntries, string(typed.ConversationID), nil
	case domain.MemberJoined:
		return kafka.TopicMessagingEntries, string(typed.ConversationID), nil
	case domain.MemberLeft:
		return kafka.TopicMessagingEntries, string(typed.ConversationID), nil
	case domain.RoleChanged:
		return kafka.TopicMessagingEntries, string(typed.ConversationID), nil
	case domain.InviteCreated:
		return kafka.TopicMessagingEntries, string(typed.ConversationID), nil
	case domain.InviteRedeemed:
		return kafka.TopicMessagingEntries, string(typed.ConversationID), nil
	case domain.InviteRevoked:
		return kafka.TopicMessagingEntries, string(typed.ConversationID), nil
	case domain.CursorAdvanced:
		return kafka.TopicMessagingReceipts, string(typed.ConversationID), nil
	case domain.EntriesDelivered:
		return kafka.TopicMessagingReceipts, string(typed.ConversationID), nil
	default:
		// An unmapped event would be published on an empty key, landing on an
		// arbitrary partition and silently losing its order against everything else
		// in its conversation. Better to fail the write that produced it.
		return "", "", fmt.Errorf("no route for event %s", event.EventName())
	}
}
