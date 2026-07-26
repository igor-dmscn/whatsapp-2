package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"comms/internal/calling/internal/domain"
	"comms/internal/platform/kafka"
	"comms/internal/platform/outbox"
)

// OutboxPublisher writes recorded events to the outbox, in the caller's transaction.
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
			return err //nolint:wrapcheck // already named where it happened.
		}
		records = append(records, record)
	}

	if err := p.writer.Write(ctx, records); err != nil {
		return fmt.Errorf("publish calling events: %w", err)
	}
	return nil
}

// route decides which topic an event belongs on and what orders it.
//
// Keyed by call, because the ordering that matters is within one call: a departure must
// not be projected before the arrival it follows. Keying by conversation would order
// unrelated calls against each other and, for a conversation with a call every day, put a
// year of them on one partition.
func route(event domain.Event) (topic string, key string, err error) {
	switch typed := event.(type) {
	case domain.CallStarted:
		return kafka.TopicCallingEvents, string(typed.CallID), nil
	case domain.ParticipantJoined:
		return kafka.TopicCallingEvents, string(typed.CallID), nil
	case domain.ParticipantLeft:
		return kafka.TopicCallingEvents, string(typed.CallID), nil
	case domain.CallEnded:
		return kafka.TopicCallingEvents, string(typed.CallID), nil
	default:
		// An unmapped event would land on an arbitrary partition and lose its order
		// against the rest of its call. Failing the write that produced it is what caught
		// two of these in phase 5.
		return "", "", fmt.Errorf("no route for event %s", event.EventName())
	}
}
