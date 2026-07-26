package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"comms/internal/media/internal/domain"
	"comms/internal/platform/kafka"
	"comms/internal/platform/outbox"
)

// OutboxPublisher writes recorded events to the outbox, in the caller's transaction.
//
// This is the whole of MD-3. A processing job that is a row in the same transaction as
// the upload it describes cannot be lost by a worker crash, cannot be published for an
// upload that was rolled back, and is redelivered until a consumer commits having
// handled it.
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
		return fmt.Errorf("publish media events: %w", err)
	}
	return nil
}

// route decides which topic an event belongs on and what orders it.
//
// Keyed by attachment, not by conversation. Two uploads in the same conversation have
// nothing to say to each other and processing them in parallel is the point; what must
// be ordered is the events about one attachment, so that a replay cannot process an
// upload before whatever superseded it.
func route(event domain.Event) (topic string, key string, err error) {
	switch typed := event.(type) {
	case domain.AttachmentUploaded:
		return kafka.TopicMediaAttachments, string(typed.AttachmentID), nil
	default:
		// Unmapped events would be published on an empty key onto an arbitrary
		// partition, losing their order. Failing the write that produced it is what
		// caught two of these during phase 5.
		return "", "", fmt.Errorf("no route for event %s", event.EventName())
	}
}
