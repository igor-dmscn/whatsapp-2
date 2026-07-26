package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"comms/internal/identity/internal/domain"
	"comms/internal/platform/kafka"
	"comms/internal/platform/outbox"
)

// OutboxPublisher writes recorded events to the outbox, in the caller's transaction.
//
// This replaces the interim publisher that wrote them to the log. The difference
// that matters is not the destination: it is that the event row commits with the
// state change or not at all, so `identity.device_revoked` cannot be lost by a crash
// between saving the device and announcing it — and Messaging, which disconnects
// revoked devices, cannot miss one.
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
		// Keyed by account so that everything about one account is ordered, and
		// nothing about one account is ordered against another. A device revoked and
		// then re-registered must not be seen in the other order.
		key, err := partitionKey(event)
		if err != nil {
			return err
		}

		record, err := outbox.Encode(kafka.TopicIdentityEvents, key, event.EventName(), event.OccurredAt(), event)
		if err != nil {
			return err
		}
		records = append(records, record)
	}

	if err := p.writer.Write(ctx, records); err != nil {
		return fmt.Errorf("publish identity events: %w", err)
	}
	return nil
}

// partitionKey is the identifier an event is about, which is what orders it.
//
// Keyed by subject: the device where the event has one, the account otherwise. Not
// uniformly by account, because Session does not model an account — it belongs to a
// device, and the account is reachable only through it. Adding an account to the
// aggregate to satisfy a partitioning scheme would be letting the transport dictate
// the model.
//
// What that gives: everything about one device is ordered, so registered → revoked →
// a session started afterwards cannot be seen out of order. That is the ordering
// Messaging depends on when it disconnects a revoked device's sockets.
//
// What it does not give: account-level events are not ordered against device-level
// ones. Nothing needs that — an account exists before any of its devices, and that
// is guaranteed by the API accepting the calls in that order, not by Kafka.
//
// A type switch rather than a method on Event: the key is a transport decision, and
// making every aggregate answer "what is your partition key" would put Kafka's
// partitioning model into the domain.
func partitionKey(event domain.Event) (string, error) {
	switch typed := event.(type) {
	case domain.AccountRegistered:
		return string(typed.AccountID), nil
	case domain.CredentialAdded:
		return string(typed.AccountID), nil
	case domain.DeviceRegistered:
		return string(typed.DeviceID), nil
	case domain.DeviceRevoked:
		return string(typed.DeviceID), nil
	case domain.SessionStarted:
		return string(typed.DeviceID), nil
	case domain.SessionRotated:
		return string(typed.DeviceID), nil
	default:
		// A new event type nobody mapped would otherwise publish on an empty key,
		// landing on an arbitrary partition and silently losing its ordering against
		// everything else about the same subject.
		return "", fmt.Errorf("no partition key for event %s", event.EventName())
	}
}
