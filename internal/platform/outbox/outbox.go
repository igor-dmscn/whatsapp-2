// Package outbox stores events in Postgres for a relay to publish to Kafka.
//
// This is the join between the two halves of ADR-0003: Postgres is the system of
// record, Kafka carries the consequences. Publishing to Kafka directly from a use
// case cannot be made correct — the state change and the publish are two commits
// against two systems, and whichever order they are attempted in, a crash between
// them either loses the event or announces something that did not happen. Neither
// is acceptable when the event is what drives unread counts, receipts and
// notifications.
//
// So the event is written to a table in the same transaction as the state change,
// making it atomic by construction, and a relay publishes it afterwards. The
// guarantee that buys is at-least-once with per-key ordering, never at-most-once.
//
// The mechanism is here, in platform, because it is the same for every context. The
// decision of which topic an event belongs on and what its key is stays inside the
// context that raised it: that is a modelling question, not a transport one.
package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"comms/internal/platform/database"
)

// Record is one event awaiting publication.
type Record struct {
	// Topic is the Kafka topic to publish on.
	Topic string
	// Key decides the partition, and therefore what is ordered relative to what.
	// Events sharing a key are delivered in the order they were written; events
	// with different keys have no order at all. Choosing the key is choosing the
	// unit of ordering — for messaging that is the conversation.
	Key string
	// Name is the event's wire name, stable across Go refactors because consumers
	// and rows already written depend on it.
	Name string
	// Payload is the event, encoded. Bytes rather than any, so that encoding
	// failures happen in the context that owns the type rather than here.
	Payload []byte
	// OccurredAt is when the fact happened, which is not when it was published.
	OccurredAt time.Time
}

// Writer appends records inside the caller's transaction.
type Writer struct {
	db database.Conn
}

// NewWriter returns a writer over db.
func NewWriter(db *sql.DB) *Writer {
	return &Writer{db: database.NewConn(db)}
}

// Write appends records to the outbox.
//
// It refuses to run outside a transaction. Writing an event row on its own
// connection compiles, runs, and produces exactly the split-brain this package
// exists to prevent — so the mistake is made impossible rather than left to review.
func (w *Writer) Write(ctx context.Context, records []Record) error {
	if len(records) == 0 {
		return nil
	}
	if err := database.RequireTransaction(ctx); err != nil {
		return fmt.Errorf("outbox write: %w", err)
	}

	for _, record := range records {
		if _, err := w.db.ExecContext(ctx,
			`INSERT INTO outbox (topic, key, event_name, payload, occurred_at)
			 VALUES ($1, $2, $3, $4, $5)`,
			record.Topic, record.Key, record.Name, record.Payload, record.OccurredAt,
		); err != nil {
			return fmt.Errorf("insert outbox record %s: %w", record.Name, err)
		}
	}
	return nil
}

// Envelope is the wire format of every published event.
//
// The name and the time are carried in the payload as well as in the row and the
// Kafka header, because a consumer reading from Kafka has neither: the row is not
// there and a header is easy to lose through a proxy or a mirroring tool. Making the
// message self-describing costs a few bytes and removes a class of consumer that
// works until something in the middle drops a header.
type Envelope struct {
	Name       string          `json:"name"`
	OccurredAt time.Time       `json:"occurred_at"`
	Data       json.RawMessage `json:"data"`
}

// Encode wraps an event in an envelope and returns the Record to store.
//
// A helper rather than a method so contexts can call it with their own types without
// exposing them here.
func Encode(topic, key, name string, occurredAt time.Time, event any) (Record, error) {
	data, err := json.Marshal(event)
	if err != nil {
		return Record{}, fmt.Errorf("encode event %s: %w", name, err)
	}

	payload, err := json.Marshal(Envelope{Name: name, OccurredAt: occurredAt, Data: data})
	if err != nil {
		return Record{}, fmt.Errorf("encode envelope for %s: %w", name, err)
	}

	return Record{
		Topic:      topic,
		Key:        key,
		Name:       name,
		Payload:    payload,
		OccurredAt: occurredAt,
	}, nil
}

// Open decodes an envelope and the event inside it.
//
// Generic so a consumer names the type it expects and gets a decode error rather
// than a zero value when the wire format and the expectation disagree.
func Open[T any](payload []byte) (T, time.Time, error) {
	var (
		envelope Envelope
		event    T
	)
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return event, time.Time{}, fmt.Errorf("decode envelope: %w", err)
	}
	if err := json.Unmarshal(envelope.Data, &event); err != nil {
		return event, envelope.OccurredAt, fmt.Errorf("decode %s: %w", envelope.Name, err)
	}
	return event, envelope.OccurredAt, nil
}
