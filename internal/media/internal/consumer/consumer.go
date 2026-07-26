// Package consumer turns published upload events into derived variants.
//
// The consuming half of the attachment flow. Its correctness rests on two things it
// does not implement itself: the record is redelivered until this returns without
// error, and processing the same attachment twice produces the same variants (MD-2, MD-3).
package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"comms/internal/media/internal/app"
	"comms/internal/media/internal/domain"
	"comms/internal/platform/kafka"
	"comms/internal/platform/logging"
	"comms/internal/platform/outbox"
)

// uploadedEvent is the wire form this consumer reads.
//
// A separate struct from the domain event, for the reason the projections give: a
// consumer is a separate deployment and may be older than the producer, so it decodes
// what it understands rather than requiring the Go types to agree.
type uploadedEvent struct {
	AttachmentID string `json:"AttachmentID"`
}

// Processor handles attachment records.
type Processor struct {
	service *app.Service
	logger  *slog.Logger
}

// NewProcessor returns a processor over service.
func NewProcessor(service *app.Service, logger *slog.Logger) *Processor {
	return &Processor{service: service, logger: logger}
}

// Topics is what a processor needs to consume.
func Topics() []string { return []string{kafka.TopicMediaAttachments} }

// errUnprocessable marks a record that will never succeed, so it can be skipped
// rather than retried forever.
//
// The same distinction the projections draw, and it matters more here: deriving
// variants is expensive, so a record that cannot succeed would occupy a worker
// indefinitely as well as blocking its partition.
var errUnprocessable = errors.New("record cannot be applied")

// Apply handles one record.
func (p *Processor) Apply(ctx context.Context, record kafka.Record) error {
	name := record.Name
	if name == "" {
		var envelope outbox.Envelope
		if err := json.Unmarshal(record.Value, &envelope); err != nil {
			return fmt.Errorf("decode envelope: %w", err)
		}
		name = envelope.Name
	}

	if name != "media.attachment_uploaded" {
		// Not an error. A consumer that failed on events it does not know would stop
		// the partition the first time a newer producer published something new.
		logging.With(ctx, p.logger).Debug("ignoring event", slog.String("event", name))
		return nil
	}

	err := p.process(ctx, record.Value)
	if errors.Is(err, errUnprocessable) {
		logging.With(ctx, p.logger).Error("skipping unprocessable record",
			slog.String("event", name),
			slog.Int64("offset", record.Offset),
			slog.Any("error", err),
		)
		return nil
	}
	return err
}

func (p *Processor) process(ctx context.Context, value []byte) error {
	event, _, err := outbox.Open[uploadedEvent](value)
	if err != nil {
		return fmt.Errorf("%w: decode upload event: %v", errUnprocessable, err)
	}
	if event.AttachmentID == "" {
		return fmt.Errorf("%w: upload event names no attachment", errUnprocessable)
	}

	if err := p.service.Process(ctx, domain.AttachmentID(event.AttachmentID)); err != nil {
		if errors.Is(err, domain.ErrAttachmentNotFound) {
			// The row is gone: the attachment was deleted after its event was
			// published. Nothing to derive and nothing to retry.
			return fmt.Errorf("%w: attachment %s no longer exists",
				errUnprocessable, event.AttachmentID)
		}
		// Everything else — the store unreachable, Postgres unreachable — is transient
		// and returned, which leaves the offset uncommitted so the job is redelivered.
		// That is MD-3: a crash or an outage costs delay, not the job.
		return err
	}
	return nil
}
