package app

import (
	"context"
	"log/slog"

	"comms/internal/messaging/internal/domain"
	"comms/internal/platform/logging"
)

// LoggingPublisher writes domain events to the log.
//
// Interim, like Identity's: events are published after the aggregate is saved, so a
// crash between the two loses them. Phase 3 replaces this with an outbox writing
// event rows in the same transaction (ADR-0003). ConversationRepository.AppendEntry
// already accepts events for that reason — the transaction boundary the outbox needs
// exists before the outbox does.
type LoggingPublisher struct {
	logger *slog.Logger
}

func NewLoggingPublisher(logger *slog.Logger) *LoggingPublisher {
	return &LoggingPublisher{logger: logger}
}

var _ domain.EventPublisher = (*LoggingPublisher)(nil)

func (p *LoggingPublisher) Publish(ctx context.Context, events []domain.Event) error {
	logger := logging.With(ctx, p.logger)
	for _, event := range events {
		logger.Info("domain event",
			slog.String("event", event.EventName()),
			slog.Time("occurred_at", event.OccurredAt()),
			slog.Any("payload", event),
		)
	}
	return nil
}
