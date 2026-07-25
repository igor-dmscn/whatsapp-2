package identityapp

import (
	"context"
	"log/slog"

	"comms/internal/identity"
	"comms/internal/platform/logging"
)

// LoggingPublisher writes domain events to the log.
//
// This is the phase 1 implementation and it is honest about being interim: events
// are published after the aggregate is saved, so a crash between the two loses
// them. Phase 3 replaces this with an outbox publisher that writes event rows in
// the same transaction as the aggregate, which is the only reliable arrangement
// (ADR-0003). The port does not change when it does.
//
// It is not a no-op standing in for the real thing. Aggregate state changes
// appearing in the log under the request's correlation identifier is how a
// "why did my device get revoked" question gets answered, and that is worth
// having on its own.
type LoggingPublisher struct {
	logger *slog.Logger
}

// NewLoggingPublisher returns a publisher writing to logger.
func NewLoggingPublisher(logger *slog.Logger) *LoggingPublisher {
	return &LoggingPublisher{logger: logger}
}

var _ identity.EventPublisher = (*LoggingPublisher)(nil)

// Publish logs each event. It never returns an error: there is nothing a caller
// could do about a failed log write, and failing a committed use case over one
// would be worse than the missing line.
func (p *LoggingPublisher) Publish(ctx context.Context, events []identity.Event) error {
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
