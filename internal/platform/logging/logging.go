// Package logging sets up structured logging and carries correlation
// identifiers through context.
//
// Correlation matters here more than in most systems: a single send fans out
// across an HTTP request, a Redis publish, an outbox row, a Kafka event and a
// worker projection. Without one identifier threaded through all of them, those
// are five unrelated log lines in five different places.
package logging

import (
	"context"
	"log/slog"
	"os"
)

// correlationKey is unexported so nothing outside this package can put a value
// under it, which keeps Correlate the only way in.
type correlationKey struct{}

// CorrelationField is the log attribute key. Exported so log queries and
// dashboards have one name to agree on.
const CorrelationField = "correlation_id"

// New returns a logger writing JSON to stderr. Level comes from LOG_LEVEL
// (debug, info, warn, error), defaulting to info.
func New(service string) *slog.Logger {
	var level slog.Level
	if err := level.UnmarshalText([]byte(os.Getenv("LOG_LEVEL"))); err != nil {
		level = slog.LevelInfo
	}

	handler := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level})
	return slog.New(handler).With(slog.String("service", service))
}

// Correlate returns a context carrying id.
func Correlate(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationKey{}, id)
}

// CorrelationID returns the identifier carried by ctx, or "" if there is none.
func CorrelationID(ctx context.Context) string {
	id, _ := ctx.Value(correlationKey{}).(string)
	return id
}

// With returns logger annotated with the correlation identifier in ctx, or the
// logger unchanged when ctx carries none.
func With(ctx context.Context, logger *slog.Logger) *slog.Logger {
	if id := CorrelationID(ctx); id != "" {
		return logger.With(slog.String(CorrelationField, id))
	}
	return logger
}
