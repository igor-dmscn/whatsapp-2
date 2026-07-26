// Package tracing turns the correlation identifier into spans (NF-16).
//
// The logs half of that requirement has been true since phase 0: one identifier, threaded through
// a request's context, and now through the outbox and Kafka into the worker. What logs cannot show
// is *shape* — that a send spent 4 ms in Postgres and 300 ms waiting for a broker is a fact about
// nesting and duration, and reconstructing it from timestamps across five services is the work
// tracing exists to remove.
//
// Off unless configured. `OTEL_EXPORTER_OTLP_ENDPOINT` is what turns it on, and with nothing set
// the tracer is a no-op: spans are still created, cost a few nanoseconds, and go nowhere. That is
// deliberate rather than lazy — NF-15 says the whole system starts locally with one command and no
// cloud dependencies, so requiring a collector to run would break a stated requirement to satisfy
// another one.
//
// The correlation identifier is put on every span as an attribute, and the span's own trace
// identifier is *not* used in its place. Two reasons: a client may supply the correlation
// identifier itself, so a person reporting "this failed for me at 14:02" can quote something the
// server did not invent; and it already appears on every log line, which is where somebody looks
// first. One value, findable from either side.
package tracing

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.30.0"
	"go.opentelemetry.io/otel/trace"

	"comms/internal/platform/logging"
)

// CorrelationAttribute is the span attribute the correlation identifier is carried in.
//
// The same name as the log field, so that one value is searchable by one name in both places.
const CorrelationAttribute = "correlation_id"

// Setup installs a global tracer, returning a function that flushes it.
//
// Returns a no-op shutdown and no error when no endpoint is configured, so a caller writes the
// same four lines whether or not tracing is on — a boolean at every call site is how half of them
// end up on the wrong side of it.
func Setup(ctx context.Context, service string, logger *slog.Logger) (func(context.Context), error) {
	// The propagator is installed either way. It costs nothing, and it means a deployment that
	// turns tracing on later finds the plumbing already in place rather than discovering that
	// context was never propagated.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))

	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		logger.Info("tracing is off; set OTEL_EXPORTER_OTLP_ENDPOINT to enable it")
		return func(context.Context) {}, nil
	}

	exporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("create otlp exporter: %w", err)
	}

	described, err := resource.Merge(resource.Default(), resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(service),
	))
	if err != nil {
		return nil, fmt.Errorf("describe this service: %w", err)
	}

	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(described),
	)
	otel.SetTracerProvider(provider)
	logger.Info("tracing is on", slog.String("endpoint", endpoint))

	return func(ctx context.Context) {
		// Its own deadline: shutdown is called while the process is already stopping, often
		// on a context that is cancelled, and losing the last batch of spans because the
		// exporter was handed a dead context is a poor way to end a trace.
		flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), flushGrace)
		defer cancel()

		if err := provider.Shutdown(flushCtx); err != nil {
			logger.Warn("flush traces", slog.Any("error", err))
		}
	}, nil
}

const flushGrace = 5 * time.Second

// Start begins a span, tagged with whatever correlation identifier the context carries.
//
// A thin wrapper, and worth it for one line: every span in this system gets the correlation
// attribute without any caller remembering to add it. A span without it is findable only by
// somebody who already knows the trace identifier, which is nobody reading a log line.
func Start(ctx context.Context, name string, attributes ...attribute.KeyValue) (context.Context, trace.Span) {
	if correlationID := logging.CorrelationID(ctx); correlationID != "" {
		attributes = append(attributes, attribute.String(CorrelationAttribute, correlationID))
	}

	return otel.Tracer("comms").Start(ctx, name, trace.WithAttributes(attributes...))
}

// Carrier adapts a set of string headers to what the propagator expects.
//
// Kafka headers are the one transport here that OpenTelemetry has no packaged carrier for — HTTP
// has one built in. Eight lines rather than another dependency.
type Carrier map[string]string

func (c Carrier) Get(key string) string { return c[key] }
func (c Carrier) Set(key, value string) { c[key] = value }
func (c Carrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for key := range c {
		keys = append(keys, key)
	}
	return keys
}

// Inject writes the current span's context into a carrier, for putting on a message.
func Inject(ctx context.Context) Carrier {
	carrier := Carrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return carrier
}

// Extract restores a span context from a carrier, so a consumer's span continues the producer's
// trace rather than beginning an unrelated one.
//
// This is the join that makes a trace span processes. Without it, the request that wrote an event
// and the worker that consumed it are two traces with no relationship, which is the situation
// before this package — and the reason the correlation identifier had to do all the work alone.
func Extract(ctx context.Context, carrier Carrier) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}
