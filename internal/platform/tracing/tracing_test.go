// Package tracing_test asserts the two things about tracing that can be wrong quietly.
//
// A trace that does not span processes looks fine in isolation: every service produces spans,
// every span has a duration, and nothing reports an error — the failure is that the request and
// the work it caused are two unrelated traces, which you only notice when you try to follow one.
// And a span with no correlation identifier is findable only by somebody who already has the
// trace identifier, which is nobody reading a log line.
//
// No collector and no exporter. What is under test is propagation and attribution, which are
// decided before an exporter sees anything — so these run with the recorder the SDK provides,
// which is also what makes them runnable with `go test ./...` and nothing else.
package tracing_test

import (
	"context"
	"log/slog"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"comms/internal/platform/logging"
	"comms/internal/platform/tracing"
)

// recorded installs a tracer that keeps every finished span in memory.
func recorded(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()

	// The propagator has to be installed for Inject and Extract to do anything, and Setup is
	// what installs it — called with no endpoint configured, which is the no-op path every
	// developer machine runs.
	if _, err := tracing.Setup(context.Background(), "test", slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("setup: %v", err)
	}

	recorder := tracetest.NewSpanRecorder()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder)))
	return recorder
}

// TestASpanCarriesTheCorrelationIdentifier is what makes a trace findable from a log line.
func TestASpanCarriesTheCorrelationIdentifier(t *testing.T) {
	recorder := recorded(t)

	ctx := logging.Correlate(context.Background(), "trace-me")
	_, span := tracing.Start(ctx, "something")
	span.End()

	finished := recorder.Ended()
	if len(finished) != 1 {
		t.Fatalf("recorded %d spans, want 1", len(finished))
	}

	var found string
	for _, attribute := range finished[0].Attributes() {
		if string(attribute.Key) == tracing.CorrelationAttribute {
			found = attribute.Value.AsString()
		}
	}
	if found != "trace-me" {
		t.Fatalf("the span carries %q as its correlation identifier, want %q", found, "trace-me")
	}
}

// TestASpanWithNoIdentifierCarriesNoAttribute: absence rather than an empty string.
//
// A span attributed with "" says "correlated with nothing", which is a claim. Anything running
// without a request behind it — a relay tick, a heartbeat — legitimately has no identifier.
func TestASpanWithNoIdentifierCarriesNoAttribute(t *testing.T) {
	recorder := recorded(t)

	_, span := tracing.Start(context.Background(), "background work")
	span.End()

	for _, attribute := range recorder.Ended()[0].Attributes() {
		if string(attribute.Key) == tracing.CorrelationAttribute {
			t.Fatalf("a span with no request behind it carries %q", attribute.Value.AsString())
		}
	}
}

// TestATraceSurvivesBeingCarriedOnAMessage is the claim NF-16 actually makes: one trace across
// processes, not one per process.
//
// The carrier stands in for Kafka headers, which is where this is used. What is asserted is that
// the consumer's span belongs to the producer's trace and names it as its parent — a consumer
// that extracted nothing would produce a root span with the same name and look identical in
// isolation.
func TestATraceSurvivesBeingCarriedOnAMessage(t *testing.T) {
	recorder := recorded(t)

	// The producer: an api node handling a request.
	producerCtx, producing := tracing.Start(
		logging.Correlate(context.Background(), "one-request"), "http POST /entries")
	carried := tracing.Inject(producerCtx)
	producing.End()

	if len(carried) == 0 {
		t.Fatal("nothing was injected, so nothing could be propagated")
	}

	// The consumer: a worker, in another process, given only the headers.
	consumerCtx := tracing.Extract(context.Background(), carried)
	_, consuming := tracing.Start(consumerCtx, "kafka.consume messaging.entries")
	consuming.End()

	finished := recorder.Ended()
	if len(finished) != 2 {
		t.Fatalf("recorded %d spans, want 2", len(finished))
	}

	// Ended in the order they finished, so the producer is first.
	produced, consumed := finished[0], finished[1]
	if produced.SpanContext().TraceID() != consumed.SpanContext().TraceID() {
		t.Fatalf("two traces (%s and %s), want one",
			produced.SpanContext().TraceID(), consumed.SpanContext().TraceID())
	}
	if consumed.Parent().SpanID() != produced.SpanContext().SpanID() {
		t.Fatalf("the consumer's parent is %s, want the producer's %s",
			consumed.Parent().SpanID(), produced.SpanContext().SpanID())
	}
}
