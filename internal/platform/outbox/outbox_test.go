// Package outbox_test establishes the guarantees the rest of the system assumes:
// that an event row commits with the change it describes, that the relay never loses
// a row, and that stopping the relay costs delay rather than data.
package outbox_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"comms/internal/platform/database"
	"comms/internal/platform/database/testdb"
	"comms/internal/platform/id"
	"comms/internal/platform/kafka"
	"comms/internal/platform/logging"
	"comms/internal/platform/outbox"
)

// recordingPublisher stands in for Kafka. What is being tested here is the relay's
// contract with the table, not the broker.
type recordingPublisher struct {
	mutex     sync.Mutex
	published []kafka.Message
	// failNext makes the next publish fail, which is how the crash-mid-publish path
	// is reached deliberately rather than hoped for.
	failNext bool
}

func (p *recordingPublisher) Publish(_ context.Context, messages []kafka.Message) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.failNext {
		p.failNext = false
		return errors.New("broker unavailable")
	}
	p.published = append(p.published, messages...)
	return nil
}

// onTopic returns every message published on one topic, so a test sharing a database with others
// asserts on its own rows.
func (p *recordingPublisher) onTopic(topic string) []kafka.Message {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	found := make([]kafka.Message, 0, len(p.published))
	for _, message := range p.published {
		if message.Topic == topic {
			found = append(found, message)
		}
	}
	return found
}

// names returns what was published on one topic.
//
// Filtered by topic because `go test ./...` runs packages concurrently and the other
// packages write to the same outbox table — the relay drains all of it, so an
// unfiltered assertion is an assertion about whatever else happened to be running.
func (p *recordingPublisher) names(topic string) []string {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	names := make([]string, 0, len(p.published))
	for _, message := range p.published {
		if message.Topic == topic {
			names = append(names, message.Name)
		}
	}
	return names
}

func (p *recordingPublisher) fail() {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.failNext = true
}

type harness struct {
	db        *sql.DB
	writer    *outbox.Writer
	relay     *outbox.Relay
	publisher *recordingPublisher
	// topic isolates a test's rows from every other test's, since they share a
	// database and the relay drains the whole table.
	topic string
}

func newHarness(t *testing.T) *harness {
	t.Helper()

	db := testdb.Open(t)
	publisher := &recordingPublisher{}

	return &harness{
		db:        db,
		writer:    outbox.NewWriter(db),
		relay:     outbox.NewRelay(db, publisher, slog.New(slog.DiscardHandler)),
		publisher: publisher,
		topic:     "test." + t.Name(),
	}
}

// drainAll empties the outbox, including rows other packages' tests wrote.
func (h *harness) drainAll(t *testing.T) {
	t.Helper()

	for {
		drained, err := h.relay.Drain(context.Background())
		if err != nil {
			t.Fatalf("drain: %v", err)
		}
		if drained == 0 {
			return
		}
	}
}

// pending counts unpublished rows on this test's topic only.
func (h *harness) pending(t *testing.T) int {
	t.Helper()

	var count int
	err := h.db.QueryRow(
		`SELECT count(*) FROM outbox WHERE published_at IS NULL AND topic = $1`, h.topic).Scan(&count)
	if err != nil {
		t.Fatalf("count pending: %v", err)
	}
	return count
}

// write appends records in a transaction, as production always does.
func (h *harness) write(t *testing.T, names ...string) {
	t.Helper()

	records := make([]outbox.Record, 0, len(names))
	for index, name := range names {
		record, err := outbox.Encode(h.topic, fmt.Sprintf("key-%d", index), name, time.Now(), map[string]string{"name": name})
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		records = append(records, record)
	}

	err := database.InTransaction(context.Background(), h.db, func(ctx context.Context) error {
		return h.writer.Write(ctx, records)
	})
	if err != nil {
		t.Fatalf("write: %v", err)
	}
}

func TestWritingOutsideATransactionIsRefused(t *testing.T) {
	// The whole point of the outbox is that the row and the state change commit
	// together. A write on its own connection compiles and runs and quietly
	// reintroduces the split-brain, so it is refused rather than trusted to review.
	test := newHarness(t)

	record, err := outbox.Encode(test.topic, "key", "test.event", time.Now(), map[string]string{})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	err = test.writer.Write(context.Background(), []outbox.Record{record})
	if !errors.Is(err, database.ErrNoTransaction) {
		t.Fatalf("error = %v, want ErrNoTransaction", err)
	}
}

func TestRolledBackWorkPublishesNothing(t *testing.T) {
	// The guarantee in the direction that matters most: an event must never announce
	// something that did not happen.
	test := newHarness(t)

	record, err := outbox.Encode(test.topic, "key", "test.event", time.Now(), map[string]string{})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	failure := errors.New("the use case failed after recording its event")
	err = database.InTransaction(context.Background(), test.db, func(ctx context.Context) error {
		if err := test.writer.Write(ctx, []outbox.Record{record}); err != nil {
			return err
		}
		return failure
	})
	if !errors.Is(err, failure) {
		t.Fatalf("error = %v, want the injected failure", err)
	}

	test.drainAll(t)
	if published := test.publisher.names(test.topic); len(published) != 0 {
		t.Errorf("published %v from a rolled back transaction, want nothing", published)
	}
}

func TestRelayPublishesEveryRowInOrderAndOnlyOnce(t *testing.T) {
	test := newHarness(t)
	test.write(t, "first", "second", "third")

	test.drainAll(t)

	// Order matters: rows are published in the order they were written, which is what
	// makes per-key ordering hold downstream.
	names := test.publisher.names(test.topic)
	want := []string{"first", "second", "third"}
	for index, expected := range want {
		if index >= len(names) || names[index] != expected {
			t.Fatalf("published %v, want %v", names, want)
		}
	}

	// A second pass republishes nothing.
	test.drainAll(t)
	if got := len(test.publisher.names(test.topic)); got != 3 {
		t.Errorf("published %d messages in total, want 3 — a row was published twice", got)
	}
}

func TestAFailedPublishLeavesRowsToRetry(t *testing.T) {
	// The crash-mid-publish case, which is why the pipeline is at-least-once and why
	// consumers must be idempotent (NF-8). What must never happen is a row marked
	// published that was not.
	test := newHarness(t)
	test.write(t, "first", "second")

	test.publisher.fail()
	if _, err := test.relay.Drain(context.Background()); err == nil {
		t.Fatal("drain succeeded despite a failing publisher")
	}

	// Nothing was marked published, so nothing was lost.
	if pending := test.pending(t); pending != 2 {
		t.Fatalf("pending = %d after a failed publish, want 2", pending)
	}

	test.drainAll(t)
	if pending := test.pending(t); pending != 0 {
		t.Errorf("pending = %d after retrying, want 0", pending)
	}
	if got := len(test.publisher.names(test.topic)); got != 2 {
		t.Errorf("published %d after the retry, want 2", got)
	}
}

func TestStoppingTheRelayDelaysEventsRatherThanLosingThem(t *testing.T) {
	// The plan's verification, run: stop the relay, write ten events, start it again,
	// and assert all ten publish and none twice.
	test := newHarness(t)

	names := make([]string, 0, 10)
	for index := range 10 {
		names = append(names, fmt.Sprintf("event-%d", index))
	}
	// Written while nothing is draining, which is exactly the state a stopped relay
	// leaves the table in.
	test.write(t, names...)

	if pending := test.pending(t); pending != 10 {
		t.Fatalf("pending = %d while the relay is stopped, want 10", pending)
	}

	test.drainAll(t)

	published := test.publisher.names(test.topic)
	if len(published) != 10 {
		t.Fatalf("published %d, want 10", len(published))
	}
	seen := map[string]int{}
	for _, name := range published {
		seen[name]++
	}
	for _, name := range names {
		if seen[name] != 1 {
			t.Errorf("%s published %d times, want once", name, seen[name])
		}
	}
}

func TestRelayRunDrainsWhatArrivesWhileItRuns(t *testing.T) {
	test := newHarness(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		test.relay.Run(ctx)
	}()

	test.write(t, "written while running")

	// Polled rather than slept on: the relay's latency is a poll interval, and
	// asserting on a fixed sleep would be a slower test that fails on a loaded machine.
	deadline := time.Now().Add(5 * time.Second)
	for {
		pending := test.pending(t)
		if pending == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("relay left %d rows pending", pending)
		}
		time.Sleep(20 * time.Millisecond)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("relay did not stop when its context was cancelled")
	}
}

func TestEnvelopeRoundTrip(t *testing.T) {
	// Consumers read the name and time out of the payload, not only out of the Kafka
	// header, because a header is easy to lose in transit and a message that cannot
	// describe itself is a consumer that fails obscurely.
	type payload struct {
		Sequence int64  `json:"Sequence"`
		Author   string `json:"AuthorID"`
	}

	occurredAt := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	record, err := outbox.Encode("topic", "key", "messaging.entry_appended", occurredAt,
		payload{Sequence: 41, Author: "account-1"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	decoded, at, err := outbox.Open[payload](record.Payload)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if decoded.Sequence != 41 || decoded.Author != "account-1" {
		t.Errorf("decoded = %+v, want sequence 41 and account-1", decoded)
	}
	if !at.Equal(occurredAt) {
		t.Errorf("occurred at %v, want %v", at, occurredAt)
	}
}

// TestTheCorrelationIdentifierSurvivesTheOutbox is NF-16's missing half.
//
// A send is an HTTP request, a Redis publish, an outbox row, a Kafka event and a worker
// projection. Requests have carried a correlation identifier since phase 0; events did not, so
// the worker's half of every trace was five log lines in five places with nothing tying them to
// the request that caused them.
//
// Asserted at the seam rather than end to end. What could go wrong is the identifier being
// dropped between the request's context and the row, or between the row and the Kafka message —
// and both of those are here. Whether a consumer then logs it is the consumer's own doing, and
// the kafka package restores it into the handler's context so that it cannot forget.
func TestTheCorrelationIdentifierSurvivesTheOutbox(t *testing.T) {
	h := newHarness(t)

	correlationID := "trace-" + id.New()
	ctx := logging.Correlate(context.Background(), correlationID)

	if err := database.InTransaction(ctx, h.db, func(ctx context.Context) error {
		record, err := outbox.Encode(h.topic, "key-1", "test.event", time.Now(),
			map[string]string{"hello": "world"})
		if err != nil {
			return err
		}
		return h.writer.Write(ctx, []outbox.Record{record})
	}); err != nil {
		t.Fatalf("write: %v", err)
	}

	h.drainAll(t)

	published := h.publisher.onTopic(h.topic)
	if len(published) != 1 {
		t.Fatalf("published %d messages, want 1", len(published))
	}
	if published[0].CorrelationID != correlationID {
		t.Fatalf("the message carries %q, want the request's %q",
			published[0].CorrelationID, correlationID)
	}
}

// TestAnEventWithNoRequestBehindItCarriesNoIdentifier: absence is not an empty string.
//
// A scheduled sweep or a backfill legitimately has no request behind it, and recording "" would
// say "correlated with nothing" — a claim rather than an absence. It also matters on the wire:
// an empty header is indistinguishable from one a proxy blanked, and would put an empty field on
// every log line the consumer writes.
func TestAnEventWithNoRequestBehindItCarriesNoIdentifier(t *testing.T) {
	h := newHarness(t)

	// No Correlate on this context, which is what a background job has.
	if err := database.InTransaction(context.Background(), h.db, func(ctx context.Context) error {
		record, err := outbox.Encode(h.topic, "key-2", "test.event", time.Now(),
			map[string]string{"hello": "world"})
		if err != nil {
			return err
		}
		return h.writer.Write(ctx, []outbox.Record{record})
	}); err != nil {
		t.Fatalf("write: %v", err)
	}

	h.drainAll(t)

	published := h.publisher.onTopic(h.topic)
	if len(published) != 1 {
		t.Fatalf("published %d messages, want 1", len(published))
	}
	if published[0].CorrelationID != "" {
		t.Fatalf("an event with no request behind it carries %q", published[0].CorrelationID)
	}
}
