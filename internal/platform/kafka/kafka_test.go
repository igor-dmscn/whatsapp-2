// Package kafka_test proves the transport end of ADR-0003: that what the relay
// publishes is what a consumer group receives, keyed and ordered as the projections
// depend on.
//
// Skipped unless KAFKA_BROKERS is set, the same bargain testdb makes for Postgres.
// The projections' own correctness is established without a broker in
// internal/messaging/projections_test.go — what needs a real broker is only whether
// the wire between them behaves.
package kafka_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"comms/internal/platform/kafka"
)

// topicFor gives a test its own topic, so nothing it publishes reaches a real
// consumer.
func topicFor(t *testing.T) string {
	return fmt.Sprintf("test.%s.%d", t.Name(), time.Now().UnixNano())
}

// brokers skips the test when Kafka is unavailable.
func brokers(t *testing.T) []string {
	t.Helper()

	configured := os.Getenv("KAFKA_BROKERS")
	if configured == "" {
		t.Skip("KAFKA_BROKERS not set, skipping integration test")
	}
	return kafka.Brokers(configured)
}

func TestPublishedRecordsReachAConsumerInKeyOrder(t *testing.T) {
	addresses := brokers(t)
	logger := slog.New(slog.DiscardHandler)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// A topic of its own, not messaging.entries. Publishing test records onto a
	// production topic hands them to the real projector, and a record shaped for a
	// transport assertion is not a record a projection can apply — which is how this
	// test wedged the worker before it had its own topic.
	topic := topicFor(t)
	if err := kafka.EnsureTopics(ctx, addresses, 3, logger, topic); err != nil {
		t.Fatalf("ensure topics: %v", err)
	}

	producer, err := kafka.NewProducer(ctx, addresses, logger)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer producer.Close()

	// A key unique to this run, so the assertions are about this test's records and
	// not about whatever the topic already holds.
	key := fmt.Sprintf("conversation-%d", time.Now().UnixNano())

	const count = 20
	messages := make([]kafka.Message, 0, count)
	for index := range count {
		messages = append(messages, kafka.Message{
			Topic: topic,
			Key:   key,
			Name:  "messaging.entry_appended",
			Value: []byte(fmt.Sprintf(`{"name":"messaging.entry_appended","data":{"Sequence":%d}}`, index+1)),
		})
	}

	if err := producer.Publish(ctx, messages); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Its own group, so this test consumes from the start without competing with the
	// worker's group or moving its offsets.
	group := fmt.Sprintf("test-%d", time.Now().UnixNano())
	consumer, err := kafka.NewConsumer(addresses, group, []string{topic}, logger)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}

	var (
		mutex    sync.Mutex
		received []kafka.Record
		done     = make(chan struct{})
	)

	consumeCtx, stopConsuming := context.WithCancel(ctx)
	defer stopConsuming()

	go func() {
		defer close(done)
		consumer.Run(consumeCtx, func(_ context.Context, record kafka.Record) error {
			if record.Key != key {
				return nil
			}
			mutex.Lock()
			defer mutex.Unlock()
			received = append(received, record)
			if len(received) == count {
				stopConsuming()
			}
			return nil
		})
	}()

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timed out waiting for records")
	}

	mutex.Lock()
	defer mutex.Unlock()

	if len(received) != count {
		t.Fatalf("received %d records, want %d", len(received), count)
	}

	// Order within one key is the whole reason messaging events are keyed by
	// conversation: entry 41 must not be projected before entry 40.
	for index, record := range received {
		want := fmt.Sprintf(`"Sequence":%d}`, index+1)
		if !strings.Contains(string(record.Value), want) {
			t.Fatalf("record %d = %s, want it to contain %s — key ordering was not preserved",
				index, record.Value, want)
		}
		if record.Name != "messaging.entry_appended" {
			t.Errorf("record %d name = %q, want the header to survive", index, record.Name)
		}
	}
}

func TestAHandlerFailureLeavesRecordsToBeRedelivered(t *testing.T) {
	// At-least-once, deliberately. A projection that skipped a record it could not
	// process would be silently wrong forever, which is worse than processing one
	// twice — and processing twice is what the projections are built to tolerate.
	addresses := brokers(t)
	logger := slog.New(slog.DiscardHandler)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	topic := topicFor(t)
	if err := kafka.EnsureTopics(ctx, addresses, 3, logger, topic); err != nil {
		t.Fatalf("ensure topics: %v", err)
	}

	producer, err := kafka.NewProducer(ctx, addresses, logger)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer producer.Close()

	key := fmt.Sprintf("conversation-%d", time.Now().UnixNano())
	if err := producer.Publish(ctx, []kafka.Message{{
		Topic: topic,
		Key:   key,
		Name:  "messaging.cursor_advanced",
		Value: []byte(`{"name":"messaging.cursor_advanced","data":{"Through":1}}`),
	}}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	group := fmt.Sprintf("test-%d", time.Now().UnixNano())

	// First consumer: fails on the record, so its offset is never committed.
	failing, err := kafka.NewConsumer(addresses, group, []string{topic}, logger)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}

	seen := make(chan struct{}, 1)
	failCtx, stopFailing := context.WithCancel(ctx)
	failDone := make(chan struct{})
	go func() {
		defer close(failDone)
		failing.Run(failCtx, func(_ context.Context, record kafka.Record) error {
			if record.Key != key {
				return nil
			}
			select {
			case seen <- struct{}{}:
			default:
			}
			return fmt.Errorf("deliberate failure")
		})
	}()

	select {
	case <-seen:
	case <-ctx.Done():
		t.Fatal("timed out waiting for the first delivery")
	}
	stopFailing()
	<-failDone

	// Second consumer in the same group: the record is still there, because nothing
	// acknowledged it.
	recovering, err := kafka.NewConsumer(addresses, group, []string{topic}, logger)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}

	redelivered := make(chan struct{}, 1)
	recoverCtx, stopRecovering := context.WithCancel(ctx)
	defer stopRecovering()
	recoverDone := make(chan struct{})
	go func() {
		defer close(recoverDone)
		recovering.Run(recoverCtx, func(_ context.Context, record kafka.Record) error {
			if record.Key != key {
				return nil
			}
			select {
			case redelivered <- struct{}{}:
			default:
			}
			return nil
		})
	}()

	select {
	case <-redelivered:
	case <-ctx.Done():
		t.Fatal("the record was not redelivered after a handler failure — an event was lost")
	}
	stopRecovering()
	<-recoverDone
}
