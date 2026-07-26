// Package kafka publishes and consumes events.
//
// Kafka is not the log (ADR-0003). Postgres holds the truth; these topics carry
// consequences — projections, receipts, notifications, media work. Nothing here may
// be the only copy of anything.
//
// franz-go rather than sarama or kafka-go: it is pure Go, so no cgo and no
// librdkafka in the build, and its consumer-group implementation is the one that
// handles rebalances without losing or double-committing offsets on its own.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"comms/internal/platform/logging"
)

// Topics every deployment expects to exist.
//
// Named here rather than in the contexts that use them because the topic list is a
// deployment concern — something has to create them — while what goes on each topic
// is not. Keyed by conversation for messaging and by account for identity, which is
// what makes per-conversation and per-account ordering hold.
const (
	// TopicMessagingEntries carries what happened to a conversation's log.
	TopicMessagingEntries = "messaging.entries"
	// TopicMessagingReceipts carries cursor advances and delivery acknowledgements.
	// Separate from entries so a burst of read receipts cannot delay the entries a
	// projection needs first.
	TopicMessagingReceipts = "messaging.receipts"
	// TopicIdentityEvents carries account, device and session facts. Messaging
	// consumes device revocation from here.
	TopicIdentityEvents = "identity.events"
)

// AllTopics is what EnsureTopics creates.
var AllTopics = []string{TopicMessagingEntries, TopicMessagingReceipts, TopicIdentityEvents}

// Brokers splits a comma-separated broker list.
func Brokers(configured string) []string {
	brokers := strings.Split(configured, ",")
	for index := range brokers {
		brokers[index] = strings.TrimSpace(brokers[index])
	}
	return brokers
}

// Producer publishes records.
type Producer struct {
	client *kgo.Client
	logger *slog.Logger
}

// NewProducer connects to the brokers and verifies they answer.
//
// Failing at start rather than on the first publish: a relay that cannot reach
// Kafka has nothing useful to do, and discovering that from a backlog rather than a
// startup error wastes the time between.
func NewProducer(ctx context.Context, brokers []string, logger *slog.Logger) (*Producer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		// Idempotent production, which franz-go enables by default: a retried
		// publish after a lost acknowledgement does not append a duplicate. It does
		// not make the pipeline exactly-once — the relay can still republish a row
		// whose mark did not commit — which is why consumers are idempotent too.
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(5*time.Millisecond),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx); err != nil {
		client.Close()
		return nil, fmt.Errorf("ping kafka: %w", err)
	}

	return &Producer{client: client, logger: logger}, nil
}

// Message is one record to publish.
type Message struct {
	Topic string
	Key   string
	Value []byte
	// Name is the event name, carried as a header so a consumer can route without
	// decoding the payload first.
	Name string
}

// Publish sends messages and waits for them to be acknowledged by every in-sync
// replica.
//
// Synchronous on purpose. The caller — the relay — marks rows published only after
// this returns, and doing that before the broker has the record would lose events
// on a broker failure while reporting success.
func (p *Producer) Publish(ctx context.Context, messages []Message) error {
	if len(messages) == 0 {
		return nil
	}

	records := make([]*kgo.Record, 0, len(messages))
	for _, message := range messages {
		records = append(records, &kgo.Record{
			Topic: message.Topic,
			Key:   []byte(message.Key),
			Value: message.Value,
			Headers: []kgo.RecordHeader{
				{Key: "event_name", Value: []byte(message.Name)},
			},
		})
	}

	results := p.client.ProduceSync(ctx, records...)
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("publish %d records: %w", len(records), err)
	}
	return nil
}

// Close flushes and disconnects.
func (p *Producer) Close() {
	p.client.Close()
}

// EnsureTopics creates any topic that does not exist. With no topics named, it
// creates AllTopics.
//
// Auto-creation is a broker setting nobody should depend on: it produces a
// single-partition topic with default retention, which is never what was wanted, and
// it hides a typo in a topic name as a new topic rather than an error.
func EnsureTopics(ctx context.Context, brokers []string, partitions int32, logger *slog.Logger, topics ...string) error {
	if len(topics) == 0 {
		topics = AllTopics
	}

	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return fmt.Errorf("create kafka client: %w", err)
	}
	defer client.Close()

	admin := kadm.NewClient(client)
	responses, err := admin.CreateTopics(ctx, partitions, -1, nil, topics...)
	if err != nil {
		return fmt.Errorf("create topics: %w", err)
	}

	for _, response := range responses {
		switch {
		case response.Err == nil:
			logger.Info("topic created", slog.String("topic", response.Topic))
		case errors.Is(response.Err, kerr.TopicAlreadyExists):
			logger.Debug("topic exists", slog.String("topic", response.Topic))
		default:
			return fmt.Errorf("create topic %s: %w", response.Topic, response.Err)
		}
	}
	return nil
}

// Handler processes one record. Returning an error stops the batch: the offset is
// not committed, so the record is redelivered.
type Handler func(ctx context.Context, record Record) error

// Record is a consumed event.
type Record struct {
	Topic  string
	Key    string
	Name   string
	Value  []byte
	Offset int64
}

// Consumer reads a consumer group's share of one or more topics.
type Consumer struct {
	client *kgo.Client
	logger *slog.Logger
	group  string
}

// NewConsumer joins a consumer group.
func NewConsumer(brokers []string, group string, topics []string, logger *slog.Logger) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		// Earliest, so a projection added later builds from the whole history rather
		// than from whenever it happened to be deployed. Consumers are idempotent
		// (NF-8), which is what makes replaying the topic a safe thing to do.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		// Offsets are committed by this code after a batch is handled, not on a
		// timer. Automatic commits acknowledge records that have been fetched rather
		// than records that have been processed, which turns any handler failure
		// into silent data loss.
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka consumer: %w", err)
	}
	return &Consumer{client: client, logger: logger, group: group}, nil
}

// Run polls and handles records until ctx is cancelled.
//
// A handler error stops that batch without committing, so everything from the
// failing record on is redelivered. That is at-least-once, deliberately: a
// projection that skipped a record it could not process would be silently wrong
// forever, which is worse than processing one twice.
func (c *Consumer) Run(ctx context.Context, handle Handler) {
	defer c.client.Close()

	for {
		if ctx.Err() != nil {
			return
		}

		fetches := c.client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, fetchErr := range errs {
				if errors.Is(fetchErr.Err, context.Canceled) {
					return
				}
				c.logger.Warn("kafka fetch",
					slog.String("topic", fetchErr.Topic),
					slog.Any("error", fetchErr.Err),
				)
			}
			continue
		}

		failed := false
		fetches.EachRecord(func(record *kgo.Record) {
			// Everything after a failure in this poll is left uncommitted too:
			// committing later records would acknowledge past the one that failed
			// and lose it.
			if failed {
				return
			}
			if err := handle(ctx, toRecord(record)); err != nil {
				logging.With(ctx, c.logger).Error("handle record",
					slog.String("topic", record.Topic),
					slog.Int64("offset", record.Offset),
					slog.Any("error", err),
				)
				failed = true
			}
		})

		if failed {
			continue
		}
		if err := c.client.CommitUncommittedOffsets(ctx); err != nil {
			// Not fatal: the records will be redelivered and reapplied, which is
			// what the handlers are built to tolerate.
			c.logger.Warn("commit offsets", slog.Any("error", err))
		}
	}
}

func toRecord(record *kgo.Record) Record {
	var name string
	for _, header := range record.Headers {
		if header.Key == "event_name" {
			name = string(header.Value)
		}
	}
	return Record{
		Topic:  record.Topic,
		Key:    string(record.Key),
		Name:   name,
		Value:  record.Value,
		Offset: record.Offset,
	}
}
