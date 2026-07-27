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
	"go.opentelemetry.io/otel/attribute"

	"comms/internal/platform/logging"
	"comms/internal/platform/tracing"
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
	// TopicCallingEvents carries what happened to a call, keyed by call.
	//
	// Nothing consumes it yet. It exists because a call log is the obvious next thing
	// somebody will want and these events are the only record that a call happened —
	// unlike a projection, which can be rebuilt, a fact not published is gone.
	TopicCallingEvents = "calling.events"
	// TopicMediaAttachments carries uploads awaiting processing.
	//
	// This one is a work queue rather than a stream of facts other contexts observe,
	// which is why it is on its own topic: a thirty-second video occupies a consumer
	// for as long as it takes to decode, and behind entries on a shared topic that
	// would delay every unread badge in the system.
	TopicMediaAttachments = "media.attachments"
)

// AllTopics is what EnsureTopics creates.
var AllTopics = []string{
	TopicMessagingEntries, TopicMessagingReceipts, TopicIdentityEvents,
	TopicMediaAttachments, TopicCallingEvents, TopicDeadLetter,
}

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
	// CorrelationID ties this event to the request that caused it (NF-16). A header
	// rather than part of the payload, so that adding it did not change the wire format
	// every consumer already parses — and so the relay does not have to decode a payload
	// to attach it. Empty for an event with no request behind it.
	CorrelationID string
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

	// The publishing span, and the trace context that goes on every record in this batch.
	// One span for the batch rather than per record: the relay publishes what it drained
	// together, and a span per row would be a hundred siblings saying the same thing.
	ctx, span := tracing.Start(ctx, "kafka.publish",
		attribute.Int("messaging.batch.message_count", len(messages)))
	defer span.End()
	carried := tracing.Inject(ctx)

	records := make([]*kgo.Record, 0, len(messages))
	for _, message := range messages {
		records = append(records, &kgo.Record{
			Topic:   message.Topic,
			Key:     []byte(message.Key),
			Value:   message.Value,
			Headers: headersFor(message, carried),
		})
	}

	results := p.client.ProduceSync(ctx, records...)
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("publish %d records: %w", len(records), err)
	}
	return nil
}

// headersFor builds a record's headers.
//
// The event name is always there; the correlation identifier only when there is one. An empty
// header would be indistinguishable from a header a proxy blanked, and the consumer would then
// log "correlation_id: " on every line — which is worse than the field being absent.
func headersFor(message Message, carried tracing.Carrier) []kgo.RecordHeader {
	headers := []kgo.RecordHeader{{Key: "event_name", Value: []byte(message.Name)}}
	if message.CorrelationID != "" {
		headers = append(headers,
			kgo.RecordHeader{Key: CorrelationHeader, Value: []byte(message.CorrelationID)})
	}
	// The trace context, so a consumer's span continues this trace instead of starting an
	// unrelated one. Absent entirely when tracing is off, which is when Inject returns
	// nothing — a header carrying an invalid trace is worse than no header.
	for key, value := range carried {
		headers = append(headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
	}
	return headers
}

// CorrelationHeader is the header a correlation identifier travels in.
//
// The same name the HTTP layer uses, lowercased as Kafka headers conventionally are, so that one
// grep finds the identifier wherever it appears.
const CorrelationHeader = "correlation_id"

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

// Handler processes one record. Returning an error stops that partition: the offset is not
// committed and the position is rewound, so the record is delivered again.
type Handler func(ctx context.Context, record Record) error

// Record is a consumed event.
type Record struct {
	Topic string
	Key   string
	Name  string
	Value []byte
	// Partition and Offset together identify the record. Offset alone does not: offsets are
	// per partition, so two records on one topic routinely share one.
	Partition int32
	Offset    int64
	// CorrelationID ties this record to the request that caused it, or is empty. Restored
	// into the handler's context by Run, so a consumer's log lines carry it without every
	// consumer having to remember to.
	CorrelationID string
	// carried holds the record's remaining headers, which is where trace context lives.
	// Unexported: a handler has no business reading raw headers, and Run has already used
	// them to continue the trace by the time the handler sees the record.
	carried tracing.Carrier
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

// retryDelay bounds how fast a failing partition is retried.
//
// Doubling from the first to the last, reset by any poll that fully succeeds. A handler failing
// because Postgres is unreachable would otherwise re-read and re-fail as fast as the broker can
// serve it, which turns one outage into two.
const (
	retryDelayFirst = 100 * time.Millisecond
	retryDelayMax   = 5 * time.Second
)

// Run polls and handles records until ctx is cancelled.
//
// A handler error rewinds that partition to the failing record, so it and everything behind it
// are delivered again. That is at-least-once, deliberately: a projection that skipped a record it
// could not process would be silently wrong forever, which is worse than processing one twice. A
// record that can never succeed is the handler's own to skip — each consumer decides that for
// itself and dead-letters it (see the projector's errUnprocessable), because what is
// unprocessable is a question about the event, not about Kafka.
//
// The rewind is the whole mechanism, and leaving it out is a silent bug rather than a slow one.
// Not committing is not enough: the fetch position lives in this client's memory and has already
// moved past the record, so a failure without a rewind means this consumer never sees that record
// again — and the next poll that succeeds commits over it. That reads in the logs as one handled
// failure and is in fact a lost event.
func (c *Consumer) Run(ctx context.Context, handle Handler) {
	defer c.client.Close()

	var delay time.Duration
	for {
		if ctx.Err() != nil {
			return
		}
		if delay > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
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

		var handled []*kgo.Record
		rewind := make(map[string]map[int32]kgo.EpochOffset)

		// Per partition rather than per record, because a partition is the unit that has an
		// order worth keeping. One partition failing says nothing about the others, and
		// stopping all of them would let one slow projection hold up every other.
		fetches.EachPartition(func(partition kgo.FetchTopicPartition) {
			for _, record := range partition.Records {
				held := toRecord(record)
				// The identifier is put back on the context before the handler runs, so that
				// everything a projection logs lands under the same identifier as the HTTP
				// request that caused the event — which is the whole of NF-16 and the half
				// that was missing. Restored here rather than in each consumer, because a
				// consumer that forgot would be silently uncorrelated.
				handlerCtx := ctx
				if held.CorrelationID != "" {
					handlerCtx = logging.Correlate(ctx, held.CorrelationID)
				}

				// The trace continued rather than begun, which is what makes one send a
				// single trace across api and worker instead of two unrelated ones.
				handlerCtx = tracing.Extract(handlerCtx, held.carried)
				handlerCtx, span := tracing.Start(handlerCtx, "kafka.consume "+held.Topic,
					attribute.String("messaging.destination.name", held.Topic),
					attribute.String("messaging.message.name", held.Name))

				err := handle(handlerCtx, held)
				span.End()

				if err != nil {
					logging.With(handlerCtx, c.logger).Error("handle record",
						slog.String("topic", record.Topic),
						slog.Int("partition", int(record.Partition)),
						slog.Int64("offset", record.Offset),
						slog.Any("error", err),
					)
					// Back to this record, not past it. Everything after it in this
					// partition is left unread rather than attempted: they are behind it
					// in an order that exists to be honoured, and a cursor advance
					// applied before the entry it refers to is a projection disagreeing
					// with the log.
					if rewind[record.Topic] == nil {
						rewind[record.Topic] = make(map[int32]kgo.EpochOffset)
					}
					rewind[record.Topic][record.Partition] = kgo.EpochOffset{
						Epoch:  record.LeaderEpoch,
						Offset: record.Offset,
					}
					return
				}
				handled = append(handled, record)
			}
		})

		// Only what was handled, and by record rather than by "everything fetched". The
		// distinction is the bug this replaces: committing what was fetched acknowledges
		// records nothing has looked at.
		if len(handled) > 0 {
			if err := c.client.CommitRecords(ctx, handled...); err != nil {
				// Not fatal: the records will be redelivered and reapplied, which is
				// what the handlers are built to tolerate.
				c.logger.Warn("commit offsets", slog.Any("error", err))
			}
		}

		if len(rewind) == 0 {
			delay = 0
			continue
		}

		// After the commit and outside the poll, which is where franz-go asks for this. A
		// rebalance in between drops the partitions this no longer owns, and their new owner
		// starts from the offset just committed — which excludes the failing record, so it is
		// delivered there instead. The outcome is the same either way.
		//
		// ponytail: a rebalance mid-batch is handled by being harmless rather than by being
		// prevented. BlockRebalanceOnPoll, if this ever needs to be exact rather than
		// eventually right.
		c.client.SetOffsets(rewind)

		delay = min(max(2*delay, retryDelayFirst), retryDelayMax)
	}
}

func toRecord(record *kgo.Record) Record {
	var name, correlationID string
	carried := tracing.Carrier{}
	for _, header := range record.Headers {
		switch header.Key {
		case "event_name":
			name = string(header.Value)
		case CorrelationHeader:
			correlationID = string(header.Value)
		default:
			// Everything else is offered to the propagator, which takes the trace headers
			// and ignores the rest. Naming them here instead would mean this file knowing
			// the field names of a specification it does not implement.
			carried[header.Key] = string(header.Value)
		}
	}
	return Record{
		Topic:         record.Topic,
		Key:           string(record.Key),
		Name:          name,
		Value:         record.Value,
		Partition:     record.Partition,
		Offset:        record.Offset,
		CorrelationID: correlationID,
		carried:       carried,
	}
}
