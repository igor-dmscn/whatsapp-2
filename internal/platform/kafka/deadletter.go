package kafka

import (
	"context"
	"encoding/json"
	"log/slog"
	"strconv"
	"time"
)

// TopicDeadLetter is where records that can never be applied are put.
//
// One topic for every consumer rather than one each. What an operator does with these is look at
// them, and looking in five places is how nobody looks at all; the record carries which consumer
// gave up on it, which is the only thing a per-consumer topic would have added.
const TopicDeadLetter = "platform.dead_letter"

// DeadLetter is a record a consumer could not apply, and what it knew at the time.
//
// The original bytes are kept verbatim. Whatever made the record unprocessable is in there, and a
// summary written by the code that could not understand it is not evidence.
type DeadLetter struct {
	// Consumer is which consumer group gave up, since several read the same topics.
	Consumer string `json:"consumer"`
	// Reason is the error, as text. Not a code: nothing branches on this, a person reads it.
	Reason string `json:"reason"`
	// Topic, Partition and Offset locate the original, so it can be found in the log it came
	// from rather than only in the copy here.
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Name      string `json:"name,omitempty"`
	// Value is the record as it arrived, base64 in JSON like every other opaque payload here.
	Value []byte `json:"value"`
	// FailedAt is when the consumer gave up, which is not when the record was produced. The
	// gap between them is how far behind a consumer was, and is usually the interesting part.
	FailedAt time.Time `json:"failed_at"`
}

// DeadLetters publishes records a consumer has given up on.
type DeadLetters struct {
	producer *Producer
	consumer string
	logger   *slog.Logger
}

// NewDeadLetters returns a publisher labelled with the consumer group using it.
//
// A nil producer is allowed and means "log only", which is what a consumer running somewhere
// without a producer does — and is the behaviour every consumer had before this existed.
func NewDeadLetters(producer *Producer, consumer string, logger *slog.Logger) *DeadLetters {
	return &DeadLetters{producer: producer, consumer: consumer, logger: logger}
}

// Record publishes a record that could not be applied.
//
// Best-effort, and it must be: this is already the failure path, and a consumer that stopped
// because it could not report that it was skipping something would have converted one lost
// projection into a stalled partition — the exact outcome the skip exists to avoid.
//
// Keyed by topic and offset so that redeliveries of the same bad record land on one partition and
// next to each other, which is what makes the topic readable rather than a scattering.
func (d *DeadLetters) Record(ctx context.Context, record Record, reason error) {
	logger := d.logger.With(
		slog.String("consumer", d.consumer),
		slog.String("topic", record.Topic),
		slog.Int64("offset", record.Offset),
		slog.String("event", record.Name),
	)

	if d.producer == nil {
		logger.Error("dropping unprocessable record; no dead-letter producer",
			slog.Any("error", reason))
		return
	}

	letter := DeadLetter{
		Consumer:  d.consumer,
		Reason:    reason.Error(),
		Topic:     record.Topic,
		Partition: record.Partition,
		Offset:    record.Offset,
		Name:      record.Name,
		Value:     record.Value,
		FailedAt:  time.Now(),
	}

	encoded, err := json.Marshal(letter)
	if err != nil {
		logger.Error("cannot encode a dead letter", slog.Any("error", err))
		return
	}

	// Its own timeout rather than the consumer's context, which may already be cancelled:
	// a worker shutting down mid-record should still manage to record why it gave up.
	publishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), deadLetterTimeout)
	defer cancel()

	if err := d.producer.Publish(publishCtx, []Message{{
		Topic: TopicDeadLetter,
		Key:   record.Topic + ":" + strconv.FormatInt(record.Offset, 10),
		Value: encoded,
		Name:  "platform.record_skipped",
	}}); err != nil {
		logger.Error("cannot publish a dead letter; the record is only in this log",
			slog.Any("error", err), slog.Any("cause", reason))
		return
	}

	// Still logged, at error, even when publishing worked. A skipped record means something
	// is now permanently a little wrong, and the log is where somebody is already looking.
	logger.Error("skipped an unprocessable record", slog.Any("error", reason))
}

const deadLetterTimeout = 5 * time.Second
