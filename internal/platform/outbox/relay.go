package outbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"comms/internal/platform/database"
	"comms/internal/platform/kafka"
)

// Publisher is what a relay hands records to. Declared here so this package does
// not depend on the Kafka client, and so tests can count what was published.
type Publisher interface {
	Publish(ctx context.Context, messages []kafka.Message) error
}

// batchSize is how many rows one pass claims.
//
// Bounded because the relay holds a transaction for the length of a pass, and a
// pass that publishes ten thousand records holds a lock and a Kafka round trip open
// for as long as that takes.
const batchSize = 100

// idleDelay is how long to wait after finding nothing.
//
// Polling rather than LISTEN/NOTIFY. Notification would cut the idle latency to
// nothing, but it is delivered at-most-once and only to currently-connected
// sessions, so a poll is still needed for anything written while the relay was
// down. Given that, one mechanism is better than two — and the ceiling this puts on
// delivery latency is well inside NF-7's two seconds.
//
// ponytail: fixed 200ms poll. LISTEN/NOTIFY as a wake-up hint on top, if idle
// latency ever needs to be lower than this.
const idleDelay = 200 * time.Millisecond

// Relay moves outbox rows to Kafka.
//
// The claim is the transaction that marks rows published: rows are read, published,
// and marked in one transaction, so a crash anywhere leaves them unpublished and
// they go again. That is at-least-once. Exactly-once is not available here — the
// publish and the mark are against different systems, which is the same problem the
// outbox solved one layer down and cannot solve again — so consumers are idempotent
// instead (NF-8).
type Relay struct {
	db        database.Conn
	publisher Publisher
	logger    *slog.Logger
}

// NewRelay returns a relay.
func NewRelay(db *sql.DB, publisher Publisher, logger *slog.Logger) *Relay {
	return &Relay{db: database.NewConn(db), publisher: publisher, logger: logger}
}

// Run drains the outbox until ctx is cancelled.
func (r *Relay) Run(ctx context.Context) {
	r.logger.Info("outbox relay started")

	for {
		published, err := r.Drain(ctx)
		switch {
		case errors.Is(err, context.Canceled):
			r.logger.Info("outbox relay stopped")
			return
		case err != nil:
			r.logger.Error("drain outbox", slog.Any("error", err))
		case published > 0:
			// Straight back round: a backlog is drained as fast as it can be
			// published rather than at one batch per poll interval.
			continue
		}

		select {
		case <-ctx.Done():
			r.logger.Info("outbox relay stopped")
			return
		case <-time.After(idleDelay):
		}
	}
}

// Drain publishes one batch and reports how many rows it moved. Exported so a test
// can run exactly one pass rather than racing a goroutine.
func (r *Relay) Drain(ctx context.Context) (int, error) {
	var published int

	err := r.db.InTransaction(ctx, func(ctx context.Context) error {
		// FOR UPDATE, not SKIP LOCKED. Skipping locked rows would let a second relay
		// publish row 20 while the first still holds row 19 — reordering two events
		// for the same conversation, which is exactly what keying by conversation
		// exists to prevent. One relay at a time is the price of ordering.
		//
		// ponytail: this serialises the relay across replicas rather than sharding
		// it. Per-key claims, if one relay ever stops keeping up.
		rows, err := r.db.QueryContext(ctx,
			`SELECT id, topic, key, event_name, payload, correlation_id
			   FROM outbox
			  WHERE published_at IS NULL
			  ORDER BY id
			  LIMIT $1
			 FOR UPDATE`,
			batchSize,
		)
		if err != nil {
			return fmt.Errorf("select unpublished: %w", err)
		}

		var (
			ids      []int64
			messages []kafka.Message
		)
		for rows.Next() {
			var (
				id            int64
				message       kafka.Message
				correlationID sql.NullString
			)
			if err := rows.Scan(&id, &message.Topic, &message.Key, &message.Name,
				&message.Value, &correlationID); err != nil {
				rows.Close()
				return fmt.Errorf("scan outbox row: %w", err)
			}
			// Carried onto the message so the consumer can log under the same identifier
			// as the request that caused the row (NF-16). Rows written before the column
			// existed, and events with no request behind them, have none.
			message.CorrelationID = correlationID.String
			ids = append(ids, id)
			messages = append(messages, message)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("read outbox rows: %w", err)
		}
		rows.Close()

		if len(ids) == 0 {
			return nil
		}

		// Attempts are counted before the publish, so a row that makes the publisher
		// fail every time is visible as a row with many attempts rather than as an
		// invisible loop. The increment is rolled back along with everything else
		// when the publish fails, which is the honest trade: the count reflects
		// completed passes, not attempted ones.
		if _, err := r.db.ExecContext(ctx,
			`UPDATE outbox SET attempts = attempts + 1 WHERE id = ANY($1)`, ids,
		); err != nil {
			return fmt.Errorf("count attempt: %w", err)
		}

		if err := r.publisher.Publish(ctx, messages); err != nil {
			return fmt.Errorf("publish batch: %w", err)
		}

		if _, err := r.db.ExecContext(ctx,
			`UPDATE outbox SET published_at = now() WHERE id = ANY($1)`, ids,
		); err != nil {
			return fmt.Errorf("mark published: %w", err)
		}

		published = len(ids)
		return nil
	})
	if err != nil {
		return 0, err
	}
	return published, nil
}

// Pending reports how many rows are waiting. For health reporting and tests.
func (r *Relay) Pending(ctx context.Context) (int64, error) {
	var pending int64
	err := r.db.QueryRowContext(ctx,
		`SELECT count(*) FROM outbox WHERE published_at IS NULL`,
	).Scan(&pending)
	if err != nil {
		return 0, fmt.Errorf("count pending: %w", err)
	}
	return pending, nil
}
