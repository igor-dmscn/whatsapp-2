// Command worker runs everything that happens after a request has been answered:
// the outbox relay, and the consumers that build read models from what it publishes.
//
// It holds no client connections and owns no HTTP surface, so it can be restarted at
// will. That is the point of splitting it out (ADR-0007) — and it is what makes the
// relay safe to stop: unpublished rows stay in the table and go when it returns.
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"comms/internal/messaging"
	"comms/internal/platform/config"
	"comms/internal/platform/database"
	"comms/internal/platform/kafka"
	"comms/internal/platform/logging"
	"comms/internal/platform/outbox"
)

func main() {
	logger := logging.New("worker")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, logger); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("fatal", slog.Any("error", err))
		os.Exit(1)
	}

	logger.Info("shutdown complete")
}

func run(ctx context.Context, logger *slog.Logger) error {
	db, err := database.Open(config.MustEnv("DATABASE_URL"))
	if err != nil {
		return err
	}
	defer db.Close()

	pingCtx, cancelPing := context.WithTimeout(ctx, 5*time.Second)
	defer cancelPing()
	if err := db.PingContext(pingCtx); err != nil {
		return err
	}

	brokers := kafka.Brokers(config.EnvOr("KAFKA_BROKERS", "localhost:9092"))

	// Created here rather than left to broker auto-creation, which would give a
	// single-partition topic with default retention and would silently turn a typo in
	// a topic name into a new topic.
	partitions := config.EnvIntOr("KAFKA_PARTITIONS", 3)
	if err := kafka.EnsureTopics(ctx, brokers, int32(partitions), logger); err != nil {
		return err
	}

	producer, err := kafka.NewProducer(ctx, brokers, logger)
	if err != nil {
		return err
	}
	defer producer.Close()

	projections, err := messaging.NewProjections(db, brokers, logger)
	if err != nil {
		return err
	}

	relay := outbox.NewRelay(db, producer, logger)

	// The relay and the projections are independent: the relay moves rows to Kafka
	// and the projections consume them back. Running both here is a deployment
	// convenience, not a coupling — either could be its own binary the moment one of
	// them needs to scale separately from the other.
	var running sync.WaitGroup
	running.Add(2)

	go func() {
		defer running.Done()
		relay.Run(ctx)
	}()
	go func() {
		defer running.Done()
		projections.Run(ctx)
	}()

	logger.Info("worker started", slog.Int("partitions", partitions))

	<-ctx.Done()
	logger.Info("draining")
	running.Wait()
	return nil
}
