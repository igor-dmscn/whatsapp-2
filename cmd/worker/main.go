// Command worker consumes domain events from Kafka to build read models and to
// process attachments into variants. It holds no client connections, so it can
// be restarted at will.
package main

import (
	"context"
	"log/slog"
	"os/signal"
	"syscall"

	"comms/internal/platform/config"
	"comms/internal/platform/logging"
)

func main() {
	logger := logging.New("worker")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	brokers := config.EnvOr("KAFKA_BROKERS", "localhost:9092")
	logger.Info("starting", slog.String("brokers", brokers))

	// Phase 3 replaces this with the projection consumers, phase 7 with media
	// processing.
	<-ctx.Done()

	logger.Info("shutdown complete")
}
