// Command api serves the HTTP and WebSocket surface for Identity, Messaging and
// Media. Bounded contexts are packages within this binary; the boundaries
// between them are enforced by the architecture test, not by the network.
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
	logger := logging.New("api")

	// Signal handling is written out in each binary rather than shared. It is
	// ten lines, it is the first thing a reader of this repository will look
	// at, and the three binaries will diverge as soon as one holds sockets,
	// one consumes Kafka and one owns a UDP port range.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	addr := config.EnvOr("API_ADDR", ":8080")
	logger.Info("starting", slog.String("addr", addr))

	// Phase 1 replaces this with the HTTP and WebSocket server.
	<-ctx.Done()

	logger.Info("shutdown complete")
}
