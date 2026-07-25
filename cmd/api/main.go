// Command api serves the HTTP and WebSocket surface for Identity, Messaging and
// Media. Bounded contexts are packages within this binary; the boundaries
// between them are enforced by the architecture test, not by the network.
//
// This is also the only place allowed to know about more than one context. All
// wiring happens here, which is what lets contexts depend on ports rather than
// on each other.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"comms/internal/identity"
	"comms/internal/messaging"
	"comms/internal/platform/config"
	"comms/internal/platform/database"
	"comms/internal/platform/httpx"
	"comms/internal/platform/logging"
)

// shutdownGrace is how long in-flight requests get to finish. Long enough for a
// normal request, short enough that a deploy is not held up by a stuck one.
const shutdownGrace = 15 * time.Second

// allowedOrigins is which origins may open a WebSocket.
//
// Same-origin by default. A permissive policy here would let any page a user visits
// open an authenticated socket on their behalf, so the development origin is opt-in
// through configuration rather than a built-in exception.
func allowedOrigins() []string {
	if configured := config.EnvOr("ALLOWED_ORIGINS", ""); configured != "" {
		return strings.Split(configured, ",")
	}
	return nil
}

func main() {
	logger := logging.New("api")

	// Signal handling is written out in each binary rather than shared. It is
	// ten lines, it is the first thing a reader of this repository will look
	// at, and the three binaries will diverge as soon as one holds sockets,
	// one consumes Kafka and one owns a UDP port range.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, logger); err != nil {
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

	// Fail at boot rather than on the first request: a node that cannot reach
	// its database should never enter a load balancer's rotation.
	pingCtx, cancelPing := context.WithTimeout(ctx, 5*time.Second)
	defer cancelPing()
	if err := db.PingContext(pingCtx); err != nil {
		return err
	}

	// Long-lived sockets and short database calls have different needs. Sizing
	// is revisited in phase 10 against a real load test; these are placeholders
	// chosen to be obviously finite rather than obviously correct.
	db.SetMaxOpenConns(config.EnvIntOr("DATABASE_MAX_CONNS", 25))
	db.SetMaxIdleConns(config.EnvIntOr("DATABASE_IDLE_CONNS", 5))
	db.SetConnMaxLifetime(time.Hour)

	redisClient, err := messaging.OpenRedis(ctx, config.EnvOr("REDIS_URL", "redis://localhost:6379"))
	if err != nil {
		return err
	}
	defer redisClient.Close()

	// Each context wires itself. cmd/api cannot see the packages being composed —
	// they are behind a nested internal/ fence — so composition necessarily happens
	// inside each context, and this file is left doing only what it should: choosing
	// which contexts exist and how they are joined.
	identityModule := identity.New(db, logger)

	messagingModule := messaging.New(db, redisClient, identityModule, identityModule.Caller,
		messaging.Options{AllowedOrigins: allowedOrigins()}, logger)

	// One goroutine per process reads broadcasts for every socket this node holds.
	go messagingModule.Run(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteJSON(w, logger, http.StatusOK, map[string]any{
			"status":      "ok",
			"connections": messagingModule.ConnectionCount(),
		})
	})
	identityModule.Routes(mux)
	messagingModule.Routes(mux, identityModule.Authenticated)

	server := &http.Server{
		Addr:              config.EnvOr("API_ADDR", ":8080"),
		Handler:           httpx.Chain(mux, httpx.Correlate, httpx.LogRequests(logger)),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}

	// No WriteTimeout: it would cap the lifetime of the WebSocket connections
	// phase 2 introduces, since an upgraded connection is one long write.
	// Per-message deadlines guard those instead.

	serverFailed := make(chan error, 1)
	go func() {
		logger.Info("listening", slog.String("addr", server.Addr))
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverFailed <- err
		}
	}()

	select {
	case err := <-serverFailed:
		return err
	case <-ctx.Done():
	}

	logger.Info("draining", slog.Duration("grace", shutdownGrace))
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancelShutdown()

	if err := server.Shutdown(shutdownCtx); err != nil {
		return err
	}
	return nil
}
