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

	"comms/internal/calling"
	"comms/internal/identity"
	"comms/internal/media"
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

// storeConfig is where attachment bytes live.
//
// One endpoint and one static key: MinIO in development, and the same code against S3
// in a deployment that sets these differently. No credential provider chain, because
// there is one credential.
func storeConfig() media.Config {
	return media.Config{
		Endpoint:  config.EnvOr("S3_ENDPOINT", "http://localhost:9000"),
		Bucket:    config.EnvOr("S3_BUCKET", "comms-attachments"),
		Region:    config.EnvOr("S3_REGION", "us-east-1"),
		AccessKey: config.MustEnv("S3_ACCESS_KEY"),
		SecretKey: config.MustEnv("S3_SECRET_KEY"),
	}
}

// callFrames joins Messaging's socket to Calling's signalling.
//
// Eight lines of adapter because the two contexts declare the same Session shape under
// different names, and Go compares method signatures by name rather than by structure. That
// is the price of neither context importing the other, and this file is where such joins
// belong.
type callFrames struct {
	calling *calling.Module
}

func (c callFrames) HandleFrame(
	ctx context.Context,
	session messaging.Session,
	frameType string,
	raw []byte,
) error {
	return c.calling.HandleFrame(ctx, session, frameType, raw)
}

func (c callFrames) SocketClosed(ctx context.Context, session messaging.Session) {
	c.calling.SocketClosed(ctx, session)
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

	attachments, err := media.OpenStore(ctx, storeConfig())
	if err != nil {
		return err
	}

	// Media asks Messaging who may attach and who may view: an attachment is exactly as
	// visible as the entry referencing it, and that rule belongs to the context that
	// owns membership. No notifier here — this process signs URLs and serves metadata,
	// and has no readiness to announce.
	mediaModule := media.New(db, attachments, messagingModule, nil, identityModule.Caller, logger)

	// Calling asks Messaging one question — may this account join — and reaches clients
	// through it. Where media is forwarded is the one other choice, and it is made here.
	callingModule, err := calling.New(db, messagingModule, messagingModule, calling.Options{
		// The forwarding node, shared by every api node when it is set. Unset means media
		// is forwarded in this process, which is what a single-process development machine
		// wants — and which means a call belongs to the node that started it.
		MediaNodeURL: config.EnvOr("SFU_URL", ""),

		// This process's media address, used only when forwarding is in it, and it must be
		// distinct per process. Defaulting it to a shared literal is what made CL-4 fail
		// silently: two api nodes both called themselves the same thing, so the check that
		// a call belongs to *this* node passed on both, and a call started on one was
		// quietly continued on the other's media plane. Two participants, two SFUs, no
		// shared media, no error.
		Address:    config.EnvOr("SFU_ADDRESS", "local"+config.EnvOr("API_ADDR", ":8080")),
		UDPPortMin: uint16(config.EnvIntOr("SFU_UDP_PORT_MIN", 0)),
		UDPPortMax: uint16(config.EnvIntOr("SFU_UDP_PORT_MAX", 0)),
		PublicIP:   config.EnvOr("SFU_PUBLIC_IP", ""),
		Logger:     logger,
	})
	if err != nil {
		return err
	}
	defer callingModule.Close()

	// One subscription per process for the offers a remote media node produces, exactly as
	// Messaging runs one for broadcasts. Returns immediately when forwarding is in process.
	go callingModule.Run(ctx)

	// Call signalling rides the socket the client already has (ADR-0004). Registered here
	// because this is the only file allowed to know both contexts exist.
	messagingModule.RegisterFrames(callingModule.Frames(), callFrames{callingModule})

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteJSON(w, logger, http.StatusOK, map[string]any{
			"status":      "ok",
			"connections": messagingModule.ConnectionCount(),
			"calls":       callingModule.Calls(),
		})
	})
	identityModule.Routes(mux)
	messagingModule.Routes(mux, identityModule.Authenticated)
	mediaModule.Routes(mux, identityModule.Authenticated)

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
