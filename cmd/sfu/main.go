// Command sfu forwards call media between participants. It is deployed
// separately from api because it is CPU bound, latency critical, and needs a raw
// UDP port range that cannot be put behind an HTTP load balancer.
//
// Two ports, and the difference between them is the whole design. Signalling arrives on
// SFU_ADDR over HTTP from api nodes — a few kilobytes of SDP per participant. Media arrives
// on the UDP range and never touches an HTTP handler.
//
// Stateless in the sense that matters: this process knows which transports it holds and
// nothing about who may hold one. A call exists because an api node recorded it, and if this
// process is restarted the calls on it are gone and their clients rejoin.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"comms/internal/calling"
	"comms/internal/platform/config"
	"comms/internal/platform/httpx"
	"comms/internal/platform/logging"
)

// shutdownGrace is how long signalling requests get to finish.
//
// Short, and shorter than api's, because nothing here is worth waiting for: a call whose
// node is going away ends, and the useful thing is that its clients find out quickly rather
// than that an in-flight join completes onto a process that is about to stop forwarding.
const shutdownGrace = 5 * time.Second

func main() {
	logger := logging.New("sfu")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, logger); err != nil {
		logger.Error("fatal", slog.Any("error", err))
		os.Exit(1)
	}

	logger.Info("shutdown complete")
}

func run(ctx context.Context, logger *slog.Logger) error {
	// A real range by default, unlike api's zero. A node that lets the operating system
	// choose is undeployable — nobody can open a firewall for "whatever it picks" — and the
	// default being finite is what makes that visible in development too.
	node, err := calling.NewMediaNode(calling.Options{
		UDPPortMin: uint16(config.EnvIntOr("SFU_UDP_PORT_MIN", 50000)),
		UDPPortMax: uint16(config.EnvIntOr("SFU_UDP_PORT_MAX", 50100)),
		PublicIP:   config.EnvOr("SFU_PUBLIC_IP", ""),
		Logger:     logger,
	})
	if err != nil {
		return err
	}
	defer node.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteJSON(w, logger, http.StatusOK, map[string]any{
			"status": "ok",
			"calls":  node.Calls(),
			// Zero here with calls above zero is the one failure this process can have
			// that looks like health: it forwards media and cannot tell anybody about a
			// new publisher, so every call is stuck at whoever negotiated first.
			"offer_subscribers": node.OfferSubscribers(),
		})
	})
	node.Routes(mux)

	server := &http.Server{
		Addr:              config.EnvOr("SFU_ADDR", ":8090"),
		Handler:           httpx.Chain(mux, httpx.Correlate),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}

	// No request logging middleware and no WriteTimeout, for the same reason: one of these
	// endpoints is a stream that lasts as long as an api node does. Logging it once at the
	// start says everything a log line could, and a write deadline would end it on a
	// schedule.

	failed := make(chan error, 1)
	go func() {
		logger.Info("listening",
			slog.String("addr", server.Addr),
			slog.Int("udp_port_min", config.EnvIntOr("SFU_UDP_PORT_MIN", 50000)),
			slog.Int("udp_port_max", config.EnvIntOr("SFU_UDP_PORT_MAX", 50100)))
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			failed <- err
		}
	}()

	select {
	case err := <-failed:
		return err
	case <-ctx.Done():
	}

	logger.Info("draining", slog.Duration("grace", shutdownGrace))
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		return err
	}
	return nil
}
