// Command sfu forwards call media between participants. It is deployed
// separately from api because it is CPU bound, latency critical, and needs a raw
// UDP port range that cannot be put behind an HTTP load balancer.
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
	logger := logging.New("sfu")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.Info("starting",
		slog.Int("udp_port_min", config.EnvIntOr("SFU_UDP_PORT_MIN", 50000)),
		slog.Int("udp_port_max", config.EnvIntOr("SFU_UDP_PORT_MAX", 50100)),
	)

	// Phase 9 replaces this with the media forwarder. Phase 8 builds the
	// harness that will test it, deliberately first.
	<-ctx.Done()

	logger.Info("shutdown complete")
}
