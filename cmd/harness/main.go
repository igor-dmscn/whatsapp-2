// Command harness drives a media server with as many participants as asked for, and
// reports what each one received.
//
// A separate binary from the test suite because the two answer different questions. The
// tests establish that forwarding is correct at two or twenty peers and fail if it is
// not. This runs against whatever is listening — the stub, or the real server from phase
// 9 — for however long is asked, and prints numbers. A load test that can only pass or
// fail cannot tell you the shape of a degradation.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"comms/internal/harness"
	"comms/internal/platform/logging"
)

func main() {
	var (
		url        = flag.String("url", "", "signalling base URL; empty starts a local stub")
		peers      = flag.Int("peers", 4, "how many participants to simulate")
		publishers = flag.Int("publishers", 1, "how many of them publish")
		duration   = flag.Duration("for", 10*time.Second, "how long to run")
		bitrate    = flag.Int("bitrate", 600_000, "each publisher's target bits per second")
		framerate  = flag.Int("framerate", 30, "each publisher's frames per second")
		throttleAt = flag.Duration("throttle-after", 0,
			"throttle every publisher to a tenth of its bitrate after this long, zero to never")
	)
	flag.Parse()

	logger := logging.New("harness")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, logger, options{
		url:        *url,
		peers:      *peers,
		publishers: *publishers,
		duration:   *duration,
		bitrate:    *bitrate,
		framerate:  *framerate,
		throttleAt: *throttleAt,
	}); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("fatal", slog.Any("error", err))
		os.Exit(1)
	}
}

// publishSettle is how long publishers get to have their tracks noticed before
// receivers join. Several frame intervals at any sensible framerate.
const publishSettle = 500 * time.Millisecond

type options struct {
	url        string
	peers      int
	publishers int
	duration   time.Duration
	bitrate    int
	framerate  int
	throttleAt time.Duration
}

func run(ctx context.Context, logger *slog.Logger, options options) error {
	if options.peers < 1 {
		return errors.New("at least one peer is needed")
	}
	if options.publishers > options.peers {
		options.publishers = options.peers
	}

	url := options.url
	if url == "" {
		// A local stub, so the harness is runnable — and measurable — with nothing else
		// deployed. This is what establishes the floor: whatever it costs here is what the
		// harness costs, and anything worse against a real server belongs to the server.
		stub, server := harness.Serve(logger)
		defer server.Close()
		defer stub.Close()
		url = server.URL
		logger.Info("started a local stub", slog.String("url", url))
	}

	// Publishers first, and a pause before the receivers: neither the stub nor a
	// first-cut SFU renegotiates, so a receiver that joins before a publisher's tracks
	// exist sees nothing and the measurement would be of an empty call.
	//
	// A sleep rather than a wait on the server's state, because this points at a remote
	// server whose internals it cannot see. The tests, which own the stub, wait properly.
	peers := make([]*harness.Peer, 0, options.peers)
	defer func() {
		closing, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, peer := range peers {
			_ = peer.Close(closing)
		}
	}()

	started := time.Now()
	for index := range options.peers {
		publishing := index < options.publishers
		peer, err := harness.NewPeer(ctx, harness.NewHTTPSignaller(url), harness.PeerOptions{
			Name:    naming(publishing, index),
			Publish: publishing,
			Logger:  logger,
			Source: harness.SourceOptions{
				Bitrate: options.bitrate, Framerate: options.framerate,
			},
		})
		if err != nil {
			return fmt.Errorf("peer %d: %w", index, err)
		}
		peers = append(peers, peer)

		if index == options.publishers-1 {
			logger.Info("publishers joined",
				slog.Int("count", options.publishers),
				slog.Duration("took", time.Since(started)))
			time.Sleep(publishSettle)
		}
	}
	joined := time.Since(started)

	logger.Info("all peers joined",
		slog.Int("peers", options.peers),
		slog.Duration("took", joined),
		slog.Duration("for", options.duration))

	if options.throttleAt > 0 {
		go throttle(ctx, logger, peers[:options.publishers], options)
	}

	select {
	case <-ctx.Done():
		logger.Info("interrupted, reporting what there is")
	case <-time.After(options.duration):
	}

	report(logger, peers, joined)
	return nil
}

// throttle drops every publisher's bitrate mid-run.
//
// The lever phase 9's layer-switch assertion pulls, exposed here so it can be pulled by
// hand against a running server rather than only from a test.
func throttle(ctx context.Context, logger *slog.Logger, publishers []*harness.Peer, options options) {
	select {
	case <-ctx.Done():
		return
	case <-time.After(options.throttleAt):
	}

	target := options.bitrate / 10
	for _, peer := range publishers {
		peer.Source().Throttle(target)
		// The receiver-side estimate too, because a server reacts to what it is told as
		// well as to what it measures.
		if err := peer.ReportBandwidth(target); err != nil {
			logger.Debug("report bandwidth", slog.Any("error", err))
		}
	}
	logger.Info("throttled every publisher", slog.Int("to_bits_per_second", target))
}

// report prints one line per peer and one summary.
//
// Per peer as well as in aggregate, deliberately: a fan-out that works for the first few
// receivers and thins out afterwards averages to something that looks fine.
func report(logger *slog.Logger, peers []*harness.Peer, joined time.Duration) {
	var (
		total     int
		receiving int
		silent    int
		slowest   time.Duration
		gaps      int
	)

	for _, peer := range peers {
		for kind, arrived := range peer.Reports() {
			logger.Info("received", slog.String("track", kind), slog.String("report", arrived.String()))
		}

		video := peer.Received("video")
		total += video.Packets
		gaps += video.Gaps
		if video.Packets > 0 {
			receiving++
			if video.TimeToFirst > slowest {
				slowest = video.TimeToFirst
			}
		} else {
			silent++
		}
	}

	logger.Info("summary",
		slog.Int("peers", len(peers)),
		slog.Duration("join_time", joined.Round(time.Millisecond)),
		slog.Int("receiving_video", receiving),
		slog.Int("silent", silent),
		slog.Duration("slowest_first_packet", slowest.Round(time.Millisecond)),
		slog.Int("video_packets", total),
		slog.Int("gaps", gaps),
	)
}

func naming(publishing bool, index int) string {
	if publishing {
		return "publisher-" + strconv.Itoa(index)
	}
	return "receiver-" + strconv.Itoa(index)
}
