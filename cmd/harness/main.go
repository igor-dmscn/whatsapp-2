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
		url = flag.String("url", "", "signalling base URL; empty starts a local stub")
		// node points at cmd/sfu rather than at the stub, and is what NF-14 is measured
		// against: a capacity claim about a deployed process cannot be answered by driving
		// something else. The two flags are separate rather than one flag with a mode,
		// because they speak different protocols and confusing them produces a 404 rather
		// than a wrong number.
		node = flag.String("node", "",
			"media node base URL, e.g. http://localhost:8090; takes precedence over -url")
		calls = flag.Int("calls", 1,
			"how many concurrent calls to spread the peers across; requires -node")
		peers      = flag.Int("peers", 4, "how many participants to simulate, per call")
		publishers = flag.Int("publishers", 1, "how many of them publish, per call")
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
		node:       *node,
		calls:      *calls,
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
	node       string
	calls      int
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

	if options.calls < 1 {
		options.calls = 1
	}
	if options.calls > 1 && options.node == "" {
		return errors.New("several calls need -node: the stub has no notion of a call")
	}

	url := options.url
	if options.node == "" && url == "" {
		// A local stub, so the harness is runnable — and measurable — with nothing else
		// deployed. This is what establishes the floor: whatever it costs here is what the
		// harness costs, and anything worse against a real server belongs to the server.
		stub, server := harness.Serve(logger)
		defer server.Close()
		defer stub.Close()
		url = server.URL
		logger.Info("started a local stub", slog.String("url", url))
	}

	// Publishers first, and a pause before the receivers: the stub does not renegotiate, so
	// a receiver that joins before a publisher's tracks exist sees nothing and the
	// measurement would be of an empty call. The real node does renegotiate and does not
	// need this, but it costs half a second and keeps one code path.
	//
	// A sleep rather than a wait on the server's state, because this points at a remote
	// server whose internals it cannot see. The tests, which own the stub, wait properly.
	peers := make([]*harness.Peer, 0, options.peers*options.calls)
	defer func() {
		closing, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, peer := range peers {
			_ = peer.Close(closing)
		}
	}()

	started := time.Now()
	for call := range options.calls {
		// One identifier per call, stable and readable, so a node's logs can be matched to a
		// run. The node creates a call on first join and forgets it when the last
		// participant leaves, so nothing has to exist beforehand.
		callID := fmt.Sprintf("capacity-%d", call)

		for index := range options.peers {
			publishing := index < options.publishers
			name := naming(publishing, index)
			if options.calls > 1 {
				name = fmt.Sprintf("call-%d-%s", call, name)
			}

			peer, err := harness.NewPeer(ctx, signaller(options, url, callID, name, logger),
				harness.PeerOptions{
					Name:    name,
					Publish: publishing,
					Logger:  logger,
					Source: harness.SourceOptions{
						Bitrate: options.bitrate, Framerate: options.framerate,
					},
				})
			if err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
			peers = append(peers, peer)

			if index == options.publishers-1 {
				time.Sleep(publishSettle)
			}
		}
		logger.Info("call joined",
			slog.String("call", callID),
			slog.Int("peers", options.peers),
			slog.Duration("elapsed", time.Since(started).Round(time.Millisecond)))
	}
	joined := time.Since(started)

	logger.Info("all peers joined",
		slog.Int("calls", options.calls),
		slog.Int("peers", len(peers)),
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

// signaller picks how this peer reaches the server: a real media node, or the stub.
func signaller(
	options options,
	url, callID, participantID string,
	logger *slog.Logger,
) harness.Signaller {
	if options.node != "" {
		return harness.NewNodeSignaller(options.node, callID, participantID, logger)
	}
	return harness.NewHTTPSignaller(url)
}

func naming(publishing bool, index int) string {
	if publishing {
		return "publisher-" + strconv.Itoa(index)
	}
	return "receiver-" + strconv.Itoa(index)
}
