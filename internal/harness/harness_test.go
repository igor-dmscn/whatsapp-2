// Package harness_test verifies the harness against a stub that forwards and nothing
// else.
//
// The point of the exercise: a harness that cannot be trusted proves nothing about the
// server it is pointed at, so it is checked against something whose behaviour is known
// before phase 9 gives it something whose behaviour is the question.
package harness_test

import (
	"context"
	"log/slog"
	"os"
	"runtime"
	"testing"
	"time"

	"comms/internal/harness"
)

// budget is how long a peer gets to connect and receive its first packet.
//
// Generous, because this runs on whatever a developer's machine happens to be doing. The
// number that matters is measured and reported rather than asserted at this margin —
// NF-3's two seconds is phase 9's assertion against a real server, not this one's against
// a stub.
const budget = 15 * time.Second

func quiet() *slog.Logger {
	if os.Getenv("HARNESS_DEBUG") != "" {
		return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return slog.New(slog.DiscardHandler)
}

// TestAPublishedTrackReachesAnotherPeer is the irreducible assertion: media crosses the
// server. Everything else in this package is a refinement of it.
func TestAPublishedTrackReachesAnotherPeer(t *testing.T) {
	t.Parallel()

	stub, server := harness.Serve(quiet())
	defer server.Close()
	defer stub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	publisher, err := harness.NewPeer(ctx, harness.NewHTTPSignaller(server.URL),
		harness.PeerOptions{Name: "publisher", Publish: true, Logger: quiet()})
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}
	defer func() { _ = publisher.Close(context.Background()) }()

	// Waited for, because the stub adds tracks at join time and does not renegotiate.
	// This is also the ordering that is easy to get wrong and easy to not notice: the
	// first version of the stub added tracks before setting the remote description, which
	// negotiated cleanly and delivered nothing.
	if err := stub.WaitForPublished(ctx, 2); err != nil {
		t.Fatalf("publisher's tracks never reached the stub: %v", err)
	}

	receiver, err := harness.NewPeer(ctx, harness.NewHTTPSignaller(server.URL),
		harness.PeerOptions{Name: "receiver", Logger: quiet()})
	if err != nil {
		t.Fatalf("receiver: %v", err)
	}
	defer func() { _ = receiver.Close(context.Background()) }()

	if err := receiver.WaitForMedia(ctx, "video", "audio"); err != nil {
		t.Fatalf("no media arrived: %v", err)
	}

	// Long enough for several frames and at least one scheduled keyframe.
	time.Sleep(2500 * time.Millisecond)

	video := receiver.Received("video")
	audio := receiver.Received("audio")
	t.Logf("receiver saw %s", video)
	t.Logf("receiver saw %s", audio)

	if video.Packets == 0 || audio.Packets == 0 {
		t.Fatalf("video %d packets, audio %d packets", video.Packets, audio.Packets)
	}
	// The assertion that the harness's own keyframe detection works, and that the stub
	// forwards the packets carrying them. Without this the source could be emitting
	// frames nothing can decode and every other test here would still pass.
	if video.Keyframes == 0 {
		t.Fatal("no keyframe was detected in the forwarded video")
	}
	// Sequence continuity: an unloaded loopback path should lose nothing at all.
	if video.Gaps > 0 {
		t.Fatalf("%d gaps on an unloaded path", video.Gaps)
	}
	if video.Duplicates > 0 {
		t.Fatalf("%d duplicate packets", video.Duplicates)
	}
}

// TestAPeerDoesNotReceiveItsOwnMedia: an SFU that echoes a publisher back to itself
// looks like it works from one browser and doubles everyone's bandwidth.
func TestAPeerDoesNotReceiveItsOwnMedia(t *testing.T) {
	t.Parallel()

	stub, server := harness.Serve(quiet())
	defer server.Close()
	defer stub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	alone, err := harness.NewPeer(ctx, harness.NewHTTPSignaller(server.URL),
		harness.PeerOptions{Name: "alone", Publish: true, Logger: quiet()})
	if err != nil {
		t.Fatalf("peer: %v", err)
	}
	defer func() { _ = alone.Close(context.Background()) }()

	time.Sleep(1500 * time.Millisecond)

	if received := alone.Received("video"); received.Packets > 0 {
		t.Fatalf("a lone publisher received %d of its own video packets", received.Packets)
	}
}

// TestAKeyframeArrivesOnDemand is the mechanism CL-6's recovery rests on, and the one
// phase 9's layer switch will use: a receiver asks, and a fresh keyframe appears.
func TestAKeyframeArrivesOnDemand(t *testing.T) {
	t.Parallel()

	stub, server := harness.Serve(quiet())
	defer server.Close()
	defer stub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	// A long keyframe interval, so a keyframe arriving during the window below is one
	// that was asked for rather than one that was due.
	publisher, err := harness.NewPeer(ctx, harness.NewHTTPSignaller(server.URL),
		harness.PeerOptions{
			Name: "publisher", Publish: true, Logger: quiet(),
			Source: harness.SourceOptions{KeyframeEvery: 100_000},
		})
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}
	defer func() { _ = publisher.Close(context.Background()) }()

	if err := stub.WaitForPublished(ctx, 2); err != nil {
		t.Fatalf("publisher's tracks never reached the stub: %v", err)
	}
	receiver, err := harness.NewPeer(ctx, harness.NewHTTPSignaller(server.URL),
		harness.PeerOptions{Name: "receiver", Logger: quiet()})
	if err != nil {
		t.Fatalf("receiver: %v", err)
	}
	defer func() { _ = receiver.Close(context.Background()) }()

	if err := receiver.WaitForMedia(ctx, "video"); err != nil {
		t.Fatalf("no media arrived: %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	before := receiver.Received("video").Keyframes
	if err := receiver.RequestKeyframe(); err != nil {
		t.Fatalf("request keyframe: %v", err)
	}

	// Waited for rather than slept through, so the test reports how long recovery took
	// instead of asserting against an arbitrary sleep.
	started := time.Now()
	deadline := time.Now().Add(5 * time.Second)
	for receiver.Received("video").Keyframes <= before {
		if time.Now().After(deadline) {
			t.Fatalf("no keyframe within %s of asking (had %d)", time.Since(started), before)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Logf("keyframe arrived %s after the request", time.Since(started).Round(time.Millisecond))
}

// TestThrottlingChangesWhatArrives is the lever phase 9 pulls to provoke a layer switch.
// If the publisher's bitrate cannot be changed mid-run and observed downstream, that
// assertion cannot be written at all.
func TestThrottlingChangesWhatArrives(t *testing.T) {
	t.Parallel()

	stub, server := harness.Serve(quiet())
	defer server.Close()
	defer stub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	publisher, err := harness.NewPeer(ctx, harness.NewHTTPSignaller(server.URL),
		harness.PeerOptions{
			Name: "publisher", Publish: true, Logger: quiet(),
			Source: harness.SourceOptions{Bitrate: 1_200_000, Framerate: 30},
		})
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}
	defer func() { _ = publisher.Close(context.Background()) }()

	if err := stub.WaitForPublished(ctx, 2); err != nil {
		t.Fatalf("publisher's tracks never reached the stub: %v", err)
	}
	receiver, err := harness.NewPeer(ctx, harness.NewHTTPSignaller(server.URL),
		harness.PeerOptions{Name: "receiver", Logger: quiet()})
	if err != nil {
		t.Fatalf("receiver: %v", err)
	}
	defer func() { _ = receiver.Close(context.Background()) }()

	if err := receiver.WaitForMedia(ctx, "video"); err != nil {
		t.Fatalf("no media arrived: %v", err)
	}

	// Two windows of the same length, throttled between them. Compared as packet counts
	// rather than as bitrates: a bitrate over a two-second window on a shared machine is
	// noisy, and the count of packets is what actually halved.
	time.Sleep(1500 * time.Millisecond)
	fast := receiver.Received("video").Packets

	publisher.Source().Throttle(150_000)
	time.Sleep(200 * time.Millisecond) // let the change take effect
	middle := receiver.Received("video").Packets
	time.Sleep(1500 * time.Millisecond)
	slow := receiver.Received("video").Packets - middle

	t.Logf("1.2 Mbit/s window: %d packets; 150 kbit/s window: %d packets", fast, slow)
	if slow >= fast {
		t.Fatalf("throttling to an eighth of the bitrate did not reduce what arrived: %d then %d",
			fast, slow)
	}

	// And a receiver estimate reaches the server without erroring, which is the other
	// half of the lever — phase 9 reacts to this by choosing a lower layer.
	if err := receiver.ReportBandwidth(150_000); err != nil {
		t.Fatalf("report bandwidth: %v", err)
	}
}

// TestARetransmissionRequestIsAccepted covers the message CL-6's recovery is made of.
// What it establishes here is that the harness can send it and the path survives; the
// recovery itself is phase 9's assertion, because a stub that does not buffer cannot
// retransmit.
func TestARetransmissionRequestIsAccepted(t *testing.T) {
	t.Parallel()

	stub, server := harness.Serve(quiet())
	defer server.Close()
	defer stub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	publisher, err := harness.NewPeer(ctx, harness.NewHTTPSignaller(server.URL),
		harness.PeerOptions{Name: "publisher", Publish: true, Logger: quiet()})
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}
	defer func() { _ = publisher.Close(context.Background()) }()

	if err := stub.WaitForPublished(ctx, 2); err != nil {
		t.Fatalf("publisher's tracks never reached the stub: %v", err)
	}
	receiver, err := harness.NewPeer(ctx, harness.NewHTTPSignaller(server.URL),
		harness.PeerOptions{Name: "receiver", Logger: quiet()})
	if err != nil {
		t.Fatalf("receiver: %v", err)
	}
	defer func() { _ = receiver.Close(context.Background()) }()

	if err := receiver.WaitForMedia(ctx, "video"); err != nil {
		t.Fatalf("no media arrived: %v", err)
	}

	if err := receiver.ReportLost([]uint16{1, 2, 3}); err != nil {
		t.Fatalf("report lost: %v", err)
	}

	// Media keeps flowing afterwards, which is the thing that would break if the
	// feedback were malformed.
	before := receiver.Received("video").Packets
	time.Sleep(500 * time.Millisecond)
	if receiver.Received("video").Packets <= before {
		t.Fatal("media stopped after a retransmission request")
	}
}

// TestTwentyPeersRunWithoutTheHarnessBecomingTheBottleneck is this phase's verification,
// and the reason it is a measurement rather than a pass or fail: a harness slower than
// the thing it tests proves nothing, so what it costs has to be a number somebody can
// read.
func TestTwentyPeersRunWithoutTheHarnessBecomingTheBottleneck(t *testing.T) {
	if testing.Short() {
		t.Skip("twenty peers is not a short test")
	}
	t.Parallel()

	const peers = 20

	stub, server := harness.Serve(quiet())
	defer server.Close()
	defer stub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// One publisher and nineteen receivers, rather than twenty publishers. That is the
	// shape that stresses forwarding — one track fanned out nineteen ways — and twenty
	// mutual publishers would be 380 forwarded tracks, which measures the machine.
	joined := make([]*harness.Peer, 0, peers)
	defer func() {
		for _, peer := range joined {
			_ = peer.Close(context.Background())
		}
	}()

	started := time.Now()
	publisher, err := harness.NewPeer(ctx, harness.NewHTTPSignaller(server.URL),
		harness.PeerOptions{Name: "publisher", Publish: true, Logger: quiet()})
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}
	joined = append(joined, publisher)

	if err := stub.WaitForPublished(ctx, 2); err != nil {
		t.Fatalf("publisher's tracks never reached the stub: %v", err)
	}

	for index := 1; index < peers; index++ {
		peer, err := harness.NewPeer(ctx, harness.NewHTTPSignaller(server.URL),
			harness.PeerOptions{Name: "receiver-" + itoa(index), Logger: quiet()})
		if err != nil {
			t.Fatalf("receiver %d: %v", index, err)
		}
		joined = append(joined, peer)
	}
	joinTime := time.Since(started)

	if stub.Participants() != peers {
		t.Fatalf("the stub holds %d participants, want %d", stub.Participants(), peers)
	}

	// Every receiver has to see media, not just some of them. A fan-out that works for
	// the first few and silently stops is the failure this is for.
	for _, peer := range joined[1:] {
		if err := peer.WaitForMedia(ctx, "video"); err != nil {
			t.Fatalf("fan-out incomplete: %v", err)
		}
	}
	time.Sleep(3 * time.Second)

	var (
		total   int
		slowest time.Duration
		worst   int
	)
	for _, peer := range joined[1:] {
		report := peer.Received("video")
		total += report.Packets
		if report.TimeToFirst > slowest {
			slowest = report.TimeToFirst
		}
		if report.Gaps > worst {
			worst = report.Gaps
		}
	}

	// Reported, not asserted, except where a number means the harness itself failed.
	t.Logf("%d peers on %d cores: joined in %s, slowest first packet %s, %d packets received, worst gap count %d",
		peers, runtime.NumCPU(), joinTime.Round(time.Millisecond),
		slowest.Round(time.Millisecond), total, worst)

	// What would make the harness the bottleneck: peers that connect but receive
	// nothing, or a fan-out that thins out as it goes. Both would be invisible in an
	// aggregate.
	for _, peer := range joined[1:] {
		if report := peer.Received("video"); report.Packets == 0 {
			t.Fatal("a peer connected and received nothing")
		}
	}
	// A publisher at 30 frames a second for three seconds is ~90 frames; nineteen
	// receivers should therefore see well over a thousand packets between them. A tenth
	// of that means the harness could not keep up, which is the thing being measured.
	if total < 1000 {
		t.Fatalf("only %d packets across 19 receivers in 3 seconds — the harness is the bottleneck", total)
	}
}

// TestLeavingReleasesTheParticipant: a load run that leaks participants measures
// something other than what it claims to, and CL-3 depends on the server seeing a leave.
func TestLeavingReleasesTheParticipant(t *testing.T) {
	t.Parallel()

	stub, server := harness.Serve(quiet())
	defer server.Close()
	defer stub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	peer, err := harness.NewPeer(ctx, harness.NewHTTPSignaller(server.URL),
		harness.PeerOptions{Name: "transient", Publish: true, Logger: quiet()})
	if err != nil {
		t.Fatalf("peer: %v", err)
	}
	if stub.Participants() != 1 {
		t.Fatalf("the stub holds %d participants after one join", stub.Participants())
	}

	if err := peer.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
	if stub.Participants() != 0 {
		t.Fatalf("the stub still holds %d participants after a leave", stub.Participants())
	}
}

func itoa(number int) string {
	if number == 0 {
		return "0"
	}
	var digits [8]byte
	index := len(digits)
	for number > 0 {
		index--
		digits[index] = byte('0' + number%10)
		number /= 10
	}
	return string(digits[index:])
}
