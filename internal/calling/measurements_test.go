// The non-functional claims phase 9 makes about calls, measured rather than asserted.
//
// Separate from calling_test.go because these are a different kind of test. Those establish
// that the thing works and fail when it does not; these produce numbers, print them, and fail
// only if a number is worse than a requirement. A measurement that can only pass or fail
// cannot tell you the shape of a degradation, so both log what they saw either way.
//
// Through the whole stack — the frame handler, the use cases, Postgres, the media plane —
// because that is what a requirement about a *call* means. Measuring the media plane alone
// would leave out signalling, which is a real part of the time a person waits.
//
// What these are not: a benchmark of a deployment. They run against loopback on a developer's
// machine, so they establish that the code is not the reason a limit is missed. NF-14, the
// capacity claim, cannot be honestly answered this way at all and is a separate script — see
// scripts/capacity.sh.
package calling_test

import (
	"context"
	"testing"
	"time"

	"comms/internal/harness"
)

// TestJoinToFirstMediaMeetsNF3 measures the wait a person actually experiences: pressing join
// until something appears.
//
// Timed from the joiner's own point of view — the harness records first arrival against the
// moment the peer began negotiating — because a server-side measurement would leave out the
// two things most likely to be slow, which are ICE gathering and the signalling round trip.
//
// Repeated, because the requirement is a p95 and one sample cannot be one. Each iteration is a
// whole call: somebody publishing, somebody joining, both leaving. The call ends when the
// second of them leaves (CL-3), so the next iteration starts a new one in the same
// conversation, which is also a small check that nothing leaks between calls.
func TestJoinToFirstMediaMeetsNF3(t *testing.T) {
	t.Parallel()
	held := newHarnessed(t, permissive{})

	ctx, cancel := context.WithTimeout(context.Background(), 3*budget)
	defer cancel()

	samples := make([]time.Duration, 0, nf3Samples)
	for iteration := range nf3Samples {
		publisher := held.join(t, ctx, "publisher")
		// The publisher's tracks have to have reached the server before the joiner arrives,
		// or this measures how long until somebody starts sending rather than how long
		// until what they send is forwarded. A joiner that arrives early is not *wrong* —
		// the server re-offers, and phase 9 has tests for exactly that — it would just be
		// timing the wrong thing.
		//
		// A settle rather than a wait on a condition, because whether the server has a
		// publisher's tracks is not observable from out here: the module reports how many
		// calls it forwards and deliberately not what is inside one. It sits outside the
		// measurement, which starts when the joiner begins negotiating.
		time.Sleep(publishSettle)

		joiner := held.join(t, ctx, "joiner")
		if err := joiner.peer.WaitForWatchableVideo(ctx); err != nil {
			t.Fatalf("iteration %d: the joiner saw nothing watchable: %v", iteration, err)
		}

		// Time to the first *keyframe*, not the first packet. Video packets arriving are
		// not a picture: a decoder joining mid-stream has no reference frame and shows
		// nothing until a keyframe, so the first packet measures the transport and
		// flatters the requirement by however long the publisher's next scheduled keyframe
		// happened to be away — up to two seconds, which is the whole budget.
		//
		// What makes this number small is the server asking the publisher for a keyframe as
		// soon as somebody is there to receive one. Measuring the first packet instead
		// cannot see that behaviour at all, so removing it would not have failed anything.
		received := joiner.peer.Received("video")
		if received.TimeToFirstKeyframe == 0 {
			t.Fatalf("iteration %d: video arrived with no keyframe in it (%s)", iteration, received)
		}
		samples = append(samples, received.TimeToFirstKeyframe)

		if err := joiner.peer.Close(ctx); err != nil {
			t.Fatalf("iteration %d: joiner leaving: %v", iteration, err)
		}
		if err := publisher.peer.Close(ctx); err != nil {
			t.Fatalf("iteration %d: publisher leaving: %v", iteration, err)
		}
	}

	p50 := harness.Percentile(samples, 50)
	p95 := harness.Percentile(samples, 95)
	t.Logf("join to first watchable video over %d calls: p50 %s, p95 %s, worst %s",
		len(samples), p50.Round(time.Millisecond), p95.Round(time.Millisecond),
		harness.Percentile(samples, 100).Round(time.Millisecond))

	if p95 > nf3Limit {
		t.Fatalf("NF-3: join to first media p95 is %s, want under %s", p95, nf3Limit)
	}
}

const (
	// nf3Samples is how many calls to time. Twenty is enough for a p95 to mean something
	// and few enough that the whole measurement is seconds rather than minutes.
	nf3Samples = 20
	// nf3Limit is the requirement.
	nf3Limit = 2 * time.Second
	// publishSettle is how long the publisher gets before the joiner arrives. Several frame
	// intervals at any sensible framerate, and the same value cmd/harness uses.
	publishSettle = 500 * time.Millisecond
)

// TestOneWayAudioLatencyMeetsNF4 measures send to receive through the forwarding server.
//
// The stamp is in the audio payload, which is the one part of a packet a selective forwarder
// is forbidden to touch — see audioFrame in the harness. Measuring from the RTP header instead
// would be measuring against something the server is allowed to rewrite.
//
// Audio rather than video, which is what the requirement says and is also the right choice:
// video frames span several packets and "when did the frame arrive" needs a definition, while
// an audio packet is one packet and its arrival is unambiguous.
func TestOneWayAudioLatencyMeetsNF4(t *testing.T) {
	t.Parallel()
	held := newHarnessed(t, permissive{})

	ctx, cancel := context.WithTimeout(context.Background(), 2*budget)
	defer cancel()

	alice := held.join(t, ctx, "alice")
	bob := held.join(t, ctx, "bob")

	if err := bob.peer.WaitForMedia(ctx, "audio"); err != nil {
		t.Fatalf("bob received no audio: %v", err)
	}
	if err := alice.peer.WaitForMedia(ctx, "audio"); err != nil {
		t.Fatalf("alice received no audio: %v", err)
	}

	// Long enough for a few hundred packets at 20 ms each, so a p95 is not four samples.
	// Nothing to wait *for* here — the measurement is of a steady state — so this is a
	// sleep rather than a condition.
	time.Sleep(nf4Window)

	for name, peer := range map[string]*harness.Peer{"alice": alice.peer, "bob": bob.peer} {
		received := peer.Received("audio")
		if len(received.Latencies) < nf4MinimumSamples {
			t.Fatalf("%s has %d latency samples, too few to say anything (%s)",
				name, len(received.Latencies), received)
		}

		p50, p95 := received.LatencyAt(50), received.LatencyAt(95)
		t.Logf("%s: one-way audio latency p50 %s, p95 %s, worst %s, over %d packets",
			name, p50.Round(time.Microsecond), p95.Round(time.Microsecond),
			received.LatencyAt(100).Round(time.Microsecond), len(received.Latencies))

		if p95 > nf4Limit {
			t.Fatalf("NF-4: %s sees one-way audio latency p95 of %s, want under %s",
				name, p95, nf4Limit)
		}
	}
}

const (
	// nf4Window is how long to let audio flow before reading the distribution.
	nf4Window = 3 * time.Second
	// nf4MinimumSamples guards against a passing measurement that measured almost nothing.
	// Three seconds at 20 ms per packet is 150; a third of that is a generous floor and
	// still enough that a p95 is not one sample wearing a hat.
	nf4MinimumSamples = 50
	// nf4Limit is the requirement.
	nf4Limit = 200 * time.Millisecond
)
