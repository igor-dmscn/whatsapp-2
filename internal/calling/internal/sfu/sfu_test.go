// Package sfu_test drives the real media plane with the phase-8 harness.
//
// This is what building the harness first was for. The peers here are the same ones that
// were verified against a stub, so a failure is the server's — and the assertions are the
// ones a receiver could make, because that is all a receiver can see.
package sfu_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"

	"comms/internal/calling/internal/sfu"
	"comms/internal/harness"
)

const budget = 20 * time.Second

func quiet() *slog.Logger {
	if os.Getenv("SFU_DEBUG") != "" {
		return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return slog.New(slog.DiscardHandler)
}

// direct signals to a server in this process.
//
// No transport at all, deliberately: what is under test is forwarding and renegotiation,
// and putting HTTP or a WebSocket in the middle would test those instead. Phase 9's
// signalling layer is what carries these same four calls over the socket that already
// exists.
type direct struct {
	server        *sfu.Server
	callID        string
	participantID string

	// offers, and the lock that makes closing it safe.
	//
	// A sync.Once was here first, and it was not enough: it stops a second close but not a
	// send racing the first one, which is the actual hazard — a participant leaves while a
	// renegotiation is already in flight for them. The race detector found it once the
	// server gained a retry, having tolerated it for a phase.
	mutex  sync.Mutex
	closed bool
	offers chan string
}

func newDirect(server *sfu.Server, callID, participantID string) *direct {
	return &direct{
		server: server, callID: callID, participantID: participantID,
		offers: make(chan string, 4),
	}
}

func (d *direct) Join(_ context.Context, offer webrtc.SessionDescription) (webrtc.SessionDescription, error) {
	answer, err := d.server.Join(d.callID, d.participantID, offer.SDP)
	if err != nil {
		return webrtc.SessionDescription{}, err
	}
	return webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: answer}, nil
}

func (d *direct) Leave(context.Context) error {
	d.server.Leave(d.callID, d.participantID)

	d.mutex.Lock()
	defer d.mutex.Unlock()
	if !d.closed {
		d.closed = true
		close(d.offers)
	}
	return nil
}

func (d *direct) Offers() <-chan string { return d.offers }

func (d *direct) Answer(_ context.Context, answer string) error {
	return d.server.Answer(d.callID, d.participantID, answer)
}

// offer is how the server's renegotiator reaches this signaller.
func (d *direct) offer(sdp string) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	if d.closed {
		return
	}

	select {
	case d.offers <- sdp:
	default:
		// A participant four offers behind is not going to catch up, and blocking here
		// would stop the server forwarding for everyone else.
	}
}

// room is a server plus the signallers of everyone in it.
type room struct {
	server *sfu.Server

	mutex      sync.Mutex
	signallers map[string]*direct
}

func newRoom(t *testing.T) *room {
	t.Helper()

	server, err := sfu.New(sfu.Options{Logger: quiet()})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	held := &room{server: server, signallers: make(map[string]*direct)}
	server.SetRenegotiator(func(_ context.Context, _, participantID, offer string) error {
		held.mutex.Lock()
		signaller := held.signallers[participantID]
		held.mutex.Unlock()
		if signaller == nil {
			return errors.New("nobody is signalling for that participant")
		}
		signaller.offer(offer)
		return nil
	})

	t.Cleanup(server.Close)
	return held
}

// join adds a peer to a call.
func (r *room) join(t *testing.T, ctx context.Context, callID, name string, publish bool) *harness.Peer {
	t.Helper()

	signaller := newDirect(r.server, callID, name)
	r.mutex.Lock()
	r.signallers[name] = signaller
	r.mutex.Unlock()

	peer, err := harness.NewPeer(ctx, signaller, harness.PeerOptions{
		Name: name, Publish: publish, Logger: quiet(),
	})
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	t.Cleanup(func() { _ = peer.Close(context.Background()) })
	return peer
}

// TestBothSidesOfATwoWayCallReceive is the assertion renegotiation exists for. The second
// to join gets the first's tracks in its answer; the first learns of the second's only
// because the server offers the other way.
func TestBothSidesOfATwoWayCallReceive(t *testing.T) {
	t.Parallel()
	room := newRoom(t)

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	alice := room.join(t, ctx, "call-1", "alice", true)
	bob := room.join(t, ctx, "call-1", "bob", true)

	if err := bob.WaitForMedia(ctx, "video", "audio"); err != nil {
		t.Fatalf("bob received nothing: %v", err)
	}
	// The direction that only works if the server re-offered.
	if err := alice.WaitForMedia(ctx, "video", "audio"); err != nil {
		t.Fatalf("alice received nothing — the server did not renegotiate: %v", err)
	}

	time.Sleep(2 * time.Second)

	for name, peer := range map[string]*harness.Peer{"alice": alice, "bob": bob} {
		video := peer.Received("video")
		t.Logf("%s saw %s", name, video)
		if video.Keyframes == 0 {
			t.Fatalf("%s never saw a keyframe", name)
		}
		if video.Gaps > 0 {
			t.Fatalf("%s saw %d gaps on a loopback path", name, video.Gaps)
		}
	}
}

// TestAThirdParticipantSeesAndIsSeenByBothOthers: a call is not two calls, and the
// fan-out has to work in every direction at once.
func TestAThirdParticipantSeesAndIsSeenByBothOthers(t *testing.T) {
	t.Parallel()
	room := newRoom(t)

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	alice := room.join(t, ctx, "call-2", "alice", true)
	bob := room.join(t, ctx, "call-2", "bob", true)
	carol := room.join(t, ctx, "call-2", "carol", true)

	for name, peer := range map[string]*harness.Peer{"alice": alice, "bob": bob, "carol": carol} {
		if err := peer.WaitForMedia(ctx, "video"); err != nil {
			t.Fatalf("%s received nothing: %v", name, err)
		}
	}
	// Each of them must end up receiving two tracks of video, not one: a server that
	// renegotiates once and stops leaves everybody seeing exactly one other person.
	//
	// Waited for rather than slept through, because a three-party call is three chained
	// exchanges and how long they take is the machine's business. What is being asserted
	// is that they all complete.
	deadline := time.Now().Add(10 * time.Second)
	for {
		incomplete := ""
		for name, peer := range map[string]*harness.Peer{"alice": alice, "bob": bob, "carol": carol} {
			if videoTracks(peer) < 2 {
				incomplete = name
				break
			}
		}
		if incomplete == "" {
			break
		}
		if time.Now().After(deadline) {
			for name, peer := range map[string]*harness.Peer{"alice": alice, "bob": bob, "carol": carol} {
				t.Logf("%s is receiving %d video tracks", name, videoTracks(peer))
			}
			t.Fatalf("%s never received everybody", incomplete)
		}
		time.Sleep(50 * time.Millisecond)
	}

	if got := room.server.Participants("call-2"); got != 3 {
		t.Fatalf("the node holds %d participants, want 3", got)
	}
}

// videoTracks counts how many distinct video tracks a peer is actually receiving.
func videoTracks(peer *harness.Peer) int {
	tracks := 0
	for _, report := range peer.Reports() {
		if report.Kind == "video" && report.Packets > 0 {
			tracks++
		}
	}
	return tracks
}

// TestAReceiverOnlyParticipantIsNotForwardedToAnyone: the shape a listener takes, and the
// check that a server does not try to forward from somebody publishing nothing.
func TestAReceiverOnlyParticipantIsNotForwardedToAnyone(t *testing.T) {
	t.Parallel()
	room := newRoom(t)

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	speaker := room.join(t, ctx, "call-3", "speaker", true)
	listener := room.join(t, ctx, "call-3", "listener", false)

	if err := listener.WaitForMedia(ctx, "video", "audio"); err != nil {
		t.Fatalf("the listener received nothing: %v", err)
	}
	time.Sleep(1500 * time.Millisecond)

	if received := speaker.Received("video"); received.Packets > 0 {
		t.Fatalf("the speaker received %d packets from a listener", received.Packets)
	}
}

// TestAKeyframeRequestReachesThePublisher is CL-6's other half: a decoder that has lost
// its reference frame asks, and the server relays to whoever can answer.
func TestAKeyframeRequestReachesThePublisher(t *testing.T) {
	t.Parallel()
	room := newRoom(t)

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	// A keyframe interval long enough that one arriving during the window below was
	// asked for rather than due.
	signaller := newDirect(room.server, "call-4", "publisher")
	room.mutex.Lock()
	room.signallers["publisher"] = signaller
	room.mutex.Unlock()

	publisher, err := harness.NewPeer(ctx, signaller, harness.PeerOptions{
		Name: "publisher", Publish: true, Logger: quiet(),
		Source: harness.SourceOptions{KeyframeEvery: 100_000},
	})
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}
	t.Cleanup(func() { _ = publisher.Close(context.Background()) })

	receiver := room.join(t, ctx, "call-4", "receiver", false)
	if err := receiver.WaitForMedia(ctx, "video"); err != nil {
		t.Fatalf("no media arrived: %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	before := receiver.Received("video").Keyframes
	if err := receiver.RequestKeyframe(); err != nil {
		t.Fatalf("request keyframe: %v", err)
	}

	started := time.Now()
	deadline := time.Now().Add(5 * time.Second)
	for receiver.Received("video").Keyframes <= before {
		if time.Now().After(deadline) {
			t.Fatalf("no keyframe within %s of asking", time.Since(started))
		}
		time.Sleep(20 * time.Millisecond)
	}
	// CL-6 bounds recovery at two seconds. Reported so the margin is visible rather than
	// implied.
	elapsed := time.Since(started)
	t.Logf("keyframe arrived %s after the request", elapsed.Round(time.Millisecond))
	if elapsed > 2*time.Second {
		t.Fatalf("keyframe recovery took %s, over CL-6's two seconds", elapsed)
	}
}

// TestLeavingReleasesTheTransportAndForgetsAnEmptyCall: a node that keeps empty calls
// keeps a memory leak with a name, and a departed participant whose tracks are still
// forwarded is worse.
func TestLeavingReleasesTheTransportAndForgetsAnEmptyCall(t *testing.T) {
	t.Parallel()
	room := newRoom(t)

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	alice := room.join(t, ctx, "call-5", "alice", true)
	bob := room.join(t, ctx, "call-5", "bob", true)

	if err := bob.WaitForMedia(ctx, "video"); err != nil {
		t.Fatalf("bob received nothing: %v", err)
	}

	if err := alice.Close(ctx); err != nil {
		t.Fatalf("alice leaving: %v", err)
	}
	if got := room.server.Participants("call-5"); got != 1 {
		t.Fatalf("the node holds %d participants after one left, want 1", got)
	}

	if err := bob.Close(ctx); err != nil {
		t.Fatalf("bob leaving: %v", err)
	}
	if got := room.server.Calls(); got != 0 {
		t.Fatalf("the node still holds %d calls after everyone left", got)
	}
}

// TestCallsAreIsolated: two calls on one node must not hear each other. It is one map
// lookup away from being wrong, and the failure is the worst kind a call system can have.
func TestCallsAreIsolated(t *testing.T) {
	t.Parallel()
	room := newRoom(t)

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	insider := room.join(t, ctx, "call-6", "insider", true)
	stranger := room.join(t, ctx, "call-7", "stranger", false)

	// Somebody in the first call, so it is actually forwarding.
	witness := room.join(t, ctx, "call-6", "witness", false)
	if err := witness.WaitForMedia(ctx, "video"); err != nil {
		t.Fatalf("the first call is not forwarding: %v", err)
	}
	time.Sleep(1500 * time.Millisecond)

	if received := stranger.Received("video"); received.Packets > 0 {
		t.Fatalf("a participant in another call received %d packets", received.Packets)
	}
	if room.server.Calls() != 2 {
		t.Fatalf("the node holds %d calls, want 2", room.server.Calls())
	}
	_ = insider
}
