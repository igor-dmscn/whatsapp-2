// Package calling_test drives the context through the frames a client actually sends,
// against real Postgres and the real media plane.
//
// Through the frames rather than the service, deliberately: signalling is where the
// interesting failures are — a join whose answer never comes back, a renegotiation offer
// delivered to the wrong socket — and none of those are visible from a use case.
package calling_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"

	"comms/internal/calling"
	"comms/internal/harness"
	"comms/internal/platform/database/testdb"
	"comms/internal/platform/id"
)

const budget = 25 * time.Second

// permissive answers Messaging's question with yes, so that tests about signalling are not
// also tests about membership. The one that cares sets refuse.
type permissive struct {
	refuse bool
}

func (p permissive) MayJoin(context.Context, string, string) (bool, error) { return !p.refuse, nil }

// recordingNotifier remembers which conversations were told a call changed.
type recordingNotifier struct {
	mutex   sync.Mutex
	changed []string
}

func (n *recordingNotifier) CallChanged(_ context.Context, conversationID, _ string) error {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	n.changed = append(n.changed, conversationID)
	return nil
}

func (n *recordingNotifier) count() int {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	return len(n.changed)
}

// socket stands in for a client's WebSocket.
//
// Frames are queued rather than delivered to a callback, so a test can wait for the one it
// wants and assert on the rest — which is what a client does.
type socket struct {
	accountID string
	deviceID  string

	mutex  sync.Mutex
	frames []map[string]any
	offers chan string
}

func newSocket(accountID, deviceID string) *socket {
	return &socket{accountID: accountID, deviceID: deviceID, offers: make(chan string, 4)}
}

func (s *socket) AccountID() string { return s.accountID }
func (s *socket) DeviceID() string  { return s.deviceID }

func (s *socket) Send(frame any) error {
	encoded, err := json.Marshal(frame)
	if err != nil {
		return err
	}
	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		return err
	}

	s.mutex.Lock()
	s.frames = append(s.frames, decoded)
	s.mutex.Unlock()

	// A server-initiated offer goes to its own channel: something has to answer it, and
	// that is what the harness peer is for.
	if decoded["type"] == "call.offer" {
		select {
		case s.offers <- decoded["sdp"].(string):
		default:
		}
	}
	return nil
}

// await returns the first frame of a type, waiting for it to arrive.
func (s *socket) await(t *testing.T, frameType string) map[string]any {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for {
		s.mutex.Lock()
		for _, frame := range s.frames {
			if frame["type"] == frameType {
				s.mutex.Unlock()
				return frame
			}
		}
		held := len(s.frames)
		s.mutex.Unlock()

		if time.Now().After(deadline) {
			t.Fatalf("no %s frame arrived (%d frames held)", frameType, held)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// caller is a harness peer plus the socket its signalling rides.
type caller struct {
	socket *socket
	peer   *harness.Peer
	callID string
}

// harnessed wires a module over a real database.
type harnessed struct {
	module       *calling.Module
	notifier     *recordingNotifier
	conversation string
}

func newHarnessed(t *testing.T, permission permissive) *harnessed {
	t.Helper()

	db := testdb.Open(t)
	notifier := &recordingNotifier{}

	module, err := calling.New(db, permission, notifier, calling.Options{
		Address: "test-node",
		Logger:  slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatalf("wire calling: %v", err)
	}
	t.Cleanup(module.Close)

	// A real identifier, because the column is uuid.
	return &harnessed{module: module, notifier: notifier, conversation: id.New()}
}

// signaller carries a peer's negotiation over the frame handler, which is what a browser's
// socket does.
type signaller struct {
	held    *harnessed
	socket  *socket
	call    *caller
	answers chan string
}

func (s *signaller) Join(
	ctx context.Context,
	offer webrtc.SessionDescription,
) (webrtc.SessionDescription, error) {
	frame, err := json.Marshal(map[string]string{
		"type": "call.join", "conversation_id": s.held.conversation, "sdp": offer.SDP,
	})
	if err != nil {
		return webrtc.SessionDescription{}, err
	}
	if err := s.held.module.HandleFrame(ctx, s.socket, "call.join", frame); err != nil {
		return webrtc.SessionDescription{}, err
	}

	joined := s.socket.awaited("call.joined")
	if joined == nil {
		return webrtc.SessionDescription{}, errors.New("no call.joined frame")
	}
	s.call.callID, _ = joined["call_id"].(string)

	answer, _ := joined["sdp"].(string)
	return webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: answer}, nil
}

func (s *signaller) Leave(ctx context.Context) error {
	if s.call.callID == "" {
		return nil
	}
	frame, err := json.Marshal(map[string]string{"type": "call.leave", "call_id": s.call.callID})
	if err != nil {
		return err
	}
	return s.held.module.HandleFrame(ctx, s.socket, "call.leave", frame)
}

func (s *signaller) Offers() <-chan string { return s.socket.offers }

func (s *signaller) Answer(ctx context.Context, answer string) error {
	frame, err := json.Marshal(map[string]string{
		"type": "call.answer", "call_id": s.call.callID, "sdp": answer,
	})
	if err != nil {
		return err
	}
	return s.held.module.HandleFrame(ctx, s.socket, "call.answer", frame)
}

// awaited returns a frame of a type if one has already arrived, without waiting.
func (s *socket) awaited(frameType string) map[string]any {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	for _, frame := range s.frames {
		if frame["type"] == frameType {
			return frame
		}
	}
	return nil
}

// join puts a caller into the conversation's call through the frame handler.
func (h *harnessed) join(t *testing.T, ctx context.Context, name string) *caller {
	t.Helper()

	held := &caller{socket: newSocket(id.New(), id.New())}
	peer, err := harness.NewPeer(ctx, &signaller{held: h, socket: held.socket, call: held},
		harness.PeerOptions{Name: name, Publish: true, Logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	held.peer = peer
	t.Cleanup(func() { _ = peer.Close(context.Background()) })
	return held
}

// TestTwoPeopleJoinOneCallAndSeeEachOther is phase 9 end to end: signalling over a socket,
// entitlement from membership, one call per conversation, and media both ways.
func TestTwoPeopleJoinOneCallAndSeeEachOther(t *testing.T) {
	t.Parallel()
	held := newHarnessed(t, permissive{})

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	alice := held.join(t, ctx, "alice")
	bob := held.join(t, ctx, "bob")

	// One call, not two: the second to join found the first's rather than starting another
	// (CL-2). This is the assertion the partial unique index and the single Join use case
	// exist to make true.
	if alice.callID != bob.callID {
		t.Fatalf("two calls in one conversation: %s and %s", alice.callID, bob.callID)
	}

	// The second joiner is answered; the first is offered. Both directions have to work or
	// a two-person call is one-way.
	if err := bob.peer.WaitForMedia(ctx, "video", "audio"); err != nil {
		t.Fatalf("bob received nothing: %v", err)
	}
	if err := alice.peer.WaitForMedia(ctx, "video", "audio"); err != nil {
		t.Fatalf("alice received nothing — renegotiation did not reach her socket: %v", err)
	}

	joined := bob.socket.await(t, "call.joined")
	if joined["state"] != "active" {
		t.Fatalf("the call is %q with two people in it, want active", joined["state"])
	}
	participants, _ := joined["participants"].([]any)
	if len(participants) != 2 {
		t.Fatalf("the joined frame lists %d participants, want 2", len(participants))
	}

	// Everyone in the conversation was told, twice: once per join.
	if held.notifier.count() < 2 {
		t.Fatalf("clients were told %d times", held.notifier.count())
	}
}

// TestSomebodyWhoIsNotAMemberCannotJoin is CL-1. Calling holds no access rules of its own,
// so this checks that it honours the answer rather than that it has an opinion.
func TestSomebodyWhoIsNotAMemberCannotJoin(t *testing.T) {
	t.Parallel()
	held := newHarnessed(t, permissive{refuse: true})

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	stranger := newSocket(id.New(), id.New())
	frame, err := json.Marshal(map[string]string{
		"type": "call.join", "conversation_id": held.conversation, "sdp": "v=0",
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	err = held.module.HandleFrame(ctx, stranger, "call.join", frame)
	if err == nil {
		t.Fatal("a non-member joined a call")
	}
	if err.Error() != "you may not join this call" {
		t.Fatalf("got %q", err.Error())
	}
	if held.module.Calls() != 0 {
		t.Fatalf("the node holds %d calls after a refused join", held.module.Calls())
	}
}

// TestAJoinWithNoOfferIsRefusedBeforeACallExists: a participant with no transport would be
// recorded as present and forward nothing, which is worse than a refusal.
func TestAJoinWithNoOfferIsRefusedBeforeACallExists(t *testing.T) {
	t.Parallel()
	held := newHarnessed(t, permissive{})

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	client := newSocket(id.New(), id.New())
	frame, err := json.Marshal(map[string]string{
		"type": "call.join", "conversation_id": held.conversation, "sdp": "",
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	if err := held.module.HandleFrame(ctx, client, "call.join", frame); err == nil {
		t.Fatal("a join with no offer was accepted")
	}
	if held.module.Calls() != 0 {
		t.Fatalf("the node holds %d calls after a refused join", held.module.Calls())
	}
}

// TestAskingWhetherThereIsACall is the durable half of a ring: the notification is
// ephemeral, so a client that missed it finds out by asking.
func TestAskingWhetherThereIsACall(t *testing.T) {
	t.Parallel()
	held := newHarnessed(t, permissive{})

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	asker := newSocket(id.New(), id.New())
	ask, err := json.Marshal(map[string]string{
		"type": "call.active", "conversation_id": held.conversation,
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	// Before anyone calls: an answer, not a failure.
	if err := held.module.HandleFrame(ctx, asker, "call.active", ask); err != nil {
		t.Fatalf("asking about a conversation with no call: %v", err)
	}
	asker.await(t, "call.none")

	alice := held.join(t, ctx, "alice")

	after := newSocket(id.New(), id.New())
	if err := held.module.HandleFrame(ctx, after, "call.active", ask); err != nil {
		t.Fatalf("asking about a live call: %v", err)
	}
	current := after.await(t, "call.current")
	if current["call_id"] != alice.callID {
		t.Fatalf("got call %v, want %s", current["call_id"], alice.callID)
	}
	// No answer to a call nobody offered: the SDP belongs to a join.
	if current["sdp"] != nil && current["sdp"] != "" {
		t.Fatal("a call.current frame carried an SDP")
	}
}

// TestTheLastToLeaveEndsTheCallAndReleasesTheNode is CL-3 through the socket, and the check
// that state and media are released together.
func TestTheLastToLeaveEndsTheCallAndReleasesTheNode(t *testing.T) {
	t.Parallel()
	held := newHarnessed(t, permissive{})

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	alice := held.join(t, ctx, "alice")
	bob := held.join(t, ctx, "bob")
	if err := bob.peer.WaitForMedia(ctx, "video"); err != nil {
		t.Fatalf("bob received nothing: %v", err)
	}

	if err := alice.peer.Close(ctx); err != nil {
		t.Fatalf("alice leaving: %v", err)
	}
	alice.socket.await(t, "call.left")

	// The call is still live with one person in it, and asking says so.
	ask, err := json.Marshal(map[string]string{
		"type": "call.active", "conversation_id": held.conversation,
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	watcher := newSocket(id.New(), id.New())
	if err := held.module.HandleFrame(ctx, watcher, "call.active", ask); err != nil {
		t.Fatalf("asking: %v", err)
	}
	watcher.await(t, "call.current")

	if err := bob.peer.Close(ctx); err != nil {
		t.Fatalf("bob leaving: %v", err)
	}

	// Ended, so asking now finds nothing — and the node is holding no transports.
	gone := newSocket(id.New(), id.New())
	if err := held.module.HandleFrame(ctx, gone, "call.active", ask); err != nil {
		t.Fatalf("asking after the call ended: %v", err)
	}
	gone.await(t, "call.none")

	if held.module.Calls() != 0 {
		t.Fatalf("the node still holds %d calls", held.module.Calls())
	}
}

// TestAClosedSocketTakesItsParticipantOutOfTheCall: a crashed tab says nothing, and without
// this a call keeps somebody nobody can see — CL-3 would never fire.
func TestAClosedSocketTakesItsParticipantOutOfTheCall(t *testing.T) {
	t.Parallel()
	held := newHarnessed(t, permissive{})

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	alice := held.join(t, ctx, "alice")
	bob := held.join(t, ctx, "bob")
	if err := bob.peer.WaitForMedia(ctx, "video"); err != nil {
		t.Fatalf("bob received nothing: %v", err)
	}

	// Bob's client vanishes without a leave frame — which is what a closed tab is.
	held.module.SocketClosed(ctx, bob.socket)

	ask, err := json.Marshal(map[string]string{
		"type": "call.active", "conversation_id": held.conversation,
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	watcher := newSocket(id.New(), id.New())
	if err := held.module.HandleFrame(ctx, watcher, "call.active", ask); err != nil {
		t.Fatalf("asking: %v", err)
	}

	current := watcher.await(t, "call.current")
	participants, _ := current["participants"].([]any)
	if len(participants) != 1 {
		t.Fatalf("the call lists %d participants after one vanished, want 1", len(participants))
	}
	_ = alice
}
