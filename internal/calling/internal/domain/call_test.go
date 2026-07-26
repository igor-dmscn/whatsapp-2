// Package domain_test covers the call lifecycle, which is where the calling
// requirements live: who is in a call, when it becomes active, and when it is over.
package domain_test

import (
	"errors"
	"testing"
	"time"

	"comms/internal/calling/internal/domain"
)

var (
	conversation = domain.ConversationID("conversation-1")
	alice        = domain.AccountID("alice")
	bob          = domain.AccountID("bob")
	at           = time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
)

func started(t *testing.T) *domain.Call {
	t.Helper()

	call, err := domain.Start("call-1", conversation, "sfu-1", alice, "alice-laptop", at)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	return call
}

func TestAStartedCallIsRingingWithItsStarterInIt(t *testing.T) {
	t.Parallel()
	call := started(t)

	if call.State() != domain.StateRinging {
		t.Fatalf("got state %q, want ringing", call.State())
	}
	if call.Size() != 1 {
		t.Fatalf("got %d participants, want 1", call.Size())
	}
	if !call.Includes("alice-laptop") {
		t.Fatal("the starter is not in the call")
	}

	events := call.TakeEvents()
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	// The node is in the event because it is the one fact that cannot be derived later:
	// every participant must reach the same one (CL-4).
	if started, ok := events[0].(domain.CallStarted); !ok || started.Node != "sfu-1" {
		t.Fatalf("got %#v", events[0])
	}
}

func TestACallWithoutANodeIsRefused(t *testing.T) {
	t.Parallel()

	// A call allocated to nowhere would negotiate with nothing. Refused at construction
	// rather than discovered when the first participant tries to send media.
	if _, err := domain.Start("call-1", conversation, "", alice, "alice-laptop", at); err == nil {
		t.Fatal("a call with no media node was accepted")
	}
}

// TestASecondParticipantMakesItActive: ringing means "started and alone", and nothing
// about anybody having pressed accept.
func TestASecondParticipantMakesItActive(t *testing.T) {
	t.Parallel()
	call := started(t)
	call.TakeEvents()

	if err := call.Join(bob, "bob-phone", at.Add(time.Second)); err != nil {
		t.Fatalf("join: %v", err)
	}
	if call.State() != domain.StateActive {
		t.Fatalf("got state %q, want active", call.State())
	}
	if call.Size() != 2 {
		t.Fatalf("got %d participants", call.Size())
	}
	if len(call.TakeEvents()) != 1 {
		t.Fatal("joining raised no event")
	}
}

// TestJoiningTwiceFromOneDeviceIsIdempotent is what makes a lost answer harmless. Without
// it, a retry produces a call whose participant count is wrong and whose media is
// duplicated.
func TestJoiningTwiceFromOneDeviceIsIdempotent(t *testing.T) {
	t.Parallel()
	call := started(t)
	call.TakeEvents()

	for attempt := range 3 {
		if err := call.Join(bob, "bob-phone", at.Add(time.Second)); err != nil {
			t.Fatalf("attempt %d: %v", attempt, err)
		}
	}
	if call.Size() != 2 {
		t.Fatalf("three joins from one device produced %d participants", call.Size())
	}
	if events := len(call.TakeEvents()); events != 1 {
		t.Fatalf("three joins raised %d events", events)
	}
}

// TestTwoDevicesOfOneAccountAreTwoParticipantsAndOneFace: a person on a laptop and a
// phone is two transports carrying two copies of the media, and one tile in a call UI.
func TestTwoDevicesOfOneAccountAreTwoParticipantsAndOneFace(t *testing.T) {
	t.Parallel()
	call := started(t)

	if err := call.Join(alice, "alice-phone", at.Add(time.Second)); err != nil {
		t.Fatalf("join: %v", err)
	}
	if call.Size() != 2 {
		t.Fatalf("got %d participants, want 2", call.Size())
	}
	if present := call.Present(); len(present) != 1 || present[0] != alice {
		t.Fatalf("got %v present, want one alice", present)
	}
}

// TestTheLastToLeaveEndsTheCall is CL-3.
func TestTheLastToLeaveEndsTheCall(t *testing.T) {
	t.Parallel()
	call := started(t)
	if err := call.Join(bob, "bob-phone", at.Add(time.Second)); err != nil {
		t.Fatalf("join: %v", err)
	}
	call.TakeEvents()

	if err := call.Leave("bob-phone", at.Add(2*time.Second)); err != nil {
		t.Fatalf("first leave: %v", err)
	}
	if call.State() == domain.StateEnded {
		t.Fatal("the call ended while somebody was still in it")
	}
	// Drained, so what follows is only the last departure's events.
	call.TakeEvents()

	if err := call.Leave("alice-laptop", at.Add(3*time.Second)); err != nil {
		t.Fatalf("second leave: %v", err)
	}
	if call.State() != domain.StateEnded {
		t.Fatalf("got state %q after the last participant left", call.State())
	}
	if call.EndedAt().IsZero() {
		t.Fatal("an ended call with no end time")
	}

	// A departure and an ending, in that order: a consumer that only knew the call ended
	// could not tell who was last out.
	events := call.TakeEvents()
	if len(events) != 2 {
		t.Fatalf("got %d events, want a departure and an ending", len(events))
	}
	if _, ok := events[1].(domain.CallEnded); !ok {
		t.Fatalf("the last event is %T", events[1])
	}
}

func TestLeavingTwiceOrLeavingACallYouAreNotInIsHarmless(t *testing.T) {
	t.Parallel()
	call := started(t)

	if err := call.Leave("somebody-else", at); err != nil {
		t.Fatalf("leaving a call you are not in: %v", err)
	}
	if call.Size() != 1 {
		t.Fatalf("got %d participants", call.Size())
	}

	if err := call.Leave("alice-laptop", at); err != nil {
		t.Fatalf("leave: %v", err)
	}
	// The call has ended, and a client whose socket died mid-departure will say so again.
	if err := call.Leave("alice-laptop", at); err != nil {
		t.Fatalf("leaving twice: %v", err)
	}
}

func TestAnEndedCallCannotBeJoined(t *testing.T) {
	t.Parallel()
	call := started(t)
	if err := call.Leave("alice-laptop", at); err != nil {
		t.Fatalf("leave: %v", err)
	}

	if err := call.Join(bob, "bob-phone", at.Add(time.Second)); !errors.Is(err, domain.ErrCallEnded) {
		t.Fatalf("got %v, want ErrCallEnded", err)
	}
}

// TestRejoiningAfterLeavingKeepsTheEarlierDeparture: the record of who was in a call is
// what a call log is made of, and a rejoin must not erase it.
func TestRejoiningAfterLeavingKeepsTheEarlierDeparture(t *testing.T) {
	t.Parallel()
	call := started(t)
	if err := call.Join(bob, "bob-phone", at.Add(time.Second)); err != nil {
		t.Fatalf("join: %v", err)
	}
	if err := call.Leave("bob-phone", at.Add(2*time.Second)); err != nil {
		t.Fatalf("leave: %v", err)
	}
	if err := call.Join(bob, "bob-phone", at.Add(3*time.Second)); err != nil {
		t.Fatalf("rejoin: %v", err)
	}

	if call.Size() != 2 {
		t.Fatalf("got %d present after a rejoin", call.Size())
	}
	departed := 0
	for _, participant := range call.Participants() {
		if !participant.Present() {
			departed++
		}
	}
	if departed != 1 {
		t.Fatalf("got %d departures in the record, want 1", departed)
	}
}

func TestTheParticipantLimitHolds(t *testing.T) {
	t.Parallel()
	call := started(t)

	for index := 1; index < domain.MaxParticipants; index++ {
		device := domain.DeviceID("device-" + string(rune('a'+index)))
		if err := call.Join(domain.AccountID(device), device, at); err != nil {
			t.Fatalf("join %d: %v", index, err)
		}
	}
	if call.Size() != domain.MaxParticipants {
		t.Fatalf("got %d participants, want %d", call.Size(), domain.MaxParticipants)
	}

	if err := call.Join("one-too-many", "one-too-many", at); !errors.Is(err, domain.ErrCallIsFull) {
		t.Fatalf("got %v, want ErrCallIsFull", err)
	}

	// A departure makes room, which is what makes the limit a limit on concurrency
	// rather than on how many people may ever have been in the call.
	if err := call.Leave("device-b", at.Add(time.Second)); err != nil {
		t.Fatalf("leave: %v", err)
	}
	if err := call.Join("late", "late", at.Add(2*time.Second)); err != nil {
		t.Fatalf("joining after a departure: %v", err)
	}
}

// TestParticipantsAreCopiedOut keeps a caller from rewriting what the aggregate holds.
func TestParticipantsAreCopiedOut(t *testing.T) {
	t.Parallel()
	call := started(t)

	call.Participants()[0].LeftAt = at
	if !call.Includes("alice-laptop") {
		t.Fatal("a caller removed a participant through the accessor")
	}
}
