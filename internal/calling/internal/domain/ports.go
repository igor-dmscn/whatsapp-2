package domain

import "context"

// CallRepository stores and loads calls.
//
// One repository, because Call is the only aggregate root here: a participant has no
// life outside its call, so there is no way to load one on its own.
type CallRepository interface {
	Save(ctx context.Context, call *Call) error

	// Find returns a call, or ErrCallNotFound.
	Find(ctx context.Context, id CallID) (*Call, error)

	// ActiveIn returns the live call in a conversation, or ErrCallNotFound.
	//
	// This is CL-2 expressed as a query the write path uses under a lock: a conversation
	// has at most one active call, and starting a second returns the existing one.
	ActiveIn(ctx context.Context, conversationID ConversationID) (*Call, error)

	// LiveFor returns every unended call a device is present in.
	//
	// For cleaning up after a socket that closed. A device is normally in at most one call,
	// so this returns a slice rather than one because "normally" is not a guarantee and a
	// leaked participant is the failure it exists to prevent.
	LiveFor(ctx context.Context, device DeviceID) ([]*Call, error)
}

// EventPublisher carries recorded events out of the context.
type EventPublisher interface {
	Publish(ctx context.Context, events []Event) error
}

// Transactor runs work atomically, so a call's state and its events commit together.
type Transactor interface {
	InTransaction(ctx context.Context, work func(ctx context.Context) error) error
}

// Conversations is what Calling needs to know about Messaging, and no more.
//
// One question. Calling holds no access rules of its own (CL-1): entitlement to join a
// call is entitlement to be in the conversation it belongs to, and that lives in the
// context that owns membership.
type Conversations interface {
	MayJoin(ctx context.Context, conversationID, accountID string) (bool, error)
}

// MediaNodes allocates a call to an SFU and carries signalling to it.
//
// The one port whose shape was decided by the test harness rather than by the server
// (docs/plan.md phase 8): a participant offers, and the node answers. Nothing about
// trickle ICE, because the harness established that a single exchange is sufficient for
// a node on a known address.
type MediaNodes interface {
	// Allocate picks a node for a new call. Returns ErrNoMediaNode if none is available.
	Allocate(ctx context.Context) (string, error)

	// Join hands a participant's offer to the node holding the call and returns its
	// answer. The node is told which call and which participant, because forwarding is
	// per call and a participant must not receive its own media.
	Join(ctx context.Context, node string, call CallID, participant DeviceID, offer string) (string, error)

	// Answer carries a participant's answer to an offer the node sent.
	//
	// The direction the phase-8 harness did not anticipate. A call of two is asymmetric:
	// the second to join is answered, and the first has to be *offered* the second's
	// media — so signalling is bidirectional and this is the return leg.
	Answer(ctx context.Context, node string, call CallID, participant DeviceID, answer string) error

	// Leave tells the node a participant has gone, so it can release the transport
	// rather than waiting for ICE to time out.
	Leave(ctx context.Context, node string, call CallID, participant DeviceID) error
}

// Notifier tells connected clients about a call.
//
// Ephemeral (ADR-0005) and allowed to fail: a missed ring is recovered by the callee
// asking whether there is a call when it next looks, and a missed departure is corrected
// by the next participant list. Satisfied by Messaging, which owns the socket fanout.
type Notifier interface {
	// CallChanged tells everyone in a conversation to look at its call again.
	CallChanged(ctx context.Context, conversationID, callID string) error
}

// IDs generates the identifiers the domain requires but does not produce.
type IDs interface {
	NewCallID() CallID
}
