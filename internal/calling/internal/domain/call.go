// Package domain is Calling's model: calls, who is in them, and where their media is
// forwarded.
package domain

import "time"

// Identifiers. Conversations and accounts are references into other contexts, held as
// plain strings: entitlement to join a call derives from membership of its conversation
// (CL-1), and Calling asks rather than knowing.
type (
	CallID         string
	ConversationID string
	AccountID      string
	// DeviceID matters here in a way it does not elsewhere: a person on a laptop and a
	// phone is two transports carrying two copies of the media, so a participant is
	// per device rather than per account.
	DeviceID string
)

// State is where a call is in its life.
type State string

const (
	// StateRinging means the call has been started and nobody else has joined.
	StateRinging State = "ringing"
	// StateActive means at least two participants are present.
	StateActive State = "active"
	// StateEnded means the last participant left. Terminal: a call is never revived,
	// because reviving one would make "who was in this call" unanswerable.
	StateEnded State = "ended"
)

// MaxParticipants bounds one call.
//
// Eight, and the number is a media decision rather than a product one: an SFU forwards
// n·(n−1) streams for n publishers, so eight is 56 forwarded streams and sixteen is 240.
// Beyond this the answer is not a bigger call but a different design — a broadcast with
// few publishers and many receivers.
const MaxParticipants = 8

// Participant is one device in a call.
//
// Not an aggregate root of its own: a participant has no life outside its call, and
// every rule about one — may they join, are they the last to leave — is a rule about the
// call. So the call is the boundary and this is inside it.
type Participant struct {
	AccountID AccountID
	DeviceID  DeviceID
	JoinedAt  time.Time
	// LeftAt is zero while they are present. Kept rather than deleted so that "who was
	// in this call" has an answer after it ends.
	LeftAt time.Time
}

// Present reports whether this participant is still in the call.
func (p Participant) Present() bool { return p.LeftAt.IsZero() }

// Call is an aggregate root: a live audio or video session belonging to a conversation.
type Call struct {
	recorder

	id             CallID
	conversationID ConversationID
	state          State
	// node is the SFU holding this call's media. Recorded because every participant
	// must reach the same one (CL-4) — forwarding between nodes would double the media
	// path for no benefit, and is the thing a second node would silently start doing.
	node         string
	participants []Participant
	startedAt    time.Time
	endedAt      time.Time
}

// Start begins a call in a conversation.
//
// The starter is a participant from the outset. A call with nobody in it is a state
// nothing needs and one that CL-3 would immediately end.
func Start(
	id CallID,
	conversationID ConversationID,
	node string,
	starter AccountID,
	device DeviceID,
	now time.Time,
) (*Call, error) {
	if id == "" {
		return nil, ValidationError{"id", "must not be empty"}
	}
	if conversationID == "" {
		return nil, ValidationError{"conversation_id", "must not be empty"}
	}
	if node == "" {
		return nil, ValidationError{"node", "a call must be allocated to a media node"}
	}
	if starter == "" {
		return nil, ValidationError{"account_id", "must not be empty"}
	}

	call := &Call{
		id:             id,
		conversationID: conversationID,
		state:          StateRinging,
		node:           node,
		participants: []Participant{
			{AccountID: starter, DeviceID: device, JoinedAt: now},
		},
		startedAt: now,
	}
	call.record(CallStarted{
		occurred:       occurred{now},
		CallID:         id,
		ConversationID: conversationID,
		StarterID:      starter,
		Node:           node,
	})
	return call, nil
}

// ReconstituteCall rebuilds a call from storage. Only a repository should call this, and
// it validates nothing (ADR-0010).
func ReconstituteCall(
	id CallID,
	conversationID ConversationID,
	state State,
	node string,
	participants []Participant,
	startedAt time.Time,
	endedAt time.Time,
) *Call {
	return &Call{
		id:             id,
		conversationID: conversationID,
		state:          state,
		node:           node,
		participants:   participants,
		startedAt:      startedAt,
		endedAt:        endedAt,
	}
}

// Join adds a participant.
//
// Idempotent on the device: a client that retries a join because the answer was lost
// gets the call it is already in rather than a second participant. Without that, a flaky
// network produces a call whose participant count is wrong and whose media is duplicated.
func (c *Call) Join(accountID AccountID, device DeviceID, now time.Time) error {
	if c.state == StateEnded {
		return ErrCallEnded
	}

	for index := range c.participants {
		if c.participants[index].DeviceID != device {
			continue
		}
		if c.participants[index].Present() {
			return nil
		}
		// Rejoining after leaving: the same device, a new presence. The earlier
		// departure stays in the record.
		break
	}

	if c.Size() >= MaxParticipants {
		return ErrCallIsFull
	}

	c.participants = append(c.participants,
		Participant{AccountID: accountID, DeviceID: device, JoinedAt: now})

	// The second presence is what makes a call active rather than ringing. Stated as a
	// count rather than as "somebody accepted", because a call that two people joined
	// simultaneously never had an acceptance and is no less active for it.
	if c.state == StateRinging && c.Size() > 1 {
		c.state = StateActive
	}

	c.record(ParticipantJoined{
		occurred:       occurred{now},
		CallID:         c.id,
		ConversationID: c.conversationID,
		AccountID:      accountID,
		DeviceID:       device,
	})
	return nil
}

// Leave removes a participant, ending the call if they were the last (CL-3).
//
// Leaving a call you are not in is not an error: a client that lost its socket
// mid-departure will say so again, and the second attempt must not fail.
func (c *Call) Leave(device DeviceID, now time.Time) error {
	if c.state == StateEnded {
		return nil
	}

	found := false
	for index := range c.participants {
		if c.participants[index].DeviceID == device && c.participants[index].Present() {
			c.participants[index].LeftAt = now
			found = true
			break
		}
	}
	if !found {
		return nil
	}

	c.record(ParticipantLeft{
		occurred:       occurred{now},
		CallID:         c.id,
		ConversationID: c.conversationID,
		DeviceID:       device,
	})

	if c.Size() == 0 {
		c.state = StateEnded
		c.endedAt = now
		c.record(CallEnded{
			occurred:       occurred{now},
			CallID:         c.id,
			ConversationID: c.conversationID,
			Duration:       now.Sub(c.startedAt),
		})
	}
	return nil
}

// Size is how many participants are present now.
func (c *Call) Size() int {
	present := 0
	for _, participant := range c.participants {
		if participant.Present() {
			present++
		}
	}
	return present
}

// Includes reports whether a device is present.
func (c *Call) Includes(device DeviceID) bool {
	for _, participant := range c.participants {
		if participant.DeviceID == device && participant.Present() {
			return true
		}
	}
	return false
}

// Live reports whether this call can still be joined.
func (c *Call) Live() bool { return c.state != StateEnded }

func (c *Call) ID() CallID                     { return c.id }
func (c *Call) ConversationID() ConversationID { return c.conversationID }
func (c *Call) State() State                   { return c.state }
func (c *Call) Node() string                   { return c.node }
func (c *Call) StartedAt() time.Time           { return c.startedAt }
func (c *Call) EndedAt() time.Time             { return c.endedAt }

// Participants returns a copy, present and departed alike.
func (c *Call) Participants() []Participant {
	return append([]Participant(nil), c.participants...)
}

// Present returns the accounts currently in the call, deduplicated across devices.
//
// What a call UI shows: one tile per person, not one per transport.
func (c *Call) Present() []AccountID {
	seen := make(map[AccountID]bool, len(c.participants))
	accounts := make([]AccountID, 0, len(c.participants))
	for _, participant := range c.participants {
		if !participant.Present() || seen[participant.AccountID] {
			continue
		}
		seen[participant.AccountID] = true
		accounts = append(accounts, participant.AccountID)
	}
	return accounts
}

// String reports shape, never who is in it.
func (c *Call) String() string {
	return "Call(" + string(c.id) + ", " + string(c.state) + ")"
}
