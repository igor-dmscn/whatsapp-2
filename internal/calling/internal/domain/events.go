package domain

import "time"

// Event is a fact: something that happened, named in the past tense.
type Event interface {
	EventName() string
	OccurredAt() time.Time
}

// recorder is embedded by aggregates to collect the events they raise.
type recorder struct {
	events []Event
}

func (r *recorder) record(event Event) { r.events = append(r.events, event) }

// TakeEvents returns events recorded since the last call and clears them.
func (r *recorder) TakeEvents() []Event {
	taken := r.events
	r.events = nil
	return taken
}

type occurred struct {
	At time.Time
}

func (o occurred) OccurredAt() time.Time { return o.At }

// CallStarted is raised when a call comes into existence.
//
// It names the node, because that is the one fact about a call that cannot be derived
// later: every participant must reach the same SFU (CL-4), and where a call lives is
// decided once.
type CallStarted struct {
	occurred
	CallID         CallID
	ConversationID ConversationID
	StarterID      AccountID
	Node           string
}

func (CallStarted) EventName() string { return "calling.call_started" }

// ParticipantJoined is raised when a device enters a call.
type ParticipantJoined struct {
	occurred
	CallID         CallID
	ConversationID ConversationID
	AccountID      AccountID
	DeviceID       DeviceID
}

func (ParticipantJoined) EventName() string { return "calling.participant_joined" }

// ParticipantLeft is raised when a device leaves.
type ParticipantLeft struct {
	occurred
	CallID         CallID
	ConversationID ConversationID
	DeviceID       DeviceID
}

func (ParticipantLeft) EventName() string { return "calling.participant_left" }

// CallEnded is raised when the last participant leaves (CL-3).
//
// It carries the duration because that is the only thing anything downstream has wanted
// so far — a call log entry — and because computing it from two events would mean
// consumers keeping state to answer a question the aggregate already knows.
type CallEnded struct {
	occurred
	CallID         CallID
	ConversationID ConversationID
	Duration       time.Duration
}

func (CallEnded) EventName() string { return "calling.call_ended" }
