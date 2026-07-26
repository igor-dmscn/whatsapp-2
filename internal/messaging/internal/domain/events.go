package domain

import "time"

// Event is a fact: something that happened, named in the past tense.
//
// Recorded by aggregates as a side effect of behaviour, never constructed by
// callers. From phase 3 these are written to the outbox in the same transaction
// as the aggregate and published to Kafka (ADR-0003).
type Event interface {
	// EventName is the wire name, stable across Go refactors because consumers
	// and stored outbox rows depend on it.
	EventName() string
	OccurredAt() time.Time
}

// recorder is embedded by aggregates to collect the events they raise.
type recorder struct {
	events []Event
}

func (r *recorder) record(event Event) {
	r.events = append(r.events, event)
}

// TakeEvents returns events recorded since the last call and clears them.
// Draining, so that whoever takes them owns publishing them exactly once.
func (r *recorder) TakeEvents() []Event {
	taken := r.events
	r.events = nil
	return taken
}

type occurred struct {
	At time.Time
}

func (o occurred) OccurredAt() time.Time { return o.At }

// ConversationStarted is raised when a conversation comes into existence.
type ConversationStarted struct {
	occurred
	ConversationID ConversationID
	Kind           Kind
}

func (ConversationStarted) EventName() string { return "messaging.conversation_started" }

// EntryAppended is raised when a position in the log is filled.
//
// It carries the content type and size but not the body. Events travel beyond the
// aggregate that raised them, and a message body has no business doing so — that
// is ADR-0001 holding at the event boundary as well as the storage one. Consumers
// needing the content read the entry; every consumer built so far needs only the
// envelope.
type EntryAppended struct {
	occurred
	ConversationID ConversationID
	EntryID        EntryID
	Sequence       Sequence
	AuthorID       AccountID
	ContentType    string
	Size           int
}

func (EntryAppended) EventName() string { return "messaging.entry_appended" }

// MemberJoined is raised when an account gains a membership.
type MemberJoined struct {
	occurred
	ConversationID ConversationID
	AccountID      AccountID
	Role           Role
	VisibleFrom    Sequence
}

func (MemberJoined) EventName() string { return "messaging.member_joined" }

// CursorAdvanced is raised when a member reports having read through a position.
//
// Through, not "one more": a client that read fifty messages sends one event, and a
// client whose earlier acknowledgement was lost is corrected by the next one. An
// increment would need every event to arrive exactly once, which is the guarantee
// this system does not have.
type CursorAdvanced struct {
	occurred
	ConversationID ConversationID
	AccountID      AccountID
	Through        Sequence
}

func (CursorAdvanced) EventName() string { return "messaging.cursor_advanced" }

// EntriesDelivered is raised when a member's device has taken delivery through a
// position. The middle of MS-13's three states.
type EntriesDelivered struct {
	occurred
	ConversationID ConversationID
	AccountID      AccountID
	Through        Sequence
}

func (EntriesDelivered) EventName() string { return "messaging.entries_delivered" }

// MemberLeft is raised when a membership stops participating.
type MemberLeft struct {
	occurred
	ConversationID ConversationID
	AccountID      AccountID
}

func (MemberLeft) EventName() string { return "messaging.member_left" }
