package domain

import "time"

// Event is a fact: something that happened, named in the past tense, that other
// parts of the system may care about.
//
// Events are recorded by aggregates as a side effect of behaviour, never
// constructed by callers. An aggregate that changed state and recorded nothing
// has hidden that change from the rest of the system.
type Event interface {
	// EventName is the wire name, stable across refactors because consumers and
	// stored outbox rows depend on it. Renaming a Go type must not rename this.
	EventName() string
	OccurredAt() time.Time
}

// recorder is embedded by aggregates to collect the events they raise.
type recorder struct {
	events []Event
}

// record appends an event raised by aggregate behaviour.
func (r *recorder) record(event Event) {
	r.events = append(r.events, event)
}

// TakeEvents returns the events recorded since the last call and clears them.
//
// Draining rather than reading is deliberate: the caller that takes the events
// becomes responsible for publishing them exactly once, and a second caller
// getting the same events would publish them twice.
func (r *recorder) TakeEvents() []Event {
	taken := r.events
	r.events = nil
	return taken
}

// occurred carries the timestamp every event needs.
type occurred struct {
	At time.Time
}

func (o occurred) OccurredAt() time.Time { return o.At }

// --- Account events ---

// AccountRegistered is raised when an account first comes into existence.
type AccountRegistered struct {
	occurred
	AccountID AccountID
	Handle    Handle
}

func (AccountRegistered) EventName() string { return "identity.account_registered" }

// CredentialAdded is raised when a new way of proving control is added.
//
// The material is deliberately absent. An event is published beyond the
// aggregate that raised it, and a password digest has no business travelling.
type CredentialAdded struct {
	occurred
	AccountID    AccountID
	CredentialID CredentialID
	Kind         CredentialKind
}

func (CredentialAdded) EventName() string { return "identity.credential_added" }

// --- Device events ---

// DeviceRegistered is raised when a device first acts for an account.
type DeviceRegistered struct {
	occurred
	AccountID AccountID
	DeviceID  DeviceID
	Name      string
}

func (DeviceRegistered) EventName() string { return "identity.device_registered" }

// DeviceRevoked is raised when a device may no longer act.
//
// This is the event with a real consumer waiting: Messaging must close any
// WebSocket held by a revoked device rather than letting it keep receiving
// entries until its access token happens to expire.
type DeviceRevoked struct {
	occurred
	AccountID AccountID
	DeviceID  DeviceID
}

func (DeviceRevoked) EventName() string { return "identity.device_revoked" }

// --- Session events ---

// SessionStarted is raised when a device begins a new session.
type SessionStarted struct {
	occurred
	SessionID SessionID
	DeviceID  DeviceID
}

func (SessionStarted) EventName() string { return "identity.session_started" }

// SessionRotated is raised when a refresh token is exchanged.
//
// Worth publishing because it is the signal that detects refresh-token theft: a
// session rotating far more often than a client would explain is a replay.
type SessionRotated struct {
	occurred
	SessionID SessionID
	DeviceID  DeviceID
}

func (SessionRotated) EventName() string { return "identity.session_rotated" }
