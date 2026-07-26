package domain

import "errors"

// Errors callers are expected to distinguish.
var (
	ErrCallNotFound = errors.New("call not found")

	// ErrCallEnded means the call is over. Terminal by design: reviving one would make
	// "who was in this call" unanswerable.
	ErrCallEnded = errors.New("call has ended")

	// ErrCallIsFull means the participant limit is reached.
	ErrCallIsFull = errors.New("call is at its participant limit")

	// ErrNotPermitted means the account may not join this call. Entitlement derives
	// solely from membership of the conversation (CL-1), so this is Messaging's answer
	// relayed rather than a rule of Calling's own.
	ErrNotPermitted = errors.New("not permitted to join this call")

	// ErrNoMediaNode means no SFU is available to hold the call. A start refused for
	// this reason is a capacity problem, not the caller's fault, and must not be
	// reported as one.
	ErrNoMediaNode = errors.New("no media node is available")
)

// ValidationError names the field that was wrong.
type ValidationError struct {
	Field  string
	Reason string
}

func (e ValidationError) Error() string { return e.Field + ": " + e.Reason }
