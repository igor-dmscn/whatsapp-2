package messaging

import "errors"

// Errors callers are expected to distinguish.
var (
	ErrConversationNotFound     = errors.New("conversation not found")
	ErrEntryNotFound            = errors.New("entry not found")
	ErrNotAMember               = errors.New("not a member of this conversation")
	ErrNotPermittedToWrite      = errors.New("not permitted to write to this conversation")
	ErrDirectConversationIsFull = errors.New("a direct conversation has exactly two members")
	ErrGroupIsFull              = errors.New("group is at its member limit")
	ErrAlreadyAMember           = errors.New("already a member of this conversation")
	ErrCannotMessageSelf        = errors.New("cannot start a direct conversation with yourself")

	// ErrEntryAlreadySent means this author already sent an entry with this
	// client-supplied identifier. Not a failure — the caller resolves it by
	// returning the entry that already exists (MS-2).
	ErrEntryAlreadySent = errors.New("entry already sent")

	// ErrSequenceAlreadyTaken means a position was written twice. It should be
	// unreachable: appends hold the conversation's row lock, so no two writers can
	// pick the same position. It exists because the unique index that would catch
	// such a bug deserves a name rather than surfacing as a raw driver error.
	ErrSequenceAlreadyTaken = errors.New("sequence already taken")
)

// ValidationError names the field that was wrong.
type ValidationError struct {
	Field  string
	Reason string
}

func (e ValidationError) Error() string {
	return e.Field + ": " + e.Reason
}
