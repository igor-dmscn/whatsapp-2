package domain

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

	// ErrNotPermittedToAdminister means the actor may not change who belongs.
	ErrNotPermittedToAdminister = errors.New("not permitted to change this conversation's membership")

	// ErrMembershipIsFixed means the conversation's membership cannot change at
	// all. Direct conversations are the case: the pair is the conversation's
	// identity, so adding or removing anyone would make it a different thing
	// while keeping its history (MS-4).
	ErrMembershipIsFixed = errors.New("this conversation's membership cannot be changed")

	// ErrCannotRemoveSelf means an admin tried to remove their own membership
	// rather than leaving.
	ErrCannotRemoveSelf = errors.New("leave the conversation rather than removing yourself")

	// ErrNotTheAuthor means only the entry's author may do this (MS-8, MS-9).
	ErrNotTheAuthor = errors.New("only the author may amend this entry")

	// ErrCannotAmendAnAmendment means the target is itself a revision or retraction.
	// A second edit amends the original, not the first edit.
	ErrCannotAmendAnAmendment = errors.New("amendments target the original entry")

	// ErrEntryRetracted means the target has been withdrawn and cannot be edited.
	ErrEntryRetracted = errors.New("this entry has been retracted")

	ErrInviteNotFound  = errors.New("invite not found")
	ErrInviteExpired   = errors.New("invite has expired")
	ErrInviteRevoked   = errors.New("invite has been revoked")
	ErrInviteExhausted = errors.New("invite has been used its maximum number of times")

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
