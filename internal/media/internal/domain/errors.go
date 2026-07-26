package domain

import "errors"

// Errors callers are expected to distinguish.
var (
	ErrAttachmentNotFound = errors.New("attachment not found")

	// ErrTooLarge is MD-4 refused at the point of request, before a byte moves.
	ErrTooLarge = errors.New("attachment is larger than the limit")

	// ErrNotPending means the upload has already been completed or has failed.
	ErrNotPending = errors.New("attachment is not awaiting an upload")

	// ErrNotUploaded means variants were offered for an attachment whose bytes are
	// not in the store.
	ErrNotUploaded = errors.New("attachment has not been uploaded")

	// ErrAlreadyReady means a ready attachment was marked failed, which would take a
	// displayable photo away from clients already showing it.
	ErrAlreadyReady = errors.New("attachment is already ready")

	// ErrSizeMismatch means the store holds a different number of bytes than the
	// uploader declared — a truncated transfer, since a larger one is refused by the
	// signed URL.
	ErrSizeMismatch = errors.New("uploaded size does not match what was declared")

	// ErrNotUploadedYet means the client called completion before the bytes arrived.
	ErrNotUploadedYet = errors.New("no bytes have been uploaded")

	// ErrNotPermitted means the account may not attach to, or view, this
	// conversation. One error for both because the answer to a viewer who is not
	// entitled must not distinguish "not a member" from "no such attachment".
	ErrNotPermitted = errors.New("not permitted")
)

// ValidationError names the field that was wrong.
type ValidationError struct {
	Field  string
	Reason string
}

func (e ValidationError) Error() string {
	return e.Field + ": " + e.Reason
}
