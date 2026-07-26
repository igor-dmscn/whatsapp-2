package domain

import (
	"context"
	"time"
)

// AttachmentRepository stores and loads attachments.
//
// One repository, because Attachment is the only aggregate root here and its variants
// are part of it — saving an attachment saves its variants, and there is no way to
// load a variant on its own.
type AttachmentRepository interface {
	// Save writes the attachment and its variants.
	//
	// An upsert on both, so that reprocessing writes the same set rather than a second
	// one (MD-2). That is a primary key doing the work, not the worker remembering.
	Save(ctx context.Context, attachment *Attachment) error

	// Find returns an attachment, or ErrAttachmentNotFound.
	Find(ctx context.Context, id AttachmentID) (*Attachment, error)
}

// EventPublisher writes events where the relay will find them.
type EventPublisher interface {
	Publish(ctx context.Context, events []Event) error
}

// Transactor runs work atomically. The state change and its outbox row commit
// together or not at all (ADR-0003).
type Transactor interface {
	InTransaction(ctx context.Context, work func(ctx context.Context) error) error
}

// ObjectStore is where the bytes live.
//
// Declared here rather than the concrete store being used directly, because the one
// thing Media must be able to promise is that bytes never pass through the process
// serving requests — and a port with no method that returns content, only URLs and
// whole-object reads used by the worker, is that promise written down.
type ObjectStore interface {
	// Presign returns a URL authorising exactly one request with exactly those
	// headers. Signing the length is what enforces the cap at the store (MD-4).
	Presign(method, key string, headers map[string]string, expiry time.Duration) string

	// Size reports what the store holds, for checking an upload against what was
	// declared without reading it.
	Size(ctx context.Context, key string) (int64, bool, error)

	// Get reads an object whole. The worker's path, not the request path.
	Get(ctx context.Context, key string) ([]byte, error)

	// Put writes a variant.
	Put(ctx context.Context, key, contentType string, body []byte) error

	// Delete removes an object, for an upload that was refused.
	Delete(ctx context.Context, key string) error
}

// Conversations is what Media needs to know about Messaging, and no more.
//
// Media holds no access rules of its own: entitlement to attach to a conversation is
// entitlement to write to it, and entitlement to view an attachment is entitlement to
// see the entry that references it. Both live in Messaging, which owns membership and
// the join-point history policy. Asking rather than reimplementing is what stops the
// two drifting apart — the alternative is a second copy of the visibility rule that
// nobody remembers to update.
type Conversations interface {
	// MayAttach reports whether an account may add an attachment to a conversation.
	MayAttach(ctx context.Context, conversationID, accountID string) (bool, error)

	// MayView reports whether an account may see the attachment — by finding the
	// entry that references it and applying the same visibility rule the log uses. An
	// attachment no entry references yet is visible to its owner only.
	MayView(ctx context.Context, conversationID, accountID, attachmentID string) (bool, error)
}

// Notifier tells connected clients that an attachment changed.
//
// Ephemeral and allowed to fail (ADR-0005): a client that misses it re-fetches the
// attachment when it renders, which it must do anyway to obtain variant URLs. So this
// carries no state — only "look again".
type Notifier interface {
	AttachmentChanged(ctx context.Context, conversationID, attachmentID string) error
}
