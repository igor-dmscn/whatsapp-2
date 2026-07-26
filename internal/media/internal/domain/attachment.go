// Package domain is Media's model: attachments, their lifecycle, and the variants
// derived from them.
package domain

import (
	"strings"
	"time"
)

// AttachmentID identifies an attachment.
type AttachmentID string

// AccountID and ConversationID are references into other contexts.
//
// Plain identifiers, never Identity's or Messaging's types: Media authorises through
// a port and stores nothing about either. See CONTEXT-MAP.md.
type (
	AccountID      string
	ConversationID string
)

// MaxBytes is the cap on an attachment (MD-4).
//
// Checked when the upload is requested, which is before any transfer starts, and
// bound again by the signed URL — the store refuses a body whose length differs from
// what was signed. So an oversized upload is refused twice and read nowhere.
const MaxBytes int64 = 100 << 20

// State is where an attachment is in its lifecycle.
//
// Three states rather than a ready flag, because "the bytes are not here yet" and
// "the bytes are here but cannot be displayed" are different to a client: the first
// is an upload in progress, the second is a placeholder awaiting a thumbnail (MD-1).
type State string

const (
	// StatePending means a URL has been issued and the bytes have not arrived.
	StatePending State = "pending"
	// StateUploaded means the bytes are in the store and variants are not.
	StateUploaded State = "uploaded"
	// StateReady means every variant exists and can be served.
	StateReady State = "ready"
	// StateFailed means processing will not succeed however often it is retried.
	StateFailed State = "failed"
)

// VariantName names one derived rendition.
type VariantName string

const (
	// VariantThumbnail is what a transcript shows: small enough that a screenful of
	// them is cheaper than one original.
	VariantThumbnail VariantName = "thumbnail"
	// VariantDisplay is what opening an attachment shows — bounded, so a photo
	// straight off a phone is not sent at full sensor resolution to be scaled down
	// by the browser.
	VariantDisplay VariantName = "display"
)

// Variant is one derived rendition of an attachment.
//
// A value, not an entity: variants have no identity of their own and no lifecycle.
// They are regenerable from the retained original at any time (MD-5), which is what
// makes it safe to overwrite them on reprocessing.
type Variant struct {
	Name        VariantName
	ContentType string
	ObjectKey   string
	Width       int
	Height      int
	ByteSize    int64
}

// Attachment is an aggregate root: one photo or video, and the variants derived
// from it.
type Attachment struct {
	recorder

	id             AttachmentID
	conversationID ConversationID
	ownerID        AccountID
	contentType    string
	byteSize       int64
	state          State
	objectKey      string
	failure        string
	variants       []Variant
	createdAt      time.Time
	readyAt        time.Time
}

// NewAttachment records the intent to upload one.
//
// The row exists before the bytes do, deliberately: the identifier has to be handed
// out before the transfer so that the entry referencing it can be sent while the
// upload is still running (MD-1), and so that an abandoned upload is a visible
// pending row rather than an object nobody knows about.
func NewAttachment(
	id AttachmentID,
	conversationID ConversationID,
	ownerID AccountID,
	contentType string,
	byteSize int64,
	now time.Time,
) (*Attachment, error) {
	if id == "" {
		return nil, ValidationError{"id", "must not be empty"}
	}
	if conversationID == "" {
		return nil, ValidationError{"conversation_id", "must not be empty"}
	}
	if ownerID == "" {
		return nil, ValidationError{"owner_id", "must not be empty"}
	}
	if !Supported(contentType) {
		return nil, ValidationError{"content_type", "must be a supported photo or video type"}
	}
	if byteSize <= 0 {
		return nil, ValidationError{"byte_size", "must be greater than zero"}
	}
	if byteSize > MaxBytes {
		return nil, ErrTooLarge
	}

	return &Attachment{
		id:             id,
		conversationID: conversationID,
		ownerID:        ownerID,
		contentType:    contentType,
		byteSize:       byteSize,
		state:          StatePending,
		// Keyed by identifier rather than by anything derived from the content. A
		// content-addressed key would deduplicate, and would also mean one account
		// deleting an attachment could remove another's — which is a feature to decide
		// on, not a side effect to inherit from a naming scheme.
		objectKey: "attachments/" + string(id) + "/original",
		createdAt: now,
	}, nil
}

// ReconstituteAttachment rebuilds one from storage. Only a repository should call
// this, and it validates nothing (ADR-0010).
func ReconstituteAttachment(
	id AttachmentID,
	conversationID ConversationID,
	ownerID AccountID,
	contentType string,
	byteSize int64,
	state State,
	objectKey string,
	failure string,
	variants []Variant,
	createdAt time.Time,
	readyAt time.Time,
) *Attachment {
	return &Attachment{
		id:             id,
		conversationID: conversationID,
		ownerID:        ownerID,
		contentType:    contentType,
		byteSize:       byteSize,
		state:          state,
		objectKey:      objectKey,
		failure:        failure,
		variants:       variants,
		createdAt:      createdAt,
		readyAt:        readyAt,
	}
}

// MarkUploaded accepts that the bytes have arrived, given what the store reports
// holding.
//
// Idempotent: a client retrying the completion call, or calling it after processing
// has already finished, gets success and no second event. Without that, a lost
// response would either duplicate the processing job or leave the attachment pending
// forever depending on which way the client guessed.
func (a *Attachment) MarkUploaded(storedBytes int64, now time.Time) error {
	if a.state == StateUploaded || a.state == StateReady {
		return nil
	}
	if a.state != StatePending {
		return ErrNotPending
	}
	// The declared size is what was signed, so this can only differ if the client
	// uploaded fewer bytes than it promised — a truncated transfer. A larger body was
	// already refused by the store.
	if storedBytes != a.byteSize {
		return ErrSizeMismatch
	}

	a.state = StateUploaded
	a.record(AttachmentUploaded{
		occurred:       occurred{now},
		AttachmentID:   a.id,
		ConversationID: a.conversationID,
		OwnerID:        a.ownerID,
		ContentType:    a.contentType,
		ByteSize:       a.byteSize,
		ObjectKey:      a.objectKey,
	})
	return nil
}

// MarkReady records the variants and makes the attachment displayable.
//
// Accepting an already-ready attachment is what makes reprocessing safe (MD-2): the
// variants are replaced by the identical set and nothing downstream can tell the
// difference. No event is raised — readiness reaches clients over the ephemeral path,
// because a client that misses the notification discovers it on the next fetch and a
// durable event would be a promise nothing needs.
func (a *Attachment) MarkReady(variants []Variant, now time.Time) error {
	if a.state != StateUploaded && a.state != StateReady {
		return ErrNotUploaded
	}

	a.variants = variants
	a.state = StateReady
	a.failure = ""
	a.readyAt = now
	return nil
}

// MarkFailed records that processing cannot succeed.
//
// For content that decodes to nothing — a file claiming to be a photo that is not
// one. Retrying that forever would wedge the consumer, so it is a terminal state
// with the reason attached (MD-3 is about crashes, not about bad content).
func (a *Attachment) MarkFailed(reason string, now time.Time) error {
	if a.state == StateReady {
		return ErrAlreadyReady
	}
	a.state = StateFailed
	a.failure = reason
	a.readyAt = now
	return nil
}

func (a *Attachment) ID() AttachmentID               { return a.id }
func (a *Attachment) ConversationID() ConversationID { return a.conversationID }
func (a *Attachment) OwnerID() AccountID             { return a.ownerID }
func (a *Attachment) ContentType() string            { return a.contentType }
func (a *Attachment) ByteSize() int64                { return a.byteSize }
func (a *Attachment) State() State                   { return a.state }
func (a *Attachment) ObjectKey() string              { return a.objectKey }
func (a *Attachment) Failure() string                { return a.failure }
func (a *Attachment) CreatedAt() time.Time           { return a.createdAt }
func (a *Attachment) ReadyAt() time.Time             { return a.readyAt }

// Variants returns a copy, so a caller cannot rewrite what the aggregate holds.
func (a *Attachment) Variants() []Variant {
	return append([]Variant(nil), a.variants...)
}

// IsImage reports whether variants can be derived from this content.
//
// Video is stored and served as uploaded. Deriving a poster frame or a lower-bitrate
// copy needs a transcoder — a native dependency, a process pool and a queue with
// different scaling properties from everything else here — so the client renders
// video with the player's own first frame instead. The lifecycle is unchanged: a video
// still goes pending, uploaded, ready, so there is one shape of state for a client to
// handle. See docs/plan.md phase 7.
func (a *Attachment) IsImage() bool { return strings.HasPrefix(a.contentType, "image/") }

// Displayable reports whether a client can render this attachment now.
func (a *Attachment) Displayable() bool { return a.state == StateReady }

// supported is what may be uploaded.
//
// An allow list, not a deny list: the store makes the declared type binding, and
// clients render by it, so anything not on this list is something no client knows how
// to show and the worker cannot process.
var supported = map[string]bool{
	"image/jpeg":      true,
	"image/png":       true,
	"image/gif":       true,
	"image/webp":      true,
	"video/mp4":       true,
	"video/webm":      true,
	"video/quicktime": true,
}

// Supported reports whether a content type may be uploaded.
func Supported(contentType string) bool {
	// Parameters are stripped: browsers send "video/webm;codecs=vp8" for a recording,
	// and the codec list is not what decides whether the type is allowed.
	if index := strings.IndexByte(contentType, ';'); index >= 0 {
		contentType = contentType[:index]
	}
	return supported[strings.ToLower(strings.TrimSpace(contentType))]
}
