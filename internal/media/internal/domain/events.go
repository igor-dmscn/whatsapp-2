package domain

import "time"

// Event is a fact: something that happened, named in the past tense.
//
// Recorded by aggregates and written to the outbox in the same transaction as the
// change they describe (ADR-0003), which is what makes a processing job impossible to
// lose without also losing the upload it describes.
type Event interface {
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
func (r *recorder) TakeEvents() []Event {
	taken := r.events
	r.events = nil
	return taken
}

type occurred struct {
	At time.Time
}

func (o occurred) OccurredAt() time.Time { return o.At }

// AttachmentUploaded is raised when the bytes have arrived and are unprocessed.
//
// This is the processing job. It carries everything the worker needs — the object key
// and the content type — so that a consumer does not have to read the row back before
// it can decide what to do, and so a replay is self-contained.
type AttachmentUploaded struct {
	occurred
	AttachmentID   AttachmentID
	ConversationID ConversationID
	OwnerID        AccountID
	ContentType    string
	ByteSize       int64
	ObjectKey      string
}

func (AttachmentUploaded) EventName() string { return "media.attachment_uploaded" }
