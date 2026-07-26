// Package domain_test covers the attachment lifecycle, which is where the media
// requirements live: what may be uploaded, when it becomes displayable, and what
// happens when the same thing is done twice.
package domain_test

import (
	"errors"
	"testing"
	"time"

	"comms/internal/media/internal/domain"
)

var (
	conversation = domain.ConversationID("conversation-1")
	owner        = domain.AccountID("account-1")
	at           = time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
)

func requested(t *testing.T, contentType string, size int64) *domain.Attachment {
	t.Helper()

	attachment, err := domain.NewAttachment("attachment-1", conversation, owner, contentType, size, at)
	if err != nil {
		t.Fatalf("request upload: %v", err)
	}
	return attachment
}

func TestARequestedAttachmentIsPendingAndKnowsWhereItsBytesGo(t *testing.T) {
	t.Parallel()
	attachment := requested(t, "image/jpeg", 4096)

	if attachment.State() != domain.StatePending {
		t.Fatalf("got state %q, want pending", attachment.State())
	}
	if attachment.Displayable() {
		t.Fatal("an attachment with no bytes is displayable")
	}
	if attachment.ObjectKey() != "attachments/attachment-1/original" {
		t.Fatalf("got object key %q", attachment.ObjectKey())
	}
	// Nothing has happened yet worth telling anyone about: the job exists once the
	// bytes do.
	if events := attachment.TakeEvents(); len(events) != 0 {
		t.Fatalf("got %d events on request, want none", len(events))
	}
}

// TestTheSizeLimitIsRefusedBeforeAnyTransfer is MD-4 at the earliest point it can be
// enforced: the request is refused, so no URL is issued and nothing is transferred.
func TestTheSizeLimitIsRefusedBeforeAnyTransfer(t *testing.T) {
	t.Parallel()

	_, err := domain.NewAttachment("a", conversation, owner, "video/mp4", domain.MaxBytes+1, at)
	if !errors.Is(err, domain.ErrTooLarge) {
		t.Fatalf("got %v, want ErrTooLarge", err)
	}

	// The limit itself is allowed: a cap that refuses exactly its own value is a
	// different cap.
	if _, err := domain.NewAttachment("a", conversation, owner, "video/mp4", domain.MaxBytes, at); err != nil {
		t.Fatalf("an attachment of exactly the limit was refused: %v", err)
	}
}

func TestOnlyPhotosAndVideoMayBeUploaded(t *testing.T) {
	t.Parallel()

	for _, contentType := range []string{"application/pdf", "text/html", "application/octet-stream", ""} {
		if _, err := domain.NewAttachment("a", conversation, owner, contentType, 10, at); err == nil {
			t.Fatalf("%q was accepted", contentType)
		}
	}

	// A browser recording declares its codecs in the type, and the codec list is not
	// what decides whether the type is allowed.
	if _, err := domain.NewAttachment("a", conversation, owner, "video/webm;codecs=vp8,opus", 10, at); err != nil {
		t.Fatalf("a browser recording was refused: %v", err)
	}
}

func TestCompletingAnUploadRaisesTheProcessingJob(t *testing.T) {
	t.Parallel()
	attachment := requested(t, "image/png", 4096)

	if err := attachment.MarkUploaded(4096, at); err != nil {
		t.Fatalf("mark uploaded: %v", err)
	}
	if attachment.State() != domain.StateUploaded {
		t.Fatalf("got state %q, want uploaded", attachment.State())
	}

	events := attachment.TakeEvents()
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	uploaded, ok := events[0].(domain.AttachmentUploaded)
	if !ok {
		t.Fatalf("got %T, want AttachmentUploaded", events[0])
	}
	// The job is self-contained: a consumer can act on it without reading the row
	// back, which is what makes a replay independent of what has happened since.
	if uploaded.ObjectKey != attachment.ObjectKey() || uploaded.ContentType != "image/png" {
		t.Fatalf("the event does not carry what a processor needs: %+v", uploaded)
	}
}

// TestCompletingTwiceRaisesOneJob is why a lost response is harmless. The client
// retries, and the retry neither fails nor enqueues a second processing job.
func TestCompletingTwiceRaisesOneJob(t *testing.T) {
	t.Parallel()
	attachment := requested(t, "image/png", 10)

	if err := attachment.MarkUploaded(10, at); err != nil {
		t.Fatalf("first completion: %v", err)
	}
	attachment.TakeEvents()

	if err := attachment.MarkUploaded(10, at); err != nil {
		t.Fatalf("second completion: %v", err)
	}
	if events := attachment.TakeEvents(); len(events) != 0 {
		t.Fatalf("the retry raised %d events", len(events))
	}
}

// TestATruncatedUploadIsRefused covers the one direction the store cannot catch: a
// larger body fails the signature, a smaller one succeeds and leaves a broken object.
func TestATruncatedUploadIsRefused(t *testing.T) {
	t.Parallel()
	attachment := requested(t, "image/jpeg", 4096)

	if err := attachment.MarkUploaded(2048, at); !errors.Is(err, domain.ErrSizeMismatch) {
		t.Fatalf("got %v, want ErrSizeMismatch", err)
	}
	if attachment.State() != domain.StatePending {
		t.Fatalf("a refused completion moved the state to %q", attachment.State())
	}
}

func TestVariantsMakeAnAttachmentDisplayable(t *testing.T) {
	t.Parallel()
	attachment := requested(t, "image/jpeg", 4096)
	if err := attachment.MarkUploaded(4096, at); err != nil {
		t.Fatalf("mark uploaded: %v", err)
	}

	variants := []domain.Variant{
		{Name: domain.VariantThumbnail, ContentType: "image/jpeg", ObjectKey: "k1", Width: 320, Height: 240},
	}
	if err := attachment.MarkReady(variants, at); err != nil {
		t.Fatalf("mark ready: %v", err)
	}
	if !attachment.Displayable() {
		t.Fatal("a ready attachment is not displayable")
	}
	if attachment.ReadyAt().IsZero() {
		t.Fatal("ready with no time")
	}
}

// TestReprocessingReplacesRatherThanAccumulates is MD-2 in the aggregate. The
// constraint in the database is the other half; this is the rule that makes a second
// pass legal at all.
func TestReprocessingReplacesRatherThanAccumulates(t *testing.T) {
	t.Parallel()
	attachment := requested(t, "image/jpeg", 4096)
	if err := attachment.MarkUploaded(4096, at); err != nil {
		t.Fatalf("mark uploaded: %v", err)
	}

	variants := []domain.Variant{
		{Name: domain.VariantThumbnail, ObjectKey: "k1"},
		{Name: domain.VariantDisplay, ObjectKey: "k2"},
	}
	if err := attachment.MarkReady(variants, at); err != nil {
		t.Fatalf("first pass: %v", err)
	}
	if err := attachment.MarkReady(variants, at.Add(time.Minute)); err != nil {
		t.Fatalf("second pass: %v", err)
	}
	if got := len(attachment.Variants()); got != 2 {
		t.Fatalf("got %d variants after two passes, want 2", got)
	}
}

func TestVariantsCannotBeRecordedForBytesThatAreNotThere(t *testing.T) {
	t.Parallel()
	attachment := requested(t, "image/jpeg", 4096)

	if err := attachment.MarkReady(nil, at); !errors.Is(err, domain.ErrNotUploaded) {
		t.Fatalf("got %v, want ErrNotUploaded", err)
	}
}

// TestFailureIsTerminalButCannotUndoReadiness stops a redelivered record that fails
// on the second pass from taking a photo away from clients already showing it.
func TestFailureIsTerminalButCannotUndoReadiness(t *testing.T) {
	t.Parallel()
	attachment := requested(t, "image/jpeg", 4096)
	if err := attachment.MarkUploaded(4096, at); err != nil {
		t.Fatalf("mark uploaded: %v", err)
	}

	if err := attachment.MarkFailed("not an image", at); err != nil {
		t.Fatalf("mark failed: %v", err)
	}
	if attachment.State() != domain.StateFailed || attachment.Failure() == "" {
		t.Fatalf("got state %q failure %q", attachment.State(), attachment.Failure())
	}

	ready := requested(t, "image/jpeg", 4096)
	if err := ready.MarkUploaded(4096, at); err != nil {
		t.Fatalf("mark uploaded: %v", err)
	}
	if err := ready.MarkReady(nil, at); err != nil {
		t.Fatalf("mark ready: %v", err)
	}
	if err := ready.MarkFailed("late failure", at); !errors.Is(err, domain.ErrAlreadyReady) {
		t.Fatalf("got %v, want ErrAlreadyReady", err)
	}
}

// TestVideoIsNotProcessedIntoVariants records the deliberate limit: video keeps the
// same lifecycle so clients have one shape of state to handle, and gets no derived
// renditions because that needs a transcoder.
func TestVideoIsNotProcessedIntoVariants(t *testing.T) {
	t.Parallel()

	if requested(t, "video/mp4", 4096).IsImage() {
		t.Fatal("video is treated as an image")
	}
	if !requested(t, "image/webp", 4096).IsImage() {
		t.Fatal("a photo is not treated as an image")
	}
}

// TestVariantsAreCopiedOut keeps a caller from rewriting what the aggregate holds,
// which is the point of unexported fields.
func TestVariantsAreCopiedOut(t *testing.T) {
	t.Parallel()
	attachment := requested(t, "image/jpeg", 10)
	if err := attachment.MarkUploaded(10, at); err != nil {
		t.Fatalf("mark uploaded: %v", err)
	}
	if err := attachment.MarkReady([]domain.Variant{{Name: domain.VariantThumbnail}}, at); err != nil {
		t.Fatalf("mark ready: %v", err)
	}

	attachment.Variants()[0].ObjectKey = "somewhere else"
	if attachment.Variants()[0].ObjectKey != "" {
		t.Fatal("a caller rewrote the aggregate's variants")
	}
}
