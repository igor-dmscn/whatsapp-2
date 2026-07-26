// Package app orchestrates Media's use cases: issuing upload URLs, accepting
// completed uploads, deriving variants, and answering what a client may see.
//
// It owns the clock, identifiers, transaction boundaries and publication. It owns no
// rules: what may be uploaded is the aggregate's, and who may upload or view is
// Messaging's, asked through a port.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"comms/internal/media/internal/domain"
	"comms/internal/media/internal/imaging"
	"comms/internal/platform/id"
)

// Clock is injected so expiry can be tested without sleeping.
type Clock func() time.Time

// uploadWindow is how long a signed upload URL stays valid.
//
// Long enough for a hundred megabytes on a slow connection, short enough that a URL
// leaked out of a client's memory is not a standing write grant. A client whose URL
// expires mid-transfer requests another; nothing is lost, because the attachment row
// is already there and only the signature has gone stale.
const uploadWindow = 30 * time.Minute

// viewWindow is how long a signed URL to read a variant stays valid.
//
// Short deliberately. A photo URL is the content: it is unauthenticated once issued,
// so a pasted link should stop working, and every client already re-fetches attachment
// metadata when it renders.
const viewWindow = time.Hour

// renditions are the variants derived from every image.
//
// Two, because a transcript shows thumbnails and opening one shows a bounded copy —
// and because the original is retained (MD-5), so anything else can be derived later
// by replaying the upload event rather than by guessing now what might be wanted.
var renditions = []imaging.Rendition{
	{Name: string(domain.VariantThumbnail), MaxEdge: 320},
	{Name: string(domain.VariantDisplay), MaxEdge: 1280},
}

// Service carries out Media's use cases.
type Service struct {
	attachments   domain.AttachmentRepository
	store         domain.ObjectStore
	conversations domain.Conversations
	notifier      domain.Notifier
	events        domain.EventPublisher
	transactor    domain.Transactor
	now           Clock
	logger        *slog.Logger
}

// NewService wires a Service.
func NewService(
	attachments domain.AttachmentRepository,
	store domain.ObjectStore,
	conversations domain.Conversations,
	notifier domain.Notifier,
	events domain.EventPublisher,
	transactor domain.Transactor,
	now Clock,
	logger *slog.Logger,
) *Service {
	if now == nil {
		now = time.Now
	}
	return &Service{attachments, store, conversations, notifier, events, transactor, now, logger}
}

// Upload is what a client needs to transfer the bytes itself.
type Upload struct {
	AttachmentID string
	// URL and Headers together authorise exactly one request. The headers are part of
	// the signature, so a client that sends different ones is refused by the store —
	// which is how the size cap is enforced without api reading anything (MD-4).
	URL       string
	Method    string
	Headers   map[string]string
	ExpiresAt time.Time
}

// RequestUpload records the intent to attach something and returns where to put it.
//
// Nothing is transferred through this process, now or later. The reason the row is
// written before the bytes exist is that the identifier has to be handed out first:
// the entry referencing the attachment is sent while the upload is still running, so a
// message with a 90 MB video is readable immediately (MD-1).
func (s *Service) RequestUpload(
	ctx context.Context,
	actor domain.AccountID,
	conversationID domain.ConversationID,
	contentType string,
	byteSize int64,
) (Upload, error) {
	// Authorisation before validation, so that someone with no business in this
	// conversation learns nothing about what it would have accepted.
	permitted, err := s.conversations.MayAttach(ctx, string(conversationID), string(actor))
	if err != nil {
		return Upload{}, fmt.Errorf("check permission to attach: %w", err)
	}
	if !permitted {
		return Upload{}, domain.ErrNotPermitted
	}

	attachment, err := domain.NewAttachment(
		domain.AttachmentID(id.New()), conversationID, actor, contentType, byteSize, s.now())
	if err != nil {
		return Upload{}, err
	}

	if err := s.attachments.Save(ctx, attachment); err != nil {
		return Upload{}, fmt.Errorf("save attachment: %w", err)
	}

	// content-length and content-type are signed, which makes the declaration binding:
	// the store refuses a body of a different size or type, so the cap holds on a
	// request this process never sees.
	headers := map[string]string{
		"content-type":   contentType,
		"content-length": strconv.FormatInt(byteSize, 10),
	}

	return Upload{
		AttachmentID: string(attachment.ID()),
		URL:          s.store.Presign(http.MethodPut, attachment.ObjectKey(), headers, uploadWindow),
		Method:       http.MethodPut,
		Headers:      headers,
		ExpiresAt:    s.now().Add(uploadWindow),
	}, nil
}

// CompleteUpload accepts that the bytes have arrived and enqueues the processing job.
//
// The size is checked against the store rather than trusted, which catches the one
// direction a signed URL cannot: a transfer that stopped early. A larger body was
// already refused by the store.
func (s *Service) CompleteUpload(
	ctx context.Context,
	actor domain.AccountID,
	attachmentID domain.AttachmentID,
) (*domain.Attachment, error) {
	attachment, err := s.attachments.Find(ctx, attachmentID)
	if err != nil {
		return nil, err
	}
	// Only the uploader completes their own upload. Not a membership question: someone
	// else in the conversation has no business declaring another account's transfer
	// finished.
	if attachment.OwnerID() != actor {
		return nil, domain.ErrNotPermitted
	}

	stored, found, err := s.store.Size(ctx, attachment.ObjectKey())
	if err != nil {
		return nil, fmt.Errorf("check uploaded object: %w", err)
	}
	if !found {
		return nil, domain.ErrNotUploadedYet
	}

	if err := attachment.MarkUploaded(stored, s.now()); err != nil {
		if errors.Is(err, domain.ErrSizeMismatch) {
			// A truncated upload leaves an object nobody will ever read. Removed here
			// rather than by a sweeper, because this is the moment it is known to be
			// junk and the client is about to retry with a fresh attachment.
			if deleteErr := s.store.Delete(ctx, attachment.ObjectKey()); deleteErr != nil {
				s.logger.Warn("remove truncated upload",
					slog.String("attachment_id", string(attachmentID)), slog.Any("error", deleteErr))
			}
		}
		return nil, err
	}

	// The state change and the processing job commit together (ADR-0003). This is what
	// makes MD-3 true: a worker crash cannot lose a job, because the job is a row in
	// the same transaction as the upload it describes.
	if err := s.atomically(ctx, func(ctx context.Context) error {
		if err := s.attachments.Save(ctx, attachment); err != nil {
			return fmt.Errorf("save attachment: %w", err)
		}
		return s.publish(ctx, attachment.TakeEvents())
	}); err != nil {
		return nil, err
	}

	return attachment, nil
}

// View is an attachment as a client needs it: the state, and where to read the bytes.
type View struct {
	Attachment *domain.Attachment
	// OriginalURL is the retained upload. Present for every uploaded attachment,
	// because video has no derived renditions and a client must still be able to play
	// it.
	OriginalURL string
	// VariantURLs is keyed by variant name, empty until the attachment is ready.
	VariantURLs map[string]string
	ExpiresAt   time.Time
}

// Attachment returns what a client may see, or ErrNotPermitted.
//
// The URLs are signed and short-lived rather than the bytes being proxied: a photo
// served through api would put every megabyte on the request path, which is the one
// thing this context is arranged to avoid.
func (s *Service) Attachment(
	ctx context.Context,
	actor domain.AccountID,
	attachmentID domain.AttachmentID,
) (View, error) {
	attachment, err := s.attachments.Find(ctx, attachmentID)
	if err != nil {
		return View{}, err
	}

	permitted, err := s.conversations.MayView(
		ctx, string(attachment.ConversationID()), string(actor), string(attachmentID))
	if err != nil {
		return View{}, fmt.Errorf("check permission to view: %w", err)
	}
	// The owner may always see their own attachment, including before any entry
	// references it — which is what lets a client show what it is uploading.
	if !permitted && attachment.OwnerID() != actor {
		return View{}, domain.ErrNotPermitted
	}

	view := View{
		Attachment:  attachment,
		VariantURLs: make(map[string]string, len(attachment.Variants())),
		ExpiresAt:   s.now().Add(viewWindow),
	}
	if attachment.State() != domain.StatePending {
		view.OriginalURL = s.store.Presign(http.MethodGet, attachment.ObjectKey(), nil, viewWindow)
	}
	for _, variant := range attachment.Variants() {
		view.VariantURLs[string(variant.Name)] = s.store.Presign(
			http.MethodGet, variant.ObjectKey, nil, viewWindow)
	}
	return view, nil
}

// Process derives an attachment's variants and makes it displayable.
//
// The worker's use case, driven by the AttachmentUploaded event. Written to be run
// more than once on the same attachment, because at-least-once delivery guarantees it
// will be: the variants are keyed by name so a second pass overwrites the first
// (MD-2), and the aggregate accepts being marked ready when it already is.
func (s *Service) Process(ctx context.Context, attachmentID domain.AttachmentID) error {
	attachment, err := s.attachments.Find(ctx, attachmentID)
	if err != nil {
		return err
	}

	// A replay of an event whose upload has since been superseded — or an attachment
	// that failed permanently — is not work to do. Returning nil rather than an error
	// is what lets the consumer commit the record and move on.
	if attachment.State() == domain.StateFailed {
		return nil
	}
	if attachment.State() == domain.StatePending {
		s.logger.Warn("processing an attachment whose bytes are not in the store",
			slog.String("attachment_id", string(attachmentID)))
		return nil
	}

	variants, err := s.derive(ctx, attachment)
	if err != nil {
		// Content that cannot be decoded will never decode. Recorded as a terminal
		// failure so the client shows something honest and the consumer is not wedged
		// retrying one bad upload forever — which is not what MD-3 asks for. MD-3 is
		// about crashes, and a crash leaves the record uncommitted and replayable.
		if errors.Is(err, imaging.ErrUndecodable) {
			return s.fail(ctx, attachment, err)
		}
		return err
	}

	if err := attachment.MarkReady(variants, s.now()); err != nil {
		return err
	}
	if err := s.attachments.Save(ctx, attachment); err != nil {
		return fmt.Errorf("save attachment: %w", err)
	}

	// Told, not stored: the notification carries no state, only "look again". A client
	// that misses it discovers readiness the next time it renders the attachment,
	// which it must fetch for the variant URLs anyway (ADR-0005).
	if err := s.notifier.AttachmentChanged(
		ctx, string(attachment.ConversationID()), string(attachment.ID())); err != nil {
		// Deliberately not fatal to the job. Failing here would replay the whole
		// derivation to retry a message whose loss costs a client one refresh.
		s.logger.Warn("notify attachment ready",
			slog.String("attachment_id", string(attachmentID)), slog.Any("error", err))
	}

	s.logger.Info("attachment ready",
		slog.String("attachment_id", string(attachmentID)),
		slog.Int("variants", len(variants)))
	return nil
}

// derive produces and stores the variants for an attachment.
//
// Video gets none: deriving a poster frame or a smaller copy needs a transcoder, which
// is a native dependency and a different scaling problem from everything else here. It
// still passes through this path so that its lifecycle is identical to a photo's and
// clients have one shape of state to handle.
func (s *Service) derive(ctx context.Context, attachment *domain.Attachment) ([]domain.Variant, error) {
	if !attachment.IsImage() {
		return nil, nil
	}

	original, err := s.store.Get(ctx, attachment.ObjectKey())
	if err != nil {
		return nil, fmt.Errorf("read original: %w", err)
	}

	results, err := imaging.Derive(original, renditions)
	if err != nil {
		return nil, err //nolint:wrapcheck // the sentinel is what the caller switches on.
	}

	variants := make([]domain.Variant, 0, len(results))
	for _, result := range results {
		// Keyed by variant name, so the same attachment processed twice writes to the
		// same objects. A key with anything unique in it would leave the store filling
		// up with orphaned copies on every replay.
		key := "attachments/" + string(attachment.ID()) + "/" + result.Name
		if err := s.store.Put(ctx, key, result.ContentType, result.Body); err != nil {
			return nil, fmt.Errorf("write variant %s: %w", result.Name, err)
		}
		variants = append(variants, domain.Variant{
			Name:        domain.VariantName(result.Name),
			ContentType: result.ContentType,
			ObjectKey:   key,
			Width:       result.Width,
			Height:      result.Height,
			ByteSize:    int64(len(result.Body)),
		})
	}
	return variants, nil
}

// fail records a permanent processing failure and tells clients to look again.
func (s *Service) fail(ctx context.Context, attachment *domain.Attachment, reason error) error {
	if err := attachment.MarkFailed(reason.Error(), s.now()); err != nil {
		return err
	}
	if err := s.attachments.Save(ctx, attachment); err != nil {
		return fmt.Errorf("save failed attachment: %w", err)
	}

	if err := s.notifier.AttachmentChanged(
		ctx, string(attachment.ConversationID()), string(attachment.ID())); err != nil {
		s.logger.Warn("notify attachment failed",
			slog.String("attachment_id", string(attachment.ID())), slog.Any("error", err))
	}

	s.logger.Warn("attachment could not be processed",
		slog.String("attachment_id", string(attachment.ID())),
		slog.String("content_type", attachment.ContentType()),
		slog.Any("error", reason))
	return nil
}

func (s *Service) atomically(ctx context.Context, work func(context.Context) error) error {
	return s.transactor.InTransaction(ctx, work) //nolint:wrapcheck // the closure's error is the caller's own.
}

// publish records events for publication, in the caller's transaction.
func (s *Service) publish(ctx context.Context, events []domain.Event) error {
	if len(events) == 0 {
		return nil
	}
	if err := s.events.Publish(ctx, events); err != nil {
		return fmt.Errorf("publish events: %w", err)
	}
	return nil
}
