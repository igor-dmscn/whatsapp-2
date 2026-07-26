// Package api exposes Media over HTTP.
//
// Three endpoints and no bytes. Requesting an upload returns where to put it,
// completing one says the transfer finished, and reading one returns signed URLs. The
// content itself goes client-to-store and store-to-client, never through here.
package api

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"comms/internal/media/internal/app"
	"comms/internal/media/internal/domain"
	"comms/internal/platform/httpx"
)

// CallerResolver reports the authenticated account on a request.
//
// A port rather than an import: Media does not depend on Identity, so cmd/api supplies
// this, exactly as Messaging does.
type CallerResolver func(ctx context.Context) (accountID string, deviceID string)

// Handler serves the media endpoints.
type Handler struct {
	service *app.Service
	caller  CallerResolver
	logger  *slog.Logger
}

// NewHandler returns a handler.
func NewHandler(service *app.Service, caller CallerResolver, logger *slog.Logger) *Handler {
	return &Handler{service: service, caller: caller, logger: logger}
}

// Routes registers Media's endpoints. All of them need a bearer token: there is no
// unauthenticated path to an attachment, and the signed URLs are the only thing that
// can be used without one.
func (h *Handler) Routes(mux *http.ServeMux, authenticated func(http.Handler) http.Handler) {
	mux.Handle("POST /v1/attachments", authenticated(http.HandlerFunc(h.requestUpload)))
	mux.Handle("POST /v1/attachments/{attachmentID}/completion", authenticated(http.HandlerFunc(h.completeUpload)))
	mux.Handle("GET /v1/attachments/{attachmentID}", authenticated(http.HandlerFunc(h.getAttachment)))
}

type requestUploadRequest struct {
	ConversationID string `json:"conversation_id"`
	ContentType    string `json:"content_type"`
	// ByteSize is what the client is about to send, declared before it sends it. This
	// is what gets signed into the upload URL, which is what makes the cap enforceable
	// without reading anything (MD-4).
	ByteSize int64 `json:"byte_size"`
}

type uploadResponse struct {
	AttachmentID string            `json:"attachment_id"`
	URL          string            `json:"url"`
	Method       string            `json:"method"`
	Headers      map[string]string `json:"headers"`
	ExpiresAt    time.Time         `json:"expires_at"`
}

type variantResponse struct {
	Name        string `json:"name"`
	ContentType string `json:"content_type"`
	URL         string `json:"url"`
	Width       int    `json:"width"`
	Height      int    `json:"height"`
	ByteSize    int64  `json:"byte_size"`
}

type attachmentResponse struct {
	ID             string `json:"id"`
	ConversationID string `json:"conversation_id"`
	OwnerID        string `json:"owner_id"`
	ContentType    string `json:"content_type"`
	ByteSize       int64  `json:"byte_size"`
	// State is what a client renders by: pending and uploaded are placeholders, ready
	// shows the thumbnail, failed shows that it could not be processed (MD-1).
	State string `json:"state"`
	// Failure is why, present only when the state is failed. A client showing "this
	// photo could not be processed" is more use than one showing a spinner forever.
	Failure     string            `json:"failure,omitempty"`
	OriginalURL string            `json:"original_url,omitempty"`
	Variants    []variantResponse `json:"variants"`
	// ExpiresAt is when the URLs above stop working, so a client knows to re-fetch
	// rather than discovering it from a 403 on an image it is trying to show.
	ExpiresAt time.Time `json:"expires_at"`
	CreatedAt time.Time `json:"created_at"`
}

func newAttachmentResponse(view app.View) attachmentResponse {
	attachment := view.Attachment

	variants := make([]variantResponse, 0, len(attachment.Variants()))
	for _, variant := range attachment.Variants() {
		variants = append(variants, variantResponse{
			Name:        string(variant.Name),
			ContentType: variant.ContentType,
			URL:         view.VariantURLs[string(variant.Name)],
			Width:       variant.Width,
			Height:      variant.Height,
			ByteSize:    variant.ByteSize,
		})
	}

	return attachmentResponse{
		ID:             string(attachment.ID()),
		ConversationID: string(attachment.ConversationID()),
		OwnerID:        string(attachment.OwnerID()),
		ContentType:    attachment.ContentType(),
		ByteSize:       attachment.ByteSize(),
		State:          string(attachment.State()),
		Failure:        attachment.Failure(),
		OriginalURL:    view.OriginalURL,
		Variants:       variants,
		ExpiresAt:      view.ExpiresAt,
		CreatedAt:      attachment.CreatedAt(),
	}
}

func (h *Handler) requestUpload(w http.ResponseWriter, r *http.Request) {
	accountID, _ := h.caller(r.Context())

	var request requestUploadRequest
	if err := httpx.DecodeJSON(r, &request); err != nil {
		h.fail(w, http.StatusBadRequest, "malformed_body", err.Error())
		return
	}

	upload, err := h.service.RequestUpload(
		r.Context(), domain.AccountID(accountID), domain.ConversationID(request.ConversationID),
		request.ContentType, request.ByteSize,
	)
	if err != nil {
		h.writeDomainError(w, err)
		return
	}

	// 201: the attachment now exists, pending its bytes. A client may reference it in
	// an entry from this moment, which is what makes a message carrying a large video
	// readable before the video arrives (MD-1).
	httpx.WriteJSON(w, h.logger, http.StatusCreated, uploadResponse{
		AttachmentID: upload.AttachmentID,
		URL:          upload.URL,
		Method:       upload.Method,
		Headers:      upload.Headers,
		ExpiresAt:    upload.ExpiresAt,
	})
}

func (h *Handler) completeUpload(w http.ResponseWriter, r *http.Request) {
	accountID, _ := h.caller(r.Context())

	attachment, err := h.service.CompleteUpload(
		r.Context(), domain.AccountID(accountID), domain.AttachmentID(r.PathValue("attachmentID")))
	if err != nil {
		h.writeDomainError(w, err)
		return
	}

	// 202: the bytes are accepted and the variants do not exist yet. Not 200, because
	// the work this triggers has not happened — the same distinction the receipt
	// endpoint makes for a projection that has not caught up.
	httpx.WriteJSON(w, h.logger, http.StatusAccepted, map[string]string{
		"attachment_id": string(attachment.ID()),
		"state":         string(attachment.State()),
	})
}

func (h *Handler) getAttachment(w http.ResponseWriter, r *http.Request) {
	accountID, _ := h.caller(r.Context())

	view, err := h.service.Attachment(
		r.Context(), domain.AccountID(accountID), domain.AttachmentID(r.PathValue("attachmentID")))
	if err != nil {
		h.writeDomainError(w, err)
		return
	}

	httpx.WriteJSON(w, h.logger, http.StatusOK, newAttachmentResponse(view))
}

func (h *Handler) writeDomainError(w http.ResponseWriter, err error) {
	var validation domain.ValidationError
	switch {
	case errors.As(err, &validation):
		httpx.WriteError(w, h.logger, http.StatusUnprocessableEntity, httpx.ErrorBody{
			Code: "invalid_field", Message: validation.Reason, Field: validation.Field,
		})

	case errors.Is(err, domain.ErrTooLarge):
		// 413 before a byte has moved. The client asked to send too much and was told
		// so on the request that would have authorised the transfer (MD-4).
		h.fail(w, http.StatusRequestEntityTooLarge, "attachment_too_large",
			"attachments are limited to 100 MB")

	case errors.Is(err, domain.ErrAttachmentNotFound), errors.Is(err, domain.ErrNotPermitted):
		// One status for both, deliberately: an account with no claim on an attachment
		// must not be able to tell whether it exists.
		h.fail(w, http.StatusNotFound, "attachment_not_found", "no such attachment")

	case errors.Is(err, domain.ErrNotUploadedYet):
		h.fail(w, http.StatusConflict, "not_uploaded", "the bytes have not arrived")

	case errors.Is(err, domain.ErrSizeMismatch):
		h.fail(w, http.StatusConflict, "size_mismatch",
			"fewer bytes arrived than were declared; request a new upload")

	case errors.Is(err, domain.ErrNotPending):
		h.fail(w, http.StatusConflict, "not_pending", "this upload has already been completed")

	default:
		h.logger.Error("media request failed", slog.Any("error", err))
		h.fail(w, http.StatusInternalServerError, "internal_error", "something went wrong")
	}
}

func (h *Handler) fail(w http.ResponseWriter, status int, code, message string) {
	httpx.WriteError(w, h.logger, status, httpx.ErrorBody{Code: code, Message: message})
}
