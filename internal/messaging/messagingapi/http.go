// Package transport exposes messaging over HTTP and WebSocket.
//
// Sending is an HTTP request rather than a socket frame, deliberately. A send needs
// a response its author can act on — the assigned position, or a rejection — and
// request/response is what that is. The socket carries what the server initiates.
package messagingapi

import (
	"context"
	"encoding/base64"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"comms/internal/messaging"
	"comms/internal/messaging/messagingapp"
	"comms/internal/platform/httpx"
	"comms/internal/platform/logging"
)

// CallerResolver reports the authenticated account and device on a request.
//
// A port rather than an import, for the same reason as Authenticator: Messaging
// does not depend on Identity, so cmd/api supplies this.
type CallerResolver func(ctx context.Context) (accountID string, deviceID string)

// Handler serves the messaging endpoints.
type Handler struct {
	service        *messagingapp.Service
	hub            *Hub
	authenticator  Authenticator
	caller         CallerResolver
	logger         *slog.Logger
	allowedOrigins []string
}

// NewHandler returns a handler.
func NewHandler(
	service *messagingapp.Service,
	hub *Hub,
	authenticator Authenticator,
	caller CallerResolver,
	allowedOrigins []string,
	logger *slog.Logger,
) *Handler {
	return &Handler{
		service:        service,
		hub:            hub,
		authenticator:  authenticator,
		caller:         caller,
		logger:         logger,
		allowedOrigins: allowedOrigins,
	}
}

// Routes registers messaging endpoints. authenticated wraps handlers needing a
// bearer token; the socket authenticates itself in its first frame.
func (h *Handler) Routes(mux *http.ServeMux, authenticated func(http.Handler) http.Handler) {
	mux.HandleFunc("GET /v1/socket", h.Socket)

	mux.Handle("GET /v1/conversations", authenticated(http.HandlerFunc(h.listConversations)))
	mux.Handle("POST /v1/conversations/direct", authenticated(http.HandlerFunc(h.startDirect)))
	mux.Handle("GET /v1/conversations/{conversationID}", authenticated(http.HandlerFunc(h.getConversation)))
	mux.Handle("GET /v1/conversations/{conversationID}/entries", authenticated(http.HandlerFunc(h.listEntries)))
	mux.Handle("POST /v1/conversations/{conversationID}/entries", authenticated(http.HandlerFunc(h.send)))
}

// --- bodies ---

type startDirectRequest struct {
	AccountID string `json:"account_id"`
}

type sendRequest struct {
	ClientEntryID string `json:"client_entry_id"`
	ContentType   string `json:"content_type"`
	// Base64 because the payload is bytes the server does not interpret. Sending
	// it as a JSON string would force an encoding decision the server has no
	// business making (ADR-0001).
	Body string `json:"body"`
}

type conversationResponse struct {
	ID          string    `json:"id"`
	Kind        string    `json:"kind"`
	Head        int64     `json:"head"`
	Role        string    `json:"role"`
	VisibleFrom int64     `json:"visible_from"`
	CreatedAt   time.Time `json:"created_at"`
}

type entryResponse struct {
	ID             string    `json:"id"`
	ConversationID string    `json:"conversation_id"`
	Sequence       int64     `json:"sequence"`
	AuthorID       string    `json:"author_id"`
	ClientEntryID  string    `json:"client_entry_id"`
	Kind           string    `json:"kind"`
	ContentType    string    `json:"content_type"`
	Body           string    `json:"body"`
	CreatedAt      time.Time `json:"created_at"`
}

func newEntryResponse(entry *messaging.Entry) entryResponse {
	return entryResponse{
		ID:             string(entry.ID()),
		ConversationID: string(entry.ConversationID()),
		Sequence:       int64(entry.Sequence()),
		AuthorID:       string(entry.AuthorID()),
		ClientEntryID:  string(entry.ClientEntryID()),
		Kind:           string(entry.Kind()),
		ContentType:    entry.Payload().ContentType(),
		Body:           base64.StdEncoding.EncodeToString(entry.Payload().Body()),
		CreatedAt:      entry.CreatedAt(),
	}
}

// --- handlers ---

func (h *Handler) startDirect(w http.ResponseWriter, r *http.Request) {
	accountID, _ := h.caller(r.Context())

	var request startDirectRequest
	if err := httpx.DecodeJSON(r, &request); err != nil {
		h.fail(w, r, http.StatusBadRequest, "malformed_body", err.Error())
		return
	}

	conversation, err := h.service.StartDirect(r.Context(), messaging.AccountID(accountID), messaging.AccountID(request.AccountID))
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}

	httpx.WriteJSON(w, h.logger, http.StatusCreated, conversationResponse{
		ID:          string(conversation.ID()),
		Kind:        string(conversation.Kind()),
		Head:        int64(conversation.Head()),
		Role:        string(messaging.RoleMember),
		VisibleFrom: int64(messaging.FirstSequence),
		CreatedAt:   conversation.CreatedAt(),
	})
}

func (h *Handler) listConversations(w http.ResponseWriter, r *http.Request) {
	accountID, _ := h.caller(r.Context())

	memberships, err := h.service.Conversations(r.Context(), messaging.AccountID(accountID))
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}

	responses := make([]conversationResponse, 0, len(memberships))
	for _, membership := range memberships {
		if !membership.Active() {
			continue
		}
		conversation, err := h.service.Conversation(r.Context(), membership.ConversationID(), messaging.AccountID(accountID))
		if err != nil {
			h.writeDomainError(w, r, err)
			return
		}
		responses = append(responses, conversationResponse{
			ID:          string(conversation.ID()),
			Kind:        string(conversation.Kind()),
			Head:        int64(conversation.Head()),
			Role:        string(membership.Role()),
			VisibleFrom: int64(membership.VisibleFrom()),
			CreatedAt:   conversation.CreatedAt(),
		})
	}

	httpx.WriteJSON(w, h.logger, http.StatusOK, map[string]any{"conversations": responses})
}

func (h *Handler) getConversation(w http.ResponseWriter, r *http.Request) {
	accountID, _ := h.caller(r.Context())
	conversationID := messaging.ConversationID(r.PathValue("conversationID"))

	conversation, err := h.service.Conversation(r.Context(), conversationID, messaging.AccountID(accountID))
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}
	members, err := h.service.Members(r.Context(), conversationID, messaging.AccountID(accountID))
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}

	memberIDs := make([]string, 0, len(members))
	for _, member := range members {
		memberIDs = append(memberIDs, string(member.AccountID()))
	}

	httpx.WriteJSON(w, h.logger, http.StatusOK, map[string]any{
		"id":         string(conversation.ID()),
		"kind":       string(conversation.Kind()),
		"head":       int64(conversation.Head()),
		"created_at": conversation.CreatedAt(),
		"members":    memberIDs,
	})
}

func (h *Handler) listEntries(w http.ResponseWriter, r *http.Request) {
	accountID, _ := h.caller(r.Context())
	conversationID := messaging.ConversationID(r.PathValue("conversationID"))

	after, err := strconv.ParseInt(r.URL.Query().Get("after"), 10, 64)
	if err != nil || after < 0 {
		// Absent or unparseable means "from the beginning of what I may see".
		after = 0
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))

	entries, err := h.service.Fetch(r.Context(), conversationID, messaging.AccountID(accountID), messaging.Sequence(after), limit)
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}

	responses := make([]entryResponse, 0, len(entries))
	for _, entry := range entries {
		responses = append(responses, newEntryResponse(entry))
	}

	httpx.WriteJSON(w, h.logger, http.StatusOK, map[string]any{"entries": responses})
}

func (h *Handler) send(w http.ResponseWriter, r *http.Request) {
	accountID, _ := h.caller(r.Context())
	conversationID := messaging.ConversationID(r.PathValue("conversationID"))

	var request sendRequest
	if err := httpx.DecodeJSON(r, &request); err != nil {
		h.fail(w, r, http.StatusBadRequest, "malformed_body", err.Error())
		return
	}

	body, err := base64.StdEncoding.DecodeString(request.Body)
	if err != nil {
		h.fail(w, r, http.StatusBadRequest, "malformed_body", "body must be base64")
		return
	}

	entry, err := h.service.Send(
		r.Context(), conversationID, messaging.AccountID(accountID),
		request.ClientEntryID, request.ContentType, body,
	)
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}

	// 201 whether the entry was created now or by an earlier identical attempt.
	// Distinguishing them would tell a retrying client something it cannot use and
	// invite it to treat a successful retry as a failure.
	httpx.WriteJSON(w, h.logger, http.StatusCreated, newEntryResponse(entry))
}

// --- error mapping ---

func (h *Handler) writeDomainError(w http.ResponseWriter, r *http.Request, err error) {
	var validation messaging.ValidationError
	switch {
	case errors.As(err, &validation):
		httpx.WriteError(w, h.logger, http.StatusUnprocessableEntity, httpx.ErrorBody{
			Code: "invalid_field", Message: validation.Reason, Field: validation.Field,
		})

	case errors.Is(err, messaging.ErrNotAMember), errors.Is(err, messaging.ErrConversationNotFound):
		// One status for both: an account that does not belong to a conversation
		// must not be able to tell whether it exists.
		h.fail(w, r, http.StatusNotFound, "conversation_not_found", "no such conversation")

	case errors.Is(err, messaging.ErrNotPermittedToWrite):
		h.fail(w, r, http.StatusForbidden, "not_permitted", "you may not write to this conversation")

	case errors.Is(err, messaging.ErrCannotMessageSelf):
		h.fail(w, r, http.StatusUnprocessableEntity, "cannot_message_self", "you cannot message yourself")

	case errors.Is(err, messaging.ErrDirectConversationIsFull):
		h.fail(w, r, http.StatusConflict, "direct_conversation_full", "a direct conversation has exactly two members")

	case errors.Is(err, messaging.ErrGroupIsFull):
		h.fail(w, r, http.StatusConflict, "group_full", "this group is at its member limit")

	default:
		logging.With(r.Context(), h.logger).Error("unhandled error", slog.Any("error", err))
		h.fail(w, r, http.StatusInternalServerError, "internal_error", "something went wrong")
	}
}

func (h *Handler) fail(w http.ResponseWriter, _ *http.Request, status int, code, message string) {
	httpx.WriteError(w, h.logger, status, httpx.ErrorBody{Code: code, Message: message})
}
