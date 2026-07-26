// Package transport exposes messaging over HTTP and WebSocket.
//
// Sending is an HTTP request rather than a socket frame, deliberately. A send needs
// a response its author can act on — the assigned position, or a rejection — and
// request/response is what that is. The socket carries what the server initiates.
package api

import (
	"context"
	"encoding/base64"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"comms/internal/messaging/internal/app"
	"comms/internal/messaging/internal/domain"
	"comms/internal/platform/httpx"
	"comms/internal/platform/logging"
	"comms/internal/platform/ratelimit"
)

// CallerResolver reports the authenticated account and device on a request.
//
// A port rather than an import, for the same reason as Authenticator: Messaging
// does not depend on Identity, so cmd/api supplies this.
type CallerResolver func(ctx context.Context) (accountID string, deviceID string)

// FrameHandler takes socket frames Messaging does not own.
//
// The mechanism behind ADR-0004's one socket per client: a second context can carry its own
// protocol over the connection a client already has, without Messaging learning what that
// protocol means. Registered per family — everything before the first dot.
type FrameHandler interface {
	// HandleFrame is given the raw frame, so the handler decodes its own shapes. Returning
	// an error sends the client an error frame and leaves the connection open: a call that
	// cannot be joined must not cost somebody their messages.
	HandleFrame(ctx context.Context, session Session, frameType string, raw []byte) error

	// SocketClosed lets a handler clean up after a client that vanished. A crashed tab is
	// a participant who is gone whether or not it said so.
	SocketClosed(ctx context.Context, session Session)
}

// Session is what a delegated handler may do with the connection it was given.
//
// Deliberately narrow: who is on it, and how to reply. A handler with the whole Connection
// could change what the client is subscribed to, which is Messaging's business alone.
type Session interface {
	AccountID() string
	DeviceID() string
	// Send writes a frame to this connection. Non-blocking; a socket too far behind is
	// closed rather than allowed to grow memory on the server.
	Send(frame any) error
}

// Handler serves the messaging endpoints.
type Handler struct {
	service        *app.Service
	hub            *Hub
	authenticator  Authenticator
	caller         CallerResolver
	logger         *slog.Logger
	allowedOrigins []string
	limiter        *ratelimit.Limiter

	// frameHandlers is what other contexts registered, by family.
	frameMutex    sync.RWMutex
	frameHandlers map[string]FrameHandler
}

// RegisterFrames routes a family of socket frames to a handler.
//
// Called at wiring time in cmd/api, which is the only place allowed to know that two
// contexts exist.
func (h *Handler) RegisterFrames(family string, handler FrameHandler) {
	h.frameMutex.Lock()
	defer h.frameMutex.Unlock()
	h.frameHandlers[family] = handler
}

// NewHandler returns a handler.
func NewHandler(
	service *app.Service,
	hub *Hub,
	authenticator Authenticator,
	caller CallerResolver,
	allowedOrigins []string,
	limiter *ratelimit.Limiter,
	logger *slog.Logger,
) *Handler {
	return &Handler{
		service:        service,
		hub:            hub,
		authenticator:  authenticator,
		caller:         caller,
		logger:         logger,
		allowedOrigins: allowedOrigins,
		limiter:        limiter,
		frameHandlers:  make(map[string]FrameHandler),
	}
}

// Rate limits, and where each number comes from.
//
// All three are far above what a person does and far below what a loop does, which is the
// only band a useful limit can sit in. They are per account rather than per device or per
// address: a device is something a client can make more of, and an address is shared by
// everyone behind one office router.
const (
	// sendLimit is generous because a person pasting a conversation into six messages is
	// normal and a client resending a queue after being offline is normal. What it stops is
	// a loop.
	sendLimit  = 60
	sendWindow = 10 * time.Second

	// connectLimit allows for a client with exponential backoff reconnecting through a
	// flapping network, and stops one that has no backoff at all — which is the failure
	// this protects against, since a socket costs the server far more than a request.
	connectLimit  = 30
	connectWindow = time.Minute
)

// limitSends bounds how often one account may append to the log.
func (h *Handler) limitSends(next http.Handler) http.Handler {
	return httpx.RateLimited(h.limiter, "send", sendLimit, sendWindow, func(r *http.Request) string {
		accountID, _ := h.caller(r.Context())
		return accountID
	}, h.logger)(next)
}

// Routes registers messaging endpoints. authenticated wraps handlers needing a
// bearer token; the socket authenticates itself in its first frame.
func (h *Handler) Routes(mux *http.ServeMux, authenticated func(http.Handler) http.Handler) {
	mux.HandleFunc("GET /v1/socket", h.Socket)

	mux.Handle("GET /v1/conversations", authenticated(http.HandlerFunc(h.listConversations)))
	mux.Handle("POST /v1/conversations/{conversationID}/receipt", authenticated(http.HandlerFunc(h.acknowledge)))

	mux.Handle("POST /v1/conversations/{conversationID}/entries/{sequence}/revision", authenticated(http.HandlerFunc(h.revise)))
	mux.Handle("POST /v1/conversations/{conversationID}/entries/{sequence}/retraction", authenticated(http.HandlerFunc(h.retract)))
	mux.Handle("GET /v1/conversations/{conversationID}/reactions", authenticated(http.HandlerFunc(h.listReactions)))
	mux.Handle("PUT /v1/conversations/{conversationID}/entries/{sequence}/reactions", authenticated(http.HandlerFunc(h.react)))
	mux.Handle("DELETE /v1/conversations/{conversationID}/entries/{sequence}/reactions", authenticated(http.HandlerFunc(h.unreact)))

	mux.Handle("POST /v1/conversations/group", authenticated(http.HandlerFunc(h.startGroup)))
	mux.Handle("POST /v1/conversations/channel", authenticated(http.HandlerFunc(h.startChannel)))
	mux.Handle("GET /v1/conversations/{conversationID}/members", authenticated(http.HandlerFunc(h.listMembers)))
	mux.Handle("POST /v1/conversations/{conversationID}/members", authenticated(http.HandlerFunc(h.addMember)))
	mux.Handle("DELETE /v1/conversations/{conversationID}/members/{accountID}", authenticated(http.HandlerFunc(h.removeMember)))
	mux.Handle("PUT /v1/conversations/{conversationID}/members/{accountID}/role", authenticated(http.HandlerFunc(h.changeRole)))
	mux.Handle("DELETE /v1/conversations/{conversationID}/membership", authenticated(http.HandlerFunc(h.leave)))

	mux.Handle("GET /v1/conversations/{conversationID}/invites", authenticated(http.HandlerFunc(h.listInvites)))
	mux.Handle("POST /v1/conversations/{conversationID}/invites", authenticated(http.HandlerFunc(h.createInvite)))
	mux.Handle("DELETE /v1/invites/{inviteID}", authenticated(http.HandlerFunc(h.revokeInvite)))
	// Redeeming needs an authenticated account — an invite says which conversation
	// somebody may join, not who they are.
	mux.Handle("POST /v1/invites/{token}/redeem", authenticated(http.HandlerFunc(h.redeemInvite)))
	mux.Handle("POST /v1/conversations/direct", authenticated(http.HandlerFunc(h.startDirect)))
	mux.Handle("GET /v1/conversations/{conversationID}", authenticated(http.HandlerFunc(h.getConversation)))
	mux.Handle("GET /v1/conversations/{conversationID}/entries", authenticated(http.HandlerFunc(h.listEntries)))
	// Limited inside the authentication, because the limit is per account and there is no
	// account until the token has been read.
	mux.Handle("POST /v1/conversations/{conversationID}/entries",
		authenticated(h.limitSends(http.HandlerFunc(h.send))))
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
	// ReplyTo is the position this entry replies to, zero for none. A reference
	// field and nothing more (ADR-0008).
	ReplyTo int64 `json:"reply_to"`
	// AttachmentID is a photo or video this entry carries, empty for none. An
	// identifier only: whether it has finished uploading is Media's business, and an
	// entry referencing one still pending is readable now (MD-1).
	AttachmentID string `json:"attachment_id"`
}

type reviseRequest struct {
	ClientEntryID string `json:"client_entry_id"`
	ContentType   string `json:"content_type"`
	Body          string `json:"body"`
}

type retractRequest struct {
	ClientEntryID string `json:"client_entry_id"`
}

type reactRequest struct {
	Emoji string `json:"emoji"`
}

type reactionResponse struct {
	Sequence  int64     `json:"sequence"`
	AccountID string    `json:"account_id"`
	Emoji     string    `json:"emoji"`
	CreatedAt time.Time `json:"created_at"`
}

type conversationResponse struct {
	ID          string    `json:"id"`
	Kind        string    `json:"kind"`
	Head        int64     `json:"head"`
	Role        string    `json:"role"`
	VisibleFrom int64     `json:"visible_from"`
	CreatedAt   time.Time `json:"created_at"`

	// Projected fields (ADR-0002). Eventually consistent, and a client must render
	// them correctly while they are behind rather than waiting for them (NF-7) —
	// which is why they are zero-valued rather than absent when the projection has
	// not caught up.
	Unread          int64 `json:"unread"`
	ReadThrough     int64 `json:"read_through"`
	DeliveredThough int64 `json:"delivered_through"`
	// OthersReadThrough and OthersDeliveredThrough are the lowest marks among the
	// other members. A client derives each of its own entries' delivery state by
	// comparing its sequence against these, rather than the server storing a state
	// per entry per recipient (MS-13).
	OthersReadThrough      int64 `json:"others_read_through"`
	OthersDeliveredThrough int64 `json:"others_delivered_through"`
}

type addMemberRequest struct {
	AccountID string `json:"account_id"`
}

type changeRoleRequest struct {
	Role string `json:"role"`
}

type createInviteRequest struct {
	// MaxUses of zero means unlimited, matching the aggregate.
	MaxUses   int        `json:"max_uses"`
	ExpiresAt *time.Time `json:"expires_at"`
}

type memberResponse struct {
	AccountID   string     `json:"account_id"`
	Role        string     `json:"role"`
	VisibleFrom int64      `json:"visible_from"`
	JoinedAt    time.Time  `json:"joined_at"`
	LeftAt      *time.Time `json:"left_at"`
}

type inviteResponse struct {
	ID        string     `json:"id"`
	Role      string     `json:"role"`
	MaxUses   int        `json:"max_uses"`
	Uses      int        `json:"uses"`
	Revoked   bool       `json:"revoked"`
	CreatedAt time.Time  `json:"created_at"`
	ExpiresAt *time.Time `json:"expires_at"`
	// Token is returned so its creator can share the link. Only to an
	// administrator of the conversation, which is what the handlers check before
	// calling this.
	Token string `json:"token"`
}

type acknowledgeRequest struct {
	DeliveredThrough int64 `json:"delivered_through"`
	ReadThrough      int64 `json:"read_through"`
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
	// TargetSequence is the position this entry amends, absent for a message. A
	// client that does not understand revisions ignores it and shows the original,
	// which is the degradation ADR-0008 requires.
	TargetSequence int64 `json:"target_sequence,omitempty"`
	ReplyTo        int64 `json:"reply_to,omitempty"`
	// AttachmentID is the photo or video this entry carries, absent for none. The
	// client fetches its state and URLs separately, which is what lets the entry be
	// delivered before the attachment is ready.
	AttachmentID string `json:"attachment_id,omitempty"`
}

func newEntryResponse(entry *domain.Entry) entryResponse {
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
		TargetSequence: int64(entry.Target()),
		ReplyTo:        int64(entry.ReplyTo()),
		AttachmentID:   string(entry.AttachmentID()),
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

	conversation, err := h.service.StartDirect(r.Context(), domain.AccountID(accountID), domain.AccountID(request.AccountID))
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}

	httpx.WriteJSON(w, h.logger, http.StatusCreated, conversationResponse{
		ID:          string(conversation.ID()),
		Kind:        string(conversation.Kind()),
		Head:        int64(conversation.Head()),
		Role:        string(domain.RoleMember),
		VisibleFrom: int64(domain.FirstSequence),
		CreatedAt:   conversation.CreatedAt(),
	})
}

func (h *Handler) listConversations(w http.ResponseWriter, r *http.Request) {
	accountID, _ := h.caller(r.Context())

	summaries, err := h.service.Summaries(r.Context(), domain.AccountID(accountID))
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}

	responses := make([]conversationResponse, 0, len(summaries))
	for _, summary := range summaries {
		responses = append(responses, conversationResponse{
			ID:                     string(summary.Conversation.ID()),
			Kind:                   string(summary.Conversation.Kind()),
			Head:                   int64(summary.Conversation.Head()),
			Role:                   string(summary.Membership.Role()),
			VisibleFrom:            int64(summary.Membership.VisibleFrom()),
			CreatedAt:              summary.Conversation.CreatedAt(),
			Unread:                 summary.State.UnreadCount,
			ReadThrough:            int64(summary.State.ReadSequence),
			DeliveredThough:        int64(summary.State.DeliveredSequence),
			OthersReadThrough:      int64(summary.OthersReadThrough),
			OthersDeliveredThrough: int64(summary.OthersDeliveredThrough),
		})
	}

	httpx.WriteJSON(w, h.logger, http.StatusOK, map[string]any{"conversations": responses})
}

// acknowledge records how far the caller has received and read a conversation.
//
// 202 rather than 200: nothing observable has changed when this returns. The marks
// are published and projected asynchronously (ADR-0002), and a status implying the
// count is already updated would invite clients to read it back and find it stale.
func (h *Handler) acknowledge(w http.ResponseWriter, r *http.Request) {
	accountID, _ := h.caller(r.Context())
	conversationID := domain.ConversationID(r.PathValue("conversationID"))

	var request acknowledgeRequest
	if err := httpx.DecodeJSON(r, &request); err != nil {
		h.fail(w, r, http.StatusBadRequest, "malformed_body", err.Error())
		return
	}
	if request.DeliveredThrough < 0 || request.ReadThrough < 0 {
		h.fail(w, r, http.StatusUnprocessableEntity, "invalid_field", "marks must not be negative")
		return
	}

	err := h.service.Acknowledge(r.Context(), conversationID, domain.AccountID(accountID),
		domain.Sequence(request.DeliveredThrough), domain.Sequence(request.ReadThrough))
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}

	httpx.WriteJSON(w, h.logger, http.StatusAccepted, nil)
}

func (h *Handler) getConversation(w http.ResponseWriter, r *http.Request) {
	accountID, _ := h.caller(r.Context())
	conversationID := domain.ConversationID(r.PathValue("conversationID"))

	conversation, err := h.service.Conversation(r.Context(), conversationID, domain.AccountID(accountID))
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}
	members, err := h.service.Members(r.Context(), conversationID, domain.AccountID(accountID))
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
	conversationID := domain.ConversationID(r.PathValue("conversationID"))

	after, err := strconv.ParseInt(r.URL.Query().Get("after"), 10, 64)
	if err != nil || after < 0 {
		// Absent or unparseable means "from the beginning of what I may see".
		after = 0
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))

	entries, err := h.service.Fetch(r.Context(), conversationID, domain.AccountID(accountID), domain.Sequence(after), limit)
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
	conversationID := domain.ConversationID(r.PathValue("conversationID"))

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
		r.Context(), conversationID, domain.AccountID(accountID),
		request.ClientEntryID, request.ContentType, body, domain.Sequence(request.ReplyTo),
		domain.AttachmentID(request.AttachmentID),
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

// --- revisions and reactions ---

// pathSequence reads a position out of the URL.
func (h *Handler) pathSequence(r *http.Request) (domain.Sequence, bool) {
	parsed, err := strconv.ParseInt(r.PathValue("sequence"), 10, 64)
	if err != nil || parsed < int64(domain.FirstSequence) {
		return 0, false
	}
	return domain.Sequence(parsed), true
}

func (h *Handler) revise(w http.ResponseWriter, r *http.Request) {
	accountID, _ := h.caller(r.Context())
	conversationID := domain.ConversationID(r.PathValue("conversationID"))

	sequence, ok := h.pathSequence(r)
	if !ok {
		h.fail(w, r, http.StatusBadRequest, "invalid_sequence", "the target must be a position in the log")
		return
	}

	var request reviseRequest
	if err := httpx.DecodeJSON(r, &request); err != nil {
		h.fail(w, r, http.StatusBadRequest, "malformed_body", err.Error())
		return
	}
	body, err := base64.StdEncoding.DecodeString(request.Body)
	if err != nil {
		h.fail(w, r, http.StatusBadRequest, "malformed_body", "body must be base64")
		return
	}

	entry, err := h.service.Revise(r.Context(), conversationID, domain.AccountID(accountID),
		sequence, request.ClientEntryID, request.ContentType, body)
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}

	// 201, because a revision *is* a new entry with its own position — that is the
	// whole of ADR-0008, and a 200 would suggest something was updated in place.
	httpx.WriteJSON(w, h.logger, http.StatusCreated, newEntryResponse(entry))
}

func (h *Handler) retract(w http.ResponseWriter, r *http.Request) {
	accountID, _ := h.caller(r.Context())
	conversationID := domain.ConversationID(r.PathValue("conversationID"))

	sequence, ok := h.pathSequence(r)
	if !ok {
		h.fail(w, r, http.StatusBadRequest, "invalid_sequence", "the target must be a position in the log")
		return
	}

	var request retractRequest
	if err := httpx.DecodeJSON(r, &request); err != nil {
		h.fail(w, r, http.StatusBadRequest, "malformed_body", err.Error())
		return
	}

	entry, err := h.service.Retract(r.Context(), conversationID, domain.AccountID(accountID),
		sequence, request.ClientEntryID)
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}

	httpx.WriteJSON(w, h.logger, http.StatusCreated, newEntryResponse(entry))
}

func (h *Handler) react(w http.ResponseWriter, r *http.Request) {
	h.reaction(w, r, h.service.React)
}

func (h *Handler) unreact(w http.ResponseWriter, r *http.Request) {
	h.reaction(w, r, h.service.Unreact)
}

func (h *Handler) reaction(
	w http.ResponseWriter,
	r *http.Request,
	apply func(context.Context, domain.ConversationID, domain.AccountID, domain.Sequence, string) error,
) {
	accountID, _ := h.caller(r.Context())
	conversationID := domain.ConversationID(r.PathValue("conversationID"))

	sequence, ok := h.pathSequence(r)
	if !ok {
		h.fail(w, r, http.StatusBadRequest, "invalid_sequence", "the target must be a position in the log")
		return
	}

	var request reactRequest
	if err := httpx.DecodeJSON(r, &request); err != nil {
		h.fail(w, r, http.StatusBadRequest, "malformed_body", err.Error())
		return
	}

	if err := apply(r.Context(), conversationID, domain.AccountID(accountID), sequence, request.Emoji); err != nil {
		h.writeDomainError(w, r, err)
		return
	}

	// 204. A reaction has no position and no identity of its own to return, which is
	// what MS-10 means by it not being in the log.
	httpx.WriteJSON(w, h.logger, http.StatusNoContent, nil)
}

func (h *Handler) listReactions(w http.ResponseWriter, r *http.Request) {
	accountID, _ := h.caller(r.Context())
	conversationID := domain.ConversationID(r.PathValue("conversationID"))

	from, _ := strconv.ParseInt(r.URL.Query().Get("from"), 10, 64)
	to, _ := strconv.ParseInt(r.URL.Query().Get("to"), 10, 64)
	if from < int64(domain.FirstSequence) {
		from = int64(domain.FirstSequence)
	}
	if to < from {
		h.fail(w, r, http.StatusBadRequest, "invalid_range", "to must not be before from")
		return
	}

	reactions, err := h.service.Reactions(r.Context(), conversationID, domain.AccountID(accountID),
		domain.Sequence(from), domain.Sequence(to))
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}

	responses := make([]reactionResponse, 0, len(reactions))
	for _, reaction := range reactions {
		responses = append(responses, reactionResponse{
			Sequence:  int64(reaction.Sequence),
			AccountID: string(reaction.AccountID),
			Emoji:     string(reaction.Emoji),
			CreatedAt: reaction.CreatedAt,
		})
	}

	httpx.WriteJSON(w, h.logger, http.StatusOK, map[string]any{"reactions": responses})
}

// --- groups, channels and membership ---

func (h *Handler) startGroup(w http.ResponseWriter, r *http.Request) {
	h.startKind(w, r, h.service.StartGroup)
}

func (h *Handler) startChannel(w http.ResponseWriter, r *http.Request) {
	h.startKind(w, r, h.service.StartChannel)
}

func (h *Handler) startKind(
	w http.ResponseWriter,
	r *http.Request,
	start func(context.Context, domain.AccountID) (*domain.Conversation, error),
) {
	accountID, _ := h.caller(r.Context())

	conversation, err := start(r.Context(), domain.AccountID(accountID))
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}

	httpx.WriteJSON(w, h.logger, http.StatusCreated, conversationResponse{
		ID:          string(conversation.ID()),
		Kind:        string(conversation.Kind()),
		Head:        int64(conversation.Head()),
		Role:        string(domain.RoleAdmin),
		VisibleFrom: int64(domain.FirstSequence),
		CreatedAt:   conversation.CreatedAt(),
	})
}

func (h *Handler) listMembers(w http.ResponseWriter, r *http.Request) {
	accountID, _ := h.caller(r.Context())
	conversationID := domain.ConversationID(r.PathValue("conversationID"))

	members, err := h.service.Members(r.Context(), conversationID, domain.AccountID(accountID))
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}

	responses := make([]memberResponse, 0, len(members))
	for _, member := range members {
		responses = append(responses, memberResponse{
			AccountID:   string(member.AccountID()),
			Role:        string(member.Role()),
			VisibleFrom: int64(member.VisibleFrom()),
			JoinedAt:    member.JoinedAt(),
			LeftAt:      member.LeftAt(),
		})
	}

	httpx.WriteJSON(w, h.logger, http.StatusOK, map[string]any{"members": responses})
}

func (h *Handler) addMember(w http.ResponseWriter, r *http.Request) {
	accountID, _ := h.caller(r.Context())
	conversationID := domain.ConversationID(r.PathValue("conversationID"))

	var request addMemberRequest
	if err := httpx.DecodeJSON(r, &request); err != nil {
		h.fail(w, r, http.StatusBadRequest, "malformed_body", err.Error())
		return
	}

	membership, err := h.service.AddMember(r.Context(), conversationID,
		domain.AccountID(accountID), domain.AccountID(request.AccountID))
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}

	httpx.WriteJSON(w, h.logger, http.StatusCreated, memberResponse{
		AccountID:   string(membership.AccountID()),
		Role:        string(membership.Role()),
		VisibleFrom: int64(membership.VisibleFrom()),
		JoinedAt:    membership.JoinedAt(),
	})
}

func (h *Handler) removeMember(w http.ResponseWriter, r *http.Request) {
	accountID, _ := h.caller(r.Context())

	err := h.service.RemoveMember(r.Context(),
		domain.ConversationID(r.PathValue("conversationID")),
		domain.AccountID(accountID),
		domain.AccountID(r.PathValue("accountID")),
	)
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}
	httpx.WriteJSON(w, h.logger, http.StatusNoContent, nil)
}

func (h *Handler) changeRole(w http.ResponseWriter, r *http.Request) {
	accountID, _ := h.caller(r.Context())

	var request changeRoleRequest
	if err := httpx.DecodeJSON(r, &request); err != nil {
		h.fail(w, r, http.StatusBadRequest, "malformed_body", err.Error())
		return
	}

	err := h.service.ChangeRole(r.Context(),
		domain.ConversationID(r.PathValue("conversationID")),
		domain.AccountID(accountID),
		domain.AccountID(r.PathValue("accountID")),
		domain.Role(request.Role),
	)
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}
	httpx.WriteJSON(w, h.logger, http.StatusNoContent, nil)
}

func (h *Handler) leave(w http.ResponseWriter, r *http.Request) {
	accountID, _ := h.caller(r.Context())

	err := h.service.Leave(r.Context(),
		domain.ConversationID(r.PathValue("conversationID")),
		domain.AccountID(accountID),
	)
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}
	httpx.WriteJSON(w, h.logger, http.StatusNoContent, nil)
}

// --- invites ---

func newInviteResponse(invite *domain.Invite) inviteResponse {
	return inviteResponse{
		ID:        string(invite.ID()),
		Role:      string(invite.Role()),
		MaxUses:   invite.MaxUses(),
		Uses:      invite.Uses(),
		Revoked:   invite.Revoked(),
		CreatedAt: invite.CreatedAt(),
		ExpiresAt: invite.ExpiresAt(),
		Token:     string(invite.Token()),
	}
}

func (h *Handler) createInvite(w http.ResponseWriter, r *http.Request) {
	accountID, _ := h.caller(r.Context())

	var request createInviteRequest
	if err := httpx.DecodeJSON(r, &request); err != nil {
		h.fail(w, r, http.StatusBadRequest, "malformed_body", err.Error())
		return
	}

	invite, err := h.service.CreateInvite(r.Context(),
		domain.ConversationID(r.PathValue("conversationID")),
		domain.AccountID(accountID), request.MaxUses, request.ExpiresAt,
	)
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}

	httpx.WriteJSON(w, h.logger, http.StatusCreated, newInviteResponse(invite))
}

func (h *Handler) listInvites(w http.ResponseWriter, r *http.Request) {
	accountID, _ := h.caller(r.Context())

	invites, err := h.service.Invites(r.Context(),
		domain.ConversationID(r.PathValue("conversationID")), domain.AccountID(accountID))
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}

	responses := make([]inviteResponse, 0, len(invites))
	for _, invite := range invites {
		responses = append(responses, newInviteResponse(invite))
	}
	httpx.WriteJSON(w, h.logger, http.StatusOK, map[string]any{"invites": responses})
}

func (h *Handler) revokeInvite(w http.ResponseWriter, r *http.Request) {
	accountID, _ := h.caller(r.Context())

	err := h.service.RevokeInvite(r.Context(),
		domain.InviteID(r.PathValue("inviteID")), domain.AccountID(accountID))
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}
	httpx.WriteJSON(w, h.logger, http.StatusNoContent, nil)
}

func (h *Handler) redeemInvite(w http.ResponseWriter, r *http.Request) {
	accountID, _ := h.caller(r.Context())

	conversation, err := h.service.RedeemInvite(r.Context(),
		domain.InviteToken(r.PathValue("token")), domain.AccountID(accountID))
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}

	httpx.WriteJSON(w, h.logger, http.StatusOK, conversationResponse{
		ID:        string(conversation.ID()),
		Kind:      string(conversation.Kind()),
		Head:      int64(conversation.Head()),
		CreatedAt: conversation.CreatedAt(),
	})
}

// --- error mapping ---

func (h *Handler) writeDomainError(w http.ResponseWriter, r *http.Request, err error) {
	var validation domain.ValidationError
	switch {
	case errors.As(err, &validation):
		httpx.WriteError(w, h.logger, http.StatusUnprocessableEntity, httpx.ErrorBody{
			Code: "invalid_field", Message: validation.Reason, Field: validation.Field,
		})

	case errors.Is(err, domain.ErrNotAMember), errors.Is(err, domain.ErrConversationNotFound):
		// One status for both: an account that does not belong to a conversation
		// must not be able to tell whether it exists.
		h.fail(w, r, http.StatusNotFound, "conversation_not_found", "no such conversation")

	case errors.Is(err, domain.ErrNotPermittedToWrite):
		h.fail(w, r, http.StatusForbidden, "not_permitted", "you may not write to this conversation")

	case errors.Is(err, domain.ErrCannotMessageSelf):
		h.fail(w, r, http.StatusUnprocessableEntity, "cannot_message_self", "you cannot message yourself")

	case errors.Is(err, domain.ErrDirectConversationIsFull):
		h.fail(w, r, http.StatusConflict, "direct_conversation_full", "a direct conversation has exactly two members")

	case errors.Is(err, domain.ErrGroupIsFull):
		h.fail(w, r, http.StatusConflict, "group_full", "this group is at its member limit")

	case errors.Is(err, domain.ErrAlreadyAMember):
		h.fail(w, r, http.StatusConflict, "already_a_member", "that account already belongs to this conversation")

	case errors.Is(err, domain.ErrNotPermittedToAdminister):
		h.fail(w, r, http.StatusForbidden, "not_permitted", "you may not change this conversation's membership")

	case errors.Is(err, domain.ErrMembershipIsFixed):
		h.fail(w, r, http.StatusConflict, "membership_fixed", "a direct conversation's membership cannot be changed")

	case errors.Is(err, domain.ErrCannotRemoveSelf):
		h.fail(w, r, http.StatusUnprocessableEntity, "cannot_remove_self", "leave the conversation instead")

	case errors.Is(err, domain.ErrNotTheAuthor):
		h.fail(w, r, http.StatusForbidden, "not_the_author", "only the author may change this entry")

	case errors.Is(err, domain.ErrCannotAmendAnAmendment):
		h.fail(w, r, http.StatusUnprocessableEntity, "amend_the_original", "edit the original entry, not an edit of it")

	case errors.Is(err, domain.ErrEntryRetracted):
		h.fail(w, r, http.StatusConflict, "entry_retracted", "this entry has been deleted")

	case errors.Is(err, domain.ErrEntryNotFound):
		h.fail(w, r, http.StatusNotFound, "entry_not_found", "no such entry")

	case errors.Is(err, domain.ErrInviteNotFound):
		h.fail(w, r, http.StatusNotFound, "invite_not_found", "no such invite")

	case errors.Is(err, domain.ErrInviteExpired):
		h.fail(w, r, http.StatusGone, "invite_expired", "this invite has expired")

	case errors.Is(err, domain.ErrInviteRevoked):
		h.fail(w, r, http.StatusGone, "invite_revoked", "this invite has been withdrawn")

	case errors.Is(err, domain.ErrInviteExhausted):
		h.fail(w, r, http.StatusGone, "invite_exhausted", "this invite has already been used")

	default:
		logging.With(r.Context(), h.logger).Error("unhandled error", slog.Any("error", err))
		h.fail(w, r, http.StatusInternalServerError, "internal_error", "something went wrong")
	}
}

func (h *Handler) fail(w http.ResponseWriter, _ *http.Request, status int, code, message string) {
	httpx.WriteError(w, h.logger, status, httpx.ErrorBody{Code: code, Message: message})
}
