package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/coder/websocket"

	"comms/internal/messaging/internal/domain"
	"comms/internal/platform/logging"
)

// handshakeTimeout is how long an unauthenticated socket may stay open.
//
// Short, because an open socket that has not proved anything is a resource anyone
// can consume.
const handshakeTimeout = 10 * time.Second

// readLimit caps an inbound frame. Clients send small control frames here; sending
// is an HTTP request, so nothing legitimate approaches this.
const readLimit = 32 * 1024

// Authenticator resolves an access token to the account and device presenting it.
//
// Declared here rather than imported from Identity: contexts do not import each
// other (ADR-0007), so Messaging states what it needs and cmd/api supplies
// Identity's implementation. This is the port that keeps the two independent.
type Authenticator interface {
	Authenticate(ctx context.Context, accessToken string) (accountID string, deviceID string, err error)
}

// --- client frames ---

// clientFrame is the envelope every inbound message shares.
type clientFrame struct {
	Type string `json:"type"`
}

type authenticateFrame struct {
	Type  string `json:"type"`
	Token string `json:"token"`
}

// resumeFrame is what a client sends to declare what it already holds.
//
// A map of conversation to highest sequence held. Conversations the client has
// never seen are simply absent, which reads as zero — so a first-ever connection
// and a reconnection use the same frame with no special case.
type resumeFrame struct {
	Type   string           `json:"type"`
	Cursor map[string]int64 `json:"cursor"`
}

// --- server frames ---

type readyFrame struct {
	Type      string `json:"type"`
	AccountID string `json:"account_id"`
	DeviceID  string `json:"device_id"`
}

type gapFrame struct {
	Type string     `json:"type"`
	Gaps []gapEntry `json:"gaps"`
}

type gapEntry struct {
	ConversationID string `json:"conversation_id"`
	From           int64  `json:"from"`
	To             int64  `json:"to"`
}

// typingFrame is a client saying it is or is not typing, and is also the shape the server
// pushes to everyone else — one type in both directions, because it says the same thing either
// way and a second name for it would be a second thing to keep in step.
type typingFrame struct {
	Type           string `json:"type"`
	ConversationID string `json:"conversation_id"`
	// AccountID is set only on the way out. A client cannot claim somebody else is typing:
	// inbound, this is whoever the socket is authenticated as.
	AccountID string `json:"account_id,omitempty"`
	Typing    bool   `json:"typing"`
}

// presenceAskFrame is a client asking who is present in a conversation.
//
// Asked rather than pushed, and that is the trade: presence is soft state that a client polls
// while it has a conversation open, which costs a request every few seconds on a socket it is
// already holding. Pushing it would mean every connect and disconnect fanning out to every
// member of every conversation that account belongs to — the same work, moved to the moment a
// person opens their laptop, and paid for conversations nobody is looking at.
type presenceAskFrame struct {
	Type           string `json:"type"`
	ConversationID string `json:"conversation_id"`
}

// presenceFrame is the answer: the whole state of a conversation, not a change to it.
//
// A snapshot because a client can then replace what it holds, which needs no reconciliation and
// cannot drift. It also repairs the typing set, whose pushes are allowed to be lost.
type presenceFrame struct {
	Type           string   `json:"type"`
	ConversationID string   `json:"conversation_id"`
	Online         []string `json:"online"`
	Typing         []string `json:"typing"`
}

type errorFrame struct {
	Type    string `json:"type"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Socket serves the WebSocket endpoint.
func (h *Handler) Socket(w http.ResponseWriter, r *http.Request) {
	logger := logging.With(r.Context(), h.logger)

	socket, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		// Same-origin only. The browser client is served from the same origin, and
		// a permissive policy here would let any page open an authenticated socket
		// on a visitor's behalf.
		OriginPatterns: h.allowedOrigins,
	})
	if err != nil {
		logger.Debug("websocket accept", slog.Any("error", err))
		return
	}
	socket.SetReadLimit(readLimit)

	// Detached from the request context: r.Context() is cancelled when the handler
	// returns, which for an upgraded connection is immediately.
	ctx, cancel := context.WithCancel(context.WithoutCancel(r.Context()))
	defer cancel()

	connection, err := h.handshake(ctx, socket, logger)
	if err != nil {
		// The reason is already on the wire; nothing further is owed to a socket
		// that never authenticated.
		_ = socket.Close(websocket.StatusPolicyViolation, "handshake failed")
		return
	}

	h.hub.Register(ctx, connection)
	// Online from now, rather than from the first heartbeat up to ten seconds later. The
	// heartbeat's job is keeping the claim true, not making it.
	if err := h.service.Connected(ctx, connection.AccountID(), connection.DeviceID()); err != nil {
		logger.Warn("record presence", slog.Any("error", err))
	}
	defer h.hub.Unregister(ctx, connection)
	defer connection.Close("closed")
	// Registered handlers are told the socket has gone, on a context of their own: this
	// one is cancelled the moment the request returns, and cleaning up after a departed
	// participant is a database write that has to be allowed to finish.
	defer func() {
		cleanup, cancelCleanup := context.WithTimeout(context.WithoutCancel(ctx), socketCleanupGrace)
		defer cancelCleanup()
		h.closed(cleanup, connection)
	}()

	go connection.Write(ctx)
	h.read(ctx, connection, logger)
}

// socketCleanupGrace bounds what a handler may do after a socket closes. Long enough for a
// call departure to commit, short enough that a shutdown is not held up by one.
const socketCleanupGrace = 10 * time.Second

// handshake authenticates the socket and resumes it.
//
// Credentials arrive in the first frame rather than in the URL. A token in a query
// string ends up in proxy logs, browser history and referrer headers, and a
// WebSocket handshake cannot carry an Authorization header from a browser — so the
// first-frame approach is the only one that keeps the token out of places it should
// not be.
func (h *Handler) handshake(ctx context.Context, socket *websocket.Conn, logger *slog.Logger) (*Connection, error) {
	authCtx, cancel := context.WithTimeout(ctx, handshakeTimeout)
	defer cancel()

	_, raw, err := socket.Read(authCtx)
	if err != nil {
		return nil, err //nolint:wrapcheck // caller only closes the socket.
	}

	var frame authenticateFrame
	if err := json.Unmarshal(raw, &frame); err != nil || frame.Type != "authenticate" {
		writeFrameTo(authCtx, socket, errorFrame{"error", "expected_authenticate", "the first frame must authenticate"})
		return nil, errors.New("first frame was not an authentication")
	}

	accountID, deviceID, err := h.authenticator.Authenticate(authCtx, frame.Token)
	if err != nil {
		writeFrameTo(authCtx, socket, errorFrame{"error", "unauthenticated", "the access token is not valid"})
		return nil, err //nolint:wrapcheck // caller only closes the socket.
	}

	connection := NewConnection(socket, domain.AccountID(accountID), deviceID, logger)

	if err := connection.WriteFrame(authCtx, readyFrame{"ready", accountID, deviceID}); err != nil {
		return nil, err //nolint:wrapcheck // caller only closes the socket.
	}

	logger.Info("socket authenticated",
		slog.String("account_id", accountID),
		slog.String("device_id", deviceID),
	)
	return connection, nil
}

// read handles inbound frames until the connection ends.
func (h *Handler) read(ctx context.Context, connection *Connection, logger *slog.Logger) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-connection.Done():
			return
		default:
		}

		_, raw, err := connection.socket.Read(ctx)
		if err != nil {
			// Any read error ends the connection: a closed socket, a client that
			// went away, a frame over the limit. None are recoverable here, and the
			// client's remedy is the same in every case — reconnect and resume.
			connection.Close("read failed")
			return
		}

		var envelope clientFrame
		if err := json.Unmarshal(raw, &envelope); err != nil {
			_ = connection.WriteFrame(ctx, errorFrame{"error", "malformed_frame", "frame was not valid JSON"})
			continue
		}

		switch envelope.Type {
		case "resume":
			h.handleResume(ctx, connection, raw, logger)

		case "ping":
			// Application-level, distinct from the protocol ping. Clients behind
			// proxies that strip control frames still need a way to keep a
			// connection warm and to learn it is dead.
			_ = connection.WriteFrame(ctx, clientFrame{Type: "pong"})

		case "typing":
			h.handleTyping(ctx, connection, raw, logger)

		case "presence.ask":
			h.handlePresenceAsk(ctx, connection, raw, logger)

		default:
			// Frames Messaging does not own are offered to whoever registered for
			// them — call signalling, so far (ADR-0004: one socket per client, for
			// everything). Delegation rather than a switch that grows: Messaging must
			// not learn what a call is to carry one.
			if h.delegate(ctx, connection, envelope.Type, raw) {
				continue
			}
			// Still unknown. Ignored rather than fatal, so a newer client talking to
			// an older server degrades instead of disconnecting.
			logger.Debug("ignoring unknown frame", slog.String("type", envelope.Type))
		}
	}
}

// session is the narrow view of a connection a delegated handler is given.
//
// A wrapper rather than the Connection itself, and not only because AccountID returns a
// domain type here and a string there. A handler holding the whole Connection could change
// what the client is subscribed to, and what a client sees of a conversation is Messaging's
// business alone.
type session struct {
	connection *Connection
}

var _ Session = session{}

func (s session) AccountID() string { return string(s.connection.AccountID()) }
func (s session) DeviceID() string  { return s.connection.DeviceID() }

// Send marshals a frame and queues it.
//
// Marshalled here rather than by the handler, so a delegated protocol writes Go structs
// and never has to know that this connection carries JSON.
func (s session) Send(frame any) error {
	encoded, err := json.Marshal(frame)
	if err != nil {
		return fmt.Errorf("encode frame: %w", err)
	}
	s.connection.Send(encoded)
	return nil
}

// closed tells every registered handler that a connection has gone.
//
// A client that crashed said nothing, and something has to notice: without this a call
// keeps a participant nobody can see forever, and CL-3 never fires.
func (h *Handler) closed(ctx context.Context, connection *Connection) {
	// Presence withdrawn at once rather than left to expire. Not required for correctness
	// — the claim ages out either way — and worth doing because thirty seconds of a dot
	// beside somebody who closed their laptop reads as a broken feature rather than as a
	// window.
	if err := h.service.Disconnected(ctx, connection.AccountID(), connection.DeviceID()); err != nil {
		h.logger.Debug("clear presence", slog.Any("error", err))
	}

	h.frameMutex.RLock()
	handlers := make([]FrameHandler, 0, len(h.frameHandlers))
	for _, handler := range h.frameHandlers {
		handlers = append(handlers, handler)
	}
	h.frameMutex.RUnlock()

	for _, handler := range handlers {
		handler.SocketClosed(ctx, session{connection})
	}
}

// delegate offers a frame to a registered handler, reporting whether one took it.
//
// Matched on the part before the first dot, so a context registers a family — "call" —
// rather than every frame it will ever add. That keeps the socket's routing table the size
// of the number of contexts rather than the number of messages.
func (h *Handler) delegate(
	ctx context.Context,
	connection *Connection,
	frameType string,
	raw []byte,
) bool {
	family, _, found := strings.Cut(frameType, ".")
	if !found {
		return false
	}

	h.frameMutex.RLock()
	handler := h.frameHandlers[family]
	h.frameMutex.RUnlock()
	if handler == nil {
		return false
	}

	if err := handler.HandleFrame(ctx, session{connection}, frameType, raw); err != nil {
		// Reported to the client rather than closing the socket. A call that cannot be
		// joined is not a reason to lose the messages on the same connection.
		logging.With(ctx, h.logger).Warn("delegated frame",
			slog.String("type", frameType), slog.Any("error", err))
		_ = connection.WriteFrame(ctx, errorFrame{"error", "frame_failed", err.Error()})
	}
	return true
}

// handleResume answers a client's declaration of what it holds with what it is
// missing.
func (h *Handler) handleResume(ctx context.Context, connection *Connection, raw []byte, logger *slog.Logger) {
	var frame resumeFrame
	if err := json.Unmarshal(raw, &frame); err != nil {
		_ = connection.WriteFrame(ctx, errorFrame{"error", "malformed_frame", "resume frame was not valid JSON"})
		return
	}

	held := make(map[domain.ConversationID]domain.Sequence, len(frame.Cursor))
	for conversationID, sequence := range frame.Cursor {
		// A negative cursor would widen the gap to include entries the client is
		// not entitled to; clamped rather than rejected, since the server computes
		// visibility itself anyway.
		if sequence < 0 {
			sequence = 0
		}
		held[domain.ConversationID(conversationID)] = domain.Sequence(sequence)
	}

	gaps, err := h.service.Resume(ctx, connection.AccountID(), held)
	if err != nil {
		logger.Error("resume", slog.Any("error", err))
		_ = connection.WriteFrame(ctx, errorFrame{"error", "resume_failed", "could not compute what you are missing"})
		return
	}

	// Every conversation the account belongs to is followed, not only those with a
	// gap: a conversation the client is current on still needs live delivery.
	memberships, err := h.service.Conversations(ctx, connection.AccountID())
	if err != nil {
		logger.Error("list conversations", slog.Any("error", err))
		_ = connection.WriteFrame(ctx, errorFrame{"error", "resume_failed", "could not list your conversations"})
		return
	}
	for _, membership := range memberships {
		if membership.Active() {
			h.hub.Listen(ctx, connection, membership.ConversationID(), membership.VisibleFrom())
		}
	}

	response := gapFrame{Type: "gaps", Gaps: make([]gapEntry, 0, len(gaps))}
	for _, gap := range gaps {
		response.Gaps = append(response.Gaps, gapEntry{
			ConversationID: string(gap.ConversationID),
			From:           int64(gap.From),
			To:             int64(gap.To),
		})
	}

	if err := connection.WriteFrame(ctx, response); err != nil {
		connection.Close("write failed")
	}
}

// handleTyping records a client's typing claim and tells the conversation.
//
// Errors are logged rather than sent back. A refused typing claim is not something a person can
// act on, and an error frame for it would put "you may not type" on screen in the one case it
// legitimately happens — a channel reader whose client asked anyway.
func (h *Handler) handleTyping(
	ctx context.Context,
	connection *Connection,
	raw []byte,
	logger *slog.Logger,
) {
	var frame typingFrame
	if err := json.Unmarshal(raw, &frame); err != nil {
		_ = connection.WriteFrame(ctx, errorFrame{"error", "malformed_frame", "typing frame was not valid JSON"})
		return
	}

	if err := h.service.Typing(ctx,
		domain.ConversationID(frame.ConversationID), connection.AccountID(), frame.Typing); err != nil {
		logger.Debug("typing", slog.Any("error", err))
	}
}

// handlePresenceAsk answers who is present in a conversation.
func (h *Handler) handlePresenceAsk(
	ctx context.Context,
	connection *Connection,
	raw []byte,
	logger *slog.Logger,
) {
	var frame presenceAskFrame
	if err := json.Unmarshal(raw, &frame); err != nil {
		_ = connection.WriteFrame(ctx, errorFrame{"error", "malformed_frame", "presence frame was not valid JSON"})
		return
	}

	present, err := h.service.PresenceIn(ctx,
		domain.ConversationID(frame.ConversationID), connection.AccountID())
	if err != nil {
		// Silence rather than an error, for a question about something ephemeral: a client
		// that gets no answer shows nobody, which is the same thing it showed before it
		// asked. Reporting it would put a banner on screen for a dot.
		logger.Debug("presence", slog.Any("error", err))
		return
	}

	_ = connection.WriteFrame(ctx, presenceFrame{
		Type:           "presence",
		ConversationID: frame.ConversationID,
		Online:         present.Online,
		Typing:         present.Typing,
	})
}

// writeFrameTo sends a frame on a socket that has no Connection yet.
func writeFrameTo(ctx context.Context, socket *websocket.Conn, frame any) {
	encoded, err := json.Marshal(frame)
	if err != nil {
		return
	}
	writeCtx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	_ = socket.Write(writeCtx, websocket.MessageText, encoded)
}
