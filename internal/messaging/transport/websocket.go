package transport

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/coder/websocket"

	"comms/internal/messaging/domain"
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
	defer h.hub.Unregister(ctx, connection)
	defer connection.Close("closed")

	go connection.Write(ctx)
	h.read(ctx, connection, logger)
}

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

		default:
			// Unknown frames are ignored rather than fatal, so a newer client
			// talking to an older server degrades instead of disconnecting.
			logger.Debug("ignoring unknown frame", slog.String("type", envelope.Type))
		}
	}
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
