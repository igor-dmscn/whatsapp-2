// Package api carries call signalling over the socket a client already has.
//
// Signalling is domain traffic, not media: an offer is a few kilobytes of text exchanged
// once per participant, and it belongs on the connection the client is already
// authenticated on (ADR-0004). The media never comes near it — that goes straight to the
// SFU over UDP.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"comms/internal/calling/internal/app"
	"comms/internal/calling/internal/domain"
)

// Session is the part of a client's connection this handler uses.
//
// Declared here rather than imported from Messaging, for the same reason as every other
// port in this tree: Calling must not name a Messaging type. Messaging's own Session
// satisfies it, and cmd/api makes the join.
type Session interface {
	AccountID() string
	DeviceID() string
	Send(frame any) error
}

// Handler answers call frames.
type Handler struct {
	service *app.Service
	logger  *slog.Logger

	// sessions maps a device to the socket it is on, so an offer the media node
	// produces can be delivered to the right client. This is the whole reason
	// renegotiation works: the node has a participant identifier and nothing else,
	// and this is what turns one into a connection.
	mutex    sync.RWMutex
	sessions map[string]Session
}

// NewHandler returns a handler.
func NewHandler(service *app.Service, logger *slog.Logger) *Handler {
	return &Handler{
		service:  service,
		logger:   logger,
		sessions: make(map[string]Session),
	}
}

// --- client frames ---

type joinFrame struct {
	Type           string `json:"type"`
	ConversationID string `json:"conversation_id"`
	// SDP is the client's offer. Carried as text because that is what it is; parsing it
	// here would mean this server understood media negotiation, which it does not.
	SDP string `json:"sdp"`
}

type answerFrame struct {
	Type   string `json:"type"`
	CallID string `json:"call_id"`
	SDP    string `json:"sdp"`
}

type leaveFrame struct {
	Type   string `json:"type"`
	CallID string `json:"call_id"`
}

type activeFrame struct {
	Type           string `json:"type"`
	ConversationID string `json:"conversation_id"`
}

// --- server frames ---

type participantView struct {
	AccountID string `json:"account_id"`
	DeviceID  string `json:"device_id"`
}

type callView struct {
	Type           string            `json:"type"`
	CallID         string            `json:"call_id"`
	ConversationID string            `json:"conversation_id"`
	State          string            `json:"state"`
	Participants   []participantView `json:"participants"`
	// SDP is the node's answer, present only on a joined frame.
	SDP string `json:"sdp,omitempty"`
}

type offerView struct {
	Type   string `json:"type"`
	CallID string `json:"call_id"`
	SDP    string `json:"sdp"`
}

type noCallView struct {
	Type           string `json:"type"`
	ConversationID string `json:"conversation_id"`
}

func view(frameType string, call *domain.Call, sdp string) callView {
	participants := make([]participantView, 0, 4)
	for _, participant := range call.Participants() {
		if !participant.Present() {
			continue
		}
		participants = append(participants, participantView{
			AccountID: string(participant.AccountID),
			DeviceID:  string(participant.DeviceID),
		})
	}

	return callView{
		Type:           frameType,
		CallID:         string(call.ID()),
		ConversationID: string(call.ConversationID()),
		State:          string(call.State()),
		Participants:   participants,
		SDP:            sdp,
	}
}

// HandleFrame answers one call frame.
func (h *Handler) HandleFrame(ctx context.Context, session Session, frameType string, raw []byte) error {
	// Remembered on every frame rather than only on join: a client that reconnects mid-call
	// has a new socket and the same device, and an offer sent to the old one goes nowhere.
	h.remember(session)

	switch frameType {
	case "call.join":
		return h.join(ctx, session, raw)
	case "call.answer":
		return h.answer(ctx, session, raw)
	case "call.leave":
		return h.leave(ctx, session, raw)
	case "call.active":
		return h.active(ctx, session, raw)
	default:
		// Ignored rather than refused, so a newer client's frame does not produce an error
		// the user sees for a feature this server simply does not have.
		h.logger.Debug("ignoring unknown call frame", slog.String("type", frameType))
		return nil
	}
}

func (h *Handler) join(ctx context.Context, session Session, raw []byte) error {
	var frame joinFrame
	if err := json.Unmarshal(raw, &frame); err != nil {
		return fmt.Errorf("call.join was not valid JSON: %w", err)
	}

	joined, err := h.service.Join(ctx,
		domain.ConversationID(frame.ConversationID),
		domain.AccountID(session.AccountID()),
		domain.DeviceID(session.DeviceID()),
		frame.SDP,
	)
	if err != nil {
		return describe(err)
	}

	return session.Send(view("call.joined", joined.Call, joined.Answer))
}

func (h *Handler) answer(ctx context.Context, session Session, raw []byte) error {
	var frame answerFrame
	if err := json.Unmarshal(raw, &frame); err != nil {
		return fmt.Errorf("call.answer was not valid JSON: %w", err)
	}

	if err := h.service.Answer(ctx,
		domain.CallID(frame.CallID), domain.DeviceID(session.DeviceID()), frame.SDP); err != nil {
		return describe(err)
	}
	// No reply. An answer completes an exchange the server started; acknowledging it would
	// be a frame nothing waits for.
	return nil
}

func (h *Handler) leave(ctx context.Context, session Session, raw []byte) error {
	var frame leaveFrame
	if err := json.Unmarshal(raw, &frame); err != nil {
		return fmt.Errorf("call.leave was not valid JSON: %w", err)
	}

	if err := h.service.Leave(ctx,
		domain.CallID(frame.CallID), domain.DeviceID(session.DeviceID())); err != nil {
		return describe(err)
	}
	return session.Send(map[string]string{"type": "call.left", "call_id": frame.CallID})
}

// active answers "is there a call in this conversation".
//
// The durable half of a ring. The notification that a call started is ephemeral and allowed
// to fail (ADR-0005), so a client that missed it — offline, or on a node that dropped the
// publish — finds out by asking, and this is what it asks.
func (h *Handler) active(ctx context.Context, session Session, raw []byte) error {
	var frame activeFrame
	if err := json.Unmarshal(raw, &frame); err != nil {
		return fmt.Errorf("call.active was not valid JSON: %w", err)
	}

	call, err := h.service.Active(ctx,
		domain.ConversationID(frame.ConversationID), domain.AccountID(session.AccountID()))
	if err != nil {
		// No call is an answer, not a failure: a client asks about every conversation it
		// holds and most of them will not have one.
		return session.Send(noCallView{Type: "call.none", ConversationID: frame.ConversationID})
	}

	return session.Send(view("call.current", call, ""))
}

// SocketClosed takes a departed client out of whatever call it was in.
//
// A crashed tab says nothing, and something has to notice: without this a call keeps a
// participant nobody can see, CL-3 never fires, and everyone else keeps a tile for somebody
// who is not there.
func (h *Handler) SocketClosed(ctx context.Context, session Session) {
	device := session.DeviceID()

	h.mutex.Lock()
	// Only if this socket is still the one on record. A client that reconnected has a newer
	// session under the same device, and tearing down its call because the old socket
	// finally closed would drop somebody who is present.
	if h.sessions[device] == session {
		delete(h.sessions, device)
	} else {
		h.mutex.Unlock()
		return
	}
	h.mutex.Unlock()

	h.service.LeaveAll(ctx, domain.AccountID(session.AccountID()), domain.DeviceID(device))
}

// Offer delivers a media node's offer to the participant it is for.
//
// Called when a call gains a publisher: everyone already in it negotiated before that track
// existed, and this is how they are told. It is also called for participants on other api
// nodes, because a media node in its own process broadcasts its offers rather than knowing
// which socket is where — so "this is not mine" is the ordinary case and not a problem.
func (h *Handler) Offer(_ context.Context, callID, participantID, offer string) error {
	h.mutex.RLock()
	session := h.sessions[participantID]
	h.mutex.RUnlock()

	if session == nil {
		h.logger.Debug("no socket here for a renegotiation",
			slog.String("call", callID), slog.String("participant", participantID))
		return ErrNoSocket
	}

	if err := session.Send(offerView{Type: "call.offer", CallID: callID, SDP: offer}); err != nil {
		h.logger.Warn("send offer", slog.Any("error", err))
		return fmt.Errorf("send offer: %w", err)
	}
	return nil
}

// ErrNoSocket means this process holds no connection for that participant.
//
// Reported rather than swallowed so the media plane can tell an undelivered offer from a
// delivered one. What it means depends on where forwarding happens: in process, that the
// client has gone; out of process, that this was somebody else's participant.
var ErrNoSocket = errors.New("calling: no socket for that participant")

func (h *Handler) remember(session Session) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.sessions[session.DeviceID()] = session
}

// describe turns a domain error into something a client can act on.
//
// Deliberately plain text rather than codes: this rides an error frame that already carries
// one, and the distinctions that matter to a caller — you may not join, the call is full,
// the call is over — are all things a person reads.
func describe(err error) error {
	switch {
	case errors.Is(err, domain.ErrNotPermitted):
		return fmt.Errorf("you may not join this call")
	case errors.Is(err, domain.ErrCallIsFull):
		return fmt.Errorf("this call is full")
	case errors.Is(err, domain.ErrCallEnded):
		return fmt.Errorf("this call has ended")
	case errors.Is(err, domain.ErrCallNotFound):
		return fmt.Errorf("no such call")
	case errors.Is(err, domain.ErrNoMediaNode):
		return fmt.Errorf("no media node is available")
	default:
		return err
	}
}
