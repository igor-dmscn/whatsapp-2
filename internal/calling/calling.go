// Package calling is the whole of the Calling context's public surface.
//
// The model, use cases, media plane, Postgres adapters and signalling live under
// internal/, unreachable from outside this tree by compiler rule rather than by convention
// (ADR-0011).
//
// Two planes, deliberately separate. Signalling is domain traffic — a few kilobytes of SDP
// exchanged once per participant — and rides the socket the client already has (ADR-0004).
// Media is UDP straight to the forwarding server and never touches an HTTP handler.
package calling

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	"comms/internal/calling/internal/api"
	"comms/internal/calling/internal/app"
	"comms/internal/calling/internal/domain"
	"comms/internal/calling/internal/nodes"
	"comms/internal/calling/internal/postgres"
	"comms/internal/calling/internal/sfu"
	"comms/internal/platform/database"
	"comms/internal/platform/id"
)

// Conversations is what Calling needs to know about Messaging.
//
// One question, in plain strings, because Calling must not name a Messaging type.
// Entitlement to join a call is entitlement to be in the conversation it belongs to (CL-1),
// and that lives in the context that owns membership.
type Conversations interface {
	MayJoin(ctx context.Context, conversationID, accountID string) (bool, error)
}

// Notifier tells connected clients a call changed. Satisfied by Messaging, which owns the
// socket fanout.
type Notifier interface {
	CallChanged(ctx context.Context, conversationID, callID string) error
}

// Session is one client's connection, as much of it as signalling needs.
type Session interface {
	AccountID() string
	DeviceID() string
	Send(frame any) error
}

// Options are the choices a deployment makes about calling.
type Options struct {
	// Address is what this node is called in the calls it holds. Recorded on every call
	// (CL-4), so it must be something another process could resolve once media moves out.
	Address string
	// UDPPortMin and UDPPortMax bound the range media arrives on. A deployment needs a
	// range somebody can open in a firewall; zero means let the operating system choose,
	// which is right for tests and wrong for anything else.
	UDPPortMin uint16
	UDPPortMax uint16
	// PublicIP is what ICE candidates advertise, for a node behind a one-to-one NAT.
	PublicIP string
	Logger   *slog.Logger
}

// Module is a wired Calling context.
type Module struct {
	handler *api.Handler
	server  *sfu.Server
}

// New wires the context.
//
// The media plane is in this process. That is a deployment decision and the seam is
// deliberate: MediaNodes names a node by address on every call, so moving forwarding to its
// own binary (ADR-0007) is one more implementation of that port and nothing else. The trade
// being accepted meanwhile is that CPU-bound forwarding and I/O-bound sockets scale
// together — see docs/plan.md phase 9.
func New(
	db *sql.DB,
	conversations Conversations,
	notifier Notifier,
	options Options,
) (*Module, error) {
	logger := options.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	server, err := sfu.New(sfu.Options{
		UDPPortMin: options.UDPPortMin,
		UDPPortMax: options.UDPPortMax,
		PublicIP:   options.PublicIP,
		Logger:     logger,
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // already named where it happened.
	}

	address := options.Address
	if address == "" {
		address = "local"
	}

	service := app.NewService(
		postgres.NewCallRepository(db),
		conversations,
		nodes.NewLocal(address, server),
		notifier,
		postgres.NewOutboxPublisher(db),
		database.NewConn(db),
		ids{},
		time.Now,
		logger,
	)

	handler := api.NewHandler(service, logger)

	// The loop closed: the media plane produces offers when a call gains a publisher, and
	// signalling delivers them to the participant they are for. Without this a call never
	// gets past the joiner's own view — everyone already in it negotiated before the new
	// track existed.
	server.SetRenegotiator(func(ctx context.Context, callID, participantID, offer string) {
		handler.Offer(ctx, callID, participantID, offer)
	})

	return &Module{handler: handler, server: server}, nil
}

// Frames is the socket frame family this context answers, for registration by cmd/api.
func (m *Module) Frames() string { return "call" }

// HandleFrame answers one call frame. This is what Messaging delegates to.
func (m *Module) HandleFrame(ctx context.Context, session Session, frameType string, raw []byte) error {
	return m.handler.HandleFrame(ctx, session, frameType, raw) //nolint:wrapcheck // the handler's error reaches the client.
}

// SocketClosed cleans up after a client that vanished.
func (m *Module) SocketClosed(ctx context.Context, session Session) {
	m.handler.SocketClosed(ctx, session)
}

// Calls reports how many calls this node is forwarding, for health reporting.
func (m *Module) Calls() int { return m.server.Calls() }

// Close releases every transport this node holds.
func (m *Module) Close() { m.server.Close() }

// ids generates calling identifiers.
type ids struct{}

var _ domain.IDs = ids{}

func (ids) NewCallID() domain.CallID { return domain.CallID(id.New()) }
