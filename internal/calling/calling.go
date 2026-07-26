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
	// MediaNodeURL is the forwarding process this node allocates calls to, as a base URL.
	//
	// Empty means forward in this process, which is what a development machine and every
	// test wants. Set means media is out of process (ADR-0007), the forwarding node is
	// shared, and a call therefore no longer belongs to whichever api node happened to
	// start it — which is the whole point.
	MediaNodeURL string

	// Address is what this node is called in the calls it holds. Recorded on every call
	// (CL-4), so it must be something another process could resolve once media moves out.
	// Ignored when MediaNodeURL is set, because then the node has an address of its own.
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
	// server is the in-process media plane, and is nil when forwarding is out of process.
	server *sfu.Server
	// remote is the media node this process signals to, and is nil when forwarding is here.
	// One of the two is always set; which one is the deployment's choice.
	remote *nodes.Remote
}

// New wires the context.
//
// Where media is forwarded is the one deployment choice this makes, and it is a choice
// between two implementations of one port. In process, a call belongs to the api node that
// started it and a client whose socket landed elsewhere cannot join it. Out of process, the
// forwarding node is shared and that restriction goes away — at the cost of a second thing
// to deploy and a reverse channel for the offers it produces.
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

	module := &Module{}

	var media domain.MediaNodes
	if options.MediaNodeURL != "" {
		module.remote = nodes.NewRemote(options.MediaNodeURL, logger)
		media = module.remote
	} else {
		server, err := sfu.New(sfu.Options{
			UDPPortMin: options.UDPPortMin,
			UDPPortMax: options.UDPPortMax,
			PublicIP:   options.PublicIP,
			Logger:     logger,
		})
		if err != nil {
			return nil, err //nolint:wrapcheck // already named where it happened.
		}
		module.server = server

		address := options.Address
		if address == "" {
			address = "local"
		}
		media = nodes.NewLocal(address, server)
	}

	service := app.NewService(
		postgres.NewCallRepository(db),
		conversations,
		media,
		notifier,
		postgres.NewOutboxPublisher(db),
		database.NewConn(db),
		ids{},
		time.Now,
		logger,
	)

	module.handler = api.NewHandler(service, logger)

	// The loop closed: the media plane produces offers when a call gains a publisher, and
	// signalling delivers them to the participant they are for. Without this a call never
	// gets past the joiner's own view — everyone already in it negotiated before the new
	// track existed.
	//
	// Only for the in-process plane. A remote one publishes its offers to every api node
	// instead, and Run is what receives them.
	if module.server != nil {
		module.server.SetRenegotiator(func(ctx context.Context, callID, participantID, offer string) error {
			return module.handler.Offer(ctx, callID, participantID, offer)
		})
	}

	return module, nil
}

// Run receives offers from a remote media node, until ctx is done.
//
// Returns immediately when forwarding is in this process, where the media plane can call the
// handler directly. Started as a goroutine by cmd/api, alongside the one Messaging runs for
// the same reason: one subscription per process serves every socket it holds.
func (m *Module) Run(ctx context.Context) {
	if m.remote == nil {
		return
	}
	m.remote.Offers(ctx, func(callID, participantID, offer string) {
		// Every api node receives every offer, because the media plane knows a participant
		// by device and not by which socket holds it. Most of them belong to somebody else's
		// socket, and the error saying so is the ordinary case — there is nothing to report
		// it to, and reporting it would be reporting that this node is not the one.
		_ = m.handler.Offer(ctx, callID, participantID, offer)
	})
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

// Calls reports how many calls this process is forwarding, for health reporting.
//
// Zero when media is out of process, which is the truth rather than a gap: this process
// holds sockets and no transports, and the count that matters is the media node's own.
func (m *Module) Calls() int {
	if m.server == nil {
		return 0
	}
	return m.server.Calls()
}

// Close releases every transport this process holds.
func (m *Module) Close() {
	if m.server != nil {
		m.server.Close()
	}
}

// ids generates calling identifiers.
type ids struct{}

var _ domain.IDs = ids{}

func (ids) NewCallID() domain.CallID { return domain.CallID(id.New()) }
