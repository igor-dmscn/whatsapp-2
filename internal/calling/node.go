package calling

import (
	"log/slog"
	"net/http"

	"comms/internal/calling/internal/nodes"
	"comms/internal/calling/internal/sfu"
)

// MediaNode is the media plane as a process of its own (ADR-0007).
//
// Split from api because the two have nothing in common operationally: forwarding is CPU
// bound and needs a raw UDP port range that cannot go behind an HTTP load balancer, while
// holding sockets is I/O bound and scales with people rather than with pictures. Sharing a
// process means scaling both together, and it also means a call belongs to whichever api node
// happened to start it.
//
// It holds no database, no clock and no authorisation. Who may join, when a call exists and
// when it ends are all decided by an api node before this is asked to do anything — which is
// what keeps the lifecycle in one place instead of two that can disagree.
type MediaNode struct {
	server *sfu.Server
	media  *nodes.Media
}

// NewMediaNode returns a forwarding node. Options.Address and MediaNodeURL are not used: a
// node does not allocate calls and does not signal to anything.
func NewMediaNode(options Options) (*MediaNode, error) {
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

	return &MediaNode{server: server, media: nodes.NewMedia(server, logger)}, nil
}

// Routes registers the node's signalling surface: join, answer, leave, and the stream of
// offers it produces.
//
// Signalling only. Media never touches an HTTP handler — it arrives as UDP on the port range
// the options bound, which is why this surface can sit behind whatever an operator likes and
// the port range cannot.
func (n *MediaNode) Routes(mux *http.ServeMux) { n.media.Routes(mux) }

// Calls reports how many calls this node is forwarding, for health reporting.
func (n *MediaNode) Calls() int { return n.server.Calls() }

// Participants reports how many transports a call holds on this node.
func (n *MediaNode) Participants(callID string) int { return n.server.Participants(callID) }

// OfferSubscribers reports how many api nodes are listening for this node's offers.
//
// Worth reporting on its own rather than folded into a status word: zero means every call on
// this node will stop at whoever negotiated first, while everything else about the process
// looks healthy. It is the number to look at when a call works one way and not the other.
func (n *MediaNode) OfferSubscribers() int { return n.media.Subscribers() }

// Close releases every transport.
func (n *MediaNode) Close() { n.server.Close() }
