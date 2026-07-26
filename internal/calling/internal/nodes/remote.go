package nodes

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"comms/internal/calling/internal/domain"
)

// Remote reaches a media node over HTTP.
//
// The second implementation of MediaNodes, and the whole of what moving media into its own
// process costs above here: nothing in the domain, the use cases or the signalling changes,
// because a node was always named by address on every call.
type Remote struct {
	// address is the node this api process allocates new calls to.
	//
	// Only new ones. Every other method is told which node the call is already on, and goes
	// there — so a deployment with several forwarding nodes routes correctly today, with
	// only the choice of which to allocate left to make.
	address string
	client  *http.Client
	logger  *slog.Logger
}

// NewRemote returns a node reached at address, which is a base URL.
func NewRemote(address string, logger *slog.Logger) *Remote {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Remote{
		address: strings.TrimSuffix(address, "/"),
		// No timeout on the client itself, because one of its requests is a stream that is
		// meant to last for the life of the process. Per-request deadlines come from the
		// context instead, which is the only way to have both.
		client: &http.Client{},
		logger: logger,
	}
}

var _ domain.MediaNodes = (*Remote)(nil)

// Allocate returns the configured node.
//
// Without asking it anything. A health check here would be a round trip on the path of every
// call that starts one, to learn something that can be false by the time the offer is sent —
// and the offer is sent moments later in the same use case, where a node that is not there
// produces a real error rather than a guess.
func (r *Remote) Allocate(context.Context) (string, error) {
	if r.address == "" {
		return "", domain.ErrNoMediaNode
	}
	return r.address, nil
}

// Join hands a participant's offer to the node holding the call and returns its answer.
func (r *Remote) Join(
	ctx context.Context,
	node string,
	call domain.CallID,
	participant domain.DeviceID,
	offer string,
) (string, error) {
	answered, err := r.send(ctx, http.MethodPost,
		participantURL(node, call, participant), sdpBody{SDP: offer})
	if err != nil {
		return "", err
	}

	var body sdpBody
	if err := json.Unmarshal(answered, &body); err != nil {
		return "", fmt.Errorf("decode the node's answer: %w", err)
	}
	if body.SDP == "" {
		return "", errors.New("the media node answered with no sdp")
	}
	return body.SDP, nil
}

// Answer carries a participant's answer to an offer the node sent.
func (r *Remote) Answer(
	ctx context.Context,
	node string,
	call domain.CallID,
	participant domain.DeviceID,
	answer string,
) error {
	_, err := r.send(ctx, http.MethodPost,
		participantURL(node, call, participant)+"/answer", sdpBody{SDP: answer})
	return err
}

// Leave tells the node a participant has gone.
func (r *Remote) Leave(
	ctx context.Context,
	node string,
	call domain.CallID,
	participant domain.DeviceID,
) error {
	_, err := r.send(ctx, http.MethodDelete, participantURL(node, call, participant), nil)
	return err
}

// Offers delivers the node's server-initiated offers to deliver, until ctx is done.
//
// Runs for the life of the api process. Reconnecting rather than returning on failure,
// because the alternative is an api node that is up, serving, holding sockets, and quietly
// unable to complete any call that gains a third participant.
func (r *Remote) Offers(ctx context.Context, deliver func(callID, participantID, offer string)) {
	for ctx.Err() == nil {
		if err := r.consume(ctx, deliver); err != nil && ctx.Err() == nil {
			r.logger.Warn("offer stream ended",
				slog.String("node", r.address), slog.Any("error", err))
		}

		// A fixed pause rather than a backoff. There is one node and it is deployed
		// alongside this process: reconnecting promptly matters more than being polite to
		// something that is either there or being restarted.
		select {
		case <-ctx.Done():
			return
		case <-time.After(reconnectDelay):
		}
	}
}

const reconnectDelay = time.Second

// consume reads one connection's worth of offers.
func (r *Remote) consume(ctx context.Context, deliver func(callID, participantID, offer string)) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, r.address+offersPath, nil)
	if err != nil {
		return fmt.Errorf("build offer stream request: %w", err)
	}

	response, err := r.client.Do(request)
	if err != nil {
		return fmt.Errorf("open offer stream: %w", err)
	}
	defer func() { _ = response.Body.Close() }()

	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("offer stream: %s", response.Status)
	}
	r.logger.Info("subscribed to a media node's offers", slog.String("node", r.address))

	// One decoder for the life of the connection: the response is a sequence of JSON
	// values, not one document, and the keepalive newlines between them are whitespace it
	// steps over.
	decoder := json.NewDecoder(response.Body)
	for {
		var message offerMessage
		if err := decoder.Decode(&message); err != nil {
			return fmt.Errorf("read offer: %w", err)
		}
		if message.ParticipantID == "" || message.SDP == "" {
			continue
		}
		// Every subscriber receives every offer, so most of these are for somebody else's
		// socket. deliver is what knows the difference, and it drops what is not its own.
		deliver(message.CallID, message.ParticipantID, message.SDP)
	}
}

// send makes one request and returns its body, or an error naming what went wrong.
//
// Read whole rather than streamed, because every body here is a few kilobytes of SDP and
// reading it to the end is what lets the connection be reused for the next join.
func (r *Remote) send(ctx context.Context, method, address string, body any) ([]byte, error) {
	// A deadline of its own, because the context this arrives with belongs to a client's
	// socket and may have none. A media node that has wedged rather than died would
	// otherwise hold a join open for as long as the client is willing to wait, which is
	// forever — and the client cannot retry something that has not failed.
	ctx, cancel := context.WithTimeout(ctx, signallingTimeout)
	defer cancel()

	var payload io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("encode request: %w", err)
		}
		payload = bytes.NewReader(encoded)
	}

	request, err := http.NewRequestWithContext(ctx, method, address, payload)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	if payload != nil {
		request.Header.Set("Content-Type", "application/json")
	}

	response, err := r.client.Do(request)
	if err != nil {
		// A node that cannot be reached is a node that is not available, which is the error
		// the client-facing layer already knows how to describe.
		return nil, fmt.Errorf("%w: %s: %w", domain.ErrNoMediaNode, address, err)
	}
	defer func() { _ = response.Body.Close() }()

	read, err := io.ReadAll(io.LimitReader(response.Body, maxSDPBytes))
	if err != nil {
		return nil, fmt.Errorf("read the node's response: %w", err)
	}

	if response.StatusCode >= http.StatusBadRequest {
		// The node's own message. It says which step failed and why, and the alternative is
		// a status code and a guess.
		return nil, fmt.Errorf("media node: %s: %s", response.Status, strings.TrimSpace(string(read)))
	}
	return read, nil
}

// signallingTimeout bounds one request to a media node.
//
// Generous for what it is — an SDP exchange over a local network — because a node gathers ICE
// candidates to completion before answering a join, and that is the slow part.
//
// It must stay comfortably *above* the node's own cap on gathering, and getting that wrong is
// not a subtle failure: with the two set equal, a gather slow enough to hit the cap raced the
// deadline here and the client was told no media node was available, for a node that was about
// to answer. Fifteen seconds against the node's five.
const signallingTimeout = 15 * time.Second

// participantURL builds the address of one participant on one node.
func participantURL(node string, call domain.CallID, participant domain.DeviceID) string {
	path := strings.Replace(participantPath, "{call}", url.PathEscape(string(call)), 1)
	path = strings.Replace(path, "{participant}", url.PathEscape(string(participant)), 1)
	return strings.TrimSuffix(node, "/") + path
}
