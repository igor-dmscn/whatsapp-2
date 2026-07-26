package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/pion/webrtc/v4"
)

// NodeSignaller signals to a real media node — cmd/sfu — over HTTP.
//
// The stub's signaller drives the stub. This drives the thing that ships, which is what a
// capacity claim has to be measured against: NF-14 is about a deployed forwarding process on
// four vCPUs, and a number produced against an in-process stub would be a number about the
// harness.
//
// It re-declares the node's three paths rather than importing them, because they live behind
// Calling's internal fence (ADR-0011) and a load tool is not a reason to open it. The
// duplication is safe in the way that matters: a path that no longer matches is a 404 on the
// first join, which is the loudest failure available. What must never be duplicated is
// something whose mismatch is silent.
type NodeSignaller struct {
	baseURL       string
	callID        string
	participantID string
	client        *http.Client
	logger        *slog.Logger

	offers chan string
	// stop ends the offer stream. A peer that has left must not keep a stream open: twelve
	// peers each holding one is twelve subscribers the node fans every offer out to.
	stop     context.CancelFunc
	stopOnce sync.Once
}

// NewNodeSignaller returns a signaller for one participant of one call on the node at baseURL.
func NewNodeSignaller(baseURL, callID, participantID string, logger *slog.Logger) *NodeSignaller {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &NodeSignaller{
		baseURL:       strings.TrimSuffix(baseURL, "/"),
		callID:        callID,
		participantID: participantID,
		// No client timeout: one of its requests is a stream meant to last the whole run.
		client: &http.Client{},
		logger: logger,
		offers: make(chan string, 4),
	}
}

var (
	_ Signaller    = (*NodeSignaller)(nil)
	_ Renegotiable = (*NodeSignaller)(nil)
)

// Join subscribes to the node's offers and then offers to publish and receive.
//
// In that order, and it matters. The node broadcasts an offer to whoever is subscribed at the
// moment it is produced, so joining first leaves a window in which another participant's
// arrival produces an offer this peer never sees. The node retries, but a load run should be
// measuring forwarding rather than the retry.
func (n *NodeSignaller) Join(
	ctx context.Context,
	offer webrtc.SessionDescription,
) (webrtc.SessionDescription, error) {
	// Deliberately not the request's context. The stream outlives this call — it lives as
	// long as the participant does — and tying it to a join would close it on return.
	streaming, stop := context.WithCancel(context.WithoutCancel(ctx))
	n.stop = stop
	ready := make(chan struct{})
	go n.consumeOffers(streaming, ready)
	<-ready

	body, err := json.Marshal(sdpBody{SDP: offer.SDP})
	if err != nil {
		return webrtc.SessionDescription{}, fmt.Errorf("encode offer: %w", err)
	}

	answered, err := n.send(ctx, http.MethodPost, n.participantURL(), body)
	if err != nil {
		return webrtc.SessionDescription{}, err
	}

	var reply sdpBody
	if err := json.Unmarshal(answered, &reply); err != nil {
		return webrtc.SessionDescription{}, fmt.Errorf("decode answer: %w", err)
	}
	return webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: reply.SDP}, nil
}

// Answer replies to an offer the node sent.
func (n *NodeSignaller) Answer(ctx context.Context, answer string) error {
	body, err := json.Marshal(sdpBody{SDP: answer})
	if err != nil {
		return fmt.Errorf("encode answer: %w", err)
	}

	_, err = n.send(ctx, http.MethodPost, n.participantURL()+"/answer", body)
	return err
}

// Offers yields the offers the node sent for this participant.
func (n *NodeSignaller) Offers() <-chan string { return n.offers }

// Leave releases the participant and closes the offer stream.
func (n *NodeSignaller) Leave(ctx context.Context) error {
	n.stopOnce.Do(func() {
		if n.stop != nil {
			n.stop()
		}
	})

	_, err := n.send(ctx, http.MethodDelete, n.participantURL(), nil)
	return err
}

// consumeOffers reads the node's broadcast and keeps the ones addressed here.
//
// ready is closed once the subscription exists, so that Join can wait for it. Closed on
// failure too: a peer that cannot subscribe should still try to join and fail on something
// informative, rather than hanging here.
func (n *NodeSignaller) consumeOffers(ctx context.Context, ready chan struct{}) {
	defer close(n.offers)

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, n.baseURL+nodeOffersPath, nil)
	if err != nil {
		close(ready)
		return
	}

	response, err := n.client.Do(request)
	if err != nil {
		n.logger.Warn("subscribe to offers", slog.Any("error", err))
		close(ready)
		return
	}
	defer func() { _ = response.Body.Close() }()
	close(ready)

	decoder := json.NewDecoder(response.Body)
	for {
		var message struct {
			CallID        string `json:"call_id"`
			ParticipantID string `json:"participant_id"`
			SDP           string `json:"sdp"`
		}
		if err := decoder.Decode(&message); err != nil {
			return
		}
		// Somebody else's offer, which is the ordinary case: the node has no idea which
		// process holds which participant, so it sends everything to everyone.
		if message.ParticipantID != n.participantID || message.SDP == "" {
			continue
		}

		select {
		case n.offers <- message.SDP:
		case <-ctx.Done():
			return
		}
	}
}

func (n *NodeSignaller) send(ctx context.Context, method, address string, body []byte) ([]byte, error) {
	var payload io.Reader
	if body != nil {
		payload = bytes.NewReader(body)
	}

	request, err := http.NewRequestWithContext(ctx, method, address, payload)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	if payload != nil {
		request.Header.Set("Content-Type", "application/json")
	}

	response, err := n.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w", method, address, err)
	}
	defer func() { _ = response.Body.Close() }()

	read, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if response.StatusCode >= http.StatusBadRequest {
		return nil, fmt.Errorf("%s %s returned %s: %s",
			method, address, response.Status, strings.TrimSpace(string(read)))
	}
	return read, nil
}

func (n *NodeSignaller) participantURL() string {
	return n.baseURL + "/v1/calls/" + url.PathEscape(n.callID) +
		"/participants/" + url.PathEscape(n.participantID)
}

// nodeOffersPath is where a media node streams the offers it produces.
const nodeOffersPath = "/v1/offers"

// sdpBody is the node's request and response shape: an SDP and nothing else.
type sdpBody struct {
	SDP string `json:"sdp"`
}
