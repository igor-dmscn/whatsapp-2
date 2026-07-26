package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"
)

// Stub forwards media between peers, and does nothing else.
//
// It exists so the harness can be verified before the media server is written, which is
// the whole reason this phase precedes phase 9. A harness with no server to point at can
// only be tested against itself, and a test that agrees with whatever the code believes
// is the one thing a test must not be.
//
// What it is not: a call lifecycle, an entitlement check, bandwidth estimation, layer
// selection, or anything about who a participant is. Those are phase 9's, and leaving
// them out is what keeps this a stub rather than a second implementation to keep in step.
//
// What it does do is the irreducible core — accept an offer, answer it, and copy every
// published track's RTP to every other participant — because a forwarding assertion needs
// something that forwards.
type Stub struct {
	logger *slog.Logger

	mutex        sync.Mutex
	participants map[string]*participant
	// published is every live track, so a peer joining after one exists is sent it
	// rather than waiting for the next one. That ordering is the most common way an SFU
	// is quietly broken: everything works when peers join in the right order.
	published map[string]*forwarded
	next      int
}

// forwarded is one published track and what it takes to send feedback back to whoever
// published it.
//
// The publisher's connection and original SSRC are held because feedback travels the
// opposite way to media: a receiver that cannot decode sends a PLI to the server, and the
// server has to turn that into a PLI addressed to the publisher's SSRC on the publisher's
// connection. Losing that mapping is how an SFU ends up with receivers waiting forever
// for a keyframe nobody asked for.
type forwarded struct {
	track     *webrtc.TrackLocalStaticRTP
	publisher *webrtc.PeerConnection
	ssrc      uint32
}

type participant struct {
	id         string
	connection *webrtc.PeerConnection
	// mine is what this participant published, so its own tracks are not sent back to it.
	mine map[string]bool
}

// NewStub returns a stub forwarder.
func NewStub(logger *slog.Logger) *Stub {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Stub{
		logger:       logger,
		participants: make(map[string]*participant),
		published:    make(map[string]*forwarded),
	}
}

// join accepts an offer and returns an answer.
func (s *Stub) join(offer webrtc.SessionDescription) (string, webrtc.SessionDescription, error) {
	connection, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		return "", webrtc.SessionDescription{}, fmt.Errorf("new peer connection: %w", err)
	}

	s.mutex.Lock()
	s.next++
	id := fmt.Sprintf("participant-%d", s.next)
	joining := &participant{id: id, connection: connection, mine: make(map[string]bool)}
	s.participants[id] = joining
	// Copied under the lock and used outside it: holding the lock through
	// AddTrack and the SDP exchange would serialise every join in a twenty-peer run.
	existing := make([]*forwarded, 0, len(s.published))
	for _, track := range s.published {
		existing = append(existing, track)
	}
	s.mutex.Unlock()

	connection.OnTrack(func(remote *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		s.forward(joining, remote)
	})

	// The offer first, then the tracks. That order is not cosmetic: adding a track before
	// the remote description exists creates a transceiver of its own, and the answer then
	// carries more media sections than the offer asked about — so the joiner negotiates
	// successfully and receives nothing. Setting the offer first lets each added track
	// claim one of the receive-only sections the joiner already declared.
	if err := connection.SetRemoteDescription(offer); err != nil {
		return "", webrtc.SessionDescription{}, fmt.Errorf("set remote description: %w", err)
	}

	// Everything already being published. A track that starts *after* this needs
	// renegotiation, which the stub deliberately does not do — so a test that wants a
	// receiver to see a publisher waits for the publisher's tracks to exist first. See
	// WaitForPublished.
	for _, track := range existing {
		sender, err := connection.AddTrack(track.track)
		if err != nil {
			return "", webrtc.SessionDescription{}, fmt.Errorf("add existing track: %w", err)
		}
		go s.relayFeedback(sender, track)
	}

	answer, err := connection.CreateAnswer(nil)
	if err != nil {
		return "", webrtc.SessionDescription{}, fmt.Errorf("create answer: %w", err)
	}

	gathered := webrtc.GatheringCompletePromise(connection)
	if err := connection.SetLocalDescription(answer); err != nil {
		return "", webrtc.SessionDescription{}, fmt.Errorf("set local description: %w", err)
	}
	<-gathered

	return id, *connection.LocalDescription(), nil
}

// forward copies one published track to everyone else.
func (s *Stub) forward(from *participant, remote *webrtc.TrackRemote) {
	local, err := webrtc.NewTrackLocalStaticRTP(
		remote.Codec().RTPCodecCapability, remote.ID(), remote.StreamID())
	if err != nil {
		s.logger.Error("new forwarding track", slog.Any("error", err))
		return
	}

	key := from.id + ":" + remote.ID()
	track := &forwarded{track: local, publisher: from.connection, ssrc: uint32(remote.SSRC())}

	s.mutex.Lock()
	s.published[key] = track
	from.mine[remote.ID()] = true
	others := make([]*participant, 0, len(s.participants))
	for _, other := range s.participants {
		if other.id != from.id {
			others = append(others, other)
		}
	}
	s.mutex.Unlock()

	for _, other := range others {
		sender, err := other.connection.AddTrack(local)
		if err != nil {
			s.logger.Debug("add forwarded track", slog.Any("error", err))
			continue
		}
		go s.relayFeedback(sender, track)
	}

	// A keyframe is asked for as soon as there is somewhere to send it. Without this a
	// receiver joining mid-stream waits for the publisher's next scheduled keyframe,
	// which is up to two seconds of nothing — the difference between NF-3 being met and
	// missed.
	if len(others) > 0 {
		s.requestKeyframe(from.connection, uint32(remote.SSRC()))
	}

	defer func() {
		s.mutex.Lock()
		delete(s.published, key)
		s.mutex.Unlock()
	}()

	// Reading and writing whole RTP packets rather than samples: an SFU forwards, it does
	// not re-packetise, and re-packetising would throw away the payload descriptor a
	// receiver needs to find frame boundaries.
	buffer := make([]byte, 1500)
	for {
		count, _, err := remote.Read(buffer)
		if err != nil {
			return
		}
		if _, err := local.Write(buffer[:count]); err != nil && !errors.Is(err, io.ErrClosedPipe) {
			s.logger.Debug("forward packet", slog.Any("error", err))
			return
		}
	}
}

// relayFeedback carries a receiver's keyframe requests back to the publisher.
//
// Without this the loop is open: a receiver sends a PLI, the sender's RTCP queue fills
// with it, and the publisher never hears. That is exactly the bug this stub had first,
// and it presents as "the harness cannot get a keyframe on demand" rather than as
// anything to do with feedback — the sender has to be *read* for its packets to exist.
func (s *Stub) relayFeedback(sender *webrtc.RTPSender, track *forwarded) {
	buffer := make([]byte, 1500)
	for {
		count, _, err := sender.Read(buffer)
		if err != nil {
			return
		}

		packets, err := rtcp.Unmarshal(buffer[:count])
		if err != nil {
			continue
		}
		for _, packet := range packets {
			// PLI only. A NACK is answered by the responder interceptor from the send
			// buffer, which is where the packet actually is — relaying it to the
			// publisher would ask for a retransmission of a sequence number that means
			// something different there.
			if _, ok := packet.(*rtcp.PictureLossIndication); ok {
				s.requestKeyframe(track.publisher, track.ssrc)
			}
		}
	}
}

func (s *Stub) requestKeyframe(connection *webrtc.PeerConnection, ssrc uint32) {
	if err := connection.WriteRTCP([]rtcp.Packet{
		&rtcp.PictureLossIndication{MediaSSRC: ssrc},
	}); err != nil {
		s.logger.Debug("request keyframe", slog.Any("error", err))
	}
}

// leave releases a participant.
func (s *Stub) leave(id string) {
	s.mutex.Lock()
	joining, found := s.participants[id]
	delete(s.participants, id)
	s.mutex.Unlock()

	if found {
		_ = joining.connection.Close()
	}
}

// WaitForPublished blocks until the stub is forwarding at least count tracks.
//
// Needed because the stub does not renegotiate: a receiver only gets tracks that existed
// when it joined. A real SFU renegotiates and phase 9 will, at which point this becomes
// unnecessary — it is a concession to the stub's stated limitation rather than a
// property of the interface.
func (s *Stub) WaitForPublished(ctx context.Context, count int) error {
	for {
		s.mutex.Lock()
		have := len(s.published)
		s.mutex.Unlock()
		if have >= count {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for %d published tracks, have %d: %w", count, have, ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// Participants reports how many are joined, which is what a lifecycle assertion needs.
func (s *Stub) Participants() int {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return len(s.participants)
}

// Close releases every participant.
func (s *Stub) Close() {
	s.mutex.Lock()
	connections := make([]*webrtc.PeerConnection, 0, len(s.participants))
	for _, joined := range s.participants {
		connections = append(connections, joined.connection)
	}
	s.participants = make(map[string]*participant)
	s.mutex.Unlock()

	for _, connection := range connections {
		_ = connection.Close()
	}
}

// --- signalling over HTTP ---

// joinRequest and joinResponse are the stub's wire format.
//
// HTTP because it is the least interesting choice available: what is being tested is
// media forwarding, and phase 9 carries the same exchange over the WebSocket that
// already exists (ADR-0004). Signaller is what keeps that substitution a one-line change.
type joinRequest struct {
	Offer webrtc.SessionDescription `json:"offer"`
}

type joinResponse struct {
	ParticipantID string                    `json:"participant_id"`
	Answer        webrtc.SessionDescription `json:"answer"`
}

// Handler serves the stub's signalling.
func (s *Stub) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /join", func(w http.ResponseWriter, r *http.Request) {
		var request joinRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		id, answer, err := s.join(request.Offer)
		if err != nil {
			s.logger.Error("join", slog.Any("error", err))
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(joinResponse{ParticipantID: id, Answer: answer}); err != nil {
			s.logger.Error("encode answer", slog.Any("error", err))
		}
	})

	mux.HandleFunc("DELETE /participants/{id}", func(w http.ResponseWriter, r *http.Request) {
		s.leave(r.PathValue("id"))
		w.WriteHeader(http.StatusNoContent)
	})

	return mux
}

// Serve starts the stub on a test server and returns it with a signaller factory.
func Serve(logger *slog.Logger) (*Stub, *httptest.Server) {
	stub := NewStub(logger)
	return stub, httptest.NewServer(stub.Handler())
}

// HTTPSignaller signals to a stub over HTTP.
type HTTPSignaller struct {
	baseURL string
	client  *http.Client
	// participantID is learned from the join response, and is what leaving names.
	participantID string
}

// NewHTTPSignaller returns a signaller for a stub at baseURL.
func NewHTTPSignaller(baseURL string) *HTTPSignaller {
	return &HTTPSignaller{baseURL: baseURL, client: http.DefaultClient}
}

var _ Signaller = (*HTTPSignaller)(nil)

func (h *HTTPSignaller) Join(
	ctx context.Context,
	offer webrtc.SessionDescription,
) (webrtc.SessionDescription, error) {
	body, err := json.Marshal(joinRequest{Offer: offer})
	if err != nil {
		return webrtc.SessionDescription{}, fmt.Errorf("encode offer: %w", err)
	}

	request, err := http.NewRequestWithContext(
		ctx, http.MethodPost, h.baseURL+"/join", bytes.NewReader(body))
	if err != nil {
		return webrtc.SessionDescription{}, fmt.Errorf("build join: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")

	response, err := h.client.Do(request)
	if err != nil {
		return webrtc.SessionDescription{}, fmt.Errorf("join: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		detail, _ := io.ReadAll(io.LimitReader(response.Body, 512))
		return webrtc.SessionDescription{}, fmt.Errorf("join returned %s: %s", response.Status, detail)
	}

	var answer joinResponse
	if err := json.NewDecoder(response.Body).Decode(&answer); err != nil {
		return webrtc.SessionDescription{}, fmt.Errorf("decode answer: %w", err)
	}
	h.participantID = answer.ParticipantID
	return answer.Answer, nil
}

func (h *HTTPSignaller) Leave(ctx context.Context) error {
	if h.participantID == "" {
		return nil
	}

	request, err := http.NewRequestWithContext(
		ctx, http.MethodDelete, h.baseURL+"/participants/"+h.participantID, nil)
	if err != nil {
		return fmt.Errorf("build leave: %w", err)
	}

	response, err := h.client.Do(request)
	if err != nil {
		return fmt.Errorf("leave: %w", err)
	}
	defer response.Body.Close()
	return nil
}
