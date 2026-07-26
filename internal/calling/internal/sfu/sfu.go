// Package sfu forwards media between the participants of a call.
//
// Selective forwarding, not mixing: the server copies RTP from each publisher to each
// other participant and never decodes it (ADR-0006). That is what makes a call cost CPU
// proportional to connections rather than to pixels, and what makes it possible at all in
// Go.
//
// This package knows about calls and participants as identifiers and nothing else. Who
// may join, when a call ends and what a conversation is are all decided before anything
// here is asked to do something — which is why it has no database, no clock and no
// authorisation.
package sfu

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"
)

// Options configure a server.
type Options struct {
	// UDPPortMin and UDPPortMax bound the range media arrives on.
	//
	// Bounded deliberately: an SFU behind a firewall needs a range somebody can open, and
	// the alternative — an ephemeral port per transport out of the whole range — is
	// undeployable. Zero for both means "let the operating system choose", which is right
	// for tests and wrong for a deployment.
	UDPPortMin uint16
	UDPPortMax uint16

	// PublicIP is what candidates advertise, for a node behind a one-to-one NAT.
	// Empty means advertise what the interfaces report.
	PublicIP string

	Logger *slog.Logger
}

// Server holds every call this node is forwarding.
type Server struct {
	api    *webrtc.API
	logger *slog.Logger

	mutex sync.RWMutex
	calls map[string]*call
	// renegotiate delivers a server-initiated offer. Installed after construction,
	// because the thing that delivers it is the signalling layer and that needs this
	// server to exist first.
	renegotiate Renegotiator
}

// New returns a server.
func New(options Options) (*Server, error) {
	engine := &webrtc.MediaEngine{}
	// Only what is forwarded. Registering every codec Pion knows would let a client
	// negotiate something this server has no keyframe detection for, and the failure
	// would be a black rectangle rather than an error.
	if err := engine.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType: webrtc.MimeTypeVP8, ClockRate: 90000,
			// The feedback CL-6 is made of: retransmission for lost packets, and a
			// keyframe request for a decoder that has lost its reference frame.
			RTCPFeedback: []webrtc.RTCPFeedback{
				{Type: "nack"}, {Type: "nack", Parameter: "pli"},
				{Type: "ccm", Parameter: "fir"}, {Type: webrtc.TypeRTCPFBTransportCC},
			},
		},
		PayloadType: 96,
	}, webrtc.RTPCodecTypeVideo); err != nil {
		return nil, fmt.Errorf("register vp8: %w", err)
	}
	if err := engine.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2,
			RTCPFeedback: []webrtc.RTCPFeedback{{Type: webrtc.TypeRTCPFBTransportCC}},
		},
		PayloadType: 111,
	}, webrtc.RTPCodecTypeAudio); err != nil {
		return nil, fmt.Errorf("register opus: %w", err)
	}

	registry := &interceptor.Registry{}
	if err := webrtc.RegisterDefaultInterceptors(engine, registry); err != nil {
		return nil, fmt.Errorf("register interceptors: %w", err)
	}

	settings := webrtc.SettingEngine{}
	if options.UDPPortMin > 0 && options.UDPPortMax >= options.UDPPortMin {
		if err := settings.SetEphemeralUDPPortRange(options.UDPPortMin, options.UDPPortMax); err != nil {
			return nil, fmt.Errorf("set udp port range: %w", err)
		}
	}
	if options.PublicIP != "" {
		settings.SetNAT1To1IPs([]string{options.PublicIP}, webrtc.ICECandidateTypeHost)
	}

	logger := options.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	return &Server{
		api: webrtc.NewAPI(
			webrtc.WithMediaEngine(engine),
			webrtc.WithInterceptorRegistry(registry),
			webrtc.WithSettingEngine(settings),
		),
		logger: logger,
		calls:  make(map[string]*call),
	}, nil
}

// Renegotiator is how the server reaches a participant it needs to re-offer to.
//
// Renegotiation is unavoidable and it is worth saying why, because the temptation to
// avoid it is strong. A call of two people is already asymmetric: the second to join
// receives the first's tracks in the answer to their own offer, but the first knows
// nothing of the second's until told. Something has to send an offer the other way.
//
// The alternative — having clients pre-declare a receive slot per possible participant and
// filling them in place — removes renegotiation at the cost of a fixed participant limit
// baked into every client and a demuxing scheme that depends on the browser. Not worth it.
type Renegotiator func(ctx context.Context, callID, participantID, offer string)

// SetRenegotiator installs the callback used to re-offer to participants.
//
// Set after construction rather than passed in, because the thing that delivers an offer
// is the signalling layer, and the signalling layer needs the server to exist first. A nil
// renegotiator means offers are dropped and a call never gets past two-way — which is why
// this logs rather than being silently permitted.
func (s *Server) SetRenegotiator(renegotiate Renegotiator) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.renegotiate = renegotiate
}

// call is one call's participants and the tracks they publish.
type call struct {
	id string

	mutex        sync.RWMutex
	participants map[string]*participant
}

// participant is one device's transport into a call.
type participant struct {
	id         string
	connection *webrtc.PeerConnection

	mutex sync.Mutex
	// published is what this participant sends, so a later joiner can be given it.
	published []*published
	// receiving is what has already been added to this participant's connection, so a
	// second pass does not add a track twice.
	receiving map[string]bool
	// negotiating guards against two renegotiations overlapping. A second offer sent
	// before the first is answered puts the connection in a state neither side can
	// resolve, and it is the classic way an SFU wedges under a burst of joins.
	negotiating bool
	// pending records that something changed while a renegotiation was in flight.
	pending bool
}

// published is one forwarded track and what is needed to send feedback to its publisher.
type published struct {
	key   string
	track *webrtc.TrackLocalStaticRTP
	// publisher and ssrc are how a receiver's keyframe request reaches whoever can
	// answer it: feedback travels the opposite way to media, and the SSRC means something
	// different on each connection.
	publisher *webrtc.PeerConnection
	ssrc      uint32
}

// Join accepts a participant's offer and returns this node's answer.
//
// The call is created on first join, which is deliberate: the lifecycle lives in the
// Calling domain, and a media node that also decided when calls exist would be a second
// place for that rule to disagree from.
func (s *Server) Join(callID, participantID, offer string) (string, error) {
	connection, err := s.api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		return "", fmt.Errorf("new peer connection: %w", err)
	}

	joining := &participant{
		id:         participantID,
		connection: connection,
		receiving:  make(map[string]bool),
	}

	held := s.callFor(callID)
	held.mutex.Lock()
	// A rejoin from the same device replaces its transport. Without this a client that
	// reconnected would have two connections in the call and would publish twice.
	if previous, found := held.participants[participantID]; found {
		go func() { _ = previous.connection.Close() }()
	}
	held.participants[participantID] = joining
	others := held.others(participantID)
	held.mutex.Unlock()

	connection.OnTrack(func(remote *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		s.forward(callID, joining, remote)
	})
	connection.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		s.logger.Debug("participant connection",
			slog.String("call", callID), slog.String("participant", participantID),
			slog.String("state", state.String()))

		// A transport that fails is a participant who is gone, whatever the signalling
		// said. Cleaned up here so a crashed client does not leave its tracks being
		// forwarded to everyone forever.
		if state == webrtc.PeerConnectionStateFailed || state == webrtc.PeerConnectionStateClosed {
			s.release(callID, participantID)
		}
	})

	// The offer first, then the tracks. Reversing these negotiates cleanly and delivers
	// nothing: a track added before the remote description exists creates a transceiver of
	// its own, and the answer then carries more media sections than the offer asked about.
	// Found in phase 8 against the stub.
	if err := connection.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer, SDP: offer,
	}); err != nil {
		return "", fmt.Errorf("set remote description: %w", err)
	}

	for _, other := range others {
		for _, track := range other.publishedTracks() {
			if err := s.deliver(joining, track); err != nil {
				s.logger.Warn("add existing track", slog.Any("error", err))
			}
		}
	}

	answer, err := connection.CreateAnswer(nil)
	if err != nil {
		return "", fmt.Errorf("create answer: %w", err)
	}

	// Gathering completes before the answer is returned, so signalling is a single
	// exchange. The harness established in phase 8 that this is sufficient for a node on
	// a known address; a browser may still trickle its own candidates and nothing here
	// depends on whether it does.
	gathered := webrtc.GatheringCompletePromise(connection)
	if err := connection.SetLocalDescription(answer); err != nil {
		return "", fmt.Errorf("set local description: %w", err)
	}
	<-gathered

	return connection.LocalDescription().SDP, nil
}

// Answer applies a participant's answer to an offer this server sent.
func (s *Server) Answer(callID, participantID, answer string) error {
	joined, found := s.participant(callID, participantID)
	if !found {
		return ErrParticipantNotFound
	}

	if err := joined.connection.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeAnswer, SDP: answer,
	}); err != nil {
		return fmt.Errorf("set remote description: %w", err)
	}

	joined.mutex.Lock()
	joined.negotiating = false
	again := joined.pending
	joined.pending = false
	joined.mutex.Unlock()

	// Something arrived while that exchange was in flight. Renegotiated now rather than
	// left for the next join, because "the next join" may never come and the participant
	// would simply be missing somebody.
	if again {
		s.renegotiateWith(callID, joined)
	}
	return nil
}

// Leave releases a participant's transport.
func (s *Server) Leave(callID, participantID string) {
	s.release(callID, participantID)
}

// forward copies one published track to every other participant.
func (s *Server) forward(callID string, from *participant, remote *webrtc.TrackRemote) {
	local, err := webrtc.NewTrackLocalStaticRTP(
		remote.Codec().RTPCodecCapability, remote.ID(), remote.StreamID())
	if err != nil {
		s.logger.Error("new forwarding track", slog.Any("error", err))
		return
	}

	track := &published{
		key:       from.id + ":" + remote.ID(),
		track:     local,
		publisher: from.connection,
		ssrc:      uint32(remote.SSRC()),
	}

	from.mutex.Lock()
	from.published = append(from.published, track)
	from.mutex.Unlock()

	held := s.callFor(callID)
	held.mutex.RLock()
	others := held.others(from.id)
	held.mutex.RUnlock()

	for _, other := range others {
		if err := s.deliver(other, track); err != nil {
			s.logger.Warn("deliver track", slog.Any("error", err))
			continue
		}
		// Everyone already in the call needs an offer: they negotiated before this track
		// existed. This is the renegotiation that makes a two-way call work at all.
		//
		// On its own goroutine, and that is not a detail. This function runs on Pion's
		// OnTrack callback, and the read loop below does not start until it returns —
		// so renegotiating inline means waiting for ICE gathering and a client's answer
		// before forwarding a single packet. With two participants it merely delays the
		// first frame; with three it wedges, because each new publisher blocks behind the
		// previous one's exchange.
		go s.renegotiateWith(callID, other)
	}

	// A keyframe as soon as there is anyone to send it to, so a joiner does not wait for
	// the publisher's next scheduled one — up to two seconds of nothing, which is the
	// difference between NF-3 being met and missed.
	if len(others) > 0 {
		s.requestKeyframe(from.connection, track.ssrc)
	}

	defer func() {
		from.mutex.Lock()
		remaining := make([]*published, 0, len(from.published))
		for _, held := range from.published {
			if held.key != track.key {
				remaining = append(remaining, held)
			}
		}
		from.published = remaining
		from.mutex.Unlock()
	}()

	// Whole RTP packets, not samples: an SFU forwards and does not re-packetise.
	// Re-packetising would discard the payload descriptor a receiver needs to find frame
	// boundaries, which is the one part of the payload this server does read.
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

// deliver adds a forwarded track to a participant, once.
func (s *Server) deliver(to *participant, track *published) error {
	to.mutex.Lock()
	if to.receiving[track.key] {
		to.mutex.Unlock()
		return nil
	}
	to.receiving[track.key] = true
	to.mutex.Unlock()

	sender, err := to.connection.AddTrack(track.track)
	if err != nil {
		to.mutex.Lock()
		delete(to.receiving, track.key)
		to.mutex.Unlock()
		return fmt.Errorf("add track: %w", err)
	}

	// The sender has to be read for its RTCP to exist at all. Without this a receiver's
	// keyframe request fills a queue nobody drains and the publisher never hears — which
	// presents as a receiver stuck on a grey rectangle, nothing to do with feedback.
	// Phase 8 found this against the stub.
	go s.relayFeedback(sender, track)
	return nil
}

// relayFeedback carries a receiver's keyframe requests back to the publisher.
func (s *Server) relayFeedback(sender *webrtc.RTPSender, track *published) {
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
			switch packet.(type) {
			case *rtcp.PictureLossIndication, *rtcp.FullIntraRequest:
				// Relayed, because only the publisher can produce a keyframe.
				s.requestKeyframe(track.publisher, track.ssrc)
			case *rtcp.TransportLayerNack:
				// Not relayed. A NACK is answered from this server's own send buffer by
				// the responder interceptor, which is where the packet actually is —
				// forwarding it to the publisher would ask for a sequence number that
				// means something different there. This is CL-6's retransmission, and it
				// works because the server buffers rather than because the publisher does.
			}
		}
	}
}

func (s *Server) requestKeyframe(connection *webrtc.PeerConnection, ssrc uint32) {
	if err := connection.WriteRTCP([]rtcp.Packet{
		&rtcp.PictureLossIndication{MediaSSRC: ssrc},
	}); err != nil {
		s.logger.Debug("request keyframe", slog.Any("error", err))
	}
}

// renegotiateWith offers a participant the tracks it has gained.
func (s *Server) renegotiateWith(callID string, to *participant) {
	s.mutex.RLock()
	renegotiate := s.renegotiate
	s.mutex.RUnlock()

	if renegotiate == nil {
		s.logger.Error("no renegotiator installed; a participant will not see later joiners",
			slog.String("call", callID), slog.String("participant", to.id))
		return
	}

	to.mutex.Lock()
	if to.negotiating {
		// One exchange at a time. A second offer sent before the first is answered leaves
		// both sides in a state neither can resolve, which is how an SFU wedges under a
		// burst of joins.
		to.pending = true
		to.mutex.Unlock()
		return
	}
	to.negotiating = true
	to.mutex.Unlock()

	offer, err := to.connection.CreateOffer(nil)
	if err != nil {
		s.failedNegotiation(to, "create offer", err)
		return
	}

	gathered := webrtc.GatheringCompletePromise(to.connection)
	if err := to.connection.SetLocalDescription(offer); err != nil {
		s.failedNegotiation(to, "set local description", err)
		return
	}
	<-gathered

	ctx, cancel := context.WithTimeout(context.Background(), negotiationTimeout)
	defer cancel()
	renegotiate(ctx, callID, to.id, to.connection.LocalDescription().SDP)
}

// negotiationTimeout bounds how long delivering an offer may take. Short: the offer
// travels over a socket the client is already holding, or it does not travel at all.
const negotiationTimeout = 10 * time.Second

func (s *Server) failedNegotiation(to *participant, what string, err error) {
	to.mutex.Lock()
	to.negotiating = false
	to.mutex.Unlock()
	s.logger.Warn("renegotiate", slog.String("step", what), slog.Any("error", err))
}

// callFor returns the call, creating it if this is its first participant.
func (s *Server) callFor(callID string) *call {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	held, found := s.calls[callID]
	if !found {
		held = &call{id: callID, participants: make(map[string]*participant)}
		s.calls[callID] = held
	}
	return held
}

func (s *Server) participant(callID, participantID string) (*participant, bool) {
	s.mutex.RLock()
	held, found := s.calls[callID]
	s.mutex.RUnlock()
	if !found {
		return nil, false
	}

	held.mutex.RLock()
	defer held.mutex.RUnlock()
	joined, found := held.participants[participantID]
	return joined, found
}

// release closes a participant's transport and forgets the call when it is empty.
func (s *Server) release(callID, participantID string) {
	s.mutex.Lock()
	held, found := s.calls[callID]
	s.mutex.Unlock()
	if !found {
		return
	}

	held.mutex.Lock()
	leaving, present := held.participants[participantID]
	delete(held.participants, participantID)
	empty := len(held.participants) == 0
	held.mutex.Unlock()

	if present {
		_ = leaving.connection.Close()
	}
	if empty {
		// Forgotten rather than kept: the lifecycle is the Calling domain's, and a media
		// node holding an empty call is holding a memory leak with a name.
		s.mutex.Lock()
		if current, still := s.calls[callID]; still && current == held {
			current.mutex.RLock()
			stillEmpty := len(current.participants) == 0
			current.mutex.RUnlock()
			if stillEmpty {
				delete(s.calls, callID)
			}
		}
		s.mutex.Unlock()
	}
}

// Participants reports how many transports a call holds, for health and for tests.
func (s *Server) Participants(callID string) int {
	s.mutex.RLock()
	held, found := s.calls[callID]
	s.mutex.RUnlock()
	if !found {
		return 0
	}

	held.mutex.RLock()
	defer held.mutex.RUnlock()
	return len(held.participants)
}

// Calls reports how many calls this node is forwarding.
func (s *Server) Calls() int {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return len(s.calls)
}

// Close releases every call.
func (s *Server) Close() {
	s.mutex.Lock()
	calls := s.calls
	s.calls = make(map[string]*call)
	s.mutex.Unlock()

	for _, held := range calls {
		held.mutex.Lock()
		for _, joined := range held.participants {
			_ = joined.connection.Close()
		}
		held.participants = make(map[string]*participant)
		held.mutex.Unlock()
	}
}

// others returns every participant but one. Callers hold the call's lock.
func (c *call) others(except string) []*participant {
	others := make([]*participant, 0, len(c.participants))
	for id, joined := range c.participants {
		if id != except {
			others = append(others, joined)
		}
	}
	return others
}

func (p *participant) publishedTracks() []*published {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return append([]*published(nil), p.published...)
}

// ErrParticipantNotFound means the named participant has no transport on this node.
var ErrParticipantNotFound = errors.New("sfu: participant not found")
