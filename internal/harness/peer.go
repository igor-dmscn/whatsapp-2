package harness

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
)

// Signaller is how a peer reaches a call. This is the media server's interface, and
// writing it here rather than in the server is the point of building the harness first.
//
// One exchange, not trickle. A peer gathers its candidates, offers, and gets an answer
// that is complete — which is what a server on a known address with a known port range
// can always provide, and which removes an entire asynchronous protocol from both sides.
// Phase 9's browsers will trickle because browsers do, and the server will accept both;
// what this interface fixes is that trickling must remain optional.
//
// Deliberately not tied to a transport. The stub in this package signals over HTTP;
// phase 9 signals over the WebSocket that already exists (ADR-0004). Neither is visible
// from here.
type Signaller interface {
	// Join offers to publish and receive, and returns the server's answer.
	//
	// The call and the participant are the signaller's business, not the peer's: a peer
	// knows how to speak WebRTC and nothing about who it is.
	Join(ctx context.Context, offer webrtc.SessionDescription) (webrtc.SessionDescription, error)

	// Leave releases the participant. Called on close so that a load run of twenty peers
	// does not leave twenty participants behind.
	Leave(ctx context.Context) error
}

// Renegotiable is a signaller that can also carry an offer *from* the server.
//
// Added in phase 9, and its absence in phase 8 is the one thing the harness-first
// approach got wrong. The stub never re-offered, so the interface never needed to — but a
// real call of two people is asymmetric: the second to join receives the first's tracks in
// the answer to their own offer, and the first learns of the second's only if something
// offers the other way. A harness that cannot answer an offer can test forwarding to a
// joiner and never forwarding to whoever was already there.
//
// Optional rather than folded into Signaller, so the phase-8 stub and its signaller stay
// exactly as simple as they were.
type Renegotiable interface {
	// Offers yields offers the server sent. Closed when the signaller is done.
	Offers() <-chan string
	// Answer returns this peer's answer to the most recent offer.
	Answer(ctx context.Context, answer string) error
}

// PeerOptions configures a peer.
type PeerOptions struct {
	// Name appears in logs and in reports.
	Name string
	// Publish is whether this peer sends. A receive-only peer is the other half of every
	// forwarding assertion, and the shape a viewer in a broadcast takes.
	Publish bool
	Source  SourceOptions
	Logger  *slog.Logger
}

// Peer is one simulated participant.
type Peer struct {
	name       string
	connection *webrtc.PeerConnection
	signaller  Signaller
	source     *Source
	logger     *slog.Logger

	video *webrtc.TrackLocalStaticSample
	audio *webrtc.TrackLocalStaticSample

	mutex    sync.Mutex
	received map[string]*Arrivals
	joinedAt time.Time

	// publishing stops the send loops.
	stop     chan struct{}
	stopOnce sync.Once
	sending  sync.WaitGroup
}

// NewPeer builds a peer and connects it through the signaller.
//
// Everything happens here rather than in a separate Connect: a peer that exists but is
// not connected is a state nothing needs, and it is the state in which every "did you
// remember to call Connect" bug lives.
func NewPeer(ctx context.Context, signaller Signaller, options PeerOptions) (*Peer, error) {
	engine := &webrtc.MediaEngine{}
	if err := engine.RegisterDefaultCodecs(); err != nil {
		return nil, fmt.Errorf("register codecs: %w", err)
	}

	// The interceptors a real client has: NACK generation so lost packets are asked for,
	// RTCP reports so the server can estimate, TWCC so it can estimate well. Registered
	// rather than hand-rolled, because the point of the harness is to behave like a
	// client and these are what clients do.
	registry := &interceptor.Registry{}
	if err := webrtc.RegisterDefaultInterceptors(engine, registry); err != nil {
		return nil, fmt.Errorf("register interceptors: %w", err)
	}

	api := webrtc.NewAPI(webrtc.WithMediaEngine(engine), webrtc.WithInterceptorRegistry(registry))

	// No STUN server. Everything here is on one host or one network, and a public STUN
	// lookup would add a second of latency to every peer in a twenty-peer run for a
	// reflexive candidate nothing uses.
	connection, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		return nil, fmt.Errorf("new peer connection: %w", err)
	}

	logger := options.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	peer := &Peer{
		name:       options.Name,
		connection: connection,
		signaller:  signaller,
		source:     NewSource(options.Source),
		logger:     logger.With(slog.String("peer", options.Name)),
		received:   make(map[string]*Arrivals),
		stop:       make(chan struct{}),
	}

	connection.OnTrack(peer.onTrack)
	connection.OnICEConnectionStateChange(func(state webrtc.ICEConnectionState) {
		peer.logger.Debug("ice state", slog.String("state", state.String()))
	})

	if options.Publish {
		if err := peer.addTracks(); err != nil {
			_ = connection.Close()
			return nil, err
		}
	} else {
		// A receive-only peer still has to say what it is willing to receive, or the
		// server has nothing to answer with and sends nothing.
		for _, kind := range []webrtc.RTPCodecType{webrtc.RTPCodecTypeVideo, webrtc.RTPCodecTypeAudio} {
			if _, err := connection.AddTransceiverFromKind(kind,
				webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly}); err != nil {
				_ = connection.Close()
				return nil, fmt.Errorf("add %s transceiver: %w", kind, err)
			}
		}
	}

	if err := peer.negotiate(ctx); err != nil {
		_ = connection.Close()
		return nil, err
	}

	// A server that re-offers needs somebody listening. Started after the first exchange,
	// because an offer arriving mid-negotiation is a state neither side can resolve.
	if renegotiable, ok := signaller.(Renegotiable); ok {
		go peer.answerOffers(renegotiable)
	}

	if options.Publish {
		peer.publish()
	}
	return peer, nil
}

// answerOffers answers every offer the server sends.
//
// Serialised by the channel, which is the point: WebRTC permits one negotiation at a
// time, and two overlapping exchanges leave a connection in a state neither side can
// resolve.
func (p *Peer) answerOffers(signaller Renegotiable) {
	for offer := range signaller.Offers() {
		if err := p.answer(signaller, offer); err != nil {
			p.logger.Warn("answer offer", slog.Any("error", err))
		}
	}
}

func (p *Peer) answer(signaller Renegotiable, offer string) error {
	if err := p.connection.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer, SDP: offer,
	}); err != nil {
		return fmt.Errorf("set remote description: %w", err)
	}

	answer, err := p.connection.CreateAnswer(nil)
	if err != nil {
		return fmt.Errorf("create answer: %w", err)
	}

	gathered := webrtc.GatheringCompletePromise(p.connection)
	if err := p.connection.SetLocalDescription(answer); err != nil {
		return fmt.Errorf("set local description: %w", err)
	}
	<-gathered

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := signaller.Answer(ctx, p.connection.LocalDescription().SDP); err != nil {
		return fmt.Errorf("send answer: %w", err)
	}
	return nil
}

func (p *Peer) addTracks() error {
	var err error
	p.video, err = webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8}, "video", "harness-"+p.name)
	if err != nil {
		return fmt.Errorf("new video track: %w", err)
	}
	p.audio, err = webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus}, "audio", "harness-"+p.name)
	if err != nil {
		return fmt.Errorf("new audio track: %w", err)
	}

	for _, track := range []*webrtc.TrackLocalStaticSample{p.video, p.audio} {
		sender, err := p.connection.AddTrack(track)
		if err != nil {
			return fmt.Errorf("add track: %w", err)
		}
		// Feedback has to be read or it accumulates in the transport, and a peer that
		// ignores PLI never sends the keyframe a new receiver is waiting for — which
		// looks exactly like a broken SFU.
		go p.readFeedback(sender)
	}
	return nil
}

// negotiate does the whole exchange: offer, gather, answer.
func (p *Peer) negotiate(ctx context.Context) error {
	offer, err := p.connection.CreateOffer(nil)
	if err != nil {
		return fmt.Errorf("create offer: %w", err)
	}

	// Gathering completes before the offer is sent, which is what makes this exchange a
	// single round trip. SetLocalDescription starts it; the promise is how Pion says it
	// has finished.
	gathered := webrtc.GatheringCompletePromise(p.connection)
	if err := p.connection.SetLocalDescription(offer); err != nil {
		return fmt.Errorf("set local description: %w", err)
	}
	select {
	case <-gathered:
	case <-ctx.Done():
		return fmt.Errorf("gather candidates: %w", ctx.Err())
	}

	p.joinedAt = time.Now()
	answer, err := p.signaller.Join(ctx, *p.connection.LocalDescription())
	if err != nil {
		return fmt.Errorf("join: %w", err)
	}
	if err := p.connection.SetRemoteDescription(answer); err != nil {
		return fmt.Errorf("set remote description: %w", err)
	}
	return nil
}

// publish starts the send loops.
func (p *Peer) publish() {
	p.sending.Add(2)
	go func() {
		defer p.sending.Done()
		p.sendVideo()
	}()
	go func() {
		defer p.sending.Done()
		p.sendAudio()
	}()
}

func (p *Peer) sendVideo() {
	ticker := time.NewTicker(p.source.Interval())
	defer ticker.Stop()

	for {
		select {
		case <-p.stop:
			return
		case <-ticker.C:
			frame := p.source.Next()
			if err := p.video.WriteSample(media.Sample{
				Data: frame.Data, Duration: frame.Duration,
			}); err != nil && !errors.Is(err, io.ErrClosedPipe) {
				p.logger.Debug("write video", slog.Any("error", err))
				return
			}
			// The interval is re-read every frame so Throttle takes effect mid-run
			// rather than at the next reconnection.
			ticker.Reset(p.source.Interval())
		}
	}
}

// audioInterval is Opus's frame duration. Fixed at 20 ms, which is what every browser
// sends and what makes an audio track's packet count a usable clock.
const audioInterval = 20 * time.Millisecond

func (p *Peer) sendAudio() {
	ticker := time.NewTicker(audioInterval)
	defer ticker.Stop()

	// 32 kbit/s of Opus at 20 ms per packet.
	const bytesPerPacket = 32_000 / 8 / 50

	for {
		select {
		case <-p.stop:
			return
		case <-ticker.C:
			if err := p.audio.WriteSample(media.Sample{
				Data: audioFrame(bytesPerPacket), Duration: audioInterval,
			}); err != nil && !errors.Is(err, io.ErrClosedPipe) {
				p.logger.Debug("write audio", slog.Any("error", err))
				return
			}
		}
	}
}

// readFeedback drains RTCP from a sender and acts on what a publisher must act on.
func (p *Peer) readFeedback(sender *webrtc.RTPSender) {
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
			if _, ok := packet.(*rtcp.PictureLossIndication); ok {
				// A receiver cannot decode and needs a fresh start. Honouring this is
				// what makes CL-6's "decoder desync recovered by keyframe request" true
				// from the publisher's side, and it is the assertion phase 9 will make
				// when a layer switches.
				p.source.DemandKeyframe()
			}
		}
	}
}

// onTrack records everything that arrives on a forwarded track.
func (p *Peer) onTrack(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
	kind := track.Kind().String()
	// Keyed by SSRC as well as by name, because every publisher's video track is called
	// "video". Keying by name alone collapsed two publishers into one entry, so a
	// three-party call looked like a server that had renegotiated once and stopped — the
	// harness was miscounting, and the SFU was right. The stream identifier is in the key
	// too, purely so a log line says who it came from.
	key := kind + ":" + track.StreamID() + ":" + strconv.FormatUint(uint64(track.SSRC()), 10)

	p.mutex.Lock()
	arrivals, found := p.received[key]
	if !found {
		arrivals = newArrivals(kind, p.joinedAt)
		p.received[key] = arrivals
	}
	p.mutex.Unlock()

	p.logger.Debug("track", slog.String("kind", kind), slog.String("id", track.ID()))

	for {
		packet, _, err := track.ReadRTP()
		if err != nil {
			return
		}
		arrivals.record(packet, time.Now())
	}
}

// Reports returns what this peer received, keyed by kind and track.
func (p *Peer) Reports() map[string]Report {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	reports := make(map[string]Report, len(p.received))
	for key, arrivals := range p.received {
		reports[key] = arrivals.Report()
	}
	return reports
}

// Received sums what arrived of one kind across every track.
//
// The usual assertion: "did this peer receive video", not "did it receive track
// abc123", because a receiver does not choose the identifiers a server assigns.
func (p *Peer) Received(kind string) Report {
	total := Report{Kind: kind}
	for _, report := range p.Reports() {
		if report.Kind != kind {
			continue
		}
		total.Packets += report.Packets
		total.Bytes += report.Bytes
		total.Keyframes += report.Keyframes
		total.Gaps += report.Gaps
		total.Reordered += report.Reordered
		total.Duplicates += report.Duplicates
		if report.Duration > total.Duration {
			total.Duration = report.Duration
		}
		if total.TimeToFirst == 0 || (report.TimeToFirst > 0 && report.TimeToFirst < total.TimeToFirst) {
			total.TimeToFirst = report.TimeToFirst
		}
	}
	return total
}

// Source is the media this peer publishes, so a test can throttle it mid-run.
func (p *Peer) Source() *Source { return p.source }

// ReportBandwidth tells the server this peer can only receive so much.
//
// A receiver estimate sent deliberately rather than derived from a shaped network. Real
// throttling means a traffic shaper — a privileged, platform-specific, flaky thing to
// require of a test — and what the server actually reacts to is this message. So the
// harness sends it directly, and phase 9's layer switch is provoked in one line rather
// than by configuring a kernel.
//
// ponytail: this exercises the server's reaction, not its estimator. Measuring the
// estimator itself needs a real constrained path, which belongs with the load testing in
// phase 10.
func (p *Peer) ReportBandwidth(bitsPerSecond int) error {
	senders := p.connection.GetReceivers()
	ssrcs := make([]uint32, 0, len(senders))
	for _, receiver := range senders {
		if track := receiver.Track(); track != nil {
			ssrcs = append(ssrcs, uint32(track.SSRC()))
		}
	}
	if len(ssrcs) == 0 {
		return errNothingToReportOn
	}

	if err := p.connection.WriteRTCP([]rtcp.Packet{&rtcp.ReceiverEstimatedMaximumBitrate{
		Bitrate: float32(bitsPerSecond),
		SSRCs:   ssrcs,
	}}); err != nil {
		return fmt.Errorf("write remb: %w", err)
	}
	return nil
}

// ReportLost asks the server to retransmit positions this peer claims it never got.
//
// Deliberate loss, for the same reason as ReportBandwidth: what the server reacts to is
// the message. A test that wanted genuine loss would have to drop packets in the kernel;
// a test that wants to know whether retransmission works sends this.
func (p *Peer) ReportLost(sequences []uint16) error {
	if len(sequences) == 0 {
		return nil
	}

	for _, receiver := range p.connection.GetReceivers() {
		track := receiver.Track()
		if track == nil || track.Kind() != webrtc.RTPCodecTypeVideo {
			continue
		}

		pairs := make([]rtcp.NackPair, 0, len(sequences))
		for _, sequence := range sequences {
			pairs = append(pairs, rtcp.NackPair{PacketID: sequence})
		}
		if err := p.connection.WriteRTCP([]rtcp.Packet{&rtcp.TransportLayerNack{
			MediaSSRC: uint32(track.SSRC()),
			Nacks:     pairs,
		}}); err != nil {
			return fmt.Errorf("write nack: %w", err)
		}
		return nil
	}
	return errNothingToReportOn
}

// RequestKeyframe asks the server for a fresh start on every video track it forwards.
func (p *Peer) RequestKeyframe() error {
	for _, receiver := range p.connection.GetReceivers() {
		track := receiver.Track()
		if track == nil || track.Kind() != webrtc.RTPCodecTypeVideo {
			continue
		}
		if err := p.connection.WriteRTCP([]rtcp.Packet{
			&rtcp.PictureLossIndication{MediaSSRC: uint32(track.SSRC())},
		}); err != nil {
			return fmt.Errorf("write pli: %w", err)
		}
	}
	return nil
}

// Connected reports whether the transport is up.
func (p *Peer) Connected() bool {
	state := p.connection.ICEConnectionState()
	return state == webrtc.ICEConnectionStateConnected || state == webrtc.ICEConnectionStateCompleted
}

// WaitForMedia blocks until a track of each named kind has delivered a packet.
//
// The measurement NF-3 asks for — join to first media — needs a moment to wait for, and
// "the connection reported connected" is not it: a connected transport carrying nothing
// is precisely the failure this is meant to catch.
func (p *Peer) WaitForMedia(ctx context.Context, kinds ...string) error {
	for {
		missing := ""
		for _, kind := range kinds {
			if p.Received(kind).Packets == 0 {
				missing = kind
				break
			}
		}
		if missing == "" {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for %s on %s: %w", missing, p.name, ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// Close stops publishing, leaves the call and releases the connection.
func (p *Peer) Close(ctx context.Context) error {
	p.stopOnce.Do(func() { close(p.stop) })
	p.sending.Wait()

	// Left before the connection is closed, so the server sees a participant leaving
	// rather than a transport failing. The distinction matters to CL-3.
	leaveErr := p.signaller.Leave(ctx)
	closeErr := p.connection.Close()

	if leaveErr != nil {
		return leaveErr
	}
	if closeErr != nil {
		return fmt.Errorf("close connection: %w", closeErr)
	}
	return nil
}

var errNothingToReportOn = errors.New("harness: no remote track to send feedback about")
