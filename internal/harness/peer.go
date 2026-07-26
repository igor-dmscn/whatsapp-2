package harness

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"
	"github.com/pion/sdp/v3"
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
	// Simulcast makes this peer publish three video layers instead of one, which is what
	// CL-5 needs something to choose between. Off by default: most runs are not about
	// layers, and three of them is three times the bytes.
	Simulcast bool
	Source    SourceOptions
	Logger    *slog.Logger
}

// Peer is one simulated participant.
type Peer struct {
	name       string
	connection *webrtc.PeerConnection
	signaller  Signaller
	options    PeerOptions
	logger     *slog.Logger

	// video is one entry per published layer, each with its own source: the point of
	// simulcast is that the layers differ, so they cannot share a frame generator.
	video []*layer
	// videoSender carries every layer, because they are encodings of one sender.
	videoSender *webrtc.RTPSender
	audio       *webrtc.TrackLocalStaticSample

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

	// The extensions simulcast is carried by, and they have to be registered on the
	// *publisher* as well as on the server. Pion offers `a=simulcast:send` and puts the
	// stream identifier in an RTP header extension only if its media engine knows the
	// extension; without it three layers go out with nothing to tell them apart and the
	// server logs "failed Simulcast probing" while OnTrack never fires. RegisterDefaultCodecs
	// does not bring them, which is easy to assume and wrong.
	for _, extension := range []string{sdp.SDESMidURI, sdp.SDESRTPStreamIDURI} {
		if err := engine.RegisterHeaderExtension(
			webrtc.RTPHeaderExtensionCapability{URI: extension}, webrtc.RTPCodecTypeVideo,
		); err != nil {
			return nil, fmt.Errorf("register %s: %w", extension, err)
		}
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
		options:    options,
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
	audio, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus}, "audio", "harness-"+p.name)
	if err != nil {
		return fmt.Errorf("new audio track: %w", err)
	}
	p.audio = audio

	// Video, as one layer or several. Several is what a browser does when asked, and what
	// CL-5 is about: the same camera encoded at three qualities so the server can choose one
	// per receiver without asking the publisher to change anything.
	//
	// All of them on *one* sender, which is what makes it simulcast rather than three
	// cameras: they share a media section and are told apart by an RTP header extension
	// carrying the stream identifier. Three separate senders would negotiate three tracks
	// and every receiver would get all three.
	for index, quality := range p.qualities() {
		options := []func(*webrtc.TrackLocalStaticRTP){}
		if quality.rid != "" {
			options = append(options, webrtc.WithRTPStreamID(quality.rid))
		}

		track, err := webrtc.NewTrackLocalStaticRTP(
			webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8},
			"video", "harness-"+p.name, options...)
		if err != nil {
			return fmt.Errorf("new video track %s: %w", quality.rid, err)
		}
		p.video = append(p.video, &layer{
			rid: quality.rid, track: track, source: NewSource(quality.source),
		})

		if index > 0 {
			// AddEncoding rather than AddTrack: it attaches this layer to the sender the
			// first one created. Pion refuses it if the first was not created with a stream
			// identifier, which is the one ordering constraint here.
			if err := p.videoSender.AddEncoding(track); err != nil {
				return fmt.Errorf("add encoding %s: %w", quality.rid, err)
			}
			continue
		}

		sender, err := p.connection.AddTrack(track)
		if err != nil {
			return fmt.Errorf("add video track: %w", err)
		}
		p.videoSender = sender
	}

	senders := []*webrtc.RTPSender{p.videoSender}
	audioSender, err := p.connection.AddTrack(p.audio)
	if err != nil {
		return fmt.Errorf("add audio track: %w", err)
	}
	senders = append(senders, audioSender)

	for _, sender := range senders {
		// Feedback has to be read or it accumulates in the transport, and a peer that
		// ignores PLI never sends the keyframe a new receiver is waiting for — which
		// looks exactly like a broken SFU.
		go p.readFeedback(sender)
	}
	return nil
}

// quality is one video layer this peer will publish.
type quality struct {
	rid    string
	source SourceOptions
}

// layer is a published video layer: the track it goes out on, what generates its frames, and
// the RTP state a publisher has to keep for itself.
//
// Raw RTP rather than samples, and that is forced rather than chosen. A simulcast publisher has
// to stamp the stream identifier into an RTP header extension on every packet, because that is
// the only thing telling three layers sharing one media section apart — and Pion's sender does
// not do it. Its own simulcast test writes the extension by hand, which is the clearest
// possible statement that the sample path cannot. So the frame is packetised here, and the
// sequence numbers and timestamps are this peer's own business.
type layer struct {
	rid    string
	track  *webrtc.TrackLocalStaticRTP
	source *Source

	payloader codecs.VP8Payloader
	sequence  uint16
	timestamp uint32
}

// videoClockRate is VP8's, and is what a frame duration is expressed in.
const videoClockRate = 90000

// rtpMTU is how much of a frame goes in one packet. 1200 bytes leaves room for the RTP header,
// the extensions, and SRTP's overhead inside a 1500-byte path.
const rtpMTU = 1200

// write packetises one frame and sends it, stamped so the server can tell which layer it is.
func (l *layer) write(frame Frame, extensions videoExtensions) error {
	payloads := l.payloader.Payload(rtpMTU, frame.Data)

	for index, payload := range payloads {
		header := rtp.Header{
			Version: 2,
			// The last packet of a frame, which is how a receiver knows the frame is
			// complete. Getting this wrong makes every frame look truncated.
			Marker:         index == len(payloads)-1,
			SequenceNumber: l.sequence,
			Timestamp:      l.timestamp,
		}
		l.sequence++

		if l.rid != "" {
			header.Extension = true
			header.ExtensionProfile = oneByteExtensionProfile
			if err := header.SetExtension(extensions.midID, []byte(extensions.mid)); err != nil {
				return fmt.Errorf("set mid extension: %w", err)
			}
			if err := header.SetExtension(extensions.ridID, []byte(l.rid)); err != nil {
				return fmt.Errorf("set rid extension: %w", err)
			}
		}

		if err := l.track.WriteRTP(&rtp.Packet{Header: header, Payload: payload}); err != nil {
			return fmt.Errorf("write rtp: %w", err)
		}
	}

	// Advanced once per frame, not once per packet: every packet of one frame carries the
	// same timestamp, which is how a receiver knows they belong together.
	l.timestamp += uint32(frame.Duration.Seconds() * videoClockRate)
	return nil
}

// oneByteExtensionProfile is the RTP one-byte header extension form, which is what the
// identifiers negotiated in SDP are numbered for.
const oneByteExtensionProfile = 0x1000

// videoExtensions are the negotiated identifiers a simulcast publisher stamps packets with.
//
// Negotiated, so they cannot be constants: the numbers are assigned per connection in the SDP,
// and stamping the wrong one produces packets the server reads as some other extension.
type videoExtensions struct {
	mid   string
	midID uint8
	ridID uint8
}

// videoExtensions reads the identifiers this connection agreed on.
//
// Empty when there is nothing to stamp — a single-layer publisher, or a peer whose transceiver
// has no mid yet — and write skips the extensions in that case.
func (p *Peer) videoExtensions() videoExtensions {
	if p.videoSender == nil {
		return videoExtensions{}
	}

	var found videoExtensions
	for _, extension := range p.videoSender.GetParameters().HeaderExtensions {
		switch extension.URI {
		case sdp.SDESMidURI:
			found.midID = uint8(extension.ID) //nolint:gosec // extension ids are small by definition.
		case sdp.SDESRTPStreamIDURI:
			found.ridID = uint8(extension.ID) //nolint:gosec // as above.
		}
	}

	for _, transceiver := range p.connection.GetTransceivers() {
		if transceiver.Sender() == p.videoSender {
			found.mid = transceiver.Mid()
			break
		}
	}
	return found
}

// qualities is the layers to publish, from lowest to highest.
//
// One layer unless asked for more, because most of what the harness is used for — is this
// forwarded, how many peers can one node carry — is not about layers, and three layers would
// triple the bytes every one of those runs pushes through.
//
// The names are the convention browsers use: q for quarter, h for half, f for full.
func (p *Peer) qualities() []quality {
	base := p.options.Source
	if !p.options.Simulcast {
		return []quality{{rid: "", source: base}}
	}

	// Each step down is a quarter of the bitrate and half the frame rate, which is roughly
	// what a browser's default three-layer ladder produces. The exact numbers matter less
	// than that they are far enough apart to be told apart by measurement.
	bitrate := base.Bitrate
	if bitrate <= 0 {
		bitrate = defaultBitrate
	}
	framerate := base.Framerate
	if framerate <= 0 {
		framerate = defaultFramerate
	}

	return []quality{
		{rid: "q", source: SourceOptions{Bitrate: bitrate / 8, Framerate: framerate / 2}},
		{rid: "h", source: SourceOptions{Bitrate: bitrate / 3, Framerate: framerate}},
		{rid: "f", source: SourceOptions{Bitrate: bitrate, Framerate: framerate}},
	}
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

// publish starts the send loops: one per video layer, and one for audio.
func (p *Peer) publish() {
	// Read once, here, because the identifiers are only assigned once negotiation has
	// finished and they do not change for the life of the connection.
	extensions := p.videoExtensions()

	for _, sending := range p.video {
		p.sending.Add(1)
		go func() {
			defer p.sending.Done()
			p.sendVideo(sending, extensions)
		}()
	}

	p.sending.Add(1)
	go func() {
		defer p.sending.Done()
		p.sendAudio()
	}()
}

// sendVideo drives one layer. One goroutine per layer, because they run at different frame
// rates and a shared loop would have to send the slow ones at the fast one's rate.
func (p *Peer) sendVideo(sending *layer, extensions videoExtensions) {
	ticker := time.NewTicker(sending.source.Interval())
	defer ticker.Stop()

	for {
		select {
		case <-p.stop:
			return
		case <-ticker.C:
			frame := sending.source.Next()
			if err := sending.write(frame, extensions); err != nil &&
				!errors.Is(err, io.ErrClosedPipe) {
				p.logger.Debug("write video",
					slog.String("rid", sending.rid), slog.Any("error", err))
				return
			}
			// The interval is re-read every frame so Throttle takes effect mid-run
			// rather than at the next reconnection.
			ticker.Reset(sending.source.Interval())
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
			// Stamped as late as possible — here, not on the tick — so the measurement is
			// of the path and not of this loop's own scheduling.
			if err := p.audio.WriteSample(media.Sample{
				Data: audioFrame(bytesPerPacket, time.Now()), Duration: audioInterval,
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
				// from the publisher's side, and it is what lets a layer switch happen
				// promptly instead of at the next scheduled keyframe.
				//
				// Every layer, because RTCP arriving on this sender does not say which
				// encoding it was about — and a keyframe on a layer nobody is watching
				// costs one frame. Asking the wrong layer and not the right one would cost
				// the switch.
				for _, sending := range p.video {
					sending.source.DemandKeyframe()
				}
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
		// Samples pooled, not percentiles averaged. A p95 of two p95s is not a p95 of
		// anything, and this total is what NF-4 is read from.
		total.Latencies = append(total.Latencies, report.Latencies...)
		if report.Duration > total.Duration {
			total.Duration = report.Duration
		}
		if total.TimeToFirst == 0 || (report.TimeToFirst > 0 && report.TimeToFirst < total.TimeToFirst) {
			total.TimeToFirst = report.TimeToFirst
		}
		if total.TimeToFirstKeyframe == 0 ||
			(report.TimeToFirstKeyframe > 0 && report.TimeToFirstKeyframe < total.TimeToFirstKeyframe) {
			total.TimeToFirstKeyframe = report.TimeToFirstKeyframe
		}
	}
	slices.Sort(total.Latencies)
	return total
}

// Name is what this peer is called, for reports and failure messages.
func (p *Peer) Name() string { return p.name }

// Source is the media this peer publishes, so a test can throttle it mid-run.
//
// The highest layer, which is the one worth throttling: a publisher that drops its best
// encoding is the case a server has to react to. Returns nil for a peer that does not publish.
func (p *Peer) Source() *Source {
	if len(p.video) == 0 {
		return nil
	}
	return p.video[len(p.video)-1].source
}

// Sources is every layer's generator, lowest quality first.
func (p *Peer) Sources() []*Source {
	sources := make([]*Source, 0, len(p.video))
	for _, sending := range p.video {
		sources = append(sources, sending.source)
	}
	return sources
}

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

// WaitForWatchableVideo blocks until a keyframe has arrived.
//
// The stronger of the two waits, and the one NF-3 needs. A packet arriving proves the
// transport; a keyframe arriving is the first moment a person would see anything, because a
// decoder that joined mid-stream has no reference frame and shows nothing until then.
//
// Written after the measurement built on WaitForMedia turned out to be flattering: it
// returned two packets in, well before any keyframe, so the number it produced was the time
// to first *packet* wearing NF-3's name.
func (p *Peer) WaitForWatchableVideo(ctx context.Context) error {
	for {
		if p.Received("video").TimeToFirstKeyframe > 0 {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for a keyframe on %s: %w", p.name, ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// WaitForMedia blocks until a track of each named kind has delivered a packet.
//
// Enough for "is this forwarding at all", which is what most assertions want. Not enough for
// NF-3 — see WaitForWatchableVideo. "The connection reported connected" is not enough for
// either: a connected transport carrying nothing is precisely the failure this catches.
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
