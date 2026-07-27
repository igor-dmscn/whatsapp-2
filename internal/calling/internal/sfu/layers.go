package sfu

// Simulcast: a publisher sends its camera at several qualities at once, and the server picks
// one per receiver (CL-5, [ADR-0006](../../../../docs/adr/0006-custom-pion-sfu-with-simulcast.md)).
//
// This is the file where an SFU stops being a pipe. Everything else here copies bytes from one
// connection to another; this decides *which* bytes, and it has to lie convincingly while doing
// it — a receiver must never learn that the stream it is decoding changed source.
//
// Three things make that work, and all three are load-bearing:
//
//   - **A switch happens only on a keyframe.** A decoder resolves each frame against the
//     previous one, so handing it the middle of a different encode produces the smeared mess
//     that people describe as "the video broke".
//   - **Sequence numbers and timestamps are rewritten.** Each layer numbers its own packets
//     from its own starting point, so forwarding them unchanged looks to the receiver like
//     catastrophic loss and reordering at the moment of every switch. The offsets here are
//     what make a switch invisible.
//   - **A keyframe is asked for, not waited for.** Left alone, a switch happens whenever the
//     publisher's next scheduled keyframe arrives — up to two seconds of the old layer. NF-3
//     already taught that lesson once, at join; it is the same lesson here.
//
// What this is not: a bandwidth estimator. The server reacts to what receivers tell it
// (REMB), and measures each layer's real bitrate so that "does this fit" is a comparison
// between two measured numbers rather than between a measurement and a hopeful constant.
// Estimating available bandwidth from the server side, without a receiver saying anything,
// needs a congestion controller and belongs to a phase that has a real constrained path to
// test it on.

import (
	"fmt"
	"sync"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"
	"github.com/pion/webrtc/v4"
)

// source is one thing a participant publishes: a microphone, or a camera that may arrive as
// several quality layers.
//
// The unit of subscription, deliberately. A receiver subscribes to a *source* and gets one
// outbound track for it, whichever layer is currently chosen — so the number of tracks a
// client sees does not depend on how many layers the publisher happens to send, and a client
// that knows nothing about simulcast is a client that works.
type source struct {
	// key identifies the source within a call: the publisher and the track it sent.
	key       string
	kind      string
	trackID   string
	streamID  string
	codec     webrtc.RTPCodecCapability
	publisher *participant

	// RWMutex rather than Mutex because of snapshot below: with simulcast there is a forward
	// goroutine per layer, all reading this source, and an exclusive lock per packet would
	// serialise three of them against each other for a read.
	mutex sync.RWMutex
	// layers by RID. A publisher sending one layer has a single entry under the empty
	// string, which is what makes every non-simulcast client work unchanged.
	layers map[string]*layer
	// subscribers by receiving participant.
	subscribers map[string]*subscription
	// snapshot is subscribers as a slice, rebuilt whenever that map changes.
	//
	// The forward loop reads this once per packet — the hottest path in the process — and
	// building the list there meant an allocation per packet per source. Copy-on-write and
	// never mutated in place, so a reader may hold it after releasing the lock.
	snapshot []*subscription
}

func newSource(key string, remote *webrtc.TrackRemote, publisher *participant) *source {
	return &source{
		key:         key,
		kind:        remote.Kind().String(),
		trackID:     remote.ID(),
		streamID:    remote.StreamID(),
		codec:       remote.Codec().RTPCodecCapability,
		publisher:   publisher,
		layers:      make(map[string]*layer),
		subscribers: make(map[string]*subscription),
	}
}

// subscribe adds a receiver, and unsubscribe removes one. Both rebuild the snapshot the
// forward loop reads, which is the only reason they exist rather than the map being written
// directly: a subscriber added to the map but not to the slice receives nothing, and one
// removed from the map but not the slice is written to after it has gone.
func (s *source) subscribe(participantID string, receiving *subscription) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.subscribers[participantID] = receiving
	s.resnapshot()
}

func (s *source) unsubscribe(participantID string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	delete(s.subscribers, participantID)
	s.resnapshot()
}

// resnapshot rebuilds the slice the forward loop reads. Callers hold the lock.
func (s *source) resnapshot() {
	held := make([]*subscription, 0, len(s.subscribers))
	for _, receiving := range s.subscribers {
		held = append(held, receiving)
	}
	s.snapshot = held
}

// layer is one quality of one source.
type layer struct {
	rid string
	// ssrc is how a keyframe request reaches this layer specifically. Asking the publisher
	// for a keyframe on the wrong SSRC produces one on the wrong layer, and the switch waits
	// for the scheduled one anyway.
	ssrc uint32

	mutex sync.Mutex
	// bytes and since accumulate a bitrate over a window.
	bytes int
	since time.Time
	// measured is bits per second over the last full window, or zero before there has been
	// one. Measured rather than declared: what a publisher says in its SDP is an intention,
	// and layer selection wants to know what is actually arriving.
	measured int
}

// measurementWindow is how long a layer's bitrate is averaged over.
//
// One second: long enough that a keyframe's burst does not read as a doubled bitrate, short
// enough that a publisher which throttled itself is noticed while somebody still cares.
const measurementWindow = time.Second

// record takes one packet's size.
func (l *layer) record(size int, now time.Time) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.since.IsZero() {
		l.since = now
	}
	l.bytes += size

	if elapsed := now.Sub(l.since); elapsed >= measurementWindow {
		l.measured = int(float64(l.bytes*8) / elapsed.Seconds())
		l.bytes = 0
		l.since = now
	}
}

// bitrate is what this layer has been carrying, or zero if it is too early to say.
func (l *layer) bitrate() int {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return l.measured
}

// add records a layer of this source, or returns the one already known.
func (s *source) add(rid string, ssrc uint32) *layer {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if existing, found := s.layers[rid]; found {
		// A republished layer after a reconnect: the SSRC is new and everything else stands.
		existing.ssrc = ssrc
		return existing
	}
	added := &layer{rid: rid, ssrc: ssrc}
	s.layers[rid] = added
	return added
}

// remove forgets a layer that stopped arriving.
func (s *source) remove(rid string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	delete(s.layers, rid)
}

// layerCount is how many qualities this source is currently sending.
func (s *source) layerCount() int {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return len(s.layers)
}

// choose returns the best layer that fits an estimate, or nil if there is nothing to send.
//
// Highest-that-fits, with a margin. The margin is not decoration: a receiver's estimate covers
// everything arriving over that transport, and a video layer sized to exactly the estimate
// leaves nothing for the audio that matters more.
//
// An estimate of zero means nobody has said anything, which is the normal state of a client
// that does not send REMB — then the best layer is the right answer, because the alternative
// is degrading a call nobody complained about.
func (s *source) choose(estimate int) *layer {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if len(s.layers) == 0 {
		return nil
	}

	// Ranked by what they are actually carrying. Unmeasured layers rank as unknown and are
	// skipped, which for the first second means the layer already being forwarded stays —
	// switching to something whose cost is unknown is how a constrained receiver gets sent
	// the biggest layer in the call.
	ranked := make([]*layer, 0, len(s.layers))
	for _, held := range s.layers {
		if held.bitrate() > 0 {
			ranked = append(ranked, held)
		}
	}
	if len(ranked) == 0 {
		return nil
	}
	slicesSortByBitrate(ranked)

	if estimate <= 0 {
		return ranked[len(ranked)-1]
	}

	best := ranked[0]
	for _, candidate := range ranked[1:] {
		if float64(candidate.bitrate()) > float64(estimate)*fitMargin {
			break
		}
		best = candidate
	}
	return best
}

// fitMargin is the share of a receiver's estimate a video layer may take.
const fitMargin = 0.9

// slicesSortByBitrate orders layers from least to most demanding.
//
// An insertion sort, and not for performance: there are two or three layers, and this runs on
// a receiver's feedback rather than per packet. Written out because sorting by a value read
// under another lock is the kind of thing a comparison function hides.
func slicesSortByBitrate(layers []*layer) {
	for index := 1; index < len(layers); index++ {
		held := layers[index]
		rate := held.bitrate()
		position := index - 1
		for position >= 0 && layers[position].bitrate() > rate {
			layers[position+1] = layers[position]
			position--
		}
		layers[position+1] = held
	}
}

// subscription is one receiver's view of one source: the outbound track, which layer is going
// into it, and the arithmetic that makes a change of layer invisible.
type subscription struct {
	source *source
	// receiver is the participant this sends to. Held for logging and for feedback.
	receiver *participant
	track    *webrtc.TrackLocalStaticRTP

	mutex sync.Mutex
	// chosen is whether a layer has been picked yet, and it is a flag of its own rather than
	// `current == ""` because the empty string is a real RID: it is what a publisher sending
	// one layer uses. Conflating the two meant a single-layer stream re-entered the
	// nothing-chosen-yet branch on every packet, so only the keyframes were forwarded — two
	// packets in two seconds, and video that technically arrived.
	chosen bool
	// current is the RID being forwarded, meaningful only once chosen.
	current string
	// wanted is the RID to move to at the next keyframe, empty when settled.
	wanted string
	// estimate is the last bitrate this receiver said it could take, in bits per second.
	estimate int

	// The rewriting state. offsets are added to every packet of the current layer; highest
	// is the top of what has been sent, which is what the next layer has to continue from.
	seqOffset  uint16
	tsOffset   uint32
	highestSeq uint16
	highestTS  uint32
	// tsStep is the last timestamp gap seen, used to space the first packet after a switch.
	// Guessed at first, then observed: the real gap depends on the publisher's frame rate,
	// which is not something this server is told.
	tsStep  uint32
	started bool
	// switches counts layer changes, for tests and for logging. A number that climbs
	// steadily is a receiver flapping between layers, which is worse than either.
	switches int
}

// defaultTimestampStep is one frame at 30 fps on a 90 kHz clock, used until a real gap has
// been observed. Only ever applies to the single packet that begins a switch.
const defaultTimestampStep = 3000

func newSubscription(from *source, to *participant) (*subscription, error) {
	// One track per subscription rather than one per source shared by everyone, which is
	// what simulcast costs structurally: two receivers of the same camera may be getting
	// different layers, and the rewritten numbering is per receiver.
	track, err := webrtc.NewTrackLocalStaticRTP(from.codec, from.trackID, from.streamID)
	if err != nil {
		return nil, fmt.Errorf("new forwarding track: %w", err)
	}
	return &subscription{source: from, receiver: to, track: track, tsStep: defaultTimestampStep}, nil
}

// write forwards one packet of one layer, if that is the layer this receiver should have.
func (s *subscription) write(rid string, packet *rtp.Packet, keyframe bool) error {
	s.mutex.Lock()

	switch {
	case !s.chosen:
		// Nothing chosen yet. Video starts on a keyframe rather than on the first packet to
		// arrive, because a receiver handed the middle of a frame has nothing to decode and
		// shows nothing until the next one anyway.
		//
		// Audio has no keyframes, and gating it on one is a mistake that presents as total
		// silence with perfect video — which is exactly how it presented.
		if s.source.kind == "video" && !keyframe {
			s.mutex.Unlock()
			return nil
		}
		s.chosen = true
		s.current = rid
		s.rebase(packet)

	case rid == s.current:
		// The layer being forwarded.

	case rid == s.wanted && keyframe:
		// The moment a switch is allowed to happen.
		s.current = rid
		s.wanted = ""
		s.switches++
		s.rebase(packet)

	default:
		// A layer this receiver is not taking. Dropped, which is the entire point of
		// simulcast: the publisher sent three and the expensive two go no further.
		s.mutex.Unlock()
		return nil
	}

	outSeq := packet.SequenceNumber + s.seqOffset
	outTS := packet.Timestamp + s.tsOffset

	// Only advance the high-water mark on packets that are actually newer. A reordered
	// packet gets its correct lower number and must not drag the mark backwards, or the next
	// switch would rebase onto a number already used.
	if !s.started || newerSequence(outSeq, s.highestSeq) {
		if step := outTS - s.highestTS; step > 0 && step < maxTimestampStep {
			s.tsStep = step
		}
		s.highestSeq = outSeq
		s.highestTS = outTS
		s.started = true
	}
	s.mutex.Unlock()

	// A copy, because the caller is forwarding this same packet to every other subscriber
	// and each of them rewrites it differently.
	forwarded := *packet
	forwarded.SequenceNumber = outSeq
	forwarded.Timestamp = outTS

	return s.track.WriteRTP(&forwarded) //nolint:wrapcheck // io.ErrClosedPipe is the caller's to interpret.
}

// maxTimestampStep bounds what counts as a believable frame gap: one second on a 90 kHz clock.
// A larger jump is a publisher that paused, and adopting it as the step would put a visible
// stall into the next switch.
const maxTimestampStep = 90000

// rebase sets the offsets so that this packet continues the outbound stream.
//
// The whole trick, in four lines. The receiver has seen up to highestSeq at highestTS; this
// packet, whichever layer it came from and whatever numbering that layer uses, becomes the
// next one.
func (s *subscription) rebase(packet *rtp.Packet) {
	if !s.started {
		// Nothing sent yet, so the publisher's own numbering is as good as any and keeping
		// it makes a single-layer stream byte-identical to what it was before this file
		// existed.
		s.seqOffset, s.tsOffset = 0, 0
		return
	}
	s.seqOffset = s.highestSeq + 1 - packet.SequenceNumber
	s.tsOffset = s.highestTS + s.tsStep - packet.Timestamp
}

// want asks for a layer at the next keyframe. Returns whether anything changed.
func (s *subscription) want(rid string) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if (s.chosen && rid == s.current) || rid == s.wanted {
		return false
	}
	s.wanted = rid
	return true
}

// reported records what this receiver says it can take.
func (s *subscription) reported(bitsPerSecond int) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.estimate = bitsPerSecond
}

// state returns what this subscription is doing, for selection and for tests.
func (s *subscription) state() (current string, estimate, switches int) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.current, s.estimate, s.switches
}

// newerSequence reports whether left is ahead of right in 16-bit sequence space.
//
// Written out because `left > right` is wrong across the wrap at 65535, and getting it wrong
// here would rebase a switch onto a sequence number already sent — which a receiver reads as
// a stream that has gone backwards.
func newerSequence(left, right uint16) bool {
	return left != right && left-right < 1<<15
}

// startsKeyframe reports whether an RTP payload begins a VP8 keyframe.
//
// Two conditions, both necessary: the packet must start a partition — a keyframe's second and
// later packets carry no frame tag — and the frame tag's key-frame flag must be clear, because
// in VP8 that flag is *inverted*. Zero means keyframe.
//
// The harness has the same nine lines, and they stay separate on purpose: that copy asserts
// what arrived, this one decides what to forward, and a package that tests a server must not
// be something the server imports.
func startsKeyframe(payload []byte) bool {
	var packet codecs.VP8Packet
	frame, err := packet.Unmarshal(payload)
	if err != nil || packet.S != 1 || packet.PID != 0 {
		return false
	}
	return len(frame) > 0 && frame[0]&0x01 == 0
}
