package harness

import (
	"fmt"
	"sync"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"
)

// Arrivals is what one received track looks like from the outside.
//
// Deliberately about what arrived rather than about what was sent: an SFU rewrites
// sequence numbers and timestamps, so the only honest assertions are the ones a receiver
// could make — packets came, they were in order, keyframes appeared, and the first one
// arrived quickly enough.
type Arrivals struct {
	mutex sync.Mutex

	kind      string
	packets   int
	bytes     int
	keyframes int
	// gaps counts sequence numbers that never arrived. Not an error on its own: an SFU
	// switching layers drops packets by design, and a lossy path is what NACK exists
	// for. It is a measurement.
	gaps int
	// reordered counts packets that arrived after a later one. Distinguished from a gap
	// because they mean different things about the path.
	reordered  int
	duplicates int

	first       time.Time
	last        time.Time
	highest     uint16
	seen        map[uint16]bool
	started     bool
	firstAt     time.Time
	joinedAt    time.Time
	timeToFirst time.Duration
}

func newArrivals(kind string, joinedAt time.Time) *Arrivals {
	return &Arrivals{kind: kind, seen: make(map[uint16]bool), joinedAt: joinedAt}
}

// record takes one packet.
func (a *Arrivals) record(packet *rtp.Packet, at time.Time) {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	if !a.started {
		a.started = true
		a.first = at
		a.firstAt = at
		a.timeToFirst = at.Sub(a.joinedAt)
		a.highest = packet.SequenceNumber
	}
	a.last = at
	a.packets++
	a.bytes += len(packet.Payload)

	switch {
	case a.seen[packet.SequenceNumber]:
		a.duplicates++
	case newer(packet.SequenceNumber, a.highest):
		// Anything skipped between the last highest and this one is missing for now.
		// Counted here and uncounted if it turns up, which is what makes a retransmission
		// visible as a recovery rather than as a loss.
		for missing := a.highest + 1; missing != packet.SequenceNumber; missing++ {
			a.gaps++
		}
		a.highest = packet.SequenceNumber
	default:
		a.reordered++
		if a.gaps > 0 {
			// It was counted as missing and has arrived. This is what NACK recovery looks
			// like from a receiver's side, and the reason gaps are not asserted to be zero.
			a.gaps--
		}
	}
	a.seen[packet.SequenceNumber] = true

	if a.kind == "video" && isKeyframePacket(packet.Payload) {
		a.keyframes++
	}
}

// newer reports whether left is ahead of right in 16-bit sequence space.
//
// Written out because `left > right` is wrong across the wrap at 65535, and a harness
// that miscounts gaps once per 65536 packets is a harness nobody trusts.
func newer(left, right uint16) bool {
	return left != right && left-right < 1<<15
}

// isKeyframePacket reports whether an RTP payload starts a VP8 keyframe.
//
// Two conditions, both necessary: the packet must begin a partition — a keyframe's
// second and later packets carry no frame tag — and the frame tag's inverted flag must
// be clear.
func isKeyframePacket(payload []byte) bool {
	var packet codecs.VP8Packet
	frame, err := packet.Unmarshal(payload)
	if err != nil || packet.S != 1 || packet.PID != 0 {
		return false
	}
	return IsKeyframe(frame)
}

// Report is a snapshot of what a track received.
type Report struct {
	Kind        string
	Packets     int
	Bytes       int
	Keyframes   int
	Gaps        int
	Reordered   int
	Duplicates  int
	Duration    time.Duration
	TimeToFirst time.Duration
}

// Bitrate is what arrived, in bits per second, or zero if too little arrived to say.
func (r Report) Bitrate() int {
	if r.Duration <= 0 {
		return 0
	}
	return int(float64(r.Bytes*8) / r.Duration.Seconds())
}

func (r Report) String() string {
	return fmt.Sprintf("%s: %d packets, %d keyframes, %d gaps, %d reordered, %d kbit/s, first in %s",
		r.Kind, r.Packets, r.Keyframes, r.Gaps, r.Reordered, r.Bitrate()/1000,
		r.TimeToFirst.Round(time.Millisecond))
}

// Report returns a snapshot.
func (a *Arrivals) Report() Report {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	return Report{
		Kind:        a.kind,
		Packets:     a.packets,
		Bytes:       a.bytes,
		Keyframes:   a.keyframes,
		Gaps:        a.gaps,
		Reordered:   a.reordered,
		Duplicates:  a.duplicates,
		Duration:    a.last.Sub(a.first),
		TimeToFirst: a.timeToFirst,
	}
}
