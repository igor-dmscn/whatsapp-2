package harness

import (
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"

	"comms/internal/platform/measure"
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
	// timeToFirstKeyframe is when video first became watchable rather than merely present.
	//
	// The distinction is the whole of NF-3. Packets arriving is not a picture: a decoder
	// joining mid-stream has no reference frame and shows nothing until a keyframe, so
	// timing the first packet measures the transport and flatters the requirement. What
	// makes this number small is the server asking the publisher for a keyframe the moment
	// somebody has joined, and a measurement that cannot see that is not measuring NF-3.
	timeToFirstKeyframe time.Duration

	// latencies is send-to-receive for every stamped audio packet, which is NF-4.
	//
	// Every sample kept rather than a running summary, because the claim is about a
	// percentile and a percentile cannot be computed from a mean. Bounded, because a load
	// run is fifty packets a second per peer for as long as somebody leaves it running.
	latencies []time.Duration
}

func newArrivals(kind string, joinedAt time.Time) *Arrivals {
	return &Arrivals{kind: kind, seen: make(map[uint16]bool), joinedAt: joinedAt}
}

// maxLatencySamples bounds the slice above. Ten thousand audio packets is over three
// minutes at 20 ms each, which is longer than any run that has a reason to be timed.
const maxLatencySamples = 10_000

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

	duplicate := a.seen[packet.SequenceNumber]

	switch {
	case duplicate:
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
		if a.timeToFirstKeyframe == 0 {
			a.timeToFirstKeyframe = at.Sub(a.joinedAt)
		}
	}

	// Not duplicates. A second copy of a packet is a retransmission, and timing it as
	// though it were the original reports the cost of recovering from loss as the cost of
	// the path — which is how a latency number ends up describing something nobody asked
	// about.
	if a.kind == "audio" && !duplicate && len(a.latencies) < maxLatencySamples {
		if sentAt, stamped := audioSentAt(packet.Payload); stamped {
			a.latencies = append(a.latencies, at.Sub(sentAt))
		}
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
	// TimeToFirstKeyframe is when video became watchable, and is zero for audio and for
	// video that never carried one. This is what NF-3 is read from.
	TimeToFirstKeyframe time.Duration
	// Latencies is send-to-receive per audio packet, sorted. NF-4 is its p95.
	//
	// Handed over as samples rather than as a summary, so that a caller aggregating several
	// tracks can put them together and take one percentile — which is the only correct way
	// to do it. Percentiles of percentiles are not percentiles.
	Latencies []time.Duration
}

// LatencyAt returns the given percentile of this report's audio latency, or zero if there
// are no samples.
//
// Nearest-rank, on sorted samples: the smallest sample at or above the given share of the
// distribution. No interpolation, because interpolating between two measurements invents a
// value that was never observed, and these are used to check a stated limit.
func (r Report) LatencyAt(percentile float64) time.Duration {
	return measure.Percentile(r.Latencies, percentile)
}

// Percentile is re-exported so a test measuring across peers has one name to call.
//
// The arithmetic lives in internal/platform/measure, because four different things in this
// project are now measured as percentiles and a percentile written twice is a percentile that
// will be written differently the third time.
func Percentile(samples []time.Duration, share float64) time.Duration {
	return measure.Percentile(samples, share)
}

// Bitrate is what arrived, in bits per second, or zero if too little arrived to say.
func (r Report) Bitrate() int {
	if r.Duration <= 0 {
		return 0
	}
	return int(float64(r.Bytes*8) / r.Duration.Seconds())
}

func (r Report) String() string {
	line := fmt.Sprintf("%s: %d packets, %d keyframes, %d gaps, %d reordered, %d kbit/s, first in %s",
		r.Kind, r.Packets, r.Keyframes, r.Gaps, r.Reordered, r.Bitrate()/1000,
		r.TimeToFirst.Round(time.Millisecond))

	if r.TimeToFirstKeyframe > 0 {
		line += fmt.Sprintf(", watchable in %s", r.TimeToFirstKeyframe.Round(time.Millisecond))
	}
	if len(r.Latencies) > 0 {
		line += fmt.Sprintf(", latency p50 %s p95 %s over %d",
			r.LatencyAt(50).Round(time.Microsecond),
			r.LatencyAt(95).Round(time.Microsecond), len(r.Latencies))
	}
	return line
}

// Report returns a snapshot.
func (a *Arrivals) Report() Report {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	// Copied and sorted here, so a report is a value a caller can hold while the track keeps
	// arriving. Handing over the live slice would be a race the caller could not see.
	latencies := append([]time.Duration(nil), a.latencies...)
	slices.Sort(latencies)

	return Report{
		Kind:                a.kind,
		Packets:             a.packets,
		Bytes:               a.bytes,
		Keyframes:           a.keyframes,
		Gaps:                a.gaps,
		Reordered:           a.reordered,
		Duplicates:          a.duplicates,
		Duration:            a.last.Sub(a.first),
		TimeToFirst:         a.timeToFirst,
		TimeToFirstKeyframe: a.timeToFirstKeyframe,
		Latencies:           latencies,
	}
}
