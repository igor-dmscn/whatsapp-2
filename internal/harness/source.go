// Package harness drives a media server the way many participants would, and asserts
// on what it forwards.
//
// Built before the media server, deliberately (docs/plan.md phase 8). Two reasons.
// Browser tabs cannot load-test an SFU — twenty of them on one machine measure Chrome,
// not the server. And writing the client first forces the server's interface to be
// decided before its internals, which is why Signaller in peer.go is the first thing in
// this package rather than an afterthought.
package harness

import (
	"encoding/binary"
	"sync"
	"time"
)

// Frame is one encoded video or audio frame, as far as anything that forwards it is
// concerned: opaque bytes with a duration.
type Frame struct {
	Data     []byte
	Duration time.Duration
	Keyframe bool
}

// Source produces frames without an encoder.
//
// Synthetic rather than a captured file, and that is not a shortcut. An SFU never
// decodes what it forwards: it reads the VP8 payload descriptor to find frame
// boundaries and keyframes, and copies the rest. So the only properties of real media
// that matter here are the ones this produces — a valid descriptor, honest keyframe
// marking, plausible frame sizes and a steady rate.
//
// What it buys: no binary fixture in the repository that nobody can inspect, a bitrate
// and resolution that are parameters rather than properties of a file, and the ability
// to demand a keyframe at an exact moment, which is what phase 9's layer-switch
// assertion needs.
type Source struct {
	// mutex guards everything below. Not an afterthought: the send loop reads this on
	// one goroutine while feedback from the server writes it on another — a keyframe is
	// demanded by a PLI arriving, and the bitrate is changed by a test mid-run. The race
	// detector found both.
	mutex sync.Mutex

	// bitrate is what the source aims for, in bits per second.
	bitrate int
	// framerate is frames per second.
	framerate int
	// keyframeEvery is how many frames apart keyframes are, zero for only the first.
	keyframeEvery int
	// keyframeRatio is how much larger a keyframe is than a delta frame. Real encoders
	// produce keyframes several times the size, and an SFU switching layers has to
	// carry that burst — a harness that pretends every frame is the same size hides it.
	keyframeRatio int

	frames int
	// demanded is set when something has asked for a keyframe out of sequence, which is
	// what a PLI arriving from the server does.
	demanded bool
}

// SourceOptions configures a source. Zero values mean the defaults below.
type SourceOptions struct {
	Bitrate       int
	Framerate     int
	KeyframeEvery int
}

// NewSource returns a source producing frames at roughly the given bitrate.
func NewSource(options SourceOptions) *Source {
	source := &Source{
		bitrate:       options.Bitrate,
		framerate:     options.Framerate,
		keyframeEvery: options.KeyframeEvery,
		keyframeRatio: 6,
	}
	if source.bitrate <= 0 {
		// 600 kbit/s: a plausible middle simulcast layer, and small enough that twenty
		// peers on one machine are not measuring the harness.
		source.bitrate = 600_000
	}
	if source.framerate <= 0 {
		source.framerate = 30
	}
	if source.keyframeEvery == 0 {
		// Two seconds. Short enough that a receiver joining mid-stream does not wait
		// long, which is the trade every real encoder makes here.
		source.keyframeEvery = source.framerate * 2
	}
	return source
}

// Interval is how long to wait between frames.
func (s *Source) Interval() time.Duration {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.interval()
}

// interval is Interval for callers already holding the lock.
//
// Split out because the first version of Next called Interval while holding the mutex,
// which is a deadlock — and one that presents as the whole suite hanging rather than as
// anything to do with a lock. Go's mutex is not reentrant, and the fix is a private
// unlocked form rather than a lock that pretends to be.
func (s *Source) interval() time.Duration {
	return time.Second / time.Duration(s.framerate)
}

// Bitrate is what this source is currently aiming for.
func (s *Source) Bitrate() int {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.bitrate
}

// Throttle changes the target bitrate.
//
// The lever phase 9's layer-switch assertion pulls: a publisher that drops to a
// fraction of its bitrate, or a receiver told to expect less, must make the server
// choose a different layer. Changing it while running is the point — a bitrate fixed at
// construction could only test the steady state.
func (s *Source) Throttle(bitsPerSecond int) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if bitsPerSecond > 0 {
		s.bitrate = bitsPerSecond
	}
}

// DemandKeyframe makes the next frame a keyframe.
//
// What a PLI from the server turns into. Recorded rather than acted on immediately
// because the next frame is where it can be honoured, and an encoder behaves the same
// way.
func (s *Source) DemandKeyframe() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.demanded = true
}

// Next returns the next frame.
func (s *Source) Next() Frame {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	keyframe := s.frames == 0 || s.demanded || s.frames%s.keyframeEvery == 0
	s.demanded = false
	s.frames++

	// Bits per frame at the target rate, then split between keyframes and delta frames
	// so the average comes out right rather than the peak.
	perFrame := s.bitrate / s.framerate / 8
	size := perFrame
	if keyframe {
		size = perFrame * s.keyframeRatio
	} else if s.keyframeEvery > 1 {
		// Deltas carry what the keyframes' extra size took out of the budget.
		size = perFrame - (perFrame*(s.keyframeRatio-1))/(s.keyframeEvery-1)
	}
	if size < vp8HeaderSize+1 {
		size = vp8HeaderSize + 1
	}

	return Frame{Data: vp8Frame(size, keyframe, s.frames), Duration: s.interval(), Keyframe: keyframe}
}

// vp8HeaderSize is the uncompressed data chunk a keyframe carries: three bytes of frame
// tag, a three-byte start code, and four bytes of dimensions.
const vp8HeaderSize = 10

// vp8Frame builds a frame whose header is a real VP8 frame tag and whose remainder is
// filler.
//
// The header has to be right because it is the only part anything reads. Everything
// after it is compressed coefficients that only a decoder cares about, and nothing in
// this system is a decoder — so it is a counter, which makes a forwarded frame
// identifiable in a capture.
func vp8Frame(size int, keyframe bool, ordinal int) []byte {
	frame := make([]byte, size)

	// The frame tag: 24 bits, little-endian, laid out as
	// key_frame(1) | version(3) | show_frame(1) | first_part_size(19).
	// The key frame flag is *inverted*: zero means keyframe. That inversion is the
	// single most common mistake in code that detects keyframes, so it is written out.
	var tag uint32
	partition := uint32(size / 2)
	tag = partition << 5
	tag |= 1 << 4 // show_frame
	if !keyframe {
		tag |= 1 // an interframe sets the flag keyframes clear
	}
	frame[0] = byte(tag)
	frame[1] = byte(tag >> 8)
	frame[2] = byte(tag >> 16)

	if keyframe {
		// The start code every VP8 keyframe carries, then 14 by 14 macroblocks — 224 by
		// 224, which is not a real resolution and does not need to be, since nothing
		// scales it.
		frame[3], frame[4], frame[5] = 0x9d, 0x01, 0x2a
		binary.LittleEndian.PutUint16(frame[6:8], 224)
		binary.LittleEndian.PutUint16(frame[8:10], 224)
	}

	// A recognisable body: the frame's ordinal repeated, so a forwarded packet can be
	// traced back to what produced it.
	for index := vp8HeaderSize; index < len(frame); index++ {
		frame[index] = byte(ordinal + index)
	}
	return frame
}

// IsKeyframe reports whether a VP8 frame is a keyframe.
//
// The inverse of the flag in the tag, which is why this exists as a named function
// rather than as a bitmask at each call site.
func IsKeyframe(frame []byte) bool {
	return len(frame) > 0 && frame[0]&0x01 == 0
}

// audioFrame is one 20 ms Opus packet's worth of bytes, stamped with the time it was made.
//
// Opus has no payload descriptor and nothing forwards it conditionally, so the contents
// are arbitrary. The TOC byte is set to something plausible anyway: a capture that says
// "SILK, 20 ms, mono" is easier to read than one that says nothing.
//
// The stamp is how NF-4 — one-way audio latency through the SFU — is measured, and it is in
// the *payload* rather than in the header on purpose. A forwarder rewrites SSRC and may
// rewrite sequence numbers and timestamps; the payload is the one part it is contractually
// forbidden to touch, because touching it would mean decoding. So a stamp there measures the
// real path and cannot be confused by anything the server legitimately does to the header.
//
// It only means anything when publisher and receiver share a clock, which for a harness run
// they do — one process. Across machines this would be measuring clock skew.
func audioFrame(size int, stampedAt time.Time) []byte {
	frame := make([]byte, size)
	frame[0] = 0x08 // config 1: SILK narrowband, 20 ms, mono

	for index := 1; index < len(frame); index++ {
		frame[index] = byte(index)
	}
	if size >= audioStampEnd {
		binary.BigEndian.PutUint64(frame[audioStampStart:audioStampEnd], uint64(stampedAt.UnixNano()))
	}
	return frame
}

// Where the send time sits in an audio payload: eight bytes straight after the TOC byte.
const (
	audioStampStart = 1
	audioStampEnd   = audioStampStart + 8
)

// audioSentAt reads the stamp back, reporting whether there was one.
//
// The bounds check is not ceremony. This reads a packet that arrived over a network, and a
// short payload — a truncated packet, or audio from something that is not this harness — must
// be a missing measurement rather than a panic in a load test.
func audioSentAt(payload []byte) (time.Time, bool) {
	if len(payload) < audioStampEnd {
		return time.Time{}, false
	}
	nanos := binary.BigEndian.Uint64(payload[audioStampStart:audioStampEnd])
	if nanos == 0 {
		return time.Time{}, false
	}
	return time.Unix(0, int64(nanos)), true
}
