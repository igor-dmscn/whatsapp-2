// Package measure holds the arithmetic the project's own measurements need.
//
// Here rather than in each place that measures something, because there are now several — call
// setup latency, audio latency, send-to-delivery, gap sync — and a percentile written twice is a
// percentile that will be written differently the third time. Which matters: a requirement stated
// as a p95 is checked against whatever this function decides a p95 is.
package measure

import "time"

// Percentile returns the nearest-rank percentile of some durations, in any order.
//
// Nearest-rank, on a sorted copy, with no interpolation. Interpolating between two measurements
// invents a value nobody observed, and these are used to decide whether a stated limit was met —
// so the answer should be a number that actually happened.
//
// The copy is not politeness. A caller measuring something is usually still measuring it, and
// sorting the slice it is appending to would reorder its own samples underneath it.
func Percentile(samples []time.Duration, share float64) time.Duration {
	if len(samples) == 0 {
		return 0
	}

	sorted := make([]time.Duration, len(samples))
	copy(sorted, samples)
	sortDurations(sorted)

	// Ceiling of share × count, as an index. The nudge is because share/100*count on floats
	// lands a hair under a whole number for exact cases — p100 of 50 samples computing to
	// 49.999999 and truncating to rank 48, which is p98 wearing p100's name.
	rank := int(share/100*float64(len(sorted))+0.999999) - 1
	if rank < 0 {
		rank = 0
	}
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	return sorted[rank]
}

// sortDurations is an insertion sort, which is the right one here: these slices are tens or
// hundreds of samples, and this runs once at the end of a measurement rather than in it.
func sortDurations(samples []time.Duration) {
	for index := 1; index < len(samples); index++ {
		held := samples[index]
		position := index - 1
		for position >= 0 && samples[position] > held {
			samples[position+1] = samples[position]
			position--
		}
		samples[position+1] = held
	}
}
