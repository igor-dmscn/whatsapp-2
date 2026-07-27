// An internal test, unlike sfu_test.go, because what it measures is the cost of one unexported
// call on the hottest path in the process.
package sfu

import (
	"fmt"
	"testing"
)

// BenchmarkSourceSubscriptions measures what every forwarded packet pays to find its receivers.
//
// The forward loop calls this once per packet — around 1500 times a second per video source, so
// a ten-publisher call is fifteen thousand times a second. Building the list here meant an
// allocation each time, and with simulcast three forward goroutines per source contended on an
// exclusive lock to do it. Both should now be gone: zero allocations, and flat in subscribers.
func BenchmarkSourceSubscriptions(b *testing.B) {
	for _, subscribers := range []int{1, 5, 20} {
		b.Run(fmt.Sprint(subscribers), func(b *testing.B) {
			published := &source{
				layers:      make(map[string]*layer),
				subscribers: make(map[string]*subscription),
			}
			for index := range subscribers {
				published.subscribe(fmt.Sprint(index), &subscription{})
			}

			b.ReportAllocs()
			for b.Loop() {
				_ = published.subscriptions()
			}
		})
	}
}
