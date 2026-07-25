package identityapi

import (
	"sync"
	"time"
)

// RateLimiter is a fixed-window counter keyed by caller.
//
// ponytail: per-process and in-memory, so the effective limit is the configured
// one multiplied by the number of api nodes. That is acceptable for ID-5, whose
// purpose is to stop handle enumeration rather than to meter precisely. Phase 10
// replaces this with a Redis-backed limiter shared across nodes.
type RateLimiter struct {
	limit  int
	window time.Duration

	mutex   sync.Mutex
	windows map[string]*window
}

type window struct {
	count   int
	resetAt time.Time
}

// NewRateLimiter allows limit calls per key per window.
func NewRateLimiter(limit int, w time.Duration) *RateLimiter {
	return &RateLimiter{limit: limit, window: w, windows: make(map[string]*window)}
}

// Allow records an attempt by key and reports whether it is within the limit.
func (r *RateLimiter) Allow(key string, now time.Time) bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// Sweeping on write keeps the map bounded without a background goroutine to
	// own and shut down. It is O(n) but only runs once the map is large enough
	// for that to be cheaper than leaking.
	if len(r.windows) > sweepThreshold {
		for key, existing := range r.windows {
			if now.After(existing.resetAt) {
				delete(r.windows, key)
			}
		}
	}

	existing, found := r.windows[key]
	if !found || now.After(existing.resetAt) {
		r.windows[key] = &window{count: 1, resetAt: now.Add(r.window)}
		return true
	}

	existing.count++
	return existing.count <= r.limit
}

// sweepThreshold is the map size at which expired entries are cleared.
const sweepThreshold = 10_000
