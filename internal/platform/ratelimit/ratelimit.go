// Package ratelimit bounds how often one caller may do something.
//
// In Redis rather than in each process, because the limit is a property of the caller and not
// of whichever node they happened to reach. Counted per node, a limit of thirty across four
// nodes is a limit of a hundred and twenty, and it loosens every time the deployment grows —
// which is the opposite of what a limit is for.
//
// **It fails open.** A limiter that cannot reach Redis allows the action, and that is a
// deliberate choice rather than an oversight: this exists to stop abuse and accidents, not to
// enforce anything the system's correctness rests on, and refusing every send in the datacentre
// because a cache is unreachable would be a far worse outage than the one it prevents. The
// plan's own requirement says it too — kill Redis and sends still succeed.
package ratelimit

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

// Limiter counts actions per caller per window.
type Limiter struct {
	client *redis.Client
	logger *slog.Logger
}

// New returns a limiter over client. A nil client makes every action allowed, which is what a
// deployment that has chosen not to limit anything looks like.
func New(client *redis.Client, logger *slog.Logger) *Limiter {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Limiter{client: client, logger: logger}
}

// allowScript counts one action and reports the count and the window's remaining life.
//
// A script because the two commands have to be one thing. Sent separately, a process dying
// between the increment and the expiry leaves a counter with no expiry — a caller permanently
// at their limit, with nothing to clear it and no reason anybody would look. Pipelining does not
// fix that; Redis executes the commands independently either way.
var allowScript = redis.NewScript(`
	local count = redis.call('INCR', KEYS[1])
	if count == 1 then
		redis.call('PEXPIRE', KEYS[1], ARGV[1])
	end
	return {count, redis.call('PTTL', KEYS[1])}
`)

// Decision is the answer, and why.
type Decision struct {
	// Allowed is whether the action may proceed.
	Allowed bool
	// RetryAfter is how long until the window resets. Zero when allowed.
	RetryAfter time.Duration
	// Count is how many actions this window has seen, including this one.
	Count int
}

// Allow counts one action against a key and reports whether it may proceed.
//
// A fixed window, and the flaw is worth naming: a caller who bursts at the end of one window and
// again at the start of the next gets twice the limit for an instant. A sliding window or a token
// bucket would not, at the cost of holding timestamps per caller. For the thing this is actually
// for — somebody hammering a form, a client stuck in a reconnect loop — twice the limit briefly
// is not a different outcome, and the simpler mechanism is the one that will still be understood
// when it starts refusing somebody legitimately.
func (l *Limiter) Allow(
	ctx context.Context,
	key string,
	limit int,
	window time.Duration,
) Decision {
	if l.client == nil || limit <= 0 {
		return Decision{Allowed: true}
	}

	result, err := allowScript.Run(ctx, l.client, []string{"ratelimit:" + key},
		window.Milliseconds()).Int64Slice()
	if err != nil {
		// Fails open, loudly. Logged rather than swallowed because a limiter that has
		// silently stopped limiting is exactly the state somebody needs to know about
		// before the day it matters.
		l.logger.Warn("rate limiter unavailable; allowing",
			slog.String("key", key), slog.Any("error", err))
		return Decision{Allowed: true}
	}
	if len(result) != 2 {
		return Decision{Allowed: true}
	}

	count, remaining := int(result[0]), time.Duration(result[1])*time.Millisecond
	if count <= limit {
		return Decision{Allowed: true, Count: count}
	}
	// A floor on what a caller is told to wait. A TTL that has just been read as zero or
	// negative — the window ending between the increment and the read — would otherwise
	// become "retry immediately", which is advice to try again and be refused again.
	if remaining <= 0 {
		remaining = window
	}
	return Decision{Allowed: false, RetryAfter: remaining, Count: count}
}

// Reset clears a caller's counter, for tests and for an operator undoing a mistake.
func (l *Limiter) Reset(ctx context.Context, key string) error {
	if l.client == nil {
		return nil
	}
	if err := l.client.Del(ctx, "ratelimit:"+key).Err(); err != nil {
		return fmt.Errorf("reset rate limit: %w", err)
	}
	return nil
}
