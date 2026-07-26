// Package ratelimit_test drives the limiter against real Redis.
//
// Real Redis rather than a fake, for the reason every other integration test here uses the real
// thing: what is being tested is a Lua script's atomicity and a key's expiry, and a fake would
// agree with whatever this code believes about both.
package ratelimit_test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"comms/internal/platform/id"
	"comms/internal/platform/ratelimit"
)

func openRedis(t *testing.T) *redis.Client {
	t.Helper()

	url := os.Getenv("REDIS_URL")
	if url == "" {
		t.Skip("REDIS_URL not set, skipping integration test")
	}

	options, err := redis.ParseURL(url)
	if err != nil {
		t.Fatalf("parse redis url: %v", err)
	}
	client := redis.NewClient(options)
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping redis: %v", err)
	}
	return client
}

func TestTheLimitIsTheLimit(t *testing.T) {
	t.Parallel()
	limiter := ratelimit.New(openRedis(t), slog.New(slog.DiscardHandler))

	ctx := context.Background()
	key := "test:" + id.New()

	for attempt := 1; attempt <= 3; attempt++ {
		if decision := limiter.Allow(ctx, key, 3, time.Minute); !decision.Allowed {
			t.Fatalf("attempt %d of 3 was refused", attempt)
		}
	}

	fourth := limiter.Allow(ctx, key, 3, time.Minute)
	if fourth.Allowed {
		t.Fatal("a fourth action was allowed against a limit of three")
	}
	// Told how long to wait, and told something usable. "Retry immediately" from a
	// limiter that will refuse again is worse than no advice.
	if fourth.RetryAfter <= 0 || fourth.RetryAfter > time.Minute {
		t.Fatalf("retry after %s, want something inside the window", fourth.RetryAfter)
	}
}

func TestTheWindowExpires(t *testing.T) {
	t.Parallel()
	limiter := ratelimit.New(openRedis(t), slog.New(slog.DiscardHandler))

	ctx := context.Background()
	key := "test:" + id.New()

	// A window short enough to wait out, which is the only way to test that the key
	// expires at all. Mocking the clock would test that the code passes a number to Redis.
	const window = 300 * time.Millisecond
	if decision := limiter.Allow(ctx, key, 1, window); !decision.Allowed {
		t.Fatal("the first action was refused")
	}
	if decision := limiter.Allow(ctx, key, 1, window); decision.Allowed {
		t.Fatal("the second action inside the window was allowed")
	}

	time.Sleep(window + 100*time.Millisecond)

	if decision := limiter.Allow(ctx, key, 1, window); !decision.Allowed {
		t.Fatal("the window did not expire — the counter has no expiry")
	}
}

func TestCallersAreCountedSeparately(t *testing.T) {
	t.Parallel()
	limiter := ratelimit.New(openRedis(t), slog.New(slog.DiscardHandler))

	ctx := context.Background()
	mine, theirs := "test:"+id.New(), "test:"+id.New()

	if decision := limiter.Allow(ctx, mine, 1, time.Minute); !decision.Allowed {
		t.Fatal("my first action was refused")
	}
	if decision := limiter.Allow(ctx, mine, 1, time.Minute); decision.Allowed {
		t.Fatal("my second action was allowed")
	}
	// The whole point of a per-caller limit: somebody else hitting theirs must not cost
	// me mine. A limiter keyed on the wrong thing passes every test above and this one
	// is what catches it.
	if decision := limiter.Allow(ctx, theirs, 1, time.Minute); !decision.Allowed {
		t.Fatal("somebody else's first action was refused because of mine")
	}
}

// TestNoRedisMeansNoLimit is the failure mode this is designed to have.
//
// A limiter that cannot reach Redis allows the action. Stated as a test because it is the kind
// of decision somebody later reads as a bug and "fixes" — turning a cache outage into a total
// outage, and contradicting the requirement that sends survive Redis being killed.
func TestNoRedisMeansNoLimit(t *testing.T) {
	t.Parallel()

	limiter := ratelimit.New(nil, slog.New(slog.DiscardHandler))
	for attempt := range 10 {
		if decision := limiter.Allow(context.Background(), "test", 1, time.Minute); !decision.Allowed {
			t.Fatalf("attempt %d was refused with no limiter configured", attempt)
		}
	}
}
