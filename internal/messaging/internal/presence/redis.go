// Package presence answers who is online and who is typing.
//
// Redis only, with expiry, and that is the whole design rather than an optimisation. Both of
// these facts are true for seconds and false afterwards, and neither is worth anything once it
// is stale: "was online three hours ago" is not presence, and a typing indicator that outlives
// the typing is worse than none. Writing them to Postgres would mean a durable record of
// something that is never correct for long, plus the work of deleting it again.
//
// Expiry is also what makes a node dying safe. Presence is a claim a node makes on behalf of a
// socket it holds; a node that stops refreshing stops making the claim, with no cleanup to run
// and nobody to run it. The alternative — a set that is added to on connect and removed from on
// disconnect — leaves a permanently online ghost every time a process is killed, which is the
// failure every presence system has had at least once.
//
// Sorted sets rather than plain keys with a TTL, because presence is per account and an account
// has several devices on several nodes. Each device is a member scored by when it was last
// seen, so a query is a range over the last window and a dead node's device ages out of it
// while its live sibling keeps the account online. A single key with one expiry could not do
// that: whichever node refreshed last would keep the dead device alive.
package presence

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"comms/internal/messaging/internal/domain"
)

// Windows.
const (
	// Online is how long a device stays online after its node last said so. Three
	// heartbeats' worth, so one lost refresh does not blink somebody offline.
	Online = 30 * time.Second

	// Heartbeat is how often a node renews the sockets it holds. Comfortably inside
	// Online, because the cost of being early is one Redis pipeline and the cost of being
	// late is somebody appearing to leave.
	Heartbeat = 10 * time.Second

	// Typing is how long a typing claim lasts without renewal. Short: a person who stopped
	// mid-word should stop showing as typing quickly, and a client that renews while keys
	// are still being pressed keeps it true.
	Typing = 6 * time.Second
)

// Store keeps presence and typing in Redis.
type Store struct {
	client *redis.Client
}

// NewStore returns a store over client.
func NewStore(client *redis.Client) *Store {
	return &Store{client: client}
}

func onlineKey(accountID string) string      { return "presence:" + accountID }
func typingKey(conversationID string) string { return "typing:" + conversationID }

// Renew records that a device is connected, and is what a node calls on a heartbeat.
//
// The key's own expiry is set alongside the member's score, so that an account nobody has
// refreshed disappears entirely rather than lingering as an empty set. Two windows' worth,
// because the scores inside are what decide who is online — this only stops the key itself
// from being immortal.
func (s *Store) Renew(ctx context.Context, accountID, deviceID string, now time.Time) error {
	pipeline := s.client.Pipeline()
	pipeline.ZAdd(ctx, onlineKey(accountID), redis.Z{
		Score:  float64(now.Unix()),
		Member: deviceID,
	})
	pipeline.Expire(ctx, onlineKey(accountID), 2*Online)

	if _, err := pipeline.Exec(ctx); err != nil {
		return fmt.Errorf("renew presence: %w", err)
	}
	return nil
}

// renewChunk is how many claims go in one pipeline.
//
// A node holding ten thousand sockets would otherwise assemble twenty thousand commands into a
// single buffer and hand Redis the lot. Chunked, the same heartbeat is ten round trips.
//
// ponytail: a fixed chunk. A Lua script taking the whole set, if one Exec per thousand ever
// shows up in a profile.
const renewChunk = 1000

// RenewAll renews every claim a node holds, in one pipeline per chunk.
//
// The heartbeat this serves runs every Heartbeat for every socket on the node, so it is the one
// presence operation whose cost scales with how busy a node is. One round trip per claim made a
// node with ten thousand sockets spend half its heartbeat window renewing them, and a node with
// twenty thousand unable to finish inside the window at all — which does not read as slowness,
// it reads as everybody blinking offline as the keys expire behind the loop.
func (s *Store) RenewAll(ctx context.Context, claims []domain.DeviceClaim, now time.Time) error {
	score := float64(now.Unix())

	for start := 0; start < len(claims); start += renewChunk {
		end := min(start+renewChunk, len(claims))

		pipeline := s.client.Pipeline()
		for _, claim := range claims[start:end] {
			key := onlineKey(claim.AccountID)
			pipeline.ZAdd(ctx, key, redis.Z{Score: score, Member: claim.DeviceID})
			pipeline.Expire(ctx, key, 2*Online)
		}
		if _, err := pipeline.Exec(ctx); err != nil {
			return fmt.Errorf("renew %d presence claims: %w", end-start, err)
		}
	}
	return nil
}

// Gone forgets a device immediately, on a socket closing cleanly.
//
// Not required for correctness — the score would age out on its own — and worth doing anyway,
// because "they closed the tab and vanished at once" is what people expect and thirty seconds
// of a stale dot is the kind of thing that reads as a bug.
func (s *Store) Gone(ctx context.Context, accountID, deviceID string) error {
	if err := s.client.ZRem(ctx, onlineKey(accountID), deviceID).Err(); err != nil {
		return fmt.Errorf("clear presence: %w", err)
	}
	return nil
}

// OnlineAmong returns which of the given accounts have a device connected.
//
// Asked in bulk because the question is always asked in bulk: a client rendering a conversation
// list wants every peer at once, and one round trip per name would make presence the most
// expensive thing on the screen.
func (s *Store) OnlineAmong(ctx context.Context, accountIDs []string, now time.Time) (map[string]bool, error) {
	online := make(map[string]bool, len(accountIDs))
	if len(accountIDs) == 0 {
		return online, nil
	}

	// The floor of the window, as a score. Anything older is a device whose node stopped
	// refreshing — usually because it died, since a clean close removes the member.
	since := strconv.FormatInt(now.Add(-Online).Unix(), 10)

	pipeline := s.client.Pipeline()
	counts := make([]*redis.IntCmd, len(accountIDs))
	for index, accountID := range accountIDs {
		counts[index] = pipeline.ZCount(ctx, onlineKey(accountID), since, "+inf")
	}
	if _, err := pipeline.Exec(ctx); err != nil && !isNoResult(err) {
		return nil, fmt.Errorf("read presence: %w", err)
	}

	for index, count := range counts {
		if value, err := count.Result(); err == nil && value > 0 {
			online[accountIDs[index]] = true
		}
	}
	return online, nil
}

// Typing records that an account is typing in a conversation.
//
// Per account rather than per device, like a read cursor: a person typing on their phone is
// typing, and which of their devices it was is not something anybody else's screen should show.
func (s *Store) Typing(ctx context.Context, conversationID, accountID string, now time.Time) error {
	pipeline := s.client.Pipeline()
	pipeline.ZAdd(ctx, typingKey(conversationID), redis.Z{
		Score:  float64(now.UnixMilli()),
		Member: accountID,
	})
	pipeline.Expire(ctx, typingKey(conversationID), 2*Typing)

	if _, err := pipeline.Exec(ctx); err != nil {
		return fmt.Errorf("record typing: %w", err)
	}
	return nil
}

// StoppedTyping clears a claim, for a client that sent a message or cleared its box.
func (s *Store) StoppedTyping(ctx context.Context, conversationID, accountID string) error {
	if err := s.client.ZRem(ctx, typingKey(conversationID), accountID).Err(); err != nil {
		return fmt.Errorf("clear typing: %w", err)
	}
	return nil
}

// TypingIn returns who is currently typing in a conversation.
//
// Stale members are dropped as they are found, which is the only cleanup this needs: the
// question is asked far more often than a claim expires, so the set stays small without
// anything sweeping it.
func (s *Store) TypingIn(ctx context.Context, conversationID string, now time.Time) ([]string, error) {
	key := typingKey(conversationID)
	floor := strconv.FormatInt(now.Add(-Typing).UnixMilli(), 10)

	pipeline := s.client.Pipeline()
	pipeline.ZRemRangeByScore(ctx, key, "-inf", "("+floor)
	current := pipeline.ZRangeByScore(ctx, key, &redis.ZRangeBy{Min: floor, Max: "+inf"})

	if _, err := pipeline.Exec(ctx); err != nil && !isNoResult(err) {
		return nil, fmt.Errorf("read typing: %w", err)
	}

	typing, err := current.Result()
	if err != nil && !isNoResult(err) {
		return nil, fmt.Errorf("read typing: %w", err)
	}
	return typing, nil
}

// FirstTime claims a notification, reporting whether this process is the first to do so.
//
// SET NX with an expiry, which is the whole mechanism: the first caller sets the key and is told
// so, everybody else finds it already there. Atomic in one command, so two nodes consuming the
// same redelivered record cannot both decide they are first.
//
// It lives here rather than in the push package because it is the same kind of thing as
// everything else in this file — a fact that matters for minutes and then does not. A permanent
// record of every notification ever sent would be a table nobody reads.
func (s *Store) FirstTime(ctx context.Context, key string) (bool, error) {
	claimed, err := s.client.SetNX(ctx, "notified:"+key, "1", NotificationMemory).Result()
	if err != nil {
		return false, fmt.Errorf("claim notification: %w", err)
	}
	return claimed, nil
}

// NotificationMemory is how long a sent notification is remembered, and it is a bet on how late
// a redelivery can be.
//
// An hour. Kafka redelivers within seconds normally, and after a consumer group is reset or a
// partition is reassigned it can be much later — but a notification about an hour-old message is
// not something to suppress, it is something that should never have been queued.
const NotificationMemory = time.Hour

// isNoResult reports whether an error is Redis saying a key does not exist.
//
// Which is an answer here rather than a failure: nobody online in a conversation nobody has
// opened is the ordinary case, and a pipeline whose commands all missed reports redis.Nil.
func isNoResult(err error) bool {
	return err == redis.Nil
}
