package api

import (
	"context"
	"fmt"
	"log/slog"
	"testing"

	"github.com/redis/go-redis/v9"

	"comms/internal/messaging/internal/broadcast"
	"comms/internal/messaging/internal/domain"
)

// newTestHub returns a hub whose Redis is unreachable. Subscribing fails and is
// logged, which is enough for the bookkeeping under test: what conversations the
// node would subscribe to is decided before Redis is asked.
func newTestHub() *Hub {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1})
	return &Hub{
		redis:       client,
		pubsub:      client.Subscribe(context.Background()),
		logger:      slog.New(slog.DiscardHandler),
		connections: make(map[domain.AccountID]map[*Connection]struct{}),
		followers:   make(map[domain.ConversationID]map[*Connection]struct{}),
	}
}

// hubWith builds a hub holding n connections, each following one conversation of
// its own. No sockets and no Redis: it wires the index the way Listen does, since
// delivery to a conversation none of them follows is what is being measured.
func hubWith(n int) *Hub {
	h := &Hub{
		logger:      slog.New(slog.DiscardHandler),
		connections: make(map[domain.AccountID]map[*Connection]struct{}, n),
		followers:   make(map[domain.ConversationID]map[*Connection]struct{}, n),
	}
	for i := range n {
		accountID := domain.AccountID(fmt.Sprintf("account-%d", i))
		conversationID := domain.ConversationID(fmt.Sprintf("conversation-%d", i))

		connection := NewConnection(nil, accountID, "device", h.logger)
		connection.watch(conversationID, domain.FirstSequence)

		h.connections[accountID] = map[*Connection]struct{}{connection: {}}
		h.follow(conversationID, connection)
	}
	return h
}

// BenchmarkDeliverEntry measures one broadcast for a conversation nobody local
// follows. Before the followers index this walked every connection on the node;
// it should now be flat in node size.
func BenchmarkDeliverEntry(b *testing.B) {
	for _, n := range []int{100, 1_000, 10_000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			h := hubWith(n)
			message := broadcast.BroadcastMessage{ConversationID: "elsewhere", Sequence: 1}
			for b.Loop() {
				h.deliverEntry(message)
			}
		})
	}
}

// TestDeliverEntryRespectsJoinPoint is the check the index must not break: being
// indexed under a conversation is not entitlement to every position in it.
func TestDeliverEntryRespectsJoinPoint(t *testing.T) {
	h := newTestHub()
	ctx := context.Background()

	early := NewConnection(nil, "early", "device", h.logger)
	late := NewConnection(nil, "late", "device", h.logger)
	h.Listen(ctx, early, "shared", domain.FirstSequence)
	h.Listen(ctx, late, "shared", 40)

	if got := len(h.followers["shared"]); got != 2 {
		t.Fatalf("followers of shared = %d, want 2", got)
	}

	h.deliverEntry(broadcast.BroadcastMessage{ConversationID: "shared", Sequence: 39})
	if got := len(early.outbound); got != 1 {
		t.Errorf("early received %d frames for position 39, want 1", got)
	}
	if got := len(late.outbound); got != 0 {
		t.Errorf("late received %d frames for position 39, want 0 — it joined at 40", got)
	}

	h.deliverEntry(broadcast.BroadcastMessage{ConversationID: "shared", Sequence: 40})
	if got := len(late.outbound); got != 1 {
		t.Errorf("late received %d frames for position 40, want 1", got)
	}

	// Listening twice must not inflate the index, or the conversation is never
	// unsubscribed.
	h.Listen(ctx, early, "shared", domain.FirstSequence)
	if got := len(h.followers["shared"]); got != 2 {
		t.Errorf("followers of shared = %d after a repeat Listen, want 2", got)
	}

	// And the last follower leaving must drop the conversation rather than leave an
	// empty map behind for every conversation the node has ever seen.
	h.Unregister(ctx, early)
	h.Unregister(ctx, late)
	if _, found := h.followers["shared"]; found {
		t.Error("shared still indexed after both connections left")
	}
}
