package messagingapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"

	"github.com/redis/go-redis/v9"

	"comms/internal/messaging"
	"comms/internal/messaging/messagingredis"
)

// Hub tracks the connections this node holds and routes broadcasts to them.
//
// It is the receiving half of ADR-0005's ephemeral path. Two subscription kinds:
// one per conversation any local connection belongs to, so that an entry costs the
// sender a single publish; and one per locally connected account, for messages
// addressed to an account rather than a conversation.
//
// Subscriptions are reference counted. Ten devices in the same conversation share
// one Redis subscription, and it is dropped when the last of them goes.
type Hub struct {
	redis  *redis.Client
	pubsub *redis.PubSub
	logger *slog.Logger

	mutex sync.RWMutex
	// connections holds every live connection for an account. An account is
	// commonly connected from more than one device (a CLI and a browser), and all
	// of them receive everything — read state is per membership, not per device.
	connections map[messaging.AccountID]map[*Connection]struct{}
	// listeners counts local connections per conversation, so a subscription is
	// dropped only when the last interested connection leaves.
	listeners map[messaging.ConversationID]int
}

// NewHub returns a hub subscribing through client.
func NewHub(client *redis.Client, logger *slog.Logger) *Hub {
	return &Hub{
		redis:       client,
		pubsub:      client.Subscribe(context.Background()),
		logger:      logger,
		connections: make(map[messaging.AccountID]map[*Connection]struct{}),
		listeners:   make(map[messaging.ConversationID]int),
	}
}

// Run delivers messages until ctx is cancelled. It owns the pubsub connection, so
// exactly one goroutine reads from Redis regardless of how many sockets are held.
func (h *Hub) Run(ctx context.Context) {
	defer func() { _ = h.pubsub.Close() }()

	incoming := h.pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case message, ok := <-incoming:
			if !ok {
				return
			}
			h.dispatch(ctx, message)
		}
	}
}

// dispatch routes one Redis message to the connections that should see it.
func (h *Hub) dispatch(ctx context.Context, message *redis.Message) {
	switch {
	case isEntriesChannel(message.Channel):
		var broadcast messagingredis.BroadcastMessage
		if err := json.Unmarshal([]byte(message.Payload), &broadcast); err != nil {
			h.logger.Warn("decode broadcast", slog.Any("error", err))
			return
		}
		h.deliverEntry(broadcast)

	case isControlChannel(message.Channel):
		var control messagingredis.ControlMessage
		if err := json.Unmarshal([]byte(message.Payload), &control); err != nil {
			h.logger.Warn("decode control message", slog.Any("error", err))
			return
		}
		h.handleControl(ctx, control)
	}
}

// deliverEntry sends a broadcast to every local connection entitled to it.
func (h *Hub) deliverEntry(broadcast messagingredis.BroadcastMessage) {
	conversationID := messaging.ConversationID(broadcast.ConversationID)

	h.mutex.RLock()
	targets := make([]*Connection, 0, 8)
	for _, connections := range h.connections {
		for connection := range connections {
			// Visibility is checked per connection: a member who joined at
			// position 40 must not receive 39, even though their node is
			// subscribed to the conversation for other members' sake.
			if connection.Sees(conversationID, messaging.Sequence(broadcast.Sequence)) {
				targets = append(targets, connection)
			}
		}
	}
	h.mutex.RUnlock()

	// Sent outside the lock: a slow socket must not block the hub, and the send
	// itself is non-blocking — see Connection.Send.
	for _, connection := range targets {
		connection.Send(broadcast)
	}
}

// handleControl reacts to a message addressed to an account.
func (h *Hub) handleControl(ctx context.Context, control messagingredis.ControlMessage) {
	if control.Type != "conversation_started" {
		return
	}

	accountID := messaging.AccountID(control.AccountID)
	conversationID := messaging.ConversationID(control.ConversationID)

	h.mutex.Lock()
	connections := make([]*Connection, 0, len(h.connections[accountID]))
	for connection := range h.connections[accountID] {
		connections = append(connections, connection)
	}
	h.mutex.Unlock()

	// A conversation created while a recipient is connected would otherwise
	// deliver nothing live until they reconnected.
	for _, connection := range connections {
		h.Listen(ctx, connection, conversationID, messaging.FirstSequence)
	}
}

// Register adds a connection and subscribes to what it needs to hear.
func (h *Hub) Register(ctx context.Context, connection *Connection) {
	h.mutex.Lock()
	if h.connections[connection.AccountID()] == nil {
		h.connections[connection.AccountID()] = make(map[*Connection]struct{})
	}
	h.connections[connection.AccountID()][connection] = struct{}{}

	newChannels := []string{controlChannelFor(connection.AccountID())}
	for conversationID := range connection.Visibility() {
		h.listeners[conversationID]++
		if h.listeners[conversationID] == 1 {
			newChannels = append(newChannels, entriesChannelFor(conversationID))
		}
	}
	h.mutex.Unlock()

	if err := h.pubsub.Subscribe(ctx, newChannels...); err != nil {
		h.logger.Warn("subscribe", slog.Any("error", err), slog.Int("channels", len(newChannels)))
	}
}

// Listen adds one conversation to a connection's interests.
func (h *Hub) Listen(ctx context.Context, connection *Connection, conversationID messaging.ConversationID, visibleFrom messaging.Sequence) {
	h.mutex.Lock()
	if connection.alreadySees(conversationID) {
		h.mutex.Unlock()
		return
	}
	connection.watch(conversationID, visibleFrom)

	h.listeners[conversationID]++
	subscribe := h.listeners[conversationID] == 1
	h.mutex.Unlock()

	if subscribe {
		if err := h.pubsub.Subscribe(ctx, entriesChannelFor(conversationID)); err != nil {
			h.logger.Warn("subscribe conversation", slog.Any("error", err))
		}
	}
}

// Unregister removes a connection and drops subscriptions nothing local needs.
func (h *Hub) Unregister(ctx context.Context, connection *Connection) {
	h.mutex.Lock()
	delete(h.connections[connection.AccountID()], connection)

	stale := make([]string, 0, 4)
	if len(h.connections[connection.AccountID()]) == 0 {
		delete(h.connections, connection.AccountID())
		stale = append(stale, controlChannelFor(connection.AccountID()))
	}

	for conversationID := range connection.Visibility() {
		h.listeners[conversationID]--
		if h.listeners[conversationID] <= 0 {
			delete(h.listeners, conversationID)
			stale = append(stale, entriesChannelFor(conversationID))
		}
	}
	h.mutex.Unlock()

	if len(stale) > 0 {
		if err := h.pubsub.Unsubscribe(ctx, stale...); err != nil {
			h.logger.Warn("unsubscribe", slog.Any("error", err))
		}
	}
}

// DisconnectDevice closes any connection held by a device.
//
// Called when Identity reports a device revoked. Without it a revoked device keeps
// receiving entries on an already-open socket until its access token happens to
// expire — authentication only runs at connect time.
func (h *Hub) DisconnectDevice(deviceID string) {
	h.mutex.RLock()
	var doomed []*Connection
	for _, connections := range h.connections {
		for connection := range connections {
			if connection.DeviceID() == deviceID {
				doomed = append(doomed, connection)
			}
		}
	}
	h.mutex.RUnlock()

	for _, connection := range doomed {
		connection.Close("device revoked")
	}
}

// ConnectionCount reports how many connections this node holds. Used by tests and
// by the health endpoint.
func (h *Hub) ConnectionCount() int {
	h.mutex.RLock()
	defer h.mutex.RUnlock()

	var count int
	for _, connections := range h.connections {
		count += len(connections)
	}
	return count
}

func entriesChannelFor(id messaging.ConversationID) string { return "entries:" + string(id) }
func controlChannelFor(id messaging.AccountID) string      { return "control:" + string(id) }

func isEntriesChannel(channel string) bool { return len(channel) > 8 && channel[:8] == "entries:" }
func isControlChannel(channel string) bool { return len(channel) > 8 && channel[:8] == "control:" }
