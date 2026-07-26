package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"

	"github.com/redis/go-redis/v9"

	"comms/internal/messaging/internal/broadcast"
	"comms/internal/messaging/internal/domain"
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
	connections map[domain.AccountID]map[*Connection]struct{}
	// listeners counts local connections per conversation, so a subscription is
	// dropped only when the last interested connection leaves.
	listeners map[domain.ConversationID]int
}

// NewHub returns a hub subscribing through client.
func NewHub(client *redis.Client, logger *slog.Logger) *Hub {
	return &Hub{
		redis:       client,
		pubsub:      client.Subscribe(context.Background()),
		logger:      logger,
		connections: make(map[domain.AccountID]map[*Connection]struct{}),
		listeners:   make(map[domain.ConversationID]int),
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
		// Two shapes share this channel. Peeked at rather than given separate
		// channels, because a reaction is about a conversation exactly as an entry is
		// and a second channel would double every node's subscription count for
		// information the same connections already want.
		var envelope struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal([]byte(message.Payload), &envelope); err != nil {
			h.logger.Warn("decode broadcast", slog.Any("error", err))
			return
		}

		if envelope.Type == "call" {
			var changed broadcast.CallMessage
			if err := json.Unmarshal([]byte(message.Payload), &changed); err != nil {
				h.logger.Warn("decode call change", slog.Any("error", err))
				return
			}
			// Every connection following the conversation, with no visibility check: a
			// call belongs to the conversation rather than to a position in its log, so
			// there is no sequence to compare against a join point. What a late joiner
			// learns is that a call exists, which they are entitled to join anyway.
			h.deliverToFollowers(domain.ConversationID(changed.ConversationID), changed)
			return
		}

		if envelope.Type == "attachment" {
			var attachment broadcast.AttachmentMessage
			if err := json.Unmarshal([]byte(message.Payload), &attachment); err != nil {
				h.logger.Warn("decode attachment change", slog.Any("error", err))
				return
			}
			h.deliverAttachment(attachment)
			return
		}

		if envelope.Type == "reaction" {
			var reaction broadcast.ReactionMessage
			if err := json.Unmarshal([]byte(message.Payload), &reaction); err != nil {
				h.logger.Warn("decode reaction", slog.Any("error", err))
				return
			}
			h.deliverReaction(reaction)
			return
		}

		var broadcast broadcast.BroadcastMessage
		if err := json.Unmarshal([]byte(message.Payload), &broadcast); err != nil {
			h.logger.Warn("decode broadcast", slog.Any("error", err))
			return
		}
		h.deliverEntry(broadcast)

	case isControlChannel(message.Channel):
		var control broadcast.ControlMessage
		if err := json.Unmarshal([]byte(message.Payload), &control); err != nil {
			h.logger.Warn("decode control message", slog.Any("error", err))
			return
		}
		h.handleControl(ctx, control)
	}
}

// deliverEntry sends a broadcast to every local connection entitled to it.
func (h *Hub) deliverEntry(broadcast broadcast.BroadcastMessage) {
	conversationID := domain.ConversationID(broadcast.ConversationID)

	h.mutex.RLock()
	targets := make([]*Connection, 0, 8)
	for _, connections := range h.connections {
		for connection := range connections {
			// Visibility is checked per connection: a member who joined at
			// position 40 must not receive 39, even though their node is
			// subscribed to the conversation for other members' sake.
			if connection.Sees(conversationID, domain.Sequence(broadcast.Sequence)) {
				targets = append(targets, connection)
			}
		}
	}
	h.mutex.RUnlock()

	if len(targets) == 0 {
		return
	}

	// Encoded once for every recipient on this node. The payload is identical, and
	// re-marshalling it per connection was pure waste on a channel with many local
	// readers.
	encoded, err := json.Marshal(broadcast)
	if err != nil {
		h.logger.Error("encode entry frame", slog.Any("error", err))
		return
	}

	// Sent outside the lock: a slow socket must not block the hub, and the send
	// itself is non-blocking — see Connection.Send.
	for _, connection := range targets {
		connection.Send(encoded)
	}
}

// deliverReaction sends a reaction to every local connection entitled to see the
// entry it is about.
func (h *Hub) deliverReaction(message broadcast.ReactionMessage) {
	conversationID := domain.ConversationID(message.ConversationID)

	h.mutex.RLock()
	targets := make([]*Connection, 0, 8)
	for _, connections := range h.connections {
		for connection := range connections {
			// The same visibility check as an entry, for the same reason: a reaction
			// on position 39 tells you position 39 exists.
			if connection.Sees(conversationID, domain.Sequence(message.Sequence)) {
				targets = append(targets, connection)
			}
		}
	}
	h.mutex.RUnlock()

	if len(targets) == 0 {
		return
	}

	encoded, err := json.Marshal(message)
	if err != nil {
		h.logger.Error("encode reaction frame", slog.Any("error", err))
		return
	}
	for _, connection := range targets {
		connection.Send(encoded)
	}
}

// deliverToFollowers sends a frame to every local connection following a conversation.
//
// No visibility check, which is right for facts about a conversation rather than about a
// position in its log — a call, an attachment becoming ready. Anything that names a sequence
// must go through deliverEntry instead, where the join point is applied.
func (h *Hub) deliverToFollowers(conversationID domain.ConversationID, frame any) {
	h.mutex.RLock()
	targets := make([]*Connection, 0, 8)
	for _, connections := range h.connections {
		for connection := range connections {
			if connection.alreadySees(conversationID) {
				targets = append(targets, connection)
			}
		}
	}
	h.mutex.RUnlock()

	if len(targets) == 0 {
		return
	}

	encoded, err := json.Marshal(frame)
	if err != nil {
		h.logger.Error("encode frame", slog.Any("error", err))
		return
	}
	for _, connection := range targets {
		connection.Send(encoded)
	}
}

// deliverAttachment tells every local connection following the conversation to look
// at an attachment again.
//
// No visibility check, and that is a decision rather than an omission. Readiness is a
// fact about an attachment, which has no position in the log, so there is no sequence
// to check against a join point. What a connection outside the attachment's visibility
// learns is one opaque identifier — and if it acts on it, the fetch is refused, because
// Media asks Messaging whether the entry referencing that attachment is visible to the
// asker. The authorisation lives on the read, where it can be exact.
func (h *Hub) deliverAttachment(message broadcast.AttachmentMessage) {
	conversationID := domain.ConversationID(message.ConversationID)

	h.mutex.RLock()
	targets := make([]*Connection, 0, 8)
	for _, connections := range h.connections {
		for connection := range connections {
			if connection.alreadySees(conversationID) {
				targets = append(targets, connection)
			}
		}
	}
	h.mutex.RUnlock()

	if len(targets) == 0 {
		return
	}

	encoded, err := json.Marshal(message)
	if err != nil {
		h.logger.Error("encode attachment frame", slog.Any("error", err))
		return
	}
	for _, connection := range targets {
		connection.Send(encoded)
	}
}

// handleControl reacts to a message addressed to an account.
func (h *Hub) handleControl(ctx context.Context, control broadcast.ControlMessage) {
	if control.Type != "conversation_started" {
		return
	}

	accountID := domain.AccountID(control.AccountID)
	conversationID := domain.ConversationID(control.ConversationID)

	h.mutex.Lock()
	connections := make([]*Connection, 0, len(h.connections[accountID]))
	for connection := range h.connections[accountID] {
		connections = append(connections, connection)
	}
	h.mutex.Unlock()

	// A conversation created while a recipient is connected would otherwise
	// deliver nothing live until they reconnected.
	for _, connection := range connections {
		h.Listen(ctx, connection, conversationID, domain.FirstSequence)
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
func (h *Hub) Listen(ctx context.Context, connection *Connection, conversationID domain.ConversationID, visibleFrom domain.Sequence) {
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

func entriesChannelFor(id domain.ConversationID) string { return "entries:" + string(id) }
func controlChannelFor(id domain.AccountID) string      { return "control:" + string(id) }

func isEntriesChannel(channel string) bool { return len(channel) > 8 && channel[:8] == "entries:" }
func isControlChannel(channel string) bool { return len(channel) > 8 && channel[:8] == "control:" }
