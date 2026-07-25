package messagingapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/coder/websocket"

	"comms/internal/messaging"
	"comms/internal/messaging/messagingredis"
)

// outboundBuffer is how many messages may queue for one socket.
//
// A bounded buffer forces the slow-consumer question to be answered rather than
// discovered: past this, the connection is closed instead of the server growing
// memory on its behalf. Recovery is the client reconnecting and syncing the gap,
// which costs it one fetch and costs the server nothing.
const outboundBuffer = 64

// writeTimeout bounds a single frame write. A socket that cannot accept a frame in
// this long is not slow, it is gone.
const writeTimeout = 10 * time.Second

// Connection is one authenticated WebSocket.
//
// It knows which conversations it should hear about and from which position, so
// that visibility is enforced per connection rather than per node. A node
// subscribes to a conversation because *some* local connection belongs to it;
// whether *this* connection may see a given entry is a different question.
type Connection struct {
	socket    *websocket.Conn
	accountID messaging.AccountID
	deviceID  string
	logger    *slog.Logger

	outbound chan messagingredis.BroadcastMessage

	// closeOnce guards against the several paths that can end a connection at
	// once: client close, write failure, buffer overflow, device revocation.
	closeOnce sync.Once
	done      chan struct{}
	closeMsg  string

	mutex sync.RWMutex
	// visibility maps each conversation this connection follows to the first
	// position it may see.
	visibility map[messaging.ConversationID]messaging.Sequence
}

// NewConnection wraps an authenticated socket.
func NewConnection(socket *websocket.Conn, accountID messaging.AccountID, deviceID string, logger *slog.Logger) *Connection {
	return &Connection{
		socket:     socket,
		accountID:  accountID,
		deviceID:   deviceID,
		logger:     logger,
		outbound:   make(chan messagingredis.BroadcastMessage, outboundBuffer),
		done:       make(chan struct{}),
		visibility: make(map[messaging.ConversationID]messaging.Sequence),
	}
}

func (c *Connection) AccountID() messaging.AccountID { return c.accountID }
func (c *Connection) DeviceID() string               { return c.deviceID }

// Visibility returns a copy of what this connection follows.
func (c *Connection) Visibility() map[messaging.ConversationID]messaging.Sequence {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	copied := make(map[messaging.ConversationID]messaging.Sequence, len(c.visibility))
	for conversationID, from := range c.visibility {
		copied[conversationID] = from
	}
	return copied
}

// watch records that this connection follows a conversation from a position.
// Callers hold the hub's lock; this does not take its own.
func (c *Connection) watch(conversationID messaging.ConversationID, visibleFrom messaging.Sequence) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.visibility[conversationID] = visibleFrom
}

func (c *Connection) alreadySees(conversationID messaging.ConversationID) bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	_, found := c.visibility[conversationID]
	return found
}

// Sees reports whether this connection is entitled to an entry at a position.
func (c *Connection) Sees(conversationID messaging.ConversationID, sequence messaging.Sequence) bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	visibleFrom, following := c.visibility[conversationID]
	return following && sequence >= visibleFrom
}

// Send queues a message, or closes the connection if it cannot keep up.
//
// Non-blocking on purpose. The alternative — blocking until the socket drains —
// would let one unresponsive client stall the hub goroutine and stop delivery for
// everyone on the node.
func (c *Connection) Send(message messagingredis.BroadcastMessage) {
	select {
	case c.outbound <- message:
	case <-c.done:
	default:
		// Dropping a message silently would leave the client believing it is
		// current when it is not. Closing makes it reconnect and sync the gap,
		// which is the mechanism that already exists for exactly this.
		c.logger.Warn("outbound buffer full, closing connection",
			slog.String("account_id", string(c.accountID)),
			slog.String("device_id", c.deviceID),
		)
		c.Close("too slow")
	}
}

// Write pumps queued messages onto the socket until the connection ends.
func (c *Connection) Write(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.done:
			return
		case message := <-c.outbound:
			if err := c.writeJSON(ctx, message); err != nil {
				c.Close("write failed")
				return
			}
		}
	}
}

// WriteFrame sends one message immediately, bypassing the queue. Used for
// handshake replies, which must not be reordered behind buffered entries.
func (c *Connection) WriteFrame(ctx context.Context, message any) error {
	return c.writeJSON(ctx, message)
}

func (c *Connection) writeJSON(ctx context.Context, message any) error {
	encoded, err := json.Marshal(message)
	if err != nil {
		// Encoding failures are bugs, not transport problems, and must not be
		// reported as a dead socket.
		c.logger.Error("encode outbound message", slog.Any("error", err))
		return nil
	}

	writeCtx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()

	//nolint:wrapcheck // the caller only decides whether to close; the cause is logged here.
	return c.socket.Write(writeCtx, websocket.MessageText, encoded)
}

// Close ends the connection once, whichever path gets there first.
func (c *Connection) Close(reason string) {
	c.closeOnce.Do(func() {
		c.closeMsg = reason
		close(c.done)
		// StatusNormalClosure even for "too slow" and "device revoked": the client
		// should reconnect and resync, which is exactly what a normal closure
		// tells it to consider.
		_ = c.socket.Close(websocket.StatusNormalClosure, reason)
	})
}

// Done is closed when the connection ends.
func (c *Connection) Done() <-chan struct{} { return c.done }
