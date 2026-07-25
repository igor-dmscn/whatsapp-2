package infrastructure

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"comms/internal/messaging/domain"
)

// Broadcast channel names.
//
// Entries go to a per-conversation channel rather than per-recipient, so one send
// is one publish regardless of member count — a channel broadcast to fifty
// thousand readers costs the same as a direct message (NF-12). The cost is moved
// to the subscriber, which listens to the conversations its connected accounts
// belong to.
//
// Control messages are per-account, because "you have been added to a
// conversation" is addressed to an account rather than to a conversation the
// recipient is not yet listening to.
func entriesChannel(id domain.ConversationID) string { return "entries:" + string(id) }
func controlChannel(id domain.AccountID) string      { return "control:" + string(id) }

// RedisBroadcaster publishes onto the ephemeral path of ADR-0005.
//
// At-most-once and allowed to fail. A dropped publish is recovered by the
// receiving client noticing a sequence gap, which it must do for offline sync
// anyway.
type RedisBroadcaster struct {
	client *redis.Client
}

// NewRedisBroadcaster returns a broadcaster over client.
func NewRedisBroadcaster(client *redis.Client) *RedisBroadcaster {
	return &RedisBroadcaster{client: client}
}

var _ domain.Broadcaster = (*RedisBroadcaster)(nil)

// BroadcastMessage is the wire form of an entry on the ephemeral path.
//
// The body is carried so a listener can render immediately without a round trip.
// It is base64 in JSON because the payload is bytes the server does not interpret
// and must survive transit unexamined (ADR-0001).
type BroadcastMessage struct {
	Type           string    `json:"type"`
	ConversationID string    `json:"conversation_id"`
	EntryID        string    `json:"entry_id"`
	Sequence       int64     `json:"sequence"`
	AuthorID       string    `json:"author_id"`
	ClientEntryID  string    `json:"client_entry_id"`
	Kind           string    `json:"kind"`
	ContentType    string    `json:"content_type"`
	Body           string    `json:"body"`
	CreatedAt      time.Time `json:"created_at"`
}

// ControlMessage tells a node something about an account rather than a
// conversation.
type ControlMessage struct {
	Type           string `json:"type"`
	AccountID      string `json:"account_id"`
	ConversationID string `json:"conversation_id"`
}

func (b *RedisBroadcaster) BroadcastEntry(ctx context.Context, entry *domain.Entry) error {
	encoded, err := json.Marshal(BroadcastMessage{
		Type:           "entry",
		ConversationID: string(entry.ConversationID()),
		EntryID:        string(entry.ID()),
		Sequence:       int64(entry.Sequence()),
		AuthorID:       string(entry.AuthorID()),
		ClientEntryID:  string(entry.ClientEntryID()),
		Kind:           string(entry.Kind()),
		ContentType:    entry.Payload().ContentType(),
		Body:           base64.StdEncoding.EncodeToString(entry.Payload().Body()),
		CreatedAt:      entry.CreatedAt(),
	})
	if err != nil {
		return fmt.Errorf("encode entry broadcast: %w", err)
	}

	if err := b.client.Publish(ctx, entriesChannel(entry.ConversationID()), encoded).Err(); err != nil {
		return fmt.Errorf("publish entry: %w", err)
	}
	return nil
}

func (b *RedisBroadcaster) NotifyConversationStarted(ctx context.Context, accountID domain.AccountID, conversationID domain.ConversationID) error {
	encoded, err := json.Marshal(ControlMessage{
		Type:           "conversation_started",
		AccountID:      string(accountID),
		ConversationID: string(conversationID),
	})
	if err != nil {
		return fmt.Errorf("encode control message: %w", err)
	}

	if err := b.client.Publish(ctx, controlChannel(accountID), encoded).Err(); err != nil {
		return fmt.Errorf("publish control message: %w", err)
	}
	return nil
}

// OpenRedis returns a client, verifying it can be reached.
//
// Failing at boot is deliberate: a node that cannot reach Redis will deliver
// nothing live, and should not enter a load balancer's rotation.
func OpenRedis(ctx context.Context, url string) (*redis.Client, error) {
	options, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}

	client := redis.NewClient(options)
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := client.Ping(pingCtx).Err(); err != nil {
		return nil, fmt.Errorf("ping redis: %w", err)
	}
	return client, nil
}
