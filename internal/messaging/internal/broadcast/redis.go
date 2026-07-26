package broadcast

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"comms/internal/messaging/internal/domain"
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
	// TargetSequence and ReplyTo travel with the broadcast so a live client can
	// apply an edit without a round trip. Omitting them would make every revision
	// cost the recipient a fetch to discover what it amends — which is the one thing
	// carrying the body was meant to avoid.
	TargetSequence int64 `json:"target_sequence,omitempty"`
	ReplyTo        int64 `json:"reply_to,omitempty"`
	// AttachmentID travels with the broadcast for the same reason the body does: a
	// live recipient must be able to render a placeholder immediately, and discovering
	// the reference would otherwise cost a fetch of an entry it already has.
	AttachmentID string `json:"attachment_id,omitempty"`
}

// ReactionMessage is the wire form of a reaction on the ephemeral path.
//
// On the conversation's channel, like an entry, so the hub's existing per-connection
// visibility check applies unchanged: a member who joined at position 40 must not be
// told about a reaction on 39.
//
// Removed rather than a separate message type, because a client applies both the same
// way — set or clear one (account, emoji) pair on one entry — and two types would be
// two code paths for one idea.
type ReactionMessage struct {
	Type           string    `json:"type"`
	ConversationID string    `json:"conversation_id"`
	Sequence       int64     `json:"sequence"`
	AccountID      string    `json:"account_id"`
	Emoji          string    `json:"emoji"`
	Removed        bool      `json:"removed"`
	CreatedAt      time.Time `json:"created_at"`
}

// AttachmentMessage tells clients an attachment changed and carries no state.
//
// Deliberately only "look again": readiness is a fact about an attachment, not about a
// position in the log, and a frame carrying variant URLs would be stale the moment they
// expire. The client re-fetches, which it must do anyway to obtain those URLs.
//
// On the conversation's channel, like an entry, so no new subscription is needed. It
// has no sequence, which means the hub cannot apply its per-connection visibility check
// — see deliverAttachment for why that is safe.
type AttachmentMessage struct {
	Type           string `json:"type"`
	ConversationID string `json:"conversation_id"`
	AttachmentID   string `json:"attachment_id"`
}

// CallMessage tells clients a conversation's call changed, and carries no state.
//
// The same shape and the same reasoning as an attachment change: what a client needs is to
// ask again, and a frame carrying participants would be stale the moment somebody joined.
// This is what makes a phone ring — and it is allowed to fail, because the durable answer to
// "is there a call" is a query the client can make itself (ADR-0005).
type CallMessage struct {
	Type           string `json:"type"`
	ConversationID string `json:"conversation_id"`
	CallID         string `json:"call_id"`
}

// TypingMessage tells a conversation that somebody started or stopped typing.
//
// On the conversation's channel like an entry, and with no sequence — so the hub cannot apply
// its per-connection visibility check, and does not need to. Typing is a fact about a person
// now, not about a position in the log, and there is no join point it could predate.
//
// Started and stopped in one shape, with a flag, because a client applies both the same way:
// add or remove one account from one set.
type TypingMessage struct {
	Type           string `json:"type"`
	ConversationID string `json:"conversation_id"`
	AccountID      string `json:"account_id"`
	Typing         bool   `json:"typing"`
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
		TargetSequence: int64(entry.Target()),
		ReplyTo:        int64(entry.ReplyTo()),
		AttachmentID:   string(entry.AttachmentID()),
	})
	if err != nil {
		return fmt.Errorf("encode entry broadcast: %w", err)
	}

	if err := b.client.Publish(ctx, entriesChannel(entry.ConversationID()), encoded).Err(); err != nil {
		return fmt.Errorf("publish entry: %w", err)
	}
	return nil
}

func (b *RedisBroadcaster) BroadcastReaction(ctx context.Context, reaction domain.Reaction, removed bool) error {
	encoded, err := json.Marshal(ReactionMessage{
		Type:           "reaction",
		ConversationID: string(reaction.ConversationID),
		Sequence:       int64(reaction.Sequence),
		AccountID:      string(reaction.AccountID),
		Emoji:          string(reaction.Emoji),
		Removed:        removed,
		CreatedAt:      reaction.CreatedAt,
	})
	if err != nil {
		return fmt.Errorf("encode reaction broadcast: %w", err)
	}

	if err := b.client.Publish(ctx, entriesChannel(reaction.ConversationID), encoded).Err(); err != nil {
		return fmt.Errorf("publish reaction: %w", err)
	}
	return nil
}

// AttachmentChanged is how Media reaches connected clients.
//
// Media does not publish here itself: it would have to know this package's channel
// names, and a shared string is a coupling with no compiler to notice when it breaks.
// So Messaging owns the fanout and Media asks for it through a port.
func (b *RedisBroadcaster) AttachmentChanged(ctx context.Context, conversationID, attachmentID string) error {
	encoded, err := json.Marshal(AttachmentMessage{
		Type:           "attachment",
		ConversationID: conversationID,
		AttachmentID:   attachmentID,
	})
	if err != nil {
		return fmt.Errorf("encode attachment broadcast: %w", err)
	}

	if err := b.client.Publish(ctx, entriesChannel(domain.ConversationID(conversationID)), encoded).Err(); err != nil {
		return fmt.Errorf("publish attachment change: %w", err)
	}
	return nil
}

// CallChanged is how Calling reaches connected clients.
//
// On the conversation's channel, so a call announces itself to exactly the people entitled to
// join it, at the cost of one publish however many that is.
func (b *RedisBroadcaster) CallChanged(ctx context.Context, conversationID, callID string) error {
	encoded, err := json.Marshal(CallMessage{
		Type:           "call",
		ConversationID: conversationID,
		CallID:         callID,
	})
	if err != nil {
		return fmt.Errorf("encode call broadcast: %w", err)
	}

	if err := b.client.Publish(ctx, entriesChannel(domain.ConversationID(conversationID)), encoded).Err(); err != nil {
		return fmt.Errorf("publish call change: %w", err)
	}
	return nil
}

func (b *RedisBroadcaster) BroadcastTyping(
	ctx context.Context,
	conversationID domain.ConversationID,
	accountID domain.AccountID,
	typing bool,
) error {
	encoded, err := json.Marshal(TypingMessage{
		Type:           "typing",
		ConversationID: string(conversationID),
		AccountID:      string(accountID),
		Typing:         typing,
	})
	if err != nil {
		return fmt.Errorf("encode typing broadcast: %w", err)
	}

	if err := b.client.Publish(ctx, entriesChannel(conversationID), encoded).Err(); err != nil {
		return fmt.Errorf("publish typing: %w", err)
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
