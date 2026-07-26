package domain

import (
	"context"
	"time"
)

// ConversationRepository stores Conversation aggregates.
type ConversationRepository interface {
	// Start persists a new conversation together with its initial memberships.
	//
	// One method rather than two saves because a conversation with no members is
	// not a state worth being able to represent — and for direct conversations the
	// pair uniqueness check and the membership rows must land together or two
	// people messaging each other simultaneously end up with two conversations,
	// each holding half the history.
	//
	// Returns ErrAlreadyAMember if a direct conversation already exists for the
	// pair, with the existing conversation.
	Start(ctx context.Context, conversation *Conversation, memberships []*Membership) error

	// ByID returns ErrConversationNotFound if there is no such conversation.
	ByID(ctx context.Context, id ConversationID) (*Conversation, error)

	// DirectBetween finds the direct conversation for a pair of accounts.
	// Returns ErrConversationNotFound if they have none.
	DirectBetween(ctx context.Context, first, second AccountID) (*Conversation, error)

	// AppendEntry runs append inside a transaction holding the conversation's row
	// lock, and persists the resulting entry and advanced head together.
	//
	// Inverted into a callback so that loading, assigning a position, and saving
	// all happen under one lock. Concurrent writers to the same conversation queue
	// rather than collide — which is what ADR-0003 means by writes serialising per
	// conversation. The earlier design read the head, appended, and saved with a
	// version check; that fails every writer but one per round, so sixteen
	// concurrent senders need sixteen attempts. Serialising is both simpler and
	// correct under any amount of contention.
	//
	// The entry and the head advance must land together: an entry with no head
	// advance is invisible to sync, and a head advance with no entry leaves a
	// permanent gap every client requests forever.
	//
	// Events recorded by the aggregate are returned rather than published here, so
	// that phase 3 can write them to the outbox inside this same transaction
	// without changing the port (ADR-0003).
	AppendEntry(ctx context.Context, id ConversationID, append AppendFunc) (*Entry, []Event, error)
}

// AppendFunc appends to a conversation loaded under lock.
type AppendFunc func(*Conversation) (*Entry, error)

// MembershipRepository stores Membership aggregates.
type MembershipRepository interface {
	Save(ctx context.Context, membership *Membership) error

	// Of returns one account's membership of one conversation.
	// Returns ErrNotAMember if it has none.
	Of(ctx context.Context, conversationID ConversationID, accountID AccountID) (*Membership, error)

	// In returns every membership of a conversation.
	//
	// Only safe for direct conversations and groups, which are bounded. Channels
	// are not enumerated this way — see Recipients on the fan-out port.
	In(ctx context.Context, conversationID ConversationID) ([]*Membership, error)

	// ForAccount returns every conversation an account belongs to.
	ForAccount(ctx context.Context, accountID AccountID) ([]*Membership, error)

	// CountIn reports how many accounts belong to a conversation, without loading
	// them — which is what makes the group cap checkable for a conversation that
	// is too large to enumerate.
	CountIn(ctx context.Context, conversationID ConversationID) (int, error)
}

// EntryRepository reads entries. There is no write method: entries come into
// existence only through ConversationRepository.AppendEntry, because the
// conversation is what assigns their position.
type EntryRepository interface {
	// Range returns entries in a closed interval, in order, capped at limit.
	Range(ctx context.Context, conversationID ConversationID, from, to Sequence, limit int) ([]*Entry, error)

	// ByClientEntryID finds an entry an author already sent, which is how a retry
	// becomes a lookup instead of a duplicate (MS-2).
	// Returns ErrEntryNotFound if there is none.
	ByClientEntryID(ctx context.Context, conversationID ConversationID, authorID AccountID, clientEntryID ClientEntryID) (*Entry, error)
}

// EventPublisher carries recorded domain events out of the context.
//
// Phase 3 replaces the implementation with an outbox writing event rows in the
// same transaction as the aggregate (ADR-0003). AppendEntry already takes events
// for exactly that reason: the transaction boundary is in place before the outbox
// that will use it.
type EventPublisher interface {
	Publish(ctx context.Context, events []Event) error
}

// Broadcaster delivers an entry to whichever nodes hold connections that should
// see it.
//
// This is the ephemeral path of ADR-0005: at-most-once and allowed to fail. A
// dropped broadcast is recovered by the receiving client noticing a sequence gap,
// which it must do anyway for offline sync — one mechanism doing two jobs.
//
// Broadcasting is per conversation rather than per recipient so that the cost of a
// send does not scale with member count (NF-12). A channel broadcast to fifty
// thousand readers is one publish.
type Broadcaster interface {
	// BroadcastEntry announces an entry to a conversation's listeners.
	BroadcastEntry(ctx context.Context, entry *Entry) error

	// NotifyConversationStarted tells an account's listeners that it has been
	// added to a conversation, so their node can begin listening to it. Without
	// this a newly created conversation would deliver nothing live until the
	// recipient reconnected.
	NotifyConversationStarted(ctx context.Context, accountID AccountID, conversationID ConversationID) error
}

// IDs generates the identifiers the domain requires but does not produce.
type IDs interface {
	NewConversationID() ConversationID
	NewEntryID() EntryID
	NewInviteID() InviteID

	// NewInviteToken returns an unguessable token. In the domain's ports because
	// the domain requires the value and refuses a short one, but cannot produce
	// randomness without a library.
	NewInviteToken() InviteToken
}

// MemberState is the projected per-member view of a conversation (ADR-0002).
//
// Every field is eventually consistent. An entry can exist and be readable while
// this still says nothing is unread, and clients must render that window correctly
// rather than treating it as an error (NF-7).
type MemberState struct {
	ConversationID    ConversationID
	AccountID         AccountID
	ReadSequence      Sequence
	DeliveredSequence Sequence
	UnreadCount       int64
	UpdatedAt         time.Time
}

// DeliveryState is what MS-13 asks to be observable, from the sender's side.
type DeliveryState string

const (
	// DeliverySent means the entry is in the log and nothing more is known.
	DeliverySent DeliveryState = "sent"
	// DeliveryDelivered means every other member's device has taken it.
	DeliveryDelivered DeliveryState = "delivered"
	// DeliveryRead means every other member has read it.
	DeliveryRead DeliveryState = "read"
)

// MemberStateStore maintains the projection.
//
// Not a repository: it stores no aggregate and enforces no invariant. It is a read
// model, written only by the consumer that projects events and read only by queries
// that would otherwise have to aggregate across the whole log.
//
// Every method is idempotent. That is a requirement of the port, not an accident of
// the implementation — the consumer calling it is at-least-once (NF-8).
type MemberStateStore interface {
	// EnsureMember creates the row for a membership.
	EnsureMember(ctx context.Context, conversationID ConversationID, accountID AccountID, at time.Time) error

	// CountEntry increments the unread count of every member but the author.
	CountEntry(ctx context.Context, conversationID ConversationID, sequence Sequence, author AccountID, at time.Time) error

	// TouchAuthor advances the author's own marks to their own entry, because
	// sending a message is reading it.
	TouchAuthor(ctx context.Context, conversationID ConversationID, sequence Sequence, author AccountID, at time.Time) error

	// MarkRead advances a member's read mark and recomputes their unread count.
	MarkRead(ctx context.Context, conversationID ConversationID, accountID AccountID, through Sequence, at time.Time) error

	// MarkDelivered advances a member's delivery mark.
	MarkDelivered(ctx context.Context, conversationID ConversationID, accountID AccountID, through Sequence, at time.Time) error

	// ForAccount returns the state of every conversation an account belongs to.
	ForAccount(ctx context.Context, accountID AccountID) ([]MemberState, error)

	// Of returns one member's state, zero-valued if the projection has not caught up.
	Of(ctx context.Context, conversationID ConversationID, accountID AccountID) (MemberState, error)

	// Others returns the lowest read and delivery marks among the other members,
	// which is the position through which everyone else is up to date.
	Others(ctx context.Context, conversationID ConversationID, excluding AccountID) (readThrough Sequence, deliveredThrough Sequence, err error)
}

// Transactor runs work atomically.
//
// Declared because the application layer owns transaction boundaries (ADR-0010) and
// the transactional outbox depends on it: an event row must commit with the state
// change it describes, so the use case — not the repository — decides what "together"
// means.
type Transactor interface {
	InTransaction(ctx context.Context, work func(context.Context) error) error
}

// InviteRepository stores Invite aggregates.
type InviteRepository interface {
	Save(ctx context.Context, invite *Invite) error

	// ByToken loads an invite by its secret, returning ErrInviteNotFound if there
	// is none. The lookup is by token rather than by identifier because redeeming
	// is the only unauthenticated-ish path into a conversation, and the token is
	// all the redeemer has.
	ByToken(ctx context.Context, token InviteToken) (*Invite, error)

	// ByID loads an invite for its creator to revoke.
	ByID(ctx context.Context, id InviteID) (*Invite, error)

	// In lists a conversation's invites, so administrators can see and withdraw
	// links they have handed out.
	In(ctx context.Context, conversationID ConversationID) ([]*Invite, error)
}
