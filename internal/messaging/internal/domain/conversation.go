// Package domain models conversations, who belongs to them, and the ordered log
// of what was said.
//
// It imports only the standard library. Identifiers, timestamps and payload bytes
// arrive from callers.
//
// # Aggregates
//
//   - Conversation — the log's identity and, critically, its sequence authority.
//     Appending is behaviour on this aggregate because gaplessness is its
//     invariant and nothing else can protect it.
//   - Membership — one account's participation. A separate root because a
//     channel's membership set is unbounded; see membership.go.
//   - Entry — one position in the log. A separate root because entries are
//     unbounded, but only ever created by Conversation.Append, which is what keeps
//     sequence assignment in one place.
//
// See ADR-0010 for the conventions these follow.
package domain

import (
	"sort"
	"strings"
	"time"
)

// ConversationID identifies a conversation.
type ConversationID string

// Kind is a conversation's shape. The three differ only in their membership and
// writing rules; the log beneath them is identical.
type Kind string

const (
	// KindDirect is between exactly two accounts, fixed at creation.
	KindDirect Kind = "direct"

	// KindGroup has mutable membership and every member may write.
	KindGroup Kind = "group"

	// KindChannel has a small set of writers and an unbounded set of readers.
	KindChannel Kind = "channel"
)

// DirectMemberCount is how many accounts a direct conversation has, always.
const DirectMemberCount = 2

// GroupMemberLimit caps group size (NF-13).
const GroupMemberLimit = 256

// Conversation is an aggregate root: the identity of a log, and the authority
// that assigns positions within it.
//
// It deliberately does not hold its memberships. What it holds is `head`, the
// highest position assigned so far, which is the state the gapless-sequence
// invariant is protected by. Concurrent appends are serialised by the repository
// holding a row lock while this aggregate assigns the next position, so nothing
// here needs to reason about contention.
type Conversation struct {
	recorder

	id        ConversationID
	kind      Kind
	head      Sequence
	createdAt time.Time
}

// StartDirect creates a direct conversation between two accounts.
func StartDirect(id ConversationID, now time.Time) (*Conversation, error) {
	return start(id, KindDirect, now)
}

// StartGroup creates a group conversation.
func StartGroup(id ConversationID, now time.Time) (*Conversation, error) {
	return start(id, KindGroup, now)
}

// StartChannel creates a broadcast channel.
func StartChannel(id ConversationID, now time.Time) (*Conversation, error) {
	return start(id, KindChannel, now)
}

func start(id ConversationID, kind Kind, now time.Time) (*Conversation, error) {
	if id == "" {
		return nil, ValidationError{"id", "must not be empty"}
	}

	conversation := &Conversation{id: id, kind: kind, createdAt: now}
	conversation.record(ConversationStarted{
		occurred:       occurred{now},
		ConversationID: id,
		Kind:           kind,
	})
	return conversation, nil
}

// ReconstituteConversation rebuilds a conversation from storage. Only a
// repository should call this.
func ReconstituteConversation(
	id ConversationID,
	kind Kind,
	head Sequence,
	createdAt time.Time,
) *Conversation {
	return &Conversation{id: id, kind: kind, head: head, createdAt: createdAt}
}

func (c *Conversation) ID() ConversationID   { return c.id }
func (c *Conversation) Kind() Kind           { return c.kind }
func (c *Conversation) Head() Sequence       { return c.head }
func (c *Conversation) CreatedAt() time.Time { return c.createdAt }

// Append adds an entry to the log and advances the head.
//
// This is the only way an entry comes into existence, which is what makes
// gaplessness enforceable: one aggregate assigns every position, and it does so
// by incrementing state it owns.
//
// The author's membership is passed in rather than looked up, because Membership
// is a separate root — see membership.go for why. Conversation asks the
// membership what it may do rather than inspecting its role, so the two
// aggregates stay independent.
func (c *Conversation) Append(
	entryID EntryID,
	author *Membership,
	clientEntryID ClientEntryID,
	payload Payload,
	now time.Time,
) (*Entry, error) {
	if author == nil {
		return nil, ErrNotAMember
	}
	if author.ConversationID() != c.id {
		// A membership of a different conversation authorises nothing here. This
		// is the check that stops a caller passing any membership it happens to
		// hold.
		return nil, ErrNotAMember
	}
	if !author.MayWrite() {
		return nil, ErrNotPermittedToWrite
	}

	sequence := c.head.Next()
	entry, err := newEntry(entryID, c.id, sequence, author.AccountID(), clientEntryID, KindMessage, payload, now)
	if err != nil {
		return nil, err
	}

	c.head = sequence
	c.record(EntryAppended{
		occurred:       occurred{now},
		ConversationID: c.id,
		EntryID:        entryID,
		Sequence:       sequence,
		AuthorID:       author.AccountID(),
		ContentType:    payload.ContentType(),
		Size:           payload.Size(),
	})

	return entry, nil
}

// AuthoriseJoin reports whether another account may be added.
//
// The direct-conversation rule lives here because it is about the conversation's
// shape, not about any one membership: a direct conversation has exactly two
// participants, forever, and adding a third would silently turn it into a group
// that two people believed was private.
func (c *Conversation) AuthoriseJoin(currentMemberCount int) error {
	switch c.kind {
	case KindDirect:
		if currentMemberCount >= DirectMemberCount {
			return ErrDirectConversationIsFull
		}
	case KindGroup:
		if currentMemberCount >= GroupMemberLimit {
			return ErrGroupIsFull
		}
	case KindChannel:
		// Readers are uncapped by design (NF-10).
	}
	return nil
}

// JoiningPosition is where a new membership's visibility begins.
//
// The entire history policy, in one method. Groups start at the head, so nothing
// said before someone arrived is visible to them; channels start at the first
// entry, because a broadcast with no back catalogue is useless to a new
// subscriber. Direct conversations never gain members, so the value is moot.
func (c *Conversation) JoiningPosition() Sequence {
	if c.kind == KindChannel {
		return FirstSequence
	}
	return c.head.Next()
}

// DefaultRole is what a newly joined account holds.
func (c *Conversation) DefaultRole() Role {
	if c.kind == KindChannel {
		return RoleReader
	}
	return RoleMember
}

// DirectKey is the canonical identity of a direct conversation between two
// accounts, independent of which one started it.
//
// Sorted so that (ana, bruno) and (bruno, ana) produce the same key, which lets a
// unique index guarantee one direct conversation per pair. Without it, both
// accounts messaging each other simultaneously creates two conversations and each
// sees half the history.
func DirectKey(first, second AccountID) string {
	pair := []string{string(first), string(second)}
	sort.Strings(pair)
	return strings.Join(pair, ":")
}
