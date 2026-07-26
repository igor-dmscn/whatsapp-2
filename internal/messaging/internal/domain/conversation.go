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
	replyTo Sequence,
	attachmentID AttachmentID,
	now time.Time,
) (*Entry, error) {
	if err := c.mayWrite(author); err != nil {
		return nil, err
	}
	if replyTo != 0 {
		// A reply is a reference field and nothing more (ADR-0008), but a reference
		// to a position that does not exist is not a reply — it is a client bug that
		// would render as a dangling quotation forever.
		if replyTo < FirstSequence || replyTo > c.head {
			return nil, ValidationError{"reply_to", "is not a position in this conversation"}
		}
		if !author.CanSee(replyTo) {
			// Quoting history you cannot read would leak it back into the
			// conversation for everybody, including yourself.
			return nil, ValidationError{"reply_to", "is before your join point"}
		}
	}

	sequence := c.head.Next()
	entry, err := newEntry(entryID, c.id, sequence, author.AccountID(), clientEntryID,
		KindMessage, payload, 0, replyTo, attachmentID, now)
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

// Revise appends an entry replacing an earlier one's content (MS-8).
//
// An append, not an update. Clients sync strictly forward, so editing entry 41 in
// place would be invisible to every client that had already passed it — which is the
// whole of ADR-0008 and the reason this returns a new entry with its own position.
//
// Author only. An edit that could be made by somebody else is a way to put words in
// their mouth, attributed to them, with no trace of who really wrote them.
func (c *Conversation) Revise(
	entryID EntryID,
	author *Membership,
	target *Entry,
	clientEntryID ClientEntryID,
	payload Payload,
	now time.Time,
) (*Entry, error) {
	if err := c.amendable(author, target); err != nil {
		return nil, err
	}
	if target.AuthorID() != author.AccountID() {
		return nil, ErrNotTheAuthor
	}
	return c.amend(entryID, author, target, clientEntryID, KindRevision, payload, now)
}

// Retract appends an entry withdrawing an earlier one's content — delete for
// everyone (MS-9).
//
// The author, or an administrator of the conversation. An administrator can remove
// somebody else's message but cannot change it: taking something down and putting
// different words in its place are not the same power, and only one of them is
// needed to moderate.
func (c *Conversation) Retract(
	entryID EntryID,
	actor *Membership,
	target *Entry,
	clientEntryID ClientEntryID,
	now time.Time,
) (*Entry, error) {
	if err := c.amendable(actor, target); err != nil {
		return nil, err
	}
	if target.AuthorID() != actor.AccountID() && !actor.MayAdminister() {
		return nil, ErrNotTheAuthor
	}

	// No payload. A retraction carries nothing, which is what makes it impossible for
	// a client to render withdrawn content by misreading the kind.
	return c.amend(entryID, actor, target, clientEntryID, KindRetraction, NoPayload(), now)
}

// amendable is the part both amendments share.
func (c *Conversation) amendable(actor *Membership, target *Entry) error {
	if err := c.mayWrite(actor); err != nil {
		return err
	}
	if target == nil {
		return ErrEntryNotFound
	}
	if target.ConversationID() != c.id {
		return ErrEntryNotFound
	}
	if target.Kind().Amends() {
		// Amendments target the original, never each other. A chain would make a
		// client resolve an arbitrary number of hops to render one message, and the
		// second edit of a message is naturally expressed as a second amendment of
		// the same original.
		return ErrCannotAmendAnAmendment
	}
	if !actor.CanSee(target.Sequence()) {
		return ErrEntryNotFound
	}
	return nil
}

func (c *Conversation) amend(
	entryID EntryID,
	actor *Membership,
	target *Entry,
	clientEntryID ClientEntryID,
	kind EntryKind,
	payload Payload,
	now time.Time,
) (*Entry, error) {
	sequence := c.head.Next()
	// No attachment: an amendment changes what an entry says, not what it carries.
	// Editing a photo's caption leaves the photo where it is, and retracting the entry
	// takes the caption away while the attachment stops being referenced.
	entry, err := newEntry(
		entryID, c.id, sequence, actor.AccountID(), clientEntryID, kind, payload,
		target.Sequence(), 0, "", now)
	if err != nil {
		return nil, err
	}

	c.head = sequence
	c.record(EntryAmended{
		occurred:       occurred{now},
		ConversationID: c.id,
		EntryID:        entryID,
		Sequence:       sequence,
		TargetSequence: target.Sequence(),
		AuthorID:       actor.AccountID(),
		Kind:           kind,
	})
	return entry, nil
}

// mayWrite is the membership check every append shares.
func (c *Conversation) mayWrite(author *Membership) error {
	if author == nil {
		return ErrNotAMember
	}
	if author.ConversationID() != c.id {
		// A membership of a different conversation authorises nothing here. This
		// is the check that stops a caller passing any membership it happens to
		// hold.
		return ErrNotAMember
	}
	if !author.MayWrite() {
		return ErrNotPermittedToWrite
	}
	return nil
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
