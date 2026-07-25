package domain

import "time"

// EntryID identifies an entry.
type EntryID string

// EntryKind distinguishes what occupies a position in the log.
//
// Two kinds exist because ADR-0008 makes edits and deletes new entries rather
// than mutations: clients sync strictly forward, so editing entry 41 in place
// would be invisible to every client that had already passed it.
type EntryKind string

const (
	// KindMessage carries a payload from its author.
	KindMessage EntryKind = "message"

	// KindRevision amends or retracts an earlier entry. Created in phase 5; named
	// here so that clients written now must already tolerate an unfamiliar kind
	// rather than assuming every entry is renderable.
	KindRevision EntryKind = "revision"
)

// Entry is an aggregate root: one position in a conversation's log.
//
// Created only by Conversation.Append, which is why newEntry is unexported —
// sequence assignment belongs to the conversation, and an entry that could be
// constructed independently could be given any position.
//
// Entries are never modified once written. There is no behaviour on this type
// that changes it, and that absence is deliberate rather than unfinished.
type Entry struct {
	id             EntryID
	conversationID ConversationID
	sequence       Sequence
	authorID       AccountID
	clientEntryID  ClientEntryID
	kind           EntryKind
	payload        Payload
	createdAt      time.Time
}

func newEntry(
	id EntryID,
	conversationID ConversationID,
	sequence Sequence,
	authorID AccountID,
	clientEntryID ClientEntryID,
	kind EntryKind,
	payload Payload,
	now time.Time,
) (*Entry, error) {
	if id == "" {
		return nil, ValidationError{"id", "must not be empty"}
	}
	if sequence < FirstSequence {
		return nil, ValidationError{"sequence", "must be at least one"}
	}
	if authorID == "" {
		return nil, ValidationError{"author_id", "must not be empty"}
	}
	if clientEntryID == "" {
		return nil, ValidationError{"client_entry_id", "must not be empty"}
	}

	return &Entry{
		id:             id,
		conversationID: conversationID,
		sequence:       sequence,
		authorID:       authorID,
		clientEntryID:  clientEntryID,
		kind:           kind,
		payload:        payload,
		createdAt:      now,
	}, nil
}

// ReconstituteEntry rebuilds an entry from storage. Only a repository should call
// this.
func ReconstituteEntry(
	id EntryID,
	conversationID ConversationID,
	sequence Sequence,
	authorID AccountID,
	clientEntryID ClientEntryID,
	kind EntryKind,
	payload Payload,
	createdAt time.Time,
) *Entry {
	return &Entry{
		id:             id,
		conversationID: conversationID,
		sequence:       sequence,
		authorID:       authorID,
		clientEntryID:  clientEntryID,
		kind:           kind,
		payload:        payload,
		createdAt:      createdAt,
	}
}

func (e *Entry) ID() EntryID                    { return e.id }
func (e *Entry) ConversationID() ConversationID { return e.conversationID }
func (e *Entry) Sequence() Sequence             { return e.sequence }
func (e *Entry) AuthorID() AccountID            { return e.authorID }
func (e *Entry) ClientEntryID() ClientEntryID   { return e.clientEntryID }
func (e *Entry) Kind() EntryKind                { return e.kind }
func (e *Entry) Payload() Payload               { return e.payload }
func (e *Entry) CreatedAt() time.Time           { return e.createdAt }

// String reports position and shape, never content.
func (e *Entry) String() string {
	return "Entry(" + string(e.conversationID) + "#" + itoa(int(e.sequence)) + ")"
}

// Gap is a stretch of positions a client is missing.
//
// Both bounds are inclusive. Gaps are computed rather than guessed because
// sequences are gapless: a client holding up to From-1 and told the head is To
// knows exactly what to request, with no "everything since roughly this time"
// approximation and no way to silently skip an entry (MS-3).
type Gap struct {
	ConversationID ConversationID
	From           Sequence
	To             Sequence
}

// Count is how many entries the gap covers.
func (g Gap) Count() int64 {
	if g.To < g.From {
		return 0
	}
	return int64(g.To-g.From) + 1
}

// GapFor returns what a membership is missing, given where it has read up to and
// where the log now ends.
//
// Returns false when there is nothing to fetch. Visibility is applied here, so a
// membership that joined at position 40 is never offered entries 1 to 39 — the
// history policy holds even when a client asks for everything.
func GapFor(membership *Membership, head Sequence, clientHas Sequence) (Gap, bool) {
	from := clientHas.Next()
	if from < membership.VisibleFrom() {
		from = membership.VisibleFrom()
	}
	if from > head {
		return Gap{}, false
	}
	return Gap{ConversationID: membership.ConversationID(), From: from, To: head}, true
}
