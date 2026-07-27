package domain

import "time"

// EntryID identifies an entry.
type EntryID string

// AttachmentID references a photo or video held by Media.
//
// An identifier and nothing else. Messaging stores no attachment metadata, has no
// foreign key to Media's tables, and cannot tell whether the attachment is ready — a
// message referencing one is readable the instant it is sent, which is the point
// (MD-1). See CONTEXT-MAP.md.
type AttachmentID string

// EntryKind distinguishes what occupies a position in the log.
//
// Two kinds exist because ADR-0008 makes edits and deletes new entries rather
// than mutations: clients sync strictly forward, so editing entry 41 in place
// would be invisible to every client that had already passed it.
type EntryKind string

const (
	// KindMessage carries a payload from its author.
	KindMessage EntryKind = "message"

	// KindRevision replaces an earlier entry's content with its own.
	KindRevision EntryKind = "revision"

	// KindRetraction withdraws an earlier entry's content — delete for everyone.
	//
	// A separate kind from KindRevision rather than a revision with an empty
	// payload. "Empty means deleted" is an encoding a client can get wrong in a way
	// that shows withdrawn content, and the two are different acts with different
	// permissions (MS-9).
	KindRetraction EntryKind = "retraction"
)

// Amends reports whether this kind refers to an earlier entry.
func (k EntryKind) Amends() bool {
	return k == KindRevision || k == KindRetraction
}

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
	// target is the position this entry amends, zero for an ordinary message.
	target Sequence
	// replyTo is the position this entry replies to, zero for none.
	replyTo Sequence
	// attachmentID is the photo or video this entry carries, empty for none.
	attachmentID AttachmentID
	createdAt    time.Time
}

func newEntry(
	id EntryID,
	conversationID ConversationID,
	sequence Sequence,
	authorID AccountID,
	clientEntryID ClientEntryID,
	kind EntryKind,
	payload Payload,
	target Sequence,
	replyTo Sequence,
	attachmentID AttachmentID,
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
	if kind.Amends() && target < FirstSequence {
		return nil, ValidationError{"target_sequence", "must name the entry being amended"}
	}
	if !kind.Amends() && target != 0 {
		return nil, ValidationError{"target_sequence", "only a revision or retraction amends an entry"}
	}
	if payload.Empty() && attachmentID == "" && kind != KindRetraction {
		// An entry with neither content nor an attachment says nothing and occupies a
		// position. Only a retraction is legitimately empty — it means "what was here is
		// withdrawn", which is content of a sort.
		return nil, ValidationError{"body", "an entry must carry content or an attachment"}
	}

	return &Entry{
		id:             id,
		conversationID: conversationID,
		sequence:       sequence,
		authorID:       authorID,
		clientEntryID:  clientEntryID,
		kind:           kind,
		payload:        payload,
		target:         target,
		replyTo:        replyTo,
		attachmentID:   attachmentID,
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
	target Sequence,
	replyTo Sequence,
	attachmentID AttachmentID,
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
		target:         target,
		replyTo:        replyTo,
		attachmentID:   attachmentID,
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

// Target is the position this entry amends, or zero if it amends nothing.
func (e *Entry) Target() Sequence { return e.target }

// ReplyTo is the position this entry replies to, or zero for none.
func (e *Entry) ReplyTo() Sequence { return e.replyTo }

// AttachmentID is the photo or video this entry carries, or empty for none.
func (e *Entry) AttachmentID() AttachmentID { return e.attachmentID }

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
	return GapBetween(membership.ConversationID(), membership.VisibleFrom(), head, clientHas)
}

// GapBetween is GapFor over the two values it actually reads.
//
// For callers holding a summary rather than a membership: the conversation list and a socket
// resume answer this from the read model, and loading an aggregate per conversation to reach two
// numbers is the cost ADR-0002 exists to avoid. One implementation either way, so the two paths
// cannot drift on where a joiner's history begins.
func GapBetween(conversationID ConversationID, visibleFrom, head, clientHas Sequence) (Gap, bool) {
	from := clientHas.Next()
	if from < visibleFrom {
		from = visibleFrom
	}
	if from > head {
		return Gap{}, false
	}
	return Gap{ConversationID: conversationID, From: from, To: head}, true
}
