package domain

import "time"

// AccountID references an account. Identity owns accounts; Messaging refers to
// them by identifier and holds no account data of its own.
type AccountID string

// Role is what a membership may do in its conversation.
type Role string

const (
	// RoleMember may read and write.
	RoleMember Role = "member"

	// RoleAdmin may read, write, and change membership.
	RoleAdmin Role = "admin"

	// RoleReader may read only. Every channel subscriber holds this.
	RoleReader Role = "reader"
)

// Membership is an aggregate root: an account's participation in a conversation.
//
// A root rather than an entity inside Conversation, and this is the most
// consequential boundary decision in the context. A channel's membership set is
// unbounded — fifty thousand readers by NF-10 — and an aggregate that cannot be
// loaded is not an aggregate. Keeping memberships inside Conversation would mean
// loading fifty thousand rows to append one entry.
//
// The cost of the split is that no single aggregate can enforce a rule spanning
// both. Conversation therefore takes a Membership as an argument when it needs to
// authorise something, rather than holding a reference to one.
type Membership struct {
	recorder

	conversationID ConversationID
	accountID      AccountID
	role           Role
	visibleFrom    Sequence
	joinedAt       time.Time
	leftAt         *time.Time
}

// Join creates a membership.
//
// visibleFrom is the whole of the history policy, and it is one number: passing
// the conversation's current head gives "no history before you arrived", and
// passing FirstSequence gives the full back catalogue. Groups use the former,
// channels the latter, and neither needs a branch anywhere else.
func Join(
	conversationID ConversationID,
	accountID AccountID,
	role Role,
	visibleFrom Sequence,
	now time.Time,
) (*Membership, error) {
	if conversationID == "" {
		return nil, ValidationError{"conversation_id", "must not be empty"}
	}
	if accountID == "" {
		return nil, ValidationError{"account_id", "must not be empty"}
	}
	switch role {
	case RoleMember, RoleAdmin, RoleReader:
	default:
		return nil, ValidationError{"role", "unknown role"}
	}
	if visibleFrom < 0 {
		return nil, ValidationError{"visible_from", "must not be negative"}
	}

	membership := &Membership{
		conversationID: conversationID,
		accountID:      accountID,
		role:           role,
		visibleFrom:    visibleFrom,
		joinedAt:       now,
	}
	membership.record(MemberJoined{
		occurred:       occurred{now},
		ConversationID: conversationID,
		AccountID:      accountID,
		Role:           role,
		VisibleFrom:    visibleFrom,
	})
	return membership, nil
}

// ReconstituteMembership rebuilds a membership from storage. Only a repository
// should call this.
func ReconstituteMembership(
	conversationID ConversationID,
	accountID AccountID,
	role Role,
	visibleFrom Sequence,
	joinedAt time.Time,
	leftAt *time.Time,
) *Membership {
	return &Membership{
		conversationID: conversationID,
		accountID:      accountID,
		role:           role,
		visibleFrom:    visibleFrom,
		joinedAt:       joinedAt,
		leftAt:         leftAt,
	}
}

func (m *Membership) ConversationID() ConversationID { return m.conversationID }
func (m *Membership) AccountID() AccountID           { return m.accountID }
func (m *Membership) Role() Role                     { return m.role }
func (m *Membership) VisibleFrom() Sequence          { return m.visibleFrom }
func (m *Membership) JoinedAt() time.Time            { return m.joinedAt }
func (m *Membership) LeftAt() *time.Time             { return m.leftAt }

// Active reports whether the membership currently participates.
func (m *Membership) Active() bool { return m.leftAt == nil }

// MayWrite reports whether this membership is permitted to append.
func (m *Membership) MayWrite() bool {
	return m.Active() && m.role != RoleReader
}

// MayAdminister reports whether this membership may change who belongs.
func (m *Membership) MayAdminister() bool {
	return m.Active() && m.role == RoleAdmin
}

// CanSee reports whether an entry at the given position is visible.
//
// Asked of the membership rather than compared by callers, so that every place
// needing the check gets the same answer — a comparison written by hand in the
// transport layer is how history leaks.
func (m *Membership) CanSee(sequence Sequence) bool {
	return sequence >= m.visibleFrom
}

// Leave marks the membership as no longer participating, and reports whether
// anything changed.
//
// Left rather than deleted: an entry's author must remain resolvable after they
// leave, and a conversation whose history refers to accounts with no membership
// row cannot be rendered.
func (m *Membership) Leave(now time.Time) (changed bool) {
	if m.leftAt != nil {
		return false
	}
	m.leftAt = &now
	m.record(MemberLeft{
		occurred:       occurred{now},
		ConversationID: m.conversationID,
		AccountID:      m.accountID,
	})
	return true
}
