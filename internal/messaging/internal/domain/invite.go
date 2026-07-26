package domain

import "time"

// InviteID identifies an invite.
type InviteID string

// InviteToken is the secret in a shareable link.
//
// A distinct type from InviteID so the two cannot be swapped by mistake. The
// identifier is safe to log and appears in URLs the server builds for itself; the
// token is the credential, and anyone holding it can join.
type InviteToken string

// InviteTokenMinLength is the shortest token this model accepts.
//
// An invite is an unauthenticated credential — presenting it is sufficient to join
// — so it needs the entropy of one. The value is a floor the domain enforces; how
// the bytes are generated is infrastructure's business.
const InviteTokenMinLength = 32

// Invite is an aggregate root: permission to join a conversation, deferred.
//
// A root of its own rather than state on Conversation, because it has an independent
// lifetime — created, shared, redeemed repeatedly, revoked, expired — and because a
// conversation with ten thousand historical invites should not be an aggregate that
// must load them to append a message.
//
// It counts its uses rather than being consumed by the first: a group link shared in
// a mailing list is the ordinary case, and single-use is the special one, expressed
// by setting maxUses to one.
type Invite struct {
	recorder

	id             InviteID
	conversationID ConversationID
	token          InviteToken
	createdBy      AccountID
	role           Role
	createdAt      time.Time
	expiresAt      *time.Time
	// maxUses of zero means unlimited. Zero rather than a pointer because "no
	// limit" and "a limit of none" would otherwise be the same value, and a limit
	// of none is not a thing anybody wants.
	maxUses int
	uses    int
	revoked bool
}

// CreateInvite issues an invite.
//
// The role a redeemer receives is fixed when the invite is made rather than when it
// is redeemed. An invite whose grant could change after being shared is an invite
// nobody can reason about — and for channels it is what keeps a subscription link
// from quietly becoming a writer link.
func CreateInvite(
	id InviteID,
	conversationID ConversationID,
	token InviteToken,
	createdBy AccountID,
	role Role,
	maxUses int,
	expiresAt *time.Time,
	now time.Time,
) (*Invite, error) {
	if id == "" {
		return nil, ValidationError{"id", "must not be empty"}
	}
	if conversationID == "" {
		return nil, ValidationError{"conversation_id", "must not be empty"}
	}
	if len(token) < InviteTokenMinLength {
		return nil, ValidationError{"token", "is not long enough to be unguessable"}
	}
	if createdBy == "" {
		return nil, ValidationError{"created_by", "must not be empty"}
	}
	switch role {
	case RoleMember, RoleReader:
	case RoleAdmin:
		// Refused deliberately. An admin link is a link that hands out the power to
		// hand out power, and it would do so to whoever forwarded it furthest.
		return nil, ValidationError{"role", "an invite may not grant administration"}
	default:
		return nil, ValidationError{"role", "unknown role"}
	}
	if maxUses < 0 {
		return nil, ValidationError{"max_uses", "must not be negative"}
	}
	if expiresAt != nil && !expiresAt.After(now) {
		return nil, ValidationError{"expires_at", "must be in the future"}
	}

	invite := &Invite{
		id:             id,
		conversationID: conversationID,
		token:          token,
		createdBy:      createdBy,
		role:           role,
		createdAt:      now,
		expiresAt:      expiresAt,
		maxUses:        maxUses,
	}
	invite.record(InviteCreated{
		occurred:       occurred{now},
		ConversationID: conversationID,
		InviteID:       id,
		CreatedBy:      createdBy,
		Role:           role,
	})
	return invite, nil
}

// ReconstituteInvite rebuilds an invite from storage. Only a repository should call it.
func ReconstituteInvite(
	id InviteID,
	conversationID ConversationID,
	token InviteToken,
	createdBy AccountID,
	role Role,
	maxUses int,
	uses int,
	revoked bool,
	createdAt time.Time,
	expiresAt *time.Time,
) *Invite {
	return &Invite{
		id:             id,
		conversationID: conversationID,
		token:          token,
		createdBy:      createdBy,
		role:           role,
		createdAt:      createdAt,
		expiresAt:      expiresAt,
		maxUses:        maxUses,
		uses:           uses,
		revoked:        revoked,
	}
}

func (i *Invite) ID() InviteID                   { return i.id }
func (i *Invite) ConversationID() ConversationID { return i.conversationID }
func (i *Invite) CreatedBy() AccountID           { return i.createdBy }
func (i *Invite) Role() Role                     { return i.role }
func (i *Invite) MaxUses() int                   { return i.maxUses }
func (i *Invite) Uses() int                      { return i.uses }
func (i *Invite) Revoked() bool                  { return i.revoked }
func (i *Invite) CreatedAt() time.Time           { return i.createdAt }
func (i *Invite) ExpiresAt() *time.Time          { return i.expiresAt }

// Token returns the secret. Named plainly, but note the String methods below: it is
// deliberately hard to leak this by formatting the aggregate.
func (i *Invite) Token() InviteToken { return i.token }

// String and GoString keep the token out of logs. Both, because %v and %#v take
// different paths and a secret surviving one of them is a secret in the logs
// (ADR-0010).
func (i *Invite) String() string   { return "Invite(" + string(i.id) + ")" }
func (i *Invite) GoString() string { return "Invite(" + string(i.id) + ")" }

// Redeem consumes one use of the invite, or explains why it cannot be used.
//
// The distinct errors matter: an expired link and a revoked link mean different
// things to the person holding them, and telling them apart is the difference
// between "ask for a new link" and "you were not meant to have this".
func (i *Invite) Redeem(now time.Time) error {
	if i.revoked {
		return ErrInviteRevoked
	}
	if i.expiresAt != nil && !i.expiresAt.After(now) {
		return ErrInviteExpired
	}
	if i.maxUses > 0 && i.uses >= i.maxUses {
		return ErrInviteExhausted
	}

	i.uses++
	i.record(InviteRedeemed{
		occurred:       occurred{now},
		ConversationID: i.conversationID,
		InviteID:       i.id,
		Uses:           i.uses,
	})
	return nil
}

// Revoke withdraws the invite, reporting whether anything changed.
func (i *Invite) Revoke(now time.Time) (changed bool) {
	if i.revoked {
		return false
	}
	i.revoked = true
	i.record(InviteRevoked{
		occurred:       occurred{now},
		ConversationID: i.conversationID,
		InviteID:       i.id,
	})
	return true
}
