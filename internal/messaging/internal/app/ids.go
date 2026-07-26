package app

import (
	"comms/internal/messaging/internal/domain"
	"comms/internal/platform/id"
)

// IDs generates messaging identifiers.
type IDs struct{}

var _ domain.IDs = IDs{}

func (IDs) NewConversationID() domain.ConversationID {
	return domain.ConversationID(id.New())
}

func (IDs) NewEntryID() domain.EntryID { return domain.EntryID(id.New()) }

func (IDs) NewInviteID() domain.InviteID { return domain.InviteID(id.New()) }

// NewInviteToken returns a URL-safe token with 256 bits of entropy.
//
// An invite is an unauthenticated credential: presenting it is sufficient to join.
// A UUID would be the obvious reach here and would be wrong — UUIDv7 embeds a
// timestamp and leaves only 74 random bits, so a link created in a known minute is
// meaningfully more guessable than it looks.
func (IDs) NewInviteToken() domain.InviteToken {
	return domain.InviteToken(id.Secret(32))
}
