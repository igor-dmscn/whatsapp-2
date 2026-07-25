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
