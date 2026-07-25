package messagingapp

import (
	"comms/internal/messaging"
	"comms/internal/platform/id"
)

// IDs generates messaging identifiers.
type IDs struct{}

var _ messaging.IDs = IDs{}

func (IDs) NewConversationID() messaging.ConversationID {
	return messaging.ConversationID(id.New())
}

func (IDs) NewEntryID() messaging.EntryID { return messaging.EntryID(id.New()) }
