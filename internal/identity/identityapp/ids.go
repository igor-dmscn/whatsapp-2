package identityapp

import (
	"comms/internal/identity"
	"comms/internal/platform/id"
)

// IDs generates identity identifiers. It exists so the domain can require
// identifiers without knowing how they are produced.
type IDs struct{}

var _ identity.IDs = IDs{}

func (IDs) NewAccountID() identity.AccountID       { return identity.AccountID(id.New()) }
func (IDs) NewCredentialID() identity.CredentialID { return identity.CredentialID(id.New()) }
func (IDs) NewDeviceID() identity.DeviceID         { return identity.DeviceID(id.New()) }
func (IDs) NewSessionID() identity.SessionID       { return identity.SessionID(id.New()) }
