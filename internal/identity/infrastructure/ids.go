package infrastructure

import (
	"comms/internal/identity/domain"
	"comms/internal/platform/id"
)

// IDs generates identity identifiers. It exists so the domain can require
// identifiers without knowing how they are produced.
type IDs struct{}

var _ domain.IDs = IDs{}

func (IDs) NewAccountID() domain.AccountID       { return domain.AccountID(id.New()) }
func (IDs) NewCredentialID() domain.CredentialID { return domain.CredentialID(id.New()) }
func (IDs) NewDeviceID() domain.DeviceID         { return domain.DeviceID(id.New()) }
func (IDs) NewSessionID() domain.SessionID       { return domain.SessionID(id.New()) }
