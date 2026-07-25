package identity

import "time"

// DeviceID identifies a device.
type DeviceID string

// Device is an aggregate root: one client installation acting for an account.
//
// It is a root rather than an entity inside Account because its lifecycle is its
// own — revoking a device from a phone that has been lost should not require
// loading the account and all of its credentials.
//
// Devices hold no read state. A cursor belongs to a membership, so reading on one
// device is reading on all of them — see the Messaging glossary.
type Device struct {
	recorder

	id        DeviceID
	accountID AccountID
	name      string
	createdAt time.Time
	revokedAt *time.Time
}

// RegisterDevice creates a device acting for an account.
func RegisterDevice(id DeviceID, accountID AccountID, name string, now time.Time) (*Device, error) {
	if id == "" {
		return nil, ValidationError{"id", "must not be empty"}
	}
	if accountID == "" {
		return nil, ValidationError{"account_id", "must not be empty"}
	}
	if name == "" {
		return nil, ValidationError{"name", "must not be empty"}
	}
	if len(name) > 64 {
		return nil, ValidationError{"name", "must be at most 64 characters"}
	}

	device := &Device{id: id, accountID: accountID, name: name, createdAt: now}
	device.record(DeviceRegistered{
		occurred:  occurred{now},
		AccountID: accountID,
		DeviceID:  id,
		Name:      name,
	})
	return device, nil
}

// ReconstituteDevice rebuilds a device from storage. Only a repository should
// call this.
func ReconstituteDevice(
	id DeviceID,
	accountID AccountID,
	name string,
	createdAt time.Time,
	revokedAt *time.Time,
) *Device {
	return &Device{id: id, accountID: accountID, name: name, createdAt: createdAt, revokedAt: revokedAt}
}

func (d *Device) ID() DeviceID          { return d.id }
func (d *Device) AccountID() AccountID  { return d.accountID }
func (d *Device) Name() string          { return d.name }
func (d *Device) CreatedAt() time.Time  { return d.createdAt }
func (d *Device) RevokedAt() *time.Time { return d.revokedAt }

// Revoked reports whether the device may no longer act for its account.
func (d *Device) Revoked() bool {
	return d.revokedAt != nil
}

// BelongsTo reports whether the device acts for the given account.
//
// Ownership is asked of the aggregate rather than compared by a caller, so that
// every place needing the check gets the same answer.
func (d *Device) BelongsTo(accountID AccountID) bool {
	return d.accountID == accountID
}

// Revoke stops the device acting, and reports whether anything changed.
//
// Revoking an already-revoked device keeps the original time and raises no second
// event: when a device stopped being trusted is a fact, and consumers should not
// see it happen twice.
func (d *Device) Revoke(now time.Time) (changed bool) {
	if d.revokedAt != nil {
		return false
	}
	d.revokedAt = &now
	d.record(DeviceRevoked{
		occurred:  occurred{now},
		AccountID: d.accountID,
		DeviceID:  d.id,
	})
	return true
}
