package identity

import (
	"context"
	"time"
)

// The ports below are declared by the domain and implemented in infrastructure.
//
// One repository per aggregate root, and each one loads and saves a whole
// aggregate. There is no CredentialRepository: a credential is an entity inside
// Account, and letting anything save one on its own would put the
// one-password invariant somewhere the aggregate cannot see it.
//
// The interfaces are split so a caller depends only on what it uses — the
// authentication path needs sessions and devices, and has no business being able
// to register accounts.

// AccountRepository stores Account aggregates, credentials included.
type AccountRepository interface {
	// Save persists an account and its credentials as one unit.
	//
	// Returns ErrHandleTaken or ErrEmailTaken when either is already in use, and
	// ErrPasswordAlreadySet when a second password would be stored — the
	// database enforces these too, because the aggregate cannot see a concurrent
	// request holding the same handle.
	Save(ctx context.Context, account *Account) error

	// ByHandle loads by normalised handle. Returns ErrAccountNotFound.
	ByHandle(ctx context.Context, normalisedHandle string) (*Account, error)

	// ByID returns ErrAccountNotFound if there is no such account.
	ByID(ctx context.Context, id AccountID) (*Account, error)
}

// DeviceRepository stores Device aggregates.
type DeviceRepository interface {
	Save(ctx context.Context, device *Device) error

	// ByID returns ErrDeviceNotFound if there is no such device.
	ByID(ctx context.Context, id DeviceID) (*Device, error)

	ForAccount(ctx context.Context, id AccountID) ([]*Device, error)
}

// SessionRepository stores Session aggregates.
type SessionRepository interface {
	// Save persists a session, replacing both digests atomically. This is what
	// makes rotation a single state transition rather than a delete and two
	// inserts that a crash can interrupt.
	Save(ctx context.Context, session *Session) error

	// ByAccessDigest returns ErrSessionNotFound if no session holds the digest.
	ByAccessDigest(ctx context.Context, digest []byte) (*Session, error)

	// ByRefreshDigest returns ErrSessionNotFound if no session holds the digest.
	ByRefreshDigest(ctx context.Context, digest []byte) (*Session, error)

	// DeleteForDevice removes every session belonging to a device.
	//
	// An optimisation, not the revocation mechanism. Authentication checks
	// whether the device is revoked on every request, so a failure here leaves
	// the system correct rather than leaving a revoked device usable.
	DeleteForDevice(ctx context.Context, id DeviceID) error

	// DeleteExpired removes sessions whose refresh token expired before the
	// given time. Reports how many went.
	DeleteExpired(ctx context.Context, before time.Time) (int64, error)
}

// Hasher turns a passphrase into storable material and checks it again later.
//
// A domain service: producing credential material is part of the model — it is
// what a password credential *is* — but it cannot be expressed without a
// cryptographic library, so the domain declares the operation and infrastructure
// supplies it.
type Hasher interface {
	// Hash requires a validated Passphrase, so an unacceptable secret cannot
	// reach storage down a path that forgot to check it.
	Hash(passphrase Passphrase) (material string, err error)

	// Verify takes the presented secret as a raw string rather than a Passphrase,
	// deliberately. Raising the minimum length later must not lock out accounts
	// whose credential was acceptable when they created it — a policy change is
	// not a reason to deny someone their own account.
	//
	// It returns an error only when material cannot be interpreted. A wrong
	// secret is (false, nil): being wrong is an expected outcome, not a fault.
	Verify(material, presented string) (bool, error)
}

// EventPublisher carries recorded domain events out of the context.
//
// Publishing is separate from saving in this phase, which means a crash between
// the two loses events. That is a known gap, not a design: phase 3 replaces the
// implementation with a transactional outbox writing event rows in the same
// transaction as the aggregate, which is the only way to make this reliable
// (see ADR-0003). The port does not change when it does.
type EventPublisher interface {
	Publish(ctx context.Context, events []Event) error
}

// IDs generates the identifiers the domain requires but does not produce.
type IDs interface {
	NewAccountID() AccountID
	NewCredentialID() CredentialID
	NewDeviceID() DeviceID
	NewSessionID() SessionID
}
