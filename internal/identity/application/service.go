// Package application orchestrates identity use cases.
//
// It owns the things the domain deliberately does not know about: the clock,
// identifier generation, transaction boundaries, and publishing the events
// aggregates recorded. It owns no rules. Anything that would still be true if
// this system had no database and no HTTP belongs one layer down — and if a rule
// appears here, that is a sign the aggregate it belongs to is anemic.
package application

import (
	"context"
	"errors"
	"fmt"
	"time"

	"comms/internal/identity/domain"
)

// Clock is injected so expiry can be tested without sleeping.
type Clock func() time.Time

// Service carries out identity use cases.
type Service struct {
	accounts domain.AccountRepository
	devices  domain.DeviceRepository
	sessions domain.SessionRepository
	hasher   domain.Hasher
	events   domain.EventPublisher
	ids      domain.IDs
	now      Clock
}

// NewService wires a Service. Passing nil for now defaults to time.Now.
func NewService(
	accounts domain.AccountRepository,
	devices domain.DeviceRepository,
	sessions domain.SessionRepository,
	hasher domain.Hasher,
	events domain.EventPublisher,
	ids domain.IDs,
	now Clock,
) *Service {
	if now == nil {
		now = time.Now
	}
	return &Service{accounts, devices, sessions, hasher, events, ids, now}
}

// Session is what a client receives on register, login or refresh.
type Session struct {
	DeviceID         domain.DeviceID
	AccessToken      string
	AccessExpiresAt  time.Time
	RefreshToken     string
	RefreshExpiresAt time.Time
}

func newSession(deviceID domain.DeviceID, secrets domain.Secrets) Session {
	return Session{
		DeviceID:         deviceID,
		AccessToken:      secrets.Access,
		AccessExpiresAt:  secrets.AccessExpiresAt,
		RefreshToken:     secrets.Refresh,
		RefreshExpiresAt: secrets.RefreshExpiresAt,
	}
}

// Register creates an account with a password credential and signs in its first
// device.
//
// Registering returns a session rather than requiring an immediate login: a
// separate round trip proves nothing the registration did not already prove.
func (s *Service) Register(ctx context.Context, handle, email, passphrase, deviceName string) (*domain.Account, Session, error) {
	parsedPassphrase, err := domain.ParsePassphrase(passphrase)
	if err != nil {
		return nil, Session{}, err
	}

	material, err := s.hasher.Hash(parsedPassphrase)
	if err != nil {
		return nil, Session{}, fmt.Errorf("hash passphrase: %w", err)
	}

	now := s.now()
	account, err := domain.RegisterAccount(
		s.ids.NewAccountID(),
		s.ids.NewCredentialID(),
		handle, email,
		domain.KindPassword, material,
		now,
	)
	if err != nil {
		return nil, Session{}, err
	}

	if err := s.accounts.Save(ctx, account); err != nil {
		return nil, Session{}, fmt.Errorf("save account: %w", err)
	}
	s.publish(ctx, account.TakeEvents())

	session, err := s.startDeviceSession(ctx, account.ID(), deviceName, now)
	if err != nil {
		return nil, Session{}, err
	}

	return account, session, nil
}

// Login authenticates a passphrase and starts a session on a new device.
func (s *Service) Login(ctx context.Context, handle, passphrase, deviceName string) (*domain.Account, Session, error) {
	parsedHandle, err := domain.ParseHandle(handle)
	if err != nil {
		// A malformed handle cannot exist, so this is a failed login rather than a
		// validation error. Saying otherwise would let a caller learn which
		// handle shapes are possible.
		return nil, Session{}, domain.ErrInvalidCredential
	}

	account, err := s.accounts.ByHandle(ctx, parsedHandle.Normalised())
	if err != nil {
		if errors.Is(err, domain.ErrAccountNotFound) {
			// Deliberately indistinguishable from a wrong passphrase, so login
			// cannot be used to enumerate handles.
			return nil, Session{}, domain.ErrInvalidCredential
		}
		return nil, Session{}, fmt.Errorf("look up account: %w", err)
	}

	credential, found := account.PasswordCredential()
	if !found {
		return nil, Session{}, domain.ErrInvalidCredential
	}

	verified, err := s.hasher.Verify(credential.Material(), passphrase)
	if err != nil {
		return nil, Session{}, fmt.Errorf("verify passphrase: %w", err)
	}
	if !verified {
		return nil, Session{}, domain.ErrInvalidCredential
	}

	session, err := s.startDeviceSession(ctx, account.ID(), deviceName, s.now())
	if err != nil {
		return nil, Session{}, err
	}

	return account, session, nil
}

// Refresh exchanges a refresh token for a new pair.
//
// The rotation itself is one state transition on the Session aggregate, saved as
// one update — so a crash cannot leave a device with no usable tokens.
func (s *Service) Refresh(ctx context.Context, refreshToken string) (Session, error) {
	now := s.now()

	session, err := s.sessions.ByRefreshDigest(ctx, domain.DigestToken(refreshToken))
	if err != nil {
		if errors.Is(err, domain.ErrSessionNotFound) {
			return Session{}, domain.ErrInvalidToken
		}
		return Session{}, fmt.Errorf("look up session: %w", err)
	}

	device, err := s.devices.ByID(ctx, session.DeviceID())
	if err != nil {
		return Session{}, fmt.Errorf("look up device: %w", err)
	}
	if device.Revoked() {
		return Session{}, domain.ErrDeviceRevoked
	}

	secrets, err := session.Rotate(refreshToken, now)
	if err != nil {
		return Session{}, err
	}
	if err := s.sessions.Save(ctx, session); err != nil {
		return Session{}, fmt.Errorf("save session: %w", err)
	}
	s.publish(ctx, session.TakeEvents())

	return newSession(device.ID(), secrets), nil
}

// Authenticate resolves an access token to the account and device presenting it.
//
// Device revocation is checked here rather than inferred from session deletion,
// which is what makes ID-4 hold even when that deletion failed or raced.
func (s *Service) Authenticate(ctx context.Context, accessToken string) (domain.AccountID, domain.DeviceID, error) {
	session, err := s.sessions.ByAccessDigest(ctx, domain.DigestToken(accessToken))
	if err != nil {
		if errors.Is(err, domain.ErrSessionNotFound) {
			return "", "", domain.ErrInvalidToken
		}
		return "", "", fmt.Errorf("look up session: %w", err)
	}

	if err := session.VerifyAccess(accessToken, s.now()); err != nil {
		return "", "", err
	}

	device, err := s.devices.ByID(ctx, session.DeviceID())
	if err != nil {
		return "", "", fmt.Errorf("look up device: %w", err)
	}
	if device.Revoked() {
		return "", "", domain.ErrDeviceRevoked
	}

	return device.AccountID(), device.ID(), nil
}

// RevokeDevice stops a device acting for its account and drops its sessions.
func (s *Service) RevokeDevice(ctx context.Context, accountID domain.AccountID, deviceID domain.DeviceID) error {
	device, err := s.devices.ByID(ctx, deviceID)
	if err != nil {
		return fmt.Errorf("look up device: %w", err)
	}
	if !device.BelongsTo(accountID) {
		// Reported as absent rather than forbidden: an account has no business
		// learning that another account's device exists.
		return domain.ErrDeviceNotFound
	}

	if !device.Revoke(s.now()) {
		// Already revoked. Nothing changed, so nothing is saved and no second
		// event is published.
		return nil
	}

	if err := s.devices.Save(ctx, device); err != nil {
		return fmt.Errorf("save device: %w", err)
	}
	if err := s.sessions.DeleteForDevice(ctx, device.ID()); err != nil {
		return fmt.Errorf("delete device sessions: %w", err)
	}
	s.publish(ctx, device.TakeEvents())
	return nil
}

// Devices lists an account's devices, revoked ones included — a device that
// vanishes on revocation is a device its owner cannot audit.
func (s *Service) Devices(ctx context.Context, accountID domain.AccountID) ([]*domain.Device, error) {
	devices, err := s.devices.ForAccount(ctx, accountID)
	if err != nil {
		return nil, fmt.Errorf("list devices: %w", err)
	}
	return devices, nil
}

// LookupHandle finds an account by exact handle. Discovery is exact-match only:
// prefix search over handles is an enumeration tool (ID-5).
func (s *Service) LookupHandle(ctx context.Context, handle string) (*domain.Account, error) {
	parsed, err := domain.ParseHandle(handle)
	if err != nil {
		return nil, domain.ErrAccountNotFound
	}

	account, err := s.accounts.ByHandle(ctx, parsed.Normalised())
	if err != nil {
		if errors.Is(err, domain.ErrAccountNotFound) {
			return nil, domain.ErrAccountNotFound
		}
		return nil, fmt.Errorf("look up account: %w", err)
	}
	return account, nil
}

// LookupByID returns an account by identifier.
func (s *Service) LookupByID(ctx context.Context, accountID domain.AccountID) (*domain.Account, error) {
	account, err := s.accounts.ByID(ctx, accountID)
	if err != nil {
		if errors.Is(err, domain.ErrAccountNotFound) {
			return nil, domain.ErrAccountNotFound
		}
		return nil, fmt.Errorf("look up account: %w", err)
	}
	return account, nil
}

// AddCredential adds a way of proving control of an account.
//
// Passphrases are validated and hashed here; other kinds carry material the
// caller already holds. The rule about which credentials may coexist lives on the
// aggregate, not here.
func (s *Service) AddCredential(ctx context.Context, accountID domain.AccountID, kind domain.CredentialKind, secret string) error {
	account, err := s.accounts.ByID(ctx, accountID)
	if err != nil {
		return fmt.Errorf("look up account: %w", err)
	}

	material := secret
	if kind == domain.KindPassword {
		parsed, err := domain.ParsePassphrase(secret)
		if err != nil {
			return err
		}
		if material, err = s.hasher.Hash(parsed); err != nil {
			return fmt.Errorf("hash passphrase: %w", err)
		}
	}

	if err := account.AddCredential(s.ids.NewCredentialID(), kind, material, s.now()); err != nil {
		return err
	}
	if err := s.accounts.Save(ctx, account); err != nil {
		return fmt.Errorf("save account: %w", err)
	}
	s.publish(ctx, account.TakeEvents())
	return nil
}

// PurgeExpiredSessions deletes sessions that can no longer be refreshed. Expiry
// is enforced on read, so this only reclaims space and may run whenever.
func (s *Service) PurgeExpiredSessions(ctx context.Context) (int64, error) {
	deleted, err := s.sessions.DeleteExpired(ctx, s.now())
	if err != nil {
		return 0, fmt.Errorf("purge expired sessions: %w", err)
	}
	return deleted, nil
}

// startDeviceSession registers a device and opens its first session.
func (s *Service) startDeviceSession(ctx context.Context, accountID domain.AccountID, deviceName string, now time.Time) (Session, error) {
	if deviceName == "" {
		deviceName = "unnamed device"
	}

	device, err := domain.RegisterDevice(s.ids.NewDeviceID(), accountID, deviceName, now)
	if err != nil {
		return Session{}, err
	}
	if err := s.devices.Save(ctx, device); err != nil {
		return Session{}, fmt.Errorf("save device: %w", err)
	}
	s.publish(ctx, device.TakeEvents())

	session, secrets, err := domain.StartSession(s.ids.NewSessionID(), device.ID(), now)
	if err != nil {
		return Session{}, err
	}
	if err := s.sessions.Save(ctx, session); err != nil {
		return Session{}, fmt.Errorf("save session: %w", err)
	}
	s.publish(ctx, session.TakeEvents())

	return newSession(device.ID(), secrets), nil
}

// publish hands recorded events to the publisher.
//
// A publish failure does not fail the use case: the state change is already
// committed, and refusing a successful registration because a log line could not
// be written would be worse than a missing event. Phase 3 removes the choice by
// writing events in the same transaction as the aggregate.
func (s *Service) publish(ctx context.Context, events []domain.Event) {
	if len(events) == 0 {
		return
	}
	_ = s.events.Publish(ctx, events)
}
