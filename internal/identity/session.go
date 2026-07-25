package identity

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"time"
)

// SessionID identifies a session.
type SessionID string

// Session lifetimes.
//
// Access tokens are resolved against stored state on every request, which is what
// lets ID-4 be met by changing that state rather than by waiting for expiry. The
// fifteen minutes therefore bounds the damage from a leaked token; it is not the
// revocation mechanism. See ADR-0009.
const (
	AccessTokenLifetime  = 15 * time.Minute
	RefreshTokenLifetime = 30 * 24 * time.Hour
)

// tokenEntropyBytes is 256 bits. Tokens are bearer secrets with no structure to
// fall back on, so there is no reason to be frugal.
const tokenEntropyBytes = 32

// Session is an aggregate root: one device's authenticated session, holding the
// access and refresh tokens currently valid for it.
//
// Making the pair one aggregate is what makes rotation atomic. Modelled as
// independent tokens, exchanging a refresh token means deleting one row and
// inserting two, and a crash between them destroys the session. Here it is one
// state transition on one aggregate, saved as one update.
//
// Only digests are held. The secrets exist once, in the response that issues
// them, and are thereafter only ever presented by the client — so a database
// dump yields nothing usable.
type Session struct {
	recorder

	id       SessionID
	deviceID DeviceID
	access   tokenPart
	refresh  tokenPart
}

// tokenPart is one half of a session's credentials.
type tokenPart struct {
	digest    []byte
	expiresAt time.Time
}

// Secrets are the values handed to a client. They are returned from behaviour
// rather than stored on the aggregate, so that there is no accessor anywhere that
// could hand a secret back out after the fact.
type Secrets struct {
	Access           string
	AccessExpiresAt  time.Time
	Refresh          string
	RefreshExpiresAt time.Time
}

// StartSession begins a session for a device.
func StartSession(id SessionID, deviceID DeviceID, now time.Time) (*Session, Secrets, error) {
	if id == "" {
		return nil, Secrets{}, ValidationError{"id", "must not be empty"}
	}
	if deviceID == "" {
		return nil, Secrets{}, ValidationError{"device_id", "must not be empty"}
	}

	session := &Session{id: id, deviceID: deviceID}
	secrets, err := session.issue(now)
	if err != nil {
		return nil, Secrets{}, err
	}

	session.record(SessionStarted{
		occurred:  occurred{now},
		SessionID: id,
		DeviceID:  deviceID,
	})
	return session, secrets, nil
}

// ReconstituteSession rebuilds a session from storage. Only a repository should
// call this.
func ReconstituteSession(
	id SessionID,
	deviceID DeviceID,
	accessDigest []byte,
	accessExpiresAt time.Time,
	refreshDigest []byte,
	refreshExpiresAt time.Time,
) *Session {
	return &Session{
		id:       id,
		deviceID: deviceID,
		access:   tokenPart{digest: accessDigest, expiresAt: accessExpiresAt},
		refresh:  tokenPart{digest: refreshDigest, expiresAt: refreshExpiresAt},
	}
}

func (s *Session) ID() SessionID               { return s.id }
func (s *Session) DeviceID() DeviceID          { return s.deviceID }
func (s *Session) AccessDigest() []byte        { return s.access.digest }
func (s *Session) AccessExpiresAt() time.Time  { return s.access.expiresAt }
func (s *Session) RefreshDigest() []byte       { return s.refresh.digest }
func (s *Session) RefreshExpiresAt() time.Time { return s.refresh.expiresAt }

// VerifyAccess reports whether the presented secret is this session's current
// access token and is still usable.
func (s *Session) VerifyAccess(presented string, now time.Time) error {
	if !matches(s.access.digest, presented) {
		return ErrInvalidToken
	}
	if !now.Before(s.access.expiresAt) {
		return ErrTokenExpired
	}
	return nil
}

// Rotate exchanges a refresh token for a new pair.
//
// The presented secret is checked against the refresh digest specifically, so an
// access token cannot be traded for one — the two are indistinguishable strings,
// and this check is the only thing stopping a fifteen-minute token becoming a
// thirty-day one.
//
// Rotation replaces both digests, so the presented token stops working the
// instant this succeeds. A stolen refresh token is usable at most once, and its
// use is detectable: the legitimate client's next rotation fails, which is an
// event rather than a silent thirty-day compromise.
func (s *Session) Rotate(presentedRefresh string, now time.Time) (Secrets, error) {
	if !matches(s.refresh.digest, presentedRefresh) {
		return Secrets{}, ErrInvalidToken
	}
	if !now.Before(s.refresh.expiresAt) {
		return Secrets{}, ErrTokenExpired
	}

	secrets, err := s.issue(now)
	if err != nil {
		return Secrets{}, err
	}

	s.record(SessionRotated{
		occurred:  occurred{now},
		SessionID: s.id,
		DeviceID:  s.deviceID,
	})
	return secrets, nil
}

// issue generates a fresh pair and installs it on the session.
func (s *Session) issue(now time.Time) (Secrets, error) {
	accessSecret, err := newSecret()
	if err != nil {
		return Secrets{}, err
	}
	refreshSecret, err := newSecret()
	if err != nil {
		return Secrets{}, err
	}

	s.access = tokenPart{digest: DigestToken(accessSecret), expiresAt: now.Add(AccessTokenLifetime)}
	s.refresh = tokenPart{digest: DigestToken(refreshSecret), expiresAt: now.Add(RefreshTokenLifetime)}

	return Secrets{
		Access:           accessSecret,
		AccessExpiresAt:  s.access.expiresAt,
		Refresh:          refreshSecret,
		RefreshExpiresAt: s.refresh.expiresAt,
	}, nil
}

// newSecret returns a fresh bearer secret.
func newSecret() (string, error) {
	raw := make([]byte, tokenEntropyBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

// DigestToken returns the stored form of a token secret.
//
// A plain SHA-256 rather than a password hash is correct and deliberate: the
// secret is 256 bits of uniformly random data, so there is no dictionary to
// attack and nothing for a slow hash to defend against. Argon2 here would add
// latency to every authenticated request for no gain.
func DigestToken(secret string) []byte {
	digest := sha256.Sum256([]byte(secret))
	return digest[:]
}

// matches compares a stored digest against a presented secret in constant time.
func matches(digest []byte, presented string) bool {
	return subtle.ConstantTimeCompare(digest, DigestToken(presented)) == 1
}

// String keeps digests and expiries out of logs.
func (s *Session) String() string {
	return "Session(" + string(s.id) + ")"
}
