// Package domain models accounts, the credentials that prove control of them,
// and the devices and sessions that act on their behalf.
//
// It imports only the standard library, which is enforced by
// internal/arch/boundaries_test.go. Identifiers and timestamps therefore arrive
// from the caller: how an identifier is generated, and what the clock says, are
// not facts about the model.
//
// # Aggregates
//
// Three roots, each loaded and saved as a whole:
//
//   - Account, which contains its Credentials. A credential has no meaning or
//     lifetime apart from the account it proves, so it is an entity inside this
//     aggregate rather than a root of its own.
//   - Device, referenced by an account but with its own lifecycle — it is
//     registered and revoked independently, and revoking one must not require
//     loading the account.
//   - Session, which belongs to a device and rotates independently of it.
//
// Fields are unexported throughout. An aggregate whose fields can be assigned
// from outside cannot promise anything about its own state, and every invariant
// below would be a suggestion.
package domain

import (
	"net/mail"
	"strings"
	"time"
	"unicode"
)

// AccountID identifies an account. Other contexts refer to accounts by this
// value and never by a type owned here.
type AccountID string

// Handle is the unique public name by which an account is found and addressed.
// It is stored in the case its owner chose, and compared without case.
type Handle string

const (
	handleMinLength = 3
	handleMaxLength = 32
)

// ParseHandle validates and returns a handle.
//
// The rules are deliberately narrow. A handle is how one person identifies
// another, so anything that lets two handles look alike — mixed scripts, unicode
// lookalikes, leading or trailing punctuation — is an impersonation vector rather
// than a feature.
func ParseHandle(raw string) (Handle, error) {
	if len(raw) < handleMinLength {
		return "", ValidationError{"handle", "must be at least 3 characters"}
	}
	if len(raw) > handleMaxLength {
		return "", ValidationError{"handle", "must be at most 32 characters"}
	}

	for index, character := range raw {
		switch {
		case character >= 'a' && character <= 'z',
			character >= 'A' && character <= 'Z':
		case unicode.IsDigit(character) && character < unicode.MaxASCII:
			if index == 0 {
				return "", ValidationError{"handle", "must start with a letter"}
			}
		case character == '_' || character == '.':
			if index == 0 || index == len(raw)-1 {
				return "", ValidationError{"handle", "must not start or end with punctuation"}
			}
		default:
			return "", ValidationError{"handle", "may contain only letters, digits, underscore and dot"}
		}
	}

	return Handle(raw), nil
}

// Normalised returns the form used for uniqueness and lookup.
func (h Handle) Normalised() string {
	return strings.ToLower(string(h))
}

// Email is an account's recovery address. It is not an identity — accounts are
// identified by handle — and it is never shown to another account.
type Email string

// ParseEmail validates and returns an email address.
func ParseEmail(raw string) (Email, error) {
	address, err := mail.ParseAddress(raw)
	if err != nil {
		return "", ValidationError{"email", "is not a valid address"}
	}
	// mail.ParseAddress accepts `Name <a@b.c>`; storing that verbatim would then
	// fail to match itself on lookup.
	if address.Address != raw {
		return "", ValidationError{"email", "must be a bare address"}
	}
	return Email(raw), nil
}

// Normalised returns the form used for uniqueness and lookup.
func (e Email) Normalised() string {
	return strings.ToLower(string(e))
}

// Account is the aggregate root for a principal and its credentials.
type Account struct {
	recorder

	id          AccountID
	handle      Handle
	email       Email
	credentials []Credential
	createdAt   time.Time
}

// RegisterAccount creates an account with its first credential.
//
// An account and a way of authenticating are created together because an account
// nobody can log into is not a state worth being able to represent, even
// briefly. The credential is built here rather than passed in so that the
// one-password invariant holds from the first instant.
func RegisterAccount(
	accountID AccountID,
	credentialID CredentialID,
	handle, email string,
	kind CredentialKind,
	material string,
	now time.Time,
) (*Account, error) {
	if accountID == "" {
		return nil, ValidationError{"id", "must not be empty"}
	}
	parsedHandle, err := ParseHandle(handle)
	if err != nil {
		return nil, err
	}
	parsedEmail, err := ParseEmail(email)
	if err != nil {
		return nil, err
	}

	account := &Account{
		id:        accountID,
		handle:    parsedHandle,
		email:     parsedEmail,
		createdAt: now,
	}
	account.record(AccountRegistered{
		occurred:  occurred{now},
		AccountID: accountID,
		Handle:    parsedHandle,
	})

	if err := account.AddCredential(credentialID, kind, material, now); err != nil {
		return nil, err
	}

	return account, nil
}

// ReconstituteAccount rebuilds an account from storage.
//
// Separate from RegisterAccount because loading is not registering: no
// validation is re-run — the stored state was valid when it was written and
// rejecting it now would lock people out over a tightened rule — and no events
// are raised, since nothing has happened.
//
// Only a repository should call this.
func ReconstituteAccount(
	id AccountID,
	handle Handle,
	email Email,
	credentials []Credential,
	createdAt time.Time,
) *Account {
	return &Account{
		id:          id,
		handle:      handle,
		email:       email,
		credentials: credentials,
		createdAt:   createdAt,
	}
}

func (a *Account) ID() AccountID        { return a.id }
func (a *Account) Handle() Handle       { return a.handle }
func (a *Account) Email() Email         { return a.email }
func (a *Account) CreatedAt() time.Time { return a.createdAt }

// Credentials returns a copy, so a caller cannot reach past the aggregate and
// alter what it holds.
func (a *Account) Credentials() []Credential {
	return append([]Credential(nil), a.credentials...)
}

// AddCredential adds a way of proving control of this account.
//
// The one-password rule is enforced here, and again by a partial unique index in
// the database. Both are needed: the aggregate catches the ordinary case with a
// useful error, and the index catches two concurrent requests, which the
// aggregate cannot see.
func (a *Account) AddCredential(id CredentialID, kind CredentialKind, material string, now time.Time) error {
	credential, err := newCredential(id, a.id, kind, material, now)
	if err != nil {
		return err
	}

	if kind == KindPassword && a.hasCredential(KindPassword) {
		return ErrPasswordAlreadySet
	}

	a.credentials = append(a.credentials, credential)
	a.record(CredentialAdded{
		occurred:     occurred{now},
		AccountID:    a.id,
		CredentialID: id,
		Kind:         kind,
	})
	return nil
}

// PasswordCredential returns the account's password credential.
func (a *Account) PasswordCredential() (Credential, bool) {
	for _, credential := range a.credentials {
		if credential.Kind() == KindPassword {
			return credential, true
		}
	}
	return Credential{}, false
}

func (a *Account) hasCredential(kind CredentialKind) bool {
	for _, credential := range a.credentials {
		if credential.Kind() == kind {
			return true
		}
	}
	return false
}
