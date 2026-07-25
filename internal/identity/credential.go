package identity

import "time"

// CredentialID identifies a credential.
type CredentialID string

// CredentialKind is how a credential proves control of an account.
//
// An account holds a set of credentials rather than a password field, so adding
// a kind is an insert and not a migration. That is why the type exists while only
// one kind is implemented.
type CredentialKind string

const (
	// KindPassword is an Argon2id digest of a passphrase.
	KindPassword CredentialKind = "password"

	// KindPasskey is a WebAuthn credential. Not yet implemented; named so that
	// the shape it must fit is visible rather than imagined.
	KindPasskey CredentialKind = "passkey"
)

// Credential is an entity inside the Account aggregate. It has identity, but no
// meaning or lifetime apart from the account it proves, so it is never loaded,
// saved or reasoned about on its own.
type Credential struct {
	id        CredentialID
	accountID AccountID
	kind      CredentialKind
	material  string
	createdAt time.Time
}

// newCredential is unexported: a credential may only come into being through
// Account.AddCredential, which is where the rules about which credentials may
// coexist are enforced.
func newCredential(id CredentialID, accountID AccountID, kind CredentialKind, material string, now time.Time) (Credential, error) {
	if id == "" {
		return Credential{}, ValidationError{"id", "must not be empty"}
	}
	if accountID == "" {
		return Credential{}, ValidationError{"account_id", "must not be empty"}
	}
	if kind != KindPassword && kind != KindPasskey {
		return Credential{}, ValidationError{"kind", "unknown credential kind"}
	}
	if material == "" {
		return Credential{}, ValidationError{"material", "must not be empty"}
	}

	return Credential{id: id, accountID: accountID, kind: kind, material: material, createdAt: now}, nil
}

// ReconstituteCredential rebuilds a credential from storage. Only a repository
// should call this.
func ReconstituteCredential(
	id CredentialID,
	accountID AccountID,
	kind CredentialKind,
	material string,
	createdAt time.Time,
) Credential {
	return Credential{id: id, accountID: accountID, kind: kind, material: material, createdAt: createdAt}
}

func (c Credential) ID() CredentialID     { return c.id }
func (c Credential) AccountID() AccountID { return c.accountID }
func (c Credential) Kind() CredentialKind { return c.kind }
func (c Credential) CreatedAt() time.Time { return c.createdAt }

// Material is what proves the credential: an Argon2id digest for a password, a
// public key for a passkey. Opaque to the domain — interpreting it belongs to
// whatever can verify it, which is not this layer.
func (c Credential) Material() string { return c.material }

// String keeps material out of logs. A struct with an unexported field would
// print it under %+v without this.
func (c Credential) String() string {
	return "Credential(" + string(c.id) + ", " + string(c.kind) + ")"
}
