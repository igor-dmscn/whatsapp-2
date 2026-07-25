package domain

// Passphrase is a validated secret, ready to be hashed into credential material.
//
// This lives in the domain rather than in application because what makes a
// credential acceptable is a rule about the model, not about orchestration. It is
// a value object and not a plain string so that it is impossible to hash an
// unvalidated one by accident: Hasher's caller must produce a Passphrase first.
type Passphrase struct {
	value string
}

const (
	// passphraseMinLength is a length floor and nothing else. Composition rules
	// ("one digit, one symbol") reliably produce weaker secrets than length
	// alone, because people satisfy them with predictable substitutions —
	// Password1! is compliant and worthless.
	passphraseMinLength = 12

	// passphraseMaxLength bounds the work an unauthenticated caller can ask for.
	// Argon2id has no input limit, so without this, hashing a multi-megabyte
	// body is a cheap way to make the server expensive.
	passphraseMaxLength = 1024
)

// ParsePassphrase validates a secret, or reports why it is not acceptable.
func ParsePassphrase(raw string) (Passphrase, error) {
	if len(raw) < passphraseMinLength {
		return Passphrase{}, ValidationError{"passphrase", "must be at least 12 characters"}
	}
	if len(raw) > passphraseMaxLength {
		return Passphrase{}, ValidationError{"passphrase", "must be at most 1024 characters"}
	}
	return Passphrase{value: raw}, nil
}

// Reveal returns the secret. Named to be conspicuous at call sites: there are
// exactly two legitimate reasons to reach for it, hashing and verifying, and
// anything else showing up in a review is a leak.
func (p Passphrase) Reveal() string {
	return p.value
}

// String hides the secret from accidental logging. Both String and GoString are
// implemented because %v and %#v take different paths, and a secret that survives
// one formatting verb is a secret in the logs.
func (p Passphrase) String() string   { return "[redacted]" }
func (p Passphrase) GoString() string { return "[redacted]" }
