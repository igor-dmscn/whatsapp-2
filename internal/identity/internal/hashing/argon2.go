package hashing

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"golang.org/x/crypto/argon2"

	"comms/internal/identity/internal/domain"
)

// Argon2Params are the cost parameters of an Argon2id hash.
//
// The defaults follow the RFC 9106 second recommended option: 64 MiB of memory,
// three passes, four lanes. Memory is the parameter that matters — it is what
// makes purpose-built cracking hardware expensive, in a way that iteration count
// alone does not.
type Argon2Params struct {
	Memory      uint32 // KiB
	Iterations  uint32
	Parallelism uint8
	SaltLength  uint32
	KeyLength   uint32
}

// DefaultArgon2Params returns the parameters used for new credentials.
func DefaultArgon2Params() Argon2Params {
	return Argon2Params{
		Memory:      64 * 1024,
		Iterations:  3,
		Parallelism: 4,
		SaltLength:  16,
		KeyLength:   32,
	}
}

// Argon2Hasher hashes passphrases with Argon2id.
//
// Parameters are encoded into the stored material rather than held in config, so
// that raising the cost later does not invalidate existing credentials: an old
// digest still carries the parameters needed to verify it.
type Argon2Hasher struct {
	params Argon2Params
}

// NewArgon2Hasher returns a hasher using the given parameters.
func NewArgon2Hasher(params Argon2Params) *Argon2Hasher {
	return &Argon2Hasher{params: params}
}

var _ domain.Hasher = (*Argon2Hasher)(nil)

// Hash returns material in the standard PHC string format.
func (h *Argon2Hasher) Hash(passphrase domain.Passphrase) (string, error) {
	salt := make([]byte, h.params.SaltLength)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("generate salt: %w", err)
	}

	key := argon2.IDKey(
		[]byte(passphrase.Reveal()),
		salt,
		h.params.Iterations,
		h.params.Memory,
		h.params.Parallelism,
		h.params.KeyLength,
	)

	return fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2.Version,
		h.params.Memory,
		h.params.Iterations,
		h.params.Parallelism,
		base64.RawStdEncoding.EncodeToString(salt),
		base64.RawStdEncoding.EncodeToString(key),
	), nil
}

// ErrUnsupportedHash is returned when stored material cannot be interpreted.
var ErrUnsupportedHash = errors.New("unsupported hash format")

// Verify reports whether presented produced material, using the parameters
// recorded in material rather than the hasher's current ones.
func (h *Argon2Hasher) Verify(material, presented string) (bool, error) {
	parts := strings.Split(material, "$")
	if len(parts) != 6 || parts[1] != "argon2id" {
		return false, ErrUnsupportedHash
	}

	var version int
	if _, err := fmt.Sscanf(parts[2], "v=%d", &version); err != nil {
		return false, fmt.Errorf("parse version: %w", err)
	}
	if version != argon2.Version {
		return false, ErrUnsupportedHash
	}

	var memory, iterations uint32
	var parallelism uint8
	if _, err := fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d", &memory, &iterations, &parallelism); err != nil {
		return false, fmt.Errorf("parse parameters: %w", err)
	}

	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil {
		return false, fmt.Errorf("decode salt: %w", err)
	}
	expected, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil {
		return false, fmt.Errorf("decode key: %w", err)
	}

	actual := argon2.IDKey([]byte(presented), salt, iterations, memory, parallelism, uint32(len(expected)))

	return subtle.ConstantTimeCompare(expected, actual) == 1, nil
}
