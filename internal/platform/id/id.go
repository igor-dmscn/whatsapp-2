// Package id generates identifiers.
//
// UUIDv7 rather than v4: the leading 48 bits are a millisecond timestamp, so
// generated identifiers sort by creation time. That keeps primary key index
// inserts at the right-hand edge of the B-tree instead of scattering them, which
// matters for tables that only ever grow — which is most of them here.
package id

import (
	"crypto/rand"
	"encoding/base64"

	"github.com/google/uuid"
)

// New returns a fresh UUIDv7 as a string.
//
// uuid.NewV7 fails only if the system entropy source fails. There is no useful
// way to continue without identifiers, and no caller could do anything with the
// error but stop, so this panics rather than propagating a case that cannot be
// handled.
func New() string {
	generated, err := uuid.NewV7()
	if err != nil {
		panic("generate uuid: " + err.Error())
	}
	return generated.String()
}

// Secret returns a URL-safe token with bytes of entropy from the system source.
//
// Distinct from New because the two answer different questions. An identifier needs
// to be unique and is fine to be guessable; a secret needs to be unguessable and its
// uniqueness is incidental. UUIDv7 is the wrong tool for the second — it embeds a
// timestamp, so a token created in a known minute has far less entropy than its
// length suggests.
//
// Panics on entropy failure, for the same reason New does: there is no way to
// continue without it and no caller could do anything but stop.
func Secret(bytes int) string {
	buffer := make([]byte, bytes)
	if _, err := rand.Read(buffer); err != nil {
		panic("generate secret: " + err.Error())
	}
	// Raw URL encoding: no padding to strip and safe in a path segment, which is
	// where an invite token ends up.
	return base64.RawURLEncoding.EncodeToString(buffer)
}
