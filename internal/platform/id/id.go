// Package id generates identifiers.
//
// UUIDv7 rather than v4: the leading 48 bits are a millisecond timestamp, so
// generated identifiers sort by creation time. That keeps primary key index
// inserts at the right-hand edge of the B-tree instead of scattering them, which
// matters for tables that only ever grow — which is most of them here.
package id

import "github.com/google/uuid"

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
