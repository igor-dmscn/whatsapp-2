package main

import (
	"context"

	"comms/internal/identity"
	"comms/internal/identity/identityapi"
)

// This file holds the adapters that let one context satisfy another's port.
//
// It lives in cmd/api because that is the only place allowed to know about more
// than one context (ADR-0007). Messaging declares what it needs — an Authenticator,
// a CallerResolver — in its own package, using its own types. Identity happens to
// be able to provide both. Neither imports the other; the join is made here, and
// the architecture test enforces that it is made nowhere else.

// identityAuthenticator adapts Identity's service to Messaging's Authenticator.
//
// The signature is deliberately in plain strings rather than either context's
// identifier types. A shared type would be a shared dependency, which is the thing
// the boundary exists to prevent.
type identityAuthenticator struct {
	authenticate func(ctx context.Context, accessToken string) (identity.AccountID, identity.DeviceID, error)
}

func (a identityAuthenticator) Authenticate(ctx context.Context, accessToken string) (string, string, error) {
	accountID, deviceID, err := a.authenticate(ctx, accessToken)
	if err != nil {
		return "", "", err //nolint:wrapcheck // the caller distinguishes nothing; it rejects.
	}
	return string(accountID), string(deviceID), nil
}

// callerFromRequest adapts Identity's request-scoped caller to Messaging's
// CallerResolver. Both contexts' HTTP surfaces are wrapped by the same
// authentication middleware, so the value is already on the context by the time a
// messaging handler runs.
func callerFromRequest(ctx context.Context) (string, string) {
	accountID, deviceID := identityapi.Caller(ctx)
	return string(accountID), string(deviceID)
}
