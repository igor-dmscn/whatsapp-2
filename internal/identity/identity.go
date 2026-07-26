// Package identity is the whole of the Identity context's public surface.
//
// Everything else — the model, the use cases, the Postgres adapters, the HTTP
// handlers — lives under internal/, which the Go compiler makes unreachable from
// outside this directory tree. That is not a convention this repository asks people
// to respect; it is a build error. Nothing outside Identity can depend on an
// Identity aggregate, a repository, or a table, because nothing outside Identity
// can name them.
//
// What that buys, beyond encapsulation: the layer packages can be called domain,
// app, postgres and api rather than identitydomain, identityapp and so on. Two
// contexts may both have a package called domain and never collide, because no
// single file is permitted to import both. See ADR-0011.
package identity

import (
	"context"
	"database/sql"
	"log/slog"
	"net/http"
	"time"

	"comms/internal/identity/internal/api"
	"comms/internal/identity/internal/app"
	"comms/internal/identity/internal/hashing"
	"comms/internal/identity/internal/postgres"
	"comms/internal/platform/database"
)

// Module is a wired Identity context.
type Module struct {
	service *app.Service
	handler *api.Handler
}

// New wires the context. The composition root for Identity lives here rather than
// in cmd/api, because cmd/api cannot see the packages being composed — which is
// the point.
func New(db *sql.DB, logger *slog.Logger) *Module {
	service := app.NewService(
		postgres.NewAccountRepository(db),
		postgres.NewDeviceRepository(db),
		postgres.NewSessionRepository(db),
		hashing.NewArgon2Hasher(hashing.DefaultArgon2Params()),
		// Events go to the outbox, in the same transaction as the change they
		// describe (ADR-0003). The relay in cmd/worker publishes them to Kafka.
		postgres.NewOutboxPublisher(db),
		database.NewConn(db),
		app.IDs{},
		time.Now,
	)

	return &Module{service: service, handler: api.NewHandler(service, logger)}
}

// Routes registers Identity's HTTP endpoints.
func (m *Module) Routes(mux *http.ServeMux) {
	m.handler.Routes(mux)
}

// Authenticated rejects requests without a valid access token, and puts the
// resolved caller on the request context.
//
// Exported because other contexts' HTTP surfaces are wrapped by it too. They do
// not know what it does; they know it is middleware.
func (m *Module) Authenticated(next http.Handler) http.Handler {
	return m.handler.Authenticated(next)
}

// Authenticate resolves an access token to the account and device presenting it.
//
// Returns plain strings, deliberately. A caller in another context must not be
// able to name an Identity type — that is what keeps the contexts independent, and
// what lets Messaging declare its own Authenticator port without importing
// anything from here.
func (m *Module) Authenticate(ctx context.Context, accessToken string) (accountID string, deviceID string, err error) {
	account, device, err := m.service.Authenticate(ctx, accessToken)
	if err != nil {
		return "", "", err //nolint:wrapcheck // callers reject; the cause is Identity's own error.
	}
	return string(account), string(device), nil
}

// Caller reports the authenticated account and device carried by a request
// context, or empty strings if the request was not authenticated.
func (m *Module) Caller(ctx context.Context) (accountID string, deviceID string) {
	account, device := api.Caller(ctx)
	return string(account), string(device)
}

// PurgeExpiredSessions deletes sessions that can no longer be refreshed. Exposed
// for the worker binary to schedule; expiry is enforced on read, so this only
// reclaims space.
func (m *Module) PurgeExpiredSessions(ctx context.Context) (int64, error) {
	deleted, err := m.service.PurgeExpiredSessions(ctx)
	if err != nil {
		return 0, err //nolint:wrapcheck // already wrapped by the service.
	}
	return deleted, nil
}
