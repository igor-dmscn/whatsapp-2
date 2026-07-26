// Package messaging is the whole of the Messaging context's public surface.
//
// The model, use cases, Postgres adapters, Redis broadcast and HTTP/WebSocket
// handlers live under internal/, unreachable from outside this tree by compiler
// rule rather than by convention. See ADR-0011.
package messaging

import (
	"context"
	"database/sql"
	"log/slog"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"

	"comms/internal/messaging/internal/api"
	"comms/internal/messaging/internal/app"
	"comms/internal/messaging/internal/broadcast"
	"comms/internal/messaging/internal/postgres"
	"comms/internal/messaging/internal/projection"
	"comms/internal/platform/database"
	"comms/internal/platform/kafka"
)

// Authenticator resolves an access token to the account and device presenting it.
//
// Declared by Messaging, in plain strings, because Messaging must not name an
// Identity type. Identity's Module satisfies it without either side importing the
// other — the join is made in cmd/api, and the compiler makes any shortcut
// impossible.
type Authenticator interface {
	Authenticate(ctx context.Context, accessToken string) (accountID string, deviceID string, err error)
}

// CallerResolver reports the authenticated account and device on a request context.
type CallerResolver func(ctx context.Context) (accountID string, deviceID string)

// Middleware is what wraps handlers that require authentication.
type Middleware func(http.Handler) http.Handler

// Options are the choices a deployment makes about Messaging.
type Options struct {
	// AllowedOrigins may open a WebSocket. Empty means same-origin only: a
	// permissive policy would let any page a user visits open an authenticated
	// socket on their behalf.
	AllowedOrigins []string
}

// Module is a wired Messaging context.
type Module struct {
	service *app.Service
	handler *api.Handler
	hub     *api.Hub
}

// New wires the context.
func New(
	db *sql.DB,
	redisClient *redis.Client,
	authenticator Authenticator,
	caller CallerResolver,
	options Options,
	logger *slog.Logger,
) *Module {
	service := app.NewService(
		postgres.NewConversationRepository(db),
		postgres.NewMembershipRepository(db),
		postgres.NewEntryRepository(db),
		postgres.NewMemberStateStore(db),
		broadcast.NewRedisBroadcaster(redisClient),
		// Events go to the outbox, in the same transaction as the change they
		// describe (ADR-0003). The relay in cmd/worker publishes them to Kafka, and
		// the projector consumes them back into the member-state read model.
		postgres.NewOutboxPublisher(db),
		database.NewConn(db),
		app.IDs{},
		time.Now,
		logger,
	)

	hub := api.NewHub(redisClient, logger)

	return &Module{
		service: service,
		hub:     hub,
		handler: api.NewHandler(service, hub, authenticator, api.CallerResolver(caller), options.AllowedOrigins, logger),
	}
}

// Run delivers broadcasts to this node's connections until ctx is cancelled.
//
// One goroutine per process reads from Redis regardless of how many sockets the
// node holds.
func (m *Module) Run(ctx context.Context) {
	m.hub.Run(ctx)
}

// Routes registers Messaging's HTTP and WebSocket endpoints. authenticated wraps
// the handlers needing a bearer token; the socket authenticates in its first frame.
func (m *Module) Routes(mux *http.ServeMux, authenticated Middleware) {
	m.handler.Routes(mux, authenticated)
}

// DisconnectDevice closes any connection this node holds for a device.
//
// Called when Identity reports a device revoked. Authentication runs only at
// connect time, so without this a revoked device keeps receiving entries on an
// already-open socket until its access token happens to expire.
func (m *Module) DisconnectDevice(deviceID string) {
	m.hub.DisconnectDevice(deviceID)
}

// ConnectionCount reports how many sockets this node holds, for health reporting.
func (m *Module) ConnectionCount() int {
	return m.hub.ConnectionCount()
}

// OpenRedis returns a client, verifying it can be reached.
//
// Re-exported because the caller owns the client's lifetime — several contexts may
// share one — while the code that knows how to open it belongs in here.
func OpenRedis(ctx context.Context, url string) (*redis.Client, error) {
	client, err := broadcast.OpenRedis(ctx, url)
	if err != nil {
		return nil, err //nolint:wrapcheck // already wrapped where it happened.
	}
	return client, nil
}

// Projections builds Messaging's read models from published events.
//
// Exposed as one object because cmd/worker cannot see the packages it is made of —
// they are behind the internal/ fence — so the context composes its own consumer and
// hands out something that runs.
type Projections struct {
	consumer  *kafka.Consumer
	projector *projection.Projector
	logger    *slog.Logger
}

// NewProjections wires the projection consumer.
//
// Its own consumer group, so that adding a second kind of consumer later — the
// notification sender of phase 10, say — does not make the two compete for
// partitions or share a replay.
func NewProjections(db *sql.DB, brokers []string, logger *slog.Logger) (*Projections, error) {
	consumer, err := kafka.NewConsumer(brokers, "messaging-projections", projection.Topics(), logger)
	if err != nil {
		return nil, err //nolint:wrapcheck // already wrapped where it happened.
	}

	return &Projections{
		consumer:  consumer,
		projector: projection.NewProjector(postgres.NewMemberStateStore(db), logger),
		logger:    logger,
	}, nil
}

// Run consumes until ctx is cancelled.
func (p *Projections) Run(ctx context.Context) {
	p.logger.Info("messaging projections started")
	p.consumer.Run(ctx, p.projector.Apply)
	p.logger.Info("messaging projections stopped")
}
