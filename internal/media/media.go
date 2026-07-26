// Package media is the whole of the Media context's public surface.
//
// The model, use cases, image processing, Postgres adapters and HTTP handlers live
// under internal/, unreachable from outside this tree by compiler rule rather than by
// convention. See ADR-0011.
//
// Media is the one context that reads the content of what a user sent, and it is
// arranged so that reading happens in exactly one place: the worker, off the request
// path, on bytes it fetched itself. The api process handles metadata and signatures
// only — no attachment ever passes through it, in either direction.
package media

import (
	"context"
	"database/sql"
	"log/slog"
	"net/http"
	"time"

	"comms/internal/media/internal/api"
	"comms/internal/media/internal/app"
	"comms/internal/media/internal/consumer"
	"comms/internal/media/internal/domain"
	"comms/internal/media/internal/postgres"
	"comms/internal/platform/database"
	"comms/internal/platform/kafka"
	"comms/internal/platform/objectstore"
)

// Conversations is what Media needs to know about Messaging.
//
// Declared here, in plain strings, because Media must not name a Messaging type.
// Messaging's Module satisfies it without either side importing the other — the join
// is made in cmd/api, and the compiler makes any shortcut impossible.
type Conversations interface {
	MayAttach(ctx context.Context, conversationID, accountID string) (bool, error)
	MayView(ctx context.Context, conversationID, accountID, attachmentID string) (bool, error)
}

// Notifier tells connected clients an attachment changed.
//
// Satisfied by Messaging, which owns the socket fanout. Media does not publish onto
// Messaging's channels itself: it would have to know their names, and a shared string
// is a coupling with no compiler to notice when it breaks.
type Notifier interface {
	AttachmentChanged(ctx context.Context, conversationID, attachmentID string) error
}

// CallerResolver reports the authenticated account and device on a request context.
type CallerResolver func(ctx context.Context) (accountID string, deviceID string)

// Middleware is what wraps handlers that require authentication.
type Middleware func(http.Handler) http.Handler

// Store is where attachment bytes live.
//
// Re-exported as an alias so cmd/api and cmd/worker can construct one — they own its
// lifetime, since api signs URLs and worker reads and writes objects — while this
// context stays the only place that knows what keys mean.
type Store = objectstore.Store

// Config is where the object store is and how to reach it.
type Config = objectstore.Config

// OpenStore returns a store for the configured bucket, creating the bucket if it is
// not there.
//
// Creating it at boot rather than in a deployment step, so a fresh checkout against a
// fresh MinIO works with no manual setup — the same reason migrations run
// automatically.
func OpenStore(ctx context.Context, config Config) (*Store, error) {
	store, err := objectstore.New(config, time.Now)
	if err != nil {
		return nil, err //nolint:wrapcheck // already named where it happened.
	}
	if err := store.EnsureBucket(ctx); err != nil {
		return nil, err //nolint:wrapcheck // already named where it happened.
	}
	return store, nil
}

// Module is a wired Media context.
type Module struct {
	service *app.Service
	handler *api.Handler
}

// New wires the context.
//
// The notifier is optional: the api process serves metadata and signs URLs and has no
// readiness to announce, so it passes nil rather than being handed a Redis client it
// would never publish on.
func New(
	db *sql.DB,
	store *Store,
	conversations Conversations,
	notifier Notifier,
	caller CallerResolver,
	logger *slog.Logger,
) *Module {
	service := app.NewService(
		postgres.NewAttachmentRepository(db),
		store,
		conversations,
		orSilent(notifier),
		postgres.NewOutboxPublisher(db),
		database.NewConn(db),
		time.Now,
		logger,
	)

	return &Module{
		service: service,
		handler: api.NewHandler(service, api.CallerResolver(caller), logger),
	}
}

// Routes registers Media's HTTP endpoints.
func (m *Module) Routes(mux *http.ServeMux, authenticated Middleware) {
	m.handler.Routes(mux, authenticated)
}

// Processing derives variants from published upload events.
//
// Exposed as one object because cmd/worker cannot see the packages it is made of —
// they are behind the internal/ fence — so the context composes its own consumer and
// hands out something that runs.
type Processing struct {
	consumer  *kafka.Consumer
	processor *consumer.Processor
	logger    *slog.Logger
}

// NewProcessing wires the attachment consumer.
//
// Its own consumer group, separate from the projections': deriving variants from a
// video takes as long as it takes, and sharing a group would make every unread badge
// in the system wait behind it.
func NewProcessing(
	db *sql.DB,
	store *Store,
	notifier Notifier,
	brokers []string,
	logger *slog.Logger,
) (*Processing, error) {
	group, err := kafka.NewConsumer(brokers, "media-processing", consumer.Topics(), logger)
	if err != nil {
		return nil, err //nolint:wrapcheck // already named where it happened.
	}

	// No Conversations port here: the worker derives variants and authorises nothing.
	// Whether anyone may see the result is decided when they ask for it.
	service := app.NewService(
		postgres.NewAttachmentRepository(db),
		store,
		refuseAll{},
		orSilent(notifier),
		postgres.NewOutboxPublisher(db),
		database.NewConn(db),
		time.Now,
		logger,
	)

	return &Processing{
		consumer:  group,
		processor: consumer.NewProcessor(service, logger),
		logger:    logger,
	}, nil
}

// Run consumes until ctx is cancelled.
func (p *Processing) Run(ctx context.Context) {
	p.logger.Info("media processing started")
	p.consumer.Run(ctx, p.processor.Apply)
	p.logger.Info("media processing stopped")
}

// refuseAll is the authorisation port in a process that must not make authorisation
// decisions. Reaching it is a wiring mistake, and it should look like one.
type refuseAll struct{}

func (refuseAll) MayAttach(context.Context, string, string) (bool, error)       { return false, nil }
func (refuseAll) MayView(context.Context, string, string, string) (bool, error) { return false, nil }

// silent is a notifier that says nothing, for the process that has nobody to tell.
type silent struct{}

func (silent) AttachmentChanged(context.Context, string, string) error { return nil }

func orSilent(notifier Notifier) domain.Notifier {
	if notifier == nil {
		return silent{}
	}
	return notifier
}
