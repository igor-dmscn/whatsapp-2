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
	"comms/internal/messaging/internal/domain"
	"comms/internal/messaging/internal/postgres"
	"comms/internal/messaging/internal/presence"
	"comms/internal/messaging/internal/projection"
	"comms/internal/messaging/internal/push"
	"comms/internal/platform/database"
	"comms/internal/platform/kafka"
	"comms/internal/platform/ratelimit"
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
	// notifier is the same broadcaster the service publishes entries through. Held
	// separately because Media reaches it directly, without a use case in between:
	// "look at this attachment again" is not a messaging decision.
	notifier *broadcast.RedisBroadcaster
	logger   *slog.Logger
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
	broadcaster := broadcast.NewRedisBroadcaster(redisClient)

	service := app.NewService(
		postgres.NewConversationRepository(db),
		postgres.NewMembershipRepository(db),
		postgres.NewEntryRepository(db),
		postgres.NewInviteRepository(db),
		postgres.NewReactionStore(db),
		postgres.NewMemberStateStore(db),
		// Presence and typing live only in Redis, with expiry, because both are false
		// within seconds and worthless once stale (see internal/presence).
		presence.NewStore(redisClient),
		broadcaster,
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
		service:  service,
		hub:      hub,
		notifier: broadcaster,
		logger:   logger,
		handler: api.NewHandler(service, hub, authenticator, api.CallerResolver(caller),
			options.AllowedOrigins, ratelimit.New(redisClient, logger), logger),
	}
}

// Run delivers broadcasts to this node's connections until ctx is cancelled.
//
// One goroutine per process reads from Redis regardless of how many sockets the
// node holds.
func (m *Module) Run(ctx context.Context) {
	// The presence heartbeat alongside the broadcast reader, because both are "one
	// goroutine per process serving every socket it holds".
	go m.renewPresence(ctx)

	m.hub.Run(ctx)
}

// renewPresence tells Redis, repeatedly, which devices this node is holding sockets for.
//
// Repeatedly and not once, which is the whole mechanism: presence is a claim with an expiry, so
// a node that stops — crashes, is deployed over, loses its network — stops making it, and the
// people it was holding go offline without anybody having to notice or clean up. A set that were
// added to on connect and removed on disconnect would leave a permanently online ghost for every
// process ever killed.
func (m *Module) renewPresence(ctx context.Context) {
	ticker := time.NewTicker(presence.Heartbeat)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, held := range m.hub.Connected() {
				accountID, deviceID := held[0], held[1]
				if err := m.service.Connected(ctx, domain.AccountID(accountID), deviceID); err != nil {
					m.logger.Warn("renew presence",
						slog.String("account_id", accountID), slog.Any("error", err))
				}
			}
		}
	}
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

// MayAttach reports whether an account may add an attachment to a conversation.
//
// Media's port, satisfied here in plain strings so that neither context imports the
// other. It is the same question as "may this account send here": an attachment is
// only ever reachable through an entry, so a reader in a channel and a removed member
// are both refused.
func (m *Module) MayAttach(ctx context.Context, conversationID, accountID string) (bool, error) {
	return m.service.MayAttach(ctx, conversationID, accountID) //nolint:wrapcheck // already named where it happened.
}

// MayView reports whether an account may see an attachment.
//
// An attachment is exactly as visible as the entry that references it, which keeps the
// join-point policy in the one place that owns it: a member who joined at position 40
// cannot read entry 39 and must not be able to fetch its photo either.
func (m *Module) MayView(ctx context.Context, conversationID, accountID, attachmentID string) (bool, error) {
	return m.service.MayView(ctx, conversationID, accountID, attachmentID) //nolint:wrapcheck // already named where it happened.
}

// AttachmentChanged tells connected clients to look at an attachment again.
//
// Satisfied here rather than by Media publishing onto these channels itself, which
// would mean sharing a channel-name string across a context boundary with no compiler
// to notice when it drifts.
func (m *Module) AttachmentChanged(ctx context.Context, conversationID, attachmentID string) error {
	return m.notifier.AttachmentChanged(ctx, conversationID, attachmentID) //nolint:wrapcheck // already named where it happened.
}

// MayJoin reports whether an account may join a conversation's call.
//
// Calling's port (CL-1). The same question as "may this account send here": joining a call
// means publishing media into the conversation, so a reader in a channel is refused and a
// removed member is refused. One rule, asked two ways.
func (m *Module) MayJoin(ctx context.Context, conversationID, accountID string) (bool, error) {
	return m.service.MayAttach(ctx, conversationID, accountID) //nolint:wrapcheck // already named where it happened.
}

// CallChanged tells clients following a conversation to look at its call again.
func (m *Module) CallChanged(ctx context.Context, conversationID, callID string) error {
	return m.notifier.CallChanged(ctx, conversationID, callID) //nolint:wrapcheck // already named where it happened.
}

// Session is one client's socket, as much of it as a delegated protocol needs.
type Session interface {
	AccountID() string
	DeviceID() string
	Send(frame any) error
}

// FrameHandler takes socket frames Messaging does not own.
//
// This is ADR-0004's one socket per client made possible: another context carries its own
// protocol over the connection a client already holds, and Messaging never learns what that
// protocol means. Registered per family — everything before the first dot of a frame type.
type FrameHandler interface {
	HandleFrame(ctx context.Context, session Session, frameType string, raw []byte) error
	SocketClosed(ctx context.Context, session Session)
}

// RegisterFrames routes a family of socket frames to a handler.
//
// Called from cmd/api, the only place allowed to know that two contexts exist.
func (m *Module) RegisterFrames(family string, handler FrameHandler) {
	m.handler.RegisterFrames(family, frames{handler})
}

// frames adapts a public handler to the internal one.
//
// Two interfaces with identical method sets and different names, which Go treats as
// unrelated for method signatures — but an api.Session value satisfies Session, so the
// adaptation is a pass-through. The alternative is exporting the internal type, which would
// make the fence decorative.
type frames struct {
	handler FrameHandler
}

var _ api.FrameHandler = frames{}

func (f frames) HandleFrame(ctx context.Context, session api.Session, frameType string, raw []byte) error {
	return f.handler.HandleFrame(ctx, session, frameType, raw) //nolint:wrapcheck // the handler's error reaches the client.
}

func (f frames) SocketClosed(ctx context.Context, session api.Session) {
	f.handler.SocketClosed(ctx, session)
}

// Notifier reaches connected clients without a database or an HTTP surface.
//
// For cmd/worker, which derives attachment variants and has to announce them but holds
// no sockets and serves no requests. A whole Module there would need a database handle
// and an authenticator it would never use.
type Notifier struct {
	broadcaster *broadcast.RedisBroadcaster
}

// NewNotifier returns a notifier publishing through client.
func NewNotifier(client *redis.Client) *Notifier {
	return &Notifier{broadcaster: broadcast.NewRedisBroadcaster(client)}
}

// AttachmentChanged tells clients following a conversation to look at an attachment
// again.
func (n *Notifier) AttachmentChanged(ctx context.Context, conversationID, attachmentID string) error {
	return n.broadcaster.AttachmentChanged(ctx, conversationID, attachmentID) //nolint:wrapcheck // already named where it happened.
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
// NewProjections wires the read-model consumer.
//
// The producer is for dead letters and may be nil, which degrades to logging — a consumer that
// could not report a skipped record must still skip it, or one bad message stops the partition.
func NewProjections(
	db *sql.DB,
	brokers []string,
	producer *kafka.Producer,
	logger *slog.Logger,
) (*Projections, error) {
	consumer, err := kafka.NewConsumer(brokers, "messaging-projections", projection.Topics(), logger)
	if err != nil {
		return nil, err //nolint:wrapcheck // already wrapped where it happened.
	}

	return &Projections{
		consumer: consumer,
		projector: projection.NewProjector(postgres.NewMemberStateStore(db),
			kafka.NewDeadLetters(producer, "messaging-projections", logger), logger),
		logger: logger,
	}, nil
}

// Run consumes until ctx is cancelled.
func (p *Projections) Run(ctx context.Context) {
	p.logger.Info("messaging projections started")
	p.consumer.Run(ctx, p.projector.Apply)
	p.logger.Info("messaging projections stopped")
}

// PushNotifications is the consumer that decides who to wake up about an entry.
//
// Its own consumer group, so that a slow push provider does not hold up the projections unread
// badges depend on — the same reason media processing has its own.
type PushNotifications struct {
	consumer *kafka.Consumer
	notifier *push.Notifier
	logger   *slog.Logger
}

// NewPushNotifications wires the consumer, with a sender that logs.
//
// The provider is a seam rather than an implementation, and ADR-0014 says why: credentials for
// APNs or FCM cannot exist in a system that runs entirely on one machine with no cloud
// dependencies, and the decision — who, and whether they are already looking — is where all the
// judgement is anyway.
func NewPushNotifications(
	db *sql.DB,
	redisClient *redis.Client,
	brokers []string,
	producer *kafka.Producer,
	logger *slog.Logger,
) (*PushNotifications, error) {
	consumer, err := kafka.NewConsumer(brokers, "messaging-push", push.Topics(), logger)
	if err != nil {
		return nil, err //nolint:wrapcheck // already wrapped where it happened.
	}

	store := presence.NewStore(redisClient)
	return &PushNotifications{
		consumer: consumer,
		notifier: push.NewNotifier(
			postgres.NewMembershipRepository(db),
			store,
			store,
			push.NewLoggingSender(logger),
			kafka.NewDeadLetters(producer, "messaging-push", logger),
			logger,
		),
		logger: logger,
	}, nil
}

// Run consumes until ctx is cancelled.
func (p *PushNotifications) Run(ctx context.Context) {
	p.logger.Info("push notifications started")
	p.consumer.Run(ctx, p.notifier.Apply)
	p.logger.Info("push notifications stopped")
}
