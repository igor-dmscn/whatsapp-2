// Package application orchestrates messaging use cases.
//
// It owns the clock, identifier generation, transaction boundaries, and publishing
// recorded events. It owns no rules: what a membership may do, where a new
// member's visibility starts, and which position an entry gets are all decided by
// aggregates.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"comms/internal/messaging/internal/domain"
	"comms/internal/platform/logging"
)

// Clock is injected so ordering can be tested without sleeping.
type Clock func() time.Time

// Service carries out messaging use cases.
type Service struct {
	conversations domain.ConversationRepository
	memberships   domain.MembershipRepository
	entries       domain.EntryRepository
	invites       domain.InviteRepository
	reactions     domain.ReactionStore
	state         domain.MemberStateStore
	broadcaster   domain.Broadcaster
	events        domain.EventPublisher
	transactor    domain.Transactor
	ids           domain.IDs
	now           Clock
	logger        *slog.Logger
}

// NewService wires a Service.
func NewService(
	conversations domain.ConversationRepository,
	memberships domain.MembershipRepository,
	entries domain.EntryRepository,
	invites domain.InviteRepository,
	reactions domain.ReactionStore,
	state domain.MemberStateStore,
	broadcaster domain.Broadcaster,
	events domain.EventPublisher,
	transactor domain.Transactor,
	ids domain.IDs,
	now Clock,
	logger *slog.Logger,
) *Service {
	if now == nil {
		now = time.Now
	}
	return &Service{
		conversations, memberships, entries, invites, reactions, state,
		broadcaster, events, transactor, ids, now, logger,
	}
}

// maxRangeLimit caps how many entries one fetch returns.
//
// A client syncing a long absence pages through rather than asking for everything;
// a single unbounded response is how a large gap becomes a memory incident on both
// ends.
const maxRangeLimit = 500

// StartDirect returns the direct conversation between two accounts, creating it if
// they have none.
//
// Idempotent on the pair, so a client that starts a conversation twice — or two
// clients that start it simultaneously — end up in the same one. Without that,
// each would hold half the history and neither would know.
func (s *Service) StartDirect(ctx context.Context, initiator, other domain.AccountID) (*domain.Conversation, error) {
	if initiator == other {
		return nil, domain.ErrCannotMessageSelf
	}

	existing, err := s.conversations.DirectBetween(ctx, initiator, other)
	if err == nil {
		return existing, nil
	}
	if !errors.Is(err, domain.ErrConversationNotFound) {
		return nil, fmt.Errorf("look up direct conversation: %w", err)
	}

	now := s.now()
	conversation, err := domain.StartDirect(s.ids.NewConversationID(), now)
	if err != nil {
		return nil, err
	}

	// Both memberships see the whole log: a direct conversation has no history
	// that predates either participant.
	memberships := make([]*domain.Membership, 0, domain.DirectMemberCount)
	for _, accountID := range []domain.AccountID{initiator, other} {
		membership, err := domain.Join(conversation.ID(), accountID, domain.RoleMember, domain.FirstSequence, now)
		if err != nil {
			return nil, err
		}
		memberships = append(memberships, membership)
	}

	if err := s.conversations.Start(ctx, conversation, memberships); err != nil {
		// Lost a race to create the same pair's conversation. The other writer's
		// conversation is just as good, so use it rather than failing.
		if errors.Is(err, domain.ErrAlreadyAMember) {
			existing, lookupErr := s.conversations.DirectBetween(ctx, initiator, other)
			if lookupErr != nil {
				return nil, fmt.Errorf("look up direct conversation after conflict: %w", lookupErr)
			}
			return existing, nil
		}
		return nil, fmt.Errorf("start conversation: %w", err)
	}

	// The conversation, its memberships and their events in one transaction. A
	// membership without its member_joined event is a conversation the projection
	// never learns anybody belongs to, so its unread count stays zero forever.
	if err := s.atomically(ctx, func(ctx context.Context) error {
		events := conversation.TakeEvents()
		for _, membership := range memberships {
			events = append(events, membership.TakeEvents()...)
		}
		return s.publish(ctx, events)
	}); err != nil {
		return nil, err
	}

	// Tell both accounts' nodes to start listening, or nothing this conversation
	// produces will arrive live until they reconnect.
	//
	// Both, including the initiator: a socket authenticated before this call has no
	// membership to resume from and so was never subscribed. Notifying only the
	// recipient leaves the person who started the conversation the one who cannot
	// see replies to it — and every other device they have open in the same state.
	for _, accountID := range []domain.AccountID{initiator, other} {
		if err := s.broadcaster.NotifyConversationStarted(ctx, accountID, conversation.ID()); err != nil {
			logging.With(ctx, s.logger).Warn("notify conversation started",
				slog.String("conversation_id", string(conversation.ID())),
				slog.String("account_id", string(accountID)),
				slog.Any("error", err),
			)
		}
	}

	return conversation, nil
}

// Send appends an entry to a conversation.
//
// Idempotent on the client-supplied identifier: a retry returns the entry that
// already exists, with its original position, rather than creating a second one
// (MS-2).
func (s *Service) Send(
	ctx context.Context,
	conversationID domain.ConversationID,
	author domain.AccountID,
	clientEntryID string,
	contentType string,
	body []byte,
	replyTo domain.Sequence,
	attachmentID domain.AttachmentID,
) (*domain.Entry, error) {
	// Authorisation before content. The aggregate checks this again in Append and
	// that is where the rule lives — MayWrite is the membership's own method, called
	// here rather than restated. What the early call buys is that an account with no
	// business writing is refused before the server does any work on its payload,
	// which matters more once phase 7 makes payloads large.
	membership, err := s.memberships.Of(ctx, conversationID, author)
	if err != nil {
		if errors.Is(err, domain.ErrNotAMember) {
			// Reported as absence rather than forbidden, so that a caller cannot
			// use send failures to discover which conversations exist.
			return nil, domain.ErrNotAMember
		}
		return nil, fmt.Errorf("look up membership: %w", err)
	}
	if !membership.MayWrite() {
		return nil, domain.ErrNotPermittedToWrite
	}

	parsedClientID, err := domain.ParseClientEntryID(clientEntryID)
	if err != nil {
		return nil, err
	}
	// A photo with no caption is an entry with no content, which NewPayload rightly
	// refuses on its own. What makes it legitimate is the attachment, so the two are
	// validated together rather than the payload rule being loosened for everyone.
	payload, err := domain.PayloadFor(contentType, body, attachmentID)
	if err != nil {
		return nil, err
	}

	// Checked before the transaction so the common retry costs one indexed read
	// rather than a write that fails on a unique index.
	if existing, err := s.entries.ByClientEntryID(ctx, conversationID, author, parsedClientID); err == nil {
		return existing, nil
	} else if !errors.Is(err, domain.ErrEntryNotFound) {
		return nil, fmt.Errorf("look up existing entry: %w", err)
	}

	var entry *domain.Entry
	err = s.atomically(ctx, func(ctx context.Context) error {
		appended, events, err := s.conversations.AppendEntry(ctx, conversationID,
			func(conversation *domain.Conversation) (*domain.Entry, error) {
				return conversation.Append(s.ids.NewEntryID(), membership, parsedClientID,
					payload, replyTo, attachmentID, s.now())
			},
		)
		if err != nil {
			return err
		}
		entry = appended
		// AppendEntry joins this transaction rather than opening its own, so the
		// outbox rows below commit with the entry — which is the whole of ADR-0003's
		// guarantee, and the reason the entry's position and its event can never
		// disagree.
		return s.publish(ctx, events)
	})
	if err != nil {
		if errors.Is(err, domain.ErrEntryAlreadySent) {
			// An identical send won the race. Returning it makes the retry a lookup
			// rather than a failure, which is the whole point of MS-2.
			existing, lookupErr := s.entries.ByClientEntryID(ctx, conversationID, author, parsedClientID)
			if lookupErr != nil {
				return nil, fmt.Errorf("look up entry after conflict: %w", lookupErr)
			}
			return existing, nil
		}
		return nil, fmt.Errorf("append entry: %w", err)
	}

	// The ephemeral path, and it is allowed to fail: the entry is committed, and a
	// listener that misses this will notice the sequence gap and refetch. Failing
	// the send here would turn a delivery hiccup into lost work.
	if err := s.broadcaster.BroadcastEntry(ctx, entry); err != nil {
		logging.With(ctx, s.logger).Warn("broadcast entry",
			slog.String("conversation_id", string(conversationID)),
			slog.Int64("sequence", int64(entry.Sequence())),
			slog.Any("error", err),
		)
	}

	return entry, nil
}

// readerMembership loads a membership that entitles somebody to read.
//
// Separate from memberships.Of, and the difference is the bug it fixes: Of answers
// "is there a row", which stays true of somebody who was removed. Every read path
// that called Of directly left a removed member reading the conversation
// indefinitely. Found by a test that removed a member and then read as them.
//
// The rule itself is still the membership's — Active() is its method. What this adds
// is that no read path can forget to ask.
// MayAttach reports whether an account may add an attachment to a conversation.
//
// Media asks this before issuing an upload URL. It is the same question as "may this
// account send here", because an attachment is only ever reachable through an entry —
// so a reader in a channel is refused, and a removed member is refused (MS-6).
func (s *Service) MayAttach(ctx context.Context, conversationID, accountID string) (bool, error) {
	membership, err := s.readerMembership(
		ctx, domain.ConversationID(conversationID), domain.AccountID(accountID))
	if err != nil {
		if errors.Is(err, domain.ErrNotAMember) {
			return false, nil
		}
		return false, fmt.Errorf("look up membership: %w", err)
	}
	return membership.MayWrite(), nil
}

// MayView reports whether an account may see an attachment.
//
// The rule is that an attachment is exactly as visible as the entry that references
// it. That keeps the join-point policy in one place: a member who joined at position 40
// cannot read entry 39, and must not be able to fetch its photo either — which asking
// about the attachment on its own could not decide.
//
// An attachment no entry references yet is visible to nobody here. Media allows its
// owner, which is what lets a client show what it is uploading, and is a claim only
// Media can make since it holds the ownership.
func (s *Service) MayView(ctx context.Context, conversationID, accountID, attachmentID string) (bool, error) {
	membership, err := s.readerMembership(
		ctx, domain.ConversationID(conversationID), domain.AccountID(accountID))
	if err != nil {
		if errors.Is(err, domain.ErrNotAMember) {
			return false, nil
		}
		return false, fmt.Errorf("look up membership: %w", err)
	}

	entry, err := s.entries.ByAttachment(
		ctx, domain.ConversationID(conversationID), domain.AttachmentID(attachmentID))
	if err != nil {
		if errors.Is(err, domain.ErrEntryNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("look up entry by attachment: %w", err)
	}

	return membership.CanSee(entry.Sequence()), nil
}

func (s *Service) readerMembership(
	ctx context.Context,
	conversationID domain.ConversationID,
	reader domain.AccountID,
) (*domain.Membership, error) {
	membership, err := s.memberships.Of(ctx, conversationID, reader)
	if err != nil {
		return nil, err
	}
	if !membership.Active() {
		// Absence rather than forbidden, like every other non-membership: a removed
		// member should not be able to confirm the conversation still exists.
		return nil, domain.ErrNotAMember
	}
	return membership, nil
}

// Fetch returns a range of entries a caller is entitled to see.
//
// Visibility is applied from the caller's membership rather than trusted from the
// request, so asking for entry 1 in a group joined at 40 returns nothing rather
// than history that predates the caller.
func (s *Service) Fetch(
	ctx context.Context,
	conversationID domain.ConversationID,
	reader domain.AccountID,
	after domain.Sequence,
	limit int,
) ([]*domain.Entry, error) {
	membership, err := s.readerMembership(ctx, conversationID, reader)
	if err != nil {
		return nil, err
	}
	conversation, err := s.conversations.ByID(ctx, conversationID)
	if err != nil {
		return nil, fmt.Errorf("load conversation: %w", err)
	}

	gap, hasGap := domain.GapFor(membership, conversation.Head(), after)
	if !hasGap {
		return nil, nil
	}

	if limit <= 0 || limit > maxRangeLimit {
		limit = maxRangeLimit
	}

	entries, err := s.entries.Range(ctx, conversationID, gap.From, gap.To, limit)
	if err != nil {
		return nil, fmt.Errorf("read entries: %w", err)
	}
	return entries, nil
}

// Resume reports what a client is missing, given what it already holds.
//
// The heart of the sync protocol (MS-3). Conversations the client is current on
// produce no gap and are omitted, so a client that missed nothing is told nothing
// — and a client added to a conversation while offline learns about it here,
// without a separate mechanism.
func (s *Service) Resume(
	ctx context.Context,
	reader domain.AccountID,
	clientHas map[domain.ConversationID]domain.Sequence,
) ([]domain.Gap, error) {
	memberships, err := s.memberships.ForAccount(ctx, reader)
	if err != nil {
		return nil, fmt.Errorf("list memberships: %w", err)
	}

	gaps := make([]domain.Gap, 0, len(memberships))
	for _, membership := range memberships {
		if !membership.Active() {
			continue
		}

		conversation, err := s.conversations.ByID(ctx, membership.ConversationID())
		if err != nil {
			return nil, fmt.Errorf("load conversation: %w", err)
		}

		// A conversation absent from the client's map yields zero, which means
		// "nothing yet" — so a new membership is reported as a gap from its
		// visibility start without needing a separate case.
		if gap, hasGap := domain.GapFor(membership, conversation.Head(), clientHas[membership.ConversationID()]); hasGap {
			gaps = append(gaps, gap)
		}
	}
	return gaps, nil
}

// Conversations lists the conversations an account belongs to.
func (s *Service) Conversations(ctx context.Context, accountID domain.AccountID) ([]*domain.Membership, error) {
	memberships, err := s.memberships.ForAccount(ctx, accountID)
	if err != nil {
		return nil, fmt.Errorf("list memberships: %w", err)
	}
	return memberships, nil
}

// Conversation returns a conversation a caller belongs to.
func (s *Service) Conversation(ctx context.Context, conversationID domain.ConversationID, reader domain.AccountID) (*domain.Conversation, error) {
	if _, err := s.readerMembership(ctx, conversationID, reader); err != nil {
		return nil, err
	}
	conversation, err := s.conversations.ByID(ctx, conversationID)
	if err != nil {
		return nil, fmt.Errorf("load conversation: %w", err)
	}
	return conversation, nil
}

// Members returns a conversation's memberships, for a caller who belongs to it.
func (s *Service) Members(ctx context.Context, conversationID domain.ConversationID, reader domain.AccountID) ([]*domain.Membership, error) {
	if _, err := s.readerMembership(ctx, conversationID, reader); err != nil {
		return nil, err
	}
	members, err := s.memberships.In(ctx, conversationID)
	if err != nil {
		return nil, fmt.Errorf("list members: %w", err)
	}
	return members, nil
}

// atomically runs work in one transaction.
func (s *Service) atomically(ctx context.Context, work func(context.Context) error) error {
	return s.transactor.InTransaction(ctx, work) //nolint:wrapcheck // the closure's error is the caller's own.
}

// publish records events for publication, in the caller's transaction.
//
// A failure here fails the use case, which is the opposite of what the interim
// publisher did and is the point of the change. An entry committed without its event
// is an entry that never reaches an unread badge, a receipt or a notification, and
// nothing afterwards would ever discover the omission.
func (s *Service) publish(ctx context.Context, events []domain.Event) error {
	if len(events) == 0 {
		return nil
	}
	if err := s.events.Publish(ctx, events); err != nil {
		return fmt.Errorf("publish events: %w", err)
	}
	return nil
}

// Acknowledge records how far a member has received and read a conversation.
//
// Both marks in one call, because a client that has just rendered messages knows both
// answers at once and two endpoints would mean two round trips saying the same thing.
// Either may be zero, meaning "no change to this one".
//
// Nothing is stored synchronously. The events go to the outbox and the projection
// applies them (ADR-0002), so this returns before any count has moved — the window
// NF-7 requires clients to render correctly rather than treat as a failure.
func (s *Service) Acknowledge(
	ctx context.Context,
	conversationID domain.ConversationID,
	accountID domain.AccountID,
	deliveredThrough domain.Sequence,
	readThrough domain.Sequence,
) error {
	conversation, err := s.conversations.ByID(ctx, conversationID)
	if err != nil {
		return fmt.Errorf("look up conversation: %w", err)
	}

	membership, err := s.readerMembership(ctx, conversationID, accountID)
	if err != nil {
		return fmt.Errorf("look up membership: %w", err)
	}

	now := s.now()
	if deliveredThrough > 0 {
		if err := membership.Received(deliveredThrough, conversation.Head(), now); err != nil {
			return err
		}
	}
	if readThrough > 0 {
		if err := membership.Read(readThrough, conversation.Head(), now); err != nil {
			return err
		}
	}

	return s.atomically(ctx, func(ctx context.Context) error {
		return s.publish(ctx, membership.TakeEvents())
	})
}

// Summary is a conversation as the list screen needs it: the conversation, this
// member's place in it, and how far everyone else has got.
type Summary struct {
	Conversation *domain.Conversation
	Membership   *domain.Membership
	State        domain.MemberState
	// OthersReadThrough and OthersDeliveredThrough are the lowest marks among the
	// other members, which is what MS-13's per-entry state is derived from.
	OthersReadThrough      domain.Sequence
	OthersDeliveredThrough domain.Sequence
}

// Summaries returns every conversation an account belongs to, with its projected state.
//
// This is the screen ADR-0002 rejected computing on read. The projection makes it a
// handful of indexed lookups instead of an aggregate query over the whole log.
func (s *Service) Summaries(ctx context.Context, accountID domain.AccountID) ([]Summary, error) {
	memberships, err := s.memberships.ForAccount(ctx, accountID)
	if err != nil {
		return nil, fmt.Errorf("list memberships: %w", err)
	}

	summaries := make([]Summary, 0, len(memberships))
	for _, membership := range memberships {
		if !membership.Active() {
			continue
		}

		conversation, err := s.conversations.ByID(ctx, membership.ConversationID())
		if err != nil {
			return nil, fmt.Errorf("look up conversation: %w", err)
		}

		state, err := s.state.Of(ctx, membership.ConversationID(), accountID)
		if err != nil {
			return nil, fmt.Errorf("look up member state: %w", err)
		}

		othersRead, othersDelivered, err := s.state.Others(ctx, membership.ConversationID(), accountID)
		if err != nil {
			return nil, fmt.Errorf("look up others' marks: %w", err)
		}

		summaries = append(summaries, Summary{
			Conversation:           conversation,
			Membership:             membership,
			State:                  state,
			OthersReadThrough:      othersRead,
			OthersDeliveredThrough: othersDelivered,
		})
	}
	return summaries, nil
}

// --- groups and channels ---

// StartGroup creates a group with its creator as the only member, an administrator.
//
// The creator is an admin rather than a member because a group whose creator cannot
// add anybody is a group that cannot be used, and because somebody has to be able to
// promote the next administrator.
func (s *Service) StartGroup(ctx context.Context, creator domain.AccountID) (*domain.Conversation, error) {
	return s.startWithCreator(ctx, domain.StartGroup, creator)
}

// StartChannel creates a channel with its creator as an administrator.
//
// Every subsequent joiner is a reader, and reading the whole back catalogue: a
// broadcast with no history is useless to a new subscriber (MS-6).
func (s *Service) StartChannel(ctx context.Context, creator domain.AccountID) (*domain.Conversation, error) {
	return s.startWithCreator(ctx, domain.StartChannel, creator)
}

func (s *Service) startWithCreator(
	ctx context.Context,
	start func(domain.ConversationID, time.Time) (*domain.Conversation, error),
	creator domain.AccountID,
) (*domain.Conversation, error) {
	if creator == "" {
		return nil, domain.ValidationError{Field: "account_id", Reason: "must not be empty"}
	}

	now := s.now()
	conversation, err := start(s.ids.NewConversationID(), now)
	if err != nil {
		return nil, err
	}

	membership, err := domain.Join(conversation.ID(), creator, domain.RoleAdmin, domain.FirstSequence, now)
	if err != nil {
		return nil, err
	}

	if err := s.conversations.Start(ctx, conversation, []*domain.Membership{membership}); err != nil {
		return nil, fmt.Errorf("start conversation: %w", err)
	}

	if err := s.atomically(ctx, func(ctx context.Context) error {
		events := conversation.TakeEvents()
		events = append(events, membership.TakeEvents()...)
		return s.publish(ctx, events)
	}); err != nil {
		return nil, err
	}

	return conversation, nil
}

// AddMember adds an account to a conversation on an administrator's behalf.
//
// The joining position comes from the conversation, not from the caller. That is the
// whole of the history policy (MS-5, MS-6): a group member sees nothing said before
// they arrived, a channel subscriber sees everything. Letting a caller pass the
// position would make the policy a request parameter, which is how someone ends up
// reading a group's history by asking nicely.
func (s *Service) AddMember(
	ctx context.Context,
	conversationID domain.ConversationID,
	actor domain.AccountID,
	joiner domain.AccountID,
) (*domain.Membership, error) {
	if joiner == "" {
		return nil, domain.ValidationError{Field: "account_id", Reason: "must not be empty"}
	}

	conversation, err := s.conversations.ByID(ctx, conversationID)
	if err != nil {
		return nil, fmt.Errorf("look up conversation: %w", err)
	}

	actorMembership, err := s.memberships.Of(ctx, conversationID, actor)
	if err != nil {
		return nil, fmt.Errorf("look up membership: %w", err)
	}

	// The domain service, because the rule spans the conversation's kind and the
	// actor's role and belongs to neither aggregate.
	if err := domain.AuthoriseMembershipChange(conversation, actorMembership); err != nil {
		return nil, err
	}

	existing, err := s.memberships.Of(ctx, conversationID, joiner)
	switch {
	case err == nil && existing.Active():
		return nil, domain.ErrAlreadyAMember
	case err != nil && !errors.Is(err, domain.ErrNotAMember):
		return nil, fmt.Errorf("look up joiner: %w", err)
	}

	count, err := s.memberships.CountIn(ctx, conversationID)
	if err != nil {
		return nil, fmt.Errorf("count members: %w", err)
	}
	// The aggregate holds the limit; the count is state it cannot see. NF-13's cap is
	// also enforced by the aggregate having the number, not by this call site knowing
	// it.
	if err := conversation.AuthoriseJoin(count); err != nil {
		return nil, err
	}

	return s.join(ctx, conversation, joiner, conversation.DefaultRole())
}

// RedeemInvite joins the caller to a conversation using a shared link.
//
// The path into a conversation for someone no administrator has heard of. The invite
// carries the role, fixed when it was created, so a link cannot grant more later than
// it appeared to grant when it was shared.
func (s *Service) RedeemInvite(
	ctx context.Context,
	token domain.InviteToken,
	joiner domain.AccountID,
) (*domain.Conversation, error) {
	if joiner == "" {
		return nil, domain.ValidationError{Field: "account_id", Reason: "must not be empty"}
	}

	invite, err := s.invites.ByToken(ctx, token)
	if err != nil {
		return nil, fmt.Errorf("look up invite: %w", err)
	}

	conversation, err := s.conversations.ByID(ctx, invite.ConversationID())
	if err != nil {
		return nil, fmt.Errorf("look up conversation: %w", err)
	}

	// Already a member: the invite is not consumed and this is not an error. Somebody
	// clicking a link twice, or a link they were already given, should land in the
	// conversation rather than be told off.
	if existing, err := s.memberships.Of(ctx, conversation.ID(), joiner); err == nil && existing.Active() {
		return conversation, nil
	} else if err != nil && !errors.Is(err, domain.ErrNotAMember) {
		return nil, fmt.Errorf("look up membership: %w", err)
	}

	count, err := s.memberships.CountIn(ctx, conversation.ID())
	if err != nil {
		return nil, fmt.Errorf("count members: %w", err)
	}
	if err := conversation.AuthoriseJoin(count); err != nil {
		return nil, err
	}

	if err := invite.Redeem(s.now()); err != nil {
		return nil, err
	}

	// The redemption and the membership commit together. A use counted without a
	// membership silently spends a single-use link; a membership without the count
	// lets a single-use link be used twice.
	if err := s.atomically(ctx, func(ctx context.Context) error {
		if err := s.invites.Save(ctx, invite); err != nil {
			return err
		}
		if _, err := s.join(ctx, conversation, joiner, invite.Role()); err != nil {
			return err
		}
		return s.publish(ctx, invite.TakeEvents())
	}); err != nil {
		return nil, err
	}

	return conversation, nil
}

// join creates and saves a membership at the conversation's joining position.
func (s *Service) join(
	ctx context.Context,
	conversation *domain.Conversation,
	joiner domain.AccountID,
	role domain.Role,
) (*domain.Membership, error) {
	membership, err := domain.Join(
		conversation.ID(), joiner, role, conversation.JoiningPosition(), s.now())
	if err != nil {
		return nil, err
	}

	if err := s.atomically(ctx, func(ctx context.Context) error {
		if err := s.memberships.Save(ctx, membership); err != nil {
			return err
		}
		return s.publish(ctx, membership.TakeEvents())
	}); err != nil {
		return nil, err
	}

	// Their node has to start listening or the conversation delivers nothing live
	// until they reconnect. Allowed to fail: the ephemeral path always is.
	if err := s.broadcaster.NotifyConversationStarted(ctx, joiner, conversation.ID()); err != nil {
		logging.With(ctx, s.logger).Warn("notify conversation started",
			slog.String("conversation_id", string(conversation.ID())),
			slog.Any("error", err),
		)
	}

	return membership, nil
}

// RemoveMember withdraws somebody's membership on an administrator's behalf.
func (s *Service) RemoveMember(
	ctx context.Context,
	conversationID domain.ConversationID,
	actor domain.AccountID,
	subject domain.AccountID,
) error {
	if actor == subject {
		// Leaving and being removed are different acts with different meanings, and
		// only one of them should be reachable by asking to remove yourself.
		return domain.ErrCannotRemoveSelf
	}

	conversation, err := s.conversations.ByID(ctx, conversationID)
	if err != nil {
		return fmt.Errorf("look up conversation: %w", err)
	}

	actorMembership, err := s.memberships.Of(ctx, conversationID, actor)
	if err != nil {
		return fmt.Errorf("look up membership: %w", err)
	}
	if err := domain.AuthoriseMembershipChange(conversation, actorMembership); err != nil {
		return err
	}

	subjectMembership, err := s.memberships.Of(ctx, conversationID, subject)
	if err != nil {
		return fmt.Errorf("look up subject: %w", err)
	}

	return s.leave(ctx, subjectMembership)
}

// Leave withdraws the caller's own membership.
//
// Available in every kind, direct conversations included: a membership change made by
// its own subject is not the same act as one made about them, and the reason direct
// membership is fixed is that nobody else may alter it.
func (s *Service) Leave(
	ctx context.Context,
	conversationID domain.ConversationID,
	accountID domain.AccountID,
) error {
	membership, err := s.memberships.Of(ctx, conversationID, accountID)
	if err != nil {
		return fmt.Errorf("look up membership: %w", err)
	}
	return s.leave(ctx, membership)
}

func (s *Service) leave(ctx context.Context, membership *domain.Membership) error {
	if !membership.Leave(s.now()) {
		// Already gone. Nothing changed, so nothing is saved and nothing announced.
		return nil
	}

	return s.atomically(ctx, func(ctx context.Context) error {
		if err := s.memberships.Save(ctx, membership); err != nil {
			return err
		}
		return s.publish(ctx, membership.TakeEvents())
	})
}

// ChangeRole moves a member to a different role.
func (s *Service) ChangeRole(
	ctx context.Context,
	conversationID domain.ConversationID,
	actor domain.AccountID,
	subject domain.AccountID,
	role domain.Role,
) error {
	conversation, err := s.conversations.ByID(ctx, conversationID)
	if err != nil {
		return fmt.Errorf("look up conversation: %w", err)
	}

	actorMembership, err := s.memberships.Of(ctx, conversationID, actor)
	if err != nil {
		return fmt.Errorf("look up membership: %w", err)
	}
	if err := domain.AuthoriseMembershipChange(conversation, actorMembership); err != nil {
		return err
	}

	subjectMembership, err := s.memberships.Of(ctx, conversationID, subject)
	if err != nil {
		return fmt.Errorf("look up subject: %w", err)
	}

	changed, err := subjectMembership.ChangeRole(role, s.now())
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}

	return s.atomically(ctx, func(ctx context.Context) error {
		if err := s.memberships.Save(ctx, subjectMembership); err != nil {
			return err
		}
		return s.publish(ctx, subjectMembership.TakeEvents())
	})
}

// --- invites ---

// CreateInvite issues a shareable link to a conversation.
func (s *Service) CreateInvite(
	ctx context.Context,
	conversationID domain.ConversationID,
	actor domain.AccountID,
	maxUses int,
	expiresAt *time.Time,
) (*domain.Invite, error) {
	conversation, err := s.conversations.ByID(ctx, conversationID)
	if err != nil {
		return nil, fmt.Errorf("look up conversation: %w", err)
	}

	actorMembership, err := s.memberships.Of(ctx, conversationID, actor)
	if err != nil {
		return nil, fmt.Errorf("look up membership: %w", err)
	}
	if err := domain.AuthoriseInvite(conversation, actorMembership); err != nil {
		return nil, err
	}

	invite, err := domain.CreateInvite(
		s.ids.NewInviteID(), conversationID, s.ids.NewInviteToken(),
		actor, conversation.DefaultRole(), maxUses, expiresAt, s.now(),
	)
	if err != nil {
		return nil, err
	}

	if err := s.atomically(ctx, func(ctx context.Context) error {
		if err := s.invites.Save(ctx, invite); err != nil {
			return err
		}
		return s.publish(ctx, invite.TakeEvents())
	}); err != nil {
		return nil, err
	}

	return invite, nil
}

// RevokeInvite withdraws a link.
func (s *Service) RevokeInvite(
	ctx context.Context,
	inviteID domain.InviteID,
	actor domain.AccountID,
) error {
	invite, err := s.invites.ByID(ctx, inviteID)
	if err != nil {
		return fmt.Errorf("look up invite: %w", err)
	}

	conversation, err := s.conversations.ByID(ctx, invite.ConversationID())
	if err != nil {
		return fmt.Errorf("look up conversation: %w", err)
	}

	actorMembership, err := s.memberships.Of(ctx, invite.ConversationID(), actor)
	if err != nil {
		return fmt.Errorf("look up membership: %w", err)
	}
	if err := domain.AuthoriseInvite(conversation, actorMembership); err != nil {
		return err
	}

	if !invite.Revoke(s.now()) {
		return nil
	}

	return s.atomically(ctx, func(ctx context.Context) error {
		if err := s.invites.Save(ctx, invite); err != nil {
			return err
		}
		return s.publish(ctx, invite.TakeEvents())
	})
}

// Invites lists a conversation's links for an administrator.
func (s *Service) Invites(
	ctx context.Context,
	conversationID domain.ConversationID,
	actor domain.AccountID,
) ([]*domain.Invite, error) {
	conversation, err := s.conversations.ByID(ctx, conversationID)
	if err != nil {
		return nil, fmt.Errorf("look up conversation: %w", err)
	}

	actorMembership, err := s.memberships.Of(ctx, conversationID, actor)
	if err != nil {
		return nil, fmt.Errorf("look up membership: %w", err)
	}
	if err := domain.AuthoriseInvite(conversation, actorMembership); err != nil {
		return nil, err
	}

	invites, err := s.invites.In(ctx, conversationID)
	if err != nil {
		return nil, fmt.Errorf("list invites: %w", err)
	}
	return invites, nil
}

// --- revisions and reactions ---

// Revise edits an entry by appending a revision to the log (MS-8).
//
// Author only, enforced by the aggregate. What this adds is the check the aggregate
// cannot make: it does not hold the log, so it cannot know whether the target has
// already been retracted.
func (s *Service) Revise(
	ctx context.Context,
	conversationID domain.ConversationID,
	author domain.AccountID,
	target domain.Sequence,
	clientEntryID string,
	contentType string,
	body []byte,
) (*domain.Entry, error) {
	membership, targetEntry, err := s.amendable(ctx, conversationID, author, target)
	if err != nil {
		return nil, err
	}

	parsedClientID, err := domain.ParseClientEntryID(clientEntryID)
	if err != nil {
		return nil, err
	}
	payload, err := domain.NewPayload(contentType, body)
	if err != nil {
		return nil, err
	}

	return s.appendAmendment(ctx, conversationID, author, parsedClientID,
		func(conversation *domain.Conversation) (*domain.Entry, error) {
			return conversation.Revise(
				s.ids.NewEntryID(), membership, targetEntry, parsedClientID, payload, s.now())
		})
}

// Retract deletes an entry for everyone by appending a retraction (MS-9).
func (s *Service) Retract(
	ctx context.Context,
	conversationID domain.ConversationID,
	actor domain.AccountID,
	target domain.Sequence,
	clientEntryID string,
) (*domain.Entry, error) {
	membership, targetEntry, err := s.amendable(ctx, conversationID, actor, target)
	if err != nil {
		return nil, err
	}

	parsedClientID, err := domain.ParseClientEntryID(clientEntryID)
	if err != nil {
		return nil, err
	}

	return s.appendAmendment(ctx, conversationID, actor, parsedClientID,
		func(conversation *domain.Conversation) (*domain.Entry, error) {
			return conversation.Retract(s.ids.NewEntryID(), membership, targetEntry, parsedClientID, s.now())
		})
}

// amendable loads what an amendment needs and refuses the cases the aggregate cannot
// see.
func (s *Service) amendable(
	ctx context.Context,
	conversationID domain.ConversationID,
	actor domain.AccountID,
	target domain.Sequence,
) (*domain.Membership, *domain.Entry, error) {
	membership, err := s.readerMembership(ctx, conversationID, actor)
	if err != nil {
		return nil, nil, err
	}

	targetEntry, err := s.entries.AtSequence(ctx, conversationID, target)
	if err != nil {
		return nil, nil, fmt.Errorf("look up target entry: %w", err)
	}

	// Whether the target is already retracted is a fact about the log, which the
	// Conversation aggregate does not hold — it holds only the head. So the check
	// lives here rather than being faked in the aggregate with data passed in.
	latest, err := s.entries.LatestAmendmentFor(ctx, conversationID, target)
	switch {
	case err == nil && latest.Kind() == domain.KindRetraction:
		// Terminal. Editing something already withdrawn would put content back on
		// screen for any client that applied the retraction and then the edit.
		return nil, nil, domain.ErrEntryRetracted
	case err != nil && !errors.Is(err, domain.ErrEntryNotFound):
		return nil, nil, fmt.Errorf("look up amendments: %w", err)
	}

	return membership, targetEntry, nil
}

// appendAmendment runs an amendment through the same path as a send.
//
// Deliberately the same path: an amendment takes a position in the log, so it needs
// the conversation's row lock, the outbox row in the same transaction, and the
// broadcast afterwards — exactly as an ordinary message does. Anything else would be
// a second way to write to the log, with its own chances of being wrong.
func (s *Service) appendAmendment(
	ctx context.Context,
	conversationID domain.ConversationID,
	author domain.AccountID,
	clientEntryID domain.ClientEntryID,
	amend domain.AppendFunc,
) (*domain.Entry, error) {
	var entry *domain.Entry

	err := s.atomically(ctx, func(ctx context.Context) error {
		appended, events, err := s.conversations.AppendEntry(ctx, conversationID, amend)
		if err != nil {
			return err
		}
		entry = appended
		return s.publish(ctx, events)
	})
	if err != nil {
		if errors.Is(err, domain.ErrEntryAlreadySent) {
			existing, lookupErr := s.entries.ByClientEntryID(ctx, conversationID, author, clientEntryID)
			if lookupErr != nil {
				return nil, fmt.Errorf("look up amendment after conflict: %w", lookupErr)
			}
			return existing, nil
		}
		return nil, fmt.Errorf("append amendment: %w", err)
	}

	if err := s.broadcaster.BroadcastEntry(ctx, entry); err != nil {
		logging.With(ctx, s.logger).Warn("broadcast amendment",
			slog.String("conversation_id", string(conversationID)),
			slog.Any("error", err),
		)
	}
	return entry, nil
}

// React adds a reaction to an entry (MS-10).
//
// No sequence number is taken and the conversation's head does not move. That is the
// point: reacting is high-churn, and a position per tap would wake every connected
// client and cost every client a gap to fill (ADR-0008).
func (s *Service) React(
	ctx context.Context,
	conversationID domain.ConversationID,
	accountID domain.AccountID,
	sequence domain.Sequence,
	emoji string,
) error {
	membership, err := s.readerMembership(ctx, conversationID, accountID)
	if err != nil {
		return err
	}
	conversation, err := s.conversations.ByID(ctx, conversationID)
	if err != nil {
		return fmt.Errorf("look up conversation: %w", err)
	}

	reaction, err := domain.NewReaction(membership, sequence, conversation.Head(), emoji, s.now())
	if err != nil {
		return err
	}

	// No outbox row and no transaction. A reaction is not in the log, so there is no
	// event for a projection to build from and nothing for the state to be atomic
	// with — writing one would be ceremony that also spends a Kafka partition's
	// throughput on taps.
	if err := s.reactions.Add(ctx, reaction); err != nil {
		return fmt.Errorf("add reaction: %w", err)
	}

	if err := s.broadcaster.BroadcastReaction(ctx, reaction, false); err != nil {
		logging.With(ctx, s.logger).Warn("broadcast reaction", slog.Any("error", err))
	}
	return nil
}

// Unreact withdraws a reaction. Withdrawing one that is not there is not an error:
// with several devices on one account it is an ordinary race, not a mistake.
func (s *Service) Unreact(
	ctx context.Context,
	conversationID domain.ConversationID,
	accountID domain.AccountID,
	sequence domain.Sequence,
	emoji string,
) error {
	membership, err := s.readerMembership(ctx, conversationID, accountID)
	if err != nil {
		return err
	}
	conversation, err := s.conversations.ByID(ctx, conversationID)
	if err != nil {
		return fmt.Errorf("look up conversation: %w", err)
	}

	// Validated through the same constructor, so an emoji that could never have been
	// stored cannot be used to probe which entries exist.
	reaction, err := domain.NewReaction(membership, sequence, conversation.Head(), emoji, s.now())
	if err != nil {
		return err
	}

	if err := s.reactions.Remove(ctx, conversationID, sequence, accountID, reaction.Emoji); err != nil {
		return fmt.Errorf("remove reaction: %w", err)
	}

	if err := s.broadcaster.BroadcastReaction(ctx, reaction, true); err != nil {
		logging.With(ctx, s.logger).Warn("broadcast reaction removal", slog.Any("error", err))
	}
	return nil
}

// Reactions returns the reactions on a stretch of entries.
//
// A range rather than a cursor, and that is the whole light sync path: a client asks
// about what is on screen. If this ever needs ordering or history, ADR-0008 says
// reactions belong in the log after all.
func (s *Service) Reactions(
	ctx context.Context,
	conversationID domain.ConversationID,
	reader domain.AccountID,
	from, to domain.Sequence,
) ([]domain.Reaction, error) {
	membership, err := s.readerMembership(ctx, conversationID, reader)
	if err != nil {
		return nil, err
	}

	// Clamped to what this member may see, so asking about position 1 in a group
	// joined at 40 reveals nothing about who reacted to what before they arrived.
	if from < membership.VisibleFrom() {
		from = membership.VisibleFrom()
	}
	if to < from {
		return nil, nil
	}

	reactions, err := s.reactions.Range(ctx, conversationID, from, to)
	if err != nil {
		return nil, fmt.Errorf("list reactions: %w", err)
	}
	return reactions, nil
}
