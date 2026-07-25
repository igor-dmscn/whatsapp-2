// Package infrastructure implements the ports declared by the messaging domain.
package infrastructure

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"comms/internal/messaging/domain"
)

// One repository type per aggregate root (ADR-0010).
type (
	// ConversationRepository stores Conversation aggregates.
	ConversationRepository struct{ db *sql.DB }

	// MembershipRepository stores Membership aggregates.
	MembershipRepository struct{ db *sql.DB }

	// EntryRepository reads entries.
	EntryRepository struct{ db *sql.DB }
)

func NewConversationRepository(db *sql.DB) *ConversationRepository {
	return &ConversationRepository{db}
}
func NewMembershipRepository(db *sql.DB) *MembershipRepository { return &MembershipRepository{db} }
func NewEntryRepository(db *sql.DB) *EntryRepository           { return &EntryRepository{db} }

var (
	_ domain.ConversationRepository = (*ConversationRepository)(nil)
	_ domain.MembershipRepository   = (*MembershipRepository)(nil)
	_ domain.EntryRepository        = (*EntryRepository)(nil)
)

const uniqueViolation = "23505"

const (
	directKeyIndex = "conversations_direct_key"
	sequenceIndex  = "entries_conversation_sequence_key"
	clientIDIndex  = "entries_client_entry_id_key"
)

// --- Conversation ---

// Start writes a conversation and its initial memberships in one transaction.
func (r *ConversationRepository) Start(ctx context.Context, conversation *domain.Conversation, memberships []*domain.Membership) error {
	transaction, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = transaction.Rollback() }()

	// Only direct conversations carry a pair key; for the others the column stays
	// NULL so the partial unique index ignores them.
	var directKey any
	if conversation.Kind() == domain.KindDirect && len(memberships) == domain.DirectMemberCount {
		directKey = domain.DirectKey(memberships[0].AccountID(), memberships[1].AccountID())
	}

	_, err = transaction.ExecContext(ctx,
		`INSERT INTO conversations (id, kind, head, created_at, direct_key)
		 VALUES ($1, $2, $3, $4, $5)`,
		string(conversation.ID()), string(conversation.Kind()), int64(conversation.Head()),
		conversation.CreatedAt(), directKey,
	)
	if err != nil {
		if constraintName(err) == directKeyIndex {
			// Another writer created this pair's conversation first.
			return domain.ErrAlreadyAMember
		}
		return fmt.Errorf("insert conversation: %w", err)
	}

	for _, membership := range memberships {
		if err := insertMembership(ctx, transaction, membership); err != nil {
			return err
		}
	}

	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

func (r *ConversationRepository) ByID(ctx context.Context, id domain.ConversationID) (*domain.Conversation, error) {
	return scanConversation(r.db.QueryRowContext(ctx,
		`SELECT id, kind, head, created_at FROM conversations WHERE id = $1`,
		string(id),
	))
}

func (r *ConversationRepository) DirectBetween(ctx context.Context, first, second domain.AccountID) (*domain.Conversation, error) {
	return scanConversation(r.db.QueryRowContext(ctx,
		`SELECT id, kind, head, created_at FROM conversations WHERE direct_key = $1`,
		domain.DirectKey(first, second),
	))
}

// AppendEntry appends under the conversation's row lock.
//
// SELECT ... FOR UPDATE is the whole mechanism: concurrent senders to the same
// conversation queue at this line instead of racing to claim a position and
// failing. It is a lock held for the length of one insert, and ADR-0003 already
// accepted that message writes serialise per conversation.
func (r *ConversationRepository) AppendEntry(
	ctx context.Context,
	id domain.ConversationID,
	append domain.AppendFunc,
) (*domain.Entry, []domain.Event, error) {
	transaction, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = transaction.Rollback() }()

	conversation, err := scanConversation(transaction.QueryRowContext(ctx,
		`SELECT id, kind, head, created_at FROM conversations WHERE id = $1 FOR UPDATE`,
		string(id),
	))
	if err != nil {
		return nil, nil, err
	}

	// The aggregate assigns the position. The repository's job is only to make sure
	// nobody else is doing so at the same time.
	entry, err := append(conversation)
	if err != nil {
		return nil, nil, err
	}

	if _, err := transaction.ExecContext(ctx,
		`UPDATE conversations SET head = $1 WHERE id = $2`,
		int64(conversation.Head()), string(conversation.ID()),
	); err != nil {
		return nil, nil, fmt.Errorf("advance conversation head: %w", err)
	}

	_, err = transaction.ExecContext(ctx,
		`INSERT INTO entries (id, conversation_id, sequence, author_id, client_entry_id, kind, content_type, body, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		string(entry.ID()), string(entry.ConversationID()), int64(entry.Sequence()),
		string(entry.AuthorID()), string(entry.ClientEntryID()), string(entry.Kind()),
		entry.Payload().ContentType(), entry.Payload().Body(), entry.CreatedAt(),
	)
	if err != nil {
		switch constraintName(err) {
		case sequenceIndex:
			// Unreachable while the lock is held. Named rather than surfaced as a
			// driver error so that if it ever fires, the cause is unambiguous.
			return nil, nil, domain.ErrSequenceAlreadyTaken
		case clientIDIndex:
			// The caller's idempotency check raced with an identical send. The entry
			// that won is the right answer, so report it as already sent.
			return nil, nil, domain.ErrEntryAlreadySent
		}
		return nil, nil, fmt.Errorf("insert entry: %w", err)
	}

	// Taken before commit so that phase 3 can write these to the outbox in this
	// same transaction.
	events := conversation.TakeEvents()

	if err := transaction.Commit(); err != nil {
		return nil, nil, fmt.Errorf("commit: %w", err)
	}
	return entry, events, nil
}

func scanConversation(row scanner) (*domain.Conversation, error) {
	var (
		id        domain.ConversationID
		kind      domain.Kind
		head      int64
		createdAt time.Time
	)
	err := row.Scan(&id, &kind, &head, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.ErrConversationNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("select conversation: %w", err)
	}
	return domain.ReconstituteConversation(id, kind, domain.Sequence(head), createdAt), nil
}

// --- Membership ---

func (r *MembershipRepository) Save(ctx context.Context, membership *domain.Membership) error {
	return insertMembership(ctx, r.db, membership)
}

// execer is what *sql.DB and *sql.Tx have in common.
type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func insertMembership(ctx context.Context, db execer, membership *domain.Membership) error {
	_, err := db.ExecContext(ctx,
		`INSERT INTO memberships (conversation_id, account_id, role, visible_from, joined_at, left_at)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (conversation_id, account_id) DO UPDATE
		     SET role = EXCLUDED.role, left_at = EXCLUDED.left_at`,
		string(membership.ConversationID()), string(membership.AccountID()), string(membership.Role()),
		int64(membership.VisibleFrom()), membership.JoinedAt(), membership.LeftAt(),
	)
	if err != nil {
		return fmt.Errorf("upsert membership: %w", err)
	}
	return nil
}

func (r *MembershipRepository) Of(ctx context.Context, conversationID domain.ConversationID, accountID domain.AccountID) (*domain.Membership, error) {
	membership, err := scanMembership(r.db.QueryRowContext(ctx,
		`SELECT conversation_id, account_id, role, visible_from, joined_at, left_at
		   FROM memberships WHERE conversation_id = $1 AND account_id = $2`,
		string(conversationID), string(accountID),
	))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.ErrNotAMember
	}
	if err != nil {
		return nil, fmt.Errorf("select membership: %w", err)
	}
	return membership, nil
}

func (r *MembershipRepository) In(ctx context.Context, conversationID domain.ConversationID) ([]*domain.Membership, error) {
	return r.queryMemberships(ctx,
		`SELECT conversation_id, account_id, role, visible_from, joined_at, left_at
		   FROM memberships WHERE conversation_id = $1 ORDER BY joined_at`,
		string(conversationID),
	)
}

func (r *MembershipRepository) ForAccount(ctx context.Context, accountID domain.AccountID) ([]*domain.Membership, error) {
	return r.queryMemberships(ctx,
		`SELECT conversation_id, account_id, role, visible_from, joined_at, left_at
		   FROM memberships WHERE account_id = $1 ORDER BY joined_at`,
		string(accountID),
	)
}

func (r *MembershipRepository) CountIn(ctx context.Context, conversationID domain.ConversationID) (int, error) {
	var count int
	err := r.db.QueryRowContext(ctx,
		`SELECT count(*) FROM memberships WHERE conversation_id = $1 AND left_at IS NULL`,
		string(conversationID),
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count memberships: %w", err)
	}
	return count, nil
}

func (r *MembershipRepository) queryMemberships(ctx context.Context, query string, args ...any) ([]*domain.Membership, error) {
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("select memberships: %w", err)
	}
	defer rows.Close()

	var memberships []*domain.Membership
	for rows.Next() {
		membership, err := scanMembership(rows)
		if err != nil {
			return nil, fmt.Errorf("scan membership: %w", err)
		}
		memberships = append(memberships, membership)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate memberships: %w", err)
	}
	return memberships, nil
}

func scanMembership(row scanner) (*domain.Membership, error) {
	var (
		conversationID domain.ConversationID
		accountID      domain.AccountID
		role           domain.Role
		visibleFrom    int64
		joinedAt       time.Time
		leftAt         *time.Time
	)
	if err := row.Scan(&conversationID, &accountID, &role, &visibleFrom, &joinedAt, &leftAt); err != nil {
		return nil, err
	}
	return domain.ReconstituteMembership(
		conversationID, accountID, role, domain.Sequence(visibleFrom), joinedAt, leftAt,
	), nil
}

// --- Entry ---

func (r *EntryRepository) Range(
	ctx context.Context,
	conversationID domain.ConversationID,
	from, to domain.Sequence,
	limit int,
) ([]*domain.Entry, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, conversation_id, sequence, author_id, client_entry_id, kind, content_type, body, created_at
		   FROM entries
		  WHERE conversation_id = $1 AND sequence >= $2 AND sequence <= $3
		  ORDER BY sequence
		  LIMIT $4`,
		string(conversationID), int64(from), int64(to), limit,
	)
	if err != nil {
		return nil, fmt.Errorf("select entries: %w", err)
	}
	defer rows.Close()

	var entries []*domain.Entry
	for rows.Next() {
		entry, err := scanEntry(rows)
		if err != nil {
			return nil, fmt.Errorf("scan entry: %w", err)
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate entries: %w", err)
	}
	return entries, nil
}

func (r *EntryRepository) ByClientEntryID(
	ctx context.Context,
	conversationID domain.ConversationID,
	authorID domain.AccountID,
	clientEntryID domain.ClientEntryID,
) (*domain.Entry, error) {
	entry, err := scanEntry(r.db.QueryRowContext(ctx,
		`SELECT id, conversation_id, sequence, author_id, client_entry_id, kind, content_type, body, created_at
		   FROM entries
		  WHERE conversation_id = $1 AND author_id = $2 AND client_entry_id = $3`,
		string(conversationID), string(authorID), string(clientEntryID),
	))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.ErrEntryNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("select entry: %w", err)
	}
	return entry, nil
}

func scanEntry(row scanner) (*domain.Entry, error) {
	var (
		id             domain.EntryID
		conversationID domain.ConversationID
		sequence       int64
		authorID       domain.AccountID
		clientEntryID  domain.ClientEntryID
		kind           domain.EntryKind
		contentType    string
		body           []byte
		createdAt      time.Time
	)
	if err := row.Scan(&id, &conversationID, &sequence, &authorID, &clientEntryID, &kind, &contentType, &body, &createdAt); err != nil {
		return nil, err
	}

	payload, err := domain.NewPayload(contentType, body)
	if err != nil {
		// Stored rows were valid when written. Reaching here means the row is
		// corrupt or a rule changed, and silently returning an empty payload
		// would hide that.
		return nil, fmt.Errorf("reconstitute payload for entry %s: %w", id, err)
	}

	return domain.ReconstituteEntry(
		id, conversationID, domain.Sequence(sequence), authorID, clientEntryID, kind, payload, createdAt,
	), nil
}

// scanner is what *sql.Row and *sql.Rows have in common.
type scanner interface {
	Scan(dest ...any) error
}

// constraintName returns the index a unique violation was raised against, or ""
// for any other error.
func constraintName(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
		return pgErr.ConstraintName
	}
	return ""
}
