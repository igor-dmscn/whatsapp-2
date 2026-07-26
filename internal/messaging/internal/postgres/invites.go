package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"comms/internal/messaging/internal/domain"
	"comms/internal/platform/database"
)

// InviteRepository stores Invite aggregates.
type InviteRepository struct {
	db database.Conn
}

// NewInviteRepository returns a repository over db.
func NewInviteRepository(db *sql.DB) *InviteRepository {
	return &InviteRepository{db: database.NewConn(db)}
}

var _ domain.InviteRepository = (*InviteRepository)(nil)

func (r *InviteRepository) Save(ctx context.Context, invite *domain.Invite) error {
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO invites (id, conversation_id, token, created_by, role, max_uses, uses, revoked, created_at, expires_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		 ON CONFLICT (id) DO UPDATE
		     SET uses = EXCLUDED.uses, revoked = EXCLUDED.revoked`,
		string(invite.ID()), string(invite.ConversationID()), string(invite.Token()),
		string(invite.CreatedBy()), string(invite.Role()), invite.MaxUses(), invite.Uses(),
		invite.Revoked(), invite.CreatedAt(), invite.ExpiresAt(),
	)
	if err != nil {
		return fmt.Errorf("upsert invite: %w", err)
	}
	return nil
}

func (r *InviteRepository) ByToken(ctx context.Context, token domain.InviteToken) (*domain.Invite, error) {
	return r.load(ctx, `token = $1`, string(token))
}

func (r *InviteRepository) ByID(ctx context.Context, id domain.InviteID) (*domain.Invite, error) {
	return r.load(ctx, `id = $1`, string(id))
}

func (r *InviteRepository) load(ctx context.Context, predicate string, arg any) (*domain.Invite, error) {
	//nolint:gosec // predicate is one of two literals in this file, never caller input.
	return scanInvite(r.db.QueryRowContext(ctx,
		`SELECT id, conversation_id, token, created_by, role, max_uses, uses, revoked, created_at, expires_at
		   FROM invites WHERE `+predicate,
		arg,
	))
}

func (r *InviteRepository) In(ctx context.Context, conversationID domain.ConversationID) ([]*domain.Invite, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, conversation_id, token, created_by, role, max_uses, uses, revoked, created_at, expires_at
		   FROM invites WHERE conversation_id = $1 ORDER BY created_at DESC`,
		string(conversationID),
	)
	if err != nil {
		return nil, fmt.Errorf("select invites: %w", err)
	}
	defer rows.Close()

	var invites []*domain.Invite
	for rows.Next() {
		invite, err := scanInvite(rows)
		if err != nil {
			return nil, err
		}
		invites = append(invites, invite)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read invites: %w", err)
	}
	return invites, nil
}

func scanInvite(row scanner) (*domain.Invite, error) {
	var (
		id             domain.InviteID
		conversationID domain.ConversationID
		token          domain.InviteToken
		createdBy      domain.AccountID
		role           domain.Role
		maxUses        int
		uses           int
		revoked        bool
		createdAt      time.Time
		expiresAt      *time.Time
	)
	err := row.Scan(&id, &conversationID, &token, &createdBy, &role, &maxUses, &uses, &revoked, &createdAt, &expiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.ErrInviteNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("select invite: %w", err)
	}
	return domain.ReconstituteInvite(
		id, conversationID, token, createdBy, role, maxUses, uses, revoked, createdAt, expiresAt,
	), nil
}
