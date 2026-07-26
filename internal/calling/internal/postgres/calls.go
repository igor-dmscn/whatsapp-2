// Package postgres adapts Calling's ports to the database.
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"comms/internal/calling/internal/domain"
	"comms/internal/platform/database"
)

// liveCallIndex is the partial unique index that enforces CL-2. Named so that a
// violation becomes a domain answer rather than a driver error.
const liveCallIndex = "calls_one_live_per_conversation"

const uniqueViolation = "23505"

// CallRepository stores Call aggregates.
type CallRepository struct {
	db database.Conn
}

// NewCallRepository returns a repository over db.
func NewCallRepository(db *sql.DB) *CallRepository {
	return &CallRepository{db: database.NewConn(db)}
}

var _ domain.CallRepository = (*CallRepository)(nil)

// Save writes the call and its participants.
//
// Both in one transaction, because a call whose participants did not land is a call CL-3
// can never end: nobody is recorded as present, so no departure can be the last.
func (r *CallRepository) Save(ctx context.Context, call *domain.Call) error {
	return r.db.InTransaction(ctx, func(ctx context.Context) error {
		var endedAt any
		if !call.EndedAt().IsZero() {
			endedAt = call.EndedAt()
		}

		_, err := r.db.ExecContext(ctx,
			`INSERT INTO calls (id, conversation_id, state, node, started_at, ended_at)
			 VALUES ($1, $2, $3, $4, $5, $6)
			 ON CONFLICT (id) DO UPDATE SET state = EXCLUDED.state, ended_at = EXCLUDED.ended_at`,
			string(call.ID()), string(call.ConversationID()), string(call.State()),
			call.Node(), call.StartedAt(), endedAt,
		)
		if err != nil {
			if constraintName(err) == liveCallIndex {
				// Another writer already has a live call in this conversation. Reported
				// as the domain answer, which the caller resolves by joining that one.
				return domain.ErrCallInProgress
			}
			return fmt.Errorf("save call: %w", err)
		}

		// The node is deliberately not in the update list: where a call's media lives is
		// decided once (CL-4), and an update that could move it would let a second api
		// node quietly re-point a call whose participants are already connected.
		for _, participant := range call.Participants() {
			var leftAt any
			if !participant.LeftAt.IsZero() {
				leftAt = participant.LeftAt
			}

			_, err := r.db.ExecContext(ctx,
				`INSERT INTO call_participants (call_id, device_id, account_id, joined_at, left_at)
				 VALUES ($1, $2, $3, $4, $5)
				 ON CONFLICT (call_id, device_id, joined_at) DO UPDATE SET left_at = EXCLUDED.left_at`,
				string(call.ID()), string(participant.DeviceID), string(participant.AccountID),
				participant.JoinedAt, leftAt,
			)
			if err != nil {
				return fmt.Errorf("save participant: %w", err)
			}
		}
		return nil
	})
}

// Find returns a call with its participants, or ErrCallNotFound.
func (r *CallRepository) Find(ctx context.Context, id domain.CallID) (*domain.Call, error) {
	return r.scan(ctx, `SELECT id, conversation_id, state, node, started_at, ended_at
	                      FROM calls WHERE id = $1`, string(id))
}

// ActiveIn returns the live call in a conversation, or ErrCallNotFound.
//
// The same predicate as the unique index, so what this finds and what that refuses are
// the same set by construction rather than by two definitions kept in step by hand.
func (r *CallRepository) ActiveIn(
	ctx context.Context,
	conversationID domain.ConversationID,
) (*domain.Call, error) {
	return r.scan(ctx, `SELECT id, conversation_id, state, node, started_at, ended_at
	                      FROM calls WHERE conversation_id = $1 AND state <> 'ended'`,
		string(conversationID))
}

// LiveFor returns every unended call a device is present in.
//
// Present, not merely recorded: a device that left and rejoined has two rows, and a query
// that ignored left_at would report calls it is no longer in and try to remove it twice.
func (r *CallRepository) LiveFor(ctx context.Context, device domain.DeviceID) ([]*domain.Call, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT DISTINCT c.id
		   FROM calls c
		   JOIN call_participants p ON p.call_id = c.id
		  WHERE p.device_id = $1 AND p.left_at IS NULL AND c.state <> 'ended'`,
		string(device),
	)
	if err != nil {
		return nil, fmt.Errorf("select live calls: %w", err)
	}
	defer rows.Close()

	var ids []domain.CallID
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan call id: %w", err)
		}
		ids = append(ids, domain.CallID(id))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read live calls: %w", err)
	}

	// Loaded whole afterwards rather than joined in one query: the aggregate needs all of
	// its participants, and rows.Next cannot be nested inside another query on the same
	// connection.
	calls := make([]*domain.Call, 0, len(ids))
	for _, id := range ids {
		call, err := r.Find(ctx, id)
		if err != nil {
			return nil, err
		}
		calls = append(calls, call)
	}
	return calls, nil
}

func (r *CallRepository) scan(ctx context.Context, query string, args ...any) (*domain.Call, error) {
	var (
		id, conversationID, state, node string
		startedAt                       time.Time
		endedAt                         sql.NullTime
	)

	err := r.db.QueryRowContext(ctx, query, args...).
		Scan(&id, &conversationID, &state, &node, &startedAt, &endedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.ErrCallNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("select call: %w", err)
	}

	participants, err := r.participantsOf(ctx, domain.CallID(id))
	if err != nil {
		return nil, err
	}

	return domain.ReconstituteCall(
		domain.CallID(id), domain.ConversationID(conversationID), domain.State(state), node,
		participants, startedAt, endedAt.Time,
	), nil
}

// participantsOf loads a call's presences, oldest first.
//
// Ordered by when they joined, because the aggregate's rejoin rule walks the list looking
// for a device's most recent presence and an arbitrary order would make it find an older
// one.
func (r *CallRepository) participantsOf(
	ctx context.Context,
	id domain.CallID,
) ([]domain.Participant, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT device_id, account_id, joined_at, left_at
		   FROM call_participants WHERE call_id = $1 ORDER BY joined_at`,
		string(id),
	)
	if err != nil {
		return nil, fmt.Errorf("load participants: %w", err)
	}
	defer rows.Close()

	var participants []domain.Participant
	for rows.Next() {
		var (
			deviceID, accountID string
			joinedAt            time.Time
			leftAt              sql.NullTime
		)
		if err := rows.Scan(&deviceID, &accountID, &joinedAt, &leftAt); err != nil {
			return nil, fmt.Errorf("scan participant: %w", err)
		}
		participants = append(participants, domain.Participant{
			AccountID: domain.AccountID(accountID),
			DeviceID:  domain.DeviceID(deviceID),
			JoinedAt:  joinedAt,
			LeftAt:    leftAt.Time,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read participants: %w", err)
	}
	return participants, nil
}

// constraintName returns the index a unique violation names, or "".
func constraintName(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
		return pgErr.ConstraintName
	}
	return ""
}
