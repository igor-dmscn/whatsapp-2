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

// MemberStateStore reads and writes the per-member projection of ADR-0002.
//
// Every write here is idempotent, and by construction rather than by a
// deduplication table. Applying the same event twice, or applying events out of
// order, reaches the same state — which is what lets the whole topic be replayed
// (NF-8) and what makes MS-12's forward-only cursor a property of the storage rather
// than a check somebody has to remember.
type MemberStateStore struct {
	db database.Conn
}

// NewMemberStateStore returns a store over db.
func NewMemberStateStore(db *sql.DB) *MemberStateStore {
	return &MemberStateStore{db: database.NewConn(db)}
}

var _ domain.MemberStateStore = (*MemberStateStore)(nil)

// EnsureMember creates the row for a new membership, leaving an existing one alone.
func (s *MemberStateStore) EnsureMember(
	ctx context.Context,
	conversationID domain.ConversationID,
	accountID domain.AccountID,
	at time.Time,
) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO conversation_member_state (conversation_id, account_id, updated_at)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (conversation_id, account_id) DO NOTHING`,
		string(conversationID), string(accountID), at,
	)
	if err != nil {
		return fmt.Errorf("ensure member state: %w", err)
	}
	return nil
}

// CountEntry increments the unread count for everyone but the author.
//
// One statement, and the guard is in it: projected_sequence is what makes this
// idempotent. A redelivered entry has a sequence the row has already counted, the
// WHERE clause excludes it, and no badge moves. Doing the check in Go — read, compare,
// write — would be a race between two consumer instances rather than a guard.
//
// The insert covers the member whose row has not been created yet, which happens
// whenever entry and membership events are projected out of order relative to each
// other. They are on the same partition so that is rare, but "rare" is not a
// guarantee, and a missing row would otherwise mean a permanently wrong badge.
func (s *MemberStateStore) CountEntry(
	ctx context.Context,
	conversationID domain.ConversationID,
	sequence domain.Sequence,
	author domain.AccountID,
	at time.Time,
) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO conversation_member_state
		     (conversation_id, account_id, unread_count, projected_sequence, updated_at)
		 SELECT m.conversation_id, m.account_id, 1, $2, $4
		   FROM memberships m
		  WHERE m.conversation_id = $1
		    AND m.account_id <> $3
		    AND m.left_at IS NULL
		    AND $2 >= m.visible_from
		 ON CONFLICT (conversation_id, account_id) DO UPDATE
		     SET unread_count = conversation_member_state.unread_count + 1,
		         projected_sequence = EXCLUDED.projected_sequence,
		         updated_at = EXCLUDED.updated_at
		   WHERE conversation_member_state.projected_sequence < EXCLUDED.projected_sequence`,
		string(conversationID), int64(sequence), string(author), at,
	)
	if err != nil {
		return fmt.Errorf("count entry into member state: %w", err)
	}
	return nil
}

// TouchAuthor advances the author's own marks to their own entry.
//
// Sending a message is reading it. Without this the sender's own messages sit
// unread-of, so their next cursor advance has to cover positions they wrote
// themselves, and any conversation they spoke in last shows a stale count.
func (s *MemberStateStore) TouchAuthor(
	ctx context.Context,
	conversationID domain.ConversationID,
	sequence domain.Sequence,
	author domain.AccountID,
	at time.Time,
) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO conversation_member_state
		     (conversation_id, account_id, read_sequence, delivered_sequence, projected_sequence, updated_at)
		 VALUES ($1, $2, $3, $3, $3, $4)
		 ON CONFLICT (conversation_id, account_id) DO UPDATE
		     SET read_sequence = GREATEST(conversation_member_state.read_sequence, EXCLUDED.read_sequence),
		         delivered_sequence = GREATEST(conversation_member_state.delivered_sequence, EXCLUDED.delivered_sequence),
		         projected_sequence = GREATEST(conversation_member_state.projected_sequence, EXCLUDED.projected_sequence),
		         updated_at = EXCLUDED.updated_at`,
		string(conversationID), string(author), int64(sequence), at,
	)
	if err != nil {
		return fmt.Errorf("touch author member state: %w", err)
	}
	return nil
}

// MarkRead advances the read mark and recomputes the unread count from the log.
//
// Recomputed rather than decremented, and this is the one place a count is derived
// instead of incremented. Two reasons: an exact answer costs one indexed count over
// a range, so there is no reason to accept an approximate one; and a decrement would
// have to know how many of the positions being cleared were the member's own, which
// is the same query with extra steps.
//
// GREATEST is MS-12. An advance that arrives after a later one changes nothing.
func (s *MemberStateStore) MarkRead(
	ctx context.Context,
	conversationID domain.ConversationID,
	accountID domain.AccountID,
	through domain.Sequence,
	at time.Time,
) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO conversation_member_state
		     (conversation_id, account_id, read_sequence, delivered_sequence, updated_at)
		 VALUES ($1, $2, $3, $3, $4)
		 ON CONFLICT (conversation_id, account_id) DO UPDATE
		     SET read_sequence = GREATEST(conversation_member_state.read_sequence, EXCLUDED.read_sequence),
		         -- Reading implies delivery: a member cannot have read what they were
		         -- never sent, and a client whose delivery acknowledgement was lost
		         -- would otherwise show as read-but-not-delivered.
		         delivered_sequence = GREATEST(conversation_member_state.delivered_sequence, EXCLUDED.read_sequence),
		         updated_at = EXCLUDED.updated_at`,
		string(conversationID), string(accountID), int64(through), at,
	)
	if err != nil {
		return fmt.Errorf("mark read: %w", err)
	}
	return s.recount(ctx, conversationID, accountID)
}

// MarkDelivered advances the delivery mark.
func (s *MemberStateStore) MarkDelivered(
	ctx context.Context,
	conversationID domain.ConversationID,
	accountID domain.AccountID,
	through domain.Sequence,
	at time.Time,
) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO conversation_member_state
		     (conversation_id, account_id, delivered_sequence, updated_at)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (conversation_id, account_id) DO UPDATE
		     SET delivered_sequence = GREATEST(conversation_member_state.delivered_sequence, EXCLUDED.delivered_sequence),
		         updated_at = EXCLUDED.updated_at`,
		string(conversationID), string(accountID), int64(through), at,
	)
	if err != nil {
		return fmt.Errorf("mark delivered: %w", err)
	}
	return nil
}

// recount sets unread_count to what the log actually says it is.
func (s *MemberStateStore) recount(
	ctx context.Context,
	conversationID domain.ConversationID,
	accountID domain.AccountID,
) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE conversation_member_state s
		    SET unread_count = (
		        SELECT count(*)
		          FROM entries e
		          JOIN memberships m
		            ON m.conversation_id = e.conversation_id AND m.account_id = s.account_id
		         WHERE e.conversation_id = s.conversation_id
		           AND e.sequence > s.read_sequence
		           AND e.sequence >= m.visible_from
		           AND e.author_id <> s.account_id
		    )
		  WHERE s.conversation_id = $1 AND s.account_id = $2`,
		string(conversationID), string(accountID),
	)
	if err != nil {
		return fmt.Errorf("recount unread: %w", err)
	}
	return nil
}

// ForAccount returns the projected state of every conversation an account belongs to.
func (s *MemberStateStore) ForAccount(ctx context.Context, accountID domain.AccountID) ([]domain.MemberState, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT conversation_id, read_sequence, delivered_sequence, unread_count, updated_at
		   FROM conversation_member_state
		  WHERE account_id = $1
		  ORDER BY updated_at DESC`,
		string(accountID),
	)
	if err != nil {
		return nil, fmt.Errorf("select member state: %w", err)
	}
	defer rows.Close()

	var states []domain.MemberState
	for rows.Next() {
		var state domain.MemberState
		if err := rows.Scan(
			&state.ConversationID, &state.ReadSequence, &state.DeliveredSequence,
			&state.UnreadCount, &state.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan member state: %w", err)
		}
		state.AccountID = accountID
		states = append(states, state)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read member state: %w", err)
	}
	return states, nil
}

// Of returns one member's projected state.
func (s *MemberStateStore) Of(
	ctx context.Context,
	conversationID domain.ConversationID,
	accountID domain.AccountID,
) (domain.MemberState, error) {
	state := domain.MemberState{ConversationID: conversationID, AccountID: accountID}
	err := s.db.QueryRowContext(ctx,
		`SELECT read_sequence, delivered_sequence, unread_count, updated_at
		   FROM conversation_member_state
		  WHERE conversation_id = $1 AND account_id = $2`,
		string(conversationID), string(accountID),
	).Scan(&state.ReadSequence, &state.DeliveredSequence, &state.UnreadCount, &state.UpdatedAt)

	if errors.Is(err, sql.ErrNoRows) {
		// Not an error. A conversation whose projection has not caught up yet reads
		// as nothing read and nothing unread, which is what NF-7 requires clients to
		// tolerate — and returning an error here would make the caller decide how to
		// render a window the design says is normal.
		return state, nil
	}
	if err != nil {
		return state, fmt.Errorf("select member state: %w", err)
	}
	return state, nil
}

// Others' marks are no longer read one conversation at a time. They are a column of the
// conversation-list query now (see summaries.go), because they were never wanted on their own —
// only ever beside everything else about a conversation, once per conversation on a screen.
// Kept in one place so the two cannot disagree about who counts as a member.
