package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"comms/internal/messaging/internal/domain"
	"comms/internal/platform/database"
)

// SummaryStore answers "every conversation this account is in, with where it has read to".
//
// The read side of ADR-0002, and the query the schema was built for: memberships_account_id_idx
// exists to serve exactly this, and conversation_member_state_by_account exists to order it.
// Both were there before this type was — what came before asked per conversation, so an account
// in fifty conversations paid a hundred and fifty round trips for one screen.
type SummaryStore struct {
	db database.Conn
}

// NewSummaryStore returns a store over db.
func NewSummaryStore(db *sql.DB) *SummaryStore {
	return &SummaryStore{db: database.NewConn(db)}
}

var _ domain.SummaryStore = (*SummaryStore)(nil)

// ForAccount returns one row per conversation the account still belongs to.
//
// Ended memberships are excluded in SQL rather than returned and skipped in Go, which is also
// what makes the others' marks correct: somebody who left is not somebody whose read mark holds
// everyone else back.
func (s *SummaryStore) ForAccount(
	ctx context.Context,
	accountID domain.AccountID,
) ([]domain.ConversationSummary, error) {
	rows, err := s.db.QueryContext(ctx,
		// The lateral is per conversation what Others is per call: the lowest mark among the
		// other members. COALESCE inside the min, not outside it — min skips NULLs, so a
		// member with no projected row yet would be ignored rather than counted as having read
		// nothing, and one member's mark would be reported as everyone's.
		//
		// It yields NULL rather than no row when the reader is the only member left, because an
		// aggregate over nothing is one NULL row. The COALESCE outside catches that, and it is
		// the case worth a test: there is nobody whose marks could be lower, so the answer is
		// zero.
		//
		// Ordered by the projection's updated_at, falling back to creation for a conversation
		// nothing has happened in yet. This is what conversation_member_state_by_account was
		// created for.
		`SELECT c.id, c.kind, c.head, c.created_at,
		        m.role, m.visible_from,
		        COALESCE(s.unread_count, 0),
		        COALESCE(s.read_sequence, 0),
		        COALESCE(s.delivered_sequence, 0),
		        COALESCE(o.others_read, 0),
		        COALESCE(o.others_delivered, 0)
		   FROM memberships m
		   JOIN conversations c ON c.id = m.conversation_id
		   LEFT JOIN conversation_member_state s
		     ON s.conversation_id = m.conversation_id AND s.account_id = m.account_id
		   LEFT JOIN LATERAL (
		       SELECT min(COALESCE(os.read_sequence, 0))      AS others_read,
		              min(COALESCE(os.delivered_sequence, 0)) AS others_delivered
		         FROM memberships om
		         LEFT JOIN conversation_member_state os
		           ON os.conversation_id = om.conversation_id AND os.account_id = om.account_id
		        WHERE om.conversation_id = m.conversation_id
		          AND om.account_id <> m.account_id
		          AND om.left_at IS NULL
		   ) o ON TRUE
		  WHERE m.account_id = $1 AND m.left_at IS NULL
		  ORDER BY COALESCE(s.updated_at, c.created_at) DESC`,
		string(accountID),
	)
	if err != nil {
		return nil, fmt.Errorf("select conversation summaries: %w", err)
	}
	defer rows.Close()

	var summaries []domain.ConversationSummary
	for rows.Next() {
		var summary domain.ConversationSummary
		if err := rows.Scan(
			&summary.ConversationID, &summary.Kind, &summary.Head, &summary.CreatedAt,
			&summary.Role, &summary.VisibleFrom,
			&summary.Unread, &summary.ReadThrough, &summary.DeliveredThrough,
			&summary.OthersReadThrough, &summary.OthersDeliveredThrough,
		); err != nil {
			return nil, fmt.Errorf("scan conversation summary: %w", err)
		}
		summaries = append(summaries, summary)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read conversation summaries: %w", err)
	}
	return summaries, nil
}
