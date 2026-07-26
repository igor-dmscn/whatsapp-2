package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"comms/internal/messaging/internal/domain"
	"comms/internal/platform/database"
)

// ReactionStore holds reaction state, outside the log (ADR-0008).
type ReactionStore struct {
	db database.Conn
}

// NewReactionStore returns a store over db.
func NewReactionStore(db *sql.DB) *ReactionStore {
	return &ReactionStore{db: database.NewConn(db)}
}

var _ domain.ReactionStore = (*ReactionStore)(nil)

// Add records a reaction, doing nothing if it is already there.
//
// DO NOTHING rather than an upsert that refreshes created_at: the first tap is when it
// happened, and a second tap from another device of the same account should not
// reorder anything.
func (s *ReactionStore) Add(ctx context.Context, reaction domain.Reaction) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO reactions (conversation_id, sequence, account_id, emoji, created_at)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (conversation_id, sequence, account_id, emoji) DO NOTHING`,
		string(reaction.ConversationID), int64(reaction.Sequence),
		string(reaction.AccountID), string(reaction.Emoji), reaction.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("add reaction: %w", err)
	}
	return nil
}

// Remove withdraws a reaction. Removing one that is not there is not an error.
func (s *ReactionStore) Remove(
	ctx context.Context,
	conversationID domain.ConversationID,
	sequence domain.Sequence,
	accountID domain.AccountID,
	emoji domain.Emoji,
) error {
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM reactions
		  WHERE conversation_id = $1 AND sequence = $2 AND account_id = $3 AND emoji = $4`,
		string(conversationID), int64(sequence), string(accountID), string(emoji),
	)
	if err != nil {
		return fmt.Errorf("remove reaction: %w", err)
	}
	return nil
}

// Range returns the reactions on a stretch of entries.
func (s *ReactionStore) Range(
	ctx context.Context,
	conversationID domain.ConversationID,
	from, to domain.Sequence,
) ([]domain.Reaction, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT conversation_id, sequence, account_id, emoji, created_at
		   FROM reactions
		  WHERE conversation_id = $1 AND sequence >= $2 AND sequence <= $3
		  ORDER BY sequence, created_at`,
		string(conversationID), int64(from), int64(to),
	)
	if err != nil {
		return nil, fmt.Errorf("select reactions: %w", err)
	}
	defer rows.Close()

	var reactions []domain.Reaction
	for rows.Next() {
		var (
			reaction domain.Reaction
			sequence int64
			at       time.Time
		)
		if err := rows.Scan(&reaction.ConversationID, &sequence, &reaction.AccountID,
			&reaction.Emoji, &at); err != nil {
			return nil, fmt.Errorf("scan reaction: %w", err)
		}
		reaction.Sequence = domain.Sequence(sequence)
		reaction.CreatedAt = at
		reactions = append(reactions, reaction)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read reactions: %w", err)
	}
	return reactions, nil
}
