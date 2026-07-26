// Package postgres adapts Media's ports to the database.
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"comms/internal/media/internal/domain"
	"comms/internal/platform/database"
)

// AttachmentRepository stores attachments and the variants derived from them.
type AttachmentRepository struct {
	db database.Conn
}

// NewAttachmentRepository returns a repository over db.
func NewAttachmentRepository(db *sql.DB) *AttachmentRepository {
	return &AttachmentRepository{db: database.NewConn(db)}
}

var _ domain.AttachmentRepository = (*AttachmentRepository)(nil)

// Save writes the attachment and its variants.
//
// One statement each, both upserts, and deliberately not wrapped in a transaction of
// their own: the caller that needs atomicity — completing an upload, which must commit
// its outbox row with the state change — already holds one, and database.Conn joins it.
// A variant set written without its attachment row is not a state this code can reach,
// because the attachment row is written first and processing only ever adds variants to
// one that exists.
func (r *AttachmentRepository) Save(ctx context.Context, attachment *domain.Attachment) error {
	var readyAt any
	if !attachment.ReadyAt().IsZero() {
		readyAt = attachment.ReadyAt()
	}

	_, err := r.db.ExecContext(ctx,
		`INSERT INTO attachments (
		     id, conversation_id, owner_id, content_type, byte_size,
		     state, object_key, failure, created_at, ready_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		 ON CONFLICT (id) DO UPDATE SET
		     state    = EXCLUDED.state,
		     failure  = EXCLUDED.failure,
		     ready_at = EXCLUDED.ready_at`,
		string(attachment.ID()), string(attachment.ConversationID()), string(attachment.OwnerID()),
		attachment.ContentType(), attachment.ByteSize(),
		string(attachment.State()), attachment.ObjectKey(), attachment.Failure(),
		attachment.CreatedAt(), readyAt,
	)
	if err != nil {
		return fmt.Errorf("save attachment: %w", err)
	}

	// Only the lifecycle columns are updated above. What was declared at upload —
	// owner, conversation, type, size, object key — is immutable by then, and letting
	// an update rewrite it would make a replayed save able to change what the signature
	// was issued for.
	for _, variant := range attachment.Variants() {
		_, err := r.db.ExecContext(ctx,
			`INSERT INTO attachment_variants (
			     attachment_id, name, content_type, object_key, width, height, byte_size)
			 VALUES ($1, $2, $3, $4, $5, $6, $7)
			 ON CONFLICT (attachment_id, name) DO UPDATE SET
			     content_type = EXCLUDED.content_type,
			     object_key   = EXCLUDED.object_key,
			     width        = EXCLUDED.width,
			     height       = EXCLUDED.height,
			     byte_size    = EXCLUDED.byte_size`,
			string(attachment.ID()), string(variant.Name), variant.ContentType,
			variant.ObjectKey, variant.Width, variant.Height, variant.ByteSize,
		)
		if err != nil {
			return fmt.Errorf("save variant %s: %w", variant.Name, err)
		}
	}
	return nil
}

// Find returns an attachment with its variants, or ErrAttachmentNotFound.
func (r *AttachmentRepository) Find(
	ctx context.Context,
	id domain.AttachmentID,
) (*domain.Attachment, error) {
	var (
		conversationID, ownerID, contentType, state, objectKey, failure string
		byteSize                                                        int64
		createdAt                                                       time.Time
		readyAt                                                         sql.NullTime
	)

	err := r.db.QueryRowContext(ctx,
		`SELECT conversation_id, owner_id, content_type, byte_size,
		        state, object_key, failure, created_at, ready_at
		   FROM attachments WHERE id = $1`,
		string(id),
	).Scan(&conversationID, &ownerID, &contentType, &byteSize,
		&state, &objectKey, &failure, &createdAt, &readyAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.ErrAttachmentNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("find attachment: %w", err)
	}

	variants, err := r.variantsOf(ctx, id)
	if err != nil {
		return nil, err
	}

	return domain.ReconstituteAttachment(
		id, domain.ConversationID(conversationID), domain.AccountID(ownerID),
		contentType, byteSize, domain.State(state), objectKey, failure,
		variants, createdAt, readyAt.Time,
	), nil
}

// variantsOf loads an attachment's variants, ordered by name so that two clients
// listing them see the same order.
func (r *AttachmentRepository) variantsOf(
	ctx context.Context,
	id domain.AttachmentID,
) ([]domain.Variant, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT name, content_type, object_key, width, height, byte_size
		   FROM attachment_variants WHERE attachment_id = $1 ORDER BY name`,
		string(id),
	)
	if err != nil {
		return nil, fmt.Errorf("load variants: %w", err)
	}
	defer rows.Close()

	var variants []domain.Variant
	for rows.Next() {
		var variant domain.Variant
		var name string
		if err := rows.Scan(&name, &variant.ContentType, &variant.ObjectKey,
			&variant.Width, &variant.Height, &variant.ByteSize); err != nil {
			return nil, fmt.Errorf("scan variant: %w", err)
		}
		variant.Name = domain.VariantName(name)
		variants = append(variants, variant)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read variants: %w", err)
	}
	return variants, nil
}
