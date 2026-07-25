// Package infrastructure implements the ports declared by the identity identity.
//
// Each repository loads and saves one whole aggregate. Reconstitution goes
// through the domain's Reconstitute* functions rather than assigning fields,
// which is why those exist: the aggregates have no exported fields to assign.
package identitypg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"comms/internal/identity"
)

// One repository type per aggregate root, rather than one type implementing all
// three ports. Each aggregate gets a repository whose method names read the same
// way at every call site — Save, ByID — which a single shared type cannot do,
// because Go permits only one method of a given name.
type (
	// AccountRepository stores Account aggregates, credentials included.
	AccountRepository struct{ db *sql.DB }

	// DeviceRepository stores Device aggregates.
	DeviceRepository struct{ db *sql.DB }

	// SessionRepository stores Session aggregates.
	SessionRepository struct{ db *sql.DB }
)

func NewAccountRepository(db *sql.DB) *AccountRepository { return &AccountRepository{db} }
func NewDeviceRepository(db *sql.DB) *DeviceRepository   { return &DeviceRepository{db} }
func NewSessionRepository(db *sql.DB) *SessionRepository { return &SessionRepository{db} }

var (
	_ identity.AccountRepository = (*AccountRepository)(nil)
	_ identity.DeviceRepository  = (*DeviceRepository)(nil)
	_ identity.SessionRepository = (*SessionRepository)(nil)
)

// Index names relied on to turn a constraint violation into a domain error.
// Uniqueness is enforced by the database rather than by a read-then-write, which
// would race under concurrent registration.
const (
	handleIndex   = "accounts_handle_key"
	emailIndex    = "accounts_email_key"
	passwordIndex = "credentials_one_password_per_account"
)

// uniqueViolation is Postgres error code 23505.
const uniqueViolation = "23505"

// --- Account ---

// Save persists an account and its credentials in one transaction.
//
// Credentials are upserted rather than replaced: they are only ever added, never
// removed or edited, so deleting and reinserting would churn the table and lose
// created_at for no reason.
func (r *AccountRepository) Save(ctx context.Context, account *identity.Account) error {
	transaction, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = transaction.Rollback() }()

	_, err = transaction.ExecContext(ctx,
		`INSERT INTO accounts (id, handle, email, created_at)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (id) DO UPDATE SET handle = EXCLUDED.handle, email = EXCLUDED.email`,
		string(account.ID()), string(account.Handle()), string(account.Email()), account.CreatedAt(),
	)
	if err != nil {
		switch constraintName(err) {
		case handleIndex:
			return identity.ErrHandleTaken
		case emailIndex:
			return identity.ErrEmailTaken
		}
		return fmt.Errorf("upsert account: %w", err)
	}

	for _, credential := range account.Credentials() {
		_, err = transaction.ExecContext(ctx,
			`INSERT INTO credentials (id, account_id, kind, material, created_at)
			 VALUES ($1, $2, $3, $4, $5)
			 ON CONFLICT (id) DO NOTHING`,
			string(credential.ID()), string(credential.AccountID()), string(credential.Kind()),
			credential.Material(), credential.CreatedAt(),
		)
		if err != nil {
			if constraintName(err) == passwordIndex {
				return identity.ErrPasswordAlreadySet
			}
			return fmt.Errorf("upsert credential: %w", err)
		}
	}

	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

func (r *AccountRepository) ByHandle(ctx context.Context, normalisedHandle string) (*identity.Account, error) {
	return r.loadAccount(ctx, `lower(handle) = $1`, normalisedHandle)
}

func (r *AccountRepository) ByID(ctx context.Context, accountID identity.AccountID) (*identity.Account, error) {
	return r.loadAccount(ctx, `id = $1`, string(accountID))
}

// loadAccount reads an account and its credentials, and hands both to the domain
// to reconstitute.
func (r *AccountRepository) loadAccount(ctx context.Context, predicate string, arg any) (*identity.Account, error) {
	var (
		id        identity.AccountID
		handle    identity.Handle
		email     identity.Email
		createdAt time.Time
	)
	err := r.db.QueryRowContext(ctx,
		`SELECT id, handle, email, created_at FROM accounts WHERE `+predicate, arg,
	).Scan(&id, &handle, &email, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, identity.ErrAccountNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("select account: %w", err)
	}

	credentials, err := r.credentialsFor(ctx, id)
	if err != nil {
		return nil, err
	}

	return identity.ReconstituteAccount(id, handle, email, credentials, createdAt), nil
}

func (r *AccountRepository) credentialsFor(ctx context.Context, accountID identity.AccountID) ([]identity.Credential, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, account_id, kind, material, created_at
		   FROM credentials WHERE account_id = $1 ORDER BY created_at`,
		string(accountID),
	)
	if err != nil {
		return nil, fmt.Errorf("select credentials: %w", err)
	}
	defer rows.Close()

	var credentials []identity.Credential
	for rows.Next() {
		var (
			id        identity.CredentialID
			owner     identity.AccountID
			kind      identity.CredentialKind
			material  string
			createdAt time.Time
		)
		if err := rows.Scan(&id, &owner, &kind, &material, &createdAt); err != nil {
			return nil, fmt.Errorf("scan credential: %w", err)
		}
		credentials = append(credentials, identity.ReconstituteCredential(id, owner, kind, material, createdAt))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate credentials: %w", err)
	}
	return credentials, nil
}

// --- Device ---

func (r *DeviceRepository) Save(ctx context.Context, device *identity.Device) error {
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO devices (id, account_id, name, created_at, revoked_at)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, revoked_at = EXCLUDED.revoked_at`,
		string(device.ID()), string(device.AccountID()), device.Name(), device.CreatedAt(), device.RevokedAt(),
	)
	if err != nil {
		return fmt.Errorf("upsert device: %w", err)
	}
	return nil
}

func (r *DeviceRepository) ByID(ctx context.Context, deviceID identity.DeviceID) (*identity.Device, error) {
	device, err := scanDevice(r.db.QueryRowContext(ctx,
		`SELECT id, account_id, name, created_at, revoked_at FROM devices WHERE id = $1`,
		string(deviceID),
	))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, identity.ErrDeviceNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("select device: %w", err)
	}
	return device, nil
}

func (r *DeviceRepository) ForAccount(ctx context.Context, accountID identity.AccountID) ([]*identity.Device, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, account_id, name, created_at, revoked_at
		   FROM devices WHERE account_id = $1 ORDER BY created_at`,
		string(accountID),
	)
	if err != nil {
		return nil, fmt.Errorf("select devices: %w", err)
	}
	defer rows.Close()

	var devices []*identity.Device
	for rows.Next() {
		device, err := scanDevice(rows)
		if err != nil {
			return nil, fmt.Errorf("scan device: %w", err)
		}
		devices = append(devices, device)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate devices: %w", err)
	}
	return devices, nil
}

// scanner is what *sql.Row and *sql.Rows have in common.
type scanner interface {
	Scan(dest ...any) error
}

func scanDevice(row scanner) (*identity.Device, error) {
	var (
		id        identity.DeviceID
		accountID identity.AccountID
		name      string
		createdAt time.Time
		revokedAt *time.Time
	)
	if err := row.Scan(&id, &accountID, &name, &createdAt, &revokedAt); err != nil {
		return nil, err
	}
	return identity.ReconstituteDevice(id, accountID, name, createdAt, revokedAt), nil
}

// --- Session ---

// Save writes both digests in one statement, which is what makes rotation
// atomic.
func (r *SessionRepository) Save(ctx context.Context, session *identity.Session) error {
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO sessions (id, device_id, access_digest, access_expires_at, refresh_digest, refresh_expires_at)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (id) DO UPDATE SET
		     access_digest      = EXCLUDED.access_digest,
		     access_expires_at  = EXCLUDED.access_expires_at,
		     refresh_digest     = EXCLUDED.refresh_digest,
		     refresh_expires_at = EXCLUDED.refresh_expires_at`,
		string(session.ID()), string(session.DeviceID()),
		session.AccessDigest(), session.AccessExpiresAt(),
		session.RefreshDigest(), session.RefreshExpiresAt(),
	)
	if err != nil {
		return fmt.Errorf("upsert session: %w", err)
	}
	return nil
}

func (r *SessionRepository) ByAccessDigest(ctx context.Context, digest []byte) (*identity.Session, error) {
	return r.loadSession(ctx, `access_digest = $1`, digest)
}

func (r *SessionRepository) ByRefreshDigest(ctx context.Context, digest []byte) (*identity.Session, error) {
	return r.loadSession(ctx, `refresh_digest = $1`, digest)
}

func (r *SessionRepository) loadSession(ctx context.Context, predicate string, digest []byte) (*identity.Session, error) {
	var (
		id               identity.SessionID
		deviceID         identity.DeviceID
		accessDigest     []byte
		accessExpiresAt  time.Time
		refreshDigest    []byte
		refreshExpiresAt time.Time
	)
	err := r.db.QueryRowContext(ctx,
		`SELECT id, device_id, access_digest, access_expires_at, refresh_digest, refresh_expires_at
		   FROM sessions WHERE `+predicate, digest,
	).Scan(&id, &deviceID, &accessDigest, &accessExpiresAt, &refreshDigest, &refreshExpiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, identity.ErrSessionNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("select session: %w", err)
	}

	return identity.ReconstituteSession(id, deviceID, accessDigest, accessExpiresAt, refreshDigest, refreshExpiresAt), nil
}

func (r *SessionRepository) DeleteForDevice(ctx context.Context, deviceID identity.DeviceID) error {
	if _, err := r.db.ExecContext(ctx, `DELETE FROM sessions WHERE device_id = $1`, string(deviceID)); err != nil {
		return fmt.Errorf("delete device sessions: %w", err)
	}
	return nil
}

func (r *SessionRepository) DeleteExpired(ctx context.Context, before time.Time) (int64, error) {
	// Keyed on the refresh expiry: a session whose access token expired is still
	// usable, and deleting it would log someone out mid-conversation.
	result, err := r.db.ExecContext(ctx, `DELETE FROM sessions WHERE refresh_expires_at < $1`, before)
	if err != nil {
		return 0, fmt.Errorf("delete expired sessions: %w", err)
	}
	deleted, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("count deleted sessions: %w", err)
	}
	return deleted, nil
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
