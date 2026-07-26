package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// Conn is a database handle that uses the transaction on the context when there is
// one, and the pool when there is not.
//
// It exists so that the transactional outbox can be what it claims to be. An event
// row has to be written in the same transaction as the state change it describes —
// otherwise a crash between the two either loses the event or announces something
// that did not happen. That means a repository must be able to participate in a
// transaction it did not open and does not own.
//
// The alternative, threading an explicit *sql.Tx through every repository method,
// gives every read a parameter it does not use and makes the type signature of a
// query depend on whether it happens to be inside a transaction. This carries it on
// the context instead, which is invisible to code that does not care.
//
// It deliberately mirrors *sql.DB's method names and signatures, so a repository
// holding one reads identically to a repository holding a pool.
type Conn struct {
	db *sql.DB
}

// NewConn wraps a pool.
func NewConn(db *sql.DB) Conn { return Conn{db: db} }

// txKey carries the active transaction. Unexported and of a private type, so
// nothing outside this package can put a transaction on a context or take one off.
type txKey struct{}

// queryer is the overlap between *sql.DB and *sql.Tx that repositories use.
type queryer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func (c Conn) queryer(ctx context.Context) queryer {
	if transaction, active := ctx.Value(txKey{}).(*sql.Tx); active {
		return transaction
	}
	return c.db
}

func (c Conn) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return c.queryer(ctx).ExecContext(ctx, query, args...) //nolint:wrapcheck // a pass-through; the caller wraps.
}

func (c Conn) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return c.queryer(ctx).QueryContext(ctx, query, args...) //nolint:wrapcheck // a pass-through; the caller wraps.
}

func (c Conn) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return c.queryer(ctx).QueryRowContext(ctx, query, args...)
}

// InTransaction runs work in a transaction, committing if it returns nil and
// rolling back otherwise.
//
// Work already inside a transaction joins the existing one rather than opening a
// second: two transactions on separate connections cannot see each other's
// uncommitted rows, so a nested one would deadlock against its own parent the first
// time it touched a locked row.
//
// ponytail: joining means an inner failure rolls back the whole outer transaction.
// That is the conservative direction, and correct for every caller here — each one
// wants all or nothing. Savepoints, if a caller ever needs to recover from a
// partial failure and continue.
func (c Conn) InTransaction(ctx context.Context, work func(context.Context) error) error {
	if _, active := ctx.Value(txKey{}).(*sql.Tx); active {
		return work(ctx)
	}

	transaction, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	// Rollback after a successful commit returns ErrTxDone, which is why the error
	// is dropped: by then there is nothing to roll back and nothing to report.
	defer func() { _ = transaction.Rollback() }()

	if err := work(context.WithValue(ctx, txKey{}, transaction)); err != nil {
		return err
	}

	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

// InTransaction is the free function form, for code holding a pool rather than a Conn.
func InTransaction(ctx context.Context, db *sql.DB, work func(context.Context) error) error {
	return NewConn(db).InTransaction(ctx, work)
}

// ErrNoTransaction is returned by RequireTransaction when there is none.
var ErrNoTransaction = errors.New("this must run inside a transaction")

// RequireTransaction fails when the caller is not in a transaction.
//
// Used by the outbox writer. Writing an event row outside a transaction compiles,
// runs, and silently produces exactly the split-brain the outbox exists to prevent
// — so it is refused rather than trusted to review.
func RequireTransaction(ctx context.Context) error {
	if _, active := ctx.Value(txKey{}).(*sql.Tx); !active {
		return ErrNoTransaction
	}
	return nil
}
