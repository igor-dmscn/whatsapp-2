// Package database opens connections to Postgres and applies migrations.
package database

import (
	"database/sql"
	"embed"
	"fmt"
	"log/slog"
	"os"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" sql driver
	"github.com/pressly/goose/v3"
)

// migrations is embedded so a binary carries the schema it expects. There is no
// way to deploy code and forget to ship the migrations that go with it.
//
//go:embed migrations/*.sql
var migrations embed.FS

// Open returns a database handle. It does not verify connectivity — call Ping.
func Open(dsn string) (*sql.DB, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	return db, nil
}

// Migrate applies every migration not yet recorded as applied.
//
// Forward only. Migrations are never edited after merge, so there is no down
// path in normal operation and no dirty-state recovery to reason about — a
// mistake is corrected by a new migration, not by rewriting an old one.
func Migrate(db *sql.DB, logger *slog.Logger) error {
	goose.SetBaseFS(migrations)
	goose.SetLogger(gooseLogger{logger})
	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("set dialect: %w", err)
	}
	if err := goose.Up(db, "migrations"); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	return nil
}

// gooseLogger adapts goose's printf-style logger onto slog, so that migration
// output is the same JSON as everything else. A deploy that emits two log
// formats is a deploy whose logs cannot be queried as one thing.
type gooseLogger struct {
	logger *slog.Logger
}

func (g gooseLogger) Printf(format string, v ...any) {
	g.logger.Info(strings.TrimSpace(fmt.Sprintf(format, v...)))
}

func (g gooseLogger) Fatalf(format string, v ...any) {
	g.logger.Error(strings.TrimSpace(fmt.Sprintf(format, v...)))
	os.Exit(1)
}
