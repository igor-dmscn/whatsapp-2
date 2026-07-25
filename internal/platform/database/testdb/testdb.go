// Package testdb gives integration tests a migrated database.
//
// Tests run against the real Postgres from docker-compose rather than a fake.
// The repositories here exist to translate constraint violations and SQL
// behaviour into domain errors, and a fake would simply agree with whatever the
// code already believes — which is the one thing a test must not do.
package testdb

import (
	"database/sql"
	"log/slog"
	"os"
	"sync"
	"testing"

	"comms/internal/platform/database"
)

var (
	once      sync.Once
	shared    *sql.DB
	openError error
)

// Open returns a migrated database, skipping the test if DATABASE_URL is unset.
//
// Skipping rather than failing keeps `go test ./...` usable without Docker
// running. CI sets DATABASE_URL, so nothing is silently skipped where it counts.
func Open(t *testing.T) *sql.DB {
	t.Helper()

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set, skipping integration test")
	}

	// Migrations run once per test binary, not once per test.
	once.Do(func() {
		shared, openError = database.Open(dsn)
		if openError != nil {
			return
		}
		quiet := slog.New(slog.DiscardHandler)
		openError = database.Migrate(shared, quiet)
	})
	if openError != nil {
		t.Fatalf("prepare test database: %v", openError)
	}

	return shared
}
