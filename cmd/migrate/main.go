// Command migrate applies database migrations and exits.
//
// A separate command rather than a step inside api: migrations must be able to
// run before any api node starts, and several api nodes racing to migrate the
// same database on deploy is a problem worth not having.
package main

import (
	"log/slog"
	"os"

	"comms/internal/platform/config"
	"comms/internal/platform/database"
	"comms/internal/platform/logging"
)

func main() {
	logger := logging.New("migrate")

	db, err := database.Open(config.MustEnv("DATABASE_URL"))
	if err != nil {
		logger.Error("open database", slog.Any("error", err))
		os.Exit(1)
	}
	defer db.Close()

	if err := database.Migrate(db, logger); err != nil {
		logger.Error("migrate", slog.Any("error", err))
		os.Exit(1)
	}

	logger.Info("migrations applied")
}
