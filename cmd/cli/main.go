// Command cli is the terminal client.
//
// Messaging only, deliberately: a terminal cannot render video, and the SFU is
// load-tested by a headless harness rather than by this (ADR-0006, and the phase-18
// grilling decision behind it).
//
// It holds the same local SQLite store as the browser client and implements the same
// sync contract (docs/client-sync.md), which is what makes "the CLI and the browser
// give the same answer to the same search" a claim worth testing.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	tea "github.com/charmbracelet/bubbletea"

	"comms/internal/client"
	"comms/internal/platform/config"
)

func main() {
	var (
		apiURL     = flag.String("api", config.EnvOr("COMMS_API", "http://localhost:8080"), "api base URL")
		storePath  = flag.String("store", defaultStorePath(), "local store path")
		handle     = flag.String("handle", "", "handle to sign in as")
		passphrase = flag.String("passphrase", "", "passphrase")
		email      = flag.String("email", "", "email, when registering")
		register   = flag.Bool("register", false, "create the account rather than signing in")
		search     = flag.String("search", "", "search the local store and exit, without connecting")
		syncOnce   = flag.Bool("sync", false, "sync once and exit, without starting the interface")
	)
	flag.Parse()

	if err := run(*apiURL, *storePath, *handle, *passphrase, *email, *register, *search, *syncOnce); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// defaultStorePath puts the store where the platform expects application data.
func defaultStorePath() string {
	if configured := os.Getenv("COMMS_STORE"); configured != "" {
		return configured
	}
	directory, err := os.UserConfigDir()
	if err != nil {
		// A home directory that cannot be determined is not a reason to refuse to
		// start; the current directory is a worse place but a working one.
		return "comms.db"
	}
	return filepath.Join(directory, "comms", "comms.db")
}

func run(apiURL, storePath, handle, passphrase, email string, register bool, search string, syncOnce bool) error {
	if storePath != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(storePath), 0o700); err != nil {
			return fmt.Errorf("create store directory: %w", err)
		}
	}

	store, err := client.OpenStore(storePath)
	if err != nil {
		return err
	}
	defer store.Close()

	ctx := context.Background()

	// Search without connecting, which is the point of having a local store: it works
	// with the network off, and it is the only search this system has (ADR-0001).
	if search != "" {
		return searchLocally(ctx, store, search)
	}

	session, err := signIn(ctx, store, apiURL, handle, passphrase, email, register)
	if err != nil {
		return err
	}

	api := client.NewAPI(apiURL, session, func(rotated client.Session) error {
		return store.SaveSession(ctx, rotated)
	})
	syncer := client.NewSyncer(api, store)

	// Sync once and exit. For scripts and for the cross-client test that asserts this
	// client and the browser answer the same search identically — which needs both to
	// have synced the same conversation without either being driven by hand.
	if syncOnce {
		return syncer.Refresh(ctx)
	}

	program := tea.NewProgram(newModel(ctx, api, store, syncer), tea.WithAltScreen())

	// Syncing runs alongside the UI and pokes it when the store changes. The UI never
	// waits on the network: it renders the store, and the store is filled behind it.
	syncCtx, stopSyncing := context.WithCancel(ctx)
	defer stopSyncing()
	go syncer.Run(syncCtx)
	go func() {
		for {
			select {
			case <-syncCtx.Done():
				return
			case <-syncer.Changed:
				program.Send(storeChanged{})
			case err := <-syncer.Errors:
				program.Send(syncFailed{err})
			}
		}
	}()

	if _, err := program.Run(); err != nil {
		return fmt.Errorf("run interface: %w", err)
	}
	return nil
}

// signIn uses the stored session if there is one, and otherwise the flags.
func signIn(
	ctx context.Context,
	store *client.Store,
	apiURL, handle, passphrase, email string,
	register bool,
) (client.Session, error) {
	stored, found, err := store.LoadSession(ctx)
	if err != nil {
		return client.Session{}, err
	}
	if found && handle == "" {
		return stored, nil
	}

	if handle == "" || passphrase == "" {
		return client.Session{}, errors.New("no stored session: pass -handle and -passphrase")
	}

	var session client.Session
	if register {
		if email == "" {
			return client.Session{}, errors.New("registering needs -email")
		}
		session, err = client.Register(ctx, apiURL, handle, email, passphrase, "cli")
	} else {
		session, err = client.Login(ctx, apiURL, handle, passphrase, "cli")
	}
	if err != nil {
		return client.Session{}, err
	}

	if err := store.SaveSession(ctx, session); err != nil {
		return client.Session{}, err
	}
	return session, nil
}

// searchLocally prints matches and exits. Offline by construction — it never builds an
// API client, so there is nothing to fail when the network is gone.
func searchLocally(ctx context.Context, store *client.Store, query string) error {
	results, err := store.Search(ctx, query, 50)
	if err != nil {
		return err
	}
	if len(results) == 0 {
		fmt.Println("no matches")
		return nil
	}
	for _, result := range results {
		fmt.Printf("%s #%d  %s\n", result.ConversationID[:8], result.Sequence, result.Body)
	}
	return nil
}
