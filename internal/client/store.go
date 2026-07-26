// Package client is the Go client: HTTP calls, the socket protocol, and a local
// SQLite store.
//
// It depends on no bounded context. Everything it knows about the server it knows
// over HTTP, which puts it in exactly the position any third-party client would be
// in — and means the contract in docs/client-sync.md is the only thing shared between
// this and the browser client.
//
// Shared by cmd/cli and, from phase 8, the SFU load harness.
package client

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite" // registers the "sqlite" driver; pure Go, no cgo
)

// Store is a client's local copy of what it has synced.
//
// SQLite rather than a file of JSON, for one reason that matters: search. The server
// cannot search — it never reads message text (ADR-0001) — so every client must, and
// full-text search over a growing corpus is the thing SQLite is for.
//
// The schema mirrors the browser client's deliberately. Both must give the same answer
// to the same query, and the cheapest way to hold two implementations to that is for
// both to run the same SQL against the same shape.
type Store struct {
	db *sql.DB
}

// OpenStore opens or creates a store at path and brings its schema up to date.
//
// ":memory:" works and is what tests use.
func OpenStore(path string) (*Store, error) {
	// WAL so a read (rendering) never blocks behind a write (syncing), and a busy
	// timeout so the two do not race into an error the moment a sync lands while the
	// UI is drawing.
	dsn := path + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)"
	if path == ":memory:" {
		dsn = path
	}

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open local store: %w", err)
	}

	// One connection. SQLite tolerates more, but a local store has one writer by
	// definition and a pool only adds ways for two goroutines to interleave writes.
	db.SetMaxOpenConns(1)

	store := &Store{db: db}
	if err := store.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return store, nil
}

// Close releases the database.
func (s *Store) Close() error {
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("close local store: %w", err)
	}
	return nil
}

// migrations are applied in order and recorded, so a client upgraded across several
// versions catches up in one run.
//
// Client-side migrations are not optional: the store lives on somebody's device and
// outlives any single version of the code. A client that dropped and rebuilt its
// database on every schema change would re-sync the whole history each time, which for
// a phone on mobile data is the difference between an upgrade and an incident.
var migrations = []string{
	`CREATE TABLE conversations (
		id                 TEXT PRIMARY KEY,
		kind               TEXT NOT NULL,
		head               INTEGER NOT NULL DEFAULT 0,
		role               TEXT NOT NULL DEFAULT 'member',
		visible_from       INTEGER NOT NULL DEFAULT 1,
		unread             INTEGER NOT NULL DEFAULT 0,
		-- contiguous is the highest sequence held with nothing missing below it: the
		-- only number safe to resume from (docs/client-sync.md). Persisted, because a
		-- cold start must resume from where the last session got to rather than from
		-- zero.
		contiguous         INTEGER NOT NULL DEFAULT 0,
		others_read        INTEGER NOT NULL DEFAULT 0,
		others_delivered   INTEGER NOT NULL DEFAULT 0,
		label              TEXT NOT NULL DEFAULT '',
		updated_at         TEXT NOT NULL DEFAULT ''
	)`,

	`CREATE TABLE entries (
		conversation_id TEXT NOT NULL,
		sequence        INTEGER NOT NULL,
		id              TEXT NOT NULL,
		author_id       TEXT NOT NULL,
		client_entry_id TEXT NOT NULL,
		kind            TEXT NOT NULL,
		body            TEXT NOT NULL,
		target_sequence INTEGER NOT NULL DEFAULT 0,
		reply_to        INTEGER NOT NULL DEFAULT 0,
		created_at      TEXT NOT NULL,
		PRIMARY KEY (conversation_id, sequence)
	)`,

	// Full-text search over message bodies. The server cannot do this, so the client
	// must, and an external-content table keeps one copy of the text rather than two.
	`CREATE VIRTUAL TABLE entries_fts USING fts5(
		body,
		content = 'entries',
		content_rowid = 'rowid',
		tokenize = 'unicode61 remove_diacritics 2'
	)`,

	// Triggers rather than application code, so a write that forgets to update the
	// index is impossible rather than merely discouraged.
	`CREATE TRIGGER entries_fts_insert AFTER INSERT ON entries BEGIN
		INSERT INTO entries_fts (rowid, body) VALUES (new.rowid, new.body);
	END`,
	`CREATE TRIGGER entries_fts_delete AFTER DELETE ON entries BEGIN
		INSERT INTO entries_fts (entries_fts, rowid, body) VALUES ('delete', old.rowid, old.body);
	END`,
	`CREATE TRIGGER entries_fts_update AFTER UPDATE ON entries BEGIN
		INSERT INTO entries_fts (entries_fts, rowid, body) VALUES ('delete', old.rowid, old.body);
		INSERT INTO entries_fts (rowid, body) VALUES (new.rowid, new.body);
	END`,

	// Pending sends: written before the request goes out, cleared when the server
	// assigns a position. This is what makes an optimistic send survive the app being
	// closed mid-send, and what makes the retry idempotent — the client identifier is
	// held here, not regenerated.
	`CREATE TABLE pending (
		client_entry_id TEXT PRIMARY KEY,
		conversation_id TEXT NOT NULL,
		body            TEXT NOT NULL,
		reply_to        INTEGER NOT NULL DEFAULT 0,
		created_at      TEXT NOT NULL,
		attempts        INTEGER NOT NULL DEFAULT 0
	)`,

	`CREATE TABLE reactions (
		conversation_id TEXT NOT NULL,
		sequence        INTEGER NOT NULL,
		account_id      TEXT NOT NULL,
		emoji           TEXT NOT NULL,
		PRIMARY KEY (conversation_id, sequence, account_id, emoji)
	)`,

	`CREATE INDEX entries_by_conversation ON entries (conversation_id, sequence)`,
}

func (s *Store) migrate() error {
	if _, err := s.db.Exec(`CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)`); err != nil {
		return fmt.Errorf("create schema_version: %w", err)
	}

	var applied int
	err := s.db.QueryRow(`SELECT coalesce(max(version), 0) FROM schema_version`).Scan(&applied)
	if err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}

	for index := applied; index < len(migrations); index++ {
		transaction, err := s.db.Begin()
		if err != nil {
			return fmt.Errorf("begin migration %d: %w", index+1, err)
		}
		if _, err := transaction.Exec(migrations[index]); err != nil {
			_ = transaction.Rollback()
			return fmt.Errorf("apply migration %d: %w", index+1, err)
		}
		if _, err := transaction.Exec(`INSERT INTO schema_version (version) VALUES (?)`, index+1); err != nil {
			_ = transaction.Rollback()
			return fmt.Errorf("record migration %d: %w", index+1, err)
		}
		if err := transaction.Commit(); err != nil {
			return fmt.Errorf("commit migration %d: %w", index+1, err)
		}
	}
	return nil
}

// --- conversations ---

// LocalConversation is a conversation as the client holds it.
type LocalConversation struct {
	ID              string
	Kind            string
	Head            int64
	Role            string
	VisibleFrom     int64
	Unread          int64
	Contiguous      int64
	OthersRead      int64
	OthersDelivered int64
	Label           string
}

// SaveConversation upserts what the server said about a conversation.
//
// contiguous is deliberately not written here: it is the client's own bookkeeping and
// the server has no opinion about it. Overwriting it from a server response would
// discard the client's knowledge of what it holds.
func (s *Store) SaveConversation(ctx context.Context, conversation LocalConversation) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO conversations
		     (id, kind, head, role, visible_from, unread, others_read, others_delivered, label, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
		 ON CONFLICT (id) DO UPDATE SET
		     kind = excluded.kind,
		     head = max(conversations.head, excluded.head),
		     role = excluded.role,
		     visible_from = excluded.visible_from,
		     unread = excluded.unread,
		     others_read = excluded.others_read,
		     others_delivered = excluded.others_delivered,
		     label = CASE WHEN excluded.label != '' THEN excluded.label ELSE conversations.label END,
		     updated_at = datetime('now')`,
		conversation.ID, conversation.Kind, conversation.Head, conversation.Role,
		conversation.VisibleFrom, conversation.Unread, conversation.OthersRead,
		conversation.OthersDelivered, conversation.Label,
	)
	if err != nil {
		return fmt.Errorf("save conversation: %w", err)
	}
	return nil
}

// Conversations returns what is held, most recently active first.
func (s *Store) Conversations(ctx context.Context) ([]LocalConversation, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, kind, head, role, visible_from, unread, contiguous, others_read, others_delivered, label
		   FROM conversations ORDER BY head DESC, id`)
	if err != nil {
		return nil, fmt.Errorf("list conversations: %w", err)
	}
	defer rows.Close()

	var conversations []LocalConversation
	for rows.Next() {
		var conversation LocalConversation
		if err := rows.Scan(&conversation.ID, &conversation.Kind, &conversation.Head,
			&conversation.Role, &conversation.VisibleFrom, &conversation.Unread,
			&conversation.Contiguous, &conversation.OthersRead, &conversation.OthersDelivered,
			&conversation.Label); err != nil {
			return nil, fmt.Errorf("scan conversation: %w", err)
		}
		conversations = append(conversations, conversation)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read conversations: %w", err)
	}
	return conversations, nil
}

// Cursor returns the contiguous mark per conversation — the resume frame's contents.
func (s *Store) Cursor(ctx context.Context) (map[string]int64, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, contiguous FROM conversations`)
	if err != nil {
		return nil, fmt.Errorf("read cursor: %w", err)
	}
	defer rows.Close()

	cursor := map[string]int64{}
	for rows.Next() {
		var (
			id         string
			contiguous int64
		)
		if err := rows.Scan(&id, &contiguous); err != nil {
			return nil, fmt.Errorf("scan cursor: %w", err)
		}
		cursor[id] = contiguous
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read cursor: %w", err)
	}
	return cursor, nil
}

// --- entries ---

// LocalEntry is an entry as the client holds it. Bodies are stored decoded, because
// the client is where they are read and search needs text rather than base64.
type LocalEntry struct {
	ConversationID string
	Sequence       int64
	ID             string
	AuthorID       string
	ClientEntryID  string
	Kind           string
	Body           string
	TargetSequence int64
	ReplyTo        int64
	CreatedAt      string
}

// SaveEntries stores entries and advances the contiguous mark, in one transaction.
//
// One transaction for both, and it matters: a mark saved without its entries would
// make the client resume past messages it does not hold, permanently. That is the
// persistent form of the bug docs/client-sync.md warns about, and the only place it
// can be introduced is here.
func (s *Store) SaveEntries(ctx context.Context, conversationID string, entries []LocalEntry) error {
	if len(entries) == 0 {
		return nil
	}

	transaction, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin save entries: %w", err)
	}
	defer func() { _ = transaction.Rollback() }()

	for _, entry := range entries {
		if _, err := transaction.ExecContext(ctx,
			`INSERT INTO entries
			     (conversation_id, sequence, id, author_id, client_entry_id, kind, body, target_sequence, reply_to, created_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			 ON CONFLICT (conversation_id, sequence) DO NOTHING`,
			entry.ConversationID, entry.Sequence, entry.ID, entry.AuthorID, entry.ClientEntryID,
			entry.Kind, entry.Body, entry.TargetSequence, entry.ReplyTo, entry.CreatedAt,
		); err != nil {
			return fmt.Errorf("insert entry %d: %w", entry.Sequence, err)
		}

		// A stored entry is one the client no longer has pending. Cleared by client
		// identifier, so an entry that arrives over the socket before its own send
		// response is still reconciled.
		if _, err := transaction.ExecContext(ctx,
			`DELETE FROM pending WHERE client_entry_id = ?`, entry.ClientEntryID); err != nil {
			return fmt.Errorf("clear pending: %w", err)
		}
	}

	if err := advanceContiguous(ctx, transaction, conversationID); err != nil {
		return err
	}

	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit save entries: %w", err)
	}
	return nil
}

// advanceContiguous walks the mark forward over what is now held.
//
// Recomputed from the rows rather than incremented, so it cannot drift: whatever the
// arrival order was, the answer is a fact about the table.
func advanceContiguous(ctx context.Context, transaction *sql.Tx, conversationID string) error {
	var (
		mark        int64
		visibleFrom int64
	)
	err := transaction.QueryRowContext(ctx,
		`SELECT contiguous, visible_from FROM conversations WHERE id = ?`, conversationID).
		Scan(&mark, &visibleFrom)
	if errors.Is(err, sql.ErrNoRows) {
		// Entries for a conversation the client has no row for yet. The mark is set
		// when the conversation arrives; nothing to advance.
		return nil
	}
	if err != nil {
		return fmt.Errorf("read contiguous mark: %w", err)
	}

	// Positions below the join point are not this member's to hold, so the mark starts
	// at the point before it rather than at zero — the same rule the browser client
	// applies when a fetch skips positions.
	if mark < visibleFrom-1 {
		mark = visibleFrom - 1
	}

	for {
		var held bool
		if err := transaction.QueryRowContext(ctx,
			`SELECT EXISTS (SELECT 1 FROM entries WHERE conversation_id = ? AND sequence = ?)`,
			conversationID, mark+1).Scan(&held); err != nil {
			return fmt.Errorf("check held: %w", err)
		}
		if !held {
			break
		}
		mark++
	}

	if _, err := transaction.ExecContext(ctx,
		`UPDATE conversations SET contiguous = ?, head = max(head, ?) WHERE id = ?`,
		mark, mark, conversationID); err != nil {
		return fmt.Errorf("save contiguous mark: %w", err)
	}
	return nil
}

// CloseOver moves the mark over positions the server will never send.
//
// The other rule from docs/client-sync.md: a fetch that skips positions has told the
// client those positions are not its to see. Without it, a member who joined late
// treats the history before their join point as a permanent gap.
func (s *Store) CloseOver(ctx context.Context, conversationID string, through int64) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE conversations SET contiguous = max(contiguous, ?) WHERE id = ?`,
		through, conversationID)
	if err != nil {
		return fmt.Errorf("close over positions: %w", err)
	}
	return nil
}

// Entries returns a conversation's entries in order.
func (s *Store) Entries(ctx context.Context, conversationID string) ([]LocalEntry, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT conversation_id, sequence, id, author_id, client_entry_id, kind, body, target_sequence, reply_to, created_at
		   FROM entries WHERE conversation_id = ? ORDER BY sequence`, conversationID)
	if err != nil {
		return nil, fmt.Errorf("list entries: %w", err)
	}
	defer rows.Close()

	return scanEntries(rows)
}

func scanEntries(rows *sql.Rows) ([]LocalEntry, error) {
	var entries []LocalEntry
	for rows.Next() {
		var entry LocalEntry
		if err := rows.Scan(&entry.ConversationID, &entry.Sequence, &entry.ID, &entry.AuthorID,
			&entry.ClientEntryID, &entry.Kind, &entry.Body, &entry.TargetSequence,
			&entry.ReplyTo, &entry.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan entry: %w", err)
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read entries: %w", err)
	}
	return entries, nil
}

// --- search ---

// SearchResult is one hit.
type SearchResult struct {
	ConversationID string
	Sequence       int64
	Body           string
}

// Search finds entries matching a query, most recent first.
//
// The server cannot do this: it never reads message text (ADR-0001). So search is a
// client feature, and both clients must give the same answer — the browser client runs
// the same query against the same schema, which is why the two are written to match
// rather than each being idiomatic for its platform.
func (s *Store) Search(ctx context.Context, query string, limit int) ([]SearchResult, error) {
	term := ftsQuery(query)
	if term == "" {
		return nil, nil
	}
	if limit <= 0 {
		limit = 50
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT e.conversation_id, e.sequence, e.body
		   FROM entries_fts f
		   JOIN entries e ON e.rowid = f.rowid
		  WHERE entries_fts MATCH ?
		    -- Retractions carry no text, and an entry whose content was withdrawn must
		    -- not be findable by what it used to say. The row is deleted from the index
		    -- when the retraction is applied; this excludes the amendments themselves,
		    -- which are not messages.
		    AND e.kind = 'message'
		  ORDER BY e.sequence DESC
		  LIMIT ?`,
		term, limit)
	if err != nil {
		return nil, fmt.Errorf("search: %w", err)
	}
	defer rows.Close()

	var results []SearchResult
	for rows.Next() {
		var result SearchResult
		if err := rows.Scan(&result.ConversationID, &result.Sequence, &result.Body); err != nil {
			return nil, fmt.Errorf("scan result: %w", err)
		}
		results = append(results, result)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read results: %w", err)
	}
	return results, nil
}

// ftsQuery turns what somebody typed into an FTS5 expression.
//
// Quoted term by term, then prefix-matched on the last one. Two reasons this is not
// the raw string: FTS5's syntax would otherwise be exposed, so a query containing a
// quote or a NEAR is a syntax error rather than a search; and prefix matching on the
// final term is what makes results appear while typing rather than after.
//
// Both clients implement this identically. It is the part of "same search semantics"
// that is a decision rather than a query.
func ftsQuery(raw string) string {
	var terms []string
	for _, word := range strings.Fields(raw) {
		// Doubling internal quotes is FTS5's escape, and the only one needed inside a
		// quoted term.
		cleaned := strings.ReplaceAll(word, `"`, `""`)
		if cleaned == "" {
			continue
		}
		terms = append(terms, `"`+cleaned+`"`)
	}
	if len(terms) == 0 {
		return ""
	}
	// Prefix on the last term only: "hello wor" should find "hello world" while
	// prefixing every term would make "a b" match far too much.
	terms[len(terms)-1] += `*`
	return strings.Join(terms, " AND ")
}

// --- amendments ---

// ApplyAmendment records a revision or retraction against its target.
//
// The client keeps both the amendment and the amended entry: the amendment occupies a
// real position that sync must pass through, and the original's row is what search and
// rendering read. So this updates the target's body in place *locally* — which is not a
// contradiction of ADR-0008, because the log is on the server and this is a cache of
// what the log currently means.
func (s *Store) ApplyAmendment(ctx context.Context, conversationID string, target int64, kind, body string) error {
	switch kind {
	case "revision":
		if _, err := s.db.ExecContext(ctx,
			`UPDATE entries SET body = ? WHERE conversation_id = ? AND sequence = ?`,
			body, conversationID, target); err != nil {
			return fmt.Errorf("apply revision: %w", err)
		}
	case "retraction":
		// Emptied rather than deleted: the position stays held so the contiguous mark
		// does not develop a hole, and the trigger removes the old text from the search
		// index — a withdrawn message must not be findable by what it used to say.
		if _, err := s.db.ExecContext(ctx,
			`UPDATE entries SET body = '', kind = 'retracted' WHERE conversation_id = ? AND sequence = ?`,
			conversationID, target); err != nil {
			return fmt.Errorf("apply retraction: %w", err)
		}
	}
	return nil
}

// --- pending sends ---

// Pending is a send that has not been acknowledged.
type Pending struct {
	ClientEntryID  string
	ConversationID string
	Body           string
	ReplyTo        int64
	Attempts       int
}

// AddPending records a send before it is attempted.
//
// Written first, deliberately. A send that goes out before being recorded is a send
// that is lost if the process dies waiting for the response — and worse, one whose
// client identifier is gone, so the retry would create a second entry rather than
// being recognised as the same one (MS-2).
func (s *Store) AddPending(ctx context.Context, pending Pending) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO pending (client_entry_id, conversation_id, body, reply_to, created_at)
		 VALUES (?, ?, ?, ?, datetime('now'))
		 ON CONFLICT (client_entry_id) DO UPDATE SET attempts = pending.attempts + 1`,
		pending.ClientEntryID, pending.ConversationID, pending.Body, pending.ReplyTo)
	if err != nil {
		return fmt.Errorf("add pending: %w", err)
	}
	return nil
}

// PendingSends returns what has not been acknowledged, oldest first.
func (s *Store) PendingSends(ctx context.Context) ([]Pending, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT client_entry_id, conversation_id, body, reply_to, attempts
		   FROM pending ORDER BY created_at`)
	if err != nil {
		return nil, fmt.Errorf("list pending: %w", err)
	}
	defer rows.Close()

	var sends []Pending
	for rows.Next() {
		var pending Pending
		if err := rows.Scan(&pending.ClientEntryID, &pending.ConversationID, &pending.Body,
			&pending.ReplyTo, &pending.Attempts); err != nil {
			return nil, fmt.Errorf("scan pending: %w", err)
		}
		sends = append(sends, pending)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read pending: %w", err)
	}
	return sends, nil
}

// ClearPending removes an acknowledged send.
func (s *Store) ClearPending(ctx context.Context, clientEntryID string) error {
	if _, err := s.db.ExecContext(ctx,
		`DELETE FROM pending WHERE client_entry_id = ?`, clientEntryID); err != nil {
		return fmt.Errorf("clear pending: %w", err)
	}
	return nil
}

// --- reactions ---

// SetReaction records or removes one account's reaction.
func (s *Store) SetReaction(ctx context.Context, conversationID string, sequence int64, accountID, emoji string, removed bool) error {
	if removed {
		_, err := s.db.ExecContext(ctx,
			`DELETE FROM reactions WHERE conversation_id = ? AND sequence = ? AND account_id = ? AND emoji = ?`,
			conversationID, sequence, accountID, emoji)
		if err != nil {
			return fmt.Errorf("remove reaction: %w", err)
		}
		return nil
	}

	_, err := s.db.ExecContext(ctx,
		`INSERT INTO reactions (conversation_id, sequence, account_id, emoji) VALUES (?, ?, ?, ?)
		 ON CONFLICT DO NOTHING`,
		conversationID, sequence, accountID, emoji)
	if err != nil {
		return fmt.Errorf("add reaction: %w", err)
	}
	return nil
}

// ReactionCounts returns emoji to holder count for one conversation.
func (s *Store) ReactionCounts(ctx context.Context, conversationID string) (map[int64]map[string]int, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT sequence, emoji, count(*) FROM reactions WHERE conversation_id = ?
		  GROUP BY sequence, emoji`, conversationID)
	if err != nil {
		return nil, fmt.Errorf("count reactions: %w", err)
	}
	defer rows.Close()

	counts := map[int64]map[string]int{}
	for rows.Next() {
		var (
			sequence int64
			emoji    string
			count    int
		)
		if err := rows.Scan(&sequence, &emoji, &count); err != nil {
			return nil, fmt.Errorf("scan reaction count: %w", err)
		}
		if counts[sequence] == nil {
			counts[sequence] = map[string]int{}
		}
		counts[sequence][emoji] = count
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read reaction counts: %w", err)
	}
	return counts, nil
}

// --- session ---

// SaveSession persists the token pair so a restart does not require signing in again.
//
// In the same database as everything else. It is a secret at rest on a device that
// already holds every message the account can read, so a separate store would protect
// nothing while adding a file to keep in step.
func (s *Store) SaveSession(ctx context.Context, session Session) error {
	if _, err := s.db.ExecContext(ctx,
		`CREATE TABLE IF NOT EXISTS session (
			singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
			account_id TEXT NOT NULL, handle TEXT NOT NULL, device_id TEXT NOT NULL,
			access_token TEXT NOT NULL, access_expires_at TEXT NOT NULL,
			refresh_token TEXT NOT NULL, refresh_expires_at TEXT NOT NULL
		)`); err != nil {
		return fmt.Errorf("create session table: %w", err)
	}

	_, err := s.db.ExecContext(ctx,
		`INSERT INTO session (singleton, account_id, handle, device_id, access_token, access_expires_at, refresh_token, refresh_expires_at)
		 VALUES (1, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT (singleton) DO UPDATE SET
		     account_id = excluded.account_id, handle = excluded.handle, device_id = excluded.device_id,
		     access_token = excluded.access_token, access_expires_at = excluded.access_expires_at,
		     refresh_token = excluded.refresh_token, refresh_expires_at = excluded.refresh_expires_at`,
		session.Account.ID, session.Account.Handle, session.DeviceID,
		session.AccessToken, session.AccessExpiresAt.Format(time.RFC3339Nano),
		session.RefreshToken, session.RefreshExpiresAt.Format(time.RFC3339Nano))
	if err != nil {
		return fmt.Errorf("save session: %w", err)
	}
	return nil
}

// LoadSession returns the stored session, or false if there is none.
func (s *Store) LoadSession(ctx context.Context) (Session, bool, error) {
	var (
		session          Session
		accessExpiresAt  string
		refreshExpiresAt string
	)
	err := s.db.QueryRowContext(ctx,
		`SELECT account_id, handle, device_id, access_token, access_expires_at, refresh_token, refresh_expires_at
		   FROM session WHERE singleton = 1`).
		Scan(&session.Account.ID, &session.Account.Handle, &session.DeviceID,
			&session.AccessToken, &accessExpiresAt, &session.RefreshToken, &refreshExpiresAt)
	if err != nil {
		// A missing table is a store that has never held a session, which is not an
		// error — it is a first run.
		if errors.Is(err, sql.ErrNoRows) || strings.Contains(err.Error(), "no such table") {
			return Session{}, false, nil
		}
		return Session{}, false, fmt.Errorf("load session: %w", err)
	}

	session.AccessExpiresAt, _ = time.Parse(time.RFC3339Nano, accessExpiresAt)
	session.RefreshExpiresAt, _ = time.Parse(time.RFC3339Nano, refreshExpiresAt)
	return session, true, nil
}
