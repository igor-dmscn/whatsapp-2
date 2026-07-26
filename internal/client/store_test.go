// Package client_test covers the local store: the contiguous mark across restarts,
// pending sends, and search.
//
// No infrastructure. SQLite in memory, so these always run — which matters because the
// mark is the one piece of client state whose corruption loses messages permanently.
package client_test

import (
	"context"
	"testing"

	"comms/internal/client"
)

func newStore(t *testing.T) *client.Store {
	t.Helper()

	store, err := client.OpenStore(":memory:")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

// conversation saves a conversation with sensible defaults.
func conversation(t *testing.T, store *client.Store, id string, visibleFrom int64) {
	t.Helper()

	err := store.SaveConversation(context.Background(), client.LocalConversation{
		ID: id, Kind: "direct", Role: "member", VisibleFrom: visibleFrom,
	})
	if err != nil {
		t.Fatalf("save conversation: %v", err)
	}
}

func entries(t *testing.T, store *client.Store, conversationID string, sequences ...int64) {
	t.Helper()

	local := make([]client.LocalEntry, 0, len(sequences))
	for _, sequence := range sequences {
		local = append(local, client.LocalEntry{
			ConversationID: conversationID,
			Sequence:       sequence,
			ID:             "entry",
			AuthorID:       "account-1",
			ClientEntryID:  "client-" + string(rune('a'+sequence)),
			Kind:           "message",
			Body:           "message body",
			CreatedAt:      "2026-01-01T00:00:00Z",
		})
	}
	if err := store.SaveEntries(context.Background(), conversationID, local); err != nil {
		t.Fatalf("save entries: %v", err)
	}
}

func markOf(t *testing.T, store *client.Store, conversationID string) int64 {
	t.Helper()

	cursor, err := store.Cursor(context.Background())
	if err != nil {
		t.Fatalf("cursor: %v", err)
	}
	return cursor[conversationID]
}

func TestTheMarkAdvancesOnlyOverContiguousEntries(t *testing.T) {
	// The rule the whole protocol rests on, persisted. Holding 1, 2 and 7, the mark is
	// 2 — because resuming at 7 would abandon 3 through 6 for good, and unlike the
	// in-memory case that loss survives a restart.
	store := newStore(t)
	conversation(t, store, "conversation-1", 1)

	entries(t, store, "conversation-1", 1, 2, 7)

	if got := markOf(t, store, "conversation-1"); got != 2 {
		t.Errorf("mark = %d, want 2 — it advanced over a hole", got)
	}

	// Filling the hole carries the mark past what was already held above it.
	entries(t, store, "conversation-1", 3, 4, 5, 6)
	if got := markOf(t, store, "conversation-1"); got != 7 {
		t.Errorf("mark = %d after filling the gap, want 7", got)
	}
}

func TestTheMarkStartsAtTheJoinPointRatherThanZero(t *testing.T) {
	// A member who joined at position 40 will never be sent 1 to 39. Without this the
	// mark stays at zero, every later entry looks like a hole, and the client refetches
	// for the life of the session.
	store := newStore(t)
	conversation(t, store, "conversation-1", 40)

	entries(t, store, "conversation-1", 40, 41)

	if got := markOf(t, store, "conversation-1"); got != 41 {
		t.Errorf("mark = %d, want 41 — the join point was not respected", got)
	}
}

func TestClosingOverPositionsMovesTheMarkPastWhatWillNeverArrive(t *testing.T) {
	store := newStore(t)
	conversation(t, store, "conversation-1", 1)

	if err := store.CloseOver(context.Background(), "conversation-1", 39); err != nil {
		t.Fatalf("close over: %v", err)
	}
	entries(t, store, "conversation-1", 40)

	if got := markOf(t, store, "conversation-1"); got != 40 {
		t.Errorf("mark = %d, want 40", got)
	}
}

func TestTheMarkNeverMovesBackwards(t *testing.T) {
	store := newStore(t)
	conversation(t, store, "conversation-1", 1)
	entries(t, store, "conversation-1", 1, 2, 3)

	// A conversation refreshed from the server must not reset the client's own
	// bookkeeping: the server has no opinion about what this device holds.
	conversation(t, store, "conversation-1", 1)

	if got := markOf(t, store, "conversation-1"); got != 3 {
		t.Errorf("mark = %d after a server refresh, want 3 — the server overwrote local state", got)
	}
}

func TestStoringAnEntryTwiceIsOneEntry(t *testing.T) {
	store := newStore(t)
	conversation(t, store, "conversation-1", 1)

	entries(t, store, "conversation-1", 1)
	entries(t, store, "conversation-1", 1)

	held, err := store.Entries(context.Background(), "conversation-1")
	if err != nil {
		t.Fatalf("entries: %v", err)
	}
	if len(held) != 1 {
		t.Errorf("held %d entries after storing the same one twice, want 1", len(held))
	}
}

// --- pending sends ---

func TestAPendingSendSurvivesUntilItIsAcknowledged(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()
	conversation(t, store, "conversation-1", 1)

	err := store.AddPending(ctx, client.Pending{
		ClientEntryID: "client-1", ConversationID: "conversation-1", Body: "sent while offline",
	})
	if err != nil {
		t.Fatalf("add pending: %v", err)
	}

	pending, err := store.PendingSends(ctx)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(pending) != 1 || pending[0].Body != "sent while offline" {
		t.Fatalf("pending = %+v, want one held send", pending)
	}

	// The entry arriving is what clears it — matched by client identifier, so the
	// socket echo reconciles it even if the send's own response was lost. That is what
	// makes "sent while offline, arrives exactly once" true.
	if err := store.SaveEntries(ctx, "conversation-1", []client.LocalEntry{{
		ConversationID: "conversation-1", Sequence: 1, ID: "entry-1", AuthorID: "account-1",
		ClientEntryID: "client-1", Kind: "message", Body: "sent while offline",
		CreatedAt: "2026-01-01T00:00:00Z",
	}}); err != nil {
		t.Fatalf("save entries: %v", err)
	}

	pending, err = store.PendingSends(ctx)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(pending) != 0 {
		t.Errorf("pending = %+v after the entry arrived, want none", pending)
	}
}

func TestRetryingAPendingSendKeepsItsIdentifier(t *testing.T) {
	// The identifier is what makes the retry idempotent (MS-2). Regenerating it would
	// turn one message into two.
	store := newStore(t)
	ctx := context.Background()

	for range 3 {
		err := store.AddPending(ctx, client.Pending{
			ClientEntryID: "client-1", ConversationID: "conversation-1", Body: "one message",
		})
		if err != nil {
			t.Fatalf("add pending: %v", err)
		}
	}

	pending, err := store.PendingSends(ctx)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("pending = %d rows for three attempts, want 1", len(pending))
	}
	if pending[0].Attempts != 2 {
		t.Errorf("attempts = %d, want 2 — the count is what makes a stuck send visible", pending[0].Attempts)
	}
}

// --- search ---

func searchable(t *testing.T, store *client.Store, bodies ...string) {
	t.Helper()

	conversation(t, store, "conversation-1", 1)
	local := make([]client.LocalEntry, 0, len(bodies))
	for index, body := range bodies {
		local = append(local, client.LocalEntry{
			ConversationID: "conversation-1", Sequence: int64(index + 1),
			ID: "entry", AuthorID: "account-1", ClientEntryID: "client",
			Kind: "message", Body: body, CreatedAt: "2026-01-01T00:00:00Z",
		})
	}
	if err := store.SaveEntries(context.Background(), "conversation-1", local); err != nil {
		t.Fatalf("save entries: %v", err)
	}
}

func found(t *testing.T, store *client.Store, query string) []string {
	t.Helper()

	results, err := store.Search(context.Background(), query, 50)
	if err != nil {
		t.Fatalf("search %q: %v", query, err)
	}
	bodies := make([]string, 0, len(results))
	for _, result := range results {
		bodies = append(bodies, result.Body)
	}
	return bodies
}

func TestSearchFindsWholeWordsAndPrefixes(t *testing.T) {
	store := newStore(t)
	searchable(t, store, "the quick brown fox", "a slow green turtle", "quicksand everywhere")

	if got := found(t, store, "quick"); len(got) != 2 {
		t.Errorf("search for quick found %v, want the fox and the quicksand — the last term is a prefix", got)
	}
	if got := found(t, store, "brown fox"); len(got) != 1 {
		t.Errorf("search for two words found %v, want one — terms are ANDed", got)
	}
	if got := found(t, store, "fox brown"); len(got) != 1 {
		t.Errorf("search is order-sensitive: %v", got)
	}
}

func TestSearchIgnoresDiacriticsAndCase(t *testing.T) {
	// Both clients configure the tokeniser the same way. Somebody searching "cafe"
	// expecting "café" is not a corner case, it is most people.
	store := newStore(t)
	searchable(t, store, "meet at the café", "CAFETERIA opens late")

	if got := found(t, store, "cafe"); len(got) != 2 {
		t.Errorf("search for cafe found %v, want both", got)
	}
	if got := found(t, store, "CAFÉ"); len(got) != 2 {
		t.Errorf("search is case sensitive: %v", got)
	}
}

func TestSearchTreatsPunctuationAsTextRatherThanSyntax(t *testing.T) {
	// FTS5's own syntax must never leak through. A query containing a quote, a NEAR or
	// a colon is somebody searching for those characters, not writing an expression —
	// and a syntax error where a result was expected is indistinguishable from "no
	// results" to the person typing.
	store := newStore(t)
	searchable(t, store, `she said "hello" loudly`, "NEAR the station", "ratio 3:1")

	for _, query := range []string{`"hello"`, "NEAR", "3:1", "AND", "*", "((", `"`} {
		if _, err := store.Search(context.Background(), query, 10); err != nil {
			t.Errorf("search %q returned an error: %v", query, err)
		}
	}

	if got := found(t, store, `hello`); len(got) != 1 {
		t.Errorf("search for hello found %v, want the quoted one", got)
	}
}

func TestSearchFindsNothingForAnEmptyQuery(t *testing.T) {
	store := newStore(t)
	searchable(t, store, "something")

	if got := found(t, store, "   "); len(got) != 0 {
		t.Errorf("blank search found %v, want nothing", got)
	}
}

func TestARetractedMessageIsNotFindableByWhatItSaid(t *testing.T) {
	// The consequence of a delete-for-everyone that would otherwise be missed. A
	// withdrawn message that still turns up in search has not been withdrawn.
	store := newStore(t)
	ctx := context.Background()
	searchable(t, store, "the regretted message")

	if got := found(t, store, "regretted"); len(got) != 1 {
		t.Fatalf("search found %v before the retraction, want one", got)
	}

	if err := store.ApplyAmendment(ctx, "conversation-1", 1, "retraction", ""); err != nil {
		t.Fatalf("apply retraction: %v", err)
	}

	if got := found(t, store, "regretted"); len(got) != 0 {
		t.Errorf("search found %v after the retraction — withdrawn text is still indexed", got)
	}
}

func TestAnEditedMessageIsFindableByItsNewTextOnly(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()
	searchable(t, store, "the original wording")

	if err := store.ApplyAmendment(ctx, "conversation-1", 1, "revision", "the corrected wording"); err != nil {
		t.Fatalf("apply revision: %v", err)
	}

	if got := found(t, store, "corrected"); len(got) != 1 {
		t.Errorf("search for the new wording found %v, want one", got)
	}
	if got := found(t, store, "original"); len(got) != 0 {
		t.Errorf("search for the replaced wording found %v, want none", got)
	}
}

// --- migrations ---

func TestOpeningAnExistingStoreAppliesOnlyNewMigrations(t *testing.T) {
	// The store lives on somebody's device and outlives any version of the code. A
	// client that rebuilt its database on every schema change would re-sync the whole
	// history each time — on mobile data, the difference between an upgrade and an
	// incident.
	path := t.TempDir() + "/comms.db"

	first, err := client.OpenStore(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	conversation(t, first, "conversation-1", 1)
	entries(t, first, "conversation-1", 1, 2)
	if err := first.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	second, err := client.OpenStore(path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer second.Close()

	if got := markOf(t, second, "conversation-1"); got != 2 {
		t.Errorf("mark = %d after reopening, want 2 — a cold start must resume where it left off", got)
	}
	held, err := second.Entries(context.Background(), "conversation-1")
	if err != nil {
		t.Fatalf("entries: %v", err)
	}
	if len(held) != 2 {
		t.Errorf("held %d entries after reopening, want 2", len(held))
	}

	// And search works against the reopened index, which is the part a rebuilt-on-open
	// schema would silently lose.
	if got := found(t, second, "message"); len(got) != 2 {
		t.Errorf("search after reopening found %v, want both", got)
	}
}
