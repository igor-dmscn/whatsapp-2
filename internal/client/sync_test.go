package client_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"comms/internal/client"
	"comms/internal/platform/id"
)

// apiURL skips these tests when no server is running.
//
// The same bargain as testdb: a test needing infrastructure must not fail on a machine
// without it, and must not silently pass either. `make e2e` provides the server.
func apiURL(t *testing.T) string {
	t.Helper()

	configured := os.Getenv("COMMS_API")
	if configured == "" {
		t.Skip("COMMS_API not set, skipping integration test")
	}
	return configured
}

// person is a registered account with its own store and syncer.
type person struct {
	api    *client.API
	store  *client.Store
	syncer *client.Syncer
}

func newPerson(t *testing.T, baseURL, handle string) *person {
	t.Helper()

	ctx := context.Background()
	session, err := client.Register(ctx, baseURL, handle, handle+"@example.test",
		"correct horse battery staple", "test")
	if err != nil {
		t.Fatalf("register %s: %v", handle, err)
	}

	store, err := client.OpenStore(t.TempDir() + "/" + handle + ".db")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	if err := store.SaveSession(ctx, session); err != nil {
		t.Fatalf("save session: %v", err)
	}

	api := client.NewAPI(baseURL, session, func(rotated client.Session) error {
		return store.SaveSession(ctx, rotated)
	})
	return &person{api: api, store: store, syncer: client.NewSyncer(api, store)}
}

// unique keeps handles from colliding across runs, since registration is permanent.
//
// The *tail* of a UUIDv7, not the head. The leading bits are a millisecond timestamp,
// so two tests starting in the same millisecond produced the same prefix and the second
// one failed with handle_taken — which is the same property that makes v7 the wrong
// choice for an invite token, met from the other direction.
func unique() string {
	raw := strings.ReplaceAll(id.New(), "-", "")
	return raw[len(raw)-10:]
}

// waitFor polls until a condition holds, which is how eventual consistency is asserted
// without sleeping for a guessed duration.
func waitFor(t *testing.T, what string, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestTheStoreHoldsWhatWasSyncedAcrossARestart(t *testing.T) {
	// NF-5's premise: a cold start renders from the local store. This asserts the store
	// actually holds a conversation and its entries after the process that synced them
	// has gone, which is what makes rendering before connecting possible at all.
	baseURL := apiURL(t)
	ctx := context.Background()

	suffix := unique()
	ana := newPerson(t, baseURL, "ana"+suffix)
	bruno := newPerson(t, baseURL, "bruno"+suffix)

	conversation, err := ana.api.StartDirect(ctx, bruno.api.AccountID())
	if err != nil {
		t.Fatalf("start direct: %v", err)
	}
	for _, text := range []string{"first", "second", "third"} {
		if _, err := ana.api.Send(ctx, conversation.ID, id.New(), text, 0); err != nil {
			t.Fatalf("send: %v", err)
		}
	}

	// Bruno syncs, with no socket involved: Refresh is the catch-up path, and a cold
	// start runs it before connecting.
	if err := bruno.syncer.Refresh(ctx); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	entries, err := bruno.store.Entries(ctx, conversation.ID)
	if err != nil {
		t.Fatalf("entries: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("held %d entries, want 3", len(entries))
	}

	cursor, err := bruno.store.Cursor(ctx)
	if err != nil {
		t.Fatalf("cursor: %v", err)
	}
	if cursor[conversation.ID] != 3 {
		t.Errorf("mark = %d, want 3", cursor[conversation.ID])
	}

	// And search works against what was synced, offline — nothing here touches the API.
	results, err := bruno.store.Search(ctx, "second", 10)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(results) != 1 || results[0].Body != "second" {
		t.Errorf("search found %+v, want the second message", results)
	}
}

func TestSendingWhileTheServerIsUnreachableArrivesExactlyOnceLater(t *testing.T) {
	// The plan's verification, and the reason a pending send is written before it is
	// attempted. Offline is simulated by pointing the client at a port nothing answers,
	// which is a truer test than mocking the transport: the failure arrives the way a
	// real one does.
	baseURL := apiURL(t)
	ctx := context.Background()

	suffix := unique()
	ana := newPerson(t, baseURL, "ana"+suffix)
	bruno := newPerson(t, baseURL, "bruno"+suffix)

	conversation, err := ana.api.StartDirect(ctx, bruno.api.AccountID())
	if err != nil {
		t.Fatalf("start direct: %v", err)
	}

	// A syncer that cannot reach anything. Its store is ana's, so what it records is
	// what ana's real client will flush.
	offline := client.NewSyncer(
		client.NewAPI("http://127.0.0.1:1", ana.api.Session(), nil),
		ana.store,
	)

	clientEntryID := id.New()
	if err := offline.Send(ctx, conversation.ID, clientEntryID, "sent while offline", 0); err == nil {
		t.Fatal("send succeeded against an unreachable server")
	}

	pending, err := ana.store.PendingSends(ctx)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(pending) != 1 || pending[0].ClientEntryID != clientEntryID {
		t.Fatalf("pending = %+v, want the failed send held with its identifier", pending)
	}

	// Back online. Run flushes pending before it connects.
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	go ana.syncer.Run(runCtx)

	waitFor(t, "the pending send to flush", func() bool {
		held, err := ana.store.PendingSends(ctx)
		return err == nil && len(held) == 0
	})

	// Exactly one entry, with one position. Two would mean the identifier was not
	// carried through the retry.
	if err := bruno.syncer.Refresh(ctx); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	entries, err := bruno.store.Entries(ctx, conversation.ID)
	if err != nil {
		t.Fatalf("entries: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("recipient holds %d entries, want exactly 1", len(entries))
	}
	if entries[0].Body != "sent while offline" {
		t.Errorf("body = %q", entries[0].Body)
	}
}

func TestALiveEntryArrivesOverTheSocketAndLandsInTheStore(t *testing.T) {
	baseURL := apiURL(t)
	ctx := context.Background()

	suffix := unique()
	ana := newPerson(t, baseURL, "ana"+suffix)
	bruno := newPerson(t, baseURL, "bruno"+suffix)

	conversation, err := ana.api.StartDirect(ctx, bruno.api.AccountID())
	if err != nil {
		t.Fatalf("start direct: %v", err)
	}

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	go bruno.syncer.Run(runCtx)

	// Wait until bruno's client knows the conversation exists, so the send below is a
	// live delivery rather than a catch-up.
	waitFor(t, "the conversation to appear locally", func() bool {
		conversations, err := bruno.store.Conversations(ctx)
		return err == nil && len(conversations) > 0
	})

	if _, err := ana.api.Send(ctx, conversation.ID, id.New(), "live", 0); err != nil {
		t.Fatalf("send: %v", err)
	}

	waitFor(t, "the entry to arrive over the socket", func() bool {
		entries, err := bruno.store.Entries(ctx, conversation.ID)
		return err == nil && len(entries) == 1 && entries[0].Body == "live"
	})
}

func TestAGapIsFilledOnReconnect(t *testing.T) {
	// The same guarantee the browser client is held to, in Go. Bruno syncs, stops
	// syncing, misses messages entirely, and gets them all on the next connection —
	// with the mark resuming from where it was rather than from zero.
	baseURL := apiURL(t)
	ctx := context.Background()

	suffix := unique()
	ana := newPerson(t, baseURL, "ana"+suffix)
	bruno := newPerson(t, baseURL, "bruno"+suffix)

	conversation, err := ana.api.StartDirect(ctx, bruno.api.AccountID())
	if err != nil {
		t.Fatalf("start direct: %v", err)
	}
	if _, err := ana.api.Send(ctx, conversation.ID, id.New(), "before", 0); err != nil {
		t.Fatalf("send: %v", err)
	}

	if err := bruno.syncer.Refresh(ctx); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	// Nothing of bruno's is running now. Three messages arrive that he cannot possibly
	// receive live.
	for _, text := range []string{"while away 1", "while away 2", "while away 3"} {
		if _, err := ana.api.Send(ctx, conversation.ID, id.New(), text, 0); err != nil {
			t.Fatalf("send: %v", err)
		}
	}

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	go bruno.syncer.Run(runCtx)

	waitFor(t, "the gap to fill", func() bool {
		entries, err := bruno.store.Entries(ctx, conversation.ID)
		return err == nil && len(entries) == 4
	})

	entries, err := bruno.store.Entries(ctx, conversation.ID)
	if err != nil {
		t.Fatalf("entries: %v", err)
	}
	for index, entry := range entries {
		if entry.Sequence != int64(index+1) {
			t.Errorf("entry %d has position %d — the log is not gapless locally", index, entry.Sequence)
		}
	}
}

func TestAnEditIsAppliedToTheMessageItAmends(t *testing.T) {
	// The client half of ADR-0008 in Go, matching web/src/transcript.ts. The local row
	// for the original is updated; the amendment keeps its own position so the mark
	// passes through it.
	baseURL := apiURL(t)
	ctx := context.Background()

	suffix := unique()
	ana := newPerson(t, baseURL, "ana"+suffix)
	bruno := newPerson(t, baseURL, "bruno"+suffix)

	conversation, err := ana.api.StartDirect(ctx, bruno.api.AccountID())
	if err != nil {
		t.Fatalf("start direct: %v", err)
	}

	original, err := ana.api.Send(ctx, conversation.ID, id.New(), "the original wording", 0)
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	// Bruno syncs past it, which is the case that makes an in-place edit invisible.
	if err := bruno.syncer.Refresh(ctx); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	if _, err := ana.api.Revise(ctx, conversation.ID, original.Sequence, id.New(), "the corrected wording"); err != nil {
		t.Fatalf("revise: %v", err)
	}

	if err := bruno.syncer.Refresh(ctx); err != nil {
		t.Fatalf("refresh after the edit: %v", err)
	}

	entries, err := bruno.store.Entries(ctx, conversation.ID)
	if err != nil {
		t.Fatalf("entries: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("held %d entries, want 2 — the amendment takes its own position", len(entries))
	}
	if entries[0].Body != "the corrected wording" {
		t.Errorf("the original reads %q, want the correction applied", entries[0].Body)
	}

	// And search finds the new wording, not the old one — the same semantics the
	// browser client must give.
	if results, err := bruno.store.Search(ctx, "corrected", 10); err != nil || len(results) != 1 {
		t.Errorf("search for the new wording found %+v (%v), want one", results, err)
	}
	if results, err := bruno.store.Search(ctx, "original", 10); err != nil || len(results) != 0 {
		t.Errorf("search for the replaced wording found %+v (%v), want none", results, err)
	}
}
