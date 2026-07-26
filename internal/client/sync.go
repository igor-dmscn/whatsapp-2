package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/coder/websocket"
)

// Syncer keeps a local store up to date from the server.
//
// The Go half of docs/client-sync.md, and it implements the same rules as
// web/src/sync.ts because that document is the contract rather than a description of
// either one. The differences are all mechanical — goroutines instead of callbacks, a
// SQLite store instead of maps — and where the two disagree about a rule, one of them
// is wrong.
//
// The rule that matters most: resume from the highest sequence held with nothing
// missing below it, never from the highest held. Here that mark is persisted, so a cold
// start resumes where the last session reached instead of refetching everything.
type Syncer struct {
	api   *API
	store *Store

	// Changed is signalled after the store has been written to, so a UI can redraw
	// without polling. Buffered and never blocking: a UI that is slow to redraw must
	// not slow down syncing.
	Changed chan struct{}

	// Errors carries what went wrong, for a UI to show. Also non-blocking.
	Errors chan error
}

// NewSyncer returns a syncer over an API and a store.
func NewSyncer(api *API, store *Store) *Syncer {
	return &Syncer{
		api:     api,
		store:   store,
		Changed: make(chan struct{}, 1),
		Errors:  make(chan error, 8),
	}
}

func (s *Syncer) changed() {
	select {
	case s.Changed <- struct{}{}:
	default:
	}
}

func (s *Syncer) failed(err error) {
	if err == nil {
		return
	}
	select {
	case s.Errors <- err:
	default:
	}
}

// Run connects, syncs, and keeps syncing until ctx is cancelled.
//
// Reconnects with jittered backoff. Every failure ends in the same place — reconnect
// and resume from the mark — which is the property that makes the protocol have one
// recovery path rather than one per failure mode.
func (s *Syncer) Run(ctx context.Context) {
	attempt := 0
	for {
		if ctx.Err() != nil {
			return
		}

		err := s.session(ctx)
		switch {
		case ctx.Err() != nil:
			return
		case err != nil:
			s.failed(err)
		}

		delay := min(500*time.Millisecond<<attempt, 10*time.Second)
		attempt++
		// Jittered so a node restarting does not have every client it dropped return
		// in the same instant.
		jittered := time.Duration(float64(delay) * (0.5 + rand.Float64()/2)) //nolint:gosec // jitter, not a secret

		select {
		case <-ctx.Done():
			return
		case <-time.After(jittered):
		}
	}
}

// session holds one connection for as long as it lasts.
func (s *Syncer) session(ctx context.Context) error {
	// The catch-up before the socket, not after: a client that renders from its store
	// and then connects shows something immediately, and a client that waits for a
	// socket to show anything is a client that shows nothing offline.
	if err := s.Refresh(ctx); err != nil {
		// Not fatal. Offline, the store is still what the UI draws from — which is the
		// whole point of having one.
		s.failed(err)
	}
	if err := s.flushPending(ctx); err != nil {
		s.failed(err)
	}

	token, err := s.api.Token(ctx)
	if err != nil {
		return err
	}

	socket, _, err := websocket.Dial(ctx, s.api.SocketURL(), nil)
	if err != nil {
		return fmt.Errorf("dial socket: %w", err)
	}
	defer func() { _ = socket.CloseNow() }()
	socket.SetReadLimit(1 << 20)

	// Credentials in the first frame, never the URL: a token in a query string ends up
	// in proxy logs and browser history.
	if err := writeFrame(ctx, socket, map[string]string{"type": "authenticate", "token": token}); err != nil {
		return err
	}

	if _, err := readFrame(ctx, socket, "ready"); err != nil {
		return err
	}

	cursor, err := s.store.Cursor(ctx)
	if err != nil {
		return err
	}
	if err := writeFrame(ctx, socket, map[string]any{"type": "resume", "cursor": cursor}); err != nil {
		return err
	}

	// Anything the socket says arrives from here on, gaps included.
	for {
		frame, err := readAny(ctx, socket)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read frame: %w", err)
		}

		switch frame["type"] {
		case "gaps":
			s.failed(s.fillGaps(ctx, frame))
		case "entry":
			s.failed(s.applyEntry(ctx, frame))
		case "reaction":
			s.failed(s.applyReaction(ctx, frame))
		case "error":
			return fmt.Errorf("server: %v", frame["message"])
		}
	}
}

// Refresh pulls the conversation list and fills whatever is missing.
//
// Called before connecting and whenever a UI asks. Idempotent: it fetches from each
// conversation's mark, so calling it twice costs two empty responses rather than
// duplicating anything.
func (s *Syncer) Refresh(ctx context.Context) error {
	conversations, err := s.api.Conversations(ctx)
	if err != nil {
		return err
	}

	for _, conversation := range conversations {
		if err := s.store.SaveConversation(ctx, LocalConversation{
			ID:              conversation.ID,
			Kind:            conversation.Kind,
			Head:            conversation.Head,
			Role:            conversation.Role,
			VisibleFrom:     conversation.VisibleFrom,
			Unread:          conversation.Unread,
			OthersRead:      conversation.OthersReadThrough,
			OthersDelivered: conversation.OthersDeliveredThrough,
		}); err != nil {
			return err
		}
		if err := s.fill(ctx, conversation.ID); err != nil {
			return err
		}
	}

	s.changed()
	return nil
}

// fill pages forward from a conversation's mark until the server has nothing more.
func (s *Syncer) fill(ctx context.Context, conversationID string) error {
	for {
		cursor, err := s.store.Cursor(ctx)
		if err != nil {
			return err
		}
		after := cursor[conversationID]

		entries, err := s.api.Entries(ctx, conversationID, after)
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			return nil
		}

		// Positions between the request and the first entry returned are ones this
		// account may not see, or that never existed. Closing the mark over them is
		// what stops a member who joined late treating the history before their join
		// point as a permanent gap.
		if entries[0].Sequence > after+1 {
			if err := s.store.CloseOver(ctx, conversationID, entries[0].Sequence-1); err != nil {
				return err
			}
		}

		if err := s.apply(ctx, conversationID, entries); err != nil {
			return err
		}

		last := entries[len(entries)-1].Sequence
		if last <= after {
			// A page that does not advance would loop forever. Not expected; a client
			// loop against a server is the wrong place to assume that.
			return nil
		}
	}
}

// apply stores entries and applies any amendments among them.
func (s *Syncer) apply(ctx context.Context, conversationID string, entries []Entry) error {
	local := make([]LocalEntry, 0, len(entries))
	for _, entry := range entries {
		local = append(local, LocalEntry{
			ConversationID: entry.ConversationID,
			Sequence:       entry.Sequence,
			ID:             entry.ID,
			AuthorID:       entry.AuthorID,
			ClientEntryID:  entry.ClientEntryID,
			Kind:           entry.Kind,
			Body:           entry.Text(),
			TargetSequence: entry.TargetSequence,
			ReplyTo:        entry.ReplyTo,
			CreatedAt:      entry.CreatedAt.Format(time.RFC3339Nano),
		})
	}

	if err := s.store.SaveEntries(ctx, conversationID, local); err != nil {
		return err
	}

	// Amendments applied after the batch is stored, in sequence order, so the last one
	// wins — the same rule the browser client's resolve() applies.
	for _, entry := range entries {
		if entry.TargetSequence == 0 {
			continue
		}
		if err := s.store.ApplyAmendment(ctx, conversationID, entry.TargetSequence, entry.Kind, entry.Text()); err != nil {
			return err
		}
	}

	s.changed()
	return nil
}

func (s *Syncer) fillGaps(ctx context.Context, frame map[string]any) error {
	gaps, ok := frame["gaps"].([]any)
	if !ok {
		return nil
	}
	for _, raw := range gaps {
		gap, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		conversationID, _ := gap["conversation_id"].(string)
		if conversationID == "" {
			continue
		}
		if err := s.fill(ctx, conversationID); err != nil {
			return err
		}
	}
	return nil
}

func (s *Syncer) applyEntry(ctx context.Context, frame map[string]any) error {
	entry := Entry{
		ID:             stringOf(frame["entry_id"]),
		ConversationID: stringOf(frame["conversation_id"]),
		Sequence:       int64Of(frame["sequence"]),
		AuthorID:       stringOf(frame["author_id"]),
		ClientEntryID:  stringOf(frame["client_entry_id"]),
		Kind:           stringOf(frame["kind"]),
		Body:           stringOf(frame["body"]),
		TargetSequence: int64Of(frame["target_sequence"]),
		ReplyTo:        int64Of(frame["reply_to"]),
		CreatedAt:      time.Now(),
	}
	if entry.ConversationID == "" || entry.Sequence == 0 {
		return nil
	}

	// A live entry above the mark means something below it never arrived — the
	// dropped-broadcast case ADR-0005 accepts. Store it, then fill the hole.
	if err := s.apply(ctx, entry.ConversationID, []Entry{entry}); err != nil {
		return err
	}

	cursor, err := s.store.Cursor(ctx)
	if err != nil {
		return err
	}
	if entry.Sequence > cursor[entry.ConversationID] {
		return s.fill(ctx, entry.ConversationID)
	}
	return nil
}

func (s *Syncer) applyReaction(ctx context.Context, frame map[string]any) error {
	removed, _ := frame["removed"].(bool)
	if err := s.store.SetReaction(ctx,
		stringOf(frame["conversation_id"]), int64Of(frame["sequence"]),
		stringOf(frame["account_id"]), stringOf(frame["emoji"]), removed); err != nil {
		return err
	}
	s.changed()
	return nil
}

// --- sending ---

// Send records a pending entry, then attempts it.
//
// Recorded first, deliberately. A send attempted before being written is one that is
// lost if the process dies waiting for a response — and worse, one whose client
// identifier is gone, so a retry creates a second entry rather than being recognised as
// the same one (MS-2).
func (s *Syncer) Send(ctx context.Context, conversationID, clientEntryID, text string, replyTo int64) error {
	if err := s.store.AddPending(ctx, Pending{
		ClientEntryID:  clientEntryID,
		ConversationID: conversationID,
		Body:           text,
		ReplyTo:        replyTo,
	}); err != nil {
		return err
	}
	s.changed()

	entry, err := s.api.Send(ctx, conversationID, clientEntryID, text, replyTo)
	if err != nil {
		// Left pending. Offline is the ordinary case here, and the next connection
		// flushes it with the same identifier — so it sends exactly once even though
		// it was attempted twice.
		return err
	}

	return s.apply(ctx, conversationID, []Entry{entry})
}

// flushPending retries everything unacknowledged.
//
// Every retry carries its original client identifier, which is what makes "send while
// offline, come back, and the entry arrives exactly once" true rather than hopeful.
func (s *Syncer) flushPending(ctx context.Context) error {
	pending, err := s.store.PendingSends(ctx)
	if err != nil {
		return err
	}

	for _, send := range pending {
		entry, err := s.api.Send(ctx, send.ConversationID, send.ClientEntryID, send.Body, send.ReplyTo)
		if err != nil {
			var failure Error
			if errors.As(err, &failure) && failure.Status >= 400 && failure.Status < 500 && !failure.Unauthorised() {
				// The server has refused it and will refuse it again — a conversation
				// that no longer accepts writes, a payload that is no longer valid.
				// Retrying forever would block every later send behind it.
				s.failed(fmt.Errorf("dropping unsendable message: %w", err))
				if clearErr := s.store.ClearPending(ctx, send.ClientEntryID); clearErr != nil {
					return clearErr
				}
				continue
			}
			return err
		}

		if err := s.apply(ctx, send.ConversationID, []Entry{entry}); err != nil {
			return err
		}
	}
	return nil
}

// --- frames ---

func writeFrame(ctx context.Context, socket *websocket.Conn, frame any) error {
	encoded, err := json.Marshal(frame)
	if err != nil {
		return fmt.Errorf("encode frame: %w", err)
	}

	writeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := socket.Write(writeCtx, websocket.MessageText, encoded); err != nil {
		return fmt.Errorf("write frame: %w", err)
	}
	return nil
}

func readAny(ctx context.Context, socket *websocket.Conn) (map[string]any, error) {
	_, raw, err := socket.Read(ctx)
	if err != nil {
		return nil, err //nolint:wrapcheck // the caller decides what a read failure means.
	}

	var frame map[string]any
	if err := json.Unmarshal(raw, &frame); err != nil {
		return nil, fmt.Errorf("decode frame: %w", err)
	}
	return frame, nil
}

// readFrame reads until a frame of the wanted type arrives.
func readFrame(ctx context.Context, socket *websocket.Conn, want string) (map[string]any, error) {
	deadline, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	for {
		frame, err := readAny(deadline, socket)
		if err != nil {
			return nil, err
		}
		if frame["type"] == "error" {
			return nil, fmt.Errorf("server refused: %v", frame["message"])
		}
		if frame["type"] == want {
			return frame, nil
		}
	}
}

func stringOf(value any) string {
	text, _ := value.(string)
	return text
}

func int64Of(value any) int64 {
	number, _ := value.(float64)
	return int64(number)
}
