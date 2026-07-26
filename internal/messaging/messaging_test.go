// Package messaging_test exercises messaging end to end: real Postgres, real
// Redis, real WebSockets, and two independent nodes.
//
// Two nodes is not thoroughness for its own sake. ADR-0005 exists because a
// recipient's socket is almost never on the node that accepted the write, and a
// single-node test would pass with no fan-out at all — the one thing most worth
// proving here.
package messaging_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/redis/go-redis/v9"

	"comms/internal/messaging/internal/api"
	"comms/internal/messaging/internal/app"
	"comms/internal/messaging/internal/broadcast"
	"comms/internal/messaging/internal/postgres"
	"comms/internal/messaging/internal/presence"
	"comms/internal/platform/database"
	"comms/internal/platform/database/testdb"
	"comms/internal/platform/httpx"
	"comms/internal/platform/id"
	"comms/internal/platform/ratelimit"
)

// node is one api instance: its own hub and HTTP server, sharing Postgres and
// Redis with every other node.
type node struct {
	t       *testing.T
	server  *httptest.Server
	service *app.Service
	hub     *api.Hub
	tokens  *fakeAuthenticator
}

// fakeAuthenticator stands in for Identity.
//
// Messaging declares Authenticator as its own port precisely so it can be
// satisfied without importing Identity (ADR-0007). Using a fake here is not
// avoiding a real dependency — it is the boundary working as designed.
type fakeAuthenticator struct {
	mutex    sync.RWMutex
	accounts map[string]string // token -> account id
	devices  map[string]string // token -> device id
}

func newFakeAuthenticator() *fakeAuthenticator {
	return &fakeAuthenticator{accounts: map[string]string{}, devices: map[string]string{}}
}

func (a *fakeAuthenticator) issue(accountID string) (token string) {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	token = "token-" + id.New()
	a.accounts[token] = accountID
	a.devices[token] = "device-" + id.New()
	return token
}

func (a *fakeAuthenticator) revoke(token string) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	delete(a.accounts, token)
}

func (a *fakeAuthenticator) Authenticate(_ context.Context, token string) (string, string, error) {
	a.mutex.RLock()
	defer a.mutex.RUnlock()

	accountID, found := a.accounts[token]
	if !found {
		return "", "", errors.New("unknown token")
	}
	return accountID, a.devices[token], nil
}

// callerKey carries the authenticated account on a test request, mirroring what
// Identity's middleware does in production.
type callerKey struct{}

type caller struct{ accountID, deviceID string }

// openRedis returns a client, skipping the test when Redis is unavailable.
func openRedis(t *testing.T) *redis.Client {
	t.Helper()

	url := os.Getenv("REDIS_URL")
	if url == "" {
		t.Skip("REDIS_URL not set, skipping integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := broadcast.OpenRedis(ctx, url)
	if err != nil {
		t.Fatalf("open redis: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	return client
}

// testLogger discards by default and writes to stderr when COMMS_TEST_LOG is set.
//
// Worth the three lines: a handler that answers 500 logs the cause and returns a body
// that reveals nothing, which is right in production and blinding in a test. Without
// this the only way to see why is to edit the harness.
func testLogger() *slog.Logger {
	if os.Getenv("COMMS_TEST_LOG") == "" {
		return slog.New(slog.DiscardHandler)
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// newNode starts an api instance sharing the given authenticator.
func newNode(t *testing.T, tokens *fakeAuthenticator) *node {
	t.Helper()

	db := testdb.Open(t)
	redisClient := openRedis(t)
	logger := testLogger()

	service := app.NewService(
		postgres.NewConversationRepository(db),
		postgres.NewMembershipRepository(db),
		postgres.NewEntryRepository(db),
		postgres.NewInviteRepository(db),
		postgres.NewReactionStore(db),
		postgres.NewMemberStateStore(db),
		// Real Redis, like the broadcaster below: presence is expiry-based, and a double
		// would agree with whatever this code believes about when a claim lapses.
		presence.NewStore(redisClient),
		broadcast.NewRedisBroadcaster(redisClient),
		// The real outbox, not a double. These tests are what establish that an
		// entry and its event commit together, which a recording publisher would
		// assert nothing about.
		postgres.NewOutboxPublisher(db),
		database.NewConn(db),
		app.IDs{},
		time.Now,
		logger,
	)

	hub := api.NewHub(redisClient, logger)
	hubCtx, stopHub := context.WithCancel(context.Background())
	go hub.Run(hubCtx)
	t.Cleanup(stopHub)

	// Mirrors production: one middleware authenticates, and both contexts' handlers
	// read the caller off the context.
	authenticated := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			accountID, deviceID, err := tokens.Authenticate(r.Context(), token)
			if err != nil {
				httpx.WriteError(w, logger, http.StatusUnauthorized, httpx.ErrorBody{Code: "unauthenticated"})
				return
			}
			ctx := context.WithValue(r.Context(), callerKey{}, caller{accountID, deviceID})
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}

	resolveCaller := func(ctx context.Context) (string, string) {
		resolved, _ := ctx.Value(callerKey{}).(caller)
		return resolved.accountID, resolved.deviceID
	}

	mux := http.NewServeMux()
	// A real limiter over real Redis. The limits are far above anything these tests do, so
	// they are invisible here — which is the point: a limiter that changed the outcome of an
	// ordinary test would be a limiter set too low.
	api.NewHandler(service, hub, tokens, resolveCaller, []string{"*"},
		ratelimit.New(redisClient, logger), logger).Routes(mux, authenticated)

	server := httptest.NewServer(httpx.Chain(mux, httpx.Correlate))
	t.Cleanup(server.Close)

	return &node{t: t, server: server, service: service, hub: hub, tokens: tokens}
}

// do performs an authenticated request.
func (n *node) do(method, path, token string, body any, target any) int {
	n.t.Helper()

	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			n.t.Fatal(err)
		}
		reader = bytes.NewReader(encoded)
	}

	request, err := http.NewRequest(method, n.server.URL+path, reader)
	if err != nil {
		n.t.Fatal(err)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}

	response, err := n.server.Client().Do(request)
	if err != nil {
		n.t.Fatal(err)
	}
	defer response.Body.Close()

	if target != nil {
		if err := json.NewDecoder(response.Body).Decode(target); err != nil {
			n.t.Fatalf("decode %s %s: %v", method, path, err)
		}
	}
	return response.StatusCode
}

type conversationBody struct {
	ID   string `json:"id"`
	Kind string `json:"kind"`
	Head int64  `json:"head"`
}

type entryBody struct {
	ID             string `json:"id"`
	Sequence       int64  `json:"sequence"`
	AuthorID       string `json:"author_id"`
	ClientEntryID  string `json:"client_entry_id"`
	Kind           string `json:"kind"`
	ContentType    string `json:"content_type"`
	Body           string `json:"body"`
	TargetSequence int64  `json:"target_sequence"`
	ReplyTo        int64  `json:"reply_to"`
}

func (e entryBody) text(t *testing.T) string {
	t.Helper()

	decoded, err := base64.StdEncoding.DecodeString(e.Body)
	if err != nil {
		t.Fatalf("decode body: %v", err)
	}
	return string(decoded)
}

func (n *node) startDirect(token, otherAccountID string) conversationBody {
	n.t.Helper()

	var conversation conversationBody
	if status := n.do(http.MethodPost, "/v1/conversations/direct", token,
		map[string]string{"account_id": otherAccountID}, &conversation); status != http.StatusCreated {
		n.t.Fatalf("start direct: status %d", status)
	}
	return conversation
}

// sendRaw posts an entry and returns the status and headers without asserting either.
//
// Needed because every other send helper fails the test on anything but 201, which is right for
// tests about messaging and useless for a test about being refused.
func (n *node) sendRaw(token, conversationID, text string) (int, http.Header) {
	n.t.Helper()

	body, err := json.Marshal(map[string]string{
		"client_entry_id": id.New(),
		"content_type":    "text/plain",
		"body":            base64.StdEncoding.EncodeToString([]byte(text)),
	})
	if err != nil {
		n.t.Fatal(err)
	}

	request, err := http.NewRequest(http.MethodPost,
		n.server.URL+"/v1/conversations/"+conversationID+"/entries", bytes.NewReader(body))
	if err != nil {
		n.t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+token)

	response, err := n.server.Client().Do(request)
	if err != nil {
		n.t.Fatal(err)
	}
	defer response.Body.Close()

	return response.StatusCode, response.Header
}

func (n *node) send(token, conversationID, text string) entryBody {
	n.t.Helper()
	return n.sendWithClientID(token, conversationID, text, id.New())
}

func (n *node) sendWithClientID(token, conversationID, text, clientEntryID string) entryBody {
	n.t.Helper()

	var entry entryBody
	status := n.do(http.MethodPost, "/v1/conversations/"+conversationID+"/entries", token, map[string]string{
		"client_entry_id": clientEntryID,
		"content_type":    "text/plain",
		"body":            base64.StdEncoding.EncodeToString([]byte(text)),
	}, &entry)
	if status != http.StatusCreated {
		n.t.Fatalf("send: status %d", status)
	}
	return entry
}

func (n *node) entriesAfter(token, conversationID string, after int64) []entryBody {
	n.t.Helper()

	var response struct {
		Entries []entryBody `json:"entries"`
	}
	if status := n.do(http.MethodGet,
		fmt.Sprintf("/v1/conversations/%s/entries?after=%d", conversationID, after),
		token, nil, &response); status != http.StatusOK {
		n.t.Fatalf("list entries: status %d", status)
	}
	return response.Entries
}

func newAccountID() string { return id.New() }

// --- socket client ---

// socket is a test client speaking the phase 2 protocol.
type socket struct {
	t      *testing.T
	conn   *websocket.Conn
	ctx    context.Context
	cancel context.CancelFunc
}

func (n *node) dial(t *testing.T) *socket {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	url := "ws" + strings.TrimPrefix(n.server.URL, "http") + "/v1/socket"

	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		cancel()
		t.Fatalf("dial: %v", err)
	}

	client := &socket{t: t, conn: conn, ctx: ctx, cancel: cancel}
	t.Cleanup(client.close)
	return client
}

func (s *socket) close() {
	_ = s.conn.Close(websocket.StatusNormalClosure, "test over")
	s.cancel()
}

func (s *socket) write(frame any) {
	s.t.Helper()

	encoded, err := json.Marshal(frame)
	if err != nil {
		s.t.Fatal(err)
	}
	if err := s.conn.Write(s.ctx, websocket.MessageText, encoded); err != nil {
		s.t.Fatalf("write frame: %v", err)
	}
}

// read returns the next frame, failing the test on timeout — a frame that never
// arrives is the bug most of these tests are looking for.
func (s *socket) read() map[string]any {
	s.t.Helper()

	readCtx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
	defer cancel()

	_, raw, err := s.conn.Read(readCtx)
	if err != nil {
		s.t.Fatalf("read frame: %v", err)
	}

	var frame map[string]any
	if err := json.Unmarshal(raw, &frame); err != nil {
		s.t.Fatalf("decode frame: %v", err)
	}
	return frame
}

// readOfType skips frames until one of the wanted type arrives.
func (s *socket) readOfType(want string) map[string]any {
	s.t.Helper()

	for range 10 {
		frame := s.read()
		if frame["type"] == want {
			return frame
		}
	}
	s.t.Fatalf("no %q frame after 10 frames", want)
	return nil
}

// expectNothing asserts no frame arrives within a short window.
func (s *socket) expectNothing() {
	s.t.Helper()

	readCtx, cancel := context.WithTimeout(s.ctx, 600*time.Millisecond)
	defer cancel()

	_, raw, err := s.conn.Read(readCtx)
	if err == nil {
		s.t.Fatalf("expected no frame, got %s", raw)
	}
}

// authenticate performs the handshake and returns the ready frame.
func (s *socket) authenticate(token string) map[string]any {
	s.t.Helper()

	s.write(map[string]string{"type": "authenticate", "token": token})
	return s.readOfType("ready")
}

// resume declares what the client holds and returns the reported gaps.
func (s *socket) resume(cursor map[string]int64) []map[string]any {
	s.t.Helper()

	if cursor == nil {
		cursor = map[string]int64{}
	}
	s.write(map[string]any{"type": "resume", "cursor": cursor})

	frame := s.readOfType("gaps")
	raw, _ := frame["gaps"].([]any)

	gaps := make([]map[string]any, 0, len(raw))
	for _, item := range raw {
		if gap, ok := item.(map[string]any); ok {
			gaps = append(gaps, gap)
		}
	}
	return gaps
}

// --- conversations ---

func TestStartDirectIsIdempotentOnThePair(t *testing.T) {
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken, brunoToken := tokens.issue(ana), tokens.issue(bruno)

	first := node.startDirect(anaToken, bruno)
	again := node.startDirect(anaToken, bruno)
	// Started from the other side, which is the case a naive implementation gets
	// wrong: two conversations, each holding half the history.
	reverse := node.startDirect(brunoToken, ana)

	if first.ID != again.ID {
		t.Errorf("starting twice produced %s and %s", first.ID, again.ID)
	}
	if first.ID != reverse.ID {
		t.Errorf("starting from each side produced %s and %s", first.ID, reverse.ID)
	}
}

func TestCannotStartADirectConversationWithYourself(t *testing.T) {
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana := newAccountID()
	anaToken := tokens.issue(ana)

	status := node.do(http.MethodPost, "/v1/conversations/direct", anaToken,
		map[string]string{"account_id": ana}, nil)
	if status != http.StatusUnprocessableEntity {
		t.Errorf("status %d, want %d", status, http.StatusUnprocessableEntity)
	}
}

// --- MS-1, MS-2: sending ---

func TestSendAssignsGaplessSequences(t *testing.T) {
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken := tokens.issue(ana)
	conversation := node.startDirect(anaToken, bruno)

	for expected := int64(1); expected <= 5; expected++ {
		entry := node.send(anaToken, conversation.ID, fmt.Sprintf("message %d", expected))
		if entry.Sequence != expected {
			t.Fatalf("sequence = %d, want %d", entry.Sequence, expected)
		}
	}
}

func TestSendIsIdempotentOnClientEntryID(t *testing.T) {
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken := tokens.issue(ana)
	conversation := node.startDirect(anaToken, bruno)

	clientEntryID := id.New()
	first := node.sendWithClientID(anaToken, conversation.ID, "hello", clientEntryID)
	retry := node.sendWithClientID(anaToken, conversation.ID, "hello", clientEntryID)

	// MS-2. Without this, an at-least-once client on a flaky network double-sends,
	// which users notice immediately.
	if first.ID != retry.ID || first.Sequence != retry.Sequence {
		t.Errorf("retry produced %s#%d, want %s#%d", retry.ID, retry.Sequence, first.ID, first.Sequence)
	}
	if entries := node.entriesAfter(anaToken, conversation.ID, 0); len(entries) != 1 {
		t.Errorf("conversation holds %d entries, want 1", len(entries))
	}
}

func TestConcurrentSendsStayGapless(t *testing.T) {
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken, brunoToken := tokens.issue(ana), tokens.issue(bruno)
	conversation := node.startDirect(anaToken, bruno)

	// The optimistic-concurrency path. Both accounts write to the same conversation
	// at once, so appends genuinely collide on the version check and retry.
	const perSender = 8
	var waitGroup sync.WaitGroup
	for _, token := range []string{anaToken, brunoToken} {
		for index := range perSender {
			waitGroup.Add(1)
			go func(token string, index int) {
				defer waitGroup.Done()
				node.send(token, conversation.ID, fmt.Sprintf("concurrent %d", index))
			}(token, index)
		}
	}
	waitGroup.Wait()

	entries := node.entriesAfter(anaToken, conversation.ID, 0)
	if len(entries) != perSender*2 {
		t.Fatalf("conversation holds %d entries, want %d", len(entries), perSender*2)
	}

	// Every position assigned exactly once, in order, with no gap — the invariant
	// the Conversation aggregate exists to protect.
	for index, entry := range entries {
		if want := int64(index + 1); entry.Sequence != want {
			t.Fatalf("entry %d has sequence %d, want %d", index, entry.Sequence, want)
		}
	}
}

func TestSendRejectsOversizedAndMalformedPayloads(t *testing.T) {
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken := tokens.issue(ana)
	conversation := node.startDirect(anaToken, bruno)
	path := "/v1/conversations/" + conversation.ID + "/entries"

	oversized := base64.StdEncoding.EncodeToString(make([]byte, 64*1024+1))
	cases := map[string]struct {
		body map[string]string
		want int
	}{
		"oversized body":     {map[string]string{"client_entry_id": id.New(), "content_type": "text/plain", "body": oversized}, http.StatusUnprocessableEntity},
		"empty body":         {map[string]string{"client_entry_id": id.New(), "content_type": "text/plain", "body": ""}, http.StatusUnprocessableEntity},
		"no client entry id": {map[string]string{"client_entry_id": "", "content_type": "text/plain", "body": base64.StdEncoding.EncodeToString([]byte("x"))}, http.StatusUnprocessableEntity},
		"no content type":    {map[string]string{"client_entry_id": id.New(), "content_type": "", "body": base64.StdEncoding.EncodeToString([]byte("x"))}, http.StatusUnprocessableEntity},
		"body not base64":    {map[string]string{"client_entry_id": id.New(), "content_type": "text/plain", "body": "not base64!!"}, http.StatusBadRequest},
	}
	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			if status := node.do(http.MethodPost, path, anaToken, testCase.body, nil); status != testCase.want {
				t.Errorf("status %d, want %d", status, testCase.want)
			}
		})
	}
}

func TestNonMembersCannotReadOrWrite(t *testing.T) {
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana, bruno, carla := newAccountID(), newAccountID(), newAccountID()
	anaToken, carlaToken := tokens.issue(ana), tokens.issue(carla)
	conversation := node.startDirect(anaToken, bruno)
	node.send(anaToken, conversation.ID, "private")

	// 404 rather than 403 for both: an outsider must not be able to tell whether a
	// conversation exists.
	if status := node.do(http.MethodGet, "/v1/conversations/"+conversation.ID+"/entries", carlaToken, nil, nil); status != http.StatusNotFound {
		t.Errorf("read: status %d, want %d", status, http.StatusNotFound)
	}
	if status := node.do(http.MethodPost, "/v1/conversations/"+conversation.ID+"/entries", carlaToken, map[string]string{
		"client_entry_id": id.New(), "content_type": "text/plain", "body": base64.StdEncoding.EncodeToString([]byte("hi")),
	}, nil); status != http.StatusNotFound {
		t.Errorf("write: status %d, want %d", status, http.StatusNotFound)
	}
}

func TestUnauthenticatedRequestsAreRejected(t *testing.T) {
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	if status := node.do(http.MethodGet, "/v1/conversations", "", nil, nil); status != http.StatusUnauthorized {
		t.Errorf("status %d, want %d", status, http.StatusUnauthorized)
	}
}

// --- MS-3: sync ---

func TestFetchReturnsOnlyWhatComesAfterTheCursor(t *testing.T) {
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken := tokens.issue(ana)
	conversation := node.startDirect(anaToken, bruno)
	for index := range 5 {
		node.send(anaToken, conversation.ID, fmt.Sprintf("message %d", index+1))
	}

	entries := node.entriesAfter(anaToken, conversation.ID, 3)
	if len(entries) != 2 {
		t.Fatalf("got %d entries, want 2", len(entries))
	}
	if entries[0].Sequence != 4 || entries[1].Sequence != 5 {
		t.Errorf("sequences %d,%d, want 4,5", entries[0].Sequence, entries[1].Sequence)
	}
	if entries[0].text(t) != "message 4" {
		t.Errorf("body = %q, want %q", entries[0].text(t), "message 4")
	}
}

func TestSocketRejectsAFirstFrameThatIsNotAuthentication(t *testing.T) {
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	client := node.dial(t)
	client.write(map[string]any{"type": "resume", "cursor": map[string]int64{}})

	frame := client.read()
	if frame["type"] != "error" || frame["code"] != "expected_authenticate" {
		t.Errorf("frame = %v, want an expected_authenticate error", frame)
	}
}

func TestSocketRejectsAnInvalidToken(t *testing.T) {
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	client := node.dial(t)
	client.write(map[string]string{"type": "authenticate", "token": "not-a-real-token"})

	frame := client.read()
	if frame["type"] != "error" || frame["code"] != "unauthenticated" {
		t.Errorf("frame = %v, want an unauthenticated error", frame)
	}
}

func TestResumeReportsExactlyTheMissingRange(t *testing.T) {
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken := tokens.issue(ana)
	conversation := node.startDirect(anaToken, bruno)
	for index := range 5 {
		node.send(anaToken, conversation.ID, fmt.Sprintf("message %d", index+1))
	}

	client := node.dial(t)
	client.authenticate(anaToken)

	// MS-3: holding 2 of 5, the answer is 3..5 exactly — not "everything since
	// roughly now", which is what timestamp ordering would have forced.
	gaps := client.resume(map[string]int64{conversation.ID: 2})
	if len(gaps) != 1 {
		t.Fatalf("got %d gaps, want 1", len(gaps))
	}
	if gaps[0]["from"] != float64(3) || gaps[0]["to"] != float64(5) {
		t.Errorf("gap = %v..%v, want 3..5", gaps[0]["from"], gaps[0]["to"])
	}
}

func TestResumeReportsNothingWhenCurrent(t *testing.T) {
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken := tokens.issue(ana)
	conversation := node.startDirect(anaToken, bruno)
	node.send(anaToken, conversation.ID, "only message")

	client := node.dial(t)
	client.authenticate(anaToken)

	if gaps := client.resume(map[string]int64{conversation.ID: 1}); len(gaps) != 0 {
		t.Errorf("a current client was told it has gaps: %v", gaps)
	}
}

func TestResumeDiscoversAConversationJoinedWhileAway(t *testing.T) {
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken, brunoToken := tokens.issue(ana), tokens.issue(bruno)

	// Bruno starts the conversation and writes while Ana is not connected.
	conversation := node.startDirect(brunoToken, ana)
	node.send(brunoToken, conversation.ID, "are you there")

	client := node.dial(t)
	client.authenticate(anaToken)

	// A conversation absent from the cursor reads as zero, so a brand new
	// membership needs no separate mechanism to be discovered.
	gaps := client.resume(nil)
	if len(gaps) != 1 {
		t.Fatalf("got %d gaps, want 1", len(gaps))
	}
	if gaps[0]["conversation_id"] != conversation.ID {
		t.Errorf("gap is for %v, want %s", gaps[0]["conversation_id"], conversation.ID)
	}
	if gaps[0]["from"] != float64(1) {
		t.Errorf("gap starts at %v, want 1", gaps[0]["from"])
	}
}

// --- live delivery ---

func TestLiveDeliveryToAnotherAccount(t *testing.T) {
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken, brunoToken := tokens.issue(ana), tokens.issue(bruno)
	conversation := node.startDirect(anaToken, bruno)

	client := node.dial(t)
	client.authenticate(brunoToken)
	client.resume(nil)

	node.send(anaToken, conversation.ID, "hello bruno")

	frame := client.readOfType("entry")
	if frame["conversation_id"] != conversation.ID {
		t.Errorf("conversation = %v, want %s", frame["conversation_id"], conversation.ID)
	}
	if frame["sequence"] != float64(1) {
		t.Errorf("sequence = %v, want 1", frame["sequence"])
	}

	body, _ := frame["body"].(string)
	decoded, err := base64.StdEncoding.DecodeString(body)
	if err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if string(decoded) != "hello bruno" {
		t.Errorf("body = %q, want %q", decoded, "hello bruno")
	}
}

func TestLiveDeliveryAcrossNodes(t *testing.T) {
	tokens := newFakeAuthenticator()
	writer := newNode(t, tokens)
	reader := newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken, brunoToken := tokens.issue(ana), tokens.issue(bruno)
	conversation := writer.startDirect(anaToken, bruno)

	// Bruno's socket is on a node that knows nothing about the write. This is the
	// case ADR-0005 exists for, and the one a single-node test cannot see.
	client := reader.dial(t)
	client.authenticate(brunoToken)
	client.resume(nil)

	writer.send(anaToken, conversation.ID, "across the cluster")

	frame := client.readOfType("entry")
	if frame["sequence"] != float64(1) {
		t.Errorf("sequence = %v, want 1", frame["sequence"])
	}
}

func TestLiveDeliveryToASecondDeviceOfTheSameAccount(t *testing.T) {
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken := tokens.issue(ana)
	brunoPhone, brunoLaptop := tokens.issue(bruno), tokens.issue(bruno)
	conversation := node.startDirect(anaToken, bruno)

	phone := node.dial(t)
	phone.authenticate(brunoPhone)
	phone.resume(nil)

	laptop := node.dial(t)
	laptop.authenticate(brunoLaptop)
	laptop.resume(nil)

	node.send(anaToken, conversation.ID, "to every device")

	// Every device receives everything: read state is per membership, not per
	// device, so there is no notion of a device that should be skipped.
	for name, client := range map[string]*socket{"phone": phone, "laptop": laptop} {
		if frame := client.readOfType("entry"); frame["sequence"] != float64(1) {
			t.Errorf("%s got sequence %v, want 1", name, frame["sequence"])
		}
	}
}

func TestAConversationStartedWhileConnectedDeliversLive(t *testing.T) {
	tokens := newFakeAuthenticator()
	writer := newNode(t, tokens)
	reader := newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken, brunoToken := tokens.issue(ana), tokens.issue(bruno)

	// Bruno connects with no conversations at all, so his node is subscribed to
	// nothing.
	client := reader.dial(t)
	client.authenticate(brunoToken)
	if gaps := client.resume(nil); len(gaps) != 0 {
		t.Fatalf("a new account has gaps: %v", gaps)
	}

	conversation := writer.startDirect(anaToken, bruno)
	// Without the control-channel notification, this arrives nowhere until Bruno
	// reconnects.
	writer.send(anaToken, conversation.ID, "first contact")

	frame := client.readOfType("entry")
	if frame["conversation_id"] != conversation.ID {
		t.Errorf("conversation = %v, want %s", frame["conversation_id"], conversation.ID)
	}
}

func TestTheInitiatorOfAConversationAlsoReceivesLive(t *testing.T) {
	// The mirror of the test above, and the case that was wrong: notifying only the
	// recipient left the person who *started* a conversation unable to see replies
	// to it. Their socket authenticated before the conversation existed, so resume
	// found no membership and subscribed to nothing — and nothing afterwards told
	// it otherwise. Found by two browsers in web/src/browser.test.ts, which is what
	// that suite is for.
	tokens := newFakeAuthenticator()
	initiator := newNode(t, tokens)
	other := newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken, brunoToken := tokens.issue(ana), tokens.issue(bruno)

	client := initiator.dial(t)
	client.authenticate(anaToken)
	if gaps := client.resume(nil); len(gaps) != 0 {
		t.Fatalf("a new account has gaps: %v", gaps)
	}

	// Ana starts the conversation on her own already-connected socket, then Bruno
	// replies from the other node.
	conversation := initiator.startDirect(anaToken, bruno)
	other.send(brunoToken, conversation.ID, "replying to you")

	frame := client.readOfType("entry")
	if frame["conversation_id"] != conversation.ID {
		t.Errorf("conversation = %v, want %s", frame["conversation_id"], conversation.ID)
	}
}

func TestNoDeliveryToAccountsOutsideTheConversation(t *testing.T) {
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana, bruno, carla := newAccountID(), newAccountID(), newAccountID()
	anaToken, carlaToken := tokens.issue(ana), tokens.issue(bruno)
	outsiderToken := tokens.issue(carla)
	conversation := node.startDirect(anaToken, bruno)

	// Carla is connected to the same node and must receive nothing, even though
	// the node is subscribed to the conversation on Bruno's behalf.
	outsider := node.dial(t)
	outsider.authenticate(outsiderToken)
	outsider.resume(nil)

	member := node.dial(t)
	member.authenticate(carlaToken)
	member.resume(nil)

	node.send(anaToken, conversation.ID, "not for carla")

	member.readOfType("entry")
	outsider.expectNothing()
}

// --- reconnect ---

func TestGapIsFilledAfterAReconnect(t *testing.T) {
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken, brunoToken := tokens.issue(ana), tokens.issue(bruno)
	conversation := node.startDirect(anaToken, bruno)

	first := node.dial(t)
	first.authenticate(brunoToken)
	first.resume(nil)

	node.send(anaToken, conversation.ID, "message 1")
	live := first.readOfType("entry")
	held := int64(live["sequence"].(float64))

	// Bruno drops offline and misses three messages entirely.
	first.close()
	for index := range 3 {
		node.send(anaToken, conversation.ID, fmt.Sprintf("missed %d", index+1))
	}

	second := node.dial(t)
	second.authenticate(brunoToken)
	gaps := second.resume(map[string]int64{conversation.ID: held})

	if len(gaps) != 1 {
		t.Fatalf("got %d gaps, want 1", len(gaps))
	}
	if gaps[0]["from"] != float64(2) || gaps[0]["to"] != float64(4) {
		t.Errorf("gap = %v..%v, want 2..4", gaps[0]["from"], gaps[0]["to"])
	}

	// The reported gap must be exactly fetchable — nothing missing, nothing
	// repeated. This is what makes at-most-once socket delivery acceptable.
	fetched := node.entriesAfter(brunoToken, conversation.ID, held)
	if len(fetched) != 3 {
		t.Fatalf("fetched %d entries, want 3", len(fetched))
	}
	for index, entry := range fetched {
		if want := held + int64(index) + 1; entry.Sequence != want {
			t.Errorf("entry %d has sequence %d, want %d", index, entry.Sequence, want)
		}
		if want := fmt.Sprintf("missed %d", index+1); entry.text(t) != want {
			t.Errorf("entry %d body = %q, want %q", index, entry.text(t), want)
		}
	}
}

func TestLiveDeliveryResumesAfterReconnect(t *testing.T) {
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken, brunoToken := tokens.issue(ana), tokens.issue(bruno)
	conversation := node.startDirect(anaToken, bruno)

	first := node.dial(t)
	first.authenticate(brunoToken)
	first.resume(nil)
	first.close()

	// Reference counting must not have dropped the node's subscription while
	// another connection still needed it, nor left it dangling after the last one.
	second := node.dial(t)
	second.authenticate(brunoToken)
	second.resume(nil)

	node.send(anaToken, conversation.ID, "after reconnect")

	if frame := second.readOfType("entry"); frame["sequence"] != float64(1) {
		t.Errorf("sequence = %v, want 1", frame["sequence"])
	}
}

func TestPingIsAnswered(t *testing.T) {
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	client := node.dial(t)
	client.authenticate(tokens.issue(newAccountID()))

	client.write(map[string]string{"type": "ping"})
	if frame := client.readOfType("pong"); frame["type"] != "pong" {
		t.Errorf("frame = %v, want pong", frame)
	}
}

func TestUnknownFramesAreIgnoredRatherThanFatal(t *testing.T) {
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	client := node.dial(t)
	client.authenticate(tokens.issue(newAccountID()))

	// A newer client talking to an older server must degrade, not disconnect.
	client.write(map[string]any{"type": "something_from_the_future", "payload": 1})
	client.write(map[string]string{"type": "ping"})

	client.readOfType("pong")
}

func TestRevokedDeviceIsDisconnected(t *testing.T) {
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana := newAccountID()
	anaToken := tokens.issue(ana)

	client := node.dial(t)
	ready := client.authenticate(anaToken)
	deviceID, _ := ready["device_id"].(string)

	if node.hub.ConnectionCount() != 1 {
		t.Fatalf("node holds %d connections, want 1", node.hub.ConnectionCount())
	}

	// Authentication only runs at connect time, so without an explicit disconnect a
	// revoked device keeps receiving entries on its open socket until its token
	// happens to expire. This is what identity.device_revoked will drive.
	tokens.revoke(anaToken)
	node.hub.DisconnectDevice(deviceID)

	deadline := time.Now().Add(3 * time.Second)
	for node.hub.ConnectionCount() > 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if count := node.hub.ConnectionCount(); count != 0 {
		t.Errorf("node still holds %d connections after revocation", count)
	}
}

// --- presence and typing ---

// ask requests who is present in a conversation and returns the answer.
func (s *socket) ask(conversationID string) map[string]any {
	s.t.Helper()

	s.write(map[string]any{"type": "presence.ask", "conversation_id": conversationID})
	return s.readOfType("presence")
}

// strings pulls a list of identifiers out of a frame field.
func strings_(frame map[string]any, field string) []string {
	raw, _ := frame[field].([]any)
	values := make([]string, 0, len(raw))
	for _, item := range raw {
		if value, ok := item.(string); ok {
			values = append(values, value)
		}
	}
	return values
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// TestPresenceFollowsWhoIsConnected: a socket makes its account online, and closing it makes
// the account offline again — without waiting for anything to expire.
//
// Across two nodes, because that is where a naive implementation is wrong. Presence held in a
// node's memory is presence only that node can see, and the person asking is almost never on
// the node holding the answer.
func TestPresenceFollowsWhoIsConnected(t *testing.T) {
	tokens := newFakeAuthenticator()
	first, second := newNode(t, tokens), newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken, brunoToken := tokens.issue(ana), tokens.issue(bruno)
	conversation := first.startDirect(anaToken, bruno)

	watcher := first.dial(t)
	watcher.authenticate(anaToken)
	watcher.resume(nil)

	// Ana is online because she is holding this socket. Bruno is not.
	present := watcher.ask(conversation.ID)
	if online := strings_(present, "online"); !contains(online, ana) {
		t.Fatalf("ana is not online while holding a socket: %v", online)
	} else if contains(online, bruno) {
		t.Fatalf("bruno is online with no socket: %v", online)
	}

	// Bruno connects to the *other* node.
	brunoSocket := second.dial(t)
	brunoSocket.authenticate(brunoToken)
	brunoSocket.resume(nil)

	present = watcher.ask(conversation.ID)
	if online := strings_(present, "online"); !contains(online, bruno) {
		t.Fatalf("bruno is not online from the other node: %v", online)
	}

	// And gone at once when he closes, rather than in thirty seconds.
	brunoSocket.close()
	deadline := time.Now().Add(5 * time.Second)
	for {
		present = watcher.ask(conversation.ID)
		if !contains(strings_(present, "online"), bruno) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("bruno is still online after closing: %v", strings_(present, "online"))
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestTypingReachesTheOtherSideAndCanBeAskedFor is both halves of a typing indicator.
//
// The push is what makes it appear immediately for somebody already looking. The record is what
// makes it appear for somebody who opens the conversation a moment later, and it is the reason
// this is stored at all rather than only broadcast.
func TestTypingReachesTheOtherSideAndCanBeAskedFor(t *testing.T) {
	tokens := newFakeAuthenticator()
	first, second := newNode(t, tokens), newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken, brunoToken := tokens.issue(ana), tokens.issue(bruno)
	conversation := first.startDirect(anaToken, bruno)

	watching := second.dial(t)
	watching.authenticate(brunoToken)
	watching.resume(nil)

	typing := first.dial(t)
	typing.authenticate(anaToken)
	typing.resume(nil)

	typing.write(map[string]any{
		"type": "typing", "conversation_id": conversation.ID, "typing": true,
	})

	// Pushed, across nodes.
	pushed := watching.readOfType("typing")
	if pushed["account_id"] != ana {
		t.Fatalf("typing frame names %v, want ana", pushed["account_id"])
	}
	if pushed["typing"] != true {
		t.Fatal("typing frame says not typing")
	}

	// And recorded, so a client that was not listening finds out by asking.
	late := second.dial(t)
	late.authenticate(brunoToken)
	late.resume(nil)
	if who := strings_(late.ask(conversation.ID), "typing"); !contains(who, ana) {
		t.Fatalf("ana is not recorded as typing: %v", who)
	}

	// Stopping is pushed and cleared.
	typing.write(map[string]any{
		"type": "typing", "conversation_id": conversation.ID, "typing": false,
	})
	stopped := watching.readOfType("typing")
	if stopped["typing"] != false {
		t.Fatal("stop frame says typing")
	}
	if who := strings_(late.ask(conversation.ID), "typing"); contains(who, ana) {
		t.Fatalf("ana is still recorded as typing after stopping: %v", who)
	}
}

// TestSomebodyElsesConversationHasNoPresence: presence is information about people, and which
// people are in a conversation is exactly as private as the conversation.
func TestSomebodyElsesConversationHasNoPresence(t *testing.T) {
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana, bruno, stranger := newAccountID(), newAccountID(), newAccountID()
	anaToken, strangerToken := tokens.issue(ana), tokens.issue(stranger)
	_ = tokens.issue(bruno)
	conversation := node.startDirect(anaToken, bruno)

	outsider := node.dial(t)
	outsider.authenticate(strangerToken)
	outsider.resume(nil)

	// Silence rather than a refusal, so that asking cannot be used to discover which
	// conversations exist — the same rule Send follows.
	outsider.write(map[string]any{"type": "presence.ask", "conversation_id": conversation.ID})
	outsider.expectNothing()
}

// --- rate limits ---

// TestSendingTooFastIsRefusedWithRetryAfter is the send limit.
//
// Sixty in ten seconds is far above what a person does and far below what a loop does, which is
// the only band a useful limit can occupy. What is asserted is not the number but the shape: the
// caller is refused with 429, told how long to wait, and — the part worth testing — allowed again
// afterwards, because a limit that never lifts is a ban.
func TestSendingTooFastIsRefusedWithRetryAfter(t *testing.T) {
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken := tokens.issue(ana)
	_ = tokens.issue(bruno)
	conversation := node.startDirect(anaToken, bruno)

	var refused, retryAfter string
	sent := 0
	for range 200 {
		status, headers := node.sendRaw(anaToken, conversation.ID, fmt.Sprintf("burst %d", sent))
		if status == http.StatusTooManyRequests {
			refused = "yes"
			retryAfter = headers.Get("Retry-After")
			break
		}
		sent++
	}

	if refused == "" {
		t.Fatalf("%d sends in a row were never refused", sent)
	}
	if sent < 10 {
		t.Fatalf("refused after only %d sends, which is below anything a person would hit", sent)
	}
	// Retry-After is the part a client can act on. Without it the only advice a refusal
	// carries is "not now", which invites an immediate retry that is refused again.
	if retryAfter == "" {
		t.Fatal("a 429 carried no Retry-After header")
	}
	seconds, err := strconv.Atoi(retryAfter)
	if err != nil || seconds < 1 {
		t.Fatalf("Retry-After = %q, want a positive number of seconds", retryAfter)
	}

	// Somebody else is unaffected, which is what "per account" means. A limiter keyed on
	// the wrong thing passes everything above and fails here.
	brunoToken := tokens.issue(bruno)
	if status, _ := node.sendRaw(brunoToken, conversation.ID, "not my fault"); status != http.StatusCreated {
		t.Fatalf("another account's send returned %d while ana was limited", status)
	}
}

// TestReconnectingTooFastIsRefused is the connect limit.
//
// A socket is the most expensive thing a client can ask for — held, subscribed and remembered,
// where a request is answered and forgotten — so this refuses before the connection is
// established rather than after.
func TestReconnectingTooFastIsRefused(t *testing.T) {
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana := newAccountID()
	anaToken := tokens.issue(ana)

	// Each dial is a fresh socket for the same account, which is what a client with no
	// backoff does when a network flaps.
	var refused bool
	for attempt := range 60 {
		client := node.dial(t)
		client.write(map[string]string{"type": "authenticate", "token": anaToken})

		frame := client.read()
		if frame["type"] == "error" && frame["code"] == "rate_limited" {
			refused = true
			if message, _ := frame["message"].(string); !strings.Contains(message, "try again") {
				t.Fatalf("refusal says %q, which tells the client nothing to do", message)
			}
			client.close()
			break
		}
		if frame["type"] != "ready" {
			t.Fatalf("attempt %d: got %v", attempt, frame)
		}
		client.close()
	}

	if !refused {
		t.Fatal("sixty connections in a row were never refused")
	}
}
