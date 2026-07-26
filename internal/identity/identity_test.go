// Package identity_test exercises identity end to end over HTTP, against a real
// Postgres. There is no UI in phase 1, so these tests are the client.
package identity_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9"

	"comms/internal/identity/internal/domain"
	"time"

	"comms/internal/identity/internal/api"
	"comms/internal/identity/internal/app"
	"comms/internal/identity/internal/hashing"
	"comms/internal/identity/internal/postgres"
	"comms/internal/platform/database"
	"comms/internal/platform/database/testdb"
	"comms/internal/platform/httpx"
	"comms/internal/platform/id"
	"comms/internal/platform/ratelimit"
)

// harness is a running identity API backed by the test database.
type harness struct {
	t       *testing.T
	server  *httptest.Server
	service *app.Service
	now     func() time.Time
	events  *recordingPublisher
}

// clock is settable so token expiry can be tested without waiting.
type clock struct{ at time.Time }

func (c *clock) now() time.Time { return c.at }

func newHarness(t *testing.T) (*harness, *clock) {
	t.Helper()

	db := testdb.Open(t)

	// Argon2 at production cost would make these tests take minutes. Cost is
	// what the hasher is for, so it is turned down here and exercised at its
	// real settings by TestArgon2AtProductionCost.
	cheap := hashing.Argon2Params{Memory: 8 * 1024, Iterations: 1, Parallelism: 1, SaltLength: 16, KeyLength: 32}

	testClock := &clock{at: time.Now()}
	events := &recordingPublisher{}
	service := app.NewService(
		postgres.NewAccountRepository(db),
		postgres.NewDeviceRepository(db),
		postgres.NewSessionRepository(db),
		hashing.NewArgon2Hasher(cheap),
		events,
		database.NewConn(db),
		app.IDs{},
		testClock.now,
	)

	mux := http.NewServeMux()
	// A real limiter over real Redis, which is what ID-5 now depends on: the limit is
	// shared across nodes, so a per-process double would test something that no longer
	// exists. Skipped along with the rest of the harness when Redis is unavailable.
	api.NewHandler(service, ratelimit.New(openRedis(t), slog.New(slog.DiscardHandler)),
		slog.New(slog.DiscardHandler)).Routes(mux)
	server := httptest.NewServer(httpx.Chain(mux, httpx.Correlate))
	t.Cleanup(server.Close)

	return &harness{t: t, server: server, service: service, now: testClock.now, events: events}, testClock
}

// openRedis returns a client, skipping the test when Redis is unavailable.
//
// Identity needs Redis for one thing — ID-5's shared rate limit — which is why this appeared in
// phase 10 and not in phase 1.
func openRedis(t *testing.T) *redis.Client {
	t.Helper()

	url := os.Getenv("REDIS_URL")
	if url == "" {
		t.Skip("REDIS_URL not set, skipping integration test")
	}

	options, err := redis.ParseURL(url)
	if err != nil {
		t.Fatalf("parse redis url: %v", err)
	}
	client := redis.NewClient(options)
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping redis: %v", err)
	}
	return client
}

// recordingPublisher captures published domain events so tests can assert that
// behaviour announced itself. An aggregate that changed state silently is a bug
// that only a test like this catches.
type recordingPublisher struct {
	mutex     sync.Mutex
	published []domain.Event
}

func (p *recordingPublisher) Publish(_ context.Context, events []domain.Event) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.published = append(p.published, events...)
	return nil
}

// names returns the event names published so far.
func (p *recordingPublisher) names() []string {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	names := make([]string, 0, len(p.published))
	for _, event := range p.published {
		names = append(names, event.EventName())
	}
	return names
}

func (p *recordingPublisher) reset() {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.published = nil
}

func contains(haystack []string, needle string) bool {
	return slices.Contains(haystack, needle)
}

// do performs a request and decodes the response into target when non-nil.
func (h *harness) do(method, path, token string, body any, target any) int {
	h.t.Helper()

	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			h.t.Fatal(err)
		}
		reader = bytes.NewReader(encoded)
	}

	request, err := http.NewRequest(method, h.server.URL+path, reader)
	if err != nil {
		h.t.Fatal(err)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}

	response, err := h.server.Client().Do(request)
	if err != nil {
		h.t.Fatal(err)
	}
	defer response.Body.Close()

	if target != nil {
		if err := json.NewDecoder(response.Body).Decode(target); err != nil {
			h.t.Fatalf("decode %s %s: %v", method, path, err)
		}
	}
	return response.StatusCode
}

type session struct {
	Account struct {
		ID     string `json:"id"`
		Handle string `json:"handle"`
	} `json:"account"`
	DeviceID     string `json:"device_id"`
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
}

// uniqueHandle keeps tests independent without truncating shared tables, so they
// can run in parallel and leave the database in a state a human can inspect.
func uniqueHandle() string {
	return "t" + strings.ReplaceAll(id.New(), "-", "")[:20]
}

func (h *harness) register(handle string) session {
	h.t.Helper()

	var result session
	status := h.do(http.MethodPost, "/v1/accounts", "", map[string]string{
		"handle":      handle,
		"email":       handle + "@example.com",
		"passphrase":  "correct horse battery staple",
		"device_name": "test device",
	}, &result)
	if status != http.StatusCreated {
		h.t.Fatalf("register %s: status %d", handle, status)
	}
	return result
}

// --- ID-1, ID-5: registration and lookup ---

func TestRegisterThenLookupByHandle(t *testing.T) {
	h, _ := newHarness(t)
	handle := uniqueHandle()

	created := h.register(handle)
	if created.AccessToken == "" || created.RefreshToken == "" {
		t.Fatal("registration returned no tokens")
	}

	var found struct {
		ID     string `json:"id"`
		Handle string `json:"handle"`
	}
	if status := h.do(http.MethodGet, "/v1/accounts/"+handle, created.AccessToken, nil, &found); status != http.StatusOK {
		t.Fatalf("lookup: status %d", status)
	}
	if found.ID != created.Account.ID {
		t.Errorf("looked up %q, want %q", found.ID, created.Account.ID)
	}
}

func TestHandleIsClaimedCaseInsensitively(t *testing.T) {
	h, _ := newHarness(t)
	handle := uniqueHandle()
	h.register(handle)

	// ID-1: uniqueness must not be defeated by changing case.
	status := h.do(http.MethodPost, "/v1/accounts", "", map[string]string{
		"handle":     strings.ToUpper(handle),
		"email":      "other-" + handle + "@example.com",
		"passphrase": "correct horse battery staple",
	}, nil)
	if status != http.StatusConflict {
		t.Errorf("status %d, want %d", status, http.StatusConflict)
	}
}

func TestEmailIsClaimedOnce(t *testing.T) {
	h, _ := newHarness(t)
	first := uniqueHandle()
	h.register(first)

	status := h.do(http.MethodPost, "/v1/accounts", "", map[string]string{
		"handle":     uniqueHandle(),
		"email":      first + "@example.com",
		"passphrase": "correct horse battery staple",
	}, nil)
	if status != http.StatusConflict {
		t.Errorf("status %d, want %d", status, http.StatusConflict)
	}
}

func TestRegisterRejectsWeakAndMalformedInput(t *testing.T) {
	h, _ := newHarness(t)

	cases := map[string]map[string]string{
		"short passphrase": {"handle": uniqueHandle(), "email": "a@example.com", "passphrase": "short"},
		"bad handle":       {"handle": "_nope", "email": "b@example.com", "passphrase": "correct horse battery staple"},
		"bad email":        {"handle": uniqueHandle(), "email": "nope", "passphrase": "correct horse battery staple"},
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if status := h.do(http.MethodPost, "/v1/accounts", "", body, nil); status != http.StatusUnprocessableEntity {
				t.Errorf("status %d, want %d", status, http.StatusUnprocessableEntity)
			}
		})
	}
}

// --- login ---

func TestLoginWithCorrectPassphrase(t *testing.T) {
	h, _ := newHarness(t)
	handle := uniqueHandle()
	registered := h.register(handle)

	var logged session
	status := h.do(http.MethodPost, "/v1/sessions", "", map[string]string{
		"handle":      handle,
		"passphrase":  "correct horse battery staple",
		"device_name": "second device",
	}, &logged)
	if status != http.StatusCreated {
		t.Fatalf("login: status %d", status)
	}

	// ID-3: logging in again is a new device, not a replacement of the old one.
	if logged.DeviceID == registered.DeviceID {
		t.Error("second login reused the first device")
	}
	if logged.AccessToken == registered.AccessToken {
		t.Error("second login reused the first access token")
	}
}

func TestLoginFailuresAreIndistinguishable(t *testing.T) {
	h, _ := newHarness(t)
	handle := uniqueHandle()
	h.register(handle)

	var wrongPassphrase, unknownHandle struct {
		Error httpx.ErrorBody `json:"error"`
	}

	wrongStatus := h.do(http.MethodPost, "/v1/sessions", "", map[string]string{
		"handle": handle, "passphrase": "wrong passphrase entirely",
	}, &wrongPassphrase)
	unknownStatus := h.do(http.MethodPost, "/v1/sessions", "", map[string]string{
		"handle": uniqueHandle(), "passphrase": "correct horse battery staple",
	}, &unknownHandle)

	if wrongStatus != http.StatusUnauthorized || unknownStatus != http.StatusUnauthorized {
		t.Fatalf("statuses %d and %d, want both %d", wrongStatus, unknownStatus, http.StatusUnauthorized)
	}
	// A different code for "no such handle" would turn login into a handle
	// enumeration endpoint.
	if wrongPassphrase.Error.Code != unknownHandle.Error.Code {
		t.Errorf("codes %q and %q differ, want identical",
			wrongPassphrase.Error.Code, unknownHandle.Error.Code)
	}
}

// --- ID-2: credentials are a set ---

func TestSecondCredentialKindDoesNotDisturbTheFirst(t *testing.T) {
	h, _ := newHarness(t)
	handle := uniqueHandle()
	created := h.register(handle)

	// ID-2: adding a passkey must not require altering the existing password.
	err := h.service.AddCredential(
		t.Context(),
		domain.AccountID(created.Account.ID),
		domain.KindPasskey,
		"public-key-material-placeholder",
	)
	if err != nil {
		t.Fatalf("add passkey credential: %v", err)
	}

	status := h.do(http.MethodPost, "/v1/sessions", "", map[string]string{
		"handle": handle, "passphrase": "correct horse battery staple",
	}, nil)
	if status != http.StatusCreated {
		t.Errorf("password login after adding a passkey: status %d, want %d", status, http.StatusCreated)
	}
}

func TestOnlyOnePasswordPerAccount(t *testing.T) {
	h, _ := newHarness(t)
	created := h.register(uniqueHandle())

	// Enforced by a partial unique index rather than by application code, so
	// concurrent writes cannot slip a second password through.
	err := h.service.AddCredential(t.Context(), domain.AccountID(created.Account.ID), domain.KindPassword, "another passphrase entirely")
	if !errors.Is(err, domain.ErrPasswordAlreadySet) {
		t.Errorf("got %v, want ErrPasswordAlreadySet", err)
	}
}

// --- refresh and rotation ---

func TestRefreshRotatesAndInvalidatesTheOldToken(t *testing.T) {
	h, _ := newHarness(t)
	created := h.register(uniqueHandle())

	var refreshed struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
	}
	status := h.do(http.MethodPost, "/v1/sessions/refresh", "",
		map[string]string{"refresh_token": created.RefreshToken}, &refreshed)
	if status != http.StatusCreated {
		t.Fatalf("refresh: status %d", status)
	}
	if refreshed.RefreshToken == created.RefreshToken {
		t.Error("refresh token was not rotated")
	}

	// A stolen refresh token must be usable at most once.
	replay := h.do(http.MethodPost, "/v1/sessions/refresh", "",
		map[string]string{"refresh_token": created.RefreshToken}, nil)
	if replay != http.StatusUnauthorized {
		t.Errorf("replayed refresh token: status %d, want %d", replay, http.StatusUnauthorized)
	}

	// The new access token must work.
	if status := h.do(http.MethodGet, "/v1/me", refreshed.AccessToken, nil, nil); status != http.StatusOK {
		t.Errorf("rotated access token: status %d, want %d", status, http.StatusOK)
	}
}

func TestAccessTokenRejectedForRefresh(t *testing.T) {
	h, _ := newHarness(t)
	created := h.register(uniqueHandle())

	// Both are opaque strings of the same shape, so the kind check is the only
	// thing preventing a fifteen-minute token being traded for a thirty-day one.
	status := h.do(http.MethodPost, "/v1/sessions/refresh", "",
		map[string]string{"refresh_token": created.AccessToken}, nil)
	if status != http.StatusUnauthorized {
		t.Errorf("status %d, want %d", status, http.StatusUnauthorized)
	}
}

func TestRefreshTokenRejectedAsAccessToken(t *testing.T) {
	h, _ := newHarness(t)
	created := h.register(uniqueHandle())

	if status := h.do(http.MethodGet, "/v1/me", created.RefreshToken, nil, nil); status != http.StatusUnauthorized {
		t.Errorf("status %d, want %d", status, http.StatusUnauthorized)
	}
}

func TestExpiredAccessTokenIsRejected(t *testing.T) {
	h, testClock := newHarness(t)
	created := h.register(uniqueHandle())

	if status := h.do(http.MethodGet, "/v1/me", created.AccessToken, nil, nil); status != http.StatusOK {
		t.Fatalf("fresh token: status %d", status)
	}

	testClock.at = testClock.at.Add(domain.AccessTokenLifetime + time.Second)

	if status := h.do(http.MethodGet, "/v1/me", created.AccessToken, nil, nil); status != http.StatusUnauthorized {
		t.Errorf("expired token: status %d, want %d", status, http.StatusUnauthorized)
	}
}

// --- ID-3, ID-4: devices and revocation ---

func TestRevokingADeviceInvalidatesItsTokensImmediately(t *testing.T) {
	h, _ := newHarness(t)
	handle := uniqueHandle()
	first := h.register(handle)

	var second session
	if status := h.do(http.MethodPost, "/v1/sessions", "", map[string]string{
		"handle": handle, "passphrase": "correct horse battery staple", "device_name": "phone",
	}, &second); status != http.StatusCreated {
		t.Fatalf("second login: status %d", status)
	}

	// ID-4 requires invalidation within 30 seconds. Because access tokens are
	// checked against the database and device revocation is checked on every
	// request, the actual bound is the next request — not a token lifetime.
	if status := h.do(http.MethodDelete, "/v1/devices/"+second.DeviceID, first.AccessToken, nil, nil); status != http.StatusNoContent {
		t.Fatalf("revoke: status %d", status)
	}

	if status := h.do(http.MethodGet, "/v1/me", second.AccessToken, nil, nil); status != http.StatusUnauthorized {
		t.Errorf("revoked device access token: status %d, want %d", status, http.StatusUnauthorized)
	}
	// A revoked device must not be able to refresh its way back in.
	if status := h.do(http.MethodPost, "/v1/sessions/refresh", "",
		map[string]string{"refresh_token": second.RefreshToken}, nil); status != http.StatusUnauthorized {
		t.Errorf("revoked device refresh: status %d, want %d", status, http.StatusUnauthorized)
	}
	// The revoking device must be unaffected.
	if status := h.do(http.MethodGet, "/v1/me", first.AccessToken, nil, nil); status != http.StatusOK {
		t.Errorf("revoking device: status %d, want %d", status, http.StatusOK)
	}
}

func TestRevokedDeviceStillAppearsInTheList(t *testing.T) {
	h, _ := newHarness(t)
	handle := uniqueHandle()
	first := h.register(handle)

	var second session
	h.do(http.MethodPost, "/v1/sessions", "", map[string]string{
		"handle": handle, "passphrase": "correct horse battery staple", "device_name": "phone",
	}, &second)
	h.do(http.MethodDelete, "/v1/devices/"+second.DeviceID, first.AccessToken, nil, nil)

	var listed struct {
		Devices []struct {
			ID        string     `json:"id"`
			RevokedAt *time.Time `json:"revoked_at"`
		} `json:"devices"`
	}
	if status := h.do(http.MethodGet, "/v1/devices", first.AccessToken, nil, &listed); status != http.StatusOK {
		t.Fatalf("list devices: status %d", status)
	}
	if len(listed.Devices) != 2 {
		t.Fatalf("listed %d devices, want 2", len(listed.Devices))
	}

	// A revoked device disappearing from the list would leave its owner unable
	// to audit what happened.
	var revoked int
	for _, device := range listed.Devices {
		if device.RevokedAt != nil {
			revoked++
		}
	}
	if revoked != 1 {
		t.Errorf("%d devices show as revoked, want 1", revoked)
	}
}

func TestCannotRevokeAnotherAccountsDevice(t *testing.T) {
	h, _ := newHarness(t)
	victim := h.register(uniqueHandle())
	attacker := h.register(uniqueHandle())

	// Reported as absent rather than forbidden: an account has no business
	// learning that another account's device exists.
	status := h.do(http.MethodDelete, "/v1/devices/"+victim.DeviceID, attacker.AccessToken, nil, nil)
	if status != http.StatusNotFound {
		t.Errorf("status %d, want %d", status, http.StatusNotFound)
	}

	if status := h.do(http.MethodGet, "/v1/me", victim.AccessToken, nil, nil); status != http.StatusOK {
		t.Error("victim's device was revoked by another account")
	}
}

// --- authentication edge cases ---

func TestUnauthenticatedAndMalformedTokensAreRejected(t *testing.T) {
	h, _ := newHarness(t)

	cases := map[string]string{
		"no token":         "",
		"nonsense token":   "not-a-real-token",
		"empty bearer":     " ",
		"truncated base64": "AAAA",
	}
	for name, token := range cases {
		t.Run(name, func(t *testing.T) {
			if status := h.do(http.MethodGet, "/v1/me", token, nil, nil); status != http.StatusUnauthorized {
				t.Errorf("status %d, want %d", status, http.StatusUnauthorized)
			}
		})
	}
}

// --- ID-5: lookup is rate limited ---

func TestHandleLookupIsRateLimited(t *testing.T) {
	h, _ := newHarness(t)
	caller := h.register(uniqueHandle())
	target := uniqueHandle()
	h.register(target)

	// The limit is per caller, so a single account sweeping the namespace is
	// stopped regardless of which handles it asks about.
	var limited bool
	for range 40 {
		if h.do(http.MethodGet, "/v1/accounts/"+target, caller.AccessToken, nil, nil) == http.StatusTooManyRequests {
			limited = true
			break
		}
	}
	if !limited {
		t.Error("40 lookups in a row were never rate limited")
	}
}

// --- token hygiene ---

func TestPurgeKeepsSessionsThatCanStillBeRefreshed(t *testing.T) {
	h, testClock := newHarness(t)
	created := h.register(uniqueHandle())

	// Past every access token's expiry but well inside every refresh token's.
	testClock.at = testClock.at.Add(domain.AccessTokenLifetime + time.Minute)
	if _, err := h.service.PurgeExpiredSessions(t.Context()); err != nil {
		t.Fatalf("purge: %v", err)
	}

	// Purging on access expiry would log people out mid-conversation, so the
	// session survives and only the access token is dead.
	if status := h.do(http.MethodGet, "/v1/me", created.AccessToken, nil, nil); status != http.StatusUnauthorized {
		t.Errorf("expired access token: status %d, want %d", status, http.StatusUnauthorized)
	}
	if status := h.do(http.MethodPost, "/v1/sessions/refresh", "",
		map[string]string{"refresh_token": created.RefreshToken}, nil); status != http.StatusCreated {
		t.Errorf("unexpired refresh token: status %d, want %d", status, http.StatusCreated)
	}
}

func TestPurgeRemovesSessionsPastRefreshExpiry(t *testing.T) {
	h, testClock := newHarness(t)
	created := h.register(uniqueHandle())

	testClock.at = testClock.at.Add(domain.RefreshTokenLifetime + time.Minute)
	deleted, err := h.service.PurgeExpiredSessions(t.Context())
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if deleted < 1 {
		t.Errorf("deleted %d sessions, want at least 1", deleted)
	}

	if status := h.do(http.MethodPost, "/v1/sessions/refresh", "",
		map[string]string{"refresh_token": created.RefreshToken}, nil); status != http.StatusUnauthorized {
		t.Errorf("purged refresh token: status %d, want %d", status, http.StatusUnauthorized)
	}
}

// --- hashing ---

func TestArgon2AtProductionCost(t *testing.T) {
	// The rest of the suite runs a cheap hasher, so the real parameters are
	// verified once, here, rather than never.
	hasher := hashing.NewArgon2Hasher(hashing.DefaultArgon2Params())

	passphrase, err := domain.ParsePassphrase("correct horse battery staple")
	if err != nil {
		t.Fatal(err)
	}

	material, err := hasher.Hash(passphrase)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(material, "$argon2id$") {
		t.Errorf("material = %q, want PHC argon2id format", material)
	}

	matches, err := hasher.Verify(material, "correct horse battery staple")
	if err != nil {
		t.Fatal(err)
	}
	if !matches {
		t.Error("correct passphrase did not verify")
	}

	matches, err = hasher.Verify(material, "wrong passphrase entirely")
	if err != nil {
		t.Fatal(err)
	}
	if matches {
		t.Error("wrong passphrase verified")
	}
}

func TestVerifyUsesParametersFromStoredMaterial(t *testing.T) {
	// Cost parameters live in the stored material, so raising them later must
	// not invalidate credentials written at the old cost.
	weak := hashing.NewArgon2Hasher(hashing.Argon2Params{
		Memory: 8 * 1024, Iterations: 1, Parallelism: 1, SaltLength: 16, KeyLength: 32,
	})
	strong := hashing.NewArgon2Hasher(hashing.DefaultArgon2Params())

	passphrase, err := domain.ParsePassphrase("correct horse battery staple")
	if err != nil {
		t.Fatal(err)
	}

	material, err := weak.Hash(passphrase)
	if err != nil {
		t.Fatal(err)
	}

	matches, err := strong.Verify(material, "correct horse battery staple")
	if err != nil {
		t.Fatalf("verify old material with new parameters: %v", err)
	}
	if !matches {
		t.Error("credential hashed at a lower cost no longer verifies")
	}
}

func TestVerifyRejectsUnparseableMaterial(t *testing.T) {
	hasher := hashing.NewArgon2Hasher(hashing.DefaultArgon2Params())

	// A wrong passphrase is (false, nil); material that cannot be interpreted is
	// a fault and must be reported as one rather than as a failed login.
	if _, err := hasher.Verify("$bcrypt$whatever", "x"); !errors.Is(err, hashing.ErrUnsupportedHash) {
		t.Errorf("got %v, want ErrUnsupportedHash", err)
	}
}

// --- correlation ---

func TestCorrelationIdentifierIsEchoed(t *testing.T) {
	h, _ := newHarness(t)

	supplied := "test-correlation-" + id.New()
	request, err := http.NewRequest(http.MethodGet, h.server.URL+"/v1/me", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set(httpx.CorrelationHeader, supplied)

	response, err := h.server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()

	// NF-16: a client must be able to quote an identifier that appears in logs.
	if got := response.Header.Get(httpx.CorrelationHeader); got != supplied {
		t.Errorf("correlation header = %q, want %q", got, supplied)
	}
}

func TestUnknownFieldsAreRejected(t *testing.T) {
	h, _ := newHarness(t)

	// A client sending "password" where the field is "passphrase" must be told,
	// not silently registered with an empty passphrase.
	body := fmt.Sprintf(`{"handle":%q,"email":"a@example.com","password":"correct horse battery staple"}`, uniqueHandle())
	request, err := http.NewRequest(http.MethodPost, h.server.URL+"/v1/accounts", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")

	response, err := h.server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusBadRequest {
		t.Errorf("status %d, want %d", response.StatusCode, http.StatusBadRequest)
	}
}

// --- domain events ---

func TestRegistrationPublishesItsEvents(t *testing.T) {
	h, _ := newHarness(t)
	h.events.reset()

	h.register(uniqueHandle())

	names := h.events.names()
	for _, want := range []string{
		"identity.account_registered",
		"identity.credential_added",
		"identity.device_registered",
		"identity.session_started",
	} {
		if !contains(names, want) {
			t.Errorf("published %v, missing %q", names, want)
		}
	}
}

func TestRevocationPublishesDeviceRevoked(t *testing.T) {
	h, _ := newHarness(t)
	handle := uniqueHandle()
	first := h.register(handle)

	var second session
	h.do(http.MethodPost, "/v1/sessions", "", map[string]string{
		"handle": handle, "passphrase": "correct horse battery staple", "device_name": "phone",
	}, &second)

	h.events.reset()
	if status := h.do(http.MethodDelete, "/v1/devices/"+second.DeviceID, first.AccessToken, nil, nil); status != http.StatusNoContent {
		t.Fatalf("revoke: status %d", status)
	}

	// Messaging will consume this to close the revoked device's WebSocket rather
	// than letting it receive entries until its access token happens to expire.
	if names := h.events.names(); !contains(names, "identity.device_revoked") {
		t.Errorf("published %v, missing domain.device_revoked", names)
	}
}

func TestRevokingTwicePublishesOnce(t *testing.T) {
	h, _ := newHarness(t)
	handle := uniqueHandle()
	first := h.register(handle)

	var second session
	h.do(http.MethodPost, "/v1/sessions", "", map[string]string{
		"handle": handle, "passphrase": "correct horse battery staple", "device_name": "phone",
	}, &second)

	h.events.reset()
	h.do(http.MethodDelete, "/v1/devices/"+second.DeviceID, first.AccessToken, nil, nil)
	// The second revocation is accepted but changes nothing, so a consumer must
	// not see the device revoked twice.
	if status := h.do(http.MethodDelete, "/v1/devices/"+second.DeviceID, first.AccessToken, nil, nil); status != http.StatusNoContent {
		t.Fatalf("second revoke: status %d", status)
	}

	var revocations int
	for _, name := range h.events.names() {
		if name == "identity.device_revoked" {
			revocations++
		}
	}
	if revocations != 1 {
		t.Errorf("published %d revocations, want 1", revocations)
	}
}

func TestFailedRegistrationPublishesNothing(t *testing.T) {
	h, _ := newHarness(t)
	handle := uniqueHandle()
	h.register(handle)

	h.events.reset()
	// A rejected use case must announce nothing: a consumer reacting to an
	// account that was never created is worse than no event at all.
	if status := h.do(http.MethodPost, "/v1/accounts", "", map[string]string{
		"handle":     strings.ToUpper(handle),
		"email":      "other-" + handle + "@example.com",
		"passphrase": "correct horse battery staple",
	}, nil); status != http.StatusConflict {
		t.Fatalf("expected conflict")
	}

	if names := h.events.names(); len(names) != 0 {
		t.Errorf("published %v after a rejected registration, want none", names)
	}
}

func TestRefreshSurvivesAsOneStateTransition(t *testing.T) {
	h, _ := newHarness(t)
	created := h.register(uniqueHandle())

	// Rotation is one UPDATE on one aggregate. Modelled as a delete plus two
	// inserts, a crash mid-rotation would leave a device with no usable tokens;
	// here the pre- and post-states are the only two possible outcomes.
	var refreshed struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
	}
	if status := h.do(http.MethodPost, "/v1/sessions/refresh", "",
		map[string]string{"refresh_token": created.RefreshToken}, &refreshed); status != http.StatusCreated {
		t.Fatalf("refresh: status %d", status)
	}

	// The old access token dies with the rotation, not fifteen minutes later.
	if status := h.do(http.MethodGet, "/v1/me", created.AccessToken, nil, nil); status != http.StatusUnauthorized {
		t.Errorf("pre-rotation access token: status %d, want %d", status, http.StatusUnauthorized)
	}
	if status := h.do(http.MethodGet, "/v1/me", refreshed.AccessToken, nil, nil); status != http.StatusOK {
		t.Errorf("post-rotation access token: status %d, want %d", status, http.StatusOK)
	}
}
