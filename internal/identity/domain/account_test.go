package domain_test

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"comms/internal/identity/domain"
)

func TestParseHandle(t *testing.T) {
	valid := []string{"ana", "ana_costa", "ana.costa", "a1b2c3", "Ana", strings.Repeat("a", 32)}
	for _, input := range valid {
		if _, err := domain.ParseHandle(input); err != nil {
			t.Errorf("ParseHandle(%q) = %v, want valid", input, err)
		}
	}

	invalid := map[string]string{
		"too short":           "an",
		"too long":            strings.Repeat("a", 33),
		"leading digit":       "1ana",
		"leading underscore":  "_ana",
		"trailing underscore": "ana_",
		"trailing dot":        "ana.",
		"space":               "ana costa",
		"at sign":             "ana@costa",
		"non-ascii lookalike": "anа", // Cyrillic а
		"emoji":               "ana👋",
		"empty":               "",
	}
	for name, input := range invalid {
		t.Run(name, func(t *testing.T) {
			if _, err := domain.ParseHandle(input); err == nil {
				t.Errorf("ParseHandle(%q) = nil, want error", input)
			}
		})
	}
}

func TestHandleNormalisedIsCaseInsensitive(t *testing.T) {
	upper, err := domain.ParseHandle("AnaCosta")
	if err != nil {
		t.Fatal(err)
	}
	lower, err := domain.ParseHandle("anacosta")
	if err != nil {
		t.Fatal(err)
	}
	if upper.Normalised() != lower.Normalised() {
		t.Errorf("%q and %q normalise differently", upper, lower)
	}
}

func TestParseEmail(t *testing.T) {
	if _, err := domain.ParseEmail("ana@example.com"); err != nil {
		t.Errorf("valid email rejected: %v", err)
	}
	// A display-name form would otherwise be stored verbatim and then fail to
	// match itself on lookup.
	if _, err := domain.ParseEmail("Ana <ana@example.com>"); err == nil {
		t.Error("display-name form accepted, want rejected")
	}
	if _, err := domain.ParseEmail("not-an-email"); err == nil {
		t.Error("malformed email accepted, want rejected")
	}
}

func registerAccount(t *testing.T, handle string) *domain.Account {
	t.Helper()

	account, err := domain.RegisterAccount(
		"account-1", "credential-1", handle, "ana@example.com",
		domain.KindPassword, "$argon2id$digest", time.Now(),
	)
	if err != nil {
		t.Fatal(err)
	}
	return account
}

func TestRegisterAccountReportsFieldOnInvalidInput(t *testing.T) {
	_, err := domain.RegisterAccount(
		"account-1", "credential-1", "_bad", "ana@example.com",
		domain.KindPassword, "$argon2id$digest", time.Now(),
	)

	var validation domain.ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("got %v, want ValidationError", err)
	}
	if validation.Field != "handle" {
		t.Errorf("Field = %q, want %q", validation.Field, "handle")
	}
}

func TestRegisterAccountCreatesItsFirstCredential(t *testing.T) {
	account := registerAccount(t, "ana")

	// An account nobody can log into is not a state worth representing, so the
	// credential arrives with the account rather than in a second step.
	if _, found := account.PasswordCredential(); !found {
		t.Error("registered account has no password credential")
	}
}

func TestRegisterAccountRecordsItsEvents(t *testing.T) {
	account := registerAccount(t, "ana")

	names := eventNames(account.TakeEvents())
	for _, want := range []string{"identity.account_registered", "identity.credential_added"} {
		if !slices.Contains(names, want) {
			t.Errorf("events %v do not include %q", names, want)
		}
	}
}

func TestTakeEventsDrains(t *testing.T) {
	account := registerAccount(t, "ana")

	if len(account.TakeEvents()) == 0 {
		t.Fatal("first take returned nothing")
	}
	// Draining rather than reading is what stops two callers publishing the same
	// event twice.
	if taken := account.TakeEvents(); len(taken) != 0 {
		t.Errorf("second take returned %d events, want 0", len(taken))
	}
}

func TestAccountAllowsOnlyOnePassword(t *testing.T) {
	account := registerAccount(t, "ana")
	account.TakeEvents()

	err := account.AddCredential("credential-2", domain.KindPassword, "$argon2id$other", time.Now())
	if !errors.Is(err, domain.ErrPasswordAlreadySet) {
		t.Errorf("got %v, want ErrPasswordAlreadySet", err)
	}
	// A rejected change must record nothing: consumers would otherwise react to
	// something that did not happen.
	if taken := account.TakeEvents(); len(taken) != 0 {
		t.Errorf("rejected AddCredential recorded %d events, want 0", len(taken))
	}
}

func TestAccountAllowsSeveralPasskeys(t *testing.T) {
	account := registerAccount(t, "ana")

	// Unconstrained on purpose: an account will legitimately register a passkey
	// per device.
	if err := account.AddCredential("credential-2", domain.KindPasskey, "public-key-a", time.Now()); err != nil {
		t.Fatalf("first passkey: %v", err)
	}
	if err := account.AddCredential("credential-3", domain.KindPasskey, "public-key-b", time.Now()); err != nil {
		t.Errorf("second passkey: %v", err)
	}
}

func TestCredentialsReturnsACopy(t *testing.T) {
	account := registerAccount(t, "ana")

	credentials := account.Credentials()
	credentials[0] = domain.Credential{}

	// Handing out the backing slice would let a caller empty the aggregate from
	// outside, which is the whole reason the fields are unexported.
	if _, found := account.PasswordCredential(); !found {
		t.Error("mutating the returned slice altered the aggregate")
	}
}

func TestCredentialAndPassphraseDoNotPrintTheirSecrets(t *testing.T) {
	account := registerAccount(t, "ana")
	credential, _ := account.PasswordCredential()

	if strings.Contains(fmt.Sprintf("%v %+v", credential, credential), "argon2id") {
		t.Error("credential formatting leaks its material")
	}

	passphrase, err := domain.ParsePassphrase("correct horse battery staple")
	if err != nil {
		t.Fatal(err)
	}
	// Both verbs, because %v and %#v take different paths and a secret that
	// survives one of them is a secret in the logs.
	if formatted := fmt.Sprintf("%v %#v", passphrase, passphrase); strings.Contains(formatted, "horse") {
		t.Errorf("passphrase formatting leaks its value: %s", formatted)
	}
	if passphrase.Reveal() != "correct horse battery staple" {
		t.Error("Reveal does not return the passphrase")
	}
}

func TestParsePassphraseEnforcesLength(t *testing.T) {
	if _, err := domain.ParsePassphrase("short"); err == nil {
		t.Error("short passphrase accepted")
	}
	if _, err := domain.ParsePassphrase(strings.Repeat("a", 1025)); err == nil {
		t.Error("oversized passphrase accepted")
	}
	if _, err := domain.ParsePassphrase(strings.Repeat("a", 12)); err != nil {
		t.Errorf("12-character passphrase rejected: %v", err)
	}
}

func eventNames(events []domain.Event) []string {
	names := make([]string, 0, len(events))
	for _, event := range events {
		names = append(names, event.EventName())
	}
	return names
}

func TestDeviceRevocationIsIdempotent(t *testing.T) {
	device, err := domain.RegisterDevice("device-1", "account-1", "laptop", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if device.Revoked() {
		t.Fatal("new device reports revoked")
	}
	device.TakeEvents()

	first := time.Now()
	if changed := device.Revoke(first); !changed {
		t.Error("first Revoke reported no change")
	}
	if changed := device.Revoke(first.Add(time.Hour)); changed {
		t.Error("second Revoke reported a change")
	}

	// When a device stopped being trusted is a fact worth preserving, so a second
	// revocation must not move it forward or announce itself again.
	if !device.RevokedAt().Equal(first) {
		t.Errorf("RevokedAt = %v, want %v", device.RevokedAt(), first)
	}
	if names := eventNames(device.TakeEvents()); len(names) != 1 {
		t.Errorf("recorded %v, want exactly one DeviceRevoked", names)
	}
}

func TestDeviceBelongsTo(t *testing.T) {
	device, err := domain.RegisterDevice("device-1", "account-1", "laptop", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if !device.BelongsTo("account-1") {
		t.Error("device does not belong to its own account")
	}
	if device.BelongsTo("account-2") {
		t.Error("device belongs to an unrelated account")
	}
}

func TestStartSessionProducesDistinctSecrets(t *testing.T) {
	now := time.Now()

	first, firstSecrets, err := domain.StartSession("session-1", "device-1", now)
	if err != nil {
		t.Fatal(err)
	}
	_, secondSecrets, err := domain.StartSession("session-2", "device-1", now)
	if err != nil {
		t.Fatal(err)
	}

	if firstSecrets.Access == secondSecrets.Access || firstSecrets.Refresh == secondSecrets.Refresh {
		t.Error("two sessions share a secret")
	}
	if firstSecrets.Access == firstSecrets.Refresh {
		t.Error("access and refresh secrets are identical")
	}
	if err := first.VerifyAccess(firstSecrets.Access, now); err != nil {
		t.Errorf("session rejects its own access secret: %v", err)
	}
	if err := first.VerifyAccess(secondSecrets.Access, now); err == nil {
		t.Error("session accepts another session's access secret")
	}
	// The secret must not be recoverable from what is stored.
	if string(first.AccessDigest()) == firstSecrets.Access {
		t.Error("digest equals secret")
	}
}

func TestSessionRejectsAccessSecretForRotation(t *testing.T) {
	now := time.Now()
	session, secrets, err := domain.StartSession("session-1", "device-1", now)
	if err != nil {
		t.Fatal(err)
	}

	// The two are indistinguishable strings, so this check is the only thing
	// stopping a fifteen-minute token being traded for a thirty-day one.
	if _, err := session.Rotate(secrets.Access, now); !errors.Is(err, domain.ErrInvalidToken) {
		t.Errorf("got %v, want ErrInvalidToken", err)
	}
	if err := session.VerifyAccess(secrets.Refresh, now); !errors.Is(err, domain.ErrInvalidToken) {
		t.Errorf("refresh secret accepted as access: got %v", err)
	}
}

func TestSessionRotationInvalidatesBothOldSecrets(t *testing.T) {
	now := time.Now()
	session, original, err := domain.StartSession("session-1", "device-1", now)
	if err != nil {
		t.Fatal(err)
	}
	session.TakeEvents()

	rotated, err := session.Rotate(original.Refresh, now)
	if err != nil {
		t.Fatal(err)
	}

	if rotated.Refresh == original.Refresh || rotated.Access == original.Access {
		t.Error("rotation reused a secret")
	}
	// One state transition on one aggregate: both old secrets die together, so a
	// stolen refresh token is usable at most once.
	if _, err := session.Rotate(original.Refresh, now); !errors.Is(err, domain.ErrInvalidToken) {
		t.Errorf("replayed refresh secret: got %v, want ErrInvalidToken", err)
	}
	if err := session.VerifyAccess(original.Access, now); !errors.Is(err, domain.ErrInvalidToken) {
		t.Errorf("pre-rotation access secret still valid: got %v", err)
	}
	if err := session.VerifyAccess(rotated.Access, now); err != nil {
		t.Errorf("rotated access secret rejected: %v", err)
	}
	if names := eventNames(session.TakeEvents()); !slices.Contains(names, "identity.session_rotated") {
		t.Errorf("events %v do not include a rotation", names)
	}
}

func TestExpiredRefreshCannotRotate(t *testing.T) {
	now := time.Now()
	session, secrets, err := domain.StartSession("session-1", "device-1", now)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := session.Rotate(secrets.Refresh, now.Add(domain.RefreshTokenLifetime)); !errors.Is(err, domain.ErrTokenExpired) {
		t.Errorf("got %v, want ErrTokenExpired", err)
	}
}

func TestSessionLifetimesDifferByToken(t *testing.T) {
	now := time.Now()
	session, secrets, err := domain.StartSession("session-1", "device-1", now)
	if err != nil {
		t.Fatal(err)
	}

	if !session.AccessExpiresAt().Before(session.RefreshExpiresAt()) {
		t.Error("access token does not expire before refresh token")
	}
	if err := session.VerifyAccess(secrets.Access, now); err != nil {
		t.Errorf("freshly issued token rejected: %v", err)
	}
	// At the expiry instant, not after it: a token valid exactly at its deadline
	// is an off-by-one waiting to be exploited.
	if err := session.VerifyAccess(secrets.Access, session.AccessExpiresAt()); !errors.Is(err, domain.ErrTokenExpired) {
		t.Errorf("token at its expiry instant: got %v, want ErrTokenExpired", err)
	}
}
