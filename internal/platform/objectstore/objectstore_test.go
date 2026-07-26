// Package objectstore_test runs against the real MinIO from docker-compose.
//
// A fake store would accept whatever signature this package produces, which is the
// one thing under test. So these are round trips: sign, send, and let a real S3
// implementation judge.
package objectstore_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"comms/internal/platform/id"
	"comms/internal/platform/objectstore"
)

// open returns a store against a bucket of this test run's own, skipping when there
// is no MinIO to talk to.
func open(t *testing.T) *objectstore.Store {
	t.Helper()

	endpoint := os.Getenv("S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("S3_ENDPOINT not set, skipping object store test")
	}

	store, err := objectstore.New(objectstore.Config{
		Endpoint:  endpoint,
		Bucket:    "test-" + id.New(),
		Region:    os.Getenv("S3_REGION"),
		AccessKey: os.Getenv("S3_ACCESS_KEY"),
		SecretKey: os.Getenv("S3_SECRET_KEY"),
	}, time.Now)
	if err != nil {
		t.Fatalf("configure store: %v", err)
	}

	if err := store.EnsureBucket(context.Background()); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	return store
}

func TestAnObjectSurvivesARoundTrip(t *testing.T) {
	t.Parallel()
	store := open(t)
	ctx := context.Background()

	content := []byte("the bytes a client uploaded")
	if err := store.Put(ctx, "photos/one/original", "application/octet-stream", content); err != nil {
		t.Fatalf("put: %v", err)
	}

	fetched, err := store.Get(ctx, "photos/one/original")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(fetched, content) {
		t.Fatalf("got %q, want %q", fetched, content)
	}
}

func TestEnsureBucketIsIdempotent(t *testing.T) {
	t.Parallel()
	store := open(t)

	// Called on every boot, so the second call must be as uneventful as the first.
	if err := store.EnsureBucket(context.Background()); err != nil {
		t.Fatalf("second EnsureBucket: %v", err)
	}
}

func TestSizeDistinguishesAbsentFromEmpty(t *testing.T) {
	t.Parallel()
	store := open(t)
	ctx := context.Background()

	if _, found, err := store.Size(ctx, "nothing/here"); err != nil || found {
		t.Fatalf("absent object: found=%v err=%v", found, err)
	}

	if err := store.Put(ctx, "declared/original", "text/plain", []byte("0123456789")); err != nil {
		t.Fatalf("put: %v", err)
	}

	size, found, err := store.Size(ctx, "declared/original")
	if err != nil || !found {
		t.Fatalf("present object: found=%v err=%v", found, err)
	}
	if size != 10 {
		t.Fatalf("got size %d, want 10", size)
	}
}

func TestDeleteRemovesAnObjectAndToleratesItBeingGone(t *testing.T) {
	t.Parallel()
	store := open(t)
	ctx := context.Background()

	if err := store.Put(ctx, "temporary/original", "text/plain", []byte("x")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := store.Delete(ctx, "temporary/original"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, found, _ := store.Size(ctx, "temporary/original"); found {
		t.Fatal("object still present after delete")
	}
	if err := store.Delete(ctx, "temporary/original"); err != nil {
		t.Fatalf("delete of an absent object: %v", err)
	}
}

// TestAPresignedURLUploadsWithoutCredentials is the property the whole upload flow
// rests on: a client holding only the URL can write the object, and the bytes never
// pass through this process.
func TestAPresignedURLUploadsWithoutCredentials(t *testing.T) {
	t.Parallel()
	store := open(t)
	ctx := context.Background()

	content := []byte("uploaded straight to the store")
	url := store.Presign(http.MethodPut, "direct/original", map[string]string{
		"content-type":   "image/jpeg",
		"content-length": strconv.Itoa(len(content)),
	}, time.Minute)

	upload(t, url, "image/jpeg", content, http.StatusOK)

	fetched, err := store.Get(ctx, "direct/original")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(fetched, content) {
		t.Fatal("what the store holds is not what was uploaded")
	}
}

// TestASignedLengthCannotBeExceeded is MD-4 enforced where it costs nothing: the
// store refuses a body of the wrong size, so the cap holds on a request the api never
// sees and no oversized transfer completes.
func TestASignedLengthCannotBeExceeded(t *testing.T) {
	t.Parallel()
	store := open(t)

	declared := 10
	url := store.Presign(http.MethodPut, "lying/original", map[string]string{
		"content-type":   "image/jpeg",
		"content-length": strconv.Itoa(declared),
	}, time.Minute)

	// Twice what was declared and signed for.
	upload(t, url, "image/jpeg", bytes.Repeat([]byte("x"), declared*2), http.StatusForbidden)

	if _, found, _ := store.Size(context.Background(), "lying/original"); found {
		t.Fatal("the store kept an object it should have refused")
	}
}

// TestASignedContentTypeCannotBeSwapped stops an uploader turning an image
// attachment into whatever it likes — the recorded content type is what clients
// render by, and the store is what makes the declaration binding.
func TestASignedContentTypeCannotBeSwapped(t *testing.T) {
	t.Parallel()
	store := open(t)

	content := []byte("<script>alert(1)</script>")
	url := store.Presign(http.MethodPut, "swapped/original", map[string]string{
		"content-type":   "image/jpeg",
		"content-length": strconv.Itoa(len(content)),
	}, time.Minute)

	upload(t, url, "text/html", content, http.StatusForbidden)
}

func TestAnExpiredURLIsRefused(t *testing.T) {
	t.Parallel()
	open(t) // for its skip when there is no store to talk to.

	// Signed as of two hours ago with a one-minute life, which is the same thing the
	// store sees when a client sits on a URL.
	stale, err := objectstore.New(objectstore.Config{
		Endpoint:  os.Getenv("S3_ENDPOINT"),
		Bucket:    "test-" + id.New(),
		AccessKey: os.Getenv("S3_ACCESS_KEY"),
		SecretKey: os.Getenv("S3_SECRET_KEY"),
	}, func() time.Time { return time.Now().Add(-2 * time.Hour) })
	if err != nil {
		t.Fatalf("configure store: %v", err)
	}

	url := stale.Presign(http.MethodPut, "late/original", nil, time.Minute)
	upload(t, url, "", []byte("too late"), http.StatusForbidden)
}

// upload sends one PUT to a presigned URL with no credentials of its own, and asserts
// the status. This is deliberately a plain http.Client: what a browser does.
func upload(t *testing.T, url, contentType string, body []byte, want int) {
	t.Helper()

	request, err := http.NewRequestWithContext(
		context.Background(), http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != want {
		detail, _ := io.ReadAll(io.LimitReader(response.Body, 512))
		t.Fatalf("got %s, want %d: %s", response.Status, want, detail)
	}
}

func TestConfigurationIsCheckedAtBoot(t *testing.T) {
	t.Parallel()

	// A missing key must fail where it can be seen, not on the first upload of the
	// day. Nothing here reaches the network.
	if _, err := objectstore.New(objectstore.Config{Endpoint: "http://localhost:9000"}, time.Now); err == nil {
		t.Fatal("a store with no bucket or credentials was accepted")
	}
}
