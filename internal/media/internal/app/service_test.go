// Package app_test drives Media's use cases against real Postgres and real MinIO.
//
// The interesting properties here are all about what happens twice — a completion
// retried, an upload processed again after a crash — and about a cap that has to hold
// before any transfer starts. None of those can be established against a fake store
// that agrees with whatever this code believes.
package app_test

import (
	"bytes"
	"context"
	"errors"
	"image"
	"image/color"
	"image/jpeg"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"comms/internal/media/internal/app"
	"comms/internal/media/internal/domain"
	"comms/internal/media/internal/postgres"
	"comms/internal/platform/database"
	"comms/internal/platform/database/testdb"
	"comms/internal/platform/id"
	"comms/internal/platform/objectstore"
)

// permissive says yes, so that tests about the lifecycle are not also tests about
// authorisation. The tests that care set the fields.
type permissive struct {
	refuseAttach bool
	refuseView   bool
}

func (p permissive) MayAttach(context.Context, string, string) (bool, error) {
	return !p.refuseAttach, nil
}

func (p permissive) MayView(context.Context, string, string, string) (bool, error) {
	return !p.refuseView, nil
}

// recordingNotifier remembers what clients were told to look at again.
type recordingNotifier struct {
	mutex   sync.Mutex
	changed []string
}

func (n *recordingNotifier) AttachmentChanged(_ context.Context, _, attachmentID string) error {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	n.changed = append(n.changed, attachmentID)
	return nil
}

func (n *recordingNotifier) saw(attachmentID string) int {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	count := 0
	for _, seen := range n.changed {
		if seen == attachmentID {
			count++
		}
	}
	return count
}

// failingStore wraps a real store and breaks one operation on demand, which is how the
// crash-mid-processing path is reached deliberately rather than hoped for.
type failingStore struct {
	*objectstore.Store
	failGet bool
}

func (s *failingStore) Get(ctx context.Context, key string) ([]byte, error) {
	if s.failGet {
		return nil, errors.New("object store unreachable")
	}
	return s.Store.Get(ctx, key) //nolint:wrapcheck // the real store's error is the point.
}

// harness is a wired service over real infrastructure.
type harness struct {
	service      *app.Service
	store        *failingStore
	repository   *postgres.AttachmentRepository
	notifier     *recordingNotifier
	conversation domain.ConversationID
	owner        domain.AccountID
}

func newHarness(t *testing.T, permission permissive) *harness {
	t.Helper()

	db := testdb.Open(t)

	endpoint := requireEnv(t, "S3_ENDPOINT")
	store, err := objectstore.New(objectstore.Config{
		Endpoint:  endpoint,
		Bucket:    "media-" + id.New(),
		AccessKey: requireEnv(t, "S3_ACCESS_KEY"),
		SecretKey: requireEnv(t, "S3_SECRET_KEY"),
	}, time.Now)
	if err != nil {
		t.Fatalf("configure store: %v", err)
	}
	if err := store.EnsureBucket(context.Background()); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	wrapped := &failingStore{Store: store}
	repository := postgres.NewAttachmentRepository(db)
	notifier := &recordingNotifier{}

	return &harness{
		service: app.NewService(
			repository, wrapped, permission, notifier,
			postgres.NewOutboxPublisher(db), database.NewConn(db),
			time.Now, slog.New(slog.DiscardHandler),
		),
		store:      wrapped,
		repository: repository,
		notifier:   notifier,
		// Real identifiers, because the columns are uuid and a test that writes
		// "conversation-1" would be testing nothing about the schema.
		conversation: domain.ConversationID(id.New()),
		owner:        domain.AccountID(id.New()),
	}
}

func requireEnv(t *testing.T, name string) string {
	t.Helper()

	value := os.Getenv(name)
	if value == "" {
		t.Skipf("%s not set, skipping media integration test", name)
	}
	return value
}

// fetch reads a signed URL with no credentials, which is what a browser does with a
// variant in an img tag.
func fetch(t *testing.T, url string) []byte {
	t.Helper()

	response, err := http.Get(url) //nolint:noctx,gosec // a test fetching its own signed URL.
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		t.Fatalf("fetch returned %s", response.Status)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return body
}

// outboxJobs counts the processing jobs enqueued for an attachment.
//
// Read from the outbox rather than from a fake publisher, because what must be true is
// that one row exists — the row is the job, and a second row is a second derivation.
func outboxJobs(t *testing.T, attachmentID string) int {
	t.Helper()

	var count int
	err := testdb.Open(t).QueryRow(
		`SELECT count(*) FROM outbox WHERE event_name = $1 AND key = $2`,
		"media.attachment_uploaded", attachmentID,
	).Scan(&count)
	if err != nil {
		t.Fatalf("count outbox rows: %v", err)
	}
	return count
}

// photo returns an encoded JPEG that decodes to something with detail in it.
func photo(t *testing.T, width, height int) []byte {
	t.Helper()

	source := image.NewRGBA(image.Rect(0, 0, width, height))
	for y := range height {
		for x := range width {
			source.Set(x, y, color.RGBA{R: uint8(x % 256), G: uint8(y % 256), B: 64, A: 255})
		}
	}

	var encoded bytes.Buffer
	if err := jpeg.Encode(&encoded, source, nil); err != nil {
		t.Fatalf("encode photo: %v", err)
	}
	return encoded.Bytes()
}

// upload does what a client does: PUT the bytes straight to the store, with no
// credentials of its own.
func upload(t *testing.T, target app.Upload, contentType string, body []byte) {
	t.Helper()

	request, err := http.NewRequestWithContext(
		context.Background(), target.Method, target.URL, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build upload: %v", err)
	}
	request.Header.Set("Content-Type", contentType)

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		t.Fatalf("upload returned %s", response.Status)
	}
}

// sent runs the whole client-side sequence and returns the attachment.
func (h *harness) sent(t *testing.T, contentType string, body []byte) domain.AttachmentID {
	t.Helper()
	ctx := context.Background()

	target, err := h.service.RequestUpload(
		ctx, h.owner, h.conversation, contentType, int64(len(body)))
	if err != nil {
		t.Fatalf("request upload: %v", err)
	}

	upload(t, target, contentType, body)

	if _, err := h.service.CompleteUpload(
		ctx, h.owner, domain.AttachmentID(target.AttachmentID)); err != nil {
		t.Fatalf("complete upload: %v", err)
	}
	return domain.AttachmentID(target.AttachmentID)
}

// TestAPhotoBecomesDisplayable is the whole flow: a URL, a direct transfer, a
// completion, and variants that can actually be fetched.
func TestAPhotoBecomesDisplayable(t *testing.T) {
	t.Parallel()
	harness := newHarness(t, permissive{})
	ctx := context.Background()

	attachmentID := harness.sent(t, "image/jpeg", photo(t, 1600, 1200))

	// Before processing, the attachment is referenced and not displayable — which is
	// what a client renders as a placeholder (MD-1).
	view, err := harness.service.Attachment(ctx, harness.owner, attachmentID)
	if err != nil {
		t.Fatalf("view before processing: %v", err)
	}
	if view.Attachment.Displayable() {
		t.Fatal("an unprocessed attachment is displayable")
	}
	if len(view.VariantURLs) != 0 {
		t.Fatalf("got %d variant URLs before processing", len(view.VariantURLs))
	}

	if err := harness.service.Process(ctx, attachmentID); err != nil {
		t.Fatalf("process: %v", err)
	}

	view, err = harness.service.Attachment(ctx, harness.owner, attachmentID)
	if err != nil {
		t.Fatalf("view after processing: %v", err)
	}
	if !view.Attachment.Displayable() {
		t.Fatalf("state is %q after processing", view.Attachment.State())
	}
	if len(view.VariantURLs) != 2 {
		t.Fatalf("got %d variants, want thumbnail and display", len(view.VariantURLs))
	}

	// The URLs are what a browser puts in an img tag, so they have to work with no
	// credentials attached.
	thumbnail := fetch(t, view.VariantURLs[string(domain.VariantThumbnail)])
	if len(thumbnail) == 0 {
		t.Fatal("the thumbnail URL returned nothing")
	}
	config, _, err := image.DecodeConfig(bytes.NewReader(thumbnail))
	if err != nil {
		t.Fatalf("the thumbnail is not an image: %v", err)
	}
	if config.Width != 320 {
		t.Fatalf("thumbnail is %d wide, want 320", config.Width)
	}

	// The original is retained (MD-5), which is what makes any variant regenerable.
	if len(fetch(t, view.OriginalURL)) == 0 {
		t.Fatal("the original is not readable")
	}

	if harness.notifier.saw(string(attachmentID)) != 1 {
		t.Fatalf("clients were told %d times", harness.notifier.saw(string(attachmentID)))
	}
}

// TestProcessingTwiceLeavesOneSetOfVariants is MD-2, and it is the property
// at-least-once delivery makes mandatory rather than desirable.
func TestProcessingTwiceLeavesOneSetOfVariants(t *testing.T) {
	t.Parallel()
	harness := newHarness(t, permissive{})
	ctx := context.Background()

	attachmentID := harness.sent(t, "image/jpeg", photo(t, 800, 600))

	for pass := range 3 {
		if err := harness.service.Process(ctx, attachmentID); err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
	}

	attachment, err := harness.repository.Find(ctx, attachmentID)
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	if got := len(attachment.Variants()); got != 2 {
		t.Fatalf("got %d variants after three passes, want 2", got)
	}
	if attachment.State() != domain.StateReady {
		t.Fatalf("state is %q", attachment.State())
	}
}

// TestACrashMidProcessingLeavesTheJobReplayable is MD-3. The failure is injected at
// the point a worker would die — after the job was published, before the variants
// exist — and what must be true afterwards is that a retry completes it.
func TestACrashMidProcessingLeavesTheJobReplayable(t *testing.T) {
	t.Parallel()
	harness := newHarness(t, permissive{})
	ctx := context.Background()

	attachmentID := harness.sent(t, "image/jpeg", photo(t, 800, 600))

	harness.store.failGet = true
	if err := harness.service.Process(ctx, attachmentID); err == nil {
		t.Fatal("processing succeeded with the store unreachable")
	}

	// The error is returned rather than swallowed, which is what leaves the Kafka
	// offset uncommitted and the record redelivered. The attachment must not have
	// been marked failed: that would be permanent, and this is not.
	attachment, err := harness.repository.Find(ctx, attachmentID)
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	if attachment.State() != domain.StateUploaded {
		t.Fatalf("state is %q after a transient failure, want uploaded", attachment.State())
	}

	harness.store.failGet = false
	if err := harness.service.Process(ctx, attachmentID); err != nil {
		t.Fatalf("replay: %v", err)
	}

	attachment, err = harness.repository.Find(ctx, attachmentID)
	if err != nil {
		t.Fatalf("find after replay: %v", err)
	}
	if attachment.State() != domain.StateReady {
		t.Fatalf("state is %q after the replay", attachment.State())
	}
}

// TestSomethingThatIsNotAPhotoFailsPermanently is the other half of MD-3: a crash is
// retried, but content that will never decode must not be, or one bad upload occupies
// a consumer forever.
func TestSomethingThatIsNotAPhotoFailsPermanently(t *testing.T) {
	t.Parallel()
	harness := newHarness(t, permissive{})
	ctx := context.Background()

	// Declared as a photo, and is not one. The store enforces the declared type and
	// length, not that the bytes decode.
	attachmentID := harness.sent(t, "image/jpeg", []byte("this is not a photo at all"))

	// No error: the job is finished, and finished means the offset commits.
	if err := harness.service.Process(ctx, attachmentID); err != nil {
		t.Fatalf("process: %v", err)
	}

	attachment, err := harness.repository.Find(ctx, attachmentID)
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	if attachment.State() != domain.StateFailed {
		t.Fatalf("state is %q, want failed", attachment.State())
	}
	if attachment.Failure() == "" {
		t.Fatal("a failed attachment with no reason recorded")
	}
	// Clients are told, so a placeholder can become "this could not be processed"
	// rather than a spinner that never stops.
	if harness.notifier.saw(string(attachmentID)) != 1 {
		t.Fatal("clients were not told about the failure")
	}
}

// TestVideoBecomesReadyWithoutVariants records the deliberate limit, and the reason it
// is safe: the lifecycle is identical to a photo's, so a client has one shape of state
// to handle.
func TestVideoBecomesReadyWithoutVariants(t *testing.T) {
	t.Parallel()
	harness := newHarness(t, permissive{})
	ctx := context.Background()

	// Not a real container, and it does not need to be: nothing decodes it.
	attachmentID := harness.sent(t, "video/mp4", bytes.Repeat([]byte("mp4"), 1000))

	if err := harness.service.Process(ctx, attachmentID); err != nil {
		t.Fatalf("process: %v", err)
	}

	view, err := harness.service.Attachment(ctx, harness.owner, attachmentID)
	if err != nil {
		t.Fatalf("view: %v", err)
	}
	if !view.Attachment.Displayable() {
		t.Fatalf("state is %q", view.Attachment.State())
	}
	if len(view.VariantURLs) != 0 {
		t.Fatalf("got %d variants for a video", len(view.VariantURLs))
	}
	// The original is how a video is played, so its URL must be there.
	if view.OriginalURL == "" {
		t.Fatal("a ready video has no URL to play")
	}
}

// TestTheSizeCapIsRefusedBeforeAnyTransfer is MD-4 at the api boundary: no URL is
// issued, so there is nothing to transfer.
func TestTheSizeCapIsRefusedBeforeAnyTransfer(t *testing.T) {
	t.Parallel()
	harness := newHarness(t, permissive{})

	oversized := domain.MaxBytes + 1
	target, err := harness.service.RequestUpload(
		context.Background(), harness.owner, harness.conversation, "video/mp4", oversized)
	if !errors.Is(err, domain.ErrTooLarge) {
		t.Fatalf("got %v, want ErrTooLarge", err)
	}
	if target.URL != "" {
		t.Fatal("an upload URL was issued for an oversized attachment")
	}
}

// TestAnOversizedTransferIsRefusedByTheStore is the other half of MD-4: a client that
// lies about the size cannot transfer more than it declared, because the length is part
// of the signature. api never reads a byte either way.
func TestAnOversizedTransferIsRefusedByTheStore(t *testing.T) {
	t.Parallel()
	harness := newHarness(t, permissive{})

	declared := int64(1024)
	target, err := harness.service.RequestUpload(
		context.Background(), harness.owner, harness.conversation, "image/jpeg", declared)
	if err != nil {
		t.Fatalf("request upload: %v", err)
	}

	request, err := http.NewRequestWithContext(context.Background(), target.Method, target.URL,
		bytes.NewReader(bytes.Repeat([]byte("x"), int(declared)*4)))
	if err != nil {
		t.Fatalf("build upload: %v", err)
	}
	request.Header.Set("Content-Type", "image/jpeg")

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("the store accepted %d bytes against a signature for %d: %s",
			declared*4, declared, response.Status)
	}
}

// TestCompletingBeforeTheBytesArriveIsRefused is the reachable half of the size
// check. A truncated transfer never lands at all — the store refuses a body whose
// length disagrees with the signature, so it is an absent object rather than a short
// one — and a client that calls completion after a failed upload must be told, not left
// with an attachment that is pending forever.
func TestCompletingBeforeTheBytesArriveIsRefused(t *testing.T) {
	t.Parallel()
	harness := newHarness(t, permissive{})
	ctx := context.Background()

	target, err := harness.service.RequestUpload(
		ctx, harness.owner, harness.conversation, "image/jpeg", 4096)
	if err != nil {
		t.Fatalf("request upload: %v", err)
	}

	// Nothing was uploaded.
	_, err = harness.service.CompleteUpload(ctx, harness.owner, domain.AttachmentID(target.AttachmentID))
	if !errors.Is(err, domain.ErrNotUploadedYet) {
		t.Fatalf("got %v, want ErrNotUploadedYet", err)
	}

	// And no job was enqueued, so nothing downstream is waiting on an upload that
	// never happened.
	if jobs := outboxJobs(t, target.AttachmentID); jobs != 0 {
		t.Fatalf("got %d jobs for an attachment with no bytes", jobs)
	}
}

// TestCompletingTwiceIsIdempotent is what makes a lost response harmless, and it is
// checked against the outbox: the second call must not enqueue a second job.
func TestCompletingTwiceIsIdempotent(t *testing.T) {
	t.Parallel()
	harness := newHarness(t, permissive{})
	ctx := context.Background()

	body := photo(t, 200, 200)
	target, err := harness.service.RequestUpload(
		ctx, harness.owner, harness.conversation, "image/jpeg", int64(len(body)))
	if err != nil {
		t.Fatalf("request upload: %v", err)
	}
	upload(t, target, "image/jpeg", body)

	for attempt := range 3 {
		if _, err := harness.service.CompleteUpload(
			ctx, harness.owner, domain.AttachmentID(target.AttachmentID)); err != nil {
			t.Fatalf("attempt %d: %v", attempt, err)
		}
	}

	if jobs := outboxJobs(t, target.AttachmentID); jobs != 1 {
		t.Fatalf("got %d processing jobs from three completions, want 1", jobs)
	}
}

// TestSomeoneElseCannotCompleteAnUpload: declaring another account's transfer finished
// is not a membership question.
func TestSomeoneElseCannotCompleteAnUpload(t *testing.T) {
	t.Parallel()
	harness := newHarness(t, permissive{})
	ctx := context.Background()

	body := photo(t, 100, 100)
	target, err := harness.service.RequestUpload(
		ctx, harness.owner, harness.conversation, "image/jpeg", int64(len(body)))
	if err != nil {
		t.Fatalf("request upload: %v", err)
	}
	upload(t, target, "image/jpeg", body)

	stranger := domain.AccountID(id.New())
	if _, err := harness.service.CompleteUpload(
		ctx, stranger, domain.AttachmentID(target.AttachmentID)); !errors.Is(err, domain.ErrNotPermitted) {
		t.Fatalf("got %v, want ErrNotPermitted", err)
	}
}

// TestSomeoneWhoMayNotWriteCannotObtainAnUploadURL: a reader in a channel, or a removed
// member, must not be handed a signed write to the bucket.
func TestSomeoneWhoMayNotWriteCannotObtainAnUploadURL(t *testing.T) {
	t.Parallel()
	harness := newHarness(t, permissive{refuseAttach: true})

	target, err := harness.service.RequestUpload(
		context.Background(), harness.owner, harness.conversation, "image/jpeg", 1024)
	if !errors.Is(err, domain.ErrNotPermitted) {
		t.Fatalf("got %v, want ErrNotPermitted", err)
	}
	if target.URL != "" {
		t.Fatal("a URL was issued to an account that may not write")
	}
}

// TestSomeoneWhoCannotSeeTheEntryCannotSeeTheAttachment is the visibility rule holding
// where it matters. Messaging answers it; what is checked here is that Media asks and
// honours the answer, including for an account that is in the conversation.
func TestSomeoneWhoCannotSeeTheEntryCannotSeeTheAttachment(t *testing.T) {
	t.Parallel()
	harness := newHarness(t, permissive{refuseView: true})
	ctx := context.Background()

	attachmentID := harness.sent(t, "image/jpeg", photo(t, 100, 100))

	other := domain.AccountID(id.New())
	if _, err := harness.service.Attachment(ctx, other, attachmentID); !errors.Is(err, domain.ErrNotPermitted) {
		t.Fatalf("got %v, want ErrNotPermitted", err)
	}

	// The owner still sees their own, which is what lets a client show what it just
	// uploaded before any entry references it.
	if _, err := harness.service.Attachment(ctx, harness.owner, attachmentID); err != nil {
		t.Fatalf("the owner was refused their own attachment: %v", err)
	}
}
