package messaging_test

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"comms/internal/messaging/internal/domain"
	"comms/internal/messaging/internal/postgres"
	"comms/internal/messaging/internal/projection"
	"comms/internal/platform/database/testdb"
	"comms/internal/platform/kafka"
)

// projections drives the projector without Kafka in the way.
//
// What is under test here is whether the projector draws the right conclusions from
// a sequence of events, and whether it draws the same ones twice. Putting a broker
// in the middle of that would test the broker.
type projections struct {
	t         *testing.T
	db        *sql.DB
	state     *postgres.MemberStateStore
	projector *projection.Projector
	// applied is how far through the outbox this harness has read, so that a second
	// pass can deliberately re-read from the beginning.
	applied int64
}

func newProjections(t *testing.T) *projections {
	t.Helper()

	db := testdb.Open(t)
	state := postgres.NewMemberStateStore(db)

	return &projections{
		t:     t,
		db:    db,
		state: state,
		// A nil producer for dead letters, which degrades to logging. These tests assert
		// what the projector does with a record, not where a skipped one is filed.
		projector: projection.NewProjector(state,
			kafka.NewDeadLetters(nil, "messaging-projections", slog.New(slog.DiscardHandler)),
			slog.New(slog.DiscardHandler)),
	}
}

// records reads outbox rows written since a starting point, as the relay would
// publish them.
func (p *projections) records(from int64) (records []kafka.Record, lastID int64) {
	p.t.Helper()

	rows, err := p.db.Query(
		`SELECT id, topic, key, event_name, payload FROM outbox WHERE id > $1 ORDER BY id`, from)
	if err != nil {
		p.t.Fatalf("read outbox: %v", err)
	}
	defer rows.Close()

	lastID = from
	for rows.Next() {
		var (
			id     int64
			record kafka.Record
		)
		if err := rows.Scan(&id, &record.Topic, &record.Key, &record.Name, &record.Value); err != nil {
			p.t.Fatalf("scan outbox: %v", err)
		}
		records = append(records, record)
		lastID = id
	}
	if err := rows.Err(); err != nil {
		p.t.Fatalf("read outbox: %v", err)
	}
	return records, lastID
}

// catchUp applies everything not yet applied.
func (p *projections) catchUp() {
	p.t.Helper()

	records, lastID := p.records(p.applied)
	p.apply(records)
	p.applied = lastID
}

// replayAll applies every record from the beginning, which is what a consumer group
// reset does and what NF-8 says must be harmless.
func (p *projections) replayAll() {
	p.t.Helper()

	records, _ := p.records(0)
	p.apply(records)
}

func (p *projections) apply(records []kafka.Record) {
	p.t.Helper()

	for _, record := range records {
		if err := p.projector.Apply(context.Background(), record); err != nil {
			p.t.Fatalf("apply %s: %v", record.Name, err)
		}
	}
}

func (p *projections) stateOf(conversationID, accountID string) domain.MemberState {
	p.t.Helper()

	state, err := p.state.Of(context.Background(),
		domain.ConversationID(conversationID), domain.AccountID(accountID))
	if err != nil {
		p.t.Fatalf("read member state: %v", err)
	}
	return state
}

// acknowledge posts a receipt, the way a client reports what it has seen.
func (n *node) acknowledge(token, conversationID string, deliveredThrough, readThrough int64) {
	n.t.Helper()

	body := map[string]int64{"delivered_through": deliveredThrough, "read_through": readThrough}
	if status := n.do(http.MethodPost, "/v1/conversations/"+conversationID+"/receipt", token, body, nil); status != http.StatusAccepted {
		n.t.Fatalf("acknowledge: status %d", status)
	}
}

type summaryBody struct {
	ID                     string `json:"id"`
	Head                   int64  `json:"head"`
	Unread                 int64  `json:"unread"`
	ReadThrough            int64  `json:"read_through"`
	OthersReadThrough      int64  `json:"others_read_through"`
	OthersDeliveredThrough int64  `json:"others_delivered_through"`
}

func (n *node) summaries(token string) []summaryBody {
	n.t.Helper()

	var body struct {
		Conversations []summaryBody `json:"conversations"`
	}
	if status := n.do(http.MethodGet, "/v1/conversations", token, nil, &body); status != http.StatusOK {
		n.t.Fatalf("list conversations: status %d", status)
	}
	return body.Conversations
}

func (n *node) summaryOf(token, conversationID string) summaryBody {
	n.t.Helper()

	for _, summary := range n.summaries(token) {
		if summary.ID == conversationID {
			return summary
		}
	}
	n.t.Fatalf("conversation %s not in the caller's list", conversationID)
	return summaryBody{}
}

func TestUnreadCountsFollowTheLog(t *testing.T) {
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)
	events := newProjections(t)
	events.catchUp()

	ana, bruno := newAccountID(), newAccountID()
	anaToken := tokens.issue(ana)
	conversation := node.startDirect(anaToken, bruno)

	node.send(anaToken, conversation.ID, "one")
	node.send(anaToken, conversation.ID, "two")
	node.send(anaToken, conversation.ID, "three")
	events.catchUp()

	if got := events.stateOf(conversation.ID, bruno).UnreadCount; got != 3 {
		t.Errorf("bruno's unread = %d, want 3", got)
	}
	// Sending is reading. An author whose own messages counted as unread would see a
	// badge on every conversation they had just spoken in.
	if got := events.stateOf(conversation.ID, ana).UnreadCount; got != 0 {
		t.Errorf("ana's unread = %d, want 0", got)
	}
	if got := events.stateOf(conversation.ID, ana).ReadSequence; got != 3 {
		t.Errorf("ana's read mark = %d, want 3", got)
	}
}

func TestReplayingEveryEventChangesNothing(t *testing.T) {
	// The idempotency test NF-8 asks for, made explicit rather than assumed. A
	// consumer group reset, a rebalance, or a relay republishing a row whose mark did
	// not commit all produce exactly this: the same events, again, in order.
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)
	events := newProjections(t)
	events.catchUp()

	ana, bruno := newAccountID(), newAccountID()
	anaToken := tokens.issue(ana)
	conversation := node.startDirect(anaToken, bruno)

	node.send(anaToken, conversation.ID, "one")
	node.send(anaToken, conversation.ID, "two")
	events.catchUp()

	before := events.stateOf(conversation.ID, bruno)
	if before.UnreadCount != 2 {
		t.Fatalf("unread = %d before replay, want 2", before.UnreadCount)
	}

	// Twice, because one replay could coincidentally land on the same answer while a
	// second reveals an accumulating error.
	events.replayAll()
	events.replayAll()

	after := events.stateOf(conversation.ID, bruno)
	if after.UnreadCount != before.UnreadCount {
		t.Errorf("unread = %d after replay, want %d — the badge was double counted",
			after.UnreadCount, before.UnreadCount)
	}
	if after.ReadSequence != before.ReadSequence || after.DeliveredSequence != before.DeliveredSequence {
		t.Errorf("marks moved on replay: %+v, want %+v", after, before)
	}
}

func TestReadingClearsTheBadgeForTheAccountNotTheDevice(t *testing.T) {
	// MS-11. The second token is a second device of the same account, which is the
	// only thing that makes this test different from the one above.
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)
	events := newProjections(t)
	events.catchUp()

	ana, bruno := newAccountID(), newAccountID()
	anaToken := tokens.issue(ana)
	brunoPhone, brunoLaptop := tokens.issue(bruno), tokens.issue(bruno)

	conversation := node.startDirect(anaToken, bruno)
	node.send(anaToken, conversation.ID, "one")
	node.send(anaToken, conversation.ID, "two")
	events.catchUp()

	if got := node.summaryOf(brunoLaptop, conversation.ID).Unread; got != 2 {
		t.Fatalf("unread on the laptop = %d, want 2", got)
	}

	// Read on the phone.
	node.acknowledge(brunoPhone, conversation.ID, 2, 2)
	events.catchUp()

	// Cleared on the laptop, because the cursor belongs to the membership.
	if got := node.summaryOf(brunoLaptop, conversation.ID).Unread; got != 0 {
		t.Errorf("unread on the laptop = %d after reading on the phone, want 0", got)
	}
	if got := node.summaryOf(brunoLaptop, conversation.ID).ReadThrough; got != 2 {
		t.Errorf("read mark on the laptop = %d, want 2", got)
	}
}

func TestTheCursorOnlyMovesForward(t *testing.T) {
	// MS-12. Not enforced by rejecting the request — the projection takes the greater
	// of the two, which is what also makes an out-of-order redelivery harmless.
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)
	events := newProjections(t)
	events.catchUp()

	ana, bruno := newAccountID(), newAccountID()
	anaToken, brunoToken := tokens.issue(ana), tokens.issue(bruno)

	conversation := node.startDirect(anaToken, bruno)
	for range 3 {
		node.send(anaToken, conversation.ID, "message")
	}
	events.catchUp()

	node.acknowledge(brunoToken, conversation.ID, 3, 3)
	events.catchUp()

	node.acknowledge(brunoToken, conversation.ID, 1, 1)
	events.catchUp()

	state := events.stateOf(conversation.ID, bruno)
	if state.ReadSequence != 3 {
		t.Errorf("read mark = %d after an out-of-order advance, want 3", state.ReadSequence)
	}
	if state.UnreadCount != 0 {
		t.Errorf("unread = %d, want 0 — a stale advance revived the badge", state.UnreadCount)
	}
}

func TestACursorBeyondTheEndIsRejected(t *testing.T) {
	// A client parking its cursor past the head would never be told about anything
	// again, so the aggregate refuses it. This is the part of the rule the projection
	// cannot enforce, which is why it lives in the model.
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken, brunoToken := tokens.issue(ana), tokens.issue(bruno)

	conversation := node.startDirect(anaToken, bruno)
	node.send(anaToken, conversation.ID, "only one")

	status := node.do(http.MethodPost, "/v1/conversations/"+conversation.ID+"/receipt", brunoToken,
		map[string]int64{"read_through": 99}, nil)
	if status != http.StatusUnprocessableEntity {
		t.Errorf("status = %d for a cursor past the head, want 422", status)
	}
}

func TestDeliveryStateProgressesFromSentToDeliveredToRead(t *testing.T) {
	// MS-13, from the sender's side. Derived from the recipient's two marks rather
	// than stored per entry per recipient, which would be the fan-out-on-write
	// ADR-0002 rejected.
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)
	events := newProjections(t)
	events.catchUp()

	ana, bruno := newAccountID(), newAccountID()
	anaToken, brunoToken := tokens.issue(ana), tokens.issue(bruno)

	conversation := node.startDirect(anaToken, bruno)
	first := node.send(anaToken, conversation.ID, "one")
	second := node.send(anaToken, conversation.ID, "two")
	events.catchUp()

	summary := node.summaryOf(anaToken, conversation.ID)
	if got := deliveryOf(first.Sequence, summary); got != domain.DeliverySent {
		t.Errorf("first entry = %s before any acknowledgement, want sent", got)
	}

	// Bruno's device has both but he has read only the first.
	node.acknowledge(brunoToken, conversation.ID, 2, 1)
	events.catchUp()

	summary = node.summaryOf(anaToken, conversation.ID)
	if got := deliveryOf(first.Sequence, summary); got != domain.DeliveryRead {
		t.Errorf("first entry = %s, want read", got)
	}
	if got := deliveryOf(second.Sequence, summary); got != domain.DeliveryDelivered {
		t.Errorf("second entry = %s, want delivered", got)
	}
}

// deliveryOf is what a client computes from a conversation summary.
func deliveryOf(sequence int64, summary summaryBody) domain.DeliveryState {
	return projection.DeliveryOf(
		domain.Sequence(sequence),
		domain.Sequence(summary.OthersReadThrough),
		domain.Sequence(summary.OthersDeliveredThrough),
	)
}

func TestAnEntryIsReadableBeforeItsProjectionCatchesUp(t *testing.T) {
	// NF-7 stated as a test. The window where an entry exists and its badge does not
	// is normal and permanent-by-design; what must not happen is the read path
	// failing or the conversation disappearing during it.
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken, brunoToken := tokens.issue(ana), tokens.issue(bruno)

	conversation := node.startDirect(anaToken, bruno)
	node.send(anaToken, conversation.ID, "readable immediately")

	// Nothing has been projected. The entry is readable anyway, because the log is the
	// source of truth and the projection is only a convenience.
	entries := node.entriesAfter(brunoToken, conversation.ID, 0)
	if len(entries) != 1 {
		t.Fatalf("fetched %d entries before projection, want 1", len(entries))
	}

	summary := node.summaryOf(brunoToken, conversation.ID)
	if summary.Unread != 0 {
		t.Errorf("unread = %d before projection, want 0", summary.Unread)
	}
	if summary.Head != 1 {
		t.Errorf("head = %d, want 1 — the head comes from the log, not the projection", summary.Head)
	}
}

func TestUnknownEventsAreIgnoredRatherThanFatal(t *testing.T) {
	// A consumer that failed on an event it did not recognise would stop its partition
	// the first time a newer producer published something new, turning a
	// forward-compatible change into an outage.
	events := newProjections(t)

	err := events.projector.Apply(context.Background(), kafka.Record{
		Topic: kafka.TopicMessagingEntries,
		Name:  "messaging.something_from_the_future",
		Value: []byte(`{"name":"messaging.something_from_the_future","occurred_at":"2026-07-25T00:00:00Z","data":{}}`),
	})
	if err != nil {
		t.Errorf("apply unknown event: %v, want nil", err)
	}
}

func TestProjectionReadsTheEventNameFromThePayloadWhenTheHeaderIsMissing(t *testing.T) {
	// Headers are lost by proxies and mirroring tools. The envelope carries the name
	// so a consumer degrades to reading it rather than silently skipping the record.
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)
	events := newProjections(t)
	events.catchUp()

	ana, bruno := newAccountID(), newAccountID()
	anaToken := tokens.issue(ana)
	conversation := node.startDirect(anaToken, bruno)
	node.send(anaToken, conversation.ID, "one")

	records, _ := events.records(events.applied)
	for index := range records {
		records[index].Name = ""
	}
	events.apply(records)

	if got := events.stateOf(conversation.ID, bruno).UnreadCount; got != 1 {
		t.Errorf("unread = %d with no event-name header, want 1", got)
	}
}

// nowish is a loose deadline for eventual consistency assertions.
const nowish = 2 * time.Second

func TestProjectionConvergesWithinTheRequiredWindow(t *testing.T) {
	// NF-7's two seconds, measured rather than asserted in prose. Kafka is not in this
	// path — the relay's poll interval and the projector's work are, and those are what
	// the budget is actually spent on.
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)
	events := newProjections(t)
	events.catchUp()

	ana, bruno := newAccountID(), newAccountID()
	anaToken := tokens.issue(ana)
	conversation := node.startDirect(anaToken, bruno)

	const messages = 50
	started := time.Now()
	for range messages {
		node.send(anaToken, conversation.ID, "under load")
	}
	events.catchUp()

	if elapsed := time.Since(started); elapsed > nowish {
		t.Errorf("projecting %d entries took %v, want under %v", messages, elapsed, nowish)
	}
	if got := events.stateOf(conversation.ID, bruno).UnreadCount; got != messages {
		t.Errorf("unread = %d, want %d", got, messages)
	}
}

func TestAMalformedRecordIsSkippedRatherThanBlockingThePartition(t *testing.T) {
	// Found by running the worker: one record with no identifiers stopped every
	// projection in the system at offset 0, permanently. At-least-once plus a
	// deterministic failure is an infinite retry, and everything behind it on that
	// partition waits forever.
	//
	// The distinction the projector now draws: a database that is down is transient
	// and must be retried; a record that can never be applied is skipped, loudly.
	events := newProjections(t)

	unprocessable := []kafka.Record{
		{
			// No conversation, no author — what a record shaped for something other
			// than a projection looks like.
			Name:  "messaging.entry_appended",
			Value: []byte(`{"name":"messaging.entry_appended","occurred_at":"2026-07-25T00:00:00Z","data":{"Sequence":1}}`),
		},
		{
			// Not a position in the log.
			Name:  "messaging.entry_appended",
			Value: []byte(`{"name":"messaging.entry_appended","occurred_at":"2026-07-25T00:00:00Z","data":{"ConversationID":"019f9bcc-0000-7000-8000-000000000001","AuthorID":"019f9bcc-0000-7000-8000-000000000002","Sequence":0}}`),
		},
		{
			// Undecodable payload.
			Name:  "messaging.cursor_advanced",
			Value: []byte(`{"name":"messaging.cursor_advanced","occurred_at":"2026-07-25T00:00:00Z","data":"not an object"}`),
		},
	}

	for index, record := range unprocessable {
		if err := events.projector.Apply(context.Background(), record); err != nil {
			t.Errorf("record %d returned %v; a record that can never be applied must be "+
				"skipped, or it blocks every projection behind it", index, err)
		}
	}
}

func TestProjectionRetriesWhenItsStoreFails(t *testing.T) {
	// The other half of the distinction above: a transient failure must be reported so
	// the offset is not committed and the record comes back. Skipping it would lose an
	// unread count for good.
	failing := &failingStateStore{}
	projector := projection.NewProjector(failing,
		kafka.NewDeadLetters(nil, "messaging-projections", slog.New(slog.DiscardHandler)),
		slog.New(slog.DiscardHandler))

	record := kafka.Record{
		Name: "messaging.entry_appended",
		Value: []byte(`{"name":"messaging.entry_appended","occurred_at":"2026-07-25T00:00:00Z",` +
			`"data":{"ConversationID":"019f9bcc-0000-7000-8000-000000000001",` +
			`"AuthorID":"019f9bcc-0000-7000-8000-000000000002","Sequence":1}}`),
	}

	if err := projector.Apply(context.Background(), record); err == nil {
		t.Error("apply returned nil when the store failed; the record would be committed and lost")
	}
}

// failingStateStore fails the way an unreachable database does.
type failingStateStore struct{}

var errStoreUnavailable = errors.New("database is unreachable")

func (failingStateStore) EnsureMember(context.Context, domain.ConversationID, domain.AccountID, time.Time) error {
	return errStoreUnavailable
}

func (failingStateStore) CountEntry(context.Context, domain.ConversationID, domain.Sequence, domain.AccountID, time.Time) error {
	return errStoreUnavailable
}

func (failingStateStore) TouchAuthor(context.Context, domain.ConversationID, domain.Sequence, domain.AccountID, time.Time) error {
	return errStoreUnavailable
}

func (failingStateStore) MarkRead(context.Context, domain.ConversationID, domain.AccountID, domain.Sequence, time.Time) error {
	return errStoreUnavailable
}

func (failingStateStore) MarkDelivered(context.Context, domain.ConversationID, domain.AccountID, domain.Sequence, time.Time) error {
	return errStoreUnavailable
}

func (failingStateStore) ForAccount(context.Context, domain.AccountID) ([]domain.MemberState, error) {
	return nil, errStoreUnavailable
}

func (failingStateStore) Of(context.Context, domain.ConversationID, domain.AccountID) (domain.MemberState, error) {
	return domain.MemberState{}, errStoreUnavailable
}

func (failingStateStore) Others(context.Context, domain.ConversationID, domain.AccountID) (domain.Sequence, domain.Sequence, error) {
	return 0, 0, errStoreUnavailable
}
