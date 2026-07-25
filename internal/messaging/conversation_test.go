package messaging_test

import (
	"errors"
	"slices"
	"testing"

	"comms/internal/messaging"
	"time"
)

func payload(t *testing.T, text string) messaging.Payload {
	t.Helper()

	built, err := messaging.NewPayload("text/plain", []byte(text))
	if err != nil {
		t.Fatal(err)
	}
	return built
}

func directWithMember(t *testing.T) (*messaging.Conversation, *messaging.Membership) {
	t.Helper()
	now := time.Now()

	conversation, err := messaging.StartDirect("conversation-1", now)
	if err != nil {
		t.Fatal(err)
	}
	membership, err := messaging.Join(conversation.ID(), "account-1", messaging.RoleMember, messaging.FirstSequence, now)
	if err != nil {
		t.Fatal(err)
	}
	conversation.TakeEvents()
	membership.TakeEvents()

	return conversation, membership
}

func eventNames(events []messaging.Event) []string {
	names := make([]string, 0, len(events))
	for _, event := range events {
		names = append(names, event.EventName())
	}
	return names
}

// --- MS-1: sequences ---

func TestAppendAssignsGaplessSequences(t *testing.T) {
	conversation, membership := directWithMember(t)
	now := time.Now()

	var positions []messaging.Sequence
	for index := range 5 {
		entry, err := conversation.Append(
			messaging.EntryID("entry-"+string(rune('a'+index))),
			membership,
			messaging.ClientEntryID("client-"+string(rune('a'+index))),
			payload(t, "hello"),
			now,
		)
		if err != nil {
			t.Fatal(err)
		}
		positions = append(positions, entry.Sequence())
	}

	// Gaplessness is the property the whole sync protocol rests on: a client that
	// holds up to n and hears the head is m knows exactly what it is missing.
	want := []messaging.Sequence{1, 2, 3, 4, 5}
	if !slices.Equal(positions, want) {
		t.Errorf("sequences = %v, want %v", positions, want)
	}
	if conversation.Head() != 5 {
		t.Errorf("head = %d, want 5", conversation.Head())
	}
}

func TestFirstEntryIsSequenceOne(t *testing.T) {
	conversation, membership := directWithMember(t)

	// Zero has to mean "nothing yet" so that a client's absent cursor and a fresh
	// conversation are the same case. That only works if entries start at one.
	if conversation.Head() != 0 {
		t.Errorf("new conversation head = %d, want 0", conversation.Head())
	}

	entry, err := conversation.Append("entry-1", membership, "client-1", payload(t, "hi"), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if entry.Sequence() != messaging.FirstSequence {
		t.Errorf("first sequence = %d, want %d", entry.Sequence(), messaging.FirstSequence)
	}
}

// --- authorisation ---

func TestAppendRejectsMembershipOfAnotherConversation(t *testing.T) {
	conversation, _ := directWithMember(t)

	stranger, err := messaging.Join("some-other-conversation", "account-9", messaging.RoleMember, messaging.FirstSequence, time.Now())
	if err != nil {
		t.Fatal(err)
	}

	// Without this check, a caller holding any membership at all could write
	// anywhere — the conversation must verify the membership is its own.
	_, err = conversation.Append("entry-1", stranger, "client-1", payload(t, "hi"), time.Now())
	if !errors.Is(err, messaging.ErrNotAMember) {
		t.Errorf("got %v, want ErrNotAMember", err)
	}
	if conversation.Head() != 0 {
		t.Error("rejected append advanced the head")
	}
}

func TestAppendRejectsNilMembership(t *testing.T) {
	conversation, _ := directWithMember(t)

	_, err := conversation.Append("entry-1", nil, "client-1", payload(t, "hi"), time.Now())
	if !errors.Is(err, messaging.ErrNotAMember) {
		t.Errorf("got %v, want ErrNotAMember", err)
	}
}

func TestAppendRejectsReaders(t *testing.T) {
	now := time.Now()
	channel, err := messaging.StartChannel("channel-1", now)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := messaging.Join(channel.ID(), "account-1", messaging.RoleReader, messaging.FirstSequence, now)
	if err != nil {
		t.Fatal(err)
	}

	// MS-7: a channel subscriber may read and nothing else.
	_, err = channel.Append("entry-1", reader, "client-1", payload(t, "hi"), now)
	if !errors.Is(err, messaging.ErrNotPermittedToWrite) {
		t.Errorf("got %v, want ErrNotPermittedToWrite", err)
	}
}

func TestAppendRejectsDepartedMembers(t *testing.T) {
	conversation, membership := directWithMember(t)
	membership.Leave(time.Now())

	_, err := conversation.Append("entry-1", membership, "client-1", payload(t, "hi"), time.Now())
	if !errors.Is(err, messaging.ErrNotPermittedToWrite) {
		t.Errorf("got %v, want ErrNotPermittedToWrite", err)
	}
}

func TestRejectedAppendRecordsNoEvent(t *testing.T) {
	conversation, membership := directWithMember(t)
	membership.Leave(time.Now())
	membership.TakeEvents()

	_, _ = conversation.Append("entry-1", membership, "client-1", payload(t, "hi"), time.Now())

	if names := eventNames(conversation.TakeEvents()); len(names) != 0 {
		t.Errorf("rejected append recorded %v, want nothing", names)
	}
}

func TestAppendRecordsEntryAppended(t *testing.T) {
	conversation, membership := directWithMember(t)

	if _, err := conversation.Append("entry-1", membership, "client-1", payload(t, "hi"), time.Now()); err != nil {
		t.Fatal(err)
	}

	events := conversation.TakeEvents()
	if names := eventNames(events); !slices.Contains(names, "messaging.entry_appended") {
		t.Fatalf("recorded %v, want messaging.entry_appended", names)
	}

	appended, ok := events[0].(messaging.EntryAppended)
	if !ok {
		t.Fatalf("event is %T, want EntryAppended", events[0])
	}
	if appended.Sequence != messaging.FirstSequence {
		t.Errorf("event sequence = %d, want %d", appended.Sequence, messaging.FirstSequence)
	}
	// ADR-0001 at the event boundary: an event travels beyond the aggregate, so it
	// carries the shape of the payload and never the payload.
	if appended.Size != 2 {
		t.Errorf("event size = %d, want 2", appended.Size)
	}
}

// --- MS-4, NF-13: membership limits ---

func TestDirectConversationRejectsAThirdMember(t *testing.T) {
	conversation, _ := directWithMember(t)

	if err := conversation.AuthoriseJoin(1); err != nil {
		t.Errorf("second member rejected: %v", err)
	}
	// MS-4. Allowing a third would silently turn a conversation two people
	// believed was private into a group.
	if err := conversation.AuthoriseJoin(messaging.DirectMemberCount); !errors.Is(err, messaging.ErrDirectConversationIsFull) {
		t.Errorf("got %v, want ErrDirectConversationIsFull", err)
	}
}

func TestGroupRejectsMembersPastTheLimit(t *testing.T) {
	group, err := messaging.StartGroup("group-1", time.Now())
	if err != nil {
		t.Fatal(err)
	}

	if err := group.AuthoriseJoin(messaging.GroupMemberLimit - 1); err != nil {
		t.Errorf("member below the limit rejected: %v", err)
	}
	if err := group.AuthoriseJoin(messaging.GroupMemberLimit); !errors.Is(err, messaging.ErrGroupIsFull) {
		t.Errorf("got %v, want ErrGroupIsFull", err)
	}
}

func TestChannelReadersAreUncapped(t *testing.T) {
	channel, err := messaging.StartChannel("channel-1", time.Now())
	if err != nil {
		t.Fatal(err)
	}

	// NF-10 designs for fifty thousand.
	if err := channel.AuthoriseJoin(50_000); err != nil {
		t.Errorf("channel capped readers at 50000: %v", err)
	}
}

// --- MS-5, MS-6: history policy ---

func TestJoiningPositionEncodesTheHistoryPolicy(t *testing.T) {
	now := time.Now()

	group, err := messaging.StartGroup("group-1", now)
	if err != nil {
		t.Fatal(err)
	}
	member, err := messaging.Join(group.ID(), "account-1", messaging.RoleMember, messaging.FirstSequence, now)
	if err != nil {
		t.Fatal(err)
	}
	for index := range 10 {
		if _, err := group.Append(messaging.EntryID("e"+string(rune('a'+index))), member, messaging.ClientEntryID("c"+string(rune('a'+index))), payload(t, "x"), now); err != nil {
			t.Fatal(err)
		}
	}

	// MS-5: a group member sees nothing said before they arrived.
	if got := group.JoiningPosition(); got != 11 {
		t.Errorf("group joining position = %d, want 11", got)
	}

	channel, err := messaging.StartChannel("channel-1", now)
	if err != nil {
		t.Fatal(err)
	}
	// MS-6: a broadcast with no back catalogue is useless to a new subscriber.
	if got := channel.JoiningPosition(); got != messaging.FirstSequence {
		t.Errorf("channel joining position = %d, want %d", got, messaging.FirstSequence)
	}
}

func TestDefaultRoleFollowsKind(t *testing.T) {
	now := time.Now()

	for _, testCase := range []struct {
		kind  string
		start func(messaging.ConversationID, time.Time) (*messaging.Conversation, error)
		want  messaging.Role
	}{
		{"direct", messaging.StartDirect, messaging.RoleMember},
		{"group", messaging.StartGroup, messaging.RoleMember},
		{"channel", messaging.StartChannel, messaging.RoleReader},
	} {
		t.Run(testCase.kind, func(t *testing.T) {
			conversation, err := testCase.start("conversation-1", now)
			if err != nil {
				t.Fatal(err)
			}
			if got := conversation.DefaultRole(); got != testCase.want {
				t.Errorf("default role = %q, want %q", got, testCase.want)
			}
		})
	}
}

// --- MS-3: gaps ---

func TestGapForRespectsVisibility(t *testing.T) {
	// A membership that joined at 41 must never be offered 1 to 40, however far
	// back its cursor claims to be.
	membership, err := messaging.Join("conversation-1", "account-1", messaging.RoleMember, 41, time.Now())
	if err != nil {
		t.Fatal(err)
	}

	gap, hasGap := messaging.GapFor(membership, 50, 0)
	if !hasGap {
		t.Fatal("no gap reported")
	}
	if gap.From != 41 || gap.To != 50 {
		t.Errorf("gap = %d..%d, want 41..50", gap.From, gap.To)
	}
	if gap.Count() != 10 {
		t.Errorf("count = %d, want 10", gap.Count())
	}
}

func TestGapForReportsNothingWhenCurrent(t *testing.T) {
	membership, err := messaging.Join("conversation-1", "account-1", messaging.RoleMember, messaging.FirstSequence, time.Now())
	if err != nil {
		t.Fatal(err)
	}

	if _, hasGap := messaging.GapFor(membership, 40, 40); hasGap {
		t.Error("a current client was told it has a gap")
	}
	// An empty conversation has head 0 and a client holding 0 — the case that
	// would produce a 1..0 gap if the bounds were compared carelessly.
	if _, hasGap := messaging.GapFor(membership, 0, 0); hasGap {
		t.Error("an empty conversation reported a gap")
	}
}

func TestGapForOneMissingEntry(t *testing.T) {
	membership, err := messaging.Join("conversation-1", "account-1", messaging.RoleMember, messaging.FirstSequence, time.Now())
	if err != nil {
		t.Fatal(err)
	}

	gap, hasGap := messaging.GapFor(membership, 41, 40)
	if !hasGap {
		t.Fatal("no gap reported")
	}
	if gap.From != 41 || gap.To != 41 || gap.Count() != 1 {
		t.Errorf("gap = %d..%d (%d), want 41..41 (1)", gap.From, gap.To, gap.Count())
	}
}

func TestMembershipCanSee(t *testing.T) {
	membership, err := messaging.Join("conversation-1", "account-1", messaging.RoleMember, 41, time.Now())
	if err != nil {
		t.Fatal(err)
	}

	if membership.CanSee(40) {
		t.Error("membership can see an entry before its visibility start")
	}
	if !membership.CanSee(41) {
		t.Error("membership cannot see the entry at its visibility start")
	}
}

// --- direct conversation identity ---

func TestDirectKeyIsOrderIndependent(t *testing.T) {
	// Without this, both accounts messaging each other simultaneously creates two
	// conversations and each sees half the history.
	if messaging.DirectKey("ana", "bruno") != messaging.DirectKey("bruno", "ana") {
		t.Error("direct key depends on argument order")
	}
	if messaging.DirectKey("ana", "bruno") == messaging.DirectKey("ana", "carla") {
		t.Error("different pairs share a direct key")
	}
}

// --- payload ---

func TestPayloadCopiesItsBytes(t *testing.T) {
	body := []byte("hello")
	built, err := messaging.NewPayload("text/plain", body)
	if err != nil {
		t.Fatal(err)
	}

	body[0] = 'j'
	if string(built.Body()) != "hello" {
		t.Error("altering the caller's slice changed the payload")
	}

	returned := built.Body()
	returned[0] = 'j'
	if string(built.Body()) != "hello" {
		t.Error("altering the returned slice changed the payload")
	}
}

func TestPayloadRejectsEmptyAndOversized(t *testing.T) {
	if _, err := messaging.NewPayload("text/plain", nil); err == nil {
		t.Error("empty body accepted")
	}
	if _, err := messaging.NewPayload("", []byte("x")); err == nil {
		t.Error("empty content type accepted")
	}
	if _, err := messaging.NewPayload("text/plain", make([]byte, 64*1024+1)); err == nil {
		t.Error("oversized body accepted")
	}
}

func TestPayloadDoesNotPrintItsBody(t *testing.T) {
	built := payload(t, "a secret message")

	// A log line containing message bodies would defeat the point of the type.
	if formatted := built.String(); formatted != "Payload(text/plain, 16 bytes)" {
		t.Errorf("String() = %q, want the shape and not the content", formatted)
	}
}

func TestClientEntryIDIsRequired(t *testing.T) {
	// MS-2 depends on it: without an identifier, a retry cannot be told from a
	// second message.
	if _, err := messaging.ParseClientEntryID(""); err == nil {
		t.Error("empty client entry id accepted")
	}
	if _, err := messaging.ParseClientEntryID(string(make([]byte, 65))); err == nil {
		t.Error("oversized client entry id accepted")
	}
}

// --- membership lifecycle ---

func TestLeaveIsIdempotentAndRecordsOnce(t *testing.T) {
	_, membership := directWithMember(t)

	first := time.Now()
	if changed := membership.Leave(first); !changed {
		t.Error("first Leave reported no change")
	}
	if changed := membership.Leave(first.Add(time.Hour)); changed {
		t.Error("second Leave reported a change")
	}
	if !membership.LeftAt().Equal(first) {
		t.Errorf("LeftAt = %v, want %v", membership.LeftAt(), first)
	}
	if names := eventNames(membership.TakeEvents()); len(names) != 1 {
		t.Errorf("recorded %v, want exactly one MemberLeft", names)
	}
}

func TestRoleCapabilities(t *testing.T) {
	now := time.Now()
	for _, testCase := range []struct {
		role            messaging.Role
		mayWrite        bool
		mayAdministrate bool
	}{
		{messaging.RoleAdmin, true, true},
		{messaging.RoleMember, true, false},
		{messaging.RoleReader, false, false},
	} {
		t.Run(string(testCase.role), func(t *testing.T) {
			membership, err := messaging.Join("conversation-1", "account-1", testCase.role, messaging.FirstSequence, now)
			if err != nil {
				t.Fatal(err)
			}
			if membership.MayWrite() != testCase.mayWrite {
				t.Errorf("MayWrite = %v, want %v", membership.MayWrite(), testCase.mayWrite)
			}
			if membership.MayAdminister() != testCase.mayAdministrate {
				t.Errorf("MayAdminister = %v, want %v", membership.MayAdminister(), testCase.mayAdministrate)
			}
		})
	}
}
