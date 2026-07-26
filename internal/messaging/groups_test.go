package messaging_test

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"net/http"
	"testing"
	"time"

	"comms/internal/messaging/internal/domain"
	"comms/internal/platform/database/testdb"
)

// --- helpers ---

func (n *node) startGroup(token string) conversationBody {
	n.t.Helper()

	var conversation conversationBody
	if status := n.do(http.MethodPost, "/v1/conversations/group", token, nil, &conversation); status != http.StatusCreated {
		n.t.Fatalf("start group: status %d", status)
	}
	return conversation
}

func (n *node) startChannel(token string) conversationBody {
	n.t.Helper()

	var conversation conversationBody
	if status := n.do(http.MethodPost, "/v1/conversations/channel", token, nil, &conversation); status != http.StatusCreated {
		n.t.Fatalf("start channel: status %d", status)
	}
	return conversation
}

type memberBody struct {
	AccountID   string `json:"account_id"`
	Role        string `json:"role"`
	VisibleFrom int64  `json:"visible_from"`
}

func (n *node) addMember(token, conversationID, accountID string) (memberBody, int) {
	n.t.Helper()

	var member memberBody
	status := n.do(http.MethodPost, "/v1/conversations/"+conversationID+"/members", token,
		map[string]string{"account_id": accountID}, &member)
	return member, status
}

func (n *node) mustAddMember(token, conversationID, accountID string) memberBody {
	n.t.Helper()

	member, status := n.addMember(token, conversationID, accountID)
	if status != http.StatusCreated {
		n.t.Fatalf("add member: status %d", status)
	}
	return member
}

func (n *node) members(token, conversationID string) []memberBody {
	n.t.Helper()

	var body struct {
		Members []memberBody `json:"members"`
	}
	if status := n.do(http.MethodGet, "/v1/conversations/"+conversationID+"/members", token, nil, &body); status != http.StatusOK {
		n.t.Fatalf("list members: status %d", status)
	}
	return body.Members
}

type inviteBody struct {
	ID      string `json:"id"`
	Role    string `json:"role"`
	MaxUses int    `json:"max_uses"`
	Uses    int    `json:"uses"`
	Revoked bool   `json:"revoked"`
	Token   string `json:"token"`
}

func (n *node) createInvite(token, conversationID string, maxUses int, expiresAt *time.Time) inviteBody {
	n.t.Helper()

	var invite inviteBody
	body := map[string]any{"max_uses": maxUses, "expires_at": expiresAt}
	if status := n.do(http.MethodPost, "/v1/conversations/"+conversationID+"/invites", token, body, &invite); status != http.StatusCreated {
		n.t.Fatalf("create invite: status %d", status)
	}
	return invite
}

func (n *node) redeemInvite(token, inviteToken string) (conversationBody, int) {
	n.t.Helper()

	var conversation conversationBody
	status := n.do(http.MethodPost, "/v1/invites/"+inviteToken+"/redeem", token, nil, &conversation)
	return conversation, status
}

// --- history on join ---

func TestAGroupMemberCannotReadAnythingBeforeTheyJoined(t *testing.T) {
	// The load-bearing test of the phase, and asserted at the API rather than in the
	// UI: history hidden by a client is history anyone can fetch with curl.
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken, brunoToken := tokens.issue(ana), tokens.issue(bruno)

	group := node.startGroup(anaToken)
	node.send(anaToken, group.ID, "said before bruno arrived")
	node.send(anaToken, group.ID, "also before")

	member := node.mustAddMember(anaToken, group.ID, bruno)
	if member.VisibleFrom != 3 {
		t.Fatalf("visible from %d, want 3 — a group member starts at the head (MS-5)", member.VisibleFrom)
	}

	node.send(anaToken, group.ID, "said after bruno arrived")

	// Bruno asks for everything from the beginning. The server, not the client,
	// decides what that means.
	entries := node.entriesAfter(brunoToken, group.ID, 0)
	if len(entries) != 1 {
		t.Fatalf("bruno fetched %d entries, want 1", len(entries))
	}
	if got := entries[0].text(t); got != "said after bruno arrived" {
		t.Errorf("bruno read %q — history before his join point leaked", got)
	}

	// And asking for a specific earlier position is refused just as firmly.
	before := node.entriesAfter(brunoToken, group.ID, 0)
	for _, entry := range before {
		if entry.Sequence < 3 {
			t.Errorf("bruno received sequence %d, below his join point", entry.Sequence)
		}
	}
}

func TestAChannelSubscriberReadsFromTheBeginning(t *testing.T) {
	// MS-6, the opposite policy from the same one number. A broadcast with no back
	// catalogue is useless to a new subscriber.
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken, brunoToken := tokens.issue(ana), tokens.issue(bruno)

	channel := node.startChannel(anaToken)
	node.send(anaToken, channel.ID, "first broadcast")
	node.send(anaToken, channel.ID, "second broadcast")

	member := node.mustAddMember(anaToken, channel.ID, bruno)
	if member.VisibleFrom != int64(domain.FirstSequence) {
		t.Fatalf("visible from %d, want %d", member.VisibleFrom, domain.FirstSequence)
	}
	if member.Role != string(domain.RoleReader) {
		t.Errorf("role = %s, want reader — channel joiners do not get to write", member.Role)
	}

	entries := node.entriesAfter(brunoToken, channel.ID, 0)
	if len(entries) != 2 {
		t.Fatalf("subscriber fetched %d entries, want 2", len(entries))
	}
}

func TestANonWriterCannotPostToAChannel(t *testing.T) {
	// MS-7.
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken, brunoToken := tokens.issue(ana), tokens.issue(bruno)

	channel := node.startChannel(anaToken)
	node.mustAddMember(anaToken, channel.ID, bruno)

	status := node.do(http.MethodPost, "/v1/conversations/"+channel.ID+"/entries", brunoToken,
		map[string]string{
			"client_entry_id": "attempt",
			"content_type":    "text/plain; charset=utf-8",
			"body":            base64.StdEncoding.EncodeToString([]byte("let me in")),
		}, nil)
	if status != http.StatusForbidden {
		t.Errorf("status = %d for a reader posting to a channel, want 403", status)
	}
}

func TestAChannelWriterCanPostOncePromoted(t *testing.T) {
	// The other half of MS-7: the role is what decides, and it is changeable.
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken, brunoToken := tokens.issue(ana), tokens.issue(bruno)

	channel := node.startChannel(anaToken)
	node.mustAddMember(anaToken, channel.ID, bruno)

	if status := node.do(http.MethodPut, "/v1/conversations/"+channel.ID+"/members/"+bruno+"/role",
		anaToken, map[string]string{"role": string(domain.RoleMember)}, nil); status != http.StatusNoContent {
		t.Fatalf("change role: status %d", status)
	}

	entry := node.send(brunoToken, channel.ID, "now permitted")
	if entry.Sequence != 1 {
		t.Errorf("sequence = %d, want 1", entry.Sequence)
	}
}

// --- limits ---

func TestAGroupRejectsMemberBeyondItsLimit(t *testing.T) {
	// NF-13. The cap is 256, and the aggregate holds it — this asserts the number is
	// enforced, not that the constant exists.
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana := newAccountID()
	anaToken := tokens.issue(ana)
	group := node.startGroup(anaToken)

	// The creator is the first member, so 255 more fill it.
	for index := 1; index < domain.GroupMemberLimit; index++ {
		node.mustAddMember(anaToken, group.ID, newAccountID())
	}

	if _, status := node.addMember(anaToken, group.ID, newAccountID()); status != http.StatusConflict {
		t.Errorf("status = %d for the %dth member, want 409", status, domain.GroupMemberLimit+1)
	}
}

func TestADirectConversationRejectsAThirdMember(t *testing.T) {
	// MS-4. Refused because membership is fixed, not because of a count: the pair is
	// the conversation's identity, and no role is senior enough to change that.
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana, bruno, carla := newAccountID(), newAccountID(), newAccountID()
	anaToken := tokens.issue(ana)

	conversation := node.startDirect(anaToken, bruno)

	_, status := node.addMember(anaToken, conversation.ID, carla)
	if status != http.StatusConflict {
		t.Errorf("status = %d adding a third member to a direct conversation, want 409", status)
	}
}

// --- who may change membership ---

func TestOnlyAnAdministratorMayChangeMembership(t *testing.T) {
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana, bruno, carla := newAccountID(), newAccountID(), newAccountID()
	anaToken, brunoToken := tokens.issue(ana), tokens.issue(bruno)

	group := node.startGroup(anaToken)
	node.mustAddMember(anaToken, group.ID, bruno)

	// Bruno is an ordinary member.
	if _, status := node.addMember(brunoToken, group.ID, carla); status != http.StatusForbidden {
		t.Errorf("status = %d for a member adding somebody, want 403", status)
	}

	// Promoted, he may.
	if status := node.do(http.MethodPut, "/v1/conversations/"+group.ID+"/members/"+bruno+"/role",
		anaToken, map[string]string{"role": string(domain.RoleAdmin)}, nil); status != http.StatusNoContent {
		t.Fatalf("promote: status %d", status)
	}
	node.mustAddMember(brunoToken, group.ID, carla)
}

func TestAnOutsiderCannotSeeOrChangeAGroup(t *testing.T) {
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana, bruno, carla := newAccountID(), newAccountID(), newAccountID()
	anaToken, carlaToken := tokens.issue(ana), tokens.issue(carla)

	group := node.startGroup(anaToken)
	node.mustAddMember(anaToken, group.ID, bruno)

	// Reported as absent, not forbidden: an account has no business learning which
	// conversations exist.
	if status := node.do(http.MethodGet, "/v1/conversations/"+group.ID+"/members", carlaToken, nil, nil); status != http.StatusNotFound {
		t.Errorf("status = %d for an outsider listing members, want 404", status)
	}
}

func TestRemovingAMemberStopsTheirAccess(t *testing.T) {
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken, brunoToken := tokens.issue(ana), tokens.issue(bruno)

	group := node.startGroup(anaToken)
	node.mustAddMember(anaToken, group.ID, bruno)
	node.send(anaToken, group.ID, "while bruno was here")

	if got := len(node.entriesAfter(brunoToken, group.ID, 0)); got != 1 {
		t.Fatalf("bruno fetched %d entries while a member, want 1", got)
	}

	if status := node.do(http.MethodDelete, "/v1/conversations/"+group.ID+"/members/"+bruno, anaToken, nil, nil); status != http.StatusNoContent {
		t.Fatalf("remove member: status %d", status)
	}

	if status := node.do(http.MethodGet, "/v1/conversations/"+group.ID+"/entries", brunoToken, nil, nil); status != http.StatusNotFound {
		t.Errorf("status = %d for a removed member reading, want 404", status)
	}

	// The membership row survives, marked as left: an entry's author must remain
	// resolvable after they go, or the history cannot be rendered.
	members := node.members(anaToken, group.ID)
	if len(members) != 2 {
		t.Errorf("listed %d members after a removal, want 2 — the row is kept, marked left", len(members))
	}
}

func TestAnAdministratorCannotRemoveThemselves(t *testing.T) {
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana := newAccountID()
	anaToken := tokens.issue(ana)
	group := node.startGroup(anaToken)

	if status := node.do(http.MethodDelete, "/v1/conversations/"+group.ID+"/members/"+ana, anaToken, nil, nil); status != http.StatusUnprocessableEntity {
		t.Errorf("status = %d removing yourself, want 422 — leaving is a different act", status)
	}

	// Leaving is available and is not the same call.
	if status := node.do(http.MethodDelete, "/v1/conversations/"+group.ID+"/membership", anaToken, nil, nil); status != http.StatusNoContent {
		t.Errorf("leave: status %d", status)
	}
}

// --- invites ---

func TestAnInviteLetsSomebodyNobodyAddedJoin(t *testing.T) {
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken, brunoToken := tokens.issue(ana), tokens.issue(bruno)

	group := node.startGroup(anaToken)
	node.send(anaToken, group.ID, "before the invite")
	invite := node.createInvite(anaToken, group.ID, 0, nil)

	if invite.Role != string(domain.RoleMember) {
		t.Errorf("invite role = %s, want member", invite.Role)
	}
	if len(invite.Token) < domain.InviteTokenMinLength {
		t.Errorf("token is %d characters, want at least %d", len(invite.Token), domain.InviteTokenMinLength)
	}

	joined, status := node.redeemInvite(brunoToken, invite.Token)
	if status != http.StatusOK {
		t.Fatalf("redeem: status %d", status)
	}
	if joined.ID != group.ID {
		t.Errorf("joined %s, want %s", joined.ID, group.ID)
	}

	// The history policy applies to invited members exactly as it does to added
	// ones — the way in does not change what is visible.
	if got := len(node.entriesAfter(brunoToken, group.ID, 0)); got != 0 {
		t.Errorf("invited member fetched %d entries of prior history, want 0", got)
	}
}

func TestRedeemingTwiceIsHarmlessAndCostsOneUse(t *testing.T) {
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken, brunoToken := tokens.issue(ana), tokens.issue(bruno)

	group := node.startGroup(anaToken)
	invite := node.createInvite(anaToken, group.ID, 0, nil)

	if _, status := node.redeemInvite(brunoToken, invite.Token); status != http.StatusOK {
		t.Fatalf("first redeem: status %d", status)
	}
	// Somebody clicking the link twice should land in the conversation, not be told
	// off — and it must not spend a second use.
	if _, status := node.redeemInvite(brunoToken, invite.Token); status != http.StatusOK {
		t.Errorf("second redeem: status %d, want 200", status)
	}

	var body struct {
		Invites []inviteBody `json:"invites"`
	}
	if status := node.do(http.MethodGet, "/v1/conversations/"+group.ID+"/invites", anaToken, nil, &body); status != http.StatusOK {
		t.Fatalf("list invites: status %d", status)
	}
	if len(body.Invites) != 1 || body.Invites[0].Uses != 1 {
		t.Errorf("uses = %v, want one use for one member joining", body.Invites)
	}
}

func TestASingleUseInviteIsSpent(t *testing.T) {
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana, bruno, carla := newAccountID(), newAccountID(), newAccountID()
	anaToken, brunoToken, carlaToken := tokens.issue(ana), tokens.issue(bruno), tokens.issue(carla)

	group := node.startGroup(anaToken)
	invite := node.createInvite(anaToken, group.ID, 1, nil)

	if _, status := node.redeemInvite(brunoToken, invite.Token); status != http.StatusOK {
		t.Fatalf("first redeem: status %d", status)
	}
	if _, status := node.redeemInvite(carlaToken, invite.Token); status != http.StatusGone {
		t.Errorf("status = %d for a second person using a single-use invite, want 410", status)
	}
}

func TestARevokedInviteStopsWorking(t *testing.T) {
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken, brunoToken := tokens.issue(ana), tokens.issue(bruno)

	group := node.startGroup(anaToken)
	invite := node.createInvite(anaToken, group.ID, 0, nil)

	if status := node.do(http.MethodDelete, "/v1/invites/"+invite.ID, anaToken, nil, nil); status != http.StatusNoContent {
		t.Fatalf("revoke: status %d", status)
	}

	if _, status := node.redeemInvite(brunoToken, invite.Token); status != http.StatusGone {
		t.Errorf("status = %d redeeming a revoked invite, want 410", status)
	}
}

func TestAnExpiredInviteStopsWorking(t *testing.T) {
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken, brunoToken := tokens.issue(ana), tokens.issue(bruno)

	group := node.startGroup(anaToken)

	// A second out, then waited past. Expiry is compared against the clock, so this
	// is the one place a short sleep is the honest test rather than a smell.
	soon := time.Now().Add(300 * time.Millisecond)
	invite := node.createInvite(anaToken, group.ID, 0, &soon)
	time.Sleep(400 * time.Millisecond)

	if _, status := node.redeemInvite(brunoToken, invite.Token); status != http.StatusGone {
		t.Errorf("status = %d redeeming an expired invite, want 410", status)
	}
}

func TestAnInviteMayNotGrantAdministration(t *testing.T) {
	// An admin link hands out the power to hand out power, to whoever forwards it
	// furthest. Refused in the aggregate, so no transport can offer it.
	_, err := domain.CreateInvite(
		"invite-1", "conversation-1", domain.InviteToken("0123456789abcdef0123456789abcdef"),
		"account-1", domain.RoleAdmin, 0, nil, time.Now(),
	)
	var validation domain.ValidationError
	if !errors.As(err, &validation) || validation.Field != "role" {
		t.Errorf("error = %v, want a validation error about the role", err)
	}
}

func TestOnlyAnAdministratorMayCreateAnInvite(t *testing.T) {
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken, brunoToken := tokens.issue(ana), tokens.issue(bruno)

	group := node.startGroup(anaToken)
	node.mustAddMember(anaToken, group.ID, bruno)

	if status := node.do(http.MethodPost, "/v1/conversations/"+group.ID+"/invites", brunoToken,
		map[string]any{"max_uses": 0}, nil); status != http.StatusForbidden {
		t.Errorf("status = %d for a member creating an invite, want 403", status)
	}
}

// --- NF-12 ---

func TestSendWriteCountDoesNotGrowWithMemberCount(t *testing.T) {
	// NF-12, measured rather than asserted. ADR-0002's whole claim is that a send
	// costs the same whether two people or fifty thousand are listening, and the way
	// that claim fails is somebody adding a per-recipient write in a later phase.
	//
	// Counted from Postgres's own statistics for the entries and outbox tables, which
	// is as close to "how many rows did that write" as it is possible to get without
	// instrumenting the driver.
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)
	db := testdb.Open(t)

	ana := newAccountID()
	anaToken := tokens.issue(ana)

	measure := func(memberCount int) int64 {
		group := node.startGroup(anaToken)
		for range memberCount - 1 {
			node.mustAddMember(anaToken, group.ID, newAccountID())
		}

		before := insertedRows(t, db)
		node.send(anaToken, group.ID, "one message")
		return insertedRows(t, db) - before
	}

	small := measure(2)
	large := measure(64)

	if small != large {
		t.Errorf("a send wrote %d rows with 2 members and %d rows with 64 — "+
			"write cost is scaling with membership, which is what ADR-0002 exists to prevent",
			small, large)
	}
	t.Logf("one send writes %d rows regardless of member count", small)

	// What this measures, precisely: the rows a send commits before answering — the
	// entry and its outbox row. It is not a claim that the system does no per-member
	// work at all. Unread counts are per member and the projection necessarily touches
	// one row each, which ADR-0002 accepted when it made them a read model.
	//
	// The distinction is the one that matters under load: that work is off the request
	// path, batched, and allowed to lag (NF-7), so a channel with fifty thousand
	// readers costs a sender the same as a direct message. What the assertion above
	// protects is exactly that — somebody adding a synchronous per-recipient write in
	// a later phase, which is the specific mistake ADR-0002 calls reintroducing
	// fan-out-on-write through the back door.
	if small != 2 {
		t.Errorf("a send committed %d rows, want 2 (the entry and its outbox row) — "+
			"if this changed deliberately, say what the new row is for", small)
	}
}

// insertedRows counts rows in the tables a send touches.
//
// Counted directly rather than read from pg_stat: the statistics collector updates
// asynchronously, so a test that reads it immediately after a write measures whenever
// the collector last got round to it. Two exact counts are slower and correct.
func insertedRows(t *testing.T, db *sql.DB) int64 {
	t.Helper()

	var entries, outbox, memberships, state int64
	err := db.QueryRowContext(context.Background(),
		`SELECT (SELECT count(*) FROM entries),
		        (SELECT count(*) FROM outbox),
		        (SELECT count(*) FROM memberships),
		        (SELECT count(*) FROM conversation_member_state)`,
	).Scan(&entries, &outbox, &memberships, &state)
	if err != nil {
		t.Fatalf("count rows: %v", err)
	}
	return entries + outbox + memberships + state
}
