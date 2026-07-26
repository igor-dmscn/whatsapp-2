package messaging_test

import (
	"encoding/base64"
	"net/http"
	"strconv"
	"testing"

	"comms/internal/messaging/internal/domain"
	"comms/internal/platform/id"
)

// --- helpers ---

func (n *node) revise(token, conversationID string, target int64, text string) (entryBody, int) {
	n.t.Helper()

	var entry entryBody
	status := n.do(http.MethodPost,
		"/v1/conversations/"+conversationID+"/entries/"+strconv.FormatInt(target, 10)+"/revision", token,
		map[string]string{
			"client_entry_id": id.New(),
			"content_type":    "text/plain; charset=utf-8",
			"body":            base64.StdEncoding.EncodeToString([]byte(text)),
		}, &entry)
	return entry, status
}

func (n *node) retract(token, conversationID string, target int64) (entryBody, int) {
	n.t.Helper()

	var entry entryBody
	status := n.do(http.MethodPost,
		"/v1/conversations/"+conversationID+"/entries/"+strconv.FormatInt(target, 10)+"/retraction", token,
		map[string]string{"client_entry_id": id.New()}, &entry)
	return entry, status
}

func (n *node) react(token, conversationID string, sequence int64, emoji string) int {
	n.t.Helper()
	return n.do(http.MethodPut,
		"/v1/conversations/"+conversationID+"/entries/"+strconv.FormatInt(sequence, 10)+"/reactions", token,
		map[string]string{"emoji": emoji}, nil)
}

func (n *node) unreact(token, conversationID string, sequence int64, emoji string) int {
	n.t.Helper()
	return n.do(http.MethodDelete,
		"/v1/conversations/"+conversationID+"/entries/"+strconv.FormatInt(sequence, 10)+"/reactions", token,
		map[string]string{"emoji": emoji}, nil)
}

type reactionBody struct {
	Sequence  int64  `json:"sequence"`
	AccountID string `json:"account_id"`
	Emoji     string `json:"emoji"`
}

func (n *node) reactions(token, conversationID string, from, to int64) []reactionBody {
	n.t.Helper()

	var body struct {
		Reactions []reactionBody `json:"reactions"`
	}
	path := "/v1/conversations/" + conversationID + "/reactions?from=" + strconv.FormatInt(from, 10) + "&to=" + strconv.FormatInt(to, 10)
	if status := n.do(http.MethodGet, path, token, nil, &body); status != http.StatusOK {
		n.t.Fatalf("list reactions: status %d", status)
	}
	return body.Reactions
}

// --- revisions ---

func TestAClientAlreadyPastAnEntryStillReceivesItsEdit(t *testing.T) {
	// The load-bearing test of the phase, and the whole reason ADR-0008 makes an edit
	// a new entry. Bruno has synced past position 1. If the edit modified position 1
	// in place, he would sync forward from 1 forever and never see it.
	tokens := newFakeAuthenticator()
	writer := newNode(t, tokens)
	reader := newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken, brunoToken := tokens.issue(ana), tokens.issue(bruno)
	conversation := writer.startDirect(anaToken, bruno)

	original := writer.send(anaToken, conversation.ID, "the original wording")

	// Bruno connects and syncs past it.
	client := reader.dial(t)
	client.authenticate(brunoToken)
	client.resume(map[string]int64{conversation.ID: original.Sequence})

	// Ana edits it afterwards.
	edit, status := writer.revise(anaToken, conversation.ID, original.Sequence, "the corrected wording")
	if status != http.StatusCreated {
		t.Fatalf("revise: status %d", status)
	}
	if edit.Sequence != original.Sequence+1 {
		t.Fatalf("the edit took position %d, want %d — an edit is a new entry",
			edit.Sequence, original.Sequence+1)
	}

	// It arrives as an ordinary live entry, because that is what it is.
	frame := client.readOfType("entry")
	if frame["kind"] != string(domain.KindRevision) {
		t.Errorf("kind = %v, want revision", frame["kind"])
	}
	// The target travels with the broadcast, so a live client applies the edit without
	// a round trip. Read defensively: a missing field here would otherwise panic and
	// hide what is actually a wire-format regression.
	target, ok := frame["target_sequence"].(float64)
	if !ok || int64(target) != original.Sequence {
		t.Errorf("target = %v, want %d — the broadcast must carry what the edit amends",
			frame["target_sequence"], original.Sequence)
	}

	// And a client that syncs forward from where it was gets the edit too, which is
	// the same guarantee from the other direction.
	fetched := reader.entriesAfter(brunoToken, conversation.ID, original.Sequence)
	if len(fetched) != 1 || fetched[0].text(t) != "the corrected wording" {
		t.Errorf("fetched %d entries after the original, want the edit", len(fetched))
	}
}

func TestAnOlderClientDegradesToTheOriginalRatherThanCrashing(t *testing.T) {
	// A client written before phase 5 receives an entry of a kind it has never heard
	// of. It must ignore it and go on showing the original — the entry it does
	// understand is still there, unmodified, which is what makes the degradation
	// possible at all.
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken, brunoToken := tokens.issue(ana), tokens.issue(bruno)
	conversation := node.startDirect(anaToken, bruno)

	original := node.send(anaToken, conversation.ID, "the original wording")
	if _, status := node.revise(anaToken, conversation.ID, original.Sequence, "the corrected wording"); status != http.StatusCreated {
		t.Fatalf("revise: status %d", status)
	}

	entries := node.entriesAfter(brunoToken, conversation.ID, 0)
	if len(entries) != 2 {
		t.Fatalf("fetched %d entries, want 2", len(entries))
	}

	// Position 1 is untouched. This is the assertion an old client's correctness rests
	// on: nothing in the log was modified, so ignoring what it does not understand
	// leaves it showing something true rather than something stale.
	if entries[0].text(t) != "the original wording" {
		t.Errorf("the original now reads %q — it was modified in place", entries[0].text(t))
	}
	if entries[0].Sequence != original.Sequence {
		t.Errorf("the original moved to position %d", entries[0].Sequence)
	}
}

func TestOnlyTheAuthorMayEdit(t *testing.T) {
	// MS-8. An edit by somebody else would put words in the author's mouth,
	// attributed to them.
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken, brunoToken := tokens.issue(ana), tokens.issue(bruno)
	conversation := node.startDirect(anaToken, bruno)

	entry := node.send(anaToken, conversation.ID, "ana's words")

	if _, status := node.revise(brunoToken, conversation.ID, entry.Sequence, "bruno's words"); status != http.StatusForbidden {
		t.Errorf("status = %d for somebody else editing, want 403", status)
	}
}

func TestAnAdministratorMayRetractButNotEdit(t *testing.T) {
	// MS-9, and the asymmetry is the point: moderation needs the power to take
	// something down, not the power to replace it with different words.
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken, brunoToken := tokens.issue(ana), tokens.issue(bruno)

	group := node.startGroup(anaToken)
	node.mustAddMember(anaToken, group.ID, bruno)
	entry := node.send(brunoToken, group.ID, "bruno's message")

	if _, status := node.revise(anaToken, group.ID, entry.Sequence, "rewritten by the admin"); status != http.StatusForbidden {
		t.Errorf("status = %d for an admin editing somebody else's entry, want 403", status)
	}
	if _, status := node.retract(anaToken, group.ID, entry.Sequence); status != http.StatusCreated {
		t.Errorf("status = %d for an admin retracting, want 201", status)
	}
}

func TestARetractionCarriesNoContentAndIsFinal(t *testing.T) {
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken := tokens.issue(ana)
	conversation := node.startDirect(anaToken, bruno)

	entry := node.send(anaToken, conversation.ID, "regretted immediately")
	retraction, status := node.retract(anaToken, conversation.ID, entry.Sequence)
	if status != http.StatusCreated {
		t.Fatalf("retract: status %d", status)
	}
	if retraction.text(t) != "" {
		t.Errorf("the retraction carries %q, want nothing", retraction.text(t))
	}

	// Terminal: editing something withdrawn would put content back on screen for any
	// client that applied the retraction and then the edit.
	if _, status := node.revise(anaToken, conversation.ID, entry.Sequence, "second thoughts"); status != http.StatusConflict {
		t.Errorf("status = %d editing a retracted entry, want 409", status)
	}
}

func TestASecondEditAmendsTheOriginalNotTheFirstEdit(t *testing.T) {
	// A chain would make a client resolve an arbitrary number of hops to render one
	// message. Amendments target the original, so applying the highest-sequenced one
	// is always correct.
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken := tokens.issue(ana)
	conversation := node.startDirect(anaToken, bruno)

	original := node.send(anaToken, conversation.ID, "first wording")
	first, _ := node.revise(anaToken, conversation.ID, original.Sequence, "second wording")

	if _, status := node.revise(anaToken, conversation.ID, first.Sequence, "third wording"); status != http.StatusUnprocessableEntity {
		t.Errorf("status = %d amending an amendment, want 422", status)
	}

	second, status := node.revise(anaToken, conversation.ID, original.Sequence, "third wording")
	if status != http.StatusCreated {
		t.Fatalf("second edit of the original: status %d", status)
	}
	if second.Sequence <= first.Sequence {
		t.Errorf("the second edit took position %d, not after %d", second.Sequence, first.Sequence)
	}
}

// --- replies ---

func TestAReplyIsAReferenceAndNothingMore(t *testing.T) {
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken, brunoToken := tokens.issue(ana), tokens.issue(bruno)
	conversation := node.startDirect(anaToken, bruno)

	question := node.send(anaToken, conversation.ID, "a question")

	var reply entryBody
	status := node.do(http.MethodPost, "/v1/conversations/"+conversation.ID+"/entries", brunoToken,
		map[string]any{
			"client_entry_id": id.New(),
			"content_type":    "text/plain; charset=utf-8",
			"body":            base64.StdEncoding.EncodeToString([]byte("an answer")),
			"reply_to":        question.Sequence,
		}, &reply)
	if status != http.StatusCreated {
		t.Fatalf("reply: status %d", status)
	}
	if reply.ReplyTo != question.Sequence {
		t.Errorf("reply_to = %d, want %d", reply.ReplyTo, question.Sequence)
	}
}

func TestAReplyToAPositionThatDoesNotExistIsRejected(t *testing.T) {
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken := tokens.issue(ana)
	conversation := node.startDirect(anaToken, bruno)

	status := node.do(http.MethodPost, "/v1/conversations/"+conversation.ID+"/entries", anaToken,
		map[string]any{
			"client_entry_id": id.New(),
			"content_type":    "text/plain; charset=utf-8",
			"body":            base64.StdEncoding.EncodeToString([]byte("replying to nothing")),
			"reply_to":        99,
		}, nil)
	if status != http.StatusUnprocessableEntity {
		t.Errorf("status = %d replying to a position that does not exist, want 422", status)
	}
}

// --- reactions ---

func TestReactingManyTimesTakesNoPositionsInTheLog(t *testing.T) {
	// MS-10, and the assertion the plan asks for: react a hundred times and the head
	// does not move. A position per tap would wake every connected client and cost
	// every one of them a gap to fill.
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken, brunoToken := tokens.issue(ana), tokens.issue(bruno)
	conversation := node.startDirect(anaToken, bruno)

	entry := node.send(anaToken, conversation.ID, "a popular message")

	before := node.summaryOf(anaToken, conversation.ID).Head

	// A hundred taps: alternating on and off from two accounts, which is what a
	// contested reaction looks like.
	emoji := []string{"👍", "🎉", "😀", "🔥"}
	for index := range 100 {
		token := anaToken
		if index%2 == 0 {
			token = brunoToken
		}
		symbol := emoji[index%len(emoji)]
		if status := node.react(token, conversation.ID, entry.Sequence, symbol); status != http.StatusNoContent {
			t.Fatalf("react: status %d", status)
		}
		if index%3 == 0 {
			if status := node.unreact(token, conversation.ID, entry.Sequence, symbol); status != http.StatusNoContent {
				t.Fatalf("unreact: status %d", status)
			}
		}
	}

	after := node.summaryOf(anaToken, conversation.ID).Head
	if after != before {
		t.Errorf("head moved from %d to %d — reactions are taking positions in the log", before, after)
	}
}

func TestReactingTwiceIsOneReaction(t *testing.T) {
	// Idempotent because several devices on one account make a duplicate tap ordinary
	// rather than exceptional.
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken := tokens.issue(ana)
	conversation := node.startDirect(anaToken, bruno)
	entry := node.send(anaToken, conversation.ID, "a message")

	node.react(anaToken, conversation.ID, entry.Sequence, "👍")
	node.react(anaToken, conversation.ID, entry.Sequence, "👍")

	reactions := node.reactions(anaToken, conversation.ID, 1, entry.Sequence)
	if len(reactions) != 1 {
		t.Errorf("stored %d reactions for two identical taps, want 1", len(reactions))
	}
}

func TestUnreactingSomethingAbsentIsNotAnError(t *testing.T) {
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken := tokens.issue(ana)
	conversation := node.startDirect(anaToken, bruno)
	entry := node.send(anaToken, conversation.ID, "a message")

	if status := node.unreact(anaToken, conversation.ID, entry.Sequence, "👍"); status != http.StatusNoContent {
		t.Errorf("status = %d removing a reaction that was never there, want 204", status)
	}
}

func TestAReactionArrivesLiveOnTheOtherNode(t *testing.T) {
	tokens := newFakeAuthenticator()
	writer := newNode(t, tokens)
	reader := newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken, brunoToken := tokens.issue(ana), tokens.issue(bruno)
	conversation := writer.startDirect(anaToken, bruno)
	entry := writer.send(anaToken, conversation.ID, "react to this")

	client := reader.dial(t)
	client.authenticate(brunoToken)
	client.resume(map[string]int64{conversation.ID: entry.Sequence})

	if status := writer.react(anaToken, conversation.ID, entry.Sequence, "🎉"); status != http.StatusNoContent {
		t.Fatalf("react: status %d", status)
	}

	frame := client.readOfType("reaction")
	if frame["emoji"] != "🎉" {
		t.Errorf("emoji = %v, want 🎉", frame["emoji"])
	}
	sequence, ok := frame["sequence"].(float64)
	if !ok || int64(sequence) != entry.Sequence {
		t.Errorf("sequence = %v, want %d", frame["sequence"], entry.Sequence)
	}
	if frame["removed"] != false {
		t.Errorf("removed = %v, want false", frame["removed"])
	}
}

func TestTextIsNotAReaction(t *testing.T) {
	// The field is rendered verbatim into every member's client. "Any short string"
	// would be a way to put arbitrary text on somebody else's message without it
	// counting as a message — no author, no position, no way to report it.
	for _, attempt := range []string{"lol", "a", "1", " ", "👍 nice", "<script>", "."} {
		if _, err := domain.ParseEmoji(attempt); err == nil {
			t.Errorf("ParseEmoji(%q) was accepted, want a rejection", attempt)
		}
	}

	// And the ones that must work, including sequences and modifiers.
	for _, attempt := range []string{"👍", "🎉", "❤️", "👍🏽", "👨‍👩‍👧"} {
		if _, err := domain.ParseEmoji(attempt); err != nil {
			t.Errorf("ParseEmoji(%q) = %v, want it accepted", attempt, err)
		}
	}
}

func TestReactionsAreNotVisibleBeforeAJoinPoint(t *testing.T) {
	// A reaction on position 39 tells you position 39 exists, and who was in the
	// conversation to react to it.
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken, brunoToken := tokens.issue(ana), tokens.issue(bruno)

	group := node.startGroup(anaToken)
	early := node.send(anaToken, group.ID, "before bruno")
	node.react(anaToken, group.ID, early.Sequence, "👍")

	node.mustAddMember(anaToken, group.ID, bruno)
	later := node.send(anaToken, group.ID, "after bruno")
	node.react(anaToken, group.ID, later.Sequence, "🎉")

	visible := node.reactions(brunoToken, group.ID, 1, later.Sequence)
	if len(visible) != 1 {
		t.Fatalf("bruno saw %d reactions, want 1", len(visible))
	}
	if visible[0].Sequence != later.Sequence {
		t.Errorf("bruno saw a reaction on position %d, below his join point", visible[0].Sequence)
	}
}

func TestAReaderMayReact(t *testing.T) {
	// Reading and reacting are the same entitlement. A channel subscriber who could
	// not react would be unable to do the one thing a broadcast audience does.
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken, brunoToken := tokens.issue(ana), tokens.issue(bruno)

	channel := node.startChannel(anaToken)
	entry := node.send(anaToken, channel.ID, "a broadcast")
	node.mustAddMember(anaToken, channel.ID, bruno)

	if status := node.react(brunoToken, channel.ID, entry.Sequence, "👍"); status != http.StatusNoContent {
		t.Errorf("status = %d for a reader reacting, want 204", status)
	}
}
