// The latency claims Messaging makes, measured rather than asserted.
//
// Two numbers, and both are about a person waiting: NF-1 is how long after somebody sends until
// the other person's screen could show it, and NF-2 is how long a client that has been away waits
// before it is current again. Everything else in this package establishes that the system is
// correct; these establish that it is quick enough to be worth being correct about.
//
// Loopback on a developer's machine, one process per node, one Postgres. So what they establish is
// that the *code* is not the reason a limit would be missed — a deployment adds a network between
// every arrow and has to be measured where it runs. Stated here rather than left to be assumed.
package messaging_test

import (
	"fmt"
	"testing"
	"time"

	"comms/internal/messaging/internal/domain"
	"comms/internal/platform/id"
	"comms/internal/platform/measure"
)

// TestSendToDeliveryMeetsNF1 measures a send until the recipient's socket has it.
//
// Across two nodes, which is the only honest way: ADR-0005 exists because the recipient's socket
// is almost never on the node that accepted the write, so a single-node measurement would leave
// out the Redis hop that every real delivery makes.
//
// Timed from just before the HTTP request until the frame arrives, which includes the write
// transaction, the outbox row, the publish, the subscribing node's dispatch and the socket write.
// A server-side measurement would leave out the last two, which are the ones a person experiences.
func TestSendToDeliveryMeetsNF1(t *testing.T) {
	tokens := newFakeAuthenticator()
	sending, receiving := newNode(t, tokens), newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken, brunoToken := tokens.issue(ana), tokens.issue(bruno)
	conversation := sending.startDirect(anaToken, bruno)

	listener := receiving.dial(t)
	listener.authenticate(brunoToken)
	listener.resume(nil)

	// One send first, not measured. It pays for the subscription being registered, the
	// connection pool warming and Postgres planning the statements — none of which a person
	// pays for on the hundredth message, and all of which would land in the first sample.
	sending.send(anaToken, conversation.ID, "warming up")
	listener.readOfType("entry")

	samples := make([]time.Duration, 0, nf1Samples)
	for index := range nf1Samples {
		started := time.Now()
		sending.send(anaToken, conversation.ID, fmt.Sprintf("message %d", index))
		listener.readOfType("entry")
		samples = append(samples, time.Since(started))
	}

	p50, p95 := measure.Percentile(samples, 50), measure.Percentile(samples, 95)
	t.Logf("send to delivery over %d messages: p50 %s, p95 %s, worst %s",
		len(samples), p50.Round(time.Millisecond), p95.Round(time.Millisecond),
		measure.Percentile(samples, 100).Round(time.Millisecond))

	if p95 > nf1Limit {
		t.Fatalf("NF-1: send to delivery p95 is %s, want under %s", p95, nf1Limit)
	}
}

const (
	// nf1Samples is how many messages to time. Enough for a p95 to mean something, few
	// enough that the measurement is seconds.
	nf1Samples = 50
	nf1Limit   = 300 * time.Millisecond
)

// TestGapSyncOfAThousandEntriesMeetsNF2 measures a client catching up after being away.
//
// A thousand entries is the number the requirement names, and it is more than one page: a fetch
// returns at most five hundred, so this measures the paging a real client does rather than one
// unbounded query. Timed to the point where the client holds all thousand, because that is when
// it is current — a first page that arrives quickly and a second that never does is not sync.
func TestGapSyncOfAThousandEntriesMeetsNF2(t *testing.T) {
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	ana, bruno := newAccountID(), newAccountID()
	anaToken, brunoToken := tokens.issue(ana), tokens.issue(bruno)
	conversation := node.startDirect(anaToken, bruno)

	// Written directly rather than through the HTTP send path, which is rate limited to sixty
	// in ten seconds — a thousand sends would take three minutes of waiting to measure
	// something that has nothing to do with sending.
	for index := range nf2Entries {
		if _, err := node.service.Send(t.Context(),
			domain.ConversationID(conversation.ID), domain.AccountID(ana),
			id.New(), "text/plain", []byte(fmt.Sprintf("entry %d", index+1)), 0, "",
		); err != nil {
			t.Fatalf("seed entry %d: %v", index+1, err)
		}
	}

	// A client that holds nothing, resuming. The gap it is told about is the whole log.
	catching := node.dial(t)
	catching.authenticate(brunoToken)

	started := time.Now()
	gaps := catching.resume(nil)
	if len(gaps) != 1 {
		t.Fatalf("got %d gaps, want 1", len(gaps))
	}
	if gaps[0]["to"] != float64(nf2Entries) {
		t.Fatalf("the gap ends at %v, want %d", gaps[0]["to"], nf2Entries)
	}

	// Paged through exactly as a client does, until it holds everything.
	held := int64(0)
	for held < nf2Entries {
		fetched := node.entriesAfter(brunoToken, conversation.ID, held)
		if len(fetched) == 0 {
			t.Fatalf("a fetch after %d returned nothing with %d still missing", held, nf2Entries-held)
		}
		held = fetched[len(fetched)-1].Sequence
	}
	elapsed := time.Since(started)

	t.Logf("gap sync of %d entries took %s, in pages of at most 500",
		nf2Entries, elapsed.Round(time.Millisecond))

	// One sample, deliberately. The requirement is a p95 and this is not one — a thousand
	// entries take long enough to seed that fifty repetitions would be minutes of setup to
	// sharpen a number that is an order of magnitude inside its limit. If it were close, this
	// would have to be repeated; it is not, and saying so is better than implying a
	// distribution from one measurement.
	if elapsed > nf2Limit {
		t.Fatalf("NF-2: gap sync of %d entries took %s, want under %s",
			nf2Entries, elapsed, nf2Limit)
	}
}

const (
	nf2Entries = 1000
	nf2Limit   = time.Second
)

// TestConversationListDoesNotSlowDownWithMoreConversations measures the shape of the query
// rather than its speed.
//
// The conversation list used to cost three round trips per conversation plus one: a lookup for
// the conversation, one for the caller's own marks, one for everybody else's. That is invisible
// on a test account with two conversations and grows for the rest of somebody's life with the
// product — the people for whom the screen matters most are the ones it is slowest for.
//
// So this compares a small account against a large one instead of asserting a limit. A ratio
// near one means the cost is in the query and not in the number of rows it returns; the number
// that must not appear is twelve.
func TestConversationListDoesNotSlowDownWithMoreConversations(t *testing.T) {
	tokens := newFakeAuthenticator()
	node := newNode(t, tokens)

	timeList := func(token string) time.Duration {
		samples := make([]time.Duration, 0, listSamples)
		for range listSamples {
			started := time.Now()
			node.summaries(token)
			samples = append(samples, time.Since(started))
		}
		return measure.Percentile(samples, 50)
	}

	few := tokens.issue(newAccountID())
	for range listFew {
		node.startGroup(few)
	}
	many := tokens.issue(newAccountID())
	for range listMany {
		node.startGroup(many)
	}

	// Warmed for both, so Postgres planning the statement lands in neither sample.
	timeList(few)
	timeList(many)

	small, large := timeList(few), timeList(many)
	ratio := float64(large) / float64(small)
	t.Logf("conversation list p50: %s at %d conversations, %s at %d — %.1fx for %.0fx the rows",
		small.Round(time.Microsecond), listFew, large.Round(time.Microsecond), listMany,
		ratio, float64(listMany)/float64(listFew))

	if ratio > listRatioLimit {
		t.Fatalf("listing %d conversations costs %.1fx listing %d, want under %.1fx: the per-conversation work is back",
			listMany, ratio, listFew, listRatioLimit)
	}
}

const (
	// listFew and listMany are the two account sizes compared. listMany is twelve times
	// listFew, so a per-conversation round trip shows up as roughly twelve times the cost.
	listFew  = 5
	listMany = 60
	// listSamples is enough for a median to survive one slow scheduling moment.
	listSamples = 15
	// listRatioLimit is set between the two implementations rather than tight against the
	// current one. Twelve times the rows measures at 2.4–3.3x here, because the lateral that
	// finds everybody else's marks is real per-row work; a round trip per conversation
	// measured near twelve. Anything under six is the first shape, not the second — and
	// leaving that much room is what keeps this from failing on a busy machine over nothing.
	listRatioLimit = 6.0
)
