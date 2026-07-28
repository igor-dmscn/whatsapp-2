# Internals

How this system actually works: the object graph, what runs on which goroutine, where every
piece of state lives, and each significant flow traced through the source.

Source: `comms` @ `f0037bf`. Paths are relative to the repository root.

Companion docs: [architecture](./architecture.md) is the shape and the rules; [flows](./flows.md)
draws four of them; [client-sync](./client-sync.md) is the contract clients must implement; the
[ADRs](./adr/) record why. This document is the level below all of them — the code, in the order
it executes.

---

## 1. Layering

Three binaries, four contexts, one module path. The contexts are packages inside `api`; the split
into binaries is by resource profile, not by boundary ([ADR-0007](./adr/0007-modular-monolith-split-by-resource-profile.md)).

```
cmd/api                      I/O bound: thousands of idle sockets, low CPU
│  identity.New(db, redis, logger)                       identity/identity.go:45
│  messaging.New(db, redis, identityModule, …)            messaging/messaging.go:66
│  media.New(db, store, messagingModule, nil, …)          media/media.go
│  calling.New(db, messagingModule, messagingModule, …)   calling/calling.go:91
│  messagingModule.RegisterFrames(callingModule.Frames(), callFrames{…})  cmd/api/main.go:197
│
├─ go messagingModule.Run(ctx)      cmd/api/main.go:153 → hub.Run + renewPresence
└─ go callingModule.Run(ctx)        cmd/api/main.go:193 → remote offer stream, or returns

cmd/worker                   CPU bound, bursty, holds nothing
│  outbox.NewRelay(db, producer, logger).Run             platform/outbox/relay.go:60
│  messaging.NewProjections(…).Run                       group "messaging-projections"
│  messaging.NewPushNotifications(…).Run                 group "messaging-push"
│  media.NewProcessing(…).Run                            group of its own
└─ four goroutines, one WaitGroup                        cmd/worker/main.go:138-158

cmd/sfu                      CPU bound, latency critical, owns a UDP range
│  calling.NewMediaNode(…)  → nodes.Media over sfu.Server
└─ HTTP: POST/DELETE participant, GET offer stream       calling/internal/nodes/media.go:65
```

Inside a context the layering is ports-and-adapters, and the fence is the compiler, not a
convention ([ADR-0011](./adr/0011-nested-internal-fences.md)):

```
internal/messaging/
    messaging.go              package messaging     ← the only importable surface
    internal/
        api/       HTTP handler, WebSocket, Hub, Connection
            ↓
        app/       Service: use cases, clock, ids, transaction boundaries
            ↓
        domain/    Conversation, Membership, Entry, ports   ← imports only stdlib
            ↑
        postgres/  broadcast/  presence/  projection/  push/    implement domain ports
```

`internal/messaging/internal/...` is unreachable from `cmd/` and from every other context, which
is why each context owns its own composition root and why `cmd/api` does nothing but choose which
contexts exist and how they are joined. It also cannot name an `api.Session`, which is why
`messaging.go:244` has a pass-through adapter for two identical interfaces with different names —
Go compares method signatures by name.

Cross-context calls are ports declared in plain strings by the *asking* side:

| Port | Declared by | Satisfied by | Question |
|---|---|---|---|
| `Authenticator` | `messaging.go:35` | `identity.Module.Authenticate` | who is this token |
| `Conversations.MayAttach` | media's domain | `messaging.go:178` | may this account attach |
| `Conversations.MayView` | media's domain | `messaging.go:187` | may this account see it |
| `Conversations.MayJoin` | `calling.go:33` | `messaging.go:205` → `MayAttach` | may this account join a call |
| `Notifier.CallChanged` | `calling.go:39` | `messaging.go:210` | tell the conversation |
| `FrameHandler` | `messaging.go:226` | `calling.Module` | carry another protocol on my socket |

`MayJoin` delegating to `MayAttach` is one rule asked three ways: an attachment, a call and a
message are all "may this account publish into this conversation".

---

## 2. Where state lives

| State | Owner | Scope | Structure |
|---|---|---|---|
| Live sockets by account | `Hub.connections` (`api/hub.go:33`) | one api process | `map[AccountID]map[*Connection]struct{}` |
| Sockets by conversation | `Hub.followers` (`api/hub.go:38`) | one api process | `map[ConversationID]map[*Connection]struct{}`; length **is** the refcount |
| Redis subscriptions | `Hub.pubsub` (`api/hub.go:26`) | one api process | one `redis.PubSub`, one reader goroutine |
| What one socket may see | `Connection.visibility` (`api/connection.go:67`) | one socket | `map[ConversationID]Sequence` — first position allowed |
| Pending writes for a socket | `Connection.outbound` (`api/connection.go:56`) | one socket | `chan []byte`, cap 64, **non-blocking send** |
| Last acted-on typing claim | `Connection.lastTyping` (`api/connection.go:69`) | one socket | `time.Time`, 1s throttle |
| Delegated frame families | `Handler.frameHandlers` | one api process | `map[string]FrameHandler`, keyed on the part before the first `.` |
| Call signalling sockets | `calling/api/frames.go` `Handler.sessions` | one api process | `map[deviceID]Session`, rewritten on **every** frame (`:134`) |
| Calls being forwarded | `sfu.Server.calls` (`sfu/sfu.go:54`) | one media process | `map[callID]*call` → `map[participantID]*participant` |
| What a participant publishes | `participant.sources` (`sfu/sfu.go:196`) | one transport | `map[sourceKey]*source`; one entry per camera, not per layer |
| Layers of one source | `source.layers` (`sfu/layers.go:62`) | one source | `map[rid]*layer`; a non-simulcast publisher is one entry under `""` |
| Receivers of one source | `source.subscribers` + `source.snapshot` (`sfu/layers.go:64`) | one source | map for mutation, **copy-on-write slice** for the per-packet read |
| Rewriting offsets | `subscription` (`sfu/layers.go:261`) | one receiver × one source | seq/ts offsets, high-water marks, chosen RID |
| Offer stream subscribers | `nodes.fanout.subscribers` (`nodes/media.go:229`) | one media process | `map[int]chan offerMessage`, cap 8 each, **dropped when full** |
| Presence | Redis sorted set `presence:<account>` | whole deployment | member = device, score = last seen; 30s window |
| Typing | Redis sorted set `typing:<conversation>` | whole deployment | member = account, score = ms; 6s window |
| Rate-limit counters | Redis `ratelimit:<name>:<key>` | whole deployment | INCR + PEXPIRE in one Lua script |
| Notification suppression | Redis `notified:<conv>:<seq>:<account>` | whole deployment | `SET NX`, 1h |
| Everything durable | Postgres | system of record | `platform/database/migrations/`; one context owns each table |
| Unpublished events | `outbox` table | system of record | claimed `FOR UPDATE`, marked `published_at` |
| Client's log | SQLite (OPFS in the browser, a file for the CLI) | one client | mirrors the server's shape, plus `contiguous` |

Nothing in the api process survives a restart, and nothing needs to: every piece of it is either
re-derived from the client's next `resume` (§4, §6) or is a claim that expires on its own (§9).

The two indexes on `Hub` are the whole of the receive side. `connections` answers "who is this
account" — used by device revocation and the control channel; `followers` answers "who wants this
conversation" — used by every entry, and its per-conversation length doubles as the subscription
reference count, so ten devices in one conversation cost one Redis subscription (`hub.go:324`).

---

## 3. Concurrency model

Go, so goroutines rather than a thread pool. Nine kinds, and the discipline is: **the two
per-process readers must never block on one client.**

| # | Goroutine | Count | Runs |
|---|---|---|---|
| 1 | `net/http` handler | one per request | Everything HTTP. For `GET /v1/socket` it lives for the socket's whole life and runs `read` (`websocket.go:235`) |
| 2 | `Connection.Write` | one per socket | Drains `outbound` onto the wire, 10s deadline per frame (`connection.go:163`) |
| 3 | `Hub.Run` | **one per api process** | Reads the single Redis pubsub channel and fans out (`hub.go:54`) |
| 4 | `Module.renewPresence` | one per api process | Every 10s, renews every claim the node holds (`messaging.go:131`) |
| 5 | `Remote.Offers` | one per api process | Long-lived NDJSON stream from the media node (`nodes/remote.go:118`) |
| 6 | `sfu.forward` | **one per layer per publisher** | `remote.Read` → rewrite → write to each subscriber (`sfu.go:503`) |
| 7 | `sfu.relayFeedback` | one per subscription | `sender.Read` → PLI/FIR/REMB handling (`sfu.go:580`) |
| 8 | `sfu.selectLayers` | one per media process | 1s tick, reconsiders every subscription (`sfu.go:660`) |
| 9 | worker consumers + relay | four in `cmd/worker` | `PollFetches` loops and the outbox drain |

Consequences worth internalising:

- **`Hub.Run` is a single point of delivery for the whole node.** It therefore never blocks:
  targets are collected under `RLock`, the lock is released, the payload is marshalled **once**
  (`hub.go:175`), and each `Connection.Send` is a non-blocking channel send that closes the socket
  rather than waiting (`connection.go:146`). One unresponsive client cannot stop delivery for
  everybody else on the process.
- **Frames from one socket are handled in order**, because `read` is one goroutine per socket and
  it calls `handleFrame` synchronously (`websocket.go:245-274`). A slow frame — a call join that
  waits on ICE gathering — blocks that client's next frame and nobody else's. This is the opposite
  trade from a worker pool, and it is the right one here: a per-socket ordering guarantee for free.
- **`sfu.forward` runs on Pion's `OnTrack` callback and the read loop does not start until the
  callback returns.** Renegotiation is therefore fired on its own goroutine (`sfu.go:478`), and the
  comment explains what happens otherwise: with two participants it delays the first frame, with
  three it wedges, because each new publisher blocks behind the previous one's exchange.
- **Simulcast means three `forward` goroutines per camera**, all reading one `*source`. That is why
  `source` holds an `RWMutex` and a copy-on-write `snapshot` slice (`layers.go:59-70`): the
  per-packet read must not serialise three publishers against each other or allocate.
- **Nothing in the api process is CPU bound**, which is why `api` and `worker` are separate
  binaries and why deriving a thumbnail cannot delay an unread badge.
- **Cleanup after a socket runs on a detached context.** `r.Context()` is cancelled the instant an
  upgraded handler returns, so the socket's context is built with `context.WithoutCancel`
  (`websocket.go:141`), and the `SocketClosed` handlers get a further 10s budget of their own
  (`websocket.go:163-167`) because leaving a call is a database write.

---

## 4. Flow: opening a socket

```
GET /v1/socket  Upgrade: websocket
```

1. `Handler.Socket` (`api/websocket.go:124`). `websocket.Accept` with `OriginPatterns` from
   `ALLOWED_ORIGINS`; empty means same-origin only, because a permissive policy would let any page
   a user visits open an authenticated socket on their behalf (`cmd/api/main.go:41`). Read limit
   32 KiB — clients send small control frames here, since sending a message is an HTTP request.
2. Context detached from the request (`:141`), then `handshake`.
3. `handshake` (`:184`), under a 10s deadline:
   - first frame must be `{"type":"authenticate","token":…}`. **Credentials never in the URL**: a
     query string lands in proxy logs, browser history and referrers, and a browser cannot put an
     `Authorization` header on a WebSocket handshake.
   - `authenticator.Authenticate` → Identity resolves the access-token digest against stored state
     (`identity/internal/domain/session.go:116`). Resolution on every use is what makes revocation
     immediate rather than a 15-minute wait ([ADR-0009](./adr/0009-opaque-tokens-not-jwt.md)).
   - rate limit `connect:<account>`, 30 per minute (`api/http.go:122`). Applied *after*
     authentication because the limit is per account and an unauthenticated socket has no account;
     the handshake deadline is what bounds the other case. The refusal carries a retry-after in
     seconds, because a socket is the most expensive thing a client can ask for.
   - `readyFrame` written directly, bypassing the queue (`connection.go:181`).
4. `hub.Register` (`hub.go:282`): index under the account, subscribe to `control:<account>` plus
   `entries:<conversation>` for anything already in `Visibility()` — which at this point is empty,
   so in practice one `SUBSCRIBE` with one channel.
5. `service.Connected` marks presence *now* rather than at the first heartbeat up to ten seconds
   later (`websocket.go:155`).
6. `go connection.Write(ctx)`, then `read` on this goroutine.
7. The client sends `{"type":"resume","cursor":{…}}` → §6.

Every inbound frame gets a fresh correlation identifier and its own span
(`websocket.go:267-271`) — per frame, not per connection, because a connection lives for hours and
a span that long is a bar on a chart rather than a measurement.

Unknown frame types are ignored after being offered to the delegation table (`:305-316`), so a
newer client talking to an older server degrades instead of disconnecting.

### Closing

Any read error ends the connection (`websocket.go:245`): a closed socket, a client that went away,
a frame over the limit. None are recoverable and the client's remedy is identical in every case.
Then, in `defer` order: `closed()` → presence withdrawn and every `FrameHandler` told
(`websocket.go:351`), `connection.Close`, `hub.Unregister` → drop subscriptions nothing local
needs (`hub.go:335`).

`Connection.Close` is guarded by a `sync.Once` (`connection.go:205`) because four paths reach it:
client close, write failure, buffer overflow, device revocation. It always closes with
`StatusNormalClosure` — even for "too slow" and "device revoked" — because what the client should
do is reconnect and resync, which is exactly what a normal closure tells it to consider.

---

## 5. Flow: sending an entry

`POST /v1/conversations/{id}/entries` → `Service.Send` (`app/service.go:299`).

```
Send
├─ memberships.Of + MayWrite            authorisation before touching the payload (:314)
├─ ParseClientEntryID, PayloadFor       (:327-337)
├─ entries.ByClientEntryID              idempotency as one indexed read (:341)
└─ atomically:                          database/conn.go:75
   ├─ conversations.AppendEntry         postgres/postgres.go:111
   │  ├─ SELECT … FOR UPDATE            the whole serialisation mechanism (:123)
   │  ├─ conversation.Append            the aggregate assigns the position (domain/conversation.go:123)
   │  ├─ UPDATE conversations SET head
   │  └─ INSERT INTO entries            unique (conversation, sequence) and (conversation, author, client_id)
   └─ publish(events)                   → outbox rows, same transaction (:363)
   COMMIT                               ← the sender's acknowledgement waits for exactly this
└─ broadcaster.BroadcastEntry           allowed to fail (:381)
```

Five things this path is arranged to guarantee:

**Gaplessness is an aggregate invariant with a database backstop.** `Conversation` holds `head`
and is the only thing that assigns a position (`domain/conversation.go:113-168`). Concurrent
senders queue on `SELECT … FOR UPDATE` rather than racing, and
`entries_conversation_sequence_key` (`migrations/00003_messaging.sql`) would turn a bug in that
reasoning into a failed write rather than silent corruption. `ErrSequenceAlreadyTaken` is named
for exactly that case and is unreachable while the lock is held (`postgres.go:163`).

**Idempotency is a lookup, not a duplicate.** The pre-check at `:341` makes the common retry one
indexed read. The race — two identical sends in flight — loses on
`entries_client_entry_id_key`, and `ErrEntryAlreadySent` is turned back into the winning entry
(`:366-374`). Scoped to `(conversation, author, client_entry_id)` so one client's identifier
cannot collide with another's.

**The event cannot be missing.** `publish` writes outbox rows through `Conn`, which joins the
caller's transaction rather than opening a second (`database/conn.go:75`), and
`outbox.Writer.Write` **refuses to run outside a transaction** at all
(`platform/outbox/outbox.go:69` → `database.RequireTransaction`). A publish outside the
transaction compiles and runs and silently reintroduces the split-brain the outbox exists to
prevent, so it is made impossible rather than left to review.

**A publish failure fails the use case.** `service.publish` returns its error
(`app/service.go:584`), unlike the ephemeral broadcast below. An entry committed without its event
never reaches an unread badge, a receipt or a notification, and nothing afterwards would discover
the omission.

**The broadcast is allowed to fail.** `BroadcastEntry` at `:381` is logged and swallowed. The
entry is committed; a listener that misses it notices a sequence gap and refetches, which it must
do for offline sync anyway. Failing the send here would turn a delivery hiccup into lost work.

Rate limit: `send:<account>`, 60 per 10s (`api/http.go:116`), wrapped *inside* the authentication
middleware because the key is the caller (`httpx.go:83`).

### Revisions, retractions, reactions

Edits and deletes are new entries referencing earlier ones
([ADR-0008](./adr/0008-mutations-are-log-entries.md)) — `Conversation.Revise` and `.Retract`
(`domain/conversation.go:178`, `:202`). Clients sync strictly forward, so mutating entry 41 in
place would be invisible to anyone already past it. Revision is author-only; retraction is author
or admin, because taking something down and putting different words in its place are not the same
power. An amendment cannot amend an amendment (`:232`), so a client resolves one hop, never a
chain.

Reactions are **not** entries: they carry no sequence, live in their own table, and travel on the
conversation's channel as `ReactionMessage` with the sequence of the entry they are *about*
(`broadcast/redis.go:82`) — so the hub's visibility check applies unchanged.

---

## 6. Flow: resume and gap sync

The mechanism that makes at-most-once socket delivery acceptable. The contract is
[client-sync.md](./client-sync.md); this is the server half.

`handleResume` (`websocket.go:407`) → `Service.Resume` (`app/service.go:525`):

```
summaries.ForAccount(reader)             one query, postgres/summaries.go:34
for each conversation:
    GapBetween(id, visibleFrom, head, clientHas[id])     domain/entry.go:211
        from = max(clientHas+1, visibleFrom)
        from > head → no gap
Following = every conversation, gap or not
Gaps      = only those with something missing
```

Then, back in the handler:

- `hub.Listen` for **every** conversation in `Following` (`:434`), not only the ones with a gap: a
  client that is current still needs live delivery. `Listen` records the visibility floor on the
  connection and subscribes the node if this is the first local follower (`hub.go:303`).
- one `gapFrame` with `{conversation_id, from, to}` per gap.

Four properties fall out of one design:

- **A negative cursor is clamped, not rejected** (`websocket.go:419`). The server computes
  visibility itself, so a lying client gains nothing.
- **Absent means zero**, so a first-ever connection and a month-old reconnection send the same
  frame with no special case.
- **A new membership is a gap from its visibility start**, so "added to a conversation while
  offline" needs no separate mechanism.
- **One query, not two.** `ResumedSession` (`:511`) returns gaps and follow-set together because
  they are one question over one row set; answering them separately meant listing the account's
  conversations twice per socket and loading an aggregate per conversation in between — which a
  deploy turns into a reconnect storm's worth of queries.

`SummaryStore.ForAccount` is the query the schema was built for: memberships joined to
conversations and to the projection, plus a `LATERAL` that takes the **minimum** of the other
active members' marks for MS-13's delivery state. `COALESCE` goes *inside* the `min`, because `min`
skips NULLs and one member with no projected row yet would otherwise have one person's mark
reported as everyone's; and there is a second `COALESCE` outside, because an aggregate over no
rows is one NULL row — the case where the reader is the only member left.

Fetching is `GET …/entries?after=N`, capped at 500 (`app/service.go:210`), with visibility applied
from the caller's membership rather than trusted from the request (`:488`).

### The contiguous mark

Both clients hold, per conversation, the highest sequence **with nothing missing below it** —
`contiguous` in the browser's SQLite (`web/src/store.ts`), `Cursor()` in the CLI's
(`client/store.go`). Holding 1, 2 and 7, the mark is 2. Resuming from 7 would abandon 3–6
permanently, because sync runs strictly forward and nothing would ask again.

Two things move it, and `Syncer.fill` (`client/sync.go:200`) implements both:

1. contiguous arrival advances it, climbing through anything already held above;
2. **a fetch that skips positions closes over them** (`:220` → `store.CloseOver`). Asking for what
   follows 0 and being handed 40 first means 1–39 are not this account's to see. Without this a
   member who joined at 40 treats their pre-join history as a permanent gap and never converges.

A live entry above the mark is stored *and rendered*, then the hole below it is filled
(`client/sync.go:298-329`). Withholding it would make a dropped broadcast look like a message that
never arrived.

Sends are recorded as pending **before** being attempted (`client/sync.go:351`) — a send attempted
first is one whose client identifier is gone if the process dies, so the retry creates a second
entry instead of being recognised. `flushPending` (`:377`) retries with the original identifier,
and drops anything the server answers with a non-auth 4xx, because retrying that forever blocks
every later send behind it.

---

## 7. Flow: broadcast → client

```
Service.Send / React / Typing / Media / Calling
│
├─ RedisBroadcaster.Publish → "entries:<conversation>"   broadcast/redis.go:26
│    entry · reaction · typing · attachment · call        one channel, five shapes
│  or                    → "control:<account>"
│    conversation_started
│
└─ every api process, including the publisher:
     Hub.Run                 one goroutine, the only pubsub reader   hub.go:54
       dispatch              peek at {"type":…}, decode the right shape   hub.go:72
         deliverEntry        followers ∩ Sees(conversation, sequence)     hub.go:153
         deliverReaction     same visibility check, same reason           hub.go:190
         deliverToFollowers  every follower, no check                     hub.go:223
         handleControl       every socket of one account → Listen         hub.go:259
       json.Marshal once, then Connection.Send per target
         → outbound (cap 64) → Connection.Write → socket
```

**One publish per conversation, not per recipient.** A channel broadcast to fifty thousand readers
costs the same as a direct message; the cost moves to the subscriber, which listens only to the
conversations its connected accounts belong to (`broadcast/redis.go:17-21`).

**Five message shapes share `entries:`** and are distinguished by peeking at `type`
(`hub.go:79-140`). A second channel per shape would double every node's subscription count for
information the same connections already want.

**The visibility split is the interesting part.** Following a conversation is not the same as being
entitled to a position in it: a node subscribes because *some* local connection belongs to the
conversation, and `Connection.Sees` (`connection.go:133`) decides per socket. So:

| Shape | Visibility check | Why |
|---|---|---|
| `entry` | `Sees(conv, sequence)` | a member who joined at 40 must not receive 39 |
| `reaction` | `Sees(conv, sequence)` | a reaction on 39 tells you 39 exists |
| `typing` | none | a fact about a person now; no sequence to compare |
| `call` | none | a call belongs to the conversation, not to a position in its log |
| `attachment` | none | readiness has no position — see below |

The attachment case is a decision rather than an omission (`hub.go:245-256`). A connection outside
the attachment's visibility learns one opaque identifier, and if it acts on it the fetch is
refused, because Media asks Messaging whether the entry referencing that attachment is visible to
the asker (`messaging.go:187`). The authorisation lives on the read, where it can be exact.

Typing is deliberately *not* filtered for the person who typed: their own client ignores itself,
and filtering here would stop the frame also serving that account's second device, which does want
it (`hub.go:107-112`).

`conversation_started` is per account rather than per conversation, because "you have been added"
is addressed to somebody who is not yet listening to the conversation; the hub answers it by
calling `Listen` on every socket that account holds (`hub.go:276-278`).

### Backpressure

`Connection.Send` (`connection.go:146`) is `select` with a `default`. A full buffer closes the
connection with "too slow" rather than dropping the frame silently or blocking the hub. Dropping
would leave a client believing it is current when it is not; blocking would stall delivery for
every socket on the node. Closing makes the client reconnect and sync the gap, which is the
mechanism that already exists for exactly this.

That is the pattern everywhere a queue exists in this system:

| Bound | Value | Overflow behaviour |
|---|---|---|
| `outboundBuffer` (`connection.go:21`) | 64 frames | close the socket; client resyncs |
| `writeTimeout` (`connection.go:25`) | 10s | close the socket |
| `readLimit` (`websocket.go:30`) | 32 KiB | read error → close |
| `handshakeTimeout` (`websocket.go:26`) | 10s | close, nothing owed |
| `offerBuffer` (`nodes/media.go:279`) | 8 offers | drop, log, rely on the answer-grace retry |
| `batchSize` (`relay.go:26`) | 100 rows | next pass takes the rest immediately |
| `maxRangeLimit` (`app/service.go:210`) | 500 entries | client pages |
| `maxSDPBytes` (`nodes/media.go:207`) | 1 MiB | 400 |

---

## 8. Flow: outbox → Kafka → consumers

### The relay

`Relay.Drain` (`platform/outbox/relay.go:88`), one transaction per pass:

```sql
SELECT … FROM outbox WHERE published_at IS NULL ORDER BY id LIMIT 100 FOR UPDATE
UPDATE outbox SET attempts = attempts + 1 WHERE id = ANY($1)
-- publish, waiting for all in-sync replicas
UPDATE outbox SET published_at = now() WHERE id = ANY($1)
COMMIT
```

- **`FOR UPDATE`, not `SKIP LOCKED`** (`:92`). Skipping locked rows would let a second relay
  publish row 20 while the first holds 19, reordering two events for one conversation — which is
  exactly what keying by conversation exists to prevent. One relay at a time is the price of
  ordering, and it is marked in-source as a known ceiling.
- **`ProduceSync` with `AllISRAcks`** (`kafka.go:93`, `:154`). Marking rows published before the
  broker has the record would lose events on a broker failure while reporting success.
- **At-least-once, never at-most-once.** A crash between publish and mark republishes the row;
  franz-go's idempotent producer suppresses the duplicate append, but not the *replay* of a row
  whose mark never committed. Exactly-once is the same two-systems problem one layer up, so
  consumers are idempotent instead.
- **Attempts are counted before the publish** (`:144`) and roll back with it. The count therefore
  reflects completed passes rather than attempted ones — the honest trade, and it makes a
  permanently failing row visible as a row with many attempts rather than as an invisible loop.
- Straight back round on a non-empty pass (`:71`), 200ms idle delay otherwise. Polling rather than
  `LISTEN/NOTIFY`, because notification is at-most-once and to connected sessions only, so a poll
  is needed anyway for anything written while the relay was down — one mechanism beats two.

Topic and key are chosen by the context that raised the event, not by the transport
(`postgres/events.go:64`). Every messaging event is keyed by conversation, which is the only
ordering that means anything — sequence numbers are meaningless across conversations. Receipts get
their own topic so a burst of cursor advances from somebody scrolling a year of history cannot
delay the projection of new messages.

An unmapped event fails the write that produced it (`events.go:88`), rather than being published
on an empty key onto an arbitrary partition.

### The consumer loop

`Consumer.Run` (`platform/kafka/kafka.go:305`) is where at-least-once is actually implemented, and
the rewind is the whole mechanism:

```
PollFetches
EachPartition:                            per partition, because that is the unit with an order
  for each record:
    restore correlation id                logging.Correlate  (:351)
    continue the trace                    tracing.Extract    (:356)
    handle(record)
      error → record the rewind offset for this partition and STOP this partition (:376)
      ok    → append to `handled`
CommitRecords(handled…)                   only what was handled, by record (:393)
SetOffsets(rewind)                        after the commit, outside the poll (:413)
delay = min(max(2*delay, 100ms), 5s)      reset by any fully successful poll
```

Two mistakes this is written against, both documented in place:

- **Not committing is not enough.** The fetch position lives in the client's memory and has already
  moved past the failing record, so a failure without an explicit rewind means this consumer never
  sees it again — and the next successful poll commits over it. In the logs that reads as one
  handled failure and is in fact a lost event.
- **Committing what was *fetched*** acknowledges records nothing has looked at. `DisableAutoCommit`
  (`:273`) plus per-record commits is the fix.

`ConsumeResetOffset(AtStart)` (`:268`) means a projection added later builds from the whole
history. That is only safe because every write is idempotent, which is the next section.

### Unprocessable vs transient

Every consumer draws the same line, and it is the line that keeps a partition alive:

| Failure | Class | Handling |
|---|---|---|
| Postgres/object store unreachable | transient | return the error → rewind → retry with backoff |
| malformed payload, no identifiers, sequence < 1 | permanent | `errUnprocessable` → dead-letter → commit and move on |
| unknown event name | neither | ignored (`projection.go:120`) so a newer producer cannot cause an outage |
| attachment row gone | permanent | nothing to derive (`media/consumer.go:93`) |
| undecodable image | permanent | mark the attachment failed (`media/app/service.go:282`) |

An at-least-once pipeline plus a deterministic failure is an infinite retry, and one bad record
would otherwise stop every projection behind it on that partition indefinitely. The dead-letter
topic (`platform/kafka/deadletter.go:16`) exists because a log line scrolls away and what an
operator needs is the record itself, verbatim, with which consumer gave up on it. Publishing a
dead letter is best-effort by necessity — a consumer that stopped because it could not report a
skip would have converted a lost projection into a stalled partition.

### The projections

`Projector.Apply` (`projection/projection.go:98`) → four writes, all idempotent **by
construction** rather than by a dedup table (`postgres/projection.go`):

| Event | Write | What makes it idempotent |
|---|---|---|
| `entry_appended` | `TouchAuthor` then `CountEntry` | author's marks are `GREATEST`; the count is guarded by `projected_sequence <` in the same statement |
| `member_joined` | `EnsureMember` | `ON CONFLICT DO NOTHING` |
| `cursor_advanced` | `MarkRead` + `recount` | `GREATEST` — this *is* MS-12's forward-only rule; the count is recomputed from the log, not decremented |
| `entries_delivered` | `MarkDelivered` | `GREATEST` |

Three details that are not obvious from the shape:

- **Order within `entry_appended` matters.** `TouchAuthor` is unguarded and `CountEntry` is
  guarded, so the unguarded write goes first (`projection.go:168-174`). Reversed, a process dying
  between them would leave the author's own marks permanently behind, because the redelivery would
  hit the guard and skip.
- **`CountEntry` inserts as well as updates**, covering a member whose state row does not exist yet
  — rare, because entry and membership events share a partition, but "rare" is not a guarantee and
  a missing row is a permanently wrong badge.
- **`MarkRead` also advances the delivery mark** (`projection.go:145`). Reading implies delivery: a
  client whose delivery acknowledgement was lost would otherwise show as read-but-not-delivered.

MS-12 lives in the projection rather than the aggregate, and the reason is stated in
`domain/membership.go:170-182`: the aggregate does not hold the cursor, and reading the projection
to validate a write against it would be a race dressed up as a check. Taking the greater of the
two makes ordering irrelevant — so the forward-only rule and idempotent redelivery turn out to be
one mechanism, not two.

Delivery state is derived from two integers per membership (`projection.go:231`), never stored per
entry per recipient — that would be the fan-out-on-write
[ADR-0002](./adr/0002-conversation-log-with-projected-member-state.md) rejected, wearing a different hat.

---

## 9. Presence, typing, rate limits — everything that expires

All three live only in Redis, and that is the design rather than an optimisation
(`presence/redis.go:1-19`).

**Presence is a claim with an expiry, renewed by whoever holds the socket.** `renewPresence`
(`messaging.go:131`) ticks every 10s, asks the hub for every `DeviceClaim` it holds
(`hub.go:402`), and `RenewAll` writes them in pipelines of 1000 (`presence/redis.go:99`). A node
that crashes, is deployed over, or loses its network simply stops making the claim, and the people
it was holding go offline with nothing to clean up and nobody to run the cleanup. A set that was
added to on connect and removed on disconnect leaves a permanently online ghost for every process
ever killed — the failure every presence system has had at least once.

Sorted sets rather than keys with a TTL, because an account has several devices on several nodes:
member = device, score = last seen, so a dead node's device ages out of the 30s window while its
live sibling keeps the account online. A single key with one expiry could not do that — whichever
node refreshed last would keep the dead device alive.

Windows: `Online` 30s, `Heartbeat` 10s, `Typing` 6s (`presence/redis.go:34-48`). Three heartbeats
per window, so one lost refresh does not blink somebody offline.

A clean close calls `Gone` (`:123`) immediately. Not required for correctness — the score ages out
either way — and worth doing because thirty seconds of a dot beside somebody who closed their
laptop reads as a broken feature rather than as a window.

**Presence is asked, not pushed** (`websocket.go:94-104`). A client polls while it has a
conversation open, on a socket it already holds. Pushing it would mean every connect and
disconnect fanning out to every member of every conversation that account belongs to — the same
work, moved to the moment somebody opens their laptop, and paid for conversations nobody is
looking at. The answer is a whole snapshot, not a change (`:106`), so a client replaces what it
holds and cannot drift; it also repairs the typing set, whose pushes are allowed to be lost.

**Typing is throttled per connection, and only in one direction.** `claimsTyping`
(`connection.go:120`) drops a start claim within 1s of the last acted-on one, *before* the service
and therefore before the membership read; a **stop always gets through**, because it is the frame
that clears an indicator and dropping it leaves somebody typing on every other screen until the
claim expires (`websocket.go:469-478`).

**Rate limits are in Redis and fail open** (`ratelimit/ratelimit.go:1-12`). Counted per node, a
limit of thirty across four nodes is a limit of a hundred and twenty and loosens every time the
deployment grows. The INCR and the PEXPIRE are one Lua script (`:45`) because a process dying
between them leaves a counter with no expiry — a caller permanently at their limit, with nothing
to clear it. And it allows the action when Redis is unreachable, loudly: refusing every send in
the deployment because a cache is down is a worse outage than the one being prevented.

It is a fixed window, and the flaw is named in place (`:65-70`): a caller who bursts at the end of
one window and the start of the next gets twice the limit briefly. For what this is actually for —
somebody hammering a form, a client stuck in a reconnect loop — that is not a different outcome.

**Push suppression** is the same shape: `SET NX` with an hour's expiry (`presence/redis.go:221`),
atomic in one command so two nodes consuming the same redelivered record cannot both decide they
are first.

### The push decision

`push.Notifier.notify` (`push/push.go:157`) is three rules, and each is somebody's complaint if it
is missing: not the author, not somebody already looking, not twice. Presence unavailable means
notify anyway (`:189-197`) — a notification somebody did not need is a smaller failure than
silence about a message they did. The claim is taken *before* sending (`:208`), because a crash
between the two loses one notification while the other order sends one twice on every redelivery,
and a duplicated buzz is the failure people notice.

> **A discrepancy, found while tracing this.** `push.appended` (`push/push.go:149`) decodes
> `{"payload":{"conversation_id":…,"author_id":…,"sequence":…}}`, but the wire format written by
> `outbox.Encode` (`platform/outbox/outbox.go:120`) is
> `{"name":…,"occurred_at":…,"data":{"ConversationID":…,"AuthorID":…,"Sequence":…}}`. Every other
> consumer reads it through `outbox.Open[T]` (`projection.go:151`, `media/consumer.go:84`); this
> one does not. `json.Unmarshal` ignores the unknown keys, leaves the struct zeroed, and `:164`
> then classifies the record as `errUnprocessable` — so as the code stands, every
> `messaging.entry_appended` reaching the `messaging-push` group is dead-lettered and no
> notification is ever sent. There is no test over this package. Worth confirming against a
> running worker before treating §14's push row as accurate.

---

## 10. Flow: attachment upload → variants

The one flow where the server opens a payload
([ADR-0001](./adr/0001-content-opaque-server-e2ee-deferred.md)), arranged so it happens in exactly
one process — and **`api` never holds a byte in either direction**.

```
POST /v1/attachments            → RequestUpload   media/app/service.go:98
   MayAttach?  → row (pending)  → presigned PUT with content-type and content-length signed in
PUT bytes                        client → object store, directly
POST …/complete                 → CompleteUpload  media/app/service.go:147
   store.Size (trust nothing)   → MarkUploaded → attachment row + outbox row, one transaction
relay → media.attachments (keyed by attachment) → worker
   Process                      media/app/service.go:258
     derive: read original, two renditions (320 / 1280 max edge), write variants
     MarkReady → save
     notifier.AttachmentChanged → Redis "entries:<conversation>"     (not Kafka)
api hub → "look at this attachment again" → client re-fetches → signed variant URLs (1h)
```

- **The identifier is issued before the bytes exist**, which is the whole point of steps 1–3 being
  separate from the transfer. An entry can reference an attachment that is still uploading, so a
  message carrying a 90 MB video is readable immediately and displayable later (MD-1).
- **The size cap is enforced twice and read nowhere.** Refused at request time if over the limit;
  and `content-length` is *signed into the URL* (`:128-131`), so a client that lies is refused by
  the store on a request api never sees. `CompleteUpload` then checks the stored size against the
  declaration, which catches the one direction a signed URL cannot: a transfer that stopped early
  — and deletes the junk object then and there (`:176`), because that is the moment it is known to
  be junk.
- **Idempotency is two constraints, not two code paths.** The aggregate refuses to raise a second
  job for an upload it has already accepted; variants are keyed `(attachment, name)` so a second
  pass overwrites the first (`:338`). A key with anything unique in it would leave the store
  filling up with orphaned copies on every replay.
- **Readiness goes over Redis, not Kafka.** Its only purpose is to reach an open socket, Redis is
  already the socket fanout, and a client that misses it discovers the change the next time it
  renders — which it must fetch anyway for the URLs. A durable event with no durable consumer is a
  promise nothing needs.
- **The notification carries no state** (`broadcast/redis.go:92-105`). Variant URLs expire within
  the hour, so a frame containing them would be stale by the time a client acted on it; "look
  again" is the whole message.
- **Media holds no access rules.** Whether an account may attach and whether it may view are asked
  of Messaging (`media/app/service.go:107`, `:226`). An attachment is exactly as visible as the
  entry that references it, which keeps the join-point policy in the context that owns it. The
  owner may always see their own, including before any entry references it — which is what lets a
  client show what it is uploading (`:233`).
- Video gets no variants (`:318`): a poster frame needs a transcoder, which is a native dependency
  and a different scaling problem. It still passes through the same path so its lifecycle is
  identical to a photo's and clients have one shape of state to handle.
- Attachments get their **own consumer group**, because decoding a large photo occupies a consumer
  for as long as it takes and sharing a group with the projections would make every unread badge
  in the system wait behind it.

---

## 11. Flow: joining a call

Two planes. Signalling is domain traffic — a few kilobytes of SDP per participant — and rides the
socket the client already holds ([ADR-0004](./adr/0004-single-websocket-transport.md)).
Media is UDP straight to the forwarding process and never touches an HTTP handler.

```
client → {"type":"call.join", conversation_id, sdp}
  Messaging.read → delegate("call")        websocket.go:377   family = before the first dot
  calling/api/frames.go:131  HandleFrame
    remember(session)                      :134  every frame, so a reconnect re-registers
    app.Service.Join                       calling/app/service.go:70
      conversations.MayJoin                CL-1: the only authorisation in the flow
      enter                                :119  find the live call, or start one
        ActiveIn → join   |   Allocate + domain.Start → Save
        ErrCallInProgress → exactly one retry, then join the winner
        state committed with its outbox events
      nodes.Join(call.Node(), …)           local.go (in process) or remote.go:66 (HTTP)
        sfu.Server.Join                    sfu/sfu.go:228
      announce → CallChanged → Redis       allowed to fail
  session.Send(call.joined{answer, participants})
```

**There is no separate "start".** A call exists because somebody joined a conversation that had
none, so CL-2 — at most one call per conversation — is not a rule that has to be enforced: no code
path can create a second. Two people pressing call in the same instant race on the partial unique
index `calls_one_live_per_conversation` (`migrations/00009_calls.sql`), and the loser's remedy is
`enter`'s single retry (`:119-148`). Exactly one retry, not a loop: after a conflict there is
definitely a live call to find, and if the lookup then says there is not, the call ended in between
and that is `ErrCallEnded`.

**State is committed before the media exchange** (`:91` before `:96`). A node answering an offer
for a call nobody recorded would forward media for a call that does not exist, and the participant
would be invisible to everyone asking who is present. If the node then refuses, the participant is
left recorded without a transport (`:98-104`) — deliberately, because the client's remedy is to
retry the join, which is idempotent on the device, and a rollback would race with whatever the node
did manage to set up.

**Leaving goes the other way round** (`:230`): the node first, then the state. A participant whose
state says they left while their transport is still forwarding is the worse ordering — everyone
keeps receiving somebody the interface says has gone.

**A vanished client is noticed by the socket, not by a timeout.** `SocketClosed`
(`frames.go:227`) checks that the closing session is still the one on record before tearing
anything down — a client that reconnected has a newer session under the same device, and dropping
its call because the old socket finally closed would remove somebody who is present. Then
`LeaveAll` (`app/service.go:289`) leaves every live call for that device.

`call.active` (`frames.go:205`) is the durable half of a ring: the notification is ephemeral and
allowed to fail, so a client that missed it asks. No call is an answer rather than a failure,
because a client asks about every conversation it holds and most will not have one. Non-membership
is answered as *absence* (`app/service.go:269`), so asking about calls cannot be used to discover
which conversations exist — the same rule `Send` and `PresenceIn` follow.

### The offer coming back

This is the asymmetry that makes even a two-person call work, and it is the one thing in the system
that needs to reach a *particular* socket from a process holding none
([ADR-0013](./adr/0013-media-node-broadcasts-its-offers.md)).

The second person to join receives the first's tracks in the answer to their own offer. The first
knows nothing of the second's until something offers the other way. So when a new track arrives,
`forward` (`sfu.go:463-479`) adds it to everyone already in the call and fires
`renegotiateWith` — on its own goroutine, because this runs on Pion's `OnTrack` callback and the
read loop does not start until it returns.

Delivery depends on where forwarding lives:

- **In process** — `SetRenegotiator` calls the signalling handler directly
  (`calling.go:149`). `ErrNoSocket` means the client has gone.
- **Out of process** — the media node has no sockets, cannot know which api node holds which
  device, and so **broadcasts**: `nodes.Media` publishes to a fanout of NDJSON streams
  (`nodes/media.go:43-53`, `:130`), every api process subscribes with `Remote.Offers`
  (`nodes/remote.go:118`), and each drops the offers that are not its own — which is why
  `Module.Run` discards the error (`calling.go:171`). The alternative, a registry mapping devices
  to nodes, is a second source of truth about where a client is and one more thing to be stale.
  The cost is a few kilobytes per join per api node.

Both sides of that stream are written against silence. The node sends a bare newline every 20s
(`nodes/media.go:158-182`), because a subscriber that went away without closing is
indistinguishable from an idle one until something is written — and until then the node believes
its offers are being delivered. A newline is whitespace between JSON values and the decoder never
sees it (`nodes/remote.go:159`). The api side reconnects forever with a fixed 1s pause, because
the alternative is a node that is up, serving, holding sockets, and quietly unable to complete any
call that gains a third participant.

`publish` returning zero deliveries is reported as a failure (`nodes/media.go:45`) so the media
plane retries: `offerTo` makes up to five attempts a second apart (`sfu.go:813-830`), aimed at
exactly one window — a node with an offer to send and no api node subscribed, which exists at
startup and while a subscriber reconnects.

### One exchange at a time

The classic way an SFU wedges is two overlapping renegotiations, and the guard is four fields on
`participant` (`sfu.go:200-208`):

```
negotiating   an exchange is in flight
pending       something changed while it was
exchange      a monotonic number, so the abandon timer cannot abandon its successor
```

- A joiner is created with `negotiating: true` **before** going into the call's map
  (`sfu.go:248`), because another publisher's track arriving in that window would otherwise call
  `CreateOffer` on a connection sitting in `have-remote-offer`, which Pion rejects. The comment
  records where this was found: at 64 peers, where the window is hit often enough to see — at four
  it is invisible and the bug is exactly as present.
- `offerTo` sets `pending` and returns if an exchange is in flight (`:776-783`).
- `finishedExchange` (`:352`) clears the flag and starts the deferred exchange, and is shared by
  the two places an exchange ends — the answer to a join (`:340`) and a client's answer to a
  server offer (`:430`). They were the same nine lines, and they have to be the same, because a
  participant left with `negotiating` set never receives another track for the rest of the call.
- An offer that is never answered would wedge the participant forever, so `abandon` is scheduled
  at 15s (`:806`, `:844`) and checks the exchange number before clearing anything — an answer may
  already have cleared it and a later offer may own it now.

### Gathering, and the two timeouts that must not be equal

`Join` waits for ICE gathering to complete before answering (`sfu.go:330-334`), so signalling is a
single exchange; a browser may still trickle and nothing here depends on whether it does.

The wait is bounded at 5s (`sfu.go:408`) against the api node's 15s per-request deadline
(`nodes/remote.go:233`). The comment records what happens when those are set equal: a gather slow
enough to hit the cap raced the deadline, and the client was told no media node was available for
a node that was about to answer. The cap is also deliberately *not* NF-3's two seconds — gathering
binds a socket per address on the host, and a machine with a docker bridge per project has ten of
them, so a tight cap does not enforce a latency budget, it silently truncates the candidate list.

---

## 12. Inside the media plane

`sfu` knows about calls and participants as identifiers and nothing else: no database, no clock, no
authorisation (`sfu/sfu.go:1-12`). Selective forwarding, never mixing — the server copies RTP and
does not decode it, which is what makes a call cost CPU proportional to connections rather than to
pixels ([ADR-0006](./adr/0006-custom-pion-sfu-with-simulcast.md)).

The media engine registers **only** VP8 and Opus (`sfu.go:71-93`). Registering everything Pion
knows would let a client negotiate something this server has no keyframe detection for, and the
failure would be a black rectangle rather than an error. The two simulcast header extensions —
`SDES-MID` and `SDES-RTP-Stream-ID` — are registered by hand (`:104`), because a simulcast sender
puts the stream identifier in an RTP extension rather than the SDP: without them, three layers
arrive as three unlabelled streams with no way to say which is which.

### One source, several layers, one track per receiver

```
forward(callID, from, remote)                       once per layer — Pion calls OnTrack per layer
  key = participant + ":" + remote.ID()             so three layers are ONE source
  first one creates the source and renegotiates; the rest just add a layer
  loop: remote.Read → packet.Unmarshal
        layer.record(size, now)                     measured bitrate, 1s window
        keyframe = video && startsKeyframe(payload)
        for each subscription in source.snapshot:   copy-on-write slice, no alloc per packet
            subscription.write(rid, packet, keyframe)
```

A receiver subscribes to a **source**, not a layer (`layers.go:40-46`), so the number of tracks a
client sees does not depend on how many layers the publisher sends — and a client that knows
nothing about simulcast works unchanged.

`subscription.write` (`layers.go:313`) is where the server has to lie convincingly:

| Case | Behaviour |
|---|---|
| nothing chosen yet, video, not a keyframe | drop — a receiver handed the middle of a frame shows nothing anyway |
| nothing chosen yet, audio | take it. **Audio has no keyframes**, and gating it on one presents as total silence with perfect video, which is exactly how it presented |
| `rid == current` | forward |
| `rid == wanted && keyframe` | switch: `current = rid`, `rebase`, `switches++` |
| anything else | drop — the expensive two layers go no further, which is the entire point of simulcast |

`rebase` (`layers.go:384`) is the trick in four lines: the receiver has seen up to `highestSeq` at
`highestTS`, so this packet — whatever numbering its layer uses — becomes the next one. Each layer
numbers its own packets from its own start, so forwarding unchanged looks to the receiver like
catastrophic loss and reordering at every switch.

Three details around it, each written out because getting them wrong is subtle:

- `chosen` is a separate flag, not `current == ""`, because **the empty string is a real RID** — it
  is what a single-layer publisher uses. Conflating them meant a single-layer stream re-entered the
  nothing-chosen-yet branch on every packet, so only keyframes were forwarded: two packets in two
  seconds, and video that technically arrived (`layers.go:269-273`).
- the high-water mark only advances on packets that are genuinely newer, via `newerSequence`
  (`:427`), which is written out because `left > right` is wrong across the wrap at 65535 — and
  getting it wrong rebases a switch onto a sequence already sent, which a receiver reads as a
  stream that went backwards.
- `tsStep` is guessed at 3000 (one frame at 30fps on a 90kHz clock) and then *observed*, bounded to
  one second (`:377`) so a publisher that paused does not put a visible stall into the next switch.
- `rebase` keeps the publisher's own numbering when nothing has been sent yet (`:385`), which makes
  a single-layer stream byte-identical to what it was before simulcast existed.

### Choosing a layer

```
relayFeedback                       one goroutine per subscription        sfu.go:580
  PLI / FIR  → requestCurrentKeyframe   relayed, but only for the layer this receiver is on
  NACK       → NOT relayed              answered from this server's own send buffer
  REMB       → reported(bitrate) + reselect
selectLayers                        1s tick, every subscription           sfu.go:660
  reselect → source.choose(estimate) → subscription.want(rid) → requestKeyframe(that layer's SSRC)
```

`choose` (`layers.go:201`) is highest-that-fits at 90% of the estimate, ranked by **measured**
bitrate — what a publisher declares in its SDP is an intention, and selection wants to know what is
actually arriving. Unmeasured layers rank as unknown and are skipped, so for the first second the
layer already being forwarded stays; switching to something whose cost is unknown is how a
constrained receiver gets sent the biggest layer in the call. An estimate of zero — the normal state
of a client that sends no REMB — takes the best layer, because the alternative is degrading a call
nobody complained about.

The 1s tick is not redundant with the feedback path, and the first version got this wrong
(`sfu.go:649-657`): reacting only to feedback means a client that sends none stays forever on
whichever layer happened to deliver the first keyframe — a receiver on a fast link watching the
smallest encoding, with nothing anywhere reporting a problem.

**A NACK is not relayed** (`sfu.go:600-606`), and that is CL-6 working because the *server*
buffers: the sequence number means something different at the publisher, and with simulcast it
might not exist on that layer at all.

### Keyframes are asked for, never waited for

Three places request one, and the reason is the same each time: left alone, a switch or a join
waits for the publisher's next scheduled keyframe — up to two seconds of grey.

| Trigger | Site | Why |
|---|---|---|
| a track appears while somebody is watching | `sfu.go:489` | the watcher can decode from now |
| a participant's transport reaches `connected` | `sfu.go:288` → `requestKeyframesFor` | **not** when the tracks were added, a few hundred ms earlier and before the transport exists — a keyframe produced then is forwarded into a connection that cannot carry it and is simply lost. This is the change that took NF-3 from 1.57 s to 110 ms |
| a layer switch is wanted | `sfu.go:646` | a switch may only happen on a keyframe |

`requestKeyframesFor` asks on **every** layer (`:736`), because which one this participant will end
up receiving is not decided yet — selection needs a second of measurement, and the first keyframe
to arrive is what starts the stream.

### Releasing a transport

`release` (`sfu.go:900`) takes the participant off every source still being published *and* closes
the connection. Closing alone is not enough: the write fails with `io.ErrClosedPipe`, which
`forward` deliberately ignores because a receiver going away is not an error — so the forward loops
would keep writing to a transport that has gone. A failed or closed connection state calls
`release` too (`:273`), so a crashed client does not leave its tracks being forwarded forever, and
a rejoin from the same device replaces its transport rather than publishing twice (`:255`).

A `Join` that fails releases what it created (`:296-301`), or the client's retry — its only remedy
— would find a stale twin holding a `negotiating` flag nobody will clear.

---

## 13. Cross-cutting plumbing

**Transactions travel on the context.** `database.Conn` (`platform/database/conn.go:26`) uses the
transaction on the context if there is one and the pool otherwise, mirroring `*sql.DB`'s method
names so a repository holding one reads identically to a repository holding a pool. The key type is
unexported, so nothing outside the package can put a transaction on a context or take one off.
`InTransaction` **joins** rather than nesting (`:75`), because two transactions on separate
connections cannot see each other's uncommitted rows and a nested one would deadlock against its
own parent on the first locked row. The consequence — an inner failure rolls back the whole outer
transaction — is the conservative direction and correct for every caller here.

The alternative, threading an explicit `*sql.Tx` through every repository method, gives every read
a parameter it does not use and makes a query's signature depend on whether it happens to be inside
a transaction.

**Aggregates record events; the app layer drains them.** `recorder.TakeEvents`
(`domain/events.go:28`) is draining, so whoever takes them owns publishing them exactly once.
`AppendEntry` takes them *inside* its transaction (`postgres.go:178`), so the outbox rows commit
with the entry.

**One correlation identifier, everywhere.** `httpx.Correlate` (`httpx.go:122`) reuses the client's
`X-Correlation-ID` if supplied — so "this failed for me at 14:02" is findable without guessing —
echoes it back, and starts a span. The outbox reads it off the context when it writes a row
(`outbox.go:77`), the relay copies it onto the Kafka header (`relay.go:130`), and the consumer puts
it back on the handler's context before calling the handler (`kafka.go:351`) — restored there
rather than in each consumer, because a consumer that forgot would be silently uncorrelated. The
same value is a span attribute rather than the trace ID being used in its place
(`tracing/tracing.go:15-20`), because a client may supply it and it already appears on every log
line.

Tracing is off unless `OTEL_EXPORTER_OTLP_ENDPOINT` is set; spans are still created and go nowhere,
so a caller writes the same four lines either way.

`statusRecorder.Unwrap` (`httpx.go:198`) exists because without it, wrapping the logging middleware
around a WebSocket upgrade breaks the upgrade — `http.ResponseController` could not find `Hijacker`.

**Every socket write has a deadline; the server has no `WriteTimeout`** (`cmd/api/main.go:218`),
because an upgraded connection is one long write and a server-level deadline would cap its
lifetime.

---

## 14. Failure modes, as designed

| Failure | Behaviour |
|---|---|
| Bad origin / bad upgrade | `websocket.Accept` refuses; nothing is owed |
| First frame is not `authenticate` | error frame, then `StatusPolicyViolation` |
| Invalid or expired access token | `unauthenticated` error frame, close |
| Too many connections for one account | `rate_limited` with a retry-after, close |
| Malformed inbound frame | `malformed_frame` error frame, **connection survives** |
| Unknown frame type | ignored after delegation is offered — a newer client degrades |
| Delegated frame fails | `frame_failed` error frame; the socket lives, because a call that cannot be joined is not a reason to lose the messages on the same connection |
| Client cannot keep up (64 frames queued) | closed with "too slow"; recovery is reconnect + resume |
| Device revoked | `Hub.DisconnectDevice` closes it — authentication only runs at connect time, so without this a revoked device keeps receiving until its token expires |
| Redis down (broadcast) | send still commits and is acknowledged; recipients see the entry on their next resume. Presence and typing degrade to nothing; rate limits **fail open** |
| Redis down (subscribe) | logged warning; that node delivers nothing live until the subscription succeeds |
| Kafka down | outbox rows accumulate and drain on recovery. Delay, not data loss |
| Postgres unreachable from a consumer | handler error → partition rewound → retried with 100ms→5s backoff |
| Record that can never be applied | dead-lettered, offset committed, partition keeps moving. Something is now permanently a little wrong and the dead-letter topic is where it is recorded |
| Projection behind | reads return zeros rather than errors (`postgres/projection.go:251`); NF-7 requires clients to render that window correctly |
| Worker not running | messages send and arrive live; conversation lists, unread counts, receipts and thumbnails never update. Looks like data loss and is not |
| Two relays running | correct but serialised — `FOR UPDATE` is what preserves per-conversation order |
| Truncated upload | `ErrSizeMismatch`, object deleted, client retries with a fresh attachment |
| Undecodable image | attachment marked failed and clients told; the consumer is not wedged on one bad upload |
| Attachment processed twice | variants overwrite by `(attachment, name)`; the aggregate accepts being marked ready when it already is |
| Two people press call at once | one loses on `calls_one_live_per_conversation` and joins the winner's call |
| Media node refuses a join | participant recorded without a transport; the client's retry is idempotent on the device |
| Media node restarted | its calls are gone and their clients rejoin. Nothing about media is persisted |
| No api node subscribed to offers | up to five retries a second apart; then the participant will not see that joiner, logged at error. `offer_subscribers: 0` with `calls > 0` on `/health` is the one failure that looks like health |
| Offer never answered | abandoned after 15s so the next renegotiation can proceed |
| Client's tab crashes mid-call | socket close → `LeaveAll` → node releases the transport → last participant out ends the call (CL-3) |
| Transport fails without signalling | `OnConnectionStateChange` → `release`; a crashed client does not leave tracks being forwarded forever |
| Push provider slow or down | one recipient's failure is logged and the others still go; the record is not redelivered, which would notify everybody who already succeeded again |
| Push consumer, as the code stands | see the note in §9 — the wire shape does not match, so records are dead-lettered rather than delivered |
| api node killed | its presence claims expire within 30s; its clients reconnect elsewhere and resume from the mark |
| Client's OPFS snapshot is a second stale | the mark is in the snapshot, so sync asks for whatever follows it |

The pattern the table is trying to show: **there is one recovery path, and it is reconnect and
resume from the contiguous mark.** A dropped broadcast, a node restart, a buffer overflow, a month
offline and a brand-new device all take it. That is why gap detection is the delivery guarantee and
live delivery is only an optimisation — and why the same mechanism doing three jobs is cheaper than
three fallbacks that each work once.
