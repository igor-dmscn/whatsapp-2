# Build Plan

Eleven phases. Each states a goal, what it delivers, and how it is verified — where "verified" means a test or a measurement someone else could re-run, not "it looked fine".

Two rules the ordering exists to serve:

1. **The frontend grows one phase at a time.** No phase builds UI it does not need. There is never a "now do the frontend" phase, because that is how a frontend gets built against an imagined backend.
2. **Risk fails early.** The sync protocol lands in phase 2 rather than phase 8, and the SFU harness is written before the SFU it tests. The two things most likely to be wrong are the sync protocol and simulcast; neither should be discovered late.

Requirement IDs (`MS-3`, `NF-7`, …) refer to [requirements.md](./requirements.md).

---

## Phase 0 — Scaffold — **complete**

**Goal:** the repository builds, lints, tests and runs locally, with nothing in it yet.

**Deliverables**
- `go.mod`; `cmd/api`, `cmd/worker`, `cmd/sfu` as binaries that start, log, and shut down cleanly on a signal.
- `docker-compose.yml`: Postgres, Redis, Kafka, MinIO. One command, no cloud (NF-15).
- Migration tooling, with an empty first migration.
- Import-boundary linting: the `domain` package of each context may import nothing but the standard library, and no context may import another's internals (NF-17).
- `Makefile`: `up`, `down`, `migrate`, `lint`, `test`.
- Structured logging with correlation identifiers (NF-16).

**Verify:** `make up && make migrate && make lint && make test` is green from a clean clone. A deliberate cross-context import fails `make lint`. Killing a binary with SIGTERM produces a clean shutdown log, not a stack trace.

**No frontend this phase.**

---

## Phase 1 — Identity — **complete**

**Goal:** accounts exist, prove themselves, and connect devices.

**Deliverables**
- Domain: Account, Handle, Credential, Device with their invariants.
- Registration, login, token issue and refresh, device registration and revocation.
- Argon2id password credentials, stored so that adding a passkey kind later is an insert (ID-2).
- Exact-handle lookup, rate limited (ID-5).
- Aggregates record domain events; an interim publisher writes them to the log. Phase 3 swaps that implementation for the transactional outbox without changing the port.

**Verify:** integration tests for register → login → refresh → revoke. Revocation invalidates tokens within 30 seconds (ID-4). A second account cannot claim a taken handle. Credential storage is exercised by a test that adds a second credential kind to an existing account without touching the first.

**No frontend this phase** — tests are the client.

---

## Phase 2 — The log, the socket, and sync — **complete**

**Goal:** two people exchange messages in real time, and nothing is lost when the connection drops. This is the heart of the system.

Scope is deliberately narrowed to **direct conversations only**. Groups and channels are phase 4; putting them here would mean debugging membership rules and the sync protocol at the same time.

**Deliverables**
- Domain: Conversation, Membership, Entry, sequence assignment (MS-1).
- Idempotent send on client-supplied identifier (MS-2).
- WebSocket transport: authenticated connect, resume with per-conversation high-water marks, gap response (MS-3).
- Gap fetch endpoint.
- Redis Pub/Sub fan-out, with each node subscribing only to accounts it holds.

**Frontend increment:** login screen, one conversation, send and receive. In-memory state only — persistence is phase 6. Ugly is fine; wired correctly is not optional.

**Verify:** two browser sessions exchange messages live. Kill the socket mid-conversation, send from the other side, reconnect — the gap fills and no entry is missing or duplicated. Send the same client identifier twice and assert one entry with one sequence number. Run two `api` nodes and confirm delivery across them.

**Verified by** `make e2e` — two real browsers on two api nodes, all four claims asserted rather than observed. The client's own rules are covered separately and hermetically in `web/src/sync.test.ts`, and the contract both clients implement is written down in [client-sync.md](./client-sync.md).

Two bugs came out of it, neither visible to the Go suite as it stood:

- The server told only the *recipient* of a new conversation to start listening, so whoever started one could not see replies until they reconnected. Fixed, and now covered by `TestTheInitiatorOfAConversationAlsoReceivesLive`.
- A callback with an unstable identity rebuilt the client's socket on ordinary re-renders, dropping the connection and every entry held in it — indistinguishable, from the outside, from the server losing messages.

That is what this phase's frontend increment was for. Neither bug was reachable by writing more backend tests.

---

## Phase 3 — Outbox, Kafka, projections — **complete**

**Goal:** unread counts and receipts exist, and are correct despite being eventually consistent.

**Deliverables**
- Outbox table written in the send transaction; a relay that publishes and marks sent.
- Identity's interim logging event publisher replaced by the outbox publisher, so `identity.device_revoked` reaches Messaging over Kafka.
- `messaging.entries` and `messaging.receipts` topics, keyed by conversation.
- Projections: cursor, unread count, delivery state. Idempotent consumers (NF-8).
- Cursor advance, forward-only (MS-11, MS-12). Three-state delivery (MS-13).

**Frontend increment:** conversation list with unread badges, and delivery ticks on sent messages. This is the first UI that must tolerate eventual consistency — an entry can be on screen before its projection lands (NF-7).

**Verify:** unread counts converge within 2 seconds under load. Replay the same Kafka messages and assert badges and receipts are unchanged — this is the idempotency test and it must be explicit. Stop the relay, send ten entries, restart it: all ten events publish, none twice. Read on one device clears the badge on another.

**Verified.** All four, plus the transport: measured convergence was 250 ms end to end through real Kafka against the 2-second budget. Replaying every event twice leaves every badge unchanged, and removing the `projected_sequence` guard takes an unread count from 2 to 6 — so the assertion is load-bearing rather than decorative. `make e2e` covers the badges and ticks in two real browsers.

Two findings, both from running it rather than reasoning about it:

- **One undecodable record wedged every projection in the system, permanently.** At-least-once plus a deterministic failure is an infinite retry, and everything behind it on the partition waits forever. The projector now distinguishes a transient failure, which must be retried, from a record that can never be applied, which is logged and skipped. The bug arrived via a test publishing transport-shaped records onto a production topic — so the tests gained their own topics too.
- **MS-12's forward-only cursor belongs in the projection, not the aggregate.** The aggregate does not hold the cursor and cannot enforce order over it; reading the projection to validate against it would be a race dressed as a check. `GREATEST` makes a stale, duplicated or out-of-order advance all reach the same state — so the forward-only rule and idempotent redelivery are one mechanism rather than two.

---

## Phase 4 — Groups and channels — **complete**

**Goal:** the other two conversation kinds, with their membership rules.

**Deliverables**
- Membership mutation, admin role, invite links.
- Join-point policy: groups begin at head, channels at sequence 1 (MS-5, MS-6).
- Channel writer role enforcement (MS-7). Group cap of 256 (NF-13).
- Direct conversations rejecting a third membership (MS-4).

**Frontend increment:** create a group, view and manage members, join a channel.

**Verify:** a member added to a group with existing history cannot read anything before their join point, asserted at the API, not just hidden in the UI. A new channel member reads from sequence 1. A non-writer posting to a channel is rejected. Adding a 257th group member is rejected. Fan-out write count is measured and shown to be constant regardless of member count (NF-12).

**Verified.** All five. NF-12 measured at **2 rows per send** — the entry and its outbox row — with 2 members and with 64. The test states plainly what that does *not* claim: the projection is per member by design and off the request path, which is the distinction ADR-0002 actually drew.

The context gained its one domain service here, as [ADR-0010](./adr/0010-ddd-conventions.md) predicted, though as a package function rather than a type. Whether an actor may change who belongs depends on the conversation's kind *and* the actor's role; Conversation and Membership are separate roots that cannot see each other, so on either one it would be the same rule written twice.

Two authorisation bugs, both found by tests rather than review:

- **A removed member could still read the conversation.** Three read paths asked whether a membership row existed, which stays true of somebody removed. Replaced with one guard every read routes through, so the fourth read path cannot forget.
- **A reader posting to a channel got 422 rather than 403**, because their payload was validated before their permission was checked. Authorisation now precedes content — which also stops the server doing work on a payload it is going to reject, and matters more once phase 7 makes payloads large.

---

## Phase 5 — Revisions and reactions — **complete**

**Goal:** edit, delete-for-everyone and reactions, without breaking forward-only sync.

**Deliverables**
- Revision entries referencing their target (MS-8). Author-only, except admin delete (MS-9).
- Reactions as separate state with their own light sync path, carrying no sequence number (MS-10).
- Replies as a reference field on an entry.

**Frontend increment:** edit and delete affordances, reaction picker, reply rendering.

**Verify:** the load-bearing test of the whole phase — a client that has **already synced past** an entry receives and applies a later edit of it. A pre-phase-5 client that does not understand revisions must degrade to showing the original, not crash. Reacting a hundred times produces no sequence numbers, confirmed by asserting the conversation head is unchanged.

**Verified.** All three, at both levels: in Go against a real socket, and in two browsers where the reader has the message on screen when it is edited. A hundred alternating taps leave the head where it was.

Edits and retractions became two kinds rather than one kind with an empty payload. "Empty means deleted" is an encoding a client can misread in the direction that shows withdrawn content, and the permissions differ — an administrator may take something down but not replace it with different words. Moderation needs the first power; nobody needs the second.

Amendments target the original, never each other, so applying the highest-sequenced amendment for a target is always correct and there is no chain to walk.

Three findings:

- **The broadcast omitted what an edit amends**, so a live client would have had to fetch to discover it — defeating the point of carrying the body. Found by the socket assertion, which is the only place it could have been.
- **Payloads were rebuilt on load through the validating constructor**, which [ADR-0010](./adr/0010-ddd-conventions.md) forbids and which a retraction breaks at once, having no content type and no bytes. Loading now reconstitutes.
- **The unmapped-event guard from phase 3 earned its keep**: a new event with no route failed the write that produced it rather than publishing on an empty key and silently losing its ordering.

The client gained `web/src/transcript.ts` — a pure function turning entries into messages. It is ADR-0008's sentence "clients interpret entries rather than rendering them one-to-one" as code, and the place a mistake would be invisible, so it is unit-tested on its own.

---

## Phase 6 — Client persistence and search — **complete**

**Goal:** both clients work offline and can search, which the server cannot do for them.

**Deliverables**
- CLI client: Bubble Tea, SQLite local store, full sync protocol, local search.
- Browser client: SQLite over OPFS, same protocol, same search semantics.
- Client-side schema migrations for both.
- Optimistic send: local pending entry reconciled against the server sequence on acknowledgement.

**Frontend increment:** the browser client stops being in-memory. Cold start renders from local storage before the socket connects.

**Verify:** cold start with a populated store renders the conversation list in under 500 ms with the network disabled (NF-5). Search returns results offline. Send while offline, come back, and the entry sends exactly once with no duplicate. The CLI and browser produce identical results for the same query — this is the check that the two stores have not diverged in behaviour.

**Verified.** Cold start rendered the conversation list in **255 ms** against NF-5's 500. Search works with the api unreachable. A send that fails is flushed on the next connection with its original identifier, arriving exactly once, in both clients. And the cross-client check runs the CLI's `-sync` and `-search` against the same account the browser is signed in as, comparing four queries.

**One scope limit, stated rather than hidden — and since closed.** This phase's browser test cuts `/v1`, not the whole network, because a browser needs the network to fetch the document and its scripts; a *fully* offline cold start needs the app shell cached by a service worker. What this phase verified is its own part: with the api answering nothing, the screen is drawn from local storage. Phase 10 added the service worker and the test that cuts everything, which renders in 125 ms.

Four findings:

- **OPFS's synchronous access handles are worker-only**, so SQLite's OPFS VFS means the database lives in a worker and every read becomes asynchronous — which reaches all the way into rendering, because `useSyncExternalStore` needs a synchronous snapshot. Instead the database is in memory on the main thread and its bytes are snapshotted into OPFS after writes settle. Reads stay synchronous; the worst loss is the last quarter-second, which sync repairs because the mark is in the snapshot too.
- **Vite's dependency pre-bundling broke SQLite's runtime wasm resolution**, leaving the store silently falling back to memory. `optimizeDeps.exclude` fixes it; an empty `catch` is what made it hard to find, and it now logs.
- **Entries could be stored before their conversation row existed**, so the mark could not advance and a cold start rendered an empty screen over a full database. Both stores now create the row alongside the entries. The same bug was latent in the CLI.
- **A pending send could not be flushed after a reload**, because the function that posts it was remembered from an earlier send rather than being a dependency — so a message queued before a refresh stayed queued forever.

---

## Phase 7 — Media — **complete**

**Goal:** send photos and video.

**Deliverables**
- Direct-to-object-store upload with a presigned URL; bytes never transit `api`.
- `media.attachments` topic. Worker producing thumbnails and size variants, idempotently (MD-2).
- Pending → ready lifecycle, pushed to clients (MD-1).
- 100 MB cap rejected before reading the whole body (MD-4).

**Frontend increment:** attach an image or video, placeholder while pending, thumbnail on ready, full-size on demand.

**Verify:** an entry referencing a large video is readable immediately while the attachment is still pending. Kill the worker mid-processing: the job replays and the attachment does not stay pending forever (MD-3). Process the same attachment twice and assert one set of variants. Upload 101 MB and assert rejection before the transfer completes.

**Verified.** Twelve integration tests drive the use cases against real Postgres and real MinIO, and six browser tests drive two browsers on two nodes.

- **Readable while pending (MD-1):** the recipient's transcript shows the caption before the photo exists, then the placeholder becomes a thumbnail with no action on their part. Proven load-bearing by disabling the readiness notification — four browser tests then fail, because the placeholder never resolves.
- **Replayable job (MD-3):** the store is made unreachable at the point a worker would die — after the job is published, before the variants exist. `Process` returns the error rather than swallowing it, which is what leaves the Kafka offset uncommitted; the attachment stays `uploaded` rather than being marked failed, and the retry completes it. Swallowing the error instead makes that test fail.
- **One set of variants (MD-2):** processed three times, two variants. This is a primary key on `(attachment, name)` rather than the worker remembering, and the test catches a variant name that varies per pass.
- **101 MB rejected (MD-4):** refused with 413 in under two seconds through the browser, on a request carrying one JSON field. And a client that *lies* about the size cannot transfer more than it declared, because the length is part of the signature — MinIO returns 403 and stores nothing.

**Signature Version 4 by hand.** Everything needed is four verbs against one bucket plus a presigned URL, and the whole algorithm is one HMAC chain — against forty modules of AWS SDK for a deployment with one endpoint and one static key. It worked against real MinIO on the first attempt, and a deliberately corrupted signing key fails every test in that package, which is a better guarantee than a dependency's reputation.

**Two deliberate limits, stated rather than hidden.**

- **Video gets no derived renditions.** A poster frame or a smaller copy needs a transcoder: a native dependency, a process pool, and a queue that scales differently from everything else here. Video keeps the identical lifecycle — pending, uploaded, ready — so clients have one shape of state to handle, and is played from the retained original with the browser's own controls. Deferred rather than faked.
- **Photo orientation is not applied.** A phone records rotation in EXIF and a re-encoded rendition loses the tag, so a sideways photo gets a sideways thumbnail. The fix is bounded — read the orientation tag, apply one of eight transforms before scaling — and it is not done.

Three findings:

- **`.composer input` matched the attach control**, which broke every send in the browser suite at once: fifteen tests failed on a strict-mode violation from one added element. The selector is now `.composer input:not([type=file])` — the second time a composer selector has been ambiguous, so the reason is written next to it.
- **A swapped image `src` is not a loaded image.** The open-the-larger-rendition test measured `naturalWidth` after waiting for the source to change and read zero. Waiting on the decoded size is the assertion that was meant.
- **A UUIDv7 prefix is a timestamp, so it does not vary.** The first attempt to prove the idempotency test could fail used `id.New()[:4]` as a per-pass suffix — identical every time, so nothing broke and the test looked untrustworthy when it was fine. The same property bit a handle generator in phase 5.

---

## Phase 8 — SFU test harness — **complete**

**Goal:** the ability to test a media server, built before the media server.

This phase exists because browser tabs cannot load-test an SFU, and because writing the harness first forces the SFU's interface to be defined before its internals.

**Deliverables**
- Headless Pion peer publishing canned Opus and VP8 from a file.
- Multi-peer load script with a configurable participant count.
- Assertions on forwarded RTP: packets arrive, sequence continuity holds, keyframes appear when requested.
- Bandwidth throttling, so layer switching can be provoked deliberately in phase 9.

**Verify:** the harness joins a stub SFU, publishes, and asserts on what comes back. Twenty simulated peers run without the harness itself becoming the bottleneck — measured, because a harness that is slower than the thing it tests proves nothing.

**Verified.** Seven tests against a stub that forwards RTP and does nothing else.

- **Media crosses the server:** 159 video and 127 audio packets in 2.5 s, one keyframe detected, **zero gaps and zero duplicates**, first packet **103 ms** after joining.
- **Twenty peers on twelve cores:** all twenty joined in **743 ms**, slowest first packet **163 ms**, 3877 packets across nineteen receivers, worst gap count **0**. Every receiver is checked individually, because a fan-out that thins out as it goes averages to something that looks fine.
- **Keyframe on demand:** a receiver asks, and one arrives **21 ms** later. This is the mechanism CL-6's recovery and phase 9's layer switch both rest on.
- **Throttling is observable downstream:** 180 packets in a window at 1.2 Mbit/s, 47 in the next at 150 kbit/s.
- **A publisher does not receive its own media** — the failure that looks correct from one browser and doubles everyone's bandwidth.

**Three decisions worth recording.**

- **Signalling is one exchange, not trickle ICE.** A peer gathers, offers, and gets a complete answer, which a server on a known address with a known port range can always give. `Signaller` is a two-method interface with no transport in it: the stub uses HTTP, phase 9 uses the WebSocket that already exists (ADR-0004). Browsers will trickle because browsers do; what this fixes is that trickling stays optional.
- **The media is synthetic, not a captured file.** An SFU never decodes what it forwards — it reads the VP8 payload descriptor for frame boundaries and keyframes and copies the rest — so the properties that matter are a valid frame tag, honest keyframe marking, and a steady rate. That buys a bitrate and framerate that are parameters rather than properties of a file, a keyframe on demand at an exact moment, and no binary fixture in the repository that nobody can inspect.
- **Congestion is reported, not shaped.** `ReportBandwidth` sends REMB and `ReportLost` sends NACK, directly. Genuine throttling means a traffic shaper — privileged, platform-specific and flaky to require of a test — and what a server reacts to is the message. This exercises the server's *reaction*; measuring its *estimator* needs a real constrained path, which belongs with phase 10's load testing.

Three findings, all from running it:

- **A sender's RTCP must be read for its packets to exist.** The stub added forwarding tracks and never read the senders, so a receiver's keyframe request filled a queue nobody drained and the publisher never heard. It presents as "the harness cannot get a keyframe", nothing about feedback. The stub now relays PLI to the publisher's connection and SSRC — the mapping an SFU has to keep, because feedback travels the opposite way to media.
- **Adding a track before setting the remote description negotiates cleanly and delivers nothing.** Pion creates a transceiver of its own, so the answer carries more media sections than the offer asked about. Offer first, then tracks, so each one claims a receive-only section the joiner already declared. Both peers reported `connected` throughout.
- **A mutex added to fix a race deadlocked the suite.** `Next` held the lock and called `Interval`, which takes it — Go's mutex is not reentrant, and it presents as every test hanging rather than as anything to do with a lock. The race was real (`-race` found the source being read by the send loop while a PLI wrote it); the fix needed an unlocked private form rather than a lock that pretends to be reentrant.

---

## Phase 9 — SFU — **complete**

**Goal:** live audio and video calls. The largest and riskiest phase ([ADR-0006](./adr/0006-custom-pion-sfu-with-simulcast.md)).

**Deliverables, strictly in this order**
1. Signalling over the existing WebSocket; call lifecycle; entitlement from membership (CL-1, CL-2, CL-3).
2. ICE, DTLS-SRTP, single-layer Opus and VP8 forwarding, with NACK and PLI handling (CL-6).
3. **Only then** simulcast: per-receiver bandwidth estimation, layer selection, keyframe on switch (CL-5).

Step 2 is a shippable product on its own. If schedule pressure arrives, stop after it — that retreat is recorded in ADR-0006 and taking it is not a failure.

### Where this actually stands

**Done and verified.**

- **The call lifecycle**, as an aggregate with 11 tests. CL-2 and CL-3 are rules of the model: a second presence makes a call active, and the last departure ends it terminally. A participant is per device, because one person on a laptop and a phone is two transports and one tile. Joining twice from a device is idempotent.
- **The media plane** — `internal/calling/internal/sfu` — with 6 tests driven by the phase-8 harness. A three-party call forwards in every direction in **0.44 s**. A keyframe request reaches the publisher in **21 ms**, against CL-6's two seconds. Two calls on one node cannot hear each other. A departing participant's transport is released and an empty call is forgotten. Race-clean under `-race`.

- **Signalling over the existing WebSocket** (ADR-0004), with 6 integration tests against real Postgres and the real media plane. Messaging delegates frame families it does not own, so it carries call traffic without learning what a call is. There is no *start* operation: a call exists because somebody joined a conversation that had none, so CL-2 falls out — no code path could create a second, and a partial unique index catches two people pressing call at once. CL-1 is one question asked of Messaging. CL-3 fires on the last departure, whether that was a leave frame or a socket that closed.
- **Persistence**: migration 00009, one row per presence rather than per device, so a rejoin does not erase the record of who was in a call.
- **The call UI**, with **media flowing between two real browsers, both directions**. Start, ring, join, mute, hang up, participant tiles. Four states, because "a call you are in" and "a call in progress you have not joined" are the difference between a button that says leave and one that says join. 6 browser tests: the ring arrives without being asked for, both sides show a playing remote tile, mute is local, hanging up ends the call with the media node reporting zero calls.
- **`cmd/sfu`, and with it calls that cross api nodes.** Media forwarding is its own process now, both api nodes signal to it, and **a browser on node A and a browser on node B hold one call with media both ways**. The entry that used to be under *not done* said the fix was one shared media process plus an HTTP implementation of `MediaNodes`, and that nothing above the port would change. Both were right: the domain, the use cases and the signalling are untouched, and the whole of it is one package — a client, the surface it talks to, and the wire they agree on, in one directory so the two ends cannot drift.

  Two api nodes, one media node, one call, media both ways, in **0.32 s** in the integration test.

  The hard part was the one named in advance: an offer the node produces has to reach whichever api node holds that participant's socket, and the node holds no sockets to look one up with. **It broadcasts** ([ADR-0013](./adr/0013-media-node-broadcasts-its-offers.md)) — every api node subscribes to a stream of offers, delivers the ones it has a socket for, and drops the rest. A registry mapping devices to nodes would target the delivery, and is a second source of truth about where a client is: wrong for exactly as long as a reconnection takes to notice, which is when an offer is most likely to be in flight.

  Three things that only exist once the two halves are in different processes:

  - **An offer can be delivered to nobody, and silence is the wrong answer.** No api node is subscribed while the media node starts and while a stream reconnects. So `Renegotiator` returns an error and an undelivered offer is retried — without that, a one-second window costs a participant every joiner for the rest of the call, with nothing reporting it.
  - **`offer_subscribers` is a health signal, not a statistic.** Zero with calls above zero is a node forwarding media that cannot tell anybody about a new publisher. Every other number looks healthy and the symptom is one-way media — which is the misdiagnosis this phase has already made once, so it is in `/health`, in the browser suite's failure message, and `scripts/e2e.sh` refuses to start the suite until both api nodes have subscribed.
  - **An unanswered offer used to wedge a participant permanently.** The flag that permits one exchange at a time was only ever cleared by an answer, so an offer lost for any reason meant that participant never received another track for the life of the call. Pre-existing, and found by asking how the new path fails rather than by a test.

  One test-only race came out of it too, and it is worth the line because of how it was hidden. The phase-8 stub's offer channel was closed on leave under a `sync.Once`, whose comment claimed to guard a renegotiation in flight and did not: `Once` stops a second close, not a send racing the first. Race-clean for a whole phase, and `-race` found it within minutes of the server gaining a retry.

**The bug that hid all of this, and how it presented.** Media between browsers appeared to fail in one direction, with the failing direction varying between runs — which reads exactly like a renegotiation race, and the first fix attempted was one. It was not that. Two api nodes both defaulted their media address to the literal `"local"`, so the check meant to enforce CL-4 — is this call mine? — passed on both. A call started on node A was quietly continued on node B's own in-process SFU: two participants, two media planes, no shared media, no error anywhere. Both people saw what looked like a working call.

`/health` reporting `calls: 1, 1` is what settled it, and it took adding that to the failure message. The lesson is the ordinary one: the symptom pointed at the layer I had most recently written, and the cause was in wiring I had written earlier and reasoned about rather than measured.

Two things came out of it. The node address now defaults per process, so the mismatch is loud instead of silent. And a browser pair on *one* node proves the whole media path, while a pair across *two* nodes proves the shared one — that second test asserted a refusal when it was written, because an in-process media plane could not do better, and `cmd/sfu` is what turned the assertion around.

- **The measurements**, all three met, and two of them only after the measuring found something.

  | | Requirement | Measured |
  |---|---|---|
  | NF-3 | join to first media, p95 under 2 s | **109 ms** p95 over 20 calls |
  | NF-4 | one-way audio latency, p95 under 200 ms | **431 µs** p95 over ~150 packets each way |
  | NF-14 | 3 concurrent 4-participant video calls on 4 vCPUs | **16 calls, 64 participants**, no loss |

  Loopback on a developer's machine, so what they establish is that the code is not the reason a limit would be missed. NF-3 and NF-4 are Go tests; NF-14 is `scripts/capacity.sh`, because a capacity claim about a deployed process cannot be answered by a test sharing its machine with the load — it runs the real `cmd/sfu` under `GOMAXPROCS=4` and drives it with the phase-8 harness over the node's own HTTP wire.

  **The first NF-3 measurement was wrong, and finding out why was the point.** Timing the first packet gave 109 ms and passed. But video packets are not a picture: a decoder joining mid-stream has no reference frame and shows nothing until a keyframe. Timing the first *keyframe* — which is what a person waits for — gave **1.573 s** against a limit of 2, and the number was not about this server at all. It was the publisher's keyframe interval.

  The cause was a gap in the keyframe-on-join behaviour. The media plane asked a publisher for a keyframe when a *new track* appeared while somebody was watching, and not when somebody *joined* a publisher who had been sending for a while — which is the common case. Three lines, in the right place: not when the tracks are added, which is a few hundred milliseconds before the joiner's transport exists and a keyframe produced then is forwarded into a connection that cannot carry it, but when the transport reports connected. **1.573 s → 109 ms.**

  **NF-14 found a race that only exists at scale.** At 64 participants the node logged Pion refusing a renegotiation — `have-remote-offer -> SetLocal(offer)` — twice, alongside three ICE gathers that never completed. A participant is in the call's map before its own offer/answer finishes, so another publisher's track arriving in that window started a second exchange on a connection mid-join. The participant was then left with its negotiation flag set and never received another track for the rest of the call.

  Fixed by treating the join as the exchange it is. What that did to the same 64-peer run: join time 24.7 s → **9.4 s**, slowest first packet 5.08 s → **193 ms**, five warnings → **none**.

- **Simulcast (CL-5)**, step 3 — the step this plan said to stop before under pressure, and the item ADR-0006 called the largest in the project. One publisher's three qualities, one chosen **per receiver**, and the choice revisited as receivers report what they can take.

  Measured, in one call with one publisher sending a ladder of 75 / 200 / 600 kbit/s:

  | Receiver | Says it can take | Is sent |
  |---|---|---|
  | says nothing | — | **591 kbit/s** |
  | reports 150 kbit/s | 150 kbit/s | **73 kbit/s** |
  | then reports recovery | 10 Mbit/s | **591 kbit/s** again |

  Three things carry it, and all three are load-bearing:

  - **A switch happens only on a keyframe.** A decoder resolves each frame against the previous one, so handing it the middle of a different encode produces the smear people describe as "the video broke".
  - **Sequence numbers and timestamps are rewritten.** Each layer numbers its own packets from its own start, so forwarding them unchanged reads to a receiver as catastrophic loss at the moment of every switch. An offset per subscription, rebased at each switch, is what makes it invisible — and it is why a subscription now owns a track rather than sharing one per source.
  - **A keyframe is asked for, not waited for.** Otherwise the switch lands whenever the publisher's next scheduled keyframe does: up to two seconds of continuing to send a layer the receiver has just said it cannot take. The same lesson NF-3 taught at join.

  **Selection is a periodic decision, not an event handler**, and the first version got that wrong. Reacting only to bandwidth reports meant a client that sends none — most of them — stayed forever on whichever layer happened to deliver the first keyframe: a receiver on a fast link watching the smallest encoding, with nothing anywhere reporting a problem.

  Two more findings worth the space:

  - **Pion cannot publish simulcast.** It receives it, but its sender never stamps the stream identifier into the RTP header extension that tells layers apart — Pion's own simulcast test writes that extension by hand, which is as clear a statement as exists. So the harness packetises its own video and stamps its own headers, and the sample-based publish path is gone.
  - **The empty string is a real RID.** It is what a single-layer publisher uses, and using it internally to mean "no layer chosen yet" made every single-layer stream re-enter the not-chosen-yet branch on every packet. Only keyframes were forwarded: two packets in two seconds, video that technically arrived, and every existing assertion still passing.

  **The browser publishes layers too**, since a server that can choose between them is worth nothing if no real client sends them. Chrome sends two per camera here rather than the three the ladder asks for — it decides how many VP8 encodings are worth running from the capture resolution, and a headless fake device is below the threshold for three. Asking for 720p to get the third was tried: it changed the layer count not at all and cost enough encoding to time out unrelated tests, so it is not in the client.

**Not done.**

- **Layer starvation**, named as an accepted cost in ADR-0006 and still not addressed: a publisher whose upload cannot sustain three encodings starves the top one, and this server would keep selecting a layer that has stopped arriving. It needs a liveness check per layer — measured bitrate is already there to build it from.
- **Server-side bandwidth estimation.** Selection runs on what receivers report. A receiver that says nothing gets the best layer, which is right until it is not; inferring congestion without being told needs a congestion controller and a real constrained path to test it on.

**Two decisions, both forced by running it.**

- **Renegotiation cannot be avoided.** A two-party call is asymmetric: the second to join receives the first's tracks in the answer to their own offer, and the first learns of the second's only if something offers the other way. The alternative — clients pre-declaring a receive slot per possible participant — removes renegotiation at the cost of a participant limit baked into every client and a demuxing scheme that varies by browser. So the server re-offers, through a `Renegotiator` the signalling layer installs — one implementation calls the socket handler directly, the other publishes to every api node, and the media plane cannot tell which it has.
- **A NACK is answered here, not relayed.** The packet is in this server's send buffer, and the publisher's sequence numbers mean something different. CL-6's retransmission therefore works because the server buffers, not because the publisher does. A PLI *is* relayed, because only a publisher can make a keyframe.

Three findings:

- **Renegotiating inline wedges a three-party call.** It runs on Pion's `OnTrack` callback, and the RTP read loop does not start until that returns — so an inline exchange waits for ICE gathering and the client's answer before forwarding a packet. With two participants it merely delays the first frame; with three, each new publisher blocks behind the previous one's exchange and the whole suite hangs with no output.
- **The harness was miscounting, and the SFU was right.** Every publisher's video track is called `video`, and the harness keyed arrivals by name — so two publishers collapsed into one entry and a three-party call looked like a server that renegotiated once and stopped. Keyed by SSRC now. Worth noting which way this went: the instinct was to distrust the new code.
- **Phase 8's `Signaller` had a hole.** The stub never re-offered, so the interface never needed to carry an offer from the server — which is the one thing the harness-first approach got wrong. Added as an optional `Renegotiable` rather than folded in, so the stub stays as simple as it was.

**Frontend increment:** call UI — start, ring, accept, decline, mute, participant tiles, hang up.

**Verify:** two browsers plus the harness hold a group call. Drop 5% of packets and confirm video recovers rather than freezing beyond 2 seconds (CL-6). Throttle one participant with the phase-8 harness and assert the SFU switches that receiver's layer down and back up. Call join to first media under 2 seconds (NF-3). One-way audio latency under 200 ms (NF-4). Three concurrent 4-way calls on 4 vCPUs (NF-14).

Of those: two browsers hold a call on one node and across two nodes, and a three-party call forwards in every direction under the harness. NF-3 is 109 ms, NF-4 is 431 µs, NF-14 has four times the headroom the requirement asks for. Keyframe recovery is measured at 21 ms against CL-6's two seconds, but *packet loss* is not — the harness can throttle and cannot yet drop. A receiver that reports less bandwidth is switched down a layer and back up, per receiver, which is the layer-switching assertion — provoked by the report the server reacts to rather than by a shaped network.

---

## Phase 10 — Hardening — **complete**

**Goal:** the things that make it survivable, none of which are features.

**Deliverables**
- ~~Presence and typing indicators over Redis with TTLs — ephemeral, never persisted.~~ **Done.** Redis only, with expiry, and the expiry is the design rather than an optimisation: both facts are false within seconds, and expiry is also what makes a node dying safe. A node holding a socket renews a claim every ten seconds and a claim lasts thirty; a node that stops renewing stops claiming, with no cleanup to run and nobody to run it. The alternative — a set added to on connect and removed on disconnect — leaves a permanently online ghost every time a process is killed.

  Sorted sets rather than keys with a TTL, because an account has several devices on several nodes: each device is a member scored by when it was last seen, so a dead node's device ages out of the window while its live sibling keeps the account online. One key with one expiry cannot do that — whichever node refreshed last would keep the dead device alive.

  **Presence is asked for; typing is pushed.** Not an inconsistency: a typing indicator has to appear instantly for somebody already looking, and presence is soft state a client polls while it has a conversation open — eight seconds, on a socket it is already holding. Pushing presence would fan every connect and disconnect out to every member of every conversation that account belongs to, which is the same work moved to the moment somebody opens a laptop and paid for conversations nobody is looking at.

  Typing is *both* recorded and broadcast, and both are needed: the broadcast makes it appear for people already there, the record makes it appear for somebody who opens the conversation a second later and asks. Every answer is a whole snapshot rather than a delta, so a lost push, a reconnect and a first render all repair themselves the same way.

  **Verified across two nodes**, which is where a naive implementation is wrong — presence held in a node's memory is presence only that node can see. Three integration tests and three browser tests: the other person shows as here, shows as typing while they type, stops when they send, and disappears when they close the tab. A non-member asking gets silence rather than a refusal, so asking cannot be used to discover which conversations exist.
- ~~Push notifications as a Kafka consumer.~~ **Done**, and a consumer rather than something the send path does: a person waiting for their message to be accepted must not also wait for a provider on the other side of the internet, and a provider being down must not fail a send.

  The consumer holds the whole judgement, which is where all of it is. Three rules, each somebody's complaint if it is missing: **not the author**, **not somebody already looking**, and **not twice**. The second is why presence was built first — a notification on the phone in your hand while you read the message on it is worse than silence, because it teaches people to ignore notifications. The third is what at-least-once delivery makes inevitable: a duplicated unread badge is invisible, a duplicated buzz is not, and the claim has to be shared across processes because the redelivery may land on a different worker.

  **APNs and FCM are not implemented, and cannot be** ([ADR-0014](./adr/0014-push-decides-here-delivers-elsewhere.md)): both need credentials issued to a real application by a real vendor account, and NF-15 says the system starts locally with no cloud dependencies. The default sender logs. Inventing a device-token table to make the seam look complete was rejected too — tokens come from a mobile SDK, there is no mobile client, and a table with no writer asserts only that a join works.

  Suppression and presence both **fail toward sending**: if Redis cannot say, the notification goes out, because a notification somebody did not need is a smaller failure than silence about a message they did.

- ~~A dead-letter topic for records a consumer skips as permanently unprocessable.~~ **Done.** Phase 3 established that a record which can never be applied must be skipped rather than retried — retrying forever blocks every record behind it on that partition — and left a `ponytail:` comment saying the skipped record was logged and gone. It now goes to `platform.dead_letter` with the original bytes verbatim, which consumer gave up, why, and where to find it in the log it came from. Topic, partition *and* offset, because offsets are per partition and two records on one topic routinely share one.

  One topic for every consumer rather than one each: what an operator does with these is look at them, and looking in five places is how nobody looks at all. Publishing is best-effort by necessity — this is already the failure path, and a consumer that stopped because it could not report a skip would have converted one lost projection into the stalled partition the skip exists to avoid.
- ~~Rate limiting on send, handle search, and connect.~~ **Done**, in Redis rather than in each process — which is the whole change, because a limit counted per node is multiplied by the node count and loosens every time the deployment grows. Phase 1's in-process limiter said so in a `ponytail:` comment and named this phase as its replacement; it is gone.

  Sixty sends per ten seconds, thirty handle lookups per minute (ID-5), thirty connections per minute. All three are far above what a person does and far below what a loop does, which is the only band a useful limit occupies. Per account, not per device or per address: a device is something a client can make more of, and an address is shared by everyone behind one office router.

  **It fails open, deliberately.** A limiter that cannot reach Redis allows the action, because this exists to stop abuse and accidents rather than to enforce anything correctness rests on — and refusing every send because a cache is unreachable is a far worse outage than the one it prevents. That is also this phase's own requirement: kill Redis and sends still succeed. It is stated as a test, because it is the kind of decision somebody later reads as a bug and "fixes".

  One Lua script rather than INCR then EXPIRE, because a process dying between the two leaves a counter with no expiry — a caller permanently at their limit, with nothing to clear it and no reason anybody would look. A fixed window, whose flaw is named where it is chosen: a caller bursting across a window boundary gets twice the limit for an instant, which for the thing this actually guards against is not a different outcome.
- ~~OpenTelemetry traces spanning HTTP, WebSocket and Kafka on one correlation identifier (NF-16).~~ **Done**, and the substantive half was not the tracing.

  **An event did not carry the identifier at all.** Requests have since phase 0, but a send is an HTTP request, a Redis publish, an outbox row, a Kafka event and a worker projection — and the worker's half had nothing tying it to the request that caused it. Five log lines in five places, which is the situation the identifier exists to prevent. Migration 00010 puts it on the outbox row, the relay carries it to a Kafka header, and the consumer restores it into the handler's context before the handler runs — so a projection's logs carry it without every consumer remembering to.

  Nullable, and it stays nullable: a scheduled sweep or a backfill legitimately has no request behind it, and `''` would say "correlated with nothing", which is a claim rather than an absence.

  **Traces are off unless `OTEL_EXPORTER_OTLP_ENDPOINT` is set.** NF-15 says the whole system starts locally with one command and no cloud dependencies, so requiring a collector would break one requirement to satisfy another. Spans are created either way and go nowhere. The propagator is installed regardless, so a deployment that turns tracing on later finds the plumbing already there rather than discovering context was never propagated.

  Spans on all three transports: a span per HTTP request continuing whatever the caller sent, **a span per socket frame** rather than per connection — a connection lives for hours and a span that long is a bar on a chart, while what somebody wants to know is why *this* join took two seconds — and a span per Kafka batch published and per record consumed, joined by W3C trace context in the headers. Every span carries the correlation identifier as an attribute under the same name as the log field, so one value is searchable from either side.

  The tests assert the two things that go wrong quietly: a trace that does not span processes looks perfect in isolation, and a span with no correlation identifier is findable only by somebody who already has the trace identifier.
- ~~Backpressure: what happens to a slow socket consumer, and what happens when Redis or Kafka is unavailable.~~ **Done**, and mostly already built: the bounded per-socket queue is phase 2's, the outbox surviving Kafka is phase 3's. What this phase added is the tests that say so, and one of them found that the mechanism it was meant to check was not the one doing the work.

  **A slow consumer is closed, not buffered.** Closing is safe *because* of the sync protocol — the client reconnects, resumes, and is told what it missed, which is the same mechanism that already covers being offline. So a dropped slow consumer costs one fetch and no correctness, where an unbounded queue per socket is a memory incident with a delay.

  The test took four attempts and each failure was informative. A 400 KB payload is refused with 422, because an entry is capped at 64 KB and larger content is an attachment. A single sender cannot overflow a 64-message queue when its own rate limit is 60 per 10 seconds — so it takes several. And **asserting only "the client was dropped" passed with the queue made unbounded**, because the ten-second write deadline was quietly doing the work; the assertion is now on *how quickly*, which only the queue can achieve.

  **Redis unreachable: sends still succeed.** Verified against a client pointed at a closed port, which exercises every Redis-dependent path at once — the broadcast, presence, and the rate limiter, which is designed to fail open and would otherwise refuse everything the moment the cache went away. Delivery falls back to gap sync, which is what ADR-0005 and NF-9 already say it must.

  **Kafka unreachable** was already covered by phase 3: an entry and its outbox row commit together, a failed publish leaves rows to retry, and stopping the relay delays events rather than losing them (NF-6).
- ~~A service worker, so the browser client's offline cold start is genuinely offline.~~ **Done.** Phase 6 made the *data* survive being offline and said plainly that the page did not: its test cut `/v1` and left the document and scripts loading from the network, because a browser needs the network to fetch them. A **fully offline cold start now renders in 125 ms** — faster than the `/v1`-only case at 289 ms, because the shell comes from cache rather than from a dev server.

  Runtime caching, not a precache manifest, and not a plugin. A manifest means a build step, a generated list of hashed filenames, and a worker that behaves differently in development from production — which is the kind of difference discovered on the day it matters. Network-first, so being online always means being current and a deploy is picked up on the next load.

  Registered in development too, against the usual advice, for a reason: the browser suite runs against a dev server, so registering only in production would make the one thing this exists for untestable. A capability nobody can test is a capability nobody should claim.

  **`/v1` is never cached**, and that is the most important line in the worker. The client's correctness rests on knowing whether the server answered: a cached API response would make it believe it had synced when it had not, and a stale conversation list served as fresh is worse than no answer at all.

- **A flake in the media tests. Found, and it was the same bug NF-14 exposed** — worth recording because of how it was found, which was not by looking for it.

  The symptom was a hang: roughly one run in three, `go test ./...` sat at `<-gathered` inside `Join` until Go's ten-minute panic, naming a different test each time. It reproduced on `internal/calling/internal/sfu` alone under `-count=5`, and on the commit before any of the node work, so it was pre-existing and not the node work.

  Three things happened in order, and only the third was a fix:

  1. **Bounding the wait made it legible.** An unbounded wait for ICE gathering is a client's join that never returns, which is wrong on its own terms. Capped at five seconds and logged, the same flake became an 85-second failure naming the participant who received nothing, instead of 450 seconds and a goroutine dump.
  2. **That ruled out the obvious explanation.** Ten runs produced no gathering timeout at all and still failed — so a slow gather was not it, and it was not one test's own logic either.
  3. **NF-14 named it.** Driving the real node with 64 participants logged Pion refusing a renegotiation twice — `have-remote-offer -> SetLocal(offer)` — because a participant is in the call's map before its own exchange finishes. Two `SetLocalDescription` calls interleaving on one connection is also exactly what leaves a `GatheringCompletePromise` unresolved, which is the hang.

  After the fix: 33 consecutive runs of that package clean, where `-count=5` had hung twice out of two attempts. Not proof, and a race is never disproved by passing runs — but the mechanism accounts for all three symptoms, which guessing never did.

  The lesson is the same one phase 9 keeps teaching. The flake was in the test suite and looked like a test-suite problem; the cause was a production race that a load measurement found while asking about something else. What made it findable was making the server say what it was doing — the gathering timeout and the refusal both had to be logged before either meant anything.

**Verify:** a load test at target concurrency meets NF-1 and NF-2. Kill Redis: sends still succeed, delivery falls back to gap sync on reconnect. Kill Kafka: sends still succeed, the outbox drains on recovery, nothing is lost (NF-6). A deliberately slow client is disconnected rather than being allowed to consume unbounded memory.

**All four, and the two measurements are new:**

| | Requirement | Measured |
|---|---|---|
| NF-1 | send to receipt on a connected device, p95 under 300 ms | **4 ms** p95 over 50 messages, across two nodes |
| NF-2 | gap sync of 1 000 entries, p95 under 1 s | **13 ms**, paged as a client pages |

NF-1 is timed across *two* nodes, which is the only honest way: ADR-0005 exists because the recipient's socket is almost never on the node that accepted the write, so a single-node measurement would leave out the Redis hop every real delivery makes. It is timed from before the HTTP request until the frame arrives on the other socket — a server-side measurement would omit the dispatch and the socket write, which are the parts a person experiences.

NF-2 is one sample rather than a distribution, and the test says so rather than implying otherwise: a thousand entries take long enough to seed that fifty repetitions would be minutes of setup to sharpen a number already two orders of magnitude inside its limit.

Loopback on one machine, so what these establish is that the *code* is not the reason a limit would be missed. A deployment puts a network between every arrow and has to be measured where it runs.

Killing Redis, killing Kafka and dropping a slow client are covered above under backpressure — including the discovery that the slow-client test was passing for the wrong reason.

---

## Deliberately not in the plan

Every item here was decided against, not forgotten. See the out-of-scope section of [requirements.md](./requirements.md) and the ADRs for reasoning.

End-to-end encryption · server-side search · SSE · multi-node SFU cascading · call recording · CLI calling · phone-number identity · voice messages · disappearing messages · stories · forwarding · threads.
