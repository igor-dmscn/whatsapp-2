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

**One scope limit, stated rather than hidden.** The browser test cuts `/v1`, not the whole network. A browser needs the network to fetch the document and its scripts, so a *fully* offline cold start requires the app shell to be cached by a service worker — which does not exist yet and is production-build work. What is verified is this phase's part: with the api answering nothing, the screen is drawn from local storage. The service worker belongs in phase 10.

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

## Phase 9 — SFU — **partly done: the model and the media plane**

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
- **The call UI**: start, ring, join, mute, hang up, participant tiles. Four states, because "a call you are in" and "a call in progress you have not joined" are the difference between a button that says leave and one that says join. 4 browser tests across two browsers on two nodes: the ring crosses nodes, joining works, mute is local, and hanging up ends the call — asserted against `/health` reporting zero calls on the node.

**Not done.**

- **Media between two browsers.** One cause, not two: whichever side needs the server to re-offer does not get media, and which side that is varies between runs depending on whether the caller's tracks reached the node before the callee joined. The server re-offers correctly — `internal/calling`'s two-way test and the SFU's three-party test both assert media arriving at whoever joined first, through the same frame handler the UI talks to. So the fault is a browser failing to answer a server-initiated offer, and it is not diagnosed. Recorded as an `it.fails` browser test rather than skipped, so the day it works the suite goes red.
- **`cmd/sfu`.** Media forwarding runs inside `cmd/api`. That is a deployment decision and the seam is deliberate: `MediaNodes` names a node by address on every call, so moving it out is one more implementation of that port. The trade being accepted meanwhile is that CPU-bound forwarding and I/O-bound sockets scale together, which ADR-0007 split them to avoid.
- **Simulcast (CL-5)** — step 3, which the plan itself says to stop before under pressure.
- **The measurements** NF-3 (join to first media), NF-4 (audio latency) and NF-14 (three concurrent 4-way calls), all of which want media between real clients first.

**Two decisions, both forced by running it.**

- **Renegotiation cannot be avoided.** A two-party call is asymmetric: the second to join receives the first's tracks in the answer to their own offer, and the first learns of the second's only if something offers the other way. The alternative — clients pre-declaring a receive slot per possible participant — removes renegotiation at the cost of a participant limit baked into every client and a demuxing scheme that varies by browser. So the server re-offers, and `Renegotiator` is the callback the signalling layer will fill in.
- **A NACK is answered here, not relayed.** The packet is in this server's send buffer, and the publisher's sequence numbers mean something different. CL-6's retransmission therefore works because the server buffers, not because the publisher does. A PLI *is* relayed, because only a publisher can make a keyframe.

Three findings:

- **Renegotiating inline wedges a three-party call.** It runs on Pion's `OnTrack` callback, and the RTP read loop does not start until that returns — so an inline exchange waits for ICE gathering and the client's answer before forwarding a packet. With two participants it merely delays the first frame; with three, each new publisher blocks behind the previous one's exchange and the whole suite hangs with no output.
- **The harness was miscounting, and the SFU was right.** Every publisher's video track is called `video`, and the harness keyed arrivals by name — so two publishers collapsed into one entry and a three-party call looked like a server that renegotiated once and stopped. Keyed by SSRC now. Worth noting which way this went: the instinct was to distrust the new code.
- **Phase 8's `Signaller` had a hole.** The stub never re-offered, so the interface never needed to carry an offer from the server — which is the one thing the harness-first approach got wrong. Added as an optional `Renegotiable` rather than folded in, so the stub stays as simple as it was.

**Frontend increment:** call UI — start, ring, accept, decline, mute, participant tiles, hang up.

**Verify:** two browsers plus the harness hold a group call. Drop 5% of packets and confirm video recovers rather than freezing beyond 2 seconds (CL-6). Throttle one participant with the phase-8 harness and assert the SFU switches that receiver's layer down and back up. Call join to first media under 2 seconds (NF-3). One-way audio latency under 200 ms (NF-4). Three concurrent 4-way calls on 4 vCPUs (NF-14).

---

## Phase 10 — Hardening

**Goal:** the things that make it survivable, none of which are features.

**Deliverables**
- Presence and typing indicators over Redis with TTLs — ephemeral, never persisted.
- Push notifications as a Kafka consumer.
- Rate limiting on send, handle search, and connect.
- OpenTelemetry traces spanning HTTP, WebSocket and Kafka on one correlation identifier (NF-16).
- Backpressure: what happens to a slow socket consumer, and what happens when Redis or Kafka is unavailable.

**Verify:** a load test at target concurrency meets NF-1 and NF-2. Kill Redis: sends still succeed, delivery falls back to gap sync on reconnect. Kill Kafka: sends still succeed, the outbox drains on recovery, nothing is lost (NF-6). A deliberately slow client is disconnected rather than being allowed to consume unbounded memory.

---

## Deliberately not in the plan

Every item here was decided against, not forgotten. See the out-of-scope section of [requirements.md](./requirements.md) and the ADRs for reasoning.

End-to-end encryption · server-side search · SSE · multi-node SFU cascading · call recording · CLI calling · phone-number identity · voice messages · disappearing messages · stories · forwarding · threads.
