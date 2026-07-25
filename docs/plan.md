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

## Phase 2 — The log, the socket, and sync

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

---

## Phase 3 — Outbox, Kafka, projections

**Goal:** unread counts and receipts exist, and are correct despite being eventually consistent.

**Deliverables**
- Outbox table written in the send transaction; a relay that publishes and marks sent.
- Identity's interim logging event publisher replaced by the outbox publisher, so `identity.device_revoked` reaches Messaging over Kafka.
- `messaging.entries` and `messaging.receipts` topics, keyed by conversation.
- Projections: cursor, unread count, delivery state. Idempotent consumers (NF-8).
- Cursor advance, forward-only (MS-11, MS-12). Three-state delivery (MS-13).

**Frontend increment:** conversation list with unread badges, and delivery ticks on sent messages. This is the first UI that must tolerate eventual consistency — an entry can be on screen before its projection lands (NF-7).

**Verify:** unread counts converge within 2 seconds under load. Replay the same Kafka messages and assert badges and receipts are unchanged — this is the idempotency test and it must be explicit. Stop the relay, send ten entries, restart it: all ten events publish, none twice. Read on one device clears the badge on another.

---

## Phase 4 — Groups and channels

**Goal:** the other two conversation kinds, with their membership rules.

**Deliverables**
- Membership mutation, admin role, invite links.
- Join-point policy: groups begin at head, channels at sequence 1 (MS-5, MS-6).
- Channel writer role enforcement (MS-7). Group cap of 256 (NF-13).
- Direct conversations rejecting a third membership (MS-4).

**Frontend increment:** create a group, view and manage members, join a channel.

**Verify:** a member added to a group with existing history cannot read anything before their join point, asserted at the API, not just hidden in the UI. A new channel member reads from sequence 1. A non-writer posting to a channel is rejected. Adding a 257th group member is rejected. Fan-out write count is measured and shown to be constant regardless of member count (NF-12).

---

## Phase 5 — Revisions and reactions

**Goal:** edit, delete-for-everyone and reactions, without breaking forward-only sync.

**Deliverables**
- Revision entries referencing their target (MS-8). Author-only, except admin delete (MS-9).
- Reactions as separate state with their own light sync path, carrying no sequence number (MS-10).
- Replies as a reference field on an entry.

**Frontend increment:** edit and delete affordances, reaction picker, reply rendering.

**Verify:** the load-bearing test of the whole phase — a client that has **already synced past** an entry receives and applies a later edit of it. A pre-phase-5 client that does not understand revisions must degrade to showing the original, not crash. Reacting a hundred times produces no sequence numbers, confirmed by asserting the conversation head is unchanged.

---

## Phase 6 — Client persistence and search

**Goal:** both clients work offline and can search, which the server cannot do for them.

**Deliverables**
- CLI client: Bubble Tea, SQLite local store, full sync protocol, local search.
- Browser client: SQLite over OPFS, same protocol, same search semantics.
- Client-side schema migrations for both.
- Optimistic send: local pending entry reconciled against the server sequence on acknowledgement.

**Frontend increment:** the browser client stops being in-memory. Cold start renders from local storage before the socket connects.

**Verify:** cold start with a populated store renders the conversation list in under 500 ms with the network disabled (NF-5). Search returns results offline. Send while offline, come back, and the entry sends exactly once with no duplicate. The CLI and browser produce identical results for the same query — this is the check that the two stores have not diverged in behaviour.

---

## Phase 7 — Media

**Goal:** send photos and video.

**Deliverables**
- Direct-to-object-store upload with a presigned URL; bytes never transit `api`.
- `media.attachments` topic. Worker producing thumbnails and size variants, idempotently (MD-2).
- Pending → ready lifecycle, pushed to clients (MD-1).
- 100 MB cap rejected before reading the whole body (MD-4).

**Frontend increment:** attach an image or video, placeholder while pending, thumbnail on ready, full-size on demand.

**Verify:** an entry referencing a large video is readable immediately while the attachment is still pending. Kill the worker mid-processing: the job replays and the attachment does not stay pending forever (MD-3). Process the same attachment twice and assert one set of variants. Upload 101 MB and assert rejection before the transfer completes.

---

## Phase 8 — SFU test harness

**Goal:** the ability to test a media server, built before the media server.

This phase exists because browser tabs cannot load-test an SFU, and because writing the harness first forces the SFU's interface to be defined before its internals.

**Deliverables**
- Headless Pion peer publishing canned Opus and VP8 from a file.
- Multi-peer load script with a configurable participant count.
- Assertions on forwarded RTP: packets arrive, sequence continuity holds, keyframes appear when requested.
- Bandwidth throttling, so layer switching can be provoked deliberately in phase 9.

**Verify:** the harness joins a stub SFU, publishes, and asserts on what comes back. Twenty simulated peers run without the harness itself becoming the bottleneck — measured, because a harness that is slower than the thing it tests proves nothing.

---

## Phase 9 — SFU

**Goal:** live audio and video calls. The largest and riskiest phase ([ADR-0006](./adr/0006-custom-pion-sfu-with-simulcast.md)).

**Deliverables, strictly in this order**
1. Signalling over the existing WebSocket; call lifecycle; entitlement from membership (CL-1, CL-2, CL-3).
2. ICE, DTLS-SRTP, single-layer Opus and VP8 forwarding, with NACK and PLI handling (CL-6).
3. **Only then** simulcast: per-receiver bandwidth estimation, layer selection, keyframe on switch (CL-5).

Step 2 is a shippable product on its own. If schedule pressure arrives, stop after it — that retreat is recorded in ADR-0006 and taking it is not a failure.

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
