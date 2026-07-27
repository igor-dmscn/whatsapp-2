# Communication Platform

Real-time messaging, media sharing, and live audio/video calling. A Go backend with React and CLI clients, built as a reference implementation — the codebase is meant to be read, so the reasoning is committed alongside the code.

**Status: all eleven phases complete** — accounts, conversations, the entry log, WebSocket sync, cross-node delivery, the transactional outbox with Kafka and projections, groups and channels with invite links, edits, deletes, replies and reactions, a terminal client, both clients persisting locally with offline search, photos and video uploaded straight to an object store and processed into thumbnails off the request path, and a media test harness that simulates twenty participants against a forwarding server. That includes live calls between real browsers, both directions, signalled over the socket they already hold, with media forwarded by its own process so a call is joinable from any api node, and each receiver sent the quality layer its own connection can carry. Join to first watchable video is 109 ms against a 2-second budget, one-way audio latency 431 µs against 200 ms, and one node carries 16 concurrent four-person video calls on 4 vCPUs against a requirement of 3. Phase 10 hardened it: presence and typing over Redis with expiry, push notifications as a Kafka consumer, rate limits shared across nodes, backpressure, a dead-letter topic, a service worker for a genuinely offline cold start, and OpenTelemetry traces on the same correlation identifier the logs carry. See [the plan](./docs/plan.md) for exactly what exists, including what each measurement found.

## Running it

Requires Go 1.26 and Docker.

```sh
cp .env.example .env
make all       # everything, in this terminal; Ctrl-C stops all of it
```

That starts Postgres, Redis, Kafka and MinIO, applies migrations, and then runs the api,
the worker and the browser client together on <http://localhost:5173>. Their logs are
interleaved, which is the cost of one terminal instead of four.

To watch one of them on its own, the same pieces are separate targets — `make up`,
`make migrate`, then `make api`, `make worker` and `make web` in a terminal each.

Run these through `make` rather than `go run`: the Makefile is the only thing that loads
`.env`, and the binaries fail on a missing `DATABASE_URL` rather than guessing one.

The worker is not optional. It publishes the outbox and builds the read models, so without
it messages still send and arrive live but conversation lists, unread counts, receipts and
thumbnails never update — which looks like data loss and is not.

`make api` forwards call media itself, which is all one machine needs. Set `SFU_URL` and
it signals to `make sfu` instead — the configuration in which two api nodes can share a
call. `make check` runs lint and both test suites; `make build` puts binaries in `bin/`.

### Calling someone on another machine

The steps above are localhost only, and calling somebody else needs three things that
localhost gives away for free. None of them are code; all three are worth knowing before
spending an evening on it.

**HTTPS, not optional.** `getUserMedia` and the service worker both require a secure
context. Browsers exempt `localhost`, so a call works on your own machine and the same
build over `http://192.168.1.20:5173` silently has no camera. Anybody else reaching this
needs a real certificate.

**A media address they can reach.** ICE candidates advertise the address the node sees on
its own interface, which is private. Set `SFU_PUBLIC_IP` to what the outside world sees
and forward the `SFU_UDP_PORT_MIN`–`MAX` range to it.

**A path for UDP.** This is where an HTTP tunnel — ngrok, `cloudflared` — will
disappoint: it carries the signalling perfectly, both people appear in the call, and no
media ever arrives, because the tunnel only carries TCP. `iceServers` in
`web/src/call.ts` is empty for the same reason it is honest to say so here: on one
network nothing needs STUN, and across the internet a node behind a symmetric NAT needs
TURN, which is a relay to deploy rather than a line to add.

Two arrangements that do work: a VPS with a public IP, a TLS-terminating proxy serving
`web/dist` and passing `/v1` to `api`, and the UDP range open; or a Tailscale tailnet,
where `tailscale serve` provides the certificate and media flows directly between
machines. Either way, set `ALLOWED_ORIGINS` — the WebSocket is same-origin by default,
and once the client is not served by the dev server's proxy it is a different origin.

`make help` lists every target. `make e2e` runs the browser suite against two api
nodes and one media node — see [web/README.md](./web/README.md) for what that proves
and why it takes more than one of each. `make capacity` measures how many concurrent
calls one media node carries, against a real `cmd/sfu` under a CPU limit.

## Reading order

If you are new to this repository, in this order:

1. **[Context map](./CONTEXT-MAP.md)** — the four bounded contexts and how they relate. Start here; the rest of the docs use its vocabulary precisely.
2. **[Architecture](./docs/architecture.md)** — containers, the durable and ephemeral paths, data ownership, layering.
3. **[Flows](./docs/flows.md)** — sequence diagrams for sending, reconnect sync, media processing and call setup.
4. **[Client sync contract](./docs/client-sync.md)** — what every client must do to not lose messages. Read before touching either client.
5. **[Requirements](./docs/requirements.md)** — functional and non-functional, written to be testable.
6. **[Plan](./docs/plan.md)** — eleven phases with verification criteria.
7. **[ADRs](./docs/adr/)** — why each hard-to-reverse decision was made, including what was rejected.

## Glossary

There is no single glossary file. Terminology lives with its context, because a term means something in one context and nothing in another:

- [Identity](./internal/identity/CONTEXT.md) — Account, Handle, Credential, Device, Session
- [Messaging](./internal/messaging/CONTEXT.md) — Conversation, Membership, Entry, Sequence Number, Cursor, Receipt
- [Media](./internal/media/CONTEXT.md) — Attachment, Variant, Ready
- [Calling](./internal/calling/CONTEXT.md) — Call, Participant, Ringing, Layer

Each glossary is opinionated: where several words exist for one concept, one is chosen and the rest are listed as terms to avoid. Code and documentation use the chosen word.

## Shape

Three binaries, split by resource profile rather than by bounded context ([ADR-0007](./docs/adr/0007-modular-monolith-split-by-resource-profile.md)):

| Binary | Responsibility |
|--------|----------------|
| `api` | HTTP and WebSocket surface for Identity, Messaging and Media |
| `worker` | Read-model projections and media processing |
| `sfu` | Call media forwarding |
| `cli` | Terminal client — messaging only ([why](./docs/adr/0006-custom-pion-sfu-with-simulcast.md)) |

Contexts are packages inside `api`. Each exposes exactly one package and hides its layers behind a nested `internal/`, so **the compiler refuses cross-context access to a model** — not a linter, not a review convention ([ADR-0011](./docs/adr/0011-nested-internal-fences.md)). `cmd/api` imports two packages and wires them; each context composes itself.

## Decisions worth knowing up front

These surprise people, so they are stated here rather than left to be discovered:

- **Messages are not end-to-end encrypted, but the server never reads text payloads anyway.** Consequently there is no server-side search — search is local to each client. Media *is* read server-side, deliberately. ([ADR-0001](./docs/adr/0001-content-opaque-server-e2ee-deferred.md))
- **Postgres is the system of record; Kafka is not the log.** Kafka carries consequences — projections, receipts, notifications, media work. Events are written to an outbox row in the same transaction as the change they describe, because publishing to a broker from a use case cannot be made atomic. ([ADR-0003](./docs/adr/0003-postgres-is-truth-kafka-carries-events.md))
- **The forward-only read cursor is enforced by the read model, not the aggregate.** Taking the greater of what is held and what arrives makes a stale, repeated or out-of-order advance all reach the same state — so MS-12 and idempotent redelivery are one mechanism. ([architecture](./docs/architecture.md#projections))
- **Kafka and Redis are both present on purpose.** Redis Pub/Sub delivers to sockets and is allowed to drop messages; clients recover by detecting gaps in sequence numbers. ([ADR-0005](./docs/adr/0005-redis-pubsub-for-socket-fanout.md))
- **Nothing in the log is ever modified.** Edits and deletes append revisions, because clients sync strictly forward and an in-place edit would be invisible to any client that had already passed it. ([ADR-0008](./docs/adr/0008-mutations-are-log-entries.md))
- **CQRS applies to Messaging only, and without event sourcing.** Elsewhere it would be ceremony.
- **There is no server-side search, and there never will be.** The server does not read message text, so search is a client feature: both clients hold a local SQLite store with the same schema and the same full-text query, and a test asserts they answer identically. ([client sync contract](./docs/client-sync.md))
- **The clients are trusted to detect their own missing messages.** The server does not track what each device holds; a client resumes from the highest sequence it has with no hole below it, and the server answers with what is missing. ([client sync contract](./docs/client-sync.md))
- **The SFU is ours, built on Pion.** The expensive choice, made because the media path is the part of this system most worth learning from. ([ADR-0006](./docs/adr/0006-custom-pion-sfu-with-simulcast.md))
