# Communication Platform

Real-time messaging, media sharing, and live audio/video calling. A Go backend with React and CLI clients, built as a reference implementation — the codebase is meant to be read, so the reasoning is committed alongside the code.

**Status: design complete, implementation not started.** See [the plan](./docs/plan.md).

## Reading order

If you are new to this repository, in this order:

1. **[Context map](./CONTEXT-MAP.md)** — the four bounded contexts and how they relate. Start here; the rest of the docs use its vocabulary precisely.
2. **[Architecture](./docs/architecture.md)** — containers, the durable and ephemeral paths, data ownership, layering.
3. **[Flows](./docs/flows.md)** — sequence diagrams for sending, reconnect sync, media processing and call setup.
4. **[Requirements](./docs/requirements.md)** — functional and non-functional, written to be testable.
5. **[Plan](./docs/plan.md)** — eleven phases with verification criteria.
6. **[ADRs](./docs/adr/)** — why each hard-to-reverse decision was made, including what was rejected.

## Glossary

There is no single glossary file. Terminology lives with its context, because a term means something in one context and nothing in another:

- [Identity](./internal/identity/CONTEXT.md) — Account, Handle, Credential, Device
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

Contexts are packages inside `api`, with boundaries enforced by import linting — a violation is a build failure, not a code-review note.

## Decisions worth knowing up front

These surprise people, so they are stated here rather than left to be discovered:

- **Messages are not end-to-end encrypted, but the server never reads text payloads anyway.** Consequently there is no server-side search — search is local to each client. Media *is* read server-side, deliberately. ([ADR-0001](./docs/adr/0001-content-opaque-server-e2ee-deferred.md))
- **Postgres is the system of record; Kafka is not the log.** Kafka carries consequences — projections, receipts, notifications, media work. ([ADR-0003](./docs/adr/0003-postgres-is-truth-kafka-carries-events.md))
- **Kafka and Redis are both present on purpose.** Redis Pub/Sub delivers to sockets and is allowed to drop messages; clients recover by detecting gaps in sequence numbers. ([ADR-0005](./docs/adr/0005-redis-pubsub-for-socket-fanout.md))
- **Nothing in the log is ever modified.** Edits and deletes append revisions, because clients sync strictly forward and an in-place edit would be invisible to any client that had already passed it. ([ADR-0008](./docs/adr/0008-mutations-are-log-entries.md))
- **CQRS applies to Messaging only, and without event sourcing.** Elsewhere it would be ceremony.
- **The SFU is ours, built on Pion.** The expensive choice, made because the media path is the part of this system most worth learning from. ([ADR-0006](./docs/adr/0006-custom-pion-sfu-with-simulcast.md))
