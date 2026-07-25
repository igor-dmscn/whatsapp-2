# Packages are named for the context, not for the layer

A context's model lives in files directly inside its directory, and its adapters live in subdirectories whose names begin with the context:

```
internal/messaging/                  package messaging        — aggregates, ports, domain services
internal/messaging/messagingapp/     package messagingapp     — use cases, identifiers, event publishing
internal/messaging/messagingpg/      package messagingpg      — Postgres repositories
internal/messaging/messagingredis/   package messagingredis   — broadcast publisher
internal/messaging/messagingapi/     package messagingapi     — HTTP, WebSocket, hub
```

We started with `domain`, `application`, `infrastructure` and `transport` under each context, which is the conventional Go-DDD layout. It produced four packages named `domain` and four named `transport`, so `cmd/api` — the one place that legitimately knows about several contexts — had to alias every import:

```go
identityhttp  "comms/internal/identity/transport"
messaginghttp "comms/internal/messaging/transport"
```

The alias was doing the work the package name should have been doing. Go's [package names guidance](https://go.dev/blog/package-names) and Google's Go style decisions both single out generic names — `util`, `common`, `types`, `api` — as collision-prone and uninformative, and `domain`/`application`/`infrastructure` are the Go-DDD version of the same mistake. Every package name is now unique across the repository, so **no import anywhere needs an alias**, and a symbol carries its origin at the call site: `identitypg.NewAccountRepository`, `messagingredis.NewRedisBroadcaster`.

## Consequences

- **Depth replaces naming as the layering rule.** A file at a context's root is the model; a file below it is an adapter. The architecture test enforces the model's stdlib-only rule by path depth rather than by a directory called `domain`, which is a smaller rule with the same teeth.
- Import paths repeat the context, as in `identity/identityapp`. Accepted: Go's stutter guidance is about `identity.IdentityService` at the call site, not about path segments, and the alternative was an alias in every composition root forever.
- Adapters are named for what they provide rather than for being adapters. Postgres repositories and the Argon2 hasher are separate packages because "Postgres" and "cryptography" are different answers to "what is this?", where `infrastructure` was no answer at all.
- Identifier generation and event publishing live in the `*app` packages, which already own the clock, transaction boundaries and event dispatch. They are the application's concerns, not a technology.
- A new context adds one directory and four subdirectories, all self-naming. A new *layer* in an existing context is a new prefixed package — never a bare `service` or `handlers`.
