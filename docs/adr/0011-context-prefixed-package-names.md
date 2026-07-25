# Each context hides its layers behind its own internal/ fence

A context exposes exactly one package. Everything else lives under a nested
`internal/`, which the Go compiler refuses to let anything outside that context
import — including `cmd/`:

```
internal/messaging/
    messaging.go              package messaging    ← the only public surface
    internal/
        domain/               package domain       ← model, standard library only
        app/                  package app          ← use cases, clock, identifiers, events
        postgres/             package postgres
        broadcast/            package broadcast
        api/                  package api          ← HTTP, WebSocket, hub
```

`cmd/api` imports two packages, `identity` and `messaging`, and nothing else from
either context.

## Why not layer-named packages at the top level

The first layout used `domain`, `application`, `infrastructure` and `transport`
directly under each context. That gives four packages called `domain` and four
called `transport`, so `cmd/api` — the one place that legitimately knows about
several contexts — had to alias every import:

```go
identityhttp  "comms/internal/identity/transport"
messaginghttp "comms/internal/messaging/transport"
```

The alias was doing work the package name should have done. Go's
[package names guidance](https://go.dev/blog/package-names) and Google's Go style
decisions single out generic names — `util`, `common`, `types`, `api` — as
collision-prone and uninformative, and `domain`/`application`/`infrastructure` are
the Go-DDD version of the same mistake.

## Options considered

**Prefix every package with its context** — `identityapp`, `messagingpg`. This is
the convention [ardanlabs/service](https://github.com/ardanlabs/service) uses
(`userbus`, `userapp`, `userdb`), and it does remove every alias. We ran the
codebase this way before settling here. Rejected because the names get long
(`messagingredis`, `identitycrypto`), paths stutter (`identity/identityapp`), and
cross-context isolation still depends on a test we wrote rather than on the
language.

**Ardan Labs' layer-first grouping** — `business/domain/identitybus`,
`app/domain/identityapp`, `stores/identitydb`. Strong precedent and no aliases.
Rejected because a context's code then spans three or four top-level trees, which
works against the bounded-context grouping that the per-context `CONTEXT.md` files
and ADR-0007 depend on: "show me everything Messaging does" should be one directory.

**Ben Johnson's [standard package layout](https://www.gobeyond.dev/standard-package-layout/)** —
model in the root package, subpackages named for the dependency they wrap
(`postgres.ConversationRepository` reads beautifully). Rejected because it assumes
one domain per module: with four bounded contexts there are two packages called
`postgres` and two called `http`, so the aliases return — the same problem, moved.

## Consequences

- **The boundary is a build error, not a convention.** Verified: importing
  `internal/messaging/internal/domain` from `cmd/api` or from `internal/identity`
  fails to compile. This is stronger than any linter, and it is the reason the
  layer packages can keep short conventional names — two contexts may both have a
  package called `domain` because no single file is permitted to import both.
- **Generic names are safe here specifically because they are unreachable.**
  `app`, `api`, `domain` would be poor names for a published package; behind an
  internal fence they are unambiguous, since the only files that can see them are
  already inside the context.
- **Each context owns its composition root.** `cmd/api` cannot wire a repository it
  cannot name, so `New()` lives inside the context. That is where it belongs: the
  wiring is a fact about the context, and `cmd/api` is left choosing which contexts
  exist and how they are joined.
- **Facades speak in primitives.** `identity.Module.Authenticate` returns plain
  strings so that Messaging can declare its own `Authenticator` port without naming
  an Identity type. A shared type would be a shared dependency, which is the thing
  the boundary exists to prevent.
- One rule the compiler still cannot express: a context importing another context's
  *public* facade compiles fine and is still wrong. The architecture test keeps that
  one, and only that one, plus the model's standard-library-only rule.
- Paths are deeper — `internal/messaging/internal/api` rather than
  `internal/messaging/messagingapi`. Accepted; the depth is what buys the
  enforcement.
