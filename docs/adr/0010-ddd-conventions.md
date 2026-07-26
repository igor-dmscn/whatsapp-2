# Domain-Driven Design conventions

The tactical patterns every context follows. Recorded because the first draft of Identity broke most of them while looking correct — DDD-shaped directories around procedural code — and because a convention only half the contexts follow is worse than none.

## Aggregates own their state

**Fields are unexported.** Access is through methods. An aggregate whose fields can be assigned from outside cannot promise anything about its own state, and every invariant becomes a suggestion.

**Behaviour is named for what happens, not for what is set.** `device.Revoke(now)`, not `device.SetRevokedAt(t)`. A setter moves a decision out of the aggregate and into whoever called it, which is how the same rule ends up implemented differently in three places.

**Construction is validation.** `RegisterAccount` returns an error or a valid aggregate; there is no third state. A separate `Validate()` method invites code that forgets to call it.

**Loading is not constructing.** Repositories use `Reconstitute*` functions, which re-run no validation and raise no events. Re-validating on load would lock people out when a rule is tightened; raising events on load would tell consumers something happened when nothing did.

**Collections are returned as copies.** `Account.Credentials()` copies, so a caller cannot reach past the aggregate and empty it.

**Secrets do not print.** Types holding material or passphrases implement `String` *and* `GoString`, because `%v` and `%#v` take different paths and a secret surviving one of them is a secret in the logs.

## Aggregate boundaries follow lifecycle

An entity belongs inside an aggregate when it has no meaning or lifetime apart from it. A Credential is inside Account. A Device is its own root, because revoking a lost phone should not require loading the account and all its credentials.

**One repository type per root**, each loading and saving a whole aggregate. Discovered by the compiler: three ports each declaring `Save` cannot be implemented by one type, and rather than renaming methods to `SaveDevice` to work around it, the shape was wrong. There is no `CredentialRepository` — saving a credential alone would put the one-password rule somewhere the aggregate cannot see it.

**Sometimes the boundary is the invariant.** Access and refresh tokens are one Session aggregate rather than two token rows precisely so that rotation is one state transition saved as one update. Modelled separately, rotation is a delete plus two inserts and a crash between them destroys the session. That bug existed in the first draft.

## Domain events

**Aggregates record events as a side effect of behaviour.** Callers never construct them. An aggregate that changed state and recorded nothing has hidden that change from the rest of the system.

**A rejected change records nothing.** `AddCredential` returning an error records no event; revoking an already-revoked device records no second one. Consumers must never react to something that did not happen.

**`TakeEvents` drains rather than reads.** Whoever takes them owns publishing them exactly once; a second caller receiving the same events would publish twice.

**Events carry identifiers and facts, never secrets.** `CredentialAdded` names the kind, not the material — an event travels beyond the aggregate that raised it.

**Event names are wire names.** `identity.device_revoked` is stable across Go refactors, because consumers and stored outbox rows depend on it.

## Domain services

A domain service is for an operation that is part of the model but belongs to no single aggregate. It is not a home for logic that was awkward to place.

Warranted so far:

- `Hasher` — producing credential material is what a password credential *is*, but it cannot be expressed without a cryptographic library, so the domain declares the operation and infrastructure supplies it.
- `AuthoriseMembershipChange` — whether an actor may change who belongs depends on the conversation's kind *and* the actor's role. Conversation and Membership are separate roots that cannot see each other, so on either one the rule would be written twice with two chances to disagree.

Coming: bandwidth-to-layer selection is pure policy no aggregate owns (phase 9).

**A domain service is a function unless it needs state.** `AuthoriseMembershipChange` was expected to be a type and landed as a package-level function: Go's packages already namespace it, `domain.AuthoriseMembershipChange` reads no worse than a method, and a struct with one method and nothing to construct is ceremony. A service becomes a type when it has a dependency to inject, which is what `Hasher` has and this does not.

Not warranted: anything that would be a struct wrapping one method over one aggregate. That is the aggregate's own behaviour, misplaced.

## The application layer owns no rules

It owns the clock, identifier generation, transaction boundaries, and publishing recorded events. Nothing else.

**A rule appearing in application is a sign the aggregate it belongs to is anemic.** The passphrase length policy sat here in the first draft and was moved into a `Passphrase` value object — what makes a credential acceptable is a fact about the model, not about orchestration.

**Value objects at the boundary, not raw strings.** `Hasher.Hash` takes a `Passphrase`, so an unvalidated secret cannot reach storage down a path that forgot to check. But `Hasher.Verify` takes a plain string, deliberately: raising the minimum length must not lock out accounts whose credential was acceptable when they created it.

## Packages are named for their context

See [ADR-0011](./0011-nested-internal-fences.md). Briefly: a context exposes
one package and hides its layers behind a nested `internal/`, so the compiler — not a
convention — stops anything outside from depending on its model, and the layer
packages can keep short names like `domain` and `app` without colliding.

## Invariants are enforced twice where concurrency can defeat them

The aggregate enforces the one-password rule and gives a useful error. A partial unique index enforces it against two concurrent requests, which the aggregate cannot see. Both are needed, and neither is redundant.
