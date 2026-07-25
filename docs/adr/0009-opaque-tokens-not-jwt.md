# Session tokens are opaque, not JWTs

Access and refresh tokens are 256 bits of random data. The server stores only a SHA-256 digest and resolves a presented token by looking it up. There is no signed, self-describing token anywhere in the system.

This is driven by ID-4: revoking a device must invalidate its tokens within 30 seconds. A JWT is valid until it expires, so meeting that requirement with JWTs means checking every request against a revocation list — which is the database lookup that JWTs are adopted to avoid. Having paid for the lookup, the signature buys nothing.

What the choice also removes: signing key management and rotation, algorithm-confusion vulnerabilities, clock-skew handling between issuer and verifier, and the recurring temptation to put authorisation claims in a token where they go stale the moment permissions change.

## Consequences

- Every authenticated request costs one indexed primary-key lookup for the token and one for the device. Cheap, and cacheable in Redis if it ever measures as hot — the correctness argument does not depend on where the lookup is served from.
- **Revocation is checked on the device, not inferred from token deletion.** Deleting a revoked device's tokens is an optimisation; the authentication path checks the device's revoked state on every request, so ID-4 holds even when that deletion fails or races.
- Refresh tokens rotate on use, and the presented token is consumed before replacements are issued. A stolen refresh token is therefore usable at most once, and its use is detectable — the legitimate client's next refresh fails, which is an event rather than a silent thirty-day compromise.
- Tokens are digested with plain SHA-256 rather than Argon2. The input is uniformly random, so there is no dictionary to attack and nothing a slow hash would defend against; using one would add latency to every authenticated request for no gain.
- Nothing outside Identity can validate a token independently. Accepted: contexts are packages in one binary, so they call Identity directly rather than parsing a token themselves.
