# Deployables are split by resource profile, not by bounded context

Three binaries ship: `api` (Identity, Messaging, and Media's HTTP and WebSocket surface), `sfu` (call media forwarding), and `worker` (media processing and projection building). Bounded contexts are logical boundaries enforced *inside* the `api` binary by import rules and linting — a boundary violation is a build failure, not a latency problem.

The split follows resource profiles because that is what actually differs: `api` is I/O-bound and holds long-lived WebSockets, `sfu` is CPU-bound and needs raw UDP port ranges, `worker` is CPU-bound and bursty. Those want different scaling policies, different nodes and different restart behaviour.

We rejected a service per context. It makes boundary violations physically impossible, but it converts cross-context reads into network calls with their own failure modes, replaces transactions with sagas, and multiplies local development, deployment and observability by four — for a system with one engineer. We rejected a single all-in-one binary because the SFU's CPU spikes would degrade messaging for users not on a call.

## Consequences

- Messaging and Identity cannot be scaled independently. Accepted; nothing at this scale needs it.
- Because the network is not enforcing boundaries, **linting must be**. Import restrictions between context packages are a build-time gate, not a code-review convention. Without them this decision quietly degrades into a single tangled package.
- Contexts can be extracted into services later if a real reason appears. Keeping cross-context references to IDs and events — never shared tables or direct struct access — is what preserves that option.
