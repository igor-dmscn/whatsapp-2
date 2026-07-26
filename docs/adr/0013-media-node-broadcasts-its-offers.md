# A media node broadcasts its offers to every api node

The `sfu` process produces WebRTC offers of its own. When a call gains a publisher, everyone already in it negotiated before that track existed and has to be re-offered ([ADR-0006](./0006-custom-pion-sfu-with-simulcast.md)) — so an offer has to travel from a process that holds no client connections to one particular client's socket, which is held by one particular `api` node.

Every `api` node opens one long-lived stream to the media node and receives **every** offer it produces. A node delivers the offers it has a socket for and drops the rest.

We rejected a registry mapping devices to `api` nodes. It targets the delivery, and it is a second source of truth about where a client is — one that is wrong for as long as it takes a reconnection to be noticed, in exactly the situation where being wrong loses an offer. We rejected having `api` nodes poll, because an offer is on the path to first media and NF-3 gives that two seconds in total. We rejected Redis Pub/Sub, which is already in the stack for socket fanout ([ADR-0005](./0005-redis-pubsub-for-socket-fanout.md)) and would have worked: the direct stream keeps the dependency arrow pointing the way the deployment already does — `api` nodes know the media node's address because they signal to it, and the media node needs to know nothing about them.

## Consequences

- Cost is one copy of each offer per `api` node, a few kilobytes on a join. At a node count where that is the wrong trade, a registry is the upgrade and this is the thing it replaces.
- **An offer with no subscriber is retried, not dropped.** No `api` node is listening while the media node starts and while a stream reconnects, and without a retry a participant misses every joiner for the rest of the call — silently, which is what makes it worth the code. Delivery therefore reports failure, and `Renegotiator` returns an error.
- **`offer_subscribers` is a health signal, not a statistic.** Zero, with calls above zero, is a node forwarding media that cannot tell anybody about a new publisher. Everything else about the process looks healthy, and the symptom is one-way media — which phase 9 has already spent an hour misdiagnosing once.
- An `api` node sees offers for participants it does not hold, so `Offer` returning "not mine" is the ordinary case rather than an error worth reporting.
- Nothing above the port changed. `MediaNodes` already named a node by address on every call, so this is one more implementation of it plus a stream ([ADR-0007](./0007-modular-monolith-split-by-resource-profile.md)).
