# Redis Pub/Sub delivers to sockets; Kafka carries durable consequences

The system runs two brokers on purpose, along two clearly separated paths:

- **Durable path** — Postgres commit, then Kafka: projections, receipts, push notifications, media processing. Ordered, replayable, at-least-once.
- **Ephemeral path** — Redis Pub/Sub: getting a committed message to whichever node currently holds the recipient's WebSocket. Each node subscribes only to the accounts it holds.

Sticky sessions cannot solve this: a conversation's members hash to different nodes by definition, so no routing key co-locates them.

We rejected having every node consume all Kafka traffic and filter locally. It needs no new infrastructure and is stronger on delivery guarantees, but every node then does work proportional to total system traffic, so adding nodes cannot buy throughput. We rejected a presence registry with direct node-to-node gRPC forwarding as premature — it is the answer when Redis Pub/Sub is measurably the bottleneck, and it trades a managed dependency for stale-registry, node-death and split-brain failures we would own ourselves.

## Consequences

- Redis is not a new dependency here — presence, typing indicators and rate limiting already require it. This is a second use of one thing.
- **Redis Pub/Sub is at-most-once.** A message can be dropped on a connection blip, and that is accepted: correctness comes from the client noticing a gap in sequence numbers and refetching from Postgres. That reconciliation path is required for offline sync regardless, so it is not extra work — it is the same work, load-bearing in two places.
- Never move durable consequences onto the Redis path or real-time delivery onto the Kafka path. The separation is the decision; blurring it gets the worst of both.
