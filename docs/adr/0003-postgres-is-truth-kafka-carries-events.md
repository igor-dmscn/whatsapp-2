# Postgres is the system of record; Kafka carries events

Messages are committed to Postgres with a server-assigned, gapless per-conversation sequence number. A domain event is then published to Kafka, which drives projections, receipts, push notifications and media processing. Kafka is a transport for consequences, not the log itself.

We explicitly rejected making Kafka the log with offsets as sequence numbers. Reading one conversation's history would mean replaying a partition shared with thousands of unrelated conversations, so a Postgres projection would be needed anyway — leaving Kafka as an expensive write-ahead log rather than a read path. Retention would become a data-loss policy, edits and deletes would become tombstone gymnastics, and partition count would become a permanent capacity decision made on day one.

## Consequences

- Clients get a total order per conversation **and gap detection**: a client holding up to sequence 40 that is told the head is 42 knows precisely to request 41. This is what makes offline sync tractable, and it is the main thing ULID/timestamp ordering would have cost us.
- Message writes serialise per conversation. Accepted: no real conversation approaches the throughput where this matters.
- Publishing to Kafka after a Postgres commit is a dual-write. It must be solved with a transactional outbox — the event row is written in the same transaction as the message, and a relay publishes it. Publishing directly from request-handling code is a correctness bug, not a shortcut.
- Every message carries a client-generated identifier with a uniqueness constraint, so an at-least-once client retry over a flaky network is a no-op rather than a duplicate.
