# Edits and deletes are new log entries; reactions are not in the log

An edit or a delete-for-everyone appends a **revision** to the conversation log, with its own sequence number, referencing the entry it amends or retracts. Clients apply revisions over what they already hold. Nothing in the log is ever mutated in place.

This is forced by the sync protocol, not chosen for purity. Clients reconcile by comparing local high-water marks against the server's head and requesting only the gap — they sync strictly forward. Editing entry 41 in place leaves its sequence number unchanged, so every client that already synced past 41 would never learn the edit happened. In-place mutation is silently broken by our own sync design, and no amount of cache invalidation fixes it.

**Reactions are deliberately excluded from the log.** They are high-churn: a popular message in a large group would burn a sequence number per tap and wake every connected client each time. Reactions are separate state with their own lightweight sync path. Replies need no mechanism at all — they are a reference field on an entry.

## Consequences

- The log's element is an **Entry**, not a Message. A message is one kind of entry; a revision is another. Clients interpret entries rather than rendering them one-to-one.
- History is auditable by construction — the original text of an edited message is still in the log. If a product requirement ever demands true erasure (a deletion request, say), that is a separate operation that rewrites history, and it will need its own ADR.
- There are two sync paths: the sequenced entry log, and reaction state. Keep the second one lightweight; the moment it grows ordering or history requirements, it belongs in the log after all.
