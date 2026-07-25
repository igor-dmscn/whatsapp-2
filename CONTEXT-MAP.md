# Context Map

Real-time messaging, media sharing, and live audio/video calling.

## Contexts

- [Identity](./internal/identity/CONTEXT.md) — who an account is, how it proves that, and which devices act for it
- [Messaging](./internal/messaging/CONTEXT.md) — conversations, who belongs to them, and the ordered log of what was said
- [Media](./internal/media/CONTEXT.md) — accepting photos and video, and processing them into servable variants
- [Calling](./internal/calling/CONTEXT.md) — live audio and video sessions, and the forwarding of their media

Messaging is deliberately the largest. It is the product; the others serve it.

**Notifications is not a context.** Push and email are a Kafka consumer that translates Messaging events into outbound alerts. It owns no vocabulary of its own.

## Relationships

- **Identity → all**: every other context references accounts by ID only. Identity is the sole owner of handles, credentials, devices and sessions; nothing else reads or writes them.
- **Identity → Messaging**: Identity raises `identity.device_revoked`, which Messaging consumes to close the revoked device's connection rather than letting it receive entries until its access token expires. Published to the log in phase 1 and over Kafka from phase 3, when the outbox exists.
- **Messaging → Media**: a message references attachments by ID. Media publishes readiness events that Messaging surfaces to clients — a message can exist while its attachment is still processing.
- **Calling → Messaging**: a call belongs to a conversation, and entitlement to join derives from membership. Calling reads membership by ID and holds no access rules of its own.
- **Messaging internally**: the only context using CQRS. The conversation log is the write model; cursors, receipts and unread counts are asynchronously projected read models. This is CQRS without event sourcing — the log is real rows, not a replayable event stream.
