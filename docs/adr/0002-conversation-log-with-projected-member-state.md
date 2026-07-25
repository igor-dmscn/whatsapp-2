# Conversation log is the source of truth; member state is projected

A message is written exactly once, to an append-only log owned by its conversation. Everything per-member — last-read cursor, unread count, delivery and read receipts — is a read model projected asynchronously from that log, not stored alongside the message.

We rejected per-recipient fan-out-on-write: it costs N writes per message, it cannot serve broadcast channels at all, and it makes history-before-join structurally impossible rather than a policy choice. We rejected computing member state on read: the conversation-list screen would become an aggregate query across every conversation a member belongs to, which is the first query to fall over under load.

## Consequences

- Write cost is independent of member count, so channels and groups use the same path.
- History visibility on join becomes a per-conversation-type policy knob, because the history exists regardless of when someone joined.
- **Unread counts and receipts are eventually consistent.** Clients must tolerate a window where a message is readable but its projections have not caught up. This is the price of the decision and must not be papered over with synchronous writes to the projection — that would reintroduce fan-out-on-write through the back door.
- This is where the system's asynchronous messaging infrastructure genuinely earns its place, rather than being decoration.
