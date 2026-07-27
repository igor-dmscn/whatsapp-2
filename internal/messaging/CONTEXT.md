# Messaging

Conversations, who belongs to them, and the ordered log of what was said. The only context using CQRS: the conversation log is the write model, and everything per-member is a projected read model.

## Language

### Conversations

**Conversation**:
An ordered log of messages plus the set of accounts entitled to read or write it. Exists in three kinds — Direct, Group and Channel — which differ only in their membership and writing rules.
_Avoid_: chat, room, thread

**Direct**:
A conversation between exactly two accounts, whose membership is fixed at creation and can never change.
_Avoid_: DM, 1:1, private chat

**Group**:
A conversation with mutable membership where every member may write.
_Avoid_: room, multi-chat

**Channel**:
A conversation where a small set of accounts may write and an unbounded set may only read.
_Avoid_: broadcast, feed, topic

**Membership**:
An account's participation in a conversation — the role it holds, and the point in the log from which it is entitled to read. New group memberships begin at the log's head, so nothing said before joining is visible; new channel memberships begin at the start, because a broadcast with no back catalogue is useless.
_Avoid_: participant, subscriber, member

### The log

**Entry**:
Anything occupying a position in a conversation's log. Every entry is either a message or a revision. Entries are never modified once written.
_Avoid_: record, item, row, event

**Message**:
An entry carrying a payload from its sender. May reference an earlier entry, which is what a reply is.
_Avoid_: post, text, chat

**Revision**:
An entry that amends or retracts an earlier message. Clients apply revisions over what they already hold; the amended original remains in the log.
_Avoid_: edit, delete, update, tombstone

**Envelope**:
The routing metadata of an entry — who wrote it, which conversation it belongs to, its position in the log, and its delivery state. The only part of an entry the server interprets.
_Avoid_: header, metadata

**Payload**:
A message's content. Opaque to the server — never parsed, never indexed, never projected.
_Avoid_: body, content, data

**Sequence Number**:
An entry's position in its conversation's log. Monotonic and gapless within a conversation, and meaningless across conversations. A client that holds up to one number and learns of a later one knows exactly what it is missing.
_Avoid_: offset, index, position, version

**Reaction**:
A sender's lightweight response attached to a message. Deliberately not an entry — reactions carry no position in the log and sync by their own path.
_Avoid_: emoji, like, response

### Delivery

**Cursor**:
The sequence number up to which a membership has read. Belongs to the membership, not the device — reading anywhere is reading everywhere.
_Avoid_: read marker, watermark, last-seen

**Receipt**:
An acknowledgement that a message reached or was read by a particular membership.
_Avoid_: ack, tick, read state

**Delivery State**:
How far a message has got for one membership: *Sent* once it is durably in the log, *Delivered* once any of that account's devices acknowledged it, *Read* once the cursor passed it.
_Avoid_: status, ticks, ack level

**Mark**:
A high-water mark: the position through which something is true. A read mark and a delivery mark per membership, each moving forward only. Two of them answer the delivery-state question for every entry in a conversation, which is why no state is stored per entry.
_Avoid_: pointer, offset (offset is Kafka's word and means something else here)

**Revision**:
The umbrella term for an entry that amends an earlier one. Two concrete kinds: a *revision* replaces the target's content, a *retraction* withdraws it. Both take their own position in the log and reference the original, never each other.
_Avoid_: edit, update, tombstone, soft delete

**Retraction**:
Delete for everyone. Carries no payload at all, so no client can render withdrawn content by misreading a kind. Terminal — the target cannot be edited afterwards.
_Avoid_: delete, remove, redaction

**Reaction**:
Somebody's symbol on an entry, held beside the log rather than in it. Has no sequence number, so it moves no cursor, fills no gap and wakes no client with work to do.
_Avoid_: like, emoji (the emoji is the symbol; the reaction is the fact somebody applied it)

### Presence

Nothing in this section is ever persisted. Every term names something true for seconds and worthless once stale.

**Claim**:
One device's assertion that it is connected, made by the node holding its socket and true only while that node keeps renewing it. Per device rather than per account, so one tab closing does not withdraw another's — and expiring rather than deleted, so a node that dies takes its claims with it and leaves nothing to clean up.
_Avoid_: session, connection record, online flag

**Online**:
An account with at least one unexpired claim. A derived answer, never stored: asked about a whole screen of accounts at once, never asserted about one.
_Avoid_: active, present, available

**Typing Claim**:
An account's assertion that it is composing in a conversation. Per account rather than per device, like a cursor — which device somebody typed on is nobody's business. The weakest thing the server carries: lost claims show nothing and clear themselves.
_Avoid_: typing indicator, composing state

**Invite**:
A shareable, revocable permission to join a conversation at a fixed role. Counts its uses rather than being consumed by the first, so a link shared with many people is the ordinary case and single-use is a limit of one.
_Avoid_: link, join code, token (the token is the secret *inside* an invite, not the invite)

**Join Point**:
The position a membership's visibility starts from — the whole of the history policy in one number. Groups join at the head, channels at the first entry.
_Avoid_: visibility start, since, from-sequence

**Member State**:
The projection holding a membership's marks and unread count. Eventually consistent by design — a read model built from the log, never written by the request that causes it (ADR-0002).
_Avoid_: read state, membership state, counters
