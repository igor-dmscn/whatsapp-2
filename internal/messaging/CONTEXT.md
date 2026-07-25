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
