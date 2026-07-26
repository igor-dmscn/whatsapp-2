# Engineering Requirements

Requirements are stated so they can be tested. Anything here that cannot be checked by a test or a measurement is a wish, not a requirement, and should be deleted.

Terminology is defined in the [context map](../CONTEXT-MAP.md) and the per-context glossaries. Words like *entry*, *cursor* and *membership* are used here in exactly those senses.

## Functional

### Identity

| ID | Requirement |
|----|-------------|
| ID-1 | An account is created with a globally unique handle and an email address used only for recovery. |
| ID-2 | An account may hold multiple credentials of different kinds. Adding a passkey later must not require altering existing credentials. |
| ID-3 | An account may have many devices connected simultaneously. Each device authenticates independently and can be revoked independently. |
| ID-4 | Revoking a device terminates its connection and invalidates its tokens within 30 seconds. |
| ID-5 | Accounts are discoverable by exact handle. Partial-match search is rate limited per account. |

### Messaging

| ID | Requirement |
|----|-------------|
| MS-1 | Every entry receives a sequence number that is monotonic and gapless within its conversation. |
| MS-2 | Sending is idempotent on a client-supplied identifier. A retried send returns the original entry rather than creating a second one. |
| MS-3 | A client holding entries up to sequence *n* can request everything after *n* and receive it in order. |
| MS-4 | A direct conversation has exactly two memberships, fixed at creation. Attempting to add a third is rejected. |
| MS-5 | A new group membership begins at the log head. Entries before that point are not readable by it. |
| MS-6 | A new channel membership begins at sequence 1 — the full back catalogue is readable. |
| MS-7 | Only accounts with the writer role may append to a channel. All members may append to a group. |
| MS-8 | An edit or delete appends a revision referencing the target entry. No entry is ever modified in place. |
| MS-9 | A revision from an account other than the original author is rejected, except for deletes performed by a group admin. |
| MS-10 | Reactions carry no sequence number and do not appear in the log. |
| MS-11 | A cursor belongs to a membership, not a device. Advancing it on one device advances it for the account. |
| MS-12 | A cursor only ever moves forward. An out-of-order advance is ignored, not applied. |
| MS-13 | Delivery state is observable as Sent, Delivered or Read, per membership. |

### Media

| ID | Requirement |
|----|-------------|
| MD-1 | An attachment can be referenced by an entry before its variants exist. Clients render it as pending. |
| MD-2 | Variant generation is idempotent. Reprocessing the same attachment produces the same variants without duplicates. |
| MD-3 | A worker crash mid-processing must not lose the job or leave an attachment permanently pending. |
| MD-4 | Attachments are capped at 100 MB. Rejection happens before the body is fully read. |
| MD-5 | Original uploads are retained. Variants are derived and may be regenerated at any time. |

### Calling

| ID | Requirement |
|----|-------------|
| CL-1 | Entitlement to join a call derives solely from membership of its conversation. Calling holds no access rules. |
| CL-2 | A conversation has at most one active call. Starting a second returns the existing one. |
| CL-3 | A call ends when its last participant leaves. |
| CL-4 | All participants in a call connect to the same SFU node. |
| CL-5 | Participants publish multiple quality layers; the SFU selects per receiver. Done: a receiver reporting 150 kbit/s is sent 73 while another on the same publisher is sent 591, and it returns to 591 when it reports recovery. Selection runs on a tick rather than only on feedback, because a client that reports nothing must not be stuck on whichever layer arrived first. |
| CL-6 | Lost packets are recovered by retransmission, and decoder desync by keyframe request. Neither may leave video frozen for more than 2 seconds. |

## Non-functional

### Latency

| ID | Requirement |
|----|-------------|
| NF-1 | Send to receipt on a connected recipient's device: p95 under 300 ms, same region. |
| NF-2 | Gap sync of 1 000 entries: p95 under 1 second. |
| NF-3 | Call join to first media: p95 under 2 seconds. Measured at **109 ms** — and "first media" means the first keyframe, because packets arriving are not a picture. |
| NF-4 | One-way audio latency through the SFU: p95 under 200 ms, same region. Measured at **431 µs** on loopback, which is the code's share of the budget and not a deployment's. |
| NF-5 | Cold client start with a populated local store renders the conversation list in under 500 ms, without network. |

### Consistency

| ID | Requirement |
|----|-------------|
| NF-6 | An acknowledged send is durable. No acknowledged entry may be lost, in any failure mode. |
| NF-7 | Unread counts and receipts are eventually consistent, converging within 2 seconds under normal load. Clients must render correctly during the window where an entry exists and its projections do not. |
| NF-8 | Event consumers are idempotent. Redelivery must not double-count an unread badge or duplicate a receipt. |
| NF-9 | Real-time socket delivery is at-most-once. Correctness comes from client gap detection, never from assuming a push arrived. |

### Scale

Designed for the first figure, deployed at the second. Both are stated because pretending otherwise is how architecture becomes theatre.

| ID | Requirement |
|----|-------------|
| NF-10 | Designed for 100 000 accounts, 10 000 concurrent sockets per `api` node, and channels with 50 000 members. |
| NF-11 | Actually deployed for fewer than 100 accounts. Load must therefore be demonstrated by a load-test harness, not by production traffic. |
| NF-12 | Write cost per entry is independent of member count. A channel broadcast to 50 000 members performs the same number of writes as a direct message. |
| NF-13 | Groups are capped at 256 members. Channels are uncapped for readers. |
| NF-14 | One SFU node supports at least 3 concurrent 4-participant video calls on 4 vCPUs. Multi-node cascading is out of scope. Measured with `scripts/capacity.sh`: **16 calls and 64 participants** on 4 vCPUs with no loss, so the stated limit has room rather than being the ceiling. |

### Operability

| ID | Requirement |
|----|-------------|
| NF-15 | The whole system starts locally with one command and no cloud dependencies. |
| NF-16 | Every request and every event carries a correlation identifier through to logs and traces. |
| NF-17 | A cross-context import violation fails the build. Boundaries are not a code-review convention. |
| NF-18 | Schema changes are versioned migrations, applied forward, never edited after merge. |

## Explicitly out of scope

Recorded so these are decisions rather than omissions:

- End-to-end encryption — see [ADR-0001](./adr/0001-content-opaque-server-e2ee-deferred.md).
- Server-side message search, link previews, content moderation. Search is local to each client.
- Server-Sent Events as a second transport — see [ADR-0004](./adr/0004-single-websocket-transport.md).
- Multi-node SFU cascading, call recording — see [ADR-0006](./adr/0006-custom-pion-sfu-with-simulcast.md).
- Calling from the CLI. The CLI is a messaging client; a headless harness covers SFU testing.
- Phone-number identity, contact-book discovery, SMS verification.
- Voice messages, disappearing messages, stories, message forwarding, threads.
