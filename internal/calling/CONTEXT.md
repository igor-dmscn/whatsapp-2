# Calling

Live audio and video sessions, and the forwarding of their media. Holds no access rules of its own — entitlement derives from Messaging.

## Language

**Call**:
A live audio or video session belonging to a conversation. Who may join follows from membership of that conversation.
_Avoid_: room, session, meeting, conference

**Participant**:
An account currently joined to a call. Distinct from a membership: everyone in the conversation may join, only some have.
_Avoid_: attendee, peer, caller

**Ringing**:
The state of a call that has been started and is awaiting its first other participant.
_Avoid_: pending, dialing, alerting

**Layer**:
One of several quality renditions a participant publishes, so that each receiver can be sent the best one its connection will carry.
_Avoid_: stream, quality, track, simulcast level

**Media node**:
The process forwarding one call's media. Named by address on every call, so a call says where its media is rather than assuming.
_Avoid_: SFU node, server, relay, worker

**Offer**:
A description of what one side of a connection will send and receive. Either side may make one — the media node offers whenever a call gains a publisher, because whoever was already there negotiated before that track existed.
_Avoid_: invite, request, renegotiation
