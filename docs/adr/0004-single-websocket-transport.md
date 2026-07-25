# One WebSocket per device carries all real-time traffic

Each connected device holds a single WebSocket that carries inbound messages, receipts, typing, presence, and WebRTC signalling. There is one reconnection state machine, one authentication handshake, and one place where resume-from-sequence-number lives.

We considered Server-Sent Events and deliberately rejected it for v1. Live audio and video calling requires a bidirectional low-latency channel regardless, so a WebSocket is not optional — and once one exists, SSE adds a second reconnection path, a second auth path, and a second delivery mechanism while earning nothing.

## The case for revisiting

There is one genuine fit, recorded so it is not rediscovered from scratch: **read-only channel subscribers never write.** SSE's native `Last-Event-ID` resume maps directly onto conversation sequence numbers, ordinary HTTP middleware applies without being reimplemented inside a socket router, and per-node subscriber capacity is materially higher. If channel subscriber counts ever become the binding constraint, this is the change to make — and it is additive, not a migration. Until then, a second transport is unjustified.
