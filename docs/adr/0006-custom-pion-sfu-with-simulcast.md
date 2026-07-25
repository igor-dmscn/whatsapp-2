# Call media runs on our own Pion SFU, with simulcast

Media for both direct and group calls is forwarded by a Selective Forwarding Unit we build on Pion. Senders publish multiple quality layers and the SFU selects a layer per receiver based on estimated available bandwidth. Signalling rides the existing WebSocket and is domain code: who may join, ringing, who is present, what happens when the last participant leaves.

We rejected self-hosted LiveKit, which is production-grade, Go, Pion-based, and would have taken roughly none of this work. The trade was made deliberately: the codebase is itself a deliverable — a reference implementation meant to be read and learned from — and outsourcing the media path would remove the part with the most teaching value. Peer-to-peer was rejected because mesh collapses at about four participants, each peer having to encode N−1 streams.

## Accepted costs

This is the largest work item in the project and it competes directly with the messaging domain. Simulcast specifically requires receiver bandwidth estimation, keyframe coordination on layer switches, and handling of layer starvation. Getting these subtly wrong does not fail loudly — it makes calls feel bad. If schedule pressure arrives, the correct retreat is single-layer forwarding first (Opus and VP8, with NACK and PLI handling, without which a single lost packet freezes video indefinitely), adding layer selection afterwards. That retreat is additive, not a rewrite.

## Consequences

- **A forwarding SFU cannot transcode.** All participants must agree codecs up front: Opus for audio, VP8 as the cross-browser video floor.
- **coturn is not needed.** An SFU on a public IP is already a relay; with ICE-lite and a TCP/TLS-443 fallback for restrictive firewalls, a separate TURN server drops out of the architecture.
- A call is capped by a single machine, and all its participants must route to the same node. Multi-node cascading is explicitly out of scope — it is where SFU design becomes genuinely hard, and it cannot be tested meaningfully without geographic spread.
