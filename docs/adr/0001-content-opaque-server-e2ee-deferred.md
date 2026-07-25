# Content-opaque server, E2EE deferred

Messages are protected by TLS in transit and encryption at rest, but are **not** end-to-end encrypted in v1. To keep E2EE a later addition rather than a redesign, the server treats every message payload as an opaque blob and routes purely on envelope metadata (sender, conversation, ordering, delivery state).

We considered full Signal-protocol E2EE up front and rejected it for now: the crypto cascade (multi-device sessions, group sender keys, lost-device recovery, no server-side history) would have consumed the whole timeline before the first photo was sent. We also rejected a plaintext-readable server, which would have closed the E2EE door by making search, previews and content read models depend on plaintext.

## Deliberate exception: media

Media payloads **are** readable by the server. Attachments go through a server-side pipeline for thumbnails, size variants and format normalisation. We accepted this knowingly: it keeps weak clients viable (browser-side video transcoding is expensive and fragile), and it gives the system one honest asynchronous workload.

The cost is explicit — adding E2EE later means dismantling the media pipeline and rebuilding it in every client. Text messaging would migrate cleanly; media would not. E2EE is therefore a *smaller* change than a rewrite, but not a free one.

## Consequences

- No server-side full-text search, link unfurling, or content moderation over text. Search is a client-side concern over a local database.
- Read models and projections may only ever be built over envelope metadata and media-derived metadata — never over text payload content.
- For text, adding E2EE later should be a client-side change plus a key-distribution service, not a change to routing, storage, or read models. Any feature that would break this promise needs a new ADR overturning this one.
