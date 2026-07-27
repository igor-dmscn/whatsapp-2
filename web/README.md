# Browser client

A Vite single-page app ([ADR-0012](../docs/adr/0012-vite-spa-for-the-browser-client.md)). It grows one phase at a time along with the backend, so at any commit it does exactly what the server can support and no more.

**What exists now:** sign in, direct conversations, groups and channels, the conversation list with unread badges, replies, reactions, edits and deletes, photo and video sharing, presence and typing, local persistence with offline search, a service worker for an offline cold start, and video calls.

## Running it

```sh
make all    # from the repository root: dependencies, api, worker and this, together
```

Or `make web` alone, against an api already running on :8080.

The dev server proxies `/v1` to the api rather than pointing the client at port 8080 directly, so the browser talks to one origin in development exactly as it will in production. Nothing has to be configured to permit it, and no CORS policy exists to drift out of step.

## Layout

| File | What it is |
|------|------------|
| `src/api.ts` | HTTP calls and the token pair. Refreshes before expiry rather than after a 401. |
| `src/sync.ts` | The socket protocol: connect, resume, detect gaps, fill them. No React. |
| `src/App.tsx` | The screens. |
| `src/sync.test.ts` | The protocol against a fake socket. Fast, hermetic, always runs. |
| `src/browser.test.ts` | Real browsers against two api nodes and one media node. Opt-in. |

`sync.ts` is free of React on purpose. The protocol is stateful and has to survive re-renders and StrictMode's double-mounted effects; keeping it outside the component tree also means it can be tested with no DOM at all.

## Tests

```sh
npm test        # unit; no infrastructure needed
make e2e        # browsers, two api nodes, one media node, one Redis
```

`make e2e` starts the media node, both api nodes and both dev servers itself. It needs `make up` and `make migrate` to have run, and a Chromium: it uses whichever revision is in Playwright's cache, or `CHROMIUM_PATH` if you point it somewhere else. `npx playwright install chromium` provides one if there is none.

Why two of everything: with a single api node, cross-node delivery is never exercised and every assertion still passes. That is the part of the ephemeral path most likely to be silently broken.

And one media node, because forwarding is a separate process (ADR-0007) and that is what makes a call belong to neither api node. Running forwarding inside `api` is supported and is what one machine does — it is also the configuration in which a call across two nodes cannot work, so it is not the one to test against.

## What the browser tests are for

They are not a UI regression suite. They exist because four claims cannot be checked any other way, and each has already caught a real bug:

- **Delivery crosses nodes.** Alice on one node, bob on the other, one Redis between them.
- **A socket dying loses nothing.** Alice goes offline, bob sends three messages, alice returns and ends up with all of them, once each, in order.
- **A repeated client identifier writes one entry.** Driven under the UI, since the composer correctly issues a fresh identifier per send.
- **Two real browsers see and hear each other**, on one api node and across two. A remote tile with a non-zero `videoWidth` is the only assertion that distinguishes a negotiated connection carrying media from one carrying nothing, and the difference between those two is invisible to every Go test — it is what phase 9's worst bug looked like from the outside.

The second and third have equivalents in `internal/messaging/messaging_test.go`. The difference is what is being asked: the Go tests ask whether the server is right, these ask whether the client draws the right conclusion from it.

Two bugs found this way, both invisible to the Go suite at the time:

- The server told only the *recipient* of a new conversation to start listening, so whoever started one could not see replies to it until they reconnected. Now covered by `TestTheInitiatorOfAConversationAlsoReceivesLive`.
- An unstable callback identity rebuilt the socket on ordinary re-renders, dropping the connection and everything held in it — which looks exactly like the server losing messages.
