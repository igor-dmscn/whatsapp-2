# A Vite single-page app, not a server-rendered framework

Recorded late: this was settled while designing and never written down, and the code now depends on it.

## Decision

The browser client is a Vite single-page app talking to the Go api over `/v1`. No Next.js, no Remix, no server-side rendering, no JavaScript server of any kind in the deployment.

## Why not Next.js

It is the default answer for a React application, so the reasons it is the wrong one here are worth stating.

**There is no server-side rendering to do.** Every screen is behind authentication and its content is a conversation the server cannot read ([ADR-0001](./0001-content-opaque-server-e2ee-deferred.md)). Rendering on the server would mean shipping a JavaScript process that fetches ciphertext-shaped payloads it cannot turn into HTML. The first paint that matters is not the server's — it is the local store's, in phase 6, from data already on the device.

**It would add a second backend.** Route handlers and server actions are a place for logic to accumulate, and the logic would be in TypeScript, outside the bounded contexts, unreachable by the architecture test. The value of this repository is that the model is in one language behind compiler-enforced boundaries; a Node tier that can also talk to Postgres undoes that quietly.

**Nothing it optimises for is a constraint here.** No SEO — the app is entirely private. No cold-start latency for anonymous visitors — there are none. What it costs is a process to deploy, a second dependency tree, and a rendering model that has to be worked around for a client whose defining feature is a long-lived socket and local state.

## What was also rejected

**Create React App** — unmaintained.

**A meta-framework for routing alone** — phase 2 has two screens and later phases add a handful. The router can be added when there is something to route; adding a framework to get one is the wrong order.

**SolidJS or Svelte**, both of which suit this shape well. React wins on nothing technical: it is what the project asked for, and its ecosystem is where the WebRTC and virtualised-list work of phases 7 and 9 already exists.

## Consequences

The build output is static files. The api serves them in production, so there is one origin, one deployment, and no CORS configuration — the development proxy exists to make that true locally too.

State management is React's own. `useSyncExternalStore` over a plain class holding the socket, no Redux, no Zustand, no TanStack Query: there is one source of pushed state and it already needs to live outside the component tree to survive re-renders. A cache library would be a second place for the same entries to live, and they would disagree.

Phase 6 replaces the in-memory store with SQLite over OPFS. That is a change behind the same interface, which is the main thing this decision has to leave room for.
