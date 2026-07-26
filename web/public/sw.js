// The service worker, and the only reason it exists: a cold start with no network.
//
// Phase 6 made the *data* survive being offline — conversations and messages come out of a local
// SQLite store, and the client hydrates from it before it connects. What it could not do is
// survive the page being reloaded, because a browser needs the network to fetch the document and
// its scripts. So the offline test in phase 6 cut `/v1` and not the whole network, and said so.
// This closes that gap.
//
// Runtime caching, not a precache manifest. A manifest means a build plugin, a generated list of
// hashed filenames, and a service worker that behaves differently in development from production
// — which is exactly the kind of difference that gets discovered on the day it matters. Caching
// what has actually been fetched needs none of that and works identically in both.
//
// Network-first, so being online always means being current: a deploy is picked up on the next
// load rather than whenever a cache decides. The cache is a fallback for when the network fails,
// which is the only thing it is here for.

const CACHE = 'comms-shell-v1'

// install and activate take over immediately rather than waiting for every tab to close.
//
// The usual argument against skipWaiting is that a new worker may serve assets an old page does
// not expect. It does not apply to a network-first worker with no precache: what it serves is
// whatever the network just returned, and the whole point of this one is to be in charge the
// first time somebody loses their connection rather than the second.
self.addEventListener('install', () => {
  self.skipWaiting()
})

self.addEventListener('activate', (event) => {
  event.waitUntil(
    (async () => {
      // Old caches from a previous version of this worker. Named rather than swept blindly:
      // another origin's worker cannot see these, but a future version of this file might
      // keep a second cache on purpose.
      const names = await caches.keys()
      await Promise.all(names.filter((name) => name !== CACHE).map((name) => caches.delete(name)))
      await self.clients.claim()
    })(),
  )
})

self.addEventListener('fetch', (event) => {
  const request = event.request

  // GET only. A POST is a change somebody is waiting on, and replaying one out of a cache
  // would be inventing an action they did not take.
  if (request.method !== 'GET') return

  const url = new URL(request.url)
  if (url.origin !== self.location.origin) return

  // The API is never cached, and this is the most important line in the file. The client's
  // correctness rests on knowing whether the server answered: a cached `/v1` response would
  // make it believe it had synced when it had not, and a stale conversation list served as
  // fresh is worse in every way than no answer at all. Requests here must be allowed to fail
  // so the client falls back to its own store, which it is built to do.
  if (url.pathname.startsWith('/v1')) return

  event.respondWith(networkFirst(request))
})

/**
 * networkFirst answers from the network, falling back to whatever was cached.
 */
async function networkFirst(request) {
  const cache = await caches.open(CACHE)

  try {
    const response = await fetch(request)
    // Only successful, complete responses. Caching a 404 or an opaque cross-origin response
    // would turn a transient failure into a permanent one, served confidently.
    if (response.ok && response.type === 'basic') {
      // Not awaited: the response is handed back at once and the cache is written behind
      // it. Awaiting would put a disk write in front of every asset on the page.
      void cache.put(request, response.clone())
    }
    return response
  } catch (failure) {
    const cached = await cache.match(request)
    if (cached) return cached

    // A navigation to a URL nothing was cached for — a deep link, or a reload with a query
    // string — falls back to the shell. This client renders one document and decides what
    // to show from its own state, so the shell is the right answer for any path.
    if (request.mode === 'navigate') {
      const shell = (await cache.match('/')) ?? (await cache.match('/index.html'))
      if (shell) return shell
    }
    throw failure
  }
}
