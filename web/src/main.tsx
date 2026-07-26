import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'

import { App } from './App'
import './styles.css'

// StrictMode stays on. It double-invokes effects in development, which is exactly
// the pressure the socket lifecycle should be under: a connection that cannot
// survive being mounted, torn down and mounted again is one that will not survive
// a reconnect either.
createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
)

// The service worker, registered in development as well as production.
//
// Registering it only in production is the usual advice, and it is the wrong trade here: this
// worker is network-first with no precache, so it does not interfere with a dev server — and the
// alternative is that the one thing it exists for, a cold start with no network, is untestable by
// the browser suite, which runs against a dev server. A capability nobody can test is a
// capability nobody should claim.
//
// Failure is ignored on purpose. A browser without service workers, or a page served from a
// context that forbids them, is a client that works exactly as it did before this existed.
if ('serviceWorker' in navigator) {
  window.addEventListener('load', () => {
    navigator.serviceWorker.register('/sw.js').catch(() => {
      // Nothing to do and nobody to tell: offline reloads will not work, and everything
      // else will.
    })
  })
}
