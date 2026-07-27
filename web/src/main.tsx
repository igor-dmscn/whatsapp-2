import { Component, StrictMode, type ReactNode } from 'react'
import { createRoot } from 'react-dom/client'

import { App } from './App'
import './styles.css'

/**
 * Boundary turns a crash into a message.
 *
 * Without one, React unmounts the whole tree and the page goes white — which is what a
 * `crypto.randomUUID is not a function` in the composer looked like from the outside, and it
 * carries no information at all. The person seeing it cannot tell a bug from a lost
 * connection from a bad address, and neither can anyone they report it to.
 *
 * It catches errors thrown while rendering, which is deliberately not everything: a rejected
 * fetch still belongs to the code that awaited it, and those already have handling. This is
 * for the ones with nowhere else to go.
 */
class Boundary extends Component<{ children: ReactNode }, { failure: Error | null }> {
  state = { failure: null as Error | null }

  static getDerivedStateFromError(failure: Error) {
    return { failure }
  }

  componentDidCatch(failure: Error) {
    // Still logged. The banner names what broke; the console keeps the stack that says where.
    console.error('unrecoverable render error', failure)
  }

  render() {
    if (!this.state.failure) return this.props.children

    return (
      <main className="centred">
        <div className="card">
          <h1>comms</h1>
          <p>Something in the interface broke and could not carry on.</p>
          <p className="muted">{this.state.failure.message}</p>
          <button type="button" onClick={() => window.location.reload()}>
            Reload
          </button>
        </div>
      </main>
    )
  }
}

// StrictMode stays on. It double-invokes effects in development, which is exactly
// the pressure the socket lifecycle should be under: a connection that cannot
// survive being mounted, torn down and mounted again is one that will not survive
// a reconnect either.
createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <Boundary>
      <App />
    </Boundary>
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
