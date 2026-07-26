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
