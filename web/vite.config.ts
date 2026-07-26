import react from '@vitejs/plugin-react'
import { defineConfig } from 'vitest/config'

// The dev server proxies /v1 to the api binary rather than pointing the client at
// http://localhost:8080 directly. Two reasons, both about being the same origin as
// production: no CORS configuration exists to drift out of sync, and the WebSocket
// passes the server's same-origin check without ALLOWED_ORIGINS being set.
//
// changeOrigin is left off deliberately — the Host header must keep the browser's
// value or the server sees Origin and Host disagree and rejects the upgrade.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/v1': {
        target: process.env.API_URL ?? 'http://localhost:8080',
        ws: true,
      },
    },
  },
  test: {
    // The default five seconds is right for unit tests and far too short for the
    // browser ones, which wait on reconnects and gap fetches against a real server.
    testTimeout: 90_000,
    hookTimeout: 90_000,
  },
})
