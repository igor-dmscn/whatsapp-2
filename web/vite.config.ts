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
  // SQLite's WebAssembly is fetched at runtime by a `new URL(..., import.meta.url)`
  // inside the package. Vite's dependency pre-bundling rewrites that module, which
  // breaks the resolution and leaves the wasm 404ing — excluding it keeps the package's
  // own layout intact. Found by the store silently falling back to memory.
  optimizeDeps: { exclude: ['@sqlite.org/sqlite-wasm'] },
  server: {
    port: 5173,
    // Hosts this dev server will answer to. The check exists to stop a page on another
    // site from resolving its own name to 127.0.0.1 and talking to your dev server, so it
    // is not one to switch off wholesale — a tunnel's domain is named instead, and the
    // leading dot covers the random subdomain ngrok assigns each run. Without this, every
    // request through a tunnel is a 403 reading "this host is not allowed".
    //
    // More than one tunnel domain, because reachability is not up to us: ngrok's domains are
    // widely filtered by carriers, ISPs and DNS blockers — it gets used for malware command
    // and control, so it is blocked as a category. That presents as a blank tab loading
    // forever, with no request ever arriving, and it is not something a setting here fixes.
    // Cloudflare's is the usual way out, being harder to block as a category.
    //
    // PUBLIC_HOST is for the others: a Tailscale name, or a domain of your own.
    allowedHosts: [
      '.ngrok-free.app',
      '.ngrok.app',
      '.trycloudflare.com',
      '.ts.net',
      ...(process.env.PUBLIC_HOST ? [process.env.PUBLIC_HOST] : []),
    ],
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
