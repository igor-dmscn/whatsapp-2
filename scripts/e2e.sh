#!/usr/bin/env bash
#
# Runs the browser suite against two api nodes and two dev servers.
#
# Two of each, deliberately. One node would leave cross-node delivery untested,
# and cross-node delivery is the part of the ephemeral path most likely to be
# quietly broken — every assertion still passes on a single process.
#
# Assumes `make up` and `make migrate` have already run.

set -euo pipefail

cd "$(dirname "$0")/.."

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  . ./.env
  set +a
fi

logs="$(mktemp -d)"
pids=()

cleanup() {
  local status=$?
  for pid in "${pids[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  # Kept on failure and only on failure: a passing run's logs are noise, and a
  # failing one is very hard to diagnose without them.
  if [[ $status -ne 0 ]]; then
    echo "logs in $logs"
  else
    rm -rf "$logs"
  fi
}
trap cleanup EXIT

waitFor() {
  local url=$1
  for _ in $(seq 1 60); do
    # Errors are suppressed because the first few attempts are expected to fail;
    # the loop timing out is the only failure worth reporting.
    if curl -fsS -o /dev/null "$url" 2>/dev/null; then return 0; fi
    sleep 0.5
  done
  echo "timed out waiting for $url" >&2
  return 1
}

echo "building api, worker and cli"
# The cli is built because the browser suite runs it: one of phase 6's verifications is
# that both clients answer the same search identically, which needs both to exist.
go build -o bin/ ./cmd/api ./cmd/worker ./cmd/cli

echo "starting two api nodes"
API_ADDR=:8080 ./bin/api >"$logs/api-8080.log" 2>&1 &
pids+=($!)
API_ADDR=:8081 ./bin/api >"$logs/api-8081.log" 2>&1 &
pids+=($!)

waitFor http://localhost:8080/health
waitFor http://localhost:8081/health

# From phase 3 the badge and tick assertions need the relay and the projections
# running. One worker is enough — it is the only thing that may run more than once
# only with care, since the relay claims rows with FOR UPDATE to preserve ordering.
echo "starting the worker"
./bin/worker >"$logs/worker.log" 2>&1 &
pids+=($!)

echo "starting two dev servers"
(cd web && API_URL=http://localhost:8080 npx vite --port 5173 --strictPort >"$logs/web-5173.log" 2>&1) &
pids+=($!)
(cd web && API_URL=http://localhost:8081 npx vite --port 5174 --strictPort >"$logs/web-5174.log" 2>&1) &
pids+=($!)

waitFor http://localhost:5173/
waitFor http://localhost:5174/

echo "running browser suite"
cd web && COMMS_E2E=1 npx vitest run src/browser.test.ts
