#!/usr/bin/env bash
#
# Runs the browser suite against two api nodes, one media node and two dev servers.
#
# Two api nodes, deliberately. One would leave cross-node delivery untested, and
# cross-node delivery is the part of the ephemeral path most likely to be quietly
# broken — every assertion still passes on a single process.
#
# One media node, also deliberately. Both api nodes forward through it, which is
# what makes a call belong to neither of them; running forwarding inside api
# instead is supported and is what a development machine does, but it is the
# configuration in which a cross-node call cannot work.
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

echo "building api, sfu, worker and cli"
# The cli is built because the browser suite runs it: one of phase 6's verifications is
# that both clients answer the same search identically, which needs both to exist.
go build -o bin/ ./cmd/api ./cmd/sfu ./cmd/worker ./cmd/cli

# The media node first, so that neither api node spends its first second retrying a
# subscription. The suite would still pass — an offer with nowhere to go is retried —
# but a test that depends on the retry is a test of the retry.
echo "starting the media node"
SFU_ADDR=:8090 ./bin/sfu >"$logs/sfu-8090.log" 2>&1 &
pids+=($!)
waitFor http://localhost:8090/health

echo "starting two api nodes"
export SFU_URL=http://localhost:8090
API_ADDR=:8080 ./bin/api >"$logs/api-8080.log" 2>&1 &
pids+=($!)
API_ADDR=:8081 ./bin/api >"$logs/api-8081.log" 2>&1 &
pids+=($!)

waitFor http://localhost:8080/health
waitFor http://localhost:8081/health

# Both api nodes subscribed to the node's offers, which is the one part of the split
# that nothing else would report. Without it calls still start and still carry media
# one way, so waiting here is what keeps a broken reverse channel from presenting as a
# flaky media assertion twenty seconds later.
echo "waiting for both api nodes to subscribe to offers"
for _ in $(seq 1 40); do
  subscribers=$(curl -fsS http://localhost:8090/health | sed -n 's/.*"offer_subscribers":\([0-9]*\).*/\1/p')
  [[ ${subscribers:-0} -ge 2 ]] && break
  sleep 0.25
done
if [[ ${subscribers:-0} -lt 2 ]]; then
  echo "only ${subscribers:-0} api nodes subscribed to the media node's offers" >&2
  exit 1
fi

# From phase 3 the badge and tick assertions need the relay and the projections
# running. One worker is enough — it is the only thing that may run more than once
# only with care, since the relay claims rows with FOR UPDATE to preserve ordering.
echo "starting the worker"
./bin/worker >"$logs/worker.log" 2>&1 &
pids+=($!)

echo "starting two dev servers"
# exec, and vite's own binary rather than npx, so the recorded pid is the server itself.
# Both matter: without exec the pid is the subshell, and through npx it is a wrapper — and
# cleanup killing either of those leaves the real node process holding the port. Two dev
# servers were left on 5173 and 5174 for nine hours that way, which the next run cannot bind.
(cd web && exec env API_URL=http://localhost:8080 ./node_modules/.bin/vite --port 5173 --strictPort >"$logs/web-5173.log" 2>&1) &
pids+=($!)
(cd web && exec env API_URL=http://localhost:8081 ./node_modules/.bin/vite --port 5174 --strictPort >"$logs/web-5174.log" 2>&1) &
pids+=($!)

waitFor http://localhost:5173/
waitFor http://localhost:5174/

echo "running browser suite"
cd web && COMMS_E2E=1 npx vitest run src/browser.test.ts ${VITEST_ARGS:-}
