#!/usr/bin/env bash
#
# NF-14: one media node supports at least 3 concurrent 4-participant video calls
# on 4 vCPUs.
#
# A script rather than a Go test, for two reasons. The claim is about a *deployed
# process* under a CPU limit, and a test that ran the forwarder in its own process
# would be measuring a machine it shares with the peers simulating the load. And
# the limit is the interesting part: GOMAXPROCS on the node and nothing else, so
# what is measured is the node's four cores rather than the host's twelve.
#
# The peers are not limited, deliberately. Twelve simulated participants encoding
# nothing and copying RTP cost far less than the node, and constraining them would
# turn a measurement of the server into a measurement of the harness.
#
# Assumes nothing running. Starts and stops its own node.
#
# Usage: scripts/capacity.sh [calls] [peers-per-call] [duration]

set -euo pipefail

cd "$(dirname "$0")/.."

calls=${1:-3}
peers=${2:-4}
duration=${3:-20s}
cpus=${SFU_CPUS:-4}
port=${SFU_PORT:-8099}

logs="$(mktemp -d)"
node=

cleanup() {
  local status=$?
  if [[ -n $node ]]; then
    kill "$node" 2>/dev/null || true
    wait "$node" 2>/dev/null || true
  fi
  if [[ $status -ne 0 ]]; then
    echo "node log in $logs/sfu.log" >&2
  else
    rm -rf "$logs"
  fi
}
trap cleanup EXIT

echo "building"
go build -o bin/ ./cmd/sfu ./cmd/harness

echo "starting a media node on :$port with GOMAXPROCS=$cpus"
# A UDP range wide enough for every transport in the run. Twelve participants at a
# handful of candidates each will not fit in a range of ten, and the failure would
# look like a node that cannot gather rather than one that is out of ports.
GOMAXPROCS="$cpus" SFU_ADDR=":$port" SFU_UDP_PORT_MIN=51000 SFU_UDP_PORT_MAX=51400 \
  ./bin/sfu >"$logs/sfu.log" 2>&1 &
node=$!

for _ in $(seq 1 40); do
  if curl -fsS -o /dev/null "http://localhost:$port/health" 2>/dev/null; then break; fi
  sleep 0.25
done
curl -fsS -o /dev/null "http://localhost:$port/health"

echo "driving $calls calls of $peers for $duration"
# Every peer publishes. NF-14 says four-participant *video* calls, so a call of four
# is four publishers and twelve forwarded streams — a receive-only peer would make
# the number look better and answer a question nobody asked.
#
# In the background so the node can be asked what it is holding *while* it holds it.
# Asking afterwards reports zero, because an empty call is forgotten — which is worth
# checking too, but is a different question.
./bin/harness \
  -node "http://localhost:$port" \
  -calls "$calls" \
  -peers "$peers" \
  -publishers "$peers" \
  -for "$duration" >"$logs/harness.log" 2>&1 &
driver=$!

# Sampled once everyone is in, not on a timer. Joins are sequential and a large run
# takes tens of seconds to fill, so a fixed sleep asks the node how many calls it
# holds while most of them have not started — which reads as a capacity failure and
# is a measurement of the harness's own ramp.
for _ in $(seq 1 240); do
  grep -q '"msg":"all peers joined"' "$logs/harness.log" 2>/dev/null && break
  sleep 0.5
done

echo
echo "the node, with everyone joined:"
during=$(curl -fsS "http://localhost:$port/health")
echo "  $during"

wait "$driver"

echo
echo "what every peer received:"
grep '"msg":"summary"' "$logs/harness.log" | sed 's/^/  /'

echo
echo "the node, after everyone left:"
after=$(curl -fsS "http://localhost:$port/health")
echo "  $after"

# What the node logged that is worth reading: an offer that went nowhere, a
# subscriber falling behind, gathering that was slow. Silence here is the result.
echo
echo "warnings and errors from the node:"
grep -E '"level":"(WARN|ERROR)"' "$logs/sfu.log" | sed 's/^/  /' || echo "  none"

# The claim, checked rather than printed. Reading numbers out of JSON with grep is
# crude and adequate: these are three integers this repository's own binaries just
# wrote, not a document from elsewhere.
field() { grep -o "\"$2\":[0-9]*" <<<"$1" | head -1 | cut -d: -f2; }

echo
failed=0
if [[ $(field "$during" calls) -ne $calls ]]; then
  echo "FAIL: the node held $(field "$during" calls) calls with everyone joined, want $calls" >&2
  failed=1
fi
if [[ $(field "$after" calls) -ne 0 ]]; then
  echo "FAIL: the node still holds $(field "$after" calls) calls after everyone left" >&2
  failed=1
fi

summary=$(grep '"msg":"summary"' "$logs/harness.log" | head -1)
silent=$(field "$summary" silent)
gaps=$(field "$summary" gaps)
receiving=$(field "$summary" receiving_video)

if [[ ${silent:-1} -ne 0 ]]; then
  echo "FAIL: $silent of $((calls * peers)) peers received no video" >&2
  failed=1
fi
# Gaps are not automatically a failure — an SFU drops packets by design once there
# are layers to choose between — but with one layer and a loopback path there is
# nothing legitimate to drop, so any gap here is the node falling behind.
if [[ ${gaps:-1} -ne 0 ]]; then
  echo "FAIL: $gaps gaps in forwarded video on a loopback path" >&2
  failed=1
fi

if [[ $failed -ne 0 ]]; then
  exit 1
fi

echo "NF-14 met: $calls concurrent $peers-participant video calls on $cpus vCPUs,"
echo "           $receiving of $((calls * peers)) peers receiving, $gaps gaps."
