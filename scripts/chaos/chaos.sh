#!/bin/bash
# Claims under faults on a three-node focal cluster in Docker (goal
# condition 3). The founder's session is placed to survive one failure, so
# all three nodes are voters. A stream of claims is written through the
# founder while every node's interface loses and delays packets, and one
# voter is SIGKILLed and later restarted. Afterwards every acknowledged
# claim must read back on two nodes. Every loop and wait is counted.
# Usage: chaos.sh IMAGE CLAIMS LOSS% DELAYms [kill|partition]
#   kill:      SIGKILL fc-host-a for the middle third, then restart it.
#   partition: cut fc-host-b off both ways (100% loss) for the middle third,
#              then reconnect it; the node stays up throughout.
set -uo pipefail
IMAGE=${1:?image}; CLAIMS=${2:-120}; LOSS=${3:-5}; DELAY=${4:-50}; MODE=${5:-kill}
HERE=$(cd "$(dirname "$0")" && pwd)
OUT=${OUT:-$(mktemp -d)}; mkdir -p "$OUT"; export OUT
NODES=(fc-founder fc-host-a fc-host-b)
FAR=4102444800000

focal() { local node=$1; shift; docker exec "$node" /focal --data-dir /var/lib/focal "$@"; }
now_ms() { perl -MTime::HiRes=time -e 'printf "%d\n", time*1000'; }
pause() { perl -e "select(undef,undef,undef,$1)"; }

bash "$HERE/down.sh" >/dev/null
bash "$HERE/cluster.sh" "$IMAGE" || exit 1

identity=$(focal fc-founder diagnose node --identity)
tenant=$(echo "$identity" | ${PYTHON:-python3} -c 'import json,sys;print(json.load(sys.stdin)["result"]["identity"]["tenant"])')
session=$(echo "$identity" | ${PYTHON:-python3} -c 'import json,sys;print(json.load(sys.stdin)["result"]["identity"]["session"])')

# One tolerated failure: three voters. A plan refused on a stale view is
# planned again (24 §16), at most 30 times.
for attempt in $(seq 1 30); do
  planned=$(focal fc-founder cluster sessions plan --tenant "$tenant" --session "$session" --max-failures 1 2>&1)
  echo "$planned" | grep -q '"state": *"planned"' && break
  pause 2
done
echo "$planned" | grep -q '"state": *"planned"' || { echo "never planned: $planned"; exit 1; }
for attempt in $(seq 1 180); do
  voters=$(focal fc-founder cluster placement 2>/dev/null | ${PYTHON:-python3} -c '
import json,sys
view=json.load(sys.stdin)["result"]["placement"]
for p in view.get("partitions",[]):
    for s in p.get("sessions",[]):
        if s.get("pending") is None and s.get("achieved_max_failures")==1:
            print(len(s["voters"]))' 2>/dev/null)
  [ "$voters" = "3" ] && break
  pause 2
done
[ "$voters" = "3" ] || { echo "session never reached three voters"; exit 1; }
focal fc-founder cluster replicas activate-native >/dev/null 2>&1 || true
echo "session placed on three voters"

# Faults: loss and delay on every node, from a sidecar in its namespace.
# The sidecar image holds tc already: a node cut off cannot fetch packages,
# and its reconnection must not depend on its own network.
docker image inspect focal-chaos-tc >/dev/null 2>&1 || \
  docker build -q -t focal-chaos-tc -f "$HERE/tc.Dockerfile" "$HERE" >/dev/null
netem() { # node rule: replace the node's root qdisc with this netem rule
  # shellcheck disable=SC2086
  docker run --rm --net "container:$1" --cap-add NET_ADMIN focal-chaos-tc \
    qdisc replace dev eth0 root netem $2 || echo "netem on $1 failed"
}
for node in "${NODES[@]}"; do
  netem "$node" "delay ${DELAY}ms $((DELAY/2))ms loss ${LOSS}%"
done
echo "netem: ${LOSS}% loss, ${DELAY}ms ± $((DELAY/2))ms on every node"

KILL_AT=$((CLAIMS/3)); START_AT=$((2*CLAIMS/3))
: > "$OUT/acked"; : > "$OUT/failed"; : > "$OUT/latency"
for i in $(seq 1 "$CLAIMS"); do
  if [ "$MODE" = kill ]; then
    [ "$i" -eq "$KILL_AT" ] && { docker kill -s KILL fc-host-a >/dev/null; echo "claim $i: SIGKILL fc-host-a"; }
    [ "$i" -eq "$START_AT" ] && { docker start fc-host-a >/dev/null; echo "claim $i: restarted fc-host-a"; }
  else
    [ "$i" -eq "$KILL_AT" ] && { netem fc-host-b "loss 100%"; echo "claim $i: fc-host-b cut off"; }
    [ "$i" -eq "$START_AT" ] && { netem fc-host-b "delay ${DELAY}ms $((DELAY/2))ms loss ${LOSS}%"; echo "claim $i: fc-host-b reconnected"; }
  fi
  doc="{\"target\":\"self\",\"action\":\"handoff\",\"description\":\"chaos $i\",\"validations\":[{\"kind\":\"receipt\",\"description\":\"Receive the report testament\",\"deadline\":{\"at\":$FAR}}]}"
  t0=$(now_ms)
  out=$(focal fc-founder submit claim --json "$doc" 2>&1); code=$?
  claim=$(echo "$out" | awk -F'\t' '$1=="CREATED" && $2=="claim" {print $3; exit}')
  if [ $code -eq 0 ] && [ -n "$claim" ]; then
    # The committed creation is the replicated, durable claim; posting it
    # is a lifecycle step whose policy a self-handoff does not meet.
    t1=$(now_ms)
    echo "$claim" >> "$OUT/acked"; echo $((t1-t0)) >> "$OUT/latency"
  else
    echo "$i submit $code $(echo "$out" | head -c 200)" >> "$OUT/failed"
  fi
done

# Verdict: every acknowledged claim reads back on every node, the one that
# was killed or cut off included; none is duplicated.
missing=0
for claim in $(cat "$OUT/acked"); do
  for node in "${NODES[@]}"; do
    focal "$node" get claim "$claim" >/dev/null 2>&1 || { missing=$((missing+1)); echo "missing $claim on $node"; }
  done
done
acked=$(wc -l < "$OUT/acked" | tr -d ' ')
dupes=$(sort "$OUT/acked" | uniq -d | wc -l | tr -d ' ')
failed=$(wc -l < "$OUT/failed" | tr -d ' ')
sort -n "$OUT/latency" > "$OUT/latency.sorted"
pct() { local n; n=$(wc -l < "$OUT/latency.sorted"); [ "$n" -gt 0 ] && sed -n "$(( (n*$1+99)/100 ))p" "$OUT/latency.sorted"; }
echo "claims $CLAIMS acked $acked refused_or_unknown $failed missing_reads $missing duplicates $dupes"
echo "end-to-end ms (CLI submit via docker exec): p50 $(pct 50) p99 $(pct 99) max $(tail -1 "$OUT/latency.sorted")"
