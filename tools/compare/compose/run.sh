#!/usr/bin/env bash
# One measured run of the comparison (docs/qualification/competitive-p99.md):
# SYSTEM's three nodes up with every acknowledged write on disk at a quorum
# (the plan's durable row), the generator on the bench network driving it
# open-loop at RATE a second for SECONDS after WARMUP, the report copied to
# OUT, and everything taken down with its volumes. Every wait is bounded.
#
#   run.sh SYSTEM RATE SECONDS WARMUP PAYLOAD OUT
#   SYSTEM: focal | kafka | nats | redis
set -euo pipefail
cd "$(dirname "$0")"
system="$1"; rate="$2"; seconds="$3"; warmup="$4"; payload="$5"; out="$6"
mkdir -p "$out"
export NATS_SYNC=always KAFKA_FLUSH=1 REDIS_AOF=yes REDIS_FSYNC=always
dc() { docker compose -f compose.yaml --profile "$system" "$@"; }
# A command retried until it succeeds, at most $1 times two seconds apart.
retry() {
  local tries=$1 n=0; shift
  until "$@"; do
    n=$((n + 1))
    if [ "$n" -ge "$tries" ]; then echo "gave up after $tries: $*" >&2; return 1; fi
    sleep 2
  done
}
# What a stalled or failed run leaves for diagnosis, printed before the teardown:
# each container's state, memory and CPU, each focal node's replicas and its log.
evidence() {
  echo "::group::evidence ($system at $rate/s)"
  dc ps -a || true
  docker stats --no-stream --format '{{.Name}} cpu={{.CPUPerc}} mem={{.MemUsage}}' || true
  for c in $(dc ps -a --format '{{.Name}}'); do
    docker inspect "$c" --format "$c oom={{.State.OOMKilled}} exit={{.State.ExitCode}} status={{.State.Status}}" || true
  done
  if [ "$system" = focal ]; then
    for n in focal1 focal2 focal3; do
      echo "--- $n replicas"
      timeout 30 docker compose -f compose.yaml --profile focal exec -T "$n" focal --data-dir /data inspect replicas --replicas || true
      echo "--- $n log"
      dc logs --no-color --tail 120 "$n" || true
    done
  fi
  echo "::endgroup::"
}
cleanup() { dc down -v --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT
cleanup
dc up -d
report="$system-$rate-$payload.json"

case "$system" in
  focal)
    bash focal-bootstrap.sh
    worker=$(dc exec -T focal1 focal --data-dir /data inspect identity | python3 -c '
import json, sys
w = json.load(sys.stdin)["worker"]
print(w if isinstance(w, str) else bytes(w).hex())')
    total=$(( rate * (seconds + warmup) ))
    # focal-load's most callers at every rate, each paced from its intended
    # start, so the offered rate is the same whatever the callers. One request
    # is out per caller: at one caller a hundred per second, focal at 1000/s
    # had ten requests out where the other systems' generator allows 4096, and
    # a write past ten milliseconds left the rate unoffered, its backlog
    # counted as seconds of latency.
    # One participant holds at most sixteen connections to a node: sixteen
    # callers, each on its own, each with up to 64 writes under way, sent at
    # their places on the schedule whatever the earlier ones are doing — the
    # other systems' generator likewise sends open loop, up to 4096 out.
    callers=16
    connections=16
    inflight=64
    dc exec -T focal-load sh -c "cat > /reports/shape.yaml" <<EOF
claims: $total
transport: enrolled
enrollment: /client/CLIENT.contexts/enrollment-bench
worker: "$worker"
profile: authored_v1
concurrency: $callers
connections: $connections
inflight: $inflight
rate: $rate
warmup_ms: $(( warmup * 1000 ))
EOF
    # Bounded at four times the run's intended length and two minutes more: a
    # run that cannot keep the offered rate still ends, its evidence printed.
    if ! timeout $(( (seconds + warmup) * 4 + 120 )) docker compose -f compose.yaml --profile focal \
      exec -T -e "FOCAL_LOAD_WRITES_CSV=/reports/$report.csv" focal-load \
      focal-load --shape /reports/shape.yaml --out "/reports/$report"; then
      evidence
      exit 1
    fi
    dc exec -T focal-load cat "/reports/$report" > "$out/$report"
    # Each write's intended start and latency, for where its tail falls in time.
    dc exec -T focal-load cat "/reports/$report.csv" > "$out/$report.csv" || true
    # Each node's replicas as they report themselves, the cost of every
    # checkpoint they wrote by stage among them, for where the tail's time went.
    for n in focal1 focal2 focal3; do
      dc exec -T "$n" focal --data-dir /data inspect replicas --replicas \
        > "$out/$report.$n.replicas.json" 2>/dev/null || true
      # The node's metrics: its log's flush and commit-wait quantiles and its
      # owners' longest periods, for whether a stall was the disk's.
      dc exec -T "$n" focal --data-dir /data inspect node --metrics \
        > "$out/$report.$n.metrics.txt" 2>/dev/null || true
    done
    ;;
  kafka) endpoints=kafka1:9092,kafka2:9092,kafka3:9092 ;;
  nats) endpoints=nats1:4222,nats2:4222,nats3:4222 ;;
  redis) endpoints=redis1:6379,redis2:6379,redis3:6379 ;;
  *) echo "unknown system $system" >&2; exit 2 ;;
esac

if [ "$system" != focal ]; then
  # The cluster forms its quorum on its own clock: the generator's first
  # connection is asked again until it succeeds, as an operator's client would.
  generate() {
    dc exec -T generator focal-compare --system "$system" --durable \
      --endpoints "$endpoints" --rate "$rate" --payload "$payload" \
      --seconds "$seconds" --warmup "$warmup" --out "/reports/$report"
  }
  retry 60 generate || { evidence; exit 1; }
  dc exec -T generator cat "/reports/$report" > "$out/$report"
fi
echo "wrote $out/$report"
