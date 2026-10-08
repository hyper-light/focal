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
cleanup() { dc down -v --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT
cleanup
dc up -d
report="$system-$rate-$payload.json"

case "$system" in
  focal)
    bash focal-bootstrap.sh
    worker=$(dc exec -T focal1 focal --data-dir /data identity | python3 -c '
import json, sys
w = json.load(sys.stdin)["worker"]
print(w if isinstance(w, str) else bytes(w).hex())')
    total=$(( rate * (seconds + warmup) ))
    # As many callers as the rate needs at the bench's tens of milliseconds a
    # request, within focal-load's bound of 64; each paced from its intended start.
    callers=$(( rate / 100 )); [ "$callers" -lt 4 ] && callers=4; [ "$callers" -gt 64 ] && callers=64
    dc exec -T focal-load sh -c "cat > /reports/shape.yaml" <<EOF
claims: $total
transport: enrolled
enrollment: /client
worker: "$worker"
profile: authored_v1
concurrency: $callers
rate: $rate
warmup_ms: $(( warmup * 1000 ))
EOF
    dc exec -T focal-load focal-load --shape /reports/shape.yaml --out "/reports/$report"
    dc exec -T focal-load cat "/reports/$report" > "$out/$report"
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
  retry 60 generate
  dc exec -T generator cat "/reports/$report" > "$out/$report"
fi
echo "wrote $out/$report"
