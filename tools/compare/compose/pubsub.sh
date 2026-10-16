#!/usr/bin/env bash
# One measured run of the at-most-once comparison (doc 32 §5, step 2): SYSTEM's one node up,
# the generator publishing CORPUS messages open-loop at RATE a second for SECONDS after WARMUP to a
# subscriber placed first, the report copied to OUT, everything taken down with its volumes. Every
# wait is bounded.
#
#   pubsub.sh SYSTEM RATE SECONDS WARMUP CORPUS OUT
#   SYSTEM: focal-bus | nats | kafka | redis       CORPUS: claim | event | testament | entropy
set -euo pipefail
cd "$(dirname "$0")"
system="$1"; rate="$2"; seconds="$3"; warmup="$4"; corpus="$5"; out="$6"
mkdir -p "$out"
# A project of its own: compose's default (the directory's name) is the other comparisons' too, and
# a `down --remove-orphans` would take theirs.
project="${COMPOSE_PROJECT_NAME:-focal-pubsub}"
# A busy machine unless BUSY=0 (busy.yaml): the node held to the last NODE_CPUS cores of the
# engine's machine, a busy loop on each beside it.
files="-f pubsub.yaml"
if [ "${BUSY:-1}" != 0 ]; then
  cores=$(docker info --format '{{.NCPU}}')
  first=$((cores - ${NODE_CPUS:-1}))
  last=$((cores - 1))
  export NODE_CPUSET="${NODE_CPUSET:-$first-$last}"
  files="$files -f busy.yaml"
fi
# Extra compose files, for an experiment's overrides.
for extra in ${EXTRA_COMPOSE:-}; do files="$files -f $extra"; done
# shellcheck disable=SC2086
dc() { docker compose -p "$project" $files --profile "$system" "$@"; }
cleanup() { dc down -v --remove-orphans >/dev/null 2>&1 || true; }
evidence() {
  echo "::group::evidence ($system at $rate/s)"
  dc ps -a || true
  docker stats --no-stream --format '{{.Name}} cpu={{.CPUPerc}} mem={{.MemUsage}}' || true
  dc logs --no-color --tail 80 || true
  echo "::endgroup::"
}
# Each of the run's containers' CPU as its cgroup (v2) counts it, before and after the load.
cpu_stat() {
  local phase=$1 c
  echo "$phase - wall_usec $(perl -MTime::HiRes=time -e 'printf "%d", time() * 1e6')"
  for c in $(dc ps --format '{{.Name}}'); do
    docker exec "$c" cat /sys/fs/cgroup/cpu.stat 2>/dev/null \
      | awk -v p="$phase" -v c="$c" '{ print p, c, $1, $2 }' || true
  done
}
# Each container's UDP and TCP counters (its own network namespace): datagrams dropped for a full
# receive buffer, segments retransmitted; and its interface's bytes and packets each way, what
# every system puts on the wire for the same messages.
net_stat() {
  local phase=$1 c
  for c in $(dc ps --format '{{.Name}}'); do
    docker exec "$c" cat /proc/net/snmp 2>/dev/null \
      | awk -v p="$phase" -v c="$c" '/^(Udp|Tcp):/ { print p, c, $0 }' || true
    docker exec "$c" cat /proc/net/dev 2>/dev/null \
      | awk -v p="$phase" -v c="$c" '/eth0:/ { sub(/.*eth0:/, ""); print p, c, "Dev: rx_bytes", $1, "rx_packets", $2, "tx_bytes", $9, "tx_packets", $10 }' || true
  done
}
trap cleanup EXIT
cleanup
dc up -d
case "$system" in
  focal-bus) endpoint=bus:7443; extra="--pki /pki" ;;
  nats) endpoint=nats:4222; extra="" ;;
  kafka) endpoint=kafka:9092; extra="" ;;
  redis) endpoint=redis:6379; extra="" ;;
  *) echo "unknown system $system" >&2; exit 2 ;;
esac
report="pubsub-$system-$rate-$corpus.json"
# The node is ready when it says so: the bus its `Listening` line, Redis its answer to PING, NATS
# its log's "Server is ready", Kafka's "Kafka Server started"; at most sixty seconds.
ready() {
  case "$system" in
    focal-bus) dc logs bus 2>/dev/null | grep -q '"Listening"' ;;
    redis) dc exec -T redis redis-cli ping 2>/dev/null | grep -q PONG ;;
    nats) dc logs nats 2>/dev/null | grep -q "Server is ready" ;;
    kafka) dc logs kafka 2>/dev/null | grep -q "Kafka Server started" ;;
  esac
}
n=0
until ready; do
  n=$((n + 1))
  if [ "$n" -ge 60 ]; then evidence; exit 1; fi
  sleep 1
done
echo "busy=${BUSY:-1} node_cpuset=${NODE_CPUSET:-none} node_cpus=${NODE_CPUS:-1}" > "$out/$report.condition.txt"
cpu_stat before > "$out/$report.cpu.txt"
net_stat before > "$out/$report.net.txt"
# One run, bounded at four times its length and a minute.
# shellcheck disable=SC2086
if ! timeout $(( (seconds + warmup) * 4 + 60 )) \
  docker compose -p "$project" $files --profile "$system" exec -T generator focal-compare \
    --system "$system" --mode pubsub --endpoints "$endpoint" --rate "$rate" \
    --seconds "$seconds" --warmup "$warmup" --corpus "$corpus" $extra --out "/reports/$report"; then
  evidence
  exit 1
fi
cpu_stat after >> "$out/$report.cpu.txt"
net_stat after >> "$out/$report.net.txt"
dc exec -T generator cat "/reports/$report" > "$out/$report"
# The node's own account, which it prints as it stops.
dc stop -t 15 >/dev/null 2>&1 || true
dc logs --no-color --tail 200 > "$out/$report.logs.txt" 2>&1 || true
echo "wrote $out/$report"
