#!/usr/bin/env bash
# One measured run of the comparison (docs/qualification/competitive-p99.md):
# SYSTEM's three nodes up with every acknowledged write on disk at a quorum
# (the plan's durable row), the generator on the bench network driving it
# open-loop at RATE a second for SECONDS after WARMUP, the report copied to
# OUT, and everything taken down with its volumes. Every wait is bounded.
#
#   run.sh SYSTEM RATE SECONDS WARMUP PAYLOAD OUT
#   SYSTEM: focal | kafka | nats | redis
#   MODE (environment): append (default), each record stored; or deliver,
#   each record also taken by a consumer and acknowledged by it durably, a
#   work queue's message end to end (focal: its seven-write lifecycle, the
#   consumer an enrolled client of its own; RATE is messages a second).
set -euo pipefail
cd "$(dirname "$0")"
system="$1"; rate="$2"; seconds="$3"; warmup="$4"; payload="$5"; out="$6"
mode="${MODE:-append}"
case "$mode" in
  append) suffix="" ;;
  deliver) suffix="-deliver" ;;
  *) echo "unknown mode $mode" >&2; exit 2 ;;
esac
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
# Each of the run's containers' CPU as its cgroup (v2) counts it, read on the
# host so nothing runs inside a container: the microseconds it ran, and those
# and the times its CPU quota held it back. Taken before and after the load,
# each line `PHASE CONTAINER KEY VALUE`: a system's CPU per acknowledged write
# is the difference over the writes it acknowledged. Prints nothing where the
# host has no cgroup v2 hierarchy (a run on macOS).
# Where the host cannot read a container's process (Docker Desktop's VM on macOS), each is read
# inside its container instead: its cgroup's own `cpu.stat`, its namespace's `/proc/net`.
cpu_stat() {
  local phase=$1 c pid group
  echo "$phase - wall_usec $(perl -MTime::HiRes=time -e 'printf "%d", time() * 1e6')"
  for c in $(dc ps --format '{{.Name}}'); do
    pid=$(docker inspect --format '{{.State.Pid}}' "$c" 2>/dev/null) || continue
    group=$(awk -F: '$1 == "0" && $2 == "" { print $3 }' "/proc/$pid/cgroup" 2>/dev/null) || group=""
    if [ -n "$group" ] && [ -r "/sys/fs/cgroup$group/cpu.stat" ]; then
      awk -v p="$phase" -v c="$c" '{ print p, c, $1, $2 }' "/sys/fs/cgroup$group/cpu.stat"
    else
      docker exec "$c" cat /sys/fs/cgroup/cpu.stat 2>/dev/null \
        | awk -v p="$phase" -v c="$c" '{ print p, c, $1, $2 }' || true
    fi
  done
}
# Each of the run's containers' network as its own namespace counts it, read
# on the host (`/proc/PID/net` is the namespace of PID): its UDP and TCP
# counters and its interface's bytes and packets each way, before and after
# the load, each line `PHASE CONTAINER KEY VALUE`, for what a write costs the
# network: datagrams or segments, and bytes. Prints nothing off Linux.
net_stat() {
  local phase=$1 c pid snmp dev
  for c in $(dc ps --format '{{.Name}}'); do
    pid=$(docker inspect --format '{{.State.Pid}}' "$c" 2>/dev/null) || continue
    if [ -r "/proc/$pid/net/snmp" ] && [ -r "/proc/$pid/net/dev" ]; then
      snmp=$(cat "/proc/$pid/net/snmp"); dev=$(cat "/proc/$pid/net/dev")
    else
      snmp=$(docker exec "$c" cat /proc/net/snmp 2>/dev/null) || continue
      dev=$(docker exec "$c" cat /proc/net/dev 2>/dev/null) || continue
    fi
    printf '%s\n' "$snmp" | awk -v p="$phase" -v c="$c" '
      $1 == "Udp:" || $1 == "Tcp:" {
        if (!($1 in names)) { names[$1] = $0; next }
        n = split(names[$1], k); split($0, v)
        for (i = 2; i <= n; i++) print p, c, substr($1, 1, length($1) - 1) "." k[i], v[i]
      }'
    printf '%s\n' "$dev" | awk -v p="$phase" -v c="$c" '
      { sub(/^[ \t]+/, "") }
      /^eth0:/ {
        sub(/^eth0:[ \t]*/, ""); split($0, f, /[ \t]+/)
        print p, c, "eth0.rx_bytes", f[1]; print p, c, "eth0.rx_packets", f[2]
        print p, c, "eth0.tx_bytes", f[9]; print p, c, "eth0.tx_packets", f[10]
      }'
  done
}
# Each image the profile runs, pulled once and one at a time before the cluster
# starts: a registry's anonymous pulls are rate-limited per address (ECR Public
# answers `Rate exceeded` past about one a second, Docker Hub refuses past its
# own quota), and `compose up` pulls one image for three containers at once.
# Bounded: six attempts each, the pause doubling from fifteen seconds (about 8 minutes),
# as a public registry's `toomanyrequests` lasts minutes, not seconds.
pull_images() {
  local image tries pause
  for image in $(dc config --images | sort -u); do
    docker image inspect "$image" >/dev/null 2>&1 && continue
    tries=0; pause=15
    until docker pull --quiet "$image" >/dev/null; do
      tries=$((tries + 1))
      if [ "$tries" -ge 6 ]; then echo "gave up pulling $image" >&2; return 1; fi
      sleep "$pause"; pause=$((pause * 2))
    done
  done
}
trap cleanup EXIT
cleanup
pull_images
dc up -d
report="$system-$rate-$payload$suffix.json"

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
    # Append: every claim targets the node's worker. Deliver: every claim targets the
    # consumer, a client of its own that completes it.
    if [ "$mode" = deliver ]; then
      lifecycle="lifecycle: deliver
consumer_enrollment: /client/CLIENT.contexts/enrollment-consumer"
    else
      lifecycle="worker: \"$worker\""
    fi
    dc exec -T focal-load sh -c "cat > /reports/shape.yaml" <<EOF
claims: $total
transport: enrolled
enrollment: /client/CLIENT.contexts/enrollment-bench
$lifecycle
profile: authored_v1
concurrency: $callers
connections: $connections
inflight: $inflight
rate: $rate
warmup_ms: $(( warmup * 1000 ))
EOF
    # Bounded at four times the run's intended length and two minutes more: a
    # run that cannot keep the offered rate still ends, its evidence printed.
    # Every node's processor with call graphs, twenty seconds into the
    # measured window: where each node's CPU goes per write.
    for n in focal1 focal2 focal3; do
      dc exec -d "$n" sh -c "sleep $(( warmup + 20 )); perf record -F 499 --call-graph fp -p 1 -o /data/perf.data -- sleep 20 > /data/perf.log 2>&1" || true
    done
    cpu_stat before > "$out/$report.cpu.txt"
    net_stat before > "$out/$report.net.txt"
    if ! timeout $(( (seconds + warmup) * 4 + 120 )) docker compose -f compose.yaml --profile focal \
      exec -T -e "FOCAL_LOAD_WRITES_CSV=/reports/$report.csv" focal-load \
      focal-load --shape /reports/shape.yaml --out "/reports/$report"; then
      evidence
      exit 1
    fi
    cpu_stat after >> "$out/$report.cpu.txt"
    net_stat after >> "$out/$report.net.txt"
    for n in focal1 focal2 focal3; do
      echo "::group::$n by thread and function (focal at $rate/s)"
      dc exec -T "$n" sh -c "cat /data/perf.log; perf report -i /data/perf.data --stdio --no-children --sort comm -g none 2>/dev/null | grep -v '^#' | grep -v '^\$' | head -20" || true
      dc exec -T "$n" sh -c "perf report -i /data/perf.data --stdio --children --sort comm,symbol -g none --percent-limit 1 2>/dev/null | grep -v '^#' | grep -v '^\$' | head -160" || true
      dc exec -T "$n" sh -c "perf report -i /data/perf.data --stdio --no-children --sort comm,symbol -g none --percent-limit 0.3 2>/dev/null | grep -v '^#' | grep -v '^\$' | head -90" || true
      # DIAG: the session owner's callers of its heaviest symbols (the range store's point lookup,
      # memcpy, malloc), as the frame-pointer call graphs recorded them
      dc exec -T "$n" sh -c "perf report -i /data/perf.data --stdio --no-children --comm focal-node-sess --sort symbol -g caller,0.5,callee,function,percent --percent-limit 1.5 2>/dev/null | grep -v '^#' | grep -v '^\$' | head -600" || true
      # DIAG: every sample's whole stack, folded root first under its thread's name, counted
      # (Brendan Gregg's collapsed format), for attributing a symbol to every caller it has.
      dc exec -T "$n" sh -c "perf script -i /data/perf.data -F comm,ip,sym 2>/dev/null" \
        | awk '
          function emit(  s, i) { if (n == 0) return; s = comm; for (i = n; i >= 1; i--) s = s ";" f[i]; c[s]++; n = 0 }
          /^\t/ { n++; sym = $2; for (j = 3; j <= NF; j++) sym = sym " " $j; f[n] = sym; next }
          /^[ \t]*$/ { emit(); next }
          { emit(); comm = $1; next }
          END { emit(); for (k in c) print k, c[k] }
        ' > "$out/$report.$n.folded" || true
      wc -l "$out/$report.$n.folded" || true
      echo "::endgroup::"
    done
    dc exec -T focal-load cat "/reports/$report" > "$out/$report"
    # DIAG: every node's log, whatever the run's outcome: a session that stopped mid-run leaves
    # nothing else (2026-10-14, a follower's session stopped at 4,000/s with no trace).
    for n in focal1 focal2 focal3; do
      dc logs --no-color "$n" > "$out/$report.$n.log" 2>&1 || true
    done
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
    dc exec -T generator focal-compare --system "$system" --durable --mode "$mode" \
      --endpoints "$endpoints" --rate "$rate" --payload "$payload" \
      --seconds "$seconds" --warmup "$warmup" --out "/reports/$report"
  }
  cpu_stat before > "$out/$report.cpu.txt"
  net_stat before > "$out/$report.net.txt"
  retry 60 generate || { evidence; exit 1; }
  cpu_stat after >> "$out/$report.cpu.txt"
  net_stat after >> "$out/$report.net.txt"
  dc exec -T generator cat "/reports/$report" > "$out/$report"
fi
echo "wrote $out/$report"
