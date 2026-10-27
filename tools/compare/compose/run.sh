#!/usr/bin/env bash
# One measured run of the comparison (docs/qualification/competitive-p99.md):
# SYSTEM's three nodes up with every acknowledged write on disk at a quorum
# (the plan's durable row), the generator on the bench network driving it
# open-loop at RATE a second for SECONDS after WARMUP, the report copied to
# OUT, and everything taken down with its volumes. Every wait is bounded.
#
#   run.sh SYSTEM RATE SECONDS WARMUP PAYLOAD OUT
#   SYSTEM: focal | focal-bus | focal-bus-fates | kafka | nats | redis
#   MODE (environment): append (default), each record stored; or deliver,
#   each record also taken by a consumer and acknowledged by it durably, a
#   work queue's message end to end (focal: its seven-write lifecycle, the
#   consumer an enrolled client of its own; RATE is messages a second).
#   RATE 0: no schedule, each answer sending the next record, the window held
#   full (a closed loop: Kafka's producer-perf-test at --throughput -1), for a
#   system's ceiling at that window.
#   AGENTS (environment), deliver mode on focal: the messages go among that many enrolled
#   agents, each a client of its own (focal-load's `agents`), where unset one issuer and one
#   consumer; the report's file names it.
#   MODE work, focal alone (no other system has its lifecycle): each claim done among three of
#   AGENTS enrolled agents, 3 at least, an issuer, a worker and an evaluator, its eleven writes
#   each learned of from the next agent's inbox (focal-load's `lifecycle: work`); RATE is
#   claims a second.
#   FAULTS (environment), focal: what the cluster suffers while the load runs, `;`-separated,
#   each timed from the load's start (its warm-up included), every one undone by the run's end
#   and logged with its time (`<report>.faults.txt`):
#     netem=ARGS        `tc qdisc ... netem ARGS` on every node's interface for the whole run,
#                       e.g. `netem=delay 80ms 30ms loss 3% reorder 5%`
#     netem-peers=ARGS  as netem, on only what each node sends the other nodes (replication,
#                       content copies, liveness): a `prio` root whose first band is netem, the
#                       peers' addresses filtered to it, the rest of the node's datagrams sent as
#                       they come
#     netem-clients=ARGS  as netem-peers, on only what each node sends the load's clients
#     kill=NODE@S+D     NODE killed S seconds in, started again D later. NODE is focal1, focal2 or
#                       focal3, or a role in the bench session's placement as the fault begins
#                       (`focal inspect placement`): `copy`, a node holding one of its content
#                       copies other than its preferred leader, or `leader`, its preferred leader.
#                       A role makes a run hit what it names whichever node the run seated there.
#     isolate=NODE@S+D  NODE (as kill's) cut off from every peer and client (all its datagrams
#                       lost) S seconds in, for D seconds
#   The report's file names the run as faulted (`-faults`).
#   INFLIGHT (environment): the generator's window, the requests out at most
#   (focal-compare's own default, 4096, where unset); a run given one names it
#   in its report's file.
set -euo pipefail
cd "$(dirname "$0")"
system="$1"; rate="$2"; seconds="$3"; warmup="$4"; payload="$5"; out="$6"
mode="${MODE:-append}"
window_args=()
window=""
if [ -n "${INFLIGHT:-}" ]; then window_args=(--inflight "$INFLIGHT"); window="-w$INFLIGHT"; fi
case "$mode" in
  append) suffix="" ;;
  deliver) suffix="-deliver${AGENTS:+-agents$AGENTS}" ;;
  work)
    if [ "$system" != focal ]; then echo "work is focal's lifecycle: no $system run" >&2; exit 2; fi
    if [ "${AGENTS:-0}" -lt 3 ]; then echo "work goes among AGENTS agents, 3 at least" >&2; exit 2; fi
    suffix="-work-agents$AGENTS" ;;
  *) echo "unknown mode $mode" >&2; exit 2 ;;
esac
mkdir -p "$out"
export NATS_SYNC=always KAFKA_FLUSH=1 REDIS_AOF=yes REDIS_FSYNC=always
# focal-bus-fates: the bus's nodes each stating its own fate (the replicated class, core step
# R-8). On one host the fates are declared, not independent: the run measures the mechanism, and
# its report says which class each acknowledgement achieved.
profile="$system"
if [ "$system" = focal-bus-fates ]; then
  profile=focal-bus
  export BUS_FATES="--fate 1=1 --fate 2=2 --fate 3=3"
fi
dc() { docker compose -f compose.yaml --profile "$profile" "$@"; }
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
  if [ "$profile" = focal-bus ]; then
    for n in focal-bus1 focal-bus2 focal-bus3; do
      echo "--- $n log"
      dc logs --no-color --tail 120 "$n" || true
    done
  fi
  if [ "$system" = focal ]; then
    for n in focal1 focal2 focal3; do
      echo "--- $n replicas"
      timeout 30 docker compose -f compose.yaml --profile focal exec -T "$n" focal --data-dir /data inspect replicas --replicas || true
      # Its metrics: the listener's connections, refusals and stream pool among them.
      echo "--- $n metrics"
      timeout 30 docker compose -f compose.yaml --profile focal exec -T "$n" focal --data-dir /data inspect node --metrics \
        | grep -E '^focal_(listener|peer|overload|admission|session)' || true
      echo "--- $n log"
      dc logs --no-color --tail 120 "$n" || true
    done
  fi
  echo "::endgroup::"
}
cleanup() { dc down -v --remove-orphans >/dev/null 2>&1 || true; }
# The FAULTS a run suffers, applied as its load starts: the shaping every node keeps, and each
# timed fault in a background job of its own whose PID is kept, so the load's end stops any not
# yet done and undoes what it did. Each step is logged with the seconds since the load began.
fault_pids=()
fault_shaping=""
# Which of a node's datagrams the shaping takes: all (empty), its peers' (netem-peers) or the
# load's clients' (netem-clients).
fault_directed=""
fault_started=0
fault_log() { echo "$(( $(date +%s) - fault_started ))s $*" >> "$out/$report.faults.txt"; }
fault_shape() {
  local node=$1 how=$2
  if [ -n "$how" ]; then
    dc exec -T "$node" tc qdisc replace dev eth0 root netem $how
  else
    dc exec -T "$node" tc qdisc del dev eth0 root 2>/dev/null || true
  fi
}
# The addresses a directed shaping takes NODE's datagrams to, one a line: the other nodes', or the
# load's.
fault_targets() {
  local node=$1 peer
  if [ "$fault_directed" = netem-peers ]; then
    for peer in focal1 focal2 focal3; do
      [ "$peer" = "$node" ] || container_address "$peer"
    done
  else
    container_address focal-load
  fi
}
# NODE shaped as the run's shaping says again (after a restart or an isolation): all its datagrams,
# or those to its directed targets; nothing where the run shapes none.
fault_reshape() {
  local node=$1
  [ -n "$fault_shaping" ] || { fault_shape "$node" ""; return 0; }
  if [ -n "$fault_directed" ]; then
    local targets=()
    mapfile -t targets < <(fault_targets "$node")
    fault_shape_to "$node" "$fault_shaping" "${targets[@]}"
  else
    fault_shape "$node" "$fault_shaping"
  fi
}
# A container's address on the compose network: what a directed shaping filters by.
container_address() {
  docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}' \
    "$(dc ps -q "$1")" 2>/dev/null | awk '{print $1}'
}
# netem on only the datagrams NODE sends the addresses given: a `prio` root of two bands, every
# priority mapped to the second (sent as it comes), the first band's child netem HOW, and a u32
# filter sending each address's datagrams to the first. Undone as the whole shaping is
# (`fault_shape NODE ""`, the root deleted with its bands and filters).
fault_shape_to() {
  local node=$1 how=$2 address
  shift 2
  dc exec -T "$node" tc qdisc replace dev eth0 root handle 1: prio bands 2 \
    priomap 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1
  dc exec -T "$node" tc qdisc add dev eth0 parent 1:1 handle 10: netem $how
  for address in "$@"; do
    dc exec -T "$node" tc filter add dev eth0 parent 1: protocol ip prio 1 u32 \
      match ip dst "$address/32" flowid 1:1
  done
}
# The container a fault's NODE names: itself, or the node that holds the role in the bench
# session's placement now, by the zone it is configured in (focalN.yaml: a, b, c), read from the
# first node that answers. Nothing where no node answers or none holds the role.
fault_node() {
  local role=$1 n
  case "$role" in focal1 | focal2 | focal3) echo "$role"; return 0 ;; esac
  for n in focal1 focal2 focal3; do
    dc exec -T "$n" focal --data-dir /data inspect placement 2>/dev/null | python3 -c '
import json, sys
role = sys.argv[1]
document = json.load(sys.stdin)
placement = document.get("result", document)
placement = placement.get("placement", placement)
zones = {node["node"]: node.get("zone") for partition in placement["partitions"] for node in partition["nodes"]}
for partition in placement["partitions"]:
    for session in partition["sessions"]:
        leader = session["preferred_leader"]
        held = [leader] if role == "leader" else sorted(c for c in session["content_copies"] if c != leader)
        for node in held:
            if zones.get(node) in ("a", "b", "c"):
                print("focal%d" % ("abc".index(zones[node]) + 1), session["session"], session["content_copies"], leader)
                sys.exit(0)
sys.exit(1)
' "$role" && return 0
  done
  return 1
}
faults_start() {
  [ -n "${FAULTS:-}" ] || return 0
  fault_started=$(date +%s)
  : > "$out/$report.faults.txt"
  local spec what node at down
  IFS=';' read -ra specs <<< "$FAULTS"
  for spec in "${specs[@]}"; do
    case "$spec" in
      netem=*)
        fault_shaping=${spec#netem=}
        for node in focal1 focal2 focal3; do fault_shape "$node" "$fault_shaping"; done
        fault_log "netem on every node: $fault_shaping"
        ;;
      netem-peers=* | netem-clients=*)
        fault_directed=${spec%%=*}; fault_shaping=${spec#*=}
        for node in focal1 focal2 focal3; do
          fault_reshape "$node"
          fault_log "$fault_directed on $node to $(fault_targets "$node" | tr '\n' ' '): $fault_shaping"
        done
        ;;
      kill=*@*+* | isolate=*@*+*)
        what=${spec%%=*}; node=${spec#*=}; node=${node%@*}
        at=${spec#*@}; at=${at%+*}; down=${spec##*+}
        case "$node" in focal1 | focal2 | focal3 | copy | leader) ;; *) echo "FAULTS: no node $node" >&2; return 2 ;; esac
        (
          sleep "$at"
          role=$node
          if ! found=$(fault_node "$role"); then
            fault_log "no node holds $role: $what not done"
            exit 0
          fi
          node=${found%% *}
          [ "$role" = "$node" ] || fault_log "$role is $node (session, copies, leader: ${found#* })"
          if [ "$what" = kill ]; then
            dc kill "$node" >/dev/null 2>&1; fault_log "$node killed"
            sleep "$down"
            dc start "$node" >/dev/null 2>&1
            fault_reshape "$node"
            fault_log "$node started"
          else
            fault_shape "$node" "loss 100%"; fault_log "$node isolated"
            sleep "$down"
            fault_reshape "$node"; fault_log "$node rejoined"
          fi
        ) &
        fault_pids+=("$!")
        ;;
      *) echo "FAULTS: no fault $spec" >&2; return 2 ;;
    esac
  done
}
# The load is over: a fault still waiting is stopped, a node it left down started again and one
# it left cut off joined again, so the run's evidence reads every node.
faults_end() {
  [ -n "${FAULTS:-}" ] || return 0
  local pid node
  for pid in "${fault_pids[@]}"; do kill "$pid" 2>/dev/null || true; done
  wait "${fault_pids[@]}" 2>/dev/null || true
  for node in focal1 focal2 focal3; do
    if [ "$(docker inspect -f '{{.State.Running}}' "$(dc ps -a -q "$node")" 2>/dev/null)" != true ]; then
      dc start "$node" >/dev/null 2>&1 || true; fault_log "$node started at the end"
    fi
    fault_shape "$node" ""
  done
  fault_log "every fault undone"
}
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
# The machine's disks as its kernel counts them (/proc/diskstats), before and
# after the load, each line `PHASE DEVICE KEY VALUE`: writes completed, sectors
# written, the milliseconds the device was busy, and its flushes completed
# (Linux 5.5 and later), for whether a run measured its disk rather than its
# system: the three nodes of a run share one. Prints nothing off Linux.
disk_stat() {
  local phase=$1
  [ -r /proc/diskstats ] || return 0
  awk -v p="$phase" '$3 !~ /^(loop|ram|zram|dm-)/ && NF >= 14 {
    print p, $3, "writes", $8; print p, $3, "sectors_written", $10; print p, $3, "busy_ms", $13
    if (NF >= 20) print p, $3, "flushes", $19
  }' /proc/diskstats
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
report="$system-$rate-$payload$window$suffix${FAULTS:+-faults}.json"

case "$system" in
  focal)
    if [ "$rate" = 0 ] || [ -n "$window" ]; then
      echo "focal-load runs on its own schedule and callers: no closed loop or window here" >&2
      exit 2
    fi
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
    if [ "$mode" = work ]; then
      # The agents are homed over the cluster's nodes, as its clients would be: each reads, and
      # waits on its inbox, at its home and writes at the session's leader (focal-load's
      # `homes`), where every agent dialed the founder its invitation names, and the leader
      # served every read beside every write.
      homes=$(dc exec -T focal1 focal --data-dir /data list nodes | python3 -c '
import json, sys
nodes = sorted(json.load(sys.stdin)["result"]["nodes"], key=lambda node: node["node"])
if not nodes:
    sys.exit("the cluster lists no nodes")
print("homes:")
for node in nodes:
    print("  - endpoint: " + json.dumps(node["advertise"]))
    print("    server_name: " + json.dumps(node["server_name"]))
')
      lifecycle="lifecycle: work
agents: $AGENTS
agent_enrollments: /client/agents
$homes"
    elif [ "$mode" = deliver ] && [ -n "${AGENTS:-}" ]; then
      lifecycle="lifecycle: deliver
agents: $AGENTS
agent_enrollments: /client/agents"
    elif [ "$mode" = deliver ]; then
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
    # A deliver run's last message may then wait on each side's step twice, for
    # its post and its testament, each up to focal-load's WAIT (two minutes,
    # tools/load/src/watch.rs) before it counts the message undelivered: the
    # bound holds both, so a message the events never show ends the run with
    # its report saying so, never killed without one.
    bound=$(( (seconds + warmup) * 4 + 120 ))
    if [ "$mode" = deliver ]; then
      bound=$(( bound + 2 * 120 ))
    elif [ "$mode" = work ]; then
      # A work claim's agents wait four times: the post, the testament, the evaluation due and
      # the settlement.
      bound=$(( bound + 4 * 120 ))
    fi
    cpu_stat before > "$out/$report.cpu.txt"
    net_stat before > "$out/$report.net.txt"
    disk_stat before > "$out/$report.disk.txt"
    faults_start
    if ! timeout "$bound" docker compose -f compose.yaml --profile focal \
      exec -T -e "FOCAL_LOAD_WRITES_CSV=/reports/$report.csv" focal-load \
      focal-load --shape /reports/shape.yaml --out "/reports/$report" \
      2> >(tee >(grep DIAGS > "$out/$report.diags") >&2); then
      faults_end
      evidence
      exit 1
    fi
    faults_end
    cpu_stat after >> "$out/$report.cpu.txt"
    net_stat after >> "$out/$report.net.txt"
    disk_stat after >> "$out/$report.disk.txt"
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
  focal-bus | focal-bus-fates) endpoints=focal-bus1:7443,focal-bus2:7443,focal-bus3:7443 ;;
  kafka) endpoints=kafka1:9092,kafka2:9092,kafka3:9092 ;;
  nats) endpoints=nats1:4222,nats2:4222,nats3:4222 ;;
  redis) endpoints=redis1:6379,redis2:6379,redis3:6379 ;;
  *) echo "unknown system $system" >&2; exit 2 ;;
esac

if [ "$system" != focal ]; then
  # The cluster forms its quorum on its own clock: the generator's first
  # connection is asked again until it succeeds, as an operator's client would.
  # focal's bus reads the identity directory its nodes share.
  pki=()
  generator_system="$system"
  if [ "$profile" = focal-bus ]; then pki=(--pki /pki); generator_system=focal-bus; fi
  generate() {
    dc exec -T generator focal-compare --system "$generator_system" --durable --mode "$mode" \
      --endpoints "$endpoints" --rate "$rate" --payload "$payload" \
      --seconds "$seconds" --warmup "$warmup" "${window_args[@]}" "${pki[@]}" --out "/reports/$report"
  }
  cpu_stat before > "$out/$report.cpu.txt"
  net_stat before > "$out/$report.net.txt"
  disk_stat before > "$out/$report.disk.txt"
  retry 60 generate || { evidence; exit 1; }
  cpu_stat after >> "$out/$report.cpu.txt"
  net_stat after >> "$out/$report.net.txt"
  disk_stat after >> "$out/$report.disk.txt"
  dc exec -T generator cat "/reports/$report" > "$out/$report"
  # Each bus node's log, whatever the run's outcome: stopped first (a node stops on its signal and
  # states what its member did: its elections, its answers that waited), then read, for where a
  # tail or an unanswered record came from.
  if [ "$profile" = focal-bus ]; then
    dc stop focal-bus1 focal-bus2 focal-bus3 >/dev/null 2>&1 || true
    for n in focal-bus1 focal-bus2 focal-bus3; do
      dc logs --no-color "$n" > "$out/$report.$n.log" 2>&1 || true
    done
  fi
fi
echo "wrote $out/$report"
