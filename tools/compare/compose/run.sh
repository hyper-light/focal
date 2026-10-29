#!/usr/bin/env bash
# One measured run of the comparison (docs/qualification/competitive-p99.md):
# SYSTEM's nodes (three; focal's NODES) up with every acknowledged write on disk at a quorum
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
#   NODES (environment), focal: the nodes its cluster runs, focal1 to focalN over zones a, b and c
#   (focal-nodes.sh, written beside compose.yaml for every run: 3 to 203, 3 where unset, as every
#   competitor runs three). Past three, nodes that would hold more CPUs than the host has beside
#   the generator share what it leaves (fractions of a CPU). A run of other than three names them
#   in its report's file (`-nodes33`).
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
#     kill=NODES@S+D    NODES killed S seconds in, all at once, started again D later. NODES is
#                       a node (focalK, K of 1 to NODES), a list of them (`focal4,focal7`), a
#                       count (`12`: any 12 of the nodes, each as likely as another, chosen as the
#                       fault begins, the same ones again for the same FAULT_SEED, which the log
#                       states), or a role in the bench session's placement as the fault begins
#                       (`focal inspect placement`): `copy`, a node holding one of its content
#                       copies other than its preferred leader, or `leader`, its preferred leader.
#                       A role makes a run hit what it names whichever node the run seated there.
#     isolate=NODES@S+D NODES (as kill's) each cut off from every peer and client (all its
#                       datagrams lost) S seconds in, for D seconds
#   The report's file names the run as faulted (`-faults`).
#   INFLIGHT (environment): the generator's window, the requests out at most
#   (focal-compare's own default, 4096, where unset); a run given one names it
#   in its report's file.
set -euo pipefail
# bash 4.4 or later: `mapfile`, `wait -n`, and empty arrays under `set -u` (macOS's own is 3.2).
if [ "${BASH_VERSINFO[0]}" -lt 4 ] || { [ "${BASH_VERSINFO[0]}" -eq 4 ] && [ "${BASH_VERSINFO[1]}" -lt 4 ]; }; then
  echo "run.sh needs bash 4.4 or later, not $BASH_VERSION" >&2
  exit 2
fi
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
# Every compose call takes focal's nodes beside compose.yaml (focal-nodes.yaml, below), whatever
# the system, so each run's model holds every service a run of this checkout starts.
compose=(docker compose -f compose.yaml -f focal-nodes.yaml)
dc() { "${compose[@]}" --profile "$profile" "$@"; }
# The host's CPUs, as Docker counts them (its VM's where it runs in one): focal's nodes past three
# are fit to them (focal-nodes.sh), and work done for every node at once runs as many at a time.
host_cpus="${HOST_CPUS:-$(docker info --format '{{.NCPU}}')}"
# focal's nodes for this run: the last run's taken down first with the file that made them (a node
# past this run's count left neither running nor holding its volume), then written for NODES and
# checked by compose before anything starts.
if [ -s focal-nodes.yaml ]; then
  "${compose[@]}" --profile focal down -v --remove-orphans >/dev/null 2>&1 || true
fi
HOST_CPUS="$host_cpus" sh focal-nodes.sh > focal-nodes.yaml.new
mv focal-nodes.yaml.new focal-nodes.yaml
dc config --quiet
# Each node as the model states it, in order (cluster.py nodes): NAME ADDRESS ZONE a line.
table=$("${compose[@]}" --profile focal config --format json | python3 cluster.py nodes)
mapfile -t focal_nodes < <(awk '{ print $1 }' <<< "$table")
# CALL NODE ARGS... for each NODE before `--` (`each focal4 focal7 -- fault_shape "loss 100%"`),
# as many at once as the host has CPUs, each a `docker compose` call of its own; fails when any
# did. A subshell, so it waits on its own calls alone.
each() (
  local nodes=() running=0 failed=0 node
  while [ "$#" -gt 0 ] && [ "$1" != -- ]; do nodes+=("$1"); shift; done
  shift
  for node in "${nodes[@]}"; do
    if [ "$running" -ge "$host_cpus" ]; then
      wait -n || failed=1
      running=$((running - 1))
    fi
    "$1" "$node" "${@:2}" &
    running=$((running + 1))
  done
  while [ "$running" -gt 0 ]; do
    wait -n || failed=1
    running=$((running - 1))
  done
  return "$failed"
)
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
    local held n
    held=$(mktemp -d)
    each "${focal_nodes[@]}" -- node_evidence "$held" || true
    for n in "${focal_nodes[@]}"; do cat "$held/$n" 2>/dev/null || true; done
    rm -rf "$held"
  fi
  echo "::endgroup::"
}
# NODE's replicas, metrics and log for the evidence, into DIR/NODE.
node_evidence() {
  local n=$1 dir=$2
  {
    echo "--- $n replicas"
    timeout 30 "${compose[@]}" --profile focal exec -T "$n" focal --data-dir /data inspect replicas --replicas || true
    # Its metrics: the listener's connections, refusals and stream pool among them.
    echo "--- $n metrics"
    timeout 30 "${compose[@]}" --profile focal exec -T "$n" focal --data-dir /data inspect node --metrics \
      | grep -E '^focal_(listener|peer|overload|admission|session)' || true
    echo "--- $n log"
    timeout 30 "${compose[@]}" --profile focal logs --no-color --tail 120 "$n" || true
  } > "$dir/$n" 2>&1
}
# NODE's replicas as they report themselves, the cost of every checkpoint they wrote by stage among
# them, for where the tail's time went; and its metrics: its log's flush and commit-wait quantiles
# and its owners' longest periods, for whether a stall was the disk's.
node_report() {
  local n=$1
  timeout 30 "${compose[@]}" --profile focal exec -T "$n" focal --data-dir /data inspect replicas --replicas \
    > "$out/$report.$n.replicas.json" 2>/dev/null || true
  timeout 30 "${compose[@]}" --profile focal exec -T "$n" focal --data-dir /data inspect node --metrics \
    > "$out/$report.$n.metrics.txt" 2>/dev/null || true
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
# What a count of nodes (`kill=12@...`) draws its nodes by: FAULT_SEED, else one drawn here, logged.
fault_seed="${FAULT_SEED:-}"
fault_log() { echo "$(( $(date +%s) - fault_started ))s $*" >> "$out/$report.faults.txt"; }
fault_shape() {
  local node=$1 how=$2
  if [ -n "$how" ]; then
    dc exec -T "$node" tc qdisc replace dev eth0 root netem $how
  else
    dc exec -T "$node" tc qdisc del dev eth0 root 2>/dev/null || true
  fi
}
# The addresses a directed shaping takes NODE's datagrams to, one a line: the other nodes', as the
# model gives them (a node down has its address still), or the load's.
fault_targets() {
  local node=$1
  if [ "$fault_directed" = netem-peers ]; then
    awk -v node="$node" '$1 != node { print $2 }' <<< "$table"
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
# NODE given the run's directed shaping, and the log told where it goes.
fault_direct() {
  local node=$1
  fault_reshape "$node"
  fault_log "$fault_directed on $node to $(fault_targets "$node" | tr '\n' ' '): $fault_shaping"
}
# A container's address on the compose network: what a directed shaping filters by.
container_address() {
  docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}' \
    "$(dc ps -q "$1")" 2>/dev/null | awk '{print $1}'
}
# netem on only the datagrams NODE sends the addresses given: a `prio` root of two bands, every
# priority mapped to the second (sent as it comes), the first band's child netem HOW, and a u32
# filter sending each address's datagrams to the first. One call into the node however many the
# addresses. Undone as the whole shaping is (`fault_shape NODE ""`, the root deleted with its
# bands and filters).
fault_shape_to() {
  local node=$1 how=$2 address script
  shift 2
  script="tc qdisc replace dev eth0 root handle 1: prio bands 2 priomap 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1"
  script+=" && tc qdisc add dev eth0 parent 1:1 handle 10: netem $how"
  for address in "$@"; do
    script+=" && tc filter add dev eth0 parent 1: protocol ip prio 1 u32 match ip dst $address/32 flowid 1:1"
  done
  dc exec -T "$node" sh -c "$script"
}
# The node that holds ROLE in the bench session's placement now, from the first node that answers,
# and what it holds (cluster.py role): NAME SESSION COPIES LEADER. Nothing where no node answers or
# none holds the role.
fault_node() {
  local role=$1 n placement
  for n in "${focal_nodes[@]}"; do
    placement=$(timeout 30 "${compose[@]}" --profile focal exec -T "$n" focal --data-dir /data inspect placement 2>/dev/null) || continue
    python3 cluster.py role "$role" "$table" <<< "$placement"
    return
  done
  return 1
}
# Whether a fault's NODES can be taken: a node of the cluster, a list of them none twice, a count of
# 1 to the cluster's nodes, or a role.
fault_nodes_valid() {
  local nodes=$1 node seen=" " names=()
  case "$nodes" in
    copy | leader) return 0 ;;
    '' | *[!0-9]*) ;;
    *) [ "$nodes" -ge 1 ] && [ "$nodes" -le "${#focal_nodes[@]}" ]; return ;;
  esac
  IFS=, read -ra names <<< "$nodes"
  [ "${#names[@]}" -gt 0 ] || return 1
  for node in "${names[@]}"; do
    awk -v node="$node" '$1 == node { found = 1 } END { exit !found }' <<< "$table" || return 1
    case "$seen" in *" $node "*) return 1 ;; esac
    seen+="$node "
  done
}
# The nodes a fault's NODES names as it begins, space-separated: a list as given, a count drawn by
# the run's seed and the fault's place in FAULTS (INDEX), a role as the placement holds it. How a
# count or a role came out is logged. Fails where no node holds the role.
fault_nodes() {
  local nodes=$1 index=$2 found
  case "$nodes" in
    copy | leader)
      found=$(fault_node "$nodes") || return 1
      fault_log "$nodes is ${found%% *} (session, copies, leader: ${found#* })"
      echo "${found%% *}"
      ;;
    *[!0-9]*) echo "${nodes//,/ }" ;;
    *)
      found=$(python3 cluster.py pick "$fault_seed:$index" "$nodes" "${focal_nodes[@]}") || return 1
      fault_log "any $nodes of ${#focal_nodes[@]} (seed $fault_seed, fault $index): $found"
      echo "$found"
      ;;
  esac
}
# Every FAULTS spec checked before the cluster starts, so a run never founds a cluster to refuse
# its faults, nor starts half of them; the seed a count of nodes draws by, drawn where none is set.
faults_check() {
  [ -n "${FAULTS:-}" ] || return 0
  local spec nodes specs=()
  IFS=';' read -ra specs <<< "$FAULTS"
  for spec in "${specs[@]}"; do
    case "$spec" in
      netem=* | netem-peers=* | netem-clients=*) ;;
      kill=*@*+* | isolate=*@*+*)
        nodes=${spec#*=}; nodes=${nodes%@*}
        if ! fault_nodes_valid "$nodes"; then
          echo "FAULTS: no nodes $nodes among ${focal_nodes[*]}" >&2
          return 2
        fi
        case "$nodes" in
          '' | *[!0-9]*) ;;
          *) if [ -z "$fault_seed" ]; then fault_seed=$(od -An -N4 -tu4 /dev/urandom | tr -d ' '); fi ;;
        esac
        ;;
      *) echo "FAULTS: no fault $spec" >&2; return 2 ;;
    esac
  done
}
faults_start() {
  [ -n "${FAULTS:-}" ] || return 0
  fault_started=$(date +%s)
  : > "$out/$report.faults.txt"
  local spec what nodes at down index=0 specs=()
  IFS=';' read -ra specs <<< "$FAULTS"
  if [ -n "$fault_seed" ]; then fault_log "seed $fault_seed (FAULT_SEED)"; fi
  for spec in "${specs[@]}"; do
    index=$((index + 1))
    case "$spec" in
      netem=*)
        fault_shaping=${spec#netem=}
        each "${focal_nodes[@]}" -- fault_shape "$fault_shaping"
        fault_log "netem on every node: $fault_shaping"
        ;;
      netem-peers=* | netem-clients=*)
        fault_directed=${spec%%=*}; fault_shaping=${spec#*=}
        each "${focal_nodes[@]}" -- fault_direct
        ;;
      kill=*@*+* | isolate=*@*+*)
        what=${spec%%=*}; nodes=${spec#*=}; nodes=${nodes%@*}
        at=${spec#*@}; at=${at%+*}; down=${spec##*+}
        (
          sleep "$at"
          if ! found=$(fault_nodes "$nodes" "$index"); then
            fault_log "no node holds $nodes: $what not done"
            exit 0
          fi
          read -ra hit <<< "$found"
          if [ "$what" = kill ]; then
            if dc kill "${hit[@]}" >/dev/null 2>&1; then
              fault_log "${hit[*]} killed"
            else
              fault_log "${hit[*]}: kill refused"
            fi
            sleep "$down"
            if dc start "${hit[@]}" >/dev/null 2>&1; then
              each "${hit[@]}" -- fault_reshape || true
              fault_log "${hit[*]} started"
            else
              fault_log "${hit[*]}: start refused"
            fi
          else
            each "${hit[@]}" -- fault_shape "loss 100%" || true
            fault_log "${hit[*]} isolated"
            sleep "$down"
            each "${hit[@]}" -- fault_reshape || true
            fault_log "${hit[*]} rejoined"
          fi
        ) &
        fault_pids+=("$!")
        ;;
    esac
  done
}
# The load is over: a fault still waiting is stopped, nodes it left down started again and those
# it left cut off joined again, so the run's evidence reads every node.
faults_end() {
  [ -n "${FAULTS:-}" ] || return 0
  local pid down=()
  for pid in "${fault_pids[@]}"; do kill "$pid" 2>/dev/null || true; done
  if [ "${#fault_pids[@]}" -gt 0 ]; then wait "${fault_pids[@]}" 2>/dev/null || true; fi
  mapfile -t down < <(dc ps -a --format '{{.Service}} {{.State}}' \
    | awk -v nodes=" ${focal_nodes[*]} " '$2 != "running" && index(nodes, " " $1 " ") { print $1 }')
  if [ "${#down[@]}" -gt 0 ]; then
    dc start "${down[@]}" >/dev/null 2>&1 || true
    fault_log "${down[*]} started at the end"
  fi
  each "${focal_nodes[@]}" -- fault_shape "" || true
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
if [ "$system" = focal ]; then faults_check; fi
# A focal run of other than three nodes names them in its report's file.
nodes_suffix=""
if [ "$system" = focal ] && [ "${#focal_nodes[@]}" -ne 3 ]; then nodes_suffix="-nodes${#focal_nodes[@]}"; fi
trap cleanup EXIT
cleanup
pull_images
dc up -d
report="$system-$rate-$payload$window$suffix$nodes_suffix${FAULTS:+-faults}.json"

case "$system" in
  focal)
    if [ "$rate" = 0 ] || [ -n "$window" ]; then
      echo "focal-load runs on its own schedule and callers: no closed loop or window here" >&2
      exit 2
    fi
    bash focal-bootstrap.sh
    # The cluster the run measured: its nodes as written (their addresses, zones and limits), the
    # contacts they committed and the directory's placement of them and of the bench session.
    cp focal-nodes.yaml "$out/$report.cluster.yaml"
    timeout 30 "${compose[@]}" --profile focal exec -T focal1 focal --data-dir /data list nodes \
      > "$out/$report.contacts.json" 2>/dev/null || true
    timeout 30 "${compose[@]}" --profile focal exec -T focal1 focal --data-dir /data inspect placement \
      > "$out/$report.placement.json" 2>/dev/null || true
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
    if ! timeout "$bound" "${compose[@]}" --profile focal \
      exec -T -e "FOCAL_LOAD_WRITES_CSV=/reports/$report.csv" focal-load \
      focal-load --shape /reports/shape.yaml --out "/reports/$report"; then
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
    each "${focal_nodes[@]}" -- node_report || true
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
