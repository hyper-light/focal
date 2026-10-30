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
#   AGENTS enrolled agents, an issuer, a worker and an evaluator, its eleven writes each learned
#   of from the next agent's inbox (focal-load's `lifecycle: work`); RATE is claims a second.
#   Every session the cluster holds is a ledger driven at once (focal-load's `ledgers`), each
#   its share of the agents and the rate, so AGENTS is three a session at least (3 at three
#   nodes, 33 at 33).
#   NODES (environment), focal: the nodes its cluster runs, focal1 to focalN over zones a, b and c
#   (focal-nodes.sh, written beside compose.yaml for every run: 3 to 203, 3 where unset, as every
#   competitor runs three). Past three, nodes that would hold more CPUs than the host has beside
#   the generator share what it leaves (fractions of a CPU). A run of other than three names them
#   in its report's file (`-nodes33`). The bootstrap places a session for every node of the
#   largest zone, each on three nodes in three zones, so every node holds a replica of one (11 at
#   33 nodes, the founder's alone at three); the run keeps the placement beside its report
#   (`<report>.placement.json`, and by session and node in `<report>.sessions.json`).
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
#     isolate=NODES@S+D NODES (as kill's) each cut off from every peer and client S seconds in,
#                       for D seconds: every datagram it sends lost (netem on its root) and every
#                       one sent it dropped as it arrives (an ingress filter), its process and
#                       state going on, then joined again. focal's transport is QUIC over UDP,
#                       which has no reset to send: a D past the transport's idle timeout (10 s)
#                       drops every connection the nodes hold, at both ends, as a lost path does
#   The report's file names the run as faulted (`-faults`).
#   REST (environment), focal: seconds the cluster is measured at rest before the load, founded,
#   its clients enrolled and the generator idle (`<report>.rest.cpu.txt`, `.rest.io.txt`,
#   `.rest.disk.txt`, as the load's are taken): what the load's counters hold that the cluster
#   spends doing nothing. None where unset.
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
rest="${REST:-0}"
case "$rest" in
  '' | *[!0-9]*) echo "REST must be a whole number of seconds, not '$rest'" >&2; exit 2 ;;
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
# The work mode drives every session the fleet will hold (cluster.py wanted), each claim among
# three of its ledger's agents: refused before anything starts where the agents are too few.
if [ "$mode" = work ]; then
  ledgers_wanted=$(python3 cluster.py wanted "$table")
  # A ledger takes a caller at least, of focal-load's 64 (tools/load/src/shape.rs MAX_CONCURRENCY).
  if [ "$ledgers_wanted" -gt 64 ]; then
    echo "work over ${#focal_nodes[@]} nodes drives $ledgers_wanted sessions, each a caller at least of focal-load's 64: 192 nodes at most" >&2
    exit 2
  fi
  if [ "$AGENTS" -lt $(( 3 * ledgers_wanted )) ]; then
    echo "work over ${#focal_nodes[@]} nodes drives $ledgers_wanted sessions, each among three agents at least: AGENTS must be $(( 3 * ledgers_wanted )) at least, not $AGENTS" >&2
    exit 2
  fi
fi
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
  local n=$1 dir=$2 session
  {
    echo "--- $n replicas"
    timeout 30 "${compose[@]}" --profile focal exec -T "$n" focal --data-dir /data inspect replicas --replicas || true
    for session in $(held_sessions "$n" other); do
      echo "--- $n replicas of $session"
      timeout 30 "${compose[@]}" --profile focal exec -T "$n" focal --data-dir /data inspect replicas --replicas \
        --session "$session" || true
    done
    # Its metrics: the listener's connections, refusals and stream pool among them.
    echo "--- $n metrics"
    timeout 30 "${compose[@]}" --profile focal exec -T "$n" focal --data-dir /data inspect node --metrics \
      | grep -E '^focal_(listener|peer|overload|admission|session)' || true
    echo "--- $n log"
    timeout 30 "${compose[@]}" --profile focal logs --no-color --tail 120 "$n" || true
  } > "$dir/$n" 2>&1
}
# The sessions NODE holds a replica of, one a line, as the run's placement by node records them
# (`<report>.sessions.json`): `other`, those but the bench session; `bench`, the bench session where
# it holds it, or where the record names none of its sessions (before it is written, or a node it
# lists holding none).
held_sessions() {
  python3 -c '
import json, sys
try:
    held = json.load(open(sys.argv[1]))["nodes"].get(sys.argv[3], [])
except (OSError, ValueError, KeyError):
    held = []
if sys.argv[2] == "bench":
    print(sys.argv[4] if not held or sys.argv[4] in held else "")
else:
    print("\n".join(session for session in held if session != sys.argv[4]))' \
    "$out/$report.sessions.json" "$2" "$1" "${bench_session:-}"
}
# NODE's replicas as they report themselves, the cost of every checkpoint they wrote by stage among
# them, for where the tail's time went: the bench session's (its original session, read without
# `--session`) where it holds it, and each other session's it holds in a file of its own named by
# the session's first eight digits; and its metrics: its log's flush and commit-wait quantiles and
# its owners' longest periods, for whether a stall was the disk's.
node_report() {
  local n=$1 session
  if [ -n "$(held_sessions "$n" bench)" ]; then
    timeout 30 "${compose[@]}" --profile focal exec -T "$n" focal --data-dir /data inspect replicas --replicas \
      > "$out/$report.$n.replicas.json" 2>/dev/null || true
  fi
  for session in $(held_sessions "$n" other); do
    timeout 30 "${compose[@]}" --profile focal exec -T "$n" focal --data-dir /data inspect replicas --replicas \
      --session "$session" > "$out/$report.$n.${session:0:8}.replicas.json" 2>/dev/null || true
  done
  timeout 30 "${compose[@]}" --profile focal exec -T "$n" focal --data-dir /data inspect node --metrics \
    > "$out/$report.$n.metrics.txt" 2>/dev/null || true
  # Its health (the placement agent's last error among it) and its log's end, for what a fault left.
  timeout 30 "${compose[@]}" --profile focal exec -T "$n" focal --data-dir /data inspect node --health \
    > "$out/$report.$n.health.json" 2>/dev/null || true
  timeout 30 "${compose[@]}" --profile focal logs --no-color --tail 300 "$n" > "$out/$report.$n.log" 2>&1 || true
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
# Each line the seconds since the load began and the Unix time in milliseconds, which the report's
# CSV of writes is timed against (`run_start_epoch_ns`), so what a fault did is read write by write.
fault_log() {
  local now
  now=$(perl -MTime::HiRes=time -e 'printf "%d", time() * 1e3')
  echo "$(( now / 1000 - fault_started ))s ${now}ms $*" >> "$out/$report.faults.txt"
}
# NODE started again by a fault, waited on until it serves (the bootstrap's probe), and the log told
# when: its time to serve. Bounded as the bootstrap's waits are, 300 probes a second apart.
fault_serving() {
  local node=$1 n=0
  until timeout 10 "${compose[@]}" --profile focal exec -T "$node" focal --data-dir /data inspect node --probe serving >/dev/null 2>&1; do
    n=$((n + 1))
    if [ "$n" -ge 300 ]; then fault_log "$node not serving after $n probes"; return 1; fi
    sleep 1
  done
  fault_log "$node serving"
}
fault_shape() {
  local node=$1 how=$2
  if [ -n "$how" ]; then
    dc exec -T "$node" tc qdisc replace dev eth0 root netem $how
  else
    dc exec -T "$node" sh -c "tc qdisc del dev eth0 root 2>/dev/null; tc qdisc del dev eth0 ingress 2>/dev/null; true" || true
  fi
}
# NODE cut off from every peer and client: every datagram it sends lost, netem on its root, and
# every one sent it dropped as it arrives, an ingress qdisc whose filter matches all. With its root
# alone it went on hearing every peer and client while none heard it; a partition cuts both ways.
fault_isolate() {
  local node=$1
  dc exec -T "$node" sh -c "tc qdisc del dev eth0 ingress 2>/dev/null; tc qdisc replace dev eth0 root netem loss 100% && tc qdisc add dev eth0 handle ffff: ingress && tc filter add dev eth0 parent ffff: protocol all prio 1 matchall action drop"
}
# NODE joined again: its ingress filter gone and the run's shaping, if any, on its root again.
fault_rejoin() {
  local node=$1
  dc exec -T "$node" tc qdisc del dev eth0 ingress 2>/dev/null || true
  fault_reshape "$node"
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
# The node that holds ROLE in the bench session's placement now (the session the load drives, among
# the fleet's), from the first node that answers, and what it holds (cluster.py role): NAME SESSION
# COPIES LEADER. Nothing where no node answers or none holds the role.
fault_node() {
  local role=$1 n placement
  for n in "${focal_nodes[@]}"; do
    placement=$(timeout 30 "${compose[@]}" --profile focal exec -T "$n" focal --data-dir /data inspect placement 2>/dev/null) || continue
    python3 cluster.py role "$role" "$table" "$bench_session" <<< "$placement"
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
            fault_log "${hit[*]} killing"
            if dc kill "${hit[@]}" >/dev/null 2>&1; then
              fault_log "${hit[*]} killed"
            else
              fault_log "${hit[*]}: kill refused"
            fi
            sleep "$down"
            if dc start "${hit[@]}" >/dev/null 2>&1; then
              fault_log "${hit[*]} started"
              each "${hit[@]}" -- fault_reshape || true
              each "${hit[@]}" -- fault_serving || true
            else
              fault_log "${hit[*]}: start refused"
            fi
          else
            fault_log "${hit[*]} isolating"
            each "${hit[@]}" -- fault_isolate || fault_log "${hit[*]}: an isolation refused"
            fault_log "${hit[*]} isolated"
            sleep "$down"
            fault_log "${hit[*]} rejoining"
            each "${hit[@]}" -- fault_rejoin || true
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
# Each of the run's containers' disk as its cgroup (v2) counts it (io.stat, summed over its
# devices): the bytes and the operations it wrote (wbytes, wios), read (rbytes, rios) and
# discarded, each line `PHASE CONTAINER io.KEY VALUE`, read where cpu_stat reads its CPU: what
# focal's nodes wrote themselves, where disk_stat's counters are the whole drive's, whoever else
# writes to it. Prints nothing where the groups cannot be read.
io_stat() {
  local phase=$1 c pid group
  for c in $(dc ps --format '{{.Name}}'); do
    pid=$(docker inspect --format '{{.State.Pid}}' "$c" 2>/dev/null) || continue
    group=$(awk -F: '$1 == "0" && $2 == "" { print $3 }' "/proc/$pid/cgroup" 2>/dev/null) || group=""
    if [ -n "$group" ] && [ -r "/sys/fs/cgroup$group/io.stat" ]; then
      cat "/sys/fs/cgroup$group/io.stat"
    else
      docker exec "$c" cat /sys/fs/cgroup/io.stat 2>/dev/null || true
    fi | awk -v p="$phase" -v c="$c" '
      { for (i = 2; i <= NF; i++) if (split($i, kv, "=") == 2) sum[kv[1]] += kv[2] }
      END { for (k in sum) print p, c, "io." k, sum[k] }'
  done
}
# Each of the run's containers' memory as its cgroup (v2) counts it, read where cpu_stat reads its
# CPU: what it holds (memory.current), the most it held since it started (memory.peak, Linux 5.19
# and later), its limit (memory.max), the times it reached that limit and the kernel killed in it
# (memory.events: max, oom, oom_kill), and its CPU quota (cpu.max, microseconds of each period),
# each line `PHASE CONTAINER KEY VALUE`, a limit of `max` (none) left out; whether Docker found it
# killed for memory (`oom_killed`, 1 or 0, a container that stopped included); and the host's own
# MemTotal and MemAvailable in kilobytes (proc(5)) as `PHASE - KEY VALUE`, read in a container so
# Docker's VM is read where it runs in one.
mem_stat() {
  local phase=$1 c pid group dir killed
  docker run --rm --net none --entrypoint awk "${FOCAL_IMAGE:-focal:bench}" -v p="$phase" \
    '$1 == "MemTotal:" || $1 == "MemAvailable:" { print p, "-", substr($1, 1, length($1) - 1) "_kb", $2 }' \
    /proc/meminfo 2>/dev/null || true
  for c in $(dc ps -a --format '{{.Name}}'); do
    killed=$(docker inspect --format '{{.State.OOMKilled}}' "$c" 2>/dev/null) || continue
    echo "$phase $c oom_killed $([ "$killed" = true ] && echo 1 || echo 0)"
    pid=$(docker inspect --format '{{.State.Pid}}' "$c" 2>/dev/null) || pid=0
    group=""
    if [ "$pid" -gt 0 ]; then
      group=$(awk -F: '$1 == "0" && $2 == "" { print $3 }' "/proc/$pid/cgroup" 2>/dev/null) || group=""
    fi
    dir="/sys/fs/cgroup$group"
    {
      if [ -n "$group" ] && [ -r "$dir/memory.current" ]; then
        for f in memory.current memory.peak memory.max cpu.max; do
          if [ -r "$dir/$f" ]; then echo "$f $(cat "$dir/$f")"; fi
        done
        sed 's/^/memory.events./' "$dir/memory.events"
      elif [ "$pid" -gt 0 ]; then
        docker exec "$c" sh -c 'cd /sys/fs/cgroup && for f in memory.current memory.peak memory.max cpu.max; do if [ -r "$f" ]; then echo "$f $(cat "$f")"; fi; done; sed "s/^/memory.events./" memory.events'
      fi
    } 2>/dev/null | awk -v p="$phase" -v c="$c" '
      $1 == "cpu.max" { if ($2 ~ /^[0-9]+$/) print p, c, "cpu_quota_usec", $2; print p, c, "cpu_period_usec", $3; next }
      $2 ~ /^[0-9]+$/ { key = $1; gsub(/\./, "_", key); print p, c, key, $2 }' || true
  done
}
# The cluster's memory while the load runs, a line a second for at most SECONDS seconds: the
# seconds since it began, what the run's containers held together then (each one's
# memory.current, so their sum is the cluster's at that moment, the generator's included) and the
# host's MemAvailable in kilobytes. Each container's group is found once, before the load (a group
# is named for its container, which keeps it across a restart). Samples nothing where the host's
# groups cannot be read (Docker Desktop's VM).
mem_sampler() {
  local seconds=$1 c pid group n started files=()
  for c in $(dc ps --format '{{.Name}}'); do
    pid=$(docker inspect --format '{{.State.Pid}}' "$c" 2>/dev/null) || continue
    group=$(awk -F: '$1 == "0" && $2 == "" { print $3 }' "/proc/$pid/cgroup" 2>/dev/null) || group=""
    if [ -n "$group" ] && [ -r "/sys/fs/cgroup$group/memory.current" ]; then
      files+=("/sys/fs/cgroup$group/memory.current")
    fi
  done
  [ "${#files[@]}" -gt 0 ] || return 0
  started=$(date +%s)
  for ((n = 0; n < seconds; n++)); do
    echo "$(( $(date +%s) - started )) $(cat "${files[@]}" 2>/dev/null | awk '{ s += $1 } END { print s + 0 }') $(awk '$1 == "MemAvailable:" { print $2 }' /proc/meminfo)"
    sleep 1
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
    # contacts they committed and the directory's placement of them and of its sessions, the bench
    # session's and the fleet's (`inspect placement`), and that placement by session and by node,
    # with the nodes that hold no replica counted (cluster.py fleet: none, as the bootstrap left it).
    # The work mode drives every session it lists, so both are read until they answer, bounded.
    cp focal-nodes.yaml "$out/$report.cluster.yaml"
    founder_view() {
      timeout 30 "${compose[@]}" --profile focal exec -T focal1 focal --data-dir /data "${@:2}" > "$1" 2>/dev/null
    }
    retry 15 founder_view "$out/$report.contacts.json" list nodes
    retry 15 founder_view "$out/$report.placement.json" inspect placement
    python3 cluster.py fleet "$table" < "$out/$report.placement.json" > "$out/$report.sessions.json" || true
    identity=$(dc exec -T focal1 focal --data-dir /data inspect identity)
    worker=$(python3 -c '
import json, sys
w = json.load(sys.stdin)["worker"]
print(w if isinstance(w, str) else bytes(w).hex())' <<< "$identity")
    # The bench session: the founder's, the one the load drives and FAULTS' roles name.
    bench_session=$(python3 -c '
import json, sys
s = json.load(sys.stdin)["ledger"]["session"]
print(s if isinstance(s, str) else bytes(s).hex())' <<< "$identity")
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
      # Every session the fleet holds is a ledger the load drives at once (focal-load's
      # `ledgers`, cluster.py ledgers: the founder's alone at three nodes), each among its share of
      # the agents. A ledger's agents are homed over the nodes holding its replica, as its clients
      # would be: each reads, and waits on its inbox, at its home and writes at the session's
      # leader, where every agent dialed the founder its invitation names, and the leader served
      # every read beside every write. Past three nodes the agents of the one session were homed
      # over every node, most of them holding no replica of it. Each ledger is offered its share
      # of the rate whatever its callers (focal-load's seats), and a caller holds at most
      # `inflight` lifecycles under way, so each ledger is driven by as many callers as a run of
      # one ledger is where focal-load's most callers (64, tools/load/src/shape.rs
      # MAX_CONCURRENCY) hold that many for each, else by as many as they hold for each (5 at 11
      # sessions: 320 lifecycles under way a ledger).
      ledgers=$(python3 cluster.py ledgers "$table" "$out/$report.contacts.json" < "$out/$report.placement.json")
      sessions=$(grep -c '^  - session: ' <<< "$ledgers")
      each=$(( 64 / sessions ))
      if [ "$each" -gt "$callers" ]; then each=$callers; fi
      callers=$(( each * sessions ))
      lifecycle="lifecycle: work
agents: $AGENTS
agent_enrollments: /client/agents
$ledgers"
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
    # The cluster at rest first, where REST asks: founded, its clients enrolled, the generator idle.
    if [ "$rest" -gt 0 ]; then
      cpu_stat before > "$out/$report.rest.cpu.txt"
      io_stat before > "$out/$report.rest.io.txt"
      disk_stat before > "$out/$report.rest.disk.txt"
      sleep "$rest"
      cpu_stat after >> "$out/$report.rest.cpu.txt"
      io_stat after >> "$out/$report.rest.io.txt"
      disk_stat after >> "$out/$report.rest.disk.txt"
    fi
    cpu_stat before > "$out/$report.cpu.txt"
    io_stat before > "$out/$report.io.txt"
    net_stat before > "$out/$report.net.txt"
    disk_stat before > "$out/$report.disk.txt"
    mem_stat before > "$out/$report.mem.txt"
    # The cluster's memory a second at a time while the load runs, as long as its bound at most.
    mem_sampler "$bound" > "$out/$report.mem-samples.txt" &
    sampler=$!
    faults_start
    if ! timeout "$bound" "${compose[@]}" --profile focal \
      exec -T -e "FOCAL_LOAD_WRITES_CSV=/reports/$report.csv" focal-load \
      focal-load --shape /reports/shape.yaml --out "/reports/$report"; then
      kill "$sampler" 2>/dev/null || true
      faults_end
      mem_stat after >> "$out/$report.mem.txt"
      evidence
      exit 1
    fi
    kill "$sampler" 2>/dev/null || true
    faults_end
    cpu_stat after >> "$out/$report.cpu.txt"
    io_stat after >> "$out/$report.io.txt"
    net_stat after >> "$out/$report.net.txt"
    disk_stat after >> "$out/$report.disk.txt"
    mem_stat after >> "$out/$report.mem.txt"
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
