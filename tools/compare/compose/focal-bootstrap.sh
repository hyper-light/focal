#!/bin/sh
# Founds focal's arm of the comparison (docs/qualification/competitive-p99.md)
# once `docker compose -f compose.yaml -f focal-nodes.yaml --profile focal up -d`
# has started it: the nodes focal-nodes.sh wrote (focal1 to focalN) join by
# invitation, one session is placed on three of them (one zone each), it is
# activated natively, and a client is enrolled in the generator's container for
# focal-load. Every wait is bounded; every step may be run again.
set -eu
cd "$(dirname "$0")"
DC="docker compose -f compose.yaml -f focal-nodes.yaml --profile focal"
f1() { $DC exec -T focal1 focal --data-dir /data "$@"; }
# Every node as the model states it, in order (cluster.py nodes): NAME ADDRESS ZONE a line. focal1
# founds the cluster; the others join it.
table=$($DC config --format json | python3 cluster.py nodes)
joiners=$(echo "$table" | awk '$1 != "focal1" { print $1 }')

# The host's neighbour table holds every container's peers. Linux keeps one ARP table for every
# network namespace on a host and, past its hard limit (net.ipv4.neigh.default.gc_thresh3, 1024 by
# default), makes no entry; a node answers no ARP request from a sender it holds none for (the
# kernel's arp_process replies once it holds the sender), so a container that must resolve a node
# anew is never answered while nodes that hold their peers carry on. At 33 nodes the table refused
# entries 105,155 times and the generator's client never reached the founder. Each container holds
# at most an entry for every other container and the gateway: (N+1)^2 for N nodes and the generator,
# beside what the host holds already. Past the soft limit (gc_thresh2) live entries are collected
# and resolved again, so both limits must hold them. Read and raised in the Docker host's own
# namespace (a container on the host's network: its kernel wherever Docker runs, a VM's included),
# raised only where RAISE_HOST_LIMITS=1 allows it (a dedicated runner), refused otherwise.
image=$($DC config --format json | python3 -c 'import json, sys; print(json.load(sys.stdin)["services"]["focal1"]["image"])')
limits=$(docker run --rm --net host --entrypoint sh "$image" -c \
  'cat /proc/sys/net/ipv4/neigh/default/gc_thresh2 /proc/sys/net/ipv4/neigh/default/gc_thresh3; awk "NR == 2 { print \$1 }" /proc/net/stat/arp_cache')
soft=$(echo "$limits" | sed -n 1p)
hard=$(echo "$limits" | sed -n 2p)
held=$((0x$(echo "$limits" | sed -n 3p)))
count=$(echo "$table" | wc -l)
need=$((held + (count + 1) * (count + 1)))
if [ "$soft" -lt "$need" ] || [ "$hard" -lt "$need" ]; then
  if [ "${RAISE_HOST_LIMITS:-0}" != 1 ]; then
    echo "the host's neighbour table holds $hard entries (gc_thresh3; gc_thresh2 $soft), $held of them held: $count nodes and the generator need $(((count + 1) * (count + 1))) more; set net.ipv4.neigh.default.gc_thresh2 and gc_thresh3 to $need at least, or let the run raise them (RAISE_HOST_LIMITS=1)" >&2
    exit 2
  fi
  raised_soft=$need; [ "$soft" -gt "$need" ] && raised_soft=$soft
  raised_hard=$need; [ "$hard" -gt "$need" ] && raised_hard=$hard
  docker run --rm --privileged --net host --entrypoint sysctl "$image" -w \
    "net.ipv4.neigh.default.gc_thresh2=$raised_soft" "net.ipv4.neigh.default.gc_thresh3=$raised_hard" >/dev/null
  echo "the host's neighbour table raised for $count nodes and the generator: gc_thresh2 $soft -> $raised_soft, gc_thresh3 $hard -> $raised_hard ($held held already)"
fi

# A command retried until it succeeds, at most $1 times a second apart.
retry() {
  tries=$1; shift; n=0
  until "$@"; do
    n=$((n + 1))
    if [ "$n" -ge "$tries" ]; then echo "gave up after $tries: $*" >&2; return 1; fi
    sleep 1
  done
}
serving() { $DC exec -T "$1" focal --data-dir /data inspect node --probe serving >/dev/null 2>&1; }
# Serving is the owners running, by design asking no leadership of them (F25);
# an invitation is the root leader's to issue, so the founder is waited on until
# it leads (authoritative), as an operator inviting from it must.
authoritative() { $DC exec -T "$1" focal --data-dir /data inspect node --probe authoritative >/dev/null 2>&1; }

retry 300 serving focal1
retry 300 authoritative focal1
# The founder may lead its root before its ledger service answers (a refusal
# typed `unavailable`, exit 6, retryable): each invitation is asked again,
# bounded, until it is written.
invite() { f1 invite node --node "$1" --output "/invite/$1.invite" >/dev/null; }
for host in $joiners; do
  retry 120 invite "$host"
done
for host in $joiners; do
  retry 300 serving "$host"
done
# Every node in the cluster's own view (cluster.py joined): its committed contact at its address,
# and the directory seeing it alive in its zone. The work mode homes its agents on the contacts
# listed, so a node not yet listed would be no agent's home.
joined() {
  contacts=$(f1 list nodes 2>/dev/null) || return 1
  placement=$(f1 inspect placement 2>/dev/null) || return 1
  printf '%s\n---\n%s\n' "$contacts" "$placement" | python3 cluster.py joined "$table"
}
retry 300 joined

# Each session on three nodes, one in each zone, surviving a zone: planned against the
# target, applied, then activated natively (whose first call may answer
# `unavailable` while the session settles: the KIND campaign's D3).
# A host that has joined is planned on once it has reported its load: until
# then the plan records the capacity missing (and is still written) and its
# apply is refused `guarantee_unsatisfied`, so both are asked again, each plan
# to a file of its own (a plan is never written over).
$DC cp focal-target.yaml focal1:/data/target.yaml
attempt=0
applied() {
  attempt=$((attempt + 1))
  f1 plan deployment --config /data/target.yaml --output "/data/target-$attempt.plan" >/dev/null \
    && f1 apply deployment --plan-file "/data/target-$attempt.plan" --wait 300 >/dev/null
}
retry 60 applied
retry 30 f1 activate native

# The generator's client, enrolled over QUIC like every competitor's client, and the
# deliver mode's consumer, a client of its own that completes what the first issues.
f1 invite client --name bench --output /invite/client.invite
$DC exec -T focal-load focal --data-dir /client enroll context --invite-file /invite/client.invite bench
f1 invite client --name consumer --output /invite/consumer.invite
$DC exec -T focal-load focal --data-dir /client enroll context --invite-file /invite/consumer.invite consumer
# AGENTS (environment), for the deliver mode among agents: that many more clients, each an
# identity of its own (agent-0000, ...) in a client data directory of its own under /client/agents
# (one client directory keeps at most 64 contexts), invited on the founder and enrolled in the
# generator's container, each side in one exec so a thousand agents cost two.
if [ -n "${AGENTS:-}" ]; then
  $DC exec -T -e AGENTS="$AGENTS" focal1 sh -c '
    set -e
    i=0
    while [ "$i" -lt "$AGENTS" ]; do
      name=$(printf "agent-%04d" "$i")
      focal --data-dir /data invite client --name "$name" --output "/invite/$name.invite" >/dev/null
      i=$((i + 1))
    done'
  $DC exec -T -e AGENTS="$AGENTS" focal-load sh -c '
    set -e
    mkdir -m 700 -p /client/agents
    i=0
    while [ "$i" -lt "$AGENTS" ]; do
      name=$(printf "agent-%04d" "$i")
      mkdir -m 700 -p "/client/agents/$name"
      focal --data-dir "/client/agents/$name" enroll context --invite-file "/invite/$name.invite" "$name" >/dev/null
      i=$((i + 1))
    done'
fi
f1 inspect identity
