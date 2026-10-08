#!/bin/sh
# Founds focal's arm of the comparison (docs/qualification/competitive-p99.md)
# once `docker compose --profile focal up -d` has started it: the hosts join
# by invitation, one session is placed on all three nodes (one zone each), it
# is activated natively, and a client is enrolled in the generator's container
# for focal-load. Every wait is bounded; every step may be run again.
set -eu
cd "$(dirname "$0")"
DC="docker compose -f compose.yaml --profile focal"
f1() { $DC exec -T focal1 focal --data-dir /data "$@"; }

# A command retried until it succeeds, at most $1 times a second apart.
retry() {
  tries=$1; shift; n=0
  until "$@"; do
    n=$((n + 1))
    if [ "$n" -ge "$tries" ]; then echo "gave up after $tries: $*" >&2; return 1; fi
    sleep 1
  done
}
serving() { $DC exec -T "$1" focal --data-dir /data diagnose node --probe serving >/dev/null 2>&1; }
# Serving is the owners running, by design asking no leadership of them (F25);
# an invitation is the root leader's to issue, so the founder is waited on until
# it leads (authoritative), as an operator inviting from it must.
authoritative() { $DC exec -T "$1" focal --data-dir /data diagnose node --probe authoritative >/dev/null 2>&1; }

retry 300 serving focal1
retry 300 authoritative focal1
for host in focal2 focal3; do
  f1 cluster invite --node "$host" --output "/invite/$host.invite"
done
retry 300 serving focal2
retry 300 serving focal3

# Each session on all three nodes, surviving a zone: planned against the
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
  f1 deployment plan --config /data/target.yaml --output "/data/target-$attempt.plan" >/dev/null \
    && f1 deployment apply --plan-file "/data/target-$attempt.plan" --wait 300 >/dev/null
}
retry 60 applied
retry 30 f1 cluster replicas activate-native

# The generator's client, enrolled over QUIC like every competitor's client.
f1 cluster client invite --name bench --output /invite/client.invite
$DC exec -T focal-load focal --data-dir /client context enroll --invite-file /invite/client.invite bench
f1 identity
