#!/bin/bash
# Bring up a three-node focal cluster in Docker: a founder and two hosts
# joined by invitation, on one bridge network. Every wait is counted.
#
# Each node runs at the extreme a deployment may be given (the owner's law,
# 2026-10-10: 1-2 CPUs, 128-256 MB, contention forced): by default one CPU
# and 256 MB with no swap, so memory pressure is the node's own to answer.
#   NODE_CPUS     CPUs a node may use (default 1)
#   NODE_MEMORY   memory a node may hold, swap included (default 256m)
#   NODE_CPUSET   CPUs every node and contender is pinned to (default: none)
#   CONTEND       busy containers spinning on the same CPUs (default 0)
# Usage: cluster.sh IMAGE
set -euo pipefail
IMAGE=${1:?image}
NET=focal-chaos
NODES=(fc-founder fc-host-a fc-host-b)
OUT=${OUT:-$(mktemp -d)}
mkdir -p "$OUT"
LIMITS=(--cpus "${NODE_CPUS:-1}" --memory "${NODE_MEMORY:-256m}" --memory-swap "${NODE_MEMORY:-256m}")
if [ -n "${NODE_CPUSET:-}" ]; then
  LIMITS+=(--cpuset-cpus "$NODE_CPUSET")
fi

focal() { # node args...: run the binary inside a node's container
  local node=$1; shift
  docker exec "$node" /focal --data-dir /var/lib/focal "$@"
}

ready() { # node: wait until its start reported Ready, at most 120 checks
  local node=$1
  for _ in $(seq 1 120); do
    if docker logs "$node" 2>&1 | grep -Eq '"condition": *"(Ready|CatchingUp)"'; then
      return 0
    fi
    perl -e 'select(undef,undef,undef,1)'
  done
  echo "$node never reported Ready" >&2
  docker logs "$node" 2>&1 | tail -20 >&2
  return 1
}

docker network inspect "$NET" >/dev/null 2>&1 || docker network create "$NET" >/dev/null

for contender in $(seq 1 "${CONTEND:-0}"); do
  # A busy neighbour on the nodes' CPUs: what a shared machine gives a node.
  docker run -d --name "fc-contend-$contender" --cpus 1 \
    ${NODE_CPUSET:+--cpuset-cpus "$NODE_CPUSET"} alpine:3.20 sh -c 'while :; do :; done' >/dev/null
done

docker run -d --name fc-founder --hostname fc-founder --network "$NET" "${LIMITS[@]}" \
  -v fc-founder:/var/lib/focal --entrypoint /focal "$IMAGE" \
  --data-dir /var/lib/focal start node --listen 0.0.0.0:7443 --advertise fc-founder:7443 >/dev/null
ready fc-founder

for host in fc-host-a fc-host-b; do
  focal fc-founder invite node --node "$host" --output - > "$OUT/$host.invite"
  # An invitation is a secret: owner-only, owned by the node's user. The
  # image has no shell, so a one-shot helper writes it into its own volume.
  docker run --rm -i -v "$host-invite":/invite alpine:3.20 sh -c \
    'cat > /invite/invite.json && chown -R 65532:65532 /invite && chmod 700 /invite && chmod 600 /invite/invite.json' \
    < "$OUT/$host.invite"
  docker run -d --name "$host" --hostname "$host" --network "$NET" "${LIMITS[@]}" \
    -v "$host":/var/lib/focal -v "$host-invite":/invite --entrypoint /focal "$IMAGE" \
    --data-dir /var/lib/focal start node --listen 0.0.0.0:7443 --advertise "$host:7443" \
    --invite-file /invite/invite.json >/dev/null
  ready "$host"
done
echo "cluster up: ${NODES[*]} (cpus ${NODE_CPUS:-1}, memory ${NODE_MEMORY:-256m}, contenders ${CONTEND:-0})"
