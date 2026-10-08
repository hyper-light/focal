#!/bin/bash
# Bring up a three-node focal cluster in Docker: a founder and two hosts
# joined by invitation, on one bridge network. Every wait is counted.
# Usage: cluster.sh IMAGE
set -euo pipefail
IMAGE=${1:?image}
NET=focal-chaos
NODES=(fc-founder fc-host-a fc-host-b)
OUT=${OUT:-$(mktemp -d)}
mkdir -p "$OUT"

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

docker run -d --name fc-founder --hostname fc-founder --network "$NET" \
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
  docker run -d --name "$host" --hostname "$host" --network "$NET" \
    -v "$host":/var/lib/focal -v "$host-invite":/invite --entrypoint /focal "$IMAGE" \
    --data-dir /var/lib/focal start node --listen 0.0.0.0:7443 --advertise "$host:7443" \
    --invite-file /invite/invite.json >/dev/null
  ready "$host"
done
echo "cluster up: ${NODES[*]}"
