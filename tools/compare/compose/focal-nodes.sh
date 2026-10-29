#!/bin/sh
# focal's nodes in the comparison (docs/qualification/competitive-p99.md), NODES of them, written
# as a compose file to stdout for compose.yaml to take beside it. run.sh writes it for every run;
# by hand:
#
#   NODES=33 sh focal-nodes.sh > focal-nodes.yaml
#   docker compose -f compose.yaml -f focal-nodes.yaml --profile focal up -d
#   sh focal-bootstrap.sh
#
# NODES: 3 where unset, as each competitor runs three, so a comparison runs focal as it runs them.
# At least one a zone (the target's sessions survive the loss of one of the three zones,
# focal-target.yaml); at most 203, the bench network's addresses given by service (compose.yaml).
#
# Node K is the service focalK: its data the volume focalK, its address 10.231.0.(20+K), its
# configuration its zone's, zone a, b or c as K-1 is 0, 1 or 2 modulo 3 (focal-zone-a.yaml, ...).
# focal1 founds the cluster; every other node waits for the invitation focal-bootstrap.sh writes
# it, a bounded wait, then joins with it.
#
# The image: FOCAL_IMAGE where set, as for the generator and the bus (compose.yaml), else focal:bench.
#
# CPUs: each node holds NODE_CPUS (default 2), as each competitor's nodes and the generator do
# (compose.yaml's limits). Past three nodes, where the nodes and the generator would hold more
# CPUs than the host has (HOST_CPUS, else Docker's count), the nodes share what the generator
# leaves, each the same fraction in hundredths of a CPU (a control group's least quota is 1 ms of
# Docker's 100 ms period): limits summing past the host would limit nothing.
set -eu

nodes=${NODES:-3}
cpus=${NODE_CPUS:-2}
zones="a b c"
# The bench network's addresses given by service end where those handed to the rest begin
# (compose.yaml: 10.231.0.224/27); node K's is the 20+K-th.
first_address=20
last_address=223

zone_count=$(echo "$zones" | wc -w | tr -d ' ')
most=$((last_address - first_address))
case "$nodes" in
  '' | *[!0-9]*) echo "NODES must be a whole number, not '$nodes'" >&2; exit 2 ;;
esac
if [ "$nodes" -lt "$zone_count" ] || [ "$nodes" -gt "$most" ]; then
  echo "NODES must be $zone_count to $most (one a zone at least, one address each), not $nodes" >&2
  exit 2
fi

node_cpus=$cpus
if [ "$nodes" -gt 3 ]; then
  host=${HOST_CPUS:-}
  if [ -z "$host" ]; then
    host=$(docker info --format '{{.NCPU}}') || {
      echo "the host's CPUs are unknown: set HOST_CPUS" >&2
      exit 2
    }
  fi
  node_cpus=$(awk -v host="$host" -v cpus="$cpus" -v nodes="$nodes" 'BEGIN {
    if ((nodes + 1) * cpus <= host) { print cpus; exit 0 }
    share = int((host - cpus) * 100 / nodes) / 100
    if (share < 0.01) exit 1
    printf "%.2f\n", share
  }') || {
    echo "a host of $host CPUs cannot hold $nodes nodes beside a generator holding $cpus: each node needs 0.01 CPU at least" >&2
    exit 2
  }
fi

printf '%s\n' \
  "# focal's $nodes nodes in the comparison, written by focal-nodes.sh (NODES=$nodes, NODE_CPUS=$cpus):" \
  "# each node holds $node_cpus CPUs. Not kept in the repository: run.sh writes it for every run." \
  "x-focal-node: &focal-node" \
  "  image: \${FOCAL_IMAGE:-focal:bench}" \
  "  profiles: [focal]" \
  "  cpus: \"$node_cpus\"" \
  "  mem_limit: 2g" \
  "  cap_add: [NET_ADMIN]" \
  "" \
  "services:"
k=1
while [ "$k" -le "$nodes" ]; do
  zone=$(echo "$zones" | cut -d ' ' -f $(((k - 1) % zone_count + 1)))
  node="focal$k"
  start="focal --config /etc/focal/$node.yaml --data-dir /data start node --listen 0.0.0.0:7443 --advertise $node:7443"
  printf '  %s:\n    <<: *focal-node\n' "$node"
  printf '    networks: { bench: { ipv4_address: 10.231.0.%d } }\n' $((first_address + k))
  if [ "$k" -eq 1 ]; then
    # The founder starts at once.
    printf '    command: [%s]\n' "$(echo "$start" | sed 's/[^ ]*/"&"/g; s/ /, /g')"
  else
    # Every other node waits for its invitation, a second at a time, ten minutes at most.
    # `$$` is compose's literal `$`.
    printf '    command: ["sh", "-c", "n=0; until [ -s /invite/%s.invite ]; do n=$$((n+1)); [ $$n -gt 600 ] && exit 1; sleep 1; done; exec %s --invite-file /invite/%s.invite"]\n' \
      "$node" "$start" "$node"
  fi
  printf '    volumes: ["./focal-zone-%s.yaml:/etc/focal/%s.yaml:ro", "%s:/data", "invites:/invite"]\n' \
    "$zone" "$node" "$node"
  k=$((k + 1))
done
printf '\nvolumes:\n'
k=1
while [ "$k" -le "$nodes" ]; do
  printf '  focal%d: {}\n' "$k"
  k=$((k + 1))
done
