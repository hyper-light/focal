#!/usr/bin/env python3
"""focal's nodes in the comparison as the compose model states them, and the cluster's own view
of them (focal-nodes.sh writes the model; run.sh and focal-bootstrap.sh ask this).

    cluster.py nodes < MODEL      every focal node of the model (`docker compose config --format
                                  json`), in order: NAME ADDRESS ZONE, one a line, its zone read
                                  from the configuration it mounts
    cluster.py joined TABLE < VIEWS
                                  whether the cluster's view holds every node of TABLE (`nodes`'
                                  lines) and no other: VIEWS is `focal list nodes`, a line `---`,
                                  then `focal inspect placement`. Each node's committed contact is
                                  at its address, and the directory sees it alive in its zone.
                                  Prints the nodes by zone and exits 0 when it does; exits 1 when
                                  not yet
    cluster.py role ROLE TABLE < PLACEMENT
                                  the node holding ROLE in the bench session's placement:
                                  `leader`, its preferred leader, or `copy`, a content copy other
                                  than the preferred leader. Prints NAME SESSION COPIES LEADER;
                                  exits 1 when no node of TABLE holds it
    cluster.py pick SEED COUNT NAME...
                                  COUNT of the NAMEs, each as likely as any other, the same ones
                                  for the same SEED
"""
import json
import random
import re
import sys

NODE = re.compile(r"focal(\d+)")
ZONE = re.compile(r"""^\s*zone:\s*["']?([^"'\s#]+)""", re.MULTILINE)


def table(text):
    """The `nodes` lines as (name, address, zone)."""
    return [tuple(line.split()) for line in text.splitlines() if line.strip()]


def nodes(model):
    found = []
    for name, service in model["services"].items():
        match = NODE.fullmatch(name)
        if not match:
            continue
        address = service["networks"]["bench"]["ipv4_address"]
        configs = [v["source"] for v in service["volumes"] if v.get("target", "").startswith("/etc/focal/")]
        if len(configs) != 1:
            sys.exit(f"{name} mounts {len(configs)} configurations, not one")
        with open(configs[0], encoding="utf-8") as config:
            zone = ZONE.search(config.read())
        if not zone:
            sys.exit(f"{name}'s configuration {configs[0]} names no zone")
        found.append((int(match.group(1)), name, address, zone.group(1)))
    for _, name, address, zone in sorted(found):
        print(name, address, zone)


def placement_of(document):
    placement = document.get("result", document)
    return placement.get("placement", placement)


def joined(expected, views):
    contacts_text, placement_text = views.split("\n---\n", 1)
    contacts = json.loads(contacts_text)["result"]["nodes"]
    placement = placement_of(json.loads(placement_text))
    want_addresses = sorted(f"{address}:7443" for _, address, _ in expected)
    if sorted(contact["advertise"] for contact in contacts) != want_addresses:
        return False
    seen = {}
    for partition in placement["partitions"]:
        for node in partition["nodes"]:
            seen[node.get("advertise")] = (node.get("zone"), node.get("alive"))
    if seen != {f"{address}:7443": (zone, True) for _, address, zone in expected}:
        return False
    by_zone = {}
    for _, _, zone in expected:
        by_zone[zone] = by_zone.get(zone, 0) + 1
    zones = ", ".join(f"{zone} {count}" for zone, count in sorted(by_zone.items()))
    print(f"{len(expected)} nodes joined, each listed at its address and alive in its zone: {zones}")
    return True


def role(name, expected, document):
    placement = placement_of(document)
    by_address = {f"{address}:7443": node for node, address, _ in expected}
    containers = {}
    for partition in placement["partitions"]:
        for node in partition["nodes"]:
            container = by_address.get(node.get("advertise"))
            if container:
                containers[node["node"]] = container
    for partition in placement["partitions"]:
        for session in partition["sessions"]:
            leader = session["preferred_leader"]
            held = [leader] if name == "leader" else sorted(c for c in session["content_copies"] if c != leader)
            for node in held:
                if node in containers:
                    print(containers[node], session["session"], session["content_copies"], leader)
                    return True
    return False


def main(argv):
    if len(argv) >= 1 and argv[0] == "nodes":
        nodes(json.load(sys.stdin))
        return 0
    if len(argv) == 2 and argv[0] == "joined":
        return 0 if joined(table(argv[1]), sys.stdin.read()) else 1
    if len(argv) == 3 and argv[0] == "role" and argv[1] in ("leader", "copy"):
        return 0 if role(argv[1], table(argv[2]), json.load(sys.stdin)) else 1
    if len(argv) >= 3 and argv[0] == "pick":
        count, names = int(argv[2]), argv[3:]
        if not 1 <= count <= len(names):
            sys.exit(f"pick {count} of {len(names)}: none left or too many")
        print(" ".join(sorted(random.Random(argv[1]).sample(names, count), key=lambda n: int(NODE.fullmatch(n).group(1)))))
        return 0
    sys.exit(__doc__)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
