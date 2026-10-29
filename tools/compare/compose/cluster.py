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
    cluster.py role ROLE TABLE SESSION < PLACEMENT
                                  the node holding ROLE in the placement of SESSION (the bench
                                  session's, among the fleet's): `leader`, its preferred leader,
                                  or `copy`, a content copy other than the preferred leader.
                                  Prints NAME SESSION COPIES LEADER; exits 1 when no node of TABLE
                                  holds it
    cluster.py pick SEED COUNT NAME...
                                  COUNT of the NAMEs, each as likely as any other, the same ones
                                  for the same SEED
    cluster.py wanted TABLE      the sessions the fleet holds: one for every node of its largest
                                  zone (11 at 33 nodes, 1 at three)
    cluster.py founders TABLE < PLACEMENT
                                  the nodes that found the fleet's sessions still to be made, one
                                  a line: a session for every node of the largest zone (`wanted`),
                                  so each is founded on a node of it that no session seats yet
    cluster.py sessions TABLE < PLACEMENT
                                  every session the directory lists: TENANT SESSION FOUNDER, its
                                  founder the node that holds its first replica
    cluster.py progress < PLACEMENT
                                  how far the placement of every session has come, one line: each
                                  session's voters and, for a plan under way, its phase and each
                                  copy's step and log position. It differs from the last whenever
                                  the placement moved
    cluster.py fleet TABLE < PLACEMENT
                                  whether the sessions sit across the whole fleet: as many as the
                                  largest zone has nodes, each placed (no plan pending) on three
                                  voters in three zones, the directory holding each to survive a
                                  zone, and every node of TABLE a voter of one at least. Writes the
                                  placement by node and by session as JSON (the run keeps it beside
                                  its report) and a line saying how it stands to stderr; exits 0
                                  when it holds, 1 when not yet
"""
import collections
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


def role(name, expected, document, wanted_session):
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
            if session["session"] != wanted_session:
                continue
            leader = session["preferred_leader"]
            held = [leader] if name == "leader" else sorted(c for c in session["content_copies"] if c != leader)
            for node in held:
                if node in containers:
                    print(containers[node], session["session"], session["content_copies"], leader)
                    return True
    return False


def wanted(expected):
    """The sessions the fleet holds: one for every node of the largest zone, each session on one
    node of each zone, so that every node holds a replica of one (the smaller zones' nodes, where
    the zones differ in size, of one or two)."""
    return max(collections.Counter(zone for _, _, zone in expected).values())


def by_address(expected, placement):
    """Each node the directory lists, by its identity: its name in TABLE, found by the address it
    advertises."""
    names = {f"{address}:7443": name for name, address, _ in expected}
    found = {}
    for partition in placement["partitions"]:
        for node in partition["nodes"]:
            name = names.get(node.get("advertise"))
            if name:
                found[node["node"]] = name
    return found


def listed(placement):
    """Every session the directory lists, refused where the view leaves sessions or nodes out."""
    if any(partition.get("truncated") for partition in placement["partitions"]):
        sys.exit("the placement view is truncated (it lists at most 48 sessions and 128 nodes a "
                 "partition): the fleet cannot be read from it")
    return [session for partition in placement["partitions"] for session in partition["sessions"]]


def seated(session):
    """The voters a session has, or will have once the plan under way is done."""
    pending = session.get("pending")
    return pending["voters"] if pending else session["voters"]


def founders(expected, document):
    placement = placement_of(document)
    names = by_address(expected, placement)
    sessions = listed(placement)
    taken = {names.get(voter) for session in sessions for voter in seated(session)}
    sizes = collections.Counter(zone for _, _, zone in expected)
    # The largest zone, the first of the largest in TABLE's order where several are.
    largest = next(zone for _, _, zone in expected if sizes[zone] == max(sizes.values()))
    free = [name for name, _, zone in expected if zone == largest and name not in taken]
    for name in free[:max(0, wanted(expected) - len(sessions))]:
        print(name)


def sessions_of(expected, document):
    placement = placement_of(document)
    names = by_address(expected, placement)
    for session in listed(placement):
        founder = names.get(session.get("founder"))
        if not founder:
            sys.exit(f"session {session['session']}'s founder {session.get('founder')} is no node of the table")
        print(session["tenant"], session["session"], founder)


def progress(document):
    placement = placement_of(document)
    states = []
    for session in listed(placement):
        pending = session.get("pending")
        steps = sorted(
            (step["node"], step["phase"], step["attempt"], step["through"])
            for step in (pending["progress"] if pending else [])
        )
        states.append((
            session["session"],
            sorted(session["voters"]),
            pending["phase"] if pending else None,
            sorted(pending["voters"]) if pending else None,
            steps,
            session.get("achieved_survive"),
            session.get("achieved_max_failures"),
        ))
    print(json.dumps(sorted(states), separators=(",", ":")))


def fleet(expected, document):
    placement = placement_of(document)
    names = by_address(expected, placement)
    zone_of = {name: zone for name, _, zone in expected}
    sessions = listed(placement)
    record = {"sessions": [], "nodes": {name: [] for name, _, _ in expected}}
    unplaced = []
    for session in sessions:
        voters = [names.get(voter, str(voter)) for voter in session["voters"]]
        zones = sorted({zone_of.get(name) for name in voters if name in zone_of})
        placed = (
            session.get("pending") is None
            and len(voters) == 3
            and len(zones) == 3
            and session.get("achieved_survive") == "Zone"
            and (session.get("achieved_max_failures") or 0) >= 1
        )
        if not placed:
            unplaced.append(session["session"])
        record["sessions"].append({
            "tenant": session["tenant"],
            "session": session["session"],
            "voters": voters,
            "zones": zones,
            "preferred_leader": names.get(session["preferred_leader"], str(session["preferred_leader"])),
            "founder": names.get(session.get("founder"), str(session.get("founder"))),
            "achieved_survive": session.get("achieved_survive"),
            "achieved_max_failures": session.get("achieved_max_failures"),
            "pending": session.get("pending") is not None,
        })
        for name in voters:
            if name in record["nodes"]:
                record["nodes"][name].append(session["session"])
    holding_none = [name for name, held in record["nodes"].items() if not held]
    record["count"] = {
        "nodes": len(expected),
        "sessions": len(sessions),
        "sessions_wanted": wanted(expected),
        "sessions_not_placed_in_three_zones": len(unplaced),
        "nodes_holding_no_replica": len(holding_none),
        "most_replicas_a_node": max((len(held) for held in record["nodes"].values()), default=0),
    }
    json.dump(record, sys.stdout, indent=1, sort_keys=True)
    print()
    holds = len(sessions) == wanted(expected) and not unplaced and not holding_none
    print(
        f"{len(sessions)} sessions of {wanted(expected)} over {len(expected)} nodes: "
        f"{len(sessions) - len(unplaced)} placed on three voters in three zones; "
        f"nodes holding no replica: {len(holding_none)}"
        + (f" ({', '.join(holding_none)})" if holding_none else "")
        + ("" if holds else "; not yet"),
        file=sys.stderr,
    )
    return holds


def main(argv):
    if len(argv) >= 1 and argv[0] == "nodes":
        nodes(json.load(sys.stdin))
        return 0
    if len(argv) == 2 and argv[0] == "joined":
        return 0 if joined(table(argv[1]), sys.stdin.read()) else 1
    if len(argv) == 4 and argv[0] == "role" and argv[1] in ("leader", "copy"):
        return 0 if role(argv[1], table(argv[2]), json.load(sys.stdin), argv[3]) else 1
    if len(argv) == 2 and argv[0] == "wanted":
        print(wanted(table(argv[1])))
        return 0
    if len(argv) == 2 and argv[0] == "founders":
        founders(table(argv[1]), json.load(sys.stdin))
        return 0
    if len(argv) == 2 and argv[0] == "sessions":
        sessions_of(table(argv[1]), json.load(sys.stdin))
        return 0
    if len(argv) == 1 and argv[0] == "progress":
        progress(json.load(sys.stdin))
        return 0
    if len(argv) == 2 and argv[0] == "fleet":
        return 0 if fleet(table(argv[1]), json.load(sys.stdin)) else 1
    if len(argv) >= 3 and argv[0] == "pick":
        count, names = int(argv[2]), argv[3:]
        if not 1 <= count <= len(names):
            sys.exit(f"pick {count} of {len(names)}: none left or too many")
        print(" ".join(sorted(random.Random(argv[1]).sample(names, count), key=lambda n: int(NODE.fullmatch(n).group(1)))))
        return 0
    sys.exit(__doc__)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
