#!/usr/bin/env python3
"""How each focal run's cluster fit its host: what run.sh's counters say of its nodes.

For every focal run in DIR that kept its memory counters (`<report>.mem.txt`, run.sh's mem_stat,
taken before and after the load): the nodes' limits; the busiest node's CPU over the load against
its own limit, and the node whose quota held it back most often (`<report>.cpu.txt`); the most any
node held since it started (memory.peak) and every container's peak together, with the most the
run's containers held at one moment while the load ran (`<report>.mem-samples.txt`, a sample a
second); the host's memory and the least of it available; and how many times the kernel killed in
a container for memory, which a run that fits never shows.

    python3 tools/compare/fit.py DIR
"""
import glob
import os
import sys

MIB = 1024 * 1024


def counters(path):
    """`PHASE NAME KEY VALUE` lines as {name: {key: {phase: value}}}."""
    seen = {}
    try:
        with open(path, encoding="utf-8") as lines:
            for line in lines:
                parts = line.split()
                if len(parts) == 4 and parts[3].lstrip("-").isdigit():
                    phase, name, key, value = parts
                    seen.setdefault(name, {}).setdefault(key, {})[phase] = int(value)
    except OSError:
        pass
    return seen


def is_node(container):
    return "focal-load" not in container and "generator" not in container


def short(container):
    """compose-focal7-1 as focal7."""
    name = container.removeprefix("compose-")
    head, _, tail = name.rpartition("-")
    return head if tail.isdigit() else name


def fit(base):
    """One row for the run whose report is BASE, or None where it kept no memory counters."""
    mem = counters(base + ".mem.txt")
    host = mem.pop("-", {})
    nodes = {c: keys for c, keys in mem.items() if is_node(c)}
    if not nodes:
        return None

    def after(keys, key):
        return keys.get(key, {}).get("after", keys.get(key, {}).get("before"))

    # Each node's limits, as its group states them.
    cpus = sorted({after(k, "cpu_quota_usec") / after(k, "cpu_period_usec")
                   for k in nodes.values()
                   if after(k, "cpu_quota_usec") and after(k, "cpu_period_usec")})
    limits = sorted({after(k, "memory_max") // MIB for k in nodes.values() if after(k, "memory_max")})
    limit = (", ".join(f"{c:.2f}" for c in cpus) or "none") + " CPU; " + \
        (", ".join(f"{m:,}" for m in limits) or "none") + " MiB"

    # The busiest node over the load, and the one most often held back by its quota.
    cpu = counters(base + ".cpu.txt")
    wall = cpu.pop("-", {}).get("wall_usec", {})
    wall = wall.get("after", 0) - wall.get("before", 0)
    busiest = held = "–"
    if wall > 0:
        shares = []
        throttled = []
        for container, keys in cpu.items():
            if not is_node(container):
                continue
            delta = {k: v["after"] - v["before"] for k, v in keys.items() if len(v) == 2}
            quota = nodes.get(container, {})
            ceiling = None
            if after(quota, "cpu_quota_usec") and after(quota, "cpu_period_usec"):
                ceiling = after(quota, "cpu_quota_usec") / after(quota, "cpu_period_usec")
            used = delta.get("usage_usec", 0) / wall
            shares.append((used, container, ceiling))
            if delta.get("nr_periods"):
                throttled.append((100 * delta.get("nr_throttled", 0) / delta["nr_periods"], container))
        if shares:
            used, container, ceiling = max(shares)
            busiest = f"{short(container)} {used:.3f}" + (f" ({100 * used / ceiling:.0f}% of its limit)" if ceiling else "")
        if throttled:
            share, container = max(throttled)
            held = f"{short(container)} {share:.1f}%"

    # Memory: the most a node held, every container's peak together, and the cluster's at once.
    peaks = [(after(k, "memory_peak"), c) for c, k in nodes.items() if after(k, "memory_peak") is not None]
    peak = "–"
    if peaks:
        most, container = max(peaks)
        peak = f"{short(container)} {most / MIB:,.0f}"
    every = sum(after(k, "memory_peak") or 0 for k in mem.values())
    together = "–"
    least = None
    try:
        with open(base + ".mem-samples.txt", encoding="utf-8") as lines:
            samples = [line.split() for line in lines]
        samples = [s for s in samples if len(s) == 3 and all(p.isdigit() for p in s)]
        if samples:
            together = f"{max(int(s[1]) for s in samples) / MIB:,.0f}"
            least = min(int(s[2]) for s in samples) * 1024
    except OSError:
        pass
    total = host.get("MemTotal_kb", {})
    total = total.get("before", total.get("after"))
    available = host.get("MemAvailable_kb", {})
    available = [v * 1024 for v in available.values()]
    if least is not None:
        available.append(least)
    machine = (f"{total * 1024 / MIB:,.0f}" if total else "–") + " / " + \
        (f"{min(available) / MIB:,.0f}" if available else "–")
    kills = sum((after(k, "memory_events_oom_kill") or 0) for k in mem.values())
    stopped = sum(1 for k in mem.values() if after(k, "oom_killed"))
    name = os.path.basename(base)
    name = name[:-5] if name.endswith(".json") else name
    return (name, len(nodes), limit, busiest, held, peak, f"{every / MIB:,.0f}", together, machine,
            f"{kills} ({stopped} stopped)")


def main(argv):
    directory = argv[1] if len(argv) > 1 else "."
    rows = [row for row in (fit(path[: -len(".mem.txt")])
                            for path in sorted(glob.glob(os.path.join(directory, "*.mem.txt"))))
            if row]
    if not rows:
        return 0
    print("\n### focal's cluster on its host\n")
    print("| run | nodes | each node's limit | busiest node, CPU over the load | most throttled node, periods | "
          "most a node held, MiB | every container's peak, MiB | most held at once, MiB | "
          "host MiB: total / least available | OOM kills |")
    print("|---|---|---|---|---|---|---|---|---|---|")
    for row in rows:
        print("| " + " | ".join(str(v) for v in row) + " |")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
