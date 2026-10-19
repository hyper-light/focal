#!/usr/bin/env python3
"""Where each run's tail fell in time, beside what its nodes said then.

For every focal-compare report in DIR that carries a timeline: its measured window's worst
seconds by p99, and the events its bus nodes stated (`focal start bus` prints each suspicion, trust
restored, restart and term with the wall clock's time) within the measured window, each placed in
the run's seconds. A tail that shares its second with an event points at the event; one with
none points elsewhere (the disk, the scheduler, the client).

    python3 tools/compare/tails.py DIR [WORST]

WORST: the worst seconds listed for each run (default 3).
"""
import glob
import json
import os
import re
import sys

LINE = re.compile(r"^(?P<node>[\w.-]+?)-\d+\s+\|\s+(?P<json>\{.*\})\s*$")


def events(report_path):
    """Each event line of the run's bus node logs: (unix_ms, node, line)."""
    found = []
    for log in sorted(glob.glob(report_path + ".focal-bus*.log")):
        with open(log, encoding="utf-8", errors="replace") as lines:
            for raw in lines:
                match = LINE.match(raw.strip())
                if not match:
                    continue
                try:
                    line = json.loads(match.group("json"))
                except json.JSONDecodeError:
                    continue
                if "unix_ms" in line:
                    found.append((line["unix_ms"], match.group("node"), line))
    return sorted(found, key=lambda event: event[0])


def describe(node, line):
    kind = line.get("condition")
    if kind == "Suspected":
        silent = line.get("silent_ms")
        late = line.get("noticed_late_ms")
        return (f"{node} suspected {line.get('peer')}"
                + (f" (silent {silent:.0f} ms" if silent is not None else " (no heartbeat")
                + (f", noticed {late:.0f} ms late)" if late is not None else ")"))
    if kind == "Term":
        leader = line.get("leader")
        return f"{node} term {line.get('term')} " + (f"led by {leader}" if leader else "electing")
    return f"{node} {kind.lower()} {line.get('peer')}"


def main():
    directory = sys.argv[1] if len(sys.argv) > 1 else "."
    worst = int(sys.argv[2]) if len(sys.argv) > 2 else 3
    print("| run | worst seconds (p99 / max ms) | node events in the measured window |")
    print("|---|---|---|")
    for path in sorted(glob.glob(os.path.join(directory, "*.json"))):
        try:
            report = json.load(open(path))
        except (OSError, json.JSONDecodeError):
            continue
        timeline = report.get("timeline")
        if not timeline or "started_unix_ms" not in report:
            continue
        warmup = report.get("warmup_seconds", 0)
        measured = report.get("measured_seconds", 0)
        started = report["started_unix_ms"]
        window = [s for s in timeline if warmup <= s["second"] < warmup + measured]
        top = sorted(window, key=lambda s: s["p99_us"], reverse=True)[:worst]
        tails = ", ".join(
            f"s{s['second']}: {s['p99_us'] / 1e3:.1f} / {s['max_us'] / 1e3:.1f}" for s in top)
        begin, end = started + warmup * 1000, started + (warmup + measured) * 1000
        said = [
            f"s{(at - started) // 1000}: {describe(node, line)}"
            for at, node, line in events(path)
            if begin <= at < end
        ]
        print(f"| {os.path.basename(path)[:-5]} | {tails or '–'} | {'; '.join(said) or 'none'} |")


if __name__ == "__main__":
    main()
