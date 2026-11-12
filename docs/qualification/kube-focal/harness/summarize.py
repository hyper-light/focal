"""The record's numbers from record.py's export: claims by status and by whom they were for,
evaluations by state, turns by role, tokens. Usage: summarize.py record.json
"""
import collections
import json
import sys

record = json.load(open(sys.argv[1]))
claims = record["claims"]
evaluations = record["evaluations"]
turns = record["turns"]
totals = record["totals"]

print(f"claims: {len(claims)}")
for status, count in collections.Counter(c["status"] for c in claims).most_common():
    print(f"  {status}: {count}")
print("claims by subject:")
for subject, count in collections.Counter(c["subject"] for c in claims).most_common():
    print(f"  {subject}: {count}")
print(f"evaluations: {len(evaluations)}")
for state, count in collections.Counter(e["state"] for e in evaluations).most_common():
    print(f"  {state}: {count}")
by_role = collections.defaultdict(lambda: {"turns": 0, "input": 0, "cached": 0, "output": 0, "calls": 0, "failed": 0})
for name, turn in turns.items():
    role = name.rsplit("-", 1)[0] if name.startswith("worker") else name.split("-")[0]
    slot = by_role[role]
    slot["turns"] += 1
    slot["calls"] += turn["mcp_calls"]
    slot["failed"] += turn["mcp_failed"]
    usage = turn["usage"] or {}
    slot["input"] += usage.get("input_tokens", 0)
    slot["cached"] += usage.get("cached_input_tokens", 0)
    slot["output"] += usage.get("output_tokens", 0)
print("turns by role:")
for role, slot in sorted(by_role.items()):
    print(f"  {role}: {slot}")
print(f"totals: {totals}")
events = record.get("events") or []
sequences = {e["sequence"] for e in events}
print(f"events: {len(events)} over {len(sequences)} committed operations (last {max(sequences) if sequences else 0})")
by = collections.defaultdict(set)
for e in events:
    by[e["by"]].add(e["sequence"])
print("operations by participant:", {k: len(v) for k, v in sorted(by.items())})
print("facts:", dict(collections.Counter(e["fact"] for e in events).most_common()))
print("claim events:", dict(collections.Counter(e["kind"] for e in events if e["fact"] == "Claim").most_common()))
print("evaluation events:", dict(collections.Counter(e["kind"] for e in events if e["fact"] == "Evaluation").most_common()))
