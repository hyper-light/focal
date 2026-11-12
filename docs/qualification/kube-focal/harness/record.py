"""What the kube-focal build left: the ledger's claims and evaluations read through the architect's
MCP connection (read-only calls), and each agent's turns totalled from its transcript. Run it only
while no round is running: it starts the node for its reads and stops it. Bounded: 64 pages a list,
120 s an answer. Usage: record.py > record.json
"""
import glob
import json
import os
import select
import signal
import subprocess
import sys
import time

K = "/Users/adalundhe/fc2k"
BIN = f"{K}/bin/focal"
STATUS = {1: "Generated", 2: "Posted", 3: "Received", 4: "Progressed", 5: "TestamentGenerated",
          6: "TestamentAcknowledged", 7: "Validating", 8: "Satisfied", 9: "PostFailed",
          10: "ReceiptFailed", 11: "TestamentGenerationFailed", 12: "ValidationIncomplete",
          13: "ValidationFailed", 14: "ValidationErrored", 15: "Cancelled", 16: "Expired",
          17: "Revoked", 18: "Superseded", 19: "DependencyFailed", 20: "Deadlocked"}
PAGES = 64


def hexid(value):
    return "".join(f"{b:02x}" for b in value) if isinstance(value, list) else value


class Mcp:
    def __init__(self, role):
        self.process = subprocess.Popen(
            [BIN, "--data-dir", f"/tmp/fc2k/a/{role}", "--client-context", role, "serve", "mcp"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
        )
        self.next = 0
        self.request("initialize", {"protocolVersion": "2025-11-25", "capabilities": {},
                                    "clientInfo": {"name": "record", "version": "1"}})
        self.process.stdin.write(b'{"jsonrpc":"2.0","method":"notifications/initialized"}\n')
        self.process.stdin.flush()

    def request(self, method, params):
        self.next += 1
        line = json.dumps({"jsonrpc": "2.0", "id": self.next, "method": method, "params": params})
        self.process.stdin.write((line + "\n").encode())
        self.process.stdin.flush()
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            ready, _, _ = select.select([self.process.stdout], [], [], 1)
            if ready:
                read = self.process.stdout.readline()
                if not read:
                    raise SystemExit(f"{method}: the server closed")
                answer = json.loads(read)
                if answer.get("id") == self.next:
                    return answer
        raise SystemExit(f"{method}: no answer in 120 s")

    def tool(self, name, arguments):
        result = self.request("tools/call", {"name": name, "arguments": arguments}).get("result") or {}
        content = result.get("structuredContent")
        if content is None:
            raise SystemExit(f"{name}: {json.dumps(result)[:300]}")
        return content

    def pages(self, name, arguments):
        rows, cursor = [], None
        for _ in range(PAGES):
            args = dict(arguments, limit=256, max_visits=65536)
            if cursor is not None:
                args["cursor"] = cursor
            page = self.tool(name, args)["result"]["page"]
            rows.extend(page.get("objects") or [])
            cursor = page.get("next")
            if cursor is None:
                return rows
            if isinstance(cursor, list):
                cursor = "".join(f"{b:02x}" for b in cursor)
        raise SystemExit(f"{name}: more than {PAGES} pages")

    def close(self):
        self.process.stdin.close()
        self.process.wait(timeout=30)


def turns():
    out, totals = {}, {"turns": 0, "input": 0, "cached": 0, "output": 0, "mcp_calls": 0, "mcp_failed": 0}
    for path in sorted(glob.glob(f"{K}/runs/*.jsonl")):
        name = os.path.basename(path)[:-len(".jsonl")]
        if name.startswith("probe"):
            continue
        calls = failed = 0
        usage = None
        with open(path) as lines:
            for line in lines:
                try:
                    event = json.loads(line)
                except ValueError:
                    continue
                item = event.get("item") or {}
                if event.get("type") == "item.completed" and item.get("type") == "mcp_tool_call":
                    calls += 1
                    failed += item.get("status") == "failed"
                if event.get("type") == "turn.completed":
                    usage = event.get("usage")
        out[name] = {"mcp_calls": calls, "mcp_failed": failed, "usage": usage}
        totals["turns"] += 1
        totals["mcp_calls"] += calls
        totals["mcp_failed"] += failed
        if usage:
            totals["input"] += usage.get("input_tokens", 0)
            totals["cached"] += usage.get("cached_input_tokens", 0)
            totals["output"] += usage.get("output_tokens", 0)
    return out, totals


def main():
    ids = json.load(open(f"{K}/participants.json"))
    names = {v: k for k, v in ids.items()}
    address = open(f"{K}/address").read().strip()
    out = open(f"{K}/node.record.out", "w")
    node = subprocess.Popen([BIN, "--data-dir", "/tmp/fc2k/n", "start", "node", "--advertise", address],
                            stdout=out, stderr=subprocess.STDOUT)
    try:
        deadline = time.monotonic() + 180
        while '"condition"' not in open(f"{K}/node.record.out").read():
            if time.monotonic() > deadline or node.poll() is not None:
                raise SystemExit("the node did not start")
            time.sleep(0.2)
        mcp = Mcp("architect")
        claims = []
        for row in mcp.pages("claim.list", {}):
            claim = row["Claim"]
            identity = hexid(claim["binding"]["object"])
            claims.append({
                "id": identity,
                "status": STATUS.get(claim["status"], claim["status"]),
                "issuer": names.get(hexid(claim.get("issuer")), hexid(claim.get("issuer"))),
                "subject": names.get(hexid(claim.get("subject")), hexid(claim.get("subject"))),
                "responses": claim.get("response_count"),
            })
        evaluations = []
        for row in mcp.pages("evaluation.list", {}):
            evaluation = row["Evaluation"]
            evaluations.append({
                "claim": hexid(evaluation["key"]["claim"]),
                "validation": hexid(evaluation["key"]["validation"]),
                "state": evaluation.get("state"),
                "evaluator": names.get(hexid(evaluation.get("evaluator")), hexid(evaluation.get("evaluator"))),
                "begun": evaluation.get("has_begun"),
                "attempt": evaluation.get("attempt_index"),
            })
        # The whole history, every committed operation's facts in order: what claim.list no longer
        # shows of claims retired to the archive.
        events = []
        for row in mcp.pages("event.list", {}):
            event = row["Event"]
            principal = (event.get("invocation") or {}).get("Request", {}).get("principal")
            fact = event.get("fact") or {}
            family = next(iter(fact), None)
            detail = fact.get(family) if family else None
            kind = detail.get("kind") if isinstance(detail, dict) else None
            if isinstance(kind, dict):
                kind = next(iter(kind), None)
            events.append({
                "sequence": event.get("sequence"),
                "ordinal": event.get("ordinal"),
                "by": names.get(hexid(principal), hexid(principal) if principal else "owner"),
                "fact": family,
                "kind": kind,
            })
        mcp.close()
    finally:
        node.send_signal(signal.SIGTERM)
        node.wait(timeout=60)
    per_turn, totals = turns()
    json.dump({"claims": claims, "evaluations": evaluations, "events": events, "turns": per_turn,
               "totals": totals}, sys.stdout, indent=1)


main()
