#!/usr/bin/env python3
"""Exercise one already-built native executable; never invokes Cargo."""

import argparse
import json
from pathlib import Path
import queue
import signal
import subprocess
import tempfile
import threading
import time
import tomllib


ROOT = Path(__file__).resolve().parents[2]


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def command(binary, directory, *arguments, timeout=30):
    result = subprocess.run(
        [str(binary), "--data-dir", str(directory), *arguments],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    require(
        result.returncode == 0,
        f"command {arguments!r} exited {result.returncode}: "
        f"{result.stdout[-4000:]} {result.stderr[-4000:]}",
    )
    return result.stdout


def structured(binary, directory, *arguments):
    return json.loads(command(binary, directory, *arguments, "--format", "json"))


def start(binary, directory, log):
    process = subprocess.Popen(
        [str(binary), "--data-dir", str(directory), "start"],
        stdout=log,
        stderr=log,
    )
    deadline = time.monotonic() + 45
    try:
        last = "no status response"
        while time.monotonic() < deadline:
            require(process.poll() is None, "server exited before readiness")
            try:
                command(binary, directory, "status", timeout=5)
                return process
            except (RuntimeError, subprocess.TimeoutExpired) as error:
                last = str(error)
                time.sleep(0.1)
        raise RuntimeError(f"server never became ready: {last}")
    except BaseException:
        process.kill()
        process.wait(timeout=10)
        raise


def finish(process, crash=False):
    if process.poll() is not None:
        require(False, f"server unexpectedly exited: {process.returncode}")
    if crash:
        process.kill()
    else:
        process.send_signal(signal.SIGTERM)
    try:
        code = process.wait(timeout=20)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=10)
        raise RuntimeError("server shutdown exceeded its bound") from None
    if not crash:
        require(code == 0, f"server shutdown failed: {code}")


def mcp(binary, directory, claim_id, marker, log):
    process = subprocess.Popen(
        [str(binary), "--data-dir", str(directory), "mcp", "serve"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=log,
        text=True,
        bufsize=1,
    )
    lines = queue.Queue(maxsize=16)

    def read_lines():
        for line in process.stdout:
            lines.put(line)
        lines.put(None)

    reader = threading.Thread(target=read_lines, daemon=True)
    reader.start()

    def send(value):
        process.stdin.write(json.dumps(value, separators=(",", ":")) + "\n")
        process.stdin.flush()

    def rpc(identifier, method, params):
        send({"jsonrpc": "2.0", "id": identifier, "method": method, "params": params})
        try:
            line = lines.get(timeout=20)
        except queue.Empty:
            raise RuntimeError(f"MCP {method} timed out") from None
        require(line is not None, f"MCP closed before {method}")
        reply = json.loads(line)
        require(reply.get("jsonrpc") == "2.0", "MCP did not return JSON-RPC")
        require(reply.get("id") == identifier, f"unexpected MCP response: {reply}")
        require("error" not in reply and "result" in reply, f"MCP refused {method}: {reply}")
        return reply["result"]

    try:
        initialized = rpc(1, "initialize", {
            "protocolVersion": "2025-11-25",
            "capabilities": {},
            "clientInfo": {"name": "focal-release-smoke", "version": "1"},
        })
        require(initialized.get("protocolVersion") == "2025-11-25", "MCP negotiation changed")
        require("tools" in initialized.get("capabilities", {}), "MCP tools capability missing")
        send({"jsonrpc": "2.0", "method": "notifications/initialized"})
        names = set()
        cursor = None
        for identifier in range(2, 18):
            page = rpc(identifier, "tools/list", {} if cursor is None else {"cursor": cursor})
            names.update(tool["name"] for tool in page["tools"])
            cursor = page.get("nextCursor")
            if cursor is None:
                break
        require(cursor is None, "MCP catalog exceeded release smoke page bound")
        require({"claim.get", "claim.submit", "testament.submit"} <= names, "MCP family tools missing")
        result = rpc(20, "tools/call", {"name": "claim.get", "arguments": {"id": claim_id}})
        require(not result.get("isError", False), f"MCP claim read failed: {result}")
        require(marker in json.dumps(result.get("structuredContent")), "MCP did not read committed claim")
        process.stdin.close()
        require(process.wait(timeout=20) == 0, "MCP did not close cleanly on EOF")
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=10)
        reader.join(timeout=2)
        process.stdout.close()
        if not process.stdin.closed:
            process.stdin.close()


def smoke(binary):
    binary = binary.resolve(strict=True)
    version = tomllib.loads((ROOT / "Cargo.toml").read_text())["workspace"]["package"]["version"]
    # A short private path also fits macOS's Unix-socket path limit.
    with tempfile.TemporaryDirectory(prefix="focal-release-", dir="/tmp") as temporary:
        directory = Path(temporary) / "ledger"
        with (Path(temporary) / "service.log").open("w+") as log:
            server = None
            try:
                require(command(binary, directory, "--version").strip() == f"focal {version}", "wrong binary version")
                require("start" in command(binary, directory, "--help"), "server command missing")
                document = json.loads(command(binary, directory, "schema", "example", "claim.submit"))
                marker = "release smoke: durable server and peer clients"
                document["description"] = marker
                server = start(binary, directory, log)
                submitted = structured(binary, directory, "submit", "claim", "--json", json.dumps(document))
                require(submitted.get("condition") == "Committed", "claim submission was not committed")
                ids = submitted["result"]["claims"]
                require(len(ids) == 1, "submission did not create exactly one claim")
                claim_id = ids[0]
                posted = structured(binary, directory, "claim", "post", claim_id)
                require(posted.get("condition") == "Committed", "claim post was not committed")
                before = structured(binary, directory, "get", "claim", claim_id)
                require(before["result"]["id"] == claim_id, "wrong returned claim")
                require(marker in json.dumps(before["result"]), "authored content was lost")
                listed = structured(binary, directory, "list", "claims")
                require([row["id"] for row in listed["results"]] == [claim_id], "unexpected ledger contents")
                mcp(binary, directory, claim_id, marker, log)
                # Kill without graceful shutdown: only an acknowledged durable
                # write may be relied upon by the fresh process below.
                finish(server, crash=True)
                server = None
                server = start(binary, directory, log)
                recovered = structured(binary, directory, "get", "claim", claim_id)
                require(recovered["result"] == before["result"], "crash recovery changed the committed object")
                require(recovered["token"]["sequence"] >= before["token"]["sequence"], "recovery regressed prefix")
                mcp(binary, directory, claim_id, marker, log)
                finish(server)
                server = None
            except BaseException:
                log.flush()
                log.seek(0)
                print(log.read()[-16000:])
                raise
            finally:
                if server is not None and server.poll() is None:
                    server.kill()
                    server.wait(timeout=10)
    print(f"PASS native server, CLI, durable crash recovery and MCP: {binary.name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    smoke(parser.parse_args().binary)
