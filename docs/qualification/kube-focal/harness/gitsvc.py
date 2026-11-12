#!/usr/bin/env python3
"""The kube-focal harness's git service.

Codex's workspace-write sandbox keeps each agent's .git read-only (a hook or a config line written
there would run outside the sandbox the next time git runs). The agents still have to commit,
branch, fetch and merge, so the harness does those writes for them, on the agent's own clone only,
with hooks and fsmonitor off and no user or system git config read: nothing an agent writes in its
working tree can make these commands run code. One request at a time per clone; every request is
logged. Bounds: a request body of 64 KiB, a commit message of 8 KiB, 64 KiB of output returned
(its tail), 600 s per git command.

Run: gitsvc.py (listens on 127.0.0.1:47100 until stopped).
"""
import json
import os
import re
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

K = "/Users/adalundhe/fc2k"
PORT = 47100
REPOS = {
    f"{K}/kube": "architect",
    f"{K}/wt/worker-1": "worker-1",
    f"{K}/wt/worker-2": "worker-2",
    f"{K}/wt/worker-3": "worker-3",
    f"{K}/wt/evaluator": "evaluator",
}
LOCKS = {repo: threading.Lock() for repo in REPOS}
LOG_LOCK = threading.Lock()
LOG = f"{K}/runs/git-service.log"
MAX_BODY = 64 << 10
MAX_MESSAGE = 8 << 10
MAX_OUT = 64 << 10
TIMEOUT = 600
NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]{0,127}$")
REF = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/~^-]{0,199}$")


def git(repo, author, *args):
    cmd = [
        "git",
        "-c", "core.hooksPath=/dev/null",
        "-c", "core.fsmonitor=false",
        "-c", "protocol.file.allow=always",
        "-c", f"user.name={author}",
        "-c", f"user.email={author}@kube-focal.invalid",
        "-C", repo,
        *args,
    ]
    env = {
        "PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
        "HOME": os.environ.get("HOME", "/tmp"),
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_TERMINAL_PROMPT": "0",
        "LANG": "C",
    }
    try:
        done = subprocess.run(cmd, capture_output=True, text=True, timeout=TIMEOUT, env=env)
    except subprocess.TimeoutExpired:
        return 124, f"git {args[0]} took more than {TIMEOUT} s and was stopped\n"
    return done.returncode, done.stdout + done.stderr


def checked(pattern, value, what):
    if not isinstance(value, str) or not pattern.match(value):
        raise ValueError(f"{what} must match {pattern.pattern}")
    return value


def run(repo, author, op, args):
    if op == "fetch":
        return git(repo, author, "fetch", "--all", "--prune")
    if op == "branch":
        name = checked(NAME, args.get("name"), "name")
        start = checked(REF, args.get("start"), "start")
        return git(repo, author, "checkout", "-B", name, start)
    if op == "checkout":
        return git(repo, author, "checkout", "--detach", checked(REF, args.get("ref"), "ref"))
    if op == "commit":
        message = args.get("message")
        if not isinstance(message, str) or not message.strip() or len(message) > MAX_MESSAGE:
            raise ValueError(f"message must be 1 to {MAX_MESSAGE} characters")
        code, out = git(repo, author, "add", "-A")
        if code != 0:
            return code, out
        code, more = git(repo, author, "commit", "-m", message)
        return code, out + more
    if op == "merge":
        ref = checked(REF, args.get("ref"), "ref")
        message = args.get("message") or f"Merge {ref}"
        if not isinstance(message, str) or len(message) > MAX_MESSAGE:
            raise ValueError(f"message must be at most {MAX_MESSAGE} characters")
        return git(repo, author, "merge", "--no-ff", "-m", message, ref)
    if op == "merge-abort":
        return git(repo, author, "merge", "--abort")
    if op == "reset":
        return git(repo, author, "reset", "--hard", checked(REF, args.get("ref"), "ref"))
    raise ValueError(f"unknown op {op!r}")


def log(line):
    with LOG_LOCK, open(LOG, "a") as out:
        out.write(line + "\n")


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        started = time.time()
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0 or length > MAX_BODY:
            return self.answer(400, {"code": 2, "out": f"a request is 1 to {MAX_BODY} bytes\n"})
        try:
            request = json.loads(self.rfile.read(length))
            repo = os.path.realpath(request["repo"])
            if repo not in REPOS:
                raise ValueError(f"{repo} is not one of the build's clones")
            op = request["op"]
            args = request.get("args") or {}
            with LOCKS[repo]:
                code, out = run(repo, REPOS[repo], op, args)
                _, head = git(repo, REPOS[repo], "rev-parse", "HEAD")
        except (ValueError, KeyError, TypeError, json.JSONDecodeError) as error:
            log(f"{time.strftime('%H:%M:%S')} refused {error}")
            return self.answer(400, {"code": 2, "out": f"refused: {error}\n"})
        if len(out) > MAX_OUT:
            out = "[earlier output cut]\n" + out[-MAX_OUT:]
        log(
            f"{time.strftime('%H:%M:%S')} {REPOS[repo]} {op} {json.dumps(args)[:200]} -> {code} "
            f"head {head.strip()[:12]} in {time.time() - started:.1f} s"
        )
        self.answer(200, {"code": code, "out": out, "head": head.strip()})

    def answer(self, status, body):
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *_):
        pass


if __name__ == "__main__":
    server = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    server.daemon_threads = True
    log(f"{time.strftime('%Y-%m-%d %H:%M:%S')} listening on 127.0.0.1:{PORT}")
    server.serve_forever()
