#!/usr/bin/env python3
"""No runtime recursion (CLAUDE.md §1a), checked on the compiled code and on the source.

The call graph: every library and binary target of the workspace, and every vendored
`hyper-*` crate, is compiled to LLVM IR at opt-level 0 (`--profile recursion-ir`), where no
call is inlined and every direct call, drop glue's included, is an explicit `call` or `invoke`.
The direct-call graph's strongly connected components are found with Tarjan's algorithm, run
iteratively. A component of more than one function, or a function that calls itself, is a
cycle: refused when it passes through a function of the workspace or of a `hyper-*` crate, or
the drop glue of one of their types (`drop_in_place`, and `drop_glue` as rustc 1.98 names it), or
when it is teardown alone (drops, a channel's `release`, `disconnect`, `discard_all_messages`)
whose functions name one of their types among their generic arguments (a message that holds the
receiving end of its own channel); listed when it lies inside a third-party dependency.

Each target's IR is written to a path of its own (`--emit=llvm-ir=PATH`, under the profile's
`ir` directory): cargo names a binary's artifact only where it copies it out, never the `deps`
file whose IR sits beside it. The path is one of the unit's arguments, so a unit cargo finds
fresh holds the IR its last build of the same sources wrote there, and none but this build's is
read. A unit whose IR is missing is cleaned and built again.

The IR sees direct calls only. A call through a trait object or a function pointer is not an
edge, and so a function that builds a boxed future of itself and awaits it is invisible there.
The source check covers that form: an `async fn`, or a function returning a future, that calls
itself by name (`name(`, `Self::name(`, `self.name(`) in its own body is refused.

The checker proves itself on its fixtures first (`--self-test`): a self-recursive function, a
mutual recursion and the drop glue of a self-referential type are refused, a dependency's
cycle is listed and not refused, and a boxed async self-call is refused at the source.
"""
from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

import check_production_policy as policy

ROOT = Path(__file__).resolve().parent.parent
FIXTURES = ROOT / "scripts" / "fixtures" / "recursion"
PROFILE = "recursion-ir"
SOURCE_ROOTS = ("crates", "tools")
TEST_TREE = re.compile(r"(^|/)(tests|benches|examples)(/|$)")

DEFINE = re.compile(r'^define\b[^@]*@("(?:[^"\\]|\\.)*"|[-a-zA-Z$._0-9]+)\(')
CALL = re.compile(r'\b(?:call|invoke)\b[^@]*?@("(?:[^"\\]|\\.)*"|[-a-zA-Z$._0-9]+)\(')
def demangler() -> list[str]:
    """The command that demangles Rust's v0 symbols here: the toolchain's `llvm-cxxfilt` (rustup's
    `llvm-tools`), else the platform's `c++filt` (LLVM's, or binutils' since 2.36). Refused, never
    skipped, when neither does."""
    sysroot = subprocess.run(
        ["rustc", "--print", "sysroot"], check=True, capture_output=True, text=True
    ).stdout.strip()
    host = subprocess.run(
        ["rustc", "--print", "host-tuple"], check=False, capture_output=True, text=True
    ).stdout.strip()
    candidates = [
        [str(Path(sysroot) / "lib" / "rustlib" / host / "bin" / "llvm-cxxfilt")],
        ["llvm-cxxfilt"],
        ["c++filt"],
    ]
    for command in candidates:
        try:
            probe = subprocess.run(
                command, input="_RNvCs1234_7mycrate3foo\n", capture_output=True, text=True, check=False
            )
        except OSError:
            continue
        if probe.stdout.strip() == "mycrate::foo":
            return command
    raise SystemExit(
        "no demangler of Rust's v0 symbols: install rustup's llvm-tools component "
        "(`rustup component add llvm-tools`) or binutils 2.36 or later"
    )


def demangle_all(symbols: set[str]) -> dict[str, str]:
    """Each symbol's demangled path, all in one run of the demangler."""
    ordered = sorted(symbols)
    if not ordered:
        return {}
    names = subprocess.run(
        demangler(),
        input="\n".join(symbol.strip('"') for symbol in ordered) + "\n",
        capture_output=True,
        text=True,
        check=True,
    ).stdout.splitlines()
    return dict(zip(ordered, names))


def read_graph(files: list[Path]) -> dict[str, set[str]]:
    """The direct-call graph of the IR in `files`: each defined function and the functions it
    calls, intrinsics left out."""
    graph: dict[str, set[str]] = {}
    for path in files:
        current: str | None = None
        with path.open("r", encoding="utf-8", errors="replace") as lines:
            for line in lines:
                if current is None:
                    defined = DEFINE.match(line)
                    if defined:
                        current = defined.group(1)
                        graph.setdefault(current, set())
                    continue
                if line.startswith("}"):
                    current = None
                    continue
                for called in CALL.finditer(line):
                    callee = called.group(1)
                    if not callee.strip('"').startswith("llvm."):
                        graph[current].add(callee)
    return graph


def components(graph: dict[str, set[str]]) -> list[list[str]]:
    """The cycles of `graph`: its strongly connected components of more than one function, and
    each function that calls itself (Tarjan's algorithm, with an explicit stack: no recursion)."""
    index: dict[str, int] = {}
    low: dict[str, int] = {}
    on_stack: set[str] = set()
    stack: list[str] = []
    found: list[list[str]] = []
    counter = 0
    for root in graph:
        if root in index:
            continue
        work: list[tuple[str, list[str]]] = [(root, sorted(graph.get(root, ())))]
        index[root] = low[root] = counter
        counter += 1
        stack.append(root)
        on_stack.add(root)
        while work:
            node, pending = work[-1]
            if pending:
                callee = pending.pop()
                if callee not in index:
                    index[callee] = low[callee] = counter
                    counter += 1
                    stack.append(callee)
                    on_stack.add(callee)
                    work.append((callee, sorted(graph.get(callee, ()))))
                elif callee in on_stack:
                    low[node] = min(low[node], index[callee])
                continue
            work.pop()
            if work:
                parent = work[-1][0]
                low[parent] = min(low[parent], low[node])
            if low[node] == index[node]:
                component: list[str] = []
                while True:
                    member = stack.pop()
                    on_stack.discard(member)
                    component.append(member)
                    if member == node:
                        break
                if len(component) > 1 or node in graph.get(node, ()):
                    found.append(component)
    return found


def ours(path: str, crates: set[str]) -> bool:
    """Whether the demangled function `path` is code of `crates`: defined in one of them, an
    impl of one of their types, or the drop glue of one."""
    head = path.split("::", 1)[0].lstrip("<").lstrip("&").lstrip("*")
    if head in crates:
        return True
    if path.startswith("<"):
        inner = path[1:].split(" as ", 1)[0].lstrip("&").lstrip("*")
        if inner.split("::", 1)[0] in crates:
            return True
    for glue in (
        "core::ptr::drop_in_place::<",
        "core::ptr::drop_in_place<",
        "core::ptr::drop_glue::<",
        "core::ptr::drop_glue<",
    ):
        if path.startswith(glue):
            argument = path[len(glue) :].lstrip("<").lstrip("&").lstrip("*")
            if argument.split("::", 1)[0] in crates:
                return True
    return False


TEARDOWN = {"release", "disconnect", "disconnect_receivers", "disconnect_senders", "discard_all_messages"}


def teardown(path: str) -> bool:
    """Whether the demangled function `path` takes something down: drop glue, a `Drop` impl, or a
    channel's release of its ends and messages."""
    if "drop_in_place" in path or "drop_glue" in path or "core::ops::drop::Drop>::drop" in path:
        return True
    return path.rsplit("::", 1)[-1] in TEARDOWN


def names_ours(path: str, crates: set[str]) -> bool:
    """Whether `path` names a type of `crates` anywhere in it, generic arguments included."""
    return any(re.search(rf"(?<![A-Za-z0-9_]){re.escape(crate)}::", path) for crate in crates)


def judge(cycles: list[list[str]], crates: set[str]) -> tuple[list[list[str]], list[list[str]]]:
    """The cycles refused (through the workspace's code, or teardown of its types) and those listed
    (a dependency's)."""
    names = demangle_all({symbol for cycle in cycles for symbol in cycle})
    refused: list[list[str]] = []
    listed: list[list[str]] = []
    for cycle in cycles:
        paths = sorted({names.get(symbol, symbol) for symbol in cycle})
        through = any(ours(path, crates) for path in paths)
        torn = all(teardown(path) for path in paths) and any(
            names_ours(path, crates) for path in paths
        )
        (refused if through or torn else listed).append(paths)
    return refused, listed


FN = re.compile(r"\b(async\s+)?fn\s+([A-Za-z_][A-Za-z0-9_]*)\s*(<[^{;]*?>)?\s*\(")
IMPL = re.compile(r"\b(?:impl|trait)\b[^{;]*\{")


def strip_noise(text: str) -> str:
    """`text` with comments, string, char and byte literals blanked to spaces (positions kept),
    so a brace or a name inside one counts for nothing."""
    out = list(text)
    at = 0
    length = len(text)
    while at < length:
        char = text[at]
        nxt = text[at + 1] if at + 1 < length else ""
        if char == "/" and nxt == "/":
            end = text.find("\n", at)
            end = length if end < 0 else end
            for i in range(at, end):
                out[i] = " "
            at = end
        elif char == "/" and nxt == "*":
            depth = 0
            i = at
            while i < length:
                if text.startswith("/*", i):
                    depth += 1
                    i += 2
                elif text.startswith("*/", i):
                    depth -= 1
                    i += 2
                    if depth == 0:
                        break
                else:
                    i += 1
            for j in range(at, min(i, length)):
                if out[j] != "\n":
                    out[j] = " "
            at = i
        elif char == "r" and re.match(r'r#*"', text[at:at + 260] or ""):
            hashes = len(re.match(r"r(#*)", text[at:]).group(1))
            close = '"' + "#" * hashes
            start = text.index('"', at) + 1
            end = text.find(close, start)
            end = length if end < 0 else end + len(close)
            for j in range(at, end):
                if out[j] != "\n":
                    out[j] = " "
            at = end
        elif char == '"':
            i = at + 1
            while i < length and text[i] != '"':
                i += 2 if text[i] == "\\" else 1
            for j in range(at, min(i + 1, length)):
                if out[j] != "\n":
                    out[j] = " "
            at = i + 1
        elif char == "'":
            literal = re.match(r"'(\\.|\\u\{[0-9a-fA-F]+\}|\\x[0-9a-fA-F]{2}|[^\\'])'", text[at:])
            if literal:
                for j in range(at, at + literal.end()):
                    out[j] = " "
                at += literal.end()
            else:
                at += 1
        else:
            at += 1
    return "".join(out)


def body_after(text: str, start: int) -> tuple[int, int] | None:
    """The span of the body of the function whose parameter list opened just before `start`:
    from its opening brace to just past the brace closing it; `None` for a declaration without
    a body."""
    depth_paren = 1
    at = start
    while at < len(text):
        char = text[at]
        if char in "([":
            depth_paren += 1
        elif char in ")]":
            depth_paren -= 1
        elif char == ";" and depth_paren == 0:
            return None
        elif char == "{" and depth_paren == 0:
            break
        at += 1
    else:
        return None
    open_at = at
    depth = 0
    while at < len(text):
        if text[at] == "{":
            depth += 1
        elif text[at] == "}":
            depth -= 1
            if depth == 0:
                return open_at, at + 1
        at += 1
    return open_at, len(text)


def source_findings(files: list[Path]) -> list[str]:
    """Each future-returning function of production code that calls itself by name in its own
    body; code a test build alone compiles (`#[cfg(test)]` items and the files only they include)
    is not production code."""
    found: list[str] = []
    gated = policy.gated_files(files)
    for path in files:
        if path.resolve() in gated:
            continue
        raw = path.read_text(encoding="utf-8", errors="replace")
        regions, _ = policy.test_regions(raw, path.parent)
        text = strip_noise(raw)
        impls = [
            (block.start(), policy.matching_brace(text, block.end() - 1))
            for block in IMPL.finditer(text)
        ]
        for match in FN.finditer(text):
            if any(begin <= match.start() < finish for begin, finish in regions):
                continue
            name = match.group(2)
            span = body_after(text, match.end())
            if span is None:
                continue
            signature = text[match.start() : span[0]]
            returns = signature.split("->", 1)[1] if "->" in signature else ""
            is_future = bool(match.group(1)) or re.search(
                r"\bFuture\b|\bBoxFuture\b|Pin\s*<\s*Box\s*<", returns
            )
            if not is_future:
                continue
            body = text[span[0] : span[1]]
            # Inside an `impl` or a `trait` only `Self::name(` and `self.name(` call this function:
            # a bare `name(` there is a free function's. Nested items of the same name are not it.
            in_impl = any(begin <= match.start() < finish for begin, finish in impls)
            callers = r"(?:\bSelf::|\bself\.)" if in_impl else r"(?:(?<![A-Za-z0-9_.:])|\bSelf::|\bself\.)"
            calls = re.finditer(
                callers + re.escape(name) + r"\s*(?:::<[^>]*>)?\s*\(",
                body,
            )
            nested = [m.start() for m in FN.finditer(body) if m.group(2) == name]
            for call in calls:
                if any(call.start() >= start for start in nested):
                    continue
                line = raw.count("\n", 0, span[0] + call.start()) + 1
                found.append(f"{path.relative_to(ROOT) if path.is_relative_to(ROOT) else path}:{line}: `{name}` calls itself")
                break
    return found


def production_sources() -> list[Path]:
    """The workspace's sources outside test trees, and the vendored `hyper-*` crates' sources."""
    files: list[Path] = list(policy.production_sources(ROOT))
    vendor = ROOT / "vendor"
    for crate in sorted(vendor.glob("hyper-*")):
        for path in sorted((crate / "src").rglob("*.rs")):
            relative = path.relative_to(ROOT).as_posix()
            if "/tests/" in relative or relative.endswith("/tests.rs"):
                continue
            files.append(path)
    return files


def targets() -> tuple[list[tuple[str, str, str]], set[str], Path]:
    """Each library and binary target to compile, as (package, kind, name), the crate names the
    workspace's code lives under (its members' and the vendored `hyper-*` crates'), and cargo's
    target directory."""
    metadata = json.loads(
        subprocess.run(
            ["cargo", "metadata", "--format-version", "1", "--locked"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    )
    members = set(metadata["workspace_members"])
    wanted: list[tuple[str, str, str]] = []
    crates: set[str] = set()
    for package in metadata["packages"]:
        manifest = Path(package["manifest_path"]).resolve()
        vendored = manifest.parent.parent == ROOT / "vendor" and package["name"].startswith("hyper-")
        if package["id"] not in members and not vendored:
            continue
        for target in package["targets"]:
            kinds = set(target["kind"])
            crates.add(target["name"].replace("-", "_"))
            if kinds & {"lib", "rlib"}:
                wanted.append((package["name"], "lib", target["name"]))
            elif "bin" in kinds and package["id"] in members:
                wanted.append((package["name"], "bin", target["name"]))
    return wanted, crates, Path(metadata["target_directory"])


def unit_ir(package: str, kind: str, name: str, target_directory: Path) -> Path | None:
    """Compiles one target to LLVM IR under the `recursion-ir` profile, or finds it fresh: the IR
    rustc writes to a path of this target's own (`--emit=llvm-ir=PATH`), or `None` when that file is
    missing. Cargo names a binary's artifact only where it copies it out, never the `deps` file whose
    IR sits beside it, so the IR is not looked for beside an artifact. The path is part of the
    unit's arguments, so a fresh unit's file is the one its last build of these sources wrote."""
    selector = ["--lib"] if kind == "lib" else ["--bin", name]
    out = target_directory / PROFILE / "ir" / f"{package}.{kind}.{name}.ll"
    out.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            "bash",
            "scripts/cargo.sh",
            "rustc",
            "-p",
            package,
            *selector,
            "--profile",
            PROFILE,
            "--locked",
            "--",
            f"--emit=llvm-ir={out}",
        ],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return out if out.exists() else None


def build_ir(wanted: list[tuple[str, str, str]], target_directory: Path) -> list[Path]:
    """Each target's IR under the `recursion-ir` profile, from this build alone (module doc). A unit
    whose IR is missing (removed, or never written) is cleaned and built again."""
    files: list[Path] = []
    for package, kind, name in wanted:
        ir = unit_ir(package, kind, name, target_directory)
        if ir is None:
            subprocess.run(
                ["bash", "scripts/cargo.sh", "clean", "-p", package, "--profile", PROFILE],
                cwd=ROOT,
                check=True,
            )
            ir = unit_ir(package, kind, name, target_directory)
        if ir is None:
            raise SystemExit(f"recursion checker: no IR for {package} {kind} {name}")
        files.append(ir)
    return sorted(set(files))


def report(refused: list[list[str]], listed: list[list[str]], sources: list[str]) -> int:
    for cycle in listed:
        print("dependency cycle (listed):", " -> ".join(cycle[:6]), "…" if len(cycle) > 6 else "")
    for cycle in refused:
        print("RECURSION:", " -> ".join(cycle[:12]), "…" if len(cycle) > 12 else "")
    for finding in sources:
        print("RECURSION (source):", finding)
    if refused or sources:
        print(
            f"refused: {len(refused)} call-graph cycle(s) through the workspace's code and "
            f"{len(sources)} self-calling future(s); CLAUDE.md §1a",
            file=sys.stderr,
        )
        return 1
    print(f"no runtime recursion in the workspace's code ({len(listed)} dependency cycle(s) listed)")
    return 0


def self_test() -> None:
    """The checker on its fixtures: each refusal it must make, and the listing it must not refuse."""
    crates = {"focal_fixture"}
    graph = read_graph(sorted((FIXTURES / "ir").glob("*.ll")))
    refused, listed = judge(components(graph), crates)
    refused_names = {name for cycle in refused for name in cycle}
    expected = {
        "focal_fixture::walk",
        "focal_fixture::even",
        "focal_fixture::odd",
        "core::ptr::drop_in_place::<focal_fixture::Node>",
        "core::ptr::drop_glue::<focal_fixture::Tree>",
        "std::sync::mpmc::list::Channel<focal_fixture::Reply>::discard_all_messages",
    }
    missing = expected - refused_names
    if missing:
        raise SystemExit(f"self-test: cycles not refused: {sorted(missing)}")
    if "focal_fixture::iterate" in refused_names:
        raise SystemExit("self-test: a loop was taken for recursion")
    if not any("serde_fixture::nest" in cycle for cycle in listed):
        raise SystemExit("self-test: a dependency's cycle was not listed")
    if not any(
        "std::sync::mpmc::list::Channel<serde_fixture::Message>::discard_all_messages" in cycle
        for cycle in listed
    ):
        raise SystemExit("self-test: a dependency's teardown cycle was not listed")
    found = source_findings(sorted((FIXTURES / "src").glob("*.rs")))
    if not any("boxed_again" in finding for finding in found):
        raise SystemExit("self-test: a boxed async self-call was not refused")
    if any("delegates" in finding for finding in found):
        raise SystemExit("self-test: a call of a field's method was taken for a self-call")


def main() -> int:
    self_test()
    if "--self-test" in sys.argv:
        print("recursion checker: fixtures refused and listed as they must be")
        return 0
    wanted, crates, target_directory = targets()
    ir = build_ir(wanted, target_directory)
    refused, listed = judge(components(read_graph(ir)), crates)
    return report(refused, listed, source_findings(production_sources()))


if __name__ == "__main__":
    sys.exit(main())
