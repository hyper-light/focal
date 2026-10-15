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
A function's name is read by the checker's own demangler of Rust's v0 symbols (RFC 2603), the
same on every machine: a platform's c++filt prints crate disambiguators (binutils) or no v0 name
at all (an older Xcode), and a name read wrong is a cycle judged wrong.

Each target's IR is written to a path of its own (`--emit=llvm-ir=PATH`, under the profile's
`ir` directory): cargo names a binary's artifact only where it copies it out, never the `deps`
file whose IR sits beside it. The path is one of the unit's arguments, so a unit cargo finds
fresh holds the IR its last build of the same sources wrote there, and none but this build's is
read. A unit whose IR is missing is cleaned and built again.

The IR sees direct calls only. A call through a trait object or a function pointer is not an
edge, and so a function that builds a boxed future of itself and awaits it is invisible there.
The source check covers that form: an `async fn`, or a function returning a future, that calls
itself by name (`name(`, `Self::name(`, `self.name(`) in its own body is refused.

The checker proves itself on its fixtures first (`--self-test`): symbols of the workspace that
take every production of the v0 grammar its call graphs meet are read as LLVM's demangler reads
them, a self-recursive function, a mutual recursion and the drop glue of a self-referential type
are refused, a dependency's cycle is listed and not refused, and a boxed async self-call is
refused at the source.
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
# --- Rust v0 symbol demangling (RFC 2603), without recursion -------------------------------------
#
# The checker reads its call graph's names itself rather than through a platform demangler: a
# Linux runner's GNU c++filt prints crate disambiguators and a macOS runner's Xcode c++filt prints
# no v0 name at all, so the same IR read two names on two machines. The grammar is walked by an
# explicit stack of steps: each step either parses one production at the input's position or
# writes text; a backreference parses what it names at its earlier position and comes back. A
# symbol that takes more than V0_STEPS steps, or does not parse, is kept as it is.

V0_STEPS = 200_000
BASIC = {
    "a": "i8", "b": "bool", "c": "char", "d": "f64", "e": "str", "f": "f32", "h": "u8",
    "i": "isize", "j": "usize", "l": "i32", "m": "u32", "n": "i128", "o": "u128", "s": "i16",
    "t": "u16", "u": "()", "v": "...", "x": "i64", "y": "u64", "z": "!", "p": "_",
}


class V0Error(Exception):
    pass


def demangle_v0(symbol: str) -> str:
    """`symbol`'s path, as a v0 demangler prints it without disambiguators, or `symbol` itself
    when it is no v0 name."""
    text = symbol
    for prefix in ("__R", "_R", "R"):
        if text.startswith(prefix):
            text = text[len(prefix):]
            break
    else:
        return symbol
    # A suffix after a period (`.llvm.123`) is LLVM's, not the name's.
    text = text.split(".", 1)[0]
    try:
        return _V0(text).run()
    except V0Error:
        return symbol


class _V0:
    def __init__(self, text: str):
        self.text = text
        self.pos = 0
        self.out: list[str] = []
        self.quiet = 0
        # Lifetimes bound by the binders around this point (`for<'a>`).
        self.bound = 0
        # (kind, argument) steps, last first.
        self.stack: list[tuple[str, object]] = []

    def peek(self) -> str:
        return self.text[self.pos] if self.pos < len(self.text) else ""

    def next(self) -> str:
        if self.pos >= len(self.text):
            raise V0Error("end of symbol")
        c = self.text[self.pos]
        self.pos += 1
        return c

    def eat(self, c: str) -> bool:
        if self.peek() == c:
            self.pos += 1
            return True
        return False

    def emit(self, s: str) -> None:
        if not self.quiet:
            self.out.append(s)

    def base62(self) -> int:
        """A base-62 number ended by `_`; `_` alone is 0, and every other one more than it reads."""
        if self.eat("_"):
            return 0
        value = 0
        while True:
            c = self.next()
            if c == "_":
                return value + 1
            if "0" <= c <= "9":
                digit = ord(c) - ord("0")
            elif "a" <= c <= "z":
                digit = ord(c) - ord("a") + 10
            elif "A" <= c <= "Z":
                digit = ord(c) - ord("A") + 36
            else:
                raise V0Error("base-62 digit")
            value = value * 62 + digit
            if value > 1 << 64:
                raise V0Error("base-62 overflow")

    def disambiguator(self) -> int:
        """`s <base-62>`, one more than the number; 0 when absent."""
        return self.base62() + 1 if self.eat("s") else 0

    def ident(self) -> str:
        return self.ident_numbered()[0]

    def ident_numbered(self) -> tuple[str, int]:
        """An identifier and its disambiguator: what tells a closure from its siblings."""
        number = self.disambiguator()
        punycode = self.eat("u")
        # A decimal number has no leading zero: `0` stands alone, so `00` is two lengths.
        digits = self.next() if self.peek().isdigit() else ""
        if digits not in ("", "0"):
            while self.peek().isdigit():
                digits += self.next()
        if not digits:
            raise V0Error("identifier length")
        length = int(digits)
        self.eat("_")
        if self.pos + length > len(self.text):
            raise V0Error("identifier past the end")
        name = self.text[self.pos:self.pos + length]
        self.pos += length
        return (f"{{punycode:{name}}}" if punycode else name), number

    def push(self, *steps: tuple[str, object]) -> None:
        """Run `steps` next, in their order."""
        self.stack.extend(reversed(steps))

    def run(self) -> str:
        self.push(("path", True))
        # An instantiating crate may follow the path; it is not printed.
        self.stack.insert(0, ("trailer", None))
        budget = V0_STEPS
        while self.stack:
            budget -= 1
            if budget < 0:
                raise V0Error("step budget")
            kind, arg = self.stack.pop()
            getattr(self, "step_" + kind)(arg)
        return "".join(self.out)

    # -- steps --------------------------------------------------------------------------------

    def step_text(self, s: object) -> None:
        self.emit(str(s))

    def step_quiet(self, on: object) -> None:
        self.quiet += 1 if on else -1

    def step_goto(self, pos: object) -> None:
        self.pos = int(pos)  # type: ignore[arg-type]

    def step_trailer(self, _: object) -> None:
        if self.pos < len(self.text):
            self.quiet += 1
            self.push(("path", False), ("quiet", False))

    def backref(self, kind: str, arg: object) -> None:
        start = self.pos - 1
        target = self.base62()
        if target >= start:
            raise V0Error("backreference forward")
        self.push((kind, arg), ("goto", self.pos))
        self.pos = target

    def step_path(self, in_value: object) -> None:
        c = self.next()
        if c == "C":
            self.disambiguator()
            self.emit(self.ident())
        elif c == "N":
            ns = self.next()
            self.push(("path", in_value), ("nested", ns))
        elif c == "M":
            self.disambiguator()
            self.push(("quiet", True), ("path", False), ("quiet", False),
                      ("text", "<"), ("type", None), ("text", ">"))
        elif c == "X":
            self.disambiguator()
            self.push(("quiet", True), ("path", False), ("quiet", False),
                      ("text", "<"), ("type", None), ("text", " as "), ("path", False), ("text", ">"))
        elif c == "Y":
            self.push(("text", "<"), ("type", None), ("text", " as "), ("path", False), ("text", ">"))
        elif c == "I":
            self.push(("path", in_value), ("text", "::<" if in_value else "<"),
                      ("list", ("generic", ", ", ">", 0)))
        elif c == "B":
            self.backref("path", in_value)
        else:
            raise V0Error("path")

    def step_nested(self, ns: object) -> None:
        name, number = self.ident_numbered()
        ns = str(ns)
        if ns.isupper():
            what = {"C": "closure", "S": "shim"}.get(ns, ns)
            self.emit(f"::{{{what}" + (f":{name}" if name else "") + f"#{number}}}")
        elif name:
            # An item without a name (a tuple struct's constructor) prints as its path.
            self.emit("::" + name)

    def step_list(self, spec: object) -> None:
        item, separator, close, count = spec  # type: ignore[misc]
        if self.eat("E"):
            # A tuple of one keeps its comma: `(T,)`.
            self.emit("," + close if item == "member" and count == 1 else close)
            return
        if count:
            self.emit(separator)
        self.push((item, None), ("list", (item, separator, close, count + 1)))

    def step_member(self, _: object) -> None:
        """A tuple's member: a type."""
        self.step_type(None)

    def lifetime(self, index: int) -> str:
        """Lifetime `index` by de Bruijn index: 0 is erased, 1 the innermost bound."""
        if index == 0:
            return "'_"
        depth = self.bound - index
        if depth < 0:
            raise V0Error("lifetime unbound")
        return "'" + chr(ord("a") + depth) if depth < 26 else f"'_{depth}"

    def step_unbind(self, count: object) -> None:
        self.bound -= int(count)  # type: ignore[arg-type]

    def step_generic(self, _: object) -> None:
        if self.eat("L"):
            self.emit(self.lifetime(self.base62()))
        elif self.eat("K"):
            self.step_const(None)
        else:
            self.step_type(None)

    def step_const(self, _: object) -> None:
        c = self.peek()
        if c == "B":
            self.next()
            self.backref("const", None)
            return
        if c == "p":
            self.next()
            self.emit("_")
            return
        self.next()
        negative = self.eat("n")
        digits = ""
        while self.peek() != "_":
            digits += self.next()
        self.next()
        if c == "b":
            self.emit("true" if digits == "1" else "false")
        elif c == "c":
            self.emit(repr(chr(int(digits or "0", 16))))
        else:
            self.emit(("-" if negative else "") + str(int(digits or "0", 16)))

    def binder(self) -> int:
        """`G <base-62>`: as many lifetimes bound as the number and one, named and printed."""
        if not self.eat("G"):
            return 0
        count = self.base62() + 1
        names = [self.lifetime_name(self.bound + offset) for offset in range(count)]
        self.bound += count
        self.emit("for<" + ", ".join(names) + "> ")
        return count

    def lifetime_name(self, depth: int) -> str:
        return "'" + chr(ord("a") + depth) if depth < 26 else f"'_{depth}"

    def step_type(self, _: object) -> None:
        c = self.peek()
        if c in BASIC:
            self.next()
            self.emit(BASIC[c])
            return
        self.next()
        if c == "R" or c == "Q":
            named = self.lifetime(self.base62()) if self.eat("L") else "'_"
            self.emit("&" + ("" if named == "'_" else named + " ") + ("mut " if c == "Q" else ""))
            self.push(("type", None))
        elif c == "P" or c == "O":
            self.emit("*mut " if c == "O" else "*const ")
            self.push(("type", None))
        elif c == "A":
            self.push(("text", "["), ("type", None), ("text", "; "), ("const", None), ("text", "]"))
        elif c == "S":
            self.push(("text", "["), ("type", None), ("text", "]"))
        elif c == "T":
            self.push(("text", "("), ("list", ("member", ", ", ")", 0)))
        elif c == "F":
            bound = self.binder()
            prefix = ""
            if self.eat("U"):
                prefix += "unsafe "
            if self.eat("K"):
                if self.eat("C"):
                    prefix += 'extern "C" '
                else:
                    prefix += f'extern "{self.ident()}" '
            self.emit(prefix + "fn(")
            self.push(("list", ("type", ", ", ")", 0)), ("returns", None), ("unbind", bound))
        elif c == "D":
            self.emit("dyn ")
            bound = self.binder()
            self.push(("list", ("dyn", " + ", "", 0)), ("unbind", bound), ("lifetime", None))
        elif c == "B":
            self.backref("type", None)
        else:
            self.pos -= 1
            self.push(("path", False))

    def step_returns(self, _: object) -> None:
        """A function type's return: `()` is left out, as rustc writes it."""
        if self.eat("u"):
            return
        self.emit(" -> ")
        self.push(("type", None))

    def step_lifetime(self, _: object) -> None:
        if self.next() != "L":
            raise V0Error("dyn lifetime")
        self.base62()

    def step_dyn(self, _: object) -> None:
        self.push(("path", False), ("projections", 0))

    def step_projections(self, count: object) -> None:
        # A trait's associated type bindings, `p <ident> <type>`, inside its generic arguments:
        # `FnOnce<(T,), Output = ()>`, or `Iterator<Item = T>` where it has none.
        if not self.eat("p"):
            if count:
                self.emit(">")
            return
        name = self.ident()
        if count:
            self.emit(", ")
        elif not self.quiet and self.out and self.out[-1].endswith(">"):
            self.out[-1] = self.out[-1][:-1]
            self.emit(", ")
        else:
            self.emit("<")
        self.emit(f"{name} = ")
        self.push(("type", None), ("projections", int(count) + 1))


def demangle_all(symbols: set[str]) -> dict[str, str]:
    """Each symbol's demangled path, read here the same on every machine."""
    return {symbol: demangle_v0(symbol.strip('"')) for symbol in symbols}


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
    # Symbols of the workspace, each read as LLVM's demangler reads it: every production of the
    # grammar the call graph meets (295,468 symbols of the workspace's IR read the same, 2026-10-11).
    for symbol, read in (
        ('_RNvMs0_CsekvDPWo0cJz_10focal_coreNtB5_4Core3new',
         '<focal_core::Core>::new'),
        ('_RNvXs3_NtCsvUqMUTWekE_10focal_wire5peersNtB5_4SlotNtNtNtCs8Mbv00yxnRz_4core3ops4drop4Drop4drop',
         '<focal_wire::peers::Slot as core::ops::drop::Drop>::drop'),
        ('_RNCNvNtCs12HxQLK2ftI_12focal_memory6budget3ends_0B5_',
         'focal_memory::budget::end::{closure#1}'),
        ('_RNvYNcNtNtCs12HxQLK2ftI_12focal_memory5owner7OwnerId0INtNtNtCs8Mbv00yxnRz_4core3ops8function6FnOnceTyEE9call_onceB8_',
         '<focal_memory::owner::OwnerId as core::ops::function::FnOnce<(u64,)>>::call_once'),
        ('_RNvMNtCs8Mbv00yxnRz_4core6optionINtB2_6OptionFG_RL0_NtNtCsekvDPWo0cJz_10focal_core6native3KeyEyE7is_someBS_',
         "<core::option::Option<for<'a> fn(&'a focal_core::native::Key) -> u64>>::is_some"),
        ('_RINvNtCs8Mbv00yxnRz_4core3ptr9drop_glueDNtNtNtB4_6future6future6Futurep6OutputINtNtB4_6option6OptionNtNtNtCsvUqMUTWekE_10focal_wire3net5owner5ReplyEEL_EB1G_',
         'core::ptr::drop_glue::<dyn core::future::future::Future<Output = core::option::Option<focal_wire::net::owner::Reply>>>'),
        ('_RINvNtNtCsekvDPWo0cJz_10focal_core6native16projection_quote3sumKj4_EB6_',
         'focal_core::native::projection_quote::sum::<4>'),
        ('_RNvXNtCs8Mbv00yxnRz_4core6borrowNtNtCsekvDPWo0cJz_10focal_core6native3KeyINtB2_6BorrowBu_E6borrowBy_',
         '<focal_core::native::Key as core::borrow::Borrow<focal_core::native::Key>>::borrow'),
        ('_RNvMNtCs12HxQLK2ftI_12focal_memory5ownerNtB2_7OwnerId3new',
         '<focal_memory::owner::OwnerId>::new'),
        ('_RNcNtINtNtCs8Mbv00yxnRz_4core6option6OptionAhj10_E4Some0CsekvDPWo0cJz_10focal_core',
         'core::option::Option::<[u8; 16]>::Some'),
        ('_RINvNtCs8Mbv00yxnRz_4core3cmp6max_byyQNvYyNtB2_3Ord3cmpECsekvDPWo0cJz_10focal_core',
         'core::cmp::max_by::<u64, &mut <u64 as core::cmp::Ord>::cmp>'),
    ):
        if demangle_v0(symbol) != read:
            raise SystemExit(f"self-test: {symbol} read as {demangle_v0(symbol)!r}, not {read!r}")
    if demangle_v0("_RNvCs1234_7mycrate") != "_RNvCs1234_7mycrate":
        raise SystemExit("self-test: a symbol cut short was read rather than kept")
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
