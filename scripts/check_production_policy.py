#!/usr/bin/env python3
"""The production no-panic policy on the source itself (CLAUDE.md §1).

Clippy denies the policy's lints on every library and binary target
(`scripts/check-production.sh`), but a `deny` is a level a local `#[allow]`
overrides, and `forbid` cannot be used because derive macros (clap's among
them) emit allowances of their own. So the policy is also checked where an
allowance would be written: no production source may carry an `allow` of one
of the policy's lints. An allowance counts as production unless it is scoped
to test builds — `cfg_attr(test, allow(..))`, an attribute inside an item
gated by `#[cfg(test)]`, or a file that only a `#[cfg(test)]` module includes
— or lives in a test or bench tree. The policy is proved on itself first
(`--self-test`): a fixture with a production allowance is refused, one whose
allowances are test-only passes.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FIXTURES = ROOT / "scripts" / "fixtures" / "production-policy"
SOURCE_ROOTS = ("crates", "tools")
LINTS = (
    "panic",
    "unwrap_used",
    "expect_used",
    "unreachable",
    "indexing_slicing",
    "arithmetic_side_effects",
    "disallowed_macros",
    "disallowed_methods",
    "todo",
    "unimplemented",
    "dbg_macro",
)
LINT = re.compile(r"\bclippy::(" + "|".join(LINTS) + r")\b")
ALLOW = re.compile(r"#!?\[\s*allow\s*\(")
CFG_TEST = re.compile(r"#\[\s*cfg\s*\(\s*(?:test|any\s*\([^)]*\btest\b[^)]*\))\s*\)\s*\]")
PATH_ATTR = re.compile(r'#\[\s*path\s*=\s*"([^"]+)"\s*\]')
MOD_ITEM = re.compile(r"\bmod\s+(\w+)\s*(\{|;)")
TEST_TREE = re.compile(r"(^|/)(tests|benches|examples)(/|$)")


def matching_brace(text: str, open_at: int) -> int:
    """Index just past the brace that closes the one at `open_at`."""
    depth = 0
    at = open_at
    while at < len(text):
        char = text[at]
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return at + 1
        at += 1
    return len(text)


def attribute_end(text: str, start: int) -> int:
    """Index just past the `]` that closes the attribute opening at `start`."""
    depth = 0
    at = text.index("[", start)
    while at < len(text):
        char = text[at]
        if char == "[":
            depth += 1
        elif char == "]":
            depth -= 1
            if depth == 0:
                return at + 1
        at += 1
    return len(text)


def test_regions(text: str, directory: Path) -> tuple[list[tuple[int, int]], set[Path]]:
    """Spans of items gated by `#[cfg(test)]`, and the files such gated file
    modules include (their own allowances are test-only)."""
    regions: list[tuple[int, int]] = []
    included: set[Path] = set()
    for gate in CFG_TEST.finditer(text):
        at = gate.end()
        path: str | None = None
        # The attributes between the gate and its item.
        while True:
            rest = text[at:].lstrip()
            if not rest.startswith("#["):
                break
            start = len(text) - len(rest)
            end = attribute_end(text, start)
            path_attr = PATH_ATTR.match(text[start:end])
            if path_attr:
                path = path_attr.group(1)
            at = end
        item = MOD_ITEM.match(text[at:].lstrip())
        if item:
            if item.group(2) == "{":
                open_at = text.index("{", at)
                regions.append((gate.start(), matching_brace(text, open_at)))
            else:
                name = item.group(1)
                candidates = [directory / path] if path else [directory / f"{name}.rs", directory / name / "mod.rs"]
                included.update(candidate.resolve() for candidate in candidates)
        else:
            # Any other gated item: its body, if it has one, or the line.
            rest = text[at:]
            brace = rest.find("{")
            semicolon = rest.find(";")
            if brace != -1 and (semicolon == -1 or brace < semicolon):
                regions.append((gate.start(), matching_brace(text, at + brace)))
            else:
                regions.append((gate.start(), at + (semicolon if semicolon != -1 else 0) + 1))
    return regions, included


def violations(path: Path, text: str, gated_files: set[Path]) -> list[str]:
    if path.resolve() in gated_files:
        return []
    regions, _ = test_regions(text, path.parent)
    found = []
    for allowance in ALLOW.finditer(text):
        start = allowance.start()
        end = attribute_end(text, start)
        body = text[start:end]
        if not LINT.search(body):
            continue
        if any(begin <= start < finish for begin, finish in regions):
            continue
        line = text.count("\n", 0, start) + 1
        found.append(f"{path}:{line}: production allowance of a policy lint: {body.strip()}")
    return found


def gated_files(paths: list[Path]) -> set[Path]:
    gated: set[Path] = set()
    for path in paths:
        try:
            _, included = test_regions(path.read_text(encoding="utf-8"), path.parent)
        except (OSError, UnicodeDecodeError):
            continue
        gated.update(included)
    return gated


def production_sources(root: Path) -> list[Path]:
    sources = []
    for source_root in SOURCE_ROOTS:
        for path in sorted((root / source_root).rglob("*.rs")):
            relative = path.relative_to(root).as_posix()
            if TEST_TREE.search(relative):
                continue
            if "/target/" in relative or "/vendor/" in relative:
                continue
            sources.append(path)
    return sources


def check(paths: list[Path]) -> list[str]:
    gated = gated_files(paths)
    found: list[str] = []
    for path in paths:
        found.extend(violations(path, path.read_text(encoding="utf-8"), gated))
    return found


def self_test() -> None:
    negative = FIXTURES / "negative.rs"
    positive = FIXTURES / "positive.rs"
    if not violations(negative, negative.read_text(encoding="utf-8"), set()):
        raise SystemExit("check_production_policy: the policy let the negative fixture through")
    found = violations(positive, positive.read_text(encoding="utf-8"), set())
    if found:
        raise SystemExit("check_production_policy: the policy refused the positive fixture:\n" + "\n".join(found))


def main(argv: list[str]) -> int:
    self_test()
    if "--self-test" in argv:
        print("check_production_policy: self-test passed")
        return 0
    sources = production_sources(ROOT)
    found = check(sources)
    if found:
        print("\n".join(found), file=sys.stderr)
        print(f"check_production_policy: {len(found)} production allowance(s) of policy lints", file=sys.stderr)
        return 1
    print(f"check_production_policy: {len(sources)} production sources carry no allowance of a policy lint")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
