#!/usr/bin/env python3
"""The adversarial battery (terminating condition 6): every case the threat model's coverage table
names (docs/qualification/adversarial-battery.md), run together.

    python3 scripts/adversarial_battery.py check   # the table against the source: each case a test
    python3 scripts/adversarial_battery.py run     # every case run, the table printed with results
    python3 scripts/adversarial_battery.py run P   # the cases of package P alone, built alone
    python3 scripts/adversarial_battery.py list    # the cases, one a line

A case is `package:target:filter`, written in backticks in the table's Cases column: a package of
the workspace, one of its test targets (`lib`, `bin=NAME` or `test=NAME`) and a libtest filter
naming one test of that target (its function name, or a path suffix such as `tests::name` where
the name alone is not unique). `check` reads the source only: every case must be a test function
of its target, every threat must name a case, and every class of the condition must have a threat
(scripts/check-contracts.py runs it). `run` takes the test executables the workspace build made
(`cargo test --workspace --locked --no-run --message-format=json`: the build CI's Tests step makes,
so nothing is built again) and runs each once with its cases as filters, in its package's
directory as `cargo test` does. Every case must match exactly one test, and every one must pass.
"""
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TABLE = ROOT / "docs/qualification/adversarial-battery.md"
HEADER = re.compile(r"^\|\s*Id\s*\|\s*Class\s*\|\s*Surface\s*\|\s*Threat\s*\|\s*Defense\s*\|\s*Cases\s*\|\s*$")
CASE = re.compile(r"`([a-z0-9][a-z0-9-]*):(lib|bin=[a-z0-9_-]+|test=[a-z0-9_]+):([A-Za-z0-9_:]+)`")
# The classes terminating condition 6 names; every one has a threat.
CLASSES = ("decompression bomb", "size bomb", "tampering", "misuse", "protocol abuse")
RESULT = re.compile(r"^test (\S+) \.\.\. (ok|FAILED|ignored)\b")


class Case:
    def __init__(self, package, target, filter_):
        self.package, self.target, self.filter = package, target, filter_

    @property
    def key(self):
        return (self.package, self.target)

    def __str__(self):
        return f"{self.package}:{self.target}:{self.filter}"


def parse(path=TABLE):
    """The threats of the coverage table, in order: (id, class, surface, threat, defense, cases)."""
    lines = path.read_text().splitlines()
    start = next((i for i, line in enumerate(lines) if HEADER.match(line)), None)
    if start is None:
        raise SystemExit(f"{path.relative_to(ROOT)}: no coverage table (a header `| Id | Class | Surface | Threat | Defense | Cases |`)")
    threats = []
    for line in lines[start + 2:]:
        if not line.startswith("|"):
            break
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if len(cells) != 6:
            raise SystemExit(f"{path.relative_to(ROOT)}: a row of {len(cells)} cells, not 6: {line[:80]}")
        id_, class_, surface, threat, defense, cases = cells
        threats.append((id_, class_, surface, threat, defense, [Case(*m) for m in CASE.findall(cases)]))
    return threats


def packages(root=ROOT):
    """Package name -> (directory, {bin name: root file})."""
    found = {}
    for manifest in sorted(list(root.glob("crates/*/Cargo.toml")) + list(root.glob("tools/*/Cargo.toml"))):
        text = manifest.read_text()
        package = re.search(r'^\[package\][^\[]*?^name\s*=\s*"([^"]+)"', text, re.M | re.S)
        if not package:
            continue
        directory = manifest.parent
        bins = {}
        for block in re.findall(r"^\[\[bin\]\](.*?)(?=^\[|\Z)", text, re.M | re.S):
            name = re.search(r'^name\s*=\s*"([^"]+)"', block, re.M)
            file = re.search(r'^path\s*=\s*"([^"]+)"', block, re.M)
            if name:
                bins[name[1]] = directory / (file[1] if file else f"src/bin/{name[1]}.rs")
        if not bins and (directory / "src/main.rs").exists():
            bins[package[1]] = directory / "src/main.rs"
        found[package[1]] = (directory, bins)
    return found


MOD = re.compile(r"((?:#\[[^\]]*\]\s*)*)(?:pub(?:\([^)]*\))?\s+)?mod\s+([a-z_][a-z0-9_]*)\s*;")
PATH = re.compile(r'#\[path\s*=\s*"([^"]+)"\]')


def module_files(root_file):
    """The files of the crate whose root is `root_file`, following `mod name;` and `#[path]`."""
    seen, stack = [], [root_file]
    while stack:
        file = stack.pop()
        if file in seen or not file.exists():
            continue
        seen.append(file)
        text = file.read_text()
        owner = file.name in ("lib.rs", "main.rs", "mod.rs") or file.parent.name in ("tests", "bin")
        base = file.parent if owner else file.parent / file.stem
        for attributes, name in MOD.findall(text):
            path = PATH.search(attributes)
            if path:
                stack.append((file.parent / path[1]).resolve())
            else:
                stack.extend([base / f"{name}.rs", base / name / "mod.rs"])
    return seen


def target_root(found, case):
    directory, bins = found[case.package]
    if case.target == "lib":
        return directory / "src/lib.rs"
    kind, name = case.target.split("=", 1)
    if kind == "bin":
        return bins.get(name)
    for candidate in (directory / "tests" / f"{name}.rs", directory / "tests" / name / "main.rs"):
        if candidate.exists():
            return candidate
    return None


def definitions(files, name):
    test = re.compile(
        r"#\[(?:[a-z_]+::)?test\b[^\]]*\]\s*(?:#\[[^\]]*\]\s*|///[^\n]*\n\s*)*(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?fn\s+"
        + re.escape(name)
        + r"\s*\("
    )
    return [file for file in files for _ in test.finditer(file.read_text())]


def check(root=ROOT):
    """Every problem of the coverage table against the source; none when it holds."""
    errors = []
    threats = parse(root / "docs/qualification/adversarial-battery.md")
    found = packages(root)
    ids = [threat[0] for threat in threats]
    for id_ in sorted({id_ for id_ in ids if ids.count(id_) > 1}):
        errors.append(f"adversarial battery: threat {id_} is listed twice")
    for class_ in CLASSES:
        if not any(threat[1] == class_ for threat in threats):
            errors.append(f"adversarial battery: no threat of the class `{class_}`")
    files = {}
    for id_, class_, _, _, _, cases in threats:
        if class_ not in CLASSES:
            errors.append(f"adversarial battery: {id_} has the class `{class_}`, not one of {', '.join(CLASSES)}")
        if not cases:
            errors.append(f"adversarial battery: {id_} names no case")
        for case in cases:
            if case.package not in found:
                errors.append(f"adversarial battery: {id_}: {case}: no package {case.package}")
                continue
            root_file = target_root(found, case)
            if root_file is None or not root_file.exists():
                errors.append(f"adversarial battery: {id_}: {case}: no target {case.target}")
                continue
            if root_file not in files:
                files[root_file] = module_files(root_file)
            name = case.filter.rsplit("::", 1)[-1]
            defined = definitions(files[root_file], name)
            if not defined:
                errors.append(f"adversarial battery: {id_}: {case}: no test `{name}` in that target")
            elif len(defined) > 1 and "::" not in case.filter:
                errors.append(f"adversarial battery: {id_}: {case}: `{name}` names {len(defined)} tests; qualify it with its module")
    return errors


def executables(only=()):
    """(package, target) -> (executable, package directory), from the workspace's test build (or,
    for a check of some packages alone, theirs)."""
    found = packages()
    by_dir = {directory.resolve(): name for name, (directory, _) in found.items()}
    selection = [arg for package in only for arg in ("-p", package)] or ["--workspace"]
    built = subprocess.run(
        ["cargo", "test", *selection, "--locked", "--no-run", "--message-format=json"],
        cwd=ROOT, capture_output=True, text=True,
    )
    if built.returncode != 0:
        sys.stderr.write(built.stderr[-4000:])
        raise SystemExit("adversarial battery: the workspace's tests did not build")
    found_exes = {}
    for line in built.stdout.splitlines():
        if not line.startswith("{"):
            continue
        message = json.loads(line)
        if message.get("reason") != "compiler-artifact" or not message.get("executable"):
            continue
        if not message.get("profile", {}).get("test"):
            continue
        directory = Path(message["manifest_path"]).resolve().parent
        package = by_dir.get(directory)
        if package is None:
            continue
        kind, name = message["target"]["kind"], message["target"]["name"]
        if "test" in kind:
            target = f"test={name}"
        elif "bin" in kind:
            target = f"bin={name}"
        else:
            target = "lib"
        found_exes[(package, target)] = (message["executable"], directory)
    return found_exes


def run(only=()):
    threats = parse()
    problems = check()
    if problems:
        print("\n".join(problems), file=sys.stderr)
        raise SystemExit(1)
    if only:
        threats = [(*threat[:5], [case for case in threat[5] if case.package in only]) for threat in threats]
        threats = [threat for threat in threats if threat[5]]
    exes = executables(only)
    groups = {}
    for _, _, _, _, _, cases in threats:
        for case in cases:
            groups.setdefault(case.key, [])
            if case.filter not in groups[case.key]:
                groups[case.key].append(case.filter)
    outcome = {}
    failed_output = []
    began = time.monotonic()
    for key in sorted(groups):
        filters = groups[key]
        if key not in exes:
            for filter_ in filters:
                outcome[(key, filter_)] = "no test executable"
            continue
        executable, directory = exes[key]
        environment = dict(os.environ, CARGO_MANIFEST_DIR=str(directory))
        started = time.monotonic()
        ran = subprocess.run(
            [executable, *filters, "--test-threads=4"],
            cwd=directory, env=environment, capture_output=True, text=True,
        )
        results = {}
        for line in ran.stdout.splitlines():
            match = RESULT.match(line)
            if match:
                results[match[1]] = match[2]
        for filter_ in filters:
            matched = [name for name in results if filter_ in name]
            if len(matched) != 1:
                outcome[(key, filter_)] = f"matched {len(matched)} tests"
            else:
                outcome[(key, filter_)] = "passed" if results[matched[0]] == "ok" else results[matched[0]].lower()
        if ran.returncode != 0 or any(outcome[(key, f)] != "passed" for f in filters):
            failed_output.append((key, ran.stdout[-6000:] + ran.stderr[-4000:]))
        print(f"{key[0]}:{key[1]}: {len(filters)} cases, {sum(outcome[(key, f)] == 'passed' for f in filters)} passed, {time.monotonic() - started:.1f} s", flush=True)
    print()
    print("| Id | Class | Threat | Cases passed |")
    print("|---|---|---|---|")
    failures = []
    for id_, class_, _, threat, _, cases in threats:
        passed = sum(outcome.get((case.key, case.filter)) == "passed" for case in cases)
        print(f"| {id_} | {class_} | {threat} | {passed} of {len(cases)} |")
        for case in cases:
            state = outcome.get((case.key, case.filter), "not run")
            if state != "passed":
                failures.append(f"{id_}: {case}: {state}")
    total = sum(len(group) for group in groups.values())
    passed = sum(state == "passed" for state in outcome.values())
    print(f"\n{len(threats)} threats, {total} cases, {passed} passed, in {time.monotonic() - began:.1f} s")
    if failures:
        for key, output in failed_output:
            print(f"\n---- {key[0]}:{key[1]} ----\n{output}", file=sys.stderr)
        print("\n".join(failures), file=sys.stderr)
        raise SystemExit(1)


def main():
    command = sys.argv[1] if len(sys.argv) > 1 else "check"
    if command == "check":
        problems = check()
        if problems:
            print("\n".join(problems), file=sys.stderr)
            raise SystemExit(1)
        threats = parse()
        print(f"adversarial battery: {len(threats)} threats, {sum(len(t[5]) for t in threats)} cases, each a test of its target")
    elif command == "run":
        run(tuple(sys.argv[2:]))
    elif command == "list":
        for threat in parse():
            for case in threat[5]:
                print(f"{threat[0]}\t{case}")
    else:
        raise SystemExit(__doc__)


if __name__ == "__main__":
    main()
