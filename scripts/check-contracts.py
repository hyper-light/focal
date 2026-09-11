#!/usr/bin/env python3
"""Offline checks for imported provenance and authored architecture links."""
import hashlib
import json
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs/archictecutre"
errors = []
links = 0
for manifest in sorted((ROOT / "crates").glob("*/Cargo.toml")):
    if not re.search(r"(?m)^\[lints\]\s*\nworkspace\s*=\s*true\s*$", manifest.read_text()):
        errors.append(f"workspace lint policy not inherited: {manifest.relative_to(ROOT)}")
for path in sorted(DOCS.glob("*.md")):
    for target in re.findall(r"(?<!!)\[[^\]]*\]\(([^)]+)\)", path.read_text()):
        target = target.split("#", 1)[0]
        if not target or re.match(r"[a-z]+://", target):
            continue
        target = target.strip("<>")
        resolved = (path.parent / target).resolve()
        # Cross-project provenance links (the source audit cites a sibling
        # repository) escape the repository root; they are references, not
        # internal links, so their existence is not this repo's contract.
        try:
            resolved.relative_to(ROOT)
        except ValueError:
            continue
        if not resolved.exists():
            errors.append(f"{path.relative_to(ROOT)}: missing {target}")
        links += 1

manifest_path = DOCS / "reference/hecate/manifest.json"
manifest = json.loads(manifest_path.read_text())
entries = manifest["files"]
for entry in entries:
    path = manifest_path.parent / entry["snapshot_path"]
    actual = hashlib.sha256(path.read_bytes()).hexdigest()
    if actual != entry["sha256"]:
        errors.append(f"imported source changed: {path.relative_to(ROOT)}")

# The one audited unsafe file (decision 11, doc 10). The compiler denies
# `unsafe_code` everywhere and it is allowed only in this file; this check is
# the belt-and-suspenders that no other source relaxes the lint or writes an
# `unsafe` block, so the boundary cannot drift without editing this script.
UNSAFE_FILE = ROOT / "crates/focal-platform/src/windows.rs"
UNSAFE_KEYWORD = re.compile(r"\bunsafe\s*(?:\{|fn\b|impl\b|trait\b|extern\b)")
ALLOW_UNSAFE = re.compile(r"#!?\[\s*allow\s*\(\s*unsafe_code\s*\)")
for source in sorted((ROOT / "crates").glob("*/src/**/*.rs")):
    if source == UNSAFE_FILE:
        continue
    text = source.read_text()
    if UNSAFE_KEYWORD.search(text):
        errors.append(f"unsafe keyword outside the audited FFI file: {source.relative_to(ROOT)}")
    if ALLOW_UNSAFE.search(text):
        errors.append(f"allow(unsafe_code) outside the audited FFI file: {source.relative_to(ROOT)}")
if not UNSAFE_FILE.exists():
    errors.append("the audited FFI file crates/focal-platform/src/windows.rs is missing")

registry = json.loads((ROOT / "config/schema/domain-registry-v1.json").read_text())
source = (ROOT / "crates/focal-model/src/vocabulary.rs").read_text()
for name, fields in registry["vocabularies"].items():
    match = re.search(r"vocabulary!\(" + name + r"\s*\{([^}]*)\}", source, re.S)
    actual = dict((name, int(code)) for name, code in re.findall(r"(\w+)\s*=\s*(\d+)", match[1])) if match else {}
    for field, code in fields.items():
        if actual.get(field) != code:
            errors.append(f"reserved vocabulary changed: {name}.{field}={code}")
    if len(set(actual.values())) != len(actual):
        errors.append(f"duplicate numeric vocabulary code: {name}")
if errors:
    print("\n".join(errors), file=sys.stderr)
    sys.exit(1)
print(f"Verified {links} architecture links, {len(entries)} imported source hashes, and {len(registry['vocabularies'])} frozen domain vocabularies.")
