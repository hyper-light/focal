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
        if not (path.parent / target).exists():
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
