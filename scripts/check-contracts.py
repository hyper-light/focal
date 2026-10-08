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

# One cryptographic provider (decision F57, doc 07): aws-lc-rs, built from the
# sources in vendor/. No workspace manifest may depend on `ring` or turn on a
# crate's ring-backed feature; every TLS, QUIC and certificate dependency names
# the aws-lc-rs feature instead, so the choice cannot drift crate by crate.
RING_FEATURES = {
    "rustls": ("ring", "aws_lc_rs"),
    "quinn": ("rustls-ring", "rustls-aws-lc-rs"),
    "quinn-proto": ("rustls-ring", "rustls-aws-lc-rs"),
    "rcgen": ("ring", "aws_lc_rs"),
    "x509-parser": ("verify", "verify-aws"),
}
for manifest in sorted(list((ROOT / "crates").glob("*/Cargo.toml")) + list((ROOT / "tools").glob("*/Cargo.toml"))):
    for line in manifest.read_text().splitlines():
        line = line.split("#", 1)[0].strip()
        if re.match(r"^ring\s*=", line):
            errors.append(f"direct dependency on ring: {manifest.relative_to(ROOT)}")
        for crate, (ring_feature, aws_feature) in RING_FEATURES.items():
            if not re.match(rf"^{re.escape(crate)}\s*=", line):
                continue
            features = re.search(r"features\s*=\s*\[([^\]]*)\]", line)
            names = set(re.findall(r'"([^"]+)"', features[1])) if features else set()
            if ring_feature in names or aws_feature not in names:
                errors.append(
                    f"{manifest.relative_to(ROOT)}: {crate} must use the {aws_feature} feature, not {ring_feature}"
                )
# Post-quantum key exchange only (decision F58, doc 07): every TLS
# configuration in production code is built from `focal_wire::crypto_provider`
# and every QUIC one from `focal_wire::quic_client` / `quic_server`, so no
# site can rebuild aws-lc-rs's default provider, whose classical groups a
# peer could pick. Test sources build classical peers on purpose and are
# exempt.
PROVIDER_HOME = ROOT / "crates/focal-wire/src/crypto.rs"
PROVIDER_PATTERNS = (
    r"\bdefault_provider\s*\(",
    r"\bQuic(?:Client|Server)Config::try_from\b",
    r"\bQuic(?:Client|Server)Config::with_initial\b",
)
for source in sorted(list((ROOT / "crates").glob("*/src/**/*.rs")) + list((ROOT / "tools").glob("*/src/**/*.rs"))):
    if source == PROVIDER_HOME or source.name == "tests.rs" or source.stem.endswith("_tests") or "tests" in source.relative_to(ROOT).parts[3:-1]:
        continue
    text = source.read_text()
    for pattern in PROVIDER_PATTERNS:
        if re.search(pattern, text):
            errors.append(
                f"{source.relative_to(ROOT)}: builds a TLS provider or QUIC configuration outside focal_wire::crypto"
            )
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

# Every YAML document focal reads is parsed under a stated budget (no
# aliases or anchors, bounded depth, events, nodes and scalar bytes), so no
# document expands past the bytes it holds. serde-saphyr's unbudgeted entry
# points take its liberal default budget (50,000 aliases, 250,000 nodes) and
# are refused in production; an inline test module at the end of a file may
# use them to compare against the default.
YAML_UNBUDGETED = re.compile(r"\bserde_saphyr::from_(?:str|slice|reader)(?:::<[^>]*>)?\s*\(")
INLINE_TESTS = re.compile(r"^#\[cfg\(test\)\]\s*\n\s*mod\s+\w+\s*\{", re.MULTILINE)
for source in sorted(list((ROOT / "crates").glob("*/src/**/*.rs")) + list((ROOT / "tools").glob("*/src/**/*.rs"))):
    if source.name == "tests.rs" or source.stem.endswith("_tests") or "tests" in source.relative_to(ROOT).parts[3:-1]:
        continue
    text = source.read_text()
    inline = INLINE_TESTS.search(text)
    production = text[: inline.start()] if inline else text
    if YAML_UNBUDGETED.search(production):
        errors.append(f"{source.relative_to(ROOT)}: parses YAML without a stated budget (serde_saphyr::from_str_with_options)")

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
