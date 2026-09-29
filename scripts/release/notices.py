#!/usr/bin/env python3
"""Render the third-party license notices and the SPDX SBOM for a release.

Both are derived from `Cargo.lock` (the exact locked graph) with no network or
`cargo` invocation, so they are deterministic and reproducible offline. License
strings for third-party crates come from `docs/dependencies/inventory.tsv` (the
reviewed roster); workspace crates carry the project's own license. The roster
is cross-checked against the locked external graph, so a dependency added or
removed without updating the roster fails the build rather than shipping stale
notices.
"""

import json
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
INVENTORY = ROOT / "docs" / "dependencies" / "inventory.tsv"
LOCKFILE = ROOT / "Cargo.lock"
WORKSPACE = ROOT / "Cargo.toml"
WORKSPACE_LICENSE = "MIT"
NOTICES = "THIRD-PARTY-NOTICES.txt"
SBOM = "sbom.spdx.json"


def die(message):
    raise SystemExit(f"notices: {message}")


def workspace_members():
    """The names of the workspace's own crates: every package under a
    `[workspace] members` glob. A package in `Cargo.lock` without a `source`
    that is not one of these is a third-party crate built from `vendor/`
    (`[patch.crates-io]`), which the notices and the SBOM must list."""
    workspace = tomllib.loads(WORKSPACE.read_text())
    names = set()
    for pattern in workspace.get("workspace", {}).get("members", []):
        for manifest in sorted(ROOT.glob(f"{pattern}/Cargo.toml")):
            package = tomllib.loads(manifest.read_text()).get("package")
            if package and "name" in package:
                names.add(package["name"])
    if not names:
        die("no workspace members found under the [workspace] members globs")
    return names


def read_inventory():
    lines = INVENTORY.read_text().splitlines()
    if not lines or lines[0].split("\t") != ["name", "version", "license", "source"]:
        die("inventory.tsv header is missing or unexpected")
    rows = {}
    for line in lines[1:]:
        if not line.strip():
            continue
        fields = line.split("\t")
        if len(fields) != 4:
            die(f"malformed inventory row: {line!r}")
        name, version, license_, source = fields
        key = (name, version)
        if key in rows:
            die(f"duplicate inventory entry: {name} {version}")
        rows[key] = {"name": name, "version": version, "license": license_, "source": source}
    return rows


def lock_packages():
    document = tomllib.loads(LOCKFILE.read_text())
    members = workspace_members()
    packages = []
    for package in document.get("package", []):
        source = package.get("source")
        # Workspace members and vendored path crates have no `source` in the
        # lockfile; registry and git crates do. A vendored crate is third
        # party: its locator comes from the roster (the crates.io archive it
        # was unpacked from and that archive's checksum, `vendor/README.md`).
        vendored = source is None and package["name"] not in members
        packages.append(
            {
                "name": package["name"],
                "version": package["version"],
                "source": source,
                "third_party": source is not None or vendored,
                "vendored": vendored,
            }
        )
    return sorted(packages, key=lambda package: (package["name"], package["version"]))


def check_drift(inventory, packages):
    external = {(p["name"], p["version"]) for p in packages if p["third_party"]}
    listed = set(inventory)
    missing = external - listed
    extra = listed - external
    if missing or extra:
        report = []
        if missing:
            report.append("missing from inventory.tsv: " + ", ".join(f"{n} {v}" for n, v in sorted(missing)))
        if extra:
            report.append("no longer in the locked graph: " + ", ".join(f"{n} {v}" for n, v in sorted(extra)))
        die("dependency roster is stale — " + "; ".join(report))


def resolve_licenses(inventory, packages):
    resolved = []
    for package in packages:
        key = (package["name"], package["version"])
        if package["third_party"]:
            license_ = inventory[key]["license"]
            # A vendored crate's locator is the roster's: the registry archive
            # it is a verbatim copy of, with that archive's checksum.
            source = package["source"] or inventory[key]["source"]
        else:
            license_ = WORKSPACE_LICENSE
            source = None
        resolved.append({**package, "license": license_, "source": source})
    return resolved


def render_notices(packages):
    header = (
        "Focal bundles the following third-party Rust crates in its single\n"
        "executable. Each is distributed under the license shown. Full license\n"
        "texts are available from each crate's source repository and from\n"
        "https://crates.io. This file is generated from the reviewed dependency\n"
        "roster and the locked dependency graph.\n\n"
    )
    blocks = []
    for package in packages:
        if not package["third_party"]:
            continue  # the workspace's own crates are not third-party notices
        block = (
            f"{package['name']} {package['version']}\n"
            f"  License: {package['license']}\n"
            f"  Source:  {package['source']}\n"
        )
        if package["vendored"]:
            block += f"  Built from: vendor/{package['name']}, a verbatim copy of that archive\n"
        blocks.append(block)
    return header + "\n".join(blocks)


def render_sbom(packages, version, commit):
    namespace = f"https://github.com/hyper-light/focal/spdxdocs/focal-{version}"
    if commit:
        namespace += f"-{commit}"
    spdx_packages = []
    relationships = []
    for index, package in enumerate(packages):
        spdx_id = f"SPDXRef-Package-{index}"
        spdx_packages.append(
            {
                "SPDXID": spdx_id,
                "name": package["name"],
                "versionInfo": package["version"],
                "downloadLocation": package["source"] or "NOASSERTION",
                "filesAnalyzed": False,
                "licenseConcluded": "NOASSERTION",
                "licenseDeclared": package["license"],
                "copyrightText": "NOASSERTION",
            }
        )
        relationships.append(
            {
                "spdxElementId": "SPDXRef-DOCUMENT",
                "relatedSpdxElement": spdx_id,
                "relationshipType": "DESCRIBES",
            }
        )
    document = {
        "spdxVersion": "SPDX-2.3",
        "dataLicense": "CC0-1.0",
        "SPDXID": "SPDXRef-DOCUMENT",
        "name": f"focal-{version}",
        "documentNamespace": namespace,
        "creationInfo": {
            "created": "1970-01-01T00:00:00Z",
            "creators": ["Tool: focal-release-notices"],
        },
        "packages": spdx_packages,
        "relationships": relationships,
    }
    return json.dumps(document, indent=2, sort_keys=True) + "\n"


def generate(destination, version, commit):
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=True)
    inventory = read_inventory()
    packages = lock_packages()
    check_drift(inventory, packages)
    packages = resolve_licenses(inventory, packages)
    (destination / NOTICES).write_text(render_notices(packages))
    (destination / SBOM).write_text(render_sbom(packages, version, commit))
    return sorted([NOTICES, SBOM])


def main():
    if len(sys.argv) < 2:
        die("usage: notices.py <destination-dir> [version] [commit]")
    destination = sys.argv[1]
    version = sys.argv[2] if len(sys.argv) > 2 else "0.0.0"
    commit = sys.argv[3] if len(sys.argv) > 3 else ""
    written = generate(destination, version, commit)
    print("\n".join(written))


if __name__ == "__main__":
    main()
