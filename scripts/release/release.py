#!/usr/bin/env python3
"""Guard, collect, verify and publish the complete native binary release."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import struct
import subprocess
import tempfile
import tomllib

import notices


ROOT = Path(__file__).resolve().parents[2]
CATALOG = Path(__file__).with_name("platforms.json")
MANIFEST = "release-manifest.json"
SUMS = "SHA256SUMS"
SEMVER = re.compile(r"(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?\Z")
REQUIRED_TARGETS = {
    "aarch64-apple-darwin", "x86_64-apple-darwin",
    "aarch64-unknown-linux-gnu", "x86_64-unknown-linux-gnu",
    "aarch64-unknown-linux-musl", "x86_64-unknown-linux-musl",
    "x86_64-pc-windows-msvc", "aarch64-pc-windows-msvc",
}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def run(*arguments, **kwargs):
    return subprocess.run(arguments, check=True, **kwargs)


def output(*arguments):
    return run(*arguments, stdout=subprocess.PIPE, text=True).stdout.strip()


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def configuration(root=ROOT, catalog=CATALOG):
    workspace = tomllib.loads((root / "Cargo.toml").read_text())
    version = workspace["workspace"]["package"]["version"]
    toolchain = tomllib.loads((root / "rust-toolchain.toml").read_text())["toolchain"]["channel"]
    require(SEMVER.fullmatch(version), "workspace version must be a release version")
    require(re.fullmatch(r"\d+\.\d+\.\d+", toolchain), "release Rust must be an exact version")
    for manifest in sorted((root / "crates").glob("*/Cargo.toml")):
        package = tomllib.loads(manifest.read_text())["package"]
        require(package.get("version") in (version, {"workspace": True}), f"version mismatch: {manifest}")
    lock = tomllib.loads((root / "Cargo.lock").read_text())
    node = [package for package in lock["package"] if package["name"] == "focal-node"]
    require(len(node) == 1 and node[0]["version"] == version, "Cargo.lock focal-node version mismatch")
    platforms = json.loads(catalog.read_text())
    rows = platforms["include"]
    require({row["target"] for row in rows} == REQUIRED_TARGETS, "release matrix must contain all eight required targets")
    require(len({row["target"] for row in rows}) == len(rows), "duplicate target")
    require(len({row["asset"] for row in rows}) == len(rows), "duplicate release asset")
    for row in rows:
        require(re.fullmatch(r"focal-[a-z0-9-]+(?:\.exe)?", row["asset"]), "unsafe asset name")
        require(re.fullmatch(r"[a-z0-9_-]+", row["target"]), "unsafe target")
        require(isinstance(row["musl"], bool), "invalid musl flag")
    image = platforms["musl_image"]
    require(re.fullmatch(rf"rust:{re.escape(toolchain)}-alpine@sha256:[0-9a-f]{{64}}", image),
            "update the pinned official musl image when Rust changes")
    return version, toolchain, platforms


def release_tag(version, event, ref):
    if event == "workflow_dispatch":
        return None
    require(event == "push" and ref.startswith("refs/tags/"), "only an intentional tag push may publish")
    tag = ref.removeprefix("refs/tags/")
    require(tag == f"v{version}", f"tag {tag!r} does not match workspace v{version}")
    return tag


def source_commit():
    commit = output("git", "-C", str(ROOT), "rev-parse", "HEAD^{commit}")
    expected = os.environ.get("GITHUB_SHA")
    if expected:
        require(re.fullmatch(r"[0-9a-f]{40}", expected), "invalid Actions source SHA")
        resolved = output("git", "-C", str(ROOT), "rev-parse", f"{expected}^{{commit}}")
        require(commit == resolved, "checked-out commit differs from Actions source")
    return commit


def guard():
    version, toolchain, platforms = configuration()
    release_tag(version, os.environ.get("GITHUB_EVENT_NAME", ""), os.environ.get("GITHUB_REF", ""))
    source_commit()
    values = {
        "matrix": json.dumps({"include": platforms["include"]}, separators=(",", ":")),
        "toolchain": toolchain,
        "musl_image": platforms["musl_image"],
    }
    with Path(os.environ["GITHUB_OUTPUT"]).open("a") as destination:
        for name, value in values.items():
            destination.write(f"{name}={value}\n")
    print(f"Guard passed for workspace {version}, Rust {toolchain}, {len(platforms['include'])} targets")


def pe_imported_dlls(data):
    """The set of DLL names a PE32+ image imports, parsed without any library."""
    require(len(data) >= 0x40 and data[:2] == b"MZ", "not a PE image")
    lfanew = struct.unpack_from("<I", data, 0x3C)[0]
    require(data[lfanew : lfanew + 4] == b"PE\0\0", "missing PE signature")
    coff = lfanew + 4
    number_of_sections = struct.unpack_from("<H", data, coff + 2)[0]
    size_of_optional = struct.unpack_from("<H", data, coff + 16)[0]
    optional = coff + 20
    require(struct.unpack_from("<H", data, optional)[0] == 0x20B, "expected a PE32+ (64-bit) image")
    # Data directory 1 is the import table (RVA, size); PE32+ directories begin
    # at optional-header offset 112, each eight bytes.
    import_rva = struct.unpack_from("<I", data, optional + 112 + 8)[0]
    sections = []
    for index in range(number_of_sections):
        base = optional + size_of_optional + 40 * index
        virtual_size, virtual_address, raw_size, raw_pointer = struct.unpack_from("<IIII", data, base + 8)
        sections.append((virtual_address, max(virtual_size, raw_size), raw_pointer))

    def to_offset(rva):
        for virtual_address, span, raw_pointer in sections:
            if virtual_address <= rva < virtual_address + span:
                return raw_pointer + (rva - virtual_address)
        return None

    dlls = set()
    if import_rva == 0:
        return dlls
    table = to_offset(import_rva)
    require(table is not None, "import table RVA is outside every section")
    for index in range(4096):
        descriptor = table + 20 * index
        fields = struct.unpack_from("<IIIII", data, descriptor)
        if fields == (0, 0, 0, 0, 0):
            break
        name = to_offset(fields[3])
        require(name is not None, "import name RVA is outside every section")
        dlls.add(data[name : data.index(b"\0", name)].decode("ascii", "replace").lower())
    return dlls


def verify_native(binary, row, version):
    architecture = row["target"].split("-")[0]
    machine = {"arm64": "aarch64", "AMD64": "x86_64", "ARM64": "aarch64"}.get(
        platform.machine(), platform.machine()
    )
    require(machine == architecture, "cross-compiled output must be smoked on its native architecture")
    with binary.open("rb") as source:
        header = source.read(64)
    require(len(header) == 64, "truncated binary")
    if "linux" in row["target"]:
        require(platform.system() == "Linux", "Linux binary needs a Linux smoke host")
        require(header[:6] == b"\x7fELF\x02\x01", "expected little-endian ELF64")
        require(struct.unpack_from("<H", header, 18)[0] == {"x86_64": 62, "aarch64": 183}[architecture],
                "ELF architecture differs from asset name")
        dynamic = output("readelf", "--dynamic", str(binary))
        if row["musl"]:
            require("INTERP" not in output("readelf", "--program-headers", str(binary)), "musl executable has an interpreter")
            require("(NEEDED)" not in dynamic, "musl executable has dynamic dependencies")
        else:
            needed = re.findall(r"Shared library: \[([^]]+)\]", dynamic)
            allowed = {"libc.so.6", "libm.so.6", "libpthread.so.0", "libdl.so.2", "librt.so.1", "libgcc_s.so.1",
                       "ld-linux-x86-64.so.2", "ld-linux-aarch64.so.1"}
            require(set(needed) <= allowed, f"unpackaged shared dependencies: {set(needed) - allowed}")
            versions = re.findall(r"GLIBC_(\d+)\.(\d+)(?:\.(\d+))?", output("readelf", "--version-info", str(binary)))
            require(all(tuple(int(part or 0) for part in value) <= (2, 39, 0) for value in versions),
                    "GNU executable exceeds the documented glibc 2.39 baseline")
    elif "windows" in row["target"]:
        require(platform.system() == "Windows", "Windows binary needs a Windows smoke host")
        require(header[:2] == b"MZ", "expected a PE image")
        data = binary.read_bytes()
        lfanew = struct.unpack_from("<I", data, 0x3C)[0]
        require(struct.unpack_from("<H", data, lfanew + 4)[0] == {"x86_64": 0x8664, "aarch64": 0xAA64}[architecture],
                "PE machine differs from asset name")
        # Only the OS libraries Focal links: kernel, security (SID/DACL), the
        # RNG, sockets and the API-set stubs. No bundled runtime, no OpenSSL.
        allowed = {"kernel32.dll", "advapi32.dll", "bcrypt.dll", "ntdll.dll", "ws2_32.dll",
                   "userenv.dll", "secur32.dll", "crypt32.dll", "rpcrt4.dll", "kernelbase.dll"}
        unexpected = {name for name in pe_imported_dlls(data)
                      if name not in allowed and not name.startswith("api-ms-win-")}
        require(not unexpected, f"unpackaged Windows imports: {sorted(unexpected)}")
    else:
        require(platform.system() == "Darwin", "macOS binary needs a macOS smoke host")
        require(header[:4] == b"\xcf\xfa\xed\xfe", "expected little-endian Mach-O64")
        require(struct.unpack_from("<I", header, 4)[0] == {"x86_64": 0x01000007, "aarch64": 0x0100000C}[architecture],
                "Mach-O architecture differs from asset name")
        libraries = output("otool", "-L", str(binary)).splitlines()[1:]
        require(all(line.strip().startswith(("/usr/lib/", "/System/Library/")) for line in libraries),
                "macOS binary depends on unpackaged libraries")
    require(output(str(binary), "--version") == f"focal {version}", "binary version differs from tag")


def stage(target, destination):
    version, toolchain, platforms = configuration()
    matches = [row for row in platforms["include"] if row["target"] == target]
    require(len(matches) == 1, "target is absent from the release matrix")
    row = matches[0]
    name = "focal.exe" if "windows" in target else "focal"
    binary = ROOT / "target" / target / "release" / name
    require(binary.is_file() and not binary.is_symlink(), "release output is not a regular executable")
    verify_native(binary, row, version)
    destination.mkdir(parents=True, exist_ok=False)
    asset = destination / row["asset"]
    shutil.copyfile(binary, asset)
    asset.chmod(0o755)
    metadata = {**row, "version": version, "toolchain": toolchain, "commit": source_commit(),
                "sha256": digest(asset), "bytes": asset.stat().st_size,
                "roles": ["server", "cli", "mcp"], "native_smoke": "server-cli-crash-recovery-mcp"}
    (destination / f"{row['asset']}.json").write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n")


def regular_files(directory):
    require(directory.is_dir() and not directory.is_symlink(), f"not a regular directory: {directory}")
    files = list(directory.iterdir())
    require(all(path.is_file() and not path.is_symlink() for path in files), f"unexpected file type in {directory}")
    return {path.name for path in files}


def collect(source, destination, version, toolchain, platforms, commit):
    require(not destination.exists(), "refusing to replace an existing collected release")
    expected = {f"binary-{row['asset']}" for row in platforms["include"]}
    require(source.is_dir() and not source.is_symlink(), "artifact root missing")
    require({path.name for path in source.iterdir()} == expected, "missing or unexpected platform artifact")
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=".focal-release-", dir=destination.parent))
    try:
        records = []
        for row in sorted(platforms["include"], key=lambda item: item["asset"]):
            folder = source / f"binary-{row['asset']}"
            require(regular_files(folder) == {row["asset"], f"{row['asset']}.json"}, "incomplete or unexpected platform files")
            metadata = json.loads((folder / f"{row['asset']}.json").read_text())
            for name, value in {**row, "version": version, "toolchain": toolchain, "commit": commit,
                                "roles": ["server", "cli", "mcp"],
                                "native_smoke": "server-cli-crash-recovery-mcp"}.items():
                require(metadata.get(name) == value, f"artifact provenance mismatch: {row['asset']} {name}")
            binary = folder / row["asset"]
            require(metadata.get("bytes") == binary.stat().st_size > 0, "binary size mismatch")
            require(metadata.get("sha256") == digest(binary), "binary digest mismatch")
            shutil.copyfile(binary, temporary / row["asset"])
            (temporary / row["asset"]).chmod(0o755)
            records.append(metadata)
        manifest = {"schema": 1, "version": version, "commit": commit, "toolchain": toolchain, "assets": records}
        (temporary / MANIFEST).write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
        # The third-party notices and the SPDX SBOM ship beside the binaries; the
        # generator cross-checks the reviewed dependency roster against the locked
        # graph, so drift fails the release rather than shipping stale notices.
        extra = notices.generate(temporary, version, commit)
        names = sorted([row["asset"] for row in records] + [MANIFEST] + extra)
        (temporary / SUMS).write_text("".join(f"{digest(temporary / name)}  {name}\n" for name in names))
        verify_collection(temporary, version, toolchain, platforms, commit)
        temporary.rename(destination)
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)


def verify_collection(directory, version, toolchain, platforms, commit):
    expected = {row["asset"] for row in platforms["include"]} | {MANIFEST, SUMS, notices.NOTICES, notices.SBOM}
    require(regular_files(directory) == expected, "release asset set is incomplete or unexpected")
    manifest = json.loads((directory / MANIFEST).read_text())
    require((manifest.get("schema"), manifest.get("version"), manifest.get("toolchain"), manifest.get("commit"))
            == (1, version, toolchain, commit), "release manifest source mismatch")
    records = manifest["assets"]
    require(len(records) == len(platforms["include"]), "duplicate or missing manifest asset")
    require({row["asset"] for row in records} == expected - {MANIFEST, SUMS, notices.NOTICES, notices.SBOM},
            "manifest asset names mismatch")
    for row in records:
        platform_row = next(item for item in platforms["include"] if item["asset"] == row["asset"])
        require(all(row.get(name) == value for name, value in platform_row.items()), "manifest target mismatch")
        require(row.get("version") == version and row.get("toolchain") == toolchain and row.get("commit") == commit,
                "manifest artifact provenance mismatch")
        require(row.get("roles") == ["server", "cli", "mcp"]
                and row.get("native_smoke") == "server-cli-crash-recovery-mcp", "manifest smoke gate missing")
        binary = directory / row["asset"]
        require(row.get("bytes") == binary.stat().st_size > 0 and row.get("sha256") == digest(binary), "manifest binary mismatch")
    lines = (directory / SUMS).read_text().splitlines()
    require(len(lines) == len(expected) - 1, "checksum manifest has missing or duplicate entries")
    seen = set()
    for line in lines:
        match = re.fullmatch(r"([0-9a-f]{64})  ([a-zA-Z0-9_.-]+)", line)
        require(match is not None, "malformed checksum entry")
        value, name = match.groups()
        require(name in expected - {SUMS} and name not in seen, "unexpected or duplicate checksum file")
        require(digest(directory / name) == value, f"checksum mismatch: {name}")
        seen.add(name)
    require(seen == expected - {SUMS}, "checksum manifest omitted an asset")
    return sorted(expected)


def api(path, method="GET", body=None):
    arguments = ["gh", "api", "--method", method, path]
    if body is not None:
        arguments.extend(["--input", "-"])
    result = run(*arguments, input=None if body is None else json.dumps(body), stdout=subprocess.PIPE, text=True)
    return json.loads(result.stdout)


def verify_remote_tag(base, tag, commit):
    reference = api(f"{base}/git/ref/tags/{tag}")["object"]
    for _ in range(8):
        if reference["type"] == "commit":
            break
        require(reference["type"] == "tag", "tag does not resolve to a commit")
        reference = api(f"{base}/git/tags/{reference['sha']}")["object"]
    require(reference["type"] == "commit" and reference["sha"] == commit, "remote tag moved after qualification")


def require_release_absent(base, tag):
    # The by-tag endpoint describes published releases. Listing with the writer
    # token also includes drafts, which must never be overwritten or duplicated.
    for page in range(1, 21):
        releases = api(f"{base}/releases?per_page=100&page={page}")
        require(isinstance(releases, list), "invalid release catalog response")
        require(all(item.get("tag_name") != tag for item in releases),
                "release or draft already exists; refusing to change its assets")
        if len(releases) < 100:
            return
    raise ValueError("release history exceeds the bounded absence check")


def publish(directory):
    version, toolchain, platforms = configuration()
    tag = release_tag(version, os.environ.get("GITHUB_EVENT_NAME", ""), os.environ.get("GITHUB_REF", ""))
    require(tag is not None, "manual builds cannot publish")
    commit = source_commit()
    names = verify_collection(directory, version, toolchain, platforms, commit)
    repository = os.environ["GITHUB_REPOSITORY"]
    require(re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository), "invalid repository")
    base = f"repos/{repository}"
    verify_remote_tag(base, tag, commit)
    require_release_absent(base, tag)
    body = ("One executable contains the Focal server, human CLI and stdio MCP server. "
            "Download the raw binary for your platform, verify it with SHA256SUMS, "
            "and make it executable. No Rust installation is needed.\n\n"
            "GNU Linux binaries require glibc 2.39 or later; musl binaries are static. "
            "macOS binaries target macOS 15 or later. Windows binaries target Windows 10 "
            "1809 / Server 2019 or later. macOS notarization and Windows Authenticode "
            "signing are not applied by this release.\n\n"
            "Every asset passed native server startup, CLI mutation/read, acknowledged-write "
            "crash recovery and MCP protocol/read smoke. THIRD-PARTY-NOTICES.txt lists every "
            "bundled crate and its license; sbom.spdx.json is the SPDX 2.3 bill of materials. "
            "See release-manifest.json for target, source and digest details.\n")
    draft = api(f"{base}/releases", "POST", {"tag_name": tag, "target_commitish": commit,
                "name": f"Focal {version}", "body": body, "draft": True,
                "prerelease": "-" in version.split("+")[0]})
    require(draft.get("draft") is True, "release was not created as a draft")
    # Failed upload/verification leaves only an unpublished draft for inspection.
    # Never use --clobber, and never edit a previously existing release.
    run("gh", "release", "upload", tag, *(str(directory / name) for name in names), "--repo", repository)
    assets = api(f"{base}/releases/{draft['id']}/assets?per_page=100")
    require(len(assets) == len(names) and {item["name"] for item in assets} == set(names), "draft asset set mismatch")
    with tempfile.TemporaryDirectory(prefix="focal-release-verify-") as temporary:
        for item in assets:
            local = directory / item["name"]
            require(item.get("state") == "uploaded" and item.get("size") == local.stat().st_size, "draft upload incomplete")
            downloaded = Path(temporary) / "asset"
            with downloaded.open("wb") as sink:
                run("gh", "api", "-H", "Accept: application/octet-stream", f"{base}/releases/assets/{item['id']}", stdout=sink)
            require(digest(downloaded) == digest(local), f"uploaded bytes differ: {item['name']}")
    current = api(f"{base}/releases/{draft['id']}")
    require(current.get("draft") is True and current.get("tag_name") == tag, "draft changed during publication")
    require({(item["id"], item["name"], item["size"], item["state"]) for item in current["assets"]}
            == {(item["id"], item["name"], item["size"], item["state"]) for item in assets}, "draft assets changed during verification")
    verify_remote_tag(base, tag, commit)
    published = api(f"{base}/releases/{draft['id']}", "PATCH", {"draft": False})
    require(published.get("draft") is False, "publication did not complete")
    print(published["html_url"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser("guard")
    staged = subcommands.add_parser("stage")
    staged.add_argument("--target", required=True)
    staged.add_argument("--output", type=Path, required=True)
    for name in ("collect", "verify", "publish"):
        command = subcommands.add_parser(name)
        command.add_argument("--input", type=Path, required=True)
        if name == "collect":
            command.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "guard":
        guard()
    elif args.command == "stage":
        stage(args.target, args.output)
    elif args.command == "publish":
        publish(args.input)
    else:
        version, toolchain, platforms = configuration()
        commit = source_commit()
        if args.command == "collect":
            collect(args.input, args.output, version, toolchain, platforms, commit)
        else:
            verify_collection(args.input, version, toolchain, platforms, commit)


if __name__ == "__main__":
    main()
