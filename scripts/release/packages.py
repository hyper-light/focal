#!/usr/bin/env python3
"""Package a collected release for PyPI and npm.

Both registries receive the very executables the GitHub release attaches: a
wheel and an npm platform package per target are built from the verified
collection (`release.py collect`), never from a second compilation, so the
bytes `pip` and `npm` install are the bytes `SHA256SUMS` names. The wheel is a
PEP 427 archive with one script and no Python code (`focal_node-<v>.data/
scripts/focal`), tagged for the platform the binary was built and smoked on;
the npm package `@hyper-light/focal` is a wrapper whose optional dependencies
are the eight platform packages, each carrying one executable.

Every archive is written deterministically (the source commit's time, fixed
ownership, sorted members) so a rebuild from the same collection is the same
bytes, and `verify` re-reads every archive against the collection before
anything is published. Version numbers come from the workspace alone: the
manifests under `packaging/npm/` carry the placeholder `0.0.0`, which the
build stamps and the guard checks.
"""

import argparse
import base64
import gzip
import hashlib
import io
import json
import os
import re
import stat
import tarfile
import time
import zipfile
from pathlib import Path

import notices
import release

ROOT = release.ROOT
NPM_SOURCE = ROOT / "packaging" / "npm"
PYTHON_README = ROOT / "packaging" / "python" / "README.md"
LICENSE = ROOT / "LICENSE"
PLACEHOLDER = "0.0.0"

PYPI_NAME = "focal-node"  # `focal` and `focal-cli` belong to other projects on PyPI
WHEEL_NAME = "focal_node"  # the PEP 427 normalized form of the distribution name
NPM_SCOPE = "@hyper-light"
NPM_WRAPPER = f"{NPM_SCOPE}/focal"
SUMMARY = ("Focal: the coordination protocol and evidence ledger for AI agents — "
           "one executable with the server, the human CLI and the stdio MCP server")
AUTHOR = "Ada Lundhe <adalundhe@lundhe.audio>"
REPOSITORY = "https://github.com/hyper-light/focal"
MANIFEST = "packages-manifest.json"

# One row per release target (`platforms.json`): the npm platform package suffix
# and the wheel's platform tags. macOS binaries are built for macOS 15 (the
# lanes set MACOSX_DEPLOYMENT_TARGET); the GNU binaries need glibc 2.39, the
# runner's, so the wheel says so (PEP 600 `manylinux_2_39`); the musl binaries
# are fully static, so they satisfy any glibc floor and the musllinux tag at
# once — `pip` prefers the GNU wheel where glibc 2.39 exists and installs the
# static one everywhere else.
PLATFORMS = {
    "aarch64-apple-darwin": ("darwin-arm64", ("macosx_15_0_arm64",)),
    "x86_64-apple-darwin": ("darwin-x64", ("macosx_15_0_x86_64",)),
    "x86_64-unknown-linux-gnu": ("linux-x64-gnu", ("manylinux_2_39_x86_64",)),
    "aarch64-unknown-linux-gnu": ("linux-arm64-gnu", ("manylinux_2_39_aarch64",)),
    "x86_64-unknown-linux-musl": ("linux-x64-musl", ("manylinux_2_17_x86_64", "musllinux_1_2_x86_64")),
    "aarch64-unknown-linux-musl": ("linux-arm64-musl", ("manylinux_2_17_aarch64", "musllinux_1_2_aarch64")),
    "x86_64-pc-windows-msvc": ("win32-x64-msvc", ("win_amd64",)),
    "aarch64-pc-windows-msvc": ("win32-arm64-msvc", ("win_arm64",)),
}
CLASSIFIERS = (
    "Development Status :: 3 - Alpha",
    "Environment :: Console",
    "Intended Audience :: Developers",
    "Operating System :: MacOS",
    "Operating System :: Microsoft :: Windows",
    "Operating System :: POSIX :: Linux",
    "Programming Language :: Rust",
    "Topic :: Software Development",
)

require = release.require


def pep440(version):
    """The workspace's semantic version as PEP 440 states it: a final version
    unchanged, a pre-release `-alpha.N` / `-beta.N` / `-rc.N` as `aN` / `bN` /
    `rcN`. Anything else has no PyPI form and is refused."""
    match = re.fullmatch(r"(\d+\.\d+\.\d+)(?:-(alpha|beta|rc)\.(\d+))?", version)
    require(match is not None, f"no PyPI version for workspace version {version!r}")
    base, kind, number = match.groups()
    if kind is None:
        return base
    return base + {"alpha": "a", "beta": "b", "rc": "rc"}[kind] + number


def platform_package(platform):
    return f"{NPM_SCOPE}/focal-{platform}"


def tarball_name(name, version):
    """npm's own naming for `npm pack`: the scope's `@` dropped, `/` as `-`."""
    return f"{name.removeprefix('@').replace('/', '-')}-{version}.tgz"


def commit_time(commit):
    """The source commit's time, the timestamp every archive member carries."""
    seconds = int(release.output("git", "-C", str(ROOT), "show", "-s", "--format=%ct", commit))
    require(seconds >= 315532800, "commit time predates the ZIP epoch (1980)")
    return seconds


def urlsafe_digest(data):
    return "sha256=" + base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()


def check_sources(platforms):
    """The packaging sources agree with the release catalog: one platform
    package per target, every manifest at the placeholder version, the wrapper
    depending on exactly those packages, the text files present."""
    targets = {row["target"] for row in platforms["include"]}
    require(set(PLATFORMS) == targets == release.REQUIRED_TARGETS, "packaging platform map differs from the release catalog")
    wrapper = json.loads((NPM_SOURCE / "package.json").read_text())
    require(wrapper.get("name") == NPM_WRAPPER and wrapper.get("version") == PLACEHOLDER,
            "npm wrapper manifest must carry the placeholder version")
    require(wrapper.get("bin") == {"focal": "focal"} and wrapper.get("scripts") == {"postinstall": "node postinstall.js"},
            "npm wrapper manifest must expose the focal command and its postinstall")
    require(set(wrapper.get("files", [])) == {"focal", "postinstall.js", "LICENSE", "README.md"}, "npm wrapper files list")
    expected = {platform_package(platform): PLACEHOLDER for platform, _ in PLATFORMS.values()}
    require(wrapper.get("optionalDependencies") == expected, "npm wrapper optional dependencies differ from the platform map")
    for path in ("focal", "postinstall.js", "README.md"):
        require((NPM_SOURCE / path).is_file(), f"missing npm wrapper file {path}")
    for target, (platform, _) in PLATFORMS.items():
        manifest = json.loads((NPM_SOURCE / "platforms" / platform / "package.json").read_text())
        require(manifest.get("name") == platform_package(platform) and manifest.get("version") == PLACEHOLDER,
                f"platform manifest {platform} must carry its name and the placeholder version")
        binary = "focal.exe" if "windows" in target else "focal"
        require(manifest.get("files") == [binary, "LICENSE", "THIRD-PARTY-NOTICES.txt"], f"platform manifest {platform} files list")
        os_name, cpu = platform.split("-")[:2]
        require(manifest.get("os") == [os_name] and manifest.get("cpu") == [cpu], f"platform manifest {platform} os/cpu")
        if os_name == "linux":
            require(manifest.get("libc") == ["musl" if platform.endswith("musl") else "glibc"], f"platform manifest {platform} libc")
        else:
            require("libc" not in manifest, f"platform manifest {platform} names a libc")
    require(PYTHON_README.is_file() and LICENSE.is_file(), "missing packaging text files")


def zip_member(archive, name, data, mode, stamp):
    info = zipfile.ZipInfo(name, date_time=time.gmtime(stamp)[:6])
    info.compress_type = zipfile.ZIP_DEFLATED
    # A Unix regular file with this mode: pip keeps the executable bit only
    # when the entry says it is a regular file (S_IFREG) with one set.
    info.create_system = 3
    info.external_attr = (stat.S_IFREG | (mode & 0o7777)) << 16
    archive.writestr(info, data)


def wheel(collection, row, version, stamp, destination):
    """One wheel: the target's executable as a script, the license texts, the
    metadata; every member recorded with its digest and size."""
    platform, tags = PLATFORMS[row["target"]]
    pypi_version = pep440(version)
    binary = "focal.exe" if "windows" in row["target"] else "focal"
    data_dir = f"{WHEEL_NAME}-{pypi_version}.data"
    info_dir = f"{WHEEL_NAME}-{pypi_version}.dist-info"
    metadata = [
        "Metadata-Version: 2.4", f"Name: {PYPI_NAME}", f"Version: {pypi_version}", f"Summary: {SUMMARY}",
        f"Author-email: {AUTHOR}", "License-Expression: MIT", "License-File: LICENSE",
        "License-File: THIRD-PARTY-NOTICES.txt", *(f"Classifier: {value}" for value in CLASSIFIERS),
        "Requires-Python: >=3.8", f"Project-URL: Repository, {REPOSITORY}",
        f"Project-URL: Documentation, {REPOSITORY}/tree/main/docs", f"Project-URL: Releases, {REPOSITORY}/releases",
        "Description-Content-Type: text/markdown", "", PYTHON_README.read_text(),
    ]
    wheel_file = ["Wheel-Version: 1.0", "Generator: focal-release (scripts/release/packages.py)",
                  "Root-Is-Purelib: false", *(f"Tag: py3-none-{tag}" for tag in tags), ""]
    members = [
        (f"{data_dir}/scripts/{binary}", (collection / row["asset"]).read_bytes(), 0o755),
        (f"{info_dir}/licenses/LICENSE", LICENSE.read_bytes(), 0o644),
        (f"{info_dir}/licenses/THIRD-PARTY-NOTICES.txt", (collection / notices.NOTICES).read_bytes(), 0o644),
        (f"{info_dir}/METADATA", "\n".join(metadata).encode(), 0o644),
        (f"{info_dir}/WHEEL", "\n".join(wheel_file).encode(), 0o644),
    ]
    record = "".join(f"{name},{urlsafe_digest(data)},{len(data)}\n" for name, data, _ in members)
    record += f"{info_dir}/RECORD,,\n"
    path = destination / f"{WHEEL_NAME}-{pypi_version}-py3-none-{'.'.join(tags)}.whl"
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, data, mode in members:
            zip_member(archive, name, data, mode, stamp)
        zip_member(archive, f"{info_dir}/RECORD", record.encode(), 0o644, stamp)
    path.write_bytes(buffer.getvalue())
    return path


def npm_tarball(members, name, version, stamp, destination):
    """One npm package archive as `npm pack` lays it out (`package/` prefix,
    gzip), with fixed ownership and the commit's time on every member."""
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as archive:
        for member_name, data, mode in members:
            info = tarfile.TarInfo(f"package/{member_name}")
            info.size = len(data)
            info.mode = mode
            info.mtime = stamp
            info.uid = info.gid = 0
            info.uname = info.gname = ""
            archive.addfile(info, io.BytesIO(data))
    path = destination / tarball_name(name, version)
    with path.open("wb") as sink:
        with gzip.GzipFile(fileobj=sink, mode="wb", mtime=stamp) as compressed:
            compressed.write(buffer.getvalue())
    return path


def stamped(manifest, version):
    manifest = dict(manifest)
    manifest["version"] = version
    if "optionalDependencies" in manifest:
        manifest["optionalDependencies"] = {name: version for name in manifest["optionalDependencies"]}
    return (json.dumps(manifest, indent=2) + "\n").encode()


def build(collection, destination, version, toolchain, platforms, commit):
    """Every wheel and npm archive from a verified collection, plus a manifest
    naming each archive's digest, size, target and registry name."""
    require(not destination.exists(), "refusing to replace existing packages")
    release.verify_collection(collection, version, toolchain, platforms, commit)
    check_sources(platforms)
    stamp = commit_time(commit)
    destination.mkdir(parents=True)
    (destination / "wheels").mkdir()
    (destination / "npm").mkdir()
    entries = []
    for row in sorted(platforms["include"], key=lambda item: item["asset"]):
        platform, tags = PLATFORMS[row["target"]]
        path = wheel(collection, row, version, stamp, destination / "wheels")
        entries.append({"registry": "pypi", "name": PYPI_NAME, "version": pep440(version), "target": row["target"],
                        "file": f"wheels/{path.name}", "tags": list(tags), "bytes": path.stat().st_size, "sha256": release.digest(path)})
        binary = "focal.exe" if "windows" in row["target"] else "focal"
        manifest = json.loads((NPM_SOURCE / "platforms" / platform / "package.json").read_text())
        members = [("package.json", stamped(manifest, version), 0o644),
                   (binary, (collection / row["asset"]).read_bytes(), 0o755),
                   ("LICENSE", LICENSE.read_bytes(), 0o644),
                   ("THIRD-PARTY-NOTICES.txt", (collection / notices.NOTICES).read_bytes(), 0o644)]
        path = npm_tarball(members, manifest["name"], version, stamp, destination / "npm")
        entries.append({"registry": "npm", "name": manifest["name"], "version": version, "target": row["target"],
                        "file": f"npm/{path.name}", "bytes": path.stat().st_size, "sha256": release.digest(path)})
    wrapper = json.loads((NPM_SOURCE / "package.json").read_text())
    members = [("package.json", stamped(wrapper, version), 0o644),
               ("focal", (NPM_SOURCE / "focal").read_bytes(), 0o755),
               ("postinstall.js", (NPM_SOURCE / "postinstall.js").read_bytes(), 0o644),
               ("README.md", (NPM_SOURCE / "README.md").read_bytes(), 0o644),
               ("LICENSE", LICENSE.read_bytes(), 0o644)]
    path = npm_tarball(members, NPM_WRAPPER, version, stamp, destination / "npm")
    entries.append({"registry": "npm", "name": NPM_WRAPPER, "version": version, "target": None,
                    "file": f"npm/{path.name}", "bytes": path.stat().st_size, "sha256": release.digest(path)})
    manifest = {"schema": 1, "version": version, "commit": commit, "toolchain": toolchain, "packages": entries}
    (destination / MANIFEST).write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    verify(destination, collection, version, toolchain, platforms, commit)
    return sorted(entry["file"] for entry in entries)


def read_wheel(path):
    """A wheel's members checked against its own RECORD; returns them by name."""
    with zipfile.ZipFile(path) as archive:
        members = {info.filename: archive.read(info) for info in archive.infolist()}
        modes = {info.filename: (info.external_attr >> 16) & 0o7777 for info in archive.infolist()}
    records = [name for name in members if name.endswith(".dist-info/RECORD")]
    require(len(records) == 1, f"{path.name}: one RECORD expected")
    listed = {}
    for line in members[records[0]].decode().splitlines():
        name, digest_value, size = line.split(",")
        listed[name] = (digest_value, size)
    require(set(listed) == set(members), f"{path.name}: RECORD names differ from the archive")
    for name, data in members.items():
        if name == records[0]:
            require(listed[name] == ("", ""), f"{path.name}: RECORD must not record itself")
        else:
            require(listed[name] == (urlsafe_digest(data), str(len(data))), f"{path.name}: RECORD mismatch for {name}")
    return members, modes


def read_tarball(path):
    with tarfile.open(path, "r:gz") as archive:
        members = {}
        modes = {}
        for info in archive.getmembers():
            require(info.isfile() and info.name.startswith("package/") and ".." not in info.name, f"{path.name}: unexpected member {info.name}")
            members[info.name.removeprefix("package/")] = archive.extractfile(info).read()
            modes[info.name.removeprefix("package/")] = info.mode & 0o7777
    return members, modes


def verify(directory, collection, version, toolchain, platforms, commit):
    """Every archive re-read: its digest as the manifest names it, its
    executable the collection's bytes, its metadata this version and target."""
    release.verify_collection(collection, version, toolchain, platforms, commit)
    manifest = json.loads((directory / MANIFEST).read_text())
    require((manifest.get("schema"), manifest.get("version"), manifest.get("toolchain"), manifest.get("commit"))
            == (1, version, toolchain, commit), "packages manifest source mismatch")
    entries = manifest["packages"]
    files = {entry["file"] for entry in entries}
    require(len(files) == len(entries) == 2 * len(platforms["include"]) + 1, "packages manifest names a wrong number of archives")
    present = {str(path.relative_to(directory)) for path in directory.rglob("*") if path.is_file()} - {MANIFEST}
    require(present == files, "package archives differ from the manifest")
    expected_binary = {row["target"]: (collection / row["asset"]).read_bytes() for row in platforms["include"]}
    seen_targets = {"pypi": set(), "npm": set()}
    for entry in entries:
        path = directory / entry["file"]
        require(entry["bytes"] == path.stat().st_size and entry["sha256"] == release.digest(path), f"archive digest mismatch: {entry['file']}")
        if entry["registry"] == "pypi":
            require(entry["name"] == PYPI_NAME and entry["version"] == pep440(version), "wheel name or version")
            platform, tags = PLATFORMS[entry["target"]]
            require(entry["tags"] == list(tags) and path.name == f"{WHEEL_NAME}-{pep440(version)}-py3-none-{'.'.join(tags)}.whl", "wheel tags")
            members, modes = read_wheel(path)
            binary = "focal.exe" if "windows" in entry["target"] else "focal"
            data_dir = f"{WHEEL_NAME}-{pep440(version)}.data"
            info_dir = f"{WHEEL_NAME}-{pep440(version)}.dist-info"
            require(set(members) == {f"{data_dir}/scripts/{binary}", f"{info_dir}/licenses/LICENSE",
                                     f"{info_dir}/licenses/THIRD-PARTY-NOTICES.txt", f"{info_dir}/METADATA",
                                     f"{info_dir}/WHEEL", f"{info_dir}/RECORD"}, f"{path.name}: member set")
            require(members[f"{data_dir}/scripts/{binary}"] == expected_binary[entry["target"]], f"{path.name}: executable differs from the collection")
            require(modes[f"{data_dir}/scripts/{binary}"] == 0o755, f"{path.name}: executable mode")
            require(members[f"{info_dir}/licenses/THIRD-PARTY-NOTICES.txt"] == (collection / notices.NOTICES).read_bytes(), f"{path.name}: notices differ")
            metadata = members[f"{info_dir}/METADATA"].decode()
            require(f"\nName: {PYPI_NAME}\nVersion: {pep440(version)}\n" in metadata and metadata.startswith("Metadata-Version: 2.4\n"), f"{path.name}: METADATA")
            require(members[f"{info_dir}/WHEEL"].decode().splitlines()[3:] == [f"Tag: py3-none-{tag}" for tag in tags], f"{path.name}: WHEEL tags")
            seen_targets["pypi"].add(entry["target"])
        else:
            require(entry["registry"] == "npm" and entry["version"] == version, "npm entry")
            members, modes = read_tarball(path)
            package = json.loads(members["package.json"])
            require(package.get("name") == entry["name"] and package.get("version") == version, f"{path.name}: package.json name or version")
            require(path.name == tarball_name(entry["name"], version), f"{path.name}: archive name")
            if entry["target"] is None:
                require(entry["name"] == NPM_WRAPPER, "wrapper name")
                require(set(members) == {"package.json", "focal", "postinstall.js", "README.md", "LICENSE"}, f"{path.name}: member set")
                require(package.get("optionalDependencies") == {platform_package(platform): version for platform, _ in PLATFORMS.values()},
                        f"{path.name}: optional dependencies")
                require(modes["focal"] == 0o755 and members["focal"] == (NPM_SOURCE / "focal").read_bytes(), f"{path.name}: shim")
                seen_targets["npm"].add(None)
            else:
                platform, _ = PLATFORMS[entry["target"]]
                require(entry["name"] == platform_package(platform), f"{path.name}: platform package name")
                binary = "focal.exe" if "windows" in entry["target"] else "focal"
                require(set(members) == {"package.json", binary, "LICENSE", "THIRD-PARTY-NOTICES.txt"}, f"{path.name}: member set")
                require(members[binary] == expected_binary[entry["target"]] and modes[binary] == 0o755, f"{path.name}: executable differs from the collection")
                require(members["THIRD-PARTY-NOTICES.txt"] == (collection / notices.NOTICES).read_bytes(), f"{path.name}: notices differ")
                seen_targets["npm"].add(entry["target"])
    require(seen_targets["pypi"] == set(PLATFORMS) and seen_targets["npm"] == set(PLATFORMS) | {None}, "a target's package is missing")
    return sorted(files)


def live(name, version):
    """Whether the registry already serves this exact version: a rerun after
    a failure must not fail on what is already published, and never republishes."""
    try:
        return release.output("npm", "view", f"{name}@{version}", "version") == version
    except release.subprocess.CalledProcessError:
        return False


def publish_npm(directory, version, toolchain, platforms, commit, collection):
    """The platform packages first, then the wrapper that depends on them, each
    skipped when its version is already live. Authentication is the workflow's
    (npm trusted publishing); nothing here holds a token."""
    verify(directory, collection, version, toolchain, platforms, commit)
    entries = json.loads((directory / MANIFEST).read_text())["packages"]
    ordered = [entry for entry in entries if entry["registry"] == "npm" and entry["target"] is not None]
    ordered += [entry for entry in entries if entry["registry"] == "npm" and entry["target"] is None]
    for entry in ordered:
        if live(entry["name"], version):
            print(f"{entry['name']}@{version} is already published; left as it is")
            continue
        release.run("npm", "publish", str(directory / entry["file"]), "--access", "public")
        print(f"published {entry['name']}@{version}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser("check")
    for name in ("build", "verify", "publish-npm"):
        command = subcommands.add_parser(name)
        command.add_argument("--collection", type=Path, required=True, help="the verified release collection")
        if name == "build":
            command.add_argument("--output", type=Path, required=True)
        else:
            command.add_argument("--input", type=Path, required=True)
    args = parser.parse_args()
    version, toolchain, platforms = release.configuration()
    if args.command == "check":
        check_sources(platforms)
        print("packaging sources agree with the release catalog")
        return
    commit = release.source_commit()
    if args.command == "build":
        for name in build(args.collection, args.output, version, toolchain, platforms, commit):
            print(name)
    elif args.command == "verify":
        for name in verify(args.input, args.collection, version, toolchain, platforms, commit):
            print(name)
    else:
        publish_npm(args.input, version, toolchain, platforms, commit, args.collection)


if __name__ == "__main__":
    main()
