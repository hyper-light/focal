"""Bounded orchestration tests; no network, compiler, server or publishing."""

import copy
import json
import os
from pathlib import Path
import shutil
import struct
import tempfile
import unittest
from unittest.mock import patch

import notices
import release


class CollectionFixture:
    """A synthetic complete artifact set, collected on demand."""

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="focal-release-tests-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.version, self.toolchain, self.platforms = release.configuration()
        self.commit = "1" * 40
        self.source = self.root / "artifacts"
        self.source.mkdir()
        self.destination = self.root / "collected"
        for row in self.platforms["include"]:
            directory = self.source / f"binary-{row['asset']}"
            directory.mkdir()
            binary = directory / row["asset"]
            binary.write_bytes(f"synthetic collector test: {row['target']}".encode())
            metadata = {**row, "version": self.version, "toolchain": self.toolchain,
                        "commit": self.commit, "bytes": binary.stat().st_size,
                        "sha256": release.digest(binary), "roles": ["server", "cli", "mcp"],
                        "native_smoke": "server-cli-crash-recovery-mcp"}
            (directory / f"{row['asset']}.json").write_text(json.dumps(metadata))

    def collect(self):
        release.collect(self.source, self.destination, self.version, self.toolchain, self.platforms, self.commit)

    def verify(self):
        return release.verify_collection(self.destination, self.version, self.toolchain, self.platforms, self.commit)

    def metadata(self):
        row = self.platforms["include"][0]
        return self.source / f"binary-{row['asset']}" / f"{row['asset']}.json"


class ReleaseTests(CollectionFixture, unittest.TestCase):
    def test_tag_must_match_and_manual_tag_dispatch_never_publishes(self):
        self.assertEqual(release.release_tag(self.version, "push", f"refs/tags/v{self.version}"), f"v{self.version}")
        self.assertIsNone(release.release_tag(self.version, "workflow_dispatch", f"refs/tags/v{self.version}"))
        for event, ref in [("push", "refs/heads/main"), ("push", "refs/tags/v999.0.0"),
                           ("pull_request", f"refs/tags/v{self.version}")]:
            with self.subTest(event=event, ref=ref), self.assertRaises(ValueError):
                release.release_tag(self.version, event, ref)

    def test_collection_preserves_all_raw_bytes_and_checksums(self):
        self.collect()
        names = self.verify()
        self.assertEqual(len(names), len(self.platforms["include"]) + 4)
        for row in self.platforms["include"]:
            name = row["asset"]
            self.assertEqual((self.source / f"binary-{name}" / name).read_bytes(), (self.destination / name).read_bytes())
            self.assertEqual((self.destination / name).stat().st_mode & 0o777, 0o755)
        with self.assertRaises(ValueError):
            self.collect()
        self.verify()

    def test_missing_platform_never_exposes_partial_collection(self):
        shutil.rmtree(self.metadata().parent)
        with self.assertRaises(ValueError):
            self.collect()
        self.assertFalse(self.destination.exists())
        self.assertFalse(list(self.root.glob(".focal-release-*")))

    def test_unexpected_platform_or_extra_file_rejects(self):
        (self.source / "unexpected").mkdir()
        with self.assertRaises(ValueError):
            self.collect()
        (self.source / "unexpected").rmdir()
        (self.metadata().parent / "unrequested").write_text("not a release artifact")
        with self.assertRaises(ValueError):
            self.collect()

    def test_corrupt_binary_rolls_back_all_provisional_collection(self):
        row = self.platforms["include"][-1]
        (self.source / f"binary-{row['asset']}" / row["asset"]).write_bytes(b"corrupt")
        with self.assertRaises(ValueError):
            self.collect()
        self.assertFalse(self.destination.exists())
        self.assertFalse(list(self.root.glob(".focal-release-*")))

    def test_wrong_source_target_or_smoke_provenance_rejects(self):
        path = self.metadata()
        original = json.loads(path.read_text())
        for field, value in [("commit", "2" * 40), ("version", "999.0.0"),
                             ("target", "x86_64-pc-windows-msvc"), ("native_smoke", "not-run")]:
            with self.subTest(field=field):
                path.write_text(json.dumps({**original, field: value}))
                with self.assertRaises(ValueError):
                    self.collect()
                self.assertFalse(self.destination.exists())
        path.write_text(json.dumps(original))

    def test_checksum_verifier_rejects_tampering_duplicates_and_traversal(self):
        self.collect()
        path = self.destination / release.SUMS
        original = path.read_text()
        lines = original.splitlines()
        bad = [lines[0], *lines[0:-1]]
        for text in ["\n".join(bad) + "\n", original.replace("release-manifest.json", "../release-manifest.json"),
                     "0" * 64 + original[64:]]:
            with self.subTest(text=text[:80]):
                path.write_text(text)
                with self.assertRaises(ValueError):
                    self.verify()
        path.write_text(original)
        binary = self.destination / self.platforms["include"][0]["asset"]
        binary.write_bytes(b"changed after collection")
        with self.assertRaises(ValueError):
            self.verify()

    def test_manifest_target_and_duplicate_rows_are_checked_independently(self):
        self.collect()
        path = self.destination / release.MANIFEST
        original = json.loads(path.read_text())
        for duplicate in [True, False]:
            value = copy.deepcopy(original)
            if duplicate:
                value["assets"][1] = value["assets"][0]
            else:
                value["assets"][0]["target"] = "wrong"
            path.write_text(json.dumps(value))
            with self.assertRaises(ValueError):
                self.verify()

    def publish_fixture(self, corrupt=False, exists=False):
        self.collect()
        names = self.verify()
        base = "repos/test/focal"
        calls = []
        assets = [{"id": index, "name": name, "state": "uploaded",
                   "size": (self.destination / name).stat().st_size} for index, name in enumerate(names)]

        def api(path, method="GET", body=None):
            calls.append((method, path, body))
            if path.startswith(f"{base}/git/ref/tags/"):
                return {"object": {"type": "commit", "sha": self.commit}}
            if path.startswith(f"{base}/releases?per_page="):
                return [{"tag_name": f"v{self.version}", "draft": True}] if exists else []
            if method == "POST":
                self.assertTrue(body["draft"])
                return {"id": 4, "draft": True}
            if path.endswith("/assets?per_page=100"):
                return assets
            if method == "PATCH":
                self.assertEqual(sum(event[0] == "download" for event in calls), len(assets))
                self.assertEqual(body, {"draft": False})
                return {"draft": False, "html_url": "https://example.invalid/release"}
            return {"draft": True, "tag_name": f"v{self.version}", "assets": assets}

        def run(*arguments, **kwargs):
            if arguments[:3] == ("gh", "release", "upload"):
                calls.append(("upload", arguments, None))
                self.assertNotIn("--clobber", arguments)
            else:
                asset = assets[int(arguments[-1].rsplit("/", 1)[1])]
                data = (self.destination / asset["name"]).read_bytes()
                kwargs["stdout"].write(b"corrupt" if corrupt else data)
                calls.append(("download", asset["name"], None))

        environment = {"GITHUB_EVENT_NAME": "push", "GITHUB_REF": f"refs/tags/v{self.version}",
                       "GITHUB_REPOSITORY": "test/focal"}
        with patch.dict(os.environ, environment), patch.object(release, "configuration", return_value=(self.version, self.toolchain, self.platforms)), \
             patch.object(release, "source_commit", return_value=self.commit), patch.object(release, "api", side_effect=api), \
             patch.object(release, "run", side_effect=run):
            if corrupt or exists:
                with self.assertRaises(ValueError):
                    release.publish(self.destination)
            else:
                release.publish(self.destination)
        return calls

    def test_publish_flips_draft_only_after_every_uploaded_byte_is_verified(self):
        calls = self.publish_fixture()
        self.assertEqual(calls[-1][0], "PATCH")

    def test_corrupt_upload_stays_unpublished(self):
        calls = self.publish_fixture(corrupt=True)
        self.assertTrue(any(call[0] == "POST" for call in calls))
        self.assertFalse(any(call[0] == "PATCH" for call in calls))

    def test_existing_release_is_never_modified(self):
        calls = self.publish_fixture(exists=True)
        self.assertFalse(any(call[0] in {"POST", "upload", "PATCH"} for call in calls))



class PortableExecutableTests(unittest.TestCase):
    """The import-table parser reads DLL names from a synthetic PE32+ image."""

    @staticmethod
    def image(dll_names):
        section_rva = 0x200
        descriptors = section_rva
        names_at = descriptors + 20 * (len(dll_names) + 1)
        data = bytearray(0x400)
        data[0:2] = b"MZ"
        struct.pack_into("<I", data, 0x3C, 0x40)  # e_lfanew
        pe = 0x40
        data[pe : pe + 4] = b"PE\0\0"
        coff = pe + 4
        struct.pack_into("<H", data, coff, 0x8664)      # machine x86_64
        struct.pack_into("<H", data, coff + 2, 1)        # one section
        struct.pack_into("<H", data, coff + 16, 240)     # size of optional header
        optional = coff + 20
        struct.pack_into("<H", data, optional, 0x20B)    # PE32+ magic
        struct.pack_into("<II", data, optional + 112 + 8, section_rva, 20 * (len(dll_names) + 1))
        section = optional + 240
        data[section : section + 8] = b".idata\0\0"
        struct.pack_into("<IIII", data, section + 8, 0x200, section_rva, 0x200, section_rva)
        cursor = names_at
        for index, name in enumerate(dll_names):
            struct.pack_into("<IIIII", data, descriptors + 20 * index, 0, 0, 0, cursor, 0)
            encoded = name.encode() + b"\0"
            data[cursor : cursor + len(encoded)] = encoded
            cursor += len(encoded)
        return bytes(data)

    def test_single_import_is_read(self):
        self.assertEqual(release.pe_imported_dlls(self.image(["KERNEL32.dll"])), {"kernel32.dll"})

    def test_several_imports_are_read_and_lowercased(self):
        self.assertEqual(
            release.pe_imported_dlls(self.image(["KERNEL32.dll", "bcrypt.dll", "WS2_32.dll"])),
            {"kernel32.dll", "bcrypt.dll", "ws2_32.dll"},
        )

    def test_a_non_pe_image_is_refused(self):
        with self.assertRaises(ValueError):
            release.pe_imported_dlls(b"\x7fELF" + b"\0" * 60)



class NoticesTests(unittest.TestCase):
    def test_drift_detection_rejects_a_stale_roster(self):
        inventory = {("serde", "1.0.0"): {"name": "serde", "version": "1.0.0", "license": "MIT", "source": "reg"}}
        packages = [
            {"name": "serde", "version": "1.0.0", "source": "reg"},
            {"name": "tokio", "version": "1.0.0", "source": "reg"},  # missing from roster
            {"name": "focal-node", "version": "0.1.0", "source": None},  # workspace, not counted
        ]
        with self.assertRaises(SystemExit) as caught:
            notices.check_drift(inventory, packages)
        self.assertIn("tokio 1.0.0", str(caught.exception))

    def test_drift_detection_rejects_a_removed_dependency(self):
        inventory = {
            ("serde", "1.0.0"): {"name": "serde", "version": "1.0.0", "license": "MIT", "source": "reg"},
            ("gone", "0.1.0"): {"name": "gone", "version": "0.1.0", "license": "MIT", "source": "reg"},
        }
        packages = [{"name": "serde", "version": "1.0.0", "source": "reg"}]
        with self.assertRaises(SystemExit) as caught:
            notices.check_drift(inventory, packages)
        self.assertIn("gone 0.1.0", str(caught.exception))

    def test_workspace_crates_are_absent_from_notices_and_present_in_the_sbom(self):
        packages = [
            {"name": "serde", "version": "1.0.0", "source": "reg", "license": "MIT"},
            {"name": "focal-node", "version": "0.1.0", "source": None, "license": "MIT"},
        ]
        text = notices.render_notices(packages)
        self.assertIn("serde 1.0.0", text)
        self.assertNotIn("focal-node", text)
        sbom = json.loads(notices.render_sbom(packages, "1.0.0", "abc"))
        self.assertEqual(sbom["spdxVersion"], "SPDX-2.3")
        self.assertEqual({p["name"] for p in sbom["packages"]}, {"serde", "focal-node"})

    def test_a_vendored_crate_is_third_party_with_the_roster_locator(self):
        # A lockfile package without a source that is not a workspace member is
        # built from vendor/: it is a third-party notice with the archive it was
        # unpacked from as its locator, and counts toward roster drift.
        locator = "https://static.crates.io/crates/aws-lc-sys/aws-lc-sys-0.45.0.crate#sha256=9bff"
        inventory = {
            ("aws-lc-sys", "0.45.0"): {"name": "aws-lc-sys", "version": "0.45.0", "license": "ISC", "source": locator},
        }
        vendored = {"name": "aws-lc-sys", "version": "0.45.0", "source": None, "third_party": True, "vendored": True}
        member = {"name": "focal-node", "version": "0.1.0", "source": None, "third_party": False, "vendored": False}
        notices.check_drift(inventory, [vendored, member])
        with self.assertRaises(SystemExit) as caught:
            notices.check_drift({}, [vendored, member])
        self.assertIn("aws-lc-sys 0.45.0", str(caught.exception))
        resolved = notices.resolve_licenses(inventory, [vendored, member])
        self.assertEqual(resolved[0]["license"], "ISC")
        self.assertEqual(resolved[0]["source"], locator)
        self.assertEqual(resolved[1]["license"], notices.WORKSPACE_LICENSE)
        self.assertIsNone(resolved[1]["source"])
        text = notices.render_notices(resolved)
        self.assertIn("aws-lc-sys 0.45.0", text)
        self.assertIn(f"Source:  {locator}", text)
        self.assertIn("Built from: vendor/aws-lc-sys", text)
        self.assertNotIn("focal-node", text)
        sbom = json.loads(notices.render_sbom(resolved, "1.0.0", "abc"))
        by_name = {p["name"]: p for p in sbom["packages"]}
        self.assertEqual(by_name["aws-lc-sys"]["downloadLocation"], locator)
        self.assertEqual(by_name["focal-node"]["downloadLocation"], "NOASSERTION")

    def test_the_locked_graph_marks_the_vendored_crates_and_the_workspace_apart(self):
        members = notices.workspace_members()
        self.assertIn("focal-node", members)
        self.assertNotIn("aws-lc-sys", members)
        by_name = {p["name"]: p for p in notices.lock_packages()}
        for name in ("aws-lc-sys", "aws-lc-rs"):
            self.assertIsNone(by_name[name]["source"])
            self.assertTrue(by_name[name]["third_party"] and by_name[name]["vendored"], name)
        self.assertFalse(by_name["focal-node"]["third_party"])
        self.assertTrue(by_name["serde"]["third_party"] and not by_name["serde"]["vendored"])

    def test_generate_is_deterministic_and_covers_the_locked_graph(self):
        with tempfile.TemporaryDirectory() as first, tempfile.TemporaryDirectory() as second:
            a = notices.generate(first, "9.9.9", "cafef00d")
            b = notices.generate(second, "9.9.9", "cafef00d")
            self.assertEqual(a, [notices.NOTICES, notices.SBOM])
            for name in a:
                self.assertEqual((Path(first) / name).read_bytes(), (Path(second) / name).read_bytes())
            sbom = json.loads((Path(first) / notices.SBOM).read_text())
            locked = notices.lock_packages()
            self.assertEqual(len(sbom["packages"]), len(locked))

if __name__ == "__main__":
    unittest.main()


import packages  # noqa: E402  (the packaging tests below build on ReleaseTests' collection)


class PackagesTests(CollectionFixture, unittest.TestCase):
    """The PyPI wheels and npm archives built from a verified collection."""

    def setUp(self):
        super().setUp()
        self.collect()
        self.packages = self.root / "packages"
        self.stamp = 1_700_000_000
        self.patcher = patch.object(packages, "commit_time", return_value=self.stamp)
        self.patcher.start()
        self.addCleanup(self.patcher.stop)

    def build(self):
        return packages.build(self.destination, self.packages, self.version, self.toolchain, self.platforms, self.commit)

    def verify_packages(self):
        return packages.verify(self.packages, self.destination, self.version, self.toolchain, self.platforms, self.commit)

    def test_sources_agree_with_the_catalog(self):
        packages.check_sources(self.platforms)
        wrong = copy.deepcopy(self.platforms)
        wrong["include"][0]["target"] = "riscv64gc-unknown-linux-gnu"
        with self.assertRaises(ValueError):
            packages.check_sources(wrong)

    def test_semantic_versions_have_one_pypi_form(self):
        self.assertEqual(packages.pep440("0.1.0"), "0.1.0")
        self.assertEqual(packages.pep440("0.2.0-rc.1"), "0.2.0rc1")
        self.assertEqual(packages.pep440("1.0.0-alpha.2"), "1.0.0a2")
        self.assertEqual(packages.pep440("1.0.0-beta.3"), "1.0.0b3")
        for version in ("0.1.0-dev.1", "0.1.0+build", "1.0"):
            with self.assertRaises(ValueError):
                packages.pep440(version)

    def test_every_target_gets_a_wheel_and_a_platform_package_carrying_its_bytes(self):
        names = self.build()
        self.assertEqual(len(names), 2 * len(self.platforms["include"]) + 1)
        self.assertEqual(self.verify_packages(), names)
        wheels = sorted((self.packages / "wheels").iterdir())
        self.assertEqual({path.name for path in wheels}, {
            f"focal_node-{self.version}-py3-none-{'.'.join(tags)}.whl" for _, tags in packages.PLATFORMS.values()})
        for row in self.platforms["include"]:
            platform, tags = packages.PLATFORMS[row["target"]]
            wheel = self.packages / "wheels" / f"focal_node-{self.version}-py3-none-{'.'.join(tags)}.whl"
            members, modes = packages.read_wheel(wheel)
            binary = "focal.exe" if "windows" in row["target"] else "focal"
            script = f"focal_node-{self.version}.data/scripts/{binary}"
            self.assertEqual(members[script], (self.destination / row["asset"]).read_bytes())
            self.assertEqual(modes[script], 0o755)
            metadata = members[f"focal_node-{self.version}.dist-info/METADATA"].decode()
            self.assertIn("License-File: THIRD-PARTY-NOTICES.txt\n", metadata)
            self.assertIn("Requires-Python: >=3.8\n", metadata)
            tarball = self.packages / "npm" / packages.tarball_name(packages.platform_package(platform), self.version)
            members, modes = packages.read_tarball(tarball)
            self.assertEqual(members[binary], (self.destination / row["asset"]).read_bytes())
            self.assertEqual(modes[binary], 0o755)
            package = json.loads(members["package.json"])
            self.assertEqual((package["name"], package["version"]), (packages.platform_package(platform), self.version))
            self.assertNotIn("0.0.0", members["package.json"].decode())
        wrapper = self.packages / "npm" / packages.tarball_name(packages.NPM_WRAPPER, self.version)
        members, _ = packages.read_tarball(wrapper)
        package = json.loads(members["package.json"])
        self.assertEqual(set(package["optionalDependencies"].values()), {self.version})
        self.assertEqual(len(package["optionalDependencies"]), len(self.platforms["include"]))

    def test_archives_are_deterministic(self):
        self.build()
        first = {path.name: path.read_bytes() for path in self.packages.rglob("*") if path.is_file()}
        shutil.rmtree(self.packages)
        self.build()
        second = {path.name: path.read_bytes() for path in self.packages.rglob("*") if path.is_file()}
        self.assertEqual(first, second)

    def test_an_altered_archive_or_a_foreign_executable_is_refused(self):
        self.build()
        wheel = next((self.packages / "wheels").glob("*macosx_15_0_arm64.whl"))
        original = wheel.read_bytes()
        wheel.write_bytes(original + b"\0")
        with self.assertRaises(ValueError):
            self.verify_packages()
        wheel.write_bytes(original)
        self.verify_packages()
        row = self.platforms["include"][0]
        (self.destination / row["asset"]).write_bytes(b"another build")
        with self.assertRaises(ValueError):
            self.verify_packages()

    def test_a_missing_or_extra_archive_is_refused(self):
        self.build()
        extra = self.packages / "npm" / "stray.tgz"
        extra.write_bytes(b"")
        with self.assertRaises(ValueError):
            self.verify_packages()
        extra.unlink()
        next((self.packages / "wheels").glob("*.whl")).unlink()
        with self.assertRaises(ValueError):
            self.verify_packages()

    def test_npm_publishes_platform_packages_before_the_wrapper_and_skips_what_is_live(self):
        self.build()
        published = []
        live = {packages.platform_package("darwin-arm64")}

        def output(*arguments):
            self.assertEqual(arguments[:2], ("npm", "view"))
            name, version = arguments[2].rsplit("@", 1)
            if name in live:
                return version
            raise release.subprocess.CalledProcessError(1, arguments)

        def run(*arguments, **kwargs):
            self.assertEqual(arguments[:2], ("npm", "publish"))
            self.assertIn("--access", arguments)
            members, _ = packages.read_tarball(Path(arguments[2]))
            published.append(json.loads(members["package.json"])["name"])

        with patch.object(release, "output", side_effect=output), patch.object(release, "run", side_effect=run):
            packages.publish_npm(self.packages, self.version, self.toolchain, self.platforms, self.commit, self.destination)
        self.assertEqual(published[-1], packages.NPM_WRAPPER)
        self.assertNotIn(packages.platform_package("darwin-arm64"), published)
        self.assertEqual(len(published), len(self.platforms["include"]))
