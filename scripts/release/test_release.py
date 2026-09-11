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

import release


class ReleaseTests(unittest.TestCase):
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
        self.assertEqual(len(names), 8)
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
        with self.assertRaises(SystemExit):
            release.pe_imported_dlls(b"\x7fELF" + b"\0" * 60)


if __name__ == "__main__":
    unittest.main()
