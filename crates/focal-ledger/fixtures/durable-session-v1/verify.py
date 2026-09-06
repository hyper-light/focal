"""Verify fixed original Session data and preserved source identities."""
from pathlib import Path
import hashlib
import json

root = Path(__file__).resolve().parent
manifest = json.loads((root / "manifest.json").read_text())
sources = json.loads((root / "source-files.json").read_text())
for entry in manifest["files"]:
    data = (root / entry["path"]).read_bytes()
    assert len(data) == entry["bytes"], entry["path"]
    assert hashlib.sha256(data).hexdigest() == entry["sha256"], entry["path"]
for entry in sources["files"]:
    data = (root / entry["snapshot"]).read_bytes()
    assert len(data) == entry["bytes"], entry["path"]
    assert hashlib.sha256(data).hexdigest() == entry["sha256"], entry["path"]
generator = manifest["generator"]
assert hashlib.sha256((root / generator["path"]).read_bytes()).hexdigest() == generator["sha256"]
print(f"Verified {len(manifest['files'])} outputs, {len(sources['files'])} source files, and the original generator.")
