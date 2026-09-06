"""Compile only from the exact successful Cargo artifact graph supplied by root."""
from pathlib import Path
import argparse
import datetime
import hashlib
import json
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument("--artifacts", type=Path, required=True)
args = parser.parse_args()
base = Path(__file__).resolve().parent
artifacts = [json.loads(line) for line in args.artifacts.read_text().splitlines() if line.startswith("{")]
assert any(row.get("reason") == "build-finished" and row.get("success") is True for row in artifacts)
externs = {}
rlibs = []
native = set()
for row in artifacts:
    if row.get("reason") == "build-script-executed":
        native.update(row.get("linked_paths", []))
    if row.get("reason") != "compiler-artifact":
        continue
    for name in row.get("filenames", []):
        path = Path(name)
        if path.suffix != ".rlib":
            continue
        data = path.read_bytes()
        rlibs.append({"path": str(path), "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest(),
                      "package_id": row["package_id"], "target": row["target"]["name"],
                      "fresh": row["fresh"]})
        crate = row["target"]["name"]
        if crate in ("focal_node", "focal_ledger", "focal_log", "focal_consensus"):
            assert crate not in externs or externs[crate] == path
            externs[crate] = path
assert len(externs) == 4
folder = externs["focal_consensus"].parent
assert all(path.parent == folder for name, path in externs.items() if name != "focal_node")
assert externs["focal_node"].parent in (folder, folder.parent)
command = ["rustc", "--edition=2024", str(base / "helper.rs"), "-L", f"dependency={folder}"]
for name, path in sorted(externs.items()):
    command.extend(["--extern", f"{name}={path}"])
for path in sorted(native):
    command.extend(["-L", path])
command.extend(["-o", str(base / "helper")])
assert not (base / "helper").exists(), "refusing to overwrite an experiment executable"
result = subprocess.run(command, capture_output=True, text=True, timeout=60)
(base / "helper-build.stdout").write_text(result.stdout)
(base / "helper-build.stderr").write_text(result.stderr)
source = (base / "helper.rs").read_bytes()
record = {"time": datetime.datetime.now(datetime.timezone.utc).isoformat(), "command": command,
          "returncode": result.returncode, "rustc": subprocess.check_output(["rustc", "--version", "--verbose"], text=True),
          "source_sha256": hashlib.sha256(source).hexdigest(), "rlibs": rlibs,
          "artifact_stream_sha256": hashlib.sha256(args.artifacts.read_bytes()).hexdigest()}
if result.returncode == 0:
    record["executable_sha256"] = hashlib.sha256((base / "helper").read_bytes()).hexdigest()
(base / "helper-build.json").write_text(json.dumps(record, indent=2) + "\n")
(base / "cargo-build-artifacts.jsonl").write_bytes(args.artifacts.read_bytes())
print(result.stderr)
assert result.returncode == 0, "helper compilation failed; inspect helper-build.stderr"
print(base / "helper")
