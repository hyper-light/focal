"""Actual old executable refusal; only operates on fresh copies under --output.

Helper commands are JSON arrays keyed by install-predecessor, transition,
compact-same, compact-other. Each array may use {data} as a whole or partial
argument. The helper must use the real new DurableNode floor/checkpoint APIs;
this script deliberately does not manufacture WAL frames or mutate checksums.
"""
from pathlib import Path
import argparse
import hashlib
import json
import shutil
import struct
import subprocess
import zlib

OLD_HASH = "da54f8b8be83766555f72397ecf3a8a1bfae970995fa388b6285296e1260c4d4"


def sha(data):
    return hashlib.sha256(data).hexdigest()


def files(root):
    return {str(path.relative_to(root)): {"size": path.stat().st_size,
            "sha256": sha(path.read_bytes())}
            for path in sorted(root.rglob("*")) if path.is_file()}


def integer(data, offset):
    value = 0
    for shift in range(0, 70, 7):
        assert offset < len(data), "truncated varint"
        byte = data[offset]
        offset += 1
        value |= (byte & 127) << shift
        if byte < 128:
            assert value <= (1 << 64) - 1
            return value, offset
    raise AssertionError("oversized varint")


def wal(root):
    """Independently check CURRENT, segment and frame CRCs; preserve all kinds."""
    folder = root / "wal"
    raw = (folder / "CURRENT").read_bytes()
    assert raw[:8] == b"FOCALF01"
    body = raw[8:-4]
    assert zlib.crc32(body) == struct.unpack("<I", raw[-4:])[0]
    version, offset = integer(body, 0)
    assert version == 1
    cluster = body[offset:offset + 16]
    assert len(cluster) == 16
    offset += 16
    values = {}
    for key in ("node", "stream", "generation", "segment", "byte", "sequence", "checksum"):
        values[key], offset = integer(body, offset)
    assert offset == len(body)
    sequence, previous = 0, 0
    rows = []
    for segment in range(values["segment"] + 1):
        path = folder / f"wal-{values['generation']:020}-{segment:020}.seg"
        data = path.read_bytes()
        header = data[:72]
        assert len(header) == 72 and header[:8] == b"FOCALW01"
        assert zlib.crc32(header[:68]) == struct.unpack_from("<I", header, 68)[0]
        assert struct.unpack_from("<I", header, 8)[0] == 1
        assert header[12:28] == cluster
        assert struct.unpack_from("<Q", header, 28)[0] == values["node"]
        assert struct.unpack_from("<I", header, 36)[0] == values["stream"]
        assert struct.unpack_from("<QQQ", header, 40) == (values["generation"], segment, sequence)
        assert struct.unpack_from("<I", header, 64)[0] == previous
        end = values["byte"] if segment == values["segment"] else len(data)
        assert 72 <= end <= len(data)
        cursor = 72
        while cursor < end:
            length, seq, prior, crc = struct.unpack_from("<IQII", data, cursor)
            raw = data[cursor + 20:cursor + 20 + length]
            assert len(raw) == length and cursor + 20 + length <= end
            assert seq == sequence + 1 and prior == previous
            assert zlib.crc32(data[cursor:cursor + 16] + raw) == crc
            assert len(raw) >= 16
            group, at = raw[:16].hex(), 16
            kind, at = integer(raw, at)
            index, at = integer(raw, at)
            term, at = integer(raw, at)
            payload_length, at = integer(raw, at)
            payload = raw[at:]
            assert len(payload) == payload_length
            rows.append({"group": group, "kind": kind, "index": index,
                         "term": term, "payload_bytes": len(payload),
                         "payload_sha256": sha(payload), "record_sha256": sha(raw),
                         "frame_offset": cursor, "frame_sequence": seq,
                         "payload_prefix_hex": payload[:8].hex()})
            sequence, previous = seq, crc
            cursor += length + 20
        assert cursor == end
    assert sequence == values["sequence"] and previous == values["checksum"]
    return {"cluster": cluster.hex(), **values, "records": rows}


def run(command, name, output, expect_success=True):
    result = subprocess.run(command, capture_output=True, text=True, timeout=45)
    (output / f"{name}.stdout").write_text(result.stdout)
    (output / f"{name}.stderr").write_text(result.stderr)
    evidence = {"command": command, "returncode": result.returncode,
                "stdout_sha256": sha(result.stdout.encode()),
                "stderr_sha256": sha(result.stderr.encode())}
    (output / f"{name}.command.json").write_text(json.dumps(evidence, indent=2) + "\n")
    if expect_success:
        assert result.returncode == 0, f"{name}: {result.stderr}"
    return result


def refuse(old, branch, name, output):
    before = files(branch)
    before_wal = wal(branch)
    for attempt in range(2):
        result = run([str(old), "--data-dir", str(branch), "demo"],
                     f"{name}-old-refusal-{attempt}", output, False)
        assert result.returncode > 0, f"{name}: old executable succeeded or was signaled"
        assert "invalid durable record" in result.stderr, result.stderr
        assert not result.stdout.strip(), "old executable published a demo report"
        assert "panicked" not in result.stderr
        assert files(branch) == before, "old refusal changed directory file content/names"
        assert wal(branch) == before_wal, "old refusal changed durable WAL position"
    (output / f"{name}-unchanged.json").write_text(json.dumps(before, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--old", type=Path, default=Path("/private/tmp/focal-before-execution-v1/focal"))
    parser.add_argument("--baseline", type=Path, default=Path(__file__).parent / "baseline")
    parser.add_argument("--commands", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    assert sha(args.old.read_bytes()) == OLD_HASH
    assert not args.output.exists(), "output must be a new directory"
    assert args.baseline.is_dir()
    baseline_before = files(args.baseline)
    original = wal(args.baseline)
    groups = {row["group"] for row in original["records"]}
    assert len(groups) == 1, "baseline is the real single-ledger demo"
    group = next(iter(groups))
    args.output.mkdir()
    commands = json.loads(args.commands.read_text())

    def helper(mode, data):
        command = [part.replace("{data}", str(data)) for part in commands[mode]]
        run(command, mode, args.output)
        return wal(data)

    positive = args.output / "old-positive-control"
    shutil.copytree(args.baseline, positive)
    run([str(args.old), "--data-dir", str(positive), "demo"], "old-positive", args.output)

    primed = args.output / "primed"
    shutil.copytree(args.baseline, primed)
    prime = helper("install-predecessor", primed)
    assert any(row["kind"] == 6 and row["group"] == group for row in prime["records"])
    assert not any(row["kind"] == 7 for row in prime["records"])
    old_prime = args.output / "old-predecessor-control"
    shutil.copytree(primed, old_prime)
    run([str(args.old), "--data-dir", str(old_prime), "demo"], "old-predecessor-positive", args.output)

    transition = args.output / "transition-only"
    shutil.copytree(primed, transition)
    transitioned = helper("transition", transition)
    assert transitioned["generation"] == prime["generation"], "transition unexpectedly compacted"
    # A one-hop physical transition appends exactly one new ordinal-7 record;
    # no application, Raft entry, or snapshot bytes are introduced or changed.
    assert [row["record_sha256"] for row in transitioned["records"][:-1]] == [row["record_sha256"] for row in prime["records"]]
    marker = transitioned["records"][-1]
    assert marker["kind"] == 7 and marker["group"] == group
    assert marker["index"] == marker["term"] == 0
    refuse(args.old, transition, "transition", args.output)

    snapshot_before = [row["payload_sha256"] for row in transitioned["records"] if row["group"] == group and row["kind"] == 3]
    assert snapshot_before, "must retain the original real Session checkpoint"
    results = {"prime": prime, "transition": transitioned}
    for mode in ("compact-same", "compact-other"):
        data = args.output / mode
        shutil.copytree(transition, data)
        compacted = helper(mode, data)
        assert compacted["generation"] > transitioned["generation"], "no physical checkpoint rewrite happened"
        markers = [row for row in compacted["records"] if row["group"] == group and row["kind"] == 7]
        assert len(markers) == 1 and markers[0]["record_sha256"] == marker["record_sha256"]
        assert [row["payload_sha256"] for row in compacted["records"] if row["group"] == group and row["kind"] == 3] == snapshot_before
        refuse(args.old, data, mode, args.output)
        results[mode] = compacted
    assert files(args.baseline) == baseline_before
    (args.output / "result.json").write_text(json.dumps({"old_sha256": OLD_HASH, "baseline": str(args.baseline), "group": group, "wal": results, "result": "all old executable refusal and immutability checks passed"}, indent=2) + "\n")
    print(args.output / "result.json")

if __name__ == "__main__":
    main()
