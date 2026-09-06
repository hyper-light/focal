# Original V1 codec fixtures

These bytes were captured on 2026-09-06 from the existing compiled Core library,
before introducing `durable_v1.rs`. The generator used the original
`Core::encode_checkpoint` and direct Postcard serialization of prepared entries
and apply results. Tests consume checked-in bytes; they do not regenerate them.
`manifest.json` records their sizes and SHA-256 digests for review.

The base has three admitted participant epochs. Entries 00–08 alternate legacy
`FOCALOP1` and managed `FOCALMD1`: generate, post, acquire receipt, open evidence,
attach immutable evidence, close testament, acknowledge, begin whole-work
validation, complete. Each `.result` preserves the actual receipt/outcome, deltas
and effects, including their original hashes and sequences. Managed results here
are Core results; Session's managed stream registry remains its separate existing
receipt owner.

The open-evidence checkpoint contains a durably registered artifact in an open
set with no testament. The closed checkpoint contains that same artifact and the
exact closed manifest. The codec must not manufacture missing artifact lifecycle
events from either state. The final claim is satisfied by its receipt requirement.

This corpus qualifies the exercised existing shapes. It does not cover all
command variants, failed/agentic checks or all historical Session snapshots, and
does not independently freeze nested model DTOs or the V1 reducer. Those remain
required before introducing successor object or execution semantics.

## Capture provenance

[`generator.rs.txt`](generator.rs.txt) is the exact 5,101-byte capture source,
preserved without formatting changes; its SHA-256 is in
[`capture.json`](capture.json). It is a manual fixture utility, excluded from
ordinary builds and tests. Its hardcoded destination is
`/private/tmp/focal-v1-golden`; do not point it at these checked-in fixtures.

The `original_linked_writer.sha256` value identifies the **actual standalone
executable linked to the pre-change Core/model libraries** that produced this
corpus. Its executable bytes are intentionally not committed. The recorded rlib
filenames locate what was linked; their mutable build paths are not immutable
content identities. Original rlib contents were not separately hashed at capture,
so neither those filenames nor a later rebuild proves identical compiler output.

The pre-change source can be reconstructed from Git base
`31198c225b5ab6fb7c0414fe423d069701b9c9a2` plus
[`writer-source.patch`](writer-source.patch). That patch retains preexisting Core
and dependency-lock changes. The original `focal-core/src/lib.rs` matches the
base: only the later codec delegation changed it. Other Core/model files were
unchanged by the codec tranche. [`writer-source-manifest.json`](writer-source-manifest.json)
records the resulting source bytes and SHA-256 digests. This reconstruction was
made after capture and is labeled accordingly; it does not claim an independently
reproducible binary build or freeze those nested model types.

The original checkpoint writer was exactly:

```rust
let payload = postcard::to_allocvec(&(SCHEMA_MAJOR, self))?;
let mut bytes = b"FOCALCP1".to_vec();
bytes.extend_from_slice(blake3::hash(&payload).as_bytes());
bytes.extend_from_slice(&payload);
Ok(bytes)
```

Here `SCHEMA_MAJOR` is 1 and `self` is the derived-Serde Core containing State
followed by Limits. Prepared entry bodies and expected results use direct
`postcard::to_stdvec`; the generator adds the existing `FOCALOP1`/`FOCALMD1`
entry magic. It never calls the newly introduced codec helpers.

## Independent byte checks and manual reproduction

To check the checked-in corpus without compiling or generating anything, run
this from the repository root:

```sh
python3 - <<'PY'
import hashlib, json
from pathlib import Path
root = Path('crates/focal-core/fixtures/durable-v1')
for name, expected in json.loads((root / 'manifest.json').read_text()).items():
    data = (root / name).read_bytes()
    assert len(data) == expected['bytes'], name
    assert hashlib.sha256(data).hexdigest() == expected['sha256'], name
capture = json.loads((root / 'capture.json').read_text())
assert hashlib.sha256((root / capture['generator_source_path']).read_bytes()).hexdigest() == capture['generator_source_sha256']
print('All 23 original fixture files and exact generator source match.')
PY
```

For manual reproduction, use an isolated checkout/copy at the recorded Git base,
apply `writer-source.patch` there, and verify its Core/model/source bytes against
`writer-source-manifest.json`. Build the original `focal-core` with its recorded
lockfile. Compile `generator.rs.txt` as a standalone Rust program with
`--edition=2024 --crate-name focal_v1_capture`, `-L dependency=...`, and matching
`--extern focal_core=...`, `--extern focal_model=...`, `--extern postcard=...`
rlibs from that same build. Do not mix artifacts from different feature builds.
The original capture used Rust 1.94.1 on macOS arm64; differing compiler output
does not imply differing domain bytes.

Run that program only after ensuring its temporary destination contains no work
to preserve. Compare every generated `.cp1`, `.entry` and `.result` file to the
matching checked-in file or its manifest digest. Never refresh the checked-in
corpus automatically to make a test pass. A difference needs investigation into
the source, canonical encoding, replay or toolchain; a current writer cannot
retroactively redefine these historical fixtures.
