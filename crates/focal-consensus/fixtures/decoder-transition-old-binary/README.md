# Actual old executable refusal after a decoder transition

The preserved **actual old `focal` executable** refused every transitioned data
copy before and after physical compaction. All six failed startups left every
file name, length and SHA-256 unchanged. Both old-executable positive controls
succeeded. This was not an enum-only reconstruction or a fabricated checksum
failure.

The original executable has SHA-256
`da54f8b8be83766555f72397ecf3a8a1bfae970995fa388b6285296e1260c4d4`.
`old-binary-capture.json` records its original provenance and explicitly states
that its exact complete build source was not reconstructed. No executables,
libraries, keys or temporary data directories are stored here.

## Observed results

A fresh directory was initialized with the old binary's actual
`focal --data-dir <directory> demo`. It completed the real claim, artifact and
validation history through domain sequence13 and checkpointed before exiting.
`baseline-demo.stdout`, `identity.json` and `baseline.json` preserve that result
and the original file hashes. The physical WAL initially contained Identity,
Snapshot and HardState for the real group, at Raft index14/term1. The Session
snapshot was never replaced with synthetic application state.

The helper used actual consensus APIs to install the released
`Session::managed_decoder_hash()` predecessor and then an explicitly **test-only
opaque successor**. It did not manufacture WAL records or repair checksums.

| Branch | Physical generation | Record ordinals | Old CLI outcome |
| --- | --- | --- | --- |
| Unchanged baseline copy | 2 before open | 4, 3, 1 | Exit0 |
| Durable predecessor copy | 2 before open | 4, 3, 1, 6 | Exit0 |
| Transition only | 2 | 4, 3, 1, 6, 7 | Exit1 twice, unchanged files |
| Same-group checkpoint | 3 | 4, 6, 7, 3, 1 | Exit1 twice, unchanged files |
| Other-group checkpoint | 3 | 4, 3, 1, 6, 7, 4, 3, 1 | Exit1 twice, unchanged files |

Ordinal6 is the original floor; ordinal7 is the new transition. The latter is
exactly `FOCALDT1` + big-endian u16 schema1 + predecessor32 + successor32, with
record index0/term0. The transition-only operation appended exactly that one
record, leaving every earlier record unchanged. Both checkpoint paths retained
its identical record bytes and the original group's identical Snapshot payload.

Each old startup failed with `invalid durable record` during physical WAL scan.
The runner required that specific diagnostic, a normal nonzero exit, no demo
report, and no panic; a timeout, signal, missing file, identity error or decoder
hash mismatch could not count as success. It checked the complete file set,
lengths and hashes before and after both attempts, plus CURRENT and independently
validated segment/frame checksums and ordering. File access timestamps were not
classified as durable state changes. The baseline itself remained unchanged.

The one-off helper compiler and experiment runner both exited0. The compiled
helper SHA-256 is
`fd83c77312cf98686013f90b7014d1ff0193dce8d4bc3c40cda6cc5cfdd3a558`.
Its exact source SHA-256 is
`5b1496a8de91f3b3230a35669e942eb616d21b31e5fe7460ef0a7d96beb51d6e`.

## Reviewable evidence

- `results/result.json` contains the independently checked physical WAL records
  for the predecessor, transition and both rewritten branches.
- `results/*.command.json`, `.stdout` and `.stderr` record every executed argv,
  return code and diagnostic. `*-unchanged.json` contains the file manifest that
  matched before and after both rejected startups for that branch.
- `helper.rs.txt`, `experiment.py` and `compile_helper.py` are the exact successful
  helper and orchestration sources. The runner only works on new copied branches
  and refuses to reuse an existing output directory.
- `helper-build.json` records the actual structured compiler argv, compiler
  version, source/executable hashes, all Cargo-selected rlib identities, and the
  successful artifact-stream hash. No arbitrary rlib filename was selected.
- `transition-source.json` and `transition-source/` preserve the implementation
  inspected immediately after the successful experiment. `source-audit.json`
  and `observed-source/` separately preserve sources inspected while planning;
  neither is falsely attributed to the old executable's exact build tree.

`evidence-sha256.json` pins every retained evidence file. Verify it from the
repository root using this standard-library-only command:

```sh
python3 -B crates/focal-consensus/fixtures/decoder-transition-old-binary/verify.py
```

## Reproduction and limits

This is a recorded manual compatibility experiment, not an ordinary test that
requires an unavailable old executable or generates new expected bytes. To
repeat it, obtain the old executable by the exact hash, initialize a new baseline
with its offline `demo` command, and build a compatible new focal-node library
graph with Cargo's JSON artifact output. Copy the helper source to `helper.rs`
in a separate temporary working directory alongside both Python scripts. Run
`compile_helper.py --artifacts <successful-jsonl>`, create command templates from
`commands.example.json`, then run `experiment.py --old <old-binary> --baseline
<fresh-baseline> --commands <templates> --output <new-output-directory>`.
The standalone compiler records all selected artifacts and never invokes Cargo.

`identity` is unsuitable for the refusal check because it deliberately does not
open the WAL. `demo` opens the real owner and only campaigns after successful WAL
recovery, so it exercises refusal without adding service sockets.

The synthetic successor is confined to the helper. The actual Focal Session
still composes V1; this evidence does **not** claim a successor lifecycle reducer,
wire format, CLI/MCP operation, replicated activation gate or cluster-wide
upgrade protocol. It establishes the local physical old-reader exclusion and
retention properties of the bounded one-hop transition.
