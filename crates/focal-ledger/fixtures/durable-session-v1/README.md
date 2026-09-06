# Original Session storage corpus

These 170 fixed outputs (2,993,005 payload bytes) were captured before the live
Session writers, readers, membership hashes, and managed receipt/control hashes
were routed through dedicated V1 representations. They qualify the original
storage profile; they do not introduce a successor schema or activate the new
independent lifecycles.

`manifest.json` records every output's SHA-256, BLAKE3 and size, the original
linked test executable's immutable SHA-256, compiler, command and input corpus
identities. `capture-files.tsv` records the original capture's BLAKE3 values and
Rust shapes. `capture.log` records the successful original run. The exact
one-off capture source is preserved in `generator.rs.txt`; it is intentionally
absent from the executable test tree. Normal tests cannot regenerate these
expectations using the changed writers.

## Coverage and limits

Every `.rows` file is an original native Postcard `Vec<T>`; the manifest gives
its element type. The corpus covers all 24 Session storage row/envelope types:

- SS1 and SS2: empty and populated Core/cursor/receipt/history state; SS3 adds
  absent/latest membership and every membership change; SS4 adds all four
  active/cutover placement combinations; SS5 adds inactive, empty active and
  populated request registries.
- CU1, CU2, CM1 and MU1: all nine cursor operations with empty/populated
  variants, all cursor modes, offsets, filters and resynchronization reasons;
  absent and present receipt records. MS1 covers all four stream controls.
- MC1 includes AddLearner, Promote, Remove and LeaveJoint with ordinary/joint
  membership configurations. PL1 includes every fence kind crossed with all
  three failure classes, and empty/populated placement policy collections.
- Request slots include every stream state, absent/present latest control,
  all 98 original managed receipt branches, and every original control receipt
  branch. Fixed hashes pin cursor operations, membership requests, placement
  records/digests, managed acknowledgments and stream controls.

The broad `*-NN.bin` and `.rows` objects are **codec and hash vectors**, not
necessarily admissible commands or recoverable complete Sessions. They include
synthetic mismatched nested identities, maxima, empty collections and historical
representations that current admission would refuse. `membership-views.rows` is
an additional public response representation, not an extra persisted Session
codec. Broad Core/model/stream rows come from the independently captured input
corpora named and hashed in `manifest.json`.

The separate `live-*` records come from an actually admitted single-node history:
legacy epoch negotiation and cursor registration; Created/Cutover/Activated
placement; durable managed support; stream registration; a managed claim;
managed cursor acknowledgment; and a sealed stream ordinal. `live-ss3.bin`,
`live-ss4.bin` and `live-ss5.bin` are actual original `Session::encode_checkpoint`
outputs. Their `.core` and `.context` files pin resulting Core bytes and the
Raft index, term and membership configuration. `live-ss1.bin` and `live-ss2.bin`
use the retired native envelope layouts around the same actual SS3 state; the
current original writer did not select those retired tags. All five were
accepted by the actual original restore path during capture.

`reader-cu1-trailing.bin`, `reader-cu2-trailing.bin` and
`reader-cm1-trailing.bin` are deliberately suffixed copies, **not writer output**.
The actual original readers accepted them and produced the fixed
`reader-*-accepted.bin` receipts/checkpoint. Original `postcard::from_bytes`
semantics for these three metadata tags ignored the tail. All five Session
snapshot readers rejected their corresponding `reader-ssN-trailing.bin` without
publishing a Core change. MC1, PL1, MU1 and MS1 retain their original exact-body
rule; the tests exercise the actual committed readers as well as codec parity.

The ordinary `session_storage_tests.rs` tests check fixed file hashes, all
24 frozen row codecs, all twelve envelope tags, byte-limit failures, original
hash identities, actual SS3/SS4/SS5 writer parity, disk recovery and subsequent
log replay. They restore all five fixed snapshots, replay their retained deltas,
verify complete application state remains unchanged on malformed input, and
exercise the original managed-input and delta-size thresholds. This evidence
does not establish every possible reducer history, multi-node failure schedule,
or the later lifecycle migration semantics.

## Original source identity and reproduction

`source-files.json` was secured before live Session integration. All 119 recorded
source/manifest files have exact preserved copies under `source-snapshot/`;
each copy's SHA-256 matches its recorded original identity. This includes the
original Session serialization/restore paths and the referenced Core, stream,
model, directory and consensus representation sources. Native managed hash
sources were recovered by matching their original hash against the independent
model capture's source archive after that agent switched the live functions.
The recorded source snapshot predates the one-off `cfg(test)` capture hook.
Some newly added but unused dedicated model codecs were hooked while capture
was compiling; the native Session writers and native model serializers/hash
functions stayed unchanged until the capture passed.

The immutable identity of the actual linked capture executable is
`768c51926790546e97ac0036b5e7cd519b93b5eadb56c6f70c79b0969d9c301f` (SHA-256).
The repository intentionally stores no executable. Its temporary path and the
original Cargo artifact paths are provenance information, not a promise that
those mutable files remain available. The source identity manifest's rlib
hashes were measured before the capture build; they are not falsely identified
as the complete dependency set of that final linked executable.

For an explicit forensic recapture, use a separate checkout compatible with the
recorded original workspace, restore the original files from `source-snapshot`
by removing their `.txt` suffix, and restore the input fixture corpora by the
hashes in `manifest.json`. Copy `generator.rs.txt` to
`crates/focal-ledger/src/session_original_capture.rs`, and add this module inside
the original `session::tests` module:

```rust
mod original_session_capture {
    use super::*;
    include!("session_original_capture.rs");
}
```

Then run the recorded command with `FOCAL_SESSION_CAPTURE_DIR` pointing to a
new directory that does not exist. The generator refuses overwriting any
output. Compare all 170 lengths and hashes before considering the result a
reproduction. Do not replace golden files as part of ordinary qualification.
The snapshots cover the serialization graph and its original caller source;
they are not a complete independent archive of the entire workspace/toolchain.
Rebuilding also requires the compatible workspace's remaining crates and locked
dependencies. The fixed bytes and immutable executable hash remain the original
capture evidence if that full build environment is unavailable.

Run `python3 verify.py` from this directory to verify all output and preserved
source SHA-256 values using only Python's standard library. Ordinary Rust tests
also check every captured BLAKE3 value.
