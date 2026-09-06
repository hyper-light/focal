# Original V1 commands, authenticated inputs and canonical hashes

This fixed corpus was captured on 2026-09-06 from the original compiled command,
input and canonical identity implementations, after the nested checkpoint codec
was integrated and before command/input/canonical codec integration. Ordinary
builds and tests must not execute the generator or regenerate these files.

These are **synthetic codec and hash vectors**. The generator constructs prepared
structs directly; it does not call reducer admission or commit commands. They
include newly inadmissible validation specifications, missing or inconsistent
object relationships, false authority attestations, zero historical counters,
and nonexclusive footprints. An encoded prepared envelope is not evidence that
an operation was authorized. Use the unchanged [durable-v1 corpus](../durable-v1/README.md)
for its separate admitted workflow and replay qualification.

## File types and ordering

All `.rows` files below are original Postcard serialization. Their vectors have
58 elements in the same order: indexes 0–28 contain every original `Command`
variant, in canonical tag order 1–29, with representative populated fields;
indexes 29–57 repeat those variants with empty or alternate branch fields.
Postcard command discriminants remain 0–28, distinct from canonical tags 1–29.

| File | Exact serialized type or contents |
|---|---|
| `commands.rows` | `Vec<Command>` |
| `command-00.bin` through `command-57.bin` | Individual original `Command` values |
| `legacy-inputs.rows` | `Vec<AuthenticatedInput>` |
| `managed-inputs.rows` | `Vec<ManagedAuthenticatedInput>` |
| `canonical-tags.rows` | `Vec<u16>`: original `Command::code()` results |
| `canonical-bodies.rows` | `Vec<Vec<u8>>`: each inner byte string is original Postcard `(Option<ObjectRevision>, Command)` |
| `command-hashes.rows` | `Vec<ContentHash>`: original `command_hash` results |
| `managed-command-hashes.rows` | `Vec<ContentHash>`: original `managed_command_hash` results |
| `legacy-prepared.rows` | `Vec<PreparedMutation>` |
| `managed-prepared.rows` | `Vec<PreparedManagedMutation>` |
| `legacy-prepared-bodies.rows` | `Vec<Vec<u8>>`: each inner byte string is one original `PreparedMutation` body |
| `managed-prepared-bodies.rows` | `Vec<Vec<u8>>`: each inner byte string is one original `PreparedManagedMutation` body |
| `legacy-entries.rows` | `Vec<Vec<u8>>`: each inner value is `FOCALOP1` followed by its prepared body |
| `managed-entries.rows` | `Vec<Vec<u8>>`: each inner value is `FOCALMD1` followed by its prepared body |

The 71 fixed data files total 576,697 bytes. [manifest.json](manifest.json) pins
their exact byte lengths and SHA-256 hashes. [coverage.json](coverage.json)
records the command order and branch coverage.

The populated `NewClaim` carries all 42 original validation kind × phase × mode
combinations from the previously captured
[`durable-v1-nested/broad.cp1`](../durable-v1-nested/broad.cp1). This includes
absent/present quality bars, empty/programmatic/programmatic-and-agentic handler
lists, empty/nonempty evidence schemas and contributors. The other claim has
empty validations, requirements, relations, scopes and description. Artifact
commands contain either empty inline bytes or a content reference whose length
is `u64::MAX`; that length remains scalar metadata. Manifests preserve unsorted
vector order. Monitor roots cover all three predicates or an empty set.

Inputs include both causes and runtime flags; absent/present expected revisions;
empty/nonempty authority evidence and all four durable/schema-valid combinations;
zero and maximum policy/custody revisions; high-bit IDs; UTF-8, newline and NUL
text; managed slots zero/maximum and generations one/maximum; and integer varint
boundaries. Both optional receipt-fence branches are represented. This input
corpus does not claim every possible combination of nested enum values; the
separate checkpoint corpus covers the complete reachable checkpoint graph.

## What the original writer verified

The preserved [generator.rs.txt](generator.rs.txt) was compiled with standalone
`rustc` against the original matched Core/model/Serde/Postcard/Blake3 rlibs, with
no intervening Cargo rebuild. For all 58 rows it verified:

1. The original command tags are 1–29 in the declared order.
2. The original legacy and streaming managed canonical hashes agree.
3. Independently assembling the historical canonical preimage agrees with both
   implementations: `focal.command\0`, big-endian schema `1`, ledger tenant and
   session bytes, principal bytes, big-endian command tag, big-endian u32 body
   length, then original Postcard `(expected_revision, command)` bytes.
4. Original public prepared `encode_v1` and `decode_v1` match direct original
   prepared Serde bytes and values for both legacy and managed inputs.

The entry magics are added to those verified original prepared bodies by the
fixture utility, following the original Session layout. The generator does not
call Session replay, a WAL writer, managed stream admission, or artifact custody.

## Provenance and manual reproduction

[capture.json](capture.json) records the exact original linked executable hash,
generator source hash, compiler, compilation arguments, source checkpoint hash,
and matched original rlib **content hashes**. Original build filenames alone are
not used as identities. [original-rlibs.json](original-rlibs.json) records the
available candidate rlib content identities captured before compilation; only
the matched subset in `capture.json` was linked into the successful generator.
No executable or rlib binary is stored in this directory.

[source-manifest.json](source-manifest.json) identifies the Git base and 63 exact
pre-increment source/Cargo files. Their verbatim contents are preserved as `.txt`
files under `source-snapshot/`; the snapshot includes Core, model and ledger
sources, plus the workspace lockfile. These preserve the writer source even if
the workspace's uncommitted changes or temporary files are later removed. They
are evidence files, excluded from compiled Rust paths. The source snapshot was
secured before the new command/input codec modules were integrated; unrelated
checkpoint codecs had already been integrated.

For manual reproduction, restore the recorded files over an isolated checkout
of the recorded Git base and use its captured lockfile. Build the original
Core/model libraries, then compile the preserved generator with matched Core,
model, Serde, Postcard and Blake3 rlibs. The original capture used Rust 1.94.1 on
macOS arm64. When compiling the `.rs.txt` filename, pass an explicit crate name,
for example `--crate-name focal_v1_inputs_capture`, together with `--edition=2024`.
Run from the repository root so the generator can read the previously captured
nested checkpoint. The generator refuses to overwrite its fixed temporary output
directory `/private/tmp/focal-v1-inputs-original`. Compare output bytes with the
manifest; do not regenerate checked-in fixtures to make a failing test pass.
Exact executable hashes may differ when reconstructing with different build
paths or compiler flags; the persisted byte vectors are the compatibility target.

## Qualification boundary

Compare frozen borrowed serializers with these exact original bytes, decode
through frozen owned wrappers, and compare actual prepared entry-point results.
Check both canonical hash implementations against the fixed expected hashes.
Authority context, ingress logical time, request IDs and legacy/managed request
scope remain excluded from the command intent hash; tests should also mutate
those fields independently to verify the existing exclusions.

Truncation, unknown tags/ordinals, invalid booleans and trailing bytes need
separate malformed-input tests. Collection visitors must not allocate from an
untrusted declared size, and content-reference lengths must never allocate a
payload. Preserve historical vector order and collection duplicate handling.
These vectors do not freeze the historical reducer, managed registry/cursor
protocols, emitted delta/effect identities, or enable successor lifecycle semantics.
