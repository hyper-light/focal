# Original native cursor V1 corpus

Captured before adding `focal-stream` V1 codecs. The generator calls the original
public types' Serde implementations through `postcard::to_stdvec`; it never calls
a V1 adapter. `capture.json` records compiler commands, SHA-256 build identities,
source identities, individual file sizes and hashes. `source-files.json` and
`source-snapshot/` preserve the actual native crate sources compiled for capture.
The model ID/counter/hash leaf source used by these native values is also saved;
it was checked against the earlier original model-source capture.

The capture compiled the unmodified native crate directly with standalone `rustc`,
then linked its generator against that library and the recorded model, memory,
Serde and Postcard release libraries. No Cargo process or newly added codec was
used. Build filenames are mutable local paths; the saved SHA-256 identities name
the exact capture build. `generator.rs.txt` is the complete generator source.

Every `.rows` file is an original Postcard `Vec<T>`:

| File stem | Type | Rows |
|---|---|---:|
| `consumer-ids` | `ConsumerId` | 4 |
| `consumer-keys` | `ConsumerKey` | 4 |
| `position-offsets` | `PositionOffset` | 5 |
| `positions` | `Position` | 50 |
| `cursor-tokens` | `CursorToken` | 50 |
| `delta-filters` | `DeltaFilter` | 4 |
| `resync-reasons` | `ResyncReason` | 5 |
| `cursor-modes` | `CursorMode` | 9 |
| `cursor-records` | `CursorRecord` | 36 |
| `cursor-checkpoints` | `CursorCheckpoint` | 3 |
| `cursor-commands` | `CursorCommand` | 18 |
| `cursor-operations` | `CursorOperation` | 18 |

`operation-00.bin` through `operation-08.bin` are the nine original variants in
ordinal order with populated values. `operation-09.bin` through `operation-17.bin`
repeat that order with empty/zero branches. `checkpoint-00.bin` is empty,
`checkpoint-01.bin` contains one valid live consumer, and `checkpoint-02.bin`
combines every mode/filter branch with extreme metadata.

Cases include zero/high-bit/maximum IDs; ordinal and sequence varint boundaries;
Delta versus Resolved; zero and maximum generations; every resync reason; live,
seeding, resync and protected modes; empty and sorted 128-claim filters; and sorted
consumer maps. Some cases intentionally violate registry admission: sequence zero
with a Delta offset, mismatched token ledgers, zero generations, unusual lease
metadata and an unsupported checkpoint schema. They prove serialization fidelity,
not historical admission or permission to restore such a checkpoint.

The two `unsorted-duplicate-*.input` files are explicitly **crafted reader probes**,
not alleged BTreeSet/BTreeMap writer output. The original decoder read them and the
original writer produced each `.normalized` companion. They pin native filter
sorting/de-duplication and map ordering/last-duplicate-wins semantics. Managed
cursor filters use a different Vec representation and are not covered here.

For reproduction, restore the saved native source paths by removing the trailing
`.txt` suffix in a temporary source tree, use the compiler/dependency arguments
in `capture.json` with the recorded library identities, and run the generator
into a fresh temporary output directory. Compare all 37 generated data files
byte-for-byte and verify their recorded hashes. The generator refuses to overwrite
existing outputs. Do not regenerate expected bytes from the new V1 implementation.

These fixtures freeze native cursor shapes only. They do not claim to capture or
freeze Session envelopes, Ledger cursor receipts/metadata, consumer-local
checkpoints, managed cursor snapshots or transport event encoding. The embedding
Session must separately select its historical format, fund recovery, and reject
trailing body bytes before publishing decoded state.
