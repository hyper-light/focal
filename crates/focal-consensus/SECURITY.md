# Raft codec boundary

The core is hyper-raft, vendored from the shared repository (`vendor/hyper-raft/SNAPSHOT`); its
messages, entries, hard states and snapshots are its own types, with typed kinds. focal's WAL and
wire hold them in raft-rs 0.7's protocol-buffer encoding, as they always have, through focal's own
codec (`src/envelope.rs`). No protocol-buffer runtime ships: neither Prost nor the Rust `protobuf`
runtime is in the shipped graph, and `raft-proto` is a test dependency only, the oracle the codec is
held to.

Every retained entry, hard state, snapshot, configuration-change payload and peer message is read by
the envelope's bounded reader: every length and varint is checked against the bytes that remain
before anything is taken, every buffer is reserved fallibly at its exact size, and a field number
outside 1 to 2^29 − 1, a known field of another wire type, a varint past ten bytes, a group (proto3
has none, and no raft-rs message holds one) and a kind the core does not name are refused before
the core sees them. `decode_message` enforces a nine MiB input limit and reports every refusal as
`MalformedMessage`. Network adapters additionally enforce framing limits before allocation and use
`step_authenticated` to bind the decoded sender to the verified certificate. Authentication does not
replace resource limits.

`src/envelope_tests.rs` holds the codec to raft-proto: 4,096 generated values of each type written to
raft-proto's own bytes exactly and read back, every prefix and one-byte mutation of generated
messages read as raft-proto reads them, and the refusals above. `src/tests.rs` keeps the fixed
original protobuf encoder fixtures (read, and written again byte for byte), actual recovery from old
protobuf snapshot/hard-state WAL records, and 100,000 nested groups. The original snapshot encoder
uses unpacked voters; the envelope writes them packed, as Prost does, and both recover to the same
typed state. Application canonical hashing is independent of the envelope.

The adapter rejects malformed append sequences, changes that do not read, and exhausted term/index
counters before invoking the core.

The core returns what it refuses and never asserts. A refusal, and a peer's
message that contradicts what the member holds, change nothing and stop no one.
An error that says the core's state no longer adds up, or that an operation
stopped half way, permanently stops the replica, as does an unwind from anything
the core depends on: construction and all mutable entry points contain unwinds
and return `DependencyFailure`. Partly assembled events are discarded. Reopening
revalidates durable state. Regression tests drive both, an error of the core and
an unwind inside the boundary, and verify failure containment and recovery of
the preceding commit. The boundary requires the workspace's unwind panic
strategy; process aborts and allocator OOM are outside Rust unwind containment.
No panic hook is replaced or globally silenced.
