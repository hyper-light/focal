# Raft codec boundary

The wire and log types are those of `raft-proto` with `prost-codec` alone, on the [audited immutable upstream revision](../../docs/dependencies/raft-upstream.md); the core that uses them is `focal-raft`, and `raft` itself is a test dependency of that crate. Native upstream `protocompat` delegates decoding to Prost. The affected Rust `protobuf` 2.28 runtime and unmaintained `fxhash` are removed from the workspace graph; no advisory is suppressed. Build-only `protobuf-src` C++ compiler sources are a separate dependency included in the inventory.

Every retained entry, hard state, snapshot, configuration-change payload, and peer message uses bounded `merge_from_bytes` decoding. `decode_message` enforces a nine MiB input limit. Network adapters additionally enforce framing limits before allocation and use `step_authenticated` to bind the decoded sender to the verified certificate. Authentication does not replace resource limits.

Tests exercise 100,000 nested unknown groups, fixed original protobuf encoder fixtures, and actual recovery from old protobuf snapshot/hard-state WAL records. The original snapshot encoder uses unpacked voters; Prost may write packed voters, and both recover to the same typed state. Application canonical hashing is independent of protobuf serialization.

The adapter rejects malformed append sequences, unknown message, entry,
configuration-change and transition enum values, changes that do not decode, and
exhausted term/index counters before invoking the core. The accessors the wire
types generate for their enumerations unwind on a value they do not know; they
are forbidden in production by lint (`clippy.toml`, `disallowed-methods`), and
the core reads those values as options.

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
