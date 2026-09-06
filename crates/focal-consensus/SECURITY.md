# Raft codec boundary

The adapter selects only `raft/prost-codec` on the [audited immutable upstream revision](../../docs/dependencies/raft-upstream.md). Native upstream `protocompat` delegates decoding to Prost. The affected Rust `protobuf` 2.28 runtime and unmaintained `fxhash` are removed from the workspace graph; no advisory is suppressed. Build-only `protobuf-src` C++ compiler sources are a separate dependency included in the inventory.

Every retained entry, hard state, snapshot, configuration-change payload, and peer message uses bounded `merge_from_bytes` decoding. `decode_message` enforces a nine MiB input limit. Network adapters additionally enforce framing limits before allocation and use `step_authenticated` to bind the decoded sender to the verified certificate. Authentication does not replace resource limits.

Tests exercise 100,000 nested unknown groups, fixed original protobuf encoder fixtures, and actual recovery from old protobuf snapshot/hard-state WAL records. The original snapshot encoder uses unpacked voters; Prost may write packed voters, and both recover to the same typed state. Application canonical hashing is independent of protobuf serialization.

The adapter rejects malformed append sequences, unknown message/entry enum values,
and exhausted term/index counters before invoking Raft. Upstream Raft still uses
assertions for internal invariants. Construction and all mutable entry points
contain unwinds, return `DependencyFailure`, and permanently stop the replica;
partly assembled events are discarded. Reopening revalidates durable state. A
regression test triggers an actual upstream commit-index assertion and verifies
failure containment and recovery of the preceding commit. This requires the
workspace's unwind panic strategy; process aborts and allocator OOM are outside
Rust unwind containment. No panic hook is replaced or globally silenced.
