# focal-memory

Safe Rust storage primitives for a single mutable range owner with immutable
readers. This crate performs no IO, reads no wall clock, and has no dependency on
an embedded database or an async runtime.

- `Arena<T>` uses fixed-capacity slot pages, explicit `ArenaId` provenance,
  generations, checked slot limits, and permanent retirement on generation
  exhaustion. `StableIndex<K,T>` maps stable ordered keys to local typed handles.
- `MemoryBudget` reserves bytes before allocation with an ordinary allowance
  and a completion/control reserve. Reservations roll back on drop; committed
  allocation charges follow the resource lifetime. Categories cover pages,
  indexes, pending writes, reads, monitors, deduplication, timers and recovery.
- `ImmutableContent<C>` accounts authored data once. `VersionedRecord<C,L>`
  shares that content across independent lifecycle versions.
- `RangeStore<K,V>` stores ordered entries in immutable COW pages. A batch
  updates only touched entry pages and publishes one root at the exact next
  prefix. Duplicate writes, absent deletions, arithmetic exhaustion, and memory
  pressure are explicit failures with unchanged published state.
- `prepare_batch` builds and accounts an unpublished candidate before a durable
  proposal. Dropping it cancels the candidate. `publish` checks owner/base-root
  provenance and performs no allocation. The owner must serialize this interval.
- Snapshot owners retain roots in a bounded lease registry. Callers receive
  weak leases; `advance_clock` reaps expired roots even when a caller retains
  the handle. Weak handle metadata stays charged until its last clone drops.
- Scans and breadth-first traversal return fixed-prefix pages and bound
  responses, neighbor probes, total nodes/depth/probes, and continuation state.
  Continuations bind their originating lease and query. Cumulative traversal
  truncation is explicit; per-response limits return a continuation.

Mutable owners and prepared metadata use owned state and a checked process-local
`OwnerId`; these identities cannot be serialized or reused after restart. Shared
ownership is limited to detached concurrent snapshot readers: immutable pages,
roots and content remain alive across owner publication; lease clocks and weak
lease metadata enforce expiration. `MemoryBudget` shares atomic counters because
allocations can move to worker threads and release their charge there. The
`retained_snapshot_stays_at_one_prefix_during_concurrent_writes` test runs these
readers concurrently with publication. No global mutation lock is introduced.

The runtime supplies unique incarnation IDs and monotonic time, calls
`advance_clock` on a bounded timer schedule, accounts admission queues, and maps
domain failure cases onto the public protocol. Traversal callbacks must use
ordered keyset adjacency and perform bounded indexed work at the pinned prefix.
An absent referenced key is an error; an archive adapter must translate an
archived boundary explicitly.

Memory charges cover engine-owned allocation capacity and conservative
bookkeeping. Generic keys/values supply their dynamic heap charge; arbitrary
`Clone` implementations cannot be measured automatically. Shared content is
accounted separately from entry-owned lifecycle bytes. Responses retain their
charge until dropped. These counters are not a claim about exact process RSS or
allocator fragmentation. Collections use safe standard allocation facilities;
recoverable `try_reserve` failures are typed, while a process allocator that
aborts on OOM cannot be recovered inside this crate.

The current page directory is copied in O(number of pages) per mutation;
underfull pages are reclaimed when empty, without compaction of partially full
neighbors. Family-specific graph/index adapters, cross-range transactions,
snapshot export/installation, durability, placement fencing, and distributed
publication belong to later integration work. These foundations alone do not
complete architecture package P03 or establish a distributed scale result.

Validation:

```sh
cargo test -p focal-memory --offline
cargo clippy -p focal-memory --all-targets --offline -- -D warnings
```
