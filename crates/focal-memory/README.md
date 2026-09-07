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
  updates touched entry pages and directory paths, then publishes one root at
  the exact next prefix. Duplicate writes, absent deletions, arithmetic exhaustion
  and memory pressure are explicit failures with unchanged published state.
- Its persistent page directory uses immutable nodes with 16–32 handles, except
  the root. Leaf nodes hold `Arc<Page>` handles; branches hold `Arc<Node>`
  handles and cache subtree page counts. Splits, sibling borrowing/merging and
  root collapse preserve bounded height. Separators borrow keys from descendant
  pages; directory maintenance neither clones keys nor adds sharing per row.
  Routing and boundary checks can descend through those borrowed minima, so
  their comparison cost includes an additional height factor. Scans use a fixed
  borrowed ancestor stack without allocating or cloning directory handles.
- `RangeConfig::page_bytes` bounds the complete accounting charge of an ordinary
  leaf; `max_entry_bytes` bounds inline entry bytes plus declared key/value heap.
  Larger admitted entries occupy singleton leaves. An adjacent insertion shares
  an unchanged oversized leaf instead of copying its payload. Preflight and
  construction use the same byte/count partitioner, and checkpoint import stages
  byte-bounded chunks with oversized rows alone. The generic defaults preserve
  the existing count-only layout; native Core derives finite internal ceilings.
- `prepare_batch` builds and accounts an unpublished candidate before a durable
  proposal. Dropping it cancels the candidate. `publish` checks owner/base-root
  provenance and performs no allocation. The owner must serialize this interval.
- `prepare_batch_with` and `prepare_after_with` support values without `Clone`.
  New entries move directly into prepared pages. Only retained entries in touched
  pages invoke the supplied fallible copier, after reserving those pages' complete
  charge. `PreparedRange::entries` exposes the complete ordered pending prefix.
  Existing `Clone` callers use the same preparation path.
- `plan_batch` and `plan_after` sort the owned write set in place and inspect
  touched leaves without copying values, allocating a directory buffer or
  reserving memory. Their quotes bind the exact base and inputs. New page charges
  are exact; directory and total charges are conservative upper bounds that
  include temporary nodes across all elementary edits. Builds debit actual
  allocations and enforce the cumulative directory bound. A quote alone does
  not protect capacity from another owner.
- `future_write_envelope` derives a `RangeWriteEnvelope` from fixed layout and
  owner-budget limits, independently of current occupancy. `RangeWriteLimits`
  bounds changed keys, deleted keys, incoming Put heap and actual input-vector
  capacity. At most `m` changed keys affect `m` old leaves; incoming rows split
  retained ordinary runs into a partition needing at most `3m` new pages and
  directory edits, including empty deletion groups. Unchanged oversized leaves
  stay shared. The immutable budget ceiling bounds directory height; pinned old
  roots remain separately charged. `check_plan` checks owner identity, every
  input bound and each actual preparation-charge component before construction.
  Put-only envelopes charge no hypothetical Delete payload. This is a checked
  bound for one write, not a funded evaluation or complete retry-chain guarantee.
- `MemoryBudget::funded_child` reserves spendable backing plus metadata before
  use. `RangePreparationPlan::build_in_with` can draw from an owner budget or
  its descendant pool; shared nodes/pages keep their original allocation source.
  Returning an allocation restores pool credit without new ancestor admission.
  The full backing remains held until its last handle, descendant and issued
  allocation drops. A pool accounts aggregate capacity; it does not assign
  individual evaluation entitlements or reserve disk space.
- `MemoryBudget::elastic_funded_child` creates a uniquely resizable owner pool
  with an immutable ceiling. Zero initial capacity holds only its metadata.
  Growth funds the parent charge before making new credit available; trimming
  atomically acquires idle credit before releasing backing. Concurrent allocation
  refunds restore ancestor categories before making credit reusable. The
  non-Clone `ElasticFundedPool` controller can return unused capacity while
  retained pages and descendants keep their own backing. Its owner must also
  protect idle credit promised to future work; issued bytes and available bytes
  alone cannot express those promises. Resize adds no per-growth buffer or
  per-evaluation counter allocation.
- Snapshot owners retain roots in a bounded lease registry. Callers receive
  weak leases; `advance_clock` reaps expired roots even when a caller retains
  the handle. Weak handle metadata stays charged until its last clone drops.
- Scans and breadth-first traversal return fixed-prefix pages and bound
  responses, neighbor probes, total nodes/depth/probes, and continuation state.
  Continuations bind their originating lease and query. Cumulative traversal
  truncation is explicit; per-response limits return a continuation.

Mutable owners and prepared metadata use owned state and a checked process-local
`OwnerId`; these identities cannot be serialized or reused after restart. Shared
ownership keeps immutable pages, directory nodes, roots and content alive across
pending publications and bounded concurrent snapshot readers; lease clocks and
weak lease metadata enforce expiration. `MemoryBudget` shares atomic counters
because allocations can move to worker threads and release their charge there. The
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
`Clone` implementations and supplied copiers cannot be measured automatically.
A fallible copier must preserve the source and its meaning, fit its resulting key
and value allocations within the entry's recorded heap charge, and separately
account any temporary workspace. Failure drops the entire candidate and its
provisional charges. `SnapshotLease::project_next` can inspect non-`Clone` values
without allocating a response or allowing the borrowed entry to escape.
Shared content is accounted separately from entry-owned lifecycle bytes.
Responses retain their charge until dropped. These counters are not a claim about exact process RSS or
allocator fragmentation. Collections use safe standard allocation facilities;
recoverable `try_reserve` failures are typed, while a process allocator that
aborts on OOM cannot be recovered inside this crate.

Directory allocation demand now grows with the number of leaf edits and bounded
tree height. A batch applies elementary persistent edits, so several edits can
rebuild a shared path more than once. Entry pages are reclaimed when empty;
partially full entry-page neighbors are not compacted. Directory occupancy repair
is separate from entry-page compaction.

The native Core owner now layers Admission completion envelopes, per-evaluation
entitlements and exclusive spending loans over these primitives. Its contract
prices schema verification, authored descriptors, remaining attempts and discrete
record limits; this storage envelope alone does not. Durable capacity and the
remaining object families still require integration. Family-specific graph/index adapters, cross-range
transactions, snapshot export/installation, durability, placement fencing and
distributed publication also belong to their respective owners. These primitives
alone do not complete architecture package P03 or establish a distributed scale
result. The most recent full workspace qualification passed 87 memory unit tests,
all memory integration tests, and the complete 1,476-test workspace suite, including future-write
envelopes, elastic funding with controlled interleavings and native owner
completion loans under exhausted parent budgets. The subsequent native owner
index is qualified separately in the dated implementation record, which contains
the scope and remaining work.

Validation:

```sh
cargo test -p focal-memory --offline
cargo clippy -p focal-memory --all-targets --offline -- -D warnings
```
