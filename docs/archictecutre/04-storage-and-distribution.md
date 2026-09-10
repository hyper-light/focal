# RAM storage, durability, distribution, and recovery

Status: proposed Focal implementation design; no implementation or scale result is implied.
This document adapts Hecate to Rust, RAM as the primary queryable store, disk-backed
durability, and deployments ranging from one laptop to many regions.
It preserves one ordered mutation history per session.
The user-facing deployment/configuration contract is [08](08-stepped-complexity-and-deployment.md).

## 1. Source contract and deliberate adaptations

The source documents are evidence, not executable algorithms or a complete implementation.
All paths below refer to the copied Hecate snapshot recorded by the source manifest.

| Source | Relevant contract | Focal treatment |
|---|---|---|
| [LEDGER](reference/hecate/docs/architecture/LEDGER.md), §8, original lines 341–390 | WAL before visibility; deterministic replay; complete archival retirement | Preserve; implement concrete checkpoint, archive receipt, and cursor protocols |
| [LEDGER_SUBSTRATE](reference/hecate/docs/specs/LEDGER_SUBSTRATE.md), §§2–3 | Session sequencer owns order; state/apply may partition | Preserve; distinguish session consensus groups from materialization ranges |
| [STORE](reference/hecate/docs/specs/STORE.md), §§2b, 5, 9, 16 | One WAL; directory fences; range sharding; claims retain one session log | Preserve these semantics; replace prescribed LSM/B-tree cold backends with a custom primary RAM engine |
| [WAL](reference/hecate/docs/specs/WAL.md), §§1, 3–6 | Logical logs multiplex over physical segments; durable acknowledgement; checkpoint-owned floors | Preserve the separation; specify persistence ordering and file installation explicitly |
| [CONSENSUS](reference/hecate/docs/specs/CONSENSUS.md), §§1–6 | Quorum authority, ReadIndex, membership changes, resource fencing | Preserve safety; use a Rust consensus adapter, rather than adopting the requirement to write Raft from scratch |
| [MATERIALIZER](reference/hecate/docs/specs/MATERIALIZER.md), §§4–8 | Parallel execution must equal serial replay; publish only a contiguous prefix | Preserve the contract; replace unsafe pseudocode with tracked-access validation and suffix recomputation |
| [SESSIONS](reference/hecate/docs/specs/SESSIONS.md), §2 | Sessions span nodes while order stays with one sequencer | Preserve; placement and failover need not remain region-local in Focal |
| [CACHE](reference/hecate/docs/specs/CACHE.md), §§0–4 | Cache is reconstructible; immutable content differs from mutable state | Preserve; the primary RAM engine is not an evictable cache |
| [FANOUT](reference/hecate/docs/specs/FANOUT.md), §§3–7 | Replayable deltas; hints versus durable external delivery | Preserve; do not promise network exactly-once delivery |
| [ARCHIVE](reference/hecate/docs/specs/ARCHIVE.md), §§2, 4 | Content-addressed documents, indexed custody, idempotent ingestion | Adopt retired-proof storage only initially; this source is marked presented for acceptance |

Hecate explicitly forbids synchronous WAN work on session hot paths and region failover
in CONSENSUS §1 and LEDGER_SUBSTRATE §5. Focal's requested multi-region objective changes
that deployment restriction. A session that promises survival of region loss needs an
appropriate cross-region quorum and content durability placement, with the resulting
WAN write latency. Asynchronous replication is a different guarantee, not an equivalent
way to provide zero loss of acknowledged mutations.

The initial Rust runtime may use Tokio and a `raft-rs` adapter. Module-private `Arc`
references to immutable snapshots are acceptable. Mutable domain state stays with its
owner; shared mutable maps behind a global mutex are not the storage architecture.
These are explicit adaptations of Hecate's custom-runtime and no-refcount doctrines.

## 2. Units, identifiers, and ownership

| Unit | Authority | Owns |
|---|---|---|
| Deployment root | Small replicated control group | Region identities and namespace-range delegation to regional control partitions; never one row per tenant/session or individual claims |
| Regional directory partition | Replicated control group | Bounded ranges of session placement and materialization-directory records |
| Session group | One Raft group | Ordered ledger mutations, lifecycle, accepted outcomes, route transitions, checkpoint references |
| Session sequencer | Current session leader, fenced by term | Preparation and effective-state overlay; the session's single mutation order |
| Materialization range | Session-authorized placement epoch | A contiguous interval of stable object keys, custom RAM indexes, versioned roots |
| Range replica | One task owner | Local apply progress and immutable published snapshots for its assigned interval |
| Physical WAL stream | One node-local writer task | Segments carrying records from many logical Raft logs |
| Content store | Explicit durability policy | Immutable artifacts, retired proofs, checkpoint blocks and manifests |

Two dimensions must never be conflated: consensus replication copies ordered durable
truth; state partitioning divides materialization work and memory. Splitting one claims
range does not create another independent claim sequencer or another authority log.
Generic directory/control stores may use one group per shard, as Hecate STORE describes.
Claims ranges consume their existing session log, including entries irrelevant locally.
Each assigned range replica applies committed entries to its own RAM state. A newly
selected serving replica must prove current ownership and possess the required prefix
before answering; election or process survival alone does not make it read-ready.
Replication factors for the session log and materialized ranges are distinct placement
details, both derived to satisfy the requested failure tolerance and recovery budget.

```rust
struct SessionSeq(u64);             // comparable only within one SessionId
struct RaftIndex(u64);              // includes consensus-internal entries
struct RaftTerm(u64);
struct RouteEpoch(u64);             // session-authorized range placement generation
struct WalOffset { stream: u32, segment: u64, byte: u64 }
struct GenerationalHandle { arena: RangeInstanceId, slot: u32, generation: u64 }
struct VersionId { session: SessionId, seq: SessionSeq }
struct ReadToken { version: VersionId, route_epoch: RouteEpoch }
```

The examples elide the enclosing authenticated `LedgerId { tenant, session }` where a
SessionId appears alone. Every persisted key, exported reference and client envelope
binds that full namespace; an internal session-scoped coordinate does not weaken tenancy.

Stable `ObjectId`/`ClaimId` values appear in relations, deltas, checkpoints, and RPCs.
Arena handles never escape their range owner; movement and restart rebuild them.
Handle resolution verifies arena identity as well as slot generation; equal slot numbers
from different ranges cannot accidentally resolve to each other's records.
Counter overflow is a typed capacity failure, never wraparound.
Every application entry, including a route or retirement control entry, has a SessionSeq.
Raft-internal no-op/configuration entries may lack one. The log adapter records the mapping
from committed RaftIndex to the greatest committed SessionSeq at or below that index.
An uncommitted proposal's tentative sequence can be discarded/reused after leadership
change; only a committed sequence is an externally observable identity.

## 3. Rust boundaries and receipts

Interfaces below specify responsibilities; implementation may refine concrete types.
Async IO belongs outside the synchronous reducer and RAM mutation code.

```rust
trait LedgerReducer {
    fn prepare(&self, state: &EffectiveView, cmd: Command) -> PreparedResult;
    fn apply(&self, view: &dyn TrackedRead, op: &PreparedMutation) -> ApplyResult;
}
trait SessionLog {
    async fn propose(&self, op: PreparedMutation) -> Result<DurableReceipt, LogError>;
    async fn read_barrier(&self) -> Result<RaftIndex, LogError>;
    async fn committed_from(&self, next: RaftIndex, limit: ByteLimit)
        -> Result<CommittedBatch, LogError>;
}
struct DurableReceipt {
    session: SessionId, seq: SessionSeq, raft_index: RaftIndex, term: RaftTerm,
}
// Public MutationReply/MutationReceipt are defined in 03 §5; DurableReceipt is internal.
trait PublishedViews {
    async fn wait_and_pin(&self, token: ReadToken, ranges: RangeSet)
        -> Result<PinnedRead, ReadError>;
}
trait ContentStore {
    async fn stage_and_seal(&self, input: BoundedContentStream, policy: ContentPolicy)
        -> Result<ContentAvailabilityReceipt, ContentError>;
    async fn read_verified(&self, reference: ContentRef, budget: ReadBudget)
        -> Result<BoundedContentStream, ContentError>;
}
```

The normal public mutation API returns `MutationReply::Committed` only after the mutation
is quorum-committed and published at a complete session prefix. Inform and Refuse do not
manufacture mutations; Yield requires a race-free monitor-registration contract.
An explicitly named durable
submission API may return `DurableReceipt` before materialization; its contract requires
the caller to wait or read with `min_seq = receipt.seq`. Neither receipt means that a
validator ran successfully; the reply describes the recorded domain result.
Transport timeouts after proposal produce `OutcomeUnknown(request_id)`, not a definitive
failure. Retry with the same scoped request identity retrieves or completes the same
operation. Request-dedup state and expiry rules are replicated, checkpointed state.

## 4. The custom RAM engine

`MemRange` owns live graph records, lifecycle cells, relations, and secondary indexes.
Disk is the recovery substrate; point reads and traversals do not replay the WAL.
Focal does not need RocksDB, SQLite, Redis, or an embedded KV database on this path.

Use a safe-Rust implementation first, with these concrete structures:

- A generational slot arena for records, using fixed-capacity pages and explicit byte charges.
- Immutable authored bytes stored once, separately from versioned system lifecycle cells.
- An object-ID point index with a fixed, explicitly seeded hash function; canonical output
  sorts keys and never depends on hash iteration or allocation order.
- An ordered key index for bounded scans and range transfer; begin with an ordinary
  ordered implementation, then replace internals only through the same conformance suite.
- Incoming/outgoing adjacency indexes, plus typed relation indexes needed by the reducer.
- Immutable, copy-on-write pages and persistent root references for published versions.
- A bounded root history indexed by SessionSeq, reader pins, and retired-page accounting.

One owner applies a complete mutation's write set to unpublished pages, updates all its
indexes, and installs a new immutable root. Readers receive pinned immutable roots over
bounded channels; they never borrow a mutable arena while the owner changes it.
Publication can group several consecutive mutations, but a read advertised at sequence S
must get exactly S, not a mixture of roots last updated at unrelated later sequences.
Retain intermediate roots or reconstruct an unpublished candidate root before publishing
the requested cut. Do not copy the entire range for every mutation; copy only changed
pages and index paths. Measure write amplification, root overhead, and pin retention.

The RAM arena may internally recycle slots only after all retained roots and pins release
the old generation. An object moving to another node keeps its stable ID, not its handle.
Checkpoint serialization excludes allocator addresses, map capacity, and reference counts.
Current lifecycle cells contain bounded current state and history references. Older
progress/status records may move into immutable history blocks even while a claim remains
active; snapshots retain their verified roots. Do not retain an ever-growing in-slot
history vector, and do not summarize away the original proof records to control RAM.
The serial reference model uses simple deterministic maps; it is the semantic oracle,
not a production storage dependency or an alternate durability mode.

The first implementation may serialize application within each range and parallelize
different sessions. Do not call an implementation lock-free merely because it is written
in Rust, and do not add unsafe epoch reclamation before profiling demonstrates a need.

## 5. Memory accounting and admission

RAM-primary does not mean that every historical proof remains resident forever.
Live work, required dependency state, and the active lifecycle projection must fit the
allocated working-set budget; completed released proof moves to durable archival custody.
Checkpoints alone do not reduce live-state size.

Each node divides an explicit memory envelope into separately charged pools:

```text
node_budget >= control_reserve + wal_buffers + live_state + indexes
             + speculative_state + apply_workspaces + pinned_old_pages
             + delta_history + subscriber_buffers + content_staging + safety_reserve
```

Each session and range additionally owns quotas within those pools. Count allocator/page
slack, secondary-index entries, relation fanout, reader roots, and speculative copies.
Borrowing from an idle pool requires a revocable reservation; data cannot consume the
non-borrowable control, quorum, or recovery reserve.
Admission reserves a conservative byte bound for the prepared mutation and its index
growth before proposing it. Large content is staged in bounded chunks under a separate
budget. Full queues return typed retryable pressure with an observable cause.

When pressure rises, perform bounded work in order: release expired/cancelled read pins,
prune versions below the safe floor, retire eligible proof, schedule range movement or
additional capacity, then throttle new work for that session. A stalled consumer must
receive `ResyncRequired`; it cannot pin the entire live graph or WAL indefinitely.
If all resident objects are still live, admission must reject or wait for capacity;
silently evicting authoritative lifecycle state is not permitted.
An underestimated allocation after commit cannot turn the committed mutation into an
uncommitted error: retain its log, stop publication, report capacity failure, and recover
or move the range. This is why sizing and maximum fanout limits belong at admission.

Disk is admitted the same way. One envelope per data volume
(`focal_memory::DiskBudget`, [24](24-placement-execution-and-fleet-control.md)
§10) promises every durable write its bytes before it is queued: WAL batches
and checkpoint rewrites, upload staging, sealed objects, imported chunks and
custody records. A promise is refused with a typed capacity error before any
acknowledgement when the volume's sampled free bytes, less what is already
promised, would fall below the headroom watermark (or, for ordinary work, the
completion reserve); it is charged against the estimate once the write is
behind its fence and returned if it never happens. The envelope samples the
volume at a bounded cadence and refuses fresh work while it cannot.

The sequencer's lifecycle/affordance projection is itself budgeted and measured.
It can become the ceiling for one enormous session even if heavy artifact bytes are
distributed. No document should claim that this projection remains small at arbitrary
scale. Crossing its capacity limit is a named admission/architecture constraint.

## 6. WAL and quorum durability

The node multiplexes many logical Raft logs across a bounded number of physical writers.
Each writer owns a bounded queue and a sequence of preallocated segment files.
Natural group commit batches already queued writes; batching does not change transaction
semantics, and one producer's large batch cannot monopolize control-storage capacity.

Each segment has a magic, format version, cluster/node identity, stream identity,
generation, segment number, and checksum seed. Each record has bounded length, kind,
group identity, Raft term/index where applicable, physical sequence, and checksum.
Hash content identity separately from record checksums. A generation and checksum chain
prevent stale bytes in recycled segments from becoming valid current records.
Record kinds cover entries, hard state, truncation metadata, and snapshot installation.
Entry payloads carry canonical prepared ledger mutations and content references.

The durable write state machine is:

```text
RESERVED -> ENCODED -> APPENDED -> FLUSH_SUBMITTED -> FLUSH_CONFIRMED -> ACK_ELIGIBLE
                  \-> IO_ERROR: stop dependent acknowledgements, preserve evidence
```

An append-completion event is not a durable-completion event. The adapter persists
entries before durable hard state that refers to them, and it releases vote/append
responses only after their covering durable state is confirmed. A batch flush is not
assumed physically atomic: a crash may persist only part of a batch. Recovery accepts
only records whose complete framing, checksum, and required predecessors validate.

An embedded one-voter group acknowledges after the required local disk flush.
A replicated group acknowledges only after Raft commits through a quorum whose required
entries are durably persisted. Replication into another process's RAM is insufficient.
The Rust storage adapter must document platform-specific flush semantics; do not assume
that every `sync_all` implementation implies the strongest available hardware barrier.
New segment files and manifest renames also require the containing directory's durable
installation discipline. Test these through a filesystem abstraction and real platforms.

The physical log is the only write-ahead log. The RAM engine adds no redo journal.
Checkpoint files and content objects may require their own durable writes; “one WAL”
does not mean that a checkpoint never fsyncs or that a write touches only one device.
The number of physical streams is a measured tuning parameter, not an ordering authority.
Hecate WAL §2's throughput equation simplifies to a constant if its stated batch-arrival
assumptions hold; benchmark stream counts rather than treating that formula as a proof.

## 7. Artifact availability precedes ledger references

An artifact hash proves identity, not that the bytes can be retrieved after a crash.
The mutation path must not acknowledge a durable testament referring only to volatile
staging data on the proposing node.

1. Stream bytes into bounded staging; validate declared size, schema and hash.
2. Seal the immutable object and satisfy its declared durability/failure-domain policy.
3. Return a receipt binding the content identity, length, policy, durable placement,
   placement generation, and staging/retention identity.
4. Prepare the ledger mutation using that receipt; reject a receipt with insufficient
   durability for the session's acknowledged-loss guarantee.
5. Commit the reference in the session log, converting the staged object into a durable
   ledger root through an idempotent reference-registration operation.
6. Release staging only after the committed reference is discoverable by content GC.

GC treats valid staging reservations, committed-log tails, checkpoint manifests, archive
indexes, and active transfers as roots. A crash between content sealing and log commit
can leave an orphan for later collection; a crash after log commit must not delete the
object during the reference-registration gap. Use a committed reference watermark and
staging pin until every relevant log consumer has crossed the handoff, not a short TTL.
Artifacts and checkpoints use cross-region placement when region-loss durability is
promised. A cross-region Raft quorum with all artifact copies in one region is incomplete.

Implemented (2026-09-10, [26](26-custody-archive-retention-and-restore.md) §1): each
copy's verified `Durable` answer is kept as a custody receipt named by ledger, object and
copy, and a phase that evaluates an artifact begins only when every required copy of the
current placement holds a receipt at the current scope or answers the verification now.

## 8. Watermarks, reads, and atomic visibility

Use distinct typed progress values; never overload a field called `durable`.

| Mark | Meaning | What it permits |
|---|---|---|
| `local_log_durable` | This node's complete durable Raft-log prefix | Local recovery and safe protocol responses |
| `committed_index` | Quorum-authorized Raft prefix | Deterministic application of committed entries |
| `committed_seq` | SessionSeq mapped from committed_index | Upper bound on visible domain state |
| `range_applied[r]` | Whole session prefix processed by a serving range replica | Candidate local roots, including no-op progress for irrelevant entries |
| `published_seq` | Complete prefix available across all active session ranges | Normal mutation replies, coherent graph views and monitor dispatch |
| `checkpoint_seq` | Installed manifest with every required range/content root sealed | Recovery from that checkpoint plus suffix |
| `consumer_seq[c]` | Last transactionally processed delta cursor for consumer c | Resume, dedup, and retention obligations |

For one directory epoch, `checkpoint_seq <= published_seq <= committed_seq`.
`published_seq = min(range_applied[r])` over the active required range set, subject to
successful root installation and publication checks. Compare SessionSeq only within the
same session. Unrelated groups' Raft indexes or wall-clock timestamps cannot be minimized
to create a global order. Slow optional follower replicas do not gate every read; each
range's advertised serving replica must possess the requested root and current authority.

A linearizable read follows this algorithm:

1. Resolve the session leader and range routes from bounded directory caches.
2. Run ReadIndex in the session group; map the returned Raft index to SessionSeq C.
3. Wait for a published prefix S >= C and any caller `min_seq`.
4. Acquire one `ReadToken{S, route_epoch}` and a bounded SnapshotLease pinning every
   declared/discovered required range at exactly S.
5. Read points, scan indexes, or traverse relations through those pinned views.
6. At a page-byte limit, release page-scoped borrows and return a continuation retaining
   the SnapshotLease and its roots. Release the lease and all retained pins only on
   complete query, cancellation, lease deadline or total-query work/byte-budget exhaustion.

If traversal discovers a new range, pin it at the same token before using it. If that
version is gone, return `ReadTooOld` with a restart token, not a mixed-prefix result.
An epoch change cannot silently switch an in-flight query to a new root set: finish on
pinned old owners while valid, or retry the entire view with a fresh token.
Queries have explicit node/edge/depth/byte/work budgets and resumable continuation tokens.
A token authenticates session scope, query identity, prefix, route epoch, and expiry.
An `AtLeast` token preserves its minimum sequence while refreshing current routing;
a fixed-snapshot token preserves its exact sequence and expires if that view is gone.
Paginated fixed-snapshot queries keep the same sequence under a bounded pin lease;
expiry requires an explicit restart rather than silently advancing the next page.

The publication coordinator is the session leader's fenced role, reconstructed from the
committed log and range progress after failover. Range progress messages carry session,
range, route epoch, producer identity, and term. Stale acknowledgements cannot publish a
new epoch. No external observer receives half of a child/parent lifecycle transaction.
Monitors consume the same fully published sequence, never raw per-range apply events.
This minimum frontier is intentionally a per-session head-of-line constraint; many
sessions progress independently, but one stalled active range can stall its session.

## 9. Deterministic parallel apply

Implemented for committed-record materialization on 2026-09-09
([25](25-parallel-materialization-and-ranges.md) §2); the leader's admission
stays serial by decision F38.

The reference algorithm applies one prepared mutation at a time, in SessionSeq order.
Validators, service handlers, clocks, randomness, network reads, and subprocesses never
execute during replay. Their accepted outcomes and deadline firings are logged inputs.

Parallel execution is an optimization of this algorithm, gated by differential tests.
Define a complete footprint vocabulary: object cells, lifecycle cells, adjacency lists,
secondary-index buckets/ranges, aggregate counters, monitor registrations, and range
predicates. A relation scan is not merely a point read of its source claim.
Unknown/dynamic graph effects receive a conservative session-wide or partition-wide
dependency until a complete deterministic expansion is available.

For a bounded batch with frozen base prefix B:

1. Derive declared footprints from trusted operation code; client declarations are hints.
2. Build RAW/WAR/WAW edges in sequence order using last writers and intervening readers.
   Skip self-edges when an operation both reads and writes a key; deduplicate edges.
3. Drain ready nodes on bounded workers; each computes an isolated result and write set.
4. Reads choose the greatest predecessor version strictly before the current operation,
   plus its own staged writes if the reducer requires read-your-writes semantics.
5. Record every actual point read, predicate/range read, and write through tracked APIs.
6. At a deterministic end-of-batch barrier, compare actual accesses with declarations
   and check read versions against the complete set of earlier writes.
7. If entry K has a violation, discard K and the entire later speculative suffix.
   Install only the verified prefix; recompute the suffix serially from that prefix.
8. Publish only complete consecutive mutations in sequence order after all their range
   fragments and index changes are installed.

Retaining later speculative results after recomputing K is unsafe: they may depend on
K's stale result. Serial fallback is deterministic and must not append duplicate inputs,
rerun validators, or invent a different command order. Repeated mismatches identify a
footprint bug and disable that operation's parallel eligibility until corrected.
An implementation may initially discard the entire batch and rerun it serially; this is
less efficient but easier to verify. Batch size and worker count may vary without
changing state bytes, delta bytes, outcomes, or monitor releases.

Hecate MATERIALIZER §4.2 adds a self-edge for read-modify-write keys as written.
Its §4.5 “check while applying” can miss an earlier writer that has not executed yet.
Its max-writer-only overlay cannot preserve all predecessor versions or make incomplete
dependency declarations safe. These pseudocode details are not copied into Focal.

Distributed execution uses the same ordered inputs and tracked footprints. Each range
waits for required predecessor fragments, obtains immutable remote values at the exact
dependency version, and produces an idempotent result fragment keyed by entry/range/epoch.
The coordinator advances publication only after all participants for the prefix finish.
Because inputs are already committed, this is deterministic replay coordination, not a
distributed “prepare a transaction and vote to commit” protocol. Common order alone
does not remove the need for remote-read, completion, retry, and publication barriers.

## 10. Checkpoints and crash recovery

A checkpoint is a complete session recovery manifest, not a convenient dump of one range.
Choose a published sequence S and pin every active range at S under one route epoch.
Serialize immutable blocks in canonical key order, with versioned framing and hashes.
The manifest binds session identity, S, its Raft-index/term mapping, membership snapshot,
route epoch/ranges, each range root, dedup state, lifecycle projection, retention state,
content roots, schema/codec versions, and any replay-required cursor metadata.

Installation sequence:

1. Write bounded temporary block files; verify hashes and flush required contents.
2. Install block names durably; replicate/upload blocks according to checkpoint policy.
3. Write and verify the complete manifest only after every referenced block is available.
4. Atomically install the manifest pointer using the platform's rename/directory flush
   discipline, and commit its checkpoint reference through the session group.
5. Advance `checkpoint_seq` and compaction eligibility only after installation is durable.
6. Retain a previous recoverable generation until the new generation is verified usable.

Crash before step 4 leaves unreferenced blocks; crash after step 4 recovers the complete
manifest or its previous generation. Never publish a pointer to a partially written tree.
Restoring a replica loads and verifies a checkpoint, reconstructs arena/index state,
replays only committed suffix entries through the normal reducer, rebuilds projections,
and waits for current authority before serving. Validators are not replayed.
Recover from quorum or verified checkpoints on corruption; an unrecoverable one-voter
log refuses service rather than guessing that damaged acknowledged bytes were absent.
All-zero tail detection alone does not prove a damaged suffix was unacknowledged.

Log reclamation uses an explicit retention coordinator. It takes the minimum safe floor
across checkpoint recovery, consensus membership/catch-up obligations, ongoing moves,
required delta consumers, archive custody, and content-reference registration.
Consumers that support reseeding may be moved past retained history only by an explicit
`ResyncRequired(snapshot)` contract. Consumers requiring every historical event need a
durable historical event archive; a current-state snapshot is not an event-history substitute.
Raft term/index snapshot metadata must remain consistent with compacted entries.
Physical segments are reclaimed only when all resident logical logs permit it; rewrite
live stragglers with an atomic segment-index update before unlinking old segments.

## 11. Range split, move, merge, and fencing

The directory selects cuts and placements; the session log authorizes when they take
effect. This preserves Hecate's one-splitter intent without giving a stale owner time
between an external directory CAS and its local discovery to accept stale work.

1. Commit `RangeChangeIntent{old_epoch, new_epoch, ranges, destinations}` in the session log.
2. Pin a seed prefix S, transfer hash-verified range blocks, and start destination replay.
3. Destinations report caught-up progress with their exact intent and epoch identity.
4. Gate admission of mutations touching the moving range, drain already prepared proposals,
   and commit a cutover barrier C in the same session log. Old owners stop serving new-token
   data operations beyond C; all destinations must install through C before activation.
   Both sides continue consuming control entries. Held callers wait within bounded credits
   or receive a retryable result; no intervening data mutation can fall between the seed
   cutoff and ownership activation.
5. Commit activation of the new epoch/range set. Update the directory through an
   idempotent CAS tied to that activation record; clients may see stale routing only.
6. Publish the new epoch and reopen affected admission only when its complete range set is ready. Stale-epoch requests
   receive `NotOwner{current_epoch}` and refresh routes; no old owner can self-authorize.
7. Drain bounded old read pins and watch continuations, record retirement completion,
   then delete source copies after checkpoint/recovery obligations permit it.

Every step has a stable operation ID and can fast-forward after coordinator death.
If a source fails, rebuild the destination from a verified checkpoint plus the session
log; never require the failed source's mutable memory. Merging ranges follows the same
intent/seed/catch-up/barrier/activation protocol. Initial implementation may pause session
publication briefly at cutover; bound and measure that pause under healthy qualified
conditions. Failed destinations or lost quorum have no unconditional finite recovery
bound. Bound each caller's wait with typed Retryable/Unavailable, and resume or abort the
move through committed control records when authority recovers; never force an unsafe
activation merely to satisfy a latency target.
Directory CAS alone is not a resource fence. Routing epochs, leader terms, and committed
activation records are checked where reads, fragments, writes, and transfers are accepted.
Cross-region movement additionally verifies that voter and content placement still meet
the declared session failure-domain policy before retiring any old durable copy.

## 12. Watches, deltas, caches, and archival custody

A delta identity is `(session, seq, ordinal)`; one mutation can emit multiple ordered deltas.
The replay cursor includes ordinal where necessary. Deterministic replay produces the
same delta identities and bytes. The log is the recoverable event source, not a separate
durable outbox table maintained by the RAM engine.

Seed-to-live handoff: register a bounded subscription, pin published prefix S, send bounded
snapshot chunks at S, complete the seed, then replay deltas strictly after S. Concurrent
deltas remain replayable from retained log/history; overflowing an intermediate buffer
causes an explicit resync. Emit `Resolved(S)` only when every matching delta through S
has actually been sent in order, not merely when consensus has committed S.
On reconnect, replay from the consumer's last durably processed cursor. Duplicates after
lost acknowledgements are expected; a consumer commits its side effect and cursor in
one local transaction or makes its side effects idempotent. This is not network exactly-once.

Hints can be dropped only for consumers with an authoritative recovery cursor. A dropped
last notification with no later traffic still needs a liveness mechanism: stream/session
reconciliation driven by a bounded reconnect/watchdog schedule, not an assumption that
another delta eventually arrives. Correctness-critical monitor dispatch stays separate
from cache invalidation and external fanout queues.
Mutable caches carry session sequence versions; freshness-sensitive reads use the fixed
prefix authority path. Hash-addressed immutable content needs no mutable invalidation.

Retirement is a logged custody transfer: select terminal-and-released proof, pin complete
objects/relations/artifacts, seal and index the archive bundle, obtain durable custody,
commit its archival reference, then reclaim eligible hot slots and indexes. Active monitors,
dependencies, read pins, and replay obligations constrain eligibility. Retained boundary
metadata resolves traversal to a typed archival continuation; “missing” cannot mean retired.
Archive writes and indexes are idempotent by stable bundle identity. No proof is summarized
in place or silently dropped to meet a RAM target. Historical metadata also needs a
durable bounded index strategy; accumulating one permanent RAM stub per retired object
would defeat the live-work memory bound.

## 13. Laptop to multi-region deployment

One executable hosts the same session log, reducer, memory engine, and recovery protocol.
One laptop uses one voter, local file-backed content/checkpoints, one or a few physical
WAL writers, and in-process range messages. It still flushes before acknowledging.
Distributed deployments use many independent session groups and many range placements;
RPC replaces a local message edge without changing domain mutation semantics.
The deployment progression is laptop, VM/bare metal, Kubernetes, multiple availability
zones, multiple regions, and global scale. These are placements of the same engine, not
six product modes. Users describe nodes, their real failure domains, resource allowance,
and failures the service must survive; shard counts, WAL stream counts, worker counts,
and checkpoint pacing are internal derived settings with inspectable explanations.
Provisioning must refuse an impossible guarantee and surface a changed guarantee
explicitly; topology discovery cannot silently upgrade or downgrade acknowledged-proof
durability. Kubernetes is an installation adapter, not a new consistency mechanism.

Session placement declares the failure it promises to survive. A regional session can
avoid WAN commit latency but cannot promise availability after total loss of its region.
A region-tolerant session places quorum and content copies across sufficient regions;
for example, three voters across three regions retain a majority after one region fails.
Five voters placed 2/2/1 also tolerate loss of any one of those three regions. Three
voters placed 2/1 across two regions do not tolerate loss of either region symmetrically.
Membership changes use the consensus library's supported safe reconfiguration protocol,
with learners/catch-up where supported, and tests for each accepted placement transition.

Region loss cannot authorize a minority to force-promote or serve stale mutable state.
Restore quorum from surviving voters or perform an explicit disaster recovery procedure
with a new fenced recovery generation and stated recovery point. Async replicas/backups
require the same explicit restore discipline. If the original quorum cannot authorize
the transition, administrative exclusion of old nodes/storage and client binding to the
new generation are prerequisites; a newly minted token in an isolated clone cannot
magically fence a returning old majority. Independently running offline copies of a
one-voter store violate its single-authority assumption and cannot be detected without
an external coordination authority. Supported restore creates a distinct recovery identity.
Async replicas/backups
have a measured replication lag and potential acknowledged-data loss at disaster restore;
they cannot be advertised as synchronous zero-RPO failover. Restored content and snapshots
must pass identity, session-scope, schema, and durable-placement verification.

“Meta scale” is the target envelope, not a measured result. The implementation must bound
per-node work independently of total fleet sessions: partition control directories;
cache/watch only relevant ranges; multiplex WAL, timers, transport and IO workers; avoid
one OS thread, connection, full graph, or always-running polling loop per idle session.
Root control groups process region/policy changes, not each claim, read, or range access.
Broadcasting every directory update to every node is prohibited by the capacity model.
Partition or split directories before they become a shared metadata hotspot.

Measure aggregate throughput across independent sessions separately from throughput of
one hot session. One session still has a serial preparation/order path, quorum latency,
cross-range coordination, and its minimum publication frontier. Adding claim sublogs or
weakening cross-range atomicity would be a separate domain change requiring its own design.
No database or network implementation should be credited with unmeasured linear scaling.

## 14. Implementation dependency order and gates

| Order | Deliverable | Required evidence before dependent work |
|---|---|---|
| 1 | Typed sequence/epoch/receipt model; serial reducer; canonical codec | Replay and delta equivalence; no authored/lifecycle writer crossing |
| 2 | RAM range, stable-ID indexes, immutable views, quota accounting | Point/scan/traversal oracle; pin/reclamation safety; bounded overload |
| 3 | Segment WAL, crash simulator, single-voter consensus adapter | Every acknowledged mutation survives injected crashes; corruption refuses/repairs |
| 4 | Content receipt barrier, checkpoint manifest, retention coordinator | No committed missing artifact; crash at every install/reference/GC step |
| 5 | Published prefix, read tokens, monitors, cursor subscriptions | No torn transaction/read/seed handoff; lost notifications recover |
| 6 | Replicated session groups and current-term read barriers | Partition, restart, stale leader, lost response, and membership suites |
| 7 | Tracked parallel execution and deterministic fallback | Vary workers/batches/schedules; missing footprints never alter results |
| 8 | Range replay across nodes and logged range cutover | Differential serial state; stale epoch rejection; crash at every cutover step |
| 9 | Archive custody and live-state retirement | Complete historical proof remains retrievable; RAM tracks live work |
| 10 | Regional directory partitions and multi-region placements | Region-loss policy matrix; metadata load bounds; no unsafe promotion |
| 11 | Capacity tuning, idle-group amortization, hotspot mitigation | Reproducible workloads with p50/p99 latency, RSS, IO, recovery, and cost reports |

Permanent fault tests cover every durable/publication boundary, including a crash after
quorum commit before reply, disk-full during manifest install, an artifact-registration
gap, a range dying after partial apply, an old owner returning after cutover, a retained
read root exhausting its budget, a subscriber missing its final hint, and a whole region
partitioning while membership changes. Each recovered state must equal the serial oracle
for its advertised committed prefix. A capacity limit must yield a typed outcome; it must
never become data loss, stale authority, unbounded memory, or an invented success receipt.
