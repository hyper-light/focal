# SPEC: STORE — the settled-state store (pluggable engine, sharded, watchable)

Status: ACCEPTED 2026-08-22 (rewritten to the COLLECTOR bar — the
corpus spec standard: full data model, state machines, architecture map, networking,
lifecycles, failure matrix, worked example, integration enumeration). The design
decisions were settled piecemeal in-session (GRILLING.md): the pluggable-backend
ruling, the audited `&self` trait v2, the per-shard resolved-timestamp watch with its
five fixes, the two-level foundational sharding with the one-splitter rule, and the
apply-seam split — all user-settled; this rewrite adds depth, it re-decides nothing.
Four NEW settlements this pass carries (flagged in the presentation): all-replicas-
apply, the window's concrete structure, read-repair mechanics, and the boot order.
Research on file: etcd's single-group ceiling; Delos/Borg/Spanner control planes;
FoundationDB (unbundled log/storage, async apply, cheap recovery, pluggable engines);
RocksDB/FASTER/Calvin; NVMe fsync + group-commit math; TiKV/CRDB multi-raft +
RangeFeed. Companions: `WAL.md` (the durable log floor), `CONSENSUS.md` (the meta
tree, the shard-directory amendment already landed, the roster), `MATERIALIZER.md`
(the claims apply strategy), `LEDGER_SUBSTRATE.md` (the claims ledger's read/notify),
`OBJECT_TIER.md` (checkpoints), `TRANSFER.md` (snapshot movement), `SESSIONS.md`
(the whale boundary), `IAM.md`/`REGISTRY.md` (instances of this substrate).

## 1. Role — a WAL is not a store

A write-ahead log makes a mutation stream durable; it is **not** settled, queryable
state. A real store is a log **in front of a storage engine** — the log makes the
engine crash-safe; it never replaces it (etcd runs a Raft WAL *and* a B+tree;
`CONSENSUS.md` §5 rejected that B+tree's implementation and named no replacement —
this spec is the replacement). STORE is the **foundational settled-state substrate**:
DRAM-served, NVMe-durable, checkpointed to the durable plane. One substrate, many
instances — the cluster ledger, the claims-ledger materialization, the registry, the
IAM store all instantiate it; what differs per instance is the apply/read/notify
layer above (§5, §16), never the engine.

## 1a. The whole machine, in plain terms

A bank branch. Every transaction is first written to the **carbon-copy journal**
(the consensus log — nothing is real until it's journaled, and the journal is
copied to other branches before the teller says "done"). The teller works out of
the **working drawer** (the DRAM window — the recent state, fast to hand over the
counter). Overnight, settled paperwork moves from drawers into the **vault** (the
cold backend on NVMe — orderly, indexed, holding everything that's no longer
"recent"). Periodically the vault's contents are bound into **sealed volumes and
shipped to deep storage** (checkpoints to the durable plane) — so a burned-down
branch is rebuilt from the last volume plus the journal's tail, not from memory.
Customers with **account alerts** (the watch) are notified on every posted
transaction, with a periodic "you have seen everything through 3pm" note (the
resolved mark). When one branch gets too busy, the district **splits its ledger
by account range** across two branches and updates the directory in the district
office (the shard split + directory) — and the alert subscriptions follow the
accounts, not the building.

The analogy carries the three least-obvious choices: the journal is the *only*
carbon copy (the vault keeps no second journal — one fsync per transaction, §2);
every branch clerk files the same paperwork in parallel (all replicas apply, §3 —
so when the head teller faints, the next clerk's drawer is already current); and
the alert note says *what you've seen*, not *what happened* (resolved marks make
partial delivery honest, §8).

## 1b. Terms this document uses (reading guide)

- **LSN** — log sequence number: an entry's position in the shard's committed log;
  the store's only clock for ordering (wall-clock appears nowhere in correctness).
- **Window** — the in-DRAM recent tier: committed-but-not-yet-cold state, versioned.
- **Watermark** — the highest LSN whose whole prefix has reached a given state
  (applied, durable-in-backend, checkpointed); recovery replays from one.
- **Cold / aged-out** — state that left the window into the backend; single-version.
- **Snapshot (pin)** — a consistent, immutable view of the cold store at a version;
  cheap (a reference, not a copy).
- **Checkpoint** — an immutable, content-addressed copy of a range's cold state at
  a watermark, sealed to the durable plane; recovery = checkpoint + log tail.
- **Shard** — one contiguous key-range of one instance: its own raft group + engine.
- **Directory** — the meta-group map `key-range → shard → raft group + epoch`; the
  **one splitter** — data placement and (for the claims ledger) apply partitioning
  both follow its cuts.
- **ReadIndex** — the linearizable-read discipline (`CONSENSUS.md` §3): a read is
  served at-or-above a quorum-confirmed commit index; no lease reads exist.
- **Epoch fence** — the stale-route/stale-writer killer: every routed operation
  carries the directory epoch it was computed under; a mismatch is a typed refusal.
- **Resolved(T)** — the watch's high-water promise: nothing ≤ T remains undelivered.

## 2. Data model

```rust
// ---- one instance (an instantiation of the substrate: cluster ledger, claims
// ---- materialization, registry, IAM store…) ----
struct StoreInstance {
    id: InstanceId,               // boot-registered; unregistered fails startup
    profile: InstanceProfile,     // the per-instance declarations (§16): apply
                                  //   strategy, backend kind, read/notify layer
    directory: DirectoryClient,   // §9 — cached + range-watched, epoch-fenced
}
enum BackendKind { Lsm, BTree }   // chosen by DECLARED workload property (§6):
                                  //   write/append-dominated → Lsm;
                                  //   read/watch-dominated  → BTree. A fact about
                                  //   the instance, never a mode.

// ---- one shard: one key-range, one raft group, one engine PER REPLICA ----
struct Shard {
    range: KeyRange,              // contiguous [start, end); range-sharded, never
                                  //   hashed — prefix scans and watches need it
    group: RaftHandle,            // CONSENSUS core; this shard's ordered log.
                                  //   The log IS the WAL — the engine has none (§2b)
    role: ReplicaRole,            // Leader | Follower — EVERY replica runs the full
                                  //   engine below (all-replicas-apply, §3)
    epoch: DirectoryEpoch,        // the routing fence this shard currently owns
    window: Window,               // §4 — the DRAM recent tier
    backend: Box<dyn StorageBackend>,   // §6 — the cold engine (Lsm | BTree)
    watch: WatchHub,              // §8 — leader-only emission; subscriber index
    apply: ApplyDriver,           // §5 — pumps committed entries → window
    marks: Watermarks,            // applied / backend-durable / checkpointed LSNs
}
struct Watermarks { applied: Lsn, durable: Lsn, checkpointed: Lsn }
                                  // invariant: checkpointed ≤ durable ≤ applied ≤
                                  //   commit; the log cannot truncate below
                                  //   min(durable) across consumers (CONSENSUS §5)

// ---- the window (§4): versioned, multi-writer, bounded ----
struct Window {
    index: VersionedIndex,        // key → newest-first version chain (window-only)
    floor: Lsn,                   // oldest LSN still in the window; below → backend
    arena: Arena,                 // entries allocated here; freed by age-out epoch
    epochs: EpochGuard,           // reader/writer epoch protection (§4) — the
                                  //   latch-free discipline; no lock exists
    budget: Bytes,                // derived (§13); crossing it forces age-out
}
struct WindowEntry { key: Key, lsn: Lsn, value: Option<Value>,  // None = tombstone
                     next: Option<ArenaRef> }                   // older version

// ---- the cold backend seam (§6; audited v2 — unchanged from settlement) ----
trait StorageBackend: Send + Sync {
    fn ingest(&self, batch: AgedBatch) -> Result<()>;   // aged-out, LSN-ordered,
                                                        //   immutable; idempotent +
                                                        //   monotonic; NO self-WAL,
                                                        //   no fsync on this path
    fn flush(&self) -> Result<()>;                      // promote crash-durable;
                                                        //   advance the watermark
    fn durable_watermark(&self) -> Lsn;
    fn lag(&self) -> Lag;                               // backpressure input (§10)
    fn snapshot(&self) -> Result<Box<dyn ColdSnapshot>>;
    fn seal_checkpoint(&self, range: KeyRange) -> Result<Checkpoint>;  // range-scoped:
                                                        //   Tectonic seal AND split seed
    fn recover(checkpoint: Checkpoint) -> Result<Self> where Self: Sized;
}
trait ColdSnapshot {
    fn version(&self) -> Lsn;                           // pin = durable watermark at pin
    fn point_read(&self, key: &Key) -> Result<Option<Value>>;  // checksum-verified;
                                                        //   Err(Corruption) → §7 repair
    fn range_scan(&self, r: KeyRange, after: Option<Cursor>)
        -> Result<Box<dyn OrderedIter>>;                // resumable via `after`
}
enum Value { Inline(Bytes), Ref(ContentHash) }          // WIRE_FORMAT inline-vs-ref law

// ---- checkpoints ----
struct Checkpoint { range: KeyRange, watermark: Lsn,
                    content: ContentHash,               // sealed, immutable; the
                    backend_kind: BackendKind }         //   durable plane holds bytes

// ---- the directory (owned by the region meta group; CONSENSUS §1 amendment) ----
struct DirectoryEntry { range: KeyRange, shard: ShardId,
                        group: RaftGroupId, epoch: DirectoryEpoch }
struct DirectoryClient {                                // per instance, per node
    cache: RangeMap<DirectoryEntry>,                    // range-watched (§8's own
    watch: WatchStream,                                 //   mechanism, recursively —
}                                                       //   a client watches only
                                                        //   ITS ranges' entries)

// ---- the watch (§8) ----
struct WatchHub {
    subs: IntervalTree<SubId, KeyRange>,                // range → subscribers
    streams: DetHashMap<SubId, SubStream>,              // per-sub bounded buffer +
                                                        //   cursor + state (§8 SM)
}
enum WatchEvent { Seed(SeedChunk),                      // chunked, bounded (§8)
                  Change { key: Key, value: Option<Value>, lsn: Lsn },
                  Resolved(Lsn),
                  Resync { from: Lsn } }
```

Ownership facts the types carry: a `Shard` is single-owner per replica (one task
set); `window` and `backend` are written only by `apply` (one writer per replica —
concurrency inside the window is the *claims materializer's* N appliers on the
leader of a claims instance, §4, still all under the apply driver's ownership);
`watch` emits on the leader only; `marks` are the only recovery promises.

## 2b. One log, and the placement functions

**The consensus log is the only WAL.** The backend keeps no write-ahead log and
fsyncs nothing on the apply path; on crash it recovers from its checkpoint plus the
committed log tail. One fsync per write — the log's group-commit (`WAL.md` ω) —
never two (TiKV disables the engine WAL for raft-applied data; Delos's engines are
learners above the shared log; this is `LEDGER_SUBSTRATE`'s SMR-over-log shape).

```
key   → shard:    directory.lookup(key) → (shard, group, epoch)     // range map;
                  every op carries `epoch`; mismatch ⇒ typed NotOwner ⇒ re-lookup
shard → replicas: the shard's raft group membership (CONSENSUS reconfiguration
                  records — placement is data, never a side protocol)
reads → replica:  the LEADER only (ReadIndex; no lease/follower reads — CONSENSUS §3)
writes→ replica:  the leader (raft proposal); apply happens on EVERY replica (§3)
splits→ authority: the DIRECTORY — the one splitter (§9); the claims materializer's
                  whale partitions take their cuts FROM it, never beside it
```

## 3. The shard — replica lifecycle and the apply tick

**All replicas apply (NEW settlement, flagged).** Every replica of a shard —
leader and followers — runs the full engine: applies committed entries to its own
window, ages out to its own backend, advances its own watermarks. This is standard
replicated-state-machine practice and it is what buys **fast failover**: a newly
elected leader's state is already materialized (it re-serves reads after one
ReadIndex round, not after a rebuild). The costs are honest: every replica pays
apply CPU and holds the state (that is what a replica *is*); the watch emits on
the leader only (followers maintain no subscriber state — subscribers re-attach on
failover and resume by cursor, §8).

**Replica lifecycle:**

```
BOOT ──local checkpoint? load : (fetch by hash via TRANSFER)──▶ RECOVERING
        │  backend.recover(checkpoint) then replay the committed log tail
        │  from checkpoint.watermark through the apply driver (§5 — the same
        │  pure strategies; recovery IS replay, no second code path)
        ▼
   FOLLOWER ◀──────────── step-down (de-fortified, CONSENSUS §3) ─────────── LEADER
        │ ── elected (raft; state already materialized) ──▶ LEADER: + ReadIndex
        │                                                   serving, + WatchHub,
        │                                                   + split/merge duties
        ├──▶ SPLITTING / MERGING (§9 — leader-driven, crash-stepped)
        └──▶ RETIRED (range handed off by split/merge; group dissolves via
             reconfiguration records; local state deleted AFTER the directory
             CAS makes the successor authoritative — never before)
```

**The apply tick** (every replica, continuously):

```
1. PULL    batch = group.committed_since(marks.applied)      // bounded batch
2. APPLY   strategy.apply(batch, &mut window)                // §5 — in-order or
                                                             //   MATERIALIZER; pure
3. MARK    marks.applied = batch.last_lsn
4. EMIT    if role == Leader { watch.emit(batch) }           // §8 — post-apply,
                                                             //   in LSN order
5. AGE     while window.bytes > budget || window.span > horizon {
               backend.ingest(window.age_out_oldest())       // immutable AgedBatch;
           }                                                 //   floor advances
6. FLUSH   on cadence: backend.flush(); marks.durable = backend.durable_watermark()
7. SEAL    on cadence: checkpoint = backend.seal_checkpoint(range)
           durable_plane.put(checkpoint); marks.checkpointed = checkpoint.watermark
```

Load-bearing orderings: **apply-before-emit** (a watcher never sees an LSN the
replica hasn't applied — read-your-watch, §7), and **age-out-before-budget-breach**
(the window is bounded by construction; a firehose forces aging, and if the backend
lags, §10's backpressure throttles admission — the window can never grow unbounded).

## 3a. Architecture map (what runs where)

```
REGION META GROUP (CONSENSUS per-region group)
  owns: the shard DIRECTORY (range → shard → group + epoch), directory epochs
  serves: directory changes only (cache-miss refills + the range-scoped watch) —
          NEVER on the read/write hot path
        │ placement = reconfiguration records (data, not protocol)
        ▼
PLACEMENT DRIVER (deterministic harness service; HA, advisory — §9)
  watches shard size/load census → proposes split/merge/move through admission
        │
        ▼
DATA SHARDS (per instance, many per region; each = one raft group)
  node A: [shard 7 LEADER]  [shard 12 follower] …     each replica: full engine
  node B: [shard 7 follower][shard 12 LEADER]  …      (window+backend+marks);
  node C: [shard 7 follower][shard 12 follower]…      leader adds ReadIndex
        │                                              serving + WatchHub
        │ checkpoints (content-addressed, range-scoped)
        ▼
DURABLE PLANE (OBJECT_TIER) — sealed checkpoints; recovery + split seeds + reseed
CLIENTS (subsystem chokepoints) — route via their DirectoryClient cache; every op
  epoch-fenced; watches attach to leaders, survive failover by cursor
```

## 3b. Networking, hop by hop

| Hop | Transport | Plane / class | Security & admission |
|---|---|---|---|
| raft replication (append/vote/commit) | UDP datagrams (claims-plane dual stack) | class-0 Control, quorum-critical | envelope keys; admission-reserved (CONSENSUS traffic is never shed) |
| client op → shard leader | hecate-quic session | directed-request-response | Noise identity; the op carries the directory epoch; IAM at the OWNING subsystem's chokepoint (STORE trusts its instance owner's PEP — §16) |
| ReadIndex confirmation round | rides raft's message classes | class-0 | as raft |
| watch delivery (Seed/Change/Resolved) | hecate-quic ordered streams | ordered-log archetype, credit-governed | per-subscriber stream keys; bounded buffer; overflow ⇒ Resync (never backpressure to the shard) |
| directory watch (client cache) | the §8 mechanism on the meta group | ordered-log | range-scoped subscriptions only (a split notifies only affected clients) |
| checkpoint seal / fetch | TRANSFER (chunked, verified, resumable) | bulk (Lane-A passthrough) | content-addressed; OBJECT_TIER admission |
| snapshot catch-up (new/lagging replica) | TRANSFER, then log tail via raft | bulk + class-0 | CONSENSUS §5's slow-vs-dead discrimination governs retention |

Nothing STORE-specific rides a new channel class; every hop reuses a registered
archetype — no PROTOCOL amendment is needed by this spec.

## 3c. Boot order, the registry circularity, self-observation

**Boot order (NEW settlement, flagged).** REGISTRY and IAM are *instances of this
substrate* — and boot validation reads registries. The order that resolves the
circle, per node: WAL → CONSENSUS core → **system instances first** (the registry
store, the IAM store — their shards recover from checkpoints + tails) → the boot
classifier reads FROM the recovered registry → remaining instances and services
validate against it. The registry instance's own profile is **self-hosted but
seed-pinned**: its instance registration ships in the boot image (the one
compiled-in profile), everything else is data. A registry shard that cannot recover
fails the node's boot loudly — no degraded half-registry mode exists.

**Self-observation**: every STORE surface is a chokepoint (`store.apply`,
`store.read`, `store.scan`, `store.ingest`, `store.checkpoint`, `store.split` in
the closed span registry) — it emits spans and measures like every subsystem
(TRACING §4, COLLECTOR §3); the store is not exempt from the plane it underlies.
The collector's own durable lanes ride QUEUE (not STORE), so no observability
circularity exists.

## 4. The window — concrete structure (NEW settlement, flagged)

"FASTER-style epoch-latch-free" is a citation, not a design; the design:

- **A versioned hash index**: `key → newest-first chain of WindowEntry` (arena-
  allocated). An apply inserts at the chain head with its LSN; readers walk to the
  first entry `≤` their read LSN. Point lookups O(chain); chains are short by
  construction (the window holds a bounded LSN span).
- **Ordered access** (scans, watch seeds) uses a window-resident ordered index over
  live keys (maintained by apply; the backend provides ordered cold iteration —
  the merge in §7 aligns the two).
- **Concurrency by epochs, not locks**: readers enter a read epoch; the age-out
  reclaims arena segments only when every reader epoch has advanced past them
  (FASTER's discipline). Writers: **one apply driver per replica owns all
  insertion** — for in-order instances that is one writer (trivially safe); for a
  claims instance the MATERIALIZER's N workers insert **disjoint keys by
  construction** (the DAG schedule's guarantee — disjointness IS the mutual
  exclusion, `MATERIALIZER.md` §4.3), with the same atomic-max-LSN resolution on
  the chain head that the materializer's overlay already defines. No latch exists
  on any read or write path.
- **Age-out** takes the oldest contiguous LSN span, materializes it as an
  immutable `AgedBatch` (newest surviving version per key in the span; superseded
  versions drop — the backend is single-version), ingests, advances `floor`, and
  frees arena segments under the epoch guard.
- **Bounded twice**: by bytes (`budget`, derived §13) and by LSN span (`horizon`,
  derived from the watch/read staleness contract). A reader below `floor` gets the
  typed `TooOld` and re-plans against a snapshot + checkpoint (bounded staleness —
  FDB's model).

## 5. The apply seam (unchanged settlement, stated precisely)

One contract: **consume committed entries in log order → produce state changes
into the window**, off the commit path. Strategies per instance:

- **In-order** (cluster ledger, registry, IAM): each entry applies singly, in LSN
  order. Trivial, serial, correct.
- **MATERIALIZER** (claims instances): epoch the log, schedule by declared edges,
  apply in parallel, whale-partition by the directory's cuts (`MATERIALIZER.md` —
  its Layer-2 partitions take the one splitter's ranges).

**The determinism rule**: applied state is a pure function of the committed log —
same prefix, same bytes, on every replica, at any parallelism (the materializer's
tie-breaks are log-index-based; the in-order strategy is trivially so). This is
what makes all-replicas-apply coherent (replicas can't drift) and recovery = replay
(no redo/undo machinery exists anywhere in this spec).

## 6. The pluggable backend (unchanged settlement)

The seam stays exactly the audited v2 (§2's trait): single-version cold state,
`&self` interior concurrency, no self-WAL, checksum-verified reads, range-scoped
checkpoints, prefix-recoverable. **Backend by declared workload property** — LSM
(RocksDB-shape) for write/append-dominated instances; B-tree (Redwood-shape,
SSD-native, never mmap-whole-DB) for read/watch-dominated ones. **Backend
internals — compaction policy, page management, block formats — are deliberately
out of this spec's scope**: each backend is its own implementation surface behind
the conformance suite (ST2's observational equivalence is the pluggability proof;
a backend that leaks its internals through the trait fails it). What this spec
fixes is the seam and its guarantees, not the engines' interiors.

## 7. Reads — the path, step by step

```
1. ROUTE     (shard, epoch) = directory.lookup(key); op carries epoch
2. FENCE     shard checks epoch — stale ⇒ typed NotOwner ⇒ client re-looks-up
3. LINEARIZE leader runs ReadIndex: quorum-confirm commit index C (CONSENSUS §3;
             no lease reads); wait until marks.applied ≥ C (bounded by apply lag,
             which §10 bounds)
4. PIN       snap = backend.snapshot()            // version = durable watermark V_b
5. MERGE     read window[V_b+1 ..= C] over snap:  // per key: newest window version
             point: window chain first, else snap.point_read
             scan:  k-way merge of window ordered index over snap.range_scan
                    (window wins per key; tombstones delete)
6. VERIFY    snap reads are checksum-verified; Corruption ⇒ REPAIR (below)
```

**Read-your-watch** (the coupling, stated): a watcher that received
`Change{lsn: L}` and then reads observes state ≥ L — guaranteed because the leader
emits only post-apply (§3's ordering) and serves reads at `applied ≥ C ≥ L`.

**Read-repair (NEW settlement, flagged).** A `Corruption` from the backend is a
typed event, never a served value. Repair, in order: (1) re-read the key from the
**local checkpoint** if the key's LSN ≤ `checkpointed` (content-addressed, verified
by hash); (2) otherwise re-materialize by **targeted replay** — the committed log
holds the truth (replay the key's range from the last checkpoint; expensive,
counted, correct); (3) a replica whose backend corrupts *systemically* (repair
rate over the derived alarm line) demotes itself: it discards the backend,
re-runs RECOVERING from checkpoint + tail, and the raft group's other replicas
carry reads meanwhile. There is no cross-replica byte-fetch path — replicas
converge by replay, never by copying each other's possibly-corrupt state.

## 8. The watch (settled semantics + the mechanics)

Leader-emitted, per-shard, resolved-timestamp streams. The subscriber index is an
interval tree (`WatchHub.subs`); emission is the apply tick's step 4 — in LSN
order, range-filtered per subscriber, into each subscriber's **bounded** stream
buffer.

**Per-subscriber stream state machine:**

```
ATTACH(Seed) ──▶ SEEDING: snapshot pin at V; stream SeedChunks (bounded, resumable
   │             by cursor) ──complete──▶ emit Resolved(V) ──▶ LIVE
ATTACH(FromLsn L) ── L ≥ retained floor ──▶ CATCHUP(replay window/backend deltas
   │                                        L+1..) ──▶ LIVE
   │              └─ L below retention ──▶ send Resync{from} ⇒ client re-ATTACHes(Seed)
LIVE: Change* + Resolved (cadence derived; the mark = the shard's commit watermark —
   │   no new heartbeat: it rides emission or the existing liveness fabric)
   ├── buffer overflow (slow consumer) ──▶ send Resync ⇒ drop buffer ⇒ SEEDING
   │   (the shard NEVER blocks on a subscriber)
   └── leader failover ──▶ stream breaks; client re-ATTACHes(FromLsn last-Resolved)
       to the new leader — no gap, no dup (cursor semantics; ST-watch tests)
SPLIT at key b, index I: parent emits Resolved(I) for [b,∞); directory notifies
   (range-scoped); client re-attaches to the child (FromLsn I) — continuity across
   the cut (§9)
```

Ordering promise, precisely: total within a shard; per-key global (a key lives in
one shard); cross-key cross-shard order reconstructed from `min(Resolved)` across
covering shards. Watches are not per-event linearizable; the resolved mark is the
consistency handle.

## 9. Sharding (settled two-level design + the crash-stepped protocols)

The region meta group owns the **directory**; data shards own data. Range-sharded,
never hashed. Clients cache + range-watch the directory (a split notifies only the
clients routing into it — the Endpoints-N² lesson). The **placement driver** is
advisory and HA: it proposes; the directory CAS + epoch fence are the authority;
its death stalls optimization, never correctness.

**Split (leader-driven, crash-stepped; every step idempotent):**

```
S1 parent commits SplitIntent{at b, child} in its OWN log at index I   // the
   (replicas learn the split deterministically — it is applied state)  // atomic pt
S2 seed = backend.seal_checkpoint([b, end))                            // metadata-
   (LSM: hard-link immutable files; BTree: COW root — no bulk copy)    // fast
S3 child raft group forms (reconfiguration records); child replicas
   recover(seed) then replay parent log tail for [b,end) past I
S4 directory CAS: [a,end)→parent  ⇒  [a,b)→parent + [b,end)→child, epoch++
S5 parent retires [b,end): refuses with NotOwner(new epoch); watchers
   re-attach to the child (§8); parent's cold state for [b,end) deletes
   AFTER S4 commits — never before
crash at S1..S3: the intent is applied state — replicas/new leaders resume the
   step; the directory still routes everything to the parent (no gap, no overlap)
crash at S4: CAS is atomic at the meta group — either old or new routing, never
   both; S5 is idempotent cleanup
```

**Merge (rare, driver-scheduled, briefly freezing):** M1 both leaders commit
MergeIntent; M2 the source seals its range checkpoint and **freezes** (refuses
writes with a typed retryable — the stated cost); M3 the absorber ingests the
checkpoint + the source's tail; M4 directory CAS (epoch++); M5 source group
dissolves. Crash-stepped identically; the freeze window is bounded and measured
(ST9's fuzz covers every boundary).

**Per-ledger instantiation** (unchanged): cluster ledger = region-parented,
one log per shard; claims ledger = session-parented, ONE ordered log per session
with the whale's *state and apply* partitioned along the directory's cuts
(`SESSIONS.md` §2, `MATERIALIZER.md` §5) — the one-splitter law, ST11.

## 10. Backpressure

`backend.lag()` (unflushed bytes + ingest queue + compaction debt) crossing its
derived bound throttles **that shard's admission** (typed retryable at the
proposal edge — never a silent queue): the window cannot outrun the backend, and
apply lag (step 3's wait in §7) stays bounded. Per-shard and a backstop — scale
comes from shards, not from the throttle. The claims instance inherits the same
guard below the MATERIALIZER (which has its own epoch pacing above).

## 11. Failure & recovery matrix

| What dies | What is lost | Counted where | What recovers, from where |
|---|---|---|---|
| A follower replica | nothing durable (its window) | replica-restart counter | RECOVERING: local checkpoint + log tail replay; rejoins; retention holds the tail per CONSENSUS §5's slow-vs-dead rule |
| The leader | in-flight proposals (raft's contract); watch streams break | election counter; subscriber Resync/re-attach counts | a follower with materialized state elects; ReadIndex resumes after one round; watchers re-attach by cursor — no gap, no dup |
| A whole shard's quorum | availability of that range until quorum restores | typed Unavailable at routing | replicas recover from checkpoints + tails; the range's data is durable (log + checkpoints); no other shard affected |
| A node (many replicas) | those replicas' windows | node-loss lifecycle events | each group elects elsewhere (materialized followers); the scheduler re-places replacement replicas (checkpoint fetch via TRANSFER + tail) |
| Backend corruption (spot) | one read's latency | `Corruption` + repair counters | §7 repair: checkpoint re-read or targeted replay |
| Backend corruption (systemic) | one replica's cold state | repair-rate alarm | self-demote → full RECOVERING; the group serves meanwhile |
| The directory's meta group | routing *changes* stall (splits/moves); routing itself keeps working from client caches | meta-group availability signals | CONSENSUS recovery; caches serve stale-but-fenced routes (an actually-moved range refuses with NotOwner and the client waits — correctness holds, mobility stalls) |
| A checkpoint object | nothing (it is one of several) | durable-plane scrub counters | re-seal from the live backend at next cadence; older checkpoint + longer tail covers recovery meanwhile |
| The placement driver | optimization (splits/merges/moves stall) | driver liveness | re-summon; hotspots persist until then — priced as performance, never correctness |

The invariant across rows: **durable truth = the committed log + sealed
checkpoints; every window is disposable; every recovery is replay** (one code
path, §3) — nothing is ever reconstructed from another replica's mutable state.

## 12. Refusal & loss taxonomy (closed; an uncategorized refusal is a bug)

`NotOwner(epoch) | TooOld | Retryable(lag) | Frozen(merge) | Unavailable(quorum) |
Corruption | Resync | SeedTooLarge(chunked-continue)` — every one typed, counted,
and carried on the wire as the standard error classes; CI walks every refusal path
into exactly one category (ST13). STORE loses nothing by design — the only "loss"
class is a subscriber's buffer overflow, which converts to `Resync` (a re-seed,
not a gap).

## 13. Derived constants (formula + anchors; definition sites in-code)

| Constant | Formula | Anchors |
|---|---|---|
| Window budget (bytes) | replica memory share × derived window fraction | node memory, instance profile |
| Window horizon (LSN span) | staleness contract ÷ measured apply rate | declared read/watch staleness, apply rate |
| Age-out batch size | ingest-efficiency knee of the backend (measured) | backend ingest curve |
| Flush cadence | durability-lag bound ÷ measured flush cost | WAL flush cost (µs PLP / ms consumer), lag bound |
| Checkpoint cadence | recovery-time bound ÷ measured replay rate | replay rate, RTO target |
| Split thresholds | over {bytes, write, read, watch} vs per-shard capacity | measured shard capacity |
| Backpressure bound | window budget − max in-flight epoch | window budget, epoch size |
| Watch buffer / Resolved cadence | subscriber-class delivery budget; consumer-declared freshness | delivery budget, HEALTH freshness |
| Repair alarm line | derived fraction of read rate | measured corruption base rate |
| Replica count | the environment-derived durability default (QUEUE §4's same derivation) | failure-domain tree |

## 14. Worked example — one placement record, end to end (+ two failures)

The scheduler commits a placement update `P: pod-9 → node-B` to the cluster
ledger's shard `S3` (range covers `placement/pod-9`).

1. **Route/commit**: the scheduler's chokepoint looks up `S3` (cache), sends the
   proposal with epoch 12; the leader proposes; raft commits at LSN 4407 (one
   group-commit fsync, quorum-replicated).
2. **Apply, everywhere**: all three replicas' apply ticks pull 4407, the in-order
   strategy inserts `P@4407` at the window chain head; `applied = 4407` on each.
3. **Watch**: the leader emits `Change{P, 4407}` to the two subscribers whose
   ranges cover it (the autoscaler's controller; a dashboard query cursor);
   `Resolved(4407)` follows on cadence. The controller reads back and sees ≥ 4407
   (read-your-watch).
4. **Age-out**: minutes later the window's oldest span (…4407…) ages out;
   `AgedBatch` ingests into the B-tree backend; `flush` advances `durable`; the
   next `seal_checkpoint` covers it; the log's floor may advance past 4407.
5. **Failure A — leader dies at step 3**: follower `n2` (already applied 4407)
   elects; the controller's stream breaks; it re-attaches `FromLsn(4407)` and
   receives nothing it hasn't seen — no gap, no dup; a read after one ReadIndex
   round returns `P`.
6. **Failure B — split lands mid-watch**: `S3` splits at `placement/pod-5`,
   intent at LSN 4410. The parent emits `Resolved(4410)` for the upper range; the
   directory (epoch 13) notifies exactly the clients routing there; the
   controller re-attaches to the child `FromLsn(4410)`; a stale write with epoch
   12 gets `NotOwner` and retries cleanly at the child. `P`'s history is intact
   across the cut — the child seeded from the parent's checkpoint + tail.

## 15. Laptop degenerate

`N=1`: one node; every shard's group is 1-voter (quorum = own durable append —
CONSENSUS §8, crash-injection-gated at N=1 by name); root ≡ region ≡ one meta
group; one replica applies (all-replicas-apply degenerates to it); the directory
has one entry per instance until a split threshold — derived from laptop anchors —
is ever crossed. Same code, zero modes.

## 15a. Integration (every companion touchpoint)

- **WAL** — the shard log rides WAL logical logs under the raft set's record
  kinds; group-commit ω; STORE adds no record kind (raft's are registered).
- **CONSENSUS** — the core per shard; ReadIndex; the meta tree owns the directory
  (§1 amendment, landed); data-shard writers are leader-fused in the §6 roster
  (landed); retention per §5's checkpoint-retention invariant.
- **MATERIALIZER** — the claims instance's apply strategy; its whale partitions
  take the directory's cuts (ST11).
- **LEDGER_SUBSTRATE / CACHE / FANOUT** — the claims ledger's read/notify layer is
  theirs, unchanged; STORE is the settled engine beneath it. The cluster ledger's
  read/notify is THIS spec's window + watch.
- **OBJECT_TIER** — checkpoints are ordinary content-addressed durable-plane
  objects; ledger-referenced seal manifests are already a GC root class there.
- **TRANSFER** — checkpoint/seed/snapshot movement; never a bespoke path.
- **SESSIONS** — the whale boundary; the session's colocation node keeps the
  sequencer/log; materialized state partitions per the directory (§2 amendment,
  landed 2026-08-20).
- **SCHEDULER** — replica placement + replacement via ordinary admission; the
  placement driver is a harness service under the autoscaler doctrine.
- **IAM / REGISTRY** — instances of this substrate (B-tree profile); their PEPs
  gate reads at their serving edges — STORE itself trusts its instance owner's
  chokepoint (one authorization layer, not two).
- **TRACING / COLLECTOR** — the store's chokepoints emit spans/measures (§3c);
  `store.*` spans are in the closed registry.
- **HEALTH** — lag, repair-rate, election, and availability signals ride the
  plane content-free; AbsenceIs covers a stalled shard's silence.
- **FAULTS** — crash-fault model; every state machine here is crash-stepped and
  belongs in the SIM's nemesis matrix (§17's tests name them).

## 16. Per-instance profiles (what varies, closed)

| Instance | Backend | Apply | Read/notify above |
|---|---|---|---|
| Cluster ledger | BTree | in-order | this spec's window reads + watch |
| Claims materialization | Lsm | MATERIALIZER | CACHE projection + FANOUT wake (`LEDGER_SUBSTRATE`, unchanged) |
| Registry | BTree | in-order | its resumable watch (= §8's mechanism) |
| IAM store | BTree | in-order | its serving-edge reads + Watch feed |

A profile is declared at instance registration and boot-validated; nothing else
about the engine varies per instance.

## 17. Acceptance criteria

| # | Criterion | The failure it catches |
|---|---|---|
| ST1 | Every mutable-state read is served by the engine (window+backend), never by log replay at read time (architecture test) | the WAL-is-a-store conflation |
| ST2 | **Backend observational equivalence**: LSM and B-tree byte-identical through the trait, including under concurrent ingest+read (differential conformance) | nominal pluggability |
| ST3 | **State = pure function of the log**: crash-fuzz at every apply/age/flush/seal point ⇒ recover + tail-replay identical; N=1 named | nondeterminism; buffer-and-lie |
| ST4 | **No self-WAL**: the only fsync is the shard log's (architecture test) | double-logging |
| ST5 | **No writer blocks a reader**: reads never stall on apply/ingest; the claims window sustains N disjoint appliers (differential vs serial oracle) | the `&mut` serialization class |
| ST6 | **Bounded lag**: firehose ⇒ `lag()` ⇒ admission throttle; window bounded by budget AND horizon under any load | unbounded window/replay |
| ST7 | **Corruption never served**: bit-flip injection ⇒ typed `Corruption` ⇒ §7 repair succeeds; systemic ⇒ self-demote + group serves | silent rot; repair-by-copying-corruption |
| ST8 | **Epoch fencing**: a stale route/writer is refused at every op class under split/merge/move fuzz | routing split-brain |
| ST9 | **Split/merge crash-stepped**: kill at every S/M step ⇒ exactly-once cutover, no gap/overlap window, watch continuity across the cut | torn splits; lost watchers |
| ST10 | **Watch exactness**: seed↔stream no-gap/no-dup; failover re-attach by cursor exact; slow consumer ⇒ Resync, shard never blocks; Resolved rides existing signals (no new heartbeat) | watch races; heartbeat creep |
| ST11 | **One splitter**: a whale's data-cuts ≡ its apply-cuts ≡ the directory's (architecture test) | dual splitters |
| ST12 | **All-replicas-apply failover**: leader kill ⇒ new leader serves after one ReadIndex round with pre-materialized state (measured bound); replicas never diverge (ST3 corollary, cross-replica differential) | rebuild-on-failover; replica drift |
| ST13 | **Refusal taxonomy closed** (§12): CI walks every refusal path into exactly one category | silent refusals |
| ST14 | **Boot order + circularity**: system instances recover before the classifier reads them; a failed registry shard fails boot loudly; the seed-pinned profile is the only compiled-in registration | half-registry boot; hidden bootstrap state |
| ST15 | Laptop ≡ fleet: N=1 identical semantics per client-visible API (differential); every §13 constant derived at its definition site | modes; magic numbers |

## 18. Test matrix (SIM)

| Test | Asserts |
|---|---|
| Differential backend | ST2 |
| Crash-replay fuzz | ST3/ST4 (every tick step, both backends, N=1 and N=3) |
| Concurrency probe | ST5 (reads under ingest; N disjoint appliers vs oracle) |
| Firehose | ST6 (throttle, bounded window, bounded §7 step-3 wait) |
| Bit-flip inject | ST7 (spot repair; systemic self-demote; group availability) |
| Split/merge fuzz | ST8/ST9 (kill every step; epoch fence sweep; watch continuity) |
| Watch suite | ST10 (seed/catchup/resync/failover/slow-consumer; resolved-mark cadence) |
| Whale alignment | ST11 (directory cuts vs materializer partitions, structural) |
| Failover ratchet | ST12 (leader-kill → first-read latency bound; cross-replica state differential) |
| Refusal walk | ST13 |
| Boot-order sweep | ST14 (registry-first recovery; failed-registry loud abort) |
| Laptop parity | ST15 |

## 19. References (load-bearing few)

FoundationDB SIGMOD'21 (unbundled log/storage; async apply off the commit path;
recovery finds the log end; pluggable `ssd`/`redwood`); Delos OSDI'20/SOSP'21
(RocksDB behind a shared log; learners above the log); TiKV/CRDB (multi-raft
ranges; engine-WAL disabled under raft; RangeFeed resolved timestamps); FASTER
SIGMOD'18 (epoch protection; hot/cold split); Calvin/Aria (deterministic parallel
apply — via `MATERIALIZER.md`); etcd (the single-group ceiling and the watch
guarantees its docs formalize); Percona fsync + group-commit math (the ω anchors);
Spanner OSDI'12 (directories/movedir — the split/placement shape). Companions:
`WAL.md`, `CONSENSUS.md`, `MATERIALIZER.md`, `LEDGER_SUBSTRATE.md`,
`OBJECT_TIER.md`, `TRANSFER.md`, `SESSIONS.md`, `SCHEDULER.md`, `IAM.md`,
`REGISTRY.md`, `TRACING.md`, `COLLECTOR.md`, `HEALTH.md`, `FAULTS.md`.

**Amendment (2026-08-22, TRACING/MONITORING acceptance):** this subsystem's
chokepoints emit execution spans per `TRACING.md` §3; its chokepoint registry
entries are the span roster (boot-validated; an unregistered emitter fails
startup).
