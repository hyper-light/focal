# SPEC: MATERIALIZER — the ledger's scale-free deterministic apply path

Status: ACCEPTED 2026-08-20 (grilling; materializer research
lanes on file, GRILLING.md: APPLY = Calvin SIGMOD'12, Aria VLDB'20, BOHM
VLDB'15, PWV VLDB'17, Caracal SOSP'21, Raft ATC'14, Flink-ABS 2015; LOG =
FuzzyLog OSDI'18, Tango SOSP'13, Delos OSDI'20/SOSP'21, Scalog NSDI'20, Boki
SOSP'21). The ledger is a replicated state machine materialized by cursor-play
over the shared-log primitive (WAL logical-log + CONSENSUS order); **this spec
is that apply path.** Companion to `LEDGER_CORE.md` (the state it builds) and
`SESSIONS.md` (the shard boundary).

## 1. Role and the scale-free requirement

The materializer replays a session's ordered claim-log and builds the
materialized **claims-graph** — the state that satisfaction monitors, reads,
and the FANOUT delta stream consume. It is the ledger's **write-apply** path;
reads never traverse it (they hit the CACHE / a materialized snapshot), so the
throughput at issue is strictly apply.

**Two ceilings, not one** (the research's central framing): the *order/append*
layer runs at 200K–52M records/s (CORFU/Tango/Scalog/Boki); a *single
serial materializer* doing real per-entry work (parse a claim, walk its causal
DAG, update the claims-graph, reconcile testaments) is a **separate, lower**
ceiling — Tango names it "the playback bottleneck," ~10⁴ ops/s. Delos's "apply
is not the bottleneck" does **not** transfer: their control-plane writes are
trivial, ours are not — so we assume apply is the bottleneck from day one.

**The requirement: a session has no assumed scale.** One design scales
`serial → within-node-parallel → multi-node`, by **derived parameters** (core
count, then node count), never a mode flag (RUNTIME no-modes law).

**Across sessions is free.** Claims never cross a session (domain invariant,
§12/M9): each session is a fully independent log + materializer, so aggregate
apply = Σ_sessions (per-session rate), perfectly linear, zero crossing cost.
The rest of this spec scales **one** session.

## 2. Data model

```rust
struct LogEntry { index: u64, op: ClaimOp, footprint: Footprint }
struct Footprint { reads: KeySet, writes: KeySet }   // declared from the claim's DAG deps
struct Epoch    { lo: u64, hi: u64, entries: Vec<LogEntry> }   // half-open [lo,hi)
struct DagNode  { idx: u64, pending_preds: u32, succs: Vec<u64> }
struct Overlay  { m: DetHashMap<Key, Versioned> }    // per-epoch multiversion write buffer
struct Versioned{ writer_index: u64, value: Value }  // resolved by ATOMIC-MAX writer_index
struct Materializer {
    log:       WalLogicalLog,                 // this session's ordered log (the sequence)
    committed: ClaimsGraph,                   // the materialized state; immutable during an epoch
    watermark: u64,                           // highest index whose whole prefix is committed
    workers:   WorkerPool,                    // bounded, tracked; N = derived core count
    deferred:  Vec<LogEntry>,                 // safety-net losers, replay at next epoch head
}
```

The **committed** graph is written only by the commit phase (§4.6), so during
an epoch's parallel work it is immutable — it *is* the frozen read snapshot, no
copy. The **Overlay** is arena-allocated (RUNTIME §4), sized to the epoch, freed
at commit.

## 3. The epoch state machine

Epoch lifecycle (one owner task per session drives it; workers do the apply):

```
IDLE ──read[w,w+B)──▶ READING ──scan──▶ BUILDING ──roots──▶ SCHEDULING
  ▲                                                              │
  │                                                     drain ready-queue
  └──advance watermark──◀ COMMITTING ◀──all applied──── APPLYING ⇄ CHECKING
                                                     (parallel on N workers)
```

Per-entry lifecycle within an epoch:

```
PENDING ──pending_preds hits 0──▶ READY ──worker picks──▶ APPLYING
   │                                                          │
   │                                              ┌───────────┴──────────┐
   │                                          APPLIED               ABORTED
   │                                     (buffer to overlay)   (undeclared conflict)
   │                                              │                     │
   └──────────────────────────────────◀ decrement succs        defer to next epoch head
```

An entry is **APPLIED** to the overlay (not to committed state); it becomes
**visible** only when the commit phase installs the overlay in log order.

## 4. Layer 1 — within-node DAG-parallel apply

### 4.1 Epoch sizing (derived, no literal)
`B` = the smallest window making the fixed per-epoch barrier a ≤ `target_frac`
share of apply work: `B ≈ barrier_cost / (target_frac × per_entry_apply_cost)`,
clamped by the overlay arena budget. `target_frac` and the costs are physical
anchors measured in the SIM; `B` is recomputed as they drift. Epoch boundaries
are a pure function of `(watermark, B)` — **index-derived, never wall-clock**.

### 4.2 DAG build — one linear scan, not O(B²)
```rust
let (mut last_write, mut readers) = (DetHashMap::new(), DetHashMap::new());
for e in epoch.entries {                       // in log order
    for k in e.footprint.reads  { if let Some(&w)=last_write.get(k){ edge(w,e.idx);} readers.entry(k).push(e.idx); }
    for k in e.footprint.writes { if let Some(&w)=last_write.get(k){ edge(w,e.idx);} for r in readers.take(k){ edge(r,e.idx);} last_write.insert(k,e.idx); }
}
```
Cost O(Σ declared footprint). Every edge runs **smaller-index → larger-index**.
`edge(a,b)` increments `b.pending_preds` and appends `b` to `a.succs`.

### 4.3 Parallel topological drain (Kahn, on the worker pool)
Roots (`pending_preds==0`) seed a deterministic **ready-queue**. Each of the
`N` workers loops: pop a ready entry, apply it (§4.4), then for each successor
decrement `pending_preds` and push it if it hits 0. Two entries ready at once
have **no edge between them** ⇒ (declarations complete) disjoint state ⇒
concurrent writes cannot collide: **disjointness is the mutual exclusion, no
lock on state.**

### 4.4 Overlay read/write
A worker applying `E` reads key `k` = the overlay's highest-`writer_index`-≤-`E`
entry if the DAG made `E` wait for it, else the committed value. It buffers each
write to the overlay via **atomic max-writer_index** (so any WAW resolves to the
higher index — correct log-order last-writer — even under concurrent writes).

### 4.5 Safety net (mandatory — catches incomplete declarations)
As `E` applies, its **actual** reads are checked against the overlay: if `E`
read a key some smaller-index entry wrote but `E` did **not** declare that
dependency, its read was stale ⇒ **deterministic ABORT** of `E`, defer to the
next epoch head. Same inputs ⇒ same abort on every replica; cost ≈ one re-apply
(~7.4% in Aria). Undeclared *WAW* needs no abort (the overlay's max-index
already yields the correct last-writer). Incomplete declarations degrade to
slower, never to corruption.

### 4.6 Commit (in log order, despite out-of-order execution)
The watermark advances only over a **contiguous applied prefix**. When the
ready-queue drains: install the overlay's writes for the contiguous prefix
`[w, first_deferred)` into `committed`, advance `watermark`, emit those entries'
deltas to FANOUT/monitors **in index order**, free the committed portion of the
overlay. Entries with index ≥ a deferred entry are already applied *to the
overlay* but held un-installed until the deferred entry re-applies in the next
epoch — this is what makes recovery a clean prefix (§8).

## 5. Layer 2 — multi-node partitioned apply (Calvin)

When one session outgrows a node, the **state** partitions across nodes (by key,
via SERVING §6 HRW) while the **log stays one ordered instance**. Each node
materializes only its partition of the claims-graph, applying only the entries
that touch its keys, **in the log's order**. Cross-partition claims coordinate
**deterministically with no two-phase commit** — every node already agrees on
the order (Calvin's insight), and the declared `Footprint` **is** the read/write
set Calvin requires. Log reads fan out to all apply-nodes (linear read scaling,
CORFU), so the single ordered log is not the bottleneck. A session's shard thus
spans `1→N` nodes **derived from its size** (the whale-session split the sharding
research names); a small session is `N=1`, identical code path.

## 6. Scale-freedom — one design, derived parameters

| Session size | Materializer shape | Derived parameter |
|---|---|---|
| tiny | serial apply | N_cores = 1 (the DAG drain with one worker *is* in-order serial) |
| hot | within-node DAG-parallel (§4) | N_cores > 1 |
| whale | multi-node partitioned (§5) | N_nodes > 1 |

No mode selects the shape; it falls out of `(cores, nodes)` provisioned for the
session. The `N=1`/`nodes=1` degenerate produces byte-identical state to the
parallel/distributed forms (M4).

## 7. Determinism

Every tie is broken by **log index**, never by thread or wall-clock: epoch
boundaries (§4.1), DAG edge direction (§4.2), overlay max-index (§4.4), commit
order (§4.6). Reads are against the **frozen committed snapshot + overlay**. The
**apply body must be pure** — no wall-clock, no map-iteration-order dependence,
no allocator-address leakage, no float NaN, no data race — a deterministic
*schedule* cannot rescue a nondeterministic *apply step*. The SIM enforces this:
two runs of `(seed, log)` must produce byte-identical materialized state
regardless of core or node count (M1).

## 8. Prefix recoverability

Durable truth = the input log + the last checkpoint. On crash, replay the log
suffix from the checkpoint's `watermark` — same epochs (index-derived), same
DAG, schedule-independent result — recovering to exactly the prefix
`[0, watermark)`. The watermark is the recovery point; because commit is
in-order (§4.6), the recovered state is always a **clean contiguous prefix** —
never a gap, never a half-applied or rolled-back entry (Raft State-Machine
Safety; Flink "consistent prefix").

## 9. Integration

- **WAL** — the session's logical-log is the ordered sequence; the materializer
  is a cursor-consumer of it (never a second copy).
- **CONSENSUS** — provides the order (the sequencer); the per-log floor + seal.
- **SESSIONS §2** — the session is the shard; the log / writer / sequencer stays
  the one colocation node, while the materialized state may partition across the
  session's other nodes (§5; SESSIONS §2 — a reconstructible projection, not the
  colocation unit's durable truth). The sequencer keeps the small lifecycle /
  affordance projection local (`LEDGER_SUBSTRATE.md` §2).
- **CACHE** — reads and monitors read the materialized snapshot, off the apply
  path; a hot claim-state key is cached with cache-layer invalidation.
- **FANOUT** — committed deltas are emitted in index order as the first
  consumer of the delta stream.
- **LEDGER_CORE §3** — the claim-satisfaction-monitor dispatch fires from the
  committed prefix (its own subscriber-index instance).
- **RUNTIME** — the worker pool is a bounded, tracked task set (never untracked
  goroutines); the overlay is an arena; all hashing/tiebreak via the seeded
  Driver.

## 10. Worked example

Epoch `[10,16)`; 3 cores. Entries `(index: op | reads → writes)`:

```
10: post A        | ∅ → {A}
11: post B        | ∅ → {B}
12: validate A    | {A} → {A.val}
13: post C dep A  | {A} → {C}
14: post D        | ∅ → {D}
15: satisfy A     | {A, A.val} → {A.status}
```

**DAG** (from §4.2): edges 10→12, 10→13, 10→15, 12→15. Roots (preds 0): **10,
11, 14**.

**Schedule:** t0 — cores run 10, 11, 14 concurrently (disjoint A/B/D). 10
completes ⇒ 12,13 become ready. t1 — cores run 12, 13 concurrently (disjoint
A.val/C). 12 completes ⇒ 15 ready (its other pred 10 already done). t2 — run 15.

**Overlay:** A@10, B@11, D@14, A.val@12, C@13, A.status@15. Entry 12 reads A →
overlay A@10 (it waited for 10) → correct; 15 reads A.val → overlay A.val@12 →
correct.

**A safety-net abort:** suppose 13 *also* read `B` without declaring it. At
apply, 13's actual read-set includes B; B was written by 11 (index 11 < 13); 13
declared no dep on 11 ⇒ undeclared RAW ⇒ **abort 13**, defer to the head of
epoch `[16,…)`. Commit installs the contiguous prefix `[10,13)` (10,11,12),
advances the watermark to 13; 14 and 15 are applied-to-overlay but **held**
until 13 re-applies next epoch, then 13,14,15 commit in order. Every replica
aborts 13 identically.

## 11. References (load-bearing few)

Calvin (SIGMOD 2012) — deterministic sequencing + declared sets + parallel
apply, no 2PC; multi-node linear. Aria (VLDB 2020) — frozen-snapshot batch +
runtime conflict detection (the safety net). Caracal (SOSP 2021) —
split-on-demand for hot keys; 2.12M txn/s @32 cores. Raft (ATC 2014) —
State-Machine-Safety / prefix apply. Flink ABS (2015) — consistent-prefix
recovery. FuzzyLog (OSDI 2018) / Tango (SOSP 2013) — the sharding/selective-
playback lineage. A declared-dependency claims-DAG is a *stronger* input than
Calvin's declared sets (it gives the conflict edges directly).

## 12. Acceptance criteria

| # | Criterion | The failure it catches |
|---|---|---|
| M1 | **Determinism**: two SIM runs of `(seed, log)` yield byte-identical materialized state, at any core/node count | latent apply-body nondeterminism |
| M2 | **Prefix recoverability**: crash-fuzz at every apply/commit point ⇒ recovered state = a clean contiguous log prefix, never a gap or half-applied entry | out-of-order-execution corruption |
| M3 | **Safety net**: an undeclared read-conflict ⇒ deterministic abort + reschedule, identical on every replica, never a silent stale read | incomplete-declaration corruption |
| M4 | **No modes**: `N=1` core = `N` cores = `N` nodes produce identical state; shape derives from `(cores,nodes)`, never a flag | mode creep |
| M5 | **Within-node scaling**: apply throughput near-linear with cores until conflict density saturates the serial fraction (SIM sweep) | false/absent parallelism |
| M6 | **Multi-node scaling**: a whale session partitions across nodes and scales; cross-partition coordination deterministic, no 2PC | single-session node ceiling |
| M7 | **Commit-in-order**: the watermark advances only over a fully-applied contiguous prefix; no delta/ref observable before its entry commits | premature visibility |
| M8 | **Hot-key**: a hot claim (high fan-in) is detected from cross-epoch in-degree and split (Caracal); unsplit it degrades to serial-for-that-key, never incorrect | a hot-key stall read as correctness loss |
| M9 | **Across-session independence**: no cross-session claim edge exists (domain invariant, boot/CI-asserted); sessions are independent materializers | cross-session coupling / a shared hot log |
| M10 | Every constant (epoch size, worker/node count, split threshold) derived from a physical anchor | magic numbers |

## 13. Test matrix (SIM)

| Test | Asserts |
|---|---|
| Crash-fuzz | M2 at every byte/step of an epoch |
| Determinism replay | M1 across seeds × core/node counts |
| Conflict-density sweep | M5 (0% → 100% inter-claim dependence); records the serial-fraction curve |
| Scale sweep | M4/M6 (1 core → many → many nodes), same-state assertion |
| Hot-key nemesis | M8 (one claim every other depends on) |
| Abort-storm | M3 (fraction of entries with undeclared conflicts; determinism + no corruption) |

**Amendment (2026-08-22, TRACING/MONITORING acceptance):** this subsystem's
chokepoints emit execution spans per `TRACING.md` §3; its chokepoint registry
entries are the span roster (boot-validated; an unregistered emitter fails
startup).
