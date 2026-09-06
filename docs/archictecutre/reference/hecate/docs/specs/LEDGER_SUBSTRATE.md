# SPEC: LEDGER_SUBSTRATE — the ledger as a state machine over the shared substrate

Status: ACCEPTED 2026-08-20 (grilling; the ledger-layering settle). Approved
pending a two-lane conflict-and-coherence recon (GRILLING.md, 2026-08-20); the
recon's findings are folded in — the crux **C1** (the cache is a substrate
sibling, not a shared-log *instance*), the residual **CO1** (single-owner scoped
to the sequencer, split in `LEDGER_CORE.md` AC-2), the **CO8** SESSIONS §2
amendment (a whale's materialized state partitions across the session's nodes
while the sequencer/log stays the colocation unit), and the coherence set
(CO2–CO7, CO9). Reconciles `LEDGER_CORE.md` §1's single-owner model with
`MATERIALIZER.md`'s scale-free apply — the **sequencer/apply split**. Research on
file: the ledger as a replicated state machine over a *reusable* shared-log is
Delos/Tango/CORFU/Aurora-proven (Lane A); at-most-once-notify + cursor-recover is
End-to-End-theorem-backed (Lane B). Companion to `LEDGER_CORE.md` (the
machinery), `MATERIALIZER.md` (the apply path), `docs/architecture/LEDGER.md`
(the law), and `WAL.md` / `CONSENSUS.md` / `CACHE.md` / `QUEUE.md` / `FANOUT.md`.

## 1. The shared substrate and its instances

The ledger, the queue, the topics, and the cache are **siblings on the shared
substrate** — the shared *runtime* patterns of `CACHE.md` §0 (a durable log *for
things that have one*, arena fan-out, single-owner-per-shard, HRW placement, N=1
collapse, Driver determinism, the chokepoint law) — **not** four instances of
one log. Two of them are genuine ordered-log instances; two are not, and the
`CONSENSUS.md` §6 boot roster is the discriminator:

- **Ordered-log / standing-writer instances** (roster-classified): the **ledger**
  (its claim log, materialized by `MATERIALIZER.md`); the **queue** (a `WAL.md`
  logical-log client per partition — `QUEUE.md` §2/§5); the **topic FIFO
  sequencer** (a lease+fence standing writer over its ordered stream; the topic
  *registry* is CAS-first — `CONSENSUS.md` §6). Durable topic *subscriptions* are
  themselves queue partitions (`FANOUT.md` §7) — the queue instance, not a
  separate topic log.
- **Substrate siblings that are NOT log instances**: the **cache** — writer-less,
  no durable log, roster-classified **"no durable writer" by absence**
  (`CACHE.md` §0/§9/§10, `CONSENSUS.md` §6); it is a serving-plane **read
  projection the ledger uses** (§4), never a source of truth. The **FANOUT
  router** — a **composed router** that owns no storage (`FANOUT.md` §1/§2),
  delegating durability to the queue and cache primitives.

The primitive's ops and the per-instance semantics are two independent
enumerations, not a paired mapping:

- **Ops of a log instance** (`WAL.md` + `CONSENSUS.md`): `append(record) → index`;
  `read_from(cursor, n)`; `advance_floor(index)` (reclaim); replicate (replica
  count = durability). All log instances use all four.
- **Per-instance semantics layered on top**: ledger = affordance check + claims
  lifecycle; queue = lease / ack / visibility / DLQ; topic-router = routing +
  filter + fan-out (composed); cache = admission / eviction / coherence (no log).

**Consensus lives in one reusable place** (`CONSENSUS.md` §2/§6); a per-log
instance is roster-classified lease+fence (standing writer) or CAS-first and
needs only a fault-tolerant fence, never its own consensus protocol — the Delos
MetaStore split (getting it wrong rebuilds the monolith).

## 2. The sequencer — the write path (single-owner, owns the order)

The ledger core is the session's **sequencer** (`LEDGER_CORE.md` §1/§2):

```
input → affordance check (inform | yield | refuse, over effective state)
      → build WAL record → append (CONSENSUS API) → durable-ack → hand to apply
```

It is **single-owner** because the affordance check evaluates against **effective
state = applied arenas ⊕ pending queue, speculatively applied in input order**
(`LEDGER_CORE.md` §2): a serial, in-order operation that *produces* the total
order every monitor, replay, and effective-state check stands on. Its per-input
cost is the **affordance check over a footprint-indexed speculative overlay**
(`O(footprint)`, not `O(pending)`) **plus** the append — the overlay keeps the
check cheap while apply lags; it is not merely an append, but it is not the
apply either, and it stays comfortably ahead of session demand at the sequencing
tier.

**The applied/pending boundary is the watermark** (`MATERIALIZER.md` §2/§4.6):
`applied arenas` = the materializer's committed prefix (up to `watermark`);
`pending queue` = records appended above it, not yet materialized. The check is
correct **regardless of how far the parallel apply has progressed**.

**Multi-node affordance state (recon G1).** For a *whale* session whose committed
graph partitions across nodes (§3), the sequencer keeps a full **lifecycle /
affordance projection locally** — the small `ClaimSlot.lifecycle` cells and
relations (`LEDGER_CORE.md` §1 already separates the immutable `content` from the
small mutable `lifecycle`; the projection is cheap even at whale scale) — while
the materializer partitions the heavy `content` and claims-graph across nodes. So
the affordance check stays **core-local** (no synchronous out-call — `LEDGER_CORE.md`
AC-2), fed by an **apply → sequencer feedback path** that updates the local
lifecycle projection as the materializer commits each prefix.

## 3. The apply path — the materializer (scale-free, order-preserving)

Applying the acked log into the claims-graph is `MATERIALIZER.md`. It refines the
`LEDGER_CORE.md` §2 step *"apply to arenas, assign sequence → build deltas →
emit"* from a single serial task into a scale-free path (serial `N=1` → within-
node DAG-parallel → multi-node partitioned), selected by derived parameters, no
mode.

- **Input**: the acked records + each claim's **declared footprint** (its
  causal-DAG dependencies as read/write key-sets).
- **Output**: the committed claims-graph + the delta stream, both in **index
  order**.
- **Order-preserving**: DAG edges run smaller-index → larger-index, independent
  claims commute, the parallel/distributed result is bit-identical to serial
  (`MATERIALIZER.md` §7). **The sequencer owns the order; the apply preserves it.**

## 4. The read path — the cache

Reads of ledger state (claim status, testaments, `traverse` queries) ride the
`CACHE.md`, **off the apply path** (`LEDGER_CORE.md` §1). Immutable testaments
and content are the **content-by-hash** cache face — an **opt-in specialization
declared because testaments are immutable** (`CACHE.md` §4.4, key = hash, zero
invalidation). Mutable claim-state is the **general-KV** default face, kept
coherent by invalidation: **within-node** directly on the owning shard's holder
index; **cross-node** over the FANOUT into the co-sharded holder index
(`CACHE.md` §4.1). A miss recomputes from the committed claims-graph — the cache
is a reconstructible projection, never a source of truth.

## 5. The distribute path — the fanout (first cursor consumer, no outbox)

The materializer emits committed deltas in index order (`LEDGER_CORE.md` §5,
refined here: the delta stream now has exactly one cursor — the FANOUT — with
projectors downstream of it, superseding §5's pre-FANOUT "projectors are cursor
consumers of the delta stream"). The **FANOUT** is the **first (and only) cursor
consumer** of the stream — a routing table, not a second outbox, so
`LEDGER_CORE.md` §5's no-outbox law and AC-3 hold literally. It routes each delta
to the downstream consumers, each an ordinary cursor consumer:

- **cache-invalidation** (mutable claim-state → the cache holder index, §4);
- **projections (cursor-recoverable)** — UI bridge, Archivalist mirror, frontier
  service: durable watermarks, resumable, typed RESYNC (`LEDGER_CORE.md` §5).

The ledger and its fanout are **region-local** (`CONSENSUS.md` §7: session groups
never span regions, no cross-region session failover). Cross-region is out of the
ledger's scope: the *content* the deltas reference replicates asynchronously via
the content store, and cross-region / cross-session coordination is the **SIBYL
federation layer** (its own design branch — see GRILLING) — never a live
cross-region delta mirror.

Delivery is per-subscription (`FANOUT.md` §4/§10): cursor-recoverable consumers
take **at-most-once notify** with their cursor as the backstop; **external,
non-recoverable integrations** that cannot cursor our log take **durable
at-least-once** (a QUEUE subscription with retries + DLQ).

## 6. The wake path — in-core deterministic, cross-node backstopped

Waking a parked agent when its blocking claims satisfy has two regimes:

- **In-core (the hot path)** — the satisfaction-monitor **subscriber-index
  dispatch** (`LEDGER_CORE.md` §3, its **own** instance — instantiated separately
  per owner, never shared with the cache holder index) delivers the in-order
  committed prefix to affected monitors, `O(affected)`; a monitor releases on its
  local fixpoint at the current prefix (§3 monotone-cut). Deterministic,
  reliable, and **not a delivery cursor**.
- **Cross-node (the rare path)** — a parked agent or projector on a different
  node than the ledger core is notified **at-most-once** (fast); the correctness
  backstop is **the consumer's own durable cursor over the claim state** — the
  agent inbox's watermark (`AGENTS_RUNTIME.md` §1/§3; `LEDGER.md` §8 "watermarks
  everywhere"). A dropped notify only *delays*: the consumer re-derives
  satisfaction from its cursor at the reconcile floor, never a hang. (This is the
  End-to-End argument; the *mechanism* is a cursor recover, never a poll loop.)

The correctness-critical in-core dispatch is never the general fanout's ephemeral
class and is never coupled to cache-invalidation churn (`LEDGER_CORE.md` §3:
separate subscriber-index instances, no new delivery structure).

## 7. Recovery

Durable truth = the session's **acked log** (`WAL.md`) + checkpoints. Recovery is
the materializer's **prefix-recoverable** replay (`MATERIALIZER.md` §8): replay
the acked-log suffix from the checkpoint watermark, deterministically → the exact
committed prefix. The claims-graph, the cache, and every projection are
reconstructible projections of the log — `LEDGER_CORE.md` §2's "state is a
function of the acked log, exactly" continues to hold **under parallel apply**,
because commit is in-order and replay is deterministic. FAULTS disposition:
derived state is discarded and re-derived; consensus-log corruption rebuilds from
quorum (refuse at N=1).

## 8. The interaction map

```
                 ┌───────────────── sequencer (single-owner, §2) ─────────────────┐
   input ─▶ affordance check(effective = applied⊕pending) ─▶ append ─▶ durable-ack │
                 └──────────────────────────────┬─────────────────────────────────┘
                                                ▼  (the acked, ordered log)
                        MATERIALIZER (§3, order-preserving, scale-free)
                                                │
                        committed claims-graph  +  delta stream (index order)
                             │                          │
                    CACHE (reads §4)          in-core monitors (wake, §6)   FANOUT (first cursor, §5)
                    (miss ⇒ recompute)                                         │
                                                       ┌───────────────────────┼──────────────────┐
                                                cache-inval (§4)   projections (cursor-recov)   cross-region content / external
   recovery: replay the acked log from the checkpoint watermark ⇒ exact prefix (§7)
```

## 9. Invariants

1. The **sequencer owns the order**; every consumer sees a prefix of the one
   deterministic delta stream (monotone-cut, no torn reads — `LEDGER_CORE.md` §3).
2. The **apply preserves the order** (order-preserving parallel/distributed —
   `MATERIALIZER.md`).
3. **No second delivery cursor / no outbox** — the fanout is the first and only
   *external-delivery* cursor on the delta stream (`FANOUT.md` §6); the in-core
   monitor dispatch is a subscriber-index dispatch, not a delivery structure
   (`LEDGER_CORE.md` §3, "no new delivery structure").
4. The correctness-critical **in-core wake path is never coupled** to
   serving-plane churn (separate subscriber-index instances).
5. **Consensus in one reusable place** (`CONSENSUS.md` §2/§6) — a per-log instance
   is roster-classified lease+fence or CAS-first, never carrying its own consensus.
6. **Everything is a reconstructible projection** of the acked log — the log is
   the source of truth; the claims-graph, cache, and projections are derived.

## 10. Acceptance criteria

| # | Criterion | The failure it catches |
|---|---|---|
| LS1 | **Instances**: the ledger, queue, and topic FIFO-sequencer are ordered-log instances, each roster-classified in `CONSENSUS.md` §6 (standing-writer lease+fence, or CAS-first for the topic registry); the **cache** is roster-classified **writer-less by absence** (a serving-plane read projection, not a log instance); no second log engine exists | a bespoke per-subsystem log; the cache mis-cast as a log; the monolith rebuilt |
| LS2 | **Order end-to-end**: sequencer index order = materializer apply order = delta-stream order = every consumer's cursor order (SIM assertion) | a reordering anywhere in the chain |
| LS3 | **Sequencer single-owner**: the affordance-check + append is a single-owner serial path producing the total order; `effective = applied⊕pending` (over the local lifecycle projection for a whale) holds regardless of apply progress and node partitioning | a sharded sequencer losing the order; a synchronous out-call for remote state |
| LS4 | **Apply scale-free + order-preserving**: `MATERIALIZER.md` M1/M4 (bit-identical at any core/node count) hold for the ledger's apply | apply that isn't order-preserving under parallelism |
| LS5 | **No outbox / single delivery cursor**: the fanout is the first and only external-delivery cursor on the delta stream (`FANOUT.md` §6); no outbox (`LEDGER_CORE.md` §5/AC-3) | a second delivery structure resurrected |
| LS6 | **Wake correctness**: an in-core monitor releases deterministically at its prefix fixpoint; a cross-node consumer with the notify dropped still releases via its durable cursor within the derived bound, with no poll loop | a parked agent hung on a dropped notify; a banned poll loop |
| LS7 | **Recovery**: crash ⇒ replay the acked log ⇒ exact committed prefix; the claims-graph/cache/projections reconstruct | recovery to a non-prefix / lost projection |
| LS8 | **Reads off the apply path**: sustained read load rides the cache/projections, never the sequencer mailbox | read load throttling the write order |

## 11. References

Delos (OSDI 2020 / SOSP 2021), Tango (SOSP 2013), CORFU (NSDI 2012), Amazon
Aurora (SIGMOD 2017) — a replicated state machine over a reusable shared log,
with queues/namespaces as sibling instances. End-to-End Arguments (TOCS 1984) —
the wake-path notify/cursor split. Companions: `LEDGER_CORE.md`,
`MATERIALIZER.md`, `CACHE.md`, `QUEUE.md`, `FANOUT.md`, `WAL.md`, `CONSENSUS.md`,
`SESSIONS.md`.
