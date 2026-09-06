# SPEC: ARCHIVE — the archive plane (session memory: ingest, store, baselines, the Archivalist's machinery)

Status: presented for acceptance 2026-08-22. Settles the baseline-store exchange
(GRILLING.md): session-scoped reference curves (user), lineage-only inheritance +
shipped defaults (user), the no-duplication split (fitting as a query-class
consumer of the collector; no second points store), the archive plane as its own
bounded component (user — the no-retrofit correction), infrastructure-not-agent
data paths (user), and the Archivalist's completed duty set (admission, refits,
pruning, seed export, query provision/facilitation — user). Research receipts on
file: per-entity baselines as universal practice (Datadog/CloudWatch/Azure/
Elastic `partition_field_name`), cold-start practice (suppression windows,
shipped structural priors, Steiner's population-model + LC-CUSUM default-
distrust), and the poisoning literature (ANTIDOTE 10%-chaff→8×-evasion, the
boiling-frog defeat of filter-then-retrain, Kloft–Laskov's bounded-influence
critical ratio, Cretu's micro-model voting). Companions: `MONITORING.md` (the
Scribe's outbound; the teardown gate — amended here), `COLLECTOR.md` (the query
surface this plane consumes), `HANDOFF.md` (the curves' consumer; exclusion
sources), `LEDGER.md` §8 (the retired-proof archive — the same store, its
infrastructure specified here), `SESSIONS.md` (colocation, archive-finalize),
`STORE.md`/`QUEUE.md`/`WAL.md`/`OBJECT_TIER.md`/`TRANSFER.md` (the substrate),
`AGENTS.md` (the Archivalist).

## 1. Role

Every session accumulates memory that is **not** telemetry and **not** work
proof: the Scribe's judgment-bearing records (narration, assessments, final
flushes), the distilled **reference curves** that tell detection what normal
looks like for this workload, and the sealed history that survives the session
down its lineage. The archive plane is that memory's infrastructure — a bounded
per-session component with its own purpose, retention class, and owner. It is
**not** the collector (telemetry: lossy-tolerant, expiring, operational), **not**
the ledger (work proof: complete, never sampled), and **not** the knowledge
plane (work content). The **Archivalist administers it and never carries it**:
every byte moves through deterministic services; the agent supplies judgment and
answers questions.

Three laws govern the plane:

- **Session-scoped, lineage-inherited**: nothing learned here ever crosses a
  user or session fence except *down the lineage tree* (single-owner by
  construction). The blast radius of any poisoning is the poisoner's own
  baselines — structural, not policy.
- **One collection path**: the raw performance numbers live in the collector
  (they are telemetry; detection runs on them live). This plane keeps
  **sufficient statistics and documents** — never a second copy of the points.
- **Infrastructure, not agent**: teardown, ingestion, fitting, and serving are
  service work with service latencies; the Archivalist's judgments arrive as
  claims and configure the machinery.

## 1a. The whole machine, in plain terms

A hospital's medical-records department. Ward monitors (the collector) stream
vitals continuously — that's operational, and nobody files every heartbeat. The
**records office** (the archive store) keeps what must outlast the shift: the
attending's written assessments (the Scribe's records), the discharge summary
(the final flush — and the patient may not leave until the office confirms
receipt: the teardown ack), and each patient's **chart baselines** ("her resting
heart rate runs low — that's normal *for her*"), distilled from the monitors'
history by the records staff (the fitting service), not by re-wiring the
monitors. The **medical-records officer** (the Archivalist) never carries
charts down hallways — clerks do that; the officer decides what enters the
record ("that week the patient was post-op — exclude it from her baseline"),
orders re-summaries, signs transfers of the chart when care continues elsewhere
(the lineage seed), and answers every "what does her history show?" question.
And one hospital's charts never calibrate another hospital's monitors — a new
patient with no history is judged against conservative textbook ranges (shipped
defaults) until her own chart fills in.

The analogy carries the four load-bearing choices: baselines are *per patient*
(session-scoped — the poisoning receipt made this structural); the office
**distills** from the monitors rather than duplicating them (sufficient
statistics, one collection path); the officer is **never the courier**
(infrastructure-not-agent); and exclusions are judged from **events, not from
the chart itself** ("post-op week" — a fact — never "this reading looks odd,"
which is how slow poisoning defeats naive filters).

## 1b. Terms this document uses (reading guide)

- **Reference curve** — the fitted record of *normal* for one (agent-type,
  task-class) pair in this session: per-signal, per-difficulty-bucket robust
  location/scale. What detection's risk adjustment compares against.
- **Sufficient statistics** — the bounded running accumulators (histograms,
  counts) from which curves are fitted; kilobytes, checkpointed, never raw
  points.
- **Sub-window** — one of K disjoint time slices of accumulated statistics; fits
  are computed per sub-window and **voted**, so no single slice — however
  poisoned — moves the curve more than 1/K.
- **Frozen version** — the previously published curve document. Every refit is
  compared against it; drift beyond a derived bound is withheld for judgment.
  The boiling-frog stopper.
- **Exclusion mark** — a time-span excluded from fitting, authored from
  *lifecycle facts* (a handoff, an incident, a boot) or Archivalist provenance
  judgment — never from the fitted model's own opinion of the data.
- **Lineage seed** — the session's final curves, sealed at archive-finalize for
  descendants of the same lineage to inherit as their v0.
- **Shipped defaults** — conservative structural priors versioned with the
  harness release (logical clamps, wide bounds); learned from nobody.
- **The ingest ack** — the archive ingest service's durable receipt; the thing
  pod teardown actually waits for.

## 2. Data model

```rust
// ---- the documents (content-addressed; the archive store's residents) ----
enum ArchiveDoc {
    CurveVersion(CurveVersion),
    TrajectorySummary(Summary),     // the Scribe's structured assessment of a
                                    //   span of its primary's life
    FinalFlush(FlushRecord),        // death/teardown: the drained ring tail +
                                    //   the death report (MONITORING §6)
    NarrationRecord(Narration),     // the Scribe's narrative record
    LineageSeed(SeedManifest),      // the sealed curve set + provenance chain
    RetiredProof(..),               // LEDGER §8's retired claim proofs — the
                                    //   SAME store; kinds coexist, purposes don't blur
}
struct CurveVersion {
    pair: (AgentTypeId, TaskClassId),
    version: u32,
    curves: DetHashMap<SignalId, SignalCurve>,
    fit: FitMeta,                   // window span, per-sub-window sample counts,
                                    //   votes cast/discarded, estimator id,
                                    //   exclusions applied (by mark ref)
    prev: Option<ContentHash>,      // the frozen predecessor — the version CHAIN
    drift: DriftReport,             // computed vs prev (§6) — reviewable, always
    provenance: Provenance2,        // Fitted | LineageSeed{ancestor} | ShippedDefault
}
struct SignalCurve {                // per difficulty bucket (the risk-adjustment
    buckets: Vec<BucketParams>,     //   input HANDOFF §4 consumes):
}                                   //   robust location (median) + scale (MAD) +
                                    //   sample count per bucket

// ---- the ingest service (deterministic; the ONLY landing path) ----
struct ArchiveIngest {
    lane: QueueInstance,            // its OWN QUEUE-over-WAL lane, session durable
                                    //   scope, at-least-once, env-derived durability
                                    //   — NOT an op-log lane (different plane)
    applier: IngestApplier,         // lane consumer: doc → store bytes → index row
}
// The ACK that gates pod teardown = the lane's durable enqueue ack (µs–ms,
// service-grade, always available) — NEVER an agent turn (MONITORING §6 amended).

// ---- the store + index ----
struct ArchiveStore;                // content-addressed bytes in OBJECT_TIER's
                                    //   archive storage class, session scope
                                    //   (LEDGER §8's proofs already live here)
struct ArchiveIndex;                // a session-scoped STORE.md instance
                                    //   (B-tree profile, in-order apply — a new
                                    //   row in STORE §16's profile table):
                                    //   (kind, agent, domain, lineage, time) → refs

// ---- the fitting service (deterministic; judgment-free) ----
struct Fitter {
    stats: DetHashMap<(PairId, SignalId), SubWindowRing>,
    ring_k: usize,                  // K disjoint sub-windows (derived, §12)
    marks: Vec<ExclusionMark>,      // active exclusions — consulted BEFORE
                                    //   accumulation, never after
    cursor: QueryCursor,            // its read position over the collector's
                                    //   session-scoped series (query class)
    own_log: WalLogicalLog,         // checkpoint {cursor, stats, last versions}
}                                   //   — ONE record; recovery = resume (the
                                    //   accepted client-owned pattern)
struct SubWindowRing { windows: RingBuf<StatsWindow, K> }
struct StatsWindow {                // bounded: per difficulty bucket, one
    span: TimeSpan,                 //   ExpHistogram (the shared library type —
    per_bucket: Vec<ExpHistogram>,  //   machinery reused, instance owned here)
}
struct ExclusionMark {
    span: TimeSpan, agent: Option<AgentUid>,
    reason: ExclusionReason,        // closed: Handoff | Incident | Boot | Probe |
                                    //   ArchivalistJudged{claim: ClaimRef}
}                                   // lifecycle reasons auto-author from ledger/
                                    //   lifecycle facts; the judged kind carries
                                    //   the Archivalist's CLAIM — agents act as claims

// ---- the Archivalist's control surface (judgment IN, claims-shaped) ----
enum ArchivalistDirective {         // each arrives as an ordinary claim; the
    Exclude(ExclusionMark),         //   services consume the resulting directives
    TriggerRefit(PairId),
    Prune { doc: ContentHash, reason: PruneReason },
    ApproveSeedExport(SessionUid),
    ApproveDriftPublication { pair: PairId, version: u32 },
}
```

Ownership facts: the ingest lane and applier, the fitter, and the index are
**session home services** (colocation unit — a SESSIONS §2 addition, §15). The
Archivalist holds no archive bytes in context, ever; its directives are claims;
its reads go through the query surfaces (§9).

## 3. Architecture map (what runs where)

```
POD (per agent)                       SESSION COLOCATION UNIT
  Scribe ── judgment records,           ARCHIVE INGEST (QUEUE lane + applier)
  final flush ── vsock flow ──▶ host ──▶  │  durable enqueue ── ACK ──▶ gates
                                          │                            pod teardown
                                          ▼
                                        ARCHIVE STORE (OBJECT_TIER archive class,
                                          session scope) + ARCHIVE INDEX (STORE
                                          instance, B-tree, in-order)
                                          ▲                    ▲
COLLECTOR (its own plane, untouched)      │ curve versions     │ reads
  session-scoped series ◀── query ── FITTER (stats + sub-window fits + frozen-
  (read_series, session grant)            version gate; own_log checkpoints)
                                          ▲ directives (claims)
                                        ARCHIVALIST (judgment + query facilitation
                                          — never a data hop)
  DETECTION SUBSTRATE (same unit) ── reads CurveVersions at detector birth +
                                     watches for refits (index watch)
LINEAGE: at archive-finalize, the approved LineageSeed seals into the archive;
  a descendant session imports it at create (TRANSFER, by hash) as its v0 —
  the ONLY cross-session flow, and it is lineage-down, single-owner.
```

## 3b. Networking, hop by hop (zero new channels)

| Hop | Rides | Class | Security |
|---|---|---|---|
| Scribe → ingest | the Scribe's existing vsock flow → the ingest lane | the lane's QUEUE delivery (at-least-once) | the Scribe's per-workload flow key (its records are *provably its own* — the flow-key landing is what makes trajectory provenance attributable) |
| ingest ack → init | the control channel (existing) | Control class | the teardown gate signal |
| fitter → collector | hecate-quic query (the public surface) | query class, off ingest | session-scoped `observability` grant; per-principal budget |
| fitter/applier → store | OBJECT_TIER write path (bulk via TRANSFER) | bulk | content-addressed; archive class |
| index ops | the STORE instance's own raft | class-0 | its shard's discipline |
| detection ← curves | colocated index read + watch | STORE watch | session scope |
| lineage-seed import | archive fetch by hash | bulk/TRANSFER | provenance chain verified (§7) |
| Archivalist directives | ordinary claims | the claims plane | agents act as claims |
| Archivalist queries | the archive/collector query surfaces | query class | its own IAM grants, per plane |

## 4. The ingest service — lifecycle and the teardown gate

```
doc arrives on the lane ──▶ DURABLE (the lane's WAL-backed enqueue ack — THE ack)
        │ applier consumes (at-least-once; apply keyed by content hash ⇒
        ▼  idempotent — a redelivered doc is a no-op)
     STORED (bytes content-addressed in the archive class)
        ▼
     INDEXED (the index row committed; visible to queries and watches)
```

- **The teardown gate, precisely**: MONITORING §6's flush-gated teardown waits
  for **the lane's durable ack** for the `FinalFlush` doc — a service receipt.
  If the colocation node is partitioned, teardown *waits*, visibly (a lifecycle
  signal) — the alternative (tearing down with the black box unlanded) is the
  exact loss §7 of the research showed matters most. Bounded by QUEUE
  availability, never by any agent.
- Ingest **validates by type** (closed doc kinds, H8-walk on structured fields)
  and **attributes by flow key** (the sender is cryptographically its Scribe);
  it judges nothing — judgment-shaped rejection is the Archivalist's, later,
  via `Prune`.

## 5. The fitter — collect, fit, vote, gate

**Collect** (cadence derived — comfortably inside the collector's retention
horizons, so points are always consumed before they expire):

```
1. batch = collector.read_series(session scope, pair selectors, since cursor)
2. for each observation: if covered by an ExclusionMark ⇒ skip (counted);
   else stats[pair, signal].current_window.per_bucket[difficulty].record(x)
3. rotate sub-windows on span boundaries (K disjoint slices, time-partitioned)
4. own_log.append(checkpoint{cursor', stats})        // crash ⇒ resume exactly
```

**Fit** (on derived cadence or an Archivalist `TriggerRefit`):

```
1. per sub-window w: fit_w = robust params per bucket — median + MAD read from
   the window's histogram quantiles (exact within the histogram's α — the
   ANTIDOTE lesson: robust estimators, never means/variance)
2. VOTE: param = median over {fit_1..fit_K}; a sub-window deviating from the
   vote by more than the derived bound is DISCARDED + counted (a contamination
   candidate — the Cretu micro-model defense; any one window's influence ≤ 1/K
   by construction, the Kloft–Laskov bounded-influence principle)
3. candidate = CurveVersion{v+1, prev: frozen v, drift: compare(candidate, v)}
4. THE GATE: drift ≤ derived bound ⇒ PUBLISH (store + index; detection's watch
   fires). drift > bound ⇒ WITHHELD: a consult claim to the Archivalist with
   the drift report — publication only via ApproveDriftPublication.
   Detection continues on frozen v meanwhile. THIS is the boiling-frog stopper:
   the baseline cannot slide silently at ANY rate, because every step is
   measured against a frozen predecessor and large steps need judgment —
   and the judge's input is the drift REPORT, not the (possibly poisoned) data's
   own plausibility.
```

**Why admission is model-independent (structural, AR4)**: exclusion marks come
from lifecycle facts (handoff spans, incident spans, boots, probes —
auto-authored from ledger/lifecycle records) or from the Archivalist's
provenance judgment. **No path exists from the fitted model to the admission
decision** — the research's boiling-frog result is precisely that
filter-then-retrain loops train the filter on yesterday's poison; we sever the
loop instead of tuning it.

## 6. Curves, cold start, and the inheritance ladder

A detector at birth (HANDOFF §3) resolves its pair's curve:

```
current CurveVersion in the index?            → use it (watch for refits)
else: LineageSeed imported at session create? → seed as v0 (provenance: lineage)
else: ShippedDefault (release-versioned)      → v0 (provenance: default)
```

- **Lineage seeds**: at archive-finalize, the Archivalist's
  `ApproveSeedExport` seals the session's current curve set as a
  `LineageSeed` with its full provenance chain (which sessions, which versions,
  which exclusions). A descendant on the same lineage imports it at create —
  same workload continuing under the same owner; the *only* cross-session flow,
  flagged honestly as our own extension (no monitoring-product precedent; the
  hierarchical-Bayes literature is the nearest kin, and ours is stricter — the
  "pool" is the single-owner ancestor chain, never peers).
- **Shipped defaults**: conservative structural priors versioned with the
  release (logical clamps, wide bounds — the CloudWatch pattern), learned from
  nobody, poisoning-free the way any release constant is.
- **The default-distrust posture carries** (LC-CUSUM's lesson): under seed or
  default provenance, detection runs with the wider bounds those curves declare,
  and HANDOFF's FIR seeding + the fresh-context probe remain the decision
  backstops — a thin baseline never blocks detection, it widens it.

## 7. The Archivalist (judgment + queries, atop all of it)

- **Admission/pruning**: provenance judgment as `Exclude`/`Prune` claims —
  "that span was a live incident," "that summary's span is double-counted."
- **Refits and drift**: `TriggerRefit` at will; `ApproveDriftPublication` as
  the human-grade check on large baseline moves.
- **Seed export**: the archive-finalize approval — what a lineage descendant
  will inherit is an *approved* artifact, not an automatic dump.
- **Query provision and facilitation**: the session's investigation surface —
  agents and the user consult it (`consult_peer`); it composes answers from the
  archive index/store and, under its own grants, the collector's query surface
  (`read_series`/`read_trace`/`join_work`). Queries execute on the services;
  the Archivalist directs and interprets. It can *answer* "how does this run
  compare to the last one on this lineage" precisely because the seeds carry
  provenance chains.
- **Never a hop**: no byte of ingest, fitting, or serving routes through the
  agent; a wedged Archivalist degrades *judgment* (refit approvals queue,
  admissions default to lifecycle-auto marks) — never ingestion, never
  teardown, never detection.

## 8. Failure & recovery matrix

| What fails | Effect | Counted where | Recovery |
|---|---|---|---|
| The ingest lane's node | teardown acks pause for this session | lifecycle signals | QUEUE's env-derived durability; teardown waits visibly, never proceeds unlanded |
| The applier | landing → indexing lag | applier lag | at-least-once redelivery; content-hash idempotence |
| The fitter | fitting pauses; detection unaffected (frozen versions serve) | fitter lifecycle | own_log checkpoint: resume cursor + stats exactly; missed collection windows show as sample-count dips in FitMeta — visible, honest |
| The index shard | curve reads/watches degrade per STORE's matrix | STORE's | STORE recovery; detectors keep their last-read curve (curves change rarely by design) |
| The Archivalist (agent) | judgments queue; auto lifecycle marks continue; within-bound refits continue | consult latency | agent respawn/handoff — the plane runs; only judgment-gated actions wait |
| Archive store objects | per OBJECT_TIER's matrix (replicated, content-addressed) | OBJECT_TIER's | re-fetch/scrub by hash |
| A poisoning attempt (Bar A agent skewing its own signals) | its sub-windows deviate → discarded+counted; drift gate withholds; ITS OWN baselines only (session-scoped) | vote-discard + drift-withheld counters | the Archivalist prunes; refit clean; detection ran on frozen versions throughout — §11's worked variant |
| Lineage-seed import fails at create | cold start falls to shipped defaults | import outcome | retry through TRANSFER; the ladder degrades gracefully, detection never blocked |

## 9. Refusal & loss taxonomy (closed)

`ExcludedSpan (counted skip) | VoteDiscard (contamination candidate) |
DriftWithheld | UnapprovedSeed | ImportFallback (seed→default) |
LaneWait (teardown gate) | UnknownDocKind (typed reject)` — one counter each; CI
walks every skip/withhold/reject path into exactly one category.

## 10. Derived constants

| Constant | Formula | Anchors |
|---|---|---|
| Collect cadence | ≤ collector retention horizon ÷ derived safety factor | the session's series retention |
| K (sub-windows) | contamination-tolerance target: any window ≤ 1/K influence; K from the tolerated single-window skew | drift bound, fit variance |
| Sub-window span | fit-sample sufficiency ÷ observation rate | measured signal rates |
| Drift bound | detection-sensitivity impact of a param shift (from HANDOFF's k=Δ/2 anchors) | the Δ cost anchors |
| Stats budget | K × buckets × sizeof(ExpHistogram) × pairs | pair census, α |
| Refit cadence | curve staleness cost vs fit cost | measured drift rates |
| Ack-wait alarm | derived from lane availability distribution | QUEUE availability |
| Default bounds width | the widest per-signal spread across the release's validation corpus | release validation runs |

## 11. Worked example — one pair's life, and a poisoning attempt

Session `s9`; pair `(engineer, refactor)`.

1. **Cold start**: `s9` forked from lineage node `L3`; at create it imported
   `L3`'s `LineageSeed` — the Engineer's detectors birth on seed-v0 curves
   (provenance: lineage, bounds moderately wide).
2. **Collect**: the fitter's cadence pulls the session's engineer/refactor
   series from the collector (query class, session grant); stats accumulate in
   K=6 sub-windows; a handoff at 14:00 auto-authors an
   `ExclusionMark{Handoff, 13:55–14:10}` — those observations never enter.
3. **Refit**: at cadence, six sub-window fits vote; drift vs seed-v0 is small
   (the workload matches its ancestor); v1 publishes; the detection substrate's
   watch fires; risk adjustment now runs on *this* session's own normal.
4. **The attack variant**: a Bar-A-compromised Engineer slowly pads its
   latencies, hoping to drag its baseline up so real degradation later hides
   (the boiling frog — the receipt: 5%/week defeats naive retraining). Here:
   its skewed spans dominate two of six sub-windows → both **deviate from the
   vote and are discarded, counted** (influence capped at 2/6 even before
   discard); the surviving fit still drifts past the bound → **withheld**, a
   consult claim reaches the Archivalist with the drift report and the two
   discard flags; the Archivalist prunes the spans (`ArchivalistJudged` marks),
   the clean refit publishes — and throughout, detection judged the agent
   against **frozen v1**: the padding registered as *deviation from normal*,
   exactly what HANDOFF exists to catch, and the poisoned data never became
   normal. Blast radius even on total failure of every guard: `s9`'s own
   curves — no other session, no other user (§1's scoping law).
5. **Finalize**: at archive-finalize the Archivalist approves the seed export;
   `s9`'s curves (with provenance: seeded-from-L3 + this session's versions +
   exclusions) seal for the next descendant. The final flushes landed at each
   pod's teardown against the lane's acks, months of narration and summaries
   are indexed, and the Archivalist answered "how did this compare to L3's
   run?" from the chain it curated.

## 12. Laptop degenerate

`N=1`: the ingest lane, applier, fitter, index, and store scopes all live on
the one node beside the one collector; the ladder, the gate, and the vote run
identically; K and cadences derive from laptop anchors. Zero modes.

## 13. Amendments landing with acceptance (one coordinated sweep)

- `MONITORING.md` §6/§13 — the flush's destination and the teardown gate named
  precisely: **the archive ingest service's durable ack** (not "the
  Archivalist"); the Scribe's outbound enumerates
  narration/summary/final-flush → the ingest lane.
- `HANDOFF.md` §3/§4/§11 — `RefCurveRef` resolves through the archive index;
  the cold-start ladder (§6 here) replaces "fleet priors"; the flagged homes
  rider (R4-OQ-a / R2-OQ-c) closes: curves live HERE; incidents/handoffs/probes
  auto-author exclusion marks.
- `SESSIONS.md` §2 — the colocation list gains the archive services (ingest,
  fitter, index); archive-finalize includes the seed-export approval + seal.
- `STORE.md` §16 — the archive-index instance row (B-tree, in-order, index
  reads + watch).
- `AGENTS.md` — the Archivalist's roster row gains "administers the archive
  plane; provides/facilitates investigation queries"; `CONTEXT.md` gains
  Archive plane / Reference curve / Lineage seed entries (same commit —
  glossary-wins).
- `LEDGER.md` §8 — a pointer: the retired-proof archive's infrastructure is
  this spec's store/index.
- `OBJECT_TIER.md` — confirm/register the `archive` storage class naming
  (LEDGER §8's proofs already occupy it; boot-validated per OT11) + its GC
  root (index-referenced docs; sealed seeds root until lineage retirement).
- `RANK.md`/score service — unchanged (reputation ≠ baselines; different
  consumers, stated).
- `GAPS.md` — the baseline-store exchange closes; MONITORING §18's R4-OQ-a/c
  and R2-OQ-c riders strike.

## 14. Acceptance criteria

| # | Criterion | The failure it catches |
|---|---|---|
| AR1 | **The teardown gate is a service ack**: no agent turn exists on the flush/teardown path (architecture test); ack latency is service-grade (ratchet) | pod lifecycle coupled to LLM availability |
| AR2 | **One collection path**: the fitter holds only bounded sufficient statistics; no archive component stores raw series points (architecture + memory test) | a second TSDB; double collection |
| AR3 | **Session isolation, structural**: no read/write path crosses a session fence except the lineage-seed import at create, provenance-chain-verified (architecture test + authz fuzz) | cross-tenant baseline flow; the poisoning blast radius escaping |
| AR4 | **Model-independent admission**: no data path from fitted curves to exclusion/admission decisions (architecture test); exclusion reasons are lifecycle facts or Archivalist claims | the boiling-frog filter-then-retrain loop |
| AR5 | **The drift gate**: no CurveVersion publishes with drift > bound absent an `ApproveDriftPublication` claim; detection serves frozen versions while withheld (fuzz: injected drift at every rate — fast AND slow — never publishes silently) | silent baseline slide at any speed |
| AR6 | **Bounded influence**: any single sub-window's effect on published params ≤ 1/K before discard, zero after (contamination fuzz vs oracle) | one poisoned slice steering the curve |
| AR7 | **Robust fitting**: median/MAD from histogram quantiles; no mean/variance estimator exists in the fitter (audit) — ANTIDOTE's lesson as law | fragile-estimator poisoning |
| AR8 | **The inheritance ladder**: seed → default resolution exact; a detector is never blocked by a missing curve (birth fuzz); seed provenance chains verify or the import falls back (counted) | cold-start deadlock; unprovenanced inheritance |
| AR9 | **Agents act as claims**: every Archivalist directive is a claim; zero out-of-band control paths into the services (structural) | invisible curation authority |
| AR10 | **Idempotent ingest**: redelivery/reorder fuzz ⇒ single store+index effect (content-hash keying); crash at every applier point recovers exactly | duplicate documents; lost flushes |
| AR11 | **Plane purity**: the collector, ledger, and knowledge planes are untouched (no new writes to any of them from this plane; architecture test) | the retrofit failure mode, recurring |
| AR12 | `N=1` ≡ fleet; every §10 constant derived at its definition site | modes; magic numbers |

## 15. Test matrix (SIM)

| Test | Asserts |
|---|---|
| Teardown-gate fuzz | AR1 (kill/partition at every flush point; teardown waits visibly, proceeds only on ack) |
| Memory/points audit | AR2 (stats bounded under unbounded signal volume) |
| Isolation fuzz | AR3 (every cross-session access attempt refused; seed import the only survivor, chain-verified) |
| Boiling-frog suite | AR4/AR5 (slow-ramp poisoning at the literature's rates ⇒ withheld/discarded, never published; the frozen version serves throughout) |
| Contamination sweep | AR6/AR7 (0–100% poisoned windows vs oracle; influence bound holds) |
| Birth ladder fuzz | AR8 (all three provenance paths; missing/corrupt seeds fall back counted) |
| Directive audit | AR9 (every service config change traces to a claim) |
| Ingest crash fuzz | AR10 |
| Plane-purity walk | AR11 |
| Laptop parity | AR12 |

## 16. References (load-bearing few)

Elastic `partition_field_name` ("completely independent baselines") +
CloudWatch per-metric models & excluded-time-ranges + Datadog suppression
seasons + Azure per-series statelessness — per-entity practice, universal.
Steiner 2000 (population risk model, no per-entity history needed) + LC-CUSUM
(default-distrust cold start) + Efron–Morris/CMS hierarchical shrinkage (the
pooling alternative, deliberately not taken — ours is single-owner lineage).
ANTIDOTE (Rubinstein et al., IMC'09: 10% chaff → 8× evasion; robust estimators)
+ the boiling-frog result (5%/week defeats filter-then-retrain) + Kloft–Laskov
(bounded windows ⇒ a critical influence ratio) + Cretu et al. (disjoint
micro-model voting) + NIST AI 100-2 (poisoning taxonomy). Companions as
enumerated in §13.
