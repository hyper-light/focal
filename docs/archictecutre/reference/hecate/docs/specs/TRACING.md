# SPEC: TRACING — execution tracing across every subsystem

Status: ACCEPTED 2026-08-22 (rewritten to the COLLECTOR bar; the §12 span-clause
sweep landed the same day). The
foundation was settled in-session (GRILLING.md): the three-id law, the envelope
context, the chokepoint-span pattern, and class-aware head sampling (the
research-corrected model — Dapper/Canopy head-sample; tail has no hyperscale
precedent). This rewrite adds depth and carries five NEW settlements, flagged in
the presentation: park/resume/handoff trace linkage, drop-guard closure, id
generation, keep-rate adaptation, and spawn inheritance. Research on file: Dapper
(TR 2010-1), Canopy (SOSP'17), W3C Trace Context, OTel sampling docs. Companions:
`PROTOCOL.md` (the envelope carries the context — amendment landed), `MONITORING.md`
(spans ride its rings), `COLLECTOR.md` (assembly/storage/query — ACCEPTED; its §8
assembler is this spec's downstream), `HEALTH.md` (content-free H8), `LEDGER.md`
(`trace_refs` — amendment landed), `AGENTS_RUNTIME.md` (park/resume), `HANDOFF.md`
(the localization consumer), `REGISTRY.md` (the chokepoint/keep registry).

## 1. Role — the execution story, and the three-id law

A distributed trace is the **execution story of one operation** across the whole
system: a summon crossing gateway → scheduler → consensus admission → pod boot →
VFS mount → warden compile → agent start, with the time spent and the outcome at
every hop. It answers "where did these 8 seconds go" and "which hop failed" —
questions neither the claims graph (the *work* story) nor any metric (an
*aggregate*) can answer.

The three correlation planes, and the law that they never merge:

| Id | Plane | Answers | Home |
|---|---|---|---|
| `request_id` | transport | which response pairs with which request | `PROTOCOL.md` §2 envelope |
| `trace_id` / `span_id` | execution | where one operation's time and failures went | this spec |
| `caused_by` | work | which claim caused which claim | the claims ledger |

**Disjoint by law** (`LEDGER` §9.8 — a complete durable trace graph keyed on
`caused_by` would be a second claims ledger; different lifecycle too: the claims
graph is complete and never sampled, traces are sampled-for-keep and expiring).
**Cross-linked by opaque reference, never fused (TR1):** a span may carry a claim
UID as a bounded tag; a claim's system-written lifecycle record carries
**`trace_refs`** — the trace ids of the operations that posted and serviced it
(landed with COLLECTOR acceptance). An investigator pivots claim → traces or
span → claim through each plane's own authorized read; no shared key exists.

## 1a. The whole machine, in plain terms

Package tracking. When a parcel is accepted, the counter stamps a **tracking
number** on it (the trace id, minted once at the root). Every facility it passes —
intake, sort, linehaul, delivery — **scans it in and out** (a span: where, when in,
when out, what happened), and every scan names the previous leg (the parent span),
so the journey reassembles as a chain even though no facility knows the whole
route. Scanners never stop the conveyor (spans are emitted async — tracing never
slows the operation it measures). The courier can't afford to keep full scan
history for every routine envelope forever, so the *stamp itself* says how much
history to keep (the keep flag, decided at acceptance — head sampling): insured and
express parcels keep everything (the 100% classes); bulk mail keeps a sample. A
scan that never happens (a dropped scanner) doesn't lose the parcel — the next
facility still scans it, and the assembled history is honestly marked *incomplete*
rather than pretending the gap isn't there.

The analogy carries the three least-obvious choices: the keep decision is stamped
**at the counter, not at the archive** (deciding at the end would mean hauling every
routine envelope's full history to headquarters first — the tail-sampling cost that
has no planet-scale precedent); scans are **observations, not custody** (the work
story — who was *responsible* — is the claims ledger, a different book); and a
parked parcel (held at customs) gets a **new tracking leg** linked on the customs
form, rather than one scan spanning a week of nothing (§5's park rule).

## 1b. Terms this document uses (reading guide)

- **Trace** — one operation's span tree, sharing one `trace_id`, from the root
  that minted it through every chokepoint it crossed.
- **Root** — the first span of a trace; minted where the operation enters the
  system (or where background work begins); its chokepoint's class decides keep.
- **Span** — one chokepoint's timed slice: ids, chokepoint name, timing, typed
  status, bounded tags. Content-free, always.
- **Context (`TraceCtx`)** — the propagated tag: trace id + the sender's span id +
  the keep flag. Rides every wire message; inherited by spawned tasks.
- **Keep (head sampling)** — the retention decision made once at the root and
  propagated, so a trace is kept or dropped whole. Emission is universal; *keeping*
  is sampled.
- **Chokepoint** — a boot-registered boundary every cross-component access crosses
  (the corpus's chokepoint law); the complete set of span points.
- **Gap record** — an in-band marker that a ring overwrote unread spans; assembled
  traces containing one are marked incomplete.

## 2. Data model

```rust
// ---- the context (on every wire message; PROTOCOL §1.1 envelope, landed) ----
struct TraceCtx {
    trace_id: TraceId,        // [u8; 16] — minted once at the root (§4 id law)
    span_id: SpanId,          // [u8; 8]  — the sender's current span: the parent
                              //   of whatever the receiver does for this message
    flags: u8,                // bit 0 = keep (the root's decision, §6);
}                             //   remaining bits must-be-zero (codec-rejected)

// ---- the span record (what a chokepoint emits; COLLECTOR carries it onward) ----
struct SpanRecord {
    trace_id: TraceId,
    span_id: SpanId,
    parent_span_id: Option<SpanId>,   // None ⇔ this is the root
    chokepoint: ChokepointId,         // closed registry (§3) — never free text
    node: NodeId,                     // where it ran (skew honesty, §4)
    start: Monotonic, end: Monotonic, // node-local monotonic; duration = end−start;
                                      //   cross-node order comes from parent EDGES,
                                      //   never from comparing clocks (§4)
    status: SpanStatus,               // closed taxonomy below
    tags: BoundedTags,                // bounded-cardinality: shard id, size-bucket,
                                      //   principal UID, claim UID — opaque ids and
                                      //   closed enums only (H8-walked, TR5)
}
enum SpanStatus { Ok,
                  Err(ErrorClass),    // the closed per-subsystem error taxonomy
                  Aborted,            // the operation died mid-span (§5 drop guard)
                  Parked }            // the turn parked (§5 — the trace ENDS here;
                                      //   resume roots a NEW trace, linked via the
                                      //   claim's trace_refs)

// ---- the chokepoint registry entry (REGISTRY documents; boot-validated) ----
struct ChokepointEntry {
    id: ChokepointId,                 // e.g. store.ingest, consensus.commit,
    subsystem: SubsystemId,           //   warden.verdict, gateway.call — the span
    keep: KeepPolicy,                 //   roster IS this roster (§3)
    provenance: Provenance,           // HostObserved | GuestReported (fixed by
}                                     //   where the chokepoint physically runs)
enum KeepPolicy { Always,             // the 100% classes (§6) — bounded-rate ops
                  Derived }           // baseline-rate classes; the rate itself is
                                      //   published config, adapted (§6), never
                                      //   stored in this entry

// ---- the emitter (per task; runtime-owned, invisible to the model) ----
struct TaskTraceState {               // hecate-rt task-local: the runtime carries
    ctx: TraceCtx,                    //   it; spawned tasks INHERIT it (§5);
    open: SmallVec<SpanGuard>,        //   detached background work roots fresh
}
struct SpanGuard {                    // RAII: opened on chokepoint entry, closes
    record: SpanRecord,               //   on scope exit — INCLUDING unwind/abort
}                                     //   (Drop stamps Aborted; §5)
```

## 3. The chokepoint-span pattern (the roster is the registry)

The chokepoint law already forces every cross-component access through a named,
boot-validated boundary. **Those boundaries are the span points**, and the span
roster is not a second list — it *is* the chokepoint registry (§2's entries):
`store.{apply,read,scan,ingest,checkpoint,split}`, `queue.{enqueue,lease,ack}`,
`cache.{get,put,invalidate}`, `fanout.{route,deliver}`, `consensus.{propose,
commit}`, `materializer.{epoch,apply}`, `pods.{summon,assign,teardown}`,
`vfs/serving` ops, `merge` gate ops, `iam.decide`, `registry` ops,
`warden.verdict`, `gateway.call`, `collector.{tick,uplink,query}`, the agent
runtime's turn/tool/claim boundaries — each subsystem's table, boot-validated
exactly as chokepoint coverage already is: **an unregistered emitter fails
startup; a registered chokepoint with no span emission fails the coverage walk**
(TR3). On MONITORING/TRACING acceptance, every subsystem spec gains its one-line
"chokepoints emit spans per `TRACING.md` §3" clause in one sweep (§12).

The pattern, uniform everywhere:

```rust
fn instrumented<T>(cp: ChokepointId, work: impl FnOnce() -> Result<T>) -> Result<T> {
    let _span = SpanGuard::open(cp);          // child of the task's current ctx;
                                              //   becomes the current span
    let out = work();                         // sends carry {trace_id, this span}
    // SpanGuard::drop closes the span: Ok/Err from `out` if reached normally,
    // Aborted if unwinding — a dying operation NEVER leaks an open span (§5).
    out
}
```

## 4. Ids and clocks (NEW settlements, flagged)

- **`trace_id` = 128 random bits from the seeded driver RNG; `span_id` = 64 random
  bits.** Coordination-free by design (Dapper's model). The collision bound is
  derived, not asserted: at 10¹² traces retained, the birthday bound gives
  P(any collision) < 10⁻¹³ for 128-bit ids — below every other failure rate in
  the system; 64-bit span ids collide only *within* one trace's scope, where the
  span count is bounded by the trace's own size. In the SIM, ids come from the
  seeded driver (deterministic replay holds — the no-wall-clock law).
- **Clocks: monotonic locally, edges globally.** `start`/`end` are node-local
  monotonic stamps — valid for *duration* on that node, never compared across
  nodes (skew would lie). Cross-node ordering comes from the **parent edges**
  (a child began after its parent's send, by causality); the assembler sorts
  siblings by `(parent, start)` and renders cross-node time honestly (per-node
  timelines joined by edges — TR-clock test). No TrueTime, no HLC requirement on
  the hot path; the `at` stamps in operational records are the collector's
  concern, not the span's.

## 5. Propagation — the full lifecycle, including the hard edges (NEW, flagged)

- **Within a task**: the runtime's task-local `TaskTraceState` carries the context;
  `SpanGuard`s nest (the current span is the innermost open one).
- **Across a message**: every send stamps `{trace_id, current span_id, flags}`
  into the envelope (total propagation — the codec rejects absence; landed in
  PROTOCOL). The receiver's handling opens its span as a child.
- **Across a spawn**: a task spawned *as part of* the operation **inherits** the
  context (the child's spans join the trace). **Detached background work**
  (maintenance loops, compaction, cadenced flushes) **roots a fresh trace** at its
  own chokepoint — there is no untraced execution, and no operation's trace grows
  unboundedly by adopting a daemon.
- **Park (the agent hard edge)**: when a turn parks (`AGENTS_RUNTIME`), the
  in-flight spans close with `Parked` and **the trace ends**. Resume **roots a new
  trace** at the resume chokepoint. The two traces are siblings *of the claim*:
  both land in the claim's `trace_refs` — which is exactly why that field is
  plural. The alternative (one trace spanning a week-long park) was rejected: it
  would hold assembler windows open unboundedly and time nothing real.
- **Handoff**: the predecessor's operations closed their traces (or died —
  `Aborted`/incomplete); the successor's operations root fresh; the claim's
  `trace_refs` accumulates both sides. Execution history survives *as the claim's
  reference list*, never as one synthetic mega-trace.
- **Death**: a process/pod dying mid-span emits nothing further — the host-side
  lifecycle event (pod teardown, pidfd close) is the authoritative end; the
  assembler's window seals the trace `incomplete` with the death visible as the
  missing close (TR8's honesty). The drop guard covers *unwinding*; death needs no
  cooperation, by design.

## 6. The keep decision — class-aware head sampling (settled; mechanics added)

**Everything emits; the root decides what is *kept*, once, and the decision
propagates in `flags`** — so a trace is kept or dropped whole (TR6). Head, not
tail, on the receipts: Dapper (1/1024 default, adaptive to a target rate, 0.01%
floor) and Canopy (token bucket at request entry, 1.3B traces/day) both decide at
the root; grep-confirmed neither tail-samples; tail sampling is a collector-era
technique whose own docs call it stateful and resource-intensive.

- **`Always` classes** (100%-kept; all bounded-rate by construction):
  summon/teardown, handoff, materialization/landing, merge-gate passage, Guardian
  escalations and verdicts, cross-region operations, agent turn-cycle operations.
- **`Derived` classes** (the micro-op firehose: cache gets, queue polls, store
  reads): kept at `rate = kept-volume budget ÷ measured class volume` — derived,
  adaptive, never a literal.
- **The adaptation loop (NEW, flagged)**: the region tier measures per-class kept
  volume against the budget and **publishes the derived rates as registry
  updates** at a derived cadence (the same publish→watch→apply lifecycle metrics
  use — `COLLECTOR.md` §10b; no new config plane). Nodes apply the watched rates;
  a node partitioned from the registry keeps its **last-known rates** — safe
  (rates only mistune retention volume, never correctness), counted, and visible
  as registry staleness. The keep decision itself stays local and O(1): class →
  `Always`, or a seeded-hash-vs-rate comparison on `trace_id` (deterministic per
  trace — every node computing it would agree, though only the root actually
  decides).
- **Detection never depends on a kept routine trace**: error/latency *metrics*
  cover 100% of traffic (COLLECTOR §5); the kept sample plus the `Always` classes
  supply the path when an investigation needs one.
- **Tail sampling remains a possible future opt-in** for a declared low-volume
  class — gated on a declared property like every opt-in; never the default.

## 7. Emission, and where spans physically go

Nothing in this section is a new channel (the zero-new-surface property):
host-side chokepoints emit async to their node's rings (`MONITORING.md` §5);
guest-interior spans are runtime-emitted below the model to the pod's history
ring and drain via the Scribe flow. Loss is tolerated and **counted** — a ring
overwrite leaves an in-band gap record, and any assembled trace containing one is
marked incomplete (TR8). Kept spans forward to the region assembler by
`weighted_hrw(trace_id)`; assembly, storage, retention, and query are
`COLLECTOR.md` §8/§9a (accepted). The cost receipts that price emission-always:
Dapper measured ~200 ns span create/destroy, 9–40 ns per annotation, 426 B/span,
its daemon under 0.3% of one core — emission is cheap; *keeping* is what §6
bounds.

| Flow | Rides | Class |
|---|---|---|
| host chokepoint → node ring | in-process bounded ring | not a network hop |
| guest span → host | the history ring → Scribe vsock flow | existing pod channels |
| `TraceCtx` on every message | the encrypted envelope | every plane, by law |
| kept span → assembler | COLLECTOR's class-1 datagrams | sheddable; loss ⇒ gap |
| trace reads | COLLECTOR's query surface | IAM-gated |

## 8. Provenance and trust

Spans are provenance-classed like every signal (fixed per chokepoint — §2's
entry): **host-observed** spans (warden, VMM, gateway, store, consensus — every
device-boundary crossing is host-visible by construction) are authority-grade;
**guest-reported** spans (agent-interior detail) are enrichment-grade. At Bar B a
compromised guest can lie only in its own interior spans — as itself, about
itself: it cannot forge another pod's spans (per-workload flow keys) and cannot
tamper a context in flight (the envelope is encrypted and authenticated). The
host-observed skeleton of the same trace stands, and guest-vs-host divergence is
itself a detection signal (the lie-detector pattern). Detection reads the
authority skeleton; narration and investigation may use the enrichment detail.

## 9. Failure matrix

| What fails | Effect on traces | Counted where | Honest surface |
|---|---|---|---|
| A source ring overflows | unread spans lost | ring-overrun + in-band gap records | assembled traces marked incomplete (TR8) |
| An operation panics | drop guard closes `Aborted` | error-class counters | the abort is IN the trace, timed |
| A process/pod dies mid-span | no further emission | pod lifecycle events | window seals incomplete; death visible as the missing close |
| A turn parks | trace ends `Parked` | — (by design) | resume roots a new trace; the claim's `trace_refs` links both |
| Keep-rate registry stale | last-known rates apply | registry staleness signals | retention volume mistunes; correctness untouched |
| The assembler (downstream) | COLLECTOR §11's rows | there | `AssemblerLost`/incomplete rows |
| Context absent on a message | unrepresentable | codec rejection (TR2) | a build/peer bug, loud |

## 10. Derived constants

| Constant | Formula | Anchors |
|---|---|---|
| Keep budget (region) | assembler capacity × derived share (COLLECTOR §13) | measured kept-span cost |
| Per-class derived rates | budget ÷ measured class volume, adaptive | class volume census |
| Rate-publish cadence | derived from class-volume drift rate | measured drift |
| Span buffer (per task) | max nesting depth × sizeof(SpanRecord) | measured depth ceiling |
| Gap-alarm line | derived fraction of emission rate | measured overrun base rate |
| Id sizes | fixed by the wire contract (16 B / 8 B) — the collision bound is the derivation (§4) | trace census bound |

## 11. Worked example — one summon, traced (and one failure variant)

The user asks for an Engineer; the Guide's turn posts a summon claim `C81`.

1. **Root**: the summon dispatch chokepoint mints `trace T (id=…f3a2)`, class
   `Always` ⇒ `flags.keep=1`. The dispatch record and `C81.trace_refs` both carry
   `T`.
2. **Propagation**: the scheduler's admission opens `scheduler.admit` as a child
   of the dispatch span (the message carried `{T, dispatch-span}`); consensus
   admission (`consensus.commit`), pool checkout (`pods.assign`), VFS attach
   (`vfs.attach` — 5.8 s, the eventual answer), warden compile
   (`warden.compile`), and agent start (`pods.start`) each open children of their
   callers' spans — six nodes, one tree, every hop's envelope carrying `T`.
3. **Emission/keep**: every span emits async to its node's ring; `keep=1` ⇒ all
   forward to assembler `A2` (HRW on `T`).
4. **Assembly**: the window closes; `TraceRow{T, complete}` seals; the row's
   sibling metrics (per-chokepoint envelopes) updated on 100% of traffic
   regardless.
5. **The answer**: "the summon took 8.1 s; 5.8 s is `vfs.attach` on node-4" —
   `join_work(C81)` composes the claim skeleton + `T` + the exemplared series
   (COLLECTOR §10's worked example is this one's downstream half).
6. **Failure variant**: node-4's ring overruns during the mount (a colocated
   firehose); two interior spans are overwritten, leaving a gap record. `T`
   assembles with `complete=false`; the *edges* still show `vfs.attach`'s 5.8 s
   (its open/close survived); the gap is visible, counted, and the answer stands
   — honestly marked, never silently whole.

## 12. Amendments landing with acceptance (one coordinated sweep)

Every subsystem spec gains its one-line "chokepoints emit spans per `TRACING.md`
§3" clause (STORE, QUEUE, CACHE, FANOUT, CONSENSUS, MATERIALIZER, PODS, VFS,
SERVING, MERGE, IAM, REGISTRY, SCHEDULER, TRANSFER, WAL — the update-all-sites
law; gated jointly on MONITORING's acceptance where the emission plane lands).
`REGISTRY.md` — the chokepoint/keep registry document kind named. `GAPS.md` —
the tracing row closes. `CONTEXT.md` — glossary entries (Trace, Span, Trace
context) already landed 2026-08-21.

## 13. Acceptance criteria

| # | Criterion | The failure it catches |
|---|---|---|
| TR1 | **Three-id disjointness**: no shared identity across the planes; cross-links are opaque references; a durable trace store keyed on `caused_by` is unrepresentable (architecture test) | a second claims ledger; plane fusion |
| TR2 | **Total propagation**: every wire message carries `TraceCtx` (codec-rejected absence); every chokepoint continues the incoming context; spawns inherit; detached work roots fresh | an untraceable hop; a daemon adopted into an operation's trace |
| TR3 | **Roster ≡ registry**: span sources are exactly the boot-validated chokepoint entries; an unregistered emitter fails startup; the coverage walk finds no chokepoint without spans (the collector's own included) | a silent instrumentation gap |
| TR4 | **Zero hot-path cost**: emission is async; traced-op latency at 100% emission is unchanged within the ratcheted floor; the drop guard adds no failure path | tracing slowing or breaking what it traces |
| TR5 | **Content-free**: the H8 type-walk passes over `SpanRecord`; chokepoint names and statuses are closed registries; tags bounded | work content in a span |
| TR6 | **Whole-trace keep coherence**: the root decision propagates; no mid-trace flip; the local keep computation is deterministic per trace id | half-kept traces; skewed samples |
| TR7 | **Class coverage + adaptation**: `Always` classes are always kept (differential vs oracle); `Derived` rates track the budget across injected volume shifts; stale-registry nodes degrade to last-known, counted | a missed handoff trace; budget blow-through; a config hard dependency |
| TR8 | **Loss honesty**: gaps counted in-band; incomplete never silently whole; abort/park/death each close or seal with their true status (drop-guard + window fuzz) | fabricated completeness; leaked open spans |
| TR9 | **Hard-edge linkage**: park→resume and handoff produce separate traces, all present in the claim's `trace_refs`; no synthetic mega-trace exists | unbounded windows; execution history lost at the edges |
| TR10 | **Clock honesty**: no cross-node timestamp comparison exists in assembly or rendering; sibling order within a node only; cross-node order by parent edges (architecture test + skew-injection SIM) | skew lies in rendered timelines |
| TR11 | **Id soundness**: ids from the seeded driver in SIM (replay-identical); the collision bound derivation stands at the census ceiling | id collisions; SIM nondeterminism |
| TR12 | `N=1` ≡ fleet; every §10 constant derived at its definition site | modes; magic rates |

## 14. Test matrix (SIM)

| Test | Asserts |
|---|---|
| Propagation fuzz | TR2 (every message class, spawn kinds, detached work — parented vs a serial oracle) |
| Roster boot check | TR3 (drop one registration ⇒ boot fails; coverage walk incl. collector chokepoints) |
| Overhead ratchet | TR4 (100% emission vs compiled-out; drop-guard unwind fuzz) |
| Keep differential | TR6/TR7 (root decisions vs oracle; volume-shift adaptation; registry-partition degradation) |
| Honesty fuzz | TR8 (ring overrun, panic, pod-kill, park at every point ⇒ correct status/incomplete marks) |
| Hard-edge sweep | TR9 (park/resume/handoff ⇒ trace_refs completeness; window boundedness) |
| Skew injection | TR10 (adversarial clock offsets ⇒ rendering unchanged) |
| Content-free CI | TR5 (H8 walk) |
| Laptop parity | TR12 |

## 15. Integration (every companion touchpoint)

- **PROTOCOL / WIRE_SECURITY** — `trace_ctx` in the encrypted envelope (landed);
  distinct from `request_id` by the landed distinctness law; warden-readable.
- **MONITORING** — spans ride its rings and flows; no new channel; the
  authority/enrichment split carries via §8's provenance.
- **COLLECTOR** (accepted) — the downstream: HRW assembly, `TraceRow` storage,
  query + `join_work`, the keep-volume budget's home.
- **LEDGER / LEDGER_CORE** — `trace_refs` in the system-written lifecycle record
  (landed); the skeleton read (landed); zero fusion.
- **AGENTS_RUNTIME** — the park/resume chokepoints and the turn boundaries; the
  runtime's task-local carries the context below the model.
- **HANDOFF** — the localization consumer: detection flags, the trace explains;
  handoff execution roots successor traces (§5).
- **REGISTRY** — chokepoint/keep entries are registry documents; rate adaptation
  publishes through it (§6).
- **IAM** — trace reads cross the observability capability (COLLECTOR's PEP).
- **HEALTH** — H8 over span types; gap/overrun signals ride the plane.
- **STORE / QUEUE / CACHE / FANOUT / CONSENSUS / MATERIALIZER / PODS / VFS /
  SERVING / MERGE / SCHEDULER / TRANSFER / WAL** — each emits at its registered
  chokepoints (§12's sweep lands the clauses).
- **FAULTS / SIM** — seeded ids; skew injection; the honesty fuzz belongs to the
  nemesis matrix.

## 16. Laptop degenerate

`N=1`: same context on every message, same guards, same registry; the assembler
and budget live in the one collector; derived rates come out of laptop anchors.
Zero modes.

## 17. References (load-bearing few)

Dapper (Google TR dapper-2010-1): head sampling, adaptive target rates,
out-of-band collection, span cost receipts, 64-bit probabilistically-unique ids.
Canopy (SOSP'17): token-bucket head sampling at entry, TraceID-sharded
convergence, 1.3 B traces/day. W3C Trace Context: the 16+8+flags shape
(exemplar, not dependency). OTel sampling docs: the head/tail definitions and
tail's stateful cost. Companions as enumerated in §15.
