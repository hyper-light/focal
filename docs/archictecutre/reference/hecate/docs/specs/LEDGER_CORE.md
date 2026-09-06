# SPEC: the ledger core — machinery for the proof of work

Status: ACCEPTED 2026-08-16 (with sub-decisions (a) apply-on-ack, (b) no outbox,
(c) event-carried score snapshots; amended under maximal audit — four corners
closed: effective-state affordance checks, paced retirement, normalized replay
comparison, bounded monitor closures). Amended 2026-08-20: §3's per-node subscriber
index is a canonical design reused as SEPARATE instances — the ledger's
claim-satisfaction-monitor and the CACHE's cache-holder (`CACHE.md` §4.1) — never a
shared instance; the no-outbox law is preserved. Amended 2026-08-20: §1 reconciled with
`MATERIALIZER.md` — the ledger core is the **sequencer** (single-owner, owns the
total order); **apply** is the scale-free order-preserving MATERIALIZER, not the
single serial task; the former "hundreds/sec demand" and "sharding destroys the
order" claims corrected. Substrate + interaction mechanics: `LEDGER_SUBSTRATE.md`.
The implementation companion to
`docs/architecture/LEDGER.md` (the law). Runs on hecate-rt under the memory
doctrine: single-owner tasks, arenas + generational handles, zero refcounting,
deterministic maps, apply-on-input purity.

## 1. The core task

One **ledger core** per session. Its **sequencer** — a single-owner task in the
session colocation unit — takes all mutation through its mailbox and produces the
total order; the **apply** is the separate `MATERIALIZER.md` (the split below). At
`N=1` the sequencer also holds all hot state in its arenas; for a hot/whale
session the materializer's worker pool applies in parallel and the heavy
claims-graph partitions across the session's nodes, while the sequencer keeps only
the small lifecycle/affordance projection local (`LEDGER_SUBSTRATE.md` §2).

**The sequencer and the apply path are separate — one owns the order, the
other scales** (reconciled 2026-08-20 with `MATERIALIZER.md`, superseding the
former "one task does everything, sharding destroys the order" clause). The
ledger core is the session's **sequencer**: a single-owner task that runs the
affordance check over effective state (§2), appends the mutation to the session
log, and *produces* the **total order** that replay, monitors, and
effective-state checks stand on. Appending is cheap (arena write +
consensus-batched, millions/sec class) and is **not** the bottleneck — so the
sequencer stays single-owner and its order is never sharded. **Applying the
acked log into the claims-graph is a separate, scale-free path — the
MATERIALIZER** (`MATERIALIZER.md`): a session has **no assumed scale** (an
agent fleet's claim rate is not bounded a-priori), and the real per-entry apply
— parse the claim, walk its causal DAG, update the graph, reconcile testaments
— is the tens-of-thousands/sec *serial* ceiling the materializer beats with
deterministic, **order-preserving** parallel apply (DAG edges run in
log-index order, independent claims commute, the parallel result is
bit-identical to serial). Sharding the *apply* therefore does **not** destroy
the order — the sequencer owns the order, the materializer preserves it. The
former single-owner serial apply is the materializer's `N=1` degenerate (a
small session, all state in one task's arenas); a hot session engages the
within-node lever, a whale the multi-node lever, on one code path. The former
"millions/sec ceiling" priced only arena-writes + delta-emit — it under-counted
apply, which is why the materializer exists. Read load rides projections (§5) /
the CACHE, never the mailbox. **The full substrate relationship and interaction
mechanics are `LEDGER_SUBSTRATE.md`.**

**Slot layout enforces writer disjointness structurally:**

```rust
struct ClaimSlot {
    content: WireBytes,          // issuer-authored, hecate-wire canonical — immutable
    content_hash: Cell<Option<Hash>>, // derived on demand, memoized
    lifecycle: ClaimLifecycle,   // system-written: status, history, timestamps,
                                 //   trace_refs (bounded TraceId array — the posting/
                                 //   servicing operations' traces, runtime-stamped;
                                 //   LEDGER §2 amendment 2026-08-22, COLLECTOR §10)
}
```

`content` is written once at generation; `lifecycle` is the only mutable half; the
two write paths are separate module-private functions — crossing them is
unrepresentable, not reviewed-for. Same shape for testaments, validations,
artifacts (artifact `Data` lives in the chunk store; slots hold references).

**Relations** are an edge arena: `{from: Handle, to: Handle, rel: Relation}` with
per-node in/out edge lists and one secondary index
`BTreeMap<(HandleKind, Relation), EdgeList>` — deterministic iteration everywhere
a record or delta is produced.

## 2. The commit path (sub-decision a: apply-on-ack)

```
input → affordance check (inform | yield | refuse) → build WAL record →
emit append effect (consensus API) → … → durable-ack input →
apply to arenas, assign sequence → build deltas (deterministic order) → emit
```

- **State is a function of the acked log, exactly**: mutations apply only on
  durable-ack (pipelined — the pending queue keeps throughput; determinism keeps
  replay trivial: replaying acked records reproduces arenas identically,
  re-executing no validators, no handlers).
- **Effective-state checks (amendment, closes the pipelined-affordance race)**:
  the refuse/affordance set evaluates against **effective state = applied arenas
  ⊕ pending queue, speculatively applied in input order** — every input is
  checked against exactly the state it will apply onto, so a pipelined sequence
  can never pass a check its predecessor invalidates (the Sylk
  update-on-terminal class, structurally closed). Apply-on-ack is then
  unconditional. Determinism holds because input order is total (L13).
- The **refuse set** evaluates purely over core-local state: writer-crossing,
  self-targeting, malformed relations (closed enums — decode already rejected
  unknowns), non-agentic quality bars, and the **rank override check** — which
  needs performance modulation, provided as **event-carried snapshots**
  (sub-decision c): the score service pushes modulation snapshots as ordinary
  inputs; the check reads last-known-snapshot from local state. No synchronous
  query escapes the core; determinism and replay hold by construction.
- **Deadlines** are timer inputs (virtual in SIM) — expiry transitions are
  ordinary deterministic inputs, as `LEDGER.md` requires.

## 3. The graph engine

- **Satisfaction monitors are per-parked-scope materializations**, not a global
  incremental SCC: each parked turn's monitor holds its transitive blocking
  closure, SCC-condensed at construction, satisfied interiors collapsed to
  released tokens. A per-node subscriber index (`node → monitors`) makes delta
  dispatch O(affected monitors), never O(all). **This subscriber index is a
  canonical, reusable design, instantiated SEPARATELY per owner — never combined
  into one structure serving both.** The ledger core holds the
  *claim-satisfaction-monitor* instance (sharded by claim ownership); the CACHE
  holds a **separate** *cache-holder* instance (serving plane, co-sharded with the
  HRW owner — `CACHE.md` §4.1). Same design, **separate structures, no shared
  instance** — so the correctness-critical claims path is never coupled to
  cache-invalidation churn (non-interference preserved). These are separate
  instances of one design, **not** one combined per-node index serving both, and
  the two subscriber sets are never merged. This reuse keeps the no-outbox law (§5)
  literally true: no new delivery structure is introduced. (Global incremental SCC rejected
  deliberately: its incrementality bugs are exactly the stranded-turn class L4
  exists to catch — per-scope + oracle fuzz is the more *verifiable* design.)
- **Closure memory is bounded (amendment)**: overlapping closures duplicate
  across monitors, so the aggregate carries a derived budget — parked-turn
  ceiling × live-graph anchors, derivation at the definition site — and a
  breach alarms loudly in the health plane rather than growing silently.
- **Monotone-cut release for free**: the core processes inputs in total order, so
  a monitor's view is always a prefix of the delta stream — no torn reads exist to
  defend against; release fires when the local fixpoint (`terminal ∧ children
  released`) holds at the current prefix.
- **Deadlock**: a deadline expiry inside an unsatisfied SCC selects the victim
  deterministically — lowest claim sequence — and fails it with a typed reason.
- **Traversal** (`traverse(node, filter, depth)`) is a bounded query message over
  the handle graph; crossing the retention boundary returns the typed archival
  continuation (§6).

## 4. Dispatch: agents, services, evaluators

- **Agents**: deltas ride the credit-based streams; the **inbox engine lives in
  the agent runtime** — expectation table (correlation → parked continuation) +
  standing identity subscriptions + the bounded dedup window
  (`(delta_key, seq)` LRU sized from queue-capacity derivation, plus content
  identity). One causally coherent entry point per dispatch.
- **Exactly-once is three layers doing different jobs** (stated so no layer is
  ever "simplified" away): cursor-exact stream resume (beyond-window
  retransmits impossible by construction) → windowed dedup LRU (in-window
  delivery races) → content identity (content-identical reposts). L6 tests the
  composition, not any single layer.
- **Services**: a boot-time handler registry, harness-side. Invocation in tracked
  scopes with bounded queues; overflow → durable `receipt_failed` + backpressure
  artifact; **handler failure → `testament_generation_failed` + error-trace
  artifact, produced from typed error values** — every handler is fallible by
  type (errors-as-artifacts; RUNTIME §4b's no-panic law: panic sources don't
  compile, `catch_unwind` does not exist — reconciled 2026-08-17, superseding
  this spec's former catch_unwind clause; a slipped dependency panic is an
  abort handled by the crash-recovery fault scope, never evidence plumbing).
  Synchronous handlers may compress lifecycle states into one commit; the
  wire-visible sequence is unchanged.
- **Validation evaluation**: receipt validations auto-pass in-core (pure).
  Programmatic validators run as service handlers, results committed as artifacts
  (replay replays the artifact, never the validator). Errored validators fall back
  per priority, then agentic. Agentic evaluation is a delta to the evaluator with
  testament + parent claim + pending validations preassembled — evaluation starts
  without traversal.
- **The accumulator** is agent-runtime-side: bounded, scope-keyed, flushes as one
  composite testament at scope close, suppressed on yield (no premature
  testimony).

## 5. Deltas and projections (sub-decision b: no outbox structure)

- Deltas are **derived from committed records at emission, deterministically
  ordered** — the replayed stream is byte-identical to the live stream.
- **There is no outbox table.** Projectors (UI bridge, Archivalist mirror,
  frontier service, **each primary's Scribe** — its authority-input delta lane,
  never the history ring (MONITORING §5b) — and the collector's claims capture,
  COLLECTOR §10) are ordinary **cursor consumers of the delta stream**: durable
  watermarks, resumable, typed RESYNC below retention — the machinery the protocol
  already ships. At-least-once projection = cursor + replay; per-projector
  terminal-failure surfacing = a stuck-cursor alarm in the health plane. One
  structure deleted relative to Sylk (its outbox), zero guarantees lost — the log
  *is* the outbox.

## 6. The retirement engine

**Paced work, never idle work (amendment)**: a busy core never idles, so
retirement tracks **debt** (bytes past the retention watermark); when debt
crosses a derived threshold, retirement steps interleave with mutation
processing at a derived ratio (both from arena-budget anchors). Under
saturating load, retirement keeps pace by construction (L8). The sweep is a
**fast-forward custody transfer** (each step idempotent, self-checking):

1. Collect terminal-and-released objects beyond the retention watermark.
2. Write archive entries (content-addressed docs + retirement-time index entries
   for the Archivalist's archive) — idempotent by content hash.
3. Verify presence by hash.
4. Drop hot slots, prune edge lists and secondary indexes, collapse monitor
   references to released tokens.
5. Advance the retirement watermark (durable).

A crash between any steps resumes at the missing half. Hot memory is bounded by
live work; the archive is complete by content identity; traversals past the
watermark return archival continuations.

## 7. Test matrix (failure each catches)

| # | Test | Catches |
|---|---|---|
| L1 | Lifecycle conformance sweep: every transition × injected failure at every boundary ⇒ exactly the spec'd durable state | undocumented states; lost failure facts |
| L2 | Writer disjointness: compile-visibility probe + runtime attempt ⇒ unrepresentable/refused | the split eroding |
| L3 | Replay: arenas identical from acked log **with derived caches normalized** (lazy `content_hash` memoization state excluded from comparison — computed-when differs between live and replay; values must match, presence must not); zero validator/handler re-execution (instrumented) | hidden state; replay side effects; a CI gate that quietly lies |
| L4 | Monitor oracle fuzz: randomized graphs (cycles included) vs brute-force fixpoint oracle; delta-order fuzz ⇒ identical release decisions, no lost wakeup | satisfaction bugs — the class that strands turns |
| L5 | Deadlock: constructed SCCs ⇒ deadline then lowest-sequence victim, identical across seeds | nondeterministic victim selection |
| L6 | Dedup: retransmit/replay/content-identity fuzz ⇒ exactly-once effects at every seam | duplicate work; duplicate render |
| L7 | Rank refuse at commit (with RANK.md K1): override shapes refused purely from local snapshot state | authority checks leaving the core |
| L8 | Retirement under load: hot bounds hold; every retired object retrievable by hash; crash mid-sweep fast-forwards; archival continuations typed | unbounded growth; lossy cooling; half-retired limbo |
| L9 | Projector discipline: kill/lag/resume projectors ⇒ cursor recovery + RESYNC; architecture test: no outbox structure exists | a second delivery structure growing back |
| L10 | Service dispatch: typed handler error ⇒ failure testament + trace artifact; overflow ⇒ receipt_failed + backpressure artifact; compression wire-identical | crashing validators; silent overload |
| L11 | Accumulator: bounded, flush-on-close, suppressed-on-yield | testimony floods; premature testimony |
| L12 | Modulation snapshots: rank verdicts identical under snapshot-delivery reordering within a window; replay identical | score coupling breaking purity |
| L13 | Effective-state checks: racing mutation fuzz through the pending window vs a serial oracle — zero divergence between checked-against state and applied-onto state; the update-on-terminal class unrepresentable | the pipelined-affordance race |
| L14 | Retirement pacing: saturating mutation load — debt stays under threshold, hot bounds hold, mutation latency degradation within derived budget | retirement starvation; the "idle work" lie |
| L15 | Monitor closure budget: adversarial shared-closure graphs — aggregate memory within derived budget, breach alarms loud | silent closure sprawl |

## 8. Acceptance criteria

1. L3 (replay), L4 (oracle-verified satisfaction), and L13 (effective-state
   checks) are permanent CI gates.
1b. The **materializer apply** throughput is measured in CI against the derived
   session demand model (`MATERIALIZER.md` M5 within-node, M6 multi-node) — apply
   is the binding ceiling, not the sequencer's append; the margin is reported,
   never assumed.
2a. The **sequencer** is a single-owner task; no lock, no shared state, no
   synchronous out-call exists in it (architecture test).
2b. The **materializer** holds no lock on state (footprint disjointness is the
   mutual exclusion, `MATERIALIZER.md` §4.3); its only shared structure is the
   per-epoch overlay resolved by atomic-max; its workers and partition-nodes are
   bounded and tracked; at `N=1` it collapses into the sequencer's node/arenas.
3. No outbox structure exists; projectors are cursor consumers only (L9).
4. Hot memory bounded by live work (L8), archive complete by content identity.
5. Memory doctrine holds: arenas + generational handles, zero Arc/Rc, deterministic
   maps (inherits the runtime lint wall).
6. Every capacity, window, and cadence derives from stated anchors at its
   definition site.
