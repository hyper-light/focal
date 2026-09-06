# SPEC: sessions and lineages

Status: ACCEPTED 2026-08-16 (grilling Branch 19; verdict held pending Branch 21,
unblocked by `SERVING.md` — §3's contract is that spec's machine; all four
acceptance riders executed with the SERVING.md commit). Research on file:
CitC/EdenFS/Scalar, namespace/HNC/Capsule/vCluster, prebuild pools,
jj/Pijul/eg-walker/mergiraf divergence corpus, 267K-merge overlap study,
agentic-PR conflict rates, selection-gap funnel corpus (§7 numbers landed).
Vocabulary: **"workspace" is retired**; the concepts are **lineage**,
**session**, and the pod's **work volume**. Amended 2026-08-20
(CACHE/QUEUE/FANOUT acceptance): §2 opens the colocation-unit membership to
any session-scoped durable-writer primitive (the queue core + the topic
FIFO-sequencer, per CONSENSUS §6).

## 1. The lineage (first-class)

- `Lineage = { fork tree of baseline manifests, fork relations, materialization
  target }`. The target is a local tree (laptop binding) or a source-control ref
  (fleet binding) behind one port; landing to a shared trunk exits through the
  org's land-queue adapter, never around it.
- Sessions attach to lineage nodes. The lineage **outlives its sessions**: close
  them all and the tree, targets, and archive-recalled knowledge remain.
- **The materialization lease is lineage-scoped and single-holder**: exactly one
  session at a time may materialize to a given target; the lease carries a fencing
  generation; supersession (a winner landing) bumps it and kills stale holders at
  the chokepoint.

## 2. The session

- The isolation and namespace unit: `(owner, lineage node, purpose, template)`.
  **Born template-stamped, never bare** (Capsule's admission-stamping, not HNC's
  async propagation): quota, SafetyPolicy posture, daemon-office set, budgets
  arrive atomically with creation.
- **A session spans nodes** (clarified 2026-08-18): pods place on any machines
  the scheduler chooses; the colocation unit is the session's HOME SERVICES
  only (ledger core, merge proposer, frontier, field service, **the
  claims-capture task and the detection substrate** (`COLLECTOR.md` §10,
  `HANDOFF.md` §4 — session-scoped observability machinery, summoned in the
  same gang admission; added 2026-08-22, COLLECTOR acceptance), and any
  session-scoped durable-writer primitive (the queue core and the topic
  FIFO-sequencer, per `CONSENSUS.md` §6) — one node, logs replicated across
  the session group); the sequencer/log/writer stays this one colocation node,
  but the **materialized ledger graph** — a reconstructible read projection, not
  identity- or work-bearing durable truth — may partition across the session's
  other nodes for a whale session (`MATERIALIZER.md` §5, `LEDGER_SUBSTRATE.md`
  §3); volumes attach from any node
  (SERVING §0's single-surface law). Nothing in this spec's isolation story
  depends on physical adjacency — isolation is keys + fencing + brokered
  grants, never machine boundaries.
- **Everything identity- or work-bearing is per-session**: colocation unit, ledger,
  services, key root (pod keys derive from it; blast radius = one session), pods,
  wardens, work volumes. Nothing inside is reachable from another session absent a
  Sibyl-brokered capability grant (Biscuit-shaped, attenuable,
  offline-verifiable, fencing-bound). The **generic substrate is shared below the
  isolation line**: warm-tier VMs (identity-free by PODS T7), chunk store
  (tenant-partitioned), snapshot pages (DAX) — isolation of everything that
  matters, dedup of everything that doesn't.
- The **home session** is a distinguished session with no lineage: it hosts the
  **Sibyl** (`docs/specs/SIBYL.md`, accepted), the catalog, and the user-scope
  ledger for session-lifecycle claims. It inherits all session machinery
  (placement, consensus, scale-to-zero); the terminal attaches here first.

## 3. The physical contract

- Session working state = `(pinned baseline manifest, witnessed delta overlay,
  green chain)`. The baseline never moves under a session; the overlay contains
  exactly the materialized-iff-diverged entries (EdenFS's contract); green is the
  session's merge-gate output as specified.
- **No reconcile operation exists**: every write is witnessed at the serving
  boundary and lands in the overlay transactionally — the Perforce-class
  discovery-by-scanning disease is structurally absent. Status is a manifest read;
  divergence is a queryable property (CitC's measured expectation: average overlay
  &lt; 10 files).

## 4. Forking

- **Fork = lineage metadata fork + session provisioning.** The manifest part is
  O(1); the session part is template-stamped admission + skeleton (warm tier or
  ms-class cold) + **per-session pods via ordinary summon claims** — daemon
  offices per template eagerness, worker pods lazily when the Guide issues work.
  A forked-but-idle variant costs a skeleton.
- N-general: A|B|C|D… are siblings in the lineage tree; variant-count ceilings
  derive at admission (resource anchors + measured coverage economics); rapid
  create/fork/land/destroy churn is priced machinery (session skeletons, monotonic
  identity, fast-forward teardown).

## 5. The landing engine

Landing (adopt-result into a lineage head, or materialization to a target) is a
distinct three-layer machine — never the intra-session streaming merge reused:

1. **Manifest prune** — three-way against the shared baseline; identical hashes
   short-circuit; single-side files land by reference; only **doubly-touched**
   files proceed. Receipt: conflict probability tracks simultaneously-changed
   files (ρ≈0.6), not divergence duration (no significant correlation) — cost
   follows the true risk distribution.
2. **Per-file event-graph replay** (eg-walker; two logs, known fork point — its
   best case, O((k+m)log(k+m)); fork point is a critical version, checkpointed):
   exact position alignment and overlap detection — **emitting conflict values on
   intersection, never interleaving** (the CRDT default is wrong for code).
   Detector, never resolver: the auto-merge semantics ADR-0005 rejected remain
   rejected here; the intra-session gate never touches this machinery. Input op
   logs derive from witnessed writes at seal time (`SERVING.md` §3, `VFS.md` §5).
3. **Conflicts as first-class algebra values** (jj's term-list representation):
   landing into a lineage head **always succeeds**; conflict values propagate
   through subsequent landings by term extension + cancellation (no nesting);
   same-change edits auto-collapse; N-ary composition carries **one** N-way
   conflict; markers materialize/parse back for late, partial resolution; and
   **a resolution is itself a change** (Pijul's invariant) — recorded in the op
   log, deterministically replayed.
- **Composition landings** (evaluator verdicts like "A's parser + C's cache") are
  declared-order sequences of the same pairwise machine; simultaneous N-way merge
  is structurally absent.
- **Resolver proposers, exactly placed**: structural merge (mergiraf-class) runs
  as an optional, version-pinned, provenance-recorded pass over conflict values —
  its output must pass tests before a conflict counts resolved (receipts: 84.1%
  resolution, 28 false conflicts, **403 silently-missed real conflicts**). LLM
  resolution is an advisory proposer only; the chokepoint validates (receipt: an
  LLM judge accepted 4/5 structurally broken merges). Neither is ever a silent
  resolver.
- **Conflict-deposit propagation** (per-template policy): a landed increment
  overlapping an open sibling's work deposits a conflict value into that sibling's
  overlay — early, evidence-bearing awareness in its agents' ambient digest; no
  forced rebase; never load-bearing for correctness.

## 6. Review gates (user review is a first-class station)

- The adopt-result/materialization claim carries a **user validation by default**
  (approvals-as-claims; the user is the evaluator). SafetyPolicy generalizes
  `disk_write_mode` to per-tier review posture: **materialization to a real target
  defaults to prompt, always, and requires zero unresolved conflict values**;
  adoption into a virtual lineage head may run auto per template (reversible by
  supersession).
- The review surface is the **evaluation testament presented whole**: verifier
  results, judge rationale, diffs, outstanding conflict values — evidence to rule
  on, not a yes/no prompt.
- Review is not only terminal: the catalog supports attaching a terminal to any
  live session at any time — inspecting a variant mid-flight is a first-class act.

## 7. Evaluation (the funnel, with receipts — selection research on file)

Published composites close ~40–60% of the oracle–random selection gap; the funnel
stacks the mechanisms with measured lift, cheapest first:

- **Stage 0 — generate for selectability** (≈0 marginal cost): variants are
  diversified, and each variant's **Scribe narrative digest is its structured
  self-summary** — the judged representation (summaries beat raw trajectories and
  bare patches, RTV/PDR) — unified with machinery that already exists.
- **Stage 1 — mechanical triage** (+2–4 pp): normalized-patch dedup (39% of
  correct candidates are syntactic clones), build/typecheck/lint, regression
  tests as rejection sampling — **a filter, never a proof** (measured verdicts
  ~61% precision / 93% recall; hardened suites strip 4–9 pp from "passing"
  patches, so the harness assumes its test signal is softer than it looks).
- **Stage 2 — cross-variant differential execution** (highest leverage, +4–10
  pp): all survivors run against the shared generated-test pool scored by
  agreement (CodeT/B4 lineage — B4 up to +50% relative when tests are
  unreliable); for top-cluster *disagreements only*, synthesize distinguishing
  inputs and adjudicate pairwise on actual outputs (S*). Hard finding honored:
  **naive generated-test voting alone measured *worse* than public-tests-only**
  — differential adjudication is what pays, not vote counting.
- **Stage 3 — hybrid scoring** (+3–8 pp): execution evidence + an execution-free
  scorer; each family saturates alone (~42–43%), the hybrid clears both (51%,
  R2E-Gym). Hard constraint honored: **critics trained on benchmark data are
  worthless off-distribution (AUC ≤ random)** — Hecate's critic trains only on
  its own production outcomes (landing/merge/survival — ledger-observable), and
  ships training-free (repo-grounded rubric + bidirectional reconstruction)
  until those labels exist. The observe-mode discipline applies.
- **Stage 4 — committee adjudication of 2–3 finalists** (+1–6 pp): pairwise
  tournament over summaries + evidence, aggregated votes, **family-diverse
  judges** (a panel of smaller diverse judges beats one large judge with less
  bias at ~7× lower cost), both orders, generator-blind, diff-normalized.
  Verifier margin breaks ties; both-fail re-dispatches.
- **Stage 5 — composition, corrected by the evidence**: patch-space splicing of
  overlapping solutions has **no published positive result** and predicted
  failure modes (~65% of problems admit multiple *distinct* correct strategies —
  merged strategies conflict). Therefore: (a) §5's composition landings are
  restricted to **disjoint-or-verdict-clean parts**, and a composed result
  **re-enters the verifier suite as a new candidate before any review gate**;
  (b) the preferred composition is **regeneration** (PDR-style): the evaluator
  spawns a fresh variant session *conditioned on the top summaries* — consensus
  adopted, conflicts reconciled, dead ends skipped (~half the steps of a cold
  attempt) — triggered when finalists pass **disjoint subsets** of the
  differential pool (the published signal that neither is complete).
- **Budget policy**: selection is ~6% of spend — generation dominates — so the
  funnel's intelligence goes to **when to stop generating**: critic-gated early
  stopping (measured +17.7 pp at an average 1.35 attempts vs fixed-N) and
  adaptive width (K=8 captures nearly all of K=16). Variant ceilings (§4) are
  economics-derived from these curves plus resource anchors.

## 8. Lifecycle

States and transitions, each a fast-forward multi-step operation:
`create` (template stamp + skeleton + lineage attach) · `attached` (terminals;
input lease) · `idle` (daemons to zero, field/frontier checkpoint — costs ≈ 0) ·
`resume` (checkpoint + replay-since) · `fork` (§4) · `adopt-result` / `land`
(§5–6) · `close` (drain; pending materializations resolved or abandoned-with-
reason; retirement flush; archive finalize) · `archived`.
- **GC is licensed by continuous exfiltration** (the production rule: aggressive
  TTLs are safe only because durable state continuously leaves — ours does by
  construction: increments to the ledger, overlays host-side, proof to the
  archive). Idle-session TTLs derive from activity; deletion warns; the lineage
  and archive survive; **zero durable work can be lost by GC** (tested).
- Session-lifecycle acts are claims on the user-plane ledger (created,
  forked-from, granted, landed, closed) — the Sibyl's proof of work.

## 9. Scale-down floors

One command to a talking Guide (home session boots in ms; work session
auto-created); the variant apparatus dormant-not-absent at one session; session
infra floor (ledger core + WAL segments + services) is a **ratcheted measured
budget** against laptop anchors; derived formulas — skeleton pools, ceilings,
TTLs, eagerness — produce laptop and fleet behavior from the same arithmetic.

## 10. Test matrix (failure each catches)

| # | Test | Catches |
|---|---|---|
| SES1 | Isolation structural: no cross-session reachability of pods/keys/overlays/ledgers; grants only via brokered tokens with fencing | the isolation law eroding |
| SES2 | Fork cost: O(manifest)+O(bindings) measured; idle fork = skeleton only; eager set matches template exactly | fork economics regressing; eager sprawl |
| SES3 | Landing ∝ overlap: synthetic large-divergence benchmark — cost scales with doubly-touched files, never total divergence or duration | the Sapling large-stale regime returning |
| SES4 | Conflict algebra: land-with-conflicts succeeds; rebases transform without nesting; same-change collapses; resolution-as-change replays byte-identically | conflict machinery diverging from jj's proven semantics |
| SES5 | Review gates: materialization blocked absent user validation + zero unresolved conflicts; auto only where elected; evidence testament complete | silent landings; hollow review |
| SES6 | Conflict deposits: policy-on ⇒ overlapping landings deposit values into open siblings (visible in digests); policy-off ⇒ nothing; never a forced rebase | streaming becoming load-bearing or leaky |
| SES7 | Crawl degradation: full-tree crawl in-guest degrades to bounded cache-fill throughput, never per-file round-trips | the virtual-FS cliff |
| SES8 | Churn storm: rapid create/fork/land/destroy sweeps — budgets hold, leak scan clean, identity monotonic, admission ceilings loud | namespace-sprawl class |
| SES9 | GC safety: TTL-close of idle sessions loses zero durable work; warns; lineage + archive intact | GC eating live work (the Codespaces failure mode) |
| SES10 | Lifecycle fast-forward: crash at every transition boundary resumes at the missing half | half-forked/half-closed limbo |
| SES11 | Laptop floor: infra floor within anchors; one-command first-run to a responsive Guide within derived budget | floor creep |
| SES12 | Lease + fencing: single materialization holder per target; supersession bumps generation; stale holders die at the chokepoint | disk split-brain |
| SES13 | Composition: declared-order K-part landing — disjoint parts by reference, overlaps as per-file replay verdicts, one N-way conflict value | pairwise assumptions returning |

## 11. Acceptance criteria

1. SES1, SES3, SES4, SES5 permanent CI gates.
2. **"Workspace" is absent from the design vocabulary** (grep gate; the in-guest
   mount path literal is the sole exemption).
3. Per-session pods always; zero shared identity-bearing state (SES1 structural).
4. The landing engine is a pure function of (ancestor, log A, log B, engine
   version) — same inputs ⇒ same output manifest hash; speculative landing queues
   compose on top without new machinery.
5. Resolver proposers version-pinned + provenance-recorded; no resolution counts
   without passing tests; no LLM output lands unvalidated (architecture test).
6. All ceilings, TTLs, eagerness, and floors derived with derivations in place;
   §7's numbers slot in from the selection research without shape change.
7. `SessionTemplate` is a registry kind; sessions cannot be created bare
   (admission-stamped, tested).
8. SATISFIED: the Sibyl's spec (`docs/specs/SIBYL.md`), AGENTS.md entry, and
   Workstreams rank row are accepted; the name is ratified.
