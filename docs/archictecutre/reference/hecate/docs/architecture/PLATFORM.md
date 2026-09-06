# Platform

Cross-cutting machinery: safety policy, models and effort, failover, handoff, health,
performance scoring, observability boundaries — and the ledger of Sylk faults Hecate is
structurally forbidden from repeating.

Terminology follows `../../CONTEXT.md`.

## 1. SafetyPolicy

One user-owned **config object** drives every Guardian gate decision. It is not ledger
state; its history lives in config/logs.

- `trust_mode`: `standard | low`. Low-trust **drastically lowers the auto-approve
  ceiling** — what Hecate is willing to auto-approve shrinks sharply even if
  auto-approval remains enabled — and raises the Guardian's reasoning effort to
  max (Anthropic) / ultra (OpenAI).
- `auto_approve_risk_ceiling`: the risk level at or below which the Guardian
  auto-approves skill/tool invocations. Interpreted against `trust_mode`.
- `disk_write_mode`: `auto | prompt` — whether disk commits at the merge gate require
  explicit user approval.
- `network_egress_policy`: the default posture for external egress grants.

Every gate decision cites the SafetyPolicy field it applied — that is what makes
auto-approvals auditable and low-trust a one-field flip with predictable blast radius.

## 2. Models and effort

**One authority.** A single ModelConfig source defines, per agent: primary model,
alternate model, context window, and the per-stage effort catalog. Sylk ran three
contradictory sources (a descriptor table, per-agent constants, a config store) with
stale IDs on most rows; that is the anti-pattern.

- **Per-stage effort**: an agent's effort resolves per request from its stage catalog.
  Substantive stages get the headline tier (max/ultra/xhigh/high per `AGENTS.md` §1);
  internal stages (classification, routing, commentary) are pinned cheap. Session-level
  effort preferences never leak into internal stages.
- **Tiers**: Anthropic effort expresses as thinking budgets derived from output ceilings
  (max = the top budget); OpenAI as reasoning-effort strings up to `ultra`; Gemini as
  its native thinking controls. The mapping lives in one place.
- **Context**: declared windows must be *requested* windows — Sylk declared 1M contexts
  and never sent the beta header that enables them. If the config says 1M, the request
  layer asks for 1M, and utilization math uses the real number (Sylk hardcoded 100K in
  its handoff manager, corrupting every utilization ratio).
- **Pinning**: a pod's model configuration is resolved and pinned at summon. Config
  changes reach live work only via handoff.

## 3. Failover

When an agent's primary provider fails: an **in-place swap to the alternate at a turn
boundary** (the transcript is provider-agnostic), sticky until the current claim reaches
a terminal state, then revert to primary.

Failover is an **operational event**: config-driven, logged, never ledger state, and
never called a handoff — handoff means agent replacement for context or performance,
nothing else.

## 4. Handoff

The replacement of an agent instance, preserving its work.

- **Context handoff**: the primary hit its context threshold (derived from the model's
  real window). Scribe-initiated, unilateral.
- **Performance handoff**: degradation or drift from user intent, detected by the
  detection substrate and SPRT-confirmed. The Scribe requests; the Guardian
  adjudicates with the fresh-context probe as its single evidence request — and
  **may deny** (fresh-also-fails ⇒ task-hard ⇒ the corrective targets the claims;
  `HANDOFF.md` §8; amended 2026-08-22 — supersedes "cannot outright block").
- Continuity: the successor keeps the predecessor's UID chain; the Scribe supplies the
  narrative digest; claims, parked turns, and validations survive because they are
  ledger state. Identity is re-resolved at access time, never captured into cloned
  state — frozen identity is Sylk's duplicate-agent bug family.
- Handoff is a fast-forward multi-step operation: idempotent, self-checking steps that
  verify their own completion, so a crash mid-handoff resumes at the missing half.

## 5. The health plane

One consolidated per-agent signal stream — not Sylk's four disconnected subsystems.

Signals: token progress, turn quality (stop-reason, tool-call coherence, output ratio),
context fit against the real window, claim coherence (is the agent's ledger activity
consistent with its assignment), liveness, and resource pressure from the pod boundary.

Consumers: each agent's **Scribe** (handoff judgment, narration), the **performance
score service** (§6), the **Guardian** (adjudication of detected conduct and
performance — the detection substrate detects, the Scribe judges; resource
response), and the
**Guide** (orchestration visibility). Health is continuous scoring feeding judgment —
never a string-matched phase roll-up, and never itself an authority: agents and the
Guardian act on it.

## 6. The performance score service

A harness service computes each agent's per-domain performance score from
ledger-observable outcomes — validation pass rates, challenge outcomes, corrective
frequency — weighted by prevalence, specificity, and trust, with the agent's Scribe as
its per-agent signal source.

- Modulates rank **bindingness only** (`AGENTS.md` §2.2): demotes a poorly-performing
  agent's feedback from binding to advisory; never inverts rank order.
- Harness state, computed by the pushed single-writer score service — a pure fold
  over its logged input stream, snapshots entering the ledger as logged core
  inputs (`MONITORING.md` §8; amended 2026-08-22 — supersedes "derived on
  demand"). Not ledger state, not user-visible ranking.
- The Scribe feeds signals but does not compute scores — it initiates handoffs on the
  same data, and holding both powers would let it manufacture evidence for its own
  trigger.

## 7. Observability boundaries

Three homes, no blending:

- **Ledger**: claims, testaments, validations, artifacts. Work truth, and only work
  truth.
- **Config**: SafetyPolicy, ModelConfig, rank matrix, budgets' derivation anchors.
  Versioned; changes are config history.
- **Logs**: operational events — failovers, admissions, gate decisions' telemetry,
  resource pressure, protocol health. Structured, queryable, and *not* a second work
  authority. The Logs home's machinery is `COLLECTOR.md` (accepted 2026-08-22).

The UI renders from ledger deltas for work and from logs for operations; it never infers
work completion from anything but lifecycle deltas.

## 8. The Sylk fault ledger — do not repeat

Findings from the full Sylk survey (agents, VFS/OT, substrate). Each entry is a
structural rule for Hecate, not a criticism to remember fondly.

**Wiring and truth:**
1. *Durability was optional and off.* The production session was constructed without a
   storage root, so the entire WAL/recovery discipline — code-complete — never ran.
   Hecate: durability is not a config option; there is no constructor without it.
2. *Capabilities shipped half-wired.* MCP runtime, substrate provisioner, cache tiering,
   fabric bridge, conflict-resolution UI — all built, none reachable. Hecate: a
   capability ships wired with an end-to-end proof, or it does not ship.
3. *Two parallel paths for one concept.* Dual consult mechanisms, dual write paths with
   different contracts, four copies of pod composition, three model-config authorities,
   two effort systems, two content stores with two hash families. Hecate: one authority
   per concept; the second implementation is the bug.

**Merge and VFS:**
4. *No conflict detection at merge.* The only conflict branch was unreachable; the
   resolver was a no-op; the one stale-base check was dead code. Hecate:
   canonical-rebase merge with a deterministic conflict verdict (ADR-0005), leases as
   guidance only.
5. *Rejected work reached disk.* Green was never reverted and flushes wrote the whole
   overlay. Hecate: green admits only increment-validated work; disk commits are
   per-descriptor; failure fixes forward (ADR-0003).
6. *Audit read a base predating the merge under audit.* Post-merge audit is gone
   entirely; validation precedes merge.
7. *A merge could succeed and become unrecordable* (applied to green, then failed to
   enqueue). Hecate: merge application and its ledger record commit together or not at
   all.
8. *Nondeterministic iteration ordered the record* (map-ordered modification lists).
   Hecate: deterministic ordering everywhere a record is produced.

**Authority and prompts:**
9. *Keyword classifiers gated behavior* (minimal-planning bypass, deterministic fallback
   planner, guardian intent keywords, response-censoring substring matches). Hecate:
   agentic invocation with structural consequences; no keyword gate ever decides work.
10. *Prompts drifted from registries* (guardian prompt advertised 10 of ~40 skills,
    claimed "never auto-approve" while code auto-approved). Hecate: prompt surfaces that
    describe capability are generated from the registry, and enforcement statements are
    generated from the policy that enforces them.
11. *Docs lied about models and tools* (model tables wrong on nearly every row; tools
    specified that never existed). Hecate: docs in the same change as behavior; where a
    doc can be generated from config, it is.

**Resources and process:**
12. *The resource machinery existed and its supposed consumer never imported it.*
    Hecate: the Guardian is wired to the budget/pressure plane from day one.
13. *Hardcoded context sizes corrupted decisions* (100K in the handoff manager).
    Hecate: every window/threshold derives from the pinned ModelConfig.
14. *Silent degradation* (scribes silently disabled without their provider; sandboxes
    silently non-hermetic; caps silently ignored on two of three platforms). Hecate:
    degradation is loud, labeled, and consented — a run that isn't hermetic says so to
    its consumers.
15. *Untracked goroutines and unbounded caches existed despite the rules.* Hecate
    (Rust): ownership makes the discipline structural — every task owned by a scope,
    every queue bounded by a derived budget, every drop counted.
