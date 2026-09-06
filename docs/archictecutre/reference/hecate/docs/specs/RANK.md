# SPEC: rank enforcement — authority derived, refusal structural

Status: presented for acceptance (grilling Exchange 8, recommendations accepted).
References: `AGENTS.md` §2 (semantics + FINAL matrix), `LEDGER.md` (affordances,
Refuse cases), claims-architecture scrutiny findings A1–A5 (applied to LEDGER.md).

## 1. Representation

- Feedback and challenge claims carry a **`domain`** field — a closed hecate-wire
  enum matching the rank matrix's row vocabulary exactly (one source generates both).
- **Bindingness is derived at evaluation time, never stored**: effective authority =
  (static matrix order for the domain) × (current performance modulation of the
  author). A demotion mid-flight changes the next evaluation, rewrites nothing, and
  replays identically from the same inputs.
- The matrix is versioned config, shipped with the harness, immutable at runtime.
- **Relation to the IAM plane (amendment 2026-08-18, `IAM.md` §7.2).** Rank is a
  CONSUMER of the authority plane, not a second authority: the matrix is compiled
  into a shipped **rank policy pack** — a *projection generated from this single
  config source* (the same source that generates the `domain` enum; drift is a build
  error, criterion 2), immutable via the management surface, root-anchored. The pack's
  statements compile **only** to the ledger affordance guard (the refuse rule below
  stays the sole enforcement point); modulation enters as a context attribute sourced
  from the logged score snapshot (never live state), so the rank pack is **exempt from
  the §5 freshness fence** — its inputs are all core-local and the verdict lands
  exactly per this file's table (K1: no third outcome). RANK.md remains the owner of
  matrix content and semantics; IAM owns only the compilation and distribution.

## 2. The refuse rule (the only rank enforcement point)

- **Override-shaped** = a post carrying an `invalidates`/`supersedes`-class relation
  (typed, closed enum — scrutiny finding A2; a string match here is banned) whose
  target is feedback authored by an agent of higher effective rank in that domain.
- Such a post is **structurally refused** at the ledger — a typed error naming the
  rule, counted — joining writer-disjointness and self-targeting in the narrow
  Refuse set.
- Precisely scoped: **clarification challenges are never rank-checked** (always
  legal, any rank, any score); ordinary testaments are never rank-checked; only
  override-class relations trigger the check.
- Check inputs: the matrix (config), the claim's domain (field), the author's
  modulation — read from the last **event-carried score snapshot** in core-local
  state (`LEDGER_CORE.md` §2 sub-decision (c): the score service pushes
  modulation snapshots as ordinary inputs; no synchronous query escapes the
  core; determinism and replay hold by construction).

## 3. "Cannot ignore" — already the claims graph

Binding feedback is a claim with validations: it stays unsatisfied until the target
responds (comply via corrective work, or clarify). Ignoring it is visible as claim
staleness, blocks dependent satisfaction where linked, and escalates through the
issuer and the Arbiter's coherence watch. **No enforcement machinery is built here**
— the enforcement-atop-claims ban holds because the claims graph was always the
enforcement.

## 4. The performance score service

- Per `(agent uid, domain)`, computed by a deterministic harness service from
  ledger-**observable** outcomes; the agent's Scribe is its per-agent signal source
  (signal source, never score authority — the detection substrate detects, the
  Scribe judges, the Guardian adjudicates; `HANDOFF.md`). Architecture: the score
  service, `MONITORING.md` §8 (amended 2026-08-22 — supplies what this spec
  deferred).
- Inputs, weighted: **prevalence** (judgments that prevailed — challenges upheld,
  validations that stuck), **specificity** (artifact-backed findings vs bare
  assertion), **trust** (decayed track record — validation pass rates, corrective
  frequency). Windows and decay rates derive from observed activity volume.
- **Demote-only, never invert**: below the derived threshold, the agent's feedback
  in that domain drops to *advisory* — the target may proceed on its own judgment,
  recorded. Order never flips; recovery is symmetric.
- **Observe-mode first**: the service ships computing and logging scores while
  modulating nothing, until real-traffic distributions are seen — the same rollout
  discipline as new validators. Enabling modulation is an explicit, versioned
  config change.
- Scores are harness state; demotion events are logged; **the service writes no
  ledger OBJECTS — its sole ledger-directed output is the modulation snapshot
  pushed as an ordinary logged core input** (MONITORING §8; amended 2026-08-22 —
  the prior "nothing touches the ledger" predates the logged-inputs architecture).

## 5. Boundary participants

- **The user** is an external participant with supreme rank in every domain; posts
  addressing the user are always clarification-shaped (evidence-bearing challenges
  surfaced through claim presentation; the user's ruling is final).
- **The Guardian** appears in no challenge-target vocabulary; a challenge naming it
  fails at post time as malformed, not as outranked.

## 6. Test matrix (failure each catches)

| # | Test | Catches |
|---|---|---|
| K1 | Refuse table sweep: every override shape × (rank, domain, modulation) combination lands exactly per the table — refused or legal, no third outcome | authority holes; over-broad refusal |
| K2 | Derived-not-stored: demote an author mid-flight ⇒ prior evaluations unchanged, next evaluation demoted; full replay byte-identical | stored bindingness; replay divergence |
| K3 | Clarification immunity: clarification challenges never refused at any rank/score combination (fuzzed) | rank silencing questions |
| K4 | Score determinism: identical outcome streams ⇒ identical scores, across platforms and replays | nondeterministic authority |
| K5 | Observe-mode isolation: with modulation disabled, scores provably influence zero evaluations (structural) | silent early modulation |
| K6 | No-invert fuzz: arbitrary score vectors ⇒ order never flips; the only transition is binding↔advisory | inversion by arithmetic |
| K7 | Staleness escalation: ignored binding feedback surfaces via claim staleness and fires the escalation path | feedback rotting silently |
| K8 | Typed relations: the override check is an exhaustive enum match — a new relation variant without a rank ruling is a compile error | string-matched security |

## 7. Acceptance criteria

1. The refuse rule is the **sole** rank enforcement point in the tree; K1/K2 are
   permanent CI gates.
2. The matrix and the domain enum generate from one source; drift is a build error.
3. The score service cannot modulate until observe-mode gate is explicitly lifted
   (K5 structural); its formulas carry derivations at definition sites.
4. The score service authors no ledger objects; its modulation snapshots enter
   only as logged core inputs (MONITORING §8); demotions are logged with the
   inputs that produced them.
5. Clarification challenges are structurally exempt (K3 permanent).
