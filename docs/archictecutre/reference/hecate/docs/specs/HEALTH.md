# SPEC: the health plane — one signal stream, judgment at the edges

Status: ACCEPTED 2026-08-16 (amended under maximal audit — per-class absence
semantics, the content-free law extended to all ledger content by user
direction, Branch-14 ownership boundary). Direction ratified in `PLATFORM.md`
§5: one consolidated per-agent signal plane replacing Sylk's four disconnected
subsystems; health informs, agents act. References: `PODS.md` (warden/sensor telemetry, node
rollup), `AGENTS_RUNTIME.md` (turn signals), `RANK.md` (score service consumption).

## 1. Signals

Per agent instance, computed from sources that already exist — the plane adds no
probes, it consolidates streams:

| Signal | Source |
|---|---|
| Token progress | agent runtime (streaming LLM loop) |
| Turn quality (stop reason, tool-call coherence, output ratio) | agent runtime |
| Context fit | runtime accounting against the **pinned ModelConfig window** — never a constant |
| Claim coherence (ledger activity consistent with assignment) | frontier/ledger observation |
| Liveness | protocol keepalives + init lifecycle events |
| Pod pressure (memory, fs-op rates, held/escalated ops) | warden + sensor telemetry |

All windows, EWMA weights, and thresholds are **derived** from observed activity
volume and the pinned configuration; every derivation lives at its definition site.

- **Ownership boundary**: context-fit's *accounting method* (provider-reported
  usage vs local estimate; what counts toward the window) and its threshold
  derivation belong to Branch 14 (handoff detection math) and slot in without
  changing this spec's shape — this spec owns the signal's plumbing and
  delivery only. No second spec ever owns that formula.
- **Absence semantics, per class (amendment)**: every signal class declares
  what its silence means —

  ```rust
  enum AbsenceIs {
      Degraded, // liveness, sensor: silence IS the signal (fails closed)
      Unknown,  // composite marks stale; the last value is NEVER frozen forward
  }
  ```

  — with a derived freshness bound per class. Consumers always receive
  `(value, freshness)`: staleness is data they judge, never a hidden default
  (H7).
- **The content-free law (amendment, extended by user direction)**: signals
  carry operational measurements only — counters, rates, durations,
  byte/token counts, closed-vocabulary enum codes, threshold crossings, and
  opaque references (UIDs, content hashes). **Never**: work content of any
  kind — no code, no file paths, no free text — and **no ledger content:
  no claim content, no testament content, no validation content, no artifact
  bytes, no multi-media of any form**. References may point at such objects;
  their content never rides the health plane. Structurally: no signal type
  contains an unbounded string or bytes field — the type walk is the test
  (H8). Execution spans (`TRACING.md` §3) are signal types under this law —
  chokepoint names are a closed registry, statuses a typed taxonomy, and the
  H8 type-walk covers span types identically. Narrative belongs to Scribes,
  inside their session.

## 2. The node health service

A deterministic harness service per node: consumes the streams above, maintains
per-agent composite state as its own re-derivable state, and **rolls up at the
node** — cross-node health rides the gossip plane piggybacked under the datagram
budget (`PODS.md` §3). Fan-in is bounded by node count, never pod count.

Consumers, each with its own judgment:
- **Scribes** — handoff judgment and narration color for their primary.
- **The score service** (`RANK.md` §4) — outcome-weighted inputs.
- **The Guardian** — conduct analysis and resource response.
- **The Guide** — orchestration visibility (a summon that misses budget, an agent
  that stalls).

The health service itself has **no authority**: it cannot gate, author claims, or
trigger anything. Health is continuous scoring feeding judgment — never a
string-matched phase roll-up, never an enforcement layer.

## 3. Scribe triggers

- **Context handoff**: the derived utilization threshold crossing is sufficient
  cause — Scribe-initiated, unilateral, no evidence bundle required (the number is
  the evidence).
- **Performance handoff**: the Scribe assembles an **evidence bundle** — the signal
  classes that degraded (with windows and baselines), plus its narrative of intent
  drift — and requests via the Guardian, which may soft-gate by requesting more
  evidence exactly once. The bundle's required classes are specified so the
  Guardian can adjudicate from the request alone in the common case.
- New signal classes enter **observe-mode first** (computed, logged, feeding no
  trigger) until their distributions are seen — the standing rollout discipline.

## 4. Tests and acceptance

| # | Test | Catches |
|---|---|---|
| H1 | Signal determinism: identical input streams ⇒ identical composites, in SIM and across platforms | nondeterministic health verdicts |
| H2 | Threshold derivation: swapping the pinned ModelConfig moves context thresholds; no constant exists (the Sylk 100K hardcode is the named regression) | window constants returning |
| H3 | Evidence completeness: a performance-handoff request carries every required signal class; the Guardian's single evidence request is exercised, and a second is structurally impossible | soft-gate becoming a stall loop |
| H4 | No-authority: the health service can gate nothing and author nothing (architecture test) | health becoming enforcement |
| H5 | One plane: no second health/liveness subsystem exists in the tree (architecture test — Sylk ran four) | fragmentation returning |
| H6 | Rollup bounds: control-plane health traffic bounded by node count under 10× pod growth (with `PODS.md` T10) | telemetry self-DDoS |
| H7 | Absence semantics: stalled-source fuzz per class ⇒ Degraded classes degrade, Unknown classes surface staleness; no consumer ever reads a frozen stale value as fresh | interpolation-as-lying; silent blindness |
| H8 | Content-free structural: type walk over every signal type — only numeric/duration/closed-enum/UID/hash field types exist; no unbounded string or bytes field; no ledger-content type is reachable from any signal type (architecture test) | work or ledger content leaking into the operational plane |

Acceptance: H1/H4/H5/H8 permanent; all consumers read the one stream; every
threshold derived with its derivation in place; observe-mode gate for any new
signal class or trigger formula; every signal class declares its `AbsenceIs`
semantic and freshness bound at its definition site.
