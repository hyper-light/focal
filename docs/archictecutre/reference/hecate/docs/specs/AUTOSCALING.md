# SPEC: the autoscaler — a deterministic target-tracking controller

Status: ACCEPTED 2026-08-16 (sub-decisions (i) ratio-law-only, (ii)
scale-to-zero; amended under maximal audit — four corners closed:
load-proportional signals only, silence-fails-closed freshness, parked-scope
drain semantics, ratcheted tolerance). Companion to `PODS.md` §4b (classes,
pull path, per-role table). References: Kubernetes HPA algorithm (ratio target-tracking,
tolerance, downscale stabilization — the deliberately-not-PID production consensus),
Little's law / M-M-c for target derivation, Sylk AUTOSCALING doctrine (parking,
singleflight, five-outcome taxonomy), the AIMD step discipline.

## 1. Architecture

One implementation, two instantiations — both deterministic hecate-rt tasks,
SIM-able, replayable:

- **Session autoscaler**: lives in the session colocation unit; owns replica
  targets for the session's load-driven daemons (Arbiter, Guardian, Inspector,
  Archivalist, Guide intake) and the advisory stream for work-driven roles.
- **Node autoscaler**: owns warm-pool depth and infrastructure capacity (egress
  threads) per node.

Inputs are streams that already exist: the health plane's queue depths, latencies,
and arrival rates; admission shed and `budget_exhausted` counters; in-flight
actuation state. The evaluation cadence derives from the signal windows (never
faster than the signals can change).

## 2. The control law: ratio target-tracking

Per scalable target, declared in its descriptor (`AgentRole` / service config):
a **signal** and a **target value** whose derivation is stated.

- **Signals are load-proportional classes only** (amendment): queue depth |
  arrival rate × service time | utilization — classes that scale ~inverse-
  linearly with replicas, which is the ratio law's validity condition.
  **Latency percentiles are never controller signals** (p99 is convex in load;
  the ratio law over/undershoots on it — HPA's own guidance): latency
  *budgets* enter through **target derivation** — Little's law converts a
  latency budget into the queue-depth/concurrency target the loop tracks
  (e.g., Guardian held-syscall p99 budget → derived hold-queue depth target;
  Arbiter review-staleness budget → frontier-lag target).
- **Freshness, silence fails closed** (amendment): every signal carries a
  freshness bound derived from its window; a stale signal ⇒ **hold** — no
  actuation in either direction — plus a loud health-plane alarm. The
  controller never acts on data older than its derived bound (A11).

```
desired = ceil(current_replicas × observed_signal / target_value)
```

- **Tolerance band**: no action while `|observed/target − 1| ≤ τ`, with τ
  derived from the signal's **commissioning-baseline variance, ratcheted**
  (the standing floor-ratchet pattern) — never continuously adaptive, which
  would let sustained flapping widen τ and mask the very drift that caused it;
  re-derivation only at declared re-commissioning points. Not a hand constant.
- **Asymmetric response**: scale-up acts on the *current* desired immediately;
  scale-down acts on the **maximum desired over a trailing stabilization window**
  (window derived from service time × a stated factor) — the flap-killer.
- **Bounded steps**: per-evaluation change is clamped (AIMD-flavored: at most
  double up, at most halve down), so a corrupted signal produces a bounded wrong
  action, never a cliff.
- **In-flight accounting**: desired compares against `current + pending`
  actuations; a target with materializing summons is not re-actuated. Stampedes at
  the cold path are already impossible (singleflight per cold target, `PODS.md`).
- **Bounds**: min/max per target derived from office obligations (an office with
  pending obligations floors at 1), node capacity, and budget anchors — admission
  enforces ceilings regardless (§4).

**PID and model-predictive controllers are rejected with reasons on file**: tuning
burden, integral windup under actuation latency, and the production record — HPA's
ratio law is the deliberately-boring choice proven across the largest fleets;
queueing models inform *targets*, not the loop.

## 3. Actuation

- **Scale-up** = summon requests through the front door: Guardian admission applies
  exactly as to any summoner (anti-runaway is structural, not autoscaler
  discipline); pull path is the warm tiers with assignment-only binding, zero
  registry reads (`PODS.md` §4b, T19).
- **Scale-down** = graceful drain: select the replica with fewest in-flight claims
  (ties: newest, preserving warmed caches); mark draining → takes no new claims →
  completes or parks → teardown fast-forwards; its claims redistribute by lease
  redelivery. **No replica ever dies mid-claim** — where "mid-claim" means
  mid-turn-execution: a **parked scope is not a mid-claim death** — it transfers
  via the AGENTS_RUNTIME brief+claims handoff (successor resumes from the
  ledger; volume re-binds; no transcript), and A5 asserts parked-scope resume
  equivalence on the successor.
- **Scale-to-zero** (sub-decision ii): an idle load-driven daemon with an empty
  queue and no office obligation scales to zero; the first demand signal
  re-summons through parking/singleflight with warm-tier latency. Offices with
  standing obligations floor at 1.
- **Work-driven advisories**: for Engineer/Designer, the autoscaler computes
  backlog statistics and delivers them to the **Guide as ambient information** —
  judgment input, never actuation. The autoscaler cannot summon workers.

## 4. Boundaries

- Decisions, inputs, clamps, and outcomes are **operational logs** (full
  input-vector per decision — signal, target, desired, clamp applied, action) —
  landing on the scope-tagged operational log's `op-standard` lane
  (`COLLECTOR.md` §4; amended 2026-08-22, COLLECTOR acceptance).
- The **ledger** carries only what is its to carry: the summon/teardown claims the
  actuation issues.
- Targets and derivations are **config** (registry descriptors), versioned.
- The loop is a pure function of (signal stream, config): same inputs, same
  decisions, in SIM and REAL.

## 5. The two loops compose

Replica scale-up **consumes** warm pool; the node autoscaler **replenishes** pool
toward `expected arrivals within the boot-latency horizon × derived safety factor`.
The two loops run at cadences derived apart (pool slower than replicas) so they
cannot resonate; A9 tests for cross-loop oscillation explicitly.

## 6. Test matrix (failure each catches)

| # | Test | Catches |
|---|---|---|
| A1 | Determinism: same signal stream ⇒ same decision sequence, SIM and cross-platform | nondeterministic scaling |
| A2 | Flap resistance: signal oscillating within tolerance ⇒ zero actions; step change ⇒ bounded ramp, no overshoot past step clamps | oscillation; cliff actuation |
| A3 | Asymmetry: up within one evaluation; down only after the stabilization window's max-desired agrees | premature downscale killing warm capacity |
| A4 | In-flight accounting: pending summons counted; no re-actuation while materializing | double-scaling races |
| A5 | Drain safety: scale-down under load ⇒ zero mid-turn kills; claims redistribute; **parked scopes resume equivalently on the successor** (brief+claims handoff); drained replica's scribe flushes narration | work loss on downscale; parked-scope stranding |
| A6 | Berserk autoscaler: forced pathological targets ⇒ admission ceilings hold, five-outcome accounting complete | the front door failing its one job |
| A7 | Scale-to-zero round trip: idle → 0 → demand ⇒ one singleflight activation, ready within the warm-tier budget | cold-start regression; activation stampede |
| A8 | Advisory isolation: work-driven signals reach the Guide as information; no code path lets the autoscaler summon a worker (structural) | orchestration authority leaking to a controller |
| A9 | Loop composition: sustained demand ⇒ replicas and pool converge without cross-loop oscillation | resonance between the two loops |
| A10 | Low-trust coupling: flipping `trust_mode` ⇒ Guardian hold-queue target tightens and replicas ramp; hold latency stays within budget | "low-trust means slow" |
| A11 | Staleness hold: stalled signal stream (dead publisher, stuck cursor) ⇒ zero actuation in either direction + loud alarm; recovery resumes control cleanly | acting on stale data; silent controller blindness |

## 7. Acceptance criteria

1. A1/A5/A6/A8/A11 permanent CI gates.
1b. No latency-percentile signal exists in any descriptor (architecture test);
   latency budgets appear only in target derivations.
2. The ratio law is the only controller in the tree; no PID, no per-target bespoke
   loops — a target is a descriptor, never code.
3. Every constant (tolerance, windows, factors, floors, cadences) derives from
   stated anchors at its definition site.
4. All actuation passes Guardian admission; the ledger sees only summon/teardown
   claims; decisions are fully reconstructible from operational logs.
5. The same binary serves laptop (targets mostly 0–1, pool ≈ 0, snapshot-primary)
   and fleet (pool-primary) from the same formulas — no mode flags.
