# SPEC: the agent runtime — turns, parking, skills topology, handoff

Status: presented for acceptance (grilling Branch 7; 7a/7b ratified; 7c ratified as
warden + sensor, `PODS.md` §6). The agent runtime is the in-guest process every
agent is: intake, turn engine, skill dispatch, context manager, **history emitter**
(runtime instrumentation below the model — never a "Scribe feed"; MONITORING §5c;
amended 2026-08-22) — the PRIMARY of a **two-process guest** (its Scribe companion
runs its own right-sized hecate-rt; MONITORING §1). Shard counts are
bundle-declared with their derivations (primary N-from-cores, Scribe clamped to 1;
census N+1) — running on hecate-rt with the blocking rustls provider pool
as its only threads beyond the shards.

## 1. Intake

- The runtime subscribes to its identity's delta streams (credit-based,
  `PROTOCOL.md` §4). **Event-driven, expectation-matching**: every delivered delta
  is either the fulfillment of an expectation registered at emission or a standing
  identity subscription. No polling, no pull loops.
- **One concern per dispatch**: each dispatch delivers one causally coherent entry
  point (a directed claim, a consult answer, a challenge — never mixed in one
  prompt). The agent chooses traversal depth from the entry point.

## 2. Turns (7a: serial per instance)

- **Exactly one live LLM turn per instance, ever.** One transcript, one coherent
  context, one prompt-cache lineage, one narratable sequence. Parallelism is
  summoning more pods — never interleaving one agent loop's mind (a pod holds TWO
  minds — the primary and its Scribe, each under this same one-turn discipline;
  MONITORING §1; amended 2026-08-22).
- A turn: stage resolution (the ModelConfig stage catalog is the **sole** effort
  authority — model, reasoning tier, budgets per stage; internal stages pinned
  cheap) → context assembly → streaming LLM loop → skill invocations → artifacts
  stream to the ledger → closing testament. Progress updates are observational and
  never terminal.

## 3. Parking and yield

- A consult, challenge, or dependency wait mid-turn **yields**: the turn snapshot
  parks in instance memory; the dependency wait is **durable claims-graph state**
  (the satisfaction monitor); the instance proceeds to its next claim. Resolution
  re-enters the parked turn with the answers.
- **Long tool executions yield the same way**: a tool invocation exceeding its
  derived threshold (test suites, builds) parks the turn with completion as the
  dependency — the instance takes other claims instead of idling with no LLM call
  in flight. Priced cost: resumed turns churn cache affinity; idle instances cost
  more.
- **No in-memory state is load-bearing across instances**: an instance death loses
  only ergonomics, never work — parked turns reconstruct from the claims graph plus
  the Scribe brief at the current watermark (resume-by-reconstruction). Suppressed
  testament flushes on yield prevent premature testimony.

## 4. Skill topology

- **Built-in skills are compiled into the runtime** and execute in-guest.
  TS/Python skills execute in-pod in their image-shipped runtimes behind the typed
  contract. The MCP projection is the wire form; in-guest invocation of a local
  skill is a direct call — same contract, no loopback theater.
- **Work-volume operations go through the guest mount**: reads and writes are ordinary
  file operations against the virtio-fs view; leases are validated server-side; the
  **warden** decides every crossing pre-effect. Toolchain invocations (compiler,
  tests, linters) are plain guest processes — native speed, zero gate round-trips.
- **Claims operations ride the claims plane**; external MCP servers are reached
  through the host network stack under warden policy and Guardian staging.
- **There is no call-site permission round-trip for in-guest actions** — the warden
  + sensor stack (`PODS.md` §6) replaced it. Boundary *grants* (external egress,
  disk staging, provisioning, summons) surface as `guardian_check` claims when the
  warden escalates; the runtime experiences a held syscall or a typed denial, and a
  parked turn if the hold outlasts the derived threshold. **A hold that outlasts a
  second, larger derived threshold (human-latency approvals) tears the pod down
  entirely**: claims park durably, the work volume persists host-side, and the
  verdict re-summons and reconstructs — arbitrarily long approval latency costs
  zero resident memory.

## 5. Context management

- Context accounting uses the **pinned ModelConfig's real window** — never a
  constant (the Sylk 100K hardcode is the named fault). The Scribe consumes
  utilization from the health plane; crossing the derived threshold triggers the
  unilateral context handoff.
- Context assembly: system prompt modules resolve from the bundle (registry-pinned
  at summon); claims context enters as entry-point traversal, not preassembled
  grab-bags; cache affinity is sticky per instance and resets at handoff.

## 6. Handoff execution (7b: brief + claims, never transcript)

Fast-forward idempotent steps, each self-checking:

1. Trigger: Scribe (context — unilateral) or Scribe→Guardian (performance — one
   evidence request max).
2. Successor summons through the warm tiers with the bundle **re-resolved** (the
   only moment config reaches live work).
3. **The predecessor's work volume re-binds to the successor** — volumes are
   host-side manifests that outlive instances by construction; uncommitted overlay
   work transfers by re-bind, never by copy. A self-checking step: the successor
   verifies the volume's manifest head against the claims' declared bases before
   taking work.
4. Successor receives the Scribe's narrative brief; open claims re-seed at the
   watermark; parked turns reconstruct; the transcript is **not** transferred.
5. Predecessor receives the drain order via init: finish nothing new,
   terminal-abort barriers up (the primary emits no narration — narration is the
   Scribe's, and the SCRIBE's flush gates teardown; MONITORING §6; amended
   2026-08-22).
6. Keys and fencing rotate; the predecessor's frames die at both checks; UID chain
   continuity records successor lineage.
7. Execution completes per `HANDOFF.md` §9 (amended 2026-08-22): ALL open claims
   ADOPT under the chain (none force-closed, no status invented); volumes
   re-attach under the bumped `key_epoch`; the parked turn resumes with the
   suppressed testament flush (no duplicate testimony); the successor's detectors
   FIR-seed; a performance handoff may escalate the ModelConfig tier.

A crash at any step boundary resumes at the missing half — never a duplicate agent,
never a lost claim.

## 7. Test matrix (failure each catches)

| # | Test | Catches |
|---|---|---|
| R1 | Serial invariant: instrumented runtime never has two live LLM calls; parked turns don't count | braided context; cache-lineage destruction |
| R2 | Yield equivalence: consult answered via park/resume ≡ synchronously available answer (outcome corpus) | parking changing semantics |
| R3 | Instance-death reconstruction: kill mid-park at arbitrary points; successor completes every open claim from durable state | in-memory state quietly load-bearing |
| R4 | One-concern audit: no prompt contains two unrelated entry points | grab-bag dispatch |
| R5 | Stage pinning: routing/commentary stages never resolve to conversation-tier effort, under any session setting | the Sylk effort-leak fault |
| R6 | Handoff sweep: kill at every step boundary ⇒ exactly-once completion, no duplicate identity, no lost claim, keys rotated | half-handoff limbo |
| R7 | No-transcript-transfer: structural — no code path serializes a transcript across instances | continuity by bloat |
| R8 | Topology conformance: no skill performs call-site gate round-trips for in-guest ops; every boundary effect traverses the warden (arch test with `PODS.md` T-series) | permission theater returning |
| R9 | Held-syscall behavior: warden hold ⇒ bounded block, then parked turn with typed reason; denial ⇒ typed error artifact, turn continues | agent wedged on a gate |
| R10 | Long-tool yield: tool exceeding its threshold parks the turn; instance progresses other claims; resume consumes the tool result correctly | idle instances during builds/tests |
| R11 | Hold-teardown: a human-latency hold tears the pod down; verdict re-summons; reconstruction completes all claims with the re-bound volume intact | resident-memory bleed on approvals; volume orphaning |

## 8. Acceptance criteria

1. R1 and R3 CI-gated permanently: serial turns, and reconstruction-completeness of
   parked work.
2. The stage catalog is the sole effort authority; R5 holds under every
   configuration.
3. Handoff is fast-forward (R6 sweep permanent); no transcript-transfer path exists
   in the tree (R7 structural).
4. Every context threshold and budget derives from the pinned ModelConfig; no
   window constant exists.
5. History events are runtime-emitted below the model — zero token cost, capture
   continues mid-inference; no per-turn feed exists; teardown gates on the
   SCRIBE's flush (MONITORING §5c/§6; amended 2026-08-22).
6. Ratcheted floors: non-LLM turn overhead (intake→prompt-ready) and dispatch
   latency from first CI baseline.
