# The Ledger

*Amended 2026-08-20: §5 defers the delivery-class list to `PROTOCOL.md` §3
(authoritative, not re-enumerated); §7.1's reliable plane corrected to the
`hecate-quic` session plane (no TCP, per §7.3); §8's outbox replaced by delta-stream
cursor consumers (`LEDGER_CORE.md` §5 + AC-3).*

The ledger is **the durable proof of work**: proof of work required (claims), work
claimed (receipt), and work completed or failed (testaments — judged by validations,
evidenced by artifacts). It owns those four object families **and nothing else**. Configuration belongs in config; operational events belong in logs.
The ledger is Hecate's single work-coordination authority: no agent's synchronous
reply, progress text, or route response completes work — only ledger lifecycle does.

The object model and invariants are inherited from Sylk's claims architecture (the
`CLAIMS*.md` corpus), re-expressed in Hecate terms. Where Sylk said "board," Hecate
says ledger. Terminology follows `../../CONTEXT.md`.

## 1. Scope

On the ledger: claims, testaments, validations, artifacts, their relations, and their
lifecycle. Summons, consults, challenges, approvals, merges, corrective actions — all
of these are claims and validations, so all of them live here.

Not on the ledger: SafetyPolicy and any config (config), failovers and gate telemetry
(logs), performance scores (harness state), narration (Archivalist intake), VFS bytes
(the VFS). The ledger records *that* work happened and *what evidenced it* — never the
machinery around it.

## 2. Object model

**Claim** — a precise, atomic assertion or item of directed work, issued by one
participant against another, carrying the validations that define its satisfaction. A
claim is a constraint and an obligation, not a message. Claims carry scope entries
(`file | symbol | api | test_surface | component | ux_surface` → key) — and the claims
*are* the authorization for that scope; there is no separate scope-enforcement service.
Enforcement is layered by kind: **file** scopes are physics (the warden, at the fs
boundary), **region** overlap is mathematics (the merge verdict), and
**symbol/api/surface** scopes are judgment (the Arbiter's review) — stated so no one
assumes the warden enforces what only analysis can.

**Testament** — the uniform response to a claim. Artifacts stream to the ledger first;
the testament is the closing commit, on success *or* failure. Immutable once terminal:
corrections supersede or amend, never mutate. Testaments may represent partial work,
refusal, impossibility, or interruption — the shape never changes. Confidence is
explicit (`hint | tentative | committed | consensus`).

**Validation** — one atomic means of verifying a claim, paired with a quality bar: an
instruction to the evaluator (Description = what to check, QualityBar = what standard),
verifiable from artifacts. Types: `receipt`, `test`, `inspection`, `integration`,
`contract`, `design`, `regression`. Receipt validations auto-pass on testament arrival
— proof of delivery, never proof of quality. A validation may carry both a typed
deterministic handler and a quality bar; the agentic phase runs only after the
deterministic phase passes. Both produce identical verdict shapes downstream.

**Validation provenance** — a claim's effective validation set is composed from issuer
intent plus advisory priors plus Guardian policy validators, each entry recording
`contributed_by`. Provenance is descriptive, never authoritative: the issuer owns the
set; advisors influence via prevalence, specificity, and trust — never by writing
validations directly. New programmatic validators enter in observe mode; promotion to
required is an explicit decision, never automatic.

**Artifact** — typed evidence attached to a testament. `Kind` is open; `Data` is
schema'd; `ContentHash` is immutable. **Errors are artifacts**: a failed operation
produces a testament with error artifacts, not a thrown error. Every error artifact
declares its **disposition** — `retryable` (capacity, transient dependency, provider
backpressure; may carry a *derived* retry hint) or `terminal` (unknown subject,
permission denied, invariant violation; names the authority that makes retry
pointless). Remediation decisions ground in the declared disposition, never in string
inspection of error text.

**Relations** — all structure is uniform typed edges: `issuer`, `subject`, `evaluator`,
`claim_action`, `supersedes`, `depends_on`, `awaits`, `caused_by`, `refines`,
`conflicts_with`, `derived_from`, `reviews`, `amends`, `contributed_by`. No
special-case fields. Relations and action types are **closed, append-only
hecate-wire enums** — never open strings (the rank refuse rule keys on
`invalidates`-class relations; a security check matches a type, not a string) — with
a machine-checked bijection between action types and the delta vocabulary, so
enum/taxonomy drift is a build failure. Agents author their own relations by UID and address peers by
type; the ledger canonicalizes to UIDs at post time. `caused_by` parentage is stamped
where the turn is minted (the principal carries it), so an unparented claim is
unrepresentable — Sylk's forgot-to-attach-caused_by bug family closes structurally.

**Amended 2026-08-22 (COLLECTOR acceptance):** the system-written lifecycle record
additionally carries **`trace_refs`** — the bounded set of execution-trace ids
(`TRACING.md` §1) of the operations that posted and serviced the claim.
Runtime-stamped like every lifecycle field, never agent-authored; it enables the
claim ↔ trace pivot by opaque reference (`COLLECTOR.md` §10) and stays firmly on
the metadata side of the split — no telemetry content enters the ledger.

**Participants** — anything that can issue, receive, or evaluate: `agent`, `service`
(deterministic), `system` (runtime), `external` (user, CI). Wire format, lifecycle, and
every downstream consumer are participant-agnostic — nothing branches on category.
Non-agentic claimants cannot attach validations with quality bars (enforced at post
time); their validation runs through the programmatic registry only. Service identity
is deterministic across restarts; agent UIDs re-resolve through the registry at access
time, never frozen into cloned state.

**Writer disjointness** — issuer-authored content and system-written lifecycle are
disjoint write paths. A write that crosses the boundary is rejected as a structural
invariant violation. Lifecycle transitions never rewrite content; content is never
re-submitted as lifecycle.

**Content identity** — the canonical hash over issuer-authored fields only, derived on
demand, never stored as an authored field. Re-posting identical content is a no-op, not
a duplicate; a lifecycle transition can never masquerade as a content change; duplicate
delivery or render is detectable at any seam by comparing identities.

## 3. Lifecycle

The canonical path, with a durable failure state at every boundary:

```
generated → posted → received → progressed*
  → testament_generated → testament_acknowledged
  → validating → satisfied
              | validation_incomplete | validation_failed | validation_errored
```

- **Generated ≠ posted.** Generated: durably on the ledger, not yet actionable.
  Posted: activated for its receiver. A plan's claims may be generated at plan time and
  posted only on approval — work cannot start early, and a failed posting is itself a
  durable fact.
- **Progress never completes work.** Progress updates are observational; no
  continuation resumes from them; the UI never infers completion from them. Calling
  progress on a terminal claim is moot, not an error (§5).
- **Testament closes; validation decides.** All required validations passing satisfies
  the claim; failures produce the distinct terminal states (missing evidence ≠ failed
  evidence ≠ validator error). Child and parent transitions commit in one ledger
  transaction, committed solely by the claimant runtime.
- **Consults, challenges, approvals, summons, handoffs are claims.** A consult is a
  consultation claim with (at minimum) a receipt validation; a challenge's thread is
  its claim's testament lineage; a Guardian gate is a validation with
  `evaluator = guardian` on the gated claim — **no separate approval envelope
  anywhere**. Their resolution deltas are never shed, because their issuer is parked on
  the outcome.
- **Self-targeted claims are rejected** before posting (or fail durably), except
  legitimate self-transfers (handoff).
- **One status vocabulary.** The canonical lifecycle above is the only claim-status
  set; affordance predicates (`IsTerminal`, `IsActive`) are defined over it directly.
  No coarse or legacy vocabulary exists, and no decode-only compatibility path ever
  will.
- **Deadlines are replay inputs.** Consult deadlines and claim expiries fire as
  ledger-core timer inputs (virtual time in SIM, the runtime timer wheel in REAL) —
  every timeout transition is a deterministic function of the input sequence, never
  of wall-clock observation.
- Synchronous service handlers may compress lifecycle states into one transaction; the
  wire-visible state sequence is unchanged.

## 4. The claims graph

The ledger *is* a directed graph — claims, testaments, validations, artifacts as
nodes; relations as edges — **with cycles**: agents legally consult each other
cyclically. Reads are entry-point plus `traverse(node, edge_filter, depth)`; each
delivered delta is a depth-1 entry point and the agent chooses how deep to go.

- **Blocking edges**: `awaits` waits for the dependency's *terminality* and proceeds
  even on its failure (consults: you want the answer either way); `depends_on` waits
  for *satisfaction* and propagates failure (hard prerequisites).
- **Satisfaction** is the least fixpoint of a node-local rule requiring local
  terminality — every member of an in-progress cycle is unsatisfied, with no
  greatest-fixpoint pathology. Layered propagation runs over the incrementally
  maintained SCC condensation (a DAG even when the graph isn't).
- **Parked turns** own a materialized monitor of their transitive blocking closure —
  pure delta-driven state, no live task; satisfied interiors collapse to a released
  token. Release is a monotone cut over the totally ordered delta stream: no lost
  wakeups, no torn reads.
- **Deadlock is a first-class output**, resolved deterministically: deadline first
  (the liveness floor), then eager break on the cycle's canonical victim — lowest
  claim sequence, no randomness.
- Supersession is the only retraction: an explicit arc rewrite, never a silent
  un-terminalize.

## 5. Affordances and deltas

**Affordance**: tool legality is a *derived* function of graph state — never stored —
along two orthogonal axes: the target node's lifecycle precondition, and the work
node's dependency satisfaction. Three responses:

- **Inform** (default): the ledger returns graph truth as a non-failing result. Acting
  on a terminal claim is moot, and the agent is told so, readably. Never a tool error.
- **Yield**: dependency-axis only — the turn parks and resumes when the closure
  settles. A yield, not a denial.
- **Refuse**: narrow, structural invariants only — malformed or unauthorized
  mutations, writer-disjointness violations, self-targeting, and the rank rule (an
  override-shaped testament from a lower-ranked agent against binding higher-ranked
  feedback in that domain).

Affordances never silently swallow. Every moot returns a readable result; every yield
resumes; typed errors are the single path (no untyped fallback).

**Deltas**: an immutable fact emitted after a ledger mutation commits — authoritative
and self-sufficient, never a hint. Receivers act on the delta directly and consult the
ledger only to traverse deeper. The envelope carries schema version, action (a closed
enum that *is* the lifecycle vocabulary), sequence, actor, delivery, and ordered refs.
Delivery classes are enumerated authoritatively in `PROTOCOL.md` §3 (referenced here,
never re-listed — a local re-enumeration omits classes as they are added and drifts
stale). Dedup rides `(delta_key, sequence)`
plus content identity. Intake is event-driven expectation matching — no polling, no
consumer loops; each dispatch delivers exactly one causally coherent concern.

## 6. The merge gate

How validated work reaches disk (ADR-0003). The flow:

1. Claims flow **to** the Engineer (issued by the Guide; generated via the Architect, a
   plan, or a research doc).
2. The Engineer's work **streams as increments** (artifacts). Each increment's
   increment-scoped validations pass — Guardian safety scan, lint, conflict check —
   and it **merges into green immediately** (canonical-rebase engine, `MERGE.md`).
3. The claim's whole-work validations — tests green, Inspector approval artifacts,
   design conformance, and the Arbiter's integration analysis — are evaluated by the
   **Arbiter** (merge-gate authority; the Architect joins on design questions) on the
   closing testament, and gate the **disk commit** (auto- or user-approved per
   SafetyPolicy `disk_write_mode`).
4. A failed closing validation **fixes forward**: corrective claims (Architect-authored)
   produce superseding increments. Green is never rolled back in place.

Invariant: **green = increment-validated work; disk = claim-satisfied work.** There is
no post-merge audit loop and no unvalidated byte in green — Sylk's
rejected-work-in-green, whole-overlay-flush, and stale-audit-base fault families are
unrepresentable.

Mechanics:

- **Canonical rebase with a deterministic conflict verdict** (ADR-0005, `MERGE.md`).
  The merge serializer position-maps increments through canonical history and
  *detects* overlapping/ambiguous regions — which reject into corrective claims,
  never silently interleave and never open an in-gate resolution session. The merge
  serializer is the sole mechanical merge authority; no agent hand-edits at the gate.
- **Leases guide, never guard.** An increment whose basis proves its paths disjoint
  from green's movement skips transform work entirely. Lease staleness informs; the
  conflict authority is the merge.
- Disk commits are **per-descriptor** — exactly the satisfied claim's content — with
  merge application and its ledger record committing together or not at all, and
  deterministic ordering everywhere a record is produced.
- The Designer's volumes live entirely outside this machinery; its disk path is the
  Guardian-staged overflow flow (`SUMMONING.md` §6, gate 3 lineage).

## 7. The protocol

The claims plane — ledger operations, delta streams, summon control, health — speaks a
ground-up **dual-stack protocol** (ADR-0002 — corrected to QUIC; **no TCP
anywhere in the mesh**, per §7.1 and `PROTOCOL.md` §1.2), modeled on hyperscale's
mercury-sync lineage: its transport core where it earned it, its author's AD-52
redesign where the code itself walked away, and its documented failure modes as the
design checklist. MCP is the tool plane and never carries claims traffic.

### 7.1 The two stacks

Transport selection is **static per message class** — declared where the handler is
declared, no runtime fallback, no size-based switching (hyperscale's honest lesson:
"dual stack" that works is two independent planes over one node identity):

- **UDP plane** — health, liveness, membership, gossip. Small, frequent,
  loss-tolerant, MTU-budgeted datagrams. Health piggybacks everything it can:
  per-agent signals, pressure telemetry, and handoff evidence ride the probe traffic
  under one datagram budget — computed at the *datagram* layer, envelope and AEAD
  overhead included (hyperscale budgeted the payload layer and could silently exceed
  MTU).
- **Session plane (`hecate-quic`)** — ledger operations, **delta streams**, summon
  control, transfers. Framed, ordered, resumable **over QUIC, not TCP** (matching
  §7.3 and `PROTOCOL.md` §1.2 — there is no TCP anywhere in the mesh). Delta
  subscription is a sequence-numbered stream with watermark resume: a consumer
  reconnects with its cursor and replays forward; behind-retention triggers
  deterministic full re-derivation, never best-effort repair.

### 7.2 Envelope and wire format

- Length-prefixed frames; the payload is length-prefixed *inside* the
  delimiter-parsed header so binary bodies can never confuse the scan (kept from
  hyperscale). One derived frame cap, consistent at every layer — not hyperscale's
  1 MB framer under a 3 MB validator.
- **Protocol version in the envelope**, negotiated at connect — not discovered at
  registration after a wrong-major peer has already been talking.
- **HLC timestamps** (AD-52), 8–16 bytes — not a 64-byte Lamport field spending 4.5%
  of every datagram on a small integer. Distinct send/receive/ack semantics so
  responses never inflate the clock.
- **Fencing on every RPC** (AD-52): `(cluster_id, epoch, sender_id, sender_term)` —
  a stale or half-dead sender is rejected by every receiver, not just at membership
  seams. Node identity is ephemeral per process start; a restart is never a rejoin.
- **Request IDs correlate responses.** Hyperscale correlated by
  `(peer, handler)` and two concurrent same-handler requests could receive each
  other's replies; Hecate's correlation is explicit per request.
- Serialization is schema'd typed binary with versioned envelopes (Rust-native; no
  pickle class to restrict). zstd compression with decompression caps and a
  ratio-bomb guard.

### 7.3 Security

(Reconciled 2026-08-18 — the former per-message-PSK/mTLS text described a
superseded key model; `PROTOCOL.md` §§1–2 and `WIRE_SECURITY.md` are the
ratified authorities. Summary only:)

- **Encrypt-always, both planes**: header-encrypted AES-256-GCM datagrams on
  the control plane (cleartext = the key-finding prologue only); the
  Noise-IKpsk2/private-QUIC session plane with the seal-once pipeline —
  one payload seal at the origin warden, one unseal at the destination;
  per-pod summon-mint roots with labeled derivations, atomic key_epoch
  rotation at handoff. No mTLS layer exists; there is no TCP in the mesh.
- **Integrity and replay live in the envelope parser at every acceptance
  point** — hyperscale's replay guard was well-built, well-tested, and never
  once executed on the wire path because a decorator erased the annotations
  that gated it. In Hecate the parser-resident order (length → prologue →
  key lookup → AEAD → fencing → replay window → admission → decode) is
  enforced and instrumented at host parsers, guest unseal paths, and
  terminal clients alike, before any dispatch decision exists.
- The Guardian's policing runs at endpoints (the warden seals what it
  inspected; verdicts are metadata-complete); network policing at the host
  stack applies regardless.

### 7.4 Admission and scheduling

Admission control runs **in the protocol callback, before any task exists** —
hyperscale's sharpest idea, kept and extended:

- Priority classes with per-class caps, plus **named admission groups with reserved
  slots** so the health plane owns capacity that data-plane overload cannot touch.
- Critical control traffic gets head-of-line scheduling into the next loop iteration.
- Delivery classes map onto admission: never-shed classes (directed,
  consult-resolved) versus sheddable observation traffic.
- Bounded queues everywhere, sized from derived budgets; **every silent drop is
  categorized and aggregated** (rate-limited / too-large / decrypt-failed / malformed
  / replayed / shed) as a periodic structured record — a drop with no signal is a bug.
- Flow-control drain is actually awaited on the send path; keepalive is real; a dead
  peer is discovered by the transport, not only by the other stack's failure
  detector.

### 7.5 Liveness discipline

- Dedup eligibility is a **structural property of each message type**, never a central
  denylist — hyperscale's leader-heartbeat-eaten-as-duplicate liveness bug, encoded as
  a rule.
- Terminal-abort barriers: a killed node must actually be dark — checked before
  dispatch and after handler return, with listener shutdown ordered so a restart can
  never inherit the dead process's accept queue.
- **Deterministic simulation is a production design constraint**: clock, randomness,
  and transport are seams in the shipping code; the test harness drives drop, delay,
  partition, reorder, duplication, corruption, and skew per peer pair. The protocol is
  not done until its invariant oracles pass under that harness.

## 8. Durability

- **WAL-first, always on.** Every mutation: prepare → WAL append (fsync) → in-memory
  commit → delta construction → publish. Replay is a pure function of the log — it
  re-executes no validators, no handlers, no tool loops — and the replayed delta
  stream is byte-identical to the live one. There is no constructor without
  durability; Sylk's inert-WAL wiring class is unrepresentable (fault ledger #1).
- **The ledger is a replicated state machine over the shared-log substrate.**
  The `prepare → append` half is the session's single-owner **sequencer** — it
  produces the total order. The `commit → delta construction` half is the
  scale-free, order-preserving **apply** (`MATERIALIZER.md`), not a single serial
  task: a session has no assumed scale, so apply parallelizes (within-node, then
  multi-node) while the sequencer's order is preserved exactly. The ledger, the
  queue, and the topic sequencer are **ordered-log instances on the shared
  substrate** (`WAL.md` + `CONSENSUS.md`); the **cache** is a writer-less read
  projection (not a log) and the **fanout** a composed router — not layers stacked
  on each other. The full substrate relationship and interaction mechanics are
  `LEDGER_SUBSTRATE.md`.
- **No outbox; projections are cursor consumers.** Derived consumers (UI projections,
  knowledge mirrors, narration intake) are ordinary cursor consumers of the delta
  stream — durable watermarks, resumable, typed RESYNC below retention — never
  drainers of a durable outbox table (there is no outbox structure: `LEDGER_CORE.md`
  §5 and its AC-3; the log *is* the outbox). At-least-once projection is cursor +
  replay; per-projector terminal-failure surfacing is a stuck-cursor alarm in the
  health plane, cleanly separate from the WAL's truth.
- **Watermarks everywhere.** Every consumer — projection, inbox, monitor, terminal
  client — tracks a durable cursor; recovery replays from it; behind-retention means
  deterministic re-derivation. Live deltas are authoritative for delivery; the WAL is
  authoritative for recovery; delta payloads never substitute for re-derivation.
- Compaction snapshots at sequence boundaries without pausing the ledger.
- **Retirement bounds hot state — as a custody transfer, never a loss.** The ledger
  is monotonic as *proof*, never as *memory*: terminal-and-released objects retire
  from hot state and graph indexes at derived watermarks, moving **complete, by
  content identity, into the Archivalist's archive** — content-addressed over the
  unified store, indexed at retirement time by scope, agent, domain, lineage, and
  time, with the WAL/snapshots as the recovery substrate beneath both tiers. Proof
  of work is never deleted and never summarized-in-place; it cools, fully
  interrogable: years-later evaluation retrieves the actual claims, testaments, and
  artifacts (hash-verifiable against WAL lineage) with the Archivalist's
  relevancy-weighted retrieval. Traversals crossing the retention boundary return a
  typed archival continuation — marked results, different latency class, never a
  silent absence. Explicitly not tombstonic: no delete markers exist in any read
  path, retirement is an idempotent fast-forward custody transfer with no
  timing-coupled correctness, hot memory is bounded by live work, and archive growth
  is bounded only by storage — the correct bound for proof.
- **Replication is degenerate locally, real remotely — same code path.** The WAL
  commit path is a consensus group from day one: locally a single-replica group where
  every append is the leader voting for itself; distributed, the same group at three
  replicas per session namespace. Distribution changes the replica count, never the
  commit code. This is what makes local-same-as-distributed true for the truth plane
  rather than aspirational.

## 9. Inherited invariants

The Sylk claims invariants Hecate adopts as law, restated in Hecate terms:

1. **Durable-before-transient.** Nothing publishes before the WAL holds it.
2. **Exactly-once per mutation** at every seam: `(delta_key, sequence)` plus content
   identity; re-emissions, reconnects, and recoveries are idempotent.
3. **Multi-step operations fast-forward.** Transfers, drains, teardown-after-commit,
   handoff: idempotent self-checking steps that verify their own completion, so
   redelivery and crash-resume skip instead of re-executing.
4. **Signals carry reasons.** Lifecycle notifications are typed values
   (`drained | revoked | expired | superseded`) — never bare channel closes.
5. **Guards are continuous.** A claim's preconditions hold for the life of the work;
   terminality, subject drain, or mid-flight revocation tears down dependent parked
   turns with the reason at the source — never discovered later by a lagging check.
6. **Errors are artifacts.** Denials, failures, timeouts, and outages surface as error
   artifacts on the in-flight testament; the issuer decides remediation.
7. **Progress and context text never complete work.** Only lifecycle deltas do; the UI
   renders completion from nothing else.
8. **No second authority.** No synchronous route response, no parallel store, no
   status surface, no hybrid-truth migration state. If an outcome is observable, it is
   observable as ledger state.
9. **No enforcement layer atop claims.** An agent acting out of order means the claims
   were not specific enough or not visible enough — fix the claims or the visibility.
   Enforcement bolted on top re-implements the state machine claims replace.
   **The standing/work-order split (amendment 2026-08-18, `IAM.md` §0/§7.2).** The IAM
   plane decides **standing** — may this principal issue/receive/read this *kind* of
   claim at all — evaluated at the affordance guard from a core-local compiled artifact
   (delivered as an ordinary WAL-logged input, the sub-decision-(c) score-snapshot
   shape; zero cross-plane RTT, replay-identical). Claims keep **work-order and scope**
   authority — which specific work is legal now, against which target — unchanged. This
   is not an enforcement layer atop claims: standing is orthogonal to the claim state
   machine (it gates *whether an actor participates*, never *what the work sequence
   permits*), no IAM object is a claim, and the claim scope entries remain the sole
   scope authorization. **Read visibility** (the per-kind/per-office see-matrix,
   `IAM.md` §7b) is enforced at the **ledger serving edge** — a per-pod, host-side PEP
   where the warden already sits — never in the core: the core stays identity-blind and
   emits the whole byte-identical delta stream, and the serving edge filters per
   recipient via delta-subscription classes and traverse filtering compiled from the
   residual. The default within-session posture stays broad (the coordination substrate
   is preserved); read-restriction is an optional tightening for sensitive claim
   classes. Deltas remain self-sufficient and single-streamed; the edge decides
   delivery, it does not redact a delta's bytes. **The serving edge also offers
   the `skeleton` read (amended 2026-08-22, COLLECTOR acceptance): a claim's
   lifecycle metadata only — status timeline, timestamps, `trace_refs` — under
   the caller's ledger capability, metadata-side by the split above, zero
   content. Terminal-and-released is observable in the delta vocabulary (release
   fires a delta), which the collector's capture consumes (`COLLECTOR.md` §10).**
10. **Advisors never author.** Policy and advisory systems contribute via provenance;
    the issuer owns every validation set.
11. **Expected tool calls are instructions, never authority bypasses.**
12. **No self-targeted work** absent a legitimate self-transfer.
13. **Deterministic replay.** Same WAL, same state, same delta sequence;
    non-deterministic outputs are stored as result artifacts and never re-executed.
14. **Tracked tasks, bounded queues, counted drops, deterministic shutdown** — in
    Rust, ownership and scoped tasks make this structural rather than disciplinary.
15. **Cancellation propagates** root-to-leaf; every cancelled claim reaches an
    explicit terminal state before its scope releases; shutdown drains in fixed order
    with per-scope deadlines.
