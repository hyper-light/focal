# Hecate

Hecate is a multi-agent coding harness written in Rust. It hosts an open roster of agents — a shipped ten-agent default distribution, extensible through Guardian-staged registry entries — as microVM-isolated, network-addressable workloads, coordinates their work through a durable claims ledger, and inherits its architecture (not its code) from Sylk.

## Language

### Claims

**Ledger**:
The durable proof of work: work required (claims), work claimed (receipt), and work completed or failed (testaments — judged by validations, evidenced by artifacts). Nothing else. Configuration belongs in config; operational events in logs; working state in its owning service.
_Avoid_: board, journal, queue, database

**Claim**:
A precise, atomic assertion or item of directed work issued by one participant against another, carrying the validations that define its satisfaction.
_Avoid_: task, message, request

**Testament**:
The uniform, immutable response to a claim — the closing commit after its artifacts have streamed in. Corrections supersede or amend; they never mutate.
_Avoid_: response, result, reply

**Validation**:
A single atomic means of verifying a claim, paired with a quality bar — an instruction to the evaluator, not a status flag.

**Artifact**:
A piece of typed evidence attached to a testament. Errors are artifacts: failures are reported as evidence, not thrown.

**Delta**:
An immutable fact emitted after a ledger mutation commits — authoritative and self-sufficient, never a hint or a UI decoration.
_Avoid_: event, notification

**Participant**:
Any entity that can issue, receive, or evaluate claims — agent, deterministic service, system runtime, or external actor (user, CI). Wire format and lifecycle never branch on the category.

**Affordance**:
Whether a tool call is legal, derived from graph state: the target's lifecycle precondition and the work node's dependency satisfaction — and, since the authority plane, the principal's standing to act at all (whether policy permits the action). The response to an unmet affordance is inform or yield — refusal is reserved for structural invariants (a policy denial informs or yields; it does not refuse).

### Runtime

**Lineage**:
The fork tree over a body of work — baseline manifests, fork relations, and its materialization target (a local tree or a source-control ref). Sessions attach to lineage nodes; the lineage outlives its sessions; the materialization lease is lineage-scoped.
_Avoid_: workspace, repo binding

**Session**:
The isolation and namespace unit: an independent workstream attached to a lineage node, born template-stamped (never bare) with its own colocation unit, key root, and pods — nothing inside a session is reachable from another absent a brokered grant. **A session spans machines**: its pods place on any nodes; only its home services colocate; its volumes attach from anywhere. Forkable, mergeable via landing, disposable; proof outlives it in the archive.
_Avoid_: workspace, environment

**Colocation unit**:
The session's home services — ledger core, merge service proposer, frontier, field service — placed together on one node for locality, with their logs replicated across the session group. The unit is the services, never the session: pods are not in it and place anywhere.

**Attachment**:
The per-(pod, volume) control object created at bind: pins the version, holds the lease, wires warden scopes, runs prefetch, carries accounting. A pod's view of a volume changes only through a re-attach. No claim, no attachment, no mount.

**Summon**:
A claim requesting allocation of a workload — pods, VFS volumes, permissions, network endpoints, agent assignment, health validation. Issued by an orchestrating agent (the Guide, the autoscaler as system participant), **executed by the scheduler**, gated by Guardian admission validations on the same claim, and monitored and evaluated by its issuer like any work. No agent allocates directly.
_Avoid_: spawn, activate, mint

**Pod**:
The unit of agent placement — a microVM running one primary agent and its Scribe companion, one OCI container per agent loop, with its volumes and network identity. The VM boundary is the isolation guarantee toward the world; the container boundary compartmentalizes the two loops within.
_Avoid_: process, goroutine, "one agent per pod"

**History channel**:
The one inter-loop channel in a pod: the supervisor-minted, one-way, kernel-direction-enforced flight-recorder ring the primary's runtime writes and the Scribe reads. Enrichment class — never a detector, score, or audit input.

**Runtime emitter**:
The instrumentation below the model that writes history events at turn boundaries, tool dispatch, and claim posts — zero token cost, not prompt-injectable. The reason "the agent feeds its Scribe" is always wrong.

**hecate-init**:
PID 1 in every guest: mounts, reseeds, mints the history channel, installs the pinned interior residuals, spawns and supervises both containers, and executes flush-gated teardown. Mechanism, never authority.

**Detection substrate**:
The per-session deterministic stage that runs the detector statistics over authority streams and emits enriched incidents. It detects; the Scribe judges; the Guardian adjudicates.

**Score service**:
The per-session single-writer service computing outcome-grounded reputation as a pure fold over its logged input stream; its snapshots enter the ledger as ordinary logged inputs.

**Provenance class**:
Every signal's trust label — host-observed or guest-reported. Authority decisions read host-observed only; divergence between the two is itself a signal.

**Soft gate**:
A Guardian check that may deny within bounded, declared rules or request more evidence a bounded number of times, but cannot block indefinitely. Summons and performance-driven handoffs are soft-gated.

**Warden**:
The Guardian's per-pod enforcement daemon — host-side and deterministic — deciding every boundary crossing pre-effect from compiled local policy (the SafetyPolicy ceiling, the role profile as the authority plane's per-pod residual, bundle capability atoms, active claim scopes). What policy cannot answer is held and escalated; verdicts compile back with provenance. Fail-closed: a dead warden is a frozen pod.

**Trace**:
The execution story of one operation across subsystems — the tree of spans sharing one trace id, from the root that minted it through every chokepoint it crossed. Operational signal, sampled-for-keep; distinct from the claims graph (work proof) and from request/response pairing (transport).
_Avoid_: conflating with `caused_by` or the claims graph

**Span**:
One chokepoint's timed slice of a trace — its ids, a chokepoint name from a closed registry, timing, a typed status, bounded tags. Content-free; emitted async, never blocking the operation it measures.

**Trace context**:
The tag every wire message carries — trace id, the sender's span id, and the keep flag decided at the trace root. Rides the encrypted envelope; a message minted outside any traced operation roots a fresh trace.

**Sensor**:
The guest-kernel telemetry probe inside each pod, streaming early behavioral signals to the warden. Tighten-only: its signals can narrow a pod's world, never widen it. Silence fails closed.

**Hard block**:
The Guardian's absolute stop authority — over tool and skill invocations, external network calls, excessive pod/resource/VFS allocation, and errant syscalls from within pods.

**Handoff**:
The replacement of an agent instance triggered by context exhaustion or performance degradation. Nothing else is a handoff.
_Avoid_: failover, model swap

**Context handoff**:
A handoff triggered by hitting the context threshold. Scribe-initiated, unilateral, not subject to approval.

**Performance handoff**:
A handoff triggered by detected, confirmed degradation. The Scribe requests it; the Guardian adjudicates — approving, or denying when the fresh-context probe shows the task itself is hard (the corrective then targets the claims, not the agent).

**Failover**:
Switching an agent to its alternate model when its primary provider fails. An operational event driven by config — logged, not a handoff, not ledger state.

**Lease**:
An optimistic, expiring write-basis snapshot used to guide work and skip merge effort where changes are provably disjoint. A work-reduction aid — never a substitute for real conflict detection at the merge.

**Failure-domain tree**:
The physical containment hierarchy — node, availability zone, region — against which placement, quorum spread, and authority scoping are expressed. A laptop is a depth-one tree; every topology collapse is derived from the tree, never switched by a mode.

**Epoch scope**:
The failure domain in which a fencing/epoch authority lives: the smallest domain containing every legal holder of the fenced resource, so the resource and its authority always die together.

**Merge gate**:
The single path by which completed work reaches disk. Work streams as increments: each increment's validations pass and it merges into green immediately at machine speed; the claim's whole-work validations — owned by the Arbiter, joined by the Architect where design judgment is needed — gate the disk commit (auto- or user-approved). Failures fix forward via superseding increments — green extends, it is never written in place. There is no post-merge audit loop.
_Green_ = increment-validated work; _disk_ = claim-satisfied work.

**Landing**:
The cross-session act of adopting results — into a lineage head, or materializing to a real target. Landing into a head always succeeds, carrying any overlaps as conflict values; materialization requires zero unresolved conflict values plus its review gate. A distinct machine from the intra-session merge gate, never a reuse of it.
_Avoid_: merge (reserved for the gate), sync, rebase

**Conflict value**:
A first-class datum representing an unresolved overlap between landed changes — a term list that propagates through subsequent landings, collapses on identical edits, and is resolved by a change like any other. Never markers-in-files as the source of truth; never auto-resolved by any machinery.

**Witness**:
The serving boundary's durability contract for guest writes: the write is journaled and group-committed before the reply — acked means durable. No unwitnessed byte can exist in a work volume; unacked bytes are work-bearing memory and die with the pod.

**Seal**:
The freeze of a work volume's journal epoch at increment submission: guest writeback drained, per-file edit ops derived, content chunked into the content store, manifest updated. What seals is exactly what the agent fsync'd; sealed content is immutable and cache-coherent by construction.

### Skills

**Office**:
A structural role contract the harness defines — merge-gate evaluator, protection judge, summoner, sidecar narrator — whose consequences are machinery, not personality. The registry binds agents to offices; enforcement attaches to the office, never to its holder.

**Registry**:
The slow plane and the extension surface: the declarative catalog of everything summonable and installable — agents (shipped and custom), skills, tools, MCP servers, provisionable libraries — versioned, content-addressed, Guardian-staged on entry, and resolved into pinned bundles at summon time. Shipped and custom entries differ by publisher and provenance, never by mechanism. Never a runtime authority: it cannot route, gate a transition, or author claims.

**Skill**:
A typed, code-defined capability authored against the harness API and published over MCP as tools plus a `skill://` instructional resource. Never raw markdown at the source. Built-ins are compiled Rust; user skills may be declared via TypeScript/Python bindings, loaded only after Guardian validation.

### Authority

**Rank**:
An agent's per-domain authority ordering, shipped as a static matrix with the harness — not user-editable. A lower-ranked agent may challenge a higher-ranked agent for clarification but cannot override, invalidate, or ignore its feedback. Effective authority is modulated by the agent's tracked performance: a poorly-performing higher rank cannot force compliance.
_Avoid_: hierarchy, seniority

**SafetyPolicy**:
The single user-owned configuration object driving every Guardian gate decision: trust mode, auto-approve ceiling, disk-write mode, network egress. Enabling low-trust drastically lowers the auto-approve ceiling. A config item, not ledger state. It compiles into a **Ceiling** — the user's — in the authority plane.

**Authority plane**:
The single control plane for roles, policies, and permissions across every system: it answers whether a principal may perform an action on a resource in a scope, given context, at an authority epoch. Its own replicated store, own APIs, own audit — never the ledger, which drives work. Rank, SafetyPolicy, Guardian gates, Biscuit grants, and claim affordances are its consumers, not its parts.

**Principal**:
Any participant viewed by the authority plane — an immutable UID plus a kind (agent pod, system service, user, external, node). The kind is a read field on the record; wire format and lifecycle never branch on it.

**Ceiling**:
A policy that only caps, never grants. SafetyPolicy, the Guardian hard-block classes, and a mandate's scope-down are ceilings; a decision must pass every applicable ceiling (they intersect).
_Avoid_: boundary (reserved for the pod perimeter)

**Mandate**:
An assumed role — a derived principal carrying its base, role, scope, a scope-down ceiling, expiry, an immutable provenance origin, and tags. Effective authority is the intersection of base, role, and scope-down; chaining only narrows.

**Grant**:
A narrow, evaluated, epoch-stamped, durable authorization decision made durable — the object a brokered cross-fence share or a materialized secret produces. The authoritative row lives in the authority plane; the Biscuit token is its portable projection.

### Agents

**Guide**:
The user's primary conversational agent. Summons and orchestrates all other work and issues work claims; there is no separate orchestrator.

**Arbiter**:
The merge-gate authority and the running sanity check on the cumulative big picture: a continual daemon agent that analyzes work as it merges — quality, coherence, adherence to user directives, robustness, efficiency, performance, correctness — owns whole-work validation at the gate, adjudicates merge conflicts above the author-rebase fast path, and issues unified feedback. Consults the Guide and Architect for intent; may challenge anyone, the user included, with evidence. Writes no code.

**Architect**:
The principal design specialist. Sets direction, produces plan artifacts on demand, incorporates external research, authors corrective actions, and joins merge-gate validation where needed.

**Guardian**:
The system-protection agent. Soft-gates summons (admission control), gates skill/tool invocations with risk-based auto-approval per the SafetyPolicy, hard-blocks threats, and watches resource and VFS health.

**Inspector**:
The adversarial reviewer. Challenges any agent except the Guardian; recommends and pushes back; writes to no system.

**Engineer**:
The sole implementer of code and tests. Submits testaments with artifacts — including the Inspector's approval artifacts where required — against the claims it received; does not merge.

**Archivalist**:
The ground-truth agent: record keeper, code knowledge, and deep investigation (debuggers, profiling, tracing, log ingest). Read-only toward code. Absorbs Sylk's Librarian.

**Scribe**:
The companion agent attached to every primary: narrates its primary's actions, records and serves its history, and judges handoffs — initiating context handoffs unilaterally, requesting performance handoffs for the Guardian to adjudicate. It observes; it never feeds its primary, and its primary cannot feed it.
_Avoid_: sidecar, "monitors performance" (the detection substrate detects; the Scribe judges)

**Sibyl**:
The user's workstream agent, above sessions: judges which sessions and experiments should exist — forking variants, stopping generation, arbitrating materialization, brokering cross-fence grants — and touches no work inside any session. Content-blind by default; instances partition judgment by lineage; all authority exercised as user-plane claims.
_Avoid_: steward, orchestrator, session manager

**Designer**:
The on-demand multimodal agent: ingests and produces non-code media, mocks, and wireframes into isolated VFS volumes, outside the merge machinery. Produces A/B variants of non-code outputs for the user to judge. Does not implement code.
