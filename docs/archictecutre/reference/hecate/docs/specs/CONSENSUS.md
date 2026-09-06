# SPEC: consensus — the commit authority

Status: ACCEPTED 2026-08-17 (direction ratified after the etcd challenge was
answered by the layer-classification dossier; spec accepted with three delta
sets worked in-exchange: split-brain assembly + fabric law, actor-architecture
position, cross-region). Amendments carried: (2a) CRDB-lineage exemplar
naming; (2b) bug-record-as-conformance-suite; (3-addendum) conf-change
activation semantics + #12359 countermeasures; (5a) deterministic
whole-cluster simulation (in `FAULTS.md`). Consumed by: WAL §5 (the sole
commit path at every replica count), SERVING §6 inventory map, OBJECT_TIER §4
placement map, SCHEDULER §1 meta group, REGISTRY §5 replicated revision,
LEDGER_CORE §2 append API. Resolves most of Branch 27 by construction (§6).
Companion: `FAULTS.md` (fault scope, dispositions, nemesis matrix, the
simulation gate). Amended 2026-08-20 (CACHE/QUEUE/FANOUT acceptance): §6
roster gains queue-partition + topic-sequencer (lease+fence),
topic-registry (CAS-first), cache/pub-sub (writer-less registration);
corrected the stale merge-serializer entry to leader-fused (term-only
fence) per MERGE §2.

## 1. Topology: the meta tree + N per-session groups

**The failure-domain tree is first-class**: node < AZ < region, discovered at
boot and versioned in the inventory map. Placement, quorum spread, and meta
authority are all expressed against this tree. A laptop is a depth-one tree
with a single leaf — every collapse below is tree-derived, never a mode.

- **The meta tree — one meta group per level of the tree that exists.**
  The **root group** (spans regions; low-rate, latency-tolerant) owns the
  region directory, cross-region placement policy, root-scoped refs (lineage
  heads, registry publications), and root-scoped epochs. **Per-region
  groups** own the region's **shard directory** (`key-range → data-shard →
  host group + epoch`), region-scoped epochs/leases, and the region's durable-
  plane copyset map. The region's cluster-state **data** — host-inventory, the
  session-group directory, placement, and leases-as-records — **range-shards
  across many per-region data groups** (each a Raft group + a `STORE.md` engine
  instance); the meta group owns the directory over them, not the data itself. Same core, same code at every level; on a laptop
  root ≡ region ≡ one group. Every fencing token and writer epoch in the
  system is minted by exactly one meta-tree group per §6's scoping law.
  Group membership, configuration, and reconfiguration live as ordinary
  committed records in the owning directory (the Delos VirtualLog shape:
  reconfiguration is data in the control plane, never a side protocol).
- **Session groups** (one per session) own that session's WAL commit — the
  per-session logical logs of WAL §5. Independent commit order, independent
  failure domain; a wedged session group harms exactly one session.
- **The colocation law extends to consensus**: a session's consensus group is
  part of its colocation unit — replicas placed within the session's region,
  spread across its interior failure domains (AZ/node). **No synchronous WAN
  round-trip exists on any session hot path** — WAL commit, serving, merge
  gate, warden verdicts (Physalia's placement principle as law: consensus
  colocated with the failure domain of the resource it governs).
- **Liveness is amortized to node level — no per-group heartbeats exist,
  ever.** Nodes participate in one **node-liveness fabric** per region: each
  node claims support once per derived interval with a claim that is
  **disk-write-backed** (a node with a stalled disk cannot claim support —
  CRDB's gray-failure detection, adopted as law). A session-group leader's
  authority is fortified by its node's current support epoch: it cannot lose
  leadership while support holds and loses it deterministically when support
  lapses. Idle groups tick nothing, send nothing, cost nothing — designed
  in, not retrofitted (TiKV #10017 is the named retrofit regression; CS11).
- Group placement/rebalancing is a scheduler concern executed via meta-tree
  reconfiguration records; this spec owns only the record semantics.

## 2. The core: Raft, CRDB-lineage dialect, pure

- **One pure algorithmic core**, used by every group at every tree level
  identically. Exemplar = the etcd-raft interface shape as extended by
  cockroachdb/raft (amendment 2a); tikv/raft-rs is the Rust-portability
  proof. **Exemplars, never dependencies** — the core is ours, in our
  runtime, under our lint wall, with zero etcd/CRDB/TiKV code.
- **IO-as-data, deterministic**: the core is a state machine
  `step(Message) → {outbound: Vec<Message>, to_append: Vec<Entry>,
  state_delta}`. Same state + same input ⇒ same output, always. Time enters
  only as logical `Tick` messages; election randomization is seeded and
  injected. No clock, no channel, no IO inside the core — this is what makes
  `FAULTS.md` §4's whole-cluster simulation cheap; the two are one design.
- **AsyncStorageWrites shape from day one**: storage and network effects
  leave the core as request messages and return as response messages (the
  interface CockroachDB authored for exactly our scale; 28% latency receipt
  on file). There is no synchronous Ready/Advance path to deprecate later.
- **Two persistence laws, spec-level, conformance-gated** (the v3.5 class
  made unrepresentable):
  1. **Entries-then-HardState**: log entries are durable before the
     HardState (term/vote/commit) that references them.
  2. **No message emission before covering durable state**: no vote, no
     append-response, no commit advance leaves the node before the state
     that justifies it is durable. The host's applied/durability watermark
     advances **inside the same transaction** as the applied effects — never
     a shared mutable index a background commit can race (the exact v3.5
     mechanism, named).
- **The actor position, owned**: this core *is* the actor model in its
  disciplined form — `step(Message)` actors with single-owner state and
  message passing only, as are the runtime's sharded single-owner tasks.
  Five Akka-style laxities are refused, each by standing doctrine: unbounded
  mailboxes (bounded queues + credit are law), untyped messages
  (hecate-wire is the only wire), supervision-as-blind-restart (panics are
  evidence testaments; restarts are typed lifecycle), location transparency
  that hides failure (failure is typed and loud), fire-and-forget delivery
  as the default contract (delivery semantics are per-plane and witnessed
  where it matters). Akka cluster-sharding's role — huge entity populations
  with liveness amortized above the entity — is played by session groups
  over node liveness, in consensus-grade form.

## 3. Elections, liveness, and reads

- **PreVote AND CheckQuorum together, always** — either alone has a proven
  liveness hole (Decentralized Thoughts omission analysis; the Cloudflare
  6-hour outage as the production case). Both are on unconditionally; there
  is no configuration to get wrong.
- **Leadership transfer bypasses PreVote** — the documented carve-out,
  adopted explicitly (the transfer target must win immediately despite a
  healthy leader); CS6 asserts transfers complete under active CheckQuorum.
- **Fortified leadership** (§1): a leader supported by the node-liveness
  fabric ignores election timeouts from partitioned minorities; every
  step-down path (support lapse, explicit transfer, joint-config exit)
  de-fortifies explicitly (CRDB #129098's bug class; CS12).
- **Reads: ReadIndex only, v1.** A read is serviced at or above a commit
  index confirmed by the current leader's quorum round. Learners are
  first-class ReadIndex clients (CS4). **Lease reads do not exist**: they
  import a wall-clock axiom the doctrine bans; if fortification later makes
  local reads provable from the support fabric alone, that arrives as its
  own spec amendment with its own proof obligations — never as a flag.
  - **IAM-store reads under this law (note, 2026-08-18, `IAM.md` §3).** The
    authority plane reads honor ReadIndex like any client — and get
    region-local latency without a new read mode because each scope's records
    live on the group owning its epoch scope (§6): for session/user/org scopes
    that group is **region-local**, so the ReadIndex quorum round is
    region-local (no cross-region RTT), satisfying "no synchronous WAN
    round-trip on any session hot path" (§1). Root/lineage-scope reads (holders
    anywhere ⇒ root group) would incur a cross-region round, so the plane keeps
    those off the decision path by construction — enforcement reads a compiled
    residual artifact, not the store. A genuine local-follower optimization for
    the store remains a future amendment here, never a flag.

## 3b. Split brain, structurally

Split brain is four distinct attacks; each dies at a named layer:

| Attack | Layer that kills it | Test |
|---|---|---|
| Two leaders committing in one group | Quorum intersection — coexisting leaders are in different terms; the older term cannot commit (its quorum overlaps the new one). Raft's core theorem; needs no liveness help | CN1 |
| Zombie leader serving stale reads | ReadIndex-only (§3) — every read needs a quorum confirmation a deposed leader cannot get. This is why lease reads were rejected | CN10 |
| Paused-and-resumed standing writer | Epoch fencing enforced **at the resource** (§6) — the split brain no consensus algorithm sees, closed where it happens (Kleppmann's argument; ZippyDB's shipped shape) | CN8 |
| Actors on stale topology | Versioned, fenced, epoch-consumed maps (SERVING §6, OBJECT_TIER §4) — stale-epoch writes refused by the same token discipline | CN12 |

**The fabric law**: **the node-liveness fabric is liveness-only; no safety
property depends on it.** Fortification defers elections and amortizes
heartbeats — it never authorizes a write, a read, or a commit without the
underlying quorum. Worst-case arbitrary fabric misbehavior (contradictory,
stale, withheld, or forged-within-scope support claims) degrades election
latency and nothing else (CN11). This is deliberately stronger than CRDB's
Leader Leases, which put the fabric on the read-safety path to earn local
reads; having declined lease reads, we keep the fabric off the safety path
entirely — the named spot where a future "optimization" would quietly
reintroduce the hole.

**Rejected-alternative record**: heuristic split-brain resolvers exist only
where membership lacks a quorum authority. Akka Cluster's AP-gossip
membership required deprecating `auto-down` after production split brains and
shipping the SBR heuristic menu (keep-majority/static-quorum/keep-oldest/
down-all, each with documented wrong-decision scenarios) — whose most robust
strategy, lease-majority, acquires a lease from the Kubernetes API, i.e.
borrows etcd; Elasticsearch's Zen `minimum_master_nodes` split brains
(Jepsen-documented loss) were fixed only by the 7.x Raft-style rewrite. With
Raft-owned membership the quorum *is* the resolver — deterministic, not
heuristic — and the both-sides-running window SBR concedes is exactly what
layer-3 fencing closes. This design never enters the regime where a resolver
is needed.

## 4. Reconfiguration

- **Joint consensus for all voter-set changes.** Single-server fast paths
  are permitted only with **Ongaro's guard as law**: a leader may not
  propose any configuration change until it has committed an entry (the
  term-opening no-op) in its own term (CS7).
- **Learners with promotion gating**: new members join as learners;
  promotion requires a derived catch-up threshold (lag below
  `k × append_batch`, derivation at the definition site); **voters are never
  demoted directly — demotion routes through learner** (half of the #12359
  countermeasure, API-level unrepresentable).
- **Activation semantics: apply-time, with the countermeasures as law**:
  the dialect's apply-time semantics keeps one apply path — configuration
  changes flow through the same committed-then-applied machinery as all
  state, preserving the pure-core interface; append-time would diverge from
  both exemplar forks and forfeit their fixes' conformance value. The
  #12359 hole is closed structurally: **vote and pre-vote messages carry
  conf-commit metadata** (index/term of the sender's latest committed
  configuration), making promoted-but-unaware voters and
  demoted-but-unaware quorums detectable (CS1).
- **Reconfiguration is data** (§1): every membership change is a committed
  record in the owning group and mirrored as a directory record in its
  meta-tree parent; recovery and observers replay records — no out-of-band
  membership truth exists.

## 5. Log storage boundary

- The core sees an abstract log; the host owns storage using WAL.md's
  measured durability primitives over a **dedicated log store**
  (raft-engine's lesson: consensus logs are append-heavy, prefix-truncated,
  group-multiplexed — not general KV). No mmap-whole-DB, no stop-the-world
  maintenance (the boltdb anti-patterns, named).
- **Truncation and snapshots are host-owned** with slow-vs-dead follower
  discrimination (CRDB tech note): a slow follower holds truncation within
  a derived debt bound; a dead one (node liveness says so) releases it.
  Snapshot transfer rides the transfer plane (`TRANSFER.md`) — chunked,
  verified, resumable; never a bespoke path.
- Applied-state checkpoints and log prefixes retire together under the
  checkpoint-retention invariant (no prefix drops while any recovery path
  needs it).

## 6. Single-writer subsystems (Branch 27, resolved by construction)

- **CAS-first**: where the commit is one ref swap and writers are
  intermittent — registry refs, generation pointers, landing heads,
  placement-map versions, the topic registry — there is **no election and
  no lease**:
  election-free CAS on the consensus-backed ref (`set_ref_if`), with the
  Lance-style outcome classification (rebasable / retryable / conflict) as
  the typed result.
- **Standing writers** — the ledger sequencer, queue-partition sequencers,
  and the topic-router per-group FIFO sequencer (epoch scope = the smallest
  failure domain containing every legal writer), plus any future
  open-write-stream holder — hold a **meta-tree lease in Chubby's
  coarse-grained shape** (keepalives, grace period) **plus an epoch fencing
  token enforced at the resource** — non-negotiable, because a
  paused-and-resumed writer defeats any lease alone (§3b layer 3). Every
  write the resource accepts checks the token; a stale token is a typed
  refusal. **The merge proposer is the exception: it is leader-fused** — a
  role of its session group's Raft leader, with the term as its only fence
  (MERGE §2, M13d), so it holds no separate lease or epoch token. **Each
  `STORE.md` data-shard writer is leader-fused likewise** — the data shard's own
  Raft leader owns apply and ingest, the term as its only fence, no separate
  lease; the shard directory that routes to it is a placement-map version
  (CAS-first, above), so it introduces no new epoch class.
- **The epoch-scoping law**: an epoch/fencing authority lives in the
  **smallest failure domain that contains every legal holder of the fenced
  resource**. A ledger sequencer's holders all live in the session's region
  ⇒ region-group epoch — if the region dies, resource and authority die
  together, so no resurrection is possible. Lineage heads and registry
  publications have holders anywhere ⇒ root-group epochs, WAN-committed —
  acceptable because those are human-cadence CAS operations.
- The score service (MONITORING §8) classifies as an **ordinary claims-plane
  client** — it owns no fenced resource and needs no roster entry; the roster
  covers resource-owning writers (amended 2026-08-22, MONITORING acceptance).
- Boot validates the roster: every subsystem is classified CAS-first or
  lease+fence with its epoch scope — or, where it introduces no durable
  writer at all (the cache / pub-sub plane), registers positively as **"no
  durable writer"** (chokepoint-covered-by-absence): a boot-recognized cell,
  not a new classification and not the "unclassified" state that fails
  startup. An unclassified writer still fails startup (chokepoint law); boot
  recognizes the writer-less registration. Branch 27's remaining scope =
  auditing this roster against the actual subsystem list at build time.

## 7. Cross-region

**Evidence provenance**: verified on primary text 2026-08-17 (dossier in
GRILLING.md research index; §7 re-settled with the six-amendment set,
accepted). Physalia's placement principle (P(Av|Ai), same-side-of-partition
— an intra-AZ system; the region-level application is our extrapolation of
the same argument, stated as such); Chubby's per-DC cells + one global cell
(mirrored ACLs/refs/directory pointers; <1ms local vs 250ms antipodes) as
the meta-tree precedent verbatim; Spanner's leader placement, witness
replicas (OSDI'12 §2.2), and minutes-cadence placement driver; the async
pole (S3 CRR, f4 §5.2's XOR-across-regions of sealed immutable volumes,
Tectonic's datacenter scope); the non-failover norm (Borg "a job runs in
just one cell"; K8s replacement-never-migration; F1's leader re-placement).
Clark's fate-sharing, Chubby's lock-delay, and K8s cluster-scoped Leases
ground the epoch-scoping and lease-shadow laws. Dynamo's surfaced-siblings
model is the named precedent for landing-with-conflict-values; DynamoDB
global tables' last-writer-wins is the named anti-pattern (silent loss of
one side).

- **Session groups never span regions** (§1 colocation law). Intra-session
  durability is region-interior quorum; cross-region durability is the
  durable plane's job.
- **Cross-region durability is asynchronous and content-only**: sealed
  immutable content replicates cross-region per root placement policy
  (copysets against the failure-domain tree) — no consensus per chunk,
  verification intrinsic to content addressing (the S3-CRR/f4 pole, correct
  precisely because our cross-region objects are immutable; S3 RTC's
  15-minute SLA is the industry quantification of the exposure-window term
  in the loss formula below). **Honesty note**: async is the dominant
  default, not the only practice — DynamoDB's opt-in MRSC mode and
  Spanner/CRDB multi-region quorums are the deliberate synchronous-WAN
  pole, rejected here for the stated colocation-law reasons, not ignored.
- **Sessions do not fail over across regions.** A region loss kills its
  sessions; sealed/landed work survives in the durable plane; sessions
  re-summon from lineage + archive elsewhere. Unlanded work in the lost
  region is the existing bounded loss class — OBJECT_TIER §3's formula
  verbatim with `correlated_event_rate` = region-loss rate and
  `exposure_window` = seal→cross-region-replication lag — re-derivable by
  agent effort, user-visible, never existential. Live cross-region failover
  is rejected: it would put WAN in the hot path to defend against an event
  the loss formula already prices.
- **FlexiRaft's dynamic (single-region-commit) quorum mode is rejected for
  the meta plane** — on the corrected two-branch argument (the original
  "could lose the latest epoch mints" claim was wrong as a protocol
  statement: FlexiRaft's enforced commit/election quorum intersection makes
  region failure *unavailability*, not loss). Branch 1: an epoch authority
  whose liveness dies with the fenced region is unavailable exactly when it
  is most needed — during that region's failure, when the rest of the
  system must mint replacement epochs (note the deliberate inversion: for
  *session* groups this fate-sharing is exactly what we want; for the meta
  plane it is disqualifying — that distinction IS the epoch-scoping law).
  Branch 2: permanent region destruction leaves no legal quorum ever;
  restoring availability requires operator-forced reconfiguration that
  abandons the committed tail — *there* monotonicity breaks, as an
  operational consequence. FlexiRaft's *static* multi-region mode is not
  rejected — it is the same species as this spec's own root group (a WAN
  quorum for low-rate authority state). The epoch-scoping law makes
  flexible quorums unnecessary regardless: state only region-local actors
  touch is region-scoped to begin with; genuinely global state commits on
  a global quorum at human cadence. (Recorded as compatible future work
  only if a latency-sensitive, genuinely-global write class ever appears.)
- **The lease-shadow law**: the root may not re-grant a lineage/
  materialization lease — nor re-summon a replacement session with
  materialization authority — until the prior lease's remaining validity
  has expired **plus a clock-drift margin derived from the stated maximum
  clock-rate divergence** (constants-from-data; Chubby's lock-delay is the
  precedent, and Chubby's own caveat — leases tolerate skew and pauses but
  not long-term rate divergence — is why the margin is rate-derived, never
  hand-picked). Inside that window, a partitioned-but-alive holder may
  legally act on its lease; safety there is by waiting, and the wait is
  law (CN15).
- **The externalization-fencing law**: fencing tokens protect only effects
  that pass a token-checking chokepoint — so **every external side-effect
  channel is a landing-class chokepoint carrying the root-scoped epoch**:
  source-control pushes, external API calls, messaging, any egress with
  effects beyond the archive. These already route through the
  Guardian/warden egress chokepoints; this law adds the epoch check to
  that boundary. Without it, the §7 safety argument covers archive state
  only — a zombie region's un-fenced externalizations cannot be
  retroactively conflict-valued (CN16, architecture test).
- **The region rejoin protocol** (fate-sharing covers death, not
  resurrection — Clark's model licenses losing state when the entity is
  lost; a partitioned region did not die): dead-declaration is a
  root-quorum decision, taken only after the lease-shadow window, and is
  **terminal for the region epoch**. On heal, the region rejoins under a
  **new** region epoch; no pre-partition epoch resumes any
  authority-bearing role or renews any lease; surviving sessions'
  unlanded work enters the archive **as fork branches only, never
  continuations** — overlapping descendants of one lineage node surface
  as ordinary parallel workstreams carrying conflict values (Dynamo's
  surfaced-siblings model; the LWW alternative is the named anti-pattern),
  adjudicated like any parallel work (F7).
- Region partition consequences are enumerated and closed: everything
  region-local is unaffected (that is the point of §§1/6/7); root-group
  operations from a minority region stall (cross-region placement changes,
  root-ref CAS, global publications — all human-cadence). **Nothing on any
  session path may appear in that stall list** — an architecture test
  (CN13), not prose.

## 8. The laptop degenerate

A 1-voter configuration of the same core on a depth-one failure-domain tree —
no mode, no branch: quorum ack degenerates to own durable append (WAL §5
unchanged); root ≡ region ≡ one meta group; the fabric is one node
supporting itself; cross-region machinery is inert because the tree has no
second region. **The N=1 crash-injection gate is specific and permanent**:
the v3.5 corruption showed *no symptoms* in single-replica mode, so the §2
watermark-atomicity law is CI-gated under crash injection at N=1 explicitly —
the configuration where the bug class hides is where we hunt it.

## 9. The conformance suite (amendment 2b — the bug record as executable tests)

Named, permanent, seed-replayable in SIM; each cites its source:

| # | Test | Source |
|---|---|---|
| CS1 | #12359 leaderless scenarios (promoted-unaware voter; demoted-unaware quorum) fail without conf-commit metadata, pass with it | etcd #12359 (closed stale) |
| CS2 | Isolated node with smaller term rejoins and catches up (no PreVote stuck-state) | etcd #8243 |
| CS3 | Higher-term/lower-index replica never wedges an election | etcd #8501 |
| CS4 | ReadIndex from a learner is serviced | etcd #10589 |
| CS5 | Probe/reject/hint exchanges converge; no leader-follower stall loop | etcd #13418 |
| CS6 | Leadership transfer completes under active PreVote+CheckQuorum (bypass carve-out) | dialect raft.go |
| CS7 | Concurrent single-server changes across a term boundary cannot commit through non-overlapping quorums (Ongaro guard) | raft-dev 2015 |
| CS8 | The omission matrix: partial connectivity, leader-cut-from-majority, per-link asymmetric partitions each reach stable leadership in bounded elections | Decentralized Thoughts; Cloudflare/HAOC'21 |
| CS9 | Entries-then-HardState: crash between the two ⇒ recovery never observes HardState referencing missing entries | §2 law 1 |
| CS10 | Watermark atomicity: crash at every point in apply ⇒ no committed entry ever skipped on recovery, at N=1 and N=3 | v3.5 postmortem |
| CS11 | Idle-group correctness: an idle group reports true peer state through node liveness (no false down-peers) | TiKV #10017 |
| CS12 | De-fortification on every step-down path (support lapse, transfer, joint-config exit) | CRDB #129098/#135078 |

## 10. Test matrix (beyond conformance)

| # | Test | Catches |
|---|---|---|
| CN1 | Linearizability over randomized workloads under the full FAULTS.md nemesis matrix, meta tree + session groups | safety, whole-system |
| CN2 | Idle-cost ratchet: N idle session groups ⇒ measured ~zero ticks/messages/CPU (bounded by the fabric interval alone) | per-group liveness creep |
| CN3 | Stalled-disk node cannot claim support; its groups elect elsewhere within the derived bound | gray failure |
| CN4 | Membership churn fuzz: joint entries/exits, promotions, crashes mid-change ⇒ exactly one committed configuration lineage | reconfiguration divergence |
| CN5 | Determinism: same seed + inputs ⇒ byte-identical core outputs across platforms | SIM validity; the REAL≠SIM class |
| CN6 | Truncation-vs-slow-follower: debt-bounded retention; dead follower releases; recovery snapshot-fills via TRANSFER.md | log-retention deadlock/leak |
| CN7 | CAS-first subsystems under contention: outcome classification exact; no livelock; no hidden election on the path (architecture test) | machinery creep |
| CN8 | Fencing: paused-and-resumed lease holder's writes refused at the resource by token, under maximal pause fuzz | the Kleppmann class |
| CN9 | 1-voter ≡ N-voter observable semantics for every client-visible API (differential test) | laptop-vs-fleet drift |
| CN10 | ReadIndex freshness: a read never returns state older than any write acked before it, under partition fuzz | stale reads (the k8s #59848 class) |
| CN11 | Fabric-fault injection: arbitrary support-claim corruption (contradictory/stale/withheld/forged-within-scope) ⇒ linearizability holds unconditionally; only election latency degrades, bounded and measured | the fabric law (§3b) |
| CN12 | Stale-epoch actor: any actor holding a superseded epoch (map version, lease token, key epoch) has every write refused at every resource; fuzzed across all epoch kinds | topology split brain |
| CN13 | Region partition: every session-local operation unaffected; the root stall list exactly matches §7's closed enumeration (architecture test) | WAN leaking into hot paths |
| CN14 | Region loss: unlanded-work loss ≤ the OBJECT_TIER §3 formula's bound at measured replication lag; sessions re-summon from lineage + archive with sealed work intact | the loss class drifting from its price |
| CN15 | Lease-shadow fuzz: re-grant/re-summon-with-materialization attempted at every instant inside the shadow window (prior validity + derived clock margin) is refused; at every instant after, granted; under injected clock-rate divergence up to the stated maximum | the dual-materialization window |
| CN16 | Externalization fencing: every external side-effect path carries and checks the root-scoped epoch (architecture walk); a stale-epoch egress attempt is refused at the chokepoint under partition fuzz | zombie externalization outrunning fencing |

## 11. Acceptance criteria

1. No per-group heartbeat, tick, or timer exists for idle groups (CN2
   ratchet, permanent).
2. §2's two persistence laws hold under crash injection at every N, N=1
   named (CS9/CS10 CI gates).
3. PreVote+CheckQuorum are unconditional; no flag can disable either.
4. All CS1–CS12 green before the core carries its first real commit; the
   suite is append-only.
5. Voter demotion is unrepresentable except via learner (API-level).
6. Every single-writer subsystem is classified at boot with its epoch scope;
   unclassified fails startup.
7. Wall-clock appears nowhere in the core or its correctness arguments
   (architecture test).
8. The core has zero etcd/CRDB/TiKV code; exemplar provenance is doc-only.
9. **The fabric is deletable**: no safety property, proof argument, or test
   oracle references the node-liveness fabric — removing it from the model
   leaves every safety test passing (architecture-level check; CN11 is the
   dynamic form).
10. No synchronous WAN round-trip on any session hot path (CN13 permanent);
    epoch authorities placed per the §6 scoping law, boot-validated.
10b. No external side-effect path exists without a root-scoped epoch check
    (CN16 architecture walk, permanent); the lease-shadow margin is
    rate-derived with its derivation at the definition site (CN15).
11. Meta-tree depth is derived from the failure-domain tree; no
    region/laptop mode flag exists anywhere (architecture test).

**Amendment (2026-08-22, TRACING/MONITORING acceptance):** this subsystem's
chokepoints emit execution spans per `TRACING.md` §3; its chokepoint registry
entries are the span roster (boot-validated; an unregistered emitter fails
startup).
