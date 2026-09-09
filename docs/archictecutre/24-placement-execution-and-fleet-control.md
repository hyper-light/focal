# Placement execution and fleet control

This document records the R6 design: how a committed placement intent becomes
replicated application data on authenticated hosts, what the directory records
while that happens, and what an operator can read at every step. The directory
model in §2–§5 is implemented (2026-09-09, "Placement progress in the directory
(R6.1)" in [09](09-implementation-status.md)); the controller, agent, collector,
admission, credential and split/merge sections name what the remaining R6 batches
build on top of it and are marked as such. The lifecycle and authority contracts
it relies on are in [04](04-storage-and-distribution.md) §§11–13,
[10](10-ownership-and-failure-policy.md) and the control-plane crates named in
[03](03-rust-workspace-and-interfaces.md).

## 1. Shape of the problem

`focal-directory` already held a deterministic placement planner
(`propose_placement`, `verify_placement`), a per-session transition
(`Plan → BeginPreparation → Ready* → Cutover → Activate | Abort`) fenced by
session-log records, signed node statements (`AuthorityFact`) and an installed
verifier that demands a voter majority for session facts and one self-signature
for readiness. `focal-control` replicates that partition state through its own
Raft group. What was missing on the directory side was any committed record of
*how far* each copy had come, *why* a copy or a plan stopped, *what happens to
the copies an activation leaves behind*, and *what the fleet actually provides*
while a plan is in flight. Without those rows a controller could only keep its
progress in memory, which a leader change would lose, and an operator could only
see "pending".

## 2. Assignment progress

Every node of a desired placement receives one `AssignmentProgress` row when
preparation begins:

| Field | Meaning |
|---|---|
| `node`, `node_generation` | The exact enrolled incarnation the placement names |
| `roles` | The subset of `{Voter, Materializer, ContentCopy}` derived from the desired placement; a reported row whose roles differ is refused |
| `phase` | `Assigned → Installed → CaughtUp → CustodyVerified → Promoted`; `Active`, `Draining`, `Retired` exist only for copies being retired (§4); `Failed` is off the ladder |
| `attempt` | Starts at 1; a refusal ends an attempt, a new attempt restarts the ladder |
| `through` | The session prefix the copy has applied or verified; monotone within an attempt |
| `custody_epoch` | The placement epoch whose content set the copy verified; zero below `CustodyVerified`, otherwise exactly the plan's `next_placement` |
| `refusal` | The code that failed this attempt; present only in `Failed` |

Rules the partition enforces in `SessionChange::Progress`:

- A row names a node of the desired placement at its named generation, carries
  the derived roles and a non-zero attempt; anything else is `StaleEpoch` or
  `Invalid`.
- Within one attempt the ladder position and `through` never move backwards
  (`StaleEpoch`); an exact repeat is accepted without effect, so a lost reply is
  reconciled by resending.
- A lower attempt is stale; a higher attempt is accepted as-is and may restart
  from `Assigned`, which is how the controller answers a retryable refusal.
- `CustodyVerified` and `Promoted` are never asserted by the controller alone:
  the copy's own signed `ReplicaReady` must already be recorded for that node
  (`NotReady` otherwise). Readiness itself raises the row to `CustodyVerified`
  and carries its `through` forward.
- `Promoted` is accepted only for a voter role.
- `Failed` is entered only through `SessionChange::Refuse` and rejects every
  progress report of the same attempt (`Phase`).

## 3. Plan phases, cutover and activation

`PlacementPhase` extends the earlier two states. `Planned` is set by the plan;
after `BeginPreparation` the phase is a pure function of committed rows and the
checkpoint validator refuses a stored phase that differs from the derived one:

| Phase | Derived from |
|---|---|
| `Preparing` | Some copy is still `Assigned` |
| `Catchup` | Every copy is at least `Installed`, some not yet `CaughtUp` |
| `Custody` | Every copy is at least `CaughtUp`, some not yet `CustodyVerified` |
| `Promoting` | Every copy is at least `CustodyVerified`; voters may still await promotion, or all work is done and the session log has not yet committed the cutover |
| `Cutover` | The cutover fence is recorded |
| `Failed` | Any copy is `Failed` and no cutover fence exists |

The cutover fence proves that the session log committed the desired voters, so
the partition records it only when every desired voter is `Promoted` and no
copy is `Failed` (`NotReady` otherwise). The fence carries the group's actual
membership epoch at signing: several committed configuration changes (one
learner and one promotion per new voter) precede one cutover, so both the
directory (`validate_transition_fence`) and the session log (its live cutover
rule and its checkpoint validator) require the fence's epoch to be *at least*
the epoch the placement itself implies, never exactly one above the active
fence; activation then sets the session's epoch to the fence's. After the fence, copies keep
reporting readiness while they catch up to the barrier; a refusal can no longer
fail an assignment (`Phase`). `Activate` still requires each copy's signed
readiness at or beyond the barrier, and now also requires every copy at its
required phase (`Promoted` for voters, `CustodyVerified` otherwise) with
`custody_epoch == next_placement`. Activation takes the pending plan, installs
the new placement and epochs, and moves every node of the previous placement
that the new one does not keep into the session's `retiring` map.

`Plan` carries the load-report epochs the planner relied on (`observations`);
each must name a node of the desired placement whose committed load report is at
least that new, so a plan cannot cite a report the partition never saw.

## 4. Retiring copies

`SessionDescriptor.retiring` holds one `AssignmentProgress` per copy left behind
by the last activation, at phase `Active` with the roles and generation it held
under the previous placement, `through` at the cutover barrier and
`custody_epoch` at the previous placement epoch. `SessionChange::Drain` moves a
copy to `Draining`; `SessionChange::Retire` removes a `Draining` copy and is a
no-op when the copy is already gone. Both are keyed by the activation's
operation id (`WrongOperation` otherwise) so a stale controller cannot drain a
copy under a later activation, and a node that a later placement re-includes
leaves the retiring map at that activation. Retirement is what the retention
coordinator (R8) will gate on pins; until then the controller retires only after
the drain has completed locally.

## 5. Refusals, the guarantee report, disk headroom and custody facts

`SessionChange::Refuse` records a `Refusal { operation, code, node, attempt, at }`.
A refusal naming a node fails that node's current attempt (the attempt must
match, the plan must not be cut over, and a failed attempt is not failed twice);
a refusal naming no node concerns a plan that was never committed (for example
`NoPlacement` from the planner) and must not name the live plan or the active
authority. The session keeps the newest `PartitionConfig::max_refusals` entries
(default 16) oldest first; an identical refusal is not recorded twice.
`RefusalCode::retryable()` distinguishes conditions another attempt may answer
(`NodeCapacity`, `DiskCapacity`, `CustodyUnavailable`, `LearnerBehind`,
`Expired`, `Unreachable`) from those that need a different plan (`NoPlacement`,
`Residency`, `Quorum`).

`effective_guarantee(&SessionDescriptor, &nodes)` returns a `GuaranteeReport`:
`desired` is the pending plan's intent (or the active policy), `achieved` is the
largest number of promised failure domains the *active* placement survives given
the current node registry (a missing, re-enrolled or ineligible member counts
as lost; a member whose domain cannot be evaluated makes `achieved` `None`), and
`blocked_by` lists, per node, what stands between the two: outstanding
assignment phases, refusals, the awaited cutover or activation, and copies still
draining. It allocates one bounded vector and is the source of `cluster status`
in the operator batch.

`NodeLoad` gains `disk_available`. The planner skips nodes below the caller's
minimum headroom (`PartitionConfig::min_disk_available`, default 64 MiB) and
prefers roomier nodes at equal load and memory. The frozen V1 row codec writes
the original five fields and restores `disk_available` as zero, which never
makes a node eligible for new assignments until it reports again.

`AuthorityFact::Custody(CustodyProof)` is appended for the custody batch (R8):
a self-signed statement, verified like readiness, that a node holds one
session's exact content set through a prefix at a placement epoch.

## 6. Formats

The partition checkpoint is schema 2. Schema 1 checkpoints (control checkpoint
schemas 1–3) decode through `PartitionCheckpointV1` and convert: a plan in
preparation derives one row per desired node, `CustodyVerified` where readiness
was recorded, `Promoted` for voters when the cutover fence was recorded; a
schema 1 fence over a voter that never reported readiness cannot be represented
and the restore refuses (`Phase`) rather than inventing custody. The control
replica writes checkpoint schema 4 (the schema 3 layout with the current
bootstrap) and command envelope schema 2 (the schema 1 layout with the current
`NodeLoad` and `Plan`); it still reads schema 1 envelopes, whose load reports and
plans predate these fields and therefore fail to decode. Because a control
group's genesis identity hashes its bootstrap bytes, a partition group
bootstrapped at schema 1 is not the same group at schema 2; root groups are
unchanged. These formats never shipped, so this is recorded as decision F31 in
[07](07-decisions-and-traceability.md) rather than as a migration.

## 7. The placement agent and its journals

Implemented 2026-09-09 ("The placement agent registers the founder's session
(R6.2)" in [09](09-implementation-status.md)). Every `NetworkService` runs one
`PlacementAgent` (`crates/focal-node/src/placement_agent.rs`) beside the root
controller. It acts only where the partition owner is hosted locally and this
node leads it, and otherwise over the protocol of §8. One tick, every 250 ms,
does at most one thing:

1. **Resolve journaled intents first.** Two `IntentJournal`s
   (`placement_journal.rs`, directories `cluster/placement-root` and
   `cluster/placement-partition`, markers `PLACEMENT-ROOT.initialized` and
   `PLACEMENT-PARTITION.initialized`) hold the agent's last unresolved
   `ControlRequest` per metadata owner under one stable client identity
   (`PlacementAgent::client(cluster, node)`). An intent is journaled before it
   is proposed and resubmitted with the identical request identity until the
   owner returns a receipt or refuses it before admission (a stale compare, an
   unverified proof, an invalid or foreign command); not-leader, not-ready,
   capacity, unavailable and unknown outcomes keep it pending. A restart reopens
   both journals, so a decision is neither repeated nor lost.
2. **Register the founder's session.** From the hosted replica's exported
   facts (`ReplicaHost::registration_facts`, read on the owner thread) the
   agent captures the existing `FirstSessionPlan`, submits the root
   `BootstrapGroup` grant if the session's group has none, obtains the
   session-log witness of the `Created` placement (`propose_placement` returns
   the committed record exactly), enrolls the founder in the partition,
   signs the session fact with the root owner's `prepare_session_proof`, and
   creates the session descriptor. Once the directory holds the session the
   agent never captures the plan again, so a session whose log has moved past
   its creation fence is not a registration conflict.
3. **Report load.** Whenever the node has no load row, its enrollment
   generation changed, or 30 s passed: `NodeLoad { available_memory }` from the
   node's whole memory allowance, `active_weight` from the installed replica
   count, `disk_available` as the data volume's free bytes that no queued
   durable write has been promised (§10), and a report epoch above both the
   committed one and the clock.
4. **Answer assignments with signed readiness.** For a pending plan that names
   this node and has begun preparation, once the hosted replica's own
   placement fence is the plan's cutover record (route, epoch and operation
   match), the agent exports a checkpoint (`checkpoint_evidence`), verifies
   custody of its exact prefix (`ContentHost::verify_prefix` against the
   installed custody scope), builds `ReplicaReady { through, custody }` with a
   zero attestation, has the root owner validate it against the session's
   installed group (`prepare_replica_ready_proof`: this node at its enrolled
   generation as voter, outgoing voter or learner; enrollment current for the
   proof window), signs it with the node credential, completes the attestation
   and submits `SessionChange::Ready` under `VerifiedPartition` evidence
   carrying that proof. Readiness is reported once per verified prefix: again
   only when a recorded cutover barrier lies beyond the prefix already
   reported, and never more than once per two seconds per plan, because a
   checkpoint export is not free.

Custody under the old route is deliberately not readiness for the new one; the
session log's cutover record is the first fact the agent waits for.

## 8. The placement protocol between nodes

Implemented 2026-09-09 ("A joined host installs its assignment and signs
under a quorum (R6.3)" in [09](09-implementation-status.md)). Two node-only
wire operations, both certificate-bound and refused to trusted local grants
([03](03-rust-workspace-and-interfaces.md) §9):

- **`PlacementControl { group, request }`** (tag 28) reaches a directory
  partition owner from any enrolled node. Reads are the bounded discovery
  selectors of `PeerControl`. A submit is decoded only if it is a
  `VerifiedPartition` command about the sender itself: its own `Enroll`,
  its own `ReportLoad`, its own `Ready`, or its own `Progress` at `Assigned`
  or `Installed`; a plan, a fence, a promotion or another node's facts are
  refused before any collection is decoded. The owner binds the sender's
  certificate to the enrollment it has installed (the root's own registry, or
  the copy installed with a partition's authority) and requires the request
  client to be the sender's enrolled principal.
- **`SessionSign { group, request }`** (tag 29) asks a node for its own
  signature over one `SessionFact` it can witness from its hosted replica:
  a **placement record**, answered exactly by the replica whether it leads
  or follows (`ReplicaHost::placement_witness`), or a **membership**, the
  next `GroupAuthorityGrant` checked against the configuration the replica
  applied and the receipt of the change that applied it. The reply carries
  that node's signature alone; the caller merges signatures over one
  identical statement (`placement_collect::Collected`) and stops at the
  first voter majority. An unreachable or refusing voter never vetoes.

The agent of §7 now runs on every node. Where the partition owner is not
local it reads state and authority through `PeerControl` and submits through
`PlacementControl` at the node the first partition was delegated to; its
partition journal records the identity the owner binds submissions to (the
agent's derived local principal for a local owner, the enrolled principal on
the wire), so a journaled intent is retried under the identity that
journaled it. On every node the agent:

- enrolls itself in the partition from the root authority's grant for its
  node, at the grant's generation;
- installs the copy a plan assigns to it: a `DurableNode` joining the
  session's log group on the shared WAL under the founder's bootstrap
  membership, a `Session` hosting the native engine, a fleet install, and a
  custody policy at the plan's target scope; the copy is recorded in
  `cluster/placement-installs` and reopened at every start at the route and
  placement epoch its own log has committed;
- reports `Installed` for the copy, verifies custody and signs readiness
  once the log has cut over (§7), and keeps each hosted ledger's custody
  scope at the placement the directory has activated;
- serves `SessionSign` and quorum collection through `NetworkHandles::placement`
  (`PlacementHandle::{sign, collect, status}`), the handle a controller or
  operator uses.

Membership epochs are reconciled between the session log, the directory and
the root authority: a `ChangeGroup` that only adds or removes learners keeps
the grant's `membership_epoch`, a change to the voter set advances it by one,
so the epoch a cutover fence carries equals the grant's epoch at signing.
The root owner prepares a node's membership signature
(`prepare_membership_proof`) only for a current voter of the group and only
over a legal successor grant whose members are enrolled for the window.

A host's serving scope (`ReplicaConfig::{route_epoch, policy_revision}`) and
custody policy follow the placement its session log has *activated*; a
pending cutover record changes neither until activation. After a placement
changes, requests carry the new route epoch: route discovery by participant
clients is R7's route cache, and until then a client on a changed session
must be told the route.

## 9. The placement controller

`placement_controller.rs` is the last step of the agent's tick and runs only
where the partition owner is local, this node leads it, and this node also
leads the session's own log (so it can propose the session's configuration
changes and placement records itself). It keeps no state of its own: every
step is a function of the committed partition checkpoint, the session's
applied membership and latest configuration receipt (`registration_facts`),
the root authority grant, and the session's placement fences, so a restarted
or newly elected controller resumes at the same step, and every command goes
through the agent's exact-retry journals (§7). One tick performs at most one
step per session.

**Grant follows log.** Before anything else, a configuration the session log
committed but the root grant does not name is installed as `ChangeGroup`: the
grant's voters, outgoing voters and learners are taken from the applied
configuration, its epoch advances by one only when the voter sets differ, and
the membership proof is collected from the current grant's voters
(`SessionSign` with the latest configuration receipt, §8). Every fact signed
under a grant is fenced by that grant, so this is the ordering that makes the
later signatures verify.

**Driving a plan.** With a pending placement:

| Committed state | Step |
|---|---|
| `Planned` | `BeginPreparation` (creates the `Assigned` rows) |
| A desired voter is not in the configuration and its row is at least `Installed` | `AddLearner` in the session log, with a deterministic change id derived from `(ledger, operation, node, kind)` so a lost reply finds the retained receipt |
| A desired voter is a learner and its row is at least `CaughtUp` | `Promote` |
| Configuration voters equal the desired voters and no cutover fence for this operation is committed | propose the cutover record (`membership_epoch` = the grant's epoch) |
| Cutover record committed; a voter the grant names has `CustodyVerified` | record `Promoted` for it |
| Every desired voter `Promoted` | collect the voter-majority proof of the cutover fence and record it as the barrier (`SessionChange::Cutover`) |
| Phase `Cutover`; every copy has signed readiness at or beyond the barrier and stands at its required phase | propose the `Activated` record, then collect its proof and `Activate` |
| `Failed` | nothing; the operator re-plans |

A copy reports `CaughtUp` itself, from its replica diagnostics (a known
leader, a committed index above zero, and applied at or beyond committed);
the controller never asserts a copy's progress.

**After activation.** Each copy in `retiring` is drained (`Drain`), removed
from the session log (`Remove`, same deterministic change id), then retired
(`Retire`). When the active placement no longer verifies against the live
node registry (a member's enrollment gone or its grant expired), the
controller plans again under the active policy with a heal operation id
derived from the ledger and the active record hash; when no placement is
possible it records one `NoPlacement` refusal for that operation and stops
until the registry changes. The planner's leader hint is not applied: the
log's leader stays where it is until a transfer, which no batch performs yet.

Errors are the agent's: a copy that is behind (`Behind`) and a collection that
could not reach a majority (`Collect`) are retryable and re-tick; the rest
back off (§7).

## 10. Admission: tenants, memory and the volume

Implemented 2026-09-09 ("Tenant admission and the disk envelope (R6.5)" in
[09](09-implementation-status.md)). Instruction 5 of R6 asks that a session's
admission be connected to the node's actual budgets, its disk and custody
capacity and its placement eligibility, with backpressure that stays visible
and fair under a noisy tenant. Three mechanisms carry it.

**The disk envelope.** `focal_memory::DiskBudget` is one shared envelope per
data volume. It performs no IO: each durable owner samples the volume with a
cadence the envelope decides (`sample_due`: never sampled, a bounded run of
admissions since the last sample, or the estimate within twice the headroom
of the watermark) and reports the free bytes it saw. Every durable write is
*promised* its bytes before it is queued (`reserve(kind, lane, bytes)` →
`DiskReservation`) and refused with `MemoryError::DiskCapacity` before any
acknowledgement when the volume cannot take it; the headroom is never spent,
and the completion reserve only by completion-lane work, so a tenant's
uploads cannot consume the space replication and checkpoints need. A
reservation committed after its durable fence lowers the estimate until the
next sample; one dropped uncommitted returns its promise. The owners: the WAL
writer promises every append and checkpoint-rewrite batch and commits it
after `fsync` and the fence install (which covers Raft snapshots, since they
are WAL records); the content store promises an upload's staging bytes at
`begin` (returned when the part is removed at `finish`), the sealed object at
`seal`, every imported chunk and manifest and every custody record (all
committed once installed). `SharedWal::available_bytes` now reports the
envelope's unpromised free bytes (zero while the volume cannot be sampled),
which is what the native admission watermark compares against and what the
node reports as `NodeLoad.disk_available`. The node's envelope guards the
volume with the standard 64 MiB physical watermark; the WAL writer, the
content store and the checkpoint path share it (`SharedWal::disk_budget`,
`ContentStore::open_with_disk`). The operator's `FOCAL_DISK_HEADROOM_BYTES`
stays the native admission gate for *fresh* work and does not move the
envelope, so a campaign that raises the gate above the volume's free space
still lets a node recover, commit its control plane, serve reads and answer
exact retries while every fresh candidate is refused. The envelope's
counters are shared by owners on different threads with independent
lifetimes, the same boundary as the memory budget (decision F32 in
[07](07-decisions-and-traceability.md); [10](10-ownership-and-failure-policy.md)).

**Per-domain staging.** The content store bounds the staged bytes of one
content domain (one tenant) to `StoreLimits::domain_staging_bytes()`: half of
the staging allowance and never less than one maximal upload. A tenant
uploading at full speed leaves the other half to everyone else, and the
accounting is rebuilt from the recovered uploads at open.

**Tenant admission.** `crates/focal-node/src/admission.rs` keeps the
tenants a node hosts and the memory each draws from the node budget. A node
starts with its own tenant on the budget its fleet was spawned with. When a
committed placement first assigns a session of another tenant to the node,
the agent admits the tenant before it records the copy as installed: under
`node.max_tenants` (default 8, at most 1024) and only if the node budget can
fund the plan's `required_memory` within the fixed per-tenant allowance
(512 MiB with a 128 MiB completion reserve, a child of the node budget the
node's live limit still arbitrates). An admitted tenant is registered on the
running fleet worker (`FleetManager::admit_tenant`, idempotent) with the same
scheduler quota every tenant receives, so its sessions queue under weighted
deficit round-robin beside the others and its ordinary work is throttled by
its own quota, never by another tenant's completion reserve. A node that
cannot take the tenant records `Refuse { NodeCapacity, node }` against its
own assignment, which fails that attempt in the directory and leaves the
planner to choose another node; nothing is hosted unfunded. The agent's
status carries an `AdmissionReport`: the bound, the node's memory limit,
use and reserve, the volume's sampled free bytes, promised bytes and
headroom, and per tenant its weight, allowance, use, sessions with queued
work and queued items and bytes (`FleetManager::tenant_usage`). Control
messages stay bounded as before: two in-flight requests per peer and 256 per
pool, 256 KiB per placement-control request, a 32-item control queue with
64 pending requests, and one unresolved intent per agent journal.

**Limits.** No path creates a session of a second tenant yet (the founder's
session is the only application session a deployment has), so the agent's
refusal is exercised at the admission table and the fleet, not through a
foreign assignment; the operator batch adds session creation. The per-tenant
allowance is fixed rather than derived from the plan's policy, and custody
capacity is bounded per domain at staging, not per tenant across the object
store.

## 11. Credential renewal, retirement and revocation

Implemented 2026-09-09 ("A node renews its own credential and every proof
follows its key (R6.6)" in [09](09-implementation-status.md); decision F33).
Instruction 6 of R6 asks for authority refresh, expiry, renewal, revocation,
reconnect and stale-node rejection without a joined node becoming tenant
administration.

**Identity is the key.** A node's directory identity (`NodeEnrollment.identity`)
is the identity of its enrolled key (`EnrollmentReceipt::public_key`), not
the fingerprint of a certificate. Every proof a node signs is verified by the
certificate the enrollment registry authorizes at that moment and bound to
the key that certificate carries (`certificate_key_hash`), so a renewed
certificate of the same key keeps signing for the same enrollment: no new
generation, no new grants, no stale progress rows. The transport still
authenticates by certificate fingerprint; the contact table and the peer
registry therefore learn the new fingerprint when the node presents it.

**Renewal.** The enrollment registry (schema 2) gains `Change::Renew`: the
same key and CSR receive a fresh certificate and lifetime, and the
certificate they replace moves to a bounded *retired* table with the moment
it stops authorizing (`retire_previous_at`, the sponsor's grace, 60 s by
default or `FOCAL_CREDENTIAL_GRACE_SECONDS`, never past the old expiry). Until
then both certificates authorize, so connections and statements in flight
complete; afterwards the old one is refused as expired, and a stale node
presenting it fails `authorize_node_contact`. A renewal is requested over the
enrollment transport (`EnrollmentClient::renew`, frame kind 3) with a
`RenewRequest` signed by the credential the holder presents (the current
certificate, or the retired one while it authorizes); the sponsor (the
founder, which holds the issuing authority) prepares it against the holder's
`holds_until` — a holder whose expiry the registry has already moved past is
answered with the committed renewal, never a second issuance — commits it
through the root group, and replies once its own ingress grants the new
certificate. A renewal decided within the second the current certificate was
issued cannot extend it and is refused. Revoking the invitation revokes both
certificates.

**The holder.** The network controller of a joined node renews ten days ahead
of expiry, or now on `cluster credentials renew` (`AdminCommand::RenewCredential`
over the admin socket, `cluster.credentials.renew` in the MCP catalogue,
served by a `CredentialHandle` in the node's handles). It signs the request
with the credential it holds, installs the renewed receipt over the one on
disk under the same key (`JoinKey::renew`, refusing an older receipt), then
presents the new certificate everywhere at once: the listener's TLS
configuration is replaced on the live endpoint (`ListenerIdentity`), the
peer pool presents it on every connection opened from then on and retires
the cached ones (`PeerConnectionPool::replace_identity`), and the placement
agent signs with it from then on (`PlacementHandle::replace_credentials`).
Its contact in the root is announced again under the committed generation,
so peers reach it by the fingerprint it now presents. The root derives a
contact command from the announcement with its own decision time, so a retry
after a lost reply is never the exact request the root retained under that
sequence: the controller reads a conflicting or compare-failed reply as an
earlier attempt having committed and lets the next observation of the
contact table settle it, instead of ending on it (a latent fault of the first
announcement as well, found under load). A crash between the
sponsor's commit and the install leaves the node holding the retired receipt:
it starts (its registry knows the same identity and key), sees the committed
registry ahead of what it holds, renews at once and converges on the
committed renewal without another issuance, and never announces the retired
certificate over the renewed one. A failed attempt is retried after a minute;
the controller keeps running on the credential it holds.

**Limits.** The founder's identity is the bootstrap authority's own server
certificate and is not renewed here; CA rotation is not implemented; client
(participant) credentials are not renewed yet; key rotation with a proof of
the previous key (`cluster credentials rotate`) is planned; the contact
re-announcement's request sequence is the committed contact generation plus
one, which assumes every committed contact command of a node advanced its
generation.

## 12. Liveness: probes, suspicion, extension and coordinates

Design (decision F34); implemented 2026-09-09 (`focal-node/src/liveness.rs`
with `liveness/{driver,wire,coordinates,gossip,health,suspicion}.rs`; evidence
in [09](09-implementation-status.md) under "Liveness (R6.7)"). Before it a node's loss was only
inferred from enrollment expiry, eligibility and the controller's re-plan
when a placement no longer verified; nothing observed whether a host answers.
The fleet needs a failure detector whose outcomes are committed facts, never a
node's private opinion, and whose false positives under load stay bounded.

**Membership and probes.** Every node runs one liveness driver beside its
controller. Its members are the peers the network controller installed as
routes (the committed contacts of enrolled, unrevoked nodes), so a node that
was never reachable is *unconfirmed* and cannot be suspected; a peer becomes
*confirmed* on its first acknowledgement. Each period the driver probes the
next member of a shuffled round (`Operation::Probe`, registered tag 30, Node
role, certificate-bound, at most 8 KiB, answered by the data service from the
driver's state without an owner round trip, and sent on the peer pool's own
probe lane so replication to an unresponsive peer cannot starve the
detector). A direct probe that
times out is followed by indirect probes through up to three other members,
which probe the target themselves and relay its answer; only when none
answers does suspicion start. Probe and acknowledgement carry the sender's
network coordinate, its local health and a bounded piggyback of membership
updates, so dissemination rides the probes (each update is rebroadcast at
most λ·log(n+1) times, the least-broadcast first; the buffer holds at most
64 updates).

**Lifeguard.** The driver keeps a *local health multiplier*: a score in 0..8
raised by its own probe timeouts, by suspicions of itself it had to refute,
and by ticks that ran late, lowered by successful probes and answers; every
timeout it applies is multiplied by `1 + 0.25·score` (1× to 3×), so a node
that is itself slow accuses nobody hastily. A suspicion carries the target's
incarnation and the originator; independent confirmations from other members
(gossiped suspicions of the same incarnation) shorten its deadline from
`max` toward `min` as `max − (max − min)·log(C+1)/log(K+1)` (K the
confirmation target, at most the member count), the originator's own vote
never counting; `min` and `max` scale with `log(n+1)` times the probe period
and with the local health multiplier. A refutation (an `Alive` with a higher
incarnation from the target, which increments its incarnation whenever it
learns it is suspected) clears the suspicion. A suspicion timer is set once
and only shortened by confirmations, never rescheduled by repeated gossip of
the same evidence, so gossip cannot starve it. An expired suspicion declares
the target dead; a later acknowledgement or a higher incarnation revives it.

**Late extension.** A host under load that learns it is suspected refutes
(above) and, when its own health score says it is slow, also asks its
accuser for more time: the next probe it sends the accuser carries an
extension request naming its new incarnation, a progress witness (the
placement agent's completed-tick counter, which a stuck host cannot advance)
and whether its admission is refusing capacity. The accuser keeps one
extension tracker per member and incarnation and grants
`max(min_grant, base/2^k)` for the k-th extension (base one suspicion
minimum at ten members, three seconds), at most five, only when the witness
advanced since the last grant, never within one probe period of the previous
grant, and never to a host that reports itself overloaded (such a host is
healed, not extended). A grant is a grace window during which a failed probe
of that member starts no suspicion, and it lengthens the deadline of a
suspicion already running; a new incarnation resets the tracker. The
decision rides the acknowledgement, and every request, grant and denial is a
bounded event in the driver's published view.

**Coordinates.** Each node maintains a Vivaldi coordinate (an eight-dimensional
vector, a height for the access link, an adjustment and an error estimate)
updated from every acknowledged probe's round trip against the peer's
coordinate, with the usual centering gravity and error decay. The probe
timeout for a peer is `clamp(base, k·rtt_ucb, cap)` times the local health
multiplier, where `rtt_ucb = rtt̂ + k_σ·σ` uses the two coordinates' errors,
so a cross-region member is neither suspected for its distance nor a local
member waited on for seconds; a coordinate without enough samples falls back
to conservative defaults.

**Committed outcomes.** A node's liveness becomes a directory fact through the
partition owner: each tick the placement agent tells its driver who the
members are (every other eligible enrolled node at its generation), its
progress witness and its admission state, and on the node that leads the
partition it reconciles the driver's view into
`PartitionOperation::Liveness { node, generation, alive, incarnation, witness,
decided_at }` (`witness` the leader's applied control index): one settled
verdict per tick, never for a suspicion, never for a member it has not
confirmed, never an older incarnation than the committed one, and never the
first verdict of a healthy node (alive is the default). The partition applies
it only for the current enrollment generation and a non-decreasing
incarnation, into
`NodeRecord.liveness` (partition checkpoint schema 3; schema 2 restores with
no liveness known). The planner skips a dead node, `verify_placement` treats
a dead member as stale so the controller heals the placement,
`effective_guarantee` counts it as lost (`BlockReason::DeadNode`), and the
guarantee tracker of the deployment batch will read the same fact. Liveness
never touches enrollment: a dead node keeps its identity, grants and
generation, and a revived node needs no re-enrollment.

**Limits.** Every timeout constant is a node-local default (probe period 1 s,
base timeout 300 ms, cap 2 s, suspicion 3–18 s at ten members before the
health multiplier) rather than configuration; topology facts are still
unknown geography; the fleet's liveness is one flat membership rather than
the role-aware strategies the reference design uses across tiers; the driver
answers probes for its whole node and holds at most 16 probes in flight, 64
gossip updates and 32 events; the founder's own liveness is never committed
because it leads the partition (leader transfer is a carried limit); the
extension path is exercised through crafted probes in tests, not by a
loaded host.

## 13. Remaining R6 batches

Planned, not yet implemented; each will be recorded in [09](09-implementation-status.md)
when it closes.

1. **Split/merge with the route cache**, then the **operator API**
   (`AdminRead::Placement`, `cluster status`, `cluster plan`, application
   session creation for another tenant).
