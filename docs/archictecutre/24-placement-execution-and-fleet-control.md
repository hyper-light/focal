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
copy is `Failed` (`NotReady` otherwise). The desired voters need not be the
log's whole voter set: a current voter the plan drops (a drained or dead
host, the founder a larger fleet leaves out) keeps its vote through the
cutover and the activation — the session log's cutover rule, the root's
proof preparation and the controller all ask that every desired voter votes,
not that nothing else does (changed 2026-09-10, §19) — and is removed from
the log only when the activation retires it (§4), so the old contract holds
until the new one is verified. The fence carries the group's actual
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

The partition checkpoint is schema 7 (2026-09-10: published range holders
per session; schema 6 added the founding node; every older schema converts
on decode). The account below is of the schema 2 step. Schema 1 checkpoints (control checkpoint
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
  or `Installed` — or, addressed to the root, the one root authority change
  a node makes for itself: `BootstrapGroup` for a session it created alone,
  a grant at epoch 1 naming the sender as its only voter (§16, 2026-09-10);
  a plan, a fence, a promotion, another group change or another node's
  facts are refused before any collection is decoded. The owner binds the sender's
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

A third node-only operation, **`RangeControl { group, request }`** (tag
31), asks a node for one range-movement fact its hosted replica of `group`
can state — its readiness for a replacement, its seal of a source at the
barrier, or its progress on a member it holds — each carrying the member's
rows digest; the session authority verifies the fact against its own digest
of the barrier-frozen member before it attests and proposes it
([25 §7](25-parallel-materialization-and-ranges.md)).

A fourth node-only operation, **`SessionControl { group, request }`** (tag
32), lets the node that leads a partition drive a session it does not
lead: it asks the node that leads the session's log for the session's
facts, or to apply one membership change or placement record to the log
under the log's own committed rules (§9, 2026-09-10). The leader honours it
only from a voter of the group that owns the partition whose namespace
holds the session, and answers `NotLeader { leader }` when the log leads
elsewhere.

A further exchange rides the existing custody operation rather than a new tag:
**`CustodyRequest::SeedChunk {hash, max_bytes}`** asks a peer for one
checkpoint seed chunk it holds, answered with `CustodyReply::SeedChunk
{hash, bytes}` and verified against the hash by the puller. A node serves it
to nodes of the ledger's installed placement at its route and to nodes of a
placement the directory is preparing (the agent's pending announcement,
[25 §5](25-parallel-materialization-and-ranges.md)), so a fresh copy can
seed its Session before the placement it belongs to activates. The
authority checkpoints once a learner it added has applied behind a
compacted log, since Raft discards a snapshot whose configuration does not
name the recipient ([23 §3](23-native-activation-and-import.md)), and a
hosted session that stops on a fail-closed error in the grouped worker says
why on the node's standard error (`focal: session <id> stopped: …`).

## 9. The placement controller

`placement_controller.rs` is the last step of the agent's tick and runs
where the partition owner is local and this node leads it — for every
session of the partition, whether or not this node votes in it. It keeps no
state of its own: every step is a function of the committed partition
checkpoint, the session's applied membership and latest configuration
receipt (`registration_facts`), the root authority grant, and the session's
placement fences, so a restarted or newly elected controller resumes at the
same step, and every command goes through the agent's exact-retry journals
(§7). One tick performs at most one step per session.

**The controller drives a session through its leader (2026-09-10, R9.3).**
The session's log is changed by the node that leads it. When this node
leads the log, the controller reads the session's facts and applies
membership changes and placement records on its own replica
(`SessionDriver::Local`). Otherwise it drives the log through the node that
leads it (`SessionDriver::Remote`): `Operation::SessionControl { group,
request }` (tag 32, node-only, certificate required; body
`SessionControlRequest { schema, call }`, `SessionCall::{Facts,
Membership(SessionMembershipRequest), Placement(SessionPlacementRequest)}`,
reply `SessionControlReply::{Facts, Membership(view), Placed,
Refused(failure)}` in `Response::Control`, at most 64 KiB) asks the session's
leader to state the session's facts as it applied them, or to apply one
membership change under its expected configuration, or to propose one
placement record; the leader applies it under the log's own committed
rules, exactly as a local call, and honours the request only from a voter
of the group that owns the partition whose namespace holds the session
(`authorized_controller`, from the leader's own observation of the root).
A replica that does not lead answers `NotLeader { leader }`, and the
controller follows the log to where it leads on its next pass; until a
leader is known it asks the replica it hosts, the directory's route, then
the placement's preferred leader. Range movement, balancing and holder
publication ([25](25-parallel-materialization-and-ranges.md) §6–§9) read and
drive the movement map on the log's own replica and therefore run only where
this node leads the log; for that work alone — an operator's queued move, a
transfer pending, a retired map awaiting cleanup, holders the directory has
not published — a voter that does not lead still asks the leader for
leadership (`claim_for_ranges`, one raft transfer message); a plan claims
nothing. Fence proofs are collected from the
session's voters as before (§8): a node signs by witnessing the fact on its
own replica, so the controller needs no vote of its own.

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
| Every desired voter votes in the configuration (a current voter the plan drops keeps its vote until activation retires it, §4, §19 — changed 2026-09-10 from exact equality, which could never cut over a plan that left a current voter out) and no cutover fence for this operation is committed | propose the cutover record (`membership_epoch` = the grant's epoch) |
| Cutover record committed; a voter the grant names has `CustodyVerified` | record `Promoted` for it |
| Every desired voter `Promoted` | collect the voter-majority proof of the cutover fence and record it as the barrier (`SessionChange::Cutover`) |
| Phase `Cutover`; every copy has signed readiness at or beyond the barrier and stands at its required phase | propose the `Activated` record, then collect its proof and `Activate` |
| `Failed` | nothing; the operator re-plans |

A copy reports `CaughtUp` itself, from its replica diagnostics (a known
leader, a committed index above zero, and applied at or beyond committed);
the controller never asserts a copy's progress.

**After activation.** Each copy in `retiring` is drained (`Drain`), removed
from the session log (`Remove`, same deterministic change id), then retired
(`Retire`). The retirement is a partition step that needs no session
leadership: a node that no longer leads the log — the one the plan dropped
included, once its own removal committed and it stepped down — retires
every `Draining` copy the log no longer contains (`retire_departed`,
2026-09-10). When the active placement no longer verifies against the live
node registry (a member's enrollment gone, its grant expired or re-issued,
§19), the controller plans again under the active policy with a heal
operation id derived from the ledger and the active record hash, preferring
the active placement's voters among equally eligible candidates
(`propose_placement_keeping`; the operator's plan request prefers them the
same way) so a heal moves only what it must and an expansion adds hosts to
the copies that exist; when no placement is possible it records one
`NoPlacement` refusal for that operation and stops until the registry
changes. The planner's leader hint is not applied as
such: the log's leader stays where it is until the controller, a voter of
the session with a plan or a transfer to drive, asks for leadership itself
(§9, 2026-09-10).

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

**Rotation (2026-09-10, R9.3).** `cluster credentials rotate`
(`AdminCommand::RotateCredential`, `cluster.credentials.rotate`) moves a
node's credential to a fresh key under the same identity. The holder stages
a key with its own request identity and CSR (`JOIN/node-key.next`, kept
until adopted), asks the sponsor over the enrollment transport with a
`RenewRequest` of schema 2 — the new key's request and CSR, proven by the
credential it holds (`rotation_request`) — and the sponsor prepares
`Change::Rotate { invitation, receipt, retire_previous_at }` (schema 3 of
the registry's command stream, append only): a certificate issued for the
new CSR under the same identity, the receipt naming the new request, key
and CSR, the previous certificate retired with the renewal's grace. A principal is derived from the key an enrollment began with; a rotated
key did not derive it, so the certificate issued for the rotation (and every
renewal after it) carries the principal in a CA-signed subject
(`focal-carried-principal:<principal>`, the founder's
`focal-genesis-principal` rule generalized), and the registry checkpoint,
the sponsor's apply and the holder's saved-material inspection accept an
identity only when its key derived it or a verified certificate carries it
(`identity_bound`). The rotation moves the enrolled-key index to the new
key; the retired credential names the previous key and request. The
registry authenticates a rotation by the proof's key: the key it holds now,
or the key a rotation just retired while it still authorizes, so a retry
after the sponsor committed finds the committed rotation
(`RenewPreparation::Existing`). The holder adopts it (`JoinKey::rotate_into`:
the receipt first, then the key, then the staged material is cleared) and
presents the new certificate everywhere at once as a renewal does. A crash
between the sponsor's commit and the adoption leaves the node holding the
previous key: it starts (the held certificate authorizes through the grace;
`seed_peer_registry` no longer requires the held key to be the registry's),
sees the registry's receipt under another key (`rotation_ahead`) and adopts
the committed rotation from the staged key at once. The root re-grants a
node whose enrolled key changed under its new identity at the next
generation before any other root work (`next_root_command`); the partition
learns the re-grant like a drain's (§19) and the node's seats stay its own.
`cluster credentials get` and the renewal reply report `key_identity`.

**Limits.** The founder's identity is the bootstrap authority's own server
certificate and is neither renewed nor rotated here; CA rotation is not
implemented; client (participant) credentials are not renewed or rotated
yet; the contact re-announcement's request sequence is the committed
contact generation plus one, which assumes every committed contact command
of a node advanced its generation.

A node whose own credential the committed registry no longer authorizes —
revoked, or expired past its grace — stops serving at its next root
observation and refuses to start (`ControllerError::Retired`,
`[credential_retired]`), exactly as a binary behind the upgrade fence does
(§21): the operator drains and removes it and enrolls the host again
(`runbooks/expired-credentials.md`). The placement view shows every node's
`credential` standing (`active`, `retired`, `unknown`).

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
because it leads the partition (a controller claims leadership of the
sessions it drives, §9, but the partition's own leadership is not
transferred); the
extension path is exercised through crafted probes in tests, not by a
loaded host.

## 13. Namespace split and merge

Implemented 2026-09-09 (evidence in [09](09-implementation-status.md) under
"Namespace split and merge (R6.8)"). Until now the directory had one
partition holding every session of the cluster, so the control plane's
metadata owner grew without bound. A partition now splits when it holds too
many sessions and two adjacent partitions merge back when both are small,
through committed facts only: the root's delegations, the partitions' own
logs and one fence both groups sign.

**Model** (`focal-directory`, partition checkpoint schema 4). A seal names
what leaves: `PartitionSeal { operation, destination, next_epoch, revision,
moved, source }`, `moved` the whole namespace for a transfer or a merge and
the upper part from the split key for a split, `source` the partition that
sealed (schemas 1–3 restore with `moved` the whole namespace and `source`
the partition itself). `SealForSplit { operation, destination, next_epoch,
at }` seals `[at, end)`; a sealed partition refuses every command except the
two that end a seal. The split image (`split_image`) is the sealed state
carrying only the moved sessions and, from the start, the destination's
provisional delegation (its partition and group identities derived from the
cluster and the split operation, `split_partition_id`/`split_group_id`, its
epoch the seal's next epoch, no activation yet) with the source's seal; every
voter of the source derives the same image and digest. `RootOperation::Split
{ start, at, expected_epoch, destination, fence }` narrows the delegation at
`start` to `[start, at)` at the next epoch and delegates `[at, end)` to the
destination at the same epoch, both activated by the one fence (source, the
destination, the moved range, the epochs, the sealed revision, the image
digest); `verify_delegation` requires a majority of the source group, whose
scope holds the moved keys, and of the destination group, whose scope holds
them (a transfer or split destination) or the keys right below them (a merge
destination). `Install { delegation }` is the destination's first command:
the bootstrapped image becomes the delegated partition under the fence, its
digest checked against its own state. `Release { delegation }` is the
source's: it drops the moved sessions, narrows to the kept delegation and
unseals, recomputing the image digest the fence names. A merge is the
reverse: the upper partition `SealForTransfer`s its whole namespace to the
one below, `RootOperation::Merge { start, right, expected_epoch,
right_expected_epoch, fence }` extends the lower delegation over the union at
its next epoch and removes the upper, and `Absorb { delegation, moved }` on
the lower partition takes the hash-verified sealed checkpoint of the upper
(its sessions, its node records at the higher generation), bounded by
`PartitionConfig.max_absorb_sessions` (256) because the command carries the
checkpoint; the upper partition stays sealed and retires. A delegation's
activation fence is validated by shape: arrived (the fence's destination
over exactly its namespace), released (the fence's source with the moved
keys right above it) or absorbed (the fence's destination with the moved
keys as its upper part).

**Hosting** (`focal-node`). `PartitionPlan` (the former first-partition plan)
names a partition, its group, its root delegation, the digest of the image it
bootstraps from and the control genesis fixed at planning;
`PartitionPlan::split_destination` plans a destination on the founder from
the image. A root permit authorizes hosting a partition the root delegates
to (whatever its namespace and epoch became), or an image plan whose split
the root has not committed yet, and issues the destination group's
`BootstrapGroup` grant (this node its single voter). `DirectoryHandle` reaches
every hosted partition by partition or by group (`host_of`,
`host_of_group`, `hosted`) and forwards `HostRequest::{Host, Retire}` to the
host manager, which records each hosted partition under
`cluster/partitions/<partition>.partition` (plan facts plus the image) before
opening its group on the shared WAL, reopens every record at start, drives
each partition's replication and authority refresh beside the first, and
stops every partition with the service; a retired partition's record is
removed so a restart forgets it (its sealed group keeps refusing until
shutdown). The data service routes `PeerControl`/`PlacementControl` by
group to whichever partition hosts it; the owner registry holds a slot per
hosted partition (32).

**Agent** ([§7](#7-the-placement-agent)). Each tick the placement agent
visits every root delegation in namespace order: a hosted partition this
node leads locally, the rest through the founder; it keeps one exact-retry
journal per partition (`placement-partition` for the first,
`placement-partition-<id>` for later ones), observes each partition once,
reports liveness facts from the union of their node tables, and runs its
steps (registration of the founder's session in the partition whose
namespace holds it, enrollment, load, assignments, readiness, verdicts,
control) per partition; a sealed partition only reshapes.

**Reshape** (`partition_split.rs`, on the node leading both the root and the
partition). Sessions at or above the split threshold (2,048, half the
partition capacity; an in-process test override and the
`FOCAL_PARTITION_SPLIT_SESSIONS`/`FOCAL_PARTITION_MERGE_SESSIONS` knobs)
seal the partition at the median of its keys strictly above its start (no
valid key, no split). A sealed source then, one committed step per tick and
each idempotent from the committed state: grants the destination group,
requests the host, waits until it leads the destination, commits the root
split under both groups' signatures (the fence's `destination_ready` is the
image digest the destination holds), installs the destination and releases
the source; the partition-level commands carry the same two proofs, which
the partition verifies at its installed authority. Two adjacent partitions
both at or under the merge threshold (256) whose union stays under the split
threshold merge: the upper seals for the lower, the root merges, the lower
absorbs, the upper host retires. A restart resumes from the committed facts:
the hosted record, the seal, the root's delegations and each partition's
epoch say exactly which step is next.

**Limits.** Partition groups are hosted by the founder alone (one voter; the
controller does not place control groups on other hosts yet, so a split
bounds each metadata owner's state but not the founder's total); a merge
moves at most 256 sessions because the absorb command carries the sealed
checkpoint; the founder's session is the only session a test can create,
so the split qualification uses a threshold of one; there is no route cache
yet: every node still resolves a session's partition from the root's
delegations and reaches the founder for a partition it does not host.

## 14. The route cache and serving fences

Implemented 2026-09-09 (evidence in [09](09-implementation-status.md) under
"The route cache and serving fences (R6.9)"). Until now a client reached a
ledger only through the node it was configured with: a node that hosted no
copy answered `Unavailable`, a follower answered a mutation with its own
refusal, and a client behind a placement change learned only that its epoch
was stale. Routing now comes from the directory and from each replica's own
knowledge of its leader.

**Route changes in the partition.** The partition checkpoint (schema 5, now 6
with §16; schemas 1–4 restore with an empty log complete only from their revision)
keeps `routes`, a bounded log (`PartitionConfig.max_route_log`, 1,024) of
`RouteChange { revision, ledger, route_epoch }` appended whenever a session's
route epoch changes (creation, a cutover, the sessions an absorb takes), and
`routes_from`, the revision the log is complete after (the newest evicted
change). `DirectoryPartition::route_changes(after)` returns the
`InvalidationBatch` a cache applies; when `after` lies before the floor the
batch starts at the floor instead, which the cache reads as a gap and clears
that partition's rows. A split image starts an empty log. Two control reads
serve it, open to enrolled nodes through `PeerControl`: `ControlRead::Route
{ ledger }` (the partition's `SessionRoute`: partition, delegation epoch,
source revision, route/membership/placement epochs, the preferred leader at
its generation, the activation record hash) and `ControlRead::RouteChanges {
after_revision }`.

**The route cache host** (`route_cache_host.rs`). One driver per node owns
`focal_directory::RouteCache` (1,024 routes, 128 watched partitions, thirty
second TTL, charged through the node budget) and answers the data service
through a handle: `resolve(ledger)` returns the cached route or reads the
owning partition (the root's delegation names it; a hosted partition is read
locally, any other through the founder over `PeerControl`), inserting the
result; `hint(node, epoch)` turns a node into the `RouteHint` a client
follows (the peer pool's installed reachability: address and certificate
name), never for this node itself. Once a second the driver pulls every
watched partition's route changes since the revision it applied and
invalidates exactly those rows; a gap clears the partition.

**Redirects at the data service.** Requests from enrolled nodes
(replication, custody, support) address replicas directly and are never
routed. For a client request the data service consults the directory before
any replica sees it: with no copy of the ledger here, the answer is
`RouteChanged` with the directory's leader hint (a node that only follows
redirects once more, to the leader it knows) or `Unavailable` when nothing is
known; with a copy, a claimed route epoch behind the directory's is answered
with the current epoch at the replica's own leader (this node's endpoint
when it leads, else the leader's), a mutation on a follower is redirected to
the leader the replica's consensus state names, and a current read is served
here whoever leads. The replica's own route-epoch check stays as the last
fence behind the cache. The client already follows `RouteChanged` hints over
QUIC (a new connection to the hinted endpoint under the cluster's trust,
the epoch updated per ledger) and refuses to loop on an unchanged hint; the
local socket transport cannot follow one and surfaces it.

**Limits.** The directory's `leader` is the placement's preferred leader,
not the log's: without leader transfer the two differ after an expansion,
so a redirect from a node without a copy may take one extra hop through a
follower; a cache watching the source of a split keeps routes of the moved
sessions until they expire (the leader is unchanged, only the owning
partition moved); the in-process redirect qualification drives the data
handler directly (a `Summary` read from a node without a copy, a stale
epoch after an expansion), while the local socket client follows a redirect
by resending at the hinted epoch to the same node (the only node it can
reach; the node answers the same hint again when its log leads elsewhere),
which the real-binary qualification exercises ([§17](#17-the-operators-durability-request-and-the-real-binary-qualification));
a QUIC client's redirect handling is covered by the client's own tests;
there is no serving-fence check over pinned reads across a cutover
(`retired_after`).

## 15. The operator's placement view

Implemented 2026-09-09 (evidence in [09](09-implementation-status.md) under
"The operator's placement view (R6.10)"). The placement agent keeps the
partitions as its last tick observed them; `AdminCommand::Placement` on the
admin socket turns that observation into one bounded reply
(`PlacementReply`, schema 1): per partition its delegation (identifiers,
namespace, epoch, revision, any seal), its nodes (enrollment generation,
eligibility, the detector's verdict and incarnation, the last load report)
and its sessions (placement, route/membership/placement epochs, the
promised durability and what `effective_guarantee` measures against the
live nodes, every blocker, a pending plan with per-node assignment progress,
copies being retired, and since 2026-09-10 the range holders the session's
controller published with their range epoch, [25](25-parallel-materialization-and-ranges.md)
§9), truncated past 48 sessions or 128 nodes per
partition so the reply fits the admin frame; and the controller's next
actions, derived from the same facts by the pure `planned_actions` (begin,
install, add or promote a learner, cut over, activate, drain and retire,
replan) plus the partition's reshape state (a split or merge in progress or
due). Two descriptors expose it on every surface: `cluster placement` /
`cluster.placement` and `cluster plan` / `cluster.plan` (client
`AdminResult::{Placement, Plan}`, the cluster skill at version 4).

**Limits.** The view is the agent's last observation, up to one tick old;
on a node that leads no partition it is what the founder answered; there is
no session creation for a second tenant yet, so every placement view of a
fresh cluster shows the founder's session alone.

**Readiness (2026-09-10, R9.3).** `OperatorRead::Readiness` derives the
four probes of [08 §9](08-stepped-complexity-and-deployment.md) from what
the node holds: `alive` whenever it answers; `catching_up` when its root
replica and every replica it hosts follow a known leader with no seed or
import pending and none stopped, while it leads none of them;
`authoritative` when it leads the root, or a hosted session's log at a
committed prefix (`ReplicaHost::diagnostics().authoritative`);
`policy_satisfied` when every session it hosts is listed by the directory
with its desired durability achieved and nothing blocking it (a bounded
report that leaves sessions out does not satisfy). `cluster node readiness`
prints the report; `cluster node probe --check …` exits 0 when one probe
holds and 1 (`probe_failed`) otherwise, for a supervisor. Nothing here is a
quorum read, and a process that merely listens is never authoritative.

## 16. Tenants and application sessions

Implemented and gated 2026-09-09 ([09](09-implementation-status.md),
"Tenants and application sessions (R6.11)"). Until now the
only application session was the founder's own, created at bootstrap under
the founder's tenant, and every peer grant named that tenant alone, so no
second tenant could be served even if a session existed for it.

**A host's sessions reach the root (2026-09-10).** A node that does not
lead the root submits its journaled root intents — the bootstrap of a
session group it created — through the root leader's placement-control
ingress instead of its own learning replica, which could only answer
`NotLeader`; and it checks a tenant's admission against the registry its
own root replica applied when a quorum read is not its to make (admission
is monotone: tenants are admitted, never removed, so a lagging host refuses
until the admission reaches it). A session created on any host therefore
registers like the founder's, and the partition leader plans, expands and
heals it through the host that leads it (§9).

**Node peers are not scoped by tenant (2026-09-10).** A node's connection
grant used to carry the tenants admitted when it was authorized, so a
session of a tenant admitted later could not be replicated, signed for or
driven over connections opened before the admission. A node peer is
infrastructure: it replicates, hosts and drives the sessions the directory
assigns it across every tenant, and every operation its role may issue
(`Capability::Replication` only — never a participant's) is validated
against committed placement and enrollment facts, so `permits_tenant` and
`verify_request` no longer scope a `PeerRole::Node` peer by its grant's
tenants; a client's grant still scopes it.

**Tenant admission is an enrollment fact.** The enrollment registry
(schema 3; schema 2 restores with no admitted tenants) records the tenants
the cluster serves: `Change::AdmitTenant { tenant }`, prepared by the founder
authority like an invitation (`prepare_admit_tenant`), committed through the
same enrollment control path, bounded by `EnrollmentLimits.max_tenants`, and
refused as a conflict when already admitted (which an operator retry reads
as done). Every grant a node issues from a certificate names the configured
tenants and every admitted one (`QuorumEnrollmentHost::authorize`), and the
founder's local operator socket follows the registry through a watched
grant, so admitting a tenant never needs a restart and never widens a
credential beyond the tenants the cluster committed.

**An application session is created on a node.** `AdminCommand::CreateSession
{ tenant, name }` (the founder's own tenant, or an admitted one) reaches the
placement agent, which derives the session identity from the cluster, the
tenant and the name (`focal.session.created.v1`; the same name is the same
session, so a retry is exact), admits the tenant to the node's memory and
disk envelopes, opens a fresh single-voter log on the shared WAL with a
hosting session at that ledger, installs it in the fleet with its custody
scope and records it in `cluster/placement-installs` (so a restart reopens
it like any assigned copy), and answers with the ledger. Registration is the
founder session's path generalised: every hosted, authoritative, single-voter
session absent from the partition whose namespace holds it is registered by
the agent (group grant on the root, the session's own `Created` placement
record and signed fact, the partition's `CreateSession`), so a created
session gains its directory entry, its placement policy and every later
plan exactly as the founder's did.

**Surfaces.** `cluster tenants admit --tenant` / `cluster.tenants.admit`,
`cluster tenants list` / `cluster.tenants.list`, and `cluster sessions
create --tenant --name` / `cluster.sessions.create` (the reply names the
ledger, its group and node). A client reaches a created session through a
connection document naming its tenant and session under a credential whose
grant includes the tenant.

**Records.** The partition checkpoint is schema 6: `SessionDescriptor.founder`
names the node that founded a session's log alone, so a later copy replays
that exact bootstrap membership (a session created with several voters at
once, and every session of schemas 1–5, records `None`, which hosts read as
the cluster founder, the only node that founded sessions before). The
node's install record (`cluster/placement-installs`, schema 2; schema 1
converts with every copy assigned) marks created sessions, and the local
socket's grant is a watched value the network controller republishes on every
registry refresh.

**Limits.** A created session starts on the node that created it (its
placement follows the deployment policy from there, as the founder's did);
tenants are admitted, never removed, in this batch; the founder's own
enrollment role remains the only authority that admits tenants.

## 17. The operator's durability request and the real-binary qualification

Implemented and gated 2026-09-09 ([09](09-implementation-status.md), "An
operator's durability request across three real processes (R6.12)").

**A plan is an operator's committed request.** A session is registered under
the policy the founding node's settings carried, and a policy stronger than
one node can satisfy is refused at registration (`verify_placement` requires
a quorum to survive the promised failures), so the only way a laptop session
reaches three hosts is an operator asking for it once the hosts exist:
`cluster sessions plan --tenant --session --survive node|zone|region
--max-failures N` / `cluster.sessions.plan`. The node's placement agent
answers the request on its next pass over the partition holding the session,
from committed facts alone: a pending plan is reported as `pending`; an
active placement whose policy already carries the durability and still
verifies against the live registry is `satisfied`; otherwise the planner
(`propose_placement`) picks live, eligible nodes under the active policy
with the requested durability, the agent journals `SessionChange::Plan` for
the partition (`planned`), and the controller executes it unattended as it
executes every plan ([§9](#9-the-controller)). The plan's identity derives
from the session, its authority record and the requested durability
(`focal.placement.request.v1`), so a retry names the same plan, and a
request under a different durability replaces a queued one. Requests wait
in the agent's memory only; one that outlives the agent is answered again by
its exact retry. The reply names the plan's operation, its voters and its
state. R9's `deployment plan/apply` composes this request with observed
revisions and rollback bounds; it does not replace it.

**Qualification on real binaries** (`crates/focal-node/tests/placement_binary.rs`):
three `focal` processes over QUIC (founder, two invited and joined hosts),
enrollment with load through `cluster placement`, the operator's request for
one tolerated node loss (`planned`, then `pending` on retry), the founder
(session leader and controller) killed with SIGKILL while the plan is under
way, restarted, and the plan driven to activation from the committed
directory (route epoch 2, membership epoch 3, three voters, the promised
failure achieved, `satisfied` on the same request, an empty `cluster plan`);
then one host killed (a quorum read still answers through the founder's
local socket, the directory suspects the host and measures no tolerated
failure) and returned (alive again, the guarantee whole). The local socket
follows a redirect by resending at the hinted epoch to the same node
([§14](#14-the-route-cache-and-serving-fences)), which this qualification
exercises after the expansion moved the founder's route epoch.

**Re-fencing.** A hosted replica serves clients at the route it was
installed with; when the directory activates a new route, the agent
re-fences every copy it hosts (`ReplicaHost::refence`) once the session has
applied the activation and no cutover is pending: the serving fence and the
read views move to the new epoch (pinned views and saved traversals at the
old epoch are dropped, the read clock continues), and `ReplicaProgress`
reports the served route. Until then the copy answers `Unavailable` and a
current client is redirected to the leader.

**Limits.** A request names one session at a time; residency and home
regions stay those of the active policy; the request is not journaled on
the admin side (its identity makes the retry exact); the controller runs on
the founder, so the founder's loss pauses execution until it returns; on
its return it claims leadership of every session with work (§9,
2026-09-10) rather than waiting for an operator's transfer.

A founder restarted into a group that elected another voter meanwhile
reports `Ready` as a follower (2026-09-09): startup waits for its replica to
run with a known leader, its own or another's, since its local socket routes
to the leader as any node's does; before, it waited for a quorum read only a
leader answers and never reported.

## 18. Remaining R6 batches

None. R6 closes with §17; R7 follows the plan.

## 19. Draining, replacing and removing nodes

Implemented 2026-09-10 (R9.3, first step; instruction 3 of R9 in
[REMAINING §14](../REMAINING.md); doc [08](08-stepped-complexity-and-deployment.md)
§10 "Any → smaller fleet": drain workload, transfer authority, remove member,
then release storage; unsafe deprovisioning is refused).

**Eligibility is a grant fact.** A node's placement eligibility lives in its
root topology grant (`NodeEnrollment.eligible`, §1). The planner never
selects an ineligible node (`propose_placement`), `verify_placement` fails
with `StaleNode` for a placement that names one, the root controller admits
no ineligible node as a root learner, and a placement that no longer
verifies is healed by the controller under the same policy (§9). Eligibility
gates placement and nothing else: a drained node keeps every seat it holds
until the activation that drops it retires it, so a group grant may name
it (`validate_group_proof`), the grant that follows a log containing it is
prepared and installed, and its signatures still count toward the
majorities of the facts it is asked to attest (`InstalledAuthorityVerifier`,
`prepare_membership_proof`) — until 2026-09-10 each of those refused an
ineligible member, which would have left a drained voter's log unable to
prove any later fact. A seat belongs to the node identity it was granted
to, at the generation of that grant: the same key re-granted since (a drain
or an undrain) still holds every seat at or below its current generation,
so its signatures verify and its own proofs prepare; a membership epoch
counts changes to the nodes that vote, never a member's re-grant, so the
grant that follows the log after a drain keeps the log's epoch, and the
single-step rule (at most one voter changes outside a joint configuration)
counts voters by node for the same reason. A
drain is therefore one committed fact: the root re-issues the node's grant
at its next generation with `eligible: false` (`AuthorityOperation::GrantNode
{ expected_generation: Some(old) }`, the same enrollment, endpoint,
identity, principal and expiry; the registry sets the attestation), and an
undrain re-issues it eligible again. The local administrator prepares the
command through the root (`ControlRead::PrepareEligibility { node, eligible }`
→ `ControlReadResult::PreparedEligibility { generation, command }`, root
scope, excluded from peer read-only ingress like revocation) and commits it
as an exact journaled request (`AdminCommand::Authority`, the only authority
operation the administrator may submit: a `GrantNode` at the next generation
with a zero attestation). A node whose grant already states the requested
eligibility commits nothing (`changed: false`). The founder holds the
enrollment authority and the root's bootstrap identity and is never
drained (`PrepareEligibility` for it is refused before it reaches the root).
A drain does not need the drained node's credential to be live: a
withdrawal (a grant that only turns `eligible` off and restates the
committed identity, region, zone, endpoint, authority epoch, principal and
expiry), the partition's enrollment of it, and a group change that carries
the ineligible seat out, tolerate a credential the registry no longer
authorizes (`UnverifiedAuthority`, `Expired`); a revoked or expired node
([runbooks/expired-credentials](../runbooks/expired-credentials.md)) is
drained, healed around and removed by the same commands as a healthy one.
Anything that changes what a node publishes, and every live seat, still
verifies against the live credential.

**The partition learns every grant.** A node used to enroll only its own
eligible grant into the partition; a drained grant would never have reached
it. The placement agent now brings the partition's node records up to the
installed authority: its own grant, and — where this node leads the
partition — every other node's, one enrollment per tick
(`enroll_nodes`, replacing `enroll_self`), so a re-issued grant reaches the
partition whether or not its node is up. The partition verifies each
enrollment against the installed authority as before (`verify_enrollment`).

**What follows a drain.** Load reports and readiness statements the drained
node signs under its old generation are stale (`StaleNode`) and it receives
no new assignment; every active placement that named it stops verifying,
the controller plans again under the active policy with a heal operation
(§9), the new placement is prepared, cut over and activated, and the
copies the node held are drained, removed from the session logs and retired
(§4). A placement the remaining nodes cannot satisfy records one
`NoPlacement` refusal and keeps the node's copies: the operator adds capacity
or undrains. The root group's membership is untouched by a drain.

**Removal.** `cluster nodes remove --node N` (`cluster.nodes.remove`) is
refused while the node's grant is eligible (`not_drained`, exit 5) or while
any session in the placement view still names it as a voter, materializer,
content copy, retiring copy or pending assignment (`node_holding`, exit 5),
and for a node the directory does not know (`unknown_node`, exit 4). It then
removes the node from the root group when the configuration contains it
(`MembershipChange::Remove`, the ordinary journaled `a1:` request) and
revokes the invitation that enrolled it (found by the credential's node id
across the invitation pages; the ordinary revocation request, which also
revokes the certificate a renewal replaced). Each step is exact, so a
repeated command after a crash resumes: an already removed member and an
already revoked invitation read as done. The revoked node can no longer
present its credential on any path; its data directory is the operator's to
release.

**Replacement.** `cluster nodes replace --node N --with M`
(`cluster.nodes.replace`) drains `N` once `M` is enrolled, alive, eligible
and reporting load in the placement view (`node_not_ready`, exit 5
otherwise); the healed placements are planned among every eligible node, so
`M` is a candidate, not a promise.

**The operator's view.** `cluster placement` shows each node's `generation`
and `eligible` flag (§15) and each session's `retiring` copies; the drain is
complete for a session when the node appears in none of its lists and its
guarantee is achieved again. `cluster nodes list` still shows contact
announcements only.

**Limits.** A drained node keeps its root-group vote until removed, so a
drain never weakens root quorum by itself; removing several nodes in
sequence is the operator's to pace against the root's quorum. Draining the
node that leads a session's log works through that leader until its own
removal commits and the log elects another (§9); the controller follows the
redirect. The credential revoked by removal is not re-issuable: rejoining is
a new invitation and a new node identity.

## 20. Repairing a session's custody

`cluster repair [--tenant T] [--session S] [--after A] [--limit N]`
(`AdminCommand::Repair`, `cluster.repair`) answers instruction 3 of R9 for
custody: forward repair of the copies a placement already requires, never a
new promise. The replica exports its committed prefix
(`checkpoint_evidence`, the same export a backup takes) and the evidence
coordinator walks the artifact projection it holds as a trusted node job
under the session's current placement (`JobKind::Repair`; the export's
ledger, route epoch, placement epoch and node must be the placement's). For
every artifact with content, in artifact order from `after` and at most
`limit` objects: this node verifies the object through its own store
(`CustodyRequest::Verify`, the manifest and every chunk) and counts it
verified; an object it lacks or that fails its hash is pulled from another
required copy — the content copies first, then the voters, each once — with
the copy's manifest, chunk by verified chunk, resuming at the first chunk
this node lacks or holds corrupt (a transfer opened over a chunk that fails
its hash now installs verified bytes over it), and counts as repaired; an
object no copy answers with is unrecoverable, listed (bounded to 64,
counted exactly) and turns `restore_required` on. This node then records
its own receipt when it is a required copy, and asks every other required
copy to verify the object again — receipt or not, since a receipt records
an answer once given, not the bytes still held — recording the `Durable`
answer as its receipt or, failing that, giving it the object (`push`) and
counting it pushed. Nothing is inferred from a copy that
cannot answer; nothing is written that was not verified against the object
identity the prefix names.

The walk is bounded twice: by `limit` (256 by default, 4096 at most) and by
the export's lease (30 s), after which it stops where it is; `complete`
says the projection was walked to its end, and `next_after` names the
artifact a following call resumes after. Repair is idempotent — every
object it touches converges to verified — and initiates no placement
change: copies the placement does not require are neither made nor
removed, and the archive agent's bundles keep their own custody loop (26
§4). A session whose objects are unrecoverable here is restored from a
verified backup (26 §6); repair cannot manufacture proof (08 §10).

**A fresh copy's objects (2026-09-10).** A voter or content copy the
directory adds to a session whose committed records already name content
objects does not hold them: its checkpoint install, its legacy translation
or its record replay reads them through a recording custody reader
(`RecordingReader`, wrapping the node's content reader), which notes every
object the local store lacked (`NotFound`, bounded to 64, no repeats). A
delivery a retryable custody refusal retains now names those objects
(`PendingCustody`; `Session::pending_custody`, the engine's for a replay,
the session's own for a snapshot install or translation whose engine was
not adopted), reported as `custody_pending`/`custody_objects_missing` in
the replica's progress, diagnostics and readiness (a session with objects
still being pulled is catching up). The host's support driver
(`managed_support::drive`) pulls each named object from a peer of the
ledger's custody policy — content copies and voters alike — chunk by
verified chunk under the same identity (`pull_object`, the transfer path
of §5 and 26 §1), then brings the replica's next poll forward
(`custody_pulled`); the retained delivery retries at every poll, as any
retryable custody refusal does, and applies once the objects are local. An
object no peer supplies leaves the delivery retained and the copy short of
readiness, which the placement view reports. Only an activated native
engine projects rows into the evidence export; a replica that can host one
but still serves legacy history exports the graph's projection. Custody reads — a seed, an object's manifest, the transfer
that describes it, its chunks, a verification and a cancel — are admitted
from the nodes of the installed placement *and* of an announced pending
placement at that placement's route (`authorize_read`, `check_read_scope`),
since a copy being prepared reads before the placement activates; writes
(a chunk received, a seal) stay with the installed placement's nodes. A
chunk a transfer receives over a file that holds other bytes under the same
content-addressed name replaces it (`install_transferred_chunk`): the file
was corrupt and the bytes are verified; every other install path still
refuses a differing existing file.

## 21. The upgrade fence

Upgrades roll one binary at a time and activate incompatible behaviour only
behind a committed fence ([08](08-stepped-complexity-and-deployment.md)
§10). Each binary implements a capability level (`upgrade::CAPABILITY_LEVEL`,
1 for this release) and announces it — the compiled level, or a lower one
the operator sets through `FOCAL_CAPABILITY_LEVEL` for a staged rollout or
a rehearsal; the variable never raises it — in every load report
(`NodeLoad::capability`; the frozen V1 row codec restores it as zero,
unknown). The fence itself is a fact of the enrollment registry
(`UpgradeFence { level, activated_at, revision }`, registry schema 4; a
schema-3 checkpoint restores with no fence), raised only by the founder
authority through `Change::ActivateFence { level }`: a fence only rises
(`prepare_activate_fence` refuses zero and lower levels; the same level is
a conflict an operator's retry reads as done).

`cluster upgrade status` (`AdminCommand::UpgradeStatus`,
`cluster.upgrade.status`, any node) reads the registry — through the root
quorum, or the node's own applied copy when a quorum read is not its to
make — and the directory's node records, and reports the fence, this
binary's compiled and announced levels, every listed node's reported level
(the highest across partitions; zero until it reports) and `activatable`,
the least reported level. `cluster upgrade activate --fence LEVEL`
(`AdminCommand::ActivateFence`, `cluster.upgrade.activate`, founder only)
refuses by name while any listed node reports less than `LEVEL` or none
(`members_behind`, listing them), refuses a lower level (`invalid_input`),
reads a fence already at the level as done, and otherwise commits the
activation through the founder's enrollment host and answers with the fence
as committed. The check is the founder's quorum-consistent view; the node
side closes the window it leaves.

A node checks the fence against its announced level every time its
network controller observes the root (`refresh`) and refuses to serve when
the fence is above it (`ControllerError::Fenced`, exit `upgrade_fenced`):
a binary rolled back past the fence stops as soon as its root replica
applies the activation, and does not start again while its applied registry
carries it. Behaviour gated on a level opens when `upgrade::opened(fence,
level)` holds; this release gates nothing yet, so the fence's first work is
the rollback refusal the release qualification (R10) needs. A rollback past
the fence is a restore from a verified backup (26 §6), never a downgrade.

## 22. Topology facts and the residency fence

A node's failure domains are facts its operator declares
(`topology.region`, `topology.zone` in its local configuration, [08](08-stepped-complexity-and-deployment.md)
§2, §6), never guessed from addresses. The node announces them with its
contact (`Operation::NodeContact { region, zone }`, labels of at most 64
bytes; on the wire a zone needs its region, and a node whose configuration
declares a zone without a region announces no zone, since a zone alone
names no failure domain; `ContactRecord` keeps them, contact
checkpoint schema 2, control checkpoint schema 5 with the earlier shapes
decoded), and the root leader turns them into directory identities by
derivation (`topology::region_id(label)`,
`topology::zone_id(region, zone)`; the same label is the same identity on
every node): it registers a region the root does not know yet
(`RootOperation::RegisterRegion` under the controller's evidence,
`authority_epoch` 1) and grants the node with the region and zone
identities and the region's authority epoch (`NodeEnrollment`). A node
whose announced topology changes is re-granted at its next generation, as
a rotated key is (§11), and the partition learns it as it learns a drain
(§19). The founder also registers every region its policy names
(`placement.residency`, `placement.home_regions`) ahead of any node
reporting from there, so a residency can name a region before a node runs
in it and the founder's own session registers its policy at once. The
placement view shows every node's labels and every session's residency and
ordering homes as labels ([15](#15-the-operators-placement-view)).

The residency fence executes what the planner decides. The planner keeps
plans inside `placement.residency` (§3, [placement.rs]); the fence
(`placement_executor::ResidencyFence { residency, regions }`) is installed
with every session's custody scope on every copy (the session's residency
and the region every directory-listed node reported) and refuses, before
any byte moves, every transfer the planner did not decide: a sealed
artifact replicated to a copy, a repair's pull or push (§20), a custody
obligation's ask, and an operator's range move (`cluster replicas ranges
move --node`, `outside_residency`, exit 5, checked against the placement
view before the agent refuses it again). A node of unknown region lies
outside every boundary. Seeds and objects a copy pulls come from the
placement's own peers, which the planner placed inside the boundary; a
restored session is founded under the restoring node's own configured
policy and refuses a founder outside it at registration. Backups are files
the operator writes where the operator chooses.

## 23. Metrics

The node's metrics are one bounded snapshot the service samples on a fixed
cadence (`metrics::SAMPLE_INTERVAL`, five seconds) from what it already
owns — its memory budget, the content host's volume envelope and staged
uploads, the WAL writer's counters, the fleet's and root replica's
progress, every hosted replica's diagnostics (indices, apply lag, sequence,
pending proposals, log kept beyond the checkpoint, retention floor and
cursor lag, pending seeds and objects), the peer pool's delivery counters,
the failure detector's view (§12), the credential's expiry (§11), the
placement agent's intents and admission (§7, §10), the directory's route
and placement epochs and achieved durability per session (§15), and the
upgrade fence (§21) — and publishes through a `watch` the admin socket
reads (`OperatorRead::Metrics` → `AdminResult::Metrics { text }`, CLI
`cluster node metrics` printing the text as it is, MCP
`cluster.node.metrics`). Rendering is Prometheus text exposition (version
0.0.4) with `# HELP`/`# TYPE` per series and fixed labels on every sample
(`node`, `cluster`; `focal_node_info` carries `role`, `region`, `zone` and
`capability`); label values are escaped. Nothing is sampled on a caller's
behalf: a read renders the latest snapshot, so a scraper's cadence never
drives owner work.

`node.metrics_listen` (local configuration, loopback only, [08](08-stepped-complexity-and-deployment.md)
§2) binds a `TcpListener` at open and serves `GET /metrics` over HTTP/1.0
(`metrics::serve_loopback`): one connection at a time, a request bounded to
4 KiB and two seconds, `Connection: close`, `404` for another path and
`405` for another method, no HTTP crate. It is read-only and
unauthenticated by construction, which is why it never leaves the loopback
interface; the admin socket stays the authenticated path. Sessions beyond
`metrics::MAX_SESSIONS` are counted as truncated, never silently dropped.

## 24. Reachability and packaging

A node's identity is its enrolled key and certificate, never its address.
Packaged hosts move: a rescheduled pod keeps its name and loses its
address, a rebuilt VM keeps its disk and gains another interface. So
reachability is restated, not pinned. `start --advertise` (or the file's
`node.advertise`) that resolves to other addresses than the saved ones is
adopted at that start (`NetworkState::install` rewrites `NETWORK`, schema
2, for the same node, sponsor and genesis; a different identity is still
refused), and the controller announces the new contact as it announces a
renewed certificate (§11) or a changed topology (§22). A node that moves
again before it applied its previous contact announces from a stale
generation, which the root refuses (`CompareFailed`); since nothing
reaches the node until its new address is committed, the controller asks
the root that refused it for the current contact table
(`ControlRead::Contacts` over the node's own certificate) and repeats the
announcement once from that generation (`ContactOutcome::Stale`). A live
contact is never displaced: before the root's data service forwards an
announcement that would move a node, it asks the committed address itself
(`LivenessHandle::confirm` → `PeerConnectionPool::probe_at`: one direct
probe on a connection opened for that address and closed after it, never
the installed route and never a re-resolved name, bounded by the probe
timeout cap) and refuses the move (`CompareFailed`) while anything answers
there as the node. The failure detector's own verdict is not the test: a
moved node refutes its suspicion from its new address, while a clone is a
second answer at the old one. The announcement never waits on that probe:
the driver answers at once with a verdict it reached within the last two
probe caps or, starting the probe when none is in flight, with "not yet"
(`DataService::contact_admission`: `Proceed`, `Clone`, `Unknown`), and
an unknown verdict is answered `Unavailable`, which the mover treats as
any unavailable root (another peer, the next tick). A root that waited
on the probe instead answered after the mover's own deadline, and a mover
that had moved before applying its previous contact could then never
learn it was stale (its own replica only catches up once the root reaches
its new address). So a second process started from a copy of a node's
disk cannot take the node's place while the node serves, and a moved node
is admitted as soon as its old address stops answering, a probe timeout
and a tick after its announcement (`runbooks/stale-clone.md`). The driver
holds at most `MAX_CONFIRMATIONS` confirmations; beyond that, and when a
probe cannot be sent, the answer is unknown and the mover announces again
a moment later. An operator who
gives a name (`host:port`, a DNS host and a nonzero port, at most 259
bytes) rather than an address has it carried with the contact
(`Operation::NodeContact { endpoint }`, `ContactRecord.endpoint`, contact
checkpoint schema 3, control checkpoint schema 6 with every earlier shape
decoded) and shown by `cluster placement` (`advertise`, `endpoint` per
node). The peer pool dials the announced address and, when it stops
answering, re-resolves the name once within the same deadline and tries
at most four fresh addresses (`PeerEndpoint.name`, `connect_by_name`);
the certificate it accepts never changes with the address. The founder's
invitations name the founder as its operator did, so an invitation
outlives the founder's address; the sponsor's pinned endpoint is resolved
at each use and its certificate pin still decides trust. A fleet speaks
one address family: a node's transport is bound in the family of the
address it advertises, so a name must resolve to the family the fleet
uses (a name that resolves to `::1` on one host and `127.0.0.1` on
another leaves them unable to dial each other).

`start --invite-file FILE` enrolls from the invitation when the data
directory holds no identity yet, then starts; an initialized directory
starts and ignores the file. One command therefore serves a supervised
host and a pod alike, with no init step that would need a shell.
`prepare-volume --owner UID:GID` creates the data directory for the
node's user and exits: the privileged step a packaged volume needs,
performed by the same image and nothing else. `cluster invite --output -`
writes the invitation to the caller's pipe, so a founder that has no
readable path can still issue one through `kubectl exec`. An invitation
file is read through links (a mounted secret is a link into its volume)
and must be a regular file nobody but its owner may write and nobody
outside its group may read (mode `0600` as `cluster invite` writes it, or
`0440` as a secret mounted with `defaultMode: 288` under the pod's
`fsGroup`); the journals and keys a node writes itself stay at exactly
`0600`.

`deployment render` turns the requested configuration into packaging
without touching a cluster or a node (`deployment/render`; every file is
the same bytes for the same inputs, an existing file is never
overwritten, and every infrastructure fact the render lacks is named as
`missing` rather than guessed). `render systemd` writes a hardened unit
and the configuration it reads: the node under its own user with a state
directory of mode 0700, SIGTERM and a stop timeout above the node's 30 s
cleanup bound, restart on failure, no capabilities. `render kubernetes`
writes a headless Service with not-ready addresses published (names
resolve before readiness), one founder StatefulSet and, for node
survival, one host set spread across hosts or, for zone survival, one
host set per named zone pinned by node affinity (the founder in the first
zone; `2f+1` zones are required and otherwise reported as missing), each
pod with its own volume claim and its own enrollment, a ConfigMap with one
configuration per set, disruption budgets (the founder never voluntarily
disrupted, hosts at most `max_failures` at once), probes that ask the node
(`cluster node probe --check alive` for startup, liveness and readiness;
`authoritative` and `policy` are inspection, so a healthy node is not
restarted for a missing quorum), an init step that gives the volume to
the node's user, and an invitation script that issues one invitation per
host pod from the running founder and installs them as the secret the
hosts mount. Region survival is refused by name: a cluster spans zones,
and regions are one deployment each joined by invitation (08 §7). The
requests and volumes are a stated, unqualified bootstrap allocation. The
checked-in `deploy/kubernetes` and `deploy/systemd` are the renderer's
output for `deploy/config` and a test fails on drift; `deploy/helm/focal`
templates the same objects; `deploy/container/Dockerfile` builds the
static musl binary with the release's pinned toolchain image into an
empty image with an unprivileged user and no shell.
