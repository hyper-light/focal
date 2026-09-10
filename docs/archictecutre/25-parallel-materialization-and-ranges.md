# Parallel materialization, ranges and movement

Status: R7 design and record. [04](04-storage-and-distribution.md) §9 and §11
are the contract this document implements; [05](05-implementation-plan.md)
§12–§13 name the packages; decisions are logged in
[07](07-decisions-and-traceability.md); evidence for each closed batch is in
[09](09-implementation-status.md).

## 1. What runs in parallel

The leader's admission stays serial: `NativeOwner::prepare*` is the sequencer
and the oracle of every dependency, scope and validation effect, and its
output is a committed record carrying the exact write set of one mutation
([22](22-native-record-format.md)). What can run in parallel without a second
oracle is everything that consumes committed records: followers, learners,
recovery and restore decode rows, verify custody and validate each record
against the rows it reads. Those reads are the only speculation, and every
one of them is recorded and checked. Nothing here executes a command, reruns
a validator, reads a clock or invents an order: the log has already decided
all of it, so this is deterministic replay coordination, and the serial
replay stays the reference the parallel path must match byte for byte.

## 2. The materializer (R7.1, implemented 2026-09-09)

`focal_core::native::record_codec::materialize` materializes one batch of
consecutive committed records at one base prefix.

**Stage and install.** `replay::prepare` is split. `stage` decodes and
validates a record against a `BaseRows` view (the published Core, or a
task's view of the Core plus the records staged before it) and produces a
`StagedRecord`: the complete change set, the captured write set, their
funding and the recovery work left for installation. `install` builds the
record's pages against the Core's published root and `publish_native`
swaps them in. Staging is where the work is (row construction, custody
verification, model validation); installation is a bounded page copy per
touched leaf. Installation and publication are serial and in order, so the
published prefix is always contiguous.

**Footprints and edges.** A record's write keys are read from its header
rows before any decoding. Each key maps to the object it belongs to
(`Affinity`: the claim for claim-owned families, cycles, evaluations,
results and timers; the artifact, validation, testament or monitor for
theirs), and records touching the same object, or the same key, are ordered
by an edge. Entry-local rows (`Meta`, `Event`, `Outcome`, `CreationResult`,
`ByCreated`) and identity rows carry no affinity: an identity conflict is the
same key, which the key edges order. The meta row every record writes is
excluded from key edges and handled apart (below). Edge storage is bounded
(`MaterializerLimits.max_edges`); a batch beyond it runs serially.

**Waves.** Records whose predecessors are complete are staged together on
scoped worker threads (`max_workers`, each with a fixed stack), lowest index
first; a wave of one runs inline. A task's view answers each base read from
the latest *completed* earlier record that wrote the key, else the Core, and
records `(key, observed writer)`. Completed records are shared read-only
across the wave; the `Core<NativeState>` is `Sync`, and each task owns its
trace. The previous meta row is decoded for every record ahead of the waves
(`stage_meta`: a fixed row that reads nothing), so a stage never waits on its
predecessor for the one row every record writes.

**The barrier.** When a wave ends, every read of every finished record is
compared with the complete set of writers in the batch: the observed writer
must be the latest earlier writer of that key (or none). A stale read, a
trace that overflowed `max_trace`, or a stage that refused after a stale
read is a violation: that record and every record after it are discarded,
and the rest of the batch is staged serially in order, each over all its
predecessors. A refusal without a stale read is the record's own (memory,
custody not yet local, corruption) and stops the batch there: the records
before it are installed, the delivery resumes at it.

**Publication.** Outcomes are returned for the applied prefix in order;
`NativeCommit`s, the recording range and term advance per record exactly as
the serial path advances them. Memory: every staged record holds its rows
under the session budget; a batch never holds more than `max_batch` records,
and a discarded suffix releases its rows before the serial pass.

**Where it runs.** A follower's delivery loop (`native_session_apply`)
batches a run of consecutive native records up to the next membership entry
(`record_run`) whenever the session holds no unresolved candidates of its own
and `max_workers > 1`; otherwise the per-record path is unchanged. The
leader applies its own candidates by matching the committed head, never by
materializing. Recovery after a restart replays the log through the same
loop, so a restarted follower materializes in waves too.

**Bounds and defaults.** `MaterializerLimits { max_workers 1, max_batch 64,
worker_stack_bytes 2 MiB, max_trace 8192, max_edges 65536 }`; the node hosts
sessions with `max_workers = available_parallelism().clamp(1, 4)`; the
standalone session and every test default to one worker, the serial
reference. `assume_independent` plans no edges at all and exists for the
omission campaign only.

**Evidence** (`native_session_cluster_tests`): a follower with four workers
reaches the same `native_state_digest` (the checkpoint encoding's content
hash under a fixed range identity, `checkpoint::rows_digest`) as the leader
and a one-worker follower across six independent creations (staged in
parallel waves, no violation), posts and receipts that depend on them, and a
restart that replays the whole log; a follower planning no edges with eight
workers stages a claim's post beside its creation, the barrier reports the
stale read, the suffix is redone serially, and the rows still match.
`MaterializerStats` counts batches, records, parallel batches, waves,
violations and serial fallbacks per session.

**Measured** (P10.7; `materializer_throughput_at_one_and_four_workers`,
`--ignored`, release build, macOS arm64, 2026-09-09; the run's JSON lands in
`target/measurements/`). Eight records of one delivery, a one-worker
follower replaying them one at a time against a four-worker follower
materializing them in two waves:

| Shape | One worker (serial path) | Four workers (two waves) |
|---|---|---|
| Independent creations | 910 µs | 620 µs |
| Posts and receipts of four claims | 928 µs | 913 µs |

Records this small are dominated by thread start and the page copy that
stays serial, so four workers give about 1.5× on independent records and
parity on a chained shape; the benefit grows with custody verification and
larger records, which the capacity envelope
([06](06-verification-and-operations.md)) measures on real content. The
stats a session keeps (`MaterializerStats`, with `serial_records` and
`serial_micros` for the per-record path) are what the measurement reads.

**Limits.** Affinity captures reads within an object; a record that reads
another object written earlier in the same batch (a child claim's parent, a
response's artifact from a different claim) is caught by the barrier rather
than planned, and costs a serial pass for its suffix; the worker pool is
scoped per batch rather than persistent, which keeps `&Core` borrowed
without an `Arc` and bounds thread creation by batches, not rows; the
differential clusters run under a wider engine budget than the ledger's
test limits give one session, because those limits fund every completion
obligation's future record buffers generously (a standard-limits session
admits forty posted claims; the test limits admitted about fourteen).

## 3. The storage layout order (R7.2, first step, 2026-09-09)

Every ordered structure over native keys, the row store, the recorded
mutations (`FCMUTATE` version 5), the root checkpoints (`FCNROOTS` version 5)
and every prefix scan, uses one order, `native::layout`: **affinity, then
family, then the key's fields** in declaration order with every nested enum
tagged. The affinity is the object a row belongs to (the claim for
claim-owned families, cycles, evaluations, results, deadlines and their
timers; the artifact, validation, testament, receipt or monitor for theirs;
a request's principal for its outcome), a bucket for an index family (the
status, action or verdict code; the issuer, subject, producer or evaluator;
the scope or identity hash; the relation's target claim; the created kind)
and one control affinity for the meta row, events and due timers. Identity
rows sit under their hash. The end sentinel is greatest.

So one object's rows are one contiguous span, an index family's rows for one
discriminator are contiguous, due timers are in time order, and a key span
is a unit a range can hold and move. What this order gives up is the
contiguity of one family across objects: the claim rows of every claim used
to sit together, and the identity-order listings (`claims`, `artifacts`,
`validations`, an unrestricted `evaluations` listing) walked them. They
walk the identity index instead, `ByObject(family, object)`
([22 §7](22-native-record-format.md), one unit row per claim, artifact and
declaration under its family's bucket, written with the object and
validated like every index row), and an unrestricted evaluation listing
walks that index and each claim's contiguous evaluation span. Receipts,
standalone fixed rows read by identity, sit together as one table so their
identity-order listing stays one scan. The order is total and agrees with
equality (`layout::tests` checks every pair and triple of a corpus covering
all 48 families); it changes no row bytes, only their order, and the native
decoder identity changes with the version numbers. Comparisons build the
order key of both sides (an affinity, a family and up to five identities and
five scalars), which costs a few dozen bytes per comparison; the store's
page classifier is unchanged.

Ranges (several stores over key spans of this order) are §4; chunked seeds,
the movement state machine and the balancer follow; each is recorded here as
it closes.

## 4. Range groups (R7.2, second step, 2026-09-09)

A native state's rows live in a **range group**
([ranges.rs](../../crates/focal-core/src/native/ranges.rs)): stores in key
order, each holding one span of the layout order, together holding every key
exactly once at one prefix. A *boundary* is an affinity (the first component
of the order: a claim's identity bytes, an index bucket, the control
affinity), so a boundary never divides an object's rows; a *member* is the
store from one boundary to the next, named by a durable `RangeId`. The
**layout** is the list of members with their starting affinities, the first
unbounded below; a group of one member is the default and costs nothing a
single store did not.

**One prefix, one publication.** Every write is planned as one plan per
member: the sorted change vector is divided at the boundaries (a split of the
same vector per touched member, nothing copied), each member plans its part
against its own root, and the fragments are built in member order and
published together or not at all: each fragment is checked against its member
before any root moves, and a member the write does not touch still advances
by an empty fragment, so the group has exactly one prefix. A pending
candidate therefore holds one fragment per member; the layout changes only
while nothing is pending and no delivery is under way. A read pins every
member at that prefix (one lease per member, released together) and a point
projection runs from the member holding its start into the next members
below its end.

**One owner, one clock.** The members share the owner identity every write
envelope checks and the clock that expires leases, so a completion promise
is derived once for the group: the shared envelope prices a write divided
over up to `NativeLimits.max_ranges` members (a root per member, a vector per
touched member and at most one slot per moved change, the further plans and
fragments the group keeps beside its stores) and the fragments' plans are
checked against it as a set, sums against the whole write's limits, so a
later split or merge never invalidates a funded report.

**Split and merge share pages.** A split at an affinity divides one member
into two stores that share every page wholly on one side and copy only the
page holding the boundary, once per side
([range_split.rs](../../crates/focal-memory/src/range_split.rs), a page-level
bisection under a monotone predicate); a merge joins two adjacent members
under the left one's identity sharing every page. Both build their
directories in one pass (one node per group of pages) rather than one path
copy per page, and both are refused under read leases, which pin the
undivided members.

**Checkpoints carry the layout.** `FCNROOTS` version 6 records the layout
after the row count (each member's identity and, but for the first, its
starting affinity; [22 §2](22-native-record-format.md)); the rows stay one
ordered stream. Restoration hydrates one store, validates the whole as
before, then divides it per the recorded layout: members keep their durable
identities, the producer identity (what records carry) is the caller's fresh
incarnation, so a follower that installs a leader's checkpoint adopts the
leader's layout and a replica restarting from its own checkpoint restores its
own. The rows digest of §2 frames a canonical one-member layout, so replicas
laid out differently still agree exactly when their rows agree. An import
image names one member under the import identity so every replica's image is
byte-identical.

**The layout is a session decision (R7.3, first step).** A layout change
is one committed record, `FOCALRG1`
([native_session_range.rs](../../crates/focal-ledger/src/native_session_range.rs)):
the ledger, the layout *epoch* it applies to and one operation, a split at
an affinity naming the new member or a merge naming the member whose next
boundary goes. Only the authority proposes it (`NativeSession::propose_layout`),
after checking it against the committed layout, with no candidate pending
(every candidate holds one fragment per member of the layout it was
prepared against) and no change in flight; while the record is in flight
native admission answers `LayoutChanging` (retryable), and a term change
lets an uncommitted record go. Every replica applies the record between
native records: a record whose epoch has passed is inert on every replica
alike, an applicable one is the same split or merge on each replica's own
group (pages shared, nothing shipped), and the applied layout carries the
next epoch. An authority that finds candidates pending when a layout record
applies (it lost and regained authority around one) ends them as
`SuffixEvidence::LayoutChanged`: their records, if they still commit, are
replayed from bytes and an exact retry finds them. The origin member is
named from the genesis (`origin_member`), so every replica names the one
member it starts with alike; a replica whose member bound cannot hold a
committed layout is fail-closed rather than divergent. Leases taken before
a change expire with the members they pinned (a read resyncs); members the
change left alone give up theirs on release. Checkpoints carry the epoch
with the layout, so a lagging follower that installs one and then replays
a later record lands on the same layout as everyone else
(`committed_layout_changes_apply_on_every_replica_and_fence_proposals`).
Moving a member between nodes under a fence and choosing boundaries from
measured load are §5 and §6 (R7.3, R7.4); `Core::split_native_range` and
`merge_native_range` remain as the engine's local primitives the record
applies through. The map that will name ranges to the directory is the
memory crate's generic `RangeMap<K, M>`
([range_map.rs](../../crates/focal-memory/src/range_map.rs)): spans,
gap-free validation and one contiguous replacement (move, split, merge)
with generation rules, keyed by the store's key and carrying placement
facts in `M`; the dormant `focal-ranges` crate now builds on it.

**Limits recorded.** `NativeLimits.max_ranges` (64) bounds a group and sizes
every envelope; a checkpoint layout is parsed up to `MAX_LAYOUT_MEMBERS`
(1,024) and refused beyond the owner's bound. A wider group pays a root per
member per publication and one lease per member per read. Boundaries are
affinities only: an index bucket or a control affinity can start a member,
but a member cannot begin inside an object.

## 5. Chunked checkpoint seeds (R7.3, second step, 2026-09-09)

The consensus checkpoint path keeps its explicit complete-buffer limit (the
Raft snapshot payload is one funded buffer, 8 MiB). A native Core root is
the group's every row, so a populated Session's root crosses that limit
long before the ledger is large. The session envelope therefore has two
forms ([22](22-native-record-format.md) §2, `FCNSESS1` version 4, one
form byte between the membership configuration and the Core byte count):

- **Inline** (form 0): the Core root follows the header, exactly as before,
  when it fits `Limits.inline_bytes` (4 MiB by default; the node reads
  `FOCAL_SEED_INLINE_BYTES` to move the bound for qualification).
- **Seeded** (form 1): the envelope carries the root's byte count and hash
  and a **chunk table** (count, then per chunk a BLAKE3 hash and a length)
  instead of the root. Every chunk but the last is exactly `SEED_CHUNK_BYTES`
  (1 MiB); a table whose count disagrees with the byte count, exceeds
  `Limits.assembled_bytes` (256 MiB, the assembled root's bound) or names
  more chunks than that bound allows is refused at the frame.

**Sealing.** `EncodingPlan::prepare` decides the form from the root's quoted
size; `encode_in_seeded(budget, &mut SeedStore)` streams the root through
one chunk buffer (`write_with`), sealing each full buffer into the seed
store before the next fills, then writes the manifest frame that names the
sealed chunks. Nothing holds the whole root in memory: the plan's charge is
one chunk buffer, the table and the manifest. The inline paths
(`write_into`, `encode_in`, `write_with`) refuse a seeded plan with
`Error::Seeded`, so a caller cannot produce a manifest whose chunks were
never sealed. The seed store ([seeds.rs](../../crates/focal-evidence/src/seeds.rs))
keeps `<hash>.seed` files under `seeds/<session>/` beside the node's data,
written through a temporary file, fsynced and renamed, charged to the node's
`DiskBudget` as checkpoint bytes; a chunk is verified against its hash when
it is installed and again when it is read, and removed only on purpose. The
store is content-addressed, so a root that repeats a chunk (an unchanged
first MiB) seals it once.

**Installing.** A replica that receives a Raft snapshot describes the
envelope (`Checkpoint::describe`) before it touches state: an inline frame
(`None`) installs as before; a seeded frame yields a `SeedManifest` whose
chunks are looked up in the replica's own seed store. If every chunk is local, the
root is assembled into one funded buffer, verified against the recorded
byte count and hash, and inspected exactly as an inline root
(`inspect_seeded`). Otherwise the delivery is **retained**: the engine
records `PendingSeed {index, term, missing}` and answers the install with a
retryable `CustodyPending`; the Raft snapshot stays in the log's custody,
nothing partial becomes authoritative, and the session keeps serving what
it had. The host (`ReplicaProgress.seed_pending`, `ReplicaHost::{pending_seed_chunks,
install_seed_chunk}`) pulls each missing chunk from a peer of the ledger's
placement with the custody request `SeedChunk {hash, max_bytes}`
(`CustodyReply::SeedChunk {hash, bytes}`), verifies the bytes against the
hash on the way in, seals them into its own store and retries the install
at once. A chunk that fails its hash is refused and never written. Once the
last chunk lands the retained delivery completes and the replica continues
with the entries after the snapshot. A replica that restarts on its own
seeded checkpoint assembles it from its own store the same way. While the
chunks are missing the retained delivery is not resumed at every poll
(`Session::seed_waiting`): the hosting Session keeps the missing list itself
(the engine it would have become is not adopted until the root assembles),
answers `Retry` without re-describing the manifest until a chunk lands, and
the fleet owner does not wake for it. A peer asked for a seed it does not
hold answers a definite refusal so the puller moves to the next peer.
Replica diagnostics report `seed_chunks_missing` and `delivery_retained`
([cluster-admin.md](../cluster-admin.md)).

**Who may read a seed.** Seeds are immutable and content-addressed but they
are the ledger's whole state, so the content host serves `SeedChunk` only
to an authenticated node of the ledger's **installed** custody placement at
its route, or of a placement the directory is **preparing** for the ledger:
the placement agent of every node in the active placement announces the
pending plan's scope (its next route and placement epoch) and peers
(`voters ∪ content copies`) to its content host
(`ContentHost::announce_pending`) while a plan is pending, and withdraws it
once none is. The announcement authorizes seed reads and nothing else; the
installed policy still moves only at activation, so clients at the old
route are unaffected. A fresh copy pulls with its own installed scope, the
plan's target, which is exactly what the announcement names
(`seed_chunks_are_served_to_installed_peers_and_announced_pending_peers_only`).

**Checkpointing on request.** Hosted replicas checkpoint on an operator's
explicit request, `focal cluster replicas checkpoint [--session ID]`
([cluster-admin.md](../cluster-admin.md)): the replica's worker drains what
Raft owns, encodes and installs the envelope synchronously and compacts the
log behind it; a pending proposal refuses the request with `Capacity` until
it commits. Policy-driven checkpoints and log trimming belong to retention
(R8). The real-binary qualification
(`a_seeded_native_checkpoint_carries_the_founder_session_to_new_hosts` in
[placement_binary.rs](../../crates/focal-node/tests/placement_binary.rs))
activates the founder's session natively with a 64-byte inline bound,
checkpoints it (the root is sealed as seeds), kills and restarts the
founder (it restores from its own seeds), joins two hosts and asks for one
tolerated node loss: the copies can only catch up through the seeded
snapshot, so activation at route epoch 2 with the promised failure achieved
proves every chunk was pulled under the pending announcement and the root
assembled on each host, which then holds every seed the founder sealed.
The ledger suites cover a root that needs one chunk and one that needs
several (`a_checkpoint_beyond_one_seed_chunk_installs_only_when_every_chunk_is_local`:
every chunk but the last exactly 1 MiB, the delivery retained while one
chunk is missing, byte-identical state once it lands).

**Limits recorded.** `SEED_CHUNK_BYTES` (1 MiB) is the unit of transfer and
the seed pull's `max_bytes`; `Limits.inline_bytes` (4 MiB) and
`Limits.assembled_bytes` (256 MiB) bound the two forms; a seeded manifest
names at most `assembled_bytes / SEED_CHUNK_BYTES` chunks. Sealing charges
one chunk buffer plus the table; assembling charges the whole root once.
Moving a member between nodes under a fence (§6) will seed the member's
chunks through the same store.

## 6. Movement (R7.3, third step, 2026-09-09): the state machine in the log

**What moves.** A member of a session's range group is held by the session's
voters: they admit every mutation, so they materialize every member (F38).
Movement gives a member additional or different **holders** — materializer
replicas that hold its rows and serve its reads — and later retires a holder.
The holder of a member is `Holder::Voters` (the log itself) or
`Holder::Replica(ReplicaId {node, generation})`; the map that names every
member's span, generation and placement is the range crate's
`RangeMap<StorageKey, Placement {owner: Holder, readers}>`, built from the
committed layout ([§4](#4-range-groups-r72-second-step-2026-09-09)) and
carrying its own **range epoch** (one at genesis, one more per activation or
layout change). Remote admission reads — the leader admitting against rows it
does not hold — are not part of this step; every voter keeps every member,
and the doc says so where it counts (P11.3).

**One authoritative decision.** The directory selects placements; the session
log authorizes when they take effect ([04 §11](04-storage-and-distribution.md)).
Every movement step is a `FOCALRM1` record — `{ledger, expected ordinal,
RangeOperation}` under a digest — that the authority proposes and every
replica applies between native records, exactly like a layout record. The
operations are the range crate's: `Begin(RangeIntent)`, `Snapshot`,
`Barrier`, `SourceSealed`, `Ready`, `Activate`, `Abort`, `Cleanup`
([coordinator.rs](../../crates/focal-ranges/src/coordinator.rs)). Applying a
record runs the coordinator's `prepare` against the committed state and
`publish`es it under a `CommitProof` minted from the entry: the native prefix
the record applied at (`sequence`), the record's **ordinal** (one more per
applied movement record; the crate now orders control records by ordinal and
positions barriers and seeds by prefix, since Focal's native prefix advances
only per native record), the Raft index and term, the command hash and an
attestation derived from the session genesis. A record whose ordinal is not
the next one is a duplicate or a stale proposal and is inert; a record the
committed state refuses (`prepare` fails) is inert on every replica alike and
counted (`movement_refusals`), since every replica evaluates the same state
under the same verifier — the authority validated before proposing, so a
refusal means the state moved under it and it proposes again. The
coordinator state (`RangeCheckpoint`) is the movement's durable identity:
it travels in the session envelope (`FCNSESS1` ancillary byte 0 = 1, a
movement section after the membership configuration,
[22 §2](22-native-record-format.md)), so a restart or a lagging replica's
snapshot resumes from the committed step, never from memory.

**Proofs and the verifier.** The range crate's `RangeVerifier` is the seam:
commit proofs are checked by every replica by recomputing the derived
attestation from the genesis (`LedgerRangeVerifier`); source seals,
readiness, progress, availability and recovery proofs are attested the same
way in this step, where the only holders are the voters themselves, and the
node's signed proofs (its enrolled key, verified before the authority
proposes them) arrive with the materializer host (step four). A proof for a
member the voters hold is not required: a `Ready` is demanded only for a
replacement a replica holds, a `SourceSealed` only for a source a replica
holds, `unchanged` progress only for the replica-held members that do not
move, and a seed `Snapshot` only for replica-held replacements — the log is
the voters' proof.

**Fences.** Between `Barrier` and `Activate` the authority refuses a native
mutation whose fragments touch a moving member (a source or a replacement)
with the retryable `RangeMoving`, so nothing lands between the seed cutoff
and activation; mutations elsewhere in the group continue. A layout change
is refused while a movement is pending and a `Begin` while a layout change is
in flight, so the map and the layout never diverge; an applied layout record
re-lays the map (a split gives the new member its parent's placement, a merge
keeps the left member's) and advances the range epoch. `Abort` is possible
until the barrier and refused after it (`Sealed`). `Cleanup` of a retired
epoch's copies is proposed only while no read lease is pinned on the group
(the range crate's pin rule; R8's retention floor refines it); it is inert
on replicas whose local pins differ, since pins are local.

**What the tests hold.** `a_member_moves_under_one_authoritative_decision_and_faults_at_every_barrier_recover`
(cluster suite): every replica applies the same steps to the same state and
epoch; a duplicate `Begin` is inert; `Ready` with a forged attestation is
refused before it is proposed; after the barrier a mutation on the moving
member is refused `RangeMoving` and a layout change `RangeMoving`, an
`Abort` is `Sealed`; a follower that lagged before the barrier installs the
checkpoint carrying the pending movement and continues to activation; a
restart resumes from the pending step; activation moves the map to the next
epoch under the replica holder and reopens admission; `Cleanup` waits for
pins. The range crate's own suite covers ordinals, holders and the proof
requirements by holder.

**Limits recorded.** `RangeLimits` (64 ranges, 8 historical maps, 128 pins,
a 64 MiB movement checkpoint) bound the coordinator; a movement record is at
most `MOVEMENT_RECORD_BYTES` (64 KiB); one movement per session at a time.

## 7. Holders that serve (R7.3, fourth step, 2026-09-09)

**Why every holder materializes the whole session.** A row is not
self-describing: the codec hydrates the rows of a claim's dependents from
the claim's retained acceptance policy while it decodes them
(`read_dispatch::with_policy`, `claim_dependency`), and dependents such as
responses, artifacts and definitions live under their own affinities, not
the claim's. A store holding one member's rows alone could therefore neither
decode a record's changes for that member nor restore a member-scoped
checkpoint. Until rows are self-describing, a holder of any member holds
every member: a materializer is a Raft learner of the session with native
hosting, seeded by the chunked checkpoint ([§5](#5-chunked-checkpoint-seeds-r73-second-step-2026-09-09))
and tailing the log. What movement redistributes is **serving**: the member's
reads and pins. P11.3's memory distribution stays open with this reason.

**The facts a holder states.** A replica states three facts about a member
from its own committed rows, each carrying the digest of the member's rows
at its prefix (`Core::native_member_digest`, the root frame of that member's
rows alone; an empty member digests its prefix): `Ready` for a replacement it
will hold (`through`, the seed it installed and its state), `Seal` for a
source it holds at the barrier's cut, and `Progress` for a member that
stays. The fact travels over the holder's own authenticated connection —
`Operation::RangeControl {group, request}` (tag 31, node-only, certificate
required; body `RangeControlRequest {schema, fact}`, reply
`RangeControlReply::{Fact, Refused}` in `Response::Control`) — so the
transport's authentication is the holder's signature. The authority verifies
the fact against its committed state before it attests and proposes it: the
replica the fact names is the authenticated peer and the transfer's holder,
the prefix is at or beyond the barrier, and the digest equals the authority's
own digest of the member, which the barrier froze (`fleet::verify_fact`,
`verify_progress`). A digest that disagrees is refused; nothing an unverified
peer says reaches the log.

**The controller drives it.** On the node that leads a session, the placement
agent's controller (`placement_controller::drive_movement`) answers the
operator's move requests with a `Begin` whose intent it builds from the
committed map (the member's exact span and generation, a derived transfer
identity so a repeat names the same one, the destination named with its
enrolled generation) and carries a pending transfer one proposal per pass:
the seed record for every replica-held replacement (a log-tailing holder's
seed is the log itself; the record names the member's rows digest at that
prefix as its identity), the barrier once every seed is named, each holder's
readiness and each replica-held source's seal, the progress of every
replica-held member that stays, activation, and — once the map is retired
and nothing pins the group — cleanup with an attested recovery proof. A
refusal (a candidate still pending at the barrier, a holder not yet caught
up) is retried on the next pass; a restart of the leader resumes from the
committed step, since the coordinator state is in the checkpoint.

**Serving.** A voter serves every member. A learner serves a native read only
for the members its replica holds (`fleet::serves_member`; the query's
locations are derived from the objects it names, `native_reads::locations`)
and a listing only when it holds every member; anything else is unavailable
there and the client is routed to the leader as before. Steering clients to
a member's holder through the directory and the route cache is the next
step; the operator reads every holder now with `focal cluster replicas
ranges list` and moves a member with `focal cluster replicas ranges move
--member ID --node N` ([cluster-admin.md](../cluster-admin.md)).

**What the tests hold.** In process (`fleet_native_tests`): a move of the one
member to the node's own replica through the host — the same request names
the same transfer, the seed and barrier apply, a mutation on the moving
member is refused while fenced, the replica's readiness fact verifies
against the authority's digest and a forged digest or a wrong peer is
refused, activation moves the map to the next epoch under the holder and
reopens admission, cleanup retires the history. Across three real processes
(`a_seeded_native_checkpoint_carries_the_founder_session_to_new_hosts`): the
operator moves the member to host-a and the founder's controller carries it
to activation with host-a's readiness stated over QUIC; every process
reports the same map, and the founder reports it again after a kill and
restart.

**Limits recorded.** One move at a time per session, one member per move
(a replacement covers exactly its source's span); the destination's replica
identity is its node and enrolled generation; `RangeControl` requests are at
most 4 KiB; a holder's fact is asked with a five-second bound per pass. The
directory does not publish holders yet, so clients are not steered to them.

## 8. Automatic decisions (R7.4, 2026-09-10)

A group's shape is decided from what its members hold, not by an operator
choosing shard counts (REMAINING §12 instruction 3). The session leader's
controller observes every native session's committed map once per pass
(after it has driven any transfer, §7) and keeps one bounded table of
observations per `(ledger, member)`.

**Measure.** The member's row count on the replica that answers
(`NativeRanges::member_stats`, shown as `entries` by `focal cluster replicas
ranges list`). Rows are the one quantity every replica agrees on at a
prefix; bytes differ by page layout and load differs by node, so neither
is a fact a decision can be replayed against. Load, locality and resource
policy as further inputs remain open (below).

**Hysteresis.** The target is `FOCAL_RANGE_TARGET_ENTRIES` rows per member
(250,000 by default; a campaign lowers it so a small group reshapes). A
member past twice the target on three consecutive observations is split;
two adjacent members both under a quarter of the target on three
consecutive observations are merged. An observation that no longer meets
the condition resets that member's count, so a burst or a drain never
reshapes a group. A member that leaves the map takes its observations with
it; the table holds at most 4,096 keys and admits no new key beyond that,
so it is bounded by construction. A decision consumes the observations it
was made from and needs three more before the next.

**Decision.** At most one change per session per pass, a split before a
merge, and none while the map has a transfer pending or a candidate in
flight (a layout change and a transfer never interleave, §6). A split asks
the holder for the affinity that divides the member near its middle
(`NativeRanges::member_split_point`: the first affinity at or past half
the rows that is not the member's start — a member of one object has none
and is left alone) and names the new member from the ledger, member,
affinity and epoch (`focal.range.split.member.v1`), so the same
observation proposes the same record on every leader; a merge names the
left member. Both are ordinary `FOCALRG1` layout records (§4) proposed by
the authority; a refusal (candidates pending, a change in flight, the
layout at `max_ranges`) is not an error — the member is observed again on
a later pass.

**Identities after a move.** A transfer's replacement carries a fresh
member identity in the map (§7). When the activation applies, every
replica renames the core layout's member to the map's identity in the
same apply (`Core::rename_native_member`), so a later layout record, a
checkpoint's layout, a digest and the directory all name one member alike;
a map and a layout with different member counts is a divergence no replica
serves from.

**What the tests hold.** In process (`fleet_native_tests`): after the move
of §7, a split at the member's middle through the host reaches epoch three
with two members, both under the holder and both holding rows, and a merge
joins them back with the original count. Through the real binary
(`cli_native_a1` under `FOCAL_RANGE_TARGET_ENTRIES=4`): the complete
two-party claim cycle, kill and restart, identical reads and exact retries
run while the balancer reshapes the group, which ends with several members
that all hold rows at a later epoch. The balancer's unit tests hold the
hysteresis: a dip resets a count, a split takes precedence over a merge, a
group at its member bound never splits, non-adjacent or unequal pairs never
merge, and a member that left drops its observations.

**Open in R7.** Load, locality and resource policy as decision inputs
(instruction 3); the directory's publication of holders and steering
readers to them (§7); fault cuts at every movement step through the real
binary and translation of route-bound cursors across a move (instructions
6 and 7); fair transfer budgets (P11.6).

## 9. Movement under faults, holders in the directory (R7.5, 2026-09-10)

**Every step resumes from the log.** The controller that carries a transfer
(§7) keeps nothing between passes: each pass reads the committed map and
takes the one next step it names — begin the operator's move, record the
seed, propose the barrier, ask a holder for its readiness or seal and
propose the verified fact, activate, clean up. A controller that dies at
any of those points leaves a committed prefix another controller resumes
from; a step proposed twice is the same record (a duplicate `Begin` names
the same transfer, a duplicate fact is refused as applied). The crash cuts
of doc [06](06-verification-and-operations.md) §3 gain one site per step
(`movement-begin`, `movement-seed`, `movement-barrier`, `movement-ready`,
`movement-seal`, `movement-activate`, `movement-cleanup`), each placed
after the step's evidence is gathered and before its proposal.

**The controller claims the sessions it drives.** The controller runs
where the partition's owner is local and this node leads it; it can only
propose into a session's log when it also leads that log. A controller
restarted into a group that elected another voter meanwhile used to wait
for an operator's transfer (the carried limit of [24](24-placement-execution-and-fleet-control.md)
§9). Now, for a session with work — a placement plan in progress, a
transfer pending, a queued move, a retired map awaiting cleanup, or holders
the directory has not published — a controller that is a voter of the
session asks its leader for leadership: one raft transfer request a
follower may make for itself (raft forwards it to the leader it knows,
which times this voter into a campaign), repeated at most once per pass
until the election settles. A session with nothing to drive keeps its
leader.

**A dead destination holds at the barrier.** The seed and the barrier are
the authority's own records and commit whether or not the destination is
alive; readiness is the destination's fact and never arrives while it is
down, so the transfer holds with the member fenced (a mutation on it is
refused, §6) until the destination returns, states its readiness over its
own connection, and the transfer completes. The hold is bounded by the
operator: a move to a dead node is the operator's to abort by moving the
member elsewhere, which is not automatic (open below).

**Continuations survive a move.** A listing's continuation is bound to the
route epoch and the key order; a move changes the range epoch only, and
the listing is served over every member on the leader, so a cursor taken
before a move continues after it without a gap or a repeat.

**The directory names the holders.** Once a transfer activates, the
controller publishes the settled map — every member's identity, start and
holding replica at the range epoch — as one `SessionChange::Holders` on
the session's partition (schema 7 of the partition checkpoint; older
checkpoints restore with none). The directory applies a publication only
when its epoch exceeds the one it holds (the same publication again is
idempotent, a different one at the same epoch conflicts, an older one is
stale), when its members are in key order with unique identities, and when
every holding replica is a member of the active placement at its enrolled
generation — so a holder the directory names is a replica the placement
authorized. `cluster placement` shows `range_epoch` and `holders` per
session. Clients are not yet steered to holders by it (below).

**What the test holds.** Across three real processes
(`movement_survives_a_cut_at_every_step_a_dead_destination_and_duplicate_requests`):
the founder carries a crash cut at each of the seven sites in turn, dies
there, restarts, claims the session and completes the transfer, seven
moves alternating between the hosts each settling one epoch later under
its destination on every process; a duplicate move names the same
transfer; a move to a killed host holds at the barrier with readiness
absent and a claim submission refused, then completes when the host
returns; a claims listing continued across that move returns every claim
exactly once; the directory publishes the holders at the settled epoch.

**Open in R7.** Steering readers to holders through the route cache (every
holder still materializes the whole session, so a read served by a holder
distributes serving, not memory: P11.3); an automatic abort of a transfer
whose destination never returns; load, locality and resource policy as
balancer inputs (§8); fair transfer budgets.

