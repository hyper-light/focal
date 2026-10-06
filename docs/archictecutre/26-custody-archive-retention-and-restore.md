# 26 — Custody, archive, retention and restore

This document records the design and implementation of REMAINING §13 (R8):
evidence custody bound to placement, the archive and retirement path,
retention floors, garbage collection, and backup and restore. It builds on
[04](04-storage-and-distribution.md) §7 and §12, the disk envelope of
[24](24-placement-execution-and-fleet-control.md) §10 (R6.5, the disk
budget every durable owner promises its bytes to before acknowledging) and
the checkpoint seeds and movement of
[25](25-parallel-materialization-and-ranges.md). Each section names the
batch that implemented it; evidence is in [09](09-implementation-status.md).

## 1. Custody receipts and obligations (R8.2, 2026-09-10)

**The fact.** A copy's custody of an object is a fact the copy states after
it has verified the whole object, never something inferred from a transfer
request, an install intent or a manifest. The evidence coordinator already
acknowledges an artifact only after every required copy of the current
placement answers `Durable` to the sealed object's replication
([04](04-storage-and-distribution.md) §7; `evidence_service::replicate`),
and a copy answers `Durable` only after `complete_import` recomputed the
stream digest over every chunk it holds. What R8.2 adds is that the fact is
kept and read.

**The receipt.** `focal_evidence::CustodyReceipt` (`FCCRCPT1`, 160 bytes,
fixed width): the ledger, the object's domain, root and length, the copy's
node, the route epoch and policy revision the receipt was taken under, the
recorder's clock, and an attestation (a keyed BLAKE3 of the body under
`focal.custody.receipt.v1`) so a damaged record is refused rather than
read. Receipts are custody records of a new kind (`receipts/`), named by
`(ledger, root, node)` (`focal.custody.receipt.name.v1`), so an object's
copies are read directly rather than scanned, and a newer scope for the
same copy replaces the older receipt. The coordinator writes one for this
node when it seals or verifies an object it holds, and one per copy when
that copy answers `Durable` — from the verify shortcut or the sealed
transfer. Receipts ride the disk envelope like every other record.

**The obligation.** `CustodyObligation { scope, required, held }`: the
copies the current placement requires and those with a receipt at the
current route epoch and policy revision. A required copy without one is
asked over its authenticated connection (`CustodyRequest::Verify`; this
node through its own store) and its `Durable` answer is recorded; a copy
that cannot answer is simply not held. A placement change therefore never
inherits an older placement's receipts, and the obligation of an object
after an expansion is re-established from the copies themselves.

**Eligibility.** A phase that evaluates an artifact — `BeginIncrement`
against an increment's artifact, `BeginWork` against a work slot's — is
admitted only when the artifact's obligation is satisfied (instruction 1:
validation eligibility bound to verified custody in the configured content
domain). The fleet service decodes the frame's evaluation target under the
session's own limits (`into_evaluation_artifact`, a fixed-plan decode that
never builds an artifact), reads the artifact's content pointer from the
replica's committed prefix (`ReplicaHost::artifact_pointer`), asks the
coordinator for the obligation, and refuses a frame whose copies fall
short as a retryable capacity refusal naming the missing nodes; an artifact
the prefix does not hold is left to the owner, which refuses it by the
model's own rules. Frames that carry an artifact keep their existing path
(sealed, verified and replicated before admission).

**What the tests hold.** `custody_receipt::tests`: the receipt round trip,
every one-bit forgery refused, the name binding ledger, object and copy;
a store keeps one receipt per copy across reopen, a newer scope replaces
the older, a corrupt record is an error. `evidence_service::tests`:
sealing records this node's receipt and the obligation reads it; a second
required copy nobody can reach holds none and is named missing while this
node's receipt is renewed at the new scope; the receipt survives the
store's reopen; a phase-beginning frame names its artifact (work slot and
increment) and other frames do not. The real-binary A1 cycle runs
`BeginWork` through the eligibility check on a single copy.

**Limits.** Required copies are the placement's content copies; the
obligation does not yet weigh custody against the placement's promised
failure domains (P13.4). The directory's signed `CustodyProof` fact remains
unused: the readiness fact of [24](24-placement-execution-and-fleet-control.md)
§2 carries the prefix custody digest a promotion needs, and per-object
custody is the receipt above. Receipts are not yet reclaimed with their
objects (§5, garbage collection).

## 2. Schema and validator identity across upgrades (R8.3, 2026-09-10)

**Schemas.** `focal_evidence::SchemaRegistry` records each schema as an
immutable descriptor named by its BLAKE3 hash, with the byte bound a report
under it may reach, the sequence it entered service at, and — once
superseded — its successor and when. A superseded schema keeps its identity
and bound, so a report that cited it resolves exactly as it did;
`current(schema)` follows the supersession chain (bounded by the registry's
capacity, a loop refused) to the schema new work should cite. The same
descriptor again is idempotent, the same descriptor with another bound
conflicts, a schema is superseded once, and a successor may not lead back.
The registry is bounded (`MAX_SCHEMA_DESCRIPTOR_BYTES` 4 KiB per
descriptor) and, built with `new_in`, charged for its whole capacity up
front. It implements `NativeSchemaVerifier`: every registered schema answers
its bound; verification is the built-in contract of the two shipped
schemas, and any other descriptor is refused as unsupported rather than
guessed at (the descriptor language is not interpreted).

**Validators.** `validators::Registry` gives each version a `Lifetime`
(`introduced_at`, `retired_at`). `retire(handler, at)` keeps the version's
registration — `lookup` still answers the schema and bound a historical
report cited — and `execute` refuses it as `Retired { at }`; retiring twice
at the same sequence is idempotent, at another sequence a conflict, before
the version's introduction a contradiction. A version is immutable: the
same registration again is idempotent after retirement, a different one
under the same identity conflicts.

**Audit artifacts.** External validation reports attach audit artifacts
through the same artifact path as respondent evidence (`audit generate` and
`audit post`, [16](16-peer-validation-contract.md)): the same upload
bounds, the same custody (§1) and the same authorization (the evaluator
named by the validation). No second path exists to bound.

**What the tests hold.** `registry_history::tests`: identity and bound
across supersession, idempotence and conflicts, the chain and its loop
refusal, built-in verification, honest refusal of an unknown descriptor,
capacity and descriptor bounds, the charged registry returning its bytes;
a retired validator keeps its identity and runs nothing new, with every
refusal named.

**Limits.** Registrations and retirements are process-local facts of the
node that hosts the registry; committing them to a session's log so every
replica agrees on the lifetime of a schema is deployment work for the
schema tooling of R9 (`focal schema`), which today lists built-ins only.

## 3. The log's retirement boundary and the retention floor (R8.4a, 2026-09-10)

**Checkpoint cadence.** A replica's log grew until an operator or a
learner's arrival checkpointed it. Now every replica checkpoints its
applied prefix once the entries applied past its last snapshot reach
`ReplicaConfig::checkpoint_after_entries` (4,096 by default) and the log
behind the snapshot is compacted by consensus as before: nothing is
retired before the checkpoint that covers it is durable, and a replica
that cannot checkpoint yet (a checkpoint in flight, unpersisted state, a
delivery under way, a membership, placement, evidence or activation
record in flight, a resource condition) waits for a later tick. A
proposal waiting for its quorum does not hold it back (2026-10-03): the
checkpoint is of the applied prefix and the proposal above it, and a
domain candidate, a managed or cursor command or a cursor maintenance
changes nothing a checkpoint holds until it applies — a session that
waited for its proposals to drain checkpointed under a steady load only
at a period that found none. The bound is the log's retirement boundary
(instruction 4); `diagnose cluster --replicas` shows
`log_entries_since_checkpoint`.

**The retention floor.** `RetentionReport` names, per native session, the
published prefix, the prefix registered consumers still need
(`CursorRegistry::retention_limit`), the prefix the archive reports holding
every proof through, the least of them as the floor (never past what is
published), and which of the two holds the floor there. The archive's
report is a monotone fact a replica records (`note_archived`) and restores
from its own checkpoint (the `FCNSESS` ancillary byte 1 section, beside the
movement section of [25](25-parallel-materialization-and-ranges.md) §6);
until the archive of §4 reports anything the floor is zero and the
blocker is the archive. Diagnostics show the report as `retention`.

**Why nothing is reclaimed by sequence.** A first cut retired every event
row at or below the floor with a committed record every replica applied.
It was withdrawn before it shipped: the checkpoint validators of
[22](22-native-record-format.md) reconstruct every live object from its
complete event history — its children, monitors, seals, terminal position
and every revision — and the record format's successor invariant gives
every native sequence exactly one outcome row. Trimming history by
sequence under live objects either fails restore (the withdrawn cut did)
or forces those checks to be weakened. The unit of physical reclamation
is therefore the object, as [04](04-storage-and-distribution.md) §12 and
P12.2 state: a terminal-and-released object, its dependents and their
events leave the core together once an archive bundle holds them, behind
a typed continuation the validators and readers resolve. That is §4. The
floor above bounds what any such reclamation may cover.

**What the tests hold.** `native_session::retention::tests` (the floor is
the least obligation and never past publication); `native_checkpoint`
`a_retention_section_rides_both_forms_beside_the_movement_section`;
`native_session::cluster_tests::the_archives_report_is_monotone_and_rides_checkpoints`
(a report never regresses, reaches a lagging follower through the
checkpoint that carries it, and survives a restart from that replica's
own checkpoint); the in-process fleet native test runs under a four-entry
cadence and observes the replica compacting behind its checkpoint with
the floor reported.

**Limits.** The archive's report is local to the replica that recorded it
and travels only with its checkpoints; the retirement record of §4 commits
it. Registered consumers are the only retention obligation the floor
weighs besides the archive; request-stream receipts retire through their
own committed floors ([15](15-managed-request-streams.md)). A consumer's
row does not outlive its obligation (2026-09-29, the audit's F62): an
ordinary consumer whose lease expired, or whose cursor was sent to resync,
holds nothing, and its row is retired — named in the prepared update, so
the session's owner record leaves with it — when a registration needs its
slot (until then it stays, so a consumer that comes back reads why it must
reseed); the registry's bound
(`max_consumers`, 4096) therefore bounds the live consumers, never the
names ever seen. A retired name registers again under a generation no
earlier token carries (a generation is the revision that issued it), so a
stale acknowledgment or renewal is refused as the wrong generation and
never moves the next incarnation's cursor; a protected consumer leaves by
acknowledgment alone. A command against the registry prepares one row, never
a copy of it (2026-09-29, the audit's F61): a renewal, an acknowledgment, a
seed's completion or a resync patches the row's scalars at publication, a
registration or a seed carries its one row, and the retired names leave with
it; the row's bytes join the registry's charge and leaving rows return theirs.
A poll with nothing to acknowledge is a read that commits nothing, and the
node renews a polled lease itself once half of it has passed, by a
maintenance entry with no receipt (`Session::propose_cursor_renewal`), so an
idle consumer costs at most two entries a lease term.

## 4. Retirement to the archive (R8.4b, 2026-09-10)

**The unit is a family.** A claim leaves the core with everything registered
under it: the claims it owns (owner lineage, recursively), their
declarations (declared at creation or registered later), evaluations and
results, cycles, receipts, responses, testaments, work and diagnostic
artifacts, monitors, the index rows that name any of them, and the event
rows that describe any of them. `Core::retirement_family(root)` derives the
family from the committed state alone, so every replica derives the same
one from the same prefix; it is refused, with the reason, while any member
is not terminal (`NotTerminal`) or still holds its scope (`NotReleased`),
while the root is owned by a live claim (`LiveParent`), while a live claim
outside the family depends on, relates to or monitors a member
(`LiveDependent`, from the incoming-link, relation and monitor-link rows, a
tombstoned link's owner read from its `Monitor` row), while a member's own
monitor watches a claim outside the family (`LiveMonitor`), while an
artifact outside the family took a member as an input (`LiveReference`),
while a member's evaluation has begun and is neither terminal nor fenced
(`LiveEvaluation`: the owner still holds its completion contract), when
the family exceeds 64 members or 65,536 rows (`TooLarge`), or — asked
first, before a row is walked — when the outcome the retirement publishes
would pass the core's outcome bound (`OutcomeCapacity`: `limits.outcomes`,
the bound checkpoint recovery enforces on the count it restores; below).
Outcome and creation-result rows stay: every native sequence keeps its one
outcome, and an exact retry of a retired claim's creation is still answered
from it.

**The bundle.** `Core::archive_family_quote` and `archive_family_into` write
the family's rows in key order into an `FCNARCHV` frame (magic, version 1,
profile, ledger, the prefix the bundle claims, the root, every member, the
content roots of the family's artifacts held as content objects and the
roots of the objects sealed for those held inline — the proof the bundle
keeps under custody, §5 — the row count, the rows as `put_row` writes them, then a
digest under `focal.native.archive.v1`), never accounting rows; the same
family quotes the same bundle. `record_codec::StructuralArchive::inspect` verifies a
bundle before anything in it is trusted: digest, magic and version,
profile, ledger, a nonzero prefix, root first among the members, no
duplicate member, the row count against the inspection bound, every row
present (never deleted), keys strictly ascending, no accounting row, and
nothing after the last row; it exposes the header, the digest and the rows
by family. A bundle is content: the archive agent seals it as an object of
the ledger's tenant domain under the evidence class through the content
writer's inline seal, replicates it to every other required copy of the
current placement exactly as a sealed upload is, and reads the obligation
of §1 for it — this node's own seal is its own receipt, a copy's `Durable`
answer is recorded as its receipt. The object is named by its content root
and length; the retirement record carries both, and the continuation keeps
both, so any replica can locate the bundle without a catalog record: the
continuations are the catalog.

**Reading a retired family (the audit's F11, 2026-09-29).** A participant that
kept an exact identity — an artifact, a validation, a testament, a receipt of a
claim — follows it through the claim: `archive.get` (`get archived CLAIM …`)
reads the claim, and where the claim answers with its `Retired` continuation,
asks for the object from the bundle the continuation names
(`NativeReadQuery::Archived { bundle, bytes, object }`). The read is the content
owner's, never a session's: the bundle is fetched from this node's custody under
the request's tenant scope (`CustodyStore::check_scope`), verified structurally
(`StructuralArchive::inspect`), hydrated into a core of the family alone
(`StructuralArchive::hydrate`: the same decoders, schema verification and custody
recovery of its artifacts a checkpoint restore runs, through the shared phased
hydration `recovery::hydrate_frame`, validated as a family — every member claim
present — and laid out as one member at the prefix the bundle claims), and the
object built by the documents a live read builds (`native_reads::object`), each
returned as `NativeObject::Archived` with the bundle, the family's root and the
prefix it claims; a validation comes with its evaluations in key order and their
accepted results, the pages a live `validation.get` follows. What is told apart:
denied tenant access is `Unauthorized`, custody this node does not hold (or holds
corrupt) is `Unavailable`, a row the bundle never held is `Missing`; a live family
answers unwrapped from the ledger, so the read says which it was, and its latency
— a bundle's read and hydration, bounded by the bundle's inspection limits and
the restore's work envelope — is never mistaken for a live read's. A plain read
of an evaluation or a result whose claim retired answers with the claim's
continuation instead of an absence, since its key names the claim.

**The record and its application.** `FOCALRT1` version 2 (154 fixed
bytes: magic, version, ledger, the native prefix the family was derived at,
the root, the bundle's content root, its length, the prefix it claims, the
outcome bound the authority checked the retirement against, a digest under
`focal.native.session.retirement-record.v2`; a version-1 record is the same
without the bound, 146 bytes under the `.v1` domain, and still decodes) is
a session decision like a layout record
([25 §4](25-parallel-materialization-and-ranges.md)). Only the authority
proposes it (`propose_retirement`), after deriving the family from its
committed core, asking its owner whether the outcome the retirement
publishes is to spare (below), and checking the bundle's claim against the
family (at least the family's last event, at most the derived prefix);
refused while candidates are pending, a layout change, a movement step or
another retirement is in flight, or the family is ineligible
(`NativeSessionError::Retirement(refusal)`), and a refusal proposes and
fences nothing. While the record is in flight native admission, layout
changes and movement steps answer `Retiring` (retryable), so the record is
never wasted by a later prefix. Applied, the record is inert when the prefix
it named has passed, when a movement is pending, or when the committed state
refuses the family — the same on every replica, and counted
(`retirements_inert`, a diagnostic of the replica, never an input to its
state); otherwise every replica derives the same family and
`Core::retire_native_family` deletes its rows, decrements the Meta counters
per family, writes one `Retired(claim)` continuation per member (family
49: the bundle's root and length, the prefix it claims, the claim's final
binding and status, the retirement's sequence, and the number of event rows
that left with that member), and publishes one outcome
(`NativeInvocation::Retirement(root)`, operation `Retire`, no events) at the
next sequence. On an authority the record ends every pending candidate
first (`SuffixEvidence::Retired`) and the owner is reconstructed at the
next readiness barrier, as after any record it did not author; a memory
refusal keeps the delivery for a later poll, any other refusal after the
family compared equal is fail-closed. A term change lets an unapplied
proposal go. The replica counts the families it applied; the count rides
its checkpoint's retention section beside the archive's report and is
restored from it, so a replica seeded from a checkpoint reports the count
through the prefix it installed.

**The outcome a retirement publishes (the audit's F02, 2026-09-29).** The
retirement's outcome is one of the `limits.outcomes` a node admits — the
bound checkpoint recovery enforces on the count it restores, and requires
equal to the prefix. A retirement that published the bound's last outcome
and one more made a state the same configuration could not restore
(`Contract(Capacity)` from the checkpoint, `Capacity` from an owner rebuilt
over it), and one that fit the bound but took an outcome the completion
book had promised to a live report left a core no owner rebuilt over:
`NativeOwner::new` refused at every readiness barrier and the authority
never returned. The outcome is guarded now as ordinary admission guards its
own, at three places. `Core::retirement_family` refuses the family first,
before a row is walked, when the outcomes counted plus one pass the bound
(`OutcomeCapacity`, the permanent refusal, named before any other);
`Core::retire_native_family` refuses the same at publication
(`Capacity("outcomes")`), after checking that the outcomes counted equal
the prefix (a contradiction is `InvalidManifest`) — the last fence, never
the check. `NativeOwner::check_retirement` asks the completion book what
every fresh candidate is asked (`check_slots`, with the meta row one outcome
and the prefix one sequence ahead): whether the outcome is to spare beyond
those promised to live reports and the one control the owner keeps for an
authority decision; the session names that refusal `OutcomesReserved`, and
it frees as the reports arrive. `propose_retirement` runs its gates, derives
the family, asks the owner, then encodes, so nothing is proposed or fenced
on a refusal; `Session::native_check_retirement` asks the same short of the
family, and the archive agent asks it before it seals a bundle. The record
carries the bound the retirement was checked against, since
`limits.outcomes` is a node's own setting and committed nowhere else; a
record written without a bound, or whose prefix its bound does not hold one
past, is refused at encoding and at decoding alike. At application a
replica whose own bound cannot hold the retirement's outcome, where the
record carries a bound, fails closed
(`NativeSessionError::OutcomeBound { committed, local }`, both bounds
named): it is configured below the authority, and applying nothing where
every other replica retired would diverge silently, while applying the
record would make a state its own checkpoint could not restore — the rule
a layout record applies to a replica whose member bound is lower than the
authority's (25 §4). A version-1 record carries no bound (it was proposed
without the check): where it does not fit it is inert and counted, as any
refused family, never a stop.

**What the validators reconcile.** An outcome row still counts the events
its sequence published, some of which left with a family. Every
`Retired` row says how many left with its member; checkpoint validation
counts the outcome events it cannot find, sums the continuations' counts
and requires the two to agree while the live events still equal the Meta
count — a missing event row that no continuation accounts for is still a
refusal. A continuation is checked for its own consistency (a terminal
status, a nonzero bundle and length, retirement after the prefix it
claims, at or below the checkpoint's prefix) and for the absence of the
claim and content rows it replaced. Readers answer a retired claim with
its continuation: `NativeObject::Retired` carries the claim, its final
binding and status, the bundle's root and length, the prefix it claims, the
retirement's sequence and the events that left; a `Missing` answer still
means no such claim at this prefix.

**The archive agent.** Every node runs one (`archive_agent.rs`): each tick
(`FOCAL_RETIRE_INTERVAL_MS`, five seconds by default) it walks the terminal
status buckets of every replica it hosts (`Core::retirement_candidates`,
1,024 index rows per replica per tick, resuming where its bound stopped it),
asks the replica for the family's bundle — given only where this node is
the authority, the family is eligible, the retention floor of §3 allows
it (the complete sequences every registered consumer lets retire reach
the family's last event, everything published when there is no
consumer), and the family's last event settled at least the grace ago
(`FOCAL_RETIRE_AFTER_MS`, one day by default, measured on the node's
logical clock against the logical time of the outcome that published
that event: a finished claim stays readable in the core for that long) —
seals the bundle under custody, and proposes the record once
every required copy holds a receipt; one family per replica per tick, and
a family whose copies have not all answered waits for a later tick with
its bundle already sealed. The agent holds nothing the records do not: a
restart resumes the walk from the index.

**The operator's view.** `diagnose cluster --retention [--session]`
(`diagnose.cluster.retention`) reads the floor of §3 with the families retired
through the applied prefix and whether a retirement is in flight
(`retention.retired`, `retention.retiring`, also in replica diagnostics);
`cluster archive show --claim ID [--session]` (`cluster.archive.show`)
reads a retired claim's continuation from the replica and the bundle from
this node's content store, verifies it structurally and reports the root
and length, the digest, the root and members, the rows by family, and the
required copies holding a receipt for it (`verified: false` when this node
does not hold the bundle or it fails its check; `null` when the claim has
no continuation here). The real-binary test `cli_retention.rs` runs a claim
through create, cancel and release, waits for the agent to retire it,
reads the continuation, the counts and the verified bundle with its
receipt, retries the creation exactly, kills and restarts the node and
reads all of it again.

**What the tests hold.** `native::retirement_tests` (eligibility refusals,
the family's events and last sequence, the candidate walk and its cursor,
the bundle's identity, structural verification and refusal of every
corruption, retirement, the continuation, exact restore and validation of
the checkpoint, continued admission; an owned tree keeps its parent and
takes its children; the outcome bound: a retirement one under it restores
and rebuilds an owner under the same bound and still answers the exact
retry while a fresh request meets the bound itself, one at the bound is
refused at derivation and at publication with the sequence, the counters
and the budget untouched, one past the bound is what both the checkpoint
and the owner refuse, exactly the spare outcomes' worth of families retire
and the next is refused, and the owner holds the outcomes promised to live
reports back from a retirement — at the smallest bound an owner rebuilds
under, the core's own check passes, the owner refuses, and retiring
regardless leaves a core no owner rebuilds over, while one bound higher the
retirement is allowed and the promised report and a deadline control are
admitted after it); `native_session::retirement::tests` (the record
round-trips and refuses every corruption, carries the bound its retirement
fits and refuses one it does not, and a version-1 record decodes from the
bytes its writer produced); `native_session::tests` (at the bound a
retirement is refused with a typed refusal, nothing in flight, nothing
fenced, and a reopen unchanged; one under it reopens from the log alone
and from a checkpoint under the same bound; a replica below the committed
bound fails closed on the record with both bounds named and opens under
it; a version-1 record applies where it fits and is inert and counted
where it does not);
`native_session::cluster_tests::committed_retirements_apply_on_every_replica_and_fence_proposals`
(authority-only proposal, refusals before proposal, fencing of native
proposals, layout changes and a second retirement while one is in flight,
identical state and digests on every replica, the continuation and the
outcome, the exact retry of the retired claim's creation, no second
retirement, a lagging follower seeded from a checkpoint with the count, a
restart) and
`…::a_cluster_at_the_outcome_bound_retires_and_a_lagging_follower_restores_under_it`
(three voters one under the bound: the record carries it, the follower that
missed the release and the retirement restores the retired state from the
authority's checkpoint under the same bound and keeps it across a restart
under it); `session::native_tests::a_hosted_authority_retires_under_the_outcome_bound_and_is_refused_at_it`
(the hosted authority stays authoritative after an allowed retirement and
admits nothing fresh past the bound; at the bound the refusal leaves nothing
in flight); `native_session::retention::tests` (the floor's inclusiveness);
`cli_retention.rs` as above.

**Limits.** The grace and the interval are node-local environment
settings, not committed policy: two authorities of one fleet may differ
until R9's committed policy carries them. Continuations are kept forever: one fixed row per retired
claim is the price of answering any old identity exactly; compacting them
into the catalog of a backup is R8.6. Retirement takes whole families only;
a long-lived claim keeps everything registered under it, and a family
larger than the bounds waits for R8.5's paced compaction. Object-level
hydration from a bundle into a core is the restore path's (R8.6), not a
read's: a read of a retired claim is its continuation, and the operator's
verification confirms the bundle's structure and custody, not the rows'
meaning. The agent proposes on the authority only, and the leader of a
session whose consumers lag holds every family until they catch up. A
bundle whose copies never answer stays sealed on the authority and is
re-offered every tick; nothing reclaims it before R8.5's collector.
`limits.outcomes` is a node's own setting: the record carries the bound a
retirement was checked against, and a replica configured below it stops at
the first committed retirement its bound cannot hold rather than diverge —
a fleet whose nodes differ in the bound is raised at the low node, never
retired around. A checkpoint already encoded past its bound by the
unchecked retirement is still refused at restore; its repair is a decision
the remediation record leaves open.

## 4a. Seals: the outcome history leaves the live core (F12, 2026-09-30)

Retirement takes families and leaves their outcomes, so a session's lifetime
history stayed its live capacity: every request's outcome row, kept for the
exact retry that may still ask it, counted against `limits.outcomes` for good
(the audit's F12). Resident outcomes are now exactly what the live path can
still be asked: the open obligations and the unsealed tail. An outcome is
closed when nothing asks it again through the live path — a request's once its
generation is below its principal's floor ([21 §3](21-native-input-format.md):
the fence answers `RequestHistoryExpired`, never executes the request again),
a timer's once its claim retired (its rows left with the family), a
retirement's and a seal's own as they are published. A **seal** is a session
decision beside retirement: the authority derives from its committed state, at
the committed prefix, the closed outcome and creation-result rows — whole
generations of whole principals, then the retirements' and seals' own
outcomes, at most a bundle's worth — writes them into an `FCNSEAL1` bundle
under custody ([22 §3](22-native-record-format.md)), and proposes the
`FOCALSO1` record naming the prefix, the bundle, the count it must derive, the
bound of the derivation and the floors it forces. Every replica derives the
same plan from the same prefix and applies it alike: the rows leave, each
sealed principal's window records which seal holds which generations, the
seal's row is written, the Meta counts the sealed rows and the seal, and the
seal's own outcome is published at the next prefix. A record derived at an
older prefix is inert and counted; a plan whose count differs from the
record's is a divergence and fails closed; a replica whose resident outcome
bound differs from the authority's derives other floors and fails closed by
name (`OutcomeBound`), as for a retirement. While the record is in flight
every other proposal is fenced (`Sealing`), and the owner is reconstructed at
the next readiness barrier.

**Pressure.** The live window is bounded for everyone (`limits.outcomes`) and
shared among the principals with a window (each may hold at most its share:
the window divided by the principals, at least one). When the resident
outcomes and the candidates that may still be admitted (`limits.pending`)
would pass the bound, the seal forces floors: the open generations least
recently used — by the logical time of their last request, then by principal
— close first, until what they hold covers the excess. The floors are derived
deterministically from the committed state and named by the record, so every
replica re-derives and checks them. A client whose generation was closed under
it learns so by name on its next request, resolves the outcome it may already
have from the seal, and continues in the generation the owner admits.

**Reading a sealed outcome.** An exact retry of a sealed request is refused
`RequestHistoryExpired`; its outcome is read by `request inspect --remote` (and
the adapter's `request.inspect`): the owner answers an outcome read whose
request's generation is sealed with where it went (`NativeObject::Sealed`: the
seal's ordinal, bundle and length), and the client follows it
(`NativeReadQuery::Sealed`) to the content owner, which reads the bundle under
the tenant scope and answers the outcome row, descending folds to the member
that holds it. A journal that lost the operation asks the owner's window
(`NativeReadQuery::Epochs`) which generations to probe.

**The index is bounded.** Seal rows are at most `limits.seals` (derived from
the bundle's byte bound over a fold member's bytes); at the bound a seal
carries a **fold**: the oldest half of the seal rows become one directory row
keyed by the last of them, whose bundle names the members and every window
range pointing into them, and every window's ranges follow it (adjacent
ranges under one seal merge). A window's own ranges are bounded by the same
count; the fold is applied to a window before the seal that carries it is
recorded, so a window at the bound admits the seal that makes room. Seal
bundles are content roots: the collector keeps them and a backup carries them.

**The embedded node.** A node started without a network (`focal start` on
its data directory alone) hosts its session on one owner thread and has no
network service to run the agent; until this batch it never retired a family
nor, now, sealed a generation, so its window would have filled for good. The
owner thread runs the agent's walk itself (`EmbeddedArchive`, in the
maintenance step, at the agent's interval and grace from the same settings):
the same derivations the fleet's replica owner uses (`archive_derive`:
a released family's bundle, the seal the closed outcomes yield), each bundle
sealed into the node's own content store — the one copy such a node has —
before its record is proposed and polled to commitment; a step that cannot
run now waits for a later tick and is counted. The reads of a retired or
sealed object come from that store, as on a network node.

**Bounds and settings.** `limits.principals` bounds the windows (the
enrollment bound); `limits.outcomes` sizes the window and is a node's own
setting (`FOCAL_NATIVE_OUTCOMES` for qualification; every replica of a session
runs under one value, or the seal record's bound names the difference). The
archive agent proposes a seal on each tick when the pressure floors are
non-empty or the closed rows reach half a bundle; a bundle whose copies have
not all answered is re-offered next tick (`seals_waiting`), as a retirement's.

## 5. Reclaiming bytes: the collector (R8.5, 2026-09-10)

**What is reclaimable.** Nothing the committed rows name, and nothing
young. The node's bytes outside the log and its checkpoints are the
content store's objects (sealed uploads, the payloads native artifacts
sealed at admission, archive bundles), its custody records (the checkpoint
a custody verification installed, transfer manifests, receipts), its
staging (uploads in progress and the terminal records that fence finished
identities), and each replica's seed store (the chunks of seeded
checkpoints). Objects a row names are proof and stay: a live artifact's
payload, a continuation's bundle, and everything a bundle's header names
(§4). Objects no row names — the payload of a frame the owner refused
after the node sealed it, an upload sealed and never bound, a bundle whose
record never committed — are reclaimable once they have stood untouched
for a grace. So are custody records beyond the newest few, receipts of
objects that are gone, staged uploads nobody touched for the grace,
terminal records past the fence grace, and seeds no checkpoint or pending
seed names.

**Roots.** A hosted replica's committed rows name their objects through
`Core::native_content_roots`: a bounded, resumable walk over every row
(4,096 rows a page) that yields each artifact payload held as a content
object by its pointer, each held inline by the pointer of the object every
replica sealed for it at admission (the record carries that pointer and
every replica reads the object back under it, so its root is the same on
every node), and each continuation's bundle by root and length. Every
bundle's header names its family's artifact roots and the roots of its
inline objects, so a retired family's proof is protected by reading the
bundle once (remembered per bundle root, bounded). A domain (a tenant's objects) this
node holds copies for without hosting a replica with a native core — or
whose walk cannot complete — is opaque: nothing in it is ever collected,
and the pass reports it. The `ProtectionSet` (`focal_evidence::gc`) holds
the roots by object (a stream-digest form exists for objects a caller
knows only by their bytes), the records in use and the opaque domains,
sorted once.

**The collector.** `ContentStore::collect_step` advances one pass a
bounded number of file visits at a time, across steps and processes: it
finishes staged uploads untouched past the grace as abandoned (their
terminal fence installed, their bytes returned, exactly as an explicit
finish), releases terminal records past the fence grace, and then for
every domain that is not opaque reads each manifest — a protected or young
object marks its chunks, any other past the grace is quarantined — then
quarantines each chunk no retained manifest marked (chunks are shared by
content, so a domain whose marks exceed the bound keeps its chunks and
reports the deferral); custody checkpoints and manifests keep the newest
`keep_records` and every protected one and quarantine the rest past the
grace; receipts of objects the store no longer holds follow them; and
finally every quarantine round older than the quarantine grace is deleted,
file by file, bytes counted. Quarantine is a rename into a dated round
under the store (`quarantine/<started-ms>/…`), synced like any install,
reversible by `restore_quarantined(domain, root)` until the round expires:
the manifest and every chunk of the object still in a round come back
together. Seeds have no quarantine — a seed is a copy of checkpoint bytes a
peer can serve again — and `SeedStore::collect` removes those nothing
protects past the grace, resumably. An I/O failure stops the store as any
write failure does; reopen re-syncs the quarantine root and resumes with
what the rounds hold.

**The agent.** Every node runs one (`gc.rs`): each tick
(`FOCAL_GC_INTERVAL_MS`, one minute by default) it gathers the roots of
every ledger it holds content for — the installed custody policies and the
replicas it hosts — installs the protection set in the content store
(`ContentHost::protect`), drives `collect` to completion or to a step bound
(a pass that did not finish continues at the next tick under the same
set), then sweeps every hosted replica's seeds under the chunks its latest
checkpoint and any pending seed name (`native_seed_chunks`), and publishes
the pass. Settings: `FOCAL_GC_GRACE_MS` (one day), `FOCAL_GC_QUARANTINE_MS`
(seven days), `FOCAL_GC_TERMINAL_MS` (seven days); the newest four records
of each kind stay; 1,048,576 chunk marks per domain.

**The operator.** `diagnose node --gc` (`diagnose.node.gc`) reports the
settings, whether a pass is in progress, how many completed and the last
pass: replicas walked, objects protected, opaque domains, bundles this node
could not read, and the content store's counts (visited, uploads expired,
terminals released, objects and chunks and records and receipts
quarantined, chunk sweeps deferred, files and bytes deleted) with the seed
sweeps' counts. `cluster gc restore --domain --root` (`cluster.gc.restore`)
brings a quarantined object back, exactly once. The real-binary test
`cli_gc.rs` seals a work artifact's payload and a refused frame's orphan,
watches the orphan leave through quarantine while the payload stays,
restores the orphan and watches it leave again, retires the claim and sees
the bundle and the payload it names stay and the bundle still verify, and
kills and restarts the node to restore the orphan from the quarantine that
survived.

**What the tests hold.** `store::gc::tests` (an empty pass; quarantine,
restore and deletion of an unreferenced object with its receipt and its
unshared chunk while a shared chunk stays with the object that marks it;
opaque domains and the mark bound leave bytes in place; abandoned uploads
expire and their fences are released later; custody records keep the
newest and the protected; unprotected seeds leave past the grace,
resumably); `native::retirement_tests` (bundle headers carry the family's
content roots and inline digests); `cli_gc.rs` as above.

**Limits.** A content-copy node without a core for a session keeps that
tenant's objects until it hosts one: the collector never guesses what a
remote core names. A reference established after a pass to an object the
pass found unreferenced meets a quarantined object and an honest refusal
(`CustodyPending`) until the operator restores it or the grace is raised;
the grace is the window admission in flight is given. Continuations are
not compacted (§4). The settings are node-local until R9's committed
policy. Seeds a peer is fetching mid-transfer are protected only through
the pending seed of the fetching replica and the latest checkpoint of the
serving one; a transfer's blocks are named by the movement checkpoint and
not yet by the collector, so seeds of an in-flight range movement rely on
the grace (25 §6, R8.6 names them).

## 6. Backups at a declared prefix (R8.6, first step, 2026-09-10)

**What a backup is.** A collection of copied files is not a backup: the
node's directory holds a shared log of several sessions, a content store
of several domains and seed stores whose files change under the collector.
A backup of one session is the coherent set the plan names (instruction 7
of R8): the session envelope a replica installed durably at one applied
index, the chunks of that envelope's Core root when it is seeded, the exact
authenticated tree of every content object the prefix's rows name and of
everything the archive bundles it names name in turn, and a manifest that
lists all of it with hashes and is written last. `focal_ledger::backup`
owns the format; the node (`backup.rs`) drives it.

**Taking one.** `cluster backup create --output DIR [--session ID]`
(`cluster.backup.create`) asks the hosting replica for its evidence export
(`ReplicaHost::checkpoint_evidence`, §1): the replica checkpoints at its
applied index, the consensus owner fsyncs the rewrite, and the exact
`FOCALSS7` bytes come back with the prefix they were installed at (cluster,
ledger, log group, placement genesis, node, legacy sequence, Raft index and
term, route and epochs, the placement digest and the envelope's own hash)
and the membership the checkpoint was written under. The bytes are copied
out and the export's pin released; the files are written on a blocking
thread from the node's read-only content and seed readers. The writer first
derives the **inventory** from the envelope alone: it decodes the envelope,
assembles a seeded root from the seed store, rebuilds the native Core with
the recovery path every replica uses (which reads and verifies every live
artifact's object on the way), walks `native_content_roots`, and reads
each bundle's header for the roots it names. So what the backup carries is
exactly what a restore of that envelope will need, never what the live
store happens to hold. Then, in order: `checkpoint`; `seeds/<hash>.seed`
for each chunk of a seeded root; `content/<root>.manifest` (the object's
manifest verbatim, its root the hash of those bytes) and
`content/<hash>.chunk` for every chunk (shared chunks once); and `MANIFEST`
last. Every file is installed the same way — a temporary name, the bytes,
a file sync, the rename, the directory sync — through a `BackupMedium`,
which the real filesystem and the qualification's simulated disk both
implement. A directory that already holds a manifest is refused
(`Exists`); a session that has no committed placement yet answers
`unavailable` and the operator retries once it is registered; a killed
write leaves files but no manifest, and a directory without a manifest is
not a backup.

**The manifest.** `FCLBKUP1`: the magic, a postcard body and a BLAKE3
trailer over both ([22](22-native-record-format.md) §8). The body names the
schema, the creation time, the evidence prefix, the native prefix, the
activation's genesis, the profile and content domain, the decoder pair the
log promised (managed predecessor, native successor), the membership
configuration, the envelope's hash and length, the seed chunks in table
order, every object with its length, class and chunk list sorted by root,
and the retention section's floor and retired-family count. It is decoded
only whole: a short, mismatched or trailing manifest is corrupt.

**Verifying one.** `cluster backup verify --input DIR`
(`cluster.backup.verify`) reads only the backup and runs anywhere the
binary does — a killed node, another machine — because the manifest names
its own domain and the limits derive from it. It checks the manifest's
digest and schema, the envelope's hash and length, every seed chunk, every
object's manifest against its listing and every chunk's hash and length,
then rebuilds the envelope against the backup's own files (the same
inventory, over the backup's content and seeds) and requires the objects
and seeds it names to equal the manifest's lists exactly and the envelope's
coordinates to match. It reports each count, whether this binary carries
the decoder the backup's log promised, and every problem it found (bounded
at 64), and it is `complete` only when there is none. Nothing is written.

**What the tests hold.** The ledger suite writes a hosted session's image
to the simulated disk, verifies it, refuses a second write into the same
directory, cuts the write before every one of its durable operations and
shows each survivor is no backup (no manifest) while its files are whole
or absent, names a tampered chunk and a tampered envelope, writes the same
backup through the real filesystem, and does the same for a seeded root
whose chunks travel as seed files and whose missing chunk is named. The
real-binary journey (`cli_backup.rs`) backs a session up after a sealed
work artifact, verifies it, is refused a second write, sees a tampered
chunk named while the envelope still verifies, backs up again after the
claim retired (the bundle and the payload its header names: two objects,
one bundle), verifies with the node killed, and refuses an empty
directory.

**Restoring one (R8.6, second step, 2026-09-10).** `cluster restore
--input DIR [--new-incarnation]` (`cluster.restore`) hosts the backup's
session on this node from the backup's prefix. Nothing is served before
the backup verifies (the same verification as above, including the
decoder); a session this node already hosts, or this cluster's directory
already holds, is refused — a live descriptor changes through a placement
plan, never through a restore; and the session's tenant must be one this
cluster serves (`cluster tenants admit`). Then, in order: every object the
manifest lists is imported into this node's content store exactly as a
custody transfer installs it (each chunk verified, the manifest published
last; objects already held are verified in place); the seed chunks go into
the session's own seed store; the envelope is **rewritten** for the
incarnation the restore takes — the same rows, cursors, request streams and
activation record, under this cluster and the chosen log group with the
activation genesis re-derived for them, the bootstrap membership of this
node alone, no placement record and no membership receipt (the placement
and membership sections are reset, the movement section is dropped and the
layout rebuilt from the rows), a seeded root sealed into the new seed
store; a **fresh logical log** begins from it (`DurableNode::restore_on_wal_in`:
on an empty log, the group identity, the decoder floor and transition the
backup's log promised, the snapshot at the backup's index and term under
that membership, and a hard state that commits it — a log holding any
record is refused, so a restore never overwrites history); and the copy is
opened exactly as a restart opens one, recorded in the install journal as
created here, hosted by the fleet and, on the agent's next pass,
registered with the directory as a session founded on this node at route
epoch 1. Readers of that session use a connection that names it: a saved
Unix connection with `--tenant` and `--session` for the node's operator
(the node serves every session of every tenant it admits through its local
socket), or an enrolled client's context with `--enrolled-as` and
`--session` for another session of its own tenant.

**Which incarnation.** The decision is taken before anything moves,
against the committed enrollment registry. The backup's incarnation
**continues** (`same_incarnation`, its log group kept) only when the backup
came from this cluster and every other member of the membership it was
written under — voters, learners, outgoing and incoming — has had its node
enrollment revoked: then no copy of the old log can act again and the old
authority lineage is safe to continue. Otherwise the restore is a
**recovery incarnation** (`recovery_incarnation`): a new log group derived
from the backup's group, its envelope hash and the restoring node (so the
same backup restored on the same node names the same group), a new
activation genesis, and the members that were not fenced named in the
reply. A recovery incarnation is refused unless `--new-incarnation`
acknowledges it, because two copies of one session under two geneses are
two sessions from then on. The history is the same; the authority is not.

**What the tests hold.** The ledger suite restores a backup in process into
another cluster and node: the rewritten envelope holds the prefix and the
claim's status, the session becomes the native authority alone, commits new
work at the next sequence, checkpoints and reopens on its own log, and a
second restore into that log is refused. The real-binary journey
(`cli_restore.rs`) backs a session up on one cluster, kills it, admits the
tenant on a fresh cluster, is refused the unacknowledged recovery, restores
with the acknowledgement, sees the session register there as founded by the
new node, reads the claim and the artifact's payload through the operator's
local connection addressing the restored session, backs the restored
session up again from its new incarnation (a complete backup under the new
group), is refused a second restore, and reads again after a restart.

**Limits.** The export holds the replica's checkpoint gate for the copy
only, but the checkpoint it takes is a real one: a session with a proposal
in flight answers `capacity` until it commits. A backup is one session's;
an operator backing up a tenant runs it per session. The inline objects of
retired families are carried through the bundle header; a bundle this node
cannot read is a refusal, not a silent gap. A restored session's former
participants hold credentials of the cluster they enrolled in: on another
cluster their history is readable and their claims stand, but they cannot
act until enrolled again, and a restore into the cluster they belong to
whose directory still names the session is a placement decision (R9's
plan and apply), not a restore. The continuation of an incarnation is
decided by revocation alone; a member that is merely dead is not fenced.

## 7. The operator's storage view and R8's close (2026-09-10)

**The view.** `diagnose node --storage` (`diagnose.node.storage`) is one read
that answers instruction 9 of R8 for a node: the volume envelope every
durable owner of the data directory promises its bytes to
([24](24-placement-execution-and-fleet-control.md) §10) — free bytes at
the last sample, bytes outstanding in total and by kind (log, checkpoint,
content, archive, staging), the headroom that is never spent and the
completion reserve; the uploads in progress and the bytes they have
staged; the archive agent's settings (interval, grace) and progress
(ticks, records proposed, bundles sealed whose required copies have not
all answered — the repair the agent is waiting on); the collector's
settings; and for every hosted native session whether this node is its
authority, the log kept beyond its checkpoint, and its retention floor
with the published prefix, what consumers still need, what the archive
reports and what holds the floor there. The view initiates nothing and
promises nothing: a floor held by `cursors` moves when consumers
acknowledge, one held by `archive` when the archive reports, and the
envelope's free bytes are the sample the last admission saw. Sessions
beyond a bound are reported as truncated, never silently left out.
Recoverability is what `cluster backup verify` proves of a backup (§6);
the view does not guess it.

**What R8 now holds against its close gate.** A sustained workload under
fixed memory and disk budgets
(`a_sustained_workload_stays_within_its_budgets_and_keeps_every_outcome`):
twenty-four rounds of create, finish, release, archive under custody and
retire, a checkpoint and a full collector pass every fourth round, the
session's memory within its allowance and plateaued after warm-up, no
proof quarantined, every creation's outcome answered exactly at the end,
every continuation's bundle held and verified by the store, and a backup
written and verified whole. Corrupt or missing proof is detected before
anything unsafe: a corrupted bundle chunk turns `cluster archive show`
unverified and back once repaired (`cli_retention.rs`); a corrupted
backup chunk is named by `cluster backup verify` and refused by `cluster
restore` before any file moves (`cli_backup.rs`, `cli_restore.rs`); a
missing seed chunk or artifact object fails the envelope's rebuild
(§6); a chunk that fails its hash is refused on the way in by every
import path (§1, §6). Backup and restore reproduce an explicitly stated
prefix and content inventory under cuts at every durable operation of the
write and under a killed node (§6). What R8 leaves to later packages is
stated in each section's limits: the receipt floors of P12.3 and cold
proof lookup beyond a bundle's verification, searchable indexes over
bundles, pacing by work credits under load, the compaction of
continuations, and the committed retention policy of R9.

