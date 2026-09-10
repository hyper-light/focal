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
that cannot checkpoint yet (candidates pending, a checkpoint in flight,
unpersisted state, a resource condition) waits for a later tick. The
bound is the log's retirement boundary (instruction 4); `cluster replicas
diagnostics` shows `log_entries_since_checkpoint`.

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
own committed floors ([15](15-managed-request-streams.md)).

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
(`LiveEvaluation`: the owner still holds its completion contract), or when
the family exceeds 64 members or 65,536 rows (`TooLarge`). Outcome and
creation-result rows stay: every native sequence keeps its one outcome, and
an exact retry of a retired claim's creation is still answered from it.

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

**The record and its application.** `FOCALRT1` (146 fixed bytes: magic,
version, ledger, the native prefix the family was derived at, the root, the
bundle's content root, its length, the prefix it claims, a digest under
`focal.native.session.retirement-record.v1`) is a session decision like a
layout record ([25 §4](25-parallel-materialization-and-ranges.md)). Only
the authority proposes it (`propose_retirement`), after deriving the family
from its committed core and checking the bundle's claim against it (at
least the family's last event, at most the derived prefix); refused while
candidates are pending, a layout change, a movement step or another
retirement is in flight, or the family is ineligible
(`NativeSessionError::Retirement(refusal)`). While the record is in flight
native admission, layout changes and movement steps answer `Retiring`
(retryable), so the record is never wasted by a later prefix. Applied, the
record is inert when the prefix it named has passed, when a movement is
pending, or when the committed state refuses the family — the same on
every replica; otherwise every replica derives the same family and
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

**The operator's view.** `cluster retention show [--session]`
(`cluster.retention.show`) reads the floor of §3 with the families retired
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
takes its children); `native_session::retirement::tests` (the record
round-trips and refuses every corruption);
`native_session::cluster_tests::committed_retirements_apply_on_every_replica_and_fence_proposals`
(authority-only proposal, refusals before proposal, fencing of native
proposals, layout changes and a second retirement while one is in flight,
identical state and digests on every replica, the continuation and the
outcome, the exact retry of the retired claim's creation, no second
retirement, a lagging follower seeded from a checkpoint with the count, a
restart); `native_session::retention::tests` (the floor's inclusiveness);
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

**The operator.** `cluster gc show` (`cluster.gc.show`) reports the
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

**The view.** `cluster storage show` (`cluster.storage.show`) is one read
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

