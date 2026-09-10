# Native recorded mutations and recovery construction

Status: dormant version 2 implementation, 2026-09-08. The running service still selects
the frozen V1 application format. This document describes the new
[record encoder](../../crates/focal-core/src/native/record_codec.rs), its
allocation-free outer inspector and the construction boundary needed by recovery.
It does not register a live WAL decoder or authorize an upgrade. The activation
and complete recovery requirements remain in [18 §6.14](18-lifecycle-storage-upgrade.md#614-constructing-durable-native-state-without-cloning-or-re-executing-it).

## 1. Record facts, not requests to execute again

The source is one immutable `NativePrepared` and its exact captured write set.
Every inserted or replaced value comes from that candidate's root. Deleted keys
are explicit. Encoding touches those keys only; unchanged descriptors, claims and
history are not copied into the mutation. It does not rerun authorization,
deadline discovery, graph reduction or participant work.

This distinction preserves the two-party contract. The respondent authored its
success or failure testament after attempting the requested work. That record
contains the actual artifacts, including diagnostics and failed work. Claimant
and designated-evaluator checks retain their exact evidence, outcome and original
publication coordinates. Recovery must preserve those facts, not generate new
testimony, choose a new validator or reinterpret a failure as success.

## 2. Envelope

Integers use little endian. IDs and hashes retain their fixed raw bytes. Options
use explicit zero/one tags. Collection lengths and row-body byte lengths are
checked `u32`; ledger, linked-head and cycle scalar counters use checked `u64`.
Rust enum discriminants, `usize` layout and serde ordering are not the format.

The ordered envelope is:

| Field | Encoding |
|---|---|
| Magic | Eight bytes, `FCMUTATE` |
| Version | `u16`, currently 5 (version 3 added the secondary index families of §7 and version 4 the trusted timer index; version 5 orders every row by the storage layout of [25 §3](25-parallel-materialization-and-ranges.md), affinity then family then fields, and the root checkpoint `FCNROOTS` advanced to 5 with it and to 6 when it began to carry the range layout of [25 §4](25-parallel-materialization-and-ranges.md); a record names the producer, never a member) |
| Content profile | `u8`: 0 projection-only; 1 complete authored V1 descriptors |
| Ledger | Tenant ID followed by session ID, 16 bytes each |
| Original range incarnation | 16-byte little-endian `RangeId` |
| Base native sequence | `u64` |
| Exact outcome | Full outcome described below |
| Changed-row count | `u32`, positive |
| Changes | Exactly the declared number, in strictly increasing native key order (the storage layout order of [25 §3](25-parallel-materialization-and-ranges.md) since version 5) |
| Digest | 32-byte BLAKE3 digest of every preceding byte, derive-key context `focal.native.record.v2` |

An outcome contains its ledger, namespaced invocation, native sequence, logical
time, explicit operation tag, intent hash, and ten `u32` change counts: created
claims, changed claims, definitions, evaluations, artifacts, results, receipts,
responses, result testaments and events. Operation tags are explicitly mapped by
[fixed.rs](../../crates/focal-core/src/native/record_codec/fixed.rs); they are
independent of input-command tags.

The outcome's ledger must equal the envelope ledger, and its sequence must be
exactly base plus one. This sequence is not a Raft index: membership and other
control records also consume Raft positions. The original range incarnation
identifies the producing owner; restore must establish its mapping to the newly
constructed owner. Merely matching a range ID and sequence never proves that two
candidate roots share a publication chain.

Invocation namespaces are disjoint:

| Tag | Invocation key |
|---|---|
| 0 | Participant request: principal, epoch, request ID |
| 1 | Evaluation timer: complete evaluation key, timer ID, generation |
| 2 | Claim timer: claim ID, timer ID, generation |
| 3 | Monitor timer: claim ID, monitor ID, timer ID, generation |

An evaluation key includes claim, validation, generation and a tagged Admission,
Increment, Work, MissingSlot or Delivery target with its complete target IDs and
slot where applicable. A result key additionally retains its evaluation revision.

### Complete-root checkpoint envelope

The [checkpoint encoder](../../crates/focal-core/src/native/record_codec/checkpoint.rs)
borrows the actual native Core root and streams every retained row, including
unchanged immutable bodies and older outcomes/events. It shares the row-body
grammar with mutations but has a separate envelope:

| Field | Encoding |
|---|---|
| Magic | Eight bytes, `FCNROOTS` |
| Version | `u16`, currently 6 (version 5 orders rows by the storage layout; version 6 records the range layout of [25 §4](25-parallel-materialization-and-ranges.md)) |
| Content profile | Same explicit profile tags as mutations |
| Ledger | Tenant ID and session ID |
| Original range incarnation | 16-byte little-endian `RangeId`: the producer identity records carry |
| Native prefix | `u64` |
| Complete retained row count | `u64`, checked against decoder limits and host capacity |
| Range layout | A `u32` member count (one to 1,024) and the `u64` layout epoch, then per member its 16-byte little-endian `RangeId` and one flag byte: `0` for the first member, unbounded below, `1` followed by the 16-byte affinity the member starts at for every other; starts strictly ascend and identities are distinct |
| Rows | Strictly ordered puts using the shared typed-key/body grammar, one stream whatever the layout |
| Digest | BLAKE3 of all preceding bytes, derive-key context `focal.native.checkpoint.v2` |

Genesis has prefix zero and no rows. A nonzero prefix requires a nonempty root,
Meta and at least one retained outcome. Checkpoints reject deletions. The outer
inspector enforces these structural conditions and exact row count, without
treating them as proof of complete history or a valid model. Checkpoint and
mutation envelopes cannot be substituted for one another.

Version 2 adds the actual graph capture boundary to recorded claim consequences.
The preceding dormant version 1 did not retain enough information to verify a
graph consequence against its original snapshot: a batch can emit several facts
from one capture, while a monitor or timer can capture again between facts.
The current native inspector refuses the older envelope version explicitly;
it does not guess a capture boundary or relabel old bytes. Neither dormant
version has been activated in Session. The frozen live V1 application, wire,
checkpoint and hash formats are separate and remain unchanged.

The plan allocates no whole-root staging buffer, row index, snapshot or extra
owner. It can write to an exactly sized, caller-funded destination or stream
through `write_with`. The streaming callback coalesces small field writes and
splits large borrowed chunks using a separately funded bounded buffer; it need
not retain the encoded root. Original output errors are returned by value.
Failure may leave a partial prefix to discard before retry. Encoding does not
flush the adapter, publish a checkpoint or acknowledge durability. The caller
must budget both traversals, callback I/O, persistence and checkpoint retention.

The current plan borrows Core during those traversals. Online checkpoint
scheduling from an accounted fixed-prefix pin, including expiry and concurrent
publication, remains part of service integration.
This represents Core's native rows only. Session's membership, placement,
request-stream/retirement metadata and Raft/native-prefix mapping require the
separate enclosing checkpoint and its own validation.

## 3. Changes and complete row bodies

Each change contains a mutation tag (`0` deletion, `1` put), a typed key, a `u32`
body length and the exact body bytes. A deletion has no body. A put has a nonempty
body. A present `MonitorLink(None)` has its own one-byte body; it is a retained
tombstone, not a deleted key. Every record includes the final Meta row and its
exact outcome row. They cannot be deleted. The record's `Event` rows are the
facts its stream deltas are derived from ([23 §6](23-native-activation-and-import.md)):
retained with the prefix, they are the native part of the ledger's delta
history, and no separate delta tail exists for native records.

The 47 key-family tags are explicit (30–33 are the frozen legacy rows an import retains, 23 §5; they never occur in a mutation record; 34–46 are the secondary index families of §7, unit rows whose body is one byte):

| Tag | Family | Tag | Family |
|---|---|---|---|
| 0 | IncomingHead | 15 | Cycle |
| 1 | IncomingLink | 16 | RetiredCycleHead |
| 2 | Monitor | 17 | RetiredCycle |
| 3 | MonitorHead | 18 | Work |
| 4 | MonitorLink | 19 | WorkSlot |
| 5 | MissingResult | 20 | Diagnostic |
| 6 | Meta | 21 | Response |
| 7 | Claim | 22 | ResultTestament |
| 8 | Definition | 23 | ClaimResultTestament |
| 9 | Evaluation | 24 | Outcome |
| 10 | Artifact | 25 | Event |
| 11 | ArtifactIdentity | 26 | ClaimContent |
| 12 | Accepted | 27 | ClaimIdentity |
| 13 | DeliveryResult | 28 | DefinitionIdentity |
| 14 | Receipt | 29 | CreationResult |
| 30 | LegacyTestament | 32 | LegacyRun |
| 31 | LegacyEvidenceSet | 33 | LegacyDefinition |
| 34 | ByIssuer(participant, claim) | 41 | ByArtifactKind(kind digest, artifact) |
| 35 | BySubject(participant, claim) | 42 | BySchema(schema hash, artifact) |
| 36 | ByStatus(status code, claim) | 43 | ArtifactInput(object, artifact) |
| 37 | ByAction(action code, claim) | 44 | ByEvaluator(participant, validation) |
| 38 | ByScope(kind code, key digest, claim) | 45 | ByVerdict(verdict code, result key) |
| 39 | ByRelation(kind code, target claim, claim) | 46 | ByCreated(family code, sequence, object) |
| 40 | ByProducer(participant, artifact) | 47 | DueTimer(logical time, target: claim, evaluation key or claim+monitor) |
| | | 48 | ByObject(family code, object): the identity index of claims, artifacts and declarations, one contiguous listing per family under the storage layout of [25 §3](25-parallel-materialization-and-ranges.md) |

Invocation namespaces are `0` request, `1`–`3` the three timers, `4` the one-time import and `5` a retirement (`Retirement(root claim)`, [26 §4](26-custody-archive-retention-and-restore.md)); operation tags `30` and `31` are `Import` and `Retire`; key family `49` is `Retired(claim)`, the typed continuation of a retired claim (its body: the bundle's content root and length, the prefix it claims, the claim's final binding and status, the sequence the retirement was published at, and the number of event rows that left with the claim); the archive bundle itself is an `FCNARCHV` frame (magic, version `1`, profile, ledger, the prefix it claims, the root, the members, the content roots of its artifacts held as content objects, the roots of the objects sealed for those held inline, the row count, the rows in key order as the checkpoint writes them, a trailing digest under `focal.native.archive.v1`), never restored, only read through `StructuralArchive`; claim event kind `22` is `Imported(legacy sequence)`; a claim body carries its origin byte (`0` native, `1` legacy) after `created`; the Meta row counts legacy rows after creation results. There is no encoded End sentinel. Key payloads and outcome tags are defined in
the explicit fixed-field writer/reader above. The body implementations are
[scalar/index and dispatch rows](../../crates/focal-core/src/native/record_codec/rows.rs),
[claim, evaluation and audit rows](../../crates/focal-core/src/native/record_codec/lifecycle.rs),
[evidence and descriptor rows](../../crates/focal-core/src/native/record_codec/evidence.rs),
and [publication events](../../crates/focal-core/src/native/record_codec/events.rs).

Claim bodies preserve immutable graph, lineage and acceptance declarations;
scalar lifecycle state and terminal cuts; all response links and delivery flags;
scope roots, children, cancellation/rebinding/release history; and independent
evaluation registration membership and seals. Immutable claim/validation/artifact
descriptors retain their complete bodies and original policy/profile information.
Derivable private stamps are reconstructed from the actual restored bodies, never
accepted as replacements for those bodies.

Evaluation and audit bodies preserve receipt, generation, target, handler cursors,
attempts, accepted results, programmatic evidence, suppression, fences and seals.
An audit retains its original members, result capacity, complete result sequence,
capture sequence and publication witnesses. Work and response bodies retain
attachments, diagnoses, authored outcomes, failed-work artifacts and terminal
causes. Pure delivery and missing-target results preserve their absence of an
external attempt or invented artifact.

Response and result-testament owners now retain the actual original Generated
revision. Their immutable ledger/object/content fields are shared within the
same uniquely owned row, so the extra fact costs one scalar rather than another
full binding. Posting, receipt and validation preserve that fact. The native
importer matches it to the original generation event.

Artifact bodies include the immutable descriptor and observed local content-tree
address/revision. They exclude the process-local, request-bound custody token.
Restart or follower replay must independently recover and verify local content,
placement and schema evidence before constructing a local custody capability.
The recorded pointer alone supplies no such capability.

## 4. Bounded encoding and structural inspection

`EncodingPlan::prepare` borrows the candidate and measures exact output bytes and
work. Each row is sized without a staging buffer, then encoded after its length
prefix. Row sizing, fixed-width key comparisons, bounded directory lookups,
descriptor iteration, snapshot hashing, and the record digest share the enclosing
work meter. Measurement and writing each require the quoted allowance.

`write_into` accepts an exactly sized caller-owned slice. A wrong size refuses
before touching the slice. The writer allocates no output buffer, index, row copy
or shared owner. The future WAL/Session integration must reserve that buffer
before allocating it, retain its debit until its real release boundary, and price
it in evaluator/respondent completion promises. Existing key-manifest funding
does not fund encoded bytes.

`StructuralRecord::inspect` bounds total bytes, rows, individual body bytes and
work. It checks the envelope, known key tags, strict key order, put/delete lengths,
Meta/current-outcome presence, exact outcome correspondence, complete consumption
and digest. Its row iterator borrows the original bytes and requires an explicit
work allowance for each scan. A phased decoder must account the total of all
scans and model construction passes.

Row bodies remain opaque at this boundary. A modified body with a recomputed
digest can pass structural inspection. Neither the digest nor an `EncodedRow`
authenticates a producer, validates a lifecycle, proves reference completeness or
permits a recovered root to be published.

## 5. Dependency-ordered construction and remaining integration

The implemented
[phased range builder](../../crates/focal-memory/src/range_hydration_phased.rs)
owns a detached store. Consuming phase operations accept strictly ordered,
insert-only plans with exact row counts; successive phases may differ from
canonical key order. A scoped lookup can read earlier phases and earlier staged
rows. The engine reserves each final row allowance before invoking its builder,
checks actual capacities before building the next row, and stages only one
bounded chunk. Source/decoder workspace and callback work remain separately
accounted. A refused phase drops the entire detached import.

The final validator receives only a borrowed complete view. It cannot obtain an
owner, snapshot or pin. The verified native prefix is bound and the new owner
returned only after exact cardinality and validation succeed.

The [native checkpoint adapter](../../crates/focal-core/src/native/record_codec/recovery.rs)
now connects complete body readers to this construction boundary. Its executed
checkpoint qualification is recorded in
[09](09-implementation-status.md#native-checkpoint-restoration--2026-09-07).
The subsequent incremental replay qualification is recorded separately in
[09](09-implementation-status.md#native-incremental-replay-and-restored-owner-qualification--2026-09-08).

### Native checkpoint restoration

`recovery::restore` takes an inspected checkpoint, a fresh range incarnation,
node-derived construction/work limits, the owner memory budget, and the actual
local content store/schema verifier. It preserves the original content profile
and native prefix. It returns the sole restored Core only after complete-root
validation; no intermediate claim, evaluation, or owner is externally visible.

The [scoped source API](../../crates/focal-memory/src/range_hydration_sources.rs)
allows dependency-aware quotation before the engine reserves final payload
memory. Its native plan retains raw bytes, the dependency lookup, and the quote.
Build repeats preparation against that same immutable source under cumulative
work allowances, then constructs the final owned row. Mutable descriptor input
plans remain scoped inside each pass; no self-referential parser/plan or shared
per-object owner is necessary. Source preparation may run again at a page-byte
boundary; every such pass is charged.

Construction uses eight internal phases:

| Phase | Rows restored | Dependencies available afterward |
|---|---|---|
| 0 | Scalar metadata and indices, receipts/cycles, outcomes/events, creation mappings | Original publication coordinates and identity allocations |
| 1 | Immutable claim content and validation declarations | Actual authored bodies and definition stamps |
| 2 | Artifacts | Verified local evidence, immutable descriptors and provenance |
| 3 | Work artifacts and diagnostics | Original output/error evidence and retained failure state |
| 4 | Independent evaluations and accepted/delivery/missing results | Exact target checks and retained result publications |
| 5 | Respondent testaments | Original Generated binding, outcome, evidence manifest and delivery/validation state |
| 6 | Claims, scope registries and registration sets | Complete response history and exact independent membership |
| 7 | Claimant result testaments | Frozen sealed cohorts and original publication witnesses |

This is storage dependency order, not a required participant workflow. Claim
receipt does not generate testimony. A respondent's explicit closure still
records success or failure, including errors as artifacts, before its separate
posting and claimant receipt. Evaluations, evidence objects and claims retain
their own recorded progression.

Work and response hydration need acceptance policy before the complete claim can
exist, because complete claim history itself refers to restored responses. A
temporary funded index therefore keeps borrowed original claim-body spans and
artifact publication origins. Policy preparation resolves actual declarations;
the resulting temporary policy has its own reservation and drops before that
reservation. The eventual claim owns its final policy once. Index construction
uses bounded batches in the existing paged store and drops the index before
returning the owner. It does not copy descriptor bodies or model objects.

Artifact restoration is a read-only ContentStore operation. It checks the
original request and producer, local revision, complete existing content tree,
exact pointer or inline payload correspondence, and pinned schema. It reserves
verification scratch before reading and hashing. Missing content refuses the
import; inline bytes in a descriptor cannot silently create the missing durable
tree. The returned genuine custody token moves under the final row reservation.

Complete-root validation checks counts, identities, registration and response
membership, receipts/cycles, linked indices, original positions, independent
evaluation/result history, and frozen audit coverage. Outcome sequence coverage
uses a funded bitmap, including zero-event outcomes. Object history uses a
separately funded scalar index of keys and publication coordinates; it carries
no event bodies or decoded objects. This avoids full-history scans for every
object. Its cardinality, allocation capacity, sorting and traversal are bounded,
and its entire temporary peak participates in recovery memory admission.

Parsing, model work, source callbacks and dependency lookup use separate
cumulative allowances across all phases and both preparation/build passes.
An opaque cursor or model inspection exclusively borrows its offered work
allowance; nested use of that same meter cannot spend it concurrently. Known
model work can instead be debited in advance, leaving a separate remainder for
source callbacks. Refused inspections retain their consumed work charges.
Failure retains its original cause while dropping provisional rows, pages and
scratch. The byte buffers containing the checkpoint remain caller-owned and
must stay funded until restoration ends. No new per-object Arc is introduced;
the existing range/page and budget ownership mechanisms remain in use.

A checksum and internally coherent rows do not establish checkpoint provenance
or prove that every past operation was authorized. Final scope state does not
encode every historical root set or graph snapshot, so the importer cannot
recompute every prior SCC choice or predicate fingerprint from final rows alone.
It checks retained witnesses and references; the enclosing Session must establish
the trusted checkpoint/log chain and original-to-fresh incarnation mapping.
Restoration never invents missing historical roots or reruns today's authority
to fill that gap.

### Incremental native replay construction

The current [replay implementation](../../crates/focal-core/src/native/record_codec/replay.rs)
prepares one recorded mutation against the actual preceding Core. Its successor
validation, replay and restored-owner component checks are qualified in
[09](09-implementation-status.md#native-incremental-replay-and-restored-owner-qualification--2026-09-08).
It is not registered with the running Session/WAL decoder.

The enclosing recovery chain supplies the expected original range incarnation.
Replay requires the recorded ledger, content profile, source incarnation and
base prefix to match before construction. The returned candidate uses the
current local Core incarnation; the original header and hash remain properties
of the caller-owned log record. Publication still requires that same preceding
root. This API never replays the participant command or uses current authority
to reinterpret an old action.

One canonical buffer retains the bounded mutation's borrowed encoded rows and
their owned decoded values. A changed key takes precedence over the old root
even while its row has not been built; a pending or deleted key cannot silently
resolve to its predecessor. Unchanged acceptance policies are borrowed from the
actual preceding claims. Changed claim policies are prepared from their original
recorded bodies, with temporary construction funded separately.

The same eight dependency phases construct the changed rows. A transaction-local
scalar event index supplies original artifact publication coordinates and object
history within this record. It does not copy event bodies or index the complete
ledger. Existing content must pass the same read-only custody verification as
checkpoint restoration. All row heaps share one transaction accounting owner;
there is no retained accounting handle or reference-counted owner per row.

Successor validation must preserve the already-validated base, reconcile exact
metadata and outcome deltas, and check new event chains, indices, aggregates,
seals and audit coverage against affected objects. Resumed validation cursors
borrow their immutable declaration and retain their actual previous attempt,
failure, programmatic evidence, suppression and seal state. They do not rescan
every historical attempt or invoke a handler.

Graph consequences retain `NativeGraphCapture { before_ordinal }`. That boundary
includes precisely the current mutation's events with smaller ordinals, together
with the already validated base. Facts produced by one frozen graph evaluation
share its capture boundary; a subsequent recapture records its own boundary.
The deadlock cut already retains the original trigger binding, separately from
the selected victim, so replay does not introduce another trigger identity.
Canonical graph verification must use that exact capture, including original
path or strongly connected component witnesses. Searching older prefixes for
any matching fingerprint would admit a stale witness and is forbidden.
Control captures include every original creation/cancellation/posting root,
including disconnected correction successors. Monitor releases are checked
against the active roots before their event; timer consequences require the
selected monitor to remain active with an unsettled predicate at its capture.
Ordinary dependency and satisfaction propagation also occurs without monitors.
A new claim that immediately fails retains its original empty registration
membership before the independent cohort-seal suffix.

After validation, rows move into the existing range preparation path. The
captured write set and final candidate remain separate funded owners; only
retained neighbors of touched pages are copied. Decoder buffers, row heaps,
construction workspace and new pages remain charged through their handoffs.
Range preparation accepts the already held, exact input permit from that same
budget and lane. Its drop-ordered owner keeps rows funded on every failure;
the incoming vector and payloads do not incur a second admission charge.
Every refusal must leave the preceding root and its pinned reads unchanged.
The record format can express deletion, but current native lifecycle operations
retain their rows and history; physical deletion requires a separately defined
retention operation and its successor invariants.

Native integration still requires:

1. Activation of the durable native Session inside the running service next to
   the ancillary Session protocols, populated V1 import, and replicated decoder
   activation with membership fences (18 §3–5).
2. Retention/deletion with its own operation and successor invariants; no
   current native lifecycle operation physically deletes retained history.
3. Shared live CLI/MCP dispatch of native operations and the two-participant
   restart acceptance gate.

The durable log chain, incarnation mapping, online checkpoint ownership,
encoded-buffer funding and the exact record-hash/ticket/Raft-index/native-prefix
mapping are implemented by the Session contract in section 6.

The same physical checksummed WAL and durable Ready publication machinery remain
the intended persistence layer. This format adds the missing native application
facts; it does not replace that machinery or silently activate an incomplete
decoder.

`FCNSESS1` version 2 records `records_floor` after the recording term: the native prefix that holds no record (zero at genesis, one after an import). A recording range exists exactly when the prefix has advanced past it. Version 3 follows it with `activation_index`, the Raft index of the committed activation record (never zero, never beyond the applied index), so a restored replica reports the exact activation position rather than a bound derived from the sealed legacy prefix.

Version 4 adds one **form** byte after the membership configuration, before the Core byte count and hash: `0` is the inline form (the Core root follows, as before); `1` is the seeded form, where a chunk table replaces the root — a `u32` chunk count, then per chunk a 32-byte BLAKE3 hash and a `u32` length — and the root itself lives as content-addressed seed chunks of at most 1 MiB in the node's seed store ([25 §5](25-parallel-materialization-and-ranges.md)). The table must agree with the byte count (one chunk per started MiB) and stay within the assembled-root bound; any other form byte is refused. The checksum trailer covers the manifest as it covers an inline frame, and the root's own hash is recorded in both forms, so an assembled root is verified exactly as an inline one.

Ancillary byte 0 of the same version says whether a **movement section** follows the membership configuration (before the form byte): `1` means a `u32` length and that many bytes of the range coordinator's postcard-encoded `RangeCheckpoint` ([25 §6](25-parallel-materialization-and-ranges.md)), at most `Limits.movement_bytes` (1 MiB) and never empty; `0` means none, and the other five ancillary bytes stay reserved (any other value is refused). Both forms carry it, so a restored replica resumes a transfer from the committed step.

## 7. Secondary index families

Bounded lists need an ordered key range per predicate; scanning a primary
family and filtering would make every list cost the family, and the node's
trusted timers need the due deadlines in time order. The native store
therefore keeps fourteen secondary index families (tags 34–47) as
ordinary rows of the same range, recorded, replayed, checkpointed and
restored like every other row
([index_rows.rs](../../crates/focal-core/src/native/index_rows.rs),
[index_scan.rs](../../crates/focal-core/src/native/index_scan.rs)). An index
row is a unit value: its key is the whole fact, its body one byte, its heap
charge zero. Every index row is derived from exactly one primary row:

| Primary row | Index rows it implies |
|---|---|
| Claim (at creation) | `ByObject(claim family)`, `ByIssuer`, `BySubject`, `ByCreated(claim family)`, `ByStatus`; with authored content also `ByAction`, one `ByScope` per scope (kind code and the BLAKE3 digest of the key under `focal.native.index.scope-key.v1`) and one `ByRelation` per relation whose target is a claim or exact evidence (the target column holds that claim's or artifact's identity) |
| Claim (status change) | delete `ByStatus(old)`, put `ByStatus(new)` |
| Artifact | `ByObject(artifact family)`, `ByProducer`, `ByArtifactKind` (digest of the kind under `focal.native.index.artifact-kind.v1`), `BySchema`, one `ArtifactInput` per cited input |
| Definition | `ByObject(validation family)`, one `ByEvaluator` per designated principal (the issuer of a delivery program, otherwise the check evaluator and any distinct quality evaluator) |
| Accepted result | `ByVerdict` |
| Claim with a deadline | `DueTimer(at, Claim)` until the claim's timer has been delivered (its outcome row exists); a terminal transition leaves it, so the timer fires once on the terminal claim and retires the row |
| Monitor (a scope of its claim row) | `DueTimer(at, Monitor)` while the scope is active and its timer has not been delivered |
| Evaluation | `DueTimer(at, Evaluation)` with the declaration's deadline while the evaluation is neither terminal nor fenced and its timer has not been delivered |

Artifacts, definitions and testaments carry no creation row: their primary
key ranges are already ordered, and a time-ordered read goes through the
event log. Two families change after creation. `ByStatus` moves when a
claim's status moves; the deletion of its old key is an ordinary delete
change. A `DueTimer` row is retired by the transition that settles its
timer (a fenced or terminal evaluation, a released or cancelled monitor) or
by the timer's own delivery: consumption is the retained outcome row of the
timer's invocation, which every reader (leader, replay, checkpoint) can see,
so a delivered timer whose target row did not change still carries the
deletion of its row. The node's sweep
([native_timers.rs](../../crates/focal-node/src/native_timers.rs)) scans
`DueTimer` rows due at its logical time, reads each timer's identity from the
primary row and delivers at most sixty-four per tick; a redelivered timer is
an exact retry that resolves to its recorded outcome, and a restart needs no
memory because the next sweep rescans the index (doc 17 §11).

The derivation is one function shared by three readers. The leader derives
the index changes from the exact primary rows a plan writes, before ownership
moves, and records them beside the primary changes
([original_plan.rs](../../crates/focal-core/src/native/original_plan.rs)).
Replay rederives every index put and delete from the replayed primary rows
and requires the record to carry exactly those changes, and requires every
changed claim, new artifact, new definition and new accepted result to be
covered by every change its derivation yields
([replay_validate_index.rs](../../crates/focal-core/src/native/record_codec/replay_validate_index.rs)).
Checkpoint validation requires every retained index row to rederive from the
primary row it names and every retained primary row to be covered
([read_validate_index.rs](../../crates/focal-core/src/native/record_codec/read_validate_index.rs)).
The import image writes the same rows for translated claims and artifacts
([import.rs](../../crates/focal-core/src/native/import.rs)), so an imported
prefix validates under the same rules. Index rows restore in phase 0 and live
in their own page partition, so their pages never mix with heap-bearing rows.

Index rows are funded like every other row. Each operation's construction
ceiling includes the most index rows it can write: two status rows per
changed claim, the artifact and verdict rows of a report, the artifact rows
of a diagnostic, the batch itself for a creation, and the due timers it can
change (one per created claim, per registered evaluation and per monitor
registration; for a report its own evaluation's, every sealed cohort
evaluation's and every graph consequence's; for a timer its own consumption)
([prepare_budget.rs](../../crates/focal-core/src/native/prepare_budget.rs));
a write envelope admits those timer rows as deletions beside the status
moves.
Completion and respondent promises quote them in their write envelopes and
record buffers, so a promised report or diagnostic is never short of rows
when it arrives. Two consequences are recorded rather than hidden. First,
the number of inputs a promised artifact may cite is bounded by
`NativeLimits::artifact_inputs` (default 16, never above the model's ceiling
of 64) and narrowed further when the batch leaves no room, so a small batch
narrows the promise instead of refusing every report. Second, every changed
key is priced by the range layer as a possible page copy, so an operation's
retained promise grows with its index rows; the range envelope prices a
deletion by the deleted entry's declared heap (`deleted_heap`), which is
zero for an index row, rather than by the largest entry the range admits.
A creation of `n` definitions writes roughly `3n + 8` rows; the standard
session batch of 256 therefore admits about 80 definitions per creation.

Lists over these families are stateless on the node
([native_lists.rs](../../crates/focal-node/src/native_lists.rs)): the filter
selects one indexed predicate (a relation or scope, then a participant, then
the action or status family, then creation order; an artifact's cited input,
then producer, schema and kind; a verdict for evaluations), the rest filter
residually within the caller's visit allowance, and the continuation names
the last visited row rather than the last match, so an empty page may still
continue and only an absent cursor ends a list. A cursor carries a keyed
BLAKE3 digest over the ledger, principal, route epoch and exact filter under a
per-incarnation node key; a cursor reused under another filter or principal,
tampered with, or issued by a previous incarnation is refused rather than
repositioned. Because every family but `ByStatus` is written once, a cursor
stays valid while the prefix grows: a later page sees later rows at a later
prefix, which the page's token names.

## 6. Durable Session contract

[NativeSession](../../crates/focal-ledger/src/native_session.rs) binds this
record format to one Raft group through the consensus replica. Its contract:

1. **One decoder identity.** The enclosing session checkpoint descriptor hash
   ([native_checkpoint.rs](../../crates/focal-ledger/src/native_checkpoint.rs))
   names the session envelope (`FCNSESS1`), the input frames (`FCNINPUT1`), the
   recorded mutations (`FCMUTATE3`) and the root checkpoints (`FCNROOTS3`)
   together. The session confirms exactly that hash as the group's durable
   decoder floor and refuses a log whose floor differs.
2. **Genesis is a committed fact.** The first native-domain entry a leader
   proposes is `FCNGENES1`
   ([native_session_genesis.rs](../../crates/focal-ledger/src/native_session_genesis.rs)):
   cluster, group, ledger, content profile, decoder hash and the derived genesis
   digest. Followers validate it against their physical identities; no mutation
   is admitted or applied before the genesis is applied; the enclosing checkpoint
   retains it in its activation. A supported decoder hash alone never binds a
   ledger to a group.
3. **The native prefix is not the Raft index.** Every delivered `NativeCommit`
   carries the Raft index and term, the record hash and the outcome. Each
   mutation advances the native sequence by one; genesis, layout records
   (`FOCALRG1`, [25 §4](25-parallel-materialization-and-ranges.md): 115
   fixed bytes naming the ledger, the layout epoch a split or merge applies
   to, the operation and a digest under `focal.native.session.layout-record.v1`),
   movement records (`FOCALRM1`, [25 §6](25-parallel-materialization-and-ranges.md):
   magic, version, ledger, the control ordinal the record expects to be, a
   `u32` length and the postcard-encoded range operation, then a digest under
   `focal.native.session.movement-record.v1`; at most 64 KiB) and retirement
   records (`FOCALRT1`, [26 §4](26-custody-archive-retention-and-restore.md):
   146 fixed bytes naming the ledger, the native prefix the family was derived
   at, the root claim, the bundle's content root and length, the prefix the
   bundle claims and a digest under
   `focal.native.session.retirement-record.v1`; applying one advances the
   native sequence by one through the outcome the core publishes),
   membership and Raft no-ops advance only the Raft prefix. Membership and native entries are merged
   by Raft index during delivery, so the configuration index never exceeds the
   applied index at a retained cursor.
4. **Suffix disposition needs evidence.** Unresolved candidates are discarded
   only on one of four proofs: the committed head matches (publish), a different
   committed native record at the candidate's base (conflicting prefix), an
   installed authoritative snapshot, or a fully applied entry of a newer term
   (barrier). A role change alone only marks the owner for reconstruction.
5. **Reads correlate.** Callers supply a 16-byte correlation; the readiness
   barrier of each term uses a reserved internal context. A read boundary names
   the applied Raft index and native sequence at which the read may be served.
6. **Failure classes.** Memory and capacity refusals, missing custody, consensus
   staging and persistence-pending conditions are retryable and retain the
   candidate or delivery. Authority loss retains candidates for suffix
   disposition. Corruption, format and contract violations fail closed until the
   log is reopened; a refused consensus staging reservation before any Raft state
   is taken is retryable, never fatal.
7. **Incarnations.** A replica that installs an authoritative snapshot produces
   later records under a derived incarnation of the genesis, node, snapshot index
   and term, never under the checkpoint's own producer range. The recorded
   producer of committed history is unchanged by restarts.
8. **Admission promises.** Admission reserves RAM for report construction, the
   encoded record buffer and retained pages, never disk, quorum or fan-out. A
   configurable free-space watermark on the WAL filesystem refuses fresh
   candidates before any in-memory acknowledgement; committed work is still
   answered from the root without disk.

Evidence: the one-node disk-backed
[workflow](../../crates/focal-ledger/src/native_session_workflow_tests.rs) and
[session](../../crates/focal-ledger/src/native_session_tests.rs) suites, and the
three-voter [cluster suite](../../crates/focal-ledger/src/native_session_cluster_tests.rs)
covering follower replay equality, barrier and conflict proofs, crash after
commit before reply, snapshot catch-up and planned handover, correlated read
barriers, missing custody, memory pressure and corrupted record bytes in
transit. The record bound that funds every promise is proven per row family in
[bound_tests.rs](../../crates/focal-core/src/native/record_codec/bound_tests.rs).

The unified Session hosts this engine after a committed activation record;
[23](23-native-activation-and-import.md) records the field matrix, the
activation protocol, the `FOCALSS6`/`FOCALSS7` envelopes and the import design.

## 8. The backup manifest (`FCLBKUP1`)

A backup directory ([26 §6](26-custody-archive-retention-and-restore.md)) is `MANIFEST`, `checkpoint` (the exact `FOCALSS7` envelope), `seeds/<hash>.seed` (the chunks of a seeded Core root, named and verified as the seed store names them) and `content/<root>.manifest` plus `content/<hash>.chunk` (each object's manifest verbatim and its chunks, named as the content store names them). `MANIFEST` is the 8-byte magic `FCLBKUP1`, a postcard body and a 32-byte BLAKE3 trailer over magic and body; the body is schema `1`, the creation time (unix ms), the evidence prefix (cluster, ledger, log group, placement genesis, node, legacy sequence, Raft index and term, route, placement and membership epochs, operation, placement digest, envelope hash and length, artifact count), the native prefix, the activation genesis, the profile byte, the content domain, the decoder pair (predecessor, successor), the membership configuration, the envelope hash and length again, the seed chunks (hash, length) in table order, the objects sorted by root (root, length, class, chunks as hash and length) and the retention section's `archived_through` and `retired_families`. A manifest is decoded only whole and validated field by field; it is written last and a directory without one is not a backup. The format is pre-release and may still change with a version of its magic.

