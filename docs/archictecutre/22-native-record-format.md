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
| Version | `u16`, currently 2 |
| Content profile | `u8`: 0 projection-only; 1 complete authored V1 descriptors |
| Ledger | Tenant ID followed by session ID, 16 bytes each |
| Original range incarnation | 16-byte little-endian `RangeId` |
| Base native sequence | `u64` |
| Exact outcome | Full outcome described below |
| Changed-row count | `u32`, positive |
| Changes | Exactly the declared number, in strictly increasing native key order |
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
| Version | `u16`, currently 2 |
| Content profile | Same explicit profile tags as mutations |
| Ledger | Tenant ID and session ID |
| Original range incarnation | 16-byte little-endian `RangeId` |
| Native prefix | `u64` |
| Complete retained row count | `u64`, checked against decoder limits and host capacity |
| Rows | Strictly ordered puts using the shared typed-key/body grammar |
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
exact outcome row. They cannot be deleted.

The 30 key-family tags are explicit:

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

There is no encoded End sentinel. Key payloads and outcome tags are defined in
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

1. Integrate the qualified restored-Core/`NativeOwner::with_schemas` path into
   service startup before accepting new traffic. Evaluator and respondent RAM
   credit recovery is covered by component pressure tests; complete service
   recovery and durable completion-buffer funding remain required.
2. Connect the qualified exact-predecessor mutation application to the trusted
   durable log chain and map original range incarnations into restored owners.
   Retention/deletion still needs its own operation and invariants; no current
   native lifecycle operation physically deletes retained history.
3. Online checkpoint ownership and the enclosing Session checkpoint metadata;
   encoded-buffer funding and the exact record-hash/ticket/Raft-index/native-prefix
   mapping. Publish committed heads only and discard only resolved suffixes.
4. Replicated decoder activation, compatible membership fences, restart/fault
   qualification and shared live CLI/MCP dispatch.

The same physical checksummed WAL and durable Ready publication machinery remain
the intended persistence layer. This format adds the missing native application
facts; it does not replace that machinery or silently activate an incomplete
decoder.
