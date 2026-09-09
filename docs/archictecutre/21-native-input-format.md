# Native input representation and bounded decoding

Status: dormant input encoder, structural inspector and bounded typed construction,
2026-09-07. This is an
implementation format for the native RAM owner. It is not registered in the
transport, advertised as a decoder, written to the WAL or selected by CLI/MCP.
The live service continues to use V1. The durable successor and ordered activation
remain governed by [18](18-lifecycle-storage-upgrade.md).

## 1. Boundary and ownership

The [input codec](../../crates/focal-core/src/native/input_codec.rs) represents all
28 native actor commands and the three separate timer namespaces. It retains the
complete authored claim, validation instruction, artifact and response fields;
an opaque hash cannot replace those bodies. It neither runs a validator nor
creates respondent testimony upon receipt. `CloseResponse` carries the
respondent's explicit summary, reported outcome, output manifest and diagnostic
references, including failure evidence. Claimant audit-testament operations remain
distinct from that respondent operation.

`EncodingPlan::prepare` traverses an immutable borrowed source using a counting
sink. It checks field widths, byte/work limits, the creation/profile match and
legacy acceptance/declaration correspondence. The resulting plan holds the same
borrow for `write_into`, which writes into caller-owned storage. The destination
must have the exact quoted length; a different length refuses before modifying
it. The measured and writing paths share the same encoder and visit accounting.
The codec allocates no output buffer or per-object Arc.

`StructuralInput::inspect` borrows the complete frame. It checks framing, closed
tags, descriptor schema versions, lengths, UTF-8 and aggregate resource limits,
and requires exact consumption. Its header and quote are observations of those
bytes. Inspection produces no `NativeInput`, semantic stamp, custody proof, retry verdict
or owner funding capability. Zero IDs, invalid model relationships, duplicate
identities, noncanonical sets and forged content pins require the subsequent
semantic pass. Successful structural inspection is insufficient for admission.

For the 18 actor commands with no dynamic fields and all three timer namespaces,
[`decode_fixed`](../../crates/focal-core/src/native/input_codec/fixed.rs) can then
construct the actual typed input on the stack, without heap allocation. It checks
the complete frame again under a separate visit limit. For these fixed fields,
the structural visit quote is also the exact typed-pass quote; a complete two-pass
operation must fund both passes. The resulting `FixedFrame::as_frame()` exposes
the typed input to the existing encoder without copying or changing its native
intent algorithm. The normal owner still checks semantics, ledger/profile,
authentication, exact retries and state-dependent authority.

Dynamic tags `0, 4, 6, 7, 9, 13, 15, 19, 24, 27` return `None` from this fixed-field
method after structural inspection, without a second pass. All ten have separate
borrowed construction plans in §6, completing typed construction for all 28
actor commands. Owner-selected funding remains under §7. Unknown
or malformed commands cannot use that result to bypass inspection. There is no
fallback to another decoder or implicit empty object construction.

The format includes an authored request principal; authentication must separately
establish that principal. Trusted logical/firing time, custody attestations,
assigned creation cuts, assigned receipt epochs, transition proofs and completion
loans are not input fields. A timer namespace in untrusted bytes grants no timer
authority. The envelope ledger/profile must match the selected owner before any
retained-request lookup or fresh admission.

## 2. Primitive representation

All integers use fixed-width little endian. IDs occupy 16 bytes and hashes 32
bytes. Arrays start with a `u32` item count. Strings start with a `u32` byte count
and contain UTF-8; blobs have the same length prefix and opaque bytes. Empty
strings/arrays remain representable structurally and are subject to their model
rules. No padding, alignment requirements, native pointer widths, enum-memory
layouts or serde defaults enter the format.

Options and booleans use exactly `0` or `1`; an option's payload follows `1`.
Unknown variants refuse. The frame has no embedded checksum, authentication,
compression or total-length prefix. Its caller supplies the bounded complete
frame; transport/durable envelopes remain separate work.

The checked [cursor and sinks](../../crates/focal-core/src/native/input_codec/bytes.rs)
use fallible slice access and checked arithmetic. Each primitive byte operation
charges one visit plus its byte length, including an empty operation. Count
validation costs one extra visit; UTF-8 inspection costs one plus text length in
addition to reading those bytes. This is a deterministic work allowance, not an
instruction or elapsed-time measurement. Encoding and inspection have separate
quotes because their work differs. Neither path resets its visit counter for a
nested object.

Inspection has five caller limits: total frame bytes, visits, array items, text
bytes and blob bytes. Array items share one allowance across every nested array;
text bytes share one across every string; metadata and inline payloads share the
blob allowance. Runtime scope capacities are scalar values, not decoded array
counts. Array-count checks debit before iteration. Byte bounds also constrain
fixed fields and length prefixes.

## 3. Header and namespaces

The common header is exactly 44 bytes:

| Offset | Field | Representation |
| --- | --- | --- |
| 0 | Magic | Eight ASCII bytes `FCNINPUT` |
| 8 | Format version | `u16`, currently `1` |
| 10 | Native content profile | `u8`: ProjectionOnly `0`, AuthoredV1 `1` |
| 11 | Namespace | `u8`: actor `0`, evaluation timer `1`, claim timer `2`, monitor timer `3` |
| 12 | Ledger tenant | ID |
| 28 | Ledger session | ID |

Actor frames append request principal ID, request epoch `u64`, request ID, then
one command byte at offset 84 and its body. Request identity remains the native
intent algorithm; these encoded bytes do not redefine it. ProjectionOnly refuses
`CreateAuthored`; AuthoredV1 refuses `Create`. Other command tags share both
profiles, with ordinary owner validation still required.

Timer bodies are respectively `(EvaluationKey, Deadline)`, `(claim ID, Deadline)`
and `(claim ID, monitor ID, Deadline)`. A Deadline is `(timer ID, generation u64,
at u64)`. The authored `at` is included even though the timer invocation key
does not contain it. Actual delivery time comes from the trusted owner.

## 4. Commands

The [encoder](../../crates/focal-core/src/native/input_codec/encode.rs) maps variants
explicitly. Numbers do not depend on Rust enum discriminants or V1 vocabulary
codes. Within each table row, fields occur in the order shown. `[]` means a
counted array; `?` means an explicitly tagged option.

| Tag | Operation | Body |
| --- | --- | --- |
| 0 | Create | `[Projection]`, `[Declaration]` |
| 1 | Cancel | expected Binding |
| 2 | Post | expected Binding |
| 3 | BeginAdmission | claim Binding, EvaluationKey, expected Binding |
| 4 | ReportAdmission | claim Binding, EvaluationKey, expected Binding, Report, Artifact |
| 5 | AcquireReceipt | expected Binding, new receipt ID |
| 6 | SubmitWork | claim Binding, slot `u32`, Artifact |
| 7 | SubmitDiagnostic | claim Binding, Failure, Artifact |
| 8 | ReceiveWork | claim Binding, expected artifact Binding |
| 9 | CloseResponse | claim Binding, response Binding, summary string, Confidence, Outcome, `[(slot u32, ArtifactRef)]`, `[diagnostic ArtifactRef]` |
| 10 | PostResponse | claim Binding, expected response Binding |
| 11 | ReceiveResponse | claim Binding, expected response Binding |
| 12 | FailWorkProduction | claim Binding, slot `u32`, diagnostic ArtifactRef |
| 13 | RejectWork | claim Binding, expected artifact Binding, Failure, diagnostic Artifact |
| 14 | BeginIncrement | claim Binding, EvaluationKey, expected Binding |
| 15 | ReportIncrement | claim Binding, EvaluationKey, expected Binding, Report, Artifact |
| 16 | SealIncrementTargets | claim Binding |
| 17 | EnterWholeWork | claim Binding, expected response Binding |
| 18 | BeginWork | claim Binding, EvaluationKey, expected Binding |
| 19 | ReportWork | claim Binding, EvaluationKey, expected Binding, Report, Artifact |
| 20 | GenerateResultTestament | claim Binding, new testament ID |
| 21 | PostResultTestament | expected testament Binding |
| 22 | AdoptReceipt | expected Binding, previous ReceiptFence, new receipt ID, new holder ID |
| 23 | ReleaseScope | expected Binding |
| 24 | RegisterMonitor | expected Binding, ReceiptFence?, monitor ID, `[WaitPredicate]`, Deadline |
| 25 | RebindMonitor | expected Binding, ReceiptFence?, monitor ID, predecessor Binding, successor Binding |
| 26 | CancelMonitor | expected Binding, ReceiptFence?, monitor ID |
| 27 | CreateAuthored | `[(Claim, [Validation], max_responses u32, ScopeLimits, Owner?)]` |

Confidence uses Hint `0`, Tentative `1`, Committed `2`, Consensus `3`. Reported
Outcome uses Complete `0`, Partial `1`, Refused `2`, Impossible `3`, Interrupted
`4`, Failed `5`. None is a ledger satisfaction assertion. Failure uses Work `0`,
Production `1`, Structure `2`, Metadata `3`. WaitPredicate uses Satisfied `0`,
Terminal `1`, Released `2`, each followed by a claim ID.

## 5. Shared fields and complete descriptors

[Shared field writers](../../crates/focal-core/src/native/input_codec/types.rs)
define these structures:

- Binding: ledger, object ID, content hash, revision `u64`.
- ArtifactRef: artifact ID, content hash. ObjectRef: ledger, family `u16`, object
  ID. Families are Claim `1`, Testament `2`, Validation `3`, Artifact `4`.
- ReceiptFence: receipt ID, epoch `u64`. Owner: expected Binding, ReceiptFence?.
- ScopeLimits: scopes, roots and children, each `u32`.
- Attempt: phase `u8`, index `u32`, handler ID, handler-version hash, evaluator
  ID, external-definition hash. Phases are Programmatic `0`, Quality `1`,
  Delivery `2`, MissingTarget `3`.
- Report: generation `u64`, Attempt, verdict `u8`, ArtifactRef. Verdicts are
  Pass `0`, Fail `1`, Incomplete `2`, Error `3`.
- EvaluationKey: claim ID, validation ID, generation `u64`, lookup target.
  Targets are Admission `0` with no payload; Increment `1` with artifact ID;
  Work `2` with response ID, slot `u32`, artifact ID; MissingSlot `3` with response
  ID, slot `u32`; Delivery `4` with response ID.
- Full validation Target in artifact result provenance has its own tags:
  Artifact `0` carries response Binding, slot `u32`, artifact Binding; MissingSlot
  `1` carries response Binding, slot; Delivery `2` carries response Binding;
  Admission `3` carries claim Binding; Increment `4` carries claim and artifact
  Bindings. It cannot be replaced by the lookup target.

The [descriptor writers](../../crates/focal-core/src/native/input_codec/descriptors.rs)
encode these exact body orders:

| Body | Ordered fields |
| --- | --- |
| Claim | ledger, own ID, schema `u16`, occurrence ID, description, relations, scopes, requirement pins, slots, Deadline? |
| Declaration | Binding, common declaration fields |
| Validation | ledger, own ID, schema `u16`, common declaration fields, description, quality text?, contributor IDs, policy revision `u64` |
| Legacy Projection | Binding, issuer ID, subject ID, Deadline?, max responses `u32`, graph edges, lineage Binding, cause, corrections, acceptance Binding, acceptance issuer ID, slots, ScopeLimits, Owner? |
| Artifact | ledger, own ID, schema `u16`, kind string, external schema hash, metadata blob, payload, producer ID, ReceiptFence?, result provenance?, work provenance?, input ObjectRefs, visibility strings |

Descriptor schemas currently accept exactly `1`. Derived descriptor content
hashes, declaration stamps, aggregate attempt counts and roles must be recomputed,
not accepted as replacement proofs. Artifact and standalone creation-body
preparation now derive their identities from the actual borrowed body; complete
creation-frame decoders check local cohort correspondence as described in §6.2. A legacy
Declaration retains its actual supplied Binding. External definition, schema,
version, policy and requirement-specification pins remain explicit hash fields.

Claim relations are `(kind u16, tagged target)`: target Participant `0` + ID,
Object `1` + ObjectRef, Action `2` + action `u16`, Root `3` + ID, Evidence `4` +
artifact ID + descriptor hash (an exact committed artifact; descriptor schema 2
only, cited by `reviews` and `derived_from`). Relation kinds
`1..15` are Issuer, Subject, Evaluator, ClaimAction, Supersedes, DependsOn, Awaits,
CausedBy, Refines, ConflictsWith, DerivedFrom, Reviews, Amends, ContributedBy and
Invalidates. Actions `1..10` are Work, Consultation, Challenge, Feedback, Approval,
Summon, Handoff, Evaluation, Correction and Teardown. Scope entries are
`(kind u16, key string)`, with File, Symbol, Api, TestSurface, Component and
UxSurface numbered `1..6`. Requirement pins are `(validation ID, specification
hash)`. A slot is `(index u32, missing index u32, mode, checks)`; each check is
`(declaration index u32, validation ID, mode)`. Mode is Required `0` or Observe `1`.
Empty check arrays preserve the explicit slot's delivery obligation.

In descriptor schema 1 the role relations (issuer, subject, claim action and
cause) are derived by the descriptor's role builder and every other authored
relation targets a committed claim of the same ledger. Descriptor schema 2
(2026-09-09) adds two things and nothing else: `reviews` and `derived_from`
relations may target exact evidence (tag 4), which the owner admits only when
that artifact is committed at that descriptor hash on the same ledger; and an
optional follow-up policy follows the deadline (`u8` presence, then
`corrective_allowed u8`, `max_follow_ups u16` at most 1024, `single_issuer u8`,
`escalation u8` = none `0`, holder `1`, evaluator `2`). Schema-1 bytes and
hashes are unchanged; a schema-1 descriptor carrying a policy is refused, and
the schema-2 content hash covers the policy section explicitly (present or
absent). Schema 2 also admits the corrective `invalidates` relation (kind
15) to a committed claim, never to the descriptor itself. The host-side
compiler selects schema 2 exactly when a document carries a `policy`, an
`artifact:ID@HASH` relation target or an `invalidates` relation.

Peer follow-ups (R5, decision F29) are ordinary authored claims gated by the
authored policy of the claim they follow, and never reopen it: a terminal or
released claim cannot own a child, so a follow-up links through an authored
relation instead of `caused_by`. A **correction** (action `correction`)
`invalidates` exactly one committed claim whose action is `challenge` and
whose policy has `corrective_allowed`, and `reviews` exactly one exact
artifact: the report of that challenge's terminal Fail, Incomplete or Error
verdict at the challenge's current registration generation (a passing or
still-retryable verdict is `InvalidTransition`, a stale generation
`StaleEvaluation`, any other artifact `MissingEvidence`, another kind of
claim `InvalidTarget`, a missing or forbidding policy `InvalidPolicy`). Its
author is the challenge's issuer, its current receipt holder unless
`escalation` is `none`, or, under `escalation: evaluator`, the evaluator who
reported the cited verdict; anyone else is `WrongActor`. Under
`single_issuer` a second correction of the same challenge, committed or in
the same batch, is `ConflictingCause`. A **follow-up consultation** (action
`consultation`) `refines` the consultation it continues; when that claim
carries a policy its `escalation` names who may file (issuer, holder, or a
designated evaluator of its declarations) and `max_follow_ups` bounds the
consultations refining it, counted from the relation index across the
committed prefix and the batch (`InvalidPolicy` once exhausted); a claim
without a policy bounds nobody. The same escalation governs `caused_by`
children of a live parent: `none` reserves them to the issuer, `holder`
(the schema-1 rule) admits the current receipt holder, `evaluator` also
admits a designated evaluator. The leader applies these rules at admission
and every replica at replay, each claim's citation walk and index scans
bounded by `plan_edges` of their own. A slot's `missing_declaration_index`
is a virtual declaration index: it must name no authored declaration, differ
from every check's declaration index and be unique per slot. The host-side
compiler ([focal-native-client](../../crates/focal-native-client/src/compile.rs))
enforces both before a frame is encoded, so a document that violates them is
refused locally with the same closed categories the owner would return.

Common declaration fields are claim ID, issuer ID, index `u32`, kind `u16`,
declared phase `u16`, mode, target declaration, program, Deadline. Kinds `1..7`
are Receipt, Test, Inspection, Integration, Contract, Design and Regression;
declared phases `1..3` are Admission, Increment and WholeWork. Target declaration
WholeWorkSlot `0` carries slot `u32` and label string; Delivery `1`, Admission `2` and
Increment `3` carry no payload. Program Delivery `0` carries no policy;
Programmatic `1` carries checking policy and optional quality policy; Agentic `2`
carries checking policy. Each policy has evaluator ID, external-definition hash,
required-policy hash?, and handlers. Each handler has ID, version hash, agentic
boolean, attempts `u32`, proof-schema hash and diagnostic-schema hash.

Legacy graph edges are `(DependsOn 0 | Awaits 1, claim ID)`. Cause is Root `0` or
Claim `1`, followed by its ID. Corrections are `(Supersedes 0 | Amends 1,
ObjectRef)`. The assigned creation cut is deliberately absent. Before omitting
derived acceptance summaries, encoding checks them against the actual complete
global declaration cohort for that claim. The checked work surcharge per proposal
is `2*m*n + m + n`, where `m` is the global declaration count and `n` its retained
summary count. The decoder must reconstruct and validate the same cohort; the
structural inspector does not perform that check.

[Artifact payload](../../crates/focal-core/src/native/input_codec/descriptors_artifact.rs)
Inline `0` carries a blob. Content `1` carries domain ID, root hash, length `u64`
and class `u16` (Document `1`, Evidence `2`, Checkpoint `3`). Result provenance is
claim ID, validation ID, full Target, generation `u64`, Attempt and verdict.
Work provenance is claim ID, cycle `u32` and role: Output `0` + slot `u32`,
Diagnostic `1` + Failure, or ReceiptRejection `2` + rejected ArtifactRef + Failure.
Both optional provenance fields are structurally representable; model validation
enforces the allowed combinations.

## 6. Borrowed dynamic construction

The eight dynamic operations outside creation now have bounded construction
paths. They return actual NativeInput values after the caller supplies the quoted
allowances. They do not yet select a budget source from NativeOwner, authenticate
the caller, resolve all state-dependent preconditions, verify custody or publish
anything. A construction plan is not an admission capability.

`prepare_response` and `prepare_monitor` in the
[response decoder](../../crates/focal-core/src/native/input_codec/response.rs)
borrow summary text and fixed-width encoded manifest/diagnostic/root spans.
Indexed reads produce one copied scalar record using checked offsets. No typed
scratch vector is constructed before the final buffers. All confidence/outcome
values, manifest order, diagnostic order and monitor predicate order remain
authored facts. The resulting frame plans capture exact request intent using the
same preimage helpers as owned commands. Their quote covers final bytes and
allocation count plus complete preparation/build work, separately from the
initial structural pass.

The shared [response/monitor source plans](../../crates/focal-core/src/native/response_source.rs)
bound dimensions before hashing, require exact collection termination and retain
the complete body identity. Building reserves final buffers fallibly, reconciles
actual capacities and hashes the actual produced body. Generic sources can change
through interior mutability; equal lengths alone never permit substitution.
Refusals drop partial construction. Response hashing into an existing caller
hasher is atomic on refusal. Existing slice-based response APIs retain their
interfaces and native preimages through the shared helpers.

For the six artifact-bearing commands, `artifact_input` in the
[artifact decoder](../../crates/focal-core/src/native/input_codec/artifact.rs)
parses command fields, scalar artifact provenance, inline bytes/content pointers,
and borrowed reference/visibility spans. Its mutable borrowed view prepares a
frame plan through the model's
[ArtifactSource](../../crates/focal-model/src/lifecycle/artifact_descriptor_source.rs)
interface. Source iterators yield ObjectRef values and borrowed strings without
typed scratch arrays. The actual descriptor semantic checker, canonical-order
checks, memory quote, content hashing and construction are shared with the
existing ArtifactSpec/ArtifactPlan path. Build verifies the final owned descriptor
and its own allocated ID against the prepared identity, including mutations that
occur during iteration. External storage/schema/policy authority remains an owner
responsibility.

Artifact work is accounted in separate components:

| Component | Bound and enforcement |
| --- | --- |
| Initial inspection | The complete frame's structural limits and quote from §§1–3 |
| Scalar/span parse | Separate Cursor limit; returned `parse_visits` measures this pass |
| Encoded collection reads | One shared counter across model preparation and build; ObjectRef reads cost 54, each label costs `8 + 2*byte_length`. Preparation also checks that the known second pass fits before returning the borrowing plan. Build cannot replenish the counter. |
| Model preparation/build | Shared model visit allowance and separately exposed quotes; checks/hashing cover complete bytes and canonical collections. Build preflights its conservative complete allowance before allocating. |
| Native preparation/build | Fixed command/header hashing and quote checks use 4,096 visits; build adds 16 per visibility label for the wrapper's four heap/allocation scans. These separate allowances are checked before the corresponding work. |

Final artifact input bytes include all descriptor buffers, allocator bookkeeping
and the NativeArtifactInput singleton container. The inline decoded command and
borrowed view remain on the stack. The frame plan checks actual wrapper charges
and recomputes the complete native command intent after construction. Model and
source quotes alone are not a transaction-wide quota. The managed owner admission
in §6.3 retains a selected input allowance through custody and candidate preparation;
the enclosing transport still needs its own bounded buffers and total ingress work.

### 6.1 Borrowed creation descriptor bodies

[Creation body inputs](../../crates/focal-core/src/native/input_codec/creation_content.rs)
now inspect and construct complete standalone claim, legacy declaration and
authored validation bodies. They borrow encoded collections, keep scalar fields
on the stack and allocate only the final descriptor buffers. These APIs consume
one descriptor body with exact end checking. The enclosing creation-frame plans
in §6.2 assemble complete requests and establish their existing native intent.

The model's repeatable
[ClaimSource](../../crates/focal-model/src/lifecycle/claim_source.rs),
[DeclarationSource](../../crates/focal-model/src/lifecycle/validation_definition_source.rs)
and [ValidationSource](../../crates/focal-model/src/lifecycle/validation_descriptor_source.rs)
share semantic checking, original hash preimages and owned construction with the
existing slice APIs. Claims retain authored requirement order, canonical relations
and scopes, nested checks and zero-check slots. Validation sources retain complete
handler chains, checking/quality phases, schemas, instructions and provenance.
Every declared collection must yield its exact count and then end, including
absent handler phases. Builds validate and hash the actual owned output to reject
changes made by a generic source during copying.

`BodyInspectionLimits` bounds descriptor bytes, cursor work and the additional
source parsing needed to delimit variable claim collections. `BodyParseQuote`
records those separate costs. A later exclusive `prepare` call receives distinct
model and encoded-source allowances. The source allowance spans preparation and
construction; the returned borrowing plan prevents resetting it. Preparation
refuses if the remaining source allowance cannot cover the known build passes.

`BodyConstructionQuote` includes the inline descriptor, final heap capacities,
allocator bookkeeping, allocation count, and separate model/source preparation
and build visits. Build checks the complete byte/model allowances before
allocation and verifies actual source consumption against its quote afterward.
Source factory and terminal probes are charged by the model; encoded field reads
have an additional shared byte-work meter. Handler reads cost 123 visits and
contributor reads cost 17. Claim construction reads each outer collection and
each nested check stream once; declaration construction reads each handler phase
once. Authored validation construction currently reads each handler phase three
times and contributors twice, with all final checks performed on owned output.
These repeated passes are explicitly charged, not hidden behind a dimensions-only
quote. No temporary typed policy/reference arrays are needed by these adapters.

These are local content proofs. Declaration/validation preparation checks the
supplied principal against the issuer, but the enclosing service must authenticate
that principal. Frame preparation checks actual requirement correspondence and
Required Delivery below; effective-state reference authority, dedup/retries,
selected memory funding, custody and atomic publication require owner ingress. No policy reference
causes Focal to invoke a tool, skill, script or agent.

### 6.2 Complete creation frames

`prepare_authored_creation` in the
[authored decoder](../../crates/focal-core/src/native/input_codec/authored_creation.rs)
and `prepare_legacy_creation` in the
[projection decoder](../../crates/focal-core/src/native/input_codec/legacy_creation.rs)
prepare complete tag-27/tag-0 frames without allocating typed scratch arrays.
Both return borrowing plans with full native intent, final allocation charges and
separate cumulative preparation/construction work. Construction replays the
immutable bytes into final buffers and verifies the resulting native identity.
These APIs remain dormant and do not select an owner memory reservation.

The shared model
[AcceptanceSource](../../crates/focal-model/src/lifecycle/acceptance_source.rs)
checks complete nested slots against opaque
[CheckedDeclaration](../../crates/focal-model/src/lifecycle/validation_checked.rs)
values. Only a checked model declaration or successfully prepared source can
produce these values; callers cannot manufacture a stamp/summary as proof of a
missing body. Authored validation metadata requires an extra bounded pass over
the actual body after its full content binding is derived. That pass computes the
original declaration stamp alongside the authored identities and checks them
again before returning the token. The extra source/model work is explicit.

Acceptance requires actual Required Delivery, exact parent/issuer/ledger,
distinct declaration IDs and indices, no collision with missing-presence indices,
and complete bidirectional slot/check correspondence. Zero-check slots remain
real delivery obligations. Canonical index selection preserves the existing
acceptance hash even when declaration storage order differs. Repeated source
scans check exact cardinality and a stable complete sequence, rejecting sources
that change between searches or during construction. The plan exposes its actual
full-scan counts so adapters can price every nested callback before allocating.

Authored frame preparation additionally checks each ordered requirement against
its actual validation specification, complete per-claim cardinality and
family-scoped ID uniqueness across groups. Scope/responsibility profiles stay
explicit and are checked locally; original owner bindings/receipt fences remain
part of intent. Construction retains only the original authored bodies and
profiles. The existing RAM owner derives graph/lineage/acceptance projections
when it admits them. It supplies the actual creation cut.

Projection frames carry their complete graph, lineage and acceptance declarations
but do not recover missing authored content from a binding hash. The decoder
checks canonical graph/correction streams and uses checked consuming model
constructors for their final vectors. It scans interleaved global declarations
through a bounded filter for each actual claim, with full preparation and
construction callback costs included. The original creation-intent writer is
shared with owned proposals; streaming failures leave its prior state intact.
The format still excludes owner-assigned `created` and the decoder does not
fabricate a historical creation position.

Each frame uses five cumulative work domains: byte parsing, encoded-source reads,
descriptor checking/hashing, acceptance checking, and structural/native work.
Keeping descriptor and acceptance counters separate prevents a nested callback
from replenishing its enclosing algorithm's allowance. Slot/check iterators debit
the same encoded-source meter used by declaration callbacks. A local body check
receives the global remaining allowance and debits its actual work before another
callback proceeds. Internal frame-only body preparation does not require unused
future build allowance; every actual body build checks its complete remaining
source allowance before allocation. Public standalone body preparation retains
its stronger reservation check from §6.1.

Construction quotes include replayed preparation, final capacities and allocator
bookkeeping, model/source construction and final identity/capacity checks. The
authored final inspection separately prices the two native capacity/allocation
scans over every work scope and slot; fixed header hashing is not a substitute
for this variable work. Graph and correction streams charge terminal probes as
well as each declared value. These are input-level quotes; owner integration must
also hold the selected capacity through custody and candidate preparation and
account for the initial structural scan and transport buffers.

### 6.3 Managed owner admission of decoded requests

[`DecodedRequest`](../../crates/focal-core/src/native/input_codec/admission.rs)
wraps actual opaque artifact, response, monitor and creation plans. Fixed actor
frames have a checked conversion that excludes variable commands and every timer
namespace. No constructor accepts a supplied request hash or a descriptor summary.
`NativeOwner::prepare_decoded` and its custody/schema variants consume these plans
without allocating input buffers before the owner selects their funding.

The owner requires an externally authenticated `NativeContext`. It checks the
frame ledger and content profile against its own root, then checks the request
principal and identity against the entire committed/pending prefix. The same
private identity path serves existing owned inputs. Exact retries return the
original pending ticket or committed outcome before fresh clock, queue, book,
construction-work or memory admission. A conflicting request intent refuses.
Actor ingress cannot deliver trusted timers.

Admission, Increment and WholeWork reports use shared borrowed/owned authority
checks against the actual evaluation, attempt, evaluator, parent and target.
Opaque model source plans expose frozen fields and complete bounded reference
and visibility streams. The held completion contract checks current registration,
parent, pinned schema and descriptor capacities before lending its source.
Respondent admission likewise checks the actual receipt, holder and cycle before
selecting the first mandatory Work diagnostic, an authored response closure or
posting of a recorded response. Other eligible work uses Ordinary admission.
Close preflight also rejects response/result-testament ID collisions; the complete
manifest and evidence acceptance checks remain in candidate preparation.

The input reservation precedes final construction and remains held through
custody and candidate preparation. Completion envelopes already price its overlap
with verification workspace, retained verification proof and Core's conservative
input/candidate charges. Retained pages acquire their own charges. All failures
release the temporary input allowance; successful candidates carry the existing
journal and rollback rules. There is no gap in accounting while transferring the
constructed input to Core.

`construction_visits()` sums the plan's quoted build domains with checked
arithmetic. Fresh admission requires that full work allowance and the owner's
input byte limit. Initial structural/model/source preparation remains separately
bounded by the codec. Borrowed report authority replays use the original source
counter; a second pre-allocation check ensures they have not consumed the final
construction pass. Actual owned identity, capacities and authority are checked
again before custody/publication. Ordinary creation still resolves state-dependent
references and ownership during the existing funded creation preparation.

The pressure tests exercise decoded Admission reports and a complete respondent
error-artifact → authored Failed testament → explicit posting sequence with the
ancestor budget exhausted. They also cover missing custody, wrong actors, stale
receipts, malformed response identity, exact pending/committed retries, insufficient
work/source allowance, and discard followed by retry. Receipt still creates no
testament. This is a native RAM owner API; live server/CLI/MCP dispatch remains V1.

### 6.4 Complete frame dispatch and cumulative input work

[`NativeDecodeLimits`](../../crates/focal-core/src/native/input_codec/ingress.rs)
provides one bounded dispatch path for complete borrowed actor frames. Its
`for_native` helper derives semantic byte and collection ceilings from the
existing owner limits and maximum frame size; deployments need no per-command
user configuration for these dimensions. An embedding may impose stricter
semantic limits. The five `DecodeWork` domains cover parsing, encoded source
callbacks, model checks, acceptance checks and native frame work. Their checked
sum bounds input work; unused capacity in one domain cannot replenish another.

The initial structural pass spends the parsing allowance. After a checked header,
the owner verifies ledger, content profile, principal and nonzero actor request
identity before the inspector scans the variable body. Structural inspection
without an owner remains available for framing tools. Actor dispatch refuses
timer namespaces; a timer header never manufactures an actor principal.

Typed preparation spends the remaining domain allowances. Response/monitor
preparation already combines its cursor, source and hash work, so that complete
quote belongs to the native domain. Creation preparation maps its existing five
domains directly; artifact preparation keeps parsing, source, model and native
work separate. Fixed conversion prices a conservative 4,096-visit native hash
allowance before hashing. `DecodeQuote.preparation` includes the original scan.

The opaque `DecodedRequest` retains the remaining domain limits. Fresh owner
admission checks each construction domain before reserving/building, and actual
construction checks them again. Exact retries can return after complete identity
recomputation without future construction headroom. Internal artifact frame
preparation therefore requires only its completed content proof; the standalone
artifact-plan API still promises its known final source pass. Borrowed report
preflight spends the same private encoded-source counter, and a final capacity
check prevents it from consuming the pass needed to build the input.

`NativeOwner::prepare_frame` and its custody/schema variants combine this dispatch
with §6.3. The caller retains the charged raw buffer for the synchronous call.
Input decoding does not account for asynchronously queued network buffers or
replace the existing bounded state-authorization, custody and candidate work.
Those components still need one enclosing transport/admission contract.

### 6.5 Receiving bytes without an implicit allocation

The [wire framing helpers](../../crates/focal-wire/src/frame.rs) now expose a
checked fixed header, exact payload reading into caller-owned storage and a
convenience raw frame reader. A short destination refuses before reading or
changing the payload buffer. Fragmented I/O is supported; truncation returns an
error, and extra destination capacity remains untouched. Existing typed V1 reads
and writes use fallible exact-capacity buffers. The framing bytes remain
`FOCALQ01`, version, kind and length; there is no frame checksum to reinterpret.
TLS retains its existing network integrity role. Allocation failure is a typed
transport error, and the client conservatively treats a failed receive after
submission as an uncertain outcome that needs exact retry reconciliation.

These primitives do not activate native transport. A native negotiated profile
must authenticate peer credentials and tenant before ledger selection, inspect
only a bounded fixed native prefix, and obtain a separately funded receive slot.
It may use that prefix to establish temporary report/receipt eligibility, never
to accept an artifact hash or return an outcome. Full body inspection and intent
recomputation precede every accepted retry or mutation. A request whose live
receipt/attempt has already advanced also needs a bounded retry receive slot.

The current largest artifact report prefix fits in 496 native bytes, or 512
including the outer frame header. This is a stack inspection bound, not a raw
buffer reservation: complete response frames additionally contain summaries,
manifests and diagnostics. Receive slots must cover asynchronous receive, queueing
and decoding independently of the completion book's single synchronous workspace.
Connection/stream limits and Quinn receive windows also require advance funding.
Partial or cancelled streams must be reset/resolved before recycling their slot.

## 7. Remaining implementation and acceptance gates

1. Connect the complete frame admission API to a negotiated authenticated native
   transport with funded receive/retry slots. Combine the cumulative input work,
   state authorization, custody and candidate work into the enclosing transaction
   contract. Input quotes do not price queued transport buffers or receive windows.
2. Preserve complete local creation proofs through the remaining reference-policy
   and Handoff authority work. Extend integrated borrowed ingress qualification
   across every report target and profile, retaining existing owned-path coverage
   and frozen V1 vectors/replay.
3. Complete recorded-mutation encoding, checked checkpoint hydration/import,
   resource-entitlement reconstruction, WAL/Ready and Session/quorum integration.
   Activate the complete successor through the ordered decoder transition before
   live CLI/MCP dispatch. Input framing alone supplies no restart guarantee.

Executed verification is recorded in [09](09-implementation-status.md); remaining
durability, authority, deployment and scale work remains explicit in
[18 §6.13](18-lifecycle-storage-upgrade.md#613-successor-input-codec-dependency-plan).
