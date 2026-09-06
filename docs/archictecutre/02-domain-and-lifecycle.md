# Focal domain and lifecycle contract

Status: **proposed Focal implementation contract, 2026-09-05**. This specifies what the Rust implementation must build; no behavior below is claimed to exist today. “Inherited” identifies Hecate semantics. “Focal decision” completes a missing contract or explicitly changes a source restriction. Implementers use this document when imported historical prose conflicts.

The canonical term is **testament**. A **validation** is a requirement; a **validator** implements an evaluation; a **verdict** records the evaluation result; an **artifact** supplies evidence. These are separate concepts and types.

Primary sources: [Ledger architecture](reference/hecate/docs/architecture/LEDGER.md), [Ledger core](reference/hecate/docs/specs/LEDGER_CORE.md), [Ledger substrate](reference/hecate/docs/specs/LEDGER_SUBSTRATE.md), [Wire format](reference/hecate/docs/specs/WIRE_FORMAT.md), [IAM](reference/hecate/docs/specs/IAM.md), [Rank](reference/hecate/docs/specs/RANK.md), [Agent runtime](reference/hecate/docs/specs/AGENTS_RUNTIME.md). Hecate is docs-only; [GAPS §0](reference/hecate/docs/GAPS.md) identifies its tests as obligations.

## 1. Scope, topology and durable truth

The ledger contains claims, testaments, validations, artifacts, their typed relations and lifecycle. Configuration, generic messaging, executor working memory, authorization policy, storage bytes and telemetry retain their own owners. Logged timer fires, policy snapshots and validator results are replay inputs; logging them does not make them new claim-object families.

The same types and lifecycle apply on a laptop and across a multi-region fleet. Tenant and session identity are always explicit, including at one node. A storage address, shard number, process identity, region or replica count never enters authored work content. Placement changes cannot change the identity or meaning of a claim.

One session's mutation history has one authoritative order under the consistency topology in the storage design. Distribution can partition state and execution while retaining that order. There is no global fleet sequence, global graph, global participant registry or full-fleet scan on a command path. Cross-session collaboration uses explicit export/import or coordination claims; it never creates an unchecked direct edge into another session's graph.

**Focal decision D-01:** a session is also the graph consistency and isolation namespace. A transaction can touch multiple state shards inside that namespace. A transaction cannot silently span tenant/session authorities. Cross-namespace work has an explicit saga and immutable import provenance; atomic cross-session parent/child completion is unsupported until separately designed.

## 2. Identity and canonical content

### 2.1 Identity types

| Type | Meaning | Minting and lifetime |
|---|---|---|
| `TenantId` | Globally unique tenant namespace | Enrollment/control authority; immutable |
| `SessionId` | Globally unique session within tenant | Session authority; survives relocation and restart |
| `LedgerId` | `{ tenant: TenantId, session: SessionId }` | Required prefix of every persisted object and graph reference |
| `ParticipantId` | Stable principal UID, qualified by namespace | Identity authority; separate from a process/connection ID |
| `ObjectId` | Opaque stable 128-bit object identifier | Allocated outside the reducer, validated and logged once |
| `ClaimId` | Family-specific newtype over `ObjectId` | Stable work address; independent of content digest |
| `TestamentId` | Family-specific newtype over `ObjectId` | Allocated before close; becomes visible on committed creation |
| `ValidationId` | Family-specific newtype over `ObjectId` | Allocated for a requirement; stable across execution attempts |
| `ArtifactId` | Family-specific newtype over `ObjectId` | Stable evidence address; storage movement cannot change it |
| `ContentHash` | Versioned canonical BLAKE3 digest, 256 bits | Derived identity/dedup value; never an object address |
| `ReceiptId` | Unique durable claim execution entitlement | Ledger-minted with epoch; only one current holder |
| `EvidenceSetId` | Receipt-scoped staging identity | Logged allocation; not a terminal testament ID |
| `ValidationRunId` | `(ValidationId, target_hash, phase, run_epoch)` | Durable schedule record; pinned evidence/version |
| `RequestEpoch` | Durable admitted request generation, scoped to principal/session | Server-negotiated and logged; hides bounded-history mechanics from users |
| `RequestId` | Opaque 128-bit client retry identity inside its admitted epoch | Reused on transport retry; never reused for different commands |
| `SessionSeq` | Authoritative session history coordinate | Assigned by ordering/commit layer |
| `DeltaId` | `(LedgerId, SessionSeq, ordinal)` | Distinguishes multiple ordered deltas from one mutation |
| `Handle<T>` | Arena slot + generation + owner-local provenance | Internal only; never serialized as an object identity |

All identifier newtypes are distinct Rust types. An `ArtifactId` cannot be used as a `ClaimId` because their underlying bytes have the same length. Wire references include object kind and `LedgerId`; the complete proof address is `(LedgerId, ObjectId)`. Routed references resolve stable IDs to current owners; relocations never rewrite graph content. IDs are allocated outside the pure reducer and included in the accepted logged command; replay never mints replacements. Uniqueness and namespace binding are checked at admission. No family ID is derived from a content hash.

### 2.2 Hash domains

**Focal decision D-02:** object address and content identity are separate. A content digest is BLAKE3 over a fixed domain tag, schema major, namespace, object family and canonical immutable body. Exclude the object's own allocated `ObjectId`; include referenced stable IDs. For an outgoing canonical edge, its source is implicit self and excluded from the digest, while its target ID and relation are included. This prevents an allocated ID from defeating content-identical dedup. The `(LedgerId, family, schema major, ContentHash)` index maps content identity to its existing opaque object ID. Hashes are derived, never authoritative caller assertions. Schema ancestors retain their original encoding; read-time upgrades do not silently rehash stored proof.

`ClaimContent` includes resolved issuer/subject identities and a verified immutable cause. These fields become content when generation commits, although the runtime resolves/stamps them on behalf of the issuer. Excluded fields are lifecycle, log position, receipt owner/epoch, delivery attempt, trace refs, derived caches, current policy snapshot, current physical location and wall-clock handling timestamps.

A claim carries a stable `OccurrenceId` chosen when the intent originates. Retrying the same intent preserves it. Intentionally repeating otherwise identical work mints a new occurrence. This is part of authored intent, not the transport retry ID. Identical full content including occurrence is a no-op; two intended occurrences cannot accidentally coalesce.

Validation requirements are immutable objects with preallocated `ValidationId`s; the claim's ordered requirement relations reference those IDs and their pinned specification content. Requirement↔claim references use stable IDs, not recursively embedded hashes. Artifact descriptors exist before testament close, and the closed testament references their stable IDs plus verified content digests. None of the four family IDs is hash-derived.

A single atomic creation batch may preallocate all participating IDs before calculating content digests, allowing legal DependsOn/Awaits cycles without recursive hash construction. The batch validates all endpoint kinds, namespace boundaries, cardinalities, forbidden causation/supersession cycles, content identities and footprints together. Referenced endpoints must exist in effective state or in that same batch. Exact retries preserve allocated IDs. A detected existing content identity returns its existing ID; conflicting/partially duplicate batches are returned for explicit rebinding and complete revalidation, never partially committed or silently assigned new IDs. Runtime cycles can also arise through separately logged scope wait edges.

Historical content identities remain resolvable after retirement. If the digest index is cold, an asynchronous archive lookup returns a namespace/digest-bound result with its catalog version and read pin; the sequencer rechecks it against current effective state, including pending creations and current catalog validity, before accepting creation. A stale or unpinned absence cannot authorize a new object. Lookup invalidation/unavailability yields retry/backpressure rather than assuming absence. The lookup does not perform synchronous I/O inside the reducer.

Bulk content uses the storage design's immutable `ContentRef`. Its root, length, class and dedup-domain identity are checked at the boundary. A private-domain content root is meaningful only in that domain. Neither raw bulk bytes nor unbounded strings are allowed in a claim-plane record.

### 2.3 Rust ownership shape

```rust
pub struct StoredObject<C, L> {
    content: C,                    // private, written once
    content_hash: Option<ContentHash>,     // derived cache only
    lifecycle: L,                  // private, system transitions only
}
pub struct PreparedMutation {
    request_epoch: RequestEpoch,
    request_id: RequestId,
    expected: Preconditions,
    footprint: Footprint,
    intent: NormalizedCommand,
    replay_inputs: BoundedVec<PinnedInput>,
}
```

These are module-boundary sketches, not committed codec schemas. `generate` constructs immutable content; `prepare` validates a command against effective state; `apply_committed` changes lifecycle after durable acknowledgement. No public setter exposes a mutable content/lifecycle aggregate. A normalized replay comparison excludes presence of the lazy hash cache but checks any computed value.

Preparation records normalized intent and sufficient admission/replay inputs; it does not compute the entire graph/index update. The materializer's reducer produces a `TransitionBatch` containing changes and immutable facts. This keeps expensive graph application outside the ordering owner while preserving admission against effective state.

## 3. The four object families

### 3.1 Claim

The party/cause/action/requirement fields below are **typed Rust views over canonical relation records**, not additional authoritative scalar fields. Store `Issuer`, `Subject`, `Evaluator`, `ClaimAction`, `CausedBy` and `ContributedBy` once as typed edges/relation values; accessors expose their checked cardinality and target type. Read projections may cache these views but are derived and cannot be written independently. Serialization and hashing consume the canonical relations once, avoiding two competing sources of truth for the same party or action.

| Field group | Required content | Ownership and validation |
|---|---|---|
| Identity context | `LedgerId`, `OccurrenceId`, content schema | Runtime verifies namespace; issuer owns occurrence intent |
| Parties | Exactly one issuer and one subject; resolved participant references | Issuer chooses target selector; core freezes accepted UID |
| Causation | `Cause::Claim(ClaimId)` or trusted `Cause::Root(RootCommandId)` | Runtime-stamped; verified against principal/turn context |
| Purpose | Closed `ActionType`, bounded assertion/description or document ref | Issuer-authored; no free-text dispatch authority |
| Scope | Ordered normalized `(ScopeKind, ScopeKey)` set | Issuer-owned; maximum entries/bytes charged before acceptance |
| Requirements | Immutable ordered validation specification document | Resolved once with policy/provenance before freeze |
| Relations | Initial typed outgoing relation declarations | Valid endpoints/cardinality/namespace; no owner-local handles |
| Time bounds | Optional deadline budget/absolute logical deadline | Runtime converts at acceptance; firing is a logged input |
| Domain | Optional closed rank domain, required for feedback/challenge | Generated from rank source when rank is enabled |
| Referenced inputs | Typed content/claim/artifact refs needed to do work | Read capability and namespace checked before accepting |

The initial closed `ActionType` set is `Work`, `Consultation`, `Challenge`, `Feedback`, `Approval`, `Summon`, `Handoff`, `Evaluation`, `Correction`, `Teardown`. These are Focal profile tags, not a claim that Hecate supplied a complete enum. Kind-specific payload schemas define work; unsupported profiles fail admission. Add future profiles through the append-only schema process. `ScopeKind` is `File`, `Symbol`, `Api`, `TestSurface`, `Component`, `UxSurface`; scope keys normalize under a kind-specific pinned schema. Scope is not a shard-placement instruction.

Claim lifecycle contains status, ordered status history, creation/post/receipt positions, active receipt, local completion outcome, system deadlines, validation progress, bounded trace refs, current terminal reason and release state. These are system-written fields. An arbitrary caller cannot submit them as content updates.

### 3.2 Testament

A closed testament contains `LedgerId`, `ClaimId`, receipt provenance, `EvidenceSetId`, an immutable ordered artifact manifest, bounded summary/document ref, `Confidence`, and `OutcomeKind`. The outcome vocabulary is `Complete`, `Partial`, `Refused`, `Impossible`, `Interrupted`, `Failed`; it describes testimony and does not set claim satisfaction.

`Confidence` is `Hint`, `Tentative`, `Committed`, `Consensus`, preserving Hecate's vocabulary. A confidence label never overrides a required verdict or acts as a quorum assertion.

The evidence set is mutable only through append-only staging commands under its active receipt. Closing freezes a manifest and durably creates the preallocated `TestamentId` with its separately computed content digest. The immutable testament's lifecycle records acknowledgement and evaluation progress separately. Artifact additions after close require a new evidence set and a new testament; no “patch result JSON” API exists.

**Focal decision D-03:** a claim has one active closing testament attempt per receipt epoch. A failed or superseded claim is corrected with a new claim linked by `supersedes`/`amends`; its predecessor's terminal record remains unchanged. Resubmission after a transport timeout uses the original `(RequestEpoch, RequestId)`, allocated object IDs and identical close manifest.

### 3.3 Validation requirement and verdict

| Field | Definition |
|---|---|
| `kind` | `Receipt`, `Test`, `Inspection`, `Integration`, `Contract`, `Design`, `Regression` |
| `phase` | `Admission`, `Increment`, `WholeWork`; independent of kind |
| `description` | Atomic bounded instruction or content reference |
| `quality_bar` | Optional explicit judgment standard; never a status flag |
| `mode` | `Observe` or `Required`, frozen for this claim |
| `evaluator` | Designated principal/capability-resolved evaluator |
| `handler_chain` | Ordered pinned validator IDs/versions and parameter refs |
| `evidence_contract` | Required artifact schemas/kinds and target-selection rule |
| `contributed_by` | Ordered provenance references: issuer, advisor, policy source |
| `policy_revision` | Policy resolution version that contributed this requirement |
| `deadline_budget` | Derived execution/fallback budget within claim deadline |

A verdict record references `ValidationRunId`, exact claim/testament or increment target, validator version, evidence manifest hash, evaluator, verdict value and causation. Attempt observations are retained as immutable artifact-backed records. There is one accepted terminal verdict per run epoch; a repeated identical verdict is idempotent and a conflicting duplicate is refused.

The verdict algebra is `Pass`, `Fail`, `Incomplete`, `Error`. `Fail` means evidence contradicts the standard; `Incomplete` names missing evidence; `Error` means evaluation did not complete correctly. Preserve all individual verdicts even if one aggregate claim status is selected.

### 3.4 Artifact

An artifact descriptor includes `LedgerId`, open namespaced `ArtifactKind`, pinned schema hash/version, bounded typed metadata, `ContentRef` or bounded inline value, producer/receipt/run provenance, immutable input references and inherited visibility labels. Open kinds do not imply arbitrary unvalidated JSON: an unregistered or mismatched schema is a typed rejection.

Standard initial kinds: `document`, `test_report`, `inspection_report`, `validation_verdict`, `increment_descriptor`, `error`, `allocation_result`, `approval_evidence`. Register schema ownership and bounded metadata for each; extending a kind does not require extending lifecycle enums.

`ErrorArtifact` contains a closed error code, phase, affected references, bounded diagnostic/document ref and `Disposition`. `Retryable` can contain a derived retry hint and retry deadline; `Terminal` names the authority/rule that makes retry pointless. Machine decisions never parse diagnostic strings. Internal Rust functions still return typed `Result`; the service/claim boundary converts operational failure into durable evidence when a durable claim exists.

Artifact attachment/close acknowledgement requires the referenced bytes to meet their promised durability and integrity contract. Staging references are pinned until close, abandonment or recorded custody transfer. A reference to unreplicated process memory is not durable evidence.

## 4. Relations and graph mutation

The initial closed relation set is `Issuer`, `Subject`, `Evaluator`, `ClaimAction`, `Supersedes`, `DependsOn`, `Awaits`, `CausedBy`, `Refines`, `ConflictsWith`, `DerivedFrom`, `Reviews`, `Amends`, `ContributedBy`, **`Invalidates`**. Stable discriminants and reserved extension ranges are committed in milestone P00 before codec implementation. Enum additions are append-only; removing, renumbering or reusing an assigned value requires a new schema major. `Invalidates` is a Focal completion: Hecate's rank rule uses it but its architecture list omits it.

| Relation family | Constraints | Graph meaning |
|---|---|---|
| Issuer/Subject | Exactly one each on a claim; stable participant refs | Work parties, not execution-owner mutation |
| Evaluator | Exactly one per effective validation requirement | Verdict authority |
| ClaimAction | Exactly one typed action per claim | Dispatch/semantic classification |
| DependsOn | Same session; claim→claim; cycles legal | Requires target satisfaction; terminal failure propagates |
| Awaits | Same session; claim/scope→claim; cycles legal | Requires target terminality; failure still releases wait |
| CausedBy | Same session or explicit imported provenance; trusted root variant | Immutable cause tree; no cycle permitted |
| Supersedes/Amends | New object→predecessor of compatible family | Explicit correction lineage; no cycle permitted |
| Invalidates | New evidence-bearing feedback→target feedback | Typed override rule, with rank check where configured |
| Reviews/Refines/DerivedFrom | Registered endpoint-kind combinations | Evidence/context traversal; nonblocking by default |
| ConflictsWith | Canonical edge representation, symmetric query view | Evidence of overlap; not automatic cancellation |
| ContributedBy | Requirement→validated contributor identity/provenance | Descriptive origin; never write authority |

**Focal decision D-04:** issuer-authored dependency declarations freeze with claim content. Adding or replacing an obligation requires a superseding claim. Runtime wait edges belong to a separately owned durable scope record and may be appended/rebound by explicit logged scope commands. An explicit supersession may rebind named runtime monitors; it cannot silently rewrite immutable claim content or retroactively alter a satisfied predecessor.

An edge record contains stable endpoints, relation type, author/system provenance, introduction position and optional explicit replacement lineage. Forward/reverse indices are derived. Every graph mutation's prepared footprint includes both endpoint adjacency lists and the affected monitor/identity indices; user-declared scopes are not trusted as a complete storage write footprint.

## 5. Canonical lifecycle

### 5.1 Status vocabulary

**Focal decision D-05:** the following exhaustive status set completes Hecate's missing failure/cancellation table. Names below define the serialized vocabulary; Rust uses corresponding PascalCase variants. Assign numeric discriminants and reserve extension ranges in P00, snapshot them with the wire schema, and enforce append-only evolution thereafter.

| Class | Statuses |
|---|---|
| Active | `generated`, `posted`, `received`, `progressed`, `testament_generated`, `testament_acknowledged`, `validating` |
| Successful terminal | `satisfied` |
| Boundary failure terminal | `post_failed`, `receipt_failed`, `testament_generation_failed` |
| Evaluation terminal | `validation_incomplete`, `validation_failed`, `validation_errored` |
| Control/dependency terminal | `cancelled`, `expired`, `revoked`, `superseded`, `dependency_failed`, `deadlocked` |

`is_terminal` is an exhaustive enum match over the thirteen terminal variants. `is_active` means any nonterminal status, but `is_actionable` excludes `generated` and depends on receipt, admission, dependency and standing predicates. `progressed` records that at least one progress fact exists; each further progress command emits another observational fact without creating a new status value.

There is deliberately no `generated_failed` status: invalid canonical generation has no claim object; its typed rejected-command record belongs to command audit, where available. Failure to append durably cannot truthfully be reported as a durable claim transition. A lost testament acknowledgement is a retry/resume condition, not an invented `testament_acknowledgement_failed` status. Persistent inability to finish acknowledgement reaches a logged expiry or cancellation.

### 5.2 State transition table

Every row executes through the sequencer using effective state. “Active execution” below means `received | progressed`; “open” means any active status. All emitted facts are ordered inside one committed mutation. Operations absent from this table cannot alter claim status.

| Command/input | Permitted prior state | Next state | Required effects and disposition |
|---|---|---|---|
| `GenerateClaim` / `GenerateClaimBatch` | No existing content identity, or exact retry | `generated` | Validate preallocated IDs and full batch; freeze content/requirements/edges atomically; identical repost returns existing opaque ID |
| `PostClaim` | `generated` | `posted` | Resolve current standing/target availability; activate admission validation scheduling; directed post fact |
| `FailPost` | `generated` or `posted` | `post_failed` | Record final typed targeting/admission failure and runtime error evidence |
| `AcquireReceipt` | `posted` | `received` | Admission requirements passed; dependencies permit start; mint holder+epoch entitlement; emit receipt fact |
| `FailReceipt` | `posted` | `receipt_failed` | Final bounded delivery failure/overflow disposition; durable runtime failure evidence |
| `RecordProgress` | Active execution | `progressed` | Bounded observational fact; no satisfaction/release effect |
| `BeginEvidenceSet` / `AttachArtifact` | Active execution | Unchanged | Current receipt only; validate/pin evidence; append immutable references |
| `CloseTestament` | Active execution | `testament_generated` | Current receipt; atomically freeze manifest/content; stop evidence appends |
| `AcknowledgeTestament` | `testament_generated` | `testament_acknowledged` | Verify authoritative attachment/durability; receipt validations pass exactly once |
| `BeginWholeWorkValidation` | `testament_acknowledged` | `validating` | Pin runs/evidence/versions; enqueue bounded external execution effects |
| `RecordValidationVerdict` | `posted` for admission; `received` through `validating` for increment; `validating` for whole work; isolated Observe run as below | Unchanged until phase aggregation | Current authorized run only; record evidence and schedule error-only fallback if applicable |
| `CompleteWholeWork` | `validating` | `satisfied` | All required whole-work verdicts Pass; local completion recorded; all blocking predicates satisfied |
| `CompleteWholeWork` | `validating` | `validation_failed` | Required final Fail exists; preserve other final outcomes |
| `CompleteWholeWork` | `validating` | `validation_errored` | No Fail, but required final Error exists |
| `CompleteWholeWork` | `validating` | `validation_incomplete` | No Fail/Error, but required final Incomplete exists |
| `FailTestamentGeneration` | Active execution | `testament_generation_failed` | Runtime records typed handler failure and an immutable failure testament/evidence bundle |
| `CancelClaim` | Open | `cancelled` | Authorized cancellation; record reason; cancel child execution/scopes; release only after terminalization obligations |
| `ExpireClaim` | Open | `expired` | Logged matching timer generation; immutable timeout artifact |
| `RevokeClaim` | Open | `revoked` | Logged authoritative revocation; fence receipt/validator effects; record authority reason |
| `SupersedeClaim` | Open | `superseded` | Atomically introduce compatible successor + explicit relation; fence old execution |
| `SupersedeClaim` | Terminal | Predecessor unchanged | Atomically introduce a new compatible successor and lineage edge; preserve the predecessor's terminal status, content and original verdicts |
| `PropagateDependencyFailure` | Open | `dependency_failed` | Required DependsOn target terminal-unsatisfied; deterministic causal witness |
| `BreakDeadlock` | Open | `deadlocked` | Expired SCC/deadline policy selects canonical victim; typed graph witness |
| `AdoptReceipt` | `received` through `validating` | Unchanged | Durable identity-chain transfer; increment epoch; predecessor receipt-holder effects fenced |
| `ReleaseScope` | Terminal or released dependency monitor | Unchanged | Record release only after owned work and cancellation obligations are settled |

Admission verdict Fail/Error/Incomplete does not use whole-work terminal states: after bounded policy/evaluator handling is exhausted it produces `post_failed` with all admission evidence. Retryable temporary admission remains `posted` until retry/deadline; `FailPost` records this final admission outcome. Receipt acquisition is impossible while admission is pending.

For `FailPost` from `generated`, the failure means the canonical obligation existed but activation failed permanently. Transient transport/provider capacity does not immediately terminalize a claim that is still within an explicit retry budget. A `receipt_failed` overflow is the source-mandated terminal result once the bounded dispatch policy declines further retries; error disposition still informs the issuer's choice to issue a successor.

`FailTestamentGeneration` is a runtime-owned exceptional close: it emits an immutable failed testament and error artifacts even though the ordinary agent closure did not finish. The terminal state records which boundary failed. Creation, acknowledgement and receipt-pass facts for that failure testament are ordered within the same mutation; these describe the testament object and do not advance the claim through successful closure states or transiently satisfy it.

### 5.3 Repeated, conflicting and stale operations

| Situation | Result |
|---|---|
| Same admitted `(RequestEpoch, RequestId)`, identical canonical command | Return the original result/position; no new effect |
| Same admitted `(RequestEpoch, RequestId)`, different command bytes | Structural Refuse `idempotency_conflict` |
| Request epoch below the minimum accepted generation | Return a verified archived exact receipt, otherwise `RequestHistoryExpired`; never execute |
| Unallocated/future request epoch or wrong principal/session epoch | Refuse request admission; client must negotiate a valid epoch |
| New mutation, identical authored claim content | Existing claim identity; no duplicate dispatch |
| Terminal claim receives progress or an attempt to resume its execution | Inform with current terminal truth; no lifecycle fact; explicit successor creation follows the SupersedeClaim row |
| Receipt holder or run epoch is stale | Structural Refuse with current epoch metadata permitted by visibility |
| Wrong actor for content/lifecycle/evaluation field | Structural Refuse; never rewrite ownership |
| Valid command but dependency pending | Yield if caller requested parking; otherwise Inform |
| Policy standing denied/needs approval | Inform or Yield with typed reason; does not masquerade as structural corruption |
| Malformed schema, relation, namespace or provenance | Structural Refuse before mutation |
| Completion races timeout/cancellation/revocation | First committed legal transition wins; later operation observes terminal truth |

### 5.4 Bounded request history

The durable request key is `(authenticated principal, LedgerId, RequestEpoch, RequestId)` and binds the canonical command digest and its original committed result. The service negotiates generations with the client library; a caller cannot invent an admitted epoch. The user never configures epoch numbers. Transport retry preserves the complete key and all preallocated object IDs.

Epoch admission and its minimum floor are scoped to `(authenticated principal, LedgerId)`. Multiple clients of that principal share a still-admitted epoch using distinct RequestIds. Negotiating on reconnect reuses a valid generation; it does not invalidate another client's requests by advancing the floor automatically.

Retain exact receipts for every accepted request in every still-admitted generation. To reclaim that history, durably advance the principal/session minimum accepted generation after settling or pinning its pending requests. Preparation checks this minimum against effective state, so a racing epoch advance cannot make a late old request fresh. Once a generation is below the minimum, a request may only resolve an exact archived receipt under its original digest and authority; otherwise return `RequestHistoryExpired`, even if no seen-key entry remains.

This rejects expired unknown keys without an unbounded lifetime seen-key set. Do not evict arbitrary request receipts inside a still-admissible generation. An archive lookup is pinned/versioned and rechecked before returning a historical result; missing or stale lookup evidence never authorizes execution. The client must resolve an unknown outcome explicitly, not silently move its old request into a fresh epoch.

A terminal claim's status never changes again. Adding a later review or correction creates new immutable nodes and relations; it does not overwrite historical verdicts. A supersession of an already terminal claim links a new successor and leaves the predecessor's status intact. Receipt adoption fences the predecessor holder without discarding independently leased evaluator runs whose evidence/version remains pinned and whose evaluator epoch is still valid. Outstanding runtime monitors follow a successor only through a named durable rebind command.

## 6. Validation execution protocol

### 6.1 Resolution and immutable requirements

`ResolveRequirements` combines issuer intent, advisory proposals and mandatory policy constraints before `GenerateClaim` freezes canonical content. Every resulting requirement retains `contributed_by`. Advisors propose; only the issuer-authorized resolution path creates the effective set. A mandatory policy constraint cannot disappear because the issuer omitted it from a request.

If current policy tightens after generation, posting either succeeds under that version or returns Inform/Yield requesting explicit successor content with the newly required validations. It does not mutate the generated claim. A snapshot/requirement revision used in a decision is recorded as a replay input, separate from content identity.

The registry stores stable validator ID, implementation/version digest, parameter schema, supported kinds/phases, evaluator capability, evidence schema, bounded concurrency, priority and timeout policy. Claims pin the resolved chain. Process-local function pointers are never persisted. The current registry cannot change old claims on restart.

New validators default to Observe in configuration. Promotion to Required is a reviewed configuration change affecting newly resolved claim content; existing claims need explicit supersession to adopt it. Observe failures are recorded but influence neither readiness nor satisfaction. Required policy validators are explicit exceptions to a new optional validator's Observe default and must be identified as such during requirement resolution.

**Focal decision D-06:** quality bars require a designated agentic evaluator capability, rather than banning every non-agent issuer. This deliberately refines Hecate's ambiguous “non-agentic claimants cannot attach quality bars” rule. A deterministic service may issue work for an agent to evaluate; a purely programmatic evaluator cannot accept an unsupported quality bar. Participant categories never change the wire/lifecycle shape.

### 6.2 Two-phase evaluation and attempts

1. Create a durable `ValidationRun` referencing exact immutable evidence and pinned chain. Enqueue execution only after the schedule mutation commits.
2. Execute the selected deterministic handler outside the sequencer in a bounded tracked scope.
3. Persist its typed verdict and evidence. On Pass, proceed to the quality-bar phase if present. On Fail or Incomplete, finish the requirement with that outcome.
4. On Error, record the failed attempt and try the next pinned handler according to priority, bounded attempt count and remaining deadline.
5. If deterministic handlers errored and an explicitly configured agentic fallback exists, dispatch that fallback with the errors included. Otherwise finish Error.
6. Agentic quality evaluation receives claim, target testament/increment, pending requirement and evidence manifest as one coherent entry point. It produces the same verdict shape.
7. Accept only the designated evaluator's current run epoch. Record final verdict before scheduling aggregate completion or notifying a waiter.

A deterministic Fail cannot be bypassed by trying a more lenient handler. Fallback-on-Incomplete is also prohibited by default; missing evidence requires new work/testament rather than a hidden reinterpretation. Every fallback path is in the immutable resolved execution policy. Validator retry does not re-execute an already accepted result during replay.

Receipt validation is pure: it verifies acknowledged response arrival and passes on success or failure testimony. It does not prove dispatch ownership, quality, approval or satisfaction. The active receipt record enforces execution ownership separately.

**Focal decision D-07:** every claim has at least one Required WholeWork receipt validation. Additional Required validations depend on its action profile. This removes accidental empty-set satisfaction while allowing a receipt-only consultation to complete once its answer arrives.

### 6.3 Aggregation and work dependencies

Aggregate only when all Required runs in the applicable phase have final verdicts, or a logged timeout has finalized them. Preserve all results. Deterministic severity is **Fail > Error > Incomplete > Pass**. This selects the public whole-work terminal status; it does not hide remaining errors or missing evidence. Observe runs do not delay whole-work completion and may close into their own audit/evidence scope afterward. Such a late result updates only its run/proof node, never the terminal claim lifecycle or its prior aggregate verdict. Required increment runs can finish while closure is pending; BeginWholeWorkValidation waits until their final immutable outcomes and any required applied-increment evidence are available. A closed testament manifest is not modified when its referenced validation runs produce later verdict nodes.

If every Required WholeWork verdict passes but work dependencies remain unresolved, record `LocalOutcome::Succeeded` and keep public status `validating`. The dependency engine alone can then commit `satisfied`. A local failure is terminal immediately and propagates through DependsOn edges. Admission/Increment local outcomes cannot satisfy the whole claim.

An increment's failing validation rejects that increment with immutable evidence and may lead to corrective claims; it does not immediately terminalize the enclosing work claim. Whole-work validations still judge the closing testament. Source-control merge, disk writing and increment application belong to their owning services; the ledger proves their decisions and outcomes.

## 7. Graph satisfaction and parked scopes

### 7.1 Least-fixpoint rules

For a committed prefix, define `T(v)` as public terminality, `L(v)` as completed successful local WholeWork evaluation, `D(v)` as outgoing DependsOn dependencies, and `A(v)` as outgoing Awaits dependencies. Satisfaction is the least solution:

```text
S(v) = L(v)
       AND every d in D(v) has S(d)
       AND every a in A(v) has T(a)
```

Terminal unsuccessful nodes have `S(v) = false` permanently. The engine starts newly affected nodes at false and grows only justified satisfaction. Two locally complete nodes depending on each other cannot declare each other satisfied merely because assuming both true would be consistent. Local completion is a terminal evaluation outcome, separate from public claim status; this makes Hecate's local-terminal prerequisite explicit without prematurely publishing satisfaction.

Any required dependency with terminal unsuccessful outcome causes `dependency_failed`, with a canonical witness chain. Choose the lowest originating claim sequence, then canonical edge order, when several failure causes exist. Awaits consumes terminal failure as an answer; it never converts that failure into a prerequisite failure automatically.

Example: A consults B through Awaits, while C depends on B through DependsOn. B ends `validation_failed`. A's wait releases with B's failed answer and A may continue; C ends `dependency_failed`. If A and B await one another with no terminal result, neither releases until an explicit deadline/deadlock transition changes the graph.

### 7.2 Monitor implementation contract

Each parked scope owns a bounded materialized transitive blocking closure and its SCC condensation. Use a per-node→monitor subscriber index for affected-only dispatch. The cache holder index is a separate instance; cache eviction and invalidation cannot mutate satisfaction subscriptions.

Registration commits the requested roots/predicates and a prefix watermark, computes/installs the closure at that prefix and subscribes to its suffix atomically. Completion between snapshot and subscription must be replayed before the monitor becomes observable. Every monitor therefore represents one committed prefix, even when its closure spans state shards.

A node dependency whose predicate is settled becomes a released token; do not retain the full historical interior unnecessarily. Monitor state includes stable IDs, predicate, release reason, registration/release positions and durable continuation identity. Runtime transcript snapshots are optional ergonomics; reconstructing from durable graph state cannot lose work after process death.

The monitor releases exactly once at a prefix satisfying its root predicates. A release is a typed committed fact, never a bare channel close. Cross-node wake hints can be lost; the durable inbox cursor plus a bounded reconciliation trigger discovers the release. A hint cannot be the only reason a parked agent wakes.

### 7.3 Deadlock and cancellation

**Focal decision D-08:** initial deadlock breaking is deadline-triggered. On a logged deadline event, evaluate the affected unsatisfied SCC and choose its lowest original claim sequence as victim, tie-broken by ClaimId. Emit `deadlocked` with SCC witness and deadline provenance; downstream failure/release propagation occurs deterministically. Hecate's undeveloped eager-break suggestion is deferred until a separate tested policy specifies its trigger.

A claim expiry outside a qualifying wait SCC yields `expired`. Timers carry generation IDs; stale timer firings after renewal, supersession or terminalization are no-ops. Replay consumes logged timer events and never reads wall time.

Every blocking runtime scope must have a finite effective deadline inherited from its claim or the host's declared work budget. Registration logs that deadline before parking. A closed wait cycle without a reachable deadline is not admitted as an indefinitely parked scope; report the missing bounded-work prerequisite. Deadline-free historical/generated graph records may exist, but cannot silently become permanent live waits.

Cancellation propagates through the durable ownership tree, not every informational graph edge. Terminalize or explicitly detach owned child work under policy, cancel/fence validator executions, then release the scope. Consult answers and unrelated reviewers must not be cancelled merely because they share an informational relation.

All closure counts, edge visits and retained tokens are budgeted per scope/session/owner. A large fleet does not justify an unbounded single-session closure. Budget exhaustion yields typed admission/backpressure and observable accounting; it never silently truncates the dependency graph or claims successful release.

## 8. Affordance, policy, rank and visibility

Affordance is derived from lifecycle, dependencies, standing and field/receipt ownership. It is not stored as a mutable permission flag. The sequencer checks effective state: applied prefix plus all earlier prepared/pending effects. Two concurrent receipts or closes cannot both pass against an obsolete snapshot.

Structural Refuse covers malformed content, forbidden writer crossing, forged provenance, cross-namespace mutation, forbidden self-targeting and typed rank override. Standing denial is Inform/Yield. Every result contains a typed readable reason; a no-op cannot silently swallow a command. Self-transfer/handoff is a distinct authorized action profile, not a blanket exception for self-issued work.

Rank is optional policy integration for deployments that use agent offices, not hardcoded participant class behavior. When enabled, feedback/challenges carry a closed domain. A shipped versioned matrix and logged modulation snapshot determine bindingness; modulation can demote binding→advisory but cannot invert rank. Ordinary testimony and clarification challenges are exempt from override checks.

Only `Invalidates`/override-class `Supersedes` relations trigger the override rule against higher binding feedback. Generate an exhaustive classification from the relation enum; adding a new relation without a ruling fails a compile-time conformance test. A configured protected office cannot be named in challenge targets. Policy and score changes are logged inputs and never synchronous core outcalls.

Serving edges apply tenant/session membership, per-kind read rules and derived source visibility. They filter whole deltas and traversed nodes/edges, never silently redact bytes inside authoritative deltas. A filtered client cursor tracks source positions and explicit delivered ranges; a hidden event must not look like unexplained transport loss. Read decisions and unauthorized-reference behavior must not disclose cross-session existence.

Executing someone else's claim never confers their authority. Evaluators, artifact producers and receipt holders exercise their own standing. Explicit cross-scope grants are independently evaluated and epoch-bound. Imported evidence carries source-scope provenance; content availability alone is not permission to read it.

## 9. Command surface and application obligations

| API family | Commands | Boundary obligations |
|---|---|---|
| Authored work | `GenerateClaim`, `GenerateClaimBatch`, `PostClaim`, `SupersedeClaim` | Canonical fields, scope/requirements resolution, idempotency, relation/rank checks |
| Execution | `AcquireReceipt`, `AdoptReceipt`, `RecordProgress` | Receipt epoch fencing, dependencies, bounded progress, no completion inference |
| Evidence | `BeginEvidenceSet`, `AttachArtifact`, `CloseTestament` | Schema/hash verification, durability pins, immutable close manifest |
| Evaluation | `RecordValidationVerdict` plus runtime schedule/aggregate inputs | Pinned run identity, designated evaluator, error-only fallback, all-required aggregation |
| Waits | `RegisterMonitor`, `RebindMonitor`, `ReleaseScope` | Atomic prefix/suffix registration, explicit successor following, release once |
| Control | `CancelClaim`, runtime `ExpireClaim`, `RevokeClaim`, `BreakDeadlock` | Trusted authority/timer provenance, reason artifacts, owned-child cleanup |
| Reads | `GetObject`, `GetLifecycle`, `Traverse`, `ReadDeltas`, `ResolveArchive` | Visibility, bounded pagination, committed-prefix context, typed archival/resync continuation |

Runtime-only commands are not exposed as arbitrary model tools. A model cannot call `CompleteWholeWork`, forge a timer or post an authority snapshot. The client façade builds claims and testimony; deterministic infrastructure commits lifecycle consequences.

`prepare` computes a complete footprint including objects, edges, reverse indices, dedup identities, receipt/run indices, monitors and aggregate parent consequences. Commands that would exceed a bounded transaction first produce an explicit durable continuation plan; partial application is never exposed as an atomic parent/child result. The storage plan must preserve the same serial semantics when sharding this footprint.

Read/traverse requests carry a stable root, allowed edge filter, depth, result-byte/node budget, continuation and minimum/pinned committed position. Results identify their prefix. Crossing retirement returns a typed archive continuation. Visibility-filtered omission, archived object, genuinely absent object and stale continuation are distinct internal results; external responses respect non-disclosure rules.

Every delta includes schema version, session/position/ordinal, closed lifecycle action, actor provenance, delivery class, deterministically ordered refs and sufficient committed facts for its concern. One command may generate multiple lifecycle deltas. The machine-checked bijection is **lifecycle action↔delta action**, not command↔single delta.

## 10. Required domain conformance fixtures

The implementation plan must provide executable fixtures for these behaviors; imported prose alone is not evidence:

1. Durable generated claim cannot execute before post; admission failure prevents receipt.
2. Identical content/retry returns one opaque object identity and receipt; a new occurrence creates a new obligation; expired request generations cannot silently re-execute.
3. Every state/command cell yields the specified transition, Inform, Yield or Refuse, including all terminal states.
4. Stream two artifacts, close once, acknowledge once, run deterministic then agentic validation and satisfy with exact evidence hashes.
5. Deterministic Fail never invokes fallback; Error does; Observe failure never blocks a Required result.
6. Wrong receipt/evaluator epoch, forged cause and cross-session edge cannot mutate state.
7. Parent/child completion and monitor release are atomic at a committed prefix, including across state shards.
8. Preallocated atomic graph batches and logged runtime wait edges permit legal cycles; mixed Awaits/DependsOn failures and SCC cycles match a brute-force least-fixpoint oracle.
9. Timeout/cancel/close races, receipt adoption and supersession have one deterministic outcome under duplicate/reordered delivery.
10. Restart with validators and handlers disabled reproduces identical state and ordered deltas from logged outcomes.
11. Lost final wake hint, consumer behind retention and ownership movement still recover every parked scope.
12. Retired proofs retain original stable IDs, canonical content, all verdicts/artifacts, relation provenance and hash-verifiable lineage; cold identity lookup cannot accept stale absence.

Maintain these fixtures across laptop, multi-node, multi-region topology tests using the same domain engine and stable IDs. Topology can change admission, availability, placement and latency; it cannot change what a claim means or what evidence satisfies it.
