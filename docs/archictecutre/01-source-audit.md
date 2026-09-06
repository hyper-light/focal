# Source audit: Hecate's design and Sylk's implementation

This audit separates observed implementation from proposed architecture. Hecate supplies
the target domain model and distributed substrate design. Sylk supplies the running Go
implementation, integration experience, and regression scenarios. Focal will implement
the design in Rust with RAM as primary storage and disk-backed durability. The disk log
is part of the design; an embedded SQL database is not the target state store.

Audit date: 2026-09-05. Sylk source revision inspected:
`50154e6159c7ed590728b82423dde3e7fc977c26`. Its working tree had unrelated local changes
under `.claude/`; this audit did not change sibling repositories. Source inspection and
existing test names establish the findings below. No Sylk tests were executed for this
documentation task. Historical bug documents describe earlier incidents, not necessarily
defects still present at this revision. New static concerns are explicitly labeled.

## 1. Authority and scope

Hecate is a design repository in the inspected checkout: architecture, specifications,
ADRs, context, and gap analysis. Its Rust snippets define intended seams rather than
implemented crates. A port cannot assume its proposed runtime, transport, storage,
consensus, materializer, or archive already exists. Focal's implementation plan must
provide each required capability or explicitly define the adapter that supplies it.

The main domain authority is [Hecate's Ledger architecture](reference/hecate/docs/architecture/LEDGER.md).
The machinery is specified in [Ledger core](reference/hecate/docs/specs/LEDGER_CORE.md)
and [Ledger substrate](reference/hecate/docs/specs/LEDGER_SUBSTRATE.md). The architecture
inherits Sylk's concepts but intentionally removes compatibility statuses, mutable
authored/lifecycle mixtures, and a separate outbox. Source amendments and conflicts
must be resolved by the target architecture, not silently combined into an implementation.

The intended deployment range is one laptop through multi-region, Meta-scale operation
on the same architecture. Neither sibling establishes a measured capacity claim for that
range: Hecate is design-only, and Sylk's single board lock, graph walks, independent
outbox, and whole-state snapshots are implementation references rather than a scale
target. No per-session small-workload assumption is justified. Session sequencing,
parallel apply, graph placement, replication, and regional ownership must have explicit
capacity limits and overload behavior validated by the target's own measurements.

Hecate calls the authority a **ledger**; Sylk calls it a **ClaimsBoard**. The ledger owns
claims, testaments, artifacts, validations, typed relations, and lifecycle. Configuration,
telemetry content, runtime orchestration state, and VFS bytes have their own owners.
Trace references and typed error evidence may link to operational systems without
making their full content ledger state.

Two stores in Sylk must be distinguished:

| Store | Implemented role | Authority consequence |
|---|---|---|
| `core/claims/ClaimsBoard` | Per-session or per-pipeline graph in maps guarded by one `sync.RWMutex` | Owns accepted claim mutations and lifecycle |
| `core/claims/DurableBoard` | WAL, snapshots, replay, retirement, and projection delivery around the board | Durability wrapper for the claim authority |
| `core/forest/forest_ledger` | SQLite append-only canonical input for memory-forest projections | Downstream archive/projection input, not the board's transactional store |

Evidence: [board.go](../../../sylk/core/claims/board.go), `ClaimsBoard` at line 32;
[board_durable.go](../../../sylk/core/claims/board_durable.go), `DurableBoard` at line 101;
[forest/ledger.go](../../../sylk/core/forest/ledger.go), `LedgerRecord` and `AppendCanonicalDelta`.

## 2. Implemented object model

[types.go](../../../sylk/core/claims/types.go) defines `Action` at line 694, `Claim` at
719, `Testament` at 831, `Artifact` at 896, and `Validation` at 938. Most objects carry
IDs, participant/agent attribution, session/conversation/pipeline/task context,
sequence, relations, and creation/access timestamps.

| Object | Sylk behavior | Rust design implication |
|---|---|---|
| Action | Batch envelope with action type, status, and idempotency key | Specify atomic batch boundaries; do not accidentally add a fifth independently authoritative domain family |
| Claim | Directed obligation; owns validation declarations, scope, intent, priority/deadline, and mutable lifecycle | Store authored content separately from runtime lifecycle |
| Testament | Claim response with summary, confidence, result context, and attached artifacts | Closing testimony must be durable before acknowledging handler completion |
| Artifact | Named, typed payload or reference; includes hash, size, errors, lifecycle, and presentation metadata | Separate immutable evidence bytes from artifact lifecycle and presentation projections |
| Validation | Claim-owned gate targeting an artifact name; carries handler/type contracts, requiredness, quality bar, result artifact, error, and evaluator | Pin exact target evidence and validation execution identity before dispatch |

Sylk's comments call testaments and artifacts immutable, but the structs include mutable
lifecycle, status history, access timestamps, and context. The operational meaning is
immutable evidence/content plus mutable runtime metadata. Hecate makes that separation
structural: private content and lifecycle write paths in separate slot members. Rust
must encode this distinction rather than copy the mixed Go structs wholesale.

`ClaimsBoard` owns validations inside claims and artifacts inside testaments. A separate
artifact map indexes both attached evidence and the generated-but-unattached window.
Therefore “every artifact has a testament” is only a post-attachment invariant; artifact
generation can precede the closing testament. The Rust state machine needs an explicit
unattached state, not a fabricated parent ID.

Generated testament checks live in
[board_lifecycle.go](../../../sylk/core/claims/board_lifecycle.go),
`validateGenerateTestamentActionLocked`, `validateGeneratedTestamentArtifactsLocked`,
`validateTestamentArtifactDuplicate`, and `validateTestamentArtifactParentage`:

- A testament requires summary or context and a resolving claim relation unless standalone mode is explicitly enabled.
- Duplicate testament IDs and duplicate artifact IDs or names within one testament are rejected.
- Existing generated artifacts may attach only from generated/received states and may not already have a parent testament.
- Claim, participant, and conversation parentage are checked before attaching an existing artifact.
- Artifact kind is required; size cannot be negative; typed payload declarations are validated.
- Strict generation requires a durable reference or content hash, with an explicit exemption for ephemeral progress markers.

`SubmitTestaments` is a compatibility wrapper that permits empty references. This
exception must not become an implicit target contract. Hecate's streaming evidence and
closing-commit model requires explicit byte/reference custody and a complete attachment
manifest before the testament commit succeeds.

## 3. Lifecycles, receipts, and actor authority

[lifecycle.go](../../../sylk/core/claims/lifecycle.go) implements checked claim and
testament graphs. Claim lifecycle is the only stored claim status; coarse status is
derived for older callers, with legacy conversion at decode. Hecate removes that
compatibility vocabulary entirely.

Canonical claim progression is generated → posted → received → progressed →
testament_generated → testament_acknowledged → validating → satisfied. Boundary
failures and missing/failed/errored validation are distinct terminal outcomes. Sylk
allows compressed paths for synchronous work; transitions are checked, not inferred
from text. Same-state transitions are idempotent, and terminal states cannot reopen.
Sylk additionally has a `drained` handoff force-close state. This is an observed extension,
not automatic permission to add it to Hecate's closed target vocabulary.

Testament progression is generated → posted → received → validating →
validated / validation_incomplete / validation_failed / validation_errored.
The parent claim and its response are related state machines, not synonyms.

The implementation makes generation and posting separate board methods:
`GenerateClaimAction`, `GenerateClaim`, `PostGeneratedClaims`,
`GenerateTestamentAction`, and `PostGeneratedTestaments` in `board_lifecycle.go`.
Generation keys survive recovery; posting an already posted batch is idempotent.
Generated work must not wake a target. `PostAction` and `SubmitTestaments` in
`board.go` are wrappers around these more explicit operations.

Posting resolves canonical issuer, subject, and evaluator identities, checks relation
structure and self-targeting, and applies the post policy. Receipt methods verify the
receiver against the appropriate relation. Review `resolveClaimPostIdentity`,
`validateClaimQualityBarEligibility`, `validateLifecycleReceiver`,
`canAcknowledgeClaimReceipt`, and `canAcknowledgeClaimTestament`. The target should
derive authority from a validated principal and stamped relations, not caller-supplied
actor strings or cached display names.

Receipt handling is a major integration seam. Sylk's `satisfyUngatedClaimsForTestaments`
auto-completes non-error testaments only when their claim has zero required validations.
Claims with required receipt gates use `SatisfyReceiptForTestament`. Claims with other
required gates must await those checks. Consult/challenge receipt authority differs
from routed-work receipt authority in Sylk; a port must not inherit two racing owners.
Hecate specifies pure receipt auto-pass in ledger core; the target must define exactly
which committed arrival/receipt record triggers it, once.

Progress is advisory. `UpdateClaimProgress` returns a typed `ClaimTerminalError` on a
terminal claim. The LLM-facing skill converts that into an informative success with
`recorded:false`. `SetClaimContext` silently ignores terminal updates, sealing the
terminal narrative. `TouchClaimActivity` is an in-memory liveness heartbeat without a
WAL append or projection event. Target durable deadlines must remain explicit replay
inputs; local heartbeats are not deterministic timeout decisions.

## 4. Typed artifacts and validators

[type_registry.go](../../../sylk/core/claims/type_registry.go) maps stable datatype
strings to immutable codec registrations and Go types. Duplicate datatype registrations,
duplicate Go-type registration under another name, unknown types, and nil codecs are
errors. [artifact_data.go](../../../sylk/core/claims/artifact_data.go) copies encoded bytes,
stamps SHA-256 and size, verifies them when decoding, and checks the requested type.
Existing codec calls contain panic recovery; Hecate instead requires fallible typed
handlers and treats a process panic as a runtime fault, not validation evidence plumbing.

[validator_registry.go](../../../sylk/core/claims/validator_registry.go) implements
`ValidatorRegistry`, generic `RegisterValidator<T,R>`,
`ProgrammaticValidatorDispatcher`, and `BoardValidatorDispatcher`. Registrations
include validator ID, validation/action type, determinism class, timeout, concurrency
budget, target artifact name, input datatype, and result datatype.

- Registrations require positive bounded timeout/concurrency, valid determinism, a handler, and registered payload types.
- Re-registering identical immutable metadata is idempotent; conflicting metadata for the same key is rejected.
- Lookup prefers exact validator/action matches before configured wildcard combinations.
- Input parentage/type/size/policy checks precede handler execution; output type/size checks precede committing results.
- Local in-flight validation IDs suppress duplicate dispatch; per-registration semaphores bound concurrent handlers.
- Timeout, handler failure, type mismatch, policy refusal, and dispatcher failure become structured validation outcomes.
- Recovery re-dispatches pure/content handlers; side-effecting/nondeterministic handlers require recorded idempotency evidence.

Distributed Focal needs durable attempt IDs and owner fencing around those local
protections. A process-local semaphore and `inFlight` map cannot prevent an old leader's
late result from committing after a new leader schedules the same validation. Pin handler
version, target content identity, attempt number, and authority epoch in the committed
dispatch input and reject mismatched result commits.

[artifact_validation_lifecycle.go](../../../sylk/core/claims/artifact_validation_lifecycle.go)
defines artifact states generated / generation_failed / received / receipt_failed /
attached / validating / validation_failed / validated. Validation has ready, validating,
optional/required failure or error, validating_quality_bar, quality-bar failure,
validated, and validation_incomplete. Requiredness affects parent blocking behavior;
optional failure is not equivalent to a passed required gate.

[artifact_orchestrator.go](../../../sylk/core/claims/artifact_orchestrator.go) matches
validation declarations to artifact names, starts validation, dispatches bounded work,
short-circuits remaining work on blocking failure, and commits the artifact result.
[claim_orchestrator.go](../../../sylk/core/claims/claim_orchestrator.go) checks expected
evidence names, validates artifacts, rolls outcomes into the testament and claim, and
builds a result testament. `missingValidationArtifacts` currently considers every named
validation declaration, including optional ones; requiredness semantics should be
specified deliberately in the Rust implementation rather than inferred from that helper.

[quality_bar.go](../../../sylk/core/claims/quality_bar.go) owns the agentic second phase.
The board dispatcher maps deterministic success to `validating_quality_bar` when the
gate declares a quality bar. Agentic evaluation must not start before deterministic
success. Result evidence is committed through
[result_testament.go](../../../sylk/core/claims/result_testament.go),
`BuildValidationResultTestament`, with a stable default key based on claim and source
testament. Replaying history applies stored results; it must not execute validators.

## 5. Board storage, commit path, and recovery

`ClaimsBoard` keeps maps, relation indexes, generation-key indexes, ordered claim IDs,
cached projections, and summary counters in RAM. Reads clone entities or use immutable
projections; mutations take its write lock. This is a useful single-owner semantic
baseline but has no network consensus, shard placement, or distributed ownership fencing.

`OpenDurableBoard` stores data below
`<session>/protocols/claims_board/<board>/wal/`. `events.wal.jsonl` has sequence,
event ID, board ID, event kind, actor, time, and payload. An exclusive file lock prevents
two local writers from interleaving lines. With no session directory, the same wrapper
can run entirely in memory; that mode is not evidence of crash durability.

`appendDurableEventLocked` calls `appendCommittedEvent`; `appendEvent` serializes a
record, appends it, optionally syncs according to the configured durability mode, and
then permits board mutation. Failed WAL append prevents the intended board state change.
The wrapper deduplicates logical payload fingerprints as well as application generation
keys. Focal should distinguish request identity from payload identity and define which
IDs survive retries, snapshots, retention, and leader failover.

`replayWAL` restores a snapshot and applies the tail using persisted sequences, with
legacy positional fallback. It reports malformed JSON, unknown events, missing
references, duplicate events, invalid transitions, panics, and invalid snapshots.
It continues beyond individual bad records. That recovery policy is inappropriate to
copy blindly into a replicated committed log: corruption inside the committed prefix
must not silently produce a different authoritative graph on different replicas.

Snapshots capture board state and retirement indexes. Retirement moves terminal,
quiescent claim subgraphs to `retired.jsonl`; it appends archive bytes first, commits
their offsets in a WAL event, then removes hot objects. Referenced live work pins the
subgraph. ID-to-offset read-through preserves later entity lookup. This provides a
useful custody-transfer pattern, but its permanent in-memory index still grows with
history; it is not by itself a fully bounded distributed archive design.

Evidence: [board_retirement.go](../../../sylk/core/claims/board_retirement.go),
`RetireQuiescent` and `boardRetirement.appendRecord`;
[board_compaction.go](../../../sylk/core/claims/board_compaction.go), `CompactWAL`.
Hecate replaces disk read-through with paced retirement, verified content custody,
durable watermarks, and typed archive continuations. The target must account for live
state, pending mutations, indexes, monitor closures, consumer lag, and pinned segments.

## 6. Delivery and the separate forest ledger

Sylk's [outbox.go](../../../sylk/core/claims/outbox.go) has independent JSONL records
and per-projector states: pending, in_progress, succeeded, failed_retryable, and
failed_terminal. Record identity combines board, WAL sequence, entity type/ID, and
mutation kind. Each projector has a lease, retry count, error, and update timestamp.
Projection work is bounded in batches, and canonical delivery prioritizes activation
and resolution while preserving per-entity order. It does not require global
cross-entity emission order.

Hecate intentionally deletes this structure. Committed transition records are the
replay source; deterministic deltas and projections consume durable cursors from that
log. A stuck projection alarms in health; retention loss produces typed RESYNC. Focal
must not create another independently durable outbox table or queue alongside the log.
Kafka-like durability here describes the segmented retained log and resumable consumers;
it is not a requirement to introduce Apache Kafka as a dependency.

Sylk's [canonical_delta_projector.go](../../../sylk/core/claims/canonical_delta_projector.go)
rehydrates entities from current board state to emit queued mutation deltas. This is
adequate evidence of an existing projection mechanism, not proof that old deltas are
byte-identical when emitted after later mutations. Focal must derive each historical
delta from its immutable committed transition and pinned encoding schema.

The forest's `AppendCanonicalDelta` inserts a canonical ledger row and evidence
projection in a SQL transaction. `forest_ledger.source_key` is unique; payloads,
relations, delivery records, and projection offsets have separate tables.
[schema_phase123.go](../../../sylk/core/forest/schema_phase123.go) defines these tables
and append-only enforcement. Artifact/validation projections are downstream state;
they cannot validate or authorize new board writes by themselves.

[forest/delta_ingestor.go](../../../sylk/core/forest/delta_ingestor.go) subscribes to
canonical topics through a bounded in-memory channel. On full capacity, `handleDelta`
records overflow rather than enqueuing the delta. Therefore observed bus publication
success is not proof of durable forest ingestion. Target consumers need acknowledgment
after durable cursor/effect commit, retained replay, and explicit pressure behavior.

## 7. Static hazards to exclude from the Rust design

These are code-inspection findings, not reproduced Sylk bug reports. They justify
target failure-injection tests and stronger state ownership; this task does not fix Sylk.

| Observed seam | Concrete source evidence | Required target property |
|---|---|---|
| Snapshot aliases mutable maps after releasing the board read lock | `board_durable.go:SaveSnapshot` places live maps into `walCheckpoint`, unlocks, then marshals | Pin immutable state at one committed index or copy while ownership is stable |
| Whole-WAL truncation after separately completed snapshot | `board_compaction.go:CompactWAL` calls `SaveSnapshot`, then later locks and truncates | Rotate segments; discard only a proven durable snapshot prefix and retain all newer commits |
| Independent WAL/outbox writes | `board_durable.go:appendCommittedEvent` appends WAL then inserts outbox records | Use the one committed log as delivery history; no cross-journal atomicity gap |
| Error reporting re-enters the board lock | `appendCommittedEvent` calls `RecordNotificationError` on outbox failure while board mutation callers hold `b.mu` | Emit diagnostics outside owner-critical sections; fault-inject every I/O failure path |
| Outbox writes can advance memory before journal success | `outbox.go:insertLocked` and `setProjectorStatus` update maps before append; `Claim` alone explicitly rolls back | Cursor/effect success is visible only after its chosen durability boundary |
| Lease completion is identified only by record/projector | `MarkSucceeded` and `MarkFailed` do not require a lease token | Fence every completion against attempt/owner epoch; expired workers cannot acknowledge replacement work |
| WAL replay buffers the complete file | `replayWAL` uses `os.ReadFile` then splits every line | Stream framed records with configured maximum lengths and bounded working memory |
| Historical projection reads current entities | `canonicalDispatchesForOutboxRecord` clones current claim/testament/validation | Commit sufficient immutable event data for deterministic historical replay |

Hecate's sequencer/apply split addresses these boundaries: effective state includes
ordered pending mutations for admission checks, state becomes authoritative only on
durable commit acknowledgment, and parallel materialization preserves the sequencer's
order. Merely replacing `RWMutex` with concurrent Rust maps would not supply these
properties. There is one session order; physical graph/application partitions may scale.

## 8. Agent integration lessons and regression fixtures

The shared intake chokepoint is
[agents/shared/claims_intake.go](../../../sylk/agents/shared/claims_intake.go).
`verifyIntakeOutcome` requires a successful directed handler to leave durable testimony,
a terminal state, or live downstream work. A success return with no recorded outcome
is converted to a durable failure. `entryClaimIsTerminal` suppresses terminal replay;
deadline and inactivity paths bound execution, and activity guards prevent premature
resource release while work still runs.

| Historical incident | Mechanism and lesson | Existing evidence |
|---|---|---|
| Handoff falsely fails after successful ingestion | Async receipt write lands after synchronous outcome check; commit the receipt before returning success | [handoff receipt race](../../../sylk/docs/bugs/2026-06-17-handoff-receipt-testament-async-race-false-fail.md), orchestrator synchronous testament helper |
| Request claim stays testament_generated | Receipt satisfaction depended on a racy routed delta; assign one deterministic receipt authority | [resume/replay incident](../../../sylk/docs/bugs/2026-06-15-resume-replay-strands-request-claims-and-reprocesses-terminal-claims.md), accumulator receipt tests |
| Completed work is replayed and shown errored | Resume redelivers terminal claims and orphaned turns narrate failures; terminal replay must be inert | [terminal repaint](../../../sylk/docs/bugs/2026-06-15-terminal-claim-repainted-by-orphaned-turn-error.md), intake terminal tests |
| Plan approval rejects an unchanged render | A freshly minted artifact UUID was treated as stable plan identity; distinguish evidence identity from render instance and approval supersession | [volatile artifact identity](../../../sylk/docs/bugs/2026-06-15-plan-approval-verdict-rejected-stale-volatile-artifact-uuid.md), plan review tests |
| Progress after testimony becomes tool failure | Terminal progress is moot; expose typed current-state affordance instead of initiating failure/remediation | [terminal progress](../../../sylk/docs/bugs/2026-06-19-update-claim-progress-terminal-claim-hard-error.md), progress skill tests |
| Duplicate artifact attachment strands a response | Accumulator combines repeated evidence; deduplicate before closing while retaining strict unique attachment validation | [duplicate artifact incident](../../../sylk/docs/bugs/2026-06-15-architect-request-claims-non-terminal-duplicate-artifact.md), accumulator tests |

Do not port UI-specific workarounds as ledger semantics. Preserve the demonstrated
invariants through typed APIs, atomic commits, stable content identity, and one owner
per lifecycle decision. Runtime scheduling, external tool calls, and evaluator execution
must happen outside the deterministic reducer, with their results committed as inputs.

## 9. Existing tests worth translating into Rust conformance scenarios

| Sylk test source | Concrete examples and target use |
|---|---|
| [lifecycle_test.go](../../../sylk/core/claims/lifecycle_test.go) | `TestClaimLifecycleTransitionGraphExhaustive`, `TestGeneratedTestamentPostsBeforeClaimResolution`, `TestGenerateClaimActionDoesNotWakeTargetUntilPosted`, `TestConcurrentPostGeneratedClaimIsIdempotent`: closed-state sweep, authority, generation/post separation, concurrent retries |
| [board_durable_test.go](../../../sylk/core/claims/board_durable_test.go) | `TestDurableBoardWALFirstThenMutate`, `TestDurableBoardPersistAndRecover`, `TestDurableBoardSnapshotRecovery`: commit visibility and restart equivalence |
| [validator_registry_test.go](../../../sylk/core/claims/validator_registry_test.go) | Handler failure/timeout/optional outcomes, immutable registration conflicts, pre-dispatch type rejection, duplicate dispatch, unparented input: typed validator contract |
| [artifact_validation_integration_test.go](../../../sylk/core/claims/artifact_validation_integration_test.go) | `TestFixtureClaimPassesBoardValidationAndDurableReplay`, `TestConcurrentProjectionClonesDoNotExposeArtifactOrValidationSlices`: full evidence flow and read isolation |
| [board_retirement_test.go](../../../sylk/core/claims/board_retirement_test.go) | `TestRetireQuiescent_KeepsTerminalClaimReferencedByLiveWork`, `TestRetirement_RecoveryReproducesHotRetiredSplit`: pinning, custody, crash recovery |
| [claims_intake_terminal_test.go](../../../sylk/agents/shared/claims_intake_terminal_test.go) | `TestClaimsIntakeSkipsAlreadyTerminalClaims`: replay does not restart completed work |
| [skills_update_claim_progress_test.go](../../../sylk/core/claims/skills_update_claim_progress_test.go) | Typed terminal result remains information at the agent boundary |
| [ledger_phase123_test.go](../../../sylk/core/forest/ledger_phase123_test.go) | Append-only ledger, canonical delta ingestion, projection identity: replayable consumer effects |

These tests are starting fixtures, not evidence of distributed correctness. The target
adds model-based lifecycle/monitor oracles, byte-identical replay, schema-version tests,
concurrent pending admission, crash-at-every-commit-boundary testing, consensus partition
and stale-leader cases, snapshot/compaction interleavings, consumer cursor fencing,
archive custody recovery, and bounded-memory saturation. The root implementation plan
owns the exact acceptance gates and milestone dependencies.
