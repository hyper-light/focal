//! Native wire profile (protocol 4). Participants submit borrowed `FCNINPUT`
//! frames that the authoritative owner decodes exactly as journaled; native
//! reads and bounded lists return explicit documents mirroring committed native
//! rows. This module inspects only a frame's fixed header before dispatch and
//! never decodes a body, grants authority or interprets an outcome.
use crate::*;
pub use focal_model::native_event::*;
use focal_model::*;
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "native_tests.rs"]
mod tests;

pub const NATIVE_PROTOCOL_VERSION: u16 = 4;
/// The frame magic and format version fixed by the native input format (21 §3).
pub const NATIVE_FRAME_MAGIC: [u8; 8] = *b"FCNINPUT";
pub const NATIVE_FRAME_VERSION: u16 = 1;
/// Common header (44 bytes) plus principal, request epoch and request ID.
pub const NATIVE_ACTOR_HEADER_BYTES: usize = 84;
/// The actor header and its command byte: the least a request frame carries.
pub const NATIVE_REQUEST_MIN_BYTES: usize = 85;
/// Command tags 0..=27 are registered by the input format; timers use namespaces.
pub const NATIVE_COMMAND_TAGS: u8 = 28;
pub const NATIVE_ACTOR_NAMESPACE: u8 = 0;
pub const MAX_NATIVE_LIST_CURSOR_BYTES: usize = 256;
/// Residual filtering may visit this many rows for one page.
pub const MAX_NATIVE_LIST_VISITS: u32 = 65_536;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeProfile {
    ProjectionOnly,
    AuthoredV1,
}
impl NativeProfile {
    pub const fn registered_tag(self) -> u8 {
        match self {
            Self::ProjectionOnly => 0,
            Self::AuthoredV1 => 1,
        }
    }
    pub const fn from_registered(tag: u8) -> Option<Self> {
        match tag {
            0 => Some(Self::ProjectionOnly),
            1 => Some(Self::AuthoredV1),
            _ => None,
        }
    }
}

/// The fixed header of one request frame. Nothing here is an admitted fact; the
/// owner re-derives every identity from the same bytes under its own checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeFrameHeader {
    pub profile: NativeProfile,
    pub namespace: u8,
    pub ledger: LedgerId,
    pub key: RequestKey,
    pub command: u8,
}

fn id16(frame: &[u8], range: std::ops::Range<usize>) -> Result<[u8; 16], AccessError> {
    frame
        .get(range)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or(AccessError::InvalidRequest)
}

/// Structural inspection of the fixed header only. Timer namespaces parse but
/// are never admissible from a participant.
pub fn inspect_native_frame(frame: &[u8]) -> Result<NativeFrameHeader, AccessError> {
    if frame.len() < NATIVE_REQUEST_MIN_BYTES
        || frame.get(0..8) != Some(&NATIVE_FRAME_MAGIC[..])
        || frame
            .get(8..10)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            != Some(NATIVE_FRAME_VERSION)
    {
        return Err(AccessError::InvalidRequest);
    }
    let profile = frame
        .get(10)
        .copied()
        .and_then(NativeProfile::from_registered)
        .ok_or(AccessError::InvalidRequest)?;
    let namespace = frame.get(11).copied().ok_or(AccessError::InvalidRequest)?;
    let ledger = LedgerId {
        tenant: TenantId(id16(frame, 12..28)?),
        session: SessionId(id16(frame, 28..44)?),
    };
    let principal = ParticipantId(id16(frame, 44..60)?);
    let epoch = frame
        .get(60..68)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u64::from_le_bytes)
        .ok_or(AccessError::InvalidRequest)?;
    let id = RequestId(id16(frame, 68..84)?);
    let command = frame.get(84).copied().ok_or(AccessError::InvalidRequest)?;
    if command >= NATIVE_COMMAND_TAGS {
        return Err(AccessError::InvalidRequest);
    }
    Ok(NativeFrameHeader {
        profile,
        namespace,
        ledger,
        key: RequestKey {
            principal,
            epoch: RequestEpoch(epoch),
            id,
        },
        command,
    })
}

/// A frame is admissible only when its own identity is exactly the authenticated
/// envelope: the principal is the peer, the request identity is the envelope's,
/// the ledger is the envelope's and the namespace is the actor namespace.
pub fn native_frame_admissible(
    frame: &[u8],
    peer: &AuthenticatedPeer,
    envelope: &RequestEnvelope,
) -> Result<NativeFrameHeader, AccessError> {
    let header = inspect_native_frame(frame)?;
    if header.namespace != NATIVE_ACTOR_NAMESPACE
        || header.ledger != envelope.ledger
        || header.key.principal != peer.principal()
        || matches!(peer.role(), PeerRole::Node { .. })
    {
        return Err(AccessError::Unauthorized);
    }
    if header.key.epoch != envelope.request_epoch
        || header.key.id != envelope.request_id
        || header.key.principal.is_zero()
        || header.key.id.is_zero()
        || header.key.epoch.0 == 0
    {
        return Err(AccessError::InvalidRequest);
    }
    Ok(header)
}

/// Operations of the committed native prefix, including trusted timers and the
/// legacy import. Registered numbers are protocol values, not enum layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeOperationKind {
    RegisterMonitor,
    RebindMonitor,
    CancelMonitor,
    MonitorDeadline,
    ReleaseScope,
    GenerateResultTestament,
    PostResultTestament,
    EnterWholeWork,
    SealIncrementTargets,
    BeginIncrement,
    ReportIncrement,
    FailWorkProduction,
    RejectWork,
    SubmitWork,
    SubmitDiagnostic,
    ReceiveWork,
    CloseResponse,
    PostResponse,
    ReceiveResponse,
    AcquireReceipt,
    AdoptReceipt,
    Create,
    Cancel,
    Post,
    BeginAdmission,
    ReportAdmission,
    BeginWork,
    ReportWork,
    EvaluationDeadline,
    ClaimDeadline,
    Import,
}
impl NativeOperationKind {
    pub const ALL: [Self; 31] = [
        Self::RegisterMonitor,
        Self::RebindMonitor,
        Self::CancelMonitor,
        Self::MonitorDeadline,
        Self::ReleaseScope,
        Self::GenerateResultTestament,
        Self::PostResultTestament,
        Self::EnterWholeWork,
        Self::SealIncrementTargets,
        Self::BeginIncrement,
        Self::ReportIncrement,
        Self::FailWorkProduction,
        Self::RejectWork,
        Self::SubmitWork,
        Self::SubmitDiagnostic,
        Self::ReceiveWork,
        Self::CloseResponse,
        Self::PostResponse,
        Self::ReceiveResponse,
        Self::AcquireReceipt,
        Self::AdoptReceipt,
        Self::Create,
        Self::Cancel,
        Self::Post,
        Self::BeginAdmission,
        Self::ReportAdmission,
        Self::BeginWork,
        Self::ReportWork,
        Self::EvaluationDeadline,
        Self::ClaimDeadline,
        Self::Import,
    ];
    pub const fn registered_tag(self) -> u8 {
        match self {
            Self::RegisterMonitor => 0,
            Self::RebindMonitor => 1,
            Self::CancelMonitor => 2,
            Self::MonitorDeadline => 3,
            Self::ReleaseScope => 4,
            Self::GenerateResultTestament => 5,
            Self::PostResultTestament => 6,
            Self::EnterWholeWork => 7,
            Self::SealIncrementTargets => 8,
            Self::BeginIncrement => 9,
            Self::ReportIncrement => 10,
            Self::FailWorkProduction => 11,
            Self::RejectWork => 12,
            Self::SubmitWork => 13,
            Self::SubmitDiagnostic => 14,
            Self::ReceiveWork => 15,
            Self::CloseResponse => 16,
            Self::PostResponse => 17,
            Self::ReceiveResponse => 18,
            Self::AcquireReceipt => 19,
            Self::AdoptReceipt => 20,
            Self::Create => 21,
            Self::Cancel => 22,
            Self::Post => 23,
            Self::BeginAdmission => 24,
            Self::ReportAdmission => 25,
            Self::BeginWork => 26,
            Self::ReportWork => 27,
            Self::EvaluationDeadline => 28,
            Self::ClaimDeadline => 29,
            Self::Import => 30,
        }
    }
    /// Trusted operations never arrive as participant frames.
    pub const fn participant_authored(self) -> bool {
        !matches!(
            self,
            Self::MonitorDeadline | Self::EvaluationDeadline | Self::ClaimDeadline | Self::Import
        )
    }
    pub const fn name(self) -> &'static str {
        match self {
            Self::RegisterMonitor => "register_monitor",
            Self::RebindMonitor => "rebind_monitor",
            Self::CancelMonitor => "cancel_monitor",
            Self::MonitorDeadline => "monitor_deadline",
            Self::ReleaseScope => "release_scope",
            Self::GenerateResultTestament => "generate_result_testament",
            Self::PostResultTestament => "post_result_testament",
            Self::EnterWholeWork => "enter_whole_work",
            Self::SealIncrementTargets => "seal_increment_targets",
            Self::BeginIncrement => "begin_increment",
            Self::ReportIncrement => "report_increment",
            Self::FailWorkProduction => "fail_work_production",
            Self::RejectWork => "reject_work",
            Self::SubmitWork => "submit_work",
            Self::SubmitDiagnostic => "submit_diagnostic",
            Self::ReceiveWork => "receive_work",
            Self::CloseResponse => "close_response",
            Self::PostResponse => "post_response",
            Self::ReceiveResponse => "receive_response",
            Self::AcquireReceipt => "acquire_receipt",
            Self::AdoptReceipt => "adopt_receipt",
            Self::Create => "create",
            Self::Cancel => "cancel",
            Self::Post => "post",
            Self::BeginAdmission => "begin_admission",
            Self::ReportAdmission => "report_admission",
            Self::BeginWork => "begin_work",
            Self::ReportWork => "report_work",
            Self::EvaluationDeadline => "evaluation_deadline",
            Self::ClaimDeadline => "claim_deadline",
            Self::Import => "import",
        }
    }
}

/// Committed native outcome at its native prefix position. Counts describe
/// rows the record wrote, never a satisfaction assertion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeOutcomeCounts {
    pub created: u32,
    pub changed: u32,
    pub definitions: u32,
    pub evaluations: u32,
    pub artifacts: u32,
    pub results: u32,
    pub receipts: u32,
    pub responses: u32,
    pub result_testaments: u32,
    pub events: u32,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeReceipt {
    pub invocation: NativeInvocationRef,
    pub sequence: SessionSeq,
    pub logical_time: u64,
    pub operation: NativeOperationKind,
    /// Private native intent identity of the admitted input; exact retry with
    /// the same frame reproduces it.
    pub intent: ContentHash,
    pub counts: NativeOutcomeCounts,
}
/// Accepted but not known committed. Resending the exact frame resolves it;
/// this is never reported as success.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeTicket {
    pub key: RequestKey,
    pub intent: ContentHash,
}
/// Deterministic contract refusals, one per lifecycle contract error, plus the
/// engine conditions a participant can observe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeErrorCode {
    WrongActor,
    WrongLedger,
    WrongObject,
    ContentConflict,
    StaleRevision,
    StaleReceipt,
    StaleEvaluation,
    InvalidTransition,
    InvalidTarget,
    InvalidManifest,
    MissingEvidence,
    InvalidPolicy,
    Capacity,
    ConflictingCause,
    InvalidCut,
    /// The object belongs to the frozen legacy prefix and accepts no native
    /// completion operation.
    Legacy,
    Unsupported,
}
/// Closed refusal categories mapped to stable client exit codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeRefusalKind {
    InvalidInput,
    Unauthorized,
    NotFound,
    Stale { binding: NativeBinding },
    Conflict,
    Capacity,
    Refused(NativeErrorCode),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeRefusal {
    pub kind: NativeRefusalKind,
    pub detail: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeMutationReply {
    Committed(NativeReceipt),
    Pending(NativeTicket),
    Refused(NativeRefusal),
}

/// Addresses of committed native rows. Legacy families carry frozen V1 bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeObjectRef {
    Claim(ClaimId),
    Definition(ValidationId),
    Evaluation(NativeEvaluationKey),
    Result(NativeResultRef),
    Artifact(ArtifactId),
    Work(ArtifactId),
    Diagnostic(ArtifactId),
    Response(TestamentId),
    ResultTestament(TestamentId),
    Receipt(ReceiptId),
    Monitor { claim: ClaimId, id: MonitorId },
    Outcome(NativeInvocationRef),
    CreationResult(NativeInvocationRef),
    Event { sequence: SessionSeq, ordinal: u32 },
    LegacyTestament(TestamentId),
    LegacyEvidenceSet(EvidenceSetId),
    LegacyDefinition(ValidationId),
    LegacyRun { validation: ValidationId, run: u32 },
}
/// Expansions add related objects to the same page after the claim itself.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeClaimExpand {
    pub content: bool,
    pub scopes: bool,
    pub responses: bool,
    pub evaluations: bool,
    pub history: bool,
}
/// The family of evaluation a context read selects among a declaration's
/// registrations when no exact target is named.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeContextKind {
    Admission,
    Increment,
    WholeWork,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeContextQuery {
    pub validation: ValidationId,
    pub claim: ClaimId,
    /// Restricts the selection to one family; the highest generation of that
    /// family is selected unless `target` or `generation` narrow it.
    pub kind: Option<NativeContextKind>,
    pub target: Option<NativeEvaluationTarget>,
    pub generation: Option<u64>,
    pub results_after: Option<ObjectRevision>,
    pub limit: u32,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeReadQuery {
    Objects(Vec<NativeObjectRef>),
    Claim {
        id: ClaimId,
        expand: NativeClaimExpand,
    },
    Outcome(NativeInvocationRef),
    Receipt(ReceiptId),
    Monitor {
        claim: ClaimId,
        id: MonitorId,
    },
    Responses {
        claim: ClaimId,
        after: Option<u32>,
    },
    Evaluations {
        validation: ValidationId,
        after: Option<NativeEvaluationKey>,
    },
    Results {
        evaluation: NativeEvaluationKey,
        after: Option<ObjectRevision>,
    },
    ValidationContext(NativeContextQuery),
    Events {
        after: Option<(SessionSeq, u32)>,
        limit: u32,
    },
    Standing,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeReadRequest {
    pub consistency: ReadConsistency,
    pub query: NativeReadQuery,
    pub max_items: u32,
}
impl NativeReadRequest {
    pub fn validate(&self, limits: &WireLimits) -> Result<(), AccessError> {
        if self.max_items == 0 || self.max_items > limits.max_items {
            return Err(AccessError::InvalidRequest);
        }
        let within = |count: u32| count != 0 && count <= self.max_items;
        let valid = match &self.query {
            NativeReadQuery::Objects(objects) => u32::try_from(objects.len()).is_ok_and(within),
            NativeReadQuery::Claim { id, .. } => !id.is_zero(),
            NativeReadQuery::Receipt(id) => !id.is_zero(),
            NativeReadQuery::Monitor { claim, id } => !claim.is_zero() && !id.is_zero(),
            NativeReadQuery::Responses { claim, .. } => !claim.is_zero(),
            NativeReadQuery::Evaluations { validation, .. } => !validation.is_zero(),
            NativeReadQuery::Results { evaluation, .. } => {
                !evaluation.claim.is_zero() && !evaluation.validation.is_zero()
            }
            NativeReadQuery::ValidationContext(query) => {
                !query.validation.is_zero() && !query.claim.is_zero() && within(query.limit)
            }
            NativeReadQuery::Events { limit, .. } => within(*limit),
            NativeReadQuery::Outcome(_) | NativeReadQuery::Standing => true,
        };
        if valid {
            Ok(())
        } else {
            Err(AccessError::InvalidRequest)
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeContinuation {
    Objects { index: u32 },
    Responses { cycle: u32 },
    Evaluations(NativeEvaluationKey),
    Results(ObjectRevision),
    Events { sequence: SessionSeq, ordinal: u32 },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeReadPage {
    pub token: ReadToken,
    pub native_sequence: SessionSeq,
    pub logical_time: u64,
    pub objects: Vec<NativeObject>,
    pub next: Option<NativeContinuation>,
    pub visited: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeClaimOrigin {
    Native,
    Legacy,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeCausePhase {
    Programmatic,
    Quality,
    Delivery,
    MissingTarget,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeCauseTarget {
    Admission,
    Increment {
        artifact: ArtifactId,
        content: ContentHash,
    },
    Response(TestamentId),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeBlockingKind {
    Incomplete,
    Failed,
    Errored,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeBlockingCause {
    pub target: NativeCauseTarget,
    pub declaration_index: u32,
    pub generation: Option<u64>,
    pub attempt: Option<u32>,
    pub phase: NativeCausePhase,
    pub slot: Option<u32>,
    pub artifact: Option<ArtifactRef>,
    pub kind: NativeBlockingKind,
    pub mode: ValidationMode,
    pub slot_mode: ValidationMode,
    pub evidence: Option<ArtifactRef>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeFailureKind {
    DependencyFailed,
    Deadlocked,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeGraphCut {
    pub sequence: SessionSeq,
    pub kind: NativeFailureKind,
    pub origin: NativeBinding,
    pub origin_created: SessionSeq,
    pub origin_terminal: SessionSeq,
    pub fingerprint: ContentHash,
    pub deadline: Option<Deadline>,
    pub fired_at: Option<u64>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeTerminalCut {
    Explicit(NativeCut),
    Required {
        sequence: SessionSeq,
        cause: NativeBlockingCause,
    },
    Graph(NativeGraphCut),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeResponseLink {
    pub testament: TestamentId,
    pub content: ContentHash,
    pub receipt: ReceiptFence,
    pub cycle: u32,
    pub prior: Option<TestamentId>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeObligationKind {
    DependsOn,
    Awaits,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeObligation {
    pub kind: NativeObligationKind,
    pub target: ClaimId,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeCorrectionKind {
    Supersedes,
    Amends,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeCorrection {
    pub kind: NativeCorrectionKind,
    pub predecessor: ObjectRef,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeRebinding {
    pub predecessor: ClaimId,
    pub successor: ClaimId,
    pub cut: NativeCut,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeMonitorDisposition {
    Released(NativeCut),
    Cancelled {
        terminal: SessionSeq,
        cut: NativeCut,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeMonitor {
    pub claim: ClaimId,
    pub id: MonitorId,
    pub roots: Vec<WaitPredicate>,
    pub deadline: Deadline,
    pub registered: SessionSeq,
    pub disposition: Option<NativeMonitorDisposition>,
    pub last_rebinding: Option<NativeRebinding>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeScopeLimits {
    pub scopes: u32,
    pub roots: u32,
    pub children: u32,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeOwnedChild {
    pub binding: NativeBinding,
    pub registered: SessionSeq,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeScopes {
    pub limits: NativeScopeLimits,
    pub released: bool,
    pub release_cut: Option<NativeCut>,
    pub monitors: Vec<NativeMonitor>,
    pub children: Vec<NativeOwnedChild>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeCheckPolicy {
    pub declaration_index: u32,
    pub validation: ValidationId,
    pub mode: ValidationMode,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeSlotPolicy {
    pub slot: u32,
    pub missing_declaration_index: u32,
    pub mode: ValidationMode,
    pub checks: Vec<NativeCheckPolicy>,
}
/// The immutable authored claim body, present only on authored ledgers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeClaimContent {
    pub binding: NativeBinding,
    pub schema: u16,
    pub occurrence: OccurrenceId,
    pub description: String,
    pub action: ActionType,
    pub cause: Cause,
    pub relations: Vec<Relation>,
    pub scopes: Vec<Scope>,
    pub requirements: Vec<RequirementRef>,
    pub slots: Vec<NativeSlotPolicy>,
    pub deadline: Option<Deadline>,
    pub content_hash: ContentHash,
    pub intent: ContentHash,
    /// The authored follow-up policy of descriptor schema 2; absent on
    /// schema-1 claims.
    pub policy: Option<PeerPolicy>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeClaim {
    pub binding: NativeBinding,
    pub issuer: ParticipantId,
    pub subject: ParticipantId,
    pub created: SessionSeq,
    pub deadline: Option<Deadline>,
    pub status: ClaimStatus,
    pub origin: NativeClaimOrigin,
    pub released: bool,
    pub receipt: Option<NativeEntitlement>,
    pub local_complete: bool,
    pub local_sealed_at: Option<SessionSeq>,
    pub terminal: Option<NativeTerminalCut>,
    pub latest_response: Option<NativeResponseLink>,
    pub response_count: u32,
    pub max_responses: u32,
    pub obligations: Vec<NativeObligation>,
    pub cause: Cause,
    pub corrections: Vec<NativeCorrection>,
    pub acceptance: Vec<NativeSlotPolicy>,
    pub scopes: Option<NativeScopes>,
    pub content: Option<NativeClaimContent>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeTargetDeclaration {
    WholeWorkSlot { index: u32, name: String },
    Delivery,
    Admission,
    Increment,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeHandlerPolicy {
    pub id: ValidatorId,
    pub version: ContentHash,
    pub agentic: bool,
    pub attempts: u32,
    pub proof_schema: ContentHash,
    pub diagnostic_schema: ContentHash,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativePhasePolicy {
    pub evaluator: ParticipantId,
    pub definition: ContentHash,
    pub required_policy: Option<ContentHash>,
    pub handlers: Vec<NativeHandlerPolicy>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeProgram {
    Delivery,
    Programmatic {
        check: NativePhasePolicy,
        quality: Option<NativePhasePolicy>,
    },
    Agentic {
        check: NativePhasePolicy,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeDefinition {
    pub binding: NativeBinding,
    pub claim: ClaimId,
    pub issuer: ParticipantId,
    pub declaration_index: u32,
    pub kind: ValidationKind,
    pub phase: ValidationPhase,
    pub mode: ValidationMode,
    pub target: NativeTargetDeclaration,
    pub program: NativeProgram,
    pub deadline: Deadline,
    pub attempt_bound: u32,
    /// The authored specification body, present only on authored ledgers.
    pub content: Option<NativeDefinitionContent>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeDefinitionContent {
    pub description: String,
    pub quality_bar: Option<String>,
    pub contributed_by: Vec<ParticipantId>,
    pub policy_revision: u64,
    pub schema: u16,
    pub content_hash: ContentHash,
    pub specification_hash: ContentHash,
    pub intent: ContentHash,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeSuppression {
    MissingTarget,
    ParentFailure(ContentHash),
    ArtifactFailure(ContentHash),
    CohortSealed(ContentHash),
}
/// Exact evaluation target with the bindings the owner pinned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeTarget {
    Artifact {
        response: NativeBinding,
        slot: u32,
        artifact: NativeBinding,
    },
    MissingSlot {
        response: NativeBinding,
        slot: u32,
    },
    Delivery {
        response: NativeBinding,
    },
    Admission {
        claim: NativeBinding,
    },
    Increment {
        claim: NativeBinding,
        artifact: NativeBinding,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeEvaluation {
    pub binding: NativeBinding,
    pub key: NativeEvaluationKey,
    pub target: NativeTarget,
    pub state: NativeValidationState,
    pub phase: NativePhase,
    pub declared_phase: ValidationPhase,
    pub declaration_index: u32,
    pub issuer: ParticipantId,
    pub evaluator: Option<ParticipantId>,
    pub mode: ValidationMode,
    pub receipt: Option<ReceiptFence>,
    pub has_begun: bool,
    pub attempt_index: Option<u32>,
    pub attempt_bound: u32,
    pub fence: Option<NativeAuthorityFence>,
    pub suppression: Option<NativeSuppression>,
    pub last_result: Option<NativeResultRef>,
    pub sealed: Option<ContentHash>,
    pub deadline: Deadline,
    /// The exact attempt a fenced report must name while the evaluation has
    /// begun and is not terminal; absent otherwise.
    pub current_attempt: Option<NativeAttempt>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeResult {
    pub key: NativeResultRef,
    pub binding: NativeBinding,
    pub declaration_index: u32,
    pub attempt: NativeAttempt,
    pub verdict: VerdictValue,
    pub mode: ValidationMode,
    pub receipt: Option<ReceiptFence>,
    pub evidence: Option<ArtifactRef>,
    pub programmatic_evidence: Option<ArtifactRef>,
    pub reporter: Option<ParticipantId>,
    pub resulting_state: NativeValidationState,
    pub sequence: SessionSeq,
    pub ordinal: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativePayload {
    Inline(Vec<u8>),
    Content(ContentRef),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeWorkRole {
    Output {
        slot: u32,
    },
    Diagnostic {
        reason: NativeEvidenceFailure,
    },
    ReceiptRejection {
        artifact: ArtifactRef,
        reason: NativeEvidenceFailure,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeWorkProvenance {
    pub claim: ClaimId,
    pub cycle: u32,
    pub role: NativeWorkRole,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeResultProvenance {
    pub claim: ClaimId,
    pub validation: ValidationId,
    pub target: NativeTarget,
    pub generation: u64,
    pub attempt: NativeAttempt,
    pub value: VerdictValue,
}
/// Local custody facts installed by the exclusive content writer. This is a
/// local verified-custody fact, not a placement guarantee.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeCustody {
    pub payload: ContentRef,
    pub local_revision: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeArtifact {
    pub binding: NativeBinding,
    pub id: ArtifactId,
    pub schema: u16,
    pub kind: String,
    pub schema_hash: ContentHash,
    pub metadata: Vec<u8>,
    pub payload: NativePayload,
    pub producer: ParticipantId,
    pub receipt: Option<ReceiptFence>,
    pub result_provenance: Option<NativeResultProvenance>,
    pub work_provenance: Option<NativeWorkProvenance>,
    pub inputs: Vec<ObjectRef>,
    pub visibility: Vec<String>,
    pub content_hash: ContentHash,
    pub custody: NativeCustody,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeDiagnostic {
    pub reason: NativeEvidenceFailure,
    pub artifact: ArtifactRef,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeArtifactOutcome {
    Pending,
    Passed,
    Blocked(NativeBlockingCause),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeWorkArtifact {
    pub binding: NativeBinding,
    pub reference: ArtifactRef,
    pub claim: ClaimId,
    pub cycle: u32,
    pub slot: u32,
    pub state: NativeWorkArtifactState,
    pub producer: ParticipantId,
    pub receipt: ReceiptFence,
    pub attachment: Option<TestamentId>,
    pub diagnostic: Option<NativeDiagnostic>,
    pub terminal: Option<(SessionSeq, NativeArtifactOutcome)>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeSlotBinding {
    pub slot: u32,
    pub artifact: ArtifactRef,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeFailedWork {
    pub binding: NativeBinding,
    pub slot: u32,
    pub state: NativeWorkArtifactState,
    pub diagnostic: NativeDiagnostic,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeResponseDiagnostic {
    pub producer: ParticipantId,
    pub diagnostic: NativeDiagnostic,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeResponseOutcome {
    Evaluating,
    Validated {
        sequence: SessionSeq,
    },
    Blocked {
        sequence: SessionSeq,
        cause: NativeBlockingCause,
    },
}
/// A respondent-authored testimony cycle (a native response).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeResponse {
    pub binding: NativeBinding,
    pub claim: ClaimId,
    pub receipt: ReceiptFence,
    pub cycle: u32,
    pub prior: Option<TestamentId>,
    pub respondent: ParticipantId,
    pub state: NativeResponseState,
    pub summary: String,
    pub confidence: Confidence,
    pub outcome: OutcomeKind,
    pub manifest: Vec<NativeSlotBinding>,
    pub failed_work: Vec<NativeFailedWork>,
    pub diagnostics: Vec<NativeResponseDiagnostic>,
    pub terminal: Option<NativeResponseOutcome>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeResultTestament {
    pub binding: NativeBinding,
    pub claim: ClaimId,
    pub state: NativeResultTestamentState,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeReceiptRecord {
    pub claim: ClaimId,
    pub fence: ReceiptFence,
    pub holder: ParticipantId,
    pub acquired: SessionSeq,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeCreatedFamily {
    Claim,
    Validation,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeCreatedObject {
    pub ordinal: u32,
    pub family: NativeCreatedFamily,
    pub schema: u16,
    pub content: ContentHash,
    pub requested: ObjectId,
    pub resolved: ObjectId,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeCreationResult {
    pub invocation: NativeInvocationRef,
    pub created: Vec<NativeCreatedObject>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeRegistrationState {
    Missing,
    Pending,
    Registered,
    Sealed,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeRegistration {
    pub state: NativeRegistrationState,
    pub generation: u64,
    pub eligible: bool,
    pub reason: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeManifestEntry {
    pub slot: u32,
    pub artifact: Option<NativeArtifact>,
    pub custody_verified: bool,
}
/// The issuer's acknowledgment of a response, recorded by the owner without
/// any handler attempt: the delivery evaluation's accepted result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeDeliveryOutcome {
    pub key: NativeResultRef,
    pub binding: NativeBinding,
    pub declaration_index: u32,
    pub verdict: VerdictValue,
    pub mode: ValidationMode,
    pub receipt: Option<ReceiptFence>,
    pub reporter: Option<ParticipantId>,
    pub resulting_state: NativeValidationState,
    pub sequence: SessionSeq,
    pub ordinal: u32,
}
/// Everything an evaluator needs from one fixed-prefix lease.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeValidationContext {
    pub claim: NativeClaim,
    pub definition: NativeDefinition,
    pub registration: NativeRegistration,
    pub evaluation: Option<NativeEvaluation>,
    pub target: Option<NativeTarget>,
    pub manifest: Vec<NativeManifestEntry>,
    pub results: Vec<NativeResult>,
    pub delivery: Option<NativeDeliveryOutcome>,
    pub next: Option<ObjectRevision>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativePeerRole {
    Actor,
    Evaluator,
    Runtime,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeStanding {
    pub principal: ParticipantId,
    pub role: NativePeerRole,
    pub profile: NativeProfile,
    pub native_sequence: SessionSeq,
    pub logical_time: u64,
}
/// Frozen V1 bytes retained by import; clients decode them with the durable V1
/// decoders and never reinterpret them as native rows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeLegacyRow {
    pub key: NativeObjectRef,
    pub bytes: Vec<u8>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeObject {
    Claim(Box<NativeClaim>),
    Definition(Box<NativeDefinition>),
    Evaluation(Box<NativeEvaluation>),
    Result(Box<NativeResult>),
    Artifact(Box<NativeArtifact>),
    Work(Box<NativeWorkArtifact>),
    Diagnostic(Box<NativeWorkArtifact>),
    Response(Box<NativeResponse>),
    ResultTestament(NativeResultTestament),
    Receipt(NativeReceiptRecord),
    Monitor(Box<NativeMonitor>),
    Outcome(Box<NativeReceipt>),
    CreationResult(NativeCreationResult),
    Event(Box<NativeEventRecord>),
    Context(Box<NativeValidationContext>),
    Standing(NativeStanding),
    Legacy(NativeLegacyRow),
    /// The address is absent at this prefix; absence is never abort proof.
    Missing(NativeObjectRef),
}

/// One indexed predicate is selected by the node; the rest filter residually
/// within `max_visits`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeListFilter {
    Claims {
        issuer: Option<ParticipantId>,
        subject: Option<ParticipantId>,
        status: Option<ClaimStatus>,
        action: Option<ActionType>,
        scope: Option<Scope>,
        relation: Option<Relation>,
        created_after: Option<SessionSeq>,
    },
    Artifacts {
        producer: Option<ParticipantId>,
        kind: Option<String>,
        schema: Option<ContentHash>,
        input: Option<ObjectId>,
    },
    Definitions {
        claim: Option<ClaimId>,
        evaluator: Option<ParticipantId>,
    },
    Evaluations {
        claim: Option<ClaimId>,
        validation: Option<ValidationId>,
        evaluator: Option<ParticipantId>,
        verdict: Option<VerdictValue>,
    },
    Responses {
        claim: ClaimId,
    },
    Receipts {
        holder: Option<ParticipantId>,
        claim: Option<ClaimId>,
    },
    Monitors {
        claim: ClaimId,
    },
    Events {
        after: Option<(SessionSeq, u32)>,
    },
}
impl NativeListFilter {
    fn valid(&self) -> bool {
        match self {
            Self::Claims {
                issuer,
                subject,
                scope,
                ..
            } => {
                !issuer.is_some_and(ParticipantId::is_zero)
                    && !subject.is_some_and(ParticipantId::is_zero)
                    && !scope
                        .as_ref()
                        .is_some_and(|scope| scope.key.is_empty() || scope.key.len() > 1024)
            }
            Self::Artifacts {
                producer,
                kind,
                schema,
                input,
            } => {
                !producer.is_some_and(ParticipantId::is_zero)
                    && !kind
                        .as_ref()
                        .is_some_and(|kind| kind.is_empty() || kind.len() > 256)
                    && !schema.is_some_and(|hash| hash == ContentHash::default())
                    && !input.is_some_and(ObjectId::is_zero)
            }
            Self::Definitions { claim, evaluator } => {
                !claim.is_some_and(ClaimId::is_zero)
                    && !evaluator.is_some_and(ParticipantId::is_zero)
            }
            Self::Evaluations {
                claim,
                validation,
                evaluator,
                ..
            } => {
                !claim.is_some_and(ClaimId::is_zero)
                    && !validation.is_some_and(ValidationId::is_zero)
                    && !evaluator.is_some_and(ParticipantId::is_zero)
            }
            Self::Responses { claim } | Self::Monitors { claim } => !claim.is_zero(),
            Self::Receipts { holder, claim } => {
                !holder.is_some_and(ParticipantId::is_zero) && !claim.is_some_and(ClaimId::is_zero)
            }
            Self::Events { .. } => true,
        }
    }
}
/// Opaque node-authenticated continuation; a changed filter, principal or
/// route epoch makes it invalid rather than silently repositioning.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeListCursor(pub Vec<u8>);
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeListRequest {
    pub filter: NativeListFilter,
    pub cursor: Option<NativeListCursor>,
    pub max_items: u32,
    pub max_visits: u32,
}
impl NativeListRequest {
    pub fn validate(&self, limits: &WireLimits) -> Result<(), AccessError> {
        if self.max_items == 0
            || self.max_items > limits.max_items
            || self.max_visits < self.max_items
            || self.max_visits > MAX_NATIVE_LIST_VISITS
            || self.cursor.as_ref().is_some_and(|cursor| {
                cursor.0.is_empty() || cursor.0.len() > MAX_NATIVE_LIST_CURSOR_BYTES
            })
            || !self.filter.valid()
        {
            return Err(AccessError::InvalidRequest);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeListPage {
    pub token: ReadToken,
    pub native_sequence: SessionSeq,
    pub objects: Vec<NativeObject>,
    /// May be present when objects is empty: residual filtering consumed the
    /// visit allowance and this cursor resumes after the last visited row.
    pub next: Option<NativeListCursor>,
    pub visited: u32,
}

/// Operations the native profile admits. Legacy typed submissions and legacy
/// reads stay on their own profiles; content transfer and managed request
/// streams are shared.
pub const fn native_profile_operation(operation: &Operation) -> bool {
    matches!(
        operation,
        Operation::Native { .. }
            | Operation::NativeRead(_)
            | Operation::NativeList(_)
            | Operation::Managed { .. }
            | Operation::RequestStreamControl { .. }
            | Operation::RequestStreamRead { .. }
            | Operation::Reconcile(_)
            | Operation::Summary
            | Operation::Stream(_)
            | Operation::Upload(_)
            | Operation::Download { .. }
    )
}
pub const fn is_native_operation(operation: &Operation) -> bool {
    matches!(
        operation,
        Operation::Native { .. } | Operation::NativeRead(_) | Operation::NativeList(_)
    )
}
