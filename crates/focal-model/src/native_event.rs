//! The native event vocabulary: the committed facts a native record publishes
//! (22 §3), addressed by native sequence and ordinal, in the exact shape the
//! native wire profile and the version-2 delta stream carry. These records
//! mirror committed rows; they never decode a frame, admit a mutation or
//! assert satisfaction. They live in the model so the ledger can derive
//! deltas from committed events without depending on the wire crate; the wire
//! crate re-exports every type unchanged.
use crate::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum NativeEvaluationTarget {
    Admission,
    Increment {
        artifact: ArtifactId,
    },
    Work {
        response: TestamentId,
        slot: u32,
        artifact: ArtifactId,
    },
    MissingSlot {
        response: TestamentId,
        slot: u32,
    },
    Delivery {
        response: TestamentId,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeEvaluationKey {
    pub claim: ClaimId,
    pub validation: ValidationId,
    pub target: NativeEvaluationTarget,
    pub generation: u64,
}
/// Address of one immutable accepted attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeResultRef {
    pub evaluation: NativeEvaluationKey,
    pub revision: ObjectRevision,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeInvocationRef {
    Request(RequestKey),
    EvaluationDeadline {
        evaluation: NativeEvaluationKey,
        timer: TimerId,
        generation: u64,
    },
    ClaimDeadline {
        claim: ClaimId,
        timer: TimerId,
        generation: u64,
    },
    MonitorDeadline {
        claim: ClaimId,
        monitor: MonitorId,
        timer: TimerId,
        generation: u64,
    },
    Import,
    /// A committed retirement that moved the family rooted at `root` to the
    /// archive (26 §4).
    Retirement {
        root: ClaimId,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeBinding {
    pub object: ObjectId,
    pub content: ContentHash,
    pub revision: ObjectRevision,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeEntitlement {
    pub holder: ParticipantId,
    pub fence: ReceiptFence,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeCut {
    pub position: SessionSeq,
    pub cause: ContentHash,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeValidationState {
    Ready,
    Validating,
    ValidatingQualityBar,
    Validated,
    ValidationIncomplete,
    ValidationFailed,
    ValidationFailedNotRequired,
    Errored,
    ErroredNotRequired,
    QualityBarValidationFailed,
    QualityBarValidationFailedNotRequired,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativePhase {
    Programmatic,
    Quality,
    Delivery,
    MissingTarget,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeFenceReason {
    Cancellation,
    Revocation,
    Supersession,
    Expiry,
    ReceiptAdoption,
    Evaluation,
    Deadline(Deadline),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeAuthorityFence {
    pub reason: NativeFenceReason,
    pub cause: ContentHash,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeAttempt {
    pub phase: NativePhase,
    pub index: u32,
    pub handler: ValidatorId,
    pub version: ContentHash,
    pub evaluator: ParticipantId,
    pub definition: ContentHash,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeEvidenceFailure {
    Work,
    Production,
    Structure,
    Metadata,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeWorkArtifactState {
    Generated,
    GenerationFailed,
    Received,
    ReceiptFailed,
    Attached,
    Validating,
    Validated,
    ValidationFailed,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeResponseState {
    Generated,
    Posted,
    Received,
    Validating,
    Validated,
    ValidationIncomplete,
    ValidationFailed,
    ValidationErrored,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeResultTestamentState {
    Generated,
    Posted,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeMonitorEventRecord {
    Registered {
        id: MonitorId,
        cut: NativeCut,
    },
    Rebound {
        id: MonitorId,
        predecessor: ClaimId,
        successor: ClaimId,
        cut: NativeCut,
    },
    Released {
        id: MonitorId,
        cut: NativeCut,
    },
    Cancelled {
        id: MonitorId,
        terminal: SessionSeq,
        cut: NativeCut,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeEventKindRecord {
    Monitor(NativeMonitorEventRecord),
    OwnerReleased,
    Validating,
    LocallyComplete,
    ValidationIncomplete,
    ValidationFailed,
    ValidationErrored,
    DependencyFailed,
    Created,
    ChildRegistered,
    Superseded,
    Cancelled,
    Posted,
    PostFailed,
    Received,
    ReceiptAdopted,
    Satisfied,
    TestamentGenerated,
    TestamentAcknowledged,
    ResponseObserved,
    Expired,
    Deadlocked,
    Imported(SessionSeq),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeClaimEventRecord {
    pub kind: NativeEventKindRecord,
    pub graph_before_ordinal: Option<u32>,
    pub owned_child: Option<NativeBinding>,
    pub before: Option<NativeBinding>,
    pub after: NativeBinding,
    pub status: ClaimStatus,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeEvaluationEventKind {
    MissingTarget,
    Materialized,
    Begun,
    Reported,
    AuthorityFenced,
    Sealed,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeFactRecord {
    ResultTestament {
        claim: ClaimId,
        before: Option<NativeBinding>,
        after: NativeBinding,
        state: NativeResultTestamentState,
    },
    Missing {
        key: NativeResultRef,
    },
    Registrations {
        claim: NativeBinding,
    },
    Delivery {
        key: NativeResultRef,
    },
    Work {
        claim: ClaimId,
        before: Option<NativeBinding>,
        after: NativeBinding,
        state: NativeWorkArtifactState,
    },
    Diagnostic {
        claim: ClaimId,
        binding: NativeBinding,
        reason: NativeEvidenceFailure,
    },
    Response {
        claim: ClaimId,
        before: Option<NativeBinding>,
        after: NativeBinding,
        state: NativeResponseState,
    },
    Receipt {
        claim: NativeBinding,
        fence: ReceiptFence,
        holder: ParticipantId,
    },
    ReceiptAdopted {
        claim: NativeBinding,
        previous: NativeEntitlement,
        replacement: NativeEntitlement,
        cause: ContentHash,
    },
    Artifact {
        binding: NativeBinding,
    },
    Accepted {
        key: NativeResultRef,
    },
    Claim(NativeClaimEventRecord),
    Definition {
        binding: NativeBinding,
        claim: ClaimId,
        index: u32,
        intent: ContentHash,
    },
    Evaluation {
        kind: NativeEvaluationEventKind,
        key: NativeEvaluationKey,
        before: Option<NativeBinding>,
        after: NativeBinding,
        state: NativeValidationState,
        phase: NativePhase,
        attempt: Option<NativeAttempt>,
        fence: Option<NativeAuthorityFence>,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeEventRecord {
    pub invocation: NativeInvocationRef,
    pub sequence: SessionSeq,
    pub ordinal: u32,
    pub fact: NativeFactRecord,
}
