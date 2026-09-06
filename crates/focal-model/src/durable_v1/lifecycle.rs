//! Frozen V1 checkpoint lifecycle, evaluation, monitor, and retained receipt rows.
//!
//! These codecs preserve representable historical facts without applying current
//! admission rules. Field and variant order belongs to the durable format.
use super::{Ref, V1, Value};
use crate::{
    ArtifactLifecycle, ArtifactRef, ClaimId, ClaimLifecycle, ClaimStatus, CommandResult,
    ContentHash, Deadline, EvidenceSet, EvidenceSetId, HandlerRef, LedgerId, Monitor, MonitorId,
    MutationReceipt, ObjectRevision, ParticipantId, Receipt, ReceiptFence, RequestEpoch, RequestId,
    RequestKey, SessionSeq, StatusFact, TestamentId, TestamentLifecycle, ValidationId,
    ValidationLifecycle, ValidationPhase, ValidationRun, ValidationRunId, VerdictRecord,
    VerdictValue, WaitPredicate,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

v1_struct!(Receipt {
    fence: ReceiptFence,
    holder: ParticipantId,
    acquired: SessionSeq,
});
v1_struct!(StatusFact {
    status: ClaimStatus,
    sequence: SessionSeq,
});
v1_struct!(ClaimLifecycle {
    status: ClaimStatus,
    revision: ObjectRevision,
    created: SessionSeq,
    history: Vec<StatusFact>,
    receipt: Option<Receipt>,
    evidence_set: Option<EvidenceSetId>,
    testament: Option<TestamentId>,
    local_complete: bool,
    released: bool,
    terminal_witness: Option<ClaimId>,
});
v1_struct!(TestamentLifecycle {
    created: SessionSeq,
    acknowledged: Option<SessionSeq>,
});
v1_struct!(ArtifactLifecycle {
    created: SessionSeq,
    custody_revision: u64,
});
v1_struct!(ValidationLifecycle {
    created: SessionSeq,
    latest_epoch: u64,
});
v1_struct!(ValidationRunId {
    validation: ValidationId,
    target_hash: ContentHash,
    phase: ValidationPhase,
    epoch: u64,
});
v1_struct!(ValidationRun {
    id: ValidationRunId,
    claim: ClaimId,
    evaluator: ParticipantId,
    manifest: ContentHash,
    handler_index: u32,
    quality_phase: bool,
    attempts: Vec<VerdictRecord>,
    final_verdict: Option<VerdictValue>,
});
v1_struct!(VerdictRecord {
    run: ValidationRunId,
    evaluator: ParticipantId,
    handler: HandlerRef,
    attempt: u32,
    manifest: ContentHash,
    value: VerdictValue,
    evidence: Vec<ArtifactRef>,
});
v1_struct!(EvidenceSet {
    id: EvidenceSetId,
    claim: ClaimId,
    receipt: ReceiptFence,
    artifacts: Vec<ArtifactRef>,
    closed: bool,
});
v1_struct!(Monitor {
    id: MonitorId,
    owner: ClaimId,
    roots: BTreeSet<WaitPredicate>,
    deadline: Deadline,
    registered: SessionSeq,
    released: Option<SessionSeq>,
});
v1_struct!(RequestKey {
    principal: ParticipantId,
    epoch: RequestEpoch,
    id: RequestId,
});
v1_struct!(MutationReceipt {
    ledger: LedgerId,
    key: RequestKey,
    sequence: SessionSeq,
    command_hash: ContentHash,
    outcome: CommandResult,
});

/// Fixed Serde enum ordinals 0, 1, 2; these are not vocabulary codes.
#[derive(Serialize, Deserialize)]
enum WaitPredicateV1<T> {
    Satisfied(T), // 0
    Terminal(T),  // 1
    Released(T),  // 2
}

impl V1 for WaitPredicate {
    fn serialize_v1<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Satisfied(claim) => WaitPredicateV1::Satisfied(Ref(claim)),
            Self::Terminal(claim) => WaitPredicateV1::Terminal(Ref(claim)),
            Self::Released(claim) => WaitPredicateV1::Released(Ref(claim)),
        }
        .serialize(serializer)
    }

    fn deserialize_v1<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(
            match WaitPredicateV1::<Value<ClaimId>>::deserialize(deserializer)? {
                WaitPredicateV1::Satisfied(claim) => Self::Satisfied(claim.0),
                WaitPredicateV1::Terminal(claim) => Self::Terminal(claim.0),
                WaitPredicateV1::Released(claim) => Self::Released(claim.0),
            },
        )
    }
}

/// Never reorder or add variants to this historical representation. New result
/// semantics require another codec, even if the current CommandResult evolves.
#[derive(Serialize)]
enum CommandResultRef<'a> {
    EpochAdmitted(Ref<'a, RequestEpoch>),      // 0
    EpochFloorAdvanced(Ref<'a, RequestEpoch>), // 1
    Generated(Ref<'a, Vec<ClaimId>>),          // 2
    Existing(Ref<'a, Vec<ClaimId>>),           // 3
    Claim {
        // 4
        claim: Ref<'a, ClaimId>,
        status: Ref<'a, ClaimStatus>,
    },
    Receipt {
        // 5
        claim: Ref<'a, ClaimId>,
        fence: Ref<'a, ReceiptFence>,
    },
    EvidenceSet(Ref<'a, EvidenceSetId>),  // 6
    Artifact(Ref<'a, ArtifactRef>),       // 7
    Testament(Ref<'a, TestamentId>),      // 8
    Validation(Ref<'a, ValidationRunId>), // 9
    Monitor(Ref<'a, MonitorId>),          // 10
    Noop,                                 // 11
}

#[derive(Deserialize)]
enum CommandResultValue {
    EpochAdmitted(Value<RequestEpoch>),      // 0
    EpochFloorAdvanced(Value<RequestEpoch>), // 1
    Generated(Value<Vec<ClaimId>>),          // 2
    Existing(Value<Vec<ClaimId>>),           // 3
    Claim {
        // 4
        claim: Value<ClaimId>,
        status: Value<ClaimStatus>,
    },
    Receipt {
        // 5
        claim: Value<ClaimId>,
        fence: Value<ReceiptFence>,
    },
    EvidenceSet(Value<EvidenceSetId>),  // 6
    Artifact(Value<ArtifactRef>),       // 7
    Testament(Value<TestamentId>),      // 8
    Validation(Value<ValidationRunId>), // 9
    Monitor(Value<MonitorId>),          // 10
    Noop,                               // 11
}

impl V1 for CommandResult {
    fn serialize_v1<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::EpochAdmitted(epoch) => CommandResultRef::EpochAdmitted(Ref(epoch)),
            Self::EpochFloorAdvanced(epoch) => CommandResultRef::EpochFloorAdvanced(Ref(epoch)),
            Self::Generated(claims) => CommandResultRef::Generated(Ref(claims)),
            Self::Existing(claims) => CommandResultRef::Existing(Ref(claims)),
            Self::Claim { claim, status } => CommandResultRef::Claim {
                claim: Ref(claim),
                status: Ref(status),
            },
            Self::Receipt { claim, fence } => CommandResultRef::Receipt {
                claim: Ref(claim),
                fence: Ref(fence),
            },
            Self::EvidenceSet(id) => CommandResultRef::EvidenceSet(Ref(id)),
            Self::Artifact(reference) => CommandResultRef::Artifact(Ref(reference)),
            Self::Testament(id) => CommandResultRef::Testament(Ref(id)),
            Self::Validation(id) => CommandResultRef::Validation(Ref(id)),
            Self::Monitor(id) => CommandResultRef::Monitor(Ref(id)),
            Self::Noop => CommandResultRef::Noop,
        }
        .serialize(serializer)
    }

    fn deserialize_v1<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(match CommandResultValue::deserialize(deserializer)? {
            CommandResultValue::EpochAdmitted(epoch) => Self::EpochAdmitted(epoch.0),
            CommandResultValue::EpochFloorAdvanced(epoch) => Self::EpochFloorAdvanced(epoch.0),
            CommandResultValue::Generated(claims) => Self::Generated(claims.0),
            CommandResultValue::Existing(claims) => Self::Existing(claims.0),
            CommandResultValue::Claim { claim, status } => Self::Claim {
                claim: claim.0,
                status: status.0,
            },
            CommandResultValue::Receipt { claim, fence } => Self::Receipt {
                claim: claim.0,
                fence: fence.0,
            },
            CommandResultValue::EvidenceSet(id) => Self::EvidenceSet(id.0),
            CommandResultValue::Artifact(reference) => Self::Artifact(reference.0),
            CommandResultValue::Testament(id) => Self::Testament(id.0),
            CommandResultValue::Validation(id) => Self::Validation(id.0),
            CommandResultValue::Monitor(id) => Self::Monitor(id.0),
            CommandResultValue::Noop => Self::Noop,
        })
    }
}

#[cfg(test)]
#[path = "lifecycle_tests.rs"]
mod tests;
