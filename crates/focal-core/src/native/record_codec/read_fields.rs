//! Allocation-free native record field decoding. These are recorded DTO values,
//! not authority checks or hydrated model capabilities. The enclosing importer
//! validates immutable bodies, membership and historical publication witnesses.
//! Every read uses the caller's cumulative cursor allowance; none resets it.
use super::{
    bytes::{Cursor, Error},
    fixed,
};
use crate::native::{EvaluationKey, NativeResultKey};
use focal_model::lifecycle::{
    Binding, aggregation, audit, claim, creation, evidence, graph, scope, validation,
};
use focal_model::{
    ArtifactId, ArtifactRef, ClaimId, ClaimStatus, ContentHash, Deadline, LedgerId, ObjectId,
    ObjectRevision, ParticipantId, ReceiptFence, ReceiptId, SessionSeq, TestamentId, TimerId,
    ValidationId, ValidationMode, ValidatorId, VerdictValue,
};

#[cfg(test)]
#[path = "read_fields_tests.rs"]
mod tests;

pub(super) fn optional<'a, T>(
    c: &mut Cursor<'a>,
    read: impl FnOnce(&mut Cursor<'a>) -> Result<T, Error>,
) -> Result<Option<T>, Error> {
    match c.u8()? {
        0 => Ok(None),
        1 => read(c).map(Some),
        _ => Err(Error::InvalidTag("option")),
    }
}
pub(super) fn boolean(c: &mut Cursor<'_>) -> Result<bool, Error> {
    match c.u8()? {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(Error::InvalidTag("boolean")),
    }
}
pub(super) fn ledger(c: &mut Cursor<'_>) -> Result<LedgerId, Error> {
    fixed::read_ledger(c)
}
pub(super) fn hash(c: &mut Cursor<'_>) -> Result<ContentHash, Error> {
    Ok(ContentHash(c.fixed()?))
}
pub(super) fn participant(c: &mut Cursor<'_>) -> Result<ParticipantId, Error> {
    Ok(ParticipantId(c.fixed()?))
}
pub(super) fn sequence(c: &mut Cursor<'_>) -> Result<SessionSeq, Error> {
    Ok(SessionSeq(c.u64()?))
}
pub(super) fn binding(c: &mut Cursor<'_>) -> Result<Binding, Error> {
    Ok(Binding {
        ledger: ledger(c)?,
        object: ObjectId(c.fixed()?),
        content: hash(c)?,
        revision: ObjectRevision(c.u64()?),
    })
}
pub(super) fn deadline(c: &mut Cursor<'_>) -> Result<Deadline, Error> {
    Ok(Deadline {
        timer: TimerId(c.fixed()?),
        generation: c.u64()?,
        at: c.u64()?,
    })
}
pub(super) fn receipt(c: &mut Cursor<'_>) -> Result<ReceiptFence, Error> {
    Ok(ReceiptFence {
        receipt: ReceiptId(c.fixed()?),
        epoch: c.u64()?,
    })
}
pub(super) fn artifact_ref(c: &mut Cursor<'_>) -> Result<ArtifactRef, Error> {
    Ok(ArtifactRef {
        id: ArtifactId(c.fixed()?),
        hash: hash(c)?,
    })
}
pub(super) fn optional_binding(c: &mut Cursor<'_>) -> Result<Option<Binding>, Error> {
    optional(c, binding)
}
pub(super) fn optional_hash(c: &mut Cursor<'_>) -> Result<Option<ContentHash>, Error> {
    optional(c, hash)
}
pub(super) fn optional_artifact(c: &mut Cursor<'_>) -> Result<Option<ArtifactRef>, Error> {
    optional(c, artifact_ref)
}
pub(super) fn optional_participant(c: &mut Cursor<'_>) -> Result<Option<ParticipantId>, Error> {
    optional(c, participant)
}
pub(super) fn optional_sequence(c: &mut Cursor<'_>) -> Result<Option<SessionSeq>, Error> {
    optional(c, sequence)
}
pub(super) fn optional_deadline(c: &mut Cursor<'_>) -> Result<Option<Deadline>, Error> {
    optional(c, deadline)
}
pub(super) fn optional_receipt(c: &mut Cursor<'_>) -> Result<Option<ReceiptFence>, Error> {
    optional(c, receipt)
}
pub(super) fn owner(c: &mut Cursor<'_>) -> Result<Option<creation::Owner>, Error> {
    optional(c, |c| {
        Ok(creation::Owner {
            expected: binding(c)?,
            receipt: optional_receipt(c)?,
        })
    })
}
pub(super) fn scope_limits(c: &mut Cursor<'_>) -> Result<scope::ScopeLimits, Error> {
    Ok(scope::ScopeLimits {
        scopes: c.count(usize::MAX)?,
        roots: c.count(usize::MAX)?,
        children: c.count(usize::MAX)?,
    })
}
pub(super) fn mode(c: &mut Cursor<'_>) -> Result<ValidationMode, Error> {
    match c.u8()? {
        0 => Ok(ValidationMode::Required),
        1 => Ok(ValidationMode::Observe),
        _ => Err(Error::InvalidTag("validation mode")),
    }
}
pub(super) fn verdict(c: &mut Cursor<'_>) -> Result<VerdictValue, Error> {
    match c.u8()? {
        0 => Ok(VerdictValue::Pass),
        1 => Ok(VerdictValue::Fail),
        2 => Ok(VerdictValue::Incomplete),
        3 => Ok(VerdictValue::Error),
        _ => Err(Error::InvalidTag("verdict")),
    }
}
pub(super) fn failure(c: &mut Cursor<'_>) -> Result<evidence::EvidenceFailure, Error> {
    match c.u8()? {
        0 => Ok(evidence::EvidenceFailure::Work),
        1 => Ok(evidence::EvidenceFailure::Production),
        2 => Ok(evidence::EvidenceFailure::Structure),
        3 => Ok(evidence::EvidenceFailure::Metadata),
        _ => Err(Error::InvalidTag("evidence failure")),
    }
}
pub(super) fn phase(c: &mut Cursor<'_>) -> Result<validation::Phase, Error> {
    match c.u8()? {
        0 => Ok(validation::Phase::Programmatic),
        1 => Ok(validation::Phase::Quality),
        2 => Ok(validation::Phase::Delivery),
        3 => Ok(validation::Phase::MissingTarget),
        _ => Err(Error::InvalidTag("evaluation phase")),
    }
}
pub(super) fn attempt(c: &mut Cursor<'_>) -> Result<validation::Attempt, Error> {
    Ok(validation::Attempt {
        phase: phase(c)?,
        index: c.u32()?,
        handler: ValidatorId(c.fixed()?),
        version: hash(c)?,
        evaluator: participant(c)?,
        definition: hash(c)?,
    })
}
pub(super) fn target(c: &mut Cursor<'_>) -> Result<validation::Target, Error> {
    Ok(match c.u8()? {
        0 => validation::Target::Artifact {
            response: binding(c)?,
            slot: c.u32()?,
            artifact: binding(c)?,
        },
        1 => validation::Target::MissingSlot {
            response: binding(c)?,
            slot: c.u32()?,
        },
        2 => validation::Target::Delivery {
            response: binding(c)?,
        },
        3 => validation::Target::Admission { claim: binding(c)? },
        4 => validation::Target::Increment {
            claim: binding(c)?,
            artifact: binding(c)?,
        },
        _ => return Err(Error::InvalidTag("validation target")),
    })
}
pub(super) fn evaluation(c: &mut Cursor<'_>) -> Result<EvaluationKey, Error> {
    fixed::read_evaluation(c)
}
pub(super) fn position(c: &mut Cursor<'_>) -> Result<aggregation::PublicationPosition, Error> {
    Ok(aggregation::PublicationPosition {
        sequence: sequence(c)?,
        ordinal: c.u32()?,
    })
}
pub(super) fn optional_position(
    c: &mut Cursor<'_>,
) -> Result<Option<aggregation::PublicationPosition>, Error> {
    optional(c, position)
}
pub(super) fn claim_cut(c: &mut Cursor<'_>) -> Result<claim::ClaimCut, Error> {
    Ok(claim::ClaimCut {
        position: sequence(c)?,
        cause: hash(c)?,
    })
}
pub(super) fn entitlement(c: &mut Cursor<'_>) -> Result<claim::ReceiptEntitlement, Error> {
    Ok(claim::ReceiptEntitlement {
        holder: participant(c)?,
        fence: receipt(c)?,
    })
}
pub(super) fn rebinding(c: &mut Cursor<'_>) -> Result<scope::Rebinding, Error> {
    Ok(scope::Rebinding {
        predecessor: ClaimId(c.fixed()?),
        successor: ClaimId(c.fixed()?),
        cut: claim_cut(c)?,
    })
}
pub(super) fn cancellation(c: &mut Cursor<'_>) -> Result<scope::MonitorCancellation, Error> {
    Ok(scope::MonitorCancellation {
        terminal: sequence(c)?,
        cut: claim_cut(c)?,
    })
}
pub(super) fn claim_origin(
    c: &mut Cursor<'_>,
) -> Result<focal_model::lifecycle::claim::ClaimOrigin, Error> {
    Ok(match c.u8()? {
        0 => focal_model::lifecycle::claim::ClaimOrigin::Native,
        1 => focal_model::lifecycle::claim::ClaimOrigin::Legacy,
        _ => return Err(Error::InvalidTag("claim origin")),
    })
}
pub(super) fn claim_status(c: &mut Cursor<'_>) -> Result<ClaimStatus, Error> {
    Ok(match c.u16()? {
        1 => ClaimStatus::Generated,
        2 => ClaimStatus::Posted,
        3 => ClaimStatus::Received,
        4 => ClaimStatus::Progressed,
        5 => ClaimStatus::TestamentGenerated,
        6 => ClaimStatus::TestamentAcknowledged,
        7 => ClaimStatus::Validating,
        8 => ClaimStatus::Satisfied,
        9 => ClaimStatus::PostFailed,
        10 => ClaimStatus::ReceiptFailed,
        11 => ClaimStatus::TestamentGenerationFailed,
        12 => ClaimStatus::ValidationIncomplete,
        13 => ClaimStatus::ValidationFailed,
        14 => ClaimStatus::ValidationErrored,
        15 => ClaimStatus::Cancelled,
        16 => ClaimStatus::Expired,
        17 => ClaimStatus::Revoked,
        18 => ClaimStatus::Superseded,
        19 => ClaimStatus::DependencyFailed,
        20 => ClaimStatus::Deadlocked,
        _ => return Err(Error::InvalidTag("claim status")),
    })
}
pub(super) fn work_state(c: &mut Cursor<'_>) -> Result<evidence::WorkArtifactState, Error> {
    Ok(match c.u8()? {
        0 => evidence::WorkArtifactState::Generated,
        1 => evidence::WorkArtifactState::GenerationFailed,
        2 => evidence::WorkArtifactState::Received,
        3 => evidence::WorkArtifactState::ReceiptFailed,
        4 => evidence::WorkArtifactState::Attached,
        5 => evidence::WorkArtifactState::Validating,
        6 => evidence::WorkArtifactState::Validated,
        7 => evidence::WorkArtifactState::ValidationFailed,
        _ => return Err(Error::InvalidTag("work state")),
    })
}
pub(super) fn response_state(c: &mut Cursor<'_>) -> Result<evidence::ResponseState, Error> {
    Ok(match c.u8()? {
        0 => evidence::ResponseState::Generated,
        1 => evidence::ResponseState::Posted,
        2 => evidence::ResponseState::Received,
        3 => evidence::ResponseState::Validating,
        4 => evidence::ResponseState::Validated,
        5 => evidence::ResponseState::ValidationIncomplete,
        6 => evidence::ResponseState::ValidationFailed,
        7 => evidence::ResponseState::ValidationErrored,
        _ => return Err(Error::InvalidTag("response state")),
    })
}
pub(super) fn result_testament_state(
    c: &mut Cursor<'_>,
) -> Result<audit::ResultTestamentState, Error> {
    match c.u8()? {
        0 => Ok(audit::ResultTestamentState::Generated),
        1 => Ok(audit::ResultTestamentState::Posted),
        _ => Err(Error::InvalidTag("result testament state")),
    }
}
pub(super) fn evaluation_state(c: &mut Cursor<'_>) -> Result<validation::State, Error> {
    Ok(match c.u8()? {
        0 => validation::State::Ready,
        1 => validation::State::Validating,
        2 => validation::State::ValidatingQualityBar,
        3 => validation::State::Validated,
        4 => validation::State::ValidationIncomplete,
        5 => validation::State::ValidationFailed,
        6 => validation::State::ValidationFailedNotRequired,
        7 => validation::State::Errored,
        8 => validation::State::ErroredNotRequired,
        9 => validation::State::QualityBarValidationFailed,
        10 => validation::State::QualityBarValidationFailedNotRequired,
        _ => return Err(Error::InvalidTag("evaluation state")),
    })
}
pub(super) fn optional_suppression(
    c: &mut Cursor<'_>,
) -> Result<Option<validation::Suppression>, Error> {
    Ok(match c.u8()? {
        0 => None,
        1 => Some(validation::Suppression::MissingTarget),
        2 => Some(validation::Suppression::ParentFailure(hash(c)?)),
        3 => Some(validation::Suppression::ArtifactFailure(hash(c)?)),
        4 => Some(validation::Suppression::CohortSealed(hash(c)?)),
        _ => return Err(Error::InvalidTag("evaluation suppression")),
    })
}
pub(super) fn fence(c: &mut Cursor<'_>) -> Result<validation::AuthorityFence, Error> {
    let reason = match c.u8()? {
        0 => validation::FenceReason::Cancellation,
        1 => validation::FenceReason::Revocation,
        2 => validation::FenceReason::Supersession,
        3 => validation::FenceReason::Expiry,
        4 => validation::FenceReason::ReceiptAdoption,
        5 => validation::FenceReason::Evaluation,
        6 => validation::FenceReason::Deadline(deadline(c)?),
        _ => return Err(Error::InvalidTag("authority fence")),
    };
    Ok(validation::AuthorityFence {
        reason,
        cause: hash(c)?,
    })
}
pub(super) fn optional_fence(
    c: &mut Cursor<'_>,
) -> Result<Option<validation::AuthorityFence>, Error> {
    optional(c, fence)
}
pub(super) fn blocking_cause(
    c: &mut Cursor<'_>,
) -> Result<aggregation::BlockingCauseSnapshotV1, Error> {
    let target = match c.u8()? {
        0 => aggregation::CauseTarget::Admission,
        1 => aggregation::CauseTarget::Increment {
            artifact: ArtifactId(c.fixed()?),
            content: hash(c)?,
        },
        2 => aggregation::CauseTarget::Response(TestamentId(c.fixed()?)),
        _ => return Err(Error::InvalidTag("blocking target")),
    };
    let declaration_index = c.u32()?;
    let generation = optional(c, |c| c.u64())?;
    let attempt = optional(c, |c| c.u32())?;
    let phase = match c.u8()? {
        0 => aggregation::CausePhase::Programmatic,
        1 => aggregation::CausePhase::Quality,
        2 => aggregation::CausePhase::Delivery,
        3 => aggregation::CausePhase::MissingTarget,
        _ => return Err(Error::InvalidTag("blocking phase")),
    };
    let slot = optional(c, |c| c.u32())?;
    let artifact = optional_artifact(c)?;
    let kind = match c.u8()? {
        0 => aggregation::BlockingKind::Incomplete,
        1 => aggregation::BlockingKind::Failed,
        2 => aggregation::BlockingKind::Errored,
        _ => return Err(Error::InvalidTag("blocking kind")),
    };
    Ok(aggregation::BlockingCauseSnapshotV1 {
        key: aggregation::CauseKey {
            target,
            declaration_index,
            generation,
            attempt,
            phase,
        },
        slot,
        artifact,
        kind,
        mode: mode(c)?,
        slot_mode: mode(c)?,
        evidence: optional_artifact(c)?,
    })
}
pub(super) fn terminal(c: &mut Cursor<'_>) -> Result<aggregation::TerminalCutSnapshotV1, Error> {
    Ok(aggregation::TerminalCutSnapshotV1 {
        sequence: sequence(c)?,
        cause: blocking_cause(c)?,
    })
}
pub(super) fn graph_terminal(c: &mut Cursor<'_>) -> Result<graph::TerminalCutSnapshotV1, Error> {
    let sequence = sequence(c)?;
    let kind = match c.u8()? {
        0 => graph::FailureKind::DependencyFailed,
        1 => graph::FailureKind::Deadlocked,
        _ => return Err(Error::InvalidTag("graph failure")),
    };
    Ok(graph::TerminalCutSnapshotV1 {
        sequence,
        kind,
        origin: graph::OriginSnapshotV1 {
            binding: binding(c)?,
            created: self::sequence(c)?,
            terminal: self::sequence(c)?,
        },
        fingerprint: hash(c)?,
        deadline: optional_deadline(c)?,
        fired_at: optional(c, |c| c.u64())?,
    })
}
pub(super) fn claim_terminal(c: &mut Cursor<'_>) -> Result<claim::ClaimTerminalSnapshotV1, Error> {
    Ok(match c.u8()? {
        0 => claim::ClaimTerminalSnapshotV1::Explicit(claim_cut(c)?),
        1 => claim::ClaimTerminalSnapshotV1::Required(terminal(c)?),
        2 => claim::ClaimTerminalSnapshotV1::Graph(graph_terminal(c)?),
        _ => return Err(Error::InvalidTag("claim terminal cut")),
    })
}
pub(super) fn work_terminal(c: &mut Cursor<'_>) -> Result<evidence::WorkTerminalSnapshotV1, Error> {
    Ok(match c.u8()? {
        0 => evidence::WorkTerminalSnapshotV1::Passed {
            sequence: sequence(c)?,
        },
        1 => evidence::WorkTerminalSnapshotV1::Blocked {
            sequence: sequence(c)?,
            cause: blocking_cause(c)?,
        },
        _ => return Err(Error::InvalidTag("work terminal cut")),
    })
}
pub(super) fn response_terminal(
    c: &mut Cursor<'_>,
) -> Result<evidence::ResponseTerminalSnapshotV1, Error> {
    Ok(match c.u8()? {
        0 => evidence::ResponseTerminalSnapshotV1::Validated {
            sequence: sequence(c)?,
        },
        1 => evidence::ResponseTerminalSnapshotV1::Blocked(terminal(c)?),
        _ => return Err(Error::InvalidTag("response terminal cut")),
    })
}
pub(super) fn accepted_result(
    c: &mut Cursor<'_>,
) -> Result<validation::AcceptedResultSnapshotV1, Error> {
    Ok(validation::AcceptedResultSnapshotV1 {
        binding: binding(c)?,
        ledger: ledger(c)?,
        claim: ClaimId(c.fixed()?),
        target: target(c)?,
        validation: ValidationId(c.fixed()?),
        declaration_index: c.u32()?,
        mode: mode(c)?,
        verdict: verdict(c)?,
        phase: phase(c)?,
        attempt: optional(c, |c| c.u32())?,
        generation: c.u64()?,
        receipt: optional_receipt(c)?,
        evidence: optional_artifact(c)?,
        programmatic_evidence: optional_artifact(c)?,
        reporter: optional_participant(c)?,
        resulting_state: evaluation_state(c)?,
    })
}
pub(super) fn evaluation_snapshot(
    c: &mut Cursor<'_>,
) -> Result<validation::EvaluationSnapshotV1, Error> {
    Ok(validation::EvaluationSnapshotV1 {
        binding: binding(c)?,
        target: target(c)?,
        generation: c.u64()?,
        receipt: optional_receipt(c)?,
        state: evaluation_state(c)?,
        phase: phase(c)?,
        handler: c.u64()?,
        handler_attempt: c.u32()?,
        attempt: c.u32()?,
        begun: boolean(c)?,
        suppression: optional_suppression(c)?,
        sealed: optional_hash(c)?,
        fence: optional_fence(c)?,
        programmatic_evidence: optional_artifact(c)?,
        last_result: optional(c, accepted_result)?,
    })
}
pub(super) fn result_key(c: &mut Cursor<'_>) -> Result<NativeResultKey, Error> {
    Ok(NativeResultKey {
        evaluation: evaluation(c)?,
        revision: ObjectRevision(c.u64()?),
    })
}
pub(super) fn claim_response(c: &mut Cursor<'_>) -> Result<claim::ClaimResponseSnapshotV1, Error> {
    Ok(claim::ClaimResponseSnapshotV1 {
        link: claim::ResponseLink {
            testament: TestamentId(c.fixed()?),
            content: hash(c)?,
            receipt: receipt(c)?,
            cycle: c.u32()?,
            prior: optional(c, |c| Ok(TestamentId(c.fixed()?)))?,
        },
        posted: boolean(c)?,
        received: boolean(c)?,
    })
}
pub(super) fn registration_member(
    c: &mut Cursor<'_>,
) -> Result<aggregation::RegistrationMemberSnapshotV1, Error> {
    Ok(aggregation::RegistrationMemberSnapshotV1 {
        binding: binding(c)?,
        target: target(c)?,
        generation: c.u64()?,
        receipt: optional_receipt(c)?,
        declaration_index: c.u32()?,
        mode: mode(c)?,
    })
}
pub(super) fn audit_member(c: &mut Cursor<'_>) -> Result<audit::AuditMemberSnapshotV1, Error> {
    Ok(audit::AuditMemberSnapshotV1 {
        key: audit::EvaluationKey {
            validation: ValidationId(c.fixed()?),
            target: target(c)?,
            generation: c.u64()?,
        },
        declaration_index: c.u32()?,
        binding: binding(c)?,
        receipt: optional_receipt(c)?,
        begun: boolean(c)?,
        state: evaluation_state(c)?,
        suppression: optional_suppression(c)?,
        fence: optional_fence(c)?,
        last_result: optional(c, accepted_result)?,
        sealed: optional_hash(c)?,
    })
}
