//! Explicit scalar fields shared by dormant native row and event encoders.
//! These tags belong to the native record format; Rust enum layout and serde
//! implementations are never consulted. Semantic stamps derive from retained
//! immutable bodies during hydration, rather than being trusted wire fields.
use super::{bytes, types};
use crate::native::NativeResultKey;
use bytes::{Error, Sink, write_raw as raw, write_u8, write_u16, write_u32, write_u64};
use focal_model::lifecycle::{
    Binding, aggregation, audit, claim, evidence, graph, scope, validation,
};
use focal_model::{ArtifactRef, ClaimStatus, ContentHash, Deadline, ParticipantId, SessionSeq};

pub(super) fn optional<S: Sink, T>(
    sink: &mut S,
    value: Option<T>,
    encode: impl FnOnce(&mut S, T) -> Result<(), Error>,
) -> Result<(), Error> {
    match value {
        None => write_u8(sink, 0),
        Some(value) => {
            write_u8(sink, 1)?;
            encode(sink, value)
        }
    }
}

pub(super) fn optional_binding(sink: &mut impl Sink, value: Option<Binding>) -> Result<(), Error> {
    optional(sink, value, types::binding)
}
pub(super) fn optional_hash(sink: &mut impl Sink, value: Option<ContentHash>) -> Result<(), Error> {
    optional(sink, value, |sink, hash| raw(sink, &hash.0))
}
pub(super) fn optional_artifact(
    sink: &mut impl Sink,
    value: Option<ArtifactRef>,
) -> Result<(), Error> {
    optional(sink, value, types::artifact_ref)
}
pub(super) fn optional_participant(
    sink: &mut impl Sink,
    value: Option<ParticipantId>,
) -> Result<(), Error> {
    optional(sink, value, |sink, id| raw(sink, &id.0))
}
pub(super) fn optional_sequence(
    sink: &mut impl Sink,
    value: Option<SessionSeq>,
) -> Result<(), Error> {
    optional(sink, value, |sink, value| write_u64(sink, value.0))
}
pub(super) fn optional_deadline(
    sink: &mut impl Sink,
    value: Option<Deadline>,
) -> Result<(), Error> {
    optional(sink, value, types::deadline)
}
pub(super) fn position(
    sink: &mut impl Sink,
    value: aggregation::PublicationPosition,
) -> Result<(), Error> {
    write_u64(sink, value.sequence.0)?;
    write_u32(sink, value.ordinal)
}
pub(super) fn optional_position(
    sink: &mut impl Sink,
    value: Option<aggregation::PublicationPosition>,
) -> Result<(), Error> {
    optional(sink, value, position)
}
pub(super) fn claim_cut(sink: &mut impl Sink, value: claim::ClaimCut) -> Result<(), Error> {
    write_u64(sink, value.position.0)?;
    raw(sink, &value.cause.0)
}
pub(super) fn entitlement(
    sink: &mut impl Sink,
    value: claim::ReceiptEntitlement,
) -> Result<(), Error> {
    raw(sink, &value.holder.0)?;
    types::receipt(sink, value.fence)
}
pub(super) fn rebinding(sink: &mut impl Sink, value: scope::Rebinding) -> Result<(), Error> {
    raw(sink, &value.predecessor.0)?;
    raw(sink, &value.successor.0)?;
    claim_cut(sink, value.cut)
}
pub(super) fn cancellation(
    sink: &mut impl Sink,
    value: scope::MonitorCancellation,
) -> Result<(), Error> {
    write_u64(sink, value.terminal.0)?;
    claim_cut(sink, value.cut)
}

pub(super) fn claim_origin(sink: &mut impl Sink, value: claim::ClaimOrigin) -> Result<(), Error> {
    write_u8(
        sink,
        match value {
            claim::ClaimOrigin::Native => 0,
            claim::ClaimOrigin::Legacy => 1,
        },
    )
}
pub(super) fn claim_status(sink: &mut impl Sink, value: ClaimStatus) -> Result<(), Error> {
    write_u16(
        sink,
        match value {
            ClaimStatus::Generated => 1,
            ClaimStatus::Posted => 2,
            ClaimStatus::Received => 3,
            ClaimStatus::Progressed => 4,
            ClaimStatus::TestamentGenerated => 5,
            ClaimStatus::TestamentAcknowledged => 6,
            ClaimStatus::Validating => 7,
            ClaimStatus::Satisfied => 8,
            ClaimStatus::PostFailed => 9,
            ClaimStatus::ReceiptFailed => 10,
            ClaimStatus::TestamentGenerationFailed => 11,
            ClaimStatus::ValidationIncomplete => 12,
            ClaimStatus::ValidationFailed => 13,
            ClaimStatus::ValidationErrored => 14,
            ClaimStatus::Cancelled => 15,
            ClaimStatus::Expired => 16,
            ClaimStatus::Revoked => 17,
            ClaimStatus::Superseded => 18,
            ClaimStatus::DependencyFailed => 19,
            ClaimStatus::Deadlocked => 20,
        },
    )
}
pub(super) fn work_state(
    sink: &mut impl Sink,
    value: evidence::WorkArtifactState,
) -> Result<(), Error> {
    write_u8(
        sink,
        match value {
            evidence::WorkArtifactState::Generated => 0,
            evidence::WorkArtifactState::GenerationFailed => 1,
            evidence::WorkArtifactState::Received => 2,
            evidence::WorkArtifactState::ReceiptFailed => 3,
            evidence::WorkArtifactState::Attached => 4,
            evidence::WorkArtifactState::Validating => 5,
            evidence::WorkArtifactState::Validated => 6,
            evidence::WorkArtifactState::ValidationFailed => 7,
        },
    )
}
pub(super) fn response_state(
    sink: &mut impl Sink,
    value: evidence::ResponseState,
) -> Result<(), Error> {
    write_u8(
        sink,
        match value {
            evidence::ResponseState::Generated => 0,
            evidence::ResponseState::Posted => 1,
            evidence::ResponseState::Received => 2,
            evidence::ResponseState::Validating => 3,
            evidence::ResponseState::Validated => 4,
            evidence::ResponseState::ValidationIncomplete => 5,
            evidence::ResponseState::ValidationFailed => 6,
            evidence::ResponseState::ValidationErrored => 7,
        },
    )
}
pub(super) fn result_testament_state(
    sink: &mut impl Sink,
    value: audit::ResultTestamentState,
) -> Result<(), Error> {
    write_u8(
        sink,
        match value {
            audit::ResultTestamentState::Generated => 0,
            audit::ResultTestamentState::Posted => 1,
        },
    )
}
pub(super) fn phase(sink: &mut impl Sink, value: validation::Phase) -> Result<(), Error> {
    write_u8(
        sink,
        match value {
            validation::Phase::Programmatic => 0,
            validation::Phase::Quality => 1,
            validation::Phase::Delivery => 2,
            validation::Phase::MissingTarget => 3,
        },
    )
}
pub(super) fn evaluation_state(
    sink: &mut impl Sink,
    value: validation::State,
) -> Result<(), Error> {
    write_u8(
        sink,
        match value {
            validation::State::Ready => 0,
            validation::State::Validating => 1,
            validation::State::ValidatingQualityBar => 2,
            validation::State::Validated => 3,
            validation::State::ValidationIncomplete => 4,
            validation::State::ValidationFailed => 5,
            validation::State::ValidationFailedNotRequired => 6,
            validation::State::Errored => 7,
            validation::State::ErroredNotRequired => 8,
            validation::State::QualityBarValidationFailed => 9,
            validation::State::QualityBarValidationFailedNotRequired => 10,
        },
    )
}
pub(super) fn optional_suppression(
    sink: &mut impl Sink,
    value: Option<validation::Suppression>,
) -> Result<(), Error> {
    match value {
        None => write_u8(sink, 0),
        Some(validation::Suppression::MissingTarget) => write_u8(sink, 1),
        Some(validation::Suppression::ParentFailure(cause)) => {
            write_u8(sink, 2)?;
            raw(sink, &cause.0)
        }
        Some(validation::Suppression::ArtifactFailure(cause)) => {
            write_u8(sink, 3)?;
            raw(sink, &cause.0)
        }
        Some(validation::Suppression::CohortSealed(cause)) => {
            write_u8(sink, 4)?;
            raw(sink, &cause.0)
        }
    }
}
pub(super) fn optional_fence(
    sink: &mut impl Sink,
    value: Option<validation::AuthorityFence>,
) -> Result<(), Error> {
    optional(sink, value, |sink, value| {
        match value.reason {
            validation::FenceReason::Cancellation => write_u8(sink, 0)?,
            validation::FenceReason::Revocation => write_u8(sink, 1)?,
            validation::FenceReason::Supersession => write_u8(sink, 2)?,
            validation::FenceReason::Expiry => write_u8(sink, 3)?,
            validation::FenceReason::ReceiptAdoption => write_u8(sink, 4)?,
            validation::FenceReason::Evaluation => write_u8(sink, 5)?,
            validation::FenceReason::Deadline(deadline) => {
                write_u8(sink, 6)?;
                types::deadline(sink, deadline)?;
            }
        }
        raw(sink, &value.cause.0)
    })
}

pub(super) fn blocking_cause(
    sink: &mut impl Sink,
    value: aggregation::BlockingCauseSnapshotV1,
) -> Result<(), Error> {
    match value.key.target {
        aggregation::CauseTarget::Admission => write_u8(sink, 0)?,
        aggregation::CauseTarget::Increment { artifact, content } => {
            write_u8(sink, 1)?;
            raw(sink, &artifact.0)?;
            raw(sink, &content.0)?;
        }
        aggregation::CauseTarget::Response(response) => {
            write_u8(sink, 2)?;
            raw(sink, &response.0)?;
        }
    }
    write_u32(sink, value.key.declaration_index)?;
    optional(sink, value.key.generation, write_u64)?;
    optional(sink, value.key.attempt, write_u32)?;
    write_u8(
        sink,
        match value.key.phase {
            aggregation::CausePhase::Programmatic => 0,
            aggregation::CausePhase::Quality => 1,
            aggregation::CausePhase::Delivery => 2,
            aggregation::CausePhase::MissingTarget => 3,
        },
    )?;
    optional(sink, value.slot, write_u32)?;
    optional_artifact(sink, value.artifact)?;
    write_u8(
        sink,
        match value.kind {
            aggregation::BlockingKind::Incomplete => 0,
            aggregation::BlockingKind::Failed => 1,
            aggregation::BlockingKind::Errored => 2,
        },
    )?;
    types::mode(sink, value.mode)?;
    types::mode(sink, value.slot_mode)?;
    optional_artifact(sink, value.evidence)
}
pub(super) fn terminal(
    sink: &mut impl Sink,
    value: aggregation::TerminalCutSnapshotV1,
) -> Result<(), Error> {
    write_u64(sink, value.sequence.0)?;
    blocking_cause(sink, value.cause)
}
pub(super) fn graph_terminal(
    sink: &mut impl Sink,
    value: graph::TerminalCutSnapshotV1,
) -> Result<(), Error> {
    write_u64(sink, value.sequence.0)?;
    write_u8(
        sink,
        match value.kind {
            graph::FailureKind::DependencyFailed => 0,
            graph::FailureKind::Deadlocked => 1,
        },
    )?;
    types::binding(sink, value.origin.binding)?;
    write_u64(sink, value.origin.created.0)?;
    write_u64(sink, value.origin.terminal.0)?;
    raw(sink, &value.fingerprint.0)?;
    optional_deadline(sink, value.deadline)?;
    optional(sink, value.fired_at, write_u64)
}
pub(super) fn accepted_result(
    sink: &mut impl Sink,
    value: validation::AcceptedResult,
) -> Result<(), Error> {
    sink.visit(1)?;
    accepted_snapshot(sink, value.snapshot_v1())
}
pub(super) fn accepted_snapshot(
    sink: &mut impl Sink,
    value: validation::AcceptedResultSnapshotV1,
) -> Result<(), Error> {
    types::binding(sink, value.binding)?;
    types::ledger(sink, value.ledger)?;
    raw(sink, &value.claim.0)?;
    types::target(sink, value.target)?;
    raw(sink, &value.validation.0)?;
    write_u32(sink, value.declaration_index)?;
    types::mode(sink, value.mode)?;
    types::verdict(sink, value.verdict)?;
    phase(sink, value.phase)?;
    optional(sink, value.attempt, write_u32)?;
    write_u64(sink, value.generation)?;
    types::optional_receipt(sink, value.receipt)?;
    optional_artifact(sink, value.evidence)?;
    optional_artifact(sink, value.programmatic_evidence)?;
    optional_participant(sink, value.reporter)?;
    evaluation_state(sink, value.resulting_state)
}
pub(super) fn result_key(sink: &mut impl Sink, value: NativeResultKey) -> Result<(), Error> {
    types::evaluation(sink, value.evaluation)?;
    write_u64(sink, value.revision.0)
}
