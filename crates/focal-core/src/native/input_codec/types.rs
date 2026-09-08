use super::super::*;
use super::bytes::{Error, Sink, write_count, write_raw, write_u8, write_u32, write_u64};
use focal_model::lifecycle::creation::Owner;
use focal_model::{ArtifactRef, ValidationMode, VerdictValue};

pub(in crate::native) fn ledger(sink: &mut impl Sink, value: LedgerId) -> Result<(), Error> {
    write_raw(sink, &value.tenant.0)?;
    write_raw(sink, &value.session.0)
}
pub(in crate::native) fn binding(sink: &mut impl Sink, value: Binding) -> Result<(), Error> {
    ledger(sink, value.ledger)?;
    write_raw(sink, &value.object.0)?;
    write_raw(sink, &value.content.0)?;
    write_u64(sink, value.revision.0)
}
pub(in crate::native) fn deadline(sink: &mut impl Sink, value: Deadline) -> Result<(), Error> {
    write_raw(sink, &value.timer.0)?;
    write_u64(sink, value.generation)?;
    write_u64(sink, value.at)
}
pub(in crate::native) fn receipt(sink: &mut impl Sink, value: ReceiptFence) -> Result<(), Error> {
    write_raw(sink, &value.receipt.0)?;
    write_u64(sink, value.epoch)
}
pub(in crate::native) fn optional_receipt(
    sink: &mut impl Sink,
    value: Option<ReceiptFence>,
) -> Result<(), Error> {
    match value {
        None => write_u8(sink, 0),
        Some(value) => {
            write_u8(sink, 1)?;
            receipt(sink, value)
        }
    }
}
pub(in crate::native) fn artifact_ref(
    sink: &mut impl Sink,
    value: ArtifactRef,
) -> Result<(), Error> {
    write_raw(sink, &value.id.0)?;
    write_raw(sink, &value.hash.0)
}
pub(in crate::native) fn owner(sink: &mut impl Sink, value: Option<Owner>) -> Result<(), Error> {
    match value {
        None => write_u8(sink, 0),
        Some(value) => {
            write_u8(sink, 1)?;
            binding(sink, value.expected)?;
            optional_receipt(sink, value.receipt)
        }
    }
}
pub(in crate::native) fn scope_limits(
    sink: &mut impl Sink,
    value: scope::ScopeLimits,
) -> Result<(), Error> {
    write_count(sink, value.scopes)?;
    write_count(sink, value.roots)?;
    write_count(sink, value.children)
}
pub(in crate::native) fn mode(sink: &mut impl Sink, value: ValidationMode) -> Result<(), Error> {
    write_u8(
        sink,
        match value {
            ValidationMode::Required => 0,
            ValidationMode::Observe => 1,
        },
    )
}
pub(in crate::native) fn verdict(sink: &mut impl Sink, value: VerdictValue) -> Result<(), Error> {
    write_u8(
        sink,
        match value {
            VerdictValue::Pass => 0,
            VerdictValue::Fail => 1,
            VerdictValue::Incomplete => 2,
            VerdictValue::Error => 3,
        },
    )
}
pub(in crate::native) fn failure(
    sink: &mut impl Sink,
    value: EvidenceFailure,
) -> Result<(), Error> {
    write_u8(
        sink,
        match value {
            EvidenceFailure::Work => 0,
            EvidenceFailure::Production => 1,
            EvidenceFailure::Structure => 2,
            EvidenceFailure::Metadata => 3,
        },
    )
}
pub(in crate::native) fn attempt(
    sink: &mut impl Sink,
    value: validation::Attempt,
) -> Result<(), Error> {
    write_u8(
        sink,
        match value.phase {
            validation::Phase::Programmatic => 0,
            validation::Phase::Quality => 1,
            validation::Phase::Delivery => 2,
            validation::Phase::MissingTarget => 3,
        },
    )?;
    write_u32(sink, value.index)?;
    write_raw(sink, &value.handler.0)?;
    write_raw(sink, &value.version.0)?;
    write_raw(sink, &value.evaluator.0)?;
    write_raw(sink, &value.definition.0)
}
pub(in crate::native) fn target(
    sink: &mut impl Sink,
    value: validation::Target,
) -> Result<(), Error> {
    match value {
        validation::Target::Artifact {
            response,
            slot,
            artifact,
        } => {
            write_u8(sink, 0)?;
            binding(sink, response)?;
            write_u32(sink, slot)?;
            binding(sink, artifact)
        }
        validation::Target::MissingSlot { response, slot } => {
            write_u8(sink, 1)?;
            binding(sink, response)?;
            write_u32(sink, slot)
        }
        validation::Target::Delivery { response } => {
            write_u8(sink, 2)?;
            binding(sink, response)
        }
        validation::Target::Admission { claim } => {
            write_u8(sink, 3)?;
            binding(sink, claim)
        }
        validation::Target::Increment { claim, artifact } => {
            write_u8(sink, 4)?;
            binding(sink, claim)?;
            binding(sink, artifact)
        }
    }
}
pub(in crate::native) fn evaluation(
    sink: &mut impl Sink,
    value: EvaluationKey,
) -> Result<(), Error> {
    write_raw(sink, &value.claim.0)?;
    write_raw(sink, &value.validation.0)?;
    write_u64(sink, value.generation)?;
    match value.target {
        EvaluationTarget::Admission => write_u8(sink, 0),
        EvaluationTarget::Increment { artifact } => {
            write_u8(sink, 1)?;
            write_raw(sink, &artifact.0)
        }
        EvaluationTarget::Work {
            response,
            slot,
            artifact,
        } => {
            write_u8(sink, 2)?;
            write_raw(sink, &response.0)?;
            write_u32(sink, slot)?;
            write_raw(sink, &artifact.0)
        }
        EvaluationTarget::MissingSlot { response, slot } => {
            write_u8(sink, 3)?;
            write_raw(sink, &response.0)?;
            write_u32(sink, slot)
        }
        EvaluationTarget::Delivery { response } => {
            write_u8(sink, 4)?;
            write_raw(sink, &response.0)
        }
    }
}
pub(in crate::native) fn report(
    sink: &mut impl Sink,
    value: validation::Report,
) -> Result<(), Error> {
    write_u64(sink, value.generation)?;
    attempt(sink, value.attempt)?;
    verdict(sink, value.value)?;
    artifact_ref(sink, value.evidence)
}
