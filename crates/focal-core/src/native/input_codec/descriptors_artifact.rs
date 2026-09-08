use super::super::{bytes, types};
use super::{Error, Sink, iterations};
use bytes::{
    write_count as count, write_raw as raw, write_text as text, write_u8, write_u16, write_u32,
    write_u64,
};
use focal_model::lifecycle::artifact_descriptor::{ArtifactDescriptor, PayloadSpec, WorkRole};
use focal_model::{ContentClass, ObjectKind, VerdictValue};

/// Encode the complete authored body. A decoder must recompute the descriptor's
/// derived content hash from these fields; external schema and payload roots
/// remain part of the authored input.
pub(super) fn encode(sink: &mut impl Sink, value: &ArtifactDescriptor) -> Result<(), Error> {
    raw(sink, &value.ledger().tenant.0)?;
    raw(sink, &value.ledger().session.0)?;
    raw(sink, &value.id().0)?;
    write_u16(sink, value.schema())?;
    text(sink, value.kind())?;
    raw(sink, &value.schema_hash().0)?;
    count(sink, value.metadata().len())?;
    raw(sink, value.metadata())?;
    match value.payload() {
        PayloadSpec::Inline(payload) => {
            write_u8(sink, 0)?;
            count(sink, payload.len())?;
            raw(sink, payload)?;
        }
        PayloadSpec::Content(pointer) => {
            write_u8(sink, 1)?;
            raw(sink, &pointer.domain.0)?;
            raw(sink, &pointer.root.0)?;
            write_u64(sink, pointer.length)?;
            write_u16(
                sink,
                match pointer.class {
                    ContentClass::Document => 1,
                    ContentClass::Evidence => 2,
                    ContentClass::Checkpoint => 3,
                },
            )?;
        }
    }
    raw(sink, &value.producer().0)?;
    match value.receipt() {
        None => write_u8(sink, 0)?,
        Some(receipt) => {
            write_u8(sink, 1)?;
            types::receipt(sink, receipt)?;
        }
    }
    match value.result_provenance() {
        None => write_u8(sink, 0)?,
        Some(result) => {
            write_u8(sink, 1)?;
            raw(sink, &result.claim.0)?;
            raw(sink, &result.validation.0)?;
            types::target(sink, result.target)?;
            write_u64(sink, result.generation)?;
            types::attempt(sink, result.attempt)?;
            write_u8(
                sink,
                match result.value {
                    VerdictValue::Pass => 0,
                    VerdictValue::Fail => 1,
                    VerdictValue::Incomplete => 2,
                    VerdictValue::Error => 3,
                },
            )?;
        }
    }
    match value.work_provenance() {
        None => write_u8(sink, 0)?,
        Some(work) => {
            write_u8(sink, 1)?;
            raw(sink, &work.claim.0)?;
            write_u32(sink, work.cycle)?;
            match work.role {
                WorkRole::Output { slot } => {
                    write_u8(sink, 0)?;
                    write_u32(sink, slot)?;
                }
                WorkRole::Diagnostic { reason } => {
                    write_u8(sink, 1)?;
                    types::failure(sink, reason)?;
                }
                WorkRole::ReceiptRejection { artifact, reason } => {
                    write_u8(sink, 2)?;
                    types::artifact_ref(sink, artifact)?;
                    types::failure(sink, reason)?;
                }
            }
        }
    }
    count(sink, value.inputs().len())?;
    iterations(sink, value.inputs().len())?;
    for input in value.inputs() {
        raw(sink, &input.ledger.tenant.0)?;
        raw(sink, &input.ledger.session.0)?;
        write_u16(
            sink,
            match input.kind {
                ObjectKind::Claim => 1,
                ObjectKind::Testament => 2,
                ObjectKind::Validation => 3,
                ObjectKind::Artifact => 4,
            },
        )?;
        raw(sink, &input.id.0)?;
    }
    let visibility = value.visibility();
    count(sink, visibility.len())?;
    iterations(sink, visibility.len())?;
    for label in visibility {
        text(sink, label)?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "descriptors_artifact_tests.rs"]
mod tests;
