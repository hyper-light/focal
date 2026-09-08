//! Checked scalar fields shared by borrowed artifact input inspection.
use super::bytes::Cursor;
use super::*;
use focal_model::lifecycle::artifact_descriptor::{
    ArtifactFields, ContentPointer, PayloadSpec, ResultProvenance, WorkProvenance, WorkRole,
};
use focal_model::{
    ContentClass, ContentDomainId, ObjectId, ObjectKind, ObjectRef, ValidatorId, VerdictValue,
};

pub(super) fn failure(cursor: &mut Cursor<'_>) -> Result<EvidenceFailure, CodecError> {
    match cursor.u8()? {
        0 => Ok(EvidenceFailure::Work),
        1 => Ok(EvidenceFailure::Production),
        2 => Ok(EvidenceFailure::Structure),
        3 => Ok(EvidenceFailure::Metadata),
        _ => Err(CodecError::InvalidTag("failure")),
    }
}
fn option(cursor: &mut Cursor<'_>) -> Result<bool, CodecError> {
    match cursor.u8()? {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(CodecError::InvalidTag("option")),
    }
}
fn verdict(cursor: &mut Cursor<'_>) -> Result<VerdictValue, CodecError> {
    match cursor.u8()? {
        0 => Ok(VerdictValue::Pass),
        1 => Ok(VerdictValue::Fail),
        2 => Ok(VerdictValue::Incomplete),
        3 => Ok(VerdictValue::Error),
        _ => Err(CodecError::InvalidTag("verdict")),
    }
}
fn attempt(cursor: &mut Cursor<'_>) -> Result<validation::Attempt, CodecError> {
    let phase = match cursor.u8()? {
        0 => validation::Phase::Programmatic,
        1 => validation::Phase::Quality,
        2 => validation::Phase::Delivery,
        3 => validation::Phase::MissingTarget,
        _ => return Err(CodecError::InvalidTag("attempt phase")),
    };
    Ok(validation::Attempt {
        phase,
        index: cursor.u32()?,
        handler: ValidatorId(cursor.fixed()?),
        version: ContentHash(cursor.fixed()?),
        evaluator: ParticipantId(cursor.fixed()?),
        definition: ContentHash(cursor.fixed()?),
    })
}
pub(super) fn report(cursor: &mut Cursor<'_>) -> Result<validation::Report, CodecError> {
    Ok(validation::Report {
        generation: cursor.u64()?,
        attempt: attempt(cursor)?,
        value: verdict(cursor)?,
        evidence: fixed::artifact_ref(cursor)?,
    })
}
fn target(cursor: &mut Cursor<'_>) -> Result<validation::Target, CodecError> {
    match cursor.u8()? {
        0 => Ok(validation::Target::Artifact {
            response: fixed::binding(cursor)?,
            slot: cursor.u32()?,
            artifact: fixed::binding(cursor)?,
        }),
        1 => Ok(validation::Target::MissingSlot {
            response: fixed::binding(cursor)?,
            slot: cursor.u32()?,
        }),
        2 => Ok(validation::Target::Delivery {
            response: fixed::binding(cursor)?,
        }),
        3 => Ok(validation::Target::Admission {
            claim: fixed::binding(cursor)?,
        }),
        4 => Ok(validation::Target::Increment {
            claim: fixed::binding(cursor)?,
            artifact: fixed::binding(cursor)?,
        }),
        _ => Err(CodecError::InvalidTag("validation target")),
    }
}
fn blob<'a>(cursor: &mut Cursor<'a>) -> Result<&'a [u8], CodecError> {
    let len = cursor.count(cursor.remaining())?;
    cursor.take(len)
}
pub(super) fn fields<'a>(cursor: &mut Cursor<'a>) -> Result<ArtifactFields<'a>, CodecError> {
    let ledger = fixed::ledger(cursor)?;
    let id = ArtifactId(cursor.fixed()?);
    let schema = cursor.u16()?;
    let kind = cursor.text(cursor.remaining())?;
    let schema_hash = ContentHash(cursor.fixed()?);
    let metadata = blob(cursor)?;
    let payload = match cursor.u8()? {
        0 => PayloadSpec::Inline(blob(cursor)?),
        1 => {
            let domain = ContentDomainId(cursor.fixed()?);
            let root = ContentHash(cursor.fixed()?);
            let length = cursor.u64()?;
            let class = match cursor.u16()? {
                1 => ContentClass::Document,
                2 => ContentClass::Evidence,
                3 => ContentClass::Checkpoint,
                _ => return Err(CodecError::InvalidTag("content class")),
            };
            PayloadSpec::Content(ContentPointer {
                domain,
                root,
                length,
                class,
            })
        }
        _ => return Err(CodecError::InvalidTag("artifact payload")),
    };
    let producer = ParticipantId(cursor.fixed()?);
    let receipt = fixed::optional_receipt(cursor)?;
    let result = if option(cursor)? {
        Some(ResultProvenance {
            claim: ClaimId(cursor.fixed()?),
            validation: ValidationId(cursor.fixed()?),
            target: target(cursor)?,
            generation: cursor.u64()?,
            attempt: attempt(cursor)?,
            value: verdict(cursor)?,
        })
    } else {
        None
    };
    let work = if option(cursor)? {
        let claim = ClaimId(cursor.fixed()?);
        let cycle = cursor.u32()?;
        let role = match cursor.u8()? {
            0 => WorkRole::Output {
                slot: cursor.u32()?,
            },
            1 => WorkRole::Diagnostic {
                reason: failure(cursor)?,
            },
            2 => WorkRole::ReceiptRejection {
                artifact: fixed::artifact_ref(cursor)?,
                reason: failure(cursor)?,
            },
            _ => return Err(CodecError::InvalidTag("work role")),
        };
        Some(WorkProvenance { claim, cycle, role })
    } else {
        None
    };
    Ok(ArtifactFields {
        ledger,
        id,
        schema,
        kind,
        schema_hash,
        metadata,
        payload,
        producer,
        receipt,
        result,
        work,
    })
}
pub(super) fn object_ref(cursor: &mut Cursor<'_>) -> Result<ObjectRef, CodecError> {
    let ledger = fixed::ledger(cursor)?;
    let kind = match cursor.u16()? {
        1 => ObjectKind::Claim,
        2 => ObjectKind::Testament,
        3 => ObjectKind::Validation,
        4 => ObjectKind::Artifact,
        _ => return Err(CodecError::InvalidTag("object family")),
    };
    Ok(ObjectRef {
        ledger,
        kind,
        id: ObjectId(cursor.fixed()?),
    })
}
