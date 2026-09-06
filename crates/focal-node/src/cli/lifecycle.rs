//! Thin authored adapters; standing, custody and run/revision fences remain shared.
use super::{
    CliError, Result,
    args::*,
    documents::{read_bytes, required},
};
use focal_client::{input::*, operations::*};

pub(super) fn claim_post(args: ClaimIdArgs) -> Result<(AuthoredOperation, MutationOptions)> {
    let document = match args.input.load(args.id.is_some())? {
        Some(document) => document,
        None => ClaimIdDocument {
            claim: required(args.id, "claim ID")?,
        },
    };
    Ok((AuthoredOperation::ClaimPost(document), args.mutation))
}
pub(super) fn claim_progress(args: ProgressArgs) -> Result<(AuthoredOperation, MutationOptions)> {
    let fields = args.id.is_some()
        || args.receipt.is_some()
        || args.receipt_epoch.is_some()
        || args.message.is_some();
    let document = match args.input.load(fields)? {
        Some(document) => document,
        None => ProgressDocument {
            claim: required(args.id, "claim ID")?,
            receipt: ReceiptDocument {
                id: required(args.receipt, "receipt")?,
                epoch: required(args.receipt_epoch, "receipt-epoch")?,
            },
            message: required(args.message, "message")?,
        },
    };
    Ok((AuthoredOperation::ClaimProgress(document), args.mutation))
}
pub(super) fn claim_cancel(args: CancelArgs) -> Result<(AuthoredOperation, MutationOptions)> {
    let fields = args.id.is_some() || args.reason.is_some();
    let document = match args.input.load(fields)? {
        Some(document) => document,
        None => CancelDocument {
            claim: required(args.id, "claim ID")?,
            reason: required(args.reason, "reason")?,
        },
    };
    Ok((AuthoredOperation::ClaimCancel(document), args.mutation))
}
pub(super) fn receipt(args: AcquireArgs) -> Result<(AuthoredOperation, MutationOptions)> {
    let fields = args.claim.is_some() || args.id.is_some() || args.epoch.is_some();
    let document = match args.input.load(fields)? {
        Some(document) => document,
        None => AcquireReceiptDocument {
            claim: required(args.claim, "claim ID")?,
            id: args.id,
            epoch: args.epoch.unwrap_or(1),
        },
    };
    Ok((AuthoredOperation::ReceiptAcquire(document), args.mutation))
}
pub(super) fn evidence(args: BeginEvidenceArgs) -> Result<(AuthoredOperation, MutationOptions)> {
    let fields = args.claim.is_some()
        || args.receipt.is_some()
        || args.receipt_epoch.is_some()
        || args.id.is_some();
    let document = match args.input.load(fields)? {
        Some(document) => document,
        None => BeginEvidenceDocument {
            claim: required(args.claim, "claim")?,
            receipt: ReceiptDocument {
                id: required(args.receipt, "receipt")?,
                epoch: required(args.receipt_epoch, "receipt-epoch")?,
            },
            id: args.id,
        },
    };
    Ok((AuthoredOperation::EvidenceBegin(document), args.mutation))
}
pub(super) fn receive(args: ReceiveTestamentArgs) -> Result<(AuthoredOperation, MutationOptions)> {
    let fields = args.id.is_some() || args.claim.is_some();
    let document = match args.input.load(fields)? {
        Some(document) => document,
        None => ReceiveTestamentDocument {
            claim: required(args.claim, "claim")?,
            testament: required(args.id, "testament ID")?,
        },
    };
    Ok((AuthoredOperation::TestamentReceive(document), args.mutation))
}
pub(super) fn validation_claim(
    args: ValidationClaimArgs,
    complete: bool,
) -> Result<(AuthoredOperation, MutationOptions)> {
    let document = match args.input.load(args.claim.is_some())? {
        Some(document) => document,
        None => ClaimIdDocument {
            claim: required(args.claim, "claim")?,
        },
    };
    let operation = if complete {
        AuthoredOperation::ValidationComplete(document)
    } else {
        AuthoredOperation::ValidationBegin(document)
    };
    Ok((operation, args.mutation))
}

pub(super) fn validation(args: ValidationArgs) -> Result<(AuthoredOperation, MutationOptions)> {
    let fields = args.validation.is_some()
        || args.target_hash.is_some()
        || args.phase.is_some()
        || args.epoch.is_some()
        || args.handler.is_some()
        || args.handler_version.is_some()
        || args.agentic.is_some()
        || args.attempt.is_some()
        || args.manifest.is_some()
        || args.receipt.is_some()
        || args.receipt_epoch.is_some()
        || args.value.is_some()
        || !args.evidence.is_empty();
    if let Some(document) = args.input.load(fields)? {
        return Ok((AuthoredOperation::ValidationSubmit(document), args.mutation));
    }
    if args.evidence.len() > 256 {
        return Err(InputError::Capacity.into());
    }
    let mut evidence = Vec::new();
    evidence
        .try_reserve_exact(args.evidence.len())
        .map_err(|_| InputError::Capacity)?;
    for reference in args.evidence {
        let (id, hash) = reference.split_once(':').ok_or_else(|| {
            CliError::Input("evidence must be ARTIFACT_ID:DESCRIPTOR_HASH".into())
        })?;
        evidence.push(ArtifactReferenceDocument {
            id: id.into(),
            hash: hash.into(),
        });
    }
    let receipt = match (args.receipt, args.receipt_epoch) {
        (None, None) => None,
        (id, epoch) => Some(ReceiptDocument {
            id: required(id, "receipt")?,
            epoch: required(epoch, "receipt-epoch")?,
        }),
    };
    Ok((
        AuthoredOperation::ValidationSubmit(ValidationVerdictDocument {
            validation: required(args.validation, "validation")?,
            target_hash: required(args.target_hash, "target-hash")?,
            phase: required(args.phase, "phase")?,
            epoch: required(args.epoch, "epoch")?,
            handler: HandlerDocument {
                id: required(args.handler, "handler")?,
                version: required(args.handler_version, "handler-version")?,
                agentic: args.agentic.unwrap_or(false),
            },
            attempt: required(args.attempt, "attempt")?,
            manifest: required(args.manifest, "manifest")?,
            receipt,
            value: required(args.value, "value")?,
            evidence,
        }),
        args.mutation,
    ))
}

pub(super) fn register(args: RegisterArtifactArgs) -> Result<(AuthoredOperation, MutationOptions)> {
    let fields = args.id.is_some()
        || args.kind.is_some()
        || args.schema_hash.is_some()
        || args.payload_file.is_some()
        || args.text.is_some()
        || args.metadata_file.is_some()
        || !args.input_json.is_empty()
        || !args.visibility.is_empty();
    if let Some(document) = args.input.load(fields)? {
        return Ok((AuthoredOperation::ArtifactRegister(document), args.mutation));
    }
    if args.input_json.len() > 256 || args.visibility.len() > 256 {
        return Err(InputError::Capacity.into());
    }
    let payload = match (args.payload_file, args.text) {
        (Some(path), None) => PayloadDocument::Inline {
            bytes: read_bytes(&path, 16 * 1024)?,
        },
        (None, Some(text)) => PayloadDocument::Text { text },
        _ => return Err(CliError::Input("provide --payload-file or --text".into())),
    };
    let mut inputs = Vec::new();
    inputs
        .try_reserve_exact(args.input_json.len())
        .map_err(|_| InputError::Capacity)?;
    let mut bytes = 0usize;
    for value in args.input_json {
        bytes = bytes
            .checked_add(value.len())
            .filter(|bytes| *bytes <= MAX_INPUT_BYTES)
            .ok_or(InputError::Capacity)?;
        inputs.push(parse_document(&value.into_bytes(), InputFormat::Json)?);
    }
    Ok((
        AuthoredOperation::ArtifactRegister(RegisterArtifactDocument {
            id: args.id,
            kind: required(args.kind, "kind")?,
            schema_hash: required(args.schema_hash, "schema-hash")?,
            metadata: args
                .metadata_file
                .map(|path| read_bytes(&path, 16 * 1024))
                .transpose()?
                .unwrap_or_default(),
            payload,
            inputs,
            visibility: args.visibility,
        }),
        args.mutation,
    ))
}

#[cfg(test)]
#[path = "lifecycle_tests.rs"]
mod tests;
