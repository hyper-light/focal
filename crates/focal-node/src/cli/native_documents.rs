//! Flag adaptation for the native engine: every verb becomes one authored
//! native document; the shared compiler validates and compiles it. Legacy
//! V1-only flags (receipt fences, evidence sets) are refused rather than
//! silently dropped.
use super::{
    CliError, Result,
    args::*,
    documents::{read_bytes, required},
};
use focal_client::input::{InputFormat, MAX_INPUT_BYTES, parse_document};
use focal_client::operations::*;

/// Inline payloads are bounded by the compiler's artifact limit.
const MAX_PAYLOAD_BYTES: usize = 256 * 1024;

fn refuse_legacy(present: bool, flag: &str) -> Result<()> {
    if present {
        return Err(CliError::Input(format!(
            "--{flag} belongs to the V1 engine; the native engine binds receipts and cycles from the committed claim"
        )));
    }
    Ok(())
}
fn reference(value: &str, what: &str) -> Result<NativeArtifactReferenceDocument> {
    let (id, hash) = value
        .split_once(':')
        .ok_or_else(|| CliError::Input(format!("{what} must be ARTIFACT_ID:HASH")))?;
    Ok(NativeArtifactReferenceDocument {
        id: id.into(),
        hash: hash.into(),
    })
}
fn payload(
    file: Option<std::path::PathBuf>,
    text: Option<String>,
) -> Result<NativePayloadDocument> {
    match (file, text) {
        (Some(path), None) if path != std::path::Path::new("-") => {
            Ok(NativePayloadDocument::Inline {
                bytes: read_bytes(&path, MAX_PAYLOAD_BYTES)?,
            })
        }
        (Some(_), None) => {
            use std::io::Read;
            let mut bytes = Vec::new();
            std::io::stdin()
                .lock()
                .take((MAX_PAYLOAD_BYTES as u64).saturating_add(1))
                .read_to_end(&mut bytes)?;
            if bytes.len() > MAX_PAYLOAD_BYTES {
                return Err(CliError::Input("payload exceeds the inline bound".into()));
            }
            Ok(NativePayloadDocument::Inline { bytes })
        }
        (None, Some(text)) => Ok(NativePayloadDocument::Text { text }),
        _ => Err(CliError::Input(
            "choose --text or --payload-file for the payload".into(),
        )),
    }
}
fn metadata(file: Option<std::path::PathBuf>) -> Result<Vec<u8>> {
    file.map(|path| read_bytes(&path, 16 * 1024))
        .transpose()
        .map(Option::unwrap_or_default)
}
fn inputs(values: Vec<String>) -> Result<Vec<NativeObjectReferenceDocument>> {
    values
        .into_iter()
        .map(|json| Ok(parse_document(json.as_bytes(), InputFormat::Json)?))
        .collect()
}

pub(super) fn claim(args: ClaimArgs) -> Result<(NativeClaimDocument, MutationOptions)> {
    let fields = args.id.is_some()
        || args.occurrence.is_some()
        || args.description.is_some()
        || args.target.is_some()
        || args.action.is_some()
        || !args.scope.is_empty()
        || !args.relation.is_empty()
        || !args.validation_file.is_empty()
        || !args.validation_json.is_empty()
        || args.deadline_json.is_some()
        || !args.slot_json.is_empty()
        || args.parent.is_some()
        || args.max_responses.is_some()
        || args.policy_json.is_some();
    if let Some(document) = args.input.load(fields)? {
        return Ok((document, args.mutation));
    }
    let count = args
        .validation_file
        .len()
        .saturating_add(args.validation_json.len());
    if count > 64
        || args.scope.len() > 256
        || args.relation.len() > 248
        || args.slot_json.len() > 64
    {
        return Err(CliError::Input(
            "too many validation definitions, scopes, relations or slots".into(),
        ));
    }
    let mut validations = Vec::new();
    let mut total = 0usize;
    for path in args.validation_file {
        let bytes = read_bytes(&path, MAX_INPUT_BYTES)?;
        total = total.saturating_add(bytes.len());
        let format = if path
            .extension()
            .is_some_and(|ext| ext == "yaml" || ext == "yml")
        {
            InputFormat::Yaml
        } else {
            InputFormat::Json
        };
        validations.push(parse_document(&bytes, format)?);
    }
    for json in args.validation_json {
        total = total.saturating_add(json.len());
        validations.push(parse_document(json.as_bytes(), InputFormat::Json)?);
    }
    let mut slots = Vec::new();
    for json in args.slot_json {
        total = total.saturating_add(json.len());
        slots.push(parse_document(json.as_bytes(), InputFormat::Json)?);
    }
    if total > MAX_INPUT_BYTES {
        return Err(CliError::Input("authored input exceeds its bound".into()));
    }
    let scopes = args
        .scope
        .into_iter()
        .map(|value| {
            let (kind, key) = value
                .split_once(':')
                .ok_or_else(|| CliError::Input("scope must be KIND:KEY".into()))?;
            Ok(NativeScopeDocument {
                kind: kind.into(),
                key: key.into(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let relations = args
        .relation
        .into_iter()
        .map(|value| {
            let (kind, target) = value
                .split_once(':')
                .ok_or_else(|| CliError::Input("relation must be KIND:CLAIM_ID".into()))?;
            // `artifact:ID@HASH` names exact evidence; anything else is a claim.
            let target = if target.starts_with("artifact:") {
                target.to_owned()
            } else {
                format!("claim:{}", target.strip_prefix("claim:").unwrap_or(target))
            };
            Ok(NativeRelationDocument {
                kind: kind.into(),
                target,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((
        NativeClaimDocument {
            id: args.id,
            occurrence: args.occurrence,
            description: required(args.description, "description")?,
            target: required(args.target, "target")?,
            action: args.action.unwrap_or_else(|| "work".into()),
            scopes,
            relations,
            deadline: args
                .deadline_json
                .map(|json| parse_document(json.as_bytes(), InputFormat::Json))
                .transpose()?,
            validations,
            slots,
            max_responses: args.max_responses.unwrap_or(4),
            scope_limits: NativeScopeLimitsDocument::default(),
            parent: args.parent,
            policy: args
                .policy_json
                .map(|json| parse_document(json.as_bytes(), InputFormat::Json))
                .transpose()?,
        },
        args.mutation,
    ))
}

/// Whether any authored field flag of a peer verb was given.
fn peer_fields(args: &PeerClaimArgs) -> bool {
    args.id.is_some()
        || args.occurrence.is_some()
        || args.description.is_some()
        || args.target.is_some()
        || args.artifact.is_some()
        || !args.scope.is_empty()
        || !args.relation.is_empty()
        || !args.validation_file.is_empty()
        || !args.validation_json.is_empty()
        || args.deadline_json.is_some()
        || !args.slot_json.is_empty()
        || args.parent.is_some()
        || args.max_responses.is_some()
        || args.policy_json.is_some()
}
/// The authored fields of a peer verb, assembled through the claim builder
/// so scopes, relations, validations, slots, deadline and policy parse the
/// same way as for `submit claim`. The document form is handled by the verb.
fn peer_claim(args: PeerClaimArgs, target: Option<String>) -> Result<NativeClaimDocument> {
    let (document, _) = claim(ClaimArgs {
        input: DocumentInput {
            json: None,
            yaml: None,
            file: None,
            input_format: None,
        },
        id: args.id,
        occurrence: args.occurrence,
        description: args.description,
        target: args.target.or(target),
        action: None,
        scope: args.scope,
        relation: args.relation,
        validation_file: args.validation_file,
        validation_json: args.validation_json,
        deadline_json: args.deadline_json,
        slot_json: args.slot_json,
        parent: args.parent,
        max_responses: args.max_responses,
        policy_json: args.policy_json,
        mutation: MutationOptions {
            operation: None,
            operation_id: None,
            expected_revision: None,
            output: OutputOptions {
                format: OutputFormat::Table,
            },
        },
    })?;
    Ok(document)
}
pub(super) fn challenge(
    mut args: PeerClaimArgs,
) -> Result<(NativeChallengeDocument, MutationOptions)> {
    let input = std::mem::replace(
        &mut args.input,
        DocumentInput {
            json: None,
            yaml: None,
            file: None,
            input_format: None,
        },
    );
    if let Some(document) = input.load(peer_fields(&args))? {
        return Ok((document, args.mutation));
    }
    let artifact = args.artifact.take();
    let mutation = std::mem::replace(
        &mut args.mutation,
        MutationOptions {
            operation: None,
            operation_id: None,
            expected_revision: None,
            output: OutputOptions {
                format: OutputFormat::Table,
            },
        },
    );
    let claim = peer_claim(args, None)?;
    let policy = claim.policy.ok_or_else(|| {
        CliError::Input("a challenge needs --policy-json: its follow-up policy is immutable".into())
    })?;
    Ok((
        NativeChallengeDocument {
            id: claim.id,
            occurrence: claim.occurrence,
            description: claim.description,
            target: claim.target,
            artifact,
            scopes: claim.scopes,
            relations: claim.relations,
            deadline: claim.deadline,
            validations: claim.validations,
            slots: claim.slots,
            max_responses: claim.max_responses,
            scope_limits: claim.scope_limits,
            parent: claim.parent,
            policy,
        },
        mutation,
    ))
}
pub(super) fn consult(mut args: PeerClaimArgs) -> Result<(NativeConsultDocument, MutationOptions)> {
    let input = std::mem::replace(
        &mut args.input,
        DocumentInput {
            json: None,
            yaml: None,
            file: None,
            input_format: None,
        },
    );
    if let Some(document) = input.load(peer_fields(&args))? {
        return Ok((document, args.mutation));
    }
    if args.artifact.is_some() {
        return Err(CliError::Input(
            "--artifact names the disputed artifact of a challenge; a consultation reviews through --relation".into(),
        ));
    }
    let mutation = std::mem::replace(
        &mut args.mutation,
        MutationOptions {
            operation: None,
            operation_id: None,
            expected_revision: None,
            output: OutputOptions {
                format: OutputFormat::Table,
            },
        },
    );
    let claim = peer_claim(args, None)?;
    Ok((
        NativeConsultDocument {
            id: claim.id,
            occurrence: claim.occurrence,
            description: claim.description,
            target: claim.target,
            scopes: claim.scopes,
            relations: claim.relations,
            deadline: claim.deadline,
            validations: claim.validations,
            slots: claim.slots,
            max_responses: claim.max_responses,
            scope_limits: claim.scope_limits,
            parent: claim.parent,
            policy: claim.policy,
        },
        mutation,
    ))
}
/// The compiler reads the followed claim's subject when no target is given;
/// the builder therefore accepts an absent target and marks it so.
const FOLLOWED_SUBJECT: &str = "followed";
pub(super) fn correct(args: CorrectArgs) -> Result<(NativeCorrectionDocument, MutationOptions)> {
    let CorrectArgs {
        challenge,
        verdict,
        mut claim,
    } = args;
    let input = std::mem::replace(
        &mut claim.input,
        DocumentInput {
            json: None,
            yaml: None,
            file: None,
            input_format: None,
        },
    );
    if let Some(document) =
        input.load(peer_fields(&claim) || challenge.is_some() || verdict.is_some())?
    {
        return Ok((document, claim.mutation));
    }
    if claim.artifact.is_some() {
        return Err(CliError::Input(
            "--artifact belongs to a challenge; a correction cites the verdict report with --verdict".into(),
        ));
    }
    let mutation = std::mem::replace(
        &mut claim.mutation,
        MutationOptions {
            operation: None,
            operation_id: None,
            expected_revision: None,
            output: OutputOptions {
                format: OutputFormat::Table,
            },
        },
    );
    let explicit_target = claim.target.is_some();
    let document = peer_claim(claim, Some(FOLLOWED_SUBJECT.into()))?;
    Ok((
        NativeCorrectionDocument {
            id: document.id,
            occurrence: document.occurrence,
            challenge: required(challenge, "--challenge")?,
            verdict: required(verdict, "--verdict")?,
            description: document.description,
            target: explicit_target.then_some(document.target),
            scopes: document.scopes,
            relations: document.relations,
            deadline: document.deadline,
            validations: document.validations,
            slots: document.slots,
            max_responses: document.max_responses,
            scope_limits: document.scope_limits,
            parent: document.parent,
            policy: document.policy,
        },
        mutation,
    ))
}
pub(super) fn follow_up(args: FollowUpArgs) -> Result<(NativeFollowUpDocument, MutationOptions)> {
    let FollowUpArgs { refines, mut claim } = args;
    let input = std::mem::replace(
        &mut claim.input,
        DocumentInput {
            json: None,
            yaml: None,
            file: None,
            input_format: None,
        },
    );
    if let Some(document) = input.load(peer_fields(&claim) || refines.is_some())? {
        return Ok((document, claim.mutation));
    }
    if claim.artifact.is_some() {
        return Err(CliError::Input(
            "--artifact belongs to a challenge; a follow-up reviews through --relation".into(),
        ));
    }
    let mutation = std::mem::replace(
        &mut claim.mutation,
        MutationOptions {
            operation: None,
            operation_id: None,
            expected_revision: None,
            output: OutputOptions {
                format: OutputFormat::Table,
            },
        },
    );
    let explicit_target = claim.target.is_some();
    let document = peer_claim(claim, Some(FOLLOWED_SUBJECT.into()))?;
    Ok((
        NativeFollowUpDocument {
            id: document.id,
            occurrence: document.occurrence,
            refines: required(refines, "--refines")?,
            description: document.description,
            target: explicit_target.then_some(document.target),
            scopes: document.scopes,
            relations: document.relations,
            deadline: document.deadline,
            validations: document.validations,
            slots: document.slots,
            max_responses: document.max_responses,
            scope_limits: document.scope_limits,
            parent: document.parent,
            policy: document.policy,
        },
        mutation,
    ))
}
pub(super) fn claim_target(
    args: ClaimIdArgs,
) -> Result<(NativeClaimTargetDocument, MutationOptions)> {
    let document = match args.input.load(args.id.is_some())? {
        Some(document) => document,
        None => NativeClaimTargetDocument {
            claim: required(args.id, "claim ID")?,
        },
    };
    Ok((document, args.mutation))
}
pub(super) fn cancel(args: CancelArgs) -> Result<(NativeClaimTargetDocument, MutationOptions)> {
    if args.reason.is_some() {
        return Err(CliError::Input(
            "the native engine records no cancellation reason; omit --reason".into(),
        ));
    }
    let document = match args.input.load(args.id.is_some())? {
        Some(document) => document,
        None => NativeClaimTargetDocument {
            claim: required(args.id, "claim ID")?,
        },
    };
    Ok((document, args.mutation))
}
pub(super) fn receipt(args: AcquireArgs) -> Result<(NativeReceiptDocument, MutationOptions)> {
    if args.epoch.is_some_and(|epoch| epoch != 1) {
        return Err(CliError::Input(
            "the native engine assigns receipt epochs; omit --epoch".into(),
        ));
    }
    let fields = args.claim.is_some() || args.id.is_some();
    let document = match args.input.load(fields)? {
        Some(document) => document,
        None => NativeReceiptDocument {
            claim: required(args.claim, "claim ID")?,
            id: args.id,
        },
    };
    Ok((document, args.mutation))
}
pub(super) fn work(args: ArtifactArgs) -> Result<(NativeWorkArtifactDocument, MutationOptions)> {
    refuse_legacy(args.receipt.is_some(), "receipt")?;
    refuse_legacy(args.receipt_epoch.is_some(), "receipt-epoch")?;
    refuse_legacy(args.evidence_set.is_some(), "evidence-set")?;
    let fields = args.id.is_some()
        || args.claim.is_some()
        || args.kind.is_some()
        || args.schema_hash.is_some()
        || args.payload_file.is_some()
        || args.text.is_some()
        || args.metadata_file.is_some()
        || args.slot.is_some()
        || !args.input_json.is_empty()
        || !args.visibility.is_empty();
    if let Some(document) = args.input.load(fields)? {
        return Ok((document, args.mutation));
    }
    Ok((
        NativeWorkArtifactDocument {
            claim: required(args.claim, "claim")?,
            slot: required(args.slot, "slot")?,
            id: args.id,
            kind: args.kind,
            schema_hash: args.schema_hash,
            metadata: metadata(args.metadata_file)?,
            payload: payload(args.payload_file, args.text)?,
            inputs: inputs(args.input_json)?,
            visibility: args.visibility,
        },
        args.mutation,
    ))
}
pub(super) fn diagnostic(
    args: DiagnosticArgs,
) -> Result<(NativeDiagnosticDocument, MutationOptions)> {
    let fields = args.id.is_some()
        || args.claim.is_some()
        || args.reason.is_some()
        || args.kind.is_some()
        || args.schema_hash.is_some()
        || args.payload_file.is_some()
        || args.text.is_some()
        || args.metadata_file.is_some()
        || !args.input_json.is_empty()
        || !args.visibility.is_empty();
    if let Some(document) = args.input.load(fields)? {
        return Ok((document, args.mutation));
    }
    Ok((
        NativeDiagnosticDocument {
            claim: required(args.claim, "claim")?,
            reason: required(args.reason, "reason")?,
            id: args.id,
            kind: args.kind,
            schema_hash: args.schema_hash,
            metadata: metadata(args.metadata_file)?,
            payload: payload(args.payload_file, args.text)?,
            inputs: inputs(args.input_json)?,
            visibility: args.visibility,
        },
        args.mutation,
    ))
}
pub(super) fn testament(args: TestamentArgs) -> Result<(NativeResponseDocument, MutationOptions)> {
    refuse_legacy(args.receipt.is_some(), "receipt")?;
    refuse_legacy(args.receipt_epoch.is_some(), "receipt-epoch")?;
    refuse_legacy(args.evidence_set.is_some(), "evidence-set")?;
    refuse_legacy(!args.artifact.is_empty(), "artifact")?;
    refuse_legacy(args.manifest_file.is_some(), "manifest-file")?;
    let fields = args.id.is_some()
        || args.claim.is_some()
        || args.summary.is_some()
        || args.confidence.is_some()
        || args.outcome.is_some()
        || !args.slot.is_empty()
        || !args.diagnostic.is_empty();
    if let Some(document) = args.input.load(fields)? {
        return Ok((document, args.mutation));
    }
    let manifest = args
        .slot
        .iter()
        .map(|value| {
            let (slot, artifact) = value
                .split_once('=')
                .ok_or_else(|| CliError::Input("slot must be SLOT=ARTIFACT_ID:HASH".into()))?;
            Ok(NativeSlotBindingDocument {
                slot: slot
                    .parse()
                    .map_err(|_| CliError::Input("slot number".into()))?,
                artifact: reference(artifact, "slot artifact")?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let diagnostics = args
        .diagnostic
        .iter()
        .map(|value| reference(value, "diagnostic"))
        .collect::<Result<Vec<_>>>()?;
    Ok((
        NativeResponseDocument {
            claim: required(args.claim, "claim")?,
            id: args.id,
            summary: required(args.summary, "summary")?,
            confidence: required(args.confidence, "confidence")?,
            outcome: required(args.outcome, "outcome")?,
            manifest,
            diagnostics,
        },
        args.mutation,
    ))
}
pub(super) fn response_target(
    args: ReceiveTestamentArgs,
) -> Result<(NativeResponseTargetDocument, MutationOptions)> {
    let fields = args.id.is_some() || args.claim.is_some();
    let document = match args.input.load(fields)? {
        Some(document) => document,
        None => NativeResponseTargetDocument {
            claim: required(args.claim, "claim")?,
            testament: required(args.id, "testament ID")?,
        },
    };
    Ok((document, args.mutation))
}
pub(super) fn begin(
    args: ValidationClaimArgs,
) -> Result<(NativeEvaluationDocument, MutationOptions)> {
    let fields = args.claim.is_some()
        || args.validation.is_some()
        || args.slot.is_some()
        || args.phase.is_some()
        || args.target.is_some();
    let document = match args.input.load(fields)? {
        Some(document) => document,
        None => NativeEvaluationDocument {
            claim: required(args.claim, "claim")?,
            validation: required(args.validation, "validation")?,
            slot: args.slot,
            phase: args.phase.unwrap_or_else(|| "whole_work".into()),
            target: args.target,
        },
    };
    Ok((document, args.mutation))
}
pub(super) fn seal_increments(
    args: ClaimFlagArgs,
) -> Result<(NativeClaimTargetDocument, MutationOptions)> {
    let document = match args.input.load(args.claim.is_some())? {
        Some(document) => document,
        None => NativeClaimTargetDocument {
            claim: required(args.claim, "claim")?,
        },
    };
    Ok((document, args.mutation))
}
pub(super) fn adopt(args: AdoptArgs) -> Result<(NativeAdoptReceiptDocument, MutationOptions)> {
    let fields = args.claim.is_some() || args.holder.is_some() || args.id.is_some();
    let document = match args.input.load(fields)? {
        Some(document) => document,
        None => NativeAdoptReceiptDocument {
            claim: required(args.claim, "claim ID")?,
            holder: required(args.holder, "holder")?,
            id: args.id,
        },
    };
    Ok((document, args.mutation))
}
pub(super) fn fail(args: FailArgs) -> Result<(NativeFailWorkDocument, MutationOptions)> {
    let fields = args.claim.is_some() || args.slot.is_some() || args.diagnostic.is_some();
    if let Some(document) = args.input.load(fields)? {
        return Ok((document, args.mutation));
    }
    let diagnostic = required(args.diagnostic, "diagnostic")?;
    let (diagnostic, hash) = match diagnostic.split_once(':') {
        Some((id, hash)) => (id.to_owned(), Some(hash.to_owned())),
        None => (diagnostic, None),
    };
    Ok((
        NativeFailWorkDocument {
            claim: required(args.claim, "claim")?,
            slot: required(args.slot, "slot")?,
            diagnostic,
            hash,
        },
        args.mutation,
    ))
}
pub(super) fn artifact_target(
    args: ArtifactTargetArgs,
) -> Result<(NativeArtifactTargetDocument, MutationOptions)> {
    let fields = args.id.is_some() || args.claim.is_some();
    let document = match args.input.load(fields)? {
        Some(document) => document,
        None => NativeArtifactTargetDocument {
            claim: required(args.claim, "claim")?,
            artifact: required(args.id, "artifact ID")?,
        },
    };
    Ok((document, args.mutation))
}
pub(super) fn reject(args: RejectArgs) -> Result<(NativeRejectWorkDocument, MutationOptions)> {
    let fields = args.artifact.is_some()
        || args.id.is_some()
        || args.claim.is_some()
        || args.reason.is_some()
        || args.kind.is_some()
        || args.schema_hash.is_some()
        || args.payload_file.is_some()
        || args.text.is_some()
        || args.metadata_file.is_some()
        || !args.input_json.is_empty()
        || !args.visibility.is_empty();
    if let Some(document) = args.input.load(fields)? {
        return Ok((document, args.mutation));
    }
    Ok((
        NativeRejectWorkDocument {
            claim: required(args.claim, "claim")?,
            artifact: required(args.artifact, "artifact ID")?,
            reason: required(args.reason, "reason")?,
            id: args.id,
            kind: args.kind,
            schema_hash: args.schema_hash,
            metadata: metadata(args.metadata_file)?,
            payload: payload(args.payload_file, args.text)?,
            inputs: inputs(args.input_json)?,
            visibility: args.visibility,
        },
        args.mutation,
    ))
}
pub(super) fn audit(args: AuditArgs) -> Result<(NativeAuditDocument, MutationOptions)> {
    let fields = args.claim.is_some() || args.id.is_some();
    let document = match args.input.load(fields)? {
        Some(document) => document,
        None => NativeAuditDocument {
            claim: required(args.claim, "claim")?,
            id: args.id,
        },
    };
    Ok((document, args.mutation))
}
pub(super) fn audit_target(
    args: AuditPostArgs,
) -> Result<(NativeAuditTargetDocument, MutationOptions)> {
    let document = match args.input.load(args.id.is_some())? {
        Some(document) => document,
        None => NativeAuditTargetDocument {
            testament: required(args.id, "testament ID")?,
        },
    };
    Ok((document, args.mutation))
}
pub(super) fn report(args: ReportArgs) -> Result<(NativeReportDocument, MutationOptions)> {
    let fields = args.id.is_some()
        || args.claim.is_some()
        || args.validation.is_some()
        || args.slot.is_some()
        || args.phase.is_some()
        || args.target.is_some()
        || args.verdict.is_some()
        || args.kind.is_some()
        || args.schema_hash.is_some()
        || args.payload_file.is_some()
        || args.text.is_some()
        || args.metadata_file.is_some()
        || !args.input_json.is_empty()
        || !args.visibility.is_empty();
    if let Some(document) = args.input.load(fields)? {
        return Ok((document, args.mutation));
    }
    Ok((
        NativeReportDocument {
            claim: required(args.claim, "claim")?,
            validation: required(args.validation, "validation")?,
            slot: args.slot,
            phase: args.phase.unwrap_or_else(|| "whole_work".into()),
            target: args.target,
            verdict: required(args.verdict, "verdict")?,
            id: args.id,
            kind: args.kind,
            schema_hash: args.schema_hash,
            metadata: metadata(args.metadata_file)?,
            payload: payload(args.payload_file, args.text)?,
            inputs: inputs(args.input_json)?,
            visibility: args.visibility,
        },
        args.mutation,
    ))
}

/// The flags a native list family serves; every other set flag is refused
/// rather than ignored, so a V1 predicate never silently widens a list.
fn refuse_unused(filters: &Filters, allowed: &[&str]) -> Result<()> {
    let present: [(&str, bool); 22] = [
        ("--claim", filters.claim.is_some()),
        ("--testament", filters.testament.is_some()),
        ("--source", filters.source.is_some()),
        ("--target", filters.target.is_some()),
        ("--status", filters.status.is_some()),
        ("--action", filters.action.is_some()),
        ("--producer", filters.producer.is_some()),
        ("--kind", filters.kind.is_some()),
        ("--schema-hash", filters.schema_hash.is_some()),
        ("--evaluator", filters.evaluator.is_some()),
        ("--phase", filters.phase.is_some()),
        ("--mode", filters.mode.is_some()),
        ("--scope", !filters.scopes.is_empty()),
        ("--relation", !filters.relations.is_empty()),
        ("--caused-by", filters.caused_by.is_some()),
        ("--input", !filters.inputs.is_empty()),
        ("--outcome", filters.outcome.is_some()),
        ("--confidence", filters.confidence.is_some()),
        ("--created-after", filters.created_after.is_some()),
        ("--created-through", filters.created_through.is_some()),
        ("--validation", filters.validation.is_some()),
        ("--verdict", filters.verdict.is_some()),
    ];
    for (flag, set) in present {
        if set && !allowed.contains(&flag) {
            return Err(CliError::Input(format!(
                "{flag} does not select this family on the native engine"
            )));
        }
    }
    if filters.holder.is_some() && !allowed.contains(&"--holder") {
        return Err(CliError::Input(
            "--holder does not select this family on the native engine".into(),
        ));
    }
    if filters.after.is_some() && !allowed.contains(&"--after") {
        return Err(CliError::Input(
            "--after does not select this family on the native engine".into(),
        ));
    }
    Ok(())
}
fn at_most_one<T: Clone>(values: &[T], flag: &str) -> Result<Option<T>> {
    if values.len() > 1 {
        return Err(CliError::Input(format!(
            "the native engine indexes one {flag} per list; narrow the rest afterwards"
        )));
    }
    Ok(values.first().cloned())
}
fn page(args: &ListArgs) -> NativeListPageDocument {
    NativeListPageDocument {
        cursor: args.cursor.clone(),
        limit: args.limit,
        max_visits: args.max_visits,
    }
}

/// One native list from the shared `list` flags. Claims index one scope or
/// one relation; the remaining families take their own predicates.
pub(super) fn list(command: ListCommand) -> Result<(NativeListOperation, ListArgs)> {
    let (operation, args) = match command {
        ListCommand::Claims(args) => {
            let filters = &args.filters;
            refuse_unused(
                filters,
                &[
                    "--source",
                    "--target",
                    "--status",
                    "--action",
                    "--scope",
                    "--relation",
                    "--created-after",
                ],
            )?;
            let scope = at_most_one(&filters.scopes, "--scope")?.map(|scope| NativeScopeDocument {
                kind: scope.kind,
                key: scope.key,
            });
            let relation = at_most_one(&filters.relations, "--relation")?.map(|relation| {
                NativeRelationDocument {
                    kind: relation.kind,
                    target: relation.target,
                }
            });
            let document = NativeClaimListDocument {
                issuer: filters.source.clone(),
                subject: filters.target.clone(),
                status: filters.status.clone(),
                action: filters.action.clone(),
                scope,
                relation,
                created_after: filters.created_after,
                page: page(&args),
            };
            (NativeListOperation::ClaimList(document), args)
        }
        ListCommand::Testaments(args) => {
            refuse_unused(&args.filters, &["--claim"])?;
            let document = NativeTestamentListDocument {
                claim: required(args.filters.claim.clone(), "--claim")?,
                page: page(&args),
            };
            (NativeListOperation::TestamentList(document), args)
        }
        ListCommand::Artifacts(args) => {
            let filters = &args.filters;
            refuse_unused(
                filters,
                &["--producer", "--kind", "--schema-hash", "--input"],
            )?;
            let input = at_most_one(&filters.inputs, "--input")?.map(|input| input.id);
            let document = NativeArtifactListDocument {
                producer: filters.producer.clone(),
                kind: filters.kind.clone(),
                schema: filters.schema_hash.clone(),
                input,
                page: page(&args),
            };
            (NativeListOperation::ArtifactList(document), args)
        }
        ListCommand::Validations(args) => {
            refuse_unused(&args.filters, &["--claim", "--evaluator"])?;
            let document = NativeValidationListDocument {
                claim: args.filters.claim.clone(),
                evaluator: args.filters.evaluator.clone(),
                page: page(&args),
            };
            (NativeListOperation::ValidationList(document), args)
        }
        ListCommand::Evaluations(args) => {
            refuse_unused(
                &args.filters,
                &["--claim", "--validation", "--evaluator", "--verdict"],
            )?;
            let document = NativeEvaluationListDocument {
                claim: args.filters.claim.clone(),
                validation: args.filters.validation.clone(),
                evaluator: args.filters.evaluator.clone(),
                verdict: args.filters.verdict.clone(),
                page: page(&args),
            };
            (NativeListOperation::EvaluationList(document), args)
        }
        ListCommand::Receipts(args) => {
            refuse_unused(&args.filters, &["--holder", "--claim"])?;
            let document = NativeReceiptListDocument {
                holder: args.filters.holder.clone(),
                claim: args.filters.claim.clone(),
                page: page(&args),
            };
            (NativeListOperation::ReceiptList(document), args)
        }
        ListCommand::Monitors(args) => {
            refuse_unused(&args.filters, &["--claim"])?;
            let document = NativeMonitorListDocument {
                claim: required(args.filters.claim.clone(), "--claim")?,
                page: page(&args),
            };
            (NativeListOperation::MonitorList(document), args)
        }
        ListCommand::Events(args) => {
            refuse_unused(&args.filters, &["--after"])?;
            let after = args
                .filters
                .after
                .as_deref()
                .map(|text| {
                    let (sequence, ordinal) = text
                        .split_once(':')
                        .ok_or_else(|| CliError::Input("--after is SEQUENCE:ORDINAL".into()))?;
                    Ok::<_, CliError>(NativeEventPositionDocument {
                        sequence: sequence
                            .parse()
                            .map_err(|_| CliError::Input("--after sequence".into()))?,
                        ordinal: ordinal
                            .parse()
                            .map_err(|_| CliError::Input("--after ordinal".into()))?,
                    })
                })
                .transpose()?;
            let document = NativeEventListDocument {
                after,
                page: page(&args),
            };
            (NativeListOperation::EventList(document), args)
        }
    };
    Ok((operation, args))
}
