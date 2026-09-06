use super::{CliError, Result, args::*};
use focal_client::input::*;
use serde::de::DeserializeOwned;
use std::{io::Read, path::Path};

pub(super) fn required<T>(value: Option<T>, field: &'static str) -> Result<T> {
    value.ok_or_else(|| CliError::Input(format!("missing required field: {field}")))
}
fn input_format(path: &Path, explicit: Option<Format>) -> Result<InputFormat> {
    if let Some(explicit) = explicit {
        return Ok(match explicit {
            Format::Json => InputFormat::Json,
            Format::Yaml => InputFormat::Yaml,
        });
    }
    match path.extension().and_then(|ext| ext.to_str()) {
        Some("json") => Ok(InputFormat::Json),
        Some("yaml" | "yml") => Ok(InputFormat::Yaml),
        _ => Err(CliError::Input(
            "select --input-format json|yaml for stdin or an unrecognized file extension".into(),
        )),
    }
}
pub(super) fn read_bytes(path: &Path, max: usize) -> Result<Vec<u8>> {
    if path != Path::new("-") {
        return super::super::read_file(path, max).map_err(CliError::Other);
    }
    let limit = u64::try_from(max)
        .ok()
        .and_then(|max| max.checked_add(1))
        .ok_or_else(|| CliError::Input("input bound overflow".into()))?;
    let mut bytes = Vec::new();
    std::io::stdin()
        .lock()
        .take(limit)
        .read_to_end(&mut bytes)?;
    if bytes.len() > max {
        return Err(CliError::Input("input exceeds its byte budget".into()));
    }
    Ok(bytes)
}
pub(super) fn read_document<T: DeserializeOwned>(
    path: &Path,
    explicit: Option<Format>,
) -> Result<T> {
    let format = input_format(path, explicit)?;
    Ok(parse_document(&read_bytes(path, MAX_INPUT_BYTES)?, format)?)
}
impl DocumentInput {
    pub(super) fn load<T: DeserializeOwned>(self, field_flags: bool) -> Result<Option<T>> {
        let supplied = self.json.is_some() || self.yaml.is_some() || self.file.is_some();
        if supplied && field_flags {
            return Err(CliError::Input(
                "choose authored field flags or one JSON/YAML/file document; content is not merged"
                    .into(),
            ));
        }
        if let Some(json) = self.json {
            return Ok(Some(parse_document(json.as_bytes(), InputFormat::Json)?));
        }
        if let Some(yaml) = self.yaml {
            return Ok(Some(parse_document(yaml.as_bytes(), InputFormat::Yaml)?));
        }
        self.file
            .map(|path| read_document(&path, self.input_format))
            .transpose()
    }
}
pub(super) fn claim(args: ClaimArgs) -> Result<(ClaimDocument, MutationOptions)> {
    let fields = args.id.is_some()
        || args.occurrence.is_some()
        || args.description.is_some()
        || args.target.is_some()
        || args.action.is_some()
        || !args.scope.is_empty()
        || !args.relation.is_empty()
        || !args.validation_file.is_empty()
        || !args.validation_json.is_empty()
        || args.deadline_json.is_some();
    if let Some(document) = args.input.load(fields)? {
        return Ok((document, args.mutation));
    }
    let count = args
        .validation_file
        .len()
        .checked_add(args.validation_json.len())
        .ok_or_else(|| CliError::Input("validation count overflow".into()))?;
    if count > 64 || args.scope.len() > 256 || args.relation.len() > 256 {
        return Err(CliError::Input(
            "too many validation definitions, scopes or relations".into(),
        ));
    }
    let mut validations = Vec::new();
    let mut total_bytes = 0usize;
    for path in args.validation_file {
        let bytes = read_bytes(&path, MAX_INPUT_BYTES)?;
        charge_input(&mut total_bytes, bytes.len())?;
        validations.push(parse_document(&bytes, input_format(&path, None)?)?);
    }
    for json in args.validation_json {
        charge_input(&mut total_bytes, json.len())?;
        validations.push(parse_document(json.as_bytes(), InputFormat::Json)?);
    }
    let scopes = args
        .scope
        .into_iter()
        .map(|value| {
            let (kind, key) = value
                .split_once(':')
                .ok_or_else(|| CliError::Input("scope must be KIND:KEY".into()))?;
            Ok(ScopeDocument {
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
            Ok(ClaimRelationDocument {
                kind: kind.into(),
                target: target.into(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((
        ClaimDocument {
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
        },
        args.mutation,
    ))
}
pub(super) fn claim_batch(args: ClaimBatchArgs) -> Result<(ClaimBatchDocument, MutationOptions)> {
    let fields = !args.claim_json.is_empty() || !args.claim_file.is_empty();
    if let Some(document) = args.input.load(fields)? {
        return Ok((document, args.mutation));
    }
    let count = args
        .claim_json
        .len()
        .checked_add(args.claim_file.len())
        .ok_or(InputError::Capacity)?;
    if count == 0 || count > 64 {
        return Err(InputError::Invalid("claim batch must contain 1..64 claims").into());
    }
    let mut claims = Vec::new();
    let mut bytes = 0usize;
    claims
        .try_reserve_exact(count)
        .map_err(|_| InputError::Capacity)?;
    for path in args.claim_file {
        let input = read_bytes(&path, MAX_INPUT_BYTES)?;
        charge_input(&mut bytes, input.len())?;
        claims.push(parse_document(&input, input_format(&path, None)?)?);
    }
    for input in args.claim_json {
        charge_input(&mut bytes, input.len())?;
        claims.push(parse_document(input.as_bytes(), InputFormat::Json)?);
    }
    Ok((ClaimBatchDocument { claims }, args.mutation))
}
fn charge_input(total: &mut usize, additional: usize) -> Result<()> {
    *total = total
        .checked_add(additional)
        .filter(|total| *total <= MAX_INPUT_BYTES)
        .ok_or_else(|| CliError::Input("combined authored input exceeds its byte budget".into()))?;
    Ok(())
}
pub(super) fn testament(args: TestamentArgs) -> Result<(TestamentDocument, MutationOptions)> {
    let fields = args.id.is_some()
        || args.claim.is_some()
        || args.receipt.is_some()
        || args.receipt_epoch.is_some()
        || args.evidence_set.is_some()
        || !args.artifact.is_empty()
        || args.manifest_file.is_some()
        || args.summary.is_some()
        || args.confidence.is_some()
        || args.outcome.is_some();
    if let Some(document) = args.input.load(fields)? {
        return Ok((document, args.mutation));
    }
    let manifest = match args.manifest_file {
        Some(path) => read_document(&path, None)?,
        None => {
            if args.artifact.len() > 1024 {
                return Err(CliError::Input(
                    "artifact manifest exceeds its item budget".into(),
                ));
            }
            args.artifact
                .into_iter()
                .map(|value| {
                    let (id, hash) = value.split_once(':').ok_or_else(|| {
                        CliError::Input("artifact must be ID:DESCRIPTOR_HASH".into())
                    })?;
                    Ok(ArtifactReferenceDocument {
                        id: id.into(),
                        hash: hash.into(),
                    })
                })
                .collect::<Result<Vec<_>>>()?
        }
    };
    Ok((
        TestamentDocument {
            id: args.id,
            claim: required(args.claim, "claim")?,
            receipt: ReceiptDocument {
                id: required(args.receipt, "receipt")?,
                epoch: required(args.receipt_epoch, "receipt-epoch")?,
            },
            evidence_set: required(args.evidence_set, "evidence-set")?,
            manifest,
            summary: required(args.summary, "summary")?,
            confidence: required(args.confidence, "confidence")?,
            outcome: required(args.outcome, "outcome")?,
        },
        args.mutation,
    ))
}
pub(super) fn artifact(args: ArtifactArgs) -> Result<(ArtifactDocument, MutationOptions)> {
    let fields = args.id.is_some()
        || args.claim.is_some()
        || args.receipt.is_some()
        || args.receipt_epoch.is_some()
        || args.evidence_set.is_some()
        || args.kind.is_some()
        || args.schema_hash.is_some()
        || args.payload_file.is_some()
        || args.text.is_some()
        || args.metadata_file.is_some();
    if let Some(document) = args.input.load(fields)? {
        return Ok((document, args.mutation));
    }
    let payload = match (args.payload_file, args.text) {
        (Some(path), None) => PayloadDocument::Inline {
            bytes: read_bytes(&path, 16 * 1024)?,
        },
        (None, Some(text)) => PayloadDocument::Text { text },
        _ => return Err(CliError::Input("provide --payload-file or --text".into())),
    };
    Ok((
        ArtifactDocument {
            id: args.id,
            claim: required(args.claim, "claim")?,
            receipt: ReceiptDocument {
                id: required(args.receipt, "receipt")?,
                epoch: required(args.receipt_epoch, "receipt-epoch")?,
            },
            evidence_set: required(args.evidence_set, "evidence-set")?,
            kind: required(args.kind, "kind")?,
            schema_hash: required(args.schema_hash, "schema-hash")?,
            metadata: args
                .metadata_file
                .map(|path| read_bytes(&path, 16 * 1024))
                .transpose()?
                .unwrap_or_default(),
            payload,
            inputs: Vec::new(),
            visibility: Vec::new(),
        },
        args.mutation,
    ))
}
