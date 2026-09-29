use super::{CliError, Result, args::OutputFormat};
use focal_client::pending::{OperationJournal, OperationStage};
use focal_model::*;
use focal_wire::{ListPage, ReadObject, ReadToken, TraversalPage, TraversalStop};
use serde::Serialize;
use std::io::Write;

#[derive(Serialize)]
struct JournalOutput<'a> {
    schema_version: u16,
    operation: Option<&'a str>,
    operation_path_bytes: &'a [u8],
    stage: OperationStage,
    result: Option<serde_json::Value>,
    receipt: Option<&'a MutationReceipt>,
    epoch_receipt: Option<&'a MutationReceipt>,
    pending_request: Option<String>,
}

pub(super) fn hex(bytes: &[u8]) -> String {
    let mut result = String::new();
    for byte in bytes {
        use std::fmt::Write;
        // String's formatter is infallible; propagate nothing from this adapter.
        let _ = write!(&mut result, "{byte:02x}");
    }
    result
}
pub(super) fn structured(value: &impl Serialize, format: OutputFormat) -> Result<()> {
    let mut output = std::io::stdout().lock();
    structured_to(&mut output, value, format)?;
    output.flush()?;
    Ok(())
}
/// Serialize directly to the bounded page's sink. No intermediate JSON tree or
/// second whole-result string is required for YAML output.
pub(super) fn structured_to(
    mut output: impl Write,
    value: &impl Serialize,
    format: OutputFormat,
) -> Result<()> {
    match format {
        OutputFormat::Json => serde_json::to_writer_pretty(&mut output, value)
            .map_err(|error| CliError::Other(Box::new(error)))?,
        OutputFormat::Yaml => serde_saphyr::to_io_writer(&mut output, value)
            .map_err(|error| CliError::Other(Box::new(error)))?,
        OutputFormat::Table => {
            return Err(CliError::Input(
                "structured output requires json or yaml".into(),
            ));
        }
    }
    writeln!(output)?;
    Ok(())
}
#[derive(Serialize)]
struct UnconfirmedOutput<'a> {
    schema_version: u16,
    condition: &'a str,
    operation: Option<&'a str>,
    operation_path_bytes: &'a [u8],
    request: Option<String>,
    result: Option<&'a focal_wire::MutationReply>,
}
pub(super) fn unconfirmed(
    journal: &OperationJournal,
    condition: &str,
    reply: Option<&focal_wire::MutationReply>,
    format: OutputFormat,
) -> Result<()> {
    match format {
        OutputFormat::Json | OutputFormat::Yaml => structured(
            &UnconfirmedOutput {
                schema_version: 1,
                condition,
                operation: journal.path().to_str(),
                operation_path_bytes: journal.path().as_os_str().as_encoded_bytes(),
                request: journal
                    .next_request()?
                    .map(|request| request.request_id.to_string()),
                result: reply,
            },
            format,
        ),
        OutputFormat::Table => {
            let mut out = std::io::stdout().lock();
            writeln!(out, "CONDITION\t{condition}")?;
            writeln!(out, "OPERATION\t{:?}", journal.path())?;
            if let Some(reply) = reply {
                writeln!(out, "RESULT\t{reply:?}")?;
            }
            Ok(())
        }
    }
}
pub(super) fn journal(journal: &OperationJournal, format: OutputFormat) -> Result<()> {
    let result = journal
        .receipt()
        .map(|receipt| command_result(&receipt.outcome));
    match format {
        OutputFormat::Json | OutputFormat::Yaml => structured(
            &JournalOutput {
                schema_version: 1,
                operation: journal.path().to_str(),
                operation_path_bytes: journal.path().as_os_str().as_encoded_bytes(),
                stage: journal.stage(),
                result,
                receipt: journal.receipt(),
                epoch_receipt: journal.epoch_receipt(),
                pending_request: journal
                    .next_request()?
                    .map(|request| request.request_id.to_string()),
            },
            format,
        ),
        OutputFormat::Table => {
            let mut out = std::io::stdout().lock();
            writeln!(out, "OPERATION\t{:?}", journal.path())?;
            writeln!(out, "STAGE\t{:?}", journal.stage())?;
            if let Some(receipt) = journal.receipt() {
                writeln!(out, "REQUEST\t{}", receipt.key.id)?;
                writeln!(out, "SEQUENCE\t{}", receipt.sequence.0)?;
            }
            if let Some(result) = result {
                writeln!(out, "RESULT\t{result}")?;
            }
            Ok(())
        }
    }
}
pub(super) fn command_result(outcome: &CommandResult) -> serde_json::Value {
    match outcome {
        CommandResult::Generated(ids) | CommandResult::Existing(ids) => {
            serde_json::json!({"claims":ids.iter().map(ToString::to_string).collect::<Vec<_>>()})
        }
        CommandResult::Claim { claim, status } => {
            serde_json::json!({"claim":claim.to_string(),"status":status})
        }
        CommandResult::Receipt { claim, fence } => {
            serde_json::json!({"claim":claim.to_string(),"receipt":fence.receipt.to_string(),"receipt_epoch":fence.epoch})
        }
        CommandResult::Monitor(id) => serde_json::json!({"monitor":id.to_string()}),
        CommandResult::EvidenceSet(id) => serde_json::json!({"evidence_set":id.to_string()}),
        CommandResult::Artifact(reference) => {
            serde_json::json!({"artifact":reference.id.to_string(),"hash":reference.hash.to_string()})
        }
        CommandResult::Testament(id) => serde_json::json!({"testament":id.to_string()}),
        other => serde_json::json!({"outcome":other}),
    }
}
pub(super) fn key(object: &ReadObject) -> (ObjectKind, ObjectId) {
    match object {
        ReadObject::Claim { id, .. } => (ObjectKind::Claim, ObjectId(id.0)),
        ReadObject::Testament { id, .. } => (ObjectKind::Testament, ObjectId(id.0)),
        ReadObject::Artifact { id, .. } => (ObjectKind::Artifact, ObjectId(id.0)),
        ReadObject::Validation { id, .. } | ReadObject::ValidationResults { id, .. } => {
            (ObjectKind::Validation, ObjectId(id.0))
        }
    }
}
fn authored(text: &str) -> String {
    // JSON escaping prevents authored control bytes becoming terminal commands.
    serde_json::to_string(text).unwrap_or_else(|_| "\"unrenderable text\"".into())
}
fn row(out: &mut impl Write, object: &ReadObject) -> Result<()> {
    match object {
        ReadObject::Claim { id, value } => writeln!(
            out,
            "claim\t{id}\t{:?}\t{}",
            value.lifecycle().status,
            authored(&value.content().description)
        )?,
        ReadObject::Testament { id, value } => writeln!(
            out,
            "testament\t{id}\t{:?}\t{}",
            value.content().outcome,
            authored(&value.content().summary)
        )?,
        ReadObject::Artifact { id, value } => writeln!(
            out,
            "artifact\t{id}\t{}\t{}",
            value.content_hash(),
            authored(&value.content().kind)
        )?,
        ReadObject::Validation { id, value, .. }
        | ReadObject::ValidationResults { id, value, .. } => writeln!(
            out,
            "validation\t{id}\t{:?}/{:?}\t{}",
            value.content().kind,
            value.content().mode,
            authored(&value.content().description)
        )?,
    }
    Ok(())
}
#[derive(Serialize)]
struct ObjectOutput<'a> {
    kind: ObjectKind,
    id: String,
    object: &'a ReadObject,
}
fn document(object: &ReadObject) -> ObjectOutput<'_> {
    let (kind, id) = key(object);
    ObjectOutput {
        kind,
        id: id.to_string(),
        object,
    }
}
#[derive(Serialize)]
struct GetOutput<'a> {
    schema_version: u16,
    token: ReadToken,
    result: ObjectOutput<'a>,
    cursor: Option<String>,
}
#[derive(Serialize)]
struct PageOutput<'a> {
    schema_version: u16,
    token: ReadToken,
    results: Vec<ObjectOutput<'a>>,
    cursor: Option<String>,
    visited: u32,
}
pub(super) fn object(token: ReadToken, object: &ReadObject, format: OutputFormat) -> Result<()> {
    let cursor = match object {
        ReadObject::ValidationResults {
            id,
            next: Some(after),
            ..
        } => Some(super::reads::validation_cursor(token, *id, *after)?),
        _ => None,
    };
    match format {
        OutputFormat::Json | OutputFormat::Yaml => structured(
            &GetOutput {
                schema_version: 1,
                token,
                result: document(object),
                cursor,
            },
            format,
        ),
        OutputFormat::Table => {
            let mut out = std::io::stdout().lock();
            writeln!(out, "KIND\tID\tSTATE/HASH\tDESCRIPTION")?;
            row(&mut out, object)?;
            if let ReadObject::ValidationResults { records, .. } = object {
                for record in records {
                    match &record.value {
                        ValidationResultValue::Run(run) => writeln!(
                            out,
                            "RUN\tepoch={}\ttarget={}\tfinal={:?}",
                            record.position.run.epoch,
                            record.position.run.target_hash,
                            run.final_verdict
                        )?,
                        ValidationResultValue::Attempt(verdict) => writeln!(
                            out,
                            "ATTEMPT\tepoch={}\thandler={}\tattempt={}\tverdict={:?}\tevidence={}",
                            verdict.run.epoch,
                            verdict.handler.id,
                            verdict.attempt,
                            verdict.value,
                            verdict.evidence.len()
                        )?,
                    }
                }
            }
            writeln!(out, "SEQUENCE\t{}", token.sequence.0)?;
            if let Some(cursor) = cursor {
                writeln!(out, "CURSOR\t{cursor}")?;
            }
            Ok(())
        }
    }
}
pub(super) fn validation_context(
    context: &focal_client::validation_context::ValidationContext,
    format: OutputFormat,
) -> Result<()> {
    let cursor = context
        .next
        .map(|after| super::reads::validation_cursor(context.token, context.validation_id, after))
        .transpose()?;
    match format {
        OutputFormat::Json | OutputFormat::Yaml => {
            #[derive(Serialize)]
            struct ContextOutput<'a> {
                schema_version: u16,
                context: &'a focal_client::validation_context::ValidationContext,
                cursor: Option<String>,
            }
            structured(
                &ContextOutput {
                    schema_version: 1,
                    context,
                    cursor,
                },
                format,
            )
        }
        OutputFormat::Table => {
            let mut out = std::io::stdout().lock();
            let requirement = context.validation.content();
            writeln!(out, "VALIDATION\t{}", context.validation_id)?;
            writeln!(out, "DESCRIPTION\t{}", authored(&requirement.description))?;
            writeln!(
                out,
                "REQUIREMENT\t{:?}/{:?}/{:?}",
                requirement.kind, requirement.phase, requirement.mode
            )?;
            writeln!(out, "EVALUATOR\t{}", requirement.evaluator)?;
            if let Some(bar) = &requirement.quality_bar {
                writeln!(out, "QUALITY BAR\t{}", authored(bar))?;
            }
            for handler in &requirement.handlers {
                writeln!(
                    out,
                    "HANDLER\t{}\tversion={}\tagentic={}",
                    handler.id, handler.version, handler.agentic
                )?;
            }
            writeln!(
                out,
                "CLAIM\t{}\t{:?}\t{}",
                requirement.claim,
                context.claim.lifecycle().status,
                authored(&context.claim.content().description)
            )?;
            if let Some(receipt) = &context.claim.lifecycle().receipt {
                writeln!(
                    out,
                    "CLAIM RECEIPT\t{}\tepoch={}\tholder={}",
                    receipt.fence.receipt, receipt.fence.epoch, receipt.holder
                )?;
            }
            match &context.testament {
                Some(testament) => {
                    writeln!(
                        out,
                        "CURRENT TESTAMENT\t{}\toutcome={:?}\t{}",
                        testament.id,
                        testament.value.content().outcome,
                        authored(&testament.value.content().summary)
                    )?;
                    writeln!(
                        out,
                        "TESTAMENT GENERATED\tsequence={}",
                        testament.value.lifecycle().created.0
                    )?;
                    let receipt = testament.value.content().receipt;
                    writeln!(
                        out,
                        "TESTAMENT RECEIPT\t{}\tepoch={}",
                        receipt.receipt, receipt.epoch
                    )?;
                    match testament.value.lifecycle().acknowledged {
                        Some(sequence) => {
                            writeln!(out, "TESTAMENT ACKNOWLEDGED\tsequence={}", sequence.0)?
                        }
                        None => writeln!(out, "TESTAMENT ACKNOWLEDGED\tnot recorded")?,
                    }
                    for artifact in &testament.value.content().artifacts {
                        writeln!(out, "MANIFEST ARTIFACT\t{}\t{}", artifact.id, artifact.hash)?;
                    }
                }
                None => writeln!(out, "CURRENT TESTAMENT\tnone")?,
            }
            writeln!(
                out,
                "RECORDED RESULTS\t{} records on this page; may refer to earlier evidence",
                context.records.len()
            )?;
            for record in &context.records {
                match &record.value {
                    ValidationResultValue::Run(run) => writeln!(
                        out,
                        "RUN\tepoch={}\tphase={:?}\ttarget={}\tfinal={:?}",
                        record.position.run.epoch,
                        record.position.run.phase,
                        record.position.run.target_hash,
                        run.final_verdict
                    )?,
                    ValidationResultValue::Attempt(verdict) => writeln!(
                        out,
                        "ATTEMPT\tepoch={}\thandler={}\tattempt={}\tverdict={:?}",
                        verdict.run.epoch, verdict.handler.id, verdict.attempt, verdict.value
                    )?,
                }
            }
            writeln!(out, "SEQUENCE\t{}", context.token.sequence.0)?;
            if let Some(cursor) = cursor {
                writeln!(out, "CURSOR\t{cursor}")?;
            }
            out.flush()?;
            Ok(())
        }
    }
}
pub(super) fn page(page: ListPage, format: OutputFormat) -> Result<()> {
    let cursor = page.next.as_ref().map(|cursor| hex(&cursor.bytes));
    match format {
        OutputFormat::Json | OutputFormat::Yaml => structured(
            &PageOutput {
                schema_version: 1,
                token: page.token,
                results: page.objects.iter().map(document).collect(),
                cursor,
                visited: page.visited,
            },
            format,
        ),
        OutputFormat::Table => {
            let mut out = std::io::stdout().lock();
            writeln!(out, "KIND\tID\tSTATE/HASH\tDESCRIPTION")?;
            for object in &page.objects {
                row(&mut out, object)?;
            }
            writeln!(
                out,
                "SEQUENCE\t{}\tVISITED\t{}",
                page.token.sequence.0, page.visited
            )?;
            if let Some(cursor) = cursor {
                writeln!(out, "CURSOR\t{cursor}")?;
            }
            Ok(())
        }
    }
}

/// Same page fields as the one-page command, serialized from borrowed rows.
/// The caller bounds the byte buffer and flushes it before following `next`.
pub(super) fn page_stream_to(
    page: &ListPage,
    mut out: impl Write,
    format: OutputFormat,
) -> Result<()> {
    struct Documents<'a>(&'a [ReadObject]);
    impl Serialize for Documents<'_> {
        fn serialize<S: serde::Serializer>(
            &self,
            serializer: S,
        ) -> std::result::Result<S::Ok, S::Error> {
            use serde::ser::SerializeSeq;
            let mut rows = serializer.serialize_seq(Some(self.0.len()))?;
            for object in self.0 {
                rows.serialize_element(&document(object))?;
            }
            rows.end()
        }
    }
    #[derive(Serialize)]
    struct Page<'a> {
        schema_version: u16,
        token: ReadToken,
        results: Documents<'a>,
        cursor: Option<String>,
        visited: u32,
    }
    let cursor = page.next.as_ref().map(|cursor| hex(&cursor.bytes));
    match format {
        OutputFormat::Table => {
            writeln!(out, "KIND\tID\tSTATE/HASH\tDESCRIPTION")?;
            for object in &page.objects {
                row(&mut out, object)?;
            }
            writeln!(
                out,
                "SEQUENCE\t{}\tVISITED\t{}",
                page.token.sequence.0, page.visited
            )?;
            if let Some(cursor) = cursor {
                writeln!(out, "CURSOR\t{cursor}")?;
            }
        }
        OutputFormat::Json => {
            serde_json::to_writer(
                &mut out,
                &Page {
                    schema_version: 1,
                    token: page.token,
                    results: Documents(&page.objects),
                    cursor,
                    visited: page.visited,
                },
            )
            .map_err(|error| CliError::Other(Box::new(error)))?;
            writeln!(out)?;
        }
        OutputFormat::Yaml => {
            writeln!(out, "---")?;
            structured_to(
                &mut out,
                &Page {
                    schema_version: 1,
                    token: page.token,
                    results: Documents(&page.objects),
                    cursor,
                    visited: page.visited,
                },
                format,
            )?;
        }
    }
    Ok(())
}

pub(super) fn traversal(page: TraversalPage, format: OutputFormat) -> Result<()> {
    let cursor = page.next.as_ref().map(|cursor| hex(&cursor.bytes));
    match format {
        OutputFormat::Json | OutputFormat::Yaml => {
            #[derive(Serialize)]
            struct Output<'a> {
                schema_version: u16,
                token: ReadToken,
                results: Vec<ObjectOutput<'a>>,
                cursor: Option<String>,
                stop: TraversalStop,
                visited: u32,
                total_visits: u32,
            }
            structured(
                &Output {
                    schema_version: 1,
                    token: page.token,
                    results: page.objects.iter().map(document).collect(),
                    cursor,
                    stop: page.stop,
                    visited: page.visited,
                    total_visits: page.total_visits,
                },
                format,
            )
        }
        OutputFormat::Table => {
            let mut out = std::io::stdout().lock();
            writeln!(out, "KIND\tID\tSTATE/HASH\tDESCRIPTION")?;
            for object in &page.objects {
                row(&mut out, object)?;
            }
            writeln!(
                out,
                "SEQUENCE\t{}\tSTOP\t{:?}\tVISITED\t{}\tTOTAL_VISITS\t{}",
                page.token.sequence.0, page.stop, page.visited, page.total_visits
            )?;
            if let Some(cursor) = cursor {
                writeln!(out, "CURSOR\t{cursor}")?;
            }
            out.flush()?;
            Ok(())
        }
    }
}

// The only output test builds a non-UTF8 path from raw bytes (OsStringExt),
// which is a Unix-only construction; Windows paths are UTF-16 and cannot carry
// arbitrary bytes, so the non-UTF8 output boundary is exercised on Unix.
#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::{ffi::OsString, os::unix::ffi::OsStringExt, path::PathBuf};

    #[test]
    fn non_utf8_paths_remain_exact_in_pending_and_inspection_json_without_filesystem_support() {
        // macOS cannot create this filename. Test the shared output boundary
        // independently; Linux additionally exercises an actual journal path.
        let path = PathBuf::from(OsString::from_vec(b"/private/operation \xff name".to_vec()));
        let request = RequestId::from_u128(42).to_string();
        let inspected = JournalOutput {
            schema_version: 1,
            operation: path.to_str(),
            operation_path_bytes: path.as_os_str().as_encoded_bytes(),
            stage: OperationStage::OpenEpoch,
            result: None,
            receipt: None,
            epoch_receipt: None,
            pending_request: Some(request.clone()),
        };
        let pending = UnconfirmedOutput {
            schema_version: 1,
            condition: "OutcomeUnknown",
            operation: path.to_str(),
            operation_path_bytes: path.as_os_str().as_encoded_bytes(),
            request: Some(request.clone()),
            result: None,
        };
        for (bytes, key) in [
            (serde_json::to_vec(&inspected).unwrap(), "pending_request"),
            (serde_json::to_vec(&pending).unwrap(), "request"),
        ] {
            let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert!(value["operation"].is_null());
            let recovered: Vec<u8> =
                serde_json::from_value(value["operation_path_bytes"].clone()).unwrap();
            assert_eq!(recovered, path.as_os_str().as_encoded_bytes());
            assert_eq!(value[key], request);
            assert_eq!(value["schema_version"], 1);
        }
    }
}
