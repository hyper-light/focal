//! Read-only recovery queries. Synchronous journal ownership stays on the CLI
//! thread while the client obtains a fresh owner quorum barrier.
use super::*;
use serde::Serialize;

fn fetch(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    query: ReconcileQuery,
) -> Result<ReconcileReply> {
    let request = context.envelope(Operation::Reconcile(query))?;
    Ok(runtime.block_on(
        context
            .client
            .reconcile(request, context.operation.principal),
    )?)
}

pub(super) fn query(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    query: ReconcileQuery,
    format: OutputFormat,
) -> Result<()> {
    render(&fetch(runtime, context, query)?, format)
}

pub(super) fn inspect(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    journal: &OperationJournal,
    format: OutputFormat,
) -> Result<()> {
    let request = journal.business_request()?;
    let reply = fetch(
        runtime,
        context,
        ReconcileQuery::Receipt {
            epoch: request.request_epoch,
            request: request.request_id,
        },
    )?;
    if let ReconcileResult::Receipt { resolution, .. } = &reply.page.result {
        match resolution {
            ReceiptResolution::Committed(receipt) => journal.validate_business_receipt(receipt)?,
            ReceiptResolution::CommittedCursor(_) => {
                return Err(PendingError::ReceiptMismatch.into());
            }
            ReceiptResolution::BelowFloor { .. } | ReceiptResolution::Unknown => {}
        }
    }
    render(&reply, format)
}

#[derive(Serialize)]
struct Output<'a> {
    schema_version: u16,
    reply: &'a ReconcileReply,
}
fn render(reply: &ReconcileReply, format: OutputFormat) -> Result<()> {
    if matches!(format, OutputFormat::Json) {
        return super::super::print_json(&Output {
            schema_version: 1,
            reply,
        })
        .map_err(CliError::Other);
    }
    let mut out = std::io::stdout().lock();
    writeln!(out, "PRINCIPAL\t{}", reply.page.principal)?;
    writeln!(out, "SEQUENCE\t{}", reply.page.sequence.0)?;
    writeln!(out, "APPLIED_INDEX\t{}", reply.applied_index)?;
    let epoch = match &reply.page.result {
        ReconcileResult::Epoch(epoch) => epoch,
        ReconcileResult::Receipt {
            key,
            epoch,
            resolution,
        } => {
            writeln!(out, "REQUEST\t{}", key.id)?;
            match resolution {
                ReceiptResolution::Committed(receipt) => {
                    writeln!(out, "RESOLUTION\tCOMMITTED")?;
                    writeln!(out, "COMMIT_SEQUENCE\t{}", receipt.sequence.0)?;
                    writeln!(out, "COMMAND_HASH\t{}", receipt.command_hash)?;
                }
                ReceiptResolution::CommittedCursor(receipt) => {
                    writeln!(out, "RESOLUTION\tCOMMITTED_CURSOR")?;
                    writeln!(out, "CURSOR_REVISION\t{}", receipt.revision)?;
                    writeln!(out, "COMMIT_APPLIED_INDEX\t{}", receipt.raft_index)?;
                    writeln!(out, "COMMIT_SEQUENCE\t{}", receipt.domain_sequence.0)?;
                    writeln!(out, "INTENT_HASH\t{}", receipt.intent_hash)?;
                    writeln!(out, "HISTORY_FLOOR\t{}", receipt.floor.0)?;
                    if let Some(record) = &receipt.record {
                        writeln!(out, "CONSUMER\t{}", output::hex(&record.token.key.consumer))?;
                        writeln!(out, "GENERATION\t{}", record.token.generation)?;
                        writeln!(
                            out,
                            "POSITION\t{}\t{:?}",
                            record.token.position.sequence.0, record.token.position.offset
                        )?;
                        writeln!(out, "MODE\t{:?}", record.mode)?;
                    }
                }
                ReceiptResolution::BelowFloor { .. } => {
                    writeln!(out, "RESOLUTION\tBELOW_FLOOR")?;
                    writeln!(out, "HISTORICAL_OUTCOME\tUNKNOWN")?;
                }
                ReceiptResolution::Unknown => writeln!(out, "RESOLUTION\tUNKNOWN")?,
            }
            epoch
        }
    };
    writeln!(
        out,
        "EPOCH\t{}\tADMITTED\t{}",
        epoch.epoch.0, epoch.admitted
    )?;
    if let Some(minimum) = epoch.minimum {
        writeln!(out, "MINIMUM_EPOCH\t{}", minimum.0)?;
    }
    if let Some(latest) = epoch.latest_admitted {
        writeln!(out, "LATEST_ADMITTED_EPOCH\t{}", latest.0)?;
    }
    Ok(())
}
