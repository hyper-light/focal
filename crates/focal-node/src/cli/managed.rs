//! Human adapter over the shared blocking request coordinator. Network waits
//! borrow it; output delivery and history retirement remain separate steps.
use super::*;
use focal_client::{
    managed_requests::{ManagedRequests, ManagedRequestsError},
    managed_store::{
        ManagedOperationId, ManagedOperationStore, ManagedStoreError, ManagedStoreLimits,
    },
    operation_store::{OperationIntent, StoreError},
    operations::{AuthoredOperation, PlannedOperation},
};
use serde::Serialize;

const CLIENTS: [&str; 2] = ["CLI.requests", "MCP.requests"];
fn other(error: impl std::error::Error + Send + Sync + 'static) -> CliError {
    CliError::Other(Box::new(error))
}
fn ownership_error(context: &Context, error: ManagedRequestsError) -> CliError {
    if matches!(error, ManagedRequestsError::Store(StoreError::Permissions)) {
        return std::io::Error::new(std::io::ErrorKind::PermissionDenied,
            format!("managed requests require an owner-private data directory (mode 0700) and request files (mode 0600); check permissions in {:?}", context.root)).into();
    }
    other(error)
}
fn open(context: &Context) -> Result<ManagedRequests> {
    ManagedRequests::open(
        &context.root,
        "CLI.requests",
        context.operation,
        ManagedStoreLimits::default(),
    )
    .map_err(|error| ownership_error(context, error))
}
fn existing(context: &Context, name: &str) -> Result<Option<ManagedRequests>> {
    match ManagedRequests::open_existing(
        &context.root,
        name,
        context.operation,
        ManagedStoreLimits::default(),
    ) {
        Ok(value) => Ok(Some(value)),
        Err(ManagedRequestsError::Missing) => Ok(None),
        Err(error) => Err(ownership_error(context, error)),
    }
}
fn find(context: &Context, id: ManagedOperationId) -> Result<ManagedRequests> {
    let key = id.key(context.operation);
    let mut retired = None;
    for name in CLIENTS {
        if let Some(requests) = existing(context, name)? {
            let store = match requests.store() {
                Ok(store) => store,
                Err(ManagedRequestsError::NotReady) => continue,
                Err(error) => return Err(other(error)),
            };
            let state = store.status()?;
            if state.stream == key.stream {
                return Ok(requests);
            }
            if state.stream.slot == key.stream.slot
                && state.stream.generation > key.stream.generation
            {
                retired = Some(requests);
            }
        }
    }
    retired.ok_or_else(|| ManagedStoreError::Missing.into())
}
fn maintain(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    requests: &ManagedRequests,
) -> Result<()> {
    // Slot discovery and exact pending controls are bounded by the coordinator;
    // cap total exchanges as an additional foreground operation budget.
    for _ in 0..4096 {
        let Some(request) = requests.maintenance(&mut random_id).map_err(other)? else {
            return Ok(());
        };
        let reply = match runtime.block_on(context.client.request(request.clone())) {
            Ok(reply) => reply,
            // This typed rejection is not a committed fact. The coordinator
            // may schedule an authenticated fresh slot read after a CAS race.
            Err(ClientError::Access(error)) => request.reply(Response::Error(error)),
            Err(error) => return Err(error.into()),
        };
        match requests.accept_maintenance(&request, reply) {
            Ok(()) | Err(ManagedRequestsError::Stale) => {}
            Err(error) => return Err(other(error)),
        }
    }
    Err(CliError::Input(
        "request ownership maintenance exceeded its bounded exchange budget".into(),
    ))
}
#[derive(Clone, Copy)]
enum Recovery {
    Retry,
    Reserved,
    Seal,
}
fn recovery(context: &Context, id: ManagedOperationId, action: Recovery) {
    // Diagnostics are best effort. They cannot gate business submission or
    // replace the error that left this exact operation unresolved.
    let mut error = std::io::stderr().lock();
    let command = context.invocation.as_deref().unwrap_or_else(|| {
        let _ = writeln!(
            error,
            "Recovery: keep your original --data-dir and --client-context."
        );
        "focal"
    });
    match action {
        Recovery::Retry => {
            let _ = writeln!(
                error,
                "Recovery: {command} request retry --operation-id {id}"
            );
        }
        Recovery::Seal => {
            let _ = writeln!(
                error,
                "Recovery: {command} request seal --operation-id {id}"
            );
        }
        Recovery::Reserved => {
            let _ = writeln!(
                error,
                "Saved reservation: {id}\nRecovery: {command} request pending"
            );
            let _ = writeln!(
                error,
                "To abandon this request: {command} request seal --operation-id {id}"
            );
        }
    }
}
fn recovering<T>(
    context: &Context,
    id: ManagedOperationId,
    action: Recovery,
    result: Result<T>,
) -> Result<T> {
    result.inspect_err(|error| match error {
        CliError::Managed(ManagedStoreError::Retired | ManagedStoreError::Missing) => {}
        CliError::Managed(ManagedStoreError::Unprepared) => {
            recovery(context, id, Recovery::Reserved)
        }
        _ => recovery(context, id, action),
    })
}
fn deferred(error: &CliError) {
    // The already-flushed business success is authoritative. A broken diagnostic
    // stream cannot repaint it as failed; the coordinator retains exact cleanup.
    let _ = writeln!(
        std::io::stderr().lock(),
        "Receipt cleanup deferred: {error}. Saved recovery remains available."
    );
}
fn delivered(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    requests: &ManagedRequests,
    id: ManagedOperationId,
) {
    if let Err(error) = requests
        .mark_delivered(id)
        .map_err(other)
        .and_then(|_| maintain(runtime, context, requests))
    {
        deferred(&error);
    }
}

pub(super) fn submit(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    authored: AuthoredOperation,
    options: MutationOptions,
) -> Result<()> {
    authored.preflight(&context.build)?;
    let revision = options.expected_revision.map(ObjectRevision);
    let canonical = authored.canonical_mutation_intent(revision)?;
    let descriptor = authored.descriptor();
    let (requests, id) = if let Some(id) = options.operation_id {
        let id = id.parse::<ManagedOperationId>()?;
        (find(context, id)?, id)
    } else {
        let requests = open(context)?;
        maintain(runtime, context, &requests)?;
        let id = requests
            .store()
            .map_err(other)?
            .reserve(RequestId(random_id()?))?;
        (requests, id)
    };
    let store = recovering(
        context,
        id,
        Recovery::Reserved,
        requests.store().map_err(other),
    )?;
    // Resolve only a fresh expansion. A retry uses the exact saved revision,
    // even when the claim has advanced since the first attempt.
    let revision = if revision.is_none() {
        if let Some(claim) = authored.revision_claim()? {
            match store.request(id) {
                Ok(prepared) => match prepared.request.operation {
                    Operation::Managed {
                        operation:
                            ManagedOperation::Submit {
                                expected_revision, ..
                            },
                        ..
                    } => expected_revision,
                    _ => return Err(ManagedStoreError::Conflict.into()),
                },
                Err(ManagedStoreError::Unprepared) => Some(recovering(
                    context,
                    id,
                    Recovery::Reserved,
                    runtime
                        .block_on(context.client.claim_revision(
                            context.build.ledger,
                            claim,
                            id.key(context.operation).id,
                        ))
                        .map_err(CliError::from)
                        .and_then(|revision| {
                            revision
                                .ok_or_else(|| InputError::Invalid("claim was not found").into())
                        }),
                )?),
                Err(error) => return Err(error.into()),
            }
        } else {
            revision
        }
    } else {
        revision
    };
    let prepared = store.prepare(
        id,
        OperationIntent {
            name: descriptor.name,
            version: descriptor.version,
            canonical: &canonical,
        },
        |key| {
            let PlannedOperation::Mutation(command) = authored
                .build(&context.build, &mut random_id)
                .map_err(StoreError::Expansion)?
            else {
                return Err(
                    StoreError::Expansion(InputError::Invalid("expected a mutation")).into(),
                );
            };
            let operation = Operation::Managed {
                key,
                operation: ManagedOperation::Submit {
                    expected_revision: revision,
                    command,
                },
            };
            Ok(RequestEnvelope {
                protocol: participant_protocol(&operation),
                ledger: context.build.ledger,
                route_epoch: RouteEpoch(1),
                request_epoch: RequestEpoch(1),
                request_id: key.id,
                operation,
            })
        },
    );
    recovering(
        context,
        id,
        Recovery::Reserved,
        prepared.map_err(CliError::from),
    )?;
    recovering(
        context,
        id,
        Recovery::Retry,
        drive(
            runtime,
            context,
            &requests,
            &store,
            id,
            options.output.format,
        ),
    )
}
fn drive(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    requests: &ManagedRequests,
    store: &ManagedOperationStore,
    id: ManagedOperationId,
    format: OutputFormat,
) -> Result<()> {
    let receipt = match store.receipt(id)? {
        Some(receipt) => receipt,
        None => {
            let prepared = store.request(id)?;
            match runtime.block_on(
                context
                    .client
                    .submit_managed_outcome(prepared.request, context.operation),
            ) {
                Ok(focal_client::ManagedSubmitOutcome::Committed(reply)) => {
                    store.record_receipt(id, &reply.receipt)?;
                    reply.receipt
                }
                Ok(focal_client::ManagedSubmitOutcome::Domain(outcome)) => {
                    render_domain(id, &outcome, format)?;
                    return Err(CliError::Domain(outcome));
                }
                Err(error) => {
                    let condition = if matches!(error, ClientError::OutcomeUnknown { .. }) {
                        "OutcomeUnknown"
                    } else {
                        "RequestUnconfirmed"
                    };
                    render(id, condition, None, format)?;
                    return Err(error.into());
                }
            }
        }
    };
    show_receipt(runtime, context, requests, id, &receipt, format)
}
fn show_receipt(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    requests: &ManagedRequests,
    id: ManagedOperationId,
    receipt: &ManagedReceipt,
    format: OutputFormat,
) -> Result<()> {
    render(
        id,
        if matches!(receipt.outcome, ManagedReceiptOutcome::Sealed { .. }) {
            "Sealed"
        } else {
            "Committed"
        },
        Some(receipt),
        format,
    )?;
    delivered(runtime, context, requests, id);
    Ok(())
}
pub(super) fn retry(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    text: &str,
    format: OutputFormat,
) -> Result<()> {
    let id = text.parse()?;
    let requests = find(context, id)?;
    let store = recovering(
        context,
        id,
        Recovery::Reserved,
        requests.store().map_err(other),
    )?;
    if matches!(store.request(id), Err(ManagedStoreError::Unprepared))
        && super::upload::resume_managed(runtime, context, id, format)?
    {
        return Ok(());
    }
    // Printing the saved receipt happens before maintenance can retire it.
    recovering(
        context,
        id,
        Recovery::Retry,
        drive(runtime, context, &requests, &store, id, format),
    )
}

/// Reserve only the existing managed request identity for a multipart authored
/// submission; its artifact command is prepared after actual custody sealing.
pub(super) fn reserve_for_submission(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    options: &mut MutationOptions,
) -> Result<ManagedOperationId> {
    if let Some(text) = &options.operation_id {
        let id = text.parse()?;
        find(context, id)?.store().map_err(other)?;
        return Ok(id);
    }
    let requests = open(context)?;
    maintain(runtime, context, &requests)?;
    let id = requests
        .store()
        .map_err(other)?
        .reserve(RequestId(random_id()?))?;
    options.operation_id = Some(id.to_string());
    Ok(id)
}
pub(super) fn inspect(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    text: &str,
    remote: bool,
    format: OutputFormat,
) -> Result<()> {
    let id = text.parse::<ManagedOperationId>()?;
    let requests = find(context, id)?;
    let store = requests.store().map_err(other)?;
    if remote {
        let saved = match store.receipt(id) {
            Ok(receipt) => receipt,
            Err(ManagedStoreError::Retired) => None,
            Err(error) => return Err(error.into()),
        };
        let prepared = match store.request(id) {
            Ok(prepared) => Some(prepared),
            Err(ManagedStoreError::Unprepared | ManagedStoreError::Retired) => None,
            Err(error) => return Err(error.into()),
        };
        let mut request = context.envelope(Operation::RequestStreamRead {
            cluster: context.operation.cluster,
            query: RequestStreamQuery::Receipt {
                key: id.key(context.operation),
            },
        })?;
        request.protocol = MANAGED_PROTOCOL_VERSION;
        let reply = runtime.block_on(
            context
                .client
                .request_stream_read(request, context.operation),
        )?;
        if let RequestStreamReadResult::Receipt {
            resolution: ManagedReceiptResolution::Retained(receipt),
            ..
        } = &reply.page.result
        {
            validate_remote_receipt(
                prepared.as_ref().map(|prepared| &prepared.request),
                saved.as_ref(),
                receipt,
            )?;
        }
        return render_remote(id, &reply, format);
    }
    match store.receipt(id) {
        Ok(Some(receipt)) => render(
            id,
            if matches!(receipt.outcome, ManagedReceiptOutcome::Sealed { .. }) {
                "Sealed"
            } else {
                "Committed"
            },
            Some(&receipt),
            format,
        ),
        Ok(None) => match store.request(id) {
            Ok(_) => render(id, "Pending", None, format),
            Err(ManagedStoreError::Unprepared) => render(id, "Reserved", None, format),
            Err(error) => Err(error.into()),
        },
        Err(ManagedStoreError::Retired) => render(id, "Retired", None, format),
        Err(error) => Err(error.into()),
    }
}
fn validate_remote_receipt(
    prepared: Option<&RequestEnvelope>,
    saved: Option<&ManagedReceipt>,
    receipt: &ManagedReceipt,
) -> Result<()> {
    if let Some(saved) = saved {
        if saved != receipt {
            return Err(ManagedStoreError::ReceiptMismatch.into());
        }
    } else {
        let prepared = prepared.ok_or(ManagedStoreError::ReceiptMismatch)?;
        let (key, family, hash) = managed_request_identity(prepared).map_err(other)?;
        validate_managed_receipt(receipt, &key, family, hash, &WireLimits::default())
            .map_err(|_| ManagedStoreError::ReceiptMismatch)?;
    }
    Ok(())
}
pub(super) fn reserve(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    format: OutputFormat,
) -> Result<()> {
    let requests = open(context)?;
    maintain(runtime, context, &requests)?;
    let id = requests
        .store()
        .map_err(other)?
        .reserve(RequestId(random_id()?))?;
    recovering(
        context,
        id,
        Recovery::Reserved,
        render(id, "Reserved", None, format),
    )
}
pub(super) fn pending(context: &Context, format: OutputFormat) -> Result<()> {
    #[derive(Serialize)]
    struct PendingOutput {
        schema_version: u16,
        operations: Vec<PendingRow>,
    }
    #[derive(Serialize)]
    struct PendingRow {
        client: &'static str,
        operation_id: Option<String>,
        condition: &'static str,
    }
    let mut rows = Vec::new();
    rows.try_reserve_exact(512)
        .map_err(|_| InputError::Capacity)?;
    for name in CLIENTS {
        if let Some(requests) = existing(context, name)? {
            let client = if name == "CLI.requests" { "CLI" } else { "MCP" };
            let store = match requests.store() {
                Ok(store) => store,
                Err(ManagedRequestsError::NotReady) => {
                    rows.push(PendingRow {
                        client,
                        operation_id: None,
                        condition: "OwnershipPending",
                    });
                    continue;
                }
                Err(error) => return Err(other(error)),
            };
            for id in store.outstanding()? {
                let condition = match store.receipt(id)? {
                    Some(ManagedReceipt {
                        outcome: ManagedReceiptOutcome::Sealed { .. },
                        ..
                    }) => "Sealed",
                    Some(_) => "Committed",
                    None => match store.request(id) {
                        Ok(_) => "Pending",
                        Err(ManagedStoreError::Unprepared) => "Reserved",
                        Err(error) => return Err(error.into()),
                    },
                };
                rows.push(PendingRow {
                    client,
                    operation_id: Some(id.to_string()),
                    condition,
                });
            }
        }
    }
    let mut out = std::io::stdout().lock();
    if matches!(format, OutputFormat::Json | OutputFormat::Yaml) {
        output::structured_to(
            &mut out,
            &PendingOutput {
                schema_version: 1,
                operations: rows,
            },
            format,
        )?;
    } else {
        writeln!(out, "CLIENT\tCONDITION\tOPERATION_ID")?;
        for row in rows {
            writeln!(
                out,
                "{}\t{}\t{}",
                row.client,
                row.condition,
                row.operation_id
                    .as_deref()
                    .unwrap_or("registration pending")
            )?;
        }
    }
    out.flush()?;
    Ok(())
}
pub(super) fn acknowledge(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    text: &str,
    format: OutputFormat,
) -> Result<()> {
    let id = text.parse()?;
    let requests = find(context, id)?;
    requests.mark_delivered(id).map_err(other)?;
    maintain(runtime, context, &requests)?;
    render(id, "Acknowledged", None, format)
}
pub(super) fn seal(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    text: &str,
    format: OutputFormat,
) -> Result<()> {
    let id = text.parse()?;
    let requests = find(context, id)?;
    let result = (|| {
        maintain(runtime, context, &requests)?;
        let store = requests.store().map_err(other)?;
        let receipt = seal_receipt(&store, id, || maintain(runtime, context, &requests))?;
        show_receipt(runtime, context, &requests, id, &receipt, format)
    })();
    recovering(context, id, Recovery::Seal, result)
}
fn seal_receipt(
    store: &ManagedOperationStore,
    id: ManagedOperationId,
    mut maintain: impl FnMut() -> Result<()>,
) -> Result<ManagedReceipt> {
    for _ in 0..8 {
        if let Some(receipt) = store.receipt(id)? {
            return Ok(receipt);
        }
        // Shared maintenance may have acquired the single control slot since
        // the prior drain. Complete it and recheck; sealing never submits the
        // original prepared mutation as a fallback.
        match store.prepare_seal(id, RequestId(random_id()?)) {
            Ok(_) | Err(ManagedStoreError::ControlPending) => {}
            Err(error) => return Err(error.into()),
        }
        maintain()?;
    }
    store
        .receipt(id)?
        .ok_or(ManagedStoreError::ControlPending.into())
}

#[derive(Serialize)]
struct Output<'a> {
    schema_version: u16,
    operation_id: String,
    condition: &'a str,
    result: Option<serde_json::Value>,
    receipt: Option<&'a ManagedReceipt>,
}
fn render(
    id: ManagedOperationId,
    condition: &str,
    receipt: Option<&ManagedReceipt>,
    format: OutputFormat,
) -> Result<()> {
    let result = receipt.map(|receipt| match &receipt.outcome {
        ManagedReceiptOutcome::Domain(outcome) => output::command_result(outcome),
        outcome => serde_json::json!({"outcome":outcome}),
    });
    let mut out = std::io::stdout().lock();
    if matches!(format, OutputFormat::Json | OutputFormat::Yaml) {
        output::structured_to(
            &mut out,
            &Output {
                schema_version: 1,
                operation_id: id.to_string(),
                condition,
                result,
                receipt,
            },
            format,
        )?;
    } else {
        writeln!(out, "{condition}")?;
        if let Some(receipt) = receipt {
            human_result(&mut out, &receipt.outcome)?;
        }
        if receipt.is_none() {
            writeln!(out, "OPERATION\t{id}")?;
        }
    }
    out.flush()?;
    Ok(())
}
fn human_result(out: &mut impl Write, outcome: &ManagedReceiptOutcome) -> Result<()> {
    match outcome {
        ManagedReceiptOutcome::Domain(command) => match command {
            CommandResult::Generated(ids) | CommandResult::Existing(ids) => {
                for id in ids {
                    writeln!(out, "Claim: {id}")?;
                }
            }
            CommandResult::Claim { claim, status } => writeln!(out, "Claim: {claim} ({status:?})")?,
            CommandResult::Receipt { claim, fence } => writeln!(
                out,
                "Receipt: {} (epoch {})\nClaim: {claim}",
                fence.receipt, fence.epoch
            )?,
            CommandResult::EvidenceSet(id) => writeln!(out, "Evidence set: {id}")?,
            CommandResult::Artifact(reference) => writeln!(
                out,
                "Artifact: {}\nDescriptor hash: {}",
                reference.id, reference.hash
            )?,
            CommandResult::Testament(id) => writeln!(out, "Testament: {id}")?,
            other => writeln!(out, "{other:?}")?,
        },
        ManagedReceiptOutcome::Sealed { .. } => writeln!(
            out,
            "This request cannot execute. Existing business objects are unchanged by the fence."
        )?,
        ManagedReceiptOutcome::Cursor { revision, .. } => {
            writeln!(out, "Cursor control revision: {revision}")?
        }
    }
    Ok(())
}
fn render_remote(
    id: ManagedOperationId,
    reply: &RequestStreamReadReply,
    format: OutputFormat,
) -> Result<()> {
    #[derive(Serialize)]
    struct Remote<'a> {
        schema_version: u16,
        operation_id: String,
        reply: &'a RequestStreamReadReply,
    }
    let mut out = std::io::stdout().lock();
    if matches!(format, OutputFormat::Json | OutputFormat::Yaml) {
        output::structured_to(
            &mut out,
            &Remote {
                schema_version: 1,
                operation_id: id.to_string(),
                reply,
            },
            format,
        )?;
    } else {
        writeln!(out, "OPERATION\t{id}")?;
        writeln!(out, "RESULT\t{:?}", reply.page.result)?;
    }
    out.flush()?;
    Ok(())
}
fn render_domain(
    id: ManagedOperationId,
    outcome: &DomainOutcome,
    format: OutputFormat,
) -> Result<()> {
    #[derive(Serialize)]
    struct Domain<'a> {
        schema_version: u16,
        operation_id: String,
        condition: &'static str,
        outcome: &'a DomainOutcome,
    }
    let mut out = std::io::stdout().lock();
    if matches!(format, OutputFormat::Json | OutputFormat::Yaml) {
        output::structured_to(
            &mut out,
            &Domain {
                schema_version: 1,
                operation_id: id.to_string(),
                condition: "DomainOutcome",
                outcome,
            },
            format,
        )?;
    } else {
        writeln!(out, "{outcome:?}")?;
        writeln!(out, "OPERATION\t{id}")?;
    }
    out.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create(root: &std::path::Path) -> ManagedOperationStore {
        let context = OperationContext {
            cluster: [1; 16],
            principal: ParticipantId::from_u128(2),
            ledger: LedgerId {
                tenant: TenantId::from_u128(3),
                session: SessionId::from_u128(4),
            },
        };
        let limits = ManagedStoreLimits {
            window: 2,
            ..ManagedStoreLimits::default()
        };
        let registration = RequestStreamControlInput {
            cluster: context.cluster,
            ledger: context.ledger,
            principal: context.principal,
            id: RequestId::from_u128(5),
            command: RequestStreamCommand::Register {
                slot: 0,
                expected_generation: 0,
                owner: RequestId::from_u128(6),
                window: 2,
            },
        };
        let store =
            ManagedOperationStore::create(root, context, limits, registration.clone()).unwrap();
        store
            .record_registration(RequestStreamControlReceipt {
                cluster: context.cluster,
                ledger: context.ledger,
                principal: context.principal,
                id: registration.id,
                intent_hash: registration.intent_hash().unwrap(),
                raft_index: 10,
                outcome: RequestStreamControlOutcome::Registered(RequestStreamState::Active {
                    stream: store.status().unwrap().stream,
                    owner: RequestId::from_u128(6),
                    revision: 1,
                    window: 2,
                    acknowledged_through: 0,
                }),
            })
            .unwrap();
        store
    }
    fn prepare(store: &ManagedOperationStore) -> (ManagedOperationId, RequestEnvelope) {
        let id = store.reserve(RequestId::from_u128(20)).unwrap();
        let prepared = store
            .prepare(
                id,
                OperationIntent {
                    name: "claim.post",
                    version: 1,
                    canonical: b"{}",
                },
                |key| {
                    Ok(RequestEnvelope {
                        protocol: MANAGED_PROTOCOL_VERSION,
                        ledger: key.stream.ledger,
                        route_epoch: RouteEpoch(1),
                        request_epoch: RequestEpoch(1),
                        request_id: key.id,
                        operation: Operation::Managed {
                            key,
                            operation: ManagedOperation::Submit {
                                expected_revision: None,
                                command: Command::PostClaim {
                                    claim: ClaimId::from_u128(21),
                                },
                            },
                        },
                    })
                },
            )
            .unwrap();
        (id, prepared.request)
    }
    fn finish_seal(store: &ManagedOperationStore, index: u64) -> ManagedRequestKey {
        let input = store.pending_control().unwrap().unwrap();
        let RequestStreamCommand::Seal {
            key,
            family,
            intent_hash,
            ..
        } = input.command
        else {
            panic!("only saved seal controls may be submitted")
        };
        store
            .record_control(RequestStreamControlReceipt {
                cluster: input.cluster,
                ledger: input.ledger,
                principal: input.principal,
                id: input.id,
                intent_hash: input.intent_hash().unwrap(),
                raft_index: index,
                outcome: RequestStreamControlOutcome::Sealed(Box::new(ManagedReceipt {
                    key,
                    sequence: SessionSeq(0),
                    raft_index: index,
                    intent_hash,
                    outcome: ManagedReceiptOutcome::Sealed { family },
                })),
            })
            .unwrap();
        key
    }
    #[test]
    fn seal_drains_other_saved_control_then_fences_original_without_submitting_it() {
        let dir = tempfile::tempdir().unwrap();
        let store = create(&dir.path().join("requests"));
        let (id, original) = prepare(&store);
        let other = store.reserve(RequestId::from_u128(22)).unwrap();
        store.prepare_seal(other, RequestId::from_u128(23)).unwrap();
        let mut controls = Vec::new();
        let receipt = seal_receipt(&store, id, || {
            controls.push(finish_seal(&store, 11 + controls.len() as u64));
            Ok(())
        })
        .unwrap();
        assert_eq!(
            controls.iter().map(|key| key.id).collect::<Vec<_>>(),
            vec![
                other
                    .key(OperationContext {
                        cluster: [1; 16],
                        ledger: original.ledger,
                        principal: ParticipantId::from_u128(2),
                    })
                    .id,
                original.request_id
            ]
        );
        assert!(matches!(
            receipt.outcome,
            ManagedReceiptOutcome::Sealed {
                family: ManagedRequestFamily::Domain
            }
        ));
        assert_eq!(store.request(id).unwrap().request, original);
        assert_eq!(store.receipt(id).unwrap(), Some(receipt));
        assert!(store.pending_control().unwrap().is_none());
    }
    #[test]
    fn seal_contention_is_bounded_and_preserves_saved_control_and_original() {
        let dir = tempfile::tempdir().unwrap();
        let store = create(&dir.path().join("requests"));
        let (id, original) = prepare(&store);
        let other = store.reserve(RequestId::from_u128(22)).unwrap();
        let pending = store.prepare_seal(other, RequestId::from_u128(23)).unwrap();
        let mut calls = 0;
        assert!(matches!(
            seal_receipt(&store, id, || {
                calls += 1;
                Ok(())
            }),
            Err(CliError::Managed(ManagedStoreError::ControlPending))
        ));
        assert_eq!(calls, 8);
        assert_eq!(store.pending_control().unwrap(), Some(pending));
        assert_eq!(store.request(id).unwrap().request, original);
        assert!(store.receipt(id).unwrap().is_none());
    }
    #[test]
    fn remote_inspect_requires_exact_saved_receipt_or_original_family_and_hash() {
        let dir = tempfile::tempdir().unwrap();
        let store = create(&dir.path().join("requests"));
        let (_, original) = prepare(&store);
        let (key, _, intent_hash) = managed_request_identity(&original).unwrap();
        let receipt = ManagedReceipt {
            key,
            sequence: SessionSeq(1),
            raft_index: 11,
            intent_hash,
            outcome: ManagedReceiptOutcome::Domain(CommandResult::Noop),
        };
        validate_remote_receipt(Some(&original), None, &receipt).unwrap();
        let mut forged = receipt.clone();
        forged.outcome = ManagedReceiptOutcome::Cursor {
            revision: 1,
            floor: SessionSeq(0),
            record: None,
        };
        assert!(validate_remote_receipt(Some(&original), None, &forged).is_err());
        assert!(validate_remote_receipt(None, Some(&receipt), &forged).is_err());
        assert!(validate_remote_receipt(None, None, &receipt).is_err());
        forged = receipt.clone();
        forged.intent_hash = ContentHash([9; 32]);
        assert!(validate_remote_receipt(Some(&original), None, &forged).is_err());
        validate_remote_receipt(None, Some(&receipt), &receipt).unwrap();
    }
}
