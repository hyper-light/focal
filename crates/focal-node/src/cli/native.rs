//! The native engine path of the manual CLI. One standing read per invocation
//! decides the engine; native mutations are compiled by the shared compiler
//! from one fixed-prefix read of their bindings, journaled as exact frames
//! under `n1:` references and resent verbatim until they commit.
use super::{CliError, Context, Result, args::*, native_documents, output, random_id};
use focal_client::input::InputError;
use focal_client::native_store::{
    NativeIdentity, NativeIdentityKind, NativeOperation, NativeOperationId, NativeOperationStore,
    NativeStage, NativeStoreError, NativeStoreLimits,
};
use focal_client::operations::{
    ApplicationResult, NativeAuthoredOperation, NativeContextDocument, NativeEmptyDocument,
    NativeObjectDocument, NativeReadOperation, NativeWaitDocument, OperationOutput,
};
use focal_client::{ClientError, failure};
use focal_model::*;
use focal_native_client::{CompileLimits, DriveError, NativeContentProfile, Preparation};
use focal_wire::*;
use std::{io::Write, path::Path};

/// The human CLI's native journal under the context root's `client` directory.
const STORE: &str = "native";
/// The MCP adapter's native journal beside it; each adapter owns its
/// identities and delivery marks, as the V1 request stores do.
pub(super) const MCP_STORE: &str = "mcp-native";

/// Probe the ledger's engine. A node without the native engine refuses the
/// profile at negotiation before any frame is seen.
pub(super) fn detect(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
) -> Result<Option<NativeStanding>> {
    let request = context.envelope(Operation::NativeRead(NativeReadRequest {
        consistency: ReadConsistency::Linearizable,
        query: NativeReadQuery::Standing,
        max_items: 1,
    }))?;
    match runtime.block_on(context.client.native_standing(request)) {
        Ok(standing) => Ok(standing),
        // Without a reachable owner the engine is unknown. A context that has
        // already journaled native operations stays native, so no V1 identity
        // is minted for a native ledger; nothing was sent, so no journal is
        // owed. Any other context keeps the V1 behaviour, whose local
        // validation and journaling never needed the network.
        Err(ClientError::Transport) if !initialized_in(&context.root.join("client"), STORE) => {
            Ok(None)
        }
        Err(error) => Err(error.into()),
    }
}

/// Whether a native journal named `name` has been created under `parent`.
pub(super) fn initialized_in(parent: &Path, name: &str) -> bool {
    std::fs::symlink_metadata(parent.join(format!("{name}.initialized"))).is_ok()
}

fn unavailable(what: &str) -> CliError {
    CliError::Input(format!(
        "{what} is not available on the native engine yet; it arrives with the native index families"
    ))
}

/// Route one manual command on a native ledger.
pub(super) fn run(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    command: Commands,
    standing: &NativeStanding,
) -> Result<()> {
    let (operation, options) = match command {
        Commands::Submit { command } => match command {
            SubmitCommand::Claim(args) => {
                let (document, options) = native_documents::claim(args)?;
                (NativeAuthoredOperation::ClaimSubmit(document), options)
            }
            SubmitCommand::Claims(_) => return Err(unavailable("claim batches")),
            SubmitCommand::Testament(args) => {
                let (document, options) = native_documents::testament(args)?;
                (NativeAuthoredOperation::TestamentSubmit(document), options)
            }
            SubmitCommand::Artifact(args) => {
                let (document, options) = native_documents::work(args)?;
                (NativeAuthoredOperation::ArtifactSubmit(document), options)
            }
            SubmitCommand::Validation(_) => {
                return Err(CliError::Input(
                    "use validation report on the native engine".into(),
                ));
            }
        },
        Commands::Claim { command } => match command {
            ClaimCommand::Post(args) => {
                let (document, options) = native_documents::claim_target(args)?;
                (NativeAuthoredOperation::ClaimPost(document), options)
            }
            ClaimCommand::Cancel(args) => {
                let (document, options) = native_documents::cancel(args)?;
                (NativeAuthoredOperation::ClaimCancel(document), options)
            }
            ClaimCommand::ReleaseScope(args) => {
                let (document, options) = native_documents::claim_target(args)?;
                (
                    NativeAuthoredOperation::ClaimReleaseScope(document),
                    options,
                )
            }
            ClaimCommand::Wait(args) => return wait(runtime, context, args),
            ClaimCommand::Lineage(args) => {
                let outcome = observe(
                    runtime,
                    context,
                    &NativeReadOperation::ClaimLineage(NativeObjectDocument { id: args.id }),
                )?;
                let focal_native_client::NativeReadOutcome::Page(page) = outcome else {
                    return Err(CliError::InvalidResponse);
                };
                return render_page_as("Lineage", page, args.output.format);
            }
            ClaimCommand::Challenge(args) => {
                let (document, options) = native_documents::challenge(*args)?;
                (
                    NativeAuthoredOperation::ClaimChallenge(Box::new(document)),
                    options,
                )
            }
            ClaimCommand::Consult(args) => {
                let (document, options) = native_documents::consult(*args)?;
                (
                    NativeAuthoredOperation::ClaimConsult(Box::new(document)),
                    options,
                )
            }
            ClaimCommand::Correct(args) => {
                let (document, options) = native_documents::correct(*args)?;
                (
                    NativeAuthoredOperation::ClaimCorrect(Box::new(document)),
                    options,
                )
            }
            ClaimCommand::FollowUp(args) => {
                let (document, options) = native_documents::follow_up(*args)?;
                (
                    NativeAuthoredOperation::ClaimFollowUp(Box::new(document)),
                    options,
                )
            }
            ClaimCommand::Progress(_) => {
                return Err(CliError::Input(
                    "progress messages are a V1 command; the native engine records work artifacts and diagnostics".into(),
                ));
            }
            ClaimCommand::Supersede(_) => return Err(unavailable("claim supersession")),
        },
        Commands::Receipt { command } => match command {
            ReceiptCommand::Acquire(args) => {
                let (document, options) = native_documents::receipt(args)?;
                (NativeAuthoredOperation::ReceiptAcquire(document), options)
            }
            ReceiptCommand::Adopt(args) => {
                let (document, options) = native_documents::adopt(args)?;
                (NativeAuthoredOperation::ReceiptAdopt(document), options)
            }
        },
        Commands::Testament { command } => match command {
            TestamentCommand::Submit(args) => {
                let (document, options) = native_documents::testament(*args)?;
                (NativeAuthoredOperation::TestamentSubmit(document), options)
            }
            TestamentCommand::Post(args) => {
                let (document, options) = native_documents::response_target(args)?;
                (NativeAuthoredOperation::TestamentPost(document), options)
            }
            TestamentCommand::Receive(args) => {
                let (document, options) = native_documents::response_target(args)?;
                (NativeAuthoredOperation::TestamentReceive(document), options)
            }
        },
        Commands::Validation { command } => match command {
            ValidationCommand::Begin(args) => {
                let (document, options) = native_documents::begin(args)?;
                (NativeAuthoredOperation::ValidationBegin(document), options)
            }
            ValidationCommand::Report(args) => {
                let (document, options) = native_documents::report(*args)?;
                (NativeAuthoredOperation::ValidationReport(document), options)
            }
            ValidationCommand::BeginIncrement(_) => {
                return Err(CliError::Input(
                    "use validation begin --phase increment on the native engine".into(),
                ));
            }
            ValidationCommand::SealIncrements(args) => {
                let (document, options) = native_documents::seal_increments(args)?;
                (
                    NativeAuthoredOperation::ValidationSealIncrements(document),
                    options,
                )
            }
            ValidationCommand::EnterWholeWork(args) => {
                let (document, options) = native_documents::response_target(args)?;
                (
                    NativeAuthoredOperation::ValidationEnterWholeWork(document),
                    options,
                )
            }
            ValidationCommand::Complete(_) => {
                return Err(CliError::Input(
                    "the native engine derives completion from accepted results; there is no complete command".into(),
                ));
            }
        },
        Commands::Artifact { command } => match command {
            ArtifactCommand::Submit(args) => {
                let (document, options) = native_documents::work(*args)?;
                (NativeAuthoredOperation::ArtifactSubmit(document), options)
            }
            ArtifactCommand::Diagnostic(args) => {
                let (document, options) = native_documents::diagnostic(*args)?;
                (
                    NativeAuthoredOperation::ArtifactDiagnostic(document),
                    options,
                )
            }
            ArtifactCommand::Fail(args) => {
                let (document, options) = native_documents::fail(args)?;
                (NativeAuthoredOperation::ArtifactFail(document), options)
            }
            ArtifactCommand::Receive(args) => {
                let (document, options) = native_documents::artifact_target(args)?;
                (NativeAuthoredOperation::ArtifactReceive(document), options)
            }
            ArtifactCommand::Reject(args) => {
                let (document, options) = native_documents::reject(*args)?;
                (NativeAuthoredOperation::ArtifactReject(document), options)
            }
            ArtifactCommand::Register(_) => {
                return Err(unavailable("independent artifact registration"));
            }
            ArtifactCommand::Upload { .. } => return Err(unavailable("chunked artifact upload")),
        },
        Commands::Get { command } => return get(runtime, context, command),
        Commands::List { command } => return list(runtime, context, command),
        Commands::Evidence { .. } => {
            return Err(CliError::Input(
                "evidence sets are a V1 concept; native work artifacts attach directly to the cycle".into(),
            ));
        }
        Commands::Audit { command } => match command {
            AuditCommand::Generate(args) => {
                let (document, options) = native_documents::audit(args)?;
                (NativeAuthoredOperation::AuditGenerate(document), options)
            }
            AuditCommand::Post(args) => {
                let (document, options) = native_documents::audit_target(args)?;
                (NativeAuthoredOperation::AuditPost(document), options)
            }
        },
        Commands::Monitor { command } => match command {
            super::monitor::MonitorCommand::Register(args) => {
                let (document, options) = super::monitor::native_register(*args)?;
                (NativeAuthoredOperation::MonitorRegister(document), options)
            }
            super::monitor::MonitorCommand::Rebind(args) => {
                let (document, options) = super::monitor::native_rebind(args)?;
                (NativeAuthoredOperation::MonitorRebind(document), options)
            }
            super::monitor::MonitorCommand::Cancel(args) => {
                let (document, options) = super::monitor::native_cancel(args)?;
                (NativeAuthoredOperation::MonitorCancel(document), options)
            }
            super::monitor::MonitorCommand::Get(_) => {
                return Err(CliError::Input(
                    "use list monitors --claim ID on the native engine".into(),
                ));
            }
        },
        Commands::Watch { command } => {
            return super::watch::run(
                runtime,
                context,
                command,
                focal_client::watch::WatchEngine::Native,
            );
        }
        Commands::Ledger { .. } => return Err(unavailable("graph traversal")),
        Commands::Validator { .. } => return Err(unavailable("validator contract listing")),
    };
    submit(runtime, context, standing, operation, options)
}

/// Open the client's native journal, creating it once with a marker outside
/// the store directory so a lost store cannot silently re-admit identities.
pub(super) fn store(context: &Context, create: bool) -> Result<Option<NativeOperationStore>> {
    store_in(&context.root.join("client"), STORE, create)
}
/// Open or create the native journal `name` under `parent`. The marker is
/// written only after the store is durable and lives beside it, so an
/// interrupted creation is retried and a lost store is refused rather than
/// recreated under the same identities.
pub(super) fn store_in(
    parent: &Path,
    name: &str,
    create: bool,
) -> Result<Option<NativeOperationStore>> {
    let path = parent.join(name);
    let marker = parent.join(format!("{name}.initialized"));
    if initialized_in(parent, name) {
        return Ok(Some(NativeOperationStore::open(
            path,
            NativeStoreLimits::default(),
        )?));
    }
    if !create {
        return Ok(None);
    }
    super::private_parent(parent)?;
    // Creation is serialized across processes by a lock beside the marker:
    // the first process creates the store and publishes the marker, later
    // ones find it and open. A store without its marker is an interrupted
    // creation that never admitted an identity, so it is removed and redone.
    let _creation = creation_lock(parent, name)?;
    if initialized_in(parent, name) {
        return Ok(Some(NativeOperationStore::open(
            path,
            NativeStoreLimits::default(),
        )?));
    }
    if std::fs::symlink_metadata(&path).is_ok() {
        std::fs::remove_dir_all(&path)?;
        focal_platform::sync_dir(parent)?;
    }
    let store = NativeOperationStore::create(path, NativeStoreLimits::default())?;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&marker)?;
    file.write_all(b"FCLNST01")?;
    file.sync_all()?;
    focal_platform::sync_dir(parent)?;
    Ok(Some(store))
}
/// The exclusive creation lock `<name>.lock` beside the store, held only
/// while the store is created; contenders wait for the short critical section.
fn creation_lock(parent: &Path, name: &str) -> Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options.open(parent.join(format!("{name}.lock")))?;
    let deadline = std::time::Instant::now()
        .checked_add(std::time::Duration::from_secs(5))
        .ok_or_else(|| CliError::Other("clock overflow".into()))?;
    loop {
        match file.try_lock() {
            Ok(()) => return Ok(file),
            Err(std::fs::TryLockError::WouldBlock) => {
                if std::time::Instant::now() >= deadline {
                    return Err(CliError::Other(
                        "another process is still creating the native request journal".into(),
                    ));
                }
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
            Err(std::fs::TryLockError::Error(error)) => return Err(error.into()),
        }
    }
}

pub(super) fn profile(standing: &NativeStanding) -> NativeContentProfile {
    match standing.profile {
        NativeProfile::ProjectionOnly => NativeContentProfile::ProjectionOnly,
        NativeProfile::AuthoredV1 => NativeContentProfile::AuthoredV1,
    }
}

/// One blocking bounded list on this context.
fn lists<'a>(
    runtime: &'a tokio::runtime::Runtime,
    context: &'a Context,
) -> impl FnMut(NativeListRequest) -> std::result::Result<NativeListPage, DriveError> + 'a {
    move |request| {
        let envelope = RequestEnvelope {
            protocol: NATIVE_PROTOCOL_VERSION,
            ledger: context.build.ledger,
            route_epoch: RouteEpoch(1),
            request_epoch: RequestEpoch(1),
            request_id: RequestId(random_id()?),
            operation: Operation::NativeList(request),
        };
        Ok(runtime.block_on(context.client.native_list(envelope))?)
    }
}
/// One bounded list from the shared `list` flags. `--all` follows the
/// continuation until it is absent, printing one page at a time; an empty
/// page with a cursor is a residual-filtered stretch, not the end.
fn list(runtime: &tokio::runtime::Runtime, context: &Context, command: ListCommand) -> Result<()> {
    let (mut operation, args) = native_documents::list(command)?;
    let format = args.output.format;
    let mut pages = 0u64;
    loop {
        let page =
            focal_native_client::list(&operation, &context.build, &mut lists(runtime, context))?;
        let next = page.next.as_ref().map(|cursor| output::hex(&cursor.0));
        render_list_page(page, format)?;
        pages = pages.checked_add(1).ok_or(InputError::Capacity)?;
        if !args.all {
            return Ok(());
        }
        match next {
            Some(cursor) => {
                if operation.page().cursor.as_deref() == Some(cursor.as_str()) {
                    return Err(CliError::InvalidResponse);
                }
                operation.page_mut().cursor = Some(cursor);
            }
            None => return Ok(()),
        }
    }
}
fn render_list_page(page: NativeListPage, format: OutputFormat) -> Result<()> {
    match format {
        OutputFormat::Json | OutputFormat::Yaml => output::structured(
            &ApplicationResult {
                schema_version: 2,
                operation_id: None,
                condition: "Listed".into(),
                result: OperationOutput::NativeList {
                    page: Box::new(page),
                },
            },
            format,
        ),
        OutputFormat::Table => {
            let mut out = std::io::stdout().lock();
            for object in &page.objects {
                let value =
                    serde_json::to_value(object).map_err(|e| CliError::Other(Box::new(e)))?;
                let (kind, body) = match value.as_object().and_then(|map| map.iter().next()) {
                    Some((kind, body)) => (kind.clone(), body.clone()),
                    None => (value.to_string(), serde_json::Value::Null),
                };
                writeln!(out, "OBJECT\t{kind}\t{body}")?;
            }
            writeln!(
                out,
                "PREFIX\t{}\tVISITED\t{}",
                page.native_sequence.0, page.visited
            )?;
            if let Some(cursor) = &page.next {
                writeln!(out, "CURSOR\t{}", output::hex(&cursor.0))?;
            }
            out.flush()?;
            Ok(())
        }
    }
}

/// One blocking linearizable read per driver requirement on this context.
fn reads<'a>(
    runtime: &'a tokio::runtime::Runtime,
    context: &'a Context,
) -> impl FnMut(NativeReadRequest) -> std::result::Result<NativeReadPage, DriveError> + 'a {
    move |request| {
        let envelope = RequestEnvelope {
            protocol: NATIVE_PROTOCOL_VERSION,
            ledger: context.build.ledger,
            route_epoch: RouteEpoch(1),
            request_epoch: RequestEpoch(1),
            request_id: RequestId(random_id()?),
            operation: Operation::NativeRead(request),
        };
        Ok(runtime.block_on(context.client.native_read(envelope))?)
    }
}

pub(super) fn submit(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    standing: &NativeStanding,
    operation: NativeAuthoredOperation,
    options: MutationOptions,
) -> Result<()> {
    if options.operation.is_some() {
        return Err(CliError::Input(
            "native operations are journaled under n1: references; use --operation-id".into(),
        ));
    }
    if options.expected_revision.is_some() {
        return Err(CliError::Input(
            "the native engine binds every committed revision itself; omit --expected-revision"
                .into(),
        ));
    }
    let requested = options
        .operation_id
        .as_deref()
        .map(str::parse::<NativeOperationId>)
        .transpose()?;
    let store = store(context, true)?.ok_or(CliError::InvalidResponse)?;
    let limits = CompileLimits::default();
    let prepared = focal_native_client::prepare(
        &Preparation {
            store: &store,
            context: context.operation,
            build: &context.build,
            profile: profile(standing),
            limits: &limits,
        },
        &operation,
        requested,
        &mut random_id,
        &mut reads(runtime, context),
    )?;
    drive(runtime, context, &store, prepared, options.output.format)
}

fn recovery_hint(context: &Context, id: NativeOperationId) {
    if let Some(invocation) = &context.invocation {
        let _ = writeln!(
            std::io::stderr().lock(),
            "Recovery: {invocation} request retry --operation-id {id}"
        );
    }
}
fn drive(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    store: &NativeOperationStore,
    operation: NativeOperation,
    format: OutputFormat,
) -> Result<()> {
    // A committed receipt is durable before it is printed; if printing fails
    // the operation stays listed and the recovery command reprints it.
    let deliver = |receipt: &NativeReceipt| -> Result<()> {
        if let Err(error) = render_committed(&operation, receipt, format) {
            recovery_hint(context, operation.id);
            return Err(error);
        }
        Ok(store.record_delivered(operation.id, &context.operation)?)
    };
    if let Some(receipt) = operation.receipt {
        return deliver(&receipt);
    }
    match runtime.block_on(context.client.submit_native(operation.request.clone())) {
        Ok(reply @ NativeMutationReply::Committed(receipt)) => {
            store.record_reply(operation.id, &context.operation, &reply)?;
            deliver(&receipt)
        }
        Ok(NativeMutationReply::Pending(_)) => {
            render_failure(
                &operation,
                "OutcomeUnknown",
                failure::Failure::outcome_unknown(),
                "the owner holds the frame as a pending candidate; retry the exact operation",
                format,
            )?;
            recovery_hint(context, operation.id);
            Err(CliError::Unconfirmed)
        }
        Ok(NativeMutationReply::Refused(refusal)) => {
            let classified = failure::native(&refusal);
            render_failure(
                &operation,
                classified.condition,
                classified,
                &refusal.detail,
                format,
            )?;
            store.record_refusal(operation.id, &context.operation, &refusal)?;
            Err(CliError::NativeRefused(refusal))
        }
        Err(error) => {
            let condition = if matches!(error, ClientError::OutcomeUnknown { .. }) {
                "OutcomeUnknown"
            } else {
                "RequestUnconfirmed"
            };
            render_failure(
                &operation,
                condition,
                failure::client(&error),
                &error.to_string(),
                format,
            )?;
            recovery_hint(context, operation.id);
            Err(error.into())
        }
    }
}

fn identity_json(identity: &NativeIdentity) -> serde_json::Value {
    let kind = match identity.kind {
        NativeIdentityKind::Claim => "claim",
        NativeIdentityKind::Validation => "validation",
        NativeIdentityKind::Receipt => "receipt",
        NativeIdentityKind::Artifact => "artifact",
        NativeIdentityKind::Response => "testament",
        NativeIdentityKind::ResultTestament => "result_testament",
        NativeIdentityKind::Monitor => "monitor",
    };
    serde_json::json!({"kind": kind, "id": output::hex(&identity.id)})
}
fn render_committed(
    operation: &NativeOperation,
    receipt: &NativeReceipt,
    format: OutputFormat,
) -> Result<()> {
    match format {
        OutputFormat::Json | OutputFormat::Yaml => output::structured(
            &ApplicationResult {
                schema_version: 2,
                operation_id: Some(operation.id.to_string()),
                condition: "Committed".into(),
                result: OperationOutput::Native {
                    receipt: *receipt,
                    created: operation.created.clone(),
                },
            },
            format,
        ),
        OutputFormat::Table => {
            let mut out = std::io::stdout().lock();
            writeln!(out, "CONDITION\tCommitted")?;
            writeln!(out, "OPERATION_ID\t{}", operation.id)?;
            writeln!(out, "SEQUENCE\t{}", receipt.sequence.0)?;
            writeln!(out, "OPERATION\t{}", receipt.operation.name())?;
            for identity in &operation.created {
                let value = identity_json(identity);
                writeln!(
                    out,
                    "CREATED\t{}\t{}",
                    value.get("kind").and_then(|v| v.as_str()).unwrap_or(""),
                    value.get("id").and_then(|v| v.as_str()).unwrap_or("")
                )?;
            }
            out.flush()?;
            Ok(())
        }
    }
}
fn render_failure(
    operation: &NativeOperation,
    condition: &str,
    classified: failure::Failure,
    detail: &str,
    format: OutputFormat,
) -> Result<()> {
    match format {
        OutputFormat::Json | OutputFormat::Yaml => output::structured(
            &ApplicationResult {
                schema_version: 2,
                operation_id: Some(operation.id.to_string()),
                condition: condition.into(),
                result: OperationOutput::Error {
                    code: classified.code.into(),
                    detail: detail.chars().take(4096).collect(),
                },
            },
            format,
        ),
        OutputFormat::Table => {
            let mut out = std::io::stdout().lock();
            writeln!(out, "CONDITION\t{condition}")?;
            writeln!(out, "OPERATION_ID\t{}", operation.id)?;
            writeln!(out, "CODE\t{}", classified.code)?;
            writeln!(out, "DETAIL\t{detail}")?;
            out.flush()?;
            Ok(())
        }
    }
}

/// Retry an exact journaled frame; a recorded receipt is printed without a send.
pub(super) fn retry(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    id: NativeOperationId,
    format: OutputFormat,
) -> Result<()> {
    let store = store(context, false)?.ok_or(NativeStoreError::MissingOperation)?;
    let operation = store.retry(id, &context.operation)?;
    drive(runtime, context, &store, operation, format)
}
pub(super) fn inspect(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    id: NativeOperationId,
    remote: bool,
    format: OutputFormat,
) -> Result<()> {
    if remote {
        // The owner's committed outcome for this request key; it does not
        // need this adapter's journal, so an operation another adapter
        // journaled under the same context is observable here.
        let page =
            focal_native_client::outcome(id.key(&context.operation), &mut reads(runtime, context))?;
        return render_page_as("Observed", page, format);
    }
    let store = store(context, false)?.ok_or(NativeStoreError::MissingOperation)?;
    let operation = store.retry(id, &context.operation)?;
    match (operation.receipt, &operation.refusal) {
        (Some(receipt), _) => render_committed(&operation, &receipt, format),
        (None, Some(refusal)) => {
            let classified = failure::native(refusal);
            render_failure(
                &operation,
                classified.condition,
                classified,
                &refusal.detail,
                format,
            )
        }
        (None, None) => render_failure(
            &operation,
            "Pending",
            failure::Failure::outcome_unknown(),
            "the exact frame is journaled and has no committed receipt yet",
            format,
        ),
    }
}
/// `(reference, condition)` rows for `request pending`.
pub(super) fn outstanding(context: &Context) -> Result<Vec<(String, &'static str)>> {
    let Some(store) = store(context, false)? else {
        return Ok(Vec::new());
    };
    let mut rows = Vec::new();
    for id in store.outstanding()? {
        let operation = store.retry(id, &context.operation)?;
        rows.try_reserve(1).map_err(|_| InputError::Capacity)?;
        rows.push((
            id.to_string(),
            match operation.stage() {
                NativeStage::Completed => "Committed",
                NativeStage::Pending => "Pending",
            },
        ));
    }
    Ok(rows)
}

/// One read operation on this context: plain reads, the lineage read (which
/// also lists) and the wait observer (whose pause between probes ends on
/// Ctrl-C with the journal untouched).
fn observe(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    operation: &NativeReadOperation,
) -> Result<focal_native_client::NativeReadOutcome> {
    let mut pause = |duration: std::time::Duration| {
        runtime.block_on(async {
            tokio::select! {
                () = tokio::time::sleep(duration) => Ok(()),
                signal = tokio::signal::ctrl_c() => {
                    signal.map_err(|_| DriveError::Cancelled)?;
                    Err(DriveError::Cancelled)
                }
            }
        })
    };
    Ok(focal_native_client::read(
        operation,
        &context.build,
        &mut reads(runtime, context),
        &mut lists(runtime, context),
        &mut pause,
    )?)
}
fn read(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    operation: &NativeReadOperation,
) -> Result<NativeReadPage> {
    match observe(runtime, context, operation)? {
        focal_native_client::NativeReadOutcome::Page(page) => Ok(page),
        focal_native_client::NativeReadOutcome::Wait(_) => Err(CliError::InvalidResponse),
    }
}
/// `claim wait` on the native engine: the same predicates as the V1
/// observer plus `testament`, the same bounds, printed the same way.
fn wait(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    args: super::claim_wait::WaitArgs,
) -> Result<()> {
    let fields = args.claim.is_some() || args.until.is_some() || args.timeout_ms.is_some();
    let document: NativeWaitDocument = match args.input.load(fields)? {
        Some(document) => document,
        None => NativeWaitDocument {
            claim: args
                .claim
                .ok_or_else(|| CliError::Input("claim is required".into()))?,
            until: args
                .until
                .ok_or_else(|| CliError::Input("--until is required".into()))?
                .native(),
            timeout_ms: args.timeout_ms.unwrap_or(30_000),
        },
    };
    let outcome = observe(runtime, context, &NativeReadOperation::ClaimWait(document))?;
    let focal_native_client::NativeReadOutcome::Wait(result) = outcome else {
        return Err(CliError::InvalidResponse);
    };
    let mut stdout = std::io::stdout().lock();
    match args.output.format {
        OutputFormat::Table => writeln!(
            stdout,
            "{} {} {:?} at sequence {} (released: {})",
            result.condition.as_str(),
            result.observation.id,
            result.observation.status,
            result.observation.token.sequence.0,
            result.observation.released
        )?,
        format => output::structured_to(
            &mut stdout,
            &ApplicationResult {
                schema_version: 2,
                operation_id: None,
                condition: result.condition.as_str().into(),
                result: OperationOutput::NativeWait { result },
            },
            format,
        )?,
    }
    stdout.flush()?;
    if result.condition != focal_client::claim_wait::ClaimWaitCondition::Met {
        return Err(CliError::WaitUnfinished(result.condition));
    }
    Ok(())
}
pub(super) fn render_page(page: NativeReadPage, format: OutputFormat) -> Result<()> {
    render_page_as("Read", page, format)
}
/// An observation of the owner (a remote outcome read) is labelled apart from
/// an object read so callers never mistake one for the other.
fn render_page_as(condition: &str, page: NativeReadPage, format: OutputFormat) -> Result<()> {
    match format {
        OutputFormat::Json | OutputFormat::Yaml => output::structured(
            &ApplicationResult {
                schema_version: 2,
                operation_id: None,
                condition: condition.into(),
                result: OperationOutput::NativeRead {
                    page: Box::new(page),
                },
            },
            format,
        ),
        OutputFormat::Table => {
            let mut out = std::io::stdout().lock();
            writeln!(
                out,
                "PREFIX\t{}\tLOGICAL_TIME\t{}",
                page.native_sequence.0, page.logical_time
            )?;
            for object in &page.objects {
                let value =
                    serde_json::to_value(object).map_err(|e| CliError::Other(Box::new(e)))?;
                let (kind, body) = match value.as_object().and_then(|map| map.iter().next()) {
                    Some((kind, body)) => (kind.clone(), body.clone()),
                    None => (value.to_string(), serde_json::Value::Null),
                };
                writeln!(out, "OBJECT\t{kind}\t{body}")?;
            }
            out.flush()?;
            Ok(())
        }
    }
}
/// The ledger's standing under this principal; the native `focal status`.
pub(super) fn status(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    format: OutputFormat,
) -> Result<()> {
    let page = read(
        runtime,
        context,
        &NativeReadOperation::Standing(NativeEmptyDocument::default()),
    )?;
    render_page(page, format)
}
fn get(runtime: &tokio::runtime::Runtime, context: &Context, command: GetCommand) -> Result<()> {
    let object = |id: String| NativeObjectDocument { id };
    let (operation, format) = match command {
        GetCommand::Claim(args) => {
            let Some(id) = args.id else {
                return Err(unavailable("filtered claim selection"));
            };
            (
                NativeReadOperation::ClaimGet(object(id)),
                args.output.format,
            )
        }
        GetCommand::Testament(args) => (
            NativeReadOperation::TestamentGet(object(args.id)),
            args.output.format,
        ),
        GetCommand::Artifact(args) => {
            if args.output.is_some() {
                return Err(unavailable("artifact payload download"));
            }
            (
                NativeReadOperation::ArtifactGet(object(args.id)),
                args.display.format,
            )
        }
        GetCommand::Validation(args) => {
            if args.context {
                let results_after = args
                    .cursor
                    .as_deref()
                    .map(|cursor| {
                        cursor.parse::<u64>().map_err(|_| {
                            CliError::Input(
                                "--cursor names the last seen result revision on the native engine"
                                    .into(),
                            )
                        })
                    })
                    .transpose()?;
                (
                    NativeReadOperation::ValidationContext(NativeContextDocument {
                        validation: args.id,
                        phase: args.phase.unwrap_or_else(|| "whole_work".into()),
                        slot: args.slot,
                        target: args.target,
                        generation: args.generation,
                        results_after,
                        limit: args.limit,
                    }),
                    args.output.format,
                )
            } else {
                (
                    NativeReadOperation::ValidationGet(object(args.id)),
                    args.output.format,
                )
            }
        }
    };
    let page = read(runtime, context, &operation)?;
    render_page(page, format)
}
