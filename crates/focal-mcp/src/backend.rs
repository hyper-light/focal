//! One owner performs journal filesystem work between asynchronous network waits.
use crate::ToolCall;
use focal_client::{
    Client, ClientError, ClientTransport,
    input::{BuildContext, InputError, MAX_INPUT_BYTES, parse_id},
    operations::{self, ApplicationResult, AuthoredOperation, OperationOutput, PlannedOperation},
    pending::{OperationContext, OperationJournal},
};
use focal_model::*;
use focal_wire::*;
use serde::Serialize;
use serde_json::Value;
use tokio::{runtime::Runtime, sync::oneshot};

// OperationStore owns the bounded catalogue and exact prepared requests. It is
// intentionally synchronous and lives only on this backend's OS thread.
use focal_client::managed_requests::{ManagedRequests, ManagedRequestsError};
use focal_client::managed_store::{ManagedOperationId, ManagedStoreError};
use focal_client::operation_store::{OperationIntent, OperationStore, StoreError};
#[path = "managed_backend.rs"]
mod managed;
#[path = "native_backend.rs"]
mod native;
#[path = "transfer_backend.rs"]
mod transfer;
#[path = "watch_backend.rs"]
mod watch;
pub use native::{JournalError, NativeJournal};

pub struct Backend<T: ClientTransport> {
    client: Client<T>,
    build: BuildContext,
    context: OperationContext,
    store: OperationStore,
    managed: Option<ManagedRequests>,
    admin: Option<Box<dyn crate::AdminBackend>>,
    uploads: Option<focal_client::artifact_transfer::UploadStore>,
    watches: Option<focal_client::watch::WatchStore>,
    native_journal: Option<Box<dyn NativeJournal>>,
    native: Option<native::Native>,
}
impl<T: ClientTransport> Backend<T> {
    pub fn new(
        client: Client<T>,
        build: BuildContext,
        context: OperationContext,
        store: OperationStore,
    ) -> Result<Self, InputError> {
        build.validate()?;
        if build.ledger != context.ledger
            || build.actor != context.principal
            || context.cluster == [0; 16]
        {
            return Err(InputError::Invalid("MCP backend context mismatch"));
        }
        Ok(Self {
            client,
            build,
            context,
            store,
            managed: None,
            admin: None,
            uploads: None,
            watches: None,
            native_journal: None,
            native: None,
        })
    }
    pub(crate) fn has_native(&self) -> bool {
        self.native.is_some()
    }
    /// Enable the managed namespace without changing legacy operation bindings.
    pub fn with_admin(mut self, admin: Box<dyn crate::AdminBackend>) -> Self {
        self.admin = Some(admin);
        self
    }
    pub(crate) fn has_admin(&self) -> bool {
        self.admin.is_some()
    }
    pub fn with_uploads(mut self, uploads: focal_client::artifact_transfer::UploadStore) -> Self {
        self.uploads = Some(uploads);
        self
    }
    pub(crate) fn has_uploads(&self) -> bool {
        self.uploads.is_some()
    }
    pub fn with_watches(
        mut self,
        watches: focal_client::watch::WatchStore,
    ) -> Result<Self, InputError> {
        if watches.context() != self.context {
            return Err(InputError::Invalid("watch backend context mismatch"));
        }
        self.watches = Some(watches);
        Ok(self)
    }
    pub(crate) fn has_watches(&self) -> bool {
        self.watches.is_some()
    }
    /// Enable the managed namespace without changing legacy operation bindings.
    pub fn with_managed_requests(mut self, requests: ManagedRequests) -> Result<Self, InputError> {
        if requests.context() != self.context {
            return Err(InputError::Invalid("managed backend context mismatch"));
        }
        self.managed = Some(requests);
        Ok(self)
    }
    pub(crate) fn execute(
        &mut self,
        runtime: &Runtime,
        call: &mut ToolCall,
        cancel: &mut oneshot::Receiver<()>,
    ) -> ApplicationResult {
        let mut operation_id = None;
        let result = self.execute_inner(runtime, call, cancel, &mut operation_id);
        // Native results share the application result shape at version 2.
        let schema_version = if self.has_native() { 2 } else { 1 };
        match result {
            Ok((condition, result)) => ApplicationResult {
                schema_version,
                operation_id,
                condition: condition.into(),
                result,
            },
            Err(BackendError::Domain(reply)) => ApplicationResult {
                schema_version,
                operation_id,
                condition: "DomainOutcome".into(),
                result: OperationOutput::Mutation { reply: *reply },
            },
            Err(BackendError::ManagedDomain(outcome)) => ApplicationResult {
                schema_version,
                operation_id,
                condition: "DomainOutcome".into(),
                result: OperationOutput::Mutation {
                    reply: MutationReply::Domain(*outcome),
                },
            },
            Err(BackendError::NativeRefused(refusal)) => ApplicationResult {
                schema_version,
                operation_id,
                condition: focal_client::failure::native(&refusal).condition.into(),
                result: OperationOutput::NativeRefused { refusal: *refusal },
            },
            Err(error) => ApplicationResult {
                schema_version,
                operation_id,
                condition: error.condition().into(),
                result: OperationOutput::Error {
                    code: error.code().into(),
                    detail: bounded_detail(&error),
                },
            },
        }
    }
    fn execute_inner(
        &mut self,
        runtime: &Runtime,
        call: &mut ToolCall,
        cancel: &mut oneshot::Receiver<()>,
        operation_id: &mut Option<String>,
    ) -> Result<(&'static str, OperationOutput), BackendError> {
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(BackendError::Configuration);
        }
        if cancelled(cancel) {
            return Err(BackendError::Cancelled);
        }
        // One registry names every surface; the adapter serves a surface only
        // with the standing it advertised, so a tool that was not listed is
        // refused here as well as at the protocol layer.
        match focal_client::operations::surface_of(&call.tool) {
            Some(focal_client::operations::Surface::Administration) => {
                let action = crate::admin::parse(&call.tool, std::mem::take(&mut call.arguments))?;
                let backend = self.admin.as_mut().ok_or(BackendError::Configuration)?;
                return match backend.execute(runtime, action, cancel) {
                    Ok(result) => {
                        Ok(("Administration", OperationOutput::Administration { result }))
                    }
                    Err(error) => Err(BackendError::Admin(error)),
                };
            }
            // Durable watches speak both engines: the journal saves the engine
            // it was created for and streams schema-2 deltas on a native ledger.
            Some(focal_client::operations::Surface::Watch) => {
                return self.watch(runtime, call, cancel);
            }
            Some(focal_client::operations::Surface::Transfer) => {
                if self.has_native() {
                    return Err(BackendError::Configuration);
                }
                return self.transfer(runtime, call, cancel);
            }
            Some(focal_client::operations::Surface::Application) | None => {}
        }
        if self.has_native() {
            // The native catalogue replaces the V1 application and transfer
            // tools; the owner refuses their wire profile anyway.
            return self.native_call(runtime, call, cancel, operation_id);
        }
        if matches!(
            call.tool.as_str(),
            "request.reserve" | "request.pending" | "request.acknowledge" | "request.seal"
        ) {
            return self.managed_control(runtime, call, cancel, operation_id);
        }
        if matches!(call.tool.as_str(), "request.inspect" | "request.retry") {
            let id = take_id(&mut call.arguments)?;
            *operation_id = Some(id.clone());
            let remote = if call.tool == "request.inspect" {
                call.arguments
                    .remove("remote")
                    .map(|value| {
                        value
                            .as_bool()
                            .ok_or(InputError::Invalid("remote must be a boolean"))
                    })
                    .transpose()?
                    .unwrap_or(false)
            } else {
                false
            };
            if !call.arguments.is_empty() {
                return Err(BackendError::Input(InputError::Invalid(
                    "unsupported recovery argument",
                )));
            }
            if id.starts_with("m1:") {
                return self.managed_recovery(
                    runtime,
                    &id,
                    call.tool == "request.retry",
                    remote,
                    cancel,
                );
            }
            let mut journal = self.store.open_existing(&id, &self.context)?;
            if remote {
                let business = journal.business_request()?;
                let reply = self.reconcile(
                    runtime,
                    ReconcileQuery::Receipt {
                        epoch: business.request_epoch,
                        request: business.request_id,
                    },
                    cancel,
                )?;
                if let ReconcileResult::Receipt { resolution, .. } = &reply.page.result {
                    match resolution {
                        ReceiptResolution::Committed(receipt) => {
                            journal.validate_business_receipt(receipt)?
                        }
                        ReceiptResolution::CommittedCursor(_) => {
                            return Err(focal_client::pending::PendingError::ReceiptMismatch.into());
                        }
                        ReceiptResolution::BelowFloor { .. } | ReceiptResolution::Unknown => {}
                    }
                }
                return Ok(("Reconciled", OperationOutput::Reconcile { reply }));
            }
            if call.tool == "request.retry" {
                self.drive(runtime, &mut journal, cancel)?;
            }
            return journal_result(&journal);
        }
        let descriptor = operations::find(&call.tool).ok_or(BackendError::Configuration)?;
        let expected_revision = if descriptor.mutation {
            *operation_id = Some(take_id(&mut call.arguments)?);
            call.arguments
                .remove("expected_revision")
                .map(|value| {
                    value
                        .as_u64()
                        .map(ObjectRevision)
                        .ok_or(BackendError::Input(InputError::Invalid(
                            "expected_revision must be an unsigned integer",
                        )))
                })
                .transpose()?
        } else {
            None
        };
        let bytes = bounded_json(&call.arguments)?;
        let authored = operations::parse_json(&call.tool, &bytes)?;
        if descriptor.mutation {
            let canonical = authored.canonical_mutation_intent(expected_revision)?;
            let id = operation_id.as_deref().ok_or(BackendError::Configuration)?;
            if id.starts_with("m1:") {
                authored.preflight(&self.build)?;
                return self.managed_mutation(
                    runtime,
                    id,
                    authored,
                    expected_revision,
                    &canonical,
                    cancel,
                );
            }
            let build = self.build;
            let expected_revision = if expected_revision.is_none() {
                if let Some(claim) = authored.revision_claim()? {
                    match self.store.open_existing(id, &self.context) {
                        Ok(journal) => match &journal.business_request()?.operation {
                            Operation::Submit {
                                expected_revision, ..
                            } => *expected_revision,
                            _ => return Err(StoreError::IntentConflict.into()),
                        },
                        Err(StoreError::MissingOperation) => Some(self.participant_revision(
                            runtime,
                            claim,
                            RequestId(random_id()?),
                            cancel,
                        )?),
                        Err(error) => return Err(error.into()),
                    }
                } else {
                    expected_revision
                }
            } else {
                expected_revision
            };
            let mut journal = self.store.open_or_create(
                id,
                self.context,
                OperationIntent {
                    name: descriptor.name,
                    version: descriptor.version,
                    canonical: &canonical,
                },
                || {
                    let planned = authored
                        .build(&build, &mut random_id)
                        .map_err(StoreError::Expansion)?;
                    let operation = planned
                        .into_wire(expected_revision)
                        .map_err(StoreError::Expansion)?;
                    let request =
                        envelope(build.ledger, operation).map_err(StoreError::Expansion)?;
                    let epoch = envelope(
                        build.ledger,
                        Operation::OpenEpoch {
                            epoch: RequestEpoch(1),
                        },
                    )
                    .map_err(StoreError::Expansion)?;
                    Ok((epoch, request))
                },
            )?;
            self.drive(runtime, &mut journal, cancel)?;
            journal_result(&journal)
        } else {
            let planned = authored.build(&self.build, &mut random_id)?;
            if let PlannedOperation::Reconcile(query) = planned {
                let reply = self.reconcile(runtime, query, cancel)?;
                return Ok(("Reconciled", OperationOutput::Reconcile { reply }));
            }
            if let PlannedOperation::ClaimWait {
                read,
                until,
                timeout_ms,
            } = planned
            {
                let request = envelope(self.build.ledger, Operation::Read(read))?;
                let result=runtime.block_on(async { tokio::select! {
                    result=self.client.claim_wait(request,until,std::time::Duration::from_millis(u64::from(timeout_ms)))=>result.map_err(claim_wait_error),
                    _=cancel=>Err(BackendError::Cancelled),
                }})?;
                return Ok((
                    result.condition.as_str(),
                    OperationOutput::ClaimWait { result },
                ));
            }
            if let PlannedOperation::ClaimGet(selector) = planned {
                let request = envelope(self.build.ledger, selector.into_operation())?;
                let page=runtime.block_on(async {tokio::select! {
                    result=self.client.claim_get(request)=>result.map_err(BackendError::ClaimGet),
                    _=cancel=>Err(BackendError::Cancelled),
                }})?;
                return Ok(("Read", OperationOutput::Read { page }));
            }
            if let PlannedOperation::ValidationContext(read) = planned {
                let request = envelope(self.build.ledger, Operation::Read(read))?;
                let context = runtime.block_on(async {
                    tokio::select! {
                        result=self.client.validation_context(request)=>result.map_err(validation_context_error),
                        _=cancel=>Err(BackendError::Cancelled),
                    }
                })?;
                return Ok((
                    "Read",
                    OperationOutput::ValidationContext {
                        context: Box::new(context),
                    },
                ));
            }
            let request = envelope(self.build.ledger, planned.into_wire(None)?)?;
            let reply = runtime.block_on(async {
                tokio::select! {
                    result=self.client.request(request)=>result.map_err(BackendError::Client),
                    _=cancel=>Err(BackendError::Cancelled),
                }
            })?;
            match reply.result {
                Response::Monitor(page) => {
                    let Some(monitor) = &page.monitor else {
                        return Err(BackendError::NotFound);
                    };
                    Ok((
                        if monitor.released.is_some() {
                            "Released"
                        } else {
                            "Pending"
                        },
                        OperationOutput::Monitor { page },
                    ))
                }
                Response::Summary(summary) => {
                    Ok(("Observed", OperationOutput::Summary { summary }))
                }
                Response::Read(page) => {
                    if page.objects.is_empty() {
                        return Err(BackendError::NotFound);
                    }
                    Ok(("Read", OperationOutput::Read { page }))
                }
                Response::Listed(page) => Ok(("Listed", OperationOutput::List { page })),
                Response::Validators(page) => {
                    Ok(("RecordedContracts", OperationOutput::List { page }))
                }
                Response::Traversed(page) => Ok(("Traversed", OperationOutput::Traversal { page })),
                _ => Err(BackendError::Configuration),
            }
        }
    }
    fn participant_revision(
        &self,
        runtime: &Runtime,
        claim: ClaimId,
        nonce: RequestId,
        cancel: &mut oneshot::Receiver<()>,
    ) -> Result<ObjectRevision, BackendError> {
        runtime.block_on(async {
            tokio::select! {
                result = self.client.claim_revision(self.context.ledger, claim, nonce) => {
                    result?.ok_or_else(|| InputError::Invalid("claim was not found").into())
                },
                _ = cancel => Err(BackendError::Cancelled),
            }
        })
    }
    fn reconcile(
        &self,
        runtime: &Runtime,
        query: ReconcileQuery,
        cancel: &mut oneshot::Receiver<()>,
    ) -> Result<ReconcileReply, BackendError> {
        let request = envelope(self.build.ledger, Operation::Reconcile(query))?;
        runtime.block_on(async {
            tokio::select! {
                result=self.client.reconcile(request, self.context.principal)=>result.map_err(BackendError::Client),
                _=cancel=>Err(BackendError::Cancelled),
            }
        })
    }
    fn drive(
        &self,
        runtime: &Runtime,
        journal: &mut OperationJournal,
        cancel: &mut oneshot::Receiver<()>,
    ) -> Result<(), BackendError> {
        while let Some(request) = journal.next_request()?.cloned() {
            if cancelled(cancel) {
                return Err(BackendError::Cancelled);
            }
            let reply = runtime.block_on(async {
                tokio::select! {
                    result=self.client.submit(request)=>result.map_err(BackendError::Client),
                    _=&mut *cancel=>Err(BackendError::Cancelled),
                }
            })?;
            match &reply {
                MutationReply::Committed(_)
                | MutationReply::Domain(DomainOutcome::Duplicate(_)) => {
                    journal.record_reply(&reply)?;
                }
                _ => return Err(BackendError::Domain(Box::new(reply))),
            }
        }
        Ok(())
    }
}
fn claim_wait_error(error: focal_client::claim_wait::ClaimWaitError) -> BackendError {
    use focal_client::claim_wait::ClaimWaitError;
    match error {
        ClaimWaitError::Client(error) => BackendError::Client(error),
        ClaimWaitError::NotFound => BackendError::NotFound,
        ClaimWaitError::InvalidRequest => {
            BackendError::Input(InputError::Invalid("invalid claim wait query"))
        }
    }
}
fn validation_context_error(
    error: focal_client::validation_context::ValidationContextError,
) -> BackendError {
    use focal_client::validation_context::ValidationContextError;
    match error {
        ValidationContextError::Client(error) => BackendError::Client(error),
        ValidationContextError::NotFound => BackendError::NotFound,
        ValidationContextError::InvalidRequest => {
            BackendError::Input(InputError::Invalid("validation context query"))
        }
        ValidationContextError::Capacity => BackendError::Input(InputError::Capacity),
    }
}
fn native_drive(error: &focal_native_client::DriveError) -> focal_client::failure::Failure {
    use focal_client::failure::{self, Failure};
    use focal_native_client::{CompileError, DriveError};
    match error {
        DriveError::Compile(error) => match error {
            CompileError::Input(error) => failure::input(error),
            CompileError::Contract(_) => Failure::error("invalid_input", 2),
            CompileError::Capacity(_) => Failure::error("capacity", 6),
            CompileError::Codec(_) => Failure::error("native_frame", 1),
            CompileError::Missing(_) => Failure::error("not_found", 4),
            CompileError::Unsupported(_) => Failure::error("operation_conflict", 5),
        },
        DriveError::Store(error) => failure::native_store(error),
        DriveError::Client(error) => failure::client(error),
        DriveError::Input(error) => failure::input(error),
        DriveError::ProjectionOnly => Failure::error("invalid_input", 2),
        DriveError::Cancelled => Failure::cancelled(),
    }
}
fn take_id(arguments: &mut serde_json::Map<String, Value>) -> Result<String, BackendError> {
    let Some(Value::String(id)) = arguments.remove("operation_id") else {
        return Err(InputError::Invalid("operation_id is required").into());
    };
    if id.starts_with("m1:") {
        id.parse::<ManagedOperationId>()?;
        return Ok(id);
    }
    parse_id(&id)?;
    if id.bytes().any(|byte| byte.is_ascii_uppercase()) {
        return Err(InputError::Invalid("operation_id must use lowercase hexadecimal").into());
    }
    Ok(id)
}
fn journal_result(
    journal: &OperationJournal,
) -> Result<(&'static str, OperationOutput), BackendError> {
    match journal.receipt() {
        Some(receipt) => Ok((
            "Committed",
            OperationOutput::Mutation {
                reply: MutationReply::Committed(receipt.clone()),
            },
        )),
        None => {
            let request = journal.next_request()?.ok_or(BackendError::Configuration)?;
            Ok((
                "Pending",
                OperationOutput::Mutation {
                    reply: MutationReply::Pending(RequestKey {
                        principal: journal.context().principal,
                        epoch: request.request_epoch,
                        id: request.request_id,
                    }),
                },
            ))
        }
    }
}
fn cancelled(cancel: &mut oneshot::Receiver<()>) -> bool {
    !matches!(cancel.try_recv(), Err(oneshot::error::TryRecvError::Empty))
}
fn random_id() -> Result<[u8; 16], InputError> {
    let mut bytes = [0; 16];
    getrandom::fill(&mut bytes).map_err(|_| InputError::Identity)?;
    if bytes == [0; 16] {
        return Err(InputError::Identity);
    }
    Ok(bytes)
}
fn envelope(ledger: LedgerId, operation: Operation) -> Result<RequestEnvelope, InputError> {
    Ok(RequestEnvelope {
        protocol: participant_protocol(&operation),
        ledger,
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId(random_id()?),
        operation,
    })
}
fn bounded_json(value: &impl Serialize) -> Result<Vec<u8>, InputError> {
    let mut writer = crate::codec::BoundedWriter {
        bytes: Vec::new(),
        limit: MAX_INPUT_BYTES,
    };
    serde_json::to_writer(&mut writer, value).map_err(|_| InputError::Capacity)?;
    Ok(writer.bytes)
}
fn bounded_detail(error: &impl std::fmt::Display) -> String {
    use std::fmt::Write;
    const MAX: usize = 16 * 1024;
    const SUFFIX: &str = " [truncated]";
    struct Detail(String);
    impl std::fmt::Write for Detail {
        fn write_str(&mut self, text: &str) -> std::fmt::Result {
            let left = MAX
                .checked_sub(SUFFIX.len())
                .and_then(|limit| limit.checked_sub(self.0.len()))
                .ok_or(std::fmt::Error)?;
            if text.len() > left {
                let end = text
                    .char_indices()
                    .map(|(index, _)| index)
                    .take_while(|index| *index <= left)
                    .last()
                    .unwrap_or(0);
                self.0.push_str(text.get(..end).ok_or(std::fmt::Error)?);
                return Err(std::fmt::Error);
            }
            self.0.push_str(text);
            Ok(())
        }
    }
    let mut detail = Detail(String::new());
    if detail.0.try_reserve_exact(MAX).is_err() {
        return "error detail unavailable".into();
    }
    if write!(&mut detail, "{error}").is_err() {
        detail.0.push_str(SUFFIX);
    }
    detail.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oversized_unicode_diagnostic_remains_bounded_valid_utf8() {
        let detail = super::bounded_detail(&"é".repeat(20_000));
        assert!(detail.len() <= 16 * 1024);
        assert!(detail.ends_with(" [truncated]"));
        assert!(!detail.contains('\u{fffd}'));
    }

    #[test]
    fn nested_store_errors_keep_actionable_codes() {
        fn check(make: impl Fn() -> StoreError, code: &str) {
            for error in [
                BackendError::Store(make()),
                BackendError::ManagedStore(ManagedStoreError::Store(make())),
                BackendError::Managed(ManagedRequestsError::Store(make())),
                BackendError::Managed(ManagedRequestsError::Managed(ManagedStoreError::Store(
                    make(),
                ))),
            ] {
                assert_eq!((error.condition(), error.code()), ("Error", code));
            }
        }
        check(|| StoreError::Locked, "busy");
        check(|| StoreError::Capacity, "capacity");
        check(|| StoreError::InvalidId, "invalid_input");
        check(|| StoreError::MissingOperation, "not_found");
        check(|| StoreError::IntentConflict, "operation_conflict");
        check(|| StoreError::Corrupt, "operation_store");
        check(
            || StoreError::Expansion(InputError::Identity),
            "invalid_input",
        );
        check(|| StoreError::Expansion(InputError::Capacity), "capacity");
        check(
            || StoreError::Pending(focal_client::pending::PendingError::Locked),
            "busy",
        );
    }

    #[test]
    fn coordinator_failures_preserve_retirement_capacity_and_identity() {
        for (error, condition, code) in [
            (
                ManagedRequestsError::Managed(ManagedStoreError::Retired),
                "Retired",
                "managed_retired",
            ),
            (
                ManagedRequestsError::Managed(ManagedStoreError::Capacity),
                "Error",
                "capacity",
            ),
            (ManagedRequestsError::Exhausted, "Error", "capacity"),
            (
                ManagedRequestsError::Identity(InputError::Identity),
                "Error",
                "invalid_input",
            ),
            (ManagedRequestsError::Context, "Error", "operation_conflict"),
            (ManagedRequestsError::Missing, "Error", "not_found"),
            (ManagedRequestsError::Corrupt, "Error", "managed_requests"),
        ] {
            let error = BackendError::Managed(error);
            assert_eq!((error.condition(), error.code()), (condition, code));
        }
    }

    #[test]
    fn coordinator_remote_rejections_match_direct_client_access() {
        for (access, condition, code) in [
            (AccessError::Unauthorized, "Error", "unauthorized"),
            (AccessError::InvalidRequest, "Error", "invalid_input"),
            (AccessError::Capacity, "Error", "capacity"),
            (AccessError::Unavailable, "Error", "unavailable"),
            (
                AccessError::OutcomeUnknown,
                "OutcomeUnknown",
                "outcome_unknown",
            ),
            (
                AccessError::ManagedRetired { through: 7 },
                "Retired",
                "managed_retired",
            ),
            (
                AccessError::ManagedClosed { generation: 3 },
                "Error",
                "managed_closed",
            ),
            (AccessError::ManagedConflict, "Error", "managed_conflict"),
            (
                AccessError::ManagedNotRegistered,
                "Error",
                "managed_not_registered",
            ),
        ] {
            for error in [
                BackendError::Client(ClientError::Access(access.clone())),
                BackendError::Managed(ManagedRequestsError::Remote(access)),
            ] {
                assert_eq!((error.condition(), error.code()), (condition, code));
            }
        }
    }
}
#[derive(Debug, thiserror::Error)]
enum BackendError {
    #[error(transparent)]
    ClaimGet(#[from] focal_client::claim_get::ClaimGetError),
    #[error(transparent)]
    Watch(#[from] focal_client::watch::WatchError),
    #[error(transparent)]
    Transfer(#[from] focal_client::artifact_transfer::TransferError),
    #[error(transparent)]
    Admin(#[from] crate::AdminError),
    #[error(transparent)]
    Input(#[from] InputError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Managed(#[from] ManagedRequestsError),
    #[error(transparent)]
    ManagedStore(#[from] ManagedStoreError),
    #[error(transparent)]
    Journal(#[from] focal_client::pending::PendingError),
    #[error(transparent)]
    Client(#[from] ClientError),
    #[error("client wait cancelled; saved mutations remain recoverable")]
    Cancelled,
    #[error("object not found at the observed prefix")]
    NotFound,
    #[error("invalid backend context or response")]
    Configuration,
    #[error("domain outcome; exact request remains saved: {0:?}")]
    Domain(Box<MutationReply>),
    #[error("domain outcome; exact managed request remains saved: {0:?}")]
    ManagedDomain(Box<DomainOutcome>),
    #[error(transparent)]
    NativeStore(#[from] focal_client::native_store::NativeStoreError),
    #[error(transparent)]
    NativeDrive(#[from] focal_native_client::DriveError),
    #[error("native journal unavailable: {0}")]
    NativeJournal(JournalError),
    #[error("the native owner refused the frame: {}", .0.detail)]
    NativeRefused(Box<NativeRefusal>),
    #[error("the native owner holds the frame as a pending candidate; retry the exact operation")]
    NativePending(NativeTicket),
}
impl BackendError {
    fn condition(&self) -> &'static str {
        self.classification().0
    }
    fn code(&self) -> &'static str {
        self.classification().1
    }
    fn classification(&self) -> (&'static str, &'static str) {
        use focal_client::failure::{self, Failure};
        let value = match self {
            Self::ClaimGet(error) => match error {
                focal_client::claim_get::ClaimGetError::Client(error) => failure::client(error),
                focal_client::claim_get::ClaimGetError::NotFound => Failure::error("not_found", 4),
                focal_client::claim_get::ClaimGetError::Ambiguous => Failure::error("ambiguous", 5),
                focal_client::claim_get::ClaimGetError::Incomplete => Failure {
                    condition: "Incomplete",
                    code: "incomplete",
                    exit_code: 6,
                },
                focal_client::claim_get::ClaimGetError::InvalidRequest => {
                    Failure::error("invalid_input", 2)
                }
            },
            Self::Watch(error) => failure::watch(error),
            Self::Transfer(error) => failure::transfer(error),
            Self::Input(error) => failure::input(error),
            Self::Store(error) => failure::store(error),
            Self::Managed(error) => failure::managed(error),
            Self::ManagedStore(error) => failure::managed_store(error),
            Self::Journal(error) => failure::pending(error),
            Self::Client(error) => failure::client(error),
            Self::Cancelled => Failure::cancelled(),
            Self::NotFound => Failure::error("not_found", 4),
            Self::Configuration => Failure::error("configuration", 2),
            Self::Admin(error) => return (error.condition, error.code),
            Self::Domain(_) | Self::ManagedDomain(_) => return ("DomainOutcome", "domain"),
            Self::NativeStore(error) => failure::native_store(error),
            Self::NativeDrive(error) => native_drive(error),
            Self::NativeJournal(_) => Failure::error("native_store", 1),
            Self::NativeRefused(refusal) => failure::native(refusal),
            Self::NativePending(_) => Failure::outcome_unknown(),
        };
        (value.condition, value.code)
    }
}
