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
use focal_client::operation_store::{OperationIntent, OperationStore, StoreError};

pub struct Backend<T: ClientTransport> {
    client: Client<T>,
    build: BuildContext,
    context: OperationContext,
    store: OperationStore,
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
        })
    }
    pub(crate) fn execute(
        &mut self,
        runtime: &Runtime,
        call: &mut ToolCall,
        cancel: &mut oneshot::Receiver<()>,
    ) -> ApplicationResult {
        let mut operation_id = None;
        let result = self.execute_inner(runtime, call, cancel, &mut operation_id);
        match result {
            Ok((condition, result)) => ApplicationResult {
                schema_version: 1,
                operation_id,
                condition: condition.into(),
                result,
            },
            Err(BackendError::Domain(reply)) => ApplicationResult {
                schema_version: 1,
                operation_id,
                condition: "DomainOutcome".into(),
                result: OperationOutput::Mutation { reply: *reply },
            },
            Err(error) => ApplicationResult {
                schema_version: 1,
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
            let canonical = bounded_json(&IntentFence {
                revision: expected_revision,
                authored: &authored,
            })?;
            let id = operation_id.as_deref().ok_or(BackendError::Configuration)?;
            let build = self.build;
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
            let request = envelope(self.build.ledger, planned.into_wire(None)?)?;
            let reply = runtime.block_on(async {
                tokio::select! {
                    result=self.client.request(request)=>result.map_err(BackendError::Client),
                    _=cancel=>Err(BackendError::Cancelled),
                }
            })?;
            match reply.result {
                Response::Read(page) => {
                    if page.objects.is_empty() {
                        return Err(BackendError::NotFound);
                    }
                    Ok(("Read", OperationOutput::Read { page }))
                }
                Response::Listed(page) => Ok(("Listed", OperationOutput::List { page })),
                _ => Err(BackendError::Configuration),
            }
        }
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
#[derive(Serialize)]
struct IntentFence<'a> {
    revision: Option<ObjectRevision>,
    authored: &'a AuthoredOperation,
}
fn take_id(arguments: &mut serde_json::Map<String, Value>) -> Result<String, BackendError> {
    let Some(Value::String(id)) = arguments.remove("operation_id") else {
        return Err(InputError::Invalid("operation_id is required").into());
    };
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
        protocol: PROTOCOL_VERSION,
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
    #[test]
    fn oversized_unicode_diagnostic_remains_bounded_valid_utf8() {
        let detail = super::bounded_detail(&"é".repeat(20_000));
        assert!(detail.len() <= 16 * 1024);
        assert!(detail.ends_with(" [truncated]"));
        assert!(!detail.contains('\u{fffd}'));
    }
}
#[derive(Debug, thiserror::Error)]
enum BackendError {
    #[error(transparent)]
    Input(#[from] InputError),
    #[error(transparent)]
    Store(#[from] StoreError),
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
}
impl BackendError {
    fn condition(&self) -> &'static str {
        match self {
            Self::Client(ClientError::OutcomeUnknown { .. }) => "OutcomeUnknown",
            Self::Cancelled => "Cancelled",
            Self::Domain(_) => "DomainOutcome",
            _ => "Error",
        }
    }
    fn code(&self) -> &'static str {
        match self {
            Self::Input(_) => "invalid_input",
            Self::Store(StoreError::IntentConflict | StoreError::ContextMismatch) => {
                "operation_conflict"
            }
            Self::Store(StoreError::Locked)
            | Self::Journal(focal_client::pending::PendingError::Locked) => "busy",
            Self::Store(StoreError::Capacity) => "capacity",
            Self::Store(StoreError::MissingOperation) => "not_found",
            Self::Store(_) => "operation_store",
            Self::Journal(_) => "operation_journal",
            Self::Client(ClientError::Access(access)) => match access {
                AccessError::Unauthorized => "unauthorized",
                AccessError::UnsupportedProtocol => "unsupported_protocol",
                AccessError::InvalidRequest => "invalid_input",
                AccessError::Capacity => "capacity",
                AccessError::Unavailable => "unavailable",
                AccessError::OutcomeUnknown => "outcome_unknown",
                AccessError::RouteChanged(_) => "route_changed",
                AccessError::Behind { .. } => "behind",
                AccessError::SnapshotExpired => "snapshot_expired",
                AccessError::ResyncRequired { .. } => "resync_required",
                AccessError::UnsupportedOperation => "unsupported_operation",
                AccessError::ManagedRetired { .. } => "managed_retired",
                AccessError::ManagedClosed { .. } => "managed_closed",
                AccessError::ManagedConflict => "managed_conflict",
                AccessError::ManagedNotRegistered => "managed_not_registered",
            },
            Self::Client(ClientError::OutcomeUnknown { .. }) => "outcome_unknown",
            Self::Client(_) => "transport",
            Self::Cancelled => "cancelled",
            Self::NotFound => "not_found",
            Self::Configuration => "configuration",
            Self::Domain(_) => "domain",
        }
    }
}
