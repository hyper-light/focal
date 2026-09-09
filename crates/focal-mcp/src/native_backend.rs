//! The native engine path of the MCP adapter. The engine probe runs once on
//! the worker's runtime before the catalogue is built; on a native ledger
//! every application tool compiles its document through the shared driver,
//! journals the exact frame under an `n1:` reference in this adapter's own
//! journal and drives it to a committed receipt, a closed refusal or a
//! pending outcome the recovery tools resolve. Results are acknowledged
//! explicitly, as managed results are, so a reply lost between the adapter
//! and the agent stays listed by `request.pending`.
use super::{BackendError, bounded_json, cancelled, envelope, random_id};
use crate::{Backend, ToolCall};
use focal_client::input::InputError;
use focal_client::native_store::{NativeOperation, NativeOperationId, NativeOperationStore};
use focal_client::operations::{
    OperationOutput, find_native, parse_native_json, parse_native_list_json, parse_native_read_json,
};
use focal_client::{ClientError, ClientTransport};
use focal_native_client::{CompileLimits, DriveError, NativeContentProfile, Preparation};
use focal_wire::*;
use serde_json::Value;
use tokio::{runtime::Runtime, sync::oneshot};

/// How the host opens this adapter's native journal once the probe finds a
/// native ledger. The host owns the directory layout and its markers.
pub trait NativeJournal: Send {
    /// Whether the journal was created by an earlier run of this adapter.
    fn initialized(&self) -> bool;
    /// Open the journal, creating it on first use.
    fn open(self: Box<Self>) -> Result<NativeOperationStore, JournalError>;
}
pub type JournalError = Box<dyn std::error::Error + Send + Sync>;

pub(crate) struct Native {
    store: NativeOperationStore,
    standing: NativeStanding,
    limits: CompileLimits,
}

impl<T: ClientTransport> Backend<T> {
    /// Register the native journal; the engine probe at startup decides
    /// whether it is opened. Without a registration the adapter never probes
    /// and serves the V1 catalogue.
    pub fn with_native_journal(mut self, journal: Box<dyn NativeJournal>) -> Self {
        self.native_journal = Some(journal);
        self
    }
    /// The engine probe. Runs on the worker's runtime because a remote
    /// transport binds its endpoint to the first runtime that drives it.
    pub(crate) fn detect(&mut self, runtime: &Runtime) -> Result<(), String> {
        self.detect_inner(runtime)
            .map_err(|error| error.to_string())
    }
    fn detect_inner(&mut self, runtime: &Runtime) -> Result<(), BackendError> {
        let Some(journal) = self.native_journal.take() else {
            return Ok(());
        };
        let request = envelope(
            self.build.ledger,
            Operation::NativeRead(NativeReadRequest {
                consistency: ReadConsistency::Linearizable,
                query: NativeReadQuery::Standing,
                max_items: 1,
            }),
        )?;
        let standing = match runtime.block_on(self.client.native_standing(request)) {
            Ok(standing) => standing,
            // An unreachable owner leaves the engine unknown. A journal from
            // an earlier run proves the ledger is native, so the adapter
            // refuses to start as V1 and mint identities in the wrong
            // namespace; a fresh context keeps the V1 behaviour, whose
            // startup never needed the network.
            Err(ClientError::Transport) if !journal.initialized() => None,
            Err(error) => return Err(error.into()),
        };
        if let Some(standing) = standing {
            if standing.principal != self.context.principal {
                return Err(BackendError::Configuration);
            }
            let store = journal.open().map_err(BackendError::NativeJournal)?;
            self.native = Some(Native {
                store,
                standing,
                limits: CompileLimits::default(),
            });
        }
        Ok(())
    }
    pub(crate) fn native_standing(&self) -> Option<&NativeStanding> {
        self.native.as_ref().map(|native| &native.standing)
    }
    fn native_ref(&self) -> Result<&Native, BackendError> {
        self.native.as_ref().ok_or(BackendError::Configuration)
    }
    /// One blocking linearizable read on this adapter's connection; the
    /// client's cancellation drops only the wait.
    fn native_reads<'a>(
        &'a self,
        runtime: &'a Runtime,
        cancel: &'a mut oneshot::Receiver<()>,
    ) -> impl FnMut(NativeReadRequest) -> Result<NativeReadPage, DriveError> + 'a {
        move |read| {
            let request = envelope(self.build.ledger, Operation::NativeRead(read))?;
            runtime.block_on(async {
                tokio::select! {
                    result = self.client.native_read(request) => Ok(result?),
                    _ = &mut *cancel => Err(DriveError::Cancelled),
                }
            })
        }
    }
    /// One blocking bounded list on this adapter's connection.
    fn native_lists<'a>(
        &'a self,
        runtime: &'a Runtime,
        cancel: &'a mut oneshot::Receiver<()>,
    ) -> impl FnMut(NativeListRequest) -> Result<NativeListPage, DriveError> + 'a {
        move |list| {
            let request = envelope(self.build.ledger, Operation::NativeList(list))?;
            runtime.block_on(async {
                tokio::select! {
                    result = self.client.native_list(request) => Ok(result?),
                    _ = &mut *cancel => Err(DriveError::Cancelled),
                }
            })
        }
    }
    pub(super) fn native_call(
        &self,
        runtime: &Runtime,
        call: &mut ToolCall,
        cancel: &mut oneshot::Receiver<()>,
        operation_id: &mut Option<String>,
    ) -> Result<(&'static str, OperationOutput), BackendError> {
        let native = self.native_ref()?;
        match call.tool.as_str() {
            "request.pending" => {
                if !call.arguments.is_empty() {
                    return Err(InputError::Invalid("unsupported recovery argument").into());
                }
                let ids = native.store.outstanding()?;
                let mut operation_ids = Vec::new();
                operation_ids
                    .try_reserve_exact(ids.len())
                    .map_err(|_| InputError::Capacity)?;
                for id in ids {
                    operation_ids.push(id.to_string());
                }
                Ok((
                    "Outstanding",
                    OperationOutput::ManagedRequests { operation_ids },
                ))
            }
            "request.acknowledge" => {
                let id = take_native_id(&mut call.arguments)?
                    .ok_or(InputError::Invalid("operation_id is required"))?;
                *operation_id = Some(id.to_string());
                if !call.arguments.is_empty() {
                    return Err(InputError::Invalid("unsupported recovery argument").into());
                }
                native.store.record_delivered(id, &self.context)?;
                Ok(state("Consumed"))
            }
            "request.inspect" | "request.retry" => {
                let id = take_native_id(&mut call.arguments)?
                    .ok_or(InputError::Invalid("operation_id is required"))?;
                *operation_id = Some(id.to_string());
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
                    return Err(InputError::Invalid("unsupported recovery argument").into());
                }
                if remote {
                    let page = focal_native_client::outcome(
                        id.key(&self.context),
                        &mut self.native_reads(runtime, cancel),
                    )?;
                    return Ok((
                        "Observed",
                        OperationOutput::NativeRead {
                            page: Box::new(page),
                        },
                    ));
                }
                let operation = native.store.retry(id, &self.context)?;
                if call.tool == "request.retry" {
                    return self.native_drive(runtime, native, operation, cancel);
                }
                match (operation.receipt, operation.refusal) {
                    (Some(receipt), _) => Ok(committed(receipt, operation.created)),
                    (None, Some(refusal)) => Err(BackendError::NativeRefused(Box::new(refusal))),
                    (None, None) => Ok(state("Pending")),
                }
            }
            name => {
                let descriptor = find_native(name)
                    .filter(|descriptor| {
                        crate::catalog_native::permitted(&native.standing, descriptor)
                    })
                    .ok_or(BackendError::Configuration)?;
                if descriptor.result_kind == focal_client::operations::ResultKind::List {
                    let bytes = bounded_json(&call.arguments)?;
                    let list = parse_native_list_json(name, &bytes)?;
                    let page = focal_native_client::list(
                        &list,
                        &self.build,
                        &mut self.native_lists(runtime, cancel),
                    )?;
                    return Ok((
                        "Listed",
                        OperationOutput::NativeList {
                            page: Box::new(page),
                        },
                    ));
                }
                if !descriptor.mutation {
                    let bytes = bounded_json(&call.arguments)?;
                    let read = parse_native_read_json(name, &bytes)?;
                    // The wait observer and the lineage read share this
                    // adapter's connection; the pause between wait probes is
                    // cancellable like every other wait of this adapter.
                    let outcome = {
                        let cell = std::cell::RefCell::new(&mut *cancel);
                        let mut reads = |request: NativeReadRequest| {
                            let request =
                                envelope(self.build.ledger, Operation::NativeRead(request))?;
                            let mut cancel = cell.borrow_mut();
                            runtime.block_on(async {
                                tokio::select! {
                                    result = self.client.native_read(request) => Ok(result?),
                                    _ = &mut **cancel => Err(DriveError::Cancelled),
                                }
                            })
                        };
                        let mut lists = |request: NativeListRequest| {
                            let request =
                                envelope(self.build.ledger, Operation::NativeList(request))?;
                            let mut cancel = cell.borrow_mut();
                            runtime.block_on(async {
                                tokio::select! {
                                    result = self.client.native_list(request) => Ok(result?),
                                    _ = &mut **cancel => Err(DriveError::Cancelled),
                                }
                            })
                        };
                        let mut pause = |duration: std::time::Duration| {
                            let mut cancel = cell.borrow_mut();
                            runtime.block_on(async {
                                tokio::select! {
                                    () = tokio::time::sleep(duration) => Ok(()),
                                    _ = &mut **cancel => Err(DriveError::Cancelled),
                                }
                            })
                        };
                        focal_native_client::read(
                            &read,
                            &self.build,
                            &mut reads,
                            &mut lists,
                            &mut pause,
                        )?
                    };
                    return Ok(match outcome {
                        focal_native_client::NativeReadOutcome::Page(page) => (
                            "Read",
                            OperationOutput::NativeRead {
                                page: Box::new(page),
                            },
                        ),
                        focal_native_client::NativeReadOutcome::Wait(result) => (
                            result.condition.as_str(),
                            OperationOutput::NativeWait { result },
                        ),
                    });
                }
                let requested = take_native_id(&mut call.arguments)?;
                let bytes = bounded_json(&call.arguments)?;
                let authored = parse_native_json(name, &bytes)?;
                if cancelled(cancel) {
                    return Err(BackendError::Cancelled);
                }
                let prepared = focal_native_client::prepare(
                    &Preparation {
                        store: &native.store,
                        context: self.context,
                        build: &self.build,
                        profile: profile(&native.standing),
                        limits: &native.limits,
                    },
                    &authored,
                    requested,
                    &mut random_id,
                    &mut self.native_reads(runtime, cancel),
                )?;
                *operation_id = Some(prepared.id.to_string());
                self.native_drive(runtime, native, prepared, cancel)
            }
        }
    }
    /// Send the exact journaled frame until the owner commits it. Only a
    /// committed receipt bound to the frame advances the journal; a refusal
    /// is recorded as reported and a pending ticket leaves the frame listed.
    fn native_drive(
        &self,
        runtime: &Runtime,
        native: &Native,
        operation: NativeOperation,
        cancel: &mut oneshot::Receiver<()>,
    ) -> Result<(&'static str, OperationOutput), BackendError> {
        if let Some(receipt) = operation.receipt {
            return Ok(committed(receipt, operation.created));
        }
        let reply = runtime.block_on(async {
            tokio::select! {
                result = self.client.submit_native(operation.request.clone()) => result.map_err(BackendError::Client),
                _ = &mut *cancel => Err(BackendError::Cancelled),
            }
        })?;
        match reply {
            NativeMutationReply::Committed(receipt) => {
                native
                    .store
                    .record_reply(operation.id, &self.context, &reply)?;
                Ok(committed(receipt, operation.created))
            }
            NativeMutationReply::Pending(ticket) => Err(BackendError::NativePending(ticket)),
            NativeMutationReply::Refused(refusal) => {
                native
                    .store
                    .record_refusal(operation.id, &self.context, &refusal)?;
                Err(BackendError::NativeRefused(Box::new(refusal)))
            }
        }
    }
}

fn committed(
    receipt: NativeReceipt,
    created: Vec<focal_client::native_store::NativeIdentity>,
) -> (&'static str, OperationOutput) {
    ("Committed", OperationOutput::Native { receipt, created })
}
fn state(value: &'static str) -> (&'static str, OperationOutput) {
    (
        value,
        OperationOutput::ManagedRequest {
            state: value.into(),
        },
    )
}
fn profile(standing: &NativeStanding) -> NativeContentProfile {
    match standing.profile {
        NativeProfile::ProjectionOnly => NativeContentProfile::ProjectionOnly,
        NativeProfile::AuthoredV1 => NativeContentProfile::AuthoredV1,
    }
}
/// An optional `n1:` reference; a present value must parse exactly.
fn take_native_id(
    arguments: &mut serde_json::Map<String, Value>,
) -> Result<Option<NativeOperationId>, BackendError> {
    match arguments.remove("operation_id") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(id)) => {
            if id.bytes().any(|byte| byte.is_ascii_uppercase()) {
                return Err(
                    InputError::Invalid("operation_id must use lowercase hexadecimal").into(),
                );
            }
            Ok(Some(id.parse::<NativeOperationId>()?))
        }
        Some(_) => Err(InputError::Invalid("operation_id must be an n1: reference").into()),
    }
}
