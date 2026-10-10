//! The native engine path of the MCP adapter. The engine probe runs once on
//! the worker's shard before the catalogue is built; on a native ledger
//! every application tool compiles its document through the shared driver,
//! journals the exact frame under an `n1:` reference in this adapter's own
//! journal and drives it to a committed receipt, a closed refusal or a
//! pending outcome the recovery tools resolve. Results are acknowledged
//! explicitly, as managed results are, so a reply lost between the adapter
//! and the agent stays listed by `request.pending`.
use super::{BackendError, Cancel, bounded_json, cancellable, cancelled, envelope, random_id};
use crate::{Backend, ToolCall};
use focal_client::input::InputError;
use focal_client::native_store::{NativeOperation, NativeOperationId, NativeOperationStore};
use focal_client::operations::{
    ApplicationDocument, Engine, OperationOutput, WireProfile, decode_application, find_application,
};
use focal_client::{ClientError, ClientTransport};
use focal_model::RequestId;
use focal_native_client::{CompileLimits, DriveError, NativeContentProfile, Preparation};
use focal_wire::*;
use hyper_rt::combine::{Either, race2};
use serde_json::Value;
use std::cell::Cell;
use std::future::Future;

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

/// The native driver's waits on this adapter's connection — reads, lists, submissions and the wait
/// observer's pauses — each raced against the call's cancellation, which they share: a wait takes
/// it from the cell for its time and gives it back however it ends. The driver makes them one at a
/// time; a second wait at once would find the cell empty and be refused, typed.
struct Waits<'a, 'b, T: ClientTransport> {
    backend: &'a Backend<T>,
    cancel: &'a Cell<Option<&'b mut Cancel>>,
}
/// The call's cancellation, taken from its cell for one wait and put back as the wait ends.
struct Held<'a, 'b> {
    cell: &'a Cell<Option<&'b mut Cancel>>,
    cancel: Option<&'b mut Cancel>,
}
impl Drop for Held<'_, '_> {
    fn drop(&mut self) {
        self.cell.set(self.cancel.take());
    }
}
impl<T: ClientTransport> Waits<'_, '_, T> {
    async fn race<R>(
        &self,
        work: impl Future<Output = Result<R, DriveError>>,
    ) -> Result<R, DriveError> {
        let mut held = Held {
            cell: self.cancel,
            cancel: self.cancel.take(),
        };
        let cancel = held
            .cancel
            .as_deref_mut()
            .ok_or(DriveError::Client(ClientError::Transport))?;
        match race2(work, cancel).await {
            Either::First(done) => done,
            Either::Second(_) => Err(DriveError::Cancelled),
        }
    }
}
impl<T: ClientTransport> focal_native_client::Reads for Waits<'_, '_, T> {
    async fn read(&mut self, read: NativeReadRequest) -> Result<NativeReadPage, DriveError> {
        let request = envelope(self.backend.build.ledger, Operation::NativeRead(read))?;
        self.race(async { Ok(self.backend.client.native_read(request).await?) })
            .await
    }
}
impl<T: ClientTransport> focal_native_client::Lists for Waits<'_, '_, T> {
    async fn list(&mut self, list: NativeListRequest) -> Result<NativeListPage, DriveError> {
        let request = envelope(self.backend.build.ledger, Operation::NativeList(list))?;
        self.race(async { Ok(self.backend.client.native_list(request).await?) })
            .await
    }
}
impl<T: ClientTransport> focal_native_client::Submits for Waits<'_, '_, T> {
    async fn submit(
        &mut self,
        request: RequestEnvelope,
    ) -> Result<NativeMutationReply, DriveError> {
        self.race(async { Ok(self.backend.client.submit_native(request).await?) })
            .await
    }
}
impl<T: ClientTransport> focal_native_client::Pause for Waits<'_, '_, T> {
    async fn pause(&mut self, period: std::time::Duration) -> Result<(), DriveError> {
        let nanos = u64::try_from(period.as_nanos()).unwrap_or(u64::MAX);
        self.race(async {
            hyper_rt::futures::sleep(nanos)
                .await
                .map_err(|_| DriveError::Client(ClientError::Transport))
        })
        .await
    }
}

impl<T: ClientTransport> Backend<T> {
    /// Register the native journal; the engine probe at startup decides
    /// whether it is opened. Without a registration the adapter never probes
    /// and serves the V1 catalogue.
    pub fn with_native_journal(mut self, journal: Box<dyn NativeJournal>) -> Self {
        self.native_journal = Some(journal);
        self
    }
    /// The engine probe. Runs on the worker's shard because a remote
    /// transport binds its endpoint to the shard that first drives it.
    pub(crate) async fn detect(&mut self) -> Result<(), String> {
        self.detect_inner().await.map_err(|error| error.to_string())
    }
    async fn detect_inner(&mut self) -> Result<(), BackendError> {
        let Some(journal) = self.native_journal.take() else {
            return Ok(());
        };
        // The one standing read every host performs
        // (`focal_client::operations::probe`), resolved with this adapter's
        // own journal as the proof that an unreachable ledger is native: a
        // journal from an earlier run refuses to start as V1 and mint
        // identities in the wrong namespace, a fresh context keeps the V1
        // behaviour, whose startup never needed the network. A standing for
        // another principal is a misconfigured context.
        let engine = focal_client::operations::probe(
            &self.client,
            self.build.ledger,
            RequestId(random_id()?),
            journal.initialized(),
            self.context.principal,
        )
        .await
        .map_err(|error| match error {
            ClientError::Configuration => BackendError::Configuration,
            error => error.into(),
        })?;
        if let Engine::Native(standing) = engine {
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
    /// This adapter's waits for the native driver, raced against the shared cancellation.
    fn waits<'a, 'b>(&'a self, cancel: &'a Cell<Option<&'b mut Cancel>>) -> Waits<'a, 'b, T> {
        Waits {
            backend: self,
            cancel,
        }
    }
    pub(super) fn native_call<'a>(
        &'a self,
        call: &'a mut ToolCall,
        cancel: &'a mut Cancel,
        operation_id: &'a mut Option<String>,
    ) -> super::Branch<'a> {
        Box::pin(self.native_call_inner(call, cancel, operation_id))
    }
    async fn native_call_inner(
        &self,
        call: &mut ToolCall,
        cancel: &mut Cancel,
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
                    let shared = Cell::new(Some(&mut *cancel));
                    let page = match native.store.key_of(id, &self.context) {
                        Ok(key) => {
                            focal_native_client::outcome(key, &mut self.waits(&shared)).await?
                        }
                        Err(_) => {
                            focal_native_client::outcome_by_id(
                                self.context.principal,
                                id.request(),
                                &mut self.waits(&shared),
                            )
                            .await?
                        }
                    };
                    // The wire layer validates the read reply's own envelope
                    // identity, but not that the outcome it carries is for the
                    // invocation we asked about. Bind it here, mirroring the
                    // receipt-identity check the V1 and managed inspect paths
                    // apply: a returned outcome for a different request is a
                    // ReceiptMismatch, never shown as this operation's outcome.
                    let expected = |invocation: NativeInvocationRef| {
                        matches!(invocation, NativeInvocationRef::Request(key)
                            if key.principal == self.context.principal && key.id == id.request())
                    };
                    for object in &page.objects {
                        if let NativeObject::Outcome(receipt) = object
                            && !expected(receipt.invocation)
                        {
                            return Err(
                                focal_client::native_store::NativeStoreError::ReceiptMismatch
                                    .into(),
                            );
                        }
                    }
                    return Ok((
                        "Observed",
                        OperationOutput::NativeRead {
                            page: Box::new(page),
                        },
                    ));
                }
                let operation = native.store.retry(id, &self.context)?;
                if call.tool == "request.retry" {
                    return self.native_drive(native, operation, cancel).await;
                }
                match (operation.receipt, operation.refusal) {
                    (Some(receipt), _) => Ok(committed(receipt, operation.created)),
                    // A capacity refusal admitted nothing and the exact frame stays
                    // journaled for a retry: still pending, refused for the reason it names.
                    (None, Some(refusal)) if refusal.kind == NativeRefusalKind::Capacity => {
                        Ok(("Pending", OperationOutput::NativeRefused { refusal }))
                    }
                    (None, Some(refusal)) => Err(BackendError::NativeRefused(Box::new(refusal))),
                    (None, None) => Ok(state("Pending")),
                }
            }
            name => {
                let descriptor = find_application(WireProfile::Native, name)
                    .filter(|descriptor| {
                        crate::catalog_native::permitted(&native.standing, descriptor)
                    })
                    .ok_or(BackendError::Configuration)?;
                // An `n1:` reference belongs to the adapter envelope of a
                // mutation, never to a read or list document.
                let requested = if descriptor.mutation {
                    take_native_id(&mut call.arguments)?
                } else {
                    None
                };
                let bytes = bounded_json(&call.arguments)?;
                // The engine's decoder, selected by the descriptor's result
                // kind the way every native host selects it.
                let authored = match decode_application(WireProfile::Native, name, &bytes)? {
                    ApplicationDocument::V1(_) => return Err(BackendError::Configuration),
                    ApplicationDocument::NativeList(list) => {
                        let shared = Cell::new(Some(&mut *cancel));
                        let page =
                            focal_native_client::list(&list, &self.build, &mut self.waits(&shared))
                                .await?;
                        return Ok((
                            "Listed",
                            OperationOutput::NativeList {
                                page: Box::new(page),
                            },
                        ));
                    }
                    ApplicationDocument::NativeRead(read) => {
                        return self.native_read_call(&read, cancel).await;
                    }
                    ApplicationDocument::Native(authored) => authored,
                };
                if cancelled(cancel) {
                    return Err(BackendError::Cancelled);
                }
                let preparation = Preparation {
                    store: &native.store,
                    context: self.context,
                    build: &self.build,
                    profile: profile(&native.standing),
                    limits: &native.limits,
                };
                // The journal's own upkeep first (F12).
                {
                    let shared = Cell::new(Some(&mut *cancel));
                    focal_native_client::maintain(
                        &preparation,
                        &mut random_id,
                        &mut self.waits(&shared),
                        &mut self.waits(&shared),
                    )
                    .await?;
                }
                if cancelled(cancel) {
                    return Err(BackendError::Cancelled);
                }
                let prepared = {
                    let shared = Cell::new(Some(&mut *cancel));
                    focal_native_client::prepare(
                        &preparation,
                        &authored,
                        requested,
                        &mut random_id,
                        &mut self.waits(&shared),
                    )
                    .await?
                };
                *operation_id = Some(prepared.id.to_string());
                self.native_drive(native, prepared, cancel).await
            }
        }
    }
    /// One read, the lineage read (which also lists) or the wait observer.
    /// All share this adapter's connection; the pause between wait probes is
    /// cancellable like every other wait of this adapter.
    async fn native_read_call(
        &self,
        read: &focal_client::operations::NativeReadOperation,
        cancel: &mut Cancel,
    ) -> Result<(&'static str, OperationOutput), BackendError> {
        let outcome = {
            let shared = Cell::new(Some(&mut *cancel));
            focal_native_client::read(
                read,
                &self.build,
                &mut self.waits(&shared),
                &mut self.waits(&shared),
                &mut self.waits(&shared),
            )
            .await?
        };
        Ok(match outcome {
            focal_native_client::NativeReadOutcome::Page(page) => (
                "Read",
                OperationOutput::NativeRead {
                    page: Box::new(page),
                },
            ),
            focal_native_client::NativeReadOutcome::Lineage(lineage) => {
                ("Lineage", OperationOutput::NativeLineage { lineage })
            }
            focal_native_client::NativeReadOutcome::Wait(result) => (
                result.condition.as_str(),
                OperationOutput::NativeWait { result },
            ),
        })
    }
    /// Send the exact journaled frame until the owner commits it. Only a
    /// committed receipt bound to the frame advances the journal; a refusal
    /// is recorded as reported and a pending ticket leaves the frame listed.
    async fn native_drive(
        &self,
        native: &Native,
        operation: NativeOperation,
        cancel: &mut Cancel,
    ) -> Result<(&'static str, OperationOutput), BackendError> {
        if let Some(receipt) = operation.receipt {
            return Ok(committed(receipt, operation.created));
        }
        let reply = cancellable(cancel, async {
            self.client
                .submit_native(operation.request.clone())
                .await
                .map_err(BackendError::Client)
        })
        .await?;
        match reply {
            NativeMutationReply::Committed(receipt) => {
                native
                    .store
                    .record_reply(operation.id, &self.context, &reply)?;
                Ok(committed(receipt, operation.created))
            }
            NativeMutationReply::Pending(ticket) => Err(BackendError::NativePending(ticket)),
            NativeMutationReply::Refused(refusal) => {
                // A generation the owner closed before this frame reached it
                // (F12): a committed outcome is read from the seal.
                let expired = if refusal.kind
                    == NativeRefusalKind::Refused(NativeErrorCode::RequestHistoryExpired)
                {
                    let shared = Cell::new(Some(&mut *cancel));
                    focal_native_client::expired(
                        &native.store,
                        &self.context,
                        &operation,
                        &mut self.waits(&shared),
                    )
                    .await?
                } else {
                    None
                };
                if let Some(receipt) = expired {
                    return Ok(committed(receipt, operation.created));
                }
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
