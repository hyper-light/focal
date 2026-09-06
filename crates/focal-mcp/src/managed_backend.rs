use super::*;
use focal_client::{ManagedSubmitOutcome, managed_store::ManagedOperationStore};

impl<T: ClientTransport> Backend<T> {
    fn managed_requests(&self) -> Result<&ManagedRequests, BackendError> {
        self.managed.as_ref().ok_or(BackendError::Configuration)
    }
    // All filesystem calls occur on this blocking owner, between network waits.
    // The coordinator releases its short lock before yielding an exact request.
    fn maintain_managed(
        &self,
        runtime: &Runtime,
        cancel: &mut oneshot::Receiver<()>,
    ) -> Result<(), BackendError> {
        let requests = self.managed_requests()?;
        for _ in 0..128 {
            if cancelled(cancel) {
                return Err(BackendError::Cancelled);
            }
            let Some(request) = requests.maintenance(&mut random_id)? else {
                return Ok(());
            };
            let reply = runtime.block_on(async {
                tokio::select! {
                    result=self.client.request(request.clone())=>match result {
                        Ok(reply)=>Ok(reply),
                        Err(ClientError::Access(error))=>Ok(request.reply(Response::Error(error))),
                        Err(error)=>Err(BackendError::Client(error)),
                    },
                    _=&mut *cancel=>Err(BackendError::Cancelled),
                }
            })?;
            match requests.accept_maintenance(&request, reply) {
                Ok(()) | Err(ManagedRequestsError::Stale) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Err(BackendError::ManagedStore(ManagedStoreError::Capacity))
    }
    pub(super) fn managed_control(
        &self,
        runtime: &Runtime,
        call: &mut ToolCall,
        cancel: &mut oneshot::Receiver<()>,
        operation_id: &mut Option<String>,
    ) -> Result<(&'static str, OperationOutput), BackendError> {
        let id = if matches!(call.tool.as_str(), "request.acknowledge" | "request.seal") {
            let text = take_id(&mut call.arguments)?;
            let id = text.parse::<ManagedOperationId>()?;
            *operation_id = Some(text);
            Some(id)
        } else {
            None
        };
        if !call.arguments.is_empty() {
            return Err(InputError::Invalid("unsupported managed recovery argument").into());
        }
        let requests = self.managed_requests()?;
        if call.tool == "request.pending" {
            let store = match requests.store() {
                Ok(store) => store,
                Err(ManagedRequestsError::NotReady) => {
                    return Ok((
                        "Initializing",
                        OperationOutput::ManagedRequests {
                            operation_ids: Vec::new(),
                        },
                    ));
                }
                Err(error) => return Err(error.into()),
            };
            let ids = store.outstanding()?;
            let mut operation_ids = Vec::new();
            operation_ids
                .try_reserve_exact(ids.len())
                .map_err(|_| InputError::Capacity)?;
            for id in ids {
                operation_ids.push(id.to_string());
            }
            return Ok((
                "Outstanding",
                OperationOutput::ManagedRequests { operation_ids },
            ));
        }
        self.maintain_managed(runtime, cancel)?;
        let store = requests.store()?;
        match call.tool.as_str() {
            "request.reserve" => {
                if cancelled(cancel) {
                    return Err(BackendError::Cancelled);
                }
                let id = store.reserve(RequestId(random_id()?))?;
                *operation_id = Some(id.to_string());
                Ok(state("Reserved"))
            }
            "request.acknowledge" => {
                let id = id.ok_or(BackendError::Configuration)?;
                // An exact already-retired acknowledgment is harmless. It must
                // still belong to this store's known retired generation/prefix.
                if matches!(store.receipt(id), Err(ManagedStoreError::Retired)) {
                    return Ok(state("Retired"));
                }
                requests.mark_delivered(id)?;
                self.maintain_managed(runtime, cancel)?;
                match store.receipt(id) {
                    Err(ManagedStoreError::Retired) => Ok(state("Retired")),
                    Ok(Some(_)) => Ok(state("Consumed")),
                    Ok(None) => Err(ManagedStoreError::Unresolved.into()),
                    Err(error) => Err(error.into()),
                }
            }
            "request.seal" => {
                let id = id.ok_or(BackendError::Configuration)?;
                for _ in 0..8 {
                    if store.receipt(id)?.is_some() {
                        return managed_result(&store, id);
                    }
                    if cancelled(cancel) {
                        return Err(BackendError::Cancelled);
                    }
                    // Another CLI/MCP process can save a control after the
                    // initial drain. Finish that exact action, then recheck this
                    // operation; never execute its original body as a fallback.
                    match store.prepare_seal(id, RequestId(random_id()?)) {
                        Ok(_) | Err(ManagedStoreError::ControlPending) => {}
                        Err(error) => return Err(error.into()),
                    }
                    self.maintain_managed(runtime, cancel)?;
                }
                if store.receipt(id)?.is_some() {
                    managed_result(&store, id)
                } else {
                    Err(ManagedStoreError::ControlPending.into())
                }
            }
            _ => Err(BackendError::Configuration),
        }
    }
    pub(super) fn managed_mutation(
        &self,
        runtime: &Runtime,
        id: &str,
        authored: AuthoredOperation,
        expected_revision: Option<ObjectRevision>,
        canonical: &[u8],
        cancel: &mut oneshot::Receiver<()>,
    ) -> Result<(&'static str, OperationOutput), BackendError> {
        let id = id.parse::<ManagedOperationId>()?;
        let descriptor = authored.descriptor();
        let store = self.managed_requests()?.store()?;
        if cancelled(cancel) {
            return Err(BackendError::Cancelled);
        }
        let expected_revision = if expected_revision.is_none() {
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
                    Err(ManagedStoreError::Unprepared) => Some(self.participant_revision(
                        runtime,
                        claim,
                        id.key(self.context).id,
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
        store.prepare(
            id,
            OperationIntent {
                name: descriptor.name,
                version: descriptor.version,
                canonical,
            },
            |key| {
                let PlannedOperation::Mutation(command) = authored
                    .build(&self.build, &mut random_id)
                    .map_err(StoreError::Expansion)?
                else {
                    return Err(ManagedStoreError::Conflict);
                };
                let operation = Operation::Managed {
                    key,
                    operation: ManagedOperation::Submit {
                        expected_revision,
                        command,
                    },
                };
                Ok(RequestEnvelope {
                    protocol: participant_protocol(&operation),
                    ledger: self.context.ledger,
                    route_epoch: RouteEpoch(1),
                    request_epoch: RequestEpoch(1),
                    request_id: key.id,
                    operation,
                })
            },
        )?;
        self.drive_managed(runtime, &store, id, cancel)?;
        managed_result(&store, id)
    }
    pub(super) fn managed_recovery(
        &self,
        runtime: &Runtime,
        id: &str,
        retry: bool,
        remote: bool,
        cancel: &mut oneshot::Receiver<()>,
    ) -> Result<(&'static str, OperationOutput), BackendError> {
        let id = id.parse::<ManagedOperationId>()?;
        let store = self.managed_requests()?.store()?;
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
            let key = id.key(self.context);
            let mut request = envelope(
                self.context.ledger,
                Operation::RequestStreamRead {
                    cluster: self.context.cluster,
                    query: RequestStreamQuery::Receipt { key },
                },
            )?;
            request.protocol = MANAGED_PROTOCOL_VERSION;
            let reply = runtime.block_on(async {
                tokio::select! {
                    result=self.client.request_stream_read(request,self.context)=>result.map_err(BackendError::Client),
                    _=cancel=>Err(BackendError::Cancelled),
                }
            })?;
            if let RequestStreamReadResult::Receipt {
                resolution: ManagedReceiptResolution::Retained(receipt),
                ..
            } = &reply.page.result
            {
                if let Some(saved) = saved {
                    if &saved != receipt.as_ref() {
                        return Err(ManagedStoreError::ReceiptMismatch.into());
                    }
                } else {
                    let prepared = prepared.ok_or(ManagedStoreError::ReceiptMismatch)?;
                    let (key, family, intent) = managed_request_identity(&prepared.request)
                        .map_err(|_| BackendError::Configuration)?;
                    validate_managed_receipt(receipt, &key, family, intent, &WireLimits::default())
                        .map_err(|_| ManagedStoreError::ReceiptMismatch)?;
                }
            }
            return Ok(("Reconciled", OperationOutput::ManagedReconcile { reply }));
        }
        if retry {
            self.drive_managed(runtime, &store, id, cancel)?;
        }
        match managed_result(&store, id) {
            Err(BackendError::ManagedStore(ManagedStoreError::Retired)) if !retry => {
                Ok(state("Retired"))
            }
            result => result,
        }
    }
    fn drive_managed(
        &self,
        runtime: &Runtime,
        store: &ManagedOperationStore,
        id: ManagedOperationId,
        cancel: &mut oneshot::Receiver<()>,
    ) -> Result<(), BackendError> {
        if store.receipt(id)?.is_some() {
            return Ok(());
        }
        let prepared = store.request(id)?;
        if cancelled(cancel) {
            return Err(BackendError::Cancelled);
        }
        let outcome = runtime.block_on(async {
            tokio::select! {
                result=self.client.submit_managed_outcome(prepared.request,self.context)=>result.map_err(BackendError::Client),
                _=cancel=>Err(BackendError::Cancelled),
            }
        })?;
        match outcome {
            ManagedSubmitOutcome::Committed(reply) => {
                store.record_receipt(id, &reply.receipt).map_err(Into::into)
            }
            ManagedSubmitOutcome::Domain(outcome) => {
                Err(BackendError::ManagedDomain(Box::new(outcome)))
            }
        }
    }
}
fn state(value: &'static str) -> (&'static str, OperationOutput) {
    (
        value,
        OperationOutput::ManagedRequest {
            state: value.into(),
        },
    )
}
fn managed_result(
    store: &ManagedOperationStore,
    id: ManagedOperationId,
) -> Result<(&'static str, OperationOutput), BackendError> {
    if let Some(receipt) = store.receipt(id)? {
        let condition = if matches!(receipt.outcome, ManagedReceiptOutcome::Sealed { .. }) {
            "Sealed"
        } else {
            "Committed"
        };
        return Ok((condition, OperationOutput::Managed { receipt }));
    }
    match store.request(id) {
        Ok(_) => Ok(state("Pending")),
        Err(ManagedStoreError::Unprepared) => Ok(state("Reserved")),
        Err(error) => Err(error.into()),
    }
}
