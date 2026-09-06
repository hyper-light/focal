//! Bounded async ingress feeding one blocking session owner. Disk synchronization
//! and deterministic preparation never block the networking runtime's workers.
use crate::embedded::{EmbeddedNode, NodeError};
use focal_ledger::{LedgerError, Submission};
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use focal_model::*;
use focal_wire::*;
use std::{
    future::Future,
    pin::Pin,
    sync::mpsc,
    thread::JoinHandle,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{oneshot, watch};

enum Work {
    Request(
        Box<VerifiedRequest>,
        oneshot::Sender<OwnedResponse>,
        Allocation,
    ),
    Stop(oneshot::Sender<Result<(), NodeError>>),
}
#[derive(Clone)]
pub struct LocalHost {
    sender: mpsc::SyncSender<Work>,
    ended: watch::Receiver<()>,
    budget: MemoryBudget,
    limits: WireLimits,
}
pub struct HostOwner {
    thread: JoinHandle<()>,
}

impl LocalHost {
    pub fn spawn(
        mut node: EmbeddedNode,
        limits: WireLimits,
    ) -> Result<(Self, HostOwner), NodeError> {
        limits
            .validate()
            .map_err(|error| NodeError::Domain(error.to_string()))?;
        let budget =
            MemoryBudget::new(64 * 1024 * 1024, 8 * 1024 * 1024).map_err(LedgerError::from)?;
        let host_limits = limits.clone();
        let (sender, receiver) = mpsc::sync_channel(32);
        let (liveness, ended) = watch::channel(());
        let mut views = crate::reads::ReadViews::new();
        let mut streams =
            crate::streams::Streams::new().map_err(|error| NodeError::Domain(error.to_string()))?;
        let thread = std::thread::Builder::new()
            .name("focal-session-owner".into())
            .spawn(move || {
                let _liveness = liveness;
                let mut next_tick = Instant::now();
                loop {
                    if Instant::now() >= next_tick {
                        if let Err(error) = maintain(&mut node, &mut views) {
                            use std::io::Write as _;
                            let _ = writeln!(
                                std::io::stderr().lock(),
                                "focal: owner maintenance failed: {error}"
                            );
                            break;
                        }
                        let Some(deadline) = Instant::now().checked_add(Duration::from_secs(1))
                        else {
                            break;
                        };
                        next_tick = deadline;
                    }
                    let work = match receiver
                        .recv_timeout(next_tick.saturating_duration_since(Instant::now()))
                    {
                        Ok(work) => work,
                        Err(mpsc::RecvTimeoutError::Timeout) => continue,
                        Err(mpsc::RecvTimeoutError::Disconnected) => break,
                    };
                    match work {
                        Work::Request(request, response, charge) => {
                            // A vanished caller cannot cancel an already admitted mutation.
                            let reply =
                                dispatch(&mut node, &mut views, &mut streams, *request, &limits);
                            let _ = response.send(finish_response(reply, charge));
                        }
                        Work::Stop(response) => {
                            let _ = response.send(node.checkpoint());
                            break;
                        }
                    }
                }
            })?;
        Ok((
            Self {
                sender,
                ended,
                budget,
                limits: host_limits,
            },
            HostOwner { thread },
        ))
    }
    /// Completes on owner exit, including panic or fail-stop persistence errors.
    pub async fn closed(&self) {
        let mut ended = self.ended.clone();
        let _ = ended.changed().await;
    }
    /// Returns Capacity without enqueueing when ingress is full; callers can
    /// retry after draining ingress. No blocking task or extra thread is needed.
    pub async fn stop(&self) -> Result<(), NodeError> {
        let (send, receive) = oneshot::channel();
        self.sender
            .try_send(Work::Stop(send))
            .map_err(|error| match error {
                mpsc::TrySendError::Full(_) => {
                    NodeError::Ledger(focal_ledger::LedgerError::Capacity)
                }
                mpsc::TrySendError::Disconnected(_) => {
                    NodeError::Domain("node owner stopped".into())
                }
            })?;
        receive
            .await
            .map_err(|_| NodeError::Domain("node owner stopped during shutdown".into()))?
    }
}
impl HostOwner {
    /// Joins without blocking the async executor. Unlike Tokio's blocking-pool
    /// spawn, OS thread exhaustion is returned by Builder as an I/O error.
    pub async fn join_async(self) -> Result<(), NodeError> {
        let (send, receive) = oneshot::channel();
        let _bridge = std::thread::Builder::new()
            .name("focal-owner-join".into())
            .spawn(move || {
                let _ = send.send(self.join());
            })?;
        receive
            .await
            .map_err(|_| NodeError::Domain("node join worker stopped".into()))?
    }
    pub fn join(self) -> Result<(), NodeError> {
        self.thread
            .join()
            .map_err(|_| NodeError::Domain("node owner panicked".into()))
    }
}
impl RequestHandler for LocalHost {
    fn supports_managed_requests(&self) -> bool {
        true
    }
    fn handle(
        &self,
        request: VerifiedRequest,
    ) -> Pin<Box<dyn Future<Output = ResponseEnvelope> + Send + '_>> {
        Box::pin(async move { self.handle_accounted(request).await.into_envelope() })
    }
    fn handle_accounted(&self, request: VerifiedRequest) -> OwnedHandlerFuture<'_> {
        Box::pin(async move {
            let fallback = request.request().reply(Response::Error(
                if matches!(
                    request.request().operation,
                    Operation::Reconcile(_) | Operation::RequestStreamRead { .. }
                ) {
                    AccessError::Unavailable
                } else {
                    AccessError::OutcomeUnknown
                },
            ));
            let full = request
                .request()
                .reply(Response::Error(AccessError::Capacity));
            let payload = match &request.request().operation {
                operation @ (Operation::Managed { .. }
                | Operation::RequestStreamControl { .. }
                | Operation::RequestStreamRead { .. }) => {
                    match crate::managed_requests::response_bytes(
                        operation,
                        self.limits.max_frame_bytes,
                        self.limits.max_items,
                    ) {
                        Some(bytes) => bytes,
                        None => return OwnedResponse::new(full),
                    }
                }
                Operation::Reconcile(query) => match crate::reconciliation::response_bytes(
                    query,
                    self.limits.max_frame_bytes,
                    self.limits.max_items,
                ) {
                    Some(bytes) => bytes,
                    None => return OwnedResponse::new(full),
                },
                Operation::Read(_) | Operation::List(_) => self.limits.max_frame_bytes as usize,
                Operation::Stream(stream) => {
                    stream.credits().bytes.min(self.limits.max_frame_bytes) as usize
                }
                Operation::Download { max_bytes, .. } => {
                    (*max_bytes).min(self.limits.max_frame_bytes) as usize
                }
                _ => 0,
            };
            let content = matches!(
                request.request().operation,
                Operation::Upload(_)
                    | Operation::Download { .. }
                    | Operation::Managed {
                        operation: ManagedOperation::Submit {
                            command: Command::AttachArtifact { .. }
                                | Command::RegisterArtifact { .. }
                                | Command::FailTestamentGeneration { .. },
                            ..
                        },
                        ..
                    }
                    | Operation::Submit {
                        command: Command::AttachArtifact { .. }
                            | Command::RegisterArtifact { .. }
                            | Command::FailTestamentGeneration { .. },
                        ..
                    }
            );
            let bytes = postcard::experimental::serialized_size(request.request())
                .ok()
                .and_then(|n| n.checked_mul(32))
                .and_then(|n| {
                    payload
                        .checked_mul(32)
                        .and_then(|reply| n.checked_add(reply))
                })
                .and_then(|n| n.checked_add(if content { 8 * 1024 * 1024 } else { 0 }))
                .and_then(|n| n.checked_add(4096));
            let Some(bytes) = bytes else {
                return OwnedResponse::new(full);
            };
            // VerifiedRequest supplies capability-checked operation semantics;
            // no request payload can nominate its own admission priority.
            let lane = match &request.request().operation {
                Operation::Submit { command, .. }
                | Operation::Managed {
                    operation: ManagedOperation::Submit { command, .. },
                    ..
                } => focal_ledger::mutation_lane(command),
                Operation::RequestStreamControl {
                    command:
                        RequestStreamCommand::Acknowledge { .. }
                        | RequestStreamCommand::Seal { .. }
                        | RequestStreamCommand::Close { .. },
                    ..
                } => BudgetLane::Completion,
                _ => BudgetLane::Ordinary,
            };
            let Ok(charge) = self.budget.reserve(BudgetKind::Pending, lane, bytes) else {
                return OwnedResponse::new(full);
            };
            let (send, receive) = oneshot::channel();
            match self
                .sender
                .try_send(Work::Request(Box::new(request), send, charge.commit()))
            {
                Ok(()) => {}
                Err(mpsc::TrySendError::Full(_)) => return OwnedResponse::new(full),
                Err(mpsc::TrySendError::Disconnected(Work::Request(request, _, _))) => {
                    return OwnedResponse::new(
                        request
                            .request()
                            .reply(Response::Error(AccessError::Unavailable)),
                    );
                }
                Err(mpsc::TrySendError::Disconnected(_)) => return OwnedResponse::new(fallback),
            }
            receive
                .await
                .unwrap_or_else(|_| OwnedResponse::new(fallback))
        })
    }
}
pub(crate) fn finish_response(
    mut header: ResponseEnvelope,
    mut charge: Allocation,
) -> OwnedResponse {
    // Admission covers the maximum page before constructing it. Once staging
    // is gone retain only this reply's conservative resident/encoding allowance,
    // so a small response does not pin the entire possible page budget.
    let retained = postcard::experimental::serialized_size(&header)
        .ok()
        .and_then(|n| n.checked_mul(32))
        .and_then(|n| n.checked_add(512));
    match retained {
        Some(bytes) if bytes <= charge.bytes() => {
            if charge.shrink_to(bytes).is_err() {
                header.result = Response::Error(AccessError::OutcomeUnknown);
            }
        }
        _ => header.result = Response::Error(AccessError::OutcomeUnknown),
    }
    OwnedResponse::accounted(header, charge)
}
/// An existing committed receipt remains immutable on any healthy replica.
/// A miss conveys no proposal authority and requires normal admission.
pub(crate) fn known_receipt(
    session: &focal_ledger::Session,
    input: &AuthenticatedInput,
) -> Result<Option<MutationReply>, AccessError> {
    if input.ledger != session.ledger() {
        return Err(AccessError::Unauthorized);
    }
    session.read_at_least(SessionSeq(0)).map_err(access)?;
    let key = RequestKey {
        principal: input.principal,
        epoch: input.request_epoch,
        id: input.request_id,
    };
    let Some(receipt) = session.receipt(&key) else {
        return Ok(None);
    };
    let hash = command_hash(input).map_err(|_| AccessError::InvalidRequest)?;
    Ok(Some(if hash == receipt.command_hash {
        MutationReply::Committed(receipt.clone())
    } else {
        MutationReply::Domain(DomainOutcome::refuse(
            ErrorCode::IdempotencyConflict,
            "request key is bound to another command",
        ))
    }))
}
pub(crate) fn access(error: LedgerError) -> AccessError {
    match error {
        LedgerError::Managed(error) => match error {
            focal_ledger::ManagedError::Capacity => AccessError::Capacity,
            focal_ledger::ManagedError::Unsupported => AccessError::UnsupportedOperation,
            focal_ledger::ManagedError::InvalidIdentity => AccessError::Unauthorized,
            focal_ledger::ManagedError::NotRegistered => AccessError::ManagedNotRegistered,
            focal_ledger::ManagedError::Conflict => AccessError::ManagedConflict,
            focal_ledger::ManagedError::Closed { generation } => {
                AccessError::ManagedClosed { generation }
            }
            focal_ledger::ManagedError::Retired { through } => {
                AccessError::ManagedRetired { through }
            }
        },
        LedgerError::Capacity => AccessError::Capacity,
        LedgerError::Behind => AccessError::Behind {
            published: SessionSeq(0),
        },
        LedgerError::ResyncRequired => AccessError::ResyncRequired { floor: None },
        LedgerError::NotReady { .. } => AccessError::Unavailable,
        _ => AccessError::OutcomeUnknown,
    }
}
fn maintain(node: &mut EmbeddedNode, views: &mut crate::reads::ReadViews) -> Result<(), NodeError> {
    views
        .advance(&mut node.session)
        .map_err(|error| NodeError::Domain(error.to_string()))?;
    let now = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| NodeError::Domain("system clock precedes Unix epoch".into()))?
            .as_millis(),
    )
    .map_err(|_| NodeError::Domain("system clock overflow".into()))?;
    node.session
        .maintain_cursor_clock_local(now.max(node.session.cursor_clock()))?;
    Ok(())
}
fn dispatch(
    node: &mut EmbeddedNode,
    views: &mut crate::reads::ReadViews,
    streams: &mut crate::streams::Streams,
    verified: VerifiedRequest,
    limits: &WireLimits,
) -> ResponseEnvelope {
    let principal = verified.peer().principal();
    let peer = verified.peer();
    let request = verified.request();
    // Preserve only the small reply header. The admitted payload and peer grant
    // remain borrowed until mutation authentication consumes their ownership.
    let mut response = request.reply(Response::Error(AccessError::OutcomeUnknown));
    if let Err(error) = views.advance(&mut node.session) {
        return request.reply(Response::Error(error));
    }
    let result = if request.ledger != node.identity.ledger {
        Err(AccessError::Unauthorized)
    } else if request.route_epoch != RouteEpoch(1)
        && matches!(
            request.operation,
            Operation::Reconcile(_)
                | Operation::Managed { .. }
                | Operation::RequestStreamControl { .. }
                | Operation::RequestStreamRead { .. }
        )
    {
        Err(AccessError::Unavailable)
    } else if request.route_epoch != RouteEpoch(1) {
        Err(AccessError::RouteChanged(RouteHint {
            epoch: RouteEpoch(1),
            endpoint: node
                .root()
                .join("focal.sock")
                .to_string_lossy()
                .into_owned(),
            server_name: "local".into(),
        }))
    } else {
        match &request.operation {
            Operation::Submit { .. } | Operation::OpenEpoch { .. } => (|| {
                let authority = AuthorityContext {
                    runtime: false,
                    cause: Cause::Root(node.identity.root),
                    policy_revision: 1,
                    logical_time: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map_err(|_| AccessError::Unavailable)?
                        .as_secs(),
                    evidence: Vec::new(),
                };
                let mut input = verified.into_authenticated(authority)?;
                if let Some(known) = known_receipt(&node.session, &input)? {
                    return Ok(Response::Submitted(known));
                }
                input.authority.evidence = attest(node, &input.command)?;
                node.session
                    .submit_local(&input)
                    .map(|result| {
                        Response::Submitted(match result {
                            Submission::Committed(receipt) => MutationReply::Committed(receipt),
                            Submission::Domain(outcome) => MutationReply::Domain(outcome),
                            Submission::Pending(key) => MutationReply::Pending(key),
                        })
                    })
                    .map_err(access)
            })(),
            Operation::Managed { .. }
            | Operation::RequestStreamControl { .. }
            | Operation::RequestStreamRead { .. } => {
                crate::managed_requests::local(node, views, streams, verified, limits)
            }
            Operation::ManagedSupport { .. } => Err(AccessError::UnsupportedOperation),
            Operation::Reconcile(query) => crate::reconciliation::local(
                &mut node.session,
                principal,
                query,
                request.request_id,
                request.route_epoch,
                limits,
            )
            .map(Response::Reconciled),
            Operation::Read(read) => views
                .read(
                    &mut node.session,
                    principal,
                    read,
                    request.request_id,
                    limits,
                )
                .map(Response::Read),
            Operation::List(list) => list_scope(peer, node.session.ledger(), &list.filter)
                .and_then(|scope| {
                    views.list(
                        &mut node.session,
                        crate::reads::ListReadContext {
                            principal,
                            scope,
                            request_id: request.request_id,
                            barrier: None,
                        },
                        list,
                        limits,
                    )
                })
                .map(Response::Listed),
            Operation::Upload(upload) => upload_content(node, peer, upload).map(Response::Upload),
            Operation::Download {
                content,
                offset,
                max_bytes,
            } => {
                if content.domain != ContentDomainId(node.identity.ledger.tenant.0) {
                    Err(AccessError::Unauthorized)
                } else {
                    node.content
                        .read_range(
                            content,
                            *offset,
                            (*max_bytes as usize)
                                .min(node.content.upload_chunk_bytes())
                                .min((limits.max_frame_bytes as usize).saturating_sub(256)),
                        )
                        .map(|bytes| {
                            Response::Content(ContentChunk {
                                offset: *offset,
                                eof: offset.saturating_add(bytes.len() as u64) == content.length,
                                bytes,
                            })
                        })
                        .map_err(content_error)
                }
            }
            Operation::Stream(stream) => streams
                .handle(&mut node.session, views, peer, request, stream, limits)
                .map(Response::Stream),
            Operation::Subscribe(_)
            | Operation::Raft { .. }
            | Operation::Control { .. }
            | Operation::PeerControl { .. }
            | Operation::NodeContact { .. }
            | Operation::EnrollmentControl { .. }
            | Operation::Custody(_) => Err(AccessError::UnsupportedOperation),
        }
    };
    response.result = result.unwrap_or_else(Response::Error);
    if encode_payload(&response, limits.max_frame_bytes).is_err() {
        response.result = Response::Error(AccessError::Capacity);
    }
    response
}

fn content_error(error: focal_evidence::ContentError) -> AccessError {
    match error {
        focal_evidence::ContentError::Capacity => AccessError::Capacity,
        focal_evidence::ContentError::Io(_) | focal_evidence::ContentError::Failed => {
            AccessError::OutcomeUnknown
        }
        _ => AccessError::InvalidRequest,
    }
}
fn upload_content(
    node: &mut EmbeddedNode,
    peer: &AuthenticatedPeer,
    request: &UploadRequest,
) -> Result<UploadReply, AccessError> {
    let id = focal_evidence::UploadId(upload_scope(peer, node.identity.ledger, request.upload()));
    match request {
        UploadRequest::Begin {
            length,
            digest,
            class,
            ..
        } => node
            .content
            .begin(
                id,
                ContentDomainId(node.identity.ledger.tenant.0),
                *class,
                *length,
                Some(*digest),
            )
            .map(UploadReply::Offset)
            .map_err(content_error),
        UploadRequest::Append { offset, bytes, .. } => {
            match node.content.append(id, *offset, bytes) {
                Ok(offset) => Ok(UploadReply::Offset(offset)),
                Err(focal_evidence::ContentError::Offset(offset)) => {
                    Ok(UploadReply::Offset(offset))
                }
                Err(error) => Err(content_error(error)),
            }
        }
        UploadRequest::Seal { .. } => node
            .content
            .seal(id)
            .map(UploadReply::Sealed)
            .map_err(content_error),
        UploadRequest::Cancel { .. } => node
            .content
            .finish(id)
            .map(|()| UploadReply::Cancelled)
            .map_err(content_error),
    }
}
pub(crate) fn attest(
    node: &EmbeddedNode,
    command: &Command,
) -> Result<Vec<EvidenceAttestation>, AccessError> {
    let artifact = match command {
        Command::AttachArtifact { artifact, .. } | Command::RegisterArtifact { artifact } => {
            Some(artifact)
        }
        Command::FailTestamentGeneration { error, .. } => Some(error),
        _ => None,
    };
    let Some(artifact) = artifact else {
        return Ok(Vec::new());
    };
    if artifact.content.ledger != node.identity.ledger {
        return Err(AccessError::Unauthorized);
    }
    let bytes = match &artifact.content.payload {
        ArtifactPayload::Inline(bytes) if bytes.len() <= 16 * 1024 => bytes.clone(),
        ArtifactPayload::Content(reference)
            if reference.domain == ContentDomainId(node.identity.ledger.tenant.0) =>
        {
            node.content
                .read_bytes(reference, 1024 * 1024)
                .map_err(|_| AccessError::InvalidRequest)?
        }
        _ => return Err(AccessError::InvalidRequest),
    };
    // Current ingress supports the pinned test-report schema. Arbitrary schemas
    // require a registered validator, never a caller-supplied `schema_valid` bit.
    if artifact.content.schema_hash != focal_evidence::test_report_schema() {
        return Err(AccessError::UnsupportedOperation);
    }
    let _: focal_evidence::TestReport =
        serde_json::from_slice(&bytes).map_err(|_| AccessError::InvalidRequest)?;
    Ok(vec![EvidenceAttestation {
        descriptor_hash: artifact
            .content
            .content_hash()
            .map_err(|_| AccessError::InvalidRequest)?,
        custody_revision: 1,
        durable: true,
        schema_valid: true,
    }])
}

#[cfg(test)]
#[path = "reconciliation_tests.rs"]
mod reconciliation_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt;

    #[tokio::test]
    async fn local_replies_retain_only_their_owned_bytes_through_owner_shutdown() {
        let root = tempfile::tempdir().unwrap();
        let mut settings = crate::config::Settings::default();
        settings.node.data_dir = Some(root.path().to_owned());
        let node = EmbeddedNode::open(&settings).unwrap();
        let identity = node.identity.clone();
        let limits = WireLimits::default();
        let (host, owner) = LocalHost::spawn(node, limits.clone()).unwrap();
        let peer = AuthenticatedPeer::local(PeerGrant {
            principal: identity.issuer,
            tenants: std::collections::BTreeSet::from([identity.ledger.tenant]),
            role: PeerRole::Actor,
        })
        .unwrap();
        let request = RequestEnvelope {
            protocol: PROTOCOL_VERSION,
            ledger: identity.ledger,
            route_epoch: RouteEpoch(1),
            request_epoch: RequestEpoch(1),
            request_id: RequestId::from_u128(1),
            operation: Operation::Read(ReadRequest {
                consistency: ReadConsistency::Linearizable,
                query: ReadQuery::Objects(Vec::new()),
                max_items: 1,
            }),
        };
        let first =
            focal_wire::dispatch_accounted(&host, peer.clone(), request.clone(), &limits).await;
        assert!(matches!(first.envelope().result, Response::Read(_)));
        let held = host.budget.stats().ordinary_used;
        assert!(
            held > 0 && held < 1024 * 1024,
            "small reply retained full page allowance: {held}"
        );
        let second = focal_wire::dispatch_accounted(&host, peer, request, &limits).await;
        assert!(matches!(second.envelope().result, Response::Read(_)));
        assert_eq!(host.budget.stats().ordinary_used, held * 2);
        drop(first);
        assert_eq!(host.budget.stats().ordinary_used, held);
        host.stop().await.unwrap();
        owner.join().unwrap();
        assert_eq!(host.budget.stats().ordinary_used, held);
        drop(second);
        assert_eq!(host.budget.stats().used, 0);
    }

    #[test]
    fn shutdown_backpressure_and_closed_owner_need_no_runtime_or_worker() {
        let (sender, receiver) = mpsc::sync_channel(1);
        let (_liveness, ended) = watch::channel(());
        let host = LocalHost {
            sender,
            ended,
            budget: MemoryBudget::new(64 * 1024 * 1024, 8 * 1024 * 1024).unwrap(),
            limits: WireLimits::default(),
        };
        let (send, _receive) = oneshot::channel();
        host.sender.try_send(Work::Stop(send)).unwrap();
        assert!(matches!(
            host.stop().now_or_never(),
            Some(Err(NodeError::Ledger(LedgerError::Capacity)))
        ));
        drop(receiver);
        assert!(matches!(
            host.stop().now_or_never(),
            Some(Err(NodeError::Domain(_)))
        ));
    }
}
