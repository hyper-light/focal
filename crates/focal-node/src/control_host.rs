//! Owned root/partition metadata replicas. Authentication selects the namespace;
//! durable Raft publication completes writes and quorum barriers complete reads.
use focal_consensus::{PbMessageExt, StateRole};
use focal_control::*;
use focal_directory::AuthorityVerifier;
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use focal_model::{LedgerId, RequestEpoch, RequestId, RouteEpoch};
use focal_wire::*;
use std::{
    collections::VecDeque,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::mpsc,
    thread::JoinHandle,
    time::{Duration, Instant},
};
use tokio::sync::{mpsc as async_mpsc, oneshot, watch};

#[derive(Debug, Clone)]
pub struct ControlHostConfig {
    /// Dedicated server-owned metadata namespace; never inferred from a request.
    pub namespace: LedgerId,
    pub route_epoch: RouteEpoch,
    pub queue_items: usize,
    pub pending_requests: usize,
    pub replication_queue: usize,
    pub tick: Duration,
    pub request_timeout: Duration,
}
impl ControlHostConfig {
    pub fn new(namespace: LedgerId) -> Self {
        Self {
            namespace,
            route_epoch: RouteEpoch(1),
            queue_items: 32,
            pending_requests: 64,
            replication_queue: 128,
            tick: Duration::from_millis(100),
            request_timeout: Duration::from_secs(5),
        }
    }
    fn validate(&self) -> Result<(), ControlError> {
        if self.namespace.tenant.is_zero()
            || self.namespace.session.is_zero()
            || self.route_epoch.0 == 0
            || !(1..=1024).contains(&self.queue_items)
            || !(1..=1024).contains(&self.pending_requests)
            || !(1..=1024).contains(&self.replication_queue)
            || !(Duration::from_millis(10)..=Duration::from_secs(1)).contains(&self.tick)
            || self.request_timeout.is_zero()
            || self.request_timeout > Duration::from_secs(60)
        {
            return Err(ControlError::Invalid);
        }
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub struct ControlProgress {
    pub identity: ControlIdentity,
    pub node: u64,
    pub leader: u64,
    pub term: u64,
    pub applied_index: u64,
    pub revisions: ControlRevisions,
    pub dropped_replication: u64,
    pub stopped: bool,
}
pub struct ControlReplicationFrame {
    pub target: u64,
    pub request: RequestEnvelope,
    // Retained until transport finishes, including connection setup/retries.
    _charge: Allocation,
}
enum Work {
    Request(Box<VerifiedRequest>, oneshot::Sender<Completed>, Allocation),
    Campaign(oneshot::Sender<Result<(), ControlFailure>>),
    Stop(oneshot::Sender<Result<(), ControlFailure>>),
}
struct Completed {
    response: ResponseEnvelope,
    // Delivery into a oneshot is not consumption: a slow handler must retain
    // both reservations until it actually receives the encoded response.
    _input: Allocation,
    _output: Option<Allocation>,
}
impl Completed {
    fn into_owned(self) -> OwnedResponse {
        OwnedResponse::accounted_pair(self.response, self._input, self._output)
    }
}
#[derive(Clone)]
pub struct ControlHost {
    sender: mpsc::SyncSender<Work>,
    peers: mpsc::SyncSender<Work>,
    progress: watch::Receiver<ControlProgress>,
    config: ControlHostConfig,
    limits: WireLimits,
    budget: MemoryBudget,
}
pub struct ControlOwner(JoinHandle<()>);
impl ControlOwner {
    pub fn join(self) -> Result<(), ControlFailure> {
        self.0.join().map_err(|_| ControlFailure::Unavailable)
    }
}
enum Waiting {
    Write(ControlRequestId),
    Read {
        context: Vec<u8>,
        query: ControlRead,
    },
}
struct Pending {
    header: ResponseEnvelope,
    response: oneshot::Sender<Completed>,
    waiting: Waiting,
    term: u64,
    deadline: Instant,
    _charge: Allocation,
}
struct Owner<V> {
    replica: ControlReplica,
    verifier: V,
    config: ControlHostConfig,
    limits: WireLimits,
    budget: MemoryBudget,
    pending: VecDeque<Pending>,
    outbound: async_mpsc::Sender<ControlReplicationFrame>,
    progress: watch::Sender<ControlProgress>,
    nonce: u64,
    dropped: u64,
}
impl ControlHost {
    pub fn wire_limits() -> WireLimits {
        WireLimits {
            max_frame_bytes: 10 * 1024 * 1024,
            max_cost: 40 * 1024 * 1024,
            ..WireLimits::default()
        }
    }
    pub fn spawn<V: AuthorityVerifier + Send + 'static>(
        replica: ControlReplica,
        verifier: V,
        config: ControlHostConfig,
        budget: MemoryBudget,
    ) -> Result<
        (
            Self,
            ControlOwner,
            async_mpsc::Receiver<ControlReplicationFrame>,
        ),
        ControlError,
    > {
        config.validate()?;
        let limits = Self::wire_limits();
        let (sender, receiver) = mpsc::sync_channel(config.queue_items);
        let (peers, incoming) = mpsc::sync_channel(config.replication_queue);
        let (outbound, outgoing) = async_mpsc::channel(config.replication_queue);
        let status = replica.status();
        let (progress, changes) = watch::channel(ControlProgress {
            identity: replica.identity(),
            node: status.node_id,
            leader: status.leader_id,
            term: status.term,
            applied_index: replica.applied_index(),
            revisions: replica.revisions(),
            dropped_replication: 0,
            stopped: false,
        });
        let owner = Owner {
            replica,
            verifier,
            config: config.clone(),
            limits: limits.clone(),
            budget: budget.clone(),
            pending: VecDeque::new(),
            outbound,
            progress,
            nonce: 0,
            dropped: 0,
        };
        let thread = std::thread::Builder::new()
            .name(format!("focal-control-{}", status.node_id))
            .spawn(move || owner.run(receiver, incoming))
            .map_err(|_| ControlError::Capacity)?;
        Ok((
            Self {
                sender,
                peers,
                progress: changes,
                config,
                limits,
                budget,
            },
            ControlOwner(thread),
            outgoing,
        ))
    }
    pub fn progress(&self) -> ControlProgress {
        self.progress.borrow().clone()
    }
    pub async fn closed(&self) {
        let mut changes = self.progress.clone();
        while !changes.borrow().stopped {
            if changes.changed().await.is_err() {
                break;
            }
        }
    }
    pub async fn campaign(&self) -> Result<(), ControlFailure> {
        let (send, receive) = oneshot::channel();
        self.sender
            .try_send(Work::Campaign(send))
            .map_err(queue_error)?;
        receive.await.map_err(|_| ControlFailure::Unavailable)?
    }
    pub async fn stop(&self) -> Result<(), ControlFailure> {
        let (send, receive) = oneshot::channel();
        self.sender
            .try_send(Work::Stop(send))
            .map_err(queue_error)?;
        receive.await.map_err(|_| ControlFailure::Unavailable)?
    }
    pub async fn submit(
        &self,
        peer: AuthenticatedPeer,
        request: ControlRequest,
    ) -> Result<ControlReceipt, ControlFailure> {
        let id = RequestId::from_u128(u128::from(request.id.sequence));
        match self.call(peer, id, ControlRpc::Submit(request)).await? {
            ControlReply::Committed(receipt) => Ok(receipt),
            ControlReply::Rejected(error) => Err(error),
            _ => Err(ControlFailure::Invalid),
        }
    }
    pub async fn read(
        &self,
        peer: AuthenticatedPeer,
        id: RequestId,
        query: ControlRead,
    ) -> Result<ControlReadResult, ControlFailure> {
        match self.call(peer, id, ControlRpc::Read(query)).await? {
            ControlReply::Read(value) => Ok(value),
            ControlReply::Rejected(error) => Err(error),
            _ => Err(ControlFailure::Invalid),
        }
    }
    /// Requests leader transfer. Success means initiated; observe a quorum read
    /// or progress on the target to establish the eventual leader.
    pub async fn transfer(
        &self,
        peer: AuthenticatedPeer,
        id: RequestId,
        request: ControlTransfer,
    ) -> Result<(), ControlFailure> {
        match self.call(peer, id, ControlRpc::Transfer(request)).await? {
            ControlReply::TransferInitiated { .. } => Ok(()),
            ControlReply::Rejected(error) => Err(error),
            _ => Err(ControlFailure::Invalid),
        }
    }
    async fn call(
        &self,
        peer: AuthenticatedPeer,
        id: RequestId,
        rpc: ControlRpc,
    ) -> Result<ControlReply, ControlFailure> {
        let payload = rpc
            .encode(self.limits.max_frame_bytes as usize)
            .map_err(ControlFailure::from)?;
        let request = RequestEnvelope {
            protocol: PROTOCOL_VERSION,
            ledger: self.config.namespace,
            route_epoch: self.config.route_epoch,
            request_epoch: RequestEpoch(1),
            request_id: id,
            operation: if matches!(peer.role(), PeerRole::Node { .. }) {
                Operation::PeerControl {
                    group: self.progress().identity.group,
                    request: payload,
                }
            } else {
                Operation::Control {
                    group: self.progress().identity.group,
                    request: payload,
                }
            },
        };
        let verified = verify_request(peer, request, &self.limits).map_err(access_failure)?;
        let response = self.handle(verified).await;
        match response.result {
            Response::Control { response } => {
                ControlReply::decode(&response, self.limits.max_frame_bytes as usize)
                    .map_err(ControlFailure::from)
            }
            Response::Error(error) => Err(access_failure(error)),
            _ => Err(ControlFailure::Invalid),
        }
    }
}
impl RequestHandler for ControlHost {
    fn handle(&self, request: VerifiedRequest) -> HandlerFuture<'_> {
        Box::pin(async move { self.handle_accounted(request).await.into_envelope() })
    }
    fn handle_accounted(&self, request: VerifiedRequest) -> OwnedHandlerFuture<'_> {
        Box::pin(async move {
            let unknown = request
                .request()
                .reply(Response::Error(AccessError::OutcomeUnknown));
            let unavailable = request
                .request()
                .reply(Response::Error(AccessError::Unavailable));
            let capacity = request
                .request()
                .reply(Response::Error(AccessError::Capacity));
            let peer = matches!(request.request().operation, Operation::Raft { .. });
            let Some(bytes) = postcard::experimental::serialized_size(request.request())
                .ok()
                .and_then(|n| n.checked_mul(if peer { 2 } else { 32 }))
                .and_then(|n| n.checked_add(4096))
            else {
                return OwnedResponse::new(capacity);
            };
            let Ok(charge) = self.budget.reserve(
                BudgetKind::Pending,
                if peer {
                    BudgetLane::Completion
                } else {
                    BudgetLane::Ordinary
                },
                bytes,
            ) else {
                return OwnedResponse::new(capacity);
            };
            let (send, receive) = oneshot::channel();
            let queue = if peer { &self.peers } else { &self.sender };
            match queue.try_send(Work::Request(Box::new(request), send, charge.commit())) {
                Ok(()) => receive
                    .await
                    .map(Completed::into_owned)
                    .unwrap_or_else(|_| OwnedResponse::new(unknown)),
                Err(mpsc::TrySendError::Full(_)) => OwnedResponse::new(capacity),
                Err(mpsc::TrySendError::Disconnected(_)) => OwnedResponse::new(unavailable),
            }
        })
    }
}
impl<V: AuthorityVerifier> Owner<V> {
    fn run(mut self, receiver: mpsc::Receiver<Work>, peers: mpsc::Receiver<Work>) {
        let _result = catch_unwind(AssertUnwindSafe(|| -> Result<(), ControlError> {
            self.drain()?;
            let mut next_tick = Instant::now()
                .checked_add(self.config.tick)
                .ok_or(ControlError::Capacity)?;
            loop {
                for _ in 0..8 {
                    match peers.try_recv() {
                        Ok(work) => {
                            self.work(work)?;
                        }
                        Err(_) => break,
                    }
                }
                if Instant::now() >= next_tick {
                    self.replica.tick()?;
                    self.drain()?;
                    next_tick = Instant::now()
                        .checked_add(self.config.tick)
                        .ok_or(ControlError::Capacity)?;
                }
                // Peer ingress has its own reserved queue. The short idle wait
                // bounds peer latency without spawning an extra forwarding task.
                let wait = next_tick
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_millis(5));
                match receiver.recv_timeout(wait) {
                    Ok(work) => {
                        if self.work(work)? {
                            return Ok(());
                        }
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                    Err(mpsc::RecvTimeoutError::Disconnected) => return Ok(()),
                }
            }
        }));
        while let Some(pending) = self.pending.pop_front() {
            self.finish(pending, Err(ControlFailure::OutcomeUnknown));
        }
        self.publish_progress(true);
    }
    fn work(&mut self, work: Work) -> Result<bool, ControlError> {
        match work {
            Work::Request(request, response, charge) => {
                self.request(*request, response, charge);
                self.drain()?;
            }
            Work::Campaign(response) => {
                let result = self
                    .replica
                    .campaign()
                    .and_then(|_| self.drain())
                    .map_err(ControlFailure::from);
                let _ = response.send(result);
            }
            Work::Stop(response) => {
                let result = self.drain().and_then(|_| {
                    if self.replica.has_pending() || self.replica.applied_index() == 0 {
                        Ok(())
                    } else {
                        self.replica.checkpoint()
                    }
                });
                let _ = response.send(result.map_err(ControlFailure::from));
                return Ok(true);
            }
        }
        Ok(false)
    }
    fn request(
        &mut self,
        verified: VerifiedRequest,
        response: oneshot::Sender<Completed>,
        charge: Allocation,
    ) {
        let header = verified
            .request()
            .reply(Response::Error(AccessError::OutcomeUnknown));
        let mut waiting = None;
        let mut peer_accepted = false;
        let peer_rpc = matches!(verified.request().operation, Operation::Raft { .. });
        let result = (|| -> Result<ControlReply, ControlFailure> {
            let request = verified.request();
            let principal = verified.peer().principal();
            if request.ledger != self.config.namespace {
                return Err(ControlFailure::Unauthorized);
            }
            if let Operation::Raft { group, message } = &request.operation {
                let PeerRole::Node { node_id } = verified.peer().role() else {
                    return Err(ControlFailure::Unauthorized);
                };
                if *group != self.replica.identity().group || !self.replica.accepts_peer(node_id) {
                    return Err(ControlFailure::Unauthorized);
                }
                self.replica
                    .step_authenticated(node_id, message)
                    .map_err(ControlFailure::from)?;
                self.drain().map_err(ControlFailure::from)?;
                peer_accepted = true;
                return Err(ControlFailure::Invalid);
            }
            let contact = matches!(request.operation, Operation::NodeContact { .. });
            let (group, bytes, read_only): (&[u8; 16], &[u8], bool) = match &request.operation {
                Operation::NodeContact { group, .. }
                    if matches!(verified.peer().role(), PeerRole::Node { .. }) =>
                {
                    (group, &[], false)
                }
                Operation::Control { group, request }
                    if matches!(verified.peer().role(), PeerRole::Runtime) =>
                {
                    (group, request, false)
                }
                Operation::PeerControl { group, request }
                    if matches!(verified.peer().role(), PeerRole::Node { .. }) =>
                {
                    (group, request, true)
                }
                _ => return Err(ControlFailure::Unauthorized),
            };
            if request.route_epoch != self.config.route_epoch
                || *group != self.replica.identity().group
            {
                return Err(ControlFailure::WrongOwner);
            }
            // Decode the read selector without ever deserializing a node-supplied
            // Submit body, even when it contains adversarial collection hints.
            let rpc = if contact {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_err(|_| ControlFailure::Unavailable)?
                    .as_secs();
                ControlRpc::Submit(crate::network_contacts::prepare_contact(
                    &self.replica,
                    &verified,
                    i64::try_from(now).map_err(|_| ControlFailure::Unavailable)?,
                )?)
            } else if read_only {
                ControlRpc::Read(
                    ControlRpc::decode_read_only(bytes, MAX_PEER_CONTROL_REQUEST_BYTES)
                        .map_err(|_| ControlFailure::Unauthorized)?,
                )
            } else {
                ControlRpc::decode(bytes, self.replica.limits().max_command_bytes)
                    .map_err(ControlFailure::from)?
            };
            if self.pending.len() >= self.config.pending_requests {
                return Err(ControlFailure::Capacity);
            }
            match rpc {
                ControlRpc::Transfer(request) => {
                    self.replica
                        .transfer(&request)
                        .map_err(ControlFailure::from)?;
                    Ok(ControlReply::TransferInitiated {
                        target: request.target,
                    })
                }
                ControlRpc::Submit(request) => {
                    if !contact && matches!(request.command, ControlCommand::NodeContact(_)) {
                        return Err(ControlFailure::Unauthorized);
                    }
                    if request.id.client != principal.0 {
                        return Err(ControlFailure::Unauthorized);
                    }
                    match self
                        .replica
                        .submit(request, &self.verifier)
                        .map_err(ControlFailure::from)?
                    {
                        ControlSubmission::Existing(receipt) => {
                            Ok(ControlReply::Committed(receipt))
                        }
                        ControlSubmission::Pending(id) => {
                            waiting = Some(Waiting::Write(id));
                            Err(ControlFailure::OutcomeUnknown)
                        }
                    }
                }
                ControlRpc::Read(query) => {
                    if matches!(&query, ControlRead::Receipt(id) if id.client != principal.0) {
                        return Err(ControlFailure::Unauthorized);
                    }
                    self.nonce = self.nonce.checked_add(1).ok_or(ControlFailure::Capacity)?;
                    let mut context = b"focal.control.read.v1\0".to_vec();
                    context.extend_from_slice(&self.nonce.to_be_bytes());
                    context.extend_from_slice(&principal.0);
                    context.extend_from_slice(&request.request_id.0);
                    self.replica
                        .read_index(context.clone())
                        .map_err(ControlFailure::from)?;
                    waiting = Some(Waiting::Read { context, query });
                    Err(ControlFailure::Unavailable)
                }
            }
        })();
        if peer_accepted {
            let mut header = header;
            header.result = Response::PeerAccepted;
            let _ = response.send(Completed {
                response: header,
                _input: charge,
                _output: None,
            });
            return;
        }
        if peer_rpc {
            let mut header = header;
            header.result = Response::Error(
                if matches!(
                    result,
                    Err(ControlFailure::Unauthorized | ControlFailure::WrongOwner)
                ) {
                    AccessError::Unauthorized
                } else {
                    AccessError::Unavailable
                },
            );
            let _ = response.send(Completed {
                response: header,
                _input: charge,
                _output: None,
            });
            return;
        }
        let deadline = Instant::now().checked_add(self.config.request_timeout);
        if let (Some(waiting), Some(deadline)) = (waiting, deadline) {
            self.pending.push_back(Pending {
                header,
                response,
                waiting,
                term: self.replica.status().term,
                deadline,
                _charge: charge,
            });
        } else {
            self.send(header, response, result, charge, None);
        }
    }
    fn drain(&mut self) -> Result<(), ControlError> {
        // Declare the source permit first so remaining event buffers drop
        // before it on every exit. Frames/replies below allocate new encoded
        // buffers under their own permits, retained through transport send.
        let _source_allocation;
        let mut events = self.replica.drain(&self.verifier)?;
        _source_allocation = events.take_allocation();
        let status = self.replica.status();
        for _ in 0..self.pending.len() {
            let Some(pending) = self.pending.pop_front() else {
                return Err(ControlError::Failed);
            };
            let mut read_charge = None;
            let ready = match &pending.waiting {
                Waiting::Write(id) => self
                    .replica
                    .receipt(*id)?
                    .map(ControlReply::Committed)
                    .map(Ok),
                Waiting::Read { context, query, .. }
                    if pending.term == status.term
                        && status.role == StateRole::Leader
                        && events.read_states.iter().any(|read| {
                            &read.context == context && read.index <= self.replica.applied_index()
                        }) =>
                {
                    Some((|| {
                        // Publication may have grown since admission; reserve the
                        // exact current export bound only after the read barrier.
                        let bytes = self
                            .replica
                            .read_charge(query)
                            .map_err(ControlFailure::from)?;
                        read_charge = Some(
                            self.budget
                                .reserve(BudgetKind::Control, BudgetLane::Ordinary, bytes)
                                .map_err(|_| ControlFailure::Capacity)?
                                .commit(),
                        );
                        self.replica
                            .read_local(query)
                            .map(ControlReply::Read)
                            .map_err(ControlFailure::from)
                    })())
                }
                _ => None,
            };
            if let Some(result) = ready {
                self.finish_charged(pending, result, read_charge.take());
            } else if pending.response.is_closed() {
                drop(pending);
            } else if pending.term != status.term
                || status.role != StateRole::Leader
                || Instant::now() >= pending.deadline
            {
                let failure = match pending.waiting {
                    Waiting::Write(_) => ControlFailure::OutcomeUnknown,
                    _ => ControlFailure::Unavailable,
                };
                self.finish(pending, Err(failure));
            } else {
                self.pending.push_back(pending);
            }
            drop(read_charge);
        }
        for message in events.messages {
            let Some(bytes) = (message.compute_size() as usize)
                .checked_mul(2)
                .and_then(|n| n.checked_add(4096))
            else {
                return Err(ControlError::Capacity);
            };
            let Ok(charge) =
                self.budget
                    .reserve(BudgetKind::Control, BudgetLane::Completion, bytes)
            else {
                self.dropped = self.dropped.saturating_add(1);
                continue;
            };
            let encoded = message.write_to_bytes().map_err(|_| ControlError::Failed)?;
            if encoded
                .len()
                .checked_add(256)
                .is_none_or(|n| n > self.limits.max_frame_bytes as usize)
            {
                self.dropped = self.dropped.saturating_add(1);
                continue;
            }
            self.nonce = self.nonce.checked_add(1).ok_or(ControlError::Capacity)?;
            let request = RequestEnvelope {
                protocol: PROTOCOL_VERSION,
                ledger: self.config.namespace,
                route_epoch: self.config.route_epoch,
                request_epoch: RequestEpoch(1),
                request_id: RequestId::from_u128(
                    (u128::from(status.node_id) << 64) | u128::from(self.nonce),
                ),
                operation: Operation::Raft {
                    group: self.replica.identity().group,
                    message: encoded,
                },
            };
            if self
                .outbound
                .try_send(ControlReplicationFrame {
                    target: message.to,
                    request,
                    _charge: charge.commit(),
                })
                .is_err()
            {
                self.dropped = self.dropped.saturating_add(1);
            }
        }
        self.publish_progress(false);
        Ok(())
    }
    fn finish(&self, pending: Pending, result: Result<ControlReply, ControlFailure>) {
        self.finish_charged(pending, result, None);
    }
    fn finish_charged(
        &self,
        pending: Pending,
        result: Result<ControlReply, ControlFailure>,
        output: Option<Allocation>,
    ) {
        self.send(
            pending.header,
            pending.response,
            result,
            pending._charge,
            output,
        );
    }
    fn send(
        &self,
        mut header: ResponseEnvelope,
        response: oneshot::Sender<Completed>,
        result: Result<ControlReply, ControlFailure>,
        input: Allocation,
        output: Option<Allocation>,
    ) {
        let reply = result.unwrap_or_else(ControlReply::Rejected);
        header.result = match reply.encode(self.limits.max_frame_bytes.saturating_sub(256) as usize)
        {
            Ok(response) => Response::Control { response },
            Err(_) => Response::Error(AccessError::OutcomeUnknown),
        };
        let _ = response.send(Completed {
            response: header,
            _input: input,
            _output: output,
        });
    }
    fn publish_progress(&self, stopped: bool) {
        let status = self.replica.status();
        self.progress.send_replace(ControlProgress {
            identity: self.replica.identity(),
            node: status.node_id,
            leader: status.leader_id,
            term: status.term,
            applied_index: self.replica.applied_index(),
            revisions: self.replica.revisions(),
            dropped_replication: self.dropped,
            stopped,
        });
    }
}
fn queue_error<T>(error: mpsc::TrySendError<T>) -> ControlFailure {
    match error {
        mpsc::TrySendError::Full(_) => ControlFailure::Capacity,
        mpsc::TrySendError::Disconnected(_) => ControlFailure::Unavailable,
    }
}
fn access_failure(error: AccessError) -> ControlFailure {
    match error {
        AccessError::Unauthorized => ControlFailure::Unauthorized,
        AccessError::Capacity => ControlFailure::Capacity,
        AccessError::OutcomeUnknown => ControlFailure::OutcomeUnknown,
        _ => ControlFailure::Unavailable,
    }
}
