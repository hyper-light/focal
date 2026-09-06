//! Replicated session owner. Ingress, replication and disk apply use separate
//! bounded queues. Client success waits for quorum commit and graph publication.
//! Directory/control code supplies the already authorized Session and route epoch.
use crate::{custody::CustodyScope, evidence_service::EvidenceWitness};
use crate::{
    host::{access, finish_response, known_receipt},
    reads::ReadViews,
    streams::{PendingStream, Streams},
};
use focal_consensus::PbMessageExt as _;
use focal_consensus::StateRole;
use focal_ledger::{LedgerError, Session, SessionEvents, Submission};
pub use focal_ledger::{MembershipView, SessionMembershipReceipt, SessionMembershipRequest};
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use focal_model::*;
use focal_wire::*;
use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::mpsc,
    thread::JoinHandle,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{mpsc as async_mpsc, oneshot, watch};

#[path = "fleet_group.rs"]
mod grouped;
pub use grouped::{FleetReplica, FleetReplication, FleetTenant, ReplicaFleet, ReplicaFleetParts};

#[cfg(test)]
#[path = "fleet_completion_tests.rs"]
mod completion_tests;

#[cfg(test)]
#[path = "fleet_stop_tests.rs"]
mod stop_tests;

#[cfg(test)]
#[path = "fleet_async_tests.rs"]
mod async_tests;

#[cfg(test)]
#[path = "fleet_membership_tests.rs"]
mod membership_tests;

#[derive(Clone, Debug)]
pub struct ReplicaConfig {
    pub root: RootCommandId,
    pub route_epoch: RouteEpoch,
    pub policy_revision: u64,
    pub queue_items: usize,
    pub pending_clients: usize,
    pub replication_queue: usize,
    pub tick: Duration,
    pub request_timeout: Duration,
}
impl ReplicaConfig {
    pub fn new(root: RootCommandId) -> Self {
        Self {
            root,
            route_epoch: RouteEpoch(1),
            policy_revision: 1,
            queue_items: 32,
            pending_clients: 128,
            replication_queue: 128,
            tick: Duration::from_millis(100),
            request_timeout: Duration::from_secs(5),
        }
    }
}

pub struct ReplicationFrame {
    pub target: u64,
    pub request: RequestEnvelope,
    _charge: Allocation,
}
#[derive(Clone, Debug)]
pub struct ReplicaProgress {
    pub node: u64,
    pub leader: u64,
    pub term: u64,
    pub sequence: SessionSeq,
    pub dropped_replication: u64,
    pub stopped: bool,
}
enum Work {
    Request(
        Box<AdmittedRequest>,
        oneshot::Sender<OwnedResponse>,
        Allocation,
    ),
    Probe(
        Box<VerifiedRequest>,
        oneshot::Sender<Result<ReceiptProbe, AccessError>>,
        Allocation,
    ),
    Transfer(u64, oneshot::Sender<Result<(), LedgerError>>),
    Membership(Box<MembershipCall>, Allocation),
    Stop(oneshot::Sender<Result<(), LedgerError>>),
}
pub(crate) struct ReceiptProbe {
    pub request: Box<VerifiedRequest>,
    pub known: Option<Response>,
    pub allocation: Allocation,
}
struct AdmittedRequest {
    verified: VerifiedRequest,
    witness: Option<EvidenceWitness>,
}
struct MembershipCall {
    request: Option<SessionMembershipRequest>,
    response: oneshot::Sender<Result<MembershipReply, LedgerError>>,
}
/// The reply retains its memory permit across the owner/caller boundary.
pub struct MembershipReply {
    view: MembershipView,
    _charge: Allocation,
}
impl MembershipReply {
    pub fn view(&self) -> &MembershipView {
        &self.view
    }
}
struct PendingMembershipCall {
    call: MembershipCall,
    context: Option<Vec<u8>>,
    term: u64,
    deadline: Instant,
    charge: Allocation,
}
impl PendingMembershipCall {
    fn finish(self, result: Result<MembershipView, LedgerError>) {
        let Self {
            call,
            context,
            charge,
            ..
        } = self;
        let MembershipCall { request, response } = call;
        // The caller can consume/drop its oneshot reply immediately. Destroy
        // source buffers before transferring the permit to that reply.
        drop(request);
        drop(context);
        let result = result.map(|view| MembershipReply {
            view,
            _charge: charge,
        });
        let _ = response.send(result);
    }
}
/// Classification uses an authenticated, capability-checked operation, never a
/// client-supplied priority bit. Admission of new work stays ordinary; progress
/// and termination of existing work can consume the completion allowance.
fn completion_request(request: &VerifiedRequest) -> bool {
    let Operation::Submit { command, .. } = &request.request().operation else {
        return false;
    };
    focal_ledger::mutation_lane(command) == BudgetLane::Completion
}
#[derive(Clone)]
enum HostSender {
    Direct(mpsc::SyncSender<Work>),
    Group {
        ledger: LedgerId,
        sender: mpsc::SyncSender<grouped::Routed>,
        slots: MemoryBudget,
        _backing: std::sync::Arc<Allocation>,
    },
}
impl HostSender {
    fn try_send(&self, work: Work) -> Result<(), mpsc::TrySendError<Work>> {
        match self {
            Self::Direct(sender) => sender.try_send(work),
            Self::Group {
                ledger,
                sender,
                slots,
                ..
            } => {
                let lane = grouped::lane(&work);
                let Ok(slot) = slots.reserve(BudgetKind::Pending, lane, 1) else {
                    return Err(mpsc::TrySendError::Full(work));
                };
                sender
                    .try_send(grouped::Routed {
                        ledger: *ledger,
                        work,
                        _slot: slot.commit(),
                    })
                    .map_err(|error| match error {
                        mpsc::TrySendError::Full(routed) => mpsc::TrySendError::Full(routed.work),
                        mpsc::TrySendError::Disconnected(routed) => {
                            mpsc::TrySendError::Disconnected(routed.work)
                        }
                    })
            }
        }
    }
}
#[derive(Clone)]
pub struct ReplicaHost {
    sender: HostSender,
    progress: watch::Receiver<ReplicaProgress>,
    budget: MemoryBudget,
    client_frame_bytes: u32,
}
pub struct ReplicaOwner(JoinHandle<()>);
impl ReplicaOwner {
    pub fn join(self) -> Result<(), &'static str> {
        self.0.join().map_err(|_| "replica owner panicked")
    }
}
// Pending admission already reserves 4096 bytes of per-request owner metadata;
// keeping stream fences inline avoids another separately allocated wrapper.
#[allow(clippy::large_enum_variant)]
enum WaitingFor {
    Mutation(RequestKey),
    PeerPersistence,
    Read {
        context: Vec<u8>,
        principal: ParticipantId,
        read: ReadRequest,
    },
    Stream(PendingStream),
}
struct Pending {
    header: ResponseEnvelope,
    response: oneshot::Sender<OwnedResponse>,
    waiting: WaitingFor,
    term: u64,
    deadline: Instant,
    _charge: Allocation,
}
impl Pending {
    fn finish(mut self, result: Response) {
        self.header.result = result;
        drop(self.waiting);
        let _ = self
            .response
            .send(finish_response(self.header, self._charge));
    }
}
struct Owner {
    session: Session,
    config: ReplicaConfig,
    limits: WireLimits,
    client_limits: WireLimits,
    views: ReadViews,
    streams: Streams,
    runtime: Option<focal_runtime::Runtime>,
    pending: VecDeque<Pending>,
    memberships: VecDeque<PendingMembershipCall>,
    outbound: async_mpsc::Sender<ReplicationFrame>,
    progress: watch::Sender<ReplicaProgress>,
    nonce: u64,
    dropped: u64,
    budget: MemoryBudget,
    nonblocking: bool,
    stopping: Option<(oneshot::Sender<Result<(), LedgerError>>, Instant)>,
    next_tick: Instant,
    wake_at: Instant,
}
impl ReplicaHost {
    /// Internal peer framing supports the consensus adapter's complete bounded
    /// message, including an installed checkpoint. This is a deployment default,
    /// not a knob needed to move from local to replicated operation.
    pub fn wire_limits() -> WireLimits {
        WireLimits {
            max_frame_bytes: 10 * 1024 * 1024,
            max_cost: 40 * 1024 * 1024,
            ..WireLimits::default()
        }
    }
    pub fn spawn(
        session: Session,
        config: ReplicaConfig,
        limits: WireLimits,
    ) -> Result<(Self, ReplicaOwner, async_mpsc::Receiver<ReplicationFrame>), LedgerError> {
        Self::spawn_with_runtime(session, config, limits, None)
    }
    /// The host owns runtime progress and all consensus message draining. A
    /// follower may drain worker completions, but cannot dispatch new effects.
    pub fn spawn_with_runtime(
        session: Session,
        config: ReplicaConfig,
        limits: WireLimits,
        runtime: Option<focal_runtime::Runtime>,
    ) -> Result<(Self, ReplicaOwner, async_mpsc::Receiver<ReplicationFrame>), LedgerError> {
        Self::validate(&config, &limits)?;
        let budget = MemoryBudget::new(64 * 1024 * 1024, 24 * 1024 * 1024)?;
        let (sender, receiver) = mpsc::sync_channel(config.queue_items);
        let (outbound, outgoing) = async_mpsc::channel(config.replication_queue);
        let (host, owner) = Self::assemble(
            session,
            config,
            limits,
            runtime,
            budget,
            HostSender::Direct(sender),
            outbound,
        )?;
        let node = owner.session.status().node_id;
        let thread = std::thread::Builder::new()
            .name(format!("focal-replica-{node}"))
            .spawn(move || owner.run(receiver))
            .map_err(|_| LedgerError::Capacity)?;
        Ok((host, ReplicaOwner(thread), outgoing))
    }
    fn validate(config: &ReplicaConfig, limits: &WireLimits) -> Result<(), LedgerError> {
        if config.root.is_zero()
            || config.route_epoch.0 == 0
            || config.policy_revision == 0
            || !(1..=1024).contains(&config.queue_items)
            || !(1..=1024).contains(&config.pending_clients)
            || !(1..=1024).contains(&config.replication_queue)
            || config.tick < Duration::from_millis(10)
            || config.tick > Duration::from_secs(1)
            || config.request_timeout.is_zero()
            || config.request_timeout > Duration::from_secs(60)
        {
            return Err(LedgerError::Capacity);
        }
        limits.validate().map_err(|_| LedgerError::Capacity)?;
        if limits.max_frame_bytes < 9 * 1024 * 1024 + 256 {
            return Err(LedgerError::Capacity);
        }
        Ok(())
    }
    fn assemble(
        session: Session,
        config: ReplicaConfig,
        limits: WireLimits,
        runtime: Option<focal_runtime::Runtime>,
        budget: MemoryBudget,
        sender: HostSender,
        outbound: async_mpsc::Sender<ReplicationFrame>,
    ) -> Result<(Self, Owner), LedgerError> {
        let status = session.status();
        let (progress, changes) = watch::channel(ReplicaProgress {
            node: status.node_id,
            leader: status.leader_id,
            term: status.term,
            sequence: session.sequence(),
            dropped_replication: 0,
            stopped: false,
        });
        let views = ReadViews::with_route_epoch(config.route_epoch);
        let streams = Streams::in_budget(&budget).map_err(|_| LedgerError::Capacity)?;
        // Replication snapshots need larger frames than ordinary data pages.
        // Keep client pages bounded by the standard wire page size so a single
        // read cannot consume the owner's completion allowance.
        let mut client_limits = limits.clone();
        client_limits.max_frame_bytes = client_limits
            .max_frame_bytes
            .min(WireLimits::default().max_frame_bytes);
        let client_frame_bytes = client_limits.max_frame_bytes;
        let owner = Owner {
            session,
            config,
            limits,
            client_limits,
            views,
            streams,
            runtime,
            pending: VecDeque::new(),
            memberships: VecDeque::new(),
            outbound,
            progress,
            nonce: 0,
            dropped: 0,
            budget: budget.clone(),
            nonblocking: false,
            stopping: None,
            next_tick: Instant::now(),
            wake_at: Instant::now(),
        };
        Ok((
            Self {
                sender,
                progress: changes,
                budget,
                client_frame_bytes,
            },
            owner,
        ))
    }
    pub fn progress(&self) -> ReplicaProgress {
        self.progress.borrow().clone()
    }
    pub fn memory_stats(&self) -> focal_memory::BudgetStats {
        self.budget.stats()
    }
    pub async fn closed(&self) {
        let mut changes = self.progress.clone();
        while !changes.borrow().stopped {
            if changes.changed().await.is_err() {
                break;
            }
        }
    }
    /// Trusted placement/control operation, serialized with all session work.
    /// Observe progress to confirm the transfer; queue acceptance is not election.
    pub async fn transfer_leader(&self, target: u64) -> Result<(), LedgerError> {
        let (send, receive) = oneshot::channel();
        self.sender
            .try_send(Work::Transfer(target, send))
            .map_err(|error| match error {
                mpsc::TrySendError::Full(_) => LedgerError::Capacity,
                mpsc::TrySendError::Disconnected(_) => LedgerError::Failed,
            })?;
        receive.await.map_err(|_| LedgerError::Failed)?
    }
    /// Trusted in-process control read, completed behind a quorum ReadIndex.
    pub async fn membership(&self) -> Result<MembershipReply, LedgerError> {
        self.membership_call(None).await
    }
    /// Trusted placement change. Enrollment or a Node wire identity alone does
    /// not grant this API. Success includes durable apply and a later ReadIndex.
    pub async fn change_membership(
        &self,
        request: SessionMembershipRequest,
    ) -> Result<MembershipReply, LedgerError> {
        self.membership_call(Some(request)).await
    }
    async fn membership_call(
        &self,
        request: Option<SessionMembershipRequest>,
    ) -> Result<MembershipReply, LedgerError> {
        // At most four 1024-member lists per configuration, and three owned
        // configurations across input, returned view and latest receipt, plus
        // bounded serialization and queue metadata. Nothing is held when idle.
        let bytes = request
            .as_ref()
            .map(|request| request.expected.charged_bytes())
            .transpose()?
            .unwrap_or(0)
            .checked_add(192 * 1024)
            .ok_or(LedgerError::Capacity)?;
        let charge = self
            .budget
            .reserve(BudgetKind::Control, BudgetLane::Completion, bytes)?
            .commit();
        if let Some(request) = &request {
            request.validate()?;
        }
        let (send, receive) = oneshot::channel();
        self.sender
            .try_send(Work::Membership(
                Box::new(MembershipCall {
                    request,
                    response: send,
                }),
                charge,
            ))
            .map_err(|error| match error {
                mpsc::TrySendError::Full(_) => LedgerError::Capacity,
                mpsc::TrySendError::Disconnected(_) => LedgerError::Failed,
            })?;
        receive.await.map_err(|_| LedgerError::OutcomeUnknown)?
    }
    /// A full queue returns Capacity without enqueueing; drain ingress and retry.
    pub async fn stop(&self) -> Result<(), LedgerError> {
        let (send, receive) = oneshot::channel();
        self.sender
            .try_send(Work::Stop(send))
            .map_err(|error| match error {
                mpsc::TrySendError::Full(_) => LedgerError::Capacity,
                mpsc::TrySendError::Disconnected(_) => LedgerError::Failed,
            })?;
        receive.await.map_err(|_| LedgerError::Failed)?
    }
}
impl RequestHandler for ReplicaHost {
    fn handle(
        &self,
        request: VerifiedRequest,
    ) -> Pin<Box<dyn Future<Output = ResponseEnvelope> + Send + '_>> {
        Box::pin(async move { self.submit_inner(request, None).await.into_envelope() })
    }
    fn handle_accounted(&self, request: VerifiedRequest) -> OwnedHandlerFuture<'_> {
        Box::pin(self.submit_inner(request, None))
    }
}
impl ReplicaHost {
    pub(crate) async fn submit_with_evidence(
        &self,
        request: VerifiedRequest,
        witness: EvidenceWitness,
    ) -> OwnedResponse {
        self.submit_inner(request, Some(witness)).await
    }
    async fn submit_inner(
        &self,
        request: VerifiedRequest,
        witness: Option<EvidenceWitness>,
    ) -> OwnedResponse {
        let unknown = request
            .request()
            .reply(Response::Error(AccessError::OutcomeUnknown));
        let full = request
            .request()
            .reply(Response::Error(AccessError::Capacity));
        let closed = request
            .request()
            .reply(Response::Error(AccessError::Unavailable));
        let replication = matches!(request.request().operation, Operation::Raft { .. });
        let response_bytes = match &request.request().operation {
            Operation::Read(_) => self.client_frame_bytes as usize,
            Operation::Stream(stream) => {
                stream.credits().bytes.min(self.client_frame_bytes) as usize
            }
            _ => 0,
        };
        let amount = postcard::experimental::serialized_size(request.request())
            .ok()
            .and_then(|n| n.checked_mul(if replication { 2 } else { 32 }))
            .and_then(|n| {
                response_bytes
                    .checked_mul(32)
                    .and_then(|reply| n.checked_add(reply))
            })
            .and_then(|n| n.checked_add(4096));
        let Some(amount) = amount else {
            return OwnedResponse::new(full);
        };
        let Ok(charge) = self.budget.reserve(
            BudgetKind::Pending,
            if replication || completion_request(&request) {
                BudgetLane::Completion
            } else {
                BudgetLane::Ordinary
            },
            amount,
        ) else {
            return OwnedResponse::new(full);
        };
        let (send, receive) = oneshot::channel();
        match self.sender.try_send(Work::Request(
            Box::new(AdmittedRequest {
                verified: request,
                witness,
            }),
            send,
            charge.commit(),
        )) {
            Ok(()) => receive
                .await
                .unwrap_or_else(|_| OwnedResponse::new(unknown)),
            Err(mpsc::TrySendError::Full(_)) => OwnedResponse::new(full),
            Err(mpsc::TrySendError::Disconnected(_)) => OwnedResponse::new(closed),
        }
    }
    /// Only checks the locally published immutable receipt. A miss never grants
    /// authority to propose and cannot bypass fresh custody or quorum checks.
    pub(crate) async fn probe_receipt(
        &self,
        request: VerifiedRequest,
    ) -> Result<ReceiptProbe, AccessError> {
        let bytes = postcard::experimental::serialized_size(request.request())
            .ok()
            .and_then(|n| n.checked_mul(32))
            .and_then(|n| n.checked_add(4096))
            .ok_or(AccessError::Capacity)?;
        let charge = self
            .budget
            .reserve(
                BudgetKind::Pending,
                if completion_request(&request) {
                    BudgetLane::Completion
                } else {
                    BudgetLane::Ordinary
                },
                bytes,
            )
            .map_err(|_| AccessError::Capacity)?
            .commit();
        let (send, receive) = oneshot::channel();
        self.sender
            .try_send(Work::Probe(Box::new(request), send, charge))
            .map_err(|error| match error {
                mpsc::TrySendError::Full(_) => AccessError::Capacity,
                mpsc::TrySendError::Disconnected(_) => AccessError::Unavailable,
            })?;
        receive.await.map_err(|_| AccessError::Unavailable)?
    }
}
impl Owner {
    fn run(mut self, receiver: mpsc::Receiver<Work>) {
        let mut next_tick = Instant::now();
        let result = (|| -> Result<(), LedgerError> {
            self.drain()?;
            loop {
                if Instant::now() >= next_tick {
                    self.tick()?;
                    next_tick = Instant::now()
                        .checked_add(self.config.tick)
                        .ok_or(LedgerError::Failed)?;
                }
                match receiver.recv_timeout(next_tick.saturating_duration_since(Instant::now())) {
                    Ok(work) => {
                        if self.accept(work)? {
                            return Ok(());
                        }
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                    Err(mpsc::RecvTimeoutError::Disconnected) => return Ok(()),
                }
            }
        })();
        if let Err(error) = result {
            use std::io::Write as _;
            let _ = writeln!(std::io::stderr().lock(), "focal: replica stopped: {error}");
        }
        self.close();
    }
    fn tick(&mut self) -> Result<(), LedgerError> {
        self.session.tick()?;
        if self.session.status().role == StateRole::Leader {
            let now = wall_ms()?.max(self.session.cursor_clock());
            match self.session.propose_cursor_clock(now) {
                Ok(_) | Err(LedgerError::Capacity | LedgerError::NotReady { .. }) => {}
                Err(error) => return Err(error),
            }
        }
        self.views
            .advance(&mut self.session)
            .map_err(|_| LedgerError::Failed)?;
        self.drain()
    }
    /// Shared-worker progress never waits on a disk receipt. The exact Ready
    /// remains inside Session until its WAL owner reports a completed fence.
    fn progress_group(&mut self) -> Result<bool, LedgerError> {
        self.views
            .advance(&mut self.session)
            .map_err(|_| LedgerError::Failed)?;
        self.expire_pending();
        if self.session.has_ready() {
            self.drain_with_runtime(self.stopping.is_none())?;
        }
        if let Some((_, deadline)) = self.stopping.as_ref() {
            let expired = Instant::now() >= *deadline;
            if expired && self.session.has_ready() {
                if let Some((response, _)) = self.stopping.take() {
                    let _ = response.send(Err(LedgerError::OutcomeUnknown));
                }
                return Ok(true);
            }
            if !self.session.has_ready() {
                if let Some((response, _)) = self.stopping.take() {
                    // The shared WAL already owns the recoverable durable
                    // prefix. Avoid a synchronous whole-WAL checkpoint rewrite
                    // on this multi-session worker's shutdown path.
                    let _ = response.send(Ok(()));
                }
                return Ok(true);
            }
            return Ok(false);
        }
        if !self.session.persistence_pending() && Instant::now() >= self.next_tick {
            self.tick()?;
            self.next_tick = Instant::now()
                .checked_add(self.config.tick)
                .ok_or(LedgerError::Failed)?;
        }
        Ok(false)
    }
    fn group_deadline(&self) -> Result<Instant, LedgerError> {
        if self.session.persistence_pending() || self.stopping.is_some() {
            return Instant::now()
                .checked_add(Duration::from_millis(1))
                .ok_or(LedgerError::Failed);
        }
        if self.session.has_ready() {
            return Ok(self.next_tick.min(Instant::now()));
        }
        Ok(self.next_tick)
    }
    fn accept(&mut self, work: Work) -> Result<bool, LedgerError> {
        match work {
            Work::Request(request, response, charge) => {
                self.request(request.verified, response, charge, request.witness);
                self.drain()?;
            }
            Work::Probe(request, response, charge) => {
                let result = self.probe_receipt(&request).map(|known| ReceiptProbe {
                    known,
                    request,
                    allocation: charge,
                });
                let _ = response.send(result);
            }
            Work::Transfer(target, response) => {
                let result = self.session.transfer_leader(target);
                self.drain()?;
                let _ = response.send(result);
            }
            Work::Membership(call, charge) => {
                self.accept_membership(*call, charge);
                self.drain()?;
            }
            Work::Stop(response) => {
                if self.nonblocking {
                    self.begin_stop(response)?;
                    return self.progress_group();
                }
                // Drain only work Raft already owns. The first bounded poll can
                // create the leader-readiness ReadIndex; the second consumes
                // its local Ready output. Shutdown does not dispatch effects
                // or wait for unavailable peers to commit pending proposals.
                let result = (|| {
                    self.drain_with_runtime(false)?;
                    self.drain_with_runtime(false)?;
                    // A pending proposal remains recoverable in Raft's log;
                    // it is not turned into an acknowledged checkpoint.
                    if self.session.pending_count() == 0 {
                        self.session.checkpoint()
                    } else {
                        Ok(())
                    }
                })();
                let _ = response.send(result);
                return Ok(true);
            }
        }
        Ok(false)
    }
    fn begin_stop(
        &mut self,
        response: oneshot::Sender<Result<(), LedgerError>>,
    ) -> Result<(), LedgerError> {
        if self.stopping.is_some() {
            let _ = response.send(Err(LedgerError::Capacity));
            return Ok(());
        }
        let deadline = Instant::now()
            .checked_add(self.config.request_timeout)
            .ok_or(LedgerError::Capacity)?;
        self.stopping = Some((response, deadline));
        Ok(())
    }
    fn close(&mut self) {
        while let Some(pending) = self.memberships.pop_front() {
            self.finish_membership(pending, Err(LedgerError::OutcomeUnknown));
        }
        self.memberships = VecDeque::new();
        if let Some((response, _)) = self.stopping.take() {
            let _ = response.send(Err(LedgerError::OutcomeUnknown));
        }
        while let Some(pending) = self.pending.pop_front() {
            pending.finish(Response::Error(AccessError::OutcomeUnknown));
        }
        self.publish_progress(true);
    }
    fn publish_progress(&self, stopped: bool) {
        let status = self.session.status();
        self.progress.send_replace(ReplicaProgress {
            node: status.node_id,
            leader: status.leader_id,
            term: status.term,
            sequence: self.session.sequence(),
            dropped_replication: self.dropped,
            stopped,
        });
    }
    fn request(
        &mut self,
        verified: VerifiedRequest,
        response: oneshot::Sender<OwnedResponse>,
        charge: Allocation,
        witness: Option<EvidenceWitness>,
    ) {
        let header = verified
            .request()
            .reply(Response::Error(AccessError::OutcomeUnknown));
        let mut waiting = None;
        let result = (|| -> Result<Response, AccessError> {
            let request = verified.request();
            let peer = verified.peer();
            let deadline = Instant::now()
                .checked_add(self.config.request_timeout)
                .ok_or(AccessError::Unavailable)?;
            if request.ledger != self.session.ledger() {
                return Err(AccessError::Unauthorized);
            }
            if let Operation::Raft { group, message } = &request.operation {
                let PeerRole::Node { node_id } = peer.role() else {
                    return Err(AccessError::Unauthorized);
                };
                let status = self.session.status();
                if *group != self.session.group_id()
                    || (!status.voters.contains(&node_id) && !status.learners.contains(&node_id))
                {
                    return Err(AccessError::Unauthorized);
                }
                if self.pending.len() == self.config.pending_clients {
                    return Err(AccessError::Capacity);
                }
                self.session
                    .step_authenticated(node_id, message)
                    .map_err(access)?;
                // This ingress acknowledgment and the generated Raft messages
                // remain behind the exact Ready fence, including async writes.
                waiting = Some((WaitingFor::PeerPersistence, deadline));
                return Ok(Response::Error(AccessError::Unavailable));
            }
            if request.route_epoch != self.config.route_epoch {
                return Err(AccessError::Unavailable);
            }
            match &request.operation {
                Operation::Submit { .. } | Operation::OpenEpoch { .. } => {
                    if self.pending.len() == self.config.pending_clients {
                        return Err(AccessError::Capacity);
                    }
                    let evidence = if matches!(
                        request.operation,
                        Operation::Submit {
                            command: Command::AttachArtifact { .. }
                                | Command::RegisterArtifact { .. }
                                | Command::FailTestamentGeneration { .. },
                            ..
                        }
                    ) {
                        let witness = witness.as_ref().ok_or(AccessError::UnsupportedOperation)?;
                        vec![witness.validate(
                            &verified,
                            CustodyScope {
                                ledger: self.session.ledger(),
                                route_epoch: self.config.route_epoch,
                                policy_revision: self.config.policy_revision,
                            },
                            &self.session.status().voters,
                        )?]
                    } else {
                        Vec::new()
                    };
                    let authority = AuthorityContext {
                        runtime: false,
                        cause: Cause::Root(self.config.root),
                        policy_revision: self.config.policy_revision,
                        logical_time: wall_ms().map_err(access)? / 1000,
                        evidence,
                    };
                    let input = verified.into_authenticated(authority)?;
                    match self.session.propose(&input).map_err(access)? {
                        Submission::Committed(receipt) => {
                            Ok(Response::Submitted(MutationReply::Committed(receipt)))
                        }
                        Submission::Domain(outcome) => {
                            Ok(Response::Submitted(MutationReply::Domain(outcome)))
                        }
                        Submission::Pending(key) => {
                            waiting = Some((WaitingFor::Mutation(key), deadline));
                            Ok(Response::Error(AccessError::OutcomeUnknown))
                        }
                    }
                }
                Operation::Stream(stream) => {
                    if self.pending.len() == self.config.pending_clients {
                        return Err(AccessError::Capacity);
                    }
                    let pending = self.streams.begin(
                        &mut self.session,
                        peer,
                        request,
                        stream,
                        &self.client_limits,
                    )?;
                    waiting = Some((WaitingFor::Stream(pending), deadline));
                    Ok(Response::Error(AccessError::Unavailable))
                }
                Operation::Read(read) => {
                    if matches!(read.consistency, ReadConsistency::Linearizable) {
                        if self.pending.len() == self.config.pending_clients {
                            return Err(AccessError::Capacity);
                        }
                        self.nonce = self.nonce.checked_add(1).ok_or(AccessError::Unavailable)?;
                        let mut context = b"focal.replica.read.v1\0".to_vec();
                        context.extend_from_slice(&self.nonce.to_be_bytes());
                        context.extend_from_slice(&peer.principal().0);
                        context.extend_from_slice(&request.request_id.0);
                        self.session.read_index(context.clone()).map_err(access)?;
                        waiting = Some((
                            WaitingFor::Read {
                                context,
                                principal: peer.principal(),
                                read: read.clone(),
                            },
                            deadline,
                        ));
                        Ok(Response::Error(AccessError::Unavailable))
                    } else {
                        self.views
                            .read(
                                &mut self.session,
                                peer.principal(),
                                read,
                                request.request_id,
                                &self.client_limits,
                            )
                            .map(Response::Read)
                    }
                }
                _ => Err(AccessError::UnsupportedOperation),
            }
        })();
        if let Some((waiting, deadline)) = waiting {
            self.pending.push_back(Pending {
                header,
                response,
                waiting,
                term: self.session.status().term,
                deadline,
                _charge: charge,
            });
        } else {
            let mut header = header;
            header.result = result.unwrap_or_else(Response::Error);
            let _ = response.send(finish_response(header, charge));
        }
    }
    fn probe_receipt(&self, verified: &VerifiedRequest) -> Result<Option<Response>, AccessError> {
        let request = verified.request();
        if request.ledger != self.session.ledger() {
            return Err(AccessError::Unauthorized);
        }
        if request.route_epoch != self.config.route_epoch {
            return Err(AccessError::Unavailable);
        }
        let input = verified.clone().into_authenticated(AuthorityContext {
            runtime: false,
            cause: Cause::Root(self.config.root),
            policy_revision: self.config.policy_revision,
            logical_time: 0,
            evidence: Vec::new(),
        })?;
        known_receipt(&self.session, &input).map(|known| known.map(Response::Submitted))
    }
    fn drain(&mut self) -> Result<(), LedgerError> {
        self.drain_with_runtime(true)
    }
    fn drain_with_runtime(&mut self, drive_runtime: bool) -> Result<(), LedgerError> {
        let events = if self.nonblocking {
            let Some(events) = self.session.try_poll()? else {
                self.expire_pending();
                self.publish_progress(false);
                return Ok(());
            };
            events
        } else {
            self.session.poll()?
        };
        if drive_runtime && let Some(runtime) = &mut self.runtime {
            match runtime.drive(&mut self.session) {
                Ok(_) => {}
                Err(error) if error.is_retryable() => {}
                Err(_) => return Err(LedgerError::Failed),
            }
        }
        self.resolve(&events)?;
        for message in &events.messages {
            let size = message.compute_size() as usize;
            // Oversized snapshots require the chunked snapshot transport. They
            // cannot be silently treated as installed or acknowledged.
            if size
                .checked_add(128)
                .is_none_or(|n| n > self.limits.max_frame_bytes as usize)
            {
                self.dropped = self.dropped.saturating_add(1);
                continue;
            }
            let Ok(charge) = self.budget.reserve(
                BudgetKind::Control,
                BudgetLane::Completion,
                size.checked_mul(2)
                    .and_then(|n| n.checked_add(4096))
                    .ok_or(LedgerError::Capacity)?,
            ) else {
                self.dropped = self.dropped.saturating_add(1);
                continue;
            };
            let message_bytes = message.write_to_bytes().map_err(|_| LedgerError::Corrupt)?;
            self.nonce = self.nonce.checked_add(1).ok_or(LedgerError::Capacity)?;
            let id = ((u128::from(self.session.status().node_id) << 64) | u128::from(self.nonce))
                .to_be_bytes();
            let frame = ReplicationFrame {
                target: message.to,
                _charge: charge.commit(),
                request: RequestEnvelope {
                    protocol: PROTOCOL_VERSION,
                    ledger: self.session.ledger(),
                    route_epoch: self.config.route_epoch,
                    request_epoch: RequestEpoch(1),
                    request_id: RequestId(id),
                    operation: Operation::Raft {
                        group: self.session.group_id(),
                        message: message_bytes,
                    },
                },
            };
            if self.outbound.try_send(frame).is_err() {
                self.dropped = self.dropped.saturating_add(1);
            }
        }
        self.publish_progress(false);
        Ok(())
    }
    fn resolve(&mut self, events: &SessionEvents) -> Result<(), LedgerError> {
        self.resolve_memberships(events)?;
        let status = self.session.status();
        let count = self.pending.len();
        for _ in 0..count {
            let mut pending = self.pending.pop_front().ok_or(LedgerError::Corrupt)?;
            let result = match &mut pending.waiting {
                WaitingFor::PeerPersistence => Some(Response::PeerAccepted),
                WaitingFor::Mutation(key) => self
                    .session
                    .receipt(key)
                    .cloned()
                    .map(|receipt| Response::Submitted(MutationReply::Committed(receipt))),
                WaitingFor::Read {
                    context,
                    principal,
                    read,
                } if pending.term == status.term && status.role == StateRole::Leader => events
                    .read_barriers
                    .iter()
                    .find(|(value, _)| value.as_slice() == context.as_slice())
                    .map(|(_, prefix)| {
                        let mut read = read.clone();
                        read.consistency = ReadConsistency::AtLeast(ReadToken {
                            ledger: self.session.ledger(),
                            sequence: *prefix,
                            route_epoch: self.config.route_epoch,
                        });
                        self.views
                            .read(
                                &mut self.session,
                                *principal,
                                &read,
                                pending.header.request_id,
                                &self.client_limits,
                            )
                            .map(Response::Read)
                            .unwrap_or_else(Response::Error)
                    }),
                WaitingFor::Stream(stream) if self.stopping.is_none() => {
                    match self.streams.advance(
                        &mut self.session,
                        &mut self.views,
                        stream,
                        events,
                        &self.client_limits,
                    ) {
                        Ok(reply) => reply.map(Response::Stream),
                        Err(error) => Some(Response::Error(error)),
                    }
                }
                _ => None,
            };
            if let Some(result) = result {
                pending.finish(result);
            } else if Instant::now() >= pending.deadline || status.term != pending.term {
                let error = match &pending.waiting {
                    WaitingFor::Mutation(_) => AccessError::OutcomeUnknown,
                    WaitingFor::Stream(stream) => stream.interrupted(),
                    _ => AccessError::Unavailable,
                };
                pending.finish(Response::Error(error));
            } else {
                self.pending.push_back(pending);
            }
        }
        Ok(())
    }
    fn expire_pending(&mut self) {
        let now = Instant::now();
        let count = self.memberships.len();
        for _ in 0..count {
            let Some(pending) = self.memberships.pop_front() else {
                break;
            };
            if pending.call.response.is_closed() {
                self.finish_membership(pending, Err(LedgerError::OutcomeUnknown));
                continue;
            }
            if now >= pending.deadline {
                self.finish_membership(pending, Err(LedgerError::OutcomeUnknown));
            } else {
                self.memberships.push_back(pending);
            }
        }
        // Rotate in place: no replacement queue allocation is needed while a
        // stalled disk retains both its Ready and client input reservations.
        let count = self.pending.len();
        for _ in 0..count {
            let Some(pending) = self.pending.pop_front() else {
                break;
            };
            if pending.response.is_closed() {
                drop(pending);
            } else if now >= pending.deadline {
                let error = match &pending.waiting {
                    WaitingFor::Mutation(_) => AccessError::OutcomeUnknown,
                    WaitingFor::Stream(stream) => stream.interrupted(),
                    _ => AccessError::Unavailable,
                };
                pending.finish(Response::Error(error));
            } else {
                self.pending.push_back(pending);
            }
        }
    }
}
impl Owner {
    fn finish_membership(
        &mut self,
        pending: PendingMembershipCall,
        result: Result<MembershipView, LedgerError>,
    ) {
        if self.memberships.is_empty() {
            self.memberships = VecDeque::new();
        }
        pending.finish(result);
    }
    fn accept_membership(&mut self, call: MembershipCall, charge: Allocation) {
        let admitted = (|| {
            if self.memberships.len() >= self.config.pending_clients || self.stopping.is_some() {
                return Err(LedgerError::Capacity);
            }
            self.memberships
                .try_reserve(1)
                .map_err(|_| LedgerError::Capacity)?;
            let deadline = Instant::now()
                .checked_add(self.config.request_timeout)
                .ok_or(LedgerError::Capacity)?;
            if let Some(request) = &call.request {
                self.session.propose_membership(request)?;
            } else if !self.session.is_authoritative() {
                return Err(LedgerError::NotReady {
                    leader: self.session.status().leader_id,
                });
            }
            Ok(deadline)
        })();
        match admitted {
            Ok(deadline) => self.memberships.push_back(PendingMembershipCall {
                call,
                context: None,
                term: self.session.status().term,
                deadline,
                charge,
            }),
            Err(error) => {
                if self.memberships.is_empty() {
                    self.memberships = VecDeque::new();
                }
                drop(call.request);
                let _ = call.response.send(Err(error));
                drop(charge);
            }
        }
    }
    fn resolve_memberships(&mut self, events: &SessionEvents) -> Result<(), LedgerError> {
        let status = self.session.status();
        let count = self.memberships.len();
        for _ in 0..count {
            let Some(mut pending) = self.memberships.pop_front() else {
                break;
            };
            if pending.call.response.is_closed() {
                self.finish_membership(pending, Err(LedgerError::OutcomeUnknown));
                continue;
            }
            if pending.term != status.term
                || status.role != StateRole::Leader
                || Instant::now() >= pending.deadline
            {
                self.finish_membership(pending, Err(LedgerError::OutcomeUnknown));
                continue;
            }
            let receipt_ready = match pending
                .call
                .request
                .as_ref()
                .map(|request| self.session.membership_receipt(request))
                .transpose()
            {
                Ok(receipt) => receipt.is_none_or(|receipt| receipt.is_some()),
                Err(error) => {
                    self.finish_membership(pending, Err(error));
                    continue;
                }
            };
            if let Some(context) = &pending.context {
                if events
                    .read_barriers
                    .iter()
                    .any(|(observed, _)| observed == context)
                {
                    // A later configuration can supersede our bounded receipt
                    // while the read is in flight. Never attest the wrong one.
                    if !receipt_ready {
                        self.finish_membership(pending, Err(LedgerError::MembershipConflict));
                    } else {
                        let view = self.session.membership();
                        self.finish_membership(pending, view);
                    }
                    continue;
                }
            } else if receipt_ready {
                self.nonce = self.nonce.checked_add(1).ok_or(LedgerError::Capacity)?;
                let mut context = b"focal.membership.read.v1\0".to_vec();
                context.extend_from_slice(&self.nonce.to_be_bytes());
                match self.session.read_index(context.clone()) {
                    Ok(()) => pending.context = Some(context),
                    Err(
                        LedgerError::Capacity
                        | LedgerError::Consensus(focal_consensus::ConsensusError::Capacity),
                    ) => {}
                    Err(error) => {
                        self.finish_membership(pending, Err(error));
                        continue;
                    }
                }
            }
            self.memberships.push_back(pending);
        }
        Ok(())
    }
}
fn wall_ms() -> Result<u64, LedgerError> {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| LedgerError::Failed)?
            .as_millis(),
    )
    .map_err(|_| LedgerError::Failed)
}
