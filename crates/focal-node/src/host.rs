//! Bounded async ingress feeding one blocking session owner. Disk synchronization
//! and deterministic preparation never block the networking runtime's workers.
use crate::embedded::{EmbeddedNode, NodeError};
use focal_ledger::{LedgerError, Submission};
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use focal_model::*;
use focal_rt::watch;
use focal_wire::*;
use hyper_rt::sync::OneshotSender;
use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::mpsc,
    thread::JoinHandle,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

enum Work {
    Request(
        Box<VerifiedRequest>,
        OneshotSender<OwnedResponse>,
        Allocation,
    ),
    Stop(OneshotSender<Result<(), NodeError>>),
    /// Holds the owner until its gate opens or closes: a test's way to fill the ingress queue.
    #[cfg(test)]
    Hold(mpsc::Receiver<()>),
}
/// Requests the ingress queue holds, and native proposals the owner holds
/// unanswered: holding that many, it waits for the log before it takes
/// another request, so a slow disk slows callers rather than growing a queue.
const INGRESS: usize = 32;
/// What a request other than a native read reserves of the host's budget past its frames as it
/// waits for its reply (`handle`); a native read reserves `fleet::NATIVE_READ_PLACE_BYTES` past
/// its own.
const REQUEST_FLOOR: usize = 4096;
/// What the owner made of one request: its reply, for a native frame it proposed the proposal
/// whose commit the reply waits for, and for an inbox read whose page was empty what it waits on
/// (`host_inboxes`).
pub(crate) struct Dispatched {
    pub(crate) reply: ResponseEnvelope,
    pub(crate) proposal: Option<crate::native_ingress::Proposal>,
    pub(crate) wait: Option<crate::host_inboxes::InboxWait>,
}
#[cfg(test)]
impl Dispatched {
    /// The answer of a request that is not a native proposal.
    pub(crate) fn answered(self) -> ResponseEnvelope {
        if let Some(proposal) = self.proposal {
            panic!("parked proposal {proposal:?}");
        }
        self.reply
    }
}
/// A native proposal waiting for its commit, with its reply's header, the
/// caller's answer channel and the request's charge.
pub(crate) struct Waiting {
    pub(crate) header: ResponseEnvelope,
    pub(crate) proposal: crate::native_ingress::Proposal,
    /// Owner polls since the proposal; at `COMMIT_POLLS` it is answered
    /// with its ticket.
    pub(crate) polls: usize,
    pub(crate) response: OneshotSender<OwnedResponse>,
    pub(crate) charge: Allocation,
}
impl Waiting {
    fn finish(self, result: Response, limits: &WireLimits) {
        let mut header = self.header;
        header.result = result;
        let _ = self
            .response
            .send(finish_response(bounded(header, limits), self.charge));
    }
}
/// One poll of the session serves every parked proposal: the frames
/// proposed while the last flush was in flight go to the log in one append
/// (group commit), and each is answered once its outcome is the session's,
/// or with its ticket after `COMMIT_POLLS` polls, as a lone proposal is.
pub(crate) fn settle(
    node: &mut EmbeddedNode,
    waiting: &mut VecDeque<Waiting>,
    limits: &WireLimits,
) {
    let polled = node.session.poll().map(|_| ()).map_err(access);
    let count = waiting.len();
    for _ in 0..count {
        let Some(mut entry) = waiting.pop_front() else {
            break;
        };
        let result = match &polled {
            Err(error) => Some(Response::Error(error.clone())),
            Ok(()) => match crate::native_ingress::settled(&node.session, entry.proposal) {
                Ok(Some(response)) => Some(response),
                Ok(None) => {
                    entry.polls = entry.polls.saturating_add(1);
                    (entry.polls >= crate::native_ingress::COMMIT_POLLS)
                        .then(|| crate::native_ingress::ticket(entry.proposal))
                }
                Err(error) => Some(Response::Error(error)),
            },
        };
        match result {
            Some(response) => entry.finish(response, limits),
            None => waiting.push_back(entry),
        }
    }
}
/// Answer every parked proposal: each takes at most `COMMIT_POLLS` polls.
pub(crate) fn settle_all(
    node: &mut EmbeddedNode,
    waiting: &mut VecDeque<Waiting>,
    limits: &WireLimits,
) {
    for _ in 0..crate::native_ingress::COMMIT_POLLS {
        if waiting.is_empty() {
            return;
        }
        settle(node, waiting, limits);
    }
}
#[derive(Clone)]
pub struct LocalHost {
    sender: mpsc::SyncSender<Work>,
    /// Room in the ingress queue: each request takes a place before it is queued, and the owner
    /// gives it back as it takes the request (`INGRESS`).
    credit: hyper_rt::sync::Sender<()>,
    ended: watch::Receiver<()>,
    budget: MemoryBudget,
    limits: WireLimits,
    /// The hosted ledger runs native history; an embedded node activates
    /// offline, so this is fixed for the life of the host.
    native: bool,
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
        let host_budget = budget.clone();
        let native = node.session.activation().is_native();
        // A caller that finds the queue full waits its turn for room, as the queue's bound says (a
        // slow owner slows its callers): refused at once, 13 of 250 writes from 64 callers of an
        // embedded node were refused `Capacity` on a loaded host. Places for every caller the
        // budget lets wait at once, each holding at least the least a request reserves; one place
        // in the queue past the requests for the owner's stop.
        let least = REQUEST_FLOOR.min(crate::fleet::NATIVE_READ_PLACE_BYTES);
        let waiters = budget.limit().checked_div(least).unwrap_or(0).max(1);
        let (credit, mut credits) = hyper_rt::sync::channel_with(INGRESS, waiters)
            .map_err(|_| NodeError::Ledger(LedgerError::Capacity))?;
        let (sender, receiver) = mpsc::sync_channel(INGRESS.saturating_add(1));
        let mut waiting = VecDeque::new();
        waiting
            .try_reserve_exact(INGRESS)
            .map_err(|_| NodeError::Ledger(LedgerError::Capacity))?;
        let (liveness, ended) =
            watch::channel(()).map_err(|_| NodeError::Ledger(LedgerError::Capacity))?;
        let mut views = crate::reads::ReadViews::new();
        let mut inboxes = crate::host_inboxes::Inboxes::default();
        let mut streams =
            crate::streams::Streams::new().map_err(|error| NodeError::Domain(error.to_string()))?;
        let thread = std::thread::Builder::new()
            .name("focal-session-owner".into())
            .spawn(move || {
                let _liveness = liveness;
                let mut archive = crate::archive_agent::EmbeddedArchive::from_env();
                let mut next_tick = Instant::now();
                loop {
                    if Instant::now() >= next_tick {
                        if let Err(error) =
                            maintain(&mut node, &mut views, &mut archive, &host_budget)
                        {
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
                        // Sooner when the archive's next walk of the families is due.
                        next_tick = deadline.min(archive.due());
                        // A timer's delivery may have reached an inbox a read waits on.
                        inboxes.wake(&mut node, &mut views, &limits, Instant::now());
                    }
                    // Sooner when a parked inbox read's wait ends.
                    let wake_at = inboxes.earliest().map_or(next_tick, |at| at.min(next_tick));
                    // Every queued request is taken before the owner waits
                    // on the log; it blocks for a request only when no
                    // proposal waits for its commit.
                    let work = if waiting.is_empty() {
                        match receiver
                            .recv_timeout(wake_at.saturating_duration_since(Instant::now()))
                        {
                            Ok(work) => work,
                            Err(mpsc::RecvTimeoutError::Timeout) => {
                                inboxes.wake(&mut node, &mut views, &limits, Instant::now());
                                continue;
                            }
                            Err(mpsc::RecvTimeoutError::Disconnected) => {
                                inboxes.close(&limits);
                                break;
                            }
                        }
                    } else if waiting.len() < INGRESS {
                        match receiver.try_recv() {
                            Ok(work) => work,
                            Err(mpsc::TryRecvError::Empty) => {
                                settle(&mut node, &mut waiting, &limits);
                                inboxes.wake(&mut node, &mut views, &limits, Instant::now());
                                continue;
                            }
                            Err(mpsc::TryRecvError::Disconnected) => {
                                settle_all(&mut node, &mut waiting, &limits);
                                inboxes.close(&limits);
                                break;
                            }
                        }
                    } else {
                        settle(&mut node, &mut waiting, &limits);
                        inboxes.wake(&mut node, &mut views, &limits, Instant::now());
                        continue;
                    };
                    match work {
                        Work::Request(request, response, mut charge) => {
                            // Its place in the queue goes to the next caller waiting for one.
                            let _ = credits.try_recv();
                            // A native read's page is charged to the read as the owner builds
                            // it, object by object (`native_reads::PageObjects`).
                            // A vanished caller cannot cancel an already admitted mutation.
                            let dispatched = dispatch(
                                &mut node,
                                &mut views,
                                &mut streams,
                                *request,
                                &limits,
                                &host_budget,
                                &mut charge,
                            );
                            match (dispatched.proposal, dispatched.wait) {
                                (Some(proposal), _) => waiting.push_back(Waiting {
                                    header: dispatched.reply,
                                    proposal,
                                    polls: 0,
                                    response,
                                    charge,
                                }),
                                (None, Some(wait)) => {
                                    inboxes.park(dispatched.reply, wait, response, charge);
                                }
                                (None, None) => {
                                    let _ =
                                        response.send(finish_response(dispatched.reply, charge));
                                }
                            }
                            // What the request committed may have reached an inbox.
                            inboxes.wake(&mut node, &mut views, &limits, Instant::now());
                        }
                        Work::Stop(response) => {
                            settle_all(&mut node, &mut waiting, &limits);
                            inboxes.close(&limits);
                            let _ = response.send(node.checkpoint());
                            break;
                        }
                        #[cfg(test)]
                        Work::Hold(gate) => {
                            let _ = credits.try_recv();
                            let _ = gate.recv();
                        }
                    }
                }
            })?;
        Ok((
            Self {
                sender,
                credit,
                ended,
                budget,
                limits: host_limits,
                native,
            },
            HostOwner { thread },
        ))
    }
    /// Holds the owner until the returned sender sends or drops: a test's way to fill the ingress
    /// queue. Queued as a request is, its place taken first.
    #[cfg(test)]
    async fn hold_owner(&self) -> Option<mpsc::SyncSender<()>> {
        let (release, gate) = mpsc::sync_channel(1);
        self.credit.send(()).await.ok()?;
        self.sender.try_send(Work::Hold(gate)).ok()?;
        Some(release)
    }
    /// Completes on owner exit, including panic or fail-stop persistence errors.
    pub async fn closed(&self) {
        let mut ended = self.ended.clone();
        let _ = ended.changed().await;
    }
    /// Returns Capacity without enqueueing when ingress is full; callers can
    /// retry after draining ingress. No blocking task or extra thread is needed.
    pub async fn stop(&self) -> Result<(), NodeError> {
        let (send, receive) = hyper_rt::sync::oneshot()
            .map_err(|_| NodeError::Ledger(focal_ledger::LedgerError::Capacity))?;
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
    /// Joins without blocking the shard: the join waits on a thread of its
    /// own, and OS thread exhaustion is returned by Builder as an I/O error.
    pub async fn join_async(self) -> Result<(), NodeError> {
        let (send, receive) = hyper_rt::sync::oneshot()
            .map_err(|_| NodeError::Ledger(focal_ledger::LedgerError::Capacity))?;
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
    fn supports_participant_requests(&self) -> bool {
        true
    }
    fn supports_native_requests(&self) -> bool {
        self.native
    }
    fn supports_native_waits(&self) -> bool {
        self.native
    }
    fn handle<'a>(
        &'a self,
        request: &'a VerifiedRequest,
    ) -> Pin<Box<dyn Future<Output = ResponseEnvelope> + Send + 'a>> {
        Box::pin(async move { self.handle_accounted(request).await.into_envelope() })
    }
    fn handle_accounted<'a>(&'a self, request: &'a VerifiedRequest) -> OwnedHandlerFuture<'a> {
        // `request` is queued into the host work channel and outlives this call,
        // so clone once here (the transport no longer clones every request).
        let request = request.clone();
        Box::pin(async move {
            let fallback = request.request().reply(Response::Error(
                if matches!(
                    request.request().operation,
                    Operation::Summary
                        | Operation::Monitor { .. }
                        | Operation::Reconcile(_)
                        | Operation::RequestStreamRead { .. }
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
                Operation::Monitor { .. } => crate::monitor_reads::RESPONSE_BYTES,
                Operation::NativeRead(_) | Operation::NativeList(_) | Operation::NativeWait(_) => {
                    self.limits.max_frame_bytes as usize
                }
                Operation::Native { .. } => 4096,
                Operation::Summary => {
                    match crate::ledger_summary::response_bytes(self.limits.max_frame_bytes) {
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
                Operation::Read(ReadRequest {
                    query: ReadQuery::SeedScan { max_bytes, .. },
                    ..
                }) => match ((*max_bytes).min(self.limits.max_frame_bytes) as usize).checked_mul(2)
                {
                    Some(bytes) => bytes,
                    None => return OwnedResponse::new(full),
                },
                Operation::Read(_)
                | Operation::List(_)
                | Operation::Select(_)
                | Operation::Traverse(_)
                | Operation::Validators(_) => self.limits.max_frame_bytes as usize,
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
            let native_read = matches!(
                request.request().operation,
                Operation::NativeRead(_) | Operation::NativeList(_) | Operation::NativeWait(_)
            );
            let bytes = postcard::experimental::serialized_size(request.request())
                .ok()
                .and_then(|n| {
                    if native_read {
                        // A native read waits as its request and a place: each object its page
                        // takes is charged as the owner builds it (`native_reads::PageObjects`),
                        // and given back to what the reply holds as it is answered. Reserved for
                        // every read as it waited, 32 times the client's frame, the host's budget
                        // took two reads at once and refused the rest (a deliver load on one
                        // embedded node: 177 of 200 lifecycles abandoned, every event tail's page
                        // refused).
                        n.checked_mul(2)?
                            .checked_add(crate::fleet::NATIVE_READ_PLACE_BYTES)
                    } else {
                        n.checked_mul(32)
                            .and_then(|n| {
                                payload
                                    .checked_mul(32)
                                    .and_then(|reply| n.checked_add(reply))
                            })
                            .and_then(|n| n.checked_add(if content { 8 * 1024 * 1024 } else { 0 }))
                            .and_then(|n| n.checked_add(REQUEST_FLOOR))
                    }
                });
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
                }
                | Operation::Monitor { .. } => BudgetLane::Completion,
                Operation::Native { frame } => crate::native_ingress::lane(frame),
                _ => BudgetLane::Ordinary,
            };
            let Ok(charge) = self.budget.reserve(BudgetKind::Pending, lane, bytes) else {
                return OwnedResponse::new(full);
            };
            let Ok((send, receive)) = hyper_rt::sync::oneshot() else {
                return OwnedResponse::new(full);
            };
            // Its place in the queue, waited for in turn while the queue is full.
            match self.credit.send(()).await {
                Ok(()) => {}
                // No place left to wait in: every caller the budget lets wait already waits.
                Err(hyper_rt::sync::SyncError::Full(())) => return OwnedResponse::new(full),
                Err(_) => {
                    return OwnedResponse::new(
                        request
                            .request()
                            .reply(Response::Error(AccessError::Unavailable)),
                    );
                }
            }
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
    // A reply admitted with room to spare keeps only its conservative resident/encoding
    // allowance, so a small response does not pin what its request was admitted with. A native
    // page charged object by object as the owner built it (`native_reads::PageObjects`) holds
    // what its objects were charged, which is what its reply holds: kept, and grown to the
    // reply's encoding where that is more.
    let encoded = postcard::experimental::serialized_size(&header).ok();
    let retained = encoded
        .and_then(|n| n.checked_mul(32))
        .and_then(|n| n.checked_add(512));
    match (retained, encoded.and_then(|n| n.checked_add(512))) {
        (Some(bytes), _) if bytes <= charge.bytes() => {
            if charge.shrink_to(bytes).is_err() {
                header.result = Response::Error(AccessError::OutcomeUnknown);
            }
        }
        (_, Some(floor)) if floor <= charge.bytes() => {}
        (_, Some(floor)) => {
            if charge.grow(floor.saturating_sub(charge.bytes())).is_err() {
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
/// What a read barrier that could not be begun answers: nothing was taken,
/// so the node is unavailable for the read — a replica that leads no one
/// and knows no leader (an isolated leader once it stands down,
/// `ConsensusError::NotLeader`), a session that stopped — never an unknown
/// outcome, which is a mutation's word. Capacity and the client's standing
/// keep their names.
pub(crate) fn barrier_refused(error: LedgerError) -> AccessError {
    match error {
        LedgerError::Capacity
        | LedgerError::Behind
        | LedgerError::ResyncRequired
        | LedgerError::Consensus(focal_consensus::ConsensusError::Capacity) => access(error),
        _ => AccessError::Unavailable,
    }
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
fn maintain(
    node: &mut EmbeddedNode,
    views: &mut crate::reads::ReadViews,
    archive: &mut crate::archive_agent::EmbeddedArchive,
    budget: &MemoryBudget,
) -> Result<(), NodeError> {
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
    // Trusted timers of the native engine fire from this clock; a refusal
    // of one timer is that timer's outcome, not a maintenance failure.
    crate::native_timers::sweep(&mut node.session)?;
    // The archive agent of this node (26 §4, F12): released families retire
    // and closed outcomes seal here as on a network node, into this node's
    // own store.
    archive.maintain(node, budget)?;
    Ok(())
}
pub(crate) fn dispatch(
    node: &mut EmbeddedNode,
    views: &mut crate::reads::ReadViews,
    streams: &mut crate::streams::Streams,
    verified: VerifiedRequest,
    limits: &WireLimits,
    budget: &MemoryBudget,
    charge: &mut focal_memory::Allocation,
) -> Dispatched {
    let principal = verified.peer().principal();
    let peer = verified.peer();
    let request = verified.request();
    // Preserve only the small reply header. The admitted payload and peer grant
    // remain borrowed until mutation authentication consumes their ownership.
    let mut response = request.reply(Response::Error(AccessError::OutcomeUnknown));
    if let Err(error) = views.advance(&mut node.session) {
        return Dispatched {
            reply: request.reply(Response::Error(error)),
            proposal: None,
            wait: None,
        };
    }
    let mut proposal = None;
    let mut parked = None;
    let result = if request.ledger != node.identity.ledger {
        Err(AccessError::Unauthorized)
    } else if request.route_epoch != RouteEpoch(1)
        && matches!(
            request.operation,
            Operation::Summary
                | Operation::Monitor { .. }
                | Operation::Reconcile(_)
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
                .join(crate::network_admin::LOCAL_SOCKET)
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
                let protocol = verified.request().protocol;
                let mut input = verified.into_authenticated(authority)?;
                if let Some(known) = known_receipt(&node.session, &input)? {
                    return Ok(Response::Submitted(known));
                }
                if let Some(runtime) = crate::participant_ingress::authority(
                    &node.session,
                    protocol,
                    input.principal,
                    &input.command,
                    input.expected_revision,
                )? {
                    input.authority.runtime = runtime;
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
            Operation::Native { frame } => {
                crate::native_ingress::admit_local(node, peer, request, frame, &mut proposal)
            }
            // An archived object is read from the node's own custody (the
            // audit's F11); every other read from the session's core.
            Operation::NativeRead(read) => match &read.query {
                NativeReadQuery::Archived(query) => crate::archive_reads::page(
                    &node.content,
                    budget,
                    request.ledger,
                    peer,
                    request.route_epoch,
                    read,
                    query,
                    limits,
                    charge,
                ),
                NativeReadQuery::Sealed(query) => crate::archive_reads::sealed_page(
                    &node.content,
                    request.ledger,
                    peer,
                    request.route_epoch,
                    read,
                    query,
                    limits,
                    charge,
                ),
                NativeReadQuery::Cold(query) => crate::archive_reads::cold_page(
                    &mut node.session,
                    budget,
                    peer,
                    request.route_epoch,
                    read,
                    query,
                    limits,
                    charge,
                ),
                _ => crate::native_reads::local(
                    &mut node.session,
                    peer,
                    read,
                    request.request_id,
                    request.route_epoch,
                    limits,
                    charge,
                ),
            }
            .map(Response::NativeRead),
            Operation::NativeList(list) => crate::native_lists::local(
                &mut node.session,
                views,
                peer,
                list,
                request.route_epoch,
                limits,
                charge,
            )
            .map(Response::NativeListed),
            // A page with something on it, or a cursor to go on from, is answered now; an empty
            // one waits for a delivery to its recipient (`host_inboxes`).
            Operation::NativeWait(wait) => (|| {
                wait.validate(limits)?;
                let recipient = wait.recipient().ok_or(AccessError::InvalidRequest)?;
                let page = crate::native_lists::local(
                    &mut node.session,
                    views,
                    peer,
                    &wait.list,
                    request.route_epoch,
                    limits,
                    charge,
                )?;
                if page.objects.is_empty() && page.next.is_none() {
                    parked = Some(crate::host_inboxes::InboxWait {
                        recipient,
                        until: Instant::now()
                            .checked_add(Duration::from_millis(u64::from(wait.wait_ms)))
                            .ok_or(AccessError::Unavailable)?,
                        peer: peer.clone(),
                        list: wait.list.clone(),
                        route: request.route_epoch,
                    });
                }
                Ok(Response::NativeListed(page))
            })(),
            Operation::Monitor { id } => crate::monitor_reads::local(
                &mut node.session,
                principal,
                request.request_id,
                *id,
                request.route_epoch,
                limits,
            )
            .map(Response::Monitor),
            Operation::Summary => crate::ledger_summary::local(
                &mut node.session,
                principal,
                request.request_id,
                request.route_epoch,
                limits,
            )
            .map(Response::Summary),
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
            Operation::Select(list) => selection_scope(peer, node.session.ledger(), list)
                .and_then(|scope| {
                    views.selection(
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
            Operation::Validators(list) => validator_scope(peer, node.session.ledger(), list)
                .and_then(|scope| {
                    views.validators(
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
                .map(Response::Validators),
            Operation::Traverse(traversal) => {
                traversal_scope(peer, node.session.ledger(), traversal)
                    .and_then(|scope| {
                        views.traverse(
                            &mut node.session,
                            crate::reads::ListReadContext {
                                principal,
                                scope,
                                request_id: request.request_id,
                                barrier: None,
                            },
                            traversal,
                            limits,
                        )
                    })
                    .map(Response::Traversed)
            }
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
            | Operation::RaftOrdered { .. }
            | Operation::Control { .. }
            | Operation::PeerControl { .. }
            | Operation::NodeContact { .. }
            | Operation::EnrollmentControl { .. }
            | Operation::PlacementControl { .. }
            | Operation::SessionSign { .. }
            | Operation::RangeControl { .. }
            | Operation::SessionControl { .. }
            | Operation::Probe { .. }
            | Operation::Custody(_) => Err(AccessError::UnsupportedOperation),
        }
    };
    response.result = result.unwrap_or_else(Response::Error);
    Dispatched {
        reply: bounded(response, limits),
        proposal,
        wait: parked,
    }
}
/// The limit alone decides: the reply is encoded once, by the transport.
pub(crate) fn bounded(mut response: ResponseEnvelope, limits: &WireLimits) -> ResponseEnvelope {
    if payload_len(&response, limits.max_frame_bytes).is_err() {
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
    let maximum = focal_evidence::builtin_schema_limit(artifact.content.schema_hash)
        .map_err(|_| AccessError::UnsupportedOperation)?;
    let stored;
    let bytes = match &artifact.content.payload {
        ArtifactPayload::Inline(bytes) if bytes.len() <= 16 * 1024 => bytes.as_slice(),
        ArtifactPayload::Content(reference)
            if reference.domain == ContentDomainId(node.identity.ledger.tenant.0) =>
        {
            stored = node
                .content
                .read_bytes(reference, maximum)
                .map_err(|_| AccessError::InvalidRequest)?;
            stored.as_slice()
        }
        _ => return Err(AccessError::InvalidRequest),
    };
    // Shape admission grants no claim verdict. Arbitrary schema hashes cannot
    // supply a caller-authored `schema_valid` bit or install a validator.
    focal_evidence::verify_builtin_schema(artifact.content.schema_hash, bytes)
        .map_err(|_| AccessError::InvalidRequest)?;
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
#[path = "local_summary_tests.rs"]
mod summary_tests;

#[cfg(test)]
#[path = "reconciliation_tests.rs"]
mod reconciliation_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt;

    #[test]
    fn local_replies_retain_only_their_owned_bytes_through_owner_shutdown() {
        focal_rt::block_on(async move {
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
        })
        .unwrap()
    }

    /// Native reads waiting together are all served: each waits as its request and a place,
    /// and its page is funded as the owner builds it. Reserved 32 times the client's frame as it
    /// waited, the host's budget held two at once and refused the rest for capacity.
    #[test]
    fn native_reads_waiting_together_are_all_served() {
        focal_rt::block_on(async move {
            let root = tempfile::tempdir().unwrap();
            let mut settings = crate::config::Settings::default();
            settings.node.data_dir = Some(root.path().to_owned());
            crate::native_activation::activate_local(
                &settings,
                focal_ledger::NativeContentProfile::AuthoredV1,
            )
            .unwrap();
            let node = EmbeddedNode::open(&settings).unwrap();
            let identity = node.identity.clone();
            let limits = WireLimits::default();
            let (host, owner) = LocalHost::spawn(node, limits.clone()).unwrap();
            let peer = AuthenticatedPeer::local(PeerGrant {
                principal: identity.issuer,
                tenants: std::collections::BTreeSet::from([identity.ledger.tenant]),
                role: PeerRole::Runtime,
            })
            .unwrap();
            let reads = (1..=24u128).map(|id| {
                let request = RequestEnvelope {
                    protocol: focal_wire::NATIVE_PROTOCOL_VERSION,
                    ledger: identity.ledger,
                    route_epoch: RouteEpoch(1),
                    request_epoch: RequestEpoch(1),
                    request_id: RequestId::from_u128(id),
                    operation: Operation::NativeRead(focal_wire::NativeReadRequest {
                        consistency: ReadConsistency::Linearizable,
                        query: focal_wire::NativeReadQuery::Epochs(identity.issuer),
                        max_items: 1,
                    }),
                };
                focal_wire::dispatch_accounted(&host, peer.clone(), request, &limits)
            });
            let answers = futures_util::future::join_all(reads).await;
            for answer in &answers {
                assert!(
                    matches!(answer.envelope().result, Response::NativeRead(_)),
                    "{:?}",
                    answer.envelope().result
                );
            }
            drop(answers);
            host.stop().await.unwrap();
            owner.join().unwrap();
            assert_eq!(host.budget.stats().used, 0);
        })
        .unwrap()
    }

    /// The requests answered in `requests_that_find_the_ingress_full_wait_for_room_and_are_served`.
    static ANSWERED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

    /// Requests that find the ingress queue full wait their turn for room and are served, as the
    /// queue's bound says: a slow owner slows its callers. Do: hold the owner, send the queue's
    /// worth of reads and eight more, let each run to its wait, then let the owner go. Expect:
    /// none answered while the owner is held, every one served after. Refused at once, the eight
    /// past the queue were answered `Capacity` (13 of 250 writes from 64 callers on a loaded host).
    #[test]
    fn requests_that_find_the_ingress_full_wait_for_room_and_are_served() {
        focal_rt::block_on(async move {
            let root = tempfile::tempdir().unwrap();
            let mut settings = crate::config::Settings::default();
            settings.node.data_dir = Some(root.path().to_owned());
            crate::native_activation::activate_local(
                &settings,
                focal_ledger::NativeContentProfile::AuthoredV1,
            )
            .unwrap();
            let node = EmbeddedNode::open(&settings).unwrap();
            let identity = node.identity.clone();
            let limits = WireLimits::default();
            let (host, owner) = LocalHost::spawn(node, limits.clone()).unwrap();
            let peer = AuthenticatedPeer::local(PeerGrant {
                principal: identity.issuer,
                tenants: std::collections::BTreeSet::from([identity.ledger.tenant]),
                role: PeerRole::Runtime,
            })
            .unwrap();
            // Native reads, which the owner builds: each waits as its request and a place.
            let read = |id: u128| RequestEnvelope {
                protocol: focal_wire::NATIVE_PROTOCOL_VERSION,
                ledger: identity.ledger,
                route_epoch: RouteEpoch(1),
                request_epoch: RequestEpoch(1),
                request_id: RequestId::from_u128(id),
                operation: Operation::NativeRead(focal_wire::NativeReadRequest {
                    consistency: ReadConsistency::Linearizable,
                    query: focal_wire::NativeReadQuery::Epochs(identity.issuer),
                    max_items: 1,
                }),
            };
            let release = host.hold_owner().await.unwrap();
            let mut tasks = Vec::new();
            for id in 1..=u128::try_from(INGRESS + 8).unwrap() {
                let (host, peer, limits, request) =
                    (host.clone(), peer.clone(), limits.clone(), read(id));
                tasks.push(
                    focal_rt::spawn(async move {
                        let reply =
                            focal_wire::dispatch_accounted(&host, peer, request, &limits).await;
                        ANSWERED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        matches!(reply.envelope().result, Response::NativeRead(_))
                    })
                    .unwrap(),
                );
            }
            // Each runs to its wait, a place in the queue or the reply the held owner owes: a
            // yield comes back after the shard's other tasks.
            for _ in 0..4 {
                hyper_rt::futures::yield_now().await;
            }
            assert_eq!(
                ANSWERED.load(std::sync::atomic::Ordering::Relaxed),
                0,
                "a request was answered while the owner was held"
            );
            release.send(()).unwrap();
            for task in tasks {
                assert!(task.await.unwrap(), "a request was not served");
            }
            host.stop().await.unwrap();
            owner.join().unwrap();
        })
        .unwrap()
    }

    #[test]
    fn shutdown_backpressure_and_closed_owner_need_no_runtime_or_worker() {
        let (sender, receiver) = mpsc::sync_channel(1);
        let (_liveness, ended) = watch::channel(()).unwrap();
        let (credit, _credits) = hyper_rt::sync::channel(1).unwrap();
        let host = LocalHost {
            sender,
            credit,
            ended,
            budget: MemoryBudget::new(64 * 1024 * 1024, 8 * 1024 * 1024).unwrap(),
            limits: WireLimits::default(),
            native: false,
        };
        let (send, _receive) = hyper_rt::sync::oneshot().unwrap();
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
