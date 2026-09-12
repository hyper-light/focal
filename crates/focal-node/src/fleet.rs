//! Replicated session owner. Ingress, replication and disk apply use separate
//! bounded queues. Client success waits for quorum commit and graph publication.
//! Directory/control code supplies the already authorized Session and route epoch.
use crate::{custody::CustodyScope, evidence_service::EvidenceWitness};
use crate::{
    host::{access, finish_response, known_receipt},
    reads::{ListReadContext, ReadViews},
    streams::{PendingStream, Streams},
};
use focal_consensus::PbMessageExt as _;
use focal_consensus::StateRole;
#[path = "fleet_diagnostics.rs"]
mod diagnostics;
pub use diagnostics::ReplicaDiagnosticsReply;
#[path = "fleet_registration.rs"]
mod registration;
use focal_ledger::{
    LedgerError, ManagedSubmission, RequestStreamSubmission, Session, SessionEvents, Submission,
};
use focal_ledger::{LegacyImportPayloads, PendingImport};
pub use focal_ledger::{MembershipView, SessionMembershipReceipt, SessionMembershipRequest};
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use focal_model::*;
use focal_wire::*;
pub use registration::RegistrationFactsReply;
use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::mpsc,
    thread::JoinHandle,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{mpsc as async_mpsc, oneshot, watch};

#[path = "fleet_evidence.rs"]
mod evidence_owner;
#[path = "fleet_managed_support.rs"]
mod managed_support_owner;
pub use managed_support_owner::ManagedSupportReply;
use managed_support_owner::SupportCall;
#[path = "fleet_group.rs"]
mod grouped;
#[path = "fleet_placement.rs"]
mod placement_owner;
#[path = "fleet_range.rs"]
mod range_owner;
use evidence_owner::{EvidenceCall, PendingEvidenceCall};
pub use grouped::management::{
    FleetError, FleetIncarnation, FleetInstallFailure, FleetInstallation, FleetManager,
    FleetRemoval, FleetReply, FleetStatus, ManagedFleetConfig,
};
pub use grouped::{FleetReplica, FleetReplication, FleetTenant, ReplicaFleet, ReplicaFleetParts};
pub use placement_owner::{CommittedPlacement, PlacementReply, SessionPlacementRequest};
use placement_owner::{PendingPlacementCall, PlacementCall};
pub use range_owner::{
    ArchivedFamily, RANGE_CONTROL_SCHEMA, RangeControlReply, RangeControlRequest, RangeFact,
    RangeFactRequest, RangeHistoryView, RangeMemberView, RangePendingView, RangeView,
};
pub(crate) use range_owner::{verify_fact, verify_progress};

#[cfg(test)]
#[path = "fleet_completion_tests.rs"]
mod completion_tests;
#[cfg(test)]
#[path = "fleet_import_tests.rs"]
mod import_tests;
#[cfg(test)]
#[path = "fleet_native_tests.rs"]
mod native_tests;

#[cfg(test)]
#[path = "fleet_stop_tests.rs"]
mod stop_tests;

#[cfg(test)]
#[path = "fleet_async_tests.rs"]
mod async_tests;

#[cfg(test)]
#[path = "fleet_membership_tests.rs"]
mod membership_tests;

#[cfg(test)]
#[path = "fleet_evidence_tests.rs"]
mod evidence_tests;

#[cfg(test)]
#[path = "fleet_admission_tests.rs"]
mod admission_tests;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicaConfig {
    pub root: RootCommandId,
    pub route_epoch: RouteEpoch,
    pub policy_revision: u64,
    /// This node's enrolled generation, the identity its range facts carry.
    pub node_generation: u64,
    pub queue_items: usize,
    pub pending_clients: usize,
    pub replication_queue: usize,
    pub tick: Duration,
    pub request_timeout: Duration,
    /// Checkpoint and compact the log once this many entries have applied
    /// past the last snapshot (26 §3, the log's retirement boundary).
    pub checkpoint_after_entries: u64,
    #[cfg(test)]
    checkpoint_observer: Option<CheckpointObserver>,
}
#[cfg(test)]
#[derive(Clone, Debug)]
struct CheckpointObserver(async_mpsc::Sender<LedgerId>);
#[cfg(test)]
impl PartialEq for CheckpointObserver {
    fn eq(&self, other: &Self) -> bool {
        self.0.same_channel(&other.0)
    }
}
#[cfg(test)]
impl Eq for CheckpointObserver {}
impl ReplicaConfig {
    pub fn new(root: RootCommandId) -> Self {
        Self {
            root,
            route_epoch: RouteEpoch(1),
            policy_revision: 1,
            node_generation: 1,
            queue_items: 32,
            pending_clients: 128,
            replication_queue: 128,
            tick: Duration::from_millis(100),
            request_timeout: Duration::from_secs(5),
            checkpoint_after_entries: 4096,
            #[cfg(test)]
            checkpoint_observer: None,
        }
    }
}

pub struct ReplicationFrame {
    pub target: u64,
    pub request: RequestEnvelope,
    snapshot: Option<oneshot::Sender<focal_consensus::SnapshotStatus>>,
    _charge: Allocation,
}
impl ReplicationFrame {
    pub(crate) fn report_snapshot(&mut self, accepted: bool) {
        crate::snapshot_feedback::complete(&mut self.snapshot, accepted);
    }
}
#[derive(Clone, Debug)]
pub struct ReplicaProgress {
    pub node: u64,
    pub leader: u64,
    pub term: u64,
    pub sequence: SessionSeq,
    pub dropped_replication: u64,
    pub stopped: bool,
    /// The route epoch this replica serves clients at; a committed
    /// activation moves the session ahead of it until the host re-fences.
    pub route_epoch: RouteEpoch,
    /// A native import this replica cannot apply until its host seals the
    /// inline legacy payloads with the recorded chunking (23 §5.2).
    pub import_pending: Option<PendingImport>,
    /// A seeded checkpoint this replica cannot install until its host pulls
    /// the chunks it lacks from a peer (25 §5): the snapshot's Raft
    /// coordinates and how many chunks are missing.
    pub seed_pending: Option<SeedPending>,
    /// A retained delivery this replica cannot apply until its host pulls
    /// the content objects it names from a required copy (24 §20).
    pub custody_pending: Option<CustodyPending>,
}
/// The objects a retained delivery still lacks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CustodyPending {
    pub missing: usize,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SeedPending {
    pub index: u64,
    pub term: u64,
    pub missing: usize,
}
/// The content parameters an activation proposal records for an import.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActivateNativeCall {
    /// Profile of a genesis activation; a populated prefix is always imported
    /// with the projection profile (23 §5).
    pub profile: focal_ledger::NativeContentProfile,
    pub chunk_bytes: usize,
    pub max_manifest_bytes: usize,
}
/// Largest total of inline legacy payloads copied out for host sealing.
const IMPORT_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;
enum Work {
    Diagnostics(
        oneshot::Sender<Result<ReplicaDiagnosticsReply, LedgerError>>,
        Allocation,
    ),
    Registration(
        oneshot::Sender<Result<RegistrationFactsReply, LedgerError>>,
        Allocation,
    ),
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
    Transfer(
        u64,
        Option<Box<CheckedTransfer>>,
        oneshot::Sender<Result<(), LedgerError>>,
    ),
    Membership(Box<MembershipCall>, Allocation),
    ManagedSupport(Box<SupportCall>, Allocation),
    ActivateNative(
        ActivateNativeCall,
        oneshot::Sender<Result<(), LedgerError>>,
        Allocation,
    ),
    ImportPayloads(
        oneshot::Sender<Result<Option<LegacyImportPayloads>, LedgerError>>,
        Allocation,
    ),
    /// Checkpoint the applied prefix now (an operator's explicit request).
    Checkpoint(oneshot::Sender<Result<(), LedgerError>>, Allocation),
    /// Where a committed artifact's bytes live, for its custody obligation.
    ArtifactPointer(
        focal_model::ArtifactId,
        oneshot::Sender<
            Result<
                Option<focal_model::lifecycle::artifact_descriptor::ContentPointer>,
                LedgerError,
            >,
        >,
        Allocation,
    ),
    /// The chunks a pending seeded checkpoint still lacks.
    SeedChunks(
        oneshot::Sender<Result<Vec<focal_model::ContentHash>, LedgerError>>,
        Allocation,
    ),
    /// One pulled chunk, verified and sealed; the delivery retries at once.
    InstallSeed(
        focal_model::ContentHash,
        Vec<u8>,
        oneshot::Sender<Result<(), LedgerError>>,
        Allocation,
    ),
    /// The content objects a retained delivery lacks (24 §20).
    CustodyObjects(
        oneshot::Sender<Result<Vec<focal_model::ContentRef>, LedgerError>>,
        Allocation,
    ),
    /// The host pulled objects a retained delivery lacked; it retries at once.
    CustodyPulled(oneshot::Sender<Result<(), LedgerError>>, Allocation),
    /// Serve clients at an activated route: `(route, policy revision)`.
    Refence(
        RouteEpoch,
        u64,
        oneshot::Sender<Result<(), LedgerError>>,
        Allocation,
    ),
    Placement(Box<PlacementCall>, Allocation),
    /// Range movement: the operator's view and move, the controller's steps,
    /// a replica's own facts (25 §6).
    Range(Box<range_owner::RangeCall>, Allocation),
    Evidence(Box<EvidenceCall>, Allocation),
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
    native: Option<focal_evidence::VerifiedNativeArtifact>,
}
struct MembershipCall {
    request: Option<SessionMembershipRequest>,
    response: oneshot::Sender<Result<MembershipReply, LedgerError>>,
}
struct CheckedTransfer {
    expected_index: u64,
    expected: focal_consensus::MembershipConfiguration,
    _charge: Allocation,
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
    proposed: bool,
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
    let command = match &request.request().operation {
        Operation::RequestStreamControl {
            command:
                RequestStreamCommand::Acknowledge { .. }
                | RequestStreamCommand::Seal { .. }
                | RequestStreamCommand::Close { .. },
            ..
        }
        | Operation::ManagedSupport { .. }
        | Operation::Monitor { .. } => return true,
        Operation::Submit { command, .. }
        | Operation::Managed {
            operation: ManagedOperation::Submit { command, .. },
            ..
        } => command,
        _ => return false,
    };
    focal_ledger::mutation_lane(command) == BudgetLane::Completion
}
#[derive(Clone)]
enum HostSender {
    Direct(mpsc::SyncSender<Work>),
    Group {
        ledger: LedgerId,
        incarnation: u64,
        sender: mpsc::SyncSender<grouped::FleetInput>,
        slots: MemoryBudget,
        _backing: std::sync::Arc<Allocation>,
    },
}
impl HostSender {
    fn try_send(&self, work: Work) -> Result<(), HostQueueError> {
        match self {
            Self::Direct(sender) => sender.try_send(work).map_err(host_queue_error),
            Self::Group {
                ledger,
                incarnation,
                sender,
                slots,
                ..
            } => {
                let lane = grouped::lane(&work);
                let Ok(slot) = slots.reserve(BudgetKind::Pending, lane, 1) else {
                    return Err(HostQueueError::Full);
                };
                sender
                    .try_send(grouped::FleetInput::Routed(grouped::Routed {
                        ledger: *ledger,
                        incarnation: *incarnation,
                        work,
                        _slot: slot.commit(),
                    }))
                    .map_err(host_queue_error)
            }
        }
    }
}
enum HostQueueError {
    Full,
    Disconnected,
}
fn host_queue_error<T>(error: mpsc::TrySendError<T>) -> HostQueueError {
    match error {
        mpsc::TrySendError::Full(_) => HostQueueError::Full,
        mpsc::TrySendError::Disconnected(_) => HostQueueError::Disconnected,
    }
}
#[derive(Clone)]
pub struct ReplicaHost {
    sender: HostSender,
    progress: watch::Receiver<ProgressState>,
    budget: MemoryBudget,
    client_frame_bytes: u32,
    client_max_items: u32,
    request_timeout: Duration,
}
struct ProgressState {
    value: ReplicaProgress,
    // The existing watch owns the allocation across owner, cloned handles and
    // delayed replies. Replacing only value preserves incarnation accounting.
    _allocation: Option<Allocation>,
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
    NativeMutation {
        key: RequestKey,
        outcome: focal_core::native::NativeOutcome,
    },
    NativeRead {
        correlation: focal_ledger::ReadCorrelation,
        principal: ParticipantId,
        role: NativePeerRole,
        profile: NativeProfile,
        read: NativeReadRequest,
    },
    ManagedMutation {
        key: ManagedRequestKey,
        intent: ContentHash,
        family: ManagedRequestFamily,
    },
    ManagedStream {
        stream: PendingStream,
        key: ManagedRequestKey,
        intent: ContentHash,
    },
    RequestStreamControl {
        input: Box<RequestStreamControlInput>,
        context: Option<Vec<u8>>,
    },
    RequestStreamRead {
        context: Vec<u8>,
        principal: ParticipantId,
        cluster: [u8; 16],
        query: RequestStreamQuery,
    },
    PeerPersistence,
    Read {
        context: Vec<u8>,
        principal: ParticipantId,
        read: ReadRequest,
    },
    List {
        context: Vec<u8>,
        principal: ParticipantId,
        scope: ContentHash,
        list: ListRequest,
    },
    Select {
        context: Vec<u8>,
        principal: ParticipantId,
        scope: ContentHash,
        list: SelectionRequest,
    },
    Validators {
        context: Vec<u8>,
        principal: ParticipantId,
        scope: ContentHash,
        list: ValidatorRequest,
    },
    Traverse {
        context: Vec<u8>,
        principal: ParticipantId,
        scope: ContentHash,
        traversal: TraversalRequest,
    },
    Summary {
        context: Vec<u8>,
    },
    Monitor {
        context: Vec<u8>,
        id: MonitorId,
    },
    Reconcile {
        context: Vec<u8>,
        principal: ParticipantId,
        query: ReconcileQuery,
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
    deferred_managed: VecDeque<managed_support_owner::DeferredManaged>,
    deferred_backing: Option<Allocation>,
    snapshot_feedback: crate::snapshot_feedback::SnapshotFeedback,
    placement: Option<PendingPlacementCall>,
    evidence: Option<PendingEvidenceCall>,
    outbound: async_mpsc::Sender<ReplicationFrame>,
    progress: watch::Sender<ProgressState>,
    incarnation: u64,
    nonce: u64,
    support_cursor: u64,
    dropped: u64,
    /// A member was added after the log was compacted: the next checkpoint
    /// is due so the snapshot that seeds it names it in its configuration.
    checkpoint_due: bool,
    #[cfg(test)]
    dropped_snapshots: u64,
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
        if session
            .active_route()
            .is_some_and(|route| route != config.route_epoch)
        {
            return Err(LedgerError::PlacementConflict);
        }
        let status = session.status();
        let (progress, changes) = watch::channel(ProgressState {
            value: ReplicaProgress {
                node: status.node_id,
                leader: status.leader_id,
                term: status.term,
                sequence: session.sequence(),
                dropped_replication: 0,
                stopped: false,
                route_epoch: config.route_epoch,
                import_pending: None,
                seed_pending: None,
                custody_pending: None,
            },
            _allocation: None,
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
        let client_max_items = client_limits.max_items;
        let request_timeout = config.request_timeout;
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
            deferred_managed: VecDeque::new(),
            deferred_backing: None,
            snapshot_feedback: crate::snapshot_feedback::SnapshotFeedback::default(),
            placement: None,
            evidence: None,
            outbound,
            progress,
            incarnation: 0,
            nonce: 0,
            support_cursor: 0,
            dropped: 0,
            checkpoint_due: false,
            #[cfg(test)]
            dropped_snapshots: 0,
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
                client_max_items,
                request_timeout,
            },
            owner,
        ))
    }
    pub fn progress(&self) -> ReplicaProgress {
        self.progress.borrow().value.clone()
    }
    pub fn memory_stats(&self) -> focal_memory::BudgetStats {
        self.budget.stats()
    }
    pub async fn closed(&self) {
        let mut changes = self.progress.clone();
        while !changes.borrow().value.stopped {
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
            .try_send(Work::Transfer(target, None, send))
            .map_err(|error| match error {
                HostQueueError::Full => LedgerError::Capacity,
                HostQueueError::Disconnected => LedgerError::Failed,
            })?;
        receive.await.map_err(|_| LedgerError::Failed)?
    }
    /// OS/control-owner transfer with an exact fence checked at owner dispatch.
    /// Success remains initiation, not an observed election or durable receipt.
    pub async fn transfer_leader_checked(
        &self,
        request: focal_control::ControlTransfer,
    ) -> Result<(), LedgerError> {
        let bytes = request
            .expected
            .charged_bytes()?
            .checked_add(192 * 1024)
            .ok_or(LedgerError::Capacity)?;
        let charge = self
            .budget
            .reserve(BudgetKind::Control, BudgetLane::Completion, bytes)?
            .commit();
        request.expected.validate()?;
        let (send, receive) = oneshot::channel();
        self.sender
            .try_send(Work::Transfer(
                request.target,
                Some(Box::new(CheckedTransfer {
                    expected_index: request.expected_configuration_index,
                    expected: request.expected,
                    _charge: charge,
                })),
                send,
            ))
            .map_err(|error| match error {
                HostQueueError::Full => LedgerError::Capacity,
                HostQueueError::Disconnected => LedgerError::Failed,
            })?;
        receive.await.map_err(|_| LedgerError::OutcomeUnknown)?
    }
    /// Propose the replicated activation of native history on this ledger's
    /// authority. Completion is observed through diagnostics: the record must
    /// commit and apply on every replica before native admission opens.
    pub async fn activate_native(&self, call: ActivateNativeCall) -> Result<(), LedgerError> {
        let charge = self
            .budget
            .reserve(BudgetKind::Control, BudgetLane::Completion, 64 * 1024)?
            .commit();
        let (send, receive) = oneshot::channel();
        self.sender
            .try_send(Work::ActivateNative(call, send, charge))
            .map_err(|error| match error {
                HostQueueError::Full => LedgerError::Capacity,
                HostQueueError::Disconnected => LedgerError::Failed,
            })?;
        receive.await.map_err(|_| LedgerError::OutcomeUnknown)?
    }
    /// Serve clients at the route a committed activation moved the session
    /// to ([24](../../../docs/archictecutre/24-placement-execution-and-fleet-control.md) §17):
    /// the serving fence and the read views follow the session's active
    /// route. Refused while the session is not at that route or a cutover
    /// is still pending; a route behind the served one is a conflict.
    pub async fn refence(
        &self,
        route_epoch: RouteEpoch,
        policy_revision: u64,
    ) -> Result<(), LedgerError> {
        if route_epoch.0 == 0 || policy_revision == 0 {
            return Err(LedgerError::PlacementConflict);
        }
        let charge = self
            .budget
            .reserve(BudgetKind::Control, BudgetLane::Completion, 16 * 1024)?
            .commit();
        let (send, receive) = oneshot::channel();
        self.sender
            .try_send(Work::Refence(route_epoch, policy_revision, send, charge))
            .map_err(|error| match error {
                HostQueueError::Full => LedgerError::Capacity,
                HostQueueError::Disconnected => LedgerError::Failed,
            })?;
        receive.await.map_err(|_| LedgerError::OutcomeUnknown)?
    }
    /// The inline legacy payloads a host must seal before this ledger's
    /// populated prefix can be imported; `None` when the prefix is empty.
    pub async fn import_payloads(&self) -> Result<Option<LegacyImportPayloads>, LedgerError> {
        let charge = self
            .budget
            .reserve(
                BudgetKind::Control,
                BudgetLane::Completion,
                IMPORT_PAYLOAD_BYTES.saturating_add(64 * 1024),
            )?
            .commit();
        let (send, receive) = oneshot::channel();
        self.sender
            .try_send(Work::ImportPayloads(send, charge))
            .map_err(|error| match error {
                HostQueueError::Full => LedgerError::Capacity,
                HostQueueError::Disconnected => LedgerError::Failed,
            })?;
        receive.await.map_err(|_| LedgerError::OutcomeUnknown)?
    }
    /// Checkpoint the replica's applied prefix now. The envelope is encoded
    /// and installed on the replica's worker before this returns, so the
    /// other sessions of that worker wait for it; refused (`Capacity`) while a
    /// proposal or delivery is still pending, in which case the operator
    /// retries. A native Core root beyond the inline bound is sealed as
    /// seeds (25 §5).
    pub async fn checkpoint(&self) -> Result<(), LedgerError> {
        let charge = self
            .budget
            .reserve(BudgetKind::Control, BudgetLane::Completion, 64 * 1024)?
            .commit();
        let (send, receive) = oneshot::channel();
        self.sender
            .try_send(Work::Checkpoint(send, charge))
            .map_err(|error| match error {
                HostQueueError::Full => LedgerError::Capacity,
                HostQueueError::Disconnected => LedgerError::Failed,
            })?;
        receive.await.map_err(|_| LedgerError::OutcomeUnknown)?
    }
    /// Where a committed artifact's bytes live (its domain, root and length),
    /// `None` for an artifact this replica's committed prefix does not hold.
    pub async fn artifact_pointer(
        &self,
        artifact: focal_model::ArtifactId,
    ) -> Result<Option<focal_model::lifecycle::artifact_descriptor::ContentPointer>, LedgerError>
    {
        let charge = self
            .budget
            .reserve(BudgetKind::Control, BudgetLane::Completion, 4 * 1024)?
            .commit();
        let (send, receive) = oneshot::channel();
        self.sender
            .try_send(Work::ArtifactPointer(artifact, send, charge))
            .map_err(|error| match error {
                HostQueueError::Full => LedgerError::Capacity,
                HostQueueError::Disconnected => LedgerError::Failed,
            })?;
        receive.await.map_err(|_| LedgerError::OutcomeUnknown)?
    }
    /// The chunks of a pending seeded checkpoint this replica still lacks.
    pub async fn pending_seed_chunks(&self) -> Result<Vec<focal_model::ContentHash>, LedgerError> {
        let charge = self
            .budget
            .reserve(BudgetKind::Control, BudgetLane::Completion, 64 * 1024)?
            .commit();
        let (send, receive) = oneshot::channel();
        self.sender
            .try_send(Work::SeedChunks(send, charge))
            .map_err(|error| match error {
                HostQueueError::Full => LedgerError::Capacity,
                HostQueueError::Disconnected => LedgerError::Failed,
            })?;
        receive.await.map_err(|_| LedgerError::OutcomeUnknown)?
    }
    /// Hand the replica one pulled seed chunk; it is verified against its
    /// hash, sealed, and the retained delivery retries at once.
    pub async fn install_seed_chunk(
        &self,
        hash: focal_model::ContentHash,
        bytes: Vec<u8>,
    ) -> Result<(), LedgerError> {
        let charge = self
            .budget
            .reserve(
                BudgetKind::Control,
                BudgetLane::Completion,
                bytes
                    .capacity()
                    .checked_add(64 * 1024)
                    .ok_or(LedgerError::Capacity)?,
            )?
            .commit();
        let (send, receive) = oneshot::channel();
        self.sender
            .try_send(Work::InstallSeed(hash, bytes, send, charge))
            .map_err(|error| match error {
                HostQueueError::Full => LedgerError::Capacity,
                HostQueueError::Disconnected => LedgerError::Failed,
            })?;
        receive.await.map_err(|_| LedgerError::OutcomeUnknown)?
    }
    /// The content objects a retained delivery lacks (24 §20), for the host
    /// to pull from a required copy.
    pub async fn pending_custody_objects(
        &self,
    ) -> Result<Vec<focal_model::ContentRef>, LedgerError> {
        let charge = self
            .budget
            .reserve(BudgetKind::Control, BudgetLane::Completion, 64 * 1024)?
            .commit();
        let (send, receive) = oneshot::channel();
        self.sender
            .try_send(Work::CustodyObjects(send, charge))
            .map_err(|error| match error {
                HostQueueError::Full => LedgerError::Capacity,
                HostQueueError::Disconnected => LedgerError::Failed,
            })?;
        receive.await.map_err(|_| LedgerError::OutcomeUnknown)?
    }
    /// Tell the replica its host pulled objects a retained delivery lacked;
    /// the delivery retries at once.
    pub async fn custody_pulled(&self) -> Result<(), LedgerError> {
        let charge = self
            .budget
            .reserve(BudgetKind::Control, BudgetLane::Completion, 4096)?
            .commit();
        let (send, receive) = oneshot::channel();
        self.sender
            .try_send(Work::CustodyPulled(send, charge))
            .map_err(|error| match error {
                HostQueueError::Full => LedgerError::Capacity,
                HostQueueError::Disconnected => LedgerError::Failed,
            })?;
        receive.await.map_err(|_| LedgerError::OutcomeUnknown)?
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
                HostQueueError::Full => LedgerError::Capacity,
                HostQueueError::Disconnected => LedgerError::Failed,
            })?;
        receive.await.map_err(|_| LedgerError::OutcomeUnknown)?
    }
    /// A full queue returns Capacity without enqueueing; drain ingress and retry.
    pub async fn stop(&self) -> Result<(), LedgerError> {
        let (send, receive) = oneshot::channel();
        self.sender
            .try_send(Work::Stop(send))
            .map_err(|error| match error {
                HostQueueError::Full => LedgerError::Capacity,
                HostQueueError::Disconnected => LedgerError::Failed,
            })?;
        receive.await.map_err(|_| LedgerError::Failed)?
    }
}
impl RequestHandler for ReplicaHost {
    fn supports_managed_requests(&self) -> bool {
        true
    }
    fn supports_participant_requests(&self) -> bool {
        true
    }
    fn supports_native_requests(&self) -> bool {
        true
    }
    fn handle<'a>(
        &'a self,
        request: &'a VerifiedRequest,
    ) -> Pin<Box<dyn Future<Output = ResponseEnvelope> + Send + 'a>> {
        // The request is queued into `Work` and outlives this call, so it is
        // owned from here; clone once at the queue boundary (the transport no
        // longer clones every request).
        Box::pin(async move {
            self.submit_inner(request.clone(), None, None)
                .await
                .into_envelope()
        })
    }
    fn handle_accounted<'a>(&'a self, request: &'a VerifiedRequest) -> OwnedHandlerFuture<'a> {
        Box::pin(self.submit_inner(request.clone(), None, None))
    }
}
impl ReplicaHost {
    pub(crate) async fn submit_with_evidence(
        &self,
        request: VerifiedRequest,
        witness: EvidenceWitness,
    ) -> OwnedResponse {
        self.submit_inner(request, Some(witness), None).await
    }
    /// Submit an artifact-bearing native frame whose payload the content host
    /// has sealed and verified; the owner checks the evidence against the frame.
    pub(crate) async fn submit_with_native_evidence(
        &self,
        request: VerifiedRequest,
        evidence: focal_evidence::VerifiedNativeArtifact,
    ) -> OwnedResponse {
        self.submit_inner(request, None, Some(evidence)).await
    }
    async fn submit_inner(
        &self,
        request: VerifiedRequest,
        witness: Option<EvidenceWitness>,
        native: Option<focal_evidence::VerifiedNativeArtifact>,
    ) -> OwnedResponse {
        let unknown = request.request().reply(Response::Error(
            if matches!(
                request.request().operation,
                Operation::Summary
                    | Operation::Monitor { .. }
                    | Operation::Reconcile(_)
                    | Operation::RequestStreamRead { .. }
                    | Operation::ManagedSupport { .. }
                    | Operation::NativeRead(_)
                    | Operation::NativeList(_)
            ) {
                AccessError::Unavailable
            } else {
                AccessError::OutcomeUnknown
            },
        ));
        let full = request
            .request()
            .reply(Response::Error(AccessError::Capacity));
        let closed = request
            .request()
            .reply(Response::Error(AccessError::Unavailable));
        let replication = matches!(request.request().operation, Operation::Raft { .. });
        let response_bytes = match &request.request().operation {
            Operation::Monitor { .. } => crate::monitor_reads::RESPONSE_BYTES,
            Operation::Summary => {
                match crate::ledger_summary::response_bytes(self.client_frame_bytes) {
                    Some(bytes) => bytes,
                    None => return OwnedResponse::new(full),
                }
            }
            Operation::Reconcile(query) => match crate::reconciliation::response_bytes(
                query,
                self.client_frame_bytes,
                self.client_max_items,
            ) {
                Some(bytes) => bytes,
                None => return OwnedResponse::new(full),
            },
            operation @ (Operation::Managed { .. }
            | Operation::RequestStreamControl { .. }
            | Operation::RequestStreamRead { .. }) => {
                match crate::managed_requests::response_bytes(
                    operation,
                    self.client_frame_bytes,
                    self.client_max_items,
                ) {
                    Some(bytes) => bytes,
                    None => return OwnedResponse::new(full),
                }
            }
            Operation::ManagedSupport { .. } => 32 * 1024 + 1024,
            Operation::Read(ReadRequest {
                query: ReadQuery::SeedScan { max_bytes, .. },
                ..
            }) => match ((*max_bytes).min(self.client_frame_bytes) as usize).checked_mul(2) {
                Some(bytes) => bytes,
                None => return OwnedResponse::new(full),
            },
            Operation::Read(_)
            | Operation::List(_)
            | Operation::Select(_)
            | Operation::Traverse(_)
            | Operation::Validators(_)
            | Operation::NativeRead(_)
            | Operation::NativeList(_) => self.client_frame_bytes as usize,
            // A native receipt, ticket or refusal is a small fixed document.
            Operation::Native { .. } => 4096,
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
                native,
            }),
            send,
            charge.commit(),
        )) {
            Ok(()) => receive
                .await
                .unwrap_or_else(|_| OwnedResponse::new(unknown)),
            Err(HostQueueError::Full) => OwnedResponse::new(full),
            Err(HostQueueError::Disconnected) => OwnedResponse::new(closed),
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
                HostQueueError::Full => AccessError::Capacity,
                HostQueueError::Disconnected => AccessError::Unavailable,
            })?;
        receive.await.map_err(|_| AccessError::Unavailable)?
    }
}
impl Owner {
    #[cfg(test)]
    fn observe_checkpoint_pending(&self) {
        if let Some(observer) = &self.config.checkpoint_observer {
            let _ = observer.0.try_send(self.session.ledger());
        }
    }
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
                        self.progress_managed()?;
                        self.checkpoint_if_due()?;
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
    /// A member added after the log was compacted can only be seeded by a
    /// snapshot whose configuration names it (Raft discards any other), so
    /// the authority checkpoints once such a change has applied; a log that
    /// is complete from its first entry needs no checkpoint. Retried while
    /// proposals or persistence are pending; the checkpoint itself is the
    /// synchronous one an operator's request takes.
    /// Entries applied past the last snapshot: the log this replica keeps
    /// beyond its checkpoint (26 §3).
    pub(super) fn log_entries_since_checkpoint(&self) -> u64 {
        self.session
            .status()
            .applied_index
            .saturating_sub(self.session.snapshot_index())
    }
    /// The log's retirement boundary (26 §3): once the entries applied past
    /// the last snapshot reach the configured bound, the replica checkpoints
    /// its applied prefix and the log behind it is compacted; nothing is
    /// retired before its checkpoint is durable.
    fn checkpoint_by_cadence(&mut self) -> Result<(), LedgerError> {
        if self.log_entries_since_checkpoint() < self.config.checkpoint_after_entries {
            return Ok(());
        }
        self.try_checkpoint().map(|_| ())
    }
    /// Checkpoint now unless the replica cannot yet: a resource condition or
    /// unpersisted state waits for a later tick, and nothing is a failure.
    fn try_checkpoint(&mut self) -> Result<bool, LedgerError> {
        if self.session.pending_count() != 0
            || self.session.persistence_pending()
            || self.session.checkpoint_in_flight()
            || self.stopping.is_some()
        {
            return Ok(false);
        }
        match self.session.checkpoint() {
            Ok(()) => Ok(true),
            Err(
                LedgerError::Capacity
                | LedgerError::NotReady { .. }
                | LedgerError::Consensus(
                    focal_consensus::ConsensusError::PersistencePending
                    | focal_consensus::ConsensusError::Capacity
                    | focal_consensus::ConsensusError::CheckpointIndex,
                ),
            ) => Ok(false),
            Err(LedgerError::Native(error))
                if error.class() == focal_ledger::FailureClass::Retryable =>
            {
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }
    fn checkpoint_if_due(&mut self) -> Result<(), LedgerError> {
        if !self.checkpoint_due {
            return Ok(());
        }
        if self.session.snapshot_index() == 0 {
            self.checkpoint_due = false;
            return Ok(());
        }
        if self.session.pending_count() != 0
            || self.session.persistence_pending()
            || self.session.checkpoint_in_flight()
            || self.stopping.is_some()
        {
            return Ok(());
        }
        match self.session.checkpoint() {
            Ok(()) => {
                self.checkpoint_due = false;
                Ok(())
            }
            // Resource conditions and unpersisted state wait for a later tick.
            Err(
                LedgerError::Capacity
                | LedgerError::NotReady { .. }
                | LedgerError::Consensus(
                    focal_consensus::ConsensusError::PersistencePending
                    | focal_consensus::ConsensusError::Capacity
                    | focal_consensus::ConsensusError::CheckpointIndex,
                ),
            ) => Ok(()),
            Err(LedgerError::Native(error))
                if error.class() == focal_ledger::FailureClass::Retryable =>
            {
                Ok(())
            }
            Err(error) => Err(error),
        }
    }
    fn tick(&mut self) -> Result<(), LedgerError> {
        self.session.tick()?;
        if self.session.status().role == StateRole::Leader {
            let now = wall_ms()?.max(self.session.cursor_clock());
            match self.session.propose_cursor_clock(now) {
                Ok(_) | Err(LedgerError::Capacity | LedgerError::NotReady { .. }) => {}
                Err(error) => return Err(error),
            }
            // Trusted native timers fire from the leader's clock; a deferred
            // or refused timer waits for a later tick or its primary row.
            match crate::native_timers::sweep(&mut self.session) {
                Ok(_) | Err(LedgerError::Capacity | LedgerError::NotReady { .. }) => {}
                Err(error) => return Err(error),
            }
        }
        self.views
            .advance(&mut self.session)
            .map_err(|_| LedgerError::Failed)?;
        self.drain()?;
        self.checkpoint_by_cadence()?;
        self.progress_managed()
    }
    /// Shared-worker progress never waits on a disk receipt. The exact Ready
    /// remains inside Session until its WAL owner reports a completed fence.
    fn progress_group(&mut self) -> Result<bool, LedgerError> {
        self.views
            .advance(&mut self.session)
            .map_err(|_| LedgerError::Failed)?;
        self.expire_pending();
        self.progress_evidence()?;
        if self.session.has_ready() {
            self.drain_with_runtime(self.stopping.is_none())?;
        }
        self.progress_evidence()?;
        if !self.session.persistence_pending() {
            self.poll_snapshot_feedback()?;
        }
        self.progress_managed()?;
        self.checkpoint_if_due()?;
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
        if self.session.persistence_pending() || self.stopping.is_some() || self.evidence.is_some()
        {
            return Instant::now()
                .checked_add(Duration::from_millis(1))
                .ok_or(LedgerError::Failed);
        }
        // A delivery waiting for seed chunks its host has not pulled yet
        // makes no progress on its own; it resumes when a chunk lands.
        if self.session.has_ready() && !self.session.seed_waiting() {
            return Ok(self.next_tick.min(Instant::now()));
        }
        Ok(self.next_tick)
    }
    fn accept(&mut self, work: Work) -> Result<bool, LedgerError> {
        match self.managed_gate(&work) {
            Ok(true) => {}
            Ok(false) => {
                self.defer_managed(work)?;
                return Ok(false);
            }
            Err(error) => {
                managed_support_owner::reject_managed_work(work, error);
                return Ok(false);
            }
        }
        match work {
            Work::Diagnostics(response, charge) => {
                let _ = response.send(Ok(self.diagnostics(charge)));
            }
            Work::Registration(response, charge) => {
                let _ = response.send(self.registration_facts(charge));
            }
            Work::Request(request, response, charge) => {
                self.request(
                    request.verified,
                    response,
                    charge,
                    request.witness,
                    request.native,
                );
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
            Work::Transfer(target, fence, response) => {
                let result = (|| {
                    if let Some(fence) = &fence {
                        if !self.session.is_authoritative() {
                            return Err(LedgerError::NotReady {
                                leader: self.session.status().leader_id,
                            });
                        }
                        if self.session.pending_count() != 0 {
                            return Err(LedgerError::Capacity);
                        }
                        let current = self.session.membership()?;
                        if current.configuration_index != fence.expected_index
                            || current.configuration != fence.expected
                        {
                            return Err(LedgerError::MembershipConflict);
                        }
                    }
                    self.session.transfer_leader(target)
                })();
                self.drain()?;
                let _ = response.send(result);
            }
            Work::ManagedSupport(call, charge) => self.accept_managed_support(*call, charge),
            Work::ActivateNative(call, response, charge) => {
                let result = if self.session.legacy_populated() {
                    wall_ms().and_then(|now| {
                        self.session.propose_native_import(
                            focal_ledger::NativeContentProfile::ProjectionOnly,
                            None,
                            now,
                            call.chunk_bytes,
                            call.max_manifest_bytes,
                        )
                    })
                } else {
                    self.session.propose_native_activation(call.profile)
                };
                drop(charge);
                self.drain()?;
                let _ = response.send(result);
            }
            Work::Refence(route, revision, response, charge) => {
                let result = if route < self.config.route_epoch {
                    Err(LedgerError::PlacementConflict)
                } else if route == self.config.route_epoch {
                    Ok(())
                } else if self.session.active_route() != Some(route)
                    || self
                        .session
                        .placement()
                        .is_some_and(|fence| fence.kind == focal_ledger::SessionFenceKind::Cutover)
                {
                    Err(LedgerError::PlacementConflict)
                } else {
                    self.config.route_epoch = route;
                    self.config.policy_revision = revision;
                    self.views.set_route_epoch(route);
                    self.publish_progress(false);
                    Ok(())
                };
                drop(charge);
                let _ = response.send(result);
            }
            Work::ImportPayloads(response, charge) => {
                let result = if self.session.legacy_populated() {
                    self.session
                        .legacy_import_payloads(IMPORT_PAYLOAD_BYTES)
                        .map(Some)
                } else {
                    Ok(None)
                };
                drop(charge);
                let _ = response.send(result);
            }
            Work::ArtifactPointer(artifact, response, charge) => {
                let result = self.session.native_core().map(|core| {
                    core.native_artifact(artifact)
                        .map(|artifact| artifact.custody().payload())
                });
                drop(charge);
                let _ = response.send(result);
            }
            Work::Checkpoint(response, charge) => {
                // Drain what Raft already owns first so the checkpoint covers
                // the latest applied prefix; a pending proposal stays in the
                // log and refuses the checkpoint until it commits.
                self.drain()?;
                let result = if self.session.pending_count() == 0 {
                    self.session.checkpoint()
                } else {
                    Err(LedgerError::Capacity)
                };
                drop(charge);
                self.drain()?;
                let _ = response.send(result);
            }
            Work::SeedChunks(response, charge) => {
                let result = match self.session.pending_seed() {
                    Some(pending) => pending.missing_chunks().map_err(LedgerError::Native),
                    None => Ok(Vec::new()),
                };
                drop(charge);
                let _ = response.send(result);
            }
            Work::InstallSeed(hash, bytes, response, charge) => {
                let result = self.session.install_seed_chunk(hash, &bytes);
                drop(bytes);
                drop(charge);
                let _ = response.send(result);
                self.drain()?;
            }
            Work::CustodyObjects(response, charge) => {
                let result = match self.session.pending_custody() {
                    Some(pending) => pending.missing_objects().map_err(LedgerError::Native),
                    None => Ok(Vec::new()),
                };
                drop(charge);
                let _ = response.send(result);
            }
            Work::CustodyPulled(response, charge) => {
                // The retained delivery retries at every poll; this one is
                // brought forward so the pulled objects are read at once.
                drop(charge);
                let _ = response.send(Ok(()));
                self.drain()?;
            }
            Work::Membership(call, charge) => {
                self.accept_membership(*call, charge);
                self.drain()?;
            }
            Work::Placement(call, charge) => {
                self.accept_placement(*call, charge);
                self.drain()?;
            }
            Work::Range(call, charge) => {
                self.accept_range(*call, charge);
                self.drain()?;
            }
            Work::Evidence(call, charge) => {
                self.accept_evidence(*call, charge)?;
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
                        match self.session.checkpoint() {
                            // Nothing can be checkpointed yet (a native genesis
                            // still in flight): the log is durable; a later
                            // start checkpoints normally.
                            Err(LedgerError::Native(error))
                                if error.class() == focal_ledger::FailureClass::Retryable =>
                            {
                                Ok(())
                            }
                            other => other,
                        }
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
        self.close_managed();
        self.snapshot_feedback = crate::snapshot_feedback::SnapshotFeedback::default();
        if let Some(pending) = self.evidence.take() {
            let _ = self.session.cancel_checkpoint_evidence();
            pending.finish(Err(LedgerError::OutcomeUnknown));
        }
        if let Some(pending) = self.placement.take() {
            pending.finish(Err(LedgerError::OutcomeUnknown));
        }
        while let Some(pending) = self.memberships.pop_front() {
            self.finish_membership(pending, Err(LedgerError::OutcomeUnknown));
        }
        self.memberships = VecDeque::new();
        if let Some((response, _)) = self.stopping.take() {
            let _ = response.send(Err(LedgerError::OutcomeUnknown));
        }
        while let Some(pending) = self.pending.pop_front() {
            let error = if matches!(
                pending.waiting,
                WaitingFor::Summary { .. }
                    | WaitingFor::Monitor { .. }
                    | WaitingFor::Reconcile { .. }
                    | WaitingFor::RequestStreamRead { .. }
                    | WaitingFor::NativeRead { .. }
            ) {
                AccessError::Unavailable
            } else {
                AccessError::OutcomeUnknown
            };
            pending.finish(Response::Error(error));
        }
        self.publish_progress(true);
    }
    fn publish_progress(&self, stopped: bool) {
        let status = self.session.status();
        self.progress.send_modify(|state| {
            state.value = ReplicaProgress {
                node: status.node_id,
                leader: status.leader_id,
                term: status.term,
                sequence: self.session.sequence(),
                dropped_replication: self.dropped,
                stopped,
                route_epoch: self.config.route_epoch,
                import_pending: self.session.pending_import(),
                seed_pending: self.session.pending_seed().map(|pending| SeedPending {
                    index: pending.index,
                    term: pending.term,
                    missing: pending.missing.len(),
                }),
                custody_pending: self
                    .session
                    .pending_custody()
                    .map(|pending| CustodyPending {
                        missing: pending.missing.len(),
                    }),
            }
        });
    }
    fn request(
        &mut self,
        verified: VerifiedRequest,
        response: oneshot::Sender<OwnedResponse>,
        charge: Allocation,
        witness: Option<EvidenceWitness>,
        native: Option<focal_evidence::VerifiedNativeArtifact>,
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
            if let Operation::ManagedSupport { group } = &request.operation {
                let PeerRole::Node { node_id } = peer.role() else {
                    return Err(AccessError::Unauthorized);
                };
                let status = self.session.status();
                if (!status.voters.contains(&node_id) && !status.learners.contains(&node_id))
                    || *group != self.session.group_id()
                    || request.route_epoch != self.config.route_epoch
                {
                    return Err(AccessError::Unauthorized);
                }
                // A hosted replica answers a probe with the successor promise
                // it can make: the authority of a native group admits a
                // learner only once it has recorded that promise, and it
                // learns it from this reply (the prospective learner is not
                // yet a member and cannot push its own fact).
                if self.session.native_hosted() {
                    match self.session.begin_native_support() {
                        Ok(())
                        | Err(LedgerError::NativeUnsupported)
                        | Err(LedgerError::Consensus(
                            focal_consensus::ConsensusError::PersistencePending,
                        )) => {}
                        Err(error) => return Err(access(error)),
                    }
                }
                return self
                    .session
                    .native_support()
                    .map(Response::ManagedSupport)
                    .map_err(access);
            }
            if !self.serves_route(request.route_epoch) {
                return Err(AccessError::Unavailable);
            }
            match &request.operation {
                Operation::Managed {
                    operation: ManagedOperation::Submit { .. },
                    ..
                } => {
                    if self.pending.len() == self.config.pending_clients {
                        return Err(AccessError::Capacity);
                    }
                    let (key, family, intent) = managed_request_identity(request)
                        .map_err(|_| AccessError::InvalidRequest)?;
                    if let Some(known) = crate::managed_requests::reply(
                        &self.session,
                        &key,
                        intent,
                        family,
                        None,
                        &self.client_limits,
                    )? {
                        return Ok(known);
                    }
                    let needs_evidence = matches!(
                        &request.operation,
                        Operation::Managed {
                            operation: ManagedOperation::Submit {
                                command: Command::AttachArtifact { .. }
                                    | Command::RegisterArtifact { .. }
                                    | Command::FailTestamentGeneration { .. },
                                ..
                            },
                            ..
                        }
                    );
                    let evidence = if needs_evidence {
                        vec![
                            witness
                                .as_ref()
                                .ok_or(AccessError::UnsupportedOperation)?
                                .validate(
                                    &verified,
                                    CustodyScope {
                                        ledger: self.session.ledger(),
                                        route_epoch: self.config.route_epoch,
                                        policy_revision: self.config.policy_revision,
                                    },
                                    &self.session.status().voters,
                                )?,
                        ]
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
                    let protocol = verified.request().protocol;
                    let mut input = verified.into_managed(authority)?;
                    if let Some(runtime) = crate::participant_ingress::authority(
                        &self.session,
                        protocol,
                        input.key.stream.principal,
                        &input.command,
                        input.expected_revision,
                    )? {
                        input.authority.runtime = runtime;
                    }
                    match self.session.propose_managed(&input).map_err(access)? {
                        ManagedSubmission::Committed(receipt) => {
                            Ok(Response::Managed(ManagedReply {
                                receipt: *receipt,
                                stream: None,
                            }))
                        }
                        ManagedSubmission::Pending(key) => {
                            waiting = Some((
                                WaitingFor::ManagedMutation {
                                    key,
                                    intent,
                                    family,
                                },
                                deadline,
                            ));
                            Ok(Response::Error(AccessError::OutcomeUnknown))
                        }
                        ManagedSubmission::Domain(outcome) => {
                            Ok(Response::Submitted(MutationReply::Domain(outcome)))
                        }
                    }
                }
                Operation::Managed {
                    operation: ManagedOperation::Cursor(stream),
                    ..
                } => {
                    if self.pending.len() == self.config.pending_clients {
                        return Err(AccessError::Capacity);
                    }
                    let (key, family, intent) = managed_request_identity(request)
                        .map_err(|_| AccessError::InvalidRequest)?;
                    if let Some(receipt) = self
                        .session
                        .managed_receipt(&key, intent, family)
                        .map_err(access)?
                        && matches!(receipt.outcome, ManagedReceiptOutcome::Sealed { .. })
                    {
                        return Ok(Response::Managed(ManagedReply {
                            receipt: crate::managed_requests::receipt_copy(
                                receipt,
                                &self.client_limits,
                            )?,
                            stream: None,
                        }));
                    }
                    let stream = self.streams.begin(
                        &mut self.session,
                        peer,
                        request,
                        stream,
                        &self.client_limits,
                    )?;
                    waiting = Some((
                        WaitingFor::ManagedStream {
                            stream,
                            key,
                            intent,
                        },
                        deadline,
                    ));
                    Ok(Response::Error(AccessError::OutcomeUnknown))
                }
                Operation::RequestStreamControl { .. } => {
                    if self.pending.len() == self.config.pending_clients {
                        return Err(AccessError::Capacity);
                    }
                    let input = verified.into_request_stream_control()?;
                    match self
                        .session
                        .propose_request_stream(&input)
                        .map_err(access)?
                    {
                        RequestStreamSubmission::Committed(_)
                        | RequestStreamSubmission::Pending(_) => {}
                    }
                    waiting = Some((
                        WaitingFor::RequestStreamControl {
                            input: Box::new(input),
                            context: None,
                        },
                        deadline,
                    ));
                    Ok(Response::Error(AccessError::OutcomeUnknown))
                }
                Operation::RequestStreamRead { cluster, query } => {
                    if self.pending.len() == self.config.pending_clients {
                        return Err(AccessError::Capacity);
                    }
                    if *cluster != self.session.cluster_id() {
                        return Err(AccessError::Unauthorized);
                    }
                    self.nonce = self.nonce.checked_add(1).ok_or(AccessError::Unavailable)?;
                    let mut context = b"focal.replica.managed.read.v1\0".to_vec();
                    context.extend_from_slice(&self.nonce.to_be_bytes());
                    context.extend_from_slice(&peer.principal().0);
                    context.extend_from_slice(&request.request_id.0);
                    self.session.read_index(context.clone()).map_err(access)?;
                    waiting = Some((
                        WaitingFor::RequestStreamRead {
                            context,
                            principal: peer.principal(),
                            cluster: *cluster,
                            query: *query,
                        },
                        deadline,
                    ));
                    Ok(Response::Error(AccessError::Unavailable))
                }
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
                    let protocol = verified.request().protocol;
                    let mut input = verified.into_authenticated(authority)?;
                    if let Some(runtime) = crate::participant_ingress::authority(
                        &self.session,
                        protocol,
                        input.principal,
                        &input.command,
                        input.expected_revision,
                    )? {
                        input.authority.runtime = runtime;
                    }
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
                Operation::Monitor { id } => {
                    if !self.session.is_authoritative() {
                        return Err(AccessError::Unavailable);
                    }
                    if self.pending.len() == self.config.pending_clients {
                        return Err(AccessError::Capacity);
                    }
                    self.nonce = self.nonce.checked_add(1).ok_or(AccessError::Unavailable)?;
                    let mut context = b"focal.replica.monitor.v1\0".to_vec();
                    context.extend_from_slice(&self.nonce.to_be_bytes());
                    context.extend_from_slice(&peer.principal().0);
                    context.extend_from_slice(&request.request_id.0);
                    self.session.read_index(context.clone()).map_err(access)?;
                    waiting = Some((WaitingFor::Monitor { context, id: *id }, deadline));
                    Ok(Response::Error(AccessError::Unavailable))
                }
                Operation::Summary => {
                    if !self.session.is_authoritative() {
                        return Err(AccessError::Unavailable);
                    }
                    if self.pending.len() == self.config.pending_clients {
                        return Err(AccessError::Capacity);
                    }
                    self.nonce = self.nonce.checked_add(1).ok_or(AccessError::Unavailable)?;
                    let mut context = b"focal.replica.summary.v1\0".to_vec();
                    context.extend_from_slice(&self.nonce.to_be_bytes());
                    context.extend_from_slice(&peer.principal().0);
                    context.extend_from_slice(&request.request_id.0);
                    self.session.read_index(context.clone()).map_err(access)?;
                    waiting = Some((WaitingFor::Summary { context }, deadline));
                    Ok(Response::Error(AccessError::Unavailable))
                }
                Operation::Reconcile(query) => {
                    if !self.session.is_authoritative() {
                        return Err(AccessError::Unavailable);
                    }
                    if self.pending.len() == self.config.pending_clients {
                        return Err(AccessError::Capacity);
                    }
                    self.nonce = self.nonce.checked_add(1).ok_or(AccessError::Unavailable)?;
                    let mut context = b"focal.replica.reconcile.v1\0".to_vec();
                    context.extend_from_slice(&self.nonce.to_be_bytes());
                    context.extend_from_slice(&peer.principal().0);
                    context.extend_from_slice(&request.request_id.0);
                    self.session.read_index(context.clone()).map_err(access)?;
                    waiting = Some((
                        WaitingFor::Reconcile {
                            context,
                            principal: peer.principal(),
                            query: *query,
                        },
                        deadline,
                    ));
                    Ok(Response::Error(AccessError::Unavailable))
                }
                Operation::List(list) => {
                    let scope = list_scope(peer, self.session.ledger(), &list.filter)?;
                    if list.cursor.is_none() {
                        if !self.session.is_authoritative() {
                            return Err(AccessError::Unavailable);
                        }
                        if self.pending.len() == self.config.pending_clients {
                            return Err(AccessError::Capacity);
                        }
                        self.nonce = self.nonce.checked_add(1).ok_or(AccessError::Unavailable)?;
                        let mut context = b"focal.replica.list.v1\0".to_vec();
                        context.extend_from_slice(&self.nonce.to_be_bytes());
                        context.extend_from_slice(&peer.principal().0);
                        context.extend_from_slice(&request.request_id.0);
                        self.session.read_index(context.clone()).map_err(access)?;
                        waiting = Some((
                            WaitingFor::List {
                                context,
                                principal: peer.principal(),
                                scope,
                                list: list.clone(),
                            },
                            deadline,
                        ));
                        Ok(Response::Error(AccessError::Unavailable))
                    } else {
                        self.views
                            .list(
                                &mut self.session,
                                ListReadContext {
                                    principal: peer.principal(),
                                    scope,
                                    request_id: request.request_id,
                                    barrier: None,
                                },
                                list,
                                &self.client_limits,
                            )
                            .map(Response::Listed)
                    }
                }
                Operation::Select(list) => {
                    let scope = selection_scope(peer, self.session.ledger(), list)?;
                    if list.query.cursor.is_none() {
                        if !self.session.is_authoritative() {
                            return Err(AccessError::Unavailable);
                        }
                        if self.pending.len() == self.config.pending_clients {
                            return Err(AccessError::Capacity);
                        }
                        self.nonce = self.nonce.checked_add(1).ok_or(AccessError::Unavailable)?;
                        let mut context = b"focal.replica.selection.v1\0".to_vec();
                        context.extend_from_slice(&self.nonce.to_be_bytes());
                        context.extend_from_slice(&peer.principal().0);
                        context.extend_from_slice(&request.request_id.0);
                        self.session.read_index(context.clone()).map_err(access)?;
                        waiting = Some((
                            WaitingFor::Select {
                                context,
                                principal: peer.principal(),
                                scope,
                                list: list.clone(),
                            },
                            deadline,
                        ));
                        Ok(Response::Error(AccessError::Unavailable))
                    } else {
                        self.views
                            .selection(
                                &mut self.session,
                                ListReadContext {
                                    principal: peer.principal(),
                                    scope,
                                    request_id: request.request_id,
                                    barrier: None,
                                },
                                list,
                                &self.client_limits,
                            )
                            .map(Response::Listed)
                    }
                }
                Operation::Validators(list) => {
                    let scope = validator_scope(peer, self.session.ledger(), list)?;
                    if list.query.cursor.is_none() {
                        if !self.session.is_authoritative() {
                            return Err(AccessError::Unavailable);
                        }
                        if self.pending.len() == self.config.pending_clients {
                            return Err(AccessError::Capacity);
                        }
                        self.nonce = self.nonce.checked_add(1).ok_or(AccessError::Unavailable)?;
                        let mut context = b"focal.replica.validators.v1\0".to_vec();
                        context.extend_from_slice(&self.nonce.to_be_bytes());
                        context.extend_from_slice(&peer.principal().0);
                        context.extend_from_slice(&request.request_id.0);
                        self.session.read_index(context.clone()).map_err(access)?;
                        waiting = Some((
                            WaitingFor::Validators {
                                context,
                                principal: peer.principal(),
                                scope,
                                list: list.clone(),
                            },
                            deadline,
                        ));
                        Ok(Response::Error(AccessError::Unavailable))
                    } else {
                        self.views
                            .validators(
                                &mut self.session,
                                ListReadContext {
                                    principal: peer.principal(),
                                    scope,
                                    request_id: request.request_id,
                                    barrier: None,
                                },
                                list,
                                &self.client_limits,
                            )
                            .map(Response::Validators)
                    }
                }
                Operation::Traverse(traversal) => {
                    let scope = traversal_scope(peer, self.session.ledger(), traversal)?;
                    if traversal.cursor.is_none() {
                        if !self.session.is_authoritative() {
                            return Err(AccessError::Unavailable);
                        }
                        if self.pending.len() == self.config.pending_clients {
                            return Err(AccessError::Capacity);
                        }
                        self.nonce = self.nonce.checked_add(1).ok_or(AccessError::Unavailable)?;
                        let mut context = b"focal.replica.traversal.v1\0".to_vec();
                        context.extend_from_slice(&self.nonce.to_be_bytes());
                        context.extend_from_slice(&peer.principal().0);
                        context.extend_from_slice(&request.request_id.0);
                        self.session.read_index(context.clone()).map_err(access)?;
                        waiting = Some((
                            WaitingFor::Traverse {
                                context,
                                principal: peer.principal(),
                                scope,
                                traversal: traversal.clone(),
                            },
                            deadline,
                        ));
                        Ok(Response::Error(AccessError::Unavailable))
                    } else {
                        self.views
                            .traverse(
                                &mut self.session,
                                ListReadContext {
                                    principal: peer.principal(),
                                    scope,
                                    request_id: request.request_id,
                                    barrier: None,
                                },
                                traversal,
                                &self.client_limits,
                            )
                            .map(Response::Traversed)
                    }
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
                Operation::Native { frame } => {
                    if self.pending.len() == self.config.pending_clients {
                        return Err(AccessError::Capacity);
                    }
                    let header = native_frame_admissible(frame, peer, request)?;
                    // Artifact payloads reach this owner only through the data
                    // service, which had the exclusive content writer seal and
                    // verify them first; the owner still checks that evidence
                    // against the exact frame before admission.
                    if crate::native_ingress::artifact_bearing(header.command) && native.is_none() {
                        return Err(AccessError::UnsupportedOperation);
                    }
                    let context = crate::native_ingress::context(peer, &self.session)?;
                    crate::fault::hit(crate::fault::FaultSite::BeforePropose);
                    match self.session.propose_native_frame(
                        context,
                        frame,
                        focal_ledger::NativeCustody::Evidence(native.as_ref()),
                    ) {
                        Ok(focal_ledger::NativeSubmission::Committed(outcome)) => {
                            crate::fault::hit(crate::fault::FaultSite::AfterCommitBeforeReply);
                            Ok(Response::Native(NativeMutationReply::Committed(
                                crate::native_documents::outcome(outcome),
                            )))
                        }
                        Ok(focal_ledger::NativeSubmission::Pending { outcome, .. }) => {
                            waiting = Some((
                                WaitingFor::NativeMutation {
                                    key: header.key,
                                    outcome,
                                },
                                deadline,
                            ));
                            Ok(Response::Error(AccessError::OutcomeUnknown))
                        }
                        Err(error) => crate::native_ingress::failure(error).map(Response::Native),
                    }
                }
                Operation::NativeRead(read) => {
                    read.validate(&self.client_limits)?;
                    let profile = crate::native_reads::profile(&self.session)?;
                    let role = crate::native_reads::role(peer)?;
                    // A learner serves only the members it holds (25 §6);
                    // a voter holds every member.
                    if !crate::native_reads::locations(&read.query)
                        .into_iter()
                        .all(|location| self.serves_member(location))
                    {
                        return Err(AccessError::Unavailable);
                    }
                    if matches!(read.consistency, ReadConsistency::Linearizable) {
                        if !self.session.is_authoritative() {
                            return Err(AccessError::Unavailable);
                        }
                        if self.pending.len() == self.config.pending_clients {
                            return Err(AccessError::Capacity);
                        }
                        self.nonce = self.nonce.checked_add(1).ok_or(AccessError::Unavailable)?;
                        let correlation = crate::native_reads::correlation(
                            peer.principal(),
                            request.request_id,
                            self.nonce,
                        );
                        self.session
                            .native_read_index(correlation)
                            .map_err(access)?;
                        waiting = Some((
                            WaitingFor::NativeRead {
                                correlation,
                                principal: peer.principal(),
                                role,
                                profile,
                                read: read.clone(),
                            },
                            deadline,
                        ));
                        Ok(Response::Error(AccessError::Unavailable))
                    } else {
                        let core = self.session.native_core().map_err(access)?;
                        crate::native_reads::check_consistency(
                            core,
                            self.session.ledger(),
                            &read.consistency,
                        )?;
                        crate::native_reads::page(
                            &crate::native_reads::Reader {
                                core,
                                ledger: self.session.ledger(),
                                profile,
                                principal: peer.principal(),
                                role,
                                route: request.route_epoch,
                            },
                            read,
                        )
                        .map(Response::NativeRead)
                    }
                }
                Operation::NativeList(list) => {
                    list.validate(&self.client_limits)?;
                    let profile = crate::native_reads::profile(&self.session)?;
                    let role = crate::native_reads::role(peer)?;
                    if !self.serves_all_members() {
                        return Err(AccessError::Unavailable);
                    }
                    let key = *self.views.list_key()?;
                    let core = self.session.native_core().map_err(access)?;
                    crate::native_lists::serve(
                        &crate::native_reads::Reader {
                            core,
                            ledger: self.session.ledger(),
                            profile,
                            principal: peer.principal(),
                            role,
                            route: request.route_epoch,
                        },
                        &key,
                        list,
                    )
                    .map(Response::NativeListed)
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
    // Serving metadata belongs to the installed placement. A committed cutover
    // closes it, and activation requires trusted reinstallation with the new
    // route/policy. Consensus traffic continues to catch up while it is closed.
    fn serves_route(&self, route: RouteEpoch) -> bool {
        route == self.config.route_epoch
            && self
                .session
                .active_route()
                .is_none_or(|active| active == route)
            && self
                .session
                .placement()
                .is_none_or(|fence| fence.kind != focal_ledger::SessionFenceKind::Cutover)
    }
    fn probe_receipt(&self, verified: &VerifiedRequest) -> Result<Option<Response>, AccessError> {
        let request = verified.request();
        if request.ledger != self.session.ledger() {
            return Err(AccessError::Unauthorized);
        }
        if !self.serves_route(request.route_epoch) {
            return Err(AccessError::Unavailable);
        }
        if let Operation::Managed { .. } = &request.operation {
            let (key, family, intent) =
                managed_request_identity(request).map_err(|_| AccessError::InvalidRequest)?;
            return crate::managed_requests::reply(
                &self.session,
                &key,
                intent,
                family,
                None,
                &self.client_limits,
            );
        }
        let input = verified.to_authenticated(AuthorityContext {
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
        // A retained delivery (a retryable native refusal, an import waiting
        // for sealed custody) resumes at the next poll; it never stops the
        // replica.
        let events = if self.nonblocking {
            match self.session.try_poll() {
                Ok(Some(events)) => events,
                Ok(None) => {
                    self.expire_pending();
                    self.publish_progress(false);
                    return Ok(());
                }
                Err(LedgerError::Retry) => {
                    self.publish_progress(false);
                    return Ok(());
                }
                Err(error) => return Err(error),
            }
        } else {
            match self.session.poll() {
                Ok(events) => events,
                Err(LedgerError::Retry) => {
                    self.publish_progress(false);
                    return Ok(());
                }
                Err(error) => return Err(error),
            }
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
            let snapshot = match self.snapshot_feedback.begin(message, &self.budget) {
                Ok(snapshot) => snapshot,
                Err(_) => {
                    // Metadata admission also cannot strand Raft in Snapshot.
                    self.session.report_snapshot_at(
                        message.to,
                        message.term,
                        message.get_snapshot().get_metadata().index,
                        focal_consensus::SnapshotStatus::Failure,
                    )?;
                    self.dropped = self.dropped.saturating_add(1);
                    continue;
                }
            };
            let size = message.compute_size() as usize;
            // Oversized snapshots require the chunked snapshot transport. They
            // cannot be silently treated as installed or acknowledged.
            if size
                .checked_add(128)
                .is_none_or(|n| n > self.limits.max_frame_bytes as usize)
            {
                #[cfg(test)]
                if snapshot.is_some() {
                    self.dropped_snapshots = self.dropped_snapshots.saturating_add(1);
                }
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
            let Ok(message_bytes) = message.write_to_bytes() else {
                drop(snapshot);
                self.dropped = self.dropped.saturating_add(1);
                continue;
            };
            self.nonce = self.nonce.checked_add(1).ok_or(LedgerError::Capacity)?;
            let id = ((u128::from(self.session.status().node_id) << 64) | u128::from(self.nonce))
                .to_be_bytes();
            let frame = ReplicationFrame {
                target: message.to,
                snapshot,
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
        self.poll_snapshot_feedback()?;
        self.publish_progress(false);
        Ok(())
    }
    fn poll_snapshot_feedback(&mut self) -> Result<(), LedgerError> {
        self.snapshot_feedback
            .poll(self.session.status().term, |peer, term, index, status| {
                self.session.report_snapshot_at(peer, term, index, status)
            })
    }
    fn resolve(&mut self, events: &SessionEvents) -> Result<(), LedgerError> {
        self.resolve_memberships(events)?;
        self.resolve_placement(events)?;
        let status = self.session.status();
        let count = self.pending.len();
        for _ in 0..count {
            let mut pending = self.pending.pop_front().ok_or(LedgerError::Corrupt)?;
            if !self.serves_route(pending.header.route_epoch)
                && !matches!(
                    pending.waiting,
                    WaitingFor::Mutation(_)
                        | WaitingFor::NativeMutation { .. }
                        | WaitingFor::ManagedMutation { .. }
                        | WaitingFor::PeerPersistence
                )
            {
                pending.finish(Response::Error(AccessError::Unavailable));
                continue;
            }
            let result = match &mut pending.waiting {
                WaitingFor::PeerPersistence => Some(Response::PeerAccepted),
                WaitingFor::ManagedMutation {
                    key,
                    intent,
                    family,
                } => match crate::managed_requests::reply(
                    &self.session,
                    key,
                    *intent,
                    *family,
                    None,
                    &self.client_limits,
                ) {
                    Ok(reply) => reply,
                    Err(error) => Some(Response::Error(error)),
                },
                WaitingFor::ManagedStream {
                    stream,
                    key,
                    intent,
                } if self.stopping.is_none() => {
                    match self.streams.advance(
                        &mut self.session,
                        &mut self.views,
                        stream,
                        events,
                        &self.client_limits,
                    ) {
                        Ok(Some(delivery)) => match crate::managed_requests::reply(
                            &self.session,
                            key,
                            *intent,
                            ManagedRequestFamily::Cursor,
                            Some(delivery),
                            &self.client_limits,
                        ) {
                            Ok(reply) => reply,
                            Err(error) => Some(Response::Error(error)),
                        },
                        Ok(None) => None,
                        Err(error) => Some(Response::Error(error)),
                    }
                }
                WaitingFor::RequestStreamRead {
                    context,
                    principal,
                    cluster,
                    query,
                } if pending.term == status.term && self.session.is_authoritative() => events
                    .read_barriers
                    .iter()
                    .find(|(value, _)| value == context)
                    .map(|(_, prefix)| {
                        crate::managed_requests::read_after_barrier(
                            &self.session,
                            *principal,
                            *cluster,
                            query,
                            *prefix,
                            pending.header.route_epoch,
                            &self.client_limits,
                        )
                        .map(Response::RequestStreamRead)
                        .unwrap_or_else(Response::Error)
                    }),
                WaitingFor::RequestStreamControl { input, context }
                    if pending.term == status.term
                        && self.session.is_authoritative()
                        && self.stopping.is_none() =>
                {
                    if let Some(context) = context {
                        if events
                            .read_barriers
                            .iter()
                            .any(|(value, _)| value == context)
                        {
                            match crate::managed_requests::control_reply(
                                &self.session,
                                input,
                                pending.header.route_epoch,
                                &self.client_limits,
                            ) {
                                Ok(reply) => reply,
                                Err(error) => Some(Response::Error(error)),
                            }
                        } else {
                            None
                        }
                    } else {
                        match self.session.request_stream_receipt(input) {
                            Ok(Some(_)) => {
                                self.nonce =
                                    self.nonce.checked_add(1).ok_or(LedgerError::Capacity)?;
                                let mut next = b"focal.replica.managed.control.v1\0".to_vec();
                                next.extend_from_slice(&self.nonce.to_be_bytes());
                                next.extend_from_slice(&input.principal.0);
                                next.extend_from_slice(&input.id.0);
                                match self.session.read_index(next.clone()) {
                                    Ok(()) => {
                                        *context = Some(next);
                                        None
                                    }
                                    Err(error) => Some(Response::Error(access(error))),
                                }
                            }
                            Ok(None) => None,
                            Err(error) => Some(Response::Error(access(error))),
                        }
                    }
                }
                WaitingFor::Mutation(key) => self
                    .session
                    .receipt(key)
                    .cloned()
                    .map(|receipt| Response::Submitted(MutationReply::Committed(receipt))),
                WaitingFor::NativeMutation { key, outcome } => {
                    match self.session.native_outcome(*key) {
                        Ok(Some(committed)) if committed == *outcome => {
                            crate::fault::hit(crate::fault::FaultSite::AfterCommitBeforeReply);
                            Some(Response::Native(NativeMutationReply::Committed(
                                crate::native_documents::outcome(committed),
                            )))
                        }
                        Ok(Some(_)) => Some(Response::Native(NativeMutationReply::Refused(
                            NativeRefusal {
                                kind: NativeRefusalKind::Conflict,
                                detail: "request key committed another native intent".into(),
                            },
                        ))),
                        Ok(None) => None,
                        Err(error) => Some(Response::Error(access(error))),
                    }
                }
                WaitingFor::NativeRead {
                    correlation,
                    principal,
                    role,
                    profile,
                    read,
                } if pending.term == status.term && self.session.is_authoritative() => events
                    .native_read_boundaries
                    .iter()
                    .find(|boundary| boundary.correlation == *correlation)
                    .map(
                        |boundary| match self.session.native_read_at_least(*boundary) {
                            Ok(core) => crate::native_reads::page(
                                &crate::native_reads::Reader {
                                    core,
                                    ledger: self.session.ledger(),
                                    profile: *profile,
                                    principal: *principal,
                                    role: *role,
                                    route: pending.header.route_epoch,
                                },
                                read,
                            )
                            .map(Response::NativeRead)
                            .unwrap_or_else(Response::Error),
                            Err(error) => Response::Error(access(error)),
                        },
                    ),
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
                WaitingFor::Monitor { context, id }
                    if pending.term == status.term && self.session.is_authoritative() =>
                {
                    events
                        .read_barriers
                        .iter()
                        .find(|(value, _)| value.as_slice() == context.as_slice())
                        .map(|(_, prefix)| {
                            crate::monitor_reads::after_barrier(
                                &self.session,
                                *id,
                                *prefix,
                                pending.header.route_epoch,
                                &self.client_limits,
                            )
                            .map(Response::Monitor)
                            .unwrap_or_else(Response::Error)
                        })
                }
                WaitingFor::Summary { context }
                    if pending.term == status.term && self.session.is_authoritative() =>
                {
                    events
                        .read_barriers
                        .iter()
                        .find(|(value, _)| value.as_slice() == context.as_slice())
                        .map(|(_, prefix)| {
                            crate::ledger_summary::after_barrier(
                                &self.session,
                                *prefix,
                                pending.header.route_epoch,
                                &self.client_limits,
                            )
                            .map(Response::Summary)
                            .unwrap_or_else(Response::Error)
                        })
                }
                WaitingFor::Reconcile {
                    context,
                    principal,
                    query,
                } if pending.term == status.term && self.session.is_authoritative() => events
                    .read_barriers
                    .iter()
                    .find(|(value, _)| value.as_slice() == context.as_slice())
                    .map(|(_, prefix)| {
                        crate::reconciliation::after_barrier(
                            &self.session,
                            *principal,
                            query,
                            *prefix,
                            pending.header.route_epoch,
                            &self.client_limits,
                        )
                        .map(Response::Reconciled)
                        .unwrap_or_else(Response::Error)
                    }),
                WaitingFor::List {
                    context,
                    principal,
                    scope,
                    list,
                } if pending.term == status.term && status.role == StateRole::Leader => events
                    .read_barriers
                    .iter()
                    .find(|(value, _)| value.as_slice() == context.as_slice())
                    .map(|(_, prefix)| {
                        self.views
                            .list(
                                &mut self.session,
                                ListReadContext {
                                    principal: *principal,
                                    scope: *scope,
                                    request_id: pending.header.request_id,
                                    barrier: Some(*prefix),
                                },
                                list,
                                &self.client_limits,
                            )
                            .map(Response::Listed)
                            .unwrap_or_else(Response::Error)
                    }),
                WaitingFor::Select {
                    context,
                    principal,
                    scope,
                    list,
                } if pending.term == status.term && status.role == StateRole::Leader => events
                    .read_barriers
                    .iter()
                    .find(|(value, _)| value.as_slice() == context.as_slice())
                    .map(|(_, prefix)| {
                        self.views
                            .selection(
                                &mut self.session,
                                ListReadContext {
                                    principal: *principal,
                                    scope: *scope,
                                    request_id: pending.header.request_id,
                                    barrier: Some(*prefix),
                                },
                                list,
                                &self.client_limits,
                            )
                            .map(Response::Listed)
                            .unwrap_or_else(Response::Error)
                    }),
                WaitingFor::Validators {
                    context,
                    principal,
                    scope,
                    list,
                } if pending.term == status.term && status.role == StateRole::Leader => events
                    .read_barriers
                    .iter()
                    .find(|(value, _)| value.as_slice() == context.as_slice())
                    .map(|(_, prefix)| {
                        self.views
                            .validators(
                                &mut self.session,
                                ListReadContext {
                                    principal: *principal,
                                    scope: *scope,
                                    request_id: pending.header.request_id,
                                    barrier: Some(*prefix),
                                },
                                list,
                                &self.client_limits,
                            )
                            .map(Response::Validators)
                            .unwrap_or_else(Response::Error)
                    }),
                WaitingFor::Traverse {
                    context,
                    principal,
                    scope,
                    traversal,
                } if pending.term == status.term && status.role == StateRole::Leader => events
                    .read_barriers
                    .iter()
                    .find(|(value, _)| value.as_slice() == context.as_slice())
                    .map(|(_, prefix)| {
                        self.views
                            .traverse(
                                &mut self.session,
                                ListReadContext {
                                    principal: *principal,
                                    scope: *scope,
                                    request_id: pending.header.request_id,
                                    barrier: Some(*prefix),
                                },
                                traversal,
                                &self.client_limits,
                            )
                            .map(Response::Traversed)
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
                    WaitingFor::Mutation(_)
                    | WaitingFor::ManagedMutation { .. }
                    | WaitingFor::RequestStreamControl { .. } => AccessError::OutcomeUnknown,
                    WaitingFor::Stream(stream) | WaitingFor::ManagedStream { stream, .. } => {
                        stream.interrupted()
                    }
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
        self.expire_placement();
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
                    WaitingFor::Mutation(_)
                    | WaitingFor::ManagedMutation { .. }
                    | WaitingFor::RequestStreamControl { .. } => AccessError::OutcomeUnknown,
                    WaitingFor::Stream(stream) | WaitingFor::ManagedStream { stream, .. } => {
                        stream.interrupted()
                    }
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
            let mut proposed = true;
            if let Some(request) = &call.request {
                match self.session.propose_membership(request) {
                    Ok(()) => {}
                    Err(LedgerError::Managed(focal_ledger::ManagedError::Unsupported))
                        if matches!(
                            request.change,
                            focal_consensus::MembershipChange::AddLearner { .. }
                                | focal_consensus::MembershipChange::Promote { .. }
                        ) =>
                    {
                        let current = self.session.membership()?;
                        if current.configuration_index != request.expected_index
                            || current.configuration != request.expected
                        {
                            return Err(LedgerError::MembershipConflict);
                        }
                        proposed = false;
                    }
                    Err(error) => return Err(error),
                }
            } else if !self.session.is_authoritative() {
                return Err(LedgerError::NotReady {
                    leader: self.session.status().leader_id,
                });
            }
            Ok((deadline, proposed))
        })();
        match admitted {
            Ok((deadline, proposed)) => self.memberships.push_back(PendingMembershipCall {
                call,
                proposed,
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
            if !pending.proposed {
                if self.stopping.is_some() {
                    self.finish_membership(pending, Err(LedgerError::OutcomeUnknown));
                    continue;
                }
                let Some(request) = pending.call.request.as_ref() else {
                    self.finish_membership(pending, Err(LedgerError::Corrupt));
                    continue;
                };
                match self.session.propose_membership(request) {
                    Ok(()) => pending.proposed = true,
                    Err(
                        LedgerError::Managed(focal_ledger::ManagedError::Unsupported)
                        | LedgerError::Capacity,
                    ) => {
                        self.memberships.push_back(pending);
                        continue;
                    }
                    Err(error) => {
                        self.finish_membership(pending, Err(error));
                        continue;
                    }
                }
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
                        if matches!(
                            pending.call.request.as_ref().map(|request| &request.change),
                            Some(focal_consensus::MembershipChange::AddLearner { .. })
                        ) {
                            self.checkpoint_due = true;
                        }
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

#[cfg(test)]
#[path = "fleet_list_tests.rs"]
mod list_tests;
