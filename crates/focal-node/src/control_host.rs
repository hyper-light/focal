//! Owned root/partition metadata replicas. Authentication selects the namespace;
//! durable Raft publication completes writes and quorum barriers complete reads.
use focal_consensus::StateRole;
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

/// Entries a control group applies past its last checkpoint before it checkpoints again
/// ([`ControlHostConfig::checkpoint_interval`]'s default).
pub const CHECKPOINT_INTERVAL: u64 = 1024;
#[derive(Debug, Clone)]
pub struct ControlHostConfig {
    /// Dedicated server-owned metadata namespace; never inferred from a request.
    pub namespace: LedgerId,
    pub route_epoch: RouteEpoch,
    pub queue_items: usize,
    pub pending_requests: usize,
    pub replication_queue: usize,
    pub tick: Duration,
    /// The longest the tick period is stretched for a far group (27 §3.1
    /// P2). It bounds how long a dead leader goes unnoticed: the election
    /// timeout is at most the election ticks times this.
    pub tick_ceiling: Duration,
    pub request_timeout: Duration,
    /// Compact the metadata log once this many entries have been applied past
    /// the last snapshot. Bounds the log in steady state and lets a lagging
    /// follower catch up from a trusted snapshot instead of replaying every
    /// authority fact (24 §16). Never zero.
    pub checkpoint_interval: u64,
    /// Trusted immutable-genesis pin; absent means remote enrollment is denied.
    pub enrollment_authority: Option<crate::network_control::FounderControlAuthority>,
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
            tick_ceiling: Duration::from_secs(2),
            request_timeout: Duration::from_secs(5),
            checkpoint_interval: CHECKPOINT_INTERVAL,
            enrollment_authority: None,
        }
    }
    fn validate(&self) -> Result<(), ControlError> {
        if self.namespace.tenant.is_zero()
            || self.namespace.session.is_zero()
            || self.route_epoch.0 == 0
            || !(1..=1024).contains(&self.queue_items)
            || !(1..=1024).contains(&self.pending_requests)
            || self.checkpoint_interval == 0
            || !(1..=1024).contains(&self.replication_queue)
            || !(Duration::from_millis(10)..=Duration::from_secs(1)).contains(&self.tick)
            || self.tick_ceiling < self.tick
            || self.tick_ceiling > Duration::from_secs(10)
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
    /// Exchanges the driver could not make at all, told to the core (27
    /// §3.3); reports coalesced into a peer already held, and reports
    /// beyond the bound on peers held.
    pub peers_unreachable: u64,
    /// Appends this replica refused for not holding the entry before them
    /// (27 §12): what a frame that overtook another cost before the order
    /// was kept, and what a lost frame costs still.
    pub appends_rejected: u64,
    /// Frames held for the one they overtook, frames let go past their
    /// patience or their lane without it, and frames behind what was
    /// already stepped from their source (27 §12).
    pub frames_held: u64,
    pub frames_let_go: u64,
    pub frames_stale: u64,
    pub peer_reports_coalesced: u64,
    pub peer_reports_dropped: u64,
    pub stopped: bool,
    /// The compaction floor: index of the most recent metadata snapshot, or
    /// zero before the first compaction. `applied_index - snapshot_index` is
    /// the retained log length.
    pub snapshot_index: u64,
    /// Per-peer replication progress while this node leads (diagnostic).
    pub peers: Vec<focal_consensus::PeerProgress>,
    /// Why the owner stopped, when it stopped on a failure rather than on
    /// request: the error its loop ended with, or that it unwound. Without
    /// this a stopped control plane reads as an unexplained shutdown.
    pub failure: Option<String>,
}
struct ControlProgressState {
    value: ControlProgress,
    // A directory owner uses its existing watch to retain fixed queue/stack
    // accounting through escaped host and egress lifetimes.
    _allocation: Option<Allocation>,
}
pub struct ControlReplicationFrame {
    pub target: u64,
    pub request: RequestEnvelope,
    snapshot: Option<oneshot::Sender<focal_consensus::SnapshotStatus>>,
    /// Where the driver says the frame did not reach its peer (`ReplicationFrame::lost`).
    lost: Option<mpsc::SyncSender<u64>>,
    /// `ReplicationFrame::urgent`.
    pub(crate) urgent: bool,
    // Retained until transport finishes, including connection setup/retries.
    _charge: Allocation,
}
impl ControlReplicationFrame {
    /// The frame did not reach its peer (`ReplicationFrame::lost`).
    pub(crate) fn lost(&mut self) {
        if let Some(lost) = self.lost.take() {
            let _ = lost.try_send(self.target);
        }
    }
    /// Transport acceptance releases snapshot flow control only. The separate
    /// Raft response remains responsible for durable replication progress.
    pub(crate) fn report_snapshot(&mut self, accepted: bool) {
        crate::snapshot_feedback::complete(&mut self.snapshot, accepted);
    }
}
#[path = "directory_bootstrap_owner.rs"]
mod directory_bootstrap_owner;
pub use directory_bootstrap_owner::DirectoryReplication;
#[path = "directory_bootstrap_host.rs"]
mod directory_bootstrap_host;
use directory_bootstrap_host::{DirectoryReply, PendingDirectory};
#[path = "directory_authority_host.rs"]
mod directory_authority_host;
use directory_authority_host::{PendingAuthority, RefreshReply};

#[path = "local_intent_host.rs"]
mod local_intent_host;
use local_intent_host::LocalIntentWrite;
pub(crate) use local_intent_host::{LocalIntentError, save_local_intent};

enum Work {
    PersistLocalIntent(Box<LocalIntentWrite>),
    Request(Box<VerifiedRequest>, oneshot::Sender<Completed>, Allocation),
    ObserveRoot(
        oneshot::Sender<Result<RootObservation, ControlFailure>>,
        Allocation,
    ),
    /// The configuration this replica applied and the entry that last
    /// changed it: what a voter witnesses from its own log before it signs
    /// the group's membership fact for the root (F24).
    WitnessMembership(
        oneshot::Sender<Result<MembershipWitness, ControlFailure>>,
        Allocation,
    ),
    PrepareSessionProof {
        witness: Box<focal_ledger::CommittedPlacement>,
        window: crate::placement_proof::ProofWindow,
        response: oneshot::Sender<
            Result<
                crate::placement_proof::SessionProofPermit,
                crate::placement_proof::PlacementProofError,
            >,
        >,
        _input: Allocation,
    },
    PrepareReplicaReady {
        ready: Box<focal_directory::ReplicaReady>,
        group: focal_directory::LogGroupId,
        window: crate::placement_proof::ProofWindow,
        response: oneshot::Sender<
            Result<
                crate::placement_proof::SessionProofPermit,
                crate::placement_proof::PlacementProofError,
            >,
        >,
        _input: Allocation,
    },
    PrepareMembershipProof {
        next: Box<focal_directory::GroupAuthorityGrant>,
        record: crate::placement_proof::MembershipRecord,
        window: crate::placement_proof::ProofWindow,
        response: oneshot::Sender<
            Result<
                crate::placement_proof::SessionProofPermit,
                crate::placement_proof::PlacementProofError,
            >,
        >,
        _input: Allocation,
    },
    PrepareDelegationProof {
        group: focal_directory::LogGroupId,
        fence: Box<focal_directory::DelegationFence>,
        source: bool,
        window: crate::placement_proof::ProofWindow,
        response: oneshot::Sender<
            Result<
                crate::placement_proof::SessionProofPermit,
                crate::placement_proof::PlacementProofError,
            >,
        >,
        _input: Allocation,
    },
    PrepareDirectory {
        plan: Box<crate::directory_bootstrap::FirstDirectoryPlan>,
        response: DirectoryReply,
        input: Allocation,
    },
    RefreshDirectory {
        permit: Box<crate::directory_bootstrap::PartitionBootstrapPermit>,
        response: RefreshReply,
        input: Allocation,
        reply_charge: Allocation,
    },
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
    progress: watch::Receiver<ControlProgressState>,
    config: ControlHostConfig,
    limits: WireLimits,
    budget: MemoryBudget,
    pub(crate) pace: TickPeriod,
}
pub struct ControlOwner(JoinHandle<()>);

pub(crate) use crate::pace::TickPeriod;

/// One durable local prefix for transport reconstruction. This observation
/// grants neither a current quorum read nor permission to change membership.
/// Its owned allowance remains live until the observer releases every view.
pub struct RootObservation {
    snapshot: ControlSnapshot,
    contacts: ContactSnapshot,
    configuration: ControlConfiguration,
    authority: Option<focal_directory::AuthorityCheckpoint>,
    _input: Allocation,
    _state: Allocation,
}
impl RootObservation {
    pub fn snapshot(&self) -> &ControlSnapshot {
        &self.snapshot
    }
    pub fn contacts(&self) -> &ContactSnapshot {
        &self.contacts
    }
    pub fn configuration(&self) -> &ControlConfiguration {
        &self.configuration
    }
    pub fn authority(&self) -> Option<&focal_directory::AuthorityCheckpoint> {
        self.authority.as_ref()
    }
}
/// A replica's applied configuration and the entry that last changed it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MembershipWitness {
    pub configuration: ControlConfiguration,
    pub record: Option<focal_control::ControlMembershipRecord>,
}
impl ControlOwner {
    pub fn join(self) -> Result<(), ControlFailure> {
        self.0.join().map_err(|_| ControlFailure::Unavailable)
    }
}
enum Waiting {
    /// What waits its turn for the replica's one proposal, which another
    /// request holds: taken when it is proposed. A replica decides one
    /// command at a time, and what came while it decided one was refused
    /// for capacity — an operator's `membership remove` that met a
    /// placement intent of the node's own, on a machine slow enough for
    /// the two to meet. It waits here for its own time, in the order it
    /// came, and is refused for capacity only when this list is full.
    Turn(Option<Box<Turn>>),
    Write(ControlRequestId),
    Read {
        context: Vec<u8>,
        query: ControlRead,
    },
    Enrollment {
        context: Vec<u8>,
        request: Option<Box<ControlRequest>>,
    },
}
/// What a caller asked that takes the replica's one proposal.
enum Turn {
    Submit(Box<ControlRequest>),
    Transfer(ControlTransfer),
}
/// A peer's frame held for the one it overtook (27 §12), answered once it
/// is stepped and the drain that follows has run.
struct HeldControlFrame {
    source: u64,
    message: Vec<u8>,
    header: ResponseEnvelope,
    response: oneshot::Sender<Completed>,
    charge: Allocation,
}
struct Pending {
    header: ResponseEnvelope,
    response: oneshot::Sender<Completed>,
    waiting: Waiting,
    term: u64,
    /// The owner's period at which the request is given up: the request
    /// time in the periods it holds at the configured tick, counted as the
    /// owner runs them (27 §3.1 P2). A request waits its time in the
    /// owner's rounds and not the clock's, so an owner that a loaded
    /// machine slows gives up nothing it would have answered.
    deadline: u64,
    enrollment: bool,
    root_peer: Option<RootPeer>,
    _charge: Allocation,
}
#[derive(Clone, Copy)]
struct RootPeer {
    node: u64,
    principal: [u8; 16],
    fingerprint: [u8; 32],
    /// The enrolled key the peer was admitted under when the listener's
    /// projection did not name its certificate: a renewal this replica may
    /// not have applied yet (24 §11).
    key: Option<[u8; 32]>,
}
struct Owner<V> {
    replica: ControlReplica,
    initial: Option<ControlEvents>,
    verifier: V,
    config: ControlHostConfig,
    limits: WireLimits,
    budget: MemoryBudget,
    pending: VecDeque<Pending>,
    directory: Option<PendingDirectory>,
    authority_refresh: Option<PendingAuthority>,
    snapshot_feedback: crate::snapshot_feedback::SnapshotFeedback,
    outbound: async_mpsc::Sender<ControlReplicationFrame>,
    progress: watch::Sender<ControlProgressState>,
    nonce: u64,
    dropped: u64,
    /// The order this owner's bulk frames to each peer leave in (27 §12):
    /// the term they belong to and the last sequence given within it,
    /// pruned to the members the replica accepts. A term is the epoch: only
    /// a leader sends bulk frames and a node leads again only in a later
    /// term, where an incarnation drawn at random at a restart was smaller
    /// than the one before as often as not.
    ordered: std::collections::BTreeMap<u64, (u64, u64)>,
    /// A peer's bulk frames held for the ones they overtook, stepped in
    /// their order (`crate::resequence`).
    resequencer: crate::resequence::Resequencer<HeldControlFrame>,
    /// Held frames stepped since the last drain, answered once it has run.
    stepped: Vec<(ResponseEnvelope, oneshot::Sender<Completed>, Allocation)>,
    /// `ControlProgress::appends_rejected`, `frames_held`, `frames_let_go`
    /// and `frames_stale`.
    appends_rejected: u64,
    frames_held: u64,
    frames_let_go: u64,
    frames_stale: u64,
    /// Whether a request took its turn for the proposal in the drain under
    /// way: the next drain proposes it.
    took_turn: bool,
    unreachable: u64,
    failure: Option<String>,
    pub(crate) pace: TickPeriod,
    /// Peers the driver could not reach, reported to the core each tick.
    lost_sender: mpsc::SyncSender<u64>,
    lost: mpsc::Receiver<u64>,
    /// Peers the driver reported lost, each once, kept until the core can
    /// be told (bounded by the members a configuration names); reports of
    /// a peer already held are coalesced, and reports beyond the bound are
    /// dropped, both counted.
    lost_peers: Vec<u64>,
    lost_coalesced: u64,
    lost_dropped: u64,
    /// The reply to a stop, and the owner's period at which a leader's
    /// hand-off is given up on: the stop completes once the log leads
    /// elsewhere, or then.
    stopping: Option<(oneshot::Sender<Result<(), ControlFailure>>, u64)>,
}
impl ControlHost {
    /// Validate a durable session-owner witness against this control owner's
    /// installed authority. The witness retains its export allowance while
    /// queued; the returned permit owns its allowance through signing/delivery.
    /// The clock is sampled by the owner at dispatch, never trusted from a peer.
    /// This local capability is deliberately absent from the wire RPC.
    pub async fn prepare_session_proof(
        &self,
        witness: focal_ledger::CommittedPlacement,
        window: crate::placement_proof::ProofWindow,
    ) -> Result<
        crate::placement_proof::SessionProofPermit,
        crate::placement_proof::PlacementProofError,
    > {
        use crate::placement_proof::PlacementProofError;
        let input = self
            .budget
            .reserve(BudgetKind::Control, BudgetLane::Completion, 512)
            .map_err(|_| PlacementProofError::Capacity)?
            .commit();
        let (response, receive) = oneshot::channel();
        self.sender
            .try_send(Work::PrepareSessionProof {
                // The witness already owns its full structural export charge.
                // Boxing keeps unrelated bounded queue entries small.
                witness: Box::new(witness),
                window,
                response,
                _input: input,
            })
            .map_err(|error| match error {
                mpsc::TrySendError::Full(_) => PlacementProofError::Capacity,
                mpsc::TrySendError::Disconnected(_) => PlacementProofError::Unavailable,
            })?;
        receive
            .await
            .map_err(|_| PlacementProofError::Unavailable)?
    }
    /// Validate this node's own readiness fact against the installed session
    /// authority and return a permit the node's credential signs. Local only.
    pub async fn prepare_replica_ready(
        &self,
        ready: focal_directory::ReplicaReady,
        group: focal_directory::LogGroupId,
        window: crate::placement_proof::ProofWindow,
    ) -> Result<
        crate::placement_proof::SessionProofPermit,
        crate::placement_proof::PlacementProofError,
    > {
        use crate::placement_proof::PlacementProofError;
        let input = self
            .budget
            .reserve(BudgetKind::Control, BudgetLane::Completion, 512)
            .map_err(|_| PlacementProofError::Capacity)?
            .commit();
        let (response, receive) = oneshot::channel();
        self.sender
            .try_send(Work::PrepareReplicaReady {
                ready: Box::new(ready),
                group,
                window,
                response,
                _input: input,
            })
            .map_err(|error| match error {
                mpsc::TrySendError::Full(_) => PlacementProofError::Capacity,
                mpsc::TrySendError::Disconnected(_) => PlacementProofError::Unavailable,
            })?;
        receive
            .await
            .map_err(|_| PlacementProofError::Unavailable)?
    }
    /// Validate a session group's next membership against the installed
    /// authority and return a permit this node's credential signs. Local only.
    pub async fn prepare_membership_proof(
        &self,
        next: focal_directory::GroupAuthorityGrant,
        record: crate::placement_proof::MembershipRecord,
        window: crate::placement_proof::ProofWindow,
    ) -> Result<
        crate::placement_proof::SessionProofPermit,
        crate::placement_proof::PlacementProofError,
    > {
        use crate::placement_proof::PlacementProofError;
        let input = self
            .budget
            .reserve(BudgetKind::Control, BudgetLane::Completion, 512)
            .map_err(|_| PlacementProofError::Capacity)?
            .commit();
        let (response, receive) = oneshot::channel();
        self.sender
            .try_send(Work::PrepareMembershipProof {
                next: Box::new(next),
                record,
                window,
                response,
                _input: input,
            })
            .map_err(|error| match error {
                mpsc::TrySendError::Full(_) => PlacementProofError::Capacity,
                mpsc::TrySendError::Disconnected(_) => PlacementProofError::Unavailable,
            })?;
        receive
            .await
            .map_err(|_| PlacementProofError::Unavailable)?
    }
    /// Validate a delegation fence against the installed authority and return
    /// a permit this node's credential signs as a voter of `group`, the
    /// source group (`source`) or the destination group. Local only.
    pub async fn prepare_delegation_proof(
        &self,
        group: focal_directory::LogGroupId,
        fence: focal_directory::DelegationFence,
        source: bool,
        window: crate::placement_proof::ProofWindow,
    ) -> Result<
        crate::placement_proof::SessionProofPermit,
        crate::placement_proof::PlacementProofError,
    > {
        use crate::placement_proof::PlacementProofError;
        let input = self
            .budget
            .reserve(BudgetKind::Control, BudgetLane::Completion, 512)
            .map_err(|_| PlacementProofError::Capacity)?
            .commit();
        let (response, receive) = oneshot::channel();
        self.sender
            .try_send(Work::PrepareDelegationProof {
                group,
                fence: Box::new(fence),
                source,
                window,
                response,
                _input: input,
            })
            .map_err(|error| match error {
                mpsc::TrySendError::Full(_) => PlacementProofError::Capacity,
                mpsc::TrySendError::Disconnected(_) => PlacementProofError::Unavailable,
            })?;
        receive
            .await
            .map_err(|_| PlacementProofError::Unavailable)?
    }
    /// Trusted in-process observation, deliberately absent from the wire RPC.
    /// Followers can rebuild authenticated replication routes without first
    /// asking the same unavailable quorum they need those routes to reach.
    pub async fn observe_root(&self) -> Result<RootObservation, ControlFailure> {
        let charge = self
            .budget
            .reserve(BudgetKind::Control, BudgetLane::Completion, 512)
            .map_err(|_| ControlFailure::Capacity)?
            .commit();
        let (send, receive) = oneshot::channel();
        self.sender
            .try_send(Work::ObserveRoot(send, charge))
            .map_err(|error| match error {
                mpsc::TrySendError::Full(_) => ControlFailure::Capacity,
                mpsc::TrySendError::Disconnected(_) => ControlFailure::Unavailable,
            })?;
        receive.await.map_err(|_| ControlFailure::Unavailable)?
    }
    /// What this replica applied of its own group's configuration, from its
    /// owner thread: no quorum read, a follower's committed view (F24).
    pub async fn witness_membership(&self) -> Result<MembershipWitness, ControlFailure> {
        let charge = self
            .budget
            .reserve(BudgetKind::Control, BudgetLane::Completion, 512)
            .map_err(|_| ControlFailure::Capacity)?
            .commit();
        let (send, receive) = oneshot::channel();
        self.sender
            .try_send(Work::WitnessMembership(send, charge))
            .map_err(|error| match error {
                mpsc::TrySendError::Full(_) => ControlFailure::Capacity,
                mpsc::TrySendError::Disconnected(_) => ControlFailure::Unavailable,
            })?;
        receive.await.map_err(|_| ControlFailure::Unavailable)?
    }
    pub fn wire_limits() -> WireLimits {
        WireLimits {
            max_frame_bytes: 10 * 1024 * 1024,
            max_cost: 40 * 1024 * 1024,
            ..WireLimits::for_consensus(
                u32::try_from(focal_consensus::DEFAULT_INFLIGHT_WINDOW).unwrap_or(u32::MAX),
            )
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
        Self::spawn_inner(replica, verifier, config, budget, None)
    }
    /// Resume a replica already drained by trusted startup authentication. Its
    /// owned recovery output is consumed once by the normal framing path before
    /// any subsequent drain, rather than discarded before routes are installed.
    pub fn spawn_recovered<V: AuthorityVerifier + Send + 'static>(
        replica: ControlReplica,
        verifier: V,
        config: ControlHostConfig,
        budget: MemoryBudget,
        initial: ControlEvents,
    ) -> Result<
        (
            Self,
            ControlOwner,
            async_mpsc::Receiver<ControlReplicationFrame>,
        ),
        ControlError,
    > {
        if initial.applied_index != replica.applied_index() {
            return Err(ControlError::Invalid);
        }
        Self::spawn_inner(replica, verifier, config, budget, Some(initial))
    }
    fn spawn_inner<V: AuthorityVerifier + Send + 'static>(
        replica: ControlReplica,
        verifier: V,
        config: ControlHostConfig,
        budget: MemoryBudget,
        initial: Option<ControlEvents>,
    ) -> Result<
        (
            Self,
            ControlOwner,
            async_mpsc::Receiver<ControlReplicationFrame>,
        ),
        ControlError,
    > {
        config.validate()?;
        if config
            .enrollment_authority
            .as_ref()
            .is_some_and(|authority| {
                authority.identity() != replica.identity()
                    || authority.namespace() != config.namespace
            })
        {
            return Err(ControlError::WrongOwner);
        }
        let limits = Self::wire_limits();
        let (sender, receiver) = mpsc::sync_channel(config.queue_items);
        let (peers, incoming) = mpsc::sync_channel(config.replication_queue);
        let (outbound, outgoing) = async_mpsc::channel(config.replication_queue);
        let status = replica.status();
        let pace = TickPeriod::default();
        let (progress, changes) = watch::channel(ControlProgressState {
            value: ControlProgress {
                identity: replica.identity(),
                node: status.node_id,
                leader: status.leader_id,
                term: status.term,
                applied_index: replica.applied_index(),
                revisions: replica.revisions(),
                dropped_replication: 0,
                peers_unreachable: 0,
                appends_rejected: 0,
                frames_held: 0,
                frames_let_go: 0,
                frames_stale: 0,
                peer_reports_coalesced: 0,
                peer_reports_dropped: 0,
                stopped: false,
                snapshot_index: 0,
                peers: Vec::new(),
                failure: None,
            },
            _allocation: None,
        });
        let (lost_sender, lost) = mpsc::sync_channel(crate::fleet::LOST_PEERS);
        let owner = Owner {
            replica,
            initial,
            verifier,
            config: config.clone(),
            stopping: None,
            lost_sender,
            lost,
            lost_peers: Vec::new(),
            lost_coalesced: 0,
            lost_dropped: 0,
            limits: limits.clone(),
            budget: budget.clone(),
            pending: VecDeque::new(),
            directory: None,
            authority_refresh: None,
            snapshot_feedback: Default::default(),
            outbound,
            progress,
            nonce: 0,
            dropped: 0,
            ordered: std::collections::BTreeMap::new(),
            resequencer: crate::resequence::Resequencer::new(
                focal_consensus::DEFAULT_INFLIGHT_WINDOW,
                crate::fleet::LOST_PEERS,
            ),
            stepped: Vec::new(),
            appends_rejected: 0,
            frames_held: 0,
            frames_let_go: 0,
            frames_stale: 0,
            took_turn: false,
            unreachable: 0,
            failure: None,
            pace: pace.clone(),
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
                pace,
                config,
                limits,
                budget,
            },
            ControlOwner(thread),
            outgoing,
        ))
    }
    /// Derive this owner's tick period from the measured paths to the
    /// group's other voters (27 §3.1 P2) and put it in force from the next
    /// tick. A group whose paths all sit inside the configured period keeps
    /// the configured period. The owner's own stalls do not touch the
    /// period: they are the replica's patience (`TickPeriod::patience`).
    pub fn pace<'a>(
        &self,
        paths: impl IntoIterator<Item = &'a focal_timing::PathRtt>,
    ) -> focal_timing::TickPace {
        let pace = focal_timing::TickPace::derive(
            self.config.tick,
            self.config.tick_ceiling,
            // Before the owner has opened its replica the count is unknown;
            // one tick is the conservative reading (the longest period).
            self.pace.election_tick().max(1),
            paths,
        );
        self.pace.publish(pace);
        pace
    }
    /// The periods the owner has run; what a wait on it is charged in
    /// (27 §3.1 P8).
    pub fn periods(&self) -> u64 {
        self.pace.periods()
    }
    /// Periods in one election timeout of this replica; none before its
    /// owner has opened it.
    pub fn election_periods(&self) -> u64 {
        u64::try_from(self.pace.election_tick()).unwrap_or(u64::MAX)
    }
    /// The periods in which the replica was not ticked: refused the room,
    /// or still persisting.
    pub fn refused_periods(&self) -> u64 {
        self.pace.refused()
    }
    /// The longest a period of the owner took, from one to the next: a
    /// stall of the owner, which its replica's followers may have taken
    /// for its death.
    pub fn longest_period(&self) -> Duration {
        self.pace.longest()
    }
    /// The tick period in force.
    pub fn tick_period(&self) -> Duration {
        self.pace.get(self.config.tick, self.config.tick_ceiling)
    }
    /// The pace in force: the last derivation, or the configured period
    /// with no samples before any.
    pub fn current_pace(&self) -> focal_timing::TickPace {
        self.pace
            .derived()
            .unwrap_or_else(|| focal_timing::TickPace::floor(self.config.tick))
    }
    pub fn progress(&self) -> ControlProgress {
        self.progress.borrow().value.clone()
    }
    pub async fn closed(&self) {
        let mut changes = self.progress.clone();
        while !changes.borrow().value.stopped {
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
        let response = self.handle(&verified).await;
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
    fn handle<'a>(&'a self, request: &'a VerifiedRequest) -> HandlerFuture<'a> {
        Box::pin(async move { self.handle_accounted(request).await.into_envelope() })
    }
    fn handle_accounted<'a>(&'a self, request: &'a VerifiedRequest) -> OwnedHandlerFuture<'a> {
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
            let peer = matches!(
                request.request().operation,
                Operation::Raft { .. } | Operation::RaftOrdered { .. }
            );
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
            match queue.try_send(Work::Request(
                Box::new(request.clone()),
                send,
                charge.commit(),
            )) {
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
/// A request, its answer, and what the answer's export is charged to.
type Finished = (
    Pending,
    Result<ControlReply, ControlFailure>,
    Option<Allocation>,
);
impl<V: AuthorityVerifier> Owner<V> {
    fn run(mut self, receiver: mpsc::Receiver<Work>, peers: mpsc::Receiver<Work>) {
        self.pace.announce(self.replica.election_tick());
        let result = catch_unwind(AssertUnwindSafe(|| -> Result<(), ControlError> {
            self.drain()?;
            let mut next_tick = Instant::now()
                .checked_add(self.pace.get(self.config.tick, self.config.tick_ceiling))
                .ok_or(ControlError::Capacity)?;
            let beat = self.config.tick.saturating_mul(
                u32::try_from(self.replica.heartbeat_tick().max(1)).unwrap_or(u32::MAX),
            );
            let mut next_beat = Instant::now();
            loop {
                for _ in 0..8 {
                    match peers.try_recv() {
                        Ok(work) => {
                            self.work(work)?;
                        }
                        Err(_) => break,
                    }
                }
                // The reads the members forwarded together leave in one
                // round.
                if self.replica.reads_unasked() {
                    self.drain()?;
                }
                if Instant::now() >= next_tick {
                    self.pace
                        .advance(self.pace.get(self.config.tick, self.config.tick_ceiling));
                    // What the owner has seen of its own stalls is the
                    // replica's patience before it campaigns.
                    self.replica.set_patience(
                        self.pace
                            .patience(self.config.tick, self.config.tick_ceiling),
                    )?;
                    // A tick that was refused the room, or that came while
                    // the one before it is still persisted, changed
                    // nothing: the period has passed without it. A member
                    // that is not ticked waits longer before it campaigns,
                    // and a leader sends its heartbeats a period later
                    // (27 §3.1 P3). It is no reason for the owner to end.
                    self.report_lost()?;
                    match self.replica.tick() {
                        Ok(()) => {}
                        Err(error) if self.replica.checkpoint_retryable(&error) => {
                            self.pace.refuse();
                        }
                        Err(error) => return Err(error),
                    }
                    self.drain()?;
                    self.maybe_checkpoint()?;
                    next_tick = Instant::now()
                        .checked_add(self.pace.get(self.config.tick, self.config.tick_ceiling))
                        .ok_or(ControlError::Capacity)?;
                }
                if self.handed_off() && self.finish_stop()? {
                    return Ok(());
                }
                // A stretched period stretches the election timeout, which
                // is what it is for. The heartbeats of a leader keep the
                // cadence its followers were configured to expect: each
                // node stretches by what it measured itself, and a leader
                // that beat at its own stretched period would be presumed
                // dead by a follower that measured less (27 §3.1 P2).
                if self.replica.leads()
                    && self
                        .pace
                        .stretched(self.config.tick, self.config.tick_ceiling)
                    && Instant::now() >= next_beat
                {
                    match self.replica.beat() {
                        Ok(()) => self.drain()?,
                        Err(error) if self.replica.checkpoint_retryable(&error) => {}
                        Err(error) => return Err(error),
                    }
                    next_beat = Instant::now()
                        .checked_add(beat)
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
                        // While a read waits for its round, what is queued
                        // behind it is taken before the drain that sends
                        // the round: the reads among it share the round.
                        // Counted by what the owner admits at once; work
                        // that is no read drains for itself, which sends
                        // the round and ends this.
                        let mut taken = 1usize;
                        while self.replica.reads_unasked() && taken < self.config.pending_requests {
                            let Ok(work) = receiver.try_recv() else {
                                break;
                            };
                            taken = taken.saturating_add(1);
                            if self.work(work)? {
                                return Ok(());
                            }
                        }
                        if self.replica.reads_unasked() {
                            self.drain()?;
                        }
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                    Err(mpsc::RecvTimeoutError::Disconnected) => return Ok(()),
                }
            }
        }));
        self.failure = match result {
            Ok(Ok(())) => None,
            // The replica's own cause, where it failed first: the loop's
            // last error is `Failed`, every call's answer after that.
            Ok(Err(error)) => Some(match self.replica.failure() {
                Some(cause) => format!("{error} ({cause})"),
                None => error.to_string(),
            }),
            Err(_) => Some("control owner unwound".to_owned()),
        };
        self.stop_waiters();
        self.publish_progress(true);
    }
    fn stop_waiters(&mut self) {
        while let Some(pending) = self.pending.pop_front() {
            self.finish(pending, Err(ControlFailure::OutcomeUnknown));
        }
        if let Some(pending) = self.directory.take() {
            pending.finish(Err(
                crate::directory_bootstrap::DirectoryBootstrapError::Unavailable,
            ));
        }
        if let Some(pending) = self.authority_refresh.take() {
            pending.stop();
        }
    }
    fn work(&mut self, work: Work) -> Result<bool, ControlError> {
        match work {
            Work::PersistLocalIntent(write) => write.persist(),
            Work::RefreshDirectory {
                permit,
                response,
                input,
                reply_charge,
            } => {
                self.drain()?;
                self.refresh_directory(permit, response, input, reply_charge);
                self.drain()?;
            }
            Work::PrepareDirectory {
                plan,
                response,
                input,
            } => {
                self.drain()?;
                self.prepare_directory(*plan, response, input);
                self.drain()?;
            }
            Work::PrepareSessionProof {
                witness,
                window,
                response,
                _input,
            } => {
                self.drain()?;
                let result = crate::network_bootstrap::unix_time()
                    .map_err(|_| crate::placement_proof::PlacementProofError::Unavailable)
                    .and_then(|now| {
                        crate::placement_proof::prepare_session_proof(
                            &self.replica,
                            &witness,
                            window,
                            now,
                            &self.budget,
                        )
                    });
                let _ = response.send(result);
            }
            Work::PrepareReplicaReady {
                ready,
                group,
                window,
                response,
                _input,
            } => {
                self.drain()?;
                let result = crate::network_bootstrap::unix_time()
                    .map_err(|_| crate::placement_proof::PlacementProofError::Unavailable)
                    .and_then(|now| {
                        crate::placement_proof::prepare_replica_ready_proof(
                            &self.replica,
                            &ready,
                            group,
                            window,
                            now,
                            &self.budget,
                        )
                    });
                let _ = response.send(result);
            }
            Work::PrepareMembershipProof {
                next,
                record,
                window,
                response,
                _input,
            } => {
                self.drain()?;
                let node = self.replica.status().node_id;
                let result = crate::network_bootstrap::unix_time()
                    .map_err(|_| crate::placement_proof::PlacementProofError::Unavailable)
                    .and_then(|now| {
                        crate::placement_proof::prepare_membership_proof(
                            &self.replica,
                            node,
                            &next,
                            &record,
                            window,
                            now,
                            &self.budget,
                        )
                    });
                let _ = response.send(result);
            }
            Work::PrepareDelegationProof {
                group,
                fence,
                source,
                window,
                response,
                _input,
            } => {
                self.drain()?;
                let node = self.replica.status().node_id;
                let result = crate::network_bootstrap::unix_time()
                    .map_err(|_| crate::placement_proof::PlacementProofError::Unavailable)
                    .and_then(|now| {
                        crate::placement_proof::prepare_delegation_proof(
                            &self.replica,
                            node,
                            group,
                            &fence,
                            source,
                            window,
                            now,
                            &self.budget,
                        )
                    });
                let _ = response.send(result);
            }
            Work::ObserveRoot(response, input) => {
                self.drain()?;
                let result = self.observe_root(input);
                let _ = response.send(result);
            }
            Work::WitnessMembership(response, _input) => {
                self.drain()?;
                let _ = response.send(Ok(MembershipWitness {
                    configuration: self.replica.configuration(),
                    record: self.replica.membership_record().cloned(),
                }));
            }
            Work::Request(request, response, charge) => {
                let waiting = self.replica.reads_waiting();
                self.request(*request, response, charge);
                // A request that only asked a read is not drained for here:
                // the owner's loop takes what else is queued first, and the
                // round that leaves with its drain carries every read asked
                // by then (27 §9). Anything else is drained for as it was.
                if self.replica.reads_waiting() <= waiting || !self.replica.reads_unasked() {
                    self.drain()?;
                }
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
                if self.stopping.is_some() {
                    let _ = response.send(Err(ControlFailure::Capacity));
                    return Ok(false);
                }
                // A leader hands its log off before it goes (27 §5): the
                // most caught-up voter is asked to campaign now, and the
                // stop completes once the log leads elsewhere — or after
                // the transfer's own bound, one election timeout, in this
                // owner's periods — while this replica keeps ticking and
                // beating so the heir is caught up and asked. A leader that
                // went silent cost the survivors that whole timeout.
                let bound = self.replica.heir().and_then(|heir| {
                    self.replica.hand_off(heir).ok()?;
                    let timeout = u64::try_from(self.replica.election_tick()).ok()?;
                    self.pace.periods().checked_add(timeout)
                });
                self.stopping = Some((response, bound.unwrap_or(0)));
                self.drain()?;
                return self.finish_stop();
            }
        }
        Ok(false)
    }
    /// Whether a pending stop no longer waits on a hand-off: the log leads
    /// elsewhere, or the hand-off's bound has passed.
    fn handed_off(&self) -> bool {
        self.stopping
            .as_ref()
            .is_some_and(|(_, bound)| !self.replica.leads() || self.pace.periods() >= *bound)
    }
    /// Completes a pending stop once nothing waits on a hand-off; whether
    /// the owner ends.
    fn finish_stop(&mut self) -> Result<bool, ControlError> {
        if !self.handed_off() {
            return Ok(false);
        }
        let Some((response, _)) = self.stopping.take() else {
            return Ok(false);
        };
        // Stop follow-up reads/enrollment admissions before the final
        // drain. Otherwise a committed refresh can enqueue a new
        // ReadIndex during drain and invalidate the checkpoint fence.
        // This releases caller interest only: admitted proposals and
        // their prepared state remain owned by the replica below.
        self.stop_waiters();
        let result = self.drain().and_then(|_| {
            if self.replica.has_pending() || self.replica.applied_index() == 0 {
                Ok(())
            } else {
                self.replica.checkpoint()
            }
        });
        let _ = response.send(result.map_err(ControlFailure::from));
        Ok(true)
    }
    fn observe_root(&self, input: Allocation) -> Result<RootObservation, ControlFailure> {
        if self.replica.identity().scope != ControlScope::Root {
            return Err(ControlFailure::WrongOwner);
        }
        let mut bytes = [
            ControlRead::State,
            ControlRead::Contacts,
            ControlRead::Configuration,
        ]
        .iter()
        .try_fold(0usize, |bytes, query| {
            bytes
                .checked_add(self.replica.read_charge(query)?)
                .ok_or(ControlError::Capacity)
        })
        .map_err(ControlFailure::from)?;
        if let Some(authority) = self.replica.authority() {
            // Compact serialized keys can be much smaller than nested tree
            // nodes. Use the registry's structural heap bound for this clone.
            let size = authority
                .charged_bytes()
                .map_err(|_| ControlFailure::Capacity)?;
            bytes = size
                .checked_add(4096)
                .and_then(|n| bytes.checked_add(n))
                .ok_or(ControlFailure::Capacity)?;
        }
        let state = self
            .budget
            .reserve(BudgetKind::Control, BudgetLane::Completion, bytes)
            .map_err(|_| ControlFailure::Capacity)?
            .commit();
        let ControlReadResult::State(snapshot) = self
            .replica
            .read_local(&ControlRead::State)
            .map_err(ControlFailure::from)?
        else {
            return Err(ControlFailure::WrongOwner);
        };
        let ControlReadResult::Contacts(contacts) = self
            .replica
            .read_local(&ControlRead::Contacts)
            .map_err(ControlFailure::from)?
        else {
            return Err(ControlFailure::WrongOwner);
        };
        let ControlReadResult::Configuration(configuration) = self
            .replica
            .read_local(&ControlRead::Configuration)
            .map_err(ControlFailure::from)?
        else {
            return Err(ControlFailure::WrongOwner);
        };
        Ok(RootObservation {
            snapshot,
            contacts,
            configuration,
            authority: self
                .replica
                .authority()
                .map(|authority| authority.checkpoint().clone()),
            _input: input,
            _state: state,
        })
    }
    /// The owner's period past which a held frame is stepped without the
    /// one it overtook: the probe timeout of the path it came by, as this
    /// node measures it (RFC 9002 §6.2: what is not here by then was lost),
    /// in this owner's periods, and one more for the period under way,
    /// whose phase is unknown.
    fn patience_until(&self, round_trip: std::time::Duration) -> u64 {
        self.pace
            .periods()
            .saturating_add(focal_timing::ProgressDeadline::periods(
                focal_wire::probe_timeout(round_trip),
                self.config.tick,
            ))
            .saturating_add(1)
    }
    /// Step the frames the resequencer let go, in their order.
    fn step_due(&mut self) {
        while let Some(held) = self.resequencer.take_due() {
            self.frames_let_go = self.frames_let_go.saturating_add(1);
            self.step_held(held);
        }
    }
    /// Step what was held behind the frame from `source` just stepped.
    fn step_ready(&mut self, source: u64) {
        while let Some(held) = self.resequencer.step_ready(source) {
            self.step_held(held);
        }
    }
    /// Step a held frame; it is answered once the drain that follows has
    /// run (`answer_stepped`), or refused now as its step was.
    fn step_held(&mut self, held: HeldControlFrame) {
        let HeldControlFrame {
            source,
            message,
            mut header,
            response,
            charge,
        } = held;
        let stepped = self.replica.step_authenticated(source, &message);
        if stepped.is_ok() && self.stepped.try_reserve(1).is_ok() {
            self.stepped.push((header, response, charge));
            return;
        }
        header.result = Response::Error(if stepped.is_ok() {
            AccessError::Capacity
        } else {
            AccessError::Unavailable
        });
        let _ = response.send(Completed {
            response: header,
            _input: charge,
            _output: None,
        });
    }
    /// Answer the held frames stepped since the last drain: accepted once
    /// the drain ran, unavailable if it failed.
    fn answer_stepped(&mut self, drained: bool) {
        for (mut header, response, charge) in self.stepped.drain(..) {
            header.result = if drained {
                Response::PeerAccepted
            } else {
                Response::Error(AccessError::Unavailable)
            };
            let _ = response.send(Completed {
                response: header,
                _input: charge,
                _output: None,
            });
        }
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
        let enrollment = matches!(
            verified.request().operation,
            Operation::EnrollmentControl { .. }
        );
        let mut peer_accepted = false;
        let mut held: Option<(u64, u64, u64)> = None;
        let peer_rpc = matches!(
            verified.request().operation,
            Operation::Raft { .. } | Operation::RaftOrdered { .. }
        );
        let root_peer = if self.replica.identity().scope == ControlScope::Root
            && matches!(
                verified.request().operation,
                Operation::Raft { .. }
                    | Operation::RaftOrdered { .. }
                    | Operation::PeerControl { .. }
                    | Operation::EnrollmentControl { .. }
            ) {
            match (
                verified.peer().role(),
                verified.peer().certificate_fingerprint(),
            ) {
                (PeerRole::Node { node_id }, Some(fingerprint)) => Some(RootPeer {
                    node: node_id,
                    principal: verified.peer().principal().0,
                    fingerprint,
                    key: verified.peer().renewal_of(),
                }),
                _ => None,
            }
        } else {
            None
        };
        let result = (|| -> Result<ControlReply, ControlFailure> {
            let request = verified.request();
            let principal = verified.peer().principal();
            if request.ledger != self.config.namespace {
                return Err(ControlFailure::Unauthorized);
            }
            if let Some(peer) = root_peer {
                // A request can outlive the transport grant that admitted it.
                // Publish available durable state before checking its enrollment
                // and check again when a queued read completes.
                self.drain().map_err(ControlFailure::from)?;
                self.authorize_root_peer(peer)?;
            }
            let replication = match &request.operation {
                Operation::Raft { group, message } => Some((*group, None, message)),
                Operation::RaftOrdered {
                    group,
                    epoch,
                    sequence,
                    message,
                } => Some((*group, Some((*epoch, *sequence)), message)),
                _ => None,
            };
            if let Some((group, order, message)) = replication {
                let PeerRole::Node { node_id } = verified.peer().role() else {
                    return Err(ControlFailure::Unauthorized);
                };
                if group != self.replica.identity().group || !self.replica.accepts_peer(node_id) {
                    return Err(ControlFailure::Unauthorized);
                }
                // A bulk frame is stepped in the order it left its sender
                // (27 §12): one that overtook the frame before it is held
                // for it, for its patience at most.
                if let Some((epoch, sequence)) = order {
                    match self.resequencer.admit(node_id, epoch, sequence) {
                        Err(crate::resequence::Capacity) => return Err(ControlFailure::Capacity),
                        Ok(crate::resequence::Admission::Hold) => {
                            self.frames_held = self.frames_held.saturating_add(1);
                            held = Some((
                                node_id,
                                sequence,
                                self.patience_until(verified.path_round_trip()),
                            ));
                            return Err(ControlFailure::Invalid);
                        }
                        Ok(crate::resequence::Admission::Stale) => {
                            self.frames_stale = self.frames_stale.saturating_add(1);
                        }
                        Ok(crate::resequence::Admission::Step) => {}
                    }
                    self.step_due();
                }
                self.replica
                    .step_authenticated(node_id, message)
                    .map_err(ControlFailure::from)?;
                if order.is_some() {
                    self.step_ready(node_id);
                }
                self.drain().map_err(ControlFailure::from)?;
                peer_accepted = true;
                return Err(ControlFailure::Invalid);
            }
            let contact = matches!(request.operation, Operation::NodeContact { .. });
            let placement = matches!(request.operation, Operation::PlacementControl { .. });
            // The clients a request may be named by: the peer's principal;
            // over placement control, a node's root intents by the client
            // derived from its principal (24 §16), and its partition intents
            // by its local client — the name its own agent journals under
            // while its replica leads the group, carried through the node
            // that leads it once leadership moved (F24; the decoder admits
            // the same three, and so must the receipt a retry asks for).
            let sender = match verified.peer().role() {
                PeerRole::Node { node_id } => Some(node_id),
                _ => None,
            };
            let cluster = self.replica.identity().cluster.0;
            let own_client = |client: [u8; 16]| {
                client == principal.0
                    || (placement
                        && (client == crate::placement_control::root_intent_client(principal.0)
                            || sender.is_some_and(|node| {
                                client
                                    == crate::placement_agent::PlacementAgent::local_client(
                                        cluster, node,
                                    )
                            })))
            };
            if placement {
                // A node's own placement facts: bind the certificate-backed
                // identity to the enrollment this owner has installed.
                self.drain().map_err(ControlFailure::from)?;
                match (
                    verified.peer().role(),
                    verified.peer().certificate_fingerprint(),
                ) {
                    (PeerRole::Node { node_id }, Some(fingerprint)) => {
                        self.authorize_placement_peer(
                            node_id,
                            principal.0,
                            fingerprint,
                            verified.peer().renewal_of(),
                        )?;
                    }
                    _ => return Err(ControlFailure::Unauthorized),
                }
            }
            let (group, bytes, read_only): (&[u8; 16], &[u8], bool) = match &request.operation {
                Operation::EnrollmentControl { group, request, .. }
                    if matches!(verified.peer().role(), PeerRole::Node { .. }) =>
                {
                    (group, request, false)
                }
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
                Operation::PlacementControl { group, request }
                    if matches!(verified.peer().role(), PeerRole::Node { .. }) =>
                {
                    (group, request, false)
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
            let rpc = if enrollment {
                self.config
                    .enrollment_authority
                    .as_ref()
                    .ok_or(ControlFailure::Unauthorized)?
                    .decode(&self.replica, &verified)?
            } else if contact {
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
            } else if placement {
                crate::placement_control::decode_placement_control(
                    &verified,
                    bytes,
                    self.replica.identity().cluster.0,
                )?
            } else {
                ControlRpc::decode(bytes, self.replica.limits().max_command_bytes)
                    .map_err(ControlFailure::from)?
            };
            if self
                .pending
                .len()
                .checked_add(usize::from(self.directory.is_some()))
                .and_then(|count| count.checked_add(usize::from(self.authority_refresh.is_some())))
                .is_none_or(|count| count >= self.config.pending_requests)
            {
                return Err(ControlFailure::Capacity);
            }
            if enrollment {
                let command = match rpc {
                    ControlRpc::Read(ControlRead::State) => None,
                    ControlRpc::Submit(request) => Some(Box::new(request)),
                    _ => return Err(ControlFailure::Unauthorized),
                };
                self.nonce = self.nonce.checked_add(1).ok_or(ControlFailure::Capacity)?;
                let mut context = b"focal.control.enrollment.v1\0".to_vec();
                context.extend_from_slice(&self.nonce.to_be_bytes());
                context.extend_from_slice(&principal.0);
                context.extend_from_slice(&request.request_id.0);
                // Even a cached exact receipt is behind a fresh quorum barrier.
                // A former leader cannot authorize against stale revocation.
                self.replica
                    .read_index(context.clone())
                    .map_err(ControlFailure::from)?;
                waiting = Some(Waiting::Enrollment {
                    context,
                    request: command,
                });
                return Err(ControlFailure::Unavailable);
            }
            match rpc {
                ControlRpc::Transfer(request) => {
                    if self.replica.has_pending() {
                        waiting = Some(Waiting::Turn(Some(Box::new(Turn::Transfer(request)))));
                        Err(ControlFailure::OutcomeUnknown)
                    } else {
                        self.replica
                            .transfer(&request)
                            .map_err(ControlFailure::from)?;
                        Ok(ControlReply::TransferInitiated {
                            target: request.target,
                        })
                    }
                }
                ControlRpc::Submit(request) => {
                    if !contact && matches!(request.command, ControlCommand::NodeContact(_)) {
                        return Err(ControlFailure::Unauthorized);
                    }
                    // Retiring a contact is the operator's (24 §19): only the
                    // local admin path, never a peer over the network.
                    if matches!(request.command, ControlCommand::RetireContact(_))
                        && !matches!(verified.peer().role(), PeerRole::Runtime)
                    {
                        return Err(ControlFailure::Unauthorized);
                    }
                    if !own_client(request.id.client) {
                        return Err(ControlFailure::Unauthorized);
                    }
                    if self
                        .replica
                        .pending_request()
                        .is_some_and(|held| held != request.id)
                    {
                        waiting = Some(Waiting::Turn(Some(Box::new(Turn::Submit(Box::new(
                            request,
                        ))))));
                        Err(ControlFailure::OutcomeUnknown)
                    } else {
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
                }
                ControlRpc::Read(query) => {
                    if matches!(&query, ControlRead::Receipt(id) | ControlRead::AdminReceipt {id} if !own_client(id.client))
                    {
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
        if let Some((source, sequence, until)) = held {
            // Held for the frame it overtook: answered once it is stepped
            // and the drain that follows has run.
            let (_, request) = verified.into_parts();
            let Operation::RaftOrdered { message, .. } = request.operation else {
                let mut header = header;
                header.result = Response::Error(AccessError::Unavailable);
                let _ = response.send(Completed {
                    response: header,
                    _input: charge,
                    _output: None,
                });
                return;
            };
            let frame = HeldControlFrame {
                source,
                message,
                header,
                response,
                charge,
            };
            if let Err(frame) = self.resequencer.hold(source, sequence, frame, until) {
                let mut header = frame.header;
                header.result = Response::Error(AccessError::Capacity);
                let _ = frame.response.send(Completed {
                    response: header,
                    _input: frame.charge,
                    _output: None,
                });
            }
            // A lane that was full let what it held go, this frame with it.
            self.step_due();
            if self.drain().is_err() {
                self.answer_stepped(false);
            }
            return;
        }
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
        let deadline = self.request_deadline();
        if let (Some(waiting), Some(deadline)) = (waiting, deadline) {
            self.pending.push_back(Pending {
                header,
                response,
                waiting,
                term: self.replica.status().term,
                deadline,
                enrollment,
                root_peer,
                _charge: charge,
            });
        } else {
            self.send(header, response, result, charge, None);
        }
    }
    /// A reply is given once what it was made from is released: the one that
    /// asked may look at the budget the moment it is answered, and finds
    /// there what its answer holds and nothing of the owner's.
    /// The owner's period at which a request taken now is given up: the
    /// request time in the periods it holds at the configured tick, and the
    /// ticks of the owner's own remembered stall beyond it, as its
    /// replica's patience is (`TickPeriod::patience`): a barrier this owner
    /// was late to run for a stall of its own is not given up for it.
    fn request_deadline(&self) -> Option<u64> {
        self.pace
            .periods()
            .checked_add(focal_timing::ProgressDeadline::periods(
                self.config.request_timeout,
                self.config.tick,
            ))?
            .checked_add(
                u64::try_from(
                    self.pace
                        .patience(self.config.tick, self.config.tick_ceiling),
                )
                .unwrap_or(u64::MAX),
            )
    }
    fn drain(&mut self) -> Result<(), ControlError> {
        // Frames held past their patience go first, in their order; the
        // lanes of members the replica no longer accepts are closed and
        // what they held refused (27 §12).
        if self.resequencer.expire(self.pace.periods()).is_ok() {
            self.step_due();
        }
        let mut gone = Vec::new();
        if self
            .resequencer
            .prune(|source| self.replica.accepts_peer(source), &mut gone)
            .is_ok()
        {
            for frame in gone {
                let mut header = frame.header;
                header.result = Response::Error(AccessError::Unauthorized);
                let _ = frame.response.send(Completed {
                    response: header,
                    _input: frame.charge,
                    _output: None,
                });
            }
        }
        self.ordered
            .retain(|peer, _| self.replica.accepts_peer(*peer));
        let drained = self.drain_turns();
        self.answer_stepped(drained.is_ok());
        drained
    }
    fn drain_turns(&mut self) -> Result<(), ControlError> {
        // What took its turn in a drain is proposed by the next: one more
        // drain for each request that waits, at most, and none when none
        // took a turn.
        let mut turns = self.pending.len();
        loop {
            self.took_turn = false;
            let mut finished = Vec::new();
            let drained = self.drain_events(&mut finished);
            for (pending, result, output) in finished {
                self.finish_charged(pending, result, output);
            }
            drained?;
            if !self.took_turn || turns == 0 {
                return Ok(());
            }
            turns = turns.saturating_sub(1);
        }
    }
    fn drain_events(&mut self, finished: &mut Vec<Finished>) -> Result<(), ControlError> {
        // Every request that waits may be answered by this drain.
        finished
            .try_reserve_exact(self.pending.len())
            .map_err(|_| ControlError::Capacity)?;
        // Declare the source permit first so remaining event buffers drop
        // before it on every exit. Frames/replies below allocate new encoded
        // buffers under their own permits, retained through transport send.
        let _source_allocation;
        let mut events = match self.initial.take() {
            Some(events) => events,
            // What a leader sends leaves while its own write is in flight
            // (27 §3.4, the audit's F17): its members persist it for
            // themselves, so the two writes overlap. This owner's thread
            // then waits for the write, and takes the events whole.
            None => loop {
                match self.replica.try_drain(&self.verifier) {
                    Ok(Some(events)) => break events,
                    Ok(None) => {
                        self.send_early()?;
                        if self.replica.wait_persisted()? {
                            continue;
                        }
                        // No write to wait for (a checkpoint, a log with
                        // no room yet): the drain that waits takes over.
                        match self.replica.drain(&self.verifier) {
                            Ok(events) => break events,
                            Err(error) if self.replica.checkpoint_retryable(&error) => {
                                self.pace.refuse();
                                return Ok(());
                            }
                            Err(error) => return Err(error),
                        }
                    }
                    // Refused before anything was taken: what waits to be
                    // drained waits for the next drain.
                    Err(error) if self.replica.checkpoint_retryable(&error) => {
                        self.pace.refuse();
                        return Ok(());
                    }
                    Err(error) => return Err(error),
                }
            },
        };
        _source_allocation = events.take_allocation();
        let status = self.replica.status();
        self.complete_directory(&events);
        self.complete_authority_refresh(&events);
        for _ in 0..self.pending.len() {
            let Some(mut pending) = self.pending.pop_front() else {
                return Err(ControlError::Failed);
            };
            let mut read_charge = None;
            let mut written = None;
            let mut turn = None;
            let waited_its_turn = matches!(pending.waiting, Waiting::Turn(_));
            let ready = match &mut pending.waiting {
                // Its turn has come: nothing holds the replica's proposal.
                Waiting::Turn(waited)
                    if pending.term == status.term
                        && status.role == StateRole::Leader
                        && !self.replica.has_pending() =>
                {
                    self.took_turn = true;
                    match waited.take().map(|turn| *turn) {
                        Some(Turn::Submit(request)) => {
                            match self.replica.submit(*request, &self.verifier) {
                                Ok(ControlSubmission::Existing(receipt)) => {
                                    Some(Ok(ControlReply::Committed(receipt)))
                                }
                                Ok(ControlSubmission::Pending(id)) => {
                                    written = Some(id);
                                    None
                                }
                                Err(error) => Some(Err(ControlFailure::from(error))),
                            }
                        }
                        Some(Turn::Transfer(request)) => Some(
                            self.replica
                                .transfer(&request)
                                .map(|()| ControlReply::TransferInitiated {
                                    target: request.target,
                                })
                                .map_err(ControlFailure::from),
                        ),
                        None => Some(Err(ControlFailure::Unavailable)),
                    }
                }
                // A read barrier confirmed, by this replica as leader or
                // by the leader it asked through (27 §5): the read is
                // served from what this replica has applied past it.
                Waiting::Enrollment { context, request }
                    if pending.term == status.term
                        && events.read_states.iter().any(|read| {
                            &read.context == context && read.index <= self.replica.applied_index()
                        }) =>
                {
                    let result = (|| {
                        let peer = pending.root_peer.ok_or(ControlFailure::Unauthorized)?;
                        self.config
                            .enrollment_authority
                            .as_ref()
                            .ok_or(ControlFailure::Unauthorized)?
                            .authorize_current(
                                &self.replica,
                                peer.node,
                                peer.principal,
                                peer.fingerprint,
                                peer.key,
                            )?;
                        if let Some(request) = request.take() {
                            // Admitted, and another request holds the
                            // proposal: it waits its turn like any other.
                            if self
                                .replica
                                .pending_request()
                                .is_some_and(|held| held != request.id)
                            {
                                turn = Some(Turn::Submit(request));
                                return Ok(None);
                            }
                            match self
                                .replica
                                .submit(*request, &self.verifier)
                                .map_err(ControlFailure::from)?
                            {
                                ControlSubmission::Existing(receipt) => {
                                    Ok(Some(ControlReply::Committed(receipt)))
                                }
                                ControlSubmission::Pending(id) => {
                                    written = Some(id);
                                    Ok(None)
                                }
                            }
                        } else {
                            let bytes = self
                                .replica
                                .read_charge(&ControlRead::State)
                                .map_err(ControlFailure::from)?;
                            read_charge = Some(
                                self.budget
                                    .reserve(BudgetKind::Control, BudgetLane::Ordinary, bytes)
                                    .map_err(|_| ControlFailure::Capacity)?
                                    .commit(),
                            );
                            self.replica
                                .read_local(&ControlRead::State)
                                .map(ControlReply::Read)
                                .map(Some)
                                .map_err(ControlFailure::from)
                        }
                    })();
                    match result {
                        Ok(None) => None,
                        Ok(Some(reply)) => Some(Ok(reply)),
                        Err(error) => Some(Err(error)),
                    }
                }
                Waiting::Write(id) => self
                    .replica
                    .receipt(*id)?
                    .map(ControlReply::Committed)
                    .map(Ok),
                Waiting::Read { context, query, .. }
                    if pending.term == status.term
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
            if let Some(id) = written {
                pending.waiting = Waiting::Write(id);
            }
            // A request is given its request time from its turn, and one
            // that waits its turn is given it again each time the turn
            // passes: its wait is charged to the progress of what it waits
            // on, the commands decided before it (at most as many as the
            // owner admits), never to the time they took. Before, the
            // fifth of five writes that came together was given one
            // request time for all five, and was given up on a slow disk
            // (the macOS run of 2026-10-01).
            if let Some(turn) = turn {
                pending.waiting = Waiting::Turn(Some(Box::new(turn)));
                if let Some(deadline) = self.request_deadline() {
                    pending.deadline = deadline;
                }
            } else if waited_its_turn
                && self.took_turn
                && let Some(deadline) = self.request_deadline()
            {
                pending.deadline = deadline;
            }
            if let Some(result) = ready {
                let result = if pending.enrollment {
                    pending
                        .root_peer
                        .ok_or(ControlFailure::Unauthorized)
                        .and_then(|peer| {
                            self.config
                                .enrollment_authority
                                .as_ref()
                                .ok_or(ControlFailure::Unauthorized)?
                                .authorize_current(
                                    &self.replica,
                                    peer.node,
                                    peer.principal,
                                    peer.fingerprint,
                                    peer.key,
                                )
                        })
                        .and(result)
                } else {
                    result
                };
                let result = if let Some(peer) = pending.root_peer {
                    self.authorize_root_peer(peer).and(result)
                } else {
                    result
                };
                finished.push((pending, result, read_charge.take()));
            } else if pending.response.is_closed() {
                drop(pending);
            } else if pending.term != status.term
                // A write and a turn need this replica to lead; a read
                // waits on a barrier a follower is answered too.
                || (status.role != StateRole::Leader
                    && matches!(pending.waiting, Waiting::Write(_) | Waiting::Turn(_)))
                || self.pace.periods() >= pending.deadline
            {
                let failure = match pending.waiting {
                    Waiting::Write(_) => ControlFailure::OutcomeUnknown,
                    _ => ControlFailure::Unavailable,
                };
                finished.push((pending, Err(failure), None));
            } else {
                self.pending.push_back(pending);
            }
            drop(read_charge);
        }
        self.carry(std::mem::take(&mut events.messages), status.node_id)?;
        // Drain has completed its persistence before reporting transport status.
        // Register every newly emitted flight first: an old completion must not
        // act on a newer snapshot for the same peer in this event prefix.
        self.snapshot_feedback
            .poll(status.term, |peer, term, index, result| {
                self.replica.report_snapshot_at(peer, term, index, result)
            })?;
        self.publish_progress(false);
        Ok(())
    }
    /// Send what may be sent while the replica's write is in flight
    /// (`ControlReplica::sendable`). No snapshot is among it: a snapshot is
    /// sent with the events of the drain, where what became of it is told.
    fn send_early(&mut self) -> Result<(), ControlError> {
        let Some(mut early) = self.replica.sendable()? else {
            return Ok(());
        };
        let _charge = early.take_allocation();
        let node = self.replica.status().node_id;
        self.carry(std::mem::take(&mut early.messages), node)
    }
    /// Hand the replica's messages to the driver that carries them.
    fn carry(
        &mut self,
        messages: Vec<focal_consensus::Message>,
        node: u64,
    ) -> Result<(), ControlError> {
        for message in messages {
            if message.msg_type == focal_consensus::MessageType::MsgAppendResponse && message.reject
            {
                self.appends_rejected = self.appends_rejected.saturating_add(1);
            }
            // A bulk frame carries the order it leaves in (27 §12): the next
            // sequence to its peer within its term, for as many peers as a
            // configuration names; the driver finishes it for the peer's
            // profile. Given before any frame is given up here, so a frame
            // given up leaves its gap in the order and its peer lets the
            // frames behind it go past their patience, as for one the path
            // lost; one given up before it had a sequence left no gap, and
            // the appends after it were refused in an order that showed none.
            let urgent = crate::fleet::urgent(&message);
            let sequence = if urgent {
                None
            } else {
                match self.ordered.get(&message.to) {
                    Some((term, last)) if *term == message.term => last.checked_add(1),
                    Some(_) => Some(1),
                    None if self.ordered.len() < crate::fleet::LOST_PEERS => Some(1),
                    None => None,
                }
            };
            if let Some(sequence) = sequence {
                self.ordered.insert(message.to, (message.term, sequence));
            }
            // Register the exact current flight before any local operation can
            // drop it. Replacing a prior receiver fences late transport results.
            let snapshot = match self.snapshot_feedback.begin(&message, &self.budget) {
                Ok(snapshot) => snapshot,
                Err(_) => {
                    if message.msg_type == focal_consensus::MessageType::MsgSnapshot {
                        self.replica.report_snapshot_at(
                            message.to,
                            message.term,
                            message
                                .snapshot
                                .as_deref()
                                .map_or(0, focal_consensus::snapshot_index),
                            focal_consensus::SnapshotStatus::Failure,
                        )?;
                    }
                    self.dropped = self.dropped.saturating_add(1);
                    continue;
                }
            };
            // A message the core made that raft-rs's encoding cannot state
            // is a fault of this replica, as a failed encoding was.
            let length = focal_consensus::envelope::message_len(&message)
                .map_err(|_| ControlError::Failed)?;
            let Some(bytes) = length.checked_mul(2).and_then(|n| n.checked_add(4096)) else {
                return Err(ControlError::Capacity);
            };
            let Ok(charge) =
                self.budget
                    .reserve(BudgetKind::Control, BudgetLane::Completion, bytes)
            else {
                // Told to the core as a frame the driver gave up is: the
                // member is probed (`report_lost`).
                self.dropped = self.dropped.saturating_add(1);
                let _ = self.lost_sender.try_send(message.to);
                continue;
            };
            let encoded =
                focal_consensus::encode_message(&message).map_err(|_| ControlError::Failed)?;
            if encoded
                .len()
                .checked_add(256)
                .is_none_or(|n| n > self.limits.max_frame_bytes as usize)
            {
                self.dropped = self.dropped.saturating_add(1);
                let _ = self.lost_sender.try_send(message.to);
                continue;
            }
            self.nonce = self.nonce.checked_add(1).ok_or(ControlError::Capacity)?;
            let target = message.to;
            let group = self.replica.identity().group;
            let operation = match sequence {
                Some(sequence) => Operation::RaftOrdered {
                    group,
                    epoch: message.term,
                    sequence,
                    message: encoded,
                },
                None => Operation::Raft {
                    group,
                    message: encoded,
                },
            };
            let request = RequestEnvelope {
                protocol: if matches!(operation, Operation::RaftOrdered { .. }) {
                    focal_wire::ORDERED_PROTOCOL_VERSION
                } else {
                    PROTOCOL_VERSION
                },
                ledger: self.config.namespace,
                route_epoch: self.config.route_epoch,
                request_epoch: RequestEpoch(1),
                request_id: RequestId::from_u128((u128::from(node) << 64) | u128::from(self.nonce)),
                operation,
            };
            if self
                .outbound
                .try_send(ControlReplicationFrame {
                    target: message.to,
                    request,
                    snapshot,
                    lost: Some(self.lost_sender.clone()),
                    urgent,
                    _charge: charge.commit(),
                })
                .is_err()
            {
                self.dropped = self.dropped.saturating_add(1);
                let _ = self.lost_sender.try_send(target);
            }
        }
        Ok(())
    }
    fn authorize_placement_peer(
        &self,
        node: u64,
        principal: [u8; 16],
        fingerprint: [u8; 32],
        key: Option<[u8; 32]>,
    ) -> Result<(), ControlFailure> {
        let enrollment = self
            .replica
            .installed_enrollment()
            .ok_or(ControlFailure::Unauthorized)?;
        let now = crate::network_bootstrap::unix_time().map_err(|_| ControlFailure::Unavailable)?;
        authorize_node_peer(enrollment, node, principal, fingerprint, key, now)
    }
    fn authorize_root_peer(&self, peer: RootPeer) -> Result<(), ControlFailure> {
        let enrollment = self
            .replica
            .enrollment()
            .ok_or(ControlFailure::Unauthorized)?;
        let now = crate::network_bootstrap::unix_time().map_err(|_| ControlFailure::Unavailable)?;
        authorize_node_peer(
            enrollment,
            peer.node,
            peer.principal,
            peer.fingerprint,
            peer.key,
            now,
        )
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
    /// Refresh the log snapshot when either the log has grown
    /// `checkpoint_interval` entries past the last snapshot (steady-state
    /// bounding) or a membership change has committed above the snapshot floor
    /// so the stored snapshot's configuration is stale.
    ///
    /// The second trigger is essential for correctness, not just bounding: a
    /// snapshot carries the committed configuration at its index, and a
    /// follower cannot be caught up by a snapshot whose configuration predates
    /// the follower's own admission (it would install a membership that does
    /// not contain it). The genesis snapshot excludes every later-joined node,
    /// so without this a freshly joined learner whose `next_index` reaches the
    /// compaction floor wedges: the leader can neither append below the floor
    /// nor install its stale snapshot, and it oscillates Probe/Snapshot
    /// forever. Re-checkpointing at the current applied index mints a snapshot
    /// whose configuration includes the follower, which then installs and
    /// catches up. It fires at most once per membership change (afterward the
    /// floor is at or above `configuration_index`), never per entry.
    ///
    /// Compaction is skipped, not forced, when a proposal, directory bootstrap,
    /// enrollment refresh or caller read is in flight so no in-flight fence is
    /// invalidated; the next tick retries. `NotReady`/`Busy` is transient and
    /// never fails the owner.
    fn maybe_checkpoint(&mut self) -> Result<(), ControlError> {
        let applied = self.replica.applied_index();
        if applied == 0
            || self.replica.has_pending()
            || self.directory.is_some()
            || self.authority_refresh.is_some()
            || !self.pending.is_empty()
        {
            return Ok(());
        }
        let floor = self.replica.snapshot_index();
        if applied <= floor {
            return Ok(());
        }
        // A group founded here on a sealed image compacts at founding: its
        // log begins after the image, and a member seated later (24 §13)
        // holds no image to replay it onto, so it is brought up by snapshot
        // alone — never sent the log's beginning.
        let founding = self.replica.founded_from_image() && floor == 0;
        let interval_reached = applied.saturating_sub(floor) >= self.config.checkpoint_interval;
        // The stored snapshot carries the configuration at its index. Once a
        // membership change commits above the floor, that snapshot excludes the
        // member(s) it added and can no longer catch them up, so refresh it to
        // the current committed configuration. This fires at most once per
        // membership change (afterward the floor is at or above the change),
        // never per entry, so it cannot storm.
        let stale_configuration = floor < self.replica.configuration_index();
        if !interval_reached && !stale_configuration && !founding {
            return Ok(());
        }
        match self.replica.checkpoint() {
            Ok(()) => Ok(()),
            // Compaction is opportunistic: a refusal that changed nothing is
            // tried again on a later tick and never ends the owner.
            Err(error) if self.replica.checkpoint_retryable(&error) => Ok(()),
            Err(error) => Err(error),
        }
    }
    /// What the driver could not reach since the last tick, told to the
    /// core: it probes those members instead. Gathered first, each peer
    /// once (coalesced and dropped beyond the bound, both counted), then
    /// told while the core can be told; one fenced by a write it still
    /// persists hears the rest next tick, the peers keeping their place.
    fn report_lost(&mut self) -> Result<(), ControlError> {
        for _ in 0..crate::fleet::LOST_PEERS {
            let Ok(peer) = self.lost.try_recv() else {
                break;
            };
            match self.lost_peers.binary_search(&peer) {
                Ok(_) => self.lost_coalesced = self.lost_coalesced.saturating_add(1),
                Err(at)
                    if self.lost_peers.len() < crate::fleet::LOST_PEERS
                        && self.lost_peers.try_reserve(1).is_ok() =>
                {
                    self.lost_peers.insert(at, peer);
                }
                Err(_) => self.lost_dropped = self.lost_dropped.saturating_add(1),
            }
        }
        while let Some(&peer) = self.lost_peers.last() {
            match self.replica.report_unreachable(peer) {
                Ok(()) => {
                    self.lost_peers.pop();
                    self.unreachable = self.unreachable.saturating_add(1);
                }
                Err(error) if self.replica.checkpoint_retryable(&error) => return Ok(()),
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
    fn publish_progress(&self, stopped: bool) {
        let status = self.replica.status();
        self.progress.send_modify(|state| {
            state.value = ControlProgress {
                identity: self.replica.identity(),
                node: status.node_id,
                leader: status.leader_id,
                term: status.term,
                applied_index: self.replica.applied_index(),
                revisions: self.replica.revisions(),
                dropped_replication: self.dropped,
                peers_unreachable: self.unreachable,
                appends_rejected: self.appends_rejected,
                frames_held: self.frames_held,
                frames_let_go: self.frames_let_go,
                frames_stale: self.frames_stale,
                peer_reports_coalesced: self.lost_coalesced,
                peer_reports_dropped: self.lost_dropped,
                stopped,
                snapshot_index: self.replica.snapshot_index(),
                peers: self.replica.peer_progress(),
                failure: self.failure.clone(),
            }
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

#[cfg(test)]
#[path = "control_host_auth_tests.rs"]
mod auth_tests;

#[cfg(test)]
#[path = "control_snapshot_tests.rs"]
mod snapshot_tests;

/// A node peer's authority at this replica: the certificate it presented is
/// one the committed registry authorizes for it (the current one, or one a
/// renewal retired that is still within its grace), or — when the registry
/// here does not name that certificate — the peer was admitted under the
/// key its unrevoked enrollment holds, a renewal this replica has not
/// applied yet (24 §11): the very commit it may be receiving from that peer.
pub(crate) fn authorize_node_peer(
    enrollment: &focal_enrollment::EnrollmentRegistry,
    node: u64,
    principal: [u8; 16],
    fingerprint: [u8; 32],
    key: Option<[u8; 32]>,
    now: i64,
) -> Result<(), ControlFailure> {
    if authorize_node_contact(enrollment, node, principal, fingerprint, now).is_ok() {
        return Ok(());
    }
    let key = key.ok_or(ControlFailure::Unauthorized)?;
    focal_control::authorize_enrolled_key(enrollment, node, principal, key)
        .map(|_| ())
        .map_err(|_| ControlFailure::Unauthorized)
}
