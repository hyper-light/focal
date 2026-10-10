//! Replicated session owner. Ingress, replication and disk apply use separate
//! bounded queues. Client success waits for quorum commit and graph publication.
//! Directory/control code supplies the already authorized Session and route epoch.
use crate::{custody::CustodyScope, evidence_service::EvidenceWitness};
use crate::{
    host::{access, barrier_refused, finish_response, known_receipt},
    reads::{ListReadContext, ReadViews},
    streams::{PendingStream, Streams},
};
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
    FleetRemoval, FleetReply, FleetStatus, FleetStopReport, MAX_SESSIONS, ManagedFleetConfig,
};
pub use grouped::{FleetReplica, FleetReplication, FleetTenant, ReplicaFleet, ReplicaFleetParts};
pub use placement_owner::{CommittedPlacement, PlacementReply, SessionPlacementRequest};
use placement_owner::{PendingPlacementCall, PlacementCall};
pub use range_owner::{
    ArchivedFamily, RANGE_CONTROL_SCHEMA, RangeControlReply, RangeControlRequest, RangeFact,
    RangeFactRequest, RangeHistoryView, RangeMemberView, RangePendingView, RangeView,
    SealedOutcomes,
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

/// Entries a session applies past its last checkpoint before it may checkpoint
/// again ([`ReplicaConfig::checkpoint_after_entries`]'s default): the floor
/// under the size rule, so a small state is not imaged at every few entries.
pub const CHECKPOINT_AFTER_ENTRIES: u64 = 4096;
/// The times its last image's bytes the applied log a session holds may come
/// to before it checkpoints, where a checkpoint writes the image alone (the
/// shell's; focal-log's rewrites the log beside it and keeps the floor) (Ongaro's thesis §5.1.2, "When to snapshot": a
/// snapshot once the log exceeds the previous snapshot times an expansion
/// factor), as hyper-durable's own rule and slates state it. A checkpoint
/// re-encodes the whole state, so at a fixed entry count the cost of
/// checkpointing grows with the state and its total with its square; under
/// this rule an image is written for every image's worth of log, a share of
/// half of what the session writes, whatever its size.
pub const CHECKPOINT_EXPANSION: u64 = 1;
/// The share of its session's memory the applied log may hold before it
/// checkpoints whatever its image: the log's entries are charged to the same
/// budget as the state, so a large state's log is cut before it takes the
/// room the state grows into.
pub const CHECKPOINT_MEMORY_SHARE: u64 = 8;
/// The most entries a session applies past its last checkpoint whatever their
/// bytes: what a restart replays (etcd's default snapshot count).
pub const CHECKPOINT_MAX_ENTRIES: u64 = 100_000;

/// The applied log a session holds past its last image: its entries and their
/// bytes.
#[derive(Clone, Copy, Debug)]
pub(crate) struct CheckpointLog {
    pub(crate) entries: u64,
    pub(crate) held: u64,
}

/// Whether a log past the entry floor is due to be checkpointed: once it
/// outweighs its last image (`image` bytes) times `expansion`, holds its
/// share of the session's `memory`, or holds the most entries a restart
/// replays.
pub(crate) fn checkpoint_due(log: CheckpointLog, image: u64, memory: u64, expansion: u64) -> bool {
    log.entries >= CHECKPOINT_MAX_ENTRIES
        || log.held > image.saturating_mul(expansion)
        || log.held
            >= memory
                .checked_div(CHECKPOINT_MEMORY_SHARE)
                .unwrap_or(u64::MAX)
}
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
    /// The longest the tick period is stretched for a far group (27 §3.1
    /// P2); the election timeout is at most the election ticks times this.
    pub tick_ceiling: Duration,
    pub request_timeout: Duration,
    /// Checkpoint and compact the log once this many entries have applied
    /// past the last snapshot (26 §3, the log's retirement boundary), and the
    /// size rule holds ([`CHECKPOINT_EXPANSION`]).
    pub checkpoint_after_entries: u64,
    /// The times its last image's bytes the applied log may come to before a
    /// checkpoint ([`CHECKPOINT_EXPANSION`]); zero checkpoints at the entry
    /// floor alone.
    pub checkpoint_expansion: u64,
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
            tick_ceiling: Duration::from_secs(2),
            request_timeout: Duration::from_secs(5),
            checkpoint_after_entries: CHECKPOINT_AFTER_ENTRIES,
            checkpoint_expansion: CHECKPOINT_EXPANSION,
            #[cfg(test)]
            checkpoint_observer: None,
        }
    }
}

pub struct ReplicationFrame {
    pub target: u64,
    pub request: RequestEnvelope,
    snapshot: Option<oneshot::Sender<focal_consensus::SnapshotStatus>>,
    /// Where the driver says the peer could not be reached: the owner
    /// reports it to the core, which probes the member instead of
    /// streaming to it. Bounded by the members a configuration names.
    lost: Option<mpsc::SyncSender<u64>>,
    /// What a group cannot do without — a heartbeat, a vote, an answer:
    /// everything but appends and snapshots. The driver sends it before the
    /// appends that wait for the same peer, and gives it up last
    /// (`replication::drive`).
    pub(crate) urgent: bool,
    _charge: Allocation,
}
/// Whether a message is what a group cannot do without: anything but an
/// append on its way to a member and a snapshot. An append names a place
/// in the log — the entry before what it carries — whether it carries
/// entries or only the commit that followed them; the member judges it by
/// what it holds, so it goes in the order of the appends before it
/// (27 §12). Sent ahead of them as a heartbeat is, an empty append named
/// an entry the member had yet to receive and was refused for it (the
/// jittered fleet: every refusal with the order kept was one, 2026-10-02).
pub(crate) fn urgent(message: &focal_consensus::Message) -> bool {
    let append = message.msg_type == focal_consensus::MessageType::MsgAppend;
    let snapshot = message.msg_type == focal_consensus::MessageType::MsgSnapshot;
    !append && !snapshot
}
/// The peers an owner may have lost exchanges with between two of its
/// periods: at most every member once (`hyper_raft::MAX_MEMBERS`); a peer
/// lost more often within a period is reported once.
pub(crate) const LOST_PEERS: usize = 1024;
impl ReplicationFrame {
    /// The frame did not reach its peer: it could not be reached, it
    /// refused, or the driver had no room for the frame.
    pub(crate) fn lost(&mut self) {
        if let Some(lost) = self.lost.take() {
            let _ = lost.try_send(self.target);
        }
    }
    #[cfg(test)]
    pub(crate) fn for_test(
        target: u64,
        request: RequestEnvelope,
        lost: mpsc::SyncSender<u64>,
        budget: &MemoryBudget,
    ) -> Result<Self, LedgerError> {
        Ok(Self {
            target,
            request,
            snapshot: None,
            lost: Some(lost),
            urgent: false,
            _charge: budget
                .reserve(BudgetKind::Control, BudgetLane::Completion, 4096)?
                .commit(),
        })
    }
    #[cfg(test)]
    pub(crate) fn urgent_for_test(mut self) -> Self {
        self.urgent = true;
        self
    }
    pub(crate) fn report_snapshot(&mut self, accepted: bool) {
        crate::snapshot_feedback::complete(&mut self.snapshot, accepted);
    }
}
/// A stopping leader's hand-off of its log (27 §5): the voter it asked to
/// campaign, and whether the log led elsewhere before the replica stopped
/// ticking. None while the replica did not lead when its stop began.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StopHandOff {
    pub heir: u64,
    pub completed: bool,
}
#[derive(Clone, Debug)]
pub struct ReplicaProgress {
    pub node: u64,
    pub leader: u64,
    pub term: u64,
    /// The replica's role in its term: a member that names no leader is
    /// told apart as following, asking for votes or holding them.
    pub role: StateRole,
    /// The hand-off a planned stop made, once the stop began.
    pub stop_hand_off: Option<StopHandOff>,
    pub sequence: SessionSeq,
    pub dropped_replication: u64,
    /// Exchanges the driver could not make at all, told to the core so it
    /// probes the peer instead of streaming to it (27 §3.3).
    pub peers_unreachable: u64,
    /// Appends this replica refused for not holding the entry before them
    /// (27 §12): what a frame that overtook another cost before the order
    /// was kept, and what a lost frame costs still.
    pub appends_rejected: u64,
    /// Of those, the ones the order should have spared (27 §12): refused
    /// for an entry past the last this log held when a frame of its leader
    /// was last lost to it, in a term in which the leader sent its appends
    /// ordered and one of them was taken. What a lost frame costs — the
    /// frame let go past its patience, found stale or not stepped, and the
    /// appends behind it until the leader sends again — and a term's first
    /// exchange are not among them.
    pub appends_rejected_in_order: u64,
    /// Frames held for the one they overtook (27 §12), and frames let go
    /// past their patience or their lane without it: the first is what
    /// the path reordered, the second what it lost.
    pub frames_held: u64,
    pub frames_let_go: u64,
    /// Frames behind what was already stepped from their source (27 §12):
    /// stepped as they came, the core judging them.
    pub frames_stale: u64,
    /// Frames that came, or were let go, while the replica's write was in
    /// flight, and waited for it to be durable before they were stepped
    /// (`WaitingFrame`): what was refused, and lost to its peer, before.
    pub frames_waited: u64,
    /// Reports of a peer the owner already held for the core, and reports
    /// beyond the bound on peers held: the feedback is a hint about a
    /// peer, coalesced and never grown.
    pub peer_reports_coalesced: u64,
    pub peer_reports_dropped: u64,
    /// The times this replica's owner asked what the log had answered and
    /// found its write still out. An owner the log wakes asks once as it
    /// queues a write, and again only at its tick (27 §9).
    pub waits_asked: u64,
    /// The times the log's answer to this replica's write woke its owner
    /// (`Session::notify_persisted`, on an owner that shares its thread
    /// among sessions): the answer to `waits_asked` (27 §9).
    pub waits_answered: u64,
    /// The times this replica, with a write out, was looked at because an
    /// answer's signal found its owner's signals full — which answer was not
    /// known (`GroupOwner::sweep`).
    pub waits_swept: u64,
    pub stopped: bool,
    /// The group's voters as this replica's committed configuration names
    /// them; the paths a pace is derived from (27 §3.1 P2).
    pub voters: Vec<u64>,
    /// The members this replica admits replication from by the directory's
    /// word, sorted.
    pub admitted: Vec<u64>,
    /// This replica's election priority, from its committed placement.
    pub priority: i64,
    /// The voters the directory places in the preferred leader's zone.
    pub near: Near,
    /// How often this replica asked leadership to return to the preferred
    /// leader, and how often that did not hold (27 §5).
    pub returns: crate::leader_return::Stats,
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
/// What a [`Work`] is, for the owner's slow steps.
impl Work {
    fn kind(&self) -> &'static str {
        match self {
            Self::Diagnostics(..) => "diagnostics",
            Self::Registration(..) => "registration",
            Self::Request(..) => "request",
            Self::Probe(..) => "probe",
            Self::Transfer(..) => "transfer",
            Self::Membership(..) => "membership",
            Self::ManagedSupport(..) => "managed support",
            Self::ActivateNative(..) => "activate native",
            Self::ImportPayloads(..) => "import payloads",
            Self::Checkpoint(..) => "checkpoint",
            Self::ArtifactPointer(..) => "artifact pointer",
            Self::SeedChunks(..) => "seed chunks",
            Self::InstallSeed(..) => "install seed",
            Self::CustodyObjects(..) => "custody objects",
            Self::CustodyPulled(..) => "custody pulled",
            Self::Admit(..) => "admit",
            Self::Windows(..) => "windows",
            Self::Refence(..) => "refence",
            Self::Placement(..) => "placement",
            Self::Range(..) => "range",
            Self::Evidence(..) => "evidence",
            Self::Stop(..) => "stop",
        }
    }
}
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
    /// The members the committed directory names for this session, and
    /// those of them in its preferred leader's zone.
    Admit(
        Vec<u64>,
        Near,
        oneshot::Sender<Result<(), LedgerError>>,
        Allocation,
    ),
    /// What the paths to the replica's peers hold in flight and their
    /// round trips, by peer: `(peer, window bytes, round trip ns)`. The
    /// bytes a leader sends each ahead of its answers follow (27 §11).
    Windows(Vec<(u64, u64, u64)>, Allocation),
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
    /// The owner's period at which the request is given up: its time in
    /// the owner's rounds and not the clock's (27 §3.1 P2), so an owner
    /// that a loaded machine slows gives up nothing it would have answered.
    deadline: u64,
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
        sender: grouped::OwnerQueue,
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
                        _slot: grouped::Slot::Item {
                            _allocation: slot.commit(),
                        },
                    }))
                    .map_err(|error| host_queue_error(*error))
            }
        }
    }
    /// A peer's frame. A fleet's owner has it queued in the room its
    /// session's peer reserve gives (`TickPeriod::queue_frame`), apart from
    /// the participants' queue (F56): refused only when that room is full,
    /// whatever the participants have queued. A replica's own owner takes it
    /// with the rest of its queue, which it drains as work comes.
    fn try_send_frame(
        &self,
        work: Work,
        pace: &crate::pace::TickPeriod,
    ) -> Result<(), HostQueueError> {
        match self {
            Self::Direct(sender) => sender.try_send(work).map_err(host_queue_error),
            Self::Group {
                ledger,
                incarnation,
                sender,
                ..
            } => {
                let Some(ticket) = pace.queue_frame() else {
                    return Err(HostQueueError::Full);
                };
                sender
                    .send_frame(grouped::Routed {
                        ledger: *ledger,
                        incarnation: *incarnation,
                        work,
                        _slot: grouped::Slot::Frame { _ticket: ticket },
                    })
                    .map_err(|_| HostQueueError::Disconnected)
            }
        }
    }
}
enum HostQueueError {
    Full,
    Disconnected,
}
/// The bytes of entries a leader sends a peer ahead of its answers, from
/// what the path to it holds in flight (its transport's congestion window)
/// and its round trip (27 §11).
///
/// Twice the window: a sender held to the window itself never fills it,
/// and a window that is never filled is never found to be too small — the
/// rule by which a sender's buffer is sized against its congestion window
/// (Linux `tcp_sndbuf_expand`). And a page at least where the path carries
/// a page within one beat of the leader: a path that fast is not kept to a
/// window that only says nothing was sent on it yet, while a thin one is
/// never given a page it would take many beats to carry.
fn inflight_bytes(window: u64, round_trip_ns: u64, beat: Duration, page: u64) -> u64 {
    let twice = window.saturating_mul(2);
    let beat_ns = u64::try_from(beat.as_nanos()).unwrap_or(u64::MAX);
    // What the path carries in one beat: its window, once a round trip.
    let carried = u128::from(window)
        .saturating_mul(u128::from(beat_ns))
        .checked_div(u128::from(round_trip_ns))
        .map_or(0, |bytes| u64::try_from(bytes).unwrap_or(u64::MAX));
    twice.max(page.min(carried))
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
    pace: crate::pace::TickPeriod,
    tick: Duration,
    tick_ceiling: Duration,
}
/// The most members a replica admits by the directory's word: the largest
/// configuration, entering and leaving.
const MAX_ADMITTED: usize = 2048;
/// The election priority of a session's preferred leader, of the voters in
/// its zone, and of its other voters (27 §5).
pub const PREFERRED_LEADER_PRIORITY: i64 = 3;
pub const ZONE_PRIORITY: i64 = 2;
pub const VOTER_PRIORITY: i64 = 1;
/// The voters the committed directory places in the zone of a session's
/// preferred leader: who leads in its place where it cannot.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Near {
    /// The preferred leader they are near to. They rank by it only while
    /// the session's own committed placement prefers the same.
    pub leader: u64,
    /// Sorted, without the leader.
    pub voters: Vec<u64>,
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
impl WaitingFor {
    /// Whether only this replica as leader answers the wait: a read its
    /// own barrier confirms, or a request that waits for one. A leader that
    /// steps down within its term (check-quorum) answers none of them, and
    /// the barrier it waits for dies with its leadership: the wait is given
    /// up at once, not at its deadline. A term that changes gives up every
    /// wait of the term before it.
    fn answered_by_leader(&self) -> bool {
        matches!(
            self,
            WaitingFor::Read { .. }
                | WaitingFor::List { .. }
                | WaitingFor::Select { .. }
                | WaitingFor::Validators { .. }
                | WaitingFor::Traverse { .. }
                | WaitingFor::Summary { .. }
                | WaitingFor::Monitor { .. }
                | WaitingFor::Reconcile { .. }
                | WaitingFor::RequestStreamRead { .. }
                | WaitingFor::RequestStreamControl { .. }
        )
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
/// A peer's frame held for the one it overtook (27 §12): stepped when that
/// one comes, or when its patience passes, and answered as every peer frame
/// is, at the Ready fence.
struct HeldFrame {
    source: u64,
    message: Vec<u8>,
    pending: Pending,
}
/// A peer's frame due to be stepped while the replica's write was in flight
/// (`Owner::waiting_frames`). Consensus takes no input until that write is
/// durable, and refusing the frame lost it: the peer counted it lost, told
/// its core this member was unreachable, and what it acknowledged waited for
/// its next exchange — under load a follower lost two of five of its frames
/// to its leader this way. It waits here, in the order frames came, and is
/// stepped once the write is durable; answered then at the Ready fence as
/// every peer frame is.
struct WaitingFrame {
    frame: HeldFrame,
    /// An ordered frame: one not stepped is lost to its source's stream.
    ordered: bool,
    /// A frame let go past its patience or found stale: a loss, noted once
    /// it is stepped (`Owner::lost_to`).
    lost: bool,
}
/// One leader's appends as this replica took them (`Owner::append_streams`).
/// An answer is judged when the log is durable, after frames stepped since;
/// so a loss is kept as the place it left in the log, not as a moment.
#[derive(Clone, Copy, Debug, Default)]
struct AppendStream {
    /// The term and the first index this log lacked when a frame of the
    /// leader was last lost to it — let go past its patience, found stale,
    /// or not stepped: an append refused in that term for an entry before
    /// it is the loss's, since every append past it waits for the entries
    /// the lost frame carried.
    lost_from: Option<(u64, u64)>,
    /// The term an append of it was last taken in.
    taken_in: Option<u64>,
    /// The term it last sent an ordered frame in: a leader of an older
    /// binary sends its appends plain, and the order spares them nothing.
    ordered_in: Option<u64>,
}
/// How a peer's frame was admitted (`admit_replication`).
enum Replication {
    /// Stepped; answered at the Ready fence by the owner's period given.
    Stepped(u64),
    /// Held for the frame it overtook, until the owner's period `until`.
    Held {
        source: u64,
        sequence: u64,
        until: u64,
        deadline: u64,
    },
    /// Due to be stepped while the replica's write is in flight: waits for
    /// it to be durable (`WaitingFrame`), answered at the Ready fence after.
    Waiting {
        source: u64,
        ordered: bool,
        lost: bool,
        deadline: u64,
    },
}
struct Pending {
    header: ResponseEnvelope,
    response: oneshot::Sender<OwnedResponse>,
    waiting: WaitingFor,
    term: u64,
    /// The owner's period at which the request is given up: its time in
    /// the owner's rounds and not the clock's (27 §3.1 P2), so an owner
    /// that a loaded machine slows gives up nothing it would have answered.
    deadline: u64,
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
    /// The owner's slowest recent steps, for the replica's diagnostics.
    slow: crate::owner_steps::SlowSteps,
    config: ReplicaConfig,
    limits: WireLimits,
    client_limits: WireLimits,
    views: ReadViews,
    streams: Streams,
    runtime: Option<focal_runtime::Runtime>,
    pending: VecDeque<Pending>,
    memberships: VecDeque<PendingMembershipCall>,
    /// The members the committed directory names for this session, sorted:
    /// the peers whose replication this replica admits beside those of its
    /// own applied configuration. A copy that has applied nothing yet knows
    /// only the configuration its log began with.
    admitted: Vec<u64>,
    /// The voters in the preferred leader's zone, by the directory's word.
    near: Near,
    deferred_managed: VecDeque<managed_support_owner::DeferredManaged>,
    deferred_backing: Option<Allocation>,
    snapshot_feedback: crate::snapshot_feedback::SnapshotFeedback,
    placement: Option<PendingPlacementCall>,
    evidence: Option<PendingEvidenceCall>,
    outbound: async_mpsc::Sender<ReplicationFrame>,
    /// Peers the driver could not reach, reported to the core each period.
    lost_sender: mpsc::SyncSender<u64>,
    lost: mpsc::Receiver<u64>,
    /// Peers the driver reported lost, each once, kept until the core can
    /// be told (bounded by the members a configuration names); reports of
    /// a peer already held are coalesced, and reports beyond the bound are
    /// dropped, both counted.
    lost_peers: Vec<u64>,
    lost_coalesced: u64,
    lost_dropped: u64,
    /// `ReplicaProgress::waits_asked`.
    waits_asked: u64,
    /// `ReplicaProgress::waits_answered`.
    waits_answered: u64,
    /// `ReplicaProgress::waits_swept`.
    waits_swept: u64,
    progress: watch::Sender<ProgressState>,
    /// Drawn when this owner started: with the node id it scopes every read
    /// context the owner mints, so a nonce that restarts from zero, or one
    /// aligned with another replica's, never repeats a context (F63).
    incarnation: u64,
    nonce: u64,
    /// The order this owner's bulk frames to each peer leave in (27 §12):
    /// the term they belong to and the last sequence given within it,
    /// pruned to the configuration's members each pass. A term, not the
    /// owner's incarnation: only a leader sends bulk frames, and a node
    /// leads again only in a later term, so a source's epochs only grow —
    /// an incarnation drawn at random at a restart was smaller than the
    /// one before as often as not, and its frames were taken for stale.
    ordered: std::collections::BTreeMap<u64, (u64, u64)>,
    /// A peer's bulk frames held for the ones they overtook, stepped in
    /// their order (`crate::resequence`).
    resequencer: crate::resequence::Resequencer<HeldFrame>,
    /// Peers' frames due to be stepped while the replica's write was in
    /// flight, in the order they came (`WaitingFrame`): stepped once it is
    /// durable. Within the peers' reserve with those held and pending
    /// (`Owner::pending_peers`).
    waiting_frames: VecDeque<WaitingFrame>,
    /// `ReplicaProgress::frames_waited`.
    frames_waited: u64,
    /// `ReplicaProgress::appends_rejected`.
    appends_rejected: u64,
    /// `ReplicaProgress::appends_rejected_in_order`.
    appends_rejected_in_order: u64,
    /// What became of each leader's appends here (`AppendStream`), for as
    /// many peers as the order is kept for, pruned with the configuration.
    append_streams: std::collections::BTreeMap<u64, AppendStream>,
    /// `ReplicaProgress::frames_held`, `frames_let_go` and `frames_stale`.
    frames_held: u64,
    frames_let_go: u64,
    frames_stale: u64,
    support_cursor: u64,
    dropped: u64,
    unreachable: u64,
    /// Whether the stored snapshot named every member, at the configuration
    /// and snapshot indexes it was last asked at (`checkpoint_for_members`).
    members_named: Option<((u64, u64), bool)>,
    #[cfg(test)]
    dropped_snapshots: u64,
    budget: MemoryBudget,
    pace: crate::pace::TickPeriod,
    /// When leadership goes back to the placement's preferred leader.
    leader_return: crate::leader_return::LeaderReturn,
    nonblocking: bool,
    /// The owner takes its work in batches (27 §9): a request it admits
    /// leaves its drain to the batch's end, so the proposals and reads of a
    /// batch go in one write and one round. An owner that shares its thread
    /// among sessions always does — the session drains at its next pass,
    /// after what was dispatched to it in this one; a replica's own owner
    /// does while it takes what is queued (`Owner::take`).
    batching: bool,
    /// A request of the batch waits for the batch's drain: the drain runs
    /// at the batch's end whether or not the replica has anything ready. A
    /// peer's frame is answered at the Ready fence, by the poll the drain
    /// makes; a frame whose step left nothing ready — a message of an older
    /// term the core passes over — is answered by an empty poll. Left to
    /// what else made the replica ready, it waited for its deadline: on a
    /// path that carries a peer's frames one after another, everything
    /// behind it with it, elections and read rounds outlasting their timeouts.
    drain_owed: bool,
    /// The reply to a stop, and the owner's period at which the stop is
    /// given up on.
    stopping: Option<(oneshot::Sender<Result<(), LedgerError>>, u64)>,
    /// While a stopping leader hands its log off: the owner's period at
    /// which the hand-off is given up on.
    handing_off: Option<u64>,
    /// The hand-off this replica's stop made, reported in its progress.
    stop_hand_off: Option<StopHandOff>,
    next_tick: Instant,
    /// When a leader whose period is stretched sends its next heartbeats.
    next_beat: Instant,
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
            ..WireLimits::for_consensus(
                u32::try_from(focal_consensus::DEFAULT_INFLIGHT_WINDOW).unwrap_or(u32::MAX),
            )
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
            || config.tick_ceiling < config.tick
            || config.tick_ceiling > Duration::from_secs(10)
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
        let (lost_sender, lost) = mpsc::sync_channel(LOST_PEERS);
        let (progress, changes) = watch::channel(ProgressState {
            value: ReplicaProgress {
                node: status.node_id,
                leader: status.leader_id,
                term: status.term,
                role: status.role,
                stop_hand_off: None,
                sequence: session.sequence(),
                dropped_replication: 0,
                peers_unreachable: 0,
                frames_held: 0,
                frames_let_go: 0,
                frames_stale: 0,
                frames_waited: 0,
                appends_rejected: 0,
                appends_rejected_in_order: 0,
                peer_reports_coalesced: 0,
                peer_reports_dropped: 0,
                waits_asked: 0,
                waits_answered: 0,
                waits_swept: 0,
                stopped: false,
                voters: status.voters.clone(),
                admitted: Vec::new(),
                priority: session.priority(),
                near: Near::default(),
                returns: crate::leader_return::Stats::default(),
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
        let tick = config.tick;
        let tick_ceiling = config.tick_ceiling;
        let pace = crate::pace::TickPeriod::default();
        pace.announce(session.election_tick());
        let mut incarnation = [0u8; 8];
        getrandom::fill(&mut incarnation).map_err(|_| LedgerError::Capacity)?;
        let incarnation = u64::from_le_bytes(incarnation);
        let lane = session.inflight_window();
        let owner = Owner {
            leader_return: crate::leader_return::LeaderReturn::new(session.election_tick()),
            pace: pace.clone(),
            session,
            config,
            limits,
            client_limits,
            views,
            streams,
            runtime,
            pending: VecDeque::new(),
            memberships: VecDeque::new(),
            admitted: Vec::new(),
            near: Near::default(),
            deferred_managed: VecDeque::new(),
            deferred_backing: None,
            snapshot_feedback: crate::snapshot_feedback::SnapshotFeedback::default(),
            placement: None,
            evidence: None,
            outbound,
            lost_sender,
            lost,
            lost_peers: Vec::new(),
            lost_coalesced: 0,
            lost_dropped: 0,
            waits_asked: 0,
            waits_answered: 0,
            waits_swept: 0,
            progress,
            incarnation,
            nonce: 0,
            ordered: std::collections::BTreeMap::new(),
            resequencer: crate::resequence::Resequencer::new(lane, LOST_PEERS),
            waiting_frames: VecDeque::new(),
            frames_waited: 0,
            appends_rejected: 0,
            appends_rejected_in_order: 0,
            append_streams: std::collections::BTreeMap::new(),
            frames_held: 0,
            frames_let_go: 0,
            frames_stale: 0,
            support_cursor: 0,
            dropped: 0,
            unreachable: 0,
            members_named: None,
            slow: crate::owner_steps::SlowSteps::default(),
            #[cfg(test)]
            dropped_snapshots: 0,
            budget: budget.clone(),
            nonblocking: false,
            batching: false,
            drain_owed: false,
            stopping: None,
            handing_off: None,
            stop_hand_off: None,
            next_tick: Instant::now(),
            next_beat: Instant::now(),
            wake_at: Instant::now(),
        };
        // The room its peers' frames have before its first period.
        owner.pace.set_frame_room(owner.peer_reserve());
        Ok((
            Self {
                sender,
                progress: changes,
                budget,
                client_frame_bytes,
                client_max_items,
                request_timeout,
                pace,
                tick,
                tick_ceiling,
            },
            owner,
        ))
    }
    pub fn progress(&self) -> ReplicaProgress {
        self.progress.borrow().value.clone()
    }
    /// Reads the replica's progress where it is, without copying it: what
    /// a view over every hosted session reads each round (the audit's F26).
    /// The progress is held for `read`'s length only, so `read` asks
    /// nothing of the replica.
    pub fn observe<R>(&self, read: impl FnOnce(&ReplicaProgress) -> R) -> R {
        read(&self.progress.borrow().value)
    }
    /// Whether the owner's tick period in force is stretched past the
    /// configured one, for a far or slow group (27 §3.1 P2).
    pub fn stretched(&self) -> bool {
        self.tick_period() > self.tick
    }
    /// Derive this replica's tick period from the measured paths to its
    /// group's other voters (27 §3.1 P2), in force from its next tick.
    pub fn pace<'a>(
        &self,
        paths: impl IntoIterator<Item = &'a focal_timing::PathRtt>,
    ) -> focal_timing::TickPace {
        let pace = focal_timing::TickPace::derive(
            self.tick,
            self.tick_ceiling,
            self.pace.election_tick().max(1),
            paths,
        );
        self.pace.publish(pace);
        pace
    }
    /// How late the session's commits are answered among its voters, from
    /// how late each other voter answers its messages
    /// (`PeerConnectionPool::replication_lateness`) and how many voters there
    /// are, this node among them: its leader's quorum patience and what a
    /// request is given beyond its time (27 §8.4).
    pub fn quorum(&self, tails: Vec<std::time::Duration>, voters: usize) {
        self.pace
            .publish_quorum_tail(crate::pace::quorum_tail(tails, voters));
    }
    /// The pace in force: the last derivation, or the configured period
    /// with no samples before any.
    pub fn current_pace(&self) -> focal_timing::TickPace {
        self.pace
            .derived()
            .unwrap_or_else(|| focal_timing::TickPace::floor(self.tick))
    }
    /// The periods this replica's owner has run; what a wait on it is
    /// charged in (27 §3.1 P8).
    /// The periods in which the replica was not ticked: refused the room,
    /// or still persisting.
    pub fn refused_periods(&self) -> u64 {
        self.pace.refused()
    }
    /// What the session turned away for room (`InputRefusals`).
    pub(crate) fn input_refusals(&self) -> crate::pace::InputRefusals {
        self.pace.input_refusals()
    }
    /// Periods in one election timeout of this replica.
    pub fn election_periods(&self) -> u64 {
        u64::try_from(self.pace.election_tick()).unwrap_or(u64::MAX)
    }
    pub fn periods(&self) -> u64 {
        self.pace.periods()
    }
    /// The longest a period of the owner took, from one to the next
    /// (`ControlHost::longest_period`).
    pub fn longest_period(&self) -> Duration {
        self.pace.longest()
    }
    pub fn tick_period(&self) -> Duration {
        self.pace.get(self.tick, self.tick_ceiling)
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
    /// Admit replication from the members the committed directory names
    /// for this session (its active voters, the voters of a pending plan and
    /// the copies being retired). A replica's own applied configuration
    /// names its peers once it has applied it; a copy that has applied
    /// nothing knows only the configuration its log began with, and would
    /// refuse a leader outside it. Replaces what was admitted before.
    pub async fn admit_members(&self, members: Vec<u64>) -> Result<(), LedgerError> {
        self.admit(members, Near::default()).await
    }
    /// [`Self::admit_members`], with the voters the directory places in the
    /// zone of the session's preferred leader: they outrank the other
    /// voters in an election and lead in the preferred leader's place
    /// (27 §5). Replaces what was said before.
    pub async fn admit(&self, mut members: Vec<u64>, mut near: Near) -> Result<(), LedgerError> {
        members.sort_unstable();
        members.dedup();
        near.voters.sort_unstable();
        near.voters.dedup();
        if members.len() > MAX_ADMITTED
            || members.first() == Some(&0)
            || near.voters.len() > MAX_ADMITTED
            || near.voters.first() == Some(&0)
            || near.voters.binary_search(&near.leader).is_ok()
        {
            return Err(LedgerError::Capacity);
        }
        let charge = self
            .budget
            .reserve(BudgetKind::Control, BudgetLane::Completion, 64 * 1024)?
            .commit();
        let (send, receive) = oneshot::channel();
        self.sender
            .try_send(Work::Admit(members, near, send, charge))
            .map_err(|error| match error {
                HostQueueError::Full => LedgerError::Capacity,
                HostQueueError::Disconnected => LedgerError::Failed,
            })?;
        receive.await.map_err(|_| LedgerError::OutcomeUnknown)?
    }
    /// What the paths to this replica's peers hold in flight — each peer's
    /// transport window, in bytes — and their round trips, as the node
    /// measures them: `(peer, window bytes, round trip ns)` (27 §11). While
    /// the replica leads, a peer is sent no more of entries ahead of its
    /// answers than follows from them (`inflight_bytes`). A hint,
    /// said again every round of the node's pacer: one the owner has no
    /// room for is dropped, and a peer never said keeps one page.
    pub fn inflight_windows(&self, mut windows: Vec<(u64, u64, u64)>) {
        windows.sort_unstable();
        windows.dedup_by_key(|(peer, _, _)| *peer);
        if windows.is_empty()
            || windows.len() > MAX_ADMITTED
            || windows.first().is_some_and(|(peer, _, _)| *peer == 0)
        {
            return;
        }
        let Ok(charge) =
            self.budget
                .reserve(BudgetKind::Control, BudgetLane::Completion, 64 * 1024)
        else {
            return;
        };
        let _ = self
            .sender
            .try_send(Work::Windows(windows, charge.commit()));
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
    fn supports_ordered_replication(&self) -> bool {
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
        let replication = matches!(
            request.request().operation,
            Operation::Raft { .. } | Operation::RaftOrdered { .. }
        );
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
            .and_then(|n| n.checked_add(4096))
            // A frame's place on its way to a fleet's owner, apart from the
            // participants' queue, which that queue's backing does not hold.
            .and_then(|n| {
                n.checked_add(if replication {
                    grouped::FRAME_QUEUE_BYTES
                } else {
                    0
                })
            });
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
            self.pace.refuse_input(replication);
            return OwnedResponse::new(full);
        };
        let (send, receive) = oneshot::channel();
        let work = Work::Request(
            Box::new(AdmittedRequest {
                verified: request,
                witness,
                native,
            }),
            send,
            charge.commit(),
        );
        let sent = if replication {
            self.sender.try_send_frame(work, &self.pace)
        } else {
            self.sender.try_send(work)
        };
        match sent {
            Ok(()) => receive
                .await
                .unwrap_or_else(|_| OwnedResponse::new(unknown)),
            Err(HostQueueError::Full) => {
                self.pace.refuse_input(replication);
                OwnedResponse::new(full)
            }
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
    /// The interval of a leader's heartbeats at the configured period.
    fn beat_interval(&self) -> Duration {
        self.config
            .tick
            .saturating_mul(u32::try_from(self.session.heartbeat_tick().max(1)).unwrap_or(u32::MAX))
    }
    /// Whether this owner beats apart from its ticks: it leads, and its
    /// period is stretched. A stretched period stretches the election
    /// timeout, which is what it is for; the heartbeats of a leader keep
    /// the cadence its followers were configured to expect. Each node
    /// stretches by what it measured itself, and a leader that beat at its
    /// own stretched period would be presumed dead by a follower that
    /// measured less (27 §3.1 P2).
    fn beats(&self) -> bool {
        self.pace
            .stretched(self.config.tick, self.config.tick_ceiling)
            && self.session.scalars().role == StateRole::Leader
    }
    fn beat_if_due(&mut self) -> Result<(), LedgerError> {
        if !self.beats() || Instant::now() < self.next_beat {
            return Ok(());
        }
        match self.session.beat() {
            Ok(())
            | Err(LedgerError::Consensus(
                focal_consensus::ConsensusError::PersistencePending
                | focal_consensus::ConsensusError::Capacity,
            )) => {}
            Err(error) => return Err(error),
        }
        self.next_beat = Instant::now()
            .checked_add(self.beat_interval())
            .ok_or(LedgerError::Failed)?;
        Ok(())
    }
    fn run(mut self, receiver: mpsc::Receiver<Work>) {
        let mut next_tick = Instant::now();
        let result = (|| -> Result<(), LedgerError> {
            self.drain()?;
            loop {
                if Instant::now() >= next_tick {
                    let started = Instant::now();
                    self.tick()?;
                    self.slow.note("tick", started);
                    next_tick = Instant::now()
                        .checked_add(self.pace.get(self.config.tick, self.config.tick_ceiling))
                        .ok_or(LedgerError::Failed)?;
                }
                self.beat_if_due()?;
                if self.session.has_ready() {
                    let started = Instant::now();
                    self.drain()?;
                    self.slow.note("drain", started);
                }
                let wake = if self.beats() {
                    next_tick.min(self.next_beat)
                } else {
                    next_tick
                };
                match receiver.recv_timeout(wake.saturating_duration_since(Instant::now())) {
                    Ok(work) => {
                        let started = Instant::now();
                        let what = work.kind();
                        if self.take(work, &receiver)? {
                            return Ok(());
                        }
                        self.slow.note(what, started);
                        let started = Instant::now();
                        self.progress_managed()?;
                        self.slow.note("managed progress", started);
                        let started = Instant::now();
                        self.checkpoint_for_members()?;
                        self.slow.note("member checkpoint", started);
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
    /// Takes `work` and what is queued behind it, as a batch, before the
    /// drain that writes it (27 §9): the proposals among it go in one write
    /// and the reads in one round. Counted by what the owner admits at once.
    /// Whether the owner is to stop.
    fn take(&mut self, work: Work, receiver: &mpsc::Receiver<Work>) -> Result<bool, LedgerError> {
        self.batching = true;
        let stop = self.take_batch(work, receiver);
        self.batching = false;
        if self.drain_owed || self.session.has_ready() {
            self.drain()?;
        }
        stop
    }
    fn take_batch(
        &mut self,
        work: Work,
        receiver: &mpsc::Receiver<Work>,
    ) -> Result<bool, LedgerError> {
        if self.accept(work)? {
            return Ok(true);
        }
        let mut taken = 1usize;
        while taken < self.config.pending_clients {
            let Ok(work) = receiver.try_recv() else {
                break;
            };
            taken = taken.saturating_add(1);
            if self.accept(work)? {
                return Ok(true);
            }
        }
        Ok(false)
    }
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
        // A checkpoint whose seeds were being made durable off this thread is
        // handed to consensus as soon as they are.
        retryable(self.session.poll_deferred_checkpoint())?;
        if !self.checkpoint_due()? {
            return Ok(());
        }
        self.try_checkpoint().map(|_| ())
    }
    /// Whether the log is due to be checkpointed: past the entry floor, once
    /// the applied log outweighs the last image times the expansion, takes
    /// its share of the session's memory, or holds the most entries a restart
    /// replays.
    fn checkpoint_due(&self) -> Result<bool, LedgerError> {
        let entries = self.log_entries_since_checkpoint();
        if entries < self.config.checkpoint_after_entries {
            return Ok(false);
        }
        // A checkpoint that rewrites the log it keeps (focal-log's) costs the
        // owner the log as well as the state: a longer log between images
        // makes each rewrite, on the owner's thread, longer (265 ms at 22k
        // entries on the Linux comparison). It keeps the floor alone.
        let expansion = if self.session.checkpoint_rewrites_log() {
            0
        } else {
            self.config.checkpoint_expansion
        };
        Ok(checkpoint_due(
            CheckpointLog {
                entries,
                held: self.session.applied_log_bytes()?,
            },
            self.session.checkpoint_image_bytes(),
            u64::try_from(self.session.memory_stats().limit).unwrap_or(u64::MAX),
            expansion,
        ))
    }
    /// Checkpoint now unless the replica cannot yet: a resource condition or
    /// unpersisted state waits for a later tick, and nothing is a failure.
    /// A proposal in flight does not hold it back: the checkpoint is of the
    /// applied prefix, below every proposal (`Session::checkpoint` refuses
    /// what does change that prefix, and its refusal waits here as any).
    fn try_checkpoint(&mut self) -> Result<bool, LedgerError> {
        if self.session.persistence_pending()
            || self.session.checkpoint_in_flight()
            || self.session.deferred_checkpoint_pending()
            || self.stopping.is_some()
        {
            return Ok(false);
        }
        // The owner encodes the state; its seeds are made durable off this
        // thread, so the replica's work never waits for their syncs.
        match self.session.begin_deferred_checkpoint() {
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
    /// A member added after the log was compacted can only be seeded by a
    /// snapshot whose configuration names it — Raft discards any other — so
    /// a replica whose stored snapshot does not name every member of the
    /// configuration it has applied checkpoints. Derived from what the
    /// replica has applied: whichever replica leads when the member asks to
    /// be seeded has refreshed its own snapshot, however leadership moved or
    /// the owner restarted since the change. A flag kept by the owner that
    /// resolved the change alone left a drained leader's replacement
    /// unseeded for good: leadership moved off that owner before it
    /// checkpointed, and the leader that followed sent the replacement a
    /// snapshot that did not name it at every probe (macOS CI, 27b0531).
    /// A change that only promotes or removes asks for nothing; what is
    /// pending or persisting waits for a later period. The answer is kept
    /// for the configuration and the snapshot it was found at, so a period
    /// reads two indexes.
    fn checkpoint_for_members(&mut self) -> Result<(), LedgerError> {
        let at = (
            self.session.configuration_index(),
            self.session.snapshot_index(),
        );
        if at.1 >= at.0 {
            return Ok(());
        }
        let named = match self.members_named {
            Some((seen, named)) if seen == at => named,
            _ => {
                let named = self.session.snapshot_names_every_member();
                self.members_named = Some((at, named));
                named
            }
        };
        if named {
            return Ok(());
        }
        self.try_checkpoint().map(|_| ())
    }
    fn tick(&mut self) -> Result<(), LedgerError> {
        self.pace
            .advance(self.pace.get(self.config.tick, self.config.tick_ceiling));
        self.expire_held();
        // What the owner has seen of its own stalls is the replica's
        // patience before it campaigns (`ControlHost`).
        self.session.set_patience(
            self.pace
                .patience(self.config.tick, self.config.tick_ceiling),
        )?;
        // And what its voters' answers take beyond its own periods is its
        // patience before it asks whether a quorum heard it (27 §8.4).
        self.session.set_quorum_patience(
            self.pace
                .quorum_ticks(self.config.tick, self.config.tick_ceiling),
        )?;
        // The committed placement names the session's preferred leader (27
        // §5): it outranks the other voters in an election among equally
        // current logs, so leadership returns to where placement put it and
        // a voter that merely timed out first does not take it. Policy from
        // committed state, applied by the owner; never from liveness.
        let priority = self.rank(self.session.scalars().node_id);
        if self.session.priority() != priority {
            self.session.set_priority(priority)?;
        }
        // A tick that was refused the room, or that came while the one
        // before it is still persisted, changed nothing: the period has
        // passed without it (27 §3.1 P3), and the replica goes on.
        let started = Instant::now();
        let ticked = self.session.tick();
        self.slow.note("tick: consensus", started);
        match ticked {
            Ok(()) => {}
            Err(
                LedgerError::Capacity
                | LedgerError::Consensus(
                    focal_consensus::ConsensusError::Capacity
                    | focal_consensus::ConsensusError::PersistencePending,
                ),
            ) => self.pace.refuse(),
            Err(error) => return Err(error),
        }
        self.return_leadership()?;
        if self.session.scalars().role == StateRole::Leader {
            let now = wall_ms()?.max(self.session.cursor_clock());
            match self.session.propose_cursor_clock(now) {
                Ok(_) | Err(LedgerError::Capacity | LedgerError::NotReady { .. }) => {}
                Err(error) => return Err(error),
            }
            // Trusted native timers fire from the leader's clock; a deferred
            // or refused timer waits for a later tick or its primary row.
            let started = Instant::now();
            let swept = crate::native_timers::sweep(&mut self.session);
            self.slow.note("tick: timers", started);
            match swept {
                Ok(_) | Err(LedgerError::Capacity | LedgerError::NotReady { .. }) => {}
                Err(error) => return Err(error),
            }
        }
        let started = Instant::now();
        self.views
            .advance(&mut self.session)
            .map_err(|_| LedgerError::Failed)?;
        self.slow.note("tick: read views", started);
        let started = Instant::now();
        self.drain()?;
        self.slow.note("tick: drain", started);
        let started = Instant::now();
        self.checkpoint_by_cadence()?;
        self.slow.note("tick: checkpoint", started);
        // At every period, not only beside work: a replica no request
        // reaches still seeds the members its configuration added.
        self.checkpoint_for_members()?;
        self.progress_managed()
    }
    /// The election priority of `node` by the session's committed
    /// placement: its preferred leader first, then the voters the
    /// directory places in that leader's zone, then the rest.
    fn rank(&self, node: u64) -> i64 {
        match self.session.active_placement() {
            Some(spec) if spec.placement.preferred_leader == node => PREFERRED_LEADER_PRIORITY,
            Some(spec)
                if spec.placement.preferred_leader == self.near.leader
                    && spec.placement.voters.contains_key(&node)
                    && self.near.voters.binary_search(&node).is_ok() =>
            {
                ZONE_PRIORITY
            }
            _ => VOTER_PRIORITY,
        }
    }
    /// Hands leadership to a voter the placement ranks above this replica
    /// when this replica leads in its place and that member has stayed
    /// current (`leader_return`, 27 §5): to the preferred leader, and while
    /// that one is not there to take it, to a voter in its zone. A refusal
    /// is counted and rested on; only a failure of the session itself is
    /// an error.
    fn return_leadership(&mut self) -> Result<(), LedgerError> {
        let status = self.session.scalars();
        let membership = self.session.members();
        let leads = self.session.is_authoritative();
        let own = self.rank(status.node_id);
        let votes = |node: u64| {
            node != status.node_id
                && membership.voters.contains(&node)
                && self
                    .session
                    .active_placement()
                    .is_some_and(|spec| spec.placement.voters.contains_key(&node))
        };
        // Fit to lead: caught up to what is committed and heard from, and
        // not being sent a snapshot. Not the pipeline's state: a member the
        // leader lost a message to is probed (27 §3.3) until its log moves,
        // which a log with nothing proposed never does.
        let fit = |node: u64| {
            self.session.peer(node).is_some_and(|peer| {
                peer.state != focal_consensus::PEER_SNAPSHOT
                    && peer.matched >= status.committed_index
                    && peer.recent_active
            })
        };
        let preferred = self
            .session
            .active_placement()
            .map(|spec| spec.placement.preferred_leader)
            .filter(|node| votes(*node));
        // The preferred leader, unless it is not there to take it and a
        // voter of its zone is. Counted for the preferred leader where no
        // one is, so that it is asked once it has come back and stayed.
        let target = if !leads {
            preferred
        } else {
            match preferred {
                Some(node) if fit(node) => Some(node),
                _ if own < ZONE_PRIORITY => self
                    .near
                    .voters
                    .iter()
                    .copied()
                    .find(|node| votes(*node) && self.rank(*node) > own && fit(*node))
                    .or(preferred),
                preferred => preferred,
            }
        };
        let peer = match (leads, target) {
            (true, Some(node)) => self.session.peer(node),
            _ => None,
        };
        let seen = crate::leader_return::Seen {
            leads,
            preferred: target,
            current: peer.is_some_and(|peer| {
                peer.state != focal_consensus::PEER_SNAPSHOT
                    && peer.matched >= status.committed_index
            }),
            heard: peer.is_some_and(|peer| peer.recent_active),
            transferring: self.session.transferring().is_some(),
            settled: self.stopping.is_none()
                && self.memberships.is_empty()
                && self.placement.is_none()
                && !self.session.configuration_pending(),
            quiet: self.session.pending_count() == 0,
        };
        let crate::leader_return::Verdict::Ask(target) = self.leader_return.observe(seen) else {
            return Ok(());
        };
        match self.session.transfer_leader(target) {
            Ok(()) => Ok(()),
            Err(
                error @ (LedgerError::Failed
                | LedgerError::Consensus(focal_consensus::ConsensusError::Failed)),
            ) => Err(error),
            Err(_) => {
                self.leader_return.refused();
                Ok(())
            }
        }
    }
    /// Shared-worker progress never waits on a disk receipt. The exact Ready
    /// remains inside Session until its WAL owner reports a completed fence.
    /// What the driver could not reach since the last period, told to the
    /// core: it probes those members instead of streaming to them. The
    /// reports are gathered first, each peer once (a peer lost more often
    /// is coalesced; more peers than the bound are dropped, both counted),
    /// then told while the core can be told: one fenced by a write it still
    /// persists, or short of the room, hears the rest next period, the
    /// peers keeping their place. A report is a hint about a peer, never a
    /// reason for the owner to end.
    fn report_lost(&mut self) -> Result<(), LedgerError> {
        for _ in 0..LOST_PEERS {
            let Ok(peer) = self.lost.try_recv() else {
                break;
            };
            match self.lost_peers.binary_search(&peer) {
                Ok(_) => self.lost_coalesced = self.lost_coalesced.saturating_add(1),
                Err(at)
                    if self.lost_peers.len() < LOST_PEERS
                        && self.lost_peers.try_reserve(1).is_ok() =>
                {
                    self.lost_peers.insert(at, peer);
                }
                Err(_) => self.lost_dropped = self.lost_dropped.saturating_add(1),
            }
        }
        while let Some(&peer) = self.lost_peers.last() {
            match self.session.report_unreachable(peer) {
                Ok(()) => {
                    self.lost_peers.pop();
                    self.unreachable = self.unreachable.saturating_add(1);
                }
                Err(
                    LedgerError::Consensus(
                        focal_consensus::ConsensusError::PersistencePending
                        | focal_consensus::ConsensusError::Capacity,
                    )
                    | LedgerError::Capacity
                    | LedgerError::Memory(_),
                ) => return Ok(()),
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
    fn progress_group(&mut self) -> Result<bool, LedgerError> {
        // The room its peers' frames have, as its configuration is now: at
        // least once a period, and so within one of a change.
        self.pace.set_frame_room(self.peer_reserve());
        self.report_lost()?;
        self.views
            .advance(&mut self.session)
            .map_err(|_| LedgerError::Failed)?;
        self.expire_pending();
        self.progress_evidence()?;
        if self.step_waiting() {
            self.drain_owed = true;
        }
        if self.drain_owed || self.session.has_ready() {
            self.drain_with_runtime(self.stopping.is_none())?;
        }
        self.progress_evidence()?;
        if !self.session.persistence_pending() {
            self.poll_snapshot_feedback()?;
        }
        self.progress_managed()?;
        self.checkpoint_for_members()?;
        let handing_off = self.handing_off();
        if let Some((_, deadline)) = self.stopping.as_ref()
            && !handing_off
        {
            // A stop ticks the replica no more, but its time passes in the
            // owner's periods all the same: each is counted, refused.
            if Instant::now() >= self.next_tick {
                self.pace
                    .advance(self.pace.get(self.config.tick, self.config.tick_ceiling));
                self.pace.refuse();
                self.next_tick = Instant::now()
                    .checked_add(self.pace.get(self.config.tick, self.config.tick_ceiling))
                    .ok_or(LedgerError::Failed)?;
            }
            let expired = self.pace.periods() >= *deadline;
            if expired && self.session.has_ready() {
                // What the stop made of its hand-off is published before
                // the reply: the fleet reads it once the reply is in.
                self.publish_progress(true);
                if let Some((response, _)) = self.stopping.take() {
                    let _ = response.send(Err(LedgerError::OutcomeUnknown));
                }
                return Ok(true);
            }
            if !self.session.has_ready() {
                self.publish_progress(true);
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
        if Instant::now() >= self.next_tick {
            if self.session.persistence_pending() {
                // The period passed without a tick: the writer still
                // persists what the tick before left (27 §3.1 P3). It is
                // counted, refused, so that what waits its time in the
                // owner's periods waits no longer than it would have.
                self.pace
                    .advance(self.pace.get(self.config.tick, self.config.tick_ceiling));
                self.pace.refuse();
                self.expire_held();
            } else {
                self.tick()?;
            }
            self.next_tick = Instant::now()
                .checked_add(self.pace.get(self.config.tick, self.config.tick_ceiling))
                .ok_or(LedgerError::Failed)?;
        }
        if !self.session.persistence_pending() {
            self.beat_if_due()?;
        }
        Ok(false)
    }
    fn group_deadline(&self) -> Result<Instant, LedgerError> {
        if self.stopping.is_some() || self.evidence.is_some() {
            return Instant::now()
                .checked_add(Duration::from_millis(1))
                .ok_or(LedgerError::Failed);
        }
        if self.session.persistence_pending() {
            // The log tells this owner when what the replica waits for is
            // answered (`Session::wakes_owner`, 27 §9), and the owner drains
            // the session then: nothing is asked at intervals, and the tick
            // is the bound on a signal that was lost. A write the log had no
            // room for tells no one: it is asked again as the log answers
            // the owner's other sessions, which is when room is made
            // (`GroupOwner::signalled`), and at the tick.
            return Ok(self.next_tick);
        }
        // A delivery waiting for seed chunks its host has not pulled yet
        // makes no progress on its own; it resumes when a chunk lands. The
        // drain a batch owes is due at once either way.
        if self.drain_owed || (self.session.has_ready() && !self.session.seed_waiting()) {
            return Ok(self.next_tick.min(Instant::now()));
        }
        if self.beats() {
            return Ok(self.next_tick.min(self.next_beat));
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
                let waiting = self.session.reads_waiting();
                self.request(
                    request.verified,
                    response,
                    charge,
                    request.witness,
                    request.native,
                );
                // A request that only asked a read is not drained for here:
                // the owner's loop takes what else is queued first, and the
                // round that leaves with its drain carries every read asked
                // by then (27 §9). Reads queued together are confirmed by
                // one round of heartbeats, and one asked alone leaves with
                // the drain that follows at once. Nor is a proposal, in a
                // batch: the batch's drain writes every proposal of it at
                // once. A proposal drained for as it came started its write
                // there, and a session with a write out is dispatched
                // nothing, so each write carried one proposal — a group
                // committing six entries a second held 28 requests queued
                // behind it (six fleets at once, 2026-10-03). The batch owes
                // its drain all the same (`Owner::drain_owed`).
                if self.batching {
                    self.drain_owed = true;
                } else if self.session.reads_waiting() <= waiting || !self.session.reads_unasked() {
                    self.drain()?;
                }
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
                                leader: self.session.scalars().leader_id,
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
            Work::Admit(members, near, response, charge) => {
                self.admitted = members;
                self.near = near;
                self.pace.set_frame_room(self.peer_reserve());
                self.publish_progress(false);
                drop(charge);
                let _ = response.send(Ok(()));
            }
            Work::Windows(windows, charge) => {
                let beat = self.beat_interval();
                let page = self.session.page_bytes();
                for (peer, window, round_trip_ns) in windows {
                    // A peer the configuration does not name is told of
                    // nothing; a replica that failed refuses, and stops.
                    self.session.set_inflight_bytes(
                        peer,
                        inflight_bytes(window, round_trip_ns, beat, page),
                    )?;
                }
                drop(charge);
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
                // A leader asks its heir to campaign first: the drain sends
                // that, and the group is led again without an election
                // timeout even though this owner does not stay for it.
                let result = (|| {
                    self.hand_off()?;
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
        let deadline = self.request_deadline().ok_or(LedgerError::Capacity)?;
        self.hand_off()?;
        self.stopping = Some((response, deadline));
        Ok(())
    }
    /// A leader hands its log off before it goes (27 §5): the most
    /// caught-up voter is asked to campaign now, and until the log leads
    /// elsewhere — or the transfer's own bound, one election timeout, has
    /// passed in this owner's periods — the replica ticks and beats as a
    /// leader does, so the heir is caught up and asked. A leader that went
    /// silent cost the survivors that whole timeout, for every log it led.
    fn hand_off(&mut self) -> Result<(), LedgerError> {
        // Once a stop: the owned status, which the heir is chosen from.
        let status = self.session.status();
        if status.role != StateRole::Leader {
            return Ok(());
        }
        let peers: Vec<focal_consensus::PeerProgress> = status
            .voters
            .iter()
            .filter_map(|voter| self.session.peer(*voter))
            .collect();
        // The placement's preferred leader takes it when it qualifies:
        // leadership would return there anyway.
        let preferred = self
            .session
            .active_placement()
            .map(|spec| spec.placement.preferred_leader)
            .filter(|node| {
                peers
                    .iter()
                    .any(|peer| peer.node == *node && peer.state == 1 && peer.recent_active)
                    && *node != status.node_id
            });
        let Some(heir) = preferred.or_else(|| focal_control::heir(&status, &peers)) else {
            return Ok(());
        };
        match self.session.transfer_leader(heir) {
            Ok(()) => {
                let timeout = u64::try_from(self.session.election_tick()).unwrap_or(u64::MAX);
                self.handing_off = self.pace.periods().checked_add(timeout);
                self.stop_hand_off = Some(StopHandOff {
                    heir,
                    completed: false,
                });
                Ok(())
            }
            Err(
                error @ (LedgerError::Failed
                | LedgerError::Consensus(focal_consensus::ConsensusError::Failed)),
            ) => Err(error),
            // Refused (a transfer already under way, a member that is not a
            // voter after all): the stop goes on as it did.
            Err(_) => Ok(()),
        }
    }
    /// Whether a stopping leader is still handing its log off; the hand-off
    /// is complete once the log leads elsewhere before its bound.
    fn handing_off(&mut self) -> bool {
        let Some(bound) = self.handing_off else {
            return false;
        };
        let leads = self.session.scalars().role == StateRole::Leader;
        if leads && self.pace.periods() < bound {
            return true;
        }
        if !leads && let Some(hand_off) = self.stop_hand_off.as_mut() {
            hand_off.completed = true;
        }
        self.handing_off = None;
        false
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
        while let Some(waiting) = self.waiting_frames.pop_front() {
            waiting
                .frame
                .pending
                .finish(Response::Error(AccessError::OutcomeUnknown));
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
                role: status.role,
                sequence: self.session.sequence(),
                dropped_replication: self.dropped,
                peers_unreachable: self.unreachable,
                frames_held: self.frames_held,
                frames_let_go: self.frames_let_go,
                frames_stale: self.frames_stale,
                frames_waited: self.frames_waited,
                appends_rejected: self.appends_rejected,
                appends_rejected_in_order: self.appends_rejected_in_order,
                peer_reports_coalesced: self.lost_coalesced,
                peer_reports_dropped: self.lost_dropped,
                waits_asked: self.waits_asked,
                waits_answered: self.waits_answered,
                waits_swept: self.waits_swept,
                stopped,
                voters: status.voters.clone(),
                admitted: self.admitted.clone(),
                priority: self.session.priority(),
                near: self.near.clone(),
                returns: self.leader_return.stats(),
                stop_hand_off: self.stop_hand_off,
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
    /// Pending Raft acknowledgments: each an authenticated peer's message
    /// stepped, its reply behind the exact Ready fence.
    fn pending_peers(&self) -> usize {
        self.pending
            .iter()
            .filter(|pending| matches!(pending.waiting, WaitingFor::PeerPersistence))
            .count()
            .saturating_add(self.resequencer.held())
            .saturating_add(self.waiting_frames.len())
    }
    /// Pending participant requests: the queue less the peers'.
    fn pending_participants(&self) -> usize {
        self.pending.len().saturating_sub(self.pending_peers())
    }
    /// The Raft traffic admitted beside the participants' bound (F56): every
    /// member the configuration names — voters, learners and the admitted —
    /// may have its whole in-flight window outstanding at once, the window
    /// the core itself allows a peer (`NodeConfig::max_inflight_messages`).
    /// Participants never take these slots and peers never take theirs, so
    /// admitted participant work cannot refuse the acknowledgments its own
    /// completion waits on, and peers cannot crowd the participants out.
    fn peer_reserve(&self) -> usize {
        let members = self
            .session
            .members()
            .len()
            .saturating_add(self.admitted.len())
            .max(1);
        members.saturating_mul(self.session.inflight_window())
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
    /// Frames held past their patience go, in their order; the lanes of
    /// members that left are closed and what they held refused; the order
    /// kept for departed peers is forgotten (27 §12). At every period the
    /// owner runs, since a frame's patience is counted in periods: a
    /// replica's own owner let held frames go only beside a group's
    /// progress, which it never makes, so a follower that lost one ordered
    /// frame held every one after it for ever (the evidence scenario's copy,
    /// 2026-10-03).
    fn expire_held(&mut self) {
        if self.resequencer.expire(self.pace.periods()).is_ok() {
            self.step_due();
        }
        let membership = self.session.members();
        let admitted = &self.admitted;
        let member =
            |source: u64| membership.holds(source) || admitted.binary_search(&source).is_ok();
        let mut gone = Vec::new();
        if self.resequencer.prune(&member, &mut gone).is_ok() {
            for held in gone {
                held.pending
                    .finish(Response::Error(AccessError::Unauthorized));
            }
        }
        let count = self.waiting_frames.len();
        for _ in 0..count {
            let Some(waiting) = self.waiting_frames.pop_front() else {
                break;
            };
            if member(waiting.frame.source) {
                self.waiting_frames.push_back(waiting);
            } else {
                waiting
                    .frame
                    .pending
                    .finish(Response::Error(AccessError::Unauthorized));
            }
        }
        self.ordered.retain(|peer, _| member(*peer));
        self.append_streams.retain(|peer, _| member(*peer));
    }
    /// Step the frames the resequencer let go, in their order. Each is a
    /// loss, noted once the frame is stepped (`Owner::lost_to`): a frame let
    /// go that the log takes — what a full lane let go behind one that
    /// came — moves the hole past its entries.
    fn step_due(&mut self) {
        while let Some(held) = self.resequencer.take_due() {
            self.frames_let_go = self.frames_let_go.saturating_add(1);
            self.step_or_wait(WaitingFrame {
                frame: held,
                ordered: true,
                lost: true,
            });
        }
    }
    /// Step what was held behind the frame from `source` just stepped.
    fn step_ready(&mut self, source: u64) {
        while let Some(held) = self.resequencer.step_ready(source) {
            self.step_or_wait(WaitingFrame {
                frame: held,
                ordered: true,
                lost: false,
            });
        }
    }
    /// Whether a frame due now waits: the replica's write is in flight, or
    /// frames that came before it wait already and it may not pass them.
    fn must_wait(&self) -> bool {
        self.session.persistence_pending() || !self.waiting_frames.is_empty()
    }
    /// Step a frame now, or keep it, in its order, for the replica's write
    /// in flight to be durable (`WaitingFrame`).
    fn step_or_wait(&mut self, frame: WaitingFrame) {
        if self.must_wait() {
            self.frames_waited = self.frames_waited.saturating_add(1);
            self.waiting_frames.push_back(frame);
        } else {
            self.step_frame(frame);
        }
    }
    /// Step the frames that waited for the replica's write, in the order
    /// they came, now that consensus takes input again; answered at the
    /// Ready fence of the drain that follows, as every peer frame is.
    /// Whether any was stepped.
    fn step_waiting(&mut self) -> bool {
        let mut stepped = false;
        while !self.session.persistence_pending() {
            let Some(frame) = self.waiting_frames.pop_front() else {
                break;
            };
            self.step_frame(frame);
            stepped = true;
        }
        stepped
    }
    /// Admit a peer's frame: authorized, within the peers' reserve, and
    /// stepped — now, with what was held behind it, or held itself for
    /// the frame it overtook (27 §12).
    fn admit_replication(
        &mut self,
        verified: &VerifiedRequest,
        group: [u8; 16],
        order: Option<(u64, u64)>,
    ) -> Result<Replication, AccessError> {
        let request = verified.request();
        let deadline = self.request_deadline().ok_or(AccessError::Unavailable)?;
        if request.ledger != self.session.ledger() {
            return Err(AccessError::Unauthorized);
        }
        let PeerRole::Node { node_id } = verified.peer().role() else {
            return Err(AccessError::Unauthorized);
        };
        if group != self.session.group_id()
            || (!self.session.members().holds(node_id)
                && self.admitted.binary_search(&node_id).is_err())
        {
            return Err(AccessError::Unauthorized);
        }
        // Raft traffic is admitted beside the participants (F56): a full
        // participant queue never refuses the acknowledgments its own
        // completion waits on.
        if self.pending_peers() >= self.peer_reserve() {
            return Err(AccessError::Capacity);
        }
        let message = match &request.operation {
            Operation::Raft { message, .. } | Operation::RaftOrdered { message, .. } => message,
            _ => return Err(AccessError::InvalidRequest),
        };
        // A bulk frame is stepped in the order it left its sender: one
        // that overtook the frame before it is held for it, for its
        // patience at most.
        let mut late = false;
        if let Some((epoch, sequence)) = order {
            match self.resequencer.admit(node_id, epoch, sequence) {
                Err(crate::resequence::Capacity) => return Err(AccessError::Capacity),
                Ok(crate::resequence::Admission::Hold) => {
                    self.frames_held = self.frames_held.saturating_add(1);
                    self.sent_ordered(node_id);
                    return Ok(Replication::Held {
                        source: node_id,
                        sequence,
                        until: self.patience_until(verified.path_round_trip()),
                        deadline,
                    });
                }
                Ok(crate::resequence::Admission::Stale) => {
                    self.frames_stale = self.frames_stale.saturating_add(1);
                    late = true;
                }
                Ok(crate::resequence::Admission::Step) => {}
            }
            self.sent_ordered(node_id);
            self.step_due();
        }
        // Consensus takes no input while the replica's write is in flight,
        // and a frame stepped then was refused and lost to its peer: it
        // waits for the write instead, behind any that wait already.
        if self.must_wait() {
            return Ok(Replication::Waiting {
                source: node_id,
                ordered: order.is_some(),
                lost: late,
                deadline,
            });
        }
        if let Err(error) = self.session.step_authenticated(node_id, message) {
            // An ordered frame not stepped is lost to this log as much as
            // one that never came: the appends behind it are refused.
            if order.is_some() {
                self.lost_to(node_id);
            }
            return Err(access(error));
        }
        if late {
            // A frame found stale is a loss, noted once it is stepped: one
            // the log takes carries the hole past its entries, and what is
            // refused behind the hole is refused at the log's new end (CI on
            // 8d4f322: a follower took two late frames after noting their
            // loss, and fifty refusals behind them were judged the order's).
            self.lost_to(node_id);
        }
        if order.is_some() {
            self.step_ready(node_id);
        }
        // The ingress acknowledgment and the generated Raft messages remain
        // behind the exact Ready fence, including async writes.
        Ok(Replication::Stepped(deadline))
    }
    /// Step a frame that was held or waited: answered at the Ready fence
    /// as every peer frame is, or refused as its step was. A loss is noted
    /// once the frame is stepped (`Owner::lost_to`); an ordered frame not
    /// stepped is one.
    fn step_frame(&mut self, frame: WaitingFrame) {
        let WaitingFrame {
            frame:
                HeldFrame {
                    source,
                    message,
                    mut pending,
                },
            ordered,
            lost,
        } = frame;
        match self.session.step_authenticated(source, &message) {
            Ok(()) => {
                pending.waiting = WaitingFor::PeerPersistence;
                self.pending.push_back(pending);
                if lost {
                    self.lost_to(source);
                }
            }
            Err(error) => {
                if ordered {
                    self.lost_to(source);
                }
                pending.finish(Response::Error(access(error)));
            }
        }
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
        // A peer's frame has a path of its own: stepped now, or held for
        // the frame it overtook (27 §12), and answered at the Ready fence
        // either way.
        let replication = match &verified.request().operation {
            Operation::Raft { group, .. } => Some((*group, None)),
            Operation::RaftOrdered {
                group,
                epoch,
                sequence,
                ..
            } => Some((*group, Some((*epoch, *sequence)))),
            _ => None,
        };
        if let Some((group, order)) = replication {
            match self.admit_replication(&verified, group, order) {
                Ok(Replication::Stepped(deadline)) => self.pending.push_back(Pending {
                    header,
                    response,
                    waiting: WaitingFor::PeerPersistence,
                    term: self.session.scalars().term,
                    deadline,
                    _charge: charge,
                }),
                Ok(Replication::Held {
                    source,
                    sequence,
                    until,
                    deadline,
                }) => {
                    let (_, request) = verified.into_parts();
                    let Operation::RaftOrdered { message, .. } = request.operation else {
                        let mut header = header;
                        header.result = Response::Error(AccessError::Unavailable);
                        let _ = response.send(finish_response(header, charge));
                        return;
                    };
                    let frame = HeldFrame {
                        source,
                        message,
                        pending: Pending {
                            header,
                            response,
                            waiting: WaitingFor::PeerPersistence,
                            term: self.session.scalars().term,
                            deadline,
                            _charge: charge,
                        },
                    };
                    if let Err(frame) = self.resequencer.hold(source, sequence, frame, until) {
                        frame.pending.finish(Response::Error(AccessError::Capacity));
                    }
                    // A lane that was full let what it held go, this frame
                    // with it.
                    self.step_due();
                }
                Ok(Replication::Waiting {
                    source,
                    ordered,
                    lost,
                    deadline,
                }) => {
                    let (_, request) = verified.into_parts();
                    let (Operation::Raft { message, .. } | Operation::RaftOrdered { message, .. }) =
                        request.operation
                    else {
                        let mut header = header;
                        header.result = Response::Error(AccessError::Unavailable);
                        let _ = response.send(finish_response(header, charge));
                        return;
                    };
                    self.frames_waited = self.frames_waited.saturating_add(1);
                    self.waiting_frames.push_back(WaitingFrame {
                        frame: HeldFrame {
                            source,
                            message,
                            pending: Pending {
                                header,
                                response,
                                waiting: WaitingFor::PeerPersistence,
                                term: self.session.scalars().term,
                                deadline,
                                _charge: charge,
                            },
                        },
                        ordered,
                        lost,
                    });
                    // What the resequencer held behind it follows it, in its
                    // order.
                    if ordered {
                        self.step_ready(source);
                    }
                }
                Err(error) => {
                    let mut header = header;
                    header.result = Response::Error(error);
                    let _ = response.send(finish_response(header, charge));
                }
            }
            return;
        }
        let mut waiting = None;
        let result = (|| -> Result<Response, AccessError> {
            let request = verified.request();
            let peer = verified.peer();
            let deadline = self.request_deadline().ok_or(AccessError::Unavailable)?;
            if request.ledger != self.session.ledger() {
                return Err(AccessError::Unauthorized);
            }
            if let Operation::ManagedSupport { group } = &request.operation {
                let PeerRole::Node { node_id } = peer.role() else {
                    return Err(AccessError::Unauthorized);
                };
                if (!self.session.members().holds(node_id)
                    && self.admitted.binary_search(&node_id).is_err())
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
                    if self.pending_participants() >= self.config.pending_clients {
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
                                    self.session.members().voters,
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
                    if self.pending_participants() >= self.config.pending_clients {
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
                    if self.pending_participants() >= self.config.pending_clients {
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
                    if self.pending_participants() >= self.config.pending_clients {
                        return Err(AccessError::Capacity);
                    }
                    if *cluster != self.session.cluster_id() {
                        return Err(AccessError::Unauthorized);
                    }
                    self.nonce = self.nonce.checked_add(1).ok_or(AccessError::Unavailable)?;
                    let mut context = b"focal.replica.managed.read.v1\0".to_vec();
                    context.extend_from_slice(&self.nonce.to_be_bytes());
                    context.extend_from_slice(&self.session.scalars().node_id.to_be_bytes());
                    context.extend_from_slice(&self.incarnation.to_be_bytes());
                    context.extend_from_slice(&peer.principal().0);
                    context.extend_from_slice(&request.request_id.0);
                    self.session
                        .read_index(context.clone())
                        .map_err(barrier_refused)?;
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
                    if self.pending_participants() >= self.config.pending_clients {
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
                            self.session.members().voters,
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
                    if self.pending_participants() >= self.config.pending_clients {
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
                    if self.pending_participants() >= self.config.pending_clients {
                        return Err(AccessError::Capacity);
                    }
                    self.nonce = self.nonce.checked_add(1).ok_or(AccessError::Unavailable)?;
                    let mut context = b"focal.replica.monitor.v1\0".to_vec();
                    context.extend_from_slice(&self.nonce.to_be_bytes());
                    context.extend_from_slice(&self.session.scalars().node_id.to_be_bytes());
                    context.extend_from_slice(&self.incarnation.to_be_bytes());
                    context.extend_from_slice(&peer.principal().0);
                    context.extend_from_slice(&request.request_id.0);
                    self.session
                        .read_index(context.clone())
                        .map_err(barrier_refused)?;
                    waiting = Some((WaitingFor::Monitor { context, id: *id }, deadline));
                    Ok(Response::Error(AccessError::Unavailable))
                }
                Operation::Summary => {
                    if !self.session.is_authoritative() {
                        return Err(AccessError::Unavailable);
                    }
                    if self.pending_participants() >= self.config.pending_clients {
                        return Err(AccessError::Capacity);
                    }
                    self.nonce = self.nonce.checked_add(1).ok_or(AccessError::Unavailable)?;
                    let mut context = b"focal.replica.summary.v1\0".to_vec();
                    context.extend_from_slice(&self.nonce.to_be_bytes());
                    context.extend_from_slice(&self.session.scalars().node_id.to_be_bytes());
                    context.extend_from_slice(&self.incarnation.to_be_bytes());
                    context.extend_from_slice(&peer.principal().0);
                    context.extend_from_slice(&request.request_id.0);
                    self.session
                        .read_index(context.clone())
                        .map_err(barrier_refused)?;
                    waiting = Some((WaitingFor::Summary { context }, deadline));
                    Ok(Response::Error(AccessError::Unavailable))
                }
                Operation::Reconcile(query) => {
                    if !self.session.is_authoritative() {
                        return Err(AccessError::Unavailable);
                    }
                    if self.pending_participants() >= self.config.pending_clients {
                        return Err(AccessError::Capacity);
                    }
                    self.nonce = self.nonce.checked_add(1).ok_or(AccessError::Unavailable)?;
                    let mut context = b"focal.replica.reconcile.v1\0".to_vec();
                    context.extend_from_slice(&self.nonce.to_be_bytes());
                    context.extend_from_slice(&self.session.scalars().node_id.to_be_bytes());
                    context.extend_from_slice(&self.incarnation.to_be_bytes());
                    context.extend_from_slice(&peer.principal().0);
                    context.extend_from_slice(&request.request_id.0);
                    self.session
                        .read_index(context.clone())
                        .map_err(barrier_refused)?;
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
                        if self.pending_participants() >= self.config.pending_clients {
                            return Err(AccessError::Capacity);
                        }
                        self.nonce = self.nonce.checked_add(1).ok_or(AccessError::Unavailable)?;
                        let mut context = b"focal.replica.list.v1\0".to_vec();
                        context.extend_from_slice(&self.nonce.to_be_bytes());
                        context.extend_from_slice(&self.session.scalars().node_id.to_be_bytes());
                        context.extend_from_slice(&self.incarnation.to_be_bytes());
                        context.extend_from_slice(&peer.principal().0);
                        context.extend_from_slice(&request.request_id.0);
                        self.session
                            .read_index(context.clone())
                            .map_err(barrier_refused)?;
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
                        if self.pending_participants() >= self.config.pending_clients {
                            return Err(AccessError::Capacity);
                        }
                        self.nonce = self.nonce.checked_add(1).ok_or(AccessError::Unavailable)?;
                        let mut context = b"focal.replica.selection.v1\0".to_vec();
                        context.extend_from_slice(&self.nonce.to_be_bytes());
                        context.extend_from_slice(&self.session.scalars().node_id.to_be_bytes());
                        context.extend_from_slice(&self.incarnation.to_be_bytes());
                        context.extend_from_slice(&peer.principal().0);
                        context.extend_from_slice(&request.request_id.0);
                        self.session
                            .read_index(context.clone())
                            .map_err(barrier_refused)?;
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
                        if self.pending_participants() >= self.config.pending_clients {
                            return Err(AccessError::Capacity);
                        }
                        self.nonce = self.nonce.checked_add(1).ok_or(AccessError::Unavailable)?;
                        let mut context = b"focal.replica.validators.v1\0".to_vec();
                        context.extend_from_slice(&self.nonce.to_be_bytes());
                        context.extend_from_slice(&self.session.scalars().node_id.to_be_bytes());
                        context.extend_from_slice(&self.incarnation.to_be_bytes());
                        context.extend_from_slice(&peer.principal().0);
                        context.extend_from_slice(&request.request_id.0);
                        self.session
                            .read_index(context.clone())
                            .map_err(barrier_refused)?;
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
                        if self.pending_participants() >= self.config.pending_clients {
                            return Err(AccessError::Capacity);
                        }
                        self.nonce = self.nonce.checked_add(1).ok_or(AccessError::Unavailable)?;
                        let mut context = b"focal.replica.traversal.v1\0".to_vec();
                        context.extend_from_slice(&self.nonce.to_be_bytes());
                        context.extend_from_slice(&self.session.scalars().node_id.to_be_bytes());
                        context.extend_from_slice(&self.incarnation.to_be_bytes());
                        context.extend_from_slice(&peer.principal().0);
                        context.extend_from_slice(&request.request_id.0);
                        self.session
                            .read_index(context.clone())
                            .map_err(barrier_refused)?;
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
                        if self.pending_participants() >= self.config.pending_clients {
                            return Err(AccessError::Capacity);
                        }
                        self.nonce = self.nonce.checked_add(1).ok_or(AccessError::Unavailable)?;
                        let mut context = b"focal.replica.read.v1\0".to_vec();
                        context.extend_from_slice(&self.nonce.to_be_bytes());
                        context.extend_from_slice(&self.session.scalars().node_id.to_be_bytes());
                        context.extend_from_slice(&self.incarnation.to_be_bytes());
                        context.extend_from_slice(&peer.principal().0);
                        context.extend_from_slice(&request.request_id.0);
                        self.session
                            .read_index(context.clone())
                            .map_err(barrier_refused)?;
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
                    if self.pending_participants() >= self.config.pending_clients {
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
                        // A follower serves it too: its barrier goes to the
                        // leader, and the answer waits for this copy to have
                        // applied the index it names (27 §5).
                        if !self.session.serves_native_reads() {
                            return Err(AccessError::Unavailable);
                        }
                        if self.pending_participants() >= self.config.pending_clients {
                            return Err(AccessError::Capacity);
                        }
                        self.nonce = self.nonce.checked_add(1).ok_or(AccessError::Unavailable)?;
                        let correlation = crate::native_reads::correlation(
                            self.session.scalars().node_id,
                            self.incarnation,
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
                term: self.session.scalars().term,
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
        // Frames that waited for the last write go into the Ready this drain
        // takes, and are answered at its fence.
        self.step_waiting();
        // The batch's drain, whatever its poll makes of it: a write still in
        // flight answers its frames when it is (`Owner::drain_owed`).
        self.drain_owed = false;
        // A retained delivery (a retryable native refusal, an import waiting
        // for sealed custody) resumes at the next poll; it never stops the
        // replica.
        // What a leader sends leaves while its own write is in flight (27
        // §3.4, the audit's F17): its members persist it for themselves, so
        // the two writes overlap. The events of a poll are taken whole, once
        // nothing of them is still to persist.
        let events = if self.nonblocking {
            match self.session.try_poll() {
                Ok(Some(events)) => events,
                Ok(None) => {
                    self.waits_asked = self.waits_asked.saturating_add(1);
                    self.send_early()?;
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
            loop {
                match self.session.try_poll() {
                    Ok(Some(events)) => break events,
                    Ok(None) => {
                        self.send_early()?;
                        // This owner's thread has nothing else to do for
                        // its replica: it waits for the write. Where there
                        // is none to wait for — a checkpoint, a decoder
                        // floor, a log with no room yet — the poll that
                        // waits for those takes over.
                        if self.session.wait_persisted()? {
                            continue;
                        }
                        match self.session.poll() {
                            Ok(events) => break events,
                            Err(LedgerError::Retry) => {
                                self.publish_progress(false);
                                return Ok(());
                            }
                            Err(error) => return Err(error),
                        }
                    }
                    Err(LedgerError::Retry) => {
                        self.publish_progress(false);
                        return Ok(());
                    }
                    Err(error) => return Err(error),
                }
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
        self.send(&events.messages)?;
        self.poll_snapshot_feedback()?;
        // The write is durable: what waited for it is stepped now, and owes
        // the drain that answers it.
        if self.step_waiting() {
            self.drain_owed = true;
        }
        self.publish_progress(false);
        Ok(())
    }
    /// Send what may be sent while the replica's write is in flight
    /// (`Session::sendable`). No snapshot is among it: a snapshot is sent
    /// with the events of the poll, where what became of it can be told.
    fn send_early(&mut self) -> Result<(), LedgerError> {
        let Some(mut early) = self.session.sendable()? else {
            return Ok(());
        };
        let _charge = early.take_allocation();
        self.send(&early.messages)
    }
    /// The stream of `source`'s appends, kept for as many peers as the order
    /// is (`LOST_PEERS`); none beyond them.
    fn append_stream(&mut self, source: u64) -> Option<&mut AppendStream> {
        if !self.append_streams.contains_key(&source) && self.append_streams.len() >= LOST_PEERS {
            return None;
        }
        Some(self.append_streams.entry(source).or_default())
    }
    /// `source` sent an ordered frame in this replica's term.
    fn sent_ordered(&mut self, source: u64) {
        let term = self.session.scalars().term;
        if let Some(stream) = self.append_stream(source) {
            stream.ordered_in = Some(term);
        }
    }
    /// A frame of `source` was lost to this log: what it refuses in this
    /// term short of the first entry it lacks now is the loss's. Noted once
    /// the frame lost — let go, found stale, or not stepped — has been
    /// stepped, so that what it carried, if the log took it, counts as held.
    /// Where the log cannot say where it ends, every refusal of the term is.
    fn lost_to(&mut self, source: u64) {
        let term = self.session.scalars().term;
        let from = self
            .session
            .last_log_index()
            .map_or(u64::MAX, |last| last.saturating_add(1));
        if let Some(stream) = self.append_stream(source) {
            stream.lost_from = Some(match stream.lost_from {
                Some((lost_in, before)) if lost_in == term => (term, before.max(from)),
                _ => (term, from),
            });
        }
    }
    /// What this replica's answer to `leader`'s append says (27 §12). One
    /// refused is counted; and counted as the order's to have spared when
    /// the leader sent ordered and had an append taken in the answer's
    /// term, and it was refused at or past where the log stood when a
    /// frame of the leader was last lost to it — its hint, the last entry
    /// this log could agree on, no earlier than that. A refusal that asks
    /// for a snapshot is not one for an entry.
    fn judge_append_answer(&mut self, answer: &focal_consensus::Message) {
        if answer.reject {
            self.appends_rejected = self.appends_rejected.saturating_add(1);
        }
        let Some(stream) = self.append_stream(answer.to) else {
            return;
        };
        if !answer.reject {
            stream.taken_in = Some(answer.term);
            return;
        }
        let lost = stream
            .lost_from
            .is_some_and(|(term, from)| term == answer.term && answer.reject_hint < from);
        if stream.ordered_in == Some(answer.term)
            && stream.taken_in == Some(answer.term)
            && answer.request_snapshot == 0
            && !lost
        {
            self.appends_rejected_in_order = self.appends_rejected_in_order.saturating_add(1);
        }
    }
    /// Hand the replica's messages to the driver that carries them. A
    /// message that is not handed over — no room for it here, or in the
    /// driver's queue — is told to the core as one the driver gave up is
    /// (`report_lost`): the member is probed, and nothing is taken to be on
    /// its way that is not.
    fn send(&mut self, messages: &[focal_consensus::Message]) -> Result<(), LedgerError> {
        for message in messages {
            if message.msg_type == focal_consensus::MessageType::MsgAppendResponse {
                self.judge_append_answer(message);
            }
            // A bulk frame carries the order it leaves in (27 §12): the
            // next sequence to its peer within its term, for as many peers
            // as a configuration names; the driver finishes it for the
            // peer's profile. Given before any frame is given up here, so
            // a frame given up leaves its gap in the order: its peer lets
            // the frames behind it go past their patience, as for one the
            // path lost, and takes what it refuses after for the loss's. A
            // frame given up before it had a sequence left no gap, and the
            // appends sent after it were refused in an order that showed
            // none (a peer's runs of the lossy path, its core queueing more
            // appends a Ready).
            let urgent = urgent(message);
            let sequence = if urgent {
                None
            } else {
                match self.ordered.get(&message.to) {
                    Some((term, last)) if *term == message.term => last.checked_add(1),
                    Some(_) => Some(1),
                    None if self.ordered.len() < LOST_PEERS => Some(1),
                    None => None,
                }
            };
            if let Some(sequence) = sequence {
                self.ordered.insert(message.to, (message.term, sequence));
            }
            let snapshot = match self.snapshot_feedback.begin(message, &self.budget) {
                Ok(snapshot) => snapshot,
                Err(_) => {
                    // Metadata admission also cannot strand Raft in Snapshot.
                    self.session.report_snapshot_at(
                        message.to,
                        message.term,
                        message
                            .snapshot
                            .as_deref()
                            .map_or(0, focal_consensus::snapshot_index),
                        focal_consensus::SnapshotStatus::Failure,
                    )?;
                    self.dropped = self.dropped.saturating_add(1);
                    continue;
                }
            };
            let Ok(size) = focal_consensus::envelope::message_len(message) else {
                drop(snapshot);
                self.dropped = self.dropped.saturating_add(1);
                let _ = self.lost_sender.try_send(message.to);
                continue;
            };
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
                let _ = self.lost_sender.try_send(message.to);
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
                let _ = self.lost_sender.try_send(message.to);
                continue;
            };
            let Ok(message_bytes) = focal_consensus::encode_message(message) else {
                drop(snapshot);
                self.dropped = self.dropped.saturating_add(1);
                let _ = self.lost_sender.try_send(message.to);
                continue;
            };
            self.nonce = self.nonce.checked_add(1).ok_or(LedgerError::Capacity)?;
            let id = ((u128::from(self.session.scalars().node_id) << 64) | u128::from(self.nonce))
                .to_be_bytes();
            let group = self.session.group_id();
            let operation = match sequence {
                Some(sequence) => Operation::RaftOrdered {
                    group,
                    epoch: message.term,
                    sequence,
                    message: message_bytes,
                },
                None => Operation::Raft {
                    group,
                    message: message_bytes,
                },
            };
            let frame = ReplicationFrame {
                target: message.to,
                snapshot,
                lost: Some(self.lost_sender.clone()),
                urgent,
                _charge: charge.commit(),
                request: RequestEnvelope {
                    protocol: if matches!(operation, Operation::RaftOrdered { .. }) {
                        focal_wire::ORDERED_PROTOCOL_VERSION
                    } else {
                        PROTOCOL_VERSION
                    },
                    ledger: self.session.ledger(),
                    route_epoch: self.config.route_epoch,
                    request_epoch: RequestEpoch(1),
                    request_id: RequestId(id),
                    operation,
                },
            };
            if self.outbound.try_send(frame).is_err() {
                self.dropped = self.dropped.saturating_add(1);
                let _ = self.lost_sender.try_send(message.to);
            }
        }
        Ok(())
    }
    fn poll_snapshot_feedback(&mut self) -> Result<(), LedgerError> {
        self.snapshot_feedback
            .poll(self.session.scalars().term, |peer, term, index, status| {
                self.session.report_snapshot_at(peer, term, index, status)
            })
    }
    fn resolve(&mut self, events: &SessionEvents) -> Result<(), LedgerError> {
        self.resolve_memberships(events)?;
        self.resolve_placement(events)?;
        let status = self.session.scalars();
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
                } if pending.term == status.term && self.session.serves_native_reads() => events
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
            } else if self.pace.periods() >= pending.deadline
                || status.term != pending.term
                || (status.role != StateRole::Leader && pending.waiting.answered_by_leader())
            {
                let result = self.given_up(&mut pending);
                pending.finish(result);
            } else {
                self.pending.push_back(pending);
            }
        }
        Ok(())
    }
    /// What a request the owner gives up is answered with: a read parked
    /// with an empty page gets that page — its barrier was current when it
    /// crossed it (the audit's F61) — and every other request the outcome
    /// the owner can vouch for.
    fn given_up(&self, pending: &mut Pending) -> Response {
        match &mut pending.waiting {
            WaitingFor::Stream(stream) => {
                if let Some(reply) = stream.parked_reply() {
                    return Response::Stream(reply);
                }
                Response::Error(stream.interrupted())
            }
            WaitingFor::ManagedStream { stream, .. } => Response::Error(stream.interrupted()),
            WaitingFor::Mutation(_)
            | WaitingFor::ManagedMutation { .. }
            | WaitingFor::RequestStreamControl { .. } => {
                Response::Error(AccessError::OutcomeUnknown)
            }
            _ => Response::Error(AccessError::Unavailable),
        }
    }
    /// The owner's period at which a request taken now is given up: the
    /// request time in the periods it holds at the configured tick, counted
    /// as the owner runs them (27 §3.1 P2).
    fn request_deadline(&self) -> Option<u64> {
        // And the ticks of the owner's own remembered stall, as its
        // replica's patience is (`ControlHost::request_deadline`).
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
    fn expire_pending(&mut self) {
        self.expire_placement();
        self.expire_held();
        let now = self.pace.periods();
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
            let Some(mut pending) = self.pending.pop_front() else {
                break;
            };
            if pending.response.is_closed() {
                drop(pending);
            } else if now >= pending.deadline {
                let result = self.given_up(&mut pending);
                pending.finish(result);
            } else {
                self.pending.push_back(pending);
            }
        }
        // A frame that waited for a write past its deadline is given up, a
        // loss to its source's stream as a frame not stepped is; those
        // behind it keep their order.
        let count = self.waiting_frames.len();
        for _ in 0..count {
            let Some(waiting) = self.waiting_frames.pop_front() else {
                break;
            };
            if now < waiting.frame.pending.deadline && !waiting.frame.pending.response.is_closed() {
                self.waiting_frames.push_back(waiting);
                continue;
            }
            if waiting.ordered {
                self.lost_to(waiting.frame.source);
            }
            waiting
                .frame
                .pending
                .finish(Response::Error(AccessError::OutcomeUnknown));
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
            let deadline = self.request_deadline().ok_or(LedgerError::Capacity)?;
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
                    leader: self.session.scalars().leader_id,
                });
            }
            Ok((deadline, proposed))
        })();
        match admitted {
            Ok((deadline, proposed)) => self.memberships.push_back(PendingMembershipCall {
                call,
                proposed,
                context: None,
                term: self.session.scalars().term,
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
        let status = self.session.scalars();
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
                || self.pace.periods() >= pending.deadline
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

/// A checkpoint step's outcome with what only waits for a later period taken
/// as nothing done: a resource condition or unpersisted state.
fn retryable(result: Result<bool, LedgerError>) -> Result<bool, LedgerError> {
    match result {
        Ok(done) => Ok(done),
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
