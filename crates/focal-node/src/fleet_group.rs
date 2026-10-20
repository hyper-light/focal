//! Several independent session logs share one node worker, a bounded ingress
//! queue and tenant-fair scheduling. Session identities and commit order stay
//! independent; a stopped session does not stop unrelated healthy sessions.
use super::*;
use focal_directory::{
    DirectoryError, FairScheduler, QueueUsage, ScheduleOutcome, SchedulerConfig, TenantQuota,
    WorkClass, WorkId, WorkKey, WorkMetadata,
};
use std::collections::BTreeMap;
#[path = "fleet_management.rs"]
pub(super) mod management;

const QUEUED: usize = 1024;
const RESERVED: usize = 128;
const SLICE: usize = 32;
/// The scheduler quota every tenant receives. A tenant, and each session of it, may queue as much
/// as the owner's whole input holds ([`QUEUED`]), keeping the completion lane's share of it the
/// owner keeps ([`RESERVED`]); the byte quotas count the scheduler's bookkeeping of each item,
/// which is less than an item's greatest cost (`SchedulerConfig::max_cost`), so they never bind
/// before the items do. What caps a session's queue is its admission — its slots
/// ([`queue_share`]) and its queueing delay (`TickPeriod::delay_bound`) — and a request admitted
/// is never refused here once queued: one dropped after admission is answered with an outcome
/// its participant cannot know (the scheduler's per-session 128 items dropped 92,015 admitted
/// writes in 25 s at 1,000/s once the participants' 24 slots were lifted, 2026-10-12).
fn scheduler_quota(weight: u32, max_sessions: usize) -> TenantQuota {
    let item_bytes = usize::try_from(SchedulerConfig::default().max_cost).unwrap_or(usize::MAX);
    TenantQuota {
        weight,
        max_items: QUEUED,
        reserved_items: RESERVED,
        max_bytes: QUEUED.saturating_mul(item_bytes),
        reserved_bytes: RESERVED.saturating_mul(item_bytes),
        session_items: QUEUED,
        session_reserved_items: RESERVED,
        session_bytes: QUEUED.saturating_mul(item_bytes),
        session_reserved_bytes: RESERVED.saturating_mul(item_bytes),
        max_sessions,
    }
}
/// A tenant's, or a session's, share of the owner's input: all of it, the completion lane's
/// share kept as the owner keeps it. A session alone takes the whole queue; sessions together
/// share it as they arrive, each bounded by its own queueing delay rather than by a count picked
/// for it (a session's 24 participant slots held a node at 425 writes a second when its write
/// cycle took 24 ms: Little's law, 2026-10-12).
fn queue_share(items: &MemoryBudget) -> Result<MemoryBudget, focal_memory::MemoryError> {
    items.child(QUEUED, RESERVED)
}
/// The places a session's share gives participants' requests: all but the completion lane's.
#[cfg(test)]
pub(super) const PARTICIPANT_SLOTS: usize = QUEUED.saturating_sub(RESERVED);
const STACK_BYTES: usize = 2 * 1024 * 1024;

/// The most owner shards a node runs (doc 31 §4.1, stage M1).
pub(super) const MAX_OWNER_SHARDS: usize = 64;

/// The owner shards a node runs unless it is told: one a core it is given (on Linux, the cgroup's
/// CPU quota, as the standard library reads it), at most [`MAX_OWNER_SHARDS`]. Every session's
/// owner ran on one thread a node, so a node's commits across all its sessions were one core's.
pub fn default_owner_shards() -> usize {
    std::thread::available_parallelism()
        .map_or(1, std::num::NonZeroUsize::get)
        .clamp(1, MAX_OWNER_SHARDS)
}

/// The shard whose owner holds `ledger`'s session, of `shards`: fixed by the ledger, so every
/// input for the session (its participants' work, its peers' frames, its log's answers) reaches
/// the one thread that owns it, and no shard waits on another's.
pub(super) fn shard_of(ledger: LedgerId, shards: usize) -> usize {
    let shards = u128::try_from(shards.max(1)).unwrap_or(1);
    let id = u128::from_be_bytes(ledger.session.0) ^ u128::from_be_bytes(ledger.tenant.0);
    usize::try_from(id.checked_rem(shards).unwrap_or(0)).unwrap_or(0)
}

pub struct FleetReplica {
    pub session: Session,
    pub config: ReplicaConfig,
}
#[derive(Clone)]
pub struct FleetTenant {
    pub tenant: TenantId,
    pub weight: u32,
    /// Must descend from the supplied node budget. Session construction uses
    /// this same parent through Session::from_node_in.
    pub budget: MemoryBudget,
}
pub struct ReplicaFleet;
pub type ReplicaFleetParts = (
    BTreeMap<LedgerId, ReplicaHost>,
    ReplicaOwner,
    FleetReplication,
);
/// Keeps outbound channel storage charged even after the worker has stopped.
/// The receiver cannot be detached from its accounting lifetime.
pub struct FleetReplication {
    receiver: ChannelReceiver<ReplicationFrame>,
    _backing: std::sync::Arc<Allocation>,
}
impl FleetReplication {
    /// The next frame; `None` once the owner's egress has ended (or no
    /// wait could be kept: off a shard).
    pub async fn recv(&mut self) -> Option<ReplicationFrame> {
        self.receiver.recv().await.ok()
    }
    /// A frame already queued, without waiting.
    pub fn try_recv(&mut self) -> Option<ReplicationFrame> {
        self.receiver.try_recv().ok().flatten()
    }
    /// Polls for the next frame as the replication driver does: `Closed`
    /// once the owner's egress has ended and nothing is queued.
    pub(crate) fn poll_recv(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<ReplicationFrame, hyper_rt::sync::SyncError<()>>> {
        std::future::Future::poll(std::pin::pin!(self.receiver.recv()), cx)
    }
}
pub(super) struct Routed {
    pub ledger: LedgerId,
    pub incarnation: u64,
    pub work: Work,
    pub _slot: Slot,
}
/// The place queued work holds until its owner takes it: a participant's
/// one of its session's queue items, a peer's frame one of the places its
/// session's peer reserve gives (F56) — never one of the participants'.
pub(super) enum Slot {
    Item { _allocation: Allocation },
    Frame { _ticket: crate::pace::FrameTicket },
}
/// What a peer's frame queued for a fleet's owner holds beside its bytes:
/// its place in the frames' channel and in its session's queue there,
/// which grows by doubling and is shrunk once a quarter full.
pub(super) const FRAME_QUEUE_BYTES: usize = size_of::<Routed>().saturating_mul(6);
/// The frames' channel's own blocks: the one being taken from and the one
/// being added to, of 31 frames each, whatever the frames queued.
const FRAME_CHANNEL_BYTES: usize = size_of::<Routed>().saturating_add(16).saturating_mul(64);
pub(super) enum FleetInput {
    Routed(Routed),
    Management(management::ManagementWork),
    /// A session the fleet's management installed, for this shard to own from now on. The reply
    /// says it is held: management answers the install only then.
    Adopt(Box<Owner>, mpsc::SyncSender<()>),
    /// Work every shard does for the fleet's management.
    Shard(ShardWork),
}
/// What a sharded fleet's management asks of each shard: what touches a shard's own scheduler,
/// done on the shard's thread.
pub(super) enum ShardWork {
    AdmitTenant {
        tenant: TenantId,
        quota: TenantQuota,
        reply: mpsc::SyncSender<Result<(), DirectoryError>>,
    },
    TenantUsage {
        tenants: Vec<TenantId>,
        reply: mpsc::SyncSender<BTreeMap<TenantId, QueueUsage>>,
    },
    /// The fleet stops: the shard closes its sessions and ends.
    Stop { reply: mpsc::SyncSender<()> },
    /// Holds the shard's thread until `release`, once it has said `held`: a test's shard busy
    /// with work.
    #[cfg(test)]
    Hold {
        held: mpsc::SyncSender<()>,
        release: mpsc::Receiver<()>,
    },
}
/// What the shared owner waits on: something was queued for it, or a write
/// of one of its sessions was answered by the log. A signal says there is
/// something to take, nothing more. An input's signal that finds the queue
/// full is dropped: the owner takes its input at every pass. An answer's is
/// not lost: it finds the queue full when one write of the log answers more
/// sessions than the queue holds (an owner holds up to 4,096 sessions, the
/// queue 1,024 signals — the gate of PR #4 met a thousand sessions answered
/// at once, one of them left to its tick), and then the owner is told that
/// one was not queued, and looks at every session with a write out
/// (`GroupOwner::sweep`).
pub(super) enum Signal {
    Input,
    Persisted(LedgerId),
}
/// The shared owner's queue as those that give it work hold it: the work
/// is queued, then the owner is woken.
#[derive(Clone)]
pub(super) struct OwnerQueue {
    input: mpsc::SyncSender<FleetInput>,
    /// The peers' frames, apart from the participants' work: unbounded as
    /// a channel, and bounded by the room each session's peer reserve
    /// gives (`TickPeriod::queue_frame`), each frame holding its place.
    frames: mpsc::Sender<Routed>,
    signal: mpsc::SyncSender<Signal>,
    /// That an answer's signal found the queue full: one token at most,
    /// pending until the owner takes it — a second drop while one is pending
    /// is covered by it.
    overflow: mpsc::SyncSender<()>,
}
/// The owner's ends of its queue: its input, the peers' frames, its
/// signals, and the token that says an answer's signal was not queued.
type OwnerEnds = (
    mpsc::Receiver<FleetInput>,
    mpsc::Receiver<Routed>,
    mpsc::Receiver<Signal>,
    mpsc::Receiver<()>,
);
impl OwnerQueue {
    /// The owner's input, frame and signal queues, and this handle on them.
    fn new() -> (Self, OwnerEnds) {
        let (input, inputs) = mpsc::sync_channel(QUEUED);
        let (frames, framed) = mpsc::channel();
        let (signal, signals) = mpsc::sync_channel(QUEUED);
        let (overflow, overflows) = mpsc::sync_channel(1);
        (
            Self {
                input,
                frames,
                signal,
                overflow,
            },
            (inputs, framed, signals, overflows),
        )
    }
    /// A peer's frame, queued apart from the participants' work in the
    /// place it holds in its session's room, and taken before that work
    /// (`GroupOwner::take_frames`). Refused only once the owner is gone.
    pub(super) fn send_frame(&self, routed: Routed) -> Result<(), Box<mpsc::SendError<Routed>>> {
        self.frames.send(routed).map_err(Box::new)?;
        let _ = self.signal.try_send(Signal::Input);
        Ok(())
    }
    /// Queues `input`, waiting for room: management's own sends, on its own
    /// thread, which nothing else waits on. Refused only once the owner is
    /// gone.
    pub(super) fn send(&self, input: FleetInput) -> Result<(), Box<mpsc::SendError<FleetInput>>> {
        self.input.send(input).map_err(Box::new)?;
        let _ = self.signal.try_send(Signal::Input);
        Ok(())
    }
    /// The refusal carries the work back, as the queue's own does; it is
    /// boxed, the work being large and a refusal rare.
    pub(super) fn try_send(
        &self,
        input: FleetInput,
    ) -> Result<(), Box<mpsc::TrySendError<FleetInput>>> {
        self.input.try_send(input).map_err(Box::new)?;
        let _ = self.signal.try_send(Signal::Input);
        Ok(())
    }
    /// What tells the owner that a write of `ledger`'s replica was
    /// answered (`Session::notify_persisted`): the owner drains the session
    /// then, instead of asking the log at intervals (27 §9).
    pub(super) fn persisted(&self, ledger: LedgerId) -> focal_consensus::PersistedSignal {
        let signal = self.signal.clone();
        let overflow = self.overflow.clone();
        Box::new(move || {
            let signal = signal.clone();
            let overflow = overflow.clone();
            Box::new(move || {
                // A full queue does not lose the answer: the owner is told
                // one was not queued, and sweeps (`GroupOwner::sweep`).
                if let Err(mpsc::TrySendError::Full(_)) = signal.try_send(Signal::Persisted(ledger))
                {
                    let _ = overflow.try_send(());
                }
            })
        })
    }
}
pub(super) fn lane(work: &Work) -> BudgetLane {
    match class(work) {
        WorkClass::Apply | WorkClass::Control | WorkClass::Completion => BudgetLane::Completion,
        _ => BudgetLane::Ordinary,
    }
}
fn class(work: &Work) -> WorkClass {
    match work {
        Work::Diagnostics(..)
        | Work::Registration(..)
        | Work::Stop(_)
        | Work::Transfer(..)
        | Work::ManagedSupport(..)
        | Work::ActivateNative(..)
        | Work::ImportPayloads(..)
        | Work::Checkpoint(..)
        | Work::ArtifactPointer(..)
        | Work::SeedChunks(..)
        | Work::InstallSeed(..)
        | Work::CustodyObjects(..)
        | Work::CustodyPulled(..)
        | Work::Refence(..)
        | Work::Admit(..)
        | Work::Windows(..)
        | Work::Membership(..)
        | Work::Placement(..)
        | Work::Range(..)
        | Work::Evidence(..) => WorkClass::Control,
        Work::Probe(request, ..) if completion_request(request) => WorkClass::Completion,
        Work::Probe(..) => WorkClass::Query,
        Work::Request(request, ..) => request_class(&request.verified),
        Work::Frame(request, ..) => request_class(request),
    }
}
/// The class a participant's or a peer's request is scheduled in: a peer's
/// frame applies, a read queries, a completion request completes, and the
/// rest append — the writes a session holds for its pipeline
/// ([`takes_class`]).
pub(super) fn request_class(request: &VerifiedRequest) -> WorkClass {
    match request.request().operation {
        Operation::Raft { .. } | Operation::RaftOrdered { .. } => WorkClass::Apply,
        Operation::Read(_)
        | Operation::Stream(_)
        | Operation::Reconcile(_)
        | Operation::RequestStreamRead { .. }
        | Operation::ManagedSupport { .. }
        | Operation::NativeRead(_)
        | Operation::NativeList(_)
        | Operation::NativeWait(_)
        | Operation::List(_)
        | Operation::Traverse(_)
        | Operation::Validators(_)
        | Operation::Summary
        | Operation::Monitor { .. }
        | Operation::Select(_) => WorkClass::Query,
        _ if completion_request(request) => WorkClass::Completion,
        _ => WorkClass::Append,
    }
}
impl ReplicaFleet {
    /// Trusted composition installs already authorized sessions. No thread or
    /// worker pool is created per session, and every session budget is checked
    /// against its tenant and node before any service handle is returned. The
    /// sessions are spread over the node's owner shards by ledger
    /// ([`shard_of`]), each shard a thread holding its sessions alone.
    pub fn spawn(
        node: u64,
        replicas: Vec<FleetReplica>,
        tenants: Vec<FleetTenant>,
        budget: MemoryBudget,
        limits: WireLimits,
    ) -> Result<ReplicaFleetParts, LedgerError> {
        Self::spawn_sharded(
            node,
            replicas,
            tenants,
            budget,
            limits,
            default_owner_shards(),
        )
    }

    /// [`Self::spawn`] over `shards` owner shards, never more than the
    /// sessions or [`MAX_OWNER_SHARDS`].
    pub fn spawn_sharded(
        node: u64,
        replicas: Vec<FleetReplica>,
        tenants: Vec<FleetTenant>,
        budget: MemoryBudget,
        limits: WireLimits,
        shards: usize,
    ) -> Result<ReplicaFleetParts, LedgerError> {
        if node == 0
            || replicas.is_empty()
            || replicas.len() > 4096
            || tenants.is_empty()
            || tenants.len() > 1024
        {
            return Err(LedgerError::Capacity);
        }
        let shards = shards.clamp(1, MAX_OWNER_SHARDS).min(replicas.len());
        let shared_bytes = size_of::<FleetInput>()
            .checked_add(size_of::<ReplicationFrame>())
            .and_then(|size| size.checked_add(size_of::<Signal>()))
            .and_then(|size| size.checked_add(128))
            .and_then(|size| QUEUED.checked_mul(size))
            .and_then(|bytes| bytes.checked_add(FRAME_CHANNEL_BYTES))
            .and_then(|bytes| bytes.checked_mul(shards))
            .and_then(|bytes| {
                replicas
                    .len()
                    .checked_mul(1024)
                    .and_then(|handles| bytes.checked_add(handles))
            })
            .and_then(|bytes| bytes.checked_add(size_of::<Allocation>()))
            .and_then(|bytes| bytes.checked_add(64))
            .ok_or(LedgerError::Capacity)?;
        // Host clones, the owners, and the outbound receiver can outlive one
        // another on different threads. One shared lease covers that exact
        // lifetime; per-request payloads and session state remain owned.
        let backing = std::sync::Arc::new(
            budget
                .reserve(BudgetKind::Control, BudgetLane::Completion, shared_bytes)?
                .commit(),
        );
        let (outbound, outgoing) =
            hyper_rt::sync::channel(QUEUED).map_err(|_| LedgerError::Capacity)?;
        let cluster = replicas
            .first()
            .ok_or(LedgerError::Capacity)?
            .session
            .cluster_id();
        let mut placed: Vec<Vec<FleetReplica>> = Vec::new();
        placed
            .try_reserve_exact(shards)
            .map_err(|_| LedgerError::Capacity)?;
        placed.resize_with(shards, Vec::new);
        for replica in replicas {
            let shard = shard_of(replica.session.ledger(), shards);
            placed
                .get_mut(shard)
                .ok_or(LedgerError::Capacity)?
                .push(replica);
        }
        // Every shard is built before any thread starts, so a refusal leaves
        // nothing running.
        let mut handles = BTreeMap::new();
        let mut built = Vec::new();
        built
            .try_reserve_exact(shards)
            .map_err(|_| LedgerError::Capacity)?;
        for replicas in placed {
            if replicas.is_empty() {
                continue;
            }
            built.push(Self::build_shard(
                node,
                cluster,
                replicas,
                &tenants,
                &budget,
                &limits,
                &outbound,
                &backing,
                &mut handles,
            )?);
        }
        let mut threads = Vec::new();
        threads
            .try_reserve_exact(built.len())
            .map_err(|_| LedgerError::Capacity)?;
        let count = built.len();
        for (index, (owner, receiver)) in built.into_iter().enumerate() {
            let name = if count == 1 {
                format!("focal-node-sessions-{node}")
            } else {
                format!("focal-node-sessions-{node}-{index}")
            };
            threads.push(
                std::thread::Builder::new()
                    .name(name)
                    .stack_size(STACK_BYTES)
                    .spawn(move || owner.run(receiver))
                    .map_err(|_| LedgerError::Capacity)?,
            );
        }
        Ok((
            handles,
            ReplicaOwner::new(threads),
            FleetReplication {
                receiver: outgoing,
                _backing: backing,
            },
        ))
    }

    /// One shard's owner over `replicas`, its hosts added to `handles`; its
    /// thread not yet started.
    #[allow(clippy::too_many_arguments)] // One shard's whole composition.
    fn build_shard(
        node: u64,
        cluster: [u8; 16],
        replicas: Vec<FleetReplica>,
        tenants: &[FleetTenant],
        budget: &MemoryBudget,
        limits: &WireLimits,
        outbound: &hyper_rt::sync::Sender<ReplicationFrame>,
        backing: &std::sync::Arc<Allocation>,
        handles: &mut BTreeMap<LedgerId, ReplicaHost>,
    ) -> Result<(GroupOwner, mpsc::Receiver<FleetInput>), LedgerError> {
        let bookkeeping = size_of::<Owner>()
            .checked_add(1024)
            .and_then(|n| replicas.len().checked_mul(n))
            .and_then(|n| n.checked_add(STACK_BYTES))
            .ok_or(LedgerError::Capacity)?;
        let allocation = budget
            .reserve(BudgetKind::Control, BudgetLane::Completion, bookkeeping)?
            .commit();
        let mut scheduler = FairScheduler::new(
            SchedulerConfig {
                max_items: QUEUED,
                reserved_items: RESERVED,
                ..SchedulerConfig::default()
            },
            budget.clone(),
        )
        .map_err(|_| LedgerError::Capacity)?;
        let item_budget = MemoryBudget::new(QUEUED, RESERVED)?;
        let mut tenant_budgets = BTreeMap::new();
        for tenant in tenants {
            if tenant.tenant.is_zero()
                || !tenant.budget.is_within(budget)
                || tenant_budgets.contains_key(&tenant.tenant)
            {
                return Err(LedgerError::Capacity);
            }
            scheduler
                .register_tenant(tenant.tenant, scheduler_quota(tenant.weight, 4096))
                .map_err(|_| LedgerError::Capacity)?;
            tenant_budgets.insert(
                tenant.tenant,
                (tenant.budget.clone(), queue_share(&item_budget)?),
            );
        }
        let (sender, (receiver, frames, signals, overflows)) = OwnerQueue::new();
        let mut sessions = BTreeMap::new();
        let mut wal_owners = Vec::new();
        wal_owners
            .try_reserve_exact(replicas.len())
            .map_err(|_| LedgerError::Capacity)?;
        let mut deadlines = BTreeMap::new();
        let now = Instant::now();
        for replica in replicas {
            let incarnation = u64::try_from(handles.len())
                .ok()
                .and_then(|count| count.checked_add(1))
                .ok_or(LedgerError::Capacity)?;
            // A member on focal-log keeps its WAL's writer here past every removal; a member on
            // the shell writes the node's log, which the node owns past the fleet.
            if let Ok(writer) = replica.session.shared_wal()
                && !wal_owners.iter().any(|retained: &focal_consensus::NodeStorage| {
                    matches!(retained.backend(), focal_consensus::StorageBackend::Wal(wal) if writer.is_same_writer(wal))
                })
            {
                wal_owners.push(focal_consensus::NodeStorage::wal(writer));
            }
            let ledger = replica.session.ledger();
            ReplicaHost::validate(&replica.config, limits)?;
            let (tenant, items) = tenant_budgets
                .get(&ledger.tenant)
                .ok_or(LedgerError::Capacity)?;
            if handles.contains_key(&ledger)
                || replica.session.status().node_id != node
                || replica.session.cluster_id() != cluster
                || !replica.session.is_budgeted_within(tenant)
                || replica.config.queue_items < 4
            {
                return Err(LedgerError::Capacity);
            }
            let ingress = tenant.child(64 * 1024 * 1024, 24 * 1024 * 1024)?;
            let slots = queue_share(items)?;
            let wake = sender.clone();
            let host_sender = HostSender::Group {
                ledger,
                incarnation,
                sender: sender.clone(),
                slots,
                _backing: backing.clone(),
            };
            let (host, mut owner) = ReplicaHost::assemble(
                replica.session,
                replica.config,
                limits.clone(),
                None,
                ingress,
                host_sender,
                outbound.clone(),
            )?;
            owner.nonblocking = true;
            owner.batching = true;
            // Its native proposals wait for its drains, as its drains wait
            // for its batches (`Owner::drain_with_runtime`).
            owner.session.set_native_proposals_deferred(true);
            owner.session.notify_persisted(Some(wake.persisted(ledger)));
            owner.incarnation = incarnation;
            owner.next_tick = now;
            owner.wake_at = now;
            handles.insert(ledger, host);
            sessions.insert(ledger, owner);
            deadlines.insert((now, ledger), ());
        }
        Ok((
            GroupOwner {
                sessions,
                deadlines,
                scheduler,
                frames,
                framed: BTreeMap::new(),
                signals,
                overflows,
                unwoken: std::collections::BTreeSet::new(),
                nonce: 0,
                shard: None,
                _wal_owners: wal_owners,
                _allocation: allocation,
                _backing: backing.clone(),
            },
            receiver,
        ))
    }
}
struct GroupOwner {
    sessions: BTreeMap<LedgerId, Owner>,
    deadlines: BTreeMap<(Instant, LedgerId), ()>,
    scheduler: FairScheduler<Option<Routed>>,
    /// The peers' frames queued for this owner (`OwnerQueue::send_frame`).
    frames: mpsc::Receiver<Routed>,
    /// Each session's frames taken from them and not yet dispatched, in the
    /// order they came: no more than its peer reserve gives room to, the
    /// places they hold (`FrameTicket`). A session's queue goes once it is
    /// empty, or the session stops.
    framed: BTreeMap<LedgerId, VecDeque<Routed>>,
    /// What wakes this owner when it has nothing due (`Signal`).
    signals: mpsc::Receiver<Signal>,
    /// That an answer's signal found the signals full (`OwnerQueue::persisted`).
    overflows: mpsc::Receiver<()>,
    /// The sessions that wait to persist and that the log will not tell
    /// this owner of: it had no room for their write. One is asked again
    /// for each write of this owner's the log answers — a write answered is
    /// its room given back — and each at its tick.
    unwoken: std::collections::BTreeSet<LedgerId>,
    nonce: u128,
    /// In a sharded fleet, the count of sessions this shard runs, which the fleet's status sums.
    /// A shard waits for its sessions as they are installed, so it ends only when its input
    /// closes or management stops it, never for being empty.
    shard: Option<watch::Sender<usize>>,
    // Physical writers outlive every logical-session removal. A final handle
    // may join its disk thread only after the entire fleet has stopped; one
    // stalled session cannot block another by dropping the last writer handle.
    _wal_owners: Vec<focal_consensus::NodeStorage>,
    _allocation: Allocation,
    _backing: std::sync::Arc<Allocation>,
}
/// A session that stops on a fail-closed error says why on the node's
/// standard error, as a standalone replica does; the other sessions of the
/// group are unaffected.
fn report_stop(ledger: LedgerId, error: &LedgerError) {
    use std::io::Write as _;
    let _ = writeln!(
        std::io::stderr().lock(),
        "focal: session {} stopped: {error}",
        ledger.session
    );
}
/// Whether a session takes input now: its write is not out, and it is not
/// stopping — or it is, and its leader hands the log off, which is messages
/// both ways: the heir's append responses say when it is caught up, and its
/// vote request ends this replica's term.
fn takes_input(owner: &Owner) -> bool {
    !owner.session.persistence_pending()
        && (owner.stopping.is_none() || owner.handing_off.is_some())
}
/// Whether a session takes its participants' work now: it takes input, and no session decision
/// it proposed (a layout change, a retirement, a seal) waits to apply, behind which it admits no
/// write. Its participants' work waits for it with the scheduler, as for a write out on
/// focal-log, where taken it was refused for capacity ("a committed seal is in flight"): on the
/// shell an owner takes input while its writes persist, and so met the decision a write had
/// waited out. Its peers' frames go on: they carry the decision's commit.
fn takes_participants(owner: &Owner) -> bool {
    takes_input(owner) && !owner.session.native_decision_in_flight()
}
/// Whether a session takes work of `class` now: as it takes its
/// participants' work, and its writes only while its pipeline is open
/// (`TickPeriod::pipeline_open`): a session whose oldest write has waited
/// past its bound holds the next in their queue, where their delay is
/// judged, and answers its reads meanwhile. A leader not yet ready in its term
/// (`Session::readying`) holds its writes, and the completions that commit as
/// writes do, for the term: taken, each was refused `NotReady` at every change
/// of leader until the term's first entry committed and its readiness round
/// came back. Its peers' frames and its control work go on: they are what makes
/// it ready.
fn takes_class(owner: &Owner, class: WorkClass) -> bool {
    takes_participants(owner)
        && match class {
            WorkClass::Append => {
                !owner.session.readying() && owner.pace.pipeline_open(owner.writes_in_flight())
            }
            WorkClass::Completion => !owner.session.readying(),
            _ => true,
        }
}
/// A peer's frame into its session's own queue, behind those that came
/// before it; dropped, and counted, where the queue cannot grow.
fn queue_frame(framed: &mut BTreeMap<LedgerId, VecDeque<Routed>>, owner: &Owner, routed: Routed) {
    let queue = framed.entry(routed.ledger).or_default();
    if queue.try_reserve(1).is_err() {
        owner.pace.drop_input(true);
        return;
    }
    queue.push_back(routed);
}
impl GroupOwner {
    fn run(mut self, receiver: mpsc::Receiver<FleetInput>) {
        let result = self.run_inner(receiver);
        if let Err(error) = result {
            use std::io::Write as _;
            let _ = writeln!(
                std::io::stderr().lock(),
                "focal: session fleet stopped: {error}"
            );
        }
        for owner in self.sessions.values_mut() {
            owner.close();
        }
        if let Some(running) = &self.shard {
            running.send_replace(0);
        }
    }
    fn stop_session(&mut self, ledger: LedgerId) {
        self.framed.remove(&ledger);
        if let Some(mut owner) = self.sessions.remove(&ledger) {
            self.deadlines.remove(&(owner.wake_at, ledger));
            self.unwoken.remove(&ledger);
            owner.close();
        }
        self.publish_running();
    }
    /// A shard's running count, published as its sessions change.
    fn publish_running(&self) {
        if let Some(running) = &self.shard {
            running.send_replace(self.sessions.len());
        }
    }
    /// Takes a session the fleet's management installed, due at once as each session is at its
    /// start.
    fn adopt(&mut self, mut owner: Owner) {
        let ledger = owner.session.ledger();
        let now = Instant::now();
        owner.next_tick = now;
        owner.wake_at = now;
        self.deadlines.insert((now, ledger), ());
        self.sessions.insert(ledger, owner);
        self.publish_running();
    }
    /// A sharded fleet's work for this shard; whether the shard stops.
    fn shard_work(&mut self, work: ShardWork) -> bool {
        match work {
            ShardWork::AdmitTenant {
                tenant,
                quota,
                reply,
            } => {
                let result = self.scheduler.register_tenant(tenant, quota).map(|_| ());
                let _ = reply.try_send(result);
                false
            }
            ShardWork::TenantUsage { tenants, reply } => {
                let mut usage = BTreeMap::new();
                for tenant in tenants {
                    if let Some(entry) = self.scheduler.usage(tenant) {
                        usage.insert(tenant, entry);
                    }
                }
                let _ = reply.try_send(usage);
                false
            }
            ShardWork::Stop { reply } => {
                let _ = reply.try_send(());
                true
            }
            #[cfg(test)]
            ShardWork::Hold { held, release } => {
                let _ = held.try_send(());
                let _ = release.recv();
                false
            }
        }
    }
    fn reschedule(&mut self, ledger: LedgerId) -> Result<(), LedgerError> {
        if let Some(owner) = self.sessions.get_mut(&ledger) {
            let next = owner.group_deadline()?;
            self.deadlines.remove(&(owner.wake_at, ledger));
            owner.wake_at = next;
            self.deadlines.insert((next, ledger), ());
            if owner.session.persistence_pending() && !owner.session.wakes_owner() {
                self.unwoken.insert(ledger);
            } else {
                self.unwoken.remove(&ledger);
            }
        }
        Ok(())
    }
    /// The session is due now.
    fn due(&mut self, ledger: LedgerId) {
        if let Some(owner) = self.sessions.get_mut(&ledger) {
            let now = Instant::now();
            if owner.wake_at > now {
                self.deadlines.remove(&(owner.wake_at, ledger));
                owner.wake_at = now;
                self.deadlines.insert((now, ledger), ());
            }
        }
    }
    /// Takes the signals that wait, without waiting for one: a session
    /// whose write was answered while the owner had work is due on the
    /// owner's next pass, not when the owner next has nothing to do.
    fn take_signals(&mut self) {
        for _ in 0..QUEUED {
            match self.signals.try_recv() {
                Ok(signal) => self.signalled(signal),
                Err(_) => break,
            }
        }
        if self.overflows.try_recv().is_ok() {
            self.sweep();
        }
    }
    /// An answer's signal found the signals full and was not queued
    /// (`OwnerQueue::persisted`): which session's write it answered is not
    /// known, so every session with a write out is due now — one look each,
    /// its drain reading its write's own answer — and none is left to its
    /// tick. Rare: a write of the log answering more sessions than the
    /// signals hold.
    fn sweep(&mut self) {
        let now = Instant::now();
        for (ledger, owner) in &mut self.sessions {
            if !owner.session.persistence_pending() {
                continue;
            }
            owner.count_swept();
            if owner.wake_at > now {
                self.deadlines.remove(&(owner.wake_at, *ledger));
                owner.wake_at = now;
                self.deadlines.insert((now, *ledger), ());
            }
        }
    }
    fn enqueue(&mut self, routed: Routed) -> Result<(), LedgerError> {
        let Some(owner) = self.sessions.get_mut(&routed.ledger) else {
            return Ok(());
        };
        if owner.incarnation != routed.incarnation {
            return Ok(());
        }
        if let Work::Stop(response) = routed.work {
            // Shutdown intent does not mutate Raft. Admit it even while that
            // session's retained Ready fences ordinary/control state changes,
            // so its bounded deadline cannot be trapped behind a stalled disk.
            owner.begin_stop(response)?;
            self.reschedule(routed.ledger)?;
            return Ok(());
        }
        // A stopping session takes no more work — except while its leader
        // hands the log off (27 §5): its peers' messages are how the heir is
        // caught up and asked, and how this replica's term ends.
        if owner.stopping.is_some() && owner.handing_off.is_none() {
            return Ok(());
        }
        // A peer's frame is never the scheduler's: it waits in its session's
        // own queue (`take_frames`).
        if matches!(class(&routed.work), WorkClass::Apply) {
            queue_frame(&mut self.framed, owner, routed);
            return Ok(());
        }
        self.nonce = self.nonce.checked_add(1).ok_or(LedgerError::Capacity)?;
        let cost = match &routed.work {
            Work::Request(_, _, charge)
            | Work::Probe(_, _, charge)
            | Work::ManagedSupport(_, charge)
            | Work::Membership(_, charge)
            | Work::Placement(_, charge)
            | Work::Evidence(_, charge) => charge.bytes().clamp(1, 65536) as u64,
            _ => 1,
        };
        let metadata = WorkMetadata {
            key: WorkKey {
                ledger: routed.ledger,
                id: WorkId::from_u128(self.nonce),
            },
            class: class(&routed.work),
            cost,
        };
        // A scheduler refusal drops the queued request and returns its existing
        // conservative unknown/unavailable response; it cannot publish a write.
        // These scheduler byte quotas cover metadata only. Routed requests
        // already retain their payload reservation in the actual tenant/node
        // ingress hierarchy; charging their heap again would double-count it.
        if self.scheduler.enqueue(metadata, Some(routed), 0).is_err() {
            owner.pace.drop_input(false);
        }
        Ok(())
    }
    /// Takes the peers' frames queued for this owner, each into its
    /// session's own queue in the order it came, at most as many at a pass
    /// as the participants' input holds: each was queued in its session's
    /// room, so no more wait than the sessions' peer reserves. One for a
    /// session stopped or replaced, or stopping and not handing its log
    /// off, is dropped, as `enqueue` drops one: its peer is told the outcome
    /// is unknown.
    fn take_frames(&mut self) {
        for _ in 0..QUEUED {
            let Ok(routed) = self.frames.try_recv() else {
                break;
            };
            let Some(owner) = self.sessions.get(&routed.ledger) else {
                continue;
            };
            if owner.incarnation != routed.incarnation
                || (owner.stopping.is_some() && owner.handing_off.is_none())
            {
                continue;
            }
            queue_frame(&mut self.framed, owner, routed);
        }
    }
    /// Dispatches the queued frames of every session that takes input now,
    /// in the order they came and before the participants' work (27 §12).
    /// A session with its write out takes none, as the scheduler hands it
    /// nothing: its frames wait in their room for the write to be durable,
    /// not refused. At most as many at a pass as the input holds. Whether
    /// any was dispatched.
    fn dispatch_frames(&mut self) -> Result<bool, LedgerError> {
        let mut dispatched = 0usize;
        let mut next = self.framed.keys().next().copied();
        while let Some(ledger) = next {
            if dispatched >= QUEUED {
                break;
            }
            next = self
                .framed
                .range((
                    std::ops::Bound::Excluded(ledger),
                    std::ops::Bound::Unbounded,
                ))
                .next()
                .map(|(after, _)| *after);
            let Some(owner) = self.sessions.get_mut(&ledger) else {
                self.framed.remove(&ledger);
                continue;
            };
            if !takes_input(owner) {
                continue;
            }
            let Some(mut queue) = self.framed.remove(&ledger) else {
                continue;
            };
            let mut outcome = Ok(false);
            while dispatched < QUEUED && takes_input(owner) {
                let Some(routed) = queue.pop_front() else {
                    break;
                };
                let Routed {
                    incarnation,
                    work,
                    _slot: place,
                    ..
                } = routed;
                // The frame's place is given back as the owner takes it.
                drop(place);
                if owner.incarnation != incarnation {
                    continue;
                }
                dispatched = dispatched.saturating_add(1);
                let what = work.kind();
                let started = Instant::now();
                outcome = owner.accept(work);
                owner.slow.note(what, started);
                if !matches!(outcome, Ok(false)) {
                    break;
                }
            }
            match outcome {
                Ok(false) => {
                    if !queue.is_empty() {
                        if queue.capacity() > queue.len().saturating_mul(4) {
                            queue.shrink_to(queue.len().saturating_mul(2));
                        }
                        self.framed.insert(ledger, queue);
                    }
                    self.reschedule(ledger)?;
                }
                Ok(true) => self.stop_session(ledger),
                Err(error) => {
                    report_stop(ledger, &error);
                    self.stop_session(ledger);
                }
            }
        }
        Ok(dispatched != 0)
    }
    fn input(&mut self, input: FleetInput) -> Result<bool, LedgerError> {
        match input {
            FleetInput::Routed(routed) => {
                self.enqueue(routed)?;
                Ok(false)
            }
            // Management's work goes to its own thread, never to a shard.
            FleetInput::Management(_) => Ok(false),
            FleetInput::Adopt(owner, held) => {
                self.adopt(*owner);
                let _ = held.try_send(());
                Ok(false)
            }
            FleetInput::Shard(work) => Ok(self.shard_work(work)),
        }
    }
    fn run_inner(&mut self, receiver: mpsc::Receiver<FleetInput>) -> Result<(), LedgerError> {
        loop {
            if self.sessions.is_empty() && self.shard.is_none() {
                return Ok(());
            }
            self.take_signals();
            for _ in 0..SLICE {
                let Some((&(deadline, ledger), _)) = self.deadlines.first_key_value() else {
                    break;
                };
                if deadline > Instant::now() {
                    break;
                }
                self.deadlines.pop_first();
                if let Some(owner) = self.sessions.get_mut(&ledger) {
                    match owner.progress_group() {
                        Ok(false) => self.reschedule(ledger)?,
                        Ok(true) => self.stop_session(ledger),
                        Err(error) => {
                            report_stop(ledger, &error);
                            self.stop_session(ledger);
                        }
                    }
                }
            }
            self.take_frames();
            for _ in 0..SLICE {
                match receiver.try_recv() {
                    Ok(input) => {
                        if self.input(input)? {
                            return Ok(());
                        }
                    }
                    Err(mpsc::TryRecvError::Empty) => break,
                    Err(mpsc::TryRecvError::Disconnected) => return Ok(()),
                }
            }
            // The peers' frames go first: what a session's participants wait
            // on is its peers' answers.
            let framed = self.dispatch_frames()?;
            let mut runnable = false;
            for _ in 0..SLICE {
                let sessions = &self.sessions;
                match self
                    .scheduler
                    .schedule_where(|ledger, class| {
                        sessions
                            .get(&ledger)
                            .is_none_or(|owner| takes_class(owner, class))
                    })
                    .map_err(|_| LedgerError::Failed)?
                {
                    ScheduleOutcome::Work(mut dispatch) => {
                        let routed = dispatch.payload_mut().take().ok_or(LedgerError::Corrupt)?;
                        if let Some(owner) = self.sessions.get_mut(&routed.ledger) {
                            if owner.incarnation != routed.incarnation {
                                continue;
                            }
                            // What the owner does for a dispatch is timed as
                            // its period's steps are: the slow ones are what
                            // held every request behind them.
                            let what = routed.work.kind();
                            let started = Instant::now();
                            let accepted = owner.accept(routed.work);
                            owner.slow.note(what, started);
                            match accepted {
                                Ok(false) => self.reschedule(routed.ledger)?,
                                Ok(true) => self.stop_session(routed.ledger),
                                Err(error) => {
                                    report_stop(routed.ledger, &error);
                                    self.stop_session(routed.ledger);
                                }
                            }
                        }
                        runnable = true;
                    }
                    ScheduleOutcome::Continue => {
                        // The bounded slice may have visited only disk-blocked
                        // groups. Their short deadlines provide the next hint;
                        // do not spin a CPU while all receipts are outstanding.
                        runnable = !self
                            .sessions
                            .values()
                            .any(|owner| owner.session.persistence_pending());
                        break;
                    }
                    ScheduleOutcome::Idle => {
                        runnable = false;
                        break;
                    }
                }
            }
            if runnable || framed {
                continue;
            }
            let wait = self
                .deadlines
                .first_key_value()
                .map(|((when, _), _)| when.saturating_duration_since(Instant::now()))
                .unwrap_or(Duration::from_millis(100))
                .min(Duration::from_millis(100));
            // Nothing is due and nothing runnable: the owner waits for a
            // signal — work was queued, or the log answered a write — or
            // for its next deadline. The input is taken, and found closed,
            // at the top of the round.
            if let Ok(signal) = self.signals.recv_timeout(wait) {
                self.signalled(signal);
                self.take_signals();
            }
        }
    }
    /// A session whose write the log answered is due now: its drain takes
    /// what the write released. A wake is no proof of anything: the drain
    /// reads the write's own answer, and a signal for a session that was
    /// stopped, or replaced since, costs one look. And the room the
    /// answered write held is given back: one session the log had no room
    /// for is due with it.
    fn signalled(&mut self, signal: Signal) {
        let Signal::Persisted(ledger) = signal else {
            return;
        };
        if let Some(owner) = self.sessions.get_mut(&ledger) {
            owner.count_answered();
        }
        self.due(ledger);
        if let Some(waiting) = self.unwoken.pop_first() {
            if let Some(owner) = self.sessions.get_mut(&waiting) {
                owner.count_handed();
            }
            self.due(waiting);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An answer's signal that finds the owner's signals full is not lost:
    /// the token says one was not queued, a second drop while it is pending
    /// is covered by it, and once it is taken a drop arms it again. Before,
    /// such a signal was dropped and its session left to its tick (PR #4's
    /// macOS run: one of a thousand sessions answered by one write).
    #[test]
    fn an_answer_that_finds_the_signals_full_arms_the_sweep() {
        let (queue, (_inputs, _frames, signals, overflows)) = OwnerQueue::new();
        let ledger = LedgerId {
            tenant: focal_model::TenantId::from_u128(1),
            session: focal_model::SessionId::from_u128(1),
        };
        let wake = queue.persisted(ledger);
        for _ in 0..QUEUED {
            (wake())();
        }
        assert!(overflows.try_recv().is_err(), "nothing dropped yet");
        (wake())();
        (wake())();
        assert!(overflows.try_recv().is_ok(), "a drop armed the token");
        assert!(overflows.try_recv().is_err(), "one token covers both drops");
        let mut taken = 0;
        while signals.try_recv().is_ok() {
            taken += 1;
        }
        assert_eq!(taken, QUEUED);
        // Room again: the next answer is queued, and nothing is armed.
        (wake())();
        assert!(overflows.try_recv().is_err());
        assert!(matches!(signals.try_recv(), Ok(Signal::Persisted(at)) if at == ledger));
        // Full again after the token was taken: armed again.
        for _ in 0..=QUEUED {
            (wake())();
        }
        assert!(overflows.try_recv().is_ok());
    }

    /// A wake this owner counts for a session — an answer's signal, a sweep,
    /// a hand-off — is published at the session's next step, whether or not
    /// that step has anything to drain. A tick that comes as the log answers
    /// drains the answered write before the answer's signal is taken; the
    /// signal's step then drains nothing, and its count stayed unpublished
    /// until the session's next write (the wake test read 17 sessions of 100
    /// as never woken, each drained by its tick within milliseconds of its
    /// signal).
    #[test]
    fn a_wake_counted_after_the_drain_is_published_at_the_next_step() {
        let directory = tempfile::tempdir().unwrap();
        let ledger = LedgerId {
            tenant: focal_model::TenantId::from_u128(2),
            session: focal_model::SessionId::from_u128(3),
        };
        let mut session = Session::open(
            directory.path(),
            ledger,
            focal_consensus::NodeConfig::single(1, [82; 16], ledger.session.0),
            focal_ledger::SessionLimits::default(),
        )
        .unwrap();
        session.campaign().unwrap();
        let (sender, _receiver) = mpsc::sync_channel(4);
        let (outbound, _outgoing) = hyper_rt::sync::channel(32).unwrap();
        let (host, mut owner) = ReplicaHost::assemble(
            session,
            ReplicaConfig::new(focal_model::RootCommandId::from_u128(4)),
            ReplicaHost::wire_limits(),
            None,
            MemoryBudget::new(64 * 1024 * 1024, 24 * 1024 * 1024).unwrap(),
            HostSender::Direct(sender),
            outbound,
        )
        .unwrap();
        owner.nonblocking = true;
        // The election written and drained, charged to the log's writes: the
        // session has nothing left to persist or drain.
        let writes = |owner: &Owner| {
            owner
                .session
                .shared_wal()
                .unwrap()
                .stats()
                .map_or(0, |stats| stats.group_commits)
        };
        let mut wait = focal_timing::ProgressDeadline::begin(
            &[writes(&owner)],
            u64::MAX,
            crate::test_waits::FROZEN,
        );
        while owner.session.persistence_pending() || owner.session.has_ready() {
            owner.progress_group().unwrap();
            if let Err(spent) = wait.check(&[writes(&owner)]) {
                panic!("the session never settled: {spent}");
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        owner.progress_group().unwrap();
        let before = host.progress();
        // What `signalled` and `sweep` count, the write already drained.
        owner.count_answered();
        owner.count_swept();
        owner.count_handed();
        owner.progress_group().unwrap();
        assert!(!owner.session.persistence_pending() && !owner.session.has_ready());
        let after = host.progress();
        assert_eq!(
            (after.waits_answered, after.waits_swept, after.waits_handed),
            (
                before.waits_answered + 1,
                before.waits_swept + 1,
                before.waits_handed + 1
            )
        );
    }
}
