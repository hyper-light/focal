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
const STACK_BYTES: usize = 2 * 1024 * 1024;

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
    receiver: async_mpsc::Receiver<ReplicationFrame>,
    _backing: std::sync::Arc<Allocation>,
}
impl FleetReplication {
    pub async fn recv(&mut self) -> Option<ReplicationFrame> {
        self.receiver.recv().await
    }
    pub fn try_recv(&mut self) -> Result<ReplicationFrame, async_mpsc::error::TryRecvError> {
        self.receiver.try_recv()
    }
    pub fn close(&mut self) {
        self.receiver.close();
    }
}
pub(super) struct Routed {
    pub ledger: LedgerId,
    pub incarnation: u64,
    pub work: Work,
    pub _slot: Allocation,
}
pub(super) enum FleetInput {
    Routed(Routed),
    Management(management::ManagementWork),
}
/// What the shared owner waits on: something was queued for it, or a write
/// of one of its sessions was answered by the log. A signal says there is
/// something to take, nothing more: one that finds the queue of signals
/// full is dropped, since the owner is then about to take what the queued
/// ones say and looks at its input and its sessions after each of them.
pub(super) enum Signal {
    Input,
    Persisted(LedgerId),
}
/// The shared owner's queue as those that give it work hold it: the work
/// is queued, then the owner is woken.
#[derive(Clone)]
pub(super) struct OwnerQueue {
    input: mpsc::SyncSender<FleetInput>,
    signal: mpsc::SyncSender<Signal>,
}
impl OwnerQueue {
    /// The owner's input and signal queues, and this handle on them.
    fn new() -> (Self, mpsc::Receiver<FleetInput>, mpsc::Receiver<Signal>) {
        let (input, inputs) = mpsc::sync_channel(QUEUED);
        let (signal, signals) = mpsc::sync_channel(QUEUED);
        (Self { input, signal }, inputs, signals)
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
        Box::new(move || {
            let signal = signal.clone();
            Box::new(move || {
                let _ = signal.try_send(Signal::Persisted(ledger));
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
        Work::Request(request, ..) => match request.verified.request().operation {
            Operation::Raft { .. } | Operation::RaftOrdered { .. } => WorkClass::Apply,
            Operation::Read(_)
            | Operation::Stream(_)
            | Operation::Reconcile(_)
            | Operation::RequestStreamRead { .. }
            | Operation::ManagedSupport { .. } => WorkClass::Query,
            _ if completion_request(&request.verified) => WorkClass::Completion,
            _ => WorkClass::Append,
        },
    }
}
impl ReplicaFleet {
    /// Trusted composition installs already authorized sessions. No thread or
    /// worker pool is created per session, and every session budget is checked
    /// against its tenant and node before any service handle is returned.
    pub fn spawn(
        node: u64,
        replicas: Vec<FleetReplica>,
        tenants: Vec<FleetTenant>,
        budget: MemoryBudget,
        limits: WireLimits,
    ) -> Result<ReplicaFleetParts, LedgerError> {
        if node == 0
            || replicas.is_empty()
            || replicas.len() > 4096
            || tenants.is_empty()
            || tenants.len() > 1024
        {
            return Err(LedgerError::Capacity);
        }
        let bookkeeping = size_of::<Owner>()
            .checked_add(1024)
            .and_then(|n| replicas.len().checked_mul(n))
            .and_then(|n| n.checked_add(STACK_BYTES))
            .ok_or(LedgerError::Capacity)?;
        let allocation = budget
            .reserve(BudgetKind::Control, BudgetLane::Completion, bookkeeping)?
            .commit();
        let shared_bytes = size_of::<FleetInput>()
            .checked_add(size_of::<ReplicationFrame>())
            .and_then(|size| size.checked_add(size_of::<Signal>()))
            .and_then(|size| size.checked_add(128))
            .and_then(|size| QUEUED.checked_mul(size))
            .and_then(|bytes| {
                replicas
                    .len()
                    .checked_mul(1024)
                    .and_then(|handles| bytes.checked_add(handles))
            })
            .and_then(|bytes| bytes.checked_add(size_of::<Allocation>()))
            .and_then(|bytes| bytes.checked_add(64))
            .ok_or(LedgerError::Capacity)?;
        // Host clones, the owner, and the outbound receiver can outlive one
        // another on different threads. One shared lease covers that exact
        // lifetime; per-request payloads and session state remain owned.
        let backing = std::sync::Arc::new(
            budget
                .reserve(BudgetKind::Control, BudgetLane::Completion, shared_bytes)?
                .commit(),
        );
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
                || !tenant.budget.is_within(&budget)
                || tenant_budgets.contains_key(&tenant.tenant)
            {
                return Err(LedgerError::Capacity);
            }
            scheduler
                .register_tenant(
                    tenant.tenant,
                    TenantQuota {
                        weight: tenant.weight,
                        max_items: 256,
                        reserved_items: 32,
                        max_bytes: 16 * 1024 * 1024,
                        reserved_bytes: 2 * 1024 * 1024,
                        session_items: 128,
                        session_reserved_items: 16,
                        session_bytes: 8 * 1024 * 1024,
                        session_reserved_bytes: 1024 * 1024,
                        max_sessions: 4096,
                    },
                )
                .map_err(|_| LedgerError::Capacity)?;
            tenant_budgets.insert(tenant.tenant, (tenant.budget, item_budget.child(256, 32)?));
        }
        let (sender, receiver, signals) = OwnerQueue::new();
        let (outbound, outgoing) = async_mpsc::channel(QUEUED);
        let mut sessions = BTreeMap::new();
        let mut wal_owners = Vec::new();
        wal_owners
            .try_reserve_exact(replicas.len())
            .map_err(|_| LedgerError::Capacity)?;
        let mut handles = BTreeMap::new();
        let mut deadlines = BTreeMap::new();
        let now = Instant::now();
        let cluster = replicas
            .first()
            .ok_or(LedgerError::Capacity)?
            .session
            .cluster_id();
        for replica in replicas {
            let incarnation = u64::try_from(handles.len())
                .ok()
                .and_then(|count| count.checked_add(1))
                .ok_or(LedgerError::Capacity)?;
            let writer = replica.session.shared_wal();
            if !wal_owners
                .iter()
                .any(|retained| writer.is_same_writer(retained))
            {
                wal_owners.push(writer);
            }
            let ledger = replica.session.ledger();
            ReplicaHost::validate(&replica.config, &limits)?;
            let (tenant, items) = tenant_budgets
                .get(&ledger.tenant)
                .ok_or(LedgerError::Capacity)?;
            if sessions.contains_key(&ledger)
                || replica.session.status().node_id != node
                || replica.session.cluster_id() != cluster
                || !replica.session.is_budgeted_within(tenant)
                || replica.config.queue_items < 4
            {
                return Err(LedgerError::Capacity);
            }
            let ingress = tenant.child(64 * 1024 * 1024, 24 * 1024 * 1024)?;
            let slots = items.child(replica.config.queue_items, replica.config.queue_items / 4)?;
            let wake = sender.clone();
            let sender = HostSender::Group {
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
                sender,
                outbound.clone(),
            )?;
            owner.nonblocking = true;
            owner.batching = true;
            owner.session.notify_persisted(Some(wake.persisted(ledger)));
            owner.incarnation = incarnation;
            owner.next_tick = now;
            owner.wake_at = now;
            handles.insert(ledger, host);
            sessions.insert(ledger, owner);
            deadlines.insert((now, ledger), ());
        }
        let owner = GroupOwner {
            sessions,
            deadlines,
            scheduler,
            signals,
            unwoken: std::collections::BTreeSet::new(),
            nonce: 0,
            management: None,
            _wal_owners: wal_owners,
            _allocation: allocation,
            _backing: backing.clone(),
        };
        let thread = std::thread::Builder::new()
            .name(format!("focal-node-sessions-{node}"))
            .stack_size(STACK_BYTES)
            .spawn(move || owner.run(receiver))
            .map_err(|_| LedgerError::Capacity)?;
        Ok((
            handles,
            ReplicaOwner(thread),
            FleetReplication {
                receiver: outgoing,
                _backing: backing,
            },
        ))
    }
}
struct GroupOwner {
    sessions: BTreeMap<LedgerId, Owner>,
    deadlines: BTreeMap<(Instant, LedgerId), ()>,
    scheduler: FairScheduler<Option<Routed>>,
    /// What wakes this owner when it has nothing due (`Signal`).
    signals: mpsc::Receiver<Signal>,
    /// The sessions that wait to persist and that the log will not tell
    /// this owner of: it had no room for their write. One is asked again
    /// for each write of this owner's the log answers — a write answered is
    /// its room given back — and each at its tick.
    unwoken: std::collections::BTreeSet<LedgerId>,
    nonce: u128,
    management: Option<management::ManagementOwner>,
    // Physical writers outlive every logical-session removal. A final handle
    // may join its disk thread only after the entire fleet has stopped; one
    // stalled session cannot block another by dropping the last writer handle.
    _wal_owners: Vec<focal_log::SharedWal>,
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
        if let Some(management) = &mut self.management {
            management.publish(self.sessions.len(), true);
        }
    }
    fn stop_session(&mut self, ledger: LedgerId) {
        if let Some(mut owner) = self.sessions.remove(&ledger) {
            self.deadlines.remove(&(owner.wake_at, ledger));
            self.unwoken.remove(&ledger);
            owner.close();
        }
        if let Some(management) = &mut self.management {
            management.publish(self.sessions.len(), false);
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
        let _ = self.scheduler.enqueue(metadata, Some(routed), 0);
        Ok(())
    }
    fn input(&mut self, input: FleetInput) -> Result<bool, LedgerError> {
        match input {
            FleetInput::Routed(routed) => {
                self.enqueue(routed)?;
                Ok(false)
            }
            FleetInput::Management(work) => {
                let Some(mut management) = self.management.take() else {
                    return Ok(false);
                };
                let stop = management.handle(self, work);
                self.management = Some(management);
                Ok(stop)
            }
        }
    }
    fn run_inner(&mut self, receiver: mpsc::Receiver<FleetInput>) -> Result<(), LedgerError> {
        loop {
            if self
                .management
                .as_ref()
                .is_some_and(|owner| !owner.has_manager())
                || (self.sessions.is_empty() && self.management.is_none())
            {
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
            let mut runnable = false;
            for _ in 0..SLICE {
                let sessions = &self.sessions;
                match self
                    .scheduler
                    .schedule_when(|ledger| {
                        // A stopping session takes no more work — except
                        // while its leader hands the log off, which is
                        // messages both ways: the heir's append responses
                        // say when it is caught up, and its vote request
                        // ends this replica's term.
                        sessions.get(&ledger).is_none_or(|owner| {
                            !owner.session.persistence_pending()
                                && (owner.stopping.is_none() || owner.handing_off.is_some())
                        })
                    })
                    .map_err(|_| LedgerError::Failed)?
                {
                    ScheduleOutcome::Work(mut dispatch) => {
                        let routed = dispatch.payload_mut().take().ok_or(LedgerError::Corrupt)?;
                        if let Some(owner) = self.sessions.get_mut(&routed.ledger) {
                            if owner.incarnation != routed.incarnation {
                                continue;
                            }
                            match owner.accept(routed.work) {
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
            if runnable {
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
            owner.waits_answered = owner.waits_answered.saturating_add(1);
        }
        self.due(ledger);
        if let Some(waiting) = self.unwoken.pop_first() {
            self.due(waiting);
        }
    }
}
