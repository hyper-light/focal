//! Several independent session logs share one node worker, a bounded ingress
//! queue and tenant-fair scheduling. Session identities and commit order stay
//! independent; a stopped session does not stop unrelated healthy sessions.
use super::*;
use focal_directory::{
    FairScheduler, ScheduleOutcome, SchedulerConfig, TenantQuota, WorkClass, WorkId, WorkKey,
    WorkMetadata,
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
pub(super) fn lane(work: &Work) -> BudgetLane {
    match class(work) {
        WorkClass::Apply | WorkClass::Control | WorkClass::Completion => BudgetLane::Completion,
        _ => BudgetLane::Ordinary,
    }
}
fn class(work: &Work) -> WorkClass {
    match work {
        Work::Stop(_)
        | Work::Transfer(..)
        | Work::ManagedSupport(..)
        | Work::Membership(..)
        | Work::Placement(..)
        | Work::Evidence(..) => WorkClass::Control,
        Work::Probe(request, ..) if completion_request(request) => WorkClass::Completion,
        Work::Probe(..) => WorkClass::Query,
        Work::Request(request, ..) => match request.verified.request().operation {
            Operation::Raft { .. } => WorkClass::Apply,
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
        let (sender, receiver) = mpsc::sync_channel(QUEUED);
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
    nonce: u128,
    management: Option<management::ManagementOwner>,
    // Physical writers outlive every logical-session removal. A final handle
    // may join its disk thread only after the entire fleet has stopped; one
    // stalled session cannot block another by dropping the last writer handle.
    _wal_owners: Vec<focal_log::SharedWal>,
    _allocation: Allocation,
    _backing: std::sync::Arc<Allocation>,
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
        }
        Ok(())
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
        if owner.stopping.is_some() {
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
                        Ok(true) | Err(_) => self.stop_session(ledger),
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
                        sessions.get(&ledger).is_none_or(|owner| {
                            !owner.session.persistence_pending() && owner.stopping.is_none()
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
                                Ok(true) | Err(_) => self.stop_session(routed.ledger),
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
            match receiver.recv_timeout(wait) {
                Ok(input) => {
                    if self.input(input)? {
                        return Ok(());
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => return Ok(()),
            }
        }
    }
}
