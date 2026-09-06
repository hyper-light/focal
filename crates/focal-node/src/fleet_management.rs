//! Trusted, bounded installation into an existing grouped worker. Management
//! receipts are process-local: callers reconcile durable placement before
//! constructing sessions after a restart or an unknown installation outcome.
use super::*;
use focal_log::{SharedWal, WalWriterId};
use focal_memory::OwnerId;

#[derive(Clone, Copy, Debug)]
pub struct ManagedFleetConfig {
    pub max_sessions: usize,
    pub management_queue: usize,
}
impl Default for ManagedFleetConfig {
    fn default() -> Self {
        Self {
            max_sessions: 1024,
            management_queue: 64,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FleetIncarnation {
    owner: OwnerId,
    sequence: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FleetStatus {
    pub latest_sequence: u64,
    pub installed: usize,
    pub running: usize,
    pub stopped: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum FleetError {
    #[error("fleet capacity exhausted")]
    Capacity,
    #[error("session does not match the installed node, tenant or writer policy")]
    InvalidSession,
    #[error("management operation conflicts with an installed incarnation or latest intent")]
    Conflict,
    #[error("only the latest management operation can be retried")]
    RetryExpired,
    #[error("management sequence must follow the latest accepted operation")]
    OutOfOrder,
    #[error("session must be stopped before its registration can be removed")]
    Running,
    #[error("fleet is unavailable; an accepted installation may have completed")]
    Unavailable,
}
/// Before admission, rejection returns the owned candidate. After admission,
/// reconcile with inspect/retry_install before opening another logical WAL lease.
pub struct FleetInstallFailure {
    pub error: FleetError,
    pub replica: Option<FleetReplica>,
}
impl std::fmt::Debug for FleetInstallFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FleetInstallFailure")
            .field("error", &self.error)
            .field("candidate_returned", &self.replica.is_some())
            .finish()
    }
}
#[derive(Clone)]
pub struct FleetInstallation {
    ledger: LedgerId,
    group: [u8; 16],
    incarnation: FleetIncarnation,
    config: ReplicaConfig,
    writer: WalWriterId,
    host: ReplicaHost,
}
impl FleetInstallation {
    pub fn ledger(&self) -> LedgerId {
        self.ledger
    }
    pub fn group(&self) -> [u8; 16] {
        self.group
    }
    pub fn incarnation(&self) -> FleetIncarnation {
        self.incarnation
    }
    pub fn host(&self) -> &ReplicaHost {
        &self.host
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FleetRemoval {
    pub ledger: LedgerId,
    pub incarnation: FleetIncarnation,
}
/// The management queue slot and response storage remain charged until delivery
/// is consumed. Cloned host handles retain their own incarnation allowance.
pub struct FleetReply<T> {
    value: T,
    _permit: ManagementPermit,
}
impl<T> FleetReply<T> {
    pub fn value(&self) -> &T {
        &self.value
    }
}
pub(in crate::fleet) struct ManagementPermit {
    _bytes: Allocation,
    _slot: Allocation,
}
type Reply<T> = oneshot::Sender<Result<FleetReply<T>, FleetError>>;
pub(in crate::fleet) enum ManagementWork {
    Install {
        sequence: u64,
        replica: Box<FleetReplica>,
        reply: Reply<FleetInstallation>,
        permit: ManagementPermit,
    },
    Retry {
        sequence: u64,
        reply: Reply<FleetInstallation>,
        permit: ManagementPermit,
    },
    Inspect {
        ledger: LedgerId,
        reply: Reply<Option<FleetInstallation>>,
        permit: ManagementPermit,
    },
    Remove {
        sequence: u64,
        removal: FleetRemoval,
        reply: Reply<FleetRemoval>,
        permit: ManagementPermit,
    },
    Quiesce {
        reply: oneshot::Sender<()>,
        _permit: ManagementPermit,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
        _permit: ManagementPermit,
    },
}
struct ManagementState {
    status: FleetStatus,
    quiesced: bool,
    // The sole bounded installation registry doubles as the read-only routing
    // table. No borrow escapes lookup; incarnation handles fence later dispatch.
    entries: BTreeMap<LedgerId, FleetInstallation>,
    node: u64,
    cluster: [u8; 16],
    limits: WireLimits,
    tenants: BTreeMap<TenantId, (MemoryBudget, MemoryBudget)>,
    writers: Vec<WalWriterId>,
    // This is the same physical queue lease held by grouped hosts and egress.
    // A removed session's watch cannot own storage shared by other sessions.
    _backing: std::sync::Arc<Allocation>,
}
#[derive(Clone)]
pub struct FleetManager {
    sender: mpsc::SyncSender<FleetInput>,
    state: watch::Receiver<ManagementState>,
    budget: MemoryBudget,
    slots: MemoryBudget,
}
impl FleetManager {
    pub fn status(&self) -> FleetStatus {
        self.state.borrow().status
    }
    /// Trusted in-process routing lookup. The caller must authorize the tenant
    /// before calling. A short watch borrow clones only the current fenced host;
    /// no owner round trip or mutable session access is involved.
    pub fn current_host(&self, ledger: LedgerId) -> Result<ReplicaHost, FleetError> {
        let state = self.state.borrow();
        if state.status.stopped || state.quiesced {
            return Err(FleetError::Unavailable);
        }
        let entry = state.entries.get(&ledger).ok_or(FleetError::Unavailable)?;
        if entry.host.progress().stopped {
            return Err(FleetError::Unavailable);
        }
        Ok(entry.host.clone())
    }
    /// Allocation-free traversal for node-owned background capabilities. A
    /// returned host keeps that exact installed incarnation through an exchange.
    pub(crate) fn next_host(&self, after: Option<LedgerId>) -> Option<(LedgerId, ReplicaHost)> {
        let state = self.state.borrow();
        if state.status.stopped || state.quiesced {
            return None;
        }
        let lower = after.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
        state
            .entries
            .range((lower, std::ops::Bound::Unbounded))
            .find(|(_, entry)| !entry.host.progress().stopped)
            .map(|(ledger, entry)| (*ledger, entry.host.clone()))
    }
    fn permit(&self) -> Result<ManagementPermit, FleetError> {
        if self.status().stopped {
            return Err(FleetError::Unavailable);
        }
        let slot = self
            .slots
            .reserve(BudgetKind::Control, BudgetLane::Completion, 1)
            .map_err(|_| FleetError::Capacity)?
            .commit();
        let bytes = size_of::<FleetReplica>()
            .checked_add(4096)
            .ok_or(FleetError::Capacity)?;
        let bytes = self
            .budget
            .reserve(BudgetKind::Control, BudgetLane::Completion, bytes)
            .map_err(|_| FleetError::Capacity)?
            .commit();
        Ok(ManagementPermit {
            _bytes: bytes,
            _slot: slot,
        })
    }
    fn validate(&self, candidate: &FleetReplica) -> Result<(), FleetError> {
        let state = self.state.borrow();
        if state.quiesced || state.status.stopped {
            return Err(FleetError::Unavailable);
        }
        let ledger = candidate.session.ledger();
        let Some((tenant, _)) = state.tenants.get(&ledger.tenant) else {
            return Err(FleetError::InvalidSession);
        };
        if ledger.session.is_zero()
            || candidate.session.status().node_id != state.node
            || candidate.session.cluster_id() != state.cluster
            || !candidate.session.is_budgeted_within(tenant)
            || !state
                .writers
                .contains(&candidate.session.shared_wal().writer_id())
            || candidate.config.queue_items < 4
            || candidate
                .session
                .active_route()
                .is_some_and(|route| route != candidate.config.route_epoch)
        {
            return Err(FleetError::InvalidSession);
        }
        ReplicaHost::validate(&candidate.config, &state.limits)
            .map_err(|_| FleetError::InvalidSession)
    }
    pub async fn install(
        &self,
        sequence: u64,
        replica: FleetReplica,
    ) -> Result<FleetReply<FleetInstallation>, FleetInstallFailure> {
        let permit = match self.validate(&replica).and_then(|()| self.permit()) {
            Ok(permit) => permit,
            Err(error) => {
                return Err(FleetInstallFailure {
                    error,
                    replica: Some(replica),
                });
            }
        };
        let (reply, response) = oneshot::channel();
        let input = FleetInput::Management(ManagementWork::Install {
            sequence,
            replica: Box::new(replica),
            reply,
            permit,
        });
        if let Err(error) = self.sender.try_send(input) {
            let (error, input) = match error {
                mpsc::TrySendError::Full(input) => (FleetError::Capacity, input),
                mpsc::TrySendError::Disconnected(input) => (FleetError::Unavailable, input),
            };
            let replica = match input {
                FleetInput::Management(ManagementWork::Install { replica, .. }) => Some(*replica),
                _ => None,
            };
            return Err(FleetInstallFailure { error, replica });
        }
        response
            .await
            .unwrap_or(Err(FleetError::Unavailable))
            .map_err(|error| FleetInstallFailure {
                error,
                replica: None,
            })
    }
    fn send(&self, work: ManagementWork) -> Result<(), FleetError> {
        self.sender
            .try_send(FleetInput::Management(work))
            .map_err(|error| match error {
                mpsc::TrySendError::Full(_) => FleetError::Capacity,
                mpsc::TrySendError::Disconnected(_) => FleetError::Unavailable,
            })
    }
    pub async fn retry_install(
        &self,
        sequence: u64,
    ) -> Result<FleetReply<FleetInstallation>, FleetError> {
        let permit = self.permit()?;
        let (reply, response) = oneshot::channel();
        self.send(ManagementWork::Retry {
            sequence,
            reply,
            permit,
        })?;
        response.await.unwrap_or(Err(FleetError::Unavailable))
    }
    pub async fn inspect(
        &self,
        ledger: LedgerId,
    ) -> Result<FleetReply<Option<FleetInstallation>>, FleetError> {
        let permit = self.permit()?;
        let (reply, response) = oneshot::channel();
        self.send(ManagementWork::Inspect {
            ledger,
            reply,
            permit,
        })?;
        response.await.unwrap_or(Err(FleetError::Unavailable))
    }
    /// Caller supplies committed unassignment authority. This only retires an
    /// already stopped incarnation; it never deletes or truncates its WAL.
    pub async fn remove(
        &self,
        sequence: u64,
        ledger: LedgerId,
        incarnation: FleetIncarnation,
    ) -> Result<FleetReply<FleetRemoval>, FleetError> {
        let permit = self.permit()?;
        let (reply, response) = oneshot::channel();
        self.send(ManagementWork::Remove {
            sequence,
            removal: FleetRemoval {
                ledger,
                incarnation,
            },
            reply,
            permit,
        })?;
        response.await.unwrap_or(Err(FleetError::Unavailable))
    }
    /// Quiesce routing and installation, then stop one current incarnation at a
    /// time without cloning the registry or holding its lock across an await.
    /// Finally stop the worker. Any interrupted host stop returns Unavailable
    /// after shutdown admission; its proposals retain unknown outcomes. Canceling
    /// this future leaves the fleet quiesced: resume it or call shutdown, then
    /// join the owner. The embedding service supplies its overall grace deadline.
    pub async fn stop_all(&self) -> Result<(), FleetError> {
        let permit = self.permit()?;
        let (reply, receive) = oneshot::channel();
        self.send(ManagementWork::Quiesce {
            reply,
            _permit: permit,
        })?;
        receive.await.map_err(|_| FleetError::Unavailable)?;
        let mut after = None;
        let mut complete = true;
        loop {
            let next = {
                use std::ops::Bound::{Excluded, Unbounded};
                let state = self.state.borrow();
                state
                    .entries
                    .range((after.map_or(Unbounded, Excluded), Unbounded))
                    .find(|(_, entry)| !entry.host.progress().stopped)
                    .map(|(ledger, entry)| (*ledger, entry.host.clone()))
            };
            let Some((ledger, host)) = next else {
                break;
            };
            after = Some(ledger);
            if host.stop().await.is_err() {
                complete = false;
            }
        }
        self.shutdown().await?;
        if complete {
            Ok(())
        } else {
            Err(FleetError::Unavailable)
        }
    }
    /// Stops the worker, preserving uncertain write outcomes. Joining the
    /// returned ReplicaOwner waits for physical WAL owner teardown.
    pub async fn shutdown(&self) -> Result<(), FleetError> {
        let permit = self.permit()?;
        let (reply, response) = oneshot::channel();
        self.send(ManagementWork::Shutdown {
            reply,
            _permit: permit,
        })?;
        response.await.map_err(|_| FleetError::Unavailable)
    }
}
enum Latest {
    Installed(Box<FleetInstallation>),
    Removed(FleetRemoval),
}
pub(super) struct ManagementOwner {
    state: watch::Sender<ManagementState>,
    latest: Option<Latest>,
    sequence: u64,
    owner: OwnerId,
    max_sessions: usize,
    sender: mpsc::SyncSender<FleetInput>,
    outbound: async_mpsc::Sender<ReplicationFrame>,
}
impl ManagementOwner {
    pub(super) fn has_manager(&self) -> bool {
        self.state.receiver_count() != 0
    }
    pub(super) fn publish(&mut self, running: usize, stopped: bool) {
        self.state.send_modify(|state| {
            state.status = FleetStatus {
                latest_sequence: self.sequence,
                installed: state.entries.len(),
                running,
                stopped,
            }
        });
    }
    fn next(&self, sequence: u64) -> Result<(), FleetError> {
        if sequence < self.sequence {
            return Err(FleetError::RetryExpired);
        }
        if sequence == self.sequence {
            return Err(FleetError::Conflict);
        }
        if self.sequence.checked_add(1) != Some(sequence) {
            return Err(FleetError::OutOfOrder);
        }
        Ok(())
    }
    fn retry(&self, sequence: u64) -> Result<FleetInstallation, FleetError> {
        if sequence < self.sequence {
            return Err(FleetError::RetryExpired);
        }
        if sequence != self.sequence {
            return Err(FleetError::OutOfOrder);
        }
        match &self.latest {
            Some(Latest::Installed(installation)) => Ok((**installation).clone()),
            _ => Err(FleetError::Conflict),
        }
    }
    fn install(
        &mut self,
        group: &mut GroupOwner,
        sequence: u64,
        replica: FleetReplica,
    ) -> Result<FleetInstallation, FleetError> {
        let ledger = replica.session.ledger();
        if sequence == self.sequence {
            let previous = self.retry(sequence)?;
            if previous.ledger == ledger
                && previous.group == replica.session.group_id()
                && previous.config == replica.config
                && previous.writer == replica.session.shared_wal().writer_id()
            {
                return Ok(previous);
            }
            return Err(FleetError::Conflict);
        }
        self.next(sequence)?;
        let state = self.state.borrow();
        if state.quiesced {
            return Err(FleetError::Unavailable);
        }
        if state.entries.contains_key(&ledger)
            || state
                .entries
                .values()
                .any(|entry| entry.group == replica.session.group_id())
        {
            return Err(FleetError::Conflict);
        }
        if state.entries.len() >= self.max_sessions {
            return Err(FleetError::Capacity);
        }
        let (tenant, items) = state
            .tenants
            .get(&ledger.tenant)
            .ok_or(FleetError::InvalidSession)?;
        let ingress = tenant
            .child(64 * 1024 * 1024, 24 * 1024 * 1024)
            .map_err(|_| FleetError::Capacity)?;
        let slots = items
            .child(replica.config.queue_items, replica.config.queue_items / 4)
            .map_err(|_| FleetError::Capacity)?;
        let metadata = size_of::<Owner>()
            .checked_add(4096)
            .ok_or(FleetError::Capacity)?;
        let allocation = tenant
            .reserve(BudgetKind::Control, BudgetLane::Completion, metadata)
            .map_err(|_| FleetError::Capacity)?
            .commit();
        let incarnation = FleetIncarnation {
            owner: self.owner,
            sequence,
        };
        let writer = replica.session.shared_wal().writer_id();
        let id = replica.session.group_id();
        let config = replica.config.clone();
        let sender = HostSender::Group {
            ledger,
            incarnation: sequence,
            sender: self.sender.clone(),
            slots,
            _backing: state._backing.clone(),
        };
        let (host, mut session) = ReplicaHost::assemble(
            replica.session,
            replica.config,
            state.limits.clone(),
            None,
            ingress,
            sender,
            self.outbound.clone(),
        )
        .map_err(|_| FleetError::Capacity)?;
        session
            .progress
            .send_modify(|progress| progress._allocation = Some(allocation));
        session.incarnation = sequence;
        session.nonblocking = true;
        let now = Instant::now();
        session.next_tick = now;
        session.wake_at = now;
        let installed = FleetInstallation {
            ledger,
            group: id,
            incarnation,
            config,
            writer,
            host,
        };
        group.sessions.insert(ledger, session);
        group.deadlines.insert((now, ledger), ());
        self.latest = Some(Latest::Installed(Box::new(installed.clone())));
        self.sequence = sequence;
        drop(state);
        self.state.send_modify(|state| {
            state.entries.insert(ledger, installed.clone());
            state.status = FleetStatus {
                latest_sequence: sequence,
                installed: state.entries.len(),
                running: group.sessions.len(),
                stopped: false,
            };
        });
        Ok(installed)
    }
    fn remove(
        &mut self,
        group: &GroupOwner,
        sequence: u64,
        removal: FleetRemoval,
    ) -> Result<FleetRemoval, FleetError> {
        if sequence == self.sequence {
            return match self.latest {
                Some(Latest::Removed(previous)) if previous == removal => Ok(previous),
                _ => Err(FleetError::Conflict),
            };
        }
        self.next(sequence)?;
        let state = self.state.borrow();
        let entry = state
            .entries
            .get(&removal.ledger)
            .ok_or(FleetError::Conflict)?;
        if entry.incarnation != removal.incarnation {
            return Err(FleetError::Conflict);
        }
        if group.sessions.contains_key(&removal.ledger) || !entry.host.progress().stopped {
            return Err(FleetError::Running);
        }
        drop(state);
        self.latest = Some(Latest::Removed(removal));
        self.sequence = sequence;
        let mut removed = None;
        self.state.send_modify(|state| {
            removed = state.entries.remove(&removal.ledger);
            state.status = FleetStatus {
                latest_sequence: sequence,
                installed: state.entries.len(),
                running: group.sessions.len(),
                stopped: false,
            };
        });
        // Retire handle allowances outside the routing lock.
        drop(removed);
        Ok(removal)
    }
    pub(super) fn handle(&mut self, group: &mut GroupOwner, work: ManagementWork) -> bool {
        match work {
            ManagementWork::Install {
                sequence,
                replica,
                reply,
                permit,
            } => {
                let result = self.install(group, sequence, *replica);
                let _ = reply.send(result.map(|value| FleetReply {
                    value,
                    _permit: permit,
                }));
            }
            ManagementWork::Retry {
                sequence,
                reply,
                permit,
            } => {
                let _ = reply.send(self.retry(sequence).map(|value| FleetReply {
                    value,
                    _permit: permit,
                }));
            }
            ManagementWork::Inspect {
                ledger,
                reply,
                permit,
            } => {
                let _ = reply.send(Ok(FleetReply {
                    value: self.state.borrow().entries.get(&ledger).cloned(),
                    _permit: permit,
                }));
            }
            ManagementWork::Remove {
                sequence,
                removal,
                reply,
                permit,
            } => {
                let result = self.remove(group, sequence, removal);
                let _ = reply.send(result.map(|value| FleetReply {
                    value,
                    _permit: permit,
                }));
            }
            ManagementWork::Quiesce { reply, .. } => {
                self.state.send_modify(|state| state.quiesced = true);
                let _ = reply.send(());
            }
            ManagementWork::Shutdown { reply, .. } => {
                let _ = reply.send(());
                return true;
            }
        }
        false
    }
}
impl ReplicaFleet {
    /// Creates one initially empty worker. Physical writers are retained for its
    /// entire lifetime, and only sessions built with those exact writers may be
    /// admitted. Construct this trusted owner outside a latency-sensitive worker.
    pub fn spawn_managed(
        node: u64,
        cluster: [u8; 16],
        writers: Vec<SharedWal>,
        tenants: Vec<FleetTenant>,
        budget: MemoryBudget,
        limits: WireLimits,
        config: ManagedFleetConfig,
    ) -> Result<(FleetManager, ReplicaOwner, FleetReplication), LedgerError> {
        if node == 0
            || cluster == [0; 16]
            || writers.is_empty()
            || writers.len() > 64
            || tenants.is_empty()
            || tenants.len() > 1024
            || !(1..=4096).contains(&config.max_sessions)
            || !(1..=RESERVED).contains(&config.management_queue)
        {
            return Err(LedgerError::Capacity);
        }
        limits.validate().map_err(|_| LedgerError::Capacity)?;
        let owner = OwnerId::new().map_err(|_| LedgerError::Capacity)?;
        let policy_bytes = tenants
            .len()
            .checked_mul(512)
            .and_then(|bytes| {
                writers
                    .len()
                    .checked_mul(64)
                    .and_then(|writers| bytes.checked_add(writers))
            })
            .and_then(|bytes| bytes.checked_add(4096))
            .ok_or(LedgerError::Capacity)?;
        let bookkeeping = STACK_BYTES
            .checked_add(policy_bytes)
            .ok_or(LedgerError::Capacity)?;
        let allocation = budget
            .reserve(BudgetKind::Control, BudgetLane::Completion, bookkeeping)?
            .commit();
        let mut writer_ids = Vec::new();
        writer_ids
            .try_reserve_exact(writers.len())
            .map_err(|_| LedgerError::Capacity)?;
        for writer in &writers {
            let identity = writer.identity().map_err(|_| LedgerError::Failed)?;
            if identity.node != node
                || identity.cluster != cluster
                || !writer.is_budgeted_within(&budget)
                || writer_ids.contains(&writer.writer_id())
            {
                return Err(LedgerError::Capacity);
            }
            writer_ids.push(writer.writer_id());
        }
        let shared_bytes = size_of::<FleetInput>()
            .checked_add(size_of::<ReplicationFrame>())
            .and_then(|size| size.checked_add(128))
            .and_then(|size| QUEUED.checked_mul(size))
            .and_then(|bytes| bytes.checked_add(policy_bytes))
            .ok_or(LedgerError::Capacity)?;
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
        let slots = item_budget.child(config.management_queue, config.management_queue)?;
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
                        max_sessions: config.max_sessions,
                    },
                )
                .map_err(|_| LedgerError::Capacity)?;
            tenant_budgets.insert(tenant.tenant, (tenant.budget, item_budget.child(256, 32)?));
        }
        let (sender, receiver) = mpsc::sync_channel(QUEUED);
        let (outbound, outgoing) = async_mpsc::channel(QUEUED);
        let (state, changes) = watch::channel(ManagementState {
            quiesced: false,
            entries: BTreeMap::new(),
            status: FleetStatus {
                latest_sequence: 0,
                installed: 0,
                running: 0,
                stopped: false,
            },
            node,
            cluster,
            limits,
            tenants: tenant_budgets,
            writers: writer_ids,
            _backing: backing.clone(),
        });
        let manager = FleetManager {
            sender: sender.clone(),
            state: changes,
            budget,
            slots,
        };
        let management = ManagementOwner {
            state,
            latest: None,
            sequence: 0,
            owner,
            max_sessions: config.max_sessions,
            sender,
            outbound,
        };
        let group = GroupOwner {
            sessions: BTreeMap::new(),
            deadlines: BTreeMap::new(),
            scheduler,
            nonce: 0,
            management: Some(management),
            _wal_owners: writers,
            _allocation: allocation,
            _backing: backing.clone(),
        };
        let thread = std::thread::Builder::new()
            .name(format!("focal-node-sessions-{node}"))
            .stack_size(STACK_BYTES)
            .spawn(move || group.run(receiver))
            .map_err(|_| LedgerError::Capacity)?;
        Ok((
            manager,
            ReplicaOwner(thread),
            FleetReplication {
                receiver: outgoing,
                _backing: backing,
            },
        ))
    }
}
