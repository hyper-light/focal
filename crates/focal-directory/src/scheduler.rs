use crate::*;
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use focal_model::{LedgerId, TenantId};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Weak},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(usize)]
pub enum WorkClass {
    Append,
    Transfer,
    Query,
    Apply,
    Completion,
    Control,
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Lane {
    Ordinary,
    Priority,
}
impl WorkClass {
    fn lane(self) -> Lane {
        match self {
            Self::Append | Self::Transfer | Self::Query => Lane::Ordinary,
            _ => Lane::Priority,
        }
    }
    fn next(self) -> Self {
        match self {
            Self::Append => Self::Transfer,
            Self::Transfer => Self::Query,
            Self::Query => Self::Append,
            Self::Apply => Self::Completion,
            Self::Completion => Self::Control,
            Self::Control => Self::Apply,
        }
    }
}
struct Lanes<T> {
    ordinary: T,
    priority: T,
}
impl<T> Lanes<T> {
    fn get(&self, lane: Lane) -> &T {
        match lane {
            Lane::Ordinary => &self.ordinary,
            Lane::Priority => &self.priority,
        }
    }
    fn get_mut(&mut self, lane: Lane) -> &mut T {
        match lane {
            Lane::Ordinary => &mut self.ordinary,
            Lane::Priority => &mut self.priority,
        }
    }
    fn iter(&self) -> impl Iterator<Item = &T> {
        [&self.ordinary, &self.priority].into_iter()
    }
}
struct Classes<T> {
    append: T,
    transfer: T,
    query: T,
    apply: T,
    completion: T,
    control: T,
}
impl<T> Classes<T> {
    fn from_fn(mut make: impl FnMut() -> T) -> Self {
        Self {
            append: make(),
            transfer: make(),
            query: make(),
            apply: make(),
            completion: make(),
            control: make(),
        }
    }
    fn get(&self, class: WorkClass) -> &T {
        match class {
            WorkClass::Append => &self.append,
            WorkClass::Transfer => &self.transfer,
            WorkClass::Query => &self.query,
            WorkClass::Apply => &self.apply,
            WorkClass::Completion => &self.completion,
            WorkClass::Control => &self.control,
        }
    }
    fn get_mut(&mut self, class: WorkClass) -> &mut T {
        match class {
            WorkClass::Append => &mut self.append,
            WorkClass::Transfer => &mut self.transfer,
            WorkClass::Query => &mut self.query,
            WorkClass::Apply => &mut self.apply,
            WorkClass::Completion => &mut self.completion,
            WorkClass::Control => &mut self.control,
        }
    }
}
const QUOTA_BOOKKEEPING: usize = 512;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct WorkKey {
    pub ledger: LedgerId,
    pub id: WorkId,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkMetadata {
    pub key: WorkKey,
    pub class: WorkClass,
    pub cost: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct TenantQuota {
    pub weight: u32,
    pub max_items: usize,
    pub reserved_items: usize,
    pub max_bytes: usize,
    pub reserved_bytes: usize,
    pub session_items: usize,
    pub session_reserved_items: usize,
    pub session_bytes: usize,
    pub session_reserved_bytes: usize,
    pub max_sessions: usize,
}
#[derive(Debug, Clone, Copy)]
pub struct SchedulerConfig {
    pub max_tenants: usize,
    pub max_items: usize,
    pub reserved_items: usize,
    pub quantum: u64,
    pub max_cost: u64,
    pub max_weight: u32,
    pub max_visits: usize,
    pub priority_burst: usize,
}
impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            max_tenants: 1024,
            max_items: 16_384,
            reserved_items: 1024,
            quantum: 1024,
            max_cost: 65_536,
            max_weight: 32,
            max_visits: 1024,
            priority_burst: 8,
        }
    }
}

struct SessionQuota {
    bytes: MemoryBudget,
    items: MemoryBudget,
    _metadata: Allocation,
    _tenant_metadata: Allocation,
    _session_metadata: Allocation,
}
struct JobLease {
    _node_bytes: Allocation,
    _tenant_bytes: Allocation,
    _session_bytes: Allocation,
    _node_items: Allocation,
    _tenant_items: Allocation,
    _session_items: Allocation,
}
struct Queued<T> {
    metadata: WorkMetadata,
    payload: T,
    lease: Arc<JobLease>,
}
struct JobIndex {
    class: WorkClass,
    lease: Weak<JobLease>,
    _metadata: Allocation,
    _tenant_metadata: Allocation,
    _session_metadata: Allocation,
}
struct QueueBuffer {
    _node: Allocation,
    _tenant: Allocation,
}
struct TenantQueues<T> {
    quota: TenantQuota,
    bytes: MemoryBudget,
    items: MemoryBudget,
    sessions: BTreeMap<LedgerId, SessionQuota>,
    queues: Classes<VecDeque<Queued<T>>>,
    queue_allocations: Classes<Option<QueueBuffer>>,
    lane_items: Lanes<usize>,
    deficit: Lanes<u64>,
    needs_quantum: Lanes<bool>,
    next_class: Lanes<WorkClass>,
    _metadata: Allocation,
}

/// The permits remain held while a worker owns this dispatch, including after
/// removal from the ready queues. Drop releases all node/tenant/session quotas.
pub struct Dispatch<T> {
    pub metadata: WorkMetadata,
    payload: T,
    _lease: Arc<JobLease>,
}
impl<T> Dispatch<T> {
    pub fn payload(&self) -> &T {
        &self.payload
    }
    pub fn payload_mut(&mut self) -> &mut T {
        &mut self.payload
    }
}
pub enum ScheduleOutcome<T> {
    Work(Dispatch<T>),
    Idle,
    /// Work exists but this bounded visit slice exhausted its allowance.
    /// The runtime must schedule another slice, without waiting for a new hint.
    Continue,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueUsage {
    pub items: usize,
    pub bytes: usize,
    pub queued: usize,
    pub sessions: usize,
}

/// Deterministic weighted deficit round-robin over active tenants, with class
/// rotation inside each tenant. Reserved apply/completion/control traffic gets
/// a bounded priority burst; ordinary work receives service under continuous
/// control traffic. There are no per-session workers or idle polling tasks.
pub struct FairScheduler<T> {
    config: SchedulerConfig,
    budget: MemoryBudget,
    items: MemoryBudget,
    tenants: BTreeMap<TenantId, TenantQueues<T>>,
    ready: Lanes<VecDeque<TenantId>>,
    jobs: BTreeMap<WorkKey, JobIndex>,
    priority_used: usize,
    _metadata: Allocation,
}

impl<T> FairScheduler<T> {
    pub fn new(config: SchedulerConfig, budget: MemoryBudget) -> Result<Self, DirectoryError> {
        if config.max_tenants == 0
            || config.max_items == 0
            || config.reserved_items > config.max_items
            || config.quantum == 0
            || config.max_cost == 0
            || config.max_weight == 0
            || config.max_visits == 0
            || config.priority_burst == 0
        {
            return Err(DirectoryError::Invalid("scheduler configuration"));
        }
        config
            .quantum
            .checked_mul(u64::from(config.max_weight))
            .and_then(|n| n.checked_add(config.max_cost))
            .ok_or(DirectoryError::CounterExhausted)?;
        let metadata = budget
            .reserve(
                BudgetKind::Control,
                BudgetLane::Completion,
                add(
                    add(
                        size_of::<Self>(),
                        QUOTA_BOOKKEEPING.saturating_add(ALLOCATOR_OVERHEAD.saturating_mul(2)),
                    )?,
                    mul(config.max_tenants, size_of::<TenantId>().saturating_mul(2))?,
                )?,
            )?
            .commit();
        let mut ready = Lanes {
            ordinary: VecDeque::new(),
            priority: VecDeque::new(),
        };
        for queue in [&mut ready.ordinary, &mut ready.priority] {
            queue
                .try_reserve_exact(config.max_tenants)
                .map_err(|_| DirectoryError::Capacity)?;
        }
        Ok(Self {
            config,
            budget,
            items: MemoryBudget::new(config.max_items, config.reserved_items)?,
            tenants: BTreeMap::new(),
            ready,
            jobs: BTreeMap::new(),
            priority_used: 0,
            _metadata: metadata,
        })
    }
    pub fn register_tenant(
        &mut self,
        tenant: TenantId,
        quota: TenantQuota,
    ) -> Result<(), DirectoryError> {
        if self.tenants.contains_key(&tenant) {
            return Err(DirectoryError::Duplicate);
        }
        if self.tenants.len() >= self.config.max_tenants {
            return Err(DirectoryError::Capacity);
        }
        if quota.weight == 0
            || quota.weight > self.config.max_weight
            || quota.max_sessions == 0
            || quota.session_items == 0
            || quota.session_bytes == 0
            || quota.session_reserved_items > quota.session_items
            || quota.session_reserved_bytes > quota.session_bytes
        {
            return Err(DirectoryError::Invalid("tenant quota"));
        }
        let metadata = self
            .budget
            .reserve(
                BudgetKind::Control,
                BudgetLane::Completion,
                add(
                    tree_row::<(TenantId, TenantQueues<T>)>(),
                    QUOTA_BOOKKEEPING.saturating_mul(2),
                )?,
            )?
            .commit();
        let bytes = MemoryBudget::new(quota.max_bytes, quota.reserved_bytes)?;
        let items = MemoryBudget::new(quota.max_items, quota.reserved_items)?;
        self.tenants.insert(
            tenant,
            TenantQueues {
                quota,
                bytes,
                items,
                sessions: BTreeMap::new(),
                queues: Classes::from_fn(VecDeque::new),
                queue_allocations: Classes::from_fn(|| None),
                lane_items: Lanes {
                    ordinary: 0,
                    priority: 0,
                },
                deficit: Lanes {
                    ordinary: 0,
                    priority: 0,
                },
                needs_quantum: Lanes {
                    ordinary: true,
                    priority: true,
                },
                next_class: Lanes {
                    ordinary: WorkClass::Append,
                    priority: WorkClass::Apply,
                },
                _metadata: metadata,
            },
        );
        Ok(())
    }
    pub fn usage(&self, tenant: TenantId) -> Option<QueueUsage> {
        self.tenants.get(&tenant).map(|entry| QueueUsage {
            items: entry.items.stats().used,
            bytes: entry.bytes.stats().used,
            queued: entry.lane_items.iter().sum(),
            sessions: entry.sessions.len(),
        })
    }
    pub fn pending_items(&self) -> usize {
        self.tenants
            .values()
            .map(|entry| entry.lane_items.iter().sum::<usize>())
            .sum()
    }
    pub fn active_items(&self) -> usize {
        self.items.stats().used
    }

    /// Payload heap capacity is supplied by the caller; inline payload/queue
    /// capacity, index nodes, and permits are charged by the scheduler.
    /// Every rejection drops partial reservations before returning.
    pub fn enqueue(
        &mut self,
        metadata: WorkMetadata,
        payload: T,
        payload_heap_bytes: usize,
    ) -> Result<(), DirectoryError> {
        self.reap_finished();
        if metadata.cost == 0 || metadata.cost > self.config.max_cost {
            return Err(DirectoryError::Invalid(
                "work cost outside scheduler bounds",
            ));
        }
        if self.jobs.contains_key(&metadata.key) {
            return Err(DirectoryError::Duplicate);
        }
        let lane = metadata.class.lane();
        let budget_lane = if lane == Lane::Ordinary {
            BudgetLane::Ordinary
        } else {
            BudgetLane::Completion
        };
        let tenant_id = metadata.key.ledger.tenant;
        let tenant = self
            .tenants
            .get_mut(&tenant_id)
            .ok_or(DirectoryError::Missing)?;
        tenant
            .sessions
            .retain(|_, session| session.items.stats().used != 0);
        let provisional = if !tenant.sessions.contains_key(&metadata.key.ledger) {
            if tenant.sessions.len() >= tenant.quota.max_sessions {
                return Err(DirectoryError::Capacity);
            }
            let overhead = add(
                tree_row::<(LedgerId, SessionQuota)>(),
                QUOTA_BOOKKEEPING.saturating_mul(2),
            )?;
            let charge = self
                .budget
                .reserve(BudgetKind::Pending, budget_lane, overhead)?
                .commit();
            let tenant_charge = tenant
                .bytes
                .reserve(BudgetKind::Pending, budget_lane, overhead)?
                .commit();
            let bytes = MemoryBudget::new(
                tenant.quota.session_bytes,
                tenant.quota.session_reserved_bytes,
            )?;
            let session_charge = bytes
                .reserve(BudgetKind::Pending, budget_lane, overhead)?
                .commit();
            Some(SessionQuota {
                bytes,
                items: MemoryBudget::new(
                    tenant.quota.session_items,
                    tenant.quota.session_reserved_items,
                )?,
                _metadata: charge,
                _tenant_metadata: tenant_charge,
                _session_metadata: session_charge,
            })
        } else {
            None
        };
        let session = provisional
            .as_ref()
            .or_else(|| tenant.sessions.get(&metadata.key.ledger))
            .ok_or(DirectoryError::Missing)?;
        // Dispatches can outlive the scheduler and keep six quota counter
        // allocations alive. Charge those backings with every live lease.
        let charge = add(
            payload_heap_bytes,
            add(
                mul(2, size_of::<Queued<T>>())?,
                add(
                    size_of::<JobLease>(),
                    ALLOCATOR_OVERHEAD
                        .saturating_mul(3)
                        .saturating_add(QUOTA_BOOKKEEPING.saturating_mul(6)),
                )?,
            )?,
        )?;
        let node_bytes = self
            .budget
            .reserve(BudgetKind::Pending, budget_lane, charge)?
            .commit();
        let tenant_bytes = tenant
            .bytes
            .reserve(BudgetKind::Pending, budget_lane, charge)?
            .commit();
        let session_bytes = session
            .bytes
            .reserve(BudgetKind::Pending, budget_lane, charge)?
            .commit();
        let node_items = self
            .items
            .reserve(BudgetKind::Pending, budget_lane, 1)?
            .commit();
        let tenant_items = tenant
            .items
            .reserve(BudgetKind::Pending, budget_lane, 1)?
            .commit();
        let session_items = session
            .items
            .reserve(BudgetKind::Pending, budget_lane, 1)?
            .commit();
        // A retained Weak keeps the lease control allocation alive; the index
        // separately charges that metadata until reaping removes its last Weak.
        let index_charge = add(
            tree_row::<(WorkKey, JobIndex)>(),
            add(size_of::<JobLease>(), ALLOCATOR_OVERHEAD)?,
        )?;
        let index_metadata = self
            .budget
            .reserve(BudgetKind::Index, budget_lane, index_charge)?
            .commit();
        let tenant_index = tenant
            .bytes
            .reserve(BudgetKind::Index, budget_lane, index_charge)?
            .commit();
        let session_index = session
            .bytes
            .reserve(BudgetKind::Index, budget_lane, index_charge)?
            .commit();
        let queue = tenant.queues.get_mut(metadata.class);
        let new_queue_allocation = if queue.len() == queue.capacity() {
            let capacity = queue
                .capacity()
                .max(1)
                .checked_mul(2)
                .ok_or(DirectoryError::CounterExhausted)?
                .min(tenant.quota.max_items)
                .min(self.config.max_items);
            let bytes = add(ALLOCATOR_OVERHEAD, mul(capacity, size_of::<Queued<T>>())?)?;
            let allocation = self
                .budget
                .reserve(BudgetKind::Pending, budget_lane, bytes)?
                .commit();
            let tenant_allocation = tenant
                .bytes
                .reserve(BudgetKind::Pending, budget_lane, bytes)?
                .commit();
            queue
                .try_reserve_exact(capacity.saturating_sub(queue.len()))
                .map_err(|_| DirectoryError::Capacity)?;
            Some(QueueBuffer {
                _node: allocation,
                _tenant: tenant_allocation,
            })
        } else {
            None
        };
        let lease = Arc::new(JobLease {
            _node_bytes: node_bytes,
            _tenant_bytes: tenant_bytes,
            _session_bytes: session_bytes,
            _node_items: node_items,
            _tenant_items: tenant_items,
            _session_items: session_items,
        });
        if let Some(session) = provisional {
            tenant.sessions.insert(metadata.key.ledger, session);
        }
        if let Some(allocation) = new_queue_allocation {
            *tenant.queue_allocations.get_mut(metadata.class) = Some(allocation);
        }
        if *tenant.lane_items.get_mut(lane) == 0 {
            self.ready.get_mut(lane).push_back(tenant_id);
            *tenant.needs_quantum.get_mut(lane) = true;
        }
        *tenant.lane_items.get_mut(lane) = tenant
            .lane_items
            .get(lane)
            .checked_add(1)
            .ok_or(DirectoryError::CounterExhausted)?;
        self.jobs.insert(
            metadata.key,
            JobIndex {
                class: metadata.class,
                lease: Arc::downgrade(&lease),
                _metadata: index_metadata,
                _tenant_metadata: tenant_index,
                _session_metadata: session_index,
            },
        );
        queue.push_back(Queued {
            metadata,
            payload,
            lease,
        });
        Ok(())
    }

    pub fn cancel(&mut self, key: WorkKey) -> Result<(), DirectoryError> {
        self.reap_finished();
        let index = self.jobs.get(&key).ok_or(DirectoryError::Missing)?;
        let tenant = self
            .tenants
            .get_mut(&key.ledger.tenant)
            .ok_or(DirectoryError::Missing)?;
        let queue = tenant.queues.get_mut(index.class);
        let position = queue
            .iter()
            .position(|item| item.metadata.key == key)
            .ok_or(DirectoryError::InFlight)?;
        let job = queue.remove(position).ok_or(DirectoryError::Missing)?;
        if queue.is_empty() {
            *queue = VecDeque::new();
            *tenant.queue_allocations.get_mut(index.class) = None;
        }
        let lane = job.metadata.class.lane();
        *tenant.lane_items.get_mut(lane) = tenant
            .lane_items
            .get(lane)
            .checked_sub(1)
            .ok_or(DirectoryError::Missing)?;
        if *tenant.lane_items.get_mut(lane) == 0 {
            self.ready
                .get_mut(lane)
                .retain(|id| *id != key.ledger.tenant);
            *tenant.deficit.get_mut(lane) = 0;
        }
        self.jobs.remove(&key);
        drop(job);
        self.reap_finished();
        Ok(())
    }

    pub fn schedule(&mut self) -> Result<ScheduleOutcome<T>, DirectoryError> {
        self.schedule_when(|_| true)
    }

    /// Retain blocked sessions in their original queues with every quota held.
    /// Eligibility must stay stable during this call. Skipping a session neither
    /// consumes its deficit nor prevents another session in that tenant from
    /// running. Visits and queued scans remain bounded by configured limits.
    pub fn schedule_when(
        &mut self,
        mut eligible: impl FnMut(LedgerId) -> bool,
    ) -> Result<ScheduleOutcome<T>, DirectoryError> {
        self.reap_finished();
        let ordinary = !self.ready.ordinary.is_empty();
        let priority = !self.ready.priority.is_empty();
        if !ordinary && !priority {
            return Ok(ScheduleOutcome::Idle);
        }
        let lane = if priority && (!ordinary || self.priority_used < self.config.priority_burst) {
            Lane::Priority
        } else {
            Lane::Ordinary
        };
        let other = if lane == Lane::Priority {
            Lane::Ordinary
        } else {
            Lane::Priority
        };
        match self.schedule_lane(lane, &mut eligible)? {
            ScheduleOutcome::Idle => self.schedule_lane(other, &mut eligible),
            ScheduleOutcome::Continue => match self.schedule_lane(other, &mut eligible)? {
                ScheduleOutcome::Work(work) => Ok(ScheduleOutcome::Work(work)),
                _ => Ok(ScheduleOutcome::Continue),
            },
            result => Ok(result),
        }
    }

    fn schedule_lane(
        &mut self,
        lane: Lane,
        eligible: &mut impl FnMut(LedgerId) -> bool,
    ) -> Result<ScheduleOutcome<T>, DirectoryError> {
        if self.ready.get(lane).is_empty() {
            return Ok(ScheduleOutcome::Idle);
        }
        let mut blocked = 0usize;
        for _ in 0..self.config.max_visits {
            let tenant_id = self
                .ready
                .get_mut(lane)
                .pop_front()
                .ok_or(DirectoryError::Missing)?;
            let tenant = self
                .tenants
                .get_mut(&tenant_id)
                .ok_or(DirectoryError::Missing)?;
            let mut class = *tenant.next_class.get(lane);
            let mut selected = None;
            for _ in 0..3 {
                if let Some(position) = tenant
                    .queues
                    .get(class)
                    .iter()
                    .position(|job| eligible(job.metadata.key.ledger))
                {
                    selected = Some(position);
                    break;
                }
                class = class.next();
            }
            let Some(position) = selected else {
                self.ready.get_mut(lane).push_back(tenant_id);
                blocked = blocked
                    .checked_add(1)
                    .ok_or(DirectoryError::CounterExhausted)?;
                if blocked == self.ready.get(lane).len() {
                    return Ok(ScheduleOutcome::Idle);
                }
                continue;
            };
            blocked = 0;
            if *tenant.needs_quantum.get(lane) {
                let quantum = self
                    .config
                    .quantum
                    .checked_mul(u64::from(tenant.quota.weight))
                    .ok_or(DirectoryError::CounterExhausted)?;
                *tenant.deficit.get_mut(lane) = tenant
                    .deficit
                    .get(lane)
                    .checked_add(quantum)
                    .ok_or(DirectoryError::CounterExhausted)?;
                *tenant.needs_quantum.get_mut(lane) = false;
            }
            let cost = tenant
                .queues
                .get(class)
                .get(position)
                .ok_or(DirectoryError::Missing)?
                .metadata
                .cost;
            if cost > *tenant.deficit.get(lane) {
                *tenant.needs_quantum.get_mut(lane) = true;
                self.ready.get_mut(lane).push_back(tenant_id);
                continue;
            }
            *tenant.deficit.get_mut(lane) = tenant
                .deficit
                .get(lane)
                .checked_sub(cost)
                .ok_or(DirectoryError::Missing)?;
            *tenant.next_class.get_mut(lane) = class.next();
            let job = tenant
                .queues
                .get_mut(class)
                .remove(position)
                .ok_or(DirectoryError::Missing)?;
            if tenant.queues.get(class).is_empty() {
                *tenant.queues.get_mut(class) = VecDeque::new();
                *tenant.queue_allocations.get_mut(class) = None;
            }
            *tenant.lane_items.get_mut(lane) = tenant
                .lane_items
                .get(lane)
                .checked_sub(1)
                .ok_or(DirectoryError::Missing)?;
            if *tenant.lane_items.get_mut(lane) == 0 {
                *tenant.deficit.get_mut(lane) = 0;
                *tenant.needs_quantum.get_mut(lane) = true;
            } else {
                self.ready.get_mut(lane).push_front(tenant_id);
            }
            if lane == Lane::Priority {
                self.priority_used = self
                    .priority_used
                    .saturating_add(1)
                    .min(self.config.priority_burst);
            } else {
                self.priority_used = 0;
            }
            return Ok(ScheduleOutcome::Work(Dispatch {
                metadata: job.metadata,
                payload: job.payload,
                _lease: job.lease,
            }));
        }
        Ok(ScheduleOutcome::Continue)
    }

    /// Reclaim only finished work metadata and empty session machinery. Live
    /// dispatch permits keep their quotas and prevent premature slot reuse.
    pub fn reap_finished(&mut self) {
        self.jobs.retain(|_, job| job.lease.strong_count() != 0);
        for tenant in self.tenants.values_mut() {
            tenant
                .sessions
                .retain(|_, session| session.items.stats().used != 0);
        }
    }
    pub fn remove_idle_tenant(&mut self, tenant: TenantId) -> Result<(), DirectoryError> {
        self.reap_finished();
        let entry = self.tenants.get(&tenant).ok_or(DirectoryError::Missing)?;
        if entry.items.stats().used != 0 {
            return Err(DirectoryError::InFlight);
        }
        self.tenants.remove(&tenant);
        Ok(())
    }
}
