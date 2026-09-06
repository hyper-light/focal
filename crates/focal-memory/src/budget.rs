use crate::MemoryError;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

/// Ordinary work cannot consume the reserved completion/control allowance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BudgetLane {
    Ordinary,
    Completion,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
pub enum BudgetKind {
    Arena,
    Payload,
    Index,
    Pages,
    Roots,
    ReadPins,
    Pending,
    Query,
    Monitor,
    Dedup,
    Timer,
    Recovery,
    Control,
}

const KIND_COUNT: usize = BudgetKind::Control as usize + 1;

// Named counters make category access total, including during Drop.
#[derive(Debug)]
struct KindCounters {
    arena: AtomicUsize,
    payload: AtomicUsize,
    index: AtomicUsize,
    pages: AtomicUsize,
    roots: AtomicUsize,
    readpins: AtomicUsize,
    pending: AtomicUsize,
    query: AtomicUsize,
    monitor: AtomicUsize,
    dedup: AtomicUsize,
    timer: AtomicUsize,
    recovery: AtomicUsize,
    control: AtomicUsize,
}
impl KindCounters {
    fn new() -> Self {
        Self {
            arena: AtomicUsize::new(0),
            payload: AtomicUsize::new(0),
            index: AtomicUsize::new(0),
            pages: AtomicUsize::new(0),
            roots: AtomicUsize::new(0),
            readpins: AtomicUsize::new(0),
            pending: AtomicUsize::new(0),
            query: AtomicUsize::new(0),
            monitor: AtomicUsize::new(0),
            dedup: AtomicUsize::new(0),
            timer: AtomicUsize::new(0),
            recovery: AtomicUsize::new(0),
            control: AtomicUsize::new(0),
        }
    }
    fn get(&self, kind: BudgetKind) -> &AtomicUsize {
        match kind {
            BudgetKind::Arena => &self.arena,
            BudgetKind::Payload => &self.payload,
            BudgetKind::Index => &self.index,
            BudgetKind::Pages => &self.pages,
            BudgetKind::Roots => &self.roots,
            BudgetKind::ReadPins => &self.readpins,
            BudgetKind::Pending => &self.pending,
            BudgetKind::Query => &self.query,
            BudgetKind::Monitor => &self.monitor,
            BudgetKind::Dedup => &self.dedup,
            BudgetKind::Timer => &self.timer,
            BudgetKind::Recovery => &self.recovery,
            BudgetKind::Control => &self.control,
        }
    }
    fn snapshot(&self) -> [usize; KIND_COUNT] {
        [
            self.arena.load(Ordering::Acquire),
            self.payload.load(Ordering::Acquire),
            self.index.load(Ordering::Acquire),
            self.pages.load(Ordering::Acquire),
            self.roots.load(Ordering::Acquire),
            self.readpins.load(Ordering::Acquire),
            self.pending.load(Ordering::Acquire),
            self.query.load(Ordering::Acquire),
            self.monitor.load(Ordering::Acquire),
            self.dedup.load(Ordering::Acquire),
            self.timer.load(Ordering::Acquire),
            self.recovery.load(Ordering::Acquire),
            self.control.load(Ordering::Acquire),
        ]
    }
}

#[derive(Debug)]
struct Counters {
    limit: usize,
    ordinary_limit: usize,
    total: AtomicUsize,
    ordinary: AtomicUsize,
    kinds: KindCounters,
    // One ancestor handle per budget, not per allocation. A child may outlive
    // its registering owner while detached replies still hold its permits.
    parent: Option<MemoryBudget>,
    depth: u8,
}

/// Shareable accounting only: there is no shared mutable graph behind this
/// handle. Reservations from independent owners use atomic bounded counters.
#[derive(Clone, Debug)]
pub struct MemoryBudget(Arc<Counters>);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BudgetStats {
    pub limit: usize,
    pub completion_reserve: usize,
    pub used: usize,
    pub ordinary_used: usize,
    pub by_kind: [usize; KIND_COUNT],
}

impl MemoryBudget {
    pub fn new(limit: usize, completion_reserve: usize) -> Result<Self, MemoryError> {
        Self::create(limit, completion_reserve, None)
    }
    /// All successful reservations charge this child and every ancestor. A
    /// parent's refusal rolls back the entire tentative reservation. Local
    /// allowances can be larger than a parent; the strictest live limit wins.
    pub fn child(&self, limit: usize, completion_reserve: usize) -> Result<Self, MemoryError> {
        Self::create(limit, completion_reserve, Some(self.clone()))
    }
    fn create(
        limit: usize,
        completion_reserve: usize,
        parent: Option<Self>,
    ) -> Result<Self, MemoryError> {
        if limit == 0 || completion_reserve > limit {
            return Err(MemoryError::InvalidConfiguration(
                "invalid memory allowance",
            ));
        }
        let depth = match &parent {
            Some(parent) => parent
                .0
                .depth
                .checked_add(1)
                .filter(|depth| *depth <= 8)
                .ok_or(MemoryError::InvalidConfiguration(
                    "memory budget hierarchy exceeds eight levels",
                ))?,
            None => 1,
        };
        Ok(Self(Arc::new(Counters {
            limit,
            ordinary_limit: limit.saturating_sub(completion_reserve),
            total: AtomicUsize::new(0),
            ordinary: AtomicUsize::new(0),
            kinds: KindCounters::new(),
            parent,
            depth,
        })))
    }

    pub fn reserve(
        &self,
        kind: BudgetKind,
        lane: BudgetLane,
        bytes: usize,
    ) -> Result<Reservation, MemoryError> {
        self.reserve_tree(kind, lane, bytes)?;
        Ok(Reservation(Allocation {
            budget: self.clone(),
            kind,
            lane,
            bytes,
        }))
    }
    fn reserve_tree(
        &self,
        kind: BudgetKind,
        lane: BudgetLane,
        bytes: usize,
    ) -> Result<(), MemoryError> {
        if lane == BudgetLane::Ordinary {
            reserve_counter(&self.0.ordinary, self.0.ordinary_limit, bytes)?;
        }
        if let Err(error) = reserve_counter(&self.0.total, self.0.limit, bytes) {
            if lane == BudgetLane::Ordinary {
                self.0.ordinary.fetch_sub(bytes, Ordering::AcqRel);
            }
            return Err(error);
        }
        // A category can never exceed total, so this addition cannot overflow.
        self.0.kinds.get(kind).fetch_add(bytes, Ordering::AcqRel);
        if let Some(parent) = &self.0.parent
            && let Err(error) = parent.reserve_tree(kind, lane, bytes)
        {
            self.release_local(kind, lane, bytes);
            return Err(error);
        }
        Ok(())
    }
    fn release_local(&self, kind: BudgetKind, lane: BudgetLane, bytes: usize) {
        self.0.kinds.get(kind).fetch_sub(bytes, Ordering::AcqRel);
        self.0.total.fetch_sub(bytes, Ordering::AcqRel);
        if lane == BudgetLane::Ordinary {
            self.0.ordinary.fetch_sub(bytes, Ordering::AcqRel);
        }
    }
    fn release_tree(&self, kind: BudgetKind, lane: BudgetLane, bytes: usize) {
        let mut budget = Some(self);
        while let Some(current) = budget {
            current.release_local(kind, lane, bytes);
            budget = current.0.parent.as_ref();
        }
    }
    /// Includes this budget itself. Used by owners to reject an independently
    /// budgeted component that would bypass their node/tenant resource limit.
    pub fn is_within(&self, ancestor: &Self) -> bool {
        let mut budget = Some(self);
        while let Some(current) = budget {
            if Arc::ptr_eq(&current.0, &ancestor.0) {
                return true;
            }
            budget = current.0.parent.as_ref();
        }
        false
    }
    /// Immutable ceiling for one reservation across this budget and every
    /// ancestor. This is not currently free space and grants no reservation.
    pub fn reservation_limit(&self, lane: BudgetLane) -> usize {
        let mut limit = usize::MAX;
        let mut budget = Some(self);
        while let Some(current) = budget {
            limit = limit.min(match lane {
                BudgetLane::Ordinary => current.0.ordinary_limit,
                BudgetLane::Completion => current.0.limit,
            });
            budget = current.0.parent.as_ref();
        }
        limit
    }

    /// Counters are individually exact. A concurrent allocation/drop can make
    /// this diagnostic snapshot non-atomic across categories.
    pub fn stats(&self) -> BudgetStats {
        BudgetStats {
            limit: self.0.limit,
            completion_reserve: self.0.limit.saturating_sub(self.0.ordinary_limit),
            used: self.0.total.load(Ordering::Acquire),
            ordinary_used: self.0.ordinary.load(Ordering::Acquire),
            by_kind: self.0.kinds.snapshot(),
        }
    }
}

fn reserve_counter(counter: &AtomicUsize, limit: usize, bytes: usize) -> Result<(), MemoryError> {
    counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
            used.checked_add(bytes).filter(|next| *next <= limit)
        })
        .map(|_| ())
        .map_err(|used| MemoryError::Capacity {
            requested: bytes,
            available: limit.saturating_sub(used),
        })
}

/// Admission owns this before publication/durability. Dropping it rolls back
/// every byte automatically, including early returns and unwinding.
#[derive(Debug)]
pub struct Reservation(Allocation);

impl Reservation {
    pub fn bytes(&self) -> usize {
        self.0.bytes
    }

    pub fn commit(self) -> Allocation {
        self.0
    }
}

/// Ownership of an accounted allocation; dropping it releases the charge.
/// Attach it to the immutable page or other object whose lifetime it measures.
#[derive(Debug)]
pub struct Allocation {
    budget: MemoryBudget,
    kind: BudgetKind,
    lane: BudgetLane,
    bytes: usize,
}

impl Allocation {
    pub fn bytes(&self) -> usize {
        self.bytes
    }
    /// Transfer an identical budget/category/lane permit without new admission.
    /// A mismatch leaves both owners unchanged; success empties `other`.
    pub fn absorb(&mut self, other: &mut Self) -> Result<(), MemoryError> {
        if !Arc::ptr_eq(&self.budget.0, &other.budget.0)
            || self.kind != other.kind
            || self.lane != other.lane
        {
            return Err(MemoryError::InvalidConfiguration(
                "allocation owners differ",
            ));
        }
        let total = self
            .bytes
            .checked_add(other.bytes)
            .ok_or(MemoryError::InvalidConfiguration("allocation sum overflow"))?;
        self.bytes = total;
        other.bytes = 0;
        Ok(())
    }
    /// Transfer part of an existing allowance to another owned value. Counters
    /// at every ancestor remain unchanged; each resulting owner releases only
    /// its part. This performs no new admission or heap allocation.
    pub fn split_off(&mut self, bytes: usize) -> Result<Self, MemoryError> {
        let remaining = self
            .bytes
            .checked_sub(bytes)
            .ok_or(MemoryError::InvalidConfiguration(
                "allocation split exceeds owned allowance",
            ))?;
        self.bytes = remaining;
        Ok(Self {
            budget: self.budget.clone(),
            kind: self.kind,
            lane: self.lane,
            bytes,
        })
    }
    /// Release temporary working capacity after its payload has been discarded,
    /// while retaining the remainder across an owned result/channel handoff.
    /// Growing requires separate admission and cannot be hidden in this method.
    pub fn shrink_to(&mut self, bytes: usize) -> Result<(), MemoryError> {
        let released = self
            .bytes
            .checked_sub(bytes)
            .ok_or(MemoryError::InvalidConfiguration(
                "allocation cannot grow without admission",
            ))?;
        self.budget.release_tree(self.kind, self.lane, released);
        self.bytes = bytes;
        Ok(())
    }
}

impl Drop for Allocation {
    fn drop(&mut self) {
        self.budget.release_tree(self.kind, self.lane, self.bytes);
    }
}

#[cfg(test)]
mod resizing_tests {
    use super::*;
    #[test]
    fn immutable_reservation_ceiling_includes_every_ancestor_and_ignores_usage() {
        let parent = MemoryBudget::new(100, 30).unwrap();
        let child = parent.child(200, 150).unwrap();
        assert_eq!(child.reservation_limit(BudgetLane::Ordinary), 50);
        assert_eq!(child.reservation_limit(BudgetLane::Completion), 100);
        let pressure = parent
            .reserve(BudgetKind::Payload, BudgetLane::Completion, 99)
            .unwrap();
        assert_eq!(child.reservation_limit(BudgetLane::Ordinary), 50);
        assert_eq!(child.reservation_limit(BudgetLane::Completion), 100);
        assert!(
            child
                .reserve(BudgetKind::Payload, BudgetLane::Completion, 2)
                .is_err()
        );
        drop(pressure);
    }
    #[test]
    fn split_transfers_ownership_without_readmission_or_changing_ancestor_totals() {
        let parent = MemoryBudget::new(100, 0).unwrap();
        let child = parent.child(100, 0).unwrap();
        let mut first = child
            .reserve(BudgetKind::Payload, BudgetLane::Ordinary, 100)
            .unwrap()
            .commit();
        assert!(first.split_off(101).is_err());
        assert_eq!(first.bytes(), 100);
        let mut second = first.split_off(60).unwrap();
        assert_eq!(first.bytes(), 40);
        assert_eq!(parent.stats().used, 100);
        assert_eq!(child.stats().used, 100);
        drop(first);
        assert_eq!(parent.stats().used, 60);
        second.shrink_to(20).unwrap();
        assert_eq!(child.stats().used, 20);
        assert_eq!(parent.stats().used, 20);
        drop(second);
        assert_eq!(parent.stats().used, 0);
    }
    #[test]
    fn hierarchy_enforces_each_ancestor_and_rolls_back_failed_admission() {
        let node = MemoryBudget::new(100, 20).unwrap();
        let tenant = node.child(90, 10).unwrap();
        let first = tenant.child(70, 10).unwrap();
        let second = tenant.child(70, 10).unwrap();
        let independent = MemoryBudget::new(100, 20).unwrap();
        assert!(first.is_within(&node) && first.is_within(&tenant));
        assert!(!first.is_within(&second) && !first.is_within(&independent));
        let mut held = first
            .reserve(BudgetKind::Payload, BudgetLane::Ordinary, 60)
            .unwrap()
            .commit();
        assert!(
            first
                .reserve(BudgetKind::Payload, BudgetLane::Ordinary, 1)
                .is_err()
        );
        assert!(
            second
                .reserve(BudgetKind::Payload, BudgetLane::Ordinary, 21)
                .is_err()
        );
        assert_eq!(second.stats().used, 0);
        assert_eq!(tenant.stats().used, 60);
        assert_eq!(node.stats().used, 60);
        let completion = second
            .reserve(BudgetKind::Control, BudgetLane::Completion, 30)
            .unwrap()
            .commit();
        assert_eq!(node.stats().used, 90);
        assert!(
            second
                .reserve(BudgetKind::Control, BudgetLane::Completion, 1)
                .is_err()
        );
        let spare = node
            .reserve(BudgetKind::Control, BudgetLane::Completion, 10)
            .unwrap()
            .commit();
        assert_eq!(node.stats().used, 100);
        held.shrink_to(10).unwrap();
        assert_eq!(first.stats().used, 10);
        assert_eq!(tenant.stats().used, 40);
        assert_eq!(node.stats().used, 50);
        drop(tenant);
        drop(first);
        drop(second);
        assert_eq!(node.stats().used, 50);
        drop(held);
        drop(completion);
        drop(spare);
        assert_eq!(node.stats().used, 0);
        assert_eq!(node.stats().ordinary_used, 0);
        assert!(node.stats().by_kind.iter().all(|bytes| *bytes == 0));
    }
    #[test]
    fn concurrent_siblings_share_one_bound_without_allocation_wrappers() {
        let node = MemoryBudget::new(200, 20).unwrap();
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let child = node.child(100, 10).unwrap();
                let parent = &node;
                scope.spawn(move || {
                    for _ in 0..1000 {
                        if let Ok(held) =
                            child.reserve(BudgetKind::Payload, BudgetLane::Ordinary, 37)
                        {
                            assert!(parent.stats().used <= 200);
                            std::thread::yield_now();
                            drop(held);
                        }
                    }
                });
            }
        });
        assert_eq!(node.stats().used, 0);
        let mut deepest = node.clone();
        for _ in 1..8 {
            deepest = deepest.child(200, 20).unwrap();
        }
        assert!(deepest.child(200, 20).is_err());
    }
    #[test]
    fn owned_shrink_releases_only_discarded_capacity_and_cannot_grow() {
        let budget = MemoryBudget::new(100, 10).unwrap();
        let mut first = budget
            .reserve(BudgetKind::Payload, BudgetLane::Ordinary, 90)
            .unwrap()
            .commit();
        assert!(first.shrink_to(91).is_err());
        assert_eq!(budget.stats().used, 90);
        first.shrink_to(40).unwrap();
        let second = budget
            .reserve(BudgetKind::Pending, BudgetLane::Ordinary, 50)
            .unwrap()
            .commit();
        assert_eq!(budget.stats().used, 90);
        drop(first);
        assert_eq!(budget.stats().used, 50);
        drop(second);
        assert_eq!(budget.stats().used, 0);
        assert_eq!(budget.stats().ordinary_used, 0);
        assert!(budget.stats().by_kind.iter().all(|bytes| *bytes == 0));
    }
}
