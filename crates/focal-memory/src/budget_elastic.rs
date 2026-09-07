//! Owner-controlled resizing of one coarse funded source. Credit acquisition,
//! rather than a separately sampled limit/usage pair, serializes spending with
//! trimming. Allocations may cross threads; resize authority remains unique.

use super::{Backing, BudgetKind, BudgetLane, Counters, KindCounters, MemoryBudget, MemoryError};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Debug)]
pub(super) struct ElasticBacking {
    pub(super) lane: BudgetLane,
    // Includes non-spendable metadata. Only the unique controller changes this;
    // final Counters drop releases the aggregate through its retained parent.
    held: AtomicUsize,
    available: AtomicUsize,
    metadata: usize,
}

impl ElasticBacking {
    /// Exclusively acquire already-funded credit before publishing local usage
    /// or reclassifying ancestors. Failure changes no counter.
    pub(super) fn acquire(&self, bytes: usize) -> Result<(), MemoryError> {
        self.available
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |available| {
                available.checked_sub(bytes)
            })
            .map(|_| ())
            .map_err(|available| MemoryError::Capacity {
                requested: bytes,
                available,
            })
    }

    /// Restore exact acquired credit only after local usage and every ancestor
    /// category have been refunded. Newly grown credit likewise has full parent
    /// backing before this publication. Those ownership rules bound the sum by
    /// funded capacity, including concurrent returns and in-flight acquisitions.
    pub(super) fn release(&self, bytes: usize) {
        self.available.fetch_add(bytes, Ordering::Release);
    }

    pub(super) fn held_bytes(&self) -> usize {
        self.held.load(Ordering::Acquire)
    }
}

/// Unique growth/trim authority for one shareable accounting source. The source
/// can be cloned for existing storage/custody APIs; such clones cannot resize it.
/// No per-evaluation counter allocation or per-growth permit collection is used.
///
/// Dropping this controller leaves remaining backing attached to all source
/// handles, descendants and issued allocations. Trimming is explicit. This pool
/// does not know which idle bytes an owner has promised to future operations;
/// the owner must preserve those entitlements when choosing a trim amount.
pub struct ElasticFundedPool {
    budget: MemoryBudget,
}

impl std::fmt::Debug for ElasticFundedPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ElasticFundedPool")
            .field("budget", &self.budget)
            .field("funded_capacity", &self.funded_capacity())
            .field("available", &self.available())
            .finish()
    }
}

impl ElasticFundedPool {
    /// Existing allocation APIs use this source. Its `limit` is the immutable
    /// ceiling, while available funded credit independently controls admission.
    pub fn budget(&self) -> &MemoryBudget {
        &self.budget
    }

    /// Spendable backing, including both idle and already-issued capacity.
    /// Metadata is excluded. This is a diagnostic, not an admission permit.
    pub fn funded_capacity(&self) -> usize {
        match &self.budget.0.backing {
            Backing::Elastic(backing) => backing.held_bytes().saturating_sub(backing.metadata),
            // The private constructor installs only Elastic. Remain total even
            // if an internal caller violates that representation invariant.
            Backing::None | Backing::Fixed(_) => 0,
        }
    }

    /// Currently unissued funded credit. A concurrent allocation or refund can
    /// change this immediately; trim and reserve use checked atomic acquisition.
    pub fn available(&self) -> usize {
        match &self.budget.0.backing {
            Backing::Elastic(backing) => backing.available.load(Ordering::Acquire),
            Backing::None | Backing::Fixed(_) => 0,
        }
    }

    /// Fund additional capacity in the original lane before making it usable.
    /// Failure preserves the source and parent exactly. Growth allocates no heap
    /// buffer; the temporary parent reservation transfers into aggregate backing.
    pub fn grow(&mut self, bytes: usize) -> Result<(), MemoryError> {
        let backing = self.backing()?;
        let parent = self.parent()?;
        let held = backing.held_bytes();
        let capacity =
            held.checked_sub(backing.metadata)
                .ok_or(MemoryError::InvalidConfiguration(
                    "elastic metadata missing",
                ))?;
        let next_capacity = crate::checked_add(capacity, bytes)?;
        if next_capacity > self.budget.0.limit {
            return Err(MemoryError::Capacity {
                requested: bytes,
                available: self.budget.0.limit.saturating_sub(capacity),
            });
        }
        let next_held = crate::checked_add(held, bytes)?;
        let mut reservation = parent.reserve(BudgetKind::Reserved, backing.lane, bytes)?;
        // Every fallible check is complete. The unique controller serializes
        // held-byte updates; concurrent spending/refunds only change available.
        backing.held.store(next_held, Ordering::Release);
        reservation.0.bytes = 0;
        #[cfg(test)]
        if bytes != 0 {
            super::interleaving_tests::pause(
                super::interleaving_tests::Step::GrowReady,
                &self.budget,
            );
        }
        backing.release(bytes);
        Ok(())
    }

    /// Return exactly this much unissued capacity to the parent. Metadata and
    /// acquired credit cannot be trimmed. The owner must also exclude idle
    /// capacity promised by its completion book. No partial trim occurs.
    pub fn trim_unused(&mut self, bytes: usize) -> Result<(), MemoryError> {
        let backing = self.backing()?;
        let parent = self.parent()?;
        let next_held = backing
            .held_bytes()
            .checked_sub(bytes)
            .filter(|remaining| *remaining >= backing.metadata)
            .ok_or_else(|| MemoryError::Capacity {
                requested: bytes,
                available: backing.available.load(Ordering::Acquire),
            })?;
        // Acquire exclusively before releasing any parent reservation: a racing
        // reserve either owns these bytes already or cannot obtain them now.
        backing.acquire(bytes)?;
        backing.held.store(next_held, Ordering::Release);
        #[cfg(test)]
        if bytes != 0 {
            super::interleaving_tests::pause(
                super::interleaving_tests::Step::TrimReady,
                &self.budget,
            );
        }
        parent.release_tree(BudgetKind::Reserved, backing.lane, bytes);
        Ok(())
    }

    fn backing(&self) -> Result<&ElasticBacking, MemoryError> {
        match &self.budget.0.backing {
            Backing::Elastic(backing) => Ok(backing),
            Backing::None | Backing::Fixed(_) => Err(MemoryError::InvalidConfiguration(
                "elastic controller has no elastic backing",
            )),
        }
    }

    fn parent(&self) -> Result<&MemoryBudget, MemoryError> {
        self.budget
            .0
            .parent
            .as_ref()
            .ok_or(MemoryError::InvalidConfiguration(
                "elastic backing has no parent",
            ))
    }
}

impl MemoryBudget {
    /// Create a uniquely resizable funded source. `ceiling` is an immutable
    /// spendable upper bound; `initial_capacity` may be zero. Initial backing and
    /// non-spendable counter metadata are charged to this parent before creation.
    /// Ordinary funding admits either allocation lane, while Completion funding
    /// cannot admit Ordinary work, including through any descendant source.
    ///
    /// Future growth seeks parent admission. Spending or reusing held credit
    /// does not. Returning idle capacity cannot reclaim live/pinned allocations,
    /// and remaining aggregate backing lasts until the final source disappears.
    pub fn elastic_funded_child(
        &self,
        funding_lane: BudgetLane,
        ceiling: usize,
        initial_capacity: usize,
    ) -> Result<ElasticFundedPool, MemoryError> {
        if ceiling == 0 || initial_capacity > ceiling {
            return Err(MemoryError::InvalidConfiguration(
                "invalid elastic funded allowance",
            ));
        }
        let depth = self
            .0
            .depth
            .checked_add(1)
            .filter(|depth| *depth <= 8)
            .ok_or(MemoryError::InvalidConfiguration(
                "memory budget hierarchy exceeds eight levels",
            ))?;
        let metadata = crate::checked_add(
            crate::ALLOCATOR_OVERHEAD,
            crate::checked_add(
                size_of::<Counters>(),
                crate::checked_mul(2, size_of::<usize>())?,
            )?,
        )?;
        let held = crate::checked_add(initial_capacity, metadata)?;
        let mut reservation = self.reserve(BudgetKind::Reserved, funding_lane, held)?;
        let budget = Self(Arc::new(Counters {
            limit: ceiling,
            ordinary_limit: match funding_lane {
                BudgetLane::Ordinary => ceiling,
                BudgetLane::Completion => 0,
            },
            total: AtomicUsize::new(0),
            ordinary: AtomicUsize::new(0),
            kinds: KindCounters::new(),
            parent: Some(self.clone()),
            depth,
            // Until aggregate adoption the reservation alone owns the backing.
            // No source handle or available credit has escaped construction.
            backing: Backing::Elastic(ElasticBacking {
                lane: funding_lane,
                held: AtomicUsize::new(0),
                available: AtomicUsize::new(0),
                metadata,
            }),
        }));
        let controller = ElasticFundedPool { budget };
        let backing = controller.backing()?;
        backing.held.store(held, Ordering::Release);
        reservation.0.bytes = 0;
        backing.release(initial_capacity);
        Ok(controller)
    }
}
