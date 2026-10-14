//! Owner-controlled resizing of one coarse funded source. Credit acquisition,
//! rather than a separately sampled limit/usage pair, serializes spending with
//! trimming. Allocations may cross threads; resize authority remains unique.

use super::{
    Backing, BudgetKind, BudgetLane, Ended, Funding, Level, MemoryBudget, MemoryError, Terms,
    child_metadata, release_tree, reserve_tree,
};
use std::sync::atomic::Ordering;

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
        // The private constructor installs only elastic backing. Remain total
        // even if an internal caller violates that representation invariant.
        self.backing().map_or(0, |backing| {
            backing.held_bytes().saturating_sub(backing.metadata())
        })
    }

    /// Currently unissued funded credit. A concurrent allocation or refund can
    /// change this immediately; trim and reserve use checked atomic acquisition.
    pub fn available(&self) -> usize {
        self.backing()
            .map_or(0, |backing| backing.available.load(Ordering::Acquire))
    }

    /// Fund additional capacity in the original lane before making it usable.
    /// Failure preserves the source and parent exactly. Growth allocates no heap
    /// buffer; the parent's charge goes straight into the aggregate backing.
    pub fn grow(&mut self, bytes: usize) -> Result<(), MemoryError> {
        let backing = self.backing()?;
        let (parent, lane) = self.parent()?;
        let held = backing.held_bytes();
        let capacity =
            held.checked_sub(backing.metadata())
                .ok_or(MemoryError::InvalidConfiguration(
                    "elastic metadata missing",
                ))?;
        let next_capacity = crate::checked_add(capacity, bytes)?;
        let limit = self.budget.limit();
        if next_capacity > limit {
            return Err(MemoryError::Capacity {
                requested: bytes,
                available: limit.saturating_sub(capacity),
            });
        }
        let next_held = crate::checked_add(held, bytes)?;
        reserve_tree(parent, BudgetKind::Reserved, lane, bytes)?;
        // Every fallible check is complete. The unique controller serializes
        // held-byte updates; concurrent spending/refunds only change available.
        backing.held.store(next_held, Ordering::Release);
        #[cfg(test)]
        if bytes != 0 {
            super::interleaving_tests::pause(
                super::interleaving_tests::Step::GrowReady,
                self.budget.level,
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
        let (parent, lane) = self.parent()?;
        let next_held = backing
            .held_bytes()
            .checked_sub(bytes)
            .filter(|remaining| *remaining >= backing.metadata())
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
                self.budget.level,
            );
        }
        // The pool's level holds its parent, so nothing ends here.
        release_tree(parent, BudgetKind::Reserved, lane, bytes, &mut Ended::new());
        Ok(())
    }

    fn backing(&self) -> Result<&'static Backing, MemoryError> {
        let backing = &self.budget.level.backing;
        if backing.is_elastic() {
            Ok(backing)
        } else {
            Err(MemoryError::InvalidConfiguration(
                "elastic controller has no elastic backing",
            ))
        }
    }

    /// The pool's parent level and the lane its backing is charged in.
    fn parent(&self) -> Result<(&'static Level, BudgetLane), MemoryError> {
        let level = self.budget.level;
        level
            .parent()
            .zip(level.backing.lane())
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
        if funding_lane == BudgetLane::Committed {
            return Err(MemoryError::InvalidConfiguration(
                "committed work is never prepaid",
            ));
        }
        let depth = self.child_depth()?;
        let metadata = child_metadata();
        let held = crate::checked_add(initial_capacity, metadata)?;
        reserve_tree(self.level, BudgetKind::Reserved, funding_lane, held)?;
        let budget = Self::begin(
            &Terms {
                limit: ceiling,
                ordinary_limit: match funding_lane {
                    BudgetLane::Ordinary => ceiling,
                    BudgetLane::Completion | BudgetLane::Committed => 0,
                },
                hold_limit: ceiling,
                depth,
                funding: Funding::Elastic {
                    lane: funding_lane,
                    held,
                    available: initial_capacity,
                    metadata,
                },
            },
            Some(self),
        )
        .inspect_err(|_| {
            // This budget holds its level, so nothing ends here.
            release_tree(
                self.level,
                BudgetKind::Reserved,
                funding_lane,
                held,
                &mut Ended::new(),
            );
        })?;
        Ok(ElasticFundedPool { budget })
    }
}
