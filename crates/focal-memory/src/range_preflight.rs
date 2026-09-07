//! Checked, allocation-free preparation estimates bound to one immutable base
//! and an owned write set. These are accounting estimates, not funded permits.

use super::{
    Arc, Change, Entry, PageDirectory, PreparedRange, RangeConfig, RangeStore, Root, groups,
    layout, merge_charge, page_charge, root_charge, visit_merged,
};
use crate::{ALLOCATOR_OVERHEAD, BudgetLane, MemoryBudget, MemoryError, checked_add, checked_mul};

/// Additional accounting demand while the existing base, other candidates and
/// pinned roots remain separately charged. Incoming payloads are conservatively
/// charged both as pending input and in their destination pages during build.
/// Caller-owned buffers outside the write set and extra copier workspace are
/// excluded. Directory bytes conservatively bound every issued node and edit
/// workspace allocation, including temporary intermediate nodes. New entry page
/// charges are exact. Both total fields are upper bounds; their constituent
/// maxima need not occur at the same instant.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RangePreparationCharges {
    input_pending_bytes: usize,
    directory_bytes: usize,
    new_pages_bytes: usize,
    merge_pending_bytes: usize,
    additional_peak_bytes: usize,
    additional_retained_bytes: usize,
}

impl RangePreparationCharges {
    pub fn input_pending_bytes(self) -> usize {
        self.input_pending_bytes
    }
    pub fn directory_bytes(self) -> usize {
        self.directory_bytes
    }
    pub fn new_pages_bytes(self) -> usize {
        self.new_pages_bytes
    }
    pub fn merge_pending_bytes(self) -> usize {
        self.merge_pending_bytes
    }
    pub fn additional_peak_bytes(self) -> usize {
        self.additional_peak_bytes
    }
    pub fn additional_retained_bytes(self) -> usize {
        self.additional_retained_bytes
    }
}

/// Checked write set and additional capacity bound. The private owned inputs
/// and borrowed exact base cannot be replaced after quoting. Holding this plan
/// does not reserve memory: build still performs the usual lane reservations,
/// and can refuse if other owners have consumed capacity in the meantime.
/// No values are copied during planning. The input vector is sorted in place.
/// Planning routes sorted changes through the persistent directory and inspects
/// retained entries only on touched pages. It does not flatten or scan all leaves.
pub struct RangePreparationPlan<'a, K, V> {
    pub(super) store: &'a RangeStore<K, V>,
    pub(super) base: &'a Arc<Root<K, V>>,
    pub(super) prefix: u64,
    pub(super) changes: Vec<Change<K, V>>,
    pub(super) lane: BudgetLane,
    pub(super) new_len: usize,
    pub(super) output_pages: usize,
    pub(super) charges: RangePreparationCharges,
}

impl<K: Ord + Clone, V> RangePreparationPlan<'_, K, V> {
    pub fn charges(&self) -> RangePreparationCharges {
        self.charges
    }
    pub fn base_prefix(&self) -> u64 {
        self.base.prefix
    }
    pub fn prefix(&self) -> u64 {
        self.prefix
    }
    pub fn output_pages(&self) -> usize {
        self.output_pages
    }

    /// Consume exactly the checked inputs. Retained values follow the copier
    /// contract of [`RangeStore::prepare_batch_with`]; supplied rows move.
    pub fn build_with<F>(self, copy: F) -> Result<PreparedRange<K, V>, MemoryError>
    where
        F: FnMut(&V) -> Result<V, MemoryError>,
    {
        let source = &self.store.budget;
        self.build_in_with(source, copy)
    }

    /// Construct from this owner's budget or a descendant source, including a
    /// prefunded owner pool. The source enforces the checked plan's lane and
    /// remains attached to every new root/page allocation until its final drop.
    /// Shared base pages retain their original source and require no new debit.
    pub fn build_in_with<F>(
        self,
        source: &MemoryBudget,
        mut copy: F,
    ) -> Result<PreparedRange<K, V>, MemoryError>
    where
        F: FnMut(&V) -> Result<V, MemoryError>,
    {
        if !source.is_within(&self.store.budget) {
            return Err(MemoryError::InvalidConfiguration(
                "range funding source is outside its owner budget",
            ));
        }
        self.store.prepare_planned_with(self, source, &mut copy)
    }

    pub fn build_in(self, source: &MemoryBudget) -> Result<PreparedRange<K, V>, MemoryError>
    where
        V: Clone,
    {
        self.build_in_with(source, |value| Ok(value.clone()))
    }

    pub fn build(self) -> Result<PreparedRange<K, V>, MemoryError>
    where
        V: Clone,
    {
        self.build_with(|value| Ok(value.clone()))
    }
}

impl<K: Ord + Clone, V> RangeStore<K, V> {
    /// Preflight one owned batch without reserving or allocating additional
    /// buffers. `max_bytes` bounds its conservative *additional* peak charge;
    /// existing roots/pins and extra caller/copier workspace are separate.
    pub fn plan_batch(
        &self,
        prefix: u64,
        changes: Vec<Change<K, V>>,
        lane: BudgetLane,
        max_bytes: usize,
    ) -> Result<RangePreparationPlan<'_, K, V>, MemoryError> {
        self.plan_root(&self.root, prefix, changes, lane, max_bytes)
    }

    /// Preflight against the exact owned predecessor. Publication must still
    /// validate and install the complete candidate chain in order.
    pub fn plan_after<'a>(
        &'a self,
        predecessor: &'a PreparedRange<K, V>,
        prefix: u64,
        changes: Vec<Change<K, V>>,
        lane: BudgetLane,
        max_bytes: usize,
    ) -> Result<RangePreparationPlan<'a, K, V>, MemoryError> {
        if predecessor.root.owner != self.root.owner {
            return Err(MemoryError::WrongRange);
        }
        self.plan_root(&predecessor.root, prefix, changes, lane, max_bytes)
    }

    fn plan_root<'a>(
        &'a self,
        base: &'a Arc<Root<K, V>>,
        prefix: u64,
        mut changes: Vec<Change<K, V>>,
        lane: BudgetLane,
        max_bytes: usize,
    ) -> Result<RangePreparationPlan<'a, K, V>, MemoryError> {
        let expected = base
            .prefix
            .checked_add(1)
            .ok_or(MemoryError::CounterExhausted("published prefix"))?;
        if prefix != expected {
            return Err(MemoryError::PrefixMismatch {
                expected,
                actual: prefix,
            });
        }
        if changes.len() > self.config.max_batch_entries {
            return Err(MemoryError::Capacity {
                requested: changes.len(),
                available: self.config.max_batch_entries,
            });
        }
        changes.sort_unstable_by(|left, right| left.key().cmp(right.key()));
        if changes.windows(2).any(|pair| {
            pair.first()
                .zip(pair.last())
                .is_some_and(|(left, right)| left.key() == right.key())
        }) {
            return Err(MemoryError::DuplicateKey);
        }
        let mut new_len = base.len;
        let mut input_pending_bytes = if changes.capacity() == 0 {
            0
        } else {
            checked_add(
                ALLOCATOR_OVERHEAD,
                checked_mul(changes.capacity(), size_of::<Change<K, V>>())?,
            )?
        };
        for change in &changes {
            match change {
                Change::Put(entry) => {
                    layout::check_entry::<K, V>(entry.heap_bytes, self.config)?;
                    input_pending_bytes = checked_add(input_pending_bytes, entry.heap_bytes)?;
                    if base.get(&entry.key).is_none() {
                        new_len = checked_add(new_len, 1)?;
                    }
                }
                Change::Delete(key) => {
                    let existing = base.get(key).ok_or(MemoryError::MissingKey)?;
                    // Conservatively bound the caller's cloned deletion key by
                    // the retained entry's combined key/value heap charge.
                    input_pending_bytes = checked_add(input_pending_bytes, existing.heap_bytes)?;
                    new_len = new_len.checked_sub(1).ok_or(MemoryError::MissingKey)?;
                }
            }
        }
        let mut count = Counts {
            output_pages: base.pages.len(),
            ..Counts::default()
        };
        let mut remaining = changes.as_slice();
        while let Some(group) = groups::next(base, remaining)? {
            let selected = remaining
                .get(..group.count)
                .ok_or(MemoryError::MissingKey)?;
            match base.pages.get(group.rank) {
                None if base.pages.is_empty() => {
                    let produced = count.merge::<K, V>(&[], selected, self.config)?;
                    count.directory_edits = checked_add(count.directory_edits, produced)?;
                }
                None => return Err(MemoryError::MissingKey),
                Some(page) => {
                    if let Some(split) = layout::reusable_singleton(page, selected, self.config)? {
                        let before = selected.get(..split).ok_or(MemoryError::MissingKey)?;
                        let after = selected.get(split..).ok_or(MemoryError::MissingKey)?;
                        let left = count.merge(&[], before, self.config)?;
                        let right = count.merge(&[], after, self.config)?;
                        // The existing singleton remains in place. Only its
                        // inserted neighbors require directory edits.
                        count.directory_edits =
                            checked_add(count.directory_edits, checked_add(left, right)?)?;
                    } else {
                        let produced = count.merge(&page.entries, selected, self.config)?;
                        count.output_pages = count
                            .output_pages
                            .checked_sub(1)
                            .ok_or(MemoryError::MissingKey)?;
                        count.directory_edits =
                            checked_add(count.directory_edits, produced.max(1))?;
                    }
                }
            }
            remaining = remaining
                .get(group.count..)
                .ok_or(MemoryError::MissingKey)?;
        }
        let directory_bytes = checked_add(
            root_charge::<K, V>(0)?,
            PageDirectory::<K, V>::edit_bound(
                count.directory_edits,
                checked_add(base.pages.len(), count.new_pages)?,
            )?,
        )?;
        let additional_retained_bytes = checked_add(directory_bytes, count.new_pages_bytes)?;
        let additional_peak_bytes = checked_add(
            additional_retained_bytes,
            checked_add(input_pending_bytes, count.merge_pending_bytes)?,
        )?;
        if additional_peak_bytes > max_bytes {
            return Err(MemoryError::Capacity {
                requested: additional_peak_bytes,
                available: max_bytes,
            });
        }
        Ok(RangePreparationPlan {
            store: self,
            base,
            prefix,
            changes,
            lane,
            new_len,
            output_pages: count.output_pages,
            charges: RangePreparationCharges {
                input_pending_bytes,
                directory_bytes,
                new_pages_bytes: count.new_pages_bytes,
                merge_pending_bytes: count.merge_pending_bytes,
                additional_peak_bytes,
                additional_retained_bytes,
            },
        })
    }
}

#[derive(Default)]
struct Counts {
    output_pages: usize,
    new_pages: usize,
    directory_edits: usize,
    new_pages_bytes: usize,
    merge_pending_bytes: usize,
}

impl Counts {
    fn merge<K: Ord, V>(
        &mut self,
        old: &[Entry<K, V>],
        selected: &[Change<K, V>],
        config: RangeConfig,
    ) -> Result<usize, MemoryError> {
        let initial = self.new_pages;
        let mut partition = layout::LeafPartition::new::<K, V>(config)?;
        let mut emit = |span: layout::LeafSpan| {
            self.output_pages = checked_add(self.output_pages, 1)?;
            self.new_pages = checked_add(self.new_pages, 1)?;
            self.new_pages_bytes = checked_add(
                self.new_pages_bytes,
                page_charge::<K, V>(span.len, span.heap)?,
            )?;
            Ok(())
        };
        visit_merged(old, selected, |entry| {
            partition.push(entry.heap_bytes(), &mut emit)
        })?;
        partition.finish(&mut emit)?;
        self.merge_pending_bytes =
            self.merge_pending_bytes
                .max(merge_charge::<K, V>(checked_add(
                    old.len(),
                    selected.len(),
                )?)?);
        self.new_pages
            .checked_sub(initial)
            .ok_or(MemoryError::MissingKey)
    }
}
