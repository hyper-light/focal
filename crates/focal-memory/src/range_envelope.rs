//! A future write bound depends on immutable owner limits, never current range
//! occupancy. It is an accounting contract, not a reservation or entitlement.
//!
//! For `m` changed keys, at most `m` old leaves are affected. An ordinary old
//! leaf's retained subsequence still fits its original byte and count limits.
//! Inserting `p` rows splits those subsequences into at most `t + p` fitting
//! runs, where `t <= m` counts affected leaves. Each incoming row also fits a
//! singleton (possibly oversized). Thus there is a valid ordered partition of
//! at most `t + 2p <= 3m` output pages. Greedy longest-prefix partitioning cannot
//! use more pages than this partition. Empty deletion groups need one directory
//! removal, already covered by their affected-leaf term. Unchanged oversized
//! singletons are shared; replaced/deleted oversized entries are never copied.
//! A partitioned old leaf is homogeneous, so every retained subsequence also
//! satisfies its key boundary. Each incoming key can add at most two boundaries;
//! the same `3m` bound covers arbitrary classifier values without extra padding.
//!
//! Each published base page owns at least `page_charge(1, 0)` bytes under the
//! store's budget. Its immutable limit therefore bounds the page count and
//! directory height, including after unrelated growth. Existing roots, pins,
//! caller buffers outside the changes, and extra copier workspace remain
//! separately charged. Shared ancestors may impose a smaller usable allowance;
//! this bound does not promise that a future reservation will succeed.

use super::{
    Change, Entry, PageDirectory, RangePreparationPlan, RangeStore, merge_layout_charge,
    page_charge, root_charge,
};
use crate::{ALLOCATOR_OVERHEAD, MemoryError, OwnerId, checked_add, checked_mul};

/// Maximum shape of one future write. `incoming_heap` bounds the summed heap
/// charges of Put entries, including their owned key/value allocator overhead.
/// `deleted_heap` bounds the summed retained heap charges of the entries the
/// deleted keys name, so a caller that only ever deletes heap-free unit rows
/// is not priced as if it deleted the largest possible entry. `input_capacity`
/// bounds actual Vec capacity, including unused slots.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RangeWriteLimits {
    pub changed_keys: usize,
    pub deleted_keys: usize,
    pub deleted_heap: usize,
    pub incoming_heap: usize,
    pub input_capacity: usize,
}

/// Occupancy-independent additional demand for this exact owner incarnation.
/// Bounds include cumulative directory node construction, not just nodes that
/// survive publication; totals are conservative and maxima may not coincide.
/// No memory is reserved by obtaining or checking this value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RangeWriteEnvelope {
    owner: OwnerId,
    limits: RangeWriteLimits,
    /// Stores of the group one write is spread over: every member publishes
    /// a fragment, at most `changed_keys` of them nonempty.
    members: usize,
    max_base_pages: usize,
    max_new_pages: usize,
    input_pending_bytes: usize,
    directory_bytes: usize,
    new_pages_bytes: usize,
    merge_pending_bytes: usize,
    additional_retained_bytes: usize,
    additional_peak_bytes: usize,
}

impl RangeWriteEnvelope {
    pub fn limits(self) -> RangeWriteLimits {
        self.limits
    }
    pub fn members(self) -> usize {
        self.members
    }
    /// The same envelope with `bytes` more retained per write for what a
    /// range group keeps beside its stores (its further plans and
    /// fragments); the plans checked against it never claim these bytes.
    pub fn with_group_bytes(mut self, bytes: usize) -> Result<Self, MemoryError> {
        self.directory_bytes = checked_add(self.directory_bytes, bytes)?;
        self.additional_retained_bytes = checked_add(self.additional_retained_bytes, bytes)?;
        self.additional_peak_bytes = checked_add(self.additional_peak_bytes, bytes)?;
        Ok(self)
    }
    pub fn max_base_pages(self) -> usize {
        self.max_base_pages
    }
    pub fn max_new_pages(self) -> usize {
        self.max_new_pages
    }
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
    pub fn additional_retained_bytes(self) -> usize {
        self.additional_retained_bytes
    }
    pub fn additional_peak_bytes(self) -> usize {
        self.additional_peak_bytes
    }

    /// Check the actual immutable-base/owned-input plan before spending a
    /// guarantee based on this envelope. This rejects foreign owner incarnations
    /// even when RangeId/config match, excessive spare input capacity, and every
    /// shape/charge violation. It allocates nothing and consumes no candidate.
    pub fn check_plan<K: Ord + Clone, V>(
        &self,
        plan: &RangePreparationPlan<'_, K, V>,
    ) -> Result<(), MemoryError> {
        self.check_plans(std::iter::once(plan))
    }

    /// Check the fragments of one write across a range group, one plan per
    /// member in member order, against an envelope derived with
    /// [`RangeStore::future_write_envelope_shared`] for at least that many
    /// members. Sums are bounded by the whole write's limits; every plan must
    /// belong to the group's owner incarnation; a group of one is `check_plan`.
    pub fn check_plans<'a, 'b, K: Ord + Clone + 'a, V: 'a>(
        &self,
        plans: impl IntoIterator<Item = &'b RangePreparationPlan<'a, K, V>>,
    ) -> Result<(), MemoryError>
    where
        'a: 'b,
    {
        let mut members = 0usize;
        let mut changed = 0usize;
        let mut capacity = 0usize;
        let mut deleted = 0usize;
        let mut deleted_heap = 0usize;
        let mut incoming_heap = 0usize;
        let mut input_pending = 0usize;
        let mut directory = 0usize;
        let mut new_pages = 0usize;
        let mut merge_pending = 0usize;
        let mut retained = 0usize;
        for plan in plans {
            if self.owner != plan.base.owner {
                return Err(MemoryError::WrongRange);
            }
            members = checked_add(members, 1)?;
            changed = checked_add(changed, plan.changes.len())?;
            capacity = checked_add(capacity, plan.changes.capacity())?;
            for change in &plan.changes {
                match change {
                    Change::Put(entry) => {
                        incoming_heap = checked_add(incoming_heap, entry.heap_bytes)?;
                    }
                    Change::Delete(key) => {
                        deleted = checked_add(deleted, 1)?;
                        let existing = plan.base.get(key).ok_or(MemoryError::MissingKey)?;
                        deleted_heap = checked_add(deleted_heap, existing.heap_bytes)?;
                    }
                }
            }
            let actual = plan.charges();
            input_pending = checked_add(input_pending, actual.input_pending_bytes())?;
            directory = checked_add(directory, actual.directory_bytes())?;
            new_pages = checked_add(new_pages, actual.new_pages_bytes())?;
            // Merge staging lives only while one plan builds one page group.
            merge_pending = merge_pending.max(actual.merge_pending_bytes());
            retained = checked_add(retained, actual.additional_retained_bytes())?;
        }
        if members == 0 || members > self.members {
            return Err(MemoryError::InvalidConfiguration(
                "a range group publishes one fragment per member within its envelope",
            ));
        }
        fits(changed, self.limits.changed_keys)?;
        // Dividing one input vector between members keeps its capacity and
        // adds at most one slot per moved change.
        let spare = if self.members > 1 {
            self.limits.changed_keys
        } else {
            0
        };
        fits(capacity, checked_add(self.limits.input_capacity, spare)?)?;
        fits(deleted, self.limits.deleted_keys)?;
        fits(deleted_heap, self.limits.deleted_heap)?;
        fits(incoming_heap, self.limits.incoming_heap)?;
        // Checking each component prevents a cheaper, unrelated component from
        // masking an underquoted one if the preparation algorithm later changes.
        fits(input_pending, self.input_pending_bytes)?;
        fits(directory, self.directory_bytes)?;
        fits(new_pages, self.new_pages_bytes)?;
        fits(merge_pending, self.merge_pending_bytes)?;
        fits(retained, self.additional_retained_bytes)?;
        fits(
            checked_add(checked_add(retained, input_pending)?, merge_pending)?,
            self.additional_peak_bytes,
        )
    }
}

impl<K, V> RangeStore<K, V> {
    /// Derive one future write's conservative charge from immutable layout and
    /// owner-budget limits. The returned bound remains unchanged across growth,
    /// pending prefixes and pins in this owner incarnation. Use `check_plan`
    /// before construction; ordinary preparation APIs do not enforce this future
    /// contract implicitly. This API neither funds nor guarantees completion.
    pub fn future_write_envelope(
        &self,
        limits: RangeWriteLimits,
    ) -> Result<RangeWriteEnvelope, MemoryError> {
        self.future_write_envelope_shared(limits, 1)
    }

    /// The envelope of one future write spread over a group of `members`
    /// sibling stores (25 §4): the write's rows are divided by key, so every
    /// member's fragment is bounded by the whole write; every member
    /// publishes a fragment, so each costs a root; the input vector is divided
    /// once per touched member, which keeps its capacity and adds at most
    /// one slot per moved change; merge staging is per fragment and never
    /// simultaneous. Check the fragments with [`RangeWriteEnvelope::check_plans`].
    pub fn future_write_envelope_shared(
        &self,
        limits: RangeWriteLimits,
        members: usize,
    ) -> Result<RangeWriteEnvelope, MemoryError> {
        if members == 0 {
            return Err(MemoryError::InvalidConfiguration(
                "a range group has at least one member",
            ));
        }
        let RangeWriteLimits {
            changed_keys,
            deleted_keys,
            deleted_heap,
            incoming_heap,
            input_capacity,
        } = limits;
        if deleted_keys > changed_keys || input_capacity < changed_keys {
            return Err(MemoryError::InvalidConfiguration(
                "future write needs deleted keys <= changed keys <= input capacity",
            ));
        }
        if changed_keys == 0 && incoming_heap != 0 {
            return Err(MemoryError::InvalidConfiguration(
                "an empty future write cannot carry incoming heap",
            ));
        }
        if deleted_keys == 0 && deleted_heap != 0 {
            return Err(MemoryError::InvalidConfiguration(
                "a future write without deletions cannot release deleted heap",
            ));
        }
        fits(changed_keys, self.config.max_batch_entries)?;
        let owner_limit = self.budget.limit();
        let page_header = page_charge::<K, V>(0, 0)?;
        let minimum_page = page_charge::<K, V>(1, 0)?;
        let entry_inline = size_of::<Entry<K, V>>();
        let max_base_pages =
            owner_limit
                .checked_div(minimum_page)
                .ok_or(MemoryError::InvalidConfiguration(
                    "a directory page must have a nonzero charge",
                ))?;
        let ordinary_payload = self
            .config
            .page_bytes
            .min(owner_limit)
            .saturating_sub(page_header);
        // Includes an old singleton even if the ordinary page ceiling cannot
        // describe it; its merge descriptor is inspected, its payload not copied.
        let old_rows = self
            .config
            .page_entries
            .min(ordinary_payload.checked_div(entry_inline).ok_or(
                MemoryError::InvalidConfiguration("a range entry must have a nonzero charge"),
            )?)
            .max(usize::from(max_base_pages != 0));
        let input_buffer = if input_capacity == 0 {
            0
        } else {
            checked_add(
                ALLOCATOR_OVERHEAD,
                checked_mul(input_capacity, size_of::<Change<K, V>>())?,
            )?
        };
        // A deletion clones its key into the input; the plan bounds that clone
        // by the deleted entry's retained heap, which the caller bounds here.
        let input_pending_bytes =
            checked_add(input_buffer, checked_add(incoming_heap, deleted_heap)?)?;
        let touched = members.min(changed_keys.max(1));
        let input_pending_bytes = if touched > 1 {
            checked_add(
                input_pending_bytes,
                checked_add(
                    checked_mul(touched.saturating_sub(1), ALLOCATOR_OVERHEAD)?,
                    checked_mul(changed_keys, size_of::<Change<K, V>>())?,
                )?,
            )?
        } else {
            input_pending_bytes
        };
        let max_new_pages = checked_mul(3, changed_keys)?;
        let new_pages_bytes = checked_add(
            checked_mul(max_new_pages, page_header)?,
            checked_add(
                checked_mul(changed_keys, ordinary_payload)?,
                checked_add(checked_mul(changed_keys, entry_inline)?, incoming_heap)?,
            )?,
        )?;
        let directory_bytes = checked_add(
            checked_mul(members, root_charge::<K, V>(0)?)?,
            PageDirectory::<K, V>::edit_bound(
                max_new_pages,
                checked_add(max_base_pages, max_new_pages)?,
            )?,
        )?;
        let merge_pending_bytes = if changed_keys == 0 {
            0
        } else {
            merge_layout_charge::<K, V>(
                checked_add(old_rows, changed_keys)?,
                self.partition.is_some(),
            )?
        };
        let additional_retained_bytes = checked_add(directory_bytes, new_pages_bytes)?;
        let additional_peak_bytes = checked_add(
            additional_retained_bytes,
            checked_add(input_pending_bytes, merge_pending_bytes)?,
        )?;
        Ok(RangeWriteEnvelope {
            owner: self.root.owner,
            limits,
            members,
            max_base_pages,
            max_new_pages,
            input_pending_bytes,
            directory_bytes,
            new_pages_bytes,
            merge_pending_bytes,
            additional_retained_bytes,
            additional_peak_bytes,
        })
    }
}

fn fits(actual: usize, maximum: usize) -> Result<(), MemoryError> {
    if actual > maximum {
        return Err(MemoryError::Capacity {
            requested: actual,
            available: maximum,
        });
    }
    Ok(())
}
