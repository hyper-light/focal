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
/// Delete key accounting is separately bounded from the largest possible old
/// entry. `input_capacity` bounds actual Vec capacity, including unused slots.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RangeWriteLimits {
    pub changed_keys: usize,
    pub deleted_keys: usize,
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
        if self.owner != plan.base.owner {
            return Err(MemoryError::WrongRange);
        }
        fits(plan.changes.len(), self.limits.changed_keys)?;
        fits(plan.changes.capacity(), self.limits.input_capacity)?;
        let mut deleted = 0;
        let mut incoming_heap = 0;
        for change in &plan.changes {
            match change {
                Change::Put(entry) => incoming_heap = checked_add(incoming_heap, entry.heap_bytes)?,
                Change::Delete(_) => deleted = checked_add(deleted, 1)?,
            }
        }
        fits(deleted, self.limits.deleted_keys)?;
        fits(incoming_heap, self.limits.incoming_heap)?;
        let actual = plan.charges();
        // Checking each component prevents a cheaper, unrelated component from
        // masking an underquoted one if the preparation algorithm later changes.
        fits(actual.input_pending_bytes(), self.input_pending_bytes)?;
        fits(actual.directory_bytes(), self.directory_bytes)?;
        fits(actual.new_pages_bytes(), self.new_pages_bytes)?;
        fits(actual.merge_pending_bytes(), self.merge_pending_bytes)?;
        fits(
            actual.additional_retained_bytes(),
            self.additional_retained_bytes,
        )?;
        fits(actual.additional_peak_bytes(), self.additional_peak_bytes)
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
        let RangeWriteLimits {
            changed_keys,
            deleted_keys,
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
        let old_entry_heap = self
            .config
            .max_entry_bytes
            .min(owner_limit.saturating_sub(page_header))
            .saturating_sub(entry_inline);

        let input_buffer = if input_capacity == 0 {
            0
        } else {
            checked_add(
                ALLOCATOR_OVERHEAD,
                checked_mul(input_capacity, size_of::<Change<K, V>>())?,
            )?
        };
        let input_pending_bytes = checked_add(
            input_buffer,
            checked_add(incoming_heap, checked_mul(deleted_keys, old_entry_heap)?)?,
        )?;
        let max_new_pages = checked_mul(3, changed_keys)?;
        let new_pages_bytes = checked_add(
            checked_mul(max_new_pages, page_header)?,
            checked_add(
                checked_mul(changed_keys, ordinary_payload)?,
                checked_add(checked_mul(changed_keys, entry_inline)?, incoming_heap)?,
            )?,
        )?;
        let directory_bytes = checked_add(
            root_charge::<K, V>(0)?,
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
