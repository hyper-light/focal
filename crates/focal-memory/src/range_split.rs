//! Dividing and joining the stores of one range group along key boundaries
//! (25 §4). A group's stores share one owner identity (what a write envelope
//! checks), one lease clock, one configuration, budget and partition
//! classifier. A split shares every page wholly on one side of the boundary
//! and copies only the page that holds it, once per side; a merge shares
//! every page. Neither touches the source stores or their leases.

use super::{
    DirectoryBuild, Entry, Page, PageDirectory, RangeId, RangeStore, Root, bounded_vec,
    page_charge, root_charge,
};
use crate::{BudgetKind, BudgetLane, MemoryError, checked_add};
use std::{collections::BTreeMap, sync::Arc};

fn checked_sub(left: usize, right: usize) -> Result<usize, MemoryError> {
    left.checked_sub(right).ok_or(MemoryError::MissingKey)
}

impl<K: Ord + Clone, V> RangeStore<K, V> {
    /// An empty store at this store's prefix in this store's group: the
    /// member that will hold keys nothing has written yet.
    pub fn new_sibling(&self, id: RangeId) -> Result<Self, MemoryError> {
        let allocation = self
            .budget
            .reserve(
                BudgetKind::Roots,
                BudgetLane::Ordinary,
                root_charge::<K, V>(0)?,
            )?
            .commit();
        Ok(self.with_root(Root {
            owner: self.root.owner,
            range: id,
            prefix: self.root.prefix,
            pages: PageDirectory::new(),
            len: 0,
            _allocation: allocation,
        }))
    }

    /// Whether `other` belongs to this store's group.
    pub fn is_sibling(&self, other: &Self) -> bool {
        self.root.owner == other.root.owner && Arc::ptr_eq(&self.clock, &other.clock)
    }

    /// Divide this store's rows at `at` into a store `left` holding every
    /// key below `at` and a store `right` holding `at` and every key above
    /// it. Pages wholly on one side are shared; the page holding the
    /// boundary, if any, is copied once per side through `copy` under the
    /// copier contract of [`Self::prepare_batch_with`]. Both results carry
    /// this store's prefix, owner and clock and begin with no leases; this
    /// store is unchanged and its leases keep every page.
    pub fn split_with<F>(
        &self,
        at: &K,
        left: RangeId,
        right: RangeId,
        lane: BudgetLane,
        copy: F,
    ) -> Result<(Self, Self), MemoryError>
    where
        F: FnMut(&V) -> Result<V, MemoryError>,
    {
        self.split_where_with(|key| key >= at, left, right, lane, copy)
    }

    /// As [`Self::split_with`], dividing at the first key `is_right` holds
    /// for. The predicate must be monotone over the key order (false for a
    /// prefix of the keys, true for the rest), which lets a boundary be
    /// expressed by a property of keys, such as the object they belong to,
    /// rather than by a key value. The position is found by bisecting the
    /// pages and then one page.
    pub fn split_where_with<F>(
        &self,
        is_right: impl Fn(&K) -> bool,
        left: RangeId,
        right: RangeId,
        lane: BudgetLane,
        mut copy: F,
    ) -> Result<(Self, Self), MemoryError>
    where
        F: FnMut(&V) -> Result<V, MemoryError>,
    {
        let pages = &self.root.pages;
        let total = pages.len();
        // The first page whose last entry is right of the boundary holds it
        // (or every page is left of it: index == total, offset 0).
        let mut low = 0usize;
        let mut high = total;
        while low < high {
            let middle = low.saturating_add(high.saturating_sub(low) / 2);
            let crosses = pages
                .get(middle)
                .and_then(|page| page.entries.last())
                .is_some_and(|entry| is_right(&entry.key));
            if crosses {
                high = middle;
            } else {
                low = middle.saturating_add(1);
            }
        }
        let index = low.min(total.saturating_sub(1));
        let offset = if low == total {
            pages.get(index).map_or(0, |page| page.entries.len())
        } else {
            pages.get(index).map_or(0, |page| {
                page.entries.partition_point(|entry| !is_right(&entry.key))
            })
        };
        let boundary = pages.get(index);
        let boundary_len = boundary.map_or(0, |page| page.entries.len());
        let (head, tail) = match boundary {
            Some(page) if offset > 0 && offset < boundary_len => {
                let head = self.copy_page(
                    page.entries.get(..offset).ok_or(MemoryError::MissingKey)?,
                    lane,
                    &mut copy,
                )?;
                let tail = self.copy_page(
                    page.entries.get(offset..).ok_or(MemoryError::MissingKey)?,
                    lane,
                    &mut copy,
                )?;
                (Some(head), Some(tail))
            }
            _ => (None, None),
        };
        // Pages before the boundary page are wholly left; the boundary page
        // itself is wholly left when every entry lies below `at` and wholly
        // right when none does.
        let whole_left = if boundary.is_some() && offset == boundary_len {
            checked_add(index, 1)?
        } else {
            index
        };
        let whole_right_from = if boundary.is_some() && offset == 0 {
            index
        } else {
            checked_add(index, 1)?.min(total)
        };
        let left_count = checked_add(whole_left, usize::from(head.is_some()))?;
        let right_count = checked_add(
            checked_sub(total, whole_right_from)?,
            usize::from(tail.is_some()),
        )?;
        let mut left_build = DirectoryBuild::new(
            &self.budget,
            lane,
            PageDirectory::<K, V>::build_bound(left_count)?,
        );
        let left_pages = PageDirectory::from_pages(
            left_count,
            |position| {
                if position < whole_left {
                    pages.get(position).map(Arc::clone)
                } else {
                    head.clone()
                }
            },
            &mut left_build,
        )?;
        let mut right_build = DirectoryBuild::new(
            &self.budget,
            lane,
            PageDirectory::<K, V>::build_bound(right_count)?,
        );
        let right_pages = PageDirectory::from_pages(
            right_count,
            |position| match (&tail, position) {
                (Some(tail), 0) => Some(Arc::clone(tail)),
                (Some(_), position) => pages
                    .get(
                        checked_add(whole_right_from, position)
                            .ok()?
                            .checked_sub(1)?,
                    )
                    .map(Arc::clone),
                (None, position) => pages
                    .get(checked_add(whole_right_from, position).ok()?)
                    .map(Arc::clone),
            },
            &mut right_build,
        )?;
        let mut left_len = head.as_ref().map_or(0, |page| page.entries.len());
        for page in pages.iter().take(whole_left) {
            left_len = checked_add(left_len, page.entries.len())?;
        }
        let right_len = checked_sub(self.root.len, left_len)?;
        let left_allocation = self
            .budget
            .reserve(BudgetKind::Roots, lane, root_charge::<K, V>(0)?)?
            .commit();
        let right_allocation = self
            .budget
            .reserve(BudgetKind::Roots, lane, root_charge::<K, V>(0)?)?
            .commit();
        Ok((
            self.with_root(Root {
                owner: self.root.owner,
                range: left,
                prefix: self.root.prefix,
                pages: left_pages,
                len: left_len,
                _allocation: left_allocation,
            }),
            self.with_root(Root {
                owner: self.root.owner,
                range: right,
                prefix: self.root.prefix,
                pages: right_pages,
                len: right_len,
                _allocation: right_allocation,
            }),
        ))
    }

    /// Join this store and `right`, a sibling whose every key lies above
    /// this store's, at one prefix into one store `id` sharing every page.
    /// Both sources are unchanged.
    pub fn merge_with(
        &self,
        right: &Self,
        id: RangeId,
        lane: BudgetLane,
    ) -> Result<Self, MemoryError> {
        if !self.is_sibling(right) {
            return Err(MemoryError::WrongRange);
        }
        if self.root.prefix != right.root.prefix {
            return Err(MemoryError::PrefixMismatch {
                expected: self.root.prefix,
                actual: right.root.prefix,
            });
        }
        let last = self
            .root
            .pages
            .last()
            .and_then(|page| page.entries.last())
            .map(|entry| &entry.key);
        let first = right
            .root
            .pages
            .get(0)
            .and_then(|page| page.entries.first())
            .map(|entry| &entry.key);
        if last.zip(first).is_some_and(|(last, first)| last >= first) {
            return Err(MemoryError::InvalidNeighbors);
        }
        let left_count = self.root.pages.len();
        let count = checked_add(left_count, right.root.pages.len())?;
        let mut build = DirectoryBuild::new(
            &self.budget,
            lane,
            PageDirectory::<K, V>::build_bound(count)?,
        );
        let pages = PageDirectory::from_pages(
            count,
            |position| {
                if position < left_count {
                    self.root.pages.get(position).map(Arc::clone)
                } else {
                    right
                        .root
                        .pages
                        .get(checked_sub(position, left_count).ok()?)
                        .map(Arc::clone)
                }
            },
            &mut build,
        )?;
        let allocation = self
            .budget
            .reserve(BudgetKind::Roots, lane, root_charge::<K, V>(0)?)?
            .commit();
        Ok(self.with_root(Root {
            owner: self.root.owner,
            range: id,
            prefix: self.root.prefix,
            pages,
            len: checked_add(self.root.len, right.root.len)?,
            _allocation: allocation,
        }))
    }

    fn with_root(&self, root: Root<K, V>) -> Self {
        Self {
            root: Arc::new(root),
            budget: self.budget.clone(),
            config: self.config,
            partition: self.partition,
            pins: BTreeMap::new(),
            next_lease: 1,
            clock: Arc::clone(&self.clock),
        }
    }

    fn copy_page<F>(
        &self,
        entries: &[Entry<K, V>],
        lane: BudgetLane,
        copy: &mut F,
    ) -> Result<Arc<Page<K, V>>, MemoryError>
    where
        F: FnMut(&V) -> Result<V, MemoryError>,
    {
        let heap = entries
            .iter()
            .try_fold(0usize, |heap, entry| checked_add(heap, entry.heap_bytes))?;
        let bytes = page_charge::<K, V>(entries.len(), heap)?;
        let allocation = self
            .budget
            .reserve(BudgetKind::Pages, lane, bytes)?
            .commit();
        let mut copied = bounded_vec(entries.len())?;
        for entry in entries {
            if copied.len() >= copied.capacity() {
                return Err(MemoryError::MissingKey);
            }
            copied.push(Entry {
                key: entry.key.clone(),
                value: copy(&entry.value)?,
                heap_bytes: entry.heap_bytes,
            });
        }
        Ok(Arc::new(Page {
            entries: copied,
            _allocation: allocation,
        }))
    }
}
