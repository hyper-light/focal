//! One checked key/byte/count partition rule shared by quoting and construction.
//! Layout limits are immutable for an owner's lifetime; no value is copied here.

use super::{Change, Entry, Page, RangeConfig, page_charge};
use crate::{MemoryError, checked_add};

pub(super) fn validate<K, V>(config: RangeConfig) -> Result<(), MemoryError> {
    if config.page_bytes < page_charge::<K, V>(1, 0)?
        || config.max_entry_bytes < size_of::<Entry<K, V>>()
    {
        return Err(MemoryError::InvalidConfiguration(
            "range byte limits cannot hold one empty entry",
        ));
    }
    Ok(())
}

pub(super) fn check_entry<K, V>(heap: usize, config: RangeConfig) -> Result<usize, MemoryError> {
    let bytes = checked_add(size_of::<Entry<K, V>>(), heap)?;
    if bytes > config.max_entry_bytes {
        return Err(MemoryError::ItemTooLarge {
            bytes,
            limit: config.max_entry_bytes,
        });
    }
    Ok(bytes)
}

#[derive(Clone, Copy)]
pub(super) struct LeafSpan {
    pub start: usize,
    pub len: usize,
    pub heap: usize,
}

pub(super) struct LeafPartition {
    config: RangeConfig,
    base_bytes: usize,
    entry_bytes: usize,
    span: LeafSpan,
    bytes: usize,
}

impl LeafPartition {
    pub fn new<K, V>(config: RangeConfig) -> Result<Self, MemoryError> {
        let base_bytes = page_charge::<K, V>(0, 0)?;
        Ok(Self {
            config,
            base_bytes,
            entry_bytes: size_of::<Entry<K, V>>(),
            span: LeafSpan {
                start: 0,
                len: 0,
                heap: 0,
            },
            bytes: base_bytes,
        })
    }

    pub fn push(
        &mut self,
        heap: usize,
        boundary: bool,
        emit: &mut impl FnMut(LeafSpan) -> Result<(), MemoryError>,
    ) -> Result<(), MemoryError> {
        let bytes = checked_add(self.entry_bytes, heap)?;
        if bytes > self.config.max_entry_bytes {
            return Err(MemoryError::ItemTooLarge {
                bytes,
                limit: self.config.max_entry_bytes,
            });
        }
        if self.span.len != 0
            && (boundary
                || self.span.len == self.config.page_entries
                || bytes > self.config.page_bytes.saturating_sub(self.bytes))
        {
            self.finish(emit)?;
        }
        self.span.len = checked_add(self.span.len, 1)?;
        self.span.heap = checked_add(self.span.heap, heap)?;
        self.bytes = checked_add(self.bytes, bytes)?;
        // Oversized entries are emitted alone, never combined with a neighbor.
        if self.bytes > self.config.page_bytes {
            self.finish(emit)?;
        }
        Ok(())
    }

    pub fn finish(
        &mut self,
        emit: &mut impl FnMut(LeafSpan) -> Result<(), MemoryError>,
    ) -> Result<(), MemoryError> {
        if self.span.len == 0 {
            return Ok(());
        }
        emit(self.span)?;
        self.span.start = checked_add(self.span.start, self.span.len)?;
        self.span.len = 0;
        self.span.heap = 0;
        self.bytes = self.base_bytes;
        Ok(())
    }
}

/// Where an entry of a merged page group comes from: a row the page held and
/// keeps, a row replacing one the page held, or a key the page did not hold.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum Origin {
    Old,
    Replacement,
    Inserted,
}

/// What the cut rule reads of one entry of a merged page group.
pub(super) trait Cell {
    fn heap(&self) -> usize;
    fn origin(&self) -> Origin;
    /// Whether `next`, the entry after this one, belongs to another partition.
    fn parts_from(&self, next: &Self) -> bool;
}

/// Where the cuts of a run that needs more than one leaf fall.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Pack {
    /// Each leaf as full as it goes, from the left: rows appended past every
    /// row the page held, or rows of a fresh run, as a bulk load lays them.
    Left,
    /// Each leaf as full as it goes, from the right: rows prepended before
    /// every row the page held.
    Right,
    /// Leaves of nearly equal bytes: rows inserted among the page's rows.
    Even,
}

/// Cuts a merged page group into leaves. Partition boundaries and oversized
/// entries cut it into runs as [`LeafPartition`] does. A run takes the least
/// number of leaves the leaf bounds allow (what the greedy rule takes, and
/// what every quote counts), and its cuts fall where its rows came in.
///
/// Rows inserted among a page's rows cut it evenly by bytes: the split of a
/// B-tree node in half, whose leaves fill to ln 2 (69%) under random inserts
/// (Yao, "On random 2-3 trees", Acta Informatica 1978). The greedy rule kept a
/// full leaf full and put the rest after it, so every insert into a full leaf
/// made a leaf of one row: 120,000 random inserts left 6 rows a leaf of 128
/// (doc 30 §9). Rows appended past every row the page held, or into a fresh
/// run, pack each leaf full from the left, and rows prepended before them from
/// the right, so ordered inserts fill leaves (the rightmost split of
/// PostgreSQL's nbtree and InnoDB's sequential split point do likewise).
///
/// The cuts are chosen within the bounds every leaf keeps: the `j`th cut lies
/// no later than one leaf past the cut before it and no earlier than the
/// `j`th cut of the greedy rule run from the right, so every leaf fits and
/// the run takes exactly its least number of leaves. `scratch` holds the
/// right-hand cuts: its capacity must be at least the group's length.
pub(super) struct Cutter {
    config: RangeConfig,
    base_bytes: usize,
    entry_bytes: usize,
}

impl Cutter {
    pub fn new<K, V>(config: RangeConfig) -> Result<Self, MemoryError> {
        Ok(Self {
            config,
            base_bytes: page_charge::<K, V>(0, 0)?,
            entry_bytes: size_of::<Entry<K, V>>(),
        })
    }

    pub fn cut<T: Cell>(
        &self,
        cells: &[T],
        scratch: &mut Vec<usize>,
        emit: &mut impl FnMut(LeafSpan) -> Result<(), MemoryError>,
    ) -> Result<(), MemoryError> {
        let mut at = 0usize;
        while let Some(first) = cells.get(at) {
            let next = checked_add(at, 1)?;
            if self.oversized(self.bytes(first.heap())?) {
                emit(LeafSpan {
                    start: at,
                    len: 1,
                    heap: first.heap(),
                })?;
                at = next;
                continue;
            }
            let mut end = next;
            while let (Some(previous), Some(cell)) = (
                end.checked_sub(1).and_then(|index| cells.get(index)),
                cells.get(end),
            ) {
                if previous.parts_from(cell) || self.oversized(self.bytes(cell.heap())?) {
                    break;
                }
                end = checked_add(end, 1)?;
            }
            self.cut_run(cells, at, end, scratch, emit)?;
            at = end;
        }
        Ok(())
    }

    /// An entry's bytes in a leaf, refused past the entry bound.
    fn bytes(&self, heap: usize) -> Result<usize, MemoryError> {
        let bytes = checked_add(self.entry_bytes, heap)?;
        if bytes > self.config.max_entry_bytes {
            return Err(MemoryError::ItemTooLarge {
                bytes,
                limit: self.config.max_entry_bytes,
            });
        }
        Ok(bytes)
    }

    /// An entry no leaf can hold beside another: it is a leaf of its own.
    fn oversized(&self, bytes: usize) -> bool {
        bytes > self.config.page_bytes.saturating_sub(self.base_bytes)
    }

    /// The end of the longest leaf from `start`, within `..end`: the greedy
    /// rule's next cut.
    fn reach<T: Cell>(&self, cells: &[T], start: usize, end: usize) -> Result<usize, MemoryError> {
        let (mut count, mut bytes, mut at) = (0usize, self.base_bytes, start);
        while at < end {
            let cell = cells.get(at).ok_or(MemoryError::MissingKey)?;
            let size = self.bytes(cell.heap())?;
            if count != 0
                && (count >= self.config.page_entries
                    || size > self.config.page_bytes.saturating_sub(bytes))
            {
                break;
            }
            count = checked_add(count, 1)?;
            bytes = checked_add(bytes, size)?;
            at = checked_add(at, 1)?;
        }
        Ok(at)
    }

    /// The start of the longest leaf ending at `end`, within `start..`: the
    /// greedy rule's cut run from the right.
    fn reach_back<T: Cell>(
        &self,
        cells: &[T],
        start: usize,
        end: usize,
    ) -> Result<usize, MemoryError> {
        let (mut count, mut bytes, mut at) = (0usize, self.base_bytes, end);
        while at > start {
            let index = at.checked_sub(1).ok_or(MemoryError::MissingKey)?;
            let cell = cells.get(index).ok_or(MemoryError::MissingKey)?;
            let size = self.bytes(cell.heap())?;
            if count != 0
                && (count >= self.config.page_entries
                    || size > self.config.page_bytes.saturating_sub(bytes))
            {
                break;
            }
            count = checked_add(count, 1)?;
            bytes = checked_add(bytes, size)?;
            at = index;
        }
        Ok(at)
    }

    /// Cuts `start..end`, a run with no partition boundary and no oversized
    /// entry in it.
    fn cut_run<T: Cell>(
        &self,
        cells: &[T],
        start: usize,
        end: usize,
        scratch: &mut Vec<usize>,
        emit: &mut impl FnMut(LeafSpan) -> Result<(), MemoryError>,
    ) -> Result<(), MemoryError> {
        let run = cells.get(start..end).ok_or(MemoryError::MissingKey)?;
        let span = |from: usize, to: usize| -> Result<LeafSpan, MemoryError> {
            let heap = cells
                .get(from..to)
                .ok_or(MemoryError::MissingKey)?
                .iter()
                .try_fold(0usize, |sum, cell| checked_add(sum, cell.heap()))?;
            Ok(LeafSpan {
                start: from,
                len: to.checked_sub(from).ok_or(MemoryError::MissingKey)?,
                heap,
            })
        };
        // The least number of leaves: the greedy rule's.
        let mut leaves = 0usize;
        let mut at = start;
        while at < end {
            at = self.reach(cells, at, end)?;
            leaves = checked_add(leaves, 1)?;
        }
        if leaves <= 1 {
            return if start < end {
                emit(span(start, end)?)
            } else {
                Ok(())
            };
        }
        let pack = pack(run);
        // The right-hand cuts, the least each cut may be, nearest the end first.
        scratch.clear();
        let cuts = leaves.checked_sub(1).ok_or(MemoryError::MissingKey)?;
        if scratch.capacity() < cuts {
            return Err(MemoryError::InvalidConfiguration(
                "leaf cut scratch below its group's length",
            ));
        }
        let mut back = end;
        for _ in 0..cuts {
            back = self.reach_back(cells, start, back)?;
            scratch.push(back);
        }
        let total = run.iter().try_fold(0usize, |sum, cell| {
            checked_add(sum, self.bytes(cell.heap())?)
        })?;
        // The even target's cursor: the bytes of `start..cursor`.
        let (mut cursor, mut before) = (start, 0usize);
        let mut previous = start;
        for (index, least) in scratch.iter().rev().enumerate() {
            let most = self.reach(cells, previous, end)?;
            let target = match pack {
                Pack::Left => most,
                Pack::Right => *least,
                Pack::Even => {
                    // The cut whose bytes before it are nearest the `j`th of
                    // `leaves` equal parts.
                    let part = checked_add(index, 1)?;
                    let goal = u128::try_from(total)
                        .ok()
                        .and_then(|total| total.checked_mul(u128::try_from(part).ok()?))
                        .and_then(|bytes| bytes.checked_div(u128::try_from(leaves).ok()?))
                        .and_then(|goal| usize::try_from(goal).ok())
                        .ok_or(MemoryError::CounterExhausted("leaf cut target"))?;
                    while let Some(cell) = cells.get(cursor).filter(|_| cursor < end) {
                        let size = self.bytes(cell.heap())?;
                        if checked_add(before, size)? > goal {
                            // Past the goal by more than it falls short: cut after.
                            if goal.saturating_sub(before).saturating_mul(2) > size {
                                cursor = checked_add(cursor, 1)?;
                                before = checked_add(before, size)?;
                            }
                            break;
                        }
                        cursor = checked_add(cursor, 1)?;
                        before = checked_add(before, size)?;
                    }
                    cursor
                }
            };
            if *least <= previous || *least > most {
                return Err(MemoryError::InvalidConfiguration("leaf cut bounds crossed"));
            }
            let cut = target.clamp(*least, most);
            emit(span(previous, cut)?)?;
            previous = cut;
        }
        emit(span(previous, end)?)
    }
}

/// How a run's cuts fall, from where its inserted rows lie among its old ones.
fn pack<T: Cell>(run: &[T]) -> Pack {
    let (mut first_old, mut last_old) = (None, None);
    let (mut first_insert, mut last_insert) = (None, None);
    for (index, cell) in run.iter().enumerate() {
        match cell.origin() {
            Origin::Old => {
                first_old.get_or_insert(index);
                last_old = Some(index);
            }
            Origin::Inserted => {
                first_insert.get_or_insert(index);
                last_insert = Some(index);
            }
            Origin::Replacement => {}
        }
    }
    match (first_old, last_old, first_insert, last_insert) {
        (None, ..) => Pack::Left,
        (Some(_), Some(last_old), Some(first_insert), _) if first_insert > last_old => Pack::Left,
        (Some(first_old), _, _, Some(last_insert)) if last_insert < first_old => Pack::Right,
        _ => Pack::Even,
    }
}

/// Reuse an unchanged isolated leaf when all changes sit outside its key span.
/// Oversized singletons retain the original byte-isolation rule. Partitioned
/// ordinary pages can remain shared when each adjacent incoming key starts a
/// different partition. Only counts escape; owned changes can then be consumed.
pub(super) fn reusable_page<K: crate::RangeKey, V>(
    page: &Page<K, V>,
    changes: &[Change<K, V>],
    config: RangeConfig,
    partition: Option<fn(&K) -> u64>,
) -> Result<Option<usize>, MemoryError> {
    if let [entry] = page.entries.as_slice()
        && page_charge::<K, V>(1, entry.heap_bytes)? > config.page_bytes
    {
        return Ok(changes
            .binary_search_by(|change| change.key().cmp(&entry.key))
            .err());
    }
    let Some(classify) = partition else {
        return Ok(None);
    };
    let first = page.entries.first().ok_or(MemoryError::MissingKey)?;
    let last = page.entries.last().ok_or(MemoryError::MissingKey)?;
    let split = changes.partition_point(|change| change.key() < &first.key);
    // A replacement, deletion or insertion within the retained page still
    // needs its normal ordered merge, even if the classifier is nonmonotonic.
    if changes
        .get(split)
        .is_some_and(|change| change.key() <= &last.key)
    {
        return Ok(None);
    }
    let label = classify(&first.key);
    if split
        .checked_sub(1)
        .and_then(|index| changes.get(index))
        .is_some_and(|change| classify(change.key()) == label)
        || changes
            .get(split)
            .is_some_and(|change| classify(change.key()) == label)
    {
        return Ok(None);
    }
    Ok(Some(split))
}
