//! The words a synchronization primitive's ends share (docs/runtime.md §8): a process-wide table of cells,
//! each a few atomics with a generation, claimed by the primitive that needs one and freed by the last of its
//! handles. A cell holds no value of the primitive's type, only words: the waiting task's word, a state word
//! and a version, so sharing it is a generational arena, never shared ownership of a value (no `Arc`).
//!
//! **Bounded.** At most [`MAX_CELLS`] cells live at once, and fewer when the consumer sets a lower
//! [`set_limit`]; past it a claim is refused `Capacity`. Cells are allocated a segment of [`SEGMENT`] at a
//! time, when first needed, and a segment is never freed (its cells are reused), so the table's memory is the
//! most cells ever live at once, rounded up to a segment.
//!
//! **Claiming.** A two-level bitmap of free cells: a word per segment (a bit per cell) and a word per 64
//! segments (a bit per segment that may have a free cell, set for segments not yet allocated). A claim finds a
//! marked segment and clears one free bit with a compare-and-swap; a free sets the bits again. No lock, and
//! a claim reads at most every summary word once.
//!
//! **Stale handles.** A cell's generation is bumped when it is freed, and every access through a
//! [`CellRef`] checks it first: a handle that outlived its cell finds the cell closed, never another
//! primitive's state.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};

use crate::error::RtError;
use crate::mem::Encoded;

/// Format: cells per segment, one bit each in the segment's free word.
pub const SEGMENT: usize = 64;

/// Derived: the most cells at once: as many as a task word's slot field names (`Encoded::MAX_SLOT + 1`,
/// 2^24), so a cell's index packs as a task's slot does. A bound, not an allocation: segments are allocated
/// as cells are first needed.
pub const MAX_CELLS: usize = 1 << 24;

/// Derived: the segments [`MAX_CELLS`] needs.
const SEGMENTS: usize = MAX_CELLS / SEGMENT;

/// Derived: the summary words, one bit per segment.
const SUMMARY_WORDS: usize = SEGMENTS / SEGMENT;

/// Format: "no task waits" in a cell's waiter word (no task word is all ones: its generation field would be
/// `Encoded::ANY_GENERATION` and its shard `u16::MAX`, which the registry never issues).
const NO_WAITER: u64 = u64::MAX;

/// One cell: its generation (bumped at each free), the handles that keep it, the waiting task's word, and
/// two words the primitive uses as it chooses.
#[derive(Debug)]
pub(crate) struct Cell {
    generation: AtomicU32,
    handles: AtomicU32,
    waiter: AtomicU64,
    /// The primitive's state word (a flag set, a value, a permit count).
    pub(crate) state: AtomicU64,
    /// The primitive's second word (a version, a queue length).
    pub(crate) aux: AtomicU64,
}

impl Cell {
    const fn new() -> Self {
        Self {
            generation: AtomicU32::new(0),
            handles: AtomicU32::new(0),
            waiter: AtomicU64::new(NO_WAITER),
            state: AtomicU64::new(0),
            aux: AtomicU64::new(0),
        }
    }
}

/// A segment of cells and its free bits.
#[derive(Debug)]
struct Segment {
    cells: [Cell; SEGMENT],
    free: AtomicU64,
}

/// The segments, each allocated when first needed and kept for the process.
static TABLE: [OnceLock<Box<Segment>>; SEGMENTS] = [const { OnceLock::new() }; SEGMENTS];

/// A bit per segment that may have a free cell: every segment starts free.
static NONFULL: [AtomicU64; SUMMARY_WORDS] = [const { AtomicU64::new(u64::MAX) }; SUMMARY_WORDS];

/// Cells live now.
static LIVE: AtomicUsize = AtomicUsize::new(0);

/// The consumer's bound on live cells, at most [`MAX_CELLS`].
static LIMIT: AtomicUsize = AtomicUsize::new(MAX_CELLS);

/// Sets the most cells that may be live at once (at most [`MAX_CELLS`]): the consumer's bound on its live
/// synchronization primitives. A claim past it is refused `Capacity`.
pub fn set_limit(cells: usize) {
    LIMIT.store(cells.min(MAX_CELLS), Ordering::Release);
}

/// Cells live now.
pub fn live() -> usize {
    LIVE.load(Ordering::Acquire)
}

/// A handle to a cell: its index and the generation it was claimed at. `Copy`; every access checks the
/// generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CellRef {
    index: u32,
    generation: u32,
}

fn segment(index: usize) -> Option<&'static Segment> {
    TABLE.get(index).map(|slot| {
        &**slot.get_or_init(|| {
            Box::new(Segment {
                cells: [const { Cell::new() }; SEGMENT],
                free: AtomicU64::new(u64::MAX),
            })
        })
    })
}

/// Claims a cell for a primitive with `handles` handles, its words zeroed and no waiter. Refused `Capacity`
/// at the bound.
pub(crate) fn claim(handles: u32) -> Result<CellRef, RtError> {
    let limit = LIMIT.load(Ordering::Acquire);
    let refused = RtError::Capacity {
        what: "synchronization cells",
        bound: limit,
    };
    if LIVE.fetch_add(1, Ordering::AcqRel) >= limit {
        LIVE.fetch_sub(1, Ordering::AcqRel);
        return Err(refused);
    }
    for (high, summary) in NONFULL.iter().enumerate() {
        let mut marked = summary.load(Ordering::Acquire);
        while marked != 0 {
            let low = usize::try_from(marked.trailing_zeros()).unwrap_or(0);
            marked &= marked.wrapping_sub(1);
            let index = high.saturating_mul(SEGMENT).saturating_add(low);
            if let Some(cell) = claim_in(index, high, low) {
                return Ok(start(cell, handles));
            }
        }
    }
    LIVE.fetch_sub(1, Ordering::AcqRel);
    Err(refused)
}

/// Claims a free cell of segment `index` (the `low` bit of summary word `high`), clearing the segment's
/// summary bit when it is full.
fn claim_in(index: usize, high: usize, low: usize) -> Option<(usize, &'static Cell)> {
    let segment = segment(index)?;
    loop {
        let free = segment.free.load(Ordering::Acquire);
        if free == 0 {
            // Full: say so, then look again, so a cell freed meanwhile is advertised again rather than lost.
            let summary = NONFULL.get(high)?;
            summary.fetch_and(!(1u64 << low), Ordering::AcqRel);
            if segment.free.load(Ordering::Acquire) != 0 {
                summary.fetch_or(1u64 << low, Ordering::AcqRel);
                continue;
            }
            return None;
        }
        let bit = usize::try_from(free.trailing_zeros()).unwrap_or(0);
        let taken = free & !(1u64 << bit);
        if segment
            .free
            .compare_exchange(free, taken, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            let cell = segment.cells.get(bit)?;
            return Some((index.saturating_mul(SEGMENT).saturating_add(bit), cell));
        }
    }
}

/// Readies a claimed cell and hands back its handle.
fn start((index, cell): (usize, &'static Cell), handles: u32) -> CellRef {
    cell.waiter.store(NO_WAITER, Ordering::Release);
    cell.state.store(0, Ordering::Release);
    cell.aux.store(0, Ordering::Release);
    cell.handles.store(handles, Ordering::Release);
    CellRef {
        index: u32::try_from(index).unwrap_or(u32::MAX),
        generation: cell.generation.load(Ordering::Acquire),
    }
}

impl CellRef {
    /// The cell, while this handle's generation is its own.
    pub(crate) fn cell(self) -> Option<&'static Cell> {
        let index = usize::try_from(self.index).ok()?;
        let segment = TABLE.get(index / SEGMENT)?.get()?;
        let cell = segment.cells.get(index % SEGMENT)?;
        (cell.generation.load(Ordering::Acquire) == self.generation).then_some(cell)
    }

    /// Adds a handle (a cloned end).
    pub(crate) fn retain(self) {
        if let Some(cell) = self.cell() {
            cell.handles.fetch_add(1, Ordering::AcqRel);
        }
    }

    /// The handle as one word, for a cell's `aux` to name another cell (a chain).
    pub(crate) fn to_word(self) -> u64 {
        u64::from(self.index) | (u64::from(self.generation) << 32)
    }

    /// The handle a word from [`CellRef::to_word`] names.
    pub(crate) fn from_word(word: u64) -> CellRef {
        CellRef {
            index: u32::try_from(word & u64::from(u32::MAX)).unwrap_or(u32::MAX),
            generation: u32::try_from(word >> 32).unwrap_or(u32::MAX),
        }
    }

    /// Drops a handle; the last frees the cell.
    pub(crate) fn release(self) {
        let _ = self.release_last();
    }

    /// Drops a handle, and says whether it was the last, which freed the cell.
    pub(crate) fn release_last(self) -> bool {
        let Some(cell) = self.cell() else {
            return false;
        };
        if cell.handles.fetch_sub(1, Ordering::AcqRel) != 1 {
            return false;
        }
        cell.generation.fetch_add(1, Ordering::AcqRel);
        cell.waiter.store(NO_WAITER, Ordering::Release);
        let index = usize::try_from(self.index).unwrap_or(usize::MAX);
        let (segment_index, bit) = (index / SEGMENT, index % SEGMENT);
        if let Some(segment) = TABLE.get(segment_index).and_then(OnceLock::get) {
            segment.free.fetch_or(1u64 << bit, Ordering::AcqRel);
        }
        if let Some(summary) = NONFULL.get(segment_index / SEGMENT) {
            summary.fetch_or(1u64 << (segment_index % SEGMENT), Ordering::AcqRel);
        }
        LIVE.fetch_sub(1, Ordering::AcqRel);
        true
    }

    /// Forgets the waiting task, if any (its handle leaves the cell to a next user).
    pub(crate) fn clear_waiter(self) {
        if let Some(cell) = self.cell() {
            crate::handoff::clear_waiter(&cell.waiter);
        }
    }

    /// Records `word` as the task to wake (replacing any earlier one: one waiter at a time). A
    /// read-modify-write, so a waiter that registers and then checks the state never misses a publisher
    /// that changes the state and then wakes (`crate::handoff`; mantle's final review, second pass,
    /// finding 2: a plain store let both sides read stale values).
    pub(crate) fn register(self, word: Encoded) {
        if let Some(cell) = self.cell() {
            crate::handoff::register_waiter(&cell.waiter, word.word());
        }
    }

    /// Wakes the waiting task, if one waits (taking it).
    pub(crate) fn wake(self) {
        let Some(cell) = self.cell() else {
            return;
        };
        if let Some(word) = crate::handoff::take_waiter(&cell.waiter) {
            crate::registry::wake(Encoded::from_word(word));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cell_is_freed_by_its_last_handle_and_a_stale_handle_finds_it_closed() {
        let cell = claim(2).unwrap();
        assert!(cell.cell().is_some());
        cell.release();
        assert!(cell.cell().is_some(), "one handle remains");
        cell.release();
        assert!(
            cell.cell().is_none(),
            "freed: the stale handle finds it closed"
        );
        let again = claim(1).unwrap();
        assert!(
            cell.cell().is_none(),
            "a reuse of the slot is not the stale handle's"
        );
        again.release();
    }

    #[test]
    fn claims_never_share_a_cell_across_threads() {
        let mut claimed: Vec<CellRef> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..4)
                .map(|_| scope.spawn(|| (0..500).map(|_| claim(1).unwrap()).collect::<Vec<_>>()))
                .collect();
            handles
                .into_iter()
                .flat_map(|h| h.join().unwrap())
                .collect()
        });
        claimed.sort_by_key(|cell| cell.index);
        claimed.dedup_by_key(|cell| cell.index);
        assert_eq!(claimed.len(), 2_000, "every claim took its own cell");
        for cell in claimed {
            cell.release();
        }
    }
}
