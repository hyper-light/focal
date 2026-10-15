use std::ops::Range;

use tinyvec::TinyVec;

/// A set of u64 values optimized for long runs and random insert/delete/contains
///
/// `ArrayRangeSet` uses an array representation, where each array entry represents
/// a range.
///
/// The array-based RangeSet provides 2 benefits:
/// - There exists an inline representation, which avoids the need of heap
///   allocating ACK ranges for SentFrames for small ranges.
/// - Iterating over ranges should usually be faster since there is only
///   a single cache-friendly contiguous range.
///
/// `ArrayRangeSet` is especially useful for tracking ACK ranges where the amount
/// of ranges is usually very low (since ACK numbers are in consecutive fashion
/// unless reordering or packet loss occur).
#[derive(Debug, Default)]
pub struct ArrayRangeSet(TinyVec<[Range<u64>; ARRAY_RANGE_SET_INLINE_CAPACITY]>);

/// The capacity of elements directly stored in [`ArrayRangeSet`]
///
/// An inline capacity of 2 is chosen to keep `SentFrame` below 128 bytes.
const ARRAY_RANGE_SET_INLINE_CAPACITY: usize = 2;

impl Clone for ArrayRangeSet {
    fn clone(&self) -> Self {
        // tinyvec keeps the heap representation after clones.
        // We rather prefer the inline representation for clones if possible,
        // since clones (e.g. for storage in `SentFrames`) are rarely mutated
        if self.0.is_inline() || self.0.len() > ARRAY_RANGE_SET_INLINE_CAPACITY {
            return Self(self.0.clone());
        }

        let mut vec = TinyVec::new();
        vec.extend_from_slice(self.0.as_slice());
        Self(vec)
    }
}

impl ArrayRangeSet {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = Range<u64>> + '_ {
        self.0.iter().cloned()
    }

    pub fn elts(&self) -> impl Iterator<Item = u64> + '_ {
        self.iter().flatten()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn contains(&self, x: u64) -> bool {
        for range in self.0.iter() {
            if range.start > x {
                // We only get here if there was no prior range that contained x
                return false;
            } else if range.contains(&x) {
                return true;
            }
        }
        false
    }

    pub fn subtract(&mut self, other: &Self) {
        // TODO: This can potentially be made more efficient, since the we know
        // individual ranges are not overlapping, and the next range must start
        // after the last one finished
        for range in &other.0 {
            self.remove(range.clone());
        }
    }

    /// Inserts `x`. `u64::MAX` has no half-open range and is not inserted; the values inserted
    /// are packet numbers and stream offsets, below 2^62.
    pub fn insert_one(&mut self, x: u64) -> bool {
        x.checked_add(1).is_some_and(|end| self.insert(x..end))
    }

    pub fn insert(&mut self, x: Range<u64>) -> bool {
        let mut result = false;

        if x.is_empty() {
            // Don't try to deal with ranges where x.end <= x.start
            return false;
        }

        let mut idx = 0;
        while let Some(range) = self.0.get_mut(idx) {
            if range.start > x.end {
                // The range is fully before this range and therefore not extensible.
                // Add a new range to the left
                self.0.insert(idx, x);
                return true;
            } else if range.start > x.start {
                // The new range starts before this range but overlaps.
                // Extend the current range to the left
                // Note that we don't have to merge a potential left range, since
                // this case would have been captured by merging the right range
                // in the previous loop iteration
                result = true;
                range.start = x.start;
            }

            // At this point we have handled all parts of the new range which
            // are in front of the current range. Now we handle everything from
            // the start of the current range

            if x.end <= range.end {
                // Fully contained
                return result;
            } else if x.start <= range.end {
                // Extend the current range to the end of the new range.
                // Since it's not contained it must be bigger
                range.end = x.end;

                self.merge_following(idx);
                return true;
            }

            // `idx` is below the length, so the next index is at most the length.
            idx = idx.saturating_add(1);
        }

        // Insert a range at the end
        self.0.push(x);
        true
    }

    pub fn remove(&mut self, x: Range<u64>) -> bool {
        let mut result = false;

        if x.is_empty() {
            // Don't try to deal with ranges where x.end <= x.start
            return false;
        }

        // `idx` stays at most the length: it steps past ranges that exist.
        let mut idx = 0;
        while let (Some(slot), false) = (self.0.get_mut(idx), x.start == x.end) {
            let range = slot.clone();

            if x.end <= range.start {
                // The range is fully before this range
                return result;
            } else if x.start >= range.end {
                // The range is fully after this range
                idx = idx.saturating_add(1);
                continue;
            }

            // The range overlaps with this range
            result = true;

            let left = range.start..x.start;
            let right = x.end..range.end;
            if left.is_empty() && right.is_empty() {
                self.0.remove(idx);
            } else if left.is_empty() {
                *slot = right;
                idx = idx.saturating_add(1);
            } else if right.is_empty() {
                *slot = left;
                idx = idx.saturating_add(1);
            } else {
                *slot = right;
                self.0.insert(idx, left);
                idx = idx.saturating_add(2);
            }
        }

        result
    }

    /// Merges the ranges after `idx` that overlap the range at `idx` into it.
    fn merge_following(&mut self, idx: usize) {
        let next_idx = idx.saturating_add(1);
        while let (Some(curr_end), Some(next)) = (
            self.0.get(idx).map(|range| range.end),
            self.0.get(next_idx).cloned(),
        ) {
            if curr_end < next.start {
                break;
            }
            if let Some(curr) = self.0.get_mut(idx) {
                curr.end = next.end.max(curr_end);
            }
            self.0.remove(next_idx);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn pop_min(&mut self) -> Option<Range<u64>> {
        if !self.0.is_empty() {
            Some(self.0.remove(0))
        } else {
            None
        }
    }

    pub fn min(&self) -> Option<u64> {
        self.iter().next().map(|x| x.start)
    }

    pub fn max(&self) -> Option<u64> {
        // Ranges are non-empty, so `end` is at least 1.
        self.iter().next_back().and_then(|x| x.end.checked_sub(1))
    }
}
