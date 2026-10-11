//! What a cohort's first read of its claim's declarations gave, held for the
//! acceptance passes after it.

/// How many of a claim's declarations a cohort holds.
pub(super) const HELD: usize = 8;

/// A claim's declarations as its cohort's first read gave them, replayed to
/// the acceptance source's later passes, each of which read, checked and
/// stamped every body from the frame again (17 reads of a legacy claim's body,
/// 2026-10-11). A frame is borrowed and does not change, so a replay is what a
/// read would give. A claim of more than [`HELD`] is read at every pass.
#[derive(Clone, Copy)]
pub(super) struct Held<T: Copy> {
    values: [Option<T>; HELD],
    len: usize,
    complete: bool,
}

impl<T: Copy> Held<T> {
    pub(super) const fn none() -> Self {
        Self {
            values: [None; HELD],
            len: 0,
            complete: false,
        }
    }

    /// Reads `values` through, holding what fits, and counts them; refused as
    /// the read is.
    pub(super) fn read<E>(values: impl Iterator<Item = Result<T, E>>) -> Result<(Self, usize), E> {
        let mut held = Self::none();
        let mut count = 0usize;
        for value in values {
            let value = value?;
            if let Some(slot) = held.values.get_mut(count) {
                *slot = Some(value);
            }
            count = count.saturating_add(1);
        }
        held.len = count;
        held.complete = count <= HELD;
        Ok((held, count))
    }

    /// Holds `value` after those held, counted whether or not it fits.
    pub(super) fn push(&mut self, value: T) {
        if let Some(slot) = self.values.get_mut(self.len) {
            *slot = Some(value);
        }
        self.len = self.len.saturating_add(1);
        self.complete = self.len <= HELD;
    }

    /// What is held, where every value fits; none where the claim has more.
    pub(super) fn replay(&self) -> Option<&[Option<T>]> {
        self.values.get(..self.len).filter(|_| self.complete)
    }
}
