//! The anti-replay window (RFC 4303 §3.4.3, Appendix A).
//!
//! The right edge is the highest counter whose datagram verified. A counter at or left of the
//! window's left edge, or one already seen inside it, is refused before any cryptography; the
//! window moves only after a tag verifies, so a forged datagram with a high counter cannot lock
//! out honest ones.
//!
//! The width starts at RFC 4303's default of 64 counters and widens to the reordering measured on
//! the path, within the limit the plane reserves. Widening is driven only by authenticated
//! datagrams: when a verified datagram arrives later than half the width, the width doubles, up
//! to the limit. Forged or replayed datagrams never verify, so they cannot inflate it.

use crate::Refusal;

/// RFC 4303 §3.4.3's default window: 64 counters, one word of bits.
pub const DEFAULT_WIDTH: usize = 64;
/// Bits per word of the bitmap.
const WORD_BITS: usize = 64;

/// One direction's replay state for one epoch.
#[derive(Debug)]
pub(crate) struct ReplayWindow {
    /// The highest counter that verified, or `None` before any.
    right: Option<u64>,
    /// Bit `i` set means counter `right - i` was seen. Word 0 holds `i` in `0..64`.
    seen: Vec<u64>,
    /// The width the window may widen to, in counters, a whole number of words.
    limit: usize,
}

impl ReplayWindow {
    /// A window of [`DEFAULT_WIDTH`] that may widen to `limit` counters, rounded up to whole
    /// words and never below the default.
    pub(crate) fn new(limit: usize) -> Self {
        let limit = limit
            .max(DEFAULT_WIDTH)
            .div_ceil(WORD_BITS)
            .saturating_mul(WORD_BITS);
        // The bitmap is reserved to the limit now, at install, so widening on the receive path
        // never reallocates: at most `limit / 8` bytes an epoch.
        let mut seen = Vec::with_capacity(limit.checked_div(WORD_BITS).unwrap_or(0));
        seen.resize(DEFAULT_WIDTH / WORD_BITS, 0);
        Self {
            right: None,
            seen,
            limit,
        }
    }

    /// The current width in counters.
    pub(crate) fn width(&self) -> usize {
        self.seen.len().saturating_mul(WORD_BITS)
    }

    /// How far left of the right edge `counter` is, or `None` if it is at or right of it.
    fn depth(&self, counter: u64) -> Option<u64> {
        self.right.and_then(|right| right.checked_sub(counter))
    }

    /// Whether `counter` may be opened: refused if it is outside the window on the left or
    /// already seen. Changes nothing.
    pub(crate) fn check(&self, counter: u64) -> Result<(), Refusal> {
        let Some(depth) = self.depth(counter) else {
            return Ok(());
        };
        let width = u64::try_from(self.width()).unwrap_or(u64::MAX);
        if depth >= width {
            return Err(Refusal::Stale);
        }
        if self.bit(depth) {
            return Err(Refusal::Replay);
        }
        Ok(())
    }

    /// Records `counter`, whose datagram verified and was [`check`](Self::check)ed.
    pub(crate) fn accept(&mut self, counter: u64) {
        match self.depth(counter) {
            Some(depth) => {
                self.set_bit(depth);
                self.widen_for(depth);
            }
            None => {
                let shift = self
                    .right
                    .map_or(u64::MAX, |right| counter.saturating_sub(right));
                self.shift(shift);
                self.right = Some(counter);
                self.set_bit(0);
            }
        }
    }

    fn word_and_mask(depth: u64) -> Option<(usize, u64)> {
        let depth = usize::try_from(depth).ok()?;
        let word = depth.checked_div(WORD_BITS)?;
        let bit = depth.checked_rem(WORD_BITS)?;
        let mask = 1u64.checked_shl(u32::try_from(bit).ok()?)?;
        Some((word, mask))
    }

    fn bit(&self, depth: u64) -> bool {
        Self::word_and_mask(depth)
            .and_then(|(word, mask)| self.seen.get(word).map(|bits| bits & mask != 0))
            .unwrap_or(false)
    }

    fn set_bit(&mut self, depth: u64) {
        if let Some((word, mask)) = Self::word_and_mask(depth)
            && let Some(bits) = self.seen.get_mut(word)
        {
            *bits |= mask;
        }
    }

    /// Moves every seen bit `by` counters deeper; bits that pass the left edge are forgotten.
    fn shift(&mut self, by: u64) {
        let width = u64::try_from(self.width()).unwrap_or(u64::MAX);
        if by >= width {
            self.seen.iter_mut().for_each(|bits| *bits = 0);
            return;
        }
        let Ok(by) = usize::try_from(by) else {
            return;
        };
        let words = by.checked_div(WORD_BITS).unwrap_or(0);
        let bits = u32::try_from(by.checked_rem(WORD_BITS).unwrap_or(0)).unwrap_or(0);
        let len = self.seen.len();
        for target in (0..len).rev() {
            let from = target.checked_sub(words);
            let high = from
                .and_then(|from| self.seen.get(from))
                .map_or(0, |word| word.checked_shl(bits).unwrap_or(0));
            let low = from
                .and_then(|from| from.checked_sub(1))
                .and_then(|from| self.seen.get(from))
                .map_or(0, |word| {
                    if bits == 0 {
                        0
                    } else {
                        word.checked_shr(64u32.saturating_sub(bits)).unwrap_or(0)
                    }
                });
            if let Some(slot) = self.seen.get_mut(target) {
                *slot = high | low;
            }
        }
    }

    /// Doubles the width, up to the limit, when a verified datagram arrived deeper than half of it.
    fn widen_for(&mut self, depth: u64) {
        let width = self.width();
        let half = u64::try_from(width / 2).unwrap_or(u64::MAX);
        if depth < half {
            return;
        }
        let wider = width.saturating_mul(2).min(self.limit);
        let words = wider.checked_div(WORD_BITS).unwrap_or(0);
        if words > self.seen.len() {
            self.seen.resize(words, 0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn accepted(window: &mut ReplayWindow, counter: u64) -> Result<(), Refusal> {
        window.check(counter)?;
        window.accept(counter);
        Ok(())
    }

    #[test]
    fn counters_in_order_are_accepted_once() {
        let mut window = ReplayWindow::new(DEFAULT_WIDTH);
        for counter in 0..200 {
            assert_eq!(accepted(&mut window, counter), Ok(()));
            assert_eq!(accepted(&mut window, counter), Err(Refusal::Replay));
        }
    }

    #[test]
    fn a_late_counter_inside_the_window_is_accepted_once_and_one_past_it_is_stale() {
        let mut window = ReplayWindow::new(DEFAULT_WIDTH);
        accepted(&mut window, 100).unwrap();
        assert_eq!(accepted(&mut window, 40), Ok(()), "60 behind, inside 64");
        assert_eq!(accepted(&mut window, 40), Err(Refusal::Replay));
        assert_eq!(window.check(36), Err(Refusal::Stale), "64 behind");
    }

    #[test]
    fn a_jump_past_the_width_forgets_everything_behind_it() {
        let mut window = ReplayWindow::new(DEFAULT_WIDTH);
        for counter in 0..10 {
            accepted(&mut window, counter).unwrap();
        }
        accepted(&mut window, 1_000).unwrap();
        assert_eq!(window.check(999), Ok(()));
        assert_eq!(window.check(9), Err(Refusal::Stale));
    }

    #[test]
    fn a_shift_within_the_width_keeps_what_was_seen() {
        let mut window = ReplayWindow::new(DEFAULT_WIDTH);
        for counter in [10, 12, 15] {
            accepted(&mut window, counter).unwrap();
        }
        accepted(&mut window, 50).unwrap();
        for counter in [10, 12, 15, 50] {
            assert_eq!(window.check(counter), Err(Refusal::Replay), "{counter}");
        }
        for counter in [11, 13, 49] {
            assert_eq!(window.check(counter), Ok(()), "{counter}");
        }
    }

    #[test]
    fn verified_late_datagrams_widen_the_window_up_to_its_limit() {
        let mut window = ReplayWindow::new(256);
        accepted(&mut window, 1_000).unwrap();
        assert_eq!(window.width(), 64);
        accepted(&mut window, 1_000 - 40).unwrap();
        assert_eq!(window.width(), 128, "40 deep in a window of 64");
        accepted(&mut window, 1_000 - 100).unwrap();
        assert_eq!(window.width(), 256);
        accepted(&mut window, 1_000 - 200).unwrap();
        assert_eq!(window.width(), 256, "never past the limit");
        assert_eq!(window.check(1_000 - 256), Err(Refusal::Stale));
        // What was seen survives the widening.
        assert_eq!(window.check(1_000 - 40), Err(Refusal::Replay));
    }

    #[test]
    fn a_wide_window_shifts_across_words() {
        let mut window = ReplayWindow::new(256);
        accepted(&mut window, 500).unwrap();
        accepted(&mut window, 460).unwrap();
        accepted(&mut window, 400).unwrap();
        accepted(&mut window, 300).unwrap();
        assert_eq!(window.width(), 256);
        accepted(&mut window, 570).unwrap();
        for counter in [570, 500, 460, 400] {
            assert_eq!(window.check(counter), Err(Refusal::Replay), "{counter}");
        }
        assert_eq!(window.check(300), Err(Refusal::Stale), "270 deep in 256");
        assert_eq!(window.check(401), Ok(()));
    }
}
