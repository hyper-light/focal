#![cfg_attr(
    test,
    allow(
        clippy::panic,
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::unreachable,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::disallowed_macros
    )
)]
//! Deterministic, bounded fault models shared by ledger qualification tests.
//! These models are test infrastructure, never production durability adapters.
pub mod disk;
pub mod history;
pub mod network;
pub mod path;

/// Reproducible pseudorandom source. Never use this for credentials or public identity.
#[derive(Debug, Clone)]
pub struct Seeded {
    state: u64,
}

impl Seeded {
    pub const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// SplitMix64, with wrapping arithmetic explicitly part of its definition.
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Uniform in `[0, bound)`, without the bias of a bare remainder: a
    /// draw past the last whole multiple of `bound` is drawn again, at most
    /// [`Self::REDRAWS`] times. Each redraw happens with probability under
    /// one half, so the remainder taken after the last one is reached with
    /// probability under 2⁻⁶⁴. Zero for a zero bound.
    pub fn below(&mut self, bound: u64) -> u64 {
        let Some(excess) = u64::MAX.checked_rem(bound) else {
            return 0;
        };
        // `u64::MAX - excess` is the largest value whose remainder is
        // `bound - 1`, unless all 2^64 values divide evenly.
        let last = if excess.checked_add(1) == Some(bound) {
            u64::MAX
        } else {
            u64::MAX.saturating_sub(excess).saturating_sub(1)
        };
        let mut draw = self.next_u64();
        for _ in 0..Self::REDRAWS {
            if draw <= last {
                break;
            }
            draw = self.next_u64();
        }
        draw.checked_rem(bound).unwrap_or(0)
    }
    pub const REDRAWS: u32 = 64;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seed_is_a_reproducible_fixture_not_ambient_entropy() {
        let mut random = Seeded::new(0);
        assert_eq!(random.next_u64(), 0xe220_a839_7b1d_cdaf);
        let mut copy = random.clone();
        assert_eq!(random.next_u64(), copy.next_u64());
    }

    #[test]
    fn a_bounded_draw_is_inside_its_bound_and_uniform() {
        let mut random = Seeded::new(3);
        assert_eq!(random.below(0), 0);
        assert_eq!(random.below(1), 0);
        let mut counts = [0_u32; 7];
        for _ in 0..70_000 {
            counts[random.below(7) as usize] += 1;
        }
        // 10,000 each with a standard deviation of 93.
        assert!(counts.iter().all(|count| (9_500..=10_500).contains(count)));
        assert!(random.below(u64::MAX) < u64::MAX);
        let _ = random.below(1 << 63);
    }
}
