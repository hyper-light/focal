//! The world's randomness: one generator and a named stream per source (`docs/sim.md` §3.1).
//!
//! The generator is SplitMix64 (Steele, Lea and Flood, "Fast Splittable Pseudorandom Number
//! Generators", OOPSLA 2014, doi:10.1145/2714064.2660195), the one all four projects already use,
//! here exactly as focal's `focal-sim` `Seeded` has it, with its unbiased [`Seeded::below`].
//!
//! A stream's seed is the SplitMix64 finalizer folded over the world's seed and the stream's name,
//! so a stream's draws depend on the seed and its name only: drawing more from one stream, or
//! naming a new one, leaves every other stream's sequence unchanged.

/// SplitMix64's increment: the golden ratio scaled to 64 bits, odd (Steele et al. §3; OpenJDK
/// `java.util.SplittableRandom.GOLDEN_GAMMA`; Vigna's `splitmix64.c`).
pub const GOLDEN_GAMMA: u64 = 0x9e37_79b9_7f4a_7c15;

/// SplitMix64's output function, "Stafford variant 13 of 64bit mix function" (OpenJDK
/// `java.util.SplittableRandom.mix64`; the constants and shifts are Stafford's Mix13). A bijection
/// of `u64`, so folding words through it keeps distinct inputs distinct until they collide by
/// chance.
pub const fn mix64(z: u64) -> u64 {
    let z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    let z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// A reproducible pseudorandom source: SplitMix64, as focal's `Seeded`. Never for credentials or
/// identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Seeded {
    state: u64,
}

impl Seeded {
    /// The most redraws [`Self::below`] makes before it takes the remainder of its last draw
    /// (focal's `Seeded::REDRAWS`). Each redraw happens with probability under one half, so the
    /// remainder is taken with probability under 2⁻⁶⁴.
    pub const REDRAWS: u32 = 64;

    /// The generator whose first state is `seed`.
    pub const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// The next 64 bits: the state advanced by [`GOLDEN_GAMMA`] and mixed, with wrapping
    /// arithmetic part of the definition.
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(GOLDEN_GAMMA);
        mix64(self.state)
    }

    /// Uniform in `[0, bound)`, without the bias of a bare remainder: a draw past the last whole
    /// multiple of `bound` is drawn again, at most [`Self::REDRAWS`] times. Zero for a zero bound,
    /// without a draw; a bound of one draws once (focal's `Seeded::below`, unchanged).
    pub fn below(&mut self, bound: u64) -> u64 {
        let Some(excess) = u64::MAX.checked_rem(bound) else {
            return 0;
        };
        // `u64::MAX - excess` is the largest value whose remainder is `bound - 1`, unless all 2^64
        // values divide evenly.
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
}

/// The first state of the stream named `label` and `parts` in a world seeded with `seed`: the
/// seed, the label's length and bytes (as little-endian words, the last zero-padded), the count of
/// parts and each part, each folded in by [`mix64`]. The length and the count make the encoding
/// prefix-free, so two different names are two different word sequences.
pub fn stream_seed(seed: u64, label: &str, parts: &[u64]) -> u64 {
    let fold = |state: u64, word: u64| mix64(state ^ word);
    let mut state = fold(mix64(seed), word_of_len(label.len()));
    for chunk in label.as_bytes().chunks(8) {
        let mut bytes = [0u8; 8];
        for (to, from) in bytes.iter_mut().zip(chunk) {
            *to = *from;
        }
        state = fold(state, u64::from_le_bytes(bytes));
    }
    state = fold(state, word_of_len(parts.len()));
    for part in parts {
        state = fold(state, *part);
    }
    state
}

/// A length as a word. `usize` is at most 64 bits on every target this repository builds for
/// (`CLAUDE.md` §1, Portable), so the conversion is exact there.
fn word_of_len(len: usize) -> u64 {
    u64::try_from(len).unwrap_or(u64::MAX)
}
