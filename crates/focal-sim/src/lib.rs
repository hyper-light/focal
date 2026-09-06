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
}
