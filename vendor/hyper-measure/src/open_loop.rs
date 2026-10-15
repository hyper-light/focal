//! An open loop's arrivals (`docs/tails.md` §3.1): requests scheduled ahead at an offered rate,
//! whatever the system does with earlier ones, so a stall cannot slow the offered load and hide
//! from the tail. A closed loop's next request waits for its last answer; an open loop's arrive
//! on their own, and the two give different tails at the same load (Schroeder, Wierman and
//! Harchol-Balter, "Open Versus Closed: A Cautionary Tale", NSDI 2006, §2).
//!
//! The arrivals are a Poisson process: each gap an exponential draw, `−ln(1 − U)` times the mean,
//! `U` uniform on [0, 1) from SplitMix64 (Steele, Lea and Flood, "Fast Splittable Pseudorandom
//! Number Generators", OOPSLA 2014), seeded so a run replays its schedule. A request's latency is
//! counted from its scheduled time ([`Arrivals::next_arrival`]), never from when its driver sent it.

use std::fmt;
use std::time::Duration;

/// A Poisson schedule of request times on the caller's clock, in nanoseconds.
#[derive(Clone, Debug)]
pub struct Arrivals {
    state: u64,
    mean_ns: f64,
    next_ns: f64,
}

/// Why a schedule could not be made or continued.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ArrivalsError {
    /// A rate that is not a positive finite number of requests a second.
    Rate(f64),
    /// A schedule past `u64` nanoseconds of the caller's clock.
    PastTheClock,
}

impl fmt::Display for ArrivalsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rate(rate) => write!(f, "{rate} is not a positive finite rate a second"),
            Self::PastTheClock => f.write_str("a schedule past u64 nanoseconds"),
        }
    }
}

impl std::error::Error for ArrivalsError {}

/// Format: nanoseconds a second.
const NS_PER_SECOND: f64 = 1e9;

impl Arrivals {
    /// Requests at `per_second` on average from `start_ns`, the first one draw after it.
    pub fn poisson(per_second: f64, seed: u64, start_ns: u64) -> Result<Self, ArrivalsError> {
        if !(per_second.is_finite() && per_second > 0.0) {
            return Err(ArrivalsError::Rate(per_second));
        }
        Ok(Self {
            state: seed,
            mean_ns: NS_PER_SECOND / per_second,
            next_ns: start_ns as f64,
        })
    }

    /// The next request's scheduled time.
    pub fn next_arrival(&mut self) -> Result<u64, ArrivalsError> {
        let gap = -(-self.uniform()).ln_1p() * self.mean_ns;
        self.next_ns += gap;
        let at = Duration::try_from_secs_f64(self.next_ns / NS_PER_SECOND)
            .map_err(|_| ArrivalsError::PastTheClock)?;
        u64::try_from(at.as_nanos()).map_err(|_| ArrivalsError::PastTheClock)
    }

    /// A uniform draw on [0, 1): SplitMix64's next output, its top 53 bits as a fraction.
    fn uniform(&mut self) -> f64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^= z >> 31;
        (z >> 11) as f64 / (1u64 << 53) as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// SplitMix64's first outputs for seed 0, as Vigna's reference `splitmix64.c` gives them:
    /// 0xe220a8397b1dcdaf, 0x6e789e6aa1b965f4, 0x06c45d188009454f.
    #[test]
    fn the_draws_are_splitmix64s() {
        let mut a = Arrivals::poisson(1.0, 0, 0).unwrap();
        for expected in [
            0xe220_a839_7b1d_cdafu64,
            0x6e78_9e6a_a1b9_65f4,
            0x06c4_5d18_8009_454f,
        ] {
            assert_eq!(a.uniform(), (expected >> 11) as f64 / (1u64 << 53) as f64);
        }
    }

    /// The schedule rises, replays from its seed, and its time after a hundred thousand arrivals
    /// is its start plus the exponential gaps of its generator's draws at the stated mean,
    /// recomputed from the same generator.
    #[test]
    fn a_schedule_replays_and_its_gaps_are_its_draws() {
        let (mut a, mut b) = (
            Arrivals::poisson(1_000.0, 7, 5).unwrap(),
            Arrivals::poisson(1_000.0, 7, 5).unwrap(),
        );
        let mut draws = Arrivals::poisson(1_000.0, 7, 0).unwrap();
        let mut sum = 5.0f64;
        let mut last = 5u64;
        for _ in 0..100_000 {
            let at = a.next_arrival().unwrap();
            assert_eq!(at, b.next_arrival().unwrap());
            assert!(at >= last);
            last = at;
            sum += -(-draws.uniform()).ln_1p() * 1e6;
        }
        assert_eq!(
            last,
            u64::try_from(Duration::from_secs_f64(sum / 1e9).as_nanos()).unwrap()
        );
    }

    #[test]
    fn a_rate_that_is_not_positive_and_finite_is_refused() {
        for rate in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            assert!(Arrivals::poisson(rate, 0, 0).is_err());
        }
    }
}
