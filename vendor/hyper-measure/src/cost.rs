//! What each of many pieces of work cost, every measure of `docs/tails.md` §1a at once, and the
//! tails of each over the pieces: CPU time (user and system) and, where the OS counts them,
//! instructions and cycles ([`crate::usage`]); allocations and the most bytes held at once, on the
//! measuring thread ([`crate::alloc`]); and the process's highest footprint over the whole run.
//!
//! The OS charges the process, not the thread: a piece's CPU time, instructions and cycles are its
//! own only when no other thread of the process is busy meanwhile (a test run with
//! `--test-threads 1`). The allocator's counts are the measuring thread's in any case.

use crate::alloc::{self, Counts};
use crate::usage::{self, Usage};

/// One piece of work's cost.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Cost {
    /// What the OS charged the process meanwhile; `None` where it would not say.
    pub usage: Option<Usage>,
    /// What this thread asked of the allocator meanwhile.
    pub counts: Counts,
}

/// `work`, with what it cost. The allocator's counts need [`alloc::Counting`] installed; without
/// it they are zero.
pub fn measure<T>(work: impl FnOnce() -> T) -> (T, Cost) {
    let before = usage::this().ok();
    alloc::begin();
    let done = work();
    let counts = alloc::end();
    let after = usage::this().ok();
    let usage = after
        .zip(before)
        .map(|(after, before)| after.since(&before));
    (done, Cost { usage, counts })
}

/// The median, the 99th percentile and the most of a set of values, by nearest rank.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Tails {
    /// The median.
    pub p50: u64,
    /// The 99th percentile.
    pub p99: u64,
    /// The most.
    pub max: u64,
}

impl Tails {
    /// The tails of `values`, sorted here; `None` when there are none.
    pub fn of(values: &mut [u64]) -> Option<Self> {
        values.sort_unstable();
        let rank = |per_mille: usize| {
            let at = values
                .len()
                .checked_mul(per_mille)?
                .div_ceil(1_000)
                .max(1)
                .checked_sub(1)?;
            values.get(at).copied()
        };
        Some(Self {
            p50: rank(500)?,
            p99: rank(990)?,
            max: values.last().copied()?,
        })
    }
}

impl std::fmt::Display for Tails {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "p50 {} p99 {} max {}", self.p50, self.p99, self.max)
    }
}

/// Each piece's cost, kept for their tails.
#[derive(Clone, Debug, Default)]
pub struct Costs {
    user_ns: Vec<u64>,
    system_ns: Vec<u64>,
    instructions: Vec<u64>,
    cycles: Vec<u64>,
    allocations: Vec<u64>,
    peak_bytes: Vec<u64>,
    footprint: Option<u64>,
}

impl Costs {
    /// No piece yet.
    pub fn new() -> Self {
        Self::default()
    }
    /// One more piece's cost.
    pub fn add(&mut self, cost: &Cost) {
        if let Some(usage) = cost.usage {
            self.user_ns.push(usage.user_ns);
            self.system_ns.push(usage.system_ns);
            self.instructions.extend(usage.instructions);
            self.cycles.extend(usage.cycles);
            self.footprint = self.footprint.max(usage.peak_footprint);
        }
        self.allocations.push(cost.counts.calls());
        self.peak_bytes
            .push(u64::try_from(cost.counts.peak).unwrap_or(0));
    }
    /// The pieces counted.
    pub fn len(&self) -> usize {
        self.allocations.len()
    }
    /// Whether no piece was counted.
    pub fn is_empty(&self) -> bool {
        self.allocations.is_empty()
    }
    /// The most bytes any one piece held at once, by the allocator's count.
    pub fn peak_bytes(&self) -> u64 {
        self.peak_bytes.iter().copied().max().unwrap_or(0)
    }
    /// Each measure's tails over the pieces, one line each.
    pub fn report(&self) -> String {
        let mut lines = Vec::new();
        let mut line = |name: &str, values: &[u64]| {
            let mut values = values.to_vec();
            match Tails::of(&mut values) {
                Some(tails) => lines.push(format!("{name}: {tails}")),
                None => lines.push(format!("{name}: unmeasured on this OS")),
            }
        };
        line("user ns", &self.user_ns);
        line("system ns", &self.system_ns);
        line("instructions", &self.instructions);
        line("cycles", &self.cycles);
        line("allocations", &self.allocations);
        line("peak bytes held", &self.peak_bytes);
        lines.push(match self.footprint {
            Some(bytes) => format!("process's highest footprint: {bytes} bytes"),
            None => "process's highest footprint: unmeasured on this OS".to_string(),
        });
        lines.join("\n")
    }
}

#[cfg(test)]
mod tests {
    use super::Tails;

    #[test]
    fn tails_are_taken_by_nearest_rank() {
        let mut values: Vec<u64> = (1..=200).rev().collect();
        assert_eq!(
            Tails::of(&mut values),
            Some(Tails {
                p50: 100,
                p99: 198,
                max: 200
            })
        );
        assert_eq!(
            Tails::of(&mut [7]),
            Some(Tails {
                p50: 7,
                p99: 7,
                max: 7
            })
        );
        assert_eq!(Tails::of(&mut []), None);
    }
}
