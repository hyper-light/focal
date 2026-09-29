//! The measured result (`--out` JSON): committed/refused/unknown counts, wall
//! time, end-to-end committed throughput, and request latency percentiles.
use crate::shape::WorkloadShape;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct Report {
    pub shape: WorkloadShape,
    pub committed: u64,
    pub refused: u64,
    pub unknown: u64,
    pub wall_ms: u64,
    pub throughput_ops_per_s: f64,
    pub latency_ns: Latency,
    /// Claim reads issued after the creations (0 when the shape asks for none).
    pub reads: u64,
    /// Reads that returned the claim (a miss would indicate lost committed state).
    pub read_hits: u64,
    pub read_throughput_ops_per_s: f64,
    pub read_latency_ns: Latency,
}

#[derive(Debug, Serialize)]
pub struct Latency {
    pub p50: u64,
    pub p95: u64,
    pub p99: u64,
    pub max: u64,
}

/// Percentiles of per-request latency samples in nanoseconds: nearest rank
/// on a sorted copy, the rank rounded half up.
pub fn latency(mut samples: Vec<u64>) -> Latency {
    samples.sort_unstable();
    let pick = |percent: u64| -> u64 {
        let Some(last) = samples.len().checked_sub(1) else {
            return 0;
        };
        let rank = u64::try_from(last)
            .ok()
            .and_then(|last| last.checked_mul(percent))
            .and_then(|scaled| scaled.checked_add(50))
            .map(|scaled| scaled.checked_div(100).unwrap_or(0))
            .and_then(|rank| usize::try_from(rank).ok())
            .unwrap_or(last);
        samples.get(rank.min(last)).copied().unwrap_or(0)
    };
    Latency {
        p50: pick(50),
        p95: pick(95),
        p99: pick(99),
        max: samples.last().copied().unwrap_or(0),
    }
}

/// Operations per second from a count and the nanoseconds they took, to a
/// thousandth: integer arithmetic, then the one conversion that is exact.
pub fn per_second(count: u64, nanos: u64) -> f64 {
    if nanos == 0 {
        return 0.0;
    }
    let milli_ops = u128::from(count)
        .checked_mul(1_000_000_000_000)
        .map(|scaled| scaled.checked_div(u128::from(nanos)).unwrap_or(0))
        .unwrap_or(u128::MAX);
    let milli_ops = u32::try_from(milli_ops).unwrap_or(u32::MAX);
    f64::from(milli_ops) / 1000.0
}
