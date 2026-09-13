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
    pub wall_ms: u128,
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
    pub p50: u128,
    pub p95: u128,
    pub p99: u128,
    pub max: u128,
}

/// Percentiles of per-request latency samples (nearest-rank on a sorted copy).
pub fn latency(mut samples: Vec<u128>) -> Latency {
    samples.sort_unstable();
    let pick = |p: f64| -> u128 {
        if samples.is_empty() {
            return 0;
        }
        let last = samples.len() - 1;
        let index = ((last as f64) * p).round() as usize;
        samples[index.min(last)]
    };
    Latency {
        p50: pick(0.50),
        p95: pick(0.95),
        p99: pick(0.99),
        max: samples.last().copied().unwrap_or(0),
    }
}
