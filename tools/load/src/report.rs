//! The measured result (`--out` JSON): committed/refused/unknown counts, wall
//! time, end-to-end committed throughput, request latency percentiles for the
//! whole run and for its first and second half (by start time, so growth with
//! session size shows), the read phase, and — when the shape asked for it —
//! the reopen time of the node and its first read afterwards.
use crate::error::LoadError;
use crate::shape::WorkloadShape;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct Report {
    pub shape: WorkloadShape,
    pub committed: u64,
    pub refused: u64,
    pub unknown: u64,
    /// Writes the owner refused by name — their generation closed under
    /// pressure, or not admitted yet (the audit's F12) — and re-issued in
    /// the generation the owner admits; counted apart from `refused`, as
    /// the protocol working, and in the samples twice.
    pub expired: u64,
    /// Generation floors this run advanced, as the journal does once the
    /// generation below drained.
    pub floors_advanced: u64,
    /// Wall time of the write phase, every worker included.
    pub wall_ms: u128,
    pub throughput_ops_per_s: f64,
    pub latency_ns: Latency,
    /// The writes that started in the first half of the write phase.
    pub latency_ns_first_half: Latency,
    /// The writes that started in the second half: dearer than the first when
    /// the per-op cost grows with the session.
    pub latency_ns_second_half: Latency,
    /// Claim reads issued after the creations (0 when the shape asks for none).
    pub reads: u64,
    /// Reads that returned the claim (a miss would indicate lost committed state).
    pub read_hits: u64,
    /// Wall time of the read phase, every worker included.
    pub read_wall_ms: u128,
    pub read_throughput_ops_per_s: f64,
    pub read_latency_ns: Latency,
    /// Concurrent callers the run used.
    pub workers: u16,
    /// Distinct refusal reasons seen, at most a few, for diagnosis.
    pub refusals: Vec<String>,
    /// `reopen: true`: closing the node and reopening its directory until it
    /// serves again, in milliseconds.
    pub reopen_ms: Option<u128>,
    /// `reopen: true`: the first linearizable read after the reopen.
    pub first_read_after_reopen_ns: Option<u128>,
    /// `reopen: true`: whether that read returned the last committed claim.
    pub reopen_read_hit: Option<bool>,
    /// Bytes under the node's data directory when the run ended (embedded).
    pub data_dir_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct Latency {
    pub p50: u128,
    pub p95: u128,
    pub p99: u128,
    pub p999: u128,
    pub max: u128,
}

/// Percentiles of per-request latency samples: nearest rank on the sorted
/// samples (sorted in place), the rank rounded to nearest.
pub fn latency(samples: &mut [u128]) -> Result<Latency, LoadError> {
    if samples.is_empty() {
        return Ok(Latency::default());
    }
    samples.sort_unstable();
    let last = samples
        .len()
        .checked_sub(1)
        .ok_or(LoadError::Bound("percentile of no samples"))?;
    // Ranks in per-mille, so p99.9 is as exact as the samples.
    let pick = |per_mille: usize| -> Result<u128, LoadError> {
        let index = last
            .checked_mul(per_mille)
            .and_then(|scaled| scaled.checked_add(500))
            .and_then(|scaled| scaled.checked_div(1000))
            .ok_or(LoadError::Bound("percentile rank"))?;
        samples
            .get(index.min(last))
            .copied()
            .ok_or(LoadError::Bound("percentile rank"))
    };
    Ok(Latency {
        p50: pick(500)?,
        p95: pick(950)?,
        p99: pick(990)?,
        p999: pick(999)?,
        max: samples.last().copied().unwrap_or(0),
    })
}

/// Events per second over `nanos` of wall time; zero when no time passed.
pub fn per_second(count: u64, nanos: u128) -> f64 {
    if nanos == 0 {
        return 0.0;
    }
    // Statistics only: precision loss past 2^53 is immaterial to a rate.
    count as f64 / (nanos as f64 / 1e9)
}
