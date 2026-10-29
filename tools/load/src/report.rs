//! The measured result (`--out` JSON): committed/refused/unknown counts, wall
//! time, end-to-end committed throughput, request latency percentiles for the
//! whole run and for its first and second half (by start time, so growth with
//! session size shows), the read phase, every ledger the run drove with what its
//! lifecycles did and their totals, and — when the shape asked for it — the
//! reopen time of the node and its first read afterwards.
use crate::error::LoadError;
use crate::measurements::{DeliveryMeasurements, LifecycleCounts, WriteMeasurements};
use crate::shape::WorkloadShape;
use focal_model::LedgerId;
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
    /// the protocol working, and in the samples twice. Every principal's
    /// journal's: the issuer's, a consumer's and every agent's.
    pub expired: u64,
    /// Claims whose every lifecycle step committed (the shape's `lifecycle`;
    /// a `create` lifecycle's one step): `totals.written`.
    pub lifecycles: u64,
    /// Lifecycle steps never sent: the read of the claim they bind to failed.
    pub abandoned: u64,
    /// Generation floors this run advanced, as the journal does once the
    /// generation below drained: every principal's journal's.
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
    /// Write attempts after the warm-up, separated by reply kind, with
    /// schedule delay and Client::request time measured independently.
    pub measured_writes: WriteMeasurements,
    /// Approximate Unix nanoseconds at the write phase's monotonic origin,
    /// for correlation with the host's cgroup and checkpoint-stage trace.
    pub write_phase_start_epoch_ns: u128,
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
    /// The refusal reasons seen, at most a few, each with how many writes it
    /// refused (an unknown outcome's reason with how many it left unknown).
    pub refusals: Vec<(String, u64)>,
    /// `reopen: true`: closing the node and reopening its directory until it
    /// serves again, in milliseconds.
    pub reopen_ms: Option<u128>,
    /// `reopen: true`: the first linearizable read after the reopen.
    pub first_read_after_reopen_ns: Option<u128>,
    /// `reopen: true`: whether that read returned the last committed claim.
    pub reopen_read_hit: Option<bool>,
    /// Bytes under the node's data directory when the run ended (embedded).
    pub data_dir_bytes: Option<u64>,
    /// `deliver`: each message end to end, from its place on the schedule.
    pub deliver: Option<DeliveryMeasurements>,
    /// Every ledger the run drove, in the shape's order (one where it names none): what its
    /// lifecycles did over the whole write phase, counted apart.
    pub ledgers: Vec<LedgerReport>,
    /// The ledgers' counts summed.
    pub totals: LifecycleCounts,
}

/// One ledger a run drove: which, the callers and agents that drove it, and what its lifecycles
/// did.
#[derive(Debug, Serialize)]
pub struct LedgerReport {
    pub ledger: LedgerName,
    /// Its callers: its share of the run's.
    pub callers: u16,
    /// Its agents, in a run among agents: its share of the run's, each a principal on it.
    pub agents: u16,
    #[serde(flatten)]
    pub counts: LifecycleCounts,
    /// The events its inbox tails read, and their pages refused or failed.
    pub tail_events: u64,
    pub tail_failures: u64,
    /// Its refusal reasons, counted as the run's `refusals` are.
    pub refusals: Vec<(String, u64)>,
}

/// A ledger as the CLI names it: its tenant and its session, 32 hex digits each.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LedgerName {
    pub tenant: String,
    pub session: String,
}
impl From<LedgerId> for LedgerName {
    fn from(ledger: LedgerId) -> Self {
        Self {
            tenant: ledger.tenant.to_string(),
            session: ledger.session.to_string(),
        }
    }
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
