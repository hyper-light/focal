//! A bounded write trace and separate statistics for each reply kind.
//! `sent_ns` is the Client::request boundary, so its interval to completion
//! includes client routing/retries as well as the remote request.
use crate::{
    error::LoadError,
    report::{self, Latency},
};
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use focal_model::RequestEpoch;
use serde::Serialize;
use std::{
    io::{BufWriter, Write},
    path::Path,
};

pub const WRITES_CSV_ENV: &str = "FOCAL_LOAD_WRITES_CSV";
const ALLOCATION_OVERHEAD: usize = const { 4 * size_of::<usize>() };
const CSV_BUFFER_BYTES: usize = 4096;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum WriteOutcome {
    Committed,
    Refused,
    Unknown,
    Expired,
}
impl WriteOutcome {
    fn name(self) -> &'static str {
        match self {
            Self::Committed => "committed",
            Self::Refused => "refused",
            Self::Unknown => "unknown",
            Self::Expired => "expired",
        }
    }
}

#[derive(Clone, Copy)]
pub struct WriteSample {
    pub start_ns: u128,
    pub sent_ns: u128,
    pub finished_ns: u128,
    pub worker: u16,
    pub write: u64,
    pub attempt: u128,
    pub epoch: RequestEpoch,
    pub request: u128,
    pub outcome: WriteOutcome,
}
impl WriteSample {
    pub fn latency_ns(&self) -> u128 {
        self.finished_ns.saturating_sub(self.start_ns)
    }
}

/// Every payload drops before its permit. All vectors have an explicit
/// maximum and reserve their complete capacity before the run or reduction.
struct FundedVec<T> {
    values: Vec<T>,
    _allocation: Option<Allocation>,
    limit: usize,
}
impl<T> Default for FundedVec<T> {
    fn default() -> Self {
        Self {
            values: Vec::new(),
            _allocation: None,
            limit: 0,
        }
    }
}
impl<T> FundedVec<T> {
    fn reserve(limit: usize) -> Result<Self, LoadError> {
        if limit == 0 {
            return Ok(Self::default());
        }
        let bytes = limit
            .checked_mul(size_of::<T>())
            .and_then(|bytes| bytes.checked_add(ALLOCATION_OVERHEAD))
            .ok_or(LoadError::Bound("write measurement storage"))?;
        let budget = MemoryBudget::new(bytes, 0)?;
        let allocation = budget
            .reserve(BudgetKind::Pending, BudgetLane::Ordinary, bytes)?
            .commit();
        let mut values = Vec::new();
        values
            .try_reserve_exact(limit)
            .map_err(|_| LoadError::Bound("write measurement allocation"))?;
        if values.capacity() > limit {
            return Err(LoadError::Bound("write measurement allocation capacity"));
        }
        Ok(Self {
            values,
            _allocation: Some(allocation),
            limit,
        })
    }
    fn push(&mut self, value: T) -> Result<(), LoadError> {
        if self.values.len() >= self.limit {
            return Err(LoadError::Bound("write measurement samples"));
        }
        self.values.push(value);
        Ok(())
    }
}

#[derive(Default)]
pub struct WriteSamples(FundedVec<WriteSample>);
impl WriteSamples {
    pub fn reserve(limit: usize) -> Result<Self, LoadError> {
        FundedVec::reserve(limit).map(Self)
    }
    pub fn record(&mut self, sample: WriteSample) -> Result<(), LoadError> {
        self.0.push(sample)
    }
    pub fn iter(&self) -> std::slice::Iter<'_, WriteSample> {
        self.0.values.iter()
    }
}

/// Attempts whose intended starts fall after the warm-up. Counts and
/// percentiles cover the same population; original whole-phase counts stay
/// on Report for compatibility. An expired request remains an attempt of
/// its own, separate from a later successful re-issue.
#[derive(Debug, Serialize)]
pub struct WriteMeasurements {
    pub committed: u64,
    pub refused: u64,
    pub unknown: u64,
    pub expired: u64,
    pub committed_latency_ns: Latency,
    pub refused_latency_ns: Latency,
    pub unknown_latency_ns: Latency,
    pub expired_latency_ns: Latency,
    pub schedule_delay_ns: Latency,
    pub client_request_latency_ns: Latency,
}

fn count(samples: impl Iterator) -> Result<u64, LoadError> {
    u64::try_from(samples.count()).map_err(|_| LoadError::Bound("write measurement count"))
}

fn latency(values: impl Iterator<Item = u128> + Clone) -> Result<Latency, LoadError> {
    let mut scratch = FundedVec::reserve(values.clone().count())?;
    for value in values {
        scratch.push(value)?;
    }
    report::latency(&mut scratch.values)
}

impl WriteMeasurements {
    pub fn measure<'a>(
        samples: impl Iterator<Item = &'a WriteSample> + Clone,
        warmup_ns: u128,
    ) -> Result<Self, LoadError> {
        let samples = samples.filter(|sample| sample.start_ns >= warmup_ns);
        let committed = samples
            .clone()
            .filter(|sample| sample.outcome == WriteOutcome::Committed);
        let refused = samples
            .clone()
            .filter(|sample| sample.outcome == WriteOutcome::Refused);
        let unknown = samples
            .clone()
            .filter(|sample| sample.outcome == WriteOutcome::Unknown);
        let expired = samples
            .clone()
            .filter(|sample| sample.outcome == WriteOutcome::Expired);
        Ok(Self {
            committed: count(committed.clone())?,
            refused: count(refused.clone())?,
            unknown: count(unknown.clone())?,
            expired: count(expired.clone())?,
            committed_latency_ns: latency(committed.map(WriteSample::latency_ns))?,
            refused_latency_ns: latency(refused.map(WriteSample::latency_ns))?,
            unknown_latency_ns: latency(unknown.map(WriteSample::latency_ns))?,
            expired_latency_ns: latency(expired.map(WriteSample::latency_ns))?,
            schedule_delay_ns: latency(
                samples
                    .clone()
                    .map(|sample| sample.sent_ns.saturating_sub(sample.start_ns)),
            )?,
            client_request_latency_ns: latency(
                samples.map(|sample| sample.finished_ns.saturating_sub(sample.sent_ns)),
            )?,
        })
    }
}

pub fn write_samples<'a>(
    samples: impl Iterator<Item = &'a WriteSample> + Clone,
    warmup_ns: u128,
    run_start_epoch_ns: u128,
) -> Result<(), LoadError> {
    let Some(path) = std::env::var_os(WRITES_CSV_ENV) else {
        return Ok(());
    };
    write_csv(Path::new(&path), samples, warmup_ns, run_start_epoch_ns)
}

fn write_csv<'a>(
    path: &Path,
    samples: impl Iterator<Item = &'a WriteSample> + Clone,
    warmup_ns: u128,
    run_start_epoch_ns: u128,
) -> Result<(), LoadError> {
    let samples = samples.filter(|sample| sample.start_ns >= warmup_ns);
    let mut ordered = FundedVec::reserve(samples.clone().count())?;
    for sample in samples {
        ordered.push(sample)?;
    }
    ordered
        .values
        .sort_unstable_by_key(|sample| sample.start_ns);
    let origin = ordered.values.first().map_or(0, |sample| sample.start_ns);
    let bytes = CSV_BUFFER_BYTES
        .checked_add(ALLOCATION_OVERHEAD)
        .ok_or(LoadError::Bound("write CSV buffer"))?;
    let budget = MemoryBudget::new(bytes, 0)?;
    let _allocation = budget
        .reserve(BudgetKind::Pending, BudgetLane::Ordinary, bytes)?
        .commit();
    let mut output = BufWriter::with_capacity(CSV_BUFFER_BYTES, std::fs::File::create(path)?);
    if output.capacity() > CSV_BUFFER_BYTES {
        return Err(LoadError::Bound("write CSV buffer capacity"));
    }
    writeln!(
        output,
        "start_ns,latency_ns,sent_ns,finished_ns,worker,write,attempt,epoch,request,outcome,phase_start_ns,run_start_epoch_ns"
    )?;
    for sample in &ordered.values {
        writeln!(
            output,
            "{},{},{},{},{},{},{},{},{:032x},{},{},{}",
            sample.start_ns.saturating_sub(origin),
            sample.latency_ns(),
            sample.sent_ns.saturating_sub(origin),
            sample.finished_ns.saturating_sub(origin),
            sample.worker,
            sample.write,
            sample.attempt,
            sample.epoch.0,
            sample.request,
            sample.outcome.name(),
            sample.start_ns,
            run_start_epoch_ns
        )?;
    }
    output.flush()?;
    Ok(())
}

#[cfg(test)]
#[path = "measurements_tests.rs"]
mod tests;
