// Dependency-free bench: a plain `harness = false` binary, like focal-consensus's.
// A measurement tool, not a pass/fail test.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::disallowed_macros,
    clippy::cast_precision_loss,
    clippy::arithmetic_side_effects,
    clippy::indexing_slicing
)]
//! What sealing costs a session's checkpoint seeds (29 §12.5): a checkpoint's chunks written as one
//! batch (each written as it is added, then flushed together, renamed and the directory flushed
//! once) by a directory in the clear and by one that seals, in interleaved rounds so the disk's
//! drift falls on both alike; the batch's writing (where sealing's CPU is) timed apart from its
//! commit (where the disk's flushes are), and the chunks read back verified. Then a directory in the
//! clear converted by jobs of `SEED_JOB_CHUNKS` chunks, as the collector drives it. Each figure is
//! the median of the rounds, in milliseconds a MiB.
//!
//! `FOCAL_BENCH_CHUNKS` chunks of 1 MiB a batch (default 64), `FOCAL_BENCH_ROUNDS` rounds (default
//! 8); `FOCAL_BENCH_ROUNDS_SHOWN` prints every round's writing and commit, to see a host's noise;
//! `FOCAL_BENCH_PROFILE=1` (sealed) or `2` (clear) writes batches alone, round after round, for a
//! profiler. Set `TMPDIR` to a directory on the disk to measure (a tmpfs makes every flush free).
use focal_evidence::{
    ContentForm, Conversion, SEED_CHUNK_BYTES, SEED_JOB_CHUNKS, SeedStep, SeedStore,
};
use focal_memory::{DiskBudget, DiskBudgetConfig};
use std::path::Path;
use std::time::{Duration, Instant};

fn open(data: &Path, name: &str, form: ContentForm) -> SeedStore {
    SeedStore::open(
        data.join(name),
        DiskBudget::new(DiskBudgetConfig::unbounded()).unwrap(),
        &focal_seal::test_keys(data).unwrap(),
        form,
    )
    .unwrap()
}

fn payload(seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}

fn env(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(default)
}

fn median(mut samples: Vec<Duration>) -> Duration {
    samples.sort();
    samples[samples.len() / 2]
}

fn per_mib(duration: Duration, chunks: usize) -> f64 {
    duration.as_secs_f64() * 1000.0 / chunks as f64
}

#[derive(Default)]
struct Samples {
    add: Vec<Duration>,
    commit: Vec<Duration>,
    read: Vec<Duration>,
}

/// One checkpoint's batch of `chunks` in `form`, in a directory of its own.
fn batch(data: &Path, round: usize, form: ContentForm, chunks: &[Vec<u8>], samples: &mut Samples) {
    let mut store = open(data, &format!("{form:?}-{round}"), form);
    let started = Instant::now();
    let mut batch = store.batch().unwrap();
    let hashes: Vec<_> = chunks
        .iter()
        .map(|chunk| batch.add(chunk).unwrap())
        .collect();
    let added = Instant::now();
    batch.commit(&mut store).unwrap();
    let committed = Instant::now();
    let reader = store.reader();
    for (hash, chunk) in hashes.iter().zip(chunks) {
        assert_eq!(&reader.read(*hash, SEED_CHUNK_BYTES).unwrap(), chunk);
    }
    let read = Instant::now();
    samples.add.push(added - started);
    samples.commit.push(committed - added);
    samples.read.push(read - committed);
    if std::env::var_os("FOCAL_BENCH_ROUNDS_SHOWN").is_some() {
        eprintln!(
            "round {round} {form:?}: write {:.1} ms, commit {:.1} ms",
            (added - started).as_secs_f64() * 1000.0,
            (committed - added).as_secs_f64() * 1000.0
        );
    }
}

fn main() {
    let count = env("FOCAL_BENCH_CHUNKS", 64);
    let rounds = env("FOCAL_BENCH_ROUNDS", 8);
    let directory = tempfile::tempdir().unwrap();
    let data = directory.path();
    let chunks: Vec<Vec<u8>> = (0..count)
        .map(|n| payload(n as u64 + 1, SEED_CHUNK_BYTES))
        .collect();
    // `FOCAL_BENCH_PROFILE=1` (sealed) or `2` (clear): batches' writing alone, round after round,
    // for a profiler to sample.
    let profile = env("FOCAL_BENCH_PROFILE", 0);
    if profile != 0 {
        let form = if profile == 1 {
            ContentForm::Sealed
        } else {
            ContentForm::Clear
        };
        for round in 0..rounds {
            let mut store = open(data, &format!("profile-{round}"), form);
            let mut batch = store.batch().unwrap();
            for chunk in &chunks {
                batch.add(chunk).unwrap();
            }
        }
        return;
    }
    let mut clear = Samples::default();
    let mut sealed = Samples::default();
    for round in 0..rounds {
        // Interleaved, each form first in turn.
        if round % 2 == 0 {
            batch(data, round, ContentForm::Clear, &chunks, &mut clear);
            batch(data, round, ContentForm::Sealed, &chunks, &mut sealed);
        } else {
            batch(data, round, ContentForm::Sealed, &chunks, &mut sealed);
            batch(data, round, ContentForm::Clear, &chunks, &mut clear);
        }
    }
    println!(
        "seed batch of {count} chunks of 1 MiB, {rounds} interleaved rounds, ms a MiB (median)"
    );
    println!("| phase | clear | sealed | sealed / clear |");
    println!("|---|---|---|---|");
    for (phase, a, b) in [
        ("write", clear.add, sealed.add),
        ("commit", clear.commit, sealed.commit),
        ("read verified", clear.read, sealed.read),
    ] {
        let (a, b) = (median(a), median(b));
        println!(
            "| {phase} | {:.3} | {:.3} | {:.3} |",
            per_mib(a, count),
            per_mib(b, count),
            b.as_secs_f64() / a.as_secs_f64()
        );
    }
    // A directory in the clear converted by jobs, as the collector drives it.
    let mut converted = Vec::new();
    let mut scratch = vec![0u8; SEED_CHUNK_BYTES];
    for round in 0..rounds {
        let name = format!("converted-{round}");
        {
            let mut store = open(data, &name, ContentForm::Clear);
            let mut batch = store.batch().unwrap();
            for chunk in &chunks {
                batch.add(chunk).unwrap();
            }
            batch.commit(&mut store).unwrap();
        }
        let mut store = open(data, &name, ContentForm::Sealed);
        let started = Instant::now();
        loop {
            match store.conversion_job(4096, SEED_JOB_CHUNKS).unwrap() {
                SeedStep::Done => break,
                SeedStep::Visited => {}
                SeedStep::Job(job) => {
                    let done = job.run(&mut scratch);
                    assert_ne!(store.conversion_done(done).unwrap(), Conversion::Held);
                }
            }
        }
        converted.push(started.elapsed());
        assert_eq!(store.conversion_status().converted.files, count as u64);
    }
    println!(
        "conversion by jobs of {SEED_JOB_CHUNKS}: {:.3} ms a MiB (median of {rounds})",
        per_mib(median(converted), count)
    );
}
