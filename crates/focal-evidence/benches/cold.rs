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
//! What sealing costs a session's cold streams (29 §12.6): retired families' batches appended to
//! a data stream and a mark of 16 bytes for each to a marks stream, as a replica appends them where
//! a retirement record applies, by streams in the clear and streams that seal, in interleaved
//! rounds so a host's drift falls on both alike. Each figure is the median of the rounds: the time
//! a MiB of batches takes to append (cuts included, every chunk renamed whole as pending, nothing
//! synced: a checkpoint makes chunks durable), and the bytes the streams' files hold a byte of
//! stream.
//!
//! `FOCAL_BENCH_MIB` MiB of batches a round (default 64), `FOCAL_BENCH_BATCH` bytes a batch on
//! average (default 4096), `FOCAL_BENCH_ROUNDS` rounds (default 8). A batch's size is drawn
//! uniformly from half to one and a half times the average, as a retired family's encoding is no
//! multiple of a page: batches all of one size (`FOCAL_BENCH_FIXED=1`) a page long write the
//! clear stream a page a write, aligned, where a record's overhead puts every sealed write across
//! two, and a file system that pays by the page (btrfs) shows that alignment, not sealing. `FOCAL_BENCH_PROFILE=1` (sealed) or
//! `2` (clear) appends in one form alone, round after round, for a profiler. `FOCAL_BENCH_PARTS=1`
//! times an append's parts alone instead (a seal of each size a record seals, a batch's hash, its
//! copy, and writes of each size either form writes), nanoseconds each. Set `TMPDIR` to a
//! directory on the disk to measure.
use focal_evidence::{ColdGeometry, ColdStore, ContentForm};
use focal_memory::{DiskBudget, DiskBudgetConfig, MemoryBudget};
use std::io::Write;
use std::path::Path;
use std::time::{Duration, Instant};

fn env(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(default)
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

/// The time this thread has run on a CPU, in the kernel included, where the host says
/// (Linux's `/proc/thread-self/schedstat`, nanoseconds): what a round costs without the time a
/// busy host's other work held the CPU from it.
fn cpu_now() -> Option<Duration> {
    std::fs::read_to_string("/proc/thread-self/schedstat")
        .ok()?
        .split_whitespace()
        .next()?
        .parse()
        .ok()
        .map(Duration::from_nanos)
}

fn median(mut samples: Vec<Duration>) -> Duration {
    samples.sort();
    samples[samples.len() / 2]
}

fn bytes_under(root: &Path) -> u64 {
    std::fs::read_dir(root)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.is_file())
        .map(|path| std::fs::metadata(path).unwrap().len())
        .sum()
}

/// What one round took: its time, the CPU it ran on where the host says, and the bytes its
/// streams' files hold.
struct Took {
    wall: Duration,
    cpu: Option<Duration>,
    stored: u64,
}

/// One round in `form`: `batches` appended to a fresh pair of streams.
fn round(data: &Path, name: &str, form: ContentForm, batches: &[Vec<u8>]) -> Took {
    let disk = DiskBudget::new(DiskBudgetConfig::unbounded()).unwrap();
    let memory = MemoryBudget::new(1 << 30, 256 << 20).unwrap();
    let keys = focal_seal::test_keys(data).unwrap();
    let root = data.join(name);
    let mut stream = ColdStore::open(
        root.join("data"),
        disk.clone(),
        &memory,
        ColdGeometry::STANDARD,
        None,
        &keys,
        form,
    )
    .unwrap();
    let mut marks = ColdStore::open(
        root.join("marks"),
        disk,
        &memory,
        ColdGeometry::STANDARD,
        None,
        &keys,
        form,
    )
    .unwrap();
    // `FOCAL_BENCH_SPLIT=1`: each call timed apart, its share of the round printed.
    let split = env("FOCAL_BENCH_SPLIT", 0) != 0;
    let mut parts = [Duration::ZERO; 4];
    let cpu_started = cpu_now();
    let started = Instant::now();
    for batch in batches {
        let offset = stream.length();
        let mut mark = [0u8; 16];
        mark[8..].copy_from_slice(&offset.to_le_bytes());
        if split {
            let at = Instant::now();
            let room = stream.room(batch.len()).unwrap();
            let roomed = Instant::now();
            stream.append(batch, room).unwrap();
            let appended = Instant::now();
            let room = marks.room(mark.len()).unwrap();
            let marks_roomed = Instant::now();
            marks.append(&mark, room).unwrap();
            let done = Instant::now();
            parts[0] += roomed - at;
            parts[1] += appended - roomed;
            parts[2] += marks_roomed - appended;
            parts[3] += done - marks_roomed;
            continue;
        }
        let room = stream.room(batch.len()).unwrap();
        stream.append(batch, room).unwrap();
        let room = marks.room(mark.len()).unwrap();
        marks.append(&mark, room).unwrap();
    }
    let wall = started.elapsed();
    let cpu = cpu_now()
        .zip(cpu_started)
        .map(|(now, started)| now.saturating_sub(started));
    if split {
        let each = |part: Duration| part.as_secs_f64() * 1e9 / batches.len() as f64;
        eprintln!(
            "{form:?}: ns a batch: data room {:.0}, data append {:.0}, marks room {:.0}, marks append {:.0}",
            each(parts[0]),
            each(parts[1]),
            each(parts[2]),
            each(parts[3])
        );
    }
    let stored = bytes_under(&root.join("data")) + bytes_under(&root.join("marks"));
    Took { wall, cpu, stored }
}

/// The median nanoseconds of `call` over `rounds` rounds of `count` calls each.
fn per_call(rounds: usize, count: usize, mut call: impl FnMut(usize)) -> f64 {
    let mut samples = Vec::new();
    for _ in 0..rounds {
        let started = Instant::now();
        for n in 0..count {
            call(n);
        }
        samples.push(started.elapsed());
    }
    median(samples).as_secs_f64() * 1e9 / count as f64
}

/// An append's parts timed alone (`FOCAL_BENCH_PARTS`): what each costs on this host.
fn parts(data: &Path, batch: usize, rounds: usize) {
    let count = 16384;
    // The process's key slots, locked as a node's are.
    let _keys = focal_seal::test_keys(data).unwrap();
    let key = hyper_seal::stream::VersionKey::new(
        &hyper_seal::Secret32::from_bytes(&[7u8; 32]).unwrap(),
        [9u8; 16],
    )
    .unwrap();
    let bytes = payload(1, batch);
    println!("parts of an append of {batch} bytes, {rounds} rounds of {count} (median)");
    println!("| part | ns |");
    println!("|---|---|");
    for size in [4, 16, batch] {
        let mut buffer = payload(2, size);
        let ns = per_call(rounds, count, |n| {
            std::hint::black_box(key.seal(0, n as u32, false, &mut buffer).unwrap());
        });
        println!("| seal {size} bytes | {ns:.0} |");
    }
    let mut hasher = blake3::Hasher::new();
    let ns = per_call(rounds, count, |_| {
        hasher.update(&bytes);
    });
    std::hint::black_box(hasher.finalize());
    println!("| hash {batch} bytes | {ns:.0} |");
    let ns = per_call(rounds, count, |_| {
        let mut copy = Vec::new();
        copy.try_reserve_exact(batch + 36).unwrap();
        copy.extend_from_slice(&bytes);
        std::hint::black_box(&copy);
    });
    println!("| allocate and copy {batch} bytes | {ns:.0} |");
    for size in [16, 16 + 36, batch, batch + 36] {
        let written = payload(3, size);
        let path = data.join(format!("parts-{size}"));
        let open = || {
            std::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&path)
                .unwrap()
        };
        let mut file = open();
        let ns = per_call(rounds, count, |n| {
            if n == 0 {
                // A file a round, as a chunk is: its pages fresh.
                file = open();
            }
            file.write_all(&written).unwrap();
        });
        println!("| write {size} bytes | {ns:.0} |");
        // The same writes from an offset off a page's start, as a record file's header puts them.
        let ns = per_call(rounds, count, |n| {
            if n == 0 {
                file = open();
                file.write_all(&[0u8; 24]).unwrap();
            }
            file.write_all(&written).unwrap();
        });
        println!("| write {size} bytes, 24 past a page | {ns:.0} |");
        drop(file);
        let _ = std::fs::remove_file(&path);
    }
    // A record writer's sequence, a file a chunk as a stream cuts them, against the clear's: what
    // sealing adds in place, its writes' size and its seals each taken apart.
    let chunk = focal_evidence::SEED_CHUNK_BYTES;
    let per_file = (chunk / batch).max(1);
    let path = data.join("parts-sequence");
    let fresh = |header: usize| {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(&path)
            .unwrap();
        file.write_all(&vec![0u8; header]).unwrap();
        file
    };
    for (name, header, seal, extra) in [
        ("clear: write the batch", 0, false, 0),
        ("write the batch and 36 more", 24, false, 36),
        ("seal the batch, write it alone", 0, true, 0),
        ("seal the batch, write it as a record", 24, true, 36),
    ] {
        let mut file = fresh(header);
        let ns = per_call(rounds, count, |n| {
            if n % per_file == 0 {
                file = fresh(header);
            }
            let mut buffer = Vec::new();
            buffer.try_reserve_exact(batch + 36).unwrap();
            if extra != 0 {
                buffer.extend_from_slice(&[0u8; 20]);
            }
            buffer.extend_from_slice(&bytes);
            if seal {
                let (head, payload) = buffer.split_at_mut(if extra != 0 { 20 } else { 0 });
                if extra != 0 {
                    let tag = key.seal(0, 2 * n as u32, false, &mut head[..4]).unwrap();
                    head[4..].copy_from_slice(&tag);
                }
                let tag = key.seal(0, 2 * n as u32 + 1, false, payload).unwrap();
                if extra != 0 {
                    buffer.extend_from_slice(&tag);
                }
            } else if extra != 0 {
                buffer.extend_from_slice(&[0u8; 16]);
            }
            file.write_all(&buffer).unwrap();
        });
        println!("| {name} | {ns:.0} |");
    }
    let _ = std::fs::remove_file(&path);
}

fn main() {
    let mib = env("FOCAL_BENCH_MIB", 64);
    let batch = env("FOCAL_BENCH_BATCH", 4096);
    let rounds = env("FOCAL_BENCH_ROUNDS", 8);
    let directory = tempfile::tempdir().unwrap();
    let data = directory.path();
    if env("FOCAL_BENCH_PARTS", 0) != 0 {
        parts(data, batch, rounds);
        return;
    }
    let fixed = env("FOCAL_BENCH_FIXED", 0) != 0;
    // Batches of `mib` MiB in all: each `batch` bytes, or drawn from half to one and a half times
    // it by a seeded xorshift, so every round and both forms append the same batches.
    let batches: Vec<Vec<u8>> = {
        let mut state = 0x2545_f491_4f6c_dd1du64;
        let mut batches = Vec::new();
        let mut total = 0;
        while total < mib << 20 {
            let size = if fixed {
                batch
            } else {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (batch / 2 + (state % (batch as u64 + 1)) as usize).max(1)
            };
            batches.push(payload(batches.len() as u64 + 1, size));
            total += size;
        }
        batches
    };
    let count = batches.len();
    let stream_bytes = (batches.iter().map(Vec::len).sum::<usize>() + count * 16) as f64;
    // `FOCAL_BENCH_PROFILE=1` (sealed) or `2` (clear): one form's appends alone, round after
    // round, for a profiler to sample.
    let profile = env("FOCAL_BENCH_PROFILE", 0);
    if profile != 0 {
        let form = if profile == 1 {
            ContentForm::Sealed
        } else {
            ContentForm::Clear
        };
        for round_index in 0..rounds {
            let name = format!("profile-{round_index}");
            let took = round(data, &name, form, &batches);
            eprintln!(
                "round {round_index} {form:?}: {:.3} ms a MiB",
                took.wall.as_secs_f64() * 1000.0 / mib as f64
            );
            std::fs::remove_dir_all(data.join(name)).unwrap();
        }
        return;
    }
    // Per form: each round's time and CPU, and what its streams stored.
    let mut clear = (Vec::new(), Vec::new(), 0);
    let mut sealed = (Vec::new(), Vec::new(), 0);
    for round_index in 0..rounds {
        let order = if round_index % 2 == 0 {
            [ContentForm::Clear, ContentForm::Sealed]
        } else {
            [ContentForm::Sealed, ContentForm::Clear]
        };
        for form in order {
            let took = round(data, &format!("{form:?}-{round_index}"), form, &batches);
            let samples = match form {
                ContentForm::Clear => &mut clear,
                ContentForm::Sealed => &mut sealed,
            };
            samples.0.push(took.wall);
            samples.1.extend(took.cpu);
            samples.2 = took.stored;
        }
        std::fs::remove_dir_all(data.join(format!("Clear-{round_index}"))).unwrap();
        std::fs::remove_dir_all(data.join(format!("Sealed-{round_index}"))).unwrap();
    }
    let per_mib = |duration: Duration| duration.as_secs_f64() * 1000.0 / mib as f64;
    let sizes = if fixed {
        format!("{batch} bytes")
    } else {
        format!("{} to {} bytes", batch / 2, batch / 2 + batch)
    };
    println!(
        "cold streams: {mib} MiB in {count} batches of {sizes} and a 16-byte mark each, {rounds} interleaved rounds (median)"
    );
    let measured = !clear.1.is_empty() && clear.1.len() == sealed.1.len();
    println!(
        "| form | CPU ms a MiB appended | ms a MiB appended | bytes stored a byte of stream |"
    );
    println!("|---|---|---|---|");
    let mut figures = Vec::new();
    for (name, (walls, cpus, stored)) in [("clear", clear), ("sealed", sealed)] {
        let wall = median(walls);
        let cpu = measured.then(|| median(cpus));
        println!(
            "| {name} | {} | {:.3} | {:.4} |",
            cpu.map_or("-".to_string(), |cpu| format!("{:.3}", per_mib(cpu))),
            per_mib(wall),
            stored as f64 / stream_bytes
        );
        figures.push((wall, cpu));
    }
    let ratio = |sealed: Duration, clear: Duration| sealed.as_secs_f64() / clear.as_secs_f64();
    if let [(clear_wall, clear_cpu), (sealed_wall, sealed_cpu)] = figures[..] {
        if let (Some(clear_cpu), Some(sealed_cpu)) = (clear_cpu, sealed_cpu) {
            println!("sealed / clear, CPU: {:.3}", ratio(sealed_cpu, clear_cpu));
        }
        println!(
            "sealed / clear, time: {:.3}",
            ratio(sealed_wall, clear_wall)
        );
    }
}
