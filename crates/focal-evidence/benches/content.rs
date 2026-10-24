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
//! What sealing costs the content store (29 §12.4): the same objects uploaded and sealed, read back
//! verified, and copied into a second store as a custody transfer does, by a store in the clear and
//! by one that seals; then a store in the clear converted a file a step, the count of what it must
//! convert (no write) timed apart within it. Each figure is the median of the repetitions, in
//! milliseconds an object (and MiB/s), on this host's disk: every installed file is flushed, so the
//! writes are the disk's and the reads are the CPU's (the files are in the page cache).
//!
//! `FOCAL_BENCH_OBJECTS` objects of each size (default 16), `FOCAL_BENCH_SIZES` sizes in KiB
//! (default "64 1024 8192"), 1 MiB chunks; `FOCAL_BENCH_READS` passes of reads over them (default
//! 1), for a read phase long enough to profile.
use focal_evidence::{ContentForm, ContentStore, Conversion, StoreForm, StoreLimits, UploadId};
use focal_memory::{DiskBudget, DiskBudgetConfig};
use focal_model::{ContentClass, ContentDomainId, ContentRef};
use std::path::Path;
use std::time::{Duration, Instant};

const CHUNK: usize = 1024 * 1024;

fn limits() -> StoreLimits {
    StoreLimits {
        max_content_bytes: 64 * 1024 * 1024,
        max_staging_bytes: 128 * 1024 * 1024,
        max_uploads: 16,
        chunk_bytes: CHUNK,
        max_manifest_bytes: 1024 * 1024,
    }
}

fn open(root: &Path, form: ContentForm) -> ContentStore {
    ContentStore::open_with_disk(
        root,
        limits(),
        DiskBudget::new(DiskBudgetConfig::unbounded()).unwrap(),
        &focal_seal::test_keys(root).unwrap(),
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

fn upload(store: &mut ContentStore, id: u64, domain: ContentDomainId, bytes: &[u8]) -> ContentRef {
    let mut name = [0u8; 16];
    name[..8].copy_from_slice(&id.to_le_bytes());
    let upload = UploadId(name);
    store
        .begin(
            upload,
            domain,
            ContentClass::Evidence,
            bytes.len() as u64,
            None,
        )
        .unwrap();
    for (index, part) in bytes.chunks(CHUNK).enumerate() {
        store.append(upload, (index * CHUNK) as u64, part).unwrap();
    }
    let reference = store.seal(upload).unwrap();
    store.finish(upload).unwrap();
    reference
}

fn median(samples: &mut [Duration]) -> Duration {
    samples.sort();
    samples[samples.len() / 2]
}

fn report(what: &str, form: ContentForm, bytes: usize, samples: &mut [Duration]) {
    let at = median(samples);
    let mib = bytes as f64 / (1024.0 * 1024.0);
    println!(
        "{what:>8} {form:>6} {kib:>6} KiB  {ms:>9.3} ms  {rate:>8.1} MiB/s",
        form = format!("{form:?}"),
        kib = bytes / 1024,
        ms = at.as_secs_f64() * 1000.0,
        rate = mib / at.as_secs_f64(),
    );
}

fn main() {
    let objects: usize = std::env::var("FOCAL_BENCH_OBJECTS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(16);
    let sizes: Vec<usize> = std::env::var("FOCAL_BENCH_SIZES")
        .unwrap_or_else(|_| "64 1024 8192".into())
        .split_whitespace()
        .map(|kib| kib.parse::<usize>().unwrap() * 1024)
        .collect();
    let passes: usize = std::env::var("FOCAL_BENCH_READS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1);
    let domain = ContentDomainId([7; 16]);
    for &size in &sizes {
        for form in [ContentForm::Clear, ContentForm::Sealed] {
            let source = tempfile::tempdir().unwrap();
            let target = tempfile::tempdir().unwrap();
            let mut store = open(source.path(), form);
            let mut receiver = open(target.path(), form);
            let (mut uploads, mut reads, mut imports) = (Vec::new(), Vec::new(), Vec::new());
            let mut references = Vec::new();
            for index in 0..objects {
                let bytes = payload((size * 31 + index) as u64, size);
                let began = Instant::now();
                let reference = upload(&mut store, index as u64, domain, &bytes);
                uploads.push(began.elapsed());
                references.push((reference, bytes));
            }
            eprintln!("reads begin {form:?} {size}");
            for _ in 0..passes {
                for (reference, bytes) in &references {
                    let began = Instant::now();
                    let read = store.read_bytes(reference, size).unwrap();
                    reads.push(began.elapsed());
                    assert_eq!(&read, bytes);
                }
            }
            for (reference, _) in &references {
                let began = Instant::now();
                let described = store.export_manifest(reference).unwrap();
                let imported = receiver
                    .prepare_import(reference.clone(), described.encoded().to_vec())
                    .unwrap();
                for index in 0..described.chunks() {
                    let chunk = store.read_transfer_chunk(&described, index).unwrap();
                    receiver.import_chunk(&imported, index, &chunk).unwrap();
                }
                receiver.complete_import(&imported).unwrap();
                imports.push(began.elapsed());
            }
            report("upload", form, size, &mut uploads);
            report("read", form, size, &mut reads);
            report("copy", form, size, &mut imports);
        }
        // A store in the clear converted a file a step, as its owner converts it while idle.
        let root = tempfile::tempdir().unwrap();
        let mut store = open(root.path(), ContentForm::Clear);
        for index in 0..objects {
            upload(
                &mut store,
                index as u64,
                domain,
                &payload((size * 17 + index) as u64, size),
            );
        }
        drop(store);
        let mut store = open(root.path(), ContentForm::Sealed);
        let mut scratch = vec![0; CHUNK];
        let (mut steps, mut converted) = (0usize, 0usize);
        let began = Instant::now();
        let mut counted = None;
        loop {
            match store.convert_step(&mut scratch, 4096).unwrap() {
                Conversion::Done => break,
                Conversion::Converted => converted += 1,
                Conversion::Visited | Conversion::Held => {}
            }
            steps += 1;
            if counted.is_none() && store.conversion_status().form != StoreForm::Counting {
                counted = Some((began.elapsed(), store.conversion_status().counted.files));
            }
        }
        let took = began.elapsed();
        let total = (objects * size) as f64 / (1024.0 * 1024.0);
        let (count_took, count_files) = counted.unwrap_or_default();
        println!(
            " convert  {objects} objects of {kib} KiB: {converted} files in {steps} steps, {ms:.1} ms, {rate:.1} MiB/s; count {count_files} files in {count_ms:.3} ms",
            kib = size / 1024,
            ms = took.as_secs_f64() * 1000.0,
            rate = total / took.as_secs_f64(),
            count_ms = count_took.as_secs_f64() * 1000.0,
        );
    }
}
