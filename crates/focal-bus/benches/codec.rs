// A measurement printed as a table, not a criterion run: `cargo bench -p focal-bus --bench codec`.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::disallowed_macros,
    clippy::arithmetic_side_effects,
    clippy::cast_precision_loss,
    clippy::indexing_slicing
)]
//! Step 0 of the event bus: what the dense JSON a claim, a testament or a progress event carries
//! costs to move, raw and compressed by the edges that send and receive it.
//!
//! For each shape of [`focal_bus::corpus`], a dictionary is trained on one seed's messages and every
//! codec is measured on another seed's, held out (Zstandard's dictionary guide: a dictionary helps
//! data correlated with what it was trained on, and only that). Each row states the bytes a message
//! takes, the compression ratio over the whole held-out set, and the CPU one message costs the
//! sender to compress and the receiver to decompress: the median of the timed passes and their band
//! (least and most), each pass the whole set. Allocations are counted on a pass of their own with
//! hyper-measure's counting allocator, so the count does not perturb the timing. Every message is
//! decompressed and compared with what was sent before a row is printed.
//!
//! Beside the codecs, the receiver's own work on the message: parsing it into a JSON value
//! (`serde_json`), which every respondent does whatever the codec.
//!
//! `FOCAL_BENCH_MESSAGES` (default 4,000) sets the held-out set, `FOCAL_BENCH_ROUNDS` (default 7)
//! the timed passes.

use std::hint::black_box;
use std::time::Instant;

use focal_bus::corpus::{Corpus, Shape};
use hyper_measure::alloc::{self, Counting};
use hyper_measure::stats::Summary;

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// The seed a dictionary is trained on, and the seed its codec is measured on.
const TRAIN_SEED: u64 = 0x5EED_0001;
const TEST_SEED: u64 = 0x5EED_0002;
/// Messages a dictionary is trained on: zstd's guide asks for about a hundred times the
/// dictionary's size in samples; 2,000 claims are 7 MB, a hundred times a 64 KiB dictionary.
const TRAIN_MESSAGES: usize = 2_000;
/// Messages to one frame in the batched rows: one producer's burst to one stream.
const BATCH: usize = 16;

fn env(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn messages(seed: u64, shape: Shape, count: usize) -> Vec<Vec<u8>> {
    let mut corpus = Corpus::new(seed);
    let mut text = String::new();
    (0..count)
        .map(|_| {
            corpus.next(shape, &mut text).unwrap();
            text.as_bytes().to_vec()
        })
        .collect()
}

/// How a row's codec frames its messages.
#[derive(Clone, Copy)]
enum Framing {
    /// One frame a message: a stream's subscribers each take any message alone.
    Each,
    /// One frame a burst of [`BATCH`] messages, each prefixed by its length.
    Batch,
}

struct Codec {
    label: String,
    level: i32,
    dictionary: Option<Vec<u8>>,
    framing: Framing,
}

struct Row {
    label: String,
    bytes_per_message: f64,
    ratio: f64,
    compress: Summary,
    decompress: Summary,
    compress_allocs: f64,
    decompress_allocs: f64,
}

fn compressor(codec: &Codec) -> zstd::bulk::Compressor<'static> {
    let mut compressor = match &codec.dictionary {
        Some(dictionary) => zstd::bulk::Compressor::with_dictionary(codec.level, dictionary),
        None => zstd::bulk::Compressor::new(codec.level),
    }
    .unwrap();
    // The bus carries the dictionary's identity and the payload's checksum in its own envelope
    // (the record's CRC-32C); the frame keeps its content size, which bounds a decompression.
    compressor
        .set_parameter(zstd::zstd_safe::CParameter::DictIdFlag(false))
        .unwrap();
    compressor
        .set_parameter(zstd::zstd_safe::CParameter::ChecksumFlag(false))
        .unwrap();
    compressor
        .set_parameter(zstd::zstd_safe::CParameter::ContentSizeFlag(true))
        .unwrap();
    compressor
}

fn decompressor(codec: &Codec) -> zstd::bulk::Decompressor<'static> {
    match &codec.dictionary {
        Some(dictionary) => zstd::bulk::Decompressor::with_dictionary(dictionary),
        None => zstd::bulk::Decompressor::new(),
    }
    .unwrap()
}

/// The units a codec frames: each message alone, or bursts of [`BATCH`] with length prefixes.
fn units(framing: Framing, messages: &[Vec<u8>]) -> Vec<Vec<u8>> {
    match framing {
        Framing::Each => messages.to_vec(),
        Framing::Batch => messages
            .chunks(BATCH)
            .map(|burst| {
                let mut unit = Vec::new();
                for message in burst {
                    unit.extend_from_slice(&u32::try_from(message.len()).unwrap().to_le_bytes());
                    unit.extend_from_slice(message);
                }
                unit
            })
            .collect(),
    }
}

fn measure(codec: &Codec, test: &[Vec<u8>], rounds: usize) -> Row {
    let units = units(codec.framing, test);
    let largest = units.iter().map(Vec::len).max().unwrap_or(0);
    let mut compressor = compressor(codec);
    let mut decompressor = decompressor(codec);
    let bound = zstd::zstd_safe::compress_bound(largest);
    let mut compressed: Vec<Vec<u8>> = units.iter().map(|_| Vec::with_capacity(bound)).collect();
    let mut restored = Vec::with_capacity(largest);

    // Correctness first: every unit round-trips to its bytes.
    let mut total_compressed = 0_usize;
    for (unit, out) in units.iter().zip(compressed.iter_mut()) {
        out.clear();
        compressor.compress_to_buffer(unit, out).unwrap();
        total_compressed += out.len();
        restored.clear();
        decompressor
            .decompress_to_buffer(out, &mut restored)
            .unwrap();
        assert_eq!(&restored, unit, "{} lost a message", codec.label);
    }
    let messages = test.len() as f64;
    let raw: usize = test.iter().map(Vec::len).sum();

    // The allocator's pass, apart from the timed ones.
    alloc::begin();
    for (unit, out) in units.iter().zip(compressed.iter_mut()) {
        out.clear();
        compressor.compress_to_buffer(unit, out).unwrap();
    }
    let compress_counts = alloc::end();
    alloc::begin();
    for out in &compressed {
        restored.clear();
        decompressor
            .decompress_to_buffer(out, &mut restored)
            .unwrap();
    }
    let decompress_counts = alloc::end();

    let mut compress_ns = Vec::with_capacity(rounds);
    let mut decompress_ns = Vec::with_capacity(rounds);
    for _ in 0..rounds {
        let started = Instant::now();
        for (unit, out) in units.iter().zip(compressed.iter_mut()) {
            out.clear();
            compressor.compress_to_buffer(black_box(unit), out).unwrap();
        }
        compress_ns.push(started.elapsed().as_nanos() as f64 / messages);
        let started = Instant::now();
        for out in &compressed {
            restored.clear();
            decompressor
                .decompress_to_buffer(black_box(out), &mut restored)
                .unwrap();
            black_box(&restored);
        }
        decompress_ns.push(started.elapsed().as_nanos() as f64 / messages);
    }
    Row {
        label: codec.label.clone(),
        bytes_per_message: total_compressed as f64 / messages,
        ratio: raw as f64 / total_compressed as f64,
        compress: Summary::of(&compress_ns).unwrap(),
        decompress: Summary::of(&decompress_ns).unwrap(),
        compress_allocs: compress_counts.calls() as f64 / messages,
        decompress_allocs: decompress_counts.calls() as f64 / messages,
    }
}

/// The receiver's parse of each message into a JSON value, per message.
fn parse(test: &[Vec<u8>], rounds: usize) -> (Summary, f64) {
    alloc::begin();
    for message in test {
        black_box(serde_json::from_slice::<serde_json::Value>(message).unwrap());
    }
    let counts = alloc::end();
    let mut ns = Vec::with_capacity(rounds);
    for _ in 0..rounds {
        let started = Instant::now();
        for message in test {
            black_box(serde_json::from_slice::<serde_json::Value>(black_box(message)).unwrap());
        }
        ns.push(started.elapsed().as_nanos() as f64 / test.len() as f64);
    }
    (
        Summary::of(&ns).unwrap(),
        counts.calls() as f64 / test.len() as f64,
    )
}

fn span(summary: &Summary) -> String {
    format!(
        "{:>8.0} [{:.0}–{:.0}]",
        summary.median, summary.min, summary.max
    )
}

fn main() {
    let held_out = env("FOCAL_BENCH_MESSAGES", 4_000);
    let rounds = env("FOCAL_BENCH_ROUNDS", 7).max(1);
    println!(
        "focal-bus step 0: the edge codec on dense JSON ({held_out} held-out messages a shape, {rounds} timed passes; ns are a message's, median [least–most] of the passes)"
    );
    if let Ok(load) = hyper_measure::usage::load().ok_or(()) {
        println!("host load average at start: {load:.1}");
    }
    for shape in Shape::ALL {
        let train = messages(TRAIN_SEED, shape, TRAIN_MESSAGES);
        let test = messages(TEST_SEED, shape, held_out);
        let raw: usize = test.iter().map(Vec::len).sum();
        let mean = raw as f64 / test.len() as f64;
        println!();
        println!(
            "== {} ({} messages, {:.0} B a message raw; train {} on another seed)",
            shape.label(),
            test.len(),
            mean,
            train.len()
        );
        let (parse_ns, parse_allocs) = parse(&test, rounds);
        println!(
            "   receiver's JSON parse (serde_json::Value): {} ns, {:.1} allocations a message",
            span(&parse_ns),
            parse_allocs
        );
        let mut codecs = Vec::new();
        for level in [1, 3, 9] {
            codecs.push(Codec {
                label: format!("zstd {level}"),
                level,
                dictionary: None,
                framing: Framing::Each,
            });
        }
        for size in [16 * 1024, 64 * 1024, 112 * 1024] {
            let started = Instant::now();
            let dictionary = zstd::dict::from_samples(&train, size).unwrap();
            let trained_ms = started.elapsed().as_secs_f64() * 1e3;
            println!(
                "   dictionary {} KiB asked, {} B trained in {:.0} ms",
                size / 1024,
                dictionary.len(),
                trained_ms
            );
            for level in [1, 3, 9] {
                codecs.push(Codec {
                    label: format!("zstd {level} + dict {}K", size / 1024),
                    level,
                    dictionary: Some(dictionary.clone()),
                    framing: Framing::Each,
                });
            }
            if size == 64 * 1024 {
                codecs.push(Codec {
                    label: format!("zstd 3 + dict 64K, batch {BATCH}"),
                    level: 3,
                    dictionary: Some(dictionary.clone()),
                    framing: Framing::Batch,
                });
            }
        }
        codecs.push(Codec {
            label: format!("zstd 3, batch {BATCH}"),
            level: 3,
            dictionary: None,
            framing: Framing::Batch,
        });
        println!(
            "   {:<28} {:>9} {:>7} {:>26} {:>26} {:>9} {:>9}",
            "codec", "B/msg", "ratio", "compress ns", "decompress ns", "c allocs", "d allocs"
        );
        println!(
            "   {:<28} {:>9.0} {:>7.2} {:>26} {:>26} {:>9} {:>9}",
            "raw", mean, 1.0, "-", "-", "-", "-"
        );
        for codec in &codecs {
            let row = measure(codec, &test, rounds);
            println!(
                "   {:<28} {:>9.0} {:>7.2} {:>26} {:>26} {:>9.3} {:>9.3}",
                row.label,
                row.bytes_per_message,
                row.ratio,
                span(&row.compress),
                span(&row.decompress),
                row.compress_allocs,
                row.decompress_allocs
            );
        }
    }
}
