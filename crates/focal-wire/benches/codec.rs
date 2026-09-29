// Dependency-free bench: a plain `harness = false` binary, no criterion (the
// workspace's deny.toml forbids unmaintained/unvetted deps). It reports ns/op
// and MiB/s for the request codec so a regression is visible; a measurement
// tool, not a pass/fail test.
#![allow(
    clippy::unwrap_used,
    clippy::disallowed_macros,
    clippy::cast_precision_loss,
    clippy::arithmetic_side_effects
)]
//! Performance bench for the request codec — `encode_payload` /
//! `decode_payload`, the postcard round trip every request and response pays on
//! the wire (P-gate, R11 §5). A native operation carries an opaque frame, so
//! the cost scales with the frame size; the small-frame case is the per-request
//! fixed overhead the "serialize once" work targeted.
use focal_model::{LedgerId, RequestEpoch, RequestId, RouteEpoch, SessionId, TenantId};
use focal_wire::{Operation, RequestEnvelope, decode_payload, encode_payload};
use std::time::Instant;

const LIMIT: u32 = 16 * 1024 * 1024;

fn envelope(frame_len: usize) -> RequestEnvelope {
    RequestEnvelope {
        protocol: 4,
        ledger: LedgerId {
            tenant: TenantId::from_u128(7),
            session: SessionId::from_u128(8),
        },
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(3),
        request_id: RequestId::from_u128(12),
        operation: Operation::Native {
            frame: vec![0xABu8; frame_len],
        },
    }
}

fn bench(name: &str, bytes: usize, iters: u64, mut op: impl FnMut()) {
    for _ in 0..(iters / 10).max(1) {
        op();
    }
    let start = Instant::now();
    for _ in 0..iters {
        op();
    }
    let elapsed = start.elapsed();
    let per = elapsed.as_nanos() as f64 / iters as f64;
    let mib_s = (bytes as f64 * iters as f64) / elapsed.as_secs_f64() / (1024.0 * 1024.0);
    println!("{name:32} {per:9.1} ns/op  {mib_s:9.0} MiB/s  ({iters} iters, {bytes} B)");
}

fn main() {
    let iters: u64 = std::env::var("FOCAL_BENCH_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2_000_000);
    println!("focal-wire request codec bench ({iters} iterations)\n");

    for frame_len in [0usize, 256, 4096, 65536] {
        let request = envelope(frame_len);
        let encoded = encode_payload(&request, LIMIT).unwrap();
        let wire_len = encoded.len();
        // Scale the iteration count down for the large frames so the run stays
        // quick while the per-op figure remains representative.
        let scaled = (iters / (1 + frame_len as u64 / 1024)).max(10_000);

        bench(
            &format!("encode native/{frame_len}B"),
            wire_len,
            scaled,
            || {
                let bytes = encode_payload(&request, LIMIT).unwrap();
                std::hint::black_box(&bytes);
            },
        );
        bench(
            &format!("decode native/{frame_len}B"),
            wire_len,
            scaled,
            || {
                let decoded: RequestEnvelope = decode_payload(&encoded).unwrap();
                std::hint::black_box(&decoded);
            },
        );
    }
}
