// Dependency-free allocation-count bench: a plain `harness = false` binary
// with a counting global allocator (focal-memory/benches/support/alloc_count.rs).
// It reports heap allocations, reallocations, bytes and peak growth per
// operation, which are stable under machine load; it reports no wall-clock.
#![allow(
    clippy::unwrap_used,
    clippy::disallowed_macros,
    clippy::cast_precision_loss,
    clippy::arithmetic_side_effects
)]
//! Allocation counts for the request codec (`encode_payload` /
//! `decode_payload`, the same fixture as benches/codec.rs) and for one framed
//! round trip (`write_frame` + `read_frame`) over an in-memory duplex, the
//! path the local and embedded transports pay per request and per reply.
#[path = "../../focal-memory/benches/support/alloc_count.rs"]
mod alloc_count;

use alloc_count::Meter;
use focal_model::{LedgerId, RequestEpoch, RequestId, RouteEpoch, SessionId, TenantId};
use focal_wire::{
    FrameKind, Operation, RequestEnvelope, decode_payload, encode_payload, read_frame, write_frame,
};

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

fn main() {
    alloc_count::configure(97, 1, 50_000);
    let iters: u64 = std::env::var("FOCAL_BENCH_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20_000);
    println!("focal-wire codec allocation counts ({iters} iterations)\n");
    println!("{}", alloc_count::header());
    let mut phases = Vec::new();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    // The runtime's own block_on cost, so the framed round trip below can be
    // read net of it.
    let mut meter = Meter::start("control: block_on(async {})");
    for _ in 0..iters {
        let before = meter.open();
        runtime.block_on(async {});
        meter.close(before);
    }
    phases.push(meter.finish());

    for frame_len in [0usize, 256, 4096, 65536] {
        let request = envelope(frame_len);
        let encoded = encode_payload(&request, LIMIT).unwrap();
        let scaled = (iters / (1 + frame_len as u64 / 1024)).max(1_000);

        alloc_count::reset_sites();
        let mut meter = Meter::start(&format!("encode native/{frame_len}B"));
        for _ in 0..scaled {
            let before = meter.open();
            let bytes = encode_payload(&request, LIMIT).unwrap();
            std::hint::black_box(&bytes);
            drop(bytes);
            meter.close(before);
        }
        phases.push(meter.finish());
        if frame_len == 256 {
            println!("\ntop sites: encode native/256B");
            print!("{}", alloc_count::sites_report(6));
        }

        alloc_count::reset_sites();
        let mut meter = Meter::start(&format!("decode native/{frame_len}B"));
        for _ in 0..scaled {
            let before = meter.open();
            let decoded: RequestEnvelope = decode_payload(&encoded).unwrap();
            std::hint::black_box(&decoded);
            drop(decoded);
            meter.close(before);
        }
        phases.push(meter.finish());
        if frame_len == 256 {
            println!("\ntop sites: decode native/256B");
            print!("{}", alloc_count::sites_report(6));
        }

        // One framed round trip: encode + header + write, then header read +
        // exact payload buffer + decode, as the local transport does per
        // request and per reply. The duplex holds the whole frame so the
        // write completes before the read starts.
        alloc_count::reset_sites();
        let mut meter = Meter::start(&format!("write_frame+read_frame native/{frame_len}B"));
        for _ in 0..scaled {
            let (mut writer, mut reader) = tokio::io::duplex(frame_len + 4096);
            let before = meter.open();
            let decoded: RequestEnvelope = runtime.block_on(async {
                write_frame(&mut writer, FrameKind::Request, &request, LIMIT)
                    .await
                    .unwrap();
                read_frame(&mut reader, FrameKind::Request, LIMIT)
                    .await
                    .unwrap()
            });
            std::hint::black_box(&decoded);
            drop(decoded);
            meter.close(before);
            drop((writer, reader));
        }
        phases.push(meter.finish());
        if frame_len == 256 {
            println!("\ntop sites: write_frame+read_frame native/256B");
            print!("{}", alloc_count::sites_report(8));
        }
    }

    println!();
    println!("{}", alloc_count::header());
    for phase in &phases {
        println!("{}", alloc_count::row(phase));
    }
}
