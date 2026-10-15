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
//! round trip (`write_frame` + `read_frame`) over an in-memory pipe, the path
//! the local and embedded transports pay per request and per reply.
#[path = "../../focal-memory/benches/support/alloc_count.rs"]
mod alloc_count;

use alloc_count::{Meter, Phase};
use focal_model::{LedgerId, RequestEpoch, RequestId, RouteEpoch, SessionId, TenantId};
use focal_wire::{
    FrameKind, FrameStream, Operation, RequestEnvelope, WireError, decode_payload, encode_payload,
    read_frame, write_frame,
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

/// What was written, read back in order, as the local socket carries a frame.
/// Its buffer is sized before the meter opens, so a row counts the codec and
/// the framing alone.
struct Pipe {
    bytes: Vec<u8>,
    at: usize,
}
impl FrameStream for Pipe {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, WireError> {
        let mut rest = self.bytes.get(self.at..).unwrap_or_default();
        let count = std::io::Read::read(&mut rest, buf).unwrap();
        self.at += count;
        Ok(count)
    }
    async fn write_all(&mut self, buf: &[u8]) -> Result<(), WireError> {
        self.bytes.extend_from_slice(buf);
        Ok(())
    }
}

/// `count` framed round trips, measured inside one running task: the meter
/// starts once the runtime is up, so neither its construction nor a
/// `block_on` per trip is in the row (the pipe never waits, so nothing else
/// runs while a window is open).
async fn round_trips(
    name: String,
    request: RequestEnvelope,
    frame_len: usize,
    count: u64,
) -> Phase {
    let mut meter = Meter::start(&name);
    for _ in 0..count {
        let mut pipe = Pipe {
            bytes: Vec::with_capacity(frame_len + 4096),
            at: 0,
        };
        let before = meter.open();
        write_frame(&mut pipe, FrameKind::Request, &request, LIMIT)
            .await
            .unwrap();
        let decoded: RequestEnvelope = read_frame(&mut pipe, FrameKind::Request, LIMIT)
            .await
            .unwrap();
        std::hint::black_box(&decoded);
        drop(decoded);
        meter.close(before);
        drop(pipe);
    }
    meter.finish()
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
        // request and per reply. The pipe holds the whole frame so the write
        // completes before the read starts.
        alloc_count::reset_sites();
        let name = format!("write_frame+read_frame native/{frame_len}B");
        phases.push(focal_rt::block_on(round_trips(name, request, frame_len, scaled)).unwrap());
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
