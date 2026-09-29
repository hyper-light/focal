// A counting global allocator is a test-only harness (decision 11 governs
// shipped code; check-contracts.py scopes the unsafe ban to `src/`).
#![allow(unsafe_code)]
#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
//! Adversarial wire input (R11 §3): a maliciously large or malformed frame is
//! rejected before the decoder allocates a buffer for the attacker's declared
//! length. A counting global allocator in this test binary makes the bound
//! machine-checked: the peak heap growth during a decode of an oversized or
//! truncated frame stays within a small fixed budget, never the multi-megabyte
//! size the header claimed.
use focal_wire::{FrameKind, HEADER_BYTES, WireError, read_frame};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::AsyncWriteExt;

/// Tracks live heap bytes and the peak since the last reset. One test binary,
/// one test function, so the counters are not raced by concurrent allocations.
struct Counting;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY (test allocator): delegates to the system allocator with the
        // same layout; the accounting is ordinary atomic adds.
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            let live = LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(live, Ordering::Relaxed);
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) };
    }
}
#[global_allocator]
static ALLOC: Counting = Counting;

/// Reset the peak to the current live figure and return that baseline.
fn reset_peak() -> usize {
    let live = LIVE.load(Ordering::Relaxed);
    PEAK.store(live, Ordering::Relaxed);
    live
}
/// The peak heap growth over the baseline since the last reset.
fn peak_growth(baseline: usize) -> usize {
    PEAK.load(Ordering::Relaxed).saturating_sub(baseline)
}

fn header(kind: FrameKind, declared_len: u32) -> [u8; HEADER_BYTES] {
    let mut header = [0u8; HEADER_BYTES];
    header[..8].copy_from_slice(b"FOCALQ01");
    header[8..10].copy_from_slice(&1u16.to_be_bytes());
    header[10..12].copy_from_slice(&(kind as u16).to_be_bytes());
    header[12..16].copy_from_slice(&declared_len.to_be_bytes());
    header
}

// A serde value read_frame will try to decode; a small type keeps the test's
// own allocation trivial so the measured growth is the framing layer's.
#[derive(serde::Deserialize, serde::Serialize)]
struct Small {
    value: u64,
}

#[tokio::test]
async fn oversized_and_malformed_frames_reject_before_allocating_the_declared_length() {
    // A small budget: no decode below may grow the heap by more than this.
    // It comfortably exceeds a header and a few small buffers, and is far
    // below any of the megabyte lengths the adversarial headers declare.
    const BUDGET: usize = 256 * 1024;
    let limit = 4096u32;

    // ---- A header declaring the maximum length is refused at the header,
    // before a payload buffer of that size is ever allocated.
    {
        let (mut writer, mut reader) = tokio::io::duplex(64);
        writer
            .write_all(&header(FrameKind::Request, u32::MAX))
            .await
            .unwrap();
        drop(writer);
        let baseline = reset_peak();
        let result: Result<Small, WireError> =
            read_frame(&mut reader, FrameKind::Request, limit).await;
        let growth = peak_growth(baseline);
        assert!(matches!(result, Err(WireError::Limit)), "declared u32::MAX");
        assert!(
            growth < BUDGET,
            "a max-length header allocated {growth} bytes; the buffer must not be sized to the attacker's claim"
        );
    }

    // ---- A header declaring 8 MiB (within MAX_FRAME_BYTES but above the
    // caller's `limit`) is likewise refused at the header.
    {
        let (mut writer, mut reader) = tokio::io::duplex(64);
        writer
            .write_all(&header(FrameKind::Request, 8 * 1024 * 1024))
            .await
            .unwrap();
        drop(writer);
        let baseline = reset_peak();
        let result: Result<Small, WireError> =
            read_frame(&mut reader, FrameKind::Request, limit).await;
        let growth = peak_growth(baseline);
        assert!(
            matches!(result, Err(WireError::Limit)),
            "declared 8 MiB over the limit"
        );
        assert!(
            growth < BUDGET,
            "an over-limit header allocated {growth} bytes"
        );
    }

    // ---- A truncated header (fewer than the fixed header bytes) is refused
    // without allocating a payload.
    {
        let (mut writer, mut reader) = tokio::io::duplex(64);
        writer
            .write_all(&header(FrameKind::Request, 16)[..8])
            .await
            .unwrap();
        drop(writer);
        let baseline = reset_peak();
        let result: Result<Small, WireError> =
            read_frame(&mut reader, FrameKind::Request, limit).await;
        let growth = peak_growth(baseline);
        assert!(result.is_err(), "a truncated header");
        assert!(
            growth < BUDGET,
            "a truncated header allocated {growth} bytes"
        );
    }

    // ---- The wrong magic is refused at the header.
    {
        let (mut writer, mut reader) = tokio::io::duplex(64);
        let mut bad = header(FrameKind::Request, 16);
        bad[0] = b'X';
        writer.write_all(&bad).await.unwrap();
        drop(writer);
        let baseline = reset_peak();
        let result: Result<Small, WireError> =
            read_frame(&mut reader, FrameKind::Request, limit).await;
        let growth = peak_growth(baseline);
        assert!(
            matches!(result, Err(WireError::InvalidFrame)),
            "wrong magic"
        );
        assert!(
            growth < BUDGET,
            "a bad-magic header allocated {growth} bytes"
        );
    }

    // ---- A within-limit declared length whose payload is then truncated
    // allocates only up to the declared bound, never more, and still fails.
    {
        let declared = limit; // 4096, within the caller's limit
        let (mut writer, mut reader) = tokio::io::duplex(1024);
        let mut frame = header(FrameKind::Request, declared).to_vec();
        frame.extend_from_slice(&[0u8; 100]); // far fewer than declared
        let write = tokio::spawn(async move {
            let _ = writer.write_all(&frame).await;
            drop(writer);
        });
        let baseline = reset_peak();
        let result: Result<Small, WireError> =
            read_frame(&mut reader, FrameKind::Request, limit).await;
        let growth = peak_growth(baseline);
        write.await.unwrap();
        assert!(result.is_err(), "a truncated payload");
        // The buffer is the declared length plus bounded overhead, never the
        // 8 MiB / u32::MAX the earlier headers claimed.
        assert!(
            growth < usize::try_from(declared).unwrap() + BUDGET,
            "a truncated {declared}-byte payload allocated {growth} bytes"
        );
    }
}
