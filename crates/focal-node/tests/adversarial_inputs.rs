// A counting global allocator is a test-only harness (decision 11 governs
// shipped code; check-contracts.py scopes the unsafe ban to `src/`).
#![cfg(unix)]
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
//! Adversarial node inputs (R11 §3): the node's own on-disk decoders — the
//! `POLICY` file (`CommittedPolicy::decode`) and the `IDENTITY` file
//! (`decode_identity`) — reject a malformed or maliciously length-inflated file
//! without allocating a buffer for an attacker's declared length. A counting
//! global allocator in this test binary makes the bound machine-checked: peak
//! heap growth during a decode stays within a small fixed budget, never the
//! gigabyte-scale size a leading varint could claim. The wire frame decoder has
//! the same guard in `focal-wire/tests/adversarial.rs`; this covers the two
//! decoders that read attacker-influenced bytes off the node's own disk.
//!
//! One `#[test]` function only: the counters are process-global, so a second
//! concurrent test in the same binary would race them.
use focal_node::config::CommittedPolicy;
use focal_node::embedded::decode_identity;
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

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
        // SAFETY: `ptr`/`layout` are the pair this allocator returned.
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

/// A decode of any node file is bounded work far below this. The POLICY cap is
/// 64 KiB and the IDENTITY cap 4 KiB, so 4 MiB is orders of magnitude below any
/// length-prefix amplification bug yet far above honest decode overhead.
const BOUND: usize = 4 * 1024 * 1024;

/// The `POLICY` file's frozen magic, matched verbatim by `CommittedPolicy::decode`;
/// kept here so the test frames inputs without exposing the internal constant.
const POLICY_MAGIC: &[u8; 8] = b"FCLPOL2\0";

/// A tiny deterministic xorshift: a reproducible corpus without an rng dependency.
struct Rng(u64);
impl Rng {
    fn next_byte(&mut self) -> u8 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        (x >> 24) as u8
    }
    fn bytes(&mut self, len: usize) -> Vec<u8> {
        (0..len).map(|_| self.next_byte()).collect()
    }
}

/// Every adversarial body class: empty, all-ones (a maximal varint wherever the
/// decoder reads a length), an unterminated varint, truncations, and
/// pseudo-random runs across and past each decoder's size cap.
fn corpus() -> Vec<Vec<u8>> {
    let mut cases: Vec<Vec<u8>> = vec![
        Vec::new(),
        vec![0x00],
        vec![0xFF],
        vec![0xFF; 16],
        vec![0xFF; 1024],
        vec![0x80; 1024],
    ];
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    for len in [
        0usize, 1, 7, 8, 9, 40, 41, 100, 1000, 4095, 4096, 4097, 65535, 65536, 65537,
    ] {
        cases.push(rng.bytes(len));
        // A maximal-varint prefix (huge claimed length) then a random tail.
        let mut inflated = vec![0xFFu8; 10.min(len)];
        inflated.extend(rng.bytes(len.saturating_sub(10)));
        cases.push(inflated);
    }
    cases
}

/// One decode must never panic and never allocate past `BOUND`, whatever the
/// input; `growth` is captured before the assertion's own formatting allocates.
fn assert_bounded(what: &str, len: usize, baseline: usize) {
    let growth = peak_growth(baseline);
    assert!(
        growth <= BOUND,
        "{what} decode of {len} bytes allocated {growth} bytes (bound {BOUND})"
    );
}

#[test]
fn node_on_disk_decoders_are_bounded_and_never_panic_on_any_input() {
    let dir = tempfile::tempdir().unwrap();
    let identity_path = dir.path().join("IDENTITY");
    for body in corpus() {
        // POLICY: both framings the decoder accepts — the `FCLPOL2\0` magic
        // prefix and the legacy magic-less pair — each reaches postcard with
        // attacker bytes. A malformed file is refused, a well-formed one
        // accepted; either way no buffer is sized to a claimed length.
        let mut magic_framed = POLICY_MAGIC.to_vec();
        magic_framed.extend_from_slice(&body);
        for framed in [&magic_framed, &body] {
            let baseline = reset_peak();
            let _ = CommittedPolicy::decode(framed);
            assert_bounded("policy", framed.len(), baseline);
        }

        // IDENTITY: the file is read (size-capped), its magic and content hash
        // checked, then the fixed-size record decoded. No corpus input is a
        // valid identity; each is refused, none over-allocates.
        std::fs::write(&identity_path, &body).unwrap();
        let baseline = reset_peak();
        let result = decode_identity(&identity_path);
        assert_bounded("identity", body.len(), baseline);
        assert!(
            result.is_err(),
            "an adversarial {}-byte identity file was accepted",
            body.len()
        );
    }
}
