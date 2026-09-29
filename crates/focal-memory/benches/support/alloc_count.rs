//! A counting global allocator for bench binaries only. It is never shipped:
//! decision 11 (doc 10) bans `unsafe` in `src/` and `check-contracts.py`
//! scopes that ban there; the `allocs` benches of several crates include this
//! file by `#[path]` so they share one accounting shape (the shape of the
//! adversarial tests' peak-growth allocator, extended with counts, a size
//! histogram and a bounded, sampled call-site table).
//!
//! What is counted, process-wide, while the gate is open: allocations,
//! deallocations, reallocations (a realloc is a growth the caller failed to
//! size), bytes requested, and a request-size histogram. Live and peak live
//! bytes are tracked whether or not the gate is open so a peak stays
//! consistent across gated phases.
//!
//! What the figures are, and are not (the 2026-09-29 audit's F54):
//! `bytes` is what was *requested* of the allocator — allocations plus the
//! new size of every reallocation — never live memory or resident set size.
//! A reallocation copies only when the allocator moved the block: those are
//! counted apart (`realloc_moved`, and `realloc_moved_bytes` = the
//! preserved `min(old, new)` of each moved block — the copy the program
//! observed; what the allocator did inside an in-place growth is not
//! visible here) from the reallocations that stayed in place. A phase's
//! peak and live growth are measured between `Meter::start` and `finish`,
//! so they include what the phase allocated outside its gated operations.
//! The counters' atomics and the sampler's backtraces perturb timing,
//! resident memory and page faults, so a count run attributes and a
//! separate uninstrumented run measures speed and residency; a report of
//! faults states the host's page size (`getconf PAGESIZE` — 16 KiB on
//! Apple silicon, not 4 KiB), and a fault count is events, never bytes.
//!
//! Attribution: every `SAMPLE_EVERY`th counted allocation and every
//! `REALLOC_EVERY`th counted reallocation captures
//! `std::backtrace::Backtrace::force_capture()`, renders it (symbol names, and
//! file:line where the binary carries line tables), drops the allocator's own
//! frames and the standard allocation plumbing, and folds the next `FRAMES`
//! frames into a site key. At most `MAX_SITES` distinct sites are stored;
//! further distinct stacks are counted in `dropped` only. Everything the
//! sampler itself allocates is counted in `overhead` and excluded from every
//! other figure: a thread-local flag marks re-entry, and the site table is
//! only touched under that flag.
//!
//! Counts are stable under machine load; wall-clock is not, so these benches
//! report counts only.
#![allow(
    dead_code,
    unsafe_code,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::cast_precision_loss,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::disallowed_macros,
    clippy::panic
)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::backtrace::Backtrace;
use std::cell::Cell;
use std::fmt::Write as _;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

/// Distinct call sites retained; the rest are counted in `dropped`.
pub const MAX_SITES: usize = 4096;
/// Frames kept per site after the allocator's own frames are removed.
pub const FRAMES: usize = 16;
/// Histogram buckets: request size at most each bound, then everything larger.
pub const BUCKET_BOUNDS: [usize; 6] = [16, 64, 256, 1024, 4096, 65536];
pub const BUCKETS: usize = BUCKET_BOUNDS.len() + 1;

pub struct Counting;

#[global_allocator]
static ALLOC: Counting = Counting;

static GATE: AtomicBool = AtomicBool::new(false);
static ALLOCS: AtomicU64 = AtomicU64::new(0);
static DEALLOCS: AtomicU64 = AtomicU64::new(0);
static REALLOCS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);
static REALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
static REALLOC_MOVED: AtomicU64 = AtomicU64::new(0);
static REALLOC_MOVED_BYTES: AtomicU64 = AtomicU64::new(0);
static REALLOC_IN_PLACE: AtomicU64 = AtomicU64::new(0);
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static HIST: [AtomicU64; BUCKETS] = [const { AtomicU64::new(0) }; BUCKETS];
static OVERHEAD: AtomicU64 = AtomicU64::new(0);
static SAMPLE_EVERY: AtomicU64 = AtomicU64::new(0);
static REALLOC_EVERY: AtomicU64 = AtomicU64::new(0);
static SAMPLE_BUDGET: AtomicU64 = AtomicU64::new(0);
static SAMPLES: AtomicU64 = AtomicU64::new(0);
static DROPPED: AtomicU64 = AtomicU64::new(0);
static SITES: Mutex<Vec<Site>> = Mutex::new(Vec::new());

thread_local! {
    static INSIDE: Cell<bool> = const { Cell::new(false) };
}

struct Site {
    key: u64,
    count: u64,
    reallocs: u64,
    bytes: u64,
    excerpt: String,
}

fn bucket(size: usize) -> usize {
    BUCKET_BOUNDS
        .iter()
        .position(|bound| size <= *bound)
        .unwrap_or(BUCKET_BOUNDS.len())
}

/// True while this thread is inside the sampler or the table (or when
/// thread-local storage is unavailable, e.g. during thread teardown): such
/// allocations are overhead, never counted or tracked.
fn inside() -> bool {
    INSIDE.try_with(Cell::get).unwrap_or(true)
}

fn inside_scope<T>(work: impl FnOnce() -> T) -> T {
    let was = INSIDE.try_with(|flag| flag.replace(true)).unwrap_or(true);
    let out = work();
    let _ = INSIDE.try_with(|flag| flag.set(was));
    out
}

fn track_live_add(size: usize) {
    let live = LIVE.fetch_add(size, Ordering::Relaxed).wrapping_add(size);
    PEAK.fetch_max(live, Ordering::Relaxed);
}

fn track_live_sub(size: usize) {
    let _ = LIVE.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |live| {
        Some(live.saturating_sub(size))
    });
}

fn note_alloc(size: usize) {
    if !GATE.load(Ordering::Relaxed) {
        return;
    }
    let n = ALLOCS.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
    BYTES.fetch_add(size as u64, Ordering::Relaxed);
    HIST[bucket(size)].fetch_add(1, Ordering::Relaxed);
    let every = SAMPLE_EVERY.load(Ordering::Relaxed);
    if every != 0 && n.is_multiple_of(every) {
        sample(size, None);
    }
}

fn note_realloc(old: usize, new: usize, moved: bool) {
    if !GATE.load(Ordering::Relaxed) {
        return;
    }
    let n = REALLOCS.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
    REALLOC_BYTES.fetch_add(new as u64, Ordering::Relaxed);
    if moved {
        // The allocator returned another block: the program's bytes were
        // copied into it, `min(old, new)` of them.
        REALLOC_MOVED.fetch_add(1, Ordering::Relaxed);
        REALLOC_MOVED_BYTES.fetch_add(old.min(new) as u64, Ordering::Relaxed);
    } else {
        REALLOC_IN_PLACE.fetch_add(1, Ordering::Relaxed);
    }
    let every = REALLOC_EVERY.load(Ordering::Relaxed);
    if every != 0 && n.is_multiple_of(every) {
        sample(new, Some(old));
    }
}

fn note_dealloc() {
    if GATE.load(Ordering::Relaxed) {
        DEALLOCS.fetch_add(1, Ordering::Relaxed);
    }
}

/// Frames that belong to the sampler or to the standard allocation plumbing,
/// matched against the symbol name and, when line tables are present, the
/// source path (inlined frames render as bare function names).
fn is_noise(name: &str, at: Option<&str>) -> bool {
    const NAMES: [&str; 15] = [
        "alloc_count",
        "backtrace",
        "__rust_alloc",
        "__rg_alloc",
        "__rdl_alloc",
        "__rust_realloc",
        "__rg_realloc",
        "__rdl_realloc",
        "alloc::alloc::",
        "alloc::raw_vec::",
        "core::alloc::",
        "std::alloc::",
        "GlobalAlloc",
        "inside_scope",
        "{{closure}}",
    ];
    const PATHS: [&str; 6] = [
        "alloc_count.rs",
        "/backtrace/",
        "src/backtrace.rs",
        "library/alloc/src/alloc.rs",
        "library/alloc/src/raw_vec",
        "library/core/src/alloc",
    ];
    NAMES.iter().any(|noise| name.contains(noise))
        || at.is_some_and(|at| PATHS.iter().any(|noise| at.contains(noise)))
}

/// The runtime-entry tail below `main` / a thread's start: constant for every
/// site, so it neither keys nor fills the excerpt.
fn is_tail(name: &str) -> bool {
    const TAIL: [&str; 8] = [
        "__rust_begin_short_backtrace",
        "lang_start",
        "std::rt::",
        "call_once<fn(), ()>",
        "_main",
        "std::sys::backtrace",
        "thread_start",
        "spawn_unchecked",
    ];
    TAIL.iter().any(|tail| name.contains(tail))
}

/// Fold the rendered backtrace into a site: skip the allocator's own frames
/// and the standard allocation plumbing, keep the next `FRAMES` frames, stop
/// at the runtime-entry tail.
fn fold(rendered: &str) -> (u64, String) {
    let mut excerpt = String::new();
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    let mut kept = 0usize;
    let mut skipping = true;
    let mut ended = false;
    let mut current: Option<String> = None;
    let mut pending_at: Option<String> = None;
    let mut flush = |name: &str, at: Option<&str>, kept: &mut usize, hash: &mut u64| {
        if *kept >= FRAMES {
            return;
        }
        *kept += 1;
        for byte in name.bytes().chain(at.unwrap_or("").bytes()) {
            *hash ^= u64::from(byte);
            *hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        let _ = writeln!(excerpt, "      {name}");
        if let Some(at) = at {
            let _ = writeln!(excerpt, "          at {at}");
        }
    };
    let mut settle = |current: &mut Option<String>,
                      pending_at: &mut Option<String>,
                      skipping: &mut bool,
                      ended: &mut bool,
                      kept: &mut usize,
                      hash: &mut u64| {
        if let Some(previous) = current.take() {
            if *skipping && !is_noise(&previous, pending_at.as_deref()) {
                *skipping = false;
            }
            if !*skipping && !*ended {
                if is_tail(&previous) {
                    *ended = true;
                } else {
                    flush(&previous, pending_at.as_deref(), kept, hash);
                }
            }
            *pending_at = None;
        }
    };
    for line in rendered.lines() {
        let trimmed = line.trim_start();
        if let Some(rest) = trimmed.strip_prefix("at ") {
            if current.is_some() {
                pending_at = Some(rest.to_string());
            }
            continue;
        }
        // A frame line: "<index>: <name>".
        let Some((index, name)) = trimmed.split_once(": ") else {
            continue;
        };
        if index.is_empty() || !index.bytes().all(|b| b.is_ascii_digit()) {
            continue;
        }
        settle(
            &mut current,
            &mut pending_at,
            &mut skipping,
            &mut ended,
            &mut kept,
            &mut hash,
        );
        current = Some(name.to_string());
    }
    settle(
        &mut current,
        &mut pending_at,
        &mut skipping,
        &mut ended,
        &mut kept,
        &mut hash,
    );
    (hash, excerpt)
}

fn sample(size: usize, realloc_from: Option<usize>) {
    if SAMPLE_BUDGET
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |left| {
            left.checked_sub(1)
        })
        .is_err()
    {
        DROPPED.fetch_add(1, Ordering::Relaxed);
        return;
    }
    inside_scope(|| {
        // The backtrace lock (inside std) is taken and released here, before
        // the site table's lock: the two are never held together, so two
        // sampling threads cannot wait on each other.
        let rendered = format!("{}", Backtrace::force_capture());
        let (key, excerpt) = fold(&rendered);
        match SITES.lock() {
            Ok(mut sites) => {
                if let Some(site) = sites.iter_mut().find(|site| site.key == key) {
                    site.count += 1;
                    site.bytes += size as u64;
                    if realloc_from.is_some() {
                        site.reallocs += 1;
                    }
                    SAMPLES.fetch_add(1, Ordering::Relaxed);
                } else if sites.len() < MAX_SITES {
                    sites.push(Site {
                        key,
                        count: 1,
                        reallocs: u64::from(realloc_from.is_some()),
                        bytes: size as u64,
                        excerpt,
                    });
                    SAMPLES.fetch_add(1, Ordering::Relaxed);
                } else {
                    DROPPED.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(_) => {
                DROPPED.fetch_add(1, Ordering::Relaxed);
            }
        }
    });
}

// SAFETY (bench allocator): every method delegates to the system allocator
// with the caller's layout; the accounting is atomic adds and a thread-local
// flag, and the sampler never touches the returned block.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            if inside() {
                OVERHEAD.fetch_add(1, Ordering::Relaxed);
            } else {
                track_live_add(layout.size());
                note_alloc(layout.size());
            }
        }
        ptr
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            if inside() {
                OVERHEAD.fetch_add(1, Ordering::Relaxed);
            } else {
                track_live_add(layout.size());
                note_alloc(layout.size());
            }
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if !inside() {
            track_live_sub(layout.size());
            note_dealloc();
        }
        unsafe { System.dealloc(ptr, layout) };
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let next = unsafe { System.realloc(ptr, layout, new_size) };
        if !next.is_null() {
            if inside() {
                OVERHEAD.fetch_add(1, Ordering::Relaxed);
            } else {
                track_live_sub(layout.size());
                track_live_add(new_size);
                note_realloc(layout.size(), new_size, next != ptr);
            }
        }
        next
    }
}

/// Configure sampling: every `sample_every`th allocation and every
/// `realloc_every`th reallocation (0 disables either), at most `budget`
/// samples for the whole process. Environment overrides: `FOCAL_ALLOC_SAMPLE`,
/// `FOCAL_ALLOC_REALLOC_SAMPLE`, `FOCAL_ALLOC_SAMPLE_BUDGET`.
pub fn configure(sample_every: u64, realloc_every: u64, budget: u64) {
    let env = |name: &str, default: u64| {
        std::env::var(name)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(default)
    };
    SAMPLE_EVERY.store(env("FOCAL_ALLOC_SAMPLE", sample_every), Ordering::Relaxed);
    REALLOC_EVERY.store(
        env("FOCAL_ALLOC_REALLOC_SAMPLE", realloc_every),
        Ordering::Relaxed,
    );
    SAMPLE_BUDGET.store(env("FOCAL_ALLOC_SAMPLE_BUDGET", budget), Ordering::Relaxed);
    inside_scope(|| {
        if let Ok(mut sites) = SITES.lock() {
            let _ = sites.try_reserve_exact(MAX_SITES);
        }
    });
}

/// Open or close the counting gate. Live/peak tracking is unconditional.
pub fn gate(open: bool) {
    GATE.store(open, Ordering::SeqCst);
}

/// Reset the peak to the current live figure and return that baseline.
pub fn reset_peak() -> usize {
    let live = LIVE.load(Ordering::Relaxed);
    PEAK.store(live, Ordering::Relaxed);
    live
}

pub fn live() -> usize {
    LIVE.load(Ordering::Relaxed)
}

pub fn peak_growth(baseline: usize) -> usize {
    PEAK.load(Ordering::Relaxed).saturating_sub(baseline)
}

pub fn reset_sites() {
    inside_scope(|| {
        if let Ok(mut sites) = SITES.lock() {
            sites.clear();
        }
    });
    SAMPLES.store(0, Ordering::Relaxed);
    DROPPED.store(0, Ordering::Relaxed);
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Snapshot {
    pub allocs: u64,
    pub deallocs: u64,
    pub reallocs: u64,
    pub bytes: u64,
    pub realloc_bytes: u64,
    /// Reallocations the allocator moved (the program's bytes were copied),
    /// the bytes those copies preserved, and reallocations grown in place.
    pub realloc_moved: u64,
    pub realloc_moved_bytes: u64,
    pub realloc_in_place: u64,
    pub overhead: u64,
    pub hist: [u64; BUCKETS],
}

impl Snapshot {
    pub fn now() -> Self {
        let mut hist = [0u64; BUCKETS];
        for (slot, counter) in hist.iter_mut().zip(HIST.iter()) {
            *slot = counter.load(Ordering::Relaxed);
        }
        Self {
            allocs: ALLOCS.load(Ordering::Relaxed),
            deallocs: DEALLOCS.load(Ordering::Relaxed),
            reallocs: REALLOCS.load(Ordering::Relaxed),
            bytes: BYTES.load(Ordering::Relaxed),
            realloc_bytes: REALLOC_BYTES.load(Ordering::Relaxed),
            realloc_moved: REALLOC_MOVED.load(Ordering::Relaxed),
            realloc_moved_bytes: REALLOC_MOVED_BYTES.load(Ordering::Relaxed),
            realloc_in_place: REALLOC_IN_PLACE.load(Ordering::Relaxed),
            overhead: OVERHEAD.load(Ordering::Relaxed),
            hist,
        }
    }
    /// Counts since `earlier`.
    pub fn since(&self, earlier: &Self) -> Self {
        let mut hist = [0u64; BUCKETS];
        for ((slot, now), then) in hist.iter_mut().zip(self.hist).zip(earlier.hist) {
            *slot = now.saturating_sub(then);
        }
        Self {
            allocs: self.allocs.saturating_sub(earlier.allocs),
            deallocs: self.deallocs.saturating_sub(earlier.deallocs),
            reallocs: self.reallocs.saturating_sub(earlier.reallocs),
            bytes: self.bytes.saturating_sub(earlier.bytes),
            realloc_bytes: self.realloc_bytes.saturating_sub(earlier.realloc_bytes),
            realloc_moved: self.realloc_moved.saturating_sub(earlier.realloc_moved),
            realloc_moved_bytes: self
                .realloc_moved_bytes
                .saturating_sub(earlier.realloc_moved_bytes),
            realloc_in_place: self
                .realloc_in_place
                .saturating_sub(earlier.realloc_in_place),
            overhead: self.overhead.saturating_sub(earlier.overhead),
            hist,
        }
    }
    pub fn add(&self, other: &Self) -> Self {
        let mut hist = [0u64; BUCKETS];
        for ((slot, a), b) in hist.iter_mut().zip(self.hist).zip(other.hist) {
            *slot = a.saturating_add(b);
        }
        Self {
            allocs: self.allocs.saturating_add(other.allocs),
            deallocs: self.deallocs.saturating_add(other.deallocs),
            reallocs: self.reallocs.saturating_add(other.reallocs),
            bytes: self.bytes.saturating_add(other.bytes),
            realloc_bytes: self.realloc_bytes.saturating_add(other.realloc_bytes),
            realloc_moved: self.realloc_moved.saturating_add(other.realloc_moved),
            realloc_moved_bytes: self
                .realloc_moved_bytes
                .saturating_add(other.realloc_moved_bytes),
            realloc_in_place: self.realloc_in_place.saturating_add(other.realloc_in_place),
            overhead: self.overhead.saturating_add(other.overhead),
            hist,
        }
    }
}

/// One measured phase: counts over `ops` operations plus the peak growth.
pub struct Phase {
    pub name: String,
    pub ops: u64,
    pub counts: Snapshot,
    pub peak_growth: usize,
    pub live_growth: i64,
}

/// Accumulates gated windows into one phase. `open` opens the gate and
/// returns the window's start; `close` closes it and adds the window. Fixture
/// work between windows is not counted.
pub struct Meter {
    name: String,
    ops: u64,
    counts: Snapshot,
    baseline: usize,
}

impl Meter {
    pub fn start(name: &str) -> Self {
        Self {
            name: name.to_string(),
            ops: 0,
            counts: Snapshot::default(),
            baseline: reset_peak(),
        }
    }
    pub fn open(&self) -> Snapshot {
        gate(true);
        Snapshot::now()
    }
    pub fn close(&mut self, before: Snapshot) {
        let after = Snapshot::now();
        gate(false);
        self.counts = self.counts.add(&after.since(&before));
        self.ops += 1;
    }
    /// Close a window that covered `ops` operations at once.
    pub fn close_many(&mut self, before: Snapshot, ops: u64) {
        let after = Snapshot::now();
        gate(false);
        self.counts = self.counts.add(&after.since(&before));
        self.ops += ops;
    }
    pub fn finish(self) -> Phase {
        Phase {
            name: self.name,
            ops: self.ops,
            counts: self.counts,
            peak_growth: peak_growth(self.baseline),
            live_growth: live() as i64 - self.baseline as i64,
        }
    }
}

pub fn per_op(total: u64, ops: u64) -> f64 {
    if ops == 0 {
        0.0
    } else {
        total as f64 / ops as f64
    }
}

pub fn header() -> String {
    format!(
        "{:<44} {:>8} {:>10} {:>11} {:>11} {:>12} {:>12} {:>12}  {}",
        "path",
        "ops",
        "allocs/op",
        "reallocs/op",
        "moved/op",
        "requested/op",
        "peak-growth",
        "live-growth",
        "hist ≤16 ≤64 ≤256 ≤1K ≤4K ≤64K >64K (allocs); moved = reallocations the allocator moved and the bytes they copied; requested = bytes asked for, never live"
    )
}

pub fn row(phase: &Phase) -> String {
    let c = &phase.counts;
    format!(
        "{:<44} {:>8} {:>10.2} {:>11.3} {:>11} {:>12.1} {:>12} {:>12}  {} {} {} {} {} {} {}",
        phase.name,
        phase.ops,
        per_op(c.allocs, phase.ops),
        per_op(c.reallocs, phase.ops),
        format!(
            "{:.3}/{:.0}B",
            per_op(c.realloc_moved, phase.ops),
            per_op(c.realloc_moved_bytes, phase.ops)
        ),
        per_op(c.bytes.saturating_add(c.realloc_bytes), phase.ops),
        phase.peak_growth,
        phase.live_growth,
        c.hist[0],
        c.hist[1],
        c.hist[2],
        c.hist[3],
        c.hist[4],
        c.hist[5],
        c.hist[6],
    )
}

/// The frames of a rendered excerpt, innermost first: the symbol name and,
/// when line tables resolved one, the `at file:line:col` location.
fn frames_of(excerpt: &str) -> impl Iterator<Item = (&str, Option<&str>)> {
    let mut lines = excerpt.lines().peekable();
    std::iter::from_fn(move || {
        let name = lines.next()?.trim_start();
        let at = lines
            .next_if(|line| line.trim_start().starts_with("at "))
            .map(|line| line.trim_start());
        Some((name, at))
    })
}

/// Sampled (allocation, reallocation) counts: allocations are sampled every
/// `SAMPLE_EVERY`th and reallocations every `REALLOC_EVERY`th, so the two are
/// kept apart and each share is taken within its own kind.
#[derive(Clone, Copy, Debug, Default)]
pub struct Share {
    pub allocs: u64,
    pub reallocs: u64,
}

/// Attribute every sampled site to the group of the innermost frame that
/// belongs to any of the first `fallback_from` groups (walking outward), so a
/// site is charged to the code that asked for the allocation, not to every
/// caller above it; the remaining groups (the standard library, a runtime)
/// are consulted only for a stack with no such frame. Returns the samples per
/// group, the unclassified samples and the total, since the last `reset_sites`.
pub fn share_by_innermost(groups: &[&[&str]], fallback_from: usize) -> (Vec<Share>, Share, Share) {
    inside_scope(|| {
        let mut counts = vec![Share::default(); groups.len()];
        let Ok(sites) = SITES.lock() else {
            return (counts, Share::default(), Share::default());
        };
        let mut total = Share::default();
        let mut other = Share::default();
        for site in sites.iter() {
            let share = Share {
                allocs: site.count.saturating_sub(site.reallocs),
                reallocs: site.reallocs,
            };
            total.allocs += share.allocs;
            total.reallocs += share.reallocs;
            // A frame is judged by its source path when line tables give one
            // (a symbol name carries its generic parameters, which would
            // charge a `focal-memory` page for the `focal-core` row type it
            // holds), and by its symbol name only when it has no path.
            let judge = |range: std::ops::Range<usize>| {
                frames_of(&site.excerpt).find_map(|(name, at)| {
                    let judged = at.unwrap_or(name);
                    groups
                        .get(range.clone())
                        .unwrap_or(&[])
                        .iter()
                        .position(|needles| needles.iter().any(|needle| judged.contains(needle)))
                        .map(|index| index.saturating_add(range.start))
                })
            };
            // The groups before `fallback_from` are the project's; a frame in
            // one of them wins however deep the standard library or runtime
            // frames above it go. The rest are judged only when no project
            // frame is on the stack at all.
            let group = judge(0..fallback_from.min(groups.len()))
                .or_else(|| judge(fallback_from.min(groups.len())..groups.len()));
            let slot = match group {
                Some(index) => &mut counts[index],
                None => &mut other,
            };
            slot.allocs += share.allocs;
            slot.reallocs += share.reallocs;
        }
        (counts, other, total)
    })
}

/// Print each group's estimated allocations and reallocations per operation:
/// the phase's per-op figures scaled by the group's share of the samples of
/// that kind.
pub fn shares_report(
    phase: &Phase,
    names: &[&str],
    groups: &[&[&str]],
    fallback_from: usize,
) -> String {
    let (counts, other, total) = share_by_innermost(groups, fallback_from);
    let allocs = per_op(phase.counts.allocs, phase.ops);
    let reallocs = per_op(phase.counts.reallocs, phase.ops);
    let scale = |part: u64, whole: u64, per: f64| {
        if whole == 0 {
            0.0
        } else {
            per * part as f64 / whole as f64
        }
    };
    let mut out = format!(
        "  by innermost frame ({} alloc samples, {} realloc samples): allocs/op, reallocs/op\n",
        total.allocs, total.reallocs
    );
    for (name, share) in names
        .iter()
        .zip(counts.iter().chain(std::iter::once(&other)))
    {
        let _ = writeln!(
            out,
            "    {:<22} {:>9.2} {:>9.3}",
            name,
            scale(share.allocs, total.allocs, allocs),
            scale(share.reallocs, total.reallocs, reallocs)
        );
    }
    out
}

/// The top `top` sampled sites since the last `reset_sites`, with their share
/// of the samples, and the sampling totals.
pub fn sites_report(top: usize) -> String {
    inside_scope(|| {
        let mut out = String::new();
        let samples = SAMPLES.load(Ordering::Relaxed);
        let dropped = DROPPED.load(Ordering::Relaxed);
        let Ok(sites) = SITES.lock() else {
            return "  (site table poisoned)\n".to_string();
        };
        let mut ranked: Vec<&Site> = sites.iter().collect();
        ranked.sort_by(|a, b| b.count.cmp(&a.count).then(a.key.cmp(&b.key)));
        let _ = writeln!(
            out,
            "  samples {samples} (every {}th alloc, every {}th realloc; {} dropped: budget/table), {} distinct sites",
            SAMPLE_EVERY.load(Ordering::Relaxed),
            REALLOC_EVERY.load(Ordering::Relaxed),
            dropped,
            sites.len()
        );
        for (rank, site) in ranked.iter().take(top).enumerate() {
            let share = if samples == 0 {
                0.0
            } else {
                100.0 * site.count as f64 / samples as f64
            };
            let _ = writeln!(
                out,
                "  #{:<2} {:>6} samples ({:5.1}%)  {:>5} reallocs  avg {:>8.0} B",
                rank + 1,
                site.count,
                share,
                site.reallocs,
                if site.count == 0 {
                    0.0
                } else {
                    site.bytes as f64 / site.count as f64
                }
            );
            out.push_str(&site.excerpt);
        }
        out
    })
}
