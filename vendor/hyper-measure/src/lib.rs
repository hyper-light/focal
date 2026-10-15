//! What a run costs, counted where it happens: every allocation, reallocation
//! and free the process makes ([`alloc`]), and the page faults the operating
//! system charged it ([`faults`]). The benchmarks and the end-to-end tests of
//! this repository report both per operation (`CLAUDE.md` §1a,
//! `docs/benchmarks.md`).
//!
//! It is measurement only: no shipped crate depends on it. A benchmark or a
//! test installs [`alloc::Counting`] as its global allocator and switches the
//! counting on around what it measures.
//!
//! A test or a benchmark that hands a call a `Waker` takes a counting one from
//! [`wake`]. A real-socket test or an E2E member waits for a datagram with
//! [`wait::arrives`].
//!
//! A latency benchmark schedules its requests with [`open_loop`] and states
//! each quantile it reports with its interval from [`quantile`]
//! (`docs/tails.md` §3, step T-1).
//!
//! The `unsafe` this crate needs is in five files that
//! `scripts/check-contracts.py` lists: `src/alloc.rs` (the allocator forwards
//! to the system's), `src/faults.rs` (the OS calls that read the faults),
//! `src/usage.rs` (the OS calls that read CPU time, instructions, cycles and
//! the footprint), `src/wake.rs` (a waker built over a leaked slot) and
//! `src/wait_windows.rs` (the poll a wait is on Windows).

#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::disallowed_macros
    )
)]

pub mod alloc;
pub mod cost;
pub mod faults;
pub mod open_loop;
pub mod quantile;
pub mod stats;
pub mod usage;
pub mod wait;
pub mod wake;
