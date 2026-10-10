//! `hyper-rt` — the shared runtime (docs/runtime.md), ported from slates' `rt` (ORIGIN.md): a thread-per-core executor with arena task slots and `Copy` waker
//! encodings, an intrusive run queue, a hierarchical timing wheel, per-shard inbound rings with
//! cross-shard wake kicks, the OS drivers behind one completion seam, and a deterministic
//! simulation driver (design §4.3, D-7, D-9).
//!
//! The doctrine, in one paragraph. A task belongs to one shard for its whole life; nothing is
//! work-stolen and nothing is reference counted. A `Waker` is a `RawWaker` whose data pointer is
//! the packed `shard:16 | slot:24 | generation:24` word of the task's handle, so `clone` and
//! `drop` are no-ops and `wake` from the owning shard pushes the slot onto the local run queue,
//! `wake` from another shard pushes the word onto that pair's single-producer ring and kicks the
//! target's driver, and `wake` from a foreign thread goes through the target's multi-producer ring
//! (embassy-executor's model on std [C: embassy-executor src/raw; B: `RawWakerVTable` docs]). A
//! stale generation is ignored. Timers live in a hierarchical timing wheel [A: Varghese & Lauck,
//! SOSP'87] whose tick is derived from the measured wake cost. Every operation is cancel-safe by
//! construction: resources live in arenas keyed by handle, so dropping a future releases nothing
//! it did not own; a parent's completion cancels and joins its children (hecate's task-lifecycle
//! law). The drivers (epoll, kqueue, IOCP) share one seam: block until a kick, a
//! completion or a deadline; the simulation driver replaces time and the kick with seeded,
//! single-threaded stand-ins so a whole cluster runs deterministically in one process (D-20).
//!
//! `LocalWaker` is still a nightly-only API on Rust 1.98 (`local_waker`, #118959), so the
//! executor uses `Waker` with a vtable that is thread-safe by construction; the `Copy` encoding
//! costs nothing to clone, which is what `LocalWaker` would have saved (Phase 0 task 6).
//!
//! Modules: [`error`], [`control`], [`waker`], [`registry`], [`parking`] (the kick-if-parked
//! protocol and its loom model), [`task`], [`queue`], [`timer`], [`driver`], [`sim`], [`shard`],
//! [`runtime`], [`futures`], `attribution` (who held a long poll: the task or the host), the async
//! sockets ([`udp`] and [`tcp`] on every OS, over one shared readiness future), and the OS drivers.

// Test code opts out of the no-panic wall at the crate root (CLAUDE.md §1); shipped code does not.
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::disallowed_macros,
        clippy::disallowed_methods,
        clippy::cognitive_complexity,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_possible_wrap,
        clippy::string_slice,
        clippy::unwrap_in_result,
        clippy::panic_in_result_fn,
        clippy::missing_panics_doc
    )
)]

mod attribution;
pub mod blocking;
mod cells;
pub mod combine;
pub mod control;
pub mod dns;
pub mod driver;
pub mod error;
pub mod futures;
mod handoff;
pub mod interests;
pub mod local;
mod localsys;
pub mod machine;
pub mod mem;
mod netsys;
pub mod parking;
pub mod queue;
pub mod readiness;
pub mod registry;
mod retire;
pub mod runtime;
pub mod shard;
pub mod shard_loop;
pub mod signal;
#[cfg(unix)]
mod signal_protocol;
pub mod sim;
pub mod stdio;
pub mod sync;
pub mod task;
mod thread_clock;
pub use thread_clock::CpuReading;
pub mod tcp;
mod tcpsys;
pub mod timer;
pub mod udp;
pub mod waker;
pub mod wakes;

#[cfg(target_os = "windows")]
mod afd;
#[cfg(target_os = "linux")]
pub mod epoll;
#[cfg(target_os = "windows")]
pub mod iocp;
#[cfg(any(target_os = "macos", target_os = "freebsd"))]
pub mod kqueue;

pub use driver::{Driver, DriverKind};
pub use error::RtError;
pub use registry::SlotHolder;
pub use runtime::{Runtime, RuntimeConfig, WakeTracking};
pub use shard::{ShardId, TaskId};
pub use sim::SimRuntime;
pub use task::Outcome;
pub use task::{Admission, AdmissionReceipt};
