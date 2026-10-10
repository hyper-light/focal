//! The hyper-rt adapter (docs/runtime.md §14): drives hyper-transport's
//! [`Endpoint`](hyper_transport::Endpoint) and hyper-datagram's [`Plane`](hyper_datagram::Plane) on a
//! hyper-rt shard, with the API of hyper-tokio's driver (`bind`, `new`, `endpoint`, `event`,
//! `poll_event`, `flush`, `local_addr`, `stats`), so a consumer moves from one to the other by type. The
//! sans-I/O crates never name a runtime; this crate is the one that names hyper-rt, as hyper-tokio is
//! the one that names tokio.
//!
//! **Shape.** A [`Driver`] owns one endpoint, its batched UDP socket ([`hyper_rt::udp::Batched`]) and one
//! timer. The owner holds the driver in its task and awaits [`Driver::event`], which sends what the
//! endpoint transmits, feeds it the datagrams that arrive and the timeouts that fall due, and returns its
//! next event. Between events the owner calls the endpoint through [`Driver::endpoint`]. No task, thread,
//! channel or lock inside; `event` is cancel-safe.
//!
//! **Time.** The endpoint speaks `std::time::Instant`. The driver anchors one `Instant` to the shard's clock
//! when it is made and derives every later instant from the shard's clock, so the endpoint and the shard's
//! timers agree, and on the simulation runtime the endpoint runs on simulated time, deterministically.
//!
//! **Bounds.** The outbox holds at most [`Io::batch`] datagrams; a receive turn takes at most a batch;
//! one poll takes at most [`TURNS`] turns before it yields.

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

mod driver;
mod plane;

pub use driver::{Driver, TURNS};
pub use hyper_rt::udp::{Arrival, Io, IoStats, MAX_BATCH, RECEIVE_BYTES};
pub use plane::PlaneSocket;
