//! The world every deterministic simulation here runs in (`docs/sim.md`, step S-1).
//!
//! - **Randomness** ([`rng`]): one SplitMix64 with focal's exact `below`, and a named stream per
//!   source, so adding draws to one source leaves every other's draws unchanged.
//! - **Time** ([`clock`]): virtual nanoseconds from the world's start, jumping to the next event;
//!   each node's monotonic and wall clocks as views of it, with an offset, a rate in parts per
//!   million, steps of the wall clock, and timers that fire late by a drawn amount.
//! - **The world** ([`World`]): the nodes, one event queue and each node's timer, under the
//!   **ordered** discipline (only the earliest enabled, ties chosen) or the **free** one (every
//!   pending event enabled); a [`Strategy`] chooses at each choice point.
//! - **The network** ([`net`], step S-2): messages and datagrams on focal's path model, each draw
//!   from its flow's stream and each arrival an event of the world; partitions, duplication and a
//!   stated capacity, its oldest message lost past it.
//! - **Storage** ([`storage`], step S-3): the block device, hyper-block's simulated file moved
//!   here with the lost, misdirected, zeroed and junk faults and the crash states within a
//!   persistence point; and the typed write queue, whose completions are events of the world.
//! - **The trace** ([`Trace`]) and the **digest** ([`Digest`]): every choice recorded, so a run
//!   replays from its seed or from its trace; and [`twice`], which runs a seed twice and its
//!   trace once and refuses a run whose digests differ.
//!
//! It is held to the production lints, has no `unsafe` and no dependency outside `std`
//! (`docs/sim.md` §2), and makes no OS call inside a run: its one host clock read is the
//! [`Instant`](std::time::Instant) anchor taken when a world is made (§3.2).

#![cfg_attr(
    test,
    allow(
        clippy::panic,
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::disallowed_macros
    )
)]

pub mod clock;
mod error;
pub mod net;
mod queue;
pub mod rng;
pub mod storage;
pub mod tape;
mod trace;
mod twice;
mod wakes;
mod world;

pub use clock::{Anchor, Clock, Lateness, PPM};
pub use error::SimError;
pub use queue::Discipline;
pub use rng::Seeded;
pub use trace::{Digest, Trace};
pub use twice::{Twice, twice};
pub use world::{
    Candidate, Choice, Draw, EventId, Fifo, Limits, NodeId, Random, Record, Source, Step, Strategy,
    StreamId, World,
};
