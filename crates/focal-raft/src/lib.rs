#![cfg_attr(
    test,
    allow(
        clippy::panic,
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::unreachable,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::disallowed_macros
    )
)]
//! focal's own consensus core (27 §4.2, §6 stage D): a state machine with no
//! clock, no disk and no network. It is told what time has passed
//! ([`RawNode::tick`]) and what arrived ([`RawNode::step`]), and it says what
//! to persist, send and apply ([`RawNode::ready`]).
//!
//! The classic track is Raft as Ongaro's thesis states it, with the
//! extensions focal runs on: pre-vote and check-quorum, election priority,
//! learners, joint consensus, leader transfer, an inflight window with
//! conflict hints, ReadIndex, and snapshots. It speaks the messages and
//! keeps the log of `raft-rs`, the core focal ran on before, so members on
//! either core form one group, and both are run on one schedule to compare
//! them.
//!
//! Nothing here unwinds ([`Error`]), and everything that grows has a bound
//! stated in [`Limits`].

pub mod configuration;
pub mod error;
pub mod fast;
pub mod log;
pub mod node;
pub mod progress;
pub mod proto;
pub mod quorum;
pub mod raft;
pub mod read;
pub mod storage;
mod track;

pub use configuration::{Change, Changed, Configuration, ConfigurationError};
pub use error::{Error, Result, StorageError};
pub use node::{LightReady, RawNode, Ready, SnapshotStatus};
pub use quorum::{Quorum, Tally};
pub use raft::{Config, FastStats, Limits, Precedence, Raft, SoftState, StateRole};
pub use read::ReadState;
pub use storage::{InitialState, Storage};

/// A member's identity. Zero is no member.
pub type NodeId = u64;
/// The most members a configuration names, voters and learners together.
pub const MAX_MEMBERS: usize = 1024;

#[cfg(test)]
mod tests;
