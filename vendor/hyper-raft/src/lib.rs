#![cfg_attr(
    test,
    allow(
        clippy::panic,
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::unreachable,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::disallowed_macros,
        clippy::cognitive_complexity,
        clippy::cast_possible_truncation,
        unreachable_pub
    )
)]
//! The Raft core slates, focal and mantle share: a state machine with no
//! clock, no disk and no network. It began as focal's own core (focal 27
//! §4.2, §6 stage D) and moved here with its history (`ORIGIN.md`). It is told what time has passed
//! ([`RawNode::tick`]) and what arrived ([`RawNode::step`]), and it says what
//! to persist, send and apply ([`RawNode::ready`]).
//!
//! The classic track is Raft as Ongaro's thesis states it, with the
//! extensions focal ran it with: pre-vote and check-quorum, election priority,
//! learners, joint consensus, leader transfer, an inflight window with
//! conflict hints, ReadIndex, and snapshots. It speaks the messages and
//! keeps the log of `raft-rs`, the core focal ran on before, so members on
//! either core form one group, and both are run on one schedule to compare
//! them.
//!
//! Nothing here unwinds ([`Error`]), and everything that grows has a bound
//! stated in [`Limits`].

mod ahead;
mod catchup;
pub mod configuration;
pub mod error;
pub mod fast;
pub mod log;
pub mod mutant;
pub mod node;
pub mod progress;
pub mod proto;
pub mod quorum;
pub mod raft;
pub mod read;
pub mod storage;
mod track;
mod watch;
pub mod wire;

pub use catchup::CatchUp;
pub use configuration::{Change, Changed, Configuration, ConfigurationError};
pub use error::{Error, Result, StorageError};
pub use mutant::Mutant;
pub use node::{Kept, LightReady, RawNode, Ready, SnapshotStatus, ToPersist};
pub use quorum::{Quorum, Tally};
pub use raft::{
    Ahead, Config, Elections, FastStats, HeartbeatAnswers, Limits, Lost, Outgoing, Precedence,
    Raft, ReadRounds, SoftState, StateRole, Stated,
};
pub use read::ReadState;
pub use storage::{InitialState, Storage};
pub use watch::{TRANSFER_ROUNDS, Timing};

/// A member's identity. Zero is no member.
pub type NodeId = u64;

#[cfg(test)]
mod tests;
