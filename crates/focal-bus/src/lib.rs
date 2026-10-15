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
//! focal's event bus.
//!
//! The ledger's whole concern is that a claim reaches its respondent, a testament reaches its
//! recipient, and the artifacts a testament references can be fetched correctly and quickly. The bus
//! carries the first two as small messages, held in memory, with durability off the delivery path;
//! artifacts are referenced by their content address and never travel through it
//! (`docs/archictecutre/32-event-bus.md`).
//!
//! Built in steps, each measured before the next: the payloads and their codec (step 0,
//! [`corpus`] and `benches/codec.rs`), the in-memory core (step 1), one node over QUIC (step 2) and
//! three replicated nodes (step 3).

pub mod batch;
pub mod corpus;
pub mod log;
pub mod part;
pub mod stream;
pub mod window;

/// Why the bus refused or failed an operation. Every refusal is typed; none leaves a part changed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BusError {
    /// The bytes offered are not a batch the part takes.
    Batch(batch::BatchError),
    /// The memory budget refused what the operation needs.
    Memory(focal_memory::MemoryError),
    /// A bound the operation would pass, named.
    Capacity(&'static str),
    /// A configuration no part can run with.
    Configuration(&'static str),
    /// A stream the part does not hold.
    UnknownStream(u64),
}

impl std::fmt::Display for BusError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Batch(error) => write!(f, "batch refused: {error}"),
            Self::Memory(error) => write!(f, "memory budget: {error}"),
            Self::Capacity(bound) => write!(f, "at its bound: {bound}"),
            Self::Configuration(what) => write!(f, "configuration: {what}"),
            Self::UnknownStream(stream) => write!(f, "no stream {stream} in this part"),
        }
    }
}

impl std::error::Error for BusError {}
