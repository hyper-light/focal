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
//! Durable cursor state and bounded transport over retained ledger deltas.
//!
//! The log/history source remains authoritative. This crate does not maintain
//! a second durable outbox. The embedding ledger must durably commit prepared
//! cursor transitions before publishing them or reporting acknowledgments.

mod consumer;
mod cursor;
mod registry;
mod subscription;

pub use consumer::{ConsumerCheckpoint, ConsumerDecision, PreparedConsumerAdvance};
pub use cursor::{
    ConsumerId, ConsumerKey, CursorToken, DeltaFilter, Position, PositionOffset, ResyncReason,
};
pub use registry::{
    CursorCheckpoint, CursorCommand, CursorMode, CursorOperation, CursorRecord, CursorRegistry,
    PreparedCursorUpdate, RegistryConfig,
};
pub use subscription::{
    Delivery, DeltaSource, DriveReport, ReplayBounds, ReplayLimit, StreamEvent, Subscription,
    TransportConfig,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamError {
    Invalid(&'static str),
    WrongLedger,
    WrongConsumer,
    WrongGeneration,
    WrongScope,
    MissingConsumer,
    DuplicateConsumer,
    StalePreparation,
    ClockRegression,
    CounterExhausted,
    CursorAhead,
    CursorRegression,
    BeyondDelivered,
    RetentionPinned {
        allowed_through: focal_model::SessionSeq,
    },
    ResyncRequired(ResyncReason),
    SeedNotComplete,
    SourceViolation(&'static str),
    SourceUnavailable,
    Capacity,
    Codec,
    Memory(focal_memory::MemoryError),
}

impl From<focal_memory::MemoryError> for StreamError {
    fn from(value: focal_memory::MemoryError) -> Self {
        Self::Memory(value)
    }
}
impl std::fmt::Display for StreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for StreamError {}

pub(crate) const ALLOCATOR_OVERHEAD: usize = 4 * size_of::<usize>();
pub(crate) fn add(left: usize, right: usize) -> Result<usize, StreamError> {
    left.checked_add(right).ok_or(StreamError::CounterExhausted)
}
pub(crate) fn mul(left: usize, right: usize) -> Result<usize, StreamError> {
    left.checked_mul(right).ok_or(StreamError::CounterExhausted)
}
