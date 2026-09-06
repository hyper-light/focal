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
//! Embedded and remote clients preserving durable request identities across retries.
mod client;
mod transport;
pub use client::*;
pub use focal_wire::{
    AccessError, ContentChunk, Credits, MutationReply, ObjectKey, Operation, ReadConsistency,
    ReadPage, ReadQuery, ReadRequest, ReadToken, RequestEnvelope, Response, ResponseEnvelope,
    RouteHint, SubscribeRequest, SubscriptionBatch, UploadReply, UploadRequest, WireLimits,
};
pub use focal_wire::{
    ConsumerId, CursorToken, DeltaFilter, Position, PositionOffset, StreamEvent, StreamReply,
    StreamRequest,
};
pub use transport::*;
#[cfg(test)]
mod tests;
