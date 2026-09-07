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
pub mod admin;
pub mod artifact_transfer;
mod client;
pub mod failure;
mod file_lock;
pub mod input;
pub mod managed_requests;
pub mod managed_store;
pub mod operation_store;
pub mod operations;
mod participant;
pub mod pending;
mod transport;
pub mod validation_context;
pub mod watch;
pub use client::*;
pub use focal_wire::TraversalPage;
pub use focal_wire::{
    AccessError, ContentChunk, Credits, LedgerSummary, MutationReply, ObjectKey, Operation,
    ReadConsistency, ReadPage, ReadQuery, ReadRequest, ReadToken, ReconcileReply, RequestEnvelope,
    Response, ResponseEnvelope, RouteHint, SubscribeRequest, SubscriptionBatch, UploadReply,
    UploadRequest, WireLimits,
};
pub use focal_wire::{
    ConsumerId, CursorToken, DeltaFilter, Position, PositionOffset, StreamEvent, StreamReply,
    StreamRequest,
};
pub use focal_wire::{
    ManagedOperation, ManagedReply, RequestStreamControlReply, RequestStreamReadReply,
};
pub use transport::*;
#[cfg(test)]
mod tests;

pub mod claim_get;
pub mod claim_wait;
