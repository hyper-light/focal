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

pub mod corpus;
