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
//! Bounded execution outside the ledger owner, driven by durable obligations.
mod pool;
mod service;
mod types;
use focal_model::*;
pub use service::*;
pub use types::*;
