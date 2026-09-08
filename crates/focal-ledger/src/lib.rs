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
//! Durable session sequencing and atomic publication over the deterministic core.
//! Domain objects/indexes publish through pre-reserved COW pages. The serial core
//! remains an explicitly budgeted reference; keyed/parallel apply are later gates.
mod reconciliation;
mod request_streams;
pub use request_streams::{ManagedError, RequestStreamLimits, RequestStreamReadView};
mod session;
pub mod native_session;
pub use focal_core::{Core, State};
pub use focal_model::*;
pub use reconciliation::ReconciliationView;
pub use session::*;
