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
//! Stable, versioned domain vocabulary. No clocks, storage, or execution live here.
mod canonical;
mod command;
mod ids;
mod objects;
mod vocabulary;
pub use canonical::*;
pub use command::*;
pub use ids::*;
pub use objects::*;
pub use vocabulary::*;
pub const SCHEMA_MAJOR: u16 = 1;
#[cfg(test)]
mod tests;
