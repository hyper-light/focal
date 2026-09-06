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
//! Node composition and user-facing deployment intent.
pub mod cluster;
pub mod config;
pub mod content_host;
pub mod control_host;
pub mod custody;
pub mod demo;
pub mod embedded;
pub mod evidence_service;
pub mod fleet;
#[cfg(test)]
mod fleet_tests;
pub mod host;
pub mod network_bootstrap;
pub mod network_contacts;
pub mod network_join;
pub mod network_listener;
pub mod network_state;
pub mod node_directory;
pub mod placement;
pub mod quorum_enrollment;
mod reads;
pub mod replication;
mod streams;
#[cfg(test)]
mod streams_tests;
