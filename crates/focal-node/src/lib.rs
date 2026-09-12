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
pub mod admission;
pub mod archive_agent;
pub mod backup;
pub mod cluster;
pub mod cluster_admin;
pub mod config;
pub mod content_host;
pub mod control_host;
pub mod credential_renewal;
pub mod custody;
pub mod custody_prefix;
pub mod demo;
pub mod deployment;
pub mod directory_bootstrap;
pub mod embedded;
pub mod evidence_service;
pub mod fault;
pub mod fleet;
#[cfg(test)]
mod fleet_tests;
pub mod gc;
pub mod host;
pub mod liveness;
#[cfg(test)]
mod liveness_tests;
pub mod managed_service;
pub mod metrics;
pub mod network_admin;
pub mod network_bootstrap;
pub mod network_contacts;
pub mod network_control;
pub mod network_controller;
pub mod network_join;
pub mod network_listener;
pub mod network_service;
pub mod network_state;
pub mod node_directory;
pub mod placement;
pub mod placement_agent;
pub mod placement_collect;
pub mod placement_control;
pub mod placement_executor;
pub mod placement_journal;
pub mod placement_proof;
pub mod quorum_enrollment;
pub mod range_balancer;
mod reads;
mod reconciliation;
pub mod replication;
pub mod route_cache_host;
#[cfg(test)]
mod route_cache_tests;
pub mod session_control;
pub mod session_registration;
mod streams;
#[cfg(test)]
mod streams_tests;
pub mod topology;
pub mod upgrade;

mod managed_support;

mod managed_requests;

mod participant_ingress;
mod snapshot_feedback;

mod ledger_summary;
mod monitor_reads;
pub mod native_activation;
mod native_documents;
#[cfg(test)]
#[path = "native_host_tests.rs"]
mod native_host_tests;
mod native_ingress;
mod native_lists;
mod native_reads;
mod native_timers;

/// Test-only: tighten a fresh temp path to owner-only. On Unix this sets the
/// POSIX mode the private-directory checks require; on Windows a fresh temp
/// directory already inherits an owner-only DACL from the temp root, so it is a
/// no-op (mirrors the cli_native_a1 harness). Confined to `cfg(test)`, so no
/// production build sees it.
#[cfg(test)]
pub(crate) fn set_test_mode(path: &std::path::Path, mode: u32) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
    }
    #[cfg(not(unix))]
    let _ = (path, mode);
}
