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
pub mod cluster;
pub mod cluster_admin;
pub mod config;
pub mod content_host;
pub mod control_host;
pub mod credential_renewal;
pub mod custody;
pub mod custody_prefix;
pub mod demo;
pub mod directory_bootstrap;
pub mod embedded;
pub mod evidence_service;
pub mod fault;
pub mod fleet;
#[cfg(test)]
mod fleet_tests;
pub mod host;
pub mod liveness;
#[cfg(test)]
mod liveness_tests;
pub mod managed_service;
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
pub mod placement_journal;
pub mod placement_proof;
pub mod quorum_enrollment;
mod reads;
mod reconciliation;
pub mod replication;
pub mod session_registration;
mod streams;
#[cfg(test)]
mod streams_tests;

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
