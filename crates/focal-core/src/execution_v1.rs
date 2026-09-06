//! Historical schema-1 execution for FOCALOP1 and FOCALMD1 domain entries.
//!
//! This module owns the original reducer, validation scheduling and graph rules.
//! Entry version selects these rules; current proposal policy lives separately
//! in `admission` and must not be reapplied to committed historical entries.
//!
//! The original content, lifecycle, state and output types remain shared model
//! contracts. Their V1 meanings, canonical identities and ordering must remain
//! stable; successor representations require explicit conversion. Storage views,
//! resource accounting, epoch scheduling and atomic publication remain shared
//! mechanisms outside this module.

#[path = "graph.rs"]
mod graph;
#[path = "reduce.rs"]
mod reduce;
#[path = "validation.rs"]
mod validation;

pub use graph::least_fixpoint;
#[cfg(test)]
pub(crate) use graph::{check_acyclic, cycle_containing, tracked_fixpoint};
pub(crate) use reduce::{execute_managed_on, execute_on};
