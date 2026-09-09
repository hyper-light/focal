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
//! Host-side compiler from authored native documents to `FCNINPUT1` frames.
//!
//! The CLI, MCP server and embedded hosts share this one path so that every
//! adapter submits byte-identical frames for identical documents, identities
//! and resolved bindings. Bindings are read once from a fixed ledger prefix
//! before the first send and persisted with the frame by the client journal;
//! a retry resends the exact journaled bytes and never recompiles (doc 21 §4).
//!
//! This crate depends on `focal-core` for the native input types and codec
//! (decision F17); `focal-client` itself stays free of the core crate.
mod compile;
mod driver;
mod frame;
mod observe;
mod peer;
mod resolve;
#[cfg(test)]
mod tests;

pub use compile::{Compiled, compile};
pub use driver::{
    CLAIM_EXPAND, DriveError, Lists, NativeReadOutcome, Preparation, READ_ITEMS, Reads, admissible,
    list, list_request, outcome, prepare, read, resolve,
};
pub use focal_core::native::NativeContentProfile;
pub use frame::{FrameLimits, encode_frame, fingerprint};
pub use observe::{LINEAGE_DEPTH, Pause, WAIT_PROBES, lineage, wait};
pub use resolve::{
    EvaluationSelector, Requirement, Resolved, ResolvedArtifact, ResolvedClaim, ResolvedDiagnostic,
    ResolvedEvaluation, ResolvedResponse, ResolvedResultTestament, ResolvedWork, requirements,
};

use focal_client::input::InputError;
use focal_core::native::{
    NativeError, NativeLimits,
    input_codec::{CodecError, DecodeError, EncodingLimits},
};
use focal_model::lifecycle::{
    ContractError, aggregation, artifact_descriptor, claim_descriptor, validation,
    validation_descriptor,
};

#[derive(Debug, thiserror::Error)]
pub enum CompileError {
    #[error(transparent)]
    Input(#[from] InputError),
    #[error("authored native input violates the model contract: {0:?}")]
    Contract(ContractError),
    #[error("native input construction exceeded its bounded budget: {0}")]
    Capacity(&'static str),
    #[error("native frame encoding failed: {0:?}")]
    Codec(CodecError),
    #[error("the ledger read did not supply a required object: {0}")]
    Missing(&'static str),
    #[error("the resolved ledger state cannot support this operation: {0}")]
    Unsupported(&'static str),
}
impl From<ContractError> for CompileError {
    fn from(error: ContractError) -> Self {
        Self::Contract(error)
    }
}
impl From<NativeError> for CompileError {
    fn from(error: NativeError) -> Self {
        match error {
            NativeError::Contract(error) => Self::Contract(error),
            NativeError::Capacity(what) => Self::Capacity(what),
            NativeError::Memory(_) => Self::Capacity("memory"),
            NativeError::RequestConflict => Self::Unsupported("request identity conflict"),
            NativeError::Evidence(_) => Self::Unsupported("evidence"),
        }
    }
}
impl From<focal_memory::MemoryError> for CompileError {
    fn from(_: focal_memory::MemoryError) -> Self {
        Self::Capacity("memory")
    }
}
impl From<CodecError> for CompileError {
    fn from(error: CodecError) -> Self {
        Self::Codec(error)
    }
}
impl From<DecodeError> for CompileError {
    fn from(error: DecodeError) -> Self {
        match error {
            DecodeError::Codec(error) => Self::Codec(error),
            DecodeError::Native(error) => error.into(),
        }
    }
}

/// Bounded construction limits for one authored operation. These are client
/// bounds on what a human or agent may author; the owner rechecks every
/// dimension against the session's limits when it decodes the frame.
#[derive(Debug, Clone, Copy)]
pub struct CompileLimits {
    pub claim: claim_descriptor::Limits,
    pub validation: validation_descriptor::Limits,
    pub artifact: artifact_descriptor::Limits,
    pub acceptance: aggregation::Limits,
    pub native: NativeLimits,
    pub frame: FrameLimits,
}
impl Default for CompileLimits {
    fn default() -> Self {
        Self {
            claim: claim_descriptor::Limits {
                description_bytes: 16 * 1024,
                relations: 252,
                scopes: 256,
                scope_key_bytes: 1024,
                requirements: 64,
                slots: 64,
                checks: 64,
                construction_bytes: 4 * 1024 * 1024,
            },
            validation: validation_descriptor::Limits {
                declaration: validation::Limits {
                    handlers: 64,
                    attempts: 64,
                    slot_bytes: 256,
                },
                description_bytes: 16 * 1024,
                quality_bar_bytes: 16 * 1024,
                contributors: 256,
                construction_bytes: 1024 * 1024,
            },
            artifact: artifact_descriptor::Limits {
                kind_bytes: 128,
                metadata_bytes: 16 * 1024,
                inline_bytes: 256 * 1024,
                inputs: 64,
                visibility_labels: 64,
                visibility_label_bytes: 128,
                construction_bytes: 1024 * 1024,
            },
            acceptance: aggregation::Limits {
                max_slots: 64,
                max_checks: 64,
                max_results: 4096,
                max_updates: 4096,
            },
            native: NativeLimits::default(),
            frame: FrameLimits::default(),
        }
    }
}
impl CompileLimits {
    pub fn encoding(&self) -> EncodingLimits {
        EncodingLimits {
            bytes: self.frame.bytes,
            visits: self.frame.visits,
        }
    }
}
