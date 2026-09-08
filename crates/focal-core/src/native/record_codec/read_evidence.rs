//! Detached evidence restoration. Parsing produces borrowed recorded values;
//! preparation checks intrinsic model consistency against retained dependencies.
//! Neither parsing nor hydration grants current participant or custody authority.
use super::{
    bytes::{Cursor, Error},
    read_fields as fields,
};
use crate::native::{NativeError, Row};
use focal_memory::MemoryError;
use focal_model::lifecycle::{
    ContractError, aggregation::AcceptancePolicy, artifact_descriptor::ArtifactDescriptor,
    validation::Declaration,
};
use focal_model::{ArtifactRef, ClaimId, ValidationId};

#[path = "read_evidence_content.rs"]
mod content;
#[path = "read_evidence_creation.rs"]
mod creation;
#[path = "read_evidence_response.rs"]
mod response;
#[path = "read_evidence_scalar.rs"]
mod scalar;
pub(super) use content::{Custody, artifact, claim_content, definition};
pub(super) use creation::creation;
pub(super) use response::response;
pub(super) use scalar::{ScalarInput, accepted, delivery, diagnostic, missing, work};

/// Exact final row heap, including singleton and allocator bookkeeping. The
/// enclosing store separately funds its inline Entry/Row and temporary policy
/// dependency workspace. Parsing/source/model domains are cumulative at import.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Quote {
    pub(super) heap_bytes: usize,
    pub(super) allocations: usize,
    pub(super) model_inspection_visits: usize,
    pub(super) model_build_visits: usize,
    pub(super) source_inspection_visits: usize,
    pub(super) source_build_visits: usize,
}
pub(super) trait Dependencies {
    /// May be independently restored policy workspace; never fabricate a Claim.
    fn policy(&self, claim: ClaimId) -> Result<&AcceptancePolicy, ContractError>;
    fn artifact(&self, reference: ArtifactRef) -> Result<&ArtifactDescriptor, ContractError>;
    fn declaration(&self, id: ValidationId) -> Result<&Declaration, ContractError>;
}
fn add(a: usize, b: usize) -> Result<usize, NativeError> {
    a.checked_add(b)
        .ok_or(NativeError::Capacity("recovery size"))
}
fn mul(a: usize, b: usize) -> Result<usize, NativeError> {
    a.checked_mul(b)
        .ok_or(NativeError::Capacity("recovery size"))
}
fn fits(actual: usize, limit: usize) -> Result<(), NativeError> {
    if actual > limit {
        Err(NativeError::Capacity("recovery allowance"))
    } else {
        Ok(())
    }
}
pub(super) fn codec(error: Error) -> NativeError {
    match error {
        Error::Capacity => NativeError::Capacity("recovery codec allowance"),
        Error::Allocation => NativeError::Memory(MemoryError::AllocationFailed),
        _ => NativeError::Contract(ContractError::InvalidManifest),
    }
}
fn decode(error: crate::native::input_codec::DecodeError) -> NativeError {
    match error {
        crate::native::input_codec::DecodeError::Codec(error) => codec(error),
        crate::native::input_codec::DecodeError::Native(error) => error,
    }
}
fn exact_artifact(
    deps: &impl Dependencies,
    reference: ArtifactRef,
) -> Result<&ArtifactDescriptor, NativeError> {
    let value = deps.artifact(reference)?;
    if value.id() != reference.id || value.content_hash() != reference.hash {
        return Err(ContractError::MissingEvidence.into());
    }
    Ok(value)
}
fn finish(row: Row, actual: usize, quote: Quote, allowance: usize) -> Result<Row, NativeError> {
    fits(actual, quote.heap_bytes)?;
    fits(actual, allowance)?;
    Ok(row)
}
