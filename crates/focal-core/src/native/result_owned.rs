//! Fallible native artifact/result storage. Existing descriptor buffers move
//! into one owned allocation; custody is an immutable local capability, not a
//! second lifecycle owner. Storage reserves complete old-row charges before copy.
use focal_evidence::NativeLocalCustody;
use focal_memory::MemoryError;
use focal_model::SessionSeq;
use focal_model::lifecycle::{
    ContractError,
    artifact_descriptor::ArtifactDescriptor,
    audit::ResultArtifact,
    validation::{AcceptedResult, Attempt, EvidenceFacts, Target},
};

const ALLOCATION: usize = 4 * size_of::<usize>();
const INPUT_CONTAINER: usize = size_of::<ArtifactDescriptor>() + ALLOCATION;
const ARTIFACT_CONTAINER: usize = size_of::<NativeArtifact>() + ALLOCATION;
const ACCEPTED_CONTAINER: usize = size_of::<NativeAccepted>() + ALLOCATION;

/// Typed native ingress ownership. Descriptor construction and this indirection
/// are fallible; a future decoder must reserve their ingress charge first. This
/// wrapper conveys neither schema verification nor durable custody.
#[derive(Debug)]
pub struct NativeArtifactInput(Vec<ArtifactDescriptor>);

/// Immutable descriptor and exact peer-authored provenance retained by Core.
/// Only the owner can construct this after resolving its work/evaluation authority
/// and checking the request-bound custody capability.
#[derive(Debug)]
pub struct NativeArtifact {
    descriptor: ArtifactDescriptor,
    custody: NativeLocalCustody,
}

/// One actual accepted report with its original attempt and publication cut.
/// A later evaluation phase, retry, fence or result cannot repaint this history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeAccepted {
    attempt: Attempt,
    artifact: ResultArtifact,
    sequence: SessionSeq,
    ordinal: u32,
}

#[derive(Debug)]
pub(super) struct OwnedArtifact(Vec<NativeArtifact>);
#[derive(Debug)]
pub(super) struct OwnedAccepted(Vec<NativeAccepted>);

fn add(left: usize, right: usize) -> Result<usize, MemoryError> {
    left.checked_add(right)
        .ok_or(MemoryError::CounterExhausted("native result heap charge"))
}
fn container_heap<T>(capacity: usize) -> Result<usize, MemoryError> {
    add(
        capacity
            .checked_mul(size_of::<T>())
            .ok_or(MemoryError::CounterExhausted("native result capacity"))?,
        if capacity == 0 { 0 } else { ALLOCATION },
    )
}
fn within(actual: usize, allowance: usize) -> Result<(), MemoryError> {
    if actual > allowance {
        Err(MemoryError::Capacity {
            requested: actual,
            available: allowance,
        })
    } else {
        Ok(())
    }
}
fn singleton<T>(value: T, allowance: usize) -> Result<Vec<T>, MemoryError> {
    within(container_heap::<T>(1)?, allowance)?;
    let mut rows = Vec::new();
    rows.try_reserve_exact(1)
        .map_err(|_| MemoryError::AllocationFailed)?;
    within(container_heap::<T>(rows.capacity())?, allowance)?;
    rows.push(value);
    Ok(rows)
}
fn get<T>(rows: &[T]) -> Option<&T> {
    match rows {
        [row] => Some(row),
        _ => None,
    }
}
fn descriptor_heap(descriptor: &ArtifactDescriptor) -> Result<usize, MemoryError> {
    let heap = descriptor
        .retained_heap_bytes()
        .map_err(|_| MemoryError::AllocationFailed)?;
    let allocations = descriptor
        .heap_allocations()
        .map_err(|_| MemoryError::AllocationFailed)?;
    add(
        heap,
        allocations
            .checked_mul(ALLOCATION)
            .ok_or(MemoryError::CounterExhausted(
                "native artifact allocator charge",
            ))?,
    )
}
fn copy_descriptor(descriptor: &ArtifactDescriptor) -> Result<ArtifactDescriptor, MemoryError> {
    let allowance = descriptor
        .retained_bytes()
        .map_err(|_| MemoryError::AllocationFailed)?;
    descriptor
        .try_copy(allowance)
        .map_err(|_| MemoryError::AllocationFailed)
}

impl NativeArtifactInput {
    pub const fn container_charge() -> usize {
        INPUT_CONTAINER
    }
    pub fn new(descriptor: ArtifactDescriptor) -> Result<Self, MemoryError> {
        add(Self::container_charge(), descriptor_heap(&descriptor)?)?;
        Ok(Self(singleton(descriptor, Self::container_charge())?))
    }
    pub fn get(&self) -> Option<&ArtifactDescriptor> {
        get(&self.0)
    }
    /// Move the descriptor out without copying any of its retained buffers.
    pub fn into_descriptor(mut self) -> Result<ArtifactDescriptor, MemoryError> {
        if self.0.len() != 1 {
            return Err(MemoryError::MissingKey);
        }
        self.0.pop().ok_or(MemoryError::MissingKey)
    }
    pub fn heap_charge(&self) -> Result<usize, MemoryError> {
        let descriptor = self.get().ok_or(MemoryError::MissingKey)?;
        add(
            container_heap::<ArtifactDescriptor>(self.0.capacity())?,
            descriptor_heap(descriptor)?,
        )
    }
    /// Caller reserves the complete old input charge before copying it.
    pub fn copy(&self) -> Result<Self, MemoryError> {
        let old = self.heap_charge()?;
        let copied = Self::new(copy_descriptor(self.get().ok_or(MemoryError::MissingKey)?)?)?;
        within(copied.heap_charge()?, old)?;
        Ok(copied)
    }
}

impl NativeArtifact {
    pub(super) fn from_work(
        descriptor: ArtifactDescriptor,
        custody: NativeLocalCustody,
        request: focal_model::RequestKey,
    ) -> Result<Self, ContractError> {
        if descriptor.result_provenance().is_some() || descriptor.work_provenance().is_none() {
            return Err(ContractError::MissingEvidence);
        }
        custody
            .check(request, &descriptor)
            .map_err(|_| ContractError::MissingEvidence)?;
        Ok(Self {
            descriptor,
            custody,
        })
    }

    pub(super) fn new(
        descriptor: ArtifactDescriptor,
        custody: NativeLocalCustody,
        facts: EvidenceFacts,
    ) -> Result<Self, ContractError> {
        descriptor.binding().check(&facts.binding)?;
        if descriptor.producer() != facts.producer {
            return Err(ContractError::WrongActor);
        }
        if descriptor.schema_hash() != facts.schema
            || facts.custody_revision != Some(custody.local_revision())
        {
            return Err(ContractError::MissingEvidence);
        }
        if matches!(facts.target, Target::Admission { .. }) && descriptor.receipt().is_some() {
            return Err(ContractError::StaleReceipt);
        }
        let expected = focal_model::lifecycle::artifact_descriptor::ResultProvenance {
            claim: facts.claim,
            validation: facts.validation,
            target: facts.target,
            generation: facts.generation,
            attempt: facts.attempt,
            value: facts.value,
        };
        if descriptor.result_provenance() != Some(expected) {
            return Err(ContractError::MissingEvidence);
        }
        let kind = match facts.value {
            focal_model::VerdictValue::Pass | focal_model::VerdictValue::Fail => {
                focal_model::lifecycle::validation::EvidenceKind::Proof
            }
            focal_model::VerdictValue::Incomplete | focal_model::VerdictValue::Error => {
                focal_model::lifecycle::validation::EvidenceKind::Diagnostic
            }
        };
        if facts.kind != kind {
            return Err(ContractError::MissingEvidence);
        }
        Ok(Self {
            descriptor,
            custody,
        })
    }
    pub fn descriptor(&self) -> &ArtifactDescriptor {
        &self.descriptor
    }
    pub fn custody(&self) -> NativeLocalCustody {
        self.custody
    }
    /// Project metadata from the single retained descriptor provenance. No second
    /// copy of the target and attempt is kept alongside the immutable artifact.
    pub fn facts(&self) -> Option<EvidenceFacts> {
        let provenance = self.descriptor.result_provenance()?;
        Some(EvidenceFacts {
            binding: self.descriptor.binding(),
            claim: provenance.claim,
            validation: provenance.validation,
            target: provenance.target,
            generation: provenance.generation,
            attempt: provenance.attempt,
            producer: self.descriptor.producer(),
            value: provenance.value,
            kind: match provenance.value {
                focal_model::VerdictValue::Pass | focal_model::VerdictValue::Fail => {
                    focal_model::lifecycle::validation::EvidenceKind::Proof
                }
                focal_model::VerdictValue::Incomplete | focal_model::VerdictValue::Error => {
                    focal_model::lifecycle::validation::EvidenceKind::Diagnostic
                }
            },
            schema: self.descriptor.schema_hash(),
            custody_revision: Some(self.custody.local_revision()),
        })
    }
    fn copy(&self) -> Result<Self, MemoryError> {
        Ok(Self {
            descriptor: copy_descriptor(&self.descriptor)?,
            custody: self.custody,
        })
    }
}

impl NativeAccepted {
    pub(super) fn new(
        result: AcceptedResult,
        attempt: Attempt,
        artifact: ResultArtifact,
        sequence: SessionSeq,
        ordinal: u32,
    ) -> Result<Self, ContractError> {
        if sequence.0 == 0 {
            return Err(ContractError::InvalidCut);
        }
        if result.attempt() != Some(attempt.index) || result.phase() != attempt.phase {
            return Err(ContractError::StaleEvaluation);
        }
        if result.reporter() != Some(attempt.evaluator) || artifact.producer() != attempt.evaluator
        {
            return Err(ContractError::WrongActor);
        }
        if artifact.result() != result || result.evidence() != Some(artifact.reference()) {
            return Err(ContractError::MissingEvidence);
        }
        Ok(Self {
            attempt,
            artifact,
            sequence,
            ordinal,
        })
    }
    pub(super) fn result_ref(&self) -> &AcceptedResult {
        self.artifact.result_ref()
    }
    pub fn result(&self) -> AcceptedResult {
        self.artifact.result()
    }
    pub fn attempt(&self) -> Attempt {
        self.attempt
    }
    pub fn artifact(&self) -> ResultArtifact {
        self.artifact
    }
    pub fn sequence(&self) -> SessionSeq {
        self.sequence
    }
    pub fn ordinal(&self) -> u32 {
        self.ordinal
    }
}

impl OwnedArtifact {
    pub(super) const fn container_charge() -> usize {
        ARTIFACT_CONTAINER
    }
    pub(super) fn new(artifact: NativeArtifact) -> Result<Self, MemoryError> {
        add(
            Self::container_charge(),
            descriptor_heap(&artifact.descriptor)?,
        )?;
        Ok(Self(singleton(artifact, Self::container_charge())?))
    }
    pub(super) fn get(&self) -> Option<&NativeArtifact> {
        get(&self.0)
    }
    pub(super) fn heap_charge(&self) -> Result<usize, MemoryError> {
        let artifact = self.get().ok_or(MemoryError::MissingKey)?;
        add(
            container_heap::<NativeArtifact>(self.0.capacity())?,
            descriptor_heap(&artifact.descriptor)?,
        )
    }
    pub(super) fn copy(&self) -> Result<Self, MemoryError> {
        let old = self.heap_charge()?;
        let copied = Self::new(self.get().ok_or(MemoryError::MissingKey)?.copy()?)?;
        within(copied.heap_charge()?, old)?;
        Ok(copied)
    }
}

impl OwnedAccepted {
    pub(super) const fn container_charge() -> usize {
        ACCEPTED_CONTAINER
    }
    pub(super) fn new(accepted: NativeAccepted) -> Result<Self, MemoryError> {
        Ok(Self(singleton(accepted, Self::container_charge())?))
    }
    pub(super) fn get(&self) -> Option<&NativeAccepted> {
        get(&self.0)
    }
    pub(super) fn heap_charge(&self) -> Result<usize, MemoryError> {
        self.get().ok_or(MemoryError::MissingKey)?;
        container_heap::<NativeAccepted>(self.0.capacity())
    }
    pub(super) fn copy(&self) -> Result<Self, MemoryError> {
        let old = self.heap_charge()?;
        let copied = Self::new(*self.get().ok_or(MemoryError::MissingKey)?)?;
        within(copied.heap_charge()?, old)?;
        Ok(copied)
    }
}

#[cfg(test)]
#[path = "result_owned_tests.rs"]
mod tests;
