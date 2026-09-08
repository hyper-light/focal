//! Immutable authored validation content for the native successor. This is a
//! checked model boundary, not a persisted schema or a live command decoder.
//!
//! The complete body commits both human-readable requirements and the actual
//! declaration policy. External standard, handler and schema hashes remain
//! references; the owner must resolve their authority and availability. Nothing
//! here executes a validator, grants permission or publishes a definition.
use super::{Binding, ContractError, Principal, memory as bytes, validation};
use crate::{
    ClaimId, ContentHash, Deadline, LedgerId, ObjectId, ObjectRevision, ParticipantId,
    ValidationId, ValidationKind, ValidationMode, ValidationPhase,
};

#[path = "validation_descriptor_hash.rs"]
mod hash;
#[path = "validation_descriptor_memory.rs"]
mod memory;
#[path = "validation_descriptor_source.rs"]
mod source;
pub use source::{ValidationFields, ValidationSource, ValidationSourcePlan};
#[cfg(test)]
#[path = "validation_descriptor_tests.rs"]
mod tests;

/// Borrowed internal input. Contributors are a sorted, unique set; handler
/// fallbacks retain authored order. Human-facing builders normalize sets before
/// reaching this boundary. No supplied digest stands in for the authored body.
#[derive(Debug, Clone, Copy)]
pub struct ValidationSpec<'a> {
    pub ledger: LedgerId,
    pub id: ValidationId,
    /// Internal model version, separate from any future durable codec allocation.
    pub schema: u16,
    pub claim: ClaimId,
    pub issuer: ParticipantId,
    pub declaration_index: u32,
    pub kind: ValidationKind,
    pub phase: ValidationPhase,
    pub mode: ValidationMode,
    pub target: validation::TargetDeclaration<'a>,
    pub program: validation::Program<'a>,
    pub deadline: Deadline,
    pub description: &'a str,
    pub quality_bar: Option<&'a str>,
    pub contributed_by: &'a [ParticipantId],
    pub policy_revision: u64,
}

impl<'a> ValidationSpec<'a> {
    fn declaration(self, content: ContentHash) -> validation::DeclarationSpec<'a> {
        validation::DeclarationSpec {
            binding: Binding {
                ledger: self.ledger,
                object: ObjectId(self.id.0),
                content,
                revision: ObjectRevision(1),
            },
            claim: self.claim,
            issuer: self.issuer,
            declaration_index: self.declaration_index,
            kind: self.kind,
            phase: self.phase,
            mode: self.mode,
            target: self.target,
            program: self.program,
            deadline: self.deadline,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub declaration: validation::Limits,
    pub description_bytes: usize,
    pub quality_bar_bytes: usize,
    pub contributors: usize,
    /// Inline descriptor plus requested dynamic buffers, excluding allocator
    /// bookkeeping. Owners add the quoted allocation count at their own layer.
    pub construction_bytes: usize,
}

/// Owns every authored field once. The declaration contains its derived binding;
/// callers can only borrow it. Lifecycle evaluations remain separate objects.
#[derive(Debug)]
pub struct ValidationDescriptor {
    schema: u16,
    declaration: validation::Declaration,
    description: String,
    quality_bar: Option<String>,
    contributed_by: Vec<ParticipantId>,
    policy_revision: u64,
    specification_hash: ContentHash,
}

#[derive(Debug)]
pub struct ValidationPlan<'a> {
    spec: ValidationSpec<'a>,
    principal: Principal,
    limits: Limits,
    shape: source::Shape,
}

impl ValidationDescriptor {
    /// Checks and fingerprints the entire bounded body without allocating.
    /// No fabricated content is added to an existing skeletal declaration.
    pub fn prepare<'a>(
        principal: Principal,
        spec: ValidationSpec<'a>,
        limits: Limits,
    ) -> Result<ValidationPlan<'a>, ContractError> {
        let (_, shape) = source::inspect(principal, &spec, limits, usize::MAX)?;
        Ok(ValidationPlan {
            spec,
            principal,
            limits,
            shape,
        })
    }

    pub fn schema(&self) -> u16 {
        self.schema
    }
    pub fn declaration(&self) -> &validation::Declaration {
        &self.declaration
    }
    pub fn binding(&self) -> Binding {
        self.declaration.binding()
    }
    pub fn content_hash(&self) -> ContentHash {
        self.binding().content
    }
    /// Authored requirement identity for `RequirementRef::specification`.
    /// Excludes the allocated parent claim as well as this validation's own ID.
    /// The full content binding separately includes the actual parent claim ID.
    pub fn specification_hash(&self) -> ContentHash {
        self.specification_hash
    }
    pub fn intent_fingerprint(&self) -> ContentHash {
        hash::intent(self.binding())
    }
    pub fn description(&self) -> &str {
        &self.description
    }
    pub fn quality_bar(&self) -> Option<&str> {
        self.quality_bar.as_deref()
    }
    pub fn contributed_by(&self) -> &[ParticipantId] {
        &self.contributed_by
    }
    pub fn policy_revision(&self) -> u64 {
        self.policy_revision
    }
}

impl<'a> ValidationPlan<'a> {
    pub fn spec(&self) -> ValidationSpec<'a> {
        self.spec
    }
    pub fn construction_charge(&self) -> usize {
        self.shape.charge
    }
    pub fn construction_heap_bytes(&self) -> usize {
        self.shape.heap
    }
    pub fn construction_heap_allocations(&self) -> usize {
        self.shape.allocations
    }
    pub fn content_hash(&self) -> ContentHash {
        self.shape.content_hash
    }
    pub fn specification_hash(&self) -> ContentHash {
        self.shape.specification_hash
    }
    pub fn intent_fingerprint(&self) -> ContentHash {
        hash::intent(self.spec.declaration(self.shape.content_hash).binding)
    }
    /// Caller holds the complete quote before entering. Both slice and source
    /// construction validate the actual owned body before returning it.
    pub fn build(self, max_bytes: usize) -> Result<ValidationDescriptor, ContractError> {
        source::build(
            &self.spec,
            self.principal,
            self.spec.fields(),
            self.limits,
            self.shape,
            max_bytes,
            self.shape.build_visits,
        )
    }
}
