//! Repeatable authored claim values. Only the final owned descriptor allocates.
use super::*;
use crate::PeerPolicy;
use crate::lifecycle::{aggregation, graph::VisitBudget};

#[path = "claim_source_adapters.rs"]
mod adapters;
#[path = "claim_source_build.rs"]
mod construction;
#[path = "claim_source_inspect.rs"]
mod inspection;
#[cfg(test)]
#[path = "claim_source_tests.rs"]
mod tests;

pub(super) use construction::build;
pub(super) use inspection::inspect;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClaimFields<'a> {
    pub ledger: LedgerId,
    pub id: ClaimId,
    pub schema: u16,
    pub occurrence: OccurrenceId,
    pub description: &'a str,
    pub deadline: Option<Deadline>,
    /// Schema 2 only: the immutable follow-up policy.
    pub policy: Option<PeerPolicy>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClaimSlotFields {
    pub slot: u32,
    pub missing_declaration_index: u32,
    pub mode: ValidationMode,
    pub checks: usize,
}

/// A copied slot header and repeatable sequential check values. Factories and
/// iterator steps must perform bounded, allocation-free work. A decoder must
/// additionally meter its parsing work; model visits cover model work only.
pub trait ClaimSlotSource {
    type Checks<'s>: Iterator<Item = Result<CheckPolicy, ContractError>>
    where
        Self: 's;
    fn fields(&self) -> ClaimSlotFields;
    fn checks(&self) -> Self::Checks<'_>;
}

/// A repeatable authored body. Counts are declarations, never trusted iterator
/// hints: every complete pass checks exactly that many values and terminal None.
/// Source errors propagate. Build validates and hashes the actual owned result,
/// refusing source drift without returning an unchecked descriptor.
pub trait ClaimSource<'a> {
    type Relations<'s>: Iterator<Item = Result<Relation, ContractError>>
    where
        Self: 's;
    type Scopes<'s>: Iterator<Item = Result<ScopeSpec<'a>, ContractError>>
    where
        Self: 's;
    type Requirements<'s>: Iterator<Item = Result<RequirementRef, ContractError>>
    where
        Self: 's;
    type Slot<'s>: ClaimSlotSource
    where
        Self: 's;
    type Slots<'s>: Iterator<Item = Result<Self::Slot<'s>, ContractError>>
    where
        Self: 's;
    fn fields(&self) -> ClaimFields<'a>;
    fn relation_count(&self) -> usize;
    fn scope_count(&self) -> usize;
    fn requirement_count(&self) -> usize;
    fn slot_count(&self) -> usize;
    fn relations(&self) -> Self::Relations<'_>;
    fn scopes(&self) -> Self::Scopes<'_>;
    fn requirements(&self) -> Self::Requirements<'_>;
    fn slots(&self) -> Self::Slots<'_>;
}

#[derive(Debug)]
pub struct ClaimSourcePlan<'s, 'a, S: ClaimSource<'a>> {
    source: &'s S,
    fields: ClaimFields<'a>,
    limits: Limits,
    shape: Shape,
}

impl<'s, 'a, S: ClaimSource<'a>> ClaimSourcePlan<'s, 'a, S> {
    pub fn prepare(
        source: &'s S,
        limits: Limits,
        max_visits: usize,
    ) -> Result<Self, ContractError> {
        let (fields, shape) = inspect(source, limits, max_visits)?;
        Ok(Self {
            source,
            fields,
            limits,
            shape,
        })
    }
    pub fn fields(&self) -> ClaimFields<'a> {
        self.fields
    }
    pub fn issuer(&self) -> ParticipantId {
        self.shape.roles.issuer
    }
    pub fn subject(&self) -> ParticipantId {
        self.shape.roles.subject
    }
    pub fn action(&self) -> ActionType {
        self.shape.roles.action
    }
    pub fn cause(&self) -> &Cause {
        &self.shape.roles.cause
    }
    pub fn check_postable(&self) -> Result<(), ContractError> {
        self.shape.roles.check_postable()
    }
    pub fn content_hash(&self) -> ContentHash {
        self.shape.content_hash
    }
    pub fn intent_fingerprint(&self) -> ContentHash {
        identity::intent(self.fields.id, self.shape.content_hash)
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
    pub fn inspection_visits(&self) -> usize {
        self.shape.inspection_visits
    }
    /// Model work for one source copy, actual-owned validation/hash, and final
    /// capacity reconciliation. Adapter parsing is a separate caller allowance.
    pub fn build_visits(&self) -> usize {
        self.shape.build_visits
    }
    pub fn build(
        self,
        max_bytes: usize,
        max_visits: usize,
    ) -> Result<ClaimDescriptor, ContractError> {
        build(
            self.source,
            self.fields,
            self.limits,
            self.shape,
            max_bytes,
            max_visits,
        )
    }
}

impl ClaimDescriptor {
    pub fn prepare_source<'s, 'a, S: ClaimSource<'a>>(
        source: &'s S,
        limits: Limits,
        max_visits: usize,
    ) -> Result<ClaimSourcePlan<'s, 'a, S>, ContractError> {
        ClaimSourcePlan::prepare(source, limits, max_visits)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Counts {
    relations: usize,
    scopes: usize,
    requirements: usize,
    slots: usize,
}
impl Counts {
    fn read<'a>(
        source: &impl ClaimSource<'a>,
        limits: Limits,
        visits: &mut VisitBudget,
    ) -> Result<Self, ContractError> {
        visits.charge(4)?;
        let value = Self {
            relations: source.relation_count(),
            scopes: source.scope_count(),
            requirements: source.requirement_count(),
            slots: source.slot_count(),
        };
        if value.relations > limits.relations
            || value.scopes > limits.scopes
            || value.requirements > limits.requirements
            || value.slots > limits.slots
        {
            return Err(ContractError::Capacity);
        }
        Ok(value)
    }
    fn heap(self, description: usize) -> Result<usize, ContractError> {
        let mut heap = description;
        heap = bytes::add(heap, bytes::array::<Relation>(self.relations)?)?;
        heap = bytes::add(heap, bytes::array::<AuthoredScope>(self.scopes)?)?;
        heap = bytes::add(heap, bytes::array::<RequirementRef>(self.requirements)?)?;
        bytes::add(heap, bytes::array::<AuthoredSlot>(self.slots)?)
    }
    fn allocations(self, description: usize) -> Result<usize, ContractError> {
        [
            description,
            self.relations,
            self.scopes,
            self.requirements,
            self.slots,
        ]
        .into_iter()
        .try_fold(0, |sum, count| bytes::add(sum, usize::from(count != 0)))
    }
}

#[derive(Debug)]
pub(super) struct Shape {
    counts: Counts,
    pub(super) roles: Roles,
    pub(super) heap: usize,
    pub(super) allocations: usize,
    pub(super) charge: usize,
    pub(super) content_hash: ContentHash,
    inspection_visits: usize,
    build_visits: usize,
}

// Each callback is separately debited. Constants cover scalar validation and
// maximum fixed-size identity framing: a relation has at most 184 hashed bytes,
// a pin 80, a slot header 86 and a check 70. String work is charged by byte length.
const FIELDS: usize = 512;
const RELATION: usize = 256;
const SCOPE: usize = 64;
const REQUIREMENT: usize = 96;
const SLOT: usize = 128;
const CHECK: usize = 128;

fn scaled(count: usize, width: usize) -> Result<usize, ContractError> {
    count.checked_mul(width).ok_or(ContractError::Capacity)
}
fn next<T>(
    values: &mut impl Iterator<Item = Result<T, ContractError>>,
    visits: &mut VisitBudget,
) -> Result<T, ContractError> {
    visits.charge(1)?;
    values.next().ok_or(ContractError::InvalidManifest)?
}
fn end<T>(
    values: &mut impl Iterator<Item = Result<T, ContractError>>,
    visits: &mut VisitBudget,
) -> Result<(), ContractError> {
    visits.charge(1)?;
    match values.next() {
        None => Ok(()),
        Some(Err(error)) => Err(error),
        Some(Ok(_)) => Err(ContractError::InvalidManifest),
    }
}
fn slot_fields(
    slot: &impl ClaimSlotSource,
    limits: Limits,
    visits: &mut VisitBudget,
) -> Result<ClaimSlotFields, ContractError> {
    visits.charge(SLOT)?;
    let fields = slot.fields();
    if fields.checks > limits.checks {
        return Err(ContractError::Capacity);
    }
    Ok(fields)
}
