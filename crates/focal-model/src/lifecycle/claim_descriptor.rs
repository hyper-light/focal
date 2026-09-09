//! Standalone native authored claim content, without lifecycle or authority.
//!
//! Schema 1 here is an internal descriptor contract, not a persisted successor
//! schema. Existing V1 content, native ClaimDefinition and their hashes are
//! unchanged. The owner must still verify reference existence, permissions,
//! complete policy projection, deduplication and retention before publication.
//! The initial supported relation profile follows current claim admission;
//! evaluator/contributor relations and rank-dependent Invalidates are refused.
//! Runtime response and scope allowances remain owner admission/profile values;
//! they are not inferred from this authored message.
use super::aggregation::{CheckPolicy, SlotPolicy};
use super::{Binding, ContractError, memory as bytes};
use crate::{
    ActionType, Cause, ClaimId, ContentHash, Deadline, LedgerId, ObjectId, ObjectKind,
    ObjectRevision, OccurrenceId, ParticipantId, PeerPolicy, Relation, RelationKind,
    RelationTarget, RequirementRef, ScopeKind, ValidationMode,
};

#[path = "claim_descriptor_hash.rs"]
mod identity;
#[path = "claim_source.rs"]
mod source;
pub use source::{ClaimFields, ClaimSlotFields, ClaimSlotSource, ClaimSource, ClaimSourcePlan};

/// Authored subject scope. This is not a runtime monitor or ownership scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ScopeSpec<'a> {
    pub kind: ScopeKind,
    pub key: &'a str,
}

/// Complete borrowed authored body. Relations and scopes are strictly sorted
/// sets; requirement order is authored and remains significant.
#[derive(Debug, Clone, Copy)]
pub struct ClaimSpec<'a> {
    pub ledger: LedgerId,
    pub id: ClaimId,
    pub schema: u16,
    pub occurrence: OccurrenceId,
    pub description: &'a str,
    pub relations: &'a [Relation],
    pub scopes: &'a [ScopeSpec<'a>],
    pub requirements: &'a [RequirementRef],
    pub slots: &'a [SlotPolicy<'a>],
    pub deadline: Option<Deadline>,
    /// Schema 2 only: the immutable follow-up policy.
    pub policy: Option<PeerPolicy>,
}

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub description_bytes: usize,
    pub relations: usize,
    pub scopes: usize,
    pub scope_key_bytes: usize,
    pub requirements: usize,
    pub slots: usize,
    pub checks: usize,
    pub construction_bytes: usize,
}

#[derive(Debug, PartialEq, Eq)]
struct AuthoredScope {
    kind: ScopeKind,
    key: String,
}

#[derive(Debug, PartialEq, Eq)]
struct AuthoredSlot {
    slot: u32,
    missing_declaration_index: u32,
    mode: ValidationMode,
    checks: Vec<CheckPolicy>,
}

#[derive(Debug, PartialEq, Eq)]
struct Roles {
    issuer: ParticipantId,
    subject: ParticipantId,
    action: ActionType,
    cause: Cause,
}

impl Roles {
    fn copy(&self) -> Self {
        Self {
            issuer: self.issuer,
            subject: self.subject,
            action: self.action,
            cause: match &self.cause {
                Cause::Root(root) => Cause::Root(*root),
                Cause::Claim(claim) => Cause::Claim(*claim),
            },
        }
    }

    fn check_postable(&self) -> Result<(), ContractError> {
        if self.issuer == self.subject && self.action != ActionType::Handoff {
            return Err(ContractError::InvalidPolicy);
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct ClaimDescriptor {
    ledger: LedgerId,
    id: ClaimId,
    schema: u16,
    occurrence: OccurrenceId,
    description: String,
    relations: Vec<Relation>,
    scopes: Vec<AuthoredScope>,
    requirements: Vec<RequirementRef>,
    slots: Vec<AuthoredSlot>,
    deadline: Option<Deadline>,
    policy: Option<PeerPolicy>,
    roles: Roles,
    content_hash: ContentHash,
}

#[derive(Debug)]
pub struct ClaimPlan<'a> {
    spec: ClaimSpec<'a>,
    limits: Limits,
    shape: source::Shape,
}

fn text(value: &str, maximum: usize) -> Result<(), ContractError> {
    if value.len() > maximum {
        return Err(ContractError::Capacity);
    }
    if value.trim().is_empty() || value.contains('\0') {
        return Err(ContractError::InvalidManifest);
    }
    Ok(())
}

#[derive(Default)]
struct RoleBuilder {
    issuer: Option<ParticipantId>,
    subject: Option<ParticipantId>,
    action: Option<ActionType>,
    cause: Option<Cause>,
}
impl RoleBuilder {
    fn relation(
        &mut self,
        ledger: LedgerId,
        id: ClaimId,
        relation: &Relation,
    ) -> Result<(), ContractError> {
        match (&relation.kind, &relation.target) {
            (RelationKind::Issuer, RelationTarget::Participant(value)) => {
                if value.is_zero() || self.issuer.replace(*value).is_some() {
                    return Err(ContractError::InvalidTarget);
                }
            }
            (RelationKind::Subject, RelationTarget::Participant(value)) => {
                if value.is_zero() || self.subject.replace(*value).is_some() {
                    return Err(ContractError::InvalidTarget);
                }
            }
            (RelationKind::ClaimAction, RelationTarget::Action(value)) => {
                if self.action.replace(*value).is_some() {
                    return Err(ContractError::InvalidTarget);
                }
            }
            (RelationKind::CausedBy, RelationTarget::Root(value)) => {
                if value.is_zero() || self.cause.replace(Cause::Root(*value)).is_some() {
                    return Err(ContractError::InvalidTarget);
                }
            }
            // Exact evidence: the artifact a challenge disputes or a
            // correction cites, at its committed descriptor hash.
            (
                RelationKind::Reviews | RelationKind::DerivedFrom,
                RelationTarget::Evidence(value),
            ) => {
                if value.id.is_zero() || value.hash.0 == [0; 32] {
                    return Err(ContractError::InvalidTarget);
                }
            }
            (
                RelationKind::DependsOn
                | RelationKind::Awaits
                | RelationKind::CausedBy
                | RelationKind::Supersedes
                | RelationKind::Amends
                | RelationKind::Refines
                | RelationKind::ConflictsWith
                | RelationKind::DerivedFrom
                | RelationKind::Reviews
                | RelationKind::Invalidates,
                RelationTarget::Object(value),
            ) => {
                if value.ledger != ledger {
                    return Err(ContractError::WrongLedger);
                }
                if value.kind != ObjectKind::Claim || value.id.is_zero() {
                    return Err(ContractError::InvalidTarget);
                }
                if value.id.0 == id.0
                    && matches!(
                        relation.kind,
                        RelationKind::CausedBy
                            | RelationKind::Supersedes
                            | RelationKind::Amends
                            | RelationKind::Invalidates
                    )
                {
                    return Err(ContractError::InvalidTarget);
                }
                if relation.kind == RelationKind::CausedBy
                    && self
                        .cause
                        .replace(Cause::Claim(ClaimId(value.id.0)))
                        .is_some()
                {
                    return Err(ContractError::InvalidTarget);
                }
            }
            _ => return Err(ContractError::InvalidTarget),
        }
        Ok(())
    }
    fn finish(self) -> Result<Roles, ContractError> {
        Ok(Roles {
            issuer: self.issuer.ok_or(ContractError::InvalidTarget)?,
            subject: self.subject.ok_or(ContractError::InvalidTarget)?,
            action: self.action.ok_or(ContractError::InvalidTarget)?,
            cause: self.cause.ok_or(ContractError::InvalidTarget)?,
        })
    }
}

fn reserve<T>(count: usize) -> Result<Vec<T>, ContractError> {
    let values = bytes::reserve::<T>(count)?;
    if size_of::<T>() != 0 && values.capacity() != count {
        return Err(ContractError::Capacity);
    }
    Ok(values)
}

fn string(value: &str) -> Result<String, ContractError> {
    let mut owned = reserve(value.len())?;
    owned.extend_from_slice(value.as_bytes());
    String::from_utf8(owned).map_err(|_| ContractError::InvalidManifest)
}

fn copy_relation(relation: &Relation) -> Relation {
    Relation {
        kind: relation.kind,
        target: match &relation.target {
            RelationTarget::Participant(value) => RelationTarget::Participant(*value),
            RelationTarget::Object(value) => RelationTarget::Object(*value),
            RelationTarget::Action(value) => RelationTarget::Action(*value),
            RelationTarget::Root(value) => RelationTarget::Root(*value),
            RelationTarget::Evidence(value) => RelationTarget::Evidence(*value),
        },
    }
}

impl ClaimDescriptor {
    /// Validate local structure and quote exact requested capacities without
    /// allocation. Requirements name pinned definitions; their actual content
    /// and complete acceptance correspondence remain an owner responsibility.
    pub fn prepare(spec: ClaimSpec<'_>, limits: Limits) -> Result<ClaimPlan<'_>, ContractError> {
        let (_, shape) = source::inspect(&spec, limits, usize::MAX)?;
        Ok(ClaimPlan {
            spec,
            limits,
            shape,
        })
    }

    pub fn ledger(&self) -> LedgerId {
        self.ledger
    }
    pub fn id(&self) -> ClaimId {
        self.id
    }
    pub fn schema(&self) -> u16 {
        self.schema
    }
    pub fn occurrence(&self) -> OccurrenceId {
        self.occurrence
    }
    /// The follow-up policy of a schema-2 claim.
    pub fn policy(&self) -> Option<PeerPolicy> {
        self.policy
    }
    pub fn description(&self) -> &str {
        &self.description
    }
    pub fn relations(&self) -> &[Relation] {
        &self.relations
    }
    pub fn scopes(&self) -> impl ExactSizeIterator<Item = ScopeSpec<'_>> + DoubleEndedIterator {
        self.scopes.iter().map(|scope| ScopeSpec {
            kind: scope.kind,
            key: &scope.key,
        })
    }
    pub fn requirements(&self) -> &[RequirementRef] {
        &self.requirements
    }
    /// Complete authored slot contract, including zero-check presence slots.
    /// Declaration correspondence and required Delivery remain owner checks.
    pub fn slots(
        &self,
    ) -> impl ExactSizeIterator<Item = SlotPolicy<'_>> + DoubleEndedIterator + Clone {
        self.slots.iter().map(|slot| SlotPolicy {
            slot: slot.slot,
            missing_declaration_index: slot.missing_declaration_index,
            mode: slot.mode,
            checks: &slot.checks,
        })
    }
    pub fn deadline(&self) -> Option<Deadline> {
        self.deadline
    }
    pub fn issuer(&self) -> ParticipantId {
        self.roles.issuer
    }
    pub fn subject(&self) -> ParticipantId {
        self.roles.subject
    }
    pub fn action(&self) -> ActionType {
        self.roles.action
    }
    pub fn cause(&self) -> &Cause {
        &self.roles.cause
    }
    /// Structural self-targeting rule only. This does not establish legitimate
    /// handoff, issuer authority, posting standing or lifecycle readiness.
    pub fn check_postable(&self) -> Result<(), ContractError> {
        self.roles.check_postable()
    }
    pub fn content_hash(&self) -> ContentHash {
        self.content_hash
    }
    pub fn intent_fingerprint(&self) -> ContentHash {
        identity::intent(self.id, self.content_hash)
    }
    pub fn binding(&self) -> Binding {
        Binding {
            ledger: self.ledger,
            object: ObjectId(self.id.0),
            content: self.content_hash,
            revision: ObjectRevision(1),
        }
    }

    pub fn copy_heap_bytes(&self) -> Result<usize, ContractError> {
        self.heap_bytes(false)
    }
    pub fn retained_heap_bytes(&self) -> Result<usize, ContractError> {
        self.heap_bytes(true)
    }
    fn heap_bytes(&self, retained: bool) -> Result<usize, ContractError> {
        let capacity = |length: usize, capacity: usize| if retained { capacity } else { length };
        let mut heap = bytes::add(
            capacity(self.description.len(), self.description.capacity()),
            bytes::add(
                bytes::array::<Relation>(capacity(
                    self.relations.len(),
                    self.relations.capacity(),
                ))?,
                bytes::add(
                    bytes::array::<AuthoredScope>(capacity(
                        self.scopes.len(),
                        self.scopes.capacity(),
                    ))?,
                    bytes::array::<RequirementRef>(capacity(
                        self.requirements.len(),
                        self.requirements.capacity(),
                    ))?,
                )?,
            )?,
        )?;
        heap = bytes::add(
            heap,
            bytes::array::<AuthoredSlot>(capacity(self.slots.len(), self.slots.capacity()))?,
        )?;
        for scope in &self.scopes {
            heap = bytes::add(heap, capacity(scope.key.len(), scope.key.capacity()))?;
        }
        for slot in &self.slots {
            heap = bytes::add(
                heap,
                bytes::array::<CheckPolicy>(capacity(slot.checks.len(), slot.checks.capacity()))?,
            )?;
        }
        Ok(heap)
    }
    pub fn copy_heap_allocations(&self) -> Result<usize, ContractError> {
        self.allocations(false)
    }
    pub fn heap_allocations(&self) -> Result<usize, ContractError> {
        self.allocations(true)
    }
    fn allocations(&self, retained: bool) -> Result<usize, ContractError> {
        let capacity = |length: usize, capacity: usize| if retained { capacity } else { length };
        let mut count = 0;
        for value in [
            capacity(self.description.len(), self.description.capacity()),
            capacity(self.relations.len(), self.relations.capacity()),
            capacity(self.scopes.len(), self.scopes.capacity()),
            capacity(self.requirements.len(), self.requirements.capacity()),
            capacity(self.slots.len(), self.slots.capacity()),
        ] {
            count = bytes::add(count, usize::from(value != 0))?;
        }
        for scope in &self.scopes {
            count = bytes::add(
                count,
                bytes::allocation::<u8>(capacity(scope.key.len(), scope.key.capacity())),
            )?;
        }
        for slot in &self.slots {
            count = bytes::add(
                count,
                bytes::allocation::<CheckPolicy>(capacity(
                    slot.checks.len(),
                    slot.checks.capacity(),
                )),
            )?;
        }
        Ok(count)
    }
    pub fn copy_charge(&self) -> Result<usize, ContractError> {
        bytes::total::<Self>(self.copy_heap_bytes()?)
    }
    pub fn retained_bytes(&self) -> Result<usize, ContractError> {
        bytes::total::<Self>(self.retained_heap_bytes()?)
    }
    pub fn try_copy(&self, max_bytes: usize) -> Result<Self, ContractError> {
        bytes::fits(self.copy_charge()?, max_bytes)?;
        let description = string(&self.description)?;
        let mut relations = reserve(self.relations.len())?;
        relations.extend(self.relations.iter().map(copy_relation));
        let mut scopes = reserve(self.scopes.len())?;
        for scope in &self.scopes {
            scopes.push(AuthoredScope {
                kind: scope.kind,
                key: string(&scope.key)?,
            });
        }
        let mut requirements = reserve(self.requirements.len())?;
        requirements.extend_from_slice(&self.requirements);
        let mut slots = reserve(self.slots.len())?;
        for slot in &self.slots {
            let mut checks = reserve(slot.checks.len())?;
            checks.extend_from_slice(&slot.checks);
            slots.push(AuthoredSlot {
                slot: slot.slot,
                missing_declaration_index: slot.missing_declaration_index,
                mode: slot.mode,
                checks,
            });
        }
        let copied = Self {
            ledger: self.ledger,
            id: self.id,
            schema: self.schema,
            occurrence: self.occurrence,
            description,
            relations,
            scopes,
            requirements,
            slots,
            deadline: self.deadline,
            policy: self.policy,
            roles: self.roles.copy(),
            content_hash: self.content_hash,
        };
        bytes::fits(copied.retained_bytes()?, max_bytes)?;
        Ok(copied)
    }
}

impl ClaimPlan<'_> {
    /// Inline descriptor plus final heap capacities, excluding allocator metadata.
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
    pub fn intent_fingerprint(&self) -> ContentHash {
        identity::intent(self.spec.id, self.shape.content_hash)
    }
    pub fn check_postable(&self) -> Result<(), ContractError> {
        self.shape.roles.check_postable()
    }
    pub fn build(self, max_bytes: usize) -> Result<ClaimDescriptor, ContractError> {
        source::build(
            &self.spec,
            self.spec.fields(),
            self.limits,
            self.shape,
            max_bytes,
            usize::MAX,
        )
    }
}

#[cfg(test)]
#[path = "claim_descriptor_tests.rs"]
mod tests;
