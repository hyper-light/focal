//! Checked projection of complete immutable authored inputs into creation.
//!
//! This does not retain the descriptors, resolve relation endpoints or publish
//! anything. The owner keeps the original descriptors alive and funded, then
//! moves them into immutable storage after consuming the projection. The normal
//! creation::CreationPlan remains mandatory for authority, lineage and cuts.
use super::{
    ContractError, Principal, aggregation,
    claim::ClaimDefinition,
    claim_descriptor::ClaimDescriptor,
    graph::{self, VisitBudget},
    memory as bytes,
    scope::ScopeLimits,
    succession::{Correction, CorrectionKind, Lineage},
    validation::Declaration,
    validation_descriptor::ValidationDescriptor,
};
use crate::{Cause, ClaimId, ObjectRevision, RelationKind, RelationTarget, SessionSeq};

const ALLOCATION: usize = 4 * size_of::<usize>();

/// Values supplied by the effective owner, not inferred from authored text.
#[derive(Debug, Clone, Copy)]
pub struct Profile {
    pub max_responses: u32,
    pub scope_limits: ScopeLimits,
    pub created: SessionSeq,
}

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub relations: usize,
    pub requirements: usize,
    pub slots: usize,
    pub checks: usize,
    pub visits: usize,
    pub bytes: usize,
}
impl Limits {
    fn acceptance(self) -> aggregation::Limits {
        // Requirements and checks have separate outer limits. The shared local
        // acceptance validator uses one maximum for both; runtime result/update
        // bounds are deliberately outside this authored projection.
        aggregation::Limits {
            max_slots: self.slots,
            max_checks: self.requirements.max(self.checks),
            max_results: 1,
            max_updates: 1,
        }
    }
}

#[derive(Debug)]
pub struct AuthoredCreationPlan<'a> {
    claim: &'a ClaimDescriptor,
    descriptors: &'a [ValidationDescriptor],
    profile: Profile,
    limits: Limits,
    graph: usize,
    corrections: usize,
    acceptance: aggregation::SourceShape,
    bytes: usize,
    allocations: usize,
    inspection_visits: usize,
    build_visits: usize,
}

#[derive(Debug)]
pub struct Projection<'a> {
    definition: ClaimDefinition,
    claim: &'a ClaimDescriptor,
    descriptors: &'a [ValidationDescriptor],
    visits: usize,
}
impl<'a> Projection<'a> {
    pub fn definition(&self) -> &ClaimDefinition {
        &self.definition
    }
    pub fn claim_descriptor(&self) -> &'a ClaimDescriptor {
        self.claim
    }
    pub fn descriptors(&self) -> &'a [ValidationDescriptor] {
        self.descriptors
    }
    pub fn declarations(&self) -> impl ExactSizeIterator<Item = &'a Declaration> + Clone {
        self.descriptors
            .iter()
            .map(ValidationDescriptor::declaration)
    }
    pub fn visits(&self) -> usize {
        self.visits
    }
    /// Ends every descriptor borrow; the caller can move the exact original
    /// immutable descriptors into storage without cloning declaration handlers.
    pub fn into_definition(self) -> ClaimDefinition {
        self.definition
    }
}

fn multiply(left: usize, right: usize) -> Result<usize, ContractError> {
    left.checked_mul(right).ok_or(ContractError::Capacity)
}
fn array<T>(count: usize) -> Result<usize, ContractError> {
    bytes::add(
        bytes::array::<T>(count)?,
        multiply(bytes::allocation::<T>(count), ALLOCATION)?,
    )
}
fn reserve<T>(count: usize, visits: &mut VisitBudget) -> Result<Vec<T>, ContractError> {
    visits.charge(1)?;
    let values = bytes::reserve::<T>(count)?;
    if values.capacity() != count {
        return Err(ContractError::Capacity);
    }
    Ok(values)
}

impl<'a> AuthoredCreationPlan<'a> {
    pub fn prepare(
        principal: Principal,
        claim: &'a ClaimDescriptor,
        descriptors: &'a [ValidationDescriptor],
        profile: Profile,
        limits: Limits,
    ) -> Result<Self, ContractError> {
        principal.require_actor(claim.issuer())?;
        if profile.created.0 == 0 {
            return Err(ContractError::InvalidCut);
        }
        if profile.max_responses == 0 {
            return Err(ContractError::Capacity);
        }
        // Reuse the runtime registry's allocation-free profile guard. Zero
        // allowances intentionally represent disabled scopes and children.
        super::scope::Registry::new(claim.binding(), profile.scope_limits)?;
        if claim.relations().len() > limits.relations
            || claim.requirements().len() > limits.requirements
            || claim.slots().len() > limits.slots
        {
            return Err(ContractError::Capacity);
        }
        if descriptors.len() != claim.requirements().len() {
            return Err(ContractError::InvalidPolicy);
        }
        let mut visits = VisitBudget::new(limits.visits);
        visits.charge(1)?;
        let mut checks = 0;
        for slot in claim.slots() {
            visits.charge(1)?;
            checks = bytes::add(checks, slot.checks.len())?;
            bytes::fits(checks, limits.checks)?;
        }
        for (position, descriptor) in descriptors.iter().enumerate() {
            visits.charge(1)?;
            let declaration = descriptor.declaration();
            let binding = descriptor.binding();
            if binding.ledger != claim.ledger() {
                return Err(ContractError::WrongLedger);
            }
            if declaration.claim() != claim.id() || declaration.issuer() != claim.issuer() {
                return Err(ContractError::InvalidTarget);
            }
            if binding.revision != ObjectRevision(1) {
                return Err(ContractError::StaleRevision);
            }
            for previous in descriptors.iter().take(position) {
                visits.charge(1)?;
                if previous.binding().object == binding.object {
                    return Err(ContractError::InvalidPolicy);
                }
            }
        }
        // Authored requirement order is kept intact; source storage order need
        // not match. An exact complete set permits no omitted or extra body.
        for requirement in claim.requirements() {
            visits.charge(1)?;
            let mut found = false;
            for descriptor in descriptors {
                visits.charge(1)?;
                if descriptor.binding().object.0 == requirement.id.0 {
                    if descriptor.specification_hash() != requirement.specification {
                        return Err(ContractError::ContentConflict);
                    }
                    found = true;
                    break;
                }
            }
            if !found {
                return Err(ContractError::InvalidPolicy);
            }
        }
        let (mut graph, mut corrections) = (0, 0);
        for relation in claim.relations() {
            visits.charge(1)?;
            match relation.kind {
                RelationKind::DependsOn | RelationKind::Awaits => graph = bytes::add(graph, 1)?,
                RelationKind::Supersedes | RelationKind::Amends => {
                    corrections = bytes::add(corrections, 1)?
                }
                _ => {}
            }
        }
        let before_acceptance = visits.remaining();
        let acceptance = aggregation::check_sources(
            claim.binding(),
            claim.issuer(),
            claim.slots(),
            descriptors.iter().map(ValidationDescriptor::declaration),
            limits.acceptance(),
            &mut visits,
        )?;
        let acceptance_visits = before_acceptance
            .checked_sub(visits.remaining())
            .ok_or(ContractError::Capacity)?;
        let inspection_visits = limits
            .visits
            .checked_sub(visits.remaining())
            .ok_or(ContractError::Capacity)?;
        // Source order and content are immutable. Repeating check_sources over
        // borrowed arrays therefore has the exact same counted traversal. The
        // other work is four buffer reservations, one lineage scalar guard,
        // two terminal source probes, relation projection, graph/correction
        // validation, temporary fills, and the shared acceptance builder's
        // explicitly derived loop count.
        let mut build_visits = bytes::add(acceptance_visits, acceptance.build_visits)?;
        for count in [
            7,
            claim.relations().len(),
            graph,
            corrections,
            claim.slots().len(),
            descriptors.len(),
        ] {
            build_visits = bytes::add(build_visits, count)?;
        }
        bytes::fits(bytes::add(inspection_visits, build_visits)?, limits.visits)?;
        let mut charge = bytes::add(size_of::<Projection<'_>>(), acceptance.heap)?;
        charge = bytes::add(charge, multiply(acceptance.allocations, ALLOCATION)?)?;
        for value in [
            array::<graph::Obligation>(graph)?,
            array::<Correction>(corrections)?,
            array::<aggregation::SlotPolicy<'_>>(claim.slots().len())?,
            array::<&Declaration>(descriptors.len())?,
        ] {
            charge = bytes::add(charge, value)?;
        }
        bytes::fits(charge, limits.bytes)?;
        let mut allocations = acceptance.allocations;
        for value in [graph, corrections, claim.slots().len(), descriptors.len()] {
            allocations = bytes::add(allocations, usize::from(value != 0))?;
        }
        Ok(Self {
            claim,
            descriptors,
            profile,
            limits,
            graph,
            corrections,
            acceptance,
            bytes: charge,
            allocations,
            inspection_visits,
            build_visits,
        })
    }

    /// Peak construction allowance, including final projection, temporary
    /// borrowed-reference/slot arrays, and allocator bookkeeping. Existing
    /// immutable descriptor storage remains charged to the caller separately.
    pub fn construction_bytes(&self) -> usize {
        self.bytes
    }
    pub fn construction_allocations(&self) -> usize {
        self.allocations
    }
    pub fn inspection_visits(&self) -> usize {
        self.inspection_visits
    }
    pub fn visits(&self) -> Result<usize, ContractError> {
        bytes::add(self.inspection_visits, self.build_visits)
    }

    pub fn build(self, max_bytes: usize) -> Result<Projection<'a>, ContractError> {
        bytes::fits(self.bytes, max_bytes.min(self.limits.bytes))?;
        let mut visits = VisitBudget::new(self.build_visits);
        let mut graph = reserve(self.graph, &mut visits)?;
        let mut corrections = reserve(self.corrections, &mut visits)?;
        for relation in self.claim.relations() {
            visits.charge(1)?;
            match relation.kind {
                RelationKind::DependsOn | RelationKind::Awaits => {
                    let RelationTarget::Object(target) = &relation.target else {
                        return Err(ContractError::InvalidTarget);
                    };
                    graph.push(graph::Obligation {
                        kind: if relation.kind == RelationKind::DependsOn {
                            graph::Kind::DependsOn
                        } else {
                            graph::Kind::Awaits
                        },
                        target: ClaimId(target.id.0),
                    });
                }
                RelationKind::Supersedes | RelationKind::Amends => {
                    let RelationTarget::Object(target) = &relation.target else {
                        return Err(ContractError::InvalidTarget);
                    };
                    corrections.push(Correction {
                        kind: if relation.kind == RelationKind::Supersedes {
                            CorrectionKind::Supersedes
                        } else {
                            CorrectionKind::Amends
                        },
                        predecessor: *target,
                    });
                }
                _ => {}
            }
        }
        let graph = graph::Declaration::from_owned_sorted(graph, self.graph, &mut visits)?;
        let cause = match self.claim.cause() {
            Cause::Root(root) => Cause::Root(*root),
            Cause::Claim(claim) => Cause::Claim(*claim),
        };
        let lineage = Lineage::from_owned_sorted(
            self.claim.binding(),
            cause,
            corrections,
            self.corrections,
            &mut visits,
        )?;
        let mut slots = reserve(self.claim.slots().len(), &mut visits)?;
        for slot in self.claim.slots() {
            visits.charge(1)?;
            slots.push(slot);
        }
        let mut declarations = reserve(self.descriptors.len(), &mut visits)?;
        for descriptor in self.descriptors {
            visits.charge(1)?;
            declarations.push(descriptor.declaration());
        }
        let acceptance = aggregation::AcceptancePolicy::prepare_borrowed(
            self.claim.binding(),
            self.claim.issuer(),
            &slots,
            &declarations,
            self.limits.acceptance(),
            &mut visits,
        )?;
        if acceptance.construction_charge() != self.acceptance.charge
            || acceptance.intent_fingerprint() != self.acceptance.intent
        {
            return Err(ContractError::InvalidPolicy);
        }
        let acceptance = acceptance.build_with_visits(self.acceptance.charge, &mut visits)?;
        let visits = bytes::add(
            self.inspection_visits,
            self.build_visits
                .checked_sub(visits.remaining())
                .ok_or(ContractError::Capacity)?,
        )?;
        Ok(Projection {
            definition: ClaimDefinition {
                binding: self.claim.binding(),
                issuer: self.claim.issuer(),
                subject: self.claim.subject(),
                deadline: self.claim.deadline(),
                max_responses: self.profile.max_responses,
                created: self.profile.created,
                graph,
                lineage,
                acceptance,
                scope_limits: self.profile.scope_limits,
            },
            claim: self.claim,
            descriptors: self.descriptors,
            visits,
        })
    }
}

#[cfg(test)]
#[path = "authored_creation_tests.rs"]
mod tests;
