//! Complete authored creation is a separate, immutable in-process profile.
//! No codec, execution authority or upgrade of an existing root is implied.
use super::prepare::{ALLOCATION, Extras, Scratch, add, array, within};
use super::*;
use focal_model::lifecycle::{
    authored_creation::{self, AuthoredCreationPlan},
    claim::ClaimCut,
    claim_descriptor::ClaimDescriptor,
    creation::{self, Owner},
    graph::VisitBudget,
    validation_descriptor::ValidationDescriptor,
};
use focal_model::{ObjectId, RelationTarget};

#[path = "authored_check.rs"]
mod checks;
#[path = "authored_prepare.rs"]
mod creation_work;
pub(super) use checks::{check_plan, check_storage};
pub(super) use checks::{
    declaration_pair as check_recorded_declaration, pair as check_recorded_claim,
};
pub(super) use creation_work::prepare;

#[cfg(test)]
#[path = "authored_tests.rs"]
mod tests;
#[cfg(test)]
pub(super) use tests::{recovery_fixture, replay_fixture};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeContentProfile {
    ProjectionOnly,
    AuthoredV1,
}

#[derive(Debug)]
pub struct NativeAuthoredProposal {
    pub content: ClaimDescriptor,
    pub declarations: Vec<ValidationDescriptor>,
    pub max_responses: u32,
    pub scope_limits: scope::ScopeLimits,
    pub owner: Option<Owner>,
}

pub(super) struct Proof {
    fingerprint: ContentHash,
    prefix: SessionSeq,
    request: RequestKey,
    claims: usize,
    definitions: usize,
    objects: usize,
}

pub(super) fn check_profile(
    profile: NativeContentProfile,
    command: &NativeCommand,
) -> Result<(), NativeError> {
    if matches!(
        (profile, command),
        (
            NativeContentProfile::ProjectionOnly,
            NativeCommand::CreateAuthored { .. }
        ) | (
            NativeContentProfile::AuthoredV1,
            NativeCommand::Create { .. }
        )
    ) {
        return Err(ContractError::InvalidPolicy.into());
    }
    Ok(())
}

fn nested(bytes: usize, allocations: usize) -> Result<usize, NativeError> {
    add(
        bytes,
        allocations
            .checked_mul(ALLOCATION)
            .ok_or(NativeError::Capacity("authored heaps"))?,
    )
}

pub(super) fn bound_input(
    claims: &[NativeAuthoredProposal],
    capacity: usize,
    limits: NativeLimits,
) -> Result<(), NativeError> {
    if claims.is_empty() {
        return Err(ContractError::InvalidPolicy.into());
    }
    within(claims.len(), limits.plan_nodes)?;
    let mut charge = array::<NativeAuthoredProposal>(capacity)?;
    let mut objects = claims.len();
    for claim in claims {
        charge = add(
            charge,
            nested(
                claim.content.retained_heap_bytes()?,
                claim.content.heap_allocations()?,
            )?,
        )?;
        charge = add(
            charge,
            array::<ValidationDescriptor>(claim.declarations.capacity())?,
        )?;
        objects = add(objects, claim.declarations.len())?;
        within(objects, limits.range.max_batch_entries)?;
        for declaration in &claim.declarations {
            charge = add(
                charge,
                nested(
                    declaration.retained_heap_bytes()?,
                    declaration.heap_allocations()?,
                )?,
            )?;
        }
        within(charge, limits.preparation_bytes)?;
    }
    Ok(())
}

fn count(hash: &mut blake3::Hasher, value: usize) -> Result<(), NativeError> {
    hash.update(
        &u64::try_from(value)
            .map_err(|_| NativeError::Capacity("authored count"))?
            .to_le_bytes(),
    );
    Ok(())
}

pub(super) fn fingerprint(claims: &[NativeAuthoredProposal]) -> Result<ContentHash, NativeError> {
    let mut hash = fingerprint_begin(claims.len())?;
    for proposal in claims {
        fingerprint_claim(
            &mut hash,
            proposal.content.intent_fingerprint(),
            proposal.declarations.len(),
        )?;
        for descriptor in &proposal.declarations {
            fingerprint_declaration(&mut hash, descriptor.intent_fingerprint());
        }
        fingerprint_profile(
            &mut hash,
            proposal.max_responses,
            proposal.scope_limits,
            proposal.owner,
        )?;
    }
    Ok(ContentHash(*hash.finalize().as_bytes()))
}

pub(super) fn fingerprint_begin(claims: usize) -> Result<blake3::Hasher, NativeError> {
    let mut hash = blake3::Hasher::new();
    hash.update(b"focal/native/authored-creation-intent/1");
    count(&mut hash, claims)?;
    Ok(hash)
}
pub(super) fn fingerprint_claim(
    hash: &mut blake3::Hasher,
    claim: ContentHash,
    declarations: usize,
) -> Result<(), NativeError> {
    hash.update(&claim.0);
    count(hash, declarations)
}
pub(super) fn fingerprint_declaration(hash: &mut blake3::Hasher, declaration: ContentHash) {
    hash.update(&declaration.0);
}
pub(super) fn fingerprint_profile(
    hash: &mut blake3::Hasher,
    max_responses: u32,
    scope: scope::ScopeLimits,
    owner: Option<Owner>,
) -> Result<(), NativeError> {
    hash.update(&max_responses.to_le_bytes());
    count(hash, scope.scopes)?;
    count(hash, scope.roots)?;
    count(hash, scope.children)?;
    match owner {
        None => {
            hash.update(&[0]);
        }
        Some(owner) => {
            hash.update(&[1]);
            super::intent::hash_binding(hash, owner.expected);
            super::intent::hash_optional_receipt(hash, owner.receipt);
        }
    }
    Ok(())
}

fn limits(limits: NativeLimits, visits: usize, bytes: usize) -> authored_creation::Limits {
    authored_creation::Limits {
        relations: limits.plan_edges,
        requirements: limits.definitions.min(limits.range.max_batch_entries),
        slots: limits.work_artifacts_per_cycle,
        checks: limits.evaluations_per_claim,
        visits,
        bytes,
    }
}

pub(super) fn check_postable(view: &View<'_>, claim: &ClaimState) -> Result<(), NativeError> {
    if view.state.profile == NativeContentProfile::ProjectionOnly {
        return Ok(());
    }
    let descriptor = super::authored_reads::content(
        view.get(Key::ClaimContent(ClaimId(claim.binding().object.0))),
    )
    .ok_or(ContractError::InvalidPolicy)?;
    descriptor.check_postable()?;
    // No authenticated transfer capability exists in this owner profile yet.
    // A Handoff label by itself cannot authorize self-issued work.
    if descriptor.issuer() == descriptor.subject() {
        return Err(ContractError::InvalidPolicy.into());
    }
    Ok(())
}

fn same_identity(left: Binding, right: Binding) -> bool {
    left.ledger == right.ledger && left.object == right.object && left.content == right.content
}

/// A private preparation capability binds the exact moved bodies, responsibility
/// preconditions and ordered returned mapping. Content hashes are cached only by
/// checked immutable descriptor types; this does not rehash text during writes.
fn retained_fingerprint(extras: &Extras) -> Result<ContentHash, NativeError> {
    let mut hash = blake3::Hasher::new();
    hash.update(b"focal/native/authored-retained/1");
    for extra in &extras.rows {
        match &extra.row {
            Row::ClaimContent(row) => {
                hash.update(&[0]);
                let body = row.get().ok_or(ContractError::InvalidPolicy)?;
                let profile = row.profile().ok_or(ContractError::InvalidPolicy)?;
                hash.update(&body.intent_fingerprint().0);
                hash.update(&profile.max_responses.to_le_bytes());
                count(&mut hash, profile.scope_limits.scopes)?;
                count(&mut hash, profile.scope_limits.roots)?;
                count(&mut hash, profile.scope_limits.children)?;
                match profile.owner {
                    None => {
                        hash.update(&[0]);
                    }
                    Some(owner) => {
                        hash.update(&[1]);
                        super::intent::hash_binding(&mut hash, owner.expected);
                        super::intent::hash_optional_receipt(&mut hash, owner.receipt);
                    }
                }
            }
            Row::Definition(row) => {
                if let Some(body) = row.descriptor() {
                    hash.update(&[1]);
                    hash.update(&body.intent_fingerprint().0);
                }
            }
            Row::CreationResult(row) => {
                hash.update(&[2]);
                count(&mut hash, row.get().entries().len())?;
                for object in row.get().entries() {
                    hash.update(&object.ordinal.to_le_bytes());
                    hash.update(&[match object.family {
                        NativeCreatedFamily::Claim => 0,
                        NativeCreatedFamily::Validation => 1,
                    }]);
                    hash.update(&object.schema.to_le_bytes());
                    hash.update(&object.content.0);
                    hash.update(&object.requested.0);
                    hash.update(&object.resolved.0);
                }
            }
            _ => {}
        }
    }
    Ok(ContentHash(*hash.finalize().as_bytes()))
}
