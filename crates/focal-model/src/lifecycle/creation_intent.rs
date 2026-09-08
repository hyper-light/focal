//! Streaming native creation identity. Shared with owned proposals; no authority.
use super::*;
use crate::lifecycle::{graph, succession::Correction};
use crate::{Deadline, ParticipantId};

pub struct ProposalIntentFields<'a> {
    pub binding: Binding,
    pub issuer: ParticipantId,
    pub subject: ParticipantId,
    pub deadline: Option<Deadline>,
    pub max_responses: u32,
    pub lineage_binding: Binding,
    pub cause: &'a Cause,
    pub acceptance: ContentHash,
    pub scope_limits: scope::ScopeLimits,
    pub owner: Option<Owner>,
}
/// Exact ordered proposal identity. Callers meter value-source parsing and this
/// fixed-field/linear hashing work separately. Failed pushes leave the writer unchanged.
pub struct CreationIntent {
    hash: blake3::Hasher,
    left: usize,
}
impl CreationIntent {
    pub fn new(count: usize) -> Result<Self, ContractError> {
        let mut hash = blake3::Hasher::new();
        hash.update(b"focal/native/creation-intent/1");
        hash_count(&mut hash, count)?;
        Ok(Self { hash, left: count })
    }
    pub fn push(
        &mut self,
        fields: &ProposalIntentFields<'_>,
        obligations_count: usize,
        obligations: impl Iterator<Item = Result<graph::Obligation, ContractError>>,
        corrections_count: usize,
        corrections: impl Iterator<Item = Result<Correction, ContractError>>,
    ) -> Result<(), ContractError> {
        let left = self
            .left
            .checked_sub(1)
            .ok_or(ContractError::InvalidManifest)?;
        let mut candidate = self.hash.clone();
        proposal(
            &mut candidate,
            fields,
            obligations_count,
            obligations,
            corrections_count,
            corrections,
        )?;
        self.hash = candidate;
        self.left = left;
        Ok(())
    }
    pub fn finish(self) -> Result<ContentHash, ContractError> {
        if self.left != 0 {
            return Err(ContractError::InvalidManifest);
        }
        Ok(ContentHash(*self.hash.finalize().as_bytes()))
    }
}
fn terminal<T>(
    values: &mut impl Iterator<Item = Result<T, ContractError>>,
) -> Result<(), ContractError> {
    match values.next() {
        None => Ok(()),
        Some(Err(error)) => Err(error),
        Some(Ok(_)) => Err(ContractError::InvalidManifest),
    }
}
fn proposal(
    hash: &mut blake3::Hasher,
    definition: &ProposalIntentFields<'_>,
    obligations_count: usize,
    mut obligations: impl Iterator<Item = Result<graph::Obligation, ContractError>>,
    corrections_count: usize,
    mut corrections: impl Iterator<Item = Result<Correction, ContractError>>,
) -> Result<(), ContractError> {
    hash_binding(hash, definition.binding, false);
    hash.update(&definition.issuer.0);
    hash.update(&definition.subject.0);
    match definition.deadline {
        Some(deadline) => {
            hash.update(&[1]);
            hash.update(&deadline.timer.0);
            hash.update(&deadline.generation.to_be_bytes());
            hash.update(&deadline.at.to_be_bytes());
        }
        None => {
            hash.update(&[0]);
        }
    }
    hash.update(&definition.max_responses.to_be_bytes());
    hash_count(hash, obligations_count)?;
    for _ in 0..obligations_count {
        let obligation = obligations.next().ok_or(ContractError::InvalidManifest)??;
        hash.update(&[match obligation.kind {
            graph::Kind::DependsOn => 0,
            graph::Kind::Awaits => 1,
        }]);
        hash.update(&obligation.target.0);
    }
    terminal(&mut obligations)?;
    hash_binding(hash, definition.lineage_binding, false);
    match definition.cause {
        Cause::Root(id) => {
            hash.update(&[0]);
            hash.update(&id.0);
        }
        Cause::Claim(id) => {
            hash.update(&[1]);
            hash.update(&id.0);
        }
    }
    hash_count(hash, corrections_count)?;
    for _ in 0..corrections_count {
        let correction = corrections.next().ok_or(ContractError::InvalidManifest)??;
        hash.update(&[match correction.kind {
            CorrectionKind::Supersedes => 0,
            CorrectionKind::Amends => 1,
        }]);
        // Lineage construction already restricts predecessors to Claim.
        hash.update(&correction.predecessor.ledger.tenant.0);
        hash.update(&correction.predecessor.ledger.session.0);
        hash.update(&correction.predecessor.id.0);
    }
    terminal(&mut corrections)?;
    hash.update(&definition.acceptance.0);
    hash_count(hash, definition.scope_limits.scopes)?;
    hash_count(hash, definition.scope_limits.roots)?;
    hash_count(hash, definition.scope_limits.children)?;
    match definition.owner {
        Some(owner) => {
            hash.update(&[1]);
            hash_binding(hash, owner.expected, true);
            match owner.receipt {
                Some(receipt) => {
                    hash.update(&[1]);
                    hash.update(&receipt.receipt.0);
                    hash.update(&receipt.epoch.to_be_bytes());
                }
                None => {
                    hash.update(&[0]);
                }
            }
        }
        None => {
            hash.update(&[0]);
        }
    }
    Ok(())
}

fn hash_binding(hash: &mut blake3::Hasher, binding: Binding, revision: bool) {
    hash.update(&binding.ledger.tenant.0);
    hash.update(&binding.ledger.session.0);
    hash.update(&binding.object.0);
    hash.update(&binding.content.0);
    if revision {
        hash.update(&binding.revision.0.to_be_bytes());
    }
}
fn hash_count(hash: &mut blake3::Hasher, count: usize) -> Result<(), ContractError> {
    hash.update(
        &u64::try_from(count)
            .map_err(|_| ContractError::Capacity)?
            .to_be_bytes(),
    );
    Ok(())
}
