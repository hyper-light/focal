//! Sign only session-owner facts authorized by a committed root assignment.
//! The control owner prepares an opaque permit; the key owner signs it with a
//! borrowed credential. An enrolled Node certificate alone grants no authority.
use focal_control::{ControlRead, ControlReplica, ControlScope};
use focal_directory::{
    AuthorityFact, AuthorityProof, AuthorityStatement, AuthorityVerifier, DirectoryError,
    GroupScope, NodeRecord,
};
use focal_enrollment::{CredentialMaterial, EnrollmentRole, server_fingerprint};
use focal_ledger::CommittedPlacement;
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use focal_model::ContentHash;
use std::collections::BTreeMap;

const WORKSPACE: usize = 1024 * 1024;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProofWindow {
    pub issued_at: i64,
    pub expires_at: i64,
}
#[derive(Debug, thiserror::Error)]
pub enum PlacementProofError {
    #[error("installed authority does not authorize this durable session fact")]
    Unauthorized,
    #[error("placement proof exceeds its bounded allowance")]
    Capacity,
    #[error("placement proof owner is unavailable")]
    Unavailable,
    #[error("control owner: {0}")]
    Control(#[from] focal_control::ControlError),
    #[error("placement authority: {0}")]
    Directory(#[from] DirectoryError),
    #[error("cryptographic dependency failed")]
    Signing,
}
/// No constructor or deserializer accepts a caller-authored fact. This permit
/// owns a bounded statement validated against the actual drained control owner.
pub struct SessionProofPermit {
    statement: AuthorityStatement,
    certificate: [u8; 32],
    _allocation: Allocation,
}
pub struct AccountedAuthorityProof {
    proof: AuthorityProof,
    _allocation: Allocation,
}
impl AccountedAuthorityProof {
    pub fn proof(&self) -> &AuthorityProof {
        &self.proof
    }
}
impl SessionProofPermit {
    pub fn statement(&self) -> &AuthorityStatement {
        &self.statement
    }
    pub fn sign(
        self,
        credential: &CredentialMaterial,
    ) -> Result<AccountedAuthorityProof, PlacementProofError> {
        let certificate = credential
            .certificate_chain()
            .first()
            .ok_or(PlacementProofError::Unauthorized)?;
        if server_fingerprint(certificate) != self.certificate {
            return Err(PlacementProofError::Unauthorized);
        }
        // rcgen/ring are dependency boundaries. The permit is consumed even when
        // signing unwinds; no partially signed statement can escape.
        let signature = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.statement.sign(credential)
        }))
        .map_err(|_| PlacementProofError::Signing)??;
        let mut signatures = Vec::new();
        signatures
            .try_reserve_exact(1)
            .map_err(|_| PlacementProofError::Capacity)?;
        signatures.push(signature);
        let proof = AuthorityProof {
            statement: self.statement,
            signatures,
        };
        let retained = postcard::experimental::serialized_size(&proof)
            .ok()
            .and_then(|bytes| bytes.checked_mul(4))
            .and_then(|bytes| bytes.checked_add(4096))
            .ok_or(PlacementProofError::Capacity)?;
        let mut allocation = self._allocation;
        allocation
            .shrink_to(retained)
            .map_err(|_| PlacementProofError::Capacity)?;
        Ok(AccountedAuthorityProof {
            proof,
            _allocation: allocation,
        })
    }
}
pub fn prepare_session_proof(
    owner: &ControlReplica,
    witness: &CommittedPlacement,
    window: ProofWindow,
    now: i64,
    budget: &MemoryBudget,
) -> Result<SessionProofPermit, PlacementProofError> {
    let allocation = budget
        .reserve(BudgetKind::Control, BudgetLane::Completion, WORKSPACE)
        .map_err(|_| PlacementProofError::Capacity)?
        .commit();
    // A small normal read verifies healthy, completely published owner state.
    // No decoded peer-provided snapshot is accepted at this boundary.
    drop(owner.read_local(&ControlRead::Configuration)?);
    if owner.identity().scope != ControlScope::Root
        || owner.identity().cluster.0 != witness.cluster()
    {
        return Err(PlacementProofError::Unauthorized);
    }
    let authority = owner.authority().ok_or(PlacementProofError::Unauthorized)?;
    let enrollment = owner
        .enrollment()
        .ok_or(PlacementProofError::Unauthorized)?;
    let checkpoint = authority.checkpoint();
    if checkpoint.revision == 0
        || checkpoint.applied_index == 0
        || checkpoint.applied_index > owner.applied_index()
        || enrollment.applied_index() > owner.applied_index()
        || checkpoint.anchor.cluster.0 != owner.identity().cluster.0
    {
        return Err(PlacementProofError::Unauthorized);
    }
    let fence = witness.fence();
    let group = authority
        .group(fence.log_group)
        .ok_or(PlacementProofError::Unauthorized)?;
    let configuration = witness.configuration();
    if group.scope != GroupScope::Session(fence.ledger)
        || group.genesis != witness.genesis()
        || group.membership_epoch != fence.membership_epoch
        || !group
            .voters
            .keys()
            .copied()
            .eq(configuration.voters.iter().copied())
        || !group
            .outgoing_voters
            .keys()
            .copied()
            .eq(configuration.voters_outgoing.iter().copied())
        || !group
            .learners
            .keys()
            .copied()
            .eq(configuration.learners.iter().copied())
        || !configuration.learners_next.is_empty()
        || configuration.auto_leave
        || group.voters != witness.placement().placement.voters
    {
        return Err(PlacementProofError::Unauthorized);
    }
    let node = authority
        .node(witness.node())
        .ok_or(PlacementProofError::Unauthorized)?;
    let generation = node.enrollment.generation;
    if group.voters.get(&witness.node()) != Some(&generation)
        && group.outgoing_voters.get(&witness.node()) != Some(&generation)
    {
        return Err(PlacementProofError::Unauthorized);
    }
    let verifier = authority.verifier(enrollment, &[], now)?;
    verifier.verify_enrollment(&node.enrollment)?;
    if node.expires_at < window.expires_at {
        return Err(PlacementProofError::Unauthorized);
    }
    let enrolled = enrollment
        .enrollments()
        .find(|receipt| receipt.identity.node_id == Some(witness.node()))
        .ok_or(PlacementProofError::Unauthorized)?;
    let identity = enrollment
        .authorize_certificate(&enrolled.certificate, now)
        .map_err(|_| PlacementProofError::Unauthorized)?;
    if identity.role != EnrollmentRole::Node
        || identity.principal != node.principal
        || ContentHash(server_fingerprint(&enrolled.certificate)) != node.enrollment.identity
    {
        return Err(PlacementProofError::Unauthorized);
    }
    let members = witness.placement().placement.nodes();
    // The fixed allowance covers verification/signing scratch. Account the
    // variable topology projection structurally before cloning it; compact
    // node IDs and configured endpoint lengths are not a heap-size bound.
    let projection_bytes = members.iter().try_fold(0usize, |bytes, id| {
        let grant = authority
            .node(*id)
            .ok_or(PlacementProofError::Unauthorized)?;
        size_of::<(u64, NodeRecord)>()
            .checked_mul(3)
            .and_then(|row| row.checked_add(128))
            .and_then(|row| row.checked_add(grant.enrollment.endpoint.len()))
            .and_then(|row| bytes.checked_add(row))
            .ok_or(PlacementProofError::Capacity)
    })?;
    let _projection = budget
        .reserve(
            BudgetKind::Control,
            BudgetLane::Completion,
            projection_bytes,
        )
        .map_err(|_| PlacementProofError::Capacity)?
        .commit();
    let mut nodes = BTreeMap::new();
    for id in members {
        let grant = authority
            .node(id)
            .ok_or(PlacementProofError::Unauthorized)?;
        verifier.verify_enrollment(&grant.enrollment)?;
        if grant.expires_at < window.expires_at {
            return Err(PlacementProofError::Unauthorized);
        }
        nodes.insert(
            id,
            NodeRecord {
                enrollment: grant.enrollment.clone(),
                load: None,
            },
        );
    }
    focal_directory::verify_placement(witness.placement(), &nodes, 127)?;
    let proof = AuthorityProof {
        statement: AuthorityStatement {
            anchor: checkpoint.anchor.clone(),
            authority_revision: checkpoint.revision,
            enrollment_revision: enrollment.revision(),
            group: fence.log_group,
            group_genesis: group.genesis,
            membership_epoch: group.membership_epoch,
            issued_at: window.issued_at,
            expires_at: window.expires_at,
            fact: AuthorityFact::Session(fence.clone()),
        },
        signatures: Vec::new(),
    };
    // Run the consumer's exact context/time checks before issuing a permit.
    // An unsigned share must fail solely because it has no quorum signatures.
    match authority
        .verifier(enrollment, std::slice::from_ref(&proof), now)?
        .verify_session_fence(fence)
    {
        Err(DirectoryError::Quorum) => {}
        Err(error) => return Err(error.into()),
        Ok(()) => return Err(PlacementProofError::Unauthorized),
    }
    Ok(SessionProofPermit {
        statement: proof.statement,
        certificate: server_fingerprint(&enrolled.certificate),
        _allocation: allocation,
    })
}

#[cfg(test)]
#[path = "placement_proof_tests.rs"]
mod tests;
