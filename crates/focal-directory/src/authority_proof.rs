//! Scoped statements from authenticated enrolled peers. An embedding producer
//! signs only after its corresponding log/custody operation is durably committed.
use crate::*;
use focal_enrollment::certificate_key_hash;
use focal_enrollment::{CredentialMaterial, EnrollmentRegistry, SignedNodeStatement};
use focal_model::{ContentHash, RaftIndex, RaftTerm};
use serde::{Deserialize, Serialize};

const MAX_STATEMENT: usize = 16 * 1024;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(
    clippy::large_enum_variant,
    reason = "bounded owned proof; no per-fact allocation"
)]
pub enum AuthorityFact {
    Session(SessionFence),
    /// The attestation field is zero in the signed body. The presented readiness
    /// uses AuthorityProof::attestation(), avoiding a self-referential hash.
    Replica(ReplicaReady),
    DelegationSource(DelegationFence),
    DelegationDestination(DelegationFence),
    Membership {
        next: GroupAuthorityGrant,
        index: RaftIndex,
        term: RaftTerm,
        record_hash: ContentHash,
    },
    /// Self-signed like `Replica`: the attestation field is zero in the body.
    Custody(CustodyProof),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorityStatement {
    pub anchor: AuthorityAnchor,
    pub authority_revision: u64,
    pub enrollment_revision: u64,
    pub group: LogGroupId,
    pub group_genesis: ContentHash,
    pub membership_epoch: u64,
    pub issued_at: i64,
    pub expires_at: i64,
    pub fact: AuthorityFact,
}
impl AuthorityStatement {
    /// This is a signature primitive, not an assertion of commit. Session and
    /// partition owners must construct facts from their actual committed records;
    /// artifact owners must complete durable custody before signing readiness.
    pub fn sign(
        &self,
        credential: &CredentialMaterial,
    ) -> Result<SignedNodeStatement, DirectoryError> {
        let mut buffer = [0_u8; MAX_STATEMENT];
        let bytes = postcard::to_slice(self, &mut buffer).map_err(|_| DirectoryError::Capacity)?;
        credential
            .sign_node_statement(self.anchor.cluster.0, bytes)
            .map_err(|_| DirectoryError::UnverifiedAuthority)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorityProof {
    pub statement: AuthorityStatement,
    pub signatures: Vec<SignedNodeStatement>,
}
impl AuthorityProof {
    pub fn attestation(&self) -> Result<ContentHash, DirectoryError> {
        digest(b"focal.directory.authority-statement.v1", &self.statement)
    }
}
/// A borrowed view of committed assignments and bounded evidence. The host
/// supplies a recorded decision time from its authenticated metadata command.
/// It must install these registries through its own quorum, not from the peer
/// asking to be verified. No network calls or ambient clocks occur here.
pub struct InstalledAuthorityVerifier<'a> {
    registry: &'a AuthorityRegistry,
    enrollment: &'a EnrollmentRegistry,
    proofs: &'a [AuthorityProof],
    now: i64,
}
impl AuthorityRegistry {
    pub fn verifier<'a>(
        &'a self,
        enrollment: &'a EnrollmentRegistry,
        proofs: &'a [AuthorityProof],
        decided_at: i64,
    ) -> Result<InstalledAuthorityVerifier<'a>, DirectoryError> {
        self.validate_enrollment(enrollment)?;
        if decided_at < self.checkpoint().clock || decided_at < 0 {
            return Err(DirectoryError::ClockRegression);
        }
        if proofs.len() > self.config().max_proofs {
            return Err(DirectoryError::Capacity);
        }
        let mut bytes = 0_usize;
        for proof in proofs {
            if proof.signatures.len() > self.config().max_members.saturating_mul(2)
                || postcard::experimental::serialized_size(&proof.statement)
                    .map_err(|_| DirectoryError::Capacity)?
                    > MAX_STATEMENT
            {
                return Err(DirectoryError::Capacity);
            }
            for signature in &proof.signatures {
                if signature.certificate.len() > 4096 || signature.signature.len() > 80 {
                    return Err(DirectoryError::Capacity);
                }
            }
            bytes = add(
                bytes,
                postcard::experimental::serialized_size(proof)
                    .map_err(|_| DirectoryError::Capacity)?,
            )?;
            if bytes > self.config().max_proof_bytes {
                return Err(DirectoryError::Capacity);
            }
        }
        Ok(InstalledAuthorityVerifier {
            registry: self,
            enrollment,
            proofs,
            now: decided_at,
        })
    }
}
impl InstalledAuthorityVerifier<'_> {
    fn group_for(&self, proof: &AuthorityProof) -> Result<&GroupAuthorityGrant, DirectoryError> {
        let statement = &proof.statement;
        if statement.anchor != self.registry.checkpoint().anchor {
            return Err(DirectoryError::WrongCluster);
        }
        if statement.authority_revision != self.registry.revision()
            || statement.enrollment_revision != self.enrollment.revision()
        {
            return Err(DirectoryError::StaleEpoch);
        }
        let lifetime = statement
            .expires_at
            .checked_sub(statement.issued_at)
            .ok_or(DirectoryError::Expired)?;
        if statement.issued_at < self.registry.checkpoint().clock
            || statement.issued_at > self.now
            || statement.expires_at <= self.now
            || lifetime <= 0
            || lifetime > self.registry.config().max_proof_lifetime_seconds
        {
            return Err(DirectoryError::Expired);
        }
        let group = self
            .registry
            .group(statement.group)
            .ok_or(DirectoryError::Missing)?;
        if group.genesis != statement.group_genesis
            || group.membership_epoch != statement.membership_epoch
        {
            return Err(DirectoryError::StaleEpoch);
        }
        if group.expires_at <= self.now || statement.expires_at > group.expires_at {
            return Err(DirectoryError::Expired);
        }
        Ok(group)
    }
    fn signatures(&self, proof: &AuthorityProof, only: Option<u64>) -> Result<(), DirectoryError> {
        let group = self.group_for(proof)?;
        let mut buffer = [0_u8; MAX_STATEMENT];
        let bytes = postcard::to_slice(&proof.statement, &mut buffer)
            .map_err(|_| DirectoryError::Capacity)?;
        let mut incoming = 0_usize;
        let mut outgoing = 0_usize;
        let mut sole = false;
        for (ordinal, signature) in proof.signatures.iter().enumerate() {
            // No heap set is needed. Work is bounded by max_members (<=127).
            if proof
                .signatures
                .iter()
                .take(ordinal)
                .any(|earlier| earlier.certificate == signature.certificate)
            {
                return Err(DirectoryError::Duplicate);
            }
            let identity = self
                .enrollment
                .verify_node_statement(signature, bytes, self.now)
                .map_err(|_| DirectoryError::UnverifiedAuthority)?;
            let node = identity
                .node_id
                .ok_or(DirectoryError::UnverifiedAuthority)?;
            let enrolled = self
                .registry
                .node(node)
                .ok_or(DirectoryError::UnverifiedAuthority)?;
            // The signing certificate is authorized by the registry above; the
            // grant binds the key it carries, so a renewed certificate of the
            // same key keeps signing for the same enrollment.
            let key = certificate_key_hash(&signature.certificate)
                .map_err(|_| DirectoryError::UnverifiedAuthority)?;
            if enrolled.principal != identity.principal
                || enrolled.enrollment.identity != ContentHash(key)
                || !enrolled.enrollment.eligible
                || enrolled.expires_at < proof.statement.expires_at
            {
                return Err(DirectoryError::UnverifiedAuthority);
            }
            crate::authority::validate_node_identity(enrolled, self.enrollment, self.now)?;
            let generation = enrolled.enrollment.generation;
            let voter = group.voters.get(&node) == Some(&generation);
            let old_voter = group.outgoing_voters.get(&node) == Some(&generation);
            let learner = group.learners.get(&node) == Some(&generation);
            if !(voter || old_voter || learner) {
                return Err(DirectoryError::StaleNode);
            }
            if only.is_some_and(|expected| expected != node) {
                return Err(DirectoryError::UnverifiedAuthority);
            }
            incoming = incoming.saturating_add(usize::from(voter));
            outgoing = outgoing.saturating_add(usize::from(old_voter));
            sole |= only == Some(node);
        }
        if only.is_some() {
            if !sole {
                return Err(DirectoryError::UnverifiedAuthority);
            }
        } else if incoming
            <= group
                .voters
                .len()
                .checked_div(2)
                .ok_or(DirectoryError::Quorum)?
            || (!group.outgoing_voters.is_empty()
                && outgoing
                    <= group
                        .outgoing_voters
                        .len()
                        .checked_div(2)
                        .ok_or(DirectoryError::Quorum)?)
        {
            return Err(DirectoryError::Quorum);
        }
        Ok(())
    }
    pub fn verify_membership(&self, proof: &AuthorityProof) -> Result<(), DirectoryError> {
        let current = self.group_for(proof)?;
        let AuthorityFact::Membership {
            next,
            index,
            term,
            record_hash,
        } = &proof.statement.fact
        else {
            return Err(DirectoryError::WrongOperation);
        };
        // A membership epoch counts voter-set changes, matching the epoch a
        // session fence carries; adding or removing learners keeps it.
        let voters_changed =
            next.voters != current.voters || next.outgoing_voters != current.outgoing_voters;
        let expected_epoch = if voters_changed {
            current.membership_epoch.checked_add(1)
        } else {
            Some(current.membership_epoch)
        };
        if index.0 == 0
            || term.0 == 0
            || !types::nonzero_hash(*record_hash)
            || next.group != current.group
            || next.genesis != current.genesis
            || next.scope != current.scope
            || expected_epoch != Some(next.membership_epoch)
            || (!voters_changed && next.learners == current.learners)
        {
            return Err(DirectoryError::StaleEpoch);
        }
        // A joint configuration may only contain the prior incoming voters as
        // its outgoing set; leaving joint consensus cannot replace incoming voters.
        if !next.outgoing_voters.is_empty()
            && (next.outgoing_voters != current.voters || !current.outgoing_voters.is_empty())
        {
            return Err(DirectoryError::Quorum);
        }
        if !current.outgoing_voters.is_empty()
            && (next.voters != current.voters || !next.outgoing_voters.is_empty())
        {
            return Err(DirectoryError::Quorum);
        }
        // Direct single-step transitions may change at most one voter. Larger
        // replacements must pass through the explicit joint configuration above.
        if current.outgoing_voters.is_empty() && next.outgoing_voters.is_empty() {
            let changed = current
                .voters
                .iter()
                .filter(|(id, generation)| next.voters.get(id) != Some(generation))
                .count()
                .saturating_add(
                    next.voters
                        .iter()
                        .filter(|(id, generation)| current.voters.get(id) != Some(generation))
                        .count(),
                );
            if changed > 1 {
                return Err(DirectoryError::Quorum);
            }
        }
        self.signatures(proof, None)
    }
}
impl AuthorityVerifier for InstalledAuthorityVerifier<'_> {
    fn verify_enrollment(&self, enrollment: &NodeEnrollment) -> Result<(), DirectoryError> {
        let grant = self
            .registry
            .node(enrollment.node)
            .ok_or(DirectoryError::UnverifiedAuthority)?;
        if &grant.enrollment != enrollment {
            return Err(DirectoryError::UnverifiedAuthority);
        }
        crate::authority::validate_node_identity(grant, self.enrollment, self.now)
    }
    fn verify_session_fence(&self, fence: &SessionFence) -> Result<(), DirectoryError> {
        let proof = self.proofs.iter().find(|proof| matches!(&proof.statement.fact, AuthorityFact::Session(value) if value == fence))
            .ok_or(DirectoryError::UnverifiedAuthority)?;
        let group = self.group_for(proof)?;
        if group.scope != GroupScope::Session(fence.ledger)
            || group.group != fence.log_group
            || group.membership_epoch != fence.membership_epoch
            || fence.index.0 == 0
            || fence.term.0 == 0
            || !types::nonzero_hash(fence.record_hash)
            || !types::nonzero_hash(fence.placement_digest)
        {
            return Err(DirectoryError::UnverifiedAuthority);
        }
        self.signatures(proof, None)
    }
    fn verify_replica_ready(&self, ready: &ReplicaReady) -> Result<(), DirectoryError> {
        let mut body = ready.clone();
        body.attestation = ContentHash([0; 32]);
        let proof = self.proofs.iter().find(|proof| matches!(&proof.statement.fact, AuthorityFact::Replica(value) if value == &body))
            .ok_or(DirectoryError::UnverifiedAuthority)?;
        let group = self.group_for(proof)?;
        let node = self
            .registry
            .node(ready.node)
            .ok_or(DirectoryError::Missing)?;
        if group.scope != GroupScope::Session(ready.ledger)
            || ready.node_generation != node.enrollment.generation
            || !types::nonzero_hash(ready.custody)
            || ready.attestation != proof.attestation()?
        {
            return Err(DirectoryError::UnverifiedAuthority);
        }
        self.signatures(proof, Some(ready.node))
    }
    fn verify_custody(&self, proof: &CustodyProof) -> Result<(), DirectoryError> {
        let mut body = proof.clone();
        body.attestation = ContentHash([0; 32]);
        let signed = self.proofs.iter().find(|candidate| matches!(&candidate.statement.fact, AuthorityFact::Custody(value) if value == &body))
            .ok_or(DirectoryError::UnverifiedAuthority)?;
        let group = self.group_for(signed)?;
        let node = self
            .registry
            .node(proof.node)
            .ok_or(DirectoryError::Missing)?;
        if group.scope != GroupScope::Session(proof.ledger)
            || proof.node_generation != node.enrollment.generation
            || proof.custody_epoch == 0
            || !types::nonzero_hash(proof.content)
            || proof.attestation != signed.attestation()?
        {
            return Err(DirectoryError::UnverifiedAuthority);
        }
        self.signatures(signed, Some(proof.node))
    }
    fn verify_delegation(&self, fence: &DelegationFence) -> Result<(), DirectoryError> {
        if fence.cluster != self.registry.checkpoint().anchor.cluster
            || fence.source == fence.destination
            || fence.from_epoch.checked_add(1) != Some(fence.to_epoch)
            || fence.sealed_revision == 0
            || !types::nonzero_hash(fence.checkpoint)
            || !types::nonzero_hash(fence.destination_ready)
        {
            return Err(DirectoryError::UnverifiedAuthority);
        }
        for (source, partition) in [(true, fence.source), (false, fence.destination)] {
            let proof = self
                .proofs
                .iter()
                .find(|proof| match &proof.statement.fact {
                    AuthorityFact::DelegationSource(value) => source && value == fence,
                    AuthorityFact::DelegationDestination(value) => !source && value == fence,
                    _ => false,
                })
                .ok_or(DirectoryError::UnverifiedAuthority)?;
            let group = self.group_for(proof)?;
            if group.scope
                != (GroupScope::Partition {
                    partition,
                    namespace: fence.namespace,
                })
            {
                return Err(DirectoryError::OutsideNamespace);
            }
            self.signatures(proof, None)?;
        }
        Ok(())
    }
}
