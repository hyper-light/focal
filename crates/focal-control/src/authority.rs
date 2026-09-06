//! Explicit activation of installed authority. Existing bootstrap bytes stay
//! unchanged; activation is a normal committed command in the existing group.
use crate::*;
use focal_directory::{
    AuthorityAnchor, AuthorityCheckpoint, AuthorityCommand, AuthorityOperation, AuthorityProof,
    AuthorityRegistry, InstalledAuthorityVerifier, LogGroupId, NamespaceRange,
};
use focal_enrollment::{EnrollmentRegistry, server_fingerprint};
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use focal_model::ContentHash;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(
    clippy::large_enum_variant,
    reason = "one bounded inline activation command; the host charges its complete decoded storage"
)]
pub enum AuthorityActivation {
    Root {
        expected_root_revision: u64,
        expected_enrollment_revision: u64,
        decided_at: i64,
    },
    /// The authenticated control owner obtains this snapshot after root ReadIndex,
    /// then commits this installation in the destination partition. Decoding a
    /// Node's supplied snapshot is never authorization to call this operation.
    Partition {
        expected_partition_revision: u64,
        snapshot: ControlAuthoritySnapshot,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorityInstallation {
    pub expected_source_index: u64,
    pub decided_at: i64,
    pub snapshot: ControlAuthoritySnapshot,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlEvidence {
    pub authority_revision: u64,
    pub enrollment_revision: u64,
    /// Trusted owner time recorded with the consuming command for replay.
    pub decided_at: i64,
    pub proofs: Vec<AuthorityProof>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerifiedRootCommand {
    pub command: RootCommand,
    pub evidence: ControlEvidence,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerifiedPartitionCommand {
    pub command: PartitionCommand,
    pub evidence: ControlEvidence,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlAuthoritySnapshot {
    /// Owner serving this snapshot. Install accepts snapshots served by root.
    pub identity: ControlIdentity,
    pub applied_index: u64,
    /// Immutable original root authority, including its unchanged genesis hash.
    pub source: ControlIdentity,
    pub source_index: u64,
    pub authority: AuthorityCheckpoint,
    pub enrollment: Vec<u8>,
    pub decided_at: i64,
}

pub(crate) struct RemoteEnrollment {
    pub registry: EnrollmentRegistry,
    pub _allocation: Allocation,
}
pub(crate) struct InstalledAuthority {
    pub registry: AuthorityRegistry,
    pub source: ControlIdentity,
    pub source_index: u64,
    pub decided_at: i64,
    pub remote: Option<RemoteEnrollment>,
}
impl InstalledAuthority {
    pub(crate) fn root(
        identity: ControlIdentity,
        enrollment: &EnrollmentRegistry,
        options: &ControlOptions,
        budget: &MemoryBudget,
    ) -> Result<Self, ControlError> {
        if identity.scope != ControlScope::Root {
            return Err(ControlError::WrongOwner);
        }
        let registry = AuthorityRegistry::new(
            anchor(identity, enrollment),
            options.authority,
            budget.clone(),
        )?;
        Ok(Self {
            registry,
            source: identity,
            source_index: 0,
            decided_at: 0,
            remote: None,
        })
    }
    pub(crate) fn from_snapshot(
        snapshot: ControlAuthoritySnapshot,
        identity: ControlIdentity,
        namespace: NamespaceRange,
        options: &ControlOptions,
        budget: &MemoryBudget,
        root_enrollment: Option<&EnrollmentRegistry>,
        recovery: bool,
    ) -> Result<Self, ControlError> {
        if snapshot.source.scope != ControlScope::Root
            || snapshot.source.cluster != identity.cluster
            || snapshot.source_index == 0
            || snapshot.authority.applied_index == 0
            || snapshot.authority.applied_index > snapshot.source_index
            || snapshot.authority.revision == 0
            || snapshot.decided_at < snapshot.authority.clock
            || snapshot.decided_at < 0
            || !contains(snapshot.authority.anchor.namespace, namespace)
            || if recovery {
                snapshot.identity != identity
            } else {
                snapshot.identity != snapshot.source
                    || snapshot.applied_index != snapshot.source_index
            }
        {
            return Err(ControlError::WrongOwner);
        }
        let enrollment = EnrollmentRegistry::restore(
            &snapshot.enrollment,
            identity.cluster.0,
            options.enrollment.clone(),
        )?;
        if enrollment.applied_index() > snapshot.source_index {
            return Err(ControlError::WrongOwner);
        }
        let expected = anchor(snapshot.source, &enrollment);
        if snapshot.authority.anchor != expected {
            return Err(ControlError::WrongOwner);
        }
        let registry = AuthorityRegistry::restore(
            snapshot.authority,
            &expected,
            options.authority,
            budget.clone(),
        )?;
        let remote = match root_enrollment {
            Some(local) => {
                if snapshot.source != identity
                    || snapshot.source_index != snapshot.applied_index
                    || hash("focal.control.enrollment-state.v1", local)?
                        != hash("focal.control.enrollment-state.v1", &enrollment)?
                {
                    return Err(ControlError::WrongOwner);
                }
                None
            }
            None => Some(RemoteEnrollment {
                _allocation: budget
                    .reserve(
                        BudgetKind::Control,
                        BudgetLane::Completion,
                        charge(enrollment.charged_bytes(), 16)?,
                    )?
                    .commit(),
                registry: enrollment,
            }),
        };
        Ok(Self {
            registry,
            source: snapshot.source,
            source_index: snapshot.source_index,
            decided_at: snapshot.decided_at,
            remote,
        })
    }
    pub(crate) fn enrollment<'a>(
        &'a self,
        local: Option<&'a EnrollmentRegistry>,
    ) -> Result<&'a EnrollmentRegistry, ControlError> {
        local
            .or_else(|| self.remote.as_ref().map(|entry| &entry.registry))
            .ok_or(ControlError::WrongOwner)
    }
    pub(crate) fn verifier<'a>(
        &'a self,
        local: Option<&'a EnrollmentRegistry>,
        evidence: &'a ControlEvidence,
    ) -> Result<InstalledAuthorityVerifier<'a>, ControlError> {
        let enrollment = self.enrollment(local)?;
        if evidence.authority_revision != self.registry.revision()
            || evidence.enrollment_revision != enrollment.revision()
        {
            return Err(focal_directory::DirectoryError::CompareFailed.into());
        }
        self.check_time(evidence.decided_at)?;
        Ok(self
            .registry
            .verifier(enrollment, &evidence.proofs, evidence.decided_at)?)
    }
    pub(crate) fn check_time(&self, now: i64) -> Result<(), ControlError> {
        if now < self.decided_at {
            return Err(focal_directory::DirectoryError::ClockRegression.into());
        }
        Ok(())
    }
    pub(crate) fn export(
        &self,
        identity: ControlIdentity,
        applied_index: u64,
        local: Option<&EnrollmentRegistry>,
    ) -> Result<ControlAuthoritySnapshot, ControlError> {
        let source_index = if self.remote.is_none() {
            applied_index
        } else {
            self.source_index
        };
        Ok(ControlAuthoritySnapshot {
            identity,
            applied_index,
            source: self.source,
            source_index,
            authority: self.registry.checkpoint().clone(),
            enrollment: self.enrollment(local)?.checkpoint()?,
            decided_at: self.decided_at,
        })
    }
    pub(crate) fn estimate(
        &self,
        local: Option<&EnrollmentRegistry>,
    ) -> Result<usize, ControlError> {
        postcard::experimental::serialized_size(self.registry.checkpoint())?
            .checked_add(self.enrollment(local)?.charged_bytes())
            .and_then(|n| n.checked_add(8192))
            .ok_or(ControlError::Capacity)
    }
    pub(crate) fn check_replacement(
        &self,
        next: &Self,
        expected_index: u64,
    ) -> Result<(), ControlError> {
        if self.source_index != expected_index
            || next.source != self.source
            || next.registry.checkpoint().anchor != self.registry.checkpoint().anchor
            || next.source_index <= self.source_index
            || next.registry.revision() < self.registry.revision()
            || next.registry.applied_index() < self.registry.applied_index()
            || next.decided_at < self.decided_at
            || next.enrollment(None)?.revision() < self.enrollment(None)?.revision()
            || next.enrollment(None)?.applied_index() < self.enrollment(None)?.applied_index()
        {
            return Err(focal_directory::DirectoryError::CompareFailed.into());
        }
        if next.registry.revision() == self.registry.revision()
            && next.registry.checkpoint() != self.registry.checkpoint()
        {
            return Err(ControlError::WrongOwner);
        }
        if next.enrollment(None)?.revision() == self.enrollment(None)?.revision()
            && hash("focal.control.enrollment-state.v1", next.enrollment(None)?)?
                != hash("focal.control.enrollment-state.v1", self.enrollment(None)?)?
        {
            return Err(ControlError::WrongOwner);
        }
        Ok(())
    }
}
fn anchor(identity: ControlIdentity, enrollment: &EnrollmentRegistry) -> AuthorityAnchor {
    AuthorityAnchor {
        cluster: identity.cluster,
        metadata_group: LogGroupId(identity.group),
        genesis: ContentHash(identity.genesis),
        namespace: NamespaceRange::all(),
        enrollment_ca: ContentHash(server_fingerprint(enrollment.ca_certificate())),
    }
}
fn contains(outer: NamespaceRange, inner: NamespaceRange) -> bool {
    inner.start >= outer.start
        && outer
            .end
            .is_none_or(|end| inner.end.is_some_and(|inner| inner <= end))
}

pub(crate) fn activation_command(enrollment: &EnrollmentRegistry, now: i64) -> AuthorityCommand {
    AuthorityCommand {
        expected_revision: 0,
        enrollment_revision: enrollment.revision(),
        decided_at: now,
        operation: AuthorityOperation::AdvanceClock,
    }
}
