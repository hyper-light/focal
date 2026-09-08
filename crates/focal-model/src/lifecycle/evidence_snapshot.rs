//! Versioned restoration views, independent of application V1. These establish
//! intrinsic coherence against retained immutable policy, artifacts and work
//! rows. They do not prove custody, receipt authority, journal completeness or
//! that an otherwise coherent history occurred. The native importer supplies
//! those proofs, including the actual original Generated response binding.
//! No current parent, clock, principal or execution capability is replayed.
use super::*;
use crate::lifecycle::{
    aggregation::{self, AcceptancePolicy, ArtifactOutcome, BlockingKind, ResponseOutcome},
    artifact_descriptor::{ArtifactDescriptor, WorkProvenance, WorkRole},
    graph::VisitBudget,
};
use crate::{ArtifactId, ContentHash, SessionSeq};

#[path = "evidence_response_snapshot.rs"]
mod response;
pub use response::{
    ResponseArtifacts, ResponseHydrationPlan, ResponseSnapshotFieldsV1, ResponseSnapshotSource,
    ResponseSnapshotV1,
};

#[cfg(test)]
#[path = "evidence_snapshot_tests.rs"]
mod tests;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkTerminalSnapshotV1 {
    Passed {
        sequence: SessionSeq,
    },
    Blocked {
        sequence: SessionSeq,
        cause: aggregation::BlockingCauseSnapshotV1,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseTerminalSnapshotV1 {
    Validated { sequence: SessionSeq },
    Blocked(aggregation::TerminalCutSnapshotV1),
}
impl ResponseTerminalSnapshotV1 {
    fn from_outcome(value: ResponseOutcome) -> Result<Self, ContractError> {
        match value {
            ResponseOutcome::Validated { sequence } => Ok(Self::Validated { sequence }),
            ResponseOutcome::Blocked(cut) => Ok(Self::Blocked(cut.snapshot_v1())),
            ResponseOutcome::Evaluating => Err(ContractError::InvalidCut),
        }
    }
    fn restore(self) -> Result<ResponseOutcome, ContractError> {
        match self {
            Self::Validated { sequence } if sequence.0 != 0 => {
                Ok(ResponseOutcome::Validated { sequence })
            }
            Self::Blocked(cut) => Ok(ResponseOutcome::Blocked(
                aggregation::TerminalCut::hydrate_v1(cut)?,
            )),
            _ => Err(ContractError::InvalidCut),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkArtifactSnapshotV1 {
    pub binding: Binding,
    pub claim: ClaimId,
    pub slot: u32,
    pub cycle: u32,
    pub producer: ParticipantId,
    pub receipt: ReceiptFence,
    pub state: WorkArtifactState,
    /// Full original response binding, never just its allocated ID.
    pub attachment: Option<Binding>,
    pub diagnostic: Option<Diagnostic>,
    pub terminal: Option<WorkTerminalSnapshotV1>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FailedWorkSnapshotV1 {
    pub binding: Binding,
    pub slot: u32,
    pub state: WorkArtifactState,
    pub diagnostic: Diagnostic,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResponseDiagnosticSnapshotV1 {
    pub ledger: LedgerId,
    pub claim: ClaimId,
    pub receipt: ReceiptFence,
    pub cycle: u32,
    pub producer: ParticipantId,
    pub diagnostic: Diagnostic,
}

fn require(value: bool) -> Result<(), ContractError> {
    if value {
        Ok(())
    } else {
        Err(ContractError::InvalidManifest)
    }
}
fn binding(value: Binding) -> Result<(), ContractError> {
    require(
        !value.ledger.tenant.is_zero()
            && !value.ledger.session.is_zero()
            && !value.object.is_zero()
            && value.content != ContentHash([0; 32]),
    )
}
fn same_content(left: Binding, right: Binding) -> Result<(), ContractError> {
    require(
        left.ledger == right.ledger && left.object == right.object && left.content == right.content,
    )
}
fn frame(
    ledger: LedgerId,
    claim: ClaimId,
    receipt: ReceiptFence,
    cycle: u32,
    producer: ParticipantId,
) -> Result<(), ContractError> {
    require(
        !ledger.tenant.is_zero()
            && !ledger.session.is_zero()
            && !claim.is_zero()
            && !receipt.receipt.is_zero()
            && receipt.epoch != 0
            && cycle != 0
            && cycle.checked_add(1).is_some()
            && !producer.is_zero(),
    )
}
fn descriptor(
    source: &ArtifactDescriptor,
    ledger: LedgerId,
    artifact: ArtifactRef,
    receipt: ReceiptFence,
    producer: ParticipantId,
    provenance: WorkProvenance,
) -> Result<(), ContractError> {
    require(
        source.ledger() == ledger
            && source.id() == artifact.id
            && source.content_hash() == artifact.hash
            && source.receipt() == Some(receipt)
            && source.producer() == producer
            && source.result_provenance().is_none()
            && source.work_provenance() == Some(provenance),
    )
}
fn error_descriptor(source: &ArtifactDescriptor) -> Result<(), ContractError> {
    require(
        source.kind() == "error"
            && source.schema() != 0
            && source.schema_hash() != ContentHash([0; 32]),
    )
}
fn slot_visits(policy: &AcceptancePolicy) -> Result<usize, ContractError> {
    // A binary search takes at most bit_length(n)+1 comparisons, including empty.
    usize::try_from(
        usize::BITS
            .checked_sub(policy.slot_count().leading_zeros())
            .ok_or(ContractError::Capacity)?,
    )
    .map_err(|_| ContractError::Capacity)?
    .checked_add(1)
    .ok_or(ContractError::Capacity)
}

impl WorkArtifact {
    pub fn snapshot_v1(&self) -> Result<WorkArtifactSnapshotV1, ContractError> {
        let terminal = match self.terminal {
            None => None,
            Some((sequence, ArtifactOutcome::Passed)) => {
                Some(WorkTerminalSnapshotV1::Passed { sequence })
            }
            Some((sequence, ArtifactOutcome::Blocked(cause))) => {
                Some(WorkTerminalSnapshotV1::Blocked {
                    sequence,
                    cause: cause.snapshot_v1(),
                })
            }
            Some((_, ArtifactOutcome::Pending)) => return Err(ContractError::InvalidCut),
        };
        Ok(WorkArtifactSnapshotV1 {
            binding: self.binding,
            claim: self.claim,
            slot: self.slot,
            cycle: self.cycle,
            producer: self.producer,
            receipt: self.receipt,
            state: self.state,
            attachment: self.attachment,
            diagnostic: self.diagnostic,
            terminal,
        })
    }
    /// Fixed scalar/descriptor checks plus the immutable slot binary search.
    /// Descriptor lookup, decoding and custody verification are importer work.
    pub fn hydration_visits(policy: &AcceptancePolicy) -> Result<usize, ContractError> {
        bytes::add(192, slot_visits(policy)?)
    }
    pub fn hydrate_v1(
        policy: &AcceptancePolicy,
        value: WorkArtifactSnapshotV1,
        source: &ArtifactDescriptor,
        diagnostic: Option<&ArtifactDescriptor>,
        max_visits: usize,
    ) -> Result<Self, ContractError> {
        if Self::hydration_visits(policy)? > max_visits {
            return Err(ContractError::Capacity);
        }
        binding(value.binding)?;
        frame(
            value.binding.ledger,
            value.claim,
            value.receipt,
            value.cycle,
            value.producer,
        )?;
        require(
            policy.claim().ledger == value.binding.ledger
                && policy.claim().object.0 == value.claim.0
                && policy.has_slot(value.slot),
        )?;
        let reference = ArtifactRef {
            id: ArtifactId(value.binding.object.0),
            hash: value.binding.content,
        };
        let role = if value.state == WorkArtifactState::GenerationFailed {
            WorkRole::Diagnostic {
                reason: EvidenceFailure::Production,
            }
        } else {
            WorkRole::Output { slot: value.slot }
        };
        descriptor(
            source,
            value.binding.ledger,
            reference,
            value.receipt,
            value.producer,
            WorkProvenance {
                claim: value.claim,
                cycle: value.cycle,
                role,
            },
        )?;
        let attached = matches!(
            value.state,
            WorkArtifactState::Attached
                | WorkArtifactState::Validating
                | WorkArtifactState::Validated
                | WorkArtifactState::ValidationFailed
        );
        require(value.attachment.is_some() == attached)?;
        if let Some(attachment) = value.attachment {
            require(!attachment.object.is_zero() && attachment.ledger == value.binding.ledger)?;
        }
        match value.state {
            WorkArtifactState::GenerationFailed => {
                error_descriptor(source)?;
                require(
                    value.diagnostic
                        == Some(Diagnostic {
                            reason: EvidenceFailure::Production,
                            artifact: reference,
                        })
                        && diagnostic.is_none(),
                )?;
            }
            WorkArtifactState::ReceiptFailed => {
                let failure = value.diagnostic.ok_or(ContractError::MissingEvidence)?;
                require(
                    matches!(
                        failure.reason,
                        EvidenceFailure::Structure | EvidenceFailure::Metadata
                    ) && failure.artifact.id != reference.id,
                )?;
                let diagnostic = diagnostic.ok_or(ContractError::MissingEvidence)?;
                error_descriptor(diagnostic)?;
                descriptor(
                    diagnostic,
                    value.binding.ledger,
                    failure.artifact,
                    value.receipt,
                    policy.issuer(),
                    WorkProvenance {
                        claim: value.claim,
                        cycle: value.cycle,
                        role: WorkRole::ReceiptRejection {
                            artifact: reference,
                            reason: failure.reason,
                        },
                    },
                )?;
            }
            _ => require(value.diagnostic.is_none() && diagnostic.is_none())?,
        }
        let terminal = match (value.state, value.terminal) {
            (WorkArtifactState::Validated, Some(WorkTerminalSnapshotV1::Passed { sequence }))
                if sequence.0 != 0 =>
            {
                Some((sequence, ArtifactOutcome::Passed))
            }
            (
                WorkArtifactState::ValidationFailed,
                Some(WorkTerminalSnapshotV1::Blocked { sequence, cause }),
            ) if sequence.0 != 0 => {
                let cause = aggregation::BlockingCause::hydrate_v1(cause)?;
                require(
                    cause.slot() == Some(value.slot)
                        && cause.artifact() == Some(reference)
                        && cause.mode() == crate::ValidationMode::Required
                        && value.attachment.is_some_and(|attachment| {
                            cause.key().target
                                == aggregation::CauseTarget::Response(TestamentId(
                                    attachment.object.0,
                                ))
                        }),
                )?;
                Some((sequence, ArtifactOutcome::Blocked(cause)))
            }
            (WorkArtifactState::Validated | WorkArtifactState::ValidationFailed, _) => {
                return Err(ContractError::InvalidCut);
            }
            (_, None) => None,
            _ => return Err(ContractError::InvalidCut),
        };
        Ok(Self {
            binding: value.binding,
            claim: value.claim,
            slot: value.slot,
            cycle: value.cycle,
            producer: value.producer,
            receipt: value.receipt,
            state: value.state,
            attachment: value.attachment,
            diagnostic: value.diagnostic,
            terminal,
        })
    }
}
impl FailedWork {
    pub fn snapshot_v1(self) -> FailedWorkSnapshotV1 {
        FailedWorkSnapshotV1 {
            binding: self.binding,
            slot: self.slot,
            state: self.state,
            diagnostic: self.diagnostic,
        }
    }
    /// Failed work states are immutable. Restore only against that actual row;
    /// a diagnostic cannot substitute for a nonexistent output or different slot.
    pub fn hydrate_v1(
        value: FailedWorkSnapshotV1,
        work: &WorkArtifact,
    ) -> Result<Self, ContractError> {
        let actual = Self::from_artifact(work)?.ok_or(ContractError::InvalidManifest)?;
        require(actual.snapshot_v1() == value)?;
        Ok(actual)
    }
}
impl ResponseDiagnostic {
    pub fn snapshot_v1(self) -> ResponseDiagnosticSnapshotV1 {
        ResponseDiagnosticSnapshotV1 {
            ledger: self.ledger,
            claim: self.claim,
            receipt: self.receipt,
            cycle: self.cycle,
            producer: self.producer,
            diagnostic: self.diagnostic,
        }
    }
    pub const HYDRATION_VISITS: usize = 96;
    pub fn hydrate_v1(
        value: ResponseDiagnosticSnapshotV1,
        source: &ArtifactDescriptor,
        max_visits: usize,
    ) -> Result<Self, ContractError> {
        if max_visits < Self::HYDRATION_VISITS {
            return Err(ContractError::Capacity);
        }
        frame(
            value.ledger,
            value.claim,
            value.receipt,
            value.cycle,
            value.producer,
        )?;
        descriptor(
            source,
            value.ledger,
            value.diagnostic.artifact,
            value.receipt,
            value.producer,
            WorkProvenance {
                claim: value.claim,
                cycle: value.cycle,
                role: WorkRole::Diagnostic {
                    reason: value.diagnostic.reason,
                },
            },
        )?;
        error_descriptor(source)?;
        Ok(Self {
            ledger: value.ledger,
            claim: value.claim,
            receipt: value.receipt,
            cycle: value.cycle,
            producer: value.producer,
            diagnostic: value.diagnostic,
        })
    }
}
