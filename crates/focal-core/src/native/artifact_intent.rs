//! Shared actor preimages for full artifact inputs and borrowed descriptor plans.
//! The descriptor identity is derived by model preparation; these copied fields
//! supply no custody, actor authority or state-dependent acceptance.
use super::intent::hash_binding;
use super::*;

#[derive(Debug, Clone, Copy)]
pub(super) enum ReportKind {
    Admission,
    Increment,
    Work,
}

#[derive(Debug, Clone, Copy)]
// Decoding must keep the complete scalar request on the stack before memory
// admission; boxing this variant would allocate before the owner can fund it.
#[allow(clippy::large_enum_variant)]
pub(super) enum ArtifactCommand {
    SubmitWork {
        claim: Binding,
        slot: u32,
    },
    SubmitDiagnostic {
        claim: Binding,
        reason: EvidenceFailure,
    },
    RejectWork {
        claim: Binding,
        expected: Binding,
        reason: EvidenceFailure,
    },
    Report {
        kind: ReportKind,
        claim: Binding,
        key: EvaluationKey,
        expected: Binding,
        report: validation::Report,
    },
}

impl ArtifactCommand {
    pub(super) fn build(self, artifact: NativeArtifactInput) -> NativeCommand {
        match self {
            Self::SubmitWork { claim, slot } => NativeCommand::SubmitWork {
                claim,
                slot,
                artifact,
            },
            Self::SubmitDiagnostic { claim, reason } => NativeCommand::SubmitDiagnostic {
                claim,
                reason,
                artifact,
            },
            Self::RejectWork {
                claim,
                expected,
                reason,
            } => NativeCommand::RejectWork {
                claim,
                expected,
                reason,
                artifact,
            },
            Self::Report {
                kind,
                claim,
                key,
                expected,
                report,
            } => match kind {
                ReportKind::Admission => NativeCommand::ReportAdmission {
                    claim,
                    key,
                    expected,
                    report,
                    artifact,
                },
                ReportKind::Increment => NativeCommand::ReportIncrement {
                    claim,
                    key,
                    expected,
                    report,
                    artifact,
                },
                ReportKind::Work => NativeCommand::ReportWork {
                    claim,
                    key,
                    expected,
                    report,
                    artifact,
                },
            },
        }
    }

    pub(super) fn hash_into(
        self,
        hash: &mut blake3::Hasher,
        artifact: ContentHash,
    ) -> Result<(), NativeError> {
        fn failure(hash: &mut blake3::Hasher, reason: EvidenceFailure) {
            hash.update(&[match reason {
                EvidenceFailure::Work => 0,
                EvidenceFailure::Production => 1,
                EvidenceFailure::Structure => 2,
                EvidenceFailure::Metadata => 3,
            }]);
        }
        match self {
            Self::SubmitWork { claim, slot } => {
                hash.update(&[6]);
                hash_binding(hash, claim);
                hash.update(&slot.to_le_bytes());
            }
            Self::SubmitDiagnostic { claim, reason } => {
                hash.update(&[7]);
                hash_binding(hash, claim);
                failure(hash, reason);
            }
            Self::RejectWork {
                claim,
                expected,
                reason,
            } => {
                hash.update(&[13]);
                hash_binding(hash, claim);
                hash_binding(hash, expected);
                failure(hash, reason);
            }
            Self::Report {
                kind,
                claim,
                key,
                expected,
                report,
            } => {
                match (kind, key.target) {
                    (
                        ReportKind::Work,
                        EvaluationTarget::Work {
                            response,
                            slot,
                            artifact,
                        },
                    ) => {
                        hash.update(&[19]);
                        hash.update(&response.0);
                        hash.update(&slot.to_le_bytes());
                        hash.update(&artifact.0);
                    }
                    (ReportKind::Increment, EvaluationTarget::Increment { artifact }) => {
                        hash.update(&[15]);
                        hash.update(&artifact.0);
                    }
                    (ReportKind::Admission, EvaluationTarget::Admission) => {
                        hash.update(&[4]);
                    }
                    _ => return Err(ContractError::InvalidTarget.into()),
                }
                hash_binding(hash, claim);
                hash.update(&key.claim.0);
                hash.update(&key.validation.0);
                hash.update(&key.generation.to_le_bytes());
                hash_binding(hash, expected);
                hash.update(&report.generation.to_le_bytes());
                hash.update(&[match report.attempt.phase {
                    validation::Phase::Programmatic => 0,
                    validation::Phase::Quality => 1,
                    validation::Phase::Delivery => 2,
                    validation::Phase::MissingTarget => 3,
                }]);
                hash.update(&report.attempt.index.to_le_bytes());
                hash.update(&report.attempt.handler.0);
                hash.update(&report.attempt.version.0);
                hash.update(&report.attempt.evaluator.0);
                hash.update(&report.attempt.definition.0);
                hash.update(&[match report.value {
                    focal_model::VerdictValue::Pass => 0,
                    focal_model::VerdictValue::Fail => 1,
                    focal_model::VerdictValue::Incomplete => 2,
                    focal_model::VerdictValue::Error => 3,
                }]);
                hash.update(&report.evidence.id.0);
                hash.update(&report.evidence.hash.0);
            }
        }
        hash.update(&artifact.0);
        Ok(())
    }
}
