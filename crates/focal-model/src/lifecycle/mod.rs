//! Executable successor lifecycle contract (architecture document 17).
//!
//! These rules consume authenticated principals and effective owner state. They
//! neither deserialize client commands nor publish state. They deliberately have
//! no serialized representation: installing them in a ledger requires the storage
//! compatibility and activation gate in document 18. Historical V1 execution does
//! not call this module. Successful plans must be published atomically with their
//! evidence, histories, graph consequences and durable request outcome.
//!
//! Request deduplication precedes these rules. An exact retry returns its retained
//! outcome; a new request cannot reset an object by replaying a lifecycle event.

pub mod aggregation;
pub mod artifact_descriptor;
pub mod audit;
pub mod claim;
pub mod creation;
pub mod evidence;
pub mod graph;
mod memory;
pub mod ownership;
pub mod scope;
pub mod succession;
pub mod validation;

#[cfg(test)]
mod exchange_tests;

use crate::{ContentHash, LedgerId, ObjectId, ObjectRevision, ParticipantId};

/// Trusted ingress distinguishes an Actor from an enrolled infrastructure Node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Principal {
    Actor(ParticipantId),
    Node(ParticipantId),
}

impl Principal {
    pub fn require_actor(self, expected: ParticipantId) -> Result<(), ContractError> {
        match self {
            Self::Actor(actual) if actual == expected => Ok(()),
            Self::Actor(_) | Self::Node(_) => Err(ContractError::WrongActor),
        }
    }
}

/// Identity and revision of the effective row, including any pending mutations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Binding {
    pub ledger: LedgerId,
    pub object: ObjectId,
    pub content: ContentHash,
    pub revision: ObjectRevision,
}

impl Binding {
    pub fn check(&self, expected: &Self) -> Result<(), ContractError> {
        if self.ledger != expected.ledger {
            Err(ContractError::WrongLedger)
        } else if self.object != expected.object {
            Err(ContractError::WrongObject)
        } else if self.content != expected.content {
            Err(ContractError::ContentConflict)
        } else if self.revision != expected.revision {
            Err(ContractError::StaleRevision)
        } else {
            Ok(())
        }
    }

    pub fn next(self) -> Result<Self, ContractError> {
        let revision = self
            .revision
            .0
            .checked_add(1)
            .ok_or(ContractError::Capacity)?;
        Ok(Self {
            revision: ObjectRevision(revision),
            ..self
        })
    }
}

/// Semantic refusal, not a new persisted error-code allocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ContractError {
    #[error("the authenticated Actor does not hold this role")]
    WrongActor,
    #[error("ledger binding differs")]
    WrongLedger,
    #[error("object binding differs")]
    WrongObject,
    #[error("immutable content differs")]
    ContentConflict,
    #[error("object revision is stale")]
    StaleRevision,
    #[error("execution receipt is stale")]
    StaleReceipt,
    #[error("evaluation authority or generation is stale")]
    StaleEvaluation,
    #[error("lifecycle transition is forbidden")]
    InvalidTransition,
    #[error("target does not match its immutable declaration")]
    InvalidTarget,
    #[error("response manifest is not exact or contains invalid bindings")]
    InvalidManifest,
    #[error("required durable evidence is absent")]
    MissingEvidence,
    #[error("evaluation policy is invalid or unsatisfied")]
    InvalidPolicy,
    #[error("a configured bound or counter would be exceeded")]
    Capacity,
    #[error("two causes conflict at one canonical transition key")]
    ConflictingCause,
    #[error("committed transition position is invalid")]
    InvalidCut,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn identity_revision_and_principal_guards_are_distinct() {
        let original = Binding {
            ledger: LedgerId::default(),
            object: ObjectId::from_u128(1),
            content: ContentHash([2; 32]),
            revision: ObjectRevision(3),
        };
        let mismatches = [
            (
                Binding {
                    ledger: LedgerId {
                        session: crate::SessionId::from_u128(1),
                        ..original.ledger
                    },
                    ..original
                },
                ContractError::WrongLedger,
            ),
            (
                Binding {
                    object: ObjectId::from_u128(2),
                    ..original
                },
                ContractError::WrongObject,
            ),
            (
                Binding {
                    content: ContentHash([3; 32]),
                    ..original
                },
                ContractError::ContentConflict,
            ),
            (
                original.next().expect("bounded revision"),
                ContractError::StaleRevision,
            ),
        ];
        for (expected, error) in mismatches {
            assert_eq!(original.check(&expected), Err(error));
        }
        let actor = ParticipantId::from_u128(1);
        assert_eq!(Principal::Actor(actor).require_actor(actor), Ok(()));
        assert_eq!(
            Principal::Node(actor).require_actor(actor),
            Err(ContractError::WrongActor)
        );
        assert_eq!(
            Binding {
                revision: ObjectRevision(u64::MAX),
                ..original
            }
            .next(),
            Err(ContractError::Capacity)
        );
    }
}
