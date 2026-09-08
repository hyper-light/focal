//! Compact ledger-local history. The range already owns the ledger identity;
//! it is not repeated in each before/after/child binding on every event. Reads
//! expand it into the public exact-binding view without allocating.
use super::*;
use focal_model::lifecycle::audit::ResultTestamentState;
use focal_model::{ObjectId, ObjectRevision};

#[derive(Debug, Clone, Copy)]
struct Revision {
    object: ObjectId,
    content: ContentHash,
    revision: ObjectRevision,
}
impl Revision {
    fn pack(binding: Binding) -> Self {
        Self {
            object: binding.object,
            content: binding.content,
            revision: binding.revision,
        }
    }
    fn expand(self, ledger: LedgerId) -> Binding {
        Binding {
            ledger,
            object: self.object,
            content: self.content,
            revision: self.revision,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct StoredEvent {
    invocation: NativeInvocation,
    sequence: SessionSeq,
    ordinal: u32,
    fact: Fact,
}
#[derive(Debug, Clone, Copy)]
enum Fact {
    Missing {
        key: NativeResultKey,
    },
    Registrations {
        claim: Revision,
    },
    Delivery {
        key: NativeResultKey,
    },
    Work {
        claim: ClaimId,
        before: Option<Revision>,
        after: Revision,
        state: WorkArtifactState,
    },
    Diagnostic {
        claim: ClaimId,
        binding: Revision,
        reason: EvidenceFailure,
    },
    Response {
        claim: ClaimId,
        before: Option<Revision>,
        after: Revision,
        state: ResponseState,
    },
    ResultTestament {
        claim: ClaimId,
        before: Option<Revision>,
        after: Revision,
        state: ResultTestamentState,
    },
    Receipt {
        claim: Revision,
        fence: ReceiptFence,
        holder: ParticipantId,
    },
    ReceiptAdopted {
        claim: Revision,
        previous: ReceiptEntitlement,
        replacement: ReceiptEntitlement,
        cause: ContentHash,
    },
    Artifact {
        binding: Revision,
    },
    Accepted {
        key: NativeResultKey,
    },
    Claim {
        kind: NativeEventKind,
        graph: Option<NativeGraphCapture>,
        owned_child: Option<Revision>,
        before: Option<Revision>,
        after: Revision,
        status: ClaimStatus,
    },
    Definition {
        binding: Revision,
        claim: ClaimId,
        index: u32,
        intent: ContentHash,
    },
    Evaluation {
        kind: NativeEvaluationEventKind,
        key: EvaluationKey,
        before: Option<Revision>,
        after: Revision,
        state: validation::State,
        phase: validation::Phase,
        attempt: Option<validation::Attempt>,
        fence: Option<validation::AuthorityFence>,
    },
}
impl StoredEvent {
    pub(super) fn pack(event: NativeEvent) -> Result<Self, ContractError> {
        let fact = match event.fact {
            NativeFact::Missing { key } => Fact::Missing { key },
            NativeFact::Registrations { claim } => Fact::Registrations {
                claim: Revision::pack(claim),
            },
            NativeFact::Delivery { key } => Fact::Delivery { key },
            NativeFact::Work {
                claim,
                before,
                after,
                state,
            } => {
                if before.is_some_and(|value| value.ledger != after.ledger) {
                    return Err(ContractError::WrongLedger);
                }
                Fact::Work {
                    claim,
                    before: before.map(Revision::pack),
                    after: Revision::pack(after),
                    state,
                }
            }
            NativeFact::Diagnostic {
                claim,
                binding,
                reason,
            } => Fact::Diagnostic {
                claim,
                binding: Revision::pack(binding),
                reason,
            },
            NativeFact::Response {
                claim,
                before,
                after,
                state,
            } => {
                if before.is_some_and(|value| value.ledger != after.ledger) {
                    return Err(ContractError::WrongLedger);
                }
                Fact::Response {
                    claim,
                    before: before.map(Revision::pack),
                    after: Revision::pack(after),
                    state,
                }
            }
            NativeFact::ResultTestament {
                claim,
                before,
                after,
                state,
            } => {
                if before.is_some_and(|value| value.ledger != after.ledger) {
                    return Err(ContractError::WrongLedger);
                }
                Fact::ResultTestament {
                    claim,
                    before: before.map(Revision::pack),
                    after: Revision::pack(after),
                    state,
                }
            }
            NativeFact::Receipt {
                claim,
                fence,
                holder,
            } => Fact::Receipt {
                claim: Revision::pack(claim),
                fence,
                holder,
            },
            NativeFact::ReceiptAdopted {
                claim,
                previous,
                replacement,
                cause,
            } => Fact::ReceiptAdopted {
                claim: Revision::pack(claim),
                previous,
                replacement,
                cause,
            },
            NativeFact::Artifact { binding } => Fact::Artifact {
                binding: Revision::pack(binding),
            },
            NativeFact::Accepted { key } => Fact::Accepted { key },
            NativeFact::Claim(row) => {
                row.check_graph_capture(event.ordinal)?;
                if row
                    .before
                    .is_some_and(|value| value.ledger != row.after.ledger)
                    || row
                        .owned_child
                        .is_some_and(|value| value.ledger != row.after.ledger)
                {
                    return Err(ContractError::WrongLedger);
                }
                Fact::Claim {
                    kind: row.kind,
                    graph: row.graph,
                    owned_child: row.owned_child.map(Revision::pack),
                    before: row.before.map(Revision::pack),
                    after: Revision::pack(row.after),
                    status: row.status,
                }
            }
            NativeFact::Definition {
                binding,
                claim,
                index,
                intent,
            } => Fact::Definition {
                binding: Revision::pack(binding),
                claim,
                index,
                intent,
            },
            NativeFact::Evaluation {
                kind,
                key,
                before,
                after,
                state,
                phase,
                attempt,
                fence,
            } => {
                if before.is_some_and(|value| value.ledger != after.ledger) {
                    return Err(ContractError::WrongLedger);
                }
                Fact::Evaluation {
                    kind,
                    key,
                    before: before.map(Revision::pack),
                    after: Revision::pack(after),
                    state,
                    phase,
                    attempt,
                    fence,
                }
            }
        };
        Ok(Self {
            invocation: event.invocation,
            sequence: event.sequence,
            ordinal: event.ordinal,
            fact,
        })
    }
    pub(super) fn expand(self, ledger: LedgerId) -> NativeEvent {
        NativeEvent {
            invocation: self.invocation,
            sequence: self.sequence,
            ordinal: self.ordinal,
            fact: match self.fact {
                Fact::Missing { key } => NativeFact::Missing { key },
                Fact::Registrations { claim } => NativeFact::Registrations {
                    claim: claim.expand(ledger),
                },
                Fact::Delivery { key } => NativeFact::Delivery { key },
                Fact::Work {
                    claim,
                    before,
                    after,
                    state,
                } => NativeFact::Work {
                    claim,
                    before: before.map(|value| value.expand(ledger)),
                    after: after.expand(ledger),
                    state,
                },
                Fact::Diagnostic {
                    claim,
                    binding,
                    reason,
                } => NativeFact::Diagnostic {
                    claim,
                    binding: binding.expand(ledger),
                    reason,
                },
                Fact::Response {
                    claim,
                    before,
                    after,
                    state,
                } => NativeFact::Response {
                    claim,
                    before: before.map(|value| value.expand(ledger)),
                    after: after.expand(ledger),
                    state,
                },
                Fact::ResultTestament {
                    claim,
                    before,
                    after,
                    state,
                } => NativeFact::ResultTestament {
                    claim,
                    before: before.map(|value| value.expand(ledger)),
                    after: after.expand(ledger),
                    state,
                },
                Fact::Receipt {
                    claim,
                    fence,
                    holder,
                } => NativeFact::Receipt {
                    claim: claim.expand(ledger),
                    fence,
                    holder,
                },
                Fact::ReceiptAdopted {
                    claim,
                    previous,
                    replacement,
                    cause,
                } => NativeFact::ReceiptAdopted {
                    claim: claim.expand(ledger),
                    previous,
                    replacement,
                    cause,
                },
                Fact::Artifact { binding } => NativeFact::Artifact {
                    binding: binding.expand(ledger),
                },
                Fact::Accepted { key } => NativeFact::Accepted { key },
                Fact::Claim {
                    kind,
                    graph,
                    owned_child,
                    before,
                    after,
                    status,
                } => NativeFact::Claim(NativeClaimEvent {
                    graph,
                    kind,
                    owned_child: owned_child.map(|value| value.expand(ledger)),
                    before: before.map(|value| value.expand(ledger)),
                    after: after.expand(ledger),
                    status,
                }),
                Fact::Definition {
                    binding,
                    claim,
                    index,
                    intent,
                } => NativeFact::Definition {
                    binding: binding.expand(ledger),
                    claim,
                    index,
                    intent,
                },
                Fact::Evaluation {
                    kind,
                    key,
                    before,
                    after,
                    state,
                    phase,
                    attempt,
                    fence,
                } => NativeFact::Evaluation {
                    kind,
                    key,
                    before: before.map(|value| value.expand(ledger)),
                    after: after.expand(ledger),
                    state,
                    phase,
                    attempt,
                    fence,
                },
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native::report_tests::{ISSUER, binding, request};

    fn event(fact: NativeFact) -> NativeEvent {
        NativeEvent {
            invocation: request(ISSUER, 91).into(),
            sequence: SessionSeq(77),
            ordinal: 3,
            fact,
        }
    }

    fn roundtrip(fact: NativeFact) {
        let expected = event(fact);
        let stored = StoredEvent::pack(expected).unwrap();
        assert_eq!(stored.expand(binding(1).ledger), expected);
    }

    #[test]
    fn compact_independent_history_retains_all_states_revisions_and_failure_reasons() {
        let claim = ClaimId::from_u128(42);
        let before = binding(92);
        let after = before.next().unwrap();
        for previous in [None, Some(before)] {
            for state in WorkArtifactState::ALL {
                roundtrip(NativeFact::Work {
                    claim,
                    before: previous,
                    after,
                    state: *state,
                });
            }
            for state in ResponseState::ALL {
                roundtrip(NativeFact::Response {
                    claim,
                    before: previous,
                    after,
                    state: *state,
                });
            }
            for state in [
                ResultTestamentState::Generated,
                ResultTestamentState::Posted,
            ] {
                roundtrip(NativeFact::ResultTestament {
                    claim,
                    before: previous,
                    after,
                    state,
                });
            }
        }
        for reason in [
            EvidenceFailure::Work,
            EvidenceFailure::Production,
            EvidenceFailure::Structure,
            EvidenceFailure::Metadata,
        ] {
            roundtrip(NativeFact::Diagnostic {
                claim,
                binding: after,
                reason,
            });
        }
    }

    #[test]
    fn compact_independent_history_rejects_cross_ledger_revision_pairs() {
        let claim = ClaimId::from_u128(42);
        let before = binding(92);
        for ledger in [
            LedgerId {
                tenant: focal_model::TenantId::from_u128(99),
                ..before.ledger
            },
            LedgerId {
                session: focal_model::SessionId::from_u128(99),
                ..before.ledger
            },
        ] {
            let after = Binding {
                ledger,
                ..before.next().unwrap()
            };
            for fact in [
                NativeFact::Work {
                    claim,
                    before: Some(before),
                    after,
                    state: WorkArtifactState::Received,
                },
                NativeFact::Response {
                    claim,
                    before: Some(before),
                    after,
                    state: ResponseState::Posted,
                },
                NativeFact::ResultTestament {
                    claim,
                    before: Some(before),
                    after,
                    state: ResultTestamentState::Posted,
                },
            ] {
                assert!(matches!(
                    StoredEvent::pack(event(fact)),
                    Err(ContractError::WrongLedger)
                ));
            }
        }
    }
}
