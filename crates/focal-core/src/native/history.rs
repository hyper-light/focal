//! Compact ledger-local history. The range already owns the ledger identity;
//! a retained event is held as its record encoding with every binding's ledger
//! implied by the range (`EventBindings::Implied`), at exactly its own width
//! rather than the widest fact's. Reads decode it into the public exact-binding
//! view.
use super::*;
#[cfg(test)]
use focal_model::lifecycle::audit::ResultTestamentState;

/// Whether every binding the event names is under `ledger`: the condition for
/// holding it with the ledger implied.
pub(super) fn under(event: NativeEvent, ledger: LedgerId) -> bool {
    let (first, second, third) = match event.fact {
        NativeFact::ResultTestament { before, after, .. }
        | NativeFact::Work { before, after, .. }
        | NativeFact::Response { before, after, .. }
        | NativeFact::Evaluation { before, after, .. } => (before, Some(after), None),
        NativeFact::Diagnostic { binding, .. }
        | NativeFact::Artifact { binding }
        | NativeFact::Definition { binding, .. } => (Some(binding), None, None),
        NativeFact::Registrations { claim }
        | NativeFact::Receipt { claim, .. }
        | NativeFact::ReceiptAdopted { claim, .. } => (Some(claim), None, None),
        NativeFact::Claim(row) => (row.owned_child, row.before, Some(row.after)),
        NativeFact::Missing { .. } | NativeFact::Delivery { .. } | NativeFact::Accepted { .. } => {
            (None, None, None)
        }
    };
    [first, second, third]
        .into_iter()
        .flatten()
        .all(|binding| binding.ledger == ledger)
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
        let ledger = binding(1).ledger;
        let stored = OwnedEvent::new(expected, ledger).unwrap();
        assert_eq!(stored.get(ledger), Some(expected));
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
                    OwnedEvent::new(event(fact), before.ledger),
                    Err(NativeError::Contract(ContractError::WrongLedger))
                ));
            }
        }
    }
}
