//! Compact ledger-local history. The range already owns the ledger identity;
//! it is not repeated in each before/after/child binding on every event. Reads
//! expand it into the public exact-binding view without allocating.
use super::*;
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
    request: RequestKey,
    sequence: SessionSeq,
    ordinal: u32,
    fact: Fact,
}
#[derive(Debug, Clone, Copy)]
enum Fact {
    Artifact { binding: Revision },
    Accepted { key: NativeResultKey },
    Claim {
        kind: NativeEventKind,
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
            NativeFact::Artifact { binding } => Fact::Artifact { binding: Revision::pack(binding) },
            NativeFact::Accepted { key } => Fact::Accepted { key },
            NativeFact::Claim(row) => {
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
            request: event.request,
            sequence: event.sequence,
            ordinal: event.ordinal,
            fact,
        })
    }
    pub(super) fn expand(self, ledger: LedgerId) -> NativeEvent {
        NativeEvent {
            request: self.request,
            sequence: self.sequence,
            ordinal: self.ordinal,
            fact: match self.fact {
                Fact::Artifact { binding } => NativeFact::Artifact { binding: binding.expand(ledger) },
                Fact::Accepted { key } => NativeFact::Accepted { key },
                Fact::Claim {
                    kind,
                    owned_child,
                    before,
                    after,
                    status,
                } => NativeFact::Claim(NativeClaimEvent {
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
