use super::*;
use crate::lifecycle::{aggregation, claim, graph, succession};
use crate::{
    ClaimId, ContentHash, ObjectId, ObjectRevision, ParticipantId, ReceiptFence, ReceiptId,
    RootCommandId, SessionId, SessionSeq,
};

const ISSUER: ParticipantId = ParticipantId::from_u128(1);
const OTHER: ParticipantId = ParticipantId::from_u128(77);

fn generated() -> ClaimState {
    ClaimState::generate(Principal::Actor(ISSUER), claim::tests::definition(4)).unwrap()
}

#[test]
fn baseline_post_records_only_phase_and_revision_without_a_participant_registry() {
    let mut value = generated();
    value.subject = OTHER;
    // Outstanding graph/admission work gates receipt acquisition, not posting.
    value.graph = graph::Declaration::new(
        &[graph::Obligation {
            kind: graph::Kind::DependsOn,
            target: ClaimId::from_u128(55),
        }],
        1,
    )
    .unwrap();
    let before = value.clone();
    value
        .post_owned(Principal::Actor(ISSUER), before.binding())
        .unwrap();
    assert_eq!(value.status(), ClaimStatus::Posted);
    assert_eq!(value.binding(), before.binding().next().unwrap());
    assert_eq!(value.lineage(), before.lineage());
    assert_eq!(value.graph(), before.graph());
    assert_eq!(value.acceptance(), before.acceptance());
    assert_eq!(value.scopes(), before.scopes());
    assert_eq!(value.receipt(), None);
    assert_eq!(value.response_count(), 0);
    assert_eq!(value.local_sealed_at(), None);
    assert_eq!(value.terminal_cut(), None);
}

#[test]
fn generated_parent_with_owned_child_posts_against_its_current_revision() {
    let mut parent = generated();
    let mut definition = claim::tests::definition(4);
    definition.binding.object = ObjectId::from_u128(19);
    definition.created = SessionSeq(2);
    definition.lineage = succession::Lineage::new(
        definition.binding,
        Cause::Claim(ClaimId(parent.binding().object.0)),
        &[],
        0,
    )
    .unwrap();
    definition.acceptance = aggregation::acceptance_for(definition.binding, ISSUER);
    let child = parent
        .generate_child(
            &parent.binding(),
            Principal::Actor(ISSUER),
            None,
            definition,
            claim::ClaimCut {
                position: SessionSeq(2),
                cause: ContentHash([4; 32]),
            },
        )
        .unwrap();
    let expected = parent.binding();
    assert_eq!(expected.revision, ObjectRevision(2));
    assert_eq!(parent.lineage().binding().revision, ObjectRevision(1));
    let registry = parent.scopes().clone();
    parent
        .post_owned(Principal::Actor(ISSUER), expected)
        .unwrap();
    assert_eq!(parent.binding().revision, ObjectRevision(3));
    assert_eq!(parent.scopes(), &registry);
    assert_eq!(parent.scopes().children()[0].binding(), child.binding());
}

#[test]
fn only_exact_issuer_actor_and_current_binding_can_post() {
    let original = generated();
    let binding = original.binding();
    let cases = [
        (Principal::Node(ISSUER), binding, ContractError::WrongActor),
        (Principal::Actor(OTHER), binding, ContractError::WrongActor),
        (
            Principal::Actor(ISSUER),
            Binding {
                ledger: crate::LedgerId {
                    session: SessionId::from_u128(77),
                    ..binding.ledger
                },
                ..binding
            },
            ContractError::WrongLedger,
        ),
        (
            Principal::Actor(ISSUER),
            Binding {
                object: ObjectId::from_u128(88),
                ..binding
            },
            ContractError::WrongObject,
        ),
        (
            Principal::Actor(ISSUER),
            Binding {
                content: ContentHash([99; 32]),
                ..binding
            },
            ContractError::ContentConflict,
        ),
        (
            Principal::Actor(ISSUER),
            binding.next().unwrap(),
            ContractError::StaleRevision,
        ),
    ];
    for (principal, expected, error) in cases {
        let mut value = original.clone();
        assert_eq!(value.post_owned(principal, expected), Err(error));
        assert_eq!(value, original);
    }
}

#[test]
fn all_non_generated_states_and_repeat_post_refuse_without_repainting() {
    for status in ClaimStatus::ALL
        .iter()
        .copied()
        .filter(|status| *status != ClaimStatus::Generated)
    {
        let mut value = generated();
        value.status = status;
        let before = value.clone();
        assert_eq!(
            value.post_owned(Principal::Actor(ISSUER), value.binding()),
            Err(ContractError::InvalidTransition)
        );
        assert_eq!(value, before);
    }
    let mut value = generated();
    value
        .post_owned(Principal::Actor(ISSUER), value.binding())
        .unwrap();
    let before = value.clone();
    assert_eq!(
        value.post_owned(Principal::Actor(ISSUER), value.binding()),
        Err(ContractError::InvalidTransition)
    );
    assert_eq!(value, before);
}

#[test]
fn retained_identity_and_impossible_generated_facts_are_checked_before_mutation() {
    for case in 0..7 {
        let mut value = generated();
        let error = match case {
            0 => {
                value.subject = ParticipantId::from_u128(0);
                ContractError::InvalidTarget
            }
            1 => {
                value.receipt = Some(claim::ReceiptEntitlement {
                    holder: OTHER,
                    fence: ReceiptFence {
                        receipt: ReceiptId::from_u128(3),
                        epoch: 1,
                    },
                });
                ContractError::InvalidTransition
            }
            2 => {
                value.local_complete = true;
                ContractError::InvalidTransition
            }
            3 => {
                value.local_sealed_at = Some(SessionSeq(1));
                ContractError::InvalidTransition
            }
            4 => {
                value.acceptance = aggregation::acceptance_for(
                    Binding {
                        content: ContentHash([9; 32]),
                        ..value.binding()
                    },
                    ISSUER,
                );
                ContractError::ContentConflict
            }
            5 => {
                value.lineage = succession::Lineage::root(
                    Binding {
                        ledger: crate::LedgerId {
                            session: SessionId::from_u128(77),
                            ..value.binding().ledger
                        },
                        ..value.binding()
                    },
                    RootCommandId::from_u128(1),
                )
                .unwrap();
                ContractError::WrongLedger
            }
            _ => {
                value.lineage = succession::Lineage::root(
                    Binding {
                        content: ContentHash([9; 32]),
                        ..value.binding()
                    },
                    RootCommandId::from_u128(1),
                )
                .unwrap();
                ContractError::ContentConflict
            }
        };
        let before = value.clone();
        assert_eq!(
            value.post_owned(Principal::Actor(ISSUER), value.binding()),
            Err(error),
            "case {case}"
        );
        assert_eq!(value, before);
    }
}

#[test]
fn revision_overflow_is_refused_before_posting() {
    let mut value = generated();
    value.binding.revision = ObjectRevision(u64::MAX);
    let before = value.clone();
    assert_eq!(
        value.post_owned(Principal::Actor(ISSUER), value.binding()),
        Err(ContractError::Capacity)
    );
    assert_eq!(value, before);
}
