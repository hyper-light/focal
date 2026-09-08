use super::*;
use crate::native::report_tests as fixture;
use focal_model::lifecycle::claim::{ClaimCut, ClaimIntent};
use focal_model::{ObjectId, ObjectRevision, SessionId, TenantId, ValidationMode};

fn actual_seals(count: usize) -> Vec<SealTransition> {
    let mut core = fixture::core();
    core.limits.plan_edges = 64 * 1024;
    fixture::publish(
        &mut core,
        10,
        fixture::creation(1, 1, &vec![(ValidationMode::Observe, false); count], None),
    );
    fixture::publish(&mut core, 20, fixture::post(2, fixture::binding(1)));
    let actual = core.native_claim(ClaimId::from_u128(1)).unwrap();
    let mut cancelled = actual.try_copy(actual.copy_charge().unwrap()).unwrap();
    cancelled
        .apply(
            &actual.binding(),
            Principal::Actor(fixture::ISSUER),
            ClaimIntent::Cancel {
                cut: ClaimCut {
                    position: SessionSeq(core.native_sequence().0 + 1),
                    cause: ContentHash([93; 32]),
                },
            },
        )
        .unwrap();
    (1..=u32::try_from(count).unwrap())
        .map(|index| {
            let key = fixture::key(index);
            let state = core.native_evaluation(key).unwrap();
            let seal = state
                .seal_claim(
                    core.native_definition(key.validation).unwrap(),
                    &state.binding(),
                    &cancelled,
                )
                .unwrap();
            assert!(seal.changed());
            assert!(!seal.previous().has_begun());
            seal
        })
        .collect()
}

fn different_bindings(binding: Binding) -> [Binding; 5] {
    [
        Binding {
            ledger: LedgerId {
                tenant: TenantId::from_u128(999),
                ..binding.ledger
            },
            ..binding
        },
        Binding {
            ledger: LedgerId {
                session: SessionId::from_u128(999),
                ..binding.ledger
            },
            ..binding
        },
        Binding {
            object: ObjectId::from_u128(999),
            ..binding
        },
        Binding {
            content: ContentHash([99; 32]),
            ..binding
        },
        Binding {
            revision: ObjectRevision(binding.revision.0 + 1),
            ..binding
        },
    ]
}

#[test]
fn canonical_target_identity_keeps_every_variant_binding_field_and_slot() {
    use validation::Target;
    let primary = fixture::binding(1);
    let product = fixture::binding(2);
    let targets = [
        Target::Admission { claim: primary },
        Target::Increment {
            claim: primary,
            artifact: product,
        },
        Target::Artifact {
            response: primary,
            slot: 0,
            artifact: product,
        },
        Target::MissingSlot {
            response: primary,
            slot: 0,
        },
        Target::Delivery { response: primary },
    ];
    for (index, target) in targets.iter().copied().enumerate() {
        for other in targets.iter().skip(index + 1).copied() {
            assert!(target_order(target) != target_order(other));
        }
        for changed in different_bindings(primary) {
            let alternate = match target {
                Target::Admission { .. } => Target::Admission { claim: changed },
                Target::Increment { artifact, .. } => Target::Increment {
                    claim: changed,
                    artifact,
                },
                Target::Artifact { slot, artifact, .. } => Target::Artifact {
                    response: changed,
                    slot,
                    artifact,
                },
                Target::MissingSlot { slot, .. } => Target::MissingSlot {
                    response: changed,
                    slot,
                },
                Target::Delivery { .. } => Target::Delivery { response: changed },
            };
            assert!(target_order(target) != target_order(alternate));
        }
    }
    for changed in different_bindings(product) {
        assert!(
            target_order(targets[1])
                != target_order(Target::Increment {
                    claim: primary,
                    artifact: changed,
                })
        );
        assert!(
            target_order(targets[2])
                != target_order(Target::Artifact {
                    response: primary,
                    slot: 0,
                    artifact: changed,
                })
        );
    }
    assert!(
        target_order(targets[2])
            != target_order(Target::Artifact {
                response: primary,
                slot: 1,
                artifact: product,
            })
    );
    assert!(
        target_order(targets[3])
            != target_order(Target::MissingSlot {
                response: primary,
                slot: 1,
            })
    );
}

#[test]
fn actual_token_lookup_refuses_foreign_generation_target_and_complete_before_binding() {
    let seals = actual_seals(1);
    let seal = seals.first().unwrap();
    let previous = seal.previous();
    let mut visits = 1;
    let index = SealIndex::new(&seals, &mut visits).unwrap();
    assert_eq!(visits, 0);
    let key = fixture::key(1);
    let before = previous.binding();
    let target = previous.target();
    let mut visits = 1;
    assert!(std::ptr::eq(
        index.token(key, before, target, &mut visits).unwrap(),
        seal
    ));
    assert_eq!(visits, 0);
    let mut different = key;
    different.generation += 1;
    assert!(index.token(different, before, target, &mut 1).is_err());
    different = key;
    different.target = EvaluationTarget::Delivery {
        response: TestamentId::from_u128(1),
    };
    assert!(index.token(different, before, target, &mut 1).is_err());
    for changed in different_bindings(before) {
        assert!(index.token(key, changed, target, &mut 1).is_err());
    }
    let validation::Target::Admission { claim } = target else {
        panic!("actual Admission")
    };
    for changed in different_bindings(claim) {
        assert!(
            index
                .token(
                    key,
                    before,
                    validation::Target::Admission { claim: changed },
                    &mut 1,
                )
                .is_err()
        );
    }
    assert_eq!(seal.previous(), previous);
    assert!(index.token(key, before, target, &mut 1).is_ok());
}

#[test]
fn one_linear_validation_allows_logarithmic_probes_with_an_exact_shared_visit_limit() {
    let seals = actual_seals(15);
    let mut too_small = seals.len() - 1;
    assert!(matches!(
        SealIndex::new(&seals, &mut too_small),
        Err(NativeError::Capacity(_))
    ));
    assert_eq!(too_small, 0);
    let mut remaining = seals.len();
    let index = SealIndex::new(&seals, &mut remaining).unwrap();
    assert_eq!(remaining, 0);
    // Fifteen genuine independent members must fit a four-probe search, even
    // though validating the complete canonical slice took fifteen visits.
    let bound = usize::BITS as usize - seals.len().leading_zeros() as usize;
    assert_eq!(bound, 4);
    for (offset, expected) in seals.iter().enumerate() {
        let key = fixture::key(u32::try_from(offset + 1).unwrap());
        let target = expected.previous().target();
        let mut remaining = bound;
        assert!(std::ptr::eq(
            index
                .token(key, expected.before(), target, &mut remaining)
                .unwrap(),
            expected
        ));
        let required = bound - remaining;
        assert!(required > 0);
        let mut insufficient = required - 1;
        assert!(matches!(
            index.token(key, expected.before(), target, &mut insufficient),
            Err(NativeError::Capacity(_))
        ));
        assert_eq!(insufficient, 0);
    }
    let mut empty_visits = 0;
    let empty = SealIndex::new(&[], &mut empty_visits).unwrap();
    let sample = seals.first().unwrap();
    assert!(
        empty
            .token(
                fixture::key(1),
                sample.before(),
                sample.previous().target(),
                &mut empty_visits
            )
            .is_err()
    );
    assert_eq!(empty_visits, 0);
}
