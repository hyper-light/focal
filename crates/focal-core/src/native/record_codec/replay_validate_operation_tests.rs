use super::*;
use crate::native::report_tests as fixture;
use focal_model::{ValidationMode, lifecycle::succession::CorrectionKind};

fn checked_events(core: &Core<NativeState>, outcome: NativeOutcome) -> Vec<NativeFact> {
    let facts = fixture::events(core, outcome);
    for fact in &facts {
        check(outcome.operation, *fact).unwrap();
    }
    facts
}

#[test]
fn admission_materialization_is_recorded_by_actual_post_not_creation() {
    let mut core = fixture::core();
    let created = fixture::publish(
        &mut core,
        10,
        fixture::creation(1, 1, &[(ValidationMode::Required, true)], None),
    );
    let creation = checked_events(&core, created);
    assert!(!creation.iter().any(|fact| matches!(
        fact,
        NativeFact::Evaluation {
            kind: NativeEvaluationEventKind::Materialized,
            ..
        }
    )));
    let posted = fixture::publish(&mut core, 20, fixture::post(2, fixture::binding(1)));
    let facts = checked_events(&core, posted);
    let mut materialized = 0;
    for fact in facts {
        if matches!(
            fact,
            NativeFact::Evaluation {
                kind: NativeEvaluationEventKind::Materialized,
                key: EvaluationKey {
                    target: EvaluationTarget::Admission,
                    ..
                },
                ..
            }
        ) {
            materialized += 1;
            assert!(check(NativeOperation::Create, fact).is_err());
        }
    }
    assert!(materialized > 0);
}

#[test]
fn real_cancellation_and_supersession_fences_keep_their_original_operation() {
    for supersedes in [false, true] {
        let mut core = fixture::running(&[(ValidationMode::Required, false)]);
        let input = if supersedes {
            fixture::creation(4, 2, &[], Some(CorrectionKind::Supersedes))
        } else {
            NativeInput {
                request: fixture::request(fixture::ISSUER, 4),
                command: NativeCommand::Cancel {
                    expected: core.native_claim(ClaimId::from_u128(1)).unwrap().binding(),
                },
            }
        };
        let outcome = fixture::publish(&mut core, 40, input);
        let facts = checked_events(&core, outcome);
        let mut fenced = 0;
        for fact in facts {
            if matches!(
                fact,
                NativeFact::Evaluation {
                    kind: NativeEvaluationEventKind::AuthorityFenced,
                    ..
                }
            ) {
                fenced += 1;
                // Adding the actual owner-control producers must not make an
                // unrelated posting or evaluator begin a fencing operation.
                assert!(check(NativeOperation::Post, fact).is_err());
                assert!(check(NativeOperation::BeginAdmission, fact).is_err());
            }
        }
        assert!(fenced > 0);
    }
}
