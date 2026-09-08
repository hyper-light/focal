use super::*;
use focal_model::{Deadline, MonitorId, TimerId};

#[test]
fn held_work_report_releases_multiple_monitors_under_pressure_and_discard_restores_the_promise() {
    let mut f = checked_slot_fixture_with_visits(ValidationMode::Required, 2 * 1024 * 1024);
    f.commit(ISSUER, reports::creation(2, 2, &[], None).command);
    let observer = ClaimId::from_u128(2);
    for id in [10, 11] {
        let expected = f.owner.effective().claim(observer).unwrap().binding();
        f.commit(
            ISSUER,
            NativeCommand::RegisterMonitor {
                expected,
                receipt: None,
                id: MonitorId::from_u128(id),
                roots: vec![WaitPredicate::Terminal(CLAIM)],
                deadline: Deadline {
                    timer: TimerId::from_u128(id),
                    generation: 1,
                    at: 10_000,
                },
            },
        );
    }
    complete_response(&mut f, 900, 801);
    let key = EvaluationKey {
        claim: CLAIM,
        validation: ValidationId::from_u128(301),
        target: EvaluationTarget::Work {
            response: RESPONSE,
            slot: 0,
            artifact: ArtifactId::from_u128(801),
        },
        generation: 1,
    };
    let expected = f.owner.effective().evaluation(key).unwrap().binding();
    f.commit(
        EVALUATOR,
        NativeCommand::BeginWork {
            claim: f.claim(),
            key,
            expected,
        },
    );
    let original = f.owner.effective().claim(observer).unwrap().binding();
    let prefix = f.owner.effective().sequence();
    let pressure = exhaust(&f);
    for publish in [false, true] {
        let (actor, command) = report(&f, key, 8002, VerdictValue::Pass);
        let NativeStaging::Prepared { candidate, outcome } = f.stage(actor, command).unwrap()
        else {
            panic!("an actual held report produces a candidate");
        };
        assert_eq!(
            f.owner.effective().claim(CLAIM).unwrap().status(),
            ClaimStatus::Satisfied
        );
        let row = f.owner.effective().claim(observer).unwrap();
        assert_eq!(row.status(), ClaimStatus::Generated);
        assert_eq!(row.binding(), original.next().unwrap().next().unwrap());
        assert_eq!(row.response_count(), 0);
        assert!(
            row.scopes()
                .iter()
                .all(|scope| scope.release_cut().is_some())
        );
        assert_eq!(
            (0..outcome.events)
                .filter(|ordinal| matches!(
                    f.owner
                        .effective()
                        .event(outcome.sequence, *ordinal)
                        .unwrap()
                        .fact,
                    NativeFact::Claim(NativeClaimEvent {
                        kind: NativeEventKind::Monitor(NativeMonitorEvent::Released { .. }),
                        ..
                    })
                ))
                .count(),
            2
        );
        if publish {
            f.owner.publish_after_durable(candidate).unwrap();
        } else {
            f.owner.discard_from(candidate).unwrap();
            assert_eq!(f.owner.effective().sequence(), prefix);
            let row = f.owner.effective().claim(observer).unwrap();
            assert_eq!(row.binding(), original);
            assert!(row.scopes().iter().all(|scope| scope.active()));
        }
    }
    drop(pressure);
}
