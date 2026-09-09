//! Due-timer index rows (doc 22 §7): one per live claim deadline, monitor and
//! evaluation, retired by the transitions and deliveries that settle them,
//! visible through the due scan the node's sweep uses.
use super::*;
use crate::native::report_tests::{ISSUER, binding, context, creation, post, publish, request};
use focal_memory::{MemoryBudget, RangeConfig, RangeId};
use focal_model::{Deadline, MonitorId, TimerId, ValidationMode, WaitPredicate};

fn core_with_deadlines(claims: &[(u128, u64)]) -> Core<NativeState> {
    let mut core = Core::new_native(
        binding(1).ledger,
        RangeId(7911),
        NativeLimits {
            plan_nodes: 16,
            plan_edges: 65_536,
            preparation_bytes: 2 * 1024 * 1024,
            evaluations_per_claim: 32,
            range: RangeConfig {
                page_entries: 4,
                max_batch_entries: 256,
                ..RangeConfig::default()
            },
            ..NativeLimits::default()
        },
        MemoryBudget::new(192 * 1024 * 1024, 24 * 1024 * 1024).unwrap(),
    )
    .unwrap();
    let mut authored = Vec::new();
    let mut declarations = Vec::new();
    for &(id, at) in claims {
        let input = creation(1, id, &[(ValidationMode::Required, false)], None);
        let NativeCommand::Create {
            claims: mut proposals,
            declarations: definitions,
        } = input.command
        else {
            panic!("create");
        };
        proposals[0].definition.deadline = Some(Deadline {
            timer: TimerId::from_u128(10_000 + id),
            generation: 1,
            at,
        });
        authored.extend(proposals);
        declarations.extend(definitions);
    }
    publish(
        &mut core,
        10,
        NativeInput {
            request: request(ISSUER, 1),
            command: NativeCommand::Create {
                claims: authored,
                declarations,
            },
        },
    );
    core
}

fn due(core: &Core<NativeState>, through: u64) -> Vec<(u64, TimerTarget)> {
    core.native_index_scan(NativeIndexScan::Due { through }, None)
        .map(|hit| match hit {
            NativeIndexHit::Timer { at, target } => (at, target),
            other => panic!("{other:?}"),
        })
        .collect()
}

fn fire<T, E: std::fmt::Debug>(
    result: Result<NativeStaging, E>,
    f: impl FnOnce(NativeOutcome) -> T,
) -> (NativeCandidate, T) {
    match result.unwrap() {
        NativeStaging::Prepared { candidate, outcome } => (candidate, f(outcome)),
        existing => panic!("expected a fresh timer, got {existing:?}"),
    }
}

#[test]
fn claim_deadlines_are_indexed_at_creation_and_retired_by_their_delivery() {
    let core = core_with_deadlines(&[(1, 1_000), (2, 3_000)]);
    let one = ClaimId::from_u128(1);
    let two = ClaimId::from_u128(2);
    assert_eq!(
        due(&core, u64::MAX),
        vec![
            (1_000, TimerTarget::Claim(one)),
            (3_000, TimerTarget::Claim(two))
        ]
    );
    assert_eq!(due(&core, 999), vec![]);
    assert_eq!(due(&core, 1_000), vec![(1_000, TimerTarget::Claim(one))]);
    // Posting registers the admission evaluation and its declaration deadline.
    let mut core = core;
    publish(&mut core, 20, post(21, binding(1)));
    let admission = EvaluationKey {
        claim: one,
        validation: ValidationId::from_u128(101),
        target: EvaluationTarget::Admission,
        generation: 1,
    };
    let declaration_deadline = core
        .native_evaluation(admission)
        .unwrap()
        .bind(core.native_definition(admission.validation).unwrap())
        .unwrap()
        .deadline()
        .at;
    let timers = due(&core, u64::MAX);
    assert_eq!(timers.len(), 3);
    assert!(timers.contains(&(declaration_deadline, TimerTarget::Evaluation(admission))));

    // A terminal transition leaves the claim's timer in place: the timer fires
    // once on the terminal claim, records its outcome and retires the row.
    let mut owner = NativeOwner::new(core).unwrap();
    let cancel = NativeInput {
        request: request(ISSUER, 30),
        command: NativeCommand::Cancel {
            expected: owner.committed().claim(one).unwrap().binding(),
        },
    };
    let (candidate, _) = fire(owner.prepare(context(ISSUER, 40), cancel, None), |o| o);
    owner.publish_after_durable(candidate).unwrap();
    assert!(owner.committed().claim(one).unwrap().is_terminal());
    // Cancellation fenced the admission evaluation: its timer is gone.
    let timers = due(owner.committed_core(), u64::MAX);
    assert_eq!(
        timers,
        vec![
            (1_000, TimerTarget::Claim(one)),
            (3_000, TimerTarget::Claim(two))
        ]
    );
    let input = NativeClaimDeadlineInput {
        claim: one,
        deadline: owner.committed().claim(one).unwrap().deadline().unwrap(),
    };
    // Not yet due: refused, nothing changes.
    assert!(owner.prepare_claim_deadline(input, 999).is_err());
    let (candidate, outcome) = fire(owner.prepare_claim_deadline(input, 1_000), |o| o);
    assert_eq!(
        outcome.invocation,
        TimerTarget::Claim(one).invocation(input.deadline)
    );
    owner.publish_after_durable(candidate).unwrap();
    assert_eq!(
        due(owner.committed_core(), u64::MAX),
        vec![(3_000, TimerTarget::Claim(two))]
    );
    // An exact redelivery finds the recorded outcome and writes nothing.
    assert!(matches!(
        owner.prepare_claim_deadline(input, 5_000).unwrap(),
        NativeStaging::Existing { outcome: again, candidate: None } if again == outcome
    ));
    // The live claim's timer expires it and retires the row.
    let input = NativeClaimDeadlineInput {
        claim: two,
        deadline: owner.committed().claim(two).unwrap().deadline().unwrap(),
    };
    let (candidate, _) = fire(owner.prepare_claim_deadline(input, 3_000), |o| o);
    owner.publish_after_durable(candidate).unwrap();
    assert!(owner.committed().claim(two).unwrap().is_terminal());
    assert_eq!(due(owner.committed_core(), u64::MAX), vec![]);
}

#[test]
fn monitor_timers_follow_registration_cancellation_and_their_own_delivery() {
    let mut core = core_with_deadlines(&[(1, 9_000), (2, 9_000)]);
    publish(&mut core, 20, post(21, binding(1)));
    publish(&mut core, 20, post(22, binding(2)));
    let one = ClaimId::from_u128(1);
    let two = ClaimId::from_u128(2);
    let mut owner = NativeOwner::new(core).unwrap();
    let deadline = Deadline {
        timer: TimerId::from_u128(77),
        generation: 1,
        at: 5_000,
    };
    let monitor = MonitorId::from_u128(500);
    let register = crate::native::fixtures::register_monitor(
        request(ISSUER, 31),
        owner.committed().claim(one).unwrap().binding(),
        None,
        500,
        vec![WaitPredicate::Satisfied(two)],
        deadline,
    );
    let (candidate, _) = fire(owner.prepare(context(ISSUER, 40), register, None), |o| o);
    owner.publish_after_durable(candidate).unwrap();
    let timers = due(owner.committed_core(), u64::MAX);
    assert!(timers.contains(&(5_000, TimerTarget::Monitor(one, monitor))));
    // The monitor's deadline on a live owner with an unsettled root: the
    // delivery records its outcome and retires the row whatever it decides.
    let input = NativeMonitorDeadlineInput {
        claim: one,
        monitor,
        deadline,
    };
    assert!(owner.prepare_monitor_deadline(input, 4_999).is_err());
    let (candidate, outcome) = fire(owner.prepare_monitor_deadline(input, 5_000), |o| o);
    owner.publish_after_durable(candidate).unwrap();
    let timers = due(owner.committed_core(), u64::MAX);
    assert!(
        !timers
            .iter()
            .any(|(_, target)| matches!(target, TimerTarget::Monitor(..)))
    );
    assert!(matches!(
        owner.prepare_monitor_deadline(input, 6_000).unwrap(),
        NativeStaging::Existing { outcome: again, candidate: None } if again == outcome
    ));
}
