//! Native timers are exercised only after real monitor registration commands.
use super::*;
use focal_model::{MonitorId, WaitPredicate};

fn timer(claim: u128, monitor: u128, at: u64) -> NativeMonitorDeadlineInput {
    NativeMonitorDeadlineInput {
        claim: ClaimId::from_u128(claim),
        monitor: MonitorId::from_u128(monitor),
        deadline: Deadline {
            timer: TimerId::from_u128(90_000 + monitor),
            generation: 1,
            at,
        },
    }
}
fn register_timer(
    owner: &mut NativeOwner,
    input: NativeMonitorDeadlineInput,
    target: u128,
    time: u64,
) {
    let source = owner.effective().claim(input.claim).unwrap();
    let command = NativeCommand::RegisterMonitor {
        expected: source.binding(),
        receipt: source.receipt().map(|receipt| receipt.fence),
        id: input.monitor,
        roots: vec![WaitPredicate::Terminal(ClaimId::from_u128(target))],
        deadline: input.deadline,
    };
    let candidate = stage(
        owner,
        NativeInput {
            request: request(ISSUER, 70_000 + u128::from(time)),
            command,
        },
        time,
    );
    owner.publish_after_durable(candidate).unwrap();
}
fn monitor_fire(
    owner: &mut NativeOwner,
    input: NativeMonitorDeadlineInput,
    time: u64,
) -> (NativeCandidate, NativeOutcome) {
    match owner.prepare_monitor_deadline(input, time).unwrap() {
        NativeStaging::Prepared { candidate, outcome } => (candidate, outcome),
        other => panic!("fresh monitor timer: {other:?}"),
    }
}

#[test]
fn earlier_actual_monitor_expiry_fences_checks_and_retries_exactly_without_testimony() {
    let core = authored_posted(&[
        (1, 500, &[(ValidationMode::Observe, false)], &[]),
        (2, 500, &[], &[]),
    ]);
    let mut owner = NativeOwner::new(core).unwrap();
    let input = timer(1, 1, 100);
    register_timer(&mut owner, input, 2, 25);
    begin_claim(&mut owner, 1, 1, 31);
    let before = owner
        .committed()
        .claim(input.claim)
        .unwrap()
        .try_copy(usize::MAX)
        .unwrap();
    let previous = *owner.committed().evaluation(claim_key(1, 1)).unwrap();
    let report = report_claim(&owner, 1, 1, 41, VerdictValue::Pass);
    let (candidate, outcome) = monitor_fire(&mut owner, input, 100);
    assert_eq!(outcome.operation, NativeOperation::MonitorDeadline);
    assert_eq!(
        outcome.invocation,
        NativeInvocation::MonitorDeadline(input.key())
    );
    assert_eq!(
        (
            outcome.responses,
            outcome.artifacts,
            outcome.results,
            outcome.result_testaments
        ),
        (0, 0, 0, 0)
    );
    let after = owner.effective().claim(input.claim).unwrap();
    assert_eq!(after.status(), ClaimStatus::Expired);
    assert_eq!(after.deadline(), before.deadline());
    assert_eq!(after.scopes(), before.scopes());
    assert_eq!(after.response_count(), before.response_count());
    assert_eq!(after.latest_response(), before.latest_response());
    assert_eq!(after.receipt(), before.receipt());
    assert_eq!(owner.committed().claim(input.claim).unwrap(), &before);
    assert_eq!(
        owner
            .effective()
            .claim(ClaimId::from_u128(2))
            .unwrap()
            .status(),
        ClaimStatus::Posted
    );
    let next = owner.effective().evaluation(claim_key(1, 1)).unwrap();
    assert_eq!(next.last_result(), previous.last_result());
    assert_eq!(
        next.fence().unwrap().reason,
        validation::FenceReason::Expiry
    );
    let mut store = Store::new();
    assert!(
        owner
            .prepare_with_custody(
                context(EVALUATOR, 101),
                report,
                &mut store.content,
                DOMAIN,
                &BuiltinNativeSchemas,
            )
            .is_err()
    );
    assert!(
        matches!(owner.prepare_monitor_deadline(input, 0).unwrap(), NativeStaging::Existing { outcome: found, .. } if found == outcome)
    );
    assert!(
        owner
            .prepare_monitor_deadline(
                NativeMonitorDeadlineInput {
                    deadline: Deadline {
                        at: 101,
                        ..input.deadline
                    },
                    ..input
                },
                101
            )
            .is_err()
    );
    assert_eq!(owner.discard_from(candidate).unwrap(), 1);
    assert_eq!(owner.effective().claim(input.claim).unwrap(), &before);
    assert_eq!(
        owner.effective().evaluation(claim_key(1, 1)).unwrap(),
        &previous
    );
    let (candidate, replay) = monitor_fire(&mut owner, input, 100);
    assert_eq!(replay, outcome);
    owner.publish_after_durable(candidate).unwrap();
    assert!(
        matches!(owner.prepare_monitor_deadline(input, 0).unwrap(), NativeStaging::Existing { outcome: found, .. } if found == outcome)
    );
}

#[test]
fn actual_monitor_cycle_selects_older_victim_then_releases_trigger_wait_without_expiry() {
    let core = authored_posted(&[
        (1, 500, &[(ValidationMode::Observe, false)], &[]),
        (2, 500, &[], &[]),
    ]);
    let mut owner = NativeOwner::new(core).unwrap();
    let earlier = timer(1, 1, 100);
    let input = timer(2, 2, 100);
    register_timer(&mut owner, earlier, 2, 25);
    register_timer(&mut owner, input, 1, 26);
    begin_claim(&mut owner, 1, 1, 31);
    let previous = *owner.committed().evaluation(claim_key(1, 1)).unwrap();
    let trigger_before = owner.committed().claim(input.claim).unwrap().binding();
    let (candidate, outcome) = monitor_fire(&mut owner, input, 100);
    let victim = owner.effective().claim(ClaimId::from_u128(1)).unwrap();
    assert_eq!(victim.status(), ClaimStatus::Deadlocked);
    let ClaimTerminalCut::Graph(cut) = victim.terminal_cut().unwrap() else {
        panic!("graph cut")
    };
    assert_eq!(cut.origin().binding(), trigger_before);
    assert_eq!(cut.deadline(), Some(input.deadline));
    assert_eq!(cut.fired_at(), Some(100));
    let trigger = owner.effective().claim(input.claim).unwrap();
    assert_eq!(trigger.status(), ClaimStatus::Posted);
    let scope = trigger
        .scopes()
        .iter()
        .find(|scope| scope.id() == input.monitor)
        .unwrap();
    assert_eq!(scope.release_cut().unwrap().position, outcome.sequence);
    assert!(scope.cancellation().is_none());
    assert!(!trigger.released());
    assert!(victim.scopes().iter().all(|scope| scope.active()));
    let next = owner.effective().evaluation(claim_key(1, 1)).unwrap();
    assert_eq!(next.fence(), previous.fence());
    assert_eq!(next.last_result(), previous.last_result());
    assert!(next.has_begun());
    assert_eq!(
        (outcome.responses, outcome.artifacts, outcome.results),
        (0, 0, 0)
    );
    owner.publish_after_durable(candidate).unwrap();
    let mut store = Store::new();
    let report = report_claim(&owner, 1, 1, 91, VerdictValue::Pass);
    let candidate = report_at(&mut owner, &mut store, report, 101);
    owner.publish_after_durable(candidate).unwrap();
    assert_eq!(
        owner
            .committed()
            .claim(ClaimId::from_u128(1))
            .unwrap()
            .status(),
        ClaimStatus::Deadlocked
    );
}

#[test]
fn inactive_monitor_consumes_only_its_exact_timer_and_retains_original_terminal_cut() {
    let core = authored_posted(&[(1, 500, &[], &[]), (2, 500, &[], &[])]);
    let mut owner = NativeOwner::new(core).unwrap();
    let input = timer(1, 1, 100);
    register_timer(&mut owner, input, 2, 25);
    let expected = owner.effective().claim(input.claim).unwrap().binding();
    let candidate = stage(
        &mut owner,
        NativeInput {
            request: request(ISSUER, 91),
            command: NativeCommand::Cancel { expected },
        },
        40,
    );
    owner.publish_after_durable(candidate).unwrap();
    let before = owner
        .committed()
        .claim(input.claim)
        .unwrap()
        .try_copy(usize::MAX)
        .unwrap();
    for bad in [
        NativeMonitorDeadlineInput {
            monitor: MonitorId::from_u128(99),
            ..input
        },
        NativeMonitorDeadlineInput {
            deadline: Deadline {
                generation: 2,
                ..input.deadline
            },
            ..input
        },
        NativeMonitorDeadlineInput {
            deadline: Deadline {
                at: 99,
                ..input.deadline
            },
            ..input
        },
    ] {
        assert!(owner.prepare_monitor_deadline(bad, 100).is_err());
    }
    assert!(owner.prepare_monitor_deadline(input, 99).is_err());
    let (candidate, outcome) = monitor_fire(&mut owner, input, 100);
    assert_eq!(
        (
            outcome.changed,
            outcome.events,
            outcome.evaluations,
            outcome.responses
        ),
        (0, 0, 0, 0)
    );
    assert_eq!(owner.effective().claim(input.claim).unwrap(), &before);
    owner.publish_after_durable(candidate).unwrap();
    assert_eq!(owner.committed().claim(input.claim).unwrap(), &before);
    assert!(
        matches!(owner.prepare_monitor_deadline(input, 0).unwrap(), NativeStaging::Existing { outcome: found, .. } if found == outcome)
    );
}

#[test]
fn later_monitor_cannot_skip_an_earlier_due_claim_deadline_or_consume_its_generation() {
    let core = authored_posted(&[(1, 50, &[], &[]), (2, 500, &[], &[])]);
    let mut owner = NativeOwner::new(core).unwrap();
    let input = timer(1, 1, 100);
    register_timer(&mut owner, input, 2, 25);
    let before = owner
        .committed()
        .claim(input.claim)
        .unwrap()
        .try_copy(usize::MAX)
        .unwrap();
    assert!(owner.prepare_monitor_deadline(input, 100).is_err());
    assert_eq!(owner.effective().claim(input.claim).unwrap(), &before);
    assert_eq!(owner.committed().claim(input.claim).unwrap(), &before);
}

#[test]
fn monitor_can_expire_a_claim_without_an_authored_claim_deadline_and_fence_its_checks() {
    let mut core = core();
    core.limits.plan_edges = 65_536;
    core.limits.preparation_bytes = 2 * 1024 * 1024;
    publish(
        &mut core,
        10,
        creation(1, 1, &[(ValidationMode::Observe, false)], None),
    );
    publish(&mut core, 11, creation(2, 2, &[], None));
    publish(&mut core, 20, post(21, binding(1)));
    publish(&mut core, 20, post(22, binding(2)));
    let mut owner = NativeOwner::new(core).unwrap();
    let input = timer(1, 1, 100);
    register_timer(&mut owner, input, 2, 25);
    begin_claim(&mut owner, 1, 1, 31);
    let before = *owner.committed().evaluation(claim_key(1, 1)).unwrap();
    assert_eq!(
        owner.committed().claim(input.claim).unwrap().deadline(),
        None
    );
    let (candidate, outcome) = monitor_fire(&mut owner, input, 100);
    let expired = owner.effective().claim(input.claim).unwrap();
    assert_eq!(expired.status(), ClaimStatus::Expired);
    assert_eq!(expired.deadline(), None);
    assert_eq!(expired.response_count(), 0);
    let next = owner.effective().evaluation(claim_key(1, 1)).unwrap();
    assert_eq!(next.last_result(), before.last_result());
    assert_eq!(
        next.fence().unwrap().reason,
        validation::FenceReason::Expiry
    );
    assert_eq!(next.fence().unwrap().cause, outcome.intent);
    assert_eq!(
        (outcome.responses, outcome.artifacts, outcome.results),
        (0, 0, 0)
    );
    owner.publish_after_durable(candidate).unwrap();
}

#[test]
fn ordinary_claim_timer_keeps_scc_precedence_across_monitor_release_and_fresh_trigger_revision() {
    let core = authored_posted(&[
        (1, 500, &[(ValidationMode::Observe, false)], &[]),
        (2, 100, &[(ValidationMode::Observe, false)], &[]),
    ]);
    let mut owner = NativeOwner::new(core).unwrap();
    register_timer(&mut owner, timer(1, 1, 200), 2, 25);
    register_timer(&mut owner, timer(2, 2, 200), 1, 26);
    begin_claim(&mut owner, 1, 1, 31);
    begin_claim(&mut owner, 2, 1, 32);
    let victim_before = owner
        .committed()
        .claim(ClaimId::from_u128(1))
        .unwrap()
        .binding();
    let trigger_before = owner
        .committed()
        .claim(ClaimId::from_u128(2))
        .unwrap()
        .binding();
    let original_victim_check = *owner.committed().evaluation(claim_key(1, 1)).unwrap();
    let input = super::input(&owner, 2);
    let (candidate, outcome) = super::fire(&mut owner, input, 100);
    let victim = owner.effective().claim(ClaimId::from_u128(1)).unwrap();
    let trigger = owner.effective().claim(ClaimId::from_u128(2)).unwrap();
    assert_eq!(victim.status(), ClaimStatus::Deadlocked);
    assert_eq!(trigger.status(), ClaimStatus::Expired);
    assert_eq!(
        victim.binding(),
        victim_before.next().unwrap().next().unwrap()
    );
    assert_eq!(
        trigger.binding(),
        trigger_before.next().unwrap().next().unwrap()
    );
    let ClaimTerminalCut::Graph(cut) = victim.terminal_cut().unwrap() else {
        panic!("deadlock cut")
    };
    assert_eq!(cut.origin().binding(), trigger_before);
    assert_eq!(cut.deadline(), Some(input.deadline));
    for claim in [victim, trigger] {
        assert!(!claim.released());
        assert!(claim.scopes().iter().all(|scope| {
            scope
                .release_cut()
                .is_some_and(|cut| cut.position == outcome.sequence)
        }));
        assert_eq!(claim.response_count(), 0);
    }
    assert_eq!(
        owner
            .effective()
            .evaluation(claim_key(1, 1))
            .unwrap()
            .fence(),
        original_victim_check.fence()
    );
    assert_eq!(
        owner
            .effective()
            .evaluation(claim_key(2, 1))
            .unwrap()
            .fence()
            .unwrap()
            .reason,
        validation::FenceReason::Expiry
    );
    assert_eq!(
        (outcome.responses, outcome.artifacts, outcome.results),
        (0, 0, 0)
    );
    assert_eq!(
        owner
            .committed()
            .claim(ClaimId::from_u128(1))
            .unwrap()
            .binding(),
        victim_before
    );
    assert_eq!(
        owner
            .committed()
            .claim(ClaimId::from_u128(2))
            .unwrap()
            .binding(),
        trigger_before
    );
    owner.publish_after_durable(candidate).unwrap();
}
#[path = "monitor_commands_tests.rs"]
mod command_tests;
