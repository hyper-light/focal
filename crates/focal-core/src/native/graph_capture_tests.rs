//! Real owner histories distinguish one frozen batch from subsequent SCC
//! recaptures. The original source journals, not a chosen test cut, set markers.
use super::*;

fn captures(owner: &NativeOwner, outcome: NativeOutcome) -> Vec<(u32, NativeClaimEvent)> {
    (0..outcome.events)
        .filter_map(|ordinal| {
            let event = owner.effective().event(outcome.sequence, ordinal).unwrap();
            event
                .claim_event()
                .filter(|claim| claim.graph.is_some())
                .map(|claim| (ordinal, claim))
        })
        .collect()
}

#[test]
fn one_frozen_dependency_batch_retains_a_common_capture_before_its_first_consequence() {
    let core = authored_posted(&[
        (1, 100, &[], &[]),
        (2, 200, &[], &[(graph::Kind::DependsOn, 1)]),
        (3, 200, &[], &[(graph::Kind::DependsOn, 1)]),
    ]);
    let mut owner = NativeOwner::new(core).unwrap();
    let timer = input(&owner, 1);
    let (candidate, outcome) = fire(&mut owner, timer, 100);
    let facts = captures(&owner, outcome);
    assert_eq!(facts.len(), 3);
    assert_eq!(
        (facts[0].0, facts[0].1.kind, facts[0].1.graph),
        (
            0,
            NativeEventKind::Expired,
            Some(NativeGraphCapture { before_ordinal: 0 })
        )
    );
    for (ordinal, claim) in &facts[1..] {
        assert_eq!(claim.kind, NativeEventKind::DependencyFailed);
        assert_eq!(claim.graph, Some(NativeGraphCapture { before_ordinal: 1 }));
        assert!(*ordinal >= 1);
    }
    assert_ne!(facts[1].0, facts[2].0);
    owner.discard_from(candidate).unwrap();
    let (candidate, retry) = fire(&mut owner, timer, 100);
    assert_eq!(retry, outcome);
    assert_eq!(captures(&owner, retry), facts);
    owner.publish_after_durable(candidate).unwrap();
    for (ordinal, claim) in facts {
        assert_eq!(
            owner
                .committed()
                .event(outcome.sequence, ordinal)
                .unwrap()
                .claim_event(),
            Some(claim)
        );
    }
}

#[test]
fn deadlock_then_dependency_then_expiry_record_each_actual_snapshot_boundary() {
    let mut owner = NativeOwner::new(cycle_core()).unwrap();
    let trigger = owner
        .committed()
        .claim(ClaimId::from_u128(2))
        .unwrap()
        .binding();
    let timer = input(&owner, 2);
    let (candidate, outcome) = fire(&mut owner, timer, 500);
    let facts = captures(&owner, outcome);
    assert_eq!(facts.len(), 3);
    assert_eq!(
        facts
            .iter()
            .map(|(ordinal, event)| (*ordinal, event.kind, event.graph.unwrap().before_ordinal))
            .collect::<Vec<_>>(),
        vec![
            (0, NativeEventKind::Deadlocked, 0),
            (1, NativeEventKind::DependencyFailed, 1),
            (2, NativeEventKind::Expired, 2)
        ]
    );
    let victim = owner
        .effective()
        .claim(ClaimId(facts[0].1.after.object.0))
        .unwrap();
    let Some(ClaimTerminalCut::Graph(cut)) = victim.terminal_cut() else {
        panic!("deadlock cut");
    };
    assert_eq!(cut.origin().binding(), trigger);
    assert_ne!(cut.origin().binding().object, victim.binding().object);
    assert_eq!(cut.deadline(), Some(timer.deadline));
    assert_eq!(cut.fired_at(), Some(500));
    owner.publish_after_durable(candidate).unwrap();
}
