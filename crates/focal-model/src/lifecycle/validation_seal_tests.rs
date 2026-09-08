use super::*;

fn fail_claim(claim: &mut ClaimState, definitions: &[Declaration]) {
    let mut aggregate = aggregation::ClaimAggregation::new(claim, limits()).unwrap();
    aggregate.register(&ready(claim, &definitions[1])).unwrap();
    let blocker = ready(claim, &definitions[2]);
    aggregate.register(&blocker).unwrap();
    let failure = report(claim, begin(claim, blocker), VerdictValue::Fail);
    aggregate
        .apply_acceptance(SessionSeq(3), &[], &[failure.result.unwrap()])
        .unwrap();
    claim
        .apply_admission(&claim.binding(), &aggregate.admission())
        .unwrap();
    assert_eq!(claim.status(), ClaimStatus::PostFailed);
    assert_eq!(claim.local_sealed_at(), Some(SessionSeq(3)));
}

#[test]
fn seal_requires_actual_local_outcome_exact_owner_definition_and_expected_binding() {
    let definitions = definitions(ValidationMode::Observe, false, true);
    let mut claim = posted(&definitions);
    let evaluation = begin(&claim, ready(&claim, &definitions[1])).into_state();
    let original = evaluation;
    assert!(
        evaluation
            .seal_claim(&definitions[1], &evaluation.binding(), &claim)
            .is_err()
    );
    fail_claim(&mut claim, &definitions);
    let foreign =
        ClaimState::generate(Principal::Actor(ISSUER), claim::tests::definition(4)).unwrap();
    assert!(
        evaluation
            .seal_claim(&definitions[1], &evaluation.binding(), &foreign)
            .is_err()
    );
    let alternate = super::definitions(ValidationMode::Observe, true, true);
    let mut alternate_claim = posted(&alternate);
    fail_claim(&mut alternate_claim, &alternate);
    assert!(
        evaluation
            .seal_claim(&alternate[1], &evaluation.binding(), &claim)
            .is_err()
    );
    assert!(
        evaluation
            .seal_claim(&definitions[1], &evaluation.binding(), &alternate_claim)
            .is_err()
    );
    for expected in [
        evaluation.binding().next().unwrap(),
        Binding {
            object: binding(999).object,
            ..evaluation.binding()
        },
        Binding {
            content: ContentHash([99; 32]),
            ..evaluation.binding()
        },
    ] {
        assert!(
            evaluation
                .seal_claim(&definitions[1], &expected, &claim)
                .is_err()
        );
    }
    assert_eq!(evaluation, original);
    let transition = crate::lifecycle::memory::fail_after(0, || {
        evaluation.seal_claim(&definitions[1], &evaluation.binding(), &claim)
    })
    .unwrap();
    assert!(transition.changed());
    assert_eq!(transition.before(), evaluation.binding());
    assert_eq!(transition.previous(), evaluation);
    transition.check(&evaluation, &transition.next()).unwrap();
}

#[test]
fn ready_seal_records_suppression_without_replacing_existing_suppression_or_fence() {
    let definitions = definitions(ValidationMode::Observe, false, true);
    let mut claim = posted(&definitions);
    let ready = ready(&claim, &definitions[1]);
    let begun = begin(&claim, ready);
    let fenced = ready
        .fence_deadline(
            &ready.binding(),
            &claim,
            ready.deadline(),
            ready.deadline().at,
            claim::ClaimCut {
                position: SessionSeq(2),
                cause: ContentHash([98; 32]),
            },
        )
        .unwrap();
    fail_claim(&mut claim, &definitions);
    let mut parent = begun.admission_report_owner(&claim, 2).unwrap();
    parent.evaluation = ready.binding();
    let suppressed = ready
        .begin(Principal::Actor(EVALUATOR), &ready.binding(), &parent)
        .unwrap()
        .next;
    assert!(matches!(
        suppressed.suppression(),
        Some(Suppression::ParentFailure(_))
    ));
    for before in [ready.into_state(), suppressed.into_state(), fenced] {
        let transition = before
            .seal_claim(&definitions[1], &before.binding(), &claim)
            .unwrap();
        let after = transition.next();
        assert!(transition.changed());
        assert_eq!(after.binding(), before.binding().next().unwrap());
        assert_eq!(after.state(), State::Ready);
        assert!(!after.has_begun());
        assert_eq!(after.last_result(), None);
        assert_eq!(after.fence(), before.fence());
        let before = before.bind(&definitions[1]).unwrap();
        let after = after.bind(&definitions[1]).unwrap();
        assert_eq!(
            after.suppression(),
            before
                .suppression()
                .or(Some(Suppression::CohortSealed(after.sealed().unwrap())))
        );
        assert!(after.audit_finished());
        assert!(after.admission_owner(&claim, 2).is_err());
    }
}

#[test]
fn seal_preserves_retry_and_quality_attempts_and_the_original_accepted_proof() {
    for quality in [false, true] {
        let definitions = definitions(ValidationMode::Observe, quality, true);
        let mut claim = posted(&definitions);
        let begun = begin(&claim, ready(&claim, &definitions[1]));
        let reported = report(
            &claim,
            begun,
            if quality {
                VerdictValue::Pass
            } else {
                VerdictValue::Error
            },
        );
        let accepted = reported.result.unwrap();
        let before = reported.next;
        let attempt = before.current_attempt().unwrap();
        let accepted_binding = accepted.binding();
        fail_claim(&mut claim, &definitions);
        let transition = before
            .into_state()
            .seal_claim(&definitions[1], &before.binding(), &claim)
            .unwrap();
        let after = transition.next().bind(&definitions[1]).unwrap();
        assert_eq!(after.current_attempt().unwrap(), attempt);
        assert_eq!(after.current_phase(), before.current_phase());
        assert_eq!(after.last_result(), Some(accepted));
        assert_eq!(after.last_result().unwrap().binding(), accepted_binding);
        assert_eq!(after.target(), before.target());
        assert_eq!(after.receipt(), before.receipt());
        assert_eq!(after.generation(), before.generation());
        assert_eq!(after.suppression(), before.suppression());
        assert!(!after.audit_finished());
        let next = report(&claim, after, VerdictValue::Pass);
        assert_eq!(next.result.unwrap().attempt(), Some(attempt.index));
        assert_eq!(next.result.unwrap().phase(), attempt.phase);
        assert!(next.result.unwrap().is_terminal());
        if quality {
            assert_eq!(
                next.result.unwrap().programmatic_evidence(),
                accepted.programmatic_evidence()
            );
        }
        assert_eq!(claim.status(), ClaimStatus::PostFailed);
        assert_eq!(claim.local_sealed_at(), Some(SessionSeq(3)));
    }
}

#[test]
fn original_seal_is_stable_across_later_claim_revision_and_terminal_rows_are_exact_noops() {
    let definitions = definitions(ValidationMode::Observe, false, true);
    let mut claim = posted(&definitions);
    let begun = begin(&claim, ready(&claim, &definitions[1]));
    let terminal = report(&claim, begun, VerdictValue::Pass).next.into_state();
    fail_claim(&mut claim, &definitions);
    let before = begun.into_state();
    let sealed = before
        .seal_claim(&definitions[1], &before.binding(), &claim)
        .unwrap();
    let original_cut = claim.terminal_cut();
    let original_binding = claim.binding();
    let snapshot = graph(&claim);
    let release = scope::Registry::prepare_release_owner(
        &claim,
        &snapshot,
        &[],
        claim::ClaimCut {
            position: SessionSeq(4),
            cause: ContentHash([97; 32]),
        },
    )
    .unwrap();
    claim.apply_scope(&claim.binding(), release, &[]).unwrap();
    assert_ne!(claim.binding(), original_binding);
    assert_eq!(claim.terminal_cut(), original_cut);
    assert_eq!(claim.local_sealed_at(), Some(SessionSeq(3)));
    let repeated = before
        .seal_claim(&definitions[1], &before.binding(), &claim)
        .unwrap();
    assert_eq!(repeated.next(), sealed.next());
    for source in [sealed.next(), terminal] {
        let noop = source
            .seal_claim(&definitions[1], &source.binding(), &claim)
            .unwrap();
        assert!(!noop.changed());
        assert_eq!(noop.previous(), source);
        assert_eq!(noop.next(), source);
        noop.check(&source, &source).unwrap();
        assert!(
            source
                .seal_claim(&definitions[1], &source.binding().next().unwrap(), &claim)
                .is_err()
        );
    }
    assert_eq!(terminal.sealed(), None);
}

#[test]
fn local_completion_seal_keeps_its_identity_after_a_later_graph_terminal_cut() {
    let definitions = definitions(ValidationMode::Observe, false, false);
    let mut claim = posted(&definitions);
    let before = begin(&claim, ready(&claim, &definitions[1])).into_state();
    // The graph-algebra fixture supplies the retained local-completion fact;
    // the later release below consumes the real complete graph witness.
    claim::tests::local_projection_for_graph(&mut claim);
    assert!(!claim.is_terminal());
    let first_seal = claim.local_sealed_at().unwrap();
    let local = before
        .seal_claim(&definitions[1], &before.binding(), &claim)
        .unwrap();
    let snapshot = graph(&claim);
    let release = snapshot.release(ClaimId(claim.binding().object.0)).unwrap();
    let terminal_position = SessionSeq(first_seal.0 + 5);
    claim
        .graph_release(&claim.binding(), &release, &[], terminal_position)
        .unwrap();
    assert!(claim.is_terminal());
    assert_eq!(claim.local_sealed_at(), Some(first_seal));
    assert!(matches!(
        claim.terminal_cut(),
        Some(claim::ClaimTerminalCut::Explicit(cut)) if cut.position == terminal_position
    ));
    let later = before
        .seal_claim(&definitions[1], &before.binding(), &claim)
        .unwrap();
    assert_eq!(later.next(), local.next());
    local.check(&before, &later.next()).unwrap();
}

#[test]
fn checked_seal_rejects_modified_attempt_evidence_fence_and_source_state() {
    let definitions = definitions(ValidationMode::Observe, true, true);
    let mut claim = posted(&definitions);
    let before = report(
        &claim,
        begin(&claim, ready(&claim, &definitions[1])),
        VerdictValue::Pass,
    )
    .next
    .into_state();
    fail_claim(&mut claim, &definitions);
    let seal = before
        .seal_claim(&definitions[1], &before.binding(), &claim)
        .unwrap();
    let after = seal.next();
    let mut altered = after;
    altered.last_result = None;
    assert!(seal.check(&before, &altered).is_err());
    altered = after;
    altered.attempt = altered.attempt.checked_add(1).unwrap();
    assert!(seal.check(&before, &altered).is_err());
    altered = after;
    altered.fence = Some(AuthorityFence {
        reason: FenceReason::Evaluation,
        cause: ContentHash([96; 32]),
    });
    assert!(seal.check(&before, &altered).is_err());
    altered = after;
    altered.target = Target::Admission {
        claim: binding(999),
    };
    assert!(seal.check(&before, &altered).is_err());
    let mut source = before;
    source.programmatic_evidence = None;
    assert!(seal.check(&source, &after).is_err());
    assert!(seal.check(&before, &before).is_err());
    seal.check(&before, &after).unwrap();
}

#[test]
fn legacy_seal_refuses_an_empty_cause_without_changing_a_begun_attempt() {
    let definitions = definitions(ValidationMode::Observe, false, false);
    let claim = posted(&definitions);
    let begun = begin(&claim, ready(&claim, &definitions[1]));
    let mut owner = begun.admission_report_owner(&claim, 2).unwrap();
    owner.cohort = Cohort::Sealed {
        cause: ContentHash([0; 32]),
    };
    assert!(begun.record_seal(&begun.binding(), &owner).is_err());
    assert!(begun.sealed().is_none());
    assert!(begun.current_attempt().is_ok());
}
