use crate::native::report_tests as f;
use crate::native::*;
use focal_memory::{BudgetKind, BudgetLane};
use focal_model::lifecycle::{
    artifact_descriptor::ResultProvenance, creation::Owner, succession::Lineage,
};
use focal_model::{ArtifactRef, Cause, Confidence, OutcomeKind, ValidationMode, VerdictValue};

#[path = "scope_release_journal_tests.rs"]
mod journal_tests;

const ROOT: ClaimId = ClaimId::from_u128(1);

fn release(expected: Binding, actor: ParticipantId, request: u128) -> NativeInput {
    NativeInput {
        request: f::request(actor, request),
        command: NativeCommand::ReleaseScope { expected },
    }
}

fn stage(owner: &mut NativeOwner, input: NativeInput) -> (NativeCandidate, NativeOutcome) {
    let time = owner.effective().logical_time().max(100);
    let NativeStaging::Prepared { candidate, outcome } = owner
        .prepare(f::context(input.request.principal, time), input, None)
        .unwrap()
    else {
        panic!("fresh transaction")
    };
    (candidate, outcome)
}

fn commit(owner: &mut NativeOwner, input: NativeInput) -> NativeOutcome {
    let (candidate, outcome) = stage(owner, input);
    assert_eq!(owner.publish_after_durable(candidate).unwrap(), outcome);
    outcome
}

fn refused(owner: &mut NativeOwner, input: NativeInput) {
    let budget = owner.budget_stats();
    let range = owner.range_stats();
    let pending = owner.pending_len();
    let sequence = owner.effective().sequence();
    let time = owner.effective().logical_time().max(100);
    assert!(
        owner
            .prepare(f::context(input.request.principal, time), input, None)
            .is_err()
    );
    assert_eq!(owner.budget_stats(), budget);
    assert_eq!(owner.range_stats(), range);
    assert_eq!(owner.pending_len(), pending);
    assert_eq!(owner.effective().sequence(), sequence);
}

fn binding(owner: &NativeOwner, id: u128) -> Binding {
    owner
        .effective()
        .claim(ClaimId::from_u128(id))
        .unwrap()
        .binding()
}

fn copy(owner: &NativeOwner, id: u128) -> ClaimState {
    let claim = owner.effective().claim(ClaimId::from_u128(id)).unwrap();
    claim.try_copy(claim.copy_charge().unwrap()).unwrap()
}

fn same_business(current: &ClaimState, original: &ClaimState) {
    assert_eq!(current.status(), original.status());
    assert_eq!(current.receipt(), original.receipt());
    assert_eq!(current.terminal_cut(), original.terminal_cut());
    assert_eq!(current.local_sealed_at(), original.local_sealed_at());
    assert_eq!(current.local_complete(), original.local_complete());
    assert_eq!(current.latest_response(), original.latest_response());
    assert_eq!(current.response_count(), original.response_count());
    assert_eq!(current.created(), original.created());
    assert_eq!(current.lineage(), original.lineage());
    assert_eq!(current.acceptance(), original.acceptance());
    assert_eq!(current.graph(), original.graph());
    assert_eq!(current.scopes().children(), original.scopes().children());
}

fn assert_release(owner: &NativeOwner, original: &ClaimState, outcome: NativeOutcome) {
    let current = owner
        .effective()
        .claim(ClaimId(original.binding().object.0))
        .unwrap();
    assert_eq!(current.binding(), original.binding().next().unwrap());
    assert!(current.released());
    let cut = current.scopes().release_cut().unwrap();
    assert_eq!(cut.position, outcome.sequence);
    assert_ne!(cut.cause, ContentHash([0; 32]));
    same_business(current, original);
    assert_eq!(outcome.changed, 1);
    assert_eq!(
        (
            outcome.created,
            outcome.responses,
            outcome.result_testaments,
            outcome.artifacts,
            outcome.results,
            outcome.receipts,
            outcome.evaluations
        ),
        (0, 0, 0, 0, 0, 0, 0)
    );
}

fn tree() -> NativeOwner {
    let mut core = f::core();
    core.limits.plan_edges = 65_536;
    let mut claims = Vec::new();
    let mut declarations = Vec::new();
    for id in 1..=3 {
        let NativeCommand::Create {
            claims: mut next,
            declarations: mut definitions,
        } = f::creation(id, id, &[], None).command
        else {
            panic!("create")
        };
        if id > 1 {
            let row = &mut next[0];
            row.definition.lineage = Lineage::new(
                row.definition.binding,
                Cause::Claim(ClaimId::from_u128(id - 1)),
                &[],
                0,
            )
            .unwrap();
            row.owner = Some(Owner {
                expected: f::binding(id - 1),
                receipt: None,
            });
        }
        claims.append(&mut next);
        declarations.append(&mut definitions);
    }
    f::publish(
        &mut core,
        10,
        NativeInput {
            request: f::request(f::ISSUER, 1),
            command: NativeCommand::Create {
                claims,
                declarations,
            },
        },
    );
    NativeOwner::new(core).unwrap()
}

fn cancelled_tree() -> NativeOwner {
    let mut owner = tree();
    let expected = binding(&owner, 1);
    commit(
        &mut owner,
        NativeInput {
            request: f::request(f::ISSUER, 2),
            command: NativeCommand::Cancel { expected },
        },
    );
    owner
}

#[test]
fn cancelled_owned_tree_releases_bottom_up_in_pending_order_without_rewriting_original_terminal_cuts()
 {
    let mut owner = tree();
    let generated = binding(&owner, 3);
    refused(&mut owner, release(generated, f::ISSUER, 10));
    let expected = binding(&owner, 1);
    commit(
        &mut owner,
        NativeInput {
            request: f::request(f::ISSUER, 2),
            command: NativeCommand::Cancel { expected },
        },
    );
    let originals = [copy(&owner, 1), copy(&owner, 2), copy(&owner, 3)];
    for original in &originals {
        assert_eq!(original.status(), ClaimStatus::Cancelled);
        assert!(!original.released());
        assert_eq!(original.scopes().release_cut(), None);
        assert_eq!(original.response_count(), 0);
    }
    let pinned = owner.pin(0, 1000).unwrap();
    refused(&mut owner, release(originals[0].binding(), f::ISSUER, 11));
    refused(&mut owner, release(originals[1].binding(), f::ISSUER, 12));
    let (leaf, leaf_outcome) = stage(&mut owner, release(originals[2].binding(), f::ISSUER, 13));
    assert_release(&owner, &originals[2], leaf_outcome);
    assert_eq!(owner.effective().claim(ROOT).unwrap(), &originals[0]);
    assert_eq!(
        owner.effective().claim(ClaimId::from_u128(2)).unwrap(),
        &originals[1]
    );
    let (middle, middle_outcome) =
        stage(&mut owner, release(originals[1].binding(), f::ISSUER, 12));
    assert_release(&owner, &originals[1], middle_outcome);
    let (root, root_outcome) = stage(&mut owner, release(originals[0].binding(), f::ISSUER, 11));
    assert_release(&owner, &originals[0], root_outcome);
    for original in &originals {
        assert_eq!(
            owner
                .committed()
                .claim(ClaimId(original.binding().object.0))
                .unwrap(),
            original
        );
    }
    for candidate in [leaf, middle, root] {
        owner.publish_after_durable(candidate).unwrap();
    }
    for original in &originals {
        let id = ClaimId(original.binding().object.0);
        let current = owner.committed().claim(id).unwrap();
        same_business(current, original);
        assert!(current.released());
        assert_eq!(
            pinned
                .with_claim(id, 1, |row| (
                    row.binding(),
                    row.status(),
                    row.released(),
                    row.terminal_cut()
                ))
                .unwrap(),
            Some((
                original.binding(),
                original.status(),
                false,
                original.terminal_cut()
            ))
        );
    }
    owner.release(&pinned).unwrap();
    assert!(leaf_outcome.sequence < middle_outcome.sequence);
    assert!(middle_outcome.sequence < root_outcome.sequence);
    assert_eq!(
        owner
            .prepare(
                f::context(f::ISSUER, 100),
                release(originals[2].binding(), f::ISSUER, 13),
                None
            )
            .unwrap(),
        NativeStaging::Existing {
            candidate: None,
            outcome: leaf_outcome
        }
    );
}

#[test]
fn release_requires_original_actor_and_current_binding_and_only_exact_retries_repeat_success() {
    let mut owner = cancelled_tree();
    let original = copy(&owner, 3);
    for (index, actor) in [f::SUBJECT, f::EVALUATOR, f::QUALITY]
        .into_iter()
        .enumerate()
    {
        refused(
            &mut owner,
            release(original.binding(), actor, 20 + index as u128),
        );
    }
    for expected in [
        original.binding().next().unwrap(),
        Binding {
            content: ContentHash([90; 32]),
            ..original.binding()
        },
        Binding {
            object: focal_model::ObjectId::from_u128(999),
            ..original.binding()
        },
        Binding {
            ledger: LedgerId {
                session: focal_model::SessionId::from_u128(999),
                ..original.binding().ledger
            },
            ..original.binding()
        },
    ] {
        refused(&mut owner, release(expected, f::ISSUER, 24));
    }
    let budget = owner.budget_stats();
    assert!(
        owner
            .prepare(
                NativeContext {
                    principal: Principal::Node(f::ISSUER),
                    logical_time: 100
                },
                release(original.binding(), f::ISSUER, 25),
                None
            )
            .is_err()
    );
    assert_eq!(owner.budget_stats(), budget);
    let (candidate, outcome) = stage(&mut owner, release(original.binding(), f::ISSUER, 26));
    let budget = owner.budget_stats();
    assert_eq!(
        owner
            .prepare(
                f::context(f::ISSUER, 100),
                release(original.binding(), f::ISSUER, 26),
                None
            )
            .unwrap(),
        NativeStaging::Existing {
            candidate: Some(candidate),
            outcome
        }
    );
    assert_eq!(owner.budget_stats(), budget);
    refused(&mut owner, release(original.binding(), f::ISSUER, 27));
    let current = binding(&owner, 3);
    refused(&mut owner, release(current, f::ISSUER, 27));
    let parent = binding(&owner, 2);
    refused(&mut owner, release(parent, f::ISSUER, 26));
    owner.publish_after_durable(candidate).unwrap();
    assert_release(&owner, &original, outcome);
    assert_eq!(
        owner
            .prepare(
                f::context(f::ISSUER, 100),
                release(original.binding(), f::ISSUER, 26),
                None
            )
            .unwrap(),
        NativeStaging::Existing {
            candidate: None,
            outcome
        }
    );
}

#[test]
fn release_pressure_and_copy_failure_preserve_tree_and_suffix_discard_restores_child_obligations() {
    let mut owner = cancelled_tree();
    let originals = [copy(&owner, 1), copy(&owner, 2), copy(&owner, 3)];
    let budget = owner.budget_stats();
    let range = owner.range_stats();
    let source = owner.budget_for_test();
    let pressure = source
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Completion,
            source.limit() - source.stats().used,
        )
        .unwrap()
        .commit();
    refused(&mut owner, release(originals[2].binding(), f::ISSUER, 30));
    drop(pressure);
    let failed = crate::native::prepare::fail_copies_after(0, || {
        owner.prepare(
            f::context(f::ISSUER, 100),
            release(originals[2].binding(), f::ISSUER, 30),
            None,
        )
    });
    assert!(failed.is_err());
    assert_eq!(owner.budget_stats(), budget);
    assert_eq!(owner.range_stats(), range);
    let (leaf, _) = stage(&mut owner, release(originals[2].binding(), f::ISSUER, 30));
    stage(&mut owner, release(originals[1].binding(), f::ISSUER, 31));
    assert_eq!(owner.discard_from(leaf).unwrap(), 2);
    assert_eq!(owner.budget_stats(), budget);
    assert_eq!(owner.range_stats(), range);
    for original in &originals {
        assert_eq!(
            owner
                .effective()
                .claim(ClaimId(original.binding().object.0))
                .unwrap(),
            original
        );
    }
    refused(&mut owner, release(originals[1].binding(), f::ISSUER, 31));
    commit(&mut owner, release(originals[2].binding(), f::ISSUER, 30));
    commit(&mut owner, release(originals[1].binding(), f::ISSUER, 31));
    commit(&mut owner, release(originals[0].binding(), f::ISSUER, 32));
    assert!(owner.committed().claim(ROOT).unwrap().released());
}

#[test]
fn releasing_a_satisfied_scope_preserves_respondent_testimony_and_acceptance_history() {
    let mut core = f::core();
    core.limits.plan_edges = 65_536;
    f::publish(&mut core, 10, f::creation(1, 1, &[], None));
    f::publish(&mut core, 20, f::post(2, f::binding(1)));
    let mut owner = NativeOwner::new(core).unwrap();
    let expected = binding(&owner, 1);
    commit(
        &mut owner,
        NativeInput {
            request: f::request(f::SUBJECT, 3),
            command: NativeCommand::AcquireReceipt {
                expected,
                receipt: ReceiptId::from_u128(701),
            },
        },
    );
    let claim = binding(&owner, 1);
    commit(
        &mut owner,
        NativeInput {
            request: f::request(f::SUBJECT, 4),
            command: NativeCommand::CloseResponse {
                claim,
                response: f::binding(900),
                report: NativeResponseInput {
                    summary: "Respondent completed the requested check.".into(),
                    confidence: Confidence::Committed,
                    outcome: OutcomeKind::Complete,
                    manifest: Vec::new(),
                    diagnostics: Vec::new(),
                },
            },
        },
    );
    for (request, actor, operation) in [(5, f::SUBJECT, 0), (6, f::ISSUER, 1), (7, f::ISSUER, 2)] {
        let claim = binding(&owner, 1);
        let expected = owner
            .effective()
            .response(TestamentId::from_u128(900))
            .unwrap()
            .identity()
            .binding;
        let command = match operation {
            0 => NativeCommand::PostResponse { claim, expected },
            1 => NativeCommand::ReceiveResponse { claim, expected },
            _ => NativeCommand::EnterWholeWork { claim, expected },
        };
        commit(
            &mut owner,
            NativeInput {
                request: f::request(actor, request),
                command,
            },
        );
    }
    let original = copy(&owner, 1);
    assert_eq!(original.status(), ClaimStatus::Satisfied);
    assert!(!original.released());
    let response = owner
        .committed()
        .response(TestamentId::from_u128(900))
        .unwrap();
    let testimony = response.try_copy(response.copy_charge().unwrap()).unwrap();
    let outcome = commit(&mut owner, release(original.binding(), f::ISSUER, 8));
    assert_release(&owner, &original, outcome);
    assert_eq!(
        owner.committed().response(TestamentId::from_u128(900)),
        Some(&testimony)
    );
    owner.with_effective_acceptance(ROOT, |projection| {
        assert!(matches!(projection.claim_decision().outcome(), focal_model::lifecycle::aggregation::AggregateOutcome::LocalComplete { sequence } if Some(sequence) == original.local_sealed_at()));
    }).unwrap();
    owner
        .with_effective_audit(ROOT, |audit| assert!(audit.cohort().complete()))
        .unwrap();
}

#[test]
fn scope_release_preserves_begun_late_observe_authority_and_its_reserved_report_capacity() {
    let mut core = f::running(&[
        (ValidationMode::Required, false),
        (ValidationMode::Observe, false),
    ]);
    core.limits.plan_edges = 65_536;
    let mut custody = f::Custody::new();
    let input = f::report_for(
        &core,
        None,
        100,
        1,
        VerdictValue::Fail,
        f::descriptor(f::artifact_spec(1900, f::EVALUATOR, VerdictValue::Fail)),
    );
    let evidence = f::verified(&mut custody, &input);
    let prepared = f::report(&core, input, &[], &evidence);
    core.publish_native(prepared).unwrap();
    let mut owner = NativeOwner::new(core).unwrap();
    let original = copy(&owner, 1);
    let observer = *owner.effective().evaluation(f::key(2)).unwrap();
    assert!(observer.has_begun());
    assert!(!observer.state().is_terminal());
    assert_eq!(observer.fence(), None);
    let (released, outcome) = stage(&mut owner, release(original.binding(), f::ISSUER, 101));
    assert_release(&owner, &original, outcome);
    assert_eq!(owner.effective().evaluation(f::key(2)), Some(&observer));
    let view = owner.effective();
    let attempt = observer
        .bind(view.definition(f::key(2).validation).unwrap())
        .unwrap()
        .current_attempt()
        .unwrap();
    let artifact = f::descriptor(f::artifact_spec(
        1901,
        attempt.evaluator,
        VerdictValue::Pass,
    ))
    .with_result_provenance(ResultProvenance {
        claim: ROOT,
        validation: f::key(2).validation,
        target: observer.target(),
        generation: observer.generation(),
        attempt,
        value: VerdictValue::Pass,
    })
    .unwrap();
    let report = NativeInput {
        request: f::request(attempt.evaluator, 102),
        command: NativeCommand::ReportAdmission {
            claim: view.claim(ROOT).unwrap().binding(),
            key: f::key(2),
            expected: observer.binding(),
            report: validation::Report {
                generation: observer.generation(),
                attempt,
                value: VerdictValue::Pass,
                evidence: ArtifactRef {
                    id: artifact.id(),
                    hash: artifact.content_hash(),
                },
            },
            artifact: NativeArtifactInput::new(artifact).unwrap(),
        },
    };
    let evidence = f::verified(&mut custody, &report);
    let source = owner.budget_for_test();
    let pressure = source
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Completion,
            source.limit() - source.stats().used,
        )
        .unwrap()
        .commit();
    let NativeStaging::Prepared {
        candidate: reported,
        ..
    } = owner
        .prepare(f::context(attempt.evaluator, 100), report, Some(&evidence))
        .unwrap()
    else {
        panic!("held late report")
    };
    owner.publish_after_durable(released).unwrap();
    owner.publish_after_durable(reported).unwrap();
    drop(pressure);
    let state = owner.committed().evaluation(f::key(2)).unwrap();
    assert!(state.state().is_terminal());
    assert_eq!(state.fence(), None);
    assert!(state.last_result().is_some());
    assert_release(&owner, &original, outcome);
    assert_eq!(
        owner
            .committed()
            .claim(ROOT)
            .unwrap()
            .scopes()
            .release_cut()
            .unwrap()
            .position,
        outcome.sequence
    );
}
