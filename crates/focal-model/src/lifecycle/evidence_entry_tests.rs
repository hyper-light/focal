use super::*;

fn cohort_limits() -> a::Limits {
    a::Limits {
        max_slots: 3,
        ..limits()
    }
}

fn cohort_definitions(mode: ValidationMode) -> Vec<Declaration> {
    let mut declarations = definitions(
        ValidationMode::Required,
        fixtures::programmatic(false),
        false,
        false,
    );
    let spec = DeclarationSpec {
        binding: fixtures::binding(203),
        claim: ClaimId::from_u128(9),
        declaration_index: 2,
        target: TargetDeclaration::WholeWorkSlot {
            index: 2,
            name: "supplement",
        },
        ..fixtures::specification(mode, fixtures::programmatic(false))
    };
    declarations
        .push(Declaration::new(Principal::Actor(spec.issuer), spec, fixtures::limits()).unwrap());
    declarations
}

// Two actual artifacts, including one with no checks, plus one omitted slot.
// Every owner and lifecycle fact is produced through the public checked model.
fn cohort(declarations: &[Declaration]) -> (ClaimState, Response, Vec<WorkArtifact>) {
    let mut definition = claim::tests::definition(2);
    let check = |index: usize| a::CheckPolicy {
        declaration_index: declarations[index].declaration_index(),
        validation: ValidationId(declarations[index].binding().object.0),
        mode: declarations[index].mode(),
    };
    definition.acceptance = a::AcceptancePolicy::new(
        definition.binding,
        definition.issuer,
        &[
            a::SlotPolicy {
                slot: 0,
                missing_declaration_index: 3,
                mode: ValidationMode::Required,
                checks: &[check(1)],
            },
            a::SlotPolicy {
                slot: 1,
                missing_declaration_index: 4,
                mode: ValidationMode::Required,
                checks: &[],
            },
            a::SlotPolicy {
                slot: 2,
                missing_declaration_index: 5,
                mode: declarations[2].mode(),
                checks: &[check(2)],
            },
        ],
        declarations,
        cohort_limits(),
    )
    .unwrap();
    let mut claim = ClaimState::generate(Principal::Actor(definition.issuer), definition).unwrap();
    claim
        .post_owned(Principal::Actor(claim.issuer()), claim.binding())
        .unwrap();
    let graph = graph::Snapshot::capture(
        &[&claim],
        graph::Limits {
            nodes: 2,
            edges: 2,
            visits: 64,
        },
    )
    .unwrap();
    let start = graph.start(ClaimId(claim.binding().object.0)).unwrap();
    let aggregate = a::ClaimAggregation::new(&claim, cohort_limits()).unwrap();
    claim
        .acquire_receipt(
            &claim.binding(),
            Principal::Actor(claim.subject()),
            ReceiptFence {
                receipt: ReceiptId::from_u128(100),
                epoch: 1,
            },
            &aggregate.admission(),
            &start,
            &[],
        )
        .unwrap();
    let parent = e::Parent::from_claim(&claim).unwrap();
    let work: Vec<_> = [0, 1]
        .into_iter()
        .map(|slot| {
            let binding = fixtures::binding(300 + u128::from(slot));
            WorkArtifact::generate(
                binding,
                &parent,
                Principal::Actor(parent.holder),
                slot,
                parent.receipt,
                &EvidenceAttestation {
                    descriptor_hash: binding.content,
                    custody_revision: 1,
                    durable: true,
                    schema_valid: true,
                },
            )
            .unwrap()
        })
        .collect();
    let manifest: Vec<_> = work
        .iter()
        .map(|work| e::SlotBinding {
            slot: work.slot(),
            artifact: work.reference(),
        })
        .collect();
    let plan = Response::close(
        e::ResponseIdentity {
            binding: fixtures::binding(400),
            claim: parent.claim,
            receipt: parent.receipt,
            cycle: parent.next_cycle,
            prior: parent.latest_response,
        },
        &parent,
        Principal::Actor(parent.holder),
        &work,
        &manifest,
        e::CloseReport {
            summary: "Two outputs are reported; the supplement is absent.",
            confidence: Confidence::Committed,
            outcome: OutcomeKind::Complete,
            diagnostics: &[],
            limits: e::ResponseLimits {
                artifacts: 2,
                diagnostics: 0,
                summary_bytes: 256,
                construction_bytes: 8192,
            },
        },
    )
    .unwrap();
    claim
        .observe_response(
            &claim.binding(),
            Principal::Actor(parent.holder),
            &plan.response,
        )
        .unwrap();
    let mut response = plan.response;
    post(&mut claim, &mut response);
    receive(&mut claim, &mut response);
    (claim, response, plan.attachments)
}

fn claimant_entry(
    claim: &mut ClaimState,
    response: &mut Response,
    aggregate: &mut a::ClaimAggregation,
) -> e::ResponseEntry {
    let plan = response
        .plan_begin(
            &response.identity().binding,
            claim,
            Principal::Actor(claim.issuer()),
            &aggregate.decision(),
        )
        .unwrap();
    claim
        .request_evaluation(
            &claim.binding(),
            Principal::Actor(claim.issuer()),
            &aggregate.decision(),
        )
        .unwrap();
    aggregate.rebind(claim).unwrap();
    let entry = response
        .entry_capability(&plan, claim, &aggregate.decision())
        .unwrap();
    response.apply(plan).unwrap();
    entry
}

#[test]
fn evaluator_entry_advances_the_entire_manifest_and_missing_slot_without_impersonation() {
    let declarations = cohort_definitions(ValidationMode::Required);
    let (mut claim, mut response, works) = cohort(&declarations);
    let mut aggregate = a::ClaimAggregation::new(&claim, cohort_limits()).unwrap();
    let missing = ready(&declarations[2], &claim, &response, None);
    let evaluation = begin(
        ready(&declarations[1], &claim, &response, Some(&works[0])),
        &claim,
        &response,
        Some(&works[0]),
        &aggregate,
    );
    let evaluator = Principal::Actor(evaluation.evaluator().unwrap());
    let plan = response
        .plan_evaluation(
            &response.identity().binding,
            &claim,
            &evaluation,
            &aggregate.decision(),
        )
        .unwrap();
    claim
        .observe_evaluation(&claim.binding(), &evaluation, &aggregate.decision())
        .unwrap();
    aggregate.rebind(&claim).unwrap();
    let entry = response
        .entry_capability(&plan, &claim, &aggregate.decision())
        .unwrap();
    response.apply(plan).unwrap();
    let parent = e::Parent::from_claim(&claim).unwrap();
    assert_eq!(
        works[1]
            .observe_evaluation(&works[1].binding(), &parent, &response, &evaluation)
            .unwrap_err(),
        ContractError::InvalidTarget
    );
    for work in works {
        assert_eq!(
            work.begin(&work.binding(), &parent, evaluator, &response)
                .unwrap_err(),
            ContractError::WrongActor
        );
        let next = work
            .begin_entered(&work.binding(), &claim, &response, &entry)
            .unwrap();
        assert_eq!(next.state(), WorkArtifactState::Validating);
        assert_eq!(next.binding(), work.binding().next().unwrap());
        assert!(
            next.begin_entered(&next.binding(), &claim, &response, &entry)
                .is_err()
        );
    }
    assert_eq!(
        missing
            .settle_missing(
                evaluator,
                &missing.binding(),
                &claim,
                &response,
                &aggregate.decision(),
            )
            .unwrap_err(),
        ContractError::WrongActor
    );
    let settled = missing
        .settle_missing_entered(&missing.binding(), &claim, &response, &entry)
        .unwrap();
    let result = settled.result.unwrap();
    assert_eq!(result.target(), missing.target());
    assert_eq!(result.phase(), Phase::MissingTarget);
    assert_eq!(result.resulting_state(), State::ValidationIncomplete);
    assert_eq!(result.receipt(), missing.receipt());
    assert_eq!(result.generation(), missing.generation());
    assert!(result.attempt().is_none());
    assert!(result.reporter().is_none());
    assert!(result.evidence().is_none());
    assert!(result.programmatic_evidence().is_none());
    assert!(!settled.next.has_begun());
    assert_eq!(claim.status(), ClaimStatus::Validating);
    assert_eq!(response.state(), ResponseState::Validating);
}

#[test]
fn claimant_and_entered_missing_paths_preserve_required_and_observe_semantics() {
    for mode in [ValidationMode::Required, ValidationMode::Observe] {
        let declarations = definitions(mode, fixtures::programmatic(false), false, false);
        let (mut claim, mut response, _) = received(&declarations, false);
        let evaluation = ready(&declarations[1], &claim, &response, None);
        let mut aggregate = a::ClaimAggregation::new(&claim, limits()).unwrap();
        let entry = claimant_entry(&mut claim, &mut response, &mut aggregate);
        let explicit = evaluation
            .settle_missing(
                Principal::Actor(claim.issuer()),
                &evaluation.binding(),
                &claim,
                &response,
                &aggregate.decision(),
            )
            .unwrap();
        let derived = evaluation
            .settle_missing_entered(&evaluation.binding(), &claim, &response, &entry)
            .unwrap();
        assert_eq!(explicit.next.into_state(), derived.next.into_state());
        assert_eq!(explicit.result, derived.result);
        assert_eq!(derived.result.is_some(), mode == ValidationMode::Required);
        if mode == ValidationMode::Observe {
            assert_eq!(derived.next.state(), State::Ready);
            assert_eq!(derived.next.suppression(), Some(Suppression::MissingTarget));
        }
        assert!(
            derived
                .next
                .settle_missing_entered(&derived.next.binding(), &claim, &response, &entry)
                .is_err()
        );
    }
}

#[test]
fn capability_requires_checked_current_claim_and_the_applied_response_entry() {
    let declarations = definitions(
        ValidationMode::Required,
        fixtures::programmatic(false),
        false,
        false,
    );
    let (mut claim, mut response, work) = received(&declarations, true);
    let work = work.unwrap();
    let mut aggregate = a::ClaimAggregation::new(&claim, limits()).unwrap();
    assert_eq!(
        response
            .plan_begin(
                &response.identity().binding,
                &claim,
                Principal::Actor(claim.subject()),
                &aggregate.decision(),
            )
            .unwrap_err(),
        ContractError::WrongActor
    );
    let plan = response
        .plan_begin(
            &response.identity().binding,
            &claim,
            Principal::Actor(claim.issuer()),
            &aggregate.decision(),
        )
        .unwrap();
    assert_eq!(
        response
            .entry_capability(&plan, &claim, &aggregate.decision())
            .unwrap_err(),
        ContractError::InvalidTransition
    );
    claim
        .request_evaluation(
            &claim.binding(),
            Principal::Actor(claim.issuer()),
            &aggregate.decision(),
        )
        .unwrap();
    assert_eq!(
        response
            .entry_capability(&plan, &claim, &aggregate.decision())
            .unwrap_err(),
        ContractError::StaleRevision
    );
    aggregate.rebind(&claim).unwrap();
    let entry = response
        .entry_capability(&plan, &claim, &aggregate.decision())
        .unwrap();
    assert_eq!(
        work.begin_entered(&work.binding(), &claim, &response, &entry)
            .unwrap_err(),
        ContractError::StaleRevision
    );
    response.apply(plan).unwrap();
    assert!(
        response
            .entry_capability(&plan, &claim, &aggregate.decision())
            .is_err()
    );
    assert_eq!(
        work.begin_entered(&work.binding().next().unwrap(), &claim, &response, &entry)
            .unwrap_err(),
        ContractError::StaleRevision
    );
    let mut cancelled = claim.try_copy(claim.copy_charge().unwrap()).unwrap();
    cancelled
        .apply(
            &cancelled.binding(),
            Principal::Actor(cancelled.issuer()),
            claim::ClaimIntent::Cancel {
                cut: claim::ClaimCut {
                    position: SessionSeq(20),
                    cause: ContentHash([61; 32]),
                },
            },
        )
        .unwrap();
    assert_eq!(
        work.begin_entered(&work.binding(), &cancelled, &response, &entry)
            .unwrap_err(),
        ContractError::StaleRevision
    );
    assert_eq!(work.state(), WorkArtifactState::Attached);
    assert!(
        work.begin_entered(&work.binding(), &claim, &response, &entry)
            .is_ok()
    );
}

#[test]
fn entered_structural_absence_preserves_fences_and_seals_without_external_work() {
    for mode in [ValidationMode::Required, ValidationMode::Observe] {
        let declarations = definitions(mode, fixtures::programmatic(false), false, false);
        let (mut claim, mut response, _) = received(&declarations, false);
        let evaluation = ready(&declarations[1], &claim, &response, None);
        let mut aggregate = a::ClaimAggregation::new(&claim, limits()).unwrap();
        let owner = evaluation
            .work_owner(&claim, &response, None, &aggregate.decision(), 10)
            .unwrap();
        let mut fenced_owner = owner;
        fenced_owner.logical_time = evaluation.deadline().at;
        fenced_owner.authority.state = AuthorityState::Fenced(AuthorityFence {
            reason: FenceReason::Deadline(evaluation.deadline()),
            cause: ContentHash([92; 32]),
        });
        let fenced = evaluation
            .record_fence(&evaluation.binding(), &fenced_owner)
            .unwrap();
        let mut sealed_owner = owner;
        sealed_owner.cohort = Cohort::Sealed {
            cause: ContentHash([93; 32]),
        };
        let sealed = evaluation
            .record_seal(&evaluation.binding(), &sealed_owner)
            .unwrap();
        let entry = claimant_entry(&mut claim, &mut response, &mut aggregate);
        for retained in [fenced, sealed] {
            let transition = retained
                .settle_missing_entered(&retained.binding(), &claim, &response, &entry)
                .unwrap();
            assert_eq!(transition.next.into_state(), retained.into_state());
            assert!(transition.result.is_none());
            assert!(!transition.next.has_begun());
        }
    }
}

#[test]
fn entered_missing_refuses_substituted_target_generation_receipt_and_revision() {
    let declarations = definitions(
        ValidationMode::Required,
        fixtures::programmatic(false),
        false,
        false,
    );
    let (mut claim, mut response, _) = received(&declarations, false);
    let evaluation = ready(&declarations[1], &claim, &response, None);
    let mut aggregate = a::ClaimAggregation::new(&claim, limits()).unwrap();
    let entry = claimant_entry(&mut claim, &mut response, &mut aggregate);
    let mut wrong_generation = evaluation;
    wrong_generation.stored.generation += 1;
    let mut wrong_receipt = evaluation;
    wrong_receipt.stored.receipt.as_mut().unwrap().epoch += 1;
    let mut wrong_response = evaluation;
    let Target::MissingSlot {
        response: pinned, ..
    } = &mut wrong_response.stored.target
    else {
        panic!("fixture is an actual missing slot");
    };
    pinned.content = ContentHash([62; 32]);
    for invalid in [wrong_generation, wrong_receipt, wrong_response] {
        assert!(
            invalid
                .settle_missing_entered(&invalid.binding(), &claim, &response, &entry)
                .is_err()
        );
        assert!(invalid.last_result().is_none());
        assert_eq!(invalid.state(), State::Ready);
    }
    assert_eq!(
        evaluation
            .settle_missing_entered(
                &evaluation.binding().next().unwrap(),
                &claim,
                &response,
                &entry,
            )
            .unwrap_err(),
        ContractError::StaleRevision
    );
    assert!(
        evaluation
            .settle_missing_entered(&evaluation.binding(), &claim, &response, &entry)
            .is_ok()
    );
}

#[test]
fn plans_and_capabilities_cannot_cross_reports_with_identical_public_bindings() {
    let declarations = definitions(
        ValidationMode::Required,
        fixtures::programmatic(false),
        false,
        false,
    );
    let (mut claim, mut response, work) = received(&declarations, true);
    let work = work.unwrap();
    let (other_claim, mut other_response, _) = received(&declarations, false);
    assert_eq!(response.identity(), other_response.identity());
    assert_ne!(response.manifest(), other_response.manifest());
    let other_aggregate = a::ClaimAggregation::new(&other_claim, limits()).unwrap();
    let other_plan = other_response
        .plan_begin(
            &other_response.identity().binding,
            &other_claim,
            Principal::Actor(other_claim.issuer()),
            &other_aggregate.decision(),
        )
        .unwrap();
    let mut aggregate = a::ClaimAggregation::new(&claim, limits()).unwrap();
    let plan = response
        .plan_begin(
            &response.identity().binding,
            &claim,
            Principal::Actor(claim.issuer()),
            &aggregate.decision(),
        )
        .unwrap();
    claim
        .request_evaluation(
            &claim.binding(),
            Principal::Actor(claim.issuer()),
            &aggregate.decision(),
        )
        .unwrap();
    aggregate.rebind(&claim).unwrap();
    assert_eq!(
        response
            .entry_capability(&other_plan, &claim, &aggregate.decision())
            .unwrap_err(),
        ContractError::ContentConflict
    );
    let entry = response
        .entry_capability(&plan, &claim, &aggregate.decision())
        .unwrap();
    response.apply(plan).unwrap();
    other_response.apply(other_plan).unwrap();
    assert_eq!(
        entry.check(&claim, &other_response).unwrap_err(),
        ContractError::ContentConflict
    );
    assert_eq!(
        work.begin_entered(&work.binding(), &claim, &other_response, &entry)
            .unwrap_err(),
        ContractError::ContentConflict
    );
    entry.check(&claim, &response).unwrap();
}

#[test]
fn capability_creation_rechecks_increment_sealing_after_the_claim_transition() {
    let declarations = definitions(
        ValidationMode::Required,
        fixtures::programmatic(false),
        true,
        false,
    );
    let (mut claim, response, _) = received(&declarations, false);
    let mut aggregate = a::ClaimAggregation::new(&claim, limits()).unwrap();
    assert!(
        response
            .plan_begin(
                &response.identity().binding,
                &claim,
                Principal::Actor(claim.issuer()),
                &aggregate.decision(),
            )
            .is_err()
    );
    aggregate.seal_increment_targets().unwrap();
    let plan = response
        .plan_begin(
            &response.identity().binding,
            &claim,
            Principal::Actor(claim.issuer()),
            &aggregate.decision(),
        )
        .unwrap();
    claim
        .request_evaluation(
            &claim.binding(),
            Principal::Actor(claim.issuer()),
            &aggregate.decision(),
        )
        .unwrap();
    aggregate.rebind(&claim).unwrap();
    let unsealed = a::ClaimAggregation::new(&claim, limits()).unwrap();
    assert_eq!(
        response
            .entry_capability(&plan, &claim, &unsealed.decision())
            .unwrap_err(),
        ContractError::InvalidTransition
    );
    assert!(
        response
            .entry_capability(&plan, &claim, &aggregate.decision())
            .is_ok()
    );
}
