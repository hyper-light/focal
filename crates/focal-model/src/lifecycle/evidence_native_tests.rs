use super::*;
use crate::lifecycle::artifact_descriptor::{
    ArtifactDescriptor, ArtifactSpec, Limits as ArtifactLimits, PayloadSpec, WorkProvenance,
    WorkRole,
};
use crate::lifecycle::{aggregation, claim, graph, memory, validation};
use crate::{
    ArtifactId, Confidence, ContentHash, ObjectId, ObjectRevision, OutcomeKind, ReceiptId,
    SessionSeq,
};

fn received(max: u32) -> claim::ClaimState {
    let definition = claim::tests::definition(max);
    let mut state =
        claim::ClaimState::generate(Principal::Actor(definition.issuer), definition).unwrap();
    state
        .apply(
            &state.binding(),
            Principal::Actor(state.issuer()),
            claim::ClaimIntent::Post {
                standing: claim::PostingStanding {
                    binding: state.binding(),
                    standing: claim::PredicateState::Passed,
                    target: claim::PredicateState::Passed,
                },
            },
        )
        .unwrap();
    let snapshot = graph::Snapshot::capture(
        &[&state],
        graph::Limits {
            nodes: 4,
            edges: 8,
            visits: 64,
        },
    )
    .unwrap();
    let start = snapshot.start(ClaimId(state.binding().object.0)).unwrap();
    let aggregation = aggregation::ClaimAggregation::new(
        &state,
        aggregation::Limits {
            max_slots: 4,
            max_checks: 4,
            max_results: 8,
            max_updates: 8,
        },
    )
    .unwrap();
    state
        .acquire_receipt(
            &state.binding(),
            Principal::Actor(state.subject()),
            ReceiptFence {
                receipt: ReceiptId::from_u128(100),
                epoch: 1,
            },
            &aggregation.admission(),
            &start,
            &[],
        )
        .unwrap();
    state
}

fn spec(parent: &Parent, id: u128) -> ArtifactSpec<'static> {
    ArtifactSpec {
        ledger: parent.ledger,
        id: ArtifactId::from_u128(id),
        schema: 1,
        kind: "error",
        schema_hash: ContentHash([3; 32]),
        metadata: b"{}",
        payload: PayloadSpec::Inline(
            br#"{"code":"failed","message":"The requested work failed."}"#,
        ),
        producer: parent.holder,
        receipt: Some(parent.receipt),
        result: None,
        work: Some(WorkProvenance {
            claim: parent.claim,
            cycle: parent.next_cycle,
            role: WorkRole::Diagnostic {
                reason: EvidenceFailure::Work,
            },
        }),
        inputs: &[],
        visibility: &["internal"],
    }
}

fn descriptor(spec: ArtifactSpec<'_>) -> ArtifactDescriptor {
    ArtifactDescriptor::prepare(
        spec,
        ArtifactLimits {
            kind_bytes: 64,
            metadata_bytes: 128,
            inline_bytes: 1024,
            inputs: 8,
            visibility_labels: 4,
            visibility_label_bytes: 64,
            construction_bytes: 8192,
        },
    )
    .unwrap()
    .build()
    .unwrap()
}

fn custody(hash: ContentHash) -> EvidenceAttestation {
    EvidenceAttestation {
        descriptor_hash: hash,
        custody_revision: 1,
        durable: true,
        schema_valid: true,
    }
}

fn diagnostic(source: &ArtifactDescriptor) -> Diagnostic {
    Diagnostic {
        reason: EvidenceFailure::Work,
        artifact: ArtifactRef {
            id: source.id(),
            hash: source.content_hash(),
        },
    }
}

fn report<'a>(diagnostics: &'a [ResponseDiagnostic]) -> CloseReport<'a> {
    CloseReport {
        summary: "The respondent ended the work cycle; retained evidence explains its outcome.",
        confidence: Confidence::Committed,
        outcome: OutcomeKind::Failed,
        diagnostics,
        limits: ResponseLimits {
            artifacts: 4,
            diagnostics: 4,
            summary_bytes: 1024,
            construction_bytes: 65536,
        },
    }
}

fn identity(parent: &Parent) -> ResponseIdentity {
    ResponseIdentity {
        binding: Binding {
            ledger: parent.ledger,
            object: ObjectId::from_u128(1000 + u128::from(parent.next_cycle)),
            content: ContentHash([7; 32]),
            revision: ObjectRevision(1),
        },
        claim: parent.claim,
        receipt: parent.receipt,
        cycle: parent.next_cycle,
        prior: parent.latest_response,
    }
}

fn rich(parent: &Parent) -> ClosePlan {
    let source = descriptor(spec(parent, 300));
    let diagnostic = ResponseDiagnostic::record_native(
        parent,
        Principal::Actor(parent.holder),
        parent.receipt,
        diagnostic(&source),
        &source,
        &custody(source.content_hash()),
    )
    .unwrap();
    let work: Vec<_> = (0..2)
        .map(|slot| {
            let binding = Binding {
                ledger: parent.ledger,
                object: ObjectId::from_u128(200 + u128::from(slot)),
                content: ContentHash([4; 32]),
                revision: ObjectRevision(1),
            };
            WorkArtifact::generate(
                binding,
                parent,
                Principal::Actor(parent.holder),
                slot,
                parent.receipt,
                &custody(binding.content),
            )
            .unwrap()
        })
        .collect();
    let manifest: Vec<_> = work
        .iter()
        .map(|row| SlotBinding {
            slot: row.slot(),
            artifact: row.reference(),
        })
        .collect();
    Response::close(
        identity(parent),
        parent,
        Principal::Actor(parent.holder),
        &work,
        &manifest,
        report(&[diagnostic]),
    )
    .unwrap()
}

#[test]
fn native_diagnostic_requires_exact_current_work_provenance_and_verified_custody() {
    let state = received(3);
    let parent = Parent::from_claim(&state).unwrap();
    let original = descriptor(spec(&parent, 300));
    let make = |source: &ArtifactDescriptor, actor, proof: EvidenceAttestation| {
        ResponseDiagnostic::record_native(
            &parent,
            actor,
            parent.receipt,
            diagnostic(source),
            source,
            &proof,
        )
    };
    let actor = Principal::Actor(parent.holder);
    let admitted = make(&original, actor, custody(original.content_hash())).unwrap();
    assert_eq!(
        (
            admitted.claim(),
            admitted.cycle(),
            admitted.producer(),
            admitted.receipt()
        ),
        (
            parent.claim,
            parent.next_cycle,
            parent.holder,
            parent.receipt
        )
    );
    admitted.check_parent(&parent).unwrap();
    for case in 0..9 {
        let mut changed = spec(&parent, 300);
        let expected = match case {
            0 => {
                changed.producer = parent.issuer;
                ContractError::WrongActor
            }
            1 => {
                changed.ledger.session = crate::SessionId::from_u128(999);
                ContractError::WrongLedger
            }
            2 => {
                changed.receipt.as_mut().unwrap().epoch += 1;
                ContractError::StaleReceipt
            }
            3 => {
                changed.work = None;
                ContractError::MissingEvidence
            }
            4 => {
                changed.work.as_mut().unwrap().claim = ClaimId::from_u128(999);
                ContractError::MissingEvidence
            }
            5 => {
                changed.work.as_mut().unwrap().cycle += 1;
                ContractError::MissingEvidence
            }
            6 => {
                changed.work.as_mut().unwrap().role = WorkRole::Output { slot: 0 };
                ContractError::MissingEvidence
            }
            7 => {
                changed.work.as_mut().unwrap().role = WorkRole::Diagnostic {
                    reason: EvidenceFailure::Metadata,
                };
                ContractError::MissingEvidence
            }
            _ => {
                changed.kind = "other";
                changed.work.as_mut().unwrap().role = WorkRole::Output { slot: 0 };
                ContractError::MissingEvidence
            }
        };
        let source = descriptor(changed);
        assert_eq!(
            make(&source, actor, custody(source.content_hash())),
            Err(expected),
            "case {case}"
        );
    }
    for case in 0..4 {
        let mut proof = custody(original.content_hash());
        match case {
            0 => proof.custody_revision = 0,
            1 => proof.durable = false,
            2 => proof.schema_valid = false,
            _ => proof.descriptor_hash = ContentHash([9; 32]),
        }
        assert_eq!(
            make(&original, actor, proof),
            Err(ContractError::MissingEvidence)
        );
    }
    assert_eq!(
        make(
            &original,
            Principal::Node(parent.holder),
            custody(original.content_hash())
        ),
        Err(ContractError::WrongActor)
    );
    let mut next = parent;
    next.next_cycle += 1;
    assert_eq!(
        admitted.check_parent(&next),
        Err(ContractError::InvalidManifest)
    );
    next = parent;
    next.receipt.epoch += 1;
    assert_eq!(
        admitted.check_parent(&next),
        Err(ContractError::StaleReceipt)
    );
}

#[test]
fn response_copy_owns_report_manifest_and_diagnostics_and_preserves_original_stamp_and_cut() {
    let state = received(3);
    let parent = Parent::from_claim(&state).unwrap();
    let mut plan = rich(&parent);
    for row in &plan.attachments {
        assert_eq!(
            (row.claim(), row.cycle(), row.producer(), row.receipt()),
            (
                parent.claim,
                parent.next_cycle,
                parent.holder,
                parent.receipt
            )
        );
    }
    // Copying already retained rows preserves their exact state; it does not
    // replay transitions or re-admit this historical report under today's view.
    plan.response.state = ResponseState::Validated;
    plan.response.terminal = Some(aggregation::ResponseOutcome::Validated {
        sequence: SessionSeq(9),
    });
    let original = plan.response;
    let charge = original.copy_charge().unwrap();
    let copied = original.try_copy(charge).unwrap();
    assert_eq!(copied, original);
    assert_eq!(copied.heap_allocations().unwrap(), 3);
    assert_eq!(copied.retained_bytes().unwrap(), charge);
    assert_ne!(copied.summary().as_ptr(), original.summary().as_ptr());
    assert_ne!(copied.manifest().as_ptr(), original.manifest().as_ptr());
    assert_ne!(
        copied.diagnostics().as_ptr(),
        original.diagnostics().as_ptr()
    );
    let stamp = original.report_stamp();
    drop(original);
    assert_eq!(copied.report_stamp(), stamp);
    assert_eq!(copied.reported_outcome(), OutcomeKind::Failed);
    assert_eq!(copied.manifest().len(), 2);
    assert_eq!(copied.diagnostics().len(), 1);
    assert_eq!(
        copied.terminal(),
        Some(aggregation::ResponseOutcome::Validated {
            sequence: SessionSeq(9)
        })
    );
}

#[test]
fn response_copy_preflights_and_every_owned_buffer_failure_leaves_original_unchanged() {
    let state = received(3);
    let original = rich(&Parent::from_claim(&state).unwrap()).response;
    let before = original.clone();
    let charge = original.copy_charge().unwrap();
    memory::fail_after(3, || {
        assert_eq!(original.try_copy(charge - 1), Err(ContractError::Capacity));
        assert_eq!(memory::remaining_allocations(), Some(3));
    });
    assert_eq!(original.copy_heap_allocations().unwrap(), 3);
    for after in 0..3 {
        assert_eq!(
            memory::fail_after(after, || original.try_copy(charge)),
            Err(ContractError::Capacity)
        );
        assert_eq!(original, before);
    }
    assert_eq!(original.try_copy(charge).unwrap(), before);
}

#[test]
fn claim_copy_reserves_one_history_entry_before_observation_and_refuses_declared_exhaustion() {
    let mut original = received(2);
    assert_eq!(original.max_responses(), 2);
    assert_eq!(original.response_history_heap_bytes().unwrap(), 0);
    assert_eq!(original.response_history_heap_allocations(), 0);
    for cycle in 1..=2 {
        let charge = original.copy_for_response_charge().unwrap();
        let before = original.clone();
        memory::fail_after(0, || {
            assert_eq!(
                original.try_copy_for_response(charge - 1),
                Err(ContractError::Capacity)
            );
            assert_eq!(memory::remaining_allocations(), Some(0));
        });
        for after in 0..original.copy_for_response_heap_allocations().unwrap() {
            assert_eq!(
                memory::fail_after(after, || original.try_copy_for_response(charge)),
                Err(ContractError::Capacity)
            );
            assert_eq!(original, before);
        }
        let mut copied = original.try_copy_for_response(charge).unwrap();
        assert_eq!(copied, original);
        assert_eq!(copied.retained_bytes().unwrap(), charge);
        let heap = copied.response_history_heap_bytes().unwrap();
        assert_eq!(copied.response_history_heap_allocations(), 1);
        let parent = Parent::from_claim(&copied).unwrap();
        let response = Response::close(
            identity(&parent),
            &parent,
            Principal::Actor(parent.holder),
            &[],
            &[],
            CloseReport {
                outcome: OutcomeKind::Complete,
                ..report(&[])
            },
        )
        .unwrap()
        .response;
        assert_eq!(
            copied.observe_response(
                &copied.binding(),
                Principal::Actor(parent.issuer),
                &response
            ),
            Err(ContractError::WrongActor)
        );
        assert_eq!(copied.response_count(), original.response_count());
        memory::fail_after(0, || {
            copied.observe_response(
                &copied.binding(),
                Principal::Actor(parent.holder),
                &response,
            )
        })
        .unwrap();
        assert_eq!(copied.response_count(), cycle);
        assert_eq!(copied.response_history_heap_bytes().unwrap(), heap);
        assert_eq!(copied.retained_bytes().unwrap(), charge);
        original = copied;
    }
    assert_eq!(
        original.response_history_heap_bytes().unwrap(),
        original.max_response_history_heap_bytes().unwrap()
    );
    assert_eq!(
        original.copy_for_response_charge(),
        Err(ContractError::Capacity)
    );
    assert_eq!(
        original.try_copy_for_response(usize::MAX),
        Err(ContractError::Capacity)
    );
    assert_eq!(
        original.try_copy(original.copy_charge().unwrap()).unwrap(),
        original
    );
}

#[test]
fn zero_check_output_slots_remain_declared_and_close_allocations_are_fallible_and_accounted() {
    let state = received(3);
    let parent = Parent::from_claim(&state).unwrap();
    let binding = Binding {
        object: ObjectId::from_u128(700),
        ..state.binding()
    };
    let declaration = validation::Declaration::new(
        Principal::Actor(parent.issuer),
        validation::DeclarationSpec {
            binding,
            claim: parent.claim,
            issuer: parent.issuer,
            declaration_index: 10,
            kind: crate::ValidationKind::Receipt,
            phase: crate::ValidationPhase::WholeWork,
            mode: crate::ValidationMode::Required,
            target: validation::TargetDeclaration::Delivery,
            program: validation::Program::Delivery,
            deadline: crate::Deadline {
                timer: crate::TimerId::from_u128(700),
                generation: 1,
                at: 1000,
            },
        },
        validation::Limits {
            handlers: 1,
            attempts: 1,
            slot_bytes: 1,
        },
    )
    .unwrap();
    let policy = aggregation::AcceptancePolicy::new(
        state.binding(),
        parent.issuer,
        &[aggregation::SlotPolicy {
            slot: 7,
            missing_declaration_index: 20,
            mode: crate::ValidationMode::Required,
            checks: &[],
        }],
        &[declaration],
        aggregation::Limits {
            max_slots: 2,
            max_checks: 2,
            max_results: 2,
            max_updates: 2,
        },
    )
    .unwrap();
    assert!(policy.has_slot(7));
    assert!(!policy.has_slot(0));
    assert!(!policy.has_slot(10));

    let original = rich(&parent);
    let current: Vec<_> = original
        .attachments
        .iter()
        .map(|row| WorkArtifact {
            binding: Binding {
                revision: ObjectRevision(1),
                ..row.binding()
            },
            state: WorkArtifactState::Generated,
            attachment: None,
            ..*row
        })
        .collect();
    let input = report(original.response.diagnostics());
    let build = || {
        Response::prepare_close(
            identity(&parent),
            &parent,
            Principal::Actor(parent.holder),
            &current,
            original.response.manifest(),
            input,
        )
        .unwrap()
    };
    assert_eq!(build().construction_heap_allocations().unwrap(), 4);
    for after in 0..4 {
        assert_eq!(
            memory::fail_after(after, || build().build()),
            Err(ContractError::Capacity)
        );
    }
    let quote = build().construction_charge();
    let built = build().build().unwrap();
    assert_eq!(built.retained_bytes().unwrap(), quote);
    assert_eq!(built.heap_allocations().unwrap(), 4);
    assert_eq!(built, original);
}

fn failed_work_set(parent: &Parent) -> (Vec<WorkArtifact>, ResponseDiagnostic) {
    let mut production = spec(parent, 300);
    production.work.as_mut().unwrap().role = WorkRole::Diagnostic {
        reason: EvidenceFailure::Production,
    };
    let source = descriptor(production);
    let diagnostic = Diagnostic {
        reason: EvidenceFailure::Production,
        artifact: ArtifactRef {
            id: source.id(),
            hash: source.content_hash(),
        },
    };
    let report_diagnostic = ResponseDiagnostic::record_native(
        parent,
        Principal::Actor(parent.holder),
        parent.receipt,
        diagnostic,
        &source,
        &custody(source.content_hash()),
    )
    .unwrap();
    let product = |slot: u32| {
        let binding = Binding {
            ledger: parent.ledger,
            object: ObjectId::from_u128(200 + u128::from(slot)),
            content: ContentHash([4; 32]),
            revision: ObjectRevision(1),
        };
        WorkArtifact::generate(
            binding,
            parent,
            Principal::Actor(parent.holder),
            slot,
            parent.receipt,
            &custody(binding.content),
        )
        .unwrap()
    };
    let generated = product(0);
    // A production failure is represented by the actual diagnostic address,
    // without allocating an imaginary output payload. Its revision need not
    // advance to preserve it in a response.
    let failed = WorkArtifact::generation_failed(
        Binding {
            ledger: parent.ledger,
            object: ObjectId(source.id().0),
            content: source.content_hash(),
            revision: ObjectRevision(u64::MAX),
        },
        parent,
        Principal::Actor(parent.holder),
        1,
        parent.receipt,
        diagnostic,
        &custody(source.content_hash()),
    )
    .unwrap();
    let product = product(2);
    let received = product
        .receive(&product.binding(), parent, Principal::Actor(parent.issuer))
        .unwrap();
    let rejected = WorkArtifact::generate(
        Binding {
            object: ObjectId::from_u128(203),
            ..generated.binding()
        },
        parent,
        Principal::Actor(parent.holder),
        3,
        parent.receipt,
        &custody(generated.binding().content),
    )
    .unwrap();
    let rejected = rejected
        .reject_receipt(
            &rejected.binding(),
            parent,
            Principal::Actor(parent.issuer),
            Diagnostic {
                reason: EvidenceFailure::Structure,
                artifact: ArtifactRef {
                    id: ArtifactId::from_u128(500),
                    hash: ContentHash([5; 32]),
                },
            },
            &custody(ContentHash([5; 32])),
        )
        .unwrap();
    (
        vec![generated, failed, received, rejected],
        report_diagnostic,
    )
}

fn attachable_manifest(current: &[WorkArtifact]) -> Vec<SlotBinding> {
    current
        .iter()
        .filter(|row| {
            matches!(
                row.state(),
                WorkArtifactState::Generated | WorkArtifactState::Received
            )
        })
        .map(|row| SlotBinding {
            slot: row.slot(),
            artifact: row.reference(),
        })
        .collect()
}

#[test]
fn mixed_failed_work_closes_and_delivers_without_attaching_or_repainting_failed_rows() {
    let mut state = received(3);
    let parent = Parent::from_claim(&state).unwrap();
    let (current, diagnostic) = failed_work_set(&parent);
    let before = current.clone();
    let manifest = attachable_manifest(&current);
    let plan = Response::close(
        identity(&parent),
        &parent,
        Principal::Actor(parent.holder),
        &current,
        &manifest,
        report(&[diagnostic]),
    )
    .unwrap();
    assert_eq!(current, before);
    assert_eq!(plan.attachments.len(), 2);
    assert_eq!(plan.response.manifest(), manifest);
    for (attachment, old) in plan.attachments.iter().zip([&current[0], &current[2]]) {
        assert_eq!(attachment.binding(), old.binding().next().unwrap());
        assert_eq!(attachment.state(), WorkArtifactState::Attached);
        assert_eq!(attachment.reference(), old.reference());
    }
    for (failed, old) in plan
        .response
        .failed_work()
        .iter()
        .zip([&current[1], &current[3]])
    {
        assert_eq!(failed.binding(), old.binding());
        assert_eq!(failed.slot(), old.slot());
        assert_eq!(failed.state(), old.state());
        assert_eq!(failed.diagnostic(), old.diagnostic().unwrap());
        assert_eq!(old.attachment(), None);
        assert!(
            !plan
                .response
                .manifest()
                .iter()
                .any(|row| row.slot == failed.slot())
        );
    }
    // The production diagnostic is preserved in both its truthful roles. The
    // claimant's rejection does not become a respondent-authored diagnostic.
    assert_eq!(plan.response.diagnostics(), &[diagnostic]);
    assert_eq!(
        plan.response.failed_work()[0].diagnostic(),
        diagnostic.diagnostic()
    );
    assert_ne!(
        plan.response.failed_work()[1].diagnostic(),
        diagnostic.diagnostic()
    );
    let mut response = plan.response;
    state = state
        .try_copy_for_response(state.copy_for_response_charge().unwrap())
        .unwrap();
    state
        .observe_response(&state.binding(), Principal::Actor(parent.holder), &response)
        .unwrap();
    for actor in [
        Principal::Actor(parent.holder),
        Principal::Actor(parent.issuer),
    ] {
        let transition = if response.state() == ResponseState::Generated {
            response.plan_post(&response.identity().binding, &parent, actor)
        } else {
            response.plan_receive(&response.identity().binding, &parent, actor)
        }
        .unwrap();
        response.apply(transition).unwrap();
        state
            .observe_response(&state.binding(), actor, &response)
            .unwrap();
    }
    assert_eq!(response.state(), ResponseState::Received);
    assert_eq!(state.status(), ClaimStatus::TestamentAcknowledged);
    assert_eq!(response.failed_work().len(), 2);
    assert_eq!(response.terminal(), None);
    assert!(!state.local_complete());

    // Reported completion is an assertion, not a rewrite of actual failures.
    let complete = Response::close(
        identity(&parent),
        &parent,
        Principal::Actor(parent.holder),
        &current,
        &manifest,
        CloseReport {
            outcome: OutcomeKind::Complete,
            ..report(&[diagnostic])
        },
    )
    .unwrap();
    assert_eq!(complete.response.failed_work(), response.failed_work());
    assert_eq!(complete.response.terminal(), None);
}

#[test]
fn failed_work_stamp_rejects_substituted_failures_under_the_same_response_binding() {
    let state = received(3);
    let parent = Parent::from_claim(&state).unwrap();
    let (current, diagnostic) = failed_work_set(&parent);
    let close = |rows: &[WorkArtifact]| {
        Response::close(
            identity(&parent),
            &parent,
            Principal::Actor(parent.holder),
            rows,
            &attachable_manifest(rows),
            report(&[diagnostic]),
        )
        .unwrap()
        .response
    };
    let original = close(&current);
    for case in 0..8 {
        let mut changed = current.clone();
        let row = &mut changed[3];
        match case {
            0 => row.binding.object = ObjectId::from_u128(204),
            1 => row.binding.content = ContentHash([6; 32]),
            2 => row.binding.revision = ObjectRevision(3),
            3 => row.slot = 4,
            4 => row.diagnostic.as_mut().unwrap().artifact.id = ArtifactId::from_u128(501),
            5 => row.diagnostic.as_mut().unwrap().artifact.hash = ContentHash([6; 32]),
            6 => row.diagnostic.as_mut().unwrap().reason = EvidenceFailure::Metadata,
            _ => {
                row.state = WorkArtifactState::GenerationFailed;
                row.diagnostic.as_mut().unwrap().reason = EvidenceFailure::Production;
            }
        }
        let mut substitute = close(&changed);
        let before = substitute.clone();
        let transition = original
            .plan_post(
                &original.identity().binding,
                &parent,
                Principal::Actor(parent.holder),
            )
            .unwrap();
        assert_eq!(
            substitute.apply(transition),
            Err(ContractError::ContentConflict),
            "case {case}"
        );
        assert_eq!(substitute, before);
    }
    let copied = original.try_copy(original.copy_charge().unwrap()).unwrap();
    assert_eq!(copied, original);
    assert_ne!(
        copied.failed_work().as_ptr(),
        original.failed_work().as_ptr()
    );
    drop(original);
    assert_eq!(
        copied.failed_work()[0].state(),
        WorkArtifactState::GenerationFailed
    );
    assert_eq!(
        copied.failed_work()[1].state(),
        WorkArtifactState::ReceiptFailed
    );
}

#[test]
fn failed_work_closure_and_copy_preflight_every_owned_buffer_before_publication() {
    let state = received(3);
    let parent = Parent::from_claim(&state).unwrap();
    let (current, diagnostic) = failed_work_set(&parent);
    let before = current.clone();
    let manifest = attachable_manifest(&current);
    let diagnostics = [diagnostic];
    let prepare = |input| {
        Response::prepare_close(
            identity(&parent),
            &parent,
            Principal::Actor(parent.holder),
            &current,
            &manifest,
            input,
        )
    };
    let input = report(&diagnostics);
    let charge = prepare(input).unwrap().construction_charge();
    assert_eq!(
        prepare(input)
            .unwrap()
            .construction_heap_allocations()
            .unwrap(),
        5
    );
    memory::fail_after(5, || {
        assert!(matches!(
            prepare(CloseReport {
                limits: ResponseLimits {
                    construction_bytes: charge - 1,
                    ..input.limits
                },
                ..input
            }),
            Err(ContractError::Capacity)
        ));
        assert_eq!(memory::remaining_allocations(), Some(5));
    });
    for after in 0..5 {
        assert_eq!(
            memory::fail_after(after, || prepare(input).unwrap().build()),
            Err(ContractError::Capacity)
        );
        assert_eq!(current, before);
    }
    let plan = prepare(input).unwrap().build().unwrap();
    assert_eq!(plan.retained_bytes().unwrap(), charge);
    assert_eq!(plan.heap_allocations().unwrap(), 5);
    let original = plan.response;
    let before = original.clone();
    let copy_charge = original.copy_charge().unwrap();
    assert_eq!(original.copy_heap_allocations().unwrap(), 4);
    memory::fail_after(4, || {
        assert_eq!(
            original.try_copy(copy_charge - 1),
            Err(ContractError::Capacity)
        );
        assert_eq!(memory::remaining_allocations(), Some(4));
    });
    for after in 0..4 {
        assert_eq!(
            memory::fail_after(after, || original.try_copy(copy_charge)),
            Err(ContractError::Capacity)
        );
        assert_eq!(original, before);
    }
    let copied = original.try_copy(copy_charge).unwrap();
    assert_eq!(copied.retained_bytes().unwrap(), copy_charge);
    assert_eq!(copied.heap_allocations().unwrap(), 4);
    assert_eq!(copied, original);
}

#[test]
fn rejection_and_close_races_preserve_exact_manifest_and_require_respondent_testimony() {
    let state = received(3);
    let parent = Parent::from_claim(&state).unwrap();
    let (current, diagnostic) = failed_work_set(&parent);
    let rejected = current[3];
    let close =
        |rows: &[WorkArtifact], manifest: &[SlotBinding], diagnostics: &[ResponseDiagnostic]| {
            Response::close(
                identity(&parent),
                &parent,
                Principal::Actor(parent.holder),
                rows,
                manifest,
                report(diagnostics),
            )
        };
    // A claimant-authored rejection alone cannot testify that the respondent's
    // work ended unsuccessfully. The respondent must still author its report.
    assert_eq!(
        close(&[rejected], &[], &[]),
        Err(ContractError::MissingEvidence)
    );
    let stale_manifest = [SlotBinding {
        slot: rejected.slot(),
        artifact: rejected.reference(),
    }];
    assert_eq!(
        close(&[rejected], &stale_manifest, &[diagnostic]),
        Err(ContractError::InvalidManifest)
    );
    let plan = close(&[rejected], &[], &[diagnostic]).unwrap();
    assert!(plan.attachments.is_empty());
    assert!(plan.response.manifest().is_empty());
    assert_eq!(plan.response.failed_work()[0].binding(), rejected.binding());

    let generated = current[0];
    let manifest = [SlotBinding {
        slot: generated.slot(),
        artifact: generated.reference(),
    }];
    let attached = close(&[generated], &manifest, &[diagnostic])
        .unwrap()
        .attachments[0];
    assert_eq!(
        attached.reject_receipt(
            &attached.binding(),
            &parent,
            Principal::Actor(parent.issuer),
            rejected.diagnostic().unwrap(),
            &custody(rejected.diagnostic().unwrap().artifact.hash),
        ),
        Err(ContractError::InvalidTransition)
    );

    for case in 0..6 {
        let mut failed = rejected;
        let expected = match case {
            0 => {
                failed.cycle += 1;
                ContractError::InvalidManifest
            }
            1 => {
                failed.receipt.epoch += 1;
                ContractError::StaleReceipt
            }
            2 => {
                failed.producer = parent.issuer;
                ContractError::WrongActor
            }
            3 => {
                failed.attachment = Some(identity(&parent).binding);
                ContractError::InvalidTransition
            }
            4 => {
                failed.diagnostic = None;
                ContractError::MissingEvidence
            }
            _ => {
                failed.diagnostic.as_mut().unwrap().reason = EvidenceFailure::Work;
                ContractError::InvalidPolicy
            }
        };
        assert_eq!(
            close(&[failed], &[], &[diagnostic]),
            Err(expected),
            "case {case}"
        );
    }
    assert_eq!(
        close(&[rejected, rejected], &[], &[diagnostic]),
        Err(ContractError::InvalidManifest)
    );
}
