use super::*;
use crate::lifecycle::artifact_descriptor::{ArtifactSpec, Limits as ArtifactLimits, PayloadSpec};
use crate::{ObjectId, ObjectRevision, ReceiptId, SessionId, TenantId, ValidationMode};

fn binding(id: u128) -> Binding {
    Binding {
        ledger: LedgerId {
            tenant: TenantId::from_u128(1),
            session: SessionId::from_u128(2),
        },
        object: ObjectId::from_u128(id),
        content: ContentHash([7; 32]),
        revision: ObjectRevision(1),
    }
}
fn parent() -> Parent {
    Parent {
        ledger: binding(1).ledger,
        claim: ClaimId::from_u128(1),
        issuer: ParticipantId::from_u128(2),
        holder: ParticipantId::from_u128(3),
        receipt: ReceiptFence {
            receipt: ReceiptId::from_u128(4),
            epoch: 2,
        },
        status: ClaimStatus::Received,
        local_complete: false,
        latest_response: None,
        next_cycle: 1,
    }
}
fn limits() -> ResponseLimits {
    ResponseLimits {
        artifacts: 4,
        diagnostics: 4,
        summary_bytes: 128,
        construction_bytes: 32 * 1024,
    }
}
fn artifact(id: u128, producer: ParticipantId, role: WorkRole) -> ArtifactDescriptor {
    let kind = if matches!(role, WorkRole::Output { .. }) {
        "output"
    } else {
        "error"
    };
    ArtifactDescriptor::prepare(
        ArtifactSpec {
            ledger: parent().ledger,
            id: ArtifactId::from_u128(id),
            schema: 1,
            kind,
            schema_hash: ContentHash([9; 32]),
            metadata: b"{}",
            payload: PayloadSpec::Inline(b"retained proof"),
            producer,
            receipt: Some(parent().receipt),
            result: None,
            work: Some(WorkProvenance {
                claim: parent().claim,
                cycle: 1,
                role,
            }),
            inputs: &[],
            visibility: &[],
        },
        ArtifactLimits {
            kind_bytes: 16,
            metadata_bytes: 16,
            inline_bytes: 64,
            inputs: 0,
            visibility_labels: 0,
            visibility_label_bytes: 0,
            construction_bytes: 8192,
        },
    )
    .unwrap()
    .build()
    .unwrap()
}
fn reference(value: &ArtifactDescriptor) -> ArtifactRef {
    ArtifactRef {
        id: value.id(),
        hash: value.content_hash(),
    }
}
fn custody(value: ArtifactRef) -> EvidenceAttestation {
    EvidenceAttestation {
        descriptor_hash: value.hash,
        custody_revision: 1,
        durable: true,
        schema_valid: true,
    }
}
struct Fixture {
    definitions: Vec<crate::lifecycle::validation::Declaration>,
    policy: AcceptancePolicy,
    descriptors: Vec<ArtifactDescriptor>,
    work: Vec<WorkArtifact>,
    response: Response,
    generated: Binding,
}
impl ResponseArtifacts for Fixture {
    fn artifact(&self, value: ArtifactRef) -> Result<&ArtifactDescriptor, ContractError> {
        self.descriptors
            .iter()
            .find(|a| a.id() == value.id)
            .ok_or(ContractError::MissingEvidence)
    }
    fn work(&self, id: ArtifactId) -> Result<&WorkArtifact, ContractError> {
        self.work
            .iter()
            .find(|w| w.reference().id == id)
            .ok_or(ContractError::MissingEvidence)
    }
}
impl Fixture {
    fn new(outcome: OutcomeKind) -> Self {
        Self::with_generated(
            outcome,
            Binding {
                revision: ObjectRevision(7),
                ..binding(100)
            },
        )
    }
    fn with_generated(outcome: OutcomeKind, generated: Binding) -> Self {
        let p = parent();
        use crate::lifecycle::validation::{
            Declaration, DeclarationSpec, Program, TargetDeclaration,
        };
        let delivery = Declaration::new(
            Principal::Actor(p.issuer),
            DeclarationSpec {
                binding: binding(200),
                claim: p.claim,
                issuer: p.issuer,
                declaration_index: 200,
                kind: crate::ValidationKind::Receipt,
                phase: crate::ValidationPhase::WholeWork,
                mode: ValidationMode::Required,
                target: TargetDeclaration::Delivery,
                program: Program::Delivery,
                deadline: crate::Deadline {
                    timer: crate::TimerId::from_u128(200),
                    generation: 1,
                    at: 1000,
                },
            },
            crate::lifecycle::validation::Limits {
                handlers: 1,
                attempts: 1,
                slot_bytes: 8,
            },
        )
        .unwrap();
        let check = Declaration::new(
            Principal::Actor(p.issuer),
            DeclarationSpec {
                binding: binding(201),
                claim: p.claim,
                issuer: p.issuer,
                declaration_index: 201,
                kind: crate::ValidationKind::Inspection,
                phase: crate::ValidationPhase::WholeWork,
                mode: ValidationMode::Required,
                target: TargetDeclaration::WholeWorkSlot {
                    index: 0,
                    name: "output",
                },
                program: crate::lifecycle::validation::tests::programmatic(false),
                deadline: crate::Deadline {
                    timer: crate::TimerId::from_u128(201),
                    generation: 1,
                    at: 1000,
                },
            },
            crate::lifecycle::validation::Limits {
                handlers: 4,
                attempts: 8,
                slot_bytes: 64,
            },
        )
        .unwrap();
        let definitions = vec![delivery, check];
        let policy = AcceptancePolicy::new(
            binding(1),
            p.issuer,
            &[
                aggregation::SlotPolicy {
                    slot: 0,
                    missing_declaration_index: 0,
                    mode: ValidationMode::Required,
                    checks: &[aggregation::CheckPolicy {
                        declaration_index: 201,
                        validation: crate::ValidationId::from_u128(201),
                        mode: ValidationMode::Required,
                    }],
                },
                aggregation::SlotPolicy {
                    slot: 1,
                    missing_declaration_index: 1,
                    mode: ValidationMode::Observe,
                    checks: &[],
                },
                aggregation::SlotPolicy {
                    slot: 2,
                    missing_declaration_index: 2,
                    mode: ValidationMode::Observe,
                    checks: &[],
                },
            ],
            &definitions,
            aggregation::Limits {
                max_slots: 4,
                max_checks: 2,
                max_results: 8,
                max_updates: 4,
            },
        )
        .unwrap();
        let output = artifact(10, p.holder, WorkRole::Output { slot: 0 });
        let production = artifact(
            11,
            p.holder,
            WorkRole::Diagnostic {
                reason: EvidenceFailure::Production,
            },
        );
        let rejected = artifact(12, p.holder, WorkRole::Output { slot: 2 });
        let rejection = artifact(
            13,
            p.issuer,
            WorkRole::ReceiptRejection {
                artifact: reference(&rejected),
                reason: EvidenceFailure::Metadata,
            },
        );
        let diagnostic = artifact(
            14,
            p.holder,
            WorkRole::Diagnostic {
                reason: EvidenceFailure::Work,
            },
        );
        let work = WorkArtifact::generate(
            output.binding(),
            &p,
            Principal::Actor(p.holder),
            0,
            p.receipt,
            &custody(reference(&output)),
        )
        .unwrap();
        let work = work
            .receive(&work.binding(), &p, Principal::Actor(p.issuer))
            .unwrap();
        let failed = WorkArtifact::generation_failed(
            production.binding(),
            &p,
            Principal::Actor(p.holder),
            1,
            p.receipt,
            Diagnostic {
                reason: EvidenceFailure::Production,
                artifact: reference(&production),
            },
            &custody(reference(&production)),
        )
        .unwrap();
        let rejected_work = WorkArtifact::generate(
            rejected.binding(),
            &p,
            Principal::Actor(p.holder),
            2,
            p.receipt,
            &custody(reference(&rejected)),
        )
        .unwrap();
        let rejected_work = rejected_work
            .reject_receipt(
                &rejected_work.binding(),
                &p,
                Principal::Actor(p.issuer),
                Diagnostic {
                    reason: EvidenceFailure::Metadata,
                    artifact: reference(&rejection),
                },
                &custody(reference(&rejection)),
            )
            .unwrap();
        let diagnostics: Vec<_> = [&production, &diagnostic]
            .into_iter()
            .map(|source| {
                let reason = source.work_provenance().map(|p| p.role).unwrap();
                let WorkRole::Diagnostic { reason } = reason else {
                    panic!("fixture diagnostic");
                };
                ResponseDiagnostic::record_native(
                    &p,
                    Principal::Actor(p.holder),
                    p.receipt,
                    Diagnostic {
                        reason,
                        artifact: reference(source),
                    },
                    source,
                    &custody(reference(source)),
                )
                .unwrap()
            })
            .collect();
        // The model permits a non-one original generation revision. Recovery
        // must preserve its recorded value rather than assume the native ingress convention.
        let close = Response::close(
            ResponseIdentity {
                binding: generated,
                claim: p.claim,
                receipt: p.receipt,
                cycle: 1,
                prior: None,
            },
            &p,
            Principal::Actor(p.holder),
            &[work, failed, rejected_work],
            &[SlotBinding {
                slot: 0,
                artifact: work.reference(),
            }],
            CloseReport {
                summary: "Respondent résumé: execution failed.",
                confidence: Confidence::Tentative,
                outcome,
                diagnostics: &diagnostics,
                limits: limits(),
            },
        )
        .unwrap();
        Self {
            definitions,
            policy,
            descriptors: vec![output, production, rejected, rejection, diagnostic],
            work: vec![close.attachments[0], failed, rejected_work],
            response: close.response,
            generated,
        }
    }
    fn round_trip(&self) -> Response {
        let snapshot = self
            .response
            .snapshot_v1(self.generated, usize::MAX)
            .unwrap();
        let plan =
            Response::prepare_hydration_v1(&snapshot, &self.policy, self, limits(), usize::MAX)
                .unwrap();
        let (charge, visits) = (plan.construction_charge(), plan.build_visits());
        let result = plan.build(charge, visits).unwrap();
        assert_eq!(result, self.response);
        assert_eq!(result.report_stamp(), self.response.report_stamp());
        result
    }
}

#[test]
fn response_round_trip_preserves_all_authored_outcomes_and_independent_delivery() {
    for outcome in [
        OutcomeKind::Complete,
        OutcomeKind::Partial,
        OutcomeKind::Refused,
        OutcomeKind::Impossible,
        OutcomeKind::Interrupted,
        OutcomeKind::Failed,
    ] {
        let mut fixture = Fixture::new(outcome);
        let restored = fixture.round_trip();
        assert_eq!(restored.reported_outcome(), outcome);
        assert_eq!(restored.failed_work().len(), 2);
        assert_eq!(restored.diagnostics().len(), 2);
        assert_ne!(
            restored.summary().as_ptr(),
            fixture.response.summary().as_ptr()
        );
        let transition = fixture
            .response
            .plan_post(
                &fixture.response.identity().binding,
                &parent(),
                Principal::Actor(parent().holder),
            )
            .unwrap();
        fixture.response.apply(transition).unwrap();
        fixture.round_trip();
        let transition = fixture
            .response
            .plan_receive(
                &fixture.response.identity().binding,
                &parent(),
                Principal::Actor(parent().issuer),
            )
            .unwrap();
        fixture.response.apply(transition).unwrap();
        fixture.round_trip();
        assert_eq!(fixture.work[0].state(), WorkArtifactState::Attached);
        assert_eq!(fixture.response.identity().binding.revision.0, 9);
    }
}

#[test]
fn scalar_work_and_diagnostic_restoration_checks_actual_identity_and_failed_provenance() {
    let fixture = Fixture::new(OutcomeKind::Failed);
    for work in &fixture.work {
        let diagnostic = if work.state() == WorkArtifactState::ReceiptFailed {
            Some(
                fixture
                    .artifact(work.diagnostic().unwrap().artifact)
                    .unwrap(),
            )
        } else {
            None
        };
        let snapshot = work.snapshot_v1().unwrap();
        let visits = WorkArtifact::hydration_visits(&fixture.policy).unwrap();
        assert_eq!(
            WorkArtifact::hydrate_v1(
                &fixture.policy,
                snapshot,
                fixture.artifact(work.reference()).unwrap(),
                diagnostic,
                visits
            )
            .unwrap(),
            *work
        );
        assert!(
            WorkArtifact::hydrate_v1(
                &fixture.policy,
                snapshot,
                fixture.artifact(work.reference()).unwrap(),
                diagnostic,
                visits - 1
            )
            .is_err()
        );
        let mut corrupt = snapshot;
        corrupt.receipt.epoch += 1;
        assert!(
            WorkArtifact::hydrate_v1(
                &fixture.policy,
                corrupt,
                fixture.artifact(work.reference()).unwrap(),
                diagnostic,
                visits
            )
            .is_err()
        );
    }
    let diagnostic = fixture.response.diagnostics()[0];
    let snapshot = diagnostic.snapshot_v1();
    let source = fixture.artifact(diagnostic.artifact()).unwrap();
    assert_eq!(
        ResponseDiagnostic::hydrate_v1(snapshot, source, ResponseDiagnostic::HYDRATION_VISITS)
            .unwrap(),
        diagnostic
    );
    // Descriptor content excludes its allocated ID; same hash is insufficient.
    let wrong_id = artifact(
        15,
        parent().holder,
        WorkRole::Diagnostic {
            reason: EvidenceFailure::Production,
        },
    );
    assert_eq!(wrong_id.content_hash(), source.content_hash());
    assert!(
        ResponseDiagnostic::hydrate_v1(snapshot, &wrong_id, ResponseDiagnostic::HYDRATION_VISITS)
            .is_err()
    );
}

#[test]
fn response_hydration_admits_all_buffers_before_build_and_retries_after_each_allocation_failure() {
    let fixture = Fixture::new(OutcomeKind::Failed);
    let snapshot = fixture
        .response
        .snapshot_v1(fixture.generated, usize::MAX)
        .unwrap();
    let plan =
        Response::prepare_hydration_v1(&snapshot, &fixture.policy, &fixture, limits(), usize::MAX)
            .unwrap();
    assert_eq!(plan.construction_heap_allocations(), 4);
    let (charge, visits, inspect) = (
        plan.construction_charge(),
        plan.build_visits(),
        plan.inspection_visits(),
    );
    assert_eq!(
        plan.construction_heap_bytes() + std::mem::size_of::<Response>(),
        charge
    );
    assert!(plan.build(charge - 1, visits).is_err());
    let plan =
        Response::prepare_hydration_v1(&snapshot, &fixture.policy, &fixture, limits(), inspect)
            .unwrap();
    assert!(plan.build(charge, visits - 1).is_err());
    assert!(
        Response::prepare_hydration_v1(&snapshot, &fixture.policy, &fixture, limits(), inspect - 1)
            .is_err()
    );
    for fail in 0..4 {
        let plan =
            Response::prepare_hydration_v1(&snapshot, &fixture.policy, &fixture, limits(), inspect)
                .unwrap();
        assert!(bytes::fail_after(fail, || plan.build(charge, visits)).is_err());
        fixture.round_trip();
    }
}

struct Changed<'a> {
    source: ResponseSnapshotV1<'a>,
    fields: ResponseSnapshotFieldsV1<'a>,
    change: std::cell::Cell<bool>,
}
impl ResponseSnapshotSource for Changed<'_> {
    fn fields(&self) -> Result<ResponseSnapshotFieldsV1<'_>, ContractError> {
        Ok(self.fields)
    }
    fn manifest(&self, index: usize) -> Result<SlotBinding, ContractError> {
        self.source.manifest(index)
    }
    fn failed_work(&self, index: usize) -> Result<FailedWorkSnapshotV1, ContractError> {
        self.source.failed_work(index)
    }
    fn diagnostic(&self, index: usize) -> Result<ResponseDiagnosticSnapshotV1, ContractError> {
        let mut value = self.source.diagnostic(index)?;
        if self.change.get() {
            value.cycle += 1;
        }
        Ok(value)
    }
}
#[test]
fn changed_source_and_corrupt_original_revision_manifest_and_report_frame_are_refused() {
    let fixture = Fixture::new(OutcomeKind::Failed);
    let source = fixture
        .response
        .snapshot_v1(fixture.generated, usize::MAX)
        .unwrap();
    let mut changed = Changed {
        fields: source.fields().unwrap(),
        source,
        change: std::cell::Cell::new(false),
    };
    let plan =
        Response::prepare_hydration_v1(&changed, &fixture.policy, &fixture, limits(), usize::MAX)
            .unwrap();
    let (charge, visits) = (plan.construction_charge(), plan.build_visits());
    changed.change.set(true);
    assert!(plan.build(charge, visits).is_err());
    changed.change.set(false);
    for mutation in 0..7 {
        changed.fields = source.fields().unwrap();
        match mutation {
            0 => changed.fields.generated.revision.0 -= 1,
            1 => changed.fields.identity.receipt.epoch += 1,
            2 => changed.fields.respondent = parent().issuer,
            3 => changed.fields.state = ResponseState::Posted,
            4 => changed.fields.manifest_count += 1,
            5 => changed.fields.diagnostic_count = 0,
            _ => changed.fields.summary = " ",
        }
        assert!(
            Response::prepare_hydration_v1(
                &changed,
                &fixture.policy,
                &fixture,
                limits(),
                usize::MAX
            )
            .is_err(),
            "mutation {mutation}"
        );
    }
    assert!(
        fixture
            .response
            .snapshot_v1(
                Binding {
                    revision: ObjectRevision(1),
                    ..fixture.generated
                },
                usize::MAX
            )
            .is_err()
    );
    fixture.round_trip();
}

#[test]
fn response_bounds_are_utf8_bytes_and_refuse_overflow_before_callbacks_or_allocations() {
    let fixture = Fixture::new(OutcomeKind::Failed);
    let source = fixture
        .response
        .snapshot_v1(fixture.generated, usize::MAX)
        .unwrap();
    let value = source.fields().unwrap();
    assert!(value.summary.len() > value.summary.chars().count());
    let mut short = limits();
    short.summary_bytes = value.summary.len() - 1;
    assert!(
        Response::prepare_hydration_v1(&source, &fixture.policy, &fixture, short, usize::MAX)
            .is_err()
    );
    let mut changed = Changed {
        fields: value,
        source,
        change: std::cell::Cell::new(false),
    };
    changed.fields.failed_work_count = usize::MAX;
    assert!(
        Response::prepare_hydration_v1(&changed, &fixture.policy, &fixture, limits(), usize::MAX)
            .is_err()
    );
    assert!(
        Response::prepare_hydration_v1(&source, &fixture.policy, &fixture, limits(), 0).is_err()
    );
}

#[test]
fn explicit_zero_content_and_original_revision_are_preserved_without_invented_native_identity() {
    let mut fixture = Fixture::with_generated(
        OutcomeKind::Failed,
        Binding {
            revision: ObjectRevision(0),
            content: ContentHash([0; 32]),
            ..binding(100)
        },
    );
    fixture.round_trip();
    let transition = fixture
        .response
        .plan_post(
            &fixture.response.identity().binding,
            &parent(),
            Principal::Actor(parent().holder),
        )
        .unwrap();
    fixture.response.apply(transition).unwrap();
    fixture.round_trip();
    let transition = fixture
        .response
        .plan_receive(
            &fixture.response.identity().binding,
            &parent(),
            Principal::Actor(parent().issuer),
        )
        .unwrap();
    fixture.response.apply(transition).unwrap();
    fixture.round_trip();
    assert_eq!(
        fixture.response.identity().binding.revision,
        ObjectRevision(2)
    );
    assert_eq!(
        fixture.response.identity().binding.content,
        ContentHash([0; 32])
    );
}

#[test]
fn work_revision_is_not_inferred_from_the_immutable_descriptor_revision() {
    let fixture = Fixture::new(OutcomeKind::Complete);
    let source = &fixture.descriptors[0];
    let work = WorkArtifact::generate(
        Binding {
            revision: ObjectRevision(0),
            ..source.binding()
        },
        &parent(),
        Principal::Actor(parent().holder),
        0,
        parent().receipt,
        &custody(reference(source)),
    )
    .unwrap();
    assert_eq!(source.binding().revision, ObjectRevision(1));
    let restored = WorkArtifact::hydrate_v1(
        &fixture.policy,
        work.snapshot_v1().unwrap(),
        source,
        None,
        WorkArtifact::hydration_visits(&fixture.policy).unwrap(),
    )
    .unwrap();
    assert_eq!(restored, work);
    let received = work
        .receive(
            &work.binding(),
            &parent(),
            Principal::Actor(parent().issuer),
        )
        .unwrap();
    assert_eq!(
        WorkArtifact::hydrate_v1(
            &fixture.policy,
            received.snapshot_v1().unwrap(),
            source,
            None,
            WorkArtifact::hydration_visits(&fixture.policy).unwrap()
        )
        .unwrap(),
        received
    );
}

fn terminate(fixture: &mut Fixture, verdict: crate::VerdictValue) {
    use crate::lifecycle::{
        claim::{ClaimDefinition, ClaimState},
        graph, scope, succession,
        validation::{
            Evaluation, Materialization, Target,
            tests::{owner_for, report_value},
        },
    };
    let p = parent();
    fixture
        .response
        .apply(
            fixture
                .response
                .plan_post(
                    &fixture.response.identity().binding,
                    &p,
                    Principal::Actor(p.holder),
                )
                .unwrap(),
        )
        .unwrap();
    fixture
        .response
        .apply(
            fixture
                .response
                .plan_receive(
                    &fixture.response.identity().binding,
                    &p,
                    Principal::Actor(p.issuer),
                )
                .unwrap(),
        )
        .unwrap();
    let definition = &fixture.definitions[0];
    let delivery = Evaluation::materialize(
        Principal::Actor(p.issuer),
        definition,
        Materialization {
            binding: definition.binding(),
            target: Target::Delivery {
                response: fixture.response.identity().binding,
            },
            slot_name: None,
            generation: 1,
            receipt: Some(p.receipt),
        },
    )
    .unwrap();
    let delivery = delivery
        .receive_delivery(
            Principal::Actor(p.issuer),
            &delivery.binding(),
            &owner_for(&delivery),
        )
        .unwrap()
        .result
        .unwrap();
    fixture
        .response
        .apply(
            fixture
                .response
                .plan_begin_fixture(
                    &fixture.response.identity().binding,
                    &p,
                    Principal::Actor(p.issuer),
                )
                .unwrap(),
        )
        .unwrap();
    fixture.round_trip();
    fixture.work[0] = fixture.work[0]
        .begin(
            &fixture.work[0].binding(),
            &p,
            Principal::Actor(p.issuer),
            &fixture.response,
        )
        .unwrap();
    let claim = ClaimState::generate(
        Principal::Actor(p.issuer),
        ClaimDefinition {
            binding: binding(1),
            issuer: p.issuer,
            subject: p.holder,
            deadline: None,
            max_responses: 4,
            created: SessionSeq(1),
            graph: graph::Declaration::empty(),
            lineage: succession::Lineage::root(binding(1), crate::RootCommandId::from_u128(1))
                .unwrap(),
            acceptance: fixture
                .policy
                .try_copy(fixture.policy.copy_charge().unwrap())
                .unwrap(),
            scope_limits: scope::ScopeLimits {
                scopes: 4,
                roots: 4,
                children: 4,
            },
        },
    )
    .unwrap();
    let mut aggregate = aggregation::ResponseAggregation::new(
        &claim,
        fixture.response.evaluation().unwrap(),
        &[delivery],
        aggregation::Limits {
            max_slots: 4,
            max_checks: 4,
            max_results: 16,
            max_updates: 4,
        },
    )
    .unwrap();
    let definition = &fixture.definitions[1];
    let evaluation = Evaluation::materialize(
        Principal::Actor(p.issuer),
        definition,
        Materialization {
            binding: definition.binding(),
            target: Target::Artifact {
                response: fixture.response.identity().binding,
                artifact: fixture.work[0].binding(),
                slot: 0,
            },
            slot_name: Some("output"),
            generation: 1,
            receipt: Some(p.receipt),
        },
    )
    .unwrap();
    let mut evaluation = evaluation
        .begin(
            Principal::Actor(evaluation.evaluator().unwrap()),
            &evaluation.binding(),
            &owner_for(&evaluation),
        )
        .unwrap()
        .next;
    let mut accepted = Vec::new();
    for _ in 0..definition.attempt_bound() {
        let report = report_value(&evaluation, verdict);
        accepted.push(report.result.unwrap());
        evaluation = report.next;
        if evaluation.state().is_terminal() {
            break;
        }
    }
    assert!(evaluation.state().is_terminal());
    let update = aggregate.apply(SessionSeq(40), &accepted).unwrap();
    fixture.work[0] = fixture.work[0]
        .apply_aggregate(&fixture.work[0].binding(), &update)
        .unwrap();
    fixture
        .response
        .apply(
            fixture
                .response
                .plan_aggregate(&fixture.response.identity().binding, &update)
                .unwrap()
                .unwrap(),
        )
        .unwrap();
}

#[test]
fn actual_aggregate_terminal_rows_retain_exact_cuts_and_reject_foreign_causes() {
    for (verdict, state) in [
        (crate::VerdictValue::Pass, ResponseState::Validated),
        (
            crate::VerdictValue::Incomplete,
            ResponseState::ValidationIncomplete,
        ),
        (crate::VerdictValue::Fail, ResponseState::ValidationFailed),
        (crate::VerdictValue::Error, ResponseState::ValidationErrored),
    ] {
        let mut fixture = Fixture::new(OutcomeKind::Failed);
        terminate(&mut fixture, verdict);
        assert_eq!(fixture.response.state(), state);
        let restored = fixture.round_trip();
        assert_eq!(restored.terminal(), fixture.response.terminal());
        let work = fixture.work[0];
        let snapshot = work.snapshot_v1().unwrap();
        let visits = WorkArtifact::hydration_visits(&fixture.policy).unwrap();
        assert_eq!(
            WorkArtifact::hydrate_v1(
                &fixture.policy,
                snapshot,
                fixture.artifact(work.reference()).unwrap(),
                None,
                visits
            )
            .unwrap(),
            work
        );
        if let Some(WorkTerminalSnapshotV1::Blocked { sequence, cause }) = snapshot.terminal {
            assert_eq!(sequence, SessionSeq(40));
            for target in [
                aggregation::CauseTarget::Admission,
                aggregation::CauseTarget::Response(TestamentId::from_u128(999)),
            ] {
                let mut corrupt_cause = cause;
                corrupt_cause.key.target = target;
                let mut corrupt = snapshot;
                corrupt.terminal = Some(WorkTerminalSnapshotV1::Blocked {
                    sequence,
                    cause: corrupt_cause,
                });
                assert!(
                    WorkArtifact::hydrate_v1(
                        &fixture.policy,
                        corrupt,
                        fixture.artifact(work.reference()).unwrap(),
                        None,
                        visits
                    )
                    .is_err()
                );
                let source = fixture
                    .response
                    .snapshot_v1(fixture.generated, usize::MAX)
                    .unwrap();
                let mut fields = source.fields().unwrap();
                let Some(ResponseTerminalSnapshotV1::Blocked(mut cut)) = fields.terminal else {
                    panic!("blocked response");
                };
                cut.cause.key.target = target;
                fields.terminal = Some(ResponseTerminalSnapshotV1::Blocked(cut));
                let corrupt = Changed {
                    source,
                    fields,
                    change: std::cell::Cell::new(false),
                };
                assert!(
                    Response::prepare_hydration_v1(
                        &corrupt,
                        &fixture.policy,
                        &fixture,
                        limits(),
                        usize::MAX
                    )
                    .is_err()
                );
            }
            let mut corrupt = snapshot;
            corrupt.terminal = Some(WorkTerminalSnapshotV1::Blocked {
                sequence,
                cause: aggregation::BlockingCauseSnapshotV1 {
                    mode: ValidationMode::Observe,
                    ..cause
                },
            });
            assert!(
                WorkArtifact::hydrate_v1(
                    &fixture.policy,
                    corrupt,
                    fixture.artifact(work.reference()).unwrap(),
                    None,
                    visits
                )
                .is_err()
            );
        }
    }
}
