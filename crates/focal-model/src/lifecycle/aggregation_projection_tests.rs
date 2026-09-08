use super::*;
use crate::lifecycle::{
    Principal, claim, evidence, graph, memory, scope, succession, validation as v,
};
use crate::{
    Confidence, Deadline, EvidenceAttestation, OutcomeKind, ReceiptId, RootCommandId, TimerId,
    ValidationKind, ValidationPhase,
};
use v::tests::{EVALUATOR, ISSUER, binding, programmatic, report_parts};

#[path = "aggregation_adoption_tests.rs"]
mod adoption_tests;
#[path = "aggregation_projection_quote_tests.rs"]
mod quote_tests;

struct ResponseRow {
    response: Response,
    received: Option<PublicationPosition>,
    entered: Option<PublicationPosition>,
}
struct Fixture {
    claim: ClaimState,
    registry: RegistrationSet,
    definitions: Vec<Declaration>,
    responses: Vec<ResponseRow>,
    works: Vec<WorkArtifact>,
    evaluations: Vec<EvaluationState>,
    results: Vec<(AcceptedResult, PublicationPosition)>,
    prefix: SessionSeq,
}
impl WholeWorkView for Fixture {
    fn prefix(&self) -> SessionSeq {
        self.prefix
    }
    fn declaration(&self, id: ValidationId) -> Option<&Declaration> {
        self.definitions
            .iter()
            .find(|row| row.binding().object.0 == id.0)
    }
    fn evaluation(&self, registered: RegisteredEvaluation) -> Option<&EvaluationState> {
        self.evaluations.iter().find(|row| {
            row.binding().object == registered.binding().object
                && row.target() == registered.target()
                && row.generation() == registered.generation()
        })
    }
    fn accepted(&self, result: &AcceptedResult) -> Option<PublishedResult<'_>> {
        self.results
            .iter()
            .find(|(row, _)| row == result)
            .map(|(result, position)| PublishedResult {
                result,
                position: *position,
            })
    }
    fn response(&self, id: TestamentId) -> Option<PublishedResponse<'_>> {
        self.responses
            .iter()
            .find(|row| row.response.identity().binding.object.0 == id.0)
            .map(|row| PublishedResponse {
                response: &row.response,
                received: row.received,
                entered: row.entered,
            })
    }
    fn work(&self, id: ArtifactId) -> Option<&WorkArtifact> {
        self.works.iter().find(|row| row.reference().id == id)
    }
    fn works(&self, _: ClaimId) -> impl Iterator<Item = Result<&WorkArtifact, ContractError>> {
        self.works.iter().map(Ok)
    }
}
fn policy_limits() -> Limits {
    Limits {
        max_slots: 8,
        max_checks: 16,
        max_results: 64,
        max_updates: 8,
    }
}
fn limits() -> ProjectionLimits {
    ProjectionLimits {
        responses: 8,
        works: 32,
        slots: 64,
        evaluations: 64,
        declarations: 16,
        visits: 100_000,
        bytes: 1024 * 1024,
    }
}
fn position(sequence: u64, ordinal: u32) -> PublicationPosition {
    PublicationPosition {
        sequence: SessionSeq(sequence),
        ordinal,
    }
}
fn definition(index: u32, target: v::TargetDeclaration<'_>, mode: ValidationMode) -> Declaration {
    let delivery = target == v::TargetDeclaration::Delivery;
    Declaration::new(
        Principal::Actor(ISSUER),
        v::DeclarationSpec {
            binding: binding(100 + u128::from(index)),
            claim: ClaimId::from_u128(200),
            issuer: ISSUER,
            declaration_index: index,
            kind: if delivery {
                ValidationKind::Receipt
            } else {
                ValidationKind::Test
            },
            phase: if target == v::TargetDeclaration::Increment {
                ValidationPhase::Increment
            } else {
                ValidationPhase::WholeWork
            },
            mode,
            target,
            program: if delivery {
                v::Program::Delivery
            } else {
                programmatic(false)
            },
            deadline: Deadline {
                timer: TimerId::from_u128(100 + u128::from(index)),
                generation: 1,
                at: 1000,
            },
        },
        v::tests::limits(),
    )
    .unwrap()
}
impl Fixture {
    fn new(slots: &[(ValidationMode, usize)], increment: bool) -> Self {
        let mut definitions = vec![definition(
            0,
            v::TargetDeclaration::Delivery,
            ValidationMode::Required,
        )];
        let mut checks = Vec::new();
        for (slot, (_, count)) in slots.iter().enumerate() {
            let mut rows = Vec::new();
            for _ in 0..*count {
                let index = u32::try_from(definitions.len()).unwrap();
                definitions.push(definition(
                    index,
                    v::TargetDeclaration::WholeWorkSlot {
                        index: u32::try_from(slot).unwrap(),
                        name: "output",
                    },
                    ValidationMode::Required,
                ));
                rows.push(CheckPolicy {
                    declaration_index: index,
                    validation: ValidationId::from_u128(100 + u128::from(index)),
                    mode: ValidationMode::Required,
                });
            }
            checks.push(rows);
        }
        if increment {
            definitions.push(definition(
                u32::try_from(definitions.len()).unwrap(),
                v::TargetDeclaration::Increment,
                ValidationMode::Required,
            ));
        }
        let policies: Vec<_> = slots
            .iter()
            .enumerate()
            .map(|(slot, (mode, _))| SlotPolicy {
                slot: u32::try_from(slot).unwrap(),
                missing_declaration_index: 50 + u32::try_from(slot).unwrap(),
                mode: *mode,
                checks: &checks[slot],
            })
            .collect();
        let mut claim = ClaimState::generate(
            Principal::Actor(ISSUER),
            claim::ClaimDefinition {
                binding: binding(200),
                issuer: ISSUER,
                subject: EVALUATOR,
                deadline: None,
                max_responses: 8,
                created: SessionSeq(1),
                graph: graph::Declaration::empty(),
                lineage: succession::Lineage::root(binding(200), RootCommandId::from_u128(1))
                    .unwrap(),
                acceptance: AcceptancePolicy::new(
                    binding(200),
                    ISSUER,
                    &policies,
                    &definitions,
                    policy_limits(),
                )
                .unwrap(),
                scope_limits: scope::ScopeLimits {
                    scopes: 4,
                    roots: 8,
                    children: 4,
                },
            },
        )
        .unwrap();
        claim
            .post_owned(Principal::Actor(ISSUER), claim.binding())
            .unwrap();
        let aggregate = ClaimAggregation::new(&claim, policy_limits()).unwrap();
        let graph = graph::Snapshot::capture(
            &[&claim],
            graph::Limits {
                nodes: 1,
                edges: 1,
                visits: 32,
            },
        )
        .unwrap();
        let start = graph.start(ClaimId::from_u128(200)).unwrap();
        claim
            .acquire_receipt(
                &claim.binding(),
                Principal::Actor(EVALUATOR),
                ReceiptFence {
                    receipt: ReceiptId::from_u128(9),
                    epoch: 1,
                },
                &aggregate.admission(),
                &start,
                &[],
            )
            .unwrap();
        let registry = RegistrationSet::new(&claim, 64, size_of::<RegistrationSet>()).unwrap();
        Self {
            claim,
            registry,
            definitions,
            responses: Vec::new(),
            works: Vec::new(),
            evaluations: Vec::new(),
            results: Vec::new(),
            prefix: SessionSeq(1),
        }
    }
    fn projection(&self) -> WholeWorkProjection<'_> {
        prepare_projection(&self.claim, &self.registry, self, limits())
            .unwrap()
            .build()
            .unwrap()
    }
    fn receive(&mut self, id: u128, outputs: &[(u32, u128)], sequence: u64) -> usize {
        self.receive_report(id, outputs, sequence, OutcomeKind::Complete, &[])
    }
    fn receive_report(
        &mut self,
        id: u128,
        outputs: &[(u32, u128)],
        sequence: u64,
        outcome: OutcomeKind,
        diagnostics: &[evidence::ResponseDiagnostic],
    ) -> usize {
        self.receive_report_with_work(id, outputs, sequence, outcome, diagnostics, &[])
    }
    fn receive_report_with_work(
        &mut self,
        id: u128,
        outputs: &[(u32, u128)],
        sequence: u64,
        outcome: OutcomeKind,
        diagnostics: &[evidence::ResponseDiagnostic],
        failed: &[WorkArtifact],
    ) -> usize {
        let parent = evidence::Parent::from_claim(&self.claim).unwrap();
        let mut current: Vec<_> = outputs
            .iter()
            .map(|(slot, id)| {
                WorkArtifact::generate(
                    binding(*id),
                    &parent,
                    Principal::Actor(EVALUATOR),
                    *slot,
                    parent.receipt,
                    &EvidenceAttestation {
                        descriptor_hash: binding(*id).content,
                        custody_revision: 1,
                        durable: true,
                        schema_valid: true,
                    },
                )
                .unwrap()
            })
            .collect();
        let manifest: Vec<_> = current
            .iter()
            .map(|work| evidence::SlotBinding {
                slot: work.slot(),
                artifact: work.reference(),
            })
            .collect();
        current.extend_from_slice(failed);
        let plan = Response::close(
            evidence::ResponseIdentity {
                binding: binding(id),
                claim: parent.claim,
                receipt: parent.receipt,
                cycle: parent.next_cycle,
                prior: parent.latest_response,
            },
            &parent,
            Principal::Actor(EVALUATOR),
            &current,
            &manifest,
            evidence::CloseReport {
                summary: "The respondent finished this work attempt.",
                confidence: Confidence::Committed,
                outcome,
                diagnostics,
                limits: evidence::ResponseLimits {
                    artifacts: 8,
                    diagnostics: 8,
                    summary_bytes: 128,
                    construction_bytes: 16 * 1024,
                },
            },
        )
        .unwrap();
        let mut response = plan.response;
        self.works.extend(plan.attachments);
        self.works.extend_from_slice(failed);
        self.claim
            .observe_response(
                &self.claim.binding(),
                Principal::Actor(EVALUATOR),
                &response,
            )
            .unwrap();
        let parent = evidence::Parent::from_claim(&self.claim).unwrap();
        response
            .apply(
                response
                    .plan_post(
                        &response.identity().binding,
                        &parent,
                        Principal::Actor(EVALUATOR),
                    )
                    .unwrap(),
            )
            .unwrap();
        self.claim
            .observe_response(
                &self.claim.binding(),
                Principal::Actor(EVALUATOR),
                &response,
            )
            .unwrap();
        let parent = evidence::Parent::from_claim(&self.claim).unwrap();
        response
            .apply(
                response
                    .plan_receive(
                        &response.identity().binding,
                        &parent,
                        Principal::Actor(ISSUER),
                    )
                    .unwrap(),
            )
            .unwrap();
        self.claim
            .observe_response(&self.claim.binding(), Principal::Actor(ISSUER), &response)
            .unwrap();
        let at = self.responses.len();
        self.responses.push(ResponseRow {
            response,
            received: Some(position(sequence, 0)),
            entered: None,
        });
        for offset in 0..self.definitions.len() {
            let definition = &self.definitions[offset];
            let response = &self.responses[at].response;
            let ready = match definition.target() {
                v::TargetDeclaration::Delivery => v::Evaluation::materialize_delivery(
                    Principal::Actor(ISSUER),
                    definition,
                    &self.claim,
                    response,
                )
                .unwrap(),
                v::TargetDeclaration::WholeWorkSlot { index, .. } => {
                    let work = response
                        .manifest()
                        .iter()
                        .find(|entry| entry.slot == index)
                        .and_then(|entry| {
                            self.works
                                .iter()
                                .find(|work| work.reference() == entry.artifact)
                        });
                    v::Evaluation::materialize_work(
                        Principal::Actor(ISSUER),
                        definition,
                        &self.claim,
                        response,
                        work,
                    )
                    .unwrap()
                }
                _ => continue,
            };
            self.registry
                .register(&self.claim, &ready, 1024 * 1024)
                .unwrap();
            if definition.target() == v::TargetDeclaration::Delivery {
                let owner = ready.delivery_owner(&self.claim, response, 0).unwrap();
                let transition = ready
                    .receive_delivery(Principal::Actor(ISSUER), &ready.binding(), &owner)
                    .unwrap();
                self.results.push((
                    transition.result.unwrap(),
                    position(sequence, u32::try_from(offset + 1).unwrap()),
                ));
                self.evaluations.push(transition.next.into_state());
            } else {
                self.evaluations.push(ready.into_state());
            }
        }
        self.prefix = SessionSeq(sequence);
        at
    }
    fn enter(&mut self, at: usize, sequence: u64) {
        let mut aggregate = ClaimAggregation::new(&self.claim, policy_limits()).unwrap();
        if self.registry.increment_targets_sealed() {
            aggregate.seal_increment_targets().unwrap();
        }
        if self.claim.status() != ClaimStatus::Validating {
            self.claim
                .request_evaluation(
                    &self.claim.binding(),
                    Principal::Actor(ISSUER),
                    &aggregate.decision(),
                )
                .unwrap();
        }
        aggregate.rebind(&self.claim).unwrap();
        let row = &mut self.responses[at];
        row.response
            .apply(
                row.response
                    .plan_begin(
                        &row.response.identity().binding,
                        &self.claim,
                        Principal::Actor(ISSUER),
                        &aggregate.decision(),
                    )
                    .unwrap(),
            )
            .unwrap();
        row.entered = Some(position(sequence, 0));
        let parent = evidence::Parent::from_claim(&self.claim).unwrap();
        for work in &mut self.works {
            if work.attachment() == Some(TestamentId(row.response.identity().binding.object.0)) {
                *work = work
                    .begin(
                        &work.binding(),
                        &parent,
                        Principal::Actor(ISSUER),
                        &row.response,
                    )
                    .unwrap();
            }
        }
        self.prefix = SessionSeq(sequence);
    }
    fn evaluation_index(&self, response: usize, declaration: u32) -> usize {
        let id = self.responses[response].response.identity().binding.object;
        self.evaluations.iter().position(|row| row.binding().object == binding(100 + u128::from(declaration)).object
            && matches!(row.target(), Target::Artifact { response, .. } | Target::MissingSlot { response, .. } if response.object == id)).unwrap()
    }
    fn begin(&mut self, response: usize, declaration: u32) {
        let at = self.evaluation_index(response, declaration);
        let definition = self
            .definitions
            .iter()
            .find(|row| row.declaration_index() == declaration)
            .unwrap();
        let evaluation = self.evaluations[at].bind(definition).unwrap();
        let aggregate = ClaimAggregation::new(&self.claim, policy_limits()).unwrap();
        let work = match evaluation.target() {
            Target::Artifact { artifact, .. } => self
                .works
                .iter()
                .find(|work| work.binding().object == artifact.object),
            _ => None,
        };
        let owner = evaluation
            .work_owner(
                &self.claim,
                &self.responses[response].response,
                work,
                &aggregate.decision(),
                0,
            )
            .unwrap();
        self.evaluations[at] = evaluation
            .begin(
                Principal::Actor(evaluation.evaluator().unwrap()),
                &evaluation.binding(),
                &owner,
            )
            .unwrap()
            .next
            .into_state();
    }
    fn report(
        &mut self,
        response: usize,
        declaration: u32,
        value: VerdictValue,
        at: PublicationPosition,
    ) {
        let index = self.evaluation_index(response, declaration);
        let definition = self
            .definitions
            .iter()
            .find(|row| row.declaration_index() == declaration)
            .unwrap();
        let evaluation = self.evaluations[index].bind(definition).unwrap();
        let work = match evaluation.target() {
            Target::Artifact { artifact, .. } => self
                .works
                .iter()
                .find(|work| work.binding().object == artifact.object),
            _ => None,
        };
        let owner = evaluation
            .work_report_owner(&self.claim, &self.responses[response].response, work, 0)
            .unwrap();
        let (report, evidence) = report_parts(&evaluation, value);
        let transition = evaluation
            .report(
                Principal::Actor(evaluation.evaluator().unwrap()),
                &evaluation.binding(),
                &owner,
                report,
                &evidence,
            )
            .unwrap();
        self.results.push((transition.result.unwrap(), at));
        self.evaluations[index] = transition.next.into_state();
        self.prefix = self.prefix.max(at.sequence);
    }
}

#[test]
fn borrowed_decision_iterators_cover_entered_responses_and_exact_artifacts_without_allocation() {
    let mut fixture = Fixture::new(&[(ValidationMode::Required, 1)], false);
    let first = fixture.receive(301, &[(0, 401)], 2);
    let second = fixture.receive(300, &[(0, 400)], 3);
    fixture.enter(first, 4);
    {
        let projection = fixture.projection();
        memory::fail_after(0, || {
            let mut decisions = projection.response_decisions();
            let first = decisions.next().unwrap().unwrap();
            assert_eq!(
                first.response_binding().object.0,
                TestamentId::from_u128(301).0
            );
            assert!(decisions.next().is_none());
            assert_eq!(projection.artifact_decisions().len(), 1);
            assert_eq!(
                projection.artifact_decisions()[0].artifact().id,
                ArtifactId::from_u128(401)
            );
        });
    }
    fixture.enter(second, 5);
    fixture.begin(first, 1);
    fixture.report(first, 1, VerdictValue::Pass, position(6, 1));
    let projection = fixture.projection();
    memory::fail_after(0, || {
        let mut decisions = projection.response_decisions();
        for id in [300, 301] {
            let actual = decisions.next().unwrap().unwrap();
            let expected = projection
                .response_decision(TestamentId::from_u128(id))
                .unwrap();
            assert_eq!(actual.response_binding(), expected.response_binding());
            assert_eq!(actual.response_outcome(), expected.response_outcome());
            assert_eq!(actual.sequence(), expected.sequence());
        }
        assert!(decisions.next().is_none());
        assert_eq!(projection.artifact_decisions().len(), 2);
        let passed = projection
            .artifact_decisions()
            .iter()
            .find(|value| value.artifact().id == ArtifactId::from_u128(401))
            .unwrap();
        assert_eq!(passed.outcome(), ArtifactOutcome::Passed);
        assert_eq!(passed.sequence(), SessionSeq(6));
    });
}

#[test]
fn received_rows_remain_pending_and_entry_decides_zero_check_presence() {
    for present in [false, true] {
        let mut fixture = Fixture::new(&[(ValidationMode::Required, 0)], false);
        let response = fixture.receive(300, if present { &[(0, 400)] } else { &[] }, 2);
        assert_eq!(
            fixture.projection().claim_decision().outcome(),
            AggregateOutcome::Pending
        );
        assert!(
            fixture
                .projection()
                .response_decision(TestamentId::from_u128(300))
                .is_none()
        );
        fixture.enter(response, 3);
        let projection = fixture.projection();
        if present {
            assert_eq!(
                projection.claim_decision().outcome(),
                AggregateOutcome::LocalComplete {
                    sequence: SessionSeq(3)
                }
            );
            assert_eq!(
                projection
                    .response_decision(TestamentId::from_u128(300))
                    .unwrap()
                    .outcome_for_slot(0)
                    .unwrap()
                    .outcome(),
                ArtifactOutcome::Passed
            );
        } else {
            let AggregateOutcome::Blocked(cut) = projection.claim_decision().outcome() else {
                panic!("missing slot")
            };
            assert_eq!(cut.sequence(), SessionSeq(3));
            assert_eq!(cut.cause().key().declaration_index, 50);
            assert_eq!(cut.cause().kind(), BlockingKind::Incomplete);
            assert_eq!(cut.cause().artifact(), None);
        }
    }
}

#[test]
fn complementary_partial_checks_on_distinct_artifacts_never_combine_into_a_slot_pass() {
    let mut fixture = Fixture::new(&[(ValidationMode::Required, 2)], false);
    let first = fixture.receive(300, &[(0, 400)], 2);
    let second = fixture.receive(301, &[(0, 401)], 3);
    fixture.enter(first, 4);
    fixture.enter(second, 5);
    fixture.begin(first, 1);
    fixture.begin(second, 2);
    fixture.report(first, 1, VerdictValue::Pass, position(6, 1));
    fixture.report(second, 2, VerdictValue::Pass, position(7, 1));
    let projection = fixture.projection();
    assert_eq!(
        projection.claim_decision().outcome(),
        AggregateOutcome::Pending
    );
    assert_eq!(projection.claim_decision().witnesses().count(), 0);
    for id in [300, 301] {
        assert_eq!(
            projection
                .response_decision(TestamentId::from_u128(id))
                .unwrap()
                .outcome_for_slot(0)
                .unwrap()
                .outcome(),
            ArtifactOutcome::Pending
        );
    }
}

#[test]
fn publication_order_preserves_first_terminal_cut_and_simultaneous_coverage_wins() {
    for (pass, fail) in [(6, 7), (7, 6), (6, 6)] {
        let mut fixture = Fixture::new(&[(ValidationMode::Required, 1)], false);
        let successful = fixture.receive(300, &[(0, 400)], 2);
        let failed = fixture.receive(301, &[(0, 401)], 3);
        fixture.enter(successful, 4);
        fixture.enter(failed, 5);
        fixture.begin(successful, 1);
        fixture.begin(failed, 1);
        fixture.report(successful, 1, VerdictValue::Pass, position(pass, 1));
        fixture.report(failed, 1, VerdictValue::Fail, position(fail, 2));
        // Source storage order intentionally disagrees with publication order.
        fixture.results.reverse();
        fixture.evaluations.reverse();
        let projection = fixture.projection();
        if pass <= fail {
            assert_eq!(
                projection.claim_decision().outcome(),
                AggregateOutcome::LocalComplete {
                    sequence: SessionSeq(pass)
                }
            );
        } else {
            let AggregateOutcome::Blocked(cut) = projection.claim_decision().outcome() else {
                panic!("first failure")
            };
            assert_eq!(cut.sequence(), SessionSeq(fail));
        }
        assert!(
            matches!(projection.response_decision(TestamentId::from_u128(301)).unwrap().response_outcome(),ResponseOutcome::Blocked(cut) if cut.sequence() == SessionSeq(fail))
        );
        let mut copied = fixture
            .claim
            .try_copy(fixture.claim.copy_charge().unwrap())
            .unwrap();
        copied
            .apply_aggregate(&copied.binding(), &projection.claim_decision())
            .unwrap();
        drop(projection);
        fixture.claim = copied;
        assert_eq!(
            fixture.projection().claim_decision().outcome(),
            if pass <= fail {
                AggregateOutcome::LocalComplete {
                    sequence: SessionSeq(pass),
                }
            } else {
                match fixture.claim.terminal_cut().unwrap() {
                    claim::ClaimTerminalCut::Required(cut) => AggregateOutcome::Blocked(cut),
                    _ => panic!("required"),
                }
            }
        );
    }
}

#[test]
fn a_sealed_empty_product_set_allows_whole_work_missing_slot_assessment() {
    use crate::lifecycle::artifact_descriptor::{
        ArtifactDescriptor, ArtifactSpec, Limits as ArtifactLimits, PayloadSpec, WorkProvenance,
        WorkRole,
    };
    let mut fixture = Fixture::new(&[(ValidationMode::Required, 0)], true);
    let parent = evidence::Parent::from_claim(&fixture.claim).unwrap();
    let descriptor = ArtifactDescriptor::prepare(
        ArtifactSpec {
            ledger: parent.ledger,
            id: ArtifactId::from_u128(500),
            schema: 1,
            kind: "error",
            schema_hash: ContentHash([8; 32]),
            metadata: b"{}",
            payload: PayloadSpec::Inline(
                br#"{"code":"work_failed","message":"No output could be produced."}"#,
            ),
            producer: parent.holder,
            receipt: Some(parent.receipt),
            result: None,
            work: Some(WorkProvenance {
                claim: parent.claim,
                cycle: parent.next_cycle,
                role: WorkRole::Diagnostic {
                    reason: evidence::EvidenceFailure::Work,
                },
            }),
            inputs: &[],
            visibility: &[],
        },
        ArtifactLimits {
            kind_bytes: 64,
            metadata_bytes: 128,
            inline_bytes: 1024,
            inputs: 0,
            visibility_labels: 0,
            visibility_label_bytes: 0,
            construction_bytes: 8192,
        },
    )
    .unwrap()
    .build()
    .unwrap();
    let reference = ArtifactRef {
        id: descriptor.id(),
        hash: descriptor.content_hash(),
    };
    let diagnostic = evidence::ResponseDiagnostic::record_native(
        &parent,
        Principal::Actor(parent.holder),
        parent.receipt,
        evidence::Diagnostic {
            reason: evidence::EvidenceFailure::Work,
            artifact: reference,
        },
        &descriptor,
        &EvidenceAttestation {
            descriptor_hash: descriptor.content_hash(),
            custody_revision: 1,
            durable: true,
            schema_valid: true,
        },
    )
    .unwrap();
    let response = fixture.receive_report(300, &[], 2, OutcomeKind::Failed, &[diagnostic]);
    assert_eq!(
        fixture.responses[response].response.reported_outcome(),
        OutcomeKind::Failed
    );
    assert_eq!(
        fixture.responses[response].response.diagnostics()[0].artifact(),
        reference
    );
    assert!(fixture.works.is_empty());
    assert!(!fixture.projection().claim_decision().increments_ready());
    fixture
        .registry
        .seal_increment_targets(&fixture.claim)
        .unwrap();
    assert!(fixture.projection().claim_decision().increments_ready());
    fixture.enter(response, 3);
    assert!(
        matches!(fixture.projection().claim_decision().outcome(),AggregateOutcome::Blocked(cut) if cut.cause().kind() == BlockingKind::Incomplete)
    );
}

#[test]
fn source_omissions_duplicate_positions_and_changed_original_cuts_are_rejected() {
    let mut fixture = Fixture::new(&[(ValidationMode::Required, 1)], false);
    let response = fixture.receive(300, &[(0, 400)], 2);
    fixture.enter(response, 3);
    fixture.begin(response, 1);
    fixture.report(response, 1, VerdictValue::Pass, position(4, 1));
    assert!(prepare_projection(&fixture.claim, &fixture.registry, &fixture, limits()).is_ok());
    let work = fixture.works.pop().unwrap();
    assert!(prepare_projection(&fixture.claim, &fixture.registry, &fixture, limits()).is_err());
    fixture.works.push(work);
    let old = fixture.results[1].1;
    fixture.results[1].1 = fixture.results[0].1;
    assert!(prepare_projection(&fixture.claim, &fixture.registry, &fixture, limits()).is_err());
    fixture.results[1].1 = old;
    let state = fixture.evaluations.pop().unwrap();
    assert!(prepare_projection(&fixture.claim, &fixture.registry, &fixture, limits()).is_err());
    fixture.evaluations.push(state);
    let projection = fixture.projection();
    let transition = fixture.responses[response]
        .response
        .plan_decision(
            &fixture.responses[response].response.identity().binding,
            &projection
                .response_decision(TestamentId::from_u128(300))
                .unwrap(),
        )
        .unwrap()
        .unwrap();
    drop(projection);
    fixture.responses[response]
        .response
        .apply(transition)
        .unwrap();
    fixture.results[1].1 = position(5, 1);
    fixture.prefix = SessionSeq(5);
    assert!(
        prepare_projection(&fixture.claim, &fixture.registry, &fixture, limits())
            .unwrap()
            .build()
            .is_err()
    );
}

#[test]
fn every_owned_buffer_is_precharged_and_fallible_construction_leaves_sources_unchanged() {
    let mut fixture = Fixture::new(&[(ValidationMode::Required, 1)], false);
    let response = fixture.receive(300, &[(0, 400)], 2);
    fixture.enter(response, 3);
    let charge = prepare_projection(&fixture.claim, &fixture.registry, &fixture, limits())
        .unwrap()
        .construction_charge();
    let before = fixture.claim.binding();
    memory::fail_after(0, || {
        assert!(
            prepare_projection(
                &fixture.claim,
                &fixture.registry,
                &fixture,
                ProjectionLimits {
                    bytes: charge - 1,
                    ..limits()
                }
            )
            .is_err()
        );
        assert_eq!(memory::remaining_allocations(), Some(0));
    });
    for failure in 0..11 {
        memory::fail_after(failure, || {
            assert!(
                prepare_projection(&fixture.claim, &fixture.registry, &fixture, limits())
                    .unwrap()
                    .build()
                    .is_err()
            );
        });
        assert_eq!(fixture.claim.binding(), before);
        assert_eq!(
            fixture.responses[response].response.state(),
            evidence::ResponseState::Validating
        );
        assert_eq!(fixture.evaluations[1].state(), v::State::Ready);
    }
    memory::fail_after(11, || {
        assert_eq!(
            fixture.projection().claim_decision().outcome(),
            AggregateOutcome::Pending
        );
    });
}
