use super::*;
use crate::lifecycle::{Principal, claim, graph, scope, succession, validation as v};
use crate::{ObjectId, ReceiptId, RootCommandId};
use v::tests::{EVALUATOR, ISSUER, binding, programmatic};

fn limits() -> AdmissionLimits {
    AdmissionLimits {
        declarations: 16,
        evaluations: 16,
        visits: 4096,
    }
}
fn specification(index: u32, mode: ValidationMode, quality: bool) -> v::DeclarationSpec<'static> {
    let mut spec = v::tests::specification(mode, programmatic(quality));
    spec.binding = binding(201 + u128::from(index));
    spec.declaration_index = index;
    spec.target = v::TargetDeclaration::Admission;
    spec.phase = crate::ValidationPhase::Admission;
    if index == 0 {
        spec.target = v::TargetDeclaration::Delivery;
        spec.phase = crate::ValidationPhase::WholeWork;
        spec.kind = crate::ValidationKind::Receipt;
        spec.program = v::Program::Delivery;
    }
    spec
}
fn definition(index: u32, mode: ValidationMode, quality: bool) -> Declaration {
    Declaration::new(
        Principal::Actor(ISSUER),
        specification(index, mode, quality),
        v::tests::limits(),
    )
    .unwrap()
}
#[derive(Clone, Copy)]
struct Record {
    result: AcceptedResult,
    sequence: SessionSeq,
    ordinal: u32,
}
struct View<'a> {
    definitions: &'a [Declaration],
    states: &'a [EvaluationState],
    records: &'a [Record],
    prefix: SessionSeq,
}
impl AdmissionView for View<'_> {
    fn prefix(&self) -> SessionSeq {
        self.prefix
    }
    fn declaration(&self, id: ValidationId) -> Option<&Declaration> {
        self.definitions
            .iter()
            .find(|row| row.binding().object.0 == id.0)
    }
    fn evaluation(&self, registered: RegisteredEvaluation) -> Option<&EvaluationState> {
        // Select by the actual retained lookup key, not equality with the
        // expected state: the projection must detect substituted row content.
        self.states
            .iter()
            .find(|row| row.binding().object == registered.binding().object)
    }
    fn accepted(&self, result: &AcceptedResult) -> Option<PublishedAdmissionResult<'_>> {
        self.records
            .iter()
            .find(|row| row.result.binding() == result.binding())
            .map(|row| PublishedAdmissionResult {
                result: &row.result,
                sequence: row.sequence,
                ordinal: row.ordinal,
            })
    }
}
struct Scenario {
    definitions: Vec<Declaration>,
    claim: ClaimState,
    registrations: RegistrationSet,
    states: Vec<EvaluationState>,
    records: Vec<Record>,
}
impl Scenario {
    fn new(requirements: &[(ValidationMode, bool)], reversed: bool) -> Self {
        let mut definitions = vec![definition(0, ValidationMode::Required, false)];
        for (index, (mode, quality)) in requirements.iter().copied().enumerate() {
            definitions.push(definition(u32::try_from(index + 1).unwrap(), mode, quality));
        }
        let policy = AcceptancePolicy::new(
            binding(200),
            ISSUER,
            &[],
            &definitions,
            super::super::Limits {
                max_slots: 4,
                max_checks: 16,
                max_results: 16,
                max_updates: 16,
            },
        )
        .unwrap();
        let mut claim = ClaimState::generate(
            Principal::Actor(ISSUER),
            claim::ClaimDefinition {
                binding: binding(200),
                issuer: ISSUER,
                subject: EVALUATOR,
                deadline: None,
                max_responses: 2,
                created: SessionSeq(1),
                graph: graph::Declaration::empty(),
                lineage: succession::Lineage::root(binding(200), RootCommandId::from_u128(1))
                    .unwrap(),
                acceptance: policy,
                scope_limits: scope::ScopeLimits {
                    scopes: 2,
                    roots: 4,
                    children: 4,
                },
            },
        )
        .unwrap();
        claim
            .post_owned(Principal::Actor(ISSUER), claim.binding())
            .unwrap();
        let mut registrations = RegistrationSet::new(&claim, 16, 65536).unwrap();
        let mut states = Vec::new();
        let mut positions = (1..definitions.len()).collect::<Vec<_>>();
        if reversed {
            positions.reverse();
        }
        for index in positions {
            let evaluation = v::Evaluation::materialize_admission(
                Principal::Actor(ISSUER),
                &definitions[index],
                &claim,
                1,
            )
            .unwrap();
            registrations.register(&claim, &evaluation, 65536).unwrap();
            states.push(evaluation.into_state());
        }
        Self {
            definitions,
            claim,
            registrations,
            states,
            records: Vec::new(),
        }
    }
    fn view(&self) -> View<'_> {
        View {
            definitions: &self.definitions,
            states: &self.states,
            records: &self.records,
            prefix: SessionSeq(20),
        }
    }
    fn decision(&self) -> AdmissionDecision<'_> {
        project_admission(&self.claim, &self.registrations, &self.view(), limits()).unwrap()
    }
    fn position(&self, index: usize) -> usize {
        self.states
            .iter()
            .position(|row| row.binding().object == self.definitions[index].binding().object)
            .unwrap()
    }
    fn begin(&mut self, index: usize) {
        let position = self.position(index);
        let ready = self.states[position]
            .bind(&self.definitions[index])
            .unwrap();
        self.states[position] = ready
            .begin(
                Principal::Actor(ready.evaluator().unwrap()),
                &ready.binding(),
                &ready.admission_owner(&self.claim, 1).unwrap(),
            )
            .unwrap()
            .next
            .into_state();
    }
    fn report(&mut self, index: usize, value: VerdictValue, sequence: u64, ordinal: u32) {
        let position = self.position(index);
        let evaluation = self.states[position]
            .bind(&self.definitions[index])
            .unwrap();
        let (mut report, mut evidence) = v::tests::report_parts(&evaluation, value);
        evidence.binding.object = ObjectId::from_u128(
            10000 * u128::try_from(index).unwrap() + u128::from(report.attempt.index),
        );
        report.evidence.id = ArtifactId(evidence.binding.object.0);
        let owner = evaluation.admission_report_owner(&self.claim, 1).unwrap();
        let transition = evaluation
            .report(
                Principal::Actor(report.attempt.evaluator),
                &evaluation.binding(),
                &owner,
                report,
                &evidence,
            )
            .unwrap();
        self.records.push(Record {
            result: transition.result.unwrap(),
            sequence: SessionSeq(sequence),
            ordinal,
        });
        self.states[position] = transition.next.into_state();
    }
    fn apply_failure(&mut self) {
        let mut next = self.claim.clone();
        next.apply_admission(&next.binding(), &self.decision())
            .unwrap();
        self.claim = next;
    }
}

#[test]
fn all_required_passes_allow_actual_receipt_without_testimony_while_observe_finishes_late() {
    let mut s = Scenario::new(
        &[
            (ValidationMode::Required, false),
            (ValidationMode::Observe, false),
        ],
        false,
    );
    assert_eq!(s.decision().outcome(), AdmissionOutcome::Pending);
    s.begin(1);
    s.begin(2);
    s.report(1, VerdictValue::Pass, 5, 2);
    assert_eq!(s.decision().outcome(), AdmissionOutcome::Passed);
    assert_eq!(s.decision().blocking_publication(), None);
    let graph = graph::Snapshot::capture(
        &[&s.claim],
        graph::Limits {
            nodes: 4,
            edges: 4,
            visits: 32,
        },
    )
    .unwrap();
    let start = graph.start(ClaimId::from_u128(200)).unwrap();
    let mut next = s.claim.clone();
    next.acquire_receipt(
        &next.binding(),
        Principal::Actor(EVALUATOR),
        ReceiptFence {
            receipt: ReceiptId::from_u128(9),
            epoch: 1,
        },
        &s.decision(),
        &start,
        &[],
    )
    .unwrap();
    s.claim = next;
    assert_eq!(s.claim.status(), ClaimStatus::Received);
    assert_eq!(s.claim.response_count(), 0);
    s.report(2, VerdictValue::Fail, 8, 3);
    assert_eq!(s.decision().outcome(), AdmissionOutcome::Passed);
    assert_eq!(s.records.len(), 2);
    assert_eq!(
        s.states[s.position(2)].state(),
        v::State::ValidationFailedNotRequired
    );
}

#[test]
fn first_result_cut_survives_parent_failure_and_later_stronger_sibling_failure() {
    let mut s = Scenario::new(
        &[
            (ValidationMode::Required, false),
            (ValidationMode::Required, false),
        ],
        false,
    );
    s.begin(1);
    s.begin(2);
    s.report(2, VerdictValue::Incomplete, 5, 8);
    let AdmissionOutcome::Blocked(original) = s.decision().outcome() else {
        panic!()
    };
    assert_eq!(original.sequence(), SessionSeq(5));
    assert_eq!(original.cause().kind(), BlockingKind::Incomplete);
    assert_eq!(
        s.decision().blocking_publication(),
        Some((SessionSeq(5), 8))
    );
    s.apply_failure();
    assert_eq!(s.claim.status(), ClaimStatus::PostFailed);
    s.report(1, VerdictValue::Fail, 9, 0);
    assert_eq!(s.decision().outcome(), AdmissionOutcome::Blocked(original));
    assert_eq!(
        s.claim.terminal_cut(),
        Some(ClaimTerminalCut::Required(original))
    );
    assert_eq!(s.claim.local_sealed_at(), Some(SessionSeq(5)));
}

#[test]
fn simultaneous_causes_use_declaration_order_independent_of_registry_and_event_order() {
    for reverse in [false, true] {
        let mut s = Scenario::new(
            &[
                (ValidationMode::Required, false),
                (ValidationMode::Required, false),
            ],
            reverse,
        );
        s.begin(1);
        s.begin(2);
        s.report(2, VerdictValue::Fail, 5, 0);
        s.report(1, VerdictValue::Incomplete, 5, 9);
        let decision = s.decision();
        let AdmissionOutcome::Blocked(cut) = decision.outcome() else {
            panic!()
        };
        assert_eq!(cut.cause().key().declaration_index, 1);
        assert_eq!(cut.cause().kind(), BlockingKind::Incomplete);
        assert_eq!(decision.blocking_publication(), Some((SessionSeq(5), 9)));
    }
}

#[test]
fn retryable_errors_and_programmatic_pass_before_quality_remain_pending() {
    let mut s = Scenario::new(&[(ValidationMode::Required, true)], false);
    s.begin(1);
    s.report(1, VerdictValue::Error, 4, 0);
    assert_eq!(s.decision().outcome(), AdmissionOutcome::Pending);
    s.report(1, VerdictValue::Pass, 5, 0);
    assert_eq!(s.states[0].state(), v::State::ValidatingQualityBar);
    assert_eq!(s.decision().outcome(), AdmissionOutcome::Pending);
    s.report(1, VerdictValue::Pass, 6, 0);
    assert_eq!(s.decision().outcome(), AdmissionOutcome::Passed);
    assert_eq!(s.records.len(), 3);
}

#[test]
fn final_error_uses_exhaustion_result_cut_not_first_attempt_or_current_projection() {
    let mut s = Scenario::new(&[(ValidationMode::Required, false)], false);
    s.begin(1);
    for sequence in 4..7 {
        s.report(1, VerdictValue::Error, sequence, 0);
        if sequence < 6 {
            assert_eq!(s.decision().outcome(), AdmissionOutcome::Pending);
        }
    }
    let AdmissionOutcome::Blocked(cut) = s.decision().outcome() else {
        panic!()
    };
    assert_eq!(cut.sequence(), SessionSeq(6));
    assert_eq!(cut.cause().key().attempt, Some(2));
    assert_eq!(cut.cause().kind(), BlockingKind::Errored);
}

#[test]
fn complete_manifest_registry_rows_and_publication_are_required_even_for_observe() {
    let mut s = Scenario::new(
        &[
            (ValidationMode::Required, false),
            (ValidationMode::Observe, false),
        ],
        false,
    );
    let mut incomplete = RegistrationSet::new(&s.claim, 16, 65536).unwrap();
    incomplete
        .register(
            &s.claim,
            &s.states[s.position(1)].bind(&s.definitions[1]).unwrap(),
            65536,
        )
        .unwrap();
    s.begin(1);
    s.report(1, VerdictValue::Pass, 5, 0);
    assert_eq!(
        project_admission(&s.claim, &incomplete, &s.view(), limits()).unwrap_err(),
        ContractError::MissingEvidence
    );
    for change in 0..3 {
        let mut view = s.view();
        match change {
            0 => view.definitions = &s.definitions[..2],
            1 => view.states = &s.states[..1],
            _ => view.records = &[],
        }
        assert_eq!(
            project_admission(&s.claim, &s.registrations, &view, limits()).unwrap_err(),
            ContractError::MissingEvidence
        );
    }
}

#[test]
fn current_definition_generation_and_actual_result_cannot_be_substituted() {
    let mut s = Scenario::new(&[(ValidationMode::Required, false)], false);
    let mut altered = specification(1, ValidationMode::Required, false);
    altered.deadline.at += 1;
    let definitions = vec![
        definition(0, ValidationMode::Required, false),
        Declaration::new(Principal::Actor(ISSUER), altered, v::tests::limits()).unwrap(),
    ];
    let view = View {
        definitions: &definitions,
        ..s.view()
    };
    assert!(project_admission(&s.claim, &s.registrations, &view, limits()).is_err());
    let changed = [v::Evaluation::materialize_admission(
        Principal::Actor(ISSUER),
        &s.definitions[1],
        &s.claim,
        2,
    )
    .unwrap()
    .into_state()];
    let view = View {
        states: &changed,
        ..s.view()
    };
    assert_eq!(
        project_admission(&s.claim, &s.registrations, &view, limits()).unwrap_err(),
        ContractError::StaleEvaluation
    );
    s.begin(1);
    s.report(1, VerdictValue::Fail, 5, 0);
    let mut other = Scenario::new(&[(ValidationMode::Required, false)], false);
    other.begin(1);
    other.report(1, VerdictValue::Pass, 5, 0);
    let view = View {
        records: &other.records,
        ..s.view()
    };
    assert_eq!(
        project_admission(&s.claim, &s.registrations, &view, limits()).unwrap_err(),
        ContractError::ContentConflict
    );
}

#[test]
fn original_publication_bounds_and_unique_ordinals_are_checked() {
    let mut s = Scenario::new(
        &[
            (ValidationMode::Required, false),
            (ValidationMode::Observe, false),
        ],
        false,
    );
    s.begin(1);
    s.begin(2);
    s.report(1, VerdictValue::Pass, 5, 0);
    s.report(2, VerdictValue::Fail, 5, 1);
    for sequence in [0, 21] {
        let mut records = s.records.clone();
        records[0].sequence = SessionSeq(sequence);
        let view = View {
            records: &records,
            ..s.view()
        };
        assert_eq!(
            project_admission(&s.claim, &s.registrations, &view, limits()).unwrap_err(),
            ContractError::InvalidCut
        );
    }
    s.records[1].ordinal = 0;
    assert_eq!(
        project_admission(&s.claim, &s.registrations, &s.view(), limits()).unwrap_err(),
        ContractError::ConflictingCause
    );
}

#[test]
fn existing_terminal_cause_cannot_be_reassigned_to_a_different_history_cut() {
    let mut s = Scenario::new(&[(ValidationMode::Required, false)], false);
    s.begin(1);
    s.report(1, VerdictValue::Fail, 5, 2);
    s.apply_failure();
    let original = s.claim.terminal_cut();
    s.records[0].sequence = SessionSeq(6);
    assert_eq!(
        project_admission(&s.claim, &s.registrations, &s.view(), limits()).unwrap_err(),
        ContractError::ConflictingCause
    );
    assert_eq!(s.claim.terminal_cut(), original);
}

#[test]
fn projection_bounds_refuse_without_mutating_rows_or_requiring_a_duplicate_aggregate() {
    let s = Scenario::new(&[(ValidationMode::Required, false)], false);
    let before = s.states.clone();
    for limit in [
        AdmissionLimits {
            declarations: 1,
            ..limits()
        },
        AdmissionLimits {
            evaluations: 0,
            ..limits()
        },
        AdmissionLimits {
            visits: 0,
            ..limits()
        },
    ] {
        assert_eq!(
            project_admission(&s.claim, &s.registrations, &s.view(), limit).unwrap_err(),
            ContractError::Capacity
        );
        assert_eq!(s.states, before);
    }
    assert_eq!(s.decision().outcome(), AdmissionOutcome::Pending);
}

#[test]
fn no_admission_requirements_have_vacuous_pass_with_actual_complete_definition() {
    let s = Scenario::new(&[], false);
    assert!(s.registrations.rows().is_empty());
    assert_eq!(s.definitions.len(), 1);
    assert_eq!(s.decision().outcome(), AdmissionOutcome::Passed);
    let view = View {
        definitions: &[],
        ..s.view()
    };
    assert_eq!(
        project_admission(&s.claim, &s.registrations, &view, limits()).unwrap_err(),
        ContractError::MissingEvidence
    );
}
