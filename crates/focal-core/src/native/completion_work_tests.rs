use super::*;
use crate::native::completion_envelope::{
    CompletionEnvelope, CompletionSlots, CompletionUse, EvidenceBounds,
};
use crate::native::prepare::Scratch;
use focal_model::lifecycle::artifact_descriptor::Limits as ArtifactLimits;

fn evidence_bounds() -> EvidenceBounds {
    EvidenceBounds {
        workspace_bytes: 4096,
        retained_bytes: 512,
    }
}

fn quote_with(
    fixture: &Fixture,
    limits: NativeLimits,
) -> Result<
    (
        CompletionEnvelope,
        crate::native::completion_book::GraphMembers,
    ),
    NativeError,
> {
    let view = fixture.view();
    let registered =
        crate::native::admission_authority::registered_any(&view, fixture.claim(), fixture.key(1))?;
    CompletionEnvelope::derive_work(
        &view,
        limits,
        &registered,
        ArtifactLimits {
            kind_bytes: 128,
            metadata_bytes: 1024,
            inline_bytes: 65536,
            inputs: 16,
            visibility_labels: 16,
            visibility_label_bytes: 128,
            construction_bytes: 128 * 1024,
        },
        evidence_bounds(),
    )
}

fn quote(fixture: &Fixture) -> CompletionEnvelope {
    let (envelope, members) = quote_with(fixture, fixture.core.limits).unwrap();
    drop(members);
    envelope
}

fn check(fixture: &Fixture, envelope: CompletionEnvelope) -> Result<(), NativeError> {
    let reservation = fixture.core.state.budget.reserve(
        focal_memory::BudgetKind::Pending,
        BudgetLane::Ordinary,
        envelope.work_check_bytes(),
    )?;
    let result = envelope.check_work(
        &fixture.view(),
        fixture.core.limits,
        &mut Scratch {
            used: 0,
            max: envelope.work_check_bytes(),
        },
    );
    drop(reservation);
    result
}

#[test]
fn received_target_prices_all_retry_versions_and_graph_consequences_before_entry() {
    let fixture = Fixture::new(true);
    assert_eq!(fixture.response().state(), ResponseState::Received);
    let before = fixture.core.state.budget.stats();
    let envelope = quote(&fixture);
    assert_eq!(fixture.core.state.budget.stats(), before);
    assert!(envelope.is_work());
    assert!(envelope.supports_target(fixture.key(1).target));
    assert!(!envelope.supports_target(EvaluationTarget::Admission));
    let reports = envelope.reports();
    let cohort = envelope.cohort();
    assert!(
        cohort.evaluations()
            > fixture
                .view()
                .owned_claim(fixture.key(1).claim)
                .unwrap()
                .registrations()
                .unwrap()
                .rows()
                .len()
    );
    assert_eq!(reports, 2);
    let report_index = crate::native::index_rows::report_rows(16).unwrap();
    let status = crate::native::index_rows::STATUS_ROWS;
    assert_eq!(
        envelope.slots(),
        CompletionSlots {
            artifacts: 2,
            identities: 2,
            results: 2,
            outcomes: 2,
            events: 14 + 2 * cohort.events(),
            sequences: 2,
            // Eleven primary rows and the artifact's twenty index rows per
            // report (doc 22 §7).
            new_rows: 2 * (11 + report_index) + 2 * cohort.events(),
            ..CompletionSlots::default()
        }
    );
    let storage = envelope.report_storage(CompletionUse::Regular).unwrap();
    // Sixteen primary changes, the cohort's rows, the report's index rows, a
    // status move for the parent and every sealed cohort claim, and the due
    // timers the report can retire: every deleted key beyond the status moves.
    let timers = storage.limits().deleted_keys - (1 + cohort.claims());
    assert!(timers > 1 + cohort.claims() + cohort.evaluations());
    let changed =
        16 + cohort.changed_keys() + report_index + status * (1 + cohort.claims()) + timers;
    assert_eq!(storage.limits().changed_keys, changed);
    let journal = crate::native::completion_book::journal_bytes(1 + cohort.evaluations()).unwrap();
    let writes = crate::native::mutation::bytes(changed).unwrap();
    assert!(journal > 0);
    assert_eq!(
        envelope
            .per_report_retained_bytes(CompletionUse::Regular)
            .unwrap(),
        storage.additional_retained_bytes() + writes + journal,
        "each pending Work report owns its possible mixed journal"
    );
    assert_eq!(
        envelope.total_retained_bytes(),
        2 * (storage.additional_retained_bytes() + writes + journal)
    );
    assert!(
        envelope
            .report_storage(CompletionUse::AdmissionFailure)
            .is_err()
    );
    assert!(envelope.work_check_bytes() > 0);
    assert!(envelope.workspace_bytes() > envelope.work_check_bytes());
    assert!(
        envelope.descriptor_limits().construction_bytes < fixture.core.limits.preparation_bytes
    );
    check(&fixture, envelope).unwrap();
    assert_eq!(fixture.core.state.budget.stats(), before);
}

#[test]
fn full_future_projection_and_compound_scratch_refuse_before_any_retained_responsibility() {
    let fixture = Fixture::new(true);
    let before = fixture.core.state.budget.stats();
    for limits in [
        NativeLimits {
            plan_edges: 64,
            ..fixture.core.limits
        },
        NativeLimits {
            preparation_bytes: 1024,
            ..fixture.core.limits
        },
        NativeLimits {
            range: RangeConfig {
                max_batch_entries: 15,
                ..fixture.core.limits.range
            },
            ..fixture.core.limits
        },
        NativeLimits {
            evaluations_per_claim: 2,
            ..fixture.core.limits
        },
    ] {
        assert!(quote_with(&fixture, limits).is_err());
        assert_eq!(fixture.core.state.budget.stats(), before);
    }
    assert_eq!(
        fixture
            .core
            .native_evaluation(fixture.key(1))
            .unwrap()
            .state(),
        validation::State::Ready
    );
    assert!(quote_with(&fixture, fixture.core.limits).is_ok());
    assert_eq!(fixture.core.state.budget.stats(), before);
}

#[test]
fn missing_target_has_no_external_report_envelope_or_invented_artifact() {
    let fixture = Fixture::new(false);
    let before = fixture.core.state.budget.stats();
    assert!(quote_with(&fixture, fixture.core.limits).is_err());
    assert_eq!(fixture.core.state.budget.stats(), before);
    assert!(
        fixture
            .core
            .native_artifact(ArtifactId::from_u128(800))
            .is_none()
    );
}

#[test]
fn fixed_envelope_survives_actual_entry_and_later_response_history_and_registry_growth() {
    let mut fixture = Fixture::new(true);
    let envelope = quote(&fixture);
    fixture.send(
        ISSUER,
        NativeCommand::EnterWholeWork {
            claim: fixture.claim(),
            expected: fixture.response().identity().binding,
        },
    );
    check(&fixture, envelope).unwrap();
    let held = fixture.response().identity();
    let original_work = fixture
        .core
        .native_work(ArtifactId::from_u128(800))
        .unwrap()
        .state;
    fixture.send(
        SUBJECT,
        NativeCommand::CloseResponse {
            claim: fixture.claim(),
            response: binding(901),
            report: NativeResponseInput {
                summary: "A later independently authored response.".into(),
                confidence: Confidence::Committed,
                outcome: OutcomeKind::Complete,
                manifest: vec![],
                diagnostics: vec![],
            },
        },
    );
    let second = fixture
        .core
        .native_response(TestamentId::from_u128(901))
        .unwrap()
        .identity()
        .binding;
    fixture.send(
        SUBJECT,
        NativeCommand::PostResponse {
            claim: fixture.claim(),
            expected: second,
        },
    );
    let second = fixture
        .core
        .native_response(TestamentId::from_u128(901))
        .unwrap()
        .identity()
        .binding;
    fixture.send(
        ISSUER,
        NativeCommand::ReceiveResponse {
            claim: fixture.claim(),
            expected: second,
        },
    );
    assert_eq!(
        fixture
            .core
            .native_claim(fixture.key(1).claim)
            .unwrap()
            .response_count(),
        2
    );
    assert_eq!(
        fixture
            .core
            .native_registrations(fixture.key(1).claim)
            .unwrap()
            .rows()
            .len(),
        6
    );
    let registered = fixture
        .core
        .native_registrations(fixture.key(1).claim)
        .unwrap();
    // Each received response registers its Required Delivery declaration plus
    // both authored WholeWork checks. The second response has missing targets.
    {
        assert_eq!(
            registered
                .rows()
                .iter()
                .filter(|row| matches!(row.target(), validation::Target::Delivery { .. }))
                .count(),
            2
        );
        assert_eq!(
            registered
                .rows()
                .iter()
                .filter(|row| matches!(row.target(), validation::Target::Artifact { .. }))
                .count(),
            2
        );
        assert_eq!(
            registered
                .rows()
                .iter()
                .filter(|row| matches!(row.target(), validation::Target::MissingSlot { .. }))
                .count(),
            2
        );
    }
    assert_eq!(fixture.response().identity(), held);
    assert_eq!(
        fixture
            .core
            .native_work(ArtifactId::from_u128(800))
            .unwrap()
            .state,
        original_work
    );
    let before = fixture.core.state.budget.stats();
    check(&fixture, envelope).unwrap();
    assert_eq!(fixture.core.state.budget.stats(), before);
}

#[test]
fn incoming_topology_growth_is_detected_even_when_original_parent_row_did_not_change() {
    let mut fixture = Fixture::new(true);
    let envelope = quote(&fixture);
    let original = fixture.claim();
    let mut create = f::creation(77, 77, &[], None);
    let NativeCommand::Create { claims, .. } = &mut create.command else {
        panic!("create");
    };
    claims[0].definition.graph = focal_model::lifecycle::graph::Declaration::new(
        &[focal_model::lifecycle::graph::Obligation {
            kind: focal_model::lifecycle::graph::Kind::DependsOn,
            target: ClaimId::from_u128(1),
        }],
        1,
    )
    .unwrap();
    f::publish(&mut fixture.core, 77, create);
    assert_eq!(fixture.claim(), original);
    let before = fixture.core.state.budget.stats();
    assert!(check(&fixture, envelope).is_err());
    assert_eq!(fixture.core.state.budget.stats(), before);
    let larger = quote(&fixture);
    // The new dependent joins the graph: one more changed claim, its event
    // and its status move beside the report's index rows and the due timers
    // the report can retire.
    let cohort = larger.cohort();
    let storage = larger.report_storage(CompletionUse::Regular).unwrap();
    let timers = storage.limits().deleted_keys - (2 + cohort.claims());
    assert_eq!(
        storage.limits().changed_keys,
        18 + cohort.changed_keys()
            + crate::native::index_rows::report_rows(16).unwrap()
            + crate::native::index_rows::STATUS_ROWS * (2 + cohort.claims())
            + timers
    );
    assert_eq!(larger.slots().events, 16 + 2 * larger.cohort().events());
    assert_eq!(
        larger.slots().new_rows,
        2 * (12 + crate::native::index_rows::report_rows(16).unwrap()) + 2 * cohort.events()
    );
    check(&fixture, larger).unwrap();
}

#[test]
fn temporary_graph_capture_pressure_leaves_original_source_and_later_retry_intact() {
    let fixture = Fixture::new(true);
    let before = fixture.core.state.budget.stats();
    let free = fixture
        .core
        .state
        .budget
        .limit()
        .saturating_sub(before.used);
    let pressure = fixture
        .core
        .state
        .budget
        .reserve(
            focal_memory::BudgetKind::Payload,
            BudgetLane::Completion,
            free,
        )
        .unwrap();
    let pressured = fixture.core.state.budget.stats();
    assert!(quote_with(&fixture, fixture.core.limits).is_err());
    assert_eq!(fixture.core.state.budget.stats(), pressured);
    drop(pressure);
    assert_eq!(fixture.core.state.budget.stats(), before);
    let envelope = quote(&fixture);
    check(&fixture, envelope).unwrap();
    assert_eq!(fixture.core.state.budget.stats(), before);
}
