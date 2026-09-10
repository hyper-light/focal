use super::super::report_tests::{
    EVALUATOR, artifact_spec, binding, context, creation, descriptor, key, prepared, publish,
    report_for, running,
};
use super::*;
use focal_evidence::{BuiltinNativeSchemas, ContentStore, StoreLimits, VerifiedNativeArtifact};
use focal_memory::{BudgetLane, Change, Entry};
use focal_model::lifecycle::{
    aggregation,
    artifact_descriptor::{ArtifactSpec, ContentPointer},
    creation::Owner,
    succession::Lineage,
};
use focal_model::{
    ArtifactId, ArtifactRef, Cause, ContentClass, ContentDomainId, ObjectId, ObjectKind,
    VerdictValue,
};

#[path = "completion_admission_graph_tests.rs"]
mod admission_graph_tests;
#[path = "completion_pending_journal_tests.rs"]
mod pending_journal_tests;

fn parts(core: &Core<NativeState>) -> (&ClaimState, &RegistrationSet, &validation::Declaration) {
    let Row::Claim(owner) = core.state.rows.get(&Key::Claim(key(1).claim)).unwrap() else {
        panic!("claim row");
    };
    (
        owner.claim().unwrap(),
        owner.registrations().unwrap(),
        core.native_definition(key(1).validation).unwrap(),
    )
}

fn evidence() -> EvidenceBounds {
    EvidenceBounds {
        workspace_bytes: 16 * 1024 * 1024,
        retained_bytes: size_of::<VerifiedNativeArtifact>(),
    }
}

fn envelope(core: &Core<NativeState>) -> CompletionEnvelope {
    let (claim, registrations, declaration) = parts(core);
    CompletionEnvelope::derive(
        &core.state.rows,
        core.limits,
        claim,
        registrations,
        declaration,
        descriptor_limits(core.limits, claim, registrations).unwrap(),
        evidence(),
    )
    .unwrap()
}

fn content() -> PayloadSpec<'static> {
    PayloadSpec::Content(ContentPointer {
        domain: ContentDomainId::from_u128(90),
        root: ContentHash([90; 32]),
        length: u64::MAX,
        class: ContentClass::Evidence,
    })
}

fn input(spec: ArtifactSpec<'_>, limits: ArtifactLimits) -> NativeArtifactInput {
    NativeArtifactInput::new(
        ArtifactDescriptor::prepare(spec, limits)
            .unwrap()
            .build()
            .unwrap(),
    )
    .unwrap()
}

#[test]
fn full_chain_prices_all_retained_versions_and_only_one_parent_failure() {
    for mode in [ValidationMode::Required, ValidationMode::Observe] {
        for quality in [false, true] {
            let core = running(&[(mode, quality)]);
            let envelope = envelope(&core);
            let reports = if quality { 3 } else { 2 };
            let failure = usize::from(mode == ValidationMode::Required);
            let cohort = envelope.cohort();
            assert_eq!(cohort.claims(), failure);
            assert_eq!(cohort.evaluations(), failure * 5);
            assert_eq!(envelope.reports(), reports);
            assert_eq!(
                envelope.slots(),
                CompletionSlots {
                    artifacts: reports as usize,
                    identities: reports as usize,
                    results: reports as usize,
                    outcomes: reports as usize,
                    sequences: u64::from(reports),
                    events: 3 * reports as usize + failure + cohort.events(),
                    // Seven primary rows and twenty-one index rows per report.
                    new_rows: 28 * reports as usize + failure + cohort.events(),
                    ..CompletionSlots::default()
                }
            );
            // Nine primary writes, the report's twenty-one index rows and the
            // due timers of the reported and every sealed cohort evaluation.
            let report_index = crate::native::index_rows::report_rows(16).unwrap();
            let regular_timers =
                crate::native::index_rows::timer_rows(0, 1 + cohort.evaluations(), 0).unwrap();
            assert_eq!(
                envelope
                    .report_storage(CompletionUse::Regular)
                    .unwrap()
                    .limits()
                    .changed_keys,
                9 + report_index + regular_timers
            );
            let regular = envelope
                .per_report_retained_bytes(CompletionUse::Regular)
                .unwrap();
            assert_eq!(
                regular,
                envelope
                    .report_storage(CompletionUse::Regular)
                    .unwrap()
                    .additional_retained_bytes()
                    + crate::native::mutation::bytes(9 + report_index + regular_timers).unwrap(),
                "a report keeps its write set and an inline credit update"
            );
            let expected = if failure != 0 {
                // Eleven primary rows, the cohort's rows, the report's index
                // rows, a status move for the parent and every sealed cohort
                // claim, and the due timers of every moved claim and every
                // affected evaluation.
                let failed = 11
                    + cohort.changed_keys()
                    + report_index
                    + crate::native::index_rows::STATUS_ROWS * (1 + cohort.claims())
                    + crate::native::index_rows::timer_rows(0, 1 + cohort.evaluations(), 0)
                        .unwrap();
                assert_eq!(
                    envelope
                        .report_storage(CompletionUse::AdmissionFailure)
                        .unwrap()
                        .limits()
                        .changed_keys,
                    failed
                );
                assert_eq!(
                    envelope
                        .per_report_retained_bytes(CompletionUse::AdmissionFailure)
                        .unwrap(),
                    envelope
                        .report_storage(CompletionUse::AdmissionFailure)
                        .unwrap()
                        .additional_retained_bytes()
                        + crate::native::mutation::bytes(failed).unwrap()
                        + crate::native::completion_book::journal_bytes(1 + cohort.evaluations())
                            .unwrap(),
                    "a pending failure retains the full collected update buffer"
                );
                (reports as usize - 1) * regular
                    + envelope
                        .per_report_retained_bytes(CompletionUse::AdmissionFailure)
                        .unwrap()
            } else {
                assert!(
                    envelope
                        .report_storage(CompletionUse::AdmissionFailure)
                        .is_err()
                );
                reports as usize * regular
            };
            assert_eq!(envelope.total_retained_bytes(), expected);
            assert_eq!(
                envelope.required_bytes(),
                expected + envelope.workspace_bytes()
            );
        }
    }
}

#[test]
fn cohort_visit_boundary_is_distinct_from_the_unchanged_scratch_byte_limit() {
    let core = running(&[(ValidationMode::Required, false)]);
    let (claim, registry, declaration) = parts(&core);
    let cohort = cohort_bound(core.limits, claim, registry).unwrap();
    let writer = cohort.writer_visits(1, 4).unwrap();
    let journal = cohort.journal_visits(4, 1).unwrap();
    assert!(writer > 256);
    assert!(journal <= writer);
    let descriptor = descriptor_limits(core.limits, claim, registry).unwrap();
    let before = core.native_budget();
    let limited = NativeLimits {
        plan_edges: writer - 1,
        ..core.limits
    };
    assert!(matches!(
        CompletionEnvelope::derive(
            &core.state.rows,
            limited,
            claim,
            registry,
            declaration,
            descriptor,
            evidence(),
        ),
        Err(NativeError::Capacity("cohort seal writer visits"))
    ));
    let exact = NativeLimits {
        plan_edges: writer,
        ..core.limits
    };
    let quote = CompletionEnvelope::derive(
        &core.state.rows,
        exact,
        claim,
        registry,
        declaration,
        descriptor,
        evidence(),
    )
    .unwrap();
    assert_eq!(exact.preparation_bytes, core.limits.preparation_bytes);
    assert_eq!(quote.cohort(), cohort);
    assert_eq!(core.native_budget(), before);
}

#[test]
fn completion_use_follows_actual_admission_transition_not_changed_row_count() {
    use super::super::report_tests as fixture;
    for (mode, value, expected) in [
        (
            ValidationMode::Required,
            VerdictValue::Fail,
            CompletionUse::AdmissionFailure,
        ),
        (
            ValidationMode::Required,
            VerdictValue::Error,
            CompletionUse::Regular,
        ),
        (
            ValidationMode::Observe,
            VerdictValue::Fail,
            CompletionUse::Regular,
        ),
    ] {
        let core = running(&[(mode, false)]);
        let before = core.native_claim(key(1).claim).unwrap();
        let captured = ReportParent::capture(before);
        assert_eq!(captured.claim_id(), key(1).claim);
        assert_eq!(
            captured
                .completion_use(NativeOperation::ReportAdmission, None)
                .unwrap(),
            CompletionUse::Regular
        );
        assert_eq!(
            captured
                .completion_use(NativeOperation::ReportAdmission, Some(before))
                .unwrap(),
            CompletionUse::Regular
        );
        let mut custody = fixture::Custody::new();
        let report = report_for(
            &core,
            None,
            91,
            1,
            value,
            descriptor(artifact_spec(991, EVALUATOR, value)),
        );
        let verified = fixture::verified(&mut custody, &report);
        let prepared = fixture::report(&core, report, &[], &verified);
        let after = prepared.claim(key(1).claim).unwrap();
        assert_eq!(
            captured
                .completion_use(prepared.outcome().operation, Some(after))
                .unwrap(),
            expected
        );
        // Preparation must carry the original failure event, even when the
        // final row happens to have advanced exactly once.
        let unproven =
            captured.completion_use_prepared(prepared.outcome().operation, Some(after), None);
        if expected == CompletionUse::AdmissionFailure {
            assert!(matches!(
                unproven,
                Err(NativeError::Contract(ContractError::InvalidCut))
            ));
        } else {
            assert_eq!(unproven.unwrap(), expected);
        }
        // The same changed parent is not a surcharge on another operation.
        for operation in [
            NativeOperation::ReportIncrement,
            NativeOperation::EnterWholeWork,
        ] {
            assert_eq!(
                captured.completion_use(operation, Some(after)).unwrap(),
                CompletionUse::Regular
            );
        }
        let repeated = ReportParent::capture(after);
        assert_eq!(
            repeated
                .completion_use(NativeOperation::ReportAdmission, Some(after))
                .unwrap(),
            CompletionUse::Regular
        );
    }
}

#[test]
fn admission_surcharge_refuses_other_control_and_foreign_changed_claims() {
    use super::super::report_tests as fixture;
    let mut core = running(&[(ValidationMode::Required, false)]);
    let old = core.native_claim(key(1).claim).unwrap();
    let captured = ReportParent::capture(old);
    let expected = old.binding();
    let cancelled = prepared(core.prepare_native(
        context(fixture::ISSUER, 50),
        NativeInput {
            request: fixture::request(fixture::ISSUER, 70),
            command: NativeCommand::Cancel { expected },
        },
        &[],
    ));
    assert!(
        captured
            .completion_use(
                NativeOperation::ReportAdmission,
                cancelled.claim(key(1).claim)
            )
            .is_err()
    );
    publish(&mut core, 50, creation(71, 2, &[], None));
    assert!(
        captured
            .completion_use(
                NativeOperation::ReportAdmission,
                core.native_claim(ClaimId::from_u128(2))
            )
            .is_err()
    );
}

#[test]
fn verification_and_construction_workspace_is_shared_across_attempts() {
    let short = running(&[(ValidationMode::Required, false)]);
    let long = running(&[(ValidationMode::Required, true)]);
    let a = envelope(&short);
    let b = envelope(&long);
    assert_eq!(a.workspace_bytes(), b.workspace_bytes());
    assert_eq!(
        b.total_retained_bytes() - a.total_retained_bytes(),
        a.per_report_retained_bytes(CompletionUse::Regular).unwrap()
    );
    let (claim, registrations, declaration) = parts(&short);
    let extra = CompletionEnvelope::derive(
        &short.state.rows,
        short.limits,
        claim,
        registrations,
        declaration,
        a.descriptor_limits(),
        EvidenceBounds {
            workspace_bytes: evidence().workspace_bytes + 123,
            retained_bytes: evidence().retained_bytes + 7,
        },
    )
    .unwrap();
    assert_eq!(extra.total_retained_bytes(), a.total_retained_bytes());
    assert_eq!(extra.required_bytes() - a.required_bytes(), 130);
}

#[test]
fn descriptor_cap_leaves_room_for_parent_registry_and_all_result_containers() {
    let core = running(&[(ValidationMode::Required, true)]);
    let envelope = envelope(&core);
    let caps = envelope.descriptor_limits();
    let mut spec = artifact_spec(900, EVALUATOR, VerdictValue::Error);
    spec.payload = content();
    spec.visibility = &[];
    let heap = caps.construction_bytes - size_of::<ArtifactDescriptor>();
    let metadata = vec![b'x'; heap - spec.kind.len()];
    spec.metadata = &metadata;
    let largest = input(spec, caps);
    envelope.check_descriptor(&largest).unwrap();
    assert_eq!(
        largest.get().unwrap().retained_bytes().unwrap(),
        caps.construction_bytes
    );
    let compound = largest.heap_charge().unwrap()
        + result_containers().unwrap()
        + failure_scratch(envelope.parent_heap, envelope.registry_heap).unwrap()
        + envelope.cohort().construction_bytes().unwrap();
    assert!(compound <= core.limits.preparation_bytes);
    assert!(largest.heap_charge().unwrap() < core.limits.preparation_bytes);

    let too_large = vec![b'x'; metadata.len() + 1];
    spec.metadata = &too_large;
    let mut wider = caps;
    wider.metadata_bytes = too_large.len();
    wider.construction_bytes += 1;
    let exceeded = input(spec, wider);
    assert!(envelope.check_descriptor(&exceeded).is_err());
}

#[test]
fn each_pinned_descriptor_dimension_is_enforced_and_content_is_not_inline_bytes() {
    let core = running(&[(ValidationMode::Required, false)]);
    let (claim, registrations, declaration) = parts(&core);
    let narrow = ArtifactLimits {
        kind_bytes: 5,
        metadata_bytes: 8,
        inline_bytes: 64,
        inputs: 1,
        visibility_labels: 1,
        visibility_label_bytes: 4,
        construction_bytes: 4096,
    };
    let envelope = CompletionEnvelope::derive(
        &core.state.rows,
        core.limits,
        claim,
        registrations,
        declaration,
        narrow,
        evidence(),
    )
    .unwrap();
    let wider = descriptor_limits(core.limits, claim, registrations).unwrap();
    let references = [
        ObjectRef {
            ledger: binding(1).ledger,
            kind: ObjectKind::Claim,
            id: ObjectId::from_u128(1),
        },
        ObjectRef {
            ledger: binding(1).ledger,
            kind: ObjectKind::Claim,
            id: ObjectId::from_u128(2),
        },
    ];
    let payload = [b'x'; 65];
    for case in 0..6 {
        let mut spec = artifact_spec(900 + case, EVALUATOR, VerdictValue::Error);
        spec.metadata = b"{}";
        spec.visibility = &[];
        spec.payload = content();
        match case {
            0 => spec.kind = "errors",
            1 => spec.metadata = b"123456789",
            2 => spec.payload = PayloadSpec::Inline(&payload),
            3 => spec.inputs = &references,
            4 => spec.visibility = &["aaaa", "bbbb"],
            5 => spec.visibility = &["aaaaa"],
            _ => unreachable!(),
        }
        assert!(
            envelope.check_descriptor(&input(spec, wider)).is_err(),
            "dimension {case}"
        );
    }
    let mut spec = artifact_spec(999, EVALUATOR, VerdictValue::Error);
    spec.visibility = &[];
    spec.payload = content();
    envelope.check_descriptor(&input(spec, narrow)).unwrap();
    // This checks descriptor capacity only. Real custody must still verify the
    // referenced payload and enforce the separately pinned schema maximum.
}

#[test]
fn too_small_compound_or_entry_allowance_refuses_before_begin() {
    let core = running(&[(ValidationMode::Required, false)]);
    let (claim, registrations, declaration) = parts(&core);
    let ordinary = descriptor_limits(core.limits, claim, registrations).unwrap();
    let mut limits = core.limits;
    let (parent, registry) = parent_bound(limits, claim, registrations).unwrap();
    let cohort = cohort_bound(limits, claim, registrations).unwrap();
    limits.preparation_bytes = NativeArtifactInput::container_charge()
        + result_containers().unwrap()
        + failure_scratch(parent, registry).unwrap()
        + cohort.construction_bytes().unwrap();
    assert!(descriptor_limits_with_cohort(limits, claim, registrations, cohort).is_err());
    limits.preparation_bytes += "error".len() + ALLOCATION;
    let minimal = descriptor_limits_with_cohort(limits, claim, registrations, cohort).unwrap();
    assert_eq!(minimal.kind_bytes, 5);
    assert_eq!(minimal.inline_bytes, 0);
    assert_eq!(minimal.metadata_bytes, 0);
    let bounded = CompletionEnvelope::derive(
        &core.state.rows,
        limits,
        claim,
        registrations,
        declaration,
        minimal,
        evidence(),
    )
    .unwrap();
    let mut diagnostic = artifact_spec(900, EVALUATOR, VerdictValue::Error);
    diagnostic.metadata = &[];
    diagnostic.visibility = &[];
    diagnostic.payload = content();
    bounded
        .check_descriptor(&input(diagnostic, minimal))
        .unwrap();
    limits.preparation_bytes -= 1;
    assert!(descriptor_limits_with_cohort(limits, claim, registrations, cohort).is_err());
    limits = core.limits;
    limits.range.max_entry_bytes = size_of::<Entry<Key, Row>>();
    assert!(descriptor_limits(limits, claim, registrations).is_err());
    limits = core.limits;
    limits.range.max_batch_entries = 9;
    assert!(
        CompletionEnvelope::derive(
            &core.state.rows,
            limits,
            claim,
            registrations,
            declaration,
            ordinary,
            evidence()
        )
        .is_err()
    );
    assert!(
        CompletionEnvelope::derive(
            &core.state.rows,
            core.limits,
            claim,
            registrations,
            declaration,
            ordinary,
            EvidenceBounds {
                workspace_bytes: usize::MAX,
                retained_bytes: 1
            }
        )
        .is_err()
    );
    let mut no_error_kind = ordinary;
    no_error_kind.kind_bytes = 4;
    assert!(
        CompletionEnvelope::derive(
            &core.state.rows,
            core.limits,
            claim,
            registrations,
            declaration,
            no_error_kind,
            evidence()
        )
        .is_err()
    );
}

fn report_changes(
    core: &Core<NativeState>,
    candidate: &NativePrepared,
    artifact: ArtifactId,
) -> Vec<Change<Key, Row>> {
    let mut changes = Vec::with_capacity(11);
    for entry in candidate.fragments.entries() {
        let selected = match entry.key {
            Key::Meta => true,
            Key::Claim(id) => id == key(1).claim && candidate.outcome().changed != 0,
            Key::Evaluation(id) => id == key(1),
            Key::Artifact(id) => id == artifact,
            Key::ArtifactIdentity(_) | Key::Accepted(_) => {
                core.state.rows.get(&entry.key).is_none()
            }
            Key::Outcome(request) => request == candidate.outcome().invocation,
            Key::Event(sequence, _) => sequence == candidate.outcome().sequence,
            _ => false,
        };
        if !selected {
            continue;
        }
        let row = match &entry.value {
            Row::Meta(row) => Row::Meta(*row),
            Row::Claim(row) => Row::Claim(row.copy().unwrap()),
            Row::Evaluation(row) => Row::Evaluation(row.copy().unwrap()),
            Row::Artifact(row) => Row::Artifact(row.copy().unwrap()),
            Row::ArtifactIdentity(row) => Row::ArtifactIdentity(*row),
            Row::Accepted(row) => Row::Accepted(row.copy().unwrap()),
            Row::Outcome(row) => Row::Outcome(*row),
            Row::Event(row) => Row::Event(row.copy().unwrap()),
            _ => panic!("unexpected report row"),
        };
        changes.push(Change::Put(Entry::new(entry.key, row, entry.heap_bytes)));
    }
    changes
}

#[test]
fn admitted_scope_growth_and_actual_failed_report_fit_the_original_pinned_envelope() {
    let mut core = running(&[(ValidationMode::Required, false)]);
    let envelope = envelope(&core);
    let old_heap = heap(parts(&core).0).unwrap();
    let pinned = core.pin_native(0, 100).unwrap();
    for child in 2..=9 {
        let parent = parts(&core).0.binding();
        let mut input = creation(1000 + child, child, &[], None);
        let NativeCommand::Create { claims, .. } = &mut input.command else {
            panic!("create");
        };
        let proposal = claims.first_mut().unwrap();
        proposal.definition.lineage =
            Lineage::new(binding(child), Cause::Claim(key(1).claim), &[], 0).unwrap();
        proposal.owner = Some(Owner {
            expected: parent,
            receipt: None,
        });
        publish(&mut core, 40 + child as u64, input);
        let (claim, registrations, _) = parts(&core);
        let facts = ParentFacts::new(claim, registrations).unwrap();
        envelope.check_parent(claim, registrations).unwrap();
        envelope.check_parent_facts(&facts).unwrap();
        let mut old_size = envelope;
        old_size.parent_heap = old_heap;
        assert!(old_size.check_parent_facts(&facts).is_err());
    }
    assert!(heap(parts(&core).0).unwrap() > old_heap);
    let report = report_for(
        &core,
        None,
        2000,
        1,
        VerdictValue::Fail,
        descriptor(artifact_spec(900, EVALUATOR, VerdictValue::Fail)),
    );
    let NativeCommand::ReportAdmission { artifact, .. } = &report.command else {
        panic!("report");
    };
    envelope.check_descriptor(artifact).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut store = ContentStore::open(
        dir.path(),
        StoreLimits {
            max_content_bytes: 1024 * 1024,
            max_staging_bytes: 2 * 1024 * 1024,
            max_uploads: 4,
            chunk_bytes: 17,
            max_manifest_bytes: 128 * 1024,
        },
    )
    .unwrap();
    let verified = store
        .verify_native_artifact(
            report.request,
            artifact.get().unwrap(),
            ContentDomainId::from_u128(90),
            &core.state.budget,
            &BuiltinNativeSchemas,
        )
        .unwrap();
    let candidate = prepared(core.prepare_native_evidenced(
        context(EVALUATOR, 100),
        report,
        &[],
        Some(&verified),
    ));
    let changes = report_changes(&core, &candidate, ArtifactId::from_u128(900));
    assert_eq!(changes.len(), 12);
    let Some(Row::Event(event)) = candidate
        .fragments
        .get(&Key::Event(candidate.outcome().sequence, 4))
    else {
        panic!("actual registry seal fact")
    };
    assert!(
        matches!(event.get().unwrap().expand(core.state.ledger).fact,
        NativeFact::Registrations { claim } if claim == candidate.claim(key(1).claim).unwrap().binding())
    );
    let plan = core
        .state
        .rows
        .plan_batch(
            &core.state.budget,
            candidate.outcome().sequence.0,
            changes,
            BudgetLane::Completion,
            usize::MAX,
        )
        .unwrap();
    plan.check_envelope(
        &envelope
            .report_storage(CompletionUse::AdmissionFailure)
            .unwrap(),
    )
    .unwrap();
    drop(plan);
    core.publish_native(candidate).unwrap();
    assert_eq!(parts(&core).0.status(), ClaimStatus::PostFailed);
    envelope
        .check_parent(parts(&core).0, parts(&core).1)
        .unwrap();
    assert_eq!(
        pinned
            .with_claim(key(1).claim, 0, |claim| claim.scopes().children().len())
            .unwrap(),
        Some(0)
    );
    core.release_native(&pinned).unwrap();
}

#[test]
fn parent_cap_and_storage_envelope_reject_another_owner() {
    let core = running(&[(ValidationMode::Required, false)]);
    let envelope = envelope(&core);
    let other = running(&[(ValidationMode::Required, false)]);
    // Logical claim identity may match, but publication belongs to an actual
    // range incarnation. The envelope must reject the foreign preparation.
    let plan = other
        .state
        .rows
        .plan_batch(
            &core.state.budget,
            other.native_sequence().0 + 1,
            vec![Change::Put(Entry::new(
                Key::Meta,
                Row::Meta(Meta::default()),
                0,
            ))],
            BudgetLane::Completion,
            usize::MAX,
        )
        .unwrap();
    assert!(matches!(
        plan.check_envelope(&envelope.report_storage(CompletionUse::Regular).unwrap()),
        Err(MemoryError::WrongRange)
    ));
    let mut foreign_core = super::super::report_tests::core();
    publish(&mut foreign_core, 10, creation(3000, 20, &[], None));
    let claim = foreign_core.native_claim(ClaimId::from_u128(20)).unwrap();
    let registrations = RegistrationSet::new(claim, 1, size_of::<RegistrationSet>()).unwrap();
    assert!(envelope.check_parent(claim, &registrations).is_err());
}

#[test]
fn one_checked_parent_fact_set_serves_the_complete_mixed_grant_cohort() {
    let core = running(&[
        (ValidationMode::Required, false),
        (ValidationMode::Observe, true),
        (ValidationMode::Required, true),
    ]);
    let (claim, registrations, _) = parts(&core);
    let before = (core.native_stats(), core.state.budget.stats());
    let facts = ParentFacts::new(claim, registrations).unwrap();
    for index in 1..=3 {
        let declaration = core.native_definition(key(index).validation).unwrap();
        let envelope = CompletionEnvelope::derive(
            &core.state.rows,
            core.limits,
            claim,
            registrations,
            declaration,
            descriptor_limits(core.limits, claim, registrations).unwrap(),
            evidence(),
        )
        .unwrap();
        envelope.check_parent_facts(&facts).unwrap();
        envelope.check_parent(claim, registrations).unwrap();
    }
    assert_eq!((core.native_stats(), core.state.budget.stats()), before);
    // ParentFacts contains only fixed-size fields and a borrow marker. The
    // scalar check has no source collection from which to rehash the policy or
    // traverse nested buffers: the owner can construct once, then visit grants.
    assert!(!std::mem::needs_drop::<ParentFacts<'_>>());
}

#[test]
fn checked_parent_facts_reject_substituted_policy_and_dropped_registry() {
    let required = running(&[(ValidationMode::Required, false)]);
    let observe = running(&[(ValidationMode::Observe, false)]);
    let envelope = envelope(&required);
    let (claim, registrations, _) = parts(&required);
    let (other_claim, other_registrations, _) = parts(&observe);
    assert_eq!(claim.binding(), other_claim.binding());
    // Equal caller identities cannot substitute another complete immutable
    // declaration policy, whether the registry or both actual rows changed.
    assert!(ParentFacts::new(claim, other_registrations).is_err());
    let other = ParentFacts::new(other_claim, other_registrations).unwrap();
    assert!(envelope.check_parent_facts(&other).is_err());
    assert!(
        envelope
            .check_parent(other_claim, other_registrations)
            .is_err()
    );

    let dropped = RegistrationSet::new(claim, 16, size_of::<RegistrationSet>()).unwrap();
    let dropped_facts = ParentFacts::new(claim, &dropped).unwrap();
    assert!(envelope.check_parent_facts(&dropped_facts).is_err());
    assert!(envelope.check_parent(claim, &dropped).is_err());
    assert_eq!(registrations.rows().len(), 1);
    envelope
        .check_parent_facts(&ParentFacts::new(claim, registrations).unwrap())
        .unwrap();
}

#[test]
fn scalar_parent_guards_keep_exact_heap_and_conditional_revision_margins() {
    let core = running(&[(ValidationMode::Required, false)]);
    let mut envelope = envelope(&core);
    let (claim, registrations, _) = parts(&core);
    let mut facts = ParentFacts::new(claim, registrations).unwrap();
    envelope.parent_heap = heap(claim).unwrap();
    envelope.registry_heap = transactions::registry_heap(registrations).unwrap();
    envelope.check_parent_facts(&facts).unwrap();
    envelope.check_parent(claim, registrations).unwrap();
    let mut short_parent = envelope;
    short_parent.parent_heap -= 1;
    assert!(short_parent.check_parent_facts(&facts).is_err());
    assert!(short_parent.check_parent(claim, registrations).is_err());
    let mut short_registry = envelope;
    short_registry.registry_heap -= 1;
    assert!(short_registry.check_parent_facts(&facts).is_err());
    assert!(short_registry.check_parent(claim, registrations).is_err());
    let mut changed_scope_contract = envelope;
    changed_scope_contract.scope_limits.children += 1;
    assert!(changed_scope_contract.check_parent_facts(&facts).is_err());
    let mut later_contract = envelope;
    later_contract.parent.revision = focal_model::ObjectRevision(claim.binding().revision.0 + 1);
    assert!(later_contract.check_parent_facts(&facts).is_err());

    // Test-only scalar boundary injection, not a constructed lifecycle history:
    // a Required Posted parent must retain one revision for derived failure.
    facts.binding.revision = focal_model::ObjectRevision(u64::MAX);
    assert!(envelope.check_parent_facts(&facts).is_err());
    let mut no_parent_failure = envelope;
    no_parent_failure.failed_report = None;
    no_parent_failure.check_parent_facts(&facts).unwrap();
    facts.posted = false;
    envelope.check_parent_facts(&facts).unwrap();
    facts.responses = usize::try_from(envelope.max_responses).unwrap() + 1;
    assert!(envelope.check_parent_facts(&facts).is_err());
}

fn observed_with_response_cap(max_responses: u32) -> Core<NativeState> {
    use super::super::report_tests::{ISSUER, core, post};
    let mut core = core();
    let mut input = creation(1, 1, &[(ValidationMode::Observe, false)], None);
    let NativeCommand::Create { claims, .. } = &mut input.command else {
        panic!("claim creation");
    };
    claims[0].definition.max_responses = max_responses;
    publish(&mut core, 10, input);
    publish(&mut core, 20, post(2, binding(1)));
    let claim = parts(&core).0.binding();
    let evaluation = core.native_evaluation(key(1)).unwrap().binding();
    publish(
        &mut core,
        30,
        super::super::report_tests::begin(3, claim, 1, evaluation),
    );
    assert_eq!(parts(&core).0.issuer(), ISSUER);
    core
}

#[test]
fn authored_response_history_growth_preserves_a_live_observe_grant() {
    use super::super::report_tests::{SUBJECT, request};
    use focal_model::lifecycle::evidence::{
        CloseReport, Parent, Response, ResponseIdentity, ResponseLimits, ResponseState,
    };
    use focal_model::{Confidence, OutcomeKind, ReceiptId};

    let mut core = observed_with_response_cap(4);
    let original = envelope(&core);
    let expected = parts(&core).0.binding();
    publish(
        &mut core,
        40,
        NativeInput {
            request: request(SUBJECT, 4),
            command: NativeCommand::AcquireReceipt {
                expected,
                receipt: ReceiptId::from_u128(910),
            },
        },
    );
    let (received, registrations, declaration) = parts(&core);
    assert_eq!(received.response_count(), 0);
    let mut current = received
        .try_copy(received.retained_bytes().unwrap())
        .unwrap();
    let history_bound =
        charged_heap(current.max_response_history_heap_bytes().unwrap(), 1).unwrap();
    let initial_heap = heap(&current).unwrap();
    for cycle in 1..=4 {
        let parent = Parent::from_claim(&current).unwrap();
        let closed = Response::close(
            ResponseIdentity {
                binding: binding(920 + u128::from(cycle)),
                claim: key(1).claim,
                receipt: parent.receipt,
                cycle,
                prior: parent.latest_response,
            },
            &parent,
            Principal::Actor(SUBJECT),
            &[],
            &[],
            CloseReport {
                summary: "The respondent completed work; no output slots were requested.",
                confidence: Confidence::Committed,
                outcome: OutcomeKind::Complete,
                diagnostics: &[],
                limits: ResponseLimits {
                    artifacts: 0,
                    diagnostics: 0,
                    summary_bytes: 256,
                    construction_bytes: 4096,
                },
            },
        )
        .unwrap();
        let mut next = current
            .try_copy_for_response(current.copy_for_response_charge().unwrap())
            .unwrap();
        next.observe_response(&next.binding(), Principal::Actor(SUBJECT), &closed.response)
            .unwrap();
        assert_eq!(closed.response.state(), ResponseState::Generated);
        assert_eq!(next.response_count(), usize::try_from(cycle).unwrap());
        assert_eq!(next.max_responses(), 4);
        assert!(heap(&next).unwrap() <= initial_heap + history_bound);
        original.check_parent(&next, registrations).unwrap();
        original
            .check_parent_facts(&ParentFacts::new(&next, registrations).unwrap())
            .unwrap();
        // Actual checked model facts qualify continued Admission reporting;
        // this test does not publish a native testimony transaction or verdict.
        core.native_evaluation(key(1))
            .unwrap()
            .bind(declaration)
            .unwrap()
            .admission_report_owner(&next, 100)
            .unwrap();
        current = next;
    }
    assert_eq!(current.status(), ClaimStatus::TestamentGenerated);
    assert!(current.copy_for_response_charge().is_err());
    assert_eq!(received.response_count(), 0);
    assert!(heap(&current).unwrap() > initial_heap);
    // Reconstructing the bound from a nonempty history neither double-counts
    // its current capacity nor loses the full authored maximum.
    assert_eq!(
        parent_bound(core.limits, &current, registrations)
            .unwrap()
            .0,
        original.parent_heap
    );
}

#[test]
fn response_capacity_is_pinned_independently_of_supplied_content_identity() {
    let initial = observed_with_response_cap(4);
    let larger = observed_with_response_cap(5);
    let smaller = observed_with_response_cap(3);
    let original = envelope(&initial);
    let (claim, registrations, _) = parts(&initial);
    for changed in [&larger, &smaller] {
        let (other, registry, _) = parts(changed);
        assert_eq!(other.binding(), claim.binding());
        assert_eq!(
            other.acceptance().intent_fingerprint(),
            claim.acceptance().intent_fingerprint()
        );
        let facts = ParentFacts::new(other, registry).unwrap();
        assert!(original.check_parent_facts(&facts).is_err());
        assert!(original.check_parent(other, registry).is_err());
    }
    original.check_parent(claim, registrations).unwrap();
}

#[test]
fn complete_response_history_must_fit_entry_and_quotes_need_no_free_budget() {
    use focal_memory::BudgetKind;
    let core = observed_with_response_cap(4);
    let (claim, registrations, _) = parts(&core);
    let history = charged_heap(claim.max_response_history_heap_bytes().unwrap(), 1).unwrap();
    let minimum = heap(claim).unwrap() + history;
    let registry = registry_bound(core.limits, claim, registrations).unwrap().1;
    let exact_entry =
        size_of::<Entry<Key, Row>>() + OwnedClaim::container_charge() + registry + minimum;
    let mut exact = core.limits;
    exact.range.max_entry_bytes = exact_entry;
    assert_eq!(
        parent_bound(exact, claim, registrations).unwrap().0,
        minimum
    );
    exact.range.max_entry_bytes -= 1;
    assert!(parent_bound(exact, claim, registrations).is_err());

    let budget = core.state.budget.clone();
    let occupied = budget
        .reserve(
            BudgetKind::Query,
            BudgetLane::Completion,
            budget.limit() - budget.stats().used,
        )
        .unwrap();
    let before = budget.stats();
    let bound = envelope(&core);
    bound.check_parent(claim, registrations).unwrap();
    assert_eq!(budget.stats(), before);
    drop(occupied);

    // The maximum legal authored count is still bounded by the real row/owner
    // allowance. Quoting it must refuse without allocating that history.
    let huge = observed_with_response_cap(u32::MAX);
    let (claim, registrations, declaration) = parts(&huge);
    let before = huge.state.budget.stats();
    assert!(descriptor_limits(huge.limits, claim, registrations).is_err());
    assert!(
        CompletionEnvelope::derive(
            &huge.state.rows,
            huge.limits,
            claim,
            registrations,
            declaration,
            bound.descriptor_limits(),
            evidence(),
        )
        .is_err()
    );
    assert_eq!(huge.state.budget.stats(), before);
    assert_eq!(claim.response_history_heap_bytes().unwrap(), 0);
    assert!(charged_heap(usize::MAX, 1).is_err());
}

#[test]
fn actual_received_responses_append_delivery_registrations_without_invalidating_admission() {
    use super::super::report_tests::{ISSUER, SUBJECT, request};
    use focal_model::lifecycle::evidence::{
        CloseReport, Parent, Response, ResponseIdentity, ResponseLimits,
    };
    use focal_model::{Confidence, OutcomeKind, ReceiptId};

    let mut core = observed_with_response_cap(4);
    let original = envelope(&core);
    let expected = parts(&core).0.binding();
    publish(
        &mut core,
        40,
        NativeInput {
            request: request(SUBJECT, 4),
            command: NativeCommand::AcquireReceipt {
                expected,
                receipt: ReceiptId::from_u128(930),
            },
        },
    );
    let (source, registered, admission) = parts(&core);
    let receipt = core.native_definition(key(0).validation).unwrap();
    let old_membership = registered.rows().to_vec();
    let mut current = source.try_copy(source.copy_charge().unwrap()).unwrap();
    let mut registry = registered
        .try_copy(registered.copy_charge().unwrap())
        .unwrap();
    assert_eq!(original.registrations, 1);
    assert_eq!(original.max_registrations, 5);
    for cycle in 1..=4 {
        let parent = Parent::from_claim(&current).unwrap();
        let mut response = Response::close(
            ResponseIdentity {
                binding: binding(940 + u128::from(cycle)),
                claim: key(1).claim,
                receipt: parent.receipt,
                cycle,
                prior: parent.latest_response,
            },
            &parent,
            Principal::Actor(SUBJECT),
            &[],
            &[],
            CloseReport {
                summary: "The respondent completed the work; no output slots were declared.",
                confidence: Confidence::Committed,
                outcome: OutcomeKind::Complete,
                diagnostics: &[],
                limits: ResponseLimits {
                    artifacts: 0,
                    diagnostics: 0,
                    summary_bytes: 256,
                    construction_bytes: 4096,
                },
            },
        )
        .unwrap()
        .response;
        current = current
            .try_copy_for_response(current.copy_for_response_charge().unwrap())
            .unwrap();
        current
            .observe_response(&current.binding(), Principal::Actor(SUBJECT), &response)
            .unwrap();
        let posted = response
            .plan_post(
                &response.identity().binding,
                &parent,
                Principal::Actor(SUBJECT),
            )
            .unwrap();
        response.apply(posted).unwrap();
        current
            .observe_response(&current.binding(), Principal::Actor(SUBJECT), &response)
            .unwrap();
        let received = response
            .plan_receive(
                &response.identity().binding,
                &parent,
                Principal::Actor(ISSUER),
            )
            .unwrap();
        response.apply(received).unwrap();
        current
            .observe_response(&current.binding(), Principal::Actor(ISSUER), &response)
            .unwrap();
        let delivery = validation::Evaluation::materialize_delivery(
            Principal::Actor(ISSUER),
            receipt,
            &current,
            &response,
        )
        .unwrap();
        assert_eq!(delivery.generation(), u64::from(cycle));
        assert_eq!(
            delivery.target(),
            validation::Target::Delivery {
                response: response.identity().binding
            }
        );
        registry = registry
            .try_copy_with_additional(1, registry.copy_with_additional_charge(1).unwrap())
            .unwrap();
        let capacity = registry.retained_heap_bytes().unwrap();
        assert!(registry.register(&current, &delivery, usize::MAX).unwrap());
        assert_eq!(registry.retained_heap_bytes().unwrap(), capacity);
        assert_eq!(&registry.rows()[..old_membership.len()], old_membership);
        assert_eq!(registry.rows().len(), 1 + usize::try_from(cycle).unwrap());
        original.check_parent(&current, &registry).unwrap();
        original
            .check_parent_facts(&ParentFacts::new(&current, &registry).unwrap())
            .unwrap();
        let begun = core
            .native_evaluation(key(1))
            .unwrap()
            .bind(admission)
            .unwrap();
        let owner = begun.admission_report_owner(&current, 100).unwrap();
        let report = validation::Report {
            generation: begun.generation(),
            attempt: begun.current_attempt().unwrap(),
            value: VerdictValue::Error,
            evidence: ArtifactRef {
                id: ArtifactId::from_u128(990),
                hash: ContentHash([99; 32]),
            },
        };
        // The actual attempt/actor/authority still authorizes the late report;
        // this preflight is not a custody proof or an accepted result.
        begun
            .authorize_report(
                Principal::Actor(EVALUATOR),
                &begun.binding(),
                &owner,
                report,
            )
            .unwrap();
    }
    assert_eq!(registry.rows().len(), original.max_registrations);
    assert_eq!(
        transactions::registry_heap(&registry).unwrap(),
        original.registry_heap
    );
    assert_eq!(registered.rows(), old_membership);
    assert_eq!(source.response_count(), 0);
}

fn future_target_policy_core(max_responses: u32, max_rows: usize) -> Core<NativeState> {
    use super::super::report_tests::{ISSUER, core, post};
    use focal_model::{
        Deadline, HandlerRef, TimerId, ValidationKind, ValidationPhase, ValidatorId,
    };
    let mut core = core();
    core.limits.evaluations_per_claim = max_rows;
    let mut input = creation(1, 1, &[(ValidationMode::Observe, false)], None);
    let NativeCommand::Create {
        claims,
        declarations,
    } = &mut input.command
    else {
        panic!("create")
    };
    let handler = HandlerRef {
        id: ValidatorId::from_u128(95),
        version: ContentHash([8; 32]),
        agentic: false,
    };
    let policies = [validation::HandlerPolicy {
        handler: &handler,
        attempts: 1,
        proof_schema: focal_evidence::test_report_schema(),
        diagnostic_schema: focal_evidence::error_report_schema(),
    }];
    for (index, target) in [
        (
            2,
            validation::TargetDeclaration::WholeWorkSlot {
                index: 0,
                name: "first",
            },
        ),
        (
            3,
            validation::TargetDeclaration::WholeWorkSlot {
                index: 1,
                name: "second",
            },
        ),
        (4, validation::TargetDeclaration::Increment),
    ] {
        declarations.push(
            validation::Declaration::new(
                Principal::Actor(ISSUER),
                validation::DeclarationSpec {
                    binding: binding(100 + u128::from(index)),
                    claim: key(1).claim,
                    issuer: ISSUER,
                    declaration_index: index,
                    kind: ValidationKind::Test,
                    phase: if index == 4 {
                        ValidationPhase::Increment
                    } else {
                        ValidationPhase::WholeWork
                    },
                    mode: ValidationMode::Required,
                    target,
                    program: validation::Program::Programmatic {
                        check: validation::PhasePolicy {
                            evaluator: EVALUATOR,
                            definition: ContentHash([11; 32]),
                            handlers: &policies,
                            required_policy: None,
                        },
                        quality: None,
                    },
                    deadline: Deadline {
                        timer: TimerId::from_u128(100 + u128::from(index)),
                        generation: 1,
                        at: 1000,
                    },
                },
                validation::Limits {
                    handlers: 2,
                    attempts: 2,
                    slot_bytes: 32,
                },
            )
            .unwrap(),
        );
    }
    let first = [aggregation::CheckPolicy {
        declaration_index: 2,
        validation: key(2).validation,
        mode: ValidationMode::Required,
    }];
    let second = [aggregation::CheckPolicy {
        declaration_index: 3,
        validation: key(3).validation,
        mode: ValidationMode::Required,
    }];
    claims[0].definition.max_responses = max_responses;
    claims[0].definition.acceptance = aggregation::AcceptancePolicy::new(
        binding(1),
        ISSUER,
        &[
            aggregation::SlotPolicy {
                slot: 0,
                missing_declaration_index: 10,
                mode: ValidationMode::Required,
                checks: &first,
            },
            aggregation::SlotPolicy {
                slot: 1,
                missing_declaration_index: 11,
                mode: ValidationMode::Required,
                checks: &second,
            },
            // A slot with no whole-work checks still admits an Increment target.
            aggregation::SlotPolicy {
                slot: 2,
                missing_declaration_index: 12,
                mode: ValidationMode::Observe,
                checks: &[],
            },
        ],
        declarations,
        aggregation::Limits {
            max_slots: 8,
            max_checks: 16,
            max_results: 64,
            max_updates: 64,
        },
    )
    .unwrap();
    publish(&mut core, 10, input);
    publish(&mut core, 20, post(2, binding(1)));
    let claim = parts(&core).0.binding();
    let evaluation = core.native_evaluation(key(1)).unwrap().binding();
    publish(
        &mut core,
        30,
        super::super::report_tests::begin(3, claim, 1, evaluation),
    );
    core
}

#[test]
fn future_registry_counts_all_target_families_and_preserves_the_trusted_hard_cap() {
    use focal_model::lifecycle::aggregation::RegisteredEvaluation;
    for (responses, hard_cap, expected) in [(1, 64, 7), (4, 64, 25), (4, 16, 16), (4, 1, 1)] {
        let core = future_target_policy_core(responses, hard_cap);
        let (claim, registry, _) = parts(&core);
        assert_eq!(claim.acceptance().slot_count(), 3);
        assert_eq!(registry.rows().len(), 1);
        let before = core.state.budget.stats();
        let quote = envelope(&core);
        assert_eq!(quote.registrations, 1);
        assert_eq!(quote.registry_limit, hard_cap);
        // One Admission plus, per response, one Delivery, two slot checks and
        // three Increment targets, including the zero-check slot.
        assert_eq!(quote.max_registrations, expected);
        assert_eq!(
            quote.registry_heap,
            expected * size_of::<RegisteredEvaluation>() + ALLOCATION
        );
        assert_eq!(core.state.budget.stats(), before);
    }
}

/// Model-only registered generation for envelope/accounting tests. This helper
/// does not authorize a native Begin or manufacture verified artifact custody.
pub(in crate::native) fn increment_fixture() -> (
    Core<NativeState>,
    RegistrationSet,
    validation::EvaluationState,
) {
    let core = future_target_policy_core(2, 64);
    let (claim, registrations, _) = parts(&core);
    let declaration = core.native_definition(key(4).validation).unwrap();
    let evaluation = validation::Evaluation::materialize(
        Principal::Actor(super::super::report_tests::ISSUER),
        declaration,
        validation::Materialization {
            binding: declaration.binding(),
            target: validation::Target::Increment {
                claim: claim.binding(),
                artifact: binding(880),
            },
            slot_name: None,
            generation: 1,
            receipt: Some(focal_model::ReceiptFence {
                receipt: focal_model::ReceiptId::from_u128(881),
                epoch: 1,
            }),
        },
    )
    .unwrap();
    let mut registry = registrations
        .try_copy(registrations.copy_charge().unwrap())
        .unwrap();
    registry.register(claim, &evaluation, usize::MAX).unwrap();
    let state = evaluation.into_state();
    (core, registry, state)
}

#[test]
fn required_increment_prices_nine_writes_per_attempt_without_a_claim_failure_allowance() {
    let (core, registry, state) = increment_fixture();
    let (claim, _, _) = parts(&core);
    let declaration = core.native_definition(key(4).validation).unwrap();
    let mut limits = core.limits;
    // Nine primary writes, the report's twenty-one index rows and its
    // evaluation's due timer (doc 22 §7).
    limits.range.max_batch_entries = 31;
    let before = core.state.budget.stats();
    let quote = CompletionEnvelope::derive(
        &core.state.rows,
        limits,
        claim,
        &registry,
        declaration,
        descriptor_limits(limits, claim, &registry).unwrap(),
        evidence(),
    )
    .unwrap();
    let reports = declaration.attempt_bound() as usize;
    assert_eq!(declaration.mode(), ValidationMode::Required);
    assert!(quote.supports_target(EvaluationTarget::of(state.target())));
    assert!(!quote.supports_target(EvaluationTarget::Admission));
    assert!(
        quote
            .report_storage(CompletionUse::AdmissionFailure)
            .is_err()
    );
    // Nine primary writes, the report's twenty-one index rows and the
    // reported evaluation's due timer (doc 22 §7).
    assert_eq!(
        quote
            .report_storage(CompletionUse::Regular)
            .unwrap()
            .limits()
            .changed_keys,
        9 + 21 + 1
    );
    assert_eq!(quote.slots().events, reports * 3);
    assert_eq!(quote.slots().new_rows, reports * 28);
    assert_eq!(
        quote.total_retained_bytes(),
        reports
            * quote
                .per_report_retained_bytes(CompletionUse::Regular)
                .unwrap()
    );
    assert_eq!(core.state.budget.stats(), before);
    limits.range.max_batch_entries = 8;
    assert!(
        CompletionEnvelope::derive(
            &core.state.rows,
            limits,
            claim,
            &registry,
            declaration,
            quote.descriptor_limits(),
            evidence(),
        )
        .is_err()
    );
}

#[test]
fn increment_envelope_requires_actual_registered_family_and_keeps_full_parent_bounds() {
    let (core, registry, _) = increment_fixture();
    let (claim, admission_only, _) = parts(&core);
    let declaration = core.native_definition(key(4).validation).unwrap();
    let descriptor = descriptor_limits(core.limits, claim, &registry).unwrap();
    assert!(
        CompletionEnvelope::derive(
            &core.state.rows,
            core.limits,
            claim,
            admission_only,
            declaration,
            descriptor,
            evidence(),
        )
        .is_err()
    );
    let quote = CompletionEnvelope::derive(
        &core.state.rows,
        core.limits,
        claim,
        &registry,
        declaration,
        descriptor,
        evidence(),
    )
    .unwrap();
    quote.check_parent(claim, &registry).unwrap();
    assert!(quote.check_parent(claim, admission_only).is_err());
    assert!(quote.max_registrations > quote.registrations);
    assert!(quote.parent_heap >= heap(claim).unwrap());
    assert!(quote.registry_heap >= transactions::registry_heap(&registry).unwrap());
}

#[test]
fn remaining_slots_keep_admission_surcharge_until_used_and_zero_it_after_terminal() {
    let core = running(&[(ValidationMode::Required, false)]);
    let quote = envelope(&core);
    assert_eq!(quote.remaining_slots(2, true).unwrap(), quote.slots());
    assert_eq!(
        quote.remaining_slots(1, true).unwrap(),
        CompletionSlots {
            artifacts: 1,
            identities: 1,
            results: 1,
            outcomes: 1,
            events: 4 + quote.cohort().events(),
            sequences: 1,
            new_rows: 29 + quote.cohort().events(),
            ..CompletionSlots::default()
        }
    );
    assert_eq!(
        quote.remaining_slots(1, false).unwrap(),
        CompletionSlots {
            artifacts: 1,
            identities: 1,
            results: 1,
            outcomes: 1,
            events: 3,
            sequences: 1,
            // Seven primary rows and the report's twenty-one index rows.
            new_rows: 28,
            ..CompletionSlots::default()
        }
    );
    assert_eq!(
        quote.remaining_slots(0, true).unwrap(),
        CompletionSlots::default()
    );
    assert!(quote.remaining_slots(3, false).is_err());
    let observe = running(&[(ValidationMode::Observe, false)]);
    assert!(envelope(&observe).remaining_slots(1, true).is_err());
}

#[test]
fn finite_slot_arithmetic_checks_every_dimension_and_sequence_counter() {
    let one = CompletionSlots {
        claims: 1,
        definitions: 1,
        evaluations: 1,
        artifacts: 1,
        identities: 1,
        results: 1,
        responses: 1,
        receipts: 1,
        result_testaments: 1,
        monitors: 1,
        monitor_links: 1,
        outcomes: 1,
        events: 1,
        sequences: 1,
        new_rows: 1,
    };
    let zero = CompletionSlots::default();
    for dimension in 0..15 {
        let mut maximum = zero;
        let mut unit = zero;
        match dimension {
            0 => {
                maximum.artifacts = usize::MAX;
                unit.artifacts = 1;
            }
            1 => {
                maximum.identities = usize::MAX;
                unit.identities = 1;
            }
            2 => {
                maximum.results = usize::MAX;
                unit.results = 1;
            }
            3 => {
                maximum.outcomes = usize::MAX;
                unit.outcomes = 1;
            }
            4 => {
                maximum.events = usize::MAX;
                unit.events = 1;
            }
            5 => {
                maximum.sequences = u64::MAX;
                unit.sequences = 1;
            }
            6 => {
                maximum.new_rows = usize::MAX;
                unit.new_rows = 1;
            }
            7 => {
                maximum.claims = usize::MAX;
                unit.claims = 1;
            }
            8 => {
                maximum.definitions = usize::MAX;
                unit.definitions = 1;
            }
            9 => {
                maximum.evaluations = usize::MAX;
                unit.evaluations = 1;
            }
            10 => {
                maximum.responses = usize::MAX;
                unit.responses = 1;
            }
            11 => {
                maximum.receipts = usize::MAX;
                unit.receipts = 1;
            }
            12 => {
                maximum.result_testaments = usize::MAX;
                unit.result_testaments = 1;
            }
            13 => {
                maximum.monitors = usize::MAX;
                unit.monitors = 1;
            }
            14 => {
                maximum.monitor_links = usize::MAX;
                unit.monitor_links = 1;
            }
            _ => unreachable!(),
        }
        assert_eq!(maximum.checked_add(zero).unwrap(), maximum);
        assert_eq!(maximum.checked_scale(1).unwrap(), maximum);
        assert_eq!(maximum.checked_scale(0).unwrap(), zero);
        assert!(
            maximum.checked_add(unit).is_err(),
            "addition dimension {dimension}"
        );
        assert!(
            maximum.checked_scale(2).is_err(),
            "scaling dimension {dimension}"
        );
        assert!(
            zero.checked_sub(unit).is_err(),
            "subtraction dimension {dimension}"
        );
        assert_eq!(
            maximum
                .checked_sub(unit)
                .unwrap()
                .checked_add(unit)
                .unwrap(),
            maximum
        );
    }
    assert_eq!(
        one.checked_scale(3).unwrap().checked_sub(one).unwrap(),
        one.checked_scale(2).unwrap()
    );
}

#[test]
fn append_bound_refuses_dropped_rows_changed_limits_and_excess_actual_registry_capacity() {
    let core = observed_with_response_cap(4);
    let (claim, registry, declaration) = parts(&core);
    let quote = envelope(&core);
    let mut facts = ParentFacts::new(claim, registry).unwrap();
    for case in 0..4 {
        let original = (
            facts.registrations,
            facts.registry_limit,
            facts.registry_heap,
        );
        match case {
            0 => facts.registrations = quote.registrations - 1,
            1 => facts.registrations = quote.max_registrations + 1,
            2 => facts.registry_limit += 1,
            _ => facts.registry_heap = quote.registry_heap + 1,
        }
        assert!(quote.check_parent_facts(&facts).is_err(), "case {case}");
        (
            facts.registrations,
            facts.registry_limit,
            facts.registry_heap,
        ) = original;
        quote.check_parent_facts(&facts).unwrap();
    }
    let dropped =
        RegistrationSet::new(claim, registry.max_rows(), size_of::<RegistrationSet>()).unwrap();
    assert!(quote.check_parent(claim, &dropped).is_err());
    // The existing source is untouched. Unnecessary spare capacity beyond the
    // promised bound is also a real allocation and cannot be ignored.
    let additional = quote.max_registrations;
    let wider = registry
        .try_copy_with_additional(
            additional,
            registry.copy_with_additional_charge(additional).unwrap(),
        )
        .unwrap();
    assert_eq!(wider.rows(), registry.rows());
    assert!(quote.check_parent(claim, &wider).is_err());
    let expanded = CompletionEnvelope::derive(
        &core.state.rows,
        core.limits,
        claim,
        &wider,
        declaration,
        descriptor_limits(core.limits, claim, &wider).unwrap(),
        evidence(),
    )
    .unwrap();
    assert_eq!(expanded.max_registrations, quote.max_registrations);
    assert_eq!(
        expanded.registry_heap,
        transactions::registry_heap(&wider).unwrap()
    );
    expanded.check_parent(claim, &wider).unwrap();
    quote.check_parent(claim, registry).unwrap();
}
