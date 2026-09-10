//! Managed respondent completion under actual exhausted ancestor budgets.
use super::*;
use crate::native::respondent_state::{self, RespondentCredit};
use focal_memory::{Allocation, BudgetKind, BudgetLane, Change, Entry};

fn limits(diagnostics: usize) -> NativeLimits {
    NativeLimits {
        plan_nodes: 16,
        plan_edges: 65_536,
        preparation_bytes: 2 * 1024 * 1024,
        evaluations_per_claim: 32,
        work_artifacts_per_cycle: 2,
        diagnostics_per_cycle: diagnostics,
        response_summary_bytes: 256,
        range: RangeConfig {
            max_batch_entries: 32,
            page_entries: 4,
            page_bytes: 4096,
            max_entry_bytes: 64 * 1024,
            ..RangeConfig::default()
        },
        ..NativeLimits::default()
    }
}
fn parent(f: &Fixture) -> MemoryBudget {
    f.owner.effective().source_view().state.budget.clone()
}
fn credit(f: &Fixture, configured: NativeLimits) -> RespondentCredit {
    let view = f.owner.effective();
    respondent_state::read(
        view.source_view(),
        view.claim(ClaimId::from_u128(1)).unwrap(),
        configured,
    )
    .unwrap()
    .unwrap()
    .1
}
fn fill(budget: &MemoryBudget, pressure: &mut Vec<Allocation>) {
    let available = budget.limit() - budget.stats().used;
    if available != 0 {
        pressure.push(
            budget
                .reserve(BudgetKind::Payload, BudgetLane::Completion, available)
                .unwrap()
                .commit(),
        );
    }
    assert_eq!(budget.stats().used, budget.limit());
}
fn duplicate(input: &NativeInput) -> NativeInput {
    let command = match &input.command {
        NativeCommand::SubmitDiagnostic {
            claim,
            reason,
            artifact,
        } => NativeCommand::SubmitDiagnostic {
            claim: *claim,
            reason: *reason,
            artifact: artifact.copy().unwrap(),
        },
        NativeCommand::CloseResponse {
            claim,
            response,
            report,
        } => NativeCommand::CloseResponse {
            claim: *claim,
            response: *response,
            report: NativeResponseInput {
                summary: report.summary.clone(),
                confidence: report.confidence,
                outcome: report.outcome,
                manifest: report.manifest.clone(),
                diagnostics: report.diagnostics.clone(),
            },
        },
        NativeCommand::PostResponse { claim, expected } => NativeCommand::PostResponse {
            claim: *claim,
            expected: *expected,
        },
        _ => panic!("respondent operation"),
    };
    NativeInput {
        request: input.request,
        command,
    }
}
#[track_caller]
fn prepare(f: &mut Fixture, input: NativeInput) -> Result<NativeStaging, NativeOwnerError> {
    let time = f.owner.effective().logical_time() + 1;
    f.owner.prepare_with_custody(
        context(input.request.principal, time),
        input,
        &mut f.store,
        ContentDomainId::from_u128(93),
        &BuiltinNativeSchemas,
    )
}
#[track_caller]
fn pressured(
    f: &mut Fixture,
    input: NativeInput,
    budget: &MemoryBudget,
    pressure: &mut Vec<Allocation>,
) -> (NativeCandidate, NativeOutcome) {
    fill(budget, pressure);
    let key = input.request;
    match prepare(f, input) {
        Ok(NativeStaging::Prepared { candidate, outcome }) => (candidate, outcome),
        result => panic!("funded respondent {key:?}: {result:?}"),
    }
}
fn existing(
    actual: NativeStaging,
    expected: NativeOutcome,
    expected_candidate: Option<NativeCandidate>,
) {
    let NativeStaging::Existing { outcome, candidate } = actual else {
        panic!("exact retry")
    };
    assert_eq!(outcome, expected);
    assert_eq!(candidate, expected_candidate);
}
fn capacity_refusal(result: Result<NativeStaging, NativeOwnerError>) {
    assert!(
        matches!(
            result,
            Err(NativeOwnerError::Native(
                NativeError::Memory(_)
                    | NativeError::Capacity(_)
                    | NativeError::Contract(ContractError::Capacity)
            ))
        ),
        "capacity refusal: {result:?}"
    );
}
fn diagnostic_input(f: &mut Fixture, id: u128) -> (NativeInput, ArtifactRef) {
    let artifact = f.artifact(
        id,
        WorkRole::Diagnostic {
            reason: EvidenceFailure::Work,
        },
    );
    let reference = ArtifactRef {
        id: artifact.get().unwrap().id(),
        hash: artifact.get().unwrap().content_hash(),
    };
    (
        f.input(
            SUBJECT,
            NativeCommand::SubmitDiagnostic {
                claim: f.claim(),
                reason: EvidenceFailure::Work,
                artifact,
            },
        ),
        reference,
    )
}
fn post_input(f: &mut Fixture, id: u128) -> NativeInput {
    f.input(
        SUBJECT,
        NativeCommand::PostResponse {
            claim: f.claim(),
            expected: f.response(id),
        },
    )
}

#[test]
fn actual_failed_and_partial_testimony_survive_pressure_and_each_pending_rollback() {
    let configured = limits(3);
    let mut f = Fixture::with_limits(configured);
    let pin = f.owner.pin(0, 10_000).unwrap();
    let budget = parent(&f);
    let initial = credit(&f, configured);
    assert_eq!(
        initial,
        RespondentCredit {
            diagnostics: 4,
            closes: 4,
            posts: 4
        }
    );
    assert_eq!(
        f.owner
            .committed()
            .claim(ClaimId::from_u128(1))
            .unwrap()
            .response_count(),
        0
    );
    assert!(
        f.owner
            .committed()
            .response(TestamentId::from_u128(90_002))
            .is_none()
    );
    let (diagnostic, reference) = diagnostic_input(&mut f, 90_001);
    let retry = duplicate(&diagnostic);
    let mut pressure = Vec::new();
    let (candidate, outcome) = pressured(&mut f, diagnostic, &budget, &mut pressure);
    assert_eq!(
        credit(&f, configured),
        RespondentCredit {
            diagnostics: 3,
            closes: 4,
            posts: 4
        }
    );
    existing(
        prepare(&mut f, duplicate(&retry)).unwrap(),
        outcome,
        Some(candidate),
    );
    assert_eq!(f.owner.discard_from(candidate).unwrap(), 1);
    assert_eq!(credit(&f, configured), initial);
    assert!(f.owner.effective().artifact(reference.id).is_none());
    let (candidate, repeated) = pressured(&mut f, duplicate(&retry), &budget, &mut pressure);
    assert_eq!(repeated, outcome);
    f.owner.publish_after_durable(candidate).unwrap();
    existing(prepare(&mut f, retry).unwrap(), outcome, None);
    assert_eq!(
        f.owner
            .committed()
            .claim(ClaimId::from_u128(1))
            .unwrap()
            .response_count(),
        0
    );
    let artifact = f.owner.committed().artifact(reference.id).unwrap();
    assert_eq!(artifact.descriptor().kind(), "error");
    assert_eq!(artifact.descriptor().content_hash(), reference.hash);
    assert!(artifact.facts().is_none());
    let command = f.close(90_002, OutcomeKind::Failed, vec![], vec![reference]);
    let close = f.input(SUBJECT, command);
    let retry = duplicate(&close);
    let before_close = credit(&f, configured);
    let (candidate, outcome) = pressured(&mut f, close, &budget, &mut pressure);
    assert_eq!(
        credit(&f, configured),
        RespondentCredit {
            diagnostics: 3,
            closes: 3,
            posts: 4
        }
    );
    assert_eq!(
        f.owner
            .effective()
            .response(TestamentId::from_u128(90_002))
            .unwrap()
            .reported_outcome(),
        OutcomeKind::Failed
    );
    existing(
        prepare(&mut f, duplicate(&retry)).unwrap(),
        outcome,
        Some(candidate),
    );
    f.owner.discard_from(candidate).unwrap();
    assert_eq!(credit(&f, configured), before_close);
    assert!(
        f.owner
            .effective()
            .response(TestamentId::from_u128(90_002))
            .is_none()
    );
    let (candidate, repeated) = pressured(&mut f, duplicate(&retry), &budget, &mut pressure);
    assert_eq!(repeated, outcome);
    f.owner.publish_after_durable(candidate).unwrap();
    existing(prepare(&mut f, retry).unwrap(), outcome, None);
    let post = post_input(&mut f, 90_002);
    let retry = duplicate(&post);
    let (candidate, outcome) = pressured(&mut f, post, &budget, &mut pressure);
    assert_eq!(credit(&f, configured).posts, 3);
    f.owner.discard_from(candidate).unwrap();
    assert_eq!(credit(&f, configured).posts, 4);
    assert_eq!(
        f.owner
            .effective()
            .response(TestamentId::from_u128(90_002))
            .unwrap()
            .state(),
        ResponseState::Generated
    );
    let (candidate, repeated) = pressured(&mut f, duplicate(&retry), &budget, &mut pressure);
    assert_eq!(repeated, outcome);
    f.owner.publish_after_durable(candidate).unwrap();
    existing(prepare(&mut f, retry).unwrap(), outcome, None);
    // The receipt promised the complete immutable response allowance. A later
    // cycle still reports real partial failure after earlier retained writes.
    let (diagnostic, second) = diagnostic_input(&mut f, 90_003);
    let (candidate, _) = pressured(&mut f, diagnostic, &budget, &mut pressure);
    f.owner.publish_after_durable(candidate).unwrap();
    let command = f.close(90_004, OutcomeKind::Partial, vec![], vec![second]);
    let close = f.input(SUBJECT, command);
    let (candidate, _) = pressured(&mut f, close, &budget, &mut pressure);
    f.owner.publish_after_durable(candidate).unwrap();
    let post = post_input(&mut f, 90_004);
    let (candidate, _) = pressured(&mut f, post, &budget, &mut pressure);
    f.owner.publish_after_durable(candidate).unwrap();
    assert_eq!(
        credit(&f, configured),
        RespondentCredit {
            diagnostics: 2,
            closes: 2,
            posts: 2
        }
    );
    assert_eq!(
        f.owner
            .committed()
            .response(TestamentId::from_u128(90_002))
            .unwrap()
            .reported_outcome(),
        OutcomeKind::Failed
    );
    assert_eq!(
        f.owner
            .committed()
            .response(TestamentId::from_u128(90_004))
            .unwrap()
            .reported_outcome(),
        OutcomeKind::Partial
    );
    assert!(f.owner.committed().artifact(reference.id).is_some());
    // Exercise the complete admitted allowance, including its final retirement,
    // while every earlier statement and the original snapshot remain retained.
    for (diagnostic_id, response_id) in [(90_005, 90_006), (90_007, 90_008)] {
        let (input, diagnostic) = diagnostic_input(&mut f, diagnostic_id);
        let (candidate, _) = pressured(&mut f, input, &budget, &mut pressure);
        f.owner.publish_after_durable(candidate).unwrap();
        let command = f.close(response_id, OutcomeKind::Failed, vec![], vec![diagnostic]);
        let input = f.input(SUBJECT, command);
        let (candidate, _) = pressured(&mut f, input, &budget, &mut pressure);
        f.owner.publish_after_durable(candidate).unwrap();
        let input = post_input(&mut f, response_id);
        let (candidate, _) = pressured(&mut f, input, &budget, &mut pressure);
        f.owner.publish_after_durable(candidate).unwrap();
    }
    assert_eq!(credit(&f, configured), RespondentCredit::default());
    assert_eq!(
        pin.with_claim(ClaimId::from_u128(1), 0, ClaimState::response_count)
            .unwrap(),
        Some(0)
    );
    f.owner.release(&pin).unwrap();
    drop(pressure);
}

#[test]
fn optional_production_error_cannot_spend_the_last_reserved_work_diagnostic_slot() {
    let configured = limits(1);
    let mut f = Fixture::with_limits(configured);
    let initial = credit(&f, configured);
    let prefix = f.owner.effective().sequence();
    let command = NativeCommand::SubmitDiagnostic {
        claim: f.claim(),
        reason: EvidenceFailure::Production,
        artifact: f.artifact(
            91_001,
            WorkRole::Diagnostic {
                reason: EvidenceFailure::Production,
            },
        ),
    };
    let input = f.input(SUBJECT, command);
    assert!(matches!(
        prepare(&mut f, input),
        Err(NativeOwnerError::Native(NativeError::Capacity(
            "reserved respondent diagnostic slot"
        )))
    ));
    assert_eq!(credit(&f, configured), initial);
    assert_eq!(f.owner.effective().sequence(), prefix);
    assert!(
        f.owner
            .effective()
            .artifact(ArtifactId::from_u128(91_001))
            .is_none()
    );
    let budget = parent(&f);
    let mut pressure = Vec::new();
    let (input, reference) = diagnostic_input(&mut f, 91_002);
    let (candidate, _) = pressured(&mut f, input, &budget, &mut pressure);
    f.owner.publish_after_durable(candidate).unwrap();
    let command = f.close(91_003, OutcomeKind::Failed, vec![], vec![reference]);
    let input = f.input(SUBJECT, command);
    let (candidate, _) = pressured(&mut f, input, &budget, &mut pressure);
    f.owner.publish_after_durable(candidate).unwrap();
    let input = post_input(&mut f, 91_003);
    let (candidate, _) = pressured(&mut f, input, &budget, &mut pressure);
    f.owner.publish_after_durable(candidate).unwrap();
    assert_eq!(
        credit(&f, configured),
        RespondentCredit {
            diagnostics: 3,
            closes: 3,
            posts: 3
        }
    );
    drop(pressure);
}

#[test]
fn unaffordable_receipt_and_adoption_preserve_original_responsibility_and_its_reporting_capacity() {
    let configured = limits(2);
    let mut f = Fixture::with_limits(configured);
    let work = f.work(92_001, 0);
    f.commit(ISSUER, creation(92_002, 2, &[], None).command);
    f.commit(
        ISSUER,
        NativeCommand::Post {
            expected: f
                .owner
                .effective()
                .claim(ClaimId::from_u128(2))
                .unwrap()
                .binding(),
        },
    );
    let original = f
        .owner
        .effective()
        .claim(ClaimId::from_u128(1))
        .unwrap()
        .try_copy(1024 * 1024)
        .unwrap();
    let other = f
        .owner
        .effective()
        .claim(ClaimId::from_u128(2))
        .unwrap()
        .try_copy(1024 * 1024)
        .unwrap();
    let initial = credit(&f, configured);
    let prefix = f.owner.effective().sequence();
    let budget = parent(&f);
    let mut pressure = Vec::new();
    fill(&budget, &mut pressure);
    let input = f.input(
        SUBJECT,
        NativeCommand::AcquireReceipt {
            expected: other.binding(),
            receipt: ReceiptId::from_u128(92_003),
        },
    );
    capacity_refusal(prepare(&mut f, input));
    let input = f.input(
        ISSUER,
        NativeCommand::AdoptReceipt {
            expected: original.binding(),
            previous: original.receipt().unwrap().fence,
            receipt: ReceiptId::from_u128(92_004),
            holder: ParticipantId::from_u128(85),
        },
    );
    capacity_refusal(prepare(&mut f, input));
    assert_eq!(f.owner.effective().sequence(), prefix);
    assert_eq!(
        f.owner.effective().claim(ClaimId::from_u128(1)).unwrap(),
        &original
    );
    assert_eq!(
        f.owner.effective().claim(ClaimId::from_u128(2)).unwrap(),
        &other
    );
    assert!(
        f.owner
            .effective()
            .receipt(ReceiptId::from_u128(92_003))
            .is_none()
    );
    assert!(
        f.owner
            .effective()
            .receipt(ReceiptId::from_u128(92_004))
            .is_none()
    );
    assert_eq!(credit(&f, configured), initial);
    assert!(f.owner.effective().artifact(work.artifact.id).is_some());
    let (input, diagnostic) = diagnostic_input(&mut f, 92_005);
    let (candidate, _) = pressured(&mut f, input, &budget, &mut pressure);
    f.owner.publish_after_durable(candidate).unwrap();
    let command = f.close(92_006, OutcomeKind::Partial, vec![work], vec![diagnostic]);
    let input = f.input(SUBJECT, command);
    let (candidate, _) = pressured(&mut f, input, &budget, &mut pressure);
    f.owner.publish_after_durable(candidate).unwrap();
    let input = post_input(&mut f, 92_006);
    let (candidate, _) = pressured(&mut f, input, &budget, &mut pressure);
    f.owner.publish_after_durable(candidate).unwrap();
    assert_eq!(
        credit(&f, configured),
        RespondentCredit {
            diagnostics: 3,
            closes: 3,
            posts: 3
        }
    );
    drop(pressure);
}

fn copied(f: &Fixture, configured: NativeLimits) -> Core<NativeState> {
    let source = f.owner.committed();
    let mut core = Core::new_native(
        source.ledger(),
        RangeId(93_100),
        configured,
        MemoryBudget::new(128 * 1024 * 1024, 16 * 1024 * 1024).unwrap(),
    )
    .unwrap();
    let changes: Vec<_> = source
        .source_view()
        .state
        .rows
        .entries()
        .map(|entry| {
            Change::Put(Entry::new(
                entry.key,
                crate::native::prepare::copy(&entry.value).unwrap(),
                entry.heap_bytes,
            ))
        })
        .collect();
    // Copy the committed rows in batches the configured limit admits, ending
    // at the source's sequence so the copy reports the same prefix.
    let batch = core.limits.range.max_batch_entries.max(1);
    let batches = u64::try_from(changes.len().div_ceil(batch).max(1)).unwrap();
    let start = source.sequence().0 - batches;
    core.state.rows = crate::native::ranges::NativeRanges::single_from_store(
        RangeStore::new(
            RangeId(93_100),
            start,
            core.limits.range,
            core.state.budget.clone(),
        )
        .unwrap(),
    )
    .unwrap();
    let mut remaining = changes.into_iter();
    for offset in 1..=batches {
        let chunk: Vec<_> = remaining.by_ref().take(batch).collect();
        let prepared = core
            .state
            .rows
            .prepare_batch_with(
                start + offset,
                chunk,
                BudgetLane::Ordinary,
                crate::native::prepare::copy,
            )
            .unwrap();
        core.state.rows.publish(prepared).unwrap();
    }
    assert_eq!(core.state.rows.prefix(), source.sequence().0);
    core
}

#[test]
fn received_core_reconstruction_refuses_unfunded_transfer_then_recovers_generated_post_and_next_close()
 {
    let configured = limits(2);
    let mut f = Fixture::with_limits(configured);
    let diagnostic = f.diagnostic(93_001);
    f.commit(
        SUBJECT,
        f.close(93_002, OutcomeKind::Failed, vec![], vec![diagnostic]),
    );
    let original = credit(&f, configured);
    let core = copied(&f, configured);
    let budget = core.state.budget.clone();
    let mut pressure = Vec::new();
    fill(&budget, &mut pressure);
    let refusal = NativeOwner::new(core).unwrap_err();
    assert_eq!(
        refusal
            .core
            .native_response(TestamentId::from_u128(93_002))
            .unwrap()
            .state(),
        ResponseState::Generated
    );
    assert!(refusal.core.native_artifact(diagnostic.id).is_some());
    drop(pressure);
    let owner = NativeOwner::new(refusal.core).unwrap();
    let Fixture {
        owner: previous,
        store,
        _directory,
        serial,
    } = f;
    drop(previous);
    let mut f = Fixture {
        owner,
        store,
        _directory,
        serial,
    };
    assert_eq!(credit(&f, configured), original);
    let mut pressure = Vec::new();
    let input = post_input(&mut f, 93_002);
    let (candidate, _) = pressured(&mut f, input, &budget, &mut pressure);
    f.owner.publish_after_durable(candidate).unwrap();
    let (input, diagnostic) = diagnostic_input(&mut f, 93_003);
    let (candidate, _) = pressured(&mut f, input, &budget, &mut pressure);
    f.owner.publish_after_durable(candidate).unwrap();
    let command = f.close(93_004, OutcomeKind::Failed, vec![], vec![diagnostic]);
    let input = f.input(SUBJECT, command);
    let (candidate, _) = pressured(&mut f, input, &budget, &mut pressure);
    f.owner.publish_after_durable(candidate).unwrap();
    let input = post_input(&mut f, 93_004);
    let (candidate, _) = pressured(&mut f, input, &budget, &mut pressure);
    f.owner.publish_after_durable(candidate).unwrap();
    assert_eq!(
        credit(&f, configured),
        RespondentCredit {
            diagnostics: 2,
            closes: 2,
            posts: 2
        }
    );
    drop(pressure);
}

#[path = "respondent_schema_tests.rs"]
mod schema_tests;

fn encoded_respondent(input: &NativeInput) -> Vec<u8> {
    use crate::native::input_codec::{EncodingLimits, EncodingPlan, InputFrame};
    let plan = EncodingPlan::prepare(
        InputFrame::Request {
            ledger: binding(1).ledger,
            profile: NativeContentProfile::ProjectionOnly,
            input,
        },
        EncodingLimits {
            bytes: 65_536,
            visits: 262_144,
        },
    )
    .unwrap();
    let mut bytes = vec![0; plan.quote().bytes];
    plan.write_into(&mut bytes).unwrap();
    bytes
}

#[track_caller]
fn decoded_respondent(
    f: &mut Fixture,
    bytes: &[u8],
    configured: NativeLimits,
    build_visits: usize,
) -> Result<NativeStaging, NativeOwnerError> {
    use crate::native::input_codec::{
        DecodedRequest, FrameKind, InspectionLimits, StructuralInput,
    };
    let source = StructuralInput::inspect(
        bytes,
        InspectionLimits {
            bytes: 65_536,
            visits: 262_144,
            items: 128,
            text_bytes: 8192,
            blob_bytes: 8192,
        },
    )
    .unwrap();
    let request = source.header().request.unwrap();
    let now = context(request.principal, f.owner.effective().logical_time() + 1);
    match source.header().kind {
        FrameKind::Request { command: 7 } => {
            let mut input = source.artifact_input(262_144).unwrap().unwrap();
            let plan = input
                .prepare(
                    configured,
                    focal_model::lifecycle::artifact_descriptor::Limits {
                        kind_bytes: 128,
                        metadata_bytes: 1024,
                        inline_bytes: 65_536,
                        inputs: 16,
                        visibility_labels: 16,
                        visibility_label_bytes: 128,
                        construction_bytes: 128 * 1024,
                    },
                    262_144,
                    // Parsing one complete checked body, owner authorization
                    // replays, and final construction share this allowance.
                    1_048_576,
                    262_144,
                )
                .unwrap();
            f.owner.prepare_decoded_with_custody(
                now,
                DecodedRequest::from(plan),
                build_visits,
                &mut f.store,
                ContentDomainId::from_u128(93),
                &BuiltinNativeSchemas,
            )
        }
        FrameKind::Request { command: 9 } => {
            let plan = source
                .prepare_response(configured, 262_144)
                .unwrap()
                .unwrap();
            f.owner
                .prepare_decoded(now, plan.into(), build_visits, None)
        }
        FrameKind::Request { command: 10 } => {
            let plan = source.decode_fixed(262_144).unwrap().unwrap();
            f.owner
                .prepare_decoded(now, plan.try_into().unwrap(), build_visits, None)
        }
        kind => panic!("unexpected respondent frame: {kind:?}"),
    }
}

#[test]
fn borrowed_diagnostic_and_failed_testimony_use_held_input_capacity_under_pressure() {
    let configured = limits(3);
    let mut f = Fixture::with_limits(configured);
    let budget = parent(&f);
    let initial = credit(&f, configured);
    let (input, evidence) = diagnostic_input(&mut f, 95_001);
    let diagnostic = encoded_respondent(&input);
    drop(input);
    let mut pressure = Vec::new();
    fill(&budget, &mut pressure);
    let used = budget.stats().used;
    let prefix = f.owner.effective().sequence();
    assert!(decoded_respondent(&mut f, &diagnostic, configured, 0).is_err());
    assert_eq!(budget.stats().used, used);
    assert_eq!(credit(&f, configured), initial);
    assert_eq!(f.owner.effective().sequence(), prefix);
    assert!(f.owner.effective().artifact(evidence.id).is_none());
    assert_eq!(
        f.owner
            .effective()
            .claim(ClaimId::from_u128(1))
            .unwrap()
            .response_count(),
        0,
    );
    let NativeStaging::Prepared { candidate, outcome } =
        decoded_respondent(&mut f, &diagnostic, configured, 1_048_576).unwrap()
    else {
        panic!("fresh borrowed diagnostic")
    };
    existing(
        decoded_respondent(&mut f, &diagnostic, configured, 0).unwrap(),
        outcome,
        Some(candidate),
    );
    f.owner.discard_from(candidate).unwrap();
    assert_eq!(credit(&f, configured), initial);
    assert!(f.owner.effective().artifact(evidence.id).is_none());
    fill(&budget, &mut pressure);
    let NativeStaging::Prepared {
        candidate,
        outcome: repeated,
    } = decoded_respondent(&mut f, &diagnostic, configured, 1_048_576).unwrap()
    else {
        panic!("repeated borrowed diagnostic")
    };
    assert_eq!(repeated, outcome);
    f.owner.publish_after_durable(candidate).unwrap();
    existing(
        decoded_respondent(&mut f, &diagnostic, configured, 0).unwrap(),
        outcome,
        None,
    );
    let recorded = f.owner.committed().artifact(evidence.id).unwrap();
    assert_eq!(recorded.descriptor().kind(), "error");
    assert_eq!(recorded.descriptor().content_hash(), evidence.hash);
    assert!(recorded.facts().is_none());
    assert_eq!(
        f.owner
            .committed()
            .claim(ClaimId::from_u128(1))
            .unwrap()
            .response_count(),
        0
    );

    let input = f.input(
        SUBJECT,
        f.close(95_002, OutcomeKind::Failed, vec![], vec![evidence]),
    );
    let close = encoded_respondent(&input);
    drop(input);
    let before_close = credit(&f, configured);
    fill(&budget, &mut pressure);
    let used = budget.stats().used;
    assert!(decoded_respondent(&mut f, &close, configured, 0).is_err());
    assert_eq!(budget.stats().used, used);
    assert_eq!(credit(&f, configured), before_close);
    let NativeStaging::Prepared { candidate, outcome } =
        decoded_respondent(&mut f, &close, configured, 1_048_576).unwrap()
    else {
        panic!("borrowed failure testimony")
    };
    assert_eq!(credit(&f, configured).closes, before_close.closes - 1);
    assert_eq!(
        f.owner
            .effective()
            .response(TestamentId::from_u128(95_002))
            .unwrap()
            .reported_outcome(),
        OutcomeKind::Failed
    );
    existing(
        decoded_respondent(&mut f, &close, configured, 0).unwrap(),
        outcome,
        Some(candidate),
    );
    f.owner.discard_from(candidate).unwrap();
    assert_eq!(credit(&f, configured), before_close);
    assert!(
        f.owner
            .effective()
            .response(TestamentId::from_u128(95_002))
            .is_none()
    );
    fill(&budget, &mut pressure);
    let NativeStaging::Prepared {
        candidate,
        outcome: repeated,
    } = decoded_respondent(&mut f, &close, configured, 1_048_576).unwrap()
    else {
        panic!("repeated borrowed testimony")
    };
    assert_eq!(repeated, outcome);
    f.owner.publish_after_durable(candidate).unwrap();
    existing(
        decoded_respondent(&mut f, &close, configured, 0).unwrap(),
        outcome,
        None,
    );

    let input = post_input(&mut f, 95_002);
    let post = encoded_respondent(&input);
    drop(input);
    fill(&budget, &mut pressure);
    let before_post = credit(&f, configured);
    let NativeStaging::Prepared { candidate, outcome } =
        decoded_respondent(&mut f, &post, configured, 0).unwrap()
    else {
        panic!("borrowed fixed post")
    };
    assert_eq!(credit(&f, configured).posts, before_post.posts - 1);
    f.owner.discard_from(candidate).unwrap();
    assert_eq!(credit(&f, configured), before_post);
    fill(&budget, &mut pressure);
    let NativeStaging::Prepared {
        candidate,
        outcome: repeated,
    } = decoded_respondent(&mut f, &post, configured, 0).unwrap()
    else {
        panic!("repeated borrowed post")
    };
    assert_eq!(repeated, outcome);
    f.owner.publish_after_durable(candidate).unwrap();
    existing(
        decoded_respondent(&mut f, &post, configured, 0).unwrap(),
        outcome,
        None,
    );
    assert_eq!(
        f.owner
            .committed()
            .response(TestamentId::from_u128(95_002))
            .unwrap()
            .state(),
        ResponseState::Posted
    );
    assert!(f.owner.committed().artifact(evidence.id).is_some());
    drop(pressure);
}

#[test]
fn borrowed_respondent_refusals_preserve_receipt_credit_and_source_under_pressure() {
    let configured = limits(3);
    let mut f = Fixture::with_limits(configured);
    let (input, _) = diagnostic_input(&mut f, 96_001);
    let old_diagnostic = encoded_respondent(&input);
    drop(input);
    let old_claim = f.claim();
    let old_receipt = f.parent().receipt;
    let replacement = ParticipantId::from_u128(99);
    f.commit(
        ISSUER,
        NativeCommand::AdoptReceipt {
            expected: old_claim,
            previous: old_receipt,
            receipt: ReceiptId::from_u128(96_002),
            holder: replacement,
        },
    );
    let initial = credit(&f, configured);
    let budget = parent(&f);
    let mut pressure = Vec::new();
    fill(&budget, &mut pressure);
    let before = budget.stats().used;
    let prefix = f.owner.effective().sequence();
    assert!(decoded_respondent(&mut f, &old_diagnostic, configured, 1_048_576).is_err());
    assert_eq!(credit(&f, configured), initial);
    assert_eq!(budget.stats().used, before);
    assert_eq!(f.owner.effective().sequence(), prefix);

    // A current claim fence does not make an old holder's authored body valid.
    let old_provenance = f.artifact(
        96_003,
        WorkRole::Diagnostic {
            reason: EvidenceFailure::Work,
        },
    );
    let input = f.input(
        SUBJECT,
        NativeCommand::SubmitDiagnostic {
            claim: f.claim(),
            reason: EvidenceFailure::Work,
            artifact: old_provenance,
        },
    );
    let bytes = encoded_respondent(&input);
    drop(input);
    assert!(decoded_respondent(&mut f, &bytes, configured, 1_048_576).is_err());
    assert_eq!(credit(&f, configured), initial);
    assert_eq!(budget.stats().used, before);

    // The replacement can close, but malformed allocated identity never gets
    // an input loan and cannot consume the real remaining closing allowance.
    let command = NativeCommand::CloseResponse {
        claim: f.claim(),
        response: Binding {
            content: ContentHash([0; 32]),
            ..binding(96_004)
        },
        report: NativeResponseInput {
            summary: "Finished.".into(),
            confidence: Confidence::Committed,
            outcome: OutcomeKind::Complete,
            manifest: vec![],
            diagnostics: vec![],
        },
    };
    let input = f.input(replacement, command);
    let bytes = encoded_respondent(&input);
    drop(input);
    assert!(decoded_respondent(&mut f, &bytes, configured, 1_048_576).is_err());
    assert_eq!(credit(&f, configured), initial);
    assert_eq!(budget.stats().used, before);
    assert_eq!(f.owner.effective().sequence(), prefix);
    assert!(
        f.owner
            .effective()
            .response(TestamentId::from_u128(96_004))
            .is_none()
    );
    drop(pressure);
}
