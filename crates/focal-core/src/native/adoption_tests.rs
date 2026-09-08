use super::*;

#[path = "adoption_source_tests.rs"]
mod source_tests;
use crate::native::report_tests::{self as reports, EVALUATOR, QUALITY};
use focal_memory::{BudgetKind, BudgetLane};
use focal_model::lifecycle::artifact_descriptor::ResultProvenance;
use focal_model::{ReceiptFence, VerdictValue};

const REPLACEMENT: ParticipantId = ParticipantId::from_u128(85);
const REPLACEMENT_RECEIPT: ReceiptId = ReceiptId::from_u128(702);

fn adoption(f: &Fixture) -> NativeCommand {
    NativeCommand::AdoptReceipt {
        expected: f.claim(),
        previous: f.parent().receipt,
        receipt: REPLACEMENT_RECEIPT,
        holder: REPLACEMENT,
    }
}

fn copied(input: &NativeInput) -> NativeInput {
    let NativeCommand::AdoptReceipt {
        expected,
        previous,
        receipt,
        holder,
    } = input.command
    else {
        panic!("receipt adoption")
    };
    NativeInput {
        request: input.request,
        command: NativeCommand::AdoptReceipt {
            expected,
            previous,
            receipt,
            holder,
        },
    }
}

fn authored_artifact(
    f: &Fixture,
    id: u128,
    actor: ParticipantId,
    role: WorkRole,
) -> NativeArtifactInput {
    let parent = f.parent();
    let diagnostic = matches!(role, WorkRole::Diagnostic { .. });
    NativeArtifactInput::new(descriptor(ArtifactSpec {
        ledger: parent.ledger,
        id: ArtifactId::from_u128(id),
        schema: 1,
        kind: if diagnostic { "error" } else { "test-report" },
        schema_hash: if diagnostic { error_report_schema() } else { test_report_schema() },
        metadata: b"{}",
        payload: PayloadSpec::Inline(if diagnostic {
            br#"{"code":"work_failed","message":"The replacement could not complete the requested work."}"#
        } else {
            br#"{"passed":3,"failed":0,"skipped":0}"#
        }),
        producer: actor,
        receipt: Some(parent.receipt),
        result: None,
        work: Some(WorkProvenance { claim: parent.claim, cycle: parent.next_cycle, role }),
        inputs: &[],
        visibility: &[],
    })).unwrap()
}

fn output(f: &mut Fixture, id: u128, slot: u32) -> SlotBinding {
    let actor = f.parent().holder;
    let artifact = authored_artifact(f, id, actor, WorkRole::Output { slot });
    let binding = artifact.get().unwrap().binding();
    f.commit(
        actor,
        NativeCommand::SubmitWork {
            claim: f.claim(),
            slot,
            artifact,
        },
    );
    SlotBinding {
        slot,
        artifact: ArtifactRef {
            id: ArtifactId(binding.object.0),
            hash: binding.content,
        },
    }
}

fn diagnostic(f: &mut Fixture, id: u128) -> ArtifactRef {
    let actor = f.parent().holder;
    let artifact = authored_artifact(
        f,
        id,
        actor,
        WorkRole::Diagnostic {
            reason: EvidenceFailure::Work,
        },
    );
    let binding = artifact.get().unwrap().binding();
    f.commit(
        actor,
        NativeCommand::SubmitDiagnostic {
            claim: f.claim(),
            reason: EvidenceFailure::Work,
            artifact,
        },
    );
    ArtifactRef {
        id: ArtifactId(binding.object.0),
        hash: binding.content,
    }
}

fn deliver_current(f: &mut Fixture, id: u128) {
    let actor = f.parent().holder;
    f.commit(
        actor,
        NativeCommand::PostResponse {
            claim: f.claim(),
            expected: f.response(id),
        },
    );
    f.commit(
        ISSUER,
        NativeCommand::ReceiveResponse {
            claim: f.claim(),
            expected: f.response(id),
        },
    );
}

fn refused(f: &mut Fixture, actor: ParticipantId, command: NativeCommand) {
    let budget = f.owner.budget_stats();
    let range = f.owner.range_stats();
    let sequence = f.owner.effective().sequence();
    let claim = f.claim();
    let pending = f.owner.pending_len();
    assert!(f.stage(actor, command).is_err());
    assert_eq!(f.owner.budget_stats(), budget);
    assert_eq!(f.owner.range_stats(), range);
    assert_eq!(f.owner.effective().sequence(), sequence);
    assert_eq!(f.claim(), claim);
    assert_eq!(f.owner.pending_len(), pending);
}

fn work_key(response: u128, artifact: u128) -> EvaluationKey {
    EvaluationKey {
        claim: CLAIM,
        validation: ValidationId::from_u128(301),
        target: EvaluationTarget::Work {
            response: TestamentId::from_u128(response),
            slot: 0,
            artifact: ArtifactId::from_u128(artifact),
        },
        generation: 1,
    }
}

fn begin_work(f: &mut Fixture, key: EvaluationKey) {
    let expected = f.owner.effective().evaluation(key).unwrap().binding();
    f.commit(
        EVALUATOR,
        NativeCommand::BeginWork {
            claim: f.claim(),
            key,
            expected,
        },
    );
}

fn report_work(f: &Fixture, key: EvaluationKey, id: u128, value: VerdictValue) -> NativeCommand {
    let view = f.owner.effective();
    let state = view.evaluation(key).unwrap();
    let attempt = state
        .bind(view.definition(key.validation).unwrap())
        .unwrap()
        .current_attempt()
        .unwrap();
    let mut spec = reports::artifact_spec(id, attempt.evaluator, value);
    spec.receipt = state.receipt();
    spec.visibility = &[];
    spec.result = Some(ResultProvenance {
        claim: CLAIM,
        validation: key.validation,
        target: state.target(),
        generation: state.generation(),
        attempt,
        value,
    });
    let artifact = reports::descriptor(spec);
    let evidence = ArtifactRef {
        id: artifact.id(),
        hash: artifact.content_hash(),
    };
    NativeCommand::ReportWork {
        claim: f.claim(),
        key,
        expected: state.binding(),
        report: validation::Report {
            generation: state.generation(),
            attempt,
            value,
            evidence,
        },
        artifact: NativeArtifactInput::new(artifact).unwrap(),
    }
}

#[test]
fn pending_adoption_changes_only_entitlement_and_retries_exactly_after_later_receipt_use() {
    let mut f = Fixture::new();
    let before = f.claim();
    let old = f
        .owner
        .committed()
        .receipt(f.parent().receipt.receipt)
        .unwrap();
    let state = f.owner.committed().claim(CLAIM).unwrap().status();
    let input = NativeInput {
        request: request(ISSUER, 9000),
        command: adoption(&f),
    };
    let NativeStaging::Prepared { candidate, outcome } = f
        .owner
        .prepare(context(ISSUER, f.serial as u64), copied(&input), None)
        .unwrap()
    else {
        panic!("fresh adoption")
    };
    let view = f.owner.effective();
    let current = view.claim(CLAIM).unwrap();
    let new = view.receipt(REPLACEMENT_RECEIPT).unwrap();
    assert_eq!(current.binding(), before.next().unwrap());
    assert_eq!(current.status(), state);
    assert_eq!(current.receipt().unwrap().holder, REPLACEMENT);
    assert_eq!(
        new.fence,
        ReceiptFence {
            receipt: REPLACEMENT_RECEIPT,
            epoch: old.fence.epoch + 1
        }
    );
    assert_eq!(current.receipt().unwrap().fence, new.fence);
    assert_eq!(new.claim, CLAIM);
    assert_eq!(new.holder, REPLACEMENT);
    assert_eq!(new.acquired, outcome.sequence);
    assert_eq!(view.receipt(old.fence.receipt), Some(old));
    assert_eq!(outcome.events, 2);
    assert_eq!(
        view.event(outcome.sequence, 0).unwrap().fact,
        NativeFact::Claim(NativeClaimEvent {
            kind: NativeEventKind::ReceiptAdopted,
            owned_child: None,
            before: Some(before),
            after: current.binding(),
            status: state,
        })
    );
    assert_eq!(
        view.event(outcome.sequence, 1).unwrap().fact,
        NativeFact::ReceiptAdopted {
            claim: current.binding(),
            previous: ReceiptEntitlement {
                holder: old.holder,
                fence: old.fence
            },
            replacement: current.receipt().unwrap(),
            cause: outcome.intent,
        }
    );
    assert_eq!(current.response_count(), 0);
    assert_eq!(current.latest_response(), None);
    assert_eq!(current.local_sealed_at(), None);
    assert!(!current.local_complete());
    assert_eq!(
        (
            outcome.receipts,
            outcome.changed,
            outcome.responses,
            outcome.artifacts,
            outcome.results
        ),
        (1, 1, 0, 0, 0)
    );
    assert_eq!(f.owner.committed().claim(CLAIM).unwrap().binding(), before);
    assert!(f.owner.committed().receipt(REPLACEMENT_RECEIPT).is_none());
    let pending_budget = f.owner.budget_stats();
    assert_eq!(
        f.owner
            .prepare(context(ISSUER, f.serial as u64), copied(&input), None)
            .unwrap(),
        NativeStaging::Existing {
            candidate: Some(candidate),
            outcome
        }
    );
    assert_eq!(f.owner.budget_stats(), pending_budget);
    f.owner.publish_after_durable(candidate).unwrap();
    output(&mut f, 1801, 0);
    let budget = f.owner.budget_stats();
    assert_eq!(
        f.owner
            .prepare(context(ISSUER, f.serial as u64), copied(&input), None)
            .unwrap(),
        NativeStaging::Existing {
            candidate: None,
            outcome
        }
    );
    assert_eq!(f.owner.budget_stats(), budget);
    assert_eq!(f.owner.committed().receipt(old.fence.receipt), Some(old));
}

#[test]
fn adoption_rejects_stale_identity_wrong_actor_and_reused_receipts_without_mutation() {
    let mut f = Fixture::new();
    for actor in [SUBJECT, REPLACEMENT, EVALUATOR, QUALITY] {
        let command = adoption(&f);
        refused(&mut f, actor, command);
    }
    let original = f.claim();
    let previous = f.parent().receipt;
    for (expected, previous, receipt, holder) in [
        (
            original.next().unwrap(),
            previous,
            REPLACEMENT_RECEIPT,
            REPLACEMENT,
        ),
        (
            Binding {
                content: ContentHash([88; 32]),
                ..original
            },
            previous,
            REPLACEMENT_RECEIPT,
            REPLACEMENT,
        ),
        (
            original,
            ReceiptFence {
                epoch: previous.epoch + 1,
                ..previous
            },
            REPLACEMENT_RECEIPT,
            REPLACEMENT,
        ),
        (original, previous, previous.receipt, REPLACEMENT),
        (original, previous, ReceiptId::from_u128(0), REPLACEMENT),
        (
            original,
            previous,
            REPLACEMENT_RECEIPT,
            ParticipantId::from_u128(0),
        ),
    ] {
        refused(
            &mut f,
            ISSUER,
            NativeCommand::AdoptReceipt {
                expected,
                previous,
                receipt,
                holder,
            },
        );
    }
    f.commit(ISSUER, creation(9060, 2, &[], None).command);
    f.commit(
        ISSUER,
        NativeCommand::Post {
            expected: binding(2),
        },
    );
    let second = f
        .owner
        .effective()
        .claim(ClaimId::from_u128(2))
        .unwrap()
        .binding();
    f.commit(
        SUBJECT,
        NativeCommand::AcquireReceipt {
            expected: second,
            receipt: ReceiptId::from_u128(704),
        },
    );
    let current = f.claim();
    refused(
        &mut f,
        ISSUER,
        NativeCommand::AdoptReceipt {
            expected: current,
            previous,
            receipt: ReceiptId::from_u128(704),
            holder: REPLACEMENT,
        },
    );
    let command = adoption(&f);
    let input = NativeInput {
        request: request(ISSUER, 9001),
        command,
    };
    let budget = f.owner.budget_stats();
    assert!(
        f.owner
            .prepare(
                NativeContext {
                    principal: Principal::Node(ISSUER),
                    logical_time: f.serial as u64
                },
                copied(&input),
                None
            )
            .is_err()
    );
    assert_eq!(f.owner.budget_stats(), budget);
    let NativeStaging::Prepared { candidate, .. } = f
        .owner
        .prepare(context(ISSUER, f.serial as u64), copied(&input), None)
        .unwrap()
    else {
        panic!("pending adoption")
    };
    let current = f.claim();
    let current_receipt = f.parent().receipt;
    refused(
        &mut f,
        ISSUER,
        NativeCommand::AdoptReceipt {
            expected: current,
            previous: current_receipt,
            receipt: previous.receipt,
            holder: SUBJECT,
        },
    );
    refused(
        &mut f,
        ISSUER,
        NativeCommand::AdoptReceipt {
            expected: original,
            previous,
            receipt: ReceiptId::from_u128(703),
            holder: REPLACEMENT,
        },
    );
    let mut conflicting = copied(&input);
    let NativeCommand::AdoptReceipt { holder, .. } = &mut conflicting.command else {
        panic!()
    };
    *holder = SUBJECT;
    assert!(
        f.owner
            .prepare(context(ISSUER, f.serial as u64), conflicting, None)
            .is_err()
    );
    f.owner.publish_after_durable(candidate).unwrap();
    let current = f.claim();
    let current_receipt = f.parent().receipt;
    refused(
        &mut f,
        ISSUER,
        NativeCommand::AdoptReceipt {
            expected: current,
            previous: current_receipt,
            receipt: REPLACEMENT_RECEIPT,
            holder: SUBJECT,
        },
    );
}

#[test]
fn unfinished_output_and_diagnostic_history_survive_adoption_and_replacement_slot_reuse() {
    let mut f = Fixture::new();
    let abandoned = output(&mut f, 1810, 0);
    let abandoned_diagnostic = diagnostic(&mut f, 1811);
    let old = *f.owner.committed().work(abandoned.artifact.id).unwrap();
    let old_receipt = f.parent().receipt;
    let command = adoption(&f);
    f.commit(ISSUER, command);
    let stale = authored_artifact(&f, 1812, SUBJECT, WorkRole::Output { slot: 1 });
    let current = f.claim();
    refused(
        &mut f,
        SUBJECT,
        NativeCommand::SubmitWork {
            claim: current,
            slot: 1,
            artifact: stale,
        },
    );
    f.commit(
        ISSUER,
        NativeCommand::ReceiveWork {
            claim: current,
            expected: old.state.binding(),
        },
    );
    let observed = *f.owner.committed().work(abandoned.artifact.id).unwrap();
    assert_eq!(f.claim(), current);
    assert_eq!(
        observed.state.binding(),
        old.state.binding().next().unwrap()
    );
    assert_eq!(observed.state.state(), WorkArtifactState::Received);
    assert_eq!(observed.state.receipt(), old.state.receipt());
    assert_eq!(observed.state.producer(), old.state.producer());
    assert_eq!(observed.state.claim(), old.state.claim());
    assert_eq!(observed.state.cycle(), old.state.cycle());
    assert_eq!(observed.state.slot(), old.state.slot());
    assert_eq!(observed.next, old.next);
    let close_old = f.close(900, OutcomeKind::Complete, vec![abandoned], vec![]);
    refused(&mut f, REPLACEMENT, close_old);
    let replacement = [output(&mut f, 1813, 0), output(&mut f, 1814, 1)];
    let close = f.close(900, OutcomeKind::Complete, replacement.to_vec(), vec![]);
    f.commit(REPLACEMENT, close);
    deliver_current(&mut f, 900);
    f.commit(ISSUER, enter(&f, 900));
    let view = f.owner.committed();
    assert_eq!(view.claim(CLAIM).unwrap().status(), ClaimStatus::Satisfied);
    assert_eq!(*view.work(abandoned.artifact.id).unwrap(), observed);
    assert!(view.diagnostic(abandoned_diagnostic.id).is_some());
    let response = view.response(RESPONSE).unwrap();
    assert_eq!(
        response.identity().receipt,
        ReceiptFence {
            receipt: REPLACEMENT_RECEIPT,
            epoch: old_receipt.epoch + 1
        }
    );
    assert_eq!(response.respondent(), REPLACEMENT);
    assert_eq!(response.identity().cycle, 1);
    assert_eq!(response.identity().prior, None);
    assert_eq!(response.manifest(), replacement);
    f.owner
        .with_effective_acceptance(CLAIM, |projection| {
            assert!(matches!(
                projection.claim_decision().outcome(),
                aggregation::AggregateOutcome::LocalComplete { .. }
            ));
        })
        .unwrap();
}

#[test]
fn replacement_respondent_always_authors_failed_testimony_with_its_own_diagnostic_and_lineage() {
    for posted in [false, true] {
        let mut f = Fixture::new();
        let slots = [output(&mut f, 1820, 0), output(&mut f, 1821, 1)];
        f.commit(
            SUBJECT,
            f.close(900, OutcomeKind::Complete, slots.to_vec(), vec![]),
        );
        if posted {
            f.commit(
                SUBJECT,
                NativeCommand::PostResponse {
                    claim: f.claim(),
                    expected: f.response(900),
                },
            );
        }
        let original = f.owner.committed().response(RESPONSE).unwrap();
        let old = original.try_copy(original.copy_charge().unwrap()).unwrap();
        assert_eq!(
            f.owner.committed().claim(CLAIM).unwrap().status(),
            ClaimStatus::TestamentGenerated
        );
        f.commit(ISSUER, adoption(&f));
        assert_eq!(
            f.owner.committed().claim(CLAIM).unwrap().status(),
            ClaimStatus::TestamentGenerated
        );
        assert_eq!(
            f.owner.committed().claim(CLAIM).unwrap().response_count(),
            1
        );
        let claim = f.claim();
        let expected = f.response(900);
        refused(
            &mut f,
            SUBJECT,
            NativeCommand::PostResponse { claim, expected },
        );
        refused(
            &mut f,
            ISSUER,
            NativeCommand::ReceiveResponse { claim, expected },
        );
        let empty_failure = f.close(901, OutcomeKind::Failed, vec![], vec![]);
        refused(&mut f, REPLACEMENT, empty_failure);
        let failure = diagnostic(&mut f, 1822);
        let attempted = f.close(901, OutcomeKind::Failed, vec![], vec![failure]);
        refused(&mut f, SUBJECT, attempted);
        f.commit(
            REPLACEMENT,
            f.close(901, OutcomeKind::Failed, vec![], vec![failure]),
        );
        let response = f
            .owner
            .committed()
            .response(TestamentId::from_u128(901))
            .unwrap();
        assert_eq!(response.state(), ResponseState::Generated);
        assert_eq!(response.respondent(), REPLACEMENT);
        assert_eq!(response.reported_outcome(), OutcomeKind::Failed);
        assert_eq!(response.identity().cycle, 2);
        assert_eq!(response.identity().prior, Some(RESPONSE));
        assert_eq!(
            response
                .diagnostics()
                .iter()
                .map(|row| row.artifact())
                .collect::<Vec<_>>(),
            vec![failure]
        );
        deliver_current(&mut f, 901);
        assert_eq!(
            f.owner.committed().claim(CLAIM).unwrap().status(),
            ClaimStatus::TestamentAcknowledged
        );
        f.commit(ISSUER, enter(&f, 901));
        assert_eq!(
            f.owner.committed().claim(CLAIM).unwrap().status(),
            ClaimStatus::ValidationIncomplete
        );
        let original = f.owner.committed().response(RESPONSE).unwrap();
        assert_eq!(original, &old);
        assert_eq!(
            f.owner.committed().claim(CLAIM).unwrap().response_count(),
            2
        );
    }
}

#[test]
fn pending_adoption_fences_actual_work_retry_but_discard_restores_funded_report_under_pressure() {
    let mut f = checked_slot_fixture_with_visits(ValidationMode::Required, 65_536);
    complete_response(&mut f, 900, 1830);
    let key = work_key(900, 1830);
    begin_work(&mut f, key);
    f.commit(EVALUATOR, report_work(&f, key, 1832, VerdictValue::Error));
    let previous = *f.owner.committed().evaluation(key).unwrap();
    let result = previous.last_result().unwrap();
    let accepted = *f
        .owner
        .committed()
        .result(NativeResultKey::of(result))
        .unwrap();
    let before = f.claim();
    let budget = f.owner.budget_stats();
    let range = f.owner.range_stats();
    let input = NativeInput {
        request: request(ISSUER, 9002),
        command: adoption(&f),
    };
    let NativeStaging::Prepared { candidate, outcome } = f
        .owner
        .prepare(context(ISSUER, f.serial as u64), copied(&input), None)
        .unwrap()
    else {
        panic!("adoption of active retry")
    };
    let current = *f.owner.effective().evaluation(key).unwrap();
    assert_eq!(current.binding(), previous.binding().next().unwrap());
    assert_eq!(
        current.fence().unwrap().reason,
        validation::FenceReason::ReceiptAdoption
    );
    assert_ne!(current.fence().unwrap().cause, ContentHash([0; 32]));
    assert_eq!(current.state(), previous.state());
    assert_eq!(current.phase(), previous.phase());
    assert_eq!(current.receipt(), previous.receipt());
    assert_eq!(current.generation(), previous.generation());
    assert_eq!(current.last_result(), Some(result));
    assert_eq!(
        *f.owner
            .effective()
            .result(NativeResultKey::of(result))
            .unwrap(),
        accepted
    );
    assert_eq!(outcome.results, 0);
    assert_eq!(outcome.artifacts, 0);
    let stale = report_work(&f, key, 1833, VerdictValue::Pass);
    refused(&mut f, EVALUATOR, stale);
    assert_eq!(f.owner.discard_from(candidate).unwrap(), 1);
    assert_eq!(f.claim(), before);
    assert_eq!(f.owner.effective().evaluation(key), Some(&previous));
    assert!(f.owner.effective().receipt(REPLACEMENT_RECEIPT).is_none());
    assert_eq!(f.owner.budget_stats(), budget);
    assert_eq!(f.owner.range_stats(), range);
    let source = f.owner.budget_for_test();
    let pressure = source
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Completion,
            source.limit() - source.stats().used,
        )
        .unwrap()
        .commit();
    f.commit(EVALUATOR, report_work(&f, key, 1833, VerdictValue::Pass));
    drop(pressure);
    assert_eq!(
        f.owner.committed().claim(CLAIM).unwrap().status(),
        ClaimStatus::Satisfied
    );
    assert_eq!(
        *f.owner
            .committed()
            .result(NativeResultKey::of(result))
            .unwrap(),
        accepted
    );
    assert_eq!(
        f.owner
            .committed()
            .claim(CLAIM)
            .unwrap()
            .receipt()
            .unwrap()
            .holder,
        SUBJECT
    );
    let expected = f.claim();
    let previous = f.parent().receipt;
    refused(
        &mut f,
        ISSUER,
        NativeCommand::AdoptReceipt {
            expected,
            previous,
            receipt: REPLACEMENT_RECEIPT,
            holder: REPLACEMENT,
        },
    );
}

#[test]
fn report_before_adoption_retains_the_actual_result_and_fences_its_next_attempt_in_order() {
    let mut f = checked_slot_fixture_with_visits(ValidationMode::Required, 65_536);
    complete_response(&mut f, 900, 1840);
    let key = work_key(900, 1840);
    begin_work(&mut f, key);
    let report = report_work(&f, key, 1842, VerdictValue::Error);
    let NativeStaging::Prepared {
        candidate: reported,
        outcome: report_outcome,
    } = f.stage(EVALUATOR, report).unwrap()
    else {
        panic!("actual pending report")
    };
    let previous = *f.owner.effective().evaluation(key).unwrap();
    let result = previous.last_result().unwrap();
    let accepted = *f
        .owner
        .effective()
        .result(NativeResultKey::of(result))
        .unwrap();
    assert_eq!(accepted.sequence(), report_outcome.sequence);
    let input = NativeInput {
        request: request(ISSUER, 9003),
        command: adoption(&f),
    };
    let NativeStaging::Prepared {
        candidate: adopted, ..
    } = f
        .owner
        .prepare(context(ISSUER, f.serial as u64), copied(&input), None)
        .unwrap()
    else {
        panic!("adoption after pending report")
    };
    let current = f.owner.effective().evaluation(key).unwrap();
    assert_eq!(current.binding(), previous.binding().next().unwrap());
    assert_eq!(current.last_result(), Some(result));
    assert_eq!(
        current.fence().unwrap().reason,
        validation::FenceReason::ReceiptAdoption
    );
    assert_eq!(
        f.owner.committed().evaluation(key).unwrap().last_result(),
        None
    );
    f.owner.publish_after_durable(reported).unwrap();
    f.owner.publish_after_durable(adopted).unwrap();
    assert_eq!(
        *f.owner
            .committed()
            .result(NativeResultKey::of(result))
            .unwrap(),
        accepted
    );
    let stale = report_work(&f, key, 1843, VerdictValue::Pass);
    refused(&mut f, EVALUATOR, stale);
    let replacement = [output(&mut f, 1844, 0), output(&mut f, 1845, 1)];
    f.commit(
        REPLACEMENT,
        f.close(901, OutcomeKind::Complete, replacement.to_vec(), vec![]),
    );
    deliver_current(&mut f, 901);
    let new_key = EvaluationKey {
        generation: 2,
        ..work_key(901, 1844)
    };
    begin_work(&mut f, new_key);
    f.commit(
        EVALUATOR,
        report_work(&f, new_key, 1846, VerdictValue::Pass),
    );
    assert_eq!(
        f.owner.committed().claim(CLAIM).unwrap().status(),
        ClaimStatus::Satisfied
    );
    assert_eq!(
        *f.owner
            .committed()
            .result(NativeResultKey::of(result))
            .unwrap(),
        accepted
    );
}

#[test]
fn adoption_pressure_refusal_keeps_existing_work_responsibility_reportable() {
    let mut f = checked_slot_fixture_with_visits(ValidationMode::Required, 65_536);
    complete_response(&mut f, 900, 1850);
    let key = work_key(900, 1850);
    begin_work(&mut f, key);
    let previous = *f.owner.committed().evaluation(key).unwrap();
    let source = f.owner.budget_for_test();
    let pressure = source
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Completion,
            source.limit() - source.stats().used,
        )
        .unwrap()
        .commit();
    let command = adoption(&f);
    refused(&mut f, ISSUER, command);
    assert_eq!(f.owner.committed().evaluation(key), Some(&previous));
    f.commit(EVALUATOR, report_work(&f, key, 1852, VerdictValue::Pass));
    drop(pressure);
    assert_eq!(
        f.owner.committed().claim(CLAIM).unwrap().status(),
        ClaimStatus::Satisfied
    );
    assert!(f.owner.committed().receipt(REPLACEMENT_RECEIPT).is_none());
}

fn from_core(core: Core<NativeState>) -> Fixture {
    let directory = tempfile::tempdir().unwrap();
    let store = ContentStore::open(
        directory.path(),
        StoreLimits {
            max_content_bytes: 2 * 1024 * 1024,
            max_staging_bytes: 4 * 1024 * 1024,
            max_uploads: 8,
            chunk_bytes: 17,
            max_manifest_bytes: 128 * 1024,
        },
    )
    .unwrap();
    Fixture {
        owner: NativeOwner::new(core).unwrap(),
        store,
        _directory: directory,
        serial: 100,
    }
}

#[test]
fn adoption_fences_both_ready_and_begun_admission_observers_without_changing_terminal_checks() {
    let mut core = reports::core();
    core.limits.plan_edges = 65_536;
    reports::publish(
        &mut core,
        10,
        creation(
            1,
            1,
            &[
                (ValidationMode::Required, false),
                (ValidationMode::Observe, false),
                (ValidationMode::Observe, false),
            ],
            None,
        ),
    );
    reports::publish(&mut core, 20, reports::post(2, binding(1)));
    for index in [1, 2] {
        let claim = core.native_claim(CLAIM).unwrap().binding();
        let expected = core
            .native_evaluation(reports::key(index))
            .unwrap()
            .binding();
        reports::publish(
            &mut core,
            30,
            reports::begin(10 + u128::from(index), claim, index, expected),
        );
    }
    let mut custody = reports::Custody::new();
    let input = reports::report_for(
        &core,
        None,
        9050,
        1,
        VerdictValue::Pass,
        reports::descriptor(reports::artifact_spec(1860, EVALUATOR, VerdictValue::Pass)),
    );
    let evidence = reports::verified(&mut custody, &input);
    let prepared = reports::report(&core, input, &[], &evidence);
    core.publish_native(prepared).unwrap();
    let expected = core.native_claim(CLAIM).unwrap().binding();
    reports::publish(
        &mut core,
        100,
        NativeInput {
            request: request(SUBJECT, 9051),
            command: NativeCommand::AcquireReceipt {
                expected,
                receipt: ReceiptId::from_u128(701),
            },
        },
    );
    let mut f = from_core(core);
    let before =
        [1, 2, 3].map(|index| *f.owner.committed().evaluation(reports::key(index)).unwrap());
    let result = before[0].last_result().unwrap();
    let accepted = *f
        .owner
        .committed()
        .result(NativeResultKey::of(result))
        .unwrap();
    f.commit(ISSUER, adoption(&f));
    assert_eq!(
        f.owner.committed().evaluation(reports::key(1)),
        Some(&before[0])
    );
    assert_eq!(
        *f.owner
            .committed()
            .result(NativeResultKey::of(result))
            .unwrap(),
        accepted
    );
    for (offset, index) in [(1, 2), (2, 3)] {
        let current = f.owner.committed().evaluation(reports::key(index)).unwrap();
        assert_eq!(current.binding(), before[offset].binding().next().unwrap());
        assert_eq!(current.state(), before[offset].state());
        assert_eq!(current.has_begun(), before[offset].has_begun());
        assert_eq!(current.last_result(), before[offset].last_result());
        assert_eq!(current.receipt(), None);
        assert_eq!(
            current.fence().unwrap().reason,
            validation::FenceReason::ReceiptAdoption
        );
    }
    let key = reports::key(2);
    let state = f.owner.effective().evaluation(key).unwrap();
    let attempt = state
        .bind(f.owner.effective().definition(key.validation).unwrap())
        .unwrap()
        .current_attempt()
        .unwrap();
    let mut spec = reports::artifact_spec(1861, EVALUATOR, VerdictValue::Pass);
    spec.result = Some(ResultProvenance {
        claim: CLAIM,
        validation: key.validation,
        target: state.target(),
        generation: state.generation(),
        attempt,
        value: VerdictValue::Pass,
    });
    let artifact = reports::descriptor(spec);
    let report = NativeCommand::ReportAdmission {
        claim: f.claim(),
        key,
        expected: state.binding(),
        report: validation::Report {
            generation: state.generation(),
            attempt,
            value: VerdictValue::Pass,
            evidence: ArtifactRef {
                id: artifact.id(),
                hash: artifact.content_hash(),
            },
        },
        artifact: NativeArtifactInput::new(artifact).unwrap(),
    };
    refused(&mut f, EVALUATOR, report);
    let claim = f.claim();
    let expected = f
        .owner
        .effective()
        .evaluation(reports::key(3))
        .unwrap()
        .binding();
    refused(
        &mut f,
        EVALUATOR,
        NativeCommand::BeginAdmission {
            claim,
            key: reports::key(3),
            expected,
        },
    );
}

#[test]
fn exhausted_response_allowance_and_elapsed_claim_deadline_refuse_new_responsibility() {
    let mut f = Fixture::new();
    for response in 900..904 {
        f.commit(
            SUBJECT,
            f.close(response, OutcomeKind::Complete, vec![], vec![]),
        );
    }
    assert_eq!(
        f.owner.committed().claim(CLAIM).unwrap().response_count(),
        4
    );
    let command = adoption(&f);
    refused(&mut f, ISSUER, command);

    let mut core = reports::core();
    core.limits.plan_edges = 65_536;
    let mut input = creation(1, 1, &[], None);
    let NativeCommand::Create { claims, .. } = &mut input.command else {
        panic!()
    };
    claims[0].definition.deadline = Some(focal_model::Deadline {
        timer: focal_model::TimerId::from_u128(99),
        generation: 1,
        at: 100,
    });
    reports::publish(&mut core, 10, input);
    reports::publish(&mut core, 20, reports::post(2, binding(1)));
    let expected = core.native_claim(CLAIM).unwrap().binding();
    reports::publish(
        &mut core,
        30,
        NativeInput {
            request: request(SUBJECT, 3),
            command: NativeCommand::AcquireReceipt {
                expected,
                receipt: ReceiptId::from_u128(701),
            },
        },
    );
    let mut f = from_core(core);
    f.serial = 99;
    let command = adoption(&f);
    refused(&mut f, ISSUER, command);
    assert_eq!(
        f.owner.committed().claim(CLAIM).unwrap().status(),
        ClaimStatus::Received
    );
    assert!(f.owner.committed().receipt(REPLACEMENT_RECEIPT).is_none());
}
