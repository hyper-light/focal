use super::*;
use focal_model::ReceiptFence;

const CLAIM: ClaimId = ClaimId::from_u128(1);
const HOLDER: ParticipantId = ParticipantId::from_u128(86);

fn adopt(f: &Fixture) -> NativeCommand {
    NativeCommand::AdoptReceipt {
        expected: f.claim(),
        previous: f.parent().receipt,
        receipt: ReceiptId::from_u128(702),
        holder: HOLDER,
    }
}

fn fresh_output(f: &Fixture, id: u128, slot: u32) -> (NativeCommand, SlotBinding) {
    let parent = f.parent();
    let descriptor = descriptor(ArtifactSpec {
        ledger: parent.ledger,
        id: ArtifactId::from_u128(id),
        schema: 1,
        kind: "test-report",
        schema_hash: test_report_schema(),
        metadata: b"{}",
        payload: PayloadSpec::Inline(br#"{"passed":3,"failed":0,"skipped":0}"#),
        producer: parent.holder,
        receipt: Some(parent.receipt),
        result: None,
        work: Some(WorkProvenance {
            claim: CLAIM,
            cycle: parent.next_cycle,
            role: WorkRole::Output { slot },
        }),
        inputs: &[],
        visibility: &[],
    });
    let binding = SlotBinding {
        slot,
        artifact: ArtifactRef {
            id: descriptor.id(),
            hash: descriptor.content_hash(),
        },
    };
    (
        NativeCommand::SubmitWork {
            claim: f.claim(),
            slot,
            artifact: NativeArtifactInput::new(descriptor).unwrap(),
        },
        binding,
    )
}

#[test]
fn adoption_fences_increment_attempts_reopens_sealed_target_membership_and_retains_original_history()
 {
    let mut f = fixture(&[
        (ValidationMode::Required, Program::Programmatic),
        (ValidationMode::Observe, Program::Direct),
    ]);
    let old = f.work(801, 0);
    let required = evaluation_key(&f, 1, old.artifact.id);
    let observe = evaluation_key(&f, 2, old.artifact.id);
    f.commit(EVALUATOR, begin(&f, required));
    f.commit(EVALUATOR, report(&f, required, 901, VerdictValue::Error));
    f.commit(
        ISSUER,
        NativeCommand::SealIncrementTargets { claim: f.claim() },
    );
    let previous = *f.owner.committed().evaluation(required).unwrap();
    let ready = *f.owner.committed().evaluation(observe).unwrap();
    let result = previous.last_result().unwrap();
    let accepted = *f
        .owner
        .committed()
        .result(NativeResultKey::of(result))
        .unwrap();
    let old_work = *f.owner.committed().work(old.artifact.id).unwrap();
    let old_receipt = f.parent().receipt;
    let before_budget = f.owner.budget_stats();
    let before_range = f.owner.range_stats();
    assert!(
        f.owner
            .committed()
            .registrations(CLAIM)
            .unwrap()
            .increment_targets_sealed()
    );

    let (adopted, outcome) = prepared(f.stage(ISSUER, adopt(&f)).unwrap());
    assert_eq!((outcome.artifacts, outcome.results), (0, 0));
    assert!(
        !f.owner
            .effective()
            .registrations(CLAIM)
            .unwrap()
            .increment_targets_sealed()
    );
    assert!(
        f.owner
            .committed()
            .registrations(CLAIM)
            .unwrap()
            .increment_targets_sealed()
    );
    for (key, source) in [(required, previous), (observe, ready)] {
        let changed = f.owner.effective().evaluation(key).unwrap();
        assert_eq!(changed.binding(), source.binding().next().unwrap());
        assert_eq!(
            changed.fence().unwrap().reason,
            validation::FenceReason::ReceiptAdoption
        );
        assert_eq!(changed.state(), source.state());
        assert_eq!(changed.has_begun(), source.has_begun());
        assert_eq!(changed.last_result(), source.last_result());
        assert_eq!(changed.receipt(), source.receipt());
    }
    let (new_output, _) = fresh_output(&f, 802, 0);
    prepared(f.stage(HOLDER, new_output).unwrap());
    assert!(
        f.owner
            .effective()
            .work(ArtifactId::from_u128(802))
            .is_some()
    );
    assert_eq!(f.owner.discard_from(adopted).unwrap(), 2);
    assert_eq!(f.owner.budget_stats(), before_budget);
    assert_eq!(f.owner.range_stats(), before_range);
    assert_eq!(f.owner.effective().evaluation(required), Some(&previous));
    assert_eq!(f.owner.effective().evaluation(observe), Some(&ready));
    assert!(
        f.owner
            .effective()
            .registrations(CLAIM)
            .unwrap()
            .increment_targets_sealed()
    );
    assert!(
        f.owner
            .effective()
            .work(ArtifactId::from_u128(802))
            .is_none()
    );
    assert!(
        f.owner
            .effective()
            .receipt(ReceiptId::from_u128(702))
            .is_none()
    );

    f.commit(ISSUER, adopt(&f));
    let stale = report(&f, required, 902, VerdictValue::Pass);
    assert!(f.stage(EVALUATOR, stale).is_err());
    assert!(f.stage(QUALITY, begin(&f, observe)).is_err());
    let replacement_receipt = ReceiptFence {
        receipt: ReceiptId::from_u128(702),
        epoch: old_receipt.epoch + 1,
    };
    assert_eq!(f.parent().receipt, replacement_receipt);
    assert_eq!(f.parent().next_cycle, 1);
    let mut manifest = Vec::new();
    for (id, slot) in [(802, 0), (803, 1)] {
        let (command, artifact) = fresh_output(&f, id, slot);
        f.commit(HOLDER, command);
        manifest.push(artifact);
        let new_key = evaluation_key(&f, 1, artifact.artifact.id);
        assert_eq!(new_key.generation, required.generation);
        assert_ne!(new_key, required);
        assert_eq!(
            f.owner.committed().evaluation(new_key).unwrap().receipt(),
            Some(replacement_receipt)
        );
        f.commit(EVALUATOR, begin(&f, new_key));
        f.commit(EVALUATOR, report(&f, new_key, id + 200, VerdictValue::Pass));
    }
    f.commit(
        ISSUER,
        NativeCommand::SealIncrementTargets { claim: f.claim() },
    );
    f.commit(
        HOLDER,
        f.close(900, OutcomeKind::Complete, manifest.clone(), vec![]),
    );
    f.commit(
        HOLDER,
        NativeCommand::PostResponse {
            claim: f.claim(),
            expected: f.response(900),
        },
    );
    f.commit(
        ISSUER,
        NativeCommand::ReceiveResponse {
            claim: f.claim(),
            expected: f.response(900),
        },
    );
    f.commit(
        ISSUER,
        NativeCommand::EnterWholeWork {
            claim: f.claim(),
            expected: f.response(900),
        },
    );
    let view = f.owner.committed();
    assert_eq!(view.claim(CLAIM).unwrap().status(), ClaimStatus::Satisfied);
    assert_eq!(*view.result(NativeResultKey::of(result)).unwrap(), accepted);
    assert_eq!(*view.work(old.artifact.id).unwrap(), old_work);
    assert_eq!(
        view.response(TestamentId::from_u128(900))
            .unwrap()
            .manifest(),
        manifest
    );
    assert!(view.evaluation(required).unwrap().fence().is_some());
    assert!(view.evaluation(observe).unwrap().fence().is_some());
    f.owner
        .with_effective_acceptance(CLAIM, |projection| {
            assert!(matches!(
                projection.claim_decision().outcome(),
                aggregation::AggregateOutcome::LocalComplete { .. }
            ));
        })
        .unwrap();
    f.owner
        .with_effective_audit(CLAIM, |audit| {
            assert!(audit.cohort().complete());
            assert!(audit.cohort().results().contains(&result));
            assert_eq!(
                audit.publication(result),
                Some(focal_model::lifecycle::aggregation::PublicationPosition {
                    sequence: accepted.sequence(),
                    ordinal: accepted.ordinal()
                })
            );
        })
        .unwrap();
}
