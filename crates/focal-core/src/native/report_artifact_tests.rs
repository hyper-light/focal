use super::*;
use crate::native::{admission_authority, report_artifact};
use focal_model::ReceiptId;
use focal_model::lifecycle::artifact_descriptor::ResultProvenance;

fn artifact_limits() -> ArtifactLimits {
    ArtifactLimits {
        kind_bytes: 128,
        metadata_bytes: 1024,
        inline_bytes: 65536,
        inputs: 16,
        visibility_labels: 16,
        visibility_label_bytes: 128,
        construction_bytes: 128 * 1024,
    }
}

fn specification<'a>(
    view: &View<'_>,
    id: u128,
    value: VerdictValue,
    inputs: &'a [ObjectRef],
    visibility: &'a [&'a str],
) -> ArtifactSpec<'a> {
    let state = view.evaluation(key(1)).unwrap();
    let definition = view.definition(key(1).validation).unwrap();
    let attempt = state.bind(definition).unwrap().current_attempt().unwrap();
    let mut spec = artifact_spec(id, attempt.evaluator, value);
    spec.result = Some(ResultProvenance {
        claim: key(1).claim,
        validation: key(1).validation,
        target: state.target(),
        generation: state.generation(),
        attempt,
        value,
    });
    spec.inputs = inputs;
    spec.visibility = visibility;
    spec
}

#[derive(Clone, Copy)]
struct Frame {
    context: NativeContext,
    claim: Binding,
    expected: Binding,
    report: validation::Report,
}

fn frame(view: &View<'_>, descriptor: &ArtifactDescriptor, value: VerdictValue) -> Frame {
    let state = view.evaluation(key(1)).unwrap();
    let attempt = state
        .bind(view.definition(key(1).validation).unwrap())
        .unwrap()
        .current_attempt()
        .unwrap();
    Frame {
        context: context(attempt.evaluator, 100),
        claim: view.claim(key(1).claim).unwrap().binding(),
        expected: state.binding(),
        report: validation::Report {
            generation: state.generation(),
            attempt,
            value,
            evidence: ArtifactRef {
                id: descriptor.id(),
                hash: descriptor.content_hash(),
            },
        },
    }
}

fn assert_authority(
    view: &View<'_>,
    spec: ArtifactSpec<'_>,
    value: VerdictValue,
    change: impl FnOnce(&mut Frame),
    expected: Result<(), ContractError>,
) {
    let owned = descriptor(spec);
    let borrowed =
        ArtifactDescriptor::prepare_source(&spec, artifact_limits(), usize::MAX).unwrap();
    assert_eq!(owned.content_hash(), borrowed.content_hash());
    assert_eq!(
        report_artifact::ArtifactView::fields(&owned),
        borrowed.fields()
    );
    let mut frame = frame(view, &owned, value);
    change(&mut frame);
    let owned_result = admission_authority::report(
        view,
        frame.context,
        frame.claim,
        key(1),
        frame.expected,
        frame.report,
        &owned,
        native_limits(),
    );
    let borrowed_result = admission_authority::report_view(
        view,
        frame.context,
        frame.claim,
        key(1),
        frame.expected,
        frame.report,
        &borrowed,
        native_limits(),
    );
    match expected {
        Ok(()) => {
            let (owned_rows, owned_authority) = owned_result.unwrap();
            let (borrowed_rows, borrowed_authority) = borrowed_result.unwrap();
            assert!(std::ptr::eq(owned_rows.parent, borrowed_rows.parent));
            assert!(std::ptr::eq(
                owned_rows.definition,
                borrowed_rows.definition
            ));
            assert!(std::ptr::eq(owned_rows.registry, borrowed_rows.registry));
            assert!(std::ptr::eq(owned_rows.state, borrowed_rows.state));
            assert_eq!(
                owned_rows.registration_index,
                borrowed_rows.registration_index
            );
            assert_eq!(owned_authority, borrowed_authority);
            assert_eq!(borrowed_authority.attempt(), frame.report.attempt);
            assert_eq!(borrowed_authority.schema(), spec.schema_hash);
        }
        Err(expected) => {
            for result in [owned_result, borrowed_result] {
                match result {
                    Err(NativeError::Contract(actual)) => assert_eq!(actual, expected),
                    Err(other) => panic!("expected {expected:?}, got {other:?}"),
                    Ok(_) => panic!("expected {expected:?}, got report authority"),
                }
            }
        }
    }
}

fn assert_envelope(descriptor: &impl report_artifact::ArtifactView) {
    let fields = descriptor.fields();
    let heap = descriptor.heap_charge().unwrap();
    let limits = ArtifactLimits {
        kind_bytes: fields.kind.len(),
        metadata_bytes: fields.metadata.len(),
        inline_bytes: match fields.payload {
            PayloadSpec::Inline(bytes) => bytes.len(),
            PayloadSpec::Content(_) => 0,
        },
        inputs: descriptor.input_count(),
        visibility_labels: descriptor.visibility_count(),
        visibility_label_bytes: descriptor
            .visibility()
            .map(|label| label.unwrap().len())
            .max()
            .unwrap(),
        construction_bytes: descriptor.retained_bytes().unwrap(),
    };
    report_artifact::check_limits(descriptor, limits, heap, "report dimensions").unwrap();
    assert!(matches!(
        report_artifact::check_limits(descriptor, limits, heap - 1, "report dimensions"),
        Err(NativeError::Capacity("preparation bytes"))
    ));
    for case in 0..7 {
        let mut short = limits;
        match case {
            0 => short.kind_bytes -= 1,
            1 => short.metadata_bytes -= 1,
            2 => short.inline_bytes -= 1,
            3 => short.inputs -= 1,
            4 => short.visibility_labels -= 1,
            5 => short.visibility_label_bytes -= 1,
            _ => short.construction_bytes -= 1,
        }
        let expected = if case == 6 {
            "preparation bytes"
        } else {
            "report dimensions"
        };
        assert!(matches!(
            report_artifact::check_limits(descriptor, short, heap, "report dimensions"),
            Err(NativeError::Capacity(actual)) if actual == expected
        ));
    }
}

#[test]
fn actual_source_plan_and_owned_admission_authority_share_fields_and_exact_envelope() {
    let core = running(&[(ValidationMode::Required, false)]);
    let view = View {
        state: &core.state,
        tail: None,
    };
    let inputs = [ObjectRef::claim(ledger(), key(1).claim)];
    let before = core.native_budget();
    let sequence = core.native_sequence();
    let original = *view.evaluation(key(1)).unwrap();
    for value in [
        VerdictValue::Pass,
        VerdictValue::Fail,
        VerdictValue::Incomplete,
        VerdictValue::Error,
    ] {
        let spec = specification(&view, 701, value, &inputs, &["internal"]);
        assert_authority(&view, spec, value, |_| {}, Ok(()));
        let owned = descriptor(spec);
        let borrowed =
            ArtifactDescriptor::prepare_source(&spec, artifact_limits(), usize::MAX).unwrap();
        assert_eq!(
            report_artifact::ArtifactView::retained_bytes(&owned).unwrap(),
            report_artifact::ArtifactView::retained_bytes(&borrowed).unwrap()
        );
        assert_eq!(
            report_artifact::ArtifactView::heap_charge(&owned).unwrap(),
            report_artifact::ArtifactView::heap_charge(&borrowed).unwrap()
        );
        assert_envelope(&owned);
        assert_envelope(&borrowed);
    }
    assert_eq!(core.native_budget(), before);
    assert_eq!(core.native_sequence(), sequence);
    assert_eq!(*view.evaluation(key(1)).unwrap(), original);
    assert!(core.native_artifact(ArtifactId::from_u128(701)).is_none());
}

#[test]
fn borrowed_admission_checks_exact_actor_receipt_evidence_and_registered_provenance() {
    let core = running(&[(ValidationMode::Required, false)]);
    let view = View {
        state: &core.state,
        tail: None,
    };
    for case in 0..12 {
        let mut spec = specification(&view, 702, VerdictValue::Pass, &[], &["internal"]);
        let expected = match case {
            0 => {
                spec.producer = SUBJECT;
                spec.result.as_mut().unwrap().attempt.evaluator = SUBJECT;
                ContractError::WrongActor
            }
            1 => {
                spec.receipt = Some(focal_model::ReceiptFence {
                    receipt: ReceiptId::from_u128(799),
                    epoch: 1,
                });
                ContractError::StaleReceipt
            }
            2 => {
                spec.schema_hash = error_report_schema();
                ContractError::MissingEvidence
            }
            3 => {
                spec.result = None;
                ContractError::MissingEvidence
            }
            4 => {
                spec.result.as_mut().unwrap().generation += 1;
                ContractError::MissingEvidence
            }
            5 => {
                spec.result.as_mut().unwrap().attempt.definition = ContentHash([77; 32]);
                ContractError::MissingEvidence
            }
            6 => {
                spec.result.as_mut().unwrap().value = VerdictValue::Fail;
                ContractError::MissingEvidence
            }
            7 => ContractError::WrongObject,
            8 => ContractError::ContentConflict,
            9 => ContractError::WrongActor,
            10 => {
                spec.ledger.session = SessionId::from_u128(799);
                let validation::Target::Admission { claim } =
                    &mut spec.result.as_mut().unwrap().target
                else {
                    panic!("admission fixture")
                };
                claim.ledger = spec.ledger;
                ContractError::WrongLedger
            }
            _ => {
                spec.result.as_mut().unwrap().validation = ValidationId::from_u128(799);
                ContractError::MissingEvidence
            }
        };
        assert_authority(
            &view,
            spec,
            VerdictValue::Pass,
            |frame| match case {
                7 => frame.report.evidence.id = ArtifactId::from_u128(799),
                8 => frame.report.evidence.hash = ContentHash([78; 32]),
                9 => frame.context = context(SUBJECT, 100),
                _ => {}
            },
            Err(expected),
        );
    }
    let mut wrong_kind = specification(&view, 703, VerdictValue::Error, &[], &["internal"]);
    wrong_kind.kind = "test-report";
    assert_authority(
        &view,
        wrong_kind,
        VerdictValue::Error,
        |_| {},
        Err(ContractError::MissingEvidence),
    );
    assert!(core.native_artifact(ArtifactId::from_u128(702)).is_none());
    assert_eq!(
        view.evaluation(key(1)).unwrap().state(),
        validation::State::Validating
    );
}

#[test]
fn borrowed_reports_resolve_pending_inputs_inherit_visibility_and_refuse_collisions() {
    let core = running(&[(ValidationMode::Required, true)]);
    let mut custody = Custody::new();
    let input = report_for(
        &core,
        None,
        741,
        1,
        VerdictValue::Pass,
        descriptor(artifact_spec(741, EVALUATOR, VerdictValue::Pass)),
    );
    let token = verified(&mut custody, &input);
    let first = report(&core, input, &[], &token);
    let view = View {
        state: &core.state,
        tail: Some(&first),
    };
    let inputs = [ObjectRef {
        ledger: ledger(),
        kind: ObjectKind::Artifact,
        id: ObjectId::from_u128(741),
    }];
    let before = core.native_budget();
    let original = *first.evaluation(key(1)).unwrap();
    assert!(core.native_artifact(ArtifactId::from_u128(741)).is_none());
    assert!(first.artifact(ArtifactId::from_u128(741)).is_some());
    assert_authority(
        &view,
        specification(&view, 742, VerdictValue::Pass, &inputs, &["internal"]),
        VerdictValue::Pass,
        |_| {},
        Ok(()),
    );
    assert_authority(
        &view,
        specification(&view, 742, VerdictValue::Pass, &inputs, &[]),
        VerdictValue::Pass,
        |_| {},
        Err(ContractError::InvalidPolicy),
    );
    let unknown = [ObjectRef {
        id: ObjectId::from_u128(799),
        ..inputs[0]
    }];
    assert_authority(
        &view,
        specification(&view, 742, VerdictValue::Pass, &unknown, &["internal"]),
        VerdictValue::Pass,
        |_| {},
        Err(ContractError::InvalidTarget),
    );
    // This is an independently valid quality report body with the current
    // attempt and receipt. Only its ID collides with retained pending evidence.
    assert_authority(
        &view,
        specification(&view, 741, VerdictValue::Pass, &[], &["internal"]),
        VerdictValue::Pass,
        |_| {},
        Err(ContractError::ContentConflict),
    );
    assert_eq!(core.native_budget(), before);
    assert_eq!(*first.evaluation(key(1)).unwrap(), original);
    assert!(first.artifact(ArtifactId::from_u128(742)).is_none());
}
