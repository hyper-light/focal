use super::*;
use crate::native::report_tests::{ISSUER, SUBJECT, binding, creation, publish, request};
use focal_model::{Confidence, OutcomeKind};

fn core() -> Core<NativeState> {
    let limits = NativeLimits {
        preparation_bytes: 1024 * 1024,
        plan_nodes: 16,
        plan_edges: 4096,
        response_summary_bytes: 128,
        diagnostics_per_cycle: 4,
        range: RangeConfig {
            max_batch_entries: 128,
            ..RangeConfig::default()
        },
        ..NativeLimits::default()
    };
    let mut core = Core::new_native(
        binding(1).ledger,
        RangeId(918),
        limits,
        MemoryBudget::new(128 * 1024 * 1024, 16 * 1024 * 1024).unwrap(),
    )
    .unwrap();
    // Keep the authored mandatory pure Receipt declaration even though the
    // claim has no work slots or external evaluators.
    let initial = creation(1, 1, &[], None);
    publish(&mut core, 1, initial);
    publish(
        &mut core,
        2,
        NativeInput {
            request: request(ISSUER, 2),
            command: NativeCommand::Post {
                expected: binding(1),
            },
        },
    );
    let expected = core.native_claim(ClaimId::from_u128(1)).unwrap().binding();
    publish(
        &mut core,
        3,
        NativeInput {
            request: request(SUBJECT, 3),
            command: NativeCommand::AcquireReceipt {
                expected,
                receipt: ReceiptId::from_u128(80),
            },
        },
    );
    core
}

fn descriptor_limits() -> ArtifactLimits {
    ArtifactLimits {
        kind_bytes: 5,
        metadata_bytes: 128,
        inline_bytes: 512,
        inputs: 0,
        visibility_labels: 0,
        visibility_label_bytes: 0,
        construction_bytes: size_of::<ArtifactDescriptor>() + 1024,
    }
}

fn envelope(core: &Core<NativeState>) -> RespondentEnvelope {
    let view = View {
        state: &core.state,
        tail: None,
    };
    RespondentEnvelope::derive(
        &view,
        view.claim(ClaimId::from_u128(1)).unwrap(),
        core.limits,
        descriptor_limits(),
        EvidenceBounds {
            workspace_bytes: 4096,
            retained_bytes: 256,
        },
    )
    .unwrap()
}

fn report(summary: &str) -> NativeResponseInput {
    NativeResponseInput {
        summary: summary.into(),
        confidence: Confidence::Committed,
        outcome: OutcomeKind::Complete,
        manifest: Vec::new(),
        diagnostics: Vec::new(),
    }
}

fn close(core: &mut Core<NativeState>, serial: u128) -> Binding {
    let claim = core.native_claim(ClaimId::from_u128(1)).unwrap().binding();
    let response = binding(serial);
    publish(
        core,
        serial as u64,
        NativeInput {
            request: request(SUBJECT, serial),
            command: NativeCommand::CloseResponse {
                claim,
                response,
                report: report("The authored cycle is complete."),
            },
        },
    );
    core.native_response(TestamentId(response.object.0))
        .unwrap()
        .identity()
        .binding
}

#[test]
fn independent_action_credits_retain_every_version_and_release_only_consumed_shape() {
    let core = core();
    let envelope = envelope(&core);
    let all = RespondentCredit {
        diagnostics: 4,
        closes: 4,
        posts: 4,
    };
    let demand = envelope.demand(all).unwrap();
    assert_eq!(demand.actions, 12);
    assert_eq!(demand.slots.artifacts, 4);
    assert_eq!(demand.slots.identities, 4);
    assert_eq!(demand.slots.responses, 4);
    assert_eq!(demand.slots.outcomes, 12);
    assert_eq!(demand.slots.events, 24);
    assert_eq!(demand.slots.new_rows, 60);
    let mut remainder = all;
    for (operation, next) in [
        (
            NativeOperation::SubmitDiagnostic,
            RespondentCredit {
                diagnostics: 3,
                ..all
            },
        ),
        (
            NativeOperation::CloseResponse,
            RespondentCredit {
                diagnostics: 3,
                closes: 3,
                posts: 4,
            },
        ),
        (
            NativeOperation::PostResponse,
            RespondentCredit {
                diagnostics: 3,
                closes: 3,
                posts: 3,
            },
        ),
    ] {
        let before = envelope.demand(remainder).unwrap();
        let after = envelope.demand(next).unwrap();
        assert_eq!(
            before.retained_bytes - after.retained_bytes,
            envelope
                .storage(operation)
                .unwrap()
                .additional_retained_bytes()
                + crate::native::mutation::bytes(
                    envelope.storage(operation).unwrap().limits().changed_keys
                )
                .unwrap()
        );
        assert_eq!(before.actions - after.actions, 1);
        remainder = next;
    }
    assert_eq!(
        envelope
            .demand(RespondentCredit::default())
            .unwrap()
            .retained_bytes,
        0
    );
    assert!(
        envelope
            .demand(RespondentCredit { posts: 5, ..all })
            .is_err()
    );
    assert!(envelope.storage(NativeOperation::ReceiveResponse).is_err());
}

#[test]
fn reconstruction_keeps_both_generated_posts_after_later_cycles_close() {
    let mut core = core();
    let first = close(&mut core, 10);
    close(&mut core, 11);
    let before = envelope(&core);
    assert_eq!(
        before.maximum,
        RespondentCredit {
            diagnostics: 2,
            closes: 2,
            posts: 4
        }
    );
    let claim = core.native_claim(ClaimId::from_u128(1)).unwrap().binding();
    publish(
        &mut core,
        12,
        NativeInput {
            request: request(SUBJECT, 12),
            command: NativeCommand::PostResponse {
                claim,
                expected: first,
            },
        },
    );
    let after = envelope(&core);
    assert_eq!(
        after.maximum,
        RespondentCredit {
            diagnostics: 2,
            closes: 2,
            posts: 3
        }
    );
    assert_eq!(
        before.demand(before.maximum).unwrap().retained_bytes
            - after.demand(after.maximum).unwrap().retained_bytes,
        before.post.storage.additional_retained_bytes()
            + crate::native::mutation::bytes(6 + crate::native::index_rows::STATUS_ROWS).unwrap()
    );
}

#[test]
fn authored_buffer_capacity_cannot_spend_unreserved_construction_space() {
    let core = core();
    let envelope = envelope(&core);
    envelope.check_response_input(&report("Complete")).unwrap();
    let mut oversized = report("Complete");
    oversized
        .summary
        .try_reserve_exact(envelope.response_input_heap + 1)
        .unwrap();
    assert!(envelope.check_response_input(&oversized).is_err());
    let mut dimensions = report("Complete");
    dimensions.summary = "x".repeat(envelope.summary_limit + 1);
    assert!(envelope.check_response_input(&dimensions).is_err());
    for operation in [
        NativeOperation::SubmitDiagnostic,
        NativeOperation::CloseResponse,
        NativeOperation::PostResponse,
    ] {
        let construction = envelope.construction(operation, core.limits).unwrap();
        assert!(
            construction.temporary_bytes().unwrap()
                + transient(envelope.storage(operation).unwrap()).unwrap()
                <= envelope.workspace_bytes()
        );
    }
}

#[test]
fn impossible_diagnostic_or_complete_close_capacity_refuses_before_reservation() {
    let core = core();
    let view = View {
        state: &core.state,
        tail: None,
    };
    let claim = view.claim(ClaimId::from_u128(1)).unwrap();
    for limits in [
        NativeLimits {
            diagnostics_per_cycle: 0,
            ..core.limits
        },
        NativeLimits {
            preparation_bytes: 1,
            ..core.limits
        },
        NativeLimits {
            range: RangeConfig {
                max_batch_entries: 7,
                ..core.limits.range
            },
            ..core.limits
        },
    ] {
        let before = core.state.budget.stats();
        assert!(
            RespondentEnvelope::derive(
                &view,
                claim,
                limits,
                descriptor_limits(),
                EvidenceBounds {
                    workspace_bytes: 0,
                    retained_bytes: 0
                }
            )
            .is_err()
        );
        assert_eq!(core.state.budget.stats(), before);
    }
    assert!(multiply(usize::MAX, 2).is_err());
    assert_eq!(multiply(usize::MAX, 0).unwrap(), 0);
}
