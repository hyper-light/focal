use super::*;
use crate::native::report_tests as f;
use focal_evidence::{BuiltinNativeSchemas, StoreLimits};
use focal_memory::BudgetKind;
use focal_model::ValidationMode;

pub(in crate::native) fn limits(native: NativeLimits) -> Limits {
    let declaration = validation::Limits {
        handlers: 32,
        attempts: 64,
        slot_bytes: 4096,
    };
    Limits {
        native,
        acceptance: aggregation::Limits {
            max_slots: 256,
            max_checks: 4096,
            max_results: 8192,
            max_updates: 8192,
        },
        artifact: artifact_descriptor::Limits {
            kind_bytes: 1024,
            metadata_bytes: 65_536,
            inline_bytes: 1024 * 1024,
            inputs: 256,
            visibility_labels: 256,
            visibility_label_bytes: 4096,
            construction_bytes: 4 * 1024 * 1024,
        },
        claim: claim_descriptor::Limits {
            description_bytes: 65_536,
            relations: 4096,
            scopes: 256,
            scope_key_bytes: 4096,
            requirements: 4096,
            slots: 256,
            checks: 4096,
            construction_bytes: 4 * 1024 * 1024,
        },
        declaration,
        validation: validation_descriptor::Limits {
            declaration,
            description_bytes: 65_536,
            quality_bar_bytes: 65_536,
            contributors: 256,
            construction_bytes: 4 * 1024 * 1024,
        },
        response: ResponseLimits {
            artifacts: 256,
            diagnostics: 256,
            summary_bytes: 65_536,
            construction_bytes: 4 * 1024 * 1024,
        },
        creation_objects: 4096,
        work: Work {
            parsing: 1_000_000_000,
            source: 1_000_000_000,
            model: 1_000_000_000,
            lookup: 1_000_000_000,
        },
    }
}
pub(in crate::native) fn budget() -> MemoryBudget {
    MemoryBudget::new(64 * 1024 * 1024, 8 * 1024 * 1024).unwrap()
}
pub(in crate::native) fn store(directory: &std::path::Path) -> ContentStore {
    ContentStore::open(
        directory,
        StoreLimits {
            max_content_bytes: 2 * 1024 * 1024,
            max_staging_bytes: 4 * 1024 * 1024,
            max_uploads: 8,
            chunk_bytes: 17,
            max_manifest_bytes: 128 * 1024,
        },
    )
    .unwrap()
}
pub(in crate::native) fn encode(core: &Core<NativeState>) -> Vec<u8> {
    let plan = checkpoint::EncodingPlan::prepare(
        core,
        EncodingLimits {
            bytes: 32 << 20,
            visits: 1_000_000_000,
            rows: 100_000,
        },
    )
    .unwrap();
    let mut bytes = vec![0; plan.quote().bytes];
    plan.write_into(&mut bytes).unwrap();
    bytes
}
pub(in crate::native) fn inspect(bytes: &[u8]) -> checkpoint::StructuralCheckpoint<'_> {
    checkpoint::StructuralCheckpoint::inspect(
        bytes,
        InspectionLimits {
            bytes: bytes.len(),
            visits: 1_000_000_000,
            rows: 100_000,
            row_bytes: 32 << 20,
        },
    )
    .unwrap()
}
pub(in crate::native) fn compare(original: &Core<NativeState>, restored: &Core<NativeState>) {
    assert_eq!(original.native_sequence(), restored.native_sequence());
    assert_eq!(original.state.ledger, restored.state.ledger);
    assert_eq!(original.state.profile, restored.state.profile);
    assert_ne!(original.state.rows.id(), restored.state.rows.id());
    let original_bytes = encode(original);
    let restored_bytes = encode(restored);
    let original = inspect(&original_bytes);
    let restored = inspect(&restored_bytes);
    let bodies = |checkpoint: &checkpoint::StructuralCheckpoint<'_>| {
        checkpoint
            .rows(usize::MAX)
            .unwrap()
            .map(|row| {
                let row = row.unwrap();
                (row.key, row.body().to_vec())
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(bodies(&original), bodies(&restored));
}
pub(super) fn checksum(bytes: &mut [u8]) {
    let at = bytes.len() - 32;
    let (body, digest) = bytes.split_at_mut(at);
    let mut hash = blake3::Hasher::new_derive_key(checkpoint::HASH_DOMAIN);
    hash.update(body);
    digest.copy_from_slice(hash.finalize().as_bytes());
}
pub(super) fn body_offset(bytes: &[u8], key: Key) -> usize {
    inspect(bytes)
        .rows(usize::MAX)
        .unwrap()
        .map(Result::unwrap)
        .find(|row| row.key == key)
        .unwrap()
        .body()
        .as_ptr() as usize
        - bytes.as_ptr() as usize
}

#[test]
fn empty_and_live_admission_checkpoints_restore_exact_rows_in_a_fresh_incarnation() {
    let path = tempfile::tempdir().unwrap();
    let store = store(path.path());
    let mut core = f::core();
    for stage in 0..4 {
        let bytes = encode(&core);
        let checkpoint = inspect(&bytes);
        let budget = budget();
        let restored = restore(
            &checkpoint,
            RangeId(9081),
            limits(core.limits),
            budget.clone(),
            &store,
            &BuiltinNativeSchemas,
        )
        .unwrap();
        compare(&core, &restored);
        drop(restored);
        assert_eq!(budget.stats().used, 0);
        match stage {
            0 => {
                f::publish(
                    &mut core,
                    10,
                    f::creation(1, 1, &[(ValidationMode::Required, true)], None),
                );
            }
            1 => {
                f::publish(&mut core, 20, f::post(2, f::binding(1)));
            }
            2 => {
                let claim = core.native_claim(ClaimId::from_u128(1)).unwrap().binding();
                let evaluation = core.native_evaluation(f::key(1)).unwrap().binding();
                f::publish(&mut core, 30, f::begin(3, claim, 1, evaluation));
            }
            _ => {}
        }
    }
}

#[test]
fn failed_respondent_testimony_and_independent_validation_survive_every_delivery_stage() {
    for stage in 0..4 {
        let (core, store, _directory) = super::super::evidence::tests::recovery_fixture(stage);
        let bytes = encode(&core);
        let checkpoint = inspect(&bytes);
        let budget = budget();
        let restored = restore(
            &checkpoint,
            RangeId(9082),
            limits(core.limits),
            budget.clone(),
            &store,
            &BuiltinNativeSchemas,
        )
        .unwrap();
        compare(&core, &restored);
        let response = restored
            .native_response(TestamentId::from_u128(900))
            .unwrap();
        assert_eq!(
            response.reported_outcome(),
            focal_model::OutcomeKind::Failed
        );
        assert_eq!(response.diagnostics().len(), 2);
        assert!(
            restored
                .native_artifact(ArtifactId::from_u128(802))
                .is_some()
        );
        assert!(
            restored
                .native_artifact(ArtifactId::from_u128(803))
                .is_some()
        );
        drop(restored);
        assert_eq!(budget.stats().used, 0);
    }
}

#[test]
fn structural_missing_work_recovers_without_inventing_slot_check_results() {
    let (core, evidence, _directory) =
        super::super::evidence::tests::recovery_fixture_with_checks(3, false);
    let bytes = encode(&core);
    let memory = budget();
    let restored = restore(
        &inspect(&bytes),
        RangeId(9089),
        limits(core.limits),
        memory.clone(),
        &evidence,
        &BuiltinNativeSchemas,
    )
    .unwrap();
    compare(&core, &restored);
    assert!(
        restored
            .native_response(TestamentId::from_u128(900))
            .unwrap()
            .state()
            .is_terminal()
    );
    assert!(
        !restored
            .state
            .rows
            .entries()
            .any(|entry| matches!(entry.key, Key::MissingResult(_)))
    );
    drop(restored);
    assert_eq!(memory.stats().used, 0);
}

#[test]
fn cold_reopen_reads_existing_evidence_and_recovers_a_terminal_response() {
    let (core, evidence, directory) = super::super::evidence::tests::recovery_fixture(3);
    let bytes = encode(&core);
    let expected = core.native_sequence();
    let config = limits(core.limits);
    drop(core);
    drop(evidence);
    let evidence = store(directory.path());
    let budget = budget();
    let restored = restore(
        &inspect(&bytes),
        RangeId(9083),
        config,
        budget.clone(),
        &evidence,
        &BuiltinNativeSchemas,
    )
    .unwrap();
    assert_eq!(restored.native_sequence(), expected);
    assert!(
        restored
            .native_response(TestamentId::from_u128(900))
            .unwrap()
            .state()
            .is_terminal()
    );
    drop(restored);
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn restored_failed_testimony_resumes_delivery_with_the_uninterrupted_owners_exact_history() {
    let (mut original, evidence, _directory) = super::super::evidence::tests::recovery_fixture(0);
    original.limits.plan_edges = 65_536;
    let bytes = encode(&original);
    let config = limits(original.limits);
    let memory = budget();
    let mut restored = restore(
        &inspect(&bytes),
        RangeId(9087),
        config,
        memory.clone(),
        &evidence,
        &BuiltinNativeSchemas,
    )
    .unwrap();
    for step in 0u128..5 {
        let advance = |core: &mut Core<NativeState>| {
            let claim = core.native_claim(ClaimId::from_u128(1)).unwrap().binding();
            let expected = core
                .native_response(TestamentId::from_u128(900))
                .unwrap()
                .identity()
                .binding;
            let (actor, command) = match step {
                0 => (f::SUBJECT, NativeCommand::PostResponse { claim, expected }),
                1 => (
                    f::ISSUER,
                    NativeCommand::ReceiveResponse { claim, expected },
                ),
                2 => (f::ISSUER, NativeCommand::EnterWholeWork { claim, expected }),
                3 => (
                    f::ISSUER,
                    NativeCommand::GenerateResultTestament {
                        claim,
                        id: TestamentId::from_u128(950),
                    },
                ),
                _ => (
                    f::ISSUER,
                    NativeCommand::PostResultTestament {
                        expected: core
                            .native_result_testament(TestamentId::from_u128(950))
                            .unwrap()
                            .testament()
                            .binding(),
                    },
                ),
            };
            let input = NativeInput {
                request: f::request(actor, 90_000 + step),
                command,
            };
            let prepared =
                f::prepared(core.prepare_native(f::context(actor, 300 + step as u64), input, &[]));
            core.publish_native(prepared).unwrap()
        };
        assert_eq!(advance(&mut restored), advance(&mut original));
        compare(&original, &restored);
    }
    let response = restored
        .native_response(TestamentId::from_u128(900))
        .unwrap();
    assert_eq!(
        response.reported_outcome(),
        focal_model::OutcomeKind::Failed
    );
    assert!(response.state().is_terminal());
    let audit = restored
        .native_result_testament(TestamentId::from_u128(950))
        .unwrap();
    assert_eq!(
        audit.testament().state(),
        focal_model::lifecycle::audit::ResultTestamentState::Posted
    );
    assert!(!audit.publications().is_empty());
    let resumed_bytes = encode(&restored);
    drop(restored);
    assert_eq!(memory.stats().used, 0);
    let reopened = restore(
        &inspect(&resumed_bytes),
        RangeId(9088),
        config,
        memory.clone(),
        &evidence,
        &BuiltinNativeSchemas,
    )
    .unwrap();
    compare(&original, &reopened);
    drop(reopened);
    assert_eq!(memory.stats().used, 0);
}

#[test]
fn recomputed_checksum_does_not_authorize_inconsistent_root_counts() {
    let mut core = f::core();
    f::publish(&mut core, 10, f::creation(1, 1, &[], None));
    let mut bytes = encode(&core);
    let at = body_offset(&bytes, Key::Meta);
    bytes[at..at + 8].copy_from_slice(&2u64.to_le_bytes());
    checksum(&mut bytes);
    let checkpoint = inspect(&bytes);
    let budget = budget();
    let directory = tempfile::tempdir().unwrap();
    let evidence = store(directory.path());
    assert!(
        restore(
            &checkpoint,
            RangeId(9084),
            limits(core.limits),
            budget.clone(),
            &evidence,
            &BuiltinNativeSchemas
        )
        .is_err()
    );
    assert_eq!(budget.stats().used, 0);
    assert_eq!(core.native_sequence(), SessionSeq(1));
}

#[test]
fn missing_local_evidence_refuses_and_never_repairs_from_inline_descriptor_bytes() {
    let (core, _original_store, _original_directory) =
        super::super::evidence::tests::recovery_fixture(0);
    let bytes = encode(&core);
    let checkpoint = inspect(&bytes);
    let directory = tempfile::tempdir().unwrap();
    let empty = store(directory.path());
    let budget = budget();
    assert!(matches!(
        restore(
            &checkpoint,
            RangeId(9085),
            limits(core.limits),
            budget.clone(),
            &empty,
            &BuiltinNativeSchemas
        ),
        Err(NativeError::Evidence(_))
    ));
    assert_eq!(budget.stats().used, 0);
    // The exact same empty store must still refuse a second attempt; recovery
    // cannot manufacture a missing tree from the inline body on its first pass.
    assert!(
        restore(
            &checkpoint,
            RangeId(9085),
            limits(core.limits),
            budget.clone(),
            &empty,
            &BuiltinNativeSchemas
        )
        .is_err()
    );
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn cumulative_work_and_memory_refusals_refund_all_phases_then_allow_retry() {
    let (core, evidence, _directory) = super::super::evidence::tests::recovery_fixture(3);
    let bytes = encode(&core);
    let checkpoint = inspect(&bytes);
    let budget = budget();
    for field in 0..4 {
        let mut config = limits(core.limits);
        match field {
            0 => config.work.parsing = 0,
            1 => config.work.source = 0,
            2 => config.work.model = 0,
            _ => config.work.lookup = 0,
        }
        assert!(
            restore(
                &checkpoint,
                RangeId(9086),
                config,
                budget.clone(),
                &evidence,
                &BuiltinNativeSchemas
            )
            .is_err()
        );
        assert_eq!(budget.stats().used, 0);
    }
    let blocker = budget
        .reserve(
            BudgetKind::Recovery,
            BudgetLane::Completion,
            64 * 1024 * 1024,
        )
        .unwrap();
    assert!(
        restore(
            &checkpoint,
            RangeId(9086),
            limits(core.limits),
            budget.clone(),
            &evidence,
            &BuiltinNativeSchemas
        )
        .is_err()
    );
    drop(blocker);
    assert_eq!(budget.stats().used, 0);
    let restored = restore(
        &checkpoint,
        RangeId(9086),
        limits(core.limits),
        budget.clone(),
        &evidence,
        &BuiltinNativeSchemas,
    )
    .unwrap();
    compare(&core, &restored);
    drop(restored);
    assert_eq!(budget.stats().used, 0);
}
