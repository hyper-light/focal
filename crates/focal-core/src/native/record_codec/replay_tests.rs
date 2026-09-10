use super::super::{recovery::tests as checkpoint, tests::encode as encode_mutation};
use super::*;
use crate::native::report_tests as f;
use focal_evidence::BuiltinNativeSchemas;
use focal_evidence::ContentStore;
use focal_model::{ClaimStatus, OutcomeKind, ParticipantId, ValidationMode};

#[path = "replay_authored_tests.rs"]
mod authored_tests;
#[path = "replay_control_tests.rs"]
mod control_tests;
#[path = "replay_graph_tests.rs"]
mod graph_tests;

fn inspect(bytes: &[u8]) -> StructuralRecord<'_> {
    StructuralRecord::inspect(
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
fn restored(original: &Core<NativeState>, id: u128, store: &ContentStore) -> Core<NativeState> {
    let bytes = checkpoint::encode(original);
    recovery::restore(
        &checkpoint::inspect(&bytes),
        RangeId(id),
        checkpoint::limits(original.limits),
        checkpoint::budget(),
        store,
        &BuiltinNativeSchemas,
    )
    .unwrap()
}
fn prepare_input(core: &Core<NativeState>, time: u64, input: NativeInput) -> NativePrepared {
    f::prepared(core.prepare_native(f::context(input.request.principal, time), input, &[]))
}
fn replay(
    core: &Core<NativeState>,
    bytes: &[u8],
    original: RangeId,
    store: &ContentStore,
) -> Result<NativePrepared, NativeError> {
    prepare(
        core,
        &inspect(bytes),
        original,
        checkpoint::limits(core.limits),
        store,
        &BuiltinNativeSchemas,
    )
}
fn apply(
    original: &mut Core<NativeState>,
    recovered: &mut Core<NativeState>,
    time: u64,
    input: NativeInput,
    store: &ContentStore,
) -> Vec<u8> {
    let prepared = prepare_input(original, time, input);
    // Serialize the actual unpublished candidate; replay never receives or
    // reexecutes its participant command or NativeContext.
    let bytes = encode_mutation(&prepared);
    let sequence = recovered.native_sequence();
    let recovered_prepared = replay(recovered, &bytes, original.state.rows.id(), store).unwrap();
    assert_eq!(recovered.native_sequence(), sequence);
    assert_eq!(prepared.outcome(), recovered_prepared.outcome());
    assert_eq!(recovered_prepared.fragments.id(), recovered.state.rows.id());
    assert_ne!(recovered_prepared.fragments.id(), prepared.fragments.id());
    assert_eq!(
        original.publish_native(prepared).unwrap(),
        recovered.publish_native(recovered_prepared).unwrap()
    );
    checkpoint::compare(original, recovered);
    bytes
}
fn checksum(bytes: &mut [u8]) {
    let at = bytes.len() - 32;
    let (body, checksum) = bytes.split_at_mut(at);
    let mut hash = blake3::Hasher::new_derive_key(HASH_DOMAIN);
    hash.update(body);
    checksum.copy_from_slice(hash.finalize().as_bytes());
}
fn body_offset(bytes: &[u8], key: Key) -> usize {
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
fn replay_from_genesis_matches_actual_create_post_and_begin_without_command_reexecution() {
    let directory = tempfile::tempdir().unwrap();
    let store = checkpoint::store(directory.path());
    let mut original = f::core();
    let mut recovered = restored(&original, 10_001, &store);
    let source = original.state.rows.id();
    let memory = recovered.state.budget.clone();
    let mut records = Vec::new();
    records.push(apply(
        &mut original,
        &mut recovered,
        10,
        f::creation(1, 1, &[(ValidationMode::Required, true)], None),
        &store,
    ));
    records.push(apply(
        &mut original,
        &mut recovered,
        20,
        f::post(2, f::binding(1)),
        &store,
    ));
    let claim = original
        .native_claim(ClaimId::from_u128(1))
        .unwrap()
        .binding();
    let evaluation = original.native_evaluation(f::key(1)).unwrap().binding();
    records.push(apply(
        &mut original,
        &mut recovered,
        30,
        f::begin(3, claim, 1, evaluation),
        &store,
    ));
    assert_eq!(recovered.native_sequence(), SessionSeq(3));
    assert!(recovered.native_evaluation(f::key(1)).unwrap().has_begun());
    // Both recent and older duplicate records are rejected at the exact-base
    // boundary, without another outcome, event or memory owner being retained.
    let before = checkpoint::encode(&recovered);
    let budget = recovered.native_budget();
    for bytes in &records {
        assert!(replay(&recovered, bytes, source, &store).is_err());
        assert_eq!(recovered.native_budget(), budget);
        assert_eq!(checkpoint::encode(&recovered), before);
    }
    drop(recovered);
    assert_eq!(memory.stats().used, 0);
}

#[test]
fn replay_refuses_wrong_source_range_profile_and_order_before_publishing_anything() {
    let directory = tempfile::tempdir().unwrap();
    let store = checkpoint::store(directory.path());
    let mut original = f::core();
    let mut recovered = restored(&original, 10_002, &store);
    let source = original.state.rows.id();
    let created = prepare_input(&original, 10, f::creation(1, 1, &[], None));
    let creation = encode_mutation(&created);
    original.publish_native(created).unwrap();
    let posted = prepare_input(&original, 20, f::post(2, f::binding(1)));
    let posting = encode_mutation(&posted);
    let original_budget = recovered.native_budget();
    let original_rows = checkpoint::encode(&recovered);
    assert!(replay(&recovered, &creation, RangeId(source.0 + 1), &store).is_err());
    assert!(replay(&recovered, &posting, source, &store).is_err());
    for field in 0..2 {
        let mut changed = creation.clone();
        if field == 0 {
            // Profile follows the fixed magic and version.
            changed[10] = 1;
        } else {
            // The source RangeId follows profile and the 32-byte ledger.
            changed[43..59].copy_from_slice(&(source.0 + 1).to_le_bytes());
        }
        checksum(&mut changed);
        let record = inspect(&changed);
        assert!(
            prepare(
                &recovered,
                &record,
                source,
                checkpoint::limits(recovered.limits),
                &store,
                &BuiltinNativeSchemas
            )
            .is_err()
        );
        assert_eq!(recovered.native_budget(), original_budget);
        assert_eq!(checkpoint::encode(&recovered), original_rows);
    }
    let candidate = replay(&recovered, &creation, source, &store).unwrap();
    recovered.publish_native(candidate).unwrap();
    checkpoint::compare(&original, &recovered);
    // The same previously out-of-order record becomes admissible once its
    // exact predecessor exists. Its captured source range remains unchanged.
    let candidate = replay(&recovered, &posting, source, &store).unwrap();
    original.publish_native(posted).unwrap();
    recovered.publish_native(candidate).unwrap();
    checkpoint::compare(&original, &recovered);
}

#[test]
fn checksum_repaired_successor_corruption_keeps_the_predecessor_and_pinned_read_exact() {
    let directory = tempfile::tempdir().unwrap();
    let store = checkpoint::store(directory.path());
    let mut original = f::core();
    f::publish(&mut original, 10, f::creation(1, 1, &[], None));
    let mut recovered = restored(&original, 10_003, &store);
    let source = original.state.rows.id();
    let id = ClaimId::from_u128(1);
    let pinned = recovered.pin_native(0, 100).unwrap();
    let baseline = checkpoint::encode(&recovered);
    let budget = recovered.native_budget();
    let stats = recovered.native_stats();
    let posted = prepare_input(&original, 20, f::post(2, f::binding(1)));
    let valid = encode_mutation(&posted);
    for defect in 0..3 {
        let mut bytes = valid.clone();
        match defect {
            0 => {
                let at = body_offset(&bytes, Key::Meta);
                bytes[at..at + 8].copy_from_slice(&2u64.to_le_bytes());
            }
            1 => {
                let at = body_offset(&bytes, Key::Claim(id));
                // Subject is intrinsically valid, but this successor cannot
                // rewrite the recipient admitted by the original creation.
                bytes[at + 104..at + 120].copy_from_slice(&ParticipantId::from_u128(9999).0);
            }
            _ => {
                let at = body_offset(&bytes, Key::Claim(id));
                // Generated is a valid state tag, contradicting this record's
                // actual Posted transition and previous claim state.
                bytes[at + 128..at + 130].copy_from_slice(&1u16.to_le_bytes());
            }
        }
        checksum(&mut bytes);
        let record = inspect(&bytes);
        assert!(
            prepare(
                &recovered,
                &record,
                source,
                checkpoint::limits(recovered.limits),
                &store,
                &BuiltinNativeSchemas
            )
            .is_err(),
            "defect {defect}"
        );
        assert_eq!(recovered.native_budget(), budget);
        assert_eq!(recovered.native_stats(), stats);
        assert_eq!(checkpoint::encode(&recovered), baseline);
        assert_eq!(
            pinned.with_claim(id, 0, |claim| claim.status()).unwrap(),
            Some(ClaimStatus::Generated)
        );
        assert_eq!(
            pinned
                .recorded(inspect(&valid).header().outcome.invocation, 0)
                .unwrap(),
            None
        );
    }
    let candidate = replay(&recovered, &valid, source, &store).unwrap();
    original.publish_native(posted).unwrap();
    recovered.publish_native(candidate).unwrap();
    checkpoint::compare(&original, &recovered);
    assert_eq!(
        pinned.with_claim(id, 0, |claim| claim.status()).unwrap(),
        Some(ClaimStatus::Generated)
    );
    assert_eq!(
        recovered.native_claim(id).unwrap().status(),
        ClaimStatus::Posted
    );
    recovered.release_native(&pinned).unwrap();
}

#[test]
fn cumulative_work_memory_and_dropped_candidate_refund_before_a_valid_replay_retry() {
    let directory = tempfile::tempdir().unwrap();
    let store = checkpoint::store(directory.path());
    let mut original = f::core();
    f::publish(
        &mut original,
        10,
        f::creation(1, 1, &[(ValidationMode::Required, true)], None),
    );
    let mut recovered = restored(&original, 10_004, &store);
    let source = original.state.rows.id();
    let pinned = recovered.pin_native(0, 100).unwrap();
    let id = ClaimId::from_u128(1);
    let posted = prepare_input(&original, 20, f::post(2, f::binding(1)));
    let bytes = encode_mutation(&posted);
    let record = inspect(&bytes);
    let baseline = checkpoint::encode(&recovered);
    let budget = recovered.native_budget();
    let stats = recovered.native_stats();
    for domain in 0..4 {
        let mut limits = checkpoint::limits(recovered.limits);
        match domain {
            0 => limits.work.parsing = 0,
            1 => limits.work.source = 0,
            2 => limits.work.model = 0,
            _ => limits.work.lookup = 0,
        }
        assert!(
            prepare(
                &recovered,
                &record,
                source,
                limits,
                &store,
                &BuiltinNativeSchemas
            )
            .is_err(),
            "work domain {domain}"
        );
        assert_eq!(recovered.native_budget(), budget);
        assert_eq!(recovered.native_stats(), stats);
        assert_eq!(checkpoint::encode(&recovered), baseline);
    }
    let occupied = recovered
        .state
        .budget
        .reserve(
            BudgetKind::Recovery,
            BudgetLane::Completion,
            budget.limit - budget.used,
        )
        .unwrap();
    let pressured = recovered.native_budget();
    assert!(replay(&recovered, &bytes, source, &store).is_err());
    assert_eq!(recovered.native_budget(), pressured);
    assert_eq!(recovered.native_stats(), stats);
    drop(occupied);
    assert_eq!(recovered.native_budget(), budget);
    let prepared = replay(&recovered, &bytes, source, &store).unwrap();
    assert_eq!(recovered.native_sequence(), SessionSeq(1));
    assert_eq!(
        pinned.recorded(prepared.outcome().invocation, 0).unwrap(),
        None
    );
    assert!(recovered.native_budget().used > budget.used);
    drop(prepared);
    assert_eq!(recovered.native_budget(), budget);
    assert_eq!(checkpoint::encode(&recovered), baseline);
    let prepared = replay(&recovered, &bytes, source, &store).unwrap();
    original.publish_native(posted).unwrap();
    recovered.publish_native(prepared).unwrap();
    checkpoint::compare(&original, &recovered);
    assert_eq!(
        pinned.with_claim(id, 0, |claim| claim.status()).unwrap(),
        Some(ClaimStatus::Generated)
    );
    recovered.release_native(&pinned).unwrap();
}

#[test]
fn failed_testimony_replay_preserves_diagnostics_independent_checks_and_claimant_audit() {
    let (mut original, store, _directory) = super::super::evidence::tests::recovery_fixture(0);
    original.limits.plan_edges = 65_536;
    let mut recovered = restored(&original, 10_005, &store);
    for step in 0u128..5 {
        let claim = original
            .native_claim(ClaimId::from_u128(1))
            .unwrap()
            .binding();
        let expected = original
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
                    expected: original
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
        apply(
            &mut original,
            &mut recovered,
            300 + u64::try_from(step).unwrap(),
            input,
            &store,
        );
        let response = recovered
            .native_response(TestamentId::from_u128(900))
            .unwrap();
        assert_eq!(response.reported_outcome(), OutcomeKind::Failed);
        assert_eq!(response.diagnostics().len(), 2);
        assert!(
            recovered
                .native_artifact(ArtifactId::from_u128(802))
                .is_some()
        );
        assert!(
            recovered
                .native_artifact(ArtifactId::from_u128(803))
                .is_some()
        );
    }
    let response = recovered
        .native_response(TestamentId::from_u128(900))
        .unwrap();
    assert!(response.state().is_terminal());
    let result = recovered
        .native_result_testament(TestamentId::from_u128(950))
        .unwrap();
    assert_eq!(
        result.testament().state(),
        focal_model::lifecycle::audit::ResultTestamentState::Posted
    );
    assert!(!result.publications().is_empty());
    // The replayed root itself must remain a fully restorable checkpoint.
    let reopened = restored(&recovered, 10_006, &store);
    checkpoint::compare(&original, &reopened);
    checkpoint::compare(&recovered, &reopened);
}

#[test]
fn repaired_frame_cannot_change_outcome_counts_or_delete_an_existing_history_row() {
    let directory = tempfile::tempdir().unwrap();
    let store = checkpoint::store(directory.path());
    let mut original = f::core();
    f::publish(&mut original, 10, f::creation(1, 1, &[], None));
    let recovered = restored(&original, 10_007, &store);
    let source = original.state.rows.id();
    let posting = prepare_input(&original, 20, f::post(2, f::binding(1)));
    let valid = encode_mutation(&posting);
    let before = checkpoint::encode(&recovered);
    let budget = recovered.native_budget();
    let record = inspect(&valid);
    let mut changed_outcome = record.header().outcome;
    changed_outcome.changed += 1;
    let mut measure = bytes::CountingSink::new(usize::MAX, usize::MAX);
    fixed::outcome(&mut measure, changed_outcome).unwrap();
    let mut outcome = vec![0; measure.len()];
    let mut sink = bytes::SliceSink::new(&mut outcome, measure.visits_used());
    fixed::outcome(&mut sink, changed_outcome).unwrap();
    sink.finish().unwrap();
    let mut changed = valid.clone();
    // Header before Outcome: magic8 + version2 + profile1 + ledger32 + range16 + base8.
    changed[67..67 + outcome.len()].copy_from_slice(&outcome);
    let at = body_offset(&valid, Key::Outcome(changed_outcome.invocation));
    changed[at..at + outcome.len()].copy_from_slice(&outcome);
    checksum(&mut changed);
    assert_eq!(inspect(&changed).header().outcome, changed_outcome);
    assert!(replay(&recovered, &changed, source, &store).is_err());
    assert_eq!(recovered.native_budget(), budget);
    assert_eq!(checkpoint::encode(&recovered), before);

    let deleted = Key::Event(SessionSeq(1), 0);
    assert!(recovered.state.rows.get(&deleted).is_some());
    // Insert one sorted zero-length Delete frame while preserving every
    // original body, required accounting row and successor event ordinal.
    let insertion = record
        .rows(usize::MAX)
        .unwrap()
        .map(Result::unwrap)
        .find(|row| row.key > deleted)
        .map(|row| {
            let mut key_size = bytes::CountingSink::new(usize::MAX, usize::MAX);
            fixed::key(&mut key_size, row.key).unwrap();
            row.body().as_ptr() as usize - valid.as_ptr() as usize - 4 - key_size.len() - 1
        })
        .unwrap_or(valid.len() - 32);
    assert!(
        !record
            .rows(usize::MAX)
            .unwrap()
            .any(|row| row.unwrap().key == deleted)
    );
    let mut measure = bytes::CountingSink::new(usize::MAX, usize::MAX);
    bytes::write_u8(&mut measure, 0).unwrap();
    fixed::key(&mut measure, deleted).unwrap();
    bytes::write_count(&mut measure, 0).unwrap();
    let mut deletion = vec![0; measure.len()];
    let mut sink = bytes::SliceSink::new(&mut deletion, measure.visits_used());
    bytes::write_u8(&mut sink, 0).unwrap();
    fixed::key(&mut sink, deleted).unwrap();
    bytes::write_count(&mut sink, 0).unwrap();
    sink.finish().unwrap();
    let mut changed = valid.clone();
    changed.splice(insertion..insertion, deletion);
    let count_at = 67 + outcome.len();
    changed[count_at..count_at + 4].copy_from_slice(
        &u32::try_from(record.quote().rows + 1)
            .unwrap()
            .to_le_bytes(),
    );
    checksum(&mut changed);
    assert!(inspect(&changed).rows(usize::MAX).unwrap().any(|row| {
        let row = row.unwrap();
        row.key == deleted && row.deleted()
    }));
    assert!(replay(&recovered, &changed, source, &store).is_err());
    assert_eq!(recovered.native_budget(), budget);
    assert_eq!(checkpoint::encode(&recovered), before);
}
