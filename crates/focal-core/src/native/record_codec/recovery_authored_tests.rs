//! Recovery must preserve authored protocol bodies and original resolution facts,
//! including a successful identity reuse whose outcome produced no event rows.
use super::tests::{budget, compare, encode, limits, store};
use super::*;
use focal_evidence::BuiltinNativeSchemas;
use focal_model::lifecycle::{Principal, scope::ScopeLimits};
use focal_model::{ObjectId, ParticipantId, RequestEpoch, RequestId, ValidationId};
use std::ops::Range;

fn request(id: u128) -> RequestKey {
    RequestKey {
        principal: ParticipantId::from_u128(1),
        epoch: RequestEpoch(1),
        id: RequestId::from_u128(id),
    }
}
fn inspect(bytes: &[u8]) -> checkpoint::StructuralCheckpoint<'_> {
    checkpoint::StructuralCheckpoint::inspect(
        bytes,
        InspectionLimits {
            bytes: bytes.len(),
            visits: 100_000_000,
            rows: 100_000,
            row_bytes: 32 << 20,
        },
    )
    .unwrap()
}
fn body_range(bytes: &[u8], key: Key) -> Range<usize> {
    let checkpoint = inspect(bytes);
    let row = checkpoint
        .rows(100_000_000)
        .unwrap()
        .map(Result::unwrap)
        .find(|row| row.key == key)
        .unwrap();
    // Both pointers refer to the same borrowed allocation. Integer subtraction
    // obtains a test-only byte range without dereferencing an unchecked pointer.
    let start = (row.body().as_ptr() as usize)
        .checked_sub(bytes.as_ptr() as usize)
        .unwrap();
    start..start.checked_add(row.body().len()).unwrap()
}
fn checksum(bytes: &mut [u8]) {
    let at = bytes.len().checked_sub(32).unwrap();
    let (payload, trailer) = bytes.split_at_mut(at);
    let mut hash = blake3::Hasher::new_derive_key(checkpoint::HASH_DOMAIN);
    hash.update(payload);
    trailer.copy_from_slice(hash.finalize().as_bytes());
}
fn replace_body(bytes: &[u8], key: Key, replacement: &[u8]) -> Vec<u8> {
    let range = body_range(bytes, key);
    let mut value = bytes[..range.start.checked_sub(4).unwrap()].to_vec();
    value.extend_from_slice(&u32::try_from(replacement.len()).unwrap().to_le_bytes());
    value.extend_from_slice(replacement);
    value.extend_from_slice(&bytes[range.end..]);
    checksum(&mut value);
    value
}
fn encode_row(row: &Row, ledger: LedgerId) -> Vec<u8> {
    let mut count = CountingSink::new(usize::MAX, usize::MAX);
    rows::value(&mut count, row, ledger).unwrap();
    let mut value = vec![0; count.len()];
    let mut sink = SliceSink::new(&mut value, count.visits_used());
    rows::value(&mut sink, row, ledger).unwrap();
    sink.finish().unwrap();
    value
}

#[test]
fn authored_descriptors_identity_indices_and_zero_event_reuse_survive_full_recovery() {
    let core = crate::native::authored::recovery_fixture();
    let original_budget = core.native_budget();
    let bytes = encode(&core);
    let checkpoint = inspect(&bytes);
    let directory = tempfile::tempdir().unwrap();
    let content = store(directory.path());
    let memory = budget();
    let before = memory.stats().used;
    let restored = restore(
        &checkpoint,
        RangeId(31_405),
        limits(core.limits),
        memory.clone(),
        &content,
        &BuiltinNativeSchemas,
    )
    .unwrap();
    compare(&core, &restored);
    assert_eq!(
        restored.native_content_profile(),
        NativeContentProfile::AuthoredV1
    );
    assert_eq!(
        restored
            .native_claim(ClaimId::from_u128(1))
            .unwrap()
            .status(),
        ClaimStatus::Posted
    );
    for id in [1, 2] {
        let claim = ClaimId::from_u128(id);
        let validation = ValidationId::from_u128(id);
        let old_claim = core.native_claim_content(claim).unwrap();
        let new_claim = restored.native_claim_content(claim).unwrap();
        assert_eq!(new_claim, old_claim);
        assert!(!std::ptr::eq(
            new_claim.description().as_ptr(),
            old_claim.description().as_ptr()
        ));
        let old_validation = core.native_validation_descriptor(validation).unwrap();
        let new_validation = restored.native_validation_descriptor(validation).unwrap();
        assert_eq!(new_validation.binding(), old_validation.binding());
        assert_eq!(
            new_validation.specification_hash(),
            old_validation.specification_hash()
        );
        assert_eq!(new_validation.description(), old_validation.description());
        assert_eq!(new_validation.quality_bar(), old_validation.quality_bar());
        assert_eq!(
            new_validation.contributed_by(),
            old_validation.contributed_by()
        );
        assert_eq!(
            new_validation.policy_revision(),
            old_validation.policy_revision()
        );
        assert!(
            matches!(restored.state.rows.get(&Key::ClaimIdentity(new_claim.schema(), new_claim.content_hash())),
            Some(Row::ClaimIdentity(actual)) if *actual == claim)
        );
        assert!(
            matches!(restored.state.rows.get(&Key::DefinitionIdentity(new_validation.schema(), new_validation.content_hash())),
            Some(Row::DefinitionIdentity(actual)) if *actual == validation)
        );
    }
    for id in [1, 3, 4] {
        assert_eq!(
            restored
                .native_creation_result(request(id))
                .unwrap()
                .entries(),
            core.native_creation_result(request(id)).unwrap().entries()
        );
    }
    assert_eq!(
        restored
            .native_creation_result(request(1))
            .unwrap()
            .entries(),
        restored
            .native_creation_result(request(3))
            .unwrap()
            .entries()
    );
    let reused = restored.native_outcome(request(3)).unwrap();
    assert_eq!(
        (
            reused.created,
            reused.changed,
            reused.definitions,
            reused.events
        ),
        (0, 0, 0, 0)
    );
    assert!(restored.native_event(reused.sequence, 0).is_none());
    let claim = restored
        .native_claim_content(ClaimId::from_u128(1))
        .unwrap();
    let declaration = restored
        .native_validation_descriptor(ValidationId::from_u128(1))
        .unwrap();
    let retry = NativeInput {
        request: request(3),
        command: NativeCommand::CreateAuthored {
            claims: vec![NativeAuthoredProposal {
                content: claim.try_copy(claim.copy_charge().unwrap()).unwrap(),
                declarations: vec![
                    declaration
                        .try_copy(declaration.retained_bytes().unwrap())
                        .unwrap(),
                ],
                max_responses: 4,
                scope_limits: ScopeLimits {
                    scopes: 0,
                    roots: 0,
                    children: 0,
                },
                owner: None,
            }],
        },
    };
    assert!(
        matches!(restored.prepare_native(NativeContext { principal: Principal::Actor(request(3).principal), logical_time: 0 }, retry, &[]).unwrap(),
        NativePreparation::Existing { outcome, committed: true } if outcome == reused)
    );
    assert_eq!(core.native_budget(), original_budget);
    assert!(memory.stats().used > before);
    drop(restored);
    assert_eq!(memory.stats().used, before);
}

#[test]
fn altered_authored_body_profile_identity_and_creation_mapping_refuse_and_refund() {
    let core = crate::native::authored::recovery_fixture();
    let bytes = encode(&core);
    let directory = tempfile::tempdir().unwrap();
    let content = store(directory.path());
    let original_budget = core.native_budget();
    let descriptor = core
        .native_validation_descriptor(ValidationId::from_u128(1))
        .unwrap();
    let legacy = descriptor.declaration();
    let legacy_row = Row::Definition(
        OwnedDeclaration::new(legacy.try_copy(legacy.retained_bytes().unwrap()).unwrap()).unwrap(),
    );
    let mut mappings = core
        .native_creation_result(request(3))
        .unwrap()
        .entries()
        .to_vec();
    mappings.first_mut().unwrap().resolved = ObjectId::from_u128(2);
    let checks = NativeCreationResult::inspection_visits(mappings.len()).unwrap();
    let mapping_bytes = NativeCreationResult::construction_heap(mappings.len()).unwrap();
    let altered_mapping =
        NativeCreationResult::from_owned(mappings, 2, checks, mapping_bytes).unwrap();
    let mapping_row = Row::CreationResult(OwnedCreationResult::new(altered_mapping).unwrap());
    for defect in 0..5 {
        let changed = match defect {
            0 => {
                let key = Key::ClaimContent(ClaimId::from_u128(1));
                let range = body_range(&bytes, key);
                let mut body = bytes[range].to_vec();
                let description = core
                    .native_claim_content(ClaimId::from_u128(1))
                    .unwrap()
                    .description()
                    .as_bytes();
                let at = body
                    .windows(description.len())
                    .position(|value| value == description)
                    .unwrap();
                body[at] = b'X';
                replace_body(&bytes, key, &body)
            }
            1 => {
                let mut changed = bytes.clone();
                // Fixed frame: eight magic bytes, u16 version, profile.
                changed[10] = 0;
                checksum(&mut changed);
                changed
            }
            2 => replace_body(
                &bytes,
                Key::Definition(ValidationId::from_u128(1)),
                &encode_row(&legacy_row, core.state.ledger),
            ),
            3 => replace_body(
                &bytes,
                Key::ClaimIdentity(
                    1,
                    core.native_claim_content(ClaimId::from_u128(1))
                        .unwrap()
                        .content_hash(),
                ),
                &ClaimId::from_u128(2).0,
            ),
            _ => replace_body(
                &bytes,
                Key::CreationResult(request(3).into()),
                &encode_row(&mapping_row, core.state.ledger),
            ),
        };
        // The checksum/framing still pass; decoder/root validation must refuse
        // these intrinsically parseable but inconsistent retained object facts.
        let checkpoint = inspect(&changed);
        let memory = budget();
        let before = memory.stats().used;
        let result = restore(
            &checkpoint,
            RangeId(31_406),
            limits(core.limits),
            memory.clone(),
            &content,
            &BuiltinNativeSchemas,
        );
        assert!(result.is_err(), "authored corruption {defect} was accepted");
        drop(result);
        assert_eq!(
            memory.stats().used,
            before,
            "authored corruption {defect} leaked recovery funding"
        );
        assert_eq!(core.native_budget(), original_budget);
    }
}
