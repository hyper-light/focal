use super::*;
use focal_memory::{BudgetLane, Change, Entry};

fn corrupt(core: &mut Core<NativeState>, change: Change<Key, Row>) {
    let pending = core
        .state
        .rows
        .prepare_batch_with(
            core.native_sequence().0 + 1,
            vec![change],
            BudgetLane::Ordinary,
            crate::native::prepare::copy,
        )
        .unwrap();
    core.state.rows.publish(pending).unwrap();
}

#[test]
fn reconstruction_requires_actual_bodies_indices_and_authored_definition_variants() {
    for defect in 0..5 {
        let mut core = core();
        let supplied = proposal(1, 1);
        let content = supplied.content.content_hash();
        let definition_content = supplied.declarations[0].content_hash();
        publish(&mut core, create(1, vec![supplied]));
        super::super::check_storage(&core).unwrap();
        let lease = core.pin_native(0, 100).unwrap();
        let change = match defect {
            0 => Change::Delete(Key::ClaimContent(ClaimId::from_u128(1))),
            1 => Change::Delete(Key::ClaimIdentity(1, content)),
            2 => Change::Put(Entry::new(
                Key::DefinitionIdentity(1, definition_content),
                Row::DefinitionIdentity(ValidationId::from_u128(999)),
                0,
            )),
            3 => {
                let declaration = core.native_definition(ValidationId::from_u128(1)).unwrap();
                let owned = OwnedDeclaration::new(
                    declaration
                        .try_copy(declaration.retained_bytes().unwrap())
                        .unwrap(),
                )
                .unwrap();
                Change::Put(Entry::new(
                    Key::Definition(ValidationId::from_u128(1)),
                    Row::Definition(owned),
                    OwnedDeclaration::container_charge(),
                ))
            }
            _ => Change::Delete(Key::CreationResult(key(1).into())),
        };
        corrupt(&mut core, change);
        let before = core.native_budget();
        let error = NativeOwner::new(core).unwrap_err();
        let core = error.core;
        assert_eq!(core.native_budget(), before, "defect {defect}");
        assert_eq!(
            lease
                .with_authored_claim(ClaimId::from_u128(1), 0, |body, state| (
                    body.content_hash(),
                    state.status()
                ))
                .unwrap(),
            Some((content, ClaimStatus::Generated))
        );
    }
}

#[test]
fn pinned_authored_read_refuses_a_live_claim_with_missing_body() {
    let mut core = core();
    publish(&mut core, create(1, vec![proposal(1, 1)]));
    corrupt(
        &mut core,
        Change::Delete(Key::ClaimContent(ClaimId::from_u128(1))),
    );
    let lease = core.pin_native(0, 100).unwrap();
    assert!(
        lease
            .with_authored_claim(ClaimId::from_u128(1), 0, |_, _| ())
            .is_err()
    );
    assert_eq!(
        lease
            .with_authored_claim(ClaimId::from_u128(99), 0, |_, _| ())
            .unwrap(),
        None
    );
}

#[test]
fn reconstruction_does_not_promote_projection_only_state() {
    let mut core = core();
    publish(&mut core, create(1, vec![proposal(1, 1)]));
    core.state.profile = NativeContentProfile::ProjectionOnly;
    assert!(NativeOwner::new(core).is_err());
}

#[test]
fn retained_proof_detects_changed_original_owner_and_result_positions() {
    let core = core();
    let view = View {
        state: &core.state,
        tail: None,
    };
    let supplied = proposal(1, 1);
    let mut meta = view.meta();
    let mut extras = Extras::new(128, 1024 * 1024).unwrap();
    let mut scratch = Scratch {
        used: 0,
        max: 1024 * 1024,
    };
    let plan = super::super::prepare(
        vec![supplied],
        key(1),
        context(ISSUER),
        ClaimCut {
            position: SessionSeq(1),
            cause: ContentHash([1; 32]),
        },
        &view,
        limits(),
        &mut meta,
        &mut extras,
        &mut scratch,
    )
    .unwrap();
    let expected = super::super::retained_fingerprint(&extras).unwrap();
    let outcome = NativeOutcome {
        ledger: ledger(),
        invocation: key(1).into(),
        sequence: SessionSeq(1),
        logical_time: 0,
        operation: NativeOperation::Create,
        intent: ContentHash([1; 32]),
        created: 1,
        changed: 1,
        definitions: 1,
        evaluations: 0,
        artifacts: 0,
        results: 0,
        receipts: 0,
        responses: 0,
        result_testaments: 0,
        events: 2,
    };
    super::super::check_plan(&plan, &extras, &view, meta, outcome, limits()).unwrap();
    let content_index = extras
        .rows
        .iter()
        .position(|row| matches!(row.row, Row::ClaimContent(_)))
        .unwrap();
    let (descriptor, profile) = match &extras.rows[content_index].row {
        Row::ClaimContent(row) => (
            row.get()
                .unwrap()
                .try_copy(row.get().unwrap().copy_charge().unwrap())
                .unwrap(),
            row.profile().unwrap(),
        ),
        _ => unreachable!(),
    };
    let changed = OwnedClaimContent::new(
        descriptor,
        profile.max_responses,
        profile.scope_limits,
        Some(Owner {
            expected: binding_from(&extras),
            receipt: None,
        }),
    )
    .unwrap();
    let old = std::mem::replace(
        &mut extras.rows[content_index].row,
        Row::ClaimContent(changed),
    );
    assert_ne!(
        super::super::retained_fingerprint(&extras).unwrap(),
        expected
    );
    assert!(super::super::check_plan(&plan, &extras, &view, meta, outcome, limits()).is_err());
    extras.rows[content_index].row = old;
    assert_eq!(
        super::super::retained_fingerprint(&extras).unwrap(),
        expected
    );
    super::super::check_plan(&plan, &extras, &view, meta, outcome, limits()).unwrap();
    let result = extras
        .rows
        .iter_mut()
        .find(|row| matches!(row.row, Row::CreationResult(_)))
        .unwrap();
    let Row::CreationResult(old) = &result.row else {
        unreachable!()
    };
    let mut mappings = old.get().entries().to_vec();
    mappings.reverse();
    for (index, object) in mappings.iter_mut().enumerate() {
        object.ordinal = index as u32;
    }
    let replacement = NativeCreationResult::from_owned(
        mappings,
        2,
        3,
        NativeCreationResult::construction_heap(2).unwrap(),
    )
    .unwrap();
    result.row = Row::CreationResult(OwnedCreationResult::new(replacement).unwrap());
    assert_ne!(
        super::super::retained_fingerprint(&extras).unwrap(),
        expected
    );
    assert!(super::super::check_plan(&plan, &extras, &view, meta, outcome, limits()).is_err());
}

fn binding_from(extras: &Extras) -> Binding {
    extras
        .rows
        .iter()
        .find_map(|extra| match &extra.row {
            Row::ClaimContent(row) => row.get().map(ClaimDescriptor::binding),
            _ => None,
        })
        .unwrap()
}
