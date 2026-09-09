use super::super::bytes::{CountingSink, Cursor, SliceSink};
use super::*;
use focal_model::lifecycle::{
    aggregation::CheckPolicy,
    claim_descriptor::{ClaimSpec, Limits, ScopeSpec},
};
use focal_model::{
    ClaimId, ContentHash, ObjectId, OccurrenceId, ParticipantId, Relation, RequirementRef,
    RootCommandId, SessionId, TenantId, TimerId, ValidationId, ValidationMode,
};

#[test]
fn claim_body_preserves_relations_pins_slots_and_optional_deadline_without_derived_fields() {
    let ledger = LedgerId {
        tenant: TenantId::from_u128(1),
        session: SessionId::from_u128(2),
    };
    let relations = [
        Relation {
            kind: RelationKind::Issuer,
            target: RelationTarget::Participant(ParticipantId::from_u128(5)),
        },
        Relation {
            kind: RelationKind::Subject,
            target: RelationTarget::Participant(ParticipantId::from_u128(6)),
        },
        Relation {
            kind: RelationKind::ClaimAction,
            target: RelationTarget::Action(ActionType::Work),
        },
        Relation {
            kind: RelationKind::DependsOn,
            target: RelationTarget::Object(ObjectRef::claim(ledger, ClaimId::from_u128(9))),
        },
        Relation {
            kind: RelationKind::CausedBy,
            target: RelationTarget::Root(RootCommandId::from_u128(7)),
        },
    ];
    let scopes = [
        ScopeSpec {
            kind: ScopeKind::File,
            key: "src/é",
        },
        ScopeSpec {
            kind: ScopeKind::UxSurface,
            key: "settings-panel",
        },
    ];
    // Requirement order is authored; it need not match IDs or slot check order.
    let requirements = [
        RequirementRef {
            id: ValidationId::from_u128(32),
            specification: ContentHash([42; 32]),
        },
        RequirementRef {
            id: ValidationId::from_u128(31),
            specification: ContentHash([41; 32]),
        },
    ];
    let checks = [
        CheckPolicy {
            declaration_index: 10,
            validation: ValidationId::from_u128(31),
            mode: ValidationMode::Observe,
        },
        CheckPolicy {
            declaration_index: 11,
            validation: ValidationId::from_u128(32),
            mode: ValidationMode::Required,
        },
    ];
    let slots = [
        SlotPolicy {
            slot: 7,
            missing_declaration_index: 8,
            mode: ValidationMode::Required,
            checks: &checks,
        },
        SlotPolicy {
            slot: 9,
            missing_declaration_index: 12,
            mode: ValidationMode::Observe,
            checks: &[],
        },
    ];
    let deadline = Deadline {
        timer: TimerId::from_u128(100),
        generation: 0x0102030405060708,
        at: 0x0807060504030201,
    };
    let mut absent_length = None;
    for deadline in [None, Some(deadline)] {
        let plan = ClaimDescriptor::prepare(
            ClaimSpec {
                ledger,
                id: ClaimId::from_u128(3),
                schema: 1,
                occurrence: OccurrenceId::from_u128(4),
                description: "Inspect café.\n",
                relations: &relations,
                scopes: &scopes,
                requirements: &requirements,
                slots: &slots,
                deadline,
                policy: None,
            },
            Limits {
                description_bytes: 128,
                relations: 8,
                scopes: 4,
                scope_key_bytes: 32,
                requirements: 4,
                slots: 4,
                checks: 4,
                construction_bytes: 8192,
            },
        )
        .unwrap();
        let charge = plan.construction_charge();
        let value = plan.build(charge).unwrap();
        let content_hash = value.content_hash();
        let mut count = CountingSink::new(usize::MAX, usize::MAX);
        claim(&mut count, &value).unwrap();
        let mut output = vec![0xcc; count.len()];
        let mut writer = SliceSink::new(&mut output, count.visits_used());
        claim(&mut writer, &value).unwrap();
        writer.finish().unwrap();

        let mut cursor = Cursor::new(&output, output.len(), usize::MAX).unwrap();
        assert_eq!(cursor.fixed::<16>().unwrap(), ledger.tenant.0);
        assert_eq!(cursor.fixed::<16>().unwrap(), ledger.session.0);
        assert_eq!(cursor.fixed::<16>().unwrap(), ClaimId::from_u128(3).0);
        assert_eq!(cursor.u16().unwrap(), 1);
        assert_eq!(cursor.fixed::<16>().unwrap(), OccurrenceId::from_u128(4).0);
        assert_eq!(cursor.text(128).unwrap(), "Inspect café.\n");
        assert_eq!(cursor.count(8).unwrap(), 5);
        for (kind, participant) in [(1, 5u128), (2, 6)] {
            assert_eq!(cursor.u16().unwrap(), kind);
            assert_eq!(cursor.u8().unwrap(), 0);
            assert_eq!(
                cursor.fixed::<16>().unwrap(),
                ParticipantId::from_u128(participant).0
            );
        }
        assert_eq!(cursor.u16().unwrap(), 4); // ClaimAction.
        assert_eq!(cursor.u8().unwrap(), 2); // Action target.
        assert_eq!(cursor.u16().unwrap(), 1); // Work.
        assert_eq!(cursor.u16().unwrap(), 6); // DependsOn.
        assert_eq!(cursor.u8().unwrap(), 1); // Object target.
        assert_eq!(cursor.fixed::<16>().unwrap(), ledger.tenant.0);
        assert_eq!(cursor.fixed::<16>().unwrap(), ledger.session.0);
        assert_eq!(cursor.u16().unwrap(), 1); // Claim object family.
        assert_eq!(cursor.fixed::<16>().unwrap(), ObjectId::from_u128(9).0);
        assert_eq!(cursor.u16().unwrap(), 8); // CausedBy.
        assert_eq!(cursor.u8().unwrap(), 3); // Root target.
        assert_eq!(cursor.fixed::<16>().unwrap(), RootCommandId::from_u128(7).0);
        assert_eq!(cursor.count(4).unwrap(), 2);
        for (kind, key) in [(1, "src/é"), (6, "settings-panel")] {
            assert_eq!(cursor.u16().unwrap(), kind);
            assert_eq!(cursor.text(32).unwrap(), key);
        }
        assert_eq!(cursor.count(4).unwrap(), 2);
        for (id, specification) in [(32u128, [42; 32]), (31, [41; 32])] {
            assert_eq!(cursor.fixed::<16>().unwrap(), ValidationId::from_u128(id).0);
            assert_eq!(cursor.fixed::<32>().unwrap(), specification);
        }
        assert_eq!(cursor.count(4).unwrap(), 2);
        assert_eq!(cursor.u32().unwrap(), 7);
        assert_eq!(cursor.u32().unwrap(), 8);
        assert_eq!(cursor.u8().unwrap(), 0); // Required presence.
        assert_eq!(cursor.count(4).unwrap(), 2);
        for (index, validation, mode) in [(10, 31u128, 1), (11, 32, 0)] {
            assert_eq!(cursor.u32().unwrap(), index);
            assert_eq!(
                cursor.fixed::<16>().unwrap(),
                ValidationId::from_u128(validation).0
            );
            assert_eq!(cursor.u8().unwrap(), mode);
        }
        assert_eq!(cursor.u32().unwrap(), 9);
        assert_eq!(cursor.u32().unwrap(), 12);
        assert_eq!(cursor.u8().unwrap(), 1); // Observe presence.
        assert_eq!(cursor.count(4).unwrap(), 0); // No checks still preserves the slot.
        assert_eq!(cursor.u8().unwrap(), u8::from(deadline.is_some()));
        if let Some(deadline) = deadline {
            assert_eq!(cursor.fixed::<16>().unwrap(), deadline.timer.0);
            assert_eq!(cursor.u64().unwrap(), deadline.generation);
            assert_eq!(cursor.u64().unwrap(), deadline.at);
            assert_eq!(output.len(), absent_length.unwrap() + 32);
        } else {
            absent_length = Some(output.len());
        }
        // No cached content hash, binding revision or derived role copy follows.
        assert_eq!(cursor.offset(), output.len());
        cursor.finish().unwrap();
        assert_eq!(value.content_hash(), content_hash);
        let mut short_bytes = CountingSink::new(count.len() - 1, count.visits_used());
        assert_eq!(claim(&mut short_bytes, &value), Err(Error::Capacity));
        let mut short_visits = CountingSink::new(count.len(), count.visits_used() - 1);
        assert_eq!(claim(&mut short_visits, &value), Err(Error::Capacity));
    }
}
