use super::*;
use crate::{ObjectRef, RootCommandId, SessionId, TenantId, TimerId, ValidationId};

const REQUIREMENTS: [RequirementRef; 1] = [RequirementRef {
    id: ValidationId(8u128.to_be_bytes()),
    specification: ContentHash([9; 32]),
}];

fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(1),
        session: SessionId::from_u128(2),
    }
}
fn limits() -> Limits {
    Limits {
        description_bytes: 1024,
        relations: 32,
        scopes: 16,
        scope_key_bytes: 128,
        requirements: 16,
        slots: 16,
        checks: 16,
        construction_bytes: 32 * 1024,
    }
}
fn relations() -> Vec<Relation> {
    vec![
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
            kind: RelationKind::CausedBy,
            target: RelationTarget::Root(RootCommandId::from_u128(7)),
        },
    ]
}
fn spec(relations: &[Relation]) -> ClaimSpec<'_> {
    ClaimSpec {
        ledger: ledger(),
        id: ClaimId::from_u128(3),
        schema: 1,
        occurrence: OccurrenceId::from_u128(4),
        description: "work",
        relations,
        scopes: &[ScopeSpec {
            kind: ScopeKind::File,
            key: "src",
        }],
        requirements: &REQUIREMENTS,
        slots: &[],
        deadline: Some(Deadline {
            timer: TimerId::from_u128(10),
            generation: 11,
            at: 12,
        }),
    }
}
fn build(spec: ClaimSpec<'_>) -> ClaimDescriptor {
    let plan = bytes::fail_after(0, || ClaimDescriptor::prepare(spec, limits()).unwrap());
    let content = plan.content_hash();
    let intent = plan.intent_fingerprint();
    let charge = plan.construction_charge();
    let built = plan.build(charge).unwrap();
    assert_eq!(built.content_hash(), content);
    assert_eq!(built.intent_fingerprint(), intent);
    assert_eq!(built.retained_bytes().unwrap(), charge);
    built
}

// Fixed native preimage for spec(), independent of the production field writer.
// It pins lengths, widths, order, option and enum codes without changing V1.
const PREIMAGE: &str = concat!(
    "0000000000000000000000000000001000000000000000000000000000000001000000000000000000000000000000100000",
    "0000000000000000000000000002000000000000000000000000000000020001000000000000000000000000000000100000",
    "000000000000000000000000000400000000000000000000000000000004776f726b00000000000000000000000000000010",
    "0000000000000000000000000000000400000000000000000000000000000002000100000000000000000000000000000002",
    "0001000000000000000000000000000000100000000000000000000000000000000500000000000000000000000000000002",
    "0002000000000000000000000000000000020001000000000000000000000000000000100000000000000000000000000000",
    "0006000000000000000000000000000000020004000000000000000000000000000000020003000000000000000000000000",
    "0000000200010000000000000000000000000000000200080000000000000000000000000000000200040000000000000000",
    "0000000000000010000000000000000000000000000000070000000000000000000000000000001000000000000000000000",
    "0000000000010000000000000000000000000000000200010000000000000000000000000000000373726300000000000000",
    "0000000000000000100000000000000000000000000000000100000000000000000000000000000010000000000000000000",
    "0000000000000800000000000000000000000000000020090909090909090909090909090909090909090909090909090909",
    "0909090909000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000",
    "0000020001000000000000000000000000000000100000000000000000000000000000000a00000000000000000000000000",
    "000008000000000000000b00000000000000000000000000000008000000000000000c",
);

#[test]
fn native_identity_matches_fixed_vector_excludes_own_id_and_distinguishes_occurrence() {
    let relations = relations();
    let original = build(spec(&relations));
    let preimage: Vec<_> = PREIMAGE
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect();
    let expected = ContentHash(blake3::derive_key(
        "focal/native/claim-content/1",
        &preimage,
    ));
    assert_eq!(original.content_hash(), expected);
    let mut intent = blake3::Hasher::new_derive_key("focal/native/claim-intent/1");
    intent.update(&ClaimId::from_u128(3).0);
    intent.update(&expected.0);
    assert_eq!(
        original.intent_fingerprint().0,
        *intent.finalize().as_bytes()
    );
    let moved_address = build(ClaimSpec {
        id: ClaimId::from_u128(30),
        ..spec(&relations)
    });
    assert_eq!(moved_address.content_hash(), expected);
    assert_ne!(
        moved_address.intent_fingerprint(),
        original.intent_fingerprint()
    );
    let other_occurrence = build(ClaimSpec {
        occurrence: OccurrenceId::from_u128(30),
        ..spec(&relations)
    });
    assert_ne!(other_occurrence.content_hash(), expected);
    assert_eq!(original.binding().revision, ObjectRevision(1));
    assert_eq!(original.binding().object, ObjectId(original.id().0));
    assert_eq!(original.binding().content, expected);
}

#[test]
fn owned_content_preserves_all_supported_relations_scopes_and_ordered_requirements() {
    let original = {
        let description = String::from("Inspect the complete authored claim\n");
        let mut relations = relations();
        for kind in [
            RelationKind::Supersedes,
            RelationKind::DependsOn,
            RelationKind::Awaits,
            RelationKind::Refines,
            RelationKind::ConflictsWith,
            RelationKind::DerivedFrom,
            RelationKind::Reviews,
            RelationKind::Amends,
        ] {
            relations.push(Relation {
                kind,
                target: RelationTarget::Object(ObjectRef::claim(ledger(), ClaimId::from_u128(30))),
            });
        }
        relations.sort();
        let keys: Vec<_> = ScopeKind::ALL.iter().map(|_| String::from("a/é")).collect();
        let scopes: Vec<_> = ScopeKind::ALL
            .iter()
            .zip(&keys)
            .map(|(kind, key)| ScopeSpec { kind: *kind, key })
            .collect();
        let requirements = [
            RequirementRef {
                id: ValidationId::from_u128(31),
                specification: ContentHash([32; 32]),
            },
            spec(&relations).requirements[0],
        ];
        let descriptor = build(ClaimSpec {
            description: &description,
            scopes: &scopes,
            requirements: &requirements,
            ..spec(&relations)
        });
        assert_eq!(descriptor.description(), description);
        assert_ne!(descriptor.description().as_ptr(), description.as_ptr());
        assert_eq!(descriptor.relations(), relations);
        assert_ne!(descriptor.relations().as_ptr(), relations.as_ptr());
        assert_eq!(descriptor.scopes().collect::<Vec<_>>(), scopes);
        assert_eq!(descriptor.requirements(), requirements);
        assert_ne!(descriptor.requirements().as_ptr(), requirements.as_ptr());
        descriptor
    };
    assert_eq!(original.ledger(), ledger());
    assert_eq!(original.schema(), 1);
    assert_eq!(original.occurrence(), OccurrenceId::from_u128(4));
    assert_eq!(original.issuer(), ParticipantId::from_u128(5));
    assert_eq!(original.subject(), ParticipantId::from_u128(6));
    assert_eq!(original.action(), ActionType::Work);
    assert_eq!(original.cause(), &Cause::Root(RootCommandId::from_u128(7)));
    let copy = original.try_copy(original.copy_charge().unwrap()).unwrap();
    assert_eq!(copy, original);
    let identity = original.content_hash();
    drop(original);
    assert_eq!(copy.content_hash(), identity);
    assert_eq!(copy.description(), "Inspect the complete authored claim\n");
    assert_eq!(copy.scopes().len(), 6);
    assert_eq!(copy.requirements()[0].id, ValidationId::from_u128(31));
}

#[test]
fn every_authored_dimension_changes_identity_without_normalizing_text_or_order() {
    let relations = relations();
    let baseline = build(spec(&relations));
    let mut other_parties = relations.clone();
    other_parties[0].target = RelationTarget::Participant(ParticipantId::from_u128(31));
    let mut other_cause = relations.clone();
    other_cause[3].target =
        RelationTarget::Object(ObjectRef::claim(ledger(), ClaimId::from_u128(31)));
    let scopes = [ScopeSpec {
        kind: ScopeKind::Symbol,
        key: "src",
    }];
    let requirements = [RequirementRef {
        id: ValidationId::from_u128(8),
        specification: ContentHash([31; 32]),
    }];
    for changed in [
        ClaimSpec {
            ledger: LedgerId {
                tenant: TenantId::from_u128(31),
                ..ledger()
            },
            ..spec(&relations)
        },
        ClaimSpec {
            ledger: LedgerId {
                session: SessionId::from_u128(31),
                ..ledger()
            },
            ..spec(&relations)
        },
        ClaimSpec {
            description: "work\n",
            ..spec(&relations)
        },
        ClaimSpec {
            relations: &other_parties,
            ..spec(&relations)
        },
        ClaimSpec {
            relations: &other_cause,
            ..spec(&relations)
        },
        ClaimSpec {
            scopes: &scopes,
            ..spec(&relations)
        },
        ClaimSpec {
            scopes: &[ScopeSpec {
                kind: ScopeKind::File,
                key: "other",
            }],
            ..spec(&relations)
        },
        ClaimSpec {
            requirements: &requirements,
            ..spec(&relations)
        },
        ClaimSpec {
            deadline: None,
            ..spec(&relations)
        },
        ClaimSpec {
            deadline: Some(Deadline {
                timer: TimerId::from_u128(31),
                generation: 11,
                at: 12,
            }),
            ..spec(&relations)
        },
        ClaimSpec {
            deadline: Some(Deadline {
                timer: TimerId::from_u128(10),
                generation: 31,
                at: 12,
            }),
            ..spec(&relations)
        },
        ClaimSpec {
            deadline: Some(Deadline {
                timer: TimerId::from_u128(10),
                generation: 11,
                at: 31,
            }),
            ..spec(&relations)
        },
    ] {
        let changed = build(changed);
        assert_ne!(changed.content_hash(), baseline.content_hash());
        assert_ne!(changed.intent_fingerprint(), baseline.intent_fingerprint());
    }
    let mut requirements = [
        spec(&relations).requirements[0],
        RequirementRef {
            id: ValidationId::from_u128(32),
            specification: ContentHash([33; 32]),
        },
    ];
    let ordered = build(ClaimSpec {
        requirements: &requirements,
        ..spec(&relations)
    });
    requirements.reverse();
    let reversed = build(ClaimSpec {
        requirements: &requirements,
        ..spec(&relations)
    });
    assert_ne!(ordered.content_hash(), reversed.content_hash());
    assert_eq!(reversed.requirements(), requirements);
}

#[test]
fn self_targeted_drafts_remain_representable_and_only_handoff_passes_local_post_rule() {
    let mut seen = Vec::new();
    for action in ActionType::ALL {
        let mut relations = relations();
        relations[1].target = RelationTarget::Participant(ParticipantId::from_u128(5));
        relations[2].target = RelationTarget::Action(*action);
        let plan = ClaimDescriptor::prepare(spec(&relations), limits()).unwrap();
        let expected = if *action == ActionType::Handoff {
            Ok(())
        } else {
            Err(ContractError::InvalidPolicy)
        };
        assert_eq!(plan.check_postable(), expected);
        let charge = plan.construction_charge();
        let descriptor = plan.build(charge).unwrap();
        assert_eq!(descriptor.check_postable(), expected);
        assert!(!seen.contains(&descriptor.content_hash()));
        seen.push(descriptor.content_hash());
    }
    // A local descriptor cannot prove a Receipt declaration or handoff authority.
    let relations = relations();
    let draft = build(ClaimSpec {
        scopes: &[],
        requirements: &[],
        deadline: None,
        ..spec(&relations)
    });
    assert!(draft.check_postable().is_ok());
    assert!(draft.requirements().is_empty());
}

#[test]
fn malformed_local_fields_and_unsupported_relations_refuse_before_allocation() {
    let source = relations();
    fn refuse(spec: ClaimSpec<'_>) {
        bytes::fail_after(0, || {
            assert!(ClaimDescriptor::prepare(spec, limits()).is_err());
            assert_eq!(bytes::remaining_allocations(), Some(0));
        })
    }
    for spec in [
        ClaimSpec {
            id: ClaimId::from_u128(0),
            ..spec(&source)
        },
        ClaimSpec {
            occurrence: OccurrenceId::from_u128(0),
            ..spec(&source)
        },
        ClaimSpec {
            schema: 2,
            ..spec(&source)
        },
        ClaimSpec {
            ledger: LedgerId::default(),
            ..spec(&source)
        },
        ClaimSpec {
            description: " \n",
            ..spec(&source)
        },
        ClaimSpec {
            description: "bad\0text",
            ..spec(&source)
        },
        ClaimSpec {
            scopes: &[ScopeSpec {
                kind: ScopeKind::File,
                key: " src",
            }],
            ..spec(&source)
        },
        ClaimSpec {
            scopes: &[ScopeSpec {
                kind: ScopeKind::File,
                key: "src\0",
            }],
            ..spec(&source)
        },
        ClaimSpec {
            requirements: &[RequirementRef {
                id: ValidationId::from_u128(0),
                specification: ContentHash([9; 32]),
            }],
            ..spec(&source)
        },
        ClaimSpec {
            requirements: &[RequirementRef {
                id: ValidationId::from_u128(8),
                specification: ContentHash([0; 32]),
            }],
            ..spec(&source)
        },
        ClaimSpec {
            deadline: Some(Deadline {
                timer: TimerId::from_u128(0),
                generation: 1,
                at: 1,
            }),
            ..spec(&source)
        },
        ClaimSpec {
            deadline: Some(Deadline {
                timer: TimerId::from_u128(10),
                generation: 0,
                at: 1,
            }),
            ..spec(&source)
        },
    ] {
        refuse(spec);
    }
    for index in 0..source.len() {
        let mut missing = source.clone();
        missing.remove(index);
        refuse(spec(&missing));
        let mut duplicate = source.clone();
        duplicate.push(source[index].clone());
        duplicate.sort();
        refuse(spec(&duplicate));
    }
    let mut unordered = source.clone();
    unordered.reverse();
    refuse(spec(&unordered));
    let duplicate_requirements = [spec(&source).requirements[0], spec(&source).requirements[0]];
    refuse(ClaimSpec {
        requirements: &duplicate_requirements,
        ..spec(&source)
    });
    let duplicate_scopes = [spec(&source).scopes[0], spec(&source).scopes[0]];
    refuse(ClaimSpec {
        scopes: &duplicate_scopes,
        ..spec(&source)
    });
    for relation in [
        Relation {
            kind: RelationKind::Issuer,
            target: RelationTarget::Participant(ParticipantId::from_u128(31)),
        },
        Relation {
            kind: RelationKind::Evaluator,
            target: RelationTarget::Participant(ParticipantId::from_u128(31)),
        },
        Relation {
            kind: RelationKind::ContributedBy,
            target: RelationTarget::Participant(ParticipantId::from_u128(31)),
        },
        Relation {
            kind: RelationKind::Invalidates,
            target: RelationTarget::Object(ObjectRef::claim(ledger(), ClaimId::from_u128(31))),
        },
        Relation {
            kind: RelationKind::DependsOn,
            target: RelationTarget::Participant(ParticipantId::from_u128(31)),
        },
        Relation {
            kind: RelationKind::DependsOn,
            target: RelationTarget::Object(ObjectRef {
                ledger: ledger(),
                kind: ObjectKind::Artifact,
                id: ObjectId::from_u128(31),
            }),
        },
        Relation {
            kind: RelationKind::DependsOn,
            target: RelationTarget::Object(ObjectRef::claim(
                LedgerId::default(),
                ClaimId::from_u128(31),
            )),
        },
        Relation {
            kind: RelationKind::DependsOn,
            target: RelationTarget::Object(ObjectRef::claim(ledger(), ClaimId::from_u128(0))),
        },
        Relation {
            kind: RelationKind::Supersedes,
            target: RelationTarget::Object(ObjectRef::claim(ledger(), ClaimId::from_u128(3))),
        },
        Relation {
            kind: RelationKind::Amends,
            target: RelationTarget::Object(ObjectRef::claim(ledger(), ClaimId::from_u128(3))),
        },
        Relation {
            kind: RelationKind::CausedBy,
            target: RelationTarget::Object(ObjectRef::claim(ledger(), ClaimId::from_u128(3))),
        },
    ] {
        let mut changed = source.clone();
        changed.push(relation);
        changed.sort();
        refuse(spec(&changed));
    }
}

#[test]
fn exact_quotes_refuse_every_partial_allocation_and_copy_without_changing_source() {
    let relations = relations();
    let source = spec(&relations);
    let plan = ClaimDescriptor::prepare(source, limits()).unwrap();
    let charge = plan.construction_charge();
    let allocations = plan.construction_heap_allocations();
    assert_eq!(allocations, 5);
    let original = plan.build(charge).unwrap();
    assert_eq!(original.heap_allocations().unwrap(), allocations);
    assert_eq!(original.retained_bytes().unwrap(), charge);
    for bound in [
        Limits {
            description_bytes: 3,
            ..limits()
        },
        Limits {
            relations: 3,
            ..limits()
        },
        Limits {
            scopes: 0,
            ..limits()
        },
        Limits {
            scope_key_bytes: 2,
            ..limits()
        },
        Limits {
            requirements: 0,
            ..limits()
        },
        Limits {
            construction_bytes: charge - 1,
            ..limits()
        },
    ] {
        assert!(matches!(
            bytes::fail_after(0, || ClaimDescriptor::prepare(source, bound)),
            Err(ContractError::Capacity)
        ));
    }
    for at in 0..allocations {
        assert!(matches!(
            bytes::fail_after(at, || {
                ClaimDescriptor::prepare(source, limits())
                    .unwrap()
                    .build(charge)
            }),
            Err(ContractError::Capacity)
        ));
        assert!(matches!(
            bytes::fail_after(at, || original.try_copy(charge)),
            Err(ContractError::Capacity)
        ));
        assert_eq!(
            ClaimDescriptor::prepare(source, limits())
                .unwrap()
                .content_hash(),
            original.content_hash()
        );
    }
    assert!(matches!(
        bytes::fail_after(0, || {
            ClaimDescriptor::prepare(source, limits())
                .unwrap()
                .build(charge - 1)
        }),
        Err(ContractError::Capacity)
    ));
    assert!(matches!(
        bytes::fail_after(0, || original.try_copy(charge - 1)),
        Err(ContractError::Capacity)
    ));
    let copied = bytes::fail_after(allocations, || original.try_copy(charge)).unwrap();
    assert_eq!(copied, original);
}

#[test]
fn compact_copy_prices_actual_spare_capacities_and_preserves_identity() {
    let relations = relations();
    let mut original = build(ClaimSpec {
        requirements: &[],
        ..spec(&relations)
    });
    original.description.reserve_exact(32);
    original.relations.reserve_exact(8);
    original.scopes.reserve_exact(4);
    original.scopes[0].key.reserve_exact(16);
    original.requirements.reserve_exact(4);
    assert!(original.retained_heap_bytes().unwrap() > original.copy_heap_bytes().unwrap());
    assert_eq!(original.heap_allocations().unwrap(), 5);
    assert_eq!(original.copy_heap_allocations().unwrap(), 4);
    let copied = original.try_copy(original.copy_charge().unwrap()).unwrap();
    assert_eq!(copied, original);
    assert_eq!(
        copied.retained_heap_bytes().unwrap(),
        copied.copy_heap_bytes().unwrap()
    );
    assert_eq!(copied.heap_allocations().unwrap(), 4);
}

#[test]
fn complete_slot_contract_is_owned_hashed_bounded_and_preserves_zero_check_slots() {
    let relations = relations();
    let checks = [CheckPolicy {
        declaration_index: 2,
        validation: ValidationId::from_u128(8),
        mode: ValidationMode::Required,
    }];
    let slots = [
        SlotPolicy {
            slot: 0,
            missing_declaration_index: 1,
            mode: ValidationMode::Required,
            checks: &checks,
        },
        SlotPolicy {
            slot: 1,
            missing_declaration_index: 3,
            mode: ValidationMode::Observe,
            checks: &[],
        },
    ];
    let source = ClaimSpec {
        slots: &slots,
        ..spec(&relations)
    };
    let plan = ClaimDescriptor::prepare(source, limits()).unwrap();
    let count = plan.construction_heap_allocations();
    let charge = plan.construction_charge();
    assert_eq!(count, 7);
    let descriptor = plan.build(charge).unwrap();
    assert_eq!(descriptor.heap_allocations().unwrap(), count);
    let retained = descriptor.slots().collect::<Vec<_>>();
    assert_eq!(retained.len(), 2);
    assert_eq!(retained[0].slot, 0);
    assert_eq!(retained[0].missing_declaration_index, 1);
    assert_eq!(retained[0].mode, ValidationMode::Required);
    assert_eq!(retained[0].checks, checks);
    assert_ne!(retained[0].checks.as_ptr(), checks.as_ptr());
    assert_eq!(retained[1].missing_declaration_index, 3);
    assert_eq!(retained[1].mode, ValidationMode::Observe);
    assert!(retained[1].checks.is_empty());
    for changed in [
        SlotPolicy {
            slot: 2,
            ..slots[0]
        },
        SlotPolicy {
            missing_declaration_index: 4,
            ..slots[0]
        },
        SlotPolicy {
            mode: ValidationMode::Observe,
            ..slots[0]
        },
        SlotPolicy {
            checks: &[],
            ..slots[0]
        },
    ] {
        let changed_slots = [changed];
        let changed = build(ClaimSpec {
            slots: &changed_slots,
            ..spec(&relations)
        });
        let one = build(ClaimSpec {
            slots: &slots[..1],
            ..spec(&relations)
        });
        assert_ne!(changed.content_hash(), one.content_hash());
    }
    let other_checks = [CheckPolicy {
        mode: ValidationMode::Observe,
        ..checks[0]
    }];
    let other_slots = [
        SlotPolicy {
            checks: &other_checks,
            ..slots[0]
        },
        slots[1],
    ];
    assert_ne!(
        build(ClaimSpec {
            slots: &other_slots,
            ..spec(&relations)
        })
        .content_hash(),
        descriptor.content_hash()
    );
    assert_ne!(
        build(ClaimSpec {
            slots: &slots[..1],
            ..spec(&relations)
        })
        .content_hash(),
        descriptor.content_hash()
    );
    for at in 0..count {
        assert!(matches!(
            bytes::fail_after(at, || ClaimDescriptor::prepare(source, limits())
                .unwrap()
                .build(charge)),
            Err(ContractError::Capacity)
        ));
        assert!(matches!(
            bytes::fail_after(at, || descriptor.try_copy(charge)),
            Err(ContractError::Capacity)
        ));
    }
    assert_eq!(descriptor.try_copy(charge).unwrap(), descriptor);
    for bound in [
        Limits {
            slots: 1,
            ..limits()
        },
        Limits {
            checks: 0,
            ..limits()
        },
    ] {
        assert!(matches!(
            bytes::fail_after(0, || ClaimDescriptor::prepare(source, bound)),
            Err(ContractError::Capacity)
        ));
    }
    let absent = [CheckPolicy {
        validation: ValidationId::from_u128(99),
        ..checks[0]
    }];
    let collision = [CheckPolicy {
        declaration_index: 1,
        ..checks[0]
    }];
    let duplicate = [checks[0], checks[0]];
    for checks in [&absent[..], &collision[..], &duplicate[..]] {
        let invalid = [SlotPolicy { checks, ..slots[0] }];
        assert!(
            bytes::fail_after(0, || ClaimDescriptor::prepare(
                ClaimSpec {
                    slots: &invalid,
                    ..spec(&relations)
                },
                limits()
            ))
            .is_err()
        );
    }
    for invalid in [
        [slots[1], slots[0]],
        [slots[0], slots[0]],
        [
            slots[0],
            SlotPolicy {
                missing_declaration_index: 1,
                ..slots[1]
            },
        ],
    ] {
        assert!(
            bytes::fail_after(0, || ClaimDescriptor::prepare(
                ClaimSpec {
                    slots: &invalid,
                    ..spec(&relations)
                },
                limits()
            ))
            .is_err()
        );
    }
}
