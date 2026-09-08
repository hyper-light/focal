use super::*;

fn replacement() -> ReceiptEntitlement {
    ReceiptEntitlement {
        holder: OTHER,
        fence: fence(2),
    }
}

#[test]
fn checked_adoption_changes_only_entitlement_and_revision_on_a_charged_copy() {
    let source = acknowledged();
    let original = source.clone();
    let adoption = source
        .prepare_receipt_adoption(
            &source.binding(),
            Principal::Actor(ISSUER),
            fence(1),
            replacement(),
            cut(8),
        )
        .unwrap();
    assert_eq!(adoption.previous(), source.receipt().unwrap());
    assert_eq!(adoption.binding(), source.binding());
    assert_eq!(adoption.next_binding(), source.binding().next().unwrap());
    let mut changed = source.try_copy(source.copy_charge().unwrap()).unwrap();
    changed.apply_receipt_adoption(&adoption).unwrap();
    let mut legacy = source.clone();
    apply(
        &mut legacy,
        Principal::Actor(ISSUER),
        ClaimIntent::AdoptReceipt {
            previous: fence(1),
            replacement: replacement(),
        },
    )
    .unwrap();
    assert_eq!(changed, legacy);
    assert_eq!(source, original);
    assert_eq!(changed.response_count(), source.response_count());
    assert_eq!(changed.latest_response(), source.latest_response());
    assert_eq!(changed.status(), source.status());
    assert_eq!(changed.binding().content, source.binding().content);
    assert_eq!(
        changed.apply_receipt_adoption(&adoption),
        Err(ContractError::StaleRevision)
    );
}

#[test]
fn issuer_source_epoch_and_publication_guards_refuse_before_any_mutation() {
    let source = received();
    let original = source.clone();
    for principal in [
        Principal::Actor(SUBJECT),
        Principal::Actor(OTHER),
        Principal::Node(ISSUER),
    ] {
        assert!(matches!(
            source.prepare_receipt_adoption(
                &source.binding(),
                principal,
                fence(1),
                replacement(),
                cut(8),
            ),
            Err(ContractError::WrongActor)
        ));
    }
    for (expected, previous, replacement, publication) in [
        (
            source.binding().next().unwrap(),
            fence(1),
            replacement(),
            cut(8),
        ),
        (source.binding(), fence(2), replacement(), cut(8)),
        (
            source.binding(),
            fence(1),
            ReceiptEntitlement {
                holder: OTHER,
                fence: fence(3),
            },
            cut(8),
        ),
        (
            source.binding(),
            fence(1),
            ReceiptEntitlement {
                holder: ParticipantId::from_u128(0),
                ..replacement()
            },
            cut(8),
        ),
        (source.binding(), fence(1), replacement(), cut(0)),
        (
            source.binding(),
            fence(1),
            replacement(),
            ClaimCut {
                cause: ContentHash([0; 32]),
                ..cut(8)
            },
        ),
    ] {
        assert!(
            source
                .prepare_receipt_adoption(
                    &expected,
                    Principal::Actor(ISSUER),
                    previous,
                    replacement,
                    publication
                )
                .is_err()
        );
        assert_eq!(source, original);
    }
    let mut later = source.clone();
    later.created = SessionSeq(9);
    assert!(matches!(
        later.prepare_receipt_adoption(
            &later.binding(),
            Principal::Actor(ISSUER),
            fence(1),
            replacement(),
            cut(8)
        ),
        Err(ContractError::InvalidCut)
    ));
    let adoption = source
        .prepare_receipt_adoption(
            &source.binding(),
            Principal::Actor(ISSUER),
            fence(1),
            replacement(),
            cut(8),
        )
        .unwrap();
    let mut impostor = source.clone();
    impostor.max_responses += 1;
    let unchanged = impostor.clone();
    assert_eq!(
        impostor.apply_receipt_adoption(&adoption),
        Err(ContractError::ContentConflict)
    );
    assert_eq!(impostor, unchanged);
    let mut exhausted = source.clone();
    exhausted.binding.revision = ObjectRevision(u64::MAX);
    assert!(matches!(
        exhausted.prepare_receipt_adoption(
            &exhausted.binding(),
            Principal::Actor(ISSUER),
            fence(1),
            replacement(),
            cut(8)
        ),
        Err(ContractError::Capacity)
    ));
}

#[test]
fn native_exact_epoch_does_not_change_legacy_monotonic_epoch_contract() {
    let mut source = received();
    let jumped = ReceiptEntitlement {
        holder: OTHER,
        fence: fence(7),
    };
    assert!(matches!(
        source.prepare_receipt_adoption(
            &source.binding(),
            Principal::Actor(ISSUER),
            fence(1),
            jumped,
            cut(8)
        ),
        Err(ContractError::StaleReceipt)
    ));
    apply(
        &mut source,
        Principal::Actor(ISSUER),
        ClaimIntent::AdoptReceipt {
            previous: fence(1),
            replacement: jumped,
        },
    )
    .unwrap();
    assert_eq!(source.receipt(), Some(jumped));
}
