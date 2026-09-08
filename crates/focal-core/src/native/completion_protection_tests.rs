use super::*;
use crate::native::report_tests as fixture;

fn source() -> MemoryBudget {
    MemoryBudget::new(16 * 1024 * 1024, 0).unwrap()
}
fn claims(count: u128) -> Core<NativeState> {
    let mut core = fixture::core();
    for id in 1..=count {
        fixture::publish(&mut core, id as u64, fixture::creation(id, id, &[], None));
    }
    core
}
fn members(source: &MemoryBudget, core: &Core<NativeState>, ids: &[u128]) -> GraphMembers {
    let claims: Vec<_> = ids
        .iter()
        .map(|id| core.native_claim(ClaimId::from_u128(*id)).unwrap())
        .collect();
    GraphMembers::copy_from(source, &claims).unwrap()
}
fn key(claim: u128, generation: u64) -> EvaluationKey {
    EvaluationKey {
        claim: ClaimId::from_u128(claim),
        validation: ValidationId::from_u128(900),
        target: EvaluationTarget::Work {
            response: TestamentId::from_u128(901),
            slot: 0,
            artifact: ArtifactId::from_u128(902),
        },
        generation,
    }
}
fn affected(index: &Protections, claim: u128) -> Vec<EvaluationKey> {
    index.affected(ClaimId::from_u128(claim)).collect()
}

#[test]
fn protected_membership_preserves_full_evaluation_identity_and_exact_intervals() {
    let core = claims(4);
    let source = source();
    let a = members(&source, &core, &[1, 2, 3]);
    let b = members(&source, &core, &[2, 3]);
    let mut index = Protections::new();
    for (key, members) in [(key(1, 1), &a), (key(2, 1), &b), (key(2, 2), &b)] {
        drop(index.install(&source, key, members, 32).unwrap());
    }
    assert_eq!(affected(&index, 1), vec![key(1, 1)]);
    assert_eq!(affected(&index, 2), vec![key(1, 1), key(2, 1), key(2, 2)]);
    assert_eq!(affected(&index, 3), affected(&index, 2));
    assert!(affected(&index, 4).is_empty());
    index.remove(key(2, 1), &b).unwrap();
    assert_eq!(affected(&index, 2), vec![key(1, 1), key(2, 2)]);
    index.index.validate().unwrap();
    drop((index, a, b));
    assert_eq!(source.stats().used, 0);
}

#[test]
fn tail_growth_rollback_preserves_an_older_published_retirement() {
    let core = claims(7);
    let source = source();
    let first = members(&source, &core, &[1, 2, 3]);
    let second = members(&source, &core, &[2, 3, 4, 5, 6, 7]);
    let mut index = Protections::new();
    drop(index.install(&source, key(1, 1), &first, 32).unwrap());
    let capacity = index.index.capacity();
    let later = index.install(&source, key(2, 1), &second, 32).unwrap();
    index.remove(key(1, 1), &first).unwrap();
    index.rollback(later).unwrap();
    assert_eq!(index.index.capacity(), capacity);
    assert_eq!(index.index.len(), 0);
    for id in 1..=7 {
        assert!(affected(&index, id).is_empty());
    }
    index.index.validate().unwrap();
}

#[test]
fn allocation_failure_after_partial_index_insertion_refunds_every_private_change() {
    let core = claims(4);
    let source = source();
    let members = members(&source, &core, &[1, 2, 3, 4]);
    let mut index = Protections::new();
    let allowed = array::<Added>(4).unwrap()
        + CompletionIndex::<(), Key>::slot_charge(1).unwrap()
        + CompletionIndex::<(), Key>::slot_charge(2).unwrap()
        + CompletionIndex::<(), Key>::slot_charge(4).unwrap()
        - 1;
    let pressure = source
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Ordinary,
            source.reservation_limit(BudgetLane::Ordinary) - source.stats().used - allowed,
        )
        .unwrap()
        .commit();
    let before = source.stats();
    assert!(index.install(&source, key(1, 1), &members, 32).is_err());
    assert_eq!(source.stats(), before);
    assert_eq!(index.index.len(), 0);
    assert_eq!(index.index.capacity(), 0);
    index.index.validate().unwrap();
    drop(pressure);
    drop(index.install(&source, key(1, 1), &members, 32).unwrap());
    assert_eq!(affected(&index, 4), vec![key(1, 1)]);
}

#[test]
fn duplicate_later_member_rolls_back_only_the_new_prefix() {
    let core = claims(3);
    let source = source();
    let old = members(&source, &core, &[2, 3]);
    let candidate = members(&source, &core, &[1, 2, 3]);
    let mut index = Protections::new();
    drop(index.install(&source, key(3, 1), &old, 16).unwrap());
    let before = source.stats();
    let capacity = index.index.capacity();
    assert!(index.install(&source, key(3, 1), &candidate, 16).is_err());
    assert_eq!(source.stats(), before);
    assert_eq!(index.index.capacity(), capacity);
    assert!(affected(&index, 1).is_empty());
    assert_eq!(affected(&index, 2), vec![key(3, 1)]);
    assert_eq!(affected(&index, 3), vec![key(3, 1)]);
}

#[test]
fn member_copy_and_missing_grant_root_refuse_without_retained_accounting() {
    let core = claims(3);
    let source = source();
    let a = core.native_claim(ClaimId::from_u128(1)).unwrap();
    let b = core.native_claim(ClaimId::from_u128(2)).unwrap();
    for invalid in [vec![], vec![a, a], vec![b, a]] {
        let before = source.stats();
        assert!(GraphMembers::copy_from(&source, &invalid).is_err());
        assert_eq!(source.stats(), before);
    }
    let set = GraphMembers::copy_from(&source, &[a, b]).unwrap();
    let mut index = Protections::new();
    let before = source.stats();
    assert!(index.install(&source, key(3, 1), &set, 16).is_err());
    assert_eq!(source.stats(), before);
    assert_eq!(index.index.len(), 0);
}
