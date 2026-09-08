use super::*;
use aggregation::{RegistrationSet, RegistrationSnapshotSource, RegistrationValue};
use std::cell::Cell;

fn delivery<'a>(
    claim: &ClaimState,
    response: &Response,
    declaration: &'a v::Declaration,
) -> v::Evaluation<'a> {
    v::Evaluation::materialize(
        Principal::Actor(claim.issuer()),
        declaration,
        v::Materialization {
            binding: declaration.binding(),
            target: v::Target::Delivery {
                response: response.identity().binding,
            },
            slot_name: None,
            generation: u64::from(response.identity().cycle),
            receipt: Some(response.identity().receipt),
        },
    )
    .unwrap()
}
fn registrations<'a>(
    registry: &RegistrationSet,
    declaration: &'a v::Declaration,
    states: &[v::EvaluationState],
) -> Vec<RegistrationValue<'a>> {
    assert_eq!(registry.rows().len(), states.len());
    registry
        .member_snapshots_v1()
        .zip(states)
        .map(|(member, state)| RegistrationValue {
            member,
            declaration,
            evaluation: *state,
        })
        .collect()
}
fn restore_registry(
    claim: &ClaimState,
    registry: &RegistrationSet,
    rows: &[RegistrationValue<'_>],
) -> RegistrationSet {
    let plan = bytes::fail_after(0, || {
        RegistrationSet::prepare_hydration_v1(
            claim,
            registry.snapshot_v1(),
            rows,
            registry.max_rows(),
            usize::MAX,
        )
        .unwrap()
    });
    let charge = plan.construction_charge().unwrap();
    let visits = plan.build_visits().unwrap();
    let restored = plan.build(charge, visits).unwrap();
    assert_eq!(&restored, registry);
    restored.check(claim).unwrap();
    restored
}

#[test]
fn registration_current_frames_survive_adoption_and_preserve_original_cohort_seal() {
    let (mut claim, declarations) = fixture(9, None, false);
    receive(&mut claim);
    let old_response = response(&mut claim, 500, true);
    let mut registry = RegistrationSet::new(&claim, 8, usize::MAX).unwrap();
    let ready = delivery(&claim, &old_response, &declarations[0]);
    registry.register(&claim, &ready, usize::MAX).unwrap();
    let delivered = ready
        .receive_delivery(
            Principal::Actor(claim.issuer()),
            &ready.binding(),
            &ready.delivery_owner(&claim, &old_response, 1).unwrap(),
        )
        .unwrap()
        .next
        .into_state();
    let rows = registrations(&registry, &declarations[0], &[delivered]);
    restore_registry(&claim, &registry, &rows);
    registry.seal_increment_targets(&claim).unwrap();
    let adoption = claim
        .prepare_receipt_adoption(
            &claim.binding(),
            Principal::Actor(claim.issuer()),
            claim.receipt().unwrap().fence,
            ReceiptEntitlement {
                holder: crate::ParticipantId::from_u128(7),
                fence: ReceiptFence {
                    receipt: ReceiptId::from_u128(101),
                    epoch: 2,
                },
            },
            cut(8),
        )
        .unwrap();
    registry.adopt_receipt(&adoption).unwrap();
    let historical = delivered
        .adopt_receipt(&declarations[0], &adoption)
        .unwrap();
    assert_eq!(historical, delivered);
    let mut adopted = claim.try_copy(claim.copy_charge().unwrap()).unwrap();
    adopted.apply_receipt_adoption(&adoption).unwrap();
    claim = adopted;
    assert!(!registry.increment_targets_sealed());
    let new_response = response(&mut claim, 501, true);
    let next = delivery(&claim, &new_response, &declarations[0]);
    registry.register(&claim, &next, usize::MAX).unwrap();
    let rows = registrations(
        &registry,
        &declarations[0],
        &[historical, next.into_state()],
    );
    restore_registry(&claim, &registry, &rows);
    assert_ne!(rows[0].member.receipt, rows[1].member.receipt);
    claim
        .apply(
            &claim.binding(),
            Principal::Actor(claim.issuer()),
            ClaimIntent::Cancel { cut: cut(9) },
        )
        .unwrap();
    registry.seal_targets(&claim).unwrap();
    let sealed = next
        .into_state()
        .seal_claim(&declarations[0], &next.binding(), &claim)
        .unwrap()
        .next();
    let rows = registrations(&registry, &declarations[0], &[historical, sealed]);
    let restored = restore_registry(&claim, &registry, &rows);
    assert_eq!(
        restored.audit_targets(&claim).unwrap().sealed_at(),
        SessionSeq(9)
    );
    assert_eq!(historical.last_result(), delivered.last_result());
    assert_eq!(sealed.state(), v::State::Ready);
    assert!(sealed.sealed().is_some());
    let mut wrong_cut = registry.snapshot_v1();
    wrong_cut.sealed_at = Some(SessionSeq(10));
    assert!(matches!(
        RegistrationSet::prepare_hydration_v1(&claim, wrong_cut, rows.as_slice(), 8, usize::MAX),
        Err(ContractError::InvalidCut)
    ));
}

#[test]
fn registration_exact_quotes_refuse_without_allocating_and_legacy_unstamped_seal_stays_unstamped() {
    let (mut claim, declarations) = fixture(9, None, false);
    receive(&mut claim);
    let response = response(&mut claim, 500, true);
    let ready = delivery(&claim, &response, &declarations[0]);
    let mut registry = RegistrationSet::new(&claim, 8, usize::MAX).unwrap();
    registry.register(&claim, &ready, usize::MAX).unwrap();
    let rows = registrations(&registry, &declarations[0], &[ready.into_state()]);
    let prepare = || {
        RegistrationSet::prepare_hydration_v1(
            &claim,
            registry.snapshot_v1(),
            rows.as_slice(),
            8,
            usize::MAX,
        )
        .unwrap()
    };
    let plan = bytes::fail_after(0, prepare);
    let inspection = plan.inspection_visits();
    let charge = plan.construction_charge().unwrap();
    let visits = plan.build_visits().unwrap();
    assert!(matches!(
        RegistrationSet::prepare_hydration_v1(
            &claim,
            registry.snapshot_v1(),
            rows.as_slice(),
            8,
            inspection - 1
        ),
        Err(ContractError::Capacity)
    ));
    assert!(matches!(
        bytes::fail_after(0, || plan.build(charge, visits)),
        Err(ContractError::Capacity)
    ));
    for (max_bytes, max_visits) in [(charge - 1, visits), (charge, visits - 1)] {
        assert!(matches!(
            prepare().build(max_bytes, max_visits),
            Err(ContractError::Capacity)
        ));
    }
    restore_registry(&claim, &registry, &rows);
    registry.seal_targets(&claim).unwrap();
    assert!(registry.snapshot_v1().sealed_at.is_none());
    let restored = restore_registry(&claim, &registry, &rows);
    assert!(restored.is_sealed());
    assert!(restored.audit_targets(&claim).is_err());
}

struct ChangingRegistration<'a> {
    row: RegistrationValue<'a>,
    state: Cell<v::EvaluationState>,
}
impl RegistrationSnapshotSource for ChangingRegistration<'_> {
    type Rows<'a>
        = std::iter::Once<Result<RegistrationValue<'a>, ContractError>>
    where
        Self: 'a;
    fn rows(&self) -> Self::Rows<'_> {
        std::iter::once(Ok(RegistrationValue {
            evaluation: self.state.get(),
            ..self.row
        }))
    }
}
#[test]
fn registration_rejects_changed_current_frame_duplicates_and_substituted_declaration() {
    let (mut claim, declarations) = fixture(9, None, false);
    receive(&mut claim);
    let response = response(&mut claim, 500, true);
    let ready = delivery(&claim, &response, &declarations[0]);
    let mut registry = RegistrationSet::new(&claim, 8, usize::MAX).unwrap();
    registry.register(&claim, &ready, usize::MAX).unwrap();
    let delivered = ready
        .receive_delivery(
            Principal::Actor(claim.issuer()),
            &ready.binding(),
            &ready.delivery_owner(&claim, &response, 1).unwrap(),
        )
        .unwrap()
        .next
        .into_state();
    let rows = registrations(&registry, &declarations[0], &[ready.into_state()]);
    let source = ChangingRegistration {
        row: rows[0],
        state: Cell::new(ready.into_state()),
    };
    let plan = RegistrationSet::prepare_hydration_v1(
        &claim,
        registry.snapshot_v1(),
        &source,
        8,
        usize::MAX,
    )
    .unwrap();
    let charge = plan.construction_charge().unwrap();
    let visits = plan.build_visits().unwrap();
    source.state.set(delivered);
    assert!(matches!(
        plan.build(charge, visits),
        Err(ContractError::ContentConflict)
    ));
    let plan = RegistrationSet::prepare_hydration_v1(
        &claim,
        registry.snapshot_v1(),
        &source,
        8,
        usize::MAX,
    )
    .unwrap();
    assert_eq!(plan.build(charge, visits).unwrap(), registry);
    let duplicate = [rows[0], rows[0]];
    let fields = aggregation::RegistrationSnapshotV1 {
        rows: 2,
        ..registry.snapshot_v1()
    };
    assert!(matches!(
        RegistrationSet::prepare_hydration_v1(&claim, fields, duplicate.as_slice(), 8, usize::MAX),
        Err(ContractError::StaleEvaluation)
    ));
    let (_, foreign) = fixture(10, None, false);
    let bad = [RegistrationValue {
        declaration: &foreign[0],
        ..rows[0]
    }];
    assert!(
        RegistrationSet::prepare_hydration_v1(
            &claim,
            registry.snapshot_v1(),
            bad.as_slice(),
            8,
            usize::MAX
        )
        .is_err()
    );
    let bad = [RegistrationValue {
        member: aggregation::RegistrationMemberSnapshotV1 {
            generation: 2,
            ..rows[0].member
        },
        ..rows[0]
    }];
    assert!(
        RegistrationSet::prepare_hydration_v1(
            &claim,
            registry.snapshot_v1(),
            bad.as_slice(),
            8,
            usize::MAX
        )
        .is_err()
    );
    restore_registry(&claim, &registry, &rows);
}

struct ChangingScope<'a> {
    scope: &'a scope::Scope,
    target: &'a Cell<ClaimId>,
}
impl scope::ScopeSnapshotSource for ChangingScope<'_> {
    fn fields(&self) -> scope::ScopeSnapshotV1 {
        scope::ScopeSnapshotSource::fields(&self.scope)
    }
    type Roots<'a>
        = std::iter::Once<Result<WaitPredicate, ContractError>>
    where
        Self: 'a;
    fn roots(&self) -> Self::Roots<'_> {
        std::iter::once(Ok(WaitPredicate::Terminal(self.target.get())))
    }
}
struct ChangingScopes<'a> {
    registry: &'a scope::Registry,
    target: Cell<ClaimId>,
}
impl scope::RegistrySnapshotSource for ChangingScopes<'_> {
    fn fields(&self) -> scope::RegistrySnapshotV1 {
        scope::RegistrySnapshotSource::fields(&self.registry.snapshot_v1())
    }
    type Scope<'a>
        = ChangingScope<'a>
    where
        Self: 'a;
    type Scopes<'a>
        = std::iter::Once<Result<ChangingScope<'a>, ContractError>>
    where
        Self: 'a;
    type Children<'a>
        = std::iter::Empty<Result<scope::OwnedChildSnapshotV1, ContractError>>
    where
        Self: 'a;
    fn scopes(&self) -> Self::Scopes<'_> {
        std::iter::once(Ok(ChangingScope {
            scope: self.registry.iter().next().unwrap(),
            target: &self.target,
        }))
    }
    fn children(&self) -> Self::Children<'_> {
        std::iter::empty()
    }
}
#[test]
fn scope_changed_root_refuses_after_quoted_build_and_original_source_retries() {
    let (mut claim, _) = fixture(9, None, false);
    let (peer, _) = fixture(10, None, false);
    monitor(&mut claim, &peer, 2);
    let source = ChangingScopes {
        registry: claim.scopes(),
        target: Cell::new(ClaimId::from_u128(10)),
    };
    let prepare = || {
        scope::Registry::prepare_hydration_v1(
            claim.binding(),
            claim.created(),
            claim.scopes().limits(),
            &source,
            usize::MAX,
        )
        .unwrap()
    };
    let plan = bytes::fail_after(0, prepare);
    let charge = plan.construction_charge().unwrap();
    let visits = plan.build_visits().unwrap();
    source.target.set(ClaimId::from_u128(11));
    assert!(matches!(
        plan.build(charge, visits),
        Err(ContractError::ContentConflict)
    ));
    source.target.set(ClaimId::from_u128(10));
    assert_eq!(prepare().build(charge, visits).unwrap(), *claim.scopes());
    assert!(matches!(
        scope::Registry::prepare_hydration_v1(
            Binding {
                object: ObjectId::from_u128(80),
                ..claim.binding()
            },
            claim.created(),
            claim.scopes().limits(),
            &source,
            usize::MAX
        ),
        Err(ContractError::WrongObject)
    ));
}
