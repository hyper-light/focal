use super::*;
use crate::native::report_tests as fixture;
use focal_model::ValidationMode;

fn posted() -> Core<NativeState> {
    let mut core = fixture::core();
    core.limits.plan_edges = 64 * 1024;
    fixture::publish(
        &mut core,
        10,
        fixture::creation(1, 1, &[(ValidationMode::Required, false)], None),
    );
    fixture::publish(&mut core, 20, fixture::post(2, fixture::binding(1)));
    core
}

fn parts(core: &Core<NativeState>) -> (&ClaimState, &RegistrationSet) {
    let Row::Claim(owner) = core
        .state
        .rows
        .get(&Key::Claim(fixture::key(1).claim))
        .unwrap()
    else {
        panic!("actual native claim")
    };
    (owner.claim().unwrap(), owner.registrations().unwrap())
}

#[test]
fn authored_future_members_are_priced_before_their_rows_exist_and_sealed_cohort_releases_growth() {
    let core = posted();
    let before = core.native_budget();
    let (claim, registry) = parts(&core);
    assert_eq!(registry.rows().len(), 1);
    let future =
        crate::native::completion_envelope::cohort_bound(core.limits, claim, registry).unwrap();
    assert_eq!(future.claims(), 1);
    assert_eq!(future.evaluations(), 5); // Admission plus one Receipt per allowed response.
    assert_eq!(future.events(), 6);
    assert_eq!(future.changed_keys(), 11);
    let present = CohortBudget::empty()
        .add_claim(
            claim,
            registry,
            1,
            transactions::registry_heap(registry).unwrap(),
        )
        .unwrap();
    present.check_within(future).unwrap();
    assert!(future.check_within(present).is_err());
    assert!(future.construction_bytes().unwrap() > present.construction_bytes().unwrap());
    assert!(future.incoming_heap().unwrap() > present.incoming_heap().unwrap());
    assert!(future.writer_visits(1, 4).unwrap() > present.writer_visits(1, 4).unwrap());
    assert!(future.journal_visits(4, 1).unwrap() > present.journal_visits(4, 1).unwrap());
    assert_eq!(core.native_budget(), before);

    let mut sealed = registry.try_copy(registry.copy_charge().unwrap()).unwrap();
    sealed.seal_targets(claim).unwrap();
    let finished =
        crate::native::completion_envelope::cohort_bound(core.limits, claim, &sealed).unwrap();
    assert_eq!(finished, CohortBudget::empty());
    assert_eq!(finished.construction_bytes().unwrap(), 0);
    assert_eq!(finished.incoming_heap().unwrap(), 0);
    finished.check_within(future).unwrap();
}

#[test]
fn invalid_or_overflowing_quotes_refuse_without_changing_the_original_checked_profile() {
    let core = posted();
    let (claim, registry) = parts(&core);
    let heap = transactions::registry_heap(registry).unwrap();
    let original = CohortBudget::empty()
        .add_claim(claim, registry, registry.rows().len(), heap)
        .unwrap();
    let before = core.native_budget();
    assert!(original.add_claim(claim, registry, 0, heap).is_err());
    assert!(
        original
            .add_claim(claim, registry, registry.max_rows() + 1, heap)
            .is_err()
    );
    assert!(
        original
            .add_claim(claim, registry, registry.rows().len(), heap - 1)
            .is_err()
    );
    let unrestricted = RegistrationSet::new(claim, usize::MAX, usize::MAX).unwrap();
    assert!(
        original
            .add_claim(claim, &unrestricted, usize::MAX, 0)
            .is_err()
    );
    assert!(original.writer_visits(usize::MAX, 0).is_err());
    assert!(original.writer_visits(1, usize::MAX).is_err());
    assert!(original.journal_visits(usize::MAX, 1).is_err());
    assert!(original.journal_visits(4, usize::MAX).is_err());
    assert_eq!(original.claims(), 1);
    assert_eq!(original.evaluations(), 1);
    assert_eq!(core.native_budget(), before);
}

#[test]
fn source_guard_requires_complete_actual_pending_members_and_finite_semantic_work() {
    let core = fixture::core();
    let created = fixture::prepared(core.prepare_native(
        fixture::context(fixture::ISSUER, 10),
        fixture::creation(1, 1, &[(ValidationMode::Observe, false)], None),
        &[],
    ));
    let posted = fixture::prepared(core.prepare_native(
        fixture::context(fixture::ISSUER, 20),
        fixture::post(2, fixture::binding(1)),
        &[&created],
    ));
    let effective = View {
        state: &core.state,
        tail: Some(&posted),
    };
    let row = effective.owned_claim(fixture::key(1).claim).unwrap();
    let claim = row.claim().unwrap();
    let registry = row.registrations().unwrap();
    let before = core.native_budget();
    check_source(&effective, claim, registry, core.limits).unwrap();
    let committed = View {
        state: &core.state,
        tail: None,
    };
    assert!(check_source(&committed, claim, registry, core.limits).is_err());
    assert!(
        check_source(
            &effective,
            claim,
            registry,
            NativeLimits {
                plan_edges: 0,
                ..core.limits
            }
        )
        .is_err()
    );
    assert_eq!(core.native_budget(), before);
    assert_eq!(
        effective.evaluation(fixture::key(1)).unwrap().state(),
        validation::State::Ready
    );
}
