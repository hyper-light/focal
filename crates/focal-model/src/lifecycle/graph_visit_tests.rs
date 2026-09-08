use super::*;

const GENEROUS: usize = 1_000_000;

fn capture_and_query(
    claims: &[&ClaimState],
    visits: &mut VisitBudget,
) -> Result<ContentHash, ContractError> {
    let plan = Snapshot::prepare_capture_with_visits(claims, limits(), usize::MAX, visits)?;
    let graph = plan.build_with_visits(visits)?;
    let source = claims.first().ok_or(ContractError::InvalidTarget)?;
    let witness = graph
        .deadlock_query_with_visits(
            ClaimId(source.binding().object.0),
            source.deadline().ok_or(ContractError::InvalidCut)?,
            100,
            SessionSeq(10),
            graph.deadlock_charge()?,
            visits,
        )?
        .ok_or(ContractError::InvalidTransition)?;
    Ok(witness.fingerprint)
}

#[test]
fn repeated_capture_and_scc_spend_one_shared_allowance_without_renewing_it() {
    let (a, b) = cycle();
    let claims = [&a, &b];
    let old = Snapshot::capture(&claims, limits()).unwrap();
    let expected = old
        .deadlock(cid(1), a.deadline().unwrap(), 100)
        .unwrap()
        .fingerprint;
    let mut measured = VisitBudget::new(GENEROUS);
    assert_eq!(capture_and_query(&claims, &mut measured).unwrap(), expected);
    let cost = GENEROUS - measured.remaining();
    assert!(cost > 0);

    let mut shared = VisitBudget::new(2 * cost + 7);
    shared.charge(7).unwrap();
    assert_eq!(capture_and_query(&claims, &mut shared).unwrap(), expected);
    assert_eq!(shared.remaining(), cost);
    assert_eq!(capture_and_query(&claims, &mut shared).unwrap(), expected);
    assert_eq!(shared.remaining(), 0);
    assert_eq!(
        capture_and_query(&claims, &mut shared),
        Err(ContractError::Capacity)
    );
    assert_eq!(shared.remaining(), 0);
    old.check_owner(&a, &[&b]).unwrap();
}

#[test]
fn failed_build_and_query_keep_all_preflight_and_partial_traversal_debits() {
    let (a, b) = cycle();
    let claims = [&a, &b];
    let original = [a.binding(), b.binding()];
    let mut measured = VisitBudget::new(GENEROUS);
    let plan = Snapshot::prepare_capture_with_visits(&claims, limits(), usize::MAX, &mut measured)
        .unwrap();
    let prepared_cost = GENEROUS - measured.remaining();
    let before_build = measured.remaining();
    let graph = plan.build_with_visits(&mut measured).unwrap();
    let build_cost = before_build - measured.remaining();
    assert!(prepared_cost > 0 && build_cost > 0);
    let mut tight = VisitBudget::new(prepared_cost + build_cost - 1);
    let plan =
        Snapshot::prepare_capture_with_visits(&claims, limits(), usize::MAX, &mut tight).unwrap();
    assert_eq!(tight.remaining(), build_cost - 1);
    assert!(matches!(
        plan.build_with_visits(&mut tight),
        Err(ContractError::Capacity)
    ));
    assert_eq!(tight.remaining(), 0);

    let mut measured = VisitBudget::new(GENEROUS);
    let witness = graph
        .deadlock_query_with_visits(
            cid(1),
            a.deadline().unwrap(),
            100,
            SessionSeq(10),
            usize::MAX,
            &mut measured,
        )
        .unwrap()
        .unwrap();
    assert_eq!(witness.victim().unwrap(), b.binding());
    let query_cost = GENEROUS - measured.remaining();
    assert!(query_cost > 0);
    let mut tight = VisitBudget::new(query_cost + 5);
    tight.charge(6).unwrap();
    assert!(matches!(
        graph.deadlock_query_with_visits(
            cid(1),
            a.deadline().unwrap(),
            100,
            SessionSeq(10),
            usize::MAX,
            &mut tight
        ),
        Err(ContractError::Capacity)
    ));
    // The final two-member fingerprint debit is refused before hashing; the
    // one remaining visit stays available rather than being saturated to zero.
    assert_eq!(tight.remaining(), 1);
    assert_eq!([a.binding(), b.binding()], original);
    graph.check_owner(&a, &[&b]).unwrap();
}

#[test]
fn invalid_inputs_byte_refusals_and_excess_debits_neither_renew_nor_saturate_visits() {
    let (a, b) = cycle();
    let graph = Snapshot::capture(&[&a, &b], limits()).unwrap();
    let mut shared = VisitBudget::new(50);
    shared.charge(7).unwrap();
    let remaining = shared.remaining();
    assert_eq!(shared.charge(remaining + 1), Err(ContractError::Capacity));
    assert_eq!(shared.remaining(), remaining);
    assert_eq!(shared.charge(usize::MAX), Err(ContractError::Capacity));
    assert_eq!(shared.remaining(), remaining);
    shared.charge(0).unwrap();
    assert_eq!(shared.remaining(), remaining);

    let wrong = Deadline {
        generation: 99,
        ..a.deadline().unwrap()
    };
    assert!(matches!(
        graph.deadlock_query_with_visits(
            cid(1),
            wrong,
            100,
            SessionSeq(10),
            usize::MAX,
            &mut shared
        ),
        Err(ContractError::InvalidCut)
    ));
    assert_eq!(shared.remaining(), remaining);
    assert!(matches!(
        graph.deadlock_query_with_visits(
            cid(1),
            a.deadline().unwrap(),
            99,
            SessionSeq(10),
            usize::MAX,
            &mut shared
        ),
        Err(ContractError::InvalidCut)
    ));
    assert_eq!(shared.remaining(), remaining);
    assert!(matches!(
        graph.deadlock_query_with_visits(
            cid(1),
            a.deadline().unwrap(),
            100,
            SessionSeq(10),
            graph.deadlock_charge().unwrap() - 1,
            &mut shared
        ),
        Err(ContractError::Capacity)
    ));
    assert_eq!(shared.remaining(), remaining);

    let malformed = [&a, &a];
    assert!(matches!(
        Snapshot::prepare_capture_with_visits(&malformed, limits(), usize::MAX, &mut shared),
        Err(ContractError::InvalidManifest)
    ));
    assert!(shared.remaining() < remaining);
    let after_failure = shared.remaining();
    assert_eq!(
        shared.charge(after_failure + 1),
        Err(ContractError::Capacity)
    );
    assert_eq!(shared.remaining(), after_failure);
    graph.check_owner(&a, &[&b]).unwrap();
}

#[test]
fn dependency_success_and_negative_queries_share_the_same_remaining_transaction_visits() {
    let dependent = claim(1, 1, &[(Kind::DependsOn, 2)]);
    let mut failed = claim(2, 2, &[]);
    failed
        .apply(
            &failed.binding(),
            Principal::Actor(failed.issuer()),
            claim::ClaimIntent::Cancel {
                cut: claim::ClaimCut {
                    position: SessionSeq(20),
                    cause: ContentHash([9; 32]),
                },
            },
        )
        .unwrap();
    let waiting = claim(3, 3, &[(Kind::Awaits, 2)]);
    let graph = Snapshot::capture(&[&dependent, &failed, &waiting], limits()).unwrap();
    let expected = graph.dependency_failure(cid(1)).unwrap();
    let original = graph.bindings().collect::<Vec<_>>();
    let mut measured = VisitBudget::new(GENEROUS);
    let result = graph
        .dependency_failure_with_visits(cid(1), usize::MAX, &mut measured)
        .unwrap();
    assert_eq!(result.origin(), expected.origin());
    assert_eq!(
        result.path().collect::<Vec<_>>(),
        expected.path().collect::<Vec<_>>()
    );
    assert_eq!(result.fingerprint, expected.fingerprint);
    let success_cost = GENEROUS - measured.remaining();
    let before_negative = measured.remaining();
    assert!(matches!(
        graph.dependency_failure_with_visits(cid(3), usize::MAX, &mut measured),
        Err(ContractError::InvalidTransition)
    ));
    let negative_cost = before_negative - measured.remaining();
    assert!(success_cost > 0 && negative_cost > 0);

    let mut shared = VisitBudget::new(success_cost + negative_cost);
    assert!(
        graph
            .dependency_failure_with_visits(
                cid(1),
                graph.dependency_failure_charge().unwrap(),
                &mut shared
            )
            .is_ok()
    );
    assert_eq!(shared.remaining(), negative_cost);
    assert!(matches!(
        graph.dependency_failure_with_visits(cid(3), usize::MAX, &mut shared),
        Err(ContractError::InvalidTransition)
    ));
    assert_eq!(shared.remaining(), 0);
    assert!(matches!(
        graph.dependency_failure_with_visits(cid(1), usize::MAX, &mut shared),
        Err(ContractError::Capacity)
    ));
    assert_eq!(shared.remaining(), 0);
    assert_eq!(graph.bindings().collect::<Vec<_>>(), original);
    graph.check_owner(&dependent, &[&failed, &waiting]).unwrap();
}
