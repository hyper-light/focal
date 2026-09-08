use super::*;
use crate::lifecycle::memory as bytes;

// Every retained buffer comes from checked child/monitor transitions. The
// released monitor still owns its original roots after its target leaves the
// required graph closure.
fn released_child_and_monitor() -> (ClaimState, ClaimState) {
    let mut parent = claim(1);
    let definition = child_definition(&parent, 2, 2);
    let mut child = parent
        .generate_child(
            &parent.binding(),
            Principal::Actor(ISSUER),
            None,
            definition,
            cut(2),
        )
        .unwrap();
    let mut target = claim(3);
    cancel(&mut target, 2);
    let graph = snapshot(&[&parent, &child, &target]);
    let roots = [WaitPredicate::Terminal(ClaimId::from_u128(3))];
    let transition =
        Registry::prepare_register(&parent, authority(&parent, 3), registration(&roots), &graph)
            .unwrap();
    parent
        .apply_scope(&parent.binding(), transition, &[&child, &target])
        .unwrap();
    let graph = snapshot(&[&parent, &child, &target]);
    let transition =
        Registry::prepare_release_monitor(&parent, MonitorId::from_u128(1), &graph, cut(4))
            .unwrap();
    parent
        .apply_scope(&parent.binding(), transition, &[&child, &target])
        .unwrap();
    cancel_owned(&mut parent, &mut child, 5);
    let graph = snapshot(&[&child]);
    let transition = Registry::prepare_release_owner(&child, &graph, &[], cut(6)).unwrap();
    child
        .apply_scope(&child.binding(), transition, &[])
        .unwrap();
    (parent, child)
}

#[test]
fn release_preflight_is_allocation_free_and_exact_byte_and_visit_bounds_cover_build() {
    let (owner, child) = released_child_and_monitor();
    let graph = snapshot(&[&owner, &child]);
    let peers = [&child];
    let quote = bytes::fail_after(0, || {
        let quote = Registry::prepare_release_owner_bounded(
            &owner,
            &graph,
            &peers,
            cut(7),
            usize::MAX,
            usize::MAX,
        )
        .unwrap();
        assert_eq!(bytes::remaining_allocations(), Some(0));
        quote
    });
    let charge = quote.construction_charge();
    let visits = quote.visits();
    for (max_bytes, max_visits) in [(charge - 1, visits), (charge, visits - 1)] {
        assert!(matches!(
            bytes::fail_after(0, || Registry::prepare_release_owner_bounded(
                &owner,
                &graph,
                &peers,
                cut(7),
                max_bytes,
                max_visits
            )),
            Err(ContractError::Capacity)
        ));
    }
    let transition =
        Registry::prepare_release_owner_bounded(&owner, &graph, &peers, cut(7), charge, visits)
            .unwrap()
            .build()
            .unwrap();
    assert_eq!(transition.construction_charge().unwrap(), charge);
    assert_eq!(transition.event(), Event::OwnerReleased { cut: cut(7) });
    let mut copied = owner.try_copy(owner.copy_charge().unwrap()).unwrap();
    copied
        .apply_scope(&owner.binding(), transition, &peers)
        .unwrap();
    assert!(copied.scopes().released());
    assert_eq!(copied.scopes().release_cut(), Some(cut(7)));
    assert_eq!(copied.scopes().children(), owner.scopes().children());
    assert_eq!(
        copied.scopes().iter().collect::<Vec<_>>(),
        owner.scopes().iter().collect::<Vec<_>>()
    );
    assert_eq!(copied.status(), owner.status());
    assert_eq!(copied.terminal_cut(), owner.terminal_cut());
    assert_eq!(copied.binding(), owner.binding().next().unwrap());
    assert!(!owner.scopes().released());
}

#[test]
fn every_release_allocation_and_actual_capacity_refusal_preserves_sources() {
    let (owner, child) = released_child_and_monitor();
    let original = owner.clone();
    let graph = snapshot(&[&owner, &child]);
    let peers = [&child];
    let quote = Registry::prepare_release_owner_bounded(
        &owner,
        &graph,
        &peers,
        cut(7),
        usize::MAX,
        usize::MAX,
    )
    .unwrap();
    let charge = quote.construction_charge();
    let visits = quote.visits();
    // Scope vector, child vector, retained root vector, and complete read vector.
    for allowed in 0..4 {
        let plan =
            Registry::prepare_release_owner_bounded(&owner, &graph, &peers, cut(7), charge, visits)
                .unwrap();
        assert!(matches!(
            bytes::fail_after(allowed, || plan.build()),
            Err(ContractError::Capacity)
        ));
        assert_eq!(owner, original);
    }
    let plan =
        Registry::prepare_release_owner_bounded(&owner, &graph, &peers, cut(7), charge, visits)
            .unwrap();
    assert!(matches!(
        release::with_excess_capacity(|| plan.build()),
        Err(ContractError::Capacity)
    ));
    assert_eq!(owner, original);
    assert!(child.scopes().released());
}

#[test]
fn complete_graph_sources_terminal_children_and_original_cuts_are_required() {
    let mut owner = claim(1);
    let definition = child_definition(&owner, 2, 2);
    let mut child = owner
        .generate_child(
            &owner.binding(),
            Principal::Actor(ISSUER),
            None,
            definition,
            cut(2),
        )
        .unwrap();
    let graph = snapshot(&[&owner, &child]);
    assert!(matches!(
        Registry::prepare_release_owner_bounded(
            &owner,
            &graph,
            &[&child],
            cut(3),
            usize::MAX,
            usize::MAX
        ),
        Err(ContractError::InvalidTransition)
    ));
    cancel_owned(&mut owner, &mut child, 3);
    let old_graph = snapshot(&[&owner, &child]);
    assert!(matches!(
        Registry::prepare_release_owner_bounded(
            &owner,
            &old_graph,
            &[&child],
            cut(4),
            usize::MAX,
            usize::MAX
        ),
        Err(ContractError::InvalidTransition)
    ));
    let child_graph = snapshot(&[&child]);
    let transition = Registry::prepare_release_owner(&child, &child_graph, &[], cut(4)).unwrap();
    child
        .apply_scope(&child.binding(), transition, &[])
        .unwrap();
    assert!(matches!(
        Registry::prepare_release_owner_bounded(
            &owner,
            &old_graph,
            &[&child],
            cut(5),
            usize::MAX,
            usize::MAX
        ),
        Err(ContractError::StaleRevision)
    ));
    let graph = snapshot(&[&owner, &child]);
    assert!(matches!(
        Registry::prepare_release_owner_bounded(
            &owner,
            &graph,
            &[],
            cut(5),
            usize::MAX,
            usize::MAX
        ),
        Err(ContractError::InvalidManifest)
    ));
    for invalid in [
        cut(0),
        cut(2),
        cut(3),
        ClaimCut {
            cause: ContentHash([0; 32]),
            ..cut(5)
        },
    ] {
        assert!(matches!(
            Registry::prepare_release_owner_bounded(
                &owner,
                &graph,
                &[&child],
                invalid,
                usize::MAX,
                usize::MAX
            ),
            Err(ContractError::InvalidCut)
        ));
    }
    let other = claim(9);
    assert!(
        Registry::prepare_release_owner_bounded(
            &owner,
            &graph,
            &[&other],
            cut(5),
            usize::MAX,
            usize::MAX
        )
        .is_err()
    );
    let peers = [&child];
    let plan = Registry::prepare_release_owner_bounded(
        &owner,
        &graph,
        &peers,
        cut(5),
        usize::MAX,
        usize::MAX,
    )
    .unwrap();
    let transition = plan.build().unwrap();
    let before = owner.clone();
    assert!(
        owner
            .apply_scope(&owner.binding(), transition, &[&other])
            .is_err()
    );
    assert_eq!(owner, before);
}

#[test]
fn native_release_refuses_still_active_monitors_even_when_their_predicates_are_settled() {
    let mut owner = claim(1);
    let mut target = claim(2);
    cancel(&mut target, 2);
    register(
        &mut owner,
        &target,
        &[WaitPredicate::Terminal(ClaimId::from_u128(2))],
    );
    cancel(&mut owner, 3);
    let graph = snapshot(&[&owner, &target]);
    assert!(
        graph
            .wait_settled(WaitPredicate::Terminal(ClaimId::from_u128(2)))
            .unwrap()
    );
    let peers = [&target];
    assert!(matches!(
        bytes::fail_after(0, || Registry::prepare_release_owner_bounded(
            &owner,
            &graph,
            &peers,
            cut(4),
            usize::MAX,
            usize::MAX
        )),
        Err(ContractError::InvalidTransition)
    ));
    assert!(
        owner
            .scopes()
            .iter()
            .all(|scope| scope.released().is_none())
    );
    // The compatibility API retains its historical owner-release semantics;
    // native ownership cannot obtain the bounded plan through that path.
    let old = Registry::prepare_release_owner(&owner, &graph, &[&target], cut(4)).unwrap();
    assert_eq!(old.event(), Event::OwnerReleased { cut: cut(4) });
    assert!(
        old.replacement
            .iter()
            .all(|scope| scope.released().is_none())
    );
    let monitor =
        Registry::prepare_release_monitor(&owner, MonitorId::from_u128(1), &graph, cut(4)).unwrap();
    owner
        .apply_scope(&owner.binding(), monitor, &[&target])
        .unwrap();
    let graph = snapshot(&[&owner]);
    let transition = Registry::prepare_release_owner_bounded(
        &owner,
        &graph,
        &[],
        cut(5),
        usize::MAX,
        usize::MAX,
    )
    .unwrap()
    .build()
    .unwrap();
    owner
        .apply_scope(&owner.binding(), transition, &[])
        .unwrap();
    let graph = snapshot(&[&owner]);
    assert!(matches!(
        Registry::prepare_release_owner_bounded(
            &owner,
            &graph,
            &[],
            cut(6),
            usize::MAX,
            usize::MAX
        ),
        Err(ContractError::InvalidTransition)
    ));
}
