#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
//! Where many logs are led (27 §5): the planner spreads preferred leaders
//! by what the directory has committed, keeps one unless moving it helps,
//! and a fleet it balances stays balanced.
use focal_directory::*;
use focal_model::{
    ContentHash, LedgerId, RaftIndex, RaftTerm, RouteEpoch, SessionId, SessionSeq, TenantId,
};
use std::collections::{BTreeMap, BTreeSet};

const MEMBERS: usize = 9;

fn region(id: u64) -> RegionId {
    RegionId::from_u128(u128::from(id))
}
fn node(id: u64, home: u64, weight: u64) -> NodeRecord {
    NodeRecord {
        enrollment: NodeEnrollment {
            node: id,
            generation: 1,
            region: region(home),
            zone: ZoneId::from_u128(u128::from(id)),
            endpoint: format!("node-{id}:443"),
            identity: ContentHash([id as u8; 32]),
            authority_epoch: 1,
            attestation: ContentHash([9; 32]),
            eligible: true,
        },
        load: Some(NodeLoad {
            node: id,
            generation: 1,
            report: 1,
            available_memory: 1 << 30,
            active_weight: weight,
            disk_available: 1 << 30,
            capability: 0,
        }),
        liveness: None,
    }
}
fn fleet(count: u64) -> BTreeMap<u64, NodeRecord> {
    (1..=count).map(|id| (id, node(id, 1, 0))).collect()
}
fn policy(failures: u16, home: &[u64]) -> PlacementPolicy {
    PlacementPolicy {
        durability: DurabilityIntent {
            survive: FailureClass::Node,
            max_failures: failures,
        },
        residency: BTreeSet::new(),
        home_regions: home.iter().map(|id| region(*id)).collect(),
        required_memory: 0,
    }
}
fn ledger(session: u128) -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(1),
        session: SessionId::from_u128(session),
    }
}
fn session(id: u128, voters: &[u64], leader: u64, home: &[u64]) -> SessionDescriptor {
    let members: BTreeMap<u64, u64> = voters.iter().map(|voter| (*voter, 1)).collect();
    let failures = u16::try_from((voters.len() - 1) / 2).unwrap();
    let active = PlacementSpec {
        policy: policy(failures, home),
        placement: Placement {
            voters: members.clone(),
            materializers: members.clone(),
            content_copies: members,
            preferred_leader: leader,
        },
    };
    SessionDescriptor {
        ledger: ledger(id),
        log_group: LogGroupId::from_u128(id),
        revision: 1,
        route_epoch: RouteEpoch(1),
        membership_epoch: 1,
        placement_epoch: 1,
        authority: SessionFence {
            kind: SessionFenceKind::Created,
            ledger: ledger(id),
            log_group: LogGroupId::from_u128(id),
            operation: OperationId::from_u128(id),
            sequence: SessionSeq(1),
            index: RaftIndex(2),
            term: RaftTerm(1),
            from_route: RouteEpoch(0),
            to_route: RouteEpoch(1),
            membership_epoch: 1,
            placement_epoch: 1,
            placement_digest: placement_digest(&active).unwrap(),
            record_hash: ContentHash([9; 32]),
        },
        active,
        pending: None,
        retiring: BTreeMap::new(),
        refusals: Vec::new(),
        founder: None,
        holders: None,
    }
}
fn counts(rows: &[(u64, u64)]) -> BTreeMap<u64, u64> {
    rows.iter().copied().collect()
}

#[test]
fn a_preferred_leader_is_kept_unless_another_leads_two_fewer() {
    let choose = |rows: &[(u64, u64)], current: Option<u64>, candidates: &[u64]| {
        Leading {
            counts: &counts(rows),
            current,
        }
        .choose(candidates.iter().copied())
    };
    // No one is preferred yet: the first, as the members were selected.
    assert_eq!(choose(&[], None, &[3, 1, 2]), Some(3));
    assert_eq!(choose(&[], None, &[]), None);
    // The least led, and the first of them.
    assert_eq!(choose(&[(1, 4), (2, 2), (3, 2)], None, &[1, 3, 2]), Some(3));
    assert_eq!(choose(&[(1, 4), (3, 2)], None, &[1, 3, 2]), Some(2));
    // One fewer elsewhere is no reason to move; two fewer is.
    assert_eq!(choose(&[(1, 3), (2, 2)], Some(1), &[1, 2]), Some(1));
    assert_eq!(choose(&[(1, 4), (2, 2)], Some(1), &[1, 2]), Some(2));
    assert_eq!(choose(&[(1, 2)], Some(1), &[1, 2]), Some(2));
    assert_eq!(choose(&[(1, 1)], Some(1), &[1, 2]), Some(1));
    // Who is no candidate any more is not kept.
    assert_eq!(choose(&[(1, 1), (2, 9)], Some(1), &[2, 3]), Some(3));
    assert_eq!(choose(&[(1, 1)], Some(1), &[1]), Some(1));
    // Counts at their bound move no one by overflow.
    assert_eq!(
        choose(&[(1, u64::MAX), (2, u64::MAX - 1)], Some(1), &[1, 2]),
        Some(1)
    );
    assert_eq!(
        choose(&[(1, u64::MAX), (2, u64::MAX - 2)], Some(1), &[1, 2]),
        Some(2)
    );
}

#[test]
fn the_planner_leads_a_session_where_fewest_are_led() {
    let nodes = fleet(3);
    let plan = |rows: &[(u64, u64)], current: Option<u64>, home: &[u64]| {
        propose_placement_leading(
            &nodes,
            &policy(1, home),
            &BTreeMap::new(),
            Leading {
                counts: &counts(rows),
                current,
            },
            MEMBERS,
            1,
        )
        .map(|proposal| proposal.spec.placement)
    };
    // As it was planned before leadership was counted.
    let before = propose_placement_keeping(&nodes, &policy(1, &[]), &BTreeMap::new(), MEMBERS, 1)
        .unwrap()
        .spec
        .placement;
    assert_eq!(plan(&[], None, &[]).unwrap(), before);
    assert_eq!(before.preferred_leader, 1);

    let placed = plan(&[(1, 5), (2, 3), (3, 4)], None, &[]).unwrap();
    assert_eq!(placed.preferred_leader, 2);
    assert_eq!(placed.voters, before.voters);
    assert_eq!(
        plan(&[(1, 5), (2, 4), (3, 4)], Some(1), &[])
            .unwrap()
            .preferred_leader,
        1
    );
    assert_eq!(
        plan(&[(1, 5), (2, 4), (3, 3)], Some(1), &[])
            .unwrap()
            .preferred_leader,
        3
    );
    // Only a member at home leads, however many it leads already.
    let mut abroad = fleet(3);
    for id in [2, 3] {
        abroad.get_mut(&id).unwrap().enrollment.region = region(2);
    }
    let placed = propose_placement_leading(
        &abroad,
        &policy(1, &[1]),
        &BTreeMap::new(),
        Leading {
            counts: &counts(&[(1, 100)]),
            current: None,
        },
        MEMBERS,
        1,
    )
    .unwrap();
    assert_eq!(placed.spec.placement.preferred_leader, 1);
    assert!(plan(&[], None, &[7]).is_err());
}

#[test]
fn leadership_is_counted_by_where_sessions_are_going() {
    let mut sessions = BTreeMap::new();
    for id in 1..=5u128 {
        sessions.insert(ledger(id), session(id, &[1, 2, 3], 1, &[]));
    }
    sessions.insert(ledger(6), session(6, &[1, 2, 3], 3, &[]));
    assert_eq!(leading(&sessions), counts(&[(1, 5), (3, 1)]));
    let moving = sessions.get_mut(&ledger(2)).unwrap();
    let mut desired = moving.active.clone();
    desired.placement.preferred_leader = 2;
    moving.pending = Some(PendingPlacement {
        operation: OperationId::from_u128(77),
        next_route: RouteEpoch(2),
        next_membership: 1,
        next_placement: 2,
        desired,
        phase: PlacementPhase::Planned,
        ready: BTreeMap::new(),
        barrier: None,
        observations: BTreeMap::new(),
        progress: BTreeMap::new(),
    });
    assert_eq!(leading(&sessions), counts(&[(1, 4), (2, 1), (3, 1)]));
    assert!(leading(&BTreeMap::new()).is_empty());
}

#[test]
fn a_session_moves_its_leader_only_to_a_voter_that_could_lead() {
    let nodes = fleet(4);
    let led = counts(&[(1, 6), (2, 1), (3, 2)]);
    let one = session(1, &[1, 2, 3], 1, &[]);
    let moved = leader_move(&one, &nodes, &led, MEMBERS).unwrap();
    assert_eq!(moved.placement.preferred_leader, 2);
    assert_eq!(moved.policy, one.active.policy);
    assert_eq!(moved.placement.voters, one.active.placement.voters);
    assert_eq!(
        moved.placement.content_copies,
        one.active.placement.content_copies
    );
    // Node four leads nothing and is no voter of this session.
    assert!(!moved.placement.voters.contains_key(&4));

    // Among the least led, the least loaded.
    let mut loaded = nodes.clone();
    loaded
        .get_mut(&2)
        .unwrap()
        .load
        .as_mut()
        .unwrap()
        .active_weight = 9;
    let even = counts(&[(1, 6), (2, 2), (3, 2)]);
    assert_eq!(
        leader_move(&one, &loaded, &even, MEMBERS)
            .unwrap()
            .placement
            .preferred_leader,
        3
    );

    // Among the least led, one in the zone the session is led in, however
    // loaded: the session stays as near to who it serves.
    let mut zoned = nodes.clone();
    zoned.get_mut(&3).unwrap().enrollment.zone = zoned[&1].enrollment.zone;
    zoned
        .get_mut(&3)
        .unwrap()
        .load
        .as_mut()
        .unwrap()
        .active_weight = 50;
    assert_eq!(
        leader_move(&one, &zoned, &even, MEMBERS)
            .unwrap()
            .placement
            .preferred_leader,
        3
    );
    // One led less is preferred to one that is nearer.
    assert_eq!(
        leader_move(&one, &zoned, &led, MEMBERS)
            .unwrap()
            .placement
            .preferred_leader,
        2
    );
    // A zone that is not known is near to nothing.
    let mut unknown = zoned.clone();
    for node in unknown.values_mut() {
        node.enrollment.zone = ZoneId::from_u128(0);
    }
    assert_eq!(
        leader_move(&one, &unknown, &even, MEMBERS)
            .unwrap()
            .placement
            .preferred_leader,
        2
    );

    // A voter that is dead, drained, without a report, of another
    // generation or not at home does not lead.
    let unfit: [fn(&mut NodeRecord); 5] = [
        |node| {
            node.liveness = Some(NodeLiveness {
                alive: false,
                incarnation: 1,
                witness: 3,
                decided_at: 1,
            })
        },
        |node| node.enrollment.eligible = false,
        |node| node.load = None,
        |node| node.enrollment.generation = 2,
        |node| node.enrollment.region = region(9),
    ];
    for unfit in unfit {
        let mut nodes = fleet(3);
        unfit(nodes.get_mut(&2).unwrap());
        let home = session(1, &[1, 2, 3], 1, &[1]);
        let moved = leader_move(&home, &nodes, &counts(&[(1, 6), (3, 5)]), MEMBERS);
        assert_eq!(moved, None, "{:?}", nodes.get(&2));
    }

    // A session that is moving, or leaves copies behind, waits.
    let mut moving = one.clone();
    moving.retiring.insert(
        4,
        AssignmentProgress::assigned(4, 1, BTreeSet::from([AssignmentRole::Voter])),
    );
    assert_eq!(leader_move(&moving, &nodes, &led, MEMBERS), None);
    // One fewer is no reason.
    assert_eq!(
        leader_move(&one, &nodes, &counts(&[(1, 3), (2, 2), (3, 2)]), MEMBERS),
        None
    );
    // A single voter has no one to move to.
    assert_eq!(
        leader_move(&session(1, &[1], 1, &[]), &nodes, &led, MEMBERS),
        None
    );
}

/// Every session of a fleet is led by the lowest of its nodes. Moved one at
/// a time, each by what is committed when it moves, leadership spreads
/// until no session has a voter that leads two fewer than its leader. Every
/// move lowers the sum of squares of what the nodes lead, so the moves
/// end, whatever nodes the sessions share.
#[test]
fn a_fleet_led_from_few_nodes_spreads_and_comes_to_rest() {
    for (nodes_in, members, count) in [(3u64, 3usize, 64u128), (5, 3, 200), (7, 5, 333), (4, 1, 9)]
    {
        let nodes = fleet(nodes_in);
        let mut sessions = BTreeMap::new();
        for id in 1..=count {
            // Members are consecutive nodes from one that depends on the
            // session.
            let first = u64::try_from(id % u128::from(nodes_in)).unwrap();
            let voters: Vec<u64> = (0..members as u64)
                .map(|offset| (first + offset) % nodes_in + 1)
                .collect();
            let leader = *voters.iter().min().unwrap();
            sessions.insert(ledger(id), session(id, &voters, leader, &[]));
        }
        let squares =
            |led: &BTreeMap<u64, u64>| led.values().map(|count| count * count).sum::<u64>();
        let began = squares(&leading(&sessions));
        let mut moves = 0u64;
        loop {
            let led = leading(&sessions);
            let Some((at, desired)) = sessions.iter().find_map(|(at, session)| {
                leader_move(session, &nodes, &led, MEMBERS).map(|desired| (*at, desired))
            }) else {
                break;
            };
            sessions.get_mut(&at).unwrap().active = desired;
            moves += 1;
            assert!(
                squares(&leading(&sessions)) + 2 <= squares(&led),
                "{nodes_in} nodes: a move that did not help"
            );
        }
        assert!(moves * 2 <= began, "{nodes_in} nodes: {moves} moves");
        let led = leading(&sessions);
        assert_eq!(led.values().sum::<u64>(), u64::try_from(count).unwrap());
        for session in sessions.values() {
            let placement = &session.active.placement;
            let here = led[&placement.preferred_leader];
            for voter in placement.voters.keys() {
                let there = led.get(voter).copied().unwrap_or(0);
                assert!(
                    there + LEADER_SLACK > here,
                    "{nodes_in} nodes: {led:?} and {placement:?}"
                );
            }
        }
        println!("{nodes_in} nodes, {count} sessions: {moves} moves, led {led:?}");
        if members > 1 {
            // Every two nodes share a session here, so no two differ by
            // more than one.
            let most = led.values().max().unwrap();
            let least = (1..=nodes_in)
                .map(|node| led.get(&node).copied().unwrap_or(0))
                .min()
                .unwrap();
            assert!(most - least <= 1, "{led:?}");
        } else {
            assert_eq!(moves, 0);
        }
    }
}
