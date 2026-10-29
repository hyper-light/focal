#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
//! Who keeps a seat when a placement is planned again (27 §3.1 P4): a heal
//! fills what was vacated and moves nothing else, and a move toward home
//! is one seat at a time and ends.
use focal_directory::*;
use focal_model::{
    ContentHash, LedgerId, RaftIndex, RaftTerm, RouteEpoch, SessionId, SessionSeq, TenantId,
};
use std::collections::{BTreeMap, BTreeSet};

const MEMBERS: usize = 9;
const HOME: u64 = 1;
const AWAY: u64 = 2;

fn region(id: u64) -> RegionId {
    RegionId::from_u128(u128::from(id))
}
fn node(id: u64, region_id: u64, weight: u64) -> NodeRecord {
    NodeRecord {
        enrollment: NodeEnrollment {
            node: id,
            generation: 1,
            region: region(region_id),
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
/// Nodes by identity, region and load.
fn fleet(rows: &[(u64, u64, u64)]) -> BTreeMap<u64, NodeRecord> {
    rows.iter()
        .map(|(id, region, weight)| (*id, node(*id, *region, *weight)))
        .collect()
}
fn policy(failures: u16, home: &[u64]) -> PlacementPolicy {
    PlacementPolicy {
        durability: DurabilityIntent {
            survive: FailureClass::Node,
            max_failures: failures,
        },
        residency: BTreeSet::new(),
        home_regions: home.iter().map(|id| region(*id)).collect(),
        required_memory: 1 << 20,
    }
}
fn sitting(voters: &[u64], copies: &[u64], leader: u64) -> Placement {
    let members: BTreeMap<u64, u64> = voters.iter().map(|voter| (*voter, 1)).collect();
    Placement {
        voters: members.clone(),
        materializers: members,
        content_copies: copies.iter().map(|copy| (*copy, 1)).collect(),
        preferred_leader: leader,
    }
}
fn nowhere() -> BTreeMap<u64, u64> {
    BTreeMap::new()
}
fn heal(
    nodes: &BTreeMap<u64, NodeRecord>,
    policy: &PlacementPolicy,
    sits: &Placement,
) -> Result<Placement, DirectoryError> {
    heal_placement(
        nodes,
        policy,
        sits,
        Leading {
            counts: &nowhere(),
            current: Some(sits.preferred_leader),
        },
        &nowhere(),
        MEMBERS,
        1 << 20,
    )
    .map(|proposal| proposal.spec.placement)
}
fn voters(placement: &Placement) -> Vec<u64> {
    placement.voters.keys().copied().collect()
}
fn dead(nodes: &mut BTreeMap<u64, NodeRecord>, id: u64) {
    nodes.get_mut(&id).unwrap().liveness = Some(NodeLiveness {
        alive: false,
        incarnation: 1,
        witness: 1,
        decided_at: 1,
    });
}

#[test]
fn a_fleet_that_grew_moves_no_one() {
    // Three loaded voters, and two nodes that joined with nothing on them.
    let nodes = fleet(&[
        (1, HOME, 90),
        (2, HOME, 80),
        (3, HOME, 70),
        (4, HOME, 0),
        (5, HOME, 0),
    ]);
    let sits = sitting(&[1, 2, 3], &[1, 3], 3);
    let proposal = heal_placement(
        &nodes,
        &policy(1, &[]),
        &sits,
        Leading {
            counts: &nowhere(),
            current: Some(3),
        },
        &nowhere(),
        MEMBERS,
        1 << 20,
    )
    .unwrap();
    assert_eq!(proposal.spec.placement, sits);
    assert_eq!(
        proposal.explanation,
        "Every voter that sits keeps its seat."
    );
    assert_eq!(
        proposal.observations.keys().copied().collect::<Vec<_>>(),
        [1, 2, 3]
    );
}

#[test]
fn a_death_moves_what_died() {
    let mut nodes = fleet(&[
        (1, HOME, 90),
        (2, HOME, 80),
        (3, HOME, 70),
        (4, HOME, 5),
        (5, HOME, 0),
    ]);
    dead(&mut nodes, 2);
    let healed = heal(&nodes, &policy(1, &[]), &sitting(&[1, 2, 3], &[1, 2], 1)).unwrap();
    // The least loaded takes the seat; the others sit as they sat.
    assert_eq!(voters(&healed), [1, 3, 5]);
    assert_eq!(healed.preferred_leader, 1);
    assert_eq!(healed.materializers, healed.voters);
    // The copy that sits stays a copy; the one that died is replaced by a
    // voter that sat.
    assert_eq!(
        healed.content_copies.keys().copied().collect::<Vec<_>>(),
        [1, 3]
    );
    // The leader that died is replaced by a voter, the others keep theirs.
    let healed = heal(&nodes, &policy(1, &[]), &sitting(&[1, 2, 3], &[1, 2], 2)).unwrap();
    assert_eq!(voters(&healed), [1, 3, 5]);
    assert_ne!(healed.preferred_leader, 2);
    // With no one to take the seat there is no placement, and none worse.
    let mut three = fleet(&[(1, HOME, 0), (2, HOME, 0), (3, HOME, 0)]);
    dead(&mut three, 2);
    assert_eq!(
        heal(&three, &policy(1, &[]), &sitting(&[1, 2, 3], &[1, 2], 1)),
        Err(DirectoryError::NoPlacement)
    );
}

#[test]
fn a_voter_away_from_home_keeps_its_seat_where_the_old_order_took_it() {
    // One voter at home and two that are not, from when home had one node;
    // two more have joined at home since, and one voter died.
    let mut nodes = fleet(&[
        (1, HOME, 10),
        (2, AWAY, 10),
        (3, AWAY, 10),
        (4, HOME, 0),
        (5, HOME, 0),
    ]);
    dead(&mut nodes, 3);
    let sits = sitting(&[1, 2, 3], &[1, 2], 1);
    let healed = heal(&nodes, &policy(1, &[HOME]), &sits).unwrap();
    assert_eq!(voters(&healed), [1, 2, 4], "the vacant seat went home");
    assert_eq!(healed.preferred_leader, 1);
    // As it was planned before, the death of one voter moved two.
    let before = propose_placement_keeping(&nodes, &policy(1, &[HOME]), &sits.voters, MEMBERS, 1)
        .unwrap()
        .spec
        .placement;
    assert_eq!(voters(&before), [1, 4, 5]);
}

#[test]
fn a_session_is_led_at_home_by_the_seat_given_last() {
    // No voter at home, and a node that is: no seat is vacant.
    let nodes = fleet(&[(1, HOME, 50), (2, AWAY, 10), (3, AWAY, 30), (5, AWAY, 20)]);
    let healed = heal(
        &nodes,
        &policy(1, &[HOME]),
        &sitting(&[2, 3, 5], &[2, 3], 2),
    )
    .unwrap();
    // The most loaded of them gave its seat.
    assert_eq!(voters(&healed), [1, 2, 5]);
    assert_eq!(healed.preferred_leader, 1);
    assert_eq!(
        healed.content_copies.keys().copied().collect::<Vec<_>>(),
        [2, 5]
    );
    // A seat that is vacant is the one given: no voter that sits loses one.
    let mut nodes = nodes;
    dead(&mut nodes, 5);
    let healed = heal(
        &nodes,
        &policy(1, &[HOME]),
        &sitting(&[2, 3, 5], &[2, 3], 2),
    )
    .unwrap();
    assert_eq!(voters(&healed), [1, 2, 3]);
    assert_eq!(healed.preferred_leader, 1);
    // No node at home at all: the policy cannot be kept.
    let nodes = fleet(&[(2, AWAY, 10), (3, AWAY, 30), (5, AWAY, 20)]);
    assert_eq!(
        heal(
            &nodes,
            &policy(1, &[HOME]),
            &sitting(&[2, 3, 5], &[2, 3], 2)
        ),
        Err(DirectoryError::Residency)
    );
}

#[test]
fn what_admits_a_node_to_a_seat_is_no_condition_of_keeping_one() {
    let mut nodes = fleet(&[(1, HOME, 0), (2, HOME, 0), (3, HOME, 0), (4, HOME, 0)]);
    // A voter without a report, one without room on its disk, one without
    // memory: each has its copy.
    nodes.get_mut(&1).unwrap().load = None;
    nodes
        .get_mut(&2)
        .unwrap()
        .load
        .as_mut()
        .unwrap()
        .disk_available = 0;
    nodes
        .get_mut(&3)
        .unwrap()
        .load
        .as_mut()
        .unwrap()
        .available_memory = 0;
    let sits = sitting(&[1, 2, 3], &[1, 2], 1);
    assert_eq!(heal(&nodes, &policy(1, &[]), &sits).unwrap(), sits);
    // None of them is given a seat it does not have.
    let mut others = nodes.clone();
    dead(&mut others, 4);
    assert_eq!(
        heal(&others, &policy(1, &[]), &sitting(&[4], &[4], 4)).map(|placement| voters(&placement)),
        Err(DirectoryError::NoPlacement)
    );
    // A voter that is drained, that enrolled again, or that left the
    // residency has vacated its seat. One that enrolled again is a node
    // like any other at the generation it has now.
    let unseated: [fn(&mut NodeRecord); 3] = [
        |node| node.enrollment.eligible = false,
        |node| {
            node.enrollment.generation = 2;
            node.load.as_mut().unwrap().generation = 2;
        },
        |node| node.enrollment.region = region(9),
    ];
    for (case, unseat) in unseated.into_iter().enumerate() {
        let mut nodes = fleet(&[(1, HOME, 0), (2, HOME, 5), (3, HOME, 0), (4, HOME, 9)]);
        unseat(nodes.get_mut(&2).unwrap());
        let mut policy = policy(1, &[]);
        policy.residency = BTreeSet::from([region(HOME)]);
        let healed = heal(&nodes, &policy, &sitting(&[1, 2, 3], &[1, 2], 1)).unwrap();
        if case == 1 {
            assert_eq!(healed.voters, BTreeMap::from([(1, 1), (2, 2), (3, 1)]));
        } else {
            assert_eq!(voters(&healed), [1, 3, 4], "case {case}");
        }
    }
}

#[test]
fn a_policy_of_more_voters_adds_them_and_one_of_fewer_keeps_the_best() {
    let nodes = fleet(&[
        (1, HOME, 9),
        (2, AWAY, 1),
        (3, HOME, 5),
        (4, HOME, 0),
        (5, AWAY, 0),
        (6, HOME, 2),
    ]);
    let sits = sitting(&[1, 2, 3], &[1, 2], 1);
    let more = heal(&nodes, &policy(2, &[HOME]), &sits).unwrap();
    assert_eq!(
        voters(&more),
        [1, 2, 3, 4, 6],
        "at home first, then by load"
    );
    assert_eq!(more.preferred_leader, 1);
    assert_eq!(more.content_copies.len(), 3);
    assert!(more.content_copies.contains_key(&1) && more.content_copies.contains_key(&2));
    let fewer = heal(&nodes, &policy(0, &[HOME]), &sits).unwrap();
    assert_eq!(voters(&fewer), [3], "of those at home the least loaded");
    assert_eq!(fewer.preferred_leader, 3);
    assert_eq!(
        heal(&nodes, &policy(8, &[HOME]), &sits),
        Err(DirectoryError::Capacity)
    );
    // One voter in a failure domain: two that share a zone keep one seat.
    let mut zoned = fleet(&[(1, HOME, 0), (2, HOME, 1), (3, HOME, 2), (4, HOME, 3)]);
    zoned.get_mut(&2).unwrap().enrollment.zone = zoned[&1].enrollment.zone;
    let mut by_zone = policy(1, &[]);
    by_zone.durability.survive = FailureClass::Zone;
    let healed = heal(&zoned, &by_zone, &sitting(&[1, 2, 3], &[1, 3], 1)).unwrap();
    assert_eq!(voters(&healed), [1, 3, 4]);
}

fn session(voters: &[u64], leader: u64, home: &[u64]) -> SessionDescriptor {
    let ledger = LedgerId {
        tenant: TenantId::from_u128(1),
        session: SessionId::from_u128(1),
    };
    let active = PlacementSpec {
        policy: policy(1, home),
        placement: sitting(voters, &voters[..2], leader),
    };
    SessionDescriptor {
        ledger,
        log_group: LogGroupId::from_u128(1),
        revision: 1,
        route_epoch: RouteEpoch(1),
        membership_epoch: 1,
        placement_epoch: 1,
        authority: SessionFence {
            kind: SessionFenceKind::Created,
            ledger,
            log_group: LogGroupId::from_u128(1),
            operation: OperationId::from_u128(1),
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

#[test]
fn a_move_toward_home_is_one_seat_and_the_moves_end() {
    let nodes = fleet(&[
        (1, HOME, 3),
        (2, AWAY, 7),
        (3, AWAY, 9),
        (4, HOME, 1),
        (5, HOME, 0),
        (6, AWAY, 0),
    ]);
    let mut moving = session(&[1, 2, 3], 1, &[HOME]);
    let mut moves = Vec::new();
    while let Some(proposal) = home_move(&moving, &nodes, &nowhere(), MEMBERS, 1 << 20) {
        let before = &moving.active.placement;
        let after = &proposal.spec.placement;
        let left: Vec<u64> = voters(before)
            .into_iter()
            .filter(|voter| !after.voters.contains_key(voter))
            .collect();
        let came: Vec<u64> = voters(after)
            .into_iter()
            .filter(|voter| !before.voters.contains_key(voter))
            .collect();
        assert_eq!((left.len(), came.len()), (1, 1), "one seat");
        assert_eq!(after.preferred_leader, before.preferred_leader);
        assert_eq!(after.materializers, after.voters);
        assert_eq!(after.content_copies.len(), before.content_copies.len());
        assert_eq!(proposal.spec.policy, moving.active.policy);
        assert_eq!(
            proposal.observations.keys().copied().collect::<Vec<_>>(),
            came
        );
        moves.push((left[0], came[0]));
        moving.active = proposal.spec;
        assert!(moves.len() <= 3, "the moves do not end: {moves:?}");
    }
    // The most loaded first, to the least loaded at home.
    assert_eq!(moves, [(3, 5), (2, 4)]);
    assert_eq!(voters(&moving.active.placement), [1, 4, 5]);
    // What the directory seats counts where it is more than a report says:
    // node 2 holds ten seats its report does not count yet, so its seat
    // moves first, and node 5, seated twice since it reported, is passed
    // over for node 4.
    let held = BTreeMap::from([(2, 10), (5, 2)]);
    let first = home_move(
        &session(&[1, 2, 3], 1, &[HOME]),
        &nodes,
        &held,
        MEMBERS,
        1 << 20,
    )
    .unwrap();
    assert_eq!(voters(&first.spec.placement), [1, 3, 4]);

    // Nothing moves without home regions, while the session moves or
    // leaves copies behind, where its placement does not hold, or where
    // no node at home could be seated.
    assert!(home_move(&session(&[1, 2, 3], 1, &[]), &nodes, &nowhere(), MEMBERS, 1).is_none());
    let mut pending = session(&[1, 2, 3], 1, &[HOME]);
    pending.retiring.insert(
        6,
        AssignmentProgress::assigned(6, 1, BTreeSet::from([AssignmentRole::Voter])),
    );
    assert!(home_move(&pending, &nodes, &nowhere(), MEMBERS, 1).is_none());
    let mut broken = nodes.clone();
    dead(&mut broken, 2);
    assert!(
        home_move(
            &session(&[1, 2, 3], 1, &[HOME]),
            &broken,
            &nowhere(),
            MEMBERS,
            1
        )
        .is_none()
    );
    let none_home = fleet(&[(1, HOME, 3), (2, AWAY, 7), (3, AWAY, 9), (6, AWAY, 0)]);
    assert!(
        home_move(
            &session(&[1, 2, 3], 1, &[HOME]),
            &none_home,
            &nowhere(),
            MEMBERS,
            1
        )
        .is_none()
    );
    let mut full = nodes.clone();
    for id in [4, 5] {
        full.get_mut(&id)
            .unwrap()
            .load
            .as_mut()
            .unwrap()
            .disk_available = 0;
    }
    assert!(
        home_move(
            &session(&[1, 2, 3], 1, &[HOME]),
            &full,
            &nowhere(),
            MEMBERS,
            1 << 20
        )
        .is_none()
    );
}

/// Nodes in three zones, one region, `per_zone` to a zone: node `k`,
/// counted from one, in zone `(k - 1) % 3`, as the comparison's nodes are,
/// every one reporting no copies.
fn zoned_fleet(per_zone: u64) -> BTreeMap<u64, NodeRecord> {
    (1..=3 * per_zone)
        .map(|id| {
            let mut record = node(id, HOME, 0);
            record.enrollment.zone = ZoneId::from_u128(u128::from((id - 1) % 3 + 1));
            (id, record)
        })
        .collect()
}
fn zone_policy() -> PlacementPolicy {
    let mut zonal = policy(1, &[]);
    zonal.durability.survive = FailureClass::Zone;
    zonal
}
/// Session `k` founded on `founder` alone and planned to `desired`, its
/// plan committed and not yet carried out.
fn planned(k: u128, founder: u64, desired: &Placement) -> SessionDescriptor {
    let mut planned = session(&[founder, founder, founder], founder, &[]);
    planned.ledger.session = SessionId::from_u128(k);
    planned.active.placement = sitting(&[founder], &[founder], founder);
    planned.pending = Some(PendingPlacement {
        operation: OperationId::from_u128(k),
        next_route: RouteEpoch(2),
        next_membership: 1,
        next_placement: 2,
        desired: PlacementSpec {
            policy: zone_policy(),
            placement: desired.clone(),
        },
        phase: PlacementPhase::Planned,
        ready: BTreeMap::new(),
        barrier: None,
        observations: BTreeMap::new(),
        progress: BTreeMap::new(),
    });
    planned
}
/// Plan sessions one after another within one load interval: each founded
/// on its own node of the first zone and planned to survive a zone, every
/// node's report still counting no copies. Each plan is made on the
/// directory as the plans before it left it, by what it seats
/// (`seats_held`), or, with `by_reports_alone`, by the reports and nothing
/// else.
fn plan_in_a_burst(nodes: &BTreeMap<u64, NodeRecord>, by_reports_alone: bool) -> Vec<Placement> {
    let founders: Vec<u64> = nodes
        .keys()
        .copied()
        .filter(|id| (id - 1) % 3 == 0)
        .collect();
    let mut sessions = BTreeMap::new();
    let mut placements = Vec::new();
    for (k, founder) in founders.into_iter().enumerate() {
        let held = if by_reports_alone {
            nowhere()
        } else {
            seats_held(&sessions)
        };
        let placement = heal_placement(
            nodes,
            &zone_policy(),
            &sitting(&[founder], &[founder], founder),
            Leading {
                counts: &leading(&sessions),
                current: Some(founder),
            },
            &held,
            MEMBERS,
            1 << 20,
        )
        .unwrap()
        .spec
        .placement;
        let session = planned(k as u128 + 2, founder, &placement);
        sessions.insert(session.ledger, session);
        placements.push(placement);
    }
    placements
}

/// Plans made faster than nodes report (every 30 s in `focal-node`) take
/// each seat by what the directory has seated: on 33 nodes in three zones,
/// eleven sessions planned in a burst sit one on every node, each in three
/// zones. By the reports alone every one of them took the same two nodes of
/// the other zones (the comparison's 33 nodes, 2026-10-29: four sessions
/// planned within two seconds were seated on the same two).
#[test]
fn seats_planned_in_a_burst_go_to_the_nodes_holding_the_fewest() {
    let nodes = zoned_fleet(11);
    let zone = |id: &u64| nodes[id].enrollment.zone;
    let placements = plan_in_a_burst(&nodes, false);
    assert_eq!(placements.len(), 11);
    let mut seats = BTreeMap::new();
    for placement in &placements {
        let zones: BTreeSet<ZoneId> = placement.voters.keys().map(zone).collect();
        assert_eq!(zones.len(), 3, "three voters in three zones: {placement:?}");
        assert_eq!(placement.materializers, placement.voters);
        for id in placement.voters.keys() {
            *seats.entry(*id).or_insert(0) += 1;
        }
    }
    assert_eq!(
        seats,
        nodes
            .keys()
            .map(|id| (*id, 1))
            .collect::<BTreeMap<u64, u64>>(),
        "every node seated once"
    );
    // The leaders spread too: each session is preferred where no other is.
    let leaders: BTreeSet<u64> = placements.iter().map(|p| p.preferred_leader).collect();
    assert_eq!(leaders.len(), 11);

    // By the reports alone, the defect: the same two nodes for every plan.
    let piled = plan_in_a_burst(&nodes, true);
    let others: BTreeSet<Vec<u64>> = piled
        .iter()
        .map(|placement| {
            placement
                .voters
                .keys()
                .copied()
                .filter(|id| (id - 1) % 3 != 0)
                .collect()
        })
        .collect();
    assert_eq!(others.len(), 1, "every plan took the same two: {others:?}");
}

/// What the directory seats on a node is counted from the placement its
/// sessions move to, or hold where they do not move, each node once per
/// session; and a seat goes to the node holding the fewest by its report or
/// by the directory, whichever says more.
#[test]
fn a_node_holds_what_it_reported_or_what_the_directory_seats_on_it() {
    let moving = planned(2, 4, &sitting(&[4, 5, 6], &[4, 5], 5));
    let mut still = session(&[1, 2, 3], 1, &[]);
    still.ledger.session = SessionId::from_u128(3);
    // A copy that does not vote is a node the session places all the same.
    still.active.placement.content_copies.insert(7, 1);
    let sessions = BTreeMap::from([(moving.ledger, moving), (still.ledger, still)]);
    assert_eq!(
        seats_held(&sessions),
        BTreeMap::from([(1, 1), (2, 1), (3, 1), (4, 1), (5, 1), (6, 1), (7, 1)]),
        "the moving session counted where it goes, every node once"
    );
    assert!(seats_held(&BTreeMap::new()).is_empty());

    // Node 1 reported five copies and holds none here; node 2 reported none
    // and holds two here; node 3 reported one and holds one; node 4 holds
    // three here; node 5 founded the session and sits.
    let nodes = fleet(&[
        (1, HOME, 5),
        (2, HOME, 0),
        (3, HOME, 1),
        (4, HOME, 0),
        (5, HOME, 0),
    ]);
    let held = BTreeMap::from([(2, 2), (3, 1), (4, 3)]);
    let placement = heal_placement(
        &nodes,
        &policy(1, &[]),
        &sitting(&[5], &[5], 5),
        Leading {
            counts: &nowhere(),
            current: Some(5),
        },
        &held,
        MEMBERS,
        1 << 20,
    )
    .unwrap()
    .spec
    .placement;
    assert_eq!(voters(&placement), [2, 3, 5]);
}

#[test]
fn a_death_moves_a_seat_once_it_has_stood() {
    let verdict = |alive: bool, decided_at: i64| NodeLiveness {
        alive,
        incarnation: 1,
        witness: 1,
        decided_at,
    };
    let mut nodes = fleet(&[(1, HOME, 0), (2, HOME, 0), (3, HOME, 0), (4, HOME, 0)]);
    let sits = sitting(&[1, 2, 3], &[1, 2], 1);
    // No one is dead: nothing is waited for.
    assert!(deaths_held(&sits, &nodes, 0, 10));
    nodes.get_mut(&2).unwrap().liveness = Some(verdict(false, 100));
    assert!(!deaths_held(&sits, &nodes, 100, 10));
    assert!(!deaths_held(&sits, &nodes, 109, 10));
    assert!(deaths_held(&sits, &nodes, 110, 10));
    // A clock behind the verdict has held nothing.
    assert!(!deaths_held(&sits, &nodes, 99, 10));
    assert!(!deaths_held(&sits, &nodes, i64::MIN, 10));
    assert!(deaths_held(&sits, &nodes, i64::MAX, i64::MAX - 100));
    // It came back: a verdict says so, and nothing is waited for.
    nodes.get_mut(&2).unwrap().liveness = Some(verdict(true, 105));
    assert!(deaths_held(&sits, &nodes, 106, 10));
    // Dead again, it is dead since then.
    nodes.get_mut(&2).unwrap().liveness = Some(verdict(false, 108));
    assert!(!deaths_held(&sits, &nodes, 110, 10));
    assert!(deaths_held(&sits, &nodes, 118, 10));
    // Every death of the placement must have stood, and only of it.
    nodes.get_mut(&3).unwrap().liveness = Some(verdict(false, 115));
    nodes.get_mut(&4).unwrap().liveness = Some(verdict(false, 124));
    assert!(!deaths_held(&sits, &nodes, 120, 10));
    assert!(deaths_held(&sits, &nodes, 125, 10));
}

#[test]
fn how_long_a_death_still_stands_is_the_longest_left() {
    let verdict = |alive: bool, decided_at: i64| NodeLiveness {
        alive,
        incarnation: 1,
        witness: 1,
        decided_at,
    };
    let mut nodes = fleet(&[(1, HOME, 0), (2, HOME, 0), (3, HOME, 0), (4, HOME, 0)]);
    let sits = sitting(&[1, 2, 3], &[1, 2], 1);
    // No one is dead: nothing stands.
    assert_eq!(deaths_stand_for(&sits, &nodes, 0, 10), None);
    nodes.get_mut(&2).unwrap().liveness = Some(verdict(false, 100));
    assert_eq!(deaths_stand_for(&sits, &nodes, 100, 10), Some(10));
    assert_eq!(deaths_stand_for(&sits, &nodes, 107, 10), Some(3));
    assert_eq!(deaths_stand_for(&sits, &nodes, 110, 10), None);
    // A clock behind the verdict: the whole hold.
    assert_eq!(deaths_stand_for(&sits, &nodes, 99, 10), Some(10));
    assert_eq!(deaths_stand_for(&sits, &nodes, i64::MIN, 10), Some(10));
    // The longest left of two deaths; a death outside the placement is not
    // waited for.
    nodes.get_mut(&3).unwrap().liveness = Some(verdict(false, 106));
    nodes.get_mut(&4).unwrap().liveness = Some(verdict(false, 120));
    assert_eq!(deaths_stand_for(&sits, &nodes, 108, 10), Some(8));
    assert_eq!(deaths_stand_for(&sits, &nodes, 116, 10), None);
    // One rule: held is what stands no longer.
    for now in 95..125 {
        assert_eq!(
            deaths_held(&sits, &nodes, now, 10),
            deaths_stand_for(&sits, &nodes, now, 10).is_none(),
            "{now}"
        );
    }
}
