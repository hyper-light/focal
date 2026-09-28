//! Who keeps a seat when a placement is planned again (27 §3.1 P4).
//!
//! Two decisions were one ordering, and each made the other wrong
//! somewhere. They are two:
//!
//! - **A heal** ([`heal_placement`]) fills the seats that were vacated and
//!   moves nothing else. A voter that sits, is enrolled at the generation
//!   it sits at, is eligible, alive and inside the residency keeps its
//!   seat, whatever else has joined since and wherever it is. So a death
//!   moves what died, and a fleet that grew moves nothing by growing. The
//!   one seat a heal takes from a voter that could keep it is the one the
//!   policy cannot do without: a session with home regions is led at home,
//!   and where no voter that sits is there, one that is takes the seat of
//!   the newest of those that are not.
//! - **A move toward home** ([`home_move`]) is a decision of its own: one
//!   voter that is not at home gives its seat to a node that is, one
//!   session and one seat at a time, made by the controller for a state
//!   that lasts (`leader_balancer` in `focal-node`). Every move leaves one
//!   voter fewer away from home and none more, so the moves end.
//!
//! What admits a node to a seat it does not have (a load report of its
//! generation, memory, disk) is no condition of keeping one: a copy that
//! is installed is not uninstalled for the room it would need to be
//! installed.
use crate::placement::failure_domain;
use crate::*;
use std::collections::{BTreeMap, BTreeSet};

fn at_home(policy: &PlacementPolicy, node: &NodeRecord) -> bool {
    policy.home_regions.is_empty() || policy.home_regions.contains(&node.enrollment.region)
}
/// The node that sits at `generation`, where it may keep its seat.
fn sits<'a>(
    nodes: &'a BTreeMap<u64, NodeRecord>,
    policy: &PlacementPolicy,
    id: u64,
    generation: u64,
) -> Option<&'a NodeRecord> {
    nodes.get(&id).filter(|node| {
        node.enrollment.eligible
            && node.enrollment.generation == generation
            && node.is_alive()
            && failure_domain(&node.enrollment, policy.durability.survive).is_ok()
            && (policy.residency.is_empty() || policy.residency.contains(&node.enrollment.region))
    })
}
/// The nodes that may be given a seat, the best first: at home, least
/// loaded, with the most room, by identity.
fn admitted<'a>(
    nodes: &'a BTreeMap<u64, NodeRecord>,
    policy: &PlacementPolicy,
    min_disk_available: u64,
) -> Vec<(&'a NodeRecord, NodeLoad)> {
    let mut candidates: Vec<_> = nodes
        .values()
        .filter_map(|node| {
            let load = node.load?;
            (sits(
                nodes,
                policy,
                node.enrollment.node,
                node.enrollment.generation,
            )
            .is_some()
                && load.generation == node.enrollment.generation
                && load.available_memory >= policy.required_memory
                && load.disk_available >= min_disk_available)
                .then_some((node, load))
        })
        .collect();
    candidates.sort_by_key(|(node, load)| {
        (
            !at_home(policy, node),
            load.active_weight,
            std::cmp::Reverse(load.available_memory),
            std::cmp::Reverse(load.disk_available),
            node.enrollment.node,
        )
    });
    candidates
}
fn weight(node: &NodeRecord) -> u64 {
    node.load.map_or(u64::MAX, |load| load.active_weight)
}
fn voters_needed(policy: &PlacementPolicy, max_members: usize) -> Result<usize, DirectoryError> {
    let needed = usize::from(policy.durability.max_failures)
        .checked_mul(2)
        .and_then(|voters| voters.checked_add(1))
        .ok_or(DirectoryError::CounterExhausted)?;
    if needed > max_members {
        return Err(DirectoryError::Capacity);
    }
    Ok(needed)
}
fn observed(seats: &[&NodeRecord]) -> BTreeMap<u64, u64> {
    seats
        .iter()
        .filter_map(|node| Some((node.enrollment.node, node.load?.report)))
        .collect()
}

/// The placement of `policy` that keeps every seat of `sitting` that can be
/// kept and fills the rest. `policy` is the session's own, or the one an
/// operator asks for: more voters are added beside those that sit, and
/// fewer keep the best of them.
pub fn heal_placement(
    nodes: &BTreeMap<u64, NodeRecord>,
    policy: &PlacementPolicy,
    sitting: &Placement,
    leading: Leading<'_>,
    max_members: usize,
    min_disk_available: u64,
) -> Result<PlacementProposal, DirectoryError> {
    let needed = voters_needed(policy, max_members)?;
    let mut kept: Vec<&NodeRecord> = sitting
        .voters
        .iter()
        .filter_map(|(id, generation)| sits(nodes, policy, *id, *generation))
        .collect();
    kept.sort_by_key(|node| (!at_home(policy, node), weight(node), node.enrollment.node));
    let mut domains = BTreeSet::new();
    let mut seats: Vec<&NodeRecord> = Vec::new();
    for node in kept {
        if seats.len() == needed {
            break;
        }
        if domains.insert(failure_domain(&node.enrollment, policy.durability.survive)?) {
            seats.push(node);
        }
    }
    let kept = seats.len();
    let candidates = admitted(nodes, policy, min_disk_available);
    let seated = |seats: &[&NodeRecord], node: &NodeRecord| {
        seats
            .iter()
            .any(|seat| seat.enrollment.node == node.enrollment.node)
    };
    for (node, _) in &candidates {
        if seats.len() == needed {
            break;
        }
        if !seated(&seats, node)
            && domains.insert(failure_domain(&node.enrollment, policy.durability.survive)?)
        {
            seats.push(node);
        }
    }
    if seats.len() != needed {
        return Err(DirectoryError::NoPlacement);
    }
    if !seats.iter().any(|node| at_home(policy, node)) {
        // The session is led at home: a node that is there takes the seat
        // given last, which is a voter's that sat only where no seat was
        // vacant.
        let last = seats.pop().ok_or(DirectoryError::NoPlacement)?;
        domains.remove(&failure_domain(
            &last.enrollment,
            policy.durability.survive,
        )?);
        let mut home = None;
        for (node, _) in &candidates {
            if at_home(policy, node)
                && !seated(&seats, node)
                && domains.insert(failure_domain(&node.enrollment, policy.durability.survive)?)
            {
                home = Some(*node);
                break;
            }
        }
        seats.push(home.ok_or(DirectoryError::Residency)?);
    }
    let leader = leading
        .choose(
            seats
                .iter()
                .filter(|node| at_home(policy, node))
                .map(|node| node.enrollment.node),
        )
        .ok_or(DirectoryError::Residency)?;
    let voters: BTreeMap<u64, u64> = seats
        .iter()
        .map(|node| (node.enrollment.node, node.enrollment.generation))
        .collect();
    // The copies that sit and vote stay copies; the rest are the voters
    // in the order they were seated.
    let copies = usize::from(policy.durability.max_failures).saturating_add(1);
    let mut content_copies: BTreeMap<u64, u64> = sitting
        .content_copies
        .keys()
        .filter_map(|id| voters.get_key_value(id))
        .map(|(id, generation)| (*id, *generation))
        .take(copies)
        .collect();
    for node in &seats {
        if content_copies.len() >= copies {
            break;
        }
        content_copies.insert(node.enrollment.node, node.enrollment.generation);
    }
    let spec = PlacementSpec {
        policy: policy.clone(),
        placement: Placement {
            voters: voters.clone(),
            materializers: voters,
            content_copies,
            preferred_leader: leader,
        },
    };
    verify_placement(&spec, nodes, max_members)?;
    Ok(PlacementProposal {
        observations: observed(&seats),
        spec,
        explanation: if kept == needed {
            "Every voter that sits keeps its seat."
        } else {
            "Voters that sit keep their seats; vacant seats went to eligible nodes by home, measured load, free memory, and stable ID, in independent promised failure domains."
        },
    })
}

/// The placement that gives one seat of `session` to a node at home: the
/// seat of the voter that is not, and of those the most loaded. None where
/// the session has no home regions, where every voter is at home, where no
/// node at home could be seated beside the others, and while the session
/// is moving, leaves copies behind or does not hold its placement.
pub fn home_move(
    session: &SessionDescriptor,
    nodes: &BTreeMap<u64, NodeRecord>,
    max_members: usize,
    min_disk_available: u64,
) -> Option<PlacementProposal> {
    let policy = &session.active.policy;
    let placement = &session.active.placement;
    if policy.home_regions.is_empty()
        || session.pending.is_some()
        || !session.retiring.is_empty()
        || verify_placement(&session.active, nodes, max_members).is_err()
    {
        return None;
    }
    let away = placement
        .voters
        .keys()
        .filter_map(|id| nodes.get(id))
        .filter(|node| !at_home(policy, node))
        .max_by_key(|node| (weight(node), node.enrollment.node))?;
    let mut domains = BTreeSet::new();
    for id in placement.voters.keys() {
        if *id != away.enrollment.node {
            let node = nodes.get(id)?;
            domains.insert(failure_domain(&node.enrollment, policy.durability.survive).ok()?);
        }
    }
    let (newcomer, load) = admitted(nodes, policy, min_disk_available)
        .into_iter()
        .find(|(node, _)| {
            at_home(policy, node)
                && !placement.voters.contains_key(&node.enrollment.node)
                && failure_domain(&node.enrollment, policy.durability.survive)
                    .is_ok_and(|domain| !domains.contains(&domain))
        })?;
    let mut desired = session.active.clone();
    let seat = (newcomer.enrollment.node, newcomer.enrollment.generation);
    for members in [
        &mut desired.placement.voters,
        &mut desired.placement.materializers,
        &mut desired.placement.content_copies,
    ] {
        if members.remove(&away.enrollment.node).is_some() {
            members.insert(seat.0, seat.1);
        }
    }
    verify_placement(&desired, nodes, max_members).ok()?;
    Some(PlacementProposal {
        spec: desired,
        observations: BTreeMap::from([(seat.0, load.report)]),
        explanation: "One voter that is not in a home region gave its seat to a node that is.",
    })
}

/// Whether every member of `placement` that the detector has declared dead
/// has been dead for `hold` seconds at `now`: a death that stands. A member
/// that came back has a verdict that says so, and one declared dead again
/// is dead since then. A clock behind a verdict has held nothing.
pub fn deaths_held(
    placement: &Placement,
    nodes: &BTreeMap<u64, NodeRecord>,
    now: i64,
    hold: i64,
) -> bool {
    placement.nodes().iter().all(|id| {
        nodes
            .get(id)
            .and_then(|node| node.liveness)
            .filter(|verdict| !verdict.alive)
            .is_none_or(|verdict| {
                now.checked_sub(verdict.decided_at)
                    .is_some_and(|dead| dead >= hold)
            })
    })
}
