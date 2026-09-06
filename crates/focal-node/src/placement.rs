use crate::config::{Durability, FailureDomain, Placement, Topology};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeFacts {
    pub id: u64,
    pub topology: Topology,
    /// Set by the enrolled infrastructure authority, not a work request's labels.
    pub verified: bool,
    pub eligible: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementPlan {
    pub voters: Vec<u64>,
    pub content_copies: Vec<u64>,
    pub preferred_leader: u64,
    pub durability: Durability,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PlacementError {
    #[error("duplicate or zero node identity")]
    Identity,
    #[error("node {0} lacks verified failure-domain facts")]
    Unverified(u64),
    #[error("node {0} has no required failure-domain label")]
    MissingDomain(u64),
    #[error("placement needs {required} independent domains, but only {available} are eligible")]
    Insufficient { required: usize, available: usize },
    #[error("no ordering home satisfies the residency constraint")]
    NoHome,
    #[error("assignment would lose voter quorum under a promised failure")]
    Quorum,
    #[error("assignment would lose all evidence copies under a promised failure")]
    Evidence,
    #[error("assignment includes a missing, ineligible, or out-of-residency node")]
    Ineligible,
    #[error("placement counters exceed representable capacity")]
    Capacity,
}

fn domain(node: &NodeFacts, class: FailureDomain) -> Result<String, PlacementError> {
    if !node.verified {
        return Err(PlacementError::Unverified(node.id));
    }
    Ok(match class {
        FailureDomain::Node => format!("node:{}", node.id),
        FailureDomain::Zone => format!(
            "zone:{}:{}",
            node.topology
                .region
                .as_ref()
                .ok_or(PlacementError::MissingDomain(node.id))?,
            node.topology
                .zone
                .as_ref()
                .ok_or(PlacementError::MissingDomain(node.id))?
        ),
        FailureDomain::Region => format!(
            "region:{}",
            node.topology
                .region
                .as_ref()
                .ok_or(PlacementError::MissingDomain(node.id))?
        ),
    })
}
fn in_residency(node: &NodeFacts, placement: &Placement) -> bool {
    placement.residency.is_empty()
        || node
            .topology
            .region
            .as_ref()
            .is_some_and(|r| placement.residency.contains(r))
}
fn is_home(node: &NodeFacts, placement: &Placement) -> bool {
    placement.home_regions.is_empty()
        || node
            .topology
            .region
            .as_ref()
            .is_some_and(|r| placement.home_regions.contains(r))
}

/// A minimum-size balanced assignment. Node load ordering can be supplied by a caller
/// in a future placement policy; stable ID order currently makes every decision replayable.
pub fn plan(
    nodes: &[NodeFacts],
    durability: &Durability,
    placement: &Placement,
) -> Result<PlacementPlan, PlacementError> {
    let mut ids = BTreeSet::new();
    for node in nodes {
        if node.id == 0 || !ids.insert(node.id) {
            return Err(PlacementError::Identity);
        }
    }
    let mut candidates: Vec<_> = nodes
        .iter()
        .filter(|n| n.eligible && in_residency(n, placement))
        .collect();
    candidates.sort_by_key(|n| (!is_home(n, placement), n.id));
    let needed = usize::from(durability.max_failures)
        .checked_mul(2)
        .and_then(|n| n.checked_add(1))
        .ok_or(PlacementError::Capacity)?;
    let mut domains = BTreeSet::new();
    let mut selected = Vec::new();
    for node in candidates {
        if domains.insert(domain(node, durability.survive)?) {
            selected.push(node);
        }
    }
    if selected.len() < needed {
        return Err(PlacementError::Insufficient {
            required: needed,
            available: selected.len(),
        });
    }
    selected.truncate(needed);
    let preferred_leader = selected
        .iter()
        .find(|n| is_home(n, placement))
        .ok_or(PlacementError::NoHome)?
        .id;
    let voters = selected.iter().map(|n| n.id).collect();
    let content_copies = selected
        .iter()
        .take(
            usize::from(durability.max_failures)
                .checked_add(1)
                .ok_or(PlacementError::Capacity)?,
        )
        .map(|n| n.id)
        .collect();
    let result = PlacementPlan {
        voters,
        content_copies,
        preferred_leader,
        durability: durability.clone(),
    };
    verify(&result, nodes, placement)?;
    Ok(result)
}

/// Verifies every failure set without exponential enumeration: losing the f domains
/// with most votes/copies is the worst case for each independent safety obligation.
pub fn verify(
    plan: &PlacementPlan,
    nodes: &[NodeFacts],
    placement: &Placement,
) -> Result<(), PlacementError> {
    let all: BTreeMap<_, _> = nodes.iter().map(|n| (n.id, n)).collect();
    if all.len() != nodes.len() || all.contains_key(&0) {
        return Err(PlacementError::Identity);
    }
    let surviving = |members: &[u64]| -> Result<usize, PlacementError> {
        if members.iter().collect::<BTreeSet<_>>().len() != members.len() {
            return Err(PlacementError::Identity);
        }
        let mut groups = BTreeMap::<String, usize>::new();
        for id in members {
            let node = all
                .get(id)
                .filter(|n| n.eligible && in_residency(n, placement))
                .ok_or(PlacementError::Ineligible)?;
            let count = groups
                .entry(domain(node, plan.durability.survive)?)
                .or_default();
            *count = count.checked_add(1).ok_or(PlacementError::Capacity)?;
        }
        let mut counts: Vec<_> = groups.values().copied().collect();
        counts.sort_unstable_by(|a, b| b.cmp(a));
        let lost = counts
            .into_iter()
            .take(usize::from(plan.durability.max_failures))
            .try_fold(0usize, |total, count| total.checked_add(count))
            .ok_or(PlacementError::Capacity)?;
        members
            .len()
            .checked_sub(lost)
            .ok_or(PlacementError::Capacity)
    };
    if plan.voters.is_empty()
        || surviving(&plan.voters)?
            < plan
                .voters
                .len()
                .checked_div(2)
                .and_then(|n| n.checked_add(1))
                .ok_or(PlacementError::Capacity)?
    {
        return Err(PlacementError::Quorum);
    }
    if surviving(&plan.content_copies)? == 0 {
        return Err(PlacementError::Evidence);
    }
    if !plan.voters.contains(&plan.preferred_leader)
        || !all
            .get(&plan.preferred_leader)
            .is_some_and(|n| is_home(n, placement))
    {
        return Err(PlacementError::NoHome);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn node(id: u64, region: &str) -> NodeFacts {
        NodeFacts {
            id,
            topology: Topology {
                region: Some(region.into()),
                zone: Some(format!("zone-{id}")),
            },
            verified: true,
            eligible: true,
        }
    }
    #[test]
    fn local_and_three_regions_share_one_solver() {
        let nodes = vec![node(1, "a"), node(2, "b"), node(3, "c")];
        assert_eq!(
            plan(&nodes[..1], &Durability::default(), &Placement::default())
                .unwrap()
                .voters,
            vec![1]
        );
        let regional = Durability {
            survive: FailureDomain::Region,
            max_failures: 1,
        };
        assert!(matches!(
            plan(&nodes[..2], &regional, &Placement::default()),
            Err(PlacementError::Insufficient {
                required: 3,
                available: 2
            })
        ));
        let p = plan(&nodes, &regional, &Placement::default()).unwrap();
        assert_eq!(p.voters, vec![1, 2, 3]);
        assert_eq!(p.content_copies, vec![1, 2]);
    }
    #[test]
    fn uneven_three_regions_safe_but_two_region_majority_not_symmetric() {
        let nodes = vec![
            node(1, "a"),
            node(2, "a"),
            node(3, "b"),
            node(4, "b"),
            node(5, "c"),
        ];
        let durability = Durability {
            survive: FailureDomain::Region,
            max_failures: 1,
        };
        let p = PlacementPlan {
            voters: vec![1, 2, 3, 4, 5],
            content_copies: vec![1, 3],
            preferred_leader: 1,
            durability: durability.clone(),
        };
        verify(&p, &nodes, &Placement::default()).unwrap();
        let mut bad = p.clone();
        bad.voters = vec![1, 2, 3];
        assert_eq!(
            verify(&bad, &nodes, &Placement::default()),
            Err(PlacementError::Quorum)
        );
        let mut bad = p;
        bad.content_copies = vec![1, 2];
        assert_eq!(
            verify(&bad, &nodes, &Placement::default()),
            Err(PlacementError::Evidence)
        );
    }
    #[test]
    fn residency_and_verified_topology_are_hard_constraints() {
        let mut nodes = vec![node(1, "a"), node(2, "b"), node(3, "c")];
        let placement = Placement {
            home_regions: vec!["b".into()],
            residency: vec!["a".into(), "b".into(), "c".into()],
        };
        let p = plan(&nodes, &Durability::default(), &placement).unwrap();
        assert_eq!(p.preferred_leader, 2);
        nodes[1].verified = false;
        assert!(matches!(
            plan(&nodes, &Durability::default(), &placement),
            Err(PlacementError::Unverified(2))
        ));
    }
}
