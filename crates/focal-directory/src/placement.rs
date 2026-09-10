use crate::*;
use focal_model::ContentHash;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FailureClass {
    Node,
    Zone,
    Region,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurabilityIntent {
    pub survive: FailureClass,
    pub max_failures: u16,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementPolicy {
    pub durability: DurabilityIntent,
    pub residency: BTreeSet<RegionId>,
    pub home_regions: BTreeSet<RegionId>,
    pub required_memory: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Placement {
    /// Node identity -> exact enrolled incarnation.
    pub voters: BTreeMap<u64, u64>,
    pub materializers: BTreeMap<u64, u64>,
    pub content_copies: BTreeMap<u64, u64>,
    pub preferred_leader: u64,
}
impl Placement {
    pub fn nodes(&self) -> BTreeSet<u64> {
        self.voters
            .keys()
            .chain(self.materializers.keys())
            .chain(self.content_copies.keys())
            .copied()
            .collect()
    }
    pub fn generation(&self, node: u64) -> Option<u64> {
        self.voters
            .get(&node)
            .or_else(|| self.materializers.get(&node))
            .or_else(|| self.content_copies.get(&node))
            .copied()
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementSpec {
    pub policy: PlacementPolicy,
    pub placement: Placement,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementProposal {
    pub spec: PlacementSpec,
    /// Exact load-report epochs used by the deterministic plan.
    pub observations: BTreeMap<u64, u64>,
    pub explanation: &'static str,
}

/// Canonical versioned digest of the exact policy and placement. It binds a
/// committed session activation to the directory proposal, not to current load.
pub fn placement_digest(spec: &PlacementSpec) -> Result<ContentHash, DirectoryError> {
    let original = focal_model::durable_v1::Ref(spec);
    let encoded_len = postcard::experimental::serialized_size(&original)
        .map_err(|_| DirectoryError::Invalid("placement codec"))?;
    if encoded_len > 1024 * 1024 {
        return Err(DirectoryError::Capacity);
    }
    crate::digest(b"focal:placement:v1\0", &original)
}

/// Construct a minimum independent-domain placement from measured load and
/// declared constraints. A proposal executes no join, transfer, or promotion.
/// Nodes reporting less than `min_disk_available` bytes of headroom are not
/// candidates: a copy that cannot be installed is never planned.
pub fn propose_placement(
    nodes: &BTreeMap<u64, NodeRecord>,
    policy: &PlacementPolicy,
    max_members: usize,
    min_disk_available: u64,
) -> Result<PlacementProposal, DirectoryError> {
    propose_placement_keeping(
        nodes,
        policy,
        &BTreeMap::new(),
        max_members,
        min_disk_available,
    )
}
/// [`propose_placement`] preferring `incumbents` (the active placement's
/// voters, by node) among equally eligible candidates, so an expansion adds
/// hosts to the copies that exist and a heal moves only what it must
/// ([24](../../../docs/archictecutre/24-placement-execution-and-fleet-control.md) §9, §19).
/// Home-region preference still comes first; an incumbent that is no longer
/// eligible, alive or reporting is not a candidate at all.
pub fn propose_placement_keeping(
    nodes: &BTreeMap<u64, NodeRecord>,
    policy: &PlacementPolicy,
    incumbents: &BTreeMap<u64, u64>,
    max_members: usize,
    min_disk_available: u64,
) -> Result<PlacementProposal, DirectoryError> {
    let needed = usize::from(policy.durability.max_failures)
        .checked_mul(2)
        .and_then(|n| n.checked_add(1))
        .ok_or(DirectoryError::CounterExhausted)?;
    if needed > max_members {
        return Err(DirectoryError::Capacity);
    }
    let mut candidates: Vec<_> = nodes
        .values()
        .filter_map(|node| {
            let load = node.load?;
            (node.enrollment.eligible
                && node.is_alive()
                && failure_domain(&node.enrollment, policy.durability.survive).is_ok()
                && (policy.residency.is_empty()
                    || policy.residency.contains(&node.enrollment.region))
                && load.generation == node.enrollment.generation
                && load.available_memory >= policy.required_memory
                && load.disk_available >= min_disk_available)
                .then_some((node, load))
        })
        .collect();
    candidates.sort_by_key(|(node, load)| {
        (
            !policy.home_regions.is_empty()
                && !policy.home_regions.contains(&node.enrollment.region),
            !incumbents.contains_key(&node.enrollment.node),
            load.active_weight,
            std::cmp::Reverse(load.available_memory),
            std::cmp::Reverse(load.disk_available),
            node.enrollment.node,
        )
    });
    let mut domains = BTreeSet::new();
    let mut selected = Vec::new();
    for (node, load) in candidates {
        if domains.insert(failure_domain(&node.enrollment, policy.durability.survive)?) {
            selected.push((node, load));
        }
        if selected.len() == needed {
            break;
        }
    }
    if selected.len() != needed {
        return Err(DirectoryError::NoPlacement);
    }
    let leader = selected
        .iter()
        .find(|(node, _)| {
            policy.home_regions.is_empty() || policy.home_regions.contains(&node.enrollment.region)
        })
        .ok_or(DirectoryError::Residency)?
        .0
        .enrollment
        .node;
    let voters: BTreeMap<_, _> = selected
        .iter()
        .map(|(node, _)| (node.enrollment.node, node.enrollment.generation))
        .collect();
    let content_copies = selected
        .iter()
        .take(usize::from(policy.durability.max_failures) + 1)
        .map(|(node, _)| (node.enrollment.node, node.enrollment.generation))
        .collect();
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
        observations: selected
            .iter()
            .map(|(node, load)| (node.enrollment.node, load.report))
            .collect(),
        spec,
        explanation: "Selected eligible nodes by ordering home, incumbency, measured load, free memory, and stable ID; selected independent promised failure domains.",
    })
}

/// Worst-case domain loss is the sum of the f largest domain occupancies.
/// Checking that bound proves every failure subset of size at most f without
/// exponential enumeration. Log quorum and data custody are separate checks.
pub fn verify_placement(
    spec: &PlacementSpec,
    nodes: &BTreeMap<u64, NodeRecord>,
    max_members: usize,
) -> Result<(), DirectoryError> {
    if spec
        .policy
        .residency
        .iter()
        .chain(&spec.policy.home_regions)
        .any(|region| region.0 == [0; 16])
    {
        return Err(DirectoryError::Invalid(
            "unknown region in placement policy",
        ));
    }
    let placement = &spec.placement;
    for members in [
        &placement.voters,
        &placement.materializers,
        &placement.content_copies,
    ] {
        if members.is_empty() || members.len() > max_members {
            return Err(DirectoryError::Capacity);
        }
        for (id, generation) in members {
            let node = nodes.get(id).ok_or(DirectoryError::Missing)?;
            if !node.enrollment.eligible || node.enrollment.generation != *generation {
                return Err(DirectoryError::StaleNode);
            }
            if !node.is_alive() {
                return Err(DirectoryError::DeadNode);
            }
            if !spec.policy.residency.is_empty()
                && !spec.policy.residency.contains(&node.enrollment.region)
            {
                return Err(DirectoryError::Residency);
            }
            if placement.generation(*id) != Some(*generation) {
                return Err(DirectoryError::StaleNode);
            }
        }
    }
    let remaining = |members: &BTreeMap<u64, u64>| -> Result<usize, DirectoryError> {
        let mut counts = BTreeMap::new();
        for node in members.keys() {
            let count = counts
                .entry(failure_domain(
                    &nodes.get(node).ok_or(DirectoryError::Missing)?.enrollment,
                    spec.policy.durability.survive,
                )?)
                .or_insert(0usize);
            *count = count.checked_add(1).ok_or(DirectoryError::Capacity)?;
        }
        let mut counts: Vec<_> = counts.into_values().collect();
        counts.sort_unstable_by(|a, b| b.cmp(a));
        let lost: usize = counts
            .iter()
            .take(usize::from(spec.policy.durability.max_failures))
            .try_fold(0usize, |sum, count| sum.checked_add(*count))
            .ok_or(DirectoryError::Capacity)?;
        Ok(members.len().saturating_sub(lost))
    };
    if remaining(&placement.voters)? < placement.voters.len().saturating_div(2).saturating_add(1) {
        return Err(DirectoryError::Quorum);
    }
    if remaining(&placement.content_copies)? == 0 {
        return Err(DirectoryError::Custody);
    }
    if remaining(&placement.materializers)? == 0 {
        return Err(DirectoryError::NotReady);
    }
    let leader = nodes
        .get(&placement.preferred_leader)
        .ok_or(DirectoryError::Missing)?;
    if !placement.voters.contains_key(&placement.preferred_leader)
        || (!spec.policy.home_regions.is_empty()
            && !spec.policy.home_regions.contains(&leader.enrollment.region))
    {
        return Err(DirectoryError::Residency);
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Domain {
    Node(u64),
    Zone(RegionId, ZoneId),
    Region(RegionId),
}
pub(crate) fn failure_domain(
    node: &NodeEnrollment,
    class: FailureClass,
) -> Result<Domain, DirectoryError> {
    match class {
        FailureClass::Node => Ok(Domain::Node(node.node)),
        FailureClass::Zone if node.region.0 != [0; 16] && node.zone.0 != [0; 16] => {
            Ok(Domain::Zone(node.region, node.zone))
        }
        FailureClass::Region if node.region.0 != [0; 16] => Ok(Domain::Region(node.region)),
        _ => Err(DirectoryError::Invalid("missing promised failure domain")),
    }
}

pub(crate) fn spec_charge(spec: &PlacementSpec) -> Result<usize, DirectoryError> {
    let count = add(
        spec.placement.voters.len(),
        add(
            spec.placement.materializers.len(),
            spec.placement.content_copies.len(),
        )?,
    )?;
    add(
        mul(count, tree_row::<(u64, u64)>())?,
        mul(
            add(spec.policy.residency.len(), spec.policy.home_regions.len())?,
            tree_row::<RegionId>(),
        )?,
    )
}
