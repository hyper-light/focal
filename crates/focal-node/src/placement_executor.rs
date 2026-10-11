//! The residency fence (24 §22): `placement.residency` is a hard boundary
//! for every durable copy of a session — log replicas, artifacts,
//! checkpoints and seeds, archive bundles, and derived copies — checked at
//! every transfer's initiation, so no byte moves to a node outside the
//! boundary, not even for a moment. The planner keeps placements inside
//! residency; the fence is what refuses everything the planner did not
//! decide: repairs, pushes, pulls and operator-directed moves.
use focal_directory::RegionId;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ExecutorError {
    #[error("node {node} lies outside the session's residency")]
    OutsideResidency { node: u64, region: RegionId },
    #[error("executor bounds exceeded")]
    Capacity,
}
/// A session's residency and the region each of its placement's members
/// reported: every transfer the fence judges is to or from a member (a copy
/// or a voter), so the regions of the cluster's other nodes are not its to
/// hold. It held every node's, a copy of the node table per hosted session.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResidencyFence {
    residency: BTreeSet<RegionId>,
    regions: BTreeMap<u64, RegionId>,
}
impl ResidencyFence {
    pub fn new(
        residency: BTreeSet<RegionId>,
        regions: BTreeMap<u64, RegionId>,
    ) -> Result<Self, ExecutorError> {
        if residency.len() > 1024 || regions.len() > 65536 {
            return Err(ExecutorError::Capacity);
        }
        Ok(Self { residency, regions })
    }
    /// The fence of a placement of `members`, each with the region `regions`
    /// names for it (a member that reported none is of unknown region, outside
    /// every boundary).
    pub fn for_members<'a>(
        residency: BTreeSet<RegionId>,
        members: impl IntoIterator<Item = &'a u64>,
        regions: &BTreeMap<u64, RegionId>,
    ) -> Result<Self, ExecutorError> {
        let mut known = BTreeMap::new();
        for member in members {
            if let Some(region) = regions.get(member) {
                known.insert(*member, *region);
            }
        }
        Self::new(residency, known)
    }
    /// No boundary: every node may hold a copy.
    pub fn open() -> Self {
        Self::default()
    }
    pub fn residency(&self) -> &BTreeSet<RegionId> {
        &self.residency
    }
    /// The region `node` reported, unknown when it never did.
    pub fn region(&self, node: u64) -> RegionId {
        self.regions
            .get(&node)
            .copied()
            .unwrap_or(RegionId::UNKNOWN)
    }
    /// Whether a copy may move to `node`: always without a boundary, else
    /// only when the node reported a region inside it; a node of unknown
    /// region is outside every boundary.
    pub fn admits(&self, node: u64) -> bool {
        self.residency.is_empty() || self.residency.contains(&self.region(node))
    }
    pub fn check(&self, node: u64) -> Result<(), ExecutorError> {
        if self.admits(node) {
            Ok(())
        } else {
            Err(ExecutorError::OutsideResidency {
                node,
                region: self.region(node),
            })
        }
    }
    /// The bytes this fence holds, for the placement's charge: its two maps'
    /// nodes (`focal_memory::map_bytes`).
    pub fn bytes(&self) -> Result<usize, ExecutorError> {
        focal_memory::map_bytes::<RegionId, ()>(self.residency.len())
            .and_then(|n| {
                n.checked_add(focal_memory::map_bytes::<u64, RegionId>(
                    self.regions.len(),
                )?)
            })
            .ok_or(ExecutorError::Capacity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn the_fence_admits_inside_refuses_outside_and_unknown_and_is_open_without_a_boundary() {
        let a = RegionId([1; 16]);
        let b = RegionId([2; 16]);
        let regions = BTreeMap::from([(1, a), (2, b)]);
        let fence = ResidencyFence::new(BTreeSet::from([a]), regions.clone()).unwrap();
        assert!(fence.admits(1));
        assert!(!fence.admits(2));
        assert!(!fence.admits(3));
        assert_eq!(
            fence.check(2),
            Err(ExecutorError::OutsideResidency { node: 2, region: b })
        );
        assert_eq!(
            fence.check(3),
            Err(ExecutorError::OutsideResidency {
                node: 3,
                region: RegionId::UNKNOWN
            })
        );
        let open = ResidencyFence::new(BTreeSet::new(), regions).unwrap();
        assert!(open.admits(2) && open.admits(3));
        assert!(ResidencyFence::open().admits(9));
        assert!(fence.bytes().unwrap() > 0);
    }
    #[test]
    fn a_placements_fence_holds_its_members_regions_alone() {
        let a = RegionId([1; 16]);
        let regions: BTreeMap<u64, RegionId> = (1..=1000).map(|node| (node, a)).collect();
        let fence =
            ResidencyFence::for_members(BTreeSet::from([a]), [3, 7, 9, 2000].iter(), &regions)
                .unwrap();
        assert!(fence.admits(3) && fence.admits(7) && fence.admits(9));
        // A member that reported no region is outside the boundary.
        assert!(!fence.admits(2000));
        // Three regions held, whatever the cluster's size.
        assert_eq!(
            fence.bytes().unwrap(),
            focal_memory::map_bytes::<RegionId, ()>(1).unwrap()
                + focal_memory::map_bytes::<u64, RegionId>(3).unwrap()
        );
    }
}
