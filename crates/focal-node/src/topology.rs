//! Failure-domain identities (24 §22): a region or zone label an operator
//! declares for a node becomes a directory identity by derivation, the same
//! on every node, so the root registers a region once per label and every
//! grant, plan and fence compares identities, never strings.
use crate::config::Topology;
use focal_directory::{RegionId, ZoneId};

/// The longest label a node announces.
pub const MAX_LABEL_BYTES: usize = focal_wire::MAX_TOPOLOGY_LABEL_BYTES;

fn derive(domain: &str, parts: &[&str]) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new_derive_key(domain);
    for part in parts {
        hasher.update(&(part.len() as u64).to_le_bytes());
        hasher.update(part.as_bytes());
    }
    let mut id = [0; 16];
    for (target, source) in id.iter_mut().zip(hasher.finalize().as_bytes()) {
        *target = *source;
    }
    id
}
/// The region identity a label denotes.
pub fn region_id(label: &str) -> RegionId {
    RegionId(derive("focal.directory.region.v1", &[label]))
}
/// The zone identity a zone label denotes within its region.
pub fn zone_id(region: &str, zone: &str) -> ZoneId {
    ZoneId(derive("focal.directory.zone.v1", &[region, zone]))
}
/// The identities a node's declared topology denotes: unknown (zero) for a
/// label it does not declare; a zone without a region is unknown too.
pub fn ids(topology: &Topology) -> (RegionId, ZoneId) {
    match (&topology.region, &topology.zone) {
        (Some(region), Some(zone)) => (region_id(region), zone_id(region, zone)),
        (Some(region), None) => (region_id(region), ZoneId([0; 16])),
        (None, _) => (RegionId::UNKNOWN, ZoneId([0; 16])),
    }
}
/// The identities two announced labels denote (`ids` over a contact row).
pub fn label_ids(region: Option<&str>, zone: Option<&str>) -> (RegionId, ZoneId) {
    match (region, zone) {
        (Some(region), Some(zone)) => (region_id(region), zone_id(region, zone)),
        (Some(region), None) => (region_id(region), ZoneId([0; 16])),
        (None, _) => (RegionId::UNKNOWN, ZoneId([0; 16])),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn identities_are_stable_distinct_and_unknown_without_a_region() {
        assert_eq!(region_id("eu-a"), region_id("eu-a"));
        assert_ne!(region_id("eu-a"), region_id("eu-b"));
        assert_ne!(zone_id("eu-a", "1"), zone_id("eu-b", "1"));
        assert_ne!(zone_id("eu-a", "1"), zone_id("eu-a", "2"));
        assert_ne!(region_id("ab"), RegionId::UNKNOWN);
        let none = Topology {
            zone: Some("1".into()),
            region: None,
        };
        assert_eq!(ids(&none), (RegionId::UNKNOWN, ZoneId([0; 16])));
        let region_only = Topology {
            zone: None,
            region: Some("eu-a".into()),
        };
        assert_eq!(ids(&region_only), (region_id("eu-a"), ZoneId([0; 16])));
        assert_eq!(
            label_ids(Some("eu-a"), Some("1")),
            (region_id("eu-a"), zone_id("eu-a", "1"))
        );
    }
}
