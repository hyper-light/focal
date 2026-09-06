#![allow(clippy::unwrap_used, clippy::disallowed_macros)]

use focal_model::{ParticipantId, TenantId};
use focal_wire::{AccessError, PeerGrant, PeerRegistry, PeerRole};
use std::collections::{BTreeMap, BTreeSet};

fn grant(principal: u128) -> PeerGrant {
    PeerGrant {
        principal: ParticipantId::from_u128(principal),
        tenants: BTreeSet::from([TenantId::from_u128(1)]),
        role: PeerRole::Node { node_id: 7 },
    }
}

#[test]
fn invalid_projection_preserves_all_existing_grants() {
    let registry = PeerRegistry::new(2).unwrap();
    registry
        .replace_grants(BTreeMap::from([([1; 32], grant(1)), ([2; 32], grant(2))]))
        .unwrap();
    for (candidate, expected) in [
        (
            BTreeMap::from([([3; 32], grant(3)), ([0; 32], grant(4))]),
            AccessError::InvalidRequest,
        ),
        (
            BTreeMap::from([([3; 32], grant(3)), ([4; 32], grant(0))]),
            AccessError::InvalidRequest,
        ),
        (
            BTreeMap::from([
                ([3; 32], grant(3)),
                ([4; 32], grant(4)),
                ([5; 32], grant(5)),
            ]),
            AccessError::Capacity,
        ),
    ] {
        assert_eq!(registry.replace_grants(candidate), Err(expected));
        assert_eq!(
            registry.authenticate([1; 32]).unwrap().principal(),
            ParticipantId::from_u128(1)
        );
        assert_eq!(
            registry.authenticate([2; 32]).unwrap().principal(),
            ParticipantId::from_u128(2)
        );
        assert!(matches!(
            registry.authenticate([3; 32]),
            Err(AccessError::Unauthorized)
        ));
    }
}

#[test]
fn existing_connection_registry_observes_complete_replacement_and_revocation() {
    let registry = PeerRegistry::new(2).unwrap();
    let connection_registry = registry.clone();
    registry
        .replace_grants(BTreeMap::from([([1; 32], grant(1)), ([2; 32], grant(2))]))
        .unwrap();
    registry
        .replace_grants(BTreeMap::from([([2; 32], grant(20)), ([3; 32], grant(3))]))
        .unwrap();
    assert!(matches!(
        connection_registry.authenticate([1; 32]),
        Err(AccessError::Unauthorized)
    ));
    assert_eq!(
        connection_registry
            .authenticate([2; 32])
            .unwrap()
            .principal(),
        ParticipantId::from_u128(20)
    );
    assert_eq!(
        connection_registry
            .authenticate([3; 32])
            .unwrap()
            .certificate_fingerprint(),
        Some([3; 32])
    );
    registry.replace_grants(BTreeMap::new()).unwrap();
    assert!(matches!(
        connection_registry.authenticate([2; 32]),
        Err(AccessError::Unauthorized)
    ));
    assert!(matches!(
        connection_registry.authenticate([3; 32]),
        Err(AccessError::Unauthorized)
    ));
}
