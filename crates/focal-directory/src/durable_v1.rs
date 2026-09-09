//! Frozen native placement representations used by the Session V1 codecs.
//!
//! Explicit field order and enum ordinals preserve the original Postcard bytes.
//! Decode moves directly into the owned domain values without re-running current
//! placement or enrollment admission. Enclosing readers own memory admission,
//! version selection and complete body consumption before publication.
use crate::{
    ClusterId, DelegationFence, DurabilityIntent, FailureClass, LogGroupId, NamespaceKey,
    NamespaceRange, NodeEnrollment, NodeLoad, NodeRecord, OperationId, PartitionId, Placement,
    PlacementPolicy, PlacementSpec, RegionId, ReplicaReady, SessionFence, SessionFenceKind, WorkId,
    ZoneId,
};
use focal_model::durable_v1::{Ref, V1, Value};
use focal_model::{ContentHash, LedgerId, RaftIndex, RaftTerm, RouteEpoch, SessionSeq};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::{BTreeMap, BTreeSet};

macro_rules! v1_struct {
    ($name:ident { $($field:ident: $ty:ty),+ $(,)? }) => {
        impl V1 for $name {
            #[inline]
            fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                #[derive(Serialize)]
                struct Record<'a> { $($field: Ref<'a, $ty>),+ }
                let Self { $($field),+ } = self;
                Record { $($field: Ref($field)),+ }.serialize(serializer)
            }
            fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                #[derive(Deserialize)]
                struct Record { $($field: Value<$ty>),+ }
                let Record { $($field),+ } = Record::deserialize(deserializer)?;
                Ok(Self { $($field: $field.0),+ })
            }
        }
    };
}

/// A frozen row whose live type gained fields after the V1 writer was frozen.
/// The V1 representation carries only the original fields; restored values
/// take the stated defaults for the later ones.
macro_rules! v1_struct_later {
    ($name:ident { $($field:ident: $ty:ty),+ $(,)? } later { $($later:ident: $default:expr),+ $(,)? }) => {
        impl V1 for $name {
            #[inline]
            fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                #[derive(Serialize)]
                struct Record<'a> { $($field: Ref<'a, $ty>),+ }
                let Self { $($field,)+ $($later: _,)+ } = self;
                Record { $($field: Ref($field)),+ }.serialize(serializer)
            }
            fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                #[derive(Deserialize)]
                struct Record { $($field: Value<$ty>),+ }
                let Record { $($field),+ } = Record::deserialize(deserializer)?;
                Ok(Self { $($field: $field.0,)+ $($later: $default,)+ })
            }
        }
    };
}

macro_rules! v1_unit_enum {
    ($name:ident { $($variant:ident),+ $(,)? }) => {
        impl V1 for $name {
            #[inline]
            fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                #[derive(Serialize)]
                enum Record { $($variant),+ }
                match self { $(Self::$variant => Record::$variant),+ }.serialize(serializer)
            }
            fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                #[derive(Deserialize)]
                enum Record { $($variant),+ }
                Ok(match Record::deserialize(deserializer)? { $(Record::$variant => Self::$variant),+ })
            }
        }
    };
}

// Raw fixed-size arrays are primitive shapes, not live domain Serde contracts.
macro_rules! v1_identifier {
    ($inner:ty; $($name:ident),+ $(,)?) => {$ (
        impl V1 for $name {
            #[inline]
            fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                let Self(bytes) = self;
                serializer.serialize_newtype_struct(stringify!($name), bytes)
            }
            fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                #[derive(Deserialize)]
                struct Raw($inner);
                let Raw(bytes) = Raw::deserialize(deserializer)?;
                Ok(Self(bytes))
            }
        }
    )+};
}

v1_identifier!([u8; 16]; ClusterId, RegionId, ZoneId, PartitionId, LogGroupId, OperationId, WorkId);
v1_identifier!([u8; 32]; NamespaceKey);

v1_struct!(NamespaceRange { start: NamespaceKey, end: Option<NamespaceKey> });
v1_struct!(NodeEnrollment {
    node: u64,
    generation: u64,
    region: RegionId,
    zone: ZoneId,
    endpoint: String,
    identity: ContentHash,
    authority_epoch: u64,
    attestation: ContentHash,
    eligible: bool,
});
v1_struct_later!(NodeLoad {
    node: u64,
    generation: u64,
    report: u64,
    available_memory: u64,
    active_weight: u64
} later {
    disk_available: 0
});
v1_struct_later!(NodeRecord { enrollment: NodeEnrollment, load: Option<NodeLoad> } later {
    liveness: None
});

v1_unit_enum!(SessionFenceKind {
    Created,
    Cutover,
    Activated
});
v1_struct!(SessionFence {
    kind: SessionFenceKind,
    ledger: LedgerId,
    log_group: LogGroupId,
    operation: OperationId,
    sequence: SessionSeq,
    index: RaftIndex,
    term: RaftTerm,
    from_route: RouteEpoch,
    to_route: RouteEpoch,
    membership_epoch: u64,
    placement_epoch: u64,
    placement_digest: ContentHash,
    record_hash: ContentHash,
});
v1_struct!(ReplicaReady {
    ledger: LedgerId,
    operation: OperationId,
    route_epoch: RouteEpoch,
    node: u64,
    node_generation: u64,
    through: SessionSeq,
    custody: ContentHash,
    attestation: ContentHash,
});
v1_struct!(DelegationFence {
    cluster: ClusterId,
    operation: OperationId,
    source: PartitionId,
    destination: PartitionId,
    namespace: NamespaceRange,
    from_epoch: u64,
    to_epoch: u64,
    sealed_revision: u64,
    checkpoint: ContentHash,
    destination_ready: ContentHash,
});

v1_unit_enum!(FailureClass { Node, Zone, Region });
v1_struct!(DurabilityIntent {
    survive: FailureClass,
    max_failures: u16
});
v1_struct!(PlacementPolicy {
    durability: DurabilityIntent,
    residency: BTreeSet<RegionId>,
    home_regions: BTreeSet<RegionId>,
    required_memory: u64,
});
v1_struct!(Placement {
    voters: BTreeMap<u64, u64>,
    materializers: BTreeMap<u64, u64>,
    content_copies: BTreeMap<u64, u64>,
    preferred_leader: u64,
});
v1_struct!(PlacementSpec {
    policy: PlacementPolicy,
    placement: Placement
});

#[cfg(test)]
#[path = "durable_v1_tests.rs"]
mod tests;
