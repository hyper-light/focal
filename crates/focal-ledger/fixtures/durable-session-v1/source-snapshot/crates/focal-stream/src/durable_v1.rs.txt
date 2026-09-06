//! Explicit original native-cursor Postcard representation.
//!
//! These implementations are used through focal_model::durable_v1::{Ref, Value}.
//! They preserve stored fields, enum ordinals and collection semantics without
//! applying current registry admission. The enclosing ledger owns format selection,
//! recovery memory admission and exact body consumption before publication.
use crate::{
    ConsumerId, ConsumerKey, CursorCheckpoint, CursorCommand, CursorMode, CursorOperation,
    CursorRecord, CursorToken, DeltaFilter, Position, PositionOffset, ResyncReason,
};
use focal_model::durable_v1::{Ref, V1, Value};
use focal_model::{ClaimId, ContentHash, LedgerId, SessionSeq};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::{BTreeMap, BTreeSet};

// Explicit field lists, with exhaustive live-value destructuring. Nested values
// always use their own frozen codecs, never the current type's Serde derive.
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

// Variant order is the original Postcard ordinal. Unit and named-field variants
// retain their shapes; adding a live variant requires an explicit compatibility
// decision rather than silently extending the historical format.
macro_rules! v1_struct_enum {
    ($name:ident { $($variant:ident $( { $($field:ident: $ty:ty),+ $(,)? } )?),+ $(,)? }) => {
        impl V1 for $name {
            #[inline]
            fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                #[derive(Serialize)]
                enum Record<'a> {
                    $($variant $( { $($field: Ref<'a, $ty>),+ } )?),+
                }
                let record = match self {
                    $(Self::$variant $( { $($field),+ } )? => Record::$variant $( { $($field: Ref($field)),+ } )?),+
                };
                record.serialize(serializer)
            }
            fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                #[derive(Deserialize)]
                enum Record {
                    $($variant $( { $($field: Value<$ty>),+ } )?),+
                }
                Ok(match Record::deserialize(deserializer)? {
                    $(Record::$variant $( { $($field),+ } )? => Self::$variant $( { $($field: $field.0),+ } )?),+
                })
            }
        }
    };
}

impl V1 for ConsumerId {
    #[inline]
    fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let Self(bytes) = self;
        Ref(bytes).serialize(serializer)
    }
    fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let Value(bytes) = Value::<[u8; 16]>::deserialize(deserializer)?;
        Ok(Self(bytes))
    }
}

v1_struct!(ConsumerKey {
    ledger: LedgerId,
    consumer: ConsumerId,
});
v1_struct!(Position {
    ledger: LedgerId,
    sequence: SessionSeq,
    offset: PositionOffset,
});
v1_struct!(CursorToken {
    key: ConsumerKey,
    generation: u64,
    scope: ContentHash,
    position: Position,
});
v1_struct!(CursorRecord {
    token: CursorToken,
    filter: DeltaFilter,
    expires_at: u64,
    mode: CursorMode,
});
v1_struct!(CursorCheckpoint {
    schema: u16,
    ledger: LedgerId,
    revision: u64,
    clock: u64,
    floor: SessionSeq,
    consumers: BTreeMap<ConsumerId, CursorRecord>,
});
v1_struct!(CursorCommand {
    expected_revision: u64,
    now: u64,
    operation: CursorOperation,
});

impl V1 for PositionOffset {
    #[inline]
    fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        enum Record<'a> {
            Delta(Ref<'a, u32>),
            Resolved,
        }
        match self {
            Self::Delta(ordinal) => Record::Delta(Ref(ordinal)),
            Self::Resolved => Record::Resolved,
        }
        .serialize(serializer)
    }
    fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        enum Record {
            Delta(Value<u32>),
            Resolved,
        }
        Ok(match Record::deserialize(deserializer)? {
            Record::Delta(Value(ordinal)) => Self::Delta(ordinal),
            Record::Resolved => Self::Resolved,
        })
    }
}

impl V1 for DeltaFilter {
    #[inline]
    fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        enum Record<'a> {
            All,
            Claims(Ref<'a, BTreeSet<ClaimId>>),
        }
        match self {
            Self::All => Record::All,
            Self::Claims(claims) => Record::Claims(Ref(claims)),
        }
        .serialize(serializer)
    }
    fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        enum Record {
            All,
            Claims(Value<BTreeSet<ClaimId>>),
        }
        Ok(match Record::deserialize(deserializer)? {
            Record::All => Self::All,
            Record::Claims(Value(claims)) => Self::Claims(claims),
        })
    }
}

impl V1 for ResyncReason {
    #[inline]
    fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::HistoryExpired => {
                serializer.serialize_unit_variant("ResyncReason", 0, "HistoryExpired")
            }
            Self::LeaseExpired => {
                serializer.serialize_unit_variant("ResyncReason", 1, "LeaseExpired")
            }
            Self::SlowConsumer => {
                serializer.serialize_unit_variant("ResyncReason", 2, "SlowConsumer")
            }
            Self::SnapshotExpired => {
                serializer.serialize_unit_variant("ResyncReason", 3, "SnapshotExpired")
            }
            Self::ExplicitReset => {
                serializer.serialize_unit_variant("ResyncReason", 4, "ExplicitReset")
            }
        }
    }
    fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        enum Record {
            HistoryExpired,
            LeaseExpired,
            SlowConsumer,
            SnapshotExpired,
            ExplicitReset,
        }
        Ok(match Record::deserialize(deserializer)? {
            Record::HistoryExpired => Self::HistoryExpired,
            Record::LeaseExpired => Self::LeaseExpired,
            Record::SlowConsumer => Self::SlowConsumer,
            Record::SnapshotExpired => Self::SnapshotExpired,
            Record::ExplicitReset => Self::ExplicitReset,
        })
    }
}

v1_struct_enum!(CursorMode {
    Live,
    Seeding { snapshot: SessionSeq },
    Resync { reason: ResyncReason },
    Protected,
});

v1_struct_enum!(CursorOperation {
    Register {
        consumer: ConsumerId,
        scope: ContentHash,
        filter: DeltaFilter,
        start: Position,
        expires_at: u64,
    },
    Acknowledge { token: CursorToken },
    Renew {
        consumer: ConsumerId,
        generation: u64,
        expires_at: u64,
    },
    BeginSeed {
        consumer: ConsumerId,
        scope: ContentHash,
        filter: DeltaFilter,
        snapshot: SessionSeq,
        expires_at: u64,
    },
    CompleteSeed {
        consumer: ConsumerId,
        generation: u64,
        snapshot: SessionSeq,
    },
    RequireResync {
        consumer: ConsumerId,
        generation: u64,
        reason: ResyncReason,
    },
    AdvanceFloor { through: SessionSeq },
    RegisterProtected {
        consumer: ConsumerId,
        scope: ContentHash,
        filter: DeltaFilter,
        start: Position,
    },
    AcknowledgeAndRenew { token: CursorToken, expires_at: u64 },
});

#[cfg(test)]
#[path = "durable_v1_tests.rs"]
mod tests;
