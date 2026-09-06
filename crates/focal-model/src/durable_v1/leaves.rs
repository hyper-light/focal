use super::V1;
use crate::*;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

// Delegation is limited to standard primitive wire shapes. Domain types below
// explicitly select their historical raw fields rather than their live Serde.
macro_rules! primitive {
    ($($ty:ty),+ $(,)?) => {$ (
        impl V1 for $ty {
            #[inline]
            fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                self.serialize(serializer)
            }
            fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                Self::deserialize(deserializer)
            }
        }
    )+};
}
primitive!(bool, u16, u32, u64, String, [u8; 16], [u8; 32]);

impl V1 for u8 {
    #[inline]
    fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u8(*self)
    }

    fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::deserialize(deserializer)
    }

    #[inline]
    fn serialize_sequence_v1<S: Serializer>(
        values: &[Self],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        // Postcard bytes and Vec<u8> share the exact count/raw-byte encoding.
        // This also makes the serialized-size pass constant time for payloads.
        serializer.serialize_bytes(values)
    }
}

macro_rules! newtype {
    ($inner:ty; $($name:ident),+ $(,)?) => {$ (
        impl V1 for $name {
            #[inline]
            fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                let Self(value) = self;
                serializer.serialize_newtype_struct(stringify!($name), value)
            }
            fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                #[derive(Deserialize)]
                struct Raw($inner);
                let Raw(value) = Raw::deserialize(deserializer)?;
                Ok(Self(value))
            }
        }
    )+};
}
newtype!([u8; 16]; TenantId, SessionId, ObjectId, ClaimId, TestamentId,
    ValidationId, ArtifactId, ParticipantId, RequestId, ReceiptId, EvidenceSetId,
    OccurrenceId, RootCommandId, MonitorId, ValidatorId, TimerId, ContentDomainId);
newtype!([u8; 32]; ContentHash);
newtype!(u64; SessionSeq, RequestEpoch, ObjectRevision, RouteEpoch, RaftIndex, RaftTerm);

v1_struct!(LedgerId {
    tenant: TenantId,
    session: SessionId
});
v1_struct!(ObjectRef {
    ledger: LedgerId,
    kind: ObjectKind,
    id: ObjectId
});
