use serde::{Deserialize, Serialize};
use std::fmt;

macro_rules! id {
    ($($name:ident),+ $(,)?) => {$ (
        #[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        pub struct $name(pub [u8; 16]);
        impl $name {
            pub const fn from_u128(value: u128) -> Self { Self(value.to_be_bytes()) }
            pub const fn as_bytes(&self) -> &[u8; 16] { &self.0 }
            pub const fn is_zero(self) -> bool { u128::from_be_bytes(self.0) == 0 }
        }
        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{:032x}", u128::from_be_bytes(self.0))
            }
        }
        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "{}({self})", stringify!($name)) }
        }
    )+};
}
id!(
    TenantId,
    SessionId,
    ObjectId,
    ClaimId,
    TestamentId,
    ValidationId,
    ArtifactId,
    ParticipantId,
    RequestId,
    ReceiptId,
    EvidenceSetId,
    OccurrenceId,
    RootCommandId,
    MonitorId,
    ValidatorId,
    TimerId,
    ContentDomainId
);

macro_rules! counter {
    ($($name:ident),+ $(,)?) => {$ (
        #[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        pub struct $name(pub u64);
    )+};
}
counter!(
    SessionSeq,
    RequestEpoch,
    ObjectRevision,
    RouteEpoch,
    RaftIndex,
    RaftTerm
);

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct LedgerId {
    pub tenant: TenantId,
    pub session: SessionId,
}

#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ContentHash(pub [u8; 32]);
impl fmt::Debug for ContentHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}
impl fmt::Display for ContentHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}
pub type EvidenceDigest = ContentHash;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DeltaId {
    pub ledger: LedgerId,
    pub sequence: SessionSeq,
    pub ordinal: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ObjectRef {
    pub ledger: LedgerId,
    pub kind: crate::ObjectKind,
    pub id: ObjectId,
}
impl ObjectRef {
    pub fn claim(ledger: LedgerId, id: ClaimId) -> Self {
        Self {
            ledger,
            kind: crate::ObjectKind::Claim,
            id: ObjectId(id.0),
        }
    }
}
