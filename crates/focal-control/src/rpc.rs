//! Bounded control RPC payloads. Transport authentication remains server-owned.
use crate::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlRead {
    /// One bounded root or directory-partition checkpoint, never a fleet-wide scan.
    State,
    Receipt(ControlRequestId),
    Membership,
    Authority,
    Configuration,
    Contacts,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(
    clippy::large_enum_variant,
    reason = "one bounded inline RPC/view; host reserves before decoding or exporting"
)]
pub enum ControlRpc {
    Submit(ControlRequest),
    Read(ControlRead),
    /// Initiation only: transfer has no durable mutation receipt.
    Transfer(ControlTransfer),
}
impl ControlRpc {
    pub fn encode(&self, limit: usize) -> Result<Vec<u8>, ControlError> {
        encode(self, limit)
    }
    pub fn decode(bytes: &[u8], limit: usize) -> Result<Self, ControlError> {
        decode(bytes, limit)
    }
    /// Reject the Submit discriminant before decoding any command-owned arrays.
    /// ControlRpc::Read keeps its existing wire ordinal; new variants append.
    pub fn decode_read_only(bytes: &[u8], limit: usize) -> Result<ControlRead, ControlError> {
        if bytes.is_empty() || bytes.len() > limit {
            return Err(ControlError::Capacity);
        }
        let (tag, rest) = postcard::take_from_bytes::<u32>(bytes)?;
        if tag != 1 {
            return Err(ControlError::Invalid);
        }
        decode(rest, limit)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlSnapshot {
    pub identity: ControlIdentity,
    pub applied_index: u64,
    pub revisions: ControlRevisions,
    pub state: ControlBootstrap,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlMembership {
    pub node: u64,
    pub leader: u64,
    pub term: u64,
    pub voters: Vec<u64>,
    pub learners: Vec<u64>,
    pub applied_index: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(
    clippy::large_enum_variant,
    reason = "one bounded inline RPC/view; host reserves before decoding or exporting"
)]
pub enum ControlReadResult {
    State(ControlSnapshot),
    Receipt(Option<ControlReceipt>),
    Membership(ControlMembership),
    Authority(Option<ControlAuthoritySnapshot>),
    Configuration(ControlConfiguration),
    Contacts(ContactSnapshot),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum ControlFailure {
    #[error("metadata admission capacity exceeded")]
    Capacity,
    #[error("metadata owner is unavailable")]
    Unavailable,
    #[error("metadata outcome is unknown; retry the same identity")]
    OutcomeUnknown,
    #[error("metadata replica is not leader (known leader: {leader})")]
    NotLeader { leader: u64 },
    #[error("metadata leader is not ready")]
    NotReady,
    #[error("metadata request identity conflicts with retained content")]
    RetryConflict,
    #[error("metadata request is below its durable retry floor")]
    RetryExpired,
    #[error("metadata request or acknowledgment sequence is invalid")]
    RetryOrder,
    #[error("metadata comparison failed")]
    CompareFailed,
    #[error("metadata authority rejected the request")]
    Unauthorized,
    #[error("invalid metadata request")]
    Invalid,
    #[error("metadata command does not match this owner")]
    WrongOwner,
    #[error("metadata command was rejected")]
    Rejected,
}
impl From<ControlError> for ControlFailure {
    fn from(error: ControlError) -> Self {
        match error {
            ControlError::Capacity
            | ControlError::Busy
            | ControlError::Memory(_)
            | ControlError::Consensus(focal_consensus::ConsensusError::Capacity) => Self::Capacity,
            ControlError::Consensus(focal_consensus::ConsensusError::NotLeader { leader }) => {
                Self::NotLeader { leader }
            }
            ControlError::NotReady => Self::NotReady,
            ControlError::Consensus(focal_consensus::ConsensusError::LearnerBehind) => {
                Self::NotReady
            }
            ControlError::Consensus(focal_consensus::ConsensusError::Configuration(_)) => {
                Self::Invalid
            }
            ControlError::RetryConflict => Self::RetryConflict,
            ControlError::RetryExpired => Self::RetryExpired,
            ControlError::RetryOrder => Self::RetryOrder,
            ControlError::WrongOwner => Self::WrongOwner,
            ControlError::Invalid | ControlError::Codec(_) => Self::Invalid,
            ControlError::Directory(focal_directory::DirectoryError::CompareFailed) => {
                Self::CompareFailed
            }
            ControlError::Directory(focal_directory::DirectoryError::UnverifiedAuthority) => {
                Self::Unauthorized
            }
            ControlError::Enrollment(_) | ControlError::Directory(_) => Self::Rejected,
            ControlError::Consensus(_) | ControlError::Failed | ControlError::Corrupt(_) => {
                Self::Unavailable
            }
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(
    clippy::large_enum_variant,
    reason = "one bounded inline RPC/view; host reserves before decoding or exporting"
)]
pub enum ControlReply {
    Committed(ControlReceipt),
    Read(ControlReadResult),
    Rejected(ControlFailure),
    TransferInitiated { target: u64 },
}
impl ControlReply {
    pub fn encode(&self, limit: usize) -> Result<Vec<u8>, ControlError> {
        encode(self, limit)
    }
    pub fn decode(bytes: &[u8], limit: usize) -> Result<Self, ControlError> {
        decode(bytes, limit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn peer_selector_preflight_never_decodes_submit_body_and_preserves_wire_ordinals() {
        assert_eq!(
            ControlRpc::Read(ControlRead::State).encode(64).unwrap(),
            vec![1, 0]
        );
        assert_eq!(
            ControlRpc::Read(ControlRead::Membership)
                .encode(64)
                .unwrap(),
            vec![1, 2]
        );
        assert_eq!(
            ControlRpc::Read(ControlRead::Authority).encode(64).unwrap(),
            vec![1, 3]
        );
        assert_eq!(
            ControlRpc::decode_read_only(&[1, 2], 64).unwrap(),
            ControlRead::Membership
        );
        // The malformed, potentially enormous command collection hint is never
        // interpreted: the outer Submit discriminator is rejected first.
        assert!(matches!(
            ControlRpc::decode_read_only(&[0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff], 64),
            Err(ControlError::Invalid)
        ));
        assert!(ControlRpc::decode_read_only(&[1, 2, 0], 64).is_err());
        assert!(matches!(
            ControlRpc::decode_read_only(&[1; 65], 64),
            Err(ControlError::Capacity)
        ));
    }
}
