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
    /// Trusted controller view: both exports describe one exact applied prefix.
    /// This compound selector is excluded from Node-only PeerControl ingress.
    StateAndAuthority,
    InvitationPage {
        after: Option<[u8; 16]>,
        limit: u16,
        expected_revision: Option<u64>,
    },
    Invitation {
        id: [u8; 16],
    },
    PrepareRevocation {
        id: [u8; 16],
    },
    AdminReceipt {
        id: ControlRequestId,
    },
    /// One session's route as this partition holds it (§14): where its log
    /// leads at which epochs. Partition scope only; open to enrolled nodes.
    Route {
        ledger: focal_model::LedgerId,
    },
    /// The route changes this partition committed after a revision, for a
    /// cache watching it. Partition scope only; open to enrolled nodes.
    RouteChanges {
        after_revision: u64,
    },
    /// Prepare the authority command that sets a node's eligibility for
    /// placement (drain when false): the node's grant re-issued at its next
    /// generation. Root scope, local administrator only.
    PrepareEligibility {
        node: u64,
        eligible: bool,
    },
    /// The entry that last changed this group's configuration, as applied
    /// here (`None` before any change): what a voter attests to the root
    /// for the grant to follow the log. Local or peer-control ingress.
    MembershipRecord,
    /// `State`, unless the reader holds it: a replica whose revisions are
    /// `held` exports what it exported at them (every change to a checkpoint
    /// moves a revision), so it answers only the index it has reached
    /// ([`ControlReadResult::StateHeld`]). What an observer that looks at a
    /// group again and again asks: it is sent the checkpoint once a change,
    /// not once a look. Open to enrolled nodes, as `State` is.
    StateUnless {
        held: ControlRevisions,
    },
    /// `Authority`, unless the reader holds it: a replica whose installed
    /// authority is the one it exports at `source_index` holds the reader's
    /// registry and enrollment (a partition replaces them only with a newer
    /// source index), so it answers only what moves without them, the index
    /// it has reached and the time its last command was decided at
    /// ([`ControlReadResult::AuthorityHeld`]). Open to enrolled nodes, as
    /// `Authority` is.
    AuthorityUnless {
        source_index: u64,
    },
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
        let query = decode(rest, limit)?;
        if matches!(
            query,
            ControlRead::StateAndAuthority
                | ControlRead::InvitationPage { .. }
                | ControlRead::Invitation { .. }
                | ControlRead::PrepareRevocation { .. }
                | ControlRead::AdminReceipt { .. }
                | ControlRead::PrepareEligibility { .. }
        ) {
            return Err(ControlError::Invalid);
        }
        Ok(query)
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
    StateAndAuthority {
        snapshot: Box<ControlSnapshot>,
        authority: Option<ControlAuthoritySnapshot>,
    },
    Invitations {
        identity: ControlIdentity,
        applied_index: u64,
        revision: u64,
        entries: Vec<focal_enrollment::InvitationStatus>,
        next: Option<[u8; 16]>,
    },
    Route(Option<focal_directory::SessionRoute>),
    RouteChanges(focal_directory::InvalidationBatch),
    MembershipRecord(Option<ControlMembershipRecord>),
    PreparedRevocation {
        identity: ControlIdentity,
        applied_index: u64,
        invitation: [u8; 16],
        command: focal_enrollment::EnrollmentCommand,
    },
    AdminReceipt {
        configuration: ControlConfiguration,
        enrollment_revision: u64,
        receipt: Option<ControlReceipt>,
    },
    /// The command that would set a node's eligibility; `None` when the
    /// node's grant already states it, with the generation it has now.
    PreparedEligibility {
        identity: ControlIdentity,
        applied_index: u64,
        node: u64,
        generation: u64,
        eligible: bool,
        command: Option<Box<focal_directory::AuthorityCommand>>,
    },
    /// The checkpoint the reader holds (`ControlRead::StateUnless`) is this
    /// replica's at `applied_index`.
    StateHeld {
        applied_index: u64,
    },
    /// The authority the reader holds (`ControlRead::AuthorityUnless`) is
    /// this replica's at `applied_index`, its last command decided at
    /// `decided_at`.
    AuthorityHeld {
        applied_index: u64,
        decided_at: i64,
    },
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
            | ControlError::Directory(focal_directory::DirectoryError::Capacity)
            | ControlError::Directory(focal_directory::DirectoryError::Memory(
                focal_memory::MemoryError::Capacity { .. }
                | focal_memory::MemoryError::DiskCapacity { .. }
                | focal_memory::MemoryError::AllocationFailed,
            ))
            | ControlError::Consensus(focal_consensus::ConsensusError::Capacity) => Self::Capacity,
            ControlError::Consensus(focal_consensus::ConsensusError::NotLeader { leader }) => {
                Self::NotLeader { leader }
            }
            ControlError::NotReady => Self::NotReady,
            ControlError::Consensus(
                focal_consensus::ConsensusError::LearnerBehind
                | focal_consensus::ConsensusError::MembershipPending
                | focal_consensus::ConsensusError::LeaderLeaving,
            ) => Self::NotReady,
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
    fn directory_capacity_is_retryable_while_invariant_failures_remain_rejections() {
        for error in [
            focal_directory::DirectoryError::Capacity,
            focal_directory::DirectoryError::Memory(focal_memory::MemoryError::Capacity {
                requested: 4096,
                available: 0,
            }),
            focal_directory::DirectoryError::Memory(focal_memory::MemoryError::AllocationFailed),
        ] {
            assert_eq!(
                ControlFailure::from(ControlError::Directory(error)),
                ControlFailure::Capacity
            );
        }
        for error in [
            focal_directory::DirectoryError::Invalid("authority checkpoint"),
            focal_directory::DirectoryError::Memory(focal_memory::MemoryError::WrongArena),
        ] {
            assert_eq!(
                ControlFailure::from(ControlError::Directory(error)),
                ControlFailure::Rejected
            );
        }
    }
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
            ControlRpc::Read(ControlRead::StateAndAuthority)
                .encode(64)
                .unwrap(),
            vec![1, 6]
        );
        assert!(matches!(
            ControlRpc::decode_read_only(&[1, 6], 64),
            Err(ControlError::Invalid)
        ));
        assert_eq!(
            ControlRpc::decode(&[1, 6], 64).unwrap(),
            ControlRpc::Read(ControlRead::StateAndAuthority)
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
        for (tag, query) in [
            (
                7,
                ControlRead::InvitationPage {
                    after: None,
                    limit: 1,
                    expected_revision: None,
                },
            ),
            (8, ControlRead::Invitation { id: [1; 16] }),
            (9, ControlRead::PrepareRevocation { id: [1; 16] }),
            (
                10,
                ControlRead::AdminReceipt {
                    id: ControlRequestId {
                        client: [1; 16],
                        sequence: 1,
                    },
                },
            ),
            (
                13,
                ControlRead::PrepareEligibility {
                    node: 3,
                    eligible: false,
                },
            ),
        ] {
            let bytes = ControlRpc::Read(query.clone()).encode(128).unwrap();
            assert_eq!(bytes.get(..2), Some([1, tag].as_slice()));
            assert_eq!(
                ControlRpc::decode(&bytes, 128).unwrap(),
                ControlRpc::Read(query)
            );
            assert!(matches!(
                ControlRpc::decode_read_only(&bytes, 128),
                Err(ControlError::Invalid)
            ));
        }
        // The reads that name what their reader holds append, open to the read-only ingress as
        // `State` and `Authority` are; their held answers append to the results.
        for (tag, query) in [
            (
                15,
                ControlRead::StateUnless {
                    held: ControlRevisions {
                        root: 0,
                        enrollment: 0,
                        partition: 3,
                    },
                },
            ),
            (16, ControlRead::AuthorityUnless { source_index: 9 }),
        ] {
            let bytes = ControlRpc::Read(query.clone()).encode(128).unwrap();
            assert_eq!(bytes.get(..2), Some([1, tag].as_slice()));
            assert_eq!(ControlRpc::decode_read_only(&bytes, 128).unwrap(), query);
        }
        for (tag, answer) in [
            (14, ControlReadResult::StateHeld { applied_index: 7 }),
            (
                15,
                ControlReadResult::AuthorityHeld {
                    applied_index: 7,
                    decided_at: 1,
                },
            ),
        ] {
            let bytes = postcard::to_stdvec(&answer).unwrap();
            assert_eq!(bytes.first(), Some(&tag));
            assert_eq!(
                postcard::from_bytes::<ControlReadResult>(&bytes).unwrap(),
                answer
            );
        }
    }
}
