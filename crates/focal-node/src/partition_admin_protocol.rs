//! Scoped OS-owner administration of the directory partition groups this
//! node hosts (24 §13; the audit's F24): their configuration, a membership
//! change submitted to the one this node leads, and a hand-off of a group's
//! leadership. The deployment's apply drives the seats, one exact request
//! each, as it drives the root's; `cluster partitions` drives them by hand;
//! `cluster nodes remove` vacates a leaving node's seats.
use super::*;
use focal_control::{ControlConfiguration, ControlMembershipRecord, ControlReceipt};
use focal_directory::PartitionId;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PartitionAdminCommand {
    /// The configuration of the partition's group as this node's replica
    /// applied it, and the entry that last changed it.
    Configuration { partition: [u8; 16] },
    /// One membership request to the partition's group, answered by the
    /// replica this node hosts when it leads the group; its client is the
    /// administrator's principal and its sequence the partition journal's.
    Change {
        partition: [u8; 16],
        request: Box<ControlRequest>,
    },
    /// Hand the partition group's leadership to one of its voters: from
    /// the replica this node hosts when it leads, or — asked to lead itself
    /// — through the leader, as the root's transfer. Success is initiation.
    Transfer {
        partition: [u8; 16],
        request: Box<focal_control::ControlTransfer>,
    },
}
impl PartitionAdminCommand {
    pub(super) fn validate(&self) -> Result<(), AccessError> {
        match self {
            Self::Configuration { partition } if *partition != [0; 16] => Ok(()),
            Self::Change { partition, request } if *partition != [0; 16] => {
                let ControlCommand::Membership(command) = &request.command else {
                    return Err(AccessError::InvalidRequest);
                };
                command
                    .expected
                    .validate()
                    .map_err(|_| AccessError::InvalidRequest)?;
                command
                    .change
                    .apply_to(&command.expected)
                    .map(|_| ())
                    .map_err(|_| AccessError::InvalidRequest)
            }
            Self::Transfer { partition, request } if *partition != [0; 16] => {
                request
                    .expected
                    .validate()
                    .map_err(|_| AccessError::InvalidRequest)?;
                if request.target == 0 || !request.expected.voters.contains(&request.target) {
                    return Err(AccessError::InvalidRequest);
                }
                Ok(())
            }
            _ => Err(AccessError::InvalidRequest),
        }
    }
}
#[derive(Debug, Serialize, Deserialize)]
pub enum PartitionAdminReply {
    Configuration {
        partition: [u8; 16],
        group: [u8; 16],
        leader: u64,
        configuration: Box<ControlConfiguration>,
        record: Option<Box<ControlMembershipRecord>>,
    },
    Committed {
        partition: [u8; 16],
        group: [u8; 16],
        receipt: ControlReceipt,
    },
    TransferInitiated {
        partition: [u8; 16],
        group: [u8; 16],
        target: u64,
    },
    Rejected(ControlFailure),
}
impl LocalNetworkAdmin {
    pub(super) async fn partition_command(
        &self,
        command: PartitionAdminCommand,
    ) -> Result<Vec<u8>, AccessError> {
        let reply = match self.partition_execute(command).await {
            Ok(reply) => reply,
            Err(error) => PartitionAdminReply::Rejected(error),
        };
        let size = postcard::experimental::serialized_size(&reply)
            .map_err(|_| AccessError::InvalidRequest)?;
        if size > MAX_COMMAND {
            return Err(AccessError::Capacity);
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(size)
            .map_err(|_| AccessError::Capacity)?;
        bytes.resize(size, 0);
        postcard::to_slice(&reply, &mut bytes).map_err(|_| AccessError::InvalidRequest)?;
        Ok(bytes)
    }
    async fn partition_execute(
        &self,
        command: PartitionAdminCommand,
    ) -> Result<PartitionAdminReply, ControlFailure> {
        let directory = self
            .partitions
            .as_ref()
            .ok_or(ControlFailure::Unavailable)?;
        let principal = admin_principal(&self.identity);
        if principal.is_zero() {
            return Err(ControlFailure::Unauthorized);
        }
        match command {
            PartitionAdminCommand::Configuration { partition } => {
                let host = directory
                    .host_of(PartitionId(partition))
                    .ok_or(ControlFailure::Unavailable)?;
                let progress = host.progress();
                let witness = host.witness_membership().await?;
                Ok(PartitionAdminReply::Configuration {
                    partition,
                    group: progress.identity.group,
                    leader: progress.leader,
                    configuration: Box::new(witness.configuration),
                    record: witness.record.map(Box::new),
                })
            }
            PartitionAdminCommand::Change { partition, request } => {
                if request.id.client != principal.0 {
                    return Err(ControlFailure::Unauthorized);
                }
                let host = directory
                    .host_of(PartitionId(partition))
                    .ok_or(ControlFailure::Unavailable)?;
                let group = host.progress().identity.group;
                let peer = self.directory_peer(principal, directory)?;
                let receipt = host.submit(peer, *request).await?;
                Ok(PartitionAdminReply::Committed {
                    partition,
                    group,
                    receipt,
                })
            }
            PartitionAdminCommand::Transfer { partition, request } => {
                let host = directory
                    .host_of(PartitionId(partition))
                    .ok_or(ControlFailure::Unavailable)?;
                let group = host.progress().identity.group;
                let peer = self.directory_peer(principal, directory)?;
                let target = request.target;
                // Correlation only: a transfer has no retry identity.
                let id = RequestId::from_u128(u128::from(request.expected_configuration_index));
                host.transfer(peer, id, *request).await?;
                Ok(PartitionAdminReply::TransferInitiated {
                    partition,
                    group,
                    target,
                })
            }
        }
    }
    /// The partition's requests travel under the directory's own namespace;
    /// the administrator's grant covers its tenant.
    fn directory_peer(
        &self,
        principal: focal_model::ParticipantId,
        directory: &crate::network_service::DirectoryHandle,
    ) -> Result<AuthenticatedPeer, ControlFailure> {
        AuthenticatedPeer::local(PeerGrant {
            principal,
            tenants: std::collections::BTreeSet::from([
                self.identity.ledger.tenant,
                directory.namespace().tenant,
            ]),
            role: PeerRole::Runtime,
        })
        .map_err(|_| ControlFailure::Unauthorized)
    }
}
