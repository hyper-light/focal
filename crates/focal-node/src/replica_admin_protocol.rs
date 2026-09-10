//! Scoped OS-owner administration of already installed application replicas.
use super::*;
use focal_ledger::{LedgerError, MembershipView, SessionMembershipRequest};
use focal_model::{LedgerId, SessionId, SessionSeq};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReplicaAdminCommand {
    Transfer {
        session: SessionId,
        group: [u8; 16],
        request: ControlTransfer,
    },
    List {
        after: Option<SessionId>,
        limit: u16,
    },
    Configuration {
        session: SessionId,
        group: Option<[u8; 16]>,
    },
    Change {
        session: SessionId,
        group: [u8; 16],
        request: SessionMembershipRequest,
    },
    /// Propose the committed activation of native history over an empty
    /// legacy prefix; every voter must already promise the successor decoder.
    ActivateNative { session: SessionId, group: [u8; 16] },
    /// Checkpoint the replica's applied prefix now: the Session envelope is
    /// encoded and installed synchronously, and the Raft log is compacted
    /// behind it. A native Core root beyond the inline bound is sealed as
    /// seeds beside the node's data (25 §5).
    Checkpoint { session: SessionId, group: [u8; 16] },
    /// The committed movement map of a native session (25 §6), answered by
    /// this node's replica whether it leads or follows; `group`, when given,
    /// must be the replica's.
    Ranges {
        session: SessionId,
        group: Option<[u8; 16]>,
    },
}
impl ReplicaAdminCommand {
    pub(super) fn validate(&self) -> Result<(), AccessError> {
        match self {
            Self::Transfer {
                session,
                group,
                request,
            } if !session.is_zero() && *group != [0; 16] && request.target != 0 => request
                .expected
                .validate()
                .map_err(|_| AccessError::InvalidRequest),
            Self::List { limit, .. } if *limit > 0 && *limit <= 64 => Ok(()),
            Self::Configuration { session, group }
                if !session.is_zero() && group.is_none_or(|g| g != [0; 16]) =>
            {
                Ok(())
            }
            Self::Change {
                session,
                group,
                request,
            } if !session.is_zero() && *group != [0; 16] => {
                request.validate().map_err(|_| AccessError::InvalidRequest)
            }
            Self::ActivateNative { session, group } | Self::Checkpoint { session, group }
                if !session.is_zero() && *group != [0; 16] =>
            {
                Ok(())
            }
            Self::Ranges { session, group }
                if !session.is_zero() && group.is_none_or(|g| g != [0; 16]) =>
            {
                Ok(())
            }
            _ => Err(AccessError::InvalidRequest),
        }
    }
}
#[derive(Debug, Serialize, Deserialize)]
pub struct ReplicaAdminStatus {
    pub session: SessionId,
    pub group: [u8; 16],
    pub node: u64,
    pub leader: u64,
    pub term: u64,
    pub sequence: SessionSeq,
    pub stopped: bool,
}
#[derive(Debug, Serialize, Deserialize)]
pub enum ReplicaAdminReply {
    TransferInitiated {
        session: SessionId,
        group: [u8; 16],
        target: u64,
    },
    Inventory {
        management_sequence: u64,
        replicas: Vec<ReplicaAdminStatus>,
        next: Option<SessionId>,
    },
    Configuration {
        session: SessionId,
        group: [u8; 16],
        view: Box<MembershipView>,
    },
    NativeActivationProposed {
        session: SessionId,
        group: [u8; 16],
    },
    Checkpointed {
        session: SessionId,
        group: [u8; 16],
    },
    Ranges {
        session: SessionId,
        group: [u8; 16],
        view: Box<crate::fleet::RangeView>,
    },
    Rejected(ControlFailure),
}
impl LocalNetworkAdmin {
    pub fn with_fleet(mut self, fleet: crate::fleet::FleetManager) -> Result<Self, AccessError> {
        if fleet.identity() != (self.identity.node, self.identity.cluster) {
            return Err(AccessError::Unauthorized);
        }
        self.fleet = Some(fleet);
        Ok(self)
    }
    /// The exclusive content writer, needed to seal inline legacy payloads
    /// before a populated ledger's import is proposed.
    pub fn with_content(mut self, content: crate::content_host::ContentHost) -> Self {
        self.content = Some(content);
        self
    }
    pub(super) async fn replica_command(
        &self,
        command: ReplicaAdminCommand,
    ) -> Result<Vec<u8>, AccessError> {
        let reply = match self.replica_execute(command).await {
            Ok(reply) => reply,
            Err(error) => ReplicaAdminReply::Rejected(error),
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
    async fn replica_execute(
        &self,
        command: ReplicaAdminCommand,
    ) -> Result<ReplicaAdminReply, ControlFailure> {
        let fleet = self.fleet.as_ref().ok_or(ControlFailure::Unavailable)?;
        match command {
            ReplicaAdminCommand::Transfer {
                session,
                group,
                request,
            } => {
                let (actual, host) = fleet
                    .replica_target(LedgerId {
                        tenant: self.identity.ledger.tenant,
                        session,
                    })
                    .map_err(|_| ControlFailure::Unavailable)?;
                if group != actual {
                    return Err(ControlFailure::WrongOwner);
                }
                let target = request.target;
                host.transfer_leader_checked(request)
                    .await
                    .map_err(failure)?;
                Ok(ReplicaAdminReply::TransferInitiated {
                    session,
                    group,
                    target,
                })
            }
            ReplicaAdminCommand::List { after, limit } => {
                let after = after.map(|session| LedgerId {
                    tenant: self.identity.ledger.tenant,
                    session,
                });
                let (status, rows, next) = fleet
                    .replica_page(self.identity.ledger.tenant, after, usize::from(limit))
                    .map_err(|_| ControlFailure::Unavailable)?;
                let mut replicas = Vec::new();
                replicas
                    .try_reserve_exact(rows.len())
                    .map_err(|_| ControlFailure::Capacity)?;
                for (ledger, group, progress) in rows {
                    if ledger.tenant != self.identity.ledger.tenant {
                        return Err(ControlFailure::Unauthorized);
                    }
                    replicas.push(ReplicaAdminStatus {
                        session: ledger.session,
                        group,
                        node: progress.node,
                        leader: progress.leader,
                        term: progress.term,
                        sequence: progress.sequence,
                        stopped: progress.stopped,
                    });
                }
                Ok(ReplicaAdminReply::Inventory {
                    management_sequence: status.latest_sequence,
                    replicas,
                    next: next.map(|ledger| ledger.session),
                })
            }
            ReplicaAdminCommand::Configuration { session, group } => {
                let (actual, host) = fleet
                    .replica_target(LedgerId {
                        tenant: self.identity.ledger.tenant,
                        session,
                    })
                    .map_err(|_| ControlFailure::Unavailable)?;
                if group.is_some_and(|g| g != actual) {
                    return Err(ControlFailure::WrongOwner);
                }
                let reply = host.membership().await.map_err(failure)?;
                Ok(ReplicaAdminReply::Configuration {
                    session,
                    group: actual,
                    view: Box::new(reply.view().clone()),
                })
            }
            ReplicaAdminCommand::Change {
                session,
                group,
                request,
            } => {
                let (actual, host) = fleet
                    .replica_target(LedgerId {
                        tenant: self.identity.ledger.tenant,
                        session,
                    })
                    .map_err(|_| ControlFailure::Unavailable)?;
                if group != actual {
                    return Err(ControlFailure::WrongOwner);
                }
                let reply = host.change_membership(request).await.map_err(failure)?;
                Ok(ReplicaAdminReply::Configuration {
                    session,
                    group,
                    view: Box::new(reply.view().clone()),
                })
            }
            ReplicaAdminCommand::ActivateNative { session, group } => {
                let (actual, host) = fleet
                    .replica_target(LedgerId {
                        tenant: self.identity.ledger.tenant,
                        session,
                    })
                    .map_err(|_| ControlFailure::Unavailable)?;
                if group != actual {
                    return Err(ControlFailure::WrongOwner);
                }
                let chunking = self
                    .content
                    .as_ref()
                    .map(|content| content.import_chunking())
                    .unwrap_or((0, 0));
                // A populated prefix is imported: its inline legacy payloads are
                // sealed by the exclusive writer before the proposal records
                // the root every replica must reproduce (23 §5).
                if let Some(import) = host.import_payloads().await.map_err(failure)? {
                    let content = self.content.as_ref().ok_or(ControlFailure::Unavailable)?;
                    for bytes in import.payloads {
                        content
                            .seal_import_inline(import.domain, bytes, chunking.0)
                            .await
                            .map_err(|_| ControlFailure::Unavailable)?;
                    }
                }
                host.activate_native(crate::fleet::ActivateNativeCall {
                    profile: focal_ledger::NativeContentProfile::AuthoredV1,
                    chunk_bytes: chunking.0,
                    max_manifest_bytes: chunking.1,
                })
                .await
                .map_err(failure)?;
                Ok(ReplicaAdminReply::NativeActivationProposed { session, group })
            }
            ReplicaAdminCommand::Checkpoint { session, group } => {
                let (actual, host) = fleet
                    .replica_target(LedgerId {
                        tenant: self.identity.ledger.tenant,
                        session,
                    })
                    .map_err(|_| ControlFailure::Unavailable)?;
                if group != actual {
                    return Err(ControlFailure::WrongOwner);
                }
                host.checkpoint().await.map_err(failure)?;
                Ok(ReplicaAdminReply::Checkpointed { session, group })
            }
            ReplicaAdminCommand::Ranges { session, group } => {
                let (actual, host) = fleet
                    .replica_target(LedgerId {
                        tenant: self.identity.ledger.tenant,
                        session,
                    })
                    .map_err(|_| ControlFailure::Unavailable)?;
                if group.is_some_and(|group| group != actual) {
                    return Err(ControlFailure::WrongOwner);
                }
                let view = host.range_view(false).await.map_err(failure)?;
                Ok(ReplicaAdminReply::Ranges {
                    session,
                    group: actual,
                    view: Box::new(view),
                })
            }
        }
    }
}
fn failure(error: LedgerError) -> ControlFailure {
    match error {
        LedgerError::Capacity | LedgerError::Memory(_) => ControlFailure::Capacity,
        LedgerError::NotReady { leader } => ControlFailure::NotLeader { leader },
        LedgerError::OutcomeUnknown => ControlFailure::OutcomeUnknown,
        LedgerError::MembershipConflict => ControlFailure::CompareFailed,
        LedgerError::Consensus(focal_consensus::ConsensusError::LearnerBehind) => {
            ControlFailure::NotReady
        }
        LedgerError::Managed(_) => ControlFailure::NotReady,
        LedgerError::Consensus(focal_consensus::ConsensusError::NotLeader { leader }) => {
            ControlFailure::NotLeader { leader }
        }
        LedgerError::Consensus(focal_consensus::ConsensusError::Configuration(_)) => {
            ControlFailure::Invalid
        }
        _ => ControlFailure::Unavailable,
    }
}
