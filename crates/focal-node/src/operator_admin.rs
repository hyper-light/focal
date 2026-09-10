//! Authenticated local diagnostics. No read starts durable IO or activation.
use super::*;
use focal_client::admin::{
    AdminArchiveBundle, AdminArchiveFamily, AdminNodeConfiguration, AdminNodeHealth,
    AdminNodeIdentity, AdminReplicaDiagnostics,
};
use focal_model::{ClaimId, ContentClass, ContentDomainId, ContentRef, LedgerId, SessionId};

/// The most bytes an archive bundle read for verification may take, and
/// the structural bounds it is inspected under (26 §4): the record
/// encoding limits a session hosts under, so any bundle a core wrote fits.
const MAX_ARCHIVE_BYTES: usize = 6 << 20;
const ARCHIVE_INSPECTION: focal_core::native::record_codec::InspectionLimits =
    focal_core::native::record_codec::InspectionLimits {
        bytes: 6 << 20,
        visits: 1 << 30,
        rows: 100_000,
        row_bytes: 6 << 20,
    };

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum OperatorRead {
    Identity,
    Health,
    Configuration,
    Replica {
        session: SessionId,
    },
    /// A retired claim's archive bundle as this node holds and verifies it
    /// (26 §4).
    Archive {
        session: SessionId,
        claim: ClaimId,
    },
    /// The collector's state on this node (26 §5).
    Gc,
    /// The storage view of this node (26 §7).
    Storage,
}
impl OperatorRead {
    pub(super) fn validate(self) -> Result<(), AccessError> {
        let valid = match self {
            Self::Identity | Self::Health | Self::Configuration | Self::Gc | Self::Storage => true,
            Self::Replica { session } => !session.is_zero(),
            Self::Archive { session, claim } => !session.is_zero() && !claim.is_zero(),
        };
        if valid {
            Ok(())
        } else {
            Err(AccessError::InvalidRequest)
        }
    }
}
/// Sessions the storage view lists before it reports truncation.
const MAX_STORAGE_SESSIONS: usize = 256;
#[derive(Serialize, Deserialize)]
pub(crate) enum OperatorReply {
    Identity(AdminNodeIdentity),
    Health(AdminNodeHealth),
    Configuration(AdminNodeConfiguration),
    Replica(Box<AdminReplicaDiagnostics>),
    Error(AccessError),
    /// `None` when the claim has no continuation on this replica.
    Archive(Option<Box<AdminArchiveBundle>>),
    Gc(Box<focal_client::admin::AdminGc>),
    Storage(Box<focal_client::admin::AdminStorage>),
}
impl LocalNetworkAdmin {
    pub(super) async fn operator_read(&self, read: OperatorRead) -> Result<Vec<u8>, AccessError> {
        let reply = self
            .operator_value(read)
            .await
            .unwrap_or_else(OperatorReply::Error);
        let len =
            postcard::experimental::serialized_size(&reply).map_err(|_| AccessError::Capacity)?;
        if len > MAX_COMMAND {
            return Err(AccessError::Capacity);
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(len)
            .map_err(|_| AccessError::Capacity)?;
        bytes.resize(len, 0);
        postcard::to_slice(&reply, &mut bytes).map_err(|_| AccessError::InvalidRequest)?;
        Ok(bytes)
    }
    async fn operator_value(&self, read: OperatorRead) -> Result<OperatorReply, AccessError> {
        match read {
            OperatorRead::Identity => Ok(OperatorReply::Identity(AdminNodeIdentity {
                node: self.identity.node,
                cluster: hex(&self.identity.cluster),
                tenant: self.identity.ledger.tenant.to_string(),
                session: self.identity.ledger.session.to_string(),
                issuer: self.identity.issuer.to_string(),
                root: self.identity.root.to_string(),
            })),
            OperatorRead::Configuration => {
                let namespace = root_namespace(&self.identity);
                Ok(OperatorReply::Configuration(AdminNodeConfiguration {
                    node: self.identity.node,
                    network_schema: 1,
                    listen: self.listen.to_string(),
                    advertise: self.advertise.to_string(),
                    root_group: hex(&self.root.group),
                    root_tenant: namespace.tenant.to_string(),
                    root_session: namespace.session.to_string(),
                }))
            }
            OperatorRead::Health => {
                let root = self
                    .control
                    .as_ref()
                    .ok_or(AccessError::Unavailable)?
                    .progress();
                let fleet = self
                    .fleet
                    .as_ref()
                    .ok_or(AccessError::Unavailable)?
                    .status();
                let placement = match &self.placement {
                    Some(handle) => handle.status().await.ok().map(|status| {
                        focal_client::admin::AdminPlacementAgent {
                            root_intents: status.root_intents,
                            partition_intents: status.partition_intents,
                            installed: status
                                .installed
                                .iter()
                                .map(|ledger| format!("{}/{}", ledger.tenant, ledger.session))
                                .collect(),
                            last_error: status.last_error,
                            last_refusal: status.last_refusal,
                        }
                    }),
                    None => None,
                };
                Ok(OperatorReply::Health(AdminNodeHealth {
                    node: self.identity.node,
                    root_stopped: root.stopped,
                    root_leader: root.leader,
                    root_term: root.term,
                    root_applied_index: root.applied_index,
                    fleet_stopped: fleet.stopped,
                    installed: fleet.installed,
                    running: fleet.running,
                    placement,
                }))
            }
            OperatorRead::Replica { session } => {
                let (group, host) = self
                    .fleet
                    .as_ref()
                    .ok_or(AccessError::Unavailable)?
                    .replica_target(focal_model::LedgerId {
                        tenant: self.identity.ledger.tenant,
                        session,
                    })
                    .map_err(|_| AccessError::Unavailable)?;
                let reply = host.diagnostics().await.map_err(crate::host::access)?;
                if reply.value().group != hex(&group) {
                    return Err(AccessError::Unavailable);
                }
                Ok(OperatorReply::Replica(Box::new(reply.value().clone())))
            }
            OperatorRead::Gc => {
                let handle = self.gc.as_ref().ok_or(AccessError::Unavailable)?;
                let status = handle.status();
                if status.node != self.identity.node {
                    return Err(AccessError::Unavailable);
                }
                Ok(OperatorReply::Gc(Box::new(status)))
            }
            OperatorRead::Storage => {
                use focal_client::admin::{AdminDiskStats, AdminSessionRetention, AdminStorage};
                use focal_memory::DiskKind;
                let content = self.content.as_ref().ok_or(AccessError::Unavailable)?;
                let fleet = self.fleet.as_ref().ok_or(AccessError::Unavailable)?;
                let gc = self.gc.as_ref().ok_or(AccessError::Unavailable)?.status();
                let retire = self
                    .archive
                    .as_ref()
                    .ok_or(AccessError::Unavailable)?
                    .status();
                let (disk, staged_uploads, staged_bytes) = content.disk_stats().await?;
                let kind = |kind: DiskKind| disk.by_kind.get(kind as usize).copied().unwrap_or(0);
                let mut sessions = Vec::new();
                let mut truncated = false;
                let mut after = None;
                while let Some((ledger, host)) = fleet.next_host(after) {
                    after = Some(ledger);
                    if sessions.len() >= MAX_STORAGE_SESSIONS {
                        truncated = true;
                        break;
                    }
                    let Ok(reply) = host.diagnostics().await else {
                        continue;
                    };
                    let diagnostics = reply.value();
                    sessions.try_reserve(1).map_err(|_| AccessError::Capacity)?;
                    sessions.push(AdminSessionRetention {
                        tenant: ledger.tenant.to_string(),
                        session: ledger.session.to_string(),
                        authoritative: diagnostics.native_authoritative,
                        log_entries_since_checkpoint: diagnostics.log_entries_since_checkpoint,
                        retention: diagnostics.retention.clone(),
                    });
                }
                Ok(OperatorReply::Storage(Box::new(AdminStorage {
                    node: self.identity.node,
                    disk: AdminDiskStats {
                        free: disk.free,
                        outstanding: disk.outstanding,
                        ordinary_outstanding: disk.ordinary_outstanding,
                        headroom: disk.headroom,
                        completion_reserve: disk.completion_reserve,
                        wal: kind(DiskKind::Wal),
                        checkpoint: kind(DiskKind::Checkpoint),
                        content: kind(DiskKind::Content),
                        archive: kind(DiskKind::Archive),
                        staging: kind(DiskKind::Staging),
                    },
                    staged_uploads: u64::try_from(staged_uploads).unwrap_or(u64::MAX),
                    staged_bytes,
                    retire,
                    gc: gc.config,
                    sessions,
                    truncated,
                })))
            }
            OperatorRead::Archive { session, claim } => {
                let ledger = LedgerId {
                    tenant: self.identity.ledger.tenant,
                    session,
                };
                let (group, host) = self
                    .fleet
                    .as_ref()
                    .ok_or(AccessError::Unavailable)?
                    .replica_target(ledger)
                    .map_err(|_| AccessError::Unavailable)?;
                let Some(retired) = host.retired(claim).await.map_err(crate::host::access)? else {
                    return Ok(OperatorReply::Archive(None));
                };
                let content = self.content.as_ref().ok_or(AccessError::Unavailable)?;
                let policy = content
                    .policy(ledger)
                    .await?
                    .ok_or(AccessError::Unavailable)?;
                let scope = policy.scope();
                let mut bundle = AdminArchiveBundle {
                    session: session.to_string(),
                    group: hex(&group),
                    claim: hex(&claim.0),
                    status: format!("{:?}", retired.status),
                    retired_at: retired.retired_at.0,
                    bundle: hex(&retired.bundle.0),
                    bytes: retired.bytes,
                    through: retired.through.0,
                    digest: None,
                    verified: false,
                    root: None,
                    members: Vec::new(),
                    rows: 0,
                    families: Vec::new(),
                    receipts: Vec::new(),
                };
                for peer in &policy.peers {
                    if content
                        .receipt(scope, retired.bundle, *peer)
                        .await?
                        .is_some_and(|receipt| receipt.length == retired.bytes)
                    {
                        bundle.receipts.push(*peer);
                    }
                }
                let reference = ContentRef {
                    domain: ContentDomainId(ledger.tenant.0),
                    root: retired.bundle,
                    length: retired.bytes,
                    class: ContentClass::Evidence,
                };
                // A bundle this node does not hold, or one that fails its
                // structural check, reports unverified; nothing is inferred.
                if let Ok(bytes) = content
                    .read_bytes(scope, reference, MAX_ARCHIVE_BYTES)
                    .await
                    && let Ok(archive) =
                        focal_core::native::record_codec::StructuralArchive::inspect(
                            bytes.value(),
                            ARCHIVE_INSPECTION,
                        )
                    && let Ok(families) = archive.family_counts()
                {
                    let header = archive.header();
                    bundle.verified = header.root == claim || header.members.contains(&claim);
                    bundle.digest = Some(hex(&archive.digest().0));
                    bundle.root = Some(hex(&header.root.0));
                    bundle.members = header.members.iter().map(|member| hex(&member.0)).collect();
                    bundle.rows = u64::try_from(header.count).unwrap_or(u64::MAX);
                    bundle.families = families
                        .into_iter()
                        .map(|(family, rows)| AdminArchiveFamily {
                            family: format!("{family:?}"),
                            rows: u64::try_from(rows).unwrap_or(u64::MAX),
                        })
                        .collect();
                }
                Ok(OperatorReply::Archive(Some(Box::new(bundle))))
            }
        }
    }
}
fn hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .flat_map(|byte| [byte >> 4, byte & 15])
        .filter_map(|nibble| char::from_digit(u32::from(nibble), 16))
        .collect()
}
