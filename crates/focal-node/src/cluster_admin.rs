//! Synchronous journal owner for authenticated local control administration.
//! Filesystem work stays on the caller's thread, between bounded network waits.
use crate::{
    config::Settings,
    embedded::{NodeIdentity, decode_identity},
    network_admin::{ADMIN_SOCKET, AdminCommand, AdminRead, admin_principal, admin_wire_limits},
};
use focal_client::admin::{
    AdminConfiguration, AdminContact, AdminCredential, AdminInvitation, AdminResult,
};
use focal_control::*;
use focal_enrollment::PrivateJournal;
use focal_wire::{AccessError, Response, UnixRemote, WireError};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};

const DIRECTORY: &str = "CLUSTER.admin";
const MARKER: &str = "CLUSTER.admin.initialized";
const LIMIT: usize = 55 * 1024;
mod mcp;
mod replicas;
#[cfg(test)]
mod tests;

#[derive(Debug, thiserror::Error)]
pub enum ClusterAdminError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Node(#[from] crate::embedded::NodeError),
    #[error(transparent)]
    Config(#[from] crate::config::ConfigError),
    #[error(transparent)]
    Journal(#[from] focal_enrollment::EnrollmentError),
    #[error(transparent)]
    Wire(#[from] WireError),
    #[error(transparent)]
    Access(#[from] AccessError),
    #[error(transparent)]
    Control(#[from] ControlFailure),
    #[error("credential renewal: {0}")]
    Renewal(#[from] crate::credential_renewal::RenewalError),
    #[error("admin journal is missing, corrupt, or belongs to another physical node")]
    Corrupt,
    #[error(
        "an earlier admin request remains pending; inspect and retry it before creating another"
    )]
    Pending,
    #[error(
        "admin operation is no longer the retained latest request; it cannot be executed again"
    )]
    Expired,
    #[error("invalid admin request or inconsistent response")]
    Invalid,
    #[error("invitation was not found in the committed enrollment registry")]
    NotFound,
    #[error("bounded admin capacity or request sequence is exhausted")]
    Capacity,
}
type Result<T> = std::result::Result<T, ClusterAdminError>;

impl ClusterAdminError {
    /// One classification for CLI and MCP; a retired administrative reference
    /// never proves that its historical request did not commit.
    pub fn classification(&self) -> focal_client::failure::Failure {
        use focal_client::failure::{self, Failure};
        match self {
            Self::Access(error) => failure::access(error),
            Self::Pending | Self::Control(ControlFailure::OutcomeUnknown) => {
                Failure::outcome_unknown()
            }
            Self::Control(ControlFailure::NotLeader { .. }) => Failure::error("not_leader", 6),
            Self::Control(ControlFailure::CompareFailed) => Failure::error("compare_failed", 5),
            Self::Control(ControlFailure::NotReady) => Failure::error("not_ready", 6),
            Self::Control(ControlFailure::Unavailable) => Failure::error("unavailable", 6),
            Self::Control(ControlFailure::RetryConflict) => Failure::error("operation_conflict", 5),
            Self::Control(ControlFailure::Unauthorized) => Failure::error("unauthorized", 3),
            Self::Capacity | Self::Control(ControlFailure::Capacity) => {
                Failure::error("capacity", 6)
            }
            Self::Journal(focal_enrollment::EnrollmentError::Locked) => Failure::error("busy", 6),
            Self::Expired | Self::Control(ControlFailure::RetryExpired) => Failure {
                condition: "Retired",
                code: "admin_retired",
                exit_code: 5,
            },
            Self::NotFound => Failure {
                condition: "NotFound",
                code: "not_found",
                exit_code: 4,
            },
            Self::Invalid | Self::Control(ControlFailure::Invalid | ControlFailure::RetryOrder) => {
                Failure::error("invalid_input", 2)
            }
            _ => Failure::error("admin", 1),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct Saved {
    schema: u16,
    identity: NodeIdentity,
    next: u64,
    next_control: u64,
    latest: Option<Latest>,
}
#[derive(Serialize, Deserialize)]
struct Latest {
    operation: u64,
    request: ControlRequest,
    receipt: Option<ControlReceipt>,
    superseded: bool,
}

/// Clone-free owner; the existing private journal lock serializes processes.
/// Only one latest operation is retained. Older references fail closed.
pub struct ClusterAdmin {
    root: PathBuf,
    identity: NodeIdentity,
}
impl ClusterAdmin {
    pub async fn operator(&self, read: crate::network_admin::OperatorRead) -> Result<AdminResult> {
        use crate::network_admin::{OperatorRead, operator::OperatorReply};
        let bytes = self.exchange_bytes(AdminCommand::Operator(read)).await?;
        let (reply, tail): (OperatorReply, _) =
            postcard::take_from_bytes(&bytes).map_err(|_| ClusterAdminError::Invalid)?;
        if !tail.is_empty() {
            return Err(ClusterAdminError::Invalid);
        }
        let namespace = crate::network_state::root_namespace(&self.identity);
        match (read, reply) {
            (_, OperatorReply::Error(error)) => Err(error.into()),
            (OperatorRead::Identity, OperatorReply::Identity(identity))
                if identity.node == self.identity.node
                    && identity.cluster == hex(&self.identity.cluster)
                    && identity.tenant == self.identity.ledger.tenant.to_string()
                    && identity.session == self.identity.ledger.session.to_string()
                    && identity.issuer == self.identity.issuer.to_string()
                    && identity.root == self.identity.root.to_string() =>
            {
                Ok(AdminResult::NodeIdentity { identity })
            }
            (OperatorRead::Health, OperatorReply::Health(health))
                if health.node == self.identity.node && health.running <= health.installed =>
            {
                Ok(AdminResult::NodeHealth { health })
            }
            (OperatorRead::Configuration, OperatorReply::Configuration(configuration))
                if configuration.node == self.identity.node
                    && configuration.network_schema == 1
                    && configuration.root_group
                        == hex(&crate::network_state::root_group(self.identity.cluster))
                    && configuration.root_tenant == namespace.tenant.to_string()
                    && configuration.root_session == namespace.session.to_string()
                    && configuration.listen.parse::<std::net::SocketAddr>().is_ok()
                    && configuration
                        .advertise
                        .parse::<std::net::SocketAddr>()
                        .is_ok() =>
            {
                Ok(AdminResult::NodeConfiguration { configuration })
            }
            (OperatorRead::Replica { session }, OperatorReply::Replica(value))
                if value.node == self.identity.node
                    && value.cluster == hex(&self.identity.cluster)
                    && value.session == session.to_string()
                    && value.applied_index <= value.committed_index
                    && focal_client::input::parse_id(&value.group).is_ok()
                    && focal_client::input::parse_hash(&value.compiled_managed_decoder).is_ok()
                    && value
                        .required_decoder
                        .as_ref()
                        .is_none_or(|hash| focal_client::input::parse_hash(hash).is_ok()) =>
            {
                Ok(AdminResult::ReplicaDiagnostics {
                    diagnostics: *value,
                })
            }
            _ => Err(ClusterAdminError::Invalid),
        }
    }
    /// Renew this node's own credential now, through its running controller.
    pub async fn renew_credential(&self) -> Result<AdminResult> {
        use crate::credential_renewal::CredentialReply;
        let bytes = self.exchange_bytes(AdminCommand::RenewCredential).await?;
        let (reply, tail): (CredentialReply, _) =
            postcard::take_from_bytes(&bytes).map_err(|_| ClusterAdminError::Invalid)?;
        if !tail.is_empty() {
            return Err(ClusterAdminError::Invalid);
        }
        match reply {
            CredentialReply::Renewed(summary) if summary.node == self.identity.node => {
                Ok(AdminResult::CredentialRenewed {
                    node: summary.node,
                    principal: hex(&summary.principal),
                    issued_at: summary.issued_at,
                    expires_at: summary.expires_at,
                    certificate_fingerprint: hex(&summary.certificate_fingerprint),
                    renewals: summary.renewals,
                })
            }
            CredentialReply::Renewed(_) => Err(ClusterAdminError::Invalid),
            CredentialReply::Failed(error) => Err(error.into()),
        }
    }
    pub fn open(settings: &Settings) -> Result<Self> {
        let root = settings.data_dir()?;
        let identity = decode_identity(&root.join("IDENTITY"))?;
        Ok(Self { root, identity })
    }
    pub fn identity(&self) -> &NodeIdentity {
        &self.identity
    }
    pub async fn invite_node(&self, name: &str, output: &Path) -> Result<AdminResult> {
        let bytes =
            zeroize::Zeroizing::new(self.exchange_bytes(AdminCommand::invitation(name)?).await?);
        let invitation = crate::network_join::NodeInvitation::decode(&bytes)
            .map_err(|_| ClusterAdminError::Invalid)?;
        if invitation.name() != name || invitation.genesis().founder != self.identity {
            return Err(ClusterAdminError::Invalid);
        }
        invitation
            .write_new(output)
            .map_err(|error| ClusterAdminError::Io(std::io::Error::other(error)))?;
        Ok(AdminResult::InvitationWritten {
            name: name.into(),
            output: output.to_string_lossy().into_owned(),
        })
    }
    pub async fn invite_client(&self, name: &str, output: &Path) -> Result<AdminResult> {
        let response = self
            .exchange_bytes(AdminCommand::InviteClient { name: name.into() })
            .await?;
        let bytes = zeroize::Zeroizing::new(response);
        let invitation = crate::network_join::ClientInvitation::decode(&bytes)
            .map_err(|_| ClusterAdminError::Invalid)?;
        if invitation.name() != name || invitation.genesis().founder != self.identity {
            return Err(ClusterAdminError::Invalid);
        }
        invitation
            .write_new(output)
            .map_err(|_| ClusterAdminError::Invalid)?;
        Ok(AdminResult::InvitationWritten {
            name: name.into(),
            output: output.to_string_lossy().into_owned(),
        })
    }
    pub fn available(settings: &Settings) -> Result<Option<Self>> {
        use std::os::unix::fs::{FileTypeExt, MetadataExt};
        let admin = Self::open(settings)?;
        match fs::symlink_metadata(admin.root.join(ADMIN_SOCKET)) {
            Ok(metadata) => {
                let root = fs::symlink_metadata(&admin.root)?;
                if !root.is_dir()
                    || root.mode() & 0o077 != 0
                    || !metadata.file_type().is_socket()
                    || metadata.uid() != root.uid()
                {
                    return Err(ClusterAdminError::Corrupt);
                }
                Ok(Some(admin))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }
    pub async fn read(&self, query: AdminRead) -> Result<AdminResult> {
        let reply = self.exchange(AdminCommand::Read(query)).await?;
        match (query, reply) {
            (
                AdminRead::Invitations {
                    after,
                    limit,
                    expected_revision,
                },
                ControlReply::Read(ControlReadResult::Invitations {
                    identity,
                    applied_index,
                    revision,
                    entries,
                    next,
                }),
            ) => {
                if identity.cluster.0 != self.identity.cluster
                    || identity.group != crate::network_state::root_group(self.identity.cluster)
                    || applied_index == 0
                    || entries.len() > usize::from(limit)
                    || expected_revision.is_some_and(|expected| expected != revision)
                    || entries
                        .first()
                        .is_some_and(|entry| after.is_some_and(|after| entry.id <= after))
                    || !entries
                        .windows(2)
                        .all(|pair| matches!(pair,[left,right] if left.id<right.id))
                    || next.is_some_and(|next| entries.last().is_none_or(|entry| entry.id != next))
                {
                    return Err(ClusterAdminError::Invalid);
                }
                Ok(invitations_view(
                    identity,
                    applied_index,
                    revision,
                    entries,
                    next,
                ))
            }
            (
                AdminRead::Invitation { id },
                ControlReply::Read(ControlReadResult::Invitations {
                    identity,
                    applied_index,
                    revision,
                    entries,
                    next,
                }),
            ) => {
                if identity.cluster.0 != self.identity.cluster
                    || identity.group != crate::network_state::root_group(self.identity.cluster)
                    || applied_index == 0
                    || entries.len() > 1
                    || entries.first().is_some_and(|entry| entry.id != id)
                    || next.is_some()
                {
                    return Err(ClusterAdminError::Invalid);
                }
                if entries.is_empty() {
                    return Err(ClusterAdminError::NotFound);
                }
                Ok(invitations_view(
                    identity,
                    applied_index,
                    revision,
                    entries,
                    next,
                ))
            }
            (AdminRead::Membership, ControlReply::Read(ControlReadResult::Membership(value))) => {
                if value.node != self.identity.node || value.applied_index == 0 {
                    return Err(ClusterAdminError::Invalid);
                }
                Ok(AdminResult::Membership {
                    node: value.node,
                    leader: value.leader,
                    term: value.term,
                    applied_index: value.applied_index,
                    voters: value.voters,
                    learners: value.learners,
                })
            }
            (
                AdminRead::Configuration,
                ControlReply::Read(ControlReadResult::Configuration(value)),
            ) => {
                self.validate_configuration(&value)?;
                Ok(AdminResult::Configuration {
                    configuration: configuration_view(value),
                })
            }
            (AdminRead::Contacts, ControlReply::Read(ControlReadResult::Contacts(value))) => {
                if value.identity.cluster.0 != self.identity.cluster
                    || value.identity.group
                        != crate::network_state::root_group(self.identity.cluster)
                    || value.contacts.cluster != self.identity.cluster
                    || value.contacts.applied_index > value.applied_index
                {
                    return Err(ClusterAdminError::Invalid);
                }
                let mut nodes = Vec::new();
                nodes
                    .try_reserve_exact(value.contacts.records.len())
                    .map_err(|_| ClusterAdminError::Capacity)?;
                for record in value.contacts.records {
                    nodes.push(AdminContact {
                        node: record.node,
                        principal: hex(&record.principal),
                        certificate_fingerprint: hex(&record.certificate_fingerprint),
                        advertise: record.advertise.to_string(),
                        server_name: record.server_name,
                        generation: record.generation,
                        committed_index: record.committed_index,
                    });
                }
                Ok(AdminResult::Contacts {
                    cluster: hex(&value.identity.cluster.0),
                    group: hex(&value.identity.group),
                    applied_index: value.applied_index,
                    revision: value.contacts.revision,
                    nodes,
                })
            }
            _ => Err(ClusterAdminError::Invalid),
        }
    }
    async fn configuration(&self) -> Result<ControlConfiguration> {
        match self
            .exchange(AdminCommand::Read(AdminRead::Configuration))
            .await?
        {
            ControlReply::Read(ControlReadResult::Configuration(value)) => {
                self.validate_configuration(&value)?;
                Ok(value)
            }
            _ => Err(ClusterAdminError::Invalid),
        }
    }
    fn validate_configuration(&self, value: &ControlConfiguration) -> Result<()> {
        if value.identity.cluster.0 != self.identity.cluster
            || value.identity.group != crate::network_state::root_group(self.identity.cluster)
            || value.identity.scope != ControlScope::Root
            || value.applied_index == 0
            || value.configuration_index > value.applied_index
        {
            return Err(ClusterAdminError::Invalid);
        }
        value
            .configuration
            .validate()
            .map_err(|_| ClusterAdminError::Invalid)
    }
    /// Prepare against a real quorum-read configuration and sync the exact
    /// request before transmission. An interrupted call is resumed by retry.
    pub async fn membership(
        &self,
        change: MembershipChange,
        expected_index: Option<u64>,
    ) -> Result<AdminResult> {
        let (mut journal, mut saved) = self.journal(true)?;
        if saved
            .latest
            .as_ref()
            .is_some_and(|latest| latest.receipt.is_none() && !latest.superseded)
        {
            return Err(ClusterAdminError::Pending);
        }
        let current = self.configuration().await?;
        if expected_index.is_some_and(|index| index != current.configuration_index) {
            return Err(ControlFailure::CompareFailed.into());
        }
        change
            .apply_to(&current.configuration)
            .map_err(|_| ClusterAdminError::Invalid)?;
        let sequence = saved.next_control;
        let operation = saved.next;
        let next = operation
            .checked_add(1)
            .ok_or(ClusterAdminError::Capacity)?;
        sequence.checked_add(1).ok_or(ClusterAdminError::Capacity)?;
        let request = ControlRequest {
            id: ControlRequestId {
                client: admin_principal(&self.identity).0,
                sequence,
            },
            acknowledged_through: sequence.checked_sub(1).ok_or(ClusterAdminError::Corrupt)?,
            command: ControlCommand::Membership(ControlMembershipCommand {
                expected_configuration_index: current.configuration_index,
                expected: current.configuration,
                change,
            }),
        };
        saved.next = next;
        saved.latest = Some(Latest {
            operation,
            request,
            receipt: None,
            superseded: false,
        });
        save(&mut journal, &saved)?;
        self.drive(&mut journal, &mut saved).await
    }
    pub async fn retry(&self, reference: &str) -> Result<AdminResult> {
        let (mut journal, mut saved) = self.journal(false)?;
        let latest = saved.latest.as_ref().ok_or(ClusterAdminError::Expired)?;
        if reference != operation_id(self.identity.node, latest.operation) || latest.superseded {
            return Err(ClusterAdminError::Expired);
        }
        self.drive(&mut journal, &mut saved).await
    }
    /// Revocation disables both this invitation and any credential it issued.
    /// It does not remove consensus membership or drain data placement.
    pub async fn revoke(
        &self,
        id: [u8; 16],
        expected_revision: Option<u64>,
    ) -> Result<AdminResult> {
        let (mut journal, mut saved) = self.journal(true)?;
        if saved
            .latest
            .as_ref()
            .is_some_and(|latest| latest.receipt.is_none() && !latest.superseded)
        {
            return Err(ClusterAdminError::Pending);
        }
        let reply = self
            .exchange(AdminCommand::Read(AdminRead::PrepareRevocation { id }))
            .await?;
        let ControlReply::Read(ControlReadResult::PreparedRevocation {
            identity,
            applied_index,
            invitation,
            command,
        }) = reply
        else {
            return Err(ClusterAdminError::Invalid);
        };
        if identity.cluster.0 != self.identity.cluster
            || identity.group != crate::network_state::root_group(self.identity.cluster)
            || applied_index == 0
            || invitation != id
            || command.revoked_invitation() != Some(id)
        {
            return Err(ClusterAdminError::Invalid);
        }
        if expected_revision.is_some_and(|expected| expected != command.expected_revision()) {
            return Err(ControlFailure::CompareFailed.into());
        }
        let sequence = saved.next_control;
        let operation = saved.next;
        saved.next = operation
            .checked_add(1)
            .ok_or(ClusterAdminError::Capacity)?;
        sequence.checked_add(1).ok_or(ClusterAdminError::Capacity)?;
        saved.latest = Some(Latest {
            operation,
            request: ControlRequest {
                id: ControlRequestId {
                    client: admin_principal(&self.identity).0,
                    sequence,
                },
                acknowledged_through: sequence.checked_sub(1).ok_or(ClusterAdminError::Corrupt)?,
                command: ControlCommand::Enrollment(command),
            },
            receipt: None,
            superseded: false,
        });
        save(&mut journal, &saved)?;
        self.drive(&mut journal, &mut saved).await
    }
    pub fn inspect(&self) -> Result<AdminResult> {
        let (_, saved) = self.journal(false)?;
        saved_view(self.identity.node, &saved)
    }
    pub async fn transfer(&self, target: u64, expected_index: Option<u64>) -> Result<AdminResult> {
        let current = self.configuration().await?;
        if expected_index.is_some_and(|index| index != current.configuration_index) {
            return Err(ControlFailure::CompareFailed.into());
        }
        let request = ControlTransfer {
            expected_configuration_index: current.configuration_index,
            expected: current.configuration,
            target,
        };
        match self.exchange(AdminCommand::Transfer(request)).await? {
            ControlReply::TransferInitiated { target: actual } if actual == target => {
                Ok(AdminResult::TransferInitiated { target })
            }
            _ => Err(ClusterAdminError::Invalid),
        }
    }
    async fn drive(&self, journal: &mut PrivateJournal, saved: &mut Saved) -> Result<AdminResult> {
        let latest = saved.latest.as_ref().ok_or(ClusterAdminError::Corrupt)?;
        if let Some(receipt) = latest.receipt {
            return Ok(receipt_view(self.identity.node, latest.operation, receipt));
        }
        if latest.superseded {
            return Err(ClusterAdminError::Expired);
        }
        let operation = latest.operation;
        let request = latest.request.clone();
        let ControlReply::Committed(receipt) =
            self.exchange(mutation_command(request.clone())?).await?
        else {
            return Err(ClusterAdminError::Invalid);
        };
        validate_receipt(&request, &receipt)?;
        saved
            .latest
            .as_mut()
            .ok_or(ClusterAdminError::Corrupt)?
            .receipt = Some(receipt);
        saved.next_control = receipt
            .request
            .sequence
            .checked_add(1)
            .ok_or(ClusterAdminError::Capacity)?;
        save(journal, saved)?;
        Ok(receipt_view(self.identity.node, operation, receipt))
    }
    /// Absence and its invalidating precondition must come from one fresh quorum prefix.
    pub async fn reconcile(&self, reference: &str) -> Result<AdminResult> {
        let (mut journal, mut saved) = self.journal(false)?;
        let latest = saved.latest.as_ref().ok_or(ClusterAdminError::Expired)?;
        if reference != operation_id(self.identity.node, latest.operation) {
            return Err(ClusterAdminError::Expired);
        }
        if latest.receipt.is_some() || latest.superseded {
            return saved_view(self.identity.node, &saved);
        }
        let reply = self
            .exchange(AdminCommand::Read(AdminRead::Reconcile {
                sequence: latest.request.id.sequence,
            }))
            .await?;
        let ControlReply::Read(ControlReadResult::AdminReceipt {
            configuration,
            enrollment_revision,
            receipt,
        }) = reply
        else {
            return Err(ClusterAdminError::Invalid);
        };
        self.validate_configuration(&configuration)?;
        let superseded = match &latest.request.command {
            ControlCommand::Membership(command) => {
                configuration.configuration_index > command.expected_configuration_index
            }
            ControlCommand::Enrollment(command) if command.revoked_invitation().is_some() => {
                enrollment_revision > command.expected_revision()
            }
            _ => return Err(ClusterAdminError::Corrupt),
        };
        if let Some(receipt) = receipt {
            validate_receipt(&latest.request, &receipt)?;
            if receipt.committed_index > configuration.applied_index {
                return Err(ClusterAdminError::Invalid);
            }
            saved.next_control = receipt
                .request
                .sequence
                .checked_add(1)
                .ok_or(ClusterAdminError::Capacity)?;
            saved
                .latest
                .as_mut()
                .ok_or(ClusterAdminError::Corrupt)?
                .receipt = Some(receipt);
            save(&mut journal, &saved)?;
        } else if superseded {
            saved
                .latest
                .as_mut()
                .ok_or(ClusterAdminError::Corrupt)?
                .superseded = true;
            save(&mut journal, &saved)?;
        }
        saved_view(self.identity.node, &saved)
    }
    async fn exchange(&self, command: AdminCommand) -> Result<ControlReply> {
        match ControlReply::decode(
            &self.exchange_bytes(command).await?,
            admin_wire_limits().max_frame_bytes as usize,
        )
        .map_err(|_| ClusterAdminError::Invalid)?
        {
            ControlReply::Rejected(error) => Err(error.into()),
            reply => Ok(reply),
        }
    }
    async fn exchange_bytes(&self, command: AdminCommand) -> Result<Vec<u8>> {
        let request = command.request(&self.identity)?;
        let response = UnixRemote::new(self.root.join(ADMIN_SOCKET), admin_wire_limits())?
            .request(&request)
            .await?;
        focal_wire::validate_response(&request, &response, None, &admin_wire_limits())?;
        match response.result {
            Response::Control { response } => Ok(response),
            Response::Error(error) => Err(error.into()),
            _ => Err(ClusterAdminError::Invalid),
        }
    }
    fn journal(&self, create: bool) -> Result<(PrivateJournal, Saved)> {
        let fresh = initialize(&self.root, &self.identity, create)?;
        let mut journal = PrivateJournal::open(self.root.join(DIRECTORY))?;
        let saved = match journal.read()? {
            Some(bytes) => {
                let (saved, tail): (Saved, _) =
                    postcard::take_from_bytes(&bytes).map_err(|_| ClusterAdminError::Corrupt)?;
                if !tail.is_empty() {
                    return Err(ClusterAdminError::Corrupt);
                }
                saved
            }
            None if fresh => {
                let saved = Saved {
                    schema: 2,
                    identity: self.identity.clone(),
                    next: 1,
                    next_control: 1,
                    latest: None,
                };
                save(&mut journal, &saved)?;
                saved
            }
            None => return Err(ClusterAdminError::Corrupt),
        };
        if saved.schema != 2
            || saved.identity != self.identity
            || saved.next == 0
            || saved.next_control == 0
        {
            return Err(ClusterAdminError::Corrupt);
        }
        if let Some(latest) = &saved.latest {
            if latest.request.id.client != admin_principal(&self.identity).0
                || latest.operation.checked_add(1) != Some(saved.next)
                || latest.request.id.sequence > latest.operation
                || latest.request.acknowledged_through.checked_add(1)
                    != Some(latest.request.id.sequence)
            {
                return Err(ClusterAdminError::Corrupt);
            }
            mutation_command(latest.request.clone())?.encode()?;
            if let Some(receipt) = &latest.receipt {
                validate_receipt(&latest.request, receipt)?;
            }
            let expected_control = if latest.receipt.is_some() {
                latest.request.id.sequence.checked_add(1)
            } else {
                Some(latest.request.id.sequence)
            };
            if expected_control != Some(saved.next_control)
                || (latest.superseded && latest.receipt.is_some())
            {
                return Err(ClusterAdminError::Corrupt);
            }
        } else if saved.next != 1 || saved.next_control != 1 {
            return Err(ClusterAdminError::Corrupt);
        }
        Ok((journal, saved))
    }
}
fn initialize(root: &Path, identity: &NodeIdentity, create: bool) -> Result<bool> {
    initialize_named(root, identity, create, DIRECTORY, MARKER)
}
fn initialize_named(
    root: &Path,
    identity: &NodeIdentity,
    create: bool,
    directory: &str,
    marker: &str,
) -> Result<bool> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    let metadata = fs::symlink_metadata(root)?;
    if !metadata.is_dir() || metadata.mode() & 0o077 != 0 {
        return Err(ClusterAdminError::Corrupt);
    }
    let path = root.join(marker);
    let expected = postcard::to_allocvec(identity).map_err(|_| ClusterAdminError::Capacity)?;
    match fs::symlink_metadata(&path) {
        Ok(info) => {
            if !info.is_file()
                || info.uid() != metadata.uid()
                || info.mode() & 0o077 != 0
                || info.nlink() != 1
                || info.len() != expected.len() as u64
            {
                return Err(ClusterAdminError::Corrupt);
            }
            let mut file = File::open(path)?;
            let opened = file.metadata()?;
            if opened.dev() != info.dev() || opened.ino() != info.ino() {
                return Err(ClusterAdminError::Corrupt);
            }
            let mut actual = vec![0; expected.len()];
            file.read_exact(&mut actual)?;
            if actual != expected || !root.join(directory).is_dir() {
                return Err(ClusterAdminError::Corrupt);
            }
            file.sync_all()?;
            File::open(root)?.sync_all()?;
            return Ok(false);
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && create => {
            if root.join(directory).exists() {
                return Err(ClusterAdminError::Corrupt);
            }
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(path)?;
            file.write_all(&expected)?;
            file.sync_all()?;
            File::open(root)?.sync_all()?;
            // No request can be transmitted before the child state is synced.
            // A crash during initialization leaves explicit fail-closed evidence.
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(ClusterAdminError::Expired);
        }
        Err(error) => return Err(error.into()),
    }
    Ok(true)
}
fn save(journal: &mut PrivateJournal, saved: &Saved) -> Result<()> {
    save_value(journal, saved)
}
fn save_value(journal: &mut PrivateJournal, saved: &impl Serialize) -> Result<()> {
    let size =
        postcard::experimental::serialized_size(saved).map_err(|_| ClusterAdminError::Capacity)?;
    if size > LIMIT {
        return Err(ClusterAdminError::Capacity);
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(size)
        .map_err(|_| ClusterAdminError::Capacity)?;
    bytes.resize(size, 0);
    postcard::to_slice(saved, &mut bytes).map_err(|_| ClusterAdminError::Capacity)?;
    journal.replace(&bytes)?;
    Ok(())
}
fn validate_receipt(request: &ControlRequest, receipt: &ControlReceipt) -> Result<()> {
    let bytes = postcard::to_allocvec(request).map_err(|_| ClusterAdminError::Capacity)?;
    let digest = blake3::derive_key("focal.control.request.v1", &bytes);
    if receipt.request != request.id
        || receipt.request_hash != digest
        || receipt.committed_index == 0
        || receipt.committed_term == 0
    {
        return Err(ClusterAdminError::Invalid);
    }
    Ok(())
}
fn operation_id(node: u64, sequence: u64) -> String {
    format!("a1:{node:016x}:{sequence:016x}")
}
fn saved_view(node: u64, saved: &Saved) -> Result<AdminResult> {
    let latest = saved.latest.as_ref().ok_or(ClusterAdminError::Expired)?;
    match latest.receipt {
        Some(receipt) => Ok(receipt_view(node, latest.operation, receipt)),
        None => Ok(AdminResult::Request {
            operation_id: operation_id(node, latest.operation),
            state: if latest.superseded {
                "Superseded"
            } else {
                "Pending"
            }
            .into(),
        }),
    }
}
fn receipt_view(node: u64, operation: u64, receipt: ControlReceipt) -> AdminResult {
    AdminResult::Committed {
        operation_id: operation_id(node, operation),
        client: hex(&receipt.request.client),
        sequence: receipt.request.sequence,
        request_hash: hex(&receipt.request_hash),
        committed_index: receipt.committed_index,
        committed_term: receipt.committed_term,
    }
}
fn configuration_view(value: ControlConfiguration) -> AdminConfiguration {
    AdminConfiguration {
        cluster: hex(&value.identity.cluster.0),
        group: hex(&value.identity.group),
        genesis: hex(&value.identity.genesis),
        applied_index: value.applied_index,
        configuration_index: value.configuration_index,
        voters: value.configuration.voters,
        learners: value.configuration.learners,
        voters_outgoing: value.configuration.voters_outgoing,
        learners_next: value.configuration.learners_next,
        auto_leave: value.configuration.auto_leave,
    }
}
pub(crate) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
fn mutation_command(request: ControlRequest) -> Result<AdminCommand> {
    match &request.command {
        ControlCommand::Membership(_) => Ok(AdminCommand::Membership(Box::new(request))),
        ControlCommand::Enrollment(command) if command.revoked_invitation().is_some() => {
            Ok(AdminCommand::Revocation(Box::new(request)))
        }
        _ => Err(ClusterAdminError::Corrupt),
    }
}
fn invitations_view(
    identity: ControlIdentity,
    applied_index: u64,
    revision: u64,
    entries: Vec<focal_enrollment::InvitationStatus>,
    next: Option<[u8; 16]>,
) -> AdminResult {
    AdminResult::Invitations {
        cluster: hex(&identity.cluster.0),
        group: hex(&identity.group),
        applied_index,
        revision,
        entries: entries
            .into_iter()
            .map(|entry| AdminInvitation {
                id: hex(&entry.id),
                role: match entry.role {
                    focal_enrollment::EnrollmentRole::Node => "node",
                    focal_enrollment::EnrollmentRole::Client => "client",
                }
                .into(),
                expires_at: entry.expires_at,
                revoked: entry.revoked,
                credential: entry.enrollment.map(|credential| AdminCredential {
                    node: credential.node,
                    principal: hex(&credential.principal),
                    issued_at: credential.issued_at,
                    expires_at: credential.expires_at,
                    revision: credential.revision,
                    certificate_fingerprint: hex(&credential.certificate_fingerprint),
                }),
            })
            .collect(),
        next: next.map(|id| hex(&id)),
    }
}
