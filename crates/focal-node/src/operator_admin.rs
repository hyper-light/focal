//! Authenticated local diagnostics. No read starts durable IO or activation.
use super::*;
use focal_client::admin::{
    AdminNodeConfiguration, AdminNodeHealth, AdminNodeIdentity, AdminReplicaDiagnostics,
};
use focal_model::SessionId;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum OperatorRead {
    Identity,
    Health,
    Configuration,
    Replica { session: SessionId },
}
impl OperatorRead {
    pub(super) fn validate(self) -> Result<(), AccessError> {
        if matches!(self,Self::Replica{session} if session.is_zero()) {
            Err(AccessError::InvalidRequest)
        } else {
            Ok(())
        }
    }
}
#[derive(Serialize, Deserialize)]
pub(crate) enum OperatorReply {
    Identity(AdminNodeIdentity),
    Health(AdminNodeHealth),
    Configuration(AdminNodeConfiguration),
    Replica(Box<AdminReplicaDiagnostics>),
    Error(AccessError),
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
                Ok(OperatorReply::Health(AdminNodeHealth {
                    node: self.identity.node,
                    root_stopped: root.stopped,
                    root_leader: root.leader,
                    root_term: root.term,
                    root_applied_index: root.applied_index,
                    fleet_stopped: fleet.stopped,
                    installed: fleet.installed,
                    running: fleet.running,
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
