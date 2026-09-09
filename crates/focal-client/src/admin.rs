//! Public administrative results for root control and installed application groups;
//! enrollment, membership, placement and achieved data durability remain distinct.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminNodeIdentity {
    pub node: u64,
    pub cluster: String,
    pub tenant: String,
    pub session: String,
    pub issuer: String,
    pub root: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminNodeHealth {
    pub node: u64,
    pub root_stopped: bool,
    pub root_leader: u64,
    pub root_term: u64,
    pub root_applied_index: u64,
    pub fleet_stopped: bool,
    pub installed: usize,
    pub running: usize,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminNodeConfiguration {
    pub node: u64,
    pub network_schema: u16,
    pub listen: String,
    pub advertise: String,
    pub root_group: String,
    pub root_tenant: String,
    pub root_session: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminReplicaDiagnostics {
    pub node: u64,
    pub cluster: String,
    pub session: String,
    pub group: String,
    pub leader: u64,
    pub term: u64,
    pub committed_index: u64,
    pub applied_index: u64,
    pub sequence: u64,
    pub pending: usize,
    pub authoritative: bool,
    pub persistence_pending: bool,
    pub checkpoint_pending: bool,
    pub compiled_managed_decoder: String,
    pub required_decoder: Option<String>,
    pub managed_active: bool,
    pub compiled_native_decoder: String,
    pub native_hosted: bool,
    pub native_ready: bool,
    pub native_active: bool,
    pub native_import_pending: bool,
    /// Native admission is open here: activation applied, genesis committed
    /// and this replica is the authority with its native owner rebuilt.
    pub native_authoritative: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminReplicaStatus {
    pub session: String,
    pub group: String,
    pub node: u64,
    pub leader: u64,
    pub term: u64,
    pub sequence: u64,
    pub stopped: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminReplicaMembership {
    pub cluster: String,
    pub tenant: String,
    pub session: String,
    pub group: String,
    pub configuration_index: u64,
    pub voters: Vec<u64>,
    pub learners: Vec<u64>,
    pub voters_outgoing: Vec<u64>,
    pub learners_next: Vec<u64>,
    pub auto_leave: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminConfiguration {
    pub cluster: String,
    pub group: String,
    pub genesis: String,
    pub applied_index: u64,
    pub configuration_index: u64,
    pub voters: Vec<u64>,
    pub learners: Vec<u64>,
    pub voters_outgoing: Vec<u64>,
    pub learners_next: Vec<u64>,
    pub auto_leave: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminContact {
    pub node: u64,
    pub principal: String,
    pub certificate_fingerprint: String,
    pub advertise: String,
    pub server_name: String,
    pub generation: u64,
    pub committed_index: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminInvitation {
    pub id: String,
    pub role: String,
    pub expires_at: i64,
    pub revoked: bool,
    pub credential: Option<AdminCredential>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminCredential {
    pub node: Option<u64>,
    pub principal: String,
    pub issued_at: i64,
    pub expires_at: i64,
    pub revision: u64,
    pub certificate_fingerprint: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AdminResult {
    NodeIdentity {
        identity: AdminNodeIdentity,
    },
    NodeHealth {
        health: AdminNodeHealth,
    },
    NodeConfiguration {
        configuration: AdminNodeConfiguration,
    },
    ReplicaDiagnostics {
        diagnostics: AdminReplicaDiagnostics,
    },
    ReplicaTransferInitiated {
        session: String,
        group: String,
        target: u64,
    },
    ReplicaNativeActivationProposed {
        session: String,
        group: String,
    },
    Replicas {
        node: u64,
        management_sequence: u64,
        replicas: Vec<AdminReplicaStatus>,
        next: Option<String>,
    },
    ReplicaMembership {
        membership: AdminReplicaMembership,
    },
    ReplicaCommitted {
        operation_id: String,
        request_id: String,
        request_hash: String,
        committed_index: u64,
        committed_term: u64,
        membership: AdminReplicaMembership,
    },
    ReplicaRequest {
        operation_id: String,
        session: String,
        group: String,
        state: String,
    },
    Membership {
        node: u64,
        leader: u64,
        term: u64,
        applied_index: u64,
        voters: Vec<u64>,
        learners: Vec<u64>,
    },
    Configuration {
        configuration: AdminConfiguration,
    },
    Contacts {
        cluster: String,
        group: String,
        applied_index: u64,
        revision: u64,
        nodes: Vec<AdminContact>,
    },
    Committed {
        operation_id: String,
        client: String,
        sequence: u64,
        request_hash: String,
        committed_index: u64,
        committed_term: u64,
    },
    TransferInitiated {
        target: u64,
    },
    Request {
        operation_id: String,
        state: String,
    },
    Invitations {
        cluster: String,
        group: String,
        applied_index: u64,
        revision: u64,
        entries: Vec<AdminInvitation>,
        next: Option<String>,
    },
    InvitationWritten {
        name: String,
        output: String,
    },
    /// This node's own credential was renewed under the same key.
    CredentialRenewed {
        node: u64,
        principal: String,
        issued_at: i64,
        expires_at: i64,
        certificate_fingerprint: String,
        renewals: u64,
    },
}
