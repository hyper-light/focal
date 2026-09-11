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
    /// The placement agent's bounded view (24 §7); absent on a node without
    /// a network service.
    #[serde(default)]
    pub placement: Option<AdminPlacementAgent>,
}
/// The node's readiness (doc 08 §9): the four probes a supervisor asks,
/// with the facts they are derived from. `alive` holds whenever the node
/// answers; `catching_up` when every replica it hosts and its root replica
/// follow a known leader with nothing pending but the node leads none of
/// them; `authoritative` when it leads the root or a hosted session's log
/// at a committed prefix; `policy_satisfied` when every session it hosts
/// has its desired durability achieved in the directory with nothing
/// blocking it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminReadiness {
    pub node: u64,
    pub alive: bool,
    pub catching_up: bool,
    pub authoritative: bool,
    pub policy_satisfied: bool,
    pub root: AdminRootProgress,
    pub sessions: Vec<AdminSessionReadiness>,
    /// Sessions beyond the report bound were left out (and count as not
    /// satisfying the policy).
    pub truncated: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminRootProgress {
    pub leader: u64,
    pub term: u64,
    pub applied_index: u64,
    pub stopped: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminSessionReadiness {
    pub tenant: String,
    pub session: String,
    pub leader: u64,
    pub authoritative: bool,
    pub committed_index: u64,
    pub applied_index: u64,
    pub seed_pending: bool,
    pub import_pending: bool,
    /// Objects a retained delivery names are still being pulled (24 §20).
    pub custody_pending: bool,
    pub stopped: bool,
    /// The directory's desired and achieved durability for the session;
    /// absent while the directory does not list it.
    pub desired_max_failures: Option<u16>,
    pub achieved_max_failures: Option<u16>,
    pub blocked_by: Vec<String>,
}
/// The placement agent: the exact-retry intents it completed, the copies
/// it installed and the last error its pass hit, cleared by the next pass
/// that succeeds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminPlacementAgent {
    pub root_intents: u64,
    pub partition_intents: u64,
    pub installed: Vec<String>,
    pub last_error: Option<String>,
    /// The last intent the directory or root refused before admission,
    /// by kind and failure; cleared by the next intent that commits.
    #[serde(default)]
    pub last_refusal: Option<String>,
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
    /// The chunks of a seeded checkpoint this replica still lacks (25 §5),
    /// `None` when no seeded snapshot is waiting.
    pub seed_chunks_missing: Option<usize>,
    /// The content objects a retained delivery names that this replica does
    /// not hold yet (24 §20), `None` when none is waiting.
    pub custody_objects_missing: Option<usize>,
    /// A delivery retained after a retryable refusal, resumed at the next poll.
    pub delivery_retained: bool,
    /// Native admission is open here: activation applied, genesis committed
    /// and this replica is the authority with its native owner rebuilt.
    pub native_authoritative: bool,
    /// Entries applied past the last snapshot: the log kept beyond the
    /// checkpoint (26 §3).
    pub log_entries_since_checkpoint: u64,
    /// The retention floor of a native session and its inputs (26 §3):
    /// the published prefix, what registered consumers still need, what
    /// the archive reports holding, the least of them, and what holds the
    /// floor there (`cursors` or `archive`). Absent for a session that is
    /// not native.
    pub retention: Option<AdminRetention>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminRetention {
    pub published: u64,
    pub cursors: u64,
    pub archived: u64,
    pub floor: u64,
    pub blocker: String,
    /// Families retired to the archive through the applied prefix (26 §4).
    pub retired: u64,
    /// A retirement record this authority proposed is still in flight.
    pub retiring: bool,
}
/// One retired claim's archive bundle as this node holds and verifies it
/// (26 §4): the continuation the core keeps, and what the bundle says
/// about itself once its digest and every row check.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminArchiveBundle {
    pub session: String,
    pub group: String,
    pub claim: String,
    pub status: String,
    pub retired_at: u64,
    /// The bundle's content root and length: what names the object.
    pub bundle: String,
    pub bytes: u64,
    /// The prefix the bundle claims.
    pub through: u64,
    /// The bundle's frame digest, `None` until the bundle verified.
    pub digest: Option<String>,
    /// Whether this node holds the bundle and it verified structurally.
    pub verified: bool,
    pub root: Option<String>,
    pub members: Vec<String>,
    pub rows: u64,
    pub families: Vec<AdminArchiveFamily>,
    /// Required copies of the ledger's placement holding a receipt for the
    /// bundle, in node order.
    pub receipts: Vec<u64>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminArchiveFamily {
    pub family: String,
    pub rows: u64,
}
/// The collector's settings on one node (26 §5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminGcConfig {
    pub interval_ms: u64,
    pub grace_ms: u64,
    pub quarantine_ms: u64,
    pub terminal_grace_ms: u64,
    pub keep_records: u64,
    pub max_marks: u64,
}
/// What one content-store pass did (26 §5).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminCollectorReport {
    pub visited: u64,
    pub uploads_expired: u64,
    pub terminals_released: u64,
    pub objects_quarantined: u64,
    pub chunks_quarantined: u64,
    pub chunks_deferred: u64,
    pub records_quarantined: u64,
    pub receipts_quarantined: u64,
    pub deleted: u64,
    pub bytes_deleted: u64,
    pub opaque_domains: u64,
    pub complete: bool,
}
/// What the seed sweeps of one pass did across the replicas this node hosts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminSeedReport {
    pub replicas: u64,
    pub visited: u64,
    pub removed: u64,
    pub bytes_removed: u64,
}
/// One collector pass on one node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminGcPass {
    pub started_ms: u64,
    pub finished_ms: u64,
    /// Replicas whose committed rows contributed roots.
    pub replicas: u64,
    pub protected_objects: u64,
    /// Domains this node holds copies for without a core to know their
    /// references: never collected.
    pub opaque_domains: u64,
    /// Bundles named by a continuation that this node could not read or
    /// verify; their proof is protected by name only.
    pub bundles_unreadable: u64,
    pub content: AdminCollectorReport,
    pub seeds: AdminSeedReport,
}
/// The collector's state on one node (26 §5): its settings, whether a pass
/// is in progress, how many completed, and the last one's report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminGc {
    pub node: u64,
    pub config: AdminGcConfig,
    pub passes: u64,
    pub collecting: bool,
    pub last: Option<AdminGcPass>,
}

/// The volume envelope every durable owner of a node's data directory
/// promises its bytes to (24 §10), as sampled now.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminDiskStats {
    /// Free bytes on the volume at the last sample; `None` before one.
    pub free: Option<u64>,
    /// Bytes promised and not yet returned, across every kind and lane.
    pub outstanding: u64,
    pub ordinary_outstanding: u64,
    /// Never spent: the watermark below which fresh work is refused.
    pub headroom: u64,
    /// Spendable only by work that completes something already admitted.
    pub completion_reserve: u64,
    pub wal: u64,
    pub checkpoint: u64,
    pub content: u64,
    pub archive: u64,
    pub staging: u64,
}
/// The archive agent on one node (26 §4): its settings and what it did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminArchiveAgent {
    pub interval_ms: u64,
    /// The logical time a family's last event settles before it is offered.
    pub grace_ms: u64,
    pub ticks: u64,
    /// Retirement records proposed.
    pub proposed: u64,
    /// Bundles sealed whose required copies have not all answered yet.
    pub waiting: u64,
    pub last_tick_ms: u64,
}
/// One hosted native session's retention as the storage view lists it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminSessionRetention {
    pub tenant: String,
    pub session: String,
    pub authoritative: bool,
    pub log_entries_since_checkpoint: u64,
    pub retention: Option<AdminRetention>,
}
/// The storage view of one node (26 §7): the volume's pressure, what is
/// staged, the agents' settings and progress, and every hosted session's
/// oldest retained prefix.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminStorage {
    pub node: u64,
    pub disk: AdminDiskStats,
    /// Uploads in progress and the bytes they have staged.
    pub staged_uploads: u64,
    pub staged_bytes: u64,
    pub retire: AdminArchiveAgent,
    pub gc: AdminGcConfig,
    /// Sessions this node hosts, bounded; `truncated` when more were left out.
    pub sessions: Vec<AdminSessionRetention>,
    pub truncated: bool,
}

/// One object a repair could not recover from any required copy (24 §20).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminUnrecoverableObject {
    pub artifact: String,
    pub root: String,
    pub length: u64,
    /// The copies asked for it.
    pub asked: u32,
}
/// A repair pass over a hosted session's custody on one node (24 §20):
/// what the walk over the committed artifact projection found and did.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminRepair {
    pub tenant: String,
    pub session: String,
    pub node: u64,
    /// The committed prefix the walk covered: the session's legacy
    /// sequence and the applied log index the checkpoint was taken at.
    pub sequence: u64,
    pub index: u64,
    pub artifacts: u64,
    pub objects: u64,
    /// Objects this node held and verified.
    pub verified: u64,
    /// Objects recopied onto this node from another required copy.
    pub repaired: u64,
    /// Objects given to another required copy that lacked them.
    pub pushed: u64,
    /// The first unrecoverable objects, bounded; `unrecoverable_count` is exact.
    pub unrecoverable: Vec<AdminUnrecoverableObject>,
    pub unrecoverable_count: u64,
    /// No required copy holds every object: the session needs a restore.
    pub restore_required: bool,
    /// The projection was walked to its end.
    pub complete: bool,
    /// Where a bounded or interrupted walk resumes (`--after`).
    pub next_after: Option<String>,
}
/// The committed prefix a backup holds (26 §6).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminBackupPrefix {
    pub cluster: String,
    pub tenant: String,
    pub session: String,
    pub group: String,
    pub node: u64,
    /// The legacy prefix the envelope holds.
    pub sequence: u64,
    /// The native prefix the envelope holds.
    pub native_sequence: u64,
    pub index: u64,
    pub term: u64,
    pub route_epoch: u64,
    pub placement_epoch: u64,
    pub membership_epoch: u64,
}
/// A backup written by `cluster backup create` (26 §6).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminBackup {
    pub output: String,
    pub created_ms: u64,
    pub prefix: AdminBackupPrefix,
    pub checkpoint_hash: String,
    pub checkpoint_bytes: u64,
    /// The native decoder the backup's log promised, hex.
    pub decoder: String,
    pub seeds: u64,
    pub objects: u64,
    pub chunks: u64,
    /// Archive bundles among the objects, each with the proof its header names.
    pub bundles: u64,
    pub files: u64,
    pub bytes: u64,
}
/// What `cluster backup verify` found (26 §6).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminBackupVerification {
    pub input: String,
    pub prefix: AdminBackupPrefix,
    pub checkpoint_verified: bool,
    pub seeds_listed: u64,
    pub seeds_verified: u64,
    pub objects_listed: u64,
    pub objects_verified: u64,
    pub chunks_verified: u64,
    pub bytes_verified: u64,
    /// The envelope decodes, rebuilds, and names exactly what the manifest lists.
    pub inventory_matches: bool,
    /// This binary carries the decoder the backup's log promised.
    pub decoder_supported: bool,
    /// Every check passed; the backup can be restored by a binary that
    /// carries its decoder.
    pub complete: bool,
    pub problems: Vec<String>,
}

/// A session restored from a backup on this node (26 §6).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminRestore {
    pub input: String,
    pub tenant: String,
    pub session: String,
    /// The log group the restored session lives in: the backup's own when
    /// the old incarnation continues, a derived one for a recovery.
    pub group: String,
    pub node: u64,
    /// `same_incarnation` or `recovery_incarnation`.
    pub decision: String,
    pub fenced_by: Option<u64>,
    /// Members of the backup's membership that are not fenced (empty for
    /// a continued incarnation).
    pub unfenced: Vec<u64>,
    /// The prefix the backup held, which the restored session starts at.
    pub prefix: AdminBackupPrefix,
    pub objects_imported: u64,
    pub seeds_installed: u64,
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
/// One member of a native session's movement map (doc 25 §6).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminRangeMember {
    pub id: String,
    pub generation: u64,
    /// The affinity the member starts at; absent for the first member.
    pub start: Option<String>,
    pub end: Option<String>,
    /// The holding replica's node; absent when the voters hold it.
    pub holder: Option<u64>,
    pub holder_generation: Option<u64>,
    pub readers: Vec<u64>,
    /// Rows the member holds on the replica that answered.
    pub entries: usize,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminRangePending {
    pub operation: String,
    pub old_epoch: u64,
    pub seed: u64,
    pub sources: Vec<String>,
    pub replacements: Vec<AdminRangeMember>,
    pub snapshots: Vec<String>,
    pub barrier: Option<u64>,
    pub seals: Vec<String>,
    pub ready: Vec<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminRangeHistory {
    pub operation: String,
    pub epoch: u64,
    pub proofs: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminRangeView {
    pub epoch: u64,
    pub ordinal: u64,
    pub prefix: u64,
    pub in_flight: bool,
    pub refusals: u64,
    pub members: Vec<AdminRangeMember>,
    pub pending: Option<AdminRangePending>,
    pub history: Vec<AdminRangeHistory>,
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
    /// A native session's retention floor and archive counts (26 §3, §4).
    Retention {
        session: String,
        group: String,
        retention: Option<AdminRetention>,
    },
    /// A retired claim's archive bundle (26 §4); `None` when the claim has
    /// no continuation on this replica.
    Archive {
        session: String,
        claim: String,
        archive: Option<AdminArchiveBundle>,
    },
    /// The collector's state on this node (26 §5).
    Gc {
        gc: AdminGc,
    },
    /// A quarantined object brought back, or not found in quarantine.
    GcRestored {
        domain: String,
        root: String,
        restored: bool,
    },
    /// A backup written at a declared prefix (26 §6).
    BackupCreated {
        backup: AdminBackup,
    },
    /// A backup verified file by file and against its own envelope (26 §6).
    BackupVerified {
        verification: AdminBackupVerification,
    },
    /// A session restored from a backup on this node (26 §6).
    Restored {
        restored: AdminRestore,
    },
    /// A repair pass over a hosted session's custody on this node (24 §20).
    Repaired {
        repair: AdminRepair,
    },
    /// The storage view of this node (26 §7).
    Storage {
        storage: AdminStorage,
    },
    /// The node's readiness probes with their facts (08 §9).
    Readiness {
        readiness: AdminReadiness,
    },
    /// The node's metrics as Prometheus text exposition (24 §23).
    Metrics {
        text: String,
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
    ReplicaCheckpointed {
        session: String,
        group: String,
    },
    /// The committed movement map of a native session (doc 25 §6).
    ReplicaRanges {
        session: String,
        group: String,
        ranges: AdminRangeView,
    },
    /// The transfer an operator's move request denotes.
    RangeMoveProposed {
        tenant: String,
        session: String,
        member: String,
        node: u64,
        operation: String,
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
    /// A node's placement eligibility as committed (24 §19): drained when
    /// `eligible` is false. `changed` is false when the grant already
    /// stated it, in which case nothing was committed.
    NodeEligibility {
        node: u64,
        generation: u64,
        eligible: bool,
        changed: bool,
        operation_id: Option<String>,
        committed_index: Option<u64>,
    },
    /// A drained node removed from the cluster (24 §19): its root-group
    /// membership, when it had one, and the credential its invitation
    /// issued.
    NodeRemoved {
        node: u64,
        membership_removed: bool,
        invitation: Option<String>,
        revoked: bool,
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
        /// The identity of the key the credential holds (24 §11).
        #[serde(default)]
        key_identity: String,
        #[serde(default)]
        rotations: u64,
    },
    /// This node's credential rotated to a fresh key under the same identity
    /// (24 §11).
    CredentialRotated {
        node: u64,
        principal: String,
        issued_at: i64,
        expires_at: i64,
        certificate_fingerprint: String,
        key_identity: String,
        renewals: u64,
        rotations: u64,
    },
    /// Every directory partition this node acts on, with each session's
    /// desired and achieved guarantee and what blocks it.
    Placement {
        placement: AdminPlacement,
    },
    /// The bounded next actions the placement controller would take.
    Plan {
        actions: Vec<AdminPlannedAction>,
    },
    /// The tenants the cluster serves: the founder's own and every admitted one.
    Tenants {
        founder: String,
        applied_index: u64,
        revision: u64,
        admitted: Vec<String>,
    },
    /// The upgrade fence and every node's capability (24 §21).
    Upgrade {
        upgrade: AdminUpgrade,
    },
    /// The upgrade fence after an activation; `changed` when this call
    /// raised it.
    FenceActivated {
        upgrade: AdminUpgrade,
        changed: bool,
    },
    /// An application session created on a node, or found again by its name.
    SessionCreated {
        name: String,
        tenant: String,
        session: String,
        group: String,
        node: u64,
        existing: bool,
    },
    /// The plan an operator's durability request denotes for a session.
    SessionPlanned {
        tenant: String,
        session: String,
        operation: String,
        voters: Vec<u64>,
        survive: String,
        max_failures: u16,
        /// `planned`, `pending` (a plan is under way) or `satisfied`.
        state: String,
        /// The plan was reported without being journaled.
        #[serde(default)]
        dry_run: bool,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminPlacement {
    /// When the agent last observed these partitions (unix seconds).
    pub observed_at: i64,
    pub partitions: Vec<AdminPartition>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminPartition {
    pub partition: String,
    pub group: String,
    pub namespace_start: String,
    pub namespace_end: Option<String>,
    pub epoch: u64,
    pub revision: u64,
    pub sealed: Option<AdminSeal>,
    pub nodes: Vec<AdminPlacementNode>,
    pub sessions: Vec<AdminSessionPlacement>,
    /// Sessions beyond the report bound were left out.
    pub truncated: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminSeal {
    pub operation: String,
    pub destination: String,
    pub moved_start: String,
    pub moved_end: Option<String>,
    pub next_epoch: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminPlacementNode {
    pub node: u64,
    pub generation: u64,
    pub eligible: bool,
    pub alive: bool,
    pub incarnation: Option<u64>,
    pub available_memory: Option<u64>,
    pub active_weight: Option<u64>,
    pub disk_available: Option<u64>,
    /// The capability level the node's binary last reported (24 §21).
    pub capability: Option<u32>,
    /// The failure-domain labels the node announced (24 §22); a region the
    /// directory knows only by identity is shown as its hex identity.
    pub region: Option<String>,
    pub zone: Option<String>,
    /// The address the node's committed contact announces and, when its
    /// operator gave one, the name peers re-resolve (24 §24).
    pub advertise: Option<String>,
    pub endpoint: Option<String>,
    /// `active` while the enrollment registry authorizes the node's
    /// credential, `retired` once it is revoked or expired, `unknown` when
    /// the registry could not be read (24 §11).
    pub credential: String,
}
/// The upgrade fence and the capability levels around it (24 §21).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminUpgrade {
    /// The committed fence: zero until one is activated.
    pub fence_level: u32,
    pub fence_activated_at: i64,
    pub fence_revision: u64,
    /// The level this node's binary implements, and the one it announces
    /// (lowered only through `FOCAL_CAPABILITY_LEVEL`).
    pub binary_level: u32,
    pub announced_level: u32,
    pub applied_index: u64,
    pub registry_revision: u64,
    /// Every node the directory lists with the level it last reported.
    pub nodes: Vec<AdminNodeCapability>,
    /// The highest fence every listed node supports (zero while a node
    /// has not reported).
    pub activatable: u32,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminNodeCapability {
    pub node: u64,
    /// Zero until the node reports its load under a binary that announces.
    pub capability: u32,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminSessionPlacement {
    pub tenant: String,
    pub session: String,
    pub route_epoch: u64,
    pub membership_epoch: u64,
    pub placement_epoch: u64,
    pub preferred_leader: u64,
    /// The node that founded the session's log; unknown for sessions
    /// recorded before the directory kept it.
    pub founder: Option<u64>,
    pub voters: Vec<u64>,
    pub materializers: Vec<u64>,
    pub content_copies: Vec<u64>,
    pub survive: String,
    pub max_failures: u16,
    /// The session's residency boundary and ordering homes as region labels
    /// (24 §22); empty is no boundary.
    pub residency: Vec<String>,
    pub home_regions: Vec<String>,
    pub achieved_survive: Option<String>,
    pub achieved_max_failures: Option<u16>,
    pub blocked_by: Vec<String>,
    pub phase: Option<String>,
    pub pending: Option<AdminPendingPlacement>,
    pub retiring: Vec<u64>,
    /// The range epoch the directory last published holders at; absent
    /// until the session's controller published one.
    pub range_epoch: Option<u64>,
    /// The published members in key order with their holding replicas.
    pub holders: Vec<AdminRangeHolder>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminRangeHolder {
    pub member: String,
    pub start: Option<String>,
    /// The holding replica's node; absent when the voters hold the member.
    pub node: Option<u64>,
    pub generation: Option<u64>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminPendingPlacement {
    pub operation: String,
    pub phase: String,
    pub voters: Vec<u64>,
    pub progress: Vec<AdminAssignmentProgress>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminAssignmentProgress {
    pub node: u64,
    pub phase: String,
    pub attempt: u32,
    pub through: u64,
    pub refusal: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminPlannedAction {
    pub partition: String,
    pub tenant: Option<String>,
    pub session: Option<String>,
    pub action: String,
}
