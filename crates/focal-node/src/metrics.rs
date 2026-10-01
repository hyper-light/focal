//! Metrics (doc 08 §9; 24 §23): one bounded snapshot of what this node
//! knows about itself, sampled by the service on a fixed cadence and
//! rendered as Prometheus text over the kernel-authenticated admin socket
//! (`cluster node metrics`) and, when the operator configures
//! `node.metrics_listen`, over a read-only loopback HTTP/1.0 endpoint. Every
//! series carries the node's fixed labels; nothing here is a quorum read
//! and nothing here authorizes a change.
use crate::{admission::AdmissionReport, fleet::FleetStatus};
use focal_client::admin::AdminRetention;
use focal_log::WalWriterStats;
use focal_memory::{BudgetStats, DiskStats};
use focal_wire::PeerPoolStats;
use std::fmt::Write as _;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// How often the service samples a fresh snapshot.
pub const SAMPLE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);
/// Sessions a snapshot lists at most; more are counted as truncated.
pub const MAX_SESSIONS: usize = 512;
/// Scrapes the loopback endpoint serves at once: as many as the admin
/// socket admits operators; a connection beyond them is closed unanswered,
/// as the socket closes one.
pub fn max_scrapes() -> usize {
    crate::network_admin::admin_wire_limits().max_connections
}
/// The longest request the loopback endpoint reads.
const MAX_REQUEST_BYTES: usize = 4096;

/// The fixed labels every series carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricLabels {
    pub node: u64,
    pub cluster: String,
    pub region: Option<String>,
    pub zone: Option<String>,
    /// `founder` or `host`.
    pub role: &'static str,
}
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RootMetrics {
    pub leader: u64,
    pub term: u64,
    pub applied_index: u64,
    pub stopped: bool,
    /// The metadata compaction floor; `applied_index - snapshot_index` is the
    /// retained root log length.
    pub snapshot_index: u64,
    /// Per-peer replication progress this node tracks as the root leader.
    pub peers: Vec<focal_consensus::PeerProgress>,
    /// The root owner's tick period in force, in milliseconds (27 §3.1 P2).
    pub tick_period_ms: u64,
    /// Periods in which the owner's replica was not ticked: it was
    /// refused the room or still persisted.
    pub refused_periods: u64,
    /// The longest a period of the owner took, from one to the next, in
    /// milliseconds: a stall of the owner, in a disk or a thread that was
    /// not scheduled, which its followers may have taken for its death.
    pub longest_period_ms: u64,
    /// The slowest measured voter path's round-trip tail, in microseconds.
    pub broadcast_tail_us: u64,
    /// Round trips that fed the pace; zero means the configured period.
    pub pace_samples: u64,
    /// Periods the root owner has run since it started: its progress, which
    /// a wait on this node is charged in (27 §3.1 P8).
    pub periods: u64,
    /// Exchanges of the root replica the driver could not make at all, each
    /// told to the core; reports coalesced into a peer already held, and
    /// reports dropped beyond the bound (27 §3.3).
    pub peers_unreachable: u64,
    pub peer_reports_coalesced: u64,
    pub peer_reports_dropped: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PeerRtt {
    pub peer: u64,
    pub rtt_ms: u64,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LivenessMetrics {
    pub alive: u64,
    pub suspect: u64,
    pub dead: u64,
    pub health_score: u8,
    pub probes_sent: u64,
    pub probes_answered: u64,
    pub probe_timeouts: u64,
    pub suspicions: u64,
    pub deaths: u64,
    pub refutations: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CredentialMetrics {
    pub expires_at: i64,
    pub renewals: u64,
    pub rotations: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SessionMetrics {
    pub tenant: String,
    pub session: String,
    /// Whether the session's owner answered this round; without it the
    /// owner-side series are absent from the text, never zero.
    pub observed: bool,
    pub leader: u64,
    pub term: u64,
    pub committed_index: u64,
    pub applied_index: u64,
    pub sequence: u64,
    pub pending: u64,
    pub authoritative: bool,
    /// The leader the committed placement prefers (27 §5).
    pub preferred_leader: Option<u64>,
    pub leader_returns: u64,
    pub leader_returns_failed: u64,
    pub native_authoritative: bool,
    pub log_entries_since_checkpoint: u64,
    pub retention: Option<AdminRetention>,
    pub seed_chunks_missing: Option<u64>,
    pub custody_objects_missing: Option<u64>,
    pub delivery_retained: bool,
    pub route_epoch: Option<u64>,
    pub placement_epoch: Option<u64>,
    pub desired_max_failures: Option<u16>,
    pub achieved_max_failures: Option<u16>,
    pub blocked: Option<u64>,
    /// The replica owner's tick period in force, in milliseconds, the
    /// measured tail it was derived from, in microseconds, and how many
    /// round trips fed it (27 §3.1 P2).
    pub tick_period_ms: u64,
    /// Periods the session's owner has run: the unit its replica's election
    /// timer counts, and what a wait on it is charged in (27 §3.1 P8).
    pub periods: u64,
    /// Periods in which the owner's replica was not ticked: it was
    /// refused the room or still persisted.
    pub refused_periods: u64,
    /// Exchanges of the session's replica the driver could not make at
    /// all, each told to the core (27 §3.3); reports coalesced into a peer
    /// already held for the core, and reports dropped beyond the bound.
    pub peers_unreachable: u64,
    pub peer_reports_coalesced: u64,
    pub peer_reports_dropped: u64,
    /// The longest a period of the owner took, in milliseconds
    /// (`RootMetrics::longest_period_ms`).
    pub longest_period_ms: u64,
    pub broadcast_tail_us: u64,
    pub pace_samples: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentMetrics {
    pub root_intents: u64,
    pub partition_intents: u64,
    pub installed: u64,
    pub last_error: bool,
    pub last_refusal: bool,
    pub admission: AdmissionReport,
}
/// One sample of the node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub sampled_ms: u64,
    pub labels: MetricLabels,
    pub memory: BudgetStats,
    pub disk: Option<DiskStats>,
    pub staged_uploads: u64,
    pub staged_bytes: u64,
    pub wal: Option<WalWriterStats>,
    pub fleet: FleetStatus,
    pub root: RootMetrics,
    pub peers: PeerPoolStats,
    /// Who holds the listener's connections and what it refused (27 §3.1
    /// P5).
    pub listener: focal_wire::AdmissionStats,
    /// The last measured round-trip time to each peer, bounded by the
    /// fleet's member count (24 §22): the operator's view of inter-node,
    /// and so inter-region, latency.
    pub peer_rtts: Vec<PeerRtt>,
    pub liveness: LivenessMetrics,
    pub credential: Option<CredentialMetrics>,
    pub sessions: Vec<SessionMetrics>,
    pub sessions_truncated: bool,
    /// Sessions asked in this round whose owner had not answered when the
    /// round closed — refused at its door, gone, or late (the audit's F65).
    /// Their entries carry what the node knows without the owner, and say
    /// so.
    pub sessions_unobserved: u64,
    /// How long the round took to observe what it could, in milliseconds;
    /// a round closes at the sampling cadence.
    pub collection_ms: u64,
    pub agent: Option<AgentMetrics>,
    pub fence_level: u32,
    pub announced_level: u32,
}
/// A published sample: the snapshot and its text, rendered once when it was
/// sampled (the audit's F65). Every reader — the admin socket's and the
/// loopback's — serves the text as it is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricsPage {
    pub snapshot: MetricsSnapshot,
    pub text: String,
}
impl MetricsPage {
    pub fn new(snapshot: MetricsSnapshot) -> Self {
        let text = snapshot.render();
        Self { snapshot, text }
    }
}
/// Every session asked at once and each answer taken as it comes, the round
/// closed at `deadline` (the audit's F65): an ask not answered by then is
/// left unobserved and dropped with the set — never in the way of another,
/// never passed over in silence, since the caller reads the gap. The asks
/// are bounded by their number and each by the charge it made at its owner.
pub async fn collect<T, F>(asks: Vec<F>, deadline: tokio::time::Instant) -> Vec<Option<T>>
where
    T: Send + 'static,
    F: Future<Output = Option<T>> + Send + 'static,
{
    let mut answers: Vec<Option<T>> = Vec::new();
    if answers.try_reserve_exact(asks.len()).is_err() {
        return answers;
    }
    answers.resize_with(asks.len(), || None);
    let mut tasks = tokio::task::JoinSet::new();
    for (index, ask) in asks.into_iter().enumerate() {
        tasks.spawn(async move { (index, ask.await) });
    }
    while !tasks.is_empty() {
        match tokio::time::timeout_at(deadline, tasks.join_next()).await {
            Ok(Some(Ok((index, answer)))) => {
                if let Some(slot) = answers.get_mut(index) {
                    *slot = answer;
                }
            }
            // A task that ended without an answer stays unobserved.
            Ok(Some(Err(_))) => {}
            Ok(None) | Err(_) => break,
        }
    }
    answers
}

fn escape(value: &str, out: &mut String) {
    for c in value.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            c => out.push(c),
        }
    }
}
struct Text {
    out: String,
    base: String,
}
impl Text {
    fn new(labels: &MetricLabels) -> Self {
        let mut base = String::new();
        let _ = write!(base, "node=\"{}\",cluster=\"", labels.node);
        escape(&labels.cluster, &mut base);
        base.push('"');
        Self {
            out: String::new(),
            base,
        }
    }
    fn header(&mut self, name: &str, kind: &str, help: &str) {
        let _ = writeln!(self.out, "# HELP {name} {help}");
        let _ = writeln!(self.out, "# TYPE {name} {kind}");
    }
    fn gauge(&mut self, name: &str, help: &str, value: impl std::fmt::Display) {
        self.header(name, "gauge", help);
        let _ = writeln!(self.out, "{name}{{{}}} {value}", self.base);
    }
    fn counter(&mut self, name: &str, help: &str, value: impl std::fmt::Display) {
        self.header(name, "counter", help);
        let _ = writeln!(self.out, "{name}{{{}}} {value}", self.base);
    }
    fn labeled(&mut self, name: &str, extra: &[(&str, &str)], value: impl std::fmt::Display) {
        let _ = write!(self.out, "{name}{{{}", self.base);
        for (key, label) in extra {
            let _ = write!(self.out, ",{key}=\"");
            escape(label, &mut self.out);
            self.out.push('"');
        }
        let _ = writeln!(self.out, "}} {value}");
    }
}
impl MetricsSnapshot {
    /// The snapshot as Prometheus text exposition (version 0.0.4).
    pub fn render(&self) -> String {
        let mut text = Text::new(&self.labels);
        text.header(
            "focal_node_info",
            "gauge",
            "This node's identity and declared topology.",
        );
        let region = self.labels.region.clone().unwrap_or_default();
        let zone = self.labels.zone.clone().unwrap_or_default();
        let capability = crate::upgrade::CAPABILITY_LEVEL.to_string();
        text.labeled(
            "focal_node_info",
            &[
                ("role", self.labels.role),
                ("region", region.as_str()),
                ("zone", zone.as_str()),
                ("capability", capability.as_str()),
            ],
            1,
        );
        text.gauge(
            "focal_metrics_sampled_milliseconds",
            "When this snapshot was sampled, milliseconds since the Unix epoch.",
            self.sampled_ms,
        );
        text.gauge(
            "focal_metrics_collection_milliseconds",
            "How long the sample took to observe its sessions; a round closes at the sampling cadence.",
            self.collection_ms,
        );
        text.gauge(
            "focal_memory_limit_bytes",
            "The node's memory allowance.",
            self.memory.limit,
        );
        text.gauge(
            "focal_memory_used_bytes",
            "Bytes charged to the node's allowance.",
            self.memory.used,
        );
        text.gauge(
            "focal_memory_completion_reserve_bytes",
            "Bytes kept for completion work.",
            self.memory.completion_reserve,
        );
        if let Some(disk) = &self.disk {
            if let Some(free) = disk.free {
                text.gauge(
                    "focal_disk_free_bytes",
                    "Free bytes of the data volume at the last sample.",
                    free,
                );
            }
            text.gauge(
                "focal_disk_outstanding_bytes",
                "Bytes promised to queued durable writes.",
                disk.outstanding,
            );
            text.gauge(
                "focal_disk_headroom_bytes",
                "Bytes the volume keeps free before refusing admission.",
                disk.headroom,
            );
            text.gauge(
                "focal_disk_completion_reserve_bytes",
                "Bytes of the volume kept for completion work.",
                disk.completion_reserve,
            );
            text.header(
                "focal_disk_outstanding_by_kind_bytes",
                "gauge",
                "Bytes promised to queued durable writes, by kind.",
            );
            for (index, bytes) in disk.by_kind.iter().enumerate() {
                let kind = match index {
                    0 => "wal",
                    1 => "checkpoint",
                    2 => "content",
                    3 => "archive",
                    4 => "staging",
                    _ => "other",
                };
                text.labeled(
                    "focal_disk_outstanding_by_kind_bytes",
                    &[("kind", kind)],
                    bytes,
                );
            }
        }
        text.gauge(
            "focal_uploads_staged",
            "Uploads in progress.",
            self.staged_uploads,
        );
        text.gauge(
            "focal_uploads_staged_bytes",
            "Bytes uploads in progress have staged.",
            self.staged_bytes,
        );
        if let Some(wal) = &self.wal {
            text.counter(
                "focal_wal_group_commits_total",
                "Group commits the WAL writer performed.",
                wal.group_commits,
            );
            text.counter(
                "focal_wal_appended_records_total",
                "Records the WAL writer appended.",
                wal.appended_records,
            );
            text.gauge(
                "focal_wal_indexed_records",
                "Records the WAL index holds.",
                wal.indexed_records,
            );
            text.gauge(
                "focal_wal_physical_bytes",
                "Bytes of every frame the WAL holds from its base to its tail.",
                wal.physical_bytes,
            );
            text.gauge(
                "focal_wal_live_bytes",
                "Bytes of the WAL's live frames.",
                wal.live_bytes,
            );
            text.counter(
                "focal_wal_checkpoint_bytes_total",
                "Bytes group checkpoints wrote: their records and floors.",
                wal.checkpoint_bytes,
            );
            text.counter(
                "focal_wal_reclaimed_bytes_total",
                "Bytes of the frames the WAL's base passed.",
                wal.reclaimed_bytes,
            );
            text.counter(
                "focal_wal_reclaimed_segments_total",
                "Segments removed behind the WAL's base.",
                wal.reclaimed_segments,
            );
            text.counter(
                "focal_wal_relocated_records_total",
                "Live frames the WAL's base met and wrote again at the tail.",
                wal.relocated_records,
            );
            text.counter(
                "focal_wal_relocated_bytes_total",
                "Bytes of the frames written again at the tail.",
                wal.relocated_bytes,
            );
        }
        text.gauge(
            "focal_fleet_installed",
            "Replicas installed on this node.",
            self.fleet.installed,
        );
        text.gauge(
            "focal_fleet_running",
            "Replicas running on this node.",
            self.fleet.running,
        );
        text.gauge(
            "focal_fleet_stopped",
            "Whether the fleet owner stopped.",
            u8::from(self.fleet.stopped),
        );
        text.gauge(
            "focal_root_leader",
            "The root group's leader as this replica knows it.",
            self.root.leader,
        );
        text.gauge(
            "focal_root_term",
            "The root replica's term.",
            self.root.term,
        );
        text.gauge(
            "focal_root_applied_index",
            "The root replica's applied index.",
            self.root.applied_index,
        );
        text.gauge(
            "focal_root_snapshot_index",
            "The root log compaction floor; applied minus this is the retained log length.",
            self.root.snapshot_index,
        );
        text.gauge(
            "focal_root_tick_period_milliseconds",
            "The root owner's tick period in force; stretched for a far group.",
            self.root.tick_period_ms,
        );
        text.gauge(
            "focal_root_broadcast_tail_microseconds",
            "The slowest measured root voter path's round-trip tail.",
            self.root.broadcast_tail_us,
        );
        text.gauge(
            "focal_root_pace_samples",
            "Round trips that fed the root pace; zero means the configured period.",
            self.root.pace_samples,
        );
        text.counter(
            "focal_root_periods_total",
            "Periods the root owner has run since it started; a node that is slow still advances this, a wedged one does not.",
            self.root.periods,
        );
        text.counter(
            "focal_root_periods_refused_total",
            "Periods in which the root replica was not ticked: it was refused the room, or still persisted what the tick before had left.",
            self.root.refused_periods,
        );
        text.counter(
            "focal_root_peers_unreachable_total",
            "Exchanges of the root replica the driver could not make at all, each told to the core, which probes the peer instead of streaming to it.",
            self.root.peers_unreachable,
        );
        text.counter(
            "focal_root_peer_reports_coalesced_total",
            "Reports of a lost exchange with a peer the root owner already held for the core: coalesced into the one it holds.",
            self.root.peer_reports_coalesced,
        );
        text.counter(
            "focal_root_peer_reports_dropped_total",
            "Reports of a lost exchange dropped because the root owner held reports for as many peers as a configuration can name.",
            self.root.peer_reports_dropped,
        );
        text.gauge(
            "focal_root_period_longest_ms",
            "The longest a period of the root owner took, from one to the next: a stall, which the owner's pace covers from then on.",
            self.root.longest_period_ms,
        );
        text.gauge(
            "focal_root_stopped",
            "Whether the root replica stopped.",
            u8::from(self.root.stopped),
        );
        if !self.root.peers.is_empty() {
            text.header(
                "focal_root_peer_matched",
                "gauge",
                "Highest log index the root leader knows this peer has stored.",
            );
            for peer in &self.root.peers {
                text.labeled(
                    "focal_root_peer_matched",
                    &[("peer", &peer.node.to_string())],
                    peer.matched,
                );
            }
            text.header(
                "focal_root_peer_next_index",
                "gauge",
                "The next log index the root leader will send this peer.",
            );
            for peer in &self.root.peers {
                text.labeled(
                    "focal_root_peer_next_index",
                    &[("peer", &peer.node.to_string())],
                    peer.next_index,
                );
            }
            text.header(
                "focal_root_peer_state",
                "gauge",
                "Replication state for this peer (0 probe, 1 replicate, 2 snapshot).",
            );
            for peer in &self.root.peers {
                text.labeled(
                    "focal_root_peer_state",
                    &[("peer", &peer.node.to_string())],
                    u64::from(peer.state),
                );
            }
            text.header(
                "focal_root_peer_recent_active",
                "gauge",
                "Whether the root leader has heard from this peer within the last check.",
            );
            for peer in &self.root.peers {
                text.labeled(
                    "focal_root_peer_recent_active",
                    &[("peer", &peer.node.to_string())],
                    u64::from(peer.recent_active),
                );
            }
            text.header(
                "focal_root_peer_pending_snapshot",
                "gauge",
                "Snapshot index in flight to this peer, or zero.",
            );
            for peer in &self.root.peers {
                text.labeled(
                    "focal_root_peer_pending_snapshot",
                    &[("peer", &peer.node.to_string())],
                    peer.pending_snapshot,
                );
            }
        }
        text.counter(
            "focal_peer_messages_delivered_total",
            "Peer requests answered.",
            self.peers.delivered,
        );
        text.counter(
            "focal_peer_messages_lost_total",
            "Peer requests lost or of unknown outcome.",
            self.peers.lost,
        );
        text.counter(
            "focal_peer_messages_busy_total",
            "Peer requests refused at the pool's bound.",
            self.peers.busy,
        );
        text.counter(
            "focal_peer_dials_total",
            "Peer dials attempted; those beyond connections opened failed, and a peer in its unreachable cooldown is not dialed.",
            self.peers.dials,
        );
        text.counter(
            "focal_peer_connections_opened_total",
            "Peer connections opened.",
            self.peers.connections_opened,
        );
        text.gauge(
            "focal_peer_connections_cached",
            "Peer connections held open.",
            self.peers.cached_connections,
        );
        text.gauge(
            "focal_peer_inflight",
            "Peer requests in flight.",
            self.peers.inflight,
        );
        text.gauge(
            "focal_peer_content_inflight",
            "Content to peers in flight or waiting its turn.",
            self.peers.bulk_inflight,
        );
        text.gauge(
            "focal_listener_handshakes_pending",
            "Inbound handshakes in progress.",
            self.listener.pending,
        );
        text.gauge(
            "focal_listener_identities",
            "Identities holding an inbound connection.",
            self.listener.identities,
        );
        text.gauge(
            "focal_listener_connections",
            "Authenticated inbound connections.",
            self.listener.connections,
        );
        text.counter(
            "focal_listener_admitted_total",
            "Inbound connections admitted to an identity.",
            self.listener.admitted,
        );
        text.counter(
            "focal_listener_replaced_total",
            "Connections closed for a newer one of the same identity: the one idle longest.",
            self.listener.replaced,
        );
        text.header(
            "focal_listener_refused_total",
            "counter",
            "Inbound connections refused, by the bound that refused them.",
        );
        for (bound, refused) in [
            ("handshakes", self.listener.refused_pending),
            ("identities", self.listener.refused_identities),
            ("connections", self.listener.refused_connections),
            ("bytes", self.listener.refused_bytes),
            ("memory", self.listener.refused_memory),
        ] {
            text.labeled("focal_listener_refused_total", &[("bound", bound)], refused);
        }
        text.gauge(
            "focal_listener_ingress_bytes",
            "Bytes of request bodies permitted to the listener's identities and not yet given back.",
            self.listener.bytes,
        );
        if !self.peer_rtts.is_empty() {
            text.header(
                "focal_peer_rtt_ms",
                "gauge",
                "Last measured round-trip time to a peer, milliseconds.",
            );
            for peer in &self.peer_rtts {
                text.labeled(
                    "focal_peer_rtt_ms",
                    &[("peer", &peer.peer.to_string())],
                    peer.rtt_ms,
                );
            }
        }
        text.header(
            "focal_liveness_members",
            "gauge",
            "Members by the failure detector's verdict.",
        );
        text.labeled(
            "focal_liveness_members",
            &[("status", "alive")],
            self.liveness.alive,
        );
        text.labeled(
            "focal_liveness_members",
            &[("status", "suspect")],
            self.liveness.suspect,
        );
        text.labeled(
            "focal_liveness_members",
            &[("status", "dead")],
            self.liveness.dead,
        );
        text.gauge(
            "focal_liveness_health_score",
            "The local health score the detector applies.",
            self.liveness.health_score,
        );
        text.counter(
            "focal_liveness_probes_sent_total",
            "Probes sent.",
            self.liveness.probes_sent,
        );
        text.counter(
            "focal_liveness_probes_answered_total",
            "Probes answered.",
            self.liveness.probes_answered,
        );
        text.counter(
            "focal_liveness_probe_timeouts_total",
            "Probes that timed out.",
            self.liveness.probe_timeouts,
        );
        text.counter(
            "focal_liveness_suspicions_total",
            "Suspicions raised.",
            self.liveness.suspicions,
        );
        text.counter(
            "focal_liveness_refutations_total",
            "Suspicions refuted.",
            self.liveness.refutations,
        );
        text.counter(
            "focal_liveness_deaths_total",
            "Members declared dead.",
            self.liveness.deaths,
        );
        if let Some(credential) = &self.credential {
            text.gauge(
                "focal_credential_expires_at_seconds",
                "When this node's credential expires, seconds since the Unix epoch.",
                credential.expires_at,
            );
            text.counter(
                "focal_credential_renewals_total",
                "Renewals this process performed.",
                credential.renewals,
            );
            text.counter(
                "focal_credential_rotations_total",
                "Rotations this process performed.",
                credential.rotations,
            );
        }
        text.gauge(
            "focal_upgrade_fence_level",
            "The committed upgrade fence.",
            self.fence_level,
        );
        text.gauge(
            "focal_upgrade_announced_level",
            "The capability level this node announces.",
            self.announced_level,
        );
        if let Some(agent) = &self.agent {
            text.counter(
                "focal_placement_root_intents_total",
                "Root intents the placement agent completed.",
                agent.root_intents,
            );
            text.counter(
                "focal_placement_partition_intents_total",
                "Partition intents the placement agent completed.",
                agent.partition_intents,
            );
            text.gauge(
                "focal_placement_installed",
                "Copies the placement agent installed.",
                agent.installed,
            );
            text.gauge(
                "focal_placement_agent_error",
                "Whether the agent's last pass failed.",
                u8::from(agent.last_error),
            );
            text.gauge(
                "focal_placement_agent_refused",
                "Whether the owner refused the agent's last intent.",
                u8::from(agent.last_refusal),
            );
            text.gauge(
                "focal_admission_tenants",
                "Tenants this node admits.",
                agent.admission.tenants.len(),
            );
            text.gauge(
                "focal_admission_max_tenants",
                "Tenants this node admits at most.",
                agent.admission.max_tenants,
            );
            text.gauge(
                "focal_admission_memory_used_bytes",
                "Bytes the admitted tenants charge.",
                agent.admission.memory_used,
            );
            text.header(
                "focal_admission_queued_items",
                "gauge",
                "Queued work items per admitted tenant.",
            );
            for tenant in &agent.admission.tenants {
                let id = tenant.tenant.to_string();
                text.labeled(
                    "focal_admission_queued_items",
                    &[("tenant", id.as_str())],
                    tenant.queued_items,
                );
            }
            text.header(
                "focal_admission_queued_bytes",
                "gauge",
                "Queued work bytes per admitted tenant.",
            );
            for tenant in &agent.admission.tenants {
                let id = tenant.tenant.to_string();
                text.labeled(
                    "focal_admission_queued_bytes",
                    &[("tenant", id.as_str())],
                    tenant.queued_bytes,
                );
            }
        }
        text.gauge(
            "focal_metrics_sessions_truncated",
            "Whether sessions beyond the bound were left out.",
            u8::from(self.sessions_truncated),
        );
        text.gauge(
            "focal_metrics_sessions_unobserved",
            "Sessions asked whose owner had not answered when the round closed.",
            self.sessions_unobserved,
        );
        let series: [(&str, &str, &str); 32] = [
            (
                "focal_session_observed",
                "gauge",
                "Whether the session's owner answered this round; without it the owner-side series are absent.",
            ),
            (
                "focal_session_periods_total",
                "counter",
                "Periods the session's owner has run since it started: the unit its replica's election timer counts.",
            ),
            (
                "focal_session_periods_refused_total",
                "counter",
                "Periods in which the session's replica was not ticked: it was refused the room, or still persisted.",
            ),
            (
                "focal_session_peers_unreachable_total",
                "counter",
                "Exchanges of the session's replica the driver could not make at all, each told to the core, which probes the peer instead of streaming to it.",
            ),
            (
                "focal_session_peer_reports_coalesced_total",
                "counter",
                "Reports of a lost exchange with a peer the session's owner already held for the core: coalesced into the one it holds.",
            ),
            (
                "focal_session_peer_reports_dropped_total",
                "counter",
                "Reports of a lost exchange dropped because the owner held reports for as many peers as a configuration can name; the peer's next lost exchange reports it.",
            ),
            (
                "focal_session_period_longest_ms",
                "gauge",
                "The longest a period of the session's owner took, from one to the next: a stall, which the owner's pace covers from then on.",
            ),
            (
                "focal_session_leader",
                "gauge",
                "The session log's leader as this replica knows it.",
            ),
            (
                "focal_session_preferred_leader",
                "gauge",
                "The leader the session's committed placement prefers.",
            ),
            (
                "focal_session_leader_returns_total",
                "counter",
                "Hand-overs of leadership to the preferred leader this replica asked for.",
            ),
            (
                "focal_session_leader_returns_failed_total",
                "counter",
                "Hand-overs to the preferred leader that were abandoned or did not hold.",
            ),
            ("focal_session_term", "gauge", "The replica's term."),
            (
                "focal_session_tick_period_milliseconds",
                "gauge",
                "The replica owner's tick period in force; stretched for a far or slow group.",
            ),
            (
                "focal_session_broadcast_tail_microseconds",
                "gauge",
                "The slowest measured voter path's round-trip tail of this session.",
            ),
            (
                "focal_session_pace_samples",
                "gauge",
                "Round trips that fed the session's pace; zero means the configured period.",
            ),
            (
                "focal_session_committed_index",
                "gauge",
                "The replica's committed index.",
            ),
            (
                "focal_session_applied_index",
                "gauge",
                "The replica's applied index.",
            ),
            (
                "focal_session_apply_lag",
                "gauge",
                "Committed entries not yet applied.",
            ),
            (
                "focal_session_sequence",
                "gauge",
                "The session's published sequence.",
            ),
            (
                "focal_session_pending",
                "gauge",
                "Proposals awaiting commit.",
            ),
            (
                "focal_session_authoritative",
                "gauge",
                "Whether this replica serves as authority.",
            ),
            (
                "focal_session_log_entries_since_checkpoint",
                "gauge",
                "Log entries kept beyond the checkpoint.",
            ),
            (
                "focal_session_retention_published",
                "gauge",
                "The retention report's published prefix.",
            ),
            (
                "focal_session_retention_cursors",
                "gauge",
                "The prefix every consumer acknowledged.",
            ),
            (
                "focal_session_retention_floor",
                "gauge",
                "The oldest retained prefix.",
            ),
            (
                "focal_session_cursor_lag",
                "gauge",
                "Published entries consumers have not acknowledged.",
            ),
            (
                "focal_session_seed_chunks_missing",
                "gauge",
                "Seed chunks a retained checkpoint lacks.",
            ),
            (
                "focal_session_custody_objects_missing",
                "gauge",
                "Objects a retained delivery lacks.",
            ),
            (
                "focal_session_route_epoch",
                "gauge",
                "The directory's route epoch for the session.",
            ),
            (
                "focal_session_placement_epoch",
                "gauge",
                "The directory's placement epoch for the session.",
            ),
            (
                "focal_session_achieved_max_failures",
                "gauge",
                "Failures the active placement survives.",
            ),
            (
                "focal_session_blocked",
                "gauge",
                "Conditions blocking the desired durability.",
            ),
        ];
        for (name, kind, help) in series {
            text.header(name, kind, help);
            for session in &self.sessions {
                let labels = [
                    ("tenant", session.tenant.as_str()),
                    ("session", session.session.as_str()),
                ];
                if !session.observed && owner_side(name) {
                    continue;
                }
                let value: Option<u64> = match name {
                    "focal_session_observed" => Some(u64::from(session.observed)),
                    "focal_session_periods_total" => Some(session.periods),
                    "focal_session_periods_refused_total" => Some(session.refused_periods),
                    "focal_session_peers_unreachable_total" => Some(session.peers_unreachable),
                    "focal_session_peer_reports_coalesced_total" => {
                        Some(session.peer_reports_coalesced)
                    }
                    "focal_session_peer_reports_dropped_total" => {
                        Some(session.peer_reports_dropped)
                    }
                    "focal_session_period_longest_ms" => Some(session.longest_period_ms),
                    "focal_session_leader" => Some(session.leader),
                    "focal_session_preferred_leader" => session.preferred_leader,
                    "focal_session_leader_returns_total" => Some(session.leader_returns),
                    "focal_session_leader_returns_failed_total" => {
                        Some(session.leader_returns_failed)
                    }
                    "focal_session_term" => Some(session.term),
                    "focal_session_tick_period_milliseconds" => Some(session.tick_period_ms),
                    "focal_session_broadcast_tail_microseconds" => Some(session.broadcast_tail_us),
                    "focal_session_pace_samples" => Some(session.pace_samples),
                    "focal_session_committed_index" => Some(session.committed_index),
                    "focal_session_applied_index" => Some(session.applied_index),
                    "focal_session_apply_lag" => Some(
                        session
                            .committed_index
                            .saturating_sub(session.applied_index),
                    ),
                    "focal_session_sequence" => Some(session.sequence),
                    "focal_session_pending" => Some(session.pending),
                    "focal_session_authoritative" => Some(u64::from(session.authoritative)),
                    "focal_session_log_entries_since_checkpoint" => {
                        Some(session.log_entries_since_checkpoint)
                    }
                    "focal_session_retention_published" => {
                        session.retention.as_ref().map(|r| r.published)
                    }
                    "focal_session_retention_cursors" => {
                        session.retention.as_ref().map(|r| r.cursors)
                    }
                    "focal_session_retention_floor" => session.retention.as_ref().map(|r| r.floor),
                    "focal_session_cursor_lag" => session
                        .retention
                        .as_ref()
                        .map(|r| r.published.saturating_sub(r.cursors)),
                    "focal_session_seed_chunks_missing" => session.seed_chunks_missing,
                    "focal_session_custody_objects_missing" => session.custody_objects_missing,
                    "focal_session_route_epoch" => session.route_epoch,
                    "focal_session_placement_epoch" => session.placement_epoch,
                    "focal_session_achieved_max_failures" => {
                        session.achieved_max_failures.map(u64::from)
                    }
                    "focal_session_blocked" => session.blocked,
                    _ => None,
                };
                if let Some(value) = value {
                    text.labeled(name, &labels, value);
                }
            }
        }
        text.out
    }
}

/// The series a session's owner alone can answer.
fn owner_side(name: &str) -> bool {
    matches!(
        name,
        "focal_session_committed_index"
            | "focal_session_applied_index"
            | "focal_session_apply_lag"
            | "focal_session_sequence"
            | "focal_session_pending"
            | "focal_session_authoritative"
            | "focal_session_preferred_leader"
            | "focal_session_leader_returns_total"
            | "focal_session_leader_returns_failed_total"
            | "focal_session_log_entries_since_checkpoint"
            | "focal_session_retention_published"
            | "focal_session_retention_cursors"
            | "focal_session_retention_floor"
            | "focal_session_cursor_lag"
            | "focal_session_seed_chunks_missing"
            | "focal_session_custody_objects_missing"
    )
}

/// Serve the latest page as `GET /metrics` over HTTP/1.0 on a loopback
/// listener: the request is read and judged first, under its bound, and
/// only then is the page's text — rendered once when it was sampled —
/// written; as many connections at once as the admin socket admits
/// operators, one beyond them closed unanswered; no other path.
pub async fn serve_loopback(
    listener: tokio::net::TcpListener,
    view: tokio::sync::watch::Receiver<Option<MetricsPage>>,
) -> std::io::Result<()> {
    let at_once = max_scrapes();
    let mut serving = tokio::task::JoinSet::new();
    loop {
        let (mut stream, _) = listener.accept().await?;
        while serving.try_join_next().is_some() {}
        if serving.len() >= at_once {
            drop(stream);
            continue;
        }
        let view = view.clone();
        serving.spawn(async move {
            let handled = tokio::time::timeout(std::time::Duration::from_secs(2), async {
                let mut request = Vec::new();
                let mut buffer = [0u8; 512];
                loop {
                    let read = stream.read(&mut buffer).await?;
                    if read == 0 {
                        break;
                    }
                    request.extend_from_slice(buffer.get(..read).unwrap_or(&[]));
                    if request.len() > MAX_REQUEST_BYTES
                        || request.windows(4).any(|w| w == b"\r\n\r\n")
                    {
                        break;
                    }
                }
                let line = request
                    .split(|byte| *byte == b'\n')
                    .next()
                    .unwrap_or(&[]);
                let line = String::from_utf8_lossy(line);
                let mut parts = line.split_whitespace();
                let (status, payload) = match (parts.next(), parts.next()) {
                    (Some("GET"), Some("/metrics")) => {
                        let text = view
                            .borrow()
                            .as_ref()
                            .map(|page| page.text.clone())
                            .unwrap_or_else(|| "# metrics not sampled yet\n".to_owned());
                        ("200 OK", text)
                    }
                    (Some("GET"), Some(_)) => ("404 Not Found", "not found\n".to_owned()),
                    _ => ("405 Method Not Allowed", "GET /metrics only\n".to_owned()),
                };
                let head = format!(
                    "HTTP/1.0 {status}\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    payload.len()
                );
                stream.write_all(head.as_bytes()).await?;
                stream.write_all(payload.as_bytes()).await?;
                stream.shutdown().await
            })
            .await;
            // A slow or broken client costs this one connection, nothing else.
            let _ = handled;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn snapshot(memory: BudgetStats) -> MetricsSnapshot {
        MetricsSnapshot {
            sampled_ms: 5,
            labels: MetricLabels {
                node: 7,
                cluster: "ab\"cd".into(),
                region: Some("eu-a".into()),
                zone: None,
                role: "founder",
            },
            memory,
            disk: None,
            staged_uploads: 0,
            staged_bytes: 0,
            wal: None,
            fleet: FleetStatus {
                latest_sequence: 1,
                installed: 1,
                running: 1,
                stopped: false,
            },
            root: RootMetrics::default(),
            peers: PeerPoolStats {
                delivered: 4,
                lost: 0,
                busy: 0,
                dials: 1,
                connections_opened: 1,
                cached_connections: 1,
                inflight: 0,
                bulk_inflight: 0,
            },
            listener: focal_wire::AdmissionStats::default(),
            peer_rtts: vec![PeerRtt {
                peer: 9,
                rtt_ms: 42,
            }],
            liveness: LivenessMetrics::default(),
            credential: None,
            sessions: vec![SessionMetrics {
                periods: 0,
                peers_unreachable: 0,
                peer_reports_coalesced: 0,
                peer_reports_dropped: 0,
                tenant: "t".into(),
                session: "s".into(),
                observed: true,
                committed_index: 9,
                applied_index: 7,
                ..SessionMetrics::default()
            }],
            sessions_truncated: false,
            sessions_unobserved: 0,
            collection_ms: 3,
            agent: None,
            fence_level: 0,
            announced_level: 1,
        }
    }
    /// The audit's F65: a session whose owner did not answer says so and
    /// carries no owner-side number — never a zero read as health.
    #[test]
    fn an_unobserved_session_says_so_and_carries_no_owner_side_numbers() {
        let budget = focal_memory::MemoryBudget::new(1 << 20, 1 << 16).unwrap();
        let mut sample = snapshot(budget.stats());
        let mut silent = sample.sessions[0].clone();
        silent.session = "u".into();
        silent.observed = false;
        silent.leader = 3;
        sample.sessions.push(silent);
        sample.sessions_unobserved = 1;
        let text = sample.render();
        let base = "node=\"7\",cluster=\"ab\\\"cd\"";
        assert!(text.contains(&format!(
            "focal_metrics_collection_milliseconds{{{base}}} 3\n"
        )));
        assert!(text.contains(&format!("focal_metrics_sessions_unobserved{{{base}}} 1\n")));
        assert!(text.contains(&format!(
            "focal_session_observed{{{base},tenant=\"t\",session=\"s\"}} 1\n"
        )));
        assert!(text.contains(&format!(
            "focal_session_observed{{{base},tenant=\"t\",session=\"u\"}} 0\n"
        )));
        assert!(text.contains(&format!(
            "focal_session_leader{{{base},tenant=\"t\",session=\"u\"}} 3\n"
        )));
        assert!(text.contains(&format!(
            "focal_session_applied_index{{{base},tenant=\"t\",session=\"s\"}} 7\n"
        )));
        assert!(!text.contains(&format!(
            "focal_session_applied_index{{{base},tenant=\"t\",session=\"u\"}}"
        )));
        assert!(!text.contains(&format!(
            "focal_session_apply_lag{{{base},tenant=\"t\",session=\"u\"}}"
        )));
        assert_eq!(MetricsPage::new(sample.clone()).text, text);
    }
    /// A round takes every answer that comes and closes at its deadline:
    /// the late and the failed stay unobserved, in their places.
    #[tokio::test]
    async fn a_round_closes_at_its_deadline_with_the_late_unobserved() {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(100);
        let asks: Vec<std::pin::Pin<Box<dyn Future<Output = Option<u8>> + Send>>> = vec![
            Box::pin(async { Some(1) }),
            Box::pin(async {
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                Some(2)
            }),
            Box::pin(async { None }),
            Box::pin(async {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                Some(4)
            }),
        ];
        let answers = collect(asks, deadline).await;
        assert_eq!(answers, vec![Some(1), None, None, Some(4)]);
        assert!(tokio::time::Instant::now() >= deadline);
        assert!(tokio::time::Instant::now() < deadline + std::time::Duration::from_secs(1));
    }
    /// The loopback serves the page's text as it was rendered, judges the
    /// request before it writes, and a scrape that never speaks costs no
    /// other scrape its answer.
    #[tokio::test]
    async fn a_silent_scrape_delays_no_other_and_the_text_is_the_page_s() {
        use tokio::net::{TcpListener, TcpStream};
        let budget = focal_memory::MemoryBudget::new(1 << 20, 1 << 16).unwrap();
        let page = MetricsPage::new(snapshot(budget.stats()));
        let (_publish, view) = tokio::sync::watch::channel(Some(page.clone()));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(serve_loopback(listener, view));
        let silent = TcpStream::connect(address).await.unwrap();
        let started = std::time::Instant::now();
        let exchange = |request: &'static str| async move {
            let mut stream = TcpStream::connect(address).await.unwrap();
            stream.write_all(request.as_bytes()).await.unwrap();
            let mut reply = Vec::new();
            stream.read_to_end(&mut reply).await.unwrap();
            let reply = String::from_utf8(reply).unwrap();
            let (head, body) = reply.split_once("\r\n\r\n").unwrap();
            (head.to_owned(), body.to_owned())
        };
        let (head, body) = exchange("GET /metrics HTTP/1.0\r\n\r\n").await;
        assert!(head.starts_with("HTTP/1.0 200 OK"), "{head}");
        assert_eq!(body, page.text);
        let (head, body) = exchange("GET /nothing HTTP/1.0\r\n\r\n").await;
        assert!(head.starts_with("HTTP/1.0 404"), "{head}");
        assert_eq!(body, "not found\n");
        let (head, _) = exchange("POST /metrics HTTP/1.0\r\n\r\n").await;
        assert!(head.starts_with("HTTP/1.0 405"), "{head}");
        // Three answers while the silent scrape still holds its connection:
        // served side by side, not behind its two-second bound.
        assert!(
            started.elapsed() < std::time::Duration::from_secs(1),
            "{:?}",
            started.elapsed()
        );
        drop(silent);
        server.abort();
    }
    #[test]
    fn the_text_carries_fixed_labels_escapes_them_and_derives_lags() {
        let budget = focal_memory::MemoryBudget::new(1 << 20, 1 << 16).unwrap();
        let _held = budget
            .reserve(
                focal_memory::BudgetKind::Recovery,
                focal_memory::BudgetLane::Completion,
                3,
            )
            .unwrap()
            .commit();
        let memory = budget.stats();
        let used = memory.used;
        assert!(used >= 3);
        let text = snapshot(memory).render();
        assert!(text.contains("# TYPE focal_node_info gauge"));
        assert!(text.contains(
            "focal_node_info{node=\"7\",cluster=\"ab\\\"cd\",role=\"founder\",region=\"eu-a\",zone=\"\",capability=\"1\"} 1"
        ));
        assert!(text.contains(&format!(
            "focal_memory_used_bytes{{node=\"7\",cluster=\"ab\\\"cd\"}} {used}"
        )));
        assert!(
            text.contains("focal_peer_messages_delivered_total{node=\"7\",cluster=\"ab\\\"cd\"} 4")
        );
        assert!(text.contains(
            "focal_session_apply_lag{node=\"7\",cluster=\"ab\\\"cd\",tenant=\"t\",session=\"s\"} 2"
        ));
        assert!(!text.contains("focal_session_retention_floor{"));
        assert!(text.contains("focal_upgrade_announced_level{node=\"7\",cluster=\"ab\\\"cd\"} 1"));
        assert!(text.ends_with('\n'));
    }
}
