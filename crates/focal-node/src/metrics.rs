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
    pub leader: u64,
    pub term: u64,
    pub committed_index: u64,
    pub applied_index: u64,
    pub sequence: u64,
    pub pending: u64,
    pub authoritative: bool,
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
    /// The last measured round-trip time to each peer, bounded by the
    /// fleet's member count (24 §22): the operator's view of inter-node,
    /// and so inter-region, latency.
    pub peer_rtts: Vec<PeerRtt>,
    pub liveness: LivenessMetrics,
    pub credential: Option<CredentialMetrics>,
    pub sessions: Vec<SessionMetrics>,
    pub sessions_truncated: bool,
    pub agent: Option<AgentMetrics>,
    pub fence_level: u32,
    pub announced_level: u32,
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
            "focal_root_stopped",
            "Whether the root replica stopped.",
            u8::from(self.root.stopped),
        );
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
        let series: [(&str, &str, &str); 19] = [
            (
                "focal_session_leader",
                "gauge",
                "The session log's leader as this replica knows it.",
            ),
            ("focal_session_term", "gauge", "The replica's term."),
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
                let value: Option<u64> = match name {
                    "focal_session_leader" => Some(session.leader),
                    "focal_session_term" => Some(session.term),
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

/// Serve the latest snapshot as `GET /metrics` over HTTP/1.0 on a loopback
/// listener: one connection at a time, a bounded request, no other path.
pub async fn serve_loopback(
    listener: tokio::net::TcpListener,
    view: tokio::sync::watch::Receiver<Option<MetricsSnapshot>>,
) -> std::io::Result<()> {
    loop {
        let (mut stream, _) = listener.accept().await?;
        let body = view
            .borrow()
            .as_ref()
            .map(MetricsSnapshot::render)
            .unwrap_or_else(|| "# metrics not sampled yet\n".to_owned());
        let handled = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            let mut request = Vec::new();
            let mut buffer = [0u8; 512];
            loop {
                let read = stream.read(&mut buffer).await?;
                if read == 0 {
                    break;
                }
                request.extend_from_slice(buffer.get(..read).unwrap_or(&[]));
                if request.len() > MAX_REQUEST_BYTES || request.windows(4).any(|w| w == b"\r\n\r\n") {
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
                (Some("GET"), Some("/metrics")) => ("200 OK", body.as_str()),
                (Some("GET"), Some(_)) => ("404 Not Found", "not found\n"),
                _ => ("405 Method Not Allowed", "GET /metrics only\n"),
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
                connections_opened: 1,
                cached_connections: 1,
                inflight: 0,
            },
            peer_rtts: vec![PeerRtt {
                peer: 9,
                rtt_ms: 42,
            }],
            liveness: LivenessMetrics::default(),
            credential: None,
            sessions: vec![SessionMetrics {
                tenant: "t".into(),
                session: "s".into(),
                committed_index: 9,
                applied_index: 7,
                ..SessionMetrics::default()
            }],
            sessions_truncated: false,
            agent: None,
            fence_level: 0,
            announced_level: 1,
        }
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
