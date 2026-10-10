//! Metrics (doc 08 §9; 24 §23): one bounded snapshot of what this node
//! knows about itself, sampled by the service on a fixed cadence and
//! rendered as Prometheus text over the kernel-authenticated admin socket
//! (`inspect node --metrics`) and, when the operator configures
//! `node.metrics_listen`, over a read-only loopback HTTP/1.0 endpoint. Every
//! series carries the node's fixed labels; nothing here is a quorum read
//! and nothing here authorizes a change.
use crate::{admission::AdmissionReport, fleet::FleetStatus};
use focal_client::admin::AdminRetention;
use focal_consensus::{LogLatency, LogMetrics};
use focal_log::WalWriterStats;
use focal_memory::{
    Allocation, BudgetKind, BudgetLane, BudgetStats, DiskStats, MemoryBudget, MemoryError,
};
use focal_wire::PeerPoolStats;
use std::fmt::Write as _;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// How often the service samples a fresh snapshot.
pub const SAMPLE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);
/// What `OperatorReply::Metrics` adds to its text in postcard: the
/// variant's tag (one byte while the reply has fewer than 128 variants) and
/// the text's length (a varint, three bytes below 2^21).
const METRICS_REPLY_ENVELOPE: usize = 4;
/// The longest page: what one operator read carries
/// (`network_admin::MAX_COMMAND`, an admin frame less its envelope), so a
/// node's own `inspect node --metrics` can always read its page (the audit's
/// F26). The loopback endpoint serves the same page. What does not fit is
/// counted in the page's aggregates and listed in a later round
/// (`rounds`).
pub const MAX_PAGE_BYTES: usize =
    crate::network_admin::MAX_COMMAND.saturating_sub(METRICS_REPLY_ENVELOPE);
#[path = "metrics_rounds.rs"]
pub mod rounds;
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
    /// The root leader's account of every member of its group; `peers` are
    /// the members this round lists one by one.
    pub peer_aggregates: RootPeerAggregates,
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
    /// Every admitted tenant's queued work; `admission.tenants` are the
    /// tenants this round lists one by one.
    pub tenant_aggregates: TenantAggregates,
    pub root_intents: u64,
    pub partition_intents: u64,
    pub installed: u64,
    pub last_error: bool,
    pub last_refusal: bool,
    pub admission: AdmissionReport,
}
/// What every hosted session adds to the node's totals: the counts its
/// replica's progress makes, which only grow while it runs (the audit's
/// F26).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SessionCounters {
    pub peers_unreachable: u64,
    pub appends_rejected: u64,
    pub appends_rejected_in_order: u64,
    pub frames_held: u64,
    pub frames_let_go: u64,
    pub frames_stale: u64,
    pub frames_waited: u64,
    /// What the session turned away for room (`pace::InputRefusals`).
    pub frames_refused: u64,
    pub requests_refused: u64,
    pub frames_dropped: u64,
    pub requests_dropped: u64,
    pub replication_dropped: u64,
    pub peer_reports_coalesced: u64,
    pub peer_reports_dropped: u64,
    pub waits_asked: u64,
    pub waits_answered: u64,
    pub waits_swept: u64,
    pub refused_periods: u64,
}
impl SessionCounters {
    /// A replica's counts, read in place, and the periods its owner was
    /// refused.
    pub(crate) fn of(
        progress: &crate::fleet::ReplicaProgress,
        refused_periods: u64,
        input: crate::pace::InputRefusals,
    ) -> Self {
        Self {
            peers_unreachable: progress.peers_unreachable,
            appends_rejected: progress.appends_rejected,
            appends_rejected_in_order: progress.appends_rejected_in_order,
            frames_held: progress.frames_held,
            frames_let_go: progress.frames_let_go,
            frames_stale: progress.frames_stale,
            frames_waited: progress.frames_waited,
            frames_refused: input.frames_refused,
            requests_refused: input.requests_refused,
            frames_dropped: input.frames_dropped,
            requests_dropped: input.requests_dropped,
            replication_dropped: progress.dropped_replication,
            peer_reports_coalesced: progress.peer_reports_coalesced,
            peer_reports_dropped: progress.peer_reports_dropped,
            waits_asked: progress.waits_asked,
            waits_answered: progress.waits_answered,
            waits_swept: progress.waits_swept,
            refused_periods,
        }
    }
    fn fields(&self) -> [u64; 18] {
        [
            self.peers_unreachable,
            self.appends_rejected,
            self.appends_rejected_in_order,
            self.frames_held,
            self.frames_let_go,
            self.frames_stale,
            self.frames_waited,
            self.frames_refused,
            self.requests_refused,
            self.frames_dropped,
            self.requests_dropped,
            self.replication_dropped,
            self.peer_reports_coalesced,
            self.peer_reports_dropped,
            self.waits_asked,
            self.waits_answered,
            self.waits_swept,
            self.refused_periods,
        ]
    }
    /// Whether any count fell since `last`: the replica started over, and
    /// what it had counted before is the node's still.
    pub fn restarted_since(&self, last: &Self) -> bool {
        self.fields()
            .iter()
            .zip(last.fields())
            .any(|(now, before)| *now < before)
    }
    /// Each count of `other` added to this one's, saturating.
    pub fn add(&mut self, other: &Self) {
        let sum = |a: u64, b: u64| a.saturating_add(b);
        self.peers_unreachable = sum(self.peers_unreachable, other.peers_unreachable);
        self.appends_rejected = sum(self.appends_rejected, other.appends_rejected);
        self.appends_rejected_in_order = sum(
            self.appends_rejected_in_order,
            other.appends_rejected_in_order,
        );
        self.frames_held = sum(self.frames_held, other.frames_held);
        self.frames_let_go = sum(self.frames_let_go, other.frames_let_go);
        self.frames_stale = sum(self.frames_stale, other.frames_stale);
        self.frames_waited = sum(self.frames_waited, other.frames_waited);
        self.frames_refused = sum(self.frames_refused, other.frames_refused);
        self.requests_refused = sum(self.requests_refused, other.requests_refused);
        self.frames_dropped = sum(self.frames_dropped, other.frames_dropped);
        self.requests_dropped = sum(self.requests_dropped, other.requests_dropped);
        self.replication_dropped = sum(self.replication_dropped, other.replication_dropped);
        self.peer_reports_coalesced =
            sum(self.peer_reports_coalesced, other.peer_reports_coalesced);
        self.peer_reports_dropped = sum(self.peer_reports_dropped, other.peer_reports_dropped);
        self.waits_asked = sum(self.waits_asked, other.waits_asked);
        self.waits_answered = sum(self.waits_answered, other.waits_answered);
        self.waits_swept = sum(self.waits_swept, other.waits_swept);
        self.refused_periods = sum(self.refused_periods, other.refused_periods);
    }
}
/// The node's account of every session it hosts, read in place each round
/// with no ask of an owner (the audit's F26): a stopped, leaderless or
/// waiting session shows here whether or not this round lists it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SessionAggregates {
    pub hosted: u64,
    pub stopped: u64,
    /// Sessions this node's running replica leads.
    pub leading: u64,
    /// Sessions whose running replica knows no leader.
    pub leaderless: u64,
    /// Sessions whose owner's period is stretched past the configured one.
    pub stretched: u64,
    /// Sessions waiting on a seed's chunks, a delivery's content, or an
    /// import's sealing.
    pub seeding: u64,
    pub custody_pending: u64,
    pub importing: u64,
    /// The longest period any hosted session's owner took, in milliseconds.
    pub longest_period_ms: u64,
    /// Every count the node's sessions made, those that left or started
    /// over included: it never falls.
    pub totals: SessionCounters,
}
/// The root leader's account of every member of its group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RootPeerAggregates {
    pub members: u64,
    pub probing: u64,
    pub replicating: u64,
    pub snapshotting: u64,
    /// Members the leader has not heard from within its last check.
    pub inactive: u64,
    /// Members whose pipeline is paused.
    pub paused: u64,
    /// The most entries a member's stored log is behind this replica's
    /// applied index.
    pub lag_max: u64,
}
/// The liveness view's measured paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RttAggregates {
    pub measured: u64,
    pub min_ms: u64,
    pub max_ms: u64,
}
/// The admitted tenants' queued work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TenantAggregates {
    pub queued_items: u64,
    pub queued_bytes: u64,
    /// Tenants charging their whole memory limit.
    pub at_limit: u64,
}
/// How many of a family exist, how many are flagged, and how many this
/// round lists one by one (the audit's F26).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Listing {
    pub total: u64,
    pub flagged: u64,
    pub listed: u64,
}
impl Listing {
    /// Its counts in the order the page names them.
    pub fn fields(&self) -> [u64; 3] {
        [self.total, self.flagged, self.listed]
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Listings {
    pub sessions: Listing,
    pub root_peers: Listing,
    pub peer_rtts: Listing,
    pub tenants: Listing,
}
/// What the node's storage counted: focal-log's writer below the upgrade fence's storage level, the
/// node log under the shell at it (doc 27 §15.10). Each exports its own names, so a dashboard reads
/// which one a node runs from the names present.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageMetrics {
    Wal(WalWriterStats),
    Log(LogMetrics),
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
    pub storage: Option<StorageMetrics>,
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
    /// Every hosted session, aggregated; `sessions` are those this round
    /// lists one by one.
    pub session_aggregates: SessionAggregates,
    pub rtt_aggregates: RttAggregates,
    pub listings: Listings,
    /// Rounds the sampler has run: the rotation's progress.
    pub rounds: u64,
    /// Rounds that published no page, refused their room: the page before
    /// stood.
    pub rounds_refused: u64,
    pub sessions: Vec<SessionMetrics>,
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
/// Why a round published no page: the room for it was refused, or its
/// text outgrew the page, which the rounds' choice keeps it within — a
/// defect, said so.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PageError {
    Memory(MemoryError),
    Oversized { bytes: usize },
}
/// A published sample: the snapshot and its text, rendered once when it was
/// sampled (the audit's F65), within one page and charged for as long as it
/// is published (the audit's F26). Every reader — the admin socket's and
/// the loopback's — serves the text as it is.
#[derive(Debug)]
pub struct MetricsPage {
    pub snapshot: MetricsSnapshot,
    pub text: String,
    _charge: Allocation,
}
impl MetricsPage {
    pub fn new(snapshot: MetricsSnapshot, memory: &MemoryBudget) -> Result<Self, PageError> {
        let bytes = snapshot
            .held_bytes()
            .and_then(|held| held.checked_add(MAX_PAGE_BYTES))
            .and_then(|held| held.checked_add(focal_memory::ALLOCATOR_OVERHEAD))
            .ok_or(PageError::Oversized { bytes: usize::MAX })?;
        let charge = memory
            .reserve(BudgetKind::Control, BudgetLane::Ordinary, bytes)
            .map_err(PageError::Memory)?
            .commit();
        let mut out = String::new();
        out.try_reserve_exact(MAX_PAGE_BYTES)
            .map_err(|_| PageError::Memory(MemoryError::AllocationFailed))?;
        let text = snapshot.render_into(out);
        if text.len() > MAX_PAGE_BYTES {
            return Err(PageError::Oversized { bytes: text.len() });
        }
        Ok(Self {
            snapshot,
            text,
            _charge: charge,
        })
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
/// The page's text. The node's identity — its id, cluster, role, region,
/// zone and capability — is one series, `focal_node_info`; every other
/// series carries its own labels alone. Labels every series of a target
/// shares are the target's, which the scraper attaches (Prometheus,
/// "Instrumentation: things to watch out for", target labels): repeated on
/// each line they were 73 bytes of every series, a quarter of a page whose
/// size is one operator read, and a page at its widest fitted the series of
/// two sessions of a three-node cluster.
struct Text {
    out: String,
}
impl Text {
    fn new(out: String) -> Self {
        Self { out }
    }
    fn header(&mut self, name: &str, kind: &str, help: &str) {
        let _ = writeln!(self.out, "# HELP {name} {help}");
        let _ = writeln!(self.out, "# TYPE {name} {kind}");
    }
    fn gauge(&mut self, name: &str, help: &str, value: impl std::fmt::Display) {
        self.header(name, "gauge", help);
        let _ = writeln!(self.out, "{name} {value}");
    }
    fn counter(&mut self, name: &str, help: &str, value: impl std::fmt::Display) {
        self.header(name, "counter", help);
        let _ = writeln!(self.out, "{name} {value}");
    }
    /// A latency as a summary: its quantiles where any was timed, then its sum and count.
    fn summary(&mut self, name: &str, help: &str, latency: &LogLatency) {
        self.header(name, "summary", help);
        for (quantile, value) in [
            ("0.5", latency.p50_ns),
            ("0.99", latency.p99_ns),
            ("0.999", latency.p999_ns),
        ] {
            if let Some(value) = value {
                self.labeled(name, &[("quantile", quantile)], value);
            }
        }
        let _ = writeln!(self.out, "{name}_sum {}", latency.sum_ns);
        let _ = writeln!(self.out, "{name}_count {}", latency.count);
    }
    fn labeled(&mut self, name: &str, labels: &[(&str, &str)], value: impl std::fmt::Display) {
        let _ = write!(self.out, "{name}");
        for (position, (key, label)) in labels.iter().enumerate() {
            let open = if position == 0 { '{' } else { ',' };
            let _ = write!(self.out, "{open}{key}=\"");
            escape(label, &mut self.out);
            self.out.push('"');
        }
        if !labels.is_empty() {
            self.out.push('}');
        }
        let _ = writeln!(self.out, " {value}");
    }
}
/// How long the WAL's group commits took to be durable, as a histogram in
/// seconds: each bucket counts the commits within its bound and every bound
/// below it, as Prometheus histograms do, and the last every commit.
fn render_syncs(text: &mut Text, syncs: &focal_log::SyncLatency) {
    const NAME: &str = "focal_wal_sync_seconds";
    text.header(
        NAME,
        "histogram",
        "Each group commit, from its data's sync to its commit frame's and a fence installed with it.",
    );
    let bucket = format!("{NAME}_bucket");
    let mut within = 0u64;
    for (bound, count) in focal_log::SYNC_BOUNDS_MICROS.iter().zip(syncs.buckets) {
        within = within.saturating_add(count);
        text.labeled(&bucket, &[("le", &seconds(*bound))], within);
    }
    let count = syncs.count();
    text.labeled(&bucket, &[("le", "+Inf")], count);
    let _ = writeln!(text.out, "{NAME}_sum {}", seconds(syncs.sum_micros));
    let _ = writeln!(text.out, "{NAME}_count {count}");
}
/// Microseconds as seconds, exactly: whole seconds, a point and six places.
fn seconds(micros: u64) -> String {
    format!(
        "{}.{:06}",
        micros.checked_div(1_000_000).unwrap_or(0),
        micros.checked_rem(1_000_000).unwrap_or(0)
    )
}
/// The node log's counts under the shell (doc 27 §15.10).
fn render_log(text: &mut Text, log: &LogMetrics) {
    text.counter(
        "focal_log_frames_total",
        "Frames the node log wrote and flushed.",
        log.frames,
    );
    text.counter(
        "focal_log_updates_total",
        "Updates the node log's frames carried.",
        log.updates,
    );
    text.counter(
        "focal_log_bytes_total",
        "Bytes the node log wrote: frames, persist records and confirmations.",
        log.bytes,
    );
    text.counter(
        "focal_log_flushes_total",
        "Flushes of the node log's file.",
        log.flushes,
    );
    text.summary(
        "focal_log_flush_nanoseconds",
        "Each flush of the node log's file.",
        &log.flush,
    );
    text.summary(
        "focal_log_write_nanoseconds",
        "Each frame's writes.",
        &log.write,
    );
    text.summary(
        "focal_log_commit_wait_nanoseconds",
        "Each update, from its submission to the flush that let it be answered.",
        &log.commit_wait,
    );
    text.gauge(
        "focal_log_flushing_nanoseconds",
        "How long the flush in progress has run; zero when none is.",
        log.flushing_ns.unwrap_or(0),
    );
}
impl MetricsSnapshot {
    /// The snapshot as Prometheus text exposition (version 0.0.4).
    pub fn render(&self) -> String {
        self.render_into(String::new())
    }
    /// What the snapshot's lists hold beside it: the entities a round lists,
    /// each with its owned text (a session's two labels).
    pub fn held_bytes(&self) -> Option<usize> {
        let session = size_of::<SessionMetrics>()
            .checked_add(self.sessions.first().map_or(0, |session| {
                session.tenant.len().saturating_add(session.session.len())
            }))?
            .checked_add(focal_memory::ALLOCATOR_OVERHEAD.checked_mul(2)?)?;
        let tenants = self
            .agent
            .as_ref()
            .map_or(0, |agent| agent.admission.tenants.len());
        size_of::<Self>()
            .checked_add(session.checked_mul(self.sessions.len())?)?
            .checked_add(
                size_of::<focal_consensus::PeerProgress>().checked_mul(self.root.peers.len())?,
            )?
            .checked_add(size_of::<PeerRtt>().checked_mul(self.peer_rtts.len())?)?
            .checked_add(size_of::<crate::admission::TenantReport>().checked_mul(tenants)?)?
            .checked_add(focal_memory::ALLOCATOR_OVERHEAD.checked_mul(4)?)
    }
    /// The snapshot as Prometheus text, written into `out`.
    pub fn render_into(&self, out: String) -> String {
        let mut text = Text::new(out);
        text.header(
            "focal_node_info",
            "gauge",
            "This node's identity and declared topology; the page's other series carry their own labels alone.",
        );
        let node = self.labels.node.to_string();
        let region = self.labels.region.clone().unwrap_or_default();
        let zone = self.labels.zone.clone().unwrap_or_default();
        let capability = crate::upgrade::CAPABILITY_LEVEL.to_string();
        text.labeled(
            "focal_node_info",
            &[
                ("node", node.as_str()),
                ("cluster", self.labels.cluster.as_str()),
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
        text.counter(
            "focal_metrics_rounds_total",
            "Rounds the sampler has run; each lists the flagged entities of every family first, then the next of the rest.",
            self.rounds,
        );
        text.counter(
            "focal_metrics_rounds_refused_total",
            "Rounds that published no page, refused their room; the page before stood.",
            self.rounds_refused,
        );
        let families = [
            ("sessions", self.listings.sessions),
            ("root_peers", self.listings.root_peers),
            ("peer_rtts", self.listings.peer_rtts),
            ("tenants", self.listings.tenants),
        ];
        for (field, (name, help)) in [
            (
                "focal_metrics_family_total",
                "Entities of a family the node knows: every one is in the family's aggregates.",
            ),
            (
                "focal_metrics_family_flagged",
                "Entities of a family flagged this round: listed first.",
            ),
            (
                "focal_metrics_family_listed",
                "Entities of a family this round lists one by one, within one page.",
            ),
        ]
        .into_iter()
        .enumerate()
        {
            text.header(name, "gauge", help);
            for (family, listing) in families {
                let value = listing.fields().get(field).copied().unwrap_or(0);
                text.labeled(name, &[("family", family)], value);
            }
        }
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
        if let Some(StorageMetrics::Log(log)) = &self.storage {
            render_log(&mut text, log);
        }
        if let Some(StorageMetrics::Wal(wal)) = &self.storage {
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
            render_syncs(&mut text, &wal.syncs);
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
        let members = self.root.peer_aggregates;
        text.header(
            "focal_root_members",
            "gauge",
            "The root group's members as its leader tracks them, by their pipeline's state.",
        );
        for (state, value) in [
            ("probe", members.probing),
            ("replicate", members.replicating),
            ("snapshot", members.snapshotting),
        ] {
            text.labeled("focal_root_members", &[("state", state)], value);
        }
        text.gauge(
            "focal_root_members_inactive",
            "Root members the leader has not heard from within its last check.",
            members.inactive,
        );
        text.gauge(
            "focal_root_members_paused",
            "Root members whose pipeline is paused.",
            members.paused,
        );
        text.gauge(
            "focal_root_member_lag_max",
            "The most entries a root member's stored log is behind this replica's applied index.",
            members.lag_max,
        );
        {
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
        text.header(
            "focal_peer_messages_lost_by_cause_total",
            "counter",
            "Peer requests lost, by what the peer answered: refused for room, an outcome it could not say on any attempt, refused otherwise; or no answer, lost on the way.",
        );
        let answered = self
            .peers
            .lost_refused
            .saturating_add(self.peers.lost_unknown)
            .saturating_add(self.peers.lost_rejected);
        for (cause, lost) in [
            ("refused", self.peers.lost_refused),
            ("unknown", self.peers.lost_unknown),
            ("rejected", self.peers.lost_rejected),
            ("unanswered", self.peers.lost.saturating_sub(answered)),
        ] {
            text.labeled(
                "focal_peer_messages_lost_by_cause_total",
                &[("cause", cause)],
                lost,
            );
        }
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
            "focal_peer_messages_unreachable_total",
            "Peer requests refused at once within the peer's unreachable cooldown, each spared a dial.",
            self.peers.refused_unreachable,
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
            "Connections closed for a newer one of the same identity: the one idle longest, with no request under way and none begun for the request timeout.",
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
            ("busy", self.listener.refused_busy),
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
        text.gauge(
            "focal_peer_rtt_measured",
            "Peers with a measured round-trip time.",
            self.rtt_aggregates.measured,
        );
        text.gauge(
            "focal_peer_rtt_min_ms",
            "The shortest last measured round-trip time to a peer, milliseconds.",
            self.rtt_aggregates.min_ms,
        );
        text.gauge(
            "focal_peer_rtt_max_ms",
            "The longest last measured round-trip time to a peer, milliseconds.",
            self.rtt_aggregates.max_ms,
        );
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
            text.gauge(
                "focal_admission_tenants_queued_items",
                "Queued work items over every admitted tenant.",
                agent.tenant_aggregates.queued_items,
            );
            text.gauge(
                "focal_admission_tenants_queued_bytes",
                "Queued work bytes over every admitted tenant.",
                agent.tenant_aggregates.queued_bytes,
            );
            text.gauge(
                "focal_admission_tenants_at_limit",
                "Admitted tenants charging their whole memory limit.",
                agent.tenant_aggregates.at_limit,
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
            "focal_metrics_sessions_unobserved",
            "Sessions asked whose owner had not answered when the round closed.",
            self.sessions_unobserved,
        );
        let sessions = self.session_aggregates;
        for (name, help, value) in [
            (
                "focal_sessions_hosted",
                "Sessions this node hosts.",
                sessions.hosted,
            ),
            (
                "focal_sessions_stopped",
                "Hosted sessions whose replica stopped.",
                sessions.stopped,
            ),
            (
                "focal_sessions_leading",
                "Hosted sessions this node's running replica leads.",
                sessions.leading,
            ),
            (
                "focal_sessions_leaderless",
                "Hosted sessions whose running replica knows no leader.",
                sessions.leaderless,
            ),
            (
                "focal_sessions_stretched",
                "Hosted sessions whose owner's period is stretched past the configured one.",
                sessions.stretched,
            ),
            (
                "focal_sessions_seeding",
                "Hosted sessions waiting for a seeded checkpoint's chunks.",
                sessions.seeding,
            ),
            (
                "focal_sessions_custody_pending",
                "Hosted sessions waiting for the content a retained delivery names.",
                sessions.custody_pending,
            ),
            (
                "focal_sessions_importing",
                "Hosted sessions waiting for an import's payloads to be sealed.",
                sessions.importing,
            ),
            (
                "focal_sessions_period_longest_ms",
                "The longest period any hosted session's owner took, from one to the next.",
                sessions.longest_period_ms,
            ),
        ] {
            text.gauge(name, help, value);
        }
        let totals = sessions.totals;
        for (name, help, value) in [
            (
                "focal_sessions_peers_unreachable_total",
                "Exchanges of the hosted sessions' replicas the driver could not make at all; those of sessions gone included.",
                totals.peers_unreachable,
            ),
            (
                "focal_sessions_appends_rejected_total",
                "Appends the hosted sessions' replicas refused for not holding the entry before them.",
                totals.appends_rejected,
            ),
            (
                "focal_sessions_appends_rejected_in_order_total",
                "Of those, the refusals the order of a leader's appends should have spared.",
                totals.appends_rejected_in_order,
            ),
            (
                "focal_sessions_frames_held_total",
                "Frames held for one they overtook.",
                totals.frames_held,
            ),
            (
                "focal_sessions_frames_let_go_total",
                "Frames let go past their patience or their lane.",
                totals.frames_let_go,
            ),
            (
                "focal_sessions_frames_stale_total",
                "Frames behind what was already stepped from their source.",
                totals.frames_stale,
            ),
            (
                "focal_sessions_frames_waited_total",
                "Frames that waited for their replica's write in flight to be durable before they were stepped.",
                totals.frames_waited,
            ),
            (
                "focal_sessions_frames_refused_total",
                "Peers' frames a hosted session refused for room before it queued them, its queue or memory full: each lost to its peer.",
                totals.frames_refused,
            ),
            (
                "focal_sessions_requests_refused_total",
                "Participants' requests a hosted session refused for room before it queued them, its queue or memory full.",
                totals.requests_refused,
            ),
            (
                "focal_sessions_frames_dropped_total",
                "Peers' frames the fleet's scheduler dropped at a session's quota once queued.",
                totals.frames_dropped,
            ),
            (
                "focal_sessions_requests_dropped_total",
                "Participants' requests the fleet's scheduler dropped at a session's quota once queued.",
                totals.requests_dropped,
            ),
            (
                "focal_sessions_replication_dropped_total",
                "Replication messages dropped beyond a peer's bound.",
                totals.replication_dropped,
            ),
            (
                "focal_sessions_peer_reports_coalesced_total",
                "Reports of a lost exchange coalesced into one already held for the core.",
                totals.peer_reports_coalesced,
            ),
            (
                "focal_sessions_peer_reports_dropped_total",
                "Reports of a lost exchange dropped beyond the peers a configuration can name.",
                totals.peer_reports_dropped,
            ),
            (
                "focal_sessions_waits_asked_total",
                "Times an owner asked what the log had answered and found its write still out.",
                totals.waits_asked,
            ),
            (
                "focal_sessions_waits_answered_total",
                "Times the log's answer to a write woke its owner.",
                totals.waits_answered,
            ),
            (
                "focal_sessions_waits_swept_total",
                "Times a replica with a write out was looked at because an answer found its owner's signals full.",
                totals.waits_swept,
            ),
            (
                "focal_sessions_periods_refused_total",
                "Periods in which a hosted session's replica was not ticked: refused the room, or still persisting.",
                totals.refused_periods,
            ),
        ] {
            text.counter(name, help, value);
        }
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
            storage: None,
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
                lost_refused: 0,
                lost_unknown: 0,
                lost_rejected: 0,
                busy: 0,
                dials: 1,
                refused_unreachable: 0,
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
            session_aggregates: SessionAggregates::default(),
            rtt_aggregates: RttAggregates::default(),
            listings: Listings::default(),
            rounds: 1,
            rounds_refused: 0,
            sessions_unobserved: 0,
            collection_ms: 3,
            agent: None,
            fence_level: 0,
            announced_level: 1,
        }
    }
    /// A node on the shell exports the node log's names and none of focal-log's
    /// writer's (doc 27 §15.10): counters, each latency a summary with its
    /// quantiles, sum and count, a quantile nothing timed left out.
    #[test]
    fn a_node_on_the_shell_exports_the_node_logs_counts() {
        let budget = focal_memory::MemoryBudget::new(1 << 20, 1 << 16).unwrap();
        let mut sample = snapshot(budget.stats());
        let timed = LogLatency {
            count: 4,
            sum_ns: 4_000,
            p50_ns: Some(900),
            p99_ns: Some(1_200),
            p999_ns: Some(1_200),
        };
        sample.storage = Some(StorageMetrics::Log(LogMetrics {
            frames: 3,
            updates: 4,
            bytes: 12_288,
            flushes: 3,
            flush: timed,
            write: timed,
            commit_wait: LogLatency::default(),
            flushing_ns: Some(250),
        }));
        let text = sample.render();
        for line in [
            "focal_log_frames_total 3\n",
            "focal_log_updates_total 4\n",
            "focal_log_flushes_total 3\n",
            "# TYPE focal_log_flush_nanoseconds summary\n",
            "focal_log_flush_nanoseconds{quantile=\"0.99\"} 1200\n",
            "focal_log_flush_nanoseconds_sum 4000\n",
            "focal_log_flush_nanoseconds_count 4\n",
            "focal_log_commit_wait_nanoseconds_count 0\n",
            "focal_log_flushing_nanoseconds 250\n",
        ] {
            assert!(text.contains(line), "missing {line}");
        }
        assert!(!text.contains("focal_log_commit_wait_nanoseconds{"));
        assert!(!text.contains("focal_wal_"));
    }
    /// A node on focal-log exports how long its group commits took to be
    /// durable as a histogram — each bucket counts the commits within its
    /// bound and every bound below it, the last every commit — and every
    /// node why its peer requests were lost, the unanswered being the rest.
    #[test]
    fn a_node_on_focal_log_exports_its_commits_syncs_and_its_losses_by_cause() {
        let budget = focal_memory::MemoryBudget::new(1 << 20, 1 << 16).unwrap();
        let mut sample = snapshot(budget.stats());
        let mut syncs = focal_log::SyncLatency::default();
        syncs.buckets[0] = 3;
        syncs.buckets[2] = 2;
        syncs.buckets[focal_log::SYNC_BUCKETS - 1] = 1;
        syncs.sum_micros = 1_250_300;
        sample.storage = Some(StorageMetrics::Wal(focal_log::WalWriterStats {
            group_commits: 6,
            syncs,
            ..Default::default()
        }));
        sample.peers.lost = 7;
        sample.peers.lost_refused = 1;
        sample.peers.lost_unknown = 2;
        sample.peers.lost_rejected = 1;
        let text = sample.render();
        for line in [
            "# TYPE focal_wal_sync_seconds histogram\n",
            "focal_wal_sync_seconds_bucket{le=\"0.000250\"} 3\n",
            "focal_wal_sync_seconds_bucket{le=\"0.000500\"} 3\n",
            "focal_wal_sync_seconds_bucket{le=\"0.001000\"} 5\n",
            "focal_wal_sync_seconds_bucket{le=\"0.256000\"} 5\n",
            "focal_wal_sync_seconds_bucket{le=\"+Inf\"} 6\n",
            "focal_wal_sync_seconds_sum 1.250300\n",
            "focal_wal_sync_seconds_count 6\n",
            "focal_peer_messages_lost_by_cause_total{cause=\"refused\"} 1\n",
            "focal_peer_messages_lost_by_cause_total{cause=\"unknown\"} 2\n",
            "focal_peer_messages_lost_by_cause_total{cause=\"rejected\"} 1\n",
            "focal_peer_messages_lost_by_cause_total{cause=\"unanswered\"} 3\n",
        ] {
            assert!(text.contains(line), "missing {line}");
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
        assert!(text.contains("focal_metrics_collection_milliseconds 3\n"));
        assert!(text.contains("focal_metrics_sessions_unobserved 1\n"));
        assert!(text.contains("focal_session_observed{tenant=\"t\",session=\"s\"} 1\n"));
        assert!(text.contains("focal_session_observed{tenant=\"t\",session=\"u\"} 0\n"));
        assert!(text.contains("focal_session_leader{tenant=\"t\",session=\"u\"} 3\n"));
        assert!(text.contains("focal_session_applied_index{tenant=\"t\",session=\"s\"} 7\n"));
        assert!(!text.contains("focal_session_applied_index{tenant=\"t\",session=\"u\"}"));
        assert!(!text.contains("focal_session_apply_lag{tenant=\"t\",session=\"u\"}"));
        assert_eq!(
            MetricsPage::new(sample.clone(), &budget).unwrap().text,
            text
        );
    }
    /// A round takes every answer that comes and closes at its deadline:
    /// the late and the failed stay unobserved, in their places. On the
    /// paused clock the answers and the deadline fall in their order.
    #[tokio::test(start_paused = true)]
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
        assert_eq!(tokio::time::Instant::now(), deadline);
    }
    /// The loopback serves the page's text as it was rendered, judges the
    /// request before it writes, and a scrape that never speaks costs no
    /// other scrape its answer.
    #[tokio::test]
    async fn a_silent_scrape_delays_no_other_and_the_text_is_the_page_s() {
        use tokio::net::{TcpListener, TcpStream};
        let budget = focal_memory::MemoryBudget::new(1 << 20, 1 << 16).unwrap();
        let page = MetricsPage::new(snapshot(budget.stats()), &budget).unwrap();
        let rendered = page.text.clone();
        let (_publish, view) = tokio::sync::watch::channel(Some(page));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(serve_loopback(listener, view));
        let silent = TcpStream::connect(address).await.unwrap();
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
        assert_eq!(body, rendered);
        let (head, body) = exchange("GET /nothing HTTP/1.0\r\n\r\n").await;
        assert!(head.starts_with("HTTP/1.0 404"), "{head}");
        assert_eq!(body, "not found\n");
        let (head, _) = exchange("POST /metrics HTTP/1.0\r\n\r\n").await;
        assert!(head.starts_with("HTTP/1.0 405"), "{head}");
        // Three answers while the silent scrape still holds its connection:
        // served side by side, not behind its two-second bound, which
        // would have released it first.
        let silent = silent.into_std().unwrap();
        assert_eq!(
            silent.peek(&mut [0u8; 1]).map_err(|error| error.kind()),
            Err(std::io::ErrorKind::WouldBlock),
            "the silent scrape was released before the others were answered"
        );
        drop(silent);
        server.abort();
    }
    /// The node's identity is one series, its labels escaped; every other
    /// series carries its own labels alone, the target's being the scraper's
    /// to attach (`Text`).
    #[test]
    fn the_node_info_carries_the_identity_once_and_every_series_its_own_labels() {
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
        assert!(text.contains(&format!(
            "focal_node_info{{node=\"7\",cluster=\"ab\\\"cd\",role=\"founder\",region=\"eu-a\",zone=\"\",capability=\"{}\"}} 1",
            crate::upgrade::CAPABILITY_LEVEL
        )));
        assert_eq!(
            text.lines()
                .filter(|line| !line.starts_with('#') && line.contains("node=\""))
                .count(),
            1,
            "the identity is on one series"
        );
        assert!(text.contains(&format!("\nfocal_memory_used_bytes {used}\n")));
        assert!(text.contains("\nfocal_peer_messages_delivered_total 4\n"));
        assert!(text.contains("\nfocal_session_apply_lag{tenant=\"t\",session=\"s\"} 2\n"));
        assert!(!text.contains("focal_session_retention_floor{"));
        assert!(text.contains("\nfocal_upgrade_announced_level 1\n"));
        assert!(text.ends_with('\n'));
    }
}
