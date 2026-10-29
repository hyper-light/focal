//! The workload description (`--shape` YAML): how many native claim creations
//! and reads, from how many concurrent callers, against which node, and
//! whether the embedded node is reopened afterwards to time its recovery.
//! Every count has a stated maximum; a shape past one is refused before any
//! work (R11 §5).
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// The most claims one run creates.
pub const MAX_CLAIMS: u64 = 1_000_000;
/// The most reads one run issues.
pub const MAX_READS: u64 = 10_000_000;
/// The most concurrent callers one run drives (each is an OS thread with its
/// own client and runtime).
pub const MAX_CONCURRENCY: u16 = 64;
/// The most of one caller's writes under way at once (`inflight`).
pub const MAX_INFLIGHT: u16 = 256;
/// The longest a side's inbox read is asked to wait at the node (`inbox_wait_ms`), and how long
/// it waits by default: Amazon SQS's longest long poll (`WaitTimeSeconds` up to 20 s), two
/// thirds of the 30 s a node and its client each hold a request for (RFC 6202 §5.5: a long poll's
/// timeout "ought to be set to a high value", 30 s the safer one), the last third for the empty
/// page's way back. A read waits the whole of it only while nothing comes for its side: a
/// delivery ends the wait at once, so the wait costs no latency, only the empty pages a shorter
/// one would have been answered with.
pub const MAX_INBOX_WAIT_MS: u32 = 20_000;

/// Which node the callers reach.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Transport {
    /// An `EmbeddedNode` opened in this process, over the embedded transport.
    #[default]
    Embedded,
    /// A running `focal start node` node, over its Unix socket `<data_dir>/focal.sock`,
    /// as that node's own local participant (what the CLI does without a
    /// `--client-context`).
    Unix,
    /// A remote cluster over QUIC, as the client a `focal enroll context`
    /// left in `enrollment` (`PendingClientJoin::remote_client`): what an
    /// agent on another machine is, and what the comparison drives
    /// (`docs/qualification/competitive-p99.md`).
    Enrolled,
}

/// What becomes of each claim after its creation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Lifecycle {
    /// Created and left open: the comparison's workload.
    #[default]
    Create,
    /// Created, cancelled by its issuer, then its scope released: terminal
    /// and released, so it may retire (26 §4, doc 31 M2). Each later step
    /// reads the claim first, as an agent resolves what it binds to, and is
    /// sent once the step before it committed.
    CancelRelease,
    /// Delivered as a work queue delivers a message, end to end (the comparison's `deliver`
    /// mode): the issuer creates the claim for its consumer and posts it; the consumer, a
    /// principal of its own, finds it in the ledger's events, acquires its receipt, submits its
    /// testament and posts it; the issuer finds the testament in the events, receives it,
    /// enters its whole-work validation, which satisfies a receipt claim, and releases the
    /// claim's scope, which ends its lifecycle: a delivered message's family then retires from
    /// memory to the cold tier (doc 31 4.7). Eight writes (seven before the release was the
    /// last), each compiled against the objects it binds to as read just before it
    /// (`requirements`), as an agent's client compiles them; neither side is told of the
    /// other's step but through the events it polls, as a consumer learns of a message only by
    /// fetching it.
    Deliver,
    /// Work done and judged among agents, the whole flow an agent runs (02 §3, §6): the issuer
    /// creates a work claim whose required whole-work validation names a third agent its
    /// evaluator, checking the claim's output slot, and posts it to the next agent; the worker
    /// finds it in its events, acquires its receipt, submits a work artifact for the slot and its
    /// testament citing it, and posts the testament; the issuer finds the testament in its events,
    /// receives it and enters its whole-work validation; the evaluator finds its evaluation due in
    /// its events, begins it and reports a verdict with its result artifact; the issuer finds the
    /// claim satisfied in its events and releases its scope. Eleven writes, among agents only:
    /// every hand-off is learned from the recipient's own events, as an agent learns of work.
    Work,
}
impl Lifecycle {
    /// The writes one claim's lifecycle takes.
    pub fn steps(self) -> u8 {
        match self {
            Self::Create => 1,
            Self::CancelRelease => 3,
            Self::Deliver => 8,
            Self::Work => 11,
        }
    }
    /// Whether the lifecycle goes between principals that learn of each other's steps through
    /// their events: deliver's issuer and consumer, work's issuer, worker and evaluator.
    pub fn among_principals(self) -> bool {
        matches!(self, Self::Deliver | Self::Work)
    }
}

/// The native content profile the request frames are encoded for. It must be
/// the profile the ledger was activated with: the embedded node is activated
/// by this tool (`projection_only` unless the shape says otherwise); a node
/// activated by `focal activate native` is `authored_v1`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Profile {
    ProjectionOnly,
    AuthoredV1,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkloadShape {
    /// Total native claim creations to submit end to end (1..=MAX_CLAIMS).
    pub claims: u64,
    /// Deterministic seed for request and claim identities, so a run is
    /// reproducible and two runs never collide.
    #[serde(default = "default_seed")]
    pub seed: u64,
    /// Claim reads to issue after the creations, cycling through the committed
    /// claims (0..=MAX_READS). Zero (the default) measures the write path only.
    #[serde(default)]
    pub reads: u64,
    /// Which node the callers reach (default `embedded`).
    #[serde(default)]
    pub transport: Transport,
    /// `unix`: the running node's data directory (its `IDENTITY` and
    /// `focal.sock`). `embedded`: a directory to open the node on instead of a
    /// fresh temporary one (kept afterwards, so a later run can reopen it).
    #[serde(default)]
    pub data_dir: Option<PathBuf>,
    /// The frame profile; by default the transport's own (see [`Profile`]).
    #[serde(default)]
    pub profile: Option<Profile>,
    /// Concurrent callers (1..=MAX_CONCURRENCY, default 1). The claims and the
    /// reads are split evenly among them.
    #[serde(default = "default_concurrency")]
    pub concurrency: u16,
    /// The clients the callers share (1..=concurrency; by default one each),
    /// caller `i` sending on client `i % connections`, each client's requests
    /// carried concurrently on its one connection. One participant holds at
    /// most sixteen connections to a node (its admission bound): more callers
    /// than that share them, as the other systems' generators send many
    /// requests over few connections.
    #[serde(default)]
    pub connections: Option<u16>,
    /// The most of one caller's writes under way at once (default 1: each
    /// caller waits for its write's answer). More, its writes are sent at
    /// their places on the schedule whatever earlier ones are doing; one due
    /// while all are out waits for the first to end, its latency counted
    /// from its place.
    #[serde(default = "default_inflight")]
    pub inflight: u16,
    /// `enrolled`: the enrolled client's directory (its join journal, key and
    /// adopted issuers).
    #[serde(default)]
    pub enrollment: Option<PathBuf>,
    /// `enrolled`: the participant every claim targets, as 32 hex digits (a
    /// remote client cannot read the node's `IDENTITY`). Not with `deliver`, whose claims target
    /// the consumer.
    #[serde(default)]
    pub worker: Option<String>,
    /// `deliver` over `enrolled`: the consumer's client directory, a second enrolled client
    /// whose principal every claim targets and who completes them. Over `embedded`, the
    /// consumer is the node's worker identity.
    #[serde(default)]
    pub consumer_enrollment: Option<PathBuf>,
    /// `deliver`: how long a side waits before it asks for the ledger's events again after
    /// finding none new, in milliseconds (1..=1000, default 2): its polling interval, as a
    /// consumer that fetches has one.
    #[serde(default = "default_poll_ms")]
    pub poll_ms: u16,
    /// `deliver` and `work`: how long a side's inbox read waits at the node for its next page
    /// before the node answers it empty, in milliseconds (0..=MAX_INBOX_WAIT_MS, default
    /// MAX_INBOX_WAIT_MS): the side asks the node to hold the read until a delivery to it
    /// applies (the waiting profile, `Operation::NativeWait`) where it polled. Kafka's consumer
    /// waits `fetch.max.wait.ms`, 500 by default, sized for a fetch that usually finds data; an
    /// agent's inbox is mostly idle, and 1,000 agents waiting 500 ms each read about 1,600 empty
    /// pages a second (CI 39870118553). Zero, or a node that does not wait, polls every
    /// `poll_ms`.
    #[serde(default = "default_inbox_wait_ms")]
    pub inbox_wait_ms: u32,
    /// `deliver` on the embedded node: the agents the messages go among, each an identity of its
    /// own with its own connection, request generations and inbox read, that issues claims to
    /// the agent after it around a ring and serves those the agent before it issues; lifecycle
    /// `n` (the callers' interleaved places on the schedule) is agent `n mod agents`'s, to agent
    /// `n + 1 mod agents`. At least two, and no more than the session holds principals beside
    /// the node's own issuer and worker ([`max_agents`]). Unset, one issuer and one consumer.
    #[serde(default)]
    pub agents: Option<u16>,
    /// `agents` over `enrolled`: the directory of the agents' client data directories, each an
    /// agent's own (`<dir>/agent-*`, as `focal --data-dir <dir>/<agent> enroll context <agent>`
    /// leaves it), in their order, the first `agents` of them; each agent is the principal its
    /// credential names, on a connection of its own.
    #[serde(default)]
    pub agent_enrollments: Option<PathBuf>,
    /// `agents` over `enrolled`: the nodes the agents are homed on, agent `n` on home
    /// `n mod homes` (each a node's advertised address and data server name, as `focal cluster
    /// list nodes` gives them), at most one an agent. A homed agent reads, and waits on its
    /// inbox, at its home, and writes at the ledger's leader (focal-client's `ReadRoute::Home`),
    /// as clients spread over a cluster's nodes do. Unset, each agent dials the node its
    /// invitation names and reads where it writes.
    #[serde(default)]
    pub homes: Option<Vec<Home>>,
    /// A run among agents over several ledgers at once, each counted apart in the report's
    /// `ledgers` rows and in total in its `totals`. Ledger `k` of `L` is driven by callers `k`,
    /// `k + L`, … among agents `k`, `k + L`, …, and takes every `L`th lifecycle of the run's one
    /// schedule, so each ledger is offered the same share of the claims and of the rate
    /// (`seats`). On `embedded` each is a node the run opens, a ledger of its own; on `enrolled`
    /// each names a session of the enrollment's tenant and the nodes holding its replica, which
    /// its agents are homed over. Unset, the run drives one ledger: the embedded node's, or the
    /// enrollment's.
    #[serde(default)]
    pub ledgers: Option<Vec<LedgerShape>>,
    /// Offered creations a second over all callers. Set, the writes are
    /// paced on a fixed schedule and each latency counts from its request's
    /// intended start (wrk2's constant-throughput method), so a stall is
    /// charged to every request it delays; unset, each caller sends as fast
    /// as it is answered (closed loop).
    #[serde(default)]
    pub rate: Option<u64>,
    /// Milliseconds from the phase's start whose requests are sent and not
    /// measured: connections, handshakes and caches warm, as the comparison's
    /// generator excludes its warm-up from every competitor's percentiles
    /// (docs/qualification/competitive-p99.md). Zero (the default) measures all.
    #[serde(default)]
    pub warmup_ms: u64,
    /// What becomes of each claim after its creation (default `create`: left
    /// open). Every step of a lifecycle counts as a write; `claims` and `rate`
    /// count lifecycles, each started at its place on the schedule.
    #[serde(default)]
    pub lifecycle: Lifecycle,
    /// The most of one caller's lifecycle reads under way at once: unset, as many as its writes
    /// (`inflight`). Every step of a lifecycle after its creation reads what it binds to, at
    /// least at the write before it, before it writes, so a caller's reads are its lifecycles'
    /// pace: at one read a caller, sixteen callers driving 500 agents' work read sixteen at a
    /// time, about 80 a second over a 160 ms round trip where the steps asked 180, and a claim
    /// took 69 s from its post to its settlement while each of the cluster's writes took 0.3 s
    /// (compare/work-faults, netem `delay 80ms 30ms`, 2026-10-23).
    #[serde(default)]
    pub lifecycle_reads: Option<u16>,
    /// `embedded` only: after the run, close the node and reopen the same
    /// directory, timing exec-to-serving and the first linearizable read —
    /// single-node recovery at exactly this run's retained size.
    #[serde(default)]
    pub reopen: bool,
}

/// A node an agent is homed on ([`WorkloadShape::homes`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Home {
    /// The node's advertised address, `host:port`.
    pub endpoint: String,
    /// The name its data certificate presents.
    pub server_name: String,
}

/// A ledger a run among agents drives ([`WorkloadShape::ledgers`]).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LedgerShape {
    /// `enrolled`: the ledger's session, 32 hex digits, in the enrollment's tenant, as `focal
    /// context add --enrolled-as … --session …` addresses one (a principal's grant is its
    /// tenant's). Not on `embedded`, where each ledger is a node the run opens, its session the
    /// node's own.
    #[serde(default)]
    pub session: Option<String>,
    /// `enrolled` agents: the nodes holding the ledger's replica, the ledger's agents homed over
    /// them as [`WorkloadShape::homes`] homes a run's: its agent `n` on home `n mod homes`, at most
    /// one an agent. Unset, its agents dial the node their invitations name.
    #[serde(default)]
    pub homes: Option<Vec<Home>>,
}

impl LedgerShape {
    /// The session the entry names, parsed.
    pub fn session_id(&self) -> Result<Option<[u8; 16]>, String> {
        self.session
            .as_deref()
            .map(|text| {
                focal_client::input::parse_id(text)
                    .map_err(|_| format!("ledger session {text:?} is not 32 hex digits"))
            })
            .transpose()
    }
}

fn default_seed() -> u64 {
    1
}
fn default_concurrency() -> u16 {
    1
}
fn default_inflight() -> u16 {
    1
}
fn default_poll_ms() -> u16 {
    2
}
fn default_inbox_wait_ms() -> u32 {
    MAX_INBOX_WAIT_MS
}

/// The most agents a run goes among: the principals a session keeps a request generation window
/// for (`NativeLimits::principals`), less the node's own issuer and worker.
pub fn max_agents() -> usize {
    focal_core::native::NativeLimits::default()
        .principals
        .saturating_sub(2)
}

impl WorkloadShape {
    /// The most of one caller's lifecycle reads under way at once (the field's): as many as its
    /// writes unless set lower.
    pub fn lifecycle_reads(&self) -> u16 {
        self.lifecycle_reads.unwrap_or(self.inflight)
    }
    pub fn validate(&self) -> Result<(), String> {
        if self.claims == 0 {
            return Err("claims must be greater than zero".to_string());
        }
        if self.claims > MAX_CLAIMS {
            return Err(format!("claims must not exceed {MAX_CLAIMS}"));
        }
        if self.reads > MAX_READS {
            return Err(format!("reads must not exceed {MAX_READS}"));
        }
        if self.concurrency == 0 || self.concurrency > MAX_CONCURRENCY {
            return Err(format!("concurrency must be 1..={MAX_CONCURRENCY}"));
        }
        if self
            .connections
            .is_some_and(|connections| connections == 0 || connections > self.concurrency)
        {
            return Err("connections must be 1..=concurrency".to_string());
        }
        if self.inflight == 0 || self.inflight > MAX_INFLIGHT {
            return Err(format!("inflight must be 1..={MAX_INFLIGHT}"));
        }
        if self
            .lifecycle_reads
            .is_some_and(|reads| reads == 0 || reads > self.inflight)
        {
            return Err("lifecycle_reads must be 1..=inflight".to_string());
        }
        // The seed sits above bit 40 of the id space; a larger one would fold
        // onto another seed's identities.
        if self.seed >= 1 << 24 {
            return Err("seed must be below 2^24".to_string());
        }
        match self.transport {
            Transport::Unix if self.data_dir.is_none() => {
                return Err(
                    "transport unix needs data_dir (the running node's directory)".to_string(),
                );
            }
            Transport::Unix | Transport::Enrolled if self.reopen => {
                return Err("reopen applies to the embedded transport only".to_string());
            }
            Transport::Enrolled
                if self.enrollment.is_none()
                    || (!self.lifecycle.among_principals() && self.worker.is_none()) =>
            {
                return Err(
                    "transport enrolled needs enrollment (the client's directory) and worker"
                        .to_string(),
                );
            }
            _ => {}
        }
        if self.lifecycle == Lifecycle::Work && self.agents.is_none_or(|agents| agents < 3) {
            return Err(
                "work goes among agents: an issuer, a worker and an evaluator, 3 at least".into(),
            );
        }
        if self.lifecycle.among_principals() {
            // The consumer is a second principal, and its steps are authored operations.
            if self.profile() != Profile::AuthoredV1 {
                return Err("deliver compiles authored operations: profile authored_v1".into());
            }
            match self.transport {
                Transport::Unix => {
                    return Err(
                        "deliver needs a second principal: transport embedded or enrolled".into(),
                    );
                }
                Transport::Enrolled
                    if self.consumer_enrollment.is_none() && self.agents.is_none() =>
                {
                    return Err("deliver over enrolled needs consumer_enrollment".into());
                }
                Transport::Enrolled if self.worker.is_some() => {
                    return Err("deliver's claims target the consumer, never worker".into());
                }
                _ => {}
            }
            if self.poll_ms == 0 || self.poll_ms > 1000 {
                return Err("poll_ms must be 1..=1000".into());
            }
            if self.inbox_wait_ms > MAX_INBOX_WAIT_MS {
                return Err(format!("inbox_wait_ms must be 0..={MAX_INBOX_WAIT_MS}"));
            }
            if let Some(agents) = self.agents {
                // Over several ledgers each ledger's share is bounded instead (`validate_ledgers`).
                if self.ledgers.is_none() && (agents < 2 || usize::from(agents) > max_agents()) {
                    return Err(format!("agents must be 2..={}", max_agents()));
                }
                match (self.transport, &self.agent_enrollments) {
                    (Transport::Embedded, None) | (Transport::Enrolled, Some(_)) => {}
                    (Transport::Enrolled, None) => {
                        return Err("agents over enrolled need agent_enrollments".into());
                    }
                    (Transport::Embedded, Some(_)) => {
                        return Err("agent_enrollments apply to the enrolled transport".into());
                    }
                    (Transport::Unix, _) => {
                        return Err("agents run on the embedded or the enrolled transport".into());
                    }
                }
            } else if self.agent_enrollments.is_some() {
                return Err("agent_enrollments apply to a run among agents".into());
            }
            if let Some(homes) = &self.homes {
                if self.agent_enrollments.is_none() {
                    return Err("homes apply to enrolled agents".into());
                }
                if homes.is_empty() || homes.len() > usize::from(self.agents.unwrap_or(0)) {
                    return Err("homes must be 1..=agents".into());
                }
            }
        } else if self.consumer_enrollment.is_some() {
            return Err("consumer_enrollment applies to the deliver lifecycle".into());
        } else if self.agents.is_some() {
            return Err("agents apply to the deliver lifecycle".into());
        } else if self.homes.is_some() {
            return Err("homes apply to enrolled agents".into());
        }
        if let Some(ledgers) = &self.ledgers {
            self.validate_ledgers(ledgers)?;
        }
        if self.rate == Some(0) {
            return Err("rate must be greater than zero".to_string());
        }
        Ok(())
    }

    /// A run over several ledgers: among agents, each ledger driven by a caller and a share of the
    /// lifecycles of its own, among agents of its own, as many as its lifecycle goes among and no
    /// more than a session keeps principals for; on `enrolled` each names a session of its own,
    /// and the homes its agents take; on `embedded`, neither.
    fn validate_ledgers(&self, ledgers: &[LedgerShape]) -> Result<(), String> {
        let Some(agents) = self.agents.filter(|_| self.lifecycle.among_principals()) else {
            return Err("ledgers apply to a run among agents".into());
        };
        let count = u16::try_from(ledgers.len())
            .ok()
            .filter(|count| (1..=self.concurrency).contains(count))
            .ok_or("ledgers must be 1..=concurrency: each is driven by a caller of its own")?;
        if self.claims < u64::from(count) {
            return Err("claims must be at least the ledgers: each is driven".into());
        }
        if self.homes.is_some() {
            return Err("over ledgers each ledger names the homes of its agents".into());
        }
        if self.reads > 0 || self.reopen || self.data_dir.is_some() {
            return Err("reads, reopen and data_dir apply to a run of one ledger".into());
        }
        let least = if self.lifecycle == Lifecycle::Work {
            3
        } else {
            2
        };
        let mut sessions = std::collections::BTreeSet::new();
        for (index, ledger) in (0..count).zip(ledgers) {
            // Ledger `index`'s agents: `agents / ledgers`, the first ledgers one more.
            let its = crate::seats::share(u64::from(agents), count, index)
                .map_err(|error| error.to_string())?;
            if its < least || usize::try_from(its).map_or(true, |its| its > max_agents()) {
                return Err(format!(
                    "each ledger's agents must be {least}..={}: {agents} agents over {count} \
                     ledgers give ledger {index} {its}",
                    max_agents()
                ));
            }
            match (self.transport, ledger.session_id()?) {
                (Transport::Embedded, None) => {}
                (Transport::Embedded, Some(_)) => {
                    return Err(
                        "on embedded each ledger is a node the run opens: it names no session"
                            .into(),
                    );
                }
                (Transport::Enrolled, Some(session)) => {
                    if !sessions.insert(session) {
                        return Err(format!("ledger {index} names a session named before it"));
                    }
                }
                (Transport::Enrolled, None) => {
                    return Err("on enrolled each ledger names its session".into());
                }
                (Transport::Unix, _) => {
                    return Err("ledgers run on the embedded or the enrolled transport".into());
                }
            }
            if let Some(homes) = &ledger.homes {
                if self.transport != Transport::Enrolled {
                    return Err("homes apply to enrolled agents".into());
                }
                if homes.is_empty() || u64::try_from(homes.len()).map_or(true, |homes| homes > its)
                {
                    return Err(format!(
                        "ledger {index}'s homes must be 1..={its}, its agents"
                    ));
                }
            }
        }
        Ok(())
    }

    /// The ledgers the run drives: the shape's, else the one its transport reaches.
    pub fn ledger_count(&self) -> Result<u16, String> {
        match &self.ledgers {
            Some(ledgers) => u16::try_from(ledgers.len()).map_err(|_| "ledgers past u16".into()),
            None => Ok(1),
        }
    }

    /// The nodes ledger `ledger`'s agents are homed over: its entry's, or a run of one ledger's.
    pub fn homes_of(&self, ledger: u16) -> Option<&[Home]> {
        match &self.ledgers {
            Some(ledgers) => ledgers.get(usize::from(ledger))?.homes.as_deref(),
            None => self.homes.as_deref().filter(|_| ledger == 0),
        }
    }

    /// The frame profile: the shape's, else the transport's own.
    pub fn profile(&self) -> Profile {
        self.profile.unwrap_or(match self.transport {
            Transport::Embedded => Profile::ProjectionOnly,
            Transport::Unix | Transport::Enrolled => Profile::AuthoredV1,
        })
    }
}
