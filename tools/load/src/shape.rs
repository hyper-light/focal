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
    /// `enrolled`: the enrolled client's directory (its join journal, key and
    /// adopted issuers).
    #[serde(default)]
    pub enrollment: Option<PathBuf>,
    /// `enrolled`: the participant every claim targets, as 32 hex digits (a
    /// remote client cannot read the node's `IDENTITY`).
    #[serde(default)]
    pub worker: Option<String>,
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
    /// `embedded` only: after the run, close the node and reopen the same
    /// directory, timing exec-to-serving and the first linearizable read —
    /// single-node recovery at exactly this run's retained size.
    #[serde(default)]
    pub reopen: bool,
}

fn default_seed() -> u64 {
    1
}
fn default_concurrency() -> u16 {
    1
}

impl WorkloadShape {
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
            Transport::Enrolled if self.enrollment.is_none() || self.worker.is_none() => {
                return Err(
                    "transport enrolled needs enrollment (the client's directory) and worker"
                        .to_string(),
                );
            }
            _ => {}
        }
        if self.rate == Some(0) {
            return Err("rate must be greater than zero".to_string());
        }
        Ok(())
    }

    /// The frame profile: the shape's, else the transport's own.
    pub fn profile(&self) -> Profile {
        self.profile.unwrap_or(match self.transport {
            Transport::Embedded => Profile::ProjectionOnly,
            Transport::Unix | Transport::Enrolled => Profile::AuthoredV1,
        })
    }
}
