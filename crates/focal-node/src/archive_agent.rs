//! The archive agent (26 §4). On every node, for every replica it hosts,
//! the agent offers terminal, released claims to the committed core; where
//! this node is the authority and the retention floor has passed a family's
//! last event, it takes the family's bundle from the core, seals it as
//! content of the ledger under its current placement with a receipt from
//! every required copy, and proposes the retirement record. One family per
//! replica per tick; a family that is not eligible yet, a copy that has not
//! answered, or a refusal waits for a later tick. The agent holds no state
//! the records do not: a restart resumes its walk from the status index.
use crate::fleet::ReplicaHost;
use crate::network_service::NetworkHandles;
use focal_client::admin::AdminArchiveAgent;
use focal_core::native::retirement::RetirementCursor;
use focal_model::LedgerId;
use focal_wire::AccessError;
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::sync::watch;

/// Milliseconds between the agent's ticks; the default is five seconds.
pub const INTERVAL_ENV: &str = "FOCAL_RETIRE_INTERVAL_MS";
const DEFAULT_INTERVAL: Duration = Duration::from_secs(5);
/// Milliseconds of logical time a family's last event must have settled
/// for before it is offered: the grace a finished claim stays readable in
/// the core for. The default is one day.
pub const GRACE_ENV: &str = "FOCAL_RETIRE_AFTER_MS";
const DEFAULT_GRACE_MS: u64 = 24 * 60 * 60 * 1000;
/// Status-index rows one tick visits per replica.
const VISITS_PER_TICK: usize = 1024;
/// Replicas the agent keeps a walk cursor for.
const MAX_CURSORS: usize = 4096;

/// The operator's view of the archive agent on this node.
#[derive(Clone)]
pub struct ArchiveHandle(watch::Receiver<AdminArchiveAgent>);
impl ArchiveHandle {
    pub fn status(&self) -> AdminArchiveAgent {
        *self.0.borrow()
    }
}

pub struct ArchiveAgent {
    interval: Duration,
    grace_ms: u64,
    cursors: BTreeMap<LedgerId, Option<RetirementCursor>>,
    ticks: u64,
    proposed: u64,
    waiting: u64,
    status: watch::Sender<AdminArchiveAgent>,
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| u64::try_from(elapsed.as_millis()).ok())
        .unwrap_or(0)
}

impl ArchiveAgent {
    pub fn new(interval: Duration, grace_ms: u64) -> (Self, ArchiveHandle) {
        let interval = interval.max(Duration::from_millis(10));
        let (status, receiver) = watch::channel(AdminArchiveAgent {
            interval_ms: u64::try_from(interval.as_millis()).unwrap_or(u64::MAX),
            grace_ms,
            ticks: 0,
            proposed: 0,
            waiting: 0,
            last_tick_ms: 0,
        });
        (
            Self {
                interval,
                grace_ms,
                cursors: BTreeMap::new(),
                ticks: 0,
                proposed: 0,
                waiting: 0,
                status,
            },
            ArchiveHandle(receiver),
        )
    }
    fn publish(&self) {
        let status = AdminArchiveAgent {
            interval_ms: u64::try_from(self.interval.as_millis()).unwrap_or(u64::MAX),
            grace_ms: self.grace_ms,
            ticks: self.ticks,
            proposed: self.proposed,
            waiting: self.waiting,
            last_tick_ms: now_ms(),
        };
        let _ = self.status.send(status);
    }
    /// The interval and grace from the environment, else the defaults.
    pub fn from_env() -> (Self, ArchiveHandle) {
        let millis = |name: &str| {
            std::env::var_os(name).and_then(|value| {
                value
                    .to_str()
                    .and_then(|text| text.trim().parse::<u64>().ok())
            })
        };
        let interval = millis(INTERVAL_ENV)
            .filter(|millis| *millis > 0)
            .map_or(DEFAULT_INTERVAL, Duration::from_millis);
        Self::new(interval, millis(GRACE_ENV).unwrap_or(DEFAULT_GRACE_MS))
    }
    pub async fn run(mut self, handles: &NetworkHandles) -> Result<(), AccessError> {
        if tokio::runtime::Handle::try_current().is_err() {
            return Err(AccessError::Unavailable);
        }
        loop {
            tokio::time::sleep(self.interval).await;
            self.tick(handles).await;
        }
    }
    /// One pass over the replicas this node hosts.
    pub async fn tick(&mut self, handles: &NetworkHandles) {
        let mut after = None;
        while let Some((ledger, host)) = handles.fleet.next_host(after) {
            after = Some(ledger);
            // A replica that is not native, not authoritative, or refuses
            // the walk simply waits for a later tick.
            let _ = self.step(ledger, &host, handles).await;
        }
        self.ticks = self.ticks.saturating_add(1);
        self.publish();
    }
    async fn step(
        &mut self,
        ledger: LedgerId,
        host: &ReplicaHost,
        handles: &NetworkHandles,
    ) -> Result<(), AccessError> {
        let cursor = self.cursors.get(&ledger).copied().flatten();
        let candidates = host
            .retirement_candidates(cursor, VISITS_PER_TICK)
            .await
            .map_err(|_| AccessError::Unavailable)?;
        if self.cursors.len() >= MAX_CURSORS && !self.cursors.contains_key(&ledger) {
            self.cursors.clear();
        }
        self.cursors.insert(ledger, candidates.next);
        for root in candidates.claims {
            let Some(archived) = host
                .archive_family(root, self.grace_ms)
                .await
                .map_err(|_| AccessError::Unavailable)?
            else {
                continue;
            };
            let crate::fleet::ArchivedFamily {
                bundle,
                through,
                _allocation,
                ..
            } = archived;
            let policy = handles
                .content
                .policy(ledger)
                .await?
                .ok_or(AccessError::Unavailable)?;
            let route = policy.scope().route_epoch;
            let outcome = handles.evidence.archive(ledger, route, bundle).await?;
            drop(_allocation);
            if !outcome.obligation.satisfied() {
                // A required copy has not answered with a receipt; the
                // bundle is sealed and the next tick asks again.
                self.waiting = self.waiting.saturating_add(1);
                return Ok(());
            }
            host.propose_retirement(
                root,
                outcome.reference.root,
                outcome.reference.length,
                through,
            )
            .await
            .map_err(|_| AccessError::Unavailable)?;
            self.proposed = self.proposed.saturating_add(1);
            return Ok(());
        }
        Ok(())
    }
}
