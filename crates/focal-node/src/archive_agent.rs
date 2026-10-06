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
    seals_proposed: u64,
    seals_waiting: u64,
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
            seals_proposed: 0,
            seals_waiting: 0,
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
                seals_proposed: 0,
                seals_waiting: 0,
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
            seals_proposed: self.seals_proposed,
            seals_waiting: self.seals_waiting,
            last_tick_ms: now_ms(),
        };
        let _ = self.status.send(status);
    }
    /// The interval and grace from the environment, else the defaults.
    pub fn from_env() -> (Self, ArchiveHandle) {
        let (interval, grace_ms) = settings_from_env();
        Self::new(interval, grace_ms)
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
            let _ = self.seal_step(ledger, &host, handles).await;
        }
        self.ticks = self.ticks.saturating_add(1);
        self.publish();
    }
    /// One seal for the replica when the committed state yields one worth
    /// proposing (F12): the bundle (and a fold's) sealed as content under
    /// custody first, the record proposed once every required copy holds
    /// them.
    async fn seal_step(
        &mut self,
        ledger: LedgerId,
        host: &ReplicaHost,
        handles: &NetworkHandles,
    ) -> Result<(), AccessError> {
        let Some(sealed) = host
            .seal_bundle()
            .await
            .map_err(|_| AccessError::Unavailable)?
        else {
            return Ok(());
        };
        let crate::fleet::SealedOutcomes {
            plan,
            bundle,
            fold,
            _allocation,
            ..
        } = sealed;
        let policy = handles
            .content
            .policy(ledger)
            .await?
            .ok_or(AccessError::Unavailable)?;
        let route = policy.scope().route_epoch;
        let outcome = handles.evidence.archive(ledger, route, bundle).await?;
        let folded = match fold {
            Some((plan, bytes, _)) => {
                let outcome = handles.evidence.archive(ledger, route, bytes).await?;
                Some((plan, outcome))
            }
            None => None,
        };
        drop(_allocation);
        if !outcome.obligation.satisfied()
            || folded
                .as_ref()
                .is_some_and(|(_, outcome)| !outcome.obligation.satisfied())
        {
            self.seals_waiting = self.seals_waiting.saturating_add(1);
            return Ok(());
        }
        let fold = folded.map(|(plan, outcome)| focal_core::native::seal::Fold {
            first: plan.first,
            last: plan.last,
            bundle: outcome.reference.root,
            bytes: outcome.reference.length,
        });
        host.propose_seal(plan, outcome.reference.root, outcome.reference.length, fold)
            .await
            .map_err(|_| AccessError::Unavailable)?;
        self.seals_proposed = self.seals_proposed.saturating_add(1);
        Ok(())
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

/// The interval and grace from the environment, else the defaults.
pub(crate) fn settings_from_env() -> (Duration, u64) {
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
    (interval, millis(GRACE_ENV).unwrap_or(DEFAULT_GRACE_MS))
}

/// The archive agent of an embedded node (26 §4, and the audit's F12): the
/// walk the network node's agent runs, on the owner thread at the agent's
/// interval, each bundle sealed into the node's own store — the one copy
/// such a node has — before its record is proposed and polled to
/// commitment. A step that cannot run now (a candidate in flight, the
/// session not native, custody refused) waits for a later tick, as the
/// network agent's does, and is counted.
pub(crate) struct EmbeddedArchive {
    interval: Duration,
    grace_ms: u64,
    next: std::time::Instant,
    cursor: Option<focal_core::native::retirement::RetirementCursor>,
    pub(crate) proposed: u64,
    pub(crate) seals_proposed: u64,
    pub(crate) deferred: u64,
}
impl EmbeddedArchive {
    pub(crate) fn from_env() -> Self {
        let (interval, grace_ms) = settings_from_env();
        Self {
            interval: interval.max(Duration::from_millis(10)),
            grace_ms,
            next: std::time::Instant::now(),
            cursor: None,
            proposed: 0,
            seals_proposed: 0,
            deferred: 0,
        }
    }
    /// One tick once the interval has passed since the last.
    pub(crate) fn maintain(
        &mut self,
        node: &mut crate::embedded::EmbeddedNode,
        budget: &focal_memory::MemoryBudget,
    ) -> Result<(), crate::embedded::NodeError> {
        let now = std::time::Instant::now();
        if now < self.next {
            return Ok(());
        }
        self.next = now.checked_add(self.interval).ok_or_else(|| {
            crate::embedded::NodeError::Domain("the archive interval overflows the clock".into())
        })?;
        if node.session.native_core().is_err() {
            return Ok(());
        }
        if self.retire_step(node, budget).is_err() {
            self.deferred = self.deferred.saturating_add(1);
        }
        if self.seal_step(node, budget).is_err() {
            self.deferred = self.deferred.saturating_add(1);
        }
        Ok(())
    }
    /// Seal `bytes` as an object of the ledger's tenant domain in the
    /// node's own store, as the content host seals an archive bundle.
    fn seal(
        node: &mut crate::embedded::EmbeddedNode,
        bytes: Vec<u8>,
    ) -> Result<focal_model::ContentRef, crate::embedded::NodeError> {
        let domain = focal_model::ContentDomainId(node.identity.ledger.tenant.0);
        let chunk = node.content.upload_chunk_bytes();
        Ok(node.content.seal_import_inline(domain, &bytes, chunk)?)
    }
    /// Poll the session until the record it proposed has applied, a bounded
    /// number of times; what has not applied by then applies under the
    /// next request or tick.
    fn settle(node: &mut crate::embedded::EmbeddedNode) -> Result<(), crate::embedded::NodeError> {
        for _ in 0..crate::native_ingress::COMMIT_POLLS {
            node.session.poll()?;
            if node.session.native_check_retirement().is_ok() {
                break;
            }
        }
        Ok(())
    }
    fn retire_step(
        &mut self,
        node: &mut crate::embedded::EmbeddedNode,
        budget: &focal_memory::MemoryBudget,
    ) -> Result<(), crate::embedded::NodeError> {
        let candidates = node.session.native_core().and_then(|core| {
            core.retirement_candidates(self.cursor, VISITS_PER_TICK)
                .map_err(|error| focal_ledger::LedgerError::Native(error.into()))
        })?;
        self.cursor = candidates.next;
        for root in candidates.claims {
            let Some(archived) =
                crate::archive_derive::archived_family(&node.session, budget, root, self.grace_ms)?
            else {
                continue;
            };
            let crate::fleet::ArchivedFamily {
                bundle,
                through,
                _allocation,
                ..
            } = archived;
            let reference = Self::seal(node, bundle)?;
            drop(_allocation);
            node.session.native_propose_retirement(
                root,
                reference.root,
                reference.length,
                through,
            )?;
            self.proposed = self.proposed.saturating_add(1);
            return Self::settle(node);
        }
        Ok(())
    }
    fn seal_step(
        &mut self,
        node: &mut crate::embedded::EmbeddedNode,
        budget: &focal_memory::MemoryBudget,
    ) -> Result<(), crate::embedded::NodeError> {
        let Some(sealed) = crate::archive_derive::sealed_outcomes(&node.session, budget)? else {
            return Ok(());
        };
        let crate::fleet::SealedOutcomes {
            plan,
            bundle,
            fold,
            _allocation,
            ..
        } = sealed;
        let reference = Self::seal(node, bundle)?;
        let fold = match fold {
            Some((plan, bytes, _)) => {
                let directory = Self::seal(node, bytes)?;
                Some(focal_core::native::seal::Fold {
                    first: plan.first,
                    last: plan.last,
                    bundle: directory.root,
                    bytes: directory.length,
                })
            }
            None => None,
        };
        drop(_allocation);
        node.session
            .native_propose_seal(&plan, reference.root, reference.length, fold)?;
        self.seals_proposed = self.seals_proposed.saturating_add(1);
        Self::settle(node)
    }
}
