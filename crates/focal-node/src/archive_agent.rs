//! The archive agent (26 §4). On every node, for every replica it hosts,
//! the agent offers terminal, released claims to the committed core; where
//! this node is the authority and the retention floor has passed a family's
//! last event, it takes the family's bundle from the core and seals it as
//! content of the ledger under its current placement with a receipt from
//! every required copy. The families a walk finds retire in batches, each one
//! candidate among the authority's writes (doc 31 M2), so retirement keeps
//! pace with completions and never holds a write; a batch that filled asks
//! for the next one at once. A family that is not eligible yet, a copy that
//! has not answered, or a refusal waits for a later walk. The agent holds no
//! state the records do not: a restart resumes its walk from the status index.
use crate::fleet::ReplicaHost;
use crate::network_service::NetworkHandles;
use focal_client::admin::AdminArchiveAgent;
use focal_core::native::retirement::RetirementCursor;
use focal_core::native::retirement_batch::{MAX_RETIRE_FAMILIES, RetireOffer};
use focal_model::LedgerId;
use focal_rt::watch;
use focal_wire::AccessError;
use std::collections::BTreeMap;
use std::time::Duration;

/// Milliseconds between the agent's ticks; the default is five seconds.
pub const INTERVAL_ENV: &str = "FOCAL_RETIRE_INTERVAL_MS";
const DEFAULT_INTERVAL: Duration = Duration::from_secs(5);
/// Milliseconds of logical time a family's last event must have settled
/// for before it is offered: the grace a finished claim stays readable in
/// the core for. The default is one day.
pub const GRACE_ENV: &str = "FOCAL_RETIRE_AFTER_MS";
const DEFAULT_GRACE_MS: u64 = 24 * 60 * 60 * 1000;
/// How soon the agent asks again for the seals of a replica whose seal is
/// held, in flight or was just proposed (F12): between ticks, so sealing
/// keeps pace with a window that fills faster than a bundle an interval.
const BACKLOG_INTERVAL: Duration = Duration::from_millis(20);
/// Status-index rows one tick visits per replica.
const VISITS_PER_TICK: usize = 1024;
/// Status-index rows one cold walk visits (doc 31 M2, A2): it runs on the replica's own thread,
/// so it takes at most a batch of offers, a fraction of a millisecond of derivations and one
/// retirement's candidates, before the writes queued behind it.
const COLD_VISITS: usize = MAX_RETIRE_FAMILIES;
/// How soon a walk that found nothing goes on through a pass with further to go: at a scan's
/// pace, a walk's rows every this often, rather than at once or at the interval.
const SCAN_INTERVAL: Duration = Duration::from_millis(100);
/// Replicas the agent keeps a walk for.
const MAX_CURSORS: usize = 4096;

/// What one walk of a replica's families did (doc 31 M2): how many it retired, how many it
/// found eligible and deferred (a pending candidate changes their rows, or no candidate had room
/// for them), whether a batch of offers filled, and whether the walk stopped short of the
/// index's end.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Walked {
    pub(crate) retired: usize,
    pub(crate) deferred: usize,
    pub(crate) full: bool,
    pub(crate) partial: bool,
}
impl Walked {
    fn of(walk: &crate::archive_derive::ColdWalk) -> Self {
        Self {
            retired: walk.retired,
            deferred: walk.deferred,
            full: walk.full,
            partial: walk.next.is_some(),
        }
    }
}

/// When to walk a replica's families again after `walked`, `since` after the walk before it:
/// at once ([`BACKLOG_INTERVAL`]) when it retired some and a batch filled or it stopped short,
/// since more are waiting; otherwise once a batch's worth is likely to be ready at the rate it
/// found them (those it retired, else those it had to defer), so batches stay full where
/// families arrive fast, records stay few where they arrive slowly, and a walk whose families
/// all wait on a pending candidate neither spins nor sleeps the interval. A walk that found
/// none goes on through a pass with further to go at [`SCAN_INTERVAL`], and rests the interval
/// once the pass is done. Never sooner than [`BACKLOG_INTERVAL`] nor later than the interval.
pub(crate) fn retire_pace(walked: Walked, since: Duration, interval: Duration) -> Duration {
    let interval = interval.max(BACKLOG_INTERVAL);
    if walked.retired > 0 && (walked.full || walked.partial) {
        return BACKLOG_INTERVAL;
    }
    let found = if walked.retired > 0 {
        walked.retired
    } else {
        walked.deferred
    };
    let Some(per_family) = Some(u32::try_from(found).unwrap_or(u32::MAX))
        .filter(|found| *found > 0)
        .and_then(|found| since.checked_div(found))
    else {
        // Nothing found: on through a pass with further to go at a scan's pace, else rest.
        return if walked.partial {
            SCAN_INTERVAL.clamp(BACKLOG_INTERVAL, interval)
        } else {
            interval
        };
    };
    per_family
        .saturating_mul(u32::try_from(MAX_RETIRE_FAMILIES).unwrap_or(u32::MAX))
        .clamp(BACKLOG_INTERVAL, interval)
}

/// A replica's walk: where it resumes, when it last ran and when it runs next.
#[derive(Debug, Clone, Copy)]
struct Walk {
    cursor: Option<RetirementCursor>,
    at: std::time::Instant,
    due: std::time::Instant,
    /// The replica's retire rule and when it was read: it changes only with the upgrade fence,
    /// so it is read again once an interval rather than every walk.
    rule: Option<(focal_ledger::RetireRule, std::time::Instant)>,
    /// The tick that last saw the replica hosted here: a walk not seen by a tick is let go.
    seen: u64,
}

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
    walks: BTreeMap<LedgerId, Walk>,
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
    /// Refused at the runtime's cell bound (its status is a watched value).
    pub fn new(
        interval: Duration,
        grace_ms: u64,
    ) -> Result<(Self, ArchiveHandle), focal_rt::RtError> {
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
        })?;
        Ok((
            Self {
                interval,
                grace_ms,
                walks: BTreeMap::new(),
                ticks: 0,
                proposed: 0,
                waiting: 0,
                seals_proposed: 0,
                seals_waiting: 0,
                status,
            },
            ArchiveHandle(receiver),
        ))
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
        self.status.send(status);
    }
    /// The interval and grace from the environment, else the defaults.
    pub fn from_env() -> Result<(Self, ArchiveHandle), focal_rt::RtError> {
        let (interval, grace_ms) = settings_from_env();
        Self::new(interval, grace_ms)
    }
    /// Ticks at the interval; while a seal is held, in flight or was just
    /// proposed on a replica (F12), its seals are asked again every
    /// [`BACKLOG_INTERVAL`] between ticks, so a window that seals faster
    /// than one bundle an interval is sealed as fast as custody and its
    /// commits go. Each replica's families are walked at their own pace
    /// ([`retire_pace`]): a tick comes as soon as one is due.
    pub async fn run(mut self, handles: &NetworkHandles) -> Result<(), AccessError> {
        if hyper_rt::futures::shard_id().is_none() {
            return Err(AccessError::Unavailable);
        }
        let mut ticked = std::time::Instant::now();
        let mut backlog = Backlog::default();
        loop {
            let mut wait = self.interval.saturating_sub(ticked.elapsed());
            if backlog.seals {
                wait = wait.min(BACKLOG_INTERVAL);
            }
            if let Some(due) = backlog.retire_at {
                wait = wait.min(due.saturating_duration_since(std::time::Instant::now()));
            }
            focal_rt::time::sleep(wait)
                .await
                .map_err(|_| AccessError::Unavailable)?;
            let retire_due = backlog
                .retire_at
                .is_some_and(|due| due <= std::time::Instant::now());
            let interval_due = ticked.elapsed() >= self.interval;
            if interval_due || retire_due {
                // Seals at their own cadence: each interval, or while one is in hand.
                let sealing = interval_due || backlog.seals;
                let seals_held = backlog.seals;
                backlog = self.tick(handles, sealing).await;
                if !sealing {
                    backlog.seals = seals_held;
                }
                if interval_due {
                    ticked = std::time::Instant::now();
                }
            } else {
                backlog.seals = self.seal_round(handles).await;
            }
        }
    }
    /// One pass over the replicas this node hosts: each replica's families walked if its walk is
    /// due, and its seals when `seals`; whether a seal is held, in flight or was just proposed on
    /// any (when asked), and when the soonest walk is due next. A walk of a replica no longer
    /// hosted is let go.
    pub async fn tick(&mut self, handles: &NetworkHandles, seals: bool) -> Backlog {
        let mut after = None;
        let mut backlog = Backlog::default();
        self.ticks = self.ticks.saturating_add(1);
        while let Some((ledger, host)) = handles.fleet.next_host(after) {
            after = Some(ledger);
            let now = std::time::Instant::now();
            let walk = self.walk(ledger, now);
            if walk.due <= now {
                // A replica that is not native, not authoritative, or refuses
                // the walk simply waits for its next.
                let walked = self
                    .step(ledger, &host, handles, walk.cursor)
                    .await
                    .unwrap_or_default();
                let pace = retire_pace(
                    walked.0,
                    now.saturating_duration_since(walk.at),
                    self.interval,
                );
                if let Some(next) = self.walks.get_mut(&ledger) {
                    next.cursor = walked.1;
                    next.at = now;
                    next.due = now.checked_add(pace).unwrap_or(now);
                }
            }
            if let Some(due) = self.walks.get(&ledger).map(|walk| walk.due) {
                backlog.retire_at = Some(backlog.retire_at.map_or(due, |soonest| soonest.min(due)));
            }
            if seals {
                backlog.seals |= self
                    .seal_step(ledger, &host, handles)
                    .await
                    .unwrap_or(false);
            }
        }
        let tick = self.ticks;
        self.walks.retain(|_, walk| walk.seen == tick);
        self.publish();
        backlog
    }
    /// The replica's walk, seen by this tick: a new one due at once. At the bound on walks held,
    /// every walk is let go and begins again.
    fn walk(&mut self, ledger: LedgerId, now: std::time::Instant) -> Walk {
        if self.walks.len() >= MAX_CURSORS && !self.walks.contains_key(&ledger) {
            self.walks.clear();
        }
        let interval = self.interval;
        let walk = self.walks.entry(ledger).or_insert(Walk {
            cursor: None,
            at: now.checked_sub(interval).unwrap_or(now),
            due: now,
            rule: None,
            seen: self.ticks,
        });
        walk.seen = self.ticks;
        *walk
    }
    /// The seals alone, between ticks while one is held or in flight.
    async fn seal_round(&mut self, handles: &NetworkHandles) -> bool {
        let mut after = None;
        let mut backlog = false;
        while let Some((ledger, host)) = handles.fleet.next_host(after) {
            after = Some(ledger);
            backlog |= self
                .seal_step(ledger, &host, handles)
                .await
                .unwrap_or(false);
        }
        self.publish();
        backlog
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
    ) -> Result<bool, AccessError> {
        let sealed = match host
            .seal_bundle()
            .await
            .map_err(|_| AccessError::Unavailable)?
        {
            crate::archive_derive::SealOffer::Ready(sealed) => sealed,
            crate::archive_derive::SealOffer::Busy => return Ok(true),
            crate::archive_derive::SealOffer::Nothing => return Ok(false),
        };
        let crate::fleet::SealedOutcomes {
            plan,
            bundle,
            fold,
            _allocation,
            ..
        } = *sealed;
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
            return Ok(false);
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
        Ok(true)
    }
    /// One walk of the replica's status index from `cursor`: every eligible family it finds
    /// retired, a batch at a time, each batch one candidate among the authority's writes (doc 31
    /// M2), sealed under custody first below the cold rule. What the walk did, and where the
    /// next resumes.
    async fn step(
        &mut self,
        ledger: LedgerId,
        host: &ReplicaHost,
        handles: &NetworkHandles,
        cursor: Option<RetirementCursor>,
    ) -> Result<(Walked, Option<RetirementCursor>), AccessError> {
        let now = std::time::Instant::now();
        let cached = self
            .walks
            .get(&ledger)
            .and_then(|walk| walk.rule)
            .filter(|(_, read)| now.saturating_duration_since(*read) < self.interval);
        let rule = match cached {
            Some((rule, _)) => rule,
            None => {
                let rule = host
                    .retire_rule()
                    .await
                    .map_err(|_| AccessError::Unavailable)?;
                if let Some(walk) = self.walks.get_mut(&ledger) {
                    walk.rule = Some((rule, now));
                }
                rule
            }
        };
        if rule == focal_ledger::RetireRule::Cold {
            // Retirement to every replica's cold stream (doc 31 M2, A2): nothing is sealed, so
            // the replica walks, offers and retires on its own thread, one call a walk.
            let walk = host
                .cold_walk(cursor, self.grace_ms, COLD_VISITS)
                .await
                .map_err(|_| AccessError::Unavailable)?;
            self.note_walk(&walk);
            return Ok((Walked::of(&walk), walk.next));
        }
        let candidates = host
            .retirement_candidates(cursor, VISITS_PER_TICK)
            .await
            .map_err(|_| AccessError::Unavailable)?;
        let next = candidates.next;
        let mut deferred = 0usize;
        let before = self.proposed;
        let mut offers = Vec::new();
        offers
            .try_reserve_exact(MAX_RETIRE_FAMILIES)
            .map_err(|_| AccessError::Capacity)?;
        let mut full = false;
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
                family,
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
            let exact = rule == focal_ledger::RetireRule::Exact;
            if !outcome.obligation.satisfied() {
                // A required copy has not answered with a receipt; the
                // bundle is sealed and the next walk asks again.
                self.waiting = self.waiting.saturating_add(1);
                if exact {
                    return Ok((Walked::default(), next));
                }
                continue;
            }
            if exact {
                // Below the upgrade fence a family retires alone, at the prefix it was derived
                // at (`FOCALRT1`): a binary there cannot read a batch.
                host.propose_retirement(
                    root,
                    outcome.reference.root,
                    outcome.reference.length,
                    through,
                )
                .await
                .map_err(|_| AccessError::Unavailable)?;
                self.proposed = self.proposed.saturating_add(1);
                return Ok((Walked::default(), next));
            }
            // Within the capacity reserved above: a full batch is retired
            // before another offer is taken.
            offers.push(RetireOffer {
                root,
                family,
                bundle: outcome.reference.root,
                bytes: outcome.reference.length,
            });
            if offers.len() == MAX_RETIRE_FAMILIES {
                full = true;
                deferred = deferred.saturating_add(self.retire_batch(host, &mut offers).await?);
            }
        }
        if !offers.is_empty() {
            deferred = deferred.saturating_add(self.retire_batch(host, &mut offers).await?);
        }
        let walked = Walked {
            retired: usize::try_from(self.proposed.saturating_sub(before)).unwrap_or(usize::MAX),
            deferred,
            full,
            partial: next.is_some(),
        };
        Ok((walked, next))
    }
    /// Count what a cold walk retired and left waiting.
    fn note_walk(&mut self, walk: &crate::archive_derive::ColdWalk) {
        self.proposed = self
            .proposed
            .saturating_add(u64::try_from(walk.retired).unwrap_or(u64::MAX));
        self.waiting = self.waiting.saturating_add(
            u64::try_from(walk.deferred.saturating_add(walk.refused)).unwrap_or(u64::MAX),
        );
    }
    /// Retire `offers`, leaving them empty for the next: how many were deferred.
    async fn retire_batch(
        &mut self,
        host: &ReplicaHost,
        offers: &mut Vec<RetireOffer>,
    ) -> Result<usize, AccessError> {
        let batch = host
            .retire(std::mem::take(offers))
            .await
            .map_err(|_| AccessError::Unavailable)?;
        offers
            .try_reserve_exact(MAX_RETIRE_FAMILIES)
            .map_err(|_| AccessError::Capacity)?;
        self.proposed = self
            .proposed
            .saturating_add(u64::try_from(batch.families.len()).unwrap_or(u64::MAX));
        self.waiting = self.waiting.saturating_add(
            u64::try_from(batch.deferred.len().saturating_add(batch.refused.len()))
                .unwrap_or(u64::MAX),
        );
        Ok(batch.deferred.len())
    }
}

/// What a pass of the agent left to do before its next interval.
#[derive(Debug, Clone, Copy, Default)]
pub struct Backlog {
    /// A seal is held, in flight or was just proposed (F12).
    pub seals: bool,
    /// When the soonest replica's families are due to be walked (doc 31 M2).
    pub retire_at: Option<std::time::Instant>,
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
    /// When the families are walked next, at their own pace ([`retire_pace`]), and when they
    /// last were.
    retire_due: std::time::Instant,
    walked_at: std::time::Instant,
    cursor: Option<focal_core::native::retirement::RetirementCursor>,
    pub(crate) proposed: u64,
    pub(crate) seals_proposed: u64,
    pub(crate) deferred: u64,
}
impl EmbeddedArchive {
    pub(crate) fn from_env() -> Self {
        let (interval, grace_ms) = settings_from_env();
        let interval = interval.max(Duration::from_millis(10));
        let now = std::time::Instant::now();
        Self {
            interval,
            grace_ms,
            next: now,
            retire_due: now,
            walked_at: now.checked_sub(interval).unwrap_or(now),
            cursor: None,
            proposed: 0,
            seals_proposed: 0,
            deferred: 0,
        }
    }
    /// When the families are next due a walk: the owner's maintenance comes no later.
    pub(crate) fn due(&self) -> std::time::Instant {
        self.retire_due
    }
    /// The families walked once their walk is due ([`retire_pace`]), and the seals once the
    /// interval has passed since the last.
    pub(crate) fn maintain(
        &mut self,
        node: &mut crate::embedded::EmbeddedNode,
        budget: &focal_memory::MemoryBudget,
    ) -> Result<(), crate::embedded::NodeError> {
        let now = std::time::Instant::now();
        let retire = now >= self.retire_due;
        let seal = now >= self.next;
        if !retire && !seal {
            return Ok(());
        }
        let later = |pace: Duration| {
            now.checked_add(pace).ok_or_else(|| {
                crate::embedded::NodeError::Domain(
                    "the archive interval overflows the clock".into(),
                )
            })
        };
        if seal {
            self.next = later(self.interval)?;
        }
        if retire {
            self.retire_due = later(self.interval)?;
        }
        if node.session.native_core().is_err() {
            return Ok(());
        }
        if retire {
            let walked = self.retire_step(node, budget).unwrap_or_else(|_| {
                self.deferred = self.deferred.saturating_add(1);
                Walked::default()
            });
            let pace = retire_pace(
                walked,
                now.saturating_duration_since(self.walked_at),
                self.interval,
            );
            self.walked_at = now;
            self.retire_due = later(pace)?;
        }
        if seal && self.seal_step(node, budget).is_err() {
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
    /// One walk of the status index: every eligible family it finds retired, a batch at a time,
    /// each batch one candidate among the session's writes (doc 31 M2), polled to commitment,
    /// sealed into the node's own store first below the cold rule. What the walk did.
    fn retire_step(
        &mut self,
        node: &mut crate::embedded::EmbeddedNode,
        budget: &focal_memory::MemoryBudget,
    ) -> Result<Walked, crate::embedded::NodeError> {
        let rule = node.session.native_retire_rule()?;
        if rule == focal_ledger::RetireRule::Cold {
            // To the node's own cold stream (doc 31 M2, A2): walked, offered and retired at once,
            // nothing sealed, and polled to commitment.
            let walk = crate::archive_derive::cold_walk(
                &mut node.session,
                self.cursor,
                self.grace_ms,
                COLD_VISITS,
            )?;
            self.cursor = walk.next;
            self.proposed = self
                .proposed
                .saturating_add(u64::try_from(walk.retired).unwrap_or(u64::MAX));
            if walk.submitted {
                Self::settle(node)?;
            }
            return Ok(Walked::of(&walk));
        }
        let candidates = node.session.native_core().and_then(|core| {
            core.retirement_candidates(self.cursor, VISITS_PER_TICK)
                .map_err(|error| focal_ledger::LedgerError::Native(error.into()))
        })?;
        self.cursor = candidates.next;
        let before = self.proposed;
        let mut full = false;
        let mut deferred = 0usize;
        let exact = rule == focal_ledger::RetireRule::Exact;
        let mut offers = Vec::new();
        offers
            .try_reserve_exact(MAX_RETIRE_FAMILIES)
            .map_err(|_| crate::embedded::NodeError::Domain("retirement offers".into()))?;
        for root in candidates.claims {
            let Some(archived) =
                crate::archive_derive::archived_family(&node.session, budget, root, self.grace_ms)?
            else {
                continue;
            };
            let crate::fleet::ArchivedFamily {
                bundle,
                family,
                through,
                _allocation,
                ..
            } = archived;
            let reference = Self::seal(node, bundle)?;
            drop(_allocation);
            if exact {
                // Below the level it announces a family retires alone, at the prefix it was
                // derived at (`FOCALRT1`).
                node.session.native_propose_retirement(
                    root,
                    reference.root,
                    reference.length,
                    through,
                )?;
                self.proposed = self.proposed.saturating_add(1);
                Self::settle(node)?;
                return Ok(Walked::default());
            }
            // Within the capacity reserved above: a full batch is retired
            // before another offer is taken.
            offers.push(RetireOffer {
                root,
                family,
                bundle: reference.root,
                bytes: reference.length,
            });
            if offers.len() == MAX_RETIRE_FAMILIES {
                full = true;
                deferred = deferred.saturating_add(self.retire_batch(node, &mut offers)?);
            }
        }
        if !offers.is_empty() {
            deferred = deferred.saturating_add(self.retire_batch(node, &mut offers)?);
        }
        Ok(Walked {
            retired: usize::try_from(self.proposed.saturating_sub(before)).unwrap_or(usize::MAX),
            deferred,
            full,
            partial: self.cursor.is_some(),
        })
    }
    /// Retire `offers` and poll them to commitment, leaving them empty for the next: how many
    /// were deferred.
    fn retire_batch(
        &mut self,
        node: &mut crate::embedded::EmbeddedNode,
        offers: &mut Vec<RetireOffer>,
    ) -> Result<usize, crate::embedded::NodeError> {
        let (batch, submission) = node.session.native_retire(offers)?;
        offers.clear();
        self.proposed = self
            .proposed
            .saturating_add(u64::try_from(batch.families.len()).unwrap_or(u64::MAX));
        if submission.is_some() {
            Self::settle(node)?;
        }
        Ok(batch.deferred.len())
    }
    fn seal_step(
        &mut self,
        node: &mut crate::embedded::EmbeddedNode,
        budget: &focal_memory::MemoryBudget,
    ) -> Result<(), crate::embedded::NodeError> {
        let crate::archive_derive::SealOffer::Ready(sealed) =
            crate::archive_derive::sealed_outcomes(&node.session, budget)?
        else {
            return Ok(());
        };
        let crate::fleet::SealedOutcomes {
            plan,
            bundle,
            fold,
            _allocation,
            ..
        } = *sealed;
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

#[cfg(test)]
#[path = "archive_agent_tests.rs"]
mod tests;
