//! The rounds of the metrics sampler (the audit's F26): what one page holds,
//! which entities of each family a round lists, and the counts the node's
//! sessions made, kept across rounds so that a node-level counter never
//! falls as sessions come and go.
//!
//! A page lists every family's aggregates and, within what is left of it,
//! entities one by one: each family's flagged entities first, then the next
//! of the rest after where the family's last round stopped. What one entity
//! of a family costs at most is measured by rendering, so the choice and the
//! renderer cannot drift: a page is never longer than `MAX_PAGE_BYTES`.
use super::{
    AgentMetrics, CredentialMetrics, Listings, LivenessMetrics, MAX_PAGE_BYTES, MetricLabels,
    MetricsSnapshot, PeerRtt, RootMetrics, RootPeerAggregates, RttAggregates, SessionAggregates,
    SessionCounters, SessionMetrics, TenantAggregates,
};
use crate::{
    admission::{AdmissionReport, TenantReport},
    fleet::{FleetStatus, ReplicaHost},
};
use focal_client::admin::AdminRetention;
use focal_consensus::PeerProgress;
use focal_memory::{
    ALLOCATOR_OVERHEAD, Allocation, BudgetKind, BudgetLane, BudgetStats, DISK_KIND_COUNT,
    DiskStats, MemoryBudget, MemoryError,
};
use focal_model::{LedgerId, TenantId};
use std::collections::BTreeMap;

/// The widest value a series takes: `u64::MAX` and `i64::MIN` are twenty
/// characters.
const VALUE_WIDTH: usize = 20;

/// What `text` costs at most with every value at its widest: its comment
/// lines as they are, each sample line with its value counted at
/// `VALUE_WIDTH`. A bound that holds whatever values a later round renders.
fn widest_bytes(text: &str) -> Option<usize> {
    text.lines().try_fold(0usize, |sum, line| {
        let bytes = if line.starts_with('#') {
            line.len()
        } else {
            let (head, _) = line.rsplit_once(' ')?;
            head.len().checked_add(1)?.checked_add(VALUE_WIDTH)?
        };
        sum.checked_add(bytes)?.checked_add(1)
    })
}

/// The families a page lists entity by entity, in this order in every
/// array of four below.
pub const FAMILIES: [&str; 4] = ["sessions", "root_peers", "peer_rtts", "tenants"];

/// What a page costs: its fixed part, and one entity of each family, each at
/// its widest (`widest_bytes` of the renderer's own text).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageBudget {
    pub fixed: usize,
    pub entity: [usize; 4],
}
impl PageBudget {
    /// The budget of the pages of a node labelled `labels`: `None` when its
    /// fixed part and one entity of each family exceed one page, so that no
    /// round could list each family.
    pub fn derive(labels: &MetricLabels, memory: BudgetStats) -> Option<Self> {
        let mut widest = MetricsSnapshot::widest(labels, memory);
        let fixed = widest_bytes(&widest.render())?;
        let cost = |widest: &MetricsSnapshot| widest_bytes(&widest.render())?.checked_sub(fixed);
        widest.sessions.push(SessionMetrics::widest());
        let sessions = cost(&widest)?;
        widest.sessions.clear();
        widest.root.peers.push(widest_root_peer());
        let root_peers = cost(&widest)?;
        widest.root.peers.clear();
        widest.peer_rtts.push(PeerRtt {
            peer: u64::MAX,
            rtt_ms: u64::MAX,
        });
        let peer_rtts = cost(&widest)?;
        widest.peer_rtts.clear();
        widest
            .agent
            .as_mut()?
            .admission
            .tenants
            .push(widest_tenant());
        let tenants = cost(&widest)?;
        let entity = [sessions, root_peers, peer_rtts, tenants];
        let one_each = entity
            .iter()
            .try_fold(fixed, |sum, bytes| sum.checked_add(*bytes))?;
        (one_each <= MAX_PAGE_BYTES && entity.iter().all(|bytes| *bytes > 0))
            .then_some(Self { fixed, entity })
    }
    /// How many entities of each family a round lists, of `counts`: every
    /// one while the page holds them all. Otherwise one of each family that
    /// has any, and what the page holds beyond them shared in proportion to
    /// what each family's remaining entities cost, so that every family is
    /// listed at the same fraction a round. The fixed part and the listed
    /// entities, each at its widest, stay within one page.
    pub fn capacities(&self, counts: [usize; 4]) -> [usize; 4] {
        let mut left = MAX_PAGE_BYTES.saturating_sub(self.fixed);
        let whole = counts
            .iter()
            .zip(self.entity)
            .try_fold(0usize, |sum, (count, bytes)| {
                sum.checked_add(count.checked_mul(bytes)?)
            });
        if whole.is_some_and(|whole| whole <= left) {
            return counts;
        }
        let mut listed = [0usize; 4];
        for ((listed, count), bytes) in listed.iter_mut().zip(counts).zip(self.entity) {
            if count > 0 && bytes <= left {
                *listed = 1;
                left = left.saturating_sub(bytes);
            }
        }
        let wide = |value: usize| u128::try_from(value).unwrap_or(u128::MAX);
        let mut rest = [0u128; 4];
        for (((rest, count), listed), bytes) in
            rest.iter_mut().zip(counts).zip(listed).zip(self.entity)
        {
            *rest = wide(count.saturating_sub(listed)).saturating_mul(wide(bytes));
        }
        let total = rest
            .iter()
            .fold(0u128, |sum, rest| sum.saturating_add(*rest));
        if total == 0 {
            return listed;
        }
        for (((listed, count), rest), bytes) in
            listed.iter_mut().zip(counts).zip(rest).zip(self.entity)
        {
            let share = wide(left)
                .saturating_mul(rest)
                .checked_div(total)
                .unwrap_or(0);
            let more = share.checked_div(wide(bytes)).unwrap_or(0);
            let more = usize::try_from(more).unwrap_or(usize::MAX);
            *listed = listed.saturating_add(more.min(count.saturating_sub(*listed)));
        }
        listed
    }
}

/// Which of a family's entities a round lists: the flagged first, then the
/// rest, each pass in order from just after `cursor` and wrapping, up to
/// `capacity`. `entities` are in key order, and so are the chosen keys.
/// Returns them and the key the family's next round starts after: the last
/// entity listed that was not flagged, or, when none fitted, the last
/// flagged one, so that the flagged rotate among themselves while they
/// outnumber the page.
pub fn choose<K: Ord + Copy>(
    entities: &[(K, bool)],
    cursor: Option<K>,
    capacity: usize,
) -> Result<(Vec<K>, Option<K>), MemoryError> {
    let mut chosen = Vec::new();
    chosen
        .try_reserve_exact(capacity.min(entities.len()))
        .map_err(|_| MemoryError::AllocationFailed)?;
    let start = entities.partition_point(|(key, _)| cursor.is_some_and(|cursor| *key <= cursor));
    let after = entities.get(start..).unwrap_or_default();
    let before = entities.get(..start).unwrap_or_default();
    let mut last_flagged = None;
    let mut last_other = None;
    for flagged in [true, false] {
        for (key, is_flagged) in after.iter().chain(before) {
            if chosen.len() >= capacity {
                break;
            }
            if *is_flagged == flagged {
                chosen.push(*key);
                if flagged {
                    last_flagged = Some(*key);
                } else {
                    last_other = Some(*key);
                }
            }
        }
    }
    chosen.sort_unstable();
    Ok((chosen, last_other.or(last_flagged).or(cursor)))
}

/// What one entry of a `BTreeMap<K, V>` holds at most: std's nodes hold
/// eleven entries and are kept at least five full, so an entry's share of
/// its node is at most eleven fifths of an entry, and of the node's twelve
/// links and its allocation a fifth: three entries' bytes and one
/// allocation's bookkeeping bound it.
pub fn map_entry_bytes<K, V>() -> Option<usize> {
    size_of::<K>()
        .checked_add(size_of::<V>())?
        .checked_mul(3)?
        .checked_add(ALLOCATOR_OVERHEAD)
}

/// A hosted session's last round: its installation's sequence, its counts
/// then, and whether that round asked its owner and was not answered.
#[derive(Debug, Clone, Copy)]
struct Last {
    incarnation: u64,
    counters: SessionCounters,
    unanswered: bool,
}

/// What a round's pass over every hosted session found (`Rounds::survey`):
/// the aggregates, and each session in ledger order with whether it is
/// flagged, charged while it is held.
pub struct Survey {
    pub aggregates: SessionAggregates,
    pub flags: Vec<(LedgerId, bool)>,
    _charge: Allocation,
}
/// Where each family's next round starts.
#[derive(Debug, Default, Clone, Copy)]
struct Cursors {
    sessions: Option<LedgerId>,
    root_peers: Option<u64>,
    peer_rtts: Option<u64>,
    tenants: Option<TenantId>,
}

/// The sampler's memory across rounds (the audit's F26).
pub struct Rounds {
    budget: PageBudget,
    memory: MemoryBudget,
    rounds: u64,
    cursors: Cursors,
    /// Each hosted session's last round: as many entries as the hosted
    /// set, pruned as sessions leave it, and charged.
    last: BTreeMap<LedgerId, Last>,
    charge: Option<Allocation>,
    /// What sessions that left, or started over, had counted.
    retired: SessionCounters,
    /// Rounds that published no page: refused their room.
    refused: u64,
}
impl Rounds {
    /// The rounds of a node labelled `labels`, charging what they keep to
    /// `memory`; `None` when no page could list each family
    /// (`PageBudget::derive`).
    pub fn new(labels: &MetricLabels, memory: MemoryBudget) -> Option<Self> {
        let budget = PageBudget::derive(labels, memory.stats())?;
        Some(Self {
            budget,
            memory,
            rounds: 0,
            cursors: Cursors::default(),
            last: BTreeMap::new(),
            charge: None,
            retired: SessionCounters::default(),
            refused: 0,
        })
    }
    /// Counts a round that published no page.
    pub fn refused(&mut self) {
        self.refused = self.refused.saturating_add(1);
    }
    pub fn refused_rounds(&self) -> u64 {
        self.refused
    }
    /// How many sessions' last rounds are kept, and the bytes charged for
    /// them: the hosted set's, and no more.
    pub fn kept(&self) -> (usize, usize) {
        (
            self.last.len(),
            self.charge.as_ref().map_or(0, Allocation::bytes),
        )
    }
    pub fn budget(&self) -> PageBudget {
        self.budget
    }
    /// Begins a round and returns how many have begun.
    pub fn begin(&mut self) -> u64 {
        self.rounds = self.rounds.saturating_add(1);
        self.rounds
    }
    /// Whether `ledger`'s history flags it: its owner was asked last round
    /// and did not answer, or it was refused periods since its last round.
    pub fn flagged(&self, ledger: LedgerId, counters: &SessionCounters) -> bool {
        self.last.get(&ledger).is_some_and(|last| {
            last.unanswered || counters.refused_periods > last.counters.refused_periods
        })
    }
    /// Accounts the round's hosted sessions, given in ledger order with each
    /// one's installation's sequence and counts: each one's counts are kept as its last,
    /// and those of a session gone or installed again are retired into the
    /// node's. Returns every count the node's sessions made. Refused
    /// (`Capacity`) when the room for the hosted set is: the round then
    /// keeps the last counts it had.
    pub fn account(
        &mut self,
        hosted: &[(LedgerId, u64, SessionCounters)],
    ) -> Result<SessionCounters, MemoryError> {
        let bytes = map_entry_bytes::<LedgerId, Last>()
            .and_then(|entry| entry.checked_mul(hosted.len()))
            .ok_or(MemoryError::Capacity {
                requested: usize::MAX,
                available: 0,
            })?;
        self.charge_to(bytes)?;
        let mut retired = self.retired;
        self.last.retain(|ledger, last| {
            let kept = hosted
                .binary_search_by_key(ledger, |(key, _, _)| *key)
                .is_ok();
            if !kept {
                retired.add(&last.counters);
            }
            kept
        });
        for (ledger, incarnation, counters) in hosted {
            match self.last.get_mut(ledger) {
                Some(last) => {
                    // Installed again, or started over: what it counted
                    // before is the node's still.
                    if last.incarnation != *incarnation || counters.restarted_since(&last.counters)
                    {
                        retired.add(&last.counters);
                    }
                    last.incarnation = *incarnation;
                    last.counters = *counters;
                }
                None => {
                    self.last.insert(
                        *ledger,
                        Last {
                            incarnation: *incarnation,
                            counters: *counters,
                            unanswered: false,
                        },
                    );
                }
            }
        }
        self.retired = retired;
        let mut totals = retired;
        for last in self.last.values() {
            totals.add(&last.counters);
        }
        Ok(totals)
    }
    /// The round's pass over every hosted session, read in place with no
    /// ask of its owner (the audit's F26). `visit` hands over each hosted
    /// replica with its installation's sequence
    /// (`FleetManager::visit_hosted`); `bound` is how many the round has
    /// room for, the hosted set as the fleet last reported it: one installed
    /// since is counted in the aggregates and listed from the next round. A
    /// session is flagged when its replica stopped, knows no leader, is
    /// stretched, or waits for a seed's chunks, a delivery's content or an
    /// import's sealing, or when its history flags it (`flagged`). What every
    /// session counted is kept (`account`).
    pub fn survey(
        &mut self,
        visit: impl FnOnce(&mut dyn FnMut(LedgerId, u64, &ReplicaHost)),
        bound: usize,
        local: u64,
    ) -> Result<Survey, MemoryError> {
        let refused = MemoryError::Capacity {
            requested: usize::MAX,
            available: 0,
        };
        let row = size_of::<(LedgerId, u64, SessionCounters)>()
            .checked_add(size_of::<(LedgerId, bool)>())
            .ok_or(refused.clone())?;
        let bytes = row
            .checked_mul(bound)
            .and_then(|bytes| bytes.checked_add(ALLOCATOR_OVERHEAD.checked_mul(2)?))
            .ok_or(refused)?;
        let charge = self
            .memory
            .reserve(BudgetKind::Control, BudgetLane::Ordinary, bytes)?
            .commit();
        let mut hosted: Vec<(LedgerId, u64, SessionCounters)> = Vec::new();
        hosted
            .try_reserve_exact(bound)
            .map_err(|_| MemoryError::AllocationFailed)?;
        let mut flags: Vec<(LedgerId, bool)> = Vec::new();
        flags
            .try_reserve_exact(bound)
            .map_err(|_| MemoryError::AllocationFailed)?;
        let mut aggregates = SessionAggregates::default();
        visit(&mut |ledger, incarnation, host| {
            let up = |value: &mut u64, holds: bool| {
                if holds {
                    *value = value.saturating_add(1);
                }
            };
            up(&mut aggregates.hosted, true);
            if hosted.len() >= bound {
                return;
            }
            let stretched = host.stretched();
            let refused = host.refused_periods();
            let longest = u64::try_from(host.longest_period().as_millis()).unwrap_or(u64::MAX);
            aggregates.longest_period_ms = aggregates.longest_period_ms.max(longest);
            let (counters, flagged) = host.observe(|progress| {
                let seeding = progress.seed_pending.is_some();
                let custody = progress.custody_pending.is_some();
                let importing = progress.import_pending.is_some();
                // A stopped replica leads nothing and waits for no leader:
                // the leader it last knew is not the session's.
                let running = !progress.stopped;
                up(&mut aggregates.stopped, progress.stopped);
                up(
                    &mut aggregates.leading,
                    running && local != 0 && progress.leader == local,
                );
                up(&mut aggregates.leaderless, running && progress.leader == 0);
                up(&mut aggregates.stretched, stretched);
                up(&mut aggregates.seeding, seeding);
                up(&mut aggregates.custody_pending, custody);
                up(&mut aggregates.importing, importing);
                (
                    SessionCounters::of(progress, refused),
                    progress.stopped
                        || progress.leader == 0
                        || stretched
                        || seeding
                        || custody
                        || importing,
                )
            });
            hosted.push((ledger, incarnation, counters));
            flags.push((ledger, flagged));
        });
        for ((ledger, flagged), (_, _, counters)) in flags.iter_mut().zip(&hosted) {
            *flagged = *flagged || self.flagged(*ledger, counters);
        }
        aggregates.totals = self.account(&hosted)?;
        Ok(Survey {
            aggregates,
            flags,
            _charge: charge,
        })
    }
    /// Notes which sessions this round asked and whether each answered:
    /// one that did not is flagged next round.
    pub fn answered(&mut self, asked: &[(LedgerId, bool)]) {
        for last in self.last.values_mut() {
            last.unanswered = false;
        }
        for (ledger, answered) in asked {
            if let Some(last) = self.last.get_mut(ledger) {
                last.unanswered = !answered;
            }
        }
    }
    /// The sessions this round lists, of `hosted` (in ledger order, each with
    /// whether it is flagged).
    pub fn choose_sessions(
        &mut self,
        hosted: &[(LedgerId, bool)],
        capacity: usize,
    ) -> Result<Vec<LedgerId>, MemoryError> {
        let (chosen, cursor) = choose(hosted, self.cursors.sessions, capacity)?;
        self.cursors.sessions = cursor;
        Ok(chosen)
    }
    /// The root members this round lists, of `members` (in node order).
    pub fn choose_root_peers(
        &mut self,
        members: &[(u64, bool)],
        capacity: usize,
    ) -> Result<Vec<u64>, MemoryError> {
        let (chosen, cursor) = choose(members, self.cursors.root_peers, capacity)?;
        self.cursors.root_peers = cursor;
        Ok(chosen)
    }
    /// The measured peers this round lists, of `peers` (in node order).
    pub fn choose_peer_rtts(
        &mut self,
        peers: &[(u64, bool)],
        capacity: usize,
    ) -> Result<Vec<u64>, MemoryError> {
        let (chosen, cursor) = choose(peers, self.cursors.peer_rtts, capacity)?;
        self.cursors.peer_rtts = cursor;
        Ok(chosen)
    }
    /// The admitted tenants this round lists, of `tenants` (in id order).
    pub fn choose_tenants(
        &mut self,
        tenants: &[(TenantId, bool)],
        capacity: usize,
    ) -> Result<Vec<TenantId>, MemoryError> {
        let (chosen, cursor) = choose(tenants, self.cursors.tenants, capacity)?;
        self.cursors.tenants = cursor;
        Ok(chosen)
    }
    /// Keeps the charge for what `last` holds at `bytes`: shrunk in place,
    /// or grown by a reservation of the difference.
    fn charge_to(&mut self, bytes: usize) -> Result<(), MemoryError> {
        match &mut self.charge {
            Some(held) if held.bytes() >= bytes => held.shrink_to(bytes),
            Some(held) => {
                let mut more = self
                    .memory
                    .reserve(
                        BudgetKind::Control,
                        BudgetLane::Ordinary,
                        bytes.saturating_sub(held.bytes()),
                    )?
                    .commit();
                held.absorb(&mut more)
            }
            None => {
                self.charge = Some(
                    self.memory
                        .reserve(BudgetKind::Control, BudgetLane::Ordinary, bytes)?
                        .commit(),
                );
                Ok(())
            }
        }
    }
}

/// A root member at its widest.
fn widest_root_peer() -> PeerProgress {
    PeerProgress {
        node: u64::MAX,
        matched: u64::MAX,
        next_index: u64::MAX,
        state: u8::MAX,
        recent_active: true,
        paused: true,
        pending_snapshot: u64::MAX,
    }
}
/// An admitted tenant at its widest.
fn widest_tenant() -> TenantReport {
    TenantReport {
        tenant: TenantId::from_u128(u128::MAX),
        weight: u32::MAX,
        memory_limit: u64::MAX,
        memory_used: u64::MAX,
        sessions: usize::MAX,
        queued_items: usize::MAX,
        queued_bytes: u64::MAX,
    }
}
impl SessionMetrics {
    /// A session at its widest: answered, with every series present.
    pub(crate) fn widest() -> Self {
        let wide = u64::MAX;
        let id = TenantId::from_u128(u128::MAX).to_string();
        Self {
            tenant: id.clone(),
            session: id,
            observed: true,
            leader: wide,
            term: wide,
            committed_index: wide,
            applied_index: wide,
            sequence: wide,
            pending: wide,
            authoritative: true,
            preferred_leader: Some(wide),
            leader_returns: wide,
            leader_returns_failed: wide,
            native_authoritative: true,
            log_entries_since_checkpoint: wide,
            retention: Some(AdminRetention {
                published: wide,
                cursors: wide,
                archived: wide,
                floor: wide,
                blocker: String::new(),
                retired: wide,
                retiring: true,
            }),
            seed_chunks_missing: Some(wide),
            custody_objects_missing: Some(wide),
            delivery_retained: true,
            route_epoch: Some(wide),
            placement_epoch: Some(wide),
            desired_max_failures: Some(u16::MAX),
            achieved_max_failures: Some(u16::MAX),
            blocked: Some(wide),
            tick_period_ms: wide,
            periods: wide,
            refused_periods: wide,
            peers_unreachable: wide,
            peer_reports_coalesced: wide,
            peer_reports_dropped: wide,
            longest_period_ms: wide,
            broadcast_tail_us: wide,
            pace_samples: wide,
        }
    }
}
impl MetricsSnapshot {
    /// The snapshot of a node labelled `labels` at its widest: every
    /// section present, and no entity of any family listed. What a page's
    /// fixed part costs at most (`PageBudget`); its values do not matter,
    /// since `widest_bytes` counts each at its widest.
    pub(crate) fn widest(labels: &MetricLabels, memory: BudgetStats) -> Self {
        let wide = u64::MAX;
        Self {
            sampled_ms: wide,
            labels: labels.clone(),
            memory,
            disk: Some(DiskStats {
                free: Some(wide),
                outstanding: wide,
                ordinary_outstanding: wide,
                headroom: wide,
                completion_reserve: wide,
                by_kind: [wide; DISK_KIND_COUNT],
            }),
            staged_uploads: wide,
            staged_bytes: wide,
            storage: Some(crate::metrics::StorageMetrics::Wal(
                focal_log::WalWriterStats::default(),
            )),
            fleet: FleetStatus {
                latest_sequence: wide,
                installed: usize::MAX,
                running: usize::MAX,
                stopped: true,
            },
            root: RootMetrics {
                peer_aggregates: RootPeerAggregates::default(),
                peers: Vec::new(),
                ..RootMetrics::default()
            },
            peers: focal_wire::PeerPoolStats::default(),
            listener: focal_wire::AdmissionStats::default(),
            peer_rtts: Vec::new(),
            liveness: LivenessMetrics::default(),
            credential: Some(CredentialMetrics::default()),
            session_aggregates: SessionAggregates::default(),
            rtt_aggregates: RttAggregates::default(),
            listings: Listings::default(),
            rounds: wide,
            sessions: Vec::new(),
            sessions_unobserved: wide,
            collection_ms: wide,
            rounds_refused: wide,
            agent: Some(AgentMetrics {
                tenant_aggregates: TenantAggregates::default(),
                root_intents: wide,
                partition_intents: wide,
                installed: wide,
                last_error: true,
                last_refusal: true,
                admission: AdmissionReport {
                    max_tenants: usize::MAX,
                    memory_limit: wide,
                    memory_used: wide,
                    memory_completion_reserve: wide,
                    disk_free: Some(wide),
                    disk_outstanding: wide,
                    disk_headroom: wide,
                    tenants: Vec::new(),
                },
            }),
            fence_level: u32::MAX,
            announced_level: u32::MAX,
        }
    }
}

#[cfg(test)]
#[path = "metrics_rounds_tests.rs"]
mod tests;
