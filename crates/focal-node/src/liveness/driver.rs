//! The liveness driver: one task per node that probes its members, keeps
//! their suspicions, learns from gossip and coordinates, answers probes
//! through the data service and publishes its view for the placement agent.
//! It never commits anything itself; every allocation it retains is charged
//! up front.
use super::{
    coordinates::{NetworkCoordinate, VivaldiConfig, rtt_ucb_ms},
    gossip::{GossipBuffer, LivenessUpdate, MemberStatus},
    health::LocalHealth,
    suspicion::{ExtensionDecision, ExtensionDenial, ExtensionTracker, Suspicion},
    wire::{
        ExtensionOutcome, ExtensionRequest, PROBE_SCHEMA, ProbeKind, ProbeOutcome, ProbeReply,
        ProbeRequest,
    },
};
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget, MemoryError};
use focal_model::{LedgerId, RequestEpoch, RequestId, RouteEpoch};
use focal_wire::{Operation, PROTOCOL_VERSION, PeerConnectionPool, PeerSendError, RequestEnvelope};
use futures_util::{StreamExt, stream::FuturesUnordered};
use std::{
    collections::{BTreeMap, VecDeque},
    future::Future,
    pin::Pin,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{mpsc, oneshot, watch},
    time::Instant,
};

/// Members the driver will track; the directory bounds enrolled nodes.
pub const MAX_MEMBERS: usize = 1024;
/// Probes, relays and extension requests in flight at once.
pub const MAX_INFLIGHT: usize = 16;
/// Retained events in the published view.
pub const MAX_EVENTS: usize = 32;
const INBOX_DEPTH: usize = 64;
/// Bytes reserved for the driver's whole state: members, suspicions,
/// gossip, events and one probe round.
const STATE_BYTES: usize = MAX_MEMBERS * 640 + 96 * 1024;
/// How long the data service waits for the driver to answer a direct probe.
const DIRECT_ANSWER: Duration = Duration::from_millis(750);

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LivenessConfig {
    /// One member is probed per period.
    pub period: Duration,
    /// The direct probe timeout is `clamp(base, factor·rtt_ucb, cap)` times
    /// the local health multiplier.
    pub base_timeout_ms: u64,
    pub timeout_cap_ms: u64,
    pub timeout_factor: f64,
    /// Members asked to probe on this node's behalf after a direct timeout.
    pub indirect_probes: usize,
    /// The suspicion minimum is `factor · period · max(1, log10(n+1))`
    /// (times the health multiplier); the maximum is `spread` times that.
    pub suspicion_factor: f64,
    pub suspicion_spread: f64,
    pub extension_min_grant_ms: u64,
    /// The local health score at which a suspected host asks for time.
    pub extension_score: u8,
    pub vivaldi: VivaldiConfig,
}
impl Default for LivenessConfig {
    fn default() -> Self {
        Self {
            period: Duration::from_secs(1),
            base_timeout_ms: 300,
            timeout_cap_ms: 2_000,
            timeout_factor: 3.0,
            indirect_probes: 3,
            suspicion_factor: 3.0,
            suspicion_spread: 6.0,
            extension_min_grant_ms: 1_000,
            extension_score: 2,
            vivaldi: VivaldiConfig::default(),
        }
    }
}
impl LivenessConfig {
    pub fn validate(&self) -> Result<(), MemoryError> {
        let ok = self.period >= Duration::from_millis(10)
            && self.period <= Duration::from_secs(60)
            && self.base_timeout_ms > 0
            && self.timeout_cap_ms >= self.base_timeout_ms
            && self.timeout_factor.is_finite()
            && self.timeout_factor >= 1.0
            && self.indirect_probes <= 8
            && self.suspicion_factor.is_finite()
            && self.suspicion_factor >= 1.0
            && self.suspicion_spread.is_finite()
            && self.suspicion_spread >= 1.0
            && self.extension_min_grant_ms > 0;
        if ok {
            Ok(())
        } else {
            Err(MemoryError::InvalidConfiguration("liveness"))
        }
    }
    fn period_ms(&self) -> u64 {
        u64::try_from(self.period.as_millis()).unwrap_or(u64::MAX)
    }
}

/// What the placement agent tells the driver each tick: this node's
/// enrollment generation, the members with theirs, a progress witness and
/// whether admission is refusing capacity.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LocalFacts {
    pub generation: u64,
    pub members: BTreeMap<u64, u64>,
    pub witness: u64,
    pub overloaded: bool,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemberView {
    pub generation: u64,
    pub status: MemberStatus,
    pub incarnation: u64,
    /// Acknowledged at least once at this generation.
    pub confirmed: bool,
    /// Driver milliseconds of the last status change.
    pub decided_at_ms: u64,
    pub suspected_by: Option<u64>,
    pub confirmations: u32,
    pub extensions: u32,
    pub extended_ms: u64,
    /// The direct probe timeout currently applied to this member.
    pub timeout_ms: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LivenessEvent {
    Confirmed {
        node: u64,
    },
    Suspected {
        node: u64,
        incarnation: u64,
        originator: u64,
    },
    Confirmation {
        node: u64,
        from: u64,
    },
    Refuted {
        node: u64,
        incarnation: u64,
    },
    Died {
        node: u64,
        incarnation: u64,
    },
    Revived {
        node: u64,
        incarnation: u64,
    },
    ProbeTimeout {
        node: u64,
    },
    LateTick {
        millis: u64,
    },
    /// This node learned it was suspected and bumped its incarnation.
    SelfRefutation {
        incarnation: u64,
        accuser: u64,
    },
    ExtensionRequested {
        accuser: u64,
        witness: u64,
    },
    ExtensionGranted {
        node: u64,
        millis: u64,
    },
    ExtensionDenied {
        node: u64,
        reason: ExtensionDenial,
    },
    ExtensionAnswered {
        accuser: u64,
        outcome: ExtensionOutcome,
    },
}
#[derive(Debug, Clone, PartialEq)]
pub struct LivenessView {
    pub node: u64,
    pub generation: u64,
    pub incarnation: u64,
    pub health: LocalHealth,
    pub coordinate: NetworkCoordinate,
    pub members: BTreeMap<u64, MemberView>,
    pub events: VecDeque<LivenessEvent>,
    pub ticks: u64,
    pub probes_sent: u64,
    pub probes_answered: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ProbeError {
    #[error("probe payload is invalid for this peer")]
    Invalid,
    #[error("liveness driver is at capacity")]
    Capacity,
    #[error("liveness driver is unavailable")]
    Unavailable,
}
enum Inbound {
    Probe {
        request: ProbeRequest,
        reply: oneshot::Sender<ProbeReply>,
    },
}
/// The data service and the placement agent's side of the driver.
#[derive(Clone)]
pub struct LivenessHandle {
    facts: watch::Sender<LocalFacts>,
    view: watch::Receiver<LivenessView>,
    inbox: mpsc::Sender<Inbound>,
    config: LivenessConfig,
}
impl LivenessHandle {
    /// Create the handle and its driver; the driver's state is charged now.
    pub fn channel(
        budget: &MemoryBudget,
        config: LivenessConfig,
        node: u64,
        namespace: LedgerId,
    ) -> Result<(Self, LivenessDriver), MemoryError> {
        config.validate()?;
        if node == 0 {
            return Err(MemoryError::InvalidConfiguration("liveness node"));
        }
        let allocation = budget
            .reserve(BudgetKind::Control, BudgetLane::Completion, STATE_BYTES)?
            .commit();
        let (facts, facts_rx) = watch::channel(LocalFacts::default());
        let (view_tx, view) = watch::channel(LivenessView::empty(node, &config));
        let (inbox, inbox_rx) = mpsc::channel(INBOX_DEPTH);
        let driver = LivenessDriver {
            node,
            namespace,
            config,
            facts: facts_rx,
            view: view_tx,
            inbox: inbox_rx,
            state: State::new(node, &config),
            _allocation: allocation,
        };
        Ok((
            Self {
                facts,
                view,
                inbox,
                config,
            },
            driver,
        ))
    }
    pub fn view(&self) -> LivenessView {
        self.view.borrow().clone()
    }
    pub fn config(&self) -> &LivenessConfig {
        &self.config
    }
    /// Replace the facts the driver works from; cheap when unchanged.
    pub fn report(&self, facts: LocalFacts) {
        self.facts.send_if_modified(|current| {
            if *current == facts {
                false
            } else {
                *current = facts;
                true
            }
        });
    }
    /// Answer one probe from the authenticated node `peer`.
    pub async fn answer(&self, peer: u64, body: &[u8]) -> Result<Vec<u8>, ProbeError> {
        let request =
            ProbeRequest::decode(body, &self.config.vivaldi).map_err(|_| ProbeError::Invalid)?;
        if request.sender != peer {
            return Err(ProbeError::Invalid);
        }
        let wait = match request.kind {
            ProbeKind::Direct => DIRECT_ANSWER,
            // The relay's own probe of the target, bounded by its timeout cap
            // at the saturated health multiplier, plus the direct wait.
            ProbeKind::Indirect { .. } => {
                Duration::from_millis(self.config.timeout_cap_ms.saturating_mul(3))
                    .saturating_add(DIRECT_ANSWER)
            }
        };
        let (reply, receive) = oneshot::channel();
        self.inbox
            .try_send(Inbound::Probe { request, reply })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => ProbeError::Capacity,
                mpsc::error::TrySendError::Closed(_) => ProbeError::Unavailable,
            })?;
        let reply = tokio::time::timeout(wait, receive)
            .await
            .map_err(|_| ProbeError::Unavailable)?
            .map_err(|_| ProbeError::Unavailable)?;
        reply.encode().map_err(|_| ProbeError::Capacity)
    }
}
impl LivenessView {
    fn empty(node: u64, config: &LivenessConfig) -> Self {
        Self {
            node,
            generation: 0,
            incarnation: 0,
            health: LocalHealth::default(),
            coordinate: NetworkCoordinate::origin(&config.vivaldi),
            members: BTreeMap::new(),
            events: VecDeque::new(),
            ticks: 0,
            probes_sent: 0,
            probes_answered: 0,
        }
    }
    pub fn member(&self, node: u64) -> Option<&MemberView> {
        self.members.get(&node)
    }
}

struct Pending {
    sequence: u64,
    indirect_outstanding: usize,
    acknowledged: bool,
}
struct Member {
    generation: u64,
    status: MemberStatus,
    incarnation: u64,
    confirmed: bool,
    decided_at_ms: u64,
    coordinate: Option<NetworkCoordinate>,
    suspicion: Option<Suspicion>,
    extensions: ExtensionTracker,
    grace_until_ms: u64,
    probe: Option<Pending>,
}
impl Member {
    fn new(generation: u64, now_ms: u64, config: &LivenessConfig) -> Self {
        Self {
            generation,
            status: MemberStatus::Alive,
            incarnation: 0,
            confirmed: false,
            decided_at_ms: now_ms,
            coordinate: None,
            suspicion: None,
            extensions: fresh_tracker(config),
            grace_until_ms: 0,
            probe: None,
        }
    }
    fn view(&self, timeout_ms: u64) -> MemberView {
        MemberView {
            generation: self.generation,
            status: self.status,
            incarnation: self.incarnation,
            confirmed: self.confirmed,
            decided_at_ms: self.decided_at_ms,
            suspected_by: self
                .suspicion
                .as_ref()
                .map(|suspicion| suspicion.originator),
            confirmations: self.suspicion.as_ref().map_or(0, Suspicion::confirmations),
            extensions: self.extensions.count(),
            extended_ms: self.extensions.total_ms(),
            timeout_ms,
        }
    }
}
fn fresh_tracker(config: &LivenessConfig) -> ExtensionTracker {
    // The base grant is one suspicion minimum at ten members; grants decay
    // from there, never below the configured floor, never within a period.
    let base = (config.suspicion_factor * config.period_ms() as f64 * 1.04).round();
    let base = if base.is_finite() && base >= 0.0 {
        // Bounded by the validated configuration.
        base.min(u64::MAX as f64) as u64
    } else {
        config.period_ms()
    };
    ExtensionTracker::new(
        base.max(config.extension_min_grant_ms),
        config.extension_min_grant_ms,
        config.period_ms(),
    )
}
struct State {
    generation: u64,
    incarnation: u64,
    health: LocalHealth,
    coordinate: NetworkCoordinate,
    members: BTreeMap<u64, Member>,
    gossip: GossipBuffer,
    events: VecDeque<LivenessEvent>,
    round: Vec<u64>,
    sequence: u64,
    nonce: u64,
    rng: u64,
    ticks: u64,
    probes_sent: u64,
    probes_answered: u64,
    witness: u64,
    overloaded: bool,
    /// The accuser to ask for time, once, after learning of a suspicion.
    pending_extension: Option<(u64, ExtensionRequest)>,
    inflight: usize,
}
impl State {
    fn new(node: u64, config: &LivenessConfig) -> Self {
        let started = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(1, |elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(1));
        Self {
            generation: 0,
            incarnation: started.max(1),
            health: LocalHealth::default(),
            coordinate: NetworkCoordinate::origin(&config.vivaldi),
            members: BTreeMap::new(),
            gossip: GossipBuffer::default(),
            events: VecDeque::new(),
            round: Vec::new(),
            sequence: 0,
            nonce: 0,
            rng: started ^ node.rotate_left(17) ^ 0x9E37_79B9_7F4A_7C15,
            ticks: 0,
            probes_sent: 0,
            probes_answered: 0,
            witness: 0,
            overloaded: false,
            pending_extension: None,
            inflight: 0,
        }
    }
    fn random(&mut self) -> u64 {
        // xorshift64*: only ordering decisions depend on it.
        let mut x = self.rng.max(1);
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.rng = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn event(&mut self, event: LivenessEvent) {
        if self.events.len() >= MAX_EVENTS {
            self.events.pop_front();
        }
        self.events.push_back(event);
    }
    fn population(&self) -> usize {
        self.members.len().saturating_add(1)
    }
}
enum Done {
    Direct {
        target: u64,
        sequence: u64,
        sent_at_ms: u64,
        result: Result<Vec<u8>, PeerSendError>,
        extension: bool,
    },
    Indirect {
        target: u64,
        via: u64,
        sequence: u64,
        sent_at_ms: u64,
        result: Result<Vec<u8>, PeerSendError>,
    },
    Relay {
        target: u64,
        sequence: u64,
        sent_at_ms: u64,
        request: ProbeRequest,
        reply: oneshot::Sender<ProbeReply>,
        result: Result<Vec<u8>, PeerSendError>,
    },
}
type Inflight<'a> = FuturesUnordered<Pin<Box<dyn Future<Output = Done> + Send + 'a>>>;

pub struct LivenessDriver {
    node: u64,
    namespace: LedgerId,
    config: LivenessConfig,
    facts: watch::Receiver<LocalFacts>,
    view: watch::Sender<LivenessView>,
    inbox: mpsc::Receiver<Inbound>,
    state: State,
    _allocation: Allocation,
}
impl LivenessDriver {
    /// Run until the pool closes or every handle is dropped.
    pub async fn run(mut self, pool: &PeerConnectionPool) {
        let started = Instant::now();
        let period = self.config.period;
        let mut inflight: Inflight<'_> = FuturesUnordered::new();
        let mut next = started.checked_add(period).unwrap_or(started);
        loop {
            let now_ms = elapsed_ms(started);
            tokio::select! {
                biased;
                inbound = self.inbox.recv() => {
                    let Some(Inbound::Probe { request, reply }) = inbound else {
                        return;
                    };
                    self.on_inbound(request, reply, now_ms, pool, &mut inflight);
                }
                Some(done) = inflight.next(), if !inflight.is_empty() => {
                    self.state.inflight = self.state.inflight.saturating_sub(1);
                    if self.on_done(done, elapsed_ms(started), pool, &mut inflight) {
                        return;
                    }
                }
                () = tokio::time::sleep_until(next) => {
                    let at = Instant::now();
                    let late = at.saturating_duration_since(next);
                    if late > period.checked_div(2).unwrap_or(period) {
                        self.state.health.on_late_tick();
                        let millis = u64::try_from(late.as_millis()).unwrap_or(u64::MAX);
                        self.state.event(LivenessEvent::LateTick { millis });
                    }
                    next = at.checked_add(period).unwrap_or(at);
                    if self.tick(elapsed_ms(started), pool, &mut inflight) {
                        return;
                    }
                }
            }
            self.publish();
        }
    }
    fn publish(&mut self) {
        let state = &self.state;
        let config = &self.config;
        let view = LivenessView {
            node: self.node,
            generation: state.generation,
            incarnation: state.incarnation,
            health: state.health,
            coordinate: state.coordinate,
            members: state
                .members
                .iter()
                .map(|(node, member)| {
                    (
                        *node,
                        member.view(probe_timeout_ms(
                            config,
                            &state.coordinate,
                            member,
                            &state.health,
                        )),
                    )
                })
                .collect(),
            events: state.events.clone(),
            ticks: state.ticks,
            probes_sent: state.probes_sent,
            probes_answered: state.probes_answered,
        };
        self.view.send_replace(view);
    }
    /// Fold the agent's facts in: new members start unconfirmed at their
    /// generation, a changed generation is a new member, departed members
    /// leave with their gossip.
    fn sync_facts(&mut self, now_ms: u64) {
        let facts = self.facts.borrow().clone();
        let state = &mut self.state;
        state.generation = facts.generation;
        state.witness = facts.witness;
        state.overloaded = facts.overloaded;
        let node = self.node;
        state.members.retain(|member, entry| {
            facts.members.get(member) == Some(&entry.generation) && *member != node
        });
        for (member, generation) in facts.members.iter().take(MAX_MEMBERS) {
            if *member == node || *member == 0 || *generation == 0 {
                continue;
            }
            state
                .members
                .entry(*member)
                .or_insert_with(|| Member::new(*generation, now_ms, &self.config));
        }
        let members = &state.members;
        state.round.retain(|member| members.contains_key(member));
    }
    fn tick<'a>(
        &mut self,
        now_ms: u64,
        pool: &'a PeerConnectionPool,
        inflight: &mut Inflight<'a>,
    ) -> bool {
        self.state.ticks = self.state.ticks.saturating_add(1);
        self.sync_facts(now_ms);
        self.expire_suspicions(now_ms);
        if self.state.generation == 0 || self.state.members.is_empty() {
            return false;
        }
        if let Some((accuser, request)) = self.state.pending_extension.take() {
            let free = self
                .state
                .members
                .get(&accuser)
                .is_some_and(|member| member.probe.is_none());
            if free {
                self.state.event(LivenessEvent::ExtensionRequested {
                    accuser,
                    witness: request.witness,
                });
                if let Err(closed) =
                    self.send_direct(accuser, Some(request), now_ms, pool, inflight)
                {
                    return closed;
                }
                return false;
            }
            self.state.pending_extension = Some((accuser, request));
        }
        let Some(target) = self.next_target() else {
            return false;
        };
        match self.send_direct(target, None, now_ms, pool, inflight) {
            Ok(()) => false,
            Err(closed) => closed,
        }
    }
    /// The next member of a shuffled round that has no probe in flight.
    fn next_target(&mut self) -> Option<u64> {
        for _ in 0..2 {
            if self.state.round.is_empty() {
                let mut round: Vec<u64> = self.state.members.keys().copied().collect();
                let len = round.len();
                for index in (1..len).rev() {
                    let bound = u64::try_from(index).unwrap_or(u64::MAX).saturating_add(1);
                    let pick = usize::try_from(self.state.random().checked_rem(bound).unwrap_or(0))
                        .unwrap_or(0)
                        .min(index);
                    round.swap(index, pick);
                }
                self.state.round = round;
            }
            while let Some(candidate) = self.state.round.pop() {
                if self
                    .state
                    .members
                    .get(&candidate)
                    .is_some_and(|member| member.probe.is_none())
                {
                    return Some(candidate);
                }
            }
        }
        None
    }
    fn expire_suspicions(&mut self, now_ms: u64) {
        let mut died = Vec::new();
        for (node, member) in &mut self.state.members {
            if member
                .suspicion
                .as_ref()
                .is_some_and(|suspicion| suspicion.expired(now_ms))
            {
                member.suspicion = None;
                member.status = MemberStatus::Dead;
                member.decided_at_ms = now_ms;
                died.push((*node, member.generation, member.incarnation));
            }
        }
        for (node, generation, incarnation) in died {
            self.state.event(LivenessEvent::Died { node, incarnation });
            self.gossip(LivenessUpdate {
                node,
                generation,
                incarnation,
                status: MemberStatus::Dead,
                origin: self.node,
            });
        }
    }
    fn gossip(&mut self, update: LivenessUpdate) {
        let members = self.state.population();
        self.state.gossip.add(update, members);
    }
    fn request_id(&mut self) -> RequestId {
        self.state.nonce = self.state.nonce.wrapping_add(1);
        let mut id = [0; 16];
        id[..8].copy_from_slice(&self.node.to_be_bytes());
        id[8..].copy_from_slice(&self.state.nonce.to_be_bytes());
        RequestId(id)
    }
    fn envelope(&mut self, body: Vec<u8>) -> RequestEnvelope {
        RequestEnvelope {
            protocol: PROTOCOL_VERSION,
            ledger: self.namespace,
            route_epoch: RouteEpoch(1),
            request_epoch: RequestEpoch(1),
            request_id: self.request_id(),
            operation: Operation::Probe { request: body },
        }
    }
    fn request(&mut self, kind: ProbeKind, extension: Option<ExtensionRequest>) -> ProbeRequest {
        self.state.sequence = self.state.sequence.wrapping_add(1).max(1);
        ProbeRequest {
            schema: PROBE_SCHEMA,
            kind,
            sender: self.node,
            generation: self.state.generation,
            sequence: self.state.sequence,
            incarnation: self.state.incarnation,
            coordinate: self.state.coordinate,
            health: self.state.health,
            extension,
            updates: self.state.gossip.piggyback(),
        }
    }
    fn reply(
        &mut self,
        outcome: ProbeOutcome,
        sequence: u64,
        extension: Option<ExtensionOutcome>,
    ) -> ProbeReply {
        ProbeReply {
            schema: PROBE_SCHEMA,
            outcome,
            node: self.node,
            generation: self.state.generation,
            sequence,
            incarnation: self.state.incarnation,
            coordinate: self.state.coordinate,
            health: self.state.health,
            extension,
            updates: self.state.gossip.piggyback(),
        }
    }
    fn timeout_for(&self, target: u64) -> u64 {
        self.state
            .members
            .get(&target)
            .map_or(self.config.base_timeout_ms, |member| {
                probe_timeout_ms(
                    &self.config,
                    &self.state.coordinate,
                    member,
                    &self.state.health,
                )
            })
    }
    /// Send one direct probe; `Err(true)` means the pool closed.
    fn send_direct<'a>(
        &mut self,
        target: u64,
        extension: Option<ExtensionRequest>,
        now_ms: u64,
        pool: &'a PeerConnectionPool,
        inflight: &mut Inflight<'a>,
    ) -> Result<(), bool> {
        if self.state.inflight >= MAX_INFLIGHT {
            return Ok(());
        }
        let request = self.request(ProbeKind::Direct, extension);
        let sequence = request.sequence;
        let Ok(body) = request.encode() else {
            return Ok(());
        };
        let timeout = Duration::from_millis(self.timeout_for(target));
        let envelope = self.envelope(body);
        let Some(member) = self.state.members.get_mut(&target) else {
            return Ok(());
        };
        member.probe = Some(Pending {
            sequence,
            indirect_outstanding: 0,
            acknowledged: false,
        });
        self.state.probes_sent = self.state.probes_sent.saturating_add(1);
        self.state.inflight = self.state.inflight.saturating_add(1);
        let with_extension = extension.is_some();
        inflight.push(Box::pin(async move {
            let result = probe(pool, target, &envelope, timeout).await;
            Done::Direct {
                target,
                sequence,
                sent_at_ms: now_ms,
                result,
                extension: with_extension,
            }
        }));
        Ok(())
    }
    fn send_indirect<'a>(
        &mut self,
        target: u64,
        now_ms: u64,
        pool: &'a PeerConnectionPool,
        inflight: &mut Inflight<'a>,
    ) -> usize {
        let candidates: Vec<u64> = self
            .state
            .members
            .iter()
            .filter(|(node, member)| {
                **node != target && member.confirmed && member.status == MemberStatus::Alive
            })
            .map(|(node, _)| *node)
            .collect();
        let mut chosen = Vec::new();
        let mut remaining = candidates;
        while chosen.len() < self.config.indirect_probes && !remaining.is_empty() {
            let bound = u64::try_from(remaining.len()).unwrap_or(u64::MAX);
            let pick = usize::try_from(self.state.random().checked_rem(bound).unwrap_or(0))
                .unwrap_or(0)
                .min(remaining.len().saturating_sub(1));
            chosen.push(remaining.swap_remove(pick));
        }
        let mut sent: usize = 0;
        for via in chosen {
            if self.state.inflight >= MAX_INFLIGHT {
                break;
            }
            let request = self.request(ProbeKind::Indirect { target }, None);
            let sequence = request.sequence;
            let Ok(body) = request.encode() else {
                continue;
            };
            let timeout = Duration::from_millis(
                self.timeout_for(via)
                    .saturating_add(self.timeout_for(target))
                    .min(self.config.timeout_cap_ms.saturating_mul(3)),
            );
            let envelope = self.envelope(body);
            self.state.inflight = self.state.inflight.saturating_add(1);
            self.state.probes_sent = self.state.probes_sent.saturating_add(1);
            sent = sent.saturating_add(1);
            inflight.push(Box::pin(async move {
                let result = probe(pool, via, &envelope, timeout).await;
                Done::Indirect {
                    target,
                    via,
                    sequence,
                    sent_at_ms: now_ms,
                    result,
                }
            }));
        }
        sent
    }
    fn on_inbound<'a>(
        &mut self,
        request: ProbeRequest,
        reply: oneshot::Sender<ProbeReply>,
        now_ms: u64,
        pool: &'a PeerConnectionPool,
        inflight: &mut Inflight<'a>,
    ) {
        if self.state.generation == 0 {
            // Nothing to say yet; the peer treats the missing answer as
            // inconclusive.
            return;
        }
        let known = self
            .state
            .members
            .get(&request.sender)
            .is_some_and(|member| member.generation == request.generation);
        if known {
            self.learn_alive(request.sender, request.incarnation, now_ms);
            if let Some(member) = self.state.members.get_mut(&request.sender) {
                member.coordinate = Some(request.coordinate);
            }
        }
        for update in &request.updates {
            self.on_update(*update, now_ms);
        }
        let extension = request
            .extension
            .map(|extension| self.on_extension_request(request.sender, extension, now_ms));
        match request.kind {
            ProbeKind::Direct => {
                self.state.health.on_successful_answer();
                self.state.probes_answered = self.state.probes_answered.saturating_add(1);
                let answer = self.reply(ProbeOutcome::Ack, request.sequence, extension);
                let _ = reply.send(answer);
            }
            ProbeKind::Indirect { target } => {
                let can_relay =
                    self.state.members.contains_key(&target) && self.state.inflight < MAX_INFLIGHT;
                if !can_relay {
                    let answer = self.reply(ProbeOutcome::Refused, request.sequence, extension);
                    let _ = reply.send(answer);
                    return;
                }
                let relay = self.request(ProbeKind::Direct, None);
                let sequence = relay.sequence;
                let Ok(body) = relay.encode() else {
                    let answer = self.reply(ProbeOutcome::Refused, request.sequence, extension);
                    let _ = reply.send(answer);
                    return;
                };
                let timeout = Duration::from_millis(self.timeout_for(target));
                let envelope = self.envelope(body);
                self.state.inflight = self.state.inflight.saturating_add(1);
                self.state.probes_sent = self.state.probes_sent.saturating_add(1);
                inflight.push(Box::pin(async move {
                    let result = probe(pool, target, &envelope, timeout).await;
                    Done::Relay {
                        target,
                        sequence,
                        sent_at_ms: now_ms,
                        request,
                        reply,
                        result,
                    }
                }));
            }
        }
    }
    /// Returns whether the pool closed.
    fn on_done<'a>(
        &mut self,
        done: Done,
        now_ms: u64,
        pool: &'a PeerConnectionPool,
        inflight: &mut Inflight<'a>,
    ) -> bool {
        match done {
            Done::Direct {
                target,
                sequence,
                sent_at_ms,
                result,
                extension,
            } => {
                let current = self
                    .state
                    .members
                    .get(&target)
                    .and_then(|member| member.probe.as_ref())
                    .is_some_and(|pending| pending.sequence == sequence);
                let outcome = self.classify(target, sequence, sent_at_ms, now_ms, result);
                if !current {
                    // A stale answer still teaches (classified above) but
                    // decides nothing about the probe in flight.
                    return matches!(outcome, Verdict::Closed);
                }
                match outcome {
                    Verdict::Acknowledged(reply) => {
                        if extension && let Some(answer) = reply.extension {
                            self.state.event(LivenessEvent::ExtensionAnswered {
                                accuser: target,
                                outcome: answer,
                            });
                        }
                        self.state.health.on_successful_probe();
                        if let Some(member) = self.state.members.get_mut(&target) {
                            member.probe = None;
                        }
                    }
                    Verdict::Inconclusive => {
                        if let Some(member) = self.state.members.get_mut(&target) {
                            member.probe = None;
                        }
                    }
                    Verdict::Closed => return true,
                    Verdict::Failed => {
                        self.state.health.on_probe_timeout();
                        self.state
                            .event(LivenessEvent::ProbeTimeout { node: target });
                        let sent = self.send_indirect(target, now_ms, pool, inflight);
                        match self.state.members.get_mut(&target) {
                            Some(member) if sent > 0 => {
                                if let Some(pending) = &mut member.probe {
                                    pending.indirect_outstanding = sent;
                                }
                            }
                            _ => self.probe_failed(target, now_ms),
                        }
                    }
                }
            }
            Done::Indirect {
                target,
                via,
                sequence,
                sent_at_ms,
                result,
            } => {
                let relayed = match self.classify(via, sequence, sent_at_ms, now_ms, result) {
                    Verdict::Acknowledged(reply) => match reply.outcome {
                        ProbeOutcome::Relayed {
                            target: relayed,
                            acknowledged: Some(incarnation),
                        } if relayed == target => {
                            self.learn_alive(target, incarnation, now_ms);
                            true
                        }
                        _ => false,
                    },
                    Verdict::Closed => return true,
                    Verdict::Inconclusive | Verdict::Failed => false,
                };
                let finished = match self.state.members.get_mut(&target) {
                    Some(member) => match &mut member.probe {
                        Some(pending) => {
                            pending.acknowledged |= relayed;
                            pending.indirect_outstanding =
                                pending.indirect_outstanding.saturating_sub(1);
                            if pending.indirect_outstanding == 0 {
                                Some(pending.acknowledged)
                            } else {
                                None
                            }
                        }
                        None => None,
                    },
                    None => None,
                };
                match finished {
                    Some(true) => {
                        if let Some(member) = self.state.members.get_mut(&target) {
                            member.probe = None;
                        }
                    }
                    Some(false) => self.probe_failed(target, now_ms),
                    None => {}
                }
            }
            Done::Relay {
                target,
                sequence,
                sent_at_ms,
                request,
                reply,
                result,
            } => {
                let acknowledged = match self.classify(target, sequence, sent_at_ms, now_ms, result)
                {
                    Verdict::Acknowledged(answer) => Some(answer.incarnation),
                    Verdict::Closed => return true,
                    Verdict::Inconclusive | Verdict::Failed => None,
                };
                let answer = self.reply(
                    ProbeOutcome::Relayed {
                        target,
                        acknowledged,
                    },
                    request.sequence,
                    None,
                );
                let _ = reply.send(answer);
            }
        }
        false
    }
    /// Decode and account one answer from `from`.
    fn classify(
        &mut self,
        from: u64,
        sequence: u64,
        sent_at_ms: u64,
        now_ms: u64,
        result: Result<Vec<u8>, PeerSendError>,
    ) -> Verdict {
        match result {
            Ok(bytes) => {
                let Ok(reply) = ProbeReply::decode(&bytes, &self.config.vivaldi) else {
                    return Verdict::Inconclusive;
                };
                let generation_ok = self
                    .state
                    .members
                    .get(&from)
                    .is_some_and(|member| member.generation == reply.generation);
                if reply.node != from
                    || reply.sequence != sequence
                    || !generation_ok
                    || reply.outcome == ProbeOutcome::Refused
                {
                    return Verdict::Inconclusive;
                }
                let rtt = now_ms.saturating_sub(sent_at_ms) as f64;
                self.state
                    .coordinate
                    .update(&reply.coordinate, rtt, &self.config.vivaldi);
                if let Some(member) = self.state.members.get_mut(&from) {
                    member.coordinate = Some(reply.coordinate);
                }
                self.learn_alive(from, reply.incarnation, now_ms);
                for update in &reply.updates {
                    self.on_update(*update, now_ms);
                }
                Verdict::Acknowledged(reply)
            }
            Err(PeerSendError::Closed) => Verdict::Closed,
            Err(PeerSendError::Lost) => Verdict::Failed,
            // No route, a busy transport or a peer that answered with a
            // refusal says nothing about the peer's life.
            Err(_) => Verdict::Inconclusive,
        }
    }
    /// A direct probe and every indirect one went unanswered.
    fn probe_failed(&mut self, target: u64, now_ms: u64) {
        let population = self.state.population();
        let lhm = self.state.health.multiplier();
        let config = self.config;
        let node = self.node;
        let Some(member) = self.state.members.get_mut(&target) else {
            return;
        };
        member.probe = None;
        if !member.confirmed
            || member.status == MemberStatus::Dead
            || member.suspicion.is_some()
            || now_ms < member.grace_until_ms
        {
            return;
        }
        let (min_ms, max_ms) = suspicion_bounds(&config, population, lhm);
        let target_confirmations =
            u32::try_from(population.saturating_sub(2).clamp(1, 3)).unwrap_or(1);
        member.suspicion = Some(Suspicion::new(
            member.incarnation,
            now_ms,
            node,
            min_ms,
            max_ms,
            target_confirmations,
            member.extensions.clone(),
        ));
        member.status = MemberStatus::Suspect;
        member.decided_at_ms = now_ms;
        let (generation, incarnation) = (member.generation, member.incarnation);
        self.state.event(LivenessEvent::Suspected {
            node: target,
            incarnation,
            originator: node,
        });
        self.gossip(LivenessUpdate {
            node: target,
            generation,
            incarnation,
            status: MemberStatus::Suspect,
            origin: node,
        });
    }
    /// Direct evidence that `node` answers at `incarnation`.
    fn learn_alive(&mut self, node: u64, incarnation: u64, now_ms: u64) {
        let config = self.config;
        let me = self.node;
        let Some(member) = self.state.members.get_mut(&node) else {
            return;
        };
        if incarnation < member.incarnation {
            return;
        }
        let mut events = Vec::new();
        if !member.confirmed {
            member.confirmed = true;
            events.push(LivenessEvent::Confirmed { node });
        }
        if incarnation > member.incarnation {
            member.incarnation = incarnation;
            member.extensions = fresh_tracker(&config);
            member.grace_until_ms = 0;
        }
        if member.suspicion.take().is_some() {
            events.push(LivenessEvent::Refuted { node, incarnation });
        }
        if member.status != MemberStatus::Alive {
            if member.status == MemberStatus::Dead {
                events.push(LivenessEvent::Revived { node, incarnation });
            }
            member.status = MemberStatus::Alive;
            member.decided_at_ms = now_ms;
        }
        let generation = member.generation;
        for event in events {
            self.state.event(event);
        }
        self.gossip(LivenessUpdate {
            node,
            generation,
            incarnation,
            status: MemberStatus::Alive,
            origin: me,
        });
    }
    fn on_update(&mut self, update: LivenessUpdate, now_ms: u64) {
        if update.node == self.node {
            self.on_self_update(update);
            return;
        }
        let population = self.state.population();
        let lhm = self.state.health.multiplier();
        let config = self.config;
        let Some(member) = self.state.members.get_mut(&update.node) else {
            return;
        };
        if member.generation != update.generation || update.incarnation < member.incarnation {
            return;
        }
        match update.status {
            MemberStatus::Alive => {
                if update.incarnation > member.incarnation {
                    self.learn_alive(update.node, update.incarnation, now_ms);
                }
            }
            MemberStatus::Suspect => {
                if update.incarnation > member.incarnation {
                    member.incarnation = update.incarnation;
                    member.extensions = fresh_tracker(&config);
                    member.grace_until_ms = 0;
                    member.suspicion = None;
                }
                if member.status == MemberStatus::Dead {
                    return;
                }
                match &mut member.suspicion {
                    Some(suspicion) if suspicion.incarnation == update.incarnation => {
                        if suspicion.confirm(update.origin) {
                            self.state.event(LivenessEvent::Confirmation {
                                node: update.node,
                                from: update.origin,
                            });
                            self.gossip(update);
                        }
                    }
                    Some(_) => {}
                    None => {
                        let (min_ms, max_ms) = suspicion_bounds(&config, population, lhm);
                        let target =
                            u32::try_from(population.saturating_sub(2).clamp(1, 3)).unwrap_or(1);
                        member.suspicion = Some(Suspicion::new(
                            update.incarnation,
                            now_ms,
                            update.origin,
                            min_ms,
                            max_ms,
                            target,
                            member.extensions.clone(),
                        ));
                        member.status = MemberStatus::Suspect;
                        member.decided_at_ms = now_ms;
                        self.state.event(LivenessEvent::Suspected {
                            node: update.node,
                            incarnation: update.incarnation,
                            originator: update.origin,
                        });
                        self.gossip(update);
                    }
                }
            }
            MemberStatus::Dead => {
                if member.status == MemberStatus::Dead && update.incarnation == member.incarnation {
                    return;
                }
                member.incarnation = update.incarnation;
                member.suspicion = None;
                member.status = MemberStatus::Dead;
                member.decided_at_ms = now_ms;
                self.state.event(LivenessEvent::Died {
                    node: update.node,
                    incarnation: update.incarnation,
                });
                self.gossip(update);
            }
        }
    }
    /// Gossip about this node: a suspicion or death at our incarnation or
    /// later is refuted by moving past it; a loaded host also asks its
    /// accuser for time.
    fn on_self_update(&mut self, update: LivenessUpdate) {
        if update.generation != self.state.generation
            || update.status == MemberStatus::Alive
            || update.incarnation < self.state.incarnation
        {
            return;
        }
        self.state.incarnation = update.incarnation.saturating_add(1);
        self.state.health.on_refutation_needed();
        self.state.event(LivenessEvent::SelfRefutation {
            incarnation: self.state.incarnation,
            accuser: update.origin,
        });
        let refutation = LivenessUpdate {
            node: self.node,
            generation: self.state.generation,
            incarnation: self.state.incarnation,
            status: MemberStatus::Alive,
            origin: self.node,
        };
        self.gossip(refutation);
        if self.state.health.score >= self.config.extension_score
            && update.origin != self.node
            && self.state.members.contains_key(&update.origin)
        {
            self.state.pending_extension = Some((
                update.origin,
                ExtensionRequest {
                    incarnation: self.state.incarnation,
                    witness: self.state.witness,
                    overloaded: self.state.overloaded,
                },
            ));
        }
    }
    /// A member asks for more time before this node declares it.
    fn on_extension_request(
        &mut self,
        from: u64,
        request: ExtensionRequest,
        now_ms: u64,
    ) -> ExtensionOutcome {
        let Some(member) = self.state.members.get_mut(&from) else {
            return ExtensionOutcome::Denied;
        };
        if request.incarnation < member.incarnation {
            return ExtensionOutcome::Denied;
        }
        let decision = member
            .extensions
            .request(now_ms, request.witness, request.overloaded);
        match decision {
            ExtensionDecision::Granted { millis } => {
                member.grace_until_ms = member.grace_until_ms.max(now_ms).saturating_add(millis);
                let tracker = member.extensions.clone();
                if let Some(suspicion) = &mut member.suspicion {
                    suspicion.extensions = tracker;
                }
                self.state
                    .event(LivenessEvent::ExtensionGranted { node: from, millis });
                ExtensionOutcome::Granted { millis }
            }
            ExtensionDecision::Denied(reason) => {
                self.state
                    .event(LivenessEvent::ExtensionDenied { node: from, reason });
                ExtensionOutcome::Denied
            }
        }
    }
}
enum Verdict {
    Acknowledged(ProbeReply),
    Failed,
    Inconclusive,
    Closed,
}
async fn probe(
    pool: &PeerConnectionPool,
    target: u64,
    envelope: &RequestEnvelope,
    timeout: Duration,
) -> Result<Vec<u8>, PeerSendError> {
    match tokio::time::timeout(timeout, pool.send_probe(target, envelope)).await {
        Ok(result) => result,
        Err(_) => Err(PeerSendError::Lost),
    }
}
fn elapsed_ms(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}
fn probe_timeout_ms(
    config: &LivenessConfig,
    local: &NetworkCoordinate,
    member: &Member,
    health: &LocalHealth,
) -> u64 {
    let ucb = rtt_ucb_ms(local, member.coordinate.as_ref(), &config.vivaldi);
    let base = (config.timeout_factor * ucb)
        .clamp(config.base_timeout_ms as f64, config.timeout_cap_ms as f64);
    let scaled = (base * health.multiplier()).round();
    if scaled.is_finite() && scaled >= 0.0 {
        // Bounded by three times the validated cap.
        (scaled.min(u64::MAX as f64)) as u64
    } else {
        config.timeout_cap_ms
    }
}
/// `min = factor · period · max(1, log10(n+1)) · lhm`, `max = spread · min`.
fn suspicion_bounds(config: &LivenessConfig, population: usize, lhm: f64) -> (u64, u64) {
    let n = u32::try_from(population).unwrap_or(u32::MAX);
    let scale = (f64::from(n) + 1.0).log10().max(1.0);
    let min = (config.suspicion_factor * config.period_ms() as f64 * scale * lhm).round();
    let min = if min.is_finite() && min >= 0.0 {
        // Bounded by the validated configuration and the saturating health.
        (min.min(u64::MAX as f64)) as u64
    } else {
        config.period_ms()
    };
    let max = (min as f64 * config.suspicion_spread).round();
    let max = if max.is_finite() && max >= 0.0 {
        (max.min(u64::MAX as f64)) as u64
    } else {
        min
    };
    (min.max(1), max.max(min.max(1)))
}
