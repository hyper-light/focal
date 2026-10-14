//! Bounded node-to-node replication delivery. Reachability is supplied by the
//! committed control owner; routes never grant membership or voter authority.
use crate::*;
use hyper_rt::sync::{Semaphore, WatchReceiver, watch};
use hyper_transport::PathFacts;
use std::{
    collections::BTreeMap,
    future::Future,
    net::SocketAddr,
    pin::Pin,
    rc::Rc,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

/// The shard's clock now, nanoseconds: what the pool times its exchanges and cooldowns by (a
/// simulated shard's virtual clock under test).
fn now_ns() -> u64 {
    hyper_rt::futures::now_ns()
}
/// `period` in the nanoseconds hyper-rt's waits take.
fn nanos(period: Duration) -> u64 {
    u64::try_from(period.as_nanos()).unwrap_or(u64::MAX)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerEndpoint {
    pub address: SocketAddr,
    /// The committed identity's certificate name, not an unverified redirect.
    pub server_name: String,
    /// The name the peer advertised (`host:port`, 24 §24), resolved afresh on
    /// every dial and raced against `address`; the certificate check stays
    /// the same for every candidate.
    pub name: Option<String>,
}
/// The ordered profile (27 §12): a connection that negotiated it carries a
/// group's bulk frames with the order they left their sender in
/// ([`Operation::RaftOrdered`]), and the receiver steps them in it. It
/// implies every earlier profile.
pub const ORDERED_PROTOCOL_VERSION: u16 = 5;
/// The laned profile: a connection that negotiated it carries a group's replication frames on
/// replication lanes (hyper-transport's lanes: one long-lived unidirectional stream to the peer
/// for the frames a group cannot do without, one for its bulk appends), each frame answered by
/// nothing, where an exchange of a request and a reply carried each. It implies every earlier
/// profile.
pub const LANED_PROTOCOL_VERSION: u16 = 6;
#[derive(Debug, Clone)]
pub struct PeerPoolLimits {
    pub max_routes: usize,
    pub max_connections: usize,
    pub max_inflight: usize,
    pub per_peer_inflight: usize,
    /// Liveness probes in flight at once, on their own lane: one per peer
    /// and this many overall, so replication or control traffic to a peer
    /// that stopped answering can never starve the failure detector.
    pub max_probe_inflight: usize,
    pub attempts: u8,
    pub timeout: Duration,
    pub retry_backoff: Duration,
    /// How long a peer whose dial failed is left alone before a send dials
    /// it again (a liveness probe excepted, see `connection`). Sends within
    /// the cooldown fail at once as `Lost` instead of
    /// each running a dial to its deadline, so an unreachable peer holds at
    /// most one dial's worth of send capacity per cooldown and never the
    /// capacity live peers need; zero disables it.
    pub unreachable_cooldown: Duration,
}
impl Default for PeerPoolLimits {
    fn default() -> Self {
        Self {
            max_routes: 4096,
            max_connections: 128,
            max_inflight: 256,
            per_peer_inflight: 2,
            max_probe_inflight: 16,
            attempts: 2,
            timeout: Duration::from_secs(5),
            retry_backoff: Duration::from_millis(10),
            unreachable_cooldown: Duration::from_secs(2),
        }
    }
}
impl PeerPoolLimits {
    /// The pool's lanes derived from the consensus window: as many
    /// exchanges to one peer at once as the leader's pipeline to a follower,
    /// and as many in all as every connection's lane holds.
    pub fn for_consensus(window: usize) -> Self {
        let defaults = Self::default();
        Self {
            per_peer_inflight: window.clamp(1, 65536),
            max_inflight: window
                .saturating_mul(defaults.max_connections)
                .clamp(1, 65536),
            ..defaults
        }
    }
    fn validate(&self) -> Result<(), PeerSendError> {
        if self.max_routes == 0
            || self.max_routes > 65536
            || self.max_connections == 0
            || self.max_connections > self.max_routes
            || self.max_inflight == 0
            || self.max_inflight > 65536
            || !(1..=65536).contains(&self.per_peer_inflight)
            || !(1..=64).contains(&self.max_probe_inflight)
            || !(1..=3).contains(&self.attempts)
            || self.timeout.is_zero()
            || self.timeout > Duration::from_secs(120)
            || self.retry_backoff > self.timeout
            || self.unreachable_cooldown > Duration::from_secs(60)
        {
            return Err(PeerSendError::Configuration);
        }
        Ok(())
    }
}
/// How a group's message is carried to its peer ([`PeerConnectionPool::send_as`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Delivery {
    /// On its lane where its connection speaks the laned profile
    /// ([`crate::LANED_PROTOCOL_VERSION`]): `Ok` once it is queued, answered by nothing but its
    /// group's own answer. As an exchange on a connection that does not.
    Queued,
    /// As an exchange, answered once the peer has taken it and persisted it: a snapshot, whose
    /// sender is told it was installed and moves its member on from there.
    Answered,
}
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PeerSendError {
    #[error("peer transport limits or endpoint are invalid")]
    Configuration,
    #[error("replication envelope is invalid or exceeds its frame budget")]
    InvalidRequest,
    #[error("peer transport is at its queue or connection bound; Raft must retransmit")]
    Busy,
    #[error("peer has no installed control-plane route")]
    NoRoute,
    #[error("peer route revision is stale or conflicts with installed state")]
    StaleRoutes,
    #[error("peer route changed during delivery; Raft must retransmit")]
    RouteChanged,
    #[error("replication delivery was lost or has unknown outcome; Raft must retransmit")]
    Lost,
    #[error("peer ingress rejected replication: {0}")]
    Rejected(AccessError),
    #[error("peer pool has shut down")]
    Closed,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PeerPoolStats {
    pub delivered: u64,
    pub lost: u64,
    /// Of `lost`, what the peer answered: refused for room
    /// (`AccessError::Capacity`), an outcome it could not say on any attempt
    /// (`OutcomeUnknown`), refused as serving no such group or service now
    /// (`Unavailable`, without effect: a member whose copy of a group is not
    /// open yet answers so until it is), or refused otherwise. The rest had
    /// no answer: lost on the way, or their route changed under them.
    pub lost_refused: u64,
    pub lost_unknown: u64,
    pub lost_unavailable: u64,
    pub lost_rejected: u64,
    pub busy: u64,
    /// Dials attempted, successful or not.
    pub dials: u64,
    /// Asks for a connection refused at once within a peer's unreachable
    /// cooldown, each spared a dial.
    pub refused_unreachable: u64,
    pub connections_opened: u64,
    /// Connections no longer wanted whose close their endpoint could not queue — the shard's
    /// tasks all taken: each idles out at its connection's idle timeout instead.
    pub closes_refused: u64,
    pub cached_connections: usize,
    pub inflight: usize,
    /// Content in flight or waiting its turn on a peer's lane.
    pub bulk_inflight: usize,
}
#[derive(Default)]
struct Counters {
    delivered: AtomicU64,
    lost: AtomicU64,
    lost_refused: AtomicU64,
    lost_unknown: AtomicU64,
    lost_unavailable: AtomicU64,
    lost_rejected: AtomicU64,
    busy: AtomicU64,
    dials: AtomicU64,
    unreachable: AtomicU64,
    opened: AtomicU64,
    closes_refused: AtomicU64,
}
struct Connected {
    generation: u64,
    remote: QuicRemote,
}
/// What a slot's detached dial has decided so far; every caller waiting on
/// the slot observes the same decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DialState {
    Pending,
    Connected,
    Failed,
}
impl DialState {
    /// The word the dial's watch carries.
    fn word(self) -> u64 {
        match self {
            Self::Pending => 0,
            Self::Connected => 1,
            Self::Failed => 2,
        }
    }
    fn of(word: u64) -> Self {
        match word {
            0 => Self::Pending,
            1 => Self::Connected,
            _ => Self::Failed,
        }
    }
}
/// A dial in flight for a slot: it runs on its own task so a caller giving
/// up under its own deadline neither abandons it nor makes the next caller
/// start over from the announced address (24 §24).
struct Dial {
    state: WatchReceiver,
    task: hyper_rt::TaskId,
}
/// A slot's place among the pool's connections, given back when the slot goes: slots outlive
/// their cache membership while exchanges hold them, so the count is the pool's and the slot's
/// together. The pool lives on its network's shard (27 §15: one network owner a shard, the
/// exchanges with it on that shard's tasks), so its shared counts are the shard's.
struct Reservation(Rc<AtomicUsize>);
impl Drop for Reservation {
    fn drop(&mut self) {
        let _ = self
            .0
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |held| {
                held.checked_sub(1)
            });
    }
}
/// Fresh addresses a name resolves to start this long after the announced
/// address, so an unchanged peer still answers on the address it announced
/// and a moved one is reached without waiting out the dead address.
const NAME_HEAD_START: Duration = Duration::from_millis(100);
/// At most this many fresh candidates from one resolution.
const MAX_NAME_CANDIDATES: usize = 4;
struct Slot {
    endpoint: PeerEndpoint,
    retired: AtomicBool,
    inflight: Semaphore,
    /// The lane of content: what a transfer keeps in flight to this peer.
    bulk: Semaphore,
    /// The probe lane: one liveness probe to this peer at a time.
    probes: Semaphore,
    /// Never held across an await: a retirement takes it and closes what it
    /// finds, and whoever stores a connection checks for a retirement under
    /// it, so no connection outlives its slot's retirement.
    connection: Mutex<Option<Connected>>,
    dial: Mutex<Option<Dial>>,
    generation: AtomicU64,
    /// The answers the peer has given on this slot's connections, whatever
    /// they said: a refusal is an answer. An exchange that failed while
    /// another was answered failed alone; one that failed while nothing
    /// was answered at all is no evidence the connection still carries
    /// anything.
    answered: AtomicU64,
    /// Until when a failed dial keeps this peer from being dialed again, by the shard's clock.
    unreachable_until: Mutex<Option<u64>>,
    /// What the path to the peer measured when its last answer came.
    facts: Mutex<Option<PathFacts>>,
    /// The pool's counts: a close its endpoint refused is counted there.
    counters: Rc<Counters>,
    _reservation: Reservation,
}
impl Slot {
    fn unreachable(&self) -> bool {
        self.unreachable_until
            .lock()
            .ok()
            .and_then(|until| *until)
            .is_some_and(|until| now_ns() < until)
    }
    fn clear_unreachable(&self) {
        if let Ok(mut until) = self.unreachable_until.lock() {
            *until = None;
        }
    }
    fn mark_unreachable(&self, cooldown: Duration) {
        if cooldown.is_zero() {
            return;
        }
        if let Ok(mut until) = self.unreachable_until.lock() {
            *until = Some(now_ns().saturating_add(nanos(cooldown)));
        }
    }
    fn retire(&self) {
        self.retired.store(true, Ordering::Release);
        if let Ok(mut dial) = self.dial.lock()
            && let Some(dial) = dial.take()
        {
            // From any thread: a slot is retired wherever its route is replaced.
            let _ = hyper_rt::runtime::cancel_task(dial.task);
        }
        // A poisoned lock still guards the connection to close.
        let connection = match self.connection.lock() {
            Ok(mut connection) => connection.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        };
        if let Some(connection) = connection {
            close(&connection.remote, &self.counters);
        }
    }
}
impl Drop for Slot {
    fn drop(&mut self) {
        if let Some(dial) = self.dial.get_mut().ok().and_then(|dial| dial.take()) {
            let _ = hyper_rt::runtime::cancel_task(dial.task);
        }
        let connection = match self.connection.get_mut() {
            Ok(connection) => connection.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        };
        if let Some(connection) = connection {
            close(&connection.remote, &self.counters);
        }
    }
}
#[derive(Clone, Copy, Default)]
struct Exchange {
    taken: focal_timing::ExchangeRtt,
    /// Exchanges given up on since the last one answered. Each doubles what
    /// the peer is expected to take (RFC 9002 §6.2's backoff): an estimate
    /// that made a round give up too early is not fed by the exchange it
    /// gave up on, and would otherwise never grow.
    abandoned: u32,
    /// The last bulk exchange the peer answered: its bytes and the
    /// nanoseconds it took — the rate the path showed, which sizes the
    /// next part sent it ([`PeerConnectionPool::part_bytes`]).
    delivered: Option<(u64, u64)>,
}
/// The most doublings an estimate takes.
const MAX_BACKOFF: u32 = 6;
/// The part of a chunk sent a peer at once ([`PeerConnectionPool::part_bytes`]):
/// what the path delivered in its last answered bulk exchange (`delivered`:
/// bytes, nanoseconds), stretched over `timeout`, never more than its law
/// holds in flight (`window`) over its `round_trip` stretched the same; one
/// window's worth before any bulk exchange was answered; at least a
/// datagram of the least size, at most the chunk.
pub(crate) fn part_for(
    window: u64,
    round_trip: Duration,
    delivered: Option<(u64, u64)>,
    timeout: Duration,
    chunk: usize,
) -> usize {
    let period = timeout.as_nanos();
    let law = u128::from(window)
        .saturating_mul(period)
        .checked_div(round_trip.as_nanos().max(1))
        .unwrap_or(u128::MAX);
    let carried = match delivered {
        Some((bytes, nanos)) => u128::from(bytes)
            .saturating_mul(period)
            .checked_div(u128::from(nanos.max(1)))
            .unwrap_or(u128::MAX)
            .min(law),
        None => u128::from(window).min(law),
    };
    usize::try_from(carried)
        .unwrap_or(usize::MAX)
        .clamp(
            crate::frame::LEAST_PROGRESS,
            chunk.max(crate::frame::LEAST_PROGRESS),
        )
        .min(chunk)
}
/// Whether an exchange that failed on the wire with `failure` says that its
/// connection failed, and is to be closed and dialed again: the connection
/// has `closed` already; the peer did not speak the protocol on it, or the
/// connection could not be used or trusted; or the peer `answered` nothing
/// on it — this exchange or any other — since this one was sent, which
/// leaves no evidence that it carries anything. An exchange that timed out,
/// lost its stream or could not be read while the peer answered others on
/// the same connection failed alone.
pub fn connection_failed(closed: bool, failure: &WireError, answered: bool) -> bool {
    closed
        || matches!(
            failure,
            WireError::Connection | WireError::Authentication | WireError::InvalidFrame
        )
        || !answered
}
/// One exchange in progress: given up on unless it says it was answered.
struct Asked<'a> {
    pool: &'a PeerConnectionPool,
    target: u64,
    /// Whether this operation's exchanges are measured at all.
    measured: bool,
    answered: bool,
    /// The request's bytes, and whether it is bulk: what an answered bulk
    /// exchange says the path delivered in the time it took.
    bytes: u64,
    bulk: bool,
    /// Whether it is a group's message: one that goes on its lane where its connection has one.
    replication: bool,
}
impl Asked<'_> {
    /// A group's message queued on its peer's lane: answered by nothing but its group's own
    /// answer, which its owner times (27 §8.4).
    fn queued(&mut self) {
        self.answered = true;
    }
    fn answered(&mut self, taken: Duration) {
        self.answered = true;
        if !self.measured {
            return;
        }
        if let Ok(mut state) = self.pool.state.lock()
            && state.routes.contains_key(&self.target)
        {
            let exchange = state.exchanges.entry(self.target).or_default();
            let nanos = u64::try_from(taken.as_nanos()).unwrap_or(u64::MAX);
            exchange.taken.on_sample(nanos);
            exchange.abandoned = 0;
            if self.bulk {
                exchange.delivered = Some((self.bytes, nanos.max(1)));
            }
        }
    }
    /// The peer refused: it was reached and decided, which is no sample of
    /// what an answer takes and no reason to expect it slower.
    fn refused(&mut self) {
        self.answered = true;
    }
}
impl Drop for Asked<'_> {
    fn drop(&mut self) {
        if self.answered || !self.measured {
            return;
        }
        if let Ok(mut state) = self.pool.state.lock()
            && let Some(exchange) = state.exchanges.get_mut(&self.target)
        {
            exchange.abandoned = exchange.abandoned.saturating_add(1).min(MAX_BACKOFF);
        }
    }
}
struct CacheEntry {
    // The cache and concurrent sends retain one physical connection slot;
    // removal retires it while outstanding sends keep its capacity reservation.
    slot: Rc<Slot>,
    used: u64,
}
struct State {
    revision: u64,
    routes: BTreeMap<u64, PeerEndpoint>,
    cached: BTreeMap<u64, CacheEntry>,
    /// The measured path to each routed peer (27 §3.1 P2): one estimator per
    /// peer, kept across reconnects and dropped with the peer's route.
    paths: BTreeMap<u64, focal_timing::PathRtt>,
    /// What an exchange with each routed peer takes, the peer's work
    /// included (27 §3.1 P1): one estimator per peer over the exchanges it
    /// answered, and how many in a row were given up on. Dropped with the
    /// peer's route.
    exchanges: BTreeMap<u64, Exchange>,
    clock: u64,
    closed: bool,
}

pub struct PeerConnectionPool {
    connector: QuicConnector,
    limits: PeerPoolLimits,
    state: Mutex<State>,
    inflight: Semaphore,
    probe_inflight: Semaphore,
    /// Content in flight or waiting its turn, to every peer.
    bulk_inflight: Semaphore,
    // Slots hold their place while they outlive cache membership.
    connections: Rc<AtomicUsize>,
    // A slot's detached dial records its outcome after every caller may have
    // given up; the counters outlive any one caller for the same reason the
    // connection permits do.
    counters: Rc<Counters>,
}
impl PeerConnectionPool {
    pub fn limits(&self) -> &PeerPoolLimits {
        &self.limits
    }
    /// The lane of content to one peer ([`TrafficClass::Bulk`]): the
    /// streams of a connection that nothing else can have in flight, which
    /// are all of them but those of what is asked of the peer
    /// (`per_peer_inflight`) and of its probe; one at least. A transfer goes
    /// by as many streams, since one stream carries a megabyte in a round
    /// trip, and what a group needs of the peer is never refused for the
    /// content on its way there: each stream has the megabyte of its own
    /// window in the window of the connection. Content waits its turn on
    /// the lane, in the order it came and no longer than `timeout`, so
    /// transfers to one peer share it; as much content as `max_inflight`
    /// is in flight or waits, counted apart from everything else.
    pub fn bulk_lane(&self) -> usize {
        usize::try_from(self.connector.limits().streams_per_connection)
            .unwrap_or(usize::MAX)
            .saturating_sub(self.limits.per_peer_inflight)
            .saturating_sub(1)
            .max(1)
    }
    /// By how many streams a transfer to `target` goes now
    /// ([`crate::bulk_width`]): what the law of the connection to it holds
    /// in flight decides, and one stream while there is no connection.
    pub fn bulk_width(&self, target: u64) -> usize {
        crate::bulk_width(self.window(target).unwrap_or(0), self.bulk_lane())
    }
    pub fn new(connector: QuicConnector, limits: PeerPoolLimits) -> Result<Self, PeerSendError> {
        limits.validate()?;
        // The pool's own permits are only ever tried, never waited for: one waiter each is the
        // bound the primitive asks for.
        let permits = |count: usize| {
            Semaphore::new(u64::try_from(count).unwrap_or(u64::MAX), 1)
                .map_err(|_| PeerSendError::Configuration)
        };
        Ok(Self {
            connector,
            inflight: permits(limits.max_inflight)?,
            probe_inflight: permits(limits.max_probe_inflight)?,
            bulk_inflight: permits(limits.max_inflight)?,
            connections: Rc::new(AtomicUsize::new(0)),
            limits,
            state: Mutex::new(State {
                revision: 0,
                routes: BTreeMap::new(),
                cached: BTreeMap::new(),
                paths: BTreeMap::new(),
                exchanges: BTreeMap::new(),
                clock: 0,
                closed: false,
            }),
            counters: Rc::new(Counters::default()),
        })
    }
    /// Atomically install a complete control-owner reachability snapshot. This
    /// never learns routes from replies, changes membership, or promotes voters.
    pub fn replace_routes(
        &self,
        revision: u64,
        routes: BTreeMap<u64, PeerEndpoint>,
    ) -> Result<(), PeerSendError> {
        if revision == 0
            || routes.len() > self.limits.max_routes
            || routes.iter().any(|(id, endpoint)| {
                *id == 0
                    || endpoint.address.port() == 0
                    || endpoint.server_name.len() > 253
                    || rustls::pki_types::ServerName::try_from(endpoint.server_name.clone())
                        .is_err()
                    || endpoint
                        .name
                        .as_deref()
                        .is_some_and(|name| !crate::valid_endpoint_name(name))
            })
        {
            return Err(PeerSendError::Configuration);
        }
        let mut state = self.state.lock().map_err(|_| PeerSendError::Closed)?;
        if state.closed {
            return Err(PeerSendError::Closed);
        }
        if revision < state.revision || (revision == state.revision && routes != state.routes) {
            return Err(PeerSendError::StaleRoutes);
        }
        if revision == state.revision {
            return Ok(());
        }
        state.cached.retain(|id, entry| {
            if routes.get(id) == Some(&entry.slot.endpoint) {
                true
            } else {
                entry.slot.retire();
                false
            }
        });
        state.paths.retain(|id, _| routes.contains_key(id));
        state.exchanges.retain(|id, _| routes.contains_key(id));
        state.revision = revision;
        state.routes = routes;
        Ok(())
    }
    /// The measured path to `target`: round trips of the liveness probes it
    /// answered on an open connection. `None` for a peer with no route; a routed peer
    /// that has answered nothing yet has a path with no sample.
    pub fn path(&self, target: u64) -> Option<focal_timing::PathRtt> {
        let state = self.state.lock().ok()?;
        state
            .routes
            .contains_key(&target)
            .then(|| state.paths.get(&target).copied().unwrap_or_default())
    }
    /// What the open connection to `target` holds in flight, in bytes: its
    /// congestion window, the law's estimate of what the path carries
    /// before it answers. `None` for a peer with no connection open: there
    /// is no estimate of a path nothing was sent on.
    pub fn window(&self, target: u64) -> Option<u64> {
        self.facts(target).map(|facts| facts.cwnd)
    }
    /// What the path to `target` measures now, read from its open connection; `None` while no
    /// connection to it is open. [`Self::window`] gives what it measured when its connection last
    /// answered; this asks the connection (whether its path's warm-up is still to come,
    /// `PathFacts::warming_up`, for one).
    pub async fn path_now(&self, target: u64) -> Option<PathFacts> {
        let slot = self
            .state
            .lock()
            .ok()
            .and_then(|state| state.cached.get(&target).map(|entry| entry.slot.clone()))?;
        let remote = slot
            .connection
            .lock()
            .ok()?
            .as_ref()
            .map(|entry| entry.remote.clone())?;
        remote.path().await
    }
    /// What the path to `target` measured when its connection last answered; `None` while no
    /// connection to it has.
    fn facts(&self, target: u64) -> Option<PathFacts> {
        let slot = {
            let state = self.state.lock().ok()?;
            state.cached.get(&target)?.slot.clone()
        };
        let open = slot.connection.lock().ok()?.is_some();
        let facts = *slot.facts.lock().ok()?;
        facts.filter(|_| open)
    }
    /// The profile the open connection to `target` negotiated; `None`
    /// while there is none. What a sender reads before it stamps a frame
    /// with an order ([`Operation::RaftOrdered`]): a peer whose connection
    /// does not admit the ordered profile — an older binary, a first
    /// contact not yet dialled — is sent plain `Raft`.
    pub fn peer_protocol(&self, target: u64) -> Option<u16> {
        let slot = self
            .state
            .lock()
            .ok()
            .and_then(|state| state.cached.get(&target).map(|entry| entry.slot.clone()))?;
        let cached = slot.connection.lock().ok()?;
        cached
            .as_ref()
            .map(|entry| entry.remote.negotiated().protocol)
    }
    /// The profile the connection to `target` negotiated, the connection
    /// dialled where there is none, within the pool's deadline: what a
    /// sender reads before it asks a copy what it holds (the audit's F50),
    /// an ask of the ordered profile a copy of an older binary cannot
    /// answer.
    pub async fn negotiated_with(&self, target: u64) -> Result<u16, PeerSendError> {
        let slot = self.slot(target)?;
        let (_, remote) =
            hyper_rt::futures::within(nanos(self.limits.timeout), self.connection(&slot, false))
                .await
                .map_err(|_| PeerSendError::Lost)?
                .ok_or(PeerSendError::Lost)??;
        Ok(remote.negotiated().protocol)
    }
    /// The round trip of the connection to `target`, as it measures it;
    /// `None` while there is none.
    pub fn round_trip(&self, target: u64) -> Option<Duration> {
        self.facts(target).map(|facts| facts.rtt)
    }
    /// How much of a chunk of `chunk` bytes to send `target` at once: what
    /// the path to it delivered, in the time the pool gives an exchange
    /// (`PeerPoolLimits::timeout`) — a datagram at least, the chunk at most.
    /// A part so sized crosses within one exchange time at the rate the path
    /// showed, and the lease a receiver holds a transfer under, which every
    /// part renews, outlives many of them (the audit's F49: a chunk that
    /// takes its path longer than the lease arrived to a transfer that had
    /// expired). The rate is what the last bulk exchange the peer answered
    /// delivered in the time it took, never more than the law holds in
    /// flight over a round trip; a path that has answered no bulk exchange
    /// yet is sent one window's worth, what it is known to accept in
    /// flight. It was the law's estimate alone — the window over the round
    /// trip, both cold — stretched over the whole exchange time: the first
    /// part to a copy across 128 kbit/s was 307 KiB, twenty seconds on the
    /// path, and the part that failed under a loaded gate run was 700 KiB
    /// sent again from where the copy held it (`evidence_quic`, 1.68
    /// chunks crossing for one). Where the path is not yet measured at all,
    /// the chunk goes whole, as it did. [`part_for`] is the rule.
    pub fn part_bytes(&self, target: u64, chunk: usize) -> usize {
        let Some(window) = self.window(target) else {
            return chunk;
        };
        let Some(round_trip) = self.round_trip(target) else {
            return chunk;
        };
        let delivered = self.state.lock().ok().and_then(|state| {
            state
                .exchanges
                .get(&target)
                .and_then(|exchange| exchange.delivered)
        });
        part_for(window, round_trip, delivered, self.limits.timeout, chunk)
    }
    /// What an exchange with `target` is expected to take, its work
    /// included: the tail of the exchanges it answered, doubled for each
    /// one given up on since. `None` while it has answered none.
    pub fn exchange_tail(&self, target: u64) -> Option<Duration> {
        let state = self.state.lock().ok()?;
        let exchange = state.exchanges.get(&target)?;
        let tail = exchange.taken.tail_ns()?;
        let factor = 1u64.checked_shl(exchange.abandoned.min(MAX_BACKOFF))?;
        Some(Duration::from_nanos(tail.saturating_mul(factor)))
    }
    /// How long one of `asked` peers that are asked one after another
    /// within `round` is waited for: its share of the round, so that each
    /// of them can be asked, and what an exchange with it is expected to
    /// take where that is more ([`Self::exchange_tail`]); the pool's own
    /// deadline at most. A peer across the planet is waited for as long
    /// as it takes to answer, one that stopped answering twice as long
    /// each time, and one nothing is measured of yet its share: a directed
    /// search hands its request on, and a peer given the whole round would
    /// leave the others unasked.
    pub fn exchange_wait(&self, target: u64, round: Duration, asked: usize) -> Duration {
        let share = round
            .checked_div(u32::try_from(asked.max(1)).unwrap_or(u32::MAX))
            .unwrap_or(round);
        self.exchange_tail(target)
            .map_or(share, |tail| tail.max(share))
            .min(self.limits.timeout)
    }
    /// The budget of a round that asks `targets` (27 §3.1 P1): derived from
    /// the slowest of them, and the pool's own deadline where one of them
    /// has answered nothing yet. Never longer than that deadline.
    pub fn round_budget(
        &self,
        targets: impl IntoIterator<Item = u64>,
        period: Duration,
    ) -> focal_timing::RoundBudget {
        let mut slowest = Some(Duration::ZERO);
        for target in targets {
            slowest = match (slowest, self.exchange_tail(target)) {
                (Some(slowest), Some(tail)) => Some(slowest.max(tail)),
                _ => None,
            };
        }
        focal_timing::RoundBudget::derive(period, slowest, self.limits.timeout)
    }
    /// Karn's rule: only an exchange the peer answered is a sample, timed on
    /// a connection already open, so neither a dial nor a lost request
    /// enters the estimate.
    fn observe(&self, target: u64, round_trip: Duration) {
        if let Ok(mut state) = self.state.lock()
            && state.routes.contains_key(&target)
        {
            state
                .paths
                .entry(target)
                .or_default()
                .on_sample(u64::try_from(round_trip.as_nanos()).unwrap_or(u64::MAX));
        }
    }
    /// Present a renewed credential on every connection opened from now on.
    /// Cached connections under the previous certificate are retired, so the
    /// next send to each peer opens a connection the peer authorizes anew.
    pub async fn replace_identity(&self, tls: ClientTls) -> Result<(), PeerSendError> {
        self.connector
            .replace_tls(tls)
            .await
            .map_err(|_| PeerSendError::Configuration)?;
        let mut state = self.state.lock().map_err(|_| PeerSendError::Closed)?;
        if state.closed {
            return Err(PeerSendError::Closed);
        }
        for (_, entry) in std::mem::take(&mut state.cached) {
            entry.slot.retire();
        }
        Ok(())
    }
    /// There is no internal packet queue. Global/per-peer permits reject excess
    /// work immediately; the FleetHost owns its separate bounded egress queue.
    /// Ok means ingress accepted this packet, never a quorum/durability signal.
    /// Queued on its lane where the connection has one ([`Delivery::Queued`]).
    pub async fn send(&self, target: u64, request: &RequestEnvelope) -> Result<(), PeerSendError> {
        self.send_as(target, request, Delivery::Queued).await
    }
    /// A group's message, carried as `delivery` says.
    pub async fn send_as(
        &self,
        target: u64,
        request: &RequestEnvelope,
        delivery: Delivery,
    ) -> Result<(), PeerSendError> {
        if !matches!(
            request.operation,
            Operation::Raft { .. } | Operation::RaftOrdered { .. }
        ) {
            return Err(PeerSendError::InvalidRequest);
        }
        match self
            .exchange_as(target, request, delivery == Delivery::Queued)
            .await?
        {
            Response::PeerAccepted => Ok(()),
            _ => Err(PeerSendError::InvalidRequest),
        }
    }
    /// Authenticated, bounded, same-request retries for exact content custody.
    /// A Durable reply proves only the addressed peer's local disk contract.
    pub async fn send_custody(
        &self,
        target: u64,
        request: &RequestEnvelope,
    ) -> Result<CustodyReply, PeerSendError> {
        if !matches!(request.operation, Operation::Custody(_)) {
            return Err(PeerSendError::InvalidRequest);
        }
        match self.exchange(target, request).await? {
            Response::Custody(reply) => Ok(reply),
            _ => Err(PeerSendError::InvalidRequest),
        }
    }
    /// Only the installed, trusted route table is enumerated. No response can
    /// add an endpoint. A strictly increasing cursor bounds changing snapshots
    /// without allocating a second route table.
    /// The installed reachability of one peer, for a redirect hint.
    pub fn route_endpoint(&self, node: u64) -> Result<Option<PeerEndpoint>, PeerSendError> {
        let state = self.state.lock().map_err(|_| PeerSendError::Closed)?;
        if state.closed {
            return Err(PeerSendError::Closed);
        }
        Ok(state.routes.get(&node).cloned())
    }
    pub fn next_route_target(&self, after: u64) -> Result<Option<u64>, PeerSendError> {
        let state = self.state.lock().map_err(|_| PeerSendError::Closed)?;
        if state.closed {
            return Err(PeerSendError::Closed);
        }
        Ok(state
            .routes
            .range((std::ops::Bound::Excluded(after), std::ops::Bound::Unbounded))
            .next()
            .map(|(node, _)| *node))
    }
    /// Node-only discovery/contact RPCs. Owner-side authorization remains
    /// mandatory; routing is neither a membership grant nor Runtime authority.
    pub async fn send_peer_control(
        &self,
        target: u64,
        request: &RequestEnvelope,
    ) -> Result<Vec<u8>, PeerSendError> {
        if !matches!(
            request.operation,
            Operation::PeerControl { .. } | Operation::NodeContact { .. }
        ) {
            return Err(PeerSendError::InvalidRequest);
        }
        match self.exchange(target, request).await? {
            Response::Control { response } => Ok(response),
            _ => Err(PeerSendError::InvalidRequest),
        }
    }
    /// Placement protocol to a partition owner or a session-fact signature
    /// from a peer; both reply with opaque control bytes the caller decodes.
    pub async fn send_placement(
        &self,
        target: u64,
        request: &RequestEnvelope,
    ) -> Result<Vec<u8>, PeerSendError> {
        if !matches!(
            request.operation,
            Operation::PlacementControl { .. }
                | Operation::SessionSign { .. }
                | Operation::RangeControl { .. }
                | Operation::SessionControl { .. }
        ) {
            return Err(PeerSendError::InvalidRequest);
        }
        match self.exchange(target, request).await? {
            Response::Control { response } => Ok(response),
            Response::Error(error) => Err(PeerSendError::Rejected(error)),
            _ => Err(PeerSendError::InvalidRequest),
        }
    }
    /// A liveness probe to `target` at exactly `address`, on a connection
    /// opened for it and closed after it: never the installed route, never
    /// a re-resolved name (24 §24). The certificate check is the route's.
    pub async fn probe_at(
        &self,
        target: u64,
        address: SocketAddr,
        request: &RequestEnvelope,
    ) -> Result<Vec<u8>, PeerSendError> {
        if !matches!(request.operation, Operation::Probe { .. }) || target == 0 {
            return Err(PeerSendError::InvalidRequest);
        }
        let server_name = {
            let state = self.state.lock().map_err(|_| PeerSendError::Closed)?;
            if state.closed {
                return Err(PeerSendError::Closed);
            }
            state
                .routes
                .get(&target)
                .map(|endpoint| endpoint.server_name.clone())
                .ok_or(PeerSendError::NoRoute)?
        };
        let _inflight = self
            .probe_inflight
            .try_acquire()
            .ok_or(PeerSendError::Busy)?;
        hyper_rt::futures::within(nanos(self.limits.timeout), async {
            // A connection of the probe's alone: the route's to this address, if one is up,
            // stays the route's, and this one is closed after.
            let remote = self
                .connector
                .connect_alone(address, &server_name)
                .await
                .map_err(|_| PeerSendError::Lost)?;
            let result = match remote.request(request).await {
                Ok(response) => match response.result {
                    Response::Probe(reply) => Ok(reply),
                    Response::Error(error) => Err(PeerSendError::Rejected(error)),
                    _ => Err(PeerSendError::Lost),
                },
                Err(WireError::Access(error)) => Err(PeerSendError::Rejected(error)),
                Err(_) => Err(PeerSendError::Lost),
            };
            close(&remote, &self.counters);
            result
        })
        .await
        .map_err(|_| PeerSendError::Lost)?
        .ok_or(PeerSendError::Lost)?
    }
    /// A liveness probe; the reply is the peer's opaque probe reply.
    pub async fn send_probe(
        &self,
        target: u64,
        request: &RequestEnvelope,
    ) -> Result<Vec<u8>, PeerSendError> {
        if !matches!(request.operation, Operation::Probe { .. }) {
            return Err(PeerSendError::InvalidRequest);
        }
        match self.exchange(target, request).await? {
            Response::Probe(reply) => Ok(reply),
            Response::Error(error) => Err(PeerSendError::Rejected(error)),
            _ => Err(PeerSendError::InvalidRequest),
        }
    }
    pub async fn send_managed_support(
        &self,
        target: u64,
        request: &RequestEnvelope,
    ) -> Result<focal_model::ManagedFormatSupport, PeerSendError> {
        if !matches!(request.operation, Operation::ManagedSupport { .. }) {
            return Err(PeerSendError::InvalidRequest);
        }
        match self.exchange(target, request).await? {
            Response::ManagedSupport(fact) if fact.node == target => Ok(fact),
            _ => Err(PeerSendError::InvalidRequest),
        }
    }
    pub async fn send_enrollment_control(
        &self,
        target: u64,
        request: &RequestEnvelope,
    ) -> Result<Vec<u8>, PeerSendError> {
        if !matches!(request.operation, Operation::EnrollmentControl { .. }) {
            return Err(PeerSendError::InvalidRequest);
        }
        match self.exchange(target, request).await? {
            Response::Control { response } => Ok(response),
            _ => Err(PeerSendError::InvalidRequest),
        }
    }
    async fn exchange(
        &self,
        target: u64,
        request: &RequestEnvelope,
    ) -> Result<Response, PeerSendError> {
        self.exchange_as(target, request, false).await
    }
    /// An exchange with `target`, or a group's message queued on its lane where `laned` and its
    /// connection has one.
    async fn exchange_as(
        &self,
        target: u64,
        request: &RequestEnvelope,
        laned: bool,
    ) -> Result<Response, PeerSendError> {
        if hyper_rt::futures::shard_id().is_none() {
            return Err(PeerSendError::Lost);
        }
        let result = self.send_bounded(target, request, laned).await;
        match &result {
            Ok(_) => {
                increment(&self.counters.delivered);
            }
            Err(PeerSendError::Busy) => {
                increment(&self.counters.busy);
            }
            Err(error) => {
                increment(&self.counters.lost);
                match error {
                    PeerSendError::Rejected(AccessError::Capacity) => {
                        increment(&self.counters.lost_refused);
                    }
                    PeerSendError::Rejected(AccessError::OutcomeUnknown) => {
                        increment(&self.counters.lost_unknown);
                    }
                    PeerSendError::Rejected(AccessError::Unavailable) => {
                        increment(&self.counters.lost_unavailable);
                    }
                    PeerSendError::Rejected(_) => increment(&self.counters.lost_rejected),
                    _ => {}
                }
            }
        }
        result
    }
    async fn send_bounded(
        &self,
        target: u64,
        request: &RequestEnvelope,
        laned: bool,
    ) -> Result<Response, PeerSendError> {
        let valid_operation = match &request.operation {
            Operation::Raft { group, message } => *group != [0; 16] && !message.is_empty(),
            Operation::RaftOrdered { group, message, .. } => {
                *group != [0; 16] && !message.is_empty()
            }
            Operation::Custody(_) => true,
            Operation::ManagedSupport { group } => *group != [0; 16],
            Operation::PeerControl { group, request } => {
                *group != [0; 16]
                    && !request.is_empty()
                    && request.len() <= MAX_PEER_CONTROL_REQUEST_BYTES
            }
            Operation::PlacementControl { group, request } => {
                *group != [0; 16]
                    && !request.is_empty()
                    && request.len() <= MAX_PLACEMENT_CONTROL_REQUEST_BYTES
            }
            Operation::SessionSign { group, request } => {
                *group != [0; 16]
                    && !request.is_empty()
                    && request.len() <= MAX_SESSION_SIGN_REQUEST_BYTES
            }
            Operation::RangeControl { group, request } => {
                *group != [0; 16]
                    && !request.is_empty()
                    && request.len() <= MAX_RANGE_CONTROL_REQUEST_BYTES
            }
            Operation::SessionControl { group, request } => {
                *group != [0; 16]
                    && !request.is_empty()
                    && request.len() <= MAX_SESSION_CONTROL_REQUEST_BYTES
            }
            Operation::Probe { request } => !request.is_empty() && request.len() <= MAX_PROBE_BYTES,
            Operation::NodeContact { group, .. } => *group != [0; 16],
            Operation::EnrollmentControl {
                group,
                genesis,
                request,
            } => {
                *group != [0; 16]
                    && *genesis != [0; 32]
                    && !request.is_empty()
                    && request.len() <= MAX_ENROLLMENT_CONTROL_REQUEST_BYTES
            }
            _ => false,
        };
        if target == 0
            || !valid_operation
            || request.protocol
                != match request.operation {
                    Operation::ManagedSupport { .. } => MANAGED_PROTOCOL_VERSION,
                    Operation::RaftOrdered { .. }
                    | Operation::Custody(CustodyRequest::OpenHeld { .. }) => {
                        ORDERED_PROTOCOL_VERSION
                    }
                    _ => PROTOCOL_VERSION,
                }
            || request.request_epoch.0 == 0
            || request.request_id.is_zero()
        {
            return Err(PeerSendError::InvalidRequest);
        }
        let bytes = postcard::experimental::serialized_size(request)
            .map_err(|_| PeerSendError::InvalidRequest)?;
        if bytes > self.connector.limits().max_frame_bytes as usize {
            return Err(PeerSendError::InvalidRequest);
        }
        let probe = matches!(request.operation, Operation::Probe { .. });
        let bulk = !probe && request.operation.class() == TrafficClass::Bulk;
        // Replication and probes measure the path; every other exchange
        // measures what the peer takes to answer it.
        let mut asked = Asked {
            pool: self,
            target,
            measured: !probe
                && !matches!(
                    request.operation,
                    Operation::Raft { .. } | Operation::RaftOrdered { .. }
                ),
            answered: false,
            bytes: u64::try_from(bytes).unwrap_or(u64::MAX),
            bulk,
            replication: matches!(
                request.operation,
                Operation::Raft { .. } | Operation::RaftOrdered { .. }
            ),
        };
        let _inflight = if probe {
            &self.probe_inflight
        } else if bulk {
            &self.bulk_inflight
        } else {
            &self.inflight
        }
        .try_acquire()
        .ok_or(PeerSendError::Busy)?;
        // Content waits its turn; the time it has to be exchanged in
        // begins once it has it.
        let waited = if bulk { Some(self.slot(target)?) } else { None };
        let _turn = match &waited {
            Some(slot) => Some(
                hyper_rt::futures::within(nanos(self.limits.timeout), slot.bulk.acquire())
                    .await
                    .map_err(|_| PeerSendError::Busy)?
                    .ok_or(PeerSendError::Busy)?
                    .map_err(|_| PeerSendError::Closed)?,
            ),
            None => None,
        };
        let exchange = async {
            for attempt in 0..self.limits.attempts {
                let slot = self.slot(target)?;
                // A route that changed has a lane of its own, and the turn
                // that was waited for is none on it.
                let _peer = if probe {
                    Some(slot.probes.try_acquire().ok_or(PeerSendError::Busy)?)
                } else if !bulk {
                    // A group's message waits its turn on the peer's lane,
                    // no longer than the exchange's time: a burst is carried
                    // in order, never refused for the lane being full at
                    // that instant, which cost the group a whole election
                    // timeout for a vote and its pipeline for an append.
                    Some(
                        hyper_rt::futures::within(
                            nanos(self.limits.timeout),
                            slot.inflight.acquire(),
                        )
                        .await
                        .map_err(|_| PeerSendError::Busy)?
                        .ok_or(PeerSendError::Busy)?
                        .map_err(|_| PeerSendError::Closed)?,
                    )
                } else if waited.as_ref().is_some_and(|had| Rc::ptr_eq(had, &slot)) {
                    None
                } else {
                    Some(slot.bulk.try_acquire().ok_or(PeerSendError::Busy)?)
                };
                let connected = match hyper_rt::futures::within(
                    nanos(self.limits.timeout),
                    self.connection(&slot, probe),
                )
                .await
                {
                    Ok(Some(connected)) => connected,
                    // A group's exchange has waited its time for a
                    // connection, and is not made to wait it again: the
                    // dial goes on, for whoever asks next.
                    Ok(None) | Err(_) if !bulk => return Err(PeerSendError::Lost),
                    Ok(None) | Err(_) => Err(PeerSendError::Lost),
                };
                let (generation, remote) = match connected {
                    Ok(connection) => connection,
                    Err(error) => {
                        if attempt.saturating_add(1) == self.limits.attempts {
                            return Err(error);
                        }
                        pause(spread(self.limits.retry_backoff, entropy())).await;
                        continue;
                    }
                };
                let sent = now_ns();
                let before = slot.answered.load(Ordering::Acquire);
                // An ordered frame goes as it is on a connection that admits
                // the ordered profile, and as a plain frame on one that does
                // not — a peer of an older binary (27 §12). The plain copy
                // is the one copy a mixed window costs, within the frame's
                // charge; a connection admitting the profile copies nothing.
                let plain = match &request.operation {
                    Operation::RaftOrdered { group, message, .. }
                        if remote.negotiated().protocol < ORDERED_PROTOCOL_VERSION =>
                    {
                        let mut plain = Vec::new();
                        plain
                            .try_reserve_exact(message.len())
                            .map_err(|_| PeerSendError::Busy)?;
                        plain.extend_from_slice(message);
                        Some(RequestEnvelope {
                            protocol: PROTOCOL_VERSION,
                            ledger: request.ledger,
                            route_epoch: request.route_epoch,
                            request_epoch: request.request_epoch,
                            request_id: request.request_id,
                            operation: Operation::Raft {
                                group: *group,
                                message: plain,
                            },
                        })
                    }
                    _ => None,
                };
                // A group's message to a peer that speaks the laned profile goes on its lane,
                // queued behind the lane's earlier frames and answered by nothing: what a group
                // cannot do without on the urgent lane, its bulk appends on the bulk lane, in
                // the order they left. Queued is all its sender hears; a frame its connection
                // took down with it is a message lost, which its group finds and asks again.
                if laned && asked.replication && remote.negotiated().laned() {
                    let lane = if matches!(request.operation, Operation::RaftOrdered { .. }) {
                        crate::transport::BULK_LANE
                    } else {
                        crate::transport::URGENT_LANE
                    };
                    // A full lane holds the frame until it has room, no longer than an exchange
                    // is given: past it the lane's peer takes nothing, and the frame is given up
                    // as an unanswered exchange is.
                    let queued = match hyper_rt::futures::within(
                        nanos(self.limits.timeout),
                        remote.send_lane(lane, request),
                    )
                    .await
                    {
                        Ok(Some(queued)) => queued,
                        Ok(None) | Err(_) => Err(WireError::Timeout),
                    };
                    match queued {
                        Ok(path) => {
                            // What the path measures comes with it, as with an answer: the
                            // window a leader sends its member ahead of its answers follows it
                            // (27 §11). A frame queued is no evidence its peer answers.
                            if path.is_some()
                                && let Ok(mut facts) = slot.facts.lock()
                            {
                                *facts = path;
                            }
                            asked.queued();
                            return Ok(Response::PeerAccepted);
                        }
                        Err(WireError::Access(AccessError::Capacity)) => {
                            asked.refused();
                            return Err(PeerSendError::Busy);
                        }
                        // The connection is gone, and its lanes with it: it is closed and
                        // forgotten as an exchange's failed connection is, so the next attempt
                        // dials its successor.
                        Err(WireError::Connection) => {
                            asked.refused();
                            close(&remote, &slot.counters);
                            if let Ok(mut cached) = slot.connection.lock()
                                && cached
                                    .as_ref()
                                    .is_some_and(|entry| entry.generation == generation)
                            {
                                *cached = None;
                            }
                            if attempt.saturating_add(1) == self.limits.attempts {
                                return Err(PeerSendError::Lost);
                            }
                            pause(spread(self.limits.retry_backoff, entropy())).await;
                            continue;
                        }
                        // The frame itself could not be queued (its encoding, its size): asked
                        // again it could not be either.
                        Err(_) => {
                            asked.refused();
                            return Err(PeerSendError::Lost);
                        }
                    }
                }
                let answered = remote
                    .exchange_within(plain.as_ref().unwrap_or(request), self.limits.timeout)
                    .await;
                let taken = || Duration::from_nanos(now_ns().saturating_sub(sent));
                // What kept this exchange from an answer, when the
                // connection itself is to be judged for it.
                let (failure, closed) = match answered {
                    Ok(Exchanged { response, path }) => {
                        // The peer answered on this connection, whatever it
                        // said; what the path measured comes with it.
                        slot.answered.fetch_add(1, Ordering::AcqRel);
                        if path.is_some()
                            && let Ok(mut facts) = slot.facts.lock()
                        {
                            *facts = path;
                        }
                        match response.result {
                            value @ (Response::PeerAccepted
                            | Response::Custody(_)
                            | Response::Control { .. }
                            | Response::ManagedSupport(_)
                            | Response::Probe(_)) => {
                                if slot.retired.load(Ordering::Acquire) {
                                    asked.refused();
                                    return Err(PeerSendError::RouteChanged);
                                }
                                asked.answered(taken());
                                // The path is measured by probes alone: the
                                // peer's liveness driver answers one without
                                // its replicas' owners. A replication message
                                // is answered once the peer has persisted it,
                                // and a peer that has just restarted answers
                                // its first one seconds late; a control request
                                // waits on a quorum commit. Neither is the path.
                                if matches!(request.operation, Operation::Probe { .. }) {
                                    self.observe(target, taken());
                                }
                                return Ok(value);
                            }
                            // What was asked for is not to be had now, or
                            // what became of it is not known. The peer said
                            // so over a connection that carried the question
                            // and the answer: the connection is kept, with
                            // everything else it carries (the audit's F37:
                            // it was closed, and every exchange with the
                            // peer was lost with this one). The same request
                            // is asked again on it, and what the peer last
                            // said is what the caller is told.
                            Response::Error(
                                error @ (AccessError::Unavailable | AccessError::OutcomeUnknown),
                            ) => {
                                asked.refused();
                                if attempt.saturating_add(1) == self.limits.attempts {
                                    return Err(PeerSendError::Rejected(error));
                                }
                                pause(spread(self.limits.retry_backoff, entropy())).await;
                                continue;
                            }
                            Response::Error(error) => {
                                asked.refused();
                                return Err(PeerSendError::Rejected(error));
                            }
                            _ => return Err(PeerSendError::Lost),
                        }
                    }
                    Err(Unanswered {
                        error: WireError::Limit,
                        ..
                    }) => return Err(PeerSendError::Busy),
                    Err(Unanswered {
                        error: WireError::Access(error),
                        ..
                    }) => return Err(PeerSendError::Rejected(error)),
                    Err(Unanswered { error, closed }) => (error, closed),
                };
                // The exchange failed on the wire. The connection is closed
                // for it when the connection is what failed: it is closed
                // already, the peer did not speak the protocol on it, or it
                // answered nothing at all — this exchange or any other —
                // from the time this one was sent. An exchange that timed
                // out or lost its stream while the peer answered others on
                // the same connection failed alone, and takes nothing with
                // it.
                let answered = slot.answered.load(Ordering::Acquire) != before;
                if connection_failed(closed, &failure, answered) {
                    close(&remote, &slot.counters);
                    if let Ok(mut cached) = slot.connection.lock()
                        && cached
                            .as_ref()
                            .is_some_and(|entry| entry.generation == generation)
                    {
                        *cached = None;
                    }
                }
                // A group's exchange whose peer was given its time and did
                // not answer in it is not asked again here: its second time
                // would hold the peer's lane for as long again, and its
                // owner asks again by its own clock. (One time for all the
                // attempts used to end them together.) Content is asked
                // again, as it was: its sender finds the copy's room by it.
                if !bulk && matches!(failure, WireError::Timeout) {
                    return Err(PeerSendError::Lost);
                }
                if attempt.saturating_add(1) < self.limits.attempts {
                    pause(spread(self.limits.retry_backoff, entropy())).await;
                }
            }
            Err(PeerSendError::Lost)
        };
        // Each part of an exchange has a wait of its own: its turn on the
        // lane, its connection, what it sends for as long as the path
        // carries it, its peer's answer, and what the answer brings
        // (`QuicRemote::request_within`). One time for the whole of it, the
        // same for a vote and for four megabytes of entries, gave a path
        // that carries less than a message in that time none of the
        // message (the audit's F36).
        exchange.await
    }
    fn slot(&self, target: u64) -> Result<Rc<Slot>, PeerSendError> {
        let mut state = self.state.lock().map_err(|_| PeerSendError::Closed)?;
        if state.closed {
            return Err(PeerSendError::Closed);
        }
        // Validate the route without cloning the endpoint; the steady-state
        // cache-hit path below returns without materializing it.
        if !state.routes.contains_key(&target) {
            return Err(PeerSendError::NoRoute);
        }
        state.clock = state.clock.saturating_add(1);
        let clock = state.clock;
        if let Some(entry) = state.cached.get_mut(&target) {
            entry.used = clock;
            return Ok(entry.slot.clone());
        }
        // Cache miss only: clone the endpoint the connection open needs.
        let endpoint = state
            .routes
            .get(&target)
            .cloned()
            .ok_or(PeerSendError::NoRoute)?;
        if self.connections.load(Ordering::Acquire) >= self.limits.max_connections {
            let victim = state
                .cached
                .iter()
                .filter(|(_, entry)| Rc::strong_count(&entry.slot) == 1)
                .min_by_key(|(_, entry)| entry.used)
                .map(|(id, _)| *id);
            if let Some(victim) = victim
                && let Some(entry) = state.cached.remove(&victim)
            {
                entry.slot.retire();
            }
        }
        let max = self.limits.max_connections;
        self.connections
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |held| {
                (held < max).then(|| held.saturating_add(1))
            })
            .map_err(|_| PeerSendError::Busy)?;
        let reservation = Reservation(self.connections.clone());
        // A lane's waiters are bounded by the pool's own permits: each holds one of the pool's in
        // flight while it waits for the peer's turn.
        let lane = |permits: usize| {
            Semaphore::new(
                u64::try_from(permits).unwrap_or(u64::MAX),
                self.limits.max_inflight,
            )
            .map_err(|_| PeerSendError::Busy)
        };
        let slot = Rc::new(Slot {
            endpoint,
            retired: AtomicBool::new(false),
            inflight: lane(self.limits.per_peer_inflight)?,
            bulk: lane(self.bulk_lane())?,
            probes: lane(1)?,
            connection: Mutex::new(None),
            dial: Mutex::new(None),
            generation: AtomicU64::new(0),
            answered: AtomicU64::new(0),
            unreachable_until: Mutex::new(None),
            facts: Mutex::new(None),
            counters: self.counters.clone(),
            _reservation: reservation,
        });
        state.cached.insert(
            target,
            CacheEntry {
                slot: slot.clone(),
                used: clock,
            },
        );
        Ok(slot)
    }
    /// The connection to `slot`'s peer, dialed where there is none. A
    /// liveness probe (`probe`) is never refused for a cooldown: a send
    /// refused here never reached the peer, so for a failure detector it is
    /// no probe at all (SWIM's failure is a probe sent and not answered in
    /// time), and the probe is how a peer that came back is found again.
    /// Probes have lanes of their own, so dialing for one takes nothing
    /// from the capacity the cooldown protects.
    async fn connection(
        &self,
        slot: &Rc<Slot>,
        probe: bool,
    ) -> Result<(u64, QuicRemote), PeerSendError> {
        // A peer whose dial just failed is not dialed again until its cooldown
        // passes: the send fails at once rather than holding its permits for
        // another dial deadline, so unreachable peers never consume the
        // capacity reachable ones need.
        if !probe && slot.unreachable() {
            increment(&self.counters.unreachable);
            return Err(PeerSendError::Lost);
        }
        {
            let cached = slot.connection.lock().map_err(|_| PeerSendError::Closed)?;
            if slot.retired.load(Ordering::Acquire) {
                return Err(PeerSendError::RouteChanged);
            }
            if let Some(connection) = &*cached {
                return Ok((connection.generation, connection.remote.clone()));
            }
        }
        // No connection: join the slot's dial in flight, or start one. The
        // dial is a task of its own, so this caller giving up under its own
        // deadline (a liveness probe, a bounded control read) neither
        // abandons it nor makes the next caller start over from the
        // announced address; whichever caller is still waiting when the dial
        // decides sees the same outcome.
        let mut dial = self.dial(slot)?;
        loop {
            match DialState::of(dial.borrow_and_update()) {
                DialState::Pending => dial.changed().await.map_err(|_| PeerSendError::Lost)?,
                DialState::Failed => return Err(PeerSendError::Lost),
                DialState::Connected => break,
            }
        }
        let cached = slot.connection.lock().map_err(|_| PeerSendError::Closed)?;
        if slot.retired.load(Ordering::Acquire) {
            return Err(PeerSendError::RouteChanged);
        }
        match &*cached {
            Some(connection) => Ok((connection.generation, connection.remote.clone())),
            None => Err(PeerSendError::Lost),
        }
    }
    /// The slot's dial in flight, or a new one: one dial per slot at a time,
    /// counted once however many callers wait on it.
    fn dial(&self, slot: &Rc<Slot>) -> Result<WatchReceiver, PeerSendError> {
        let mut current = slot.dial.lock().map_err(|_| PeerSendError::Closed)?;
        if let Some(dial) = current.as_ref()
            && DialState::of(dial.state.borrow()) == DialState::Pending
        {
            // Its waiters are the callers in flight to the peer, at most the pool's permits.
            return dial.state.try_clone().map_err(|_| PeerSendError::Busy);
        }
        if slot.retired.load(Ordering::Acquire) {
            return Err(PeerSendError::RouteChanged);
        }
        let dialer = self.connector.dialer().map_err(|_| PeerSendError::Closed)?;
        // A receiver for every caller that may wait on the dial, and the one the slot keeps.
        let waiters = self
            .limits
            .max_inflight
            .saturating_add(self.limits.max_probe_inflight)
            .saturating_add(1);
        let (mut sender, receiver) =
            watch(DialState::Pending.word(), waiters).map_err(|_| PeerSendError::Busy)?;
        let waiting = receiver.try_clone().map_err(|_| PeerSendError::Busy)?;
        increment(&self.counters.dials);
        let task_slot = slot.clone();
        let counters = self.counters.clone();
        // A dial is given what the connector gives its two parts, the
        // handshake of the connection and the one of the protocol on it
        // (`open_remote`), and no less for being asked for by an exchange:
        // a caller waits for it its own time and no longer, and the dial
        // that outlives it is the next caller's connection.
        let timeout = self.connector.limits().request_timeout.saturating_mul(2);
        let cooldown = self.limits.unreachable_cooldown;
        let task = hyper_rt::futures::spawn_detached(async move {
            let endpoint = task_slot.endpoint.clone();
            let (outcome, rest) =
                dial_candidates(dialer, endpoint, timeout, counters.clone()).await;
            let state = match outcome {
                Ok(remote) if task_slot.retired.load(Ordering::Acquire) => {
                    close(&remote, &counters);
                    DialState::Failed
                }
                Ok(remote) => {
                    match task_slot.generation.fetch_update(
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                        |value| value.checked_add(1),
                    ) {
                        // Stored under the lock a retirement takes, and
                        // only while the slot is not retired: a connection
                        // that finished its handshake as its route was
                        // replaced is closed here and never used.
                        Ok(generation) => match task_slot.connection.lock() {
                            Ok(mut cached) if !task_slot.retired.load(Ordering::Acquire) => {
                                *cached = Some(Connected { generation, remote });
                                // The peer answered: what the failed dial
                                // said about it no longer holds.
                                task_slot.clear_unreachable();
                                increment(&counters.opened);
                                DialState::Connected
                            }
                            _ => {
                                close(&remote, &counters);
                                DialState::Failed
                            }
                        },
                        Err(_) => {
                            close(&remote, &counters);
                            DialState::Failed
                        }
                    }
                }
                Err(_) => {
                    // Every candidate failed: the announced address and each
                    // fresh one the name resolved to.
                    task_slot.mark_unreachable(spread(cooldown, entropy()));
                    DialState::Failed
                }
            };
            sender.send(state.word());
            // The candidates still dialing when one connected are closed as they connect, within
            // the dial's own deadline, rather than left to idle out.
            if let Some(rest) = rest {
                drain_candidates(rest, timeout, &counters).await;
            }
        })
        .map_err(|_| PeerSendError::Busy)?;
        *current = Some(Dial {
            state: receiver,
            task,
        });
        Ok(waiting)
    }
    /// What the pool's socket did ([`crate::net::NetStats::io`]).
    /// What the pool's endpoint holds, refused and carried, its lanes' frames among it.
    pub async fn net_stats(&self) -> crate::net::NetStats {
        self.connector.stats().await
    }
    pub async fn io(&self) -> hyper_rt::udp::IoStats {
        self.connector.stats().await.io
    }
    pub fn stats(&self) -> PeerPoolStats {
        PeerPoolStats {
            delivered: self.counters.delivered.load(Ordering::Relaxed),
            lost: self.counters.lost.load(Ordering::Relaxed),
            lost_refused: self.counters.lost_refused.load(Ordering::Relaxed),
            lost_unknown: self.counters.lost_unknown.load(Ordering::Relaxed),
            lost_unavailable: self.counters.lost_unavailable.load(Ordering::Relaxed),
            lost_rejected: self.counters.lost_rejected.load(Ordering::Relaxed),
            busy: self.counters.busy.load(Ordering::Relaxed),
            dials: self.counters.dials.load(Ordering::Relaxed),
            refused_unreachable: self.counters.unreachable.load(Ordering::Relaxed),
            connections_opened: self.counters.opened.load(Ordering::Relaxed),
            closes_refused: self.counters.closes_refused.load(Ordering::Relaxed),
            cached_connections: self
                .state
                .lock()
                .map(|state| state.cached.len())
                .unwrap_or(0),
            inflight: self
                .limits
                .max_inflight
                .saturating_sub(usize::try_from(self.inflight.available()).unwrap_or(usize::MAX)),
            bulk_inflight: self.limits.max_inflight.saturating_sub(
                usize::try_from(self.bulk_inflight.available()).unwrap_or(usize::MAX),
            ),
        }
    }
    pub fn close(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.closed = true;
            for entry in state.cached.values() {
                entry.slot.retire();
            }
            state.cached.clear();
            state.routes.clear();
        }
    }
}
impl Drop for PeerConnectionPool {
    fn drop(&mut self) {
        self.close();
    }
}

/// A dial of one candidate address.
type Candidate = Pin<Box<dyn Future<Output = Result<QuicRemote, PeerSendError>>>>;
/// The candidates still dialing when one connected.
type Candidates = focal_rt::Concurrent<Candidate>;
/// The most candidates one dial races: the announced address, and the name's resolution (which
/// races its fresh addresses in a set of its own).
const DIAL_CANDIDATES: usize = 2;
/// Dial the announced address and, after a short head start, every fresh
/// address the advertised name resolves to (24 §24); the first connection
/// wins. A peer that moved behind its name is reached in one handshake
/// instead of after the dead address's deadline, and every candidate is
/// checked against the same certificate name. The winner, or why none won
/// within `timeout`, and the candidates still dialing, if any are.
async fn dial_candidates(
    dialer: QuicDialer,
    endpoint: PeerEndpoint,
    timeout: Duration,
    counters: Rc<Counters>,
) -> (Result<QuicRemote, PeerSendError>, Option<Candidates>) {
    let Ok(mut candidates) = Candidates::new(DIAL_CANDIDATES) else {
        return (Err(PeerSendError::Busy), None);
    };
    {
        let dialer = dialer.clone();
        let address = endpoint.address;
        let server_name = endpoint.server_name.clone();
        let announced: Candidate = Box::pin(async move {
            dialer
                .connect(address, &server_name)
                .await
                .map_err(|_| PeerSendError::Lost)
        });
        if candidates.push(announced).is_err() {
            return (Err(PeerSendError::Busy), None);
        }
    }
    if let Some(name) = endpoint.name.clone() {
        let announced = endpoint.address;
        let server_name = endpoint.server_name.clone();
        let resolving: Candidate = Box::pin(async move {
            pause(NAME_HEAD_START).await;
            let resolved = crate::transport::resolve_all(&name, MAX_NAME_CANDIDATES, timeout)
                .await
                .map_err(|_| PeerSendError::Lost)?;
            let mut fresh =
                Candidates::new(MAX_NAME_CANDIDATES).map_err(|_| PeerSendError::Busy)?;
            for address in resolved
                .into_iter()
                .filter(|address| *address != announced)
                .take(MAX_NAME_CANDIDATES)
            {
                let dialer = dialer.clone();
                let server_name = server_name.clone();
                let dialed: Candidate = Box::pin(async move {
                    dialer
                        .connect(address, &server_name)
                        .await
                        .map_err(|_| PeerSendError::Lost)
                });
                fresh.push(dialed).map_err(|_| PeerSendError::Busy)?;
            }
            let (winner, rest) = first_connection(fresh).await;
            // The winner is the dial's now; the fresh addresses still dialing are closed as they
            // connect on a task of their own, within the dial's deadline. With no task to give
            // them, they are abandoned: a connection one of them makes later is its endpoint's to
            // reuse or idle out.
            if !rest.is_empty() {
                let _ = hyper_rt::futures::spawn_detached(async move {
                    drain_candidates(rest, timeout, &counters).await;
                });
            }
            winner
        });
        if candidates.push(resolving).is_err() {
            return (Err(PeerSendError::Busy), None);
        }
    }
    let raced = hyper_rt::futures::within(nanos(timeout), first_connection(candidates)).await;
    match raced {
        Ok(Some((winner, rest))) => (winner, Some(rest)),
        // Timed out, or no timer to time it by: every candidate is abandoned.
        Ok(None) | Err(_) => (Err(PeerSendError::Lost), None),
    }
}
/// The first candidate that connected, and the candidates still dialing.
async fn first_connection(
    mut candidates: Candidates,
) -> (Result<QuicRemote, PeerSendError>, Candidates) {
    while let Some(dialed) = candidates.join_next().await {
        if let Ok(remote) = dialed {
            return (Ok(remote), candidates);
        }
    }
    (Err(PeerSendError::Lost), candidates)
}
/// Closes each candidate that connects after the winner, within `timeout`; one still dialing then
/// is abandoned.
async fn drain_candidates(mut rest: Candidates, timeout: Duration, counters: &Counters) {
    let drained = async {
        while let Some(dialed) = rest.join_next().await {
            if let Ok(extra) = dialed {
                close(&extra, counters);
            }
        }
    };
    let _ = hyper_rt::futures::within(nanos(timeout), drained).await;
}
/// A pause on the shard's clock; a shard with no timer left gives none.
async fn pause(period: Duration) {
    let _ = hyper_rt::futures::sleep(nanos(period)).await;
}
/// Closes `remote`, a connection no longer wanted; one its endpoint could not queue — the shard's
/// tasks all taken — idles out at its connection's idle timeout instead, and is counted.
fn close(remote: &QuicRemote, counters: &Counters) {
    if remote.close().is_err() {
        increment(&counters.closes_refused);
    }
}
fn increment(counter: &AtomicU64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        Some(value.saturating_add(1))
    });
}

/// One past the largest draw: what a draw is measured against.
const DRAWS: u128 = 1 << 64;
/// A pause spread by equal jitter (the audit's F64): drawn uniformly
/// between half of `delay` and the whole of it. Peers that lost one node
/// at once would otherwise retry it, and dial it again after its cooldown,
/// in step. Half the pause is kept whole because the pause has a meaning
/// of its own — an unreachable peer is not dialed again before its
/// cooldown, a lost exchange rests before it is retried — and the other
/// half is the spread.
pub(crate) fn spread(delay: Duration, random: u64) -> Duration {
    let half = delay.checked_div(2).unwrap_or(Duration::ZERO);
    let drawn = half
        .as_nanos()
        .saturating_mul(u128::from(random))
        .checked_div(DRAWS)
        .unwrap_or(0);
    half.saturating_add(Duration::from_nanos(
        u64::try_from(drawn).unwrap_or(u64::MAX),
    ))
}
/// Sixty-four random bits from the operating system — and when it has none
/// to give, the largest draw: the whole pause, never a shorter one.
fn entropy() -> u64 {
    let mut bytes = [0; 8];
    match getrandom::fill(&mut bytes) {
        Ok(()) => u64::from_le_bytes(bytes),
        Err(_) => u64::MAX,
    }
}
