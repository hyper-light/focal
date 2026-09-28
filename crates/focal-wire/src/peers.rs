//! Bounded node-to-node replication delivery. Reachability is supplied by the
//! committed control owner; routes never grant membership or voter authority.
use crate::*;
use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, watch},
    task::JoinSet,
};

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
    /// it again. Sends within the cooldown fail at once as `Lost` instead of
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
    fn validate(&self) -> Result<(), PeerSendError> {
        if self.max_routes == 0
            || self.max_routes > 65536
            || self.max_connections == 0
            || self.max_connections > self.max_routes
            || self.max_inflight == 0
            || self.max_inflight > 65536
            || !(1..=2).contains(&self.per_peer_inflight)
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
    pub busy: u64,
    /// Dials attempted, successful or not.
    pub dials: u64,
    pub connections_opened: u64,
    pub cached_connections: usize,
    pub inflight: usize,
}
#[derive(Default)]
struct Counters {
    delivered: AtomicU64,
    lost: AtomicU64,
    busy: AtomicU64,
    dials: AtomicU64,
    opened: AtomicU64,
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
/// A dial in flight for a slot: it runs on its own task so a caller giving
/// up under its own deadline neither abandons it nor makes the next caller
/// start over from the announced address (24 §24).
struct Dial {
    state: watch::Receiver<DialState>,
    task: tokio::task::JoinHandle<()>,
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
    /// The probe lane: one liveness probe to this peer at a time.
    probes: Semaphore,
    /// Never held across an await: a retirement takes it and closes what it
    /// finds, and whoever stores a connection checks for a retirement under
    /// it, so no connection outlives its slot's retirement.
    connection: Mutex<Option<Connected>>,
    dial: Mutex<Option<Dial>>,
    generation: AtomicU64,
    /// Until when a failed dial keeps this peer from being dialed again.
    unreachable_until: Mutex<Option<std::time::Instant>>,
    _reservation: OwnedSemaphorePermit,
}
impl Slot {
    fn unreachable(&self) -> bool {
        self.unreachable_until
            .lock()
            .ok()
            .and_then(|until| *until)
            .is_some_and(|until| std::time::Instant::now() < until)
    }
    fn mark_unreachable(&self, cooldown: Duration) {
        if cooldown.is_zero() {
            return;
        }
        if let Ok(mut until) = self.unreachable_until.lock() {
            *until = std::time::Instant::now().checked_add(cooldown);
        }
    }
    fn retire(&self) {
        self.retired.store(true, Ordering::Release);
        if let Ok(mut dial) = self.dial.lock()
            && let Some(dial) = dial.take()
        {
            dial.task.abort();
        }
        // A poisoned lock still guards the connection to close.
        let connection = match self.connection.lock() {
            Ok(mut connection) => connection.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        };
        if let Some(connection) = connection {
            connection.remote.close();
        }
    }
}
impl Drop for Slot {
    fn drop(&mut self) {
        if let Some(dial) = self.dial.get_mut().ok().and_then(|dial| dial.take()) {
            dial.task.abort();
        }
        let connection = match self.connection.get_mut() {
            Ok(connection) => connection.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        };
        if let Some(connection) = connection {
            connection.remote.close();
        }
    }
}
struct CacheEntry {
    // The cache and concurrent send tasks retain one physical connection slot;
    // removal retires it while outstanding sends keep its capacity reservation.
    slot: Arc<Slot>,
    used: u64,
}
struct State {
    revision: u64,
    routes: BTreeMap<u64, PeerEndpoint>,
    cached: BTreeMap<u64, CacheEntry>,
    /// The measured path to each routed peer (27 §3.1 P2): one estimator per
    /// peer, kept across reconnects and dropped with the peer's route.
    paths: BTreeMap<u64, focal_timing::PathRtt>,
    clock: u64,
    closed: bool,
}

pub struct PeerConnectionPool {
    connector: QuicConnector,
    limits: PeerPoolLimits,
    state: Mutex<State>,
    inflight: Semaphore,
    probe_inflight: Semaphore,
    // OwnedSemaphorePermit lives in slots that can outlive cache membership.
    connections: Arc<Semaphore>,
    // A slot's detached dial records its outcome after every caller may have
    // given up; the counters outlive any one caller for the same reason the
    // connection permits do.
    counters: Arc<Counters>,
}
impl PeerConnectionPool {
    pub fn limits(&self) -> &PeerPoolLimits {
        &self.limits
    }
    pub fn new(connector: QuicConnector, limits: PeerPoolLimits) -> Result<Self, PeerSendError> {
        limits.validate()?;
        Ok(Self {
            connector,
            inflight: Semaphore::new(limits.max_inflight),
            probe_inflight: Semaphore::new(limits.max_probe_inflight),
            connections: Arc::new(Semaphore::new(limits.max_connections)),
            limits,
            state: Mutex::new(State {
                revision: 0,
                routes: BTreeMap::new(),
                cached: BTreeMap::new(),
                paths: BTreeMap::new(),
                clock: 0,
                closed: false,
            }),
            counters: Arc::new(Counters::default()),
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
        state.revision = revision;
        state.routes = routes;
        Ok(())
    }
    /// The measured path to `target`: round trips of exchanges it answered
    /// on an open connection. `None` for a peer with no route; a routed peer
    /// that has answered nothing yet has a path with no sample.
    pub fn path(&self, target: u64) -> Option<focal_timing::PathRtt> {
        let state = self.state.lock().ok()?;
        state
            .routes
            .contains_key(&target)
            .then(|| state.paths.get(&target).copied().unwrap_or_default())
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
    pub fn replace_identity(&self, tls: quinn::ClientConfig) -> Result<(), PeerSendError> {
        self.connector
            .replace_tls(tls)
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
    pub async fn send(&self, target: u64, request: &RequestEnvelope) -> Result<(), PeerSendError> {
        if !matches!(request.operation, Operation::Raft { .. }) {
            return Err(PeerSendError::InvalidRequest);
        }
        match self.exchange(target, request).await? {
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
            .map_err(|_| PeerSendError::Busy)?;
        tokio::time::timeout(self.limits.timeout, async {
            let remote = self
                .connector
                .connect(address, &server_name)
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
            remote.close();
            result
        })
        .await
        .map_err(|_| PeerSendError::Lost)?
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
        if tokio::runtime::Handle::try_current().is_err() {
            return Err(PeerSendError::Lost);
        }
        let result = self.send_bounded(target, request).await;
        match &result {
            Ok(_) => {
                increment(&self.counters.delivered);
            }
            Err(PeerSendError::Busy) => {
                increment(&self.counters.busy);
            }
            Err(_) => {
                increment(&self.counters.lost);
            }
        }
        result
    }
    async fn send_bounded(
        &self,
        target: u64,
        request: &RequestEnvelope,
    ) -> Result<Response, PeerSendError> {
        let valid_operation = match &request.operation {
            Operation::Raft { group, message } => *group != [0; 16] && !message.is_empty(),
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
                != if matches!(request.operation, Operation::ManagedSupport { .. }) {
                    MANAGED_PROTOCOL_VERSION
                } else {
                    PROTOCOL_VERSION
                }
            || request.request_epoch.0 == 0
            || request.request_id.is_zero()
            || postcard::experimental::serialized_size(request)
                .map_err(|_| PeerSendError::InvalidRequest)?
                > self.connector.limits().max_frame_bytes as usize
        {
            return Err(PeerSendError::InvalidRequest);
        }
        let probe = matches!(request.operation, Operation::Probe { .. });
        let _inflight = if probe {
            &self.probe_inflight
        } else {
            &self.inflight
        }
        .try_acquire()
        .map_err(|_| PeerSendError::Busy)?;
        tokio::time::timeout(self.limits.timeout, async {
            for attempt in 0..self.limits.attempts {
                let slot = self.slot(target)?;
                let _peer = if probe { &slot.probes } else { &slot.inflight }
                    .try_acquire()
                    .map_err(|_| PeerSendError::Busy)?;
                let (generation, remote) = match self.connection(&slot).await {
                    Ok(connection) => connection,
                    Err(error) => {
                        if attempt.saturating_add(1) == self.limits.attempts {
                            return Err(error);
                        }
                        tokio::time::sleep(self.limits.retry_backoff).await;
                        continue;
                    }
                };
                let sent = std::time::Instant::now();
                match remote.request(request).await {
                    Ok(response) => match response.result {
                        value @ (Response::PeerAccepted
                        | Response::Custody(_)
                        | Response::Control { .. }
                        | Response::ManagedSupport(_)
                        | Response::Probe(_)) => {
                            if slot.retired.load(Ordering::Acquire) {
                                return Err(PeerSendError::RouteChanged);
                            }
                            // Broadcast time is a replication message
                            // or a probe answered by the peer alone. A
                            // control request waits on a quorum commit,
                            // which is the group's latency, not the path's.
                            if matches!(
                                request.operation,
                                Operation::Raft { .. } | Operation::Probe { .. }
                            ) {
                                self.observe(target, sent.elapsed());
                            }
                            return Ok(value);
                        }
                        Response::Error(AccessError::Unavailable | AccessError::OutcomeUnknown) => {
                        }
                        Response::Error(error) => return Err(PeerSendError::Rejected(error)),
                        _ => return Err(PeerSendError::Lost),
                    },
                    Err(WireError::Limit) => return Err(PeerSendError::Busy),
                    Err(WireError::Access(error)) => return Err(PeerSendError::Rejected(error)),
                    Err(_) => (),
                }
                remote.close();
                if let Ok(mut cached) = slot.connection.lock()
                    && cached
                        .as_ref()
                        .is_some_and(|entry| entry.generation == generation)
                {
                    *cached = None;
                }
                if attempt.saturating_add(1) < self.limits.attempts {
                    tokio::time::sleep(self.limits.retry_backoff).await;
                }
            }
            Err(PeerSendError::Lost)
        })
        .await
        .map_err(|_| PeerSendError::Lost)?
    }
    fn slot(&self, target: u64) -> Result<Arc<Slot>, PeerSendError> {
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
        if self.connections.available_permits() == 0 {
            let victim = state
                .cached
                .iter()
                .filter(|(_, entry)| Arc::strong_count(&entry.slot) == 1)
                .min_by_key(|(_, entry)| entry.used)
                .map(|(id, _)| *id);
            if let Some(victim) = victim
                && let Some(entry) = state.cached.remove(&victim)
            {
                entry.slot.retire();
            }
        }
        let reservation = self
            .connections
            .clone()
            .try_acquire_owned()
            .map_err(|_| PeerSendError::Busy)?;
        let slot = Arc::new(Slot {
            endpoint,
            retired: AtomicBool::new(false),
            inflight: Semaphore::new(self.limits.per_peer_inflight),
            probes: Semaphore::new(1),
            connection: Mutex::new(None),
            dial: Mutex::new(None),
            generation: AtomicU64::new(0),
            unreachable_until: Mutex::new(None),
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
    async fn connection(&self, slot: &Arc<Slot>) -> Result<(u64, QuicRemote), PeerSendError> {
        // A peer whose dial just failed is not dialed again until its cooldown
        // passes: the send fails at once rather than holding its permits for
        // another dial deadline, so unreachable peers never consume the
        // capacity reachable ones need.
        if slot.unreachable() {
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
            let state = *dial.borrow_and_update();
            match state {
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
    fn dial(&self, slot: &Arc<Slot>) -> Result<watch::Receiver<DialState>, PeerSendError> {
        let mut current = slot.dial.lock().map_err(|_| PeerSendError::Closed)?;
        if let Some(dial) = current.as_ref()
            && *dial.state.borrow() == DialState::Pending
        {
            return Ok(dial.state.clone());
        }
        if slot.retired.load(Ordering::Acquire) {
            return Err(PeerSendError::RouteChanged);
        }
        let dialer = self.connector.dialer().map_err(|_| PeerSendError::Closed)?;
        let (sender, receiver) = watch::channel(DialState::Pending);
        increment(&self.counters.dials);
        let task_slot = slot.clone();
        let counters = self.counters.clone();
        let timeout = self.limits.timeout;
        let cooldown = self.limits.unreachable_cooldown;
        let task = tokio::spawn(async move {
            let endpoint = task_slot.endpoint.clone();
            let state = match dial_candidates(dialer, endpoint, timeout).await {
                Ok(remote) if task_slot.retired.load(Ordering::Acquire) => {
                    remote.close();
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
                                increment(&counters.opened);
                                DialState::Connected
                            }
                            _ => {
                                remote.close();
                                DialState::Failed
                            }
                        },
                        Err(_) => {
                            remote.close();
                            DialState::Failed
                        }
                    }
                }
                Err(_) => {
                    // Every candidate failed: the announced address and each
                    // fresh one the name resolved to.
                    task_slot.mark_unreachable(cooldown);
                    DialState::Failed
                }
            };
            sender.send_replace(state);
        });
        *current = Some(Dial {
            state: receiver.clone(),
            task,
        });
        Ok(receiver)
    }
    pub fn stats(&self) -> PeerPoolStats {
        PeerPoolStats {
            delivered: self.counters.delivered.load(Ordering::Relaxed),
            lost: self.counters.lost.load(Ordering::Relaxed),
            busy: self.counters.busy.load(Ordering::Relaxed),
            dials: self.counters.dials.load(Ordering::Relaxed),
            connections_opened: self.counters.opened.load(Ordering::Relaxed),
            cached_connections: self
                .state
                .lock()
                .map(|state| state.cached.len())
                .unwrap_or(0),
            inflight: self
                .limits
                .max_inflight
                .saturating_sub(self.inflight.available_permits()),
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

/// Dial the announced address and, after a short head start, every fresh
/// address the advertised name resolves to (24 §24); the first connection
/// wins and the rest are abandoned. A peer that moved behind its name is
/// reached in one handshake instead of after the dead address's deadline,
/// and every candidate is checked against the same certificate name.
async fn dial_candidates(
    dialer: QuicDialer,
    endpoint: PeerEndpoint,
    timeout: Duration,
) -> Result<QuicRemote, PeerSendError> {
    let mut candidates: JoinSet<Result<QuicRemote, PeerSendError>> = JoinSet::new();
    {
        let dialer = dialer.clone();
        let address = endpoint.address;
        let server_name = endpoint.server_name.clone();
        candidates.spawn(async move {
            dialer
                .connect(address, &server_name)
                .await
                .map_err(|_| PeerSendError::Lost)
        });
    }
    if let Some(name) = endpoint.name.clone() {
        let announced = endpoint.address;
        let server_name = endpoint.server_name.clone();
        candidates.spawn(async move {
            tokio::time::sleep(NAME_HEAD_START).await;
            let resolved = tokio::net::lookup_host(name.as_str())
                .await
                .map_err(|_| PeerSendError::Lost)?;
            let mut fresh: JoinSet<Result<QuicRemote, PeerSendError>> = JoinSet::new();
            for address in resolved
                .filter(|address| *address != announced)
                .take(MAX_NAME_CANDIDATES)
            {
                let dialer = dialer.clone();
                let server_name = server_name.clone();
                fresh.spawn(async move {
                    dialer
                        .connect(address, &server_name)
                        .await
                        .map_err(|_| PeerSendError::Lost)
                });
            }
            first_connection(fresh).await
        });
    }
    tokio::time::timeout(timeout, first_connection(candidates))
        .await
        .map_err(|_| PeerSendError::Lost)?
}
/// The first candidate that connected; the others are abandoned, and one
/// that also connected meanwhile is closed rather than left to idle out.
async fn first_connection(
    mut candidates: JoinSet<Result<QuicRemote, PeerSendError>>,
) -> Result<QuicRemote, PeerSendError> {
    while let Some(joined) = candidates.join_next().await {
        if let Ok(Ok(remote)) = joined {
            candidates.abort_all();
            while let Some(other) = candidates.join_next().await {
                if let Ok(Ok(extra)) = other {
                    extra.close();
                }
            }
            return Ok(remote);
        }
    }
    Err(PeerSendError::Lost)
}
fn increment(counter: &AtomicU64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        Some(value.saturating_add(1))
    });
}
