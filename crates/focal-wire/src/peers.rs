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
use tokio::sync::{Mutex as AsyncMutex, OwnedSemaphorePermit, Semaphore};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerEndpoint {
    pub address: SocketAddr,
    /// The committed identity's certificate name, not an unverified redirect.
    pub server_name: String,
}
#[derive(Debug, Clone)]
pub struct PeerPoolLimits {
    pub max_routes: usize,
    pub max_connections: usize,
    pub max_inflight: usize,
    pub per_peer_inflight: usize,
    pub attempts: u8,
    pub timeout: Duration,
    pub retry_backoff: Duration,
}
impl Default for PeerPoolLimits {
    fn default() -> Self {
        Self {
            max_routes: 4096,
            max_connections: 128,
            max_inflight: 256,
            per_peer_inflight: 2,
            attempts: 2,
            timeout: Duration::from_secs(5),
            retry_backoff: Duration::from_millis(10),
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
            || !(1..=3).contains(&self.attempts)
            || self.timeout.is_zero()
            || self.timeout > Duration::from_secs(120)
            || self.retry_backoff > self.timeout
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
    pub connections_opened: u64,
    pub cached_connections: usize,
    pub inflight: usize,
}
#[derive(Default)]
struct Counters {
    delivered: AtomicU64,
    lost: AtomicU64,
    busy: AtomicU64,
    opened: AtomicU64,
}
struct Connected {
    generation: u64,
    remote: QuicRemote,
}
struct Slot {
    endpoint: PeerEndpoint,
    retired: AtomicBool,
    inflight: Semaphore,
    connection: AsyncMutex<Option<Connected>>,
    generation: AtomicU64,
    _reservation: OwnedSemaphorePermit,
}
impl Slot {
    fn retire(&self) {
        self.retired.store(true, Ordering::Release);
        if let Ok(mut connection) = self.connection.try_lock()
            && let Some(connection) = connection.take()
        {
            connection.remote.close();
        }
    }
}
impl Drop for Slot {
    fn drop(&mut self) {
        if let Some(connection) = self.connection.get_mut().take() {
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
    clock: u64,
    closed: bool,
}

pub struct PeerConnectionPool {
    connector: QuicConnector,
    limits: PeerPoolLimits,
    state: Mutex<State>,
    inflight: Semaphore,
    // OwnedSemaphorePermit lives in slots that can outlive cache membership.
    connections: Arc<Semaphore>,
    counters: Counters,
}
impl PeerConnectionPool {
    pub fn new(connector: QuicConnector, limits: PeerPoolLimits) -> Result<Self, PeerSendError> {
        limits.validate()?;
        Ok(Self {
            connector,
            inflight: Semaphore::new(limits.max_inflight),
            connections: Arc::new(Semaphore::new(limits.max_connections)),
            limits,
            state: Mutex::new(State {
                revision: 0,
                routes: BTreeMap::new(),
                cached: BTreeMap::new(),
                clock: 0,
                closed: false,
            }),
            counters: Counters::default(),
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
        state.revision = revision;
        state.routes = routes;
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
        let _inflight = self
            .inflight
            .try_acquire()
            .map_err(|_| PeerSendError::Busy)?;
        tokio::time::timeout(self.limits.timeout, async {
            for attempt in 0..self.limits.attempts {
                let slot = self.slot(target)?;
                let _peer = slot
                    .inflight
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
                match remote.request(request).await {
                    Ok(response) => match response.result {
                        value @ (Response::PeerAccepted
                        | Response::Custody(_)
                        | Response::Control { .. }
                        | Response::ManagedSupport(_)) => {
                            if slot.retired.load(Ordering::Acquire) {
                                return Err(PeerSendError::RouteChanged);
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
                let mut cached = slot.connection.lock().await;
                if cached
                    .as_ref()
                    .is_some_and(|entry| entry.generation == generation)
                {
                    *cached = None;
                }
                drop(cached);
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
        let endpoint = state
            .routes
            .get(&target)
            .cloned()
            .ok_or(PeerSendError::NoRoute)?;
        state.clock = state.clock.saturating_add(1);
        let clock = state.clock;
        if let Some(entry) = state.cached.get_mut(&target) {
            entry.used = clock;
            return Ok(entry.slot.clone());
        }
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
            connection: AsyncMutex::new(None),
            generation: AtomicU64::new(0),
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
    async fn connection(&self, slot: &Slot) -> Result<(u64, QuicRemote), PeerSendError> {
        let mut cached = slot.connection.lock().await;
        if slot.retired.load(Ordering::Acquire) {
            return Err(PeerSendError::RouteChanged);
        }
        if let Some(connection) = &*cached {
            return Ok((connection.generation, connection.remote.clone()));
        }
        // One connecting task per peer. At most per_peer_inflight-1 callers can
        // wait for it, and all retain global permits under the same deadline.
        let remote = self
            .connector
            .connect(slot.endpoint.address, &slot.endpoint.server_name)
            .await
            .map_err(|_| PeerSendError::Lost)?;
        if slot.retired.load(Ordering::Acquire) {
            remote.close();
            return Err(PeerSendError::RouteChanged);
        }
        let generation =
            match slot
                .generation
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                    value.checked_add(1)
                }) {
                Ok(generation) => generation,
                Err(_) => {
                    remote.close();
                    return Err(PeerSendError::Closed);
                }
            };
        increment(&self.counters.opened);
        *cached = Some(Connected {
            generation,
            remote: remote.clone(),
        });
        Ok((generation, remote))
    }
    pub fn stats(&self) -> PeerPoolStats {
        PeerPoolStats {
            delivered: self.counters.delivered.load(Ordering::Relaxed),
            lost: self.counters.lost.load(Ordering::Relaxed),
            busy: self.counters.busy.load(Ordering::Relaxed),
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

fn increment(counter: &AtomicU64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        Some(value.saturating_add(1))
    });
}
