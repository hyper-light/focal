//! The route cache: where a session's log leads, as the directory says, so
//! a node that does not serve a ledger points the client at the node that
//! does and a stale client learns the current epoch before any replica sees
//! its request ([24](../../../docs/archictecutre/24-placement-execution-and-fleet-control.md) §14).
//! One driver per node owns the bounded cache; the data service asks it
//! through a handle. Misses read the owning partition (local when hosted,
//! else the founder over `PeerControl`); every watched partition's route
//! changes are pulled once a second and applied as invalidations.
use crate::{control_host::ControlHost, network_service::NetworkHandles};
use focal_control::{ControlBootstrap, ControlRead, ControlReadResult, ControlReply, ControlRpc};
use focal_directory::{Delegation, DirectoryError, RouteCache, RouteCacheConfig, SessionRoute};
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use focal_model::{LedgerId, ParticipantId, RequestEpoch, RequestId, RouteEpoch};
use focal_wire::{
    AuthenticatedPeer, MAX_PEER_CONTROL_REQUEST_BYTES, Operation, PROTOCOL_VERSION,
    PeerConnectionPool, PeerGrant, PeerRole, RequestEnvelope, RouteHint,
};
use std::{collections::BTreeSet, time::Duration};
use tokio::{
    sync::{mpsc, oneshot},
    time::Instant,
};

/// How long a resolved route is trusted without a refresh.
const ROUTE_TTL_MS: u64 = 30_000;
/// The data service waits this long for the driver before serving without a
/// route.
const RESOLVE_TIMEOUT: Duration = Duration::from_millis(500);
const REMOTE_TIMEOUT: Duration = Duration::from_secs(3);
const REFRESH: Duration = Duration::from_secs(1);
const QUEUE: usize = 256;
/// The driver's own state: the cache rows are charged by the cache itself.
const STATE_BYTES: usize = 64 * 1024;

/// A route with the hint a client needs to reach its leader, absent when the
/// leader is this node (the caller answers with its own endpoint).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedRoute {
    pub route: SessionRoute,
    pub hint: Option<RouteHint>,
}
enum Work {
    Resolve {
        ledger: LedgerId,
        reply: oneshot::Sender<Option<ResolvedRoute>>,
    },
    /// The hint that points a client at one node at one epoch, from the
    /// pool's installed reachability.
    Hint {
        node: u64,
        epoch: RouteEpoch,
        reply: oneshot::Sender<Option<RouteHint>>,
    },
}
#[derive(Clone)]
pub struct RouteCacheHandle {
    sender: mpsc::Sender<Work>,
}
impl RouteCacheHandle {
    pub fn channel(
        budget: &MemoryBudget,
        config: RouteCacheConfig,
        node: u64,
        founder: u64,
        namespace: LedgerId,
        cluster: [u8; 16],
    ) -> Result<(Self, RouteCacheDriver), DirectoryError> {
        let allocation = budget
            .reserve(BudgetKind::Control, BudgetLane::Completion, STATE_BYTES)?
            .commit();
        let cache = RouteCache::new(config, budget.clone())?;
        let (sender, receiver) = mpsc::channel(QUEUE);
        let mut client = [0; 16];
        let mut hash = blake3::Hasher::new_derive_key("focal.route-cache.client.v1");
        hash.update(&cluster);
        hash.update(&node.to_be_bytes());
        for (target, source) in client.iter_mut().zip(hash.finalize().as_bytes()) {
            *target = *source;
        }
        Ok((
            Self { sender },
            RouteCacheDriver {
                receiver,
                cache,
                node,
                founder,
                namespace,
                client,
                nonce: 0,
                _allocation: allocation,
            },
        ))
    }
    /// A hint at `node` (another node this one can reach) at `epoch`.
    pub async fn hint(&self, node: u64, epoch: RouteEpoch) -> Option<RouteHint> {
        let (reply, receive) = oneshot::channel();
        self.sender
            .try_send(Work::Hint { node, epoch, reply })
            .ok()?;
        tokio::time::timeout(RESOLVE_TIMEOUT, receive)
            .await
            .ok()?
            .ok()?
    }
    /// The directory's route for `ledger`, or none when nothing is known in
    /// time; never an error the request path has to interpret.
    pub async fn resolve(&self, ledger: LedgerId) -> Option<ResolvedRoute> {
        let (reply, receive) = oneshot::channel();
        self.sender.try_send(Work::Resolve { ledger, reply }).ok()?;
        tokio::time::timeout(RESOLVE_TIMEOUT, receive)
            .await
            .ok()?
            .ok()?
    }
}
pub struct RouteCacheDriver {
    receiver: mpsc::Receiver<Work>,
    cache: RouteCache,
    node: u64,
    founder: u64,
    namespace: LedgerId,
    client: [u8; 16],
    nonce: u64,
    _allocation: Allocation,
}
impl RouteCacheDriver {
    pub async fn run(mut self, handles: &NetworkHandles, pool: &PeerConnectionPool) {
        let started = Instant::now();
        let mut next = started.checked_add(REFRESH).unwrap_or(started);
        loop {
            tokio::select! {
                biased;
                work = self.receiver.recv() => {
                    match work {
                        Some(Work::Resolve { ledger, reply }) => {
                            let now = elapsed_ms(started);
                            let resolved = self.resolve(handles, pool, ledger, now).await;
                            let _ = reply.send(resolved);
                        }
                        Some(Work::Hint { node, epoch, reply }) => {
                            let _ = reply.send(self.hint(pool, node, epoch));
                        }
                        None => return,
                    }
                }
                () = tokio::time::sleep_until(next) => {
                    let at = Instant::now();
                    next = at.checked_add(REFRESH).unwrap_or(at);
                    self.refresh(handles, pool, elapsed_ms(started)).await;
                }
            }
        }
    }
    fn peer(&self) -> Option<AuthenticatedPeer> {
        AuthenticatedPeer::local(PeerGrant {
            principal: ParticipantId(self.client),
            tenants: BTreeSet::from([self.namespace.tenant]),
            role: PeerRole::Runtime,
        })
        .ok()
    }
    fn request_id(&mut self) -> RequestId {
        self.nonce = self.nonce.wrapping_add(1);
        let mut id = [0; 16];
        id[..8].copy_from_slice(&self.node.to_be_bytes());
        id[8..].copy_from_slice(&self.nonce.to_be_bytes());
        RequestId(id)
    }
    /// The root's delegation for a ledger, from the trusted local root view.
    async fn delegation(&self, handles: &NetworkHandles, ledger: LedgerId) -> Option<Delegation> {
        let root = handles.control.observe_root().await.ok()?;
        let ControlBootstrap::Root { directory, .. } = &root.snapshot().state else {
            return None;
        };
        directory
            .delegations
            .range(..=focal_directory::NamespaceKey::of(ledger))
            .next_back()
            .map(|(_, delegation)| *delegation)
            .filter(|delegation| delegation.namespace.contains(ledger))
    }
    async fn delegation_of(
        &self,
        handles: &NetworkHandles,
        partition: focal_directory::PartitionId,
    ) -> Option<Delegation> {
        let root = handles.control.observe_root().await.ok()?;
        let ControlBootstrap::Root { directory, .. } = &root.snapshot().state else {
            return None;
        };
        directory
            .delegations
            .values()
            .find(|delegation| delegation.partition == partition)
            .copied()
    }
    /// One read of the partition that holds a delegation: the local host when
    /// this node hosts it, else the founder over the peer pool.
    async fn read(
        &mut self,
        handles: &NetworkHandles,
        pool: &PeerConnectionPool,
        delegation: &Delegation,
        query: ControlRead,
    ) -> Option<ControlReadResult> {
        if let Some(host) = handles.directory.host_of(delegation.partition) {
            let peer = self.peer()?;
            let id = self.request_id();
            return host.read(peer, id, query).await.ok();
        }
        let request = ControlRpc::Read(query)
            .encode(MAX_PEER_CONTROL_REQUEST_BYTES)
            .ok()?;
        let envelope = RequestEnvelope {
            protocol: PROTOCOL_VERSION,
            ledger: self.namespace,
            route_epoch: RouteEpoch(1),
            request_epoch: RequestEpoch(1),
            request_id: self.request_id(),
            operation: Operation::PeerControl {
                group: delegation.log_group.0,
                request,
            },
        };
        let bytes = tokio::time::timeout(
            REMOTE_TIMEOUT,
            pool.send_peer_control(self.founder, &envelope),
        )
        .await
        .ok()?
        .ok()?;
        match ControlReply::decode(&bytes, ControlHost::wire_limits().max_frame_bytes as usize)
            .ok()?
        {
            ControlReply::Read(value) => Some(value),
            _ => None,
        }
    }
    async fn resolve(
        &mut self,
        handles: &NetworkHandles,
        pool: &PeerConnectionPool,
        ledger: LedgerId,
        now: u64,
    ) -> Option<ResolvedRoute> {
        if let Ok(Some(route)) = self.cache.get(ledger, now) {
            let route = route.clone();
            return Some(self.resolved(pool, route));
        }
        let delegation = self.delegation(handles, ledger).await?;
        let ControlReadResult::Route(Some(route)) = self
            .read(handles, pool, &delegation, ControlRead::Route { ledger })
            .await?
        else {
            return None;
        };
        // A route the cache refuses (older than what it holds, or over
        // capacity) is still the directory's answer for this request.
        let _ = self.cache.insert(route.clone(), now, ROUTE_TTL_MS);
        Some(self.resolved(pool, route))
    }
    fn hint(&self, pool: &PeerConnectionPool, node: u64, epoch: RouteEpoch) -> Option<RouteHint> {
        if node == self.node || node == 0 {
            return None;
        }
        pool.route_endpoint(node)
            .ok()
            .flatten()
            .map(|endpoint| RouteHint {
                epoch,
                endpoint: endpoint.address.to_string(),
                server_name: endpoint.server_name,
            })
    }
    fn resolved(&self, pool: &PeerConnectionPool, route: SessionRoute) -> ResolvedRoute {
        let hint = self.hint(pool, route.leader, route.route_epoch);
        ResolvedRoute { route, hint }
    }
    /// Pull every watched partition's route changes since the revision the
    /// cache applied; a gap clears that partition's rows.
    async fn refresh(&mut self, handles: &NetworkHandles, pool: &PeerConnectionPool, now: u64) {
        let _ = self.cache.advance(now);
        let watches: Vec<_> = self.cache.watches().collect();
        for (partition, _epoch, revision) in watches {
            let Some(delegation) = self.delegation_of(handles, partition).await else {
                continue;
            };
            let Some(ControlReadResult::RouteChanges(batch)) = self
                .read(
                    handles,
                    pool,
                    &delegation,
                    ControlRead::RouteChanges {
                        after_revision: revision,
                    },
                )
                .await
            else {
                continue;
            };
            // A gap already cleared the partition inside the cache.
            let _ = self.cache.invalidate(&batch);
        }
    }
}
fn elapsed_ms(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}
