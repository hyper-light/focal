//! The route cache: where a session's log leads, as the directory says, so
//! a node that does not serve a ledger points the client at the node that
//! does and a stale client learns the current epoch before any replica sees
//! its request ([24](../../../docs/archictecutre/24-placement-execution-and-fleet-control.md) §14).
//! One driver per node owns the bounded cache; the data service asks it
//! through a handle. Misses read the owning partition (local when hosted,
//! else its leader over `PeerControl`, as far as the cache has learned it);
//! every watched partition's route changes are pulled once a second and
//! applied as invalidations.
use crate::{control_host::ControlHost, network_service::NetworkHandles};
use focal_control::{
    ControlBootstrap, ControlFailure, ControlRead, ControlReadResult, ControlReply, ControlRpc,
};
use focal_directory::{
    Delegation, DirectoryError, PartitionId, RouteCache, RouteCacheConfig, SessionRoute,
};
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use focal_model::{LedgerId, ParticipantId, RequestEpoch, RequestId, RouteEpoch};
use focal_rt::time::Instant;
use focal_wire::{
    AuthenticatedPeer, MAX_PEER_CONTROL_REQUEST_BYTES, Operation, PROTOCOL_VERSION,
    PeerConnectionPool, PeerGrant, PeerRole, RequestEnvelope, RouteHint,
};
use hyper_rt::combine::{Either, race2};
use hyper_rt::sync::{ChannelReceiver, OneshotSender, Sender};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
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
        reply: OneshotSender<Option<ResolvedRoute>>,
    },
    /// The hint that points a client at one node at one epoch, from the
    /// pool's installed reachability.
    Hint {
        node: u64,
        epoch: RouteEpoch,
        reply: OneshotSender<Option<RouteHint>>,
    },
}
#[derive(Clone)]
pub struct RouteCacheHandle {
    sender: Sender<Work>,
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
        let max_targets = config.max_partitions;
        let cache = RouteCache::new(config, budget.clone())?;
        let (sender, receiver) =
            hyper_rt::sync::channel(QUEUE).map_err(|_| DirectoryError::Capacity)?;
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
                targets: BTreeMap::new(),
                max_targets,
                _allocation: allocation,
            },
        ))
    }
    /// A hint at `node` (another node this one can reach) at `epoch`.
    pub async fn hint(&self, node: u64, epoch: RouteEpoch) -> Option<RouteHint> {
        let (reply, receive) = hyper_rt::sync::oneshot().ok()?;
        self.sender
            .try_send(Work::Hint { node, epoch, reply })
            .ok()?;
        focal_rt::time::timeout(RESOLVE_TIMEOUT, receive)
            .await
            .ok()?
            .ok()?
    }
    /// The directory's route for `ledger`, or none when nothing is known in
    /// time; never an error the request path has to interpret.
    pub async fn resolve(&self, ledger: LedgerId) -> Option<ResolvedRoute> {
        let (reply, receive) = hyper_rt::sync::oneshot().ok()?;
        self.sender.try_send(Work::Resolve { ledger, reply }).ok()?;
        focal_rt::time::timeout(RESOLVE_TIMEOUT, receive)
            .await
            .ok()?
            .ok()?
    }
}
pub struct RouteCacheDriver {
    receiver: ChannelReceiver<Work>,
    cache: RouteCache,
    node: u64,
    founder: u64,
    namespace: LedgerId,
    client: [u8; 16],
    nonce: u64,
    /// Each partition read on another node, the node it is asked of: the leader its group last
    /// named (`NotLeader`), or the voter after one that gave no answer, and before either the
    /// founder, else the first live voter (`placement_agent::partition_target_among`). Every read
    /// went to the founder, so a founder killed with eleven other nodes of 33 left each node that
    /// hosts no partition without routes for as long as it was down or led nothing: a follower
    /// named no new leader to its clients, and a node whose copy was not open yet answered them
    /// `Unavailable` (2026-10-29). At most as many as the partitions the cache watches
    /// (`RouteCacheConfig::max_partitions`), within the driver's own state (`STATE_BYTES`); a
    /// partition leaves it as it leaves the root's delegations.
    targets: BTreeMap<PartitionId, u64>,
    max_targets: usize,
    _allocation: Allocation,
}
/// What one partition read asked of another node got.
enum Asked {
    Read(Box<ControlReadResult>),
    /// The node does not lead the group; the leader it names, 0 where it knows none.
    NotLeader(u64),
    /// No answer, or the node serves no such group now.
    Silent,
    Refused,
}
impl RouteCacheDriver {
    pub async fn run(mut self, handles: &NetworkHandles, pool: &PeerConnectionPool) {
        let started = Instant::now();
        let mut next = started.checked_add(REFRESH).unwrap_or(started);
        loop {
            // Work first, then the refresh once it is due.
            match race2(self.receiver.recv(), focal_rt::time::sleep_until(next)).await {
                Either::First(Ok(Work::Resolve { ledger, reply })) => {
                    let now = elapsed_ms(started);
                    let resolved = self.resolve(handles, pool, ledger, now).await;
                    let _ = reply.send(resolved);
                }
                Either::First(Ok(Work::Hint { node, epoch, reply })) => {
                    let _ = reply.send(self.hint(pool, node, epoch));
                }
                // Every handle is gone, or no wait could be kept.
                Either::First(Err(_)) | Either::Second(Err(_)) => return,
                Either::Second(Ok(())) => {
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
    /// The root's delegation for a ledger, from the trusted local root view, with the voters the
    /// root grants its group.
    async fn delegation(
        &self,
        handles: &NetworkHandles,
        ledger: LedgerId,
    ) -> Option<(Delegation, BTreeMap<u64, u64>)> {
        let root = handles.control.observe_root().await.ok()?;
        let ControlBootstrap::Root { directory, .. } = &root.snapshot().state else {
            return None;
        };
        let delegation = directory
            .delegations
            .range(..=focal_directory::NamespaceKey::of(ledger))
            .next_back()
            .map(|(_, delegation)| *delegation)
            .filter(|delegation| delegation.namespace.contains(ledger))?;
        Some((delegation, voters_of(&root, &delegation)))
    }
    /// Observe the root once and index its delegations by partition, so a
    /// refresh over N watches costs one root observation and O(log n) lookups
    /// instead of one observation plus a linear scan per watch.
    async fn delegation_index(
        &self,
        handles: &NetworkHandles,
    ) -> Option<BTreeMap<PartitionId, (Delegation, BTreeMap<u64, u64>)>> {
        let root = handles.control.observe_root().await.ok()?;
        let ControlBootstrap::Root { directory, .. } = &root.snapshot().state else {
            return None;
        };
        Some(
            directory
                .delegations
                .values()
                .map(|delegation| {
                    (
                        delegation.partition,
                        (*delegation, voters_of(&root, delegation)),
                    )
                })
                .collect(),
        )
    }
    /// The node `delegation`'s partition is asked of ([`Self::targets`]).
    fn target(
        &self,
        handles: &NetworkHandles,
        delegation: &Delegation,
        voters: &BTreeMap<u64, u64>,
    ) -> u64 {
        let view = handles.liveness.view();
        let dead = |node: u64| {
            view.members
                .get(&node)
                .is_some_and(|member| member.status == crate::liveness::MemberStatus::Dead)
        };
        crate::placement_agent::partition_target_among(
            self.founder,
            self.node,
            voters,
            self.targets.get(&delegation.partition).copied(),
            dead,
        )
    }
    /// `node` is asked for `partition` from now on, within the bound.
    fn retarget(&mut self, partition: PartitionId, node: u64) {
        if node == 0 || node == self.node {
            return;
        }
        if self.targets.len() < self.max_targets || self.targets.contains_key(&partition) {
            self.targets.insert(partition, node);
        }
    }
    /// The voter after `failed`, which gave no answer, is asked for `delegation`'s partition next.
    fn rotate(
        &mut self,
        handles: &NetworkHandles,
        delegation: &Delegation,
        voters: &BTreeMap<u64, u64>,
        failed: u64,
    ) {
        let view = handles.liveness.view();
        let dead = |node: u64| {
            view.members
                .get(&node)
                .is_some_and(|member| member.status == crate::liveness::MemberStatus::Dead)
        };
        if let Some(next) =
            crate::placement_agent::next_partition_target(voters, failed, self.node, dead)
        {
            self.retarget(delegation.partition, next);
        }
    }
    /// One read of `delegation`'s partition asked of `target`.
    async fn ask(
        &mut self,
        pool: &PeerConnectionPool,
        target: u64,
        delegation: &Delegation,
        request: &[u8],
    ) -> Asked {
        let envelope = RequestEnvelope {
            protocol: PROTOCOL_VERSION,
            ledger: self.namespace,
            route_epoch: RouteEpoch(1),
            request_epoch: RequestEpoch(1),
            request_id: self.request_id(),
            operation: Operation::PeerControl {
                group: delegation.log_group.0,
                request: request.to_vec(),
            },
        };
        let Ok(Ok(bytes)) =
            focal_rt::time::timeout(REMOTE_TIMEOUT, pool.send_peer_control(target, &envelope))
                .await
        else {
            return Asked::Silent;
        };
        match ControlReply::decode(&bytes, ControlHost::wire_limits().max_frame_bytes as usize) {
            Ok(ControlReply::Read(value)) => Asked::Read(Box::new(value)),
            Ok(ControlReply::Rejected(ControlFailure::NotLeader { leader })) => {
                Asked::NotLeader(leader)
            }
            Ok(ControlReply::Rejected(ControlFailure::Unavailable)) => Asked::Silent,
            _ => Asked::Refused,
        }
    }
    /// One read of the partition that holds a delegation: the local host when
    /// this node hosts it, else its leader as far as this cache has learned it
    /// ([`Self::targets`]), and at once the leader a node that does not lead
    /// names; a node that gives no answer is passed for the next voter.
    async fn read(
        &mut self,
        handles: &NetworkHandles,
        pool: &PeerConnectionPool,
        (delegation, voters): (&Delegation, &BTreeMap<u64, u64>),
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
        let mut target = self.target(handles, delegation, voters);
        // The node asked, then once the leader it names.
        for _ in 0..2 {
            match self.ask(pool, target, delegation, &request).await {
                Asked::Read(value) => return Some(*value),
                Asked::NotLeader(leader) if leader != 0 && leader != target => {
                    self.retarget(delegation.partition, leader);
                    target = leader;
                }
                Asked::Silent => {
                    self.rotate(handles, delegation, voters, target);
                    return None;
                }
                Asked::NotLeader(_) | Asked::Refused => return None,
            }
        }
        None
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
        let (delegation, voters) = self.delegation(handles, ledger).await?;
        let ControlReadResult::Route(Some(route)) = self
            .read(
                handles,
                pool,
                (&delegation, &voters),
                ControlRead::Route { ledger },
            )
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
        if watches.is_empty() {
            return;
        }
        let Some(index) = self.delegation_index(handles).await else {
            return;
        };
        // A partition leaves the targets as it leaves the root's delegations.
        self.targets
            .retain(|partition, _| index.contains_key(partition));
        for (partition, _epoch, revision) in watches {
            let Some((delegation, voters)) = index.get(&partition) else {
                continue;
            };
            let Some(ControlReadResult::RouteChanges(batch)) = self
                .read(
                    handles,
                    pool,
                    (delegation, voters),
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
/// The voters the root's installed authority grants `delegation`'s group; none where it has none.
fn voters_of(
    root: &crate::control_host::RootObservation,
    delegation: &Delegation,
) -> BTreeMap<u64, u64> {
    root.authority()
        .and_then(|authority| authority.groups.get(&delegation.log_group))
        .map(|group| group.voters.clone())
        .unwrap_or_default()
}
fn elapsed_ms(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}
