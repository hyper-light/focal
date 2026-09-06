//! One node-owned store with installed session custody policy. Only local file
//! and directory sync is claimed here; aggregate custody belongs to the session.
use focal_evidence::{ContentError, ContentStore, TransferManifest};
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use focal_model::*;
use focal_wire::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, Instant},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct CustodyScope {
    pub ledger: LedgerId,
    pub route_epoch: RouteEpoch,
    pub policy_revision: u64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CustodyPolicy {
    pub ledger: LedgerId,
    pub route_epoch: RouteEpoch,
    pub policy_revision: u64,
    /// Trusted control-plane enrollment and placement, never request labels.
    pub peers: BTreeSet<u64>,
}
impl CustodyPolicy {
    pub fn scope(&self) -> CustodyScope {
        CustodyScope {
            ledger: self.ledger,
            route_epoch: self.route_epoch,
            policy_revision: self.policy_revision,
        }
    }
}
#[derive(Clone, Debug)]
pub struct CustodyConfig {
    pub node: u64,
    pub max_policies: usize,
    pub max_transfers: usize,
    pub max_transfer_bytes: u64,
    pub transfer_ttl: Duration,
}
impl CustodyConfig {
    pub fn new(node: u64) -> Self {
        Self {
            node,
            max_policies: 4096,
            max_transfers: 128,
            max_transfer_bytes: 1024 * 1024 * 1024,
            transfer_ttl: Duration::from_secs(60),
        }
    }
}
/// Moving or transforming a result keeps its permit attached. There is no
/// public extraction that silently drops accounting while returning its bytes.
pub struct Accounted<T> {
    value: T,
    _allocation: Allocation,
}
impl<T> Accounted<T> {
    pub fn value(&self) -> &T {
        &self.value
    }
    pub fn map<U>(self, map: impl FnOnce(T) -> U) -> Accounted<U> {
        Accounted {
            value: map(self.value),
            _allocation: self._allocation,
        }
    }
}
impl Accounted<ResponseEnvelope> {
    pub fn into_wire_response(self) -> OwnedResponse {
        OwnedResponse::accounted(self.value, self._allocation)
    }
}
struct Installed {
    policy: CustodyPolicy,
    _allocation: Allocation,
}
struct Transfer {
    manifest: TransferManifest,
    next_missing: usize,
    expires: Instant,
    _allocation: Allocation,
}
type TransferKey = (CustodyScope, u64, [u8; 16]);
pub struct CustodyStore {
    store: ContentStore,
    config: CustodyConfig,
    budget: MemoryBudget,
    policies: BTreeMap<LedgerId, Installed>,
    transfers: BTreeMap<TransferKey, Transfer>,
    exports: BTreeMap<(CustodyScope, ContentHash), Transfer>,
    transfer_bytes: u64,
}
impl CustodyStore {
    pub fn new(
        store: ContentStore,
        config: CustodyConfig,
        budget: MemoryBudget,
    ) -> Result<Self, AccessError> {
        if config.node == 0
            || !(1..=65536).contains(&config.max_policies)
            || !(1..=1024).contains(&config.max_transfers)
            || config.max_transfer_bytes == 0
            || config.transfer_ttl.is_zero()
            || config.transfer_ttl > Duration::from_secs(3600)
        {
            return Err(AccessError::InvalidRequest);
        }
        Ok(Self {
            store,
            config,
            budget,
            policies: BTreeMap::new(),
            transfers: BTreeMap::new(),
            exports: BTreeMap::new(),
            transfer_bytes: 0,
        })
    }
    pub fn config(&self) -> &CustodyConfig {
        &self.config
    }
    pub(crate) fn content(&self) -> &ContentStore {
        &self.store
    }
    pub(crate) fn content_mut(&mut self) -> &mut ContentStore {
        &mut self.store
    }
    pub fn retained(&self) -> (usize, u64) {
        (
            self.transfers.len().saturating_add(self.exports.len()),
            self.transfer_bytes,
        )
    }
    pub fn installed(&self, ledger: LedgerId) -> Option<&CustodyPolicy> {
        self.policies.get(&ledger).map(|row| &row.policy)
    }
    /// The caller is the trusted control owner. Equal facts retry exactly;
    /// changed facts must move a route or policy fence forward without regression.
    pub fn install_policy(&mut self, policy: CustodyPolicy) -> Result<(), AccessError> {
        if policy.ledger.tenant.is_zero()
            || policy.ledger.session.is_zero()
            || policy.route_epoch.0 == 0
            || policy.policy_revision == 0
            || policy.peers.is_empty()
            || policy.peers.len() > 1024
            || policy.peers.contains(&0)
            || !policy.peers.contains(&self.config.node)
        {
            return Err(AccessError::InvalidRequest);
        }
        if let Some(old) = self.installed(policy.ledger) {
            if old == &policy {
                return Ok(());
            }
            if policy.route_epoch < old.route_epoch
                || policy.policy_revision < old.policy_revision
                || (policy.route_epoch == old.route_epoch
                    && policy.policy_revision == old.policy_revision)
            {
                return Err(AccessError::Unavailable);
            }
        } else if self.policies.len() >= self.config.max_policies {
            return Err(AccessError::Capacity);
        }
        let bytes = policy
            .peers
            .len()
            .checked_mul(128)
            .and_then(|n| n.checked_add(4096))
            .ok_or(AccessError::Capacity)?;
        let allocation = self.reserve(BudgetKind::Control, BudgetLane::Completion, bytes)?;
        let ledger = policy.ledger;
        self.policies.insert(
            ledger,
            Installed {
                policy,
                _allocation: allocation,
            },
        );
        self.transfers
            .retain(|(scope, _, _), _| scope.ledger != ledger);
        self.exports.retain(|(scope, _), _| scope.ledger != ledger);
        self.recount()?;
        Ok(())
    }
    pub fn authorize(&self, verified: &VerifiedRequest) -> Result<CustodyScope, AccessError> {
        let request = verified.request();
        let policy = self
            .installed(request.ledger)
            .ok_or(AccessError::Unauthorized)?;
        if request.route_epoch != policy.route_epoch {
            return Err(AccessError::Unavailable);
        }
        Ok(policy.scope())
    }
    pub fn check_scope(
        &self,
        scope: CustodyScope,
        content: &ContentRef,
    ) -> Result<(), AccessError> {
        let installed = self
            .installed(scope.ledger)
            .ok_or(AccessError::Unauthorized)?;
        if installed.scope() != scope {
            return Err(AccessError::Unavailable);
        }
        if content.domain != ContentDomainId(scope.ledger.tenant.0) {
            return Err(AccessError::Unauthorized);
        }
        Ok(())
    }
    fn reserve(
        &self,
        kind: BudgetKind,
        lane: BudgetLane,
        bytes: usize,
    ) -> Result<Allocation, AccessError> {
        self.budget
            .reserve(kind, lane, bytes)
            .map(|r| r.commit())
            .map_err(|_| AccessError::Capacity)
    }
    fn deadline(&self) -> Result<Instant, AccessError> {
        Instant::now()
            .checked_add(self.config.transfer_ttl)
            .ok_or(AccessError::Unavailable)
    }
    fn room(&self, content: &ContentRef) -> Result<u64, AccessError> {
        if self.retained().0 >= self.config.max_transfers {
            return Err(AccessError::Capacity);
        }
        let total = self
            .transfer_bytes
            .checked_add(content.length)
            .ok_or(AccessError::Capacity)?;
        if total > self.config.max_transfer_bytes {
            return Err(AccessError::Capacity);
        }
        Ok(total)
    }
    fn manifest_allowance(&self) -> Result<usize, AccessError> {
        self.store
            .max_manifest_bytes()
            .checked_mul(4)
            .and_then(|n| n.checked_add(4096))
            .ok_or(AccessError::Capacity)
    }
    fn descriptor(
        &self,
        manifest: TransferManifest,
        next_missing: usize,
        allocation: Allocation,
    ) -> Result<Transfer, AccessError> {
        Ok(Transfer {
            manifest,
            next_missing,
            expires: self.deadline()?,
            _allocation: allocation,
        })
    }
    pub fn request(
        &mut self,
        verified: &VerifiedRequest,
    ) -> Result<Accounted<CustodyReply>, AccessError> {
        self.expire(Instant::now())?;
        let scope = self.authorize(verified)?;
        let PeerRole::Node { node_id } = verified.peer().role() else {
            return Err(AccessError::Unauthorized);
        };
        if !self
            .installed(scope.ledger)
            .is_some_and(|p| p.peers.contains(&node_id))
        {
            return Err(AccessError::Unauthorized);
        }
        let Operation::Custody(operation) = &verified.request().operation else {
            return Err(AccessError::InvalidRequest);
        };
        let output = match operation {
            CustodyRequest::Manifest { max_bytes, .. }
            | CustodyRequest::ReadChunk { max_bytes, .. } => {
                usize::try_from(*max_bytes).map_err(|_| AccessError::Capacity)?
            }
            _ => 0,
        };
        let response = self.reserve(
            BudgetKind::Control,
            BudgetLane::Completion,
            // The value, encoded frame and transport buffer can coexist until ACK.
            output
                .checked_mul(3)
                .and_then(|n| n.checked_add(4096))
                .ok_or(AccessError::Capacity)?,
        )?;
        let value = match operation {
            CustodyRequest::Open {
                transfer,
                policy_revision,
                content,
                manifest,
            } => {
                self.check_scope(
                    CustodyScope {
                        policy_revision: *policy_revision,
                        ..scope
                    },
                    content,
                )?;
                let key = (scope, node_id, *transfer);
                let deadline = self.deadline()?;
                if let Some(existing) = self.transfers.get_mut(&key) {
                    if existing.manifest.reference() != content
                        || existing.manifest.encoded() != manifest
                    {
                        return Err(AccessError::InvalidRequest);
                    }
                    existing.expires = deadline;
                    opened(existing)?
                } else {
                    let total = self.room(content)?;
                    let amount = manifest
                        .len()
                        .checked_mul(4)
                        .and_then(|n| n.checked_add(4096))
                        .ok_or(AccessError::Capacity)?;
                    let allocation =
                        self.reserve(BudgetKind::Control, BudgetLane::Ordinary, amount)?;
                    let descriptor = self
                        .store
                        .prepare_import(content.clone(), manifest.clone())
                        .map_err(content_error)?;
                    if descriptor.resident_bytes().map_err(content_error)? > amount {
                        return Err(AccessError::Capacity);
                    }
                    let _scan = self.reserve(
                        BudgetKind::Payload,
                        BudgetLane::Ordinary,
                        self.store.max_chunk_bytes(),
                    )?;
                    let mut next_missing = 0;
                    for index in 0..descriptor.chunks() {
                        match self.store.read_transfer_chunk(&descriptor, index) {
                            Ok(_) => {
                                next_missing = index.checked_add(1).ok_or(AccessError::Capacity)?
                            }
                            Err(ContentError::Io(error))
                                if error.kind() == std::io::ErrorKind::NotFound =>
                            {
                                break;
                            }
                            Err(error) => return Err(content_error(error)),
                        }
                    }
                    let retained = self.descriptor(descriptor, next_missing, allocation)?;
                    let reply = opened(&retained)?;
                    self.transfers.insert(key, retained);
                    self.transfer_bytes = total;
                    reply
                }
            }
            CustodyRequest::Chunk {
                transfer,
                index,
                bytes,
            } => {
                let deadline = self.deadline()?;
                let retained = self
                    .transfers
                    .get_mut(&(scope, node_id, *transfer))
                    .ok_or(AccessError::Unavailable)?;
                let index_usize = usize::try_from(*index).map_err(|_| AccessError::Capacity)?;
                if index_usize > retained.next_missing {
                    return Err(AccessError::InvalidRequest);
                }
                self.store
                    .import_chunk(&retained.manifest, index_usize, bytes)
                    .map_err(content_error)?;
                retained.expires = deadline;
                if index_usize == retained.next_missing {
                    retained.next_missing = retained
                        .next_missing
                        .checked_add(1)
                        .ok_or(AccessError::Capacity)?;
                }
                CustodyReply::ChunkStored { index: *index }
            }
            CustodyRequest::Seal { transfer } => {
                let deadline = self.deadline()?;
                let _scan = self.reserve(
                    BudgetKind::Payload,
                    BudgetLane::Completion,
                    self.store.max_chunk_bytes(),
                )?;
                let retained = self
                    .transfers
                    .get_mut(&(scope, node_id, *transfer))
                    .ok_or(AccessError::Unavailable)?;
                if retained.next_missing != retained.manifest.chunks() {
                    return Err(AccessError::InvalidRequest);
                }
                let content = self
                    .store
                    .complete_import(&retained.manifest)
                    .map_err(content_error)?;
                retained.expires = deadline;
                CustodyReply::Durable {
                    policy_revision: scope.policy_revision,
                    content,
                }
            }
            CustodyRequest::Verify {
                policy_revision,
                content,
            } => {
                self.check_scope(
                    CustodyScope {
                        policy_revision: *policy_revision,
                        ..scope
                    },
                    content,
                )?;
                let _scan = self.reserve(
                    BudgetKind::Payload,
                    BudgetLane::Completion,
                    self.manifest_allowance()?
                        .checked_add(self.store.max_chunk_bytes())
                        .ok_or(AccessError::Capacity)?,
                )?;
                self.store.verify(content).map_err(content_error)?;
                CustodyReply::Durable {
                    policy_revision: *policy_revision,
                    content: content.clone(),
                }
            }
            CustodyRequest::Manifest {
                policy_revision,
                content,
                max_bytes,
            } => {
                self.check_scope(
                    CustodyScope {
                        policy_revision: *policy_revision,
                        ..scope
                    },
                    content,
                )?;
                let _scan = self.reserve(
                    BudgetKind::Payload,
                    BudgetLane::Ordinary,
                    self.manifest_allowance()?,
                )?;
                let descriptor = self.store.export_manifest(content).map_err(content_error)?;
                if descriptor.encoded().len() > *max_bytes as usize {
                    return Err(AccessError::Capacity);
                }
                CustodyReply::Manifest {
                    content: content.clone(),
                    manifest: descriptor.encoded().to_vec(),
                }
            }
            CustodyRequest::ReadChunk {
                transfer,
                index,
                max_bytes,
            } => {
                let deadline = self.deadline()?;
                let retained = self
                    .transfers
                    .get_mut(&(scope, node_id, *transfer))
                    .ok_or(AccessError::Unavailable)?;
                let index_usize = usize::try_from(*index).map_err(|_| AccessError::Capacity)?;
                if retained
                    .manifest
                    .chunk_length(index_usize)
                    .map_err(content_error)?
                    > *max_bytes as usize
                {
                    return Err(AccessError::Capacity);
                }
                let bytes = self
                    .store
                    .read_transfer_chunk(&retained.manifest, index_usize)
                    .map_err(content_error)?;
                retained.expires = deadline;
                CustodyReply::Chunk {
                    index: *index,
                    bytes,
                }
            }
            CustodyRequest::Cancel { transfer } => {
                self.transfers.remove(&(scope, node_id, *transfer));
                self.recount()?;
                CustodyReply::Cancelled
            }
        };
        Ok(Accounted {
            value,
            _allocation: response,
        })
    }
    /// Caches the parsed tree once per installed scope/root. Chunk reads below
    /// use that descriptor until expiry; they never parse the manifest per chunk.
    pub fn export_manifest(
        &mut self,
        scope: CustodyScope,
        content: ContentRef,
    ) -> Result<Accounted<TransferManifest>, AccessError> {
        self.expire(Instant::now())?;
        self.check_scope(scope, &content)?;
        let key = (scope, content.root);
        let amount = self.manifest_allowance()?;
        let output = self.reserve(BudgetKind::Query, BudgetLane::Ordinary, amount)?;
        if !self.exports.contains_key(&key) {
            let total = self.room(&content)?;
            let allocation = self.reserve(BudgetKind::Control, BudgetLane::Ordinary, amount)?;
            let descriptor = self
                .store
                .export_manifest(&content)
                .map_err(content_error)?;
            if descriptor.resident_bytes().map_err(content_error)? > amount {
                return Err(AccessError::Capacity);
            }
            let retained = self.descriptor(descriptor, 0, allocation)?;
            self.exports.insert(key, retained);
            self.transfer_bytes = total;
        }
        let deadline = self.deadline()?;
        let cached = self.exports.get_mut(&key).ok_or(AccessError::Unavailable)?;
        if cached.manifest.reference() != &content {
            return Err(AccessError::InvalidRequest);
        }
        cached.expires = deadline;
        let value = self
            .store
            .prepare_import(content, cached.manifest.encoded().to_vec())
            .map_err(content_error)?;
        Ok(Accounted {
            value,
            _allocation: output,
        })
    }
    pub fn read_transfer_chunk(
        &mut self,
        scope: CustodyScope,
        content: ContentRef,
        index: usize,
    ) -> Result<Accounted<Vec<u8>>, AccessError> {
        self.expire(Instant::now())?;
        self.check_scope(scope, &content)?;
        let key = (scope, content.root);
        let cached = self.exports.get(&key).ok_or(AccessError::Unavailable)?;
        if cached.manifest.reference() != &content {
            return Err(AccessError::InvalidRequest);
        }
        let amount = cached
            .manifest
            .chunk_length(index)
            .map_err(content_error)?
            .checked_add(4096)
            .ok_or(AccessError::Capacity)?;
        let allocation = self.reserve(BudgetKind::Query, BudgetLane::Ordinary, amount)?;
        let value = self
            .store
            .read_transfer_chunk(&cached.manifest, index)
            .map_err(content_error)?;
        let deadline = self.deadline()?;
        self.exports
            .get_mut(&key)
            .ok_or(AccessError::Unavailable)?
            .expires = deadline;
        Ok(Accounted {
            value,
            _allocation: allocation,
        })
    }
    pub fn read_bytes(
        &self,
        scope: CustodyScope,
        content: ContentRef,
        max_bytes: usize,
    ) -> Result<Accounted<Vec<u8>>, AccessError> {
        self.check_scope(scope, &content)?;
        let length = usize::try_from(content.length).map_err(|_| AccessError::Capacity)?;
        if length > max_bytes {
            return Err(AccessError::Capacity);
        }
        let amount = length
            .checked_add(self.store.max_chunk_bytes())
            .and_then(|n| {
                self.manifest_allowance()
                    .ok()
                    .and_then(|m| n.checked_add(m))
            })
            .ok_or(AccessError::Capacity)?;
        let allocation = self.reserve(BudgetKind::Query, BudgetLane::Ordinary, amount)?;
        let value = self
            .store
            .read_bytes(&content, max_bytes)
            .map_err(content_error)?;
        Ok(Accounted {
            value,
            _allocation: allocation,
        })
    }
    pub(crate) fn accounted<T>(&self, value: T, allocation: Allocation) -> Accounted<T> {
        Accounted {
            value,
            _allocation: allocation,
        }
    }
    pub fn expire(&mut self, now: Instant) -> Result<(), AccessError> {
        self.transfers.retain(|_, transfer| transfer.expires > now);
        self.exports.retain(|_, transfer| transfer.expires > now);
        self.recount()
    }
    fn recount(&mut self) -> Result<(), AccessError> {
        self.transfer_bytes = self
            .transfers
            .values()
            .chain(self.exports.values())
            .try_fold(0u64, |sum, entry| {
                sum.checked_add(entry.manifest.reference().length)
            })
            .ok_or(AccessError::Unavailable)?;
        Ok(())
    }
}
fn opened(transfer: &Transfer) -> Result<CustodyReply, AccessError> {
    Ok(CustodyReply::Opened {
        chunks: u32::try_from(transfer.manifest.chunks()).map_err(|_| AccessError::Capacity)?,
        next_missing: u32::try_from(transfer.next_missing).map_err(|_| AccessError::Capacity)?,
    })
}
pub(crate) fn content_error(error: ContentError) -> AccessError {
    match error {
        ContentError::Capacity => AccessError::Capacity,
        ContentError::Io(_) | ContentError::Failed => AccessError::OutcomeUnknown,
        _ => AccessError::InvalidRequest,
    }
}
