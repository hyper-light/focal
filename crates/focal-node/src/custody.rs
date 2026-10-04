//! One node-owned store with installed session custody policy. Only local file
//! and directory sync is claimed here; aggregate custody belongs to the session.
use focal_evidence::{
    ContentError, ContentStore, ImportCompletion, ObjectVerification, TransferManifest, UploadId,
    UploadSealing,
};
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
    /// Where this node keeps every ledger's checkpoint seeds (25 §5); unset
    /// on a node that hosts no native session, which serves no seed.
    pub seed_root: Option<std::path::PathBuf>,
}
impl CustodyConfig {
    pub fn new(node: u64) -> Self {
        Self {
            node,
            max_policies: 4096,
            max_transfers: 128,
            max_transfer_bytes: 1024 * 1024 * 1024,
            transfer_ttl: Duration::from_secs(60),
            seed_root: None,
        }
    }
}
/// Where a ledger's checkpoint seeds live under a node's seed root: one
/// directory per ledger, named by the session.
pub fn seed_directory(root: &std::path::Path, ledger: LedgerId) -> std::path::PathBuf {
    let name: String = ledger
        .session
        .0
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    root.join(name)
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
/// The peers of a placement the directory is preparing for a ledger this
/// node serves. They may read the ledger's checkpoint seeds (immutable,
/// content-addressed) before the placement activates, so a copy can seed
/// its Session from the current owners while the route is still the old one
/// (25 §5); nothing else is authorized by an announcement.
struct PendingPeers {
    scope: CustodyScope,
    peers: BTreeSet<u64>,
    _allocation: Allocation,
}
/// The bytes of the hash a manifest names a chunk by: a manifest of as
/// many bytes names one chunk at most.
const CHUNK_NAME_BYTES: usize = focal_model::ContentHash([0; 32]).0.len();
struct Transfer {
    manifest: TransferManifest,
    next_missing: usize,
    /// The chunks that were taken, a bit for each chunk of the manifest: a
    /// transfer goes by several streams, and what they carry arrives in no
    /// order. None for a manifest that is sent from.
    taken: Vec<u64>,
    expires: Instant,
    /// The inventory of what this copy holds, under way: the next chunk to
    /// read back, until every chunk has been (the audit's F51). An open is
    /// answered once it is done.
    inventory: Option<usize>,
    /// The seal under way, a chunk a call (the audit's F51), and what it
    /// installed — answered again to a seal asked after it, a lost reply's
    /// retry, while the transfer is held.
    sealing: Option<ImportCompletion>,
    sealed: Option<ContentRef>,
    _allocation: Allocation,
}
type TransferKey = (CustodyScope, u64, [u8; 16]);
/// An object's verification for a scope that asked it (the audit's F51),
/// given up when no ask has come for a transfer's lease. Under way it goes a
/// chunk a call, held and charged. Done, it keeps only its answer, for the
/// asks that joined it while it ran: an ask that gave up and asked again
/// joins the pass under way, and one that found the pass gone with its
/// answer would begin it again from the first chunk, where a pass longer
/// than the asker's patience never ends. A request that comes after the
/// pass is done verifies afresh: the answer was of the object then.
struct Verifying {
    state: Verification,
    expires: Instant,
}
/// A pass under way holds its state on the heap, charged with it
/// (`ObjectVerification::resident_bytes` counts its own size): what is
/// done keeps only its answer, and its entry is no larger than that.
enum Verification {
    Running {
        verification: Box<ObjectVerification>,
        _allocation: Allocation,
    },
    Done(ContentRef),
}
/// An upload's seal, kept by the store between slices (the audit's F51), as
/// a transfer's is: an ask that meets a full queue, or a caller that gives
/// up, leaves it to the next ask, which carries it on from where it stands.
/// Under way it is charged; done, it keeps only the reference it installed,
/// which answers every later ask: an upload's id is never staged again
/// once finished, and its bytes never change, so a seal's reference stands.
/// Given up, done or not, when no ask came for a transfer's lease.
struct Sealing {
    state: Seal,
    expires: Instant,
}
/// As a verification's (`Verification`): the state under way on the heap,
/// charged with it (`UploadSealing::resident_bytes`).
enum Seal {
    Running {
        sealing: Box<UploadSealing>,
        _charge: Allocation,
    },
    Done(ContentRef),
}
/// A backup's content restore (the audit's F51), one at a time, kept
/// between slices as a seal is. Under way it holds the backup's manifest,
/// charged; done, the objects it imported, which answer an ask of the same
/// backup — its directory and its checkpoint's hash and creation time —
/// until no ask came for a transfer's lease.
struct Restoring {
    root: std::path::PathBuf,
    backup: (ContentHash, u64),
    state: Restore,
    expires: Instant,
}
/// As a verification's (`Verification`): the manifest and the import
/// under way on the heap, charged with them.
enum Restore {
    Running {
        manifest: Box<focal_ledger::backup::BackupManifest>,
        import: Box<focal_ledger::backup::ContentImport>,
        _charge: Allocation,
    },
    Done(u64),
}
/// An ask of a backup's content restore (`CustodyStore::restore_slice`).
pub(crate) enum RestoreAsk {
    /// Begin the restore of the backup in a directory, or carry it on.
    Begin(
        std::path::PathBuf,
        Box<focal_ledger::backup::BackupManifest>,
    ),
    /// Carry on the restore of the backup in a directory, named by its
    /// checkpoint's hash and creation time: the restore of any other
    /// backup is not carried on by it.
    Continue(std::path::PathBuf, (ContentHash, u64)),
}
/// A pass over a whole object that a request began and has not finished
/// (the audit's F51): a transfer's inventory or seal, or an object's
/// verification. The store keeps the pass's state between slices; the host
/// asks [`CustodyStore::advance`] for one slice — one chunk read back — at
/// a time, and each ask takes its own turn in the content owner's queue, so
/// other work is served between them. Every request for the same transfer
/// or object advances the same pass.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Pass {
    /// An open, answered with what the copy holds (`held`: by a bit for
    /// each chunk) once the inventory is done.
    Inventory {
        key: TransferKey,
        held: bool,
    },
    Seal {
        key: TransferKey,
    },
    Verify {
        scope: CustodyScope,
        root: ContentHash,
    },
}
/// A request answered, or a pass it began to be advanced.
pub(crate) enum Step<T> {
    Done(T),
    Pending(Pass),
}
/// Room in `entries` for `key` within `bound`. An entry that is `done` only
/// answers the asks that came for it, so at the bound the done one whose
/// lease ends first gives its place up; with none done, the place is
/// refused (`Capacity`).
fn make_room<K: Ord + Clone, V>(
    entries: &mut BTreeMap<K, V>,
    key: &K,
    bound: usize,
    done: impl Fn(&V) -> bool,
    expires: impl Fn(&V) -> Instant,
) -> Result<(), AccessError> {
    if entries.contains_key(key) || entries.len() < bound {
        return Ok(());
    }
    let finished = entries
        .iter()
        .filter(|(_, entry)| done(entry))
        .min_by_key(|(_, entry)| expires(entry))
        .map(|(finished, _)| finished.clone())
        .ok_or(AccessError::Capacity)?;
    entries.remove(&finished);
    Ok(())
}
pub struct CustodyStore {
    store: ContentStore,
    config: CustodyConfig,
    budget: MemoryBudget,
    policies: BTreeMap<LedgerId, Installed>,
    pending: BTreeMap<LedgerId, PendingPeers>,
    transfers: BTreeMap<TransferKey, Transfer>,
    exports: BTreeMap<(CustodyScope, ContentHash), Transfer>,
    /// As many as transfers at most (`max_transfers`).
    verifications: BTreeMap<(CustodyScope, ContentHash), Verifying>,
    /// As many as the store stages uploads at most.
    seals: BTreeMap<UploadId, Sealing>,
    restore: Option<Restoring>,
    transfer_bytes: u64,
    /// What the collector may not touch, installed per pass (26 §5).
    protection: Option<focal_evidence::ProtectionSet>,
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
            pending: BTreeMap::new(),
            transfers: BTreeMap::new(),
            exports: BTreeMap::new(),
            verifications: BTreeMap::new(),
            seals: BTreeMap::new(),
            restore: None,
            transfer_bytes: 0,
            protection: None,
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
    /// Every ledger with an installed custody policy: the sessions whose
    /// content this node holds copies for.
    pub(crate) fn installed_ledgers(&self) -> Result<Vec<LedgerId>, AccessError> {
        let mut ledgers = Vec::new();
        ledgers
            .try_reserve_exact(self.policies.len())
            .map_err(|_| AccessError::Capacity)?;
        ledgers.extend(self.policies.keys().copied());
        Ok(ledgers)
    }
    /// Install the protection set the next collector steps run under.
    pub(crate) fn protect(&mut self, protection: focal_evidence::ProtectionSet) {
        self.protection = Some(protection);
    }
    /// One bounded collector step under the installed protection set.
    pub(crate) fn collect(
        &mut self,
        config: focal_evidence::CollectorConfig,
        now_ms: u64,
        max_items: usize,
    ) -> Result<focal_evidence::CollectorReport, AccessError> {
        let protection = self.protection.as_ref().ok_or(AccessError::Unavailable)?;
        self.store
            .collect_step(protection, config, now_ms, max_items)
            .map_err(content_error)
    }
    pub(crate) fn restore_quarantined(
        &mut self,
        domain: ContentDomainId,
        root: ContentHash,
    ) -> Result<bool, AccessError> {
        self.store
            .restore_quarantined(domain, root)
            .map_err(content_error)
    }
    /// The caller is the trusted control owner. Equal facts retry exactly;
    /// changed facts must move a route or policy fence forward without regression.
    pub fn install_policy(&mut self, policy: CustodyPolicy) -> Result<(), AccessError> {
        let expected = self.installed(policy.ledger).map(CustodyPolicy::scope);
        self.replace_policy(expected, policy)
    }
    /// Compare against the exact installed generation. Repeating the complete
    /// target is idempotent after a lost response; a stale different target fails.
    pub fn replace_policy(
        &mut self,
        expected: Option<CustodyScope>,
        policy: CustodyPolicy,
    ) -> Result<(), AccessError> {
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
            if expected != Some(old.scope())
                || policy.route_epoch < old.route_epoch
                || policy.policy_revision < old.policy_revision
                || (policy.route_epoch == old.route_epoch
                    && policy.policy_revision == old.policy_revision)
            {
                return Err(AccessError::Unavailable);
            }
        } else {
            if expected.is_some() {
                return Err(AccessError::Unavailable);
            }
            if self.policies.len() >= self.config.max_policies {
                return Err(AccessError::Capacity);
            }
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
        // Old descriptors keep their quotas and immutable bytes until their
        // existing lease expires. Every subsequent operation checks the current
        // scope before accessing them; retirement never deletes content or
        // silently releases a transfer's retained custody.
        Ok(())
    }
    /// Announce (or withdraw, with `None`) the peers of a placement the
    /// directory is preparing for `ledger`. The caller is the trusted control
    /// owner; the announcement authorizes seed reads only and never moves the
    /// installed policy. A pending scope must lie beyond the installed one.
    pub fn announce_pending(
        &mut self,
        ledger: LedgerId,
        pending: Option<(CustodyScope, BTreeSet<u64>)>,
    ) -> Result<(), AccessError> {
        let Some((scope, peers)) = pending else {
            self.pending.remove(&ledger);
            return Ok(());
        };
        if scope.ledger != ledger
            || scope.route_epoch.0 == 0
            || scope.policy_revision == 0
            || peers.is_empty()
            || peers.len() > 1024
            || peers.contains(&0)
            || self.installed(ledger).is_some_and(|installed| {
                scope.route_epoch <= installed.route_epoch
                    || scope.policy_revision <= installed.policy_revision
            })
        {
            return Err(AccessError::InvalidRequest);
        }
        if let Some(current) = self.pending.get(&ledger) {
            if current.scope == scope && current.peers == peers {
                return Ok(());
            }
            if scope.route_epoch < current.scope.route_epoch {
                return Err(AccessError::Unavailable);
            }
        } else if self.pending.len() >= self.config.max_policies {
            return Err(AccessError::Capacity);
        }
        let bytes = peers
            .len()
            .checked_mul(128)
            .and_then(|n| n.checked_add(4096))
            .ok_or(AccessError::Capacity)?;
        let allocation = self.reserve(BudgetKind::Control, BudgetLane::Completion, bytes)?;
        self.pending.insert(
            ledger,
            PendingPeers {
                scope,
                peers,
                _allocation: allocation,
            },
        );
        Ok(())
    }
    /// Authorize a read: a node of the installed placement at its route, or
    /// a node of an announced pending placement at that placement's route
    /// (25 §5, 24 §20). A copy the directory is preparing reads the seeds
    /// and objects it lacks from this node before the placement activates.
    fn authorize_read(&self, verified: &VerifiedRequest) -> Result<CustodyScope, AccessError> {
        let request = verified.request();
        let PeerRole::Node { node_id } = verified.peer().role() else {
            return Err(AccessError::Unauthorized);
        };
        let installed = self.installed(request.ledger);
        let pending = self.pending.get(&request.ledger);
        if let Some(policy) = installed
            && policy.route_epoch == request.route_epoch
        {
            return if policy.peers.contains(&node_id) {
                Ok(policy.scope())
            } else {
                Err(AccessError::Unauthorized)
            };
        }
        if let Some(pending) = pending
            && pending.scope.route_epoch == request.route_epoch
        {
            return if pending.peers.contains(&node_id) {
                Ok(pending.scope)
            } else {
                Err(AccessError::Unauthorized)
            };
        }
        if installed.is_some() || pending.is_some() {
            Err(AccessError::Unavailable)
        } else {
            Err(AccessError::Unauthorized)
        }
    }
    /// A read's scope is the installed policy's or the announced pending
    /// placement's (`authorize_read`); the object must be of the tenant.
    fn check_read_scope(
        &self,
        scope: CustodyScope,
        content: &ContentRef,
    ) -> Result<(), AccessError> {
        self.check_read_policy(scope)?;
        if content.domain != ContentDomainId(scope.ledger.tenant.0) {
            return Err(AccessError::Unauthorized);
        }
        Ok(())
    }
    /// A read's scope is still the installed policy's or the announced
    /// pending placement's.
    fn check_read_policy(&self, scope: CustodyScope) -> Result<(), AccessError> {
        let installed = self
            .installed(scope.ledger)
            .is_some_and(|policy| policy.scope() == scope);
        let pending = self
            .pending
            .get(&scope.ledger)
            .is_some_and(|pending| pending.scope == scope);
        if !installed && !pending {
            return Err(AccessError::Unavailable);
        }
        Ok(())
    }
    pub fn check_policy(&self, scope: CustodyScope) -> Result<(), AccessError> {
        let policy = self
            .installed(scope.ledger)
            .ok_or(AccessError::Unauthorized)?;
        if policy.scope() != scope {
            return Err(AccessError::Unavailable);
        }
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
        self.check_policy(scope)?;
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
        taken: Vec<u64>,
        allocation: Allocation,
    ) -> Result<Transfer, AccessError> {
        Ok(Transfer {
            manifest,
            next_missing,
            taken,
            expires: self.deadline()?,
            inventory: None,
            sealing: None,
            sealed: None,
            _allocation: allocation,
        })
    }
    /// Open a transfer of `content` to this copy, or find it open: what the
    /// manifest names is scanned whole, and every chunk the store holds
    /// verified is noted as taken, so the first chunk lacked is the first
    /// the copy lacks and a sender is told what it holds (the audit's F50).
    /// A chunk that fails its hash is lacked: the import installs verified
    /// bytes over it (24 §20). The scan reads and hashes every chunk held, a
    /// chunk a slice ([`Pass::Inventory`], the audit's F51): an open is
    /// answered once it is done.
    fn open_transfer(
        &mut self,
        scope: CustodyScope,
        node_id: u64,
        transfer: [u8; 16],
        policy_revision: u64,
        content: &ContentRef,
        manifest: &[u8],
    ) -> Result<&Transfer, AccessError> {
        self.check_read_scope(
            CustodyScope {
                policy_revision,
                ..scope
            },
            content,
        )?;
        let key = (scope, node_id, transfer);
        let deadline = self.deadline()?;
        if !self.transfers.contains_key(&key) {
            self.take_inventory(key, content, manifest)?;
        }
        // The transfer, found or just opened: one of this object, its lease
        // renewed by the ask.
        let existing = self
            .transfers
            .get_mut(&key)
            .ok_or(AccessError::Unavailable)?;
        if existing.manifest.reference() != content || existing.manifest.encoded() != manifest {
            return Err(AccessError::InvalidRequest);
        }
        existing.expires = deadline;
        Ok(existing)
    }
    /// Open a new transfer of `content` under `key`, with the inventory of
    /// what this copy holds of it.
    fn take_inventory(
        &mut self,
        key: TransferKey,
        content: &ContentRef,
        manifest: &[u8],
    ) -> Result<(), AccessError> {
        let total = self.room(content)?;
        // A bit for each chunk the manifest may name, in words.
        let bits = manifest
            .len()
            .checked_div(CHUNK_NAME_BYTES)
            .unwrap_or(0)
            .div_ceil(8)
            .checked_add(size_of::<u64>())
            .ok_or(AccessError::Capacity)?;
        // The descriptor, the bitmap, and the seal's state the transfer
        // retains while it is sealed (the audit's F51).
        let amount = manifest
            .len()
            .checked_mul(4)
            .and_then(|n| n.checked_add(4096))
            .and_then(|n| n.checked_add(bits))
            .and_then(|n| n.checked_add(size_of::<ImportCompletion>()))
            .ok_or(AccessError::Capacity)?;
        let allocation = self.reserve(BudgetKind::Control, BudgetLane::Ordinary, amount)?;
        let descriptor = self
            .store
            .prepare_import(content.clone(), manifest.to_vec())
            .map_err(content_error)?;
        if descriptor.resident_bytes().map_err(content_error)? > amount {
            return Err(AccessError::Capacity);
        }
        let words = descriptor.chunks().div_ceil(u64::BITS as usize);
        if words.checked_mul(size_of::<u64>()).is_none_or(|n| n > bits) {
            return Err(AccessError::InvalidRequest);
        }
        let mut taken = Vec::new();
        taken
            .try_reserve_exact(words)
            .map_err(|_| AccessError::Capacity)?;
        taken.resize(words, 0);
        let mut retained = self.descriptor(descriptor, 0, taken, allocation)?;
        // Read back a chunk a slice from the first (`CustodyStore::advance`).
        retained.inventory = Some(0);
        self.transfers.insert(key, retained);
        self.transfer_bytes = total;
        Ok(())
    }
    /// A custody request answered, or the pass over a whole object it began
    /// (`Step::Pending`), which [`Self::advance`] carries on a slice at a time.
    pub(crate) fn request(
        &mut self,
        verified: &VerifiedRequest,
    ) -> Result<Step<Accounted<CustodyReply>>, AccessError> {
        self.expire(Instant::now())?;
        // Reads — a seed, an object's manifest, the transfer that describes
        // it, its chunks, a verification — are open to the nodes of the
        // installed placement and of an announced pending one; writes (a
        // chunk received, a seal) only to the installed placement's nodes.
        let read = matches!(
            verified.request().operation,
            Operation::Custody(
                CustodyRequest::SeedChunk { .. }
                    | CustodyRequest::Manifest { .. }
                    // An ask of what a copy holds opens a transfer as an
                    // open does, and is admitted where an open is: a copy
                    // being prepared pulls before its placement activates
                    // (24 §20). Held to the installed placement alone, a
                    // copy's pull of an object it lacked was refused and its
                    // delivery waited on it for ever (the gate run of the
                    // audit's F50, 2026-10-03).
                    | CustodyRequest::Open { .. }
                    | CustodyRequest::OpenHeld { .. }
                    | CustodyRequest::ReadChunk { .. }
                    | CustodyRequest::ReadChunkPart { .. }
                    | CustodyRequest::Verify { .. }
                    | CustodyRequest::Cancel { .. }
            )
        );
        let scope = if read {
            self.authorize_read(verified)?
        } else {
            self.authorize(verified)?
        };
        let PeerRole::Node { node_id } = verified.peer().role() else {
            return Err(AccessError::Unauthorized);
        };
        if !read
            && !self
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
            | CustodyRequest::ReadChunk { max_bytes, .. }
            | CustodyRequest::ReadChunkPart { max_bytes, .. }
            | CustodyRequest::SeedChunk { max_bytes, .. } => {
                usize::try_from(*max_bytes).map_err(|_| AccessError::Capacity)?
            }
            // The inventory answered: a word for every sixty-four chunks
            // the manifest may name.
            CustodyRequest::OpenHeld { manifest, .. } => manifest
                .len()
                .checked_div(CHUNK_NAME_BYTES)
                .unwrap_or(0)
                .div_ceil(u64::BITS as usize)
                .checked_mul(size_of::<u64>())
                .ok_or(AccessError::Capacity)?,
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
                let retained = self.open_transfer(
                    scope,
                    node_id,
                    *transfer,
                    *policy_revision,
                    content,
                    manifest,
                )?;
                if retained.inventory.is_some() {
                    return Ok(Step::Pending(Pass::Inventory {
                        key: (scope, node_id, *transfer),
                        held: false,
                    }));
                }
                opened(retained)?
            }
            CustodyRequest::OpenHeld {
                transfer,
                policy_revision,
                content,
                manifest,
            } => {
                let retained = self.open_transfer(
                    scope,
                    node_id,
                    *transfer,
                    *policy_revision,
                    content,
                    manifest,
                )?;
                if retained.inventory.is_some() {
                    return Ok(Step::Pending(Pass::Inventory {
                        key: (scope, node_id, *transfer),
                        held: true,
                    }));
                }
                opened_held(retained)?
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
                // Any chunk of the manifest, in any order: the store holds
                // each to the hash the manifest names it by.
                self.store
                    .import_chunk(&retained.manifest, index_usize, bytes)
                    .map_err(content_error)?;
                retained.expires = deadline;
                taken(retained, index_usize)?;
                CustodyReply::ChunkStored { index: *index }
            }
            CustodyRequest::ChunkPart {
                transfer,
                index,
                offset,
                bytes,
            } => {
                let deadline = self.deadline()?;
                let retained = self
                    .transfers
                    .get_mut(&(scope, node_id, *transfer))
                    .ok_or(AccessError::Unavailable)?;
                let index_usize = usize::try_from(*index).map_err(|_| AccessError::Capacity)?;
                let length = retained
                    .manifest
                    .chunk_length(index_usize)
                    .map_err(content_error)?;
                let staged = self
                    .store
                    .import_chunk_part(&retained.manifest, index_usize, u64::from(*offset), bytes)
                    .map_err(content_error)?;
                // A part taken is the transfer's progress: its lease is
                // renewed by it, as by a chunk (the audit's F49).
                retained.expires = deadline;
                let staged = u32::try_from(staged).map_err(|_| AccessError::Capacity)?;
                if usize::try_from(staged).map_err(|_| AccessError::Capacity)? < length {
                    CustodyReply::PartStored {
                        index: *index,
                        staged,
                    }
                } else {
                    taken(retained, index_usize)?;
                    CustodyReply::ChunkStored { index: *index }
                }
            }
            CustodyRequest::ReadChunkPart {
                transfer,
                index,
                offset,
                max_bytes,
            } => {
                let deadline = self.deadline()?;
                // The whole chunk is read to verify it; the part is cut
                // from what verified.
                let _scan = self.reserve(
                    BudgetKind::Payload,
                    BudgetLane::Completion,
                    self.store.max_chunk_bytes(),
                )?;
                let retained = self
                    .transfers
                    .get_mut(&(scope, node_id, *transfer))
                    .ok_or(AccessError::Unavailable)?;
                let index_usize = usize::try_from(*index).map_err(|_| AccessError::Capacity)?;
                let length = retained
                    .manifest
                    .chunk_length(index_usize)
                    .map_err(content_error)?;
                let offset_usize = usize::try_from(*offset).map_err(|_| AccessError::Capacity)?;
                if offset_usize >= length || *max_bytes == 0 {
                    return Err(AccessError::InvalidRequest);
                }
                let bytes = self
                    .store
                    .read_transfer_chunk(&retained.manifest, index_usize)
                    .map_err(content_error)?;
                let end = offset_usize
                    .saturating_add(usize::try_from(*max_bytes).map_err(|_| AccessError::Capacity)?)
                    .min(length);
                let part = bytes
                    .get(offset_usize..end)
                    .ok_or(AccessError::InvalidRequest)?
                    .to_vec();
                retained.expires = deadline;
                CustodyReply::ChunkPart {
                    index: *index,
                    offset: *offset,
                    length: u32::try_from(length).map_err(|_| AccessError::Capacity)?,
                    bytes: part,
                }
            }
            CustodyRequest::Seal { transfer } => {
                let key = (scope, node_id, *transfer);
                let deadline = self.deadline()?;
                let retained = self
                    .transfers
                    .get_mut(&key)
                    .ok_or(AccessError::Unavailable)?;
                retained.expires = deadline;
                // Sealed already: a lost reply's retry is answered again.
                let Some(content) = &retained.sealed else {
                    if retained.inventory.is_some()
                        || retained.next_missing != retained.manifest.chunks()
                    {
                        return Err(AccessError::InvalidRequest);
                    }
                    // Every chunk read back and the stream verified, a chunk
                    // a slice, before the manifest is installed (the
                    // audit's F51).
                    return Ok(Step::Pending(Pass::Seal { key }));
                };
                CustodyReply::Durable {
                    policy_revision: scope.policy_revision,
                    content: content.clone(),
                }
            }
            CustodyRequest::Verify {
                policy_revision,
                content,
            } => {
                let read = CustodyScope {
                    policy_revision: *policy_revision,
                    ..scope
                };
                self.check_read_scope(read, content)?;
                let key = (read, content.root);
                let deadline = self.deadline()?;
                // A verification under way is joined. One that is done, or
                // none, begins afresh: an answer was of the object when it
                // was given.
                let joined = match self.verifications.get_mut(&key) {
                    Some(Verifying {
                        state: Verification::Running { verification, .. },
                        expires,
                    }) => {
                        if verification.reference() != content {
                            return Err(AccessError::InvalidRequest);
                        }
                        *expires = deadline;
                        true
                    }
                    Some(Verifying {
                        state: Verification::Done(_),
                        ..
                    })
                    | None => false,
                };
                if !joined {
                    make_room(
                        &mut self.verifications,
                        &key,
                        self.config.max_transfers,
                        |entry| matches!(entry.state, Verification::Done(_)),
                        |entry| entry.expires,
                    )?;
                    // The object's chunk list, decoded from its manifest, is
                    // held while the verification runs.
                    let allocation = self.reserve(
                        BudgetKind::Payload,
                        BudgetLane::Completion,
                        self.manifest_allowance()?,
                    )?;
                    let verification = self.store.begin_verify(content).map_err(content_error)?;
                    if verification.resident_bytes().map_err(content_error)?
                        > self.manifest_allowance()?
                    {
                        return Err(AccessError::Capacity);
                    }
                    self.verifications.insert(
                        key,
                        Verifying {
                            state: Verification::Running {
                                verification: Box::new(verification),
                                _allocation: allocation,
                            },
                            expires: deadline,
                        },
                    );
                }
                // Every chunk read back and the stream verified, a chunk a
                // slice (the audit's F51).
                return Ok(Step::Pending(Pass::Verify {
                    scope: read,
                    root: content.root,
                }));
            }
            CustodyRequest::Manifest {
                policy_revision,
                content,
                max_bytes,
            } => {
                self.check_read_scope(
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
                if let Some(cancelled) = self.transfers.remove(&(scope, node_id, *transfer)) {
                    self.store
                        .discard_chunk_parts(&cancelled.manifest)
                        .map_err(content_error)?;
                }
                self.recount()?;
                CustodyReply::Cancelled
            }
            CustodyRequest::SeedChunk { hash, max_bytes } => {
                // Seeds are content-addressed and immutable; any node of the
                // ledger's installed or announced pending placement may read
                // one this node holds (`authorize_seed`).
                let root = self
                    .config
                    .seed_root
                    .as_ref()
                    .ok_or(AccessError::Unavailable)?;
                let reader = focal_evidence::SeedReader::open(seed_directory(root, scope.ledger))
                    .map_err(|_| AccessError::Unavailable)?;
                let limit = usize::try_from(*max_bytes).map_err(|_| AccessError::Capacity)?;
                let _scan = self.reserve(BudgetKind::Payload, BudgetLane::Ordinary, limit)?;
                // A peer that does not hold the seed says so definitely, so
                // the puller moves to the next peer instead of retrying here.
                let bytes = reader.read(*hash, limit).map_err(|error| match error {
                    ContentError::Io(io) if io.kind() == std::io::ErrorKind::NotFound => {
                        AccessError::InvalidRequest
                    }
                    other => content_error(other),
                })?;
                CustodyReply::SeedChunk { hash: *hash, bytes }
            }
        };
        Ok(Step::Done(Accounted {
            value,
            _allocation: response,
        }))
    }
    /// One slice of upload `id`'s seal (the audit's F51): the first ask
    /// begins it — the volume promised, its chunk list reserved and charged
    /// — and each ask after carries it a chunk on, the store keeping it
    /// between asks; the reference once the manifest is installed, and to
    /// every ask after (`Sealing`). A seal that fails is given up, and the
    /// next ask begins it again, the chunks already installed found in place.
    pub(crate) fn seal_slice(
        &mut self,
        id: UploadId,
        scratch: &mut [u8],
    ) -> Result<Option<ContentRef>, AccessError> {
        self.expire(Instant::now())?;
        let deadline = self.deadline()?;
        if !self.seals.contains_key(&id) {
            make_room(
                &mut self.seals,
                &id,
                self.store.max_uploads(),
                |entry| matches!(entry.state, Seal::Done(_)),
                |entry| entry.expires,
            )?;
            // The seal's chunk list and its manifest's encoding, held while
            // it goes a chunk a slice; the chunks are read into the owner's
            // scratch.
            let amount = self
                .store
                .max_manifest_bytes()
                .checked_mul(4)
                .and_then(|n| n.checked_add(4096))
                .ok_or(AccessError::Capacity)?;
            let charge = self.reserve(BudgetKind::Payload, BudgetLane::Completion, amount)?;
            let sealing = self.store.begin_seal(id).map_err(content_error)?;
            if sealing.resident_bytes().map_err(content_error)? > amount {
                return Err(AccessError::Capacity);
            }
            self.seals.insert(
                id,
                Sealing {
                    state: Seal::Running {
                        sealing: Box::new(sealing),
                        _charge: charge,
                    },
                    expires: deadline,
                },
            );
            return Ok(None);
        }
        let entry = self.seals.get_mut(&id).ok_or(AccessError::Unavailable)?;
        entry.expires = deadline;
        let advanced = match &mut entry.state {
            Seal::Done(reference) => return Ok(Some(reference.clone())),
            Seal::Running { sealing, .. } => self.store.advance_seal(sealing, scratch),
        };
        match advanced {
            Ok(None) => Ok(None),
            Ok(Some(reference)) => {
                // What the seal held is released; its reference stays.
                entry.state = Seal::Done(reference.clone());
                Ok(Some(reference))
            }
            Err(error) => {
                self.seals.remove(&id);
                Err(content_error(error))
            }
        }
    }
    /// One slice of a backup's content restore (the audit's F51). A
    /// restore of the backup asked is carried on a chunk, or answered once
    /// done. One begun for another backup is refused while it is under way,
    /// and gives its place up to a new one once done. The objects imported,
    /// once every one is.
    pub(crate) fn restore_slice(
        &mut self,
        ask: RestoreAsk,
        budget: &MemoryBudget,
        scratch: &mut [u8],
    ) -> Result<Option<u64>, AccessError> {
        self.expire(Instant::now())?;
        let deadline = self.deadline()?;
        let (root, backup) = match ask {
            RestoreAsk::Continue(root, backup) => (root, backup),
            RestoreAsk::Begin(root, manifest) => {
                let backup = (manifest.checkpoint_hash, manifest.created_ms);
                match &self.restore {
                    Some(restore) if restore.root == root && restore.backup == backup => {
                        (root, backup)
                    }
                    Some(Restoring {
                        state: Restore::Running { .. },
                        ..
                    }) => return Err(AccessError::Unavailable),
                    Some(_) | None => {
                        // The place of a restore that is done is given up
                        // before the next is charged.
                        self.restore = None;
                        // The manifest and the import's own state, each on
                        // the heap while the restore runs.
                        let charge = self.reserve(
                            BudgetKind::Recovery,
                            BudgetLane::Completion,
                            manifest
                                .resident_bytes()
                                .and_then(|bytes| {
                                    bytes.checked_add(
                                        size_of::<focal_ledger::backup::ContentImport>(),
                                    )
                                })
                                .ok_or(AccessError::Capacity)?,
                        )?;
                        self.restore = Some(Restoring {
                            root,
                            backup,
                            state: Restore::Running {
                                manifest,
                                import: Box::new(focal_ledger::backup::ContentImport::new()),
                                _charge: charge,
                            },
                            expires: deadline,
                        });
                        return Ok(None);
                    }
                }
            }
        };
        // Only the restore of the backup asked is carried on.
        let restore = self
            .restore
            .as_mut()
            .filter(|restore| restore.root == root && restore.backup == backup)
            .ok_or(AccessError::Unavailable)?;
        restore.expires = deadline;
        let advanced = match &mut restore.state {
            Restore::Done(imported) => return Ok(Some(*imported)),
            Restore::Running {
                manifest, import, ..
            } => import.advance(
                &focal_ledger::backup::FileMedium,
                &restore.root,
                manifest,
                &mut self.store,
                budget,
                scratch,
            ),
        };
        match advanced {
            Ok(None) => Ok(None),
            Ok(Some(imported)) => {
                // What the restore held is released; its count stays.
                restore.state = Restore::Done(imported);
                Ok(Some(imported))
            }
            Err(error) => {
                self.restore = None;
                Err(match error {
                    focal_ledger::backup::BackupError::Content(error) => content_error(error),
                    focal_ledger::backup::BackupError::Capacity
                    | focal_ledger::backup::BackupError::Memory(_) => AccessError::Capacity,
                    _ => AccessError::InvalidRequest,
                })
            }
        }
    }
    /// One slice of a pass a request began (the audit's F51): the next chunk
    /// of a transfer's inventory or seal, or of an object's verification,
    /// read back into `scratch` — the content owner's one buffer, a chunk
    /// long. The request's answer once the pass is done, the pass again
    /// while it is not. The scope is checked at every slice, as every
    /// operation on a transfer checks it, and the transfer or verification
    /// must still be held: a cancel, an expiry or a policy that moved on ends
    /// the pass, with nothing installed.
    pub(crate) fn advance(
        &mut self,
        pass: Pass,
        scratch: &mut [u8],
    ) -> Result<Step<Accounted<CustodyReply>>, AccessError> {
        self.expire(Instant::now())?;
        let deadline = self.deadline()?;
        let (reply, output): (CustodyReply, usize) = match pass {
            Pass::Inventory { key, held } => {
                self.check_read_policy(key.0)?;
                let retained = self
                    .transfers
                    .get_mut(&key)
                    .ok_or(AccessError::Unavailable)?;
                retained.expires = deadline;
                if let Some(next) = retained.inventory {
                    if next < retained.manifest.chunks() {
                        if self
                            .store
                            .holds_chunk(&retained.manifest, next, scratch)
                            .map_err(content_error)?
                        {
                            note(retained, next)?;
                        }
                        retained.inventory =
                            Some(next.checked_add(1).ok_or(AccessError::Capacity)?);
                        return Ok(Step::Pending(pass));
                    }
                    advance(retained)?;
                    retained.inventory = None;
                }
                if held {
                    // The inventory answered: a word for every sixty-four
                    // chunks, charged before it is copied into the reply.
                    let output = retained
                        .taken
                        .len()
                        .checked_mul(size_of::<u64>())
                        .and_then(|n| n.checked_mul(3))
                        .and_then(|n| n.checked_add(4096))
                        .ok_or(AccessError::Capacity)?;
                    let response = self
                        .budget
                        .reserve(BudgetKind::Control, BudgetLane::Completion, output)
                        .map(|reservation| reservation.commit())
                        .map_err(|_| AccessError::Capacity)?;
                    return Ok(Step::Done(Accounted {
                        value: opened_held(retained)?,
                        _allocation: response,
                    }));
                }
                (opened(retained)?, 0)
            }
            Pass::Seal { key } => {
                self.check_policy(key.0)?;
                let retained = self
                    .transfers
                    .get_mut(&key)
                    .ok_or(AccessError::Unavailable)?;
                retained.expires = deadline;
                if let Some(content) = &retained.sealed {
                    let content = content.clone();
                    (
                        CustodyReply::Durable {
                            policy_revision: key.0.policy_revision,
                            content,
                        },
                        0,
                    )
                } else {
                    if retained.inventory.is_some()
                        || retained.next_missing != retained.manifest.chunks()
                    {
                        return Err(AccessError::InvalidRequest);
                    }
                    if retained.sealing.is_none() {
                        retained.sealing = Some(
                            self.store
                                .begin_completion(&retained.manifest)
                                .map_err(content_error)?,
                        );
                    }
                    let completion = retained.sealing.as_mut().ok_or(AccessError::Unavailable)?;
                    match self
                        .store
                        .advance_completion(&retained.manifest, completion, scratch)
                    {
                        Ok(None) => return Ok(Step::Pending(pass)),
                        Ok(Some(content)) => {
                            retained.sealing = None;
                            retained.sealed = Some(content.clone());
                            (
                                CustodyReply::Durable {
                                    policy_revision: key.0.policy_revision,
                                    content,
                                },
                                0,
                            )
                        }
                        Err(error) => {
                            // Begun again by the next ask, from the first
                            // chunk; nothing was installed.
                            retained.sealing = None;
                            return Err(content_error(error));
                        }
                    }
                }
            }
            Pass::Verify { scope, root } => {
                self.check_read_policy(scope)?;
                let key = (scope, root);
                let entry = self
                    .verifications
                    .get_mut(&key)
                    .ok_or(AccessError::Unavailable)?;
                entry.expires = deadline;
                // An ask that joined a pass another ask finished is answered
                // by it (`Verifying`).
                let advanced = match &mut entry.state {
                    Verification::Done(content) => Ok(Some(content.clone())),
                    Verification::Running { verification, .. } => self
                        .store
                        .advance_verify(verification, scratch)
                        .map(|done| done.then(|| verification.reference().clone())),
                };
                match advanced {
                    Ok(None) => return Ok(Step::Pending(pass)),
                    Ok(Some(content)) => {
                        // What the pass held is released; its answer stays.
                        entry.state = Verification::Done(content.clone());
                        (
                            CustodyReply::Durable {
                                policy_revision: scope.policy_revision,
                                content,
                            },
                            0,
                        )
                    }
                    Err(error) => {
                        self.verifications.remove(&key);
                        return Err(content_error(error));
                    }
                }
            }
        };
        // The value, encoded frame and transport buffer can coexist until ACK.
        let response = self.reserve(
            BudgetKind::Control,
            BudgetLane::Completion,
            output
                .checked_mul(3)
                .and_then(|n| n.checked_add(4096))
                .ok_or(AccessError::Capacity)?,
        )?;
        Ok(Step::Done(Accounted {
            value: reply,
            _allocation: response,
        }))
    }
    /// A request answered whole: the pass it begins advanced a slice at a
    /// time to its end, as the content owner advances it.
    #[cfg(test)]
    pub(crate) fn request_whole(
        &mut self,
        verified: &VerifiedRequest,
    ) -> Result<Accounted<CustodyReply>, AccessError> {
        let mut scratch = vec![0u8; self.store.max_chunk_bytes()];
        let mut step = self.request(verified)?;
        for _ in 0..1_000_000 {
            match step {
                Step::Done(reply) => return Ok(reply),
                Step::Pending(pass) => step = self.advance(pass, &mut scratch)?,
            }
        }
        panic!("a pass that never ended")
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
            let retained = self.descriptor(descriptor, 0, Vec::new(), allocation)?;
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
        // A transfer that expired holds no more of the volume: the parts
        // of chunks it had not completed go with it (the audit's F49).
        let mut gone = Vec::new();
        gone.try_reserve_exact(
            self.transfers
                .values()
                .filter(|transfer| transfer.expires <= now)
                .count(),
        )
        .map_err(|_| AccessError::Capacity)?;
        gone.extend(
            self.transfers
                .iter()
                .filter(|(_, transfer)| transfer.expires <= now)
                .map(|(key, _)| *key),
        );
        for key in gone {
            if let Some(expired) = self.transfers.remove(&key) {
                self.store
                    .discard_chunk_parts(&expired.manifest)
                    .map_err(content_error)?;
            }
        }
        self.exports.retain(|_, transfer| transfer.expires > now);
        // A verification, a seal or a restore no ask advanced for a lease is
        // given up, what it held released (the audit's F51); nothing of it
        // was published.
        self.verifications.retain(|_, entry| entry.expires > now);
        self.seals.retain(|_, entry| entry.expires > now);
        if self
            .restore
            .as_ref()
            .is_some_and(|restore| restore.expires <= now)
        {
            self.restore = None;
        }
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
/// Chunk `index` is held whole: noted, and the first chunk lacked is moved
/// past everything held after it without a gap.
fn taken(retained: &mut Transfer, index: usize) -> Result<(), AccessError> {
    note(retained, index)?;
    advance(retained)
}
/// Chunk `index` is held whole: its bit is set.
fn note(retained: &mut Transfer, index: usize) -> Result<(), AccessError> {
    let word = index.checked_div(u64::BITS as usize).unwrap_or(0);
    let bit = u32::try_from(index.checked_rem(u64::BITS as usize).unwrap_or(0))
        .ok()
        .and_then(|bit| 1_u64.checked_shl(bit))
        .ok_or(AccessError::InvalidRequest)?;
    *retained
        .taken
        .get_mut(word)
        .ok_or(AccessError::InvalidRequest)? |= bit;
    Ok(())
}
/// The first chunk lacked is moved past everything held after it without
/// a gap.
fn advance(retained: &mut Transfer) -> Result<(), AccessError> {
    while retained.next_missing < retained.manifest.chunks() {
        let next = retained.next_missing;
        let word = next.checked_div(u64::BITS as usize).unwrap_or(0);
        let bit = u32::try_from(next.checked_rem(u64::BITS as usize).unwrap_or(0))
            .ok()
            .and_then(|bit| 1_u64.checked_shl(bit))
            .ok_or(AccessError::InvalidRequest)?;
        if retained
            .taken
            .get(word)
            .is_none_or(|taken| taken & bit == 0)
        {
            break;
        }
        retained.next_missing = next.checked_add(1).ok_or(AccessError::Capacity)?;
    }
    Ok(())
}
fn opened(transfer: &Transfer) -> Result<CustodyReply, AccessError> {
    Ok(CustodyReply::Opened {
        chunks: u32::try_from(transfer.manifest.chunks()).map_err(|_| AccessError::Capacity)?,
        next_missing: u32::try_from(transfer.next_missing).map_err(|_| AccessError::Capacity)?,
    })
}
/// What the copy holds of the object, a bit for each chunk (the audit's
/// F50); charged to the request's reply beside the request.
fn opened_held(transfer: &Transfer) -> Result<CustodyReply, AccessError> {
    let mut held = Vec::new();
    held.try_reserve_exact(transfer.taken.len())
        .map_err(|_| AccessError::Capacity)?;
    held.extend_from_slice(&transfer.taken);
    Ok(CustodyReply::OpenedHeld {
        chunks: u32::try_from(transfer.manifest.chunks()).map_err(|_| AccessError::Capacity)?,
        held,
    })
}
pub(crate) fn content_error(error: ContentError) -> AccessError {
    match error {
        ContentError::Capacity => AccessError::Capacity,
        ContentError::Io(_) | ContentError::Failed => AccessError::OutcomeUnknown,
        _ => AccessError::InvalidRequest,
    }
}
