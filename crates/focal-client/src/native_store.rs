//! Durable identity for native operations (`n1:` references). The catalogue
//! claims an operation identity before any object identity is minted; the
//! prepared record then holds the exact `FCNINPUT1` frame the client sends,
//! and the journal records the committed native receipt bound to that frame.
//! A retry resends the journaled bytes; it never recompiles the document.
//!
//! An operation's life (the audit's F04–F06): claimed, ready (the frame is
//! durable; only then may bytes leave), committed or refused, reported,
//! retired. A reported operation — its committed receipt delivered, or a
//! closed refusal reported — stays readable for an exact retry until a claim
//! needs its slot; then the longest reported retires: its frame and journal
//! leave and its identity stays taken in the catalogue's bounded retired
//! table, so the journal is bounded by its capacity, not by the work ever
//! done, and an old identity never becomes another operation. A claim whose
//! expansion failed, or that never became ready, is released, since no bytes
//! ever left under it. Every transition reads, judges and writes under one
//! hold of the store's lock.
//!
//! All methods do synchronous I/O under the store's private lock. Invoke them
//! on the CLI/MCP blocking owner outside async work, as for the V1 stores.
use crate::{
    input::{IdGenerator, InputError, parse_id},
    operation_store::{OperationIntent, StoreError, StoreUsage, files},
    pending::OperationContext,
};
use focal_model::*;
use focal_wire::{
    NATIVE_PROTOCOL_VERSION, NativeInvocationRef, NativeMutationReply, NativeReceipt,
    NativeRefusal, Operation, RequestEnvelope, inspect_native_frame,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fmt,
    path::{Path, PathBuf},
    str::FromStr,
};

/// The ordinary actor wire capability; larger frames need a format extension.
pub const MAX_NATIVE_FRAME_BYTES: usize = 1024 * 1024;
const MAX_OPERATIONS: u32 = 4096;
const CATALOGUE: &str = "catalogue.bin";
const PREPARED: &str = "prepared.bin";
const JOURNAL: &str = "journal.bin";
const CATALOGUE_MAGIC: &[u8; 8] = b"FCLNCT01";
const PREPARED_MAGIC: &[u8; 8] = b"FCLNPR01";
const JOURNAL_MAGIC: &[u8; 8] = b"FCLNJR01";
const CATALOGUE_BYTES: usize = 1024 * 1024;
const PREPARED_BYTES: usize =
    MAX_NATIVE_FRAME_BYTES + crate::operation_store::MAX_INTENT_BYTES + 8192;
const JOURNAL_BYTES: usize = 64 * 1024;
const ROOT_RESERVATION: u64 = 2 * CATALOGUE_BYTES as u64 + 4096;
// Two generations of the prepared record and the journal, their temporaries,
// small marker/lock files and bounded filesystem metadata.
const OPERATION_RESERVATION: u64 =
    2 * (PREPARED_BYTES as u64 + 44) + 2 * (JOURNAL_BYTES as u64 + 44) + 32 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum NativeStoreError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("native operation reference must be n1: followed by 32 nonzero hexadecimal digits")]
    InvalidId,
    #[error("native operation store is missing or corrupt")]
    Corrupt,
    #[error("native operation store capacity exceeded")]
    Capacity,
    #[error("native operation belongs to a different cluster, principal, or ledger")]
    ContextMismatch,
    #[error("native operation is already bound to a different authored intent")]
    IntentConflict,
    #[error("native operation has never been admitted; retry cannot create it")]
    MissingOperation,
    #[error(
        "native operation initialization is incomplete; prepare it again under the same intent"
    )]
    Incomplete,
    #[error("prepared native request does not carry this operation's exact identity")]
    InvalidRequest,
    #[error("native receipt does not prove the exact journaled frame committed")]
    ReceiptMismatch,
    #[error("reply is not a committed native receipt; the operation remains pending")]
    NotCommitted,
    #[error("native operation expansion failed: {0}")]
    Expansion(#[from] InputError),
    #[error(
        "native operation was delivered and retired; its identity stays taken and the owner answers an exact retry from its receipt"
    )]
    Retired,
}

/// The capability is stored with the catalogue; retrying with different
/// limits cannot silently increase it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeStoreLimits {
    pub max_operations: u32,
    pub max_reserved_bytes: u64,
}
impl Default for NativeStoreLimits {
    fn default() -> Self {
        Self {
            max_operations: 256,
            max_reserved_bytes: 1024 * 1024 * 1024,
        }
    }
}
impl NativeStoreLimits {
    fn validate(self) -> Result<(), NativeStoreError> {
        if self.max_operations == 0
            || self.max_operations > MAX_OPERATIONS
            || self.max_reserved_bytes < ROOT_RESERVATION
        {
            return Err(NativeStoreError::Capacity);
        }
        Ok(())
    }
}

/// Object identities a compiled frame mints, recorded so a host can report
/// them after the commit without decoding the frame again.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeIdentityKind {
    Claim,
    Validation,
    Receipt,
    Artifact,
    Response,
    ResultTestament,
    Monitor,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeIdentity {
    pub kind: NativeIdentityKind,
    pub id: [u8; 16],
}

/// `n1:` plus the request identity. The request epoch is fixed at one for
/// native operations; exactness comes from the request key and the frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct NativeOperationId(RequestId);
impl NativeOperationId {
    pub fn request(self) -> RequestId {
        self.0
    }
    pub fn key(self, context: &OperationContext) -> RequestKey {
        RequestKey {
            principal: context.principal,
            epoch: RequestEpoch(1),
            id: self.0,
        }
    }
}
impl fmt::Display for NativeOperationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "n1:{}", self.0)
    }
}
impl FromStr for NativeOperationId {
    type Err = NativeStoreError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let id = value
            .strip_prefix("n1:")
            .ok_or(NativeStoreError::InvalidId)?;
        Ok(Self(RequestId(
            parse_id(id).map_err(|_| NativeStoreError::InvalidId)?,
        )))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeStage {
    Pending,
    Completed,
}

/// What the compiler produced for one claimed identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedNativeRequest {
    pub request: RequestEnvelope,
    /// The owner-side intent fingerprint of the frame, recomputed locally.
    pub fingerprint: ContentHash,
    pub created: Vec<NativeIdentity>,
}
/// A durable native operation: the exact request and its committed receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeOperation {
    pub id: NativeOperationId,
    pub context: OperationContext,
    pub name: String,
    pub version: u16,
    pub request: RequestEnvelope,
    pub fingerprint: ContentHash,
    pub created: Vec<NativeIdentity>,
    pub receipt: Option<NativeReceipt>,
    pub refusal: Option<NativeRefusal>,
    pub delivered: bool,
}
impl NativeOperation {
    pub fn stage(&self) -> NativeStage {
        if self.receipt.is_some() {
            NativeStage::Completed
        } else {
            NativeStage::Pending
        }
    }
    pub fn key(&self) -> RequestKey {
        self.id.key(&self.context)
    }
}

/// The catalogue as schema 1 wrote it: read once more and carried forward.
#[derive(Serialize, Deserialize)]
struct CatalogueV1 {
    schema: u16,
    limits: NativeStoreLimits,
    entries: BTreeMap<[u8; 16], EntryV1>,
}
#[derive(Serialize, Deserialize)]
struct EntryV1 {
    context: OperationContext,
    intent: [u8; 32],
    ready: bool,
}
#[derive(Serialize, Deserialize)]
struct Catalogue {
    schema: u16,
    limits: NativeStoreLimits,
    /// The live operations: claimed, ready, committed or refused but not yet
    /// delivered — the journal's occupancy.
    entries: BTreeMap<[u8; 16], Entry>,
    /// Identities of delivered operations, kept taken after their frames
    /// left: at most `max_operations` of them, the oldest leaving first.
    retired: BTreeMap<[u8; 16], Retired>,
    /// The order the next report or retirement takes.
    retired_next: u64,
}
const CATALOGUE_SCHEMA: u16 = 2;
impl From<CatalogueV1> for Catalogue {
    fn from(old: CatalogueV1) -> Self {
        Self {
            schema: CATALOGUE_SCHEMA,
            limits: old.limits,
            entries: old
                .entries
                .into_iter()
                .map(|(id, entry)| {
                    (
                        id,
                        Entry {
                            context: entry.context,
                            intent: entry.intent,
                            ready: entry.ready,
                            retirable: None,
                        },
                    )
                })
                .collect(),
            retired: BTreeMap::new(),
            retired_next: 0,
        }
    }
}
#[derive(Serialize, Deserialize)]
struct Entry {
    context: OperationContext,
    intent: [u8; 32],
    ready: bool,
    /// The order at which the operation's result was reported and it may
    /// retire: a committed receipt delivered, or a closed refusal reported.
    /// A capacity refusal keeps the frame for its retry and is never
    /// retirable.
    retirable: Option<u64>,
}
/// What a delivered operation leaves behind: the intent it was bound to and
/// how it ended, so an exact retry is answered and the identity is never
/// another operation's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct Retired {
    intent: [u8; 32],
    outcome: NativeRetired,
    order: u64,
}
/// How a retired operation ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeRetired {
    /// Committed at the sequence, the receipt's digest kept.
    Committed { receipt: [u8; 32], sequence: u64 },
    /// Closed by the owner's refusal, reported.
    Refused,
}
impl Catalogue {
    fn usage(&self) -> Result<StoreUsage, NativeStoreError> {
        let operations =
            u32::try_from(self.entries.len()).map_err(|_| NativeStoreError::Capacity)?;
        let reserved_bytes = u64::from(operations)
            .checked_mul(OPERATION_RESERVATION)
            .and_then(|bytes| bytes.checked_add(ROOT_RESERVATION))
            .ok_or(NativeStoreError::Capacity)?;
        Ok(StoreUsage {
            operations,
            reserved_bytes,
        })
    }
    fn validate(&self, limits: NativeStoreLimits) -> Result<(), NativeStoreError> {
        if self.schema != CATALOGUE_SCHEMA
            || self.entries.keys().any(|id| *id == [0; 16])
            || self.retired.keys().any(|id| *id == [0; 16])
            || self.retired.keys().any(|id| self.entries.contains_key(id))
            || self.retired.len() > limits.max_operations as usize
        {
            return Err(NativeStoreError::Corrupt);
        }
        if self.limits != limits {
            return Err(StoreError::LimitsMismatch.into());
        }
        limits.validate()?;
        let usage = self.usage()?;
        if usage.operations > limits.max_operations
            || usage.reserved_bytes > limits.max_reserved_bytes
        {
            return Err(NativeStoreError::Capacity);
        }
        for entry in self.entries.values() {
            crate::operation_store::validate_context(entry.context)?;
        }
        Ok(())
    }
    /// The next order: reports and retirements share one clock.
    fn next_order(&mut self) -> Result<u64, NativeStoreError> {
        let order = self.retired_next;
        self.retired_next = order.checked_add(1).ok_or(NativeStoreError::Capacity)?;
        Ok(order)
    }
    /// The reported operation that has waited longest: the one a claim in
    /// need of a slot retires.
    fn longest_reported(&self) -> Option<[u8; 16]> {
        self.entries
            .iter()
            .filter_map(|(id, entry)| entry.retirable.map(|order| (order, *id)))
            .min()
            .map(|(_, id)| id)
    }
    /// The operation leaves the live entries and its identity joins the
    /// retired ones; beyond the bound the oldest retired identity leaves.
    fn retire(
        &mut self,
        id: [u8; 16],
        intent: [u8; 32],
        outcome: NativeRetired,
        limits: NativeStoreLimits,
    ) -> Result<(), NativeStoreError> {
        self.entries.remove(&id);
        let order = self.next_order()?;
        self.retired.insert(
            id,
            Retired {
                intent,
                outcome,
                order,
            },
        );
        while self.retired.len() > limits.max_operations as usize {
            let Some(oldest) = self
                .retired
                .iter()
                .min_by_key(|(_, retired)| retired.order)
                .map(|(id, _)| *id)
            else {
                break;
            };
            self.retired.remove(&oldest);
        }
        Ok(())
    }
}
#[derive(Serialize, Deserialize)]
struct Prepared {
    schema: u16,
    context: OperationContext,
    name: String,
    version: u16,
    canonical: Vec<u8>,
    request: RequestEnvelope,
    fingerprint: ContentHash,
    created: Vec<NativeIdentity>,
}
impl Prepared {
    fn intent(&self) -> OperationIntent<'_> {
        OperationIntent {
            name: &self.name,
            version: self.version,
            canonical: &self.canonical,
        }
    }
    fn validate(&self, id: [u8; 16]) -> Result<(), NativeStoreError> {
        if self.schema != 1 {
            return Err(NativeStoreError::Corrupt);
        }
        crate::operation_store::validate_context(self.context)?;
        self.intent().digest()?;
        validate_request(&self.context, id, &self.request, self.fingerprint)
    }
}
#[derive(Serialize, Deserialize)]
struct Journal {
    schema: u16,
    generation: u64,
    receipt: Option<NativeReceipt>,
    /// The last closed refusal the owner returned for the exact frame. A
    /// refusal is reported, not retried automatically; the frame stays
    /// journaled so an explicit retry re-asks the owner.
    refusal: Option<NativeRefusal>,
    /// The host reported the committed result or the refusal to its caller;
    /// until then the operation stays listed so a lost reply can be recovered.
    delivered: bool,
}

fn validate_request(
    context: &OperationContext,
    id: [u8; 16],
    request: &RequestEnvelope,
    fingerprint: ContentHash,
) -> Result<(), NativeStoreError> {
    let Operation::Native { frame } = &request.operation else {
        return Err(NativeStoreError::InvalidRequest);
    };
    if request.protocol != NATIVE_PROTOCOL_VERSION
        || request.ledger != context.ledger
        || request.request_epoch != RequestEpoch(1)
        || request.request_id != RequestId(id)
        || request.route_epoch.0 == 0
        || frame.len() > MAX_NATIVE_FRAME_BYTES
        || fingerprint.0 == [0; 32]
    {
        return Err(NativeStoreError::InvalidRequest);
    }
    let header = inspect_native_frame(frame).map_err(|_| NativeStoreError::InvalidRequest)?;
    if header.namespace != focal_wire::NATIVE_ACTOR_NAMESPACE
        || header.ledger != context.ledger
        || header.key
            != (RequestKey {
                principal: context.principal,
                epoch: RequestEpoch(1),
                id: RequestId(id),
            })
    {
        return Err(NativeStoreError::InvalidRequest);
    }
    Ok(())
}
fn validate_receipt(
    context: &OperationContext,
    id: [u8; 16],
    fingerprint: ContentHash,
    receipt: &NativeReceipt,
) -> Result<(), NativeStoreError> {
    let key = RequestKey {
        principal: context.principal,
        epoch: RequestEpoch(1),
        id: RequestId(id),
    };
    if receipt.invocation != NativeInvocationRef::Request(key)
        || receipt.intent != fingerprint
        || receipt.sequence.0 == 0
    {
        return Err(NativeStoreError::ReceiptMismatch);
    }
    Ok(())
}
fn component(id: [u8; 16]) -> String {
    format!("{:032x}", u128::from_be_bytes(id))
}
fn encode<T: Serialize>(value: &T, maximum: usize) -> Result<Vec<u8>, NativeStoreError> {
    let size =
        postcard::experimental::serialized_size(value).map_err(|_| NativeStoreError::Corrupt)?;
    if size > maximum {
        return Err(NativeStoreError::Capacity);
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(size)
        .map_err(|_| NativeStoreError::Capacity)?;
    bytes.resize(size, 0);
    postcard::to_slice(value, &mut bytes).map_err(|_| NativeStoreError::Corrupt)?;
    Ok(bytes)
}
fn decode<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, NativeStoreError> {
    let (value, remaining): (T, _) =
        postcard::take_from_bytes(bytes).map_err(|_| NativeStoreError::Corrupt)?;
    if !remaining.is_empty() {
        return Err(NativeStoreError::Corrupt);
    }
    Ok(value)
}

/// No store lock or background owner survives a method call. Creation is
/// separate from opening so losing a whole store cannot silently re-admit IDs.
pub struct NativeOperationStore {
    root: PathBuf,
    limits: NativeStoreLimits,
    #[cfg(test)]
    fault: std::sync::Mutex<Option<Fault>>,
}
impl NativeOperationStore {
    pub fn create(
        root: impl AsRef<Path>,
        limits: NativeStoreLimits,
    ) -> Result<Self, NativeStoreError> {
        limits.validate()?;
        let catalogue = Catalogue {
            schema: CATALOGUE_SCHEMA,
            limits,
            entries: BTreeMap::new(),
            retired: BTreeMap::new(),
            retired_next: 0,
        };
        let bytes = encode(&catalogue, CATALOGUE_BYTES)?;
        let directory = files::Directory::create_native(root.as_ref())?;
        directory.initialize()?;
        directory.write(CATALOGUE, CATALOGUE_MAGIC, &bytes, false)?;
        Ok(Self::handle(root.as_ref(), limits))
    }
    pub fn open(
        root: impl AsRef<Path>,
        limits: NativeStoreLimits,
    ) -> Result<Self, NativeStoreError> {
        limits.validate()?;
        let store = Self::handle(root.as_ref(), limits);
        let directory = files::Directory::open_native(&store.root)?;
        let catalogue = store.catalogue(&directory)?;
        store.sweep(&directory, catalogue)?;
        Ok(store)
    }
    /// What an interrupted transition left (the audit's F04, F05): a
    /// directory of a retired or unknown identity (retired before its removal
    /// was durable), and a claim that never became ready — no bytes ever
    /// left under it, and its caller was answered with the failure — leave;
    /// the operations that matter are untouched. Under the lock a claim seen
    /// not ready is not being prepared: preparation holds the lock throughout.
    fn sweep(
        &self,
        directory: &files::Directory,
        mut catalogue: Catalogue,
    ) -> Result<(), NativeStoreError> {
        let mut changed = false;
        let unready: Vec<[u8; 16]> = catalogue
            .entries
            .iter()
            .filter(|(_, entry)| !entry.ready)
            .map(|(id, _)| *id)
            .collect();
        for id in unready {
            catalogue.entries.remove(&id);
            changed = true;
        }
        if changed {
            self.save(directory, &catalogue)?;
        }
        for name in directory.children(MAX_OPERATIONS as usize)? {
            let Some(id) = identity(&name) else {
                continue;
            };
            if !catalogue.entries.contains_key(&id) {
                directory.remove_child(&name)?;
            }
        }
        Ok(())
    }
    /// How a retired operation ended, for an identity that retired.
    pub fn retired(
        &self,
        id: NativeOperationId,
    ) -> Result<Option<NativeRetired>, NativeStoreError> {
        let directory = files::Directory::open_native(&self.root)?;
        Ok(self
            .catalogue(&directory)?
            .retired
            .get(&id.0.0)
            .map(|retired| retired.outcome))
    }
    /// Identities kept taken after their operations retired.
    pub fn retired_count(&self) -> Result<usize, NativeStoreError> {
        let directory = files::Directory::open_native(&self.root)?;
        Ok(self.catalogue(&directory)?.retired.len())
    }
    fn handle(root: &Path, limits: NativeStoreLimits) -> Self {
        Self {
            root: root.into(),
            limits,
            #[cfg(test)]
            fault: std::sync::Mutex::new(None),
        }
    }
    pub fn root(&self) -> &Path {
        &self.root
    }
    pub fn usage(&self) -> Result<StoreUsage, NativeStoreError> {
        let directory = files::Directory::open_native(&self.root)?;
        self.catalogue(&directory)?.usage()
    }
    /// Claim an identity for `intent` and persist the exact request `expand`
    /// compiles for it. With `operation` given, an existing identity under the
    /// same context and intent is returned without expanding again; a different
    /// intent is refused. Without it, a fresh unused identity is minted.
    pub fn prepare(
        &self,
        context: OperationContext,
        intent: OperationIntent<'_>,
        operation: Option<NativeOperationId>,
        ids: &mut dyn IdGenerator,
        expand: impl FnOnce(RequestId) -> Result<PreparedNativeRequest, NativeStoreError>,
    ) -> Result<NativeOperation, NativeStoreError> {
        crate::operation_store::validate_context(context)?;
        let digest = intent.digest()?;
        let directory = files::Directory::open_native(&self.root)?;
        let mut catalogue = self.catalogue(&directory)?;
        let id = match operation {
            Some(operation) => operation.0.0,
            None => {
                let mut id = [0; 16];
                for _ in 0..16 {
                    let candidate = ids.next_id()?;
                    if candidate != [0; 16]
                        && !catalogue.entries.contains_key(&candidate)
                        && !catalogue.retired.contains_key(&candidate)
                    {
                        id = candidate;
                        break;
                    }
                }
                if id == [0; 16] {
                    return Err(NativeStoreError::Expansion(InputError::Identity));
                }
                id
            }
        };
        // A retired identity is taken for good: never another operation,
        // whatever the intent.
        if catalogue.retired.contains_key(&id) {
            return Err(NativeStoreError::Retired);
        }
        let component = component(id);
        let prepared_path = format!("{component}/{PREPARED}");
        let mut prepared = None;
        if let Some(entry) = catalogue.entries.get(&id) {
            if entry.context != context {
                return Err(NativeStoreError::ContextMismatch);
            }
            if entry.intent != digest {
                return Err(NativeStoreError::IntentConflict);
            }
        } else {
            let full = |usage: StoreUsage| {
                usage.operations >= self.limits.max_operations
                    || usage
                        .reserved_bytes
                        .checked_add(OPERATION_RESERVATION)
                        .is_none_or(|bytes| bytes > self.limits.max_reserved_bytes)
            };
            if full(catalogue.usage()?) {
                // The journal is full of work: the operation reported
                // longest ago retires to make the room (the audit's F04);
                // with none reported, the claim is refused.
                let oldest = catalogue
                    .longest_reported()
                    .ok_or(NativeStoreError::Capacity)?;
                let journal = self.journal(&directory, oldest)?;
                let outcome = match &journal.receipt {
                    Some(receipt) => NativeRetired::Committed {
                        receipt: digest_of(receipt)?,
                        sequence: receipt.sequence.0,
                    },
                    None => NativeRetired::Refused,
                };
                self.retire(&directory, &mut catalogue, oldest, outcome)?;
                if full(catalogue.usage()?) {
                    return Err(NativeStoreError::Capacity);
                }
            }
            // Never adopt an unindexed operation directory after metadata loss.
            if directory.exists(&component)? {
                return Err(NativeStoreError::Corrupt);
            }
            catalogue.entries.insert(
                id,
                Entry {
                    context,
                    intent: digest,
                    ready: false,
                    retirable: None,
                },
            );
            self.save(&directory, &catalogue)?;
            #[cfg(test)]
            self.fail_at(Fault::Claimed)?;
        }
        let ready = catalogue
            .entries
            .get(&id)
            .ok_or(NativeStoreError::Corrupt)?
            .ready;
        if !ready && !directory.exists(&prepared_path)? {
            // Nothing durable names this identity beyond the claim, so the
            // compiler may run (again); no bytes have ever left this store.
            let expanded = match expand(RequestId(id)) {
                Ok(expanded) => expanded,
                Err(error) => {
                    // The claim is released with the failure (the audit's
                    // F05): nothing named it and nothing left under it, so
                    // it holds no slot unseen.
                    catalogue.entries.remove(&id);
                    self.save(&directory, &catalogue)?;
                    return Err(error);
                }
            };
            validate_request(&context, id, &expanded.request, expanded.fingerprint)?;
            let value = Prepared {
                schema: 1,
                context,
                name: intent.name.into(),
                version: intent.version,
                canonical: intent.canonical.into(),
                request: expanded.request,
                fingerprint: expanded.fingerprint,
                created: expanded.created,
            };
            let bytes = encode(&value, PREPARED_BYTES)?;
            if !directory.exists(&component)? {
                directory.create_child(&component)?;
            }
            directory.write(&prepared_path, PREPARED_MAGIC, &bytes, false)?;
            #[cfg(test)]
            self.fail_at(Fault::Prepared)?;
            prepared = Some(value);
        }
        let prepared = match prepared {
            Some(value) => value,
            None => self.prepared(&directory, id, ready)?,
        };
        if prepared.context != context {
            return Err(NativeStoreError::ContextMismatch);
        }
        if prepared.name != intent.name
            || prepared.version != intent.version
            || prepared.canonical != intent.canonical
        {
            return Err(NativeStoreError::IntentConflict);
        }
        if !ready {
            catalogue
                .entries
                .get_mut(&id)
                .ok_or(NativeStoreError::Corrupt)?
                .ready = true;
            self.save(&directory, &catalogue)?;
            #[cfg(test)]
            self.fail_at(Fault::Ready)?;
        }
        self.operation(&directory, id, prepared)
    }
    /// Reopen a claimed identity for an exact retry. Recovery never expands
    /// authored input or generates identities.
    pub fn retry(
        &self,
        id: NativeOperationId,
        context: &OperationContext,
    ) -> Result<NativeOperation, NativeStoreError> {
        let directory = files::Directory::open_native(&self.root)?;
        let (_, prepared, journal) = self.journaled(&directory, id, context)?;
        Ok(assemble(id.0.0, prepared, journal))
    }
    /// A live, ready operation's catalogue, frame and journal, read under
    /// one hold of the lock — the state every transition judges and writes
    /// against (the audit's F06).
    fn journaled(
        &self,
        directory: &files::Directory,
        id: NativeOperationId,
        context: &OperationContext,
    ) -> Result<(Catalogue, Prepared, Journal), NativeStoreError> {
        crate::operation_store::validate_context(*context)?;
        let catalogue = self.catalogue(directory)?;
        if catalogue.retired.contains_key(&id.0.0) {
            return Err(NativeStoreError::Retired);
        }
        let entry = catalogue
            .entries
            .get(&id.0.0)
            .ok_or(NativeStoreError::MissingOperation)?;
        if entry.context != *context {
            return Err(NativeStoreError::ContextMismatch);
        }
        let prepared = self.prepared(directory, id.0.0, entry.ready)?;
        if prepared.intent().digest()? != entry.intent {
            return Err(NativeStoreError::Corrupt);
        }
        if !entry.ready {
            return Err(NativeStoreError::Incomplete);
        }
        let journal = self.journal(directory, id.0.0)?;
        if let Some(receipt) = &journal.receipt {
            validate_receipt(&prepared.context, id.0.0, prepared.fingerprint, receipt)?;
        }
        Ok((catalogue, prepared, journal))
    }
    fn write_journal(
        &self,
        directory: &files::Directory,
        id: [u8; 16],
        journal: &Journal,
    ) -> Result<(), NativeStoreError> {
        let path = format!("{}/{JOURNAL}", component(id));
        let replace = directory.exists(&path)?;
        directory.write(
            &path,
            JOURNAL_MAGIC,
            &encode(journal, JOURNAL_BYTES)?,
            replace,
        )?;
        Ok(())
    }
    /// The operation retires: its identity joins the retired table with how
    /// it ended (durable first), then its frame and journal leave. A crash
    /// between the two leaves a directory the next open sweeps.
    fn retire(
        &self,
        directory: &files::Directory,
        catalogue: &mut Catalogue,
        id: [u8; 16],
        outcome: NativeRetired,
    ) -> Result<(), NativeStoreError> {
        let intent = catalogue
            .entries
            .get(&id)
            .map(|entry| entry.intent)
            .ok_or(NativeStoreError::MissingOperation)?;
        catalogue.retire(id, intent, outcome, self.limits)?;
        self.save(directory, catalogue)?;
        #[cfg(test)]
        self.fail_at(Fault::Retired)?;
        directory.remove_child(&component(id))?;
        Ok(())
    }
    /// The operation's result was reported: it may retire when a claim
    /// needs its slot. Nothing changes for one reported already.
    fn mark_reported(
        &self,
        directory: &files::Directory,
        catalogue: &mut Catalogue,
        id: [u8; 16],
    ) -> Result<(), NativeStoreError> {
        let order = catalogue.next_order()?;
        let entry = catalogue
            .entries
            .get_mut(&id)
            .ok_or(NativeStoreError::MissingOperation)?;
        if entry.retirable.is_some() {
            return Ok(());
        }
        entry.retirable = Some(order);
        self.save(directory, catalogue)
    }
    /// Record only a committed receipt bound to the exact journaled frame.
    /// Pending tickets and refusals never advance the journal; a receipt
    /// already recorded is never replaced.
    pub fn record_reply(
        &self,
        id: NativeOperationId,
        context: &OperationContext,
        reply: &NativeMutationReply,
    ) -> Result<NativeStage, NativeStoreError> {
        let NativeMutationReply::Committed(receipt) = reply else {
            return Err(NativeStoreError::NotCommitted);
        };
        let directory = files::Directory::open_native(&self.root)?;
        let (_, prepared, journal) = self.journaled(&directory, id, context)?;
        validate_receipt(context, id.0.0, prepared.fingerprint, receipt)?;
        if let Some(existing) = &journal.receipt {
            return if existing == receipt {
                Ok(NativeStage::Completed)
            } else {
                Err(NativeStoreError::ReceiptMismatch)
            };
        }
        self.write_journal(
            &directory,
            id.0.0,
            &Journal {
                schema: 1,
                generation: journal
                    .generation
                    .checked_add(1)
                    .ok_or(NativeStoreError::Corrupt)?,
                receipt: Some(*receipt),
                refusal: None,
                delivered: false,
            },
        )?;
        Ok(NativeStage::Completed)
    }
    /// The committed result was reported to the caller: the operation is no
    /// longer outstanding and may retire when a claim needs its slot (the
    /// audit's F04); until then an exact retry is answered from its journal.
    /// A receipt is never delivered before it is recorded; a repeated
    /// acknowledgment, of a retired operation too, is harmless. A capacity
    /// refusal is no result: its frame waits for the retry.
    pub fn record_delivered(
        &self,
        id: NativeOperationId,
        context: &OperationContext,
    ) -> Result<(), NativeStoreError> {
        let directory = files::Directory::open_native(&self.root)?;
        let (mut catalogue, _, journal) = match self.journaled(&directory, id, context) {
            Ok(loaded) => loaded,
            Err(NativeStoreError::Retired) => return Ok(()),
            Err(error) => return Err(error),
        };
        match (&journal.receipt, &journal.refusal) {
            (Some(receipt), _) => {
                if !journal.delivered {
                    self.write_journal(
                        &directory,
                        id.0.0,
                        &Journal {
                            schema: 1,
                            generation: journal
                                .generation
                                .checked_add(1)
                                .ok_or(NativeStoreError::Corrupt)?,
                            receipt: Some(*receipt),
                            refusal: None,
                            delivered: true,
                        },
                    )?;
                }
            }
            (None, Some(refusal))
                if !matches!(refusal.kind, focal_wire::NativeRefusalKind::Capacity) => {}
            _ => return Err(NativeStoreError::NotCommitted),
        }
        self.mark_reported(&directory, &mut catalogue, id.0.0)
    }
    /// Record a refusal that was reported to the caller. A committed receipt
    /// is never overwritten. A capacity refusal admitted nothing and keeps
    /// the exact frame journaled for a later retry; every other refusal
    /// closes the frame for good, and the operation may retire when a claim
    /// needs its slot.
    pub fn record_refusal(
        &self,
        id: NativeOperationId,
        context: &OperationContext,
        refusal: &NativeRefusal,
    ) -> Result<(), NativeStoreError> {
        if refusal.detail.len() > 4096 {
            return Err(NativeStoreError::Capacity);
        }
        let directory = files::Directory::open_native(&self.root)?;
        let (mut catalogue, _, journal) = self.journaled(&directory, id, context)?;
        if journal.receipt.is_some() {
            return Err(NativeStoreError::ReceiptMismatch);
        }
        self.write_journal(
            &directory,
            id.0.0,
            &Journal {
                schema: 1,
                generation: journal
                    .generation
                    .checked_add(1)
                    .ok_or(NativeStoreError::Corrupt)?,
                receipt: None,
                refusal: Some(refusal.clone()),
                delivered: true,
            },
        )?;
        if matches!(refusal.kind, focal_wire::NativeRefusalKind::Capacity) {
            return Ok(());
        }
        self.mark_reported(&directory, &mut catalogue, id.0.0)
    }
    /// Ready operations whose result has not been reported to the caller, in
    /// identity order: pending ones and committed ones whose reply was lost.
    pub fn outstanding(&self) -> Result<Vec<NativeOperationId>, NativeStoreError> {
        let directory = files::Directory::open_native(&self.root)?;
        let catalogue = self.catalogue(&directory)?;
        let mut pending = Vec::new();
        for (id, entry) in &catalogue.entries {
            if !entry.ready {
                continue;
            }
            if !self.journal(&directory, *id)?.delivered {
                pending
                    .try_reserve(1)
                    .map_err(|_| NativeStoreError::Capacity)?;
                pending.push(NativeOperationId(RequestId(*id)));
            }
        }
        Ok(pending)
    }
    fn operation(
        &self,
        directory: &files::Directory,
        id: [u8; 16],
        prepared: Prepared,
    ) -> Result<NativeOperation, NativeStoreError> {
        let journal = self.journal(directory, id)?;
        if let Some(receipt) = &journal.receipt {
            validate_receipt(&prepared.context, id, prepared.fingerprint, receipt)?;
        }
        Ok(assemble(id, prepared, journal))
    }
    fn journal(
        &self,
        directory: &files::Directory,
        id: [u8; 16],
    ) -> Result<Journal, NativeStoreError> {
        let path = format!("{}/{JOURNAL}", component(id));
        if !directory.exists(&path)? {
            return Ok(Journal {
                schema: 1,
                generation: 0,
                receipt: None,
                refusal: None,
                delivered: false,
            });
        }
        let bytes = directory.read(&path, JOURNAL_MAGIC, JOURNAL_BYTES)?;
        let journal: Journal = decode(&bytes)?;
        if journal.schema != 1
            || journal.generation == 0
            || (journal.delivered && journal.receipt.is_none() && journal.refusal.is_none())
            || (journal.receipt.is_some() && journal.refusal.is_some())
            || encode(&journal, JOURNAL_BYTES)? != bytes
        {
            return Err(NativeStoreError::Corrupt);
        }
        Ok(journal)
    }
    fn prepared(
        &self,
        directory: &files::Directory,
        id: [u8; 16],
        ready: bool,
    ) -> Result<Prepared, NativeStoreError> {
        let component = component(id);
        let missing = if ready {
            NativeStoreError::Corrupt
        } else {
            NativeStoreError::Incomplete
        };
        if !directory.exists(&component)? {
            return Err(missing);
        }
        directory.check_child(&component)?;
        let prepared_path = format!("{component}/{PREPARED}");
        if !directory.exists(&prepared_path)? {
            return Err(missing);
        }
        let bytes = directory.read(&prepared_path, PREPARED_MAGIC, PREPARED_BYTES)?;
        let prepared: Prepared = decode(&bytes)?;
        if encode(&prepared, PREPARED_BYTES)? != bytes {
            return Err(NativeStoreError::Corrupt);
        }
        prepared.validate(id)?;
        Ok(prepared)
    }
    fn catalogue(&self, directory: &files::Directory) -> Result<Catalogue, NativeStoreError> {
        let bytes = directory.read(CATALOGUE, CATALOGUE_MAGIC, CATALOGUE_BYTES)?;
        let (schema, _): (u16, &[u8]) =
            postcard::take_from_bytes(&bytes).map_err(|_| NativeStoreError::Corrupt)?;
        let catalogue = if schema == 1 {
            let old: CatalogueV1 = decode(&bytes)?;
            if encode(&old, CATALOGUE_BYTES)? != bytes {
                return Err(NativeStoreError::Corrupt);
            }
            Catalogue::from(old)
        } else {
            let catalogue: Catalogue = decode(&bytes)?;
            if encode(&catalogue, CATALOGUE_BYTES)? != bytes {
                return Err(NativeStoreError::Corrupt);
            }
            catalogue
        };
        catalogue.validate(self.limits)?;
        Ok(catalogue)
    }
    fn save(
        &self,
        directory: &files::Directory,
        catalogue: &Catalogue,
    ) -> Result<(), NativeStoreError> {
        catalogue.validate(self.limits)?;
        Ok(directory.write(
            CATALOGUE,
            CATALOGUE_MAGIC,
            &encode(catalogue, CATALOGUE_BYTES)?,
            true,
        )?)
    }
    #[cfg(test)]
    fn fail_at(&self, fault: Fault) -> Result<(), NativeStoreError> {
        let mut injected = self.fault.lock().map_err(|_| NativeStoreError::Corrupt)?;
        if *injected == Some(fault) {
            *injected = None;
            Err(StoreError::Io(std::io::Error::other(
                "injected native-store initialization failure",
            ))
            .into())
        } else {
            Ok(())
        }
    }
    #[cfg(test)]
    pub(crate) fn inject(&self, fault: Fault) {
        if let Ok(mut injected) = self.fault.lock() {
            *injected = Some(fault);
        }
    }
}
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Fault {
    Claimed,
    Prepared,
    Ready,
    /// Between the retired identity's durability and its directory's removal.
    Retired,
}
fn assemble(id: [u8; 16], prepared: Prepared, journal: Journal) -> NativeOperation {
    NativeOperation {
        id: NativeOperationId(RequestId(id)),
        context: prepared.context,
        name: prepared.name,
        version: prepared.version,
        request: prepared.request,
        fingerprint: prepared.fingerprint,
        created: prepared.created,
        receipt: journal.receipt,
        refusal: journal.refusal,
        delivered: journal.delivered,
    }
}
/// The identity a child directory is named after, if it is one.
fn identity(name: &str) -> Option<[u8; 16]> {
    if name.len() != 32 {
        return None;
    }
    u128::from_str_radix(name, 16)
        .ok()
        .map(u128::to_be_bytes)
        .filter(|id| *id != [0; 16])
}
/// A receipt's digest: what a retired identity keeps of it.
fn digest_of(receipt: &NativeReceipt) -> Result<[u8; 32], NativeStoreError> {
    Ok(*blake3::hash(&encode(receipt, JOURNAL_BYTES)?).as_bytes())
}

#[cfg(test)]
#[path = "native_store_tests.rs"]
pub(crate) mod tests;
