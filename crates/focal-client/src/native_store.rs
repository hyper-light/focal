//! Durable identity for native operations (`n1:` references). The catalogue
//! claims an operation identity before any object identity is minted; the
//! prepared record then holds the exact `FCNINPUT1` frame the client sends,
//! and the journal records the committed native receipt bound to that frame.
//! A retry resends the journaled bytes; it never recompiles the document.
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

#[derive(Serialize, Deserialize)]
struct Catalogue {
    schema: u16,
    limits: NativeStoreLimits,
    entries: BTreeMap<[u8; 16], Entry>,
}
#[derive(Serialize, Deserialize)]
struct Entry {
    context: OperationContext,
    intent: [u8; 32],
    ready: bool,
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
        if self.schema != 1 || self.entries.keys().any(|id| *id == [0; 16]) {
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
    fault: std::cell::Cell<Option<Fault>>,
}
impl NativeOperationStore {
    pub fn create(
        root: impl AsRef<Path>,
        limits: NativeStoreLimits,
    ) -> Result<Self, NativeStoreError> {
        limits.validate()?;
        let catalogue = Catalogue {
            schema: 1,
            limits,
            entries: BTreeMap::new(),
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
        store.catalogue(&directory)?;
        Ok(store)
    }
    fn handle(root: &Path, limits: NativeStoreLimits) -> Self {
        Self {
            root: root.into(),
            limits,
            #[cfg(test)]
            fault: std::cell::Cell::new(None),
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
                    if candidate != [0; 16] && !catalogue.entries.contains_key(&candidate) {
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
            let usage = catalogue.usage()?;
            if usage.operations >= self.limits.max_operations
                || usage
                    .reserved_bytes
                    .checked_add(OPERATION_RESERVATION)
                    .is_none_or(|bytes| bytes > self.limits.max_reserved_bytes)
            {
                return Err(NativeStoreError::Capacity);
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
            let expanded = expand(RequestId(id))?;
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
        crate::operation_store::validate_context(*context)?;
        let directory = files::Directory::open_native(&self.root)?;
        let catalogue = self.catalogue(&directory)?;
        let entry = catalogue
            .entries
            .get(&id.0.0)
            .ok_or(NativeStoreError::MissingOperation)?;
        if entry.context != *context {
            return Err(NativeStoreError::ContextMismatch);
        }
        let prepared = self.prepared(&directory, id.0.0, entry.ready)?;
        if prepared.intent().digest()? != entry.intent {
            return Err(NativeStoreError::Corrupt);
        }
        if !entry.ready {
            return Err(NativeStoreError::Incomplete);
        }
        self.operation(&directory, id.0.0, prepared)
    }
    /// Record only a committed receipt bound to the exact journaled frame.
    /// Pending tickets and refusals never advance the journal.
    pub fn record_reply(
        &self,
        id: NativeOperationId,
        context: &OperationContext,
        reply: &NativeMutationReply,
    ) -> Result<NativeStage, NativeStoreError> {
        let NativeMutationReply::Committed(receipt) = reply else {
            return Err(NativeStoreError::NotCommitted);
        };
        let operation = self.retry(id, context)?;
        validate_receipt(context, id.0.0, operation.fingerprint, receipt)?;
        if let Some(existing) = &operation.receipt {
            return if existing == receipt {
                Ok(NativeStage::Completed)
            } else {
                Err(NativeStoreError::ReceiptMismatch)
            };
        }
        let directory = files::Directory::open_native(&self.root)?;
        let journal = Journal {
            schema: 1,
            generation: 1,
            receipt: Some(*receipt),
            refusal: None,
            delivered: false,
        };
        let path = format!("{}/{JOURNAL}", component(id.0.0));
        let replace = directory.exists(&path)?;
        directory.write(
            &path,
            JOURNAL_MAGIC,
            &encode(&journal, JOURNAL_BYTES)?,
            replace,
        )?;
        Ok(NativeStage::Completed)
    }
    /// Mark a committed result as reported to the caller. A receipt is never
    /// marked delivered before it is recorded.
    pub fn record_delivered(
        &self,
        id: NativeOperationId,
        context: &OperationContext,
    ) -> Result<(), NativeStoreError> {
        let operation = self.retry(id, context)?;
        let Some(receipt) = operation.receipt else {
            return Err(NativeStoreError::NotCommitted);
        };
        if operation.delivered {
            return Ok(());
        }
        let directory = files::Directory::open_native(&self.root)?;
        let journal = Journal {
            schema: 1,
            generation: 2,
            receipt: Some(receipt),
            refusal: None,
            delivered: true,
        };
        let path = format!("{}/{JOURNAL}", component(id.0.0));
        let replace = directory.exists(&path)?;
        directory.write(
            &path,
            JOURNAL_MAGIC,
            &encode(&journal, JOURNAL_BYTES)?,
            replace,
        )?;
        Ok(())
    }
    /// Record a closed refusal that was reported to the caller. The exact
    /// frame stays journaled; a committed receipt is never overwritten.
    pub fn record_refusal(
        &self,
        id: NativeOperationId,
        context: &OperationContext,
        refusal: &NativeRefusal,
    ) -> Result<(), NativeStoreError> {
        let operation = self.retry(id, context)?;
        if operation.receipt.is_some() {
            return Err(NativeStoreError::ReceiptMismatch);
        }
        if refusal.detail.len() > 4096 {
            return Err(NativeStoreError::Capacity);
        }
        let directory = files::Directory::open_native(&self.root)?;
        let journal = Journal {
            schema: 1,
            generation: 1,
            receipt: None,
            refusal: Some(refusal.clone()),
            delivered: true,
        };
        let path = format!("{}/{JOURNAL}", component(id.0.0));
        let replace = directory.exists(&path)?;
        directory.write(
            &path,
            JOURNAL_MAGIC,
            &encode(&journal, JOURNAL_BYTES)?,
            replace,
        )?;
        Ok(())
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
        Ok(NativeOperation {
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
        })
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
        let catalogue: Catalogue = decode(&bytes)?;
        catalogue.validate(self.limits)?;
        if encode(&catalogue, CATALOGUE_BYTES)? != bytes {
            return Err(NativeStoreError::Corrupt);
        }
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
        if self.fault.get() == Some(fault) {
            self.fault.set(None);
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
        self.fault.set(Some(fault));
    }
}
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Fault {
    Claimed,
    Prepared,
    Ready,
}

#[cfg(test)]
#[path = "native_store_tests.rs"]
pub(crate) mod tests;
