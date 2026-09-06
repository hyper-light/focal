//! Private, bounded managed-stream allocator. All methods perform synchronous
//! filesystem work on the caller's existing blocking owner. Every method releases
//! the short store lock before returning a request to transmit.
mod id;
pub use id::ManagedOperationId;
mod controls;
use crate::{
    RequestEnvelope,
    operation_store::{OperationIntent, StoreError, files::Directory},
    pending::OperationContext,
};
use controls::validate_registration;
use focal_model::*;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

const STATE: &str = "stream.bin";
const STATE_MAGIC: &[u8; 8] = b"FCLMCA01";
const PREPARED_MAGIC: &[u8; 8] = b"FCLMPR01";
const RECEIPT_MAGIC: &[u8; 8] = b"FCLMRE01";
const STATE_BYTES: usize = 256 * 1024;
const REQUEST_BYTES: usize = crate::pending::MAX_OPERATION_REQUEST_BYTES;
const PREPARED_BYTES: usize = REQUEST_BYTES + crate::operation_store::MAX_INTENT_BYTES + 1024;
const RECEIPT_BYTES: usize = crate::pending::MAX_OPERATION_RECEIPT_BYTES;
const ROOT_BYTES: u64 = 2 * STATE_BYTES as u64 + 4096;
const SLOT_BYTES: u64 = 2 * (PREPARED_BYTES as u64 + RECEIPT_BYTES as u64 + 44) + 16384;

#[derive(Debug, thiserror::Error)]
pub enum ManagedStoreError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("managed ID requires canonical m1:slot:generation:ordinal:request hexadecimal fields")]
    InvalidId,
    #[error("managed operation is retired and cannot execute again")]
    Retired,
    #[error("managed operation was never reserved by this store")]
    Missing,
    #[error("managed operation has a different persisted intent or identity")]
    Conflict,
    #[error("managed store has missing, corrupt, or unsupported state")]
    Corrupt,
    #[error("managed stream or operation belongs to another context")]
    Context,
    #[error("managed store capacity exceeded")]
    Capacity,
    #[error("managed stream registration has not committed")]
    NotRegistered,
    #[error("managed stream has stopped issuing requests")]
    Stopped,
    #[error("a saved stream control must be resolved before issuing another control")]
    ControlPending,
    #[error("saved request has not been prepared")]
    Unprepared,
    #[error("receipt does not prove the exact saved operation or control committed")]
    ReceiptMismatch,
    #[error("contiguous saved outcomes are required before acknowledgment or close")]
    Unresolved,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedStoreLimits {
    pub window: u32,
    pub max_reserved_bytes: u64,
}
impl Default for ManagedStoreLimits {
    fn default() -> Self {
        Self {
            window: 32,
            max_reserved_bytes: 256 * 1024 * 1024,
        }
    }
}
impl ManagedStoreLimits {
    pub(crate) fn reserved_bytes(self) -> Result<u64, ManagedStoreError> {
        u64::from(self.window)
            .checked_mul(SLOT_BYTES)
            .and_then(|bytes| bytes.checked_add(ROOT_BYTES))
            .ok_or(ManagedStoreError::Capacity)
    }
    pub(crate) fn validate(self) -> Result<(), ManagedStoreError> {
        let bytes = self.reserved_bytes()?;
        if self.window == 0 || self.window > 256 || bytes > self.max_reserved_bytes {
            return Err(ManagedStoreError::Capacity);
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Entry {
    key: ManagedRequestKey,
    intent: Option<[u8; 32]>,
    prepared: bool,
    receipt: Option<ContentHash>,
    seal: Option<SealBinding>,
}
#[derive(Clone, Copy, Serialize, Deserialize)]
struct SealBinding {
    family: ManagedRequestFamily,
    intent_hash: ContentHash,
}
#[derive(Serialize, Deserialize)]
struct State {
    schema: u16,
    context: OperationContext,
    limits: ManagedStoreLimits,
    registration: RequestStreamControlInput,
    registered: Option<RequestStreamControlReceipt>,
    revision: u64,
    frontier: u64,
    retired: u64,
    garbage_through: u64,
    stopped: bool,
    closed: bool,
    entries: Vec<Entry>,
    control: Option<RequestStreamControlInput>,
    last_control: Option<RequestStreamControlReceipt>,
}
impl State {
    fn stream(&self) -> Result<RequestStreamIdentity, ManagedStoreError> {
        let RequestStreamCommand::Register {
            slot,
            expected_generation,
            ..
        } = self.registration.command
        else {
            return Err(ManagedStoreError::Corrupt);
        };
        Ok(RequestStreamIdentity {
            cluster: self.context.cluster,
            ledger: self.context.ledger,
            principal: self.context.principal,
            slot,
            generation: expected_generation
                .checked_add(1)
                .ok_or(ManagedStoreError::Capacity)?,
        })
    }
    fn index(&self, id: ManagedOperationId) -> Result<usize, ManagedStoreError> {
        let key = id.key(self.context);
        let stream = self.stream()?;
        if self.registered.is_some()
            && key.stream.slot == stream.slot
            && key.stream.generation < stream.generation
        {
            return Err(ManagedStoreError::Retired);
        }
        if key.stream != stream {
            return Err(ManagedStoreError::Missing);
        }
        if self.closed || key.ordinal <= self.retired {
            return Err(ManagedStoreError::Retired);
        }
        let index = key
            .ordinal
            .checked_sub(self.retired)
            .and_then(|value| value.checked_sub(1))
            .and_then(|value| usize::try_from(value).ok())
            .ok_or(ManagedStoreError::Missing)?;
        let entry = self.entries.get(index).ok_or(ManagedStoreError::Missing)?;
        if entry.key != key {
            return Err(ManagedStoreError::Conflict);
        }
        Ok(index)
    }
    fn validate(
        &self,
        context: OperationContext,
        limits: ManagedStoreLimits,
    ) -> Result<(), ManagedStoreError> {
        limits.validate()?;
        if self.context != context {
            return Err(ManagedStoreError::Context);
        }
        if self.limits != limits {
            return Err(StoreError::LimitsMismatch.into());
        }
        let stream = self.stream()?;
        if self.schema != 1
            || !stream.is_valid()
            || self.registration.id.is_zero()
            || self.registration.cluster != context.cluster
            || self.registration.ledger != context.ledger
            || self.registration.principal != context.principal
            || self.retired > self.frontier
            || self.garbage_through > self.retired
            || self.frontier.checked_sub(self.retired) != u64::try_from(self.entries.len()).ok()
            || self.entries.len() > limits.window as usize
            || self
                .retired
                .checked_sub(self.garbage_through)
                .is_none_or(|n| n > u64::from(limits.window))
            || (self.closed && (!self.stopped || self.retired != self.frontier))
        {
            return Err(ManagedStoreError::Corrupt);
        }
        let RequestStreamCommand::Register { owner, window, .. } = self.registration.command else {
            return Err(ManagedStoreError::Corrupt);
        };
        if owner.is_zero() || window != limits.window {
            return Err(ManagedStoreError::Corrupt);
        }
        for (index, entry) in self.entries.iter().enumerate() {
            let ordinal = u64::try_from(index)
                .ok()
                .and_then(|n| n.checked_add(1))
                .and_then(|n| n.checked_add(self.retired))
                .ok_or(ManagedStoreError::Corrupt)?;
            if !entry.key.is_valid()
                || entry.key.stream != stream
                || entry.key.ordinal != ordinal
                || (entry.prepared && entry.intent.is_none())
                || (entry.receipt.is_some() && !entry.prepared && entry.seal.is_none())
            {
                return Err(ManagedStoreError::Corrupt);
            }
        }
        if let Some(receipt) = &self.registered {
            validate_registration(self, receipt)?;
            if self.revision == 0 {
                return Err(ManagedStoreError::Corrupt);
            }
        } else if self.frontier != 0 || self.revision != 0 || self.control.is_some() || self.stopped
        {
            return Err(ManagedStoreError::Corrupt);
        }
        Ok(())
    }
}

/// Expanded IDs and exact envelope are immutable after this record is synced.
/// No Debug implementation exposes authored artifact or claim contents.
#[derive(Serialize, Deserialize)]
pub struct PreparedManagedOperation {
    schema: u16,
    name: String,
    version: u16,
    canonical: Vec<u8>,
    pub request: RequestEnvelope,
}
impl PreparedManagedOperation {
    fn intent(&self) -> OperationIntent<'_> {
        OperationIntent {
            name: &self.name,
            version: self.version,
            canonical: &self.canonical,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManagedStoreStatus {
    pub stream: RequestStreamIdentity,
    pub registered: bool,
    pub issued_through: u64,
    pub retired_through: u64,
    pub stopped: bool,
    pub closed: bool,
}

/// One independently registered stream per store. New stores cannot adopt a
/// missing ID; only reserve() creates identities. A closed store remains a
/// durable retired namespace. Hosts keep an external initialization marker when
/// automatically creating stores so deletion cannot silently reset ownership.
pub struct ManagedOperationStore {
    root: PathBuf,
    context: OperationContext,
    limits: ManagedStoreLimits,
}
impl ManagedOperationStore {
    /// Only the coordinator's durable Initialize phase can invoke this: it has
    /// not exposed this child for allocation or transmission yet.
    pub(crate) fn finish_initialization(
        root: &Path,
        context: OperationContext,
        limits: ManagedStoreLimits,
        registration: &RequestStreamControlInput,
        receipt: &RequestStreamControlReceipt,
    ) -> Result<Self, ManagedStoreError> {
        let directory = Directory::resume_managed_creation(root)?;
        let mut state = if directory.exists(STATE)? {
            decode::<State>(&directory.read(STATE, STATE_MAGIC, STATE_BYTES)?)?
        } else {
            State {
                schema: 1,
                context,
                limits,
                registration: registration.clone(),
                registered: None,
                revision: 0,
                frontier: 0,
                retired: 0,
                garbage_through: 0,
                stopped: false,
                closed: false,
                entries: Vec::new(),
                control: None,
                last_control: None,
            }
        };
        state.validate(context, limits)?;
        if state.registration != *registration
            || state.frontier != 0
            || state.control.is_some()
            || state.stopped
            || state.last_control.is_some()
        {
            return Err(ManagedStoreError::Corrupt);
        }
        validate_registration(&state, receipt)?;
        if state
            .registered
            .as_ref()
            .is_some_and(|saved| saved != receipt)
        {
            return Err(ManagedStoreError::ReceiptMismatch);
        }
        state.registered = Some(receipt.clone());
        state.revision = 1;
        directory.write(
            STATE,
            STATE_MAGIC,
            &encode(&state, STATE_BYTES)?,
            directory.exists(STATE)?,
        )?;
        Ok(Self {
            root: root.into(),
            context,
            limits,
        })
    }
    pub fn create(
        root: impl AsRef<Path>,
        context: OperationContext,
        limits: ManagedStoreLimits,
        registration: RequestStreamControlInput,
    ) -> Result<Self, ManagedStoreError> {
        let state = State {
            schema: 1,
            context,
            limits,
            registration,
            registered: None,
            revision: 0,
            frontier: 0,
            retired: 0,
            garbage_through: 0,
            stopped: false,
            closed: false,
            entries: Vec::new(),
            control: None,
            last_control: None,
        };
        state.validate(context, limits)?;
        let bytes = encode(&state, STATE_BYTES)?;
        let directory = Directory::create_managed(root.as_ref())?;
        directory.initialize()?;
        directory.write(STATE, STATE_MAGIC, &bytes, false)?;
        Ok(Self {
            root: root.as_ref().into(),
            context,
            limits,
        })
    }
    pub fn open(
        root: impl AsRef<Path>,
        context: OperationContext,
        limits: ManagedStoreLimits,
    ) -> Result<Self, ManagedStoreError> {
        let store = Self {
            root: root.as_ref().into(),
            context,
            limits,
        };
        let (_, _) = store.load()?;
        Ok(store)
    }
    pub fn root(&self) -> &Path {
        &self.root
    }
    pub fn status(&self) -> Result<ManagedStoreStatus, ManagedStoreError> {
        let (_, state) = self.load()?;
        Ok(ManagedStoreStatus {
            stream: state.stream()?,
            registered: state.registered.is_some(),
            issued_through: state.frontier,
            retired_through: state.retired,
            stopped: state.stopped,
            closed: state.closed,
        })
    }
    pub fn registration(&self) -> Result<RequestStreamControlInput, ManagedStoreError> {
        let (_, state) = self.load()?;
        Ok(state.registration)
    }
    pub fn record_registration(
        &self,
        receipt: RequestStreamControlReceipt,
    ) -> Result<(), ManagedStoreError> {
        let (directory, mut state) = self.load()?;
        validate_registration(&state, &receipt)?;
        if let Some(saved) = &state.registered {
            return if saved == &receipt {
                Ok(())
            } else {
                Err(ManagedStoreError::ReceiptMismatch)
            };
        }
        state.registered = Some(receipt);
        state.revision = 1;
        self.save(&directory, &state)
    }
    /// The caller supplies fresh entropy on this blocking owner. The returned ID
    /// exists durably before any expanded command can be transmitted.
    pub fn reserve(&self, request: RequestId) -> Result<ManagedOperationId, ManagedStoreError> {
        let (directory, mut state) = self.load()?;
        if state.registered.is_none() {
            return Err(ManagedStoreError::NotRegistered);
        }
        if state.stopped {
            return Err(ManagedStoreError::Stopped);
        }
        if request.is_zero() {
            return Err(ManagedStoreError::InvalidId);
        }
        if state.entries.len() >= self.limits.window as usize {
            return Err(ManagedStoreError::Capacity);
        }
        let ordinal = state
            .frontier
            .checked_add(1)
            .ok_or(ManagedStoreError::Capacity)?;
        let key = ManagedRequestKey {
            stream: state.stream()?,
            ordinal,
            id: request,
        };
        state
            .entries
            .try_reserve(1)
            .map_err(|_| ManagedStoreError::Capacity)?;
        state.entries.push(Entry {
            key,
            intent: None,
            prepared: false,
            receipt: None,
            seal: None,
        });
        state.frontier = ordinal;
        self.save(&directory, &state)?;
        ManagedOperationId::from_key(key)
    }
    pub fn prepare(
        &self,
        id: ManagedOperationId,
        intent: OperationIntent<'_>,
        expand: impl FnOnce(ManagedRequestKey) -> Result<RequestEnvelope, ManagedStoreError>,
    ) -> Result<PreparedManagedOperation, ManagedStoreError> {
        let digest = intent.digest()?;
        let (directory, mut state) = self.load()?;
        let index = state.index(id)?;
        let entry = state
            .entries
            .get_mut(index)
            .ok_or(ManagedStoreError::Corrupt)?;
        if entry.seal.is_some() && !entry.prepared {
            return Err(ManagedStoreError::Stopped);
        }
        if entry.intent.is_some_and(|saved| saved != digest) {
            return Err(ManagedStoreError::Conflict);
        }
        let key = entry.key;
        let ready = entry.prepared;
        if entry.intent.is_none() {
            entry.intent = Some(digest);
            self.save(&directory, &state)?;
        }
        let name = prepared_name(key.ordinal);
        let prepared = if directory.exists(&name)? {
            self.prepared(&directory, key, digest)?
        } else if ready {
            return Err(ManagedStoreError::Corrupt);
        } else {
            let value = PreparedManagedOperation {
                schema: 1,
                name: intent.name.into(),
                version: intent.version,
                canonical: intent.canonical.into(),
                request: expand(key)?,
            };
            validate_prepared(key, digest, &value)?;
            directory.write(
                &name,
                PREPARED_MAGIC,
                &encode(&value, PREPARED_BYTES)?,
                false,
            )?;
            value
        };
        if prepared.intent().canonical != intent.canonical
            || prepared.intent().name != intent.name
            || prepared.intent().version != intent.version
        {
            return Err(ManagedStoreError::Conflict);
        }
        if !ready {
            state
                .entries
                .get_mut(index)
                .ok_or(ManagedStoreError::Corrupt)?
                .prepared = true;
            self.save(&directory, &state)?;
        }
        Ok(prepared)
    }
    pub fn request(
        &self,
        id: ManagedOperationId,
    ) -> Result<PreparedManagedOperation, ManagedStoreError> {
        let (directory, state) = self.load()?;
        let entry = state
            .entries
            .get(state.index(id)?)
            .ok_or(ManagedStoreError::Corrupt)?;
        if !entry.prepared {
            return Err(ManagedStoreError::Unprepared);
        }
        self.prepared(
            &directory,
            entry.key,
            entry.intent.ok_or(ManagedStoreError::Corrupt)?,
        )
    }
    pub fn record_receipt(
        &self,
        id: ManagedOperationId,
        receipt: &ManagedReceipt,
    ) -> Result<(), ManagedStoreError> {
        let (directory, mut state) = self.load()?;
        let index = state.index(id)?;
        let entry = state.entries.get(index).ok_or(ManagedStoreError::Corrupt)?;
        let prepared = self.prepared(
            &directory,
            entry.key,
            entry.intent.ok_or(ManagedStoreError::Unprepared)?,
        )?;
        validate_receipt(&prepared.request, receipt)?;
        let digest = receipt
            .content_hash()
            .map_err(|_| ManagedStoreError::Corrupt)?;
        if entry.receipt.is_some_and(|saved| saved != digest) {
            return Err(ManagedStoreError::ReceiptMismatch);
        }
        let name = receipt_name(entry.key.ordinal);
        if directory.exists(&name)? {
            let saved: ManagedReceipt =
                decode(&directory.read(&name, RECEIPT_MAGIC, RECEIPT_BYTES)?)?;
            if &saved != receipt {
                return Err(ManagedStoreError::ReceiptMismatch);
            }
        } else if entry.receipt.is_some() {
            return Err(ManagedStoreError::Corrupt);
        } else {
            directory.write(
                &name,
                RECEIPT_MAGIC,
                &encode(receipt, RECEIPT_BYTES)?,
                false,
            )?;
        }
        let entry = state
            .entries
            .get_mut(index)
            .ok_or(ManagedStoreError::Corrupt)?;
        entry.prepared = true;
        entry.receipt = Some(digest);
        self.save(&directory, &state)
    }
    pub fn receipt(
        &self,
        id: ManagedOperationId,
    ) -> Result<Option<ManagedReceipt>, ManagedStoreError> {
        let (directory, state) = self.load()?;
        let entry = state
            .entries
            .get(state.index(id)?)
            .ok_or(ManagedStoreError::Corrupt)?;
        self.saved_receipt(&directory, entry)
    }
    /// A crashed reserve reply is recoverable without generating another ID.
    pub fn outstanding(&self) -> Result<Vec<ManagedOperationId>, ManagedStoreError> {
        let (_, state) = self.load()?;
        let mut result = Vec::new();
        result
            .try_reserve_exact(state.entries.len())
            .map_err(|_| ManagedStoreError::Capacity)?;
        for entry in state.entries {
            result.push(ManagedOperationId::from_key(entry.key)?);
        }
        Ok(result)
    }
    fn load(&self) -> Result<(Directory, State), ManagedStoreError> {
        let directory = Directory::open_managed(&self.root)?;
        let bytes = directory.read(STATE, STATE_MAGIC, STATE_BYTES)?;
        let mut state: State = decode(&bytes)?;
        state.validate(self.context, self.limits)?;
        if encode(&state, STATE_BYTES)? != bytes {
            return Err(ManagedStoreError::Corrupt);
        }
        for entry in &state.entries {
            if (entry.prepared && !directory.exists(&prepared_name(entry.key.ordinal))?)
                || (entry.receipt.is_some()
                    && !directory.exists(&receipt_name(entry.key.ordinal))?)
            {
                return Err(ManagedStoreError::Corrupt);
            }
        }
        // A committed local retired prefix is the sole authority for deletion.
        // Crash recovery repeats bounded cleanup before another ordinal issues.
        if state.garbage_through < state.retired {
            while state.garbage_through < state.retired {
                let ordinal = state
                    .garbage_through
                    .checked_add(1)
                    .ok_or(ManagedStoreError::Corrupt)?;
                directory.remove_record(&prepared_name(ordinal))?;
                directory.remove_record(&receipt_name(ordinal))?;
                state.garbage_through = ordinal;
            }
            self.save(&directory, &state)?;
        }
        Ok((directory, state))
    }
    fn save(&self, directory: &Directory, state: &State) -> Result<(), ManagedStoreError> {
        state.validate(self.context, self.limits)?;
        directory.write(STATE, STATE_MAGIC, &encode(state, STATE_BYTES)?, true)?;
        Ok(())
    }
    fn prepared(
        &self,
        directory: &Directory,
        key: ManagedRequestKey,
        intent: [u8; 32],
    ) -> Result<PreparedManagedOperation, ManagedStoreError> {
        let bytes = directory.read(&prepared_name(key.ordinal), PREPARED_MAGIC, PREPARED_BYTES)?;
        let value = decode(&bytes)?;
        validate_prepared(key, intent, &value)?;
        if encode(&value, PREPARED_BYTES)? != bytes {
            return Err(ManagedStoreError::Corrupt);
        }
        Ok(value)
    }
    fn saved_receipt(
        &self,
        directory: &Directory,
        entry: &Entry,
    ) -> Result<Option<ManagedReceipt>, ManagedStoreError> {
        let name = receipt_name(entry.key.ordinal);
        if !directory.exists(&name)? {
            return if entry.receipt.is_none() {
                Ok(None)
            } else {
                Err(ManagedStoreError::Corrupt)
            };
        }
        let bytes = directory.read(&name, RECEIPT_MAGIC, RECEIPT_BYTES)?;
        let receipt: ManagedReceipt = decode(&bytes)?;
        if entry.prepared {
            let prepared = self.prepared(
                directory,
                entry.key,
                entry.intent.ok_or(ManagedStoreError::Corrupt)?,
            )?;
            validate_receipt(&prepared.request, &receipt)?;
        } else {
            let seal = entry.seal.ok_or(ManagedStoreError::Corrupt)?;
            validate_bound_receipt(entry.key, seal.family, seal.intent_hash, &receipt)?;
        }
        let hash = receipt
            .content_hash()
            .map_err(|_| ManagedStoreError::Corrupt)?;
        if entry.receipt.is_some_and(|saved| saved != hash)
            || encode(&receipt, RECEIPT_BYTES)? != bytes
        {
            return Err(ManagedStoreError::Corrupt);
        }
        Ok(Some(receipt))
    }
}

fn validate_prepared(
    key: ManagedRequestKey,
    digest: [u8; 32],
    value: &PreparedManagedOperation,
) -> Result<(), ManagedStoreError> {
    if value.schema != 1 || value.intent().digest()? != digest {
        return Err(ManagedStoreError::Corrupt);
    }
    let (actual, _, _) = focal_wire::managed_request_identity(&value.request)
        .map_err(|_| ManagedStoreError::Conflict)?;
    if actual != key {
        return Err(ManagedStoreError::Context);
    }
    encode(&value.request, REQUEST_BYTES)?;
    Ok(())
}
fn validate_receipt(
    request: &RequestEnvelope,
    receipt: &ManagedReceipt,
) -> Result<(), ManagedStoreError> {
    let (key, family, hash) =
        focal_wire::managed_request_identity(request).map_err(|_| ManagedStoreError::Corrupt)?;
    validate_bound_receipt(key, family, hash, receipt)
}
fn validate_bound_receipt(
    key: ManagedRequestKey,
    family: ManagedRequestFamily,
    hash: ContentHash,
    receipt: &ManagedReceipt,
) -> Result<(), ManagedStoreError> {
    focal_wire::validate_managed_receipt(
        receipt,
        &key,
        family,
        hash,
        &focal_wire::WireLimits::default(),
    )
    .map_err(|_| ManagedStoreError::ReceiptMismatch)?;
    encode(receipt, RECEIPT_BYTES)?;
    Ok(())
}
fn prepared_name(ordinal: u64) -> String {
    format!("{ordinal:016x}.prepared")
}
fn receipt_name(ordinal: u64) -> String {
    format!("{ordinal:016x}.receipt")
}
pub(crate) fn record_limit(name: &str) -> Option<usize> {
    if name == STATE {
        return Some(STATE_BYTES);
    }
    let (ordinal, kind) = name.split_once('.')?;
    if ordinal.len() != 16
        || !ordinal
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || u64::from_str_radix(ordinal, 16).ok()? == 0
    {
        return None;
    }
    match kind {
        "prepared" => Some(PREPARED_BYTES),
        "receipt" => Some(RECEIPT_BYTES),
        _ => None,
    }
}
fn encode(value: &impl Serialize, maximum: usize) -> Result<Vec<u8>, ManagedStoreError> {
    let length =
        postcard::experimental::serialized_size(value).map_err(|_| ManagedStoreError::Corrupt)?;
    if length > maximum {
        return Err(ManagedStoreError::Capacity);
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length)
        .map_err(|_| ManagedStoreError::Capacity)?;
    bytes.resize(length, 0);
    postcard::to_slice(value, &mut bytes).map_err(|_| ManagedStoreError::Corrupt)?;
    Ok(bytes)
}
fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, ManagedStoreError> {
    let (value, rest) = postcard::take_from_bytes(bytes).map_err(|_| ManagedStoreError::Corrupt)?;
    if !rest.is_empty() {
        return Err(ManagedStoreError::Corrupt);
    }
    Ok(value)
}

#[cfg(test)]
mod tests;
