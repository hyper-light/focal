//! Durable, resumable content transfer. Filesystem methods run on the caller's
//! synchronous owner between network waits. Staging is not artifact attachment:
//! only the server's custody-gated seal returns an immutable content reference.
mod files;
mod retrieve;
mod store;
pub use retrieve::{PayloadDownload, retrieve_payload};
pub use store::{UploadStore, UploadStoreLimits};

use crate::pending::OperationContext;
use crate::{ClientError, Operation, RequestEnvelope, UploadReply, UploadRequest};
use focal_model::*;
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};

pub const TRANSFER_CHUNK_BYTES: usize = 64 * 1024;
pub const MAX_TRANSFER_BYTES: u64 = 64 * 1024 * 1024;
const MAX_STATE_BYTES: usize = crate::input::MAX_INPUT_BYTES + 2048;

#[derive(Debug, thiserror::Error)]
pub enum TransferError {
    #[error("artifact transfer I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Client(#[from] ClientError),
    #[error("artifact transfer already exists; open its saved identity to retry")]
    Exists,
    #[error("artifact transfer is owned by another client")]
    Locked,
    #[error("artifact transfer requires private directories and files")]
    Permissions,
    #[error("artifact transfer identity, context, or reply is invalid")]
    Invalid,
    #[error("artifact transfer is bound to different bytes or metadata")]
    Conflict,
    #[error("artifact transfer is missing, corrupt, or uses an unsupported format")]
    Corrupt,
    #[error("artifact transfer exceeds its bounded capability")]
    Capacity,
    #[error("artifact transfer payload is incomplete")]
    Incomplete,
    #[error("artifact transfer was cancelled")]
    Cancelled,
    #[error("artifact transfer had an ambiguous write; reopen before continuing")]
    Failed,
}

/// A fixed, persisted capability. Increasing it requires a different transfer;
/// opening an existing transfer never silently raises its declared bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransferLimits {
    pub max_payload_bytes: u64,
    pub chunk_bytes: u32,
}
impl Default for TransferLimits {
    fn default() -> Self {
        Self {
            max_payload_bytes: MAX_TRANSFER_BYTES,
            chunk_bytes: TRANSFER_CHUNK_BYTES as u32,
        }
    }
}
impl TransferLimits {
    fn validate(self) -> Result<(), TransferError> {
        if self.max_payload_bytes > MAX_TRANSFER_BYTES
            || self.chunk_bytes == 0
            || self.chunk_bytes as usize > TRANSFER_CHUNK_BYTES
        {
            return Err(TransferError::Capacity);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UploadProgress {
    pub upload: [u8; 16],
    pub length: u64,
    pub digest: ContentHash,
    pub class: ContentClass,
    pub staged: u64,
    pub received: u64,
    pub reference: Option<ContentRef>,
    pub cancelled: bool,
    /// A Cancel response was recorded. Current supporting servers persist a
    /// terminal ID fence; a legacy response alone is not a capability proof.
    pub cancel_acknowledged: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UploadSpec {
    pub upload: [u8; 16],
    pub class: ContentClass,
    pub length: u64,
    pub digest: ContentHash,
}

#[derive(Clone, Serialize, Deserialize)]
struct State {
    version: u16,
    context: OperationContext,
    route_epoch: RouteEpoch,
    limits: TransferLimits,
    progress: UploadProgress,
    begun: bool,
    verified: bool,
    cancel_requested: bool,
    prefix_hash: ContentHash,
    pending: Option<PendingRequest>,
    binding: Option<Vec<u8>>,
}
#[derive(Clone, Copy, Serialize, Deserialize)]
enum PendingRequest {
    Begin,
    Append { offset: u64, count: u32 },
    Seal,
    Cancel,
}
impl State {
    fn validate(&self, context: &OperationContext) -> Result<(), TransferError> {
        self.limits.validate()?;
        if self
            .binding
            .as_ref()
            .is_some_and(|bytes| bytes.len() > crate::input::MAX_INPUT_BYTES)
        {
            return Err(TransferError::Capacity);
        }
        let p = &self.progress;
        if self.version != 1
            || self.context != *context
            || context.cluster == [0; 16]
            || context.principal.is_zero()
            || context.ledger.tenant.is_zero()
            || context.ledger.session.is_zero()
            || self.route_epoch.0 == 0
            || p.upload == [0; 16]
            || p.length > self.limits.max_payload_bytes
            || p.staged > p.length
            || p.received > p.staged
            || (!self.begun && p.received != 0)
            || (self.verified != (p.staged == p.length))
            || (p.cancelled && (!self.cancel_requested || p.reference.is_some()))
            || (p.cancel_acknowledged && !self.cancel_requested && p.reference.is_none())
        {
            return Err(TransferError::Invalid);
        }
        if let Some(reference) = &p.reference {
            validate_reference(reference, context.ledger, p.class, p.length)?;
            if !self.verified || !self.begun || p.received != p.length {
                return Err(TransferError::Corrupt);
            }
        }
        if self.verified && self.prefix_hash != p.digest {
            return Err(TransferError::Corrupt);
        }
        let pending_valid = match self.pending {
            None => true,
            Some(PendingRequest::Begin) => {
                !self.begun && !self.cancel_requested && p.reference.is_none()
            }
            Some(PendingRequest::Append { offset, count }) => {
                self.begun
                    && !self.cancel_requested
                    && p.reference.is_none()
                    && offset == p.received
                    && count != 0
                    && count <= self.limits.chunk_bytes
                    && offset
                        .checked_add(u64::from(count))
                        .is_some_and(|end| end <= p.staged)
            }
            Some(PendingRequest::Seal) => {
                self.begun
                    && self.verified
                    && p.received == p.length
                    && p.reference.is_none()
                    && !self.cancel_requested
            }
            Some(PendingRequest::Cancel) => self.cancel_requested || p.reference.is_some(),
        };
        if !pending_valid || (p.cancel_acknowledged && self.pending.is_some()) {
            return Err(TransferError::Corrupt);
        }
        Ok(())
    }
}

/// One private lock and one bounded durable byte stream. Dropping a network
/// wait does not cancel this transfer or lose any bytes needed for exact retry.
pub struct UploadJournal {
    state: State,
    directory: files::Directory,
    payload: File,
    failed: bool,
    prefix: blake3::Hasher,
}
impl UploadJournal {
    /// Bind caller-known identity and complete stream digest before sending Begin.
    /// An interrupted creation is never silently recreated by `open`.
    pub fn begin(
        path: impl AsRef<Path>,
        context: OperationContext,
        spec: UploadSpec,
        route_epoch: RouteEpoch,
        limits: TransferLimits,
    ) -> Result<Self, TransferError> {
        let state = State {
            version: 1,
            context,
            route_epoch,
            limits,
            progress: UploadProgress {
                upload: spec.upload,
                length: spec.length,
                digest: spec.digest,
                class: spec.class,
                staged: 0,
                received: 0,
                reference: None,
                cancelled: false,
                cancel_acknowledged: false,
            },
            begun: false,
            verified: spec.length == 0,
            cancel_requested: false,
            prefix_hash: ContentHash(*blake3::hash(&[]).as_bytes()),
            pending: None,
            binding: None,
        };
        state.validate(&context)?;
        if spec.length == 0 && spec.digest != ContentHash(*blake3::hash(&[]).as_bytes()) {
            return Err(TransferError::Conflict);
        }
        let bytes = encode(&state)?;
        let (directory, payload) = files::Directory::create(path.as_ref())?;
        directory.install(&bytes, true)?;
        Ok(Self {
            state,
            directory,
            payload,
            failed: false,
            prefix: blake3::Hasher::new(),
        })
    }
    pub fn open(path: impl AsRef<Path>, context: &OperationContext) -> Result<Self, TransferError> {
        let (directory, mut payload) = files::Directory::open(path.as_ref())?;
        let bytes = directory.read()?;
        let (state, remaining): (State, &[u8]) =
            postcard::take_from_bytes(&bytes).map_err(|_| TransferError::Corrupt)?;
        if !remaining.is_empty() {
            return Err(TransferError::Corrupt);
        }
        state.validate(context)?;
        // A crash can leave bytes written after the last accepted local stage.
        // They were never transmitted: requests use only the saved prefix.
        let length = payload.metadata()?.len();
        if length < state.progress.staged || length > state.progress.length {
            return Err(TransferError::Corrupt);
        }
        if length != state.progress.staged {
            payload.set_len(state.progress.staged)?;
            payload.sync_all()?;
        }
        let prefix = hash_reader(&mut payload, state.progress.staged)?;
        if ContentHash(*prefix.finalize().as_bytes()) != state.prefix_hash {
            return Err(TransferError::Corrupt);
        }
        directory.recover_marker()?;
        Ok(Self {
            state,
            directory,
            payload,
            failed: false,
            prefix,
        })
    }
    /// Whether an explicit cancellation was persisted, including an unacknowledged request.
    pub fn cancel_requested(&self) -> bool {
        self.state.cancel_requested
    }
    pub fn progress(&self) -> &UploadProgress {
        &self.state.progress
    }
    pub fn context(&self) -> OperationContext {
        self.state.context
    }
    pub fn limits(&self) -> TransferLimits {
        self.state.limits
    }
    pub fn spec(&self) -> UploadSpec {
        let progress = &self.state.progress;
        UploadSpec {
            upload: progress.upload,
            class: progress.class,
            length: progress.length,
            digest: progress.digest,
        }
    }
    pub fn reference(&self) -> Option<&ContentRef> {
        self.state.progress.reference.as_ref()
    }
    pub fn path(&self) -> &Path {
        self.directory.path()
    }
    /// Bind an adapter's immutable continuation before the first transmission.
    /// This grants no authority; it only preserves the exact authored attachment
    /// and its recovery identity alongside the staged payload.
    pub fn bind_intent(&mut self, bytes: &[u8]) -> Result<(), TransferError> {
        self.check()?;
        if bytes.len() > crate::input::MAX_INPUT_BYTES {
            return Err(TransferError::Capacity);
        }
        if let Some(saved) = &self.state.binding {
            return if saved == bytes {
                Ok(())
            } else {
                Err(TransferError::Conflict)
            };
        }
        if self.state.begun || self.state.pending.is_some() {
            return Err(TransferError::Conflict);
        }
        let mut copied = Vec::new();
        copied
            .try_reserve_exact(bytes.len())
            .map_err(|_| TransferError::Capacity)?;
        copied.extend_from_slice(bytes);
        let mut state = self.state.clone();
        state.binding = Some(copied);
        self.install(state)
    }
    pub fn intent(&self) -> Option<&[u8]> {
        self.state.binding.as_deref()
    }

    /// Persist an immutable prefix. Earlier exact bytes may be retried; gaps or
    /// replacement bytes reject without changing the transfer or server state.
    pub fn stage(&mut self, offset: u64, bytes: &[u8]) -> Result<u64, TransferError> {
        self.check()?;
        if self.state.cancel_requested || self.state.progress.reference.is_some() {
            return Err(TransferError::Cancelled);
        }
        if bytes.is_empty() || bytes.len() > TRANSFER_CHUNK_BYTES {
            return Err(TransferError::Capacity);
        }
        let end = offset
            .checked_add(bytes.len() as u64)
            .ok_or(TransferError::Capacity)?;
        if end > self.state.progress.length {
            return Err(TransferError::Capacity);
        }
        if offset < self.state.progress.staged && end <= self.state.progress.staged {
            let mut saved = buffer(bytes.len())?;
            self.payload.seek(SeekFrom::Start(offset))?;
            self.payload.read_exact(&mut saved)?;
            return if saved == bytes {
                Ok(self.state.progress.staged)
            } else {
                Err(TransferError::Conflict)
            };
        }
        if offset != self.state.progress.staged {
            return Err(TransferError::Conflict);
        }
        let mut prefix = self.prefix.clone();
        prefix.update(bytes);
        let prefix_hash = ContentHash(*prefix.finalize().as_bytes());
        if end == self.state.progress.length && prefix_hash != self.state.progress.digest {
            return Err(TransferError::Conflict);
        }
        self.payload.seek(SeekFrom::Start(offset))?;
        if let Err(error) = self
            .payload
            .write_all(bytes)
            .and_then(|()| self.payload.sync_all())
        {
            self.failed = true;
            return Err(error.into());
        }
        let mut state = self.state.clone();
        state.progress.staged = end;
        state.prefix_hash = prefix_hash;
        if end == state.progress.length {
            state.verified = true;
        }
        self.install(state)?;
        self.prefix = prefix;
        Ok(end)
    }

    /// The exact next network request. None can mean that more source bytes are
    /// needed; inspect progress rather than interpreting it as a sealed upload.
    pub fn next_request(&mut self) -> Result<Option<RequestEnvelope>, TransferError> {
        self.check()?;
        let p = &self.state.progress;
        if p.cancel_acknowledged {
            return Ok(None);
        }
        if self.state.pending.is_none() {
            let pending = if self.state.cancel_requested || p.reference.is_some() {
                PendingRequest::Cancel
            } else if !self.state.begun {
                PendingRequest::Begin
            } else if p.received < p.staged {
                let remaining = p
                    .staged
                    .checked_sub(p.received)
                    .ok_or(TransferError::Corrupt)?;
                PendingRequest::Append {
                    offset: p.received,
                    count: u32::try_from(remaining.min(u64::from(self.state.limits.chunk_bytes)))
                        .map_err(|_| TransferError::Capacity)?,
                }
            } else if self.state.verified {
                PendingRequest::Seal
            } else {
                return Ok(None);
            };
            let mut state = self.state.clone();
            state.pending = Some(pending);
            self.install(state)?;
        }
        let p = &self.state.progress;
        let operation = match self.state.pending.ok_or(TransferError::Corrupt)? {
            PendingRequest::Cancel => UploadRequest::Cancel { upload: p.upload },
            PendingRequest::Begin => UploadRequest::Begin {
                upload: p.upload,
                length: p.length,
                digest: p.digest,
                class: p.class,
            },
            PendingRequest::Append { offset, count } => {
                let mut bytes = buffer(count as usize)?;
                self.payload.seek(SeekFrom::Start(offset))?;
                self.payload.read_exact(&mut bytes)?;
                UploadRequest::Append {
                    upload: p.upload,
                    offset,
                    bytes,
                }
            }
            PendingRequest::Seal => UploadRequest::Seal { upload: p.upload },
        };
        self.envelope(operation).map(Some)
    }

    /// Persist only an exact request's bound reply. Sealed references are checked
    /// against the declared domain/class/length and originate only from ingress.
    pub fn record_reply(
        &mut self,
        request: &RequestEnvelope,
        reply: &UploadReply,
    ) -> Result<(), TransferError> {
        self.check()?;
        if self.next_request()?.as_ref() != Some(request) {
            return Err(TransferError::Invalid);
        }
        let mut state = self.state.clone();
        state.pending = None;
        match (&request.operation, reply) {
            (Operation::Upload(UploadRequest::Begin { .. }), UploadReply::Offset(offset))
                if *offset <= state.progress.staged =>
            {
                state.begun = true;
                state.progress.received = *offset;
            }
            (
                Operation::Upload(UploadRequest::Append { offset, bytes, .. }),
                UploadReply::Offset(received),
            ) if offset
                .checked_add(bytes.len() as u64)
                .is_some_and(|end| *received >= end)
                && *received <= state.progress.staged =>
            {
                state.progress.received = *received
            }
            (Operation::Upload(UploadRequest::Seal { .. }), UploadReply::Sealed(reference)) => {
                validate_reference(
                    reference,
                    state.context.ledger,
                    state.progress.class,
                    state.progress.length,
                )?;
                state.progress.reference = Some(reference.clone());
            }
            (Operation::Upload(UploadRequest::Cancel { .. }), UploadReply::Cancelled) => {
                state.progress.cancel_acknowledged = true;
                state.progress.cancelled = state.progress.reference.is_none();
            }
            _ => return Err(TransferError::Invalid),
        }
        self.install(state)
    }
    /// Stop issuing transfer work and request removal of current server staging.
    /// Repeating this call reissues exact cleanup even after a prior acknowledgment.
    /// The server retains a terminal ID fence; immutable content and independent
    /// artifact facts remain unchanged.
    pub fn cancel(&mut self) -> Result<(), TransferError> {
        self.check()?;
        if self.state.cancel_requested && !self.state.progress.cancel_acknowledged {
            return Ok(());
        }
        let mut state = self.state.clone();
        state.cancel_requested = true;
        state.progress.cancel_acknowledged = false;
        state.pending = None;
        self.install(state)
    }
    fn envelope(&self, operation: UploadRequest) -> Result<RequestEnvelope, TransferError> {
        let tag = match &operation {
            UploadRequest::Begin { .. } => 1,
            UploadRequest::Append { .. } => 2,
            UploadRequest::Seal { .. } => 3,
            UploadRequest::Cancel { .. } => 4,
        };
        let offset = match &operation {
            UploadRequest::Append { offset, .. } => *offset,
            _ => 0,
        };
        let request_id = request_id(self.state.context, self.state.progress.upload, tag, offset)?;
        Ok(RequestEnvelope {
            protocol: focal_wire::PROTOCOL_VERSION,
            request_id,
            request_epoch: RequestEpoch(1),
            ledger: self.state.context.ledger,
            route_epoch: self.state.route_epoch,
            operation: Operation::Upload(operation),
        })
    }
    fn install(&mut self, state: State) -> Result<(), TransferError> {
        state.validate(&self.state.context)?;
        let bytes = encode(&state)?;
        if let Err(error) = self.directory.install(&bytes, false) {
            self.failed = true;
            return Err(error);
        }
        self.state = state;
        Ok(())
    }
    fn check(&self) -> Result<(), TransferError> {
        if self.failed {
            Err(TransferError::Failed)
        } else {
            Ok(())
        }
    }
}

fn encode(state: &State) -> Result<Vec<u8>, TransferError> {
    let size =
        postcard::experimental::serialized_size(state).map_err(|_| TransferError::Corrupt)?;
    if size > MAX_STATE_BYTES {
        return Err(TransferError::Capacity);
    }
    let mut bytes = buffer(size)?;
    let written = postcard::to_slice(state, &mut bytes)
        .map_err(|_| TransferError::Corrupt)?
        .len();
    bytes.truncate(written);
    Ok(bytes)
}
fn buffer(size: usize) -> Result<Vec<u8>, TransferError> {
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(size)
        .map_err(|_| TransferError::Capacity)?;
    bytes.resize(size, 0);
    Ok(bytes)
}
/// Hash a seekable source without loading it into memory. The position is reset
/// to zero; exactly the declared length must be present, including an EOF check.
pub fn digest_reader(
    reader: &mut (impl Read + Seek),
    length: u64,
) -> Result<ContentHash, TransferError> {
    Ok(ContentHash(
        *hash_reader(reader, length)?.finalize().as_bytes(),
    ))
}
fn hash_reader(
    reader: &mut (impl Read + Seek),
    length: u64,
) -> Result<blake3::Hasher, TransferError> {
    if length > MAX_TRANSFER_BYTES {
        return Err(TransferError::Capacity);
    }
    reader.rewind()?;
    let mut scratch = buffer(TRANSFER_CHUNK_BYTES)?;
    let mut remaining = length;
    let mut hash = blake3::Hasher::new();
    while remaining != 0 {
        let count = usize::try_from(remaining.min(TRANSFER_CHUNK_BYTES as u64))
            .map_err(|_| TransferError::Capacity)?;
        let chunk = scratch.get_mut(..count).ok_or(TransferError::Capacity)?;
        reader.read_exact(chunk)?;
        hash.update(chunk);
        remaining = remaining
            .checked_sub(count as u64)
            .ok_or(TransferError::Corrupt)?;
    }
    let mut extra = [0; 1];
    if reader.read(&mut extra)? != 0 {
        return Err(TransferError::Conflict);
    }
    reader.rewind()?;
    Ok(hash)
}
fn validate_reference(
    reference: &ContentRef,
    ledger: LedgerId,
    class: ContentClass,
    length: u64,
) -> Result<(), TransferError> {
    if reference.domain != ContentDomainId(ledger.tenant.0)
        || reference.root.0 == [0; 32]
        || reference.class != class
        || reference.length != length
    {
        return Err(TransferError::Invalid);
    }
    Ok(())
}
fn request_id(
    context: OperationContext,
    upload: [u8; 16],
    tag: u8,
    offset: u64,
) -> Result<RequestId, TransferError> {
    let mut hash = blake3::Hasher::new();
    hash.update(b"focal.artifact-transfer.request.v1\0");
    hash.update(&context.cluster);
    hash.update(&context.ledger.tenant.0);
    hash.update(&context.ledger.session.0);
    hash.update(&context.principal.0);
    hash.update(&upload);
    hash.update(&[tag]);
    hash.update(&offset.to_be_bytes());
    let mut id = [0; 16];
    id.copy_from_slice(
        hash.finalize()
            .as_bytes()
            .get(..16)
            .ok_or(TransferError::Invalid)?,
    );
    if id == [0; 16] {
        return Err(TransferError::Invalid);
    }
    Ok(RequestId(id))
}

#[cfg(test)]
mod tests;
