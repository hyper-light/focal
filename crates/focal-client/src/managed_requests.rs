//! Synchronous managed ownership and delivered-result acknowledgment. No store
//! lock crosses a network await; one persisted maintenance action survives loss.
mod maintenance;
mod state;
use crate::{
    AccessError, Operation, RequestEnvelope, Response, ResponseEnvelope,
    input::IdGenerator,
    managed_store::{
        ManagedOperationId, ManagedOperationStore, ManagedStoreError, ManagedStoreLimits,
    },
    operation_store::{StoreError, files::Directory},
    pending::OperationContext,
};
use focal_model::*;
use serde::{Deserialize, Serialize};
use state::*;
use std::path::{Path, PathBuf};

const BYTES: usize = 256 * 1024;
const RESERVED_BYTES: u64 = 2 * (BYTES as u64 + 44) + 4096;
const MAGIC: &[u8; 8] = b"FCLMCO01";
const SCAN: u32 = 64;

#[derive(Debug, thiserror::Error)]
pub enum ManagedRequestsError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Managed(#[from] ManagedStoreError),
    #[error(transparent)]
    Identity(#[from] crate::input::InputError),
    #[error("managed ownership has not been initialized")]
    Missing,
    #[error("managed ownership is not ready; complete maintenance first")]
    NotReady,
    #[error("no usable slot was found in this bounded discovery range")]
    Exhausted,
    #[error("maintenance reply belongs to an earlier local action")]
    Stale,
    #[error("managed ownership state is missing or corrupt")]
    Corrupt,
    #[error("invalid managed ownership name or context")]
    Context,
    #[error("managed maintenance response does not match its request")]
    InvalidResponse,
    #[error("managed maintenance was rejected: {0:?}")]
    Remote(AccessError),
}
pub struct ManagedRequests {
    parent: PathBuf,
    name: String,
    context: OperationContext,
    limits: ManagedStoreLimits,
}
impl ManagedRequests {
    pub fn open(
        parent: impl AsRef<Path>,
        name: &str,
        context: OperationContext,
        limits: ManagedStoreLimits,
    ) -> Result<Self, ManagedRequestsError> {
        Self::open_inner(parent.as_ref(), name, context, limits, true)
    }
    pub fn open_existing(
        parent: impl AsRef<Path>,
        name: &str,
        context: OperationContext,
        limits: ManagedStoreLimits,
    ) -> Result<Self, ManagedRequestsError> {
        Self::open_inner(parent.as_ref(), name, context, limits, false)
    }
    fn open_inner(
        parent: &Path,
        name: &str,
        context: OperationContext,
        limits: ManagedStoreLimits,
        create: bool,
    ) -> Result<Self, ManagedRequestsError> {
        if !valid_name(name)
            || context.cluster == [0; 16]
            || context.principal.is_zero()
            || context.ledger.tenant.is_zero()
            || context.ledger.session.is_zero()
        {
            return Err(ManagedRequestsError::Context);
        }
        limits.validate()?;
        if limits
            .reserved_bytes()?
            .checked_add(RESERVED_BYTES)
            .is_none_or(|bytes| bytes > limits.max_reserved_bytes)
        {
            return Err(ManagedStoreError::Capacity.into());
        }
        let this = Self {
            parent: parent.into(),
            name: name.into(),
            context,
            limits,
        };
        let (directory, initialized) =
            Directory::coordinator(parent, name, create).map_err(|e| {
                if matches!(e, StoreError::MissingOperation) {
                    ManagedRequestsError::Missing
                } else {
                    e.into()
                }
            })?;
        let record = this.record();
        if !directory.exists(&record)? {
            if initialized || directory.exists(name)? {
                return Err(ManagedRequestsError::Corrupt);
            }
            if !create {
                return Err(ManagedRequestsError::Missing);
            }
            this.save(&directory, &State::new(context, limits), false)?;
        }
        let state = this.read(&directory)?;
        if !initialized {
            if !state.initial() || directory.exists(name)? {
                return Err(ManagedRequestsError::Corrupt);
            }
            directory.finish_coordinator()?;
        }
        Ok(this)
    }
    pub fn context(&self) -> OperationContext {
        self.context
    }
    pub fn store(&self) -> Result<ManagedOperationStore, ManagedRequestsError> {
        let (_, state) = self.load()?;
        if !matches!(state.phase, Phase::Ready) {
            return Err(ManagedRequestsError::NotReady);
        }
        Ok(ManagedOperationStore::open(
            self.parent.join(&self.name),
            self.context,
            self.limits,
        )?)
    }
    /// Mark only an exact receipt already durably retained by the child store.
    /// Merely reading a receipt never marks it delivered.
    pub fn mark_delivered(&self, id: ManagedOperationId) -> Result<(), ManagedRequestsError> {
        let (directory, mut state) = self.load()?;
        let store = self.ready(&state)?;
        let receipt = match store.receipt(id) {
            Ok(Some(receipt)) => receipt,
            Ok(None) => return Err(ManagedStoreError::Unresolved.into()),
            Err(ManagedStoreError::Retired) => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        let mark = Delivered {
            key: receipt.key,
            hash: receipt
                .content_hash()
                .map_err(|_| ManagedRequestsError::Corrupt)?,
        };
        self.prune(&store, &mut state)?;
        match state
            .delivered
            .binary_search_by_key(&mark.key.ordinal, |m| m.key.ordinal)
        {
            Ok(index) => {
                if state.delivered.get(index) != Some(&mark) {
                    return Err(ManagedRequestsError::Corrupt);
                }
            }
            Err(index) => {
                if state.delivered.len() >= self.limits.window as usize {
                    return Err(ManagedStoreError::Capacity.into());
                }
                state
                    .delivered
                    .try_reserve_exact(1)
                    .map_err(|_| ManagedStoreError::Capacity)?;
                state.delivered.insert(index, mark);
            }
        }
        self.save(&directory, &state, true)
    }
    fn ready(&self, state: &State) -> Result<ManagedOperationStore, ManagedRequestsError> {
        if !matches!(state.phase, Phase::Ready) {
            return Err(ManagedRequestsError::NotReady);
        }
        Ok(ManagedOperationStore::open(
            self.parent.join(&self.name),
            self.context,
            self.limits,
        )?)
    }
    fn prune(
        &self,
        store: &ManagedOperationStore,
        state: &mut State,
    ) -> Result<bool, ManagedRequestsError> {
        let status = store.status()?;
        let before = state.delivered.len();
        state
            .delivered
            .retain(|mark| mark.key.ordinal > status.retired_through);
        for mark in &state.delivered {
            if mark.key.stream != status.stream {
                return Err(ManagedRequestsError::Corrupt);
            }
            let receipt = store
                .receipt(ManagedOperationId::from_key(mark.key)?)?
                .ok_or(ManagedRequestsError::Corrupt)?;
            if receipt
                .content_hash()
                .map_err(|_| ManagedRequestsError::Corrupt)?
                != mark.hash
            {
                return Err(ManagedRequestsError::Corrupt);
            }
        }
        Ok(before != state.delivered.len())
    }
    fn record(&self) -> String {
        format!("{}.managed-owner", self.name)
    }
    fn load(&self) -> Result<(Directory, State), ManagedRequestsError> {
        let (directory, initialized) = Directory::coordinator(&self.parent, &self.name, false)?;
        if !initialized {
            return Err(ManagedRequestsError::Corrupt);
        }
        let state = self.read(&directory)?;
        Ok((directory, state))
    }
    fn read(&self, directory: &Directory) -> Result<State, ManagedRequestsError> {
        let bytes = directory.read(&self.record(), MAGIC, BYTES)?;
        let (state, rest): (State, _) =
            postcard::take_from_bytes(&bytes).map_err(|_| ManagedRequestsError::Corrupt)?;
        if !rest.is_empty() || encode(&state)? != bytes {
            return Err(ManagedRequestsError::Corrupt);
        }
        state.validate(self.context, self.limits)?;
        if matches!(state.phase, Phase::Ready) {
            self.ready(&state)?;
        }
        Ok(state)
    }
    fn save(
        &self,
        directory: &Directory,
        state: &State,
        replace: bool,
    ) -> Result<(), ManagedRequestsError> {
        state.validate(self.context, self.limits)?;
        directory.write(&self.record(), MAGIC, &encode(state)?, replace)?;
        Ok(())
    }
}
fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name != "."
        && name != ".."
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
}
pub(crate) fn record_limit(name: &str) -> Option<usize> {
    name.strip_suffix(".managed-owner")
        .filter(|name| valid_name(name))
        .map(|_| BYTES)
}
fn encode(value: &impl Serialize) -> Result<Vec<u8>, ManagedRequestsError> {
    let n = postcard::experimental::serialized_size(value)
        .map_err(|_| ManagedRequestsError::Corrupt)?;
    if n > BYTES {
        return Err(ManagedStoreError::Capacity.into());
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(n)
        .map_err(|_| ManagedStoreError::Capacity)?;
    bytes.resize(n, 0);
    postcard::to_slice(value, &mut bytes).map_err(|_| ManagedRequestsError::Corrupt)?;
    Ok(bytes)
}
fn digest(value: &impl Serialize) -> Result<ContentHash, ManagedRequestsError> {
    Ok(ContentHash(*blake3::hash(&encode(value)?).as_bytes()))
}
fn fresh(ids: &mut impl IdGenerator) -> Result<RequestId, ManagedRequestsError> {
    let id = RequestId(ids.next_id()?);
    if id.is_zero() {
        Err(ManagedRequestsError::Context)
    } else {
        Ok(id)
    }
}
fn same_request(a: &RequestEnvelope, b: &RequestEnvelope) -> bool {
    a.protocol == b.protocol
        && a.ledger == b.ledger
        && a.request_epoch == b.request_epoch
        && a.request_id == b.request_id
        && a.operation == b.operation
}
fn request_digest(request: &RequestEnvelope) -> Result<ContentHash, ManagedRequestsError> {
    digest(&(
        request.protocol,
        request.ledger,
        request.request_epoch,
        request.request_id,
        &request.operation,
    ))
}

#[cfg(test)]
mod tests;
