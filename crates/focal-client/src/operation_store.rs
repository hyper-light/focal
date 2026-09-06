//! Synchronous, bounded operation-ID admission over the exact pending journal.
//! A short catalogue lock protects registration; the returned journal retains
//! only its operation lock while the caller awaits network results.
pub(crate) mod files;
use crate::{
    RequestEnvelope,
    input::InputError,
    pending::{OperationContext, OperationJournal, PendingError},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

pub const MAX_INTENT_BYTES: usize = 256 * 1024;
const MAX_NAME_BYTES: usize = 64;
const MAX_OPERATIONS: u32 = 4096;
const CATALOGUE_BYTES: usize = 1024 * 1024;
const PREPARED_BYTES: usize =
    2 * crate::pending::MAX_OPERATION_REQUEST_BYTES + MAX_INTENT_BYTES + 512;
const ROOT_RESERVATION: u64 = 2 * CATALOGUE_BYTES as u64 + 4096;
// Includes the old/new journal generations, the immutable prepared record and
// its temporary, small marker/lock files, and bounded filesystem metadata.
const OPERATION_RESERVATION: u64 = 2 * (crate::pending::MAX_STATE_BYTES as u64 + 44)
    + 2 * (PREPARED_BYTES as u64 + 44)
    + 32 * 1024;
const CATALOGUE: &str = "catalogue.bin";
const PREPARED: &str = "prepared.bin";
const CATALOGUE_MAGIC: &[u8; 8] = b"FCLID001";
const PREPARED_MAGIC: &[u8; 8] = b"FCLPRE01";

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("operation store I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("operation store already exists; open it without reinitialization")]
    Exists,
    #[error("operation store is locked by another admission")]
    Locked,
    #[error("operation store requires owner-private directories and files")]
    Permissions,
    #[error("operation store is missing or corrupt")]
    Corrupt,
    #[error("operation store limits differ from its initialized capability")]
    LimitsMismatch,
    #[error("operation store capacity exceeded")]
    Capacity,
    #[error("operation ID must be exactly 32 nonzero hexadecimal digits")]
    InvalidId,
    #[error("operation intent name, version, or canonical bytes are invalid")]
    InvalidIntent,
    #[error("operation ID belongs to a different cluster, principal, or ledger")]
    ContextMismatch,
    #[error("operation ID is already bound to a different authored intent")]
    IntentConflict,
    #[error("operation ID has never been admitted; retry cannot create it")]
    MissingOperation,
    #[error("operation initialization is incomplete; saved identity cannot be regenerated")]
    Incomplete,
    #[error("operation expansion failed: {0}")]
    Expansion(#[from] InputError),
    #[error(transparent)]
    Pending(#[from] PendingError),
}

/// The capability is stored with the catalogue. Retrying with different limits
/// cannot silently increase it. Quota covers every claimed ID, even incomplete
/// initialization; this owner never automatically deletes retry identities.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreLimits {
    pub max_operations: u32,
    pub max_reserved_bytes: u64,
}
impl Default for StoreLimits {
    fn default() -> Self {
        Self {
            max_operations: 256,
            max_reserved_bytes: 512 * 1024 * 1024,
        }
    }
}
impl StoreLimits {
    fn validate(self) -> Result<(), StoreError> {
        if self.max_operations == 0
            || self.max_operations > MAX_OPERATIONS
            || self.max_reserved_bytes < ROOT_RESERVATION
        {
            return Err(StoreError::Capacity);
        }
        Ok(())
    }
}

/// Canonical bytes come from the shared typed operation registry, after defaults
/// and aliases are normalized and before any IDs are generated. Byte comparison
/// is exact; callers must keep normalization stable for this named version.
#[derive(Clone, Copy)]
pub struct OperationIntent<'a> {
    pub name: &'a str,
    pub version: u16,
    pub canonical: &'a [u8],
}
impl OperationIntent<'_> {
    fn validate(self) -> Result<(), StoreError> {
        if self.name.is_empty()
            || self.name.len() > MAX_NAME_BYTES
            || self.version == 0
            || !self.name.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"._-".contains(&byte)
            })
            || self.canonical.is_empty()
        {
            return Err(StoreError::InvalidIntent);
        }
        if self.canonical.len() > MAX_INTENT_BYTES {
            return Err(StoreError::Capacity);
        }
        Ok(())
    }
    pub(crate) fn digest(self) -> Result<[u8; 32], StoreError> {
        self.validate()?;
        let mut hash = blake3::Hasher::new_derive_key("focal.manual-operation.intent.v1");
        hash.update(
            &u64::try_from(self.name.len())
                .map_err(|_| StoreError::Capacity)?
                .to_be_bytes(),
        );
        hash.update(self.name.as_bytes());
        hash.update(&self.version.to_be_bytes());
        hash.update(
            &u64::try_from(self.canonical.len())
                .map_err(|_| StoreError::Capacity)?
                .to_be_bytes(),
        );
        hash.update(self.canonical);
        Ok(*hash.finalize().as_bytes())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreUsage {
    pub operations: u32,
    pub reserved_bytes: u64,
}
#[derive(Serialize, Deserialize)]
struct Catalogue {
    schema: u16,
    limits: StoreLimits,
    entries: BTreeMap<[u8; 16], Entry>,
}
#[derive(Serialize, Deserialize)]
struct Entry {
    context: OperationContext,
    intent: [u8; 32],
    ready: bool,
}
impl Catalogue {
    fn usage(&self) -> Result<StoreUsage, StoreError> {
        let operations = u32::try_from(self.entries.len()).map_err(|_| StoreError::Capacity)?;
        let reserved_bytes = u64::from(operations)
            .checked_mul(OPERATION_RESERVATION)
            .and_then(|bytes| bytes.checked_add(ROOT_RESERVATION))
            .ok_or(StoreError::Capacity)?;
        Ok(StoreUsage {
            operations,
            reserved_bytes,
        })
    }
    fn validate(&self, limits: StoreLimits) -> Result<(), StoreError> {
        if self.schema != 1 || self.entries.keys().any(|id| *id == [0; 16]) {
            return Err(StoreError::Corrupt);
        }
        if self.limits != limits {
            return Err(StoreError::LimitsMismatch);
        }
        limits.validate()?;
        let usage = self.usage()?;
        if usage.operations > limits.max_operations
            || usage.reserved_bytes > limits.max_reserved_bytes
        {
            return Err(StoreError::Capacity);
        }
        for entry in self.entries.values() {
            validate_context(entry.context)?;
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
    open_epoch: RequestEnvelope,
    request: RequestEnvelope,
}
impl Prepared {
    fn intent(&self) -> OperationIntent<'_> {
        OperationIntent {
            name: &self.name,
            version: self.version,
            canonical: &self.canonical,
        }
    }
    fn matches(
        &self,
        context: OperationContext,
        intent: OperationIntent<'_>,
    ) -> Result<(), StoreError> {
        if self.schema != 1 {
            return Err(StoreError::Corrupt);
        }
        if self.context != context {
            return Err(StoreError::ContextMismatch);
        }
        if self.name != intent.name
            || self.version != intent.version
            || self.canonical != intent.canonical
        {
            return Err(StoreError::IntentConflict);
        }
        Ok(())
    }
}

/// No store lock or background owner survives a method call. All methods do
/// synchronous I/O: invoke them on the CLI/MCP blocking owner outside async work.
/// Creation is explicitly separate from opening so losing a whole store cannot
/// silently re-admit IDs. A host needing automatic initialization must retain its
/// initialized marker outside this directory.
pub struct OperationStore {
    root: PathBuf,
    limits: StoreLimits,
    #[cfg(test)]
    fault: std::cell::Cell<Option<Fault>>,
}
impl OperationStore {
    pub fn create(root: impl AsRef<Path>, limits: StoreLimits) -> Result<Self, StoreError> {
        limits.validate()?;
        let catalogue = Catalogue {
            schema: 1,
            limits,
            entries: BTreeMap::new(),
        };
        let bytes = encode(&catalogue, CATALOGUE_BYTES)?;
        let directory = files::Directory::create(root.as_ref())?;
        directory.initialize()?;
        directory.write(CATALOGUE, CATALOGUE_MAGIC, &bytes, false)?;
        Ok(Self::handle(root.as_ref(), limits))
    }
    pub fn open(root: impl AsRef<Path>, limits: StoreLimits) -> Result<Self, StoreError> {
        limits.validate()?;
        let store = Self::handle(root.as_ref(), limits);
        let directory = files::Directory::open(&store.root)?;
        store.catalogue(&directory)?;
        Ok(store)
    }
    fn handle(root: &Path, limits: StoreLimits) -> Self {
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
    pub fn usage(&self) -> Result<StoreUsage, StoreError> {
        let directory = files::Directory::open(&self.root)?;
        self.catalogue(&directory)?.usage()
    }
    /// Locate a claimed ID for the existing explicit journal inspect/retry API.
    /// This does not create files or grant authority to use a different context.
    pub fn operation_path(&self, operation_id: &str) -> Result<PathBuf, StoreError> {
        let id = parse_id(operation_id)?;
        let directory = files::Directory::open(&self.root)?;
        if !self.catalogue(&directory)?.entries.contains_key(&id) {
            return Err(StoreError::MissingOperation);
        }
        Ok(self.root.join(component(id)).join("journal"))
    }
    pub fn open_or_create(
        &self,
        operation_id: &str,
        context: OperationContext,
        intent: OperationIntent<'_>,
        expand: impl FnOnce() -> Result<(RequestEnvelope, RequestEnvelope), StoreError>,
    ) -> Result<OperationJournal, StoreError> {
        self.resolve(operation_id, context, intent, Some(expand))
    }
    pub fn retry(
        &self,
        operation_id: &str,
        context: OperationContext,
        intent: OperationIntent<'_>,
    ) -> Result<OperationJournal, StoreError> {
        self.resolve(
            operation_id,
            context,
            intent,
            None::<fn() -> Result<(RequestEnvelope, RequestEnvelope), StoreError>>,
        )
    }
    /// Open only a previously claimed ID, using its saved canonical intent.
    /// Recovery may finish constructing its journal from the already saved
    /// complete requests. It never expands authored input or generates IDs.
    pub fn open_existing(
        &self,
        operation_id: &str,
        context: &OperationContext,
    ) -> Result<OperationJournal, StoreError> {
        let id = parse_id(operation_id)?;
        validate_context(*context)?;
        let directory = files::Directory::open(&self.root)?;
        let mut catalogue = self.catalogue(&directory)?;
        let entry = catalogue
            .entries
            .get(&id)
            .ok_or(StoreError::MissingOperation)?;
        if entry.context != *context {
            return Err(StoreError::ContextMismatch);
        }
        let prepared = self.prepared(&directory, id, entry.ready)?;
        prepared.matches(*context, prepared.intent())?;
        if prepared.intent().digest()? != entry.intent {
            return Err(StoreError::Corrupt);
        }
        self.finish(&directory, &mut catalogue, id, *context, prepared)
    }
    fn resolve(
        &self,
        operation_id: &str,
        context: OperationContext,
        intent: OperationIntent<'_>,
        expand: Option<impl FnOnce() -> Result<(RequestEnvelope, RequestEnvelope), StoreError>>,
    ) -> Result<OperationJournal, StoreError> {
        let id = parse_id(operation_id)?;
        validate_context(context)?;
        let digest = intent.digest()?;
        let directory = files::Directory::open(&self.root)?;
        let mut catalogue = self.catalogue(&directory)?;
        let component = component(id);
        let prepared_path = format!("{component}/{PREPARED}");
        let mut prepared = None;
        if let Some(entry) = catalogue.entries.get(&id) {
            if entry.context != context {
                return Err(StoreError::ContextMismatch);
            }
            if entry.intent != digest {
                return Err(StoreError::IntentConflict);
            }
        } else {
            let expand = expand.ok_or(StoreError::MissingOperation)?;
            let usage = catalogue.usage()?;
            if usage.operations >= self.limits.max_operations
                || usage
                    .reserved_bytes
                    .checked_add(OPERATION_RESERVATION)
                    .is_none_or(|bytes| bytes > self.limits.max_reserved_bytes)
            {
                return Err(StoreError::Capacity);
            }
            // Never adopt an unindexed operation directory after metadata loss.
            if directory.exists(&component)? {
                return Err(StoreError::Corrupt);
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
            let (open_epoch, request) = expand()?;
            let value = Prepared {
                schema: 1,
                context,
                name: intent.name.into(),
                version: intent.version,
                canonical: intent.canonical.into(),
                open_epoch,
                request,
            };
            let bytes = encode(&value, PREPARED_BYTES)?;
            directory.create_child(&component)?;
            directory.write(&prepared_path, PREPARED_MAGIC, &bytes, false)?;
            #[cfg(test)]
            self.fail_at(Fault::Prepared)?;
            prepared = Some(value);
        }
        let ready = catalogue.entries.get(&id).ok_or(StoreError::Corrupt)?.ready;
        let prepared = match prepared {
            Some(value) => value,
            None => self.prepared(&directory, id, ready)?,
        };
        prepared.matches(context, intent)?;
        self.finish(&directory, &mut catalogue, id, context, prepared)
    }
    fn prepared(
        &self,
        directory: &files::Directory,
        id: [u8; 16],
        ready: bool,
    ) -> Result<Prepared, StoreError> {
        let component = component(id);
        if !directory.exists(&component)? {
            return Err(if ready {
                StoreError::Corrupt
            } else {
                StoreError::Incomplete
            });
        }
        directory.check_child(&component)?;
        let prepared_path = format!("{component}/{PREPARED}");
        if !directory.exists(&prepared_path)? {
            return Err(if ready {
                StoreError::Corrupt
            } else {
                StoreError::Incomplete
            });
        }
        let bytes = directory.read(&prepared_path, PREPARED_MAGIC, PREPARED_BYTES)?;
        let prepared: Prepared = decode(&bytes)?;
        if encode(&prepared, PREPARED_BYTES)? != bytes {
            return Err(StoreError::Corrupt);
        }
        Ok(prepared)
    }
    fn finish(
        &self,
        directory: &files::Directory,
        catalogue: &mut Catalogue,
        id: [u8; 16],
        context: OperationContext,
        prepared: Prepared,
    ) -> Result<OperationJournal, StoreError> {
        let ready = catalogue.entries.get(&id).ok_or(StoreError::Corrupt)?.ready;
        let component = component(id);
        let journal_path = self.root.join(&component).join("journal");
        let journal = if directory.exists(&format!("{component}/journal"))? {
            if ready {
                let journal = OperationJournal::open(&journal_path, &context)?;
                if !journal.matches_requests(&prepared.open_epoch, &prepared.request) {
                    return Err(StoreError::Corrupt);
                }
                journal
            } else {
                OperationJournal::resume_prepared(
                    &journal_path,
                    context,
                    prepared.open_epoch,
                    prepared.request,
                )?
            }
        } else if ready {
            return Err(StoreError::Corrupt);
        } else {
            OperationJournal::create(
                &journal_path,
                context,
                prepared.open_epoch,
                prepared.request,
            )?
        };
        if !ready {
            #[cfg(test)]
            self.fail_at(Fault::Journal)?;
            catalogue
                .entries
                .get_mut(&id)
                .ok_or(StoreError::Corrupt)?
                .ready = true;
            self.save(directory, catalogue)?;
            #[cfg(test)]
            self.fail_at(Fault::Ready)?;
        }
        Ok(journal)
    }
    fn catalogue(&self, directory: &files::Directory) -> Result<Catalogue, StoreError> {
        let bytes = directory.read(CATALOGUE, CATALOGUE_MAGIC, CATALOGUE_BYTES)?;
        let catalogue: Catalogue = decode(&bytes)?;
        catalogue.validate(self.limits)?;
        if encode(&catalogue, CATALOGUE_BYTES)? != bytes {
            return Err(StoreError::Corrupt);
        }
        Ok(catalogue)
    }
    fn save(&self, directory: &files::Directory, catalogue: &Catalogue) -> Result<(), StoreError> {
        catalogue.validate(self.limits)?;
        directory.write(
            CATALOGUE,
            CATALOGUE_MAGIC,
            &encode(catalogue, CATALOGUE_BYTES)?,
            true,
        )
    }
    #[cfg(test)]
    fn fail_at(&self, fault: Fault) -> Result<(), StoreError> {
        if self.fault.get() == Some(fault) {
            self.fault.set(None);
            Err(std::io::Error::other("injected operation-store initialization failure").into())
        } else {
            Ok(())
        }
    }
}
fn parse_id(value: &str) -> Result<[u8; 16], StoreError> {
    crate::input::parse_id(value).map_err(|_| StoreError::InvalidId)
}
fn component(id: [u8; 16]) -> String {
    format!("{:032x}", u128::from_be_bytes(id))
}
fn validate_context(context: OperationContext) -> Result<(), StoreError> {
    if context.cluster == [0; 16]
        || context.principal.is_zero()
        || context.ledger.tenant.is_zero()
        || context.ledger.session.is_zero()
    {
        Err(StoreError::ContextMismatch)
    } else {
        Ok(())
    }
}
fn encode(value: &impl Serialize, maximum: usize) -> Result<Vec<u8>, StoreError> {
    let length = postcard::experimental::serialized_size(value).map_err(|_| StoreError::Corrupt)?;
    if length > maximum {
        return Err(StoreError::Capacity);
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length)
        .map_err(|_| StoreError::Capacity)?;
    bytes.resize(length, 0);
    postcard::to_slice(value, &mut bytes).map_err(|_| StoreError::Corrupt)?;
    Ok(bytes)
}
fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, StoreError> {
    let (value, rest) = postcard::take_from_bytes(bytes).map_err(|_| StoreError::Corrupt)?;
    if !rest.is_empty() {
        return Err(StoreError::Corrupt);
    }
    Ok(value)
}
#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum Fault {
    Claimed,
    Prepared,
    Journal,
    Ready,
}
#[cfg(test)]
mod tests;
