//! The bounded catalogue owns all claimed IDs, including incomplete creation.
//! It holds its lock only for admission; returned journals lock their own bytes.
use super::{MAX_TRANSFER_BYTES, TransferError, TransferLimits, UploadJournal, UploadSpec, files};
use crate::pending::OperationContext;
use focal_model::RouteEpoch;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

pub(super) const CATALOGUE_BYTES: usize = 256 * 1024;
const TRANSFER_OVERHEAD: u64 = 1024 * 1024;
const CATALOGUE_RESERVATION: u64 = 1024 * 1024;
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UploadStoreLimits {
    pub max_transfers: u32,
    pub max_reserved_bytes: u64,
    pub transfer: TransferLimits,
}
impl Default for UploadStoreLimits {
    fn default() -> Self {
        Self {
            max_transfers: 64,
            max_reserved_bytes: 256 * 1024 * 1024,
            transfer: TransferLimits::default(),
        }
    }
}
impl UploadStoreLimits {
    fn validate(self) -> Result<(), TransferError> {
        self.transfer.validate()?;
        if self.max_transfers == 0
            || self.max_transfers > 1024
            || self.max_reserved_bytes < CATALOGUE_RESERVATION
            || self.max_reserved_bytes > 16 * 1024 * MAX_TRANSFER_BYTES
        {
            return Err(TransferError::Capacity);
        }
        Ok(())
    }
}
#[derive(Serialize, Deserialize)]
struct Entry {
    context: OperationContext,
    spec: UploadSpec,
    route_epoch: RouteEpoch,
    ready: bool,
}
#[derive(Serialize, Deserialize)]
struct Catalogue {
    version: u16,
    limits: UploadStoreLimits,
    entries: BTreeMap<[u8; 16], Entry>,
}
impl Catalogue {
    fn validate(&self, limits: UploadStoreLimits) -> Result<(), TransferError> {
        limits.validate()?;
        if self.version != 1
            || self.limits != limits
            || self.entries.len() > limits.max_transfers as usize
        {
            return Err(TransferError::Corrupt);
        }
        for (id, entry) in &self.entries {
            if id != &entry.spec.upload
                || *id == [0; 16]
                || entry.context.cluster == [0; 16]
                || entry.context.principal.is_zero()
                || entry.context.ledger.tenant.is_zero()
                || entry.context.ledger.session.is_zero()
                || entry.route_epoch.0 == 0
                || entry.spec.length > limits.transfer.max_payload_bytes
            {
                return Err(TransferError::Corrupt);
            }
        }
        if self.reserved()? > limits.max_reserved_bytes {
            return Err(TransferError::Capacity);
        }
        Ok(())
    }
    fn reserved(&self) -> Result<u64, TransferError> {
        self.entries
            .values()
            .try_fold(CATALOGUE_RESERVATION, |sum, entry| {
                sum.checked_add(entry.spec.length)
                    .and_then(|n| n.checked_add(TRANSFER_OVERHEAD))
                    .ok_or(TransferError::Capacity)
            })
    }
    fn save(&self, directory: &files::Directory, initial: bool) -> Result<(), TransferError> {
        self.validate(self.limits)?;
        let size =
            postcard::experimental::serialized_size(self).map_err(|_| TransferError::Corrupt)?;
        if size > CATALOGUE_BYTES {
            return Err(TransferError::Capacity);
        }
        let mut bytes = super::buffer(size)?;
        let length = postcard::to_slice(self, &mut bytes)
            .map_err(|_| TransferError::Corrupt)?
            .len();
        bytes.truncate(length);
        directory.install(&bytes, initial)
    }
}

/// Create and open are separate. A service's initialized marker must live
/// outside this directory when automatic first-use creation is desired, so loss
/// of the entire store cannot silently introduce a fresh transfer-ID space.
pub struct UploadStore {
    path: PathBuf,
    limits: UploadStoreLimits,
}
impl UploadStore {
    /// Owner-private first-use startup with an external initialized marker.
    /// Any prior bootstrap evidence selects open-only and never resets IDs.
    pub fn bootstrap(
        parent: impl AsRef<Path>,
        name: &str,
        context: OperationContext,
        limits: UploadStoreLimits,
    ) -> Result<Self, TransferError> {
        limits.validate()?;
        let (_lock, first) = files::bootstrap(parent.as_ref(), name, context)?;
        if first {
            Self::create(parent.as_ref().join(name), limits)
        } else {
            Self::open(parent.as_ref().join(name), limits)
        }
    }
    pub fn create(
        path: impl AsRef<Path>,
        limits: UploadStoreLimits,
    ) -> Result<Self, TransferError> {
        limits.validate()?;
        let catalogue = Catalogue {
            version: 1,
            limits,
            entries: BTreeMap::new(),
        };
        let directory = files::Directory::create_catalogue(path.as_ref())?;
        catalogue.save(&directory, true)?;
        Ok(Self {
            path: path.as_ref().into(),
            limits,
        })
    }
    pub fn open(path: impl AsRef<Path>, limits: UploadStoreLimits) -> Result<Self, TransferError> {
        let store = Self {
            path: path.as_ref().into(),
            limits,
        };
        store.load()?;
        Ok(store)
    }
    pub fn path(&self) -> &Path {
        &self.path
    }
    pub fn limits(&self) -> UploadStoreLimits {
        self.limits
    }
    /// Exact metadata retry is checked before creating or reading any new source
    /// bytes. A ready transfer can only be opened, never replaced/reinitialized.
    pub fn begin(
        &self,
        context: OperationContext,
        spec: UploadSpec,
        route_epoch: RouteEpoch,
    ) -> Result<UploadJournal, TransferError> {
        if spec.upload == [0; 16] || spec.length > self.limits.transfer.max_payload_bytes {
            return Err(TransferError::Invalid);
        }
        if spec.length == 0 && spec.digest.0 != *blake3::hash(&[]).as_bytes() {
            return Err(TransferError::Conflict);
        }
        let (directory, mut catalogue) = self.load()?;
        if let Some(entry) = catalogue.entries.get(&spec.upload) {
            if entry.context != context || entry.spec != spec {
                return Err(TransferError::Conflict);
            }
        } else {
            if catalogue.entries.len() >= self.limits.max_transfers as usize
                || catalogue
                    .reserved()?
                    .checked_add(spec.length)
                    .and_then(|n| n.checked_add(TRANSFER_OVERHEAD))
                    .is_none_or(|n| n > self.limits.max_reserved_bytes)
            {
                return Err(TransferError::Capacity);
            }
            catalogue.entries.insert(
                spec.upload,
                Entry {
                    context,
                    spec,
                    route_epoch,
                    ready: false,
                },
            );
            catalogue.save(&directory, false)?;
        }
        let entry = catalogue
            .entries
            .get(&spec.upload)
            .ok_or(TransferError::Corrupt)?;
        let path = self.operation_path(spec.upload)?;
        let journal = match fs::symlink_metadata(&path) {
            Ok(_) => UploadJournal::open(&path, &context)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound && !entry.ready => {
                UploadJournal::begin(
                    &path,
                    context,
                    spec,
                    entry.route_epoch,
                    self.limits.transfer,
                )?
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(TransferError::Corrupt);
            }
            Err(error) => return Err(error.into()),
        };
        if journal.spec() != spec || journal.limits() != self.limits.transfer {
            return Err(TransferError::Corrupt);
        }
        if !entry.ready {
            catalogue
                .entries
                .get_mut(&spec.upload)
                .ok_or(TransferError::Corrupt)?
                .ready = true;
            catalogue.save(&directory, false)?;
        }
        Ok(journal)
    }
    pub fn open_upload(
        &self,
        upload: [u8; 16],
        context: &OperationContext,
    ) -> Result<UploadJournal, TransferError> {
        let (_directory, catalogue) = self.load()?;
        let entry = catalogue
            .entries
            .get(&upload)
            .ok_or(TransferError::Invalid)?;
        if entry.context != *context {
            return Err(TransferError::Conflict);
        }
        if !entry.ready {
            return Err(TransferError::Incomplete);
        }
        let journal = UploadJournal::open(self.operation_path(upload)?, context)?;
        if journal.spec() != entry.spec || journal.limits() != self.limits.transfer {
            return Err(TransferError::Corrupt);
        }
        Ok(journal)
    }
    pub fn operation_path(&self, upload: [u8; 16]) -> Result<PathBuf, TransferError> {
        if upload == [0; 16] {
            return Err(TransferError::Invalid);
        }
        Ok(self
            .path
            .join(format!("{:032x}", u128::from_be_bytes(upload))))
    }
    fn load(&self) -> Result<(files::Directory, Catalogue), TransferError> {
        self.limits.validate()?;
        let directory = files::Directory::open_catalogue(&self.path)?;
        let bytes = directory.read()?;
        let (catalogue, remaining): (Catalogue, &[u8]) =
            postcard::take_from_bytes(&bytes).map_err(|_| TransferError::Corrupt)?;
        if !remaining.is_empty() {
            return Err(TransferError::Corrupt);
        }
        catalogue.validate(self.limits)?;
        Ok((directory, catalogue))
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{context, spec};
    use super::*;
    #[test]
    fn catalogue_reserves_full_payload_before_id_admission_and_reopens_exact() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("uploads");
        let limits = UploadStoreLimits {
            max_transfers: 2,
            max_reserved_bytes: CATALOGUE_RESERVATION + TRANSFER_OVERHEAD + 3,
            transfer: TransferLimits::default(),
        };
        let store = UploadStore::create(&path, limits).unwrap();
        let mut invalid = spec(b"");
        invalid.digest.0 = [7; 32];
        assert!(matches!(
            store.begin(context(), invalid, RouteEpoch(1)),
            Err(TransferError::Conflict)
        ));
        assert!(store.load().unwrap().1.entries.is_empty());
        let mut journal = store.begin(context(), spec(b"abc"), RouteEpoch(1)).unwrap();
        journal.stage(0, b"abc").unwrap();
        assert!(matches!(
            store.open_upload(spec(b"abc").upload, &context()),
            Err(TransferError::Locked)
        ));
        let mut another = spec(b"x");
        another.upload = [6; 16];
        assert!(matches!(
            store.begin(context(), another, RouteEpoch(1)),
            Err(TransferError::Capacity)
        ));
        assert!(!store.operation_path(another.upload).unwrap().exists());
        drop(journal);
        drop(store);
        let store = UploadStore::open(&path, limits).unwrap();
        let journal = store.begin(context(), spec(b"abc"), RouteEpoch(7)).unwrap();
        assert_eq!(journal.progress().staged, 3);
        drop(journal);
        assert!(matches!(
            store.begin(context(), spec(b"abd"), RouteEpoch(1)),
            Err(TransferError::Conflict)
        ));
        fs::remove_file(
            store
                .operation_path(spec(b"abc").upload)
                .unwrap()
                .join("upload.bin"),
        )
        .unwrap();
        assert!(matches!(
            store.begin(context(), spec(b"abc"), RouteEpoch(1)),
            Err(TransferError::Corrupt)
        ));
    }
    #[test]
    fn only_unready_exact_claim_can_resume_absent_child_and_missing_catalogue_never_recreates() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("uploads");
        let limits = UploadStoreLimits::default();
        let store = UploadStore::create(&path, limits).unwrap();
        let (dir, mut catalogue) = store.load().unwrap();
        let spec = spec(b"abc");
        catalogue.entries.insert(
            spec.upload,
            Entry {
                context: context(),
                spec,
                route_epoch: RouteEpoch(3),
                ready: false,
            },
        );
        catalogue.save(&dir, false).unwrap();
        drop(dir);
        assert!(matches!(
            store.open_upload(spec.upload, &context()),
            Err(TransferError::Incomplete)
        ));
        let mut journal = store.begin(context(), spec, RouteEpoch(1)).unwrap();
        assert_eq!(
            journal.next_request().unwrap().unwrap().route_epoch,
            RouteEpoch(3)
        );
        drop(journal);
        fs::remove_dir_all(store.operation_path(spec.upload).unwrap()).unwrap();
        assert!(matches!(
            store.begin(context(), spec, RouteEpoch(1)),
            Err(TransferError::Corrupt)
        ));
        fs::remove_file(path.join("uploads.bin")).unwrap();
        assert!(matches!(
            UploadStore::open(&path, limits),
            Err(TransferError::Corrupt)
        ));
    }
}
