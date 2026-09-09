use super::{CliError, Context, Result, Settings};
use focal_client::{
    managed_requests::ManagedRequests,
    managed_store::ManagedStoreLimits,
    operation_store::{OperationStore, StoreError, StoreLimits},
    pending::OperationContext,
};
use fs2::FileExt;
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};

const STORE: &str = "MCP.operations";
const LOCK: &str = "MCP.operations.lock";
const MARKER: &str = "MCP.operations.initialized";
const MAGIC: &[u8; 8] = b"FCLMCP01";
const MARKER_BYTES: u64 = 104;

pub(crate) fn serve(settings: &Settings, selection: Option<&str>) -> Result<()> {
    if tokio::runtime::Handle::try_current().is_ok() {
        return Err(CliError::Input(
            "MCP foreground startup requires a blocking process owner".into(),
        ));
    }
    let Context {
        client,
        build,
        operation,
        root,
        admin_root,
        invocation: _,
    } = Context::open(settings, selection)?;
    let store = Bootstrap::open(&root)
        .and_then(|bootstrap| bootstrap.finish(&operation))
        .map_err(|error| CliError::Other(Box::new(error)))?;
    let managed = ManagedRequests::open_with(
        &root,
        "MCP.requests",
        operation,
        ManagedStoreLimits::default(),
        super::managed::rotation()?,
    )
    .map_err(|error| CliError::Other(Box::new(error)))?;
    let mut backend =
        focal_mcp::Backend::new(client, build, operation, store)?.with_managed_requests(managed)?;
    let uploads = focal_client::artifact_transfer::UploadStore::bootstrap(
        &root,
        "MCP.uploads",
        operation,
        focal_client::artifact_transfer::UploadStoreLimits::default(),
    )
    .map_err(|error| CliError::Other(Box::new(error)))?;
    backend = backend.with_uploads(uploads);
    let watches = focal_client::watch::WatchStore::open(&root, operation)
        .map_err(|error| CliError::Other(Box::new(error)))?;
    backend = backend.with_watches(watches)?;
    // The engine probe runs inside the adapter on its worker runtime; this
    // adapter's own native journal is opened only when the ledger is native.
    backend = backend.with_native_journal(Box::new(NativeJournal {
        parent: root.join("client"),
    }));
    if let Some(admin_root) = admin_root {
        let mut admin_settings = Settings::default();
        admin_settings.node.data_dir = Some(admin_root);
        if let Some(admin) = focal_node::cluster_admin::ClusterAdmin::available(&admin_settings)
            .map_err(|error| CliError::Other(Box::new(error)))?
        {
            backend = backend.with_admin(Box::new(admin));
        }
    }
    focal_mcp::serve(backend, std::io::stdin(), std::io::stdout())
        .map_err(|error| CliError::Other(Box::new(error)))
}

/// The MCP adapter's `n1:` journal, beside the human CLI's under the context
/// root; each adapter owns its identities and delivery marks.
struct NativeJournal {
    parent: PathBuf,
}
impl focal_mcp::NativeJournal for NativeJournal {
    fn initialized(&self) -> bool {
        super::native::initialized_in(&self.parent, super::native::MCP_STORE)
    }
    fn open(
        self: Box<Self>,
    ) -> std::result::Result<
        focal_client::native_store::NativeOperationStore,
        focal_mcp::JournalError,
    > {
        match super::native::store_in(&self.parent, super::native::MCP_STORE, true) {
            Ok(Some(store)) => Ok(store),
            Ok(None) => Err("native journal was not created".into()),
            Err(error) => Err(Box::new(error)),
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum BootstrapError {
    #[error("MCP operation-store initialization I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("MCP operation-store initialization is owned by another process")]
    Locked,
    #[error("MCP operation-store bootstrap requires private owner-matching files and directory")]
    Permissions,
    #[error(
        "MCP operation-store initialization is incomplete or missing; existing IDs cannot be recreated"
    )]
    Incomplete,
    #[error("MCP operation-store initialization belongs to a different authenticated context")]
    Context,
    #[error(transparent)]
    Store(#[from] StoreError),
}

// This short bootstrap lock is separate from the running node's physical-owner
// lock. It is released before handing the store to the MCP blocking worker.
// Existing bootstrap evidence always chooses open-only. A crash before complete
// first initialization fails closed rather than creating a second ID namespace.
struct Bootstrap {
    root: PathBuf,
    _lock: File,
    first: bool,
    #[cfg(test)]
    fault: std::cell::Cell<Option<Fault>>,
}
impl Bootstrap {
    fn open(root: &Path) -> std::result::Result<Self, BootstrapError> {
        use std::os::unix::fs::MetadataExt;
        let metadata = fs::symlink_metadata(root)?;
        if !metadata.is_dir() || metadata.mode() & 0o077 != 0 {
            return Err(BootstrapError::Permissions);
        }
        let lock_path = root.join(LOCK);
        let first = !exists(&lock_path)?;
        if first {
            if exists(&root.join(MARKER))? || exists(&root.join(STORE))? {
                return Err(BootstrapError::Incomplete);
            }
        } else {
            check_file(&lock_path, metadata.uid())?;
        }
        let lock = options()
            .read(true)
            .write(true)
            .create_new(first)
            .open(&lock_path)
            .map_err(|error| {
                if error.kind() == std::io::ErrorKind::AlreadyExists {
                    BootstrapError::Locked
                } else {
                    error.into()
                }
            })?;
        check_open_file(&lock_path, &lock, metadata.uid())?;
        lock.try_lock_exclusive().map_err(|error| {
            if error.kind() == std::io::ErrorKind::WouldBlock {
                BootstrapError::Locked
            } else {
                error.into()
            }
        })?;
        if first {
            lock.sync_all()?;
            File::open(root)?.sync_all()?;
        }
        Ok(Self {
            root: root.into(),
            _lock: lock,
            first,
            #[cfg(test)]
            fault: std::cell::Cell::new(None),
        })
    }
    fn finish(
        &self,
        context: &OperationContext,
    ) -> std::result::Result<OperationStore, BootstrapError> {
        if context.cluster == [0; 16]
            || context.principal.is_zero()
            || context.ledger.tenant.is_zero()
            || context.ledger.session.is_zero()
        {
            return Err(BootstrapError::Context);
        }
        if self.first {
            self.write_marker(context)?;
            #[cfg(test)]
            self.fail_at(Fault::Marker)?;
            let store = OperationStore::create(self.root.join(STORE), StoreLimits::default())?;
            #[cfg(test)]
            self.fail_at(Fault::Store)?;
            Ok(store)
        } else {
            self.read_marker(context)?;
            OperationStore::open(self.root.join(STORE), StoreLimits::default()).map_err(Into::into)
        }
    }
    fn write_marker(&self, context: &OperationContext) -> std::result::Result<(), BootstrapError> {
        let mut file = options()
            .write(true)
            .create_new(true)
            .open(self.root.join(MARKER))?;
        file.write_all(MAGIC)?;
        for field in fields(context) {
            file.write_all(field)?;
        }
        file.write_all(digest(context).as_bytes())?;
        file.sync_all()?;
        File::open(&self.root)?.sync_all()?;
        Ok(())
    }
    fn read_marker(&self, context: &OperationContext) -> std::result::Result<(), BootstrapError> {
        use std::os::unix::fs::MetadataExt;
        let path = self.root.join(MARKER);
        let owner = fs::metadata(&self.root)?.uid();
        check_file(&path, owner)?;
        let mut file = File::open(&path)?;
        check_open_file(&path, &file, owner)?;
        if file.metadata()?.len() != MARKER_BYTES {
            return Err(BootstrapError::Incomplete);
        }
        let mut magic = [0; 8];
        file.read_exact(&mut magic)?;
        if magic != *MAGIC {
            return Err(BootstrapError::Incomplete);
        }
        let mut hash = blake3::Hasher::new();
        hash.update(&magic);
        let mut mismatch = false;
        for expected in fields(context) {
            let mut actual = [0; 16];
            file.read_exact(&mut actual)?;
            hash.update(&actual);
            mismatch |= actual != *expected;
        }
        let mut checksum = [0; 32];
        file.read_exact(&mut checksum)?;
        if hash.finalize().as_bytes() != &checksum {
            return Err(BootstrapError::Incomplete);
        }
        if mismatch {
            return Err(BootstrapError::Context);
        }
        file.sync_all()?;
        File::open(&self.root)?.sync_all()?;
        Ok(())
    }
    #[cfg(test)]
    fn fail_at(&self, point: Fault) -> std::result::Result<(), BootstrapError> {
        if self.fault.get() == Some(point) {
            self.fault.set(None);
            Err(std::io::Error::other("injected MCP store bootstrap failure").into())
        } else {
            Ok(())
        }
    }
}
fn fields(context: &OperationContext) -> [&[u8; 16]; 4] {
    [
        &context.cluster,
        context.principal.as_bytes(),
        context.ledger.tenant.as_bytes(),
        context.ledger.session.as_bytes(),
    ]
}
fn digest(context: &OperationContext) -> blake3::Hash {
    let mut hash = blake3::Hasher::new();
    hash.update(MAGIC);
    for field in fields(context) {
        hash.update(field);
    }
    hash.finalize()
}
fn options() -> OpenOptions {
    use std::os::unix::fs::OpenOptionsExt;
    let mut options = OpenOptions::new();
    options.mode(0o600);
    options
}
fn exists(path: &Path) -> std::result::Result<bool, BootstrapError> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}
fn check_file(path: &Path, owner: u32) -> std::result::Result<(), BootstrapError> {
    use std::os::unix::fs::MetadataExt;
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            BootstrapError::Incomplete
        } else {
            error.into()
        }
    })?;
    if !metadata.is_file()
        || metadata.mode() & 0o077 != 0
        || metadata.uid() != owner
        || metadata.nlink() != 1
    {
        return Err(BootstrapError::Permissions);
    }
    Ok(())
}
fn check_open_file(
    path: &Path,
    file: &File,
    owner: u32,
) -> std::result::Result<(), BootstrapError> {
    use std::os::unix::fs::MetadataExt;
    check_file(path, owner)?;
    let path_metadata = fs::symlink_metadata(path)?;
    let metadata = file.metadata()?;
    if metadata.dev() != path_metadata.dev()
        || metadata.ino() != path_metadata.ino()
        || !metadata.is_file()
        || metadata.mode() & 0o077 != 0
        || metadata.uid() != owner
        || metadata.nlink() != 1
    {
        return Err(BootstrapError::Permissions);
    }
    Ok(())
}

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum Fault {
    Marker,
    Store,
}

#[cfg(test)]
mod tests {
    use super::*;
    use focal_model::{LedgerId, ParticipantId, SessionId, TenantId};
    use std::os::unix::fs::{PermissionsExt, symlink};
    fn private_root() -> tempfile::TempDir {
        let temp = tempfile::tempdir().unwrap();
        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
        temp
    }
    fn context() -> OperationContext {
        OperationContext {
            cluster: [1; 16],
            principal: ParticipantId::from_u128(2),
            ledger: LedgerId {
                tenant: TenantId::from_u128(3),
                session: SessionId::from_u128(4),
            },
        }
    }
    fn open(root: &Path) -> std::result::Result<OperationStore, BootstrapError> {
        Bootstrap::open(root)?.finish(&context())
    }
    #[test]
    fn first_use_is_automatic_and_ordinary_reopen_preserves_context_and_catalogue() {
        let temp = private_root();
        let store = open(temp.path()).unwrap();
        assert_eq!(store.root(), temp.path().join(STORE));
        assert_eq!(store.usage().unwrap().operations, 0);
        assert_eq!(
            fs::metadata(temp.path().join(MARKER))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        let original = fs::read(temp.path().join(STORE).join("catalogue.bin")).unwrap();
        let boot = Bootstrap::open(temp.path()).unwrap();
        let mut other = context();
        other.principal = ParticipantId::from_u128(8);
        assert!(matches!(boot.finish(&other), Err(BootstrapError::Context)));
        drop(boot);
        drop(open(temp.path()).unwrap());
        assert_eq!(
            fs::read(temp.path().join(STORE).join("catalogue.bin")).unwrap(),
            original
        );
    }
    #[test]
    fn interrupted_bootstrap_never_reinitializes_an_ambiguous_id_namespace() {
        for point in [None, Some(Fault::Marker), Some(Fault::Store)] {
            let temp = private_root();
            let bootstrap = Bootstrap::open(temp.path()).unwrap();
            bootstrap.fault.set(point);
            if point.is_some() {
                assert!(bootstrap.finish(&context()).is_err());
            }
            drop(bootstrap);
            if point == Some(Fault::Store) {
                assert!(open(temp.path()).is_ok());
            } else {
                assert!(open(temp.path()).is_err());
                assert!(!temp.path().join(STORE).exists());
            }
        }
    }
    #[test]
    fn lost_store_marker_or_lock_fails_closed_without_recreating_any_file() {
        for remove in [STORE, MARKER, LOCK] {
            let temp = private_root();
            drop(open(temp.path()).unwrap());
            let path = temp.path().join(remove);
            if remove == STORE {
                fs::remove_dir_all(&path).unwrap();
            } else {
                fs::remove_file(&path).unwrap();
            }
            assert!(open(temp.path()).is_err());
            assert!(!path.exists());
        }
    }
    #[test]
    fn bootstrap_lock_permissions_symlinks_and_corrupt_context_marker_are_rejected() {
        let temp = private_root();
        let bootstrap = Bootstrap::open(temp.path()).unwrap();
        assert!(matches!(
            Bootstrap::open(temp.path()),
            Err(BootstrapError::Locked)
        ));
        drop(bootstrap.finish(&context()).unwrap());
        drop(bootstrap);
        let marker = temp.path().join(MARKER);
        let original = fs::read(&marker).unwrap();
        let mut corrupt = original.clone();
        corrupt[8] ^= 1;
        fs::write(&marker, corrupt).unwrap();
        assert!(matches!(open(temp.path()), Err(BootstrapError::Incomplete)));
        fs::write(&marker, &original).unwrap();
        fs::set_permissions(&marker, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(matches!(
            open(temp.path()),
            Err(BootstrapError::Permissions)
        ));
        fs::remove_file(&marker).unwrap();
        symlink(temp.path().join("missing"), &marker).unwrap();
        assert!(matches!(
            open(temp.path()),
            Err(BootstrapError::Permissions)
        ));
        fs::remove_file(&marker).unwrap();
        let mut file = options()
            .write(true)
            .create_new(true)
            .open(&marker)
            .unwrap();
        file.write_all(&original).unwrap();
        drop(file);
        fs::hard_link(&marker, temp.path().join("linked")).unwrap();
        assert!(matches!(
            open(temp.path()),
            Err(BootstrapError::Permissions)
        ));
    }
    #[test]
    fn entered_async_runtime_is_rejected_before_any_context_or_bootstrap_io() {
        let temp = private_root();
        let mut settings = Settings::default();
        settings.node.data_dir = Some(temp.path().into());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        assert!(runtime.block_on(async { serve(&settings, None) }).is_err());
        assert_eq!(fs::read_dir(temp.path()).unwrap().count(), 0);
    }
}
