use super::{CliError, Context, Result, Settings};
use focal_client::{
    managed_requests::ManagedRequests,
    managed_store::ManagedStoreLimits,
    operation_store::{OperationStore, StoreError, StoreLimits},
    pending::OperationContext,
};
use std::{
    fs::{self, File},
    io::{Read, Write},
    path::{Path, PathBuf},
};

const STORE: &str = "MCP.operations";
const LOCK: &str = "MCP.operations.lock";
const MARKER: &str = "MCP.operations.initialized";
const MAGIC: &[u8; 8] = b"FCLMCP01";
const MARKER_BYTES: u64 = 104;

pub(crate) fn serve(settings: &Settings, selection: Option<&str>) -> Result<()> {
    let backend = backend(settings, selection)?;
    focal_mcp::serve(backend, std::io::stdin(), std::io::stdout())
        .map_err(|error| CliError::Other(Box::new(error)))
}

/// Code mode from the CLI (19 §Code mode): one program, run against the same
/// backend and journal `serve mcp` offers, so a run begun over MCP resumes
/// here with the same `--run`, and the reverse.
#[derive(clap::Subcommand)]
pub(crate) enum CodeCommand {
    /// Run a JavaScript program that calls focal's tools; print what it returns.
    Run {
        /// Your identity for the program; reuse it only to retry the same program.
        #[arg(long)]
        run: String,
        /// The program: the body of an async function.
        #[arg(long)]
        file: PathBuf,
        /// A JSON object the program reads as `input`.
        #[arg(long)]
        input: Option<PathBuf>,
        /// What `Date.now()` returns; give the same value on a retry.
        #[arg(long)]
        now_ms: Option<u64>,
    },
    /// Run a program over the tools you may call (`registry`); print what it selects.
    Search {
        #[arg(long)]
        file: PathBuf,
    },
}

pub(crate) fn code(
    settings: &Settings,
    selection: Option<&str>,
    command: CodeCommand,
) -> Result<()> {
    let mut arguments = serde_json::Map::new();
    let (tool, file) = match command {
        CodeCommand::Run {
            run,
            file,
            input,
            now_ms,
        } => {
            arguments.insert("run".into(), run.into());
            if let Some(input) = input {
                let text = bounded_text(&input, focal_client::input::MAX_INPUT_BYTES)?;
                let value: serde_json::Value = serde_json::from_str(&text)
                    .map_err(|error| CliError::Input(format!("--input: {error}")))?;
                arguments.insert("input".into(), value);
            }
            if let Some(now_ms) = now_ms {
                arguments.insert("now_ms".into(), now_ms.into());
            }
            ("code.run", file)
        }
        CodeCommand::Search { file } => ("code.search", file),
    };
    // The adapter refuses a program longer than its frame bound; reading
    // stops one byte past it.
    arguments.insert("program".into(), bounded_text(&file, 278_528)?.into());
    let backend = backend(settings, selection)?;
    let result = focal_mcp::run_code(backend, tool, arguments)
        .map_err(|error| CliError::Other(Box::new(error)))?;
    let mut stdout = std::io::stdout().lock();
    serde_json::to_writer_pretty(&mut stdout, &result)
        .map_err(|error| CliError::Other(Box::new(error)))?;
    stdout.write_all(b"\n")?;
    match &result.result {
        focal_client::operations::OperationOutput::Code {
            outcome: focal_client::operations::CodeOutcome::Failed { code, detail },
            ..
        } => Err(CliError::Program(format!("{code}: {detail}"))),
        _ => Ok(()),
    }
}

fn bounded_text(path: &Path, limit: usize) -> Result<String> {
    let mut bytes = Vec::new();
    File::open(path)?
        .take(u64::try_from(limit).map_or(u64::MAX, |n| n.saturating_add(1)))
        .read_to_end(&mut bytes)?;
    if bytes.len() > limit {
        return Err(CliError::Input(format!(
            "{} is longer than {limit} bytes",
            path.display()
        )));
    }
    String::from_utf8(bytes)
        .map_err(|_| CliError::Input(format!("{} is not UTF-8", path.display())))
}

fn backend(
    settings: &Settings,
    selection: Option<&str>,
) -> Result<focal_mcp::Backend<impl focal_client::ClientTransport + 'static>> {
    if hyper_rt::futures::shard_id().is_some() {
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
        at_rest,
    } = Context::open(settings, selection)?;
    let store = Bootstrap::open(&root)
        .and_then(|bootstrap| bootstrap.finish(&operation, at_rest.clone()))
        .map_err(|error| CliError::Other(Box::new(error)))?;
    let managed = ManagedRequests::open_with(
        &root,
        "MCP.requests",
        operation,
        ManagedStoreLimits::default(),
        super::managed::rotation()?,
        at_rest.clone(),
    )
    .map_err(|error| CliError::Other(Box::new(error)))?;
    let mut backend =
        focal_mcp::Backend::new(client, build, operation, store)?.with_managed_requests(managed)?;
    let uploads = focal_client::artifact_transfer::UploadStore::bootstrap(
        &root,
        "MCP.uploads",
        operation,
        focal_client::artifact_transfer::UploadStoreLimits::default(),
        at_rest.clone(),
    )
    .map_err(|error| CliError::Other(Box::new(error)))?;
    backend = backend.with_uploads(uploads);
    let watches = focal_client::watch::WatchStore::open(&root, operation, at_rest.clone())
        .map_err(|error| CliError::Other(Box::new(error)))?;
    backend = backend.with_watches(watches)?;
    // The engine probe runs inside the adapter on its worker runtime; this
    // adapter's own native journal is opened only when the ledger is native.
    backend = backend.with_native_journal(Box::new(NativeJournal {
        parent: root.join("client"),
        rest: at_rest,
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
    Ok(backend)
}

/// The MCP adapter's `n1:` journal, beside the human CLI's under the context
/// root; each adapter owns its identities and delivery marks.
struct NativeJournal {
    parent: PathBuf,
    /// How its records are kept at rest (29 §12.11).
    rest: focal_client::at_rest::AtRest,
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
        match super::native::store_in(&self.parent, super::native::MCP_STORE, true, self.rest) {
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
    #[error("MCP operation-store marker at rest: {0}")]
    AtRest(#[from] focal_client::at_rest::AtRestError),
}

// This short bootstrap lock is separate from the running node's physical-owner
// lock. It is released before handing the store to the MCP blocking worker.
// Existing bootstrap evidence always chooses open-only. A crash before complete
// first initialization fails closed rather than creating a second ID namespace.
struct Bootstrap {
    root: PathBuf,
    _lock: focal_platform::FileLock,
    first: bool,
    #[cfg(test)]
    fault: std::cell::Cell<Option<Fault>>,
}
impl Bootstrap {
    fn open(root: &Path) -> std::result::Result<Self, BootstrapError> {
        let owner =
            focal_platform::fs::private_dir_owner(root)?.ok_or(BootstrapError::Permissions)?;
        let lock_path = root.join(LOCK);
        let first = !exists(&lock_path)?;
        if first {
            if exists(&root.join(MARKER))? || exists(&root.join(STORE))? {
                return Err(BootstrapError::Incomplete);
            }
        } else {
            check_file(&lock_path, &owner)?;
        }
        let lock =
            focal_platform::fs::open_private(&lock_path, true, true, first).map_err(|error| {
                if error.kind() == std::io::ErrorKind::AlreadyExists {
                    BootstrapError::Locked
                } else {
                    error.into()
                }
            })?;
        check_open_file(&lock_path, &lock, &owner)?;
        let lock = focal_platform::FileLock::exclusive(lock).map_err(|error| {
            if error.kind() == std::io::ErrorKind::WouldBlock {
                BootstrapError::Locked
            } else {
                error.into()
            }
        })?;
        if first {
            lock.file().sync_all()?;
            #[cfg(unix)]
            {
                File::open(root)?.sync_all()?;
            }
        }
        Ok(Self {
            root: root.into(),
            _lock: lock,
            first,
            #[cfg(test)]
            fault: std::cell::Cell::new(None),
        })
    }
    /// The operation store, created on first use, its records kept as `rest` says (29 §12.11).
    fn finish(
        &self,
        context: &OperationContext,
        rest: focal_client::at_rest::AtRest,
    ) -> std::result::Result<OperationStore, BootstrapError> {
        if context.cluster == [0; 16]
            || context.principal.is_zero()
            || context.ledger.tenant.is_zero()
            || context.ledger.session.is_zero()
        {
            return Err(BootstrapError::Context);
        }
        // The marker names its context, so it is kept as the store's records are (29 §12.11).
        let marker = focal_client::at_rest::OwnerFile::new(&self.root, MARKER, rest.clone())?;
        if self.first {
            if marker.sealing() {
                marker.mark()?;
            }
            self.write_marker(context, &marker)?;
            #[cfg(test)]
            self.fail_at(Fault::Marker)?;
            let store =
                OperationStore::create(self.root.join(STORE), StoreLimits::default(), rest)?;
            #[cfg(test)]
            self.fail_at(Fault::Store)?;
            Ok(store)
        } else {
            self.read_marker(context, &marker)?;
            OperationStore::open(self.root.join(STORE), StoreLimits::default(), rest)
                .map_err(Into::into)
        }
    }
    fn write_marker(
        &self,
        context: &OperationContext,
        marker: &focal_client::at_rest::OwnerFile,
    ) -> std::result::Result<(), BootstrapError> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        for field in fields(context) {
            bytes.extend_from_slice(field);
        }
        bytes.extend_from_slice(digest(context).as_bytes());
        let bytes = marker.to_disk(bytes)?;
        let mut file =
            focal_platform::fs::open_private(&self.root.join(MARKER), false, true, true)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        #[cfg(unix)]
        {
            File::open(&self.root)?.sync_all()?;
        }
        Ok(())
    }
    fn read_marker(
        &self,
        context: &OperationContext,
        marker: &focal_client::at_rest::OwnerFile,
    ) -> std::result::Result<(), BootstrapError> {
        let path = self.root.join(MARKER);
        let owner = focal_platform::fs::owner_at(&self.root)?;
        check_file(&path, &owner)?;
        let mut file = File::open(&path)?;
        check_open_file(&path, &file, &owner)?;
        let most = usize::try_from(MARKER_BYTES).map_err(|_| BootstrapError::Incomplete)?;
        let bound = u64::try_from(marker.bound(most)?).map_err(|_| BootstrapError::Incomplete)?;
        if file.metadata()?.len() > bound {
            return Err(BootstrapError::Incomplete);
        }
        let mut on_disk = Vec::new();
        (&mut file).take(bound).read_to_end(&mut on_disk)?;
        // The marker was written and synced by its installer before it could be
        // read back; a read handle needs no re-sync (FlushFileBuffers rejects a
        // read-only handle on Windows). The directory entry is fenced below.
        drop(file);
        let bytes = marker.opened(on_disk.clone(), most)?;
        if bytes.len() != most || bytes.get(..8) != Some(MAGIC.as_slice()) {
            return Err(BootstrapError::Incomplete);
        }
        let mut hash = blake3::Hasher::new();
        hash.update(MAGIC);
        let mut mismatch = false;
        let saved = bytes.get(8..72).ok_or(BootstrapError::Incomplete)?;
        for (actual, expected) in saved.as_chunks::<16>().0.iter().zip(fields(context)) {
            hash.update(actual);
            mismatch |= actual != expected;
        }
        if Some(hash.finalize().as_bytes().as_slice()) != bytes.get(72..) {
            return Err(BootstrapError::Incomplete);
        }
        if mismatch {
            return Err(BootstrapError::Context);
        }
        marker.convert(&on_disk, &bytes)?;
        #[cfg(unix)]
        {
            File::open(&self.root)?.sync_all()?;
        }
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
fn exists(path: &Path) -> std::result::Result<bool, BootstrapError> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}
fn check_file(
    path: &Path,
    owner: &focal_platform::fs::Owner,
) -> std::result::Result<(), BootstrapError> {
    match focal_platform::fs::check_private_file(path, owner, 1) {
        Ok(true) => Ok(()),
        Ok(false) => Err(BootstrapError::Permissions),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Err(BootstrapError::Incomplete)
        }
        Err(error) => Err(error.into()),
    }
}
fn check_open_file(
    path: &Path,
    file: &File,
    owner: &focal_platform::fs::Owner,
) -> std::result::Result<(), BootstrapError> {
    match focal_platform::fs::check_open_private_file(path, file, owner) {
        Ok(true) => Ok(()),
        Ok(false) => Err(BootstrapError::Permissions),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Err(BootstrapError::Incomplete)
        }
        Err(error) => Err(error.into()),
    }
}

#[cfg(all(test, unix))]
fn options() -> std::fs::OpenOptions {
    use std::os::unix::fs::OpenOptionsExt;
    let mut options = std::fs::OpenOptions::new();
    options.mode(0o600);
    options
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
    fn private_root() -> tempfile::TempDir {
        let temp = tempfile::tempdir().unwrap();
        // Unix tightens the temp dir to 0700; a fresh Windows temp dir already
        // inherits an owner-only DACL from the temp root.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
        }
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
        Bootstrap::open(root)?.finish(&context(), focal_client::at_rest::AtRest::clear())
    }
    /// The marker names its store's context, so it is kept as the store's records are (29 §12.11):
    /// written as before below the at-rest level, converted with its store once the fence opens
    /// the level, its context still binding the store; one planted in the clear after the mark is
    /// refused, and put back it opens.
    #[test]
    fn the_marker_converts_with_its_store_and_still_binds_its_context() {
        use focal_client::at_rest::{AtRest, AtRestError, SEALED_MAGIC};
        let data = private_root();
        let keys = focal_seal::test_keys(data.path()).unwrap();
        let root = data.path().join("context");
        focal_platform::fs::create_dir_private(&root).unwrap();
        let below = AtRest::new(Some(keys.clone()), false);
        let sealing = AtRest::new(Some(keys), true);
        drop(
            Bootstrap::open(&root)
                .unwrap()
                .finish(&context(), below)
                .unwrap(),
        );
        let clear = fs::read(root.join(MARKER)).unwrap();
        assert!(clear.starts_with(MAGIC), "below the level, as before");
        let store = Bootstrap::open(&root)
            .unwrap()
            .finish(&context(), sealing.clone())
            .unwrap();
        drop(store);
        let sealed = fs::read(root.join(MARKER)).unwrap();
        assert!(sealed.starts_with(SEALED_MAGIC));
        assert!(!sealed.windows(16).any(|window| window == context().cluster));
        assert!(
            fs::read(root.join(STORE).join("catalogue.bin"))
                .unwrap()
                .starts_with(SEALED_MAGIC)
        );
        let mut other = context();
        other.principal = ParticipantId::from_u128(8);
        assert!(matches!(
            Bootstrap::open(&root)
                .unwrap()
                .finish(&other, sealing.clone()),
            Err(BootstrapError::Context)
        ));
        fs::write(root.join(MARKER), &clear).unwrap();
        assert!(matches!(
            Bootstrap::open(&root)
                .unwrap()
                .finish(&context(), sealing.clone()),
            Err(BootstrapError::AtRest(AtRestError::ClearAfterSealed(_)))
        ));
        fs::write(root.join(MARKER), &sealed).unwrap();
        assert!(
            Bootstrap::open(&root)
                .unwrap()
                .finish(&context(), sealing)
                .is_ok()
        );
    }
    #[test]
    fn first_use_is_automatic_and_ordinary_reopen_preserves_context_and_catalogue() {
        let temp = private_root();
        let store = open(temp.path()).unwrap();
        assert_eq!(store.root(), temp.path().join(STORE));
        assert_eq!(store.usage().unwrap().operations, 0);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(temp.path().join(MARKER))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
        let original = fs::read(temp.path().join(STORE).join("catalogue.bin")).unwrap();
        let boot = Bootstrap::open(temp.path()).unwrap();
        let mut other = context();
        other.principal = ParticipantId::from_u128(8);
        assert!(matches!(
            boot.finish(&other, focal_client::at_rest::AtRest::clear()),
            Err(BootstrapError::Context)
        ));
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
                assert!(
                    bootstrap
                        .finish(&context(), focal_client::at_rest::AtRest::clear())
                        .is_err()
                );
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
        drop(
            bootstrap
                .finish(&context(), focal_client::at_rest::AtRest::clear())
                .unwrap(),
        );
        drop(bootstrap);
        let marker = temp.path().join(MARKER);
        let original = fs::read(&marker).unwrap();
        let mut corrupt = original.clone();
        corrupt[8] ^= 1;
        fs::write(&marker, corrupt).unwrap();
        assert!(matches!(open(temp.path()), Err(BootstrapError::Incomplete)));
        fs::write(&marker, &original).unwrap();
        // POSIX permission/symlink/hardlink rejection: a group/other-readable
        // mode, a reparse-point marker and a multiply-linked marker are all
        // refused. Windows enforces the private-file contract through DACLs,
        // reparse-point refusal and the file link count instead (covered by the
        // focal-platform FFI suite), so this shape is Unix-only.
        #[cfg(unix)]
        {
            use std::os::unix::fs::{PermissionsExt, symlink};
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
    }
    #[test]
    fn a_serve_on_a_shard_is_rejected_before_any_context_or_bootstrap_io() {
        let temp = private_root();
        let mut settings = Settings::default();
        settings.node.data_dir = Some(temp.path().into());
        // The adapter owns its own worker shard: one asked for from inside
        // another's root is refused before it opens anything.
        let refused = focal_rt::block_on(async move { serve(&settings, None).is_err() }).unwrap();
        assert!(refused);
        assert_eq!(fs::read_dir(temp.path()).unwrap().count(), 0);
    }
}
