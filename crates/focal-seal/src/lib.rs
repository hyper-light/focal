#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::disallowed_macros
    )
)]
//! A node's keys at rest (docs/archictecutre/29-sealing-at-rest.md §2–§3): the root key in a file
//! outside the data directory, and in the data directory `SEAL.node`, which holds the node key
//! wrapped by the root and each store's key wrapped by the node key.
//!
//! - **The root key** is 32 random bytes in an owner-only file (`hyper_seal::FileSource`), made
//!   once at a node's first start and never made again for a data directory that has keys: a
//!   missing or wrong key file is refused, typed, never answered by a fresh key.
//! - **A key file inside the data directory is refused**: a copy of the data directory alone must
//!   never carry what opens it.
//! - **`SEAL.node`** holds wrapped keys only (AES-256-KW records, RFC 3394), so it reveals nothing
//!   without the root. It ends in a BLAKE3 hash of what precedes it, which tells a damaged file
//!   from a wrong key: key wrap's own integrity check refuses the wrong key.
//! - **Every key is random**, never derived (docs/seal.md §3.1), so one store's key reveals nothing
//!   of another's and each is rotated alone.
use std::ffi::OsStr;
use std::path::{Path, PathBuf};

use focal_platform::fs::{FileMedium, Medium as _, create_private_new, ensure_dir_private};
use hyper_seal::keys::{KeyId, KeySource as _, RECORD, Wrapped, WrappingKey};
use hyper_seal::{FileSource, SealError, Secret32};

pub mod file;

/// The file in the data directory that holds the node's wrapped keys.
pub const SEAL_FILE: &str = "SEAL.node";

/// The file in a data directory whose owner names no key file that names the root key its keys
/// are made under, recorded before that key file is made ([`resolve`]): every maker of the
/// directory's keys, processes at once or a start after one that failed or crashed, makes and
/// opens the one key file it names, never a key an attempt made that nothing names. It stays
/// beside `SEAL.node`, which names the same root.
pub const INTENT_FILE: &str = "SEAL.root";

/// Where a data directory's keys come from (§2–§3): the directory, whose `SEAL.node` holds them,
/// and the root key file that opens it. A node's or a client's: each data directory has keys of its
/// own, under the root key file its owner names. It holds no key, only where they are: a writer
/// unwraps the one store key it seals a file with, for that file ([`DataKeys::key`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DataKeys {
    pub data_dir: PathBuf,
    pub key_file: PathBuf,
}

impl DataKeys {
    pub fn new(data_dir: impl Into<PathBuf>, key_file: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            key_file: key_file.into(),
        }
    }

    /// The directory's keys made where it has none, the root key file with them where it is
    /// missing, or opened where it has them: what a node's or a client's start does before any
    /// sealed store is made, with every refusal of §2.
    pub fn ensure(&self) -> Result<(), SealSetupError> {
        open_or_create(&self.data_dir, &self.key_file).map(drop)
    }

    /// The key of `store`, unwrapped for one file and dropped after it ([`store_key`]).
    pub fn key(&self, store: Store) -> Result<WrappingKey, SealSetupError> {
        store_key(&self.data_dir, &self.key_file, store)
    }
}

/// The keys of the data directory `root` in a dependent's tests, found or made as a node's or a
/// client's start finds or makes them ([`resolve`], no key file configured), so a test and the
/// code it runs, in its process or a binary it spawns, find the same keys: under a build with
/// test support the per-user directory is the system's temporary one ([`keys_dir`]) and the root
/// key is the one every test's directory shares ([`TEST_ROOT`]). Tests in parallel may open one
/// data directory at once: its keys are published once, never one maker's over another's.
#[cfg(any(test, feature = "test-support"))]
pub fn test_keys(root: impl AsRef<Path>) -> Result<DataKeys, SealSetupError> {
    resolve(root.as_ref(), None, None)
}

/// [`test_keys`] of the data directory a private directory or a store at `path` lies in: its
/// parent, as a dependent's test lays its stores out under its own temporary directory.
#[cfg(any(test, feature = "test-support"))]
pub fn test_keys_beside(path: impl AsRef<Path>) -> Result<DataKeys, SealSetupError> {
    let path = path.as_ref();
    test_keys(path.parent().unwrap_or(path))
}
/// The format `SEAL.node` is written in.
const MAGIC: &[u8; 8] = b"FCLSEAL1";
/// Bytes of a root key (AES-256).
const ROOT_BYTES: usize = 32;
/// Bytes of one key's entry: its ID, its generation and its record under its parent.
const ENTRY: usize = 16 + 4 + RECORD;
/// Keys below the root: the node key and its five children (§3).
const KEYS: usize = 6;
/// Bytes of the BLAKE3 hash that ends the file.
const HASH: usize = 32;
/// The format `SEAL.root` is written in.
const INTENT_MAGIC: &[u8; 8] = b"FCLROOT1";
/// Bytes of `SEAL.root`: the magic, the root key's ID, the hash.
const INTENT_BYTES: usize = INTENT_MAGIC.len() + 16 + HASH;
/// Bytes of `SEAL.node`: the magic, the root's ID and generation, every entry, the hash.
const SEAL_BYTES: usize = MAGIC.len() + 16 + 4 + KEYS * ENTRY + HASH;

/// Key slots a node asks the process's locked region for (docs/seal.md §8): one page of them,
/// the least the OS locks. A node holds its six keys for its life and unwraps one more at a time
/// per thread that seals (the log's writer, the content store, the group files, the journals), so
/// a page's 128 slots at 4 KiB pages hold them several times over.
pub fn key_slots() -> usize {
    focal_platform::memory_page_bytes()
        .unwrap_or(4096)
        .checked_div(32)
        .unwrap_or(128)
}

/// Why a node's keys were not opened or made.
#[derive(Debug, thiserror::Error)]
pub enum SealSetupError {
    #[error("the root key file {0} is inside the data directory; it must live off the data volume")]
    KeyInDataDir(PathBuf),
    #[error(
        "the data directory holds sealed keys but the root key file {0} is missing: restore it, never make a new one"
    )]
    MissingKey(PathBuf),
    #[error("the root key file {0} does not open this data directory's keys")]
    WrongKey(PathBuf),
    #[error("the data directory {0} holds no sealed keys: the node's storage start makes them")]
    NoKeys(PathBuf),
    #[error("{SEAL_FILE} is damaged: {0}")]
    Damaged(&'static str),
    #[error("{INTENT_FILE} is damaged: {0}")]
    IntentDamaged(&'static str),
    #[error("the root key file {path}: {source}")]
    KeyFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("{SEAL_FILE}: {0}")]
    Io(#[source] std::io::Error),
    #[error(transparent)]
    Seal(#[from] SealError),
    #[error("the operating system's random source failed")]
    Random,
    #[error("the process's cache key is unavailable: a holder of it failed while holding it")]
    CacheKey,
    /// Another process made the data directory's keys first: what it made is opened instead.
    #[error("{SEAL_FILE} was made by another process first")]
    Raced,
}

/// The keys a node's stores seal under, unwrapped for the node's life.
pub struct NodeKeys {
    /// The parent every hyper-log writer session's key is wrapped under.
    pub log_parent: WrappingKey,
    /// The key the node log's frames and headers are MACed under.
    pub log_auth: Secret32,
    /// The parent of every group file's data key.
    pub group: WrappingKey,
    /// The parent of every content object's data key.
    pub content: WrappingKey,
    /// The parent of every journal's and small file's data key.
    pub journal: WrappingKey,
    /// The root key's ID and generation, which a backup names.
    pub root: (KeyId, u32),
}

/// The keys a node keeps once its log has taken its own: those of the stores sealed beside it.
pub struct StoreKeys {
    pub group: WrappingKey,
    pub content: WrappingKey,
    pub journal: WrappingKey,
    pub root: (KeyId, u32),
}

impl std::fmt::Debug for StoreKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreKeys")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl NodeKeys {
    /// The log's two keys (its sessions' parent and its MAC key), and the rest.
    pub fn split(self) -> ((WrappingKey, Secret32), StoreKeys) {
        (
            (self.log_parent, self.log_auth),
            StoreKeys {
                group: self.group,
                content: self.content,
                journal: self.journal,
                root: self.root,
            },
        )
    }
}

impl std::fmt::Debug for NodeKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeKeys")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

/// The root key file a node keeps by default when its configuration names none: the platform's
/// per-user configuration directory, never its data directory (§2).
pub fn default_key_file(node: &str) -> Option<PathBuf> {
    Some(keys_dir()?.join(format!("{node}.key")))
}

/// The platform's per-user directory of root key files (§2), never a data directory. Under a
/// build with test support, one under the system's temporary directory instead: no test makes a
/// key file where the user keeps theirs, and every process of a test run (its own and the
/// binaries it spawns) finds the same files.
pub fn keys_dir() -> Option<PathBuf> {
    #[cfg(any(test, feature = "test-support"))]
    {
        Some(std::env::temp_dir().join("focal-test-keys"))
    }
    #[cfg(not(any(test, feature = "test-support")))]
    {
        user_keys_dir()
    }
}

/// The platform's per-user configuration directory of root key files.
pub fn user_keys_dir() -> Option<PathBuf> {
    let home = std::env::var_os("HOME").map(PathBuf::from);
    Some(if cfg!(target_os = "macos") {
        home?.join("Library/Application Support/Focal Keys")
    } else if cfg!(windows) {
        PathBuf::from(std::env::var_os("APPDATA")?)
            .join("Focal")
            .join("keys")
    } else if let Some(config) = std::env::var_os("XDG_CONFIG_HOME") {
        PathBuf::from(config).join("focal").join("keys")
    } else {
        home?.join(".config").join("focal").join("keys")
    })
}

/// The root key file named for the root key `id` in the per-user directory (§2): what a data
/// directory whose owner names none keeps its keys under.
pub fn key_file_for(id: KeyId) -> Option<PathBuf> {
    Some(key_file_in(&keys_dir()?, id))
}

/// The root key file named for the root key `id` in the directory of key files `keys`.
fn key_file_in(keys: &Path, id: KeyId) -> PathBuf {
    let mut name = String::with_capacity(id.0.len().saturating_mul(2).saturating_add(4));
    for byte in id.0 {
        name.push_str(&format!("{byte:02x}"));
    }
    name.push_str(".key");
    keys.join(name)
}

/// The root key `SEAL.node` in `data_dir` names, none where the directory has no keys yet: what
/// finds the key file of a directory whose owner names none ([`resolve`]). The file is checked
/// whole, as an open checks it, before its ID is taken.
pub fn recorded_root(data_dir: &Path) -> Result<Option<KeyId>, SealSetupError> {
    let seal = data_dir.join(SEAL_FILE);
    let medium = FileMedium;
    if !medium.exists(&seal).map_err(SealSetupError::Io)? {
        return Ok(None);
    }
    let bytes = medium.read(&seal, SEAL_BYTES).map_err(SealSetupError::Io)?;
    if bytes.len() != SEAL_BYTES {
        return Err(SealSetupError::Damaged("a file of the wrong length"));
    }
    let (body, stated) = bytes.split_at(SEAL_BYTES.saturating_sub(HASH));
    if blake3::hash(body).as_bytes() != stated {
        return Err(SealSetupError::Damaged("its hash does not match its bytes"));
    }
    let mut at = 0usize;
    if take::<8>(body, &mut at)? != *MAGIC {
        return Err(SealSetupError::Damaged(
            "not a sealed-keys file of this format",
        ));
    }
    Ok(Some(KeyId(take::<16>(body, &mut at)?)))
}

/// The keys of the data directory `data_dir`, made where it has none (§2): under `configured`
/// where its owner names a root key file; else under the one named for its root key's ID in the
/// per-user directory ([`key_file_for`]), so a directory's key file is found from the directory
/// alone, whatever it holds or has yet to learn (a joining node has no node ID when it first seals
/// its join); and a directory whose keys an earlier binary made under the file named for its
/// node keeps that one (`legacy`). Every refusal of §2 holds: a missing key file is never made
/// again for a directory that has keys.
pub fn resolve(
    data_dir: &Path,
    configured: Option<&Path>,
    legacy: Option<PathBuf>,
) -> Result<DataKeys, SealSetupError> {
    resolve_in(
        keys_dir().as_deref(),
        data_dir,
        configured,
        legacy,
        new_root,
    )
}

/// [`resolve`] with the directory of key files `keys` and the maker of a new directory's root key
/// ID `new_root`.
fn resolve_in(
    keys: Option<&Path>,
    data_dir: &Path,
    configured: Option<&Path>,
    legacy: Option<PathBuf>,
    new_root: fn() -> Result<KeyId, SealSetupError>,
) -> Result<DataKeys, SealSetupError> {
    if let Some(key_file) = configured {
        let keys = DataKeys::new(data_dir, key_file);
        keys.ensure()?;
        return Ok(keys);
    }
    if let Some(found) = find_in(keys, data_dir, None, legacy)? {
        return Ok(found);
    }
    let keys = keys.ok_or_else(|| unnamed_keys_dir(data_dir))?;
    hyper_seal::lock_keys(key_slots())?;
    let id = intended_root(data_dir, new_root)?;
    let key_file = key_file_in(keys, id);
    // Made where it is missing, then the directory's keys under it, or opened where another maker
    // made them first: every maker names the one file the intent does.
    open_or_make(data_dir, &key_file, Some(id))?;
    Ok(DataKeys::new(data_dir, key_file))
}

/// A new data directory's root key ID (§2): random, a root key of its own.
#[cfg(not(any(test, feature = "test-support")))]
fn new_root() -> Result<KeyId, SealSetupError> {
    random_root()
}

/// A new data directory's root key ID under a build with test support: [`TEST_ROOT`].
#[cfg(any(test, feature = "test-support"))]
fn new_root() -> Result<KeyId, SealSetupError> {
    Ok(TEST_ROOT)
}

/// A random root key ID.
fn random_root() -> Result<KeyId, SealSetupError> {
    Ok(KeyId::random()?)
}

/// The root key every data directory a test makes without naming a key file shares, under a build
/// with test support: one key file in the system's temporary directory ([`keys_dir`]), made once,
/// so a test run leaves no key file behind for each data directory it made and let go of (a key
/// file a directory, in a directory nothing empties, grew by every test run). Each directory's own
/// keys below it are random, as they are everywhere ([`create_as`]), so no directory's records
/// open under another's keys.
#[cfg(any(test, feature = "test-support"))]
pub const TEST_ROOT: KeyId = KeyId(*b"focal test root.");

/// The root key ID the keys of `data_dir`, which has none, are made under: the one its
/// [`INTENT_FILE`] names, or one `new_root` makes, recorded there first; another maker's recorded
/// first is taken.
fn intended_root(
    data_dir: &Path,
    new_root: fn() -> Result<KeyId, SealSetupError>,
) -> Result<KeyId, SealSetupError> {
    if let Some(id) = read_intent(data_dir)? {
        return Ok(id);
    }
    let id = new_root()?;
    let mut bytes = Vec::with_capacity(INTENT_BYTES);
    bytes.extend_from_slice(INTENT_MAGIC);
    bytes.extend_from_slice(&id.0);
    let hash = blake3::hash(&bytes);
    bytes.extend_from_slice(hash.as_bytes());
    match publish_once(data_dir, OsStr::new(INTENT_FILE), &bytes).map_err(SealSetupError::Io)? {
        Published::Made => Ok(id),
        Published::Found => read_intent(data_dir)?.ok_or(SealSetupError::IntentDamaged(
            "gone after another maker made it",
        )),
    }
}

/// The root key ID `data_dir`'s [`INTENT_FILE`] names, none where it has none; the file is checked
/// whole first.
fn read_intent(data_dir: &Path) -> Result<Option<KeyId>, SealSetupError> {
    let path = data_dir.join(INTENT_FILE);
    let medium = FileMedium;
    if !medium.exists(&path).map_err(SealSetupError::Io)? {
        return Ok(None);
    }
    let bytes = medium
        .read(&path, INTENT_BYTES)
        .map_err(SealSetupError::Io)?;
    if bytes.len() != INTENT_BYTES {
        return Err(SealSetupError::IntentDamaged("a file of the wrong length"));
    }
    let (body, stated) = bytes.split_at(INTENT_BYTES.saturating_sub(HASH));
    if blake3::hash(body).as_bytes() != stated {
        return Err(SealSetupError::IntentDamaged(
            "its hash does not match its bytes",
        ));
    }
    let mut at = 0usize;
    if take::<8>(body, &mut at)? != *INTENT_MAGIC {
        return Err(SealSetupError::IntentDamaged(
            "not a root intent of this format",
        ));
    }
    Ok(Some(KeyId(take::<16>(body, &mut at)?)))
}

/// The refusal of a default key file where the platform names no per-user directory.
fn unnamed_keys_dir(data_dir: &Path) -> SealSetupError {
    SealSetupError::KeyFile {
        path: data_dir.to_path_buf(),
        source: std::io::Error::other("the platform names no per-user configuration directory"),
    }
}

/// [`resolve`] of a data directory that has keys, made nowhere: none where it has none yet. What a
/// reader beside a running owner (a command line on the owner's data directory) opens with, so it
/// never makes keys in a directory it does not own. A directory under `configured` is taken as
/// having keys where its `SEAL.node` is.
pub fn find(
    data_dir: &Path,
    configured: Option<&Path>,
    legacy: Option<PathBuf>,
) -> Result<Option<DataKeys>, SealSetupError> {
    find_in(keys_dir().as_deref(), data_dir, configured, legacy)
}

/// [`find`] with the directory of key files `keys`.
fn find_in(
    keys: Option<&Path>,
    data_dir: &Path,
    configured: Option<&Path>,
    legacy: Option<PathBuf>,
) -> Result<Option<DataKeys>, SealSetupError> {
    let Some(id) = recorded_root(data_dir)? else {
        return Ok(None);
    };
    if let Some(key_file) = configured {
        let keys = DataKeys::new(data_dir, key_file);
        keys.ensure()?;
        return Ok(Some(keys));
    }
    let named = key_file_in(keys.ok_or_else(|| unnamed_keys_dir(data_dir))?, id);
    let exists = |path: &Path| {
        path.try_exists().map_err(|source| SealSetupError::KeyFile {
            path: path.to_path_buf(),
            source,
        })
    };
    let key_file = match legacy {
        Some(legacy) if !exists(&named)? && exists(&legacy)? => legacy,
        _ => named,
    };
    let keys = DataKeys::new(data_dir, key_file);
    keys.ensure()?;
    Ok(Some(keys))
}

/// Whether `key_file` lies within `data_dir`, each resolved through the links that exist.
fn inside(key_file: &Path, data_dir: &Path) -> Result<bool, SealSetupError> {
    let data = data_dir.canonicalize().map_err(SealSetupError::Io)?;
    let parent = key_file
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    // The key's directory may not exist yet: resolve its nearest existing ancestor.
    let mut existing = parent.to_path_buf();
    let mut rest = Vec::new();
    while !existing.exists() {
        let Some(name) = existing.file_name().map(std::ffi::OsStr::to_os_string) else {
            break;
        };
        rest.push(name);
        if !existing.pop() {
            break;
        }
    }
    let mut resolved = if existing.as_os_str().is_empty() {
        PathBuf::from(".")
            .canonicalize()
            .map_err(SealSetupError::Io)?
    } else {
        existing.canonicalize().map_err(SealSetupError::Io)?
    };
    for name in rest.into_iter().rev() {
        resolved.push(name);
    }
    Ok(resolved.starts_with(&data))
}

/// Makes a root key file at `path`: a new owner-only file of 32 random bytes, durable with its
/// directory, never over a file that is there (`AlreadyExists`), published whole so a reader finds
/// no file or the key ([`publish_once`]): nodes starting at once under one key file make one key,
/// and every one of them opens it. What a node makes at its first start, and what an operator
/// makes for a deployment that keeps the key in a secret store (29 §10).
pub fn create_root_key(path: &Path) -> Result<(), SealSetupError> {
    let fail = |source| SealSetupError::KeyFile {
        path: path.to_path_buf(),
        source,
    };
    let dir = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    if !dir.exists() {
        let mut ancestors = Vec::new();
        let mut at = dir.to_path_buf();
        while !at.exists() {
            ancestors.push(at.clone());
            if !at.pop() {
                break;
            }
        }
        for dir in ancestors.iter().rev() {
            // Another process may make it after the look above: two nodes of one user starting
            // at once make the same keys' directory, and the one made second failed its start
            // ("File exists").
            ensure_dir_private(dir).map_err(fail)?;
            if let Some(parent) = dir.parent().filter(|p| !p.as_os_str().is_empty()) {
                focal_platform::sync_dir(parent).map_err(fail)?;
            }
        }
    }
    let name = path.file_name().ok_or_else(|| {
        fail(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "a key file's path names no file",
        ))
    })?;
    let mut bytes = zeroize::Zeroizing::new([0u8; ROOT_BYTES]);
    getrandom::fill(bytes.as_mut()).map_err(|_| SealSetupError::Random)?;
    match publish_once(dir, name, bytes.as_ref()).map_err(fail)? {
        Published::Made => Ok(()),
        Published::Found => Err(fail(std::io::Error::from(
            std::io::ErrorKind::AlreadyExists,
        ))),
    }
}

/// A random key from the operating system's generator, wiped wherever it was staged.
fn random_key() -> Result<Secret32, SealSetupError> {
    let mut bytes = [0u8; ROOT_BYTES];
    getrandom::fill(&mut bytes).map_err(|_| SealSetupError::Random)?;
    let key = Secret32::from_bytes(&bytes);
    bytes.fill(0);
    Ok(key?)
}

/// The root key in `path`, under the ID and generation `SEAL.node` records for it.
fn read_root(path: &Path, id: KeyId, generation: u32) -> Result<FileSource, SealSetupError> {
    use std::io::Read as _;
    let fail = |source| SealSetupError::KeyFile {
        path: path.to_path_buf(),
        source,
    };
    let file = focal_platform::fs::open_private(path, true, false, false).map_err(fail)?;
    let mut bytes = [0u8; ROOT_BYTES + 1];
    let mut read = 0usize;
    let mut handle = &file;
    // One byte past a key tells a longer file from a key.
    while let Some(rest) = bytes.get_mut(read..) {
        if rest.is_empty() {
            break;
        }
        let n = handle.read(rest).map_err(fail)?;
        if n == 0 {
            break;
        }
        read = read.saturating_add(n);
    }
    let key = bytes.get(..read).unwrap_or(&[]);
    let source = FileSource::new(id, generation, key, &file);
    bytes.fill(0);
    source.map_err(SealSetupError::from)
}

/// One key's entry: its ID, its generation and its record.
fn entry(out: &mut Vec<u8>, id: KeyId, generation: u32, record: &Wrapped) {
    out.extend_from_slice(&id.0);
    out.extend_from_slice(&generation.to_le_bytes());
    out.extend_from_slice(&record.encode());
}

/// Reads `[u8; N]` at `at` in `bytes`, advancing `at`.
fn take<const N: usize>(bytes: &[u8], at: &mut usize) -> Result<[u8; N], SealSetupError> {
    let end = at
        .checked_add(N)
        .ok_or(SealSetupError::Damaged("a field past the file"))?;
    let field = bytes
        .get(*at..end)
        .and_then(|f| <[u8; N]>::try_from(f).ok())
        .ok_or(SealSetupError::Damaged("a field past the file"))?;
    *at = end;
    Ok(field)
}

/// A key below the root: its ID, its generation and its record.
struct Entry {
    id: KeyId,
    generation: u32,
    record: Wrapped,
}

fn read_entry(bytes: &[u8], at: &mut usize) -> Result<Entry, SealSetupError> {
    let id = KeyId(take::<16>(bytes, at)?);
    let generation = u32::from_le_bytes(take::<4>(bytes, at)?);
    let record = Wrapped::decode(&take::<RECORD>(bytes, at)?)
        .map_err(|_| SealSetupError::Damaged("a key record that does not parse"))?;
    Ok(Entry {
        id,
        generation,
        record,
    })
}

/// Opens the keys of the data directory `data_dir` under the root key in `key_file`, or, in a data
/// directory that has none, makes them, making the root key file too where it does not exist.
pub fn open_or_create(data_dir: &Path, key_file: &Path) -> Result<NodeKeys, SealSetupError> {
    open_or_make(data_dir, key_file, None)
}

/// [`open_or_create`], the keys of a directory that has none recorded under the root key ID `root`
/// where it is given (the directory's intent, [`resolve`]), else under a random one.
fn open_or_make(
    data_dir: &Path,
    key_file: &Path,
    root: Option<KeyId>,
) -> Result<NodeKeys, SealSetupError> {
    // Keys live only in the process's locked region, made once for the derived count; a region
    // already made for at least as many serves this call too.
    hyper_seal::lock_keys(key_slots())?;
    if inside(key_file, data_dir)? {
        return Err(SealSetupError::KeyInDataDir(key_file.to_path_buf()));
    }
    let seal = data_dir.join(SEAL_FILE);
    let medium = FileMedium;
    if medium.exists(&seal).map_err(SealSetupError::Io)? {
        if !key_file
            .try_exists()
            .map_err(|source| SealSetupError::KeyFile {
                path: key_file.to_path_buf(),
                source,
            })?
        {
            return Err(SealSetupError::MissingKey(key_file.to_path_buf()));
        }
        let bytes = medium.read(&seal, SEAL_BYTES).map_err(SealSetupError::Io)?;
        return open(&bytes, key_file);
    }
    if !key_file
        .try_exists()
        .map_err(|source| SealSetupError::KeyFile {
            path: key_file.to_path_buf(),
            source,
        })?
    {
        match create_root_key(key_file) {
            // Made by another process starting this directory at once: the same file, whole.
            Err(SealSetupError::KeyFile { source, .. })
                if source.kind() == std::io::ErrorKind::AlreadyExists => {}
            made => made?,
        }
    }
    let root = match root {
        Some(id) => id,
        None => random_root()?,
    };
    match create_as(data_dir, key_file, root) {
        // Another process made the directory's keys first, under the same key file: theirs.
        Err(SealSetupError::Raced) => {
            let bytes = medium.read(&seal, SEAL_BYTES).map_err(SealSetupError::Io)?;
            open(&bytes, key_file)
        }
        made => made,
    }
}

/// The keys of `data_dir`, which has none, made under the root key in `key_file`, recorded under
/// `root_id`, and published in `SEAL.node` where none is ([`publish_once`]); [`SealSetupError::Raced`]
/// where another maker's stood first (an atomic replace let a second maker's file stand over the
/// first's, whose maker then sealed under keys no file named).
fn create_as(data_dir: &Path, key_file: &Path, root_id: KeyId) -> Result<NodeKeys, SealSetupError> {
    let mut root = read_root(key_file, root_id, 0)?;
    // The node key: random, wrapped by the root.
    let node_secret = random_key()?;
    let node_record = root.wrap(&node_secret)?;
    let node_id = KeyId::random()?;
    let node = WrappingKey::new(node_id, 0, node_secret);
    let mut bytes = Vec::with_capacity(SEAL_BYTES);
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&root_id.0);
    bytes.extend_from_slice(&0u32.to_le_bytes());
    entry(&mut bytes, node_id, 0, &node_record);
    let mut children = Vec::with_capacity(KEYS.saturating_sub(1));
    for _ in 1..KEYS {
        let (secret, record) = node.make_child()?;
        let id = KeyId::random()?;
        entry(&mut bytes, id, 0, &record);
        children.push((id, secret));
    }
    let hash = blake3::hash(&bytes);
    bytes.extend_from_slice(hash.as_bytes());
    match publish_once(data_dir, OsStr::new(SEAL_FILE), &bytes).map_err(SealSetupError::Io)? {
        Published::Made => keys(children, (root_id, 0)),
        Published::Found => Err(SealSetupError::Raced),
    }
}

/// What a no-clobber publication ([`publish_once`]) found at its name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Published {
    /// This maker's bytes stand at the name.
    Made,
    /// Another maker's do, whole: every maker syncs its bytes before it links them.
    Found,
}

/// `bytes` published as `dir`/`name` where nothing is, never over what is: written to a temporary
/// of this maker's own beside it (`<name>.<16 hex digits>.part`), synced, then linked to the name
/// (`focal_platform::fs::atomic_create_new`), which only the first maker's link does, so a reader
/// finds no file or a whole one. Once the name stands every temporary of it is removed
/// ([`sweep`]): this maker's where another's stood first, and any left by a maker that crashed or
/// failed between its write and its link; a maker whose temporary was so removed before its link
/// finds the name made. A temporary is left only by a maker that began before the name stood and
/// ended between its write and its link.
fn publish_once(dir: &Path, name: &OsStr, bytes: &[u8]) -> std::io::Result<Published> {
    let target = dir.join(name);
    let mut nonce = [0u8; 8];
    getrandom::fill(&mut nonce)
        .map_err(|_| std::io::Error::other("the operating system's random source failed"))?;
    let mut temporary = name.to_os_string();
    temporary.push(format!(".{:016x}.part", u64::from_le_bytes(nonce)));
    let temporary = dir.join(temporary);
    let written = create_private_new(&temporary, false, true).and_then(|mut file| {
        std::io::Write::write_all(&mut file, bytes)?;
        file.sync_all()
    });
    if let Err(error) = written {
        // What is left of it goes with the name's next publication's sweep.
        let _ = std::fs::remove_file(&temporary);
        return Err(error);
    }
    let published = match focal_platform::fs::atomic_create_new(&temporary, &target) {
        Ok(()) => Published::Made,
        // Another maker linked first, or removed this maker's temporary once its own stood.
        Err(_) if target.try_exists()? => Published::Found,
        Err(error) => {
            let _ = std::fs::remove_file(&temporary);
            return Err(error);
        }
    };
    sweep(dir, name)?;
    focal_platform::sync_dir(dir)?;
    Ok(published)
}

/// Removes every temporary of `name` in `dir` ([`publish_once`]): one walk of the directory's
/// entries, removing only names of a temporary's exact form. A temporary its maker still holds
/// open on Windows, already removed by another sweep, is refused (`PermissionDenied`) and goes when
/// its maker's handle closes.
fn sweep(dir: &Path, name: &OsStr) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if !is_temporary(
            entry.file_name().as_encoded_bytes(),
            name.as_encoded_bytes(),
        ) {
            continue;
        }
        match std::fs::remove_file(entry.path()) {
            Err(error)
                if !matches!(
                    error.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::PermissionDenied
                ) =>
            {
                return Err(error);
            }
            _ => {}
        }
    }
    Ok(())
}

/// Whether the file named `file` is a temporary of `name`: `<name>.<16 hex digits>.part`.
fn is_temporary(file: &[u8], name: &[u8]) -> bool {
    file.strip_prefix(name)
        .and_then(|rest| rest.strip_prefix(b"."))
        .and_then(|rest| rest.strip_suffix(b".part"))
        .is_some_and(|nonce| nonce.len() == 16 && nonce.iter().all(u8::is_ascii_hexdigit))
}

/// The node's keys from the children the node key unwrapped, in their order in `SEAL.node`.
fn keys(children: Vec<(KeyId, Secret32)>, root: (KeyId, u32)) -> Result<NodeKeys, SealSetupError> {
    let mut children = children.into_iter();
    let mut next = || {
        children
            .next()
            .ok_or(SealSetupError::Damaged("fewer keys than a node holds"))
    };
    let (log_parent_id, log_parent) = next()?;
    let (_, log_auth) = next()?;
    let (group_id, group) = next()?;
    let (content_id, content) = next()?;
    let (journal_id, journal) = next()?;
    Ok(NodeKeys {
        log_parent: WrappingKey::new(log_parent_id, 0, log_parent),
        log_auth,
        group: WrappingKey::new(group_id, 0, group),
        content: WrappingKey::new(content_id, 0, content),
        journal: WrappingKey::new(journal_id, 0, journal),
        root,
    })
}

/// `SEAL.node` opened as far as its node key: the node key, the five store keys' entries below
/// it, still wrapped, and the root key's ID and generation.
struct Opened {
    node_key: WrappingKey,
    entries: Vec<Entry>,
    root: (KeyId, u32),
}

/// The node key of `SEAL.node`'s `bytes`, unwrapped by the root key in `key_file`, with the
/// entries of the five store keys below it, still wrapped.
fn node_key(bytes: &[u8], key_file: &Path) -> Result<Opened, SealSetupError> {
    if bytes.len() != SEAL_BYTES {
        return Err(SealSetupError::Damaged("a file of the wrong length"));
    }
    let body_len = SEAL_BYTES.saturating_sub(HASH);
    let (body, stated) = bytes.split_at(body_len);
    if blake3::hash(body).as_bytes() != stated {
        return Err(SealSetupError::Damaged("its hash does not match its bytes"));
    }
    let mut at = 0usize;
    if take::<8>(body, &mut at)? != *MAGIC {
        return Err(SealSetupError::Damaged(
            "not a sealed-keys file of this format",
        ));
    }
    let root_id = KeyId(take::<16>(body, &mut at)?);
    let root_generation = u32::from_le_bytes(take::<4>(body, &mut at)?);
    let mut root = read_root(key_file, root_id, root_generation)?;
    let node = read_entry(body, &mut at)?;
    let node_secret = root.unwrap(&node.record).map_err(|error| match error {
        SealError::Unwrap => SealSetupError::WrongKey(key_file.to_path_buf()),
        other => SealSetupError::Seal(other),
    })?;
    let node_key = WrappingKey::new(node.id, node.generation, node_secret);
    let mut entries = Vec::with_capacity(KEYS.saturating_sub(1));
    for _ in 1..KEYS {
        entries.push(read_entry(body, &mut at)?);
    }
    Ok(Opened {
        node_key,
        entries,
        root: (root_id, root_generation),
    })
}

fn open(bytes: &[u8], key_file: &Path) -> Result<NodeKeys, SealSetupError> {
    let Opened {
        node_key,
        entries,
        root,
    } = node_key(bytes, key_file)?;
    let mut children = Vec::with_capacity(entries.len());
    for child in entries {
        let secret = node_key
            .unwrap(&child.record)
            .map_err(|_| SealSetupError::Damaged("a store key that does not unwrap"))?;
        children.push((child.id, secret));
    }
    keys(children, root)
}

/// A store sealed under one of the node's keys (§3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Store {
    /// The group files: images, records and checkpoints.
    Group,
    /// The content store's objects.
    Content,
    /// Journals and small files.
    Journal,
}

impl Store {
    /// The store key's place among `SEAL.node`'s entries below the node key.
    fn entry(self) -> usize {
        match self {
            Self::Group => 2,
            Self::Content => 3,
            Self::Journal => 4,
        }
    }
}

/// The key of one store of the data directory `data_dir`, unwrapped by the root key in `key_file`
/// and nothing else unwrapped: what a writer or reader of that store holds for one file and drops.
/// A data directory without keys is refused, never given new ones here: only the node's storage
/// start makes them (`open_or_create`).
pub fn store_key(
    data_dir: &Path,
    key_file: &Path,
    store: Store,
) -> Result<WrappingKey, SealSetupError> {
    hyper_seal::lock_keys(key_slots())?;
    let seal = data_dir.join(SEAL_FILE);
    let medium = FileMedium;
    if !medium.exists(&seal).map_err(SealSetupError::Io)? {
        return Err(SealSetupError::NoKeys(data_dir.to_path_buf()));
    }
    let bytes = medium.read(&seal, SEAL_BYTES).map_err(SealSetupError::Io)?;
    let Opened {
        node_key, entries, ..
    } = node_key(&bytes, key_file)?;
    let entry = entries
        .get(store.entry())
        .ok_or(SealSetupError::Damaged("fewer keys than a node holds"))?;
    let secret = node_key
        .unwrap(&entry.record)
        .map_err(|_| SealSetupError::Damaged("a store key that does not unwrap"))?;
    Ok(WrappingKey::new(entry.id, entry.generation, secret))
}

/// The parent of every cache this process keeps on a volume: random when a cache first asks for a
/// key, held in the locked region for the process's life, never written anywhere. Made at that
/// first ask rather than kept as a failure, so a moment's want of a key slot refuses that cache
/// alone.
static CACHE_PARENT: std::sync::Mutex<Option<WrappingKey>> = std::sync::Mutex::new(None);

/// What a cache's key is derived for (HKDF's salt and info, `derive_child`).
const CACHE_PURPOSE: &[u8] = b"focal cache key";

/// A cache this process keeps on a volume, such as a session replica's payload log, which is
/// emptied at every open and refilled from what the replica's records and checkpoints hold. Its
/// files are sealed under a key derived from the process's cache parent and the cache's own random
/// ID, so they open only in the process that wrote them, while it runs: after a crash, or in a copy
/// of the volume, they are ciphertext under a key nothing holds (an erase as NIST SP 800-88r1
/// describes it, with nothing left to destroy). The ID is not secret; it is every seal's additional
/// data.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Cache {
    id: [u8; 16],
}

impl Cache {
    /// A new cache, its ID random.
    pub fn new() -> Result<Self, SealSetupError> {
        let mut id = [0u8; 16];
        getrandom::fill(&mut id).map_err(|_| SealSetupError::Random)?;
        Ok(Self { id })
    }

    /// The cache's key: the same for this cache for the process's life, so its writer and each of
    /// its readers make their own. What it seals is a version and an index of
    /// `hyper_seal::stream::VersionKey`, which the cache never seals twice with other bytes.
    pub fn key(&self) -> Result<hyper_seal::stream::VersionKey, SealSetupError> {
        hyper_seal::lock_keys(key_slots())?;
        let mut held = CACHE_PARENT.lock().map_err(|_| SealSetupError::CacheKey)?;
        let parent = match &mut *held {
            Some(parent) => parent,
            empty @ None => empty.insert(WrappingKey::generate(0)?),
        };
        let (secret, _) = parent.derive_child(CACHE_PURPOSE, &self.id)?;
        drop(held);
        Ok(hyper_seal::stream::VersionKey::new(&secret, self.id)?)
    }
}

#[cfg(test)]
mod tests;
