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
use std::path::{Path, PathBuf};

use focal_platform::fs::{
    FileMedium, Medium as _, create_dir_private, create_private_new, install,
};
use hyper_seal::keys::{KeyId, KeySource as _, RECORD, Wrapped, WrappingKey};
use hyper_seal::{FileSource, SealError, Secret32};

/// The file in the data directory that holds the node's wrapped keys.
pub const SEAL_FILE: &str = "SEAL.node";
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
    let home = std::env::var_os("HOME").map(PathBuf::from);
    let dir = if cfg!(target_os = "macos") {
        home?.join("Library/Application Support/Focal Keys")
    } else if cfg!(windows) {
        PathBuf::from(std::env::var_os("APPDATA")?)
            .join("Focal")
            .join("keys")
    } else if let Some(config) = std::env::var_os("XDG_CONFIG_HOME") {
        PathBuf::from(config).join("focal").join("keys")
    } else {
        home?.join(".config").join("focal").join("keys")
    };
    Some(dir.join(format!("{node}.key")))
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
/// directory, never over a file that is there. What a node makes at its first start, and what an
/// operator makes for a deployment that keeps the key in a secret store (29 §10).
pub fn create_root_key(path: &Path) -> Result<(), SealSetupError> {
    let fail = |source| SealSetupError::KeyFile {
        path: path.to_path_buf(),
        source,
    };
    if let Some(dir) = path.parent().filter(|p| !p.as_os_str().is_empty())
        && !dir.exists()
    {
        let mut ancestors = Vec::new();
        let mut at = dir.to_path_buf();
        while !at.exists() {
            ancestors.push(at.clone());
            if !at.pop() {
                break;
            }
        }
        for dir in ancestors.iter().rev() {
            create_dir_private(dir).map_err(fail)?;
            if let Some(parent) = dir.parent().filter(|p| !p.as_os_str().is_empty()) {
                focal_platform::sync_dir(parent).map_err(fail)?;
            }
        }
    }
    let mut bytes = [0u8; ROOT_BYTES];
    getrandom::fill(&mut bytes).map_err(|_| SealSetupError::Random)?;
    let mut file = create_private_new(path, false, true).map_err(fail)?;
    let written = std::io::Write::write_all(&mut file, &bytes).and_then(|()| file.sync_all());
    bytes.fill(0);
    written.map_err(fail)?;
    if let Some(dir) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        focal_platform::sync_dir(dir).map_err(fail)?;
    }
    Ok(())
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
        create_root_key(key_file)?;
    }
    create(data_dir, key_file)
}

fn create(data_dir: &Path, key_file: &Path) -> Result<NodeKeys, SealSetupError> {
    let root_id = KeyId::random()?;
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
    let mut medium = FileMedium;
    install(&mut medium, &data_dir.join(SEAL_FILE), &bytes).map_err(SealSetupError::Io)?;
    keys(children, (root_id, 0))
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

#[cfg(test)]
mod tests;
