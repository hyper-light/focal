//! The bus's local identities: an authority of its own, each node's certificate, and one identity a
//! client, kept in a directory as DER files.
//!
//! This is the bus's standalone mode, for a laptop and for measurement: the directory is made once
//! (and only then; a directory that holds an authority is used as it is) and read by every node of
//! the group and the clients they admit. A lone node is a group of one. An enrolled deployment
//! names its identities through focal's enrollment instead (doc 32 §5). Every key file is written
//! owner-only, then renamed into place, so a reader never sees half a key; the authority is written
//! last, so a directory that holds it is whole.
//!
//! Node `n` (from 1) is peer `n` and names itself `focal-bus-node-n`; client `k` (from 1) of a group
//! of `N` nodes is peer `N + k`.
//!
//! Every key file is sealed at rest (29 §12.9). The directory has keys of its own, as a client's
//! data directory has (its `SEAL.node`, under the root key file its owner names, else the per-user
//! one named for its root key), and each key file holds a frame of its name and its key sealed whole
//! under the directory's journal key, so a key moved to another name, or carried in from another
//! directory, is refused. A directory an earlier binary wrote in the clear is converted by its first
//! opener under the directory's lock, key by key, each through the rename that writes any key, and
//! then marked sealed; from the mark a key in the clear is refused. One whose make was cut before
//! its authority, never read whole, is swept of what the cut left before it is made. The certificates are public, as
//! every handshake shows them, and stay in the clear.

use std::path::{Path, PathBuf};

use crate::BusError;

/// The lock every opener holds while it makes, converts or reads the directory: each holds it for
/// that bounded work alone.
const LOCK_FILE: &str = "LOCK";
/// The mark a directory holds once every key in it is sealed: from then on a key in the clear is
/// refused, never read.
const SEALED_FILE: &str = "SEALED";
/// Format: a sealed key file's plaintext begins with this; its name's length and its name, the key
/// and a BLAKE3 hash over all of it follow.
const KEY_MAGIC: &[u8; 8] = b"FCLBUSK1";
/// The most bytes a key holds: an ECDSA P-256 key's PKCS #8 document (RFC 5958 around RFC 5915's
/// key with its curve and public key), what rcgen's `KeyPair::generate` makes, as every key the
/// directory holds was made; a key past it was made by nothing here, and is refused.
const KEY_DOCUMENT_BYTES: usize = 138;
/// The most bytes a key file's name holds: `client-` and a count's twenty digits, or `node-`'s.
const NAME_BYTES: usize = 32;
/// A key in the clear is a DER document: it begins with a SEQUENCE's tag, where a sealed file
/// begins with its header's version.
const DER_SEQUENCE: u8 = 0x30;

/// The name node `n`'s certificate carries, which its peers and clients check when they dial it.
pub fn node_name(n: u64) -> String {
    format!("focal-bus-node-{n}")
}

/// The DER files of one identity.
#[derive(Clone, Debug)]
pub struct Identity {
    /// Its certificate.
    pub certificate: Vec<u8>,
    /// Its PKCS #8 private key.
    pub key: Vec<u8>,
}

/// A local directory's identities, as read.
#[derive(Clone, Debug)]
pub struct Local {
    /// The authority's certificate, every identity's root.
    pub root: Vec<u8>,
    /// Each node's identity, node 1 first: node `n` is peer `n`.
    pub nodes: Vec<Identity>,
    /// The clients' identities, client 1 first.
    pub clients: Vec<Identity>,
}

impl Local {
    /// The peer client `index` (from 0) is: the nodes are peers 1 to N, the clients after them.
    pub fn client_peer(&self, index: usize) -> Option<u64> {
        Self::client_peer_of(self.nodes.len(), index)
    }

    /// The peer client `index` (from 0) is in a group of `nodes` nodes.
    pub fn client_peer_of(nodes: usize, index: usize) -> Option<u64> {
        u64::try_from(nodes)
            .ok()?
            .checked_add(u64::try_from(index).ok()?)?
            .checked_add(1)
    }

    /// Node `n`'s identity (from 1).
    pub fn node(&self, n: u64) -> Option<&Identity> {
        self.nodes.get(usize::try_from(n).ok()?.checked_sub(1)?)
    }
}

fn file(dir: &Path, name: &str) -> PathBuf {
    dir.join(name)
}

/// The directory's keys (29 §12.9): made where it has none, under `key_file` where its owner names
/// one, else under the per-user root key file named for its root key, as a client's are.
fn keys(dir: &Path, key_file: Option<&Path>) -> Result<focal_seal::DataKeys, BusError> {
    focal_seal::resolve(dir, key_file, None)
        .map_err(|_| BusError::Configuration("the identity directory's keys could not be opened"))
}

/// The directory's lock, held by this opener until it lets it go: a wait the holder's, for its
/// bounded work (`FileLock::exclusive_waiting`).
fn lock(dir: &Path) -> Result<focal_platform::FileLock, BusError> {
    let file = focal_platform::fs::open_private(&file(dir, LOCK_FILE), true, true, true).map_err(
        |_| BusError::Configuration("the identity directory's lock could not be opened"),
    )?;
    focal_platform::FileLock::exclusive_waiting(file)
        .map_err(|_| BusError::Configuration("the identity directory's lock could not be taken"))
}

/// The frame of the key `key` filed as `name`: what a key file seals.
fn frame(name: &str, key: &[u8]) -> Result<Vec<u8>, BusError> {
    let name_bytes = name.as_bytes();
    let length = u8::try_from(name_bytes.len())
        .ok()
        .filter(|length| usize::from(*length) <= NAME_BYTES)
        .ok_or(BusError::Configuration(
            "an identity file's name past its bound",
        ))?;
    if key.is_empty() || key.len() > KEY_DOCUMENT_BYTES {
        return Err(BusError::Configuration(
            "a key past the documents this directory holds",
        ));
    }
    let mut framed = Vec::new();
    framed
        .try_reserve_exact(
            KEY_MAGIC
                .len()
                .saturating_add(1)
                .saturating_add(name_bytes.len())
                .saturating_add(key.len())
                .saturating_add(32),
        )
        .map_err(|_| BusError::Capacity("an identity key's frame"))?;
    framed.extend_from_slice(KEY_MAGIC);
    framed.push(length);
    framed.extend_from_slice(name_bytes);
    framed.extend_from_slice(key);
    let hash = blake3::hash(&framed);
    framed.extend_from_slice(hash.as_bytes());
    Ok(framed)
}

/// The key a frame holds, refused unless it is a whole frame of a key filed as `name`.
fn unframe(name: &str, framed: &[u8]) -> Result<Vec<u8>, BusError> {
    let corrupt = || BusError::Configuration("a key file is not the key its name holds");
    let (body, hash) = framed
        .split_at_checked(framed.len().checked_sub(32).ok_or_else(corrupt)?)
        .ok_or_else(corrupt)?;
    if blake3::hash(body).as_bytes() != hash {
        return Err(corrupt());
    }
    let rest = body
        .strip_prefix(KEY_MAGIC.as_slice())
        .ok_or_else(corrupt)?;
    let (&length, rest) = rest.split_first().ok_or_else(corrupt)?;
    let (filed, key) = rest
        .split_at_checked(usize::from(length))
        .ok_or_else(corrupt)?;
    if filed != name.as_bytes() || key.is_empty() || key.len() > KEY_DOCUMENT_BYTES {
        return Err(corrupt());
    }
    Ok(key.to_vec())
}

/// The bytes a key file holds at most, sealed: its whole frame's sealed file.
fn key_file_bound() -> Result<usize, BusError> {
    let frame = KEY_MAGIC
        .len()
        .saturating_add(1)
        .saturating_add(NAME_BYTES)
        .saturating_add(KEY_DOCUMENT_BYTES)
        .saturating_add(32);
    focal_seal::file::sealed_bound(frame).ok_or(BusError::Configuration("a key file's bound"))
}

/// The key `key` filed as `name`, sealed whole under the directory's journal key (unwrapped for
/// it and dropped after) and written owner-only through the rename that writes any identity file.
fn write_key(
    dir: &Path,
    keys: &focal_seal::DataKeys,
    name: &str,
    key: &[u8],
) -> Result<(), BusError> {
    let journal = keys
        .key(focal_seal::Store::Journal)
        .map_err(|_| BusError::Configuration("the identity directory's key could not be opened"))?;
    let framed = frame(name, key)?;
    let sealed = focal_seal::file::seal(&journal, &framed)
        .map_err(|_| BusError::Configuration("an identity key could not be sealed"))?;
    drop(journal);
    write_private(dir, name, &sealed)
}

/// The key filed as `name`: opened from its sealed file under the directory's journal key; in the
/// clear (an earlier binary's DER document) only where `clear`, before the directory is marked
/// sealed. None where `clear` and the file lies in the clear, the caller to seal it.
fn read_key(
    dir: &Path,
    keys: &focal_seal::DataKeys,
    name: &str,
    clear: bool,
) -> Result<(Vec<u8>, bool), BusError> {
    use focal_platform::fs::Medium as _;
    let bytes = focal_platform::fs::FileMedium
        .read(&file(dir, name), key_file_bound()?)
        .map_err(|_| BusError::Configuration("an identity file is missing or unreadable"))?;
    if bytes.first() == Some(&DER_SEQUENCE) {
        if !clear {
            return Err(BusError::Configuration(
                "a key lies in the clear after the identity directory was sealed",
            ));
        }
        if bytes.len() > KEY_DOCUMENT_BYTES {
            return Err(BusError::Configuration(
                "a key past the documents this directory holds",
            ));
        }
        return Ok((bytes, true));
    }
    let journal = keys
        .key(focal_seal::Store::Journal)
        .map_err(|_| BusError::Configuration("the identity directory's key could not be opened"))?;
    let framed = focal_seal::file::open(&journal, bytes, key_file_bound()?).map_err(|_| {
        BusError::Configuration("a key file does not open under its directory's keys")
    })?;
    Ok((unframe(name, &framed)?, false))
}

/// Every key an earlier binary wrote in the clear sealed, then the directory marked sealed (29
/// §12.9), under the opener's lock: the keys the directory files from 1 up, nodes' and clients'
/// alike, as `make` files them, each probed until its sequence ends, each sequence at most
/// `bound` long. A key already sealed, from a conversion a crash cut, is opened to be sure of it.
/// A cut before the mark leaves each key whole in one form or the other, and the next opener
/// finishes.
fn convert(dir: &Path, keys: &focal_seal::DataKeys, bound: usize) -> Result<(), BusError> {
    for prefix in ["node", "client"] {
        for index in 1..=bound {
            let name = format!("{prefix}-{index}.key");
            if !file(dir, &name).exists() {
                break;
            }
            let (key, clear) = read_key(dir, keys, &name, true)?;
            if clear {
                write_key(dir, keys, &name, &key)?;
            }
        }
    }
    write_private(dir, SEALED_FILE, &[])?;
    focal_platform::sync_dir(dir)
        .map_err(|_| BusError::Configuration("the identity directory could not be synced"))
}

/// What a make cut before its authority left, removed: each identity's files, filed from 1 up,
/// nodes' and clients' alike, with the staged copies a cut rename leaves, until each sequence ends,
/// each at most `bound` long, and the mark. A directory with no authority was never read whole, so
/// nothing in it was ever served; a key an earlier binary wrote there in the clear, one this make
/// files no more among them, would otherwise lie in the clear for good (29 §12.9).
fn sweep(dir: &Path, bound: usize) -> Result<(), BusError> {
    let remove = |name: &str| match std::fs::remove_file(file(dir, name)) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(_) => Err(BusError::Configuration(
            "the identity directory could not be swept",
        )),
    };
    for prefix in ["node", "client"] {
        for index in 1..=bound {
            let stem = format!("{prefix}-{index}");
            let mut found = false;
            for name in [
                format!("{stem}.key"),
                format!("{stem}.der"),
                format!(".{stem}.key.new"),
                format!(".{stem}.der.new"),
            ] {
                found |= remove(&name)?;
            }
            if !found {
                break;
            }
        }
    }
    for name in [SEALED_FILE, ".SEALED.new", ".root.der.new"] {
        remove(name)?;
    }
    focal_platform::sync_dir(dir)
        .map_err(|_| BusError::Configuration("the identity directory could not be synced"))
}

fn write_private(dir: &Path, name: &str, bytes: &[u8]) -> Result<(), BusError> {
    let staged = file(dir, &format!(".{name}.new"));
    let target = file(dir, name);
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let _ = std::fs::remove_file(&staged);
    let mut handle = options
        .open(&staged)
        .map_err(|_| BusError::Configuration("the identity directory is not writable"))?;
    std::io::Write::write_all(&mut handle, bytes)
        .and_then(|()| handle.sync_all())
        .map_err(|_| BusError::Configuration("an identity file could not be written"))?;
    std::fs::rename(&staged, &target)
        .map_err(|_| BusError::Configuration("an identity file could not be put in place"))
}

fn read(dir: &Path, name: &str) -> Result<Vec<u8>, BusError> {
    std::fs::read(file(dir, name))
        .map_err(|_| BusError::Configuration("an identity file is missing or unreadable"))
}

/// The identities in `dir`, made there first if it holds no authority: `nodes` nodes' and `clients`
/// clients'. A node bounds `clients` by what its memory admits (`serve::most_clients`). The
/// directory's keys are kept under `key_file` where its owner names one, else under the per-user
/// root key file named for the directory's root key (29 §12.9); a directory an earlier binary wrote
/// in the clear is sealed here first.
pub fn load_or_make(
    dir: &Path,
    key_file: Option<&Path>,
    nodes: usize,
    clients: usize,
) -> Result<Local, BusError> {
    if clients == 0 || nodes == 0 {
        return Err(BusError::Configuration(
            "a local directory holds at least one node and one client identity",
        ));
    }
    focal_platform::fs::create_dir_all_private(dir)
        .map_err(|_| BusError::Configuration("the identity directory could not be made"))?;
    let _lock = lock(dir)?;
    let keys = keys(dir, key_file)?;
    // The identities a directory holds were bounded by its maker's memory, as this node's clients
    // are by its own (`serve::most_clients`): a sequence past what this machine admits and what this
    // opener asks was made by nothing here.
    let envelope = focal_platform::memory_envelope()
        .and_then(|bytes| usize::try_from(bytes).ok())
        .unwrap_or(0);
    let bound = crate::serve::most_clients(envelope)
        .saturating_add(nodes)
        .saturating_add(clients);
    if !file(dir, "root.der").exists() {
        sweep(dir, bound)?;
        make(dir, &keys, nodes, clients)?;
    } else if !file(dir, SEALED_FILE).exists() {
        convert(dir, &keys, bound)?;
    }
    let root = read(dir, "root.der")?;
    let read_identity = |name: &str| -> Result<Identity, BusError> {
        Ok(Identity {
            certificate: read(dir, &format!("{name}.der"))?,
            key: read_key(dir, &keys, &format!("{name}.key"), false)?.0,
        })
    };
    let mut node_identities = Vec::new();
    for n in 1..=nodes {
        node_identities.push(read_identity(&format!("node-{n}"))?);
    }
    let mut made = Vec::new();
    for index in 1..=clients {
        made.push(read_identity(&format!("client-{index}"))?);
    }
    Ok(Local {
        root,
        nodes: node_identities,
        clients: made,
    })
}

fn make(
    dir: &Path,
    keys: &focal_seal::DataKeys,
    nodes: usize,
    clients: usize,
) -> Result<(), BusError> {
    let failed = |_| BusError::Configuration("an identity could not be made");
    let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).map_err(failed)?;
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let key = rcgen::KeyPair::generate().map_err(failed)?;
    let root = params.self_signed(&key).map_err(failed)?;
    let issuer = rcgen::Issuer::new(params, key);
    let issue = |name: &str| -> Result<(Vec<u8>, Vec<u8>), BusError> {
        let params = rcgen::CertificateParams::new(vec![name.to_owned()]).map_err(failed)?;
        let key = rcgen::KeyPair::generate().map_err(failed)?;
        let certificate = params.signed_by(&key, &issuer).map_err(failed)?;
        Ok((certificate.der().to_vec(), key.serialize_der()))
    };
    for n in 1..=nodes {
        let n = u64::try_from(n).map_err(|_| BusError::Configuration("nodes"))?;
        let (node, node_key) = issue(&node_name(n))?;
        write_private(dir, &format!("node-{n}.der"), &node)?;
        write_key(dir, keys, &format!("node-{n}.key"), &node_key)?;
    }
    for index in 1..=clients {
        let (certificate, key) = issue(&format!("focal-bus-client-{index}"))?;
        write_private(dir, &format!("client-{index}.der"), &certificate)?;
        write_key(dir, keys, &format!("client-{index}.key"), &key)?;
    }
    // Every key sealed: the directory is marked so before the authority, whose presence says the
    // directory is whole, and every name made durable with it.
    write_private(dir, SEALED_FILE, &[])?;
    write_private(dir, "root.der", root.der())?;
    focal_platform::sync_dir(dir)
        .map_err(|_| BusError::Configuration("the identity directory could not be synced"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const NODES: usize = 3;
    const CLIENTS: usize = 2;

    /// The key files a directory of [`NODES`] and [`CLIENTS`] holds, nodes' first.
    fn key_names() -> Vec<String> {
        (1..=NODES)
            .map(|n| format!("node-{n}.key"))
            .chain((1..=CLIENTS).map(|k| format!("client-{k}.key")))
            .collect()
    }

    /// A directory as a binary before 29 §12.9 made it: each key a DER document in the clear, the
    /// authority written last, no mark. Its keys, in [`key_names`]'s order.
    fn made_in_the_clear(dir: &Path) -> Vec<Vec<u8>> {
        std::fs::create_dir_all(dir).unwrap();
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let key = rcgen::KeyPair::generate().unwrap();
        let root = params.self_signed(&key).unwrap();
        let issuer = rcgen::Issuer::new(params, key);
        let mut keys = Vec::new();
        for (stem, name) in (1..=NODES)
            .map(|n| (format!("node-{n}"), node_name(n as u64)))
            .chain((1..=CLIENTS).map(|k| (format!("client-{k}"), format!("focal-bus-client-{k}"))))
        {
            let params = rcgen::CertificateParams::new(vec![name]).unwrap();
            let key = rcgen::KeyPair::generate().unwrap();
            let certificate = params.signed_by(&key, &issuer).unwrap();
            write_private(dir, &format!("{stem}.der"), certificate.der()).unwrap();
            write_private(dir, &format!("{stem}.key"), &key.serialize_der()).unwrap();
            keys.push(key.serialize_der());
        }
        write_private(dir, "root.der", root.der()).unwrap();
        keys
    }

    /// Every key of `local`, in [`key_names`]'s order.
    fn keys_of(local: &Local) -> Vec<Vec<u8>> {
        local
            .nodes
            .iter()
            .chain(&local.clients)
            .map(|identity| identity.key.clone())
            .collect()
    }

    /// Every key file of `dir` is sealed: no DER document, none of its key's bytes, and a frame of
    /// its own name under the directory's journal key.
    fn assert_sealed(dir: &Path, keys: &[Vec<u8>]) {
        assert!(
            file(dir, SEALED_FILE).exists(),
            "the directory is marked sealed"
        );
        let journal = super::keys(dir, None)
            .unwrap()
            .key(focal_seal::Store::Journal)
            .unwrap();
        for (name, key) in key_names().iter().zip(keys) {
            let bytes = std::fs::read(file(dir, name)).unwrap();
            assert_ne!(
                bytes.first(),
                Some(&DER_SEQUENCE),
                "{name} lies in the clear"
            );
            assert!(
                !bytes
                    .windows(key.len())
                    .any(|window| window == key.as_slice()),
                "{name} holds its key's bytes"
            );
            let framed =
                focal_seal::file::open(&journal, bytes, key_file_bound().unwrap()).unwrap();
            assert_eq!(&unframe(name, &framed).unwrap(), key, "{name}");
        }
    }

    fn refusal(dir: &Path) -> &'static str {
        match load_or_make(dir, None, NODES, CLIENTS) {
            Err(BusError::Configuration(why)) => why,
            other => panic!("not refused: {other:?}"),
        }
    }

    /// A directory made now keeps every key sealed and marked so, and is read back as made.
    #[test]
    fn a_made_directory_keeps_every_key_sealed() {
        let dir = tempfile::tempdir().unwrap();
        let made = load_or_make(dir.path(), None, NODES, CLIENTS).unwrap();
        assert_sealed(dir.path(), &keys_of(&made));
        let again = load_or_make(dir.path(), None, NODES, CLIENTS).unwrap();
        assert_eq!(keys_of(&again), keys_of(&made));
        assert_eq!(again.root, made.root);
    }

    /// A directory an earlier binary kept in the clear is sealed by its first opener, every key
    /// what it was; a conversion cut part way, some keys sealed and a staged file left behind, no
    /// mark, is finished by the next opener.
    #[test]
    fn a_directory_kept_in_the_clear_is_sealed_by_its_first_opener_whole_or_cut() {
        let dir = tempfile::tempdir().unwrap();
        let clear = made_in_the_clear(dir.path());
        let local = load_or_make(dir.path(), None, NODES, CLIENTS).unwrap();
        assert_eq!(keys_of(&local), clear);
        assert_sealed(dir.path(), &clear);

        let cut = tempfile::tempdir().unwrap();
        let clear = made_in_the_clear(cut.path());
        let keys = super::keys(cut.path(), None).unwrap();
        // A conversion cut after two keys and while a third was staged.
        let names = key_names();
        for (name, key) in names.iter().zip(&clear).take(2) {
            write_key(cut.path(), &keys, name, key).unwrap();
        }
        std::fs::write(file(cut.path(), &format!(".{}.new", names[2])), b"cut").unwrap();
        assert!(!file(cut.path(), SEALED_FILE).exists());
        let local = load_or_make(cut.path(), None, NODES, CLIENTS).unwrap();
        assert_eq!(keys_of(&local), clear);
        assert_sealed(cut.path(), &clear);
    }

    /// A directory an earlier binary's make cut before its authority, its keys in the clear and more
    /// of them than this opener asks for, one more staged, is swept and made: no file of it holds a
    /// key in the clear, and the keys made are sealed.
    #[test]
    fn a_directory_cut_before_its_authority_is_made_with_no_key_left_in_the_clear() {
        let dir = tempfile::tempdir().unwrap();
        made_in_the_clear(dir.path());
        // The earlier binary's make filed two clients more and was cut staging a third, before
        // its authority.
        let extra = rcgen::KeyPair::generate().unwrap().serialize_der();
        for k in CLIENTS + 1..=CLIENTS + 2 {
            write_private(dir.path(), &format!("client-{k}.key"), &extra).unwrap();
            write_private(dir.path(), &format!("client-{k}.der"), b"certificate").unwrap();
        }
        std::fs::write(
            file(dir.path(), &format!(".client-{}.key.new", CLIENTS + 3)),
            &extra,
        )
        .unwrap();
        std::fs::remove_file(file(dir.path(), "root.der")).unwrap();
        let made = load_or_make(dir.path(), None, NODES, CLIENTS).unwrap();
        assert_sealed(dir.path(), &keys_of(&made));
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            let entry = entry.unwrap();
            let name = entry.file_name().into_string().unwrap();
            let bytes = std::fs::read(entry.path()).unwrap();
            // The certificates are public, DER as every handshake shows them.
            if name.contains(".key") {
                assert_ne!(
                    bytes.first(),
                    Some(&DER_SEQUENCE),
                    "{name} lies in the clear"
                );
            }
            assert!(
                !bytes
                    .windows(extra.len())
                    .any(|window| window == extra.as_slice()),
                "{name} holds a key the cut make filed"
            );
        }
    }

    /// From the mark a key in the clear is refused, and a sealed key file changed by a byte, moved
    /// to another key's name, or carried in from another directory, is refused (29 §12.9).
    #[test]
    fn a_key_in_the_clear_after_the_mark_or_a_changed_moved_or_carried_one_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let clear = made_in_the_clear(dir.path());
        load_or_make(dir.path(), None, NODES, CLIENTS).unwrap();
        let sealed = |name: &str| std::fs::read(file(dir.path(), name)).unwrap();
        let (node_1, node_2) = (sealed("node-1.key"), sealed("node-2.key"));

        std::fs::write(file(dir.path(), "node-1.key"), &clear[0]).unwrap();
        assert_eq!(
            refusal(dir.path()),
            "a key lies in the clear after the identity directory was sealed"
        );

        let mut flipped = node_1.clone();
        let last = flipped.len() - 1;
        flipped[last] ^= 1;
        std::fs::write(file(dir.path(), "node-1.key"), &flipped).unwrap();
        assert_eq!(
            refusal(dir.path()),
            "a key file does not open under its directory's keys"
        );

        std::fs::write(file(dir.path(), "node-1.key"), &node_2).unwrap();
        assert_eq!(
            refusal(dir.path()),
            "a key file is not the key its name holds"
        );

        let other = tempfile::tempdir().unwrap();
        load_or_make(other.path(), None, NODES, CLIENTS).unwrap();
        let carried = std::fs::read(file(other.path(), "node-1.key")).unwrap();
        std::fs::write(file(dir.path(), "node-1.key"), &carried).unwrap();
        assert_eq!(
            refusal(dir.path()),
            "a key file does not open under its directory's keys"
        );

        std::fs::write(file(dir.path(), "node-1.key"), &node_1).unwrap();
        let local = load_or_make(dir.path(), None, NODES, CLIENTS).unwrap();
        assert_eq!(keys_of(&local), clear);
    }
}
