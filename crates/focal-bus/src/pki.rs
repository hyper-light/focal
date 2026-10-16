//! A bus node's local identities: an authority of its own, the node's certificate, and one
//! identity a client, kept in a directory as DER files.
//!
//! This is the bus's standalone mode, for a laptop and for measurement: the directory is made at a
//! node's first start (and only then; a directory that holds an authority is used as it is) and
//! read by the clients the node admits. An enrolled deployment names its identities through focal's
//! enrollment instead (doc 32 §5). Every key file is written owner-only, then renamed into place,
//! so a reader never sees half a key.

use std::path::{Path, PathBuf};

use crate::BusError;

/// The name the node's certificate carries, which clients check when they dial it.
pub const NODE_NAME: &str = "focal-bus";

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
    /// The node's identity.
    pub node: Identity,
    /// The clients' identities, client 1 first: client `n` is peer `n + 1`.
    pub clients: Vec<Identity>,
}

impl Local {
    /// The peer a client identity is: the node is peer 1, client `n` (from 1) peer `n + 1`.
    pub fn client_peer(index: usize) -> Option<u64> {
        u64::try_from(index).ok()?.checked_add(2)
    }
}

fn file(dir: &Path, name: &str) -> PathBuf {
    dir.join(name)
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

/// The identities in `dir`, made there first if it holds no authority: the node's and `clients`
/// clients'. A node bounds `clients` by what its memory admits (`serve::most_clients`).
pub fn load_or_make(dir: &Path, clients: usize) -> Result<Local, BusError> {
    if clients == 0 {
        return Err(BusError::Configuration(
            "a local directory holds at least one client identity",
        ));
    }
    if !file(dir, "root.der").exists() {
        make(dir, clients)?;
    }
    let root = read(dir, "root.der")?;
    let node = Identity {
        certificate: read(dir, "node.der")?,
        key: read(dir, "node.key")?,
    };
    let mut made = Vec::new();
    for index in 1..=clients {
        made.push(Identity {
            certificate: read(dir, &format!("client-{index}.der"))?,
            key: read(dir, &format!("client-{index}.key"))?,
        });
    }
    Ok(Local {
        root,
        node,
        clients: made,
    })
}

fn make(dir: &Path, clients: usize) -> Result<(), BusError> {
    let failed = |_| BusError::Configuration("an identity could not be made");
    std::fs::create_dir_all(dir)
        .map_err(|_| BusError::Configuration("the identity directory could not be made"))?;
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
    let (node, node_key) = issue(NODE_NAME)?;
    write_private(dir, "node.der", &node)?;
    write_private(dir, "node.key", &node_key)?;
    for index in 1..=clients {
        let (certificate, key) = issue(&format!("focal-bus-client-{index}"))?;
        write_private(dir, &format!("client-{index}.der"), &certificate)?;
        write_private(dir, &format!("client-{index}.key"), &key)?;
    }
    // The authority last: its presence says the directory is whole.
    write_private(dir, "root.der", root.der())
}
