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

use std::path::{Path, PathBuf};

use crate::BusError;

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
/// clients'. A node bounds `clients` by what its memory admits (`serve::most_clients`).
pub fn load_or_make(dir: &Path, nodes: usize, clients: usize) -> Result<Local, BusError> {
    if clients == 0 || nodes == 0 {
        return Err(BusError::Configuration(
            "a local directory holds at least one node and one client identity",
        ));
    }
    if !file(dir, "root.der").exists() {
        make(dir, nodes, clients)?;
    }
    let root = read(dir, "root.der")?;
    let read_identity = |name: &str| -> Result<Identity, BusError> {
        Ok(Identity {
            certificate: read(dir, &format!("{name}.der"))?,
            key: read(dir, &format!("{name}.key"))?,
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

fn make(dir: &Path, nodes: usize, clients: usize) -> Result<(), BusError> {
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
    for n in 1..=nodes {
        let n = u64::try_from(n).map_err(|_| BusError::Configuration("nodes"))?;
        let (node, node_key) = issue(&node_name(n))?;
        write_private(dir, &format!("node-{n}.der"), &node)?;
        write_private(dir, &format!("node-{n}.key"), &node_key)?;
    }
    for index in 1..=clients {
        let (certificate, key) = issue(&format!("focal-bus-client-{index}"))?;
        write_private(dir, &format!("client-{index}.der"), &certificate)?;
        write_private(dir, &format!("client-{index}.key"), &key)?;
    }
    // The authority last: its presence says the directory is whole.
    write_private(dir, "root.der", root.der())
}
