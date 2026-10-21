use super::*;
use crate::embedded::{NodeIdentity, decode_identity};
use focal_model::ParticipantId;

/// Select the local Unix principal without treating lost joined credentials as
/// founder credentials. Persisted NETWORK identifies a joined node even if its
/// JOIN directory and marker were both removed.
pub fn local_unix_principal(
    root: &Path,
    keys: Option<&focal_seal::DataKeys>,
    identity: &NodeIdentity,
    now: i64,
) -> Result<ParticipantId, JoinError> {
    let state = NetworkState::load_from(root, identity)?;
    let mut joined = state
        .as_ref()
        .is_some_and(|state| state.genesis.founder.node != identity.node);
    for name in ["JOIN", "JOIN.initialized"] {
        match fs::symlink_metadata(root.join(name)) {
            Ok(_) => joined = true,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    if joined {
        joined_unix_principal(root, keys, identity, now)
    } else if decode_identity(&root.join("IDENTITY"))? == *identity {
        Ok(identity.issuer)
    } else {
        Err(JoinError::Conflict)
    }
}

/// Read the principal that the joined node's Unix listener grants its OS owner.
/// This takes no physical-node/key leases and creates or repairs no state. It
/// does not turn the Node certificate into a remote Actor credential.
pub fn joined_unix_principal(
    root: &Path,
    keys: Option<&focal_seal::DataKeys>,
    identity: &NodeIdentity,
    now: i64,
) -> Result<ParticipantId, JoinError> {
    if decode_identity(&root.join("IDENTITY"))? != *identity
        || crate::embedded::read_bounded(&root.join("JOIN.initialized"), 64)?.as_slice()
            != b"durable join intent installed"
    {
        return Err(JoinError::Invalid);
    }
    let journal_bytes = read_record(&root.join("JOIN"), "journal.bin", keys)?;
    let (journal, trailing): (JoinJournal, _) = postcard::take_from_bytes(&journal_bytes)?;
    if !trailing.is_empty() || journal.schema != 1 {
        return Err(JoinError::Invalid);
    }
    let bundle = validate_journal(&journal)?;
    let pin = journal.key.as_ref().ok_or(JoinError::Pending)?;
    let state = NetworkState::load_from(root, identity)?.ok_or(JoinError::Pending)?;
    if state.genesis != bundle.genesis
        || state.sponsor != *bundle.invitation.trust()
        || state.listen != journal.listen
        || state.advertise != journal.advertise
    {
        return Err(JoinError::Conflict);
    }
    let key_bytes = read_record(&root.join("JOIN/node-key"), "join-key.bin", keys)?;
    let receipt_bytes = read_record(&root.join("JOIN/node-key"), "enrollment.bin", keys)?;
    let (receipt, csr) = JoinKey::inspect_saved(
        &key_bytes,
        &receipt_bytes,
        identity.cluster,
        bundle.invitation.trust().issuers.iter(),
        now,
    )?;
    if receipt.request != pin.request
        || csr != pin.csr
        || receipt.invitation != bundle.invitation.id()
        || receipt.identity.role != EnrollmentRole::Node
        || receipt.identity.node_id != Some(identity.node)
        || identity.node == state.genesis.founder.node
        || receipt.identity.principal == [0; 16]
    {
        return Err(JoinError::Conflict);
    }
    Ok(ParticipantId(receipt.identity.principal))
}

/// The payload of the record `name` of the private directory `directory`, read with no lease on it
/// and opened as the directory opens its records (sealed, 29 §12).
fn read_record(
    directory: &Path,
    name: &str,
    keys: Option<&focal_seal::DataKeys>,
) -> Result<Zeroizing<Vec<u8>>, JoinError> {
    let bytes = read_private(&directory.join(name), 64 * 1024)?;
    focal_enrollment::open_private_record(directory, &bytes, keys).map_err(|_| JoinError::Invalid)
}
