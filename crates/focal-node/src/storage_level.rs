//! The upgrade fence's level as this node last knew it, kept in its data directory so that its start
//! can choose where its groups live before it opens any of them (27 §15.8, "When it runs").
//!
//! The fence is committed in the root's enrollment registry, which lives in one of the node's own
//! groups: a start that read it there would have opened its storage already. So the node's
//! controller, on observing the committed fence, records its level here, and the next start reads
//! it. A fence never closes, so the record only rises (`record` keeps the higher); a node whose
//! fence opened `STORAGE_LEVEL` while it ran converts at its next start, and one that was down
//! learns it from its root replica and converts at the start after, one restart either way.
use std::path::Path;

use crate::embedded::{NodeError, atomic_file};

/// Format: the record's file under the data directory.
pub const FILE: &str = "STORAGE.level";
/// Format: the record's magic.
const MAGIC: &[u8; 8] = b"FOCALSL1";
/// Format: the record's length: the magic, the level and the CRC-32 of both.
const LEN: usize = 8 + 4 + 4;

/// The level the node last recorded, none if it never did. A record that is there and does not
/// read whole is an error, never none: none would start the node below a fence it already passed.
pub fn known(root: &Path) -> Result<Option<u32>, NodeError> {
    let path = root.join(FILE);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let corrupt = || NodeError::Corrupt("the storage level record does not read whole");
    if bytes.len() != LEN {
        return Err(corrupt());
    }
    let (body, sum) = bytes.split_at_checked(12).ok_or_else(corrupt)?;
    let (magic, level) = body.split_at_checked(8).ok_or_else(corrupt)?;
    let sum = u32::from_le_bytes(sum.try_into().map_err(|_| corrupt())?);
    if magic != MAGIC || crc32fast::hash(body) != sum {
        return Err(corrupt());
    }
    Ok(Some(u32::from_le_bytes(
        level.try_into().map_err(|_| corrupt())?,
    )))
}

/// Records that the committed fence is at `level`, unless a higher level is recorded already: the
/// record never falls. Durable when it returns.
pub fn record(root: &Path, level: u32) -> Result<(), NodeError> {
    if known(root)?.is_some_and(|known| known >= level) {
        return Ok(());
    }
    let mut bytes = [0u8; LEN];
    let (body, sum) = bytes.split_at_mut(12);
    let (magic, at) = body.split_at_mut(8);
    magic.copy_from_slice(MAGIC);
    at.copy_from_slice(&level.to_le_bytes());
    sum.copy_from_slice(&crc32fast::hash(body).to_le_bytes());
    atomic_file(&root.join(FILE), &bytes)?;
    Ok(())
}

/// Whether the node's groups live on the shell at its start: the fence it last knew opens
/// `STORAGE_LEVEL`.
pub fn storage_opened(root: &Path) -> Result<bool, NodeError> {
    Ok(known(root)?.is_some_and(|level| level >= crate::upgrade::STORAGE_LEVEL))
}

/// The fields beyond raft-rs's a member opened now carries (27 §15.9): `Wire::Kept` once the fence
/// the node last knew opens `RAFT_KEPT_LEVEL`, raft-rs's alone below it. Read as each member is
/// opened, before it sends, so a member opened under the fence sends under it from its first
/// message, the founder's and each joiner's alike; a member already open when the fence rose is
/// raised at the node's next start, as its storage is converted then. Members raised at different
/// moments are the case §15.9 designs for: a raised member's `kept` is skipped by one not yet
/// raised, which answers as raft-rs does, at the cost of a resend.
pub fn member_wire(root: &Path) -> Result<focal_consensus::Wire, NodeError> {
    Ok(
        if known(root)?.is_some_and(|level| level >= crate::upgrade::RAFT_KEPT_LEVEL) {
            focal_consensus::Wire::Kept
        } else {
            focal_consensus::Wire::Frozen
        },
    )
}

/// What a session's hosting follows at the node's level: when it seals, how it retires, its
/// native share, and the form its stores beside the log keep at rest
/// (`crate::network_service::native_hosting`).
#[derive(Debug, Clone, Copy)]
pub struct Rules {
    pub seal: focal_ledger::SealRule,
    pub retire: focal_ledger::RetireRule,
    pub share: focal_ledger::NativeShare,
    pub at_rest: focal_evidence::ContentForm,
}

/// How the sessions a node hosts seal (F12, 26 §4a): by the anchored rule once the fence it last
/// knew opens `SEAL_LEVEL`, as a node founded there does from its first start; by the derived
/// rule below it. Read as each session's hosting is built: a node that saw the fence rise while it
/// ran takes the anchored rule at its next start, and every binary at the level applies the
/// records of both rules meanwhile.
pub fn seal_rule(root: &Path) -> Result<focal_ledger::SealRule, NodeError> {
    Ok(
        if known(root)?.is_some_and(|level| level >= crate::upgrade::SEAL_LEVEL) {
            focal_ledger::SealRule::Anchored
        } else {
            focal_ledger::SealRule::Derived
        },
    )
}

/// How the sessions a node hosts retire (doc 31 M2): in batches among their writes once the fence
/// it last knew opens `RETIRE_LEVEL`, as a node founded there does from its first start; a family
/// at a time below it. Read as each session's hosting is built, as [`seal_rule`] is.
pub fn retire_rule(root: &Path) -> Result<focal_ledger::RetireRule, NodeError> {
    Ok(retire_rule_at(known(root)?.unwrap_or(0)))
}

/// How much of a session's allowance the native engines a node hosts admit within (doc 31 M3):
/// the wide share once the fence it last knew opens `SHARE_LEVEL`, as a node founded there does
/// from its first start; half below it. Read as each session's hosting is built, as
/// [`seal_rule`] is; every binary at the level applies within the whole allowance meanwhile.
pub fn native_share(root: &Path) -> Result<focal_ledger::NativeShare, NodeError> {
    Ok(native_share_at(known(root)?.unwrap_or(0)))
}

/// The native share a fence at `level` opens.
pub fn native_share_at(level: u32) -> focal_ledger::NativeShare {
    if level >= crate::upgrade::SHARE_LEVEL {
        focal_ledger::NativeShare::Wide
    } else {
        focal_ledger::NativeShare::Half
    }
}

/// The form a node's content store installs its files in (29 §11): sealed once the fence it last
/// knew opens `CONTENT_LEVEL`, as a node founded there does from its first start; in the clear
/// below it, where a binary that cannot open a sealed file may still be rolled back to. Read as the
/// store is opened at the node's start: a node that saw the fence rise while it ran seals from its
/// next start, converting what it holds in the clear as it serves.
pub fn content_form(root: &Path) -> Result<focal_evidence::ContentForm, NodeError> {
    Ok(content_form_at(known(root)?.unwrap_or(0)))
}

/// The content form a fence at `level` opens.
pub fn content_form_at(level: u32) -> focal_evidence::ContentForm {
    if level >= crate::upgrade::CONTENT_LEVEL {
        focal_evidence::ContentForm::Sealed
    } else {
        focal_evidence::ContentForm::Clear
    }
}

/// The form a node's stores beside its log and content keep at rest (29 §12), its sessions' seeds
/// first: sealed once the fence it last knew opens `AT_REST_LEVEL`, as a node founded there does
/// from its first start; in the clear below it, where a binary that cannot open a sealed file may
/// still be rolled back to. Read as each store is opened: a store opened after the fence rose
/// seals from then on, converting what it holds in the clear as it serves.
pub fn at_rest_form(root: &Path) -> Result<focal_evidence::ContentForm, NodeError> {
    Ok(at_rest_form_at(known(root)?.unwrap_or(0)))
}

/// The fence the client stores at `root`, in the data directory `data_dir`, are held to (29
/// §12.11): the higher of the fence the data directory records (its node's, where a node lives in
/// it) and the one recorded beside the stores (the fence a client's context learned from the
/// cluster it addresses, `root` its history). A fence never falls, so neither does this.
pub fn client_level(data_dir: &Path, root: &Path) -> Result<u32, NodeError> {
    let own = known(data_dir)?.unwrap_or(0);
    if root == data_dir {
        return Ok(own);
    }
    Ok(own.max(known(root)?.unwrap_or(0)))
}

/// How client stores held to the fence at `level` keep their records at rest (29 §12.11), under
/// their data directory's keys `keys` as found there: sealed once the level opens the at-rest form,
/// in the clear below it. A store past the level without keys is refused: it could be neither
/// sealed nor read.
pub fn client_at_rest(
    level: u32,
    keys: Option<focal_seal::DataKeys>,
) -> Result<focal_client::at_rest::AtRest, NodeError> {
    let seal = at_rest_form_at(level) == focal_evidence::ContentForm::Sealed;
    if seal && keys.is_none() {
        return Err(NodeError::ClientStoresWithoutKeys);
    }
    Ok(focal_client::at_rest::AtRest::new(keys, seal))
}

/// How the client stores at `root` in the data directory `data_dir` keep their records at rest
/// ([`client_level`], [`client_at_rest`]), with the keys `settings` finds there, made nowhere: a
/// node's start made a node's, a client's enrollment a client's.
pub fn client_at_rest_in(
    settings: &crate::config::Settings,
    data_dir: &Path,
    root: &Path,
) -> Result<focal_client::at_rest::AtRest, NodeError> {
    client_at_rest(
        client_level(data_dir, root)?,
        settings.found_data_keys(data_dir)?,
    )
}

/// The at-rest form a fence at `level` opens.
pub fn at_rest_form_at(level: u32) -> focal_evidence::ContentForm {
    if level >= crate::upgrade::AT_REST_LEVEL {
        focal_evidence::ContentForm::Sealed
    } else {
        focal_evidence::ContentForm::Clear
    }
}

/// The retire rule a fence at `level` opens: to the cold stream from `COLD_LEVEL`, in batches
/// from `RETIRE_LEVEL`, a family at a time below.
pub fn retire_rule_at(level: u32) -> focal_ledger::RetireRule {
    if level >= crate::upgrade::COLD_LEVEL {
        focal_ledger::RetireRule::Cold
    } else if level >= crate::upgrade::RETIRE_LEVEL {
        focal_ledger::RetireRule::Pipeline
    } else {
        focal_ledger::RetireRule::Exact
    }
}

#[cfg(test)]
#[cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects
    )
)]
mod tests {
    use super::*;

    /// None until recorded; a record rises and never falls; a damaged record is an error, never
    /// none; the storage level opens at `STORAGE_LEVEL`.
    #[test]
    fn the_known_level_rises_never_falls_and_is_never_read_as_absent() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(known(dir.path()).unwrap(), None);
        assert!(!storage_opened(dir.path()).unwrap());
        record(dir.path(), 3).unwrap();
        assert_eq!(known(dir.path()).unwrap(), Some(3));
        assert!(!storage_opened(dir.path()).unwrap());
        record(dir.path(), crate::upgrade::STORAGE_LEVEL).unwrap();
        assert!(storage_opened(dir.path()).unwrap());
        record(dir.path(), 2).unwrap();
        assert_eq!(
            known(dir.path()).unwrap(),
            Some(crate::upgrade::STORAGE_LEVEL)
        );
        let mut bytes = std::fs::read(dir.path().join(FILE)).unwrap();
        bytes[9] ^= 1;
        std::fs::write(dir.path().join(FILE), &bytes).unwrap();
        assert!(matches!(known(dir.path()), Err(NodeError::Corrupt(_))));
        assert!(storage_opened(dir.path()).is_err());
        std::fs::write(dir.path().join(FILE), b"short").unwrap();
        assert!(matches!(known(dir.path()), Err(NodeError::Corrupt(_))));
    }

    /// A member runs raft-rs's fields alone until the recorded fence opens `RAFT_KEPT_LEVEL`, and
    /// the fields beyond them from there; a damaged record opens no member.
    #[test]
    fn a_member_runs_kept_once_the_recorded_fence_opens_its_level() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(
            member_wire(dir.path()).unwrap(),
            focal_consensus::Wire::Frozen
        );
        record(dir.path(), crate::upgrade::RAFT_KEPT_LEVEL - 1).unwrap();
        assert_eq!(
            member_wire(dir.path()).unwrap(),
            focal_consensus::Wire::Frozen
        );
        record(dir.path(), crate::upgrade::RAFT_KEPT_LEVEL).unwrap();
        assert_eq!(
            member_wire(dir.path()).unwrap(),
            focal_consensus::Wire::Kept
        );
        record(dir.path(), crate::upgrade::STORAGE_LEVEL).unwrap();
        assert_eq!(
            member_wire(dir.path()).unwrap(),
            focal_consensus::Wire::Kept
        );
        std::fs::write(dir.path().join(FILE), b"short").unwrap();
        assert!(member_wire(dir.path()).is_err());
    }

    /// A node's native engines admit within half their sessions' allowances until the fence it
    /// last knew opens `SHARE_LEVEL`, and within the wide share from there (doc 31 M3); a node
    /// that knows no fence, or a damaged record, opens none wide.
    #[test]
    fn native_engines_admit_wide_once_the_recorded_fence_opens_the_share_level() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(
            native_share(dir.path()).unwrap(),
            focal_ledger::NativeShare::Half
        );
        record(dir.path(), crate::upgrade::SHARE_LEVEL - 1).unwrap();
        assert_eq!(
            native_share(dir.path()).unwrap(),
            focal_ledger::NativeShare::Half
        );
        record(dir.path(), crate::upgrade::SHARE_LEVEL).unwrap();
        assert_eq!(
            native_share(dir.path()).unwrap(),
            focal_ledger::NativeShare::Wide
        );
        assert_eq!(
            native_share_at(crate::upgrade::CAPABILITY_LEVEL),
            focal_ledger::NativeShare::Wide
        );
        assert_eq!(native_share_at(0), focal_ledger::NativeShare::Half);
        std::fs::write(dir.path().join(FILE), b"short").unwrap();
        assert!(native_share(dir.path()).is_err());
    }

    /// A node's content store keeps the clear until the fence it last knew opens `CONTENT_LEVEL`,
    /// and seals from there (29 §11); a node that knows no fence keeps the clear, and a damaged
    /// record opens no store.
    #[test]
    fn the_content_store_seals_once_the_recorded_fence_opens_the_content_level() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(
            content_form(dir.path()).unwrap(),
            focal_evidence::ContentForm::Clear
        );
        record(dir.path(), crate::upgrade::CONTENT_LEVEL - 1).unwrap();
        assert_eq!(
            content_form(dir.path()).unwrap(),
            focal_evidence::ContentForm::Clear
        );
        record(dir.path(), crate::upgrade::CONTENT_LEVEL).unwrap();
        assert_eq!(
            content_form(dir.path()).unwrap(),
            focal_evidence::ContentForm::Sealed
        );
        assert_eq!(
            content_form_at(crate::upgrade::CAPABILITY_LEVEL),
            focal_evidence::ContentForm::Sealed
        );
        assert_eq!(content_form_at(0), focal_evidence::ContentForm::Clear);
        std::fs::write(dir.path().join(FILE), b"short").unwrap();
        assert!(content_form(dir.path()).is_err());
    }

    /// A node's stores beside its log and content keep the clear until the fence it last recorded
    /// opens the at-rest level, and seal from it (29 §12): the content level opens nothing of them.
    #[test]
    fn the_stores_beside_the_log_seal_once_the_recorded_fence_opens_the_at_rest_level() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(
            at_rest_form(dir.path()).unwrap(),
            focal_evidence::ContentForm::Clear
        );
        record(dir.path(), crate::upgrade::CONTENT_LEVEL).unwrap();
        assert_eq!(
            content_form(dir.path()).unwrap(),
            focal_evidence::ContentForm::Sealed
        );
        assert_eq!(
            at_rest_form(dir.path()).unwrap(),
            focal_evidence::ContentForm::Clear
        );
        record(dir.path(), crate::upgrade::AT_REST_LEVEL).unwrap();
        assert_eq!(
            at_rest_form(dir.path()).unwrap(),
            focal_evidence::ContentForm::Sealed
        );
        assert_eq!(
            at_rest_form_at(crate::upgrade::CAPABILITY_LEVEL),
            focal_evidence::ContentForm::Sealed
        );
        assert_eq!(
            at_rest_form_at(crate::upgrade::AT_REST_LEVEL - 1),
            focal_evidence::ContentForm::Clear
        );
        std::fs::write(dir.path().join(FILE), b"short").unwrap();
        assert!(at_rest_form(dir.path()).is_err());
    }
}
