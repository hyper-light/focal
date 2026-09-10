//! Backups of a hosted session at a declared prefix, and their verification
//! (26 §6). The replica exports the exact envelope it installed durably;
//! the node writes it, the seeds it names and the exact trees of every
//! object its prefix names under the operator's directory, the manifest
//! last. Verification reads only the backup and runs without a node.
use crate::fleet::FleetManager;
use focal_client::admin::{AdminBackup, AdminBackupPrefix, AdminBackupVerification, AdminRestore};
use focal_ledger::backup::{self, BackupError, BackupImage, BackupManifest, FileMedium};
use focal_memory::{BudgetKind, BudgetLane, MemoryBudget};
use focal_model::LedgerId;
use focal_wire::AccessError;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// The lease the export is held under while its bytes are copied out.
const EXPORT_TTL: Duration = Duration::from_secs(30);
/// Memory a verification may draw on: the envelope, one assembled Core root
/// and the restored rows.
const VERIFY_BUDGET_BYTES: usize = 512 * 1024 * 1024;
const VERIFY_COMPLETION_BYTES: usize = 128 * 1024 * 1024;

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
fn now_ms() -> Result<u64, AccessError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| u64::try_from(elapsed.as_millis()).ok())
        .ok_or(AccessError::Unavailable)
}
fn prefix(manifest: &BackupManifest) -> AdminBackupPrefix {
    let prefix = &manifest.prefix;
    AdminBackupPrefix {
        cluster: hex(&prefix.cluster),
        tenant: prefix.ledger.tenant.to_string(),
        session: prefix.ledger.session.to_string(),
        group: hex(&prefix.group.0),
        node: prefix.node,
        sequence: prefix.sequence.0,
        native_sequence: manifest.native_sequence,
        index: prefix.index.0,
        term: prefix.term.0,
        route_epoch: prefix.route.0,
        placement_epoch: prefix.placement_epoch,
        membership_epoch: prefix.membership_epoch,
    }
}
fn ledger_error(error: focal_ledger::LedgerError) -> AccessError {
    match error {
        focal_ledger::LedgerError::Capacity | focal_ledger::LedgerError::Memory(_) => {
            AccessError::Capacity
        }
        // A session not yet registered with the directory has no committed
        // placement to export under; it will, so the operator retries.
        focal_ledger::LedgerError::NotReady { .. }
        | focal_ledger::LedgerError::OutcomeUnknown
        | focal_ledger::LedgerError::PlacementConflict => AccessError::Unavailable,
        _ => AccessError::InvalidRequest,
    }
}
fn backup_error(error: BackupError) -> AccessError {
    match error {
        BackupError::Capacity | BackupError::Memory(_) => AccessError::Capacity,
        BackupError::Exists => AccessError::InvalidRequest,
        BackupError::Io(_) => AccessError::Unavailable,
        _ => AccessError::InvalidRequest,
    }
}

/// Write a backup of the session this node hosts for `ledger` into
/// `output`, a directory that must not yet hold a backup. The replica takes
/// and fsyncs a checkpoint at its applied prefix and exports the exact
/// bytes; the files are written on a blocking thread from the node's
/// read-only content and seed readers.
pub async fn create(
    fleet: &FleetManager,
    data_dir: &Path,
    ledger: LedgerId,
    output: PathBuf,
    budget: &MemoryBudget,
) -> Result<AdminBackup, AccessError> {
    let host = fleet
        .current_host(ledger)
        .map_err(|_| AccessError::Unavailable)?;
    let snapshot = host
        .checkpoint_evidence(EXPORT_TTL)
        .await
        .map_err(ledger_error)?;
    let checkpoint_bytes = snapshot.checkpoint().len();
    let _copy = budget
        .reserve(
            BudgetKind::Recovery,
            BudgetLane::Completion,
            checkpoint_bytes
                .checked_add(64 * 1024)
                .ok_or(AccessError::Capacity)?,
        )
        .map_err(|_| AccessError::Capacity)?;
    let mut checkpoint = Vec::new();
    checkpoint
        .try_reserve_exact(checkpoint_bytes)
        .map_err(|_| AccessError::Capacity)?;
    checkpoint.extend_from_slice(snapshot.checkpoint());
    let image = BackupImage {
        checkpoint,
        prefix: snapshot.prefix().clone(),
        configuration: snapshot.placement().configuration().clone(),
    };
    // The export's pin is released before the files are written: the
    // bytes are ours, the replica continues.
    drop(snapshot);
    let domain = focal_model::ContentDomainId(ledger.tenant.0);
    let limits =
        crate::network_service::native_limits(domain).map_err(|_| AccessError::Unavailable)?;
    let reader = focal_evidence::ContentReader::open(data_dir.join("content"))
        .map_err(|_| AccessError::Unavailable)?;
    let seeds = focal_evidence::SeedReader::open(crate::custody::seed_directory(
        &data_dir.join("seeds"),
        ledger,
    ))
    .map_err(|_| AccessError::Unavailable)?;
    let work = budget
        .child(VERIFY_BUDGET_BYTES, VERIFY_COMPLETION_BYTES)
        .map_err(|_| AccessError::Capacity)?;
    let now = now_ms()?;
    let target = output.clone();
    let report = tokio::task::spawn_blocking(move || {
        let mut medium = FileMedium;
        backup::write(
            &mut medium,
            &target,
            &image,
            &seeds,
            &reader,
            backup::decoder_pair(),
            &limits,
            &work,
            now,
        )
    })
    .await
    .map_err(|_| AccessError::Unavailable)?
    .map_err(backup_error)?;
    let manifest = &report.manifest;
    let chunks = manifest
        .content
        .iter()
        .flat_map(|object| object.chunks.iter().map(|chunk| chunk.hash))
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    Ok(AdminBackup {
        output: output.to_string_lossy().into_owned(),
        created_ms: manifest.created_ms,
        prefix: prefix(manifest),
        checkpoint_hash: hex(&manifest.checkpoint_hash.0),
        checkpoint_bytes: manifest.checkpoint_bytes,
        decoder: hex(&manifest.decoder_successor),
        seeds: u64::try_from(manifest.seeds.len()).unwrap_or(u64::MAX),
        objects: u64::try_from(manifest.content.len()).unwrap_or(u64::MAX),
        chunks: u64::try_from(chunks).unwrap_or(u64::MAX),
        bundles: report.bundles,
        files: report.files,
        bytes: report.bytes,
    })
}

/// Verify a backup directory (26 §6) with this binary's decoder, under its
/// own memory budget; nothing is written and no node is consulted.
pub fn verify(input: &Path) -> Result<AdminBackupVerification, BackupError> {
    let budget = MemoryBudget::new(VERIFY_BUDGET_BYTES, VERIFY_COMPLETION_BYTES)
        .map_err(|_| BackupError::Capacity)?;
    let report = backup::verify(&FileMedium, input, backup::decoder_pair().1, &budget)?;
    let complete = report.complete() && report.decoder_supported;
    let manifest = &report.manifest;
    Ok(AdminBackupVerification {
        input: input.to_string_lossy().into_owned(),
        prefix: prefix(manifest),
        checkpoint_verified: report.checkpoint_verified,
        seeds_listed: u64::try_from(manifest.seeds.len()).unwrap_or(u64::MAX),
        seeds_verified: report.seeds_verified,
        objects_listed: u64::try_from(manifest.content.len()).unwrap_or(u64::MAX),
        objects_verified: report.objects_verified,
        chunks_verified: report.chunks_verified,
        bytes_verified: report.bytes_verified,
        inventory_matches: report.inventory_matches,
        decoder_supported: report.decoder_supported,
        complete,
        problems: report.problems,
    })
}

/// Read a backup's manifest alone.
pub fn read_manifest(input: &Path) -> Result<BackupManifest, BackupError> {
    use focal_ledger::backup::BackupMedium as _;
    let bytes = FileMedium
        .read(
            &input.join(backup::MANIFEST_FILE),
            backup::MAX_MANIFEST_BYTES,
        )
        .map_err(|_| BackupError::Missing("manifest"))?;
    BackupManifest::decode(&bytes)
}

/// Whether a restored session may continue the backup's incarnation or
/// must begin a new one (26 §6). The old incarnation continues only when
/// its source is fenced: the backup came from this cluster and every other
/// member of the membership it was written under has had its enrollment
/// revoked, so no copy of the old log can act again. Otherwise the restore
/// is a recovery incarnation under a new log group and genesis.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestoreDecision {
    SameIncarnation {
        /// The enrollment revision at which the last member was revoked.
        fenced_by: u64,
    },
    RecoveryIncarnation {
        /// Members of the backup's membership that are not fenced.
        unfenced: Vec<u64>,
    },
}
pub fn decide(
    manifest: &BackupManifest,
    cluster: [u8; 16],
    node: u64,
    registry: &focal_enrollment::EnrollmentRegistry,
) -> RestoreDecision {
    let configuration = &manifest.configuration;
    let mut members: Vec<u64> = configuration
        .voters
        .iter()
        .chain(&configuration.learners)
        .chain(&configuration.voters_outgoing)
        .chain(&configuration.learners_next)
        .copied()
        .filter(|member| *member != node)
        .collect();
    members.sort_unstable();
    members.dedup();
    if manifest.prefix.cluster != cluster {
        return RestoreDecision::RecoveryIncarnation { unfenced: members };
    }
    let unfenced: Vec<u64> = members
        .into_iter()
        .filter(|member| {
            !registry.enrollments().any(|receipt| {
                receipt.identity.role == focal_enrollment::EnrollmentRole::Node
                    && receipt.identity.node_id == Some(*member)
                    && registry
                        .invitation_revoked(receipt.invitation)
                        .is_ok_and(|revoked| revoked)
            })
        })
        .collect();
    if unfenced.is_empty() {
        RestoreDecision::SameIncarnation {
            fenced_by: registry.revision(),
        }
    } else {
        RestoreDecision::RecoveryIncarnation { unfenced }
    }
}
/// The log group a recovery incarnation takes: derived from the backup's
/// group, its envelope and the restoring node, so a repeated restore of
/// the same backup on the same node names the same group.
pub fn recovery_group(manifest: &BackupManifest, node: u64) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new_derive_key("focal.session.recovery-group.v1");
    hasher.update(&manifest.prefix.group.0);
    hasher.update(&manifest.checkpoint_hash.0);
    hasher.update(&node.to_le_bytes());
    let digest = hasher.finalize();
    let mut group = [0u8; 16];
    group.copy_from_slice(&digest.as_bytes()[..16]);
    group
}
/// An operator's restore request (26 §6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreRequest {
    pub input: PathBuf,
    pub new_incarnation: bool,
    pub decision: RestoreDecision,
}
/// What a restore produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoredSession {
    pub ledger: LedgerId,
    pub group: [u8; 16],
    pub node: u64,
    pub decision: RestoreDecision,
    pub manifest: BackupManifest,
    pub objects_imported: u64,
    pub seeds_installed: u64,
}
impl RestoredSession {
    pub fn admin(&self, input: &Path) -> AdminRestore {
        let (decision, fenced_by, unfenced) = match &self.decision {
            RestoreDecision::SameIncarnation { fenced_by } => {
                ("same_incarnation".to_owned(), Some(*fenced_by), Vec::new())
            }
            RestoreDecision::RecoveryIncarnation { unfenced } => {
                ("recovery_incarnation".to_owned(), None, unfenced.clone())
            }
        };
        AdminRestore {
            input: input.to_string_lossy().into_owned(),
            tenant: self.ledger.tenant.to_string(),
            session: self.ledger.session.to_string(),
            group: hex(&self.group),
            node: self.node,
            decision,
            fenced_by,
            unfenced,
            prefix: prefix(&self.manifest),
            objects_imported: self.objects_imported,
            seeds_installed: self.seeds_installed,
        }
    }
}
