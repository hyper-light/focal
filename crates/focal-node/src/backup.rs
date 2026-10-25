//! Backups of a hosted session at a declared prefix, and their verification
//! (26 §6). The replica exports the exact envelope it installed durably;
//! the node writes it, the seeds it names and the exact trees of every
//! object its prefix names under the operator's directory, the manifest
//! last. Verification reads only the backup and runs without a node.
use crate::fleet::FleetManager;
use focal_client::admin::{AdminBackup, AdminBackupPrefix, AdminBackupVerification, AdminRestore};
use focal_evidence::ContentForm;
use focal_ledger::backup::{
    self, BackupError, BackupFiles, BackupForm, BackupImage, BackupManifest, BackupMedium as _,
    FileMedium,
};
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
/// What a backup's refusal answers a node's caller (29 §12.8): a sealed backup asked of without its
/// key file, or with another, or one that does not open, is the request's fault.
pub(crate) fn access_error(error: BackupError) -> AccessError {
    backup_error(error)
}

/// The form of the backup in `input` (29 §12.8): in the clear where it holds no `ENCRYPTION-INFO`,
/// whatever key file is named, so a backup from below the at-rest level, or from before it, reads
/// as it always did; sealed, its key opened under the operator's key file `key_file`, refused
/// without one ([`BackupError::KeyRequired`]) or with another ([`BackupError::WrongKey`]) before
/// anything else of it is read.
pub fn form(input: &Path, key_file: Option<&Path>) -> Result<BackupForm, BackupError> {
    if !FileMedium.exists(&input.join(backup::ENCRYPTION_INFO_FILE))? {
        return Ok(BackupForm::clear());
    }
    let operator = key_file.map(focal_seal::operator_key).transpose()?;
    BackupForm::of(&FileMedium, input, operator.as_ref())
}

/// The form a backup written into `output` takes at a fence whose at-rest form is `at_rest` (29
/// §12.8): sealed under the operator's key in `key_file` from the at-rest level, in the clear below
/// it as every earlier binary writes it. A key file named below the level, or none at it, is
/// refused: below it a backup must stay readable by the binaries the fence still admits. A
/// directory holding part of a sealed backup a crash cut goes on under that backup's key, where the
/// key file opens it; a directory holding anything else is refused, so no file of another backup
/// lies in the clear beside a sealed one.
fn backup_form(
    output: &Path,
    key_file: Option<&Path>,
    at_rest: ContentForm,
) -> Result<BackupForm, AccessError> {
    let key_file = match (at_rest, key_file) {
        (ContentForm::Clear, None) => return Ok(BackupForm::clear()),
        (ContentForm::Clear, Some(_)) | (ContentForm::Sealed, None) => {
            return Err(AccessError::InvalidRequest);
        }
        (ContentForm::Sealed, Some(key_file)) => key_file,
    };
    let operator = focal_seal::operator_key(key_file).map_err(|_| AccessError::InvalidRequest)?;
    let started = FileMedium
        .exists(&output.join(backup::ENCRYPTION_INFO_FILE))
        .map_err(|_| AccessError::Unavailable)?;
    if started {
        return BackupForm::of(&FileMedium, output, Some(&operator)).map_err(backup_error);
    }
    match std::fs::read_dir(output) {
        Ok(mut entries) => {
            if entries.next().is_some() {
                return Err(AccessError::InvalidRequest);
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return Err(AccessError::Unavailable),
    }
    BackupForm::seal_under(&operator).map_err(backup_error)
}

/// Write a backup of the session this node hosts for `ledger` into
/// `output`, a directory that must not yet hold a backup, sealed under the
/// operator's key file `key_file` from the at-rest level (29 §12.8). The
/// replica takes and fsyncs a checkpoint at its applied prefix and exports
/// the exact bytes; the files are written on a blocking thread from the
/// node's read-only content and seed readers.
pub async fn create(
    fleet: &FleetManager,
    data_dir: &Path,
    reader: focal_evidence::ContentReader,
    ledger: LedgerId,
    output: PathBuf,
    key_file: Option<PathBuf>,
    budget: &MemoryBudget,
) -> Result<AdminBackup, AccessError> {
    // The form is settled before the replica is asked for anything: a request the fence refuses
    // costs the session no checkpoint.
    let at_rest =
        crate::storage_level::at_rest_form(data_dir).map_err(|_| AccessError::Unavailable)?;
    let form = backup_form(&output, key_file.as_deref(), at_rest)?;
    let (sealed, key) = (form.sealed(), form.key());
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
    // The export reads the checkpoint of a session this node hosts, which
    // its budget funds: limits sized to that budget hold it.
    let limits = crate::network_service::native_limits(domain, budget.limit(), budget.limit())
        .map_err(|_| AccessError::Unavailable)?;
    let seeds = backup::SessionSeeds {
        seeds: focal_evidence::SeedReader::open(
            crate::custody::seed_directory(&data_dir.join("seeds"), ledger),
            reader.data_keys().clone(),
        )
        .map_err(|_| AccessError::Unavailable)?,
        // The session's cold streams go with it whole (doc 31 M2, A2).
        cold: Some(crate::custody::seed_directory(
            &data_dir.join("cold"),
            ledger,
        )),
        keys: reader.data_keys().clone(),
    };
    let work = budget
        .child(VERIFY_BUDGET_BYTES, VERIFY_COMPLETION_BYTES)
        .map_err(|_| AccessError::Capacity)?;
    let now = now_ms()?;
    let target = output.clone();
    let report = focal_rt::on_pool(&focal_rt::FILES, move || {
        let mut medium = FileMedium;
        backup::write(
            &mut medium,
            &target,
            &form,
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
        sealed,
        key,
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
/// own memory budget, its key opened under the operator's key file
/// `key_file` where it is sealed (29 §12.8); nothing is written and no node
/// is consulted.
pub fn verify(
    input: &Path,
    key_file: Option<&Path>,
) -> Result<AdminBackupVerification, BackupError> {
    let budget = MemoryBudget::new(VERIFY_BUDGET_BYTES, VERIFY_COMPLETION_BYTES)
        .map_err(|_| BackupError::Capacity)?;
    let form = form(input, key_file)?;
    let files = BackupFiles {
        medium: &FileMedium,
        root: input,
        form: &form,
        budget: &budget,
    };
    let report = backup::verify(files, backup::decoder_pair().1)?;
    let complete = report.complete() && report.decoder_supported;
    let manifest = &report.manifest;
    Ok(AdminBackupVerification {
        input: input.to_string_lossy().into_owned(),
        sealed: form.sealed(),
        key: form.key(),
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

/// Read a backup's manifest alone, its key opened under the operator's key
/// file `key_file` where it is sealed (29 §12.8), under a verification's
/// budget.
pub fn read_manifest(input: &Path, key_file: Option<&Path>) -> Result<BackupManifest, BackupError> {
    let budget = MemoryBudget::new(VERIFY_BUDGET_BYTES, VERIFY_COMPLETION_BYTES)
        .map_err(|_| BackupError::Capacity)?;
    let form = form(input, key_file)?;
    BackupFiles {
        medium: &FileMedium,
        root: input,
        form: &form,
        budget: &budget,
    }
    .read_manifest()
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
    /// The operator's key file a sealed backup's key is opened under (29 §12.8).
    pub key_file: Option<PathBuf>,
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
