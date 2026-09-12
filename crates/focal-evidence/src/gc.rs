//! Reclamation of the store's bytes (26 §5). Nothing here decides what is
//! proof: the node hands the collector a protection set — every object a
//! hosted core, a continuation or an archive bundle names, every custody
//! record in use, every domain whose references this node cannot know — and
//! the collector reclaims only what lies outside it and has stood untouched
//! for the grace. Reclamation is two-step: a file is first moved into a
//! dated quarantine round (a rename on the same volume, reversible by
//! [`ContentStore::restore_quarantined`]), and only a round older than the
//! quarantine grace is deleted. One step does a bounded number of file
//! visits; a pass resumes where its bound stopped it and reports what it
//! did when it completes.
use super::*;
use std::collections::BTreeSet;
use std::fs::ReadDir;
use std::time::UNIX_EPOCH;

/// How the collector paces and what it spares.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CollectorConfig {
    /// Milliseconds a file must have stood unchanged before it is a
    /// candidate: what admission in flight may still bind.
    pub grace_ms: u64,
    /// Milliseconds a quarantined round waits before its files are deleted.
    pub quarantine_ms: u64,
    /// Milliseconds a terminal upload record fences its identity for after
    /// the upload finished.
    pub terminal_grace_ms: u64,
    /// Custody records of each kind kept regardless of age, newest first.
    pub keep_records: usize,
    /// Chunk marks one domain's pass may hold; beyond it the domain's
    /// chunks are left in place and reported as deferred.
    pub max_marks: usize,
}
impl Default for CollectorConfig {
    fn default() -> Self {
        Self {
            grace_ms: 24 * 60 * 60 * 1000,
            quarantine_ms: 7 * 24 * 60 * 60 * 1000,
            terminal_grace_ms: 7 * 24 * 60 * 60 * 1000,
            keep_records: 4,
            max_marks: 1 << 20,
        }
    }
}

/// What the collector may not touch. Built by the node from every root it
/// knows; sorted once by [`ProtectionSet::finish`] before use.
#[derive(Debug, Default, Clone)]
pub struct ProtectionSet {
    objects: Vec<(ContentDomainId, ContentHash)>,
    /// Objects named by the digest of their whole byte stream: what a row
    /// holding a payload inline can name without knowing how the node
    /// chunked it.
    streams: Vec<(ContentDomainId, ContentHash)>,
    records: Vec<(u8, ContentHash)>,
    opaque: Vec<ContentDomainId>,
    finished: bool,
}
impl ProtectionSet {
    pub fn new() -> Self {
        Self::default()
    }
    /// An object a live row, a continuation or a bundle names.
    pub fn protect_object(
        &mut self,
        domain: ContentDomainId,
        root: ContentHash,
    ) -> Result<(), ContentError> {
        self.objects
            .try_reserve_exact(1)
            .map_err(|_| ContentError::Capacity)?;
        self.objects.push((domain, root));
        self.finished = false;
        Ok(())
    }
    /// An object named by its stream digest (blake3 of its whole bytes):
    /// how a row that holds the payload inline names the object the node
    /// sealed at admission.
    pub fn protect_stream(
        &mut self,
        domain: ContentDomainId,
        digest: ContentHash,
    ) -> Result<(), ContentError> {
        self.streams
            .try_reserve_exact(1)
            .map_err(|_| ContentError::Capacity)?;
        self.streams.push((domain, digest));
        self.finished = false;
        Ok(())
    }
    /// A custody record in use: a verification's checkpoint, a manifest a
    /// transfer still serves.
    pub fn protect_record(
        &mut self,
        kind: CustodyRecordKind,
        hash: ContentHash,
    ) -> Result<(), ContentError> {
        self.records
            .try_reserve_exact(1)
            .map_err(|_| ContentError::Capacity)?;
        self.records.push((kind.tag(), hash));
        self.finished = false;
        Ok(())
    }
    /// A domain whose references this node cannot know completely (it
    /// holds copies for a session it does not host with a core): nothing in
    /// it is collected.
    pub fn opaque_domain(&mut self, domain: ContentDomainId) -> Result<(), ContentError> {
        self.opaque
            .try_reserve_exact(1)
            .map_err(|_| ContentError::Capacity)?;
        self.opaque.push(domain);
        self.finished = false;
        Ok(())
    }
    pub fn finish(&mut self) {
        self.objects.sort();
        self.objects.dedup();
        self.streams.sort();
        self.streams.dedup();
        self.records.sort();
        self.records.dedup();
        self.opaque.sort();
        self.opaque.dedup();
        self.finished = true;
    }
    /// Objects protected by root or by stream digest.
    pub fn objects(&self) -> usize {
        self.objects.len().saturating_add(self.streams.len())
    }
    fn protects_object(&self, domain: ContentDomainId, root: ContentHash) -> bool {
        self.objects.binary_search(&(domain, root)).is_ok()
    }
    fn protects_stream(&self, domain: ContentDomainId, digest: ContentHash) -> bool {
        self.streams.binary_search(&(domain, digest)).is_ok()
    }
    fn protects_record(&self, kind: CustodyRecordKind, hash: ContentHash) -> bool {
        self.records.binary_search(&(kind.tag(), hash)).is_ok()
    }
    fn is_opaque(&self, domain: ContentDomainId) -> bool {
        self.opaque.binary_search(&domain).is_ok()
    }
}

/// What one pass did; `complete` once the pass finished.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CollectorReport {
    /// Files and directory entries visited.
    pub visited: u64,
    /// Staged uploads whose part stood untouched past the grace, finished
    /// as abandoned.
    pub uploads_expired: u64,
    /// Terminal upload records past their fence grace, released.
    pub terminals_released: u64,
    pub objects_quarantined: u64,
    pub chunks_quarantined: u64,
    /// Domains whose chunks were left because the mark bound was exceeded.
    pub chunks_deferred: u64,
    pub records_quarantined: u64,
    pub receipts_quarantined: u64,
    /// Files deleted from quarantine rounds past their grace.
    pub deleted: u64,
    pub bytes_deleted: u64,
    /// Domains skipped as opaque.
    pub opaque_domains: u64,
    pub complete: bool,
}

enum Phase {
    Idle,
    Uploads,
    Terminals(Option<ReadDir>),
    Domains {
        domains: Vec<ContentDomainId>,
        index: usize,
    },
    Manifests {
        domains: Vec<ContentDomainId>,
        index: usize,
        dir: ReadDir,
        marks: BTreeSet<ContentHash>,
        overflow: bool,
    },
    Chunks {
        domains: Vec<ContentDomainId>,
        index: usize,
        dir: ReadDir,
        marks: BTreeSet<ContentHash>,
    },
    Records {
        kinds: usize,
    },
    Receipts(Option<ReadDir>),
    Quarantine {
        rounds: Vec<u64>,
        index: usize,
        dir: Option<ReadDir>,
    },
}

pub(super) struct CollectorState {
    phase: Phase,
    round: Option<u64>,
    report: CollectorReport,
}
impl CollectorState {
    pub(super) fn new() -> Self {
        Self {
            phase: Phase::Idle,
            round: None,
            report: CollectorReport::default(),
        }
    }
}

/// A file's age in milliseconds at `now`, zero when its clock is ahead.
fn age_ms(metadata: &fs::Metadata, now_ms: u64) -> Result<u64, ContentError> {
    let modified = metadata.modified()?;
    let modified_ms = modified
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0);
    Ok(now_ms.saturating_sub(modified_ms))
}
fn parse_hash(name: &str) -> Option<ContentHash> {
    if name.len() != 64 {
        return None;
    }
    let mut bytes = [0u8; 32];
    for (index, byte) in bytes.iter_mut().enumerate() {
        let pair = name.get(index.checked_mul(2)?..index.checked_mul(2)?.checked_add(2)?)?;
        *byte = u8::from_str_radix(pair, 16).ok()?;
    }
    Some(ContentHash(bytes))
}
fn parse_domain(name: &str) -> Option<ContentDomainId> {
    if name.len() != 32 {
        return None;
    }
    let mut bytes = [0u8; 16];
    for (index, byte) in bytes.iter_mut().enumerate() {
        let pair = name.get(index.checked_mul(2)?..index.checked_mul(2)?.checked_add(2)?)?;
        *byte = u8::from_str_radix(pair, 16).ok()?;
    }
    Some(ContentDomainId(bytes))
}
/// `name.ext` split into the hex identity and its extension.
fn split_name(entry: &fs::DirEntry) -> Option<(String, String)> {
    let name = entry.file_name();
    let name = name.to_str()?;
    let (stem, extension) = name.rsplit_once('.')?;
    Some((stem.to_owned(), extension.to_owned()))
}

impl ContentStore {
    fn quarantine_root(&self) -> PathBuf {
        self.root.join("quarantine")
    }
    /// The quarantine round this pass moves files into, created on first use.
    fn round_dir(&mut self, now_ms: u64, relative: &Path) -> Result<PathBuf, ContentError> {
        let round = match self.collector.round {
            Some(round) => round,
            None => {
                self.collector.round = Some(now_ms);
                now_ms
            }
        };
        let directory = self
            .quarantine_root()
            .join(format!("{round:020}"))
            .join(relative);
        durable_directory(&directory)?;
        Ok(directory)
    }
    /// Move one file into the current round under `relative`; the source
    /// directory is synced by the caller once per step.
    fn quarantine(
        &mut self,
        source: &Path,
        relative: &Path,
        now_ms: u64,
    ) -> Result<(), ContentError> {
        let target = self
            .round_dir(now_ms, relative)?
            .join(source.file_name().ok_or(ContentError::Invalid)?);
        focal_platform::fs::atomic_replace(source, &target)?;
        sync_directory(target.parent().ok_or(ContentError::Invalid)?)?;
        Ok(())
    }
    /// Advance the collector by at most `max_items` file visits. The report
    /// accumulates over the pass and says `complete` when the pass finished;
    /// the next call starts a new pass. An I/O failure stops the store as
    /// any write failure does.
    pub fn collect_step(
        &mut self,
        protection: &ProtectionSet,
        config: CollectorConfig,
        now_ms: u64,
        max_items: usize,
    ) -> Result<CollectorReport, ContentError> {
        self.check()?;
        if !protection.finished || max_items == 0 || config.keep_records == 0 {
            return Err(ContentError::Invalid);
        }
        let result = self.collect_inner(protection, config, now_ms, max_items);
        self.mark_failure(&result);
        result
    }
    fn collect_inner(
        &mut self,
        protection: &ProtectionSet,
        config: CollectorConfig,
        now_ms: u64,
        max_items: usize,
    ) -> Result<CollectorReport, ContentError> {
        let mut budget = max_items;
        loop {
            if budget == 0 {
                return Ok(self.collector.report);
            }
            let phase = std::mem::replace(&mut self.collector.phase, Phase::Idle);
            let next = match phase {
                Phase::Idle => {
                    self.collector.report = CollectorReport::default();
                    self.collector.round = None;
                    Phase::Uploads
                }
                Phase::Uploads => {
                    self.expire_uploads(config, now_ms, &mut budget)?;
                    Phase::Terminals(None)
                }
                Phase::Terminals(dir) => {
                    match self.release_terminals(dir, config, now_ms, &mut budget)? {
                        Some(dir) => Phase::Terminals(Some(dir)),
                        None => {
                            let domains = self.domains()?;
                            Phase::Domains { domains, index: 0 }
                        }
                    }
                }
                Phase::Domains { domains, index } => match domains.get(index).copied() {
                    None => Phase::Records { kinds: 0 },
                    Some(domain) if protection.is_opaque(domain) => {
                        self.collector.report.opaque_domains =
                            self.collector.report.opaque_domains.saturating_add(1);
                        Phase::Domains {
                            domains,
                            index: index.saturating_add(1),
                        }
                    }
                    Some(domain) => {
                        let dir = fs::read_dir(self.root.join("objects").join(hex(&domain.0)))?;
                        Phase::Manifests {
                            domains,
                            index,
                            dir,
                            marks: BTreeSet::new(),
                            overflow: false,
                        }
                    }
                },
                Phase::Manifests {
                    domains,
                    index,
                    mut dir,
                    mut marks,
                    mut overflow,
                } => {
                    let domain = *domains.get(index).ok_or(ContentError::Invalid)?;
                    let exhausted = self.sweep_manifests(
                        domain,
                        &mut dir,
                        &mut marks,
                        &mut overflow,
                        protection,
                        config,
                        now_ms,
                        &mut budget,
                    )?;
                    if exhausted {
                        if overflow {
                            self.collector.report.chunks_deferred =
                                self.collector.report.chunks_deferred.saturating_add(1);
                            Phase::Domains {
                                domains,
                                index: index.saturating_add(1),
                            }
                        } else {
                            let dir = fs::read_dir(self.root.join("objects").join(hex(&domain.0)))?;
                            Phase::Chunks {
                                domains,
                                index,
                                dir,
                                marks,
                            }
                        }
                    } else {
                        Phase::Manifests {
                            domains,
                            index,
                            dir,
                            marks,
                            overflow,
                        }
                    }
                }
                Phase::Chunks {
                    domains,
                    index,
                    mut dir,
                    marks,
                } => {
                    let domain = *domains.get(index).ok_or(ContentError::Invalid)?;
                    let exhausted =
                        self.sweep_chunks(domain, &mut dir, &marks, config, now_ms, &mut budget)?;
                    if exhausted {
                        Phase::Domains {
                            domains,
                            index: index.saturating_add(1),
                        }
                    } else {
                        Phase::Chunks {
                            domains,
                            index,
                            dir,
                            marks,
                        }
                    }
                }
                Phase::Records { kinds } => {
                    const KINDS: [CustodyRecordKind; 2] =
                        [CustodyRecordKind::Checkpoint, CustodyRecordKind::Manifest];
                    match KINDS.get(kinds) {
                        Some(kind) => {
                            self.sweep_records(*kind, protection, config, now_ms, &mut budget)?;
                            Phase::Records {
                                kinds: kinds.saturating_add(1),
                            }
                        }
                        None => Phase::Receipts(None),
                    }
                }
                Phase::Receipts(dir) => {
                    match self.sweep_receipts(dir, config, now_ms, &mut budget)? {
                        Some(dir) => Phase::Receipts(Some(dir)),
                        None => {
                            let rounds = self.expired_rounds(config, now_ms)?;
                            Phase::Quarantine {
                                rounds,
                                index: 0,
                                dir: None,
                            }
                        }
                    }
                }
                Phase::Quarantine { rounds, index, dir } => match rounds.get(index).copied() {
                    None => {
                        self.collector.report.complete = true;
                        let report = self.collector.report;
                        self.collector.phase = Phase::Idle;
                        self.collector.round = None;
                        return Ok(report);
                    }
                    Some(round) => {
                        let done = self.delete_round(round, dir, &mut budget)?;
                        match done {
                            None => Phase::Quarantine {
                                rounds,
                                index: index.saturating_add(1),
                                dir: None,
                            },
                            Some(dir) => Phase::Quarantine {
                                rounds,
                                index,
                                dir: Some(dir),
                            },
                        }
                    }
                },
            };
            self.collector.phase = next;
        }
    }
    fn visit(&mut self, budget: &mut usize) {
        *budget = budget.saturating_sub(1);
        self.collector.report.visited = self.collector.report.visited.saturating_add(1);
    }
    /// Staged uploads whose part stood untouched past the grace are
    /// finished as abandoned: their terminal fence is installed and their
    /// bytes returned, exactly as an explicit finish does.
    fn expire_uploads(
        &mut self,
        config: CollectorConfig,
        now_ms: u64,
        budget: &mut usize,
    ) -> Result<(), ContentError> {
        let mut stale = Vec::new();
        for (id, upload) in &self.uploads {
            if *budget == 0 {
                break;
            }
            *budget = budget.saturating_sub(1);
            self.collector.report.visited = self.collector.report.visited.saturating_add(1);
            if age_ms(&upload.file.metadata()?, now_ms)? >= config.grace_ms {
                stale
                    .try_reserve_exact(1)
                    .map_err(|_| ContentError::Capacity)?;
                stale.push(*id);
            }
        }
        for id in stale {
            match self.finish_inner(id) {
                Ok(()) => {
                    self.collector.report.uploads_expired =
                        self.collector.report.uploads_expired.saturating_add(1);
                }
                // At the terminal-uploads cap (or a transient shortage) finishing
                // an upload is deferred, not fatal: returning lets the pass reach
                // the Terminals phase, which releases terminal records and lowers
                // the count so a later pass finishes these. Aborting instead would
                // reset the phase to Idle and wedge, never reaching that release.
                Err(ContentError::Capacity) => break,
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
    /// Terminal records past their fence grace are released: a retry of
    /// that upload identity later than the grace begins afresh.
    fn release_terminals(
        &mut self,
        dir: Option<ReadDir>,
        config: CollectorConfig,
        now_ms: u64,
        budget: &mut usize,
    ) -> Result<Option<ReadDir>, ContentError> {
        let mut dir = match dir {
            Some(dir) => dir,
            None => fs::read_dir(self.root.join("staging"))?,
        };
        let mut changed = false;
        while *budget > 0 {
            let Some(entry) = dir.next() else {
                if changed {
                    sync_directory(&self.root.join("staging"))?;
                }
                return Ok(None);
            };
            let entry = entry?;
            self.visit(budget);
            let Some((stem, extension)) = split_name(&entry) else {
                continue;
            };
            if extension != "meta" {
                continue;
            }
            let bytes = read_bounded(
                &entry.path(),
                self.limits.max_manifest_bytes.max(terminal::RECORD_BYTES),
            )?;
            let Some(id) = terminal::decode(&bytes)? else {
                continue;
            };
            if hex(&id.0) != stem || age_ms(&entry.metadata()?, now_ms)? < config.terminal_grace_ms
            {
                continue;
            }
            fs::remove_file(entry.path())?;
            changed = true;
            self.terminal_uploads = self.terminal_uploads.saturating_sub(1);
            self.collector.report.terminals_released =
                self.collector.report.terminals_released.saturating_add(1);
        }
        if changed {
            sync_directory(&self.root.join("staging"))?;
        }
        Ok(Some(dir))
    }
    fn domains(&self) -> Result<Vec<ContentDomainId>, ContentError> {
        let mut domains = Vec::new();
        for entry in fs::read_dir(self.root.join("objects"))? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let name = entry.file_name();
            if let Some(domain) = name.to_str().and_then(parse_domain) {
                domains
                    .try_reserve_exact(1)
                    .map_err(|_| ContentError::Capacity)?;
                domains.push(domain);
            }
        }
        domains.sort();
        Ok(domains)
    }
    /// One domain's manifests: a protected or young object marks its
    /// chunks; any other past the grace is quarantined. Returns whether the
    /// directory is exhausted.
    #[allow(clippy::too_many_arguments)] // One phase of one state machine.
    fn sweep_manifests(
        &mut self,
        domain: ContentDomainId,
        dir: &mut ReadDir,
        marks: &mut BTreeSet<ContentHash>,
        overflow: &mut bool,
        protection: &ProtectionSet,
        config: CollectorConfig,
        now_ms: u64,
        budget: &mut usize,
    ) -> Result<bool, ContentError> {
        let directory = self.root.join("objects").join(hex(&domain.0));
        let mut changed = false;
        while *budget > 0 {
            let Some(entry) = dir.next() else {
                if changed {
                    sync_directory(&directory)?;
                }
                return Ok(true);
            };
            let entry = entry?;
            self.visit(budget);
            let Some((stem, extension)) = split_name(&entry) else {
                continue;
            };
            if extension != "manifest" {
                continue;
            }
            let Some(root) = parse_hash(&stem) else {
                continue;
            };
            let metadata = entry.metadata()?;
            // A single unreadable/oversized/corrupt manifest must not abort the
            // whole GC pass — an aborted pass resets to Idle and restarts,
            // re-hitting the poison file and never reaching quarantine deletion.
            // I/O errors stay fatal (the store's failure model); a poison manifest
            // is skipped, exactly as sweep_receipts skips an undecodable receipt.
            // Leaving its chunks unmarked is correct: an object whose manifest is
            // unreadable is already unrecoverable.
            let bytes = match read_bounded(&entry.path(), MAX_TRANSFER_MANIFEST_BYTES) {
                Ok(bytes) => bytes,
                Err(ContentError::Io(error)) => return Err(ContentError::Io(error)),
                Err(_) => continue,
            };
            let Some(payload) = bytes.get(MANIFEST_MAGIC.len()..) else {
                continue;
            };
            let Ok(manifest) = decode_manifest(payload) else {
                continue;
            };
            let keep = protection.protects_object(domain, root)
                || protection.protects_stream(domain, manifest.stream_digest)
                || age_ms(&metadata, now_ms)? < config.grace_ms;
            if keep {
                for chunk in manifest.chunks {
                    if marks.len() >= config.max_marks && !marks.contains(&chunk.hash) {
                        *overflow = true;
                        break;
                    }
                    marks.insert(chunk.hash);
                }
                continue;
            }
            self.quarantine(
                &entry.path(),
                &Path::new("objects").join(hex(&domain.0)),
                now_ms,
            )?;
            changed = true;
            self.collector.report.objects_quarantined =
                self.collector.report.objects_quarantined.saturating_add(1);
        }
        if changed {
            sync_directory(&directory)?;
        }
        Ok(false)
    }
    /// One domain's chunks: an unmarked chunk past the grace is quarantined.
    fn sweep_chunks(
        &mut self,
        domain: ContentDomainId,
        dir: &mut ReadDir,
        marks: &BTreeSet<ContentHash>,
        config: CollectorConfig,
        now_ms: u64,
        budget: &mut usize,
    ) -> Result<bool, ContentError> {
        let directory = self.root.join("objects").join(hex(&domain.0));
        let mut changed = false;
        while *budget > 0 {
            let Some(entry) = dir.next() else {
                if changed {
                    sync_directory(&directory)?;
                }
                return Ok(true);
            };
            let entry = entry?;
            self.visit(budget);
            let Some((stem, extension)) = split_name(&entry) else {
                continue;
            };
            if extension != "chunk" {
                continue;
            }
            let Some(hash) = parse_hash(&stem) else {
                continue;
            };
            if marks.contains(&hash) || age_ms(&entry.metadata()?, now_ms)? < config.grace_ms {
                continue;
            }
            self.quarantine(
                &entry.path(),
                &Path::new("objects").join(hex(&domain.0)),
                now_ms,
            )?;
            changed = true;
            self.collector.report.chunks_quarantined =
                self.collector.report.chunks_quarantined.saturating_add(1);
        }
        if changed {
            sync_directory(&directory)?;
        }
        Ok(false)
    }
    /// Custody records of one kind: the protected ones and the newest
    /// `keep_records` stay; the rest past the grace are quarantined.
    fn sweep_records(
        &mut self,
        kind: CustodyRecordKind,
        protection: &ProtectionSet,
        config: CollectorConfig,
        now_ms: u64,
        budget: &mut usize,
    ) -> Result<(), ContentError> {
        let directory = self.root.join(kind.directory());
        let mut records: Vec<(u64, ContentHash, PathBuf)> = Vec::new();
        for entry in fs::read_dir(&directory)? {
            let entry = entry?;
            self.visit(budget);
            let Some((stem, extension)) = split_name(&entry) else {
                continue;
            };
            if extension != "record" {
                continue;
            }
            let Some(hash) = parse_hash(&stem) else {
                continue;
            };
            if records.len() >= config.max_marks {
                break;
            }
            records
                .try_reserve_exact(1)
                .map_err(|_| ContentError::Capacity)?;
            records.push((age_ms(&entry.metadata()?, now_ms)?, hash, entry.path()));
        }
        // Youngest first; the first `keep_records` stay whatever their age.
        // The listing is bounded by the mark bound, so the whole kind is
        // decided in one step and charged as one visit per record moved.
        records.sort_by_key(|(age, hash, _)| (*age, *hash));
        let mut changed = false;
        for (position, (age, hash, path)) in records.into_iter().enumerate() {
            if position < config.keep_records
                || age < config.grace_ms
                || protection.protects_record(kind, hash)
            {
                continue;
            }
            self.visit(budget);
            self.quarantine(&path, Path::new(kind.directory()), now_ms)?;
            changed = true;
            self.collector.report.records_quarantined =
                self.collector.report.records_quarantined.saturating_add(1);
        }
        if changed {
            sync_directory(&directory)?;
        }
        Ok(())
    }
    /// Receipts of objects this store no longer holds, past the grace, are
    /// quarantined with them.
    fn sweep_receipts(
        &mut self,
        dir: Option<ReadDir>,
        config: CollectorConfig,
        now_ms: u64,
        budget: &mut usize,
    ) -> Result<Option<ReadDir>, ContentError> {
        let directory = self.root.join(CustodyRecordKind::Receipt.directory());
        let mut dir = match dir {
            Some(dir) => dir,
            None => fs::read_dir(&directory)?,
        };
        let mut changed = false;
        while *budget > 0 {
            let Some(entry) = dir.next() else {
                if changed {
                    sync_directory(&directory)?;
                }
                return Ok(None);
            };
            let entry = entry?;
            self.visit(budget);
            let Some((_, extension)) = split_name(&entry) else {
                continue;
            };
            if extension != "record" {
                continue;
            }
            let bytes = read_bounded(&entry.path(), CustodyRecordKind::Receipt.limit())?;
            let Ok(receipt) = crate::CustodyReceipt::decode(&bytes) else {
                continue;
            };
            let present = self
                .root
                .join("objects")
                .join(hex(&receipt.domain.0))
                .join(format!("{}.manifest", receipt.root))
                .is_file();
            if present || age_ms(&entry.metadata()?, now_ms)? < config.grace_ms {
                continue;
            }
            self.quarantine(
                &entry.path(),
                Path::new(CustodyRecordKind::Receipt.directory()),
                now_ms,
            )?;
            changed = true;
            self.collector.report.receipts_quarantined =
                self.collector.report.receipts_quarantined.saturating_add(1);
        }
        if changed {
            sync_directory(&directory)?;
        }
        Ok(Some(dir))
    }
    fn expired_rounds(
        &self,
        config: CollectorConfig,
        now_ms: u64,
    ) -> Result<Vec<u64>, ContentError> {
        let mut rounds = Vec::new();
        let quarantine = self.quarantine_root();
        if !quarantine.is_dir() {
            return Ok(rounds);
        }
        for entry in fs::read_dir(&quarantine)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(round) = name.to_str().and_then(|text| text.parse::<u64>().ok()) else {
                continue;
            };
            if round.saturating_add(config.quarantine_ms) <= now_ms
                && self.collector.round != Some(round)
            {
                rounds
                    .try_reserve_exact(1)
                    .map_err(|_| ContentError::Capacity)?;
                rounds.push(round);
            }
        }
        rounds.sort_unstable();
        Ok(rounds)
    }
    /// Delete one expired round's files, bounded; `None` once the round is
    /// gone, else the directory walk to resume.
    fn delete_round(
        &mut self,
        round: u64,
        dir: Option<ReadDir>,
        budget: &mut usize,
    ) -> Result<Option<ReadDir>, ContentError> {
        let root = self.quarantine_root().join(format!("{round:020}"));
        let mut walk = match dir {
            Some(dir) => dir,
            None => match fs::read_dir(&root) {
                Ok(dir) => dir,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(error) => return Err(error.into()),
            },
        };
        // The round holds at most three levels: kind, domain, file.
        while *budget > 0 {
            let Some(entry) = walk.next() else {
                fs::remove_dir_all(&root)?;
                sync_directory(&self.quarantine_root())?;
                return Ok(None);
            };
            let entry = entry?;
            self.visit(budget);
            self.delete_tree(&entry.path(), budget)?;
        }
        Ok(Some(walk))
    }
    fn delete_tree(&mut self, path: &Path, budget: &mut usize) -> Result<(), ContentError> {
        let metadata = fs::symlink_metadata(path)?;
        if metadata.is_dir() {
            for entry in fs::read_dir(path)? {
                let entry = entry?;
                self.visit(budget);
                self.delete_tree(&entry.path(), budget)?;
            }
            fs::remove_dir(path)?;
            return Ok(());
        }
        let bytes = metadata.len();
        fs::remove_file(path)?;
        self.collector.report.deleted = self.collector.report.deleted.saturating_add(1);
        self.collector.report.bytes_deleted =
            self.collector.report.bytes_deleted.saturating_add(bytes);
        Ok(())
    }
    /// Bring a quarantined object back: its manifest and every chunk of it
    /// still in a round. `Ok(false)` when no round holds the manifest.
    pub fn restore_quarantined(
        &mut self,
        domain: ContentDomainId,
        root: ContentHash,
    ) -> Result<bool, ContentError> {
        self.check()?;
        let result = self.restore_inner(domain, root);
        self.mark_failure(&result);
        result
    }
    fn restore_inner(
        &mut self,
        domain: ContentDomainId,
        root: ContentHash,
    ) -> Result<bool, ContentError> {
        let quarantine = self.quarantine_root();
        if !quarantine.is_dir() {
            return Ok(false);
        }
        let relative = Path::new("objects").join(hex(&domain.0));
        let target = self.root.join(&relative);
        let mut rounds = Vec::new();
        for entry in fs::read_dir(&quarantine)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                rounds
                    .try_reserve_exact(1)
                    .map_err(|_| ContentError::Capacity)?;
                rounds.push(entry.path());
            }
        }
        let manifest = format!("{root}.manifest");
        let Some(round) = rounds
            .iter()
            .find(|round| round.join(&relative).join(&manifest).is_file())
        else {
            return Ok(false);
        };
        durable_directory(&target)?;
        let source = round.join(&relative).join(&manifest);
        let bytes = read_bounded(&source, MAX_TRANSFER_MANIFEST_BYTES)?;
        if ContentHash(*blake3::hash(&bytes).as_bytes()) != root {
            return Err(ContentError::Corrupt);
        }
        let decoded = decode_manifest(
            bytes
                .get(MANIFEST_MAGIC.len()..)
                .ok_or(ContentError::Corrupt)?,
        )?;
        for chunk in decoded.chunks {
            let name = format!("{}.chunk", chunk.hash);
            if target.join(&name).is_file() {
                continue;
            }
            let Some(holder) = rounds
                .iter()
                .find(|round| round.join(&relative).join(&name).is_file())
            else {
                return Err(ContentError::Corrupt);
            };
            focal_platform::fs::atomic_replace(
                &holder.join(&relative).join(&name),
                &target.join(&name),
            )?;
        }
        focal_platform::fs::atomic_replace(&source, &target.join(&manifest))?;
        sync_directory(&target)?;
        Ok(true)
    }
}

/// What one seed sweep did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SeedReport {
    pub visited: u64,
    pub removed: u64,
    pub bytes_removed: u64,
    pub complete: bool,
}
impl SeedStore {
    /// Remove seeds nothing protects that stood past the grace, at most
    /// `max_items` visits; seeds are copies of checkpoint bytes a peer can
    /// serve again, so they need no quarantine. `protected` must be sorted.
    pub fn collect(
        &mut self,
        protected: &[ContentHash],
        grace_ms: u64,
        now_ms: u64,
        max_items: usize,
    ) -> Result<SeedReport, ContentError> {
        self.check()?;
        let mut report = SeedReport::default();
        let mut walk = match self.sweep.take() {
            Some(walk) => walk,
            None => fs::read_dir(&self.root)?,
        };
        let mut changed = false;
        let mut budget = max_items;
        while budget > 0 {
            let Some(entry) = walk.next() else {
                report.complete = true;
                break;
            };
            let entry = match entry {
                Ok(entry) => entry,
                Err(error) => {
                    self.failed = true;
                    return Err(ContentError::Io(error));
                }
            };
            budget = budget.saturating_sub(1);
            report.visited = report.visited.saturating_add(1);
            let Some((stem, extension)) = split_name(&entry) else {
                continue;
            };
            if extension != "seed" {
                continue;
            }
            let Some(hash) = parse_hash(&stem) else {
                continue;
            };
            if protected.binary_search(&hash).is_ok() {
                continue;
            }
            let metadata = match entry.metadata() {
                Ok(metadata) => metadata,
                Err(error) => {
                    self.failed = true;
                    return Err(ContentError::Io(error));
                }
            };
            if age_ms(&metadata, now_ms)? < grace_ms {
                continue;
            }
            if let Err(error) = fs::remove_file(entry.path()) {
                // Removal is idempotent (matching SeedStore::remove): the writer
                // may have removed this seed between the resumable ReadDir yielding
                // it and this step, so an already-absent file is complete, not a
                // failure that takes down seed serving until reopen.
                if error.kind() != std::io::ErrorKind::NotFound {
                    self.failed = true;
                    return Err(ContentError::Io(error));
                }
                continue;
            }
            changed = true;
            report.removed = report.removed.saturating_add(1);
            report.bytes_removed = report.bytes_removed.saturating_add(metadata.len());
        }
        if changed && let Err(error) = sync_directory(&self.root) {
            self.failed = true;
            return Err(ContentError::Io(error));
        }
        if !report.complete {
            self.sweep = Some(walk);
        }
        Ok(report)
    }
}

#[cfg(test)]
#[path = "gc_tests.rs"]
mod tests;
