//! Uploads being staged, at rest (29 §12.7). The staging directory is a key directory of its own,
//! as a seed directory is (its record, `DOMAIN.key`, under the content key): where the store seals,
//! an upload's part is a record file sealed as it is written ([`RecordWriter`], a key of its own
//! for every part), its metadata and its terminal record are sealed whole, and each is named by the
//! directory's keyed name of the upload (32 hex digits, as a name in the clear is, which hold the
//! upload's identity in the clear). Below the level, and in a store in the clear, every file is
//! written byte for byte as before.
//!
//! An upload's files lie in one form, the one its metadata was written in, until a conversion
//! writes them again in the other: its part, metadata and terminal record go together, so a
//! terminal record replaces its metadata where that lies. A crash in a conversion can leave both
//! forms: the clear one stands while its part does, the sealed one after ([`ContentStore::recover`]).
use super::record_file::{self, PART_MAGIC, RecordWriter};
use super::sealing::DomainKey;
use super::*;

/// What staging's key directory is made under: its record names it, so another directory's record
/// is not taken for it.
pub(super) const STAGING_DOMAIN: ContentDomainId = ContentDomainId(*b"focal staging   ");

/// What an upload's keyed name is taken of: a hash of its identity under a context of its own, so
/// the name is the directory's keyed name of no object's hash.
fn upload_hash(id: UploadId) -> ContentHash {
    ContentHash(blake3::derive_key("focal staging upload name", &id.0))
}

/// An upload's staged bytes: a file in the clear, or a record file sealed as it is written.
pub(super) enum Part {
    Clear(File),
    Sealed(Box<RecordWriter>),
}

impl Part {
    /// What was appended made durable.
    pub(super) fn sync(&self) -> Result<(), ContentError> {
        match self {
            Self::Clear(file) => file.sync_all().map_err(ContentError::from),
            Self::Sealed(writer) => writer.sync(),
        }
    }

    /// The staged bytes from `offset`, `out.len()` of them: read in place in the clear, opened from
    /// the records that hold them where sealed (`path` the part's own).
    pub(super) fn read_at(
        &mut self,
        path: &Path,
        offset: u64,
        out: &mut [u8],
    ) -> Result<(), ContentError> {
        match self {
            Self::Clear(file) => {
                file.seek(SeekFrom::Start(offset))?;
                file.read_exact(out)?;
                Ok(())
            }
            Self::Sealed(writer) => writer.read(
                path,
                u32::try_from(offset).map_err(|_| ContentError::Capacity)?,
                out,
            ),
        }
    }

    /// The part's file's metadata: when it was last written, which the collector ages it by.
    pub(super) fn metadata(&self) -> Result<fs::Metadata, ContentError> {
        match self {
            Self::Clear(file) => file.metadata().map_err(ContentError::from),
            Self::Sealed(writer) => writer.metadata(),
        }
    }
}

impl ContentStore {
    /// Staging's key directory's key: made with its record where `create` is set and the store
    /// seals; in the clear, none.
    pub(super) fn staging_key(&self, create: bool) -> Result<DomainKey, ContentError> {
        self.keys
            .domain(&self.root.join("staging"), STAGING_DOMAIN, create)
    }

    /// Where upload `id`'s file of `extension` lies in the form `key` writes: its keyed name
    /// sealed, its identity's hex in the clear.
    pub(super) fn staging_path(
        &self,
        key: &DomainKey,
        id: UploadId,
        extension: &str,
    ) -> Result<PathBuf, ContentError> {
        let stem = if self.rest.seals() {
            key.name(upload_hash(id))?
        } else {
            hex(&id.0)
        };
        Ok(self
            .root
            .join("staging")
            .join(format!("{stem}.{extension}")))
    }

    /// Where upload `id`'s file of `extension` may lie, each form's place with whether it seals:
    /// its name in the clear while a file may lie in the clear, its keyed name once staging's key
    /// directory stands.
    pub(super) fn staging_places(
        &self,
        id: UploadId,
        extension: &str,
    ) -> Result<Vec<(PathBuf, bool)>, ContentError> {
        let mut places = Vec::new();
        places
            .try_reserve_exact(2)
            .map_err(|_| ContentError::Capacity)?;
        if self.rest.clear_allowed() {
            places.push((
                self.root
                    .join("staging")
                    .join(format!("{}.{extension}", hex(&id.0))),
                false,
            ));
        }
        let key = self.staging_key(false)?;
        if self.rest.seals() && key.record_bytes() == 0 {
            places.push((self.staging_path(&key, id, extension)?, true));
        }
        Ok(places)
    }

    /// Where upload `id`'s terminal record lies, if one does: what fences its identity.
    pub(super) fn terminal_at(
        &self,
        id: UploadId,
    ) -> Result<Option<(PathBuf, bool)>, ContentError> {
        for (path, sealed) in self.staging_places(id, "meta")? {
            let bytes = match self.staging_read(&path, sealed) {
                Ok(bytes) => bytes,
                Err(ContentError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                    continue;
                }
                Err(error) => return Err(error),
            };
            match terminal::decode(&bytes)? {
                Some(found) if found == id => return Ok(Some((path, sealed))),
                Some(_) => return Err(ContentError::Corrupt),
                None => {}
            }
        }
        Ok(None)
    }

    /// A staging record (an upload's metadata or terminal record) read whole: opened under
    /// staging's key where `sealed`, as it is in the clear, a regular file of one link alone.
    pub(super) fn staging_read(&self, path: &Path, sealed: bool) -> Result<Vec<u8>, ContentError> {
        let most = self.limits.max_manifest_bytes.max(terminal::RECORD_BYTES);
        if !sealed {
            if !terminal::regular(path)? {
                return Err(ContentError::Io(std::io::Error::from(
                    std::io::ErrorKind::NotFound,
                )));
            }
            return read_bounded(path, most);
        }
        if !terminal::regular(path)? {
            return Err(ContentError::Io(std::io::Error::from(
                std::io::ErrorKind::NotFound,
            )));
        }
        self.staging_key(false)?.read_record(path, most)
    }

    /// A new part for upload `id` of `length` bytes in the form `key` writes, at `path`, which
    /// must not stand, and made durable: sealed, a record file under a key of its own, kept for the
    /// upload ([`DomainKey::part_key`]).
    pub(super) fn create_part(
        &self,
        key: &DomainKey,
        id: UploadId,
        path: &Path,
        length: u64,
    ) -> Result<Part, ContentError> {
        let part = if self.rest.seals() {
            let file = hyper_seal::keys::KeyId::random()
                .map_err(|_| {
                    ContentError::Io(std::io::Error::other(
                        "the operating system's random source failed",
                    ))
                })?
                .0;
            Part::Sealed(Box::new(RecordWriter::create(
                path,
                PART_MAGIC,
                file,
                key.part_key(&file, &upload_hash(id).0)?,
                usize::try_from(length).map_err(|_| ContentError::Capacity)?,
            )?))
        } else {
            Part::Clear(
                OpenOptions::new()
                    .create_new(true)
                    .read(true)
                    .write(true)
                    .open(path)?,
            )
        };
        part.sync()?;
        Ok(part)
    }

    /// The part at `path` opened again to go on with its upload in its form: in the clear as it
    /// is; sealed, read through to its end ([`RecordWriter::resume`]: a part whose last append a
    /// crash cut short is written again whole under a new key). The bytes it holds with it.
    pub(super) fn reopen_part(
        &self,
        id: UploadId,
        path: &Path,
        sealed: bool,
        length: u64,
    ) -> Result<(Part, u64), ContentError> {
        if !sealed {
            let file = OpenOptions::new().read(true).write(true).open(path)?;
            let held = file.metadata()?.len();
            // Full chunks acknowledged before restart were synced. An unacknowledged
            // tail can also survive and is verified against the caller's digest at seal.
            file.sync_all()?;
            return Ok((Part::Clear(file), held));
        }
        let key = self.staging_key(false)?;
        let writer = RecordWriter::resume(
            path,
            PART_MAGIC,
            &|file: &[u8; 16]| key.part_key(file, &upload_hash(id).0),
            usize::try_from(length).map_err(|_| ContentError::Capacity)?,
            record_file::records_bytes(self.limits.chunk_bytes)?,
            record_file::Verify::All,
        )?;
        let held = writer.plain();
        Ok((Part::Sealed(Box::new(writer)), held))
    }

    /// Upload `id`'s terminal record installed at `path` in the form `sealed` says, replacing its
    /// metadata there: synced, renamed into place, the directory synced.
    pub(super) fn install_terminal(
        &self,
        path: &Path,
        sealed: bool,
        id: UploadId,
    ) -> Result<(), ContentError> {
        if !sealed {
            return terminal::install(path, id);
        }
        self.staging_key(true)?
            .install(path, &terminal::encode(id)?)
    }

    /// Every upload's files in staging opened again: an upload staged before resumes where its part
    /// stopped, a terminal record fences its identity, and what a crash left of a write that never
    /// became one (a terminal record's or a sealed record's temporary, a part written again but not
    /// renamed) goes. Both forms are read while a file may lie in the clear. An upload with files in
    /// both, which a crash in its conversion left, is settled where the listing first meets it
    /// ([`settle`]), and the files of the form that goes are removed.
    pub(super) fn recover(&mut self) -> Result<(), ContentError> {
        let staging = self.root.join("staging");
        let key = self.staging_key(false)?;
        let keyed = self.rest.seals() && key.record_bytes() == 0;
        let mut changed = false;
        for entry in fs::read_dir(&staging)? {
            let path = entry?.path();
            match path.extension().and_then(|x| x.to_str()) {
                Some("terminal") => {
                    terminal::discard_pending(&path)?;
                    continue;
                }
                Some("install" | "resume") if keyed => {
                    // A sealed record's or a rewritten part's temporary: never renamed into place.
                    changed |= remove_staged(&path)?;
                    continue;
                }
                Some("meta") => {}
                _ => continue,
            }
            let Some((record, sealed)) = self.staged_record(&key, keyed, &path)? else {
                continue;
            };
            let id = record.id();
            if keyed && self.rest.clear_allowed() {
                let clear_meta = self.clear_staging_path(id, "meta");
                let keyed_meta = self.staging_path(&key, id, "meta")?;
                let other = if sealed { &clear_meta } else { &keyed_meta };
                if let Some((other_record, _)) = self.staged_record(&key, keyed, other)? {
                    let clear_part = self.clear_staging_path(id, "part");
                    let (clear_record, sealed_record) = if sealed {
                        (&other_record, &record)
                    } else {
                        (&record, &other_record)
                    };
                    let keep_sealed =
                        settle(clear_record, sealed_record, terminal::regular(&clear_part)?);
                    let (gone_meta, gone_part) = if keep_sealed {
                        (clear_meta, clear_part)
                    } else {
                        (keyed_meta, self.staging_path(&key, id, "part")?)
                    };
                    changed |= remove_staged(&gone_part)?;
                    changed |= remove_staged(&gone_meta)?;
                    if keep_sealed != sealed {
                        // This entry's form went; the other is met in its turn.
                        continue;
                    }
                }
            }
            let part_path = if sealed {
                self.staging_path(&key, id, "part")?
            } else {
                self.clear_staging_path(id, "part")
            };
            match record {
                Staged::Terminal(_) => {
                    self.terminal_uploads = self
                        .terminal_uploads
                        .checked_add(1)
                        .filter(|count| *count <= MAX_TERMINAL_UPLOADS)
                        .ok_or(ContentError::Capacity)?;
                    if self
                        .terminal_uploads
                        .checked_add(self.uploads.len())
                        .is_none_or(|count| count > MAX_TERMINAL_UPLOADS)
                    {
                        return Err(ContentError::Capacity);
                    }
                    terminal::remove_part(&part_path)?;
                }
                Staged::Active(meta) => self.resume_upload(meta, path, part_path, sealed)?,
            }
        }
        // A part with no metadata beside it (a sealed copy a crash cut before its metadata, a part
        // a crash left before its upload's metadata was written) is never an accepted upload.
        for entry in fs::read_dir(&staging)? {
            let path = entry?.path();
            if path.extension().and_then(|x| x.to_str()) == Some("part")
                && !terminal::regular(&path.with_extension("meta"))?
            {
                changed |= remove_staged(&path)?;
            }
        }
        if changed {
            sync_directory(&staging)?;
        }
        Ok(())
    }

    /// Upload `meta` taken up again from its part at `part_path` in its form (its metadata at
    /// `meta_path`): its bytes counted against the allowances, the rest of it promised the volume
    /// where the volume can.
    fn resume_upload(
        &mut self,
        meta: UploadMeta,
        meta_path: PathBuf,
        part_path: PathBuf,
        sealed: bool,
    ) -> Result<(), ContentError> {
        self.reserve_upload(meta.domain, meta.expected_length)?;
        let (part, offset) = self.reopen_part(meta.id, &part_path, sealed, meta.expected_length)?;
        if offset > meta.expected_length {
            return Err(ContentError::Corrupt);
        }
        self.staged_bytes = self
            .staged_bytes
            .checked_add(meta.expected_length)
            .ok_or(ContentError::Corrupt)?;
        let domain_bytes = self
            .staged_by_domain
            .get(&meta.domain)
            .copied()
            .unwrap_or(0)
            .checked_add(meta.expected_length)
            .ok_or(ContentError::Corrupt)?;
        self.staged_by_domain.insert(meta.domain, domain_bytes);
        // The bytes already received are on the volume; promise only the
        // remainder, and keep an upload resumable when the volume cannot.
        let staging = disk_reserve(
            &self.disk,
            &self.root,
            DiskKind::Staging,
            BudgetLane::Completion,
            meta.expected_length.saturating_sub(offset),
        )
        .ok();
        self.uploads.insert(
            meta.id,
            Upload {
                meta,
                part,
                part_path,
                meta_path,
                offset,
                _staging: staging,
            },
        );
        Ok(())
    }

    /// Upload `id`'s file of `extension` in the clear, named by its identity's hex.
    pub(super) fn clear_staging_path(&self, id: UploadId, extension: &str) -> PathBuf {
        self.root
            .join("staging")
            .join(format!("{}.{extension}", hex(&id.0)))
    }

    /// The staging record at `path` and whether it is sealed: in the clear where it decodes as it
    /// is and its name is its upload's in the clear, sealed where it opens under staging's key and
    /// its name is its upload's keyed name. None where it is gone; corrupt where it is neither.
    pub(super) fn staged_record(
        &self,
        key: &DomainKey,
        keyed: bool,
        path: &Path,
    ) -> Result<Option<(Staged, bool)>, ContentError> {
        let stem = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .ok_or(ContentError::Corrupt)?;
        if self.rest.clear_allowed() {
            match self.staging_read(path, false) {
                Ok(bytes) => {
                    if let Some(record) = decode_staged(&bytes)?
                        && hex(&record.id().0) == stem
                    {
                        return Ok(Some((record, false)));
                    }
                }
                Err(ContentError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Ok(None);
                }
                Err(error) => return Err(error),
            }
        }
        if !keyed {
            return Err(ContentError::Corrupt);
        }
        let bytes = match self.staging_read(path, true) {
            Ok(bytes) => bytes,
            Err(ContentError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        let record = decode_staged(&bytes)?.ok_or(ContentError::Corrupt)?;
        if self.staging_path(key, record.id(), "meta")? != path {
            return Err(ContentError::Corrupt);
        }
        Ok(Some((record, true)))
    }
}

/// Staging's pass in a store's conversion (29 §12.7): its files in the clear met in the listing's
/// order, each tallied converted as it is sealed. The count counted them before (`counted_staging`),
/// and what changes them since is counted as it changes (`ContentStore::conversion_staging`: a part
/// grows, an upload finishes, a terminal record is released). A pass that meets no file in the clear
/// ends the conversion's staging; one that met some is followed by another, which meets what was
/// written in the clear meanwhile (an upload in the clear finished during its pass).
pub(super) struct StagingPass {
    listing: fs::ReadDir,
    /// A file in the clear was removed since the directory was last flushed.
    pub(super) removed: bool,
    /// The pass met a file in the clear.
    met: bool,
    /// An upload's part being copied into its sealed form, a chunk a step.
    copy: Option<PartCopy>,
}

/// An upload's part copied into a sealed part beside it while the upload goes on in the clear: once
/// the copy has caught up, its sealed metadata is installed, the upload goes on in the sealed part,
/// and its files in the clear go.
struct PartCopy {
    id: UploadId,
    writer: Box<RecordWriter>,
    part_path: PathBuf,
    meta_path: PathBuf,
    /// The bytes copied so far: the part's whole bytes, once caught up.
    copied: u64,
    /// Its metadata's bytes in the clear.
    meta_bytes: u64,
}

/// What one piece of staging's pass did.
pub(super) enum StagingStep {
    /// A file in the clear sealed and its name in the clear removed (an upload's files, once its
    /// copy caught up).
    Converted,
    /// An entry met that leaves as it is, or a copy begun.
    Visited,
    /// A chunk of a part copied: the step ends, a chunk's worth of work done.
    Copied,
    /// The volume had no room for what the piece writes: nothing written, the same piece next.
    Held,
    /// The listing ended; whether it met a file in the clear.
    Ended { met: bool },
}

impl ContentStore {
    /// A pass over staging's files from the start of its listing.
    pub(super) fn staging_pass(&self) -> Result<StagingPass, ContentError> {
        Ok(StagingPass {
            listing: fs::read_dir(self.root.join("staging"))?,
            removed: false,
            met: false,
            copy: None,
        })
    }

    /// One piece of staging's pass: a chunk of the part being copied, or the next entry met. An
    /// upload's terminal record in the clear is sealed at its keyed name and its name in the clear
    /// removed; an upload staged in the clear has its part copied ([`PartCopy`]).
    pub(super) fn staging_step(
        &mut self,
        pass: &mut StagingPass,
        scratch: &mut [u8],
    ) -> Result<StagingStep, ContentError> {
        if pass.copy.is_some() {
            return self.copy_step(pass, scratch);
        }
        let Some(entry) = pass.listing.next() else {
            return Ok(StagingStep::Ended { met: pass.met });
        };
        let path = entry?.path();
        if path.extension().and_then(|x| x.to_str()) != Some("meta") {
            return Ok(StagingStep::Visited);
        }
        let stem = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .ok_or(ContentError::Corrupt)?
            .to_owned();
        let bytes = match self.staging_read(&path, false) {
            Ok(bytes) => bytes,
            Err(ContentError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(StagingStep::Visited);
            }
            Err(error) => return Err(error),
        };
        let Some(record) = decode_staged(&bytes)?.filter(|record| hex(&record.id().0) == stem)
        else {
            // Sealed, or not a record of staging's: left as it is.
            return Ok(StagingStep::Visited);
        };
        let length = u64::try_from(bytes.len()).map_err(|_| ContentError::Capacity)?;
        pass.met = true;
        match record {
            Staged::Terminal(id) => {
                let key = self.staging_key(false)?;
                let room = match disk_reserve(
                    &self.disk,
                    &self.root,
                    DiskKind::Staging,
                    BudgetLane::Ordinary,
                    key.file_bytes(bytes.len())?
                        .checked_add(key.record_bytes())
                        .ok_or(ContentError::Capacity)?,
                ) {
                    Ok(room) => room,
                    Err(ContentError::Capacity) => return Ok(StagingStep::Held),
                    Err(error) => return Err(error),
                };
                let key = self.staging_key(true)?;
                key.install(&self.staging_path(&key, id, "meta")?, &bytes)?;
                room.commit();
                remove_staged(&path)?;
                pass.removed = true;
                self.progress.converted.add(length);
                Ok(StagingStep::Converted)
            }
            Staged::Active(meta) => {
                let Some(upload) = self.uploads.get(&meta.id) else {
                    return Ok(StagingStep::Visited);
                };
                let Part::Clear(_) = &upload.part else {
                    return Ok(StagingStep::Visited);
                };
                let key = self.staging_key(true)?;
                let part_path = self.staging_path(&key, meta.id, "part")?;
                let meta_path = self.staging_path(&key, meta.id, "meta")?;
                let Part::Sealed(writer) =
                    self.create_part(&key, meta.id, &part_path, meta.expected_length)?
                else {
                    return Err(ContentError::Invalid);
                };
                pass.copy = Some(PartCopy {
                    id: meta.id,
                    writer,
                    part_path,
                    meta_path,
                    copied: 0,
                    meta_bytes: length,
                });
                Ok(StagingStep::Visited)
            }
        }
    }

    /// A chunk of the part being copied, or the copy caught up and the upload moved into its sealed
    /// files. An upload finished or expired in the clear meanwhile ends its copy: what the pass
    /// counted of it is gone (its part went, its metadata became a terminal record, which a pass
    /// after meets in the clear).
    fn copy_step(
        &mut self,
        pass: &mut StagingPass,
        scratch: &mut [u8],
    ) -> Result<StagingStep, ContentError> {
        let Some(copy) = pass.copy.as_mut() else {
            return Ok(StagingStep::Visited);
        };
        let live = self
            .uploads
            .get(&copy.id)
            .is_some_and(|upload| matches!(upload.part, Part::Clear(_)));
        if !live {
            // Its finish tallied its part and metadata gone and counted its terminal record
            // (`ContentStore::conversion_staging`), which a pass after meets in the clear.
            if let Some(copy) = pass.copy.take() {
                remove_staged(&copy.part_path)?;
                pass.removed = true;
            }
            return Ok(StagingStep::Visited);
        }
        let upload = self
            .uploads
            .get_mut(&copy.id)
            .ok_or(ContentError::Invalid)?;
        if copy.copied < upload.offset {
            let count = usize::try_from(upload.offset.saturating_sub(copy.copied))
                .map_err(|_| ContentError::Capacity)?
                .min(scratch.len());
            // The sealed copy promised its records before they are written.
            let room = match disk_reserve(
                &self.disk,
                &self.root,
                DiskKind::Staging,
                BudgetLane::Ordinary,
                record_file::records_bytes(count)?,
            ) {
                Ok(room) => room,
                Err(ContentError::Capacity) => return Ok(StagingStep::Held),
                Err(error) => return Err(error),
            };
            let block = scratch.get_mut(..count).ok_or(ContentError::Capacity)?;
            upload.part.read_at(&upload.part_path, copy.copied, block)?;
            copy.writer.append(block)?;
            room.commit();
            copy.copied = copy
                .copied
                .checked_add(u64::try_from(count).map_err(|_| ContentError::Capacity)?)
                .ok_or(ContentError::Capacity)?;
            return Ok(StagingStep::Copied);
        }
        // Caught up: the copy durable, its metadata sealed beside it, the upload moved into them,
        // and only then its files in the clear removed (a crash between leaves both forms, which an
        // open settles: `settle`).
        let bytes = postcard::to_stdvec(&upload.meta)?;
        let Some(copy) = pass.copy.take() else {
            return Ok(StagingStep::Visited);
        };
        copy.writer.sync()?;
        let key = self.staging_key(true)?;
        key.install(&copy.meta_path, &bytes)?;
        let upload = self
            .uploads
            .get_mut(&copy.id)
            .ok_or(ContentError::Invalid)?;
        let clear_part = std::mem::replace(&mut upload.part_path, copy.part_path);
        let clear_meta = std::mem::replace(&mut upload.meta_path, copy.meta_path);
        upload.part = Part::Sealed(copy.writer);
        remove_staged(&clear_part)?;
        remove_staged(&clear_meta)?;
        pass.removed = true;
        self.progress.converted.add(copy.meta_bytes);
        self.progress.converted.add(copy.copied);
        Ok(StagingStep::Converted)
    }
}

/// Which of an upload's two forms a crash in its conversion left stands: a terminal record over
/// metadata (its identity is fenced), a sealed terminal record over one in the clear (the same
/// fence, converted), and of two metadata the clear while its part stands (the sealed copy may be
/// whole or not), the sealed after (the conversion removed the clear part once the copy was whole).
/// Whether the sealed form stands.
fn settle(clear: &Staged, sealed: &Staged, clear_part_stands: bool) -> bool {
    match (clear, sealed) {
        (_, Staged::Terminal(_)) => true,
        (Staged::Terminal(_), Staged::Active(_)) => false,
        (Staged::Active(_), Staged::Active(_)) => !clear_part_stands,
    }
}

/// A staging file removed where it stands: whether it did.
fn remove_staged(path: &Path) -> Result<bool, ContentError> {
    if !terminal::regular(path)? {
        return Ok(false);
    }
    fs::remove_file(path)?;
    Ok(true)
}

/// A staging record: an upload's metadata, or the terminal record that replaced it.
pub(super) enum Staged {
    Active(UploadMeta),
    Terminal(UploadId),
}

impl Staged {
    pub(super) fn id(&self) -> UploadId {
        match self {
            Self::Active(meta) => meta.id,
            Self::Terminal(id) => *id,
        }
    }
}

/// A staging record's bytes decoded: a terminal record by its magic, metadata of its schema; none
/// where they are neither (a sealed file read as if in the clear).
pub(super) fn decode_staged(bytes: &[u8]) -> Result<Option<Staged>, ContentError> {
    if let Some(id) = terminal::decode(bytes)? {
        return Ok(Some(Staged::Terminal(id)));
    }
    let Ok((meta, remaining)) = postcard::take_from_bytes::<UploadMeta>(bytes) else {
        return Ok(None);
    };
    if !remaining.is_empty() || meta.schema != UPLOAD_SCHEMA {
        return Ok(None);
    }
    Ok(Some(Staged::Active(meta)))
}

#[cfg(test)]
impl ContentStore {
    /// Where kind `kind`'s record named `name` lies in the store's form (29 §12.7).
    pub(crate) fn record_path(&self, kind: CustodyRecordKind, name: ContentHash) -> PathBuf {
        #[allow(clippy::unwrap_used)]
        let key = self.record_key(kind, false).unwrap();
        let directory = self.root.join(kind.directory());
        if key.seals() {
            #[allow(clippy::unwrap_used)]
            return key
                .path(&directory, name, super::sealing::Kind::Record)
                .unwrap();
        }
        directory.join(format!("{name}.record"))
    }
    /// Where upload `id`'s part lies in the store's form.
    pub(super) fn part_path(&self, id: UploadId) -> PathBuf {
        #[allow(clippy::unwrap_used)]
        self.staging_path(&self.staging_key(false).unwrap(), id, "part")
            .unwrap()
    }
    /// Where upload `id`'s metadata or terminal record lies in the store's form.
    pub(super) fn meta_path(&self, id: UploadId) -> PathBuf {
        #[allow(clippy::unwrap_used)]
        self.staging_path(&self.staging_key(false).unwrap(), id, "meta")
            .unwrap()
    }
    /// Upload `id`'s staging record as the store reads it, wherever it lies.
    pub(super) fn staged_record_of(&self, id: UploadId) -> Option<Staged> {
        #[allow(clippy::unwrap_used)]
        for (path, sealed) in self.staging_places(id, "meta").unwrap() {
            if let Ok(bytes) = self.staging_read(&path, sealed) {
                #[allow(clippy::unwrap_used)]
                return decode_staged(&bytes).unwrap();
            }
        }
        None
    }
}

/// What the conversion counts of staging's entry at `path` (29 §12.7): an upload's metadata or
/// terminal record in the clear, its name its upload's in the clear, and with metadata the part
/// beside it in the clear; nothing for a part (counted with its metadata), a sealed file, or
/// anything else.
pub(super) fn counted_staging(path: &Path) -> Result<convert::Tally, ContentError> {
    let mut tally = convert::Tally::default();
    if path.extension().and_then(|x| x.to_str()) != Some("meta") {
        return Ok(tally);
    }
    let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
        return Ok(tally);
    };
    if !terminal::regular(path)? {
        return Ok(tally);
    }
    let bytes = match read_bounded(
        path,
        MAX_TRANSFER_MANIFEST_BYTES.max(terminal::RECORD_BYTES),
    ) {
        Ok(bytes) => bytes,
        Err(ContentError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(tally);
        }
        Err(error) => return Err(error),
    };
    let Some(record) = decode_staged(&bytes)?.filter(|record| hex(&record.id().0) == stem) else {
        return Ok(tally);
    };
    tally.add(u64::try_from(bytes.len()).map_err(|_| ContentError::Capacity)?);
    if let Staged::Active(_) = record {
        match fs::symlink_metadata(path.with_extension("part")) {
            Ok(metadata) if metadata.is_file() => tally.add(metadata.len()),
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(tally)
}

#[cfg(test)]
#[path = "staging_tests.rs"]
mod tests;
