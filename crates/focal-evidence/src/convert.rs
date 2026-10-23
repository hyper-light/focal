//! A store's files in the clear converted to their sealed form (29 §11, "Existing stores"), a file
//! a step, while the store serves.
//!
//! A store whose writer seals and that is not yet marked sealed holds files in either form: what an
//! earlier binary or this one below the fence wrote in the clear, and what its writer has sealed
//! since. Its owner converts the rest when no other work waits ([`ContentStore::convert_step`]):
//! each file in the clear is read, sealed under its domain's key at its keyed name beside it, and
//! only then is its name in the clear removed, so every file lies in at least one form at every
//! moment and a reader that looks in the clear first finds it whatever the conversion does
//! between (`sealing::DomainKey::opened`). A file already sealed (by the writer, or by a
//! conversion cut before its removal) keeps its sealed form and loses its name in the clear. Each
//! domain's directory is flushed as the conversion leaves it, so a removal is durable before the
//! store is marked sealed: a crash before the mark leaves each file whole in one form or both, and
//! the next start resumes from the store's marker (`SEALING`), which the writer keeps until the
//! last domain is done.
//!
//! The quarantine rounds go first, then the store's own objects: a quarantined object restored while
//! the conversion runs comes back in the clear from a round not yet visited into objects not yet
//! visited, and sealed from a round visited, so the pass over the objects meets every file that is
//! still in the clear. The collector holds its passes over objects while the store converts.
//!
//! - **Bounded.** A step visits at most the entries its caller allows and converts at most one file,
//!   a chunk or a manifest no longer than the format's bound, read into the owner's scratch. The
//!   pass visits each round, each domain and each file once.
//! - **Charged.** A file's sealed form (and its domain's record where none stands) is promised its
//!   bytes before it is written; refused, the step is held and nothing written, and the same file is
//!   taken first when the owner asks again.
//! - **Left as it is.** What a domain's directory holds besides chunks and manifests in the clear:
//!   keyed names, parts a transfer is receiving, temporaries, the domain's record; and a file longer
//!   than any the store writes, which was never an object's.
use std::fs::{self, File, OpenOptions, ReadDir};
use std::io::Read;
use std::path::{Path, PathBuf};

use focal_memory::{BudgetLane, DiskKind};
use focal_model::ContentDomainId;

use super::sealing::{self, Kind};
use super::{
    ContentError, ContentStore, disk_reserve, domain_directory, durable_directory, sync_directory,
};

/// What one step of a store's conversion did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Conversion {
    /// A file in the clear was sealed, and its name in the clear removed.
    Converted,
    /// The step's visits went to entries it leaves as they are.
    Visited,
    /// The volume had no room for the next file's sealed form: nothing was written, and the file
    /// is taken first when the owner asks again.
    Held,
    /// Every file is sealed and the store is marked so; or no conversion runs.
    Done,
}

/// Where a store's conversion stands: the quarantine rounds' objects, then the store's own, a
/// domain at a time, a file at a time.
pub(super) struct Converting {
    /// The quarantine rounds still to visit; none once their listing ended.
    rounds: Option<ReadDir>,
    /// Whether the store's own objects have been entered.
    own: bool,
    /// The domains of the tree being visited (a round's `objects`, or the store's own).
    tree: Option<ReadDir>,
    /// The domain being visited.
    domain: Option<Visit>,
}

/// One domain's directory being visited.
struct Visit {
    domain: ContentDomainId,
    directory: PathBuf,
    listing: ReadDir,
    /// A file whose sealed form found no room: taken first at the next step.
    pending: Option<PathBuf>,
    /// A name in the clear was removed since the directory was last flushed.
    removed: bool,
}

/// What a file's visit did.
enum Visited {
    Converted,
    Left,
    Held,
}

impl Converting {
    /// A conversion of the store at `root`, nothing visited yet.
    pub(super) fn begin(root: &Path) -> Result<Self, ContentError> {
        let rounds = match fs::read_dir(root.join("quarantine")) {
            Ok(listing) => Some(listing),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        Ok(Self {
            rounds,
            own: false,
            tree: None,
            domain: None,
        })
    }
}

impl ContentStore {
    /// Whether a conversion runs in this store: its owner calls [`Self::convert_step`] whenever no
    /// other work waits, until it is done.
    pub fn converting(&self) -> bool {
        self.conversion.is_some() && !self.failed
    }

    /// One step of the store's conversion: at most `visits` entries visited and at most one file
    /// converted, its sealed form read into the head of `scratch` (a chunk long, the owner's). A
    /// step that fails ends this conversion; the store still reads either form, and its next open
    /// resumes the conversion from its marker. An I/O failure stops the store as any write's does.
    pub fn convert_step(
        &mut self,
        scratch: &mut [u8],
        visits: usize,
    ) -> Result<Conversion, ContentError> {
        self.check()?;
        let Some(mut state) = self.conversion.take() else {
            return Ok(Conversion::Done);
        };
        let result = self.convert_inner(&mut state, scratch, visits);
        if matches!(
            result,
            Ok(Conversion::Converted | Conversion::Visited | Conversion::Held)
        ) {
            self.conversion = Some(state);
        }
        self.mark_failure(&result);
        result
    }

    fn convert_inner(
        &mut self,
        state: &mut Converting,
        scratch: &mut [u8],
        visits: usize,
    ) -> Result<Conversion, ContentError> {
        for _ in 0..visits {
            if let Some(visit) = &mut state.domain {
                let next = match visit.pending.take() {
                    Some(path) => Some(path),
                    None => match visit.listing.next() {
                        Some(entry) => Some(entry?.path()),
                        None => None,
                    },
                };
                let Some(path) = next else {
                    if visit.removed {
                        sync_directory(&visit.directory)?;
                    }
                    state.domain = None;
                    continue;
                };
                match self.convert_file(visit.domain, &visit.directory, &path, scratch)? {
                    Visited::Converted => {
                        visit.removed = true;
                        return Ok(Conversion::Converted);
                    }
                    Visited::Left => continue,
                    Visited::Held => {
                        visit.pending = Some(path);
                        return Ok(Conversion::Held);
                    }
                }
            }
            if let Some(tree) = &mut state.tree {
                let Some(entry) = tree.next() else {
                    state.tree = None;
                    continue;
                };
                let entry = entry?;
                let Some(domain) = entry.file_name().to_str().and_then(super::gc::parse_domain)
                else {
                    continue;
                };
                let directory = entry.path();
                match fs::read_dir(&directory) {
                    Ok(listing) => {
                        state.domain = Some(Visit {
                            domain,
                            directory,
                            listing,
                            pending: None,
                            removed: false,
                        });
                    }
                    Err(error) if gone(&error) => {}
                    Err(error) => return Err(error.into()),
                }
                continue;
            }
            if let Some(rounds) = &mut state.rounds {
                let Some(entry) = rounds.next() else {
                    state.rounds = None;
                    continue;
                };
                // A round the collector deletes as the conversion lists it is nothing to convert.
                match fs::read_dir(entry?.path().join("objects")) {
                    Ok(listing) => state.tree = Some(listing),
                    Err(error) if gone(&error) => {}
                    Err(error) => return Err(error.into()),
                }
                continue;
            }
            if !state.own {
                state.own = true;
                state.tree = Some(fs::read_dir(self.root.join("objects"))?);
                continue;
            }
            self.mark_sealed()?;
            return Ok(Conversion::Done);
        }
        Ok(Conversion::Visited)
    }

    /// The entry at `path` of `domain`'s directory `directory` (the store's own, or a round's):
    /// where it is a chunk or a manifest in the clear, sealed beside it under its keyed name and
    /// its name in the clear removed; anything else left as it is.
    fn convert_file(
        &mut self,
        domain: ContentDomainId,
        directory: &Path,
        path: &Path,
        scratch: &mut [u8],
    ) -> Result<Visited, ContentError> {
        let (Some(stem), Some(extension)) = (
            path.file_stem().and_then(|stem| stem.to_str()),
            path.extension().and_then(|extension| extension.to_str()),
        ) else {
            return Ok(Visited::Left);
        };
        let kind = match extension {
            "chunk" => Kind::Chunk,
            "manifest" => Kind::Manifest,
            _ => return Ok(Visited::Left),
        };
        let Some(hash) = sealing::parse_hash(stem) else {
            return Ok(Visited::Left);
        };
        let mut file = match File::open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Visited::Left);
            }
            Err(error) => return Err(error.into()),
        };
        // No file the store writes is longer than the format's bound, which the scratch holds: one
        // that is was never an object's.
        let Some(length) = usize::try_from(file.metadata()?.len())
            .ok()
            .filter(|length| *length <= scratch.len())
        else {
            return Ok(Visited::Left);
        };
        // The domain's key lives in its directory among the store's own objects, a round's files
        // sealed under it too, so a restored file opens where it comes back.
        let own = domain_directory(&self.root, domain);
        let key = self.keys.domain(&own, domain, false)?;
        // Sealed already, by the writer since the store began converting or by a conversion cut
        // before its removal: the sealed form stands where it opens to the bytes its name says,
        // and the name in the clear goes; where it does not, the bytes in the clear are sealed
        // over it.
        if key.record_bytes() == 0
            && key.holds_sealed(&key.path(directory, hash, kind)?, hash, scratch)?
        {
            drop(file);
            remove_clear(path)?;
            return Ok(Visited::Converted);
        }
        let room = match disk_reserve(
            &self.disk,
            &self.root,
            DiskKind::Content,
            BudgetLane::Ordinary,
            key.file_bytes(length)?
                .checked_add(key.record_bytes())
                .ok_or(ContentError::Capacity)?,
        ) {
            Ok(room) => room,
            Err(ContentError::Capacity) => return Ok(Visited::Held),
            Err(error) => return Err(error),
        };
        let block = scratch.get_mut(..length).ok_or(ContentError::Capacity)?;
        file.read_exact(block)?;
        // The file is exactly the length it was opened at: one written into since is not a file
        // the store installed (every install renames a whole file into place), and is refused.
        let mut extra = [0u8; 1];
        if file.read(&mut extra)? != 0 {
            return Err(ContentError::Corrupt);
        }
        drop(file);
        durable_directory(&own)?;
        let key = self.keys.writable(&own, domain, key)?;
        // Sealed as the bytes are: a file that does not hash to its name is refused as corrupt by
        // every read in either form, so it is converted as it is, never judged here.
        key.install(&key.path(directory, hash, kind)?, block)?;
        room.commit();
        remove_clear(path)?;
        Ok(Visited::Converted)
    }

    /// Every file is sealed: the store is marked so (each domain's directory flushed as the
    /// conversion left it), its marker of the conversion removed, and nothing in the clear is looked
    /// for from here on.
    pub(super) fn mark_sealed(&mut self) -> Result<(), ContentError> {
        write_marker(&self.root, sealing::SEALED)?;
        remove_marker(&self.root, sealing::SEALING)?;
        self.keys.mark_sealed();
        self.conversion = None;
        Ok(())
    }
}

/// `name` in the store's root, durable when it returns: what marks a conversion begun or ended.
pub(super) fn write_marker(root: &Path, name: &str) -> Result<(), ContentError> {
    OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(root.join(name))?
        .sync_all()?;
    sync_directory(root)?;
    Ok(())
}

/// `name` gone from the store's root, durably.
pub(super) fn remove_marker(root: &Path, name: &str) -> Result<(), ContentError> {
    match fs::remove_file(root.join(name)) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    }
    sync_directory(root)?;
    Ok(())
}

/// A name in the clear removed; its directory is flushed as the conversion leaves it.
fn remove_clear(path: &Path) -> Result<(), ContentError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

/// Whether a directory the conversion lists is gone, or never was one: a round deleted as it is
/// listed, or a file where a directory may stand.
fn gone(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
    )
}

#[cfg(test)]
#[path = "convert_tests.rs"]
mod tests;
