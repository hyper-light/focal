//! A session replica's payload log on its volume (doc 31 M3): the bytes of the rows its core
//! spills, in segment files under the session's directory, appended without a sync and read by
//! position. It is a cache of what the replica's records and checkpoints hold, so it is emptied
//! whenever the replica opens and refilled as they are applied; every replica keeps its own.
//!
//! A segment is charged to the volume's disk budget, in the ordinary lane alone, before it is
//! made: a log that cannot have its next segment refuses the payload, and the row keeps its
//! bytes in memory, so the log never takes the completion reserve or the headroom. A sealed
//! segment no row references any longer is removed ([`FilePayloads::sweep`]). A reader handed
//! to another thread holds its own handles, so removing a segment never breaks a read it began.

use focal_core::native::payloads::{PayloadError, PayloadLog, PayloadRead, PayloadRef};
use focal_memory::{BudgetLane, DiskBudget, DiskKind, DiskReservation};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// One segment's bytes at most: what a payload's offset addresses.
pub(crate) const SEGMENT_BYTES: u64 = 64 << 20;
/// The segments the log holds at once, sealed and current: what bounds its files, its handles and
/// its volume (16 GiB at most).
pub(crate) const MAX_SEGMENTS: usize = 256;

/// Where a session keeps its payload log, and the volume it is charged to.
#[derive(Clone)]
pub struct PayloadSource {
    pub root: PathBuf,
    pub disk: DiskBudget,
}

impl std::fmt::Debug for PayloadSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PayloadSource")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

struct Segment {
    file: File,
    /// The volume it holds: outstanding while it fills, committed once it is sealed.
    disk: Option<DiskReservation>,
}

struct Writer {
    segments: BTreeMap<u32, Segment>,
    /// The segment appends go to, and how many bytes it holds.
    current: u32,
    tail: u64,
}

/// A replica's payload log in segment files (see the module).
pub struct FilePayloads {
    root: PathBuf,
    disk: DiskBudget,
    /// A segment's bytes at most ([`SEGMENT_BYTES`] but in tests).
    segment_bytes: u64,
    writer: Mutex<Writer>,
}

impl std::fmt::Debug for FilePayloads {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilePayloads")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

fn segment_path(root: &Path, id: u32) -> PathBuf {
    root.join(format!("{id:010}.seg"))
}

/// Read exactly `into.len()` bytes at `offset`.
fn read_at(file: &File, into: &mut [u8], offset: u64) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_exact_at(into, offset)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        // Each read returns at least one byte or ends the loop: at most `into.len()` passes.
        let mut done = 0usize;
        while done < into.len() {
            let rest = into
                .get_mut(done..)
                .ok_or(std::io::ErrorKind::InvalidInput)?;
            let at = offset
                .checked_add(u64::try_from(done).map_err(|_| std::io::ErrorKind::InvalidInput)?)
                .ok_or(std::io::ErrorKind::InvalidInput)?;
            match file.seek_read(rest, at)? {
                0 => return Err(std::io::ErrorKind::UnexpectedEof.into()),
                read => {
                    done = done
                        .checked_add(read)
                        .ok_or(std::io::ErrorKind::InvalidData)?
                }
            }
        }
        Ok(())
    }
}

/// Write all of `bytes` at `offset`.
fn write_at(file: &File, bytes: &[u8], offset: u64) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.write_all_at(bytes, offset)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        // Each write takes at least one byte or ends the loop: at most `bytes.len()` passes.
        let mut done = 0usize;
        while done < bytes.len() {
            let rest = bytes.get(done..).ok_or(std::io::ErrorKind::InvalidInput)?;
            let at = offset
                .checked_add(u64::try_from(done).map_err(|_| std::io::ErrorKind::InvalidInput)?)
                .ok_or(std::io::ErrorKind::InvalidInput)?;
            match file.seek_write(rest, at)? {
                0 => return Err(std::io::ErrorKind::WriteZero.into()),
                written => {
                    done = done
                        .checked_add(written)
                        .ok_or(std::io::ErrorKind::InvalidData)?
                }
            }
        }
        Ok(())
    }
}

impl FilePayloads {
    /// The log at `source.root`, emptied (see the module), its first segment made and charged.
    pub fn open(source: &PayloadSource) -> std::io::Result<Self> {
        Self::open_with(source, SEGMENT_BYTES)
    }
    /// As [`Self::open`], segments of `segment_bytes` at most.
    pub(crate) fn open_with(source: &PayloadSource, segment_bytes: u64) -> std::io::Result<Self> {
        if segment_bytes == 0 || segment_bytes > SEGMENT_BYTES {
            return Err(std::io::ErrorKind::InvalidInput.into());
        }
        match std::fs::symlink_metadata(&source.root) {
            Ok(_) => {
                // A cache: whatever an earlier run left is let go, a bounded number of files.
                let mut removed = 0usize;
                for entry in std::fs::read_dir(&source.root)? {
                    removed = removed.saturating_add(1);
                    if removed > MAX_SEGMENTS.saturating_mul(4) {
                        return Err(std::io::ErrorKind::InvalidData.into());
                    }
                    let path = entry?.path();
                    if path.extension().is_some_and(|extension| extension == "seg") {
                        std::fs::remove_file(&path)?;
                    }
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                focal_platform::fs::create_dir_all_private(&source.root)?;
            }
            Err(error) => return Err(error),
        }
        let log = Self {
            root: source.root.clone(),
            disk: source.disk.clone(),
            segment_bytes,
            writer: Mutex::new(Writer {
                segments: BTreeMap::new(),
                current: 0,
                tail: 0,
            }),
        };
        {
            let mut writer = log
                .writer
                .lock()
                .map_err(|_| std::io::Error::other("payload log writer"))?;
            let first = log.segment(0)?;
            writer.segments.insert(0, first);
        }
        Ok(log)
    }

    /// Segment `id`, made new and charged to the volume, sampled first when its envelope's
    /// cadence says so (an unsampled volume admits nothing under a watermark).
    fn segment(&self, id: u32) -> std::io::Result<Segment> {
        self.disk
            .refresh_with(|| focal_platform::available_space(&self.root));
        let disk = self
            .disk
            .reserve(DiskKind::Payload, BudgetLane::Ordinary, self.segment_bytes)
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::StorageFull))?;
        let file =
            focal_platform::fs::create_private_new(&segment_path(&self.root, id), true, true)?;
        Ok(Segment {
            file,
            disk: Some(disk),
        })
    }

    /// Remove every sealed segment no row references, `live` naming those that some row does:
    /// the committed rows, every pending candidate's, a restore's. The current segment stays.
    /// How many it removed.
    pub fn sweep(&self, live: &BTreeSet<u32>) -> usize {
        let Ok(mut writer) = self.writer.lock() else {
            return 0;
        };
        let current = writer.current;
        let dead: Vec<u32> = writer
            .segments
            .keys()
            .copied()
            .filter(|id| *id != current && !live.contains(id))
            .collect();
        let mut removed = 0usize;
        for id in dead {
            // A removal that fails keeps the segment, and the next sweep tries again.
            if std::fs::remove_file(segment_path(&self.root, id)).is_ok()
                && writer.segments.remove(&id).is_some()
            {
                removed = removed.saturating_add(1);
            }
        }
        removed
    }

    /// The segments the log holds now, sealed and current.
    pub fn segments(&self) -> usize {
        self.writer.lock().map_or(0, |writer| writer.segments.len())
    }
}

impl PayloadRead for FilePayloads {
    fn read(&self, at: PayloadRef, into: &mut [u8]) -> Result<(), PayloadError> {
        if into.len() != at.len() {
            return Err(PayloadError::Corrupt);
        }
        let writer = self.writer.lock().map_err(|_| PayloadError::Unavailable)?;
        let segment = writer
            .segments
            .get(&at.segment())
            .ok_or(PayloadError::Unavailable)?;
        read_at(&segment.file, into, u64::from(at.offset()))
            .map_err(|_| PayloadError::Unavailable)?;
        if at.holds(into) {
            Ok(())
        } else {
            Err(PayloadError::Corrupt)
        }
    }
}

impl PayloadLog for FilePayloads {
    fn append(&self, bytes: &[u8]) -> Option<PayloadRef> {
        let len = u64::try_from(bytes.len()).ok()?;
        if len > self.segment_bytes {
            return None;
        }
        let mut writer = self.writer.lock().ok()?;
        if writer.tail.checked_add(len)? > self.segment_bytes {
            if writer.segments.len() >= MAX_SEGMENTS {
                return None;
            }
            let next = writer.current.checked_add(1)?;
            let segment = self.segment(next).ok()?;
            // The sealed segment's bytes are on the volume: its charge is spent.
            let current = writer.current;
            if let Some(sealed) = writer.segments.get_mut(&current)
                && let Some(disk) = sealed.disk.take()
            {
                disk.commit();
            }
            writer.segments.insert(next, segment);
            writer.current = next;
            writer.tail = 0;
        }
        let current = writer.current;
        let offset = writer.tail;
        let segment = writer.segments.get(&current)?;
        write_at(&segment.file, bytes, offset).ok()?;
        let at = PayloadRef::at(current, u32::try_from(offset).ok()?, bytes).ok()?;
        writer.tail = offset.checked_add(len)?;
        Some(at)
    }

    fn sweep(&self, live: &BTreeSet<u32>) -> usize {
        FilePayloads::sweep(self, live)
    }
    fn held_segments(&self) -> usize {
        self.segments()
    }
    fn reader(&self) -> Result<Box<dyn PayloadRead + Send>, PayloadError> {
        let writer = self.writer.lock().map_err(|_| PayloadError::Unavailable)?;
        let mut segments = BTreeMap::new();
        for (id, segment) in &writer.segments {
            let file = segment
                .file
                .try_clone()
                .map_err(|_| PayloadError::Unavailable)?;
            segments.insert(*id, file);
        }
        Ok(Box::new(FileReader { segments }))
    }
}

/// What a payload log held when the reader was made, through handles of its own.
#[derive(Debug)]
struct FileReader {
    segments: BTreeMap<u32, File>,
}

impl PayloadRead for FileReader {
    fn read(&self, at: PayloadRef, into: &mut [u8]) -> Result<(), PayloadError> {
        if into.len() != at.len() {
            return Err(PayloadError::Corrupt);
        }
        let file = self
            .segments
            .get(&at.segment())
            .ok_or(PayloadError::Unavailable)?;
        read_at(file, into, u64::from(at.offset())).map_err(|_| PayloadError::Unavailable)?;
        if at.holds(into) {
            Ok(())
        } else {
            Err(PayloadError::Corrupt)
        }
    }
}

#[cfg(test)]
#[path = "native_payloads_tests.rs"]
mod tests;
