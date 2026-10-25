//! A record file (29 §12.6, §12.7): a file appended to in place and sealed as it is written. A cold
//! stream's open chunk is one, and a whole chunk cut from it is the same file renamed into place,
//! so a cut writes nothing again; an upload being staged and a chunk a transfer is receiving are
//! others, parts that a writer opens again to go on with where it stopped. A file is its header
//! (its kind's magic and the file's ID, in the clear) and its records, each a block of its length
//! and then a block of its bytes, every block sealed with AES-256-GCM under the file's key (derived
//! from its directory's key and the file's ID, `sealing::DomainKey::record_key`) at the nonce of its
//! place among the file's blocks. A block moved within its file, cut short, or carried from another
//! file opens to nothing, and a file of one kind is not read as another's. A record holds at most
//! [`RECORD_BYTES`]: an append longer is written as several, so a read opens at most that much more
//! than it wants.
//!
//! - **No nonce seals two plaintexts.** Every file has a key of its own (its ID random, 128 bits),
//!   and a block's place is its nonce under it: a writer only appends, and a writer that fails is
//!   not written to again. A part opened again whose last append a crash cut short is written
//!   again whole, its whole records copied into a file of a new ID under a new key, before anything
//!   is appended ([`RecordWriter::resume`]): what the cut append left of its blocks may lie on the
//!   volume, and its places are never sealed over under its key. A cold stream's open writes its
//!   open chunk into a new file.
//! - **Bounded.** A writer holds its file's key (an AES-GCM context, 592 bytes with its allocation)
//!   and where reads begin, one place for each [`RECORD_BYTES`] of the most plaintext the file
//!   takes, sized when the file is made. A record is sealed in a buffer of its own, at most a
//!   record and its overhead long, while it is written, and written with one write.
use hyper_seal::TAG;
use hyper_seal::stream::VersionKey;

use super::*;

/// Format: the magic a cold stream's record file begins with. A sealed file begins with its
/// header's version, so the two are told apart by their first byte.
pub(super) const COLD_MAGIC: &[u8; 8] = b"FCLCOLD1";
/// Format: the magic a part's record file begins with: an upload being staged, or a chunk a
/// transfer is receiving.
pub(super) const PART_MAGIC: &[u8; 8] = b"FCLPART1";
/// Format: bytes of a cold record file's header: the magic and the file's ID.
pub(super) const HEADER: usize = 8 + 16;
/// Format: bytes of a part's header: the magic, the file's ID, and a tag sealed under the part's key
/// at a place no record takes ([`BINDING`]), which says whose part the file is: its key is kept for
/// its owner (an upload, a chunk), so a part under another owner's name fails here, before any of
/// its records is taken for a crash's leftovers.
pub(super) const PART_HEADER: usize = HEADER + TAG;
/// What a part's header tag is sealed at: a version no record's block takes (theirs is
/// [`VERSION`]).
const BINDING: u64 = 1;

/// The header's bytes of a record file of the kind `magic` names.
fn header_of(magic: &[u8; 8]) -> usize {
    if magic == PART_MAGIC {
        PART_HEADER
    } else {
        HEADER
    }
}
/// The most plaintext one record holds.
pub(super) const RECORD_BYTES: usize = 64 * 1024;
/// Format: bytes of a record's length block: the length (u32, little-endian) and its tag.
const LENGTH_BLOCK: usize = 4 + TAG;
/// Bytes a record takes beyond its plaintext: its length block and its bytes' tag.
pub(super) const RECORD_OVERHEAD: usize = LENGTH_BLOCK + TAG;

/// What a record's blocks are sealed at: the version of every block of a file (its key is its own).
const VERSION: u64 = 0;

/// How much of a record file [`RecordWriter::resume`] opens as it reads it through.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Verify {
    /// Every block, lengths and bytes: what an upload's part takes once an open (29 §12.7).
    All,
    /// Every length block, each record's bytes passed over by their stated length: what a part a
    /// transfer is receiving takes at each part it is sent, its bytes opened and checked against
    /// the chunk's hash once the chunk is whole, so a part's bytes are opened once, not once a
    /// part sent.
    Lengths,
}

/// The writer of one record file.
pub(super) struct RecordWriter {
    file: File,
    key: VersionKey,
    /// Its kind's magic, which a file written again for it begins with too.
    magic: &'static [u8; 8],
    /// Blocks written: the next block's place.
    blocks: u32,
    /// Plaintext bytes written.
    plain: u32,
    /// File bytes written: where the next record begins.
    size: u64,
    /// Where reads begin: the first record at or past each [`RECORD_BYTES`] of the plaintext, as
    /// (its plaintext offset, its file offset, its length block's place).
    starts: Vec<Start>,
    /// The plaintext offset past which the next record is a place reads begin from.
    next_start: u32,
}

/// A place a read begins from ([`RecordWriter::starts`]).
#[derive(Clone, Copy, Debug)]
struct Start {
    plain: u32,
    file: u64,
    block: u32,
}

/// The file bytes `bytes` of plaintext take as records appended at once: the plaintext, and a
/// record's overhead for every [`RECORD_BYTES`] of it begun. What an append's room promises.
pub(super) fn records_bytes(bytes: usize) -> Result<u64, ContentError> {
    let records = bytes.div_ceil(RECORD_BYTES);
    records
        .checked_mul(RECORD_OVERHEAD)
        .and_then(|overhead| overhead.checked_add(bytes))
        .and_then(|total| u64::try_from(total).ok())
        .ok_or(ContentError::Capacity)
}

impl RecordWriter {
    /// A new record file of the kind `magic` names at `path`, which must not stand, its header
    /// written: its ID `id` and its key `key`, made for that ID. `most` is the most plaintext the
    /// file takes (a chunk, an upload), which sizes where reads begin; a file's places count its
    /// plaintext in 32 bits, so more is refused.
    pub(super) fn create(
        path: &Path,
        magic: &'static [u8; 8],
        id: [u8; 16],
        key: VersionKey,
        most: usize,
    ) -> Result<Self, ContentError> {
        let starts = starts_for(most)?;
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(path)?;
        let mut header = [0u8; PART_HEADER];
        let (head, rest) = header
            .split_at_mut_checked(magic.len())
            .ok_or(ContentError::Capacity)?;
        head.copy_from_slice(magic);
        let (file_id, binding) = rest
            .split_at_mut_checked(id.len())
            .ok_or(ContentError::Capacity)?;
        file_id.copy_from_slice(&id);
        if magic == PART_MAGIC {
            let tag = key
                .seal(BINDING, 0, false, &mut [])
                .map_err(|_| ContentError::Corrupt)?;
            binding.copy_from_slice(&tag);
        }
        let length = header_of(magic);
        file.write_all(header.get(..length).ok_or(ContentError::Capacity)?)?;
        Ok(Self {
            file,
            key,
            magic,
            blocks: 0,
            plain: 0,
            size: u64::try_from(length).map_err(|_| ContentError::Capacity)?,
            starts,
            next_start: 0,
        })
    }

    /// The record file of the kind `magic` names at `path` opened to be appended to again: every
    /// record read through, opened as `verify` says and placed, the writer at its end. `key_of`
    /// gives the key of a file's ID, the one its header names or a new one's; `most` is the most
    /// plaintext the file takes; `torn` the most bytes one append writes ([`records_bytes`] of its
    /// largest).
    ///
    /// A file whose last records do not open, no more than `torn` bytes from the first of them to
    /// its end, is one whose last append a crash cut short: its whole records are copied into a
    /// file of a new ID under a new key, beside it (`.resume`), which is synced and renamed over
    /// it, before the writer appends anything. What the cut append wrote may lie on the volume, and
    /// its blocks' places are never sealed over under its key. A record that does not open further
    /// from the end, a header of another kind, or a file holding more than `most` is corrupt.
    pub(super) fn resume(
        path: &Path,
        magic: &'static [u8; 8],
        key_of: &dyn Fn(&[u8; 16]) -> Result<VersionKey, ContentError>,
        most: usize,
        torn: u64,
        verify: Verify,
    ) -> Result<Self, ContentError> {
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        let length = file.metadata()?.len();
        let length_of_header = header_of(magic);
        let mut header = [0u8; PART_HEADER];
        let head = header
            .get_mut(..length_of_header)
            .ok_or(ContentError::Capacity)?;
        if read_full(&mut file, head)? != length_of_header {
            return Err(ContentError::Corrupt);
        }
        let id = record_id(&header, magic).ok_or(ContentError::Corrupt)?;
        let key = key_of(&id)?;
        if magic == PART_MAGIC {
            // Whose part this is, before any record is read: another owner's opens to nothing.
            let tag: [u8; TAG] = header
                .get(HEADER..PART_HEADER)
                .and_then(|tag| tag.try_into().ok())
                .ok_or(ContentError::Corrupt)?;
            key.open(BINDING, 0, false, &mut [], &tag)
                .map_err(|_| ContentError::Corrupt)?;
        }
        let mut writer = Self {
            key,
            file,
            magic,
            blocks: 0,
            plain: 0,
            size: u64::try_from(length_of_header).map_err(|_| ContentError::Capacity)?,
            starts: starts_for(most)?,
            next_start: 0,
        };
        let most_plain = u32::try_from(most).map_err(|_| ContentError::Capacity)?;
        let mut buffer = Vec::new();
        // A record a step, each at least a byte: no more than the most plaintext the file takes.
        for _ in 0..=most {
            writer.mark_start()?;
            let mut records = Records {
                file: &mut writer.file,
                key: &writer.key,
                block: writer.blocks,
            };
            let read = match verify {
                Verify::All => records
                    .next(&mut buffer)
                    .map(|record| record.map(<[u8]>::len)),
                Verify::Lengths => records.skip(length),
            };
            match read {
                Ok(None) => return Ok(writer),
                Ok(Some(record)) => {
                    let record_length =
                        u32::try_from(record).map_err(|_| ContentError::Capacity)?;
                    let plain = writer
                        .plain
                        .checked_add(record_length)
                        .filter(|plain| *plain <= most_plain)
                        .ok_or(ContentError::Corrupt)?;
                    let sealed = records_bytes(record)?;
                    writer.blocks = records.block;
                    writer.plain = plain;
                    writer.size = writer
                        .size
                        .checked_add(sealed)
                        .ok_or(ContentError::Capacity)?;
                }
                Err(ContentError::Corrupt) => {
                    if length.saturating_sub(writer.size) > torn {
                        return Err(ContentError::Corrupt);
                    }
                    return writer.rewritten(path, key_of);
                }
                Err(error) => return Err(error),
            }
        }
        Err(ContentError::Corrupt)
    }

    /// The writer's whole records written again into a file of a new ID under a new key beside it,
    /// synced, and renamed over it: the writer goes on in the new file ([`Self::resume`]).
    fn rewritten(
        self,
        path: &Path,
        key_of: &dyn Fn(&[u8; 16]) -> Result<VersionKey, ContentError>,
    ) -> Result<Self, ContentError> {
        let temporary = path.with_extension("resume");
        match fs::remove_file(&temporary) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let id = hyper_seal::keys::KeyId::random()
            .map_err(|_| {
                ContentError::Io(std::io::Error::other(
                    "the operating system's random source failed",
                ))
            })?
            .0;
        let most = self
            .starts
            .capacity()
            .saturating_sub(1)
            .saturating_mul(RECORD_BYTES);
        let mut copy = Self::create(&temporary, self.magic, id, key_of(&id)?, most)?;
        let mut old = File::open(path)?;
        old.seek(SeekFrom::Start(
            u64::try_from(header_of(self.magic)).map_err(|_| ContentError::Capacity)?,
        ))?;
        let mut records = Records {
            file: &mut old,
            key: &self.key,
            block: 0,
        };
        let mut buffer = Vec::new();
        // A record a step, the whole records the writer read: no more than its blocks.
        for _ in 0..=self.blocks {
            if records.block >= self.blocks {
                break;
            }
            let record = records.next(&mut buffer)?.ok_or(ContentError::Corrupt)?;
            copy.append(record)?;
        }
        if copy.plain != self.plain {
            return Err(ContentError::Corrupt);
        }
        copy.sync()?;
        // Every handle on either file closed before the rename: Windows renames no open file.
        drop(old);
        drop(self);
        let Self {
            file,
            key,
            magic,
            blocks,
            plain,
            size,
            starts,
            next_start,
        } = copy;
        drop(file);
        focal_platform::fs::atomic_replace(&temporary, path)?;
        sync_directory(path.parent().ok_or(ContentError::Invalid)?)?;
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        file.seek(SeekFrom::Start(size))?;
        Ok(Self {
            file,
            key,
            magic,
            blocks,
            plain,
            size,
            starts,
            next_start,
        })
    }

    /// The file's metadata: when it was last written.
    pub(super) fn metadata(&self) -> Result<fs::Metadata, ContentError> {
        self.file.metadata().map_err(ContentError::from)
    }

    /// The plaintext bytes written.
    pub(super) fn plain(&self) -> u64 {
        u64::from(self.plain)
    }

    /// What was appended made durable.
    pub(super) fn sync(&self) -> Result<(), ContentError> {
        self.file.sync_all().map_err(ContentError::from)
    }

    /// The next record's place kept where reads begin, if its plaintext offset reached the next
    /// [`RECORD_BYTES`] since the last place.
    fn mark_start(&mut self) -> Result<(), ContentError> {
        if self.plain < self.next_start {
            return Ok(());
        }
        if self.starts.len() == self.starts.capacity() {
            return Err(ContentError::Capacity);
        }
        self.starts.push(Start {
            plain: self.plain,
            file: self.size,
            block: self.blocks,
        });
        let step = u32::try_from(RECORD_BYTES).map_err(|_| ContentError::Capacity)?;
        self.next_start = self
            .plain
            .checked_div(step)
            .and_then(|index| index.checked_add(1))
            .and_then(|index| index.checked_mul(step))
            .ok_or(ContentError::Capacity)?;
        Ok(())
    }

    /// The bytes the writer holds beyond itself: its key's context and where reads begin, what a
    /// stream charges for it.
    pub(super) fn held_bytes(&self) -> usize {
        // aws-lc's EVP_AEAD_CTX with OPENSSL_malloc's prefix (29 §12.1).
        const KEY_CONTEXT: usize = 592;
        KEY_CONTEXT.saturating_add(self.starts.capacity().saturating_mul(size_of::<Start>()))
    }

    /// `bytes` appended as records of at most [`RECORD_BYTES`]: each sealed whole (its length block,
    /// its bytes and their tag) in a buffer of its own and written at once, one write a record as a
    /// stream in the clear writes once an append: what an append costs is its writes (29 §12.6).
    /// Nothing is synced.
    pub(super) fn append(&mut self, bytes: &[u8]) -> Result<(), ContentError> {
        let mut buffer = Vec::new();
        buffer
            .try_reserve_exact(
                bytes
                    .len()
                    .min(RECORD_BYTES)
                    .saturating_add(RECORD_OVERHEAD),
            )
            .map_err(|_| ContentError::Capacity)?;
        for record in bytes.chunks(RECORD_BYTES) {
            self.mark_start()?;
            let length = u32::try_from(record.len()).map_err(|_| ContentError::Capacity)?;
            let next = self.blocks.checked_add(1).ok_or(ContentError::Capacity)?;
            buffer.clear();
            buffer.extend_from_slice(&length.to_le_bytes());
            buffer.extend_from_slice(&[0u8; TAG]);
            buffer.extend_from_slice(record);
            let (length_block, payload) = buffer
                .split_at_mut_checked(LENGTH_BLOCK)
                .ok_or(ContentError::Capacity)?;
            let (plain, tag) = length_block
                .split_at_mut_checked(4)
                .ok_or(ContentError::Capacity)?;
            let sealed = self
                .key
                .seal(VERSION, self.blocks, false, plain)
                .map_err(|_| ContentError::Corrupt)?;
            tag.copy_from_slice(&sealed);
            let sealed = self
                .key
                .seal(VERSION, next, false, payload)
                .map_err(|_| ContentError::Corrupt)?;
            buffer.extend_from_slice(&sealed);
            self.file.write_all(&buffer)?;
            self.blocks = next.checked_add(1).ok_or(ContentError::Capacity)?;
            self.plain = self
                .plain
                .checked_add(length)
                .ok_or(ContentError::Capacity)?;
            self.size = u64::try_from(buffer.len())
                .ok()
                .and_then(|written| self.size.checked_add(written))
                .ok_or(ContentError::Capacity)?;
        }
        Ok(())
    }

    /// The file's whole plaintext, read back from it at `path` through a handle of its own and
    /// opened in place ([`read_all`]): what a capture carries of the open chunk.
    pub(super) fn read_whole(&self, path: &Path) -> Result<Vec<u8>, ContentError> {
        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(
            u64::try_from(HEADER).map_err(|_| ContentError::Capacity)?,
        ))?;
        let plain = read_all(
            &mut file,
            &self.key,
            usize::try_from(self.plain).map_err(|_| ContentError::Capacity)?,
        )?;
        if plain.len() != usize::try_from(self.plain).map_err(|_| ContentError::Capacity)? {
            return Err(ContentError::Corrupt);
        }
        Ok(plain)
    }

    /// The plaintext from `offset`, `out.len()` bytes of it, read back from the file through a
    /// handle of its own: from the place nearest before `offset` reads begin from, each record
    /// opened in a buffer of its own until `out` is filled. A range past what was written is
    /// refused.
    pub(super) fn read(
        &self,
        path: &Path,
        offset: u32,
        out: &mut [u8],
    ) -> Result<(), ContentError> {
        let end = u32::try_from(out.len())
            .ok()
            .and_then(|length| offset.checked_add(length))
            .ok_or(ContentError::Capacity)?;
        if end > self.plain {
            return Err(ContentError::Invalid);
        }
        if out.is_empty() {
            return Ok(());
        }
        let start = self
            .starts
            .iter()
            .rev()
            .find(|start| start.plain <= offset)
            .copied()
            .ok_or(ContentError::Corrupt)?;
        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(start.file))?;
        let mut reader = Records {
            file: &mut file,
            key: &self.key,
            block: start.block,
        };
        let mut at = start.plain;
        let mut buffer = Vec::new();
        let mut filled = 0usize;
        // A record a step: from the place reads begin to the range's end, fewer than the
        // file's records.
        for _ in 0..=self.blocks {
            if filled == out.len() {
                return Ok(());
            }
            let record = reader.next(&mut buffer)?.ok_or(ContentError::Corrupt)?;
            let length = u32::try_from(record.len()).map_err(|_| ContentError::Capacity)?;
            let record_end = at.checked_add(length).ok_or(ContentError::Capacity)?;
            if record_end > offset {
                let from = usize::try_from(offset.saturating_sub(at))
                    .map_err(|_| ContentError::Capacity)?;
                let take = record
                    .len()
                    .saturating_sub(from)
                    .min(out.len().saturating_sub(filled));
                let to = from.checked_add(take).ok_or(ContentError::Capacity)?;
                let target = filled.checked_add(take).ok_or(ContentError::Capacity)?;
                out.get_mut(filled..target)
                    .ok_or(ContentError::Capacity)?
                    .copy_from_slice(record.get(from..to).ok_or(ContentError::Corrupt)?);
                filled = target;
            }
            at = record_end;
        }
        Err(ContentError::Corrupt)
    }
}

/// A record file's records read in order.
struct Records<'r> {
    file: &'r mut File,
    key: &'r VersionKey,
    /// The next record's length block's place.
    block: u32,
}

impl Records<'_> {
    /// The next record's plaintext, opened in `buffer`, which grows to the longest record read
    /// with it and its tag; none at the file's end. A record cut short, or longer than a record
    /// holds, is corrupt.
    fn next<'b>(&mut self, buffer: &'b mut Vec<u8>) -> Result<Option<&'b [u8]>, ContentError> {
        let Some(length) = self.length()? else {
            return Ok(None);
        };
        buffer.clear();
        buffer
            .try_reserve_exact(length.saturating_add(TAG))
            .map_err(|_| ContentError::Capacity)?;
        self.bytes_into(buffer, length)?;
        Ok(Some(buffer.as_slice()))
    }

    /// The next record's plaintext opened in place at the end of `into`, which must have the room
    /// for its bytes and their tag past what it holds: a reader of a whole file takes that room
    /// once. None at the file's end. A record cut short, longer than a record holds, or longer than
    /// the room left (more than the file could hold) is corrupt.
    fn next_into(&mut self, into: &mut Vec<u8>) -> Result<Option<usize>, ContentError> {
        let Some(length) = self.length()? else {
            return Ok(None);
        };
        self.bytes_into(into, length)?;
        Ok(Some(length))
    }

    /// The next record's length, its length block opened and its bytes passed over by it: none at
    /// the file's end. A record whose bytes and tag the file's `length` does not hold is cut short.
    fn skip(&mut self, length: u64) -> Result<Option<usize>, ContentError> {
        let Some(record) = self.length()? else {
            return Ok(None);
        };
        let past = u64::try_from(record.saturating_add(TAG)).map_err(|_| ContentError::Capacity)?;
        let at = self.file.stream_position()?;
        let end = at.checked_add(past).ok_or(ContentError::Corrupt)?;
        if end > length {
            return Err(ContentError::Corrupt);
        }
        self.file.seek(SeekFrom::Start(end))?;
        self.block = self.block.checked_add(2).ok_or(ContentError::Corrupt)?;
        Ok(Some(record))
    }

    /// The next record's length block, opened: the record's length; none at the file's end.
    fn length(&mut self) -> Result<Option<usize>, ContentError> {
        let mut length_block = [0u8; LENGTH_BLOCK];
        match read_full(self.file, &mut length_block)? {
            0 => return Ok(None),
            LENGTH_BLOCK => {}
            _ => return Err(ContentError::Corrupt),
        }
        let (plain, tag) = length_block
            .split_at_mut_checked(4)
            .ok_or(ContentError::Corrupt)?;
        let tag: [u8; TAG] = (&*tag).try_into().map_err(|_| ContentError::Corrupt)?;
        self.key
            .open(VERSION, self.block, false, plain, &tag)
            .map_err(|_| ContentError::Corrupt)?;
        let length = usize::try_from(u32::from_le_bytes(
            (&*plain).try_into().map_err(|_| ContentError::Corrupt)?,
        ))
        .map_err(|_| ContentError::Capacity)?;
        if length == 0 || length > RECORD_BYTES {
            return Err(ContentError::Corrupt);
        }
        Ok(Some(length))
    }

    /// The record's bytes and tag, `length` of them and the tag, read into the room past the end
    /// of `into` and opened there; `into` grows by the plaintext alone. Nothing is allocated: room
    /// short of them is corrupt.
    fn bytes_into(&mut self, into: &mut Vec<u8>, length: usize) -> Result<(), ContentError> {
        let start = into.len();
        let sealed = length.checked_add(TAG).ok_or(ContentError::Corrupt)?;
        let end = start.checked_add(sealed).ok_or(ContentError::Corrupt)?;
        if end > into.capacity() {
            return Err(ContentError::Corrupt);
        }
        into.resize(end, 0);
        let opened = match into.get_mut(start..end) {
            Some(target) => self.open_record(target, length),
            None => Err(ContentError::Corrupt),
        };
        // The tag goes, and on a failure what was read with it: only opened bytes stay.
        into.truncate(if opened.is_ok() {
            start.saturating_add(length)
        } else {
            start
        });
        opened
    }

    /// A record's bytes and their tag read into `target`, exactly as long, and opened there.
    fn open_record(&mut self, target: &mut [u8], length: usize) -> Result<(), ContentError> {
        if read_full(self.file, target)? != target.len() {
            return Err(ContentError::Corrupt);
        }
        let (bytes, tag) = target
            .split_at_mut_checked(length)
            .ok_or(ContentError::Corrupt)?;
        let tag: [u8; TAG] = (&*tag).try_into().map_err(|_| ContentError::Corrupt)?;
        let next = self.block.checked_add(1).ok_or(ContentError::Corrupt)?;
        self.key
            .open(VERSION, next, false, bytes, &tag)
            .map_err(|_| ContentError::Corrupt)?;
        self.block = next.checked_add(1).ok_or(ContentError::Corrupt)?;
        Ok(())
    }
}

/// Bytes read into `buffer` until it is full or the file ends: how many.
pub(super) fn read_full(file: &mut File, buffer: &mut [u8]) -> Result<usize, ContentError> {
    let mut filled = 0usize;
    // A read a step, each taking at least a byte until the end: fewer than the buffer's bytes.
    for _ in 0..=buffer.len() {
        let Some(rest) = buffer.get_mut(filled..) else {
            break;
        };
        if rest.is_empty() {
            break;
        }
        match file.read(rest) {
            Ok(0) => break,
            Ok(read) => filled = filled.saturating_add(read),
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(filled)
}

/// The ID a record file's header names, where `bytes` begin with one of the kind `magic` names:
/// none where they begin with anything else (another kind's, a sealed file, a file in the clear).
pub(super) fn record_id(bytes: &[u8], magic: &[u8; 8]) -> Option<[u8; 16]> {
    if bytes.get(..magic.len())? != magic {
        return None;
    }
    bytes.get(magic.len()..HEADER)?.try_into().ok()
}

/// Where reads begin, room for a file of `most` plaintext bytes: one place for each
/// [`RECORD_BYTES`] of it begun and the first. A file's places count its plaintext in 32 bits.
fn starts_for(most: usize) -> Result<Vec<Start>, ContentError> {
    u32::try_from(most).map_err(|_| ContentError::Capacity)?;
    let mut starts = Vec::new();
    starts
        .try_reserve_exact(most.div_ceil(RECORD_BYTES).saturating_add(1))
        .map_err(|_| ContentError::Capacity)?;
    Ok(starts)
}

/// The whole plaintext of the record file `file` (its header read already, `key` its key), at most
/// `most` bytes: what a cut chunk holds, or an open chunk captured. More is corrupt. Each record is
/// opened in place in the plaintext's own buffer, whose room is taken once: the least of `most`
/// with a record's tag and what the file's records can hold (its bytes past the header). So the
/// read holds its plaintext and a tag's room, and a chunk of many small records (a marks stream's,
/// 65,536 of 16 bytes) is not grown a record at a time.
pub(super) fn read_all(
    file: &mut File,
    key: &VersionKey,
    most: usize,
) -> Result<Vec<u8>, ContentError> {
    let held = usize::try_from(file.metadata()?.len())
        .unwrap_or(usize::MAX)
        .saturating_sub(HEADER)
        .min(most.saturating_add(TAG));
    let mut plain = Vec::new();
    plain
        .try_reserve_exact(held)
        .map_err(|_| ContentError::Capacity)?;
    let mut reader = Records {
        file,
        key,
        block: 0,
    };
    // A record a step, each at least a byte: fewer than the most bytes a file may hold.
    for _ in 0..=most {
        if reader.next_into(&mut plain)?.is_none() {
            return Ok(plain);
        }
        if plain.len() > most {
            return Err(ContentError::Corrupt);
        }
    }
    Err(ContentError::Corrupt)
}

#[cfg(test)]
#[path = "record_file_tests.rs"]
mod tests;
