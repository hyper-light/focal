//! A group's own files beside its log ([27] §15.3–§15.5): its records (`meta`: what the group
//! is, and what its entries need to be read) and its state machine's image (`image`: the state
//! at a point, with its configuration). Each is written whole by an install
//! ([`focal_platform::fs::install`]): a temporary name, the bytes, a full flush, the rename and the
//! directory's flush. So a crash leaves the file as it was before the write or as the write left
//! it, never between.
//!
//! The two are separate files so that the irreversible decoder record never rewrites an image,
//! and no image rewrite touches the records. Nothing writes both at once: what must be durable
//! before what is ordered by the rules of [27] §15.5 (O1–O6), kept by their callers.
//!
//! Each file is `MAGIC (8) | VERSION (u32) | LENGTH (u64) | payload | CRC-32 (u32)`, the checksum
//! over everything before it, as focal-log's own records are summed (crc32fast's CRC-32). A file
//! that does not read whole, of another magic, of a version this binary does not know, longer than
//! its bound, or whose checksum does not match is refused, naming the file: it is never read as
//! absent, nor as an earlier version.
//!
//! [27]: ../../../docs/archictecutre/27-consensus-roadmap-and-slates-port.md
use std::io;
use std::path::{Path, PathBuf};

use focal_platform::fs::Medium;
use hyper_raft::proto::ConfState;
use serde::{Deserialize, Serialize};

use crate::NodeConfig;

/// The directory under a data directory that holds every group's own files.
pub const GROUPS_DIR: &str = "raft/groups";
/// A group's records.
pub const META_FILE: &str = "meta";
/// A group's image.
pub const IMAGE_FILE: &str = "image";

const META_MAGIC: &[u8; 8] = b"FOCALGM1";
const IMAGE_MAGIC: &[u8; 8] = b"FOCALGI1";
const VERSION: u32 = 1;
/// Magic, version and length.
const HEADER_BYTES: usize = 8 + 4 + 8;
/// The checksum after the payload.
const TRAILER_BYTES: usize = 4;
/// The longest postcard's encoding of a `u64` or a `usize` is: a varint of 64 bits, ⌈64 / 7⌉
/// bytes (postcard's wire format).
const VARINT: usize = 10;
/// The most a list of a configuration's members encodes to: its length and at most
/// [`hyper_raft::MAX_MEMBERS`] ids.
const MEMBERS: usize = VARINT + hyper_raft::MAX_MEMBERS * VARINT;
/// The most a group's records encode to: the identity's node id, cluster and group ids, its two
/// ticks and three bounds, its voters and learners; the fast track's flag; and the two optional
/// decoder records, a hash and a pair of hashes. A larger file was written by no binary.
pub const META_BOUND: usize =
    VARINT + 16 + 16 + 5 * VARINT + 2 * MEMBERS + 1 + (1 + 32) + (1 + 2 * 32);
/// The most an image's point encodes to: its index and term, a configuration's four lists of
/// members and its flag.
const POINT_BOUND: usize = 2 * VARINT + 4 * MEMBERS + 1;

/// Why a group file was not read or written.
#[derive(Debug, thiserror::Error)]
pub enum GroupFileError {
    /// The medium refused or failed.
    #[error("group file I/O: {0}")]
    Io(#[from] io::Error),
    /// The file is not one this binary wrote whole: named, with what is wrong.
    #[error("group file {file} is corrupt: {reason}")]
    Corrupt {
        file: &'static str,
        reason: &'static str,
    },
    /// The payload did not encode or decode.
    #[error("group file encoding: {0}")]
    Encoding(#[from] postcard::Error),
    /// The file is larger than its bound.
    #[error("group file {file} exceeds its bound of {bound} bytes")]
    Bound { file: &'static str, bound: usize },
}

/// What the group is, and what its entries need to be read ([27] §15.2): its identity, whether it
/// has the fast track, and its decoder floor and transition (18 §4–§5). Written when the group is
/// made and when its floor or transition is set, never with its image.
///
/// [27]: ../../../docs/archictecutre/27-consensus-roadmap-and-slates-port.md
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GroupRecords {
    /// The group's identity, as its identity record stated it (its fast track apart).
    pub identity: NodeConfig,
    /// The group has the fast track.
    pub fast: bool,
    /// The decoder the group's entries require, once set.
    pub decoder_floor: Option<[u8; 32]>,
    /// The ordered successor of the floor, once promised: predecessor and successor.
    pub decoder_transition: Option<([u8; 32], [u8; 32])>,
}

/// The point an image is of and the configuration there: what the log's start moves to once the
/// image is durable (O3).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImagePoint {
    pub index: u64,
    pub term: u64,
    pub configuration: ConfState,
}

/// An image's point as the file holds it: the configuration's lists by name, which the core's
/// type keeps to itself.
#[derive(Serialize, Deserialize)]
struct StoredPoint {
    index: u64,
    term: u64,
    voters: Vec<u64>,
    learners: Vec<u64>,
    voters_outgoing: Vec<u64>,
    learners_next: Vec<u64>,
    auto_leave: bool,
}

impl StoredPoint {
    fn of(point: &ImagePoint) -> Self {
        let c = &point.configuration;
        Self {
            index: point.index,
            term: point.term,
            voters: c.voters.clone(),
            learners: c.learners.clone(),
            voters_outgoing: c.voters_outgoing.clone(),
            learners_next: c.learners_next.clone(),
            auto_leave: c.auto_leave,
        }
    }
    fn point(self) -> ImagePoint {
        ImagePoint {
            index: self.index,
            term: self.term,
            configuration: ConfState {
                voters: self.voters,
                learners: self.learners,
                voters_outgoing: self.voters_outgoing,
                learners_next: self.learners_next,
                auto_leave: self.auto_leave,
            },
        }
    }
}

/// The directory that holds group `group`'s files under data directory `root`.
pub fn group_dir(root: &Path, group: [u8; 16]) -> PathBuf {
    let mut name = String::with_capacity(32);
    for byte in group {
        name.push(hex(byte >> 4));
        name.push(hex(byte & 0x0f));
    }
    root.join(GROUPS_DIR).join(name)
}

fn hex(nibble: u8) -> char {
    char::from_digit(u32::from(nibble), 16).unwrap_or('0')
}

/// Makes group `group`'s directory durable under `root`, every new component with its parent
/// flushed, and returns it.
pub fn create<M: Medium>(
    medium: &mut M,
    root: &Path,
    group: [u8; 16],
) -> Result<PathBuf, GroupFileError> {
    let dir = group_dir(root, group);
    medium.create_dir(&dir)?;
    Ok(dir)
}

/// Writes the group's records whole into `dir`.
pub fn write_records<M: Medium>(
    medium: &mut M,
    dir: &Path,
    records: &GroupRecords,
) -> Result<(), GroupFileError> {
    let payload = postcard::to_stdvec(records)?;
    if payload.len() > META_BOUND {
        return Err(GroupFileError::Bound {
            file: META_FILE,
            bound: META_BOUND,
        });
    }
    let bytes = frame(META_MAGIC, &payload)?;
    focal_platform::fs::install(medium, &dir.join(META_FILE), &bytes)?;
    Ok(())
}

/// The group's records in `dir`; none where no records were ever made durable there. A file that
/// is there and does not read whole is an error, never none.
pub fn read_records<M: Medium>(
    medium: &M,
    dir: &Path,
) -> Result<Option<GroupRecords>, GroupFileError> {
    let path = dir.join(META_FILE);
    if !medium.exists(&path)? {
        return Ok(None);
    }
    let bytes = read_bounded(medium, &path, META_FILE, META_BOUND)?;
    let payload = unframe(META_FILE, META_MAGIC, &bytes)?;
    Ok(Some(postcard::from_bytes(payload)?))
}

/// Writes the group's image whole into `dir`: the point it is of, then its bytes. `bound` is the
/// most an image of this group may hold (the state machine's own bound on its checkpoint).
pub fn write_image<M: Medium>(
    medium: &mut M,
    dir: &Path,
    point: &ImagePoint,
    image: &[u8],
    bound: usize,
) -> Result<(), GroupFileError> {
    if image.len() > bound {
        return Err(GroupFileError::Bound {
            file: IMAGE_FILE,
            bound,
        });
    }
    let header = postcard::to_stdvec(&StoredPoint::of(point))?;
    if header.len() > POINT_BOUND {
        return Err(GroupFileError::Bound {
            file: IMAGE_FILE,
            bound: POINT_BOUND,
        });
    }
    let header_len = u32::try_from(header.len()).map_err(|_| GroupFileError::Bound {
        file: IMAGE_FILE,
        bound,
    })?;
    let mut payload = Vec::new();
    let len = header
        .len()
        .checked_add(image.len())
        .and_then(|n| n.checked_add(4))
        .ok_or(GroupFileError::Bound {
            file: IMAGE_FILE,
            bound,
        })?;
    payload
        .try_reserve_exact(len)
        .map_err(|_| GroupFileError::Bound {
            file: IMAGE_FILE,
            bound,
        })?;
    payload.extend_from_slice(&header_len.to_le_bytes());
    payload.extend_from_slice(&header);
    payload.extend_from_slice(image);
    let bytes = frame(IMAGE_MAGIC, &payload)?;
    focal_platform::fs::install(medium, &dir.join(IMAGE_FILE), &bytes)?;
    Ok(())
}

/// The group's image in `dir` and the point it is of; none where no image was ever made durable
/// there. `bound` is the most an image of this group may hold.
pub fn read_image<M: Medium>(
    medium: &M,
    dir: &Path,
    bound: usize,
) -> Result<Option<(ImagePoint, Vec<u8>)>, GroupFileError> {
    let path = dir.join(IMAGE_FILE);
    if !medium.exists(&path)? {
        return Ok(None);
    }
    let most = bound
        .checked_add(POINT_BOUND)
        .and_then(|n| n.checked_add(HEADER_BYTES + TRAILER_BYTES + 4))
        .ok_or(GroupFileError::Bound {
            file: IMAGE_FILE,
            bound,
        })?;
    let bytes = read_bounded(medium, &path, IMAGE_FILE, most)?;
    let payload = unframe(IMAGE_FILE, IMAGE_MAGIC, &bytes)?;
    let (len, rest) = payload.split_at_checked(4).ok_or(GroupFileError::Corrupt {
        file: IMAGE_FILE,
        reason: "no header length",
    })?;
    let len = u32::from_le_bytes(len.try_into().map_err(|_| GroupFileError::Corrupt {
        file: IMAGE_FILE,
        reason: "no header length",
    })?);
    let len = usize::try_from(len).map_err(|_| GroupFileError::Corrupt {
        file: IMAGE_FILE,
        reason: "header length",
    })?;
    let (header, image) = rest.split_at_checked(len).ok_or(GroupFileError::Corrupt {
        file: IMAGE_FILE,
        reason: "header past the payload",
    })?;
    if image.len() > bound {
        return Err(GroupFileError::Bound {
            file: IMAGE_FILE,
            bound,
        });
    }
    if header.len() > POINT_BOUND {
        return Err(GroupFileError::Corrupt {
            file: IMAGE_FILE,
            reason: "a point longer than any configuration's",
        });
    }
    let point: StoredPoint = postcard::from_bytes(header)?;
    Ok(Some((point.point(), image.to_vec())))
}

fn read_bounded<M: Medium>(
    medium: &M,
    path: &Path,
    file: &'static str,
    payload_bound: usize,
) -> Result<Vec<u8>, GroupFileError> {
    let limit = payload_bound
        .checked_add(HEADER_BYTES + TRAILER_BYTES)
        .ok_or(GroupFileError::Bound {
            file,
            bound: payload_bound,
        })?;
    medium.read(path, limit).map_err(GroupFileError::Io)
}

/// Magic, version, length, payload and the checksum over them.
fn frame(magic: &[u8; 8], payload: &[u8]) -> Result<Vec<u8>, GroupFileError> {
    let len = u64::try_from(payload.len()).map_err(|_| GroupFileError::Corrupt {
        file: "frame",
        reason: "payload length",
    })?;
    let total = payload
        .len()
        .checked_add(HEADER_BYTES + TRAILER_BYTES)
        .ok_or(GroupFileError::Corrupt {
            file: "frame",
            reason: "payload length",
        })?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(total)
        .map_err(|_| GroupFileError::Corrupt {
            file: "frame",
            reason: "allocation",
        })?;
    bytes.extend_from_slice(magic);
    bytes.extend_from_slice(&VERSION.to_le_bytes());
    bytes.extend_from_slice(&len.to_le_bytes());
    bytes.extend_from_slice(payload);
    let sum = crc32fast::hash(&bytes);
    bytes.extend_from_slice(&sum.to_le_bytes());
    Ok(bytes)
}

/// The payload of a file framed by [`frame`], verified whole.
fn unframe<'b>(
    file: &'static str,
    magic: &[u8; 8],
    bytes: &'b [u8],
) -> Result<&'b [u8], GroupFileError> {
    let corrupt = |reason| GroupFileError::Corrupt { file, reason };
    let body_len = bytes
        .len()
        .checked_sub(TRAILER_BYTES)
        .ok_or(corrupt("shorter than its frame"))?;
    let (body, sum) = bytes
        .split_at_checked(body_len)
        .ok_or(corrupt("shorter than its frame"))?;
    let sum = u32::from_le_bytes(sum.try_into().map_err(|_| corrupt("no checksum"))?);
    if crc32fast::hash(body) != sum {
        return Err(corrupt("checksum mismatch"));
    }
    let (head, payload) = body
        .split_at_checked(HEADER_BYTES)
        .ok_or(corrupt("shorter than its frame"))?;
    if head.get(..8) != Some(magic.as_slice()) {
        return Err(corrupt("another file's magic"));
    }
    let version = head
        .get(8..12)
        .and_then(|v| v.try_into().ok())
        .map(u32::from_le_bytes)
        .ok_or(corrupt("no version"))?;
    if version != VERSION {
        return Err(corrupt("a version this binary does not know"));
    }
    let len = head
        .get(12..20)
        .and_then(|v| v.try_into().ok())
        .map(u64::from_le_bytes)
        .ok_or(corrupt("no length"))?;
    if u64::try_from(payload.len()).ok() != Some(len) {
        return Err(corrupt("length mismatch"));
    }
    Ok(payload)
}

#[cfg(test)]
#[path = "group_files_tests.rs"]
mod tests;
