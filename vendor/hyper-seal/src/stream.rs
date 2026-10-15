//! A file written once, sealed by STREAM (`docs/seal.md` §4; Hoang, Reyhanitabar, Rogaway and Vizár,
//! CRYPTO 2015): segments of `S` plaintext bytes, each AES-256-GCM under the file's own data key at
//! the nonce `0 ‖ (index | LAST·[last])`, with the file's ID as additional data, and segment 0's
//! additional data every header field but the key record as well: the key record is what a rotation
//! rewrites, and the key it holds is pinned by the commitment, which segment 0 binds. A segment opens only at its own index and only as last
//! if it was sealed last, so a file cut short, extended, reordered or spliced fails to open.
//!
//! One key seals one file, and a file is never continued by a writer that did not finish it, so no
//! nonce repeats under a key.

use aws_lc_rs::aead::{AES_256_GCM, Aad, LessSafeKey, UnboundKey};

use crate::keys::{COMMITMENT, KeyId, RECORD, Wrapped, WrappingKey, check_commitment, commitment};
use crate::{SealError, Secret32, TAG, counter_nonce, fill, guarded, random_bytes, random_secret};

/// Bytes of a file's header: version 1, segment size 4, file ID 16, the data key's record 61, its
/// commitment 32.
pub const HEADER: usize = 114;

/// Bytes of the header segment 0 binds beside the file's ID: version 1, segment size 4, commitment
/// 32 (every field but the file ID, which leads the additional data, and the key record).
const BOUND: usize = 1 + 4 + COMMITMENT;

/// The smallest segment: below it the tag is more than 3% of the bytes (§4).
pub const MIN_SEGMENT: u32 = 512;

/// The largest segment the header's `u32` holds, far below GCM's 2^39 − 256 bits a seal (SP 800-38D
/// §5.2.1.1).
pub const MAX_SEGMENT: u32 = u32::MAX;

/// The top bit of a nonce's counter, set on a file's last segment.
const LAST: u64 = 1 << 63;

/// The header format this build writes.
const HEADER_VERSION: u8 = 1;

/// A file's header, as its first [`HEADER`] bytes keep it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    /// Plaintext bytes of every segment but the last.
    pub segment: u32,
    /// The file's ID: random, the additional data of every segment.
    pub file: [u8; 16],
    /// The data key, wrapped by its parent.
    pub key: Wrapped,
    /// The data key's commitment for this file (§2).
    pub commitment: [u8; COMMITMENT],
}

impl Header {
    /// The header's bytes.
    pub fn encode(&self) -> [u8; HEADER] {
        let mut out = [0u8; HEADER];
        fill(
            &mut out,
            &[
                &[HEADER_VERSION],
                &self.segment.to_le_bytes(),
                &self.file,
                &self.key.encode(),
                &self.commitment,
            ],
        );
        out
    }

    /// A header from its bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, SealError> {
        let bytes: &[u8; HEADER] = bytes.try_into().map_err(|_| SealError::Malformed)?;
        let (version, rest) = bytes.split_at(1);
        if version != [HEADER_VERSION] {
            return Err(SealError::Malformed);
        }
        let (segment, rest) = rest.split_at(4);
        let (file, rest) = rest.split_at(16);
        let (key, commit) = rest.split_at(RECORD);
        let segment = u32::from_le_bytes(segment.try_into().map_err(|_| SealError::Malformed)?);
        if segment < MIN_SEGMENT {
            return Err(SealError::Malformed);
        }
        Ok(Self {
            segment,
            file: file.try_into().map_err(|_| SealError::Malformed)?,
            key: Wrapped::decode(key)?,
            commitment: commit.try_into().map_err(|_| SealError::Malformed)?,
        })
    }
}

/// Where a writer or reader stands in a file's segments.
struct Segments {
    key: LessSafeKey,
    /// The file's ID, then the header's bound fields (version, segment size, commitment): segment
    /// 0's additional data, whose first 16 bytes are every other segment's.
    aad: [u8; 16 + BOUND],
    segment: u32,
}

impl Segments {
    fn new(data: &Secret32, header: Header) -> Result<Self, SealError> {
        let key = guarded(SealError::Seal, || {
            UnboundKey::new(&AES_256_GCM, data.bytes()).map(LessSafeKey::new)
        })?;
        let mut aad = [0u8; 16 + BOUND];
        fill(
            &mut aad,
            &[
                &header.file,
                &[HEADER_VERSION],
                &header.segment.to_le_bytes(),
                &header.commitment,
            ],
        );
        Ok(Self {
            key,
            aad,
            segment: header.segment,
        })
    }

    /// The nonce counter of segment `index`, marked if it is the last.
    fn counter(index: u64, last: bool) -> Result<u64, SealError> {
        if index >= LAST {
            return Err(SealError::Size);
        }
        Ok(if last { index | LAST } else { index })
    }

    /// The segment's additional data: the file's ID, and for segment 0 the header too.
    fn aad(&self, index: u64) -> &[u8] {
        if index == 0 {
            &self.aad
        } else {
            let (file, _) = self.aad.split_at(16);
            file
        }
    }

    /// Whether `len` plaintext bytes may be segment `index`: a full segment, or no more than one
    /// when last.
    fn fits(&self, len: usize, last: bool) -> Result<(), SealError> {
        let segment = usize::try_from(self.segment).map_err(|_| SealError::Size)?;
        if len > segment || (!last && len != segment) {
            return Err(SealError::Size);
        }
        Ok(())
    }
}

/// Seals a new file, segment by segment, in order.
pub struct FileSealer {
    segments: Segments,
    next: u64,
    done: bool,
}

impl FileSealer {
    /// A new file under a new data key, wrapped by `parent`, with segments of `segment` bytes; its
    /// header goes first in the file.
    pub fn new(parent: &WrappingKey, segment: u32) -> Result<(Self, Header), SealError> {
        if segment < MIN_SEGMENT {
            return Err(SealError::Size);
        }
        let data = random_secret()?;
        let file = random_bytes::<16>()?;
        let header = Header {
            segment,
            file,
            key: parent.wrap(&data)?,
            commitment: commitment(&data, &file)?,
        };
        let sealer = Self {
            segments: Segments::new(&data, header)?,
            next: 0,
            done: false,
        };
        Ok((sealer, header))
    }

    /// A file keyed by its content (docs/seal.md §4): its data key and its ID derived from `parent`
    /// and `content`, the 256-bit hash of its whole plaintext, so the same plaintext under the same
    /// parent always seals to the same bytes, and a holder that keeps only ciphertext sees an
    /// unchanged file as unchanged. Two different plaintexts get different keys unless their 256-bit
    /// hashes collide, so no key ever seals two plaintexts at one nonce. What a reader of the stored
    /// bytes learns is that two such files are equal, which their keyed names say already (§7). The
    /// caller's contract: `content` is the hash of exactly the plaintext it then seals.
    pub fn content_keyed(
        parent: &WrappingKey,
        content: &[u8; 32],
        segment: u32,
    ) -> Result<(Self, Header), SealError> {
        if segment < MIN_SEGMENT {
            return Err(SealError::Size);
        }
        let (data, key) = parent.derive_child(b"hyper-seal content key", content)?;
        let (id, _) = parent.derive_child(b"hyper-seal content file", content)?;
        let mut file = [0u8; 16];
        fill(&mut file, &[id.bytes()]);
        let header = Header {
            segment,
            file,
            key,
            commitment: commitment(&data, &file)?,
        };
        let sealer = Self {
            segments: Segments::new(&data, header)?,
            next: 0,
            done: false,
        };
        Ok((sealer, header))
    }

    /// Seals the next segment in place and returns its tag, which follows it in the file. Every
    /// segment but the last holds exactly the header's segment size; nothing follows the last.
    pub fn seal(&mut self, segment: &mut [u8], last: bool) -> Result<[u8; TAG], SealError> {
        if self.done {
            return Err(SealError::Size);
        }
        self.segments.fits(segment.len(), last)?;
        let index = self.next;
        let nonce = counter_nonce(Segments::counter(index, last)?);
        let aad = self.segments.aad(index);
        let key = &self.segments.key;
        let tag = guarded(SealError::Seal, || {
            key.seal_in_place_separate_tag(nonce, Aad::from(aad), segment)
        })?;
        self.next = index.checked_add(1).ok_or(SealError::Size)?;
        self.done = last;
        tag.as_ref().try_into().map_err(|_| SealError::Seal)
    }
}

/// Opens a sealed file's segments, in any order: a read opens only the segments its range covers.
pub struct FileOpener {
    segments: Segments,
}

impl FileOpener {
    /// The file whose header is `header`, its data key unwrapped by `parent` and checked against
    /// its commitment before anything opens.
    pub fn new(parent: &WrappingKey, header: &Header) -> Result<Self, SealError> {
        let data = parent.unwrap(&header.key)?;
        check_commitment(&data, &header.file, &header.commitment)?;
        Ok(Self {
            segments: Segments::new(&data, *header)?,
        })
    }

    /// Opens segment `index` in place, `last` saying whether the reader takes it for the file's
    /// last. A segment at another index, sealed as last when not taken so (or the reverse), from
    /// another file, under another key, or changed, is [`SealError::Open`].
    pub fn open(
        &self,
        index: u64,
        last: bool,
        segment: &mut [u8],
        tag: &[u8; TAG],
    ) -> Result<(), SealError> {
        self.segments.fits(segment.len(), last)?;
        let nonce = counter_nonce(Segments::counter(index, last)?);
        let aad = self.segments.aad(index);
        let key = &self.segments.key;
        guarded(SealError::Open, || {
            key.open_in_place_separate_tag(nonce, Aad::from(aad), tag, segment)
                .map(|_| ())
        })
    }

    /// The parent this file's data key is wrapped under, for a rewrap.
    pub fn parent(header: &Header) -> (KeyId, u32) {
        (header.key.parent, header.key.generation)
    }
}

/// One key over every version of an object (docs/seal.md §4), for a consumer whose versions never
/// repeat under it, through crash, restart and restore, which the consumer states and tests: each
/// segment of version `v` is sealed at nonce `v (64 bits) ‖ index (31 bits) | LAST·[last]`, so a 4
/// KiB overwrite reseals one segment and makes no key. The object's ID is every segment's
/// additional data.
pub struct VersionKey {
    key: LessSafeKey,
    object: [u8; 16],
}

/// The top bit of a versioned nonce's segment field, set on a version's last segment.
const VERSION_LAST: u32 = 1 << 31;

impl VersionKey {
    /// The key of object `object`, from its lineage key `secret`.
    pub fn new(secret: &Secret32, object: [u8; 16]) -> Result<Self, SealError> {
        let key = guarded(SealError::Seal, || {
            UnboundKey::new(&AES_256_GCM, secret.bytes()).map(LessSafeKey::new)
        })?;
        Ok(Self { key, object })
    }

    fn nonce(version: u64, index: u32, last: bool) -> Result<aws_lc_rs::aead::Nonce, SealError> {
        if index >= VERSION_LAST {
            return Err(SealError::Size);
        }
        let field = if last { index | VERSION_LAST } else { index };
        let mut nonce = [0u8; 12];
        fill(&mut nonce, &[&version.to_be_bytes(), &field.to_be_bytes()]);
        Ok(aws_lc_rs::aead::Nonce::assume_unique_for_key(nonce))
    }

    /// Seals segment `index` of version `version` in place and returns its tag.
    pub fn seal(
        &self,
        version: u64,
        index: u32,
        last: bool,
        segment: &mut [u8],
    ) -> Result<[u8; TAG], SealError> {
        let nonce = Self::nonce(version, index, last)?;
        let tag = guarded(SealError::Seal, || {
            self.key
                .seal_in_place_separate_tag(nonce, Aad::from(&self.object), segment)
        })?;
        tag.as_ref().try_into().map_err(|_| SealError::Seal)
    }

    /// Opens segment `index` of version `version` in place. Another version's, index's or
    /// object's segment, or a change, is [`SealError::Open`].
    pub fn open(
        &self,
        version: u64,
        index: u32,
        last: bool,
        segment: &mut [u8],
        tag: &[u8; TAG],
    ) -> Result<(), SealError> {
        let nonce = Self::nonce(version, index, last)?;
        guarded(SealError::Open, || {
            self.key
                .open_in_place_separate_tag(nonce, Aad::from(&self.object), tag, segment)
                .map(|_| ())
        })
    }
}

/// `header` with its data key rewrapped from `old` to `new`: the rotation that changes the header's
/// key record and nothing else (§3.1). The commitment is unchanged, since the key is.
pub fn rewrap(header: &Header, old: &WrappingKey, new: &WrappingKey) -> Result<Header, SealError> {
    let data = old.unwrap(&header.key)?;
    check_commitment(&data, &header.file, &header.commitment)?;
    Ok(Header {
        key: new.wrap(&data)?,
        ..*header
    })
}

/// Stored bytes of a file of `plain` plaintext bytes in segments of `segment`: the header, then each
/// segment and its tag. An empty file is one empty last segment.
pub fn sealed_len(plain: u64, segment: u32) -> Result<u64, SealError> {
    let segment = u64::from(segment);
    if segment == 0 {
        return Err(SealError::Size);
    }
    let count = plain.div_ceil(segment).max(1);
    let tags = count.checked_mul(16).ok_or(SealError::Size)?;
    plain
        .checked_add(tags)
        .and_then(|n| n.checked_add(HEADER as u64))
        .ok_or(SealError::Size)
}

#[cfg(test)]
mod tests {
    use super::*;

    const S: u32 = 512;

    /// A file of `len` bytes sealed: header, then (segment, tag) pairs.
    fn seal_file(
        parent: &WrappingKey,
        len: usize,
    ) -> (Header, Vec<Vec<u8>>, Vec<[u8; TAG]>, Vec<u8>) {
        let plain: Vec<u8> = (0..len).map(|i| (i * 31 % 251) as u8).collect();
        let (mut sealer, header) = FileSealer::new(parent, S).unwrap();
        let mut segs = Vec::new();
        let mut tags = Vec::new();
        let chunks: Vec<&[u8]> = if plain.is_empty() {
            vec![&[][..]]
        } else {
            plain.chunks(S as usize).collect()
        };
        let n = chunks.len();
        for (i, chunk) in chunks.into_iter().enumerate() {
            let mut buf = chunk.to_vec();
            tags.push(sealer.seal(&mut buf, i + 1 == n).unwrap());
            segs.push(buf);
        }
        (header, segs, tags, plain)
    }

    #[test]
    fn every_segment_opens_at_its_index_and_the_file_is_its_plaintext() {
        let parent = WrappingKey::generate(0).unwrap();
        for len in [0, 1, 511, 512, 513, 2048, 2049] {
            let (header, segs, tags, plain) = seal_file(&parent, len);
            let opener =
                FileOpener::new(&parent, &Header::decode(&header.encode()).unwrap()).unwrap();
            let mut out = Vec::new();
            for (i, (seg, tag)) in segs.iter().zip(&tags).enumerate() {
                let mut buf = seg.clone();
                opener
                    .open(i as u64, i + 1 == segs.len(), &mut buf, tag)
                    .unwrap();
                out.extend_from_slice(&buf);
            }
            assert_eq!(out, plain, "len {len}");
        }
    }

    #[test]
    fn a_file_cut_extended_reordered_or_spliced_does_not_open() {
        let parent = WrappingKey::generate(0).unwrap();
        let (header, segs, tags, _) = seal_file(&parent, 3 * 512);
        let opener = FileOpener::new(&parent, &header).unwrap();
        // Cut short: segment 1 taken as last.
        let mut buf = segs[1].clone();
        assert_eq!(
            opener.open(1, true, &mut buf, &tags[1]),
            Err(SealError::Open)
        );
        // Extended: the last taken as not last.
        let mut buf = segs[2].clone();
        assert_eq!(
            opener.open(2, false, &mut buf, &tags[2]),
            Err(SealError::Open)
        );
        // Reordered: segment 1 at index 0, segment 0 at index 1.
        let mut buf = segs[1].clone();
        assert_eq!(
            opener.open(0, false, &mut buf, &tags[1]),
            Err(SealError::Open)
        );
        let mut buf = segs[0].clone();
        assert_eq!(
            opener.open(1, false, &mut buf, &tags[0]),
            Err(SealError::Open)
        );
        // Spliced: another file's segment 1 under this file's.
        let (other_header, other_segs, other_tags, _) = seal_file(&parent, 3 * 512);
        let mut buf = other_segs[1].clone();
        assert_eq!(
            opener.open(1, false, &mut buf, &other_tags[1]),
            Err(SealError::Open)
        );
        assert_ne!(other_header.file, header.file);
    }

    #[test]
    fn a_flipped_bit_anywhere_fails_typed() {
        let parent = WrappingKey::generate(0).unwrap();
        let (header, segs, tags, _) = seal_file(&parent, 700);
        let opener = FileOpener::new(&parent, &header).unwrap();
        for (i, seg) in segs.iter().enumerate() {
            let last = i + 1 == segs.len();
            for at in 0..seg.len() {
                let mut buf = seg.clone();
                buf[at] ^= 0x10;
                assert_eq!(
                    opener.open(i as u64, last, &mut buf, &tags[i]),
                    Err(SealError::Open)
                );
            }
            for at in 0..TAG {
                let mut tag = tags[i];
                tag[at] ^= 1;
                let mut buf = seg.clone();
                assert_eq!(
                    opener.open(i as u64, last, &mut buf, &tag),
                    Err(SealError::Open)
                );
            }
        }
    }

    #[test]
    fn a_changed_header_opens_nothing() {
        let parent = WrappingKey::generate(0).unwrap();
        let (header, segs, tags, _) = seal_file(&parent, 600);
        // Another file's key record under this file's ID: the commitment refuses it.
        let (other, _, _, _) = seal_file(&parent, 600);
        let repointed = Header {
            key: other.key,
            ..header
        };
        assert_eq!(
            FileOpener::new(&parent, &repointed).err(),
            Some(SealError::Open)
        );
        // A header whose segment size changed: segment 0's additional data refuses it.
        let resized = Header {
            segment: 600,
            ..header
        };
        let opener = FileOpener::new(&parent, &resized).unwrap();
        let mut buf = segs[0].clone();
        assert_eq!(
            opener.open(0, false, &mut buf, &tags[0]).unwrap_err(),
            SealError::Size
        );
        let mut buf = segs[0].clone();
        assert_eq!(
            opener.open(0, true, &mut buf, &tags[0]).unwrap_err(),
            SealError::Open
        );
    }

    #[test]
    fn sizes_outside_the_construction_are_refused() {
        let parent = WrappingKey::generate(0).unwrap();
        assert_eq!(FileSealer::new(&parent, 511).err(), Some(SealError::Size));
        let (mut sealer, _) = FileSealer::new(&parent, S).unwrap();
        assert_eq!(sealer.seal(&mut [0; 100], false), Err(SealError::Size));
        assert_eq!(sealer.seal(&mut [0; 513], true), Err(SealError::Size));
        sealer.seal(&mut [0; 100], true).unwrap();
        assert_eq!(sealer.seal(&mut [0; 1], true), Err(SealError::Size));
    }

    #[test]
    fn a_rewrap_changes_only_the_key_record() {
        let mut old = WrappingKey::generate(0).unwrap();
        let new = WrappingKey::generate(1).unwrap();
        let (header, segs, tags, plain) = seal_file(&old, 900);
        old.retire();
        let rewrapped = rewrap(&header, &old, &new).unwrap();
        assert_eq!(rewrapped.file, header.file);
        assert_eq!(rewrapped.commitment, header.commitment);
        let opener = FileOpener::new(&new, &rewrapped).unwrap();
        let mut out = Vec::new();
        for (i, (seg, tag)) in segs.iter().zip(&tags).enumerate() {
            let mut buf = seg.clone();
            opener
                .open(i as u64, i + 1 == segs.len(), &mut buf, tag)
                .unwrap();
            out.extend_from_slice(&buf);
        }
        assert_eq!(out, plain);
        assert_eq!(
            FileOpener::new(&old, &rewrapped).err(),
            Some(SealError::Unwrap)
        );
    }

    #[test]
    fn a_versioned_segment_opens_only_as_its_version_index_and_object() {
        let secret = random_secret().unwrap();
        let key = VersionKey::new(&secret, [3; 16]).unwrap();
        let mut seg = vec![7u8; 4096];
        let tag = key.seal(9, 2, false, &mut seg).unwrap();
        let mut got = seg.clone();
        key.open(9, 2, false, &mut got, &tag).unwrap();
        assert_eq!(got, vec![7u8; 4096]);
        for (v, i, last) in [(8, 2, false), (9, 1, false), (9, 2, true)] {
            let mut got = seg.clone();
            assert_eq!(key.open(v, i, last, &mut got, &tag), Err(SealError::Open));
        }
        let other = VersionKey::new(&secret, [4; 16]).unwrap();
        let mut got = seg.clone();
        assert_eq!(
            other.open(9, 2, false, &mut got, &tag),
            Err(SealError::Open)
        );
        assert_eq!(
            key.seal(1, 1 << 31, false, &mut [0; 1]),
            Err(SealError::Size)
        );
    }

    /// The same plaintext under the same parent seals to the same bytes; other plaintext, or another
    /// parent, to other keys and IDs; and the file opens like any other.
    #[test]
    fn a_content_keyed_file_seals_the_same_content_to_the_same_bytes() {
        let parent = WrappingKey::generate(0).unwrap();
        let seal = |parent: &WrappingKey, hash: [u8; 32], plain: &[u8]| {
            let (mut sealer, header) = FileSealer::content_keyed(parent, &hash, 512).unwrap();
            let mut buf = plain.to_vec();
            let tag = sealer.seal(&mut buf, true).unwrap();
            (header, buf, tag)
        };
        let a = seal(&parent, [1; 32], b"chunk one");
        let again = seal(&parent, [1; 32], b"chunk one");
        assert_eq!(
            (a.0.encode(), &a.1, a.2),
            (again.0.encode(), &again.1, again.2)
        );
        let b = seal(&parent, [2; 32], b"chunk two");
        assert_ne!(a.0.file, b.0.file);
        assert_ne!(a.0.key, b.0.key);
        let other = WrappingKey::generate(0).unwrap();
        let c = seal(&other, [1; 32], b"chunk one");
        assert_ne!(a.0.key.key, c.0.key.key);
        let opener = FileOpener::new(&parent, &a.0).unwrap();
        let mut got = a.1.clone();
        opener.open(0, true, &mut got, &a.2).unwrap();
        assert_eq!(got, b"chunk one");
    }

    #[test]
    fn the_stored_length_follows_from_the_plaintext() {
        assert_eq!(sealed_len(0, 512).unwrap(), 114 + 16);
        assert_eq!(sealed_len(512, 512).unwrap(), 114 + 512 + 16);
        assert_eq!(sealed_len(513, 512).unwrap(), 114 + 513 + 32);
    }

    /// McGrew and Viega's GCM test case 14 (AES-256, a zero key, a zero 96-bit IV, one zero block),
    /// through the same key type the seals use, so the cipher is the standard's.
    #[test]
    fn the_cipher_is_gcm_test_case_14() {
        let key = LessSafeKey::new(UnboundKey::new(&AES_256_GCM, &[0u8; 32]).unwrap());
        let mut block = [0u8; 16];
        let nonce = aws_lc_rs::aead::Nonce::assume_unique_for_key([0u8; 12]);
        let tag = key
            .seal_in_place_separate_tag(nonce, Aad::empty(), &mut block)
            .unwrap();
        assert_eq!(
            block,
            [
                0xce, 0xa7, 0x40, 0x3d, 0x4d, 0x60, 0x6b, 0x6e, 0x07, 0x4e, 0xc5, 0xd3, 0xba, 0xf3,
                0x9d, 0x18
            ]
        );
        assert_eq!(
            tag.as_ref(),
            &[
                0xd0, 0xd1, 0xc8, 0xa7, 0x99, 0x99, 0x6b, 0xf0, 0x26, 0x5b, 0x98, 0xb5, 0xd4, 0x8a,
                0xb9, 0x19
            ]
        );
    }
}
