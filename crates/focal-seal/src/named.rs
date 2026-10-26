//! Whole files sealed with their names bound inside the seal (29 §12.10, §12.11): a file's bytes,
//! as its writer made them, framed with the file's name and sealed whole through [`crate::file`],
//! behind a magic in the clear that says what the file is and nothing of what it holds. A file moved
//! to another's name opens to a frame naming another file, and one carried in from another data
//! directory opens under no key of this one's, so both are refused, as a changed byte is.
//!
//! The node's small files (focal-node `node_files`) and the client's stores (focal-client) seal
//! through here, each a [`Kind`] of its own, so a file of one is never read as the other's.
use hyper_seal::keys::WrappingKey;
use zeroize::Zeroizing;

/// Why a named sealed file was not made or opened.
#[derive(Debug, thiserror::Error)]
pub enum NamedError {
    /// The seal refused it: changed, cut, or sealed under another directory's key.
    #[error("{0}")]
    Seal(#[from] crate::file::FileError),
    /// Its frame names another file: moved to this one's name.
    #[error("the sealed file {0} holds another file's frame")]
    Moved(String),
    /// Its frame does not read whole: what the seal let through is not one of this kind's frames.
    #[error("a sealed file's frame does not read whole")]
    Malformed,
    /// It holds more bytes than its reader takes.
    #[error("a sealed file's bytes past their bound of {0}")]
    Bound(usize),
    /// A frame past what this process can hold or count.
    #[error("no room for a sealed file's frame")]
    Capacity,
}

/// A kind of named sealed file: what it begins with in the clear, and what its frame begins with,
/// then its name's length (two bytes, little-endian), its name, and the file's bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Kind {
    /// Format: what a sealed file of this kind begins with, in the clear.
    pub outer: &'static [u8; 8],
    /// Format: what a sealed file of this kind's plaintext begins with.
    pub frame: &'static [u8; 8],
}

/// Derived: the bytes of a frame before its name: its magic and the name's length.
const FRAME_HEAD: usize = 8 + 2;

impl Kind {
    /// Whether `bytes` are a sealed file of this kind's: they begin with its clear magic.
    pub fn sealed(&self, bytes: &[u8]) -> bool {
        bytes.starts_with(self.outer)
    }

    /// The most bytes a file of this kind holding `most` bytes takes on disk, in either form: its
    /// bytes in the clear, or the clear magic and the seal of its frame, whatever name the frame
    /// carries. What a reader reads at most before it tells the form.
    pub fn bound(&self, most: usize) -> Result<usize, NamedError> {
        crate::file::sealed_bound(frame_bound(most)?)
            .and_then(|sealed| sealed.checked_add(self.outer.len()))
            .map(|sealed| sealed.max(most))
            .ok_or(NamedError::Capacity)
    }

    /// `bytes`, the file `name`, sealed whole under `key`: the clear magic, then the seal of its
    /// frame.
    pub fn seal(&self, key: &WrappingKey, name: &str, bytes: &[u8]) -> Result<Vec<u8>, NamedError> {
        let name_len = u16::try_from(name.len()).map_err(|_| NamedError::Capacity)?;
        let mut frame = Zeroizing::new(Vec::new());
        frame
            .try_reserve_exact(frame_len(name, bytes.len())?)
            .map_err(|_| NamedError::Capacity)?;
        frame.extend_from_slice(self.frame);
        frame.extend_from_slice(&name_len.to_le_bytes());
        frame.extend_from_slice(name.as_bytes());
        frame.extend_from_slice(bytes);
        let sealed = crate::file::seal(key, &frame)?;
        let mut out = Vec::new();
        out.try_reserve_exact(self.outer.len().saturating_add(sealed.len()))
            .map_err(|_| NamedError::Capacity)?;
        out.extend_from_slice(self.outer);
        out.extend_from_slice(&sealed);
        Ok(out)
    }

    /// The bytes the sealed file `bytes`, read as `name`, holds: opened under `key`, its frame's name
    /// checked, no more than `most` of them; wiped wherever they are let go.
    pub fn open(
        &self,
        key: &WrappingKey,
        name: &str,
        bytes: Vec<u8>,
        most: usize,
    ) -> Result<Zeroizing<Vec<u8>>, NamedError> {
        let mut sealed = bytes;
        if !sealed.starts_with(self.outer) {
            return Err(NamedError::Malformed);
        }
        sealed.drain(..self.outer.len());
        // Bounded by the longest name a frame carries, not this file's: a frame moved here from a
        // longer name opens to be refused as moved.
        let frame = crate::file::open(key, sealed, frame_bound(most)?)?;
        let (magic, rest) = frame
            .split_at_checked(self.frame.len())
            .ok_or(NamedError::Malformed)?;
        let (stated, rest) = rest.split_at_checked(2).ok_or(NamedError::Malformed)?;
        let stated = usize::from(u16::from_le_bytes(
            stated.try_into().map_err(|_| NamedError::Malformed)?,
        ));
        let (named, body) = rest.split_at_checked(stated).ok_or(NamedError::Malformed)?;
        if magic != self.frame || named != name.as_bytes() {
            return Err(NamedError::Moved(name.to_owned()));
        }
        if body.len() > most {
            return Err(NamedError::Bound(most));
        }
        let mut out = Zeroizing::new(Vec::new());
        out.try_reserve_exact(body.len())
            .map_err(|_| NamedError::Capacity)?;
        out.extend_from_slice(body);
        Ok(out)
    }
}

/// The most bytes any frame holding `most` bytes takes, its name as long as a frame's can be.
fn frame_bound(most: usize) -> Result<usize, NamedError> {
    FRAME_HEAD
        .checked_add(usize::from(u16::MAX))
        .and_then(|head| head.checked_add(most))
        .ok_or(NamedError::Capacity)
}

/// The bytes a frame of `name` holding `most` bytes takes.
fn frame_len(name: &str, most: usize) -> Result<usize, NamedError> {
    FRAME_HEAD
        .checked_add(name.len())
        .and_then(|head| head.checked_add(most))
        .ok_or(NamedError::Capacity)
}

#[cfg(test)]
mod tests {
    use super::*;

    const KIND: Kind = Kind {
        outer: b"FCLTSEAL",
        frame: b"FCLTFRM1",
    };
    const OTHER: Kind = Kind {
        outer: b"FCLOSEAL",
        frame: b"FCLOFRM1",
    };

    fn key() -> WrappingKey {
        hyper_seal::lock_keys(crate::key_slots()).unwrap();
        WrappingKey::generate(0).unwrap()
    }

    /// A file sealed reads back whole under its name and key, its bytes nowhere in the clear; under
    /// another name, another key or another kind, or with a changed byte, it is refused.
    #[test]
    fn a_named_file_opens_only_as_itself() {
        let key = key();
        let bytes = b"the file's own bytes, as its writer made them".to_vec();
        let sealed = KIND.seal(&key, "dir/record.bin", &bytes).unwrap();
        assert!(KIND.sealed(&sealed));
        assert!(!sealed.windows(8).any(|window| window == &bytes[..8]));
        assert!(sealed.len() <= KIND.bound(bytes.len()).unwrap());
        assert_eq!(
            KIND.open(&key, "dir/record.bin", sealed.clone(), bytes.len())
                .unwrap()
                .as_slice(),
            bytes.as_slice()
        );
        assert!(matches!(
            KIND.open(&key, "dir/other.bin", sealed.clone(), bytes.len()),
            Err(NamedError::Moved(name)) if name == "dir/other.bin"
        ));
        assert!(matches!(
            KIND.open(&self::key(), "dir/record.bin", sealed.clone(), bytes.len()),
            Err(NamedError::Seal(_))
        ));
        assert!(matches!(
            OTHER.open(&key, "dir/record.bin", sealed.clone(), bytes.len()),
            Err(NamedError::Malformed)
        ));
        let mut changed = sealed.clone();
        let last = changed.len() - 1;
        changed[last] ^= 1;
        assert!(matches!(
            KIND.open(&key, "dir/record.bin", changed, bytes.len()),
            Err(NamedError::Seal(_))
        ));
        assert!(matches!(
            KIND.open(&key, "dir/record.bin", sealed, bytes.len() - 1),
            Err(NamedError::Bound(most)) if most == bytes.len() - 1
        ));
    }

    /// A frame of another kind under the same key and name, its clear magic forged, is refused.
    #[test]
    fn a_frame_of_another_kind_is_refused_under_a_forged_magic() {
        let key = key();
        let mut sealed = OTHER.seal(&key, "record.bin", b"bytes").unwrap();
        sealed[..8].copy_from_slice(KIND.outer);
        assert!(matches!(
            KIND.open(&key, "record.bin", sealed, 64),
            Err(NamedError::Moved(_))
        ));
    }
}
