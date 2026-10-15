//! Sealing an appended log (`docs/seal.md` §5): each record's payload sealed alone under its writer
//! session's key at its offset in its segment, and every frame header, persist record and segment
//! header under a MAC.
//!
//! A log continues its head segment after a crash over the bytes of a torn tail, so a frame and the
//! frame written over it may hold different records at one offset. Each writer session therefore
//! has its own key: drawn when the writer starts a segment or continues one after recovery, carried
//! in the segment's header or in a key frame. Within a session the writer only appends, which
//! [`Session::seal`] enforces: every payload goes past every payload before it. So no key ever seals
//! two payloads at one offset.

use aws_lc_rs::aead::{AES_256_GCM, Aad, LessSafeKey, UnboundKey};
use aws_lc_rs::hmac;

use crate::keys::{COMMITMENT, RECORD, Wrapped, WrappingKey, check_commitment, commitment};
use crate::{SealError, Secret32, TAG, counter_nonce, fill, guarded, random_bytes, random_secret};

/// Bytes of a key frame's body: version 1, session ID 16, the session key's record 61, its
/// commitment 32.
pub const KEY_FRAME: usize = 110;

/// Bytes of a framing MAC: a full HMAC-SHA-256 (§5.1).
pub const MAC: usize = 32;

/// Bytes of a record's additional data: log 16, incarnation 8, group 16, index 8, term 8.
const RECORD_AAD: usize = 56;

/// The key frame format this build writes.
const KEY_FRAME_VERSION: u8 = 1;

/// The label framing MACs are computed under, so a framing MAC is never another use's.
const FRAME_LABEL: &[u8] = b"hyper-seal frame";

/// What says which record a payload is: everything bound into its tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordId {
    /// The log's ID.
    pub log: u128,
    /// The segment's incarnation.
    pub incarnation: u64,
    /// The record's group.
    pub group: u128,
    /// The record's index in its group.
    pub index: u64,
    /// The record's term.
    pub term: u64,
}

impl RecordId {
    fn aad(&self) -> [u8; RECORD_AAD] {
        let mut out = [0u8; RECORD_AAD];
        fill(
            &mut out,
            &[
                &self.log.to_le_bytes(),
                &self.incarnation.to_le_bytes(),
                &self.group.to_le_bytes(),
                &self.index.to_le_bytes(),
                &self.term.to_le_bytes(),
            ],
        );
        out
    }
}

/// A session's key as the log keeps it: in a segment's header for the session that opened it, or
/// in a key frame for one that continued it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyFrame {
    /// The session's ID: random, what its commitment names.
    pub session: [u8; 16],
    /// The session key, wrapped by the log's parent key.
    pub key: Wrapped,
    /// The session key's commitment (§2).
    pub commitment: [u8; COMMITMENT],
}

impl KeyFrame {
    /// The body's bytes.
    pub fn encode(&self) -> [u8; KEY_FRAME] {
        let mut out = [0u8; KEY_FRAME];
        fill(
            &mut out,
            &[
                &[KEY_FRAME_VERSION],
                &self.session,
                &self.key.encode(),
                &self.commitment,
            ],
        );
        out
    }

    /// A body from its bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, SealError> {
        let bytes: &[u8; KEY_FRAME] = bytes.try_into().map_err(|_| SealError::Malformed)?;
        let (version, rest) = bytes.split_at(1);
        if version != [KEY_FRAME_VERSION] {
            return Err(SealError::Malformed);
        }
        let (session, rest) = rest.split_at(16);
        let (key, commit) = rest.split_at(RECORD);
        Ok(Self {
            session: session.try_into().map_err(|_| SealError::Malformed)?,
            key: Wrapped::decode(key)?,
            commitment: commit.try_into().map_err(|_| SealError::Malformed)?,
        })
    }
}

fn aead_key(secret: &Secret32) -> Result<LessSafeKey, SealError> {
    guarded(SealError::Seal, || {
        UnboundKey::new(&AES_256_GCM, secret.bytes()).map(LessSafeKey::new)
    })
}

/// A writer session on one segment: seals each record's payload once, at offsets that only grow.
pub struct Session {
    key: LessSafeKey,
    /// The first offset a payload may take: past every payload and tag sealed so far.
    free: u64,
}

impl Session {
    /// A new session under a new key wrapped by `parent`, starting at `offset` in its segment (the
    /// segment's first frame, or where recovery continues). Its key frame is written before, and in
    /// the same write as, the first frame it keys.
    pub fn begin(parent: &WrappingKey, offset: u64) -> Result<(Self, KeyFrame), SealError> {
        let secret = random_secret()?;
        let session = random_bytes::<16>()?;
        let frame = KeyFrame {
            session,
            key: parent.wrap(&secret)?,
            commitment: commitment(&secret, &session)?,
        };
        Ok((
            Self {
                key: aead_key(&secret)?,
                free: offset,
            },
            frame,
        ))
    }

    /// Seals `payload` in place as the record `id` at `offset` in the segment, and returns its tag.
    /// An offset before the end of the last payload and tag sealed is refused: within a session the
    /// writer only appends, so no offset is ever sealed twice under its key.
    pub fn seal(
        &mut self,
        offset: u64,
        id: &RecordId,
        payload: &mut [u8],
    ) -> Result<[u8; TAG], SealError> {
        if offset < self.free {
            return Err(SealError::Size);
        }
        let len = u64::try_from(payload.len()).map_err(|_| SealError::Size)?;
        let end = offset
            .checked_add(len)
            .and_then(|end| end.checked_add(TAG as u64))
            .ok_or(SealError::Size)?;
        let aad = id.aad();
        let key = &self.key;
        let tag = guarded(SealError::Seal, || {
            key.seal_in_place_separate_tag(counter_nonce(offset), Aad::from(&aad), payload)
        })?;
        self.free = end;
        tag.as_ref().try_into().map_err(|_| SealError::Seal)
    }
}

/// Opens the records of one session, in any order.
pub struct SessionOpener {
    key: LessSafeKey,
}

impl SessionOpener {
    /// The session `frame` names, its key unwrapped by `parent` and checked against its commitment.
    pub fn new(parent: &WrappingKey, frame: &KeyFrame) -> Result<Self, SealError> {
        let secret = parent.unwrap(&frame.key)?;
        check_commitment(&secret, &frame.session, &frame.commitment)?;
        Ok(Self {
            key: aead_key(&secret)?,
        })
    }

    /// Opens `payload` in place as the record `id` at `offset`. Another session's record, another
    /// record's bytes, another offset, or a change is [`SealError::Open`].
    pub fn open(
        &self,
        offset: u64,
        id: &RecordId,
        payload: &mut [u8],
        tag: &[u8; TAG],
    ) -> Result<(), SealError> {
        let aad = id.aad();
        let key = &self.key;
        guarded(SealError::Open, || {
            key.open_in_place_separate_tag(counter_nonce(offset), Aad::from(&aad), tag, payload)
                .map(|_| ())
        })
    }
}

/// The MAC every frame header, persist record and segment header of one log carries (§5.1): the
/// log's authentication key, its ID, and the bytes, CRC included. A log's owner and its device each
/// hold one, the device to check the frames it reads.
#[derive(Clone)]
pub struct FrameMac {
    key: hmac::Key,
    log: [u8; 16],
}

impl FrameMac {
    /// The MAC of log `log` under its authentication key.
    pub fn new(auth: &Secret32, log: u128) -> Result<Self, SealError> {
        let key = guarded(SealError::Seal, || {
            Ok::<_, ()>(hmac::Key::new(hmac::HMAC_SHA256, auth.bytes()))
        })?;
        Ok(Self {
            key,
            log: log.to_le_bytes(),
        })
    }

    /// The MAC of `bytes`.
    pub fn mac(&self, bytes: &[u8]) -> Result<[u8; MAC], SealError> {
        self.mac_spans([bytes])
    }

    /// The MAC of `spans`, one after another: what a frame's MAC covers when the sealed records in
    /// it, each authenticated by its own tag, are left out.
    pub fn mac_spans<'a>(
        &self,
        spans: impl IntoIterator<Item = &'a [u8]>,
    ) -> Result<[u8; MAC], SealError> {
        let tag = guarded(SealError::Seal, || {
            let mut ctx = hmac::Context::with_key(&self.key);
            ctx.update(FRAME_LABEL);
            ctx.update(&self.log);
            for span in spans {
                ctx.update(span);
            }
            Ok::<_, ()>(ctx.sign())
        })?;
        tag.as_ref().try_into().map_err(|_| SealError::Seal)
    }

    /// Whether `mac` is `bytes`' MAC, in constant time. A mismatch is [`SealError::Tampered`]: bytes
    /// whose CRC held were changed by someone who recomputed it, never a torn write.
    pub fn verify(&self, bytes: &[u8], mac: &[u8; MAC]) -> Result<(), SealError> {
        let actual = self.mac(bytes)?;
        aws_lc_rs::constant_time::verify_slices_are_equal(&actual, mac)
            .map_err(|_| SealError::Tampered)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn id(index: u64) -> RecordId {
        RecordId {
            log: 7,
            incarnation: 3,
            group: 11,
            index,
            term: 2,
        }
    }

    #[test]
    fn a_record_opens_alone_and_only_as_itself() {
        let parent = WrappingKey::generate(0).unwrap();
        let (mut session, frame) = Session::begin(&parent, 4096).unwrap();
        let mut a = b"first entry".to_vec();
        let mut b = vec![9u8; 300];
        let tag_a = session.seal(4096, &id(1), &mut a).unwrap();
        let tag_b = session.seal(4096 + 11 + 16, &id(2), &mut b).unwrap();
        let opener =
            SessionOpener::new(&parent, &KeyFrame::decode(&frame.encode()).unwrap()).unwrap();
        let mut got = b.clone();
        opener.open(4096 + 27, &id(2), &mut got, &tag_b).unwrap();
        assert_eq!(got, vec![9u8; 300]);
        let mut got = a.clone();
        opener.open(4096, &id(1), &mut got, &tag_a).unwrap();
        assert_eq!(got, b"first entry");
        // At another offset, as another index, term, group, incarnation or log: refused.
        for (offset, other) in [
            (4097, id(1)),
            (4096, id(2)),
            (4096, RecordId { term: 3, ..id(1) }),
            (4096, RecordId { group: 12, ..id(1) }),
            (
                4096,
                RecordId {
                    incarnation: 4,
                    ..id(1)
                },
            ),
            (4096, RecordId { log: 8, ..id(1) }),
        ] {
            let mut got = a.clone();
            assert_eq!(
                opener.open(offset, &other, &mut got, &tag_a),
                Err(SealError::Open)
            );
        }
    }

    #[test]
    fn a_session_never_seals_an_offset_twice() {
        let parent = WrappingKey::generate(0).unwrap();
        let (mut session, _) = Session::begin(&parent, 100).unwrap();
        assert_eq!(session.seal(99, &id(1), &mut [0; 4]), Err(SealError::Size));
        session.seal(100, &id(1), &mut [0; 4]).unwrap();
        // The payload and its tag occupy 100..120: every offset inside is refused, 120 is not.
        for offset in 100..120 {
            assert_eq!(
                session.seal(offset, &id(2), &mut [0; 4]),
                Err(SealError::Size)
            );
        }
        session.seal(120, &id(2), &mut [0; 4]).unwrap();
    }

    /// A crash at every write of a session, then a continuation from the last durable offset: every
    /// (session key, offset) pair is sealed once, the acknowledged records open under their own
    /// session, and a torn record never opens under the session that wrote over it.
    #[test]
    fn a_crash_and_continuation_never_reuse_a_key_at_an_offset() {
        let parent = WrappingKey::generate(0).unwrap();
        for crash_after in 0..6u64 {
            let mut sealed: HashSet<([u8; 16], u64)> = HashSet::new();
            let (mut first, first_frame) = Session::begin(&parent, 0).unwrap();
            let mut durable = 0u64;
            let mut records = Vec::new();
            for i in 0..6u64 {
                let offset = i * 40;
                let mut payload = vec![i as u8; 24];
                let tag = first.seal(offset, &id(i), &mut payload).unwrap();
                assert!(sealed.insert((first_frame.session, offset)));
                records.push((offset, i, payload, tag));
                if i < crash_after {
                    durable = offset + 40;
                }
            }
            // Recovery keeps the durable records and continues over the torn ones.
            let (mut second, second_frame) = Session::begin(&parent, durable).unwrap();
            let torn: Vec<_> = records
                .iter()
                .filter(|(offset, ..)| *offset >= durable)
                .cloned()
                .collect();
            for (offset, i, ..) in &torn {
                let mut payload = vec![0xEE; 24];
                second.seal(*offset, &id(*i), &mut payload).unwrap();
                assert!(sealed.insert((second_frame.session, *offset)));
            }
            let first_opener = SessionOpener::new(&parent, &first_frame).unwrap();
            let second_opener = SessionOpener::new(&parent, &second_frame).unwrap();
            for (offset, i, payload, tag) in &records {
                let mut got = payload.clone();
                first_opener.open(*offset, &id(*i), &mut got, tag).unwrap();
                assert_eq!(got, vec![*i as u8; 24]);
                if *offset >= durable {
                    let mut got = payload.clone();
                    assert_eq!(
                        second_opener.open(*offset, &id(*i), &mut got, tag),
                        Err(SealError::Open)
                    );
                }
            }
            assert_ne!(first_frame.session, second_frame.session);
        }
    }

    #[test]
    fn a_session_key_that_misses_its_commitment_opens_nothing() {
        let parent = WrappingKey::generate(0).unwrap();
        let (_, a) = Session::begin(&parent, 0).unwrap();
        let (_, b) = Session::begin(&parent, 0).unwrap();
        let swapped = KeyFrame { key: b.key, ..a };
        assert_eq!(
            SessionOpener::new(&parent, &swapped).err(),
            Some(SealError::Open)
        );
    }

    #[test]
    fn a_framing_change_with_its_crc_recomputed_is_tampering() {
        let auth = random_secret().unwrap();
        let mac = FrameMac::new(&auth, 7).unwrap();
        // A hard state: term 5, vote 2, commit 40, its CRC after.
        let mut record = Vec::new();
        record.extend_from_slice(&5u64.to_le_bytes());
        record.extend_from_slice(&2u64.to_le_bytes());
        record.extend_from_slice(&40u64.to_le_bytes());
        let crc = crc_of(&record);
        record.extend_from_slice(&crc.to_le_bytes());
        let good = mac.mac(&record).unwrap();
        mac.verify(&record, &good).unwrap();
        for field in 0..3 {
            let mut changed = record[..24].to_vec();
            changed[field * 8] ^= 1;
            let crc = crc_of(&changed);
            changed.extend_from_slice(&crc.to_le_bytes());
            assert_eq!(
                mac.verify(&changed, &good),
                Err(SealError::Tampered),
                "field {field}"
            );
        }
        // Another log's MAC key or ID: tampering too.
        let other_log = FrameMac::new(&auth, 8).unwrap();
        assert_eq!(other_log.verify(&record, &good), Err(SealError::Tampered));
    }

    /// A CRC-32C for the test's records, standing in for the log's: the point is that an adversary
    /// can recompute it.
    fn crc_of(bytes: &[u8]) -> u32 {
        let mut crc = !0u32;
        for &b in bytes {
            crc ^= u32::from(b);
            for _ in 0..8 {
                crc = if crc & 1 == 1 {
                    (crc >> 1) ^ 0x82F6_3B78
                } else {
                    crc >> 1
                };
            }
        }
        !crc
    }
}
