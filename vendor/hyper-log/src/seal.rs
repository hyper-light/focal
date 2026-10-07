//! A sealed log's keys, held by the log's owner (hyper-raft docs/seal.md §5).
//!
//! - **Sessions.** The writer seals under a key of its own each time it starts writing a segment:
//!   when it opens one (the key frame in the segment's header), and the first time it writes the
//!   head segment after the log was opened (a key record first in that frame). Within a session the
//!   writer only appends, and every record's nonce is its file offset, so no key ever seals two
//!   records at one offset, crash or not.
//! - **Openers.** Every session of every live segment can be opened: its key frame, read from a
//!   header or a key record, unwrapped under the parent key and checked against its commitment. A
//!   record opens under the last session that began at or before its offset in its segment. A
//!   segment's sessions are bounded by its frames, since each session writes at least the frame its
//!   key comes in: `segment_bytes / block` of them at most, a bound the configuration gives.
//! - **Framing.** Every segment header, frame and persist record ends with a MAC under the log's
//!   authentication key, checked after its CRC and before anything it says is trusted.

use std::collections::BTreeMap;

use hyper_seal::keys::WrappingKey;
use hyper_seal::log::{FrameMac, KeyFrame, RecordId, Session, SessionOpener};
use hyper_seal::{SealError, Secret32};

use crate::LogError;
use crate::format::{self, MAC_LEN, TAG_LEN};

/// What a sealed log is given at its creation and at every open: the key its sessions' keys are
/// wrapped under (a tenant's, or the node's root), and its authentication key. The log's owner
/// holds both while the log is open.
pub struct Sealing {
    /// The key every session's key is wrapped under.
    pub parent: WrappingKey,
    /// The key every segment header's, frame's and persist record's MAC is made under.
    pub auth: Secret32,
}

/// The owner's sealing state.
pub(crate) struct Sealer {
    parent: WrappingKey,
    mac: FrameMac,
    log: u128,
    /// The session writing, and the incarnation of the segment it writes.
    writing: Option<(u64, Session)>,
    /// Each live segment's sessions, by incarnation: where each began, oldest first, and its
    /// opener.
    sessions: BTreeMap<u64, Vec<(u64, SessionOpener)>>,
}

/// Writes `src` over `dst`, which must be as long: a refusal, never a panic, on a length that
/// differs.
pub(crate) fn put_at(dst: Option<&mut [u8]>, src: &[u8]) -> Result<(), LogError> {
    let dst = dst
        .filter(|d| d.len() == src.len())
        .ok_or(LogError::Damaged(
            "bytes laid over a span of another length",
        ))?;
    for (to, from) in dst.iter_mut().zip(src) {
        *to = *from;
    }
    Ok(())
}

/// The MAC of a sealed frame (hyper-raft docs/seal.md §5.1): its header and every byte of its payload
/// but the sealed records' stored bytes. Those are authenticated by their own tags, each bound to its
/// file offset under its session's key and to its log, segment, group, index and term, so hashing
/// them again buys nothing; their lengths and CRC fields, and every record that is not sealed, are
/// covered. A payload that does not walk is tampering, its CRC having held.
pub(crate) fn frame_mac(
    mac: &FrameMac,
    frame: &[u8],
    records: u32,
) -> Result<[u8; MAC_LEN], LogError> {
    let (header, payload) = frame
        .split_at_checked(format::FRAME_HEADER_LEN)
        .ok_or(LogError::Tampered("a frame shorter than its header"))?;
    let mut sealed_spans = Vec::new();
    format::sealables(payload, records, |s| {
        sealed_spans.push((s.bytes_at, s.bytes_at.checked_add(s.stored)?));
        Some(())
    })
    .ok_or(LogError::Tampered("a frame's payload does not walk"))?;
    let mut spans = Vec::with_capacity(sealed_spans.len().saturating_mul(2).saturating_add(2));
    spans.push(header);
    let mut at = 0usize;
    for (from, to) in sealed_spans {
        spans.push(
            payload
                .get(at..from)
                .ok_or(LogError::Tampered("a sealed record out of order"))?,
        );
        at = to;
    }
    spans.push(
        payload
            .get(at..)
            .ok_or(LogError::Tampered("a sealed record past its frame"))?,
    );
    mac.mac_spans(spans).map_err(sealed)
}

/// Whether `mac` is `expected`, compared in constant time; a mismatch is tampering.
pub(crate) fn same_mac(expected: &[u8; MAC_LEN], mac: &[u8]) -> Result<(), LogError> {
    let equal = expected.len() == mac.len()
        && expected
            .iter()
            .zip(mac)
            .fold(0u8, |diff, (a, b)| diff | (a ^ b))
            == 0;
    if equal {
        Ok(())
    } else {
        Err(LogError::Tampered("a frame fails its MAC"))
    }
}

fn sealed(e: SealError) -> LogError {
    match e {
        SealError::Tampered => LogError::Tampered("framing fails its MAC"),
        SealError::Open => LogError::Tampered("a sealed record does not open"),
        other => LogError::Seal(other),
    }
}

impl Sealer {
    pub(crate) fn new(sealing: Sealing, log: u128) -> Result<Self, LogError> {
        let mac = FrameMac::new(&sealing.auth, log).map_err(sealed)?;
        Ok(Self {
            parent: sealing.parent,
            mac,
            log,
            writing: None,
            sessions: BTreeMap::new(),
        })
    }

    /// Whether the writer has a session for the segment of `incarnation`.
    pub(crate) fn writes(&self, incarnation: u64) -> bool {
        self.writing
            .as_ref()
            .is_some_and(|(inc, _)| *inc == incarnation)
    }

    /// A new session writing the segment of `incarnation` from `offset`: its key frame, for the
    /// segment's header or a key record, written in the same write as the first records it keys.
    pub(crate) fn begin(
        &mut self,
        incarnation: u64,
        offset: u64,
    ) -> Result<[u8; format::KEY_FRAME_LEN], LogError> {
        let (session, frame) = Session::begin(&self.parent, offset).map_err(sealed)?;
        self.know(incarnation, offset, &frame)?;
        self.writing = Some((incarnation, session));
        Ok(frame.encode())
    }

    /// A session found in a header or a key record: from `offset` on, the segment of
    /// `incarnation`'s records open under it.
    pub(crate) fn found(
        &mut self,
        incarnation: u64,
        offset: u64,
        frame: &[u8; format::KEY_FRAME_LEN],
    ) -> Result<(), LogError> {
        let frame = KeyFrame::decode(frame)
            .map_err(|_| LogError::Damaged("a key frame does not decode"))?;
        self.know(incarnation, offset, &frame)
    }

    fn know(&mut self, incarnation: u64, offset: u64, frame: &KeyFrame) -> Result<(), LogError> {
        let opener = SessionOpener::new(&self.parent, frame).map_err(sealed)?;
        let sessions = self.sessions.entry(incarnation).or_default();
        if sessions.last().is_some_and(|(from, _)| *from >= offset) {
            return Err(LogError::Damaged(
                "a session begins before the one before it",
            ));
        }
        sessions.push((offset, opener));
        Ok(())
    }

    /// Forgets the sessions of segments no longer live: a freed segment's records are dead.
    pub(crate) fn retain(&mut self, live: impl Fn(u64) -> bool) {
        self.sessions.retain(|inc, _| live(*inc));
        if self.writing.as_ref().is_some_and(|(inc, _)| !live(*inc)) {
            self.writing = None;
        }
    }

    /// Seals, in place, every entry and proposal of a laid payload of `records` records whose first
    /// byte is at file offset `base` in the segment of `incarnation`: each one's bytes at its file
    /// offset under the writing session, its tag after them, and its CRC over the sealed bytes.
    pub(crate) fn seal_payload(
        &mut self,
        payload: &mut [u8],
        records: u32,
        base: u64,
        incarnation: u64,
    ) -> Result<(), LogError> {
        let Some((inc, session)) = self.writing.as_mut() else {
            return Err(LogError::Damaged("a sealed frame with no session"));
        };
        if *inc != incarnation {
            return Err(LogError::Damaged(
                "a sealed frame for another segment's session",
            ));
        }
        let mut spans = Vec::new();
        format::sealables(payload, records, |s| {
            spans.push(s);
            Some(())
        })
        .ok_or(LogError::Damaged("a laid payload does not walk"))?;
        let log = self.log;
        for s in spans {
            let plain_len = s
                .stored
                .checked_sub(TAG_LEN)
                .ok_or(LogError::Damaged("a sealed record shorter than its tag"))?;
            let at = s
                .bytes_at
                .checked_add(plain_len)
                .ok_or(LogError::Damaged("an offset past usize"))?;
            let end = s
                .bytes_at
                .checked_add(s.stored)
                .ok_or(LogError::Damaged("an offset past usize"))?;
            let offset = u64::try_from(s.bytes_at)
                .ok()
                .and_then(|b| base.checked_add(b))
                .ok_or(LogError::Damaged("an offset past u64"))?;
            let id = RecordId {
                log,
                incarnation,
                group: s.group,
                index: s.index,
                term: s.term,
            };
            let plain = payload
                .get_mut(s.bytes_at..at)
                .ok_or(LogError::Damaged("a record past its payload"))?;
            let tag = session.seal(offset, &id, plain).map_err(sealed)?;
            put_at(payload.get_mut(at..end), &tag)?;
            let stored = payload
                .get(s.bytes_at..end)
                .ok_or(LogError::Damaged("a record past its payload"))?;
            let crc = format::entry_crc(s.group, s.index, s.term, stored);
            put_at(
                payload.get_mut(s.crc_at..s.crc_at.saturating_add(4)),
                &crc.to_le_bytes(),
            )?;
        }
        Ok(())
    }

    /// The plaintext of a sealed record: `stored`, its bytes and tag as read, at file offset
    /// `offset` in the segment of `incarnation`, as the entry or proposal of `group` at `index` and
    /// `term`. A record that does not open is tampering: its CRC held over what was read.
    pub(crate) fn open(
        &self,
        incarnation: u64,
        offset: u64,
        group: u128,
        index: u64,
        term: u64,
        stored: &[u8],
    ) -> Result<Vec<u8>, LogError> {
        let plain_len = stored
            .len()
            .checked_sub(TAG_LEN)
            .ok_or(LogError::Damaged("a sealed record shorter than its tag"))?;
        let (bytes, tag) = stored.split_at(plain_len);
        let tag: &[u8; TAG_LEN] = tag.try_into().map_err(|_| LogError::Damaged("a tag"))?;
        let opener = self
            .sessions
            .get(&incarnation)
            .and_then(|sessions| sessions.iter().rev().find(|(from, _)| *from <= offset))
            .map(|(_, opener)| opener)
            .ok_or(LogError::Damaged(
                "a sealed record before any session of its segment",
            ))?;
        let id = RecordId {
            log: self.log,
            incarnation,
            group,
            index,
            term,
        };
        let mut plain = bytes.to_vec();
        opener.open(offset, &id, &mut plain, tag).map_err(sealed)?;
        Ok(plain)
    }

    /// The MAC of a sealed frame, `frame` its header and payload of `records` records (§5.1).
    pub(crate) fn mac_frame(&self, frame: &[u8], records: u32) -> Result<[u8; MAC_LEN], LogError> {
        frame_mac(&self.mac, frame, records)
    }

    /// The framing MAC, for a reader of the log's frames: recovery's, and the device's.
    pub(crate) fn frame_mac(&self) -> FrameMac {
        self.mac.clone()
    }

    /// The MAC of `bytes`.
    pub(crate) fn mac(&self, bytes: &[u8]) -> Result<[u8; MAC_LEN], LogError> {
        self.mac.mac(bytes).map_err(sealed)
    }

    /// Whether `mac` is `bytes`' MAC; a mismatch is [`LogError::Tampered`].
    pub(crate) fn verify(&self, bytes: &[u8], mac: &[u8]) -> Result<(), LogError> {
        let mac: &[u8; MAC_LEN] = mac
            .try_into()
            .map_err(|_| LogError::Tampered("a MAC cut short"))?;
        self.mac.verify(bytes, mac).map_err(sealed)
    }
}
