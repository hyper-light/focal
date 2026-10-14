//! A replica's payload log (doc 31 M3): the bytes of rows written once and read rarely, held
//! off the heap where the replica has somewhere to put them, as Kafka and NATS keep messages in
//! their logs. Such a row holds where its bytes are, sixteen bytes, instead of the bytes.
//!
//! The log is a cache of what the replica's records and checkpoints hold: a replica that loses it
//! rebuilds it from them, so it is written without a sync, and every replica keeps its own. A row
//! is spilled where it is built (a leader's candidate once its record is encoded, a follower's
//! replay, a restore), never where it is published: publication stays without I/O. A log that
//! refuses a payload (its bound, an I/O error) leaves the row holding its bytes.

/// Most a spilled row's bytes may be, whatever its kind: what a reader's stack buffer holds. A row
/// whose bytes are wider stays held.
pub(crate) const SPILLED_BYTES: usize = 1024;

/// Where a payload's bytes are in the log: its segment, offset and length, and a check over the
/// bytes, so a read that returns anything else is refused rather than decoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PayloadRef {
    segment: u32,
    offset: u32,
    len: u32,
    check: u32,
}

impl PayloadRef {
    /// A payload of `bytes` at `offset` in `segment`.
    pub fn at(segment: u32, offset: u32, bytes: &[u8]) -> Result<Self, PayloadError> {
        Ok(Self {
            segment,
            offset,
            len: u32::try_from(bytes.len()).map_err(|_| PayloadError::Capacity)?,
            check: check(bytes),
        })
    }
    /// A reference from its parts, as a spilled row that keeps them apart holds them.
    pub(crate) fn from_parts(segment: u32, offset: u32, len: u32, check: u32) -> Self {
        Self {
            segment,
            offset,
            len,
            check,
        }
    }
    pub fn segment(self) -> u32 {
        self.segment
    }
    /// The check over the payload's bytes.
    pub(crate) fn check(self) -> u32 {
        self.check
    }
    pub fn offset(self) -> u32 {
        self.offset
    }
    pub fn len(self) -> usize {
        usize::try_from(self.len).unwrap_or(usize::MAX)
    }
    pub fn is_empty(self) -> bool {
        self.len == 0
    }
    /// Whether `bytes` are this payload's.
    pub fn holds(self, bytes: &[u8]) -> bool {
        bytes.len() == self.len() && check(bytes) == self.check
    }
}

/// The check a reference keeps over its bytes: their CRC-32, which refuses a read that returned
/// other bytes (a torn or misdirected write, a reused offset) at a few nanoseconds a row, where a
/// cryptographic hash took a hundred or more. The log is a replica's own cache, never a peer's
/// input, so the check guards against accident, not adversaries.
fn check(bytes: &[u8]) -> u32 {
    crc32fast::hash(bytes)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PayloadError {
    /// The log is gone or could not be read.
    #[error("payload log unavailable")]
    Unavailable,
    /// What the log returned is not the payload the reference names.
    #[error("payload does not match its reference")]
    Corrupt,
    #[error("payload log capacity")]
    Capacity,
}

/// Reads a replica's payload log. A reader may be held on another thread (a checkpoint's
/// encoder), so it takes `&self`.
pub trait PayloadRead: std::fmt::Debug {
    /// The payload at `at`, written into `into`, which is exactly `at.len()` bytes; refused
    /// unless what the log holds there passes the reference's check.
    fn read(&self, at: PayloadRef, into: &mut [u8]) -> Result<(), PayloadError>;
}

/// A replica's payload log as its core holds it. Rows are spilled where they are built, while a
/// candidate is prepared on a read-only view of the core, so an append takes `&self`: the log
/// serializes its own writes (a single writer, the owner, in practice).
pub trait PayloadLog: PayloadRead + Send + Sync {
    /// `bytes` appended: where they are, or `None` when the log refuses them (its bound, an I/O
    /// error), and the row keeps its bytes.
    fn append(&self, bytes: &[u8]) -> Option<PayloadRef>;
    /// A reader of what the log holds now, for another thread (a frozen image's encoder).
    fn reader(&self) -> Result<Box<dyn PayloadRead + Send>, PayloadError>;
    /// Let go of every sealed part of the log no row references, `live` naming the segments
    /// that some row does (every committed and pending row's, [`segments_of`]). How many parts
    /// it let go.
    fn sweep(&self, live: &std::collections::BTreeSet<u32>) -> usize {
        let _ = live;
        0
    }
    /// The parts (segments) the log holds now: what a sweep's schedule follows.
    fn held_segments(&self) -> usize {
        0
    }
    /// The payload bytes appended to the parts the log holds now, written and not yet written:
    /// what the replica has spilled off its heap.
    fn held_bytes(&self) -> u64 {
        0
    }
}

/// The segment of every spilled row among `rows`, added to `live`. Bounded by the segments a log
/// holds: a set of `u32`.
pub(super) fn segments_of<'a>(
    rows: impl Iterator<Item = &'a super::Row>,
    live: &mut std::collections::BTreeSet<u32>,
) {
    for row in rows {
        match row {
            super::Row::Event(super::OwnedEvent::Logged(at))
            | super::Row::ClaimContent(super::OwnedClaimContent::Logged(at)) => {
                live.insert(at.segment());
            }
            super::Row::Outcome(super::OwnedOutcome::Logged(at)) => {
                live.insert(at.at().segment());
            }
            super::Row::CreationResult(super::creation_result::OwnedCreationResult::Logged(at)) => {
                live.insert(at.segment());
            }
            _ => {}
        }
    }
}

/// Spill every row among `changes` the log keeps (an event; a claim's content that fits) to
/// `log`, each entry's heap charge the spilled row's: what a candidate's rows become once every
/// check has read them held, the leader's at the end of its plan and a replay's once it is
/// validated, so no check reads back what was just written. A row the log refuses keeps its
/// bytes and its charge.
pub(super) fn spill_changes(
    changes: &mut [focal_memory::Change<super::Key, super::Row>],
    log: Option<&dyn PayloadLog>,
    ledger: super::LedgerId,
) -> Result<(), super::NativeError> {
    let Some(log) = log else {
        return Ok(());
    };
    for change in changes.iter_mut() {
        let focal_memory::Change::Put(entry) = change else {
            continue;
        };
        let spilled = match (&entry.key, &entry.value) {
            (_, super::Row::Event(event)) => event.spill(ledger, log)?.map(super::Row::Event),
            (_, super::Row::ClaimContent(content)) => {
                content.spill(log).map(super::Row::ClaimContent)
            }
            (super::Key::Outcome(invocation), super::Row::Outcome(outcome)) => outcome
                .spill(ledger, *invocation, log)?
                .map(super::Row::Outcome),
            (_, super::Row::CreationResult(result)) => {
                result.spill(log)?.map(super::Row::CreationResult)
            }
            _ => None,
        };
        if let Some(row) = spilled {
            // A spilled row holds nothing on the heap, its box's included.
            entry.heap_bytes = match &row {
                super::Row::Event(event) => event.heap_charge()?,
                super::Row::ClaimContent(content) => content.heap_charge()?,
                super::Row::CreationResult(result) => result.heap_charge()?,
                _ => row.boxed_heap(),
            };
            entry.value = row;
        }
    }
    Ok(())
}

/// The read side of a held log.
pub(crate) fn source(log: &Option<Box<dyn PayloadLog>>) -> Option<&dyn PayloadRead> {
    log.as_deref().map(|log| -> &dyn PayloadRead { log })
}

/// Read the payload at `at` into a stack buffer of `N` bytes and hand the bytes to `read`: a
/// row whose bytes are bounded (an event's are) is read without allocating.
pub(crate) fn with_payload<const N: usize, T>(
    source: &dyn PayloadRead,
    at: PayloadRef,
    read: impl FnOnce(&[u8]) -> Option<T>,
) -> Option<T> {
    let mut buffer = [0u8; N];
    let bytes = buffer.get_mut(..at.len())?;
    source.read(at, bytes).ok()?;
    read(bytes)
}

/// A payload log held in memory, for tests: what a file-backed log does, without the file.
#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct MemoryPayloads {
    bytes: std::sync::Mutex<Vec<u8>>,
    /// Refuse appends past this many bytes, to exercise a full log.
    pub(crate) bound: Option<usize>,
}

#[cfg(test)]
impl MemoryPayloads {
    /// A log that refuses appends past `bound` bytes, where there is one.
    pub(crate) fn bounded(bound: Option<usize>) -> Self {
        Self {
            bytes: std::sync::Mutex::new(Vec::new()),
            bound,
        }
    }
}

#[cfg(test)]
impl PayloadRead for MemoryPayloads {
    fn read(&self, at: PayloadRef, into: &mut [u8]) -> Result<(), PayloadError> {
        let bytes = self.bytes.lock().map_err(|_| PayloadError::Unavailable)?;
        let start = usize::try_from(at.offset()).map_err(|_| PayloadError::Corrupt)?;
        let end = start.checked_add(at.len()).ok_or(PayloadError::Corrupt)?;
        let held = bytes.get(start..end).ok_or(PayloadError::Corrupt)?;
        if into.len() != at.len() || !at.holds(held) {
            return Err(PayloadError::Corrupt);
        }
        into.copy_from_slice(held);
        Ok(())
    }
}

#[cfg(test)]
impl PayloadLog for MemoryPayloads {
    fn append(&self, bytes: &[u8]) -> Option<PayloadRef> {
        let mut held = self.bytes.lock().ok()?;
        let end = held.len().checked_add(bytes.len())?;
        if self.bound.is_some_and(|bound| end > bound) {
            return None;
        }
        let at = PayloadRef::at(0, u32::try_from(held.len()).ok()?, bytes).ok()?;
        held.extend_from_slice(bytes);
        Some(at)
    }
    fn reader(&self) -> Result<Box<dyn PayloadRead + Send>, PayloadError> {
        let bytes = self.bytes.lock().map_err(|_| PayloadError::Unavailable)?;
        Ok(Box::new(Self {
            bytes: std::sync::Mutex::new(bytes.clone()),
            bound: None,
        }))
    }
}
