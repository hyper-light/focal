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

/// What spilling a row came to: its spilled form; held for what it is (it may not spill, or its
/// encoding is wider than a spilled row's), which only a change to it alters; or refused by the
/// log for now (full, or failing), to be tried again.
pub(crate) enum Spill<T> {
    Spilled(T),
    Stays,
    Refused,
}

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

/// The appends a log holds back while the candidate that made them is weighed
/// ([`PayloadLog::hold`]): what keeps them or gives them back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PayloadHold(u64);

impl PayloadHold {
    /// The hold a log numbered `serial`.
    pub fn new(serial: u64) -> Self {
        Self(serial)
    }
    pub fn serial(self) -> u64 {
        self.0
    }
}

/// A replica's payload log as its core holds it. Rows are spilled where they are built, while a
/// candidate is prepared on a read-only view of the core, so an append takes `&self`: the log
/// serializes its own writes (a single writer, the owner, in practice).
///
/// A leader's candidate spills its rows as its plan builds them, before the session has weighed
/// it, so its appends are held ([`Self::hold`]) until it is admitted ([`Self::keep`]) or refused
/// ([`Self::give_back`]): a refused candidate leaves nothing in the log, as a broker validates a
/// batch before it appends it. Its owner keeps a hold before anything else appends, so what a
/// hold holds back is its candidate's alone.
pub trait PayloadLog: PayloadRead + Send + Sync {
    /// `bytes` appended: where they are, or `None` when the log refuses them (its bound, an I/O
    /// error), and the row keeps its bytes.
    fn append(&self, bytes: &[u8]) -> Option<PayloadRef>;
    /// Hold back every byte appended from now on until [`Self::keep`] or [`Self::give_back`] is
    /// handed what this returns: none of it is the log's, or written, before. A hold begun while
    /// another stands keeps that one. None where the log cannot hold (it is failing), and what is
    /// appended is the log's at once.
    fn hold(&self) -> Option<PayloadHold>;
    /// What `hold` held back is the log's, written as anything else is. A hold that no longer
    /// stands changes nothing.
    fn keep(&self, hold: PayloadHold);
    /// What `hold` held back is let go: the log's next append goes where its first did, as if it
    /// had never been made. A hold that no longer stands changes nothing. Whether it let go of
    /// anything.
    fn give_back(&self, hold: PayloadHold) -> bool;
    /// A reader of what the log holds now, for another thread (a frozen image's encoder).
    fn reader(&self) -> Result<Box<dyn PayloadRead + Send>, PayloadError>;
    /// Let go of every sealed part of the log before segment `before` that no row references,
    /// `named` saying whether some row does (every committed and pending row's, as a sweep's pass
    /// found them, [`Marks`]). How many parts it let go.
    fn sweep(&self, named: &dyn Fn(u32) -> bool, before: u32) -> usize {
        let _ = (named, before);
        0
    }
    /// The segment appends go to now: what a sweep's pass that begins now may let go of is before
    /// it.
    fn segment(&self) -> u32 {
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

/// Where a spilled row's bytes are in the log, whatever its kind: none for a row that holds them.
pub(super) fn logged(row: &super::Row) -> Option<PayloadRef> {
    match row {
        super::Row::Event(super::OwnedEvent::Logged(at))
        | super::Row::ClaimContent(super::OwnedClaimContent::Logged(at)) => Some(*at),
        super::Row::Outcome(super::OwnedOutcome::Logged(at)) => Some(at.at()),
        super::Row::CreationResult(super::creation_result::OwnedCreationResult::Logged(at)) => {
            Some(*at)
        }
        super::Row::Claim(claim) => claim.logged(),
        super::Row::Definition(definition) => definition.logged(),
        _ => None,
    }
}

/// The committed rows a sweep's pass visits a step: a few tens of microseconds of a replica's
/// thread, so no step walks the whole root.
#[cfg(not(test))]
pub(super) const MARK_ROWS: usize = 4096;
/// A few rows a step in this crate's tests, so a pass over a test's root takes several.
#[cfg(test)]
pub(super) const MARK_ROWS: usize = 16;

/// The steps a pass over `rows` committed rows takes at most: [`MARK_ROWS`] rows a step, and the
/// last step finds the end.
pub(super) fn pass_steps(rows: usize) -> usize {
    rows.checked_div(MARK_ROWS)
        .unwrap_or(rows)
        .saturating_add(2)
}

/// What a step of the payload log's sweep did (doc 31 M3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SweepStep {
    /// No pass under way, and none due.
    Idle,
    /// A pass goes on.
    Going,
    /// A pass ended, and its sweep let go of this many segments.
    Swept(usize),
    /// A pass ended short: it could not be charged for a segment it met, so its notes are
    /// incomplete and it let go of nothing. Nothing was swept: the next pass is as due as this was.
    Short,
}

/// The share of the committed rows that, written since the last pass that swept began, makes
/// another due though the log has not grown (doc 31 M3): an eighth. A retirement or a deletion
/// leaves spilled copies no row names without sealing a segment, and a log at its segment cap
/// seals none at all. Every row written counts (put, replaced or deleted; a new row leaves no
/// copy, so a pass may come early, never late), and a pass's walk of every row costs at most
/// eight visits for each.
pub const CHURN_SHARE: u64 = 8;

/// A pass over a core's committed rows that learns which segments of its payload log they name, and
/// how many of each segment's bytes (doc 31 M3), [`MARK_ROWS`] rows a step from where the last step
/// stopped, and the sweep it allows: the segments sealed before it began that no row named.
///
/// - **Every row counts.** A pass that named the events alone let go of a segment only a claim, a
///   definition, an outcome or a content named, and that row could not be read back.
/// - **Rows put while it goes on.** A row spilled while it goes on lands in the segment appends go
///   to, from where it began, never one it may let go of; a row a publication puts while it goes on
///   may stand where it has already been, so a publication names its rows' segments to it
///   ([`Marks::published`]).
///
/// What it notes is charged as it grows, a doubling at a time, its charge admitted before the
/// growth, and kept with the room between passes. A pass that cannot be charged for a segment it
/// meets notes the shortfall and sweeps nothing: its notes are incomplete, and a segment one of its
/// rows names would read as named by none. The next pass begins afresh.
#[derive(Debug, Default)]
pub(crate) struct Marks {
    /// Where appends went when the pass began: what it may let go of is before it. None between
    /// passes.
    from: Option<u32>,
    /// The last row the pass visited: the next step begins after it.
    after: Option<super::Key>,
    /// The segments the rows it visited, the rows published since it began and a leader's pending
    /// candidates name, sorted, each with the bytes seen naming it. Bounded by the segments the log
    /// holds.
    named: Vec<(u32, u64)>,
    /// What `named`'s room is charged, held while it holds the room.
    charge: Option<focal_memory::Allocation>,
    /// A segment it met and could not note for want of memory: its sweep lets go of nothing.
    short: bool,
    /// The rows publications and seals wrote since the last pass that swept began: each it
    /// replaced or deleted may have left a spilled copy no row names.
    churn: u64,
    /// `churn` when the pass under way began: what its sweep accounts for once it sweeps.
    churn_at_begin: u64,
    /// Refuse to grow the notes' room, as a budget out of memory does (a session's tests).
    #[cfg(any(test, feature = "test-support"))]
    refuse_growth: bool,
    /// What the passes have done, since the core was made ([`PayloadSweep`]).
    done: PayloadSweep,
}

/// What a core's payload log sweep has done since the core was made (doc 31 M3): the passes that
/// ended, the segments their sweeps let go, the passes that could not be charged for what they met
/// and so swept nothing, and the bytes the last pass saw its rows name. That last is an upper bound
/// of what is live beside the log's own size, not an exact count: a row published while a pass
/// goes on is counted where it is published and again where the pass reaches it, and a row that
/// is replaced or deleted after its count still counts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PayloadSweep {
    pub passes: u64,
    pub let_go: u64,
    pub short: u64,
    pub named: u64,
}

impl Marks {
    /// Whether a pass is under way.
    pub(super) fn running(&self) -> bool {
        self.from.is_some()
    }
    /// A pass begun where appends go now: what it may let go of is before `from`.
    pub(super) fn begin(&mut self, from: u32) {
        self.from = Some(from);
        self.after = None;
        self.named.clear();
        self.short = false;
        self.churn_at_begin = self.churn;
    }
    /// `rows` rows written: put, replaced or deleted.
    pub(super) fn churned(&mut self, rows: usize) {
        self.churn = self
            .churn
            .saturating_add(u64::try_from(rows).unwrap_or(u64::MAX));
    }
    /// Whether the rows written since the last pass that swept began reach a [`CHURN_SHARE`]th of
    /// `rows`, the committed rows: another pass is due though the log has not grown.
    pub(super) fn churned_enough(&self, rows: usize) -> bool {
        self.churn != 0
            && self.churn.saturating_mul(CHURN_SHARE) >= u64::try_from(rows).unwrap_or(u64::MAX)
    }
    /// The rows written since the last pass that swept began.
    #[cfg(test)]
    pub(super) fn churn(&self) -> u64 {
        self.churn
    }
    /// Refuse, or allow again, the growth of the notes' room (a session's tests).
    #[cfg(any(test, feature = "test-support"))]
    pub(super) fn refuse_growth(&mut self, refuse: bool) {
        self.refuse_growth = refuse;
    }
    /// `at`'s segment noted as named, its bytes added to what names it; the room for a segment
    /// not yet noted grown first where there is none, and the pass short where it cannot be.
    fn name(&mut self, at: PayloadRef, budget: &focal_memory::MemoryBudget) {
        let bytes = u64::try_from(at.len()).unwrap_or(u64::MAX);
        match self
            .named
            .binary_search_by_key(&at.segment(), |(segment, _)| *segment)
        {
            Ok(found) => {
                if let Some((_, named)) = self.named.get_mut(found) {
                    *named = named.saturating_add(bytes);
                }
            }
            Err(place) => {
                if self.named.len() == self.named.capacity() && !self.grow(budget) {
                    self.short = true;
                    return;
                }
                // Within the room just made: no reallocation.
                self.named.insert(place, (at.segment(), bytes));
            }
        }
    }
    /// `named`'s room doubled (sixteen segments at least), its charge admitted first, on the
    /// completion lane: a sweep gives back what it lets go of, and runs while fresh work's lane is
    /// spent.
    fn grow(&mut self, budget: &focal_memory::MemoryBudget) -> bool {
        #[cfg(any(test, feature = "test-support"))]
        if self.refuse_growth {
            return false;
        }
        if self.charge.is_none() {
            match budget.reserve(
                focal_memory::BudgetKind::Payload,
                focal_memory::BudgetLane::Completion,
                0,
            ) {
                Ok(permit) => self.charge = Some(permit.commit()),
                Err(_) => return false,
            }
        }
        let Some(charge) = self.charge.as_mut() else {
            return false;
        };
        let have = self.named.capacity();
        let want = have.saturating_mul(2).max(16);
        let Some(more) = want
            .checked_sub(have)
            .and_then(|more| more.checked_mul(size_of::<(u32, u64)>()))
        else {
            return false;
        };
        if charge.grow(more).is_err() {
            return false;
        }
        if self
            .named
            .try_reserve_exact(want.saturating_sub(self.named.len()))
            .is_err()
        {
            let held = charge.bytes().saturating_sub(more);
            // Given back exactly what was admitted for it.
            let _ = charge.shrink_to(held);
            return false;
        }
        true
    }
    /// Whether some row the pass met names `segment`.
    pub(super) fn names(&self, segment: u32) -> bool {
        self.named
            .binary_search_by_key(&segment, |(at, _)| *at)
            .is_ok()
    }
    /// The rows a publication puts while a pass goes on name their segments to it.
    pub(super) fn published<'a>(
        &mut self,
        rows: impl Iterator<Item = &'a super::Row>,
        budget: &focal_memory::MemoryBudget,
    ) {
        if self.running() {
            self.pending(rows, budget);
        }
    }
    /// The rows of a leader's pending candidates name their segments to the pass at its end.
    pub(super) fn pending<'a>(
        &mut self,
        rows: impl Iterator<Item = &'a super::Row>,
        budget: &focal_memory::MemoryBudget,
    ) {
        for at in rows.filter_map(logged) {
            self.name(at, budget);
        }
    }
    /// What the passes have done.
    pub(super) fn done(&self) -> PayloadSweep {
        self.done
    }
    /// Where the next step begins: after the last row visited, from the first at the start.
    pub(super) fn after(&self) -> Option<&super::Key> {
        self.after.as_ref()
    }
    /// The rows a step reached, at most [`MARK_ROWS`] from `rows`: their segments named, the last
    /// of them where the next step begins. Whether it reached the last row.
    pub(super) fn visited<'a>(
        &mut self,
        rows: impl Iterator<Item = super::ranges::RowEntry<'a>>,
        budget: &focal_memory::MemoryBudget,
    ) -> bool {
        let mut visited = 0usize;
        for entry in rows {
            if let Some(at) = logged(entry.value) {
                self.name(at, budget);
            }
            self.after = Some(entry.key);
            visited = visited.saturating_add(1);
        }
        visited < MARK_ROWS
    }
    /// The pass ended: where it began, where it noted every segment it met; none where it fell
    /// short, and its sweep lets go of nothing. Its notes stay for the sweep to read until
    /// [`Self::clear`].
    pub(super) fn end(&mut self) -> Option<u32> {
        let from = self.from.take()?;
        self.after = None;
        self.done.passes = self.done.passes.saturating_add(1);
        self.done.named = self
            .named
            .iter()
            .fold(0u64, |sum, (_, bytes)| sum.saturating_add(*bytes));
        if self.short {
            self.done.short = self.done.short.saturating_add(1);
            return None;
        }
        Some(from)
    }
    /// The notes of a pass whose sweep is made let go: the room and its charge are kept.
    pub(super) fn clear(&mut self) {
        self.named.clear();
        self.short = false;
    }
    /// A pass's sweep was made and let go of `segments`: what the rows written before the pass
    /// began could have left unnamed is accounted for.
    pub(super) fn swept(&mut self, segments: usize) {
        let segments = u64::try_from(segments).unwrap_or(u64::MAX);
        self.done.let_go = self.done.let_go.saturating_add(segments);
        self.churn = self.churn.saturating_sub(self.churn_at_begin);
        self.churn_at_begin = 0;
    }
}

/// Spill every row among `changes` the log keeps (an event; a claim's content that fits) to
/// `log`, each entry's heap charge the spilled row's: what a candidate's rows become once every
/// check has read them held, the leader's at the end of its plan and a replay's once it is
/// validated, so no check reads back what was just written. A row the log refuses keeps its
/// bytes and its charge. A claim and its definitions stay held while their candidate is pending
/// (the owner's checks of a candidate read them where they are) and are spilled where their
/// publication leaves them (`Core::spill_published`).
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
    /// The holds it has numbered, and the one standing: its number and where it began.
    holds: std::sync::Mutex<(u64, Option<(u64, usize)>)>,
}

#[cfg(test)]
impl MemoryPayloads {
    /// A log that refuses appends past `bound` bytes, where there is one.
    pub(crate) fn bounded(bound: Option<usize>) -> Self {
        Self {
            bound,
            ..Self::default()
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
    fn hold(&self) -> Option<PayloadHold> {
        let mut holds = self.holds.lock().ok()?;
        let serial = holds.0.checked_add(1)?;
        let from = self.bytes.lock().ok()?.len();
        *holds = (serial, Some((serial, from)));
        Some(PayloadHold::new(serial))
    }
    fn keep(&self, hold: PayloadHold) {
        if let Ok(mut holds) = self.holds.lock()
            && holds.1.is_some_and(|(serial, _)| serial == hold.serial())
        {
            holds.1 = None;
        }
    }
    fn give_back(&self, hold: PayloadHold) -> bool {
        let Ok(mut holds) = self.holds.lock() else {
            return false;
        };
        let Some((_, from)) = holds.1.filter(|(serial, _)| *serial == hold.serial()) else {
            return false;
        };
        holds.1 = None;
        let Ok(mut bytes) = self.bytes.lock() else {
            return false;
        };
        let gave = bytes.len() > from;
        bytes.truncate(from);
        gave
    }
    fn reader(&self) -> Result<Box<dyn PayloadRead + Send>, PayloadError> {
        let bytes = self.bytes.lock().map_err(|_| PayloadError::Unavailable)?;
        Ok(Box::new(Self {
            bytes: std::sync::Mutex::new(bytes.clone()),
            ..Self::default()
        }))
    }
    fn held_bytes(&self) -> u64 {
        self.bytes
            .lock()
            .map_or(0, |bytes| u64::try_from(bytes.len()).unwrap_or(u64::MAX))
    }
}

/// A log of segments of at most `size` bytes each (a segment of its own for every append at
/// zero), its sweep letting go of every sealed segment it is told no row names, as a file-backed
/// log does: a read of a segment let go fails.
#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct SegmentedPayloads {
    size: usize,
    segments: std::sync::Mutex<Vec<Option<Vec<u8>>>>,
}

#[cfg(test)]
impl SegmentedPayloads {
    fn current(segments: &[Option<Vec<u8>>]) -> u32 {
        u32::try_from(segments.len().saturating_sub(1)).unwrap()
    }
}

#[cfg(test)]
impl PayloadRead for SegmentedPayloads {
    fn read(&self, at: PayloadRef, into: &mut [u8]) -> Result<(), PayloadError> {
        let segments = self.segments.lock().unwrap();
        let segment = segments
            .get(usize::try_from(at.segment()).unwrap())
            .and_then(Option::as_ref)
            .ok_or(PayloadError::Unavailable)?;
        let from = usize::try_from(at.offset()).unwrap();
        let bytes = segment
            .get(from..from + at.len())
            .ok_or(PayloadError::Unavailable)?;
        if into.len() != bytes.len() || !at.holds(bytes) {
            return Err(PayloadError::Corrupt);
        }
        into.copy_from_slice(bytes);
        Ok(())
    }
}

#[cfg(test)]
impl PayloadLog for SegmentedPayloads {
    fn append(&self, bytes: &[u8]) -> Option<PayloadRef> {
        let mut segments = self.segments.lock().unwrap();
        let fits = segments
            .last()
            .and_then(Option::as_ref)
            .is_some_and(|last| last.len() + bytes.len() <= self.size);
        if !fits {
            segments.push(Some(Vec::new()));
        }
        let segment = Self::current(&segments);
        let last = segments.last_mut().unwrap().as_mut().unwrap();
        let at = PayloadRef::at(segment, u32::try_from(last.len()).unwrap(), bytes).unwrap();
        last.extend_from_slice(bytes);
        Some(at)
    }
    fn hold(&self) -> Option<PayloadHold> {
        None
    }
    fn keep(&self, _: PayloadHold) {}
    fn give_back(&self, _: PayloadHold) -> bool {
        false
    }
    fn reader(&self) -> Result<Box<dyn PayloadRead + Send>, PayloadError> {
        let segments = self.segments.lock().unwrap().clone();
        Ok(Box::new(SegmentedPayloads {
            size: self.size,
            segments: std::sync::Mutex::new(segments),
        }))
    }
    fn sweep(&self, named: &dyn Fn(u32) -> bool, before: u32) -> usize {
        let mut segments = self.segments.lock().unwrap();
        let current = Self::current(&segments);
        let mut removed = 0;
        for (id, segment) in segments.iter_mut().enumerate() {
            let id = u32::try_from(id).unwrap();
            if id < before && id != current && segment.is_some() && !named(id) {
                *segment = None;
                removed += 1;
            }
        }
        removed
    }
    fn segment(&self) -> u32 {
        Self::current(&self.segments.lock().unwrap())
    }
    fn held_segments(&self) -> usize {
        self.segments.lock().unwrap().iter().flatten().count()
    }
    fn held_bytes(&self) -> u64 {
        let segments = self.segments.lock().unwrap();
        segments
            .iter()
            .flatten()
            .map(|bytes| u64::try_from(bytes.len()).unwrap())
            .sum()
    }
}
