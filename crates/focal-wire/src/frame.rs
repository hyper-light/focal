use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const MAGIC: &[u8; 8] = b"FOCALQ01";
const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
pub const HEADER_BYTES: usize = 16;

#[cfg(test)]
#[path = "frame_buffer_tests.rs"]
mod buffer_tests;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum FrameKind {
    Hello = 1,
    HelloReply = 2,
    Request = 3,
    Response = 4,
}

#[derive(Debug, Error)]
pub enum WireError {
    #[error("transport I/O failed")]
    Io(#[from] std::io::Error),
    #[error("frame exceeds negotiated allocation")]
    Limit,
    #[error("frame buffer allocation failed")]
    Allocation,
    #[error("invalid frame magic, version, kind, encoding, or trailing bytes")]
    InvalidFrame,
    #[error("operation timed out")]
    Timeout,
    #[error("TLS authentication or configuration failed")]
    Authentication,
    #[error("connection unavailable")]
    Connection,
    #[error("protocol rejected: {0}")]
    Access(#[from] crate::AccessError),
}

/// A checked frame envelope, without payload or participant authority. Read it
/// first when the caller must acquire a buffer before consuming any body bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    kind: FrameKind,
    payload_bytes: usize,
}

impl FrameHeader {
    pub fn kind(self) -> FrameKind {
        self.kind
    }

    pub fn payload_bytes(self) -> usize {
        self.payload_bytes
    }
}

fn payload_buffer(length: usize) -> Result<Vec<u8>, WireError> {
    if length > MAX_FRAME_BYTES {
        return Err(WireError::Limit);
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length)
        .map_err(|_| WireError::Allocation)?;
    if bytes.capacity() > length {
        return Err(WireError::Limit);
    }
    // Successful reservation supplies the complete capacity before zeroing.
    bytes.resize(length, 0);
    Ok(bytes)
}

/// The exact serialized length of `value`, enforcing the same frame limit as
/// [encode_payload] but without allocating or serializing into a buffer. Use
/// this on the per-message ingress/measure paths that only need the length.
pub fn payload_len<T: Serialize>(value: &T, limit: u32) -> Result<usize, WireError> {
    if u64::from(limit) > MAX_FRAME_BYTES as u64 {
        return Err(WireError::Limit);
    }
    let size =
        postcard::experimental::serialized_size(value).map_err(|_| WireError::InvalidFrame)?;
    if size > limit as usize {
        return Err(WireError::Limit);
    }
    Ok(size)
}
pub fn encode_payload<T: Serialize>(value: &T, limit: u32) -> Result<Vec<u8>, WireError> {
    if u64::from(limit) > MAX_FRAME_BYTES as u64 {
        return Err(WireError::Limit);
    }
    let capacity =
        postcard::experimental::serialized_size(value).map_err(|_| WireError::InvalidFrame)?;
    if capacity > limit as usize {
        return Err(WireError::Limit);
    }
    // The buffer is reserved to the measured size and written once, by
    // appending: no pass zeroes it first (the audit's F52), and a measured
    // size the encode did not match — a value that serialized to more or
    // to less — is an invalid frame, never a grown buffer.
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(capacity)
        .map_err(|_| WireError::Allocation)?;
    let bytes = postcard::to_extend(value, bytes).map_err(|_| WireError::InvalidFrame)?;
    if bytes.len() != capacity || bytes.capacity() != capacity {
        return Err(WireError::InvalidFrame);
    }
    Ok(bytes)
}
pub fn decode_payload<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, WireError> {
    let (value, remaining) =
        postcard::take_from_bytes(bytes).map_err(|_| WireError::InvalidFrame)?;
    if !remaining.is_empty() {
        return Err(WireError::InvalidFrame);
    }
    Ok(value)
}
pub async fn write_frame<W: AsyncWrite + Unpin, T: Serialize>(
    writer: &mut W,
    kind: FrameKind,
    value: &T,
    limit: u32,
) -> Result<(), WireError> {
    let bytes = encode_payload(value, limit)?;
    let mut header = [0u8; HEADER_BYTES];
    header[..8].copy_from_slice(MAGIC);
    header[8..10].copy_from_slice(&1u16.to_be_bytes());
    header[10..12].copy_from_slice(&(kind as u16).to_be_bytes());
    header[12..16].copy_from_slice(&(bytes.len() as u32).to_be_bytes());
    writer.write_all(&header).await?;
    writer.write_all(&bytes).await?;
    Ok(())
}
pub async fn read_frame<R: AsyncRead + Unpin, T: DeserializeOwned>(
    reader: &mut R,
    kind: FrameKind,
    limit: u32,
) -> Result<T, WireError> {
    let header = read_frame_header(reader, kind, limit).await?;
    let mut bytes = payload_buffer(header.payload_bytes())?;
    let payload = read_frame_payload_into(reader, header, &mut bytes).await?;
    decode_payload(payload)
}

/// Read only the fixed header and validate its version, kind and both length
/// limits. This uses stack storage and leaves the complete payload unread.
pub async fn read_frame_header<R: AsyncRead + Unpin>(
    reader: &mut R,
    kind: FrameKind,
    limit: u32,
) -> Result<FrameHeader, WireError> {
    let mut header = [0u8; HEADER_BYTES];
    reader.read_exact(&mut header).await?;
    if header.get(..8) != Some(MAGIC.as_slice())
        || header.get(8..10) != Some(1u16.to_be_bytes().as_slice())
        || header.get(10..12) != Some((kind as u16).to_be_bytes().as_slice())
    {
        return Err(WireError::InvalidFrame);
    }
    let len = u32::from_be_bytes(
        header
            .get(12..16)
            .ok_or(WireError::InvalidFrame)?
            .try_into()
            .map_err(|_| WireError::InvalidFrame)?,
    );
    let payload_bytes = usize::try_from(len).map_err(|_| WireError::Limit)?;
    if len > limit || payload_bytes > MAX_FRAME_BYTES {
        return Err(WireError::Limit);
    }
    Ok(FrameHeader {
        kind,
        payload_bytes,
    })
}

/// Read exactly one checked payload into caller-owned storage without allocating.
/// Insufficient storage refuses before any payload read or buffer modification.
/// Extra buffer capacity remains untouched. I/O failure may leave a partial
/// payload in the buffer; discard it and resolve/reset the stream before reuse.
/// The caller retains its buffer accounting through parsing and dispatch.
pub async fn read_frame_payload_into<'a, R: AsyncRead + Unpin>(
    reader: &mut R,
    header: FrameHeader,
    buffer: &'a mut [u8],
) -> Result<&'a [u8], WireError> {
    let payload = buffer
        .get_mut(..header.payload_bytes())
        .ok_or(WireError::Limit)?;
    reader.read_exact(payload).await?;
    Ok(payload)
}

/// What one datagram of the least size a path of QUIC carries holds.
pub const LEAST_PROGRESS: usize = 1_200;

/// The longest a peer may hold an acknowledgement before sending it, as
/// every connection of this crate advertises it: the transport parameter's
/// default (RFC 9000 §18.2, `max_ack_delay`; quinn-proto 0.11, which this
/// crate leaves at it). A sender's probe timer counts it (RFC 9002
/// §6.2.1): what a peer may wait before acknowledging is time the sender
/// cannot tell a loss in.
pub const MAX_ACK_DELAY: std::time::Duration = std::time::Duration::from_millis(25);

/// The probe timeout of a path whose round trip is `rtt`: the round trip
/// and four times its variance, and the acknowledgement delay the peer may
/// take (RFC 9002 §6.2.1) — three round trips with the variance a first
/// sample is given (§5.3), which is all that one who knows the round trip
/// alone has of it, and [`MAX_ACK_DELAY`]. On a path of a millisecond a
/// loss is recovered no sooner than the acknowledgement delay allows: a
/// probe timeout of three milliseconds was a time no sender could meet
/// (the jittered fleet of 27 §12, which refused what a lost datagram's
/// recovery brought a moment after its patience, 2026-10-02).
pub fn probe_timeout(rtt: std::time::Duration) -> std::time::Duration {
    rtt.saturating_mul(3).saturating_add(MAX_ACK_DELAY)
}

/// How long `bytes` may take to arrive over a path whose round trip is
/// `rtt`, at the least a live sender delivers: two datagrams of the least
/// size in a probe timeout. A sender whose window is as small as QUIC
/// keeps it, two datagrams (RFC 9002 §7.2), and whose every flight has to
/// be asked for again, sends two datagrams when its probe timer ends and
/// no fewer (§6.2.4, which the window does not hold back, §7.5). A sender
/// slower than that is not sending; a faster one is done sooner. A
/// payload's buffer is held no longer than this (the audit's F03:
/// occupancy is priced by the path, not by a fixed wait a byte at a time).
///
/// It was two datagrams a round trip, which is what a path delivers at
/// its best with that window and not at its least. A path whose window is
/// the smallest carries a datagram in about the round trip the reader
/// measures, whose samples are of small packets waiting behind one: 256
/// kilobytes over eight kilobits a second took 262 seconds and were given
/// 225 (the audit's F36, `adverse_paths_measured`).
pub fn residency(bytes: usize, rtt: std::time::Duration) -> std::time::Duration {
    let probes = bytes.div_ceil(LEAST_PROGRESS.saturating_mul(2));
    probe_timeout(rtt).saturating_mul(u32::try_from(probes).unwrap_or(u32::MAX))
}

/// Where a body's bytes stand among what its connection carries: what a
/// body's arrival is charged with (`Arriving`). A connection keeps one for
/// all its readers; a reader without a connection (a test's pipe) keeps its
/// own (`AloneDelivery`).
pub trait Delivery: Send + Sync {
    /// Bytes the connection has received so far, of anything.
    fn received(&self) -> u64;
    /// Stream bytes the connection's readers of `rank`'s class and the less
    /// urgent ones have read so far.
    fn delivered(&self, rank: u8) -> u64;
    /// Payload bytes the peer declared on the connection, of `rank`'s class
    /// and the less urgent ones, that no reader has read yet.
    fn backlog(&self, rank: u8) -> u64;
    /// A body of `bytes` of `rank`'s class was declared: its header read.
    fn declared(&self, rank: u8, bytes: u64);
    /// `bytes` of a declared body of `rank`'s class were read.
    fn read(&self, rank: u8, bytes: u64);
    /// `bytes` of a declared body of `rank`'s class will not be read: the
    /// reader gave it up.
    fn released(&self, rank: u8, bytes: u64);
}
/// The delivery a reader keeps for itself where no connection does: what it
/// read is all it knows was received or delivered, and all it is owed is
/// its own body.
#[derive(Debug, Default)]
pub struct AloneDelivery {
    read: std::sync::atomic::AtomicU64,
    owed: std::sync::atomic::AtomicU64,
}
impl Delivery for AloneDelivery {
    fn received(&self) -> u64 {
        self.read.load(std::sync::atomic::Ordering::Acquire)
    }
    fn delivered(&self, _rank: u8) -> u64 {
        self.read.load(std::sync::atomic::Ordering::Acquire)
    }
    fn backlog(&self, _rank: u8) -> u64 {
        self.owed.load(std::sync::atomic::Ordering::Acquire)
    }
    fn declared(&self, _rank: u8, bytes: u64) {
        self.owed
            .fetch_add(bytes, std::sync::atomic::Ordering::AcqRel);
    }
    fn read(&self, _rank: u8, bytes: u64) {
        self.read
            .fetch_add(bytes, std::sync::atomic::Ordering::AcqRel);
        saturating_sub_atomic(&self.owed, bytes);
    }
    fn released(&self, _rank: u8, bytes: u64) {
        saturating_sub_atomic(&self.owed, bytes);
    }
}
/// The delivery one connection keeps for all its readers: what each class
/// declared and read, beside what the connection received.
#[derive(Debug, Default)]
pub struct Counts {
    delivered: [std::sync::atomic::AtomicU64; crate::TrafficClass::RANKS],
    declared: [std::sync::atomic::AtomicU64; crate::TrafficClass::RANKS],
}
impl Counts {
    fn slot(
        ranked: &[std::sync::atomic::AtomicU64],
        rank: u8,
    ) -> Option<&std::sync::atomic::AtomicU64> {
        ranked.get(usize::from(rank)).or_else(|| ranked.last())
    }
    fn from_rank(ranked: &[std::sync::atomic::AtomicU64], rank: u8) -> u64 {
        ranked
            .iter()
            .skip(usize::from(rank))
            .fold(0u64, |sum, count| {
                sum.saturating_add(count.load(std::sync::atomic::Ordering::Acquire))
            })
    }
    /// What the connection's readers of `rank`'s class and the less urgent
    /// ones have read.
    pub fn delivered(&self, rank: u8) -> u64 {
        Self::from_rank(&self.delivered, rank)
    }
    /// What the peer declared of those classes and no reader has read.
    pub fn backlog(&self, rank: u8) -> u64 {
        Self::from_rank(&self.declared, rank)
    }
    pub fn declared(&self, rank: u8, bytes: u64) {
        if let Some(count) = Self::slot(&self.declared, rank) {
            count.fetch_add(bytes, std::sync::atomic::Ordering::AcqRel);
        }
    }
    pub fn read(&self, rank: u8, bytes: u64) {
        if let Some(count) = Self::slot(&self.delivered, rank) {
            count.fetch_add(bytes, std::sync::atomic::Ordering::AcqRel);
        }
        if let Some(count) = Self::slot(&self.declared, rank) {
            saturating_sub_atomic(count, bytes);
        }
    }
    pub fn released(&self, rank: u8, bytes: u64) {
        if let Some(count) = Self::slot(&self.declared, rank) {
            saturating_sub_atomic(count, bytes);
        }
    }
}
fn saturating_sub_atomic(count: &std::sync::atomic::AtomicU64, bytes: u64) {
    use std::sync::atomic::Ordering;
    let mut before = count.load(Ordering::Acquire);
    while let Err(found) = count.compare_exchange_weak(
        before,
        before.saturating_sub(bytes),
        Ordering::AcqRel,
        Ordering::Acquire,
    ) {
        before = found;
    }
}
/// What a connection had moved when a body was judged.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Moved {
    pub received: u64,
    pub delivered: u64,
}
/// A body's arrival, charged with what arrives: judged once a period, and
/// given up only when a period brought less than a datagram of the
/// connection's bytes (silence), or began with everything the peer owed of
/// the body's class and the less urgent ones delivered and the body still
/// not among it (the peer withholds it while it sends others). A body may
/// come behind what the peer sends first — the more urgent classes under
/// strict priority, and what it declared on other bodies of its class or
/// less urgent — so the wait is charged with the bytes the connection
/// delivered of its class and the less urgent ones, against what the peer
/// declared of them and has still to deliver, this body included: the most
/// that was found to be.
///
/// The wait was the body's residency, its bytes at two datagrams of the
/// least size a probe timeout of the path, and a period at least: the pace
/// of a sender limited by its path alone. A peer whose owner writes a body
/// as it has it, whose exchanges share the connection under strict
/// priority, or that is short of CPU, sends slower than its path on a path
/// whose round trip says nothing of when the body ends: at the 395 µs QUIC
/// measures on loopback an 8 MiB body got one period, and the first
/// judgement after it refused the body however much was arriving
/// (hyper-raft's port of this law, under a CPU quota: 7 of 15 runs at half
/// a core, 25 of 30 at a fifth; the rare refusals focal's CI saw on
/// ubuntu-24.04 and windows-11-arm). What a slow body holds is bounded by
/// the identity's share of the listener's ingress (F03), not by time.
#[derive(Clone, Copy, Debug)]
pub struct Arriving {
    next: tokio::time::Instant,
    before: Moved,
    owed: u64,
    charged: u64,
    had: bool,
    remaining: u64,
}
impl Arriving {
    /// A body of `remaining` bytes begins to arrive at `now`, the peer owing
    /// `backlog` of its class and the less urgent ones, the body included;
    /// first judged a `period` on.
    pub fn begin(
        now: tokio::time::Instant,
        moved: Moved,
        remaining: u64,
        backlog: u64,
        period: std::time::Duration,
    ) -> Self {
        Self {
            next: now.checked_add(period).unwrap_or(now),
            before: moved,
            owed: backlog.max(remaining),
            charged: 0,
            had: false,
            remaining,
        }
    }
    /// `bytes` of the body arrived.
    pub fn arrived(&mut self, bytes: u64) {
        self.remaining = self.remaining.saturating_sub(bytes);
    }
    /// When the body is next judged.
    pub fn due(&self) -> tokio::time::Instant {
        self.next
    }
    /// Judge the body at `now` if a judgement is due: `moved` is what the
    /// connection has moved, `backlog` what the peer owes now of the body's
    /// class and the less urgent ones; the next judgement is a `period` on.
    pub fn judge(
        &mut self,
        now: tokio::time::Instant,
        moved: Moved,
        backlog: u64,
        period: std::time::Duration,
    ) -> Result<(), WireError> {
        if now < self.next {
            return Ok(());
        }
        let before = std::mem::replace(&mut self.before, moved);
        self.next = now.checked_add(period).unwrap_or(now);
        let received = moved.received.saturating_sub(before.received);
        let least = u64::try_from(LEAST_PROGRESS)
            .unwrap_or(u64::MAX)
            .min(self.remaining);
        if self.had || received < least {
            return Err(WireError::Timeout);
        }
        self.charged = self
            .charged
            .saturating_add(moved.delivered.saturating_sub(before.delivered));
        self.owed = self.owed.max(backlog);
        self.had = self.charged >= self.owed;
        Ok(())
    }
}
/// What a body is judged by: the period the peer is given, or the probe
/// timeout of the longest round trip the path has shown while the body
/// arrives, whichever is longer — a lost flight is sent again when the
/// probe timer ends (RFC 9002 §6.2.4), and a path that carries four
/// kilobits a second takes longer than a short period to carry one datagram
/// (the audit's F36, `narrow_lossy_paths_measured`).
pub fn judgement(wait: std::time::Duration, longest: std::time::Duration) -> std::time::Duration {
    wait.max(probe_timeout(longest))
}
/// A body declared to its connection's delivery, read into it as it
/// arrives, and released if given up.
struct Declared<'a> {
    delivery: &'a (dyn Delivery + Sync),
    rank: u8,
    unread: u64,
}
impl Declared<'_> {
    fn read(&mut self, bytes: u64) {
        self.delivery.read(self.rank, bytes);
        self.unread = self.unread.saturating_sub(bytes);
    }
}
impl Drop for Declared<'_> {
    fn drop(&mut self) {
        if self.unread > 0 {
            self.delivery.released(self.rank, self.unread);
        }
    }
}
/// Read the payload of `header` and decode it, however long the path and
/// the peer take to carry it: a body is given up when it stops arriving
/// ([`Arriving`]), and by nothing else. So a megabyte arrives over a path
/// that carries a megabit in a second as over one that carries a thousand,
/// from a peer that writes it as fast as its path carries as from one that
/// writes it slower; and a peer that stops is given up within a judgement.
///
/// `round_trip` is the path's round trip as the connection measures it,
/// asked as the payload arrives; a judgement is a `wait` or the probe
/// timeout of the longest round trip seen ([`judgement`]). `delivery` is
/// the connection's, and `rank` the body's class among what the connection
/// carries ([`crate::TrafficClass::rank`]; a body whose class is not known
/// yet, a request's, is the most urgent).
pub async fn read_payload_arriving<R: AsyncRead + Unpin, T: DeserializeOwned>(
    reader: &mut R,
    header: FrameHeader,
    wait: std::time::Duration,
    round_trip: impl Fn() -> std::time::Duration,
    delivery: &(dyn Delivery + Sync),
    rank: u8,
) -> Result<T, WireError> {
    let mut bytes = payload_buffer(header.payload_bytes())?;
    let length = u64::try_from(bytes.len()).map_err(|_| WireError::Limit)?;
    delivery.declared(rank, length);
    let mut declared = Declared {
        delivery,
        rank,
        unread: length,
    };
    let moved = || Moved {
        received: delivery.received(),
        delivered: delivery.delivered(rank),
    };
    let mut longest = round_trip();
    let mut arriving = Arriving::begin(
        tokio::time::Instant::now(),
        moved(),
        length,
        delivery.backlog(rank),
        judgement(wait, longest),
    );
    let mut filled = 0_usize;
    while filled < bytes.len() {
        longest = longest.max(round_trip());
        arriving.judge(
            tokio::time::Instant::now(),
            moved(),
            delivery.backlog(rank),
            judgement(wait, longest),
        )?;
        let rest = bytes.get_mut(filled..).ok_or(WireError::Limit)?;
        if let Ok(read) = tokio::time::timeout_at(arriving.due(), reader.read(rest)).await {
            let read = read?;
            if read == 0 {
                return Err(WireError::InvalidFrame);
            }
            filled = filled.saturating_add(read);
            let read = u64::try_from(read).map_err(|_| WireError::Limit)?;
            declared.read(read);
            arriving.arrived(read);
        }
    }
    decode_payload(&bytes)
}

/// Read a raw framed payload into an already-owned buffer. Framing does not
/// deserialize, authenticate, verify content or require the enclosing stream to
/// end; callers still perform those checks for their negotiated protocol.
pub async fn read_frame_into<'a, R: AsyncRead + Unpin>(
    reader: &mut R,
    kind: FrameKind,
    limit: u32,
    buffer: &'a mut [u8],
) -> Result<&'a [u8], WireError> {
    let header = read_frame_header(reader, kind, limit).await?;
    read_frame_payload_into(reader, header, buffer).await
}
pub async fn require_end<R: AsyncRead + Unpin>(reader: &mut R) -> Result<(), WireError> {
    let mut byte = [0u8; 1];
    if reader.read(&mut byte).await? != 0 {
        return Err(WireError::InvalidFrame);
    }
    Ok(())
}
