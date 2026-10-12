//! focal's framing for byte streams — the local socket's protocol (`local`) — and the payload
//! codec every protocol of focal-wire shares: one postcard payload under a fixed header that names
//! its kind and its length, checked before any byte past it is read. QUIC's exchanges carry their
//! payloads in hyper-transport's own frames (`net`); what is here for them is the codec and the
//! path's timing figures.
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;

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
    /// The server a pinned dial reached presented a chain its TLS verified, under a leaf the
    /// dial does not pin ([`crate::net::LeafPins`]): nothing was sent to it.
    #[error("the server's certificate is not a pinned one")]
    Unpinned,
    #[error("connection unavailable")]
    Connection,
    #[error("protocol rejected: {0}")]
    Access(#[from] crate::AccessError),
    /// A local socket's path longer than this system's socket address holds
    /// ([`crate::SOCKET_PATH_BYTES`]): nothing can bind or reach a socket there.
    #[error(
        "the local socket path {} is {} bytes, more than the {most} a local socket's address holds on this system; choose a shorter data directory",
        .path.display(),
        .path.as_os_str().len()
    )]
    SocketPath {
        path: std::path::PathBuf,
        most: usize,
    },
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
/// A byte stream frames are read from and written to: a local socket
/// (`hyper_rt::local::LocalStream`), or a test's own.
pub trait FrameStream {
    /// Reads what has arrived into `buf`: its length, zero at the stream's end.
    fn read(&mut self, buf: &mut [u8]) -> impl Future<Output = Result<usize, WireError>>;
    /// Writes all of `buf`.
    fn write_all(&mut self, buf: &[u8]) -> impl Future<Output = Result<(), WireError>>;
}
impl FrameStream for hyper_rt::local::LocalStream {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, WireError> {
        hyper_rt::local::LocalStream::read(self, buf)
            .await
            .map_err(|_| WireError::Connection)
    }
    async fn write_all(&mut self, buf: &[u8]) -> Result<(), WireError> {
        hyper_rt::local::LocalStream::write_all(self, buf)
            .await
            .map_err(|_| WireError::Connection)
    }
}
/// Fills `buf` from `reader`: a stream that ends first is an invalid frame.
async fn read_exact<R: FrameStream>(reader: &mut R, buf: &mut [u8]) -> Result<(), WireError> {
    let mut filled = 0;
    while filled < buf.len() {
        let rest = buf.get_mut(filled..).ok_or(WireError::InvalidFrame)?;
        let read = reader.read(rest).await?;
        if read == 0 {
            return Err(WireError::InvalidFrame);
        }
        filled = filled.saturating_add(read);
    }
    Ok(())
}
pub async fn write_frame<W: FrameStream, T: Serialize>(
    writer: &mut W,
    kind: FrameKind,
    value: &T,
    limit: u32,
) -> Result<(), WireError> {
    let bytes = encode_payload(value, limit)?;
    let length = u32::try_from(bytes.len()).map_err(|_| WireError::Limit)?;
    let mut header = [0u8; HEADER_BYTES];
    for (at, byte) in MAGIC
        .iter()
        .chain(1u16.to_be_bytes().iter())
        .chain((kind as u16).to_be_bytes().iter())
        .chain(length.to_be_bytes().iter())
        .enumerate()
    {
        if let Some(slot) = header.get_mut(at) {
            *slot = *byte;
        }
    }
    writer.write_all(&header).await?;
    writer.write_all(&bytes).await?;
    Ok(())
}
pub async fn read_frame<R: FrameStream, T: DeserializeOwned>(
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
pub async fn read_frame_header<R: FrameStream>(
    reader: &mut R,
    kind: FrameKind,
    limit: u32,
) -> Result<FrameHeader, WireError> {
    let mut header = [0u8; HEADER_BYTES];
    read_exact(reader, &mut header).await?;
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
pub async fn read_frame_payload_into<'a, R: FrameStream>(
    reader: &mut R,
    header: FrameHeader,
    buffer: &'a mut [u8],
) -> Result<&'a [u8], WireError> {
    let payload = buffer
        .get_mut(..header.payload_bytes())
        .ok_or(WireError::Limit)?;
    read_exact(reader, payload).await?;
    Ok(payload)
}

/// Read a raw framed payload into an already-owned buffer. Framing does not
/// deserialize, authenticate, verify content or require the enclosing stream to
/// end; callers still perform those checks for their negotiated protocol.
pub async fn read_frame_into<'a, R: FrameStream>(
    reader: &mut R,
    kind: FrameKind,
    limit: u32,
    buffer: &'a mut [u8],
) -> Result<&'a [u8], WireError> {
    let header = read_frame_header(reader, kind, limit).await?;
    read_frame_payload_into(reader, header, buffer).await
}
/// The stream ends here: a byte more is an invalid frame.
pub async fn require_end<R: FrameStream>(reader: &mut R) -> Result<(), WireError> {
    let mut byte = [0u8; 1];
    if reader.read(&mut byte).await? != 0 {
        return Err(WireError::InvalidFrame);
    }
    Ok(())
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
