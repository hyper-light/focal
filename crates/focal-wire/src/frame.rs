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

pub fn encode_payload<T: Serialize>(value: &T, limit: u32) -> Result<Vec<u8>, WireError> {
    if u64::from(limit) > MAX_FRAME_BYTES as u64 {
        return Err(WireError::Limit);
    }
    let capacity =
        postcard::experimental::serialized_size(value).map_err(|_| WireError::InvalidFrame)?;
    if capacity > limit as usize {
        return Err(WireError::Limit);
    }
    let mut bytes = payload_buffer(capacity)?;
    let size = postcard::to_slice(value, &mut bytes)
        .map_err(|error| match error {
            postcard::Error::SerializeBufferFull => WireError::Limit,
            _ => WireError::InvalidFrame,
        })?
        .len();
    bytes.truncate(size);
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
