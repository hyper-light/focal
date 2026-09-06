use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const MAGIC: &[u8; 8] = b"FOCALQ01";
pub const HEADER_BYTES: usize = 16;
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

pub fn encode_payload<T: Serialize>(value: &T, limit: u32) -> Result<Vec<u8>, WireError> {
    if limit > 16 * 1024 * 1024 {
        return Err(WireError::Limit);
    }
    let capacity =
        postcard::experimental::serialized_size(value).map_err(|_| WireError::InvalidFrame)?;
    if capacity > limit as usize {
        return Err(WireError::Limit);
    }
    let mut bytes = vec![0; capacity];
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
    let mut header = [0u8; HEADER_BYTES];
    reader.read_exact(&mut header).await?;
    if &header[..8] != MAGIC
        || header[8..10] != 1u16.to_be_bytes()
        || header[10..12] != (kind as u16).to_be_bytes()
    {
        return Err(WireError::InvalidFrame);
    }
    let len = u32::from_be_bytes(
        header[12..16]
            .try_into()
            .map_err(|_| WireError::InvalidFrame)?,
    );
    if len > limit || len > 16 * 1024 * 1024 {
        return Err(WireError::Limit);
    }
    let mut bytes = vec![0; len as usize];
    reader.read_exact(&mut bytes).await?;
    decode_payload(&bytes)
}
pub async fn require_end<R: AsyncRead + Unpin>(reader: &mut R) -> Result<(), WireError> {
    let mut byte = [0u8; 1];
    if reader.read(&mut byte).await? != 0 {
        return Err(WireError::InvalidFrame);
    }
    Ok(())
}
