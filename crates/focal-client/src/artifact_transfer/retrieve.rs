use super::{TRANSFER_CHUNK_BYTES, TransferError, buffer};
use crate::{ContentChunk, Operation, RequestEnvelope};
use focal_model::{ArtifactPayload, ContentRef};
use std::io::Write;

/// A bounded cursor over one immutable payload reference. A content root names
/// the authenticated server's verified manifest, not a raw-byte stream digest.
pub struct PayloadDownload {
    content: ContentRef,
    offset: u64,
    chunk_bytes: u32,
    complete: bool,
}
impl PayloadDownload {
    pub fn new(
        content: ContentRef,
        max_total: u64,
        chunk_bytes: u32,
    ) -> Result<Self, TransferError> {
        if content.domain.is_zero() || content.root.0 == [0; 32] {
            return Err(TransferError::Invalid);
        }
        if content.length > max_total
            || chunk_bytes == 0
            || chunk_bytes as usize > TRANSFER_CHUNK_BYTES
        {
            return Err(TransferError::Capacity);
        }
        Ok(Self {
            content,
            offset: 0,
            chunk_bytes,
            complete: false,
        })
    }
    pub fn operation(&self) -> Option<Operation> {
        (!self.complete).then(|| Operation::Download {
            content: self.content.clone(),
            offset: self.offset,
            max_bytes: self.chunk_bytes,
        })
    }
    pub fn offset(&self) -> u64 {
        self.offset
    }
    pub fn complete(&self) -> bool {
        self.complete
    }
    /// Validate the complete reply before the caller publishes any of its bytes.
    /// A failed sink write can have emitted a partial chunk. The cursor remains
    /// unchanged; the caller must discard or rewind that sink before retrying.
    pub fn accept(
        &mut self,
        chunk: &ContentChunk,
        output: &mut impl Write,
    ) -> Result<(), TransferError> {
        let end = self
            .offset
            .checked_add(chunk.bytes.len() as u64)
            .ok_or(TransferError::Capacity)?;
        if self.complete
            || chunk.offset != self.offset
            || chunk.bytes.len() > self.chunk_bytes as usize
            || end > self.content.length
            || chunk.eof != (end == self.content.length)
            || (chunk.bytes.is_empty() && !chunk.eof)
        {
            return Err(TransferError::Invalid);
        }
        output.write_all(&chunk.bytes)?;
        self.offset = end;
        self.complete = chunk.eof;
        Ok(())
    }
}

/// Retrieve a complete payload on an existing synchronous owner. The fetch
/// callback may enter the caller's runtime; file writes stay outside it. Total
/// bytes and every chunk are bounded, including inline payloads. The caller owns
/// atomic output publication and any whole-operation deadline/cancellation.
pub fn retrieve_payload(
    payload: &ArtifactPayload,
    max_total: u64,
    output: &mut impl Write,
    mut envelope: impl FnMut(Operation) -> Result<RequestEnvelope, TransferError>,
    mut fetch: impl FnMut(RequestEnvelope) -> Result<ContentChunk, TransferError>,
) -> Result<(), TransferError> {
    match payload {
        ArtifactPayload::Inline(bytes) => {
            if bytes.len() as u64 > max_total {
                return Err(TransferError::Capacity);
            }
            output.write_all(bytes)?;
        }
        ArtifactPayload::Content(reference) => {
            let mut download =
                PayloadDownload::new(reference.clone(), max_total, TRANSFER_CHUNK_BYTES as u32)?;
            while let Some(operation) = download.operation() {
                let request = envelope(operation)?;
                let chunk = fetch(request)?;
                download.accept(&chunk, output)?;
            }
        }
    }
    Ok(())
}

impl<T: crate::ClientTransport> crate::Client<T> {
    /// Complete bounded in-memory retrieval for adapters whose output itself is
    /// bounded. Large file callers use `PayloadDownload` and a streaming sink.
    /// No comparison is made between raw bytes and the manifest-root hash.
    pub async fn payload_bytes(
        &self,
        request: RequestEnvelope,
        payload: &ArtifactPayload,
        max_bytes: u32,
    ) -> Result<Vec<u8>, TransferError> {
        use std::{
            future::{Future, poll_fn},
            panic::AssertUnwindSafe,
            pin::pin,
            task::Poll,
            time::Duration,
        };
        let future = async {
            tokio::time::timeout(
                self.retry_timeout().min(Duration::from_secs(30)),
                self.payload_bytes_inner(request, payload, max_bytes),
            )
            .await
            .map_err(|_| TransferError::Client(crate::ClientError::Transport))?
        };
        let mut future = pin!(future);
        poll_fn(|cx| {
            match std::panic::catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
                Ok(result) => result,
                Err(_) => Poll::Ready(Err(TransferError::Client(crate::ClientError::Transport))),
            }
        })
        .await
    }
    async fn payload_bytes_inner(
        &self,
        mut request: RequestEnvelope,
        payload: &ArtifactPayload,
        max_bytes: u32,
    ) -> Result<Vec<u8>, TransferError> {
        let cap = max_bytes.min(self.wire_limits().max_frame_bytes);
        let length = match payload {
            ArtifactPayload::Inline(bytes) => bytes.len() as u64,
            ArtifactPayload::Content(reference) => reference.length,
        };
        if length > u64::from(cap) {
            return Err(TransferError::Capacity);
        }
        let mut output = buffer(usize::try_from(length).map_err(|_| TransferError::Capacity)?)?;
        output.clear();
        match payload {
            ArtifactPayload::Inline(bytes) => output.extend_from_slice(bytes),
            ArtifactPayload::Content(reference) => {
                if reference.domain.0 != request.ledger.tenant.0 {
                    return Err(TransferError::Invalid);
                }
                let chunk = self
                    .wire_limits()
                    .max_frame_bytes
                    .saturating_sub(256)
                    .min(TRANSFER_CHUNK_BYTES as u32);
                let mut download = PayloadDownload::new(reference.clone(), u64::from(cap), chunk)?;
                while let Some(operation) = download.operation() {
                    request.operation = operation;
                    let reply = self.download(request.clone()).await?;
                    download.accept(&reply, &mut output)?;
                }
            }
        }
        Ok(output)
    }
}
