//! Same-user local transport: identical frames and ingress verification, with
//! the operating system's peer credentials replacing TLS authentication. A
//! Unix-domain socket on Unix ([unix]) and a named pipe on Windows
//! ([windows]); the frame exchange ([serve_stream], [request_stream]) is
//! shared and generic over the byte stream.
use crate::*;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

#[cfg(unix)]
mod unix;
#[cfg(unix)]
pub use unix::{LocalRemote, LocalServer};

#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub use windows::{LocalRemote, LocalServer};

/// Serve one accepted local connection: negotiate, read one request, dispatch
/// under the peer's grant, write the response. Generic over the byte stream so
/// a Unix socket and a Windows pipe share it.
pub(crate) async fn serve_stream<S, H>(
    mut stream: S,
    grant: PeerGrant,
    limits: WireLimits,
    handler: H,
) -> Result<(), WireError>
where
    S: AsyncRead + AsyncWrite + Unpin,
    H: RequestHandler,
{
    let hello: Hello = read_frame(&mut stream, FrameKind::Hello, 4096).await?;
    let negotiated = match limits.negotiate_native(
        &hello,
        handler.supports_managed_requests(),
        handler.supports_participant_requests(),
        handler.supports_native_requests(),
    ) {
        Ok(value) => value,
        Err(error) => {
            write_frame(
                &mut stream,
                FrameKind::HelloReply,
                &HelloReply::Rejected(error),
                4096,
            )
            .await?;
            stream.shutdown().await?;
            return Ok(());
        }
    };
    write_frame(
        &mut stream,
        FrameKind::HelloReply,
        &HelloReply::Accepted(negotiated),
        4096,
    )
    .await?;
    let request: RequestEnvelope =
        read_frame(&mut stream, FrameKind::Request, negotiated.max_frame_bytes).await?;
    require_end(&mut stream).await?;
    let mut limits = limits;
    limits.max_frame_bytes = negotiated.max_frame_bytes;
    limits.max_items = negotiated.max_items;
    let response = if negotiated.accepts_protocol(request.protocol) {
        dispatch_accounted(&handler, AuthenticatedPeer::local(grant)?, request, &limits).await
    } else {
        OwnedResponse::new(request.reply(Response::Error(AccessError::UnsupportedProtocol)))
    };
    write_frame(
        &mut stream,
        FrameKind::Response,
        response.envelope(),
        negotiated.max_frame_bytes,
    )
    .await?;
    stream.shutdown().await?;
    Ok(())
}

/// Send one request over an established local stream and read its response.
pub(crate) async fn request_stream<S>(
    mut stream: S,
    request: &RequestEnvelope,
    limits: &WireLimits,
) -> Result<ResponseEnvelope, WireError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let hello = Hello {
        versions: vec![request.protocol],
        max_frame_bytes: limits.max_frame_bytes,
        max_items: limits.max_items,
    };
    write_frame(&mut stream, FrameKind::Hello, &hello, 4096).await?;
    let reply: HelloReply = read_frame(&mut stream, FrameKind::HelloReply, 4096).await?;
    let negotiated = match reply {
        HelloReply::Accepted(value) => value,
        HelloReply::Rejected(error) => return Err(error.into()),
    };
    if !matches!(
        negotiated.protocol,
        PROTOCOL_VERSION
            | MANAGED_PROTOCOL_VERSION
            | PEER_PROTOCOL_VERSION
            | crate::NATIVE_PROTOCOL_VERSION
    ) || !negotiated.accepts_protocol(request.protocol)
        || negotiated.max_frame_bytes > limits.max_frame_bytes
        || negotiated.max_items > limits.max_items
    {
        return Err(WireError::InvalidFrame);
    }
    write_frame(
        &mut stream,
        FrameKind::Request,
        request,
        negotiated.max_frame_bytes,
    )
    .await?;
    stream.shutdown().await?;
    let response = read_frame(&mut stream, FrameKind::Response, negotiated.max_frame_bytes).await?;
    require_end(&mut stream).await?;
    validate_response(request, &response, None, limits)?;
    Ok(response)
}
