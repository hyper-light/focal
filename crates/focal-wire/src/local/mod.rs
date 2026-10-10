//! Same-user local transport: identical frames and ingress verification, with
//! the operating system's peer credentials replacing TLS authentication. A
//! local stream socket on every OS (`AF_UNIX` on Linux, macOS and Windows,
//! through hyper-rt's `local` sockets, 27 §15): the socket file lives in an
//! owner-only directory, so a peer of another user cannot connect at all, and
//! the kernel names who did connect, so one that could is refused.
use crate::*;
use hyper_rt::combine::{Either3, race3};
use hyper_rt::local::{LocalListener, LocalStream, current_user};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};

/// The grant a local server serves under, which its owner may change while it
/// serves (the node's local grant follows the committed registry, 24 §16).
/// Clones share it: the controller that publishes it and the server that reads
/// it at each connection must see one value, as the peer registry's clones see
/// one grant table, so this Arc is intentional.
#[derive(Clone)]
pub struct LocalGrant(Arc<RwLock<PeerGrant>>);
impl LocalGrant {
    pub fn new(grant: PeerGrant) -> Self {
        Self(Arc::new(RwLock::new(grant)))
    }
    /// The grant now.
    pub fn get(&self) -> Result<PeerGrant, WireError> {
        self.0
            .read()
            .map(|grant| grant.clone())
            .map_err(|_| WireError::Connection)
    }
    /// Changes the grant with `change`, which says whether it changed anything.
    pub fn update(&self, change: impl FnOnce(&mut PeerGrant) -> bool) -> bool {
        self.0
            .write()
            .map(|mut grant| change(&mut grant))
            .unwrap_or(false)
    }
}

/// The connections a local listener's kernel queue holds before it accepts them: its limits'
/// connections, so a burst of the callers it serves at once waits in the queue rather than being
/// refused by the kernel.
fn backlog(limits: &WireLimits) -> u32 {
    u32::try_from(limits.max_connections).unwrap_or(u32::MAX)
}

pub struct LocalServer {
    listener: LocalListener,
    path: PathBuf,
    /// The socket file this server bound, so a drop removes it only while it is still this one.
    identity: Option<focal_platform::fs::FileId>,
    grant: LocalGrant,
    limits: WireLimits,
    closed: hyper_rt::sync::Notify,
}
impl LocalServer {
    pub fn bind(
        path: impl AsRef<Path>,
        grant: PeerGrant,
        limits: WireLimits,
    ) -> Result<Self, WireError> {
        Self::bind_watched(path, LocalGrant::new(grant), limits)
    }
    pub fn bind_watched(
        path: impl AsRef<Path>,
        grant: LocalGrant,
        limits: WireLimits,
    ) -> Result<Self, WireError> {
        limits.validate()?;
        require_runtime()?;
        AuthenticatedPeer::local(grant.get()?)?;
        let path = path.as_ref().to_path_buf();
        let parent = path.parent().ok_or(WireError::Authentication)?;
        if !focal_platform::fs::dir_not_writable_by_others(parent)? {
            return Err(WireError::Authentication);
        }
        let listener =
            LocalListener::bind(&path, backlog(&limits)).map_err(|_| WireError::Connection)?;
        let identity = focal_platform::fs::path_identity(&path).ok();
        // The socket is the owner's alone: no one else's process opens it (the directory already
        // refuses them; this is the second wall on Unix).
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
        }
        if focal_platform::fs::owner_at(&path)? != focal_platform::fs::current_owner()? {
            return Err(WireError::Authentication);
        }
        Ok(Self {
            listener,
            path,
            identity,
            grant,
            limits,
            closed: hyper_rt::sync::Notify::new().map_err(|_| WireError::Connection)?,
        })
    }
    pub fn path(&self) -> &Path {
        &self.path
    }
    pub fn close(&self) {
        self.closed.notify_one();
    }
    pub async fn serve<H: RequestHandler + Clone>(&self, handler: H) -> Result<(), WireError> {
        require_runtime()?;
        let user = current_user().map_err(|_| WireError::Authentication)?;
        let mut tasks = hyper_rt::combine::TaskSet::new(self.limits.max_connections)
            .map_err(|_| WireError::Limit)?;
        loop {
            let reaped = async {
                if tasks.is_empty() {
                    std::future::pending::<()>().await;
                }
                let _ = tasks.join_next().await;
            };
            match race3(self.closed.notified(), self.listener.accept(), reaped).await {
                Either3::First(_) => break,
                Either3::Second(Ok(stream)) => {
                    // The kernel names who connected; another user's process is refused.
                    if stream.peer().map(|peer| peer.user).ok() != Some(user.clone()) {
                        continue;
                    }
                    let grant = self.grant.get()?;
                    let limits = self.limits.clone();
                    let handler = handler.clone();
                    // A connection past the bound is dropped: its caller is told by the close.
                    let _ = tasks.spawn(async move {
                        let period =
                            u64::try_from(limits.request_timeout.as_nanos()).unwrap_or(u64::MAX);
                        let _ = transport_exchange(async {
                            hyper_rt::futures::within(
                                period,
                                serve_stream(stream, grant, limits, handler),
                            )
                            .await
                            .map_err(|_| WireError::Connection)?
                            .ok_or(WireError::Timeout)?
                        })
                        .await;
                    });
                }
                Either3::Second(Err(_)) => return Err(WireError::Connection),
                Either3::Third(()) => {}
            }
        }
        tasks.cancel_all();
        while tasks.join_next().await.is_some() {}
        Ok(())
    }
}
impl Drop for LocalServer {
    fn drop(&mut self) {
        // Only the socket this server bound: a later server's at the same path stays.
        if self.identity.is_some()
            && focal_platform::fs::path_identity(&self.path).ok() == self.identity
        {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

#[derive(Clone)]
pub struct LocalRemote {
    path: PathBuf,
    limits: WireLimits,
}
impl LocalRemote {
    pub fn new(path: impl AsRef<Path>, limits: WireLimits) -> Result<Self, WireError> {
        limits.validate()?;
        Ok(Self {
            path: path.as_ref().to_path_buf(),
            limits,
        })
    }
    pub async fn request(&self, request: &RequestEnvelope) -> Result<ResponseEnvelope, WireError> {
        let period = u64::try_from(self.limits.request_timeout.as_nanos()).unwrap_or(u64::MAX);
        transport_exchange(async {
            hyper_rt::futures::within(period, self.request_inner(request))
                .await
                .map_err(|_| WireError::Connection)?
                .ok_or(WireError::Timeout)?
        })
        .await
    }
    async fn request_inner(
        &self,
        request: &RequestEnvelope,
    ) -> Result<ResponseEnvelope, WireError> {
        // The socket is the current user's, and so is the process behind it.
        let user = focal_platform::fs::current_owner()?;
        if focal_platform::fs::owner_at(&self.path)? != user {
            return Err(WireError::Authentication);
        }
        let stream = LocalStream::connect(&self.path)
            .await
            .map_err(|_| WireError::Connection)?;
        let peer = stream.peer().map_err(|_| WireError::Authentication)?;
        if Some(peer.user) != current_user().ok() {
            return Err(WireError::Authentication);
        }
        request_stream(stream, request, &self.limits).await
    }
}

/// Serve one accepted local connection: negotiate, read one request, dispatch
/// under the peer's grant, write the response.
pub(crate) async fn serve_stream<S, H>(
    mut stream: S,
    grant: PeerGrant,
    limits: WireLimits,
    handler: H,
) -> Result<(), WireError>
where
    S: FrameStream,
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
    // One length-delimited request per connection: the exchange never depends
    // on a write-half end. The kernel has already authenticated a same-user
    // peer; trailing bytes are ignored and the one-shot connection is closed
    // after the reply.
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
    .await
}

/// Send one request over an established local stream and read its response.
pub(crate) async fn request_stream<S>(
    mut stream: S,
    request: &RequestEnvelope,
    limits: &WireLimits,
) -> Result<ResponseEnvelope, WireError>
where
    S: FrameStream,
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
    let response = read_frame(&mut stream, FrameKind::Response, negotiated.max_frame_bytes).await?;
    validate_response(request, &response, None, limits)?;
    Ok(response)
}
