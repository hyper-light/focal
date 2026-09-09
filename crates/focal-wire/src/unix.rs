//! Same-user local transport: identical frames and ingress verification, with
//! kernel peer credentials replacing TLS certificate authentication.
use crate::*;
use std::{
    os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
};
use tokio::{
    io::AsyncWriteExt,
    net::{UnixListener, UnixStream},
    sync::watch,
    task::JoinSet,
};

pub struct UnixServer {
    listener: UnixListener,
    path: PathBuf,
    inode: u64,
    uid: u32,
    grant: PeerGrant,
    limits: WireLimits,
    shutdown: watch::Sender<bool>,
}
impl UnixServer {
    pub fn bind(
        path: impl AsRef<Path>,
        grant: PeerGrant,
        limits: WireLimits,
    ) -> Result<Self, WireError> {
        limits.validate()?;
        require_runtime()?;
        AuthenticatedPeer::local(grant.clone())?;
        let path = path.as_ref().to_path_buf();
        let parent = path.parent().ok_or(WireError::Authentication)?;
        let parent_metadata = std::fs::symlink_metadata(parent)?;
        if !parent_metadata.is_dir() || parent_metadata.permissions().mode() & 0o022 != 0 {
            return Err(WireError::Authentication);
        }
        let (listener, metadata) = transport_setup(|| {
            let listener = std::os::unix::net::UnixListener::bind(&path)?;
            let metadata = std::fs::symlink_metadata(&path)?;
            let mut guard = BoundPath {
                path: &path,
                inode: metadata.ino(),
                armed: true,
            };
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
            if metadata.uid() != parent_metadata.uid() {
                return Err(WireError::Authentication);
            }
            listener.set_nonblocking(true)?;
            let listener = UnixListener::from_std(listener)?;
            guard.armed = false;
            Ok((listener, metadata))
        })?;
        let (shutdown, _) = watch::channel(false);
        Ok(Self {
            listener,
            path,
            inode: metadata.ino(),
            uid: metadata.uid(),
            grant,
            limits,
            shutdown,
        })
    }
    pub fn path(&self) -> &Path {
        &self.path
    }
    pub fn close(&self) {
        self.shutdown.send_replace(true);
    }
    pub async fn serve<H: RequestHandler + Clone>(&self, handler: H) -> Result<(), WireError> {
        require_runtime()?;
        let mut shutdown = self.shutdown.subscribe();
        let mut tasks = JoinSet::new();
        loop {
            if *shutdown.borrow() {
                break;
            }
            tokio::select! {
                biased;
                _=shutdown.changed()=>break,
                Some(_)=tasks.join_next(),if !tasks.is_empty()=>{},
                accepted=self.listener.accept()=>{
                    let (stream,_)=accepted?;
                    if tasks.len() >= self.limits.max_connections {drop(stream);continue;}
                    if stream.peer_cred()?.uid()!=self.uid {drop(stream);continue;}
                    let grant=self.grant.clone();let limits=self.limits.clone();let handler=handler.clone();
                    tasks.spawn(async move {
                        let _ = transport_exchange(async {
                            tokio::time::timeout(
                                limits.request_timeout, serve_unix(stream, grant, limits, handler),
                            ).await.map_err(|_| WireError::Timeout)?
                        }).await;
                    });
                }
            }
        }
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        Ok(())
    }
}
/// A failed IO-driver registration can unwind after the OS created its socket.
/// Remove only that inode; another process's replacement is never unlinked.
struct BoundPath<'a> {
    path: &'a Path,
    inode: u64,
    armed: bool,
}
impl Drop for BoundPath<'_> {
    fn drop(&mut self) {
        if self.armed
            && std::fs::symlink_metadata(self.path).is_ok_and(|metadata| {
                metadata.ino() == self.inode && metadata.file_type().is_socket()
            })
        {
            let _ = std::fs::remove_file(self.path);
        }
    }
}
impl Drop for UnixServer {
    fn drop(&mut self) {
        if std::fs::symlink_metadata(&self.path)
            .is_ok_and(|m| m.ino() == self.inode && m.file_type().is_socket())
        {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}
async fn serve_unix<H: RequestHandler>(
    mut stream: UnixStream,
    grant: PeerGrant,
    limits: WireLimits,
    handler: H,
) -> Result<(), WireError> {
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

#[derive(Clone)]
pub struct UnixRemote {
    path: PathBuf,
    limits: WireLimits,
}
impl UnixRemote {
    pub fn new(path: impl AsRef<Path>, limits: WireLimits) -> Result<Self, WireError> {
        limits.validate()?;
        Ok(Self {
            path: path.as_ref().to_path_buf(),
            limits,
        })
    }
    pub async fn request(&self, request: &RequestEnvelope) -> Result<ResponseEnvelope, WireError> {
        transport_exchange(async {
            tokio::time::timeout(self.limits.request_timeout, self.request_inner(request))
                .await
                .map_err(|_| WireError::Timeout)?
        })
        .await
    }
    async fn request_inner(
        &self,
        request: &RequestEnvelope,
    ) -> Result<ResponseEnvelope, WireError> {
        let metadata = std::fs::symlink_metadata(&self.path)?;
        if !metadata.file_type().is_socket() || metadata.permissions().mode() & 0o077 != 0 {
            return Err(WireError::Authentication);
        }
        let mut stream = UnixStream::connect(&self.path).await?;
        if stream.peer_cred()?.uid() != metadata.uid() {
            return Err(WireError::Authentication);
        }
        let hello = Hello {
            versions: vec![request.protocol],
            max_frame_bytes: self.limits.max_frame_bytes,
            max_items: self.limits.max_items,
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
            || negotiated.max_frame_bytes > self.limits.max_frame_bytes
            || negotiated.max_items > self.limits.max_items
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
        let response =
            read_frame(&mut stream, FrameKind::Response, negotiated.max_frame_bytes).await?;
        require_end(&mut stream).await?;
        validate_response(request, &response, None, &self.limits)?;
        Ok(response)
    }
}
