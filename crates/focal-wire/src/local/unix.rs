//! The Unix-domain-socket local transport. Kernel peer credentials
//! (`SO_PEERCRED`) authenticate the same-user peer; the socket file is
//! owner-only and created under an owner-only parent directory.
use super::{request_stream, serve_stream};
use crate::*;
use std::{
    os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
};
use tokio::{net::UnixListener, sync::watch, task::JoinSet};

pub struct LocalServer {
    listener: UnixListener,
    path: PathBuf,
    inode: u64,
    uid: u32,
    grant: watch::Receiver<PeerGrant>,
    limits: WireLimits,
    shutdown: watch::Sender<bool>,
}
impl LocalServer {
    pub fn bind(
        path: impl AsRef<Path>,
        grant: PeerGrant,
        limits: WireLimits,
    ) -> Result<Self, WireError> {
        let (_fixed, grant) = watch::channel(grant);
        Self::bind_watched(path, grant, limits)
    }
    pub fn bind_watched(
        path: impl AsRef<Path>,
        grant: watch::Receiver<PeerGrant>,
        limits: WireLimits,
    ) -> Result<Self, WireError> {
        limits.validate()?;
        require_runtime()?;
        AuthenticatedPeer::local(grant.borrow().clone())?;
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
                    let grant=self.grant.borrow().clone();let limits=self.limits.clone();let handler=handler.clone();
                    tasks.spawn(async move {
                        let _ = transport_exchange(async {
                            tokio::time::timeout(
                                limits.request_timeout, serve_stream(stream, grant, limits, handler),
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
impl Drop for LocalServer {
    fn drop(&mut self) {
        if std::fs::symlink_metadata(&self.path)
            .is_ok_and(|m| m.ino() == self.inode && m.file_type().is_socket())
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
        let stream = tokio::net::UnixStream::connect(&self.path).await?;
        if stream.peer_cred()?.uid() != metadata.uid() {
            return Err(WireError::Authentication);
        }
        request_stream(stream, request, &self.limits).await
    }
}
