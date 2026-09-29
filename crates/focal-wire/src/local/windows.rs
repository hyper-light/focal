//! The Windows named-pipe local transport. The operating system's pipe peer
//! identity (`GetNamedPipeClientProcessId`/`ServerProcessId` → token SID)
//! authenticates the same-user peer; the pipe is created owner-only and
//! local-only ([focal_platform::create_pipe_server]). The pipe's random name
//! is published in an owner-only file at the bind path so the client can find
//! it — the same rendezvous a Unix socket path provides.
use super::{request_stream, serve_stream};
use crate::*;
use focal_platform::{
    NamedPipeServer, create_pipe_server, pipe_client_is_current_owner, pipe_server_is_current_owner,
};
use std::{
    ffi::{OsStr, OsString},
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{net::windows::named_pipe::ClientOptions, sync::watch, task::JoinSet};

/// `ERROR_PIPE_BUSY`: every instance of the pipe is serving a client; the
/// connect is retried until an instance frees up or the deadline passes.
const ERROR_PIPE_BUSY: i32 = 231;

/// Per-process nonce so two binds of the same path get distinct pipe names.
static NONCE: AtomicU64 = AtomicU64::new(0);

/// A local-only pipe name: a stable component derived from the bind path and a
/// unique component so a squatter cannot predict it. The stable component only
/// aids diagnosis; `first_pipe_instance` and the owner-only DACL are what
/// fence a squatter.
fn pipe_name(path: &Path) -> OsString {
    let mut stable = blake3::Hasher::new();
    stable.update(path.as_os_str().to_string_lossy().as_bytes());
    let stable = stable.finalize();
    let stable = stable.to_hex();
    let stable = stable.get(..16).unwrap_or("");
    let mut unique = blake3::Hasher::new();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or(0);
    unique.update(&nanos.to_le_bytes());
    unique.update(&u64::from(std::process::id()).to_le_bytes());
    unique.update(&NONCE.fetch_add(1, Ordering::Relaxed).to_le_bytes());
    unique.update(path.as_os_str().to_string_lossy().as_bytes());
    let unique = unique.finalize();
    let unique = unique.to_hex();
    let unique = unique.get(..16).unwrap_or("");
    OsString::from(format!(r"\\.\pipe\focal-{stable}-{unique}"))
}

/// Publish the pipe name at `path` in an owner-only file, atomically.
fn write_name_file(path: &Path, name: &OsStr) -> Result<(), WireError> {
    let temporary = path.with_extension("pipe-new");
    let mut file = focal_platform::fs::open_private(&temporary, false, true, true)?;
    file.write_all(name.to_string_lossy().as_bytes())?;
    file.sync_all()?;
    drop(file);
    focal_platform::fs::atomic_replace(&temporary, path)?;
    Ok(())
}

/// Read the pipe name a server published at `path`, refusing a file not owned
/// by the current user.
fn read_name_file(path: &Path) -> Result<OsString, WireError> {
    if focal_platform::fs::owner_at(path)? != focal_platform::fs::current_owner()? {
        return Err(WireError::Authentication);
    }
    let bytes = std::fs::read(path)?;
    let name = String::from_utf8(bytes).map_err(|_| WireError::Authentication)?;
    if name.is_empty() {
        return Err(WireError::Authentication);
    }
    Ok(OsString::from(name))
}

pub struct LocalServer {
    /// The first instance, created at bind to fence squatters; taken by
    /// `serve`, which rotates a fresh instance in before serving each peer.
    server: Mutex<Option<NamedPipeServer>>,
    name: OsString,
    path: PathBuf,
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
        let name = pipe_name(&path);
        let name_for_setup = name.clone();
        let max = limits.max_connections;
        let server = transport_setup(move || {
            create_pipe_server(&name_for_setup, true, max).map_err(WireError::from)
        })?;
        write_name_file(&path, &name)?;
        let (shutdown, _) = watch::channel(false);
        Ok(Self {
            server: Mutex::new(Some(server)),
            name,
            path,
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
        let mut current = self
            .server
            .lock()
            .ok()
            .and_then(|mut guard| guard.take())
            .ok_or(WireError::Connection)?;
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
                connected=current.connect()=>{
                    connected?;
                    let next=create_pipe_server(&self.name,false,self.limits.max_connections)?;
                    let peer=std::mem::replace(&mut current,next);
                    if tasks.len() >= self.limits.max_connections {drop(peer);continue;}
                    if !pipe_client_is_current_owner(&peer).unwrap_or(false){drop(peer);continue;}
                    let grant=self.grant.borrow().clone();let limits=self.limits.clone();let handler=handler.clone();
                    tasks.spawn(async move {
                        let _ = transport_exchange(async {
                            tokio::time::timeout(
                                limits.request_timeout, serve_stream(peer, grant, limits, handler),
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
impl Drop for LocalServer {
    fn drop(&mut self) {
        // Remove the rendezvous file only when it still names our pipe, so a
        // replacement server's file is never removed.
        if std::fs::read(&self.path)
            .is_ok_and(|bytes| bytes == self.name.to_string_lossy().as_bytes())
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
        let name = read_name_file(&self.path)?;
        let client = self.connect(&name).await?;
        if !pipe_server_is_current_owner(&client).unwrap_or(false) {
            return Err(WireError::Authentication);
        }
        request_stream(client, request, &self.limits).await
    }
    async fn connect(
        &self,
        name: &OsStr,
    ) -> Result<tokio::net::windows::named_pipe::NamedPipeClient, WireError> {
        let deadline = Instant::now()
            .checked_add(self.limits.request_timeout)
            .ok_or(WireError::Timeout)?;
        loop {
            match ClientOptions::new().open(name) {
                Ok(client) => return Ok(client),
                Err(error) if error.raw_os_error() == Some(ERROR_PIPE_BUSY) => {
                    if Instant::now() >= deadline {
                        return Err(WireError::Timeout);
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(error) => return Err(error.into()),
            }
        }
    }
}
