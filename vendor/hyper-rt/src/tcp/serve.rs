//! Accepting on every shard (docs/runtime.md §5.2). [`serve`] binds an address for a runtime and serves
//! each connection on a shard with the caller's handler:
//!
//! - **Linux**: every shard binds its own listener with `SO_REUSEPORT`, and the kernel spreads connections
//!   among them by a hash of their addresses (socket(7)); each shard accepts and serves its own.
//! - **macOS and Windows**: one acceptor shard accepts and hands each connection to the shard with the
//!   fewest open connections (the acceptor's own included), the socket moving as its owned handle in a spawn
//!   request; the handler's future is made on the target shard, so it never crosses a thread and need not
//!   be `Send`. Windows has no `SO_REUSEPORT`; macOS has it with BSD's meaning, which does not balance: every
//!   connection went to the last listener bound (`tests/tcp_serve.rs` measured 32 of 32 on one of four
//!   shards), and XNU has no `SO_REUSEPORT_LB`.
//!
//! Every connection takes a slot of the process-wide [`ConnectionBudget`] and is counted on its shard;
//! both are given back when the stream drops, wherever it went. The shard counts are process-wide cells
//! (`crate::sync::cell`), read by the Windows acceptor to choose a shard and by [`Serving::open`].
//!
//! **Descriptors run out** (`EMFILE`, `ENFILE`, `ENOBUFS`, `ENOMEM`; Winsock's `WSAEMFILE`,
//! `WSAENOBUFS`): an accept loop with connections of its own open waits until one of them closes and frees
//! its descriptor, then accepts again; it never spins on the error. With none open, its own connections
//! cannot be the cause, so the shard stops serving and records the error ([`Serving::ended`]): the
//! descriptor limit is below what the process needs, a configuration to fix, not a condition to wait out.

use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};

use super::{ConnectionBudget, SocketAddr, TcpListener, TcpStream};
use crate::control::Control;
use crate::error::RtError;
use crate::registry::{self, SlotHolder};
use crate::runtime::Runtime;
use crate::shard::ShardId;
use crate::sync::cell::{CellRef, claim};
use crate::task::SpawnRequest;
use crate::waker::polling_task;

/// Format: a shard count cell's `aux` bit, set when one of its connections closed since its accept loop
/// last looked.
const RETURNED: u64 = 1;

/// One shard's place in a served address: its open connections (`state`), and its accept loop's end
/// (`ended`'s `state` is 1 once it stopped, its `aux` the OS error code plus one, 0 for none).
#[derive(Debug)]
struct ShardCells {
    shard: ShardId,
    count: CellRef,
    ended: CellRef,
}

/// An address served on a runtime's shards. Dropping it gives the counts back; the accept loops end with
/// the runtime.
#[derive(Debug)]
pub struct Serving {
    addr: SocketAddr,
    shards: Vec<ShardCells>,
}

impl Serving {
    /// The address served (the OS-assigned port, when bound to port 0).
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// The connections open on each shard now.
    pub fn open(&self) -> Vec<(ShardId, u64)> {
        self.shards
            .iter()
            .map(|cells| (cells.shard, open(cells.count)))
            .collect()
    }

    /// The shards whose accept loop stopped, with the OS error that stopped it, if one did.
    pub fn ended(&self) -> Vec<(ShardId, Option<i32>)> {
        self.shards
            .iter()
            .filter_map(|cells| {
                let ended = cells.ended.cell()?;
                (ended.state.load(Ordering::Acquire) != 0).then(|| {
                    let code = ended.aux.load(Ordering::Acquire);
                    let code = code
                        .checked_sub(1)
                        .and_then(|code| u32::try_from(code).ok())
                        .map(|code| i32::from_ne_bytes(code.to_ne_bytes()));
                    (cells.shard, code)
                })
            })
            .collect()
    }
}

impl Drop for Serving {
    fn drop(&mut self) {
        for cells in &self.shards {
            cells.count.release();
            cells.ended.release();
        }
    }
}

/// The connections open in a shard count cell.
fn open(count: CellRef) -> u64 {
    count
        .cell()
        .map_or(0, |cell| cell.state.load(Ordering::Acquire))
}

/// Counts a connection in `count`, holding a handle for it.
pub(super) fn counted(count: CellRef) {
    if let Some(cell) = count.cell() {
        cell.state.fetch_add(1, Ordering::AcqRel);
    }
    count.retain();
}

/// Gives a connection's count back and tells the accept loop waiting on it.
pub(super) fn returned(count: CellRef) {
    if let Some(cell) = count.cell() {
        cell.state.fetch_sub(1, Ordering::AcqRel);
        cell.aux.fetch_or(RETURNED, Ordering::AcqRel);
    }
    count.wake();
    count.release();
}

/// Records why `ended`'s accept loop stopped.
fn record_end(ended: CellRef, error: &RtError) {
    let Some(cell) = ended.cell() else {
        return;
    };
    let code = match error {
        RtError::DriverRefused {
            code: Some(code), ..
        } => u64::from(u32::from_ne_bytes(code.to_ne_bytes())).saturating_add(1),
        _ => 0,
    };
    cell.aux.store(code, Ordering::Release);
    cell.state.store(1, Ordering::Release);
}

/// Whether `error` is the process running out of descriptors or socket buffers.
fn exhausted(error: &RtError) -> bool {
    let RtError::DriverRefused {
        code: Some(code), ..
    } = error
    else {
        return false;
    };
    #[cfg(unix)]
    {
        matches!(
            *code,
            libc::EMFILE | libc::ENFILE | libc::ENOBUFS | libc::ENOMEM
        )
    }
    #[cfg(windows)]
    {
        use windows_sys::Win32::Networking::WinSock::{WSAEMFILE, WSAENOBUFS};
        matches!(*code, WSAEMFILE | WSAENOBUFS)
    }
}

/// Waits until a connection counted in any of `counts` closes.
struct Returned<'a> {
    counts: &'a [CellRef],
}

impl Future for Returned<'_> {
    type Output = Result<(), RtError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let took = |counts: &[CellRef]| {
            counts.iter().any(|count| {
                count.cell().is_some_and(|cell| {
                    cell.aux.fetch_and(!RETURNED, Ordering::AcqRel) & RETURNED != 0
                })
            })
        };
        if took(self.counts) {
            return Poll::Ready(Ok(()));
        }
        let Some(word) = polling_task(cx.waker()) else {
            return Poll::Ready(Err(RtError::NotOnShardThread));
        };
        for count in self.counts {
            count.register(word);
        }
        if took(self.counts) {
            return Poll::Ready(Ok(()));
        }
        Poll::Pending
    }
}

/// Serves `addr` on every shard of `rt` (module doc): each connection takes a slot of `budget` and is
/// handed to `handler` on the shard that serves it; `backlog` is each listener's accept queue. Returns once
/// every listener is bound; the accept loops run until the runtime shuts down.
pub fn serve<H, F>(
    rt: &Runtime,
    addr: impl Into<SocketAddr>,
    backlog: u32,
    budget: ConnectionBudget,
    handler: H,
) -> Result<Serving, RtError>
where
    H: Fn(TcpStream) -> F + Clone + Send + 'static,
    F: Future<Output = ()> + 'static,
{
    let mut shards = Vec::new();
    shards
        .try_reserve_exact(rt.shard_ids().len())
        .map_err(|_| RtError::Capacity {
            what: "served shards",
            bound: rt.shard_ids().len(),
        })?;
    for shard in rt.shard_ids() {
        let count = claim(1)?;
        let ended = claim(1).inspect_err(|_| count.release())?;
        shards.push(ShardCells {
            shard: *shard,
            count,
            ended,
        });
    }
    let mut serving = Serving {
        addr: addr.into(),
        shards,
    };
    let holders = serving
        .shards
        .iter()
        .map(|cells| {
            rt.holder_of(cells.shard).ok_or(RtError::ShardGone {
                shard: cells.shard.0,
            })
        })
        .collect::<Result<Vec<SlotHolder>, RtError>>()?;
    serving.addr = start(&serving, &holders, backlog, &budget, &handler)?;
    Ok(serving)
}

/// Linux: a listener per shard on one port.
#[cfg(target_os = "linux")]
fn start<H, F>(
    serving: &Serving,
    holders: &[SlotHolder],
    backlog: u32,
    budget: &ConnectionBudget,
    handler: &H,
) -> Result<SocketAddr, RtError>
where
    H: Fn(TcpStream) -> F + Clone + Send + 'static,
    F: Future<Output = ()> + 'static,
{
    let mut bound = serving.addr;
    let mut listeners = Vec::new();
    listeners
        .try_reserve_exact(serving.shards.len())
        .map_err(|_| RtError::Capacity {
            what: "served shards",
            bound: serving.shards.len(),
        })?;
    // All bound before any serves, so a refusal leaves nothing running.
    for _ in &serving.shards {
        let listener = TcpListener::bind_shared(bound, backlog)?.with_budget(budget.clone());
        bound = listener.local_addr()?;
        listeners.push(listener);
    }
    for ((cells, holder), listener) in serving.shards.iter().zip(holders).zip(listeners) {
        cells.count.retain();
        cells.ended.retain();
        let (count, ended, handler) = (cells.count, cells.ended, handler.clone());
        let accept = async move {
            accept_here(&listener, count, ended, handler).await;
            count.release();
            ended.release();
        };
        let request = Box::new(SpawnRequest::new(Box::pin(accept), None));
        registry::send_control_to_holder(*holder, Control::Spawn(request))?;
    }
    Ok(bound)
}

/// One shard's accept loop on its own listener.
#[cfg(target_os = "linux")]
async fn accept_here<H, F>(listener: &TcpListener, count: CellRef, ended: CellRef, handler: H)
where
    H: Fn(TcpStream) -> F,
    F: Future<Output = ()> + 'static,
{
    loop {
        match listener.accept().await {
            Ok(mut stream) => {
                stream.count_in(count);
                // A shard whose arena is full drops the connection (closed); the slot comes back with it.
                let _ = crate::futures::spawn_detached(handler(stream));
            }
            Err(error) if exhausted(&error) && open(count) > 0 => {
                if let Err(error) = (Returned { counts: &[count] }).await {
                    record_end(ended, &error);
                    return;
                }
            }
            Err(error) => {
                record_end(ended, &error);
                return;
            }
        }
    }
}

/// macOS and Windows: one acceptor, on the first shard, handing each connection to the least-loaded shard.
#[cfg(not(target_os = "linux"))]
fn start<H, F>(
    serving: &Serving,
    holders: &[SlotHolder],
    backlog: u32,
    budget: &ConnectionBudget,
    handler: &H,
) -> Result<SocketAddr, RtError>
where
    H: Fn(TcpStream) -> F + Clone + Send + 'static,
    F: Future<Output = ()> + 'static,
{
    let listener = TcpListener::bind(serving.addr, backlog)?.with_budget(budget.clone());
    let bound = listener.local_addr()?;
    let (Some(acceptor), Some(first)) = (holders.first(), serving.shards.first()) else {
        return Err(RtError::BadConfig {
            what: "serving on a runtime with no shards",
        });
    };
    let mut targets = Vec::new();
    targets
        .try_reserve_exact(serving.shards.len())
        .map_err(|_| RtError::Capacity {
            what: "served shards",
            bound: serving.shards.len(),
        })?;
    for (cells, holder) in serving.shards.iter().zip(holders) {
        cells.count.retain();
        targets.push((*holder, cells.count));
    }
    first.ended.retain();
    let (ended, handler) = (first.ended, handler.clone());
    let accept = async move {
        accept_and_hand_off(&listener, &targets, ended, handler).await;
        for (_, count) in &targets {
            count.release();
        }
        ended.release();
    };
    let request = Box::new(SpawnRequest::new(Box::pin(accept), None));
    registry::send_control_to_holder(*acceptor, Control::Spawn(request))?;
    Ok(bound)
}

/// The acceptor's loop (macOS and Windows).
#[cfg(not(target_os = "linux"))]
async fn accept_and_hand_off<H, F>(
    listener: &TcpListener,
    targets: &[(SlotHolder, CellRef)],
    ended: CellRef,
    handler: H,
) where
    H: Fn(TcpStream) -> F + Clone + Send + 'static,
    F: Future<Output = ()> + 'static,
{
    let counts: Vec<CellRef> = targets.iter().map(|(_, count)| *count).collect();
    let here = registry::with_current(|context| context.id);
    loop {
        match listener.accept().await {
            Ok(mut stream) => {
                let Some((holder, count)) = targets
                    .iter()
                    .min_by_key(|(_, count)| open(*count))
                    .copied()
                else {
                    continue;
                };
                stream.count_in(count);
                if here == Some(holder.shard()) {
                    let _ = crate::futures::spawn_detached(handler(stream));
                    continue;
                }
                let (owned, slot) = stream.into_parts();
                let handler = handler.clone();
                // Made on the target shard: the handler's future never crosses a thread.
                let launch = async move {
                    if let Ok(stream) = TcpStream::from_parts(owned, slot) {
                        let _ = crate::futures::spawn_detached(handler(stream));
                    }
                };
                let request = Box::new(SpawnRequest::new(Box::pin(launch), None));
                // A target whose control queue is full drops the request, and with it the connection
                // (closed) and its slot.
                let _ = registry::send_control_to_holder(holder, Control::Spawn(request));
            }
            Err(error) if exhausted(&error) && counts.iter().any(|count| open(*count) > 0) => {
                if let Err(error) = (Returned { counts: &counts }).await {
                    record_end(ended, &error);
                    return;
                }
            }
            Err(error) => {
                record_end(ended, &error);
                return;
            }
        }
    }
}
