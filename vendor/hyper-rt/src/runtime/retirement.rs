//! Cold-owned native thread retirement. Handles transfer before a service is admitted;
//! a service later publishes only its one retirement signal and awaits its existing reply.

use std::future::{Future, poll_fn};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::pin;
use std::sync::atomic::Ordering;
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError, TrySendError, sync_channel};
use std::thread::JoinHandle;

use crate::error::RtError;
use crate::sync::cell::{CellRef, claim};
use crate::sync::{self, ChannelReceiver, Sender, SyncError};

enum Command {
    Adopt {
        slot: usize,
        threads: Vec<JoinHandle<()>>,
        reply: Option<Sender<Reply>>,
    },
    Retire(usize),
    OriginalPublished(usize),
    OriginalRetire(usize),
    Stop,
}

enum Reply {
    Adopted,
    Refused {
        error: RtError,
        threads: Vec<JoinHandle<()>>,
    },
    Retired(Result<(), RtError>),
}

struct Group {
    capacity: usize,
    threads: Vec<JoinHandle<()>>,
    reply: Option<Sender<Reply>>,
    retired: bool,
}

impl Group {
    fn retire(&mut self) -> Result<(), RtError> {
        let mut result = Ok(());
        while let Some(thread) = self.threads.pop() {
            // An ordinary native thread panic is a failed join, not loss of the
            // remaining handles. A process-aborting TLS panic cannot be recovered.
            let joined = catch_unwind(AssertUnwindSafe(|| match thread.join() {
                Ok(()) => true,
                Err(payload) => {
                    drop(payload);
                    false
                }
            }));
            if !matches!(joined, Ok(true)) && result.is_ok() {
                result = Err(RtError::BadConfig {
                    what: "a retired native thread ended abnormally",
                });
            }
        }
        self.retired = true;
        result
    }

    fn complete(&mut self, done: CellRef, completed: &mut usize) {
        let result = self.retire();
        if result.is_err()
            && let Some(cell) = done.cell()
        {
            cell.aux.store(u64::from(true), Ordering::Release);
        }
        *completed = completed.saturating_add(1);
        if let Some(cell) = done.cell() {
            cell.state.store(
                u64::try_from(*completed).unwrap_or(u64::MAX),
                Ordering::Release,
            );
        }
        if let Some(reply) = self.reply.take() {
            let _ = reply.try_send(Reply::Retired(result));
        }
    }
}

impl Drop for Group {
    fn drop(&mut self) {
        // This value exists only on the reaper (or empty, before its spawn). Even
        // an unexpected unwind cannot detach an adopted native handle.
        let _ = self.retire();
    }
}

/// One pre-reserved worker group. No native handles ever reside in a live service's Drop.
pub struct RetirementLease {
    commands: SyncSender<Command>,
    reply: Option<Sender<Reply>>,
    answers: ChannelReceiver<Reply>,
    slot: usize,
    capacity: usize,
    command_capacity: usize,
    adopted: bool,
    requested: bool,
    result: Option<Result<(), RtError>>,
}

impl std::fmt::Debug for RetirementLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetirementLease")
            .field("capacity", &self.capacity)
            .field("adopted", &self.adopted)
            .field("requested", &self.requested)
            .finish_non_exhaustive()
    }
}

impl RetirementLease {
    /// The exact worker capacity reserved at cold setup.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Whether cold adoption accepted the handles, including a later receipt failure.
    pub fn adopted(&self) -> bool {
        self.adopted
    }

    /// Transfers handles only on acceptance. A refusal leaves their entire Vec with the caller.
    /// Cold setup waits for adoption before publishing the service's engine.
    pub fn adopt(&mut self, threads: &mut Vec<JoinHandle<()>>) -> Result<(), RtError> {
        if crate::registry::with_current(|_| ()).is_some() {
            return Err(RtError::NotOnShardThread);
        }
        if self.adopted || self.requested {
            return Err(RtError::BadConfig {
                what: "native thread group adopted twice",
            });
        }
        if threads.len() > self.capacity {
            return Err(RtError::Capacity {
                what: "native retirement handles",
                bound: self.capacity,
            });
        }
        let reply = self.reply.take().ok_or(RtError::BadConfig {
            what: "native retirement adoption without its receipt",
        })?;
        let command = Command::Adopt {
            slot: self.slot,
            threads: std::mem::take(threads),
            reply: Some(reply),
        };
        if let Err(error) = self.commands.try_send(command) {
            let (mut command, refused) = match error {
                TrySendError::Full(command) => (
                    command,
                    RtError::Capacity {
                        what: "native retirement commands",
                        bound: self.command_capacity,
                    },
                ),
                TrySendError::Disconnected(command) => (
                    command,
                    RtError::BadConfig {
                        what: "native retirement owner ended before adoption",
                    },
                ),
            };
            if let Command::Adopt {
                threads: returned,
                reply,
                ..
            } = &mut command
            {
                *threads = std::mem::take(returned);
                self.reply = reply.take();
            }
            return Err(refused);
        }
        self.adopted = true;
        match self.answers.blocking_recv() {
            Ok(Reply::Adopted) => Ok(()),
            Ok(Reply::Refused {
                error,
                threads: returned,
            }) => {
                self.adopted = false;
                *threads = returned;
                Err(error)
            }
            _ => Err(RtError::BadConfig {
                what: "native retirement adoption lost its receipt after acceptance",
            }),
        }
    }

    /// Signals exactly once. The group has already transferred in cold setup.
    pub fn request(&mut self) -> Result<(), RtError> {
        if self.requested {
            return Ok(());
        }
        match self.commands.try_send(Command::Retire(self.slot)) {
            Ok(()) => {
                self.requested = true;
                Ok(())
            }
            Err(TrySendError::Full(_)) => Err(RtError::Capacity {
                what: "native retirement command credits",
                bound: self.command_capacity,
            }),
            Err(TrySendError::Disconnected(_)) if self.adopted => {
                // Owner Stop closes admission before joining adopted groups. The
                // group's completion, not that admission close, proves retirement.
                self.requested = true;
                Ok(())
            }
            Err(TrySendError::Disconnected(_)) => Err(RtError::BadConfig {
                what: "native retirement owner ended before its signal",
            }),
        }
    }

    fn accept(&mut self, reply: Reply) -> Result<(), RtError> {
        match reply {
            Reply::Retired(result) => {
                self.result = Some(result);
                Ok(())
            }
            _ => Err(RtError::BadConfig {
                what: "a native retirement reply out of order",
            }),
        }
    }

    /// None before physical thread/TLS retirement; an error result still joined every handle.
    pub fn try_result(&mut self) -> Result<Option<Result<(), RtError>>, RtError> {
        if !self.adopted {
            return Err(RtError::BadConfig {
                what: "native retirement wait before adoption",
            });
        }
        if self.result.is_none() {
            match self.answers.try_recv() {
                Ok(Some(reply)) => self.accept(reply)?,
                Ok(None) => {}
                Err(_) => {
                    return Err(RtError::BadConfig {
                        what: "native retirement completion closed early",
                    });
                }
            }
        }
        Ok(self.result.clone())
    }

    /// A canceled borrowed wait retains the same group, signal and completion.
    pub async fn wait(&mut self) -> Result<(), RtError> {
        poll_fn(|cx| std::task::Poll::Ready(context(cx))).await?;
        if !self.adopted {
            return Err(RtError::BadConfig {
                what: "native retirement wait before adoption",
            });
        }
        self.request()?;
        if self.result.is_none() {
            let reply = {
                let mut receive = pin!(self.answers.recv());
                poll_fn(|cx| {
                    if let Err(error) = context(cx) {
                        return std::task::Poll::Ready(Err(error));
                    }
                    receive.as_mut().poll(cx).map(|reply| {
                        reply.map_err(|_| RtError::BadConfig {
                            what: "native retirement completion closed early",
                        })
                    })
                })
                .await?
            };
            self.accept(reply)?;
        }
        self.result.clone().ok_or(RtError::BadConfig {
            what: "native retirement completed without a result",
        })?
    }

    /// Cold refusal cleanup only. Runtime owners await `wait` instead.
    pub fn wait_blocking(&mut self) -> Result<(), RtError> {
        if crate::registry::with_current(|_| ()).is_some() {
            return Err(RtError::NotOnShardThread);
        }
        if !self.adopted {
            return Err(RtError::BadConfig {
                what: "native retirement wait before adoption",
            });
        }
        self.request()?;
        if self.result.is_none() {
            let reply = self
                .answers
                .blocking_recv()
                .map_err(|_| RtError::BadConfig {
                    what: "native retirement completion closed early",
                })?;
            self.accept(reply)?;
        }
        self.result.clone().ok_or(RtError::BadConfig {
            what: "native retirement completed without a result",
        })?
    }
}

impl Drop for RetirementLease {
    fn drop(&mut self) {
        // Only a scalar signal is dropped on refusal. The native handles stay in the
        // runtime-owned reaper, whose EOF cleanup retires every adopted group too.
        let _ = self.request();
    }
}

fn context(cx: &std::task::Context<'_>) -> Result<(), RtError> {
    if crate::futures::current_task()
        .is_some_and(|task| crate::waker::waker_for(task.0).will_wake(cx.waker()))
    {
        Ok(())
    } else {
        Err(RtError::NotOnShardThread)
    }
}

pub(super) struct Owner {
    thread: Option<JoinHandle<()>>,
    commands: Option<SyncSender<Command>>,
    done: CellRef,
    groups: usize,
}

impl Owner {
    pub(super) fn groups(&self) -> usize {
        self.groups
    }
    pub(super) fn done(&self) -> bool {
        self.done.cell().is_some_and(|cell| {
            cell.state.load(Ordering::Acquire) == u64::try_from(self.groups).unwrap_or(u64::MAX)
        })
    }

    pub(super) fn new(capacities: &[usize]) -> Result<(Self, Vec<RetirementLease>), RtError> {
        let groups = capacities.len();
        if groups == 0 || capacities.contains(&0) {
            return Err(RtError::BadConfig {
                what: "an empty native retirement reservation",
            });
        }
        let refused = || RtError::Capacity {
            what: "native retirement setup",
            bound: groups,
        };
        // One command per lease, plus the cold owner's independent Stop credit.
        let command_capacity = groups.checked_add(1).ok_or_else(refused)?;
        std::alloc::Layout::array::<Command>(command_capacity).map_err(|_| refused())?;
        for &capacity in capacities {
            std::alloc::Layout::array::<JoinHandle<()>>(capacity).map_err(|_| {
                RtError::Capacity {
                    what: "native retirement handle layout",
                    bound: capacity,
                }
            })?;
        }
        let mut leases = Vec::new();
        let mut slots = Vec::new();
        leases.try_reserve_exact(groups).map_err(|_| refused())?;
        slots.try_reserve_exact(groups).map_err(|_| refused())?;
        let (commands, incoming) = sync_channel(command_capacity);
        for (slot, &capacity) in capacities.iter().enumerate() {
            let (reply, answers) = sync::channel(1)?;
            slots.push(Group {
                capacity,
                threads: Vec::new(),
                reply: None,
                retired: false,
            });
            leases.push(RetirementLease {
                commands: commands.clone(),
                reply: Some(reply),
                answers,
                slot,
                capacity,
                command_capacity,
                adopted: false,
                requested: false,
                result: None,
            });
        }
        let done = claim(1)?;
        done.retain();
        let spawned = super::spawn_retirement(move || {
            let _handle = CountHandle(done);
            run(slots, incoming, done);
        });
        let thread = match spawned {
            Ok(thread) => thread,
            Err(error) => {
                done.release();
                done.release();
                return Err(RtError::DriverRefused {
                    call: "retirement thread spawn",
                    code: error.raw_os_error(),
                });
            }
        };
        Ok((
            Self {
                thread: Some(thread),
                commands: Some(commands),
                done,
                groups,
            },
            leases,
        ))
    }

    pub(super) fn join(&mut self) -> Result<(), RtError> {
        // The owner has already joined every shard/service worker. Unused external
        // reservations must not make its executor wait for their eventual Drop.
        if let Some(commands) = self.commands.take() {
            // Cold owner only; the extra Stop slot is unavailable to lease commands.
            let _ = commands.try_send(Command::Stop);
        }
        let failed_owner = self
            .thread
            .take()
            .is_some_and(|thread| thread.join().is_err());
        // The native thread is joined, so no group can publish a later failure. Consume
        // this owner's report once; lease replies retain their own matching result.
        let failed_group = self
            .done
            .cell()
            .is_some_and(|cell| cell.aux.swap(0, Ordering::AcqRel) != 0);
        if failed_owner {
            return Err(RtError::BadConfig {
                what: "native retirement owner ended abnormally",
            });
        }
        if failed_group {
            return Err(RtError::BadConfig {
                what: "a retired native thread ended abnormally",
            });
        }
        Ok(())
    }
}

impl Drop for Owner {
    fn drop(&mut self) {
        let _ = self.join();
        self.done.release();
    }
}

struct CountHandle(CellRef);
impl Drop for CountHandle {
    fn drop(&mut self) {
        self.0.release();
    }
}

impl Drop for Command {
    fn drop(&mut self) {
        if let Self::Adopt { threads, reply, .. } = self {
            if threads.is_empty() && reply.is_none() {
                return;
            }
            let returned = std::mem::take(threads);
            if let Some(reply) = reply.take() {
                refuse(
                    reply,
                    returned,
                    RtError::BadConfig {
                        what: "native retirement owner stopped before adoption",
                    },
                );
            } else {
                let mut rejected = Group {
                    capacity: returned.len(),
                    threads: returned,
                    reply: None,
                    retired: false,
                };
                let _ = rejected.retire();
            }
        }
    }
}

fn refuse(reply: Sender<Reply>, threads: Vec<JoinHandle<()>>, error: RtError) {
    if let Err(
        SyncError::Closed(Reply::Refused { threads, .. })
        | SyncError::Full(Reply::Refused { threads, .. })
        | SyncError::NotOnShardThread(Reply::Refused { threads, .. }),
    ) = reply.try_send(Reply::Refused { error, threads })
    {
        // A dead adopter cannot reclaim its rejected owners. This runs on the
        // reaper, never a service; every native handle is joined before Drop.
        let mut rejected = Group {
            capacity: threads.len(),
            threads,
            reply: None,
            retired: false,
        };
        let _ = rejected.retire();
    }
}

fn adopt_group(
    groups: &mut [Group],
    slot: usize,
    threads: Vec<JoinHandle<()>>,
    reply: Sender<Reply>,
) {
    match groups.get_mut(slot) {
        Some(group)
            if !group.retired && group.reply.is_none() && threads.len() <= group.capacity =>
        {
            group.threads = threads;
            group.reply = Some(reply);
            if let Some(reply) = &group.reply {
                let _ = reply.try_send(Reply::Adopted);
            }
        }
        _ => refuse(
            reply,
            threads,
            RtError::BadConfig {
                what: "native retirement adoption outside its reservation",
            },
        ),
    }
}

fn run(mut groups: Vec<Group>, incoming: Receiver<Command>, done: CellRef) {
    let mut completed = 0usize;
    while completed < groups.len() {
        let Ok(mut command) = incoming.recv() else {
            break;
        };
        match &mut command {
            Command::Adopt {
                slot,
                threads,
                reply,
            } => {
                if let Some(reply) = reply.take() {
                    let threads = std::mem::take(threads);
                    adopt_group(&mut groups, *slot, threads, reply);
                }
            }
            Command::Retire(slot) => {
                if let Some(group) = groups.get_mut(*slot).filter(|group| !group.retired) {
                    group.complete(done, &mut completed);
                }
            }
            Command::OriginalPublished(_) | Command::OriginalRetire(_) => {}
            Command::Stop => break,
        }
    }
    // Close admission before the terminal joins. Any queued/racing late Adopt
    // Command returns its whole Vec through its own Drop; unused lease endpoints
    // can outlive shutdown without keeping this receiver or native executor alive.
    drop(incoming);
    for group in &mut groups {
        if !group.retired {
            group.complete(done, &mut completed);
        }
    }
    if let Some(cell) = done.cell() {
        cell.state.store(
            u64::try_from(groups.len()).unwrap_or(u64::MAX),
            Ordering::Release,
        );
    }
}

/// A registered physical-retirement fence used only by the cold native reaper.
/// `wait_blocking` must wait for its existing physical terminal event. Both successful
/// and physical-error returns have `is_retired() == true`; a preflight refusal is not
/// retirement. No file operation or destructor is invoked by the fence itself.
/// A malformed implementation that returns nonterminally remains retained until an
/// explicit later retirement command; shutdown cannot force-close that resource.
pub trait OriginalFence: Send + 'static {
    /// Waits on the pre-registered physical terminal event, retaining its first result.
    fn wait_blocking(&mut self) -> Result<(), RtError>;
    /// True only after the physical terminal event has been consumed or proven closed.
    fn is_retired(&self) -> bool;
}

/// One admission bit-state: open, a cold publisher owns the turn, published, or closed.
/// These encode ownership transitions, not a retry or timing budget.
/// Native ownership encoding: zero is the newly claimed cell's open state.
const ORIGINAL_OPEN: u64 = 0;
/// Native ownership encoding: the sole cold publisher has claimed its lane turn.
const ORIGINAL_PUBLISHING: u64 = 1;
/// Native ownership encoding: the whole resource was published before this state.
const ORIGINAL_OWNED: u64 = 2;
/// Native ownership encoding: unused or refused admission can never reopen.
const ORIGINAL_CLOSED: u64 = 3;

struct OriginalAdmission(CellRef);
impl OriginalAdmission {
    fn state(&self) -> Option<u64> {
        self.0.cell().map(|cell| cell.state.load(Ordering::Acquire))
    }
    fn close_open(&self) {
        if let Some(cell) = self.0.cell() {
            let _ = cell.state.compare_exchange(
                ORIGINAL_OPEN,
                ORIGINAL_CLOSED,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }
    }
}
impl Drop for OriginalAdmission {
    fn drop(&mut self) {
        self.0.release();
    }
}

/// One cold reservation pairs worker retirement with original-resource adoption.
pub type OriginalRetirement<F, W> = (RetirementLease, OriginalAdoption<F, W>);

struct OriginalResource<F, W> {
    file: F,
    fence: W,
    reply: Sender<OriginalReply>,
}

enum OriginalReply {
    Refused(RtError),
    Retired(Result<(), RtError>),
}

/// Cold, single-use adoption into this group's existing native reaper. Refusal before
/// publication leaves the entire file in its original Option and returns the fence.
pub struct OriginalAdoption<F, W> {
    incoming: Option<SyncSender<OriginalResource<F, W>>>,
    reply: Option<Sender<OriginalReply>>,
    answers: Option<ChannelReceiver<OriginalReply>>,
    commands: SyncSender<Command>,
    admission: OriginalAdmission,
    slot: usize,
    adopted: bool,
}

impl<F, W> std::fmt::Debug for OriginalAdoption<F, W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OriginalAdoption")
            .field("slot", &self.slot)
            .field("adopted", &self.adopted)
            .finish_non_exhaustive()
    }
}

impl<F: Send + 'static, W: OriginalFence> OriginalAdoption<F, W> {
    /// The only file move is the successful publication to the reaper-owned lane.
    /// No receipt wait follows this move: a post-acceptance failure must not pretend
    /// that the caller still owns F. The lane and admission state are reserved cold.
    pub fn adopt(&mut self, file: &mut Option<F>, fence: W) -> Result<OriginalLease, (W, RtError)> {
        let refused = |what| RtError::BadConfig { what };
        if crate::registry::with_current(|_| ()).is_some() {
            return Err((fence, RtError::NotOnShardThread));
        }
        if self.adopted
            || file.is_none()
            || self.incoming.is_none()
            || self.answers.is_none()
            || self.reply.is_none()
        {
            return Err((
                fence,
                refused("original resource outside its cold reservation"),
            ));
        }
        let Some(cell) = self.admission.0.cell() else {
            return Err((fence, refused("original admission cell retired early")));
        };
        if cell
            .state
            .compare_exchange(
                ORIGINAL_OPEN,
                ORIGINAL_PUBLISHING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return Err((fence, refused("original resource admission is closed")));
        }
        // No foreign call or allocation lies between owning the publication turn and
        // its final state/notification. All branches restore owners before refusal.
        let Some(incoming) = self.incoming.take() else {
            cell.state.store(ORIGINAL_CLOSED, Ordering::Release);
            let _ = self
                .commands
                .try_send(Command::OriginalPublished(self.slot));
            return Err((fence, refused("original admission lost its lane")));
        };
        let Some(answers) = self.answers.take() else {
            self.incoming = Some(incoming);
            cell.state.store(ORIGINAL_CLOSED, Ordering::Release);
            let _ = self
                .commands
                .try_send(Command::OriginalPublished(self.slot));
            return Err((fence, refused("original admission lost its receiver")));
        };
        let Some(reply) = self.reply.take() else {
            self.incoming = Some(incoming);
            self.answers = Some(answers);
            cell.state.store(ORIGINAL_CLOSED, Ordering::Release);
            let _ = self
                .commands
                .try_send(Command::OriginalPublished(self.slot));
            return Err((fence, refused("original admission lost its receipt")));
        };
        let Some(owned) = file.take() else {
            self.incoming = Some(incoming);
            self.answers = Some(answers);
            self.reply = Some(reply);
            cell.state.store(ORIGINAL_CLOSED, Ordering::Release);
            let _ = self
                .commands
                .try_send(Command::OriginalPublished(self.slot));
            return Err((fence, refused("original admission lost its file")));
        };
        let resource = OriginalResource {
            file: owned,
            fence,
            reply,
        };
        if let Err(error) = incoming.try_send(resource) {
            let resource = match error {
                TrySendError::Full(resource) | TrySendError::Disconnected(resource) => resource,
            };
            *file = Some(resource.file);
            self.reply = Some(resource.reply);
            self.answers = Some(answers);
            self.incoming = Some(incoming);
            cell.state.store(ORIGINAL_CLOSED, Ordering::Release);
            let _ = self
                .commands
                .try_send(Command::OriginalPublished(self.slot));
            return Err((
                resource.fence,
                refused("original native lane refused publication"),
            ));
        }
        cell.state.store(ORIGINAL_OWNED, Ordering::Release);
        self.adopted = true;
        // This notification and the later retirement signal own separate command
        // credits. Stop retains a Publishing receiver until this publication finishes.
        let _ = self
            .commands
            .try_send(Command::OriginalPublished(self.slot));
        Ok(OriginalLease {
            commands: self.commands.clone(),
            answers,
            slot: self.slot,
            requested: false,
            result: None,
        })
    }
}

impl<F, W> Drop for OriginalAdoption<F, W> {
    fn drop(&mut self) {
        if !self.adopted {
            self.admission.close_open();
            let _ = self.commands.try_send(Command::OriginalRetire(self.slot));
        }
    }
}

/// A separate, reusable borrowed receipt for the original resource's actual close.
/// It owns no file, fence, native handle, Box, or per-operation allocation.
pub struct OriginalLease {
    commands: SyncSender<Command>,
    answers: ChannelReceiver<OriginalReply>,
    slot: usize,
    requested: bool,
    result: Option<Result<(), RtError>>,
}
impl std::fmt::Debug for OriginalLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OriginalLease")
            .field("slot", &self.slot)
            .field("requested", &self.requested)
            .field("retired", &self.is_retired())
            .finish_non_exhaustive()
    }
}
impl OriginalLease {
    /// A terminal error still proves that the fence completed and F Drop returned/unwound.
    pub fn is_retired(&self) -> bool {
        self.result.is_some()
    }
    /// Publishes one scalar signal. A retry is possible only after a nonterminal refusal
    /// receipt has been consumed; a canceled borrowed wait cannot duplicate the signal.
    pub fn request(&mut self) -> Result<(), RtError> {
        if self.requested || self.is_retired() {
            return Ok(());
        }
        match self.commands.try_send(Command::OriginalRetire(self.slot)) {
            Ok(()) | Err(TrySendError::Disconnected(_)) => {
                self.requested = true;
                Ok(())
            }
            Err(TrySendError::Full(_)) => Err(RtError::BadConfig {
                what: "original retirement command credit missing",
            }),
        }
    }
    fn accept(&mut self, reply: OriginalReply) -> Result<(), RtError> {
        match reply {
            OriginalReply::Retired(result) => {
                self.result = Some(result);
                Ok(())
            }
            OriginalReply::Refused(error) => {
                self.requested = false;
                Err(error)
            }
        }
    }
    /// None before physical close. Context/refusal errors never set the terminal flag.
    pub fn try_result(&mut self) -> Result<Option<Result<(), RtError>>, RtError> {
        if self.result.is_none() {
            match self.answers.try_recv() {
                Ok(Some(reply)) => self.accept(reply)?,
                Ok(None) => {}
                Err(_) => {
                    return Err(RtError::BadConfig {
                        what: "original retirement completion closed early",
                    });
                }
            }
        }
        Ok(self.result.clone())
    }
    /// Cancel-safe and guarded before every poll, including already queued results.
    pub async fn wait(&mut self) -> Result<(), RtError> {
        poll_fn(|cx| std::task::Poll::Ready(context(cx))).await?;
        self.request()?;
        if self.result.is_none() {
            let reply = {
                let mut receive = pin!(self.answers.recv());
                poll_fn(|cx| {
                    if let Err(error) = context(cx) {
                        return std::task::Poll::Ready(Err(error));
                    }
                    receive.as_mut().poll(cx).map(|reply| {
                        reply.map_err(|_| RtError::BadConfig {
                            what: "original retirement completion closed early",
                        })
                    })
                })
                .await?
            };
            self.accept(reply)?;
        }
        self.result.clone().ok_or(RtError::BadConfig {
            what: "original retirement completed without a result",
        })?
    }
    /// Cold rollback only; a service uses `wait`.
    pub fn wait_blocking(&mut self) -> Result<(), RtError> {
        if crate::registry::with_current(|_| ()).is_some() {
            return Err(RtError::NotOnShardThread);
        }
        self.request()?;
        if self.result.is_none() {
            let reply = self
                .answers
                .blocking_recv()
                .map_err(|_| RtError::BadConfig {
                    what: "original retirement completion closed early",
                })?;
            self.accept(reply)?;
        }
        self.result.clone().ok_or(RtError::BadConfig {
            what: "original retirement completed without a result",
        })?
    }
}
impl Drop for OriginalLease {
    fn drop(&mut self) {
        let _ = self.request();
    }
}

struct OriginalGroup<F, W> {
    incoming: Receiver<OriginalResource<F, W>>,
    admission: OriginalAdmission,
    resource: Option<OriginalResource<F, W>>,
    workers_result: Option<Result<(), RtError>>,
    retired: bool,
    refused: bool,
}

impl Owner {
    pub(super) fn new_with_original<F: Send + 'static, W: OriginalFence>(
        capacities: &[usize],
    ) -> Result<(Self, Vec<OriginalRetirement<F, W>>), RtError> {
        let groups = capacities.len();
        let refused = || RtError::Capacity {
            what: "original retirement setup",
            bound: groups,
        };
        if groups == 0 || capacities.contains(&0) {
            return Err(RtError::BadConfig {
                what: "an empty original retirement reservation",
            });
        }
        // Each group can have one worker Adopt-or-Retire, one original publication
        // notification, and one original retirement command outstanding; Stop owns its
        // independent final slot. All retries consume a refusal before publishing again.
        let command_capacity = groups
            .checked_mul(3)
            .and_then(|n| n.checked_add(1))
            .ok_or_else(refused)?;
        command_layout::<Command>(command_capacity)?;
        command_layout::<OriginalResource<F, W>>(1)?;
        for &capacity in capacities {
            std::alloc::Layout::array::<JoinHandle<()>>(capacity).map_err(|_| refused())?;
        }
        let mut leases = Vec::new();
        let mut slots = Vec::new();
        let mut originals = Vec::new();
        leases.try_reserve_exact(groups).map_err(|_| refused())?;
        slots.try_reserve_exact(groups).map_err(|_| refused())?;
        originals.try_reserve_exact(groups).map_err(|_| refused())?;
        let (commands, incoming) = sync_channel(command_capacity);
        for (slot, &capacity) in capacities.iter().enumerate() {
            let (reply, answers) = sync::channel(1)?;
            let (original_reply, original_answers) = sync::channel(1)?;
            let (original_sender, original_incoming) = sync_channel(1);
            let admission = claim(1)?;
            admission.retain();
            originals.push(OriginalGroup {
                incoming: original_incoming,
                admission: OriginalAdmission(admission),
                resource: None,
                workers_result: None,
                retired: false,
                refused: false,
            });
            slots.push(Group {
                capacity,
                threads: Vec::new(),
                reply: None,
                retired: false,
            });
            leases.push((
                RetirementLease {
                    commands: commands.clone(),
                    reply: Some(reply),
                    answers,
                    slot,
                    capacity,
                    command_capacity,
                    adopted: false,
                    requested: false,
                    result: None,
                },
                OriginalAdoption {
                    incoming: Some(original_sender),
                    reply: Some(original_reply),
                    answers: Some(original_answers),
                    commands: commands.clone(),
                    admission: OriginalAdmission(admission),
                    slot,
                    adopted: false,
                },
            ));
        }
        let done = claim(1)?;
        done.retain();
        // Keep this receiver alive through a nonterminal foreign-fence refusal. Its
        // explicit later commands, not a spin/retry cadence, are the only retry source.
        let keepalive = commands.clone();
        let spawned = super::spawn_retirement(move || {
            let _handle = CountHandle(done);
            run_original(slots, originals, incoming, keepalive, done);
        });
        let thread = match spawned {
            Ok(thread) => thread,
            Err(error) => {
                done.release();
                done.release();
                return Err(RtError::DriverRefused {
                    call: "original retirement thread spawn",
                    code: error.raw_os_error(),
                });
            }
        };
        Ok((
            Self {
                thread: Some(thread),
                commands: Some(commands),
                done,
                groups,
            },
            leases,
        ))
    }
}

// Same Rust 1.98 bounded std channel representation preflight as sync::channel_capacity.
fn command_layout<T>(capacity: usize) -> Result<(), RtError> {
    let refused = || RtError::BadConfig {
        what: "retirement command ticket or slot layout",
    };
    capacity
        .checked_add(1)
        .and_then(usize::checked_next_power_of_two)
        .and_then(|mark| mark.checked_mul(2))
        .ok_or_else(refused)?;
    let (slot, _) = std::alloc::Layout::new::<std::sync::atomic::AtomicUsize>()
        .extend(std::alloc::Layout::new::<std::mem::MaybeUninit<T>>())
        .map_err(|_| refused())?;
    let slot = slot.pad_to_align();
    let bytes = slot.size().checked_mul(capacity).ok_or_else(refused)?;
    std::alloc::Layout::from_size_align(bytes, slot.align()).map_err(|_| refused())?;
    Ok(())
}

fn record_failure(done: CellRef) {
    if let Some(cell) = done.cell() {
        cell.aux.store(1, Ordering::Release);
    }
}
fn retire_original_workers<F, W>(
    group: &mut Group,
    original: &mut OriginalGroup<F, W>,
    done: CellRef,
) {
    if group.retired {
        return;
    }
    let result = group.retire();
    if result.is_err() {
        record_failure(done);
    }
    original.workers_result = Some(result.clone());
    if let Some(reply) = group.reply.take() {
        let _ = reply.try_send(Reply::Retired(result));
    }
}

fn caught_error<T>(outcome: std::thread::Result<T>, what: &'static str) -> Result<T, RtError> {
    outcome.map_err(|payload| {
        // A foreign panic payload Drop may itself panic; preserve the native reaper.
        let _ = catch_unwind(AssertUnwindSafe(|| drop(payload)));
        RtError::BadConfig { what }
    })
}

fn original_ready<F, W>(original: &mut OriginalGroup<F, W>) -> bool {
    match original.admission.state() {
        Some(ORIGINAL_OWNED) => {
            if original.resource.is_none() {
                match original.incoming.try_recv() {
                    Ok(resource) => original.resource = Some(resource),
                    // OWNED follows publication. Never treat a missing payload as completion.
                    Err(TryRecvError::Empty | TryRecvError::Disconnected) => return false,
                }
            }
            true
        }
        Some(ORIGINAL_CLOSED) => true,
        Some(ORIGINAL_PUBLISHING) | Some(ORIGINAL_OPEN) | None => false,
        Some(_) => false,
    }
}

// Outer Err is a nonterminal refusal; inner Err is the registered physical terminal cause.
fn original_fence<F, W: OriginalFence>(
    resource: &mut OriginalResource<F, W>,
) -> Result<Result<(), RtError>, RtError> {
    let waited = caught_error(
        catch_unwind(AssertUnwindSafe(|| resource.fence.wait_blocking())),
        "original physical fence panicked",
    )
    .and_then(|result| result);
    let retired = caught_error(
        catch_unwind(AssertUnwindSafe(|| resource.fence.is_retired())),
        "original physical terminal check panicked",
    );
    if matches!(retired, Ok(true)) {
        Ok(waited)
    } else {
        Err(waited
            .err()
            .or_else(|| retired.err())
            .unwrap_or(RtError::BadConfig {
                what: "original physical fence returned before retirement",
            }))
    }
}

fn complete_original<F, W>(
    original: &mut OriginalGroup<F, W>,
    done: CellRef,
    completed: &mut usize,
    mut result: Result<(), RtError>,
) {
    let reply = if let Some(resource) = original.resource.take() {
        let OriginalResource { file, fence, reply } = resource;
        let closed = caught_error(
            catch_unwind(AssertUnwindSafe(|| drop(file))),
            "original resource Drop panicked",
        );
        // Both owned destructors always run, even when a prior physical cause already exists.
        let fence_drop = caught_error(
            catch_unwind(AssertUnwindSafe(|| drop(fence))),
            "original physical fence Drop panicked",
        );
        result = result.and(closed.and(fence_drop));
        Some(reply)
    } else {
        None
    };
    original.retired = true;
    if result.is_err() {
        record_failure(done);
    }
    *completed = completed.saturating_add(1);
    if let Some(cell) = done.cell() {
        cell.state.store(
            u64::try_from(*completed).unwrap_or(u64::MAX),
            Ordering::Release,
        );
    }
    if let Some(reply) = reply {
        let _ = reply.try_send(OriginalReply::Retired(result));
    }
}

fn retire_original<F: Send + 'static, W: OriginalFence>(
    group: &mut Group,
    original: &mut OriginalGroup<F, W>,
    done: CellRef,
    completed: &mut usize,
    reply_refusal: bool,
) {
    if original.retired || !original_ready(original) {
        return;
    }
    // Raw field Drop may signal original first: always join the same group before its fence.
    retire_original_workers(group, original, done);
    let mut result = original.workers_result.clone().unwrap_or(Ok(()));
    if let Some(resource) = original.resource.as_mut() {
        let waited = match original_fence(resource) {
            Ok(waited) => waited,
            Err(error) => {
                original.refused = true;
                // Stop/publication cannot leave an unrequested refusal ahead of a later result.
                if reply_refusal {
                    let _ = resource.reply.try_send(OriginalReply::Refused(error));
                }
                return;
            }
        };
        if waited.is_err() || result.is_ok() {
            result = waited;
        }
    }
    complete_original(original, done, completed, result);
}

fn run_original<F: Send + 'static, W: OriginalFence>(
    mut groups: Vec<Group>,
    mut originals: Vec<OriginalGroup<F, W>>,
    incoming: Receiver<Command>,
    _keepalive: SyncSender<Command>,
    done: CellRef,
) {
    let mut completed = 0usize;
    let mut stopping = false;
    while completed < groups.len() {
        let Ok(mut command) = incoming.recv() else {
            break;
        };
        match &mut command {
            Command::Adopt {
                slot,
                threads,
                reply,
            } if !stopping => {
                if let Some(reply) = reply.take() {
                    adopt_group(&mut groups, *slot, std::mem::take(threads), reply);
                }
            }
            Command::Adopt { .. } => {} // Command Drop returns every refused handle.
            Command::Retire(slot) => {
                if let (Some(group), Some(original)) =
                    (groups.get_mut(*slot), originals.get_mut(*slot))
                {
                    retire_original_workers(group, original, done);
                }
            }
            Command::OriginalRetire(slot) => {
                if let (Some(group), Some(original)) =
                    (groups.get_mut(*slot), originals.get_mut(*slot))
                {
                    original.admission.close_open();
                    original.refused = false;
                    retire_original(group, original, done, &mut completed, true);
                }
            }
            Command::OriginalPublished(slot) if stopping => {
                if let (Some(group), Some(original)) =
                    (groups.get_mut(*slot), originals.get_mut(*slot))
                    && !original.refused
                {
                    retire_original(group, original, done, &mut completed, false);
                }
            }
            Command::OriginalPublished(_) => {}
            Command::Stop => {
                stopping = true;
                stop_original_groups(&mut groups, &mut originals, done, &mut completed);
            }
        }
    }
    // Every admission is either closed before publication or its file was fenced and
    // closed. No typed queued resource can arrive after this receiver is destroyed.
    drop(incoming);
}

fn stop_original_groups<F: Send + 'static, W: OriginalFence>(
    groups: &mut [Group],
    originals: &mut [OriginalGroup<F, W>],
    done: CellRef,
    completed: &mut usize,
) {
    for (group, original) in groups.iter_mut().zip(originals) {
        original.admission.close_open();
        if !original.refused {
            retire_original(group, original, done, completed, false);
        }
    }
}
