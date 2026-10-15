//! Tickets: how a caller hears its answer (mantle docs/design/node.md §1.3, note 32 §3.9).
//!
//! Every call into the log's owner carries a ticket, and the owner answers through it once. The
//! answer travels a reply port of the caller's own, a bounded channel that only the caller
//! receives on, so an answer unparks the one thread that waits for it and no other; a caller that
//! waits as a task instead gives a [`Waker`], which the owner wakes once, after the answer, so a
//! completion wakes only its submitter (research/26 §5.3). Nothing is shared but the channel the
//! answer moves through.
//!
//! A port is made once and reused. The caller keeps both its ends and gives the ticket a copy of
//! the sending end, which the answer uses up, so a steady caller makes no channel per call. Each
//! thread keeps the ports it has finished with; it holds at most as many as it once had calls out
//! at a time, which the log's queue bounds for submissions and which is one for a call that waits
//! (`Log::view`, `Log::term`, ...). What goes through a port holds no port: a reply that carried a
//! port's sending end would hold a channel of its own kind, and its drop would run itself (mantle
//! CLAUDE.md §10).
//!
//! A caller that waits on its answer from the moment it submits may be handed its frame's I/O: it
//! is told so through its port (`Reply::Job`), takes the I/O from a job slot of its own
//! (`device::Slot`), does it on its own thread, as mantle's writer did on its, and hears its answer
//! when the I/O is done (`owner::Owner::start`). The I/O holds the tickets of the frame's callers,
//! so it never travels a port.

use std::cell::{Cell, RefCell};
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError, sync_channel};
use std::task::Waker;

use crate::group::Mirror;
use crate::{Entry, Fetched, LogError, View};

/// Replies one ticket carries at most: its admission, for a submission whose caller hears of it,
/// or word of its frame's I/O, for one whose caller waits on its answer and never hears of its
/// admission (the two are never the same ticket's); and its answer.
const REPLIES: usize = 2;

/// A reply through a ticket.
pub(crate) enum Reply {
    /// The submission holds its room in the queue; its answer follows once it is durable.
    Admitted,
    /// The call's answer.
    Answer(Result<Answer, LogError>),
    /// The I/O of the frame that carries the submission is in its caller's job slot, for the
    /// caller to do (`Waiting::wait_doing`).
    Job,
}

/// What a call is answered.
pub(crate) enum Answer {
    /// A submission is durable: a handle's write has its entries back, the log having written
    /// them; any other has none.
    Durable(Vec<Entry>),
    /// A group's handle: its state as the log holds it.
    Claimed(Box<Mirror>),
    Groups(Vec<u128>),
    View(Option<View>),
    Term(u64),
    Entries(Fetched),
    Flushed(u64, u64),
    Fenced(bool),
    Stats(Box<crate::LogStats>),
}

/// A port, both ends.
type Port = (SyncSender<Reply>, Receiver<Reply>);

thread_local! {
    /// This thread's ports, free for its next call.
    static PORTS: RefCell<Vec<Port>> = const { RefCell::new(Vec::new()) };
}

/// A free port of this thread's, or a new one.
pub(crate) fn port() -> Port {
    PORTS
        .try_with(|ports| ports.try_borrow_mut().ok().and_then(|mut p| p.pop()))
        .ok()
        .flatten()
        .unwrap_or_else(|| sync_channel(REPLIES))
}

/// A call's two sides, over a free port of this thread's or a new one: the ticket the owner answers
/// through, and the caller's wait for the answer.
pub(crate) fn call(waker: Option<Waker>) -> (Ticket, Waiting) {
    let (reply, receive) = port();
    let ticket = Ticket::new(reply.clone(), waker);
    (ticket, Waiting::new(reply, receive))
}

/// Keeps a port, answered and empty, for this thread's next call.
fn keep(port: Port) {
    let _ = PORTS.try_with(|ports| {
        if let Ok(mut ports) = ports.try_borrow_mut() {
            ports.push(port);
        }
    });
}

/// The owner's side of a call: where its replies go, and the waker to wake after its answer.
pub(crate) struct Ticket {
    reply: Option<SyncSender<Reply>>,
    waker: Option<Waker>,
}

impl Ticket {
    fn new(reply: SyncSender<Reply>, waker: Option<Waker>) -> Self {
        Self {
            reply: Some(reply),
            waker,
        }
    }

    /// The ticket, leaving one already answered in its place: the answer is someone else's to
    /// give now.
    pub(crate) fn take(&mut self) -> Self {
        Self {
            reply: self.reply.take(),
            waker: self.waker.take(),
        }
    }

    /// Where the caller hears its replies, for a caller that waits on them from the moment it
    /// submits: word of its frame's I/O may go there. `None` once answered.
    pub(crate) fn port(&self) -> Option<SyncSender<Reply>> {
        self.reply.clone()
    }

    /// Tells the caller its submission is admitted.
    pub(crate) fn admit(&self) {
        if let Some(reply) = &self.reply {
            // The port holds both replies, so this never finds it full; a caller that stopped
            // waiting has nothing to be told.
            let _ = reply.try_send(Reply::Admitted);
        }
    }

    /// Answers the call, once, and wakes its waker. The waker is the caller's code, run on
    /// whichever of the log's threads answers, holding the device or other callers' answers: it is
    /// called inside an unwind boundary of its own, so a waker that unwinds costs its caller only
    /// the wake, the answer already in its port (CLAUDE.md §1).
    pub(crate) fn answer(&mut self, answer: Result<Answer, LogError>) {
        if let Some(reply) = self.reply.take() {
            let _ = reply.try_send(Reply::Answer(answer));
        }
        if let Some(waker) = self.waker.take() {
            let _ = crate::unwind::contained(|| waker.wake());
        }
    }

    /// Whether the call has been answered.
    pub(crate) fn answered(&self) -> bool {
        self.reply.is_none()
    }
}

impl Drop for Ticket {
    fn drop(&mut self) {
        // Never answered: the log closed before it could be, and the caller hears so.
        if !self.answered() {
            self.answer(Err(LogError::Closed));
        }
    }
}

/// The caller's side of a call: its port, both ends, the sending end kept so that the port serves
/// the caller's next call once the answer has come.
pub(crate) struct Waiting {
    port: Option<Port>,
    /// The answer was taken: a later look finds the call closed.
    taken: Cell<bool>,
}

impl Waiting {
    fn new(reply: SyncSender<Reply>, receive: Receiver<Reply>) -> Self {
        Self {
            port: Some((reply, receive)),
            taken: Cell::new(false),
        }
    }

    /// Waits for the answer.
    pub(crate) fn wait(&self) -> Result<Answer, LogError> {
        // Only a caller that keeps a job slot is handed its frame's I/O (`Waiting::wait_doing`).
        self.wait_doing(|| {})
    }

    /// Waits for the answer, doing the frame's I/O when told it is in the caller's job slot:
    /// `run` takes it from there and does it.
    pub(crate) fn wait_doing(&self, mut run: impl FnMut()) -> Result<Answer, LogError> {
        let (_, receive) = self.port.as_ref().ok_or(LogError::Closed)?;
        if self.taken.get() {
            return Err(LogError::Closed);
        }
        loop {
            match receive.recv() {
                Ok(Reply::Admitted) => {}
                Ok(Reply::Job) => run(),
                Ok(Reply::Answer(answer)) => return self.took(answer),
                Err(_) => return Err(LogError::Closed),
            }
        }
    }

    /// The answer if it has come.
    pub(crate) fn poll(&self) -> Option<Result<Answer, LogError>> {
        let Some((_, receive)) = self.port.as_ref() else {
            return Some(Err(LogError::Closed));
        };
        if self.taken.get() {
            return Some(Err(LogError::Closed));
        }
        loop {
            match receive.try_recv() {
                // A caller that polls keeps no job slot, so it is never handed I/O.
                Ok(Reply::Admitted | Reply::Job) => {}
                Ok(Reply::Answer(answer)) => return Some(self.took(answer)),
                Err(TryRecvError::Empty) => return None,
                Err(TryRecvError::Disconnected) => return Some(Err(LogError::Closed)),
            }
        }
    }

    /// Waits for a submission's admission: `Ok` once admitted, its refusal otherwise.
    pub(crate) fn admitted(&self) -> Result<(), LogError> {
        let (_, receive) = self.port.as_ref().ok_or(LogError::Closed)?;
        loop {
            match receive.recv() {
                Ok(Reply::Admitted) => return Ok(()),
                // A caller that hears its admission keeps no job slot, so it is never handed I/O.
                Ok(Reply::Job) => {}
                Ok(Reply::Answer(answer)) => {
                    return self.took(answer).and(Err(LogError::Closed));
                }
                Err(_) => return Err(LogError::Closed),
            }
        }
    }

    fn took(&self, answer: Result<Answer, LogError>) -> Result<Answer, LogError> {
        self.taken.set(true);
        answer
    }
}

impl Drop for Waiting {
    fn drop(&mut self) {
        // Answered: the port is empty, so it serves the next call. A caller that leaves before its
        // answer leaves the port to the ticket, which answers into it and is gone with it.
        if let Some(port) = self.port.take()
            && self.taken.get()
        {
            keep(port);
        }
    }
}

/// An update's answer, once it is durable or refused.
pub struct Pending(pub(crate) Waiting);

impl std::fmt::Debug for Pending {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pending").finish_non_exhaustive()
    }
}

impl Pending {
    /// Waits until the update is durable, or refused.
    pub fn wait(&self) -> Result<(), LogError> {
        self.0.wait().and_then(durable)
    }

    /// The answer if it has come. An answer is given once; a look after it finds the call
    /// closed.
    pub fn poll(&self) -> Option<Result<(), LogError>> {
        self.0.poll().map(|a| a.and_then(durable))
    }
}

fn durable(answer: Answer) -> Result<(), LogError> {
    match answer {
        Answer::Durable(_) => Ok(()),
        _ => Err(LogError::Closed),
    }
}

/// A fetch of entries, answered with the caller's reservation filled.
pub struct Fetching(pub(crate) Waiting);

impl std::fmt::Debug for Fetching {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Fetching").finish_non_exhaustive()
    }
}

impl Fetching {
    /// Waits for the entries.
    pub fn wait(&self) -> Result<Fetched, LogError> {
        self.0.wait().and_then(fetched)
    }

    /// The entries if they have come.
    pub fn poll(&self) -> Option<Result<Fetched, LogError>> {
        self.0.poll().map(|a| a.and_then(fetched))
    }
}

fn fetched(answer: Answer) -> Result<Fetched, LogError> {
    match answer {
        Answer::Entries(f) => Ok(f),
        _ => Err(LogError::Closed),
    }
}
