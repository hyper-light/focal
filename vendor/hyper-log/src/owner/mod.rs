//! The log's owner: what the log knows, its groups, its segments, its queue's room and the
//! writer's batch, held by one thread at a time and read or changed by that thread alone (mantle
//! note 32 §3.9). Callers reach it by message through a bounded inbox and hear back through their
//! tickets.
//!
//! The log runs two threads: the owner's, which holds the owner and reads the inbox, and an I/O
//! thread (`device::serve`). The owner never does I/O: it hands each job, with the device, to a
//! thread that does it and goes on answering callers, so it answers callers while a frame is
//! flushed, as readers of mantle's lock did. A frame's I/O goes to a caller whose update the frame
//! carries and who waits on its answer from the moment it submitted, a blocking writer, which is
//! awake for its answer anyway and does the write, flush, confirmation and answers on its own
//! thread, as mantle's writer did on its; any other job goes to the I/O thread. The job comes back
//! through the owner's returns, which wake no one and which the owner reads before every message
//! (`device.rs`). So a write with no one else submitting crosses from its caller to the owner
//! and back, two hand-offs, and two threads sleep for it, the caller and the owner, as with
//! mantle's writer. The two threads that took turns holding the owner (Leader/Followers) woke the
//! one not flushing once more a write: it took the owner and slept on the inbox while the other
//! flushed.
//!
//! The writer's loop is mantle-log's (`writer.rs`), run as steps between messages (`write.rs`):
//! at the top of the loop, the batch is what was held for this frame and what is queued, that is
//! what the owner has taken from its inbox and admitted since the last batch. Queries and fetches
//! are answered as they come (`read.rs`).
//!
//! The owner keeps the buffers and collections a frame needs from one frame to the next, so the
//! write path allocates nothing a frame once they have grown to the largest frame's needs.

mod read;
mod write;

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::time::Instant;

use hyper_block::block::BlockFile;
use hyper_block::buf::AlignedBuf;
use hyper_block::commit::Anticipation;

use crate::codec::Writer as Payload;
use crate::device::{Answering, Carrier, Completion, Device, Io, Job, Look, Request};
use crate::format;
use crate::room::{Room, Take};
use crate::state::State;
use crate::ticket::{Answer, Reply, Ticket};
use crate::writer::{Key, Placement, Submission};
use crate::{LogError, Params};

use read::Fetch;
pub(crate) use read::Query;
use write::{Gather, Phase, Unconfirmed};

/// What reaches the owner.
pub(crate) enum Message<F> {
    /// A submission, admitted at once, held to wait for room when `wait`, or refused.
    Submit { submission: Submission, wait: bool },
    /// A question about the log's state, answered through its ticket.
    Query(Query, Ticket),
    /// A frame's word that it is flushed, when the device's slot for it was taken.
    Done(Completion),
    /// A job is back in the owner's returns with a completion the owner must answer.
    Returned,
    /// A job and the device back from the thread that did it, as the owner read them from its
    /// returns.
    Back(Box<Carrier<F>>),
    /// The I/O thread saw the job the owner waits for come back; the receiver of the returns'
    /// tokens comes back with the word.
    Watched(Receiver<u64>),
    /// A caller's look at the file, run by the thread that holds the device.
    Look(Look<F>),
    /// A group's handle is wanted (`Log::group`).
    Claim(u128, Ticket),
    /// A group's handle was dropped.
    Release(u128),
    /// The log is closing: the owner answers what it holds and ends.
    Close,
}

/// The writer's schedule: what it holds back, its fair queue, its wait for returning
/// submitters, and what it has written.
struct Schedule {
    /// Updates held for the next batch: those a frame passed over for room, and those whose
    /// group had an update in it.
    held: VecDeque<Submission>,
    /// Start-time fair queueing's virtual time, in charged bytes: the largest start tag laid
    /// into a frame, and the largest finish tag once the backlog empties [SFQ96 §2].
    virtual_time: u128,
    /// Each group's last finish tag, while it is ahead of the virtual time and the group is
    /// one the log holds or has an update waiting: at most `max_groups` and
    /// `queue_submissions` entries.
    finish: HashMap<u128, u128>,
    /// Frames laid out so far, which date a submission passed over.
    walks: u64,
    /// Frames written in a row that swept and carried no update while updates waited: at most
    /// `max_segments` before those waiting are refused `Full`.
    fruitless: u64,
    anticipation: Anticipation,
    /// Submissions taken into a batch so far.
    received: u64,
    /// Submissions the last confirmation answered, and those queued when it did.
    answered: u64,
    backlog: u64,
    /// The first batch is the restore of a lost frame at open, which goes in one frame or not
    /// at all (mantle docs/design/raft-log.md §6).
    restoring: bool,
    /// Frames written and flushed since the log opened, and the updates they carried.
    frames: u64,
    updates: u64,
}

/// What the write path keeps from one frame to the next so that it allocates nothing once
/// each has grown to the largest frame's needs: a frame's payload, its persist record, the
/// aligned buffers the device writes, and the batch's ordering scratch.
struct Buffers {
    payload: Payload,
    /// A persist record's groups and its encoding.
    persist: format::Persist,
    record_bytes: Payload,
    /// The aligned buffers the last frame and the last persist record were laid out in: one
    /// frame's bytes at most, a segment and a block.
    frame: Option<AlignedBuf>,
    record: Option<AlignedBuf>,
    /// The ordering's keys and each group's last key (`writer::order`).
    keyed: Vec<(Key, u64, Submission)>,
    last: HashMap<u128, Key>,
    /// Groups a frame has taken an update of.
    seen: HashSet<u128>,
    /// A batch's collection, kept between batches.
    batch: VecDeque<Submission>,
    /// Lists of a frame's updates and where they went, kept between frames: one for each frame
    /// the writer holds at once, the frame on the device and the frame awaiting its confirmation
    /// ([`SPARE_FRAMES`]).
    taken: Vec<Vec<(Submission, Placement)>>,
    /// Waiters room was just handed to.
    admitted: Vec<u64>,
    /// The aligned buffers frames' own confirmations were laid out in, kept for later frames':
    /// at most [`SPARE_FRAMES`], the frame awaiting its confirmation and the frame on the device.
    confirms: Vec<AlignedBuf>,
    /// Lists of a frame's answers, kept between frames: one for each frame whose answers are out
    /// at once, the frame confirmed by the one on the device, that frame, and the one laid out.
    answering: Vec<Vec<Answering>>,
    /// The terms and lengths of the entries of the frame laid out that its handles' writes take
    /// back with their answers: what publishing the frame needs of them.
    lens: Vec<(u64, u32)>,
}

/// Frames whose update lists the writer holds at once besides the one it lays out: the frame on
/// the device and the frame flushed and awaiting its confirmation (mantle
/// docs/design/raft-log.md §3).
const SPARE_FRAMES: usize = 2;

/// Lists of answers the writer keeps for later frames: one for each frame whose answers are out
/// at once, the frame awaiting its confirmation, the frame on the device, and the one laid out.
const ANSWER_LISTS: usize = SPARE_FRAMES.saturating_add(1);

/// What the owner is wired to: the device, where it tells the device another frame follows,
/// its inbox, where the device tells it of a flush, where jobs come back and their tokens, and
/// the I/O thread.
pub(crate) struct Wiring<F> {
    pub(crate) device: Device<F>,
    pub(crate) more: SyncSender<u64>,
    pub(crate) inbox: Receiver<Message<F>>,
    pub(crate) flushes: Receiver<Completion>,
    pub(crate) returns: Receiver<Box<Carrier<F>>>,
    pub(crate) tokens: Receiver<u64>,
    pub(crate) requests: SyncSender<Request>,
}

/// A job waiting for the device, and the port of the caller who waits on it and may do it.
type Waiting<F> = (Job<F>, Option<SyncSender<Reply>>);

pub(crate) struct Owner<F> {
    p: Params,
    state: State,
    room: Room,
    /// Admitted submissions not yet taken into a batch, in the order admitted.
    intake: VecDeque<Submission>,
    /// Submissions waiting for room, by arrival number: at most the room's waiters.
    waiting: HashMap<u64, Submission>,
    phase: Phase,
    gather: Option<Gather>,
    /// The owner waits for a submission to make the next batch of.
    parked: bool,
    schedule: Schedule,
    /// The last frame flushed, while nothing durable yet says its flush completed: the next
    /// frame's persist record will, or a confirmation written when no frame follows at once
    /// (mantle docs/design/raft-log.md §3, §6). Its updates are answered only then.
    unconfirmed: Option<Unconfirmed>,
    buffers: Buffers,
    fenced: bool,
    /// The device, while no thread does I/O with it.
    device: Option<Device<F>>,
    /// I/O waiting for the device, in the order asked: at most [`crate::device::JOBS`].
    io: VecDeque<Waiting<F>>,
    /// Where jobs come back, with the device, from the thread that did them; read before every
    /// message. It wakes no one.
    returns: Receiver<Box<Carrier<F>>>,
    /// Each job's sequence once it is back, while the I/O thread does not watch for one.
    tokens: Option<Receiver<u64>>,
    /// The I/O thread's requests.
    requests: SyncSender<Request>,
    /// The box jobs travel in, while no job is out.
    carrier: Option<Box<Carrier<F>>>,
    /// Jobs given out so far: the sequence of the last.
    jobs: u64,
    /// Where the owner tells the device that another frame follows the one it has.
    more: SyncSender<u64>,
    /// The owner has told the device so for the frame it has, or laid it out knowing.
    more_told: bool,
    /// The inbox, until the owner's thread takes it to read.
    inbox: Option<Receiver<Message<F>>>,
    /// The owner has taken its first step: the restore of a lost frame at open.
    started: bool,
    /// Messages a step's drain of the inbox stopped at, handled at the loop's top, in order: a
    /// completion, whose job the draining step may belong to, or a frame's flush, a job back and
    /// the message the drain took after them. At most three.
    deferred: VecDeque<Message<F>>,
    /// The device's word that a frame is flushed while it writes the frame's confirmation
    /// (`device.rs`). It wakes no one: the owner reads it before every message it takes from the
    /// inbox, since anything a caller sends after hearing of the frame comes after the word.
    flushes: Receiver<Completion>,
    /// Fetches waiting for the device, the first of them on it: one group's each at most for
    /// every group the log holds, past which a fetch is refused `Busy`.
    fetches: VecDeque<Fetch>,
    /// The lists of a fetch done, kept for the next.
    spare_reads: crate::device::Reads,
    /// Looks at the file waiting for the device, the first on it: one for each caller that
    /// waits in `Log::with_file`.
    looks: VecDeque<Look<F>>,
    looking: bool,
    closing: bool,
    /// The I/O thread watches for the job out to come back.
    watching: bool,
    /// Groups written through their handles: at most `max_groups`.
    claimed: HashSet<u128>,
    /// For each claimed group whose handle had a write refused, the epoch that write was sent
    /// in: the group's writes sent in it or before are refused [`LogError::Behind`]
    /// (`Owner::behind`). At most one entry a claimed group, gone with the claim.
    refused: HashMap<u128, u64>,
}

impl<F: BlockFile + 'static> Owner<F> {
    pub(crate) fn new(
        p: Params,
        state: State,
        room: Room,
        restores: Vec<Submission>,
        wiring: Wiring<F>,
    ) -> Self {
        let Wiring {
            device,
            more,
            inbox,
            flushes,
            returns,
            tokens,
            requests,
        } = wiring;
        Self {
            p,
            state,
            room,
            intake: VecDeque::new(),
            waiting: HashMap::new(),
            phase: Phase::Idle,
            gather: None,
            parked: false,
            schedule: Schedule::new(restores),
            unconfirmed: None,
            buffers: Buffers::new(),
            fenced: false,
            device: Some(device),
            io: VecDeque::new(),
            returns,
            tokens: Some(tokens),
            requests,
            carrier: None,
            jobs: 0,
            more,
            more_told: false,
            inbox: Some(inbox),
            started: false,
            deferred: VecDeque::new(),
            flushes,
            fetches: VecDeque::new(),
            spare_reads: crate::device::Reads::none(),
            looks: VecDeque::new(),
            looking: false,
            closing: false,
            watching: false,
            claimed: HashSet::new(),
            refused: HashMap::new(),
        }
    }

    /// Runs the owner until the log closes and nothing is left to answer: the file, unless the
    /// device was lost.
    pub(crate) fn lead(mut self) -> Option<F> {
        let inbox = self.inbox.take()?;
        if !self.started {
            self.started = true;
            self.step(&inbox);
        }
        loop {
            if self.device.is_some()
                && let Some((job, doer)) = self.io.pop_front()
                && let Some(device) = self.device.take()
            {
                self.start(job, doer, device);
                continue;
            }
            if let Some(message) = self.deferred.pop_front() {
                self.handle(message, &inbox);
                continue;
            }
            if self.arrived() {
                continue;
            }
            if self.finished() {
                break;
            }
            self.watch();
            match self.next(&inbox) {
                // What came back before the message is heard first.
                Ok(Some(m)) => {
                    if self.arrived() {
                        self.deferred.push_back(m);
                    } else {
                        self.handle(m, &inbox);
                    }
                }
                Ok(None) => self.gathered(&inbox),
                Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => break,
            }
        }
        self.device.take().map(Device::into_file)
    }

    /// Hands a job and the device to the caller waiting on it, or else to the I/O thread.
    fn start(&mut self, job: Job<F>, doer: Option<SyncSender<Reply>>, device: Device<F>) {
        self.jobs = self.jobs.wrapping_add(1);
        let carrier = match self.carrier.take() {
            Some(mut c) => {
                c.job = Some(job);
                c.device = Some(device);
                c.completion = None;
                c.sequence = self.jobs;
                c
            }
            None => Box::new(Carrier::new(job, device, self.jobs)),
        };
        let mut io: Box<dyn Io> = carrier;
        if let Some(port) = doer {
            match port.try_send(Reply::Io(io)) {
                Ok(()) => return,
                // The caller stopped waiting: the I/O thread does the job.
                Err(TrySendError::Full(r) | TrySendError::Disconnected(r)) => match r {
                    Reply::Io(back) => io = back,
                    _ => return,
                },
            }
        }
        // Never full: the I/O thread holds at most this job and a watch (`device::REQUESTS`).
        let _ = self.requests.send(Request::Job(io));
    }

    /// What came back from the device since the owner last looked, queued to be heard in order:
    /// a frame's word that it is flushed, then the job back. Whether anything came.
    fn arrived(&mut self) -> bool {
        let back = self.returns.try_recv().ok();
        // A job back was sent after the word of its frame's flush, so the word is there to be
        // read once the job is.
        let word = self.flushes.try_recv().ok();
        let came = back.is_some() || word.is_some();
        if let Some(word) = word {
            self.deferred.push_back(Message::Done(word));
        }
        if let Some(back) = back {
            self.deferred.push_back(Message::Back(back));
        }
        came
    }

    /// Asks the I/O thread to wake the owner once the job out is back, when the owner has work
    /// waiting on the device: the job's thread wakes the owner only for a completion the owner
    /// must answer.
    fn watch(&mut self) {
        if self.device.is_some() || self.watching || !self.waits_on_device() {
            return;
        }
        if let Some(tokens) = self.tokens.take() {
            match self.requests.send(Request::Watch(self.jobs, tokens)) {
                Ok(()) => self.watching = true,
                Err(e) => {
                    if let Request::Watch(_, tokens) = e.0 {
                        self.tokens = Some(tokens);
                    }
                }
            }
        }
    }

    /// Whether anything the owner holds waits on the job out to come back: submissions for the
    /// next frame, I/O waiting for the device, waiters for room the job will free, or a close.
    fn waits_on_device(&self) -> bool {
        !self.intake.is_empty()
            || !self.schedule.held.is_empty()
            || !self.io.is_empty()
            || !self.waiting.is_empty()
            || !self.fetches.is_empty()
            || !self.looks.is_empty()
            || self.closing
    }

    /// The next message, or `None` once the writer's wait for returning submitters is over;
    /// an error once the inbox has closed.
    #[allow(
        clippy::disallowed_methods,
        reason = "hyper-log's owner waits on its own deadline (CLAUDE.md §1, sans-io's one exception)"
    )]
    fn next(&self, inbox: &Receiver<Message<F>>) -> Result<Option<Message<F>>, RecvTimeoutError> {
        let Some(deadline) = self.gather.as_ref().map(|g| g.deadline) else {
            return inbox
                .recv()
                .map(Some)
                .map_err(|_| RecvTimeoutError::Disconnected);
        };
        let left = deadline.saturating_duration_since(Instant::now());
        match inbox.recv_timeout(left) {
            Ok(m) => Ok(Some(m)),
            Err(RecvTimeoutError::Timeout) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Whether the log has closed and the owner holds nothing more to answer.
    fn finished(&self) -> bool {
        self.closing
            && self.parked
            && self.intake.is_empty()
            && self.fetches.is_empty()
            && self.looks.is_empty()
            && !self.looking
            && self.io.is_empty()
            && self.device.is_some()
            && self.deferred.is_empty()
    }

    fn handle(&mut self, message: Message<F>, inbox: &Receiver<Message<F>>) {
        self.take(message, inbox);
        self.went_on(inbox);
    }

    /// Acts on one message.
    fn take(&mut self, message: Message<F>, inbox: &Receiver<Message<F>>) {
        match message {
            Message::Submit { submission, wait } => self.submit(submission, wait),
            Message::Query(query, ticket) => self.query(query, ticket),
            Message::Done(word) => self.done(word, inbox),
            Message::Back(mut back) => {
                let completion = back.completion.take();
                self.device = back.device.take();
                self.carrier = Some(back);
                // Tokens of jobs back are for a watch; none is wanted once the job is heard.
                if let Some(tokens) = &self.tokens {
                    while tokens.try_recv().is_ok() {}
                }
                if let Some(completion) = completion {
                    self.done(completion, inbox);
                }
            }
            Message::Returned => {}
            Message::Watched(tokens) => {
                // Tokens of jobs back are for a watch: none is wanted until the next.
                while tokens.try_recv().is_ok() {}
                self.tokens = Some(tokens);
                self.watching = false;
            }
            Message::Look(look) => {
                self.looks.push_back(look);
                self.look();
            }
            Message::Claim(group, ticket) => self.claim(group, ticket),
            Message::Release(group) => {
                self.claimed.remove(&group);
                self.refused.remove(&group);
            }
            Message::Close => self.closing = true,
        }
    }

    /// Handles every message already waiting, without waiting for more. It stops at a completion,
    /// since the step that drains may be the one the completion's job belongs to, and at a word
    /// that a frame is flushed or a job back, which come before the message taken with them:
    /// those, and what follows them, are handled at the loop's top, in order.
    fn drain(&mut self, inbox: &Receiver<Message<F>>) {
        if !self.deferred.is_empty() {
            return;
        }
        loop {
            let Ok(message) = inbox.try_recv() else {
                return;
            };
            if self.arrived() {
                self.deferred.push_back(message);
                return;
            }
            if matches!(message, Message::Done(..)) {
                self.deferred.push_back(message);
                return;
            }
            self.take(message, inbox);
        }
    }

    /// Admits a submission, holds it to wait for room, or refuses it.
    fn submit(&mut self, mut s: Submission, wait: bool) {
        if self.fenced {
            s.ticket.answer(Err(LogError::Fenced));
            return;
        }
        if !s.handle && self.claimed.contains(&s.group) {
            s.ticket.answer(Err(LogError::Claimed(s.group)));
            return;
        }
        let mut admitted = std::mem::take(&mut self.buffers.admitted);
        match self.room.take(s.group, s.bytes, wait, &mut admitted) {
            Ok(Take::Admitted) => {
                // The device hears that another frame follows before the caller hears it is
                // admitted: whatever the caller does on hearing it, releasing a held flush among
                // them, comes after the word.
                let admit = s.admit;
                self.intake.push_back(s);
                self.tell_more();
                if admit && let Some(s) = self.intake.back() {
                    s.ticket.admit();
                }
            }
            Ok(Take::Waiting(seq)) => {
                self.waiting.insert(seq, s);
            }
            Err(e) => {
                self.note_refusal(&s);
                s.ticket.answer(Err(e));
            }
        }
        self.let_in(&mut admitted);
        self.buffers.admitted = admitted;
    }

    /// Tells the waiters room was handed to, each through its own ticket, and queues them.
    fn let_in(&mut self, admitted: &mut Vec<u64>) {
        for seq in admitted.drain(..) {
            if let Some(s) = self.waiting.remove(&seq) {
                let admit = s.admit;
                self.intake.push_back(s);
                self.tell_more();
                if admit && let Some(s) = self.intake.back() {
                    s.ticket.admit();
                }
            }
        }
    }

    /// Answers a submission, and gives back its room in the queue to the waiters it fits.
    fn answer(&mut self, mut s: Submission, result: Result<(), LogError>) {
        if result.is_err() {
            self.note_refusal(&s);
        }
        let back = match &result {
            Ok(()) if s.handle => crate::group::given_back(&mut s.update),
            _ => Vec::new(),
        };
        s.ticket.answer(result.map(|()| Answer::Durable(back)));
        let mut admitted = std::mem::take(&mut self.buffers.admitted);
        self.room.release(s.group, s.bytes, &mut admitted);
        self.let_in(&mut admitted);
        self.buffers.admitted = admitted;
    }

    /// A handle's write was refused: every write its handle sent in the same epoch or before is
    /// refused after it (`Owner::behind`).
    fn note_refusal(&mut self, s: &Submission) {
        if s.handle && self.claimed.contains(&s.group) {
            let epoch = self.refused.entry(s.group).or_insert(s.epoch);
            *epoch = (*epoch).max(s.epoch);
        }
    }

    /// Whether `s` was sent by its handle before the handle heard of a refusal of an earlier
    /// write: then it is refused too. A write of a later epoch ends the refusal.
    pub(super) fn behind(&mut self, s: &Submission) -> bool {
        if !s.handle {
            return false;
        }
        match self.refused.get(&s.group) {
            Some(&epoch) if s.epoch <= epoch => true,
            Some(_) => {
                self.refused.remove(&s.group);
                false
            }
            None => false,
        }
    }

    /// Hands out `group`'s handle: its state as the log holds it, with the bytes of the entries
    /// the log keeps in memory, which the handle keeps from here on.
    fn claim(&mut self, group: u128, mut ticket: Ticket) {
        let answer = if self.state.damaged.contains_key(&group) {
            Err(LogError::Damaged(
                "the group's acknowledged records are damaged; it recovers from its peers",
            ))
        } else if self.claimed.contains(&group) {
            Err(LogError::Claimed(group))
        } else if self.claimed.len() >= self.p.config.max_groups {
            Err(LogError::TooManyGroups(self.p.config.max_groups))
        } else {
            self.claimed.insert(group);
            let mirror = match self.state.groups.get_mut(&group) {
                Some(g) => crate::group::Mirror::of(g),
                None => crate::group::Mirror::none(),
            };
            Ok(Answer::Claimed(Box::new(mirror)))
        };
        ticket.answer(answer);
    }

    /// Fences the log: no submission is taken from here on, and every waiter hears it.
    fn fence(&mut self) {
        self.fenced = true;
        let mut fenced = Vec::new();
        self.room.fence(&mut fenced);
        for seq in fenced {
            if let Some(mut s) = self.waiting.remove(&seq) {
                s.ticket.answer(Err(LogError::Fenced));
            }
        }
    }

    fn done(&mut self, completion: Completion, inbox: &Receiver<Message<F>>) {
        match completion {
            Completion::Sweep(swept) => self.swept(swept, inbox),
            Completion::Frame {
                frame,
                record,
                confirm,
                result,
                took_ns,
                confirming,
                before,
                these,
            } => {
                self.buffers.frame = Some(frame);
                self.buffers.record = Some(record);
                let done = write::Flushed {
                    result,
                    took_ns,
                    confirming,
                    confirm,
                    before,
                    these,
                };
                self.written(done, inbox);
            }
            Completion::Confirm {
                record,
                result,
                these,
            } => {
                self.give_confirm(record);
                self.confirmed(result, these, inbox);
            }
            Completion::Read(reads) => self.read(reads),
            Completion::Looked => {
                self.looking = false;
                self.look();
            }
        }
    }

    /// Hands the device the next caller's look at the file, one at a time.
    fn look(&mut self) {
        if self.looking {
            return;
        }
        let Some(look) = self.looks.pop_front() else {
            return;
        };
        self.io.push_back((Job::Look(look), None));
        self.looking = true;
    }

    /// A submission was admitted while a frame is on the device: another frame follows it, so
    /// the device leaves the frame's confirmation to the next one's record, as the writer did
    /// when it found submissions queued (mantle docs/design/raft-log.md §3).
    fn tell_more(&mut self) {
        if self.more_told {
            return;
        }
        if let write::Phase::Writing(w) = &self.phase {
            // Full only with a word about the frame before still unread, which the device drops
            // as it reads this one: one is always room for this frame's.
            self.more_told = self.more.try_send(w.sequence()).is_ok();
        }
    }

    /// Keeps an emptied list of answers for a later frame, while the writer holds fewer than
    /// it can use.
    fn keep_answers(&mut self, list: Vec<Answering>) {
        if list.capacity() > 0 && self.buffers.answering.len() < ANSWER_LISTS {
            self.buffers.answering.push(list);
        }
    }
}

impl Schedule {
    fn new(restores: Vec<Submission>) -> Self {
        Self {
            restoring: !restores.is_empty(),
            held: restores.into(),
            virtual_time: 0,
            finish: HashMap::new(),
            walks: 0,
            fruitless: 0,
            anticipation: Anticipation::new(),
            received: 0,
            answered: 0,
            backlog: 0,
            frames: 0,
            updates: 0,
        }
    }
}

impl Buffers {
    fn new() -> Self {
        Self {
            payload: Payload::default(),
            persist: format::Persist::default(),
            record_bytes: Payload::default(),
            frame: None,
            record: None,
            keyed: Vec::new(),
            last: HashMap::new(),
            seen: HashSet::new(),
            batch: VecDeque::new(),
            taken: Vec::new(),
            admitted: Vec::new(),
            confirms: Vec::new(),
            answering: Vec::new(),
            lens: Vec::new(),
        }
    }
}
