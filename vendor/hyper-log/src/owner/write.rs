//! The write path, mantle-log's writer loop as steps between messages (mantle
//! docs/design/raft-log.md §3, §5, §6).
//!
//! At the top of the loop the batch is what was held for this frame and what is queued. When
//! nothing is, the last frame is confirmed on its own, or the owner waits for a submission and
//! takes the first that comes. Under `Waits::Measured` it then waits for the submitters the last
//! frame answered (`hyper_block::commit`), and commits: a sweep of the tail is read on the device,
//! the batch is laid into a frame after the sweep's copies, and the frame is written and flushed
//! on the device; once it is, the frame is published, the frame before it answered, and the loop
//! goes round. Mantle's writer did each of these in turn on its own thread, blocking on the
//! device; here the owner hands each I/O to the device, done by a caller of the frame that waits
//! on its answer or by the log's I/O thread (`owner.rs`), and goes on answering callers until it
//! comes back, and the order of everything the log decides is the same. The device finishes a frame as the writer did after its flush
//! (`device.rs`): the frame before is answered, its confirmation being this frame's record, and
//! this frame confirmed on its own unless the owner has said another follows, which is when
//! submissions were admitted while the frame was on the device: what the writer found queued
//! when its flush returned.

use std::collections::VecDeque;
use std::sync::mpsc::Receiver;
use std::time::{Duration, Instant};

use hyper_block::block::BlockFile;
use hyper_block::buf::AlignedBuf;

use super::{Message, Owner};
use crate::codec::Writer as Payload;
use crate::device::{Answering, Frame, Job, Swept};
use crate::format::{self, SegmentHeader};
use crate::stats::Timing;
use crate::ticket::Answer;
use crate::writer::{self, Placement, Submission, Sweep, SweepRead, Target};
use crate::{LogError, Waits};

/// A frame flushed and not yet confirmed, and the updates it carried: at most one frame's. Its
/// answers are the owner's while no frame on the device carries them.
pub(super) struct Unconfirmed {
    sequence: u64,
    updates: Vec<(Submission, Placement)>,
    answering: Vec<Answering>,
    /// The frame's own confirmation, laid out with the frame.
    confirm: Option<AlignedBuf>,
}

/// What the writer is waiting on the device for.
pub(super) enum Phase {
    /// Nothing: the loop runs at its next step.
    Idle,
    /// The tail's frames, read for a sweep, before the batch is laid out after the copies.
    Sweeping {
        batch: VecDeque<Submission>,
        whole: bool,
        read: SweepRead,
    },
    /// A frame's write and flush.
    Writing(Writing),
    /// A confirmation's write and flush; `forget` once it is done when it followed a commit.
    Confirming { frame: Unconfirmed, forget: bool },
}

/// What the device says of a frame's flush (`device::Completion::Frame`).
pub(super) struct Flushed {
    pub(super) result: Result<(), LogError>,
    pub(super) timing: Timing,
    /// The device is confirming the frame on its own.
    pub(super) confirming: bool,
    /// The frame's own confirmation, unless the device is writing it.
    pub(super) confirm: AlignedBuf,
    pub(super) before: Vec<Answering>,
    pub(super) these: Vec<Answering>,
}

/// A frame on the device.
pub(super) struct Writing {
    taken: Vec<(Submission, Placement)>,
    sweep: Option<Sweep>,
    target: Target,
    tail: u64,
    sequence: u64,
}

impl Writing {
    /// The frame's sequence.
    pub(super) fn sequence(&self) -> u64 {
        self.sequence
    }
}

/// The writer's wait for the submitters the last frame answered (`hyper_block::commit`).
pub(super) struct Gather {
    batch: VecDeque<Submission>,
    /// Submissions in the batch sent before the answers went out.
    before: u64,
    answered: u64,
    gathered: u64,
    pub(super) deadline: Instant,
}

/// A frame laid out: its payload and record count, the sweep before its updates, and the
/// updates with where they went.
struct Laid {
    records: u32,
    sweep: Option<Sweep>,
    taken: Vec<(Submission, Placement)>,
}

impl<F: BlockFile + 'static> Owner<F> {
    /// After a message: a submission that came while the owner was parked starts a batch, and
    /// one that came while it gathered joins the batch.
    pub(super) fn went_on(&mut self, inbox: &Receiver<Message<F>>) {
        if self.intake.is_empty() {
            return;
        }
        if self.parked {
            self.parked = false;
            let mut batch = std::mem::take(&mut self.buffers.batch);
            self.take_intake(&mut batch);
            let (answered, before) = (self.schedule.answered, self.schedule.backlog);
            self.gather_then_commit(batch, answered, before, inbox);
        } else if self.gather.is_some() {
            self.join_gathering();
            self.keep_gathering(inbox);
        }
    }

    /// Moves what is queued into `batch`, each stamped as taken.
    fn take_intake(&mut self, batch: &mut VecDeque<Submission>) {
        while let Some(s) = self.intake.pop_front() {
            let s = self.taken(s);
            batch.push_back(s);
        }
    }

    /// Adds what came while the writer gathered to the batch it gathers.
    fn join_gathering(&mut self) {
        while let Some(s) = self.intake.pop_front() {
            let s = self.taken(s);
            if let Some(g) = self.gather.as_mut() {
                g.gathered = g.gathered.saturating_add(s.bytes);
                g.batch.push_back(s);
            }
        }
    }

    /// The top of mantle's loop: the batch is what was held and what is queued.
    pub(super) fn step(&mut self, inbox: &Receiver<Message<F>>) {
        loop {
            if !matches!(self.phase, Phase::Idle) || self.gather.is_some() || self.parked {
                return;
            }
            self.drain(inbox);
            let mut batch = std::mem::take(&mut self.schedule.held);
            let held = u64::try_from(batch.len()).unwrap_or(u64::MAX);
            self.schedule.held = std::mem::take(&mut self.buffers.batch);
            self.take_intake(&mut batch);
            if !batch.is_empty() {
                let (answered, backlog) = (self.schedule.answered, self.schedule.backlog);
                self.gather_then_commit(batch, answered, backlog.saturating_add(held), inbox);
                return;
            }
            self.buffers.batch = batch;
            if !self.idle(inbox) {
                return;
            }
        }
    }

    /// Nothing is queued: the last frame is confirmed on its own, at once, since no frame would
    /// carry the confirmation; or, with none to confirm, the owner parks. Whether the loop goes
    /// round again at once.
    fn idle(&mut self, inbox: &Receiver<Message<F>>) -> bool {
        if self.unconfirmed.is_some() {
            if self.confirm(false) {
                return false;
            }
            self.schedule.backlog = self.queued(inbox);
            return true;
        }
        self.end_busy_period();
        self.parked = true;
        false
    }

    /// The backlog is empty: the busy period ends at the largest finish tag [SFQ96 §2], and no
    /// group's past service counts against it any longer.
    fn end_busy_period(&mut self) {
        let schedule = &mut self.schedule;
        let last = schedule.finish.values().copied().max().unwrap_or(0);
        schedule.virtual_time = schedule.virtual_time.max(last);
        schedule.finish.clear();
    }

    /// Submissions queued when the device answered: every message sent before the device's word
    /// has been heard before it (the inbox in order, the side channel before each message), so
    /// they are those taken. What comes after the word comes from callers who may have heard
    /// their answers, and joins the next batch at the next step.
    fn backlog_at_answers(&self) -> u64 {
        u64::try_from(self.intake.len()).unwrap_or(u64::MAX)
    }

    /// Submissions sent before the answers about to go out: they are queued ahead of any the
    /// answered submitters send next.
    fn queued(&mut self, inbox: &Receiver<Message<F>>) -> u64 {
        self.drain(inbox);
        u64::try_from(self.intake.len()).unwrap_or(u64::MAX)
    }

    /// Counts a submission the writer took. Its room in the queue is held until it is
    /// answered, so the queue's bound covers the updates held for a later frame and those in
    /// the frame being written as well as those waiting (mantle audit S03).
    ///
    /// It is stamped for the fair queue: its start tag is the larger of the virtual time and
    /// its group's last finish tag, and its finish tag its start plus its charge [SFQ96 §2
    /// eqs. 4–5]. Every group weighs the same, so a group's share of a full frame's bytes is the
    /// others' (Theorem 1); the class orders tiers instead of weighting them.
    fn taken(&mut self, mut s: Submission) -> Submission {
        let schedule = &mut self.schedule;
        schedule.received = schedule.received.saturating_add(1);
        let start = schedule
            .finish
            .get(&s.group)
            .map_or(schedule.virtual_time, |&f| f.max(schedule.virtual_time));
        // Charged bytes over the log's life stay far below 2^128: saturation is unreachable,
        // and would only order the group last.
        schedule
            .finish
            .insert(s.group, start.saturating_add(u128::from(s.bytes)));
        s.tags = writer::Tags {
            seq: schedule.received,
            start,
            passed: None,
        };
        s
    }

    /// Waits for the submitters the last confirmation answered while waiting is expected to
    /// lower total latency, then commits. The first `before` submissions in the batch were
    /// sent before the answers.
    #[allow(
        clippy::disallowed_methods,
        reason = "hyper-log's owner times its own writes (CLAUDE.md §1, sans-io's one exception)"
    )]
    fn gather_then_commit(
        &mut self,
        batch: VecDeque<Submission>,
        answered: u64,
        before: u64,
        inbox: &Receiver<Message<F>>,
    ) {
        if answered == 0 || self.p.config.waits == Waits::Never {
            self.commit_unless_fenced(batch, inbox);
            return;
        }
        let gathered = batch
            .iter()
            .fold(0u64, |sum, s| sum.saturating_add(s.bytes));
        self.gather = Some(Gather {
            batch,
            before,
            answered,
            gathered,
            deadline: Instant::now(),
        });
        self.keep_gathering(inbox);
    }

    /// Waits on for one more submitter, or ends the gathering. A batch that holds a frame's
    /// worth already is not waited on: what came next would go in a later frame whatever the
    /// wait.
    fn keep_gathering(&mut self, inbox: &Receiver<Message<F>>) {
        let full = u64::try_from(self.p.frame_room).unwrap_or(u64::MAX);
        let limit = self.p.config.queue_submissions;
        let step = match &self.gather {
            Some(g) if returned(g) < g.answered && g.batch.len() < limit && g.gathered < full => {
                self.schedule.anticipation.wait(g.batch.len())
            }
            _ => None,
        };
        match (step, self.gather.as_mut()) {
            (Some(step), Some(g)) => g.deadline = deadline(step),
            _ => self.gathered(inbox),
        }
    }

    /// The gathering is over: the writer learns how many returned and commits the batch.
    pub(super) fn gathered(&mut self, inbox: &Receiver<Message<F>>) {
        let Some(g) = self.gather.take() else {
            return;
        };
        self.schedule.anticipation.learn(g.answered, returned(&g));
        self.commit_unless_fenced(g.batch, inbox);
    }

    fn commit_unless_fenced(&mut self, batch: VecDeque<Submission>, inbox: &Receiver<Message<F>>) {
        if self.fenced {
            let mut batch = batch;
            for s in batch.drain(..) {
                self.answer(s, Err(LogError::Fenced));
            }
            self.buffers.batch = batch;
            self.step(inbox);
            return;
        }
        self.commit(batch, inbox);
    }

    /// Commits a batch: a sweep of the tail first if it is due, then the frame.
    fn commit(&mut self, batch: VecDeque<Submission>, inbox: &Receiver<Message<F>>) {
        let whole = std::mem::replace(&mut self.schedule.restoring, false);
        let read = writer::sweepable(&self.state, &self.p).and_then(|due| {
            due.then(|| writer::sweep_read(&self.state, &self.p))
                .transpose()
        });
        match read {
            Ok(Some(read)) => {
                let job = Job::Sweep {
                    segment: read.segment,
                    offset: read.offset,
                    end: read.end,
                };
                self.phase = Phase::Sweeping { batch, whole, read };
                self.dispatch(job);
            }
            Ok(None) => {
                let mut payload = std::mem::take(&mut self.buffers.payload);
                payload.clear();
                self.lay_out(batch, whole, None, payload, 0, inbox);
            }
            Err(e) => self.failed(batch, Vec::new(), e, inbox),
        }
    }

    /// Hands a write-path job to the device: one at a time, the owner waiting for each.
    fn dispatch(&mut self, job: Job<F>) {
        self.io.push_back((job, None));
    }

    /// The tail is read: its live pieces go first in the payload, then the batch.
    pub(super) fn swept(
        &mut self,
        swept: Result<Vec<Swept>, LogError>,
        inbox: &Receiver<Message<F>>,
    ) {
        let Phase::Sweeping { batch, whole, read } =
            std::mem::replace(&mut self.phase, Phase::Idle)
        else {
            return;
        };
        let mut payload = std::mem::take(&mut self.buffers.payload);
        payload.clear();
        let mut records = 0u32;
        let sweep = swept.and_then(|frames| {
            writer::sweep(
                &self.state,
                &self.p,
                &read,
                &frames,
                &mut payload,
                &mut records,
            )
        });
        match sweep {
            Ok(sweep) => self.lay_out(batch, whole, Some(sweep), payload, records, inbox),
            Err(e) => self.failed(batch, Vec::new(), e, inbox),
        }
    }

    /// Lays the batch into a frame after any sweep, and hands the frame to the device.
    fn lay_out(
        &mut self,
        mut batch: VecDeque<Submission>,
        whole: bool,
        sweep: Option<Sweep>,
        mut payload: Payload,
        records: u32,
        inbox: &Receiver<Message<F>>,
    ) {
        self.schedule.walks = self.schedule.walks.saturating_add(1);
        writer::order(&mut batch, &mut self.buffers.keyed, &mut self.buffers.last);
        let mut laid = Laid {
            records,
            sweep,
            taken: self.buffers.taken.pop().unwrap_or_default(),
        };
        let refused = match self.take_batch(&mut batch, &mut payload, &mut laid) {
            Ok(refused) => refused,
            Err(e) => {
                self.buffers.payload = payload;
                self.failed(batch, laid.taken, e, inbox);
                return;
            }
        };
        self.buffers.batch = batch;
        if whole && (refused || !self.schedule.held.is_empty()) {
            self.buffers.payload = payload;
            self.restore_in_parts(laid.taken, inbox);
            return;
        }
        if laid.taken.is_empty() && laid.sweep.is_none() {
            self.buffers.payload = payload;
            self.keep_list(laid.taken);
            self.no_frame(inbox);
            return;
        }
        self.place(payload, laid, inbox);
    }

    /// Lays each update of the ordered batch that the frame takes; passes over, holds or
    /// refuses the rest. Each update laid out moves from `batch` to `laid`; refusals are answered
    /// as they are refused. Whether any was refused; an update that does not encode leaves the
    /// rest of the batch for the commit's failure.
    fn take_batch(
        &mut self,
        batch: &mut VecDeque<Submission>,
        payload: &mut Payload,
        laid: &mut Laid,
    ) -> Result<bool, LogError> {
        let mut refused = false;
        let mut seen = std::mem::take(&mut self.buffers.seen);
        seen.clear();
        let mut new_groups = 0usize;
        while let Some(s) = batch.pop_front() {
            if !seen.insert(s.group) {
                self.schedule.held.push_back(s);
                continue;
            }
            match self.lay(s, payload, laid, new_groups, batch) {
                Ok(Some(new)) => new_groups = new_groups.saturating_add(usize::from(new)),
                Ok(None) => refused = true,
                Err(e) => {
                    self.buffers.seen = seen;
                    return Err(e);
                }
            }
        }
        self.buffers.seen = seen;
        Ok(refused)
    }

    /// Lays one update into the frame: `Some(new group)` once laid or held for room, `None` if
    /// refused and answered. One that does not encode goes back to the front of `batch`.
    fn lay(
        &mut self,
        mut s: Submission,
        payload: &mut Payload,
        laid: &mut Laid,
        new_groups: usize,
        batch: &mut VecDeque<Submission>,
    ) -> Result<Option<bool>, LogError> {
        if self.behind(&s) {
            let group = s.group;
            self.answer(s, Err(LogError::Behind(group)));
            return Ok(None);
        }
        let (new, len) = match self.fits(&s, new_groups) {
            Ok(checked) => checked,
            Err(e) => {
                self.answer(s, Err(e));
                return Ok(None);
            }
        };
        if payload.len().saturating_add(len) > self.p.frame_room {
            // The group stays taken for this frame: its later updates wait behind this one, so
            // its updates become durable in the order submitted. Passed over, it goes ahead of
            // every class from the next frame on, behind only those passed over before it.
            s.tags.passed.get_or_insert(self.schedule.walks);
            self.schedule.held.push_back(s);
            return Ok(Some(false));
        }
        let Some(placement) =
            writer::encode(payload, &mut laid.records, s.group, &s.update, s.marks)
        else {
            batch.push_front(s);
            return Err(LogError::TooLarge(len));
        };
        self.schedule.virtual_time = self.schedule.virtual_time.max(s.tags.start);
        laid.taken.push((s, placement));
        Ok(Some(new))
    }

    /// Whether `s` may be written as its group stands and fits a frame: whether it makes a new
    /// group, and the payload bytes its records take.
    fn fits(&self, s: &Submission, new_groups: usize) -> Result<(bool, usize), LogError> {
        let new = writer::validate(&self.state, &self.p.config, s, new_groups)?;
        match writer::submission_len(s.group, &s.update, s.marks) {
            Some(len) if len <= self.p.frame_room => Ok((new, len)),
            Some(len) => Err(LogError::TooLarge(len)),
            None => Err(LogError::TooLarge(usize::MAX)),
        }
    }

    /// A restore in parts would overwrite the lost frame's persist record with the first part's,
    /// and a crash before the last part was durable would leave the rest restored nowhere: none
    /// of it is written, and the open fails (mantle docs/design/raft-log.md §6).
    fn restore_in_parts(
        &mut self,
        mut taken: Vec<(Submission, Placement)>,
        inbox: &Receiver<Message<F>>,
    ) {
        let held = std::mem::take(&mut self.schedule.held);
        let parts = taken.drain(..).map(|(s, _)| s);
        let all: Vec<Submission> = parts.chain(held).collect();
        self.keep_list(taken);
        for s in all {
            let refusal = LogError::Damaged("a lost frame's restore does not fit one frame");
            self.answer(s, Err(refusal));
        }
        self.no_frame(inbox);
    }

    /// Places the frame and hands it, with its persist record, to the device; or, when no
    /// segment can take it, refuses its updates `Full`.
    fn place(&mut self, payload: Payload, laid: Laid, inbox: &Receiver<Message<F>>) {
        let tail = match &laid.sweep {
            Some(s) => s.next_tail,
            None => self.state.tail_incarnation(),
        };
        let advances = tail > self.state.durable_tail;
        // A frame makes room if it names a later tail, whose durability frees a segment, or if
        // its every update only frees what its group held: a compaction, a removal, a fence.
        let frees = |taken: &[(Submission, Placement)]| {
            !taken.is_empty() && taken.iter().all(|(s, _)| writer::frees(&s.update, s.marks))
        };
        let makes_room = advances || frees(&laid.taken);
        let target = writer::target(&self.state, &self.p, payload.len(), makes_room);
        match target {
            Ok(Some(target)) => self.write(payload, laid, target, tail, inbox),
            Ok(None) => {
                self.buffers.payload = payload;
                self.full(laid.taken, inbox);
            }
            Err(e) => {
                self.buffers.payload = payload;
                self.failed(VecDeque::new(), laid.taken, e, inbox);
            }
        }
    }

    /// Lays the frame, its persist record and its own confirmation into aligned buffers and hands
    /// them to the device, with the answers of its updates and of the frame before.
    fn write(
        &mut self,
        payload: Payload,
        laid: Laid,
        target: Target,
        tail: u64,
        inbox: &Receiver<Message<F>>,
    ) {
        let sequence = self.state.next_sequence;
        let built = self.frame_bytes(&target, laid.records, payload.as_slice(), tail, sequence);
        self.buffers.payload = payload;
        let confirms = self.state.durable;
        let job = built.and_then(|frame| {
            let updates = || laid.taken.iter().map(|(s, _)| s);
            let kept = self.buffers.record.take();
            let record = self.record_of(sequence, confirms, updates(), kept)?;
            let kept = self.buffers.confirms.pop();
            let confirm = self.record_of(sequence, sequence, updates(), kept)?;
            let at = if target.opens {
                writer::slot_start(&self.p.config, target.slot)?
            } else {
                target.offset
            };
            Ok((frame, at, record, self.record_at(sequence)?, confirm))
        });
        match job {
            Ok((frame, at, record, record_at, confirm)) => {
                let Laid {
                    sweep, mut taken, ..
                } = laid;
                // A caller of the frame that waits on its answer does the frame's I/O.
                let doer = taken
                    .iter()
                    .find(|(s, _)| s.waits)
                    .and_then(|(s, _)| s.ticket.port());
                let these = self.answers_of(&mut taken);
                let before = self
                    .unconfirmed
                    .as_mut()
                    .map(|u| std::mem::take(&mut u.answering))
                    .unwrap_or_default();
                let more = !self.intake.is_empty() || !self.schedule.held.is_empty();
                self.more_told = more;
                self.phase = Phase::Writing(Writing {
                    taken,
                    sweep,
                    target,
                    tail,
                    sequence,
                });
                self.io.push_back((
                    Job::Frame(Frame {
                        frame,
                        at,
                        record,
                        record_at,
                        sequence,
                        confirm,
                        before,
                        these,
                        more,
                    }),
                    doer,
                ));
            }
            Err(e) => self.failed(VecDeque::new(), laid.taken, e, inbox),
        }
    }

    /// The answers of a frame's updates, taken from them for the device to give: each ticket,
    /// and a handle's entries, whose terms and lengths stay for publishing the frame.
    fn answers_of(&mut self, taken: &mut [(Submission, Placement)]) -> Vec<Answering> {
        let mut these = self.buffers.answering.pop().unwrap_or_default();
        self.buffers.lens.clear();
        for (s, _) in taken.iter_mut() {
            let entries = if s.handle {
                let entries = crate::group::given_back(&mut s.update);
                let from = self.buffers.lens.len();
                self.buffers.lens.extend(
                    entries
                        .iter()
                        .map(|e| (e.term, u32::try_from(e.bytes.len()).unwrap_or(u32::MAX))),
                );
                s.lens = (from, self.buffers.lens.len());
                entries
            } else {
                Vec::new()
            };
            these.push(Answering {
                ticket: s.ticket.take(),
                entries,
            });
        }
        these
    }

    /// The frame's bytes: the segment's header first when it opens one, the frame's header, its
    /// payload. Laid into the last frame's buffer while it is large enough: every byte up to the
    /// padded end is written, so nothing of the frame before survives into this one.
    fn frame_bytes(
        &mut self,
        target: &Target,
        records: u32,
        payload: &[u8],
        tail: u64,
        sequence: u64,
    ) -> Result<AlignedBuf, LogError> {
        let p = self.p;
        let frame = format::Frame {
            log: p.id,
            incarnation: target.incarnation,
            nonce: target.nonce,
            sequence,
            tail,
            records,
        };
        let header = frame
            .header(payload)
            .ok_or(LogError::TooLarge(payload.len()))?;
        let block = p.align.get();
        let header_room = if target.opens { block } else { 0 };
        let frame_len =
            usize::try_from(target.frame_len).map_err(|_| LogError::TooLarge(payload.len()))?;
        let total = header_room
            .checked_add(frame_len)
            .ok_or(LogError::TooLarge(payload.len()))?;
        let mut buf = aligned(self.buffers.frame.take(), total, p.align)?;
        let disk = |e: hyper_block::buf::BufError| LogError::Disk(e.into());
        if target.opens {
            let header = SegmentHeader {
                log: p.id,
                incarnation: target.incarnation,
                nonce: target.nonce,
                segment_bytes: p.config.segment_bytes,
            };
            buf.extend_from_slice(&header.encode()).map_err(disk)?;
            buf.extend_zeros(block.saturating_sub(buf.len()))
                .map_err(disk)?;
        }
        buf.extend_from_slice(&header).map_err(disk)?;
        buf.extend_from_slice(payload).map_err(disk)?;
        Ok(buf)
    }

    /// The persist record of the frame of `sequence`, which confirms the frame of `confirms`,
    /// for `updates`, laid into `kept` while it is large enough.
    fn record_of<'a>(
        &mut self,
        sequence: u64,
        confirms: u64,
        updates: impl Iterator<Item = &'a Submission>,
        kept: Option<AlignedBuf>,
    ) -> Result<AlignedBuf, LogError> {
        let b = &mut self.buffers;
        b.persist.log = self.p.id;
        b.persist.sequence = sequence;
        b.persist.confirms = confirms;
        b.persist.groups.clear();
        b.persist.groups.extend(updates.map(writer::persisted));
        b.record_bytes.clear();
        b.persist
            .encode_into(&mut b.record_bytes)
            .ok_or(LogError::TooLarge(b.persist.groups.len()))?;
        let mut buf = aligned(kept, b.record_bytes.len(), self.p.align)?;
        buf.extend_from_slice(b.record_bytes.as_slice())
            .map_err(|e| LogError::Disk(e.into()))?;
        Ok(buf)
    }

    /// The file offset of the persist slot of the frame of `sequence`.
    fn record_at(&self, sequence: u64) -> Result<u64, LogError> {
        let slot = crate::recover::persist_slot(&self.p.config, self.p.align)?;
        Ok(crate::recover::persist_at(slot, sequence))
    }

    /// The frame is on the device: published, the frame before answered, and the loop goes on,
    /// with the frame's own confirmation on the device when no other frame follows.
    pub(super) fn written(&mut self, done: Flushed, inbox: &Receiver<Message<F>>) {
        let Flushed {
            result,
            timing,
            confirming,
            confirm,
            before,
            these,
        } = done;
        let Phase::Writing(w) = std::mem::replace(&mut self.phase, Phase::Idle) else {
            self.fence();
            for list in [before, these] {
                self.answer_each(list, Err(LogError::Fenced));
            }
            return;
        };
        let Writing {
            mut taken,
            sweep,
            target,
            tail,
            sequence,
        } = w;
        // The frame before's answers come back only when this frame failed.
        match self.unconfirmed.as_mut() {
            Some(u) if !before.is_empty() => u.answering = before,
            _ => self.keep_answers(before),
        }
        let published = result.and_then(|()| {
            self.schedule.anticipation.served(timing.took_ns);
            writer::publish(
                &mut self.state,
                &self.p,
                &target,
                sweep,
                &mut taken,
                tail,
                &self.buffers.lens,
            )
        });
        if let Err(e) = published {
            // Fenced before anyone hears of it, so no answer outruns the fence.
            self.fence();
            self.answer_each(these, Err(LogError::Fenced));
            self.give_confirm(confirm);
            self.failed(VecDeque::new(), taken, e, inbox);
            return;
        }
        let schedule = &mut self.schedule;
        schedule.frames = schedule.frames.saturating_add(1);
        let updates = u64::try_from(taken.len()).unwrap_or(u64::MAX);
        schedule.updates = schedule.updates.saturating_add(updates);
        let backlog = self.backlog_at_answers();
        let confirmed = match self.unconfirmed.take() {
            // This frame's flush made the record that confirms the frame before durable.
            Some(before) => self.settle(before, Ok(()), timing.flushed_at),
            None => 0,
        };
        self.count_fruitless(taken.is_empty());
        let frame = Unconfirmed {
            sequence,
            updates: taken,
            answering: these,
            confirm: None,
        };
        if confirming {
            self.give_confirm(confirm);
            self.phase = Phase::Confirming {
                frame,
                forget: false,
            };
        } else {
            self.unconfirmed = Some(Unconfirmed {
                confirm: Some(confirm),
                ..frame
            });
        }
        self.schedule.answered = confirmed;
        self.forget();
        self.schedule.backlog = backlog;
        self.step(inbox);
    }

    /// Keeps a confirmation's buffer for a later frame's: one for each frame whose confirmation
    /// is laid out at once, the frame awaiting it and the frame on the device.
    pub(super) fn give_confirm(&mut self, buf: AlignedBuf) {
        if buf.capacity() > 0 && self.buffers.confirms.len() < super::SPARE_FRAMES {
            self.buffers.confirms.push(buf);
        }
    }

    /// Answers each of `list` with `result`, and keeps the list.
    fn answer_each(&mut self, mut list: Vec<Answering>, result: Result<(), LogError>) {
        for answering in list.drain(..) {
            let mut ticket = answering.ticket;
            match &result {
                Ok(()) => ticket.answer(Ok(Answer::Durable(answering.entries))),
                Err(_) => ticket.answer(Err(LogError::Fenced)),
            }
        }
        self.keep_answers(list);
    }

    /// Frames that sweep and carry no update, in a row, while updates wait: each copies a tail
    /// with a dead piece, and its copies are all live, so after a sweep of every segment no
    /// dead piece is left to free. Past that many, the log cannot make room for what waits
    /// (mantle docs/design/raft-log.md §5).
    fn count_fruitless(&mut self, carried_none: bool) {
        if !carried_none {
            self.schedule.fruitless = 0;
            return;
        }
        self.schedule.fruitless = self.schedule.fruitless.saturating_add(1);
        if self.schedule.fruitless >= u64::from(self.p.config.max_segments) {
            self.refuse_held();
        }
    }

    /// No frame was written, so none confirms the last: a confirmation does, so that no answer
    /// waits on traffic that may only ever be refused.
    fn no_frame(&mut self, inbox: &Receiver<Message<F>>) {
        if self.confirm(true) {
            return;
        }
        self.forget();
        self.schedule.backlog = self.queued(inbox);
        self.step(inbox);
    }

    /// No segment can take the frame: every one holds live records the tail's sweep cannot
    /// free. Groups must compact. A frame that carried no update was a sweep making room for
    /// those held: none can come, and they are refused too.
    fn full(&mut self, mut taken: Vec<(Submission, Placement)>, inbox: &Receiver<Message<F>>) {
        let sweep_only = taken.is_empty();
        for (s, _) in taken.drain(..) {
            self.answer(s, Err(LogError::Full));
        }
        self.keep_list(taken);
        if sweep_only {
            self.refuse_held();
        }
        self.no_frame(inbox);
    }

    /// Answers every held update `Full`: no room can be made for them until groups compact.
    fn refuse_held(&mut self) {
        self.schedule.fruitless = 0;
        let held = std::mem::take(&mut self.schedule.held);
        for s in held {
            self.answer(s, Err(LogError::Full));
        }
    }

    /// A commit failed: the log is fenced before anyone hears of it, so no answer outruns the
    /// fence, and every update the writer holds is answered `Fenced`: those laid out, those of
    /// the batch not reached, those held for a later frame, and those of the frame before,
    /// whose confirmation will never come.
    fn failed(
        &mut self,
        batch: VecDeque<Submission>,
        taken: Vec<(Submission, Placement)>,
        _error: LogError,
        inbox: &Receiver<Message<F>>,
    ) {
        self.phase = Phase::Idle;
        self.fence();
        let held = std::mem::take(&mut self.schedule.held);
        for s in taken.into_iter().map(|(s, _)| s).chain(batch).chain(held) {
            self.answer(s, Err(LogError::Fenced));
        }
        if let Some(before) = self.unconfirmed.take() {
            self.settle(before, Err(()), None);
        }
        self.schedule.answered = 0;
        self.forget();
        self.schedule.backlog = self.queued(inbox);
        self.step(inbox);
    }

    /// Hands the device a confirmation of the last frame, written over the frame's own record in
    /// its own slot, now saying the frame was flushed: whether one is now on the device. With the
    /// log fenced, the frame's updates are answered `Fenced` at once.
    ///
    /// The next frame's record goes in the other slot before that frame's flush and may tear; a
    /// confirmation kept there would tear with it, and a frame answered and then damaged at rest
    /// would be taken for a torn one (mantle docs/design/raft-log.md §6). The rewrite puts at
    /// risk only the record of a frame not yet answered, which recovery may drop as it drops a
    /// torn tail.
    fn confirm(&mut self, forget: bool) -> bool {
        let Some(mut frame) = self.unconfirmed.take() else {
            self.schedule.answered = 0;
            return false;
        };
        if self.fenced {
            self.unanswerable(frame);
            return false;
        }
        // The confirmation was laid out with the frame, while its updates were whole.
        let (Some(record), Ok(record_at)) = (frame.confirm.take(), self.record_at(frame.sequence))
        else {
            self.fence();
            self.unanswerable(frame);
            return false;
        };
        let these = std::mem::take(&mut frame.answering);
        self.dispatch(Job::Confirm {
            record,
            record_at,
            these,
        });
        self.phase = Phase::Confirming { frame, forget };
        true
    }

    /// A frame whose confirmation will never come: its updates are answered `Fenced`.
    fn unanswerable(&mut self, frame: Unconfirmed) {
        self.settle(frame, Err(()), None);
        self.schedule.answered = 0;
    }

    /// The confirmation is durable, or failed and fenced the log: its frame's updates are
    /// answered, and the loop goes on.
    pub(super) fn confirmed(
        &mut self,
        result: Result<(), LogError>,
        these: Vec<Answering>,
        durable: Option<Instant>,
        inbox: &Receiver<Message<F>>,
    ) {
        let Phase::Confirming { mut frame, forget } =
            std::mem::replace(&mut self.phase, Phase::Idle)
        else {
            self.fence();
            self.answer_each(these, Err(LogError::Fenced));
            return;
        };
        frame.answering = these;
        let backlog = self.backlog_at_answers();
        // Nothing was queued when the answers went out: the busy period ended then, whenever
        // the answered callers' next submissions reach the inbox.
        if backlog == 0 && self.schedule.held.is_empty() && self.unconfirmed.is_none() {
            self.end_busy_period();
        }
        self.schedule.answered = match result {
            Ok(()) => self.settle(frame, Ok(()), durable),
            Err(_) => {
                self.fence();
                self.settle(frame, Err(()), None);
                0
            }
        };
        if forget {
            self.forget();
        }
        self.schedule.backlog = backlog;
        self.step(inbox);
    }

    /// Gives back the room of the updates of a frame whose confirmation is durable, `Ok`, or
    /// will never be, `Fenced`, answering those the device has not: the number settled. Its lists
    /// are kept for later frames. `durable` is when the flush that made the confirmation durable
    /// ended, which each update's commit wait runs to.
    fn settle(
        &mut self,
        frame: Unconfirmed,
        result: Result<(), ()>,
        durable: Option<Instant>,
    ) -> u64 {
        let Unconfirmed {
            mut updates,
            answering,
            confirm,
            ..
        } = frame;
        if let (Ok(()), Some(durable)) = (result, durable) {
            for (s, _) in &updates {
                self.tally.waited(s.submitted, durable);
            }
        }
        let fenced = || result.map_err(|()| LogError::Fenced);
        self.answer_each(answering, fenced());
        if let Some(buf) = confirm {
            self.give_confirm(buf);
        }
        let count = u64::try_from(updates.len()).unwrap_or(u64::MAX);
        for (s, _) in updates.drain(..) {
            // Its ticket went with the frame's answers: this gives back its room.
            self.answer(s, fenced());
        }
        self.keep_list(updates);
        count
    }

    /// Keeps an emptied list of a frame's updates for a later frame, while the writer holds
    /// fewer than it can use.
    fn keep_list(&mut self, list: Vec<(Submission, Placement)>) {
        if list.capacity() > 0 && self.buffers.taken.len() < super::SPARE_FRAMES {
            self.buffers.taken.push(list);
        }
    }

    /// Drops the finish tags that no longer order anything: those the virtual time has passed,
    /// which a start tag would take the virtual time over, and those of groups the log no
    /// longer holds with nothing waiting, so the map is bounded by the log's groups and the
    /// queue's submissions.
    fn forget(&mut self) {
        let mut waiting = std::mem::take(&mut self.buffers.seen);
        waiting.clear();
        waiting.extend(self.schedule.held.iter().map(|s| s.group));
        let now = self.schedule.virtual_time;
        let groups = &self.state.groups;
        self.schedule.finish.retain(|group, finish| {
            *finish > now && (groups.contains_key(group) || waiting.contains(group))
        });
        self.buffers.seen = waiting;
    }
}

/// An aligned buffer of `total` bytes at least: `kept` while it is large enough, emptied.
fn aligned(
    kept: Option<AlignedBuf>,
    total: usize,
    align: hyper_block::buf::Alignment,
) -> Result<AlignedBuf, LogError> {
    let mut buf = match kept {
        Some(buf) if buf.capacity() >= total => buf,
        _ => AlignedBuf::zeroed(total, align).map_err(|e| LogError::Disk(e.into()))?,
    };
    buf.clear();
    Ok(buf)
}

/// Submitters of the batch that came back since the answers went out.
fn returned(g: &Gather) -> u64 {
    u64::try_from(g.batch.len())
        .unwrap_or(u64::MAX)
        .saturating_sub(g.before)
}

#[allow(
    clippy::disallowed_methods,
    reason = "hyper-log's owner times its own writes (CLAUDE.md §1, sans-io's one exception)"
)]
fn deadline(step: Duration) -> Instant {
    let now = Instant::now();
    now.checked_add(step).unwrap_or(now)
}
