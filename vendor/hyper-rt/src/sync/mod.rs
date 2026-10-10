//! Synchronization between tasks and threads (docs/runtime.md §8): a bounded channel, a one-shot, a watched
//! word, a semaphore and a notification, each usable between any two of a task on the same shard, a task on
//! another shard, and a plain thread. One mechanism per primitive, never a local and a remote variant.
//!
//! **What the ends share** is a [`cell`]: a few atomic words in a process-wide generational table (the
//! waiting task's word, a state word, a version), freed by the last handle. **What crosses** is moved, never
//! shared: a value travels through `std::sync::mpsc::sync_channel`, bounded at the primitive's capacity.
//!
//! **Waiting.** A task waits by registering its own word in the cell and checking again before it returns
//! `Pending`; a sender publishes, then wakes the registered word. The order (publish then wake, register then
//! check) means no wake is lost: a sender that published before the register is seen by the check, and one
//! that published after finds the word (DERIVED). A plain thread uses the blocking calls, which wait in the
//! standard channel and never touch the cell's waiter.
//!
//! **No broadcast.** A wake reaches one waiter (docs/runtime.md §8; mantle note 26 §5): a watched word's
//! receivers each have their own cell, which the sender wakes one by one, at most the configured receivers.

pub mod cell;

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError, TrySendError, sync_channel};
use std::task::{Context, Poll};

use crate::error::RtError;
use crate::waker::polling_task;

use cell::{CellRef, claim};

/// Why a primitive's call ended without a value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SyncError<T> {
    /// The other end is gone: nothing more will come (or be taken).
    Closed(T),
    /// The primitive is at its bound now (a full channel, a full waiter queue); the value is handed back.
    Full(T),
    /// Polled by something that is not a hyper-rt task (a plain thread uses the blocking calls).
    NotOnShardThread(T),
}

/// Format: the state bit a closed primitive sets.
const CLOSED: u64 = 1;
/// Format: the state bit a pending notification sets.
const PENDING: u64 = 1 << 1;

// ------------------------------------------------------------------------------------------ Notify

/// A notification between one notifier and one waiter at a time: `notify_one` wakes the registered waiter,
/// and a notification with no waiter is kept as one pending permit (tokio's `Notify::notify_one` semantics,
/// one waiter at a time).
#[derive(Debug)]
pub struct Notify {
    cell: CellRef,
}

impl Notify {
    /// A notification. Refused `Capacity` at the cell bound.
    pub fn new() -> Result<Self, RtError> {
        Ok(Self { cell: claim(1)? })
    }

    /// Wakes the waiter, or keeps one pending notification for the next wait.
    pub fn notify_one(&self) {
        if let Some(cell) = self.cell.cell() {
            cell.state.fetch_or(PENDING, Ordering::AcqRel);
        }
        self.cell.wake();
    }

    /// Waits for a notification (taking it).
    pub fn notified(&self) -> Notified<'_> {
        Notified { notify: self }
    }
}

impl Drop for Notify {
    fn drop(&mut self) {
        self.cell.release();
    }
}

/// The wait of [`Notify::notified`].
#[derive(Debug)]
pub struct Notified<'a> {
    notify: &'a Notify,
}

impl Future for Notified<'_> {
    type Output = Result<(), RtError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Some(cell) = self.notify.cell.cell() else {
            return Poll::Ready(Err(RtError::ShardGone { shard: 0 }));
        };
        if cell.state.fetch_and(!PENDING, Ordering::AcqRel) & PENDING != 0 {
            return Poll::Ready(Ok(()));
        }
        let Some(word) = polling_task(cx.waker()) else {
            return Poll::Ready(Err(RtError::NotOnShardThread));
        };
        self.notify.cell.register(word);
        if cell.state.fetch_and(!PENDING, Ordering::AcqRel) & PENDING != 0 {
            return Poll::Ready(Ok(()));
        }
        Poll::Pending
    }
}

// ------------------------------------------------------------------------------------------ oneshot

/// The sending half of a one-shot.
#[derive(Debug)]
pub struct OneshotSender<T> {
    value: SyncSender<T>,
    /// After `value`: its wake runs once the channel has disconnected ([`WakeOnDrop`]).
    cell: WakeOnDrop,
}

/// The receiving half of a one-shot: a future of the value, or `Closed` when the sender went without
/// sending.
#[derive(Debug)]
pub struct OneshotReceiver<T> {
    value: Receiver<T>,
    cell: CellRef,
}

/// A one-shot: one value, from one sender to one receiver. Refused `Capacity` at the cell bound.
pub fn oneshot<T>() -> Result<(OneshotSender<T>, OneshotReceiver<T>), RtError> {
    let cell = claim(2)?;
    let (value, receiver) = sync_channel(1);
    Ok((
        OneshotSender {
            value,
            cell: WakeOnDrop(cell),
        },
        OneshotReceiver {
            value: receiver,
            cell,
        },
    ))
}

impl<T> OneshotSender<T> {
    /// Sends the value; `Closed` when the receiver is gone.
    pub fn send(self, value: T) -> Result<(), SyncError<T>> {
        let sent = match self.value.try_send(value) {
            Ok(()) => Ok(()),
            Err(TrySendError::Disconnected(value) | TrySendError::Full(value)) => {
                Err(SyncError::Closed(value))
            }
        };
        self.cell.0.wake();
        sent
    }
}

impl<T> Drop for OneshotSender<T> {
    fn drop(&mut self) {
        // The receiver learns the sender went (its channel disconnects); `cell` wakes it to see so once
        // `value` has dropped.
        if let Some(cell) = self.cell.0.cell() {
            cell.state.fetch_or(CLOSED, Ordering::AcqRel);
        }
    }
}

/// A cell handle that wakes the cell's waiter, then gives the handle back, when it drops. A sending half
/// declares it after its std channel half: fields drop in declaration order after `Drop::drop`, so the
/// channel has disconnected before the wake, and the woken receiver's retry sees the disconnect. Waking in
/// `Drop::drop` itself ran before the disconnect: the receiver could retry, find the channel empty and still
/// connected, and wait for good (CI run 37543499272, `a_thread_and_a_task_exchange_values_both_ways`).
#[derive(Debug)]
struct WakeOnDrop(CellRef);

impl Drop for WakeOnDrop {
    fn drop(&mut self) {
        self.0.wake();
        self.0.release();
    }
}

/// A sender's waiter cell in a channel's queue of senders waiting for room. Dropping it, whether the receiver
/// took it or the queue went with the receiver, grants the room to a sender still waiting (0 → `GRANTED`),
/// wakes it, and gives the queue's handle back: a sender queued as the receiver goes is woken to find the
/// channel closed, and its cell is not left behind in the dropped queue.
#[derive(Debug)]
struct Queued(CellRef);

impl Queued {
    /// Grants the room to the waiter if it still waits, and wakes it; `false` for one that left.
    fn grant(&self) -> bool {
        let granted = self.0.cell().is_some_and(|words| {
            words
                .state
                .compare_exchange(0, GRANTED, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        });
        if granted {
            self.0.wake();
        }
        granted
    }
}

impl Drop for Queued {
    fn drop(&mut self) {
        self.grant();
        self.0.release();
    }
}

impl<T> OneshotReceiver<T> {
    /// Waits for the value on a plain thread.
    pub fn blocking_recv(self) -> Result<T, SyncError<()>> {
        self.value.recv().map_err(|_| SyncError::Closed(()))
    }

    /// The value if it came, without waiting.
    pub fn try_recv(&mut self) -> Result<Option<T>, SyncError<()>> {
        match self.value.try_recv() {
            Ok(value) => Ok(Some(value)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Err(SyncError::Closed(())),
        }
    }
}

impl<T> Drop for OneshotReceiver<T> {
    fn drop(&mut self) {
        self.cell.release();
    }
}

impl<T> Future for OneshotReceiver<T> {
    type Output = Result<T, SyncError<()>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(ready) = poll_value(&mut self.as_mut().try_recv()) {
            return Poll::Ready(ready);
        }
        let Some(word) = polling_task(cx.waker()) else {
            return Poll::Ready(Err(SyncError::NotOnShardThread(())));
        };
        self.cell.register(word);
        match poll_value(&mut self.as_mut().try_recv()) {
            Some(ready) => Poll::Ready(ready),
            None => Poll::Pending,
        }
    }
}

/// A try's outcome as a ready result, or `None` while nothing came.
fn poll_value<T>(tried: &mut Result<Option<T>, SyncError<()>>) -> Option<Result<T, SyncError<()>>> {
    match std::mem::replace(tried, Ok(None)) {
        Ok(Some(value)) => Some(Ok(value)),
        Ok(None) => None,
        Err(closed) => Some(Err(closed)),
    }
}

// ------------------------------------------------------------------------------------------ channel

/// The sending half of a bounded channel; clone it for more senders.
#[derive(Debug)]
pub struct Sender<T> {
    value: SyncSender<T>,
    /// Where a sender that found the channel full queues its waiter cell to be woken when room appears.
    room: SyncSender<Queued>,
    /// After `value`: its wake runs once the last sender's drop has disconnected the channel ([`WakeOnDrop`]).
    cell: WakeOnDrop,
}

/// The receiving half of a bounded channel.
#[derive(Debug)]
pub struct ChannelReceiver<T> {
    /// Dropped first: senders that retry see the channel closed.
    value: Receiver<T>,
    /// Dropped after `value`: each sender still queued is granted, woken to find the channel closed, and its
    /// cell given back ([`Queued`]).
    room: Receiver<Queued>,
    cell: CellRef,
}

/// Format: the channel cell's state bit set when a sender that was handed room dropped its send unused: the
/// receiver passes that room to the next waiting sender (mantle's review of hyper-rt, finding 3).
const OWED: u64 = 1 << 4;

/// A bounded channel of `capacity` values from any number of senders to one receiver. A sender waiting for
/// room queues its word, at most `capacity` of them (a sender past that is told `Full`), and the receiver
/// wakes one waiting sender for each value it takes, oldest first. Refused `Capacity` at the cell bound and
/// `BadConfig` for a zero capacity.
pub fn channel<T>(capacity: usize) -> Result<(Sender<T>, ChannelReceiver<T>), RtError> {
    channel_with(capacity, capacity)
}

/// A bounded channel of `capacity` values, with room for `waiters` senders waiting at once (past it a send
/// is told `Full`): for many concurrent senders on a small channel. Refused `BadConfig` for a zero capacity or
/// no waiters.
pub fn channel_with<T>(
    capacity: usize,
    waiters: usize,
) -> Result<(Sender<T>, ChannelReceiver<T>), RtError> {
    if capacity == 0 || waiters == 0 {
        return Err(RtError::BadConfig {
            what: "a channel of no capacity, or no room for a waiting sender",
        });
    }
    let cell = claim(2)?;
    let (value, receiver) = sync_channel(capacity);
    let (room, waiting) = sync_channel(waiters);
    Ok((
        Sender {
            value,
            room,
            cell: WakeOnDrop(cell),
        },
        ChannelReceiver {
            value: receiver,
            room: waiting,
            cell,
        },
    ))
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.cell.0.retain();
        Self {
            value: self.value.clone(),
            room: self.room.clone(),
            cell: WakeOnDrop(self.cell.0),
        }
    }
}

impl<T> Sender<T> {
    /// Sends without waiting: `Full` when the channel is at its capacity, `Closed` when the receiver is gone.
    pub fn try_send(&self, value: T) -> Result<(), SyncError<T>> {
        match self.value.try_send(value) {
            Ok(()) => {
                self.cell.0.wake();
                Ok(())
            }
            Err(TrySendError::Full(value)) => Err(SyncError::Full(value)),
            Err(TrySendError::Disconnected(value)) => Err(SyncError::Closed(value)),
        }
    }

    /// Sends on a plain thread, waiting for room.
    pub fn blocking_send(&self, value: T) -> Result<(), SyncError<T>> {
        match self.value.send(value) {
            Ok(()) => {
                self.cell.0.wake();
                Ok(())
            }
            Err(std::sync::mpsc::SendError(value)) => Err(SyncError::Closed(value)),
        }
    }

    /// Sends, waiting for room as a task.
    pub fn send(&self, value: T) -> Send<'_, T> {
        Send {
            sender: self,
            value: Some(value),
            waiter: None,
        }
    }
}

/// The wait of [`Sender::send`].
///
/// A sender that finds the channel full queues a waiter cell of its own (state 0, waiting) behind the senders
/// already waiting. The receiver, taking a value, grants the room to the oldest live waiter (0 → `GRANTED`)
/// and wakes it, skipping waiters that left (`ABANDONED`). A woken sender whose room a racing sender took
/// queues again; one dropped while waiting leaves its cell `ABANDONED`; one dropped after its grant, unused,
/// marks the channel `OWED` and wakes the receiver, which hands the room on (mantle's review, finding 3: a
/// woken sender used to wait for good, and a cancelled one's word used to spend a later wake).
#[derive(Debug)]
pub struct Send<'a, T> {
    sender: &'a Sender<T>,
    value: Option<T>,
    /// The waiter cell queued for room, while one is.
    waiter: Option<CellRef>,
}

impl<T> Unpin for Send<'_, T> {}

impl<T> Send<'_, T> {
    /// Leaves the queue: the waiter's cell marked abandoned, unless the room was granted to it and not `used`,
    /// which then passes on through the receiver.
    fn leave(&mut self, used: bool) {
        let Some(waiter) = self.waiter.take() else {
            return;
        };
        if let Some(words) = waiter.cell() {
            let previous =
                words
                    .state
                    .compare_exchange(0, ABANDONED, Ordering::AcqRel, Ordering::Acquire);
            if previous == Err(GRANTED) && !used {
                if let Some(channel) = self.sender.cell.0.cell() {
                    channel.state.fetch_or(OWED, Ordering::AcqRel);
                }
                self.sender.cell.0.wake();
            }
        }
        waiter.release();
    }

    /// Queues a waiter cell holding `word`; `false` when the queue of waiting senders is full or no cell is free.
    fn queue(&mut self, word: crate::mem::Encoded) -> bool {
        let Ok(waiter) = claim(2) else {
            return false;
        };
        waiter.register(word);
        if self.sender.room.try_send(Queued(waiter)).is_err() {
            // The refused `Queued` gave the queue's handle back as it dropped; this gives back the wait's.
            waiter.release();
            return false;
        }
        self.waiter = Some(waiter);
        true
    }
}

impl<T> Drop for Send<'_, T> {
    fn drop(&mut self) {
        self.leave(false);
    }
}

impl<T> Future for Send<'_, T> {
    type Output = Result<(), SyncError<T>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Some(value) = self.value.take() else {
            return Poll::Ready(Ok(()));
        };
        let value = match self.sender.try_send(value) {
            Ok(()) => {
                self.leave(true);
                return Poll::Ready(Ok(()));
            }
            Err(SyncError::Full(value)) => value,
            Err(refused) => {
                self.leave(true);
                return Poll::Ready(Err(refused));
            }
        };
        let Some(word) = polling_task(cx.waker()) else {
            self.leave(false);
            return Poll::Ready(Err(SyncError::NotOnShardThread(value)));
        };
        // Still waiting with a cell not yet granted: keep the place, with the latest word registered.
        let waiting = self
            .waiter
            .and_then(CellRef::cell)
            .is_some_and(|words| words.state.load(Ordering::Acquire) == 0);
        if waiting {
            if let Some(waiter) = self.waiter {
                waiter.register(word);
            }
        } else {
            // No place yet, or the room granted was taken by a racing sender: queue again, at the back.
            if let Some(granted) = self.waiter.take() {
                granted.release();
            }
            if !self.queue(word) {
                // The queue is full, or went with the receiver: the channel itself says which.
                return Poll::Ready(self.sender.try_send(value));
            }
        }
        // Room may have appeared between the try and the queueing: try again before waiting.
        match self.sender.try_send(value) {
            Ok(()) => {
                self.leave(true);
                Poll::Ready(Ok(()))
            }
            Err(SyncError::Full(value)) => {
                self.value = Some(value);
                Poll::Pending
            }
            Err(refused) => {
                self.leave(true);
                Poll::Ready(Err(refused))
            }
        }
    }
}

impl<T> ChannelReceiver<T> {
    /// Takes a value without waiting: `None` when the channel is empty, `Closed` when every sender is gone.
    pub fn try_recv(&mut self) -> Result<Option<T>, SyncError<()>> {
        self.settle_owed();
        match self.value.try_recv() {
            Ok(value) => {
                self.wake_one_sender();
                Ok(Some(value))
            }
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Err(SyncError::Closed(())),
        }
    }

    /// Takes a value on a plain thread, waiting for one.
    pub fn blocking_recv(&mut self) -> Result<T, SyncError<()>> {
        let value = self.value.recv().map_err(|_| SyncError::Closed(()))?;
        self.wake_one_sender();
        Ok(value)
    }

    /// Takes a value, waiting as a task: `Closed` once every sender is gone and the channel is empty.
    pub fn recv(&mut self) -> Recv<'_, T> {
        Recv { receiver: self }
    }

    /// The room one value made goes to the live sender that waited longest; waiters that left are skipped,
    /// at most the queue's length of them.
    fn wake_one_sender(&self) {
        while let Ok(waiter) = self.room.try_recv() {
            // Dropping `waiter` gives the queue's handle back.
            if waiter.grant() {
                return;
            }
        }
    }

    /// Hands on room a sender was granted and dropped unused (`OWED`).
    fn settle_owed(&self) {
        if self
            .cell
            .cell()
            .is_some_and(|cell| cell.state.fetch_and(!OWED, Ordering::AcqRel) & OWED != 0)
        {
            self.wake_one_sender();
        }
    }
}

impl<T> Drop for ChannelReceiver<T> {
    fn drop(&mut self) {
        // Then `value` disconnects and `room` wakes every sender still queued to see so ([`Queued`]).
        self.cell.release();
    }
}

/// The wait of [`ChannelReceiver::recv`].
#[derive(Debug)]
pub struct Recv<'a, T> {
    receiver: &'a mut ChannelReceiver<T>,
}

impl<T> Future for Recv<'_, T> {
    type Output = Result<T, SyncError<()>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(ready) = poll_value(&mut self.receiver.try_recv()) {
            return Poll::Ready(ready);
        }
        let Some(word) = polling_task(cx.waker()) else {
            return Poll::Ready(Err(SyncError::NotOnShardThread(())));
        };
        self.receiver.cell.register(word);
        match poll_value(&mut self.receiver.try_recv()) {
            Some(ready) => Poll::Ready(ready),
            None => Poll::Pending,
        }
    }
}

// ------------------------------------------------------------------------------------------ watch

/// The publishing half of a watched word: one value that fits a `u64` (a flag, a level, an epoch) and its
/// version, read by any number of receivers up to the bound it was made with.
///
/// **Receivers' places** (mantle's final review, finding 6): the word claims its `bound` receiver slots when
/// it is made — cells chained through their `aux` words, the chain's head in the census cell — and a receiver
/// (the first, a `subscribe`, a `try_clone`) takes a free slot with one compare-and-swap and gives it back
/// when it drops. The sender wakes each taken slot. A receiver dropped before any send leaves nothing behind,
/// so a refusal is exactly the bound reached: clones used to travel to the sender through a channel only the
/// sender drained, and a dropped clone's entry held a place in it until the next send.
#[derive(Debug)]
pub struct WatchSender {
    cell: CellRef,
    /// The chain's owner: one handle per end, the last releases the chain (`release_census`).
    census: CellRef,
    /// The most receivers.
    bound: usize,
}

/// A receiving half of a watched word: reads the latest value and waits for the next change. Clone it for
/// another receiver (up to the bound).
#[derive(Debug)]
pub struct WatchReceiver {
    /// The shared value (`state`) and version (`aux`), and the closed bit.
    shared: CellRef,
    /// This receiver's slot, where its waiting task's word is registered.
    own: CellRef,
    seen: u64,
    /// The most receivers the word may have.
    bound: usize,
    /// The chain's owner, shared with the sender.
    census: CellRef,
}

/// Format: a slot's `state` while a receiver holds it.
const TAKEN: u64 = 1;
/// Format: the end of the slot chain.
const CHAIN_END: u64 = u64::MAX;

/// The slots of the chain headed in `census`, in order, at most `bound` (the chain's length).
fn slots(census: CellRef, bound: usize) -> impl Iterator<Item = CellRef> {
    let mut next = census
        .cell()
        .map_or(CHAIN_END, |cell| cell.aux.load(Ordering::Acquire));
    std::iter::from_fn(move || {
        if next == CHAIN_END {
            return None;
        }
        let slot = CellRef::from_word(next);
        next = slot
            .cell()
            .map_or(CHAIN_END, |cell| cell.aux.load(Ordering::Acquire));
        Some(slot)
    })
    .take(bound)
}

/// Takes a free slot of the chain: a receiver's place, or `Capacity` at the bound.
fn take_slot(census: CellRef, bound: usize) -> Result<CellRef, RtError> {
    for slot in slots(census, bound) {
        let taken = slot.cell().is_some_and(|cell| {
            cell.state
                .compare_exchange(0, TAKEN, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        });
        if taken {
            slot.clear_waiter();
            return Ok(slot);
        }
    }
    Err(RtError::Capacity {
        what: "watch receivers",
        bound,
    })
}

/// Gives a receiver's slot back.
fn give_slot(slot: CellRef) {
    slot.clear_waiter();
    if let Some(cell) = slot.cell() {
        cell.state.store(0, Ordering::Release);
    }
}

/// Drops an end's handle on the census; the last releases every slot of the chain, read before each
/// release, then frees the census itself.
fn release_census(census: CellRef, bound: usize) {
    let head = census
        .cell()
        .map_or(CHAIN_END, |cell| cell.aux.load(Ordering::Acquire));
    if !census.release_last() {
        return;
    }
    // This was the last end: no one else walks the chain now, and its cells are still claimed.
    let mut next = head;
    for _ in 0..bound {
        if next == CHAIN_END {
            break;
        }
        let slot = CellRef::from_word(next);
        next = slot
            .cell()
            .map_or(CHAIN_END, |cell| cell.aux.load(Ordering::Acquire));
        slot.release();
    }
}

/// A watched word starting at `initial`, read by at most `receivers` receivers at once (past it, a clone is
/// refused `Capacity`). Its `receivers` slots are claimed now: refused `Capacity` at the cell bound, and
/// `BadConfig` for no receivers.
pub fn watch(initial: u64, receivers: usize) -> Result<(WatchSender, WatchReceiver), RtError> {
    if receivers == 0 {
        return Err(RtError::BadConfig {
            what: "a watched word no receiver may read",
        });
    }
    let shared = claim(2)?;
    if let Some(cell) = shared.cell() {
        cell.state.store(initial, Ordering::Release);
    }
    let census = claim(2).inspect_err(|_| {
        shared.release();
        shared.release();
    })?;
    if let Some(cell) = census.cell() {
        cell.aux.store(CHAIN_END, Ordering::Release);
    }
    // The chain, built from its tail: each slot's `aux` names the next; the census heads it.
    for _ in 0..receivers {
        let slot = claim(1).inspect_err(|_| {
            shared.release();
            shared.release();
            release_census(census, receivers);
            release_census(census, receivers);
        })?;
        if let (Some(slot_cell), Some(census_cell)) = (slot.cell(), census.cell()) {
            slot_cell
                .aux
                .store(census_cell.aux.load(Ordering::Acquire), Ordering::Release);
            census_cell.aux.store(slot.to_word(), Ordering::Release);
        }
    }
    let own = take_slot(census, receivers).inspect_err(|_| {
        shared.release();
        shared.release();
        release_census(census, receivers);
        release_census(census, receivers);
    })?;
    Ok((
        WatchSender {
            cell: shared,
            census,
            bound: receivers,
        },
        WatchReceiver {
            shared,
            own,
            seen: 0,
            bound: receivers,
            census,
        },
    ))
}

impl WatchSender {
    /// Publishes `value` as the next version and wakes every receiver waiting for a change.
    pub fn send(&mut self, value: u64) {
        if let Some(cell) = self.cell.cell() {
            cell.state.store(value, Ordering::Release);
            cell.aux.fetch_add(1, Ordering::AcqRel);
        }
        self.wake_receivers();
    }

    /// The latest value.
    pub fn borrow(&self) -> u64 {
        self.cell
            .cell()
            .map_or(0, |cell| cell.state.load(Ordering::Acquire))
    }

    /// A receiver of this word (refused `Capacity` at the bound).
    pub fn subscribe(&mut self) -> Result<WatchReceiver, RtError> {
        let own = take_slot(self.census, self.bound)?;
        self.cell.retain();
        self.census.retain();
        Ok(WatchReceiver {
            shared: self.cell,
            own,
            seen: self
                .cell
                .cell()
                .map_or(0, |cell| cell.aux.load(Ordering::Acquire) & VERSION_MASK),
            bound: self.bound,
            census: self.census,
        })
    }

    /// Wakes each taken slot's waiter once (one wake per receiver, never a broadcast to one shared waiter).
    fn wake_receivers(&self) {
        for slot in slots(self.census, self.bound) {
            if slot
                .cell()
                .is_some_and(|cell| cell.state.load(Ordering::Acquire) == TAKEN)
            {
                slot.wake();
            }
        }
    }
}

impl Drop for WatchSender {
    fn drop(&mut self) {
        if let Some(cell) = self.cell.cell() {
            cell.aux.fetch_or(CLOSED << 63, Ordering::AcqRel);
        }
        self.wake_receivers();
        self.cell.release();
        release_census(self.census, self.bound);
    }
}

/// Format: the version word's top bit says the sender is gone.
const VERSION_MASK: u64 = !(CLOSED << 63);

impl WatchReceiver {
    /// The latest value, marking it seen.
    pub fn borrow_and_update(&mut self) -> u64 {
        let Some(cell) = self.shared.cell() else {
            return 0;
        };
        self.seen = cell.aux.load(Ordering::Acquire) & VERSION_MASK;
        cell.state.load(Ordering::Acquire)
    }

    /// The latest value.
    pub fn borrow(&self) -> u64 {
        self.shared
            .cell()
            .map_or(0, |cell| cell.state.load(Ordering::Acquire))
    }

    /// Waits for a version this receiver has not seen; `Closed` once the sender is gone with nothing new.
    pub fn changed(&mut self) -> Changed<'_> {
        Changed { receiver: self }
    }

    /// A change since the last seen version, or the sender's end.
    fn check(&mut self) -> Option<Result<(), SyncError<()>>> {
        let Some(cell) = self.shared.cell() else {
            return Some(Err(SyncError::Closed(())));
        };
        let version = cell.aux.load(Ordering::Acquire);
        if version & VERSION_MASK != self.seen {
            self.seen = version & VERSION_MASK;
            return Some(Ok(()));
        }
        (version & !VERSION_MASK != 0).then_some(Err(SyncError::Closed(())))
    }

    /// Another receiver of the same word, seeing what this one has seen. Refused `Capacity` at the cell bound
    /// or when every one of the word's receiver slots is held.
    pub fn try_clone(&self) -> Result<Self, RtError> {
        let own = take_slot(self.census, self.bound)?;
        self.shared.retain();
        self.census.retain();
        Ok(Self {
            shared: self.shared,
            own,
            seen: self.seen,
            bound: self.bound,
            census: self.census,
        })
    }
}

impl Drop for WatchReceiver {
    fn drop(&mut self) {
        give_slot(self.own);
        self.shared.release();
        release_census(self.census, self.bound);
    }
}

/// The wait of [`WatchReceiver::changed`].
#[derive(Debug)]
pub struct Changed<'a> {
    receiver: &'a mut WatchReceiver,
}

impl Future for Changed<'_> {
    type Output = Result<(), SyncError<()>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(ready) = self.receiver.check() {
            return Poll::Ready(ready);
        }
        let Some(word) = polling_task(cx.waker()) else {
            return Poll::Ready(Err(SyncError::NotOnShardThread(())));
        };
        self.receiver.own.register(word);
        match self.receiver.check() {
            Some(ready) => Poll::Ready(ready),
            None => Poll::Pending,
        }
    }
}

// ------------------------------------------------------------------------------------------ Semaphore

/// A counting semaphore handing permits to waiters in arrival order: a release grants its permit to the
/// waiter that waited longest, so a newcomer cannot take it ahead of one queued (docs/runtime.md §8). At most
/// `waiters` tasks wait at once; past it an acquire is refused `Full`.
#[derive(Debug)]
pub struct Semaphore {
    /// The free permits (`state`) and the waiters queued (`aux`).
    cell: CellRef,
    queue: SyncSender<CellRef>,
    queued: Receiver<CellRef>,
}

/// Format: a waiter's cell state when the permit was granted to it.
const GRANTED: u64 = 1 << 2;
/// Format: a waiter's cell state when it left the queue without a permit.
const ABANDONED: u64 = 1 << 3;

/// A permit, given back when dropped.
#[derive(Debug)]
pub struct Permit<'a> {
    semaphore: &'a Semaphore,
}

impl Drop for Permit<'_> {
    fn drop(&mut self) {
        self.semaphore.release();
    }
}

impl Semaphore {
    /// A semaphore of `permits`, with room for `waiters` queued acquirers. Refused `Capacity` at the cell bound
    /// and `BadConfig` for no waiters.
    pub fn new(permits: u64, waiters: usize) -> Result<Self, RtError> {
        if waiters == 0 {
            return Err(RtError::BadConfig {
                what: "a semaphore no acquirer may wait on",
            });
        }
        let cell = claim(1)?;
        if let Some(words) = cell.cell() {
            words.state.store(permits, Ordering::Release);
        }
        let (queue, queued) = sync_channel(waiters);
        Ok(Self {
            cell,
            queue,
            queued,
        })
    }

    /// The free permits now.
    pub fn available(&self) -> u64 {
        self.cell
            .cell()
            .map_or(0, |cell| cell.state.load(Ordering::Acquire))
    }

    /// A permit if one is free and nobody waits for one.
    pub fn try_acquire(&self) -> Option<Permit<'_>> {
        let cell = self.cell.cell()?;
        if cell.aux.load(Ordering::Acquire) != 0 {
            return None;
        }
        cell.state
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |free| {
                free.checked_sub(1)
            })
            .ok()
            .map(|_| Permit { semaphore: self })
    }

    /// Waits for a permit as a task.
    pub fn acquire(&self) -> Acquire<'_> {
        Acquire {
            semaphore: self,
            waiting: None,
        }
    }

    /// Gives a permit back: to the oldest live waiter, else to the free count. Bounded by the queue's length:
    /// each waiter that left is skipped once.
    fn release(&self) {
        let Some(cell) = self.cell.cell() else {
            return;
        };
        while let Ok(waiter) = self.queued.try_recv() {
            cell.aux.fetch_sub(1, Ordering::AcqRel);
            let Some(words) = waiter.cell() else {
                continue;
            };
            // Grant unless the waiter left first; a waiter that left took no permit.
            if words
                .state
                .compare_exchange(0, GRANTED, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                waiter.wake();
                waiter.release();
                return;
            }
            waiter.release();
        }
        cell.state.fetch_add(1, Ordering::AcqRel);
    }
}

impl Drop for Semaphore {
    fn drop(&mut self) {
        while let Ok(waiter) = self.queued.try_recv() {
            waiter.wake();
            waiter.release();
        }
        self.cell.release();
    }
}

/// The wait of [`Semaphore::acquire`].
#[derive(Debug)]
pub struct Acquire<'a> {
    semaphore: &'a Semaphore,
    /// This acquirer's own cell once it queued.
    waiting: Option<CellRef>,
}

impl<'a> Future for Acquire<'a> {
    type Output = Result<Permit<'a>, SyncError<()>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let semaphore = self.semaphore;
        if let Some(own) = self.waiting {
            let granted = own
                .cell()
                .is_some_and(|words| words.state.load(Ordering::Acquire) & GRANTED != 0);
            if granted {
                self.waiting = None;
                own.release();
                return Poll::Ready(Ok(Permit { semaphore }));
            }
            if let Some(word) = polling_task(cx.waker()) {
                own.register(word);
            }
            return Poll::Pending;
        }
        if let Some(permit) = semaphore.try_acquire() {
            return Poll::Ready(Ok(permit));
        }
        let Some(word) = polling_task(cx.waker()) else {
            return Poll::Ready(Err(SyncError::NotOnShardThread(())));
        };
        let Ok(own) = claim(2) else {
            return Poll::Ready(Err(SyncError::Full(())));
        };
        own.register(word);
        if let Some(cell) = semaphore.cell.cell() {
            cell.aux.fetch_add(1, Ordering::AcqRel);
        }
        if semaphore.queue.try_send(own).is_err() {
            if let Some(cell) = semaphore.cell.cell() {
                cell.aux.fetch_sub(1, Ordering::AcqRel);
            }
            own.release();
            own.release();
            return Poll::Ready(Err(SyncError::Full(())));
        }
        // A permit freed between the try and the queueing went to the free count, since the queue looked empty:
        // take it now, leaving the queue so no release grants this acquirer a second.
        if let Some(cell) = semaphore.cell.cell()
            && cell
                .state
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |free| {
                    free.checked_sub(1)
                })
                .is_ok()
        {
            if let Some(words) = own.cell()
                && words
                    .state
                    .compare_exchange(0, ABANDONED, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
            {
                // Granted meanwhile as well: the spare permit goes on as a release, to the next waiter first.
                semaphore.release();
            }
            own.release();
            return Poll::Ready(Ok(Permit { semaphore }));
        }
        self.waiting = Some(own);
        Poll::Pending
    }
}

impl Drop for Acquire<'_> {
    /// A wait dropped before its grant leaves the queue; one dropped after a grant it never took gives the
    /// permit back.
    fn drop(&mut self) {
        let Some(own) = self.waiting.take() else {
            return;
        };
        let left = own.cell().is_some_and(|words| {
            words
                .state
                .compare_exchange(0, ABANDONED, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        });
        if !left {
            self.semaphore.release();
        }
        own.release();
    }
}
