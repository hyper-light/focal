//! Synchronization between tasks and threads (docs/runtime.md §8): a bounded channel, a one-shot, a watched
//! word, a semaphore and a notification, each usable between any two of a task on the same shard, a task on
//! another shard, and a plain thread. One mechanism per primitive, never a local and a remote variant.
//!
//! **What the ends share** is a [`cell`]: a few atomic words in a process-wide generational table (the
//! waiter's word, a state word, a version), freed by the last handle. **What crosses** is moved, never
//! shared: a value travels through a [`ring`], bounded at the primitive's capacity, in a block the ends
//! share for as long as the cell's handles live (`ring::Shared`). No path takes a lock: the values used to
//! travel through `std::sync::mpsc::sync_channel`, whose blocked receivers and notifying senders meet in a
//! pthread mutex (mantle `docs/design/event-loop.md` D4).
//!
//! **Waiting.** A waiter, a task or a plain thread blocked in a call, registers itself in the cell and
//! checks again before it waits; a sender publishes, then wakes the registered waiter. The order (publish
//! then wake, register then check), with a fence on each side, means no wake is lost: a sender that published
//! before the register is seen by the check, and one that published after finds the waiter
//! (`crate::handoff`). A thread waits parked, and the publisher that takes it unparks it: one wake a
//! registration, however many publish.
//!
//! **No broadcast.** A wake reaches one waiter (docs/runtime.md §8; mantle note 26 §5): a watched word's
//! receivers each have their own cell, which the sender wakes one by one, at most the configured receivers.

pub mod cell;
pub(crate) mod ring;
mod room;

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering, fence};
use std::task::{Context, Poll};

use crate::error::RtError;
use crate::waker::polling_task;

use cell::{CellRef, claim};
use ring::{Padded, Refused, Ring, Shared};
use room::Room;

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
/// Format: a channel's or one-shot's state bit once its receiver is gone (a send is refused `Closed`).
const RECEIVER_GONE: u64 = 1 << 5;
/// Format: a channel's or one-shot's state bit once every sender is gone (the receiver, its values taken,
/// sees `Closed`).
const SENDERS_GONE: u64 = 1 << 6;

/// Whether `bit` is set in the state of `cell`; a cell that is gone reads as every bit set.
fn state_has(cell: CellRef, bit: u64) -> bool {
    words_have(cell.cell(), bit)
}

/// Whether `bit` is set in the state of a cell's `words`, as a live end holds them ([`Shared::words`]); words
/// that are gone read as every bit set.
fn words_have(words: Option<&cell::Cell>, bit: u64) -> bool {
    words.is_none_or(|words| words.state.load(Ordering::Acquire) & bit != 0)
}

/// Sets `bit` in the state of `cell` (a release: what the setter did before is seen by a reader of the bit).
fn set_state(cell: CellRef, bit: u64) {
    if let Some(words) = cell.cell() {
        words.state.fetch_or(bit, Ordering::AcqRel);
    }
}

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
        let Some(word) = polling_task(cx.waker()) else {
            return Poll::Ready(Err(RtError::NotOnShardThread));
        };
        let Some(cell) = self.notify.cell.cell() else {
            return Poll::Ready(Err(RtError::ShardGone { shard: 0 }));
        };
        if cell.state.fetch_and(!PENDING, Ordering::AcqRel) & PENDING != 0 {
            return Poll::Ready(Ok(()));
        }
        self.notify.cell.register(word);
        if cell.state.fetch_and(!PENDING, Ordering::AcqRel) & PENDING != 0 {
            return Poll::Ready(Ok(()));
        }
        Poll::Pending
    }
}

// ------------------------------------------------------------------------------------------ oneshot

/// What a one-shot's ends share: the one value's slot.
#[derive(Debug)]
struct OneshotState<T> {
    value: Ring<T>,
}

/// The sending half of a one-shot.
#[derive(Debug)]
pub struct OneshotSender<T> {
    /// `None` once the value went: the drop then says nothing.
    shared: Option<Shared<OneshotState<T>>>,
}

/// The receiving half of a one-shot: a future of the value, or `Closed` when the sender went without
/// sending.
#[derive(Debug)]
pub struct OneshotReceiver<T> {
    shared: Shared<OneshotState<T>>,
}

/// A one-shot: one value, from one sender to one receiver. Refused `Capacity` at the cell bound.
pub fn oneshot<T>() -> Result<(OneshotSender<T>, OneshotReceiver<T>), RtError> {
    let value = Ring::new(1)?;
    let cell = claim(1)?;
    let sender = Shared::new(OneshotState { value }, cell);
    let receiver = sender.share();
    Ok((
        OneshotSender {
            shared: Some(sender),
        },
        OneshotReceiver { shared: receiver },
    ))
}

impl<T> OneshotSender<T> {
    /// Whether the receiver has gone, so a value sent now would be refused: what an owner holding replies
    /// for its callers sweeps by, before their deadlines, once a caller stopped waiting.
    pub fn is_closed(&self) -> bool {
        self.shared
            .as_ref()
            .is_none_or(|shared| state_has(shared.cell(), RECEIVER_GONE))
    }

    /// Sends the value; `Closed` when the receiver is gone.
    pub fn send(mut self, value: T) -> Result<(), SyncError<T>> {
        let Some(shared) = self.shared.take() else {
            return Err(SyncError::Closed(value));
        };
        let cell = shared.cell();
        if state_has(cell, RECEIVER_GONE) {
            set_state(cell, SENDERS_GONE);
            return Err(SyncError::Closed(value));
        }
        let ring = &shared.get().value;
        // The one place is the one send's, so a refusal is the receiver's close (`ring`'s mark).
        let sent = match ring.push(value) {
            Ok(()) => {
                // A receiver that closed after this push claimed its place may have drained before it
                // landed: the value is dropped here, as a close drops what was sent.
                fence(Ordering::SeqCst);
                if ring.is_closed() {
                    ring.drain();
                }
                Ok(())
            }
            Err(Refused::Full(value) | Refused::Closed(value)) => Err(SyncError::Closed(value)),
        };
        // The one value went (or could not): the receiver may stop waiting either way.
        set_state(cell, SENDERS_GONE);
        cell.wake();
        sent
    }
}

impl<T> Drop for OneshotSender<T> {
    fn drop(&mut self) {
        // Gone without sending: the receiver learns so, then is woken to see it.
        if let Some(shared) = self.shared.take() {
            set_state(shared.cell(), SENDERS_GONE);
            shared.cell().wake();
        }
    }
}

impl<T> OneshotReceiver<T> {
    /// Waits for the value on a plain thread, parked until the sender wakes it.
    pub fn blocking_recv(&mut self) -> Result<T, SyncError<()>> {
        if crate::registry::current_shard().is_some() {
            return Err(SyncError::NotOnShardThread(()));
        }
        let cell = self.shared.cell();
        cell.wait_as_thread(|| poll_value(&mut self.try_recv()))
            .unwrap_or(Err(SyncError::Closed(())))
    }

    /// The value if it came, without waiting.
    pub fn try_recv(&mut self) -> Result<Option<T>, SyncError<()>> {
        // The receiver is the ring's one taker until its drop closes it (`Ring::pop_owned`).
        if let Some(value) = self.shared.get().value.pop_owned() {
            return Ok(Some(value));
        }
        if !state_has(self.shared.cell(), SENDERS_GONE) {
            return Ok(None);
        }
        // The sender is gone; what it sent before going is visible now (its state bit was set after).
        match self.shared.get().value.pop_owned() {
            Some(value) => Ok(Some(value)),
            None => Err(SyncError::Closed(())),
        }
    }
}

impl<T> Drop for OneshotReceiver<T> {
    fn drop(&mut self) {
        // The sender refuses from now on: a send that has not claimed the place is refused with its value,
        // and one that has is dropped here or by its sender (`ring`'s close).
        set_state(self.shared.cell(), RECEIVER_GONE);
        let ring = &self.shared.get().value;
        ring.close();
        ring.drain();
    }
}

impl<T> Future for OneshotReceiver<T> {
    type Output = Result<T, SyncError<()>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Some(word) = polling_task(cx.waker()) else {
            return Poll::Ready(Err(SyncError::NotOnShardThread(())));
        };
        if let Some(ready) = poll_value(&mut self.as_mut().try_recv()) {
            return Poll::Ready(ready);
        }
        self.shared.cell().register(word);
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

/// What a channel's ends share: its values, the senders waiting for room, how many senders live, and
/// whether every one is gone.
#[derive(Debug)]
struct ChannelState<T> {
    values: Ring<T>,
    room: Room,
    /// The live senders; the last to go closes the channel for the receiver.
    senders: AtomicUsize,
    /// Whether every sender is gone, on a line of its own that only the last sender's drop writes. The
    /// receiver reads it each time it finds the ring empty, and it used to read the cell's state word for
    /// it, in the process-wide table where a cell is 32 bytes, four to an Apple silicon line, beside other
    /// primitives' words that their own ends write: an empty `try_recv` cost 1.8 ns alone and 2.1–2.7 ns
    /// while two threads churned one-shots (`benchmark-results/hyper-rt-recv-20261013`).
    senders_gone: Padded<AtomicBool>,
}

/// The sending half of a bounded channel; clone it for more senders.
#[derive(Debug)]
pub struct Sender<T> {
    shared: Shared<ChannelState<T>>,
}

/// The receiving half of a bounded channel.
#[derive(Debug)]
pub struct ChannelReceiver<T> {
    shared: Shared<ChannelState<T>>,
}

/// A bounded channel of `capacity` values from any number of senders to one receiver. A sender waiting for
/// room holds one of `capacity` places (a sender past that is told `Full`), and each value the receiver
/// takes grants its room to one waiting sender, round the places ([`room`]). Refused `Capacity` at the cell
/// bound and `BadConfig` for a zero capacity.
pub fn channel<T>(capacity: usize) -> Result<(Sender<T>, ChannelReceiver<T>), RtError> {
    channel_with(capacity, capacity)
}

/// A bounded channel of `capacity` values, with places for `waiters` senders waiting at once (past it a send
/// is told `Full`): size it to the senders, tasks or threads, that may wait at once. Refused `BadConfig` for a
/// zero capacity, no waiters, or a capacity whose slots cannot be laid out.
pub fn channel_with<T>(
    capacity: usize,
    waiters: usize,
) -> Result<(Sender<T>, ChannelReceiver<T>), RtError> {
    if capacity == 0 || waiters == 0 {
        return Err(RtError::BadConfig {
            what: "a channel of no capacity, or no room for a waiting sender",
        });
    }
    let values = Ring::new(capacity)?;
    let room = Room::new(waiters)?;
    let cell = claim(1)?;
    let sender = Shared::new(
        ChannelState {
            values,
            room,
            senders: AtomicUsize::new(1),
            senders_gone: Padded(AtomicBool::new(false)),
        },
        cell,
    );
    let receiver = sender.share();
    Ok((
        Sender { shared: sender },
        ChannelReceiver { shared: receiver },
    ))
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.shared.get().senders.fetch_add(1, Ordering::Relaxed);
        Self {
            shared: self.shared.share(),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        // The last sender closes the channel for the receiver, then wakes it to see so: its values, and every
        // earlier sender's (each released by its own count above), are visible to a receiver that reads the bit.
        let state = self.shared.get();
        if state.senders.fetch_sub(1, Ordering::AcqRel) == 1 {
            state.senders_gone.0.store(true, Ordering::Release);
            self.shared.cell().wake();
        }
    }
}

impl<T> Sender<T> {
    /// Sends without waiting: `Full` when the channel is at its capacity, `Closed` when the receiver is gone.
    pub fn try_send(&self, value: T) -> Result<(), SyncError<T>> {
        self.try_send_on(value, None)
    }

    /// [`Sender::try_send`] by a task polling on the shard `here`, which its wake of a waiting task of that
    /// shard uses rather than look the thread's shard up again.
    fn try_send_on(
        &self,
        value: T,
        here: Option<&crate::shard::ShardContext>,
    ) -> Result<(), SyncError<T>> {
        let words = self.shared.words();
        if words_have(words, RECEIVER_GONE) {
            return Err(SyncError::Closed(value));
        }
        let values = &self.shared.get().values;
        match values.push(value) {
            Ok(()) => {
                // A receiver that closed after this push claimed its place may have drained before it
                // landed: what is left is dropped here, as the receiver's drop drops what was sent. The fence
                // also orders the push before the read of the waiter word, the wake's own fence's work, while
                // no drain has written since.
                fence(Ordering::SeqCst);
                if values.is_closed() {
                    values.drain();
                    if let Some(words) = words {
                        words.wake();
                    }
                } else if let Some(words) = words {
                    words.wake_fenced(here);
                }
                Ok(())
            }
            Err(Refused::Closed(value)) => Err(SyncError::Closed(value)),
            Err(Refused::Full(value)) if words_have(words, RECEIVER_GONE) => {
                Err(SyncError::Closed(value))
            }
            Err(Refused::Full(value)) => Err(SyncError::Full(value)),
        }
    }

    /// Sends on a plain thread, waiting for room: parked in a place of its own until a take grants it room,
    /// as a task waits ([`Sender::send`]); a grant whose room another sender took leaves it waiting in the same
    /// place ([`room`]). Ends when the value goes in, or the receiver goes (`Closed`); `Full` when every place
    /// for a waiting sender is held.
    pub fn blocking_send(&self, value: T) -> Result<(), SyncError<T>> {
        if crate::registry::current_shard().is_some() {
            return Err(SyncError::NotOnShardThread(value));
        }
        let value = match self.try_send(value) {
            Err(SyncError::Full(value)) => value,
            sent => return sent,
        };
        let room = &self.shared.get().room;
        let Some(place) = room.hold() else {
            return Err(SyncError::Full(value));
        };
        let cell = self.shared.cell();
        room.send_waiting(
            place,
            value,
            |value| match self.try_send(value) {
                Err(SyncError::Full(value)) => Err(value),
                sent => Ok(sent),
            },
            |waiter, granted| {
                crate::handoff::wait_as_thread(waiter, || {
                    (granted() || state_has(cell, RECEIVER_GONE)).then_some(())
                });
            },
            crate::handoff::wake_waiter,
        )
        .unwrap_or_else(|value| Err(SyncError::Full(value)))
    }

    /// Sends, waiting for room as a task.
    pub fn send(&self, value: T) -> Send<'_, T> {
        Send {
            sender: self,
            value: Some(value),
            place: None,
        }
    }
}

/// The wait of [`Sender::send`].
///
/// A sender that finds the channel full holds a place of its own among the waiting senders, its task's word
/// registered there, and checks once more; each value the receiver takes grants one waiting place its room and
/// wakes it. A woken sender whose room a racing sender took waits again in its place. One dropped while
/// waiting frees its place; one dropped after a grant it did not use hands the room on to another waiting
/// sender (mantle's review, finding 3: a woken sender used to wait for good, and a cancelled one's word used
/// to spend a later wake).
#[derive(Debug)]
pub struct Send<'a, T> {
    sender: &'a Sender<T>,
    value: Option<T>,
    /// The place held among the waiting senders, while one is.
    place: Option<usize>,
}

impl<T> Unpin for Send<'_, T> {}

impl<T> Send<'_, T> {
    /// Lets the place go, if one is held: a grant this send did not use goes on (`Room::leave`).
    fn leave(&mut self, used: bool) {
        if let Some(place) = self.place.take() {
            self.sender.shared.get().room.leave(place, used);
        }
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
        // One lookup of the thread's shard serves the task's word and the wake of a waiting task there.
        let this = &mut *self;
        crate::registry::with_here(|here| match here {
            Some(here) => this.poll_on(here, value, cx),
            None => {
                this.leave(false);
                Poll::Ready(Err(SyncError::NotOnShardThread(value)))
            }
        })
    }
}

impl<T> Send<'_, T> {
    /// [`Send::poll`] on the shard `here`, the task's word taken from it; a waker that is not the polling
    /// task's is refused `NotOnShardThread`, the value handed back.
    fn poll_on(
        &mut self,
        here: &crate::shard::ShardContext,
        value: T,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), SyncError<T>>> {
        let Some(word) = crate::waker::polling_task_in(here, cx.waker()) else {
            self.leave(false);
            return Poll::Ready(Err(SyncError::NotOnShardThread(value)));
        };
        let sender = self.sender;
        let room = &sender.shared.get().room;
        // A grant seen before the try is used by a send that follows it.
        let granted = self.place.is_some_and(|place| room.granted(place));
        let value = match sender.try_send_on(value, Some(here)) {
            Err(SyncError::Full(value)) => value,
            sent => {
                self.leave(granted);
                return Poll::Ready(sent);
            }
        };
        let place = match self.place {
            Some(place) => {
                if room.granted(place) {
                    // Granted, and the room went to a racing sender: wait again in the same place.
                    room.wait_again(place);
                }
                place
            }
            None => match room.hold() {
                Some(place) => {
                    self.place = Some(place);
                    place
                }
                None => return Poll::Ready(Err(SyncError::Full(value))),
            },
        };
        room.register_task(place, word.word());
        // Room made before the place showed waiting is seen here; room made after it is granted to the place.
        match sender.try_send_on(value, Some(here)) {
            Err(SyncError::Full(value)) => {
                self.value = Some(value);
                if room.granted(place) {
                    // A grant landed before this task's word did, so its wake went nowhere: be polled again.
                    cx.waker().wake_by_ref();
                }
                Poll::Pending
            }
            sent => {
                self.leave(false);
                Poll::Ready(sent)
            }
        }
    }
}

impl<T> ChannelReceiver<T> {
    /// Takes a value without waiting: `None` when the channel is empty, `Closed` when every sender is gone.
    pub fn try_recv(&mut self) -> Result<Option<T>, SyncError<()>> {
        let state = self.shared.get();
        // The receiver is the ring's one taker until its drop closes it (`Ring::pop_owned`).
        if let Some(value) = state.values.pop_owned() {
            state.room.grant_one();
            return Ok(Some(value));
        }
        if !state.senders_gone.0.load(Ordering::Acquire) {
            return Ok(None);
        }
        // Every sender is gone; what they sent before going is visible now (the last set the flag after).
        match state.values.pop_owned() {
            Some(value) => {
                state.room.grant_one();
                Ok(Some(value))
            }
            None => Err(SyncError::Closed(())),
        }
    }

    /// Takes a value on a plain thread, parked until a sender wakes it.
    pub fn blocking_recv(&mut self) -> Result<T, SyncError<()>> {
        if crate::registry::current_shard().is_some() {
            return Err(SyncError::NotOnShardThread(()));
        }
        let cell = self.shared.cell();
        cell.wait_as_thread(|| poll_value(&mut self.try_recv()))
            .unwrap_or(Err(SyncError::Closed(())))
    }

    /// Takes a value, waiting as a task: `Closed` once every sender is gone and the channel is empty.
    pub fn recv(&mut self) -> Recv<'_, T> {
        Recv { receiver: self }
    }
}

impl<T> Drop for ChannelReceiver<T> {
    fn drop(&mut self) {
        // Senders refuse from now on (`ring`'s close: a push that has not claimed its place is refused with
        // its value, so none lands in the room this drain frees); the values sent drop now, a sender whose
        // claim came first drops its own, and every sender waiting for room is woken to find the channel closed.
        set_state(self.shared.cell(), RECEIVER_GONE);
        let values = &self.shared.get().values;
        values.close();
        values.drain();
        self.shared.get().room.wake_all();
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
        let Some(word) = polling_task(cx.waker()) else {
            return Poll::Ready(Err(SyncError::NotOnShardThread(())));
        };
        if let Some(ready) = poll_value(&mut self.receiver.try_recv()) {
            return Poll::Ready(ready);
        }
        if let Some(words) = self.receiver.shared.words() {
            words.register(word);
        }
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
        let Some(word) = polling_task(cx.waker()) else {
            return Poll::Ready(Err(SyncError::NotOnShardThread(())));
        };
        if let Some(ready) = self.receiver.check() {
            return Poll::Ready(ready);
        }
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
    /// The queued acquirers' cells, oldest first.
    queue: Ring<CellRef>,
    /// Used from one thread at a time, as it was when its queue was a std channel (`Send`, not `Sync`).
    one_thread: std::marker::PhantomData<std::cell::Cell<()>>,
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
        let queue = Ring::new(waiters)?;
        let cell = claim(1)?;
        if let Some(words) = cell.cell() {
            words.state.store(permits, Ordering::Release);
        }
        Ok(Self {
            cell,
            queue,
            one_thread: std::marker::PhantomData,
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
        while let Some(waiter) = self.queue.pop() {
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
        while let Some(waiter) = self.queue.pop() {
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
        let Some(word) = polling_task(cx.waker()) else {
            return Poll::Ready(Err(SyncError::NotOnShardThread(())));
        };
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
            own.register(word);
            return Poll::Pending;
        }
        if let Some(permit) = semaphore.try_acquire() {
            return Poll::Ready(Ok(permit));
        }
        let Ok(own) = claim(2) else {
            return Poll::Ready(Err(SyncError::Full(())));
        };
        own.register(word);
        if let Some(cell) = semaphore.cell.cell() {
            cell.aux.fetch_add(1, Ordering::AcqRel);
        }
        if semaphore.queue.push(own).is_err() {
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
