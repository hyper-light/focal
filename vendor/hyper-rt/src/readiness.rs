//! Awaiting a socket's readiness through the shard's driver (§4.10a, §4.6): a future registers
//! one-shot interest — readable, or writable — with the current shard's driver on its first poll and
//! yields; the driver's completion (or the simulation fabric's wake) re-queues the task, and the next
//! poll returns ready so the caller retries its non-blocking syscall. One path serves both sockets:
//! the UDP datagram socket awaits readability (a real fd, or a simulated fabric port), and the TCP
//! stream awaits readability for `read`/`accept` and writability for a `write` whose send buffer
//! filled. The driver decides how the edge is watched (kqueue `EVFILT_READ`/`EVFILT_WRITE`, epoll
//! `EPOLLIN`/`EPOLLOUT`); the future is the same either way, which is why it lives here and not in the
//! socket modules. [`readable`] is public because a bridge queue's doorbell is the same edge: the
//! virtio-fs device (`slates-bridge-virtiofs`, §4.6) awaits the kick descriptor its VMM handed it —
//! an eventfd or a pipe — through the shard's driver exactly as a socket is awaited, and drains it
//! itself; the driver seam's doc names "the bridge queues" as a user of `wait` for this reason.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::error::RtError;
use crate::mem::Encoded;
use crate::registry;
use crate::shard::Ticket;
use crate::waker::polling_task;

/// Which readiness edge a caller awaits.
#[derive(Clone, Copy, Debug)]
enum Interest {
    /// The socket has data to read, or a listener has a connection to accept.
    Readable,
    /// The socket has send-buffer space for a write (or connect) that returned `EAGAIN`/`EINPROGRESS`: a TCP
    /// write, or a UDP send the kernel had no room for (every platform's driver arms it: kqueue
    /// `EVFILT_WRITE`, epoll `EPOLLOUT`, IOCP's AFD send poll).
    Writable,
}

/// Awaits one readiness edge on `raw` through the shard's driver: it takes a wait slot and registers
/// one-shot interest on the first poll and yields; the loop marks the wait fired when the handle fires,
/// and only then is a poll ready, so the caller retries its non-blocking syscall. A task woken for another
/// reason — a timer a `race` also waits on, a sibling of a `join` — finds its wait still armed and keeps
/// waiting (mantle's final review, finding 2: any re-poll used to report ready, and a wait that had not
/// fired was never withdrawn). Dropped while armed, it withdraws its own wait and no other.
#[derive(Debug)]
pub struct Ready {
    target: Target,
    interest: Interest,
    /// The wait's ticket and the task word it was registered for, while the wait is armed.
    armed: Option<(Ticket, Encoded)>,
}

/// What a readiness wait watches: an OS handle through the shard's driver, or a simulated socket through
/// its desk (docs/runtime.md §11).
#[derive(Clone, Copy, Debug)]
pub(crate) enum Target {
    /// A descriptor or socket the driver watches.
    Os(i32),
    /// A simulated socket's index on the shard's desk.
    Sim(u16),
}

impl Target {
    /// The OS handle the shard's driver arms, if any.
    fn raw(self) -> Option<i32> {
        match self {
            Self::Os(raw) => Some(raw),
            Self::Sim(_) => None,
        }
    }
}

impl Future for Ready {
    type Output = Result<(), RtError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), RtError>> {
        let Some(word) = polling_task(cx.waker()) else {
            // A waker that is not this shard's task (a combinator's, another runtime's, a poll off the shard)
            // cannot be registered, and answering "ready" would make the caller's retry loop spin on the shard
            // (mantle's review, finding 1): refused, as the synchronization primitives refuse it.
            return Poll::Ready(Err(RtError::NotOnShardThread));
        };
        if let Some((ticket, armed_for)) = self.armed {
            if armed_for == word {
                let outcome = registry::with_current(|ctx| ctx.interest_outcome(ticket)).flatten();
                return match outcome {
                    // Not fired: whatever woke the task, this wait goes on.
                    None => Poll::Pending,
                    Some(outcome) => {
                        self.armed = None;
                        Poll::Ready(outcome)
                    }
                };
            }
            // Polled by another task than the one it registered for: the old wait ends, a new one begins.
            self.abandon();
        }
        let writable = matches!(self.interest, Interest::Writable);
        let registered = registry::with_current(|ctx| match self.target {
            Target::Os(raw) => ctx.register_interest(raw, writable, word),
            Target::Sim(index) => ctx.take_wait(None, writable, word).and_then(|ticket| {
                crate::sim::sim_register(index, writable, word, ticket)
                    .map(|()| ticket)
                    .inspect_err(|_| {
                        ctx.abandon_interest(None, writable, word, ticket);
                    })
            }),
        });
        match registered {
            Some(Ok(ticket)) => {
                self.armed = Some((ticket, word));
                Poll::Pending
            }
            Some(Err(e)) => Poll::Ready(Err(e)),
            None => Poll::Ready(Err(RtError::NotOnShardThread)),
        }
    }
}

/// Waits dropped away from their shard whose end could not be marked on it while it lived: a slot held
/// against the owner's bound until it exits. A tripwire, expected zero: a mark is refused only for a slot
/// past the owner's marks, which no ticket names. A send refused because the owner is gone leaks nothing
/// (its desk went with its slots) and is not counted (the third pass, B).
static ABANDONS_LOST: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Waits whose end could not be marked on their live shard since the process started ([`ABANDONS_LOST`]).
pub fn abandons_lost() -> u64 {
    ABANDONS_LOST.load(std::sync::atomic::Ordering::Relaxed)
}

impl Ready {
    /// Ends the armed wait, if any ([`abandon`]).
    fn abandon(&mut self) {
        let writable = matches!(self.interest, Interest::Writable);
        abandon(self.target.raw(), writable, self.armed.take());
    }
}

/// Ends an armed wait, if any: its own shard withdraws it from its table and gives its slot back. On
/// another shard, or off any, the wait is marked abandoned on its shard's registry entry, which its next
/// control drain sweeps: a mark per slot, never refused for room (mantle's final review, second pass,
/// finding 4, and third pass, A: a control message could meet a full channel and leak the slot). A
/// group's own slot ends the same way, its listed members with it.
fn abandon(raw: Option<i32>, writable: bool, armed: Option<(Ticket, Encoded)>) {
    let Some((ticket, word)) = armed else {
        return;
    };
    let here = registry::with_current(|ctx| {
        ctx.owns(ticket)
            .then(|| ctx.abandon_interest(raw, writable, word, ticket))
            .is_some()
    })
    .unwrap_or(false);
    if here {
        return;
    }
    let marked = registry::with_holder(ticket.owner(), |entry| {
        entry.mark_abandoned(ticket.slot(), ticket.generation())
    });
    // `None`: the owner is gone, and its slots with it.
    if marked == Some(false) {
        ABANDONS_LOST.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Readiness waits on many handles for one task, told in the order they fired (docs/runtime.md §3.7). A
/// task that awaits each of n handles with a [`Ready`] of its own asks every one whether it fired at
/// each poll: n asks a wake, however few fired. hyper-rt-transport's driver, with a socket for each of
/// its connections, spent 5.5 µs more an exchange holding 512 than holding one (its `connections`
/// bench, Linux). A group's members are armed as a `Ready` is, one-shot on the shard's driver for the
/// polling task, each with a key its owner chooses. When a member fires, or its registration is
/// refused, the shard puts it last on the group's list, and [`ReadyGroup::next_fired`] takes the list
/// in that order: a wake costs what fired. The group holds one wait slot of its own (a share of
/// `interests_per_shard`) from its first member on, on the shard that member was armed on; armed on
/// another shard, it opens there afresh. Dropped, it lets go of its slot and of the members on its list.
#[derive(Debug, Default)]
pub struct ReadyGroup {
    /// The group's slot and the task word it was opened for, while it holds one.
    open: Option<(Ticket, Encoded)>,
}

/// A member a [`ReadyGroup`] told fired: its key, and `Ok` when its handle fired or the refusal when
/// its registration was refused.
pub type Fired = (u64, Result<(), RtError>);

/// One armed member of a [`ReadyGroup`]. Its wait ends when its group says it fired, or when the
/// member is dropped while armed, which withdraws the wait as a dropped [`Ready`] does. Dropped after
/// its group told it fired, it does nothing.
#[derive(Debug)]
pub struct Member {
    target: Target,
    writable: bool,
    armed: Option<(Ticket, Encoded)>,
}

impl Drop for Member {
    fn drop(&mut self) {
        abandon(self.target.raw(), self.writable, self.armed.take());
    }
}

impl ReadyGroup {
    /// A group holding no slot yet.
    #[must_use]
    pub const fn new() -> Self {
        Self { open: None }
    }

    /// Arms a one-shot wait on `target`'s readability (or writability) for the task polling with `cx`, a
    /// member keyed `key`. Refused `Capacity` when every wait slot is taken, `NotOnShardThread` off a
    /// shard's task.
    pub(crate) fn arm(
        &mut self,
        cx: &Context<'_>,
        target: Target,
        writable: bool,
        key: u64,
    ) -> Result<Member, RtError> {
        let Some(word) = polling_task(cx.waker()) else {
            return Err(RtError::NotOnShardThread);
        };
        let group = self.open_here(word)?;
        let registered = registry::with_current(|ctx| match target {
            Target::Os(raw) => ctx.group_member(group, Some(raw), writable, word, key),
            Target::Sim(index) => {
                ctx.group_member(group, None, writable, word, key)
                    .and_then(|ticket| {
                        crate::sim::sim_register(index, writable, word, ticket)
                            .map(|()| ticket)
                            .inspect_err(|_| {
                                ctx.abandon_interest(None, writable, word, ticket);
                            })
                    })
            }
        });
        match registered {
            Some(Ok(ticket)) => Ok(Member {
                target,
                writable,
                armed: Some((ticket, word)),
            }),
            Some(Err(e)) => Err(e),
            None => Err(RtError::NotOnShardThread),
        }
    }

    /// The group's slot on the current shard: the one it holds, or one opened for the task `word` names
    /// where it holds none here (none yet, another shard's, or one that ended).
    fn open_here(&mut self, word: Encoded) -> Result<Ticket, RtError> {
        if let Some((ticket, _)) = self.open
            && registry::with_current(|ctx| ctx.group_live(ticket)).unwrap_or(false)
        {
            return Ok(ticket);
        }
        self.close();
        let ticket = registry::with_current(|ctx| ctx.group_open(word))
            .unwrap_or(Err(RtError::NotOnShardThread))?;
        self.open = Some((ticket, word));
        Ok(ticket)
    }

    /// The next member that fired since the last call, in the order they fired: its key, and `Ok` when its
    /// handle fired or the refusal when its registration was refused. That member's wait has ended: its
    /// owner arms it again to wait again. `None` when none fired since. Refused `DriverLost` once the
    /// shard's driver is gone.
    pub fn next_fired(&mut self) -> Result<Option<Fired>, RtError> {
        let Some((ticket, _)) = self.open else {
            return Ok(None);
        };
        registry::with_current(|ctx| ctx.group_next(ticket)).unwrap_or(Ok(None))
    }

    /// Lets go of the group's slot and the members on its list, as a dropped wait is let go.
    fn close(&mut self) {
        abandon(None, false, self.open.take());
    }
}

impl Drop for ReadyGroup {
    fn drop(&mut self) {
        self.close();
    }
}

impl Drop for Ready {
    /// A wait dropped while armed (a race lost, a task cancelled) leaves the shard's table — its own node,
    /// by its ticket — so abandoned waits never accumulate there (mantle's review, findings 8 and 1).
    fn drop(&mut self) {
        self.abandon();
    }
}

/// Awaits `raw`'s readability once (a real socket fd, a simulated fabric port, or a bridge
/// queue's doorbell descriptor).
pub async fn readable(raw: i32) -> Result<(), RtError> {
    ready(Target::Os(raw), false).await
}

/// Awaits `raw`'s writability once (a real socket whose send buffer filled, or a connect in progress).
pub async fn writable(raw: i32) -> Result<(), RtError> {
    ready(Target::Os(raw), true).await
}

/// One readiness edge of `target`.
pub(crate) fn ready(target: Target, writable: bool) -> Ready {
    Ready {
        target,
        interest: if writable {
            Interest::Writable
        } else {
            Interest::Readable
        },
        armed: None,
    }
}
