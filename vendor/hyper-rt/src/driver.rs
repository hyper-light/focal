//! The driver seam: every driver blocks until a kick, a completion or a deadline, and hands back
//! completions as `(user_data, result)` pairs; a `Kick` is the thread-safe handle any thread
//! uses to wake a shard's driver (§4.3, "the three OS drivers with a common completion seam").
//!
//! Phase 0 carries the seam itself, the kick, the wait with a deadline, a pending check, and a
//! no-op operation whose completion proves the path; sockets, files and the bridge queues arrive
//! with their phases and use the same `wait`. Unix kicks carry a registry generation, not a
//! borrowed descriptor. The registry pins each borrow until its syscall finishes (§4.3).

use crate::error::RtError;
use crate::registry::SlotHolder;

/// A finished operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Completion {
    /// The word the submitter attached (a packed waker word, or a driver-private tag).
    pub user_data: u64,
    /// The operation's result as the OS reports it (bytes, or a negated errno).
    pub result: i32,
}

/// Which driver a shard runs on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DriverKind {
    /// Linux epoll with an eventfd kick.
    Epoll,
    /// macOS / BSD kqueue with an `EVFILT_USER` kick.
    Kqueue,
    /// Windows I/O completion ports.
    Iocp,
    /// The deterministic simulation.
    Simulation,
}

impl DriverKind {
    /// The name as the profile and the counters print it.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Epoll => "epoll",
            Self::Kqueue => "kqueue",
            Self::Iocp => "iocp",
            Self::Simulation => "simulation",
        }
    }
}

/// The thread-safe handle that wakes a driver from anywhere. Unix descriptors and simulation
/// flags are reached by registry generation, under a reader pin. Retirement waits for existing
/// borrows, and a copied kick cannot address a later registration in the same slot.
#[derive(Clone, Copy, Debug)]
pub enum Kick {
    /// Write eight bytes to an eventfd (Linux epoll).
    #[cfg(target_os = "linux")]
    Eventfd(KickFd),
    /// Trigger the `EVFILT_USER` event on a kqueue (macOS / BSD).
    #[cfg(any(target_os = "macos", target_os = "freebsd"))]
    Kqueue(KickFd),
    /// Post a completion packet to a completion port (Windows) the registry owns.
    #[cfg(target_os = "windows")]
    Iocp(KickPort),
    /// Set the simulation's kicked flag.
    Sim(SlotHolder),
    /// No driver to kick (registry entries in tests).
    None,
}

/// A copyable, generational name for a registry-owned descriptor (§4.3, D-8).
/// Each borrow is counted by the registry; retirement removes the entry from lookup,
/// waits out existing borrowers, and only then closes the descriptor. No reference escapes.
#[cfg(unix)]
#[derive(Clone, Copy, Debug)]
pub struct KickFd {
    holder: SlotHolder,
}

#[cfg(unix)]
impl KickFd {
    pub(crate) fn new(holder: SlotHolder) -> Self {
        Self { holder }
    }

    /// Runs `f` while this registration's descriptor is pinned. A stale kick is a typed miss.
    pub fn with<R>(&self, f: impl FnOnce(&std::os::fd::OwnedFd) -> R) -> Option<R> {
        crate::registry::with_holder(self.holder, |entry| entry.kick_fd.as_ref().map(f)).flatten()
    }

    /// The owning runtime may hand this number to its driver or IPC while its shards live.
    /// Foreign callers that need a descriptor beyond this call must duplicate it inside `with`.
    pub fn raw(&self) -> Option<i32> {
        use std::os::fd::AsRawFd;
        self.with(|fd| fd.as_raw_fd())
    }
}

/// A copyable, generational name for a registry-owned completion port (Windows; §4.3, D-8): the
/// counterpart of [`KickFd`]. The registry closes the port only after the shard's contexts ended and
/// every foreign borrow of it returned, so a kick copied before retirement is a typed miss afterwards,
/// never a packet posted to a handle value the process has since reused.
#[cfg(windows)]
#[derive(Clone, Copy, Debug)]
pub struct KickPort {
    holder: SlotHolder,
}

#[cfg(windows)]
impl KickPort {
    pub(crate) fn new(holder: SlotHolder) -> Self {
        Self { holder }
    }

    /// Runs `f` with the port's raw handle value while this registration's port is pinned.
    pub fn with<R>(&self, f: impl FnOnce(usize) -> R) -> Option<R> {
        crate::registry::with_holder(self.holder, |entry| {
            entry.kick_port.as_ref().map(|port| f(port.raw()))
        })
        .flatten()
    }

    /// The port's raw handle value, for the owning shard's driver: valid while the shard lives, since
    /// its registration is retired only after its thread has ended.
    pub fn raw(&self) -> Option<usize> {
        self.with(|raw| raw)
    }
}

impl Kick {
    /// A kick that does nothing.
    pub const fn none() -> Kick {
        Kick::None
    }

    /// Wakes the driver. Errors are ignored: a closed driver belongs to a shard that exited, and
    /// the word it would have read stays in the ring for no one, which is the documented outcome.
    pub fn kick(&self) {
        match self {
            #[cfg(target_os = "linux")]
            Kick::Eventfd(fd) => {
                let _ = fd.with(|fd| rustix::io::write(fd, &1u64.to_ne_bytes()));
            }
            #[cfg(any(target_os = "macos", target_os = "freebsd"))]
            Kick::Kqueue(kq) => crate::kqueue::trigger(kq),
            #[cfg(target_os = "windows")]
            Kick::Iocp(port) => {
                let _ = port.with(crate::iocp::post_kick);
            }
            Kick::Sim(holder) => {
                let _ = crate::registry::with_holder(*holder, |entry| {
                    if let Some(shared) = &entry.sim_shared {
                        shared.set_kicked();
                    }
                });
            }
            Kick::None => {}
        }
    }
}

/// The seam every driver implements. A driver is built on the thread that runs it, from a
/// [`DriverSeed`] the runtime prepared, so it may hold buffers of raw kernel records.
pub trait Driver {
    /// Which driver this is.
    fn kind(&self) -> DriverKind;

    /// The kick that wakes this driver from any thread.
    fn kick_handle(&self) -> Kick;

    /// Monotonic nanoseconds (virtual under simulation).
    fn now_ns(&self) -> u64;

    /// Blocks until kicked, a completion arrives, or `timeout_ns` passes (`None` waits without
    /// bound). Completions are appended to `out`. Returns `DriverLost` when the driver died.
    fn wait(&mut self, timeout_ns: Option<u64>, out: &mut Vec<Completion>) -> Result<(), RtError>;

    /// Submits an operation that completes with `user_data` on a following `wait` (the seam's
    /// self-test; every real operation follows the same path).
    fn submit_nop(&mut self, user_data: u64) -> Result<(), RtError>;

    /// Arms one-shot interest in `raw`'s readiness in the directions `want` (readable: data, a connection to
    /// accept, end of stream, an error; writable: send-buffer space, a connect's result), replacing any
    /// earlier arming of `raw`: when one of them next holds, a completion carrying `tag` and the directions
    /// that fired (`result`, as [`crate::interests::Readiness`] bits) arrives on a following `wait`. The loop
    /// keeps the waiters per handle and arms the union (`crate::interests`). `raw` is the OS handle (a `RawFd`
    /// on Unix, a `SOCKET` on Windows): epoll `EPOLLIN`/`EPOLLOUT`, kqueue `EVFILT_READ`/`EVFILT_WRITE`, IOCP
    /// one AFD poll per socket.
    fn arm(&mut self, raw: i32, want: crate::interests::Readiness, tag: u64)
    -> Result<(), RtError>;

    /// Takes back `raw`'s registration: no wait is left on it. A one-shot registration that fires to no
    /// one costs nothing (epoll, kqueue: the default does nothing); IOCP cancels the handle's AFD poll so its
    /// per-handle state is bounded by the handles waited on.
    fn disarm(&mut self, _raw: i32) {}

    /// Reserves the driver's per-handle state for `handles` handles (`interests_per_shard`), once before the
    /// first registration. `Capacity` when the reservation fails.
    fn reserve_handles(&mut self, _handles: usize) -> Result<(), RtError> {
        Ok(())
    }

    /// Whether a `wait` would return a completion or a kick without blocking, as far as the driver
    /// can tell without a syscall (an idle loop skips the wait when this is false).
    fn has_pending(&self) -> bool;

    /// The clock the shard's tasks read: the one [`Driver::now_ns`] reads, live.
    fn clock(&self) -> Clock {
        Clock::Published
    }

    /// Whether this is the simulation driver, so a `UdpSocket` uses the deterministic in-memory fabric
    /// instead of a real socket (§4.10a). Only the simulation driver overrides this.
    fn is_sim(&self) -> bool {
        false
    }
}

/// The clock a shard's tasks read ([`crate::shard::ShardContext::now_ns`]): the driver's own, read live,
/// so a task that measures time inside one poll sees it pass.
#[derive(Clone, Copy, Debug)]
pub enum Clock {
    /// Host monotonic nanoseconds since the driver's epoch, itself a host monotonic reading (the OS drivers;
    /// [`crate::machine::clock::monotonic_ns`]).
    Since(u64),
    /// The simulation's virtual clock.
    Sim(&'static crate::sim::SimShared),
    /// No live clock: the time the loop last published (a test double's driver).
    Published,
}

/// What builds a driver on the shard's thread: a closure the runtime prepared with the OS
/// resources the kick needs (created up front, so the kick is known before the thread exists).
pub type DriverSeed = Box<dyn FnOnce(Kick) -> Result<Box<dyn Driver>, RtError> + Send>;

/// A prepared driver: the seed, the kick it will answer to, and the notes of the probe.
pub struct Prepared {
    /// Builds the driver on the shard's thread, over the kick the registry slot handed the shard
    /// (the slot owns the kick's descriptor).
    pub seed: DriverSeed,
    /// The kick descriptor the slot takes ownership of (Unix: the eventfd or kqueue); `None` where the
    /// kick is not a descriptor (Windows' completion port, the simulation).
    #[cfg(unix)]
    pub kick_fd: Option<std::os::fd::OwnedFd>,
    /// Windows: the completion port, which the slot owns and closes at retirement.
    #[cfg(windows)]
    pub kick_fd: Option<crate::iocp::Port>,
    /// Elsewhere: no OS driver.
    #[cfg(not(any(unix, windows)))]
    pub kick_fd: Option<()>,
    /// What was probed and chosen.
    pub notes: Vec<String>,
}

impl std::fmt::Debug for Prepared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Prepared")
            .field("notes", &self.notes)
            .finish()
    }
}

/// Prepares the OS driver for this platform: epoll over the kick eventfd (io_uring is not carried,
/// docs/runtime.md §3.7).
#[cfg(target_os = "linux")]
pub fn os_driver(_ring_entries: u32) -> Result<Prepared, RtError> {
    let efd = crate::epoll::prepare_eventfd()?;
    Ok(Prepared {
        kick_fd: Some(efd),
        seed: Box::new(|kick| match kick {
            Kick::Eventfd(efd) => {
                Ok(Box::new(crate::epoll::EpollDriver::with_eventfd(efd)?) as Box<dyn Driver>)
            }
            _ => Err(RtError::DriverRefused {
                call: "epoll driver without its eventfd",
                code: None,
            }),
        }),
        notes: Vec::new(),
    })
}

/// Prepares the OS driver for this platform.
#[cfg(any(target_os = "macos", target_os = "freebsd"))]
pub fn os_driver(_ring_entries: u32) -> Result<Prepared, RtError> {
    Ok(Prepared {
        kick_fd: Some(crate::kqueue::prepare()?),
        seed: Box::new(|kick| match kick {
            Kick::Kqueue(kq) => {
                Ok(Box::new(crate::kqueue::KqueueDriver::from_prepared(kq)) as Box<dyn Driver>)
            }
            _ => Err(RtError::DriverRefused {
                call: "kqueue driver without its queue",
                code: None,
            }),
        }),
        notes: Vec::new(),
    })
}

/// Prepares the OS driver for this platform.
#[cfg(target_os = "windows")]
pub fn os_driver(_ring_entries: u32) -> Result<Prepared, RtError> {
    Ok(Prepared {
        kick_fd: Some(crate::iocp::prepare()?),
        seed: Box::new(|kick| match kick {
            Kick::Iocp(port) => {
                Ok(Box::new(crate::iocp::IocpDriver::from_prepared(port)?) as Box<dyn Driver>)
            }
            _ => Err(RtError::DriverRefused {
                call: "IOCP driver without its completion port",
                code: None,
            }),
        }),
        notes: Vec::new(),
    })
}

/// Host monotonic nanoseconds since `epoch_ns` (an earlier [`crate::machine::clock::monotonic_ns`] reading),
/// saturating at zero.
pub fn nanos_since(epoch_ns: u64) -> u64 {
    crate::machine::clock::monotonic_ns().saturating_sub(epoch_ns)
}

/// A rustix refusal as the driver's typed error.
#[cfg(unix)]
pub(crate) fn refused(call: &'static str, e: rustix::io::Errno) -> RtError {
    RtError::DriverRefused {
        call,
        code: Some(e.raw_os_error()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// AC-0.6: the real driver wakes for a kick, delivers a no-op, and supports a timed wait
    /// despite another kick arriving before its deadline. All registered resources are retired.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn the_os_driver_wakes_on_a_kick_and_delivers_a_nop() {
        let prepared = os_driver(64).unwrap();
        let notes = prepared.notes.clone();
        // Exercise the real registration and retirement protocol, including the foreign kick.
        // The registration outlives its driver and retires even when an assertion unwinds (its drop):
        // otherwise the parallel registry stress test can fill its abandoned ring and wait forever.
        let (registration, _control) =
            crate::registry::register(2, 1, crate::runtime::register_kick(prepared.kick_fd))
                .unwrap();
        let shard = registration.shard();
        let kick = crate::registry::with_entry(shard, |entry| entry.kick).unwrap();
        let mut driver = (prepared.seed)(kick).unwrap();
        eprintln!("driver {} notes {notes:?}", driver.kind().name());
        // The registry's kick, the one every sender uses (`kick_if_parked`), not the driver's own
        // handle: a registration whose kick reached nothing (Windows registered none until 2026-09-26)
        // passed while the driver's handle was the one tested.
        let mut out = Vec::new();
        // A kick from another thread ends an unbounded wait.
        std::thread::scope(|scope| {
            let kicker = scope.spawn(move || kick.kick());
            driver.wait(None, &mut out).unwrap();
            kicker.join().unwrap();
        });
        // A wait can return another kick before the no-op. Its deadline belongs to the whole
        // observation, not to one driver call (the same contract the shard loop uses).
        driver.submit_nop(0xABCD).unwrap();
        let deadline = driver.now_ns() + 1_000_000_000;
        while !out.iter().any(|completion| completion.user_data == 0xABCD)
            && driver.now_ns() < deadline
        {
            driver
                .wait(Some(deadline.saturating_sub(driver.now_ns())), &mut out)
                .unwrap();
        }
        assert!(out.iter().any(|c| c.user_data == 0xABCD), "{out:?}");
        // Force the extra kick that the parallel registry stress test may deliver. A timed
        // wait may return early for it; continue toward the same absolute deadline.
        kick.kick();
        let before = driver.now_ns();
        let deadline = before + 2_000_000;
        while driver.now_ns() < deadline {
            driver
                .wait(Some(deadline.saturating_sub(driver.now_ns())), &mut out)
                .unwrap();
        }
        drop(driver);
        drop(registration);
    }

    /// Shape: zero-timeout harvests the test makes with nothing ready — enough that a harvest which
    /// sleeps shows up as that many voluntary switches, far past any stray block of the test thread.
    #[cfg(unix)]
    const IDLE_HARVESTS: u64 = 256;

    /// Format: the user word the test's readiness registration carries.
    #[cfg(unix)]
    const PIPE_TAG: u64 = 0x5EAD;

    /// §4.3 (a spinning shard polls the driver without blocking): a zero-timeout wait — the harvest a
    /// spinning or busy shard makes between tasks — delivers a readiness that is already there on its
    /// first call and never puts the thread to sleep. io_uring asked for one completion with a zero
    /// timeout, which sends the kernel down its sleeping path (arm a timer, schedule out, wake on
    /// expiry): 0.3–1.0 ms per harvest on a loaded Linux VM, a millisecond on every NFS request
    /// (`docs/bugs/2026-09-26-io-uring-zero-timeout-harvest-sleeps.md`). The sleep is observed as the
    /// thread's voluntary context switches, where the OS counts them per thread (Linux).
    ///
    /// The count is the thread's, but what blocks it can be the process's: beside the other tests' threads,
    /// a page fault in the wait path waits for the address-space lock while another thread unmaps, and
    /// counts as a voluntary switch (one in ten parallel runs on Linux 6.12, 2026-10-06). So the measurement
    /// runs in a child process of this test binary that runs only this test, where no other thread
    /// allocates.
    #[test]
    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    fn a_zero_timeout_wait_delivers_what_is_ready_and_never_sleeps() {
        /// Format: the variable that tells the child it is the isolated run.
        const ISOLATED: &str = "HYPER_RT_ZERO_TIMEOUT_ISOLATED";
        if std::env::var_os(ISOLATED).is_none() {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "driver::tests::a_zero_timeout_wait_delivers_what_is_ready_and_never_sleeps",
                    "--test-threads=1",
                    "--nocapture",
                ])
                .env(ISOLATED, "1")
                .output()
                .unwrap();
            let text = String::from_utf8_lossy(&output.stdout);
            assert!(
                output.status.success() && text.contains("1 passed"),
                "the isolated run failed: {text}{}",
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }
        let prepared = os_driver(64).unwrap();
        // The registration outlives its driver and retires even when an assertion unwinds (its drop):
        // otherwise the parallel registry stress test can fill its abandoned ring and wait forever.
        let (registration, _control) =
            crate::registry::register(2, 1, crate::runtime::register_kick(prepared.kick_fd))
                .unwrap();
        let shard = registration.shard();
        let kick = crate::registry::with_entry(shard, |entry| entry.kick).unwrap();
        let mut driver = (prepared.seed)(kick).unwrap();
        let (reader, writer) = rustix::pipe::pipe().unwrap();
        driver
            .arm(
                std::os::fd::AsRawFd::as_raw_fd(&reader),
                crate::interests::Readiness::READ,
                PIPE_TAG,
            )
            .unwrap();
        rustix::io::write(&writer, &[1]).unwrap();
        let mut out = Vec::new();
        driver.wait(Some(0), &mut out).unwrap();
        assert!(
            out.iter()
                .any(|completion| completion.user_data == PIPE_TAG),
            "{}: a readiness already there was not delivered by one zero-timeout wait: {out:?}",
            driver.kind().name()
        );
        out.clear();
        let before = crate::attribution::voluntary_switches_now();
        for _ in 0..IDLE_HARVESTS {
            driver.wait(Some(0), &mut out).unwrap();
        }
        let after = crate::attribution::voluntary_switches_now();
        match before.zip(after) {
            Some((before, after)) => assert_eq!(
                after - before,
                0,
                "{}: {IDLE_HARVESTS} zero-timeout waits blocked the thread {} times",
                driver.kind().name(),
                after - before
            ),
            None => eprintln!(
                "SKIP (loud): {} — this OS counts no per-thread voluntary switches; delivery was checked",
                driver.kind().name()
            ),
        }
        drop(driver);
        drop(registration);
    }
}
