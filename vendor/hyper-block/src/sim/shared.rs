//! One simulated device under several handles, each `Send`, for tests whose engine runs threads
//! that each open their own handle onto one file: a writer and its maintenance workers, a volume and
//! its issuer.
//!
//! **One owner, reached by messages.** The device ([`Disk`], the same model [`SimFile`] runs) is
//! owned by one thread, started with the device; every handle, and the [`SimDevice`] the test keeps
//! for power and faults, talks to it over a bounded channel and waits for its answer. No state is
//! shared, so nothing is locked or counted by reference (the workspace's wall). A scoped arena the
//! handles borrow from would need the device to be `Sync`, which means a lock; the generational
//! handles of `docs/sim.md` §5 serve a single-threaded world, not threads that each own a handle.
//!
//! **What follows from one owner.** The device runs one operation at a time, in the order they
//! reach it, as a device's command queue is one order. So:
//! - a write through any handle lands in the one volatile cache, and a read through any other
//!   handle that reaches the device after it returns it;
//! - a flush through any handle makes durable every sector the device holds unflushed, whichever
//!   handle wrote it: fdatasync(2) flushes "all modified in-core data of the file referred to by
//!   fd", the file's and not the descriptor's, and F_FULLFSYNC (fcntl(2), macOS) asks the drive to
//!   flush all buffered data to permanent storage;
//! - [`SimDevice::crash`] cuts power between two operations, so every handle sees the same lost
//!   sectors, torn writes and faults [`SimFile::crash`] gives; an armed [`Fault::PowerCut`] counts
//!   writes and flushes from every handle.
//!
//! Every crash and fault replays from the seed for one order of operations; threads that race
//! choose that order themselves, so a test that needs the replay orders its threads' operations by
//! the facts it waits on.
//!
//! **Bounds.** At most `handles` handles are open at once; another is refused until one drops.
//! Each link has at most one request out, so the request queue holds one per link and a send never
//! waits for room. A handle's transfer buffer, reused for every transfer, grows to its largest
//! transfer, at most [`MAX_SIM_LEN`]. The device's thread is drawn from the process's thread budget
//! ([`crate::threads`]) and ends when the last handle and the [`SimDevice`] are dropped.

use std::cell::Cell;
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};

use super::{Crash, Disk, Fault, SimStats, check, sim_error};
#[cfg(doc)]
use super::{MAX_SIM_LEN, SimFile};
use crate::DiskError;
use crate::block::{BlockFile, Durable};
use crate::buf::Alignment;

/// The test's hold on a simulated device: it makes handles, arms faults and cuts power.
#[derive(Debug)]
pub struct SimDevice {
    link: Link,
    align: Alignment,
}

/// A handle onto a [`SimDevice`]: a [`BlockFile`] of its own, `Send`, to be moved to the thread
/// that issues its I/O.
#[derive(Debug)]
pub struct SimHandle {
    link: Link,
    align: Alignment,
}

/// One end of the device's queue: the slot the device answers through, and the transfer buffer.
struct Link {
    slot: usize,
    submit: SyncSender<Request>,
    replies: Receiver<Reply>,
    scratch: Cell<Vec<u8>>,
}

impl std::fmt::Debug for Link {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Link")
            .field("slot", &self.slot)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct Request {
    slot: usize,
    op: Op,
}

#[derive(Debug)]
enum Op {
    Read { buf: Vec<u8>, offset: u64, end: u64 },
    Write { buf: Vec<u8>, offset: u64, end: u64 },
    WriteDurable { buf: Vec<u8>, offset: u64, end: u64 },
    Sync,
    Len,
    Open,
    Close,
    Inject(Fault),
    ClearFaults,
    Crash(Crash),
    Stats,
    Image,
}

#[derive(Debug)]
enum Reply {
    /// A read or a write, with the transfer buffer handed back.
    Transfer(Result<(), DiskError>, Vec<u8>),
    Durable(Result<Durable, DiskError>, Vec<u8>),
    Done(Result<(), DiskError>),
    Len(Result<u64, DiskError>),
    Opened(Result<(usize, Receiver<Reply>), DiskError>),
    Stats(SimStats),
    Image(Vec<u8>),
}

/// The slot the [`SimDevice`] itself answers through; handles take the slots after it.
const DEVICE_SLOT: usize = 0;

impl SimDevice {
    /// An empty simulated device as [`SimFile::new`] makes one, under at most `handles` handles at
    /// once. Its thread is drawn from the process's thread budget; refused when none is left or
    /// the OS refuses it.
    #[allow(
        clippy::disallowed_methods,
        reason = "the simulated device's one owning thread (module docs): one per device, never one per handle or operation"
    )]
    pub fn new(
        align: Alignment,
        sector: Alignment,
        seed: u64,
        handles: usize,
    ) -> Result<Self, DiskError> {
        let disk = Disk::new(align, sector, seed)?;
        let links = handles.checked_add(1).ok_or_else(|| sim_error("handles"))?;
        let name = std::path::PathBuf::from(format!("sim-{seed:016x}"));
        let budget = crate::threads::reserve(1, &name)?;
        let (submit, requests) = sync_channel(links);
        let (reply, replies) = sync_channel(1);
        let mut answer = Vec::with_capacity(links);
        answer.push(Some(reply));
        answer.resize_with(links, || None);
        std::thread::Builder::new()
            .name("hyper-block-sim".into())
            .spawn(move || {
                serve(disk, &requests, answer);
                drop(budget);
            })
            .map_err(|source| DiskError::Io {
                op: "start a simulated device",
                path: name,
                source,
            })?;
        Ok(Self {
            link: Link {
                slot: DEVICE_SLOT,
                submit,
                replies,
                scratch: Cell::new(Vec::new()),
            },
            align,
        })
    }

    /// A new handle onto the device; refused when `handles` are open already.
    pub fn handle(&self) -> Result<SimHandle, DiskError> {
        self.link.open(self.align)
    }

    /// Arms a fault, as [`SimFile::inject`] does, for every handle.
    pub fn inject(&self, fault: Fault) -> Result<(), DiskError> {
        self.link.done(Op::Inject(fault))
    }

    /// Disarms every fault.
    pub fn clear_faults(&self) -> Result<(), DiskError> {
        self.link.done(Op::ClearFaults)
    }

    /// Loses power under every handle, as [`SimFile::crash`] does: unflushed sectors survive per
    /// `crash`, and what every handle reads becomes what survived.
    pub fn crash(&self, crash: Crash) -> Result<(), DiskError> {
        self.link.done(Op::Crash(crash))
    }

    /// The operations counted so far, through every handle.
    pub fn stats(&self) -> Result<SimStats, DiskError> {
        match self.link.call(Op::Stats)? {
            Reply::Stats(stats) => Ok(stats),
            _ => Err(mismatched()),
        }
    }

    /// A copy of what would survive a crash that kept nothing unflushed.
    pub fn durable_image(&self) -> Result<Vec<u8>, DiskError> {
        match self.link.call(Op::Image)? {
            Reply::Image(image) => Ok(image),
            _ => Err(mismatched()),
        }
    }
}

impl Link {
    /// Sends `op` and waits for the device's answer: the device answers every request, so the
    /// wait ends with the answer or with the device gone.
    fn call(&self, op: Op) -> Result<Reply, DiskError> {
        self.submit
            .send(Request {
                slot: self.slot,
                op,
            })
            .map_err(|_| stopped())?;
        self.replies.recv().map_err(|_| stopped())
    }

    fn done(&self, op: Op) -> Result<(), DiskError> {
        match self.call(op)? {
            Reply::Done(outcome) => outcome,
            _ => Err(mismatched()),
        }
    }

    /// A new handle onto the device, transferring at `align`.
    fn open(&self, align: Alignment) -> Result<SimHandle, DiskError> {
        let Reply::Opened(opened) = self.call(Op::Open)? else {
            return Err(mismatched());
        };
        let (slot, replies) = opened?;
        Ok(SimHandle {
            link: Link {
                slot,
                submit: self.submit.clone(),
                replies,
                scratch: Cell::new(Vec::new()),
            },
            align,
        })
    }
}

impl Drop for Link {
    /// Gives the slot back. The queue holds one request per link and this link has none out, so the
    /// send never waits; a device already gone needs nothing given back.
    fn drop(&mut self) {
        if self.slot != DEVICE_SLOT {
            let _ = self.submit.send(Request {
                slot: self.slot,
                op: Op::Close,
            });
        }
    }
}

/// The device's thread: runs each request on the one [`Disk`] in the order they arrive, and answers
/// the link that sent it. Ends when every link has been dropped.
fn serve(mut disk: Disk, requests: &Receiver<Request>, mut answer: Vec<Option<SyncSender<Reply>>>) {
    while let Ok(Request { slot, op }) = requests.recv() {
        let reply = match op {
            Op::Close => {
                if let Some(entry) = answer.get_mut(slot) {
                    *entry = None;
                }
                continue;
            }
            Op::Open => Reply::Opened(open(&mut answer)),
            Op::Read {
                mut buf,
                offset,
                end,
            } => {
                let outcome = disk.read(&mut buf, offset, end);
                Reply::Transfer(outcome, buf)
            }
            Op::Write { buf, offset, end } => {
                let outcome = disk.write(&buf, offset, end);
                Reply::Transfer(outcome, buf)
            }
            Op::WriteDurable { buf, offset, end } => {
                let outcome = disk.write_durable(&buf, offset, end);
                Reply::Durable(outcome, buf)
            }
            Op::Sync => Reply::Done(disk.sync()),
            Op::Len => Reply::Len(disk.len()),
            Op::Inject(fault) => {
                disk.inject(fault);
                Reply::Done(Ok(()))
            }
            Op::ClearFaults => {
                disk.clear_faults();
                Reply::Done(Ok(()))
            }
            Op::Crash(crash) => {
                disk.crash(crash);
                Reply::Done(Ok(()))
            }
            Op::Stats => Reply::Stats(disk.stats()),
            Op::Image => Reply::Image(disk.durable_image()),
        };
        if let Some(Some(link)) = answer.get(slot) {
            // The link waits for exactly this answer in a queue of one; a link gone has no one to
            // tell.
            let _ = link.send(reply);
        }
    }
}

/// Takes a free slot for a new handle and makes its answer queue.
fn open(answer: &mut [Option<SyncSender<Reply>>]) -> Result<(usize, Receiver<Reply>), DiskError> {
    let (slot, entry) = answer
        .iter_mut()
        .enumerate()
        .skip(DEVICE_SLOT.saturating_add(1))
        .find(|(_, entry)| entry.is_none())
        .ok_or_else(|| sim_error("every handle of the simulated device is open"))?;
    let (reply, replies) = sync_channel(1);
    *entry = Some(reply);
    Ok((slot, replies))
}

fn stopped() -> DiskError {
    sim_error("the simulated device's thread has ended")
}

fn mismatched() -> DiskError {
    sim_error("the simulated device answered another request")
}

impl SimHandle {
    /// Sends a write of `buf` to the device through the reused transfer buffer.
    fn write_with(
        &self,
        buf: &[u8],
        offset: u64,
        make: fn(Vec<u8>, u64, u64) -> Op,
    ) -> Result<Reply, DiskError> {
        let end = check(self.align, buf.as_ptr().addr(), offset, buf.len())?;
        let mut scratch = self.link.scratch.take();
        scratch.clear();
        scratch.extend_from_slice(buf);
        self.link.call(make(scratch, offset, end))
    }

    /// Puts the transfer buffer back for the next transfer.
    fn keep(&self, buf: Vec<u8>) {
        self.link.scratch.set(buf);
    }
}

impl BlockFile for SimHandle {
    fn alignment(&self) -> Alignment {
        self.align
    }

    fn len(&self) -> Result<u64, DiskError> {
        match self.link.call(Op::Len)? {
            Reply::Len(len) => len,
            _ => Err(mismatched()),
        }
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), DiskError> {
        let end = check(self.align, buf.as_ptr().addr(), offset, buf.len())?;
        let mut scratch = self.link.scratch.take();
        scratch.clear();
        scratch.resize(buf.len(), 0);
        let Reply::Transfer(outcome, scratch) = self.link.call(Op::Read {
            buf: scratch,
            offset,
            end,
        })?
        else {
            return Err(mismatched());
        };
        let copied = outcome.and_then(|()| {
            buf.copy_from_slice(scratch.get(..buf.len()).ok_or_else(mismatched)?);
            Ok(())
        });
        self.keep(scratch);
        copied
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), DiskError> {
        let Reply::Transfer(outcome, scratch) =
            self.write_with(buf, offset, |buf, offset, end| Op::Write {
                buf,
                offset,
                end,
            })?
        else {
            return Err(mismatched());
        };
        self.keep(scratch);
        outcome
    }

    /// The sim stands for a direct file on Linux, as [`SimFile`] does.
    fn fills_new_space(&self) -> bool {
        true
    }

    /// The write, durable on its own as a FUA write is, as [`SimFile`]'s.
    fn write_durable_at(&self, buf: &[u8], offset: u64) -> Result<Durable, DiskError> {
        let Reply::Durable(outcome, scratch) =
            self.write_with(buf, offset, |buf, offset, end| Op::WriteDurable {
                buf,
                offset,
                end,
            })?
        else {
            return Err(mismatched());
        };
        self.keep(scratch);
        outcome
    }

    /// Makes durable every sector the device holds unflushed, through whichever handle it was
    /// written (module docs).
    fn sync_data(&self) -> Result<(), DiskError> {
        self.link.done(Op::Sync)
    }

    /// Another handle onto the same device; refused when every handle is open.
    fn try_clone(&self) -> Result<Self, DiskError> {
        self.link.open(self.align)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buf::AlignedBuf;
    use crate::sim::SimFile;

    fn align() -> Alignment {
        Alignment::new(4096).unwrap()
    }

    fn device(seed: u64, handles: usize) -> SimDevice {
        SimDevice::new(align(), Alignment::new(512).unwrap(), seed, handles).unwrap()
    }

    fn block(byte: u8) -> AlignedBuf {
        let mut b = AlignedBuf::zeroed(4096, align()).unwrap();
        b.as_mut_capacity().fill(byte);
        b.set_len(4096).unwrap();
        b
    }

    fn read(file: &impl BlockFile, offset: u64) -> Vec<u8> {
        let mut back = block(0);
        file.read_exact_at(back.as_mut_slice(), offset).unwrap();
        back.as_slice().to_vec()
    }

    /// One script of writes, flushes, a durable write, a failed flush and a random crash, run on a
    /// `SimFile` and on a device through two handles taken in turn: per seed, every read, the
    /// durable image and the counts are the same, so torn writes and faults are `SimFile`'s.
    #[test]
    fn two_handles_behave_as_one_sim_file() {
        for seed in 0..64 {
            let file = SimFile::new(align(), Alignment::new(512).unwrap(), seed).unwrap();
            let dev = device(seed, 2);
            let (one, two) = (dev.handle().unwrap(), dev.handle().unwrap());
            let script = |a: &dyn BlockFile, b: &dyn BlockFile| -> Vec<bool> {
                vec![
                    a.write_all_at(block(1).as_slice(), 0).is_ok(),
                    b.sync_data().is_ok(),
                    b.write_all_at(block(2).as_slice(), 0).is_ok(),
                    a.write_all_at(block(3).as_slice(), 4096).is_ok(),
                    b.write_durable_at(block(4).as_slice(), 8192).is_ok(),
                    a.write_all_at(block(5).as_slice(), 12288).is_ok(),
                ]
            };
            let on_file = script(&file, &file);
            let on_device = script(&one, &two);
            assert_eq!(on_file, on_device);
            file.inject(Fault::SyncError).unwrap();
            dev.inject(Fault::SyncError).unwrap();
            assert!(file.sync_data().is_err() && two.sync_data().is_err());
            file.write_all_at(block(6).as_slice(), 16384).unwrap();
            one.write_all_at(block(6).as_slice(), 16384).unwrap();
            file.crash(Crash::Random).unwrap();
            dev.crash(Crash::Random).unwrap();
            assert_eq!(file.durable_image().unwrap(), dev.durable_image().unwrap());
            assert_eq!(file.len().unwrap(), two.len().unwrap());
            let len = file.len().unwrap();
            // A crash can keep a block's first sectors and not its last: whole blocks are read,
            // and the image above compares every byte.
            for offset in (0..len.saturating_sub(4095)).step_by(4096) {
                assert_eq!(
                    read(&file, offset),
                    read(&one, offset),
                    "seed {seed} at {offset}"
                );
            }
            assert_eq!(file.stats().unwrap(), dev.stats().unwrap());
        }
    }

    /// A write through a handle on one thread, flushed through another handle on another thread,
    /// survives a crash that keeps nothing unflushed; a write no handle flushed does not, and every
    /// handle reads what survived.
    #[test]
    fn a_flush_through_any_handle_makes_every_handle_s_writes_durable() {
        let dev = device(7, 2);
        let (writer, flusher) = (dev.handle().unwrap(), dev.handle().unwrap());
        let (writer, flusher) = std::thread::scope(|s| {
            let writer = s
                .spawn(move || {
                    writer.write_all_at(block(1).as_slice(), 0).unwrap();
                    writer
                })
                .join()
                .unwrap();
            let flusher = s
                .spawn(move || {
                    flusher.sync_data().unwrap();
                    flusher
                })
                .join()
                .unwrap();
            let writer = s
                .spawn(move || {
                    writer.write_all_at(block(2).as_slice(), 4096).unwrap();
                    writer
                })
                .join()
                .unwrap();
            (writer, flusher)
        });
        assert_eq!(read(&flusher, 4096), block(2).as_slice(), "one cache");
        dev.crash(Crash::LoseAll).unwrap();
        assert_eq!(read(&flusher, 0), block(1).as_slice());
        assert_eq!(writer.len().unwrap(), 4096, "the unflushed write is gone");
        assert_eq!(dev.stats().unwrap().crashes, 1);
    }

    /// An armed power cut counts writes and flushes from every handle, and fails every handle's
    /// after it.
    #[test]
    fn a_power_cut_counts_every_handle() {
        let dev = device(5, 2);
        let (one, two) = (dev.handle().unwrap(), dev.handle().unwrap());
        dev.inject(Fault::PowerCut { ops: 2 }).unwrap();
        one.write_all_at(block(1).as_slice(), 0).unwrap();
        two.sync_data().unwrap();
        assert!(two.write_all_at(block(2).as_slice(), 0).is_err());
        assert!(one.sync_data().is_err());
        dev.crash(Crash::LoseAll).unwrap();
        dev.clear_faults().unwrap();
        assert_eq!(read(&two, 0), block(1).as_slice());
    }

    /// Handles past the bound are refused, by the device and by a clone, until one is dropped.
    #[test]
    fn handles_past_the_bound_are_refused_until_one_drops() {
        let dev = device(1, 2);
        let one = dev.handle().unwrap();
        let two = one.try_clone().unwrap();
        assert!(dev.handle().is_err());
        assert!(two.try_clone().is_err());
        drop(one);
        let three = dev.handle().unwrap();
        three.write_all_at(block(9).as_slice(), 0).unwrap();
        assert_eq!(read(&two, 0), block(9).as_slice());
    }

    /// A handle refuses what a direct-I/O file refuses before anything reaches the device.
    #[test]
    fn a_handle_refuses_misaligned_transfers() {
        let dev = device(4, 1);
        let one = dev.handle().unwrap();
        assert!(matches!(
            one.write_all_at(&[0u8; 100], 0),
            Err(DiskError::Misaligned { .. })
        ));
        assert_eq!(dev.stats().unwrap().writes, 0);
    }
}
