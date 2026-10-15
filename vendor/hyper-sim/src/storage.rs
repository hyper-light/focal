//! The block device (bytes), `docs/sim.md` §3.5, step S-3: a file with power-loss semantics, the
//! faults the literature finds in real stacks, and the crash states a run is judged after.
//!
//! The model is hyper-block's `SimFile` moved here, the same rule for every byte. A write lands in a
//! volatile cache and is durable only after a successful flush; at a crash each sector written since
//! the last flush independently survives or not, so a multi-sector write can tear and later writes
//! can persist without earlier ones (Pillai et al., OSDI 2014, §2.2). A failed flush leaves each of
//! those sectors durable or not while the cache still returns the new bytes, as Linux does after
//! marking the pages clean (Rebello et al., ATC 2020, §3.3). Reads can fail or return flipped bits,
//! writes can fail or meet a full device, the single-block faults of Ganesan et al. (FAST 2017,
//! Table 1). New here, from the same table: a **lost write**, acknowledged and never written to the
//! medium; a **misdirected write**, written to another offset, leaving its target stale and the other
//! corrupt; and a block **read as zeros or junk**.
//!
//! The crash draws come from one SplitMix64 of the device's seed, in the order hyper-block's
//! `SimFile` made them, so a run's bytes are its seed's (hyper-log's equivalence hashes, `docs/sim.md`
//! §9's S-3 gate); junk comes from its own stream of the seed, so a test that reads junk changes no
//! crash.
//!
//! **Crash states** (§3.5): [`Crash`] is the random state (each unflushed sector kept or lost) and its
//! two extremes; a crash after every persistence point (CrashMonkey's B3, Mohan et al., OSDI 2018,
//! §4) is a harness crashing with [`Crash::LoseAll`] at each flush; and the states within a
//! persistence point (ALICE, Pillai et al. §3.3), each prefix of the writes since the last flush and
//! each with one of them omitted, are [`Device::states_since_flush`], over the writes a device records
//! when asked to ([`Device::record`]), up to the bytes it is given.
//!
//! **The typed write queue** ([`Writes`]) is the other device of §3.5: writes the harness defines,
//! made durable in order as the world completes them.

mod writes;

use std::collections::BTreeSet;
use std::fmt;

use crate::rng::{Seeded, stream_seed};

pub use writes::{WriteStats, Writes};

/// A fault the next matching operation suffers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fault {
    /// Reads overlapping `[offset, offset + len)` fail with EIO.
    ReadError {
        /// Where the failing range begins.
        offset: u64,
        /// Its length.
        len: u64,
    },
    /// Reads of the byte at `offset` return it with bit `bit` inverted; the stored byte is intact, as
    /// with a transient transfer error, unless `stored` is set.
    BitFlip {
        /// The byte's offset.
        offset: u64,
        /// The bit flipped, 0 to 7.
        bit: u8,
        /// The flip is on the medium, healed only by a write over it.
        stored: bool,
    },
    /// The next write fails with EIO before changing anything.
    WriteError,
    /// The next flush fails with EIO; each unflushed sector is left durable or not.
    SyncError,
    /// Writes that would grow the device past `len` fail with ENOSPC.
    Capacity {
        /// The device's capacity.
        len: u64,
    },
    /// Power is cut after `ops` more writes and flushes: that operation and every one after it
    /// fails, until the test crashes the device and clears its faults.
    PowerCut {
        /// Writes and flushes that still succeed.
        ops: u64,
    },
    /// The next write is acknowledged and never reaches the medium: reads return it from the cache
    /// until a crash, and no flush makes it durable (a lost write, Ganesan et al. Table 1).
    LostWrite,
    /// The next write lands at `to` instead of its own offset: its target keeps what it held, and
    /// `to` holds what the write carried (a misdirected write, Ganesan et al. Table 1; TigerBeetle's
    /// `storage.zig` misdirects within a zone).
    Misdirected {
        /// Where the write lands, aligned as the write was.
        to: u64,
    },
    /// Reads of `[offset, offset + len)` return zeros there (a block read as zeros, Ganesan et al.
    /// §3.1); the stored bytes are intact.
    ReadZeros {
        /// Where the range begins.
        offset: u64,
        /// Its length.
        len: u64,
    },
    /// Reads of `[offset, offset + len)` return bytes drawn afresh at each read (junk, Ganesan et al.
    /// §3.1); the stored bytes are intact.
    ReadJunk {
        /// Where the range begins.
        offset: u64,
        /// Its length.
        len: u64,
    },
}

/// What a crash does with sectors written since the last successful flush.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Crash {
    /// Each such sector independently survives with probability one half.
    Random,
    /// None survives.
    LoseAll,
    /// Every one survives.
    KeepAll,
}

/// The operations a device counted, so a test can assert what an engine did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Stats {
    /// Reads issued.
    pub reads: u64,
    /// Writes issued.
    pub writes: u64,
    /// Flushes issued.
    pub syncs: u64,
    /// Writes made durable on their own ([`Device::write_durable`]), each also counted a write.
    pub durable_writes: u64,
    /// Crashes the test made.
    pub crashes: u64,
}

/// What the device refuses, typed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageError {
    /// The operation failed as an EIO does: an injected fault or a cut power.
    Io {
        /// The operation: `"read"`, `"write"` or `"sync_data"`.
        op: &'static str,
    },
    /// The write would grow the device past its capacity (ENOSPC).
    NoSpace,
    /// The read reaches past the device's end.
    ShortRead {
        /// Where the read began.
        offset: u64,
        /// The bytes past the end.
        missing: usize,
    },
    /// A transfer past [`MAX_LEN`], or an offset past what this host addresses.
    TooLarge,
    /// A sector of no bytes.
    Sector,
    /// A device recording its writes since the last flush holds `bound` bytes of them already.
    RecordFull {
        /// The bytes the device was given to record.
        bound: usize,
    },
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { op } => write!(f, "{op} failed"),
            Self::NoSpace => f.write_str("no space left on the device"),
            Self::ShortRead { offset, missing } => {
                write!(f, "a read at {offset} reaches {missing} bytes past the end")
            }
            Self::TooLarge => f.write_str("a transfer past the largest simulated device"),
            Self::Sector => f.write_str("a sector of no bytes"),
            Self::RecordFull { bound } => {
                write!(
                    f,
                    "the writes since the last flush fill the {bound} bytes recorded"
                )
            }
        }
    }
}

impl std::error::Error for StorageError {}

/// The largest simulated device: tests hold two copies of it in memory.
pub const MAX_LEN: u64 = 1 << 30;

/// A write the device takes: where it lands, the misdirection it fires, and the bytes recorded with
/// it.
#[derive(Debug, Clone, Copy)]
struct Admitted {
    at: u64,
    end: u64,
    misdirected: Option<usize>,
    recorded: Option<usize>,
}

/// A write since the last flush, as a crash within a persistence point replays it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Pending {
    offset: u64,
    bytes: Vec<u8>,
}

/// A simulated block device: its images, faults, generators and counts.
#[derive(Debug, Clone)]
pub struct Device {
    /// What reads return: the durable image plus writes not yet flushed.
    visible: Vec<u8>,
    /// What survives a crash.
    durable: Vec<u8>,
    /// Sectors written since the last flush.
    dirty: BTreeSet<u64>,
    /// Sectors a lost write holds in the cache alone: never flushed, gone at a crash.
    lost: BTreeSet<u64>,
    faults: Vec<Fault>,
    /// The crash draws, SplitMix64 of the seed itself.
    crashes: Seeded,
    /// The junk reads return, a stream of its own.
    junk: Seeded,
    stats: Stats,
    sector: u64,
    /// The writes since the last flush, kept while the device records them, and the bytes it may.
    since_flush: Vec<Pending>,
    recorded: usize,
    record_bytes: usize,
}

impl Device {
    /// An empty device whose writes tear at `sector` bytes, drawing every crash from `seed`.
    pub fn new(sector: u64, seed: u64) -> Result<Self, StorageError> {
        if sector == 0 {
            return Err(StorageError::Sector);
        }
        Ok(Self {
            visible: Vec::new(),
            durable: Vec::new(),
            dirty: BTreeSet::new(),
            lost: BTreeSet::new(),
            faults: Vec::new(),
            crashes: Seeded::new(seed),
            junk: Seeded::new(stream_seed(seed, "storage.junk", &[])),
            stats: Stats::default(),
            sector,
            since_flush: Vec::new(),
            recorded: 0,
            record_bytes: 0,
        })
    }

    /// Records each write since the last flush, up to `bytes` of them, so that the states within a
    /// persistence point can be made ([`Self::states_since_flush`]); a write past them is refused
    /// ([`StorageError::RecordFull`]). Zero records nothing, the default.
    pub fn record(&mut self, bytes: usize) {
        self.record_bytes = bytes;
        if bytes == 0 {
            self.since_flush.clear();
            self.recorded = 0;
        }
    }

    /// Arms a fault. Read faults persist until cleared; the others fire once.
    pub fn inject(&mut self, fault: Fault) {
        self.faults.push(fault);
    }

    /// Disarms every fault.
    pub fn clear_faults(&mut self) {
        self.faults.clear();
    }

    /// Loses power: unflushed sectors survive per `crash`, a lost write's never, and what reads
    /// return becomes what survived.
    pub fn crash(&mut self, crash: Crash) {
        let dirty: Vec<u64> = std::mem::take(&mut self.dirty).into_iter().collect();
        for sector in dirty {
            let keep = match crash {
                Crash::Random => self.crashes.next_u64() & 1 == 1,
                Crash::LoseAll => false,
                Crash::KeepAll => true,
            };
            if keep {
                self.persist_sector(sector);
            }
        }
        self.lost.clear();
        self.visible = self.durable.clone();
        self.since_flush.clear();
        self.recorded = 0;
        self.stats.crashes = self.stats.crashes.saturating_add(1);
    }

    /// The operations counted so far.
    pub fn stats(&self) -> Stats {
        self.stats
    }

    /// What would survive a crash that kept nothing unflushed.
    pub fn durable_image(&self) -> &[u8] {
        &self.durable
    }

    /// The device's length: the end of the furthest write.
    pub fn len(&self) -> u64 {
        u64::try_from(self.visible.len()).unwrap_or(u64::MAX)
    }

    /// Whether nothing was ever written.
    pub fn is_empty(&self) -> bool {
        self.visible.is_empty()
    }

    /// Fills `buf` from `offset`.
    pub fn read(&mut self, buf: &mut [u8], offset: u64) -> Result<(), StorageError> {
        self.stats.reads = self.stats.reads.saturating_add(1);
        let len = u64::try_from(buf.len()).map_err(|_| StorageError::TooLarge)?;
        let end = offset.checked_add(len).ok_or(StorageError::TooLarge)?;
        let failed = self.faults.iter().any(|f| {
            matches!(f, Fault::ReadError { offset: o, len: l } if overlaps(offset, len, *o, *l))
        });
        if failed {
            return Err(StorageError::Io { op: "read" });
        }
        let (start, stop) = (index(offset)?, index(end)?);
        let Some(src) = self.visible.get(start..stop) else {
            return Err(StorageError::ShortRead {
                offset,
                missing: stop.saturating_sub(self.visible.len().max(start)),
            });
        };
        buf.copy_from_slice(src);
        for fault in &self.faults {
            match fault {
                Fault::BitFlip {
                    offset: at, bit, ..
                } if (offset..end).contains(at) => {
                    let at = usize::try_from(at.saturating_sub(offset)).unwrap_or(usize::MAX);
                    if let Some(byte) = buf.get_mut(at) {
                        *byte ^= 1u8.checked_shl(u32::from(*bit % 8)).unwrap_or(0);
                    }
                }
                Fault::ReadZeros { offset: o, len: l } => {
                    for byte in overlap_mut(buf, offset, *o, *l) {
                        *byte = 0;
                    }
                }
                Fault::ReadJunk { offset: o, len: l } => {
                    for byte in overlap_mut(buf, offset, *o, *l) {
                        *byte = self.junk.next_u64().to_le_bytes()[0];
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Writes `buf` at `offset` into the volatile cache.
    pub fn write(&mut self, buf: &[u8], offset: u64) -> Result<(), StorageError> {
        self.stats.writes = self.stats.writes.saturating_add(1);
        if self.power_cut() {
            return Err(StorageError::Io { op: "write" });
        }
        if let Some(i) = self.faults.iter().position(|f| *f == Fault::WriteError) {
            self.faults.remove(i);
            return Err(StorageError::Io { op: "write" });
        }
        let admitted = self.admit(buf, offset)?;
        // Nothing was taken until here: the write now happens, and fires its faults.
        if let Some(i) = admitted.misdirected {
            self.faults.remove(i);
        }
        let lost = match self.faults.iter().position(|f| *f == Fault::LostWrite) {
            Some(i) => {
                self.faults.remove(i);
                true
            }
            None => false,
        };
        if let Some(recorded) = admitted.recorded {
            self.recorded = recorded;
            if !lost {
                self.since_flush.push(Pending {
                    offset: admitted.at,
                    bytes: buf.to_vec(),
                });
            }
        }
        self.land(buf, admitted.at, admitted.end, lost)
    }

    /// Where a write lands and whether the device takes it: its offset, or a misdirected one's; past
    /// the largest device, the capacity, or what is recorded, refused, typed, with nothing changed.
    fn admit(&self, buf: &[u8], offset: u64) -> Result<Admitted, StorageError> {
        let len = u64::try_from(buf.len()).map_err(|_| StorageError::TooLarge)?;
        let misdirected = self
            .faults
            .iter()
            .position(|f| matches!(f, Fault::Misdirected { .. }));
        let at = misdirected
            .and_then(|i| match self.faults.get(i) {
                Some(Fault::Misdirected { to }) => Some(*to),
                _ => None,
            })
            .unwrap_or(offset);
        let end = at.checked_add(len).ok_or(StorageError::TooLarge)?;
        if end > MAX_LEN {
            return Err(StorageError::TooLarge);
        }
        let capacity = self.faults.iter().find_map(|f| match f {
            Fault::Capacity { len } => Some(*len),
            _ => None,
        });
        if capacity.is_some_and(|cap| end > cap) {
            return Err(StorageError::NoSpace);
        }
        let recorded = if self.record_bytes > 0 && !buf.is_empty() {
            Some(
                self.recorded
                    .checked_add(buf.len())
                    .filter(|r| *r <= self.record_bytes)
                    .ok_or(StorageError::RecordFull {
                        bound: self.record_bytes,
                    })?,
            )
        } else {
            None
        };
        Ok(Admitted {
            at,
            end,
            misdirected,
            recorded,
        })
    }

    /// Puts `buf` in the cache at `[at, end)` and marks its sectors: dirty, or a lost write's, in the
    /// cache alone, which a flush passes by and a crash takes back.
    fn land(&mut self, buf: &[u8], at: u64, end: u64, lost: bool) -> Result<(), StorageError> {
        let (start, stop) = (index(at)?, index(end)?);
        if self.visible.len() < stop {
            self.visible.resize(stop, 0);
        }
        if let Some(dst) = self.visible.get_mut(start..stop) {
            dst.copy_from_slice(buf);
        }
        if !buf.is_empty() {
            let first = at.checked_div(self.sector).unwrap_or(0);
            let last = end.saturating_sub(1).checked_div(self.sector).unwrap_or(0);
            for sector in first..=last {
                let (from, to) = if lost {
                    (&mut self.dirty, &mut self.lost)
                } else {
                    (&mut self.lost, &mut self.dirty)
                };
                from.remove(&sector);
                to.insert(sector);
            }
        }
        // A stored bit flip is re-applied on every read of its byte; a write over it heals it.
        self.faults.retain(|f| {
            !matches!(f, Fault::BitFlip { offset: o, stored: true, .. } if (at..end).contains(o))
        });
        Ok(())
    }

    /// The write, durable on its own as a FUA write is: its sectors reach the medium before it
    /// returns, and every other sector not yet flushed stays as it was, durable at the next flush or
    /// not at a crash. One operation, as a power cut counts it.
    pub fn write_durable(&mut self, buf: &[u8], offset: u64) -> Result<(), StorageError> {
        let lost = self.faults.contains(&Fault::LostWrite);
        let to = self.faults.iter().find_map(|f| match f {
            Fault::Misdirected { to } => Some(*to),
            _ => None,
        });
        self.write(buf, offset)?;
        self.stats.durable_writes = self.stats.durable_writes.saturating_add(1);
        if buf.is_empty() || lost {
            return Ok(());
        }
        let at = to.unwrap_or(offset);
        let len = u64::try_from(buf.len()).map_err(|_| StorageError::TooLarge)?;
        let end = at.checked_add(len).ok_or(StorageError::TooLarge)?;
        let first = at.checked_div(self.sector).unwrap_or(0);
        let last = end.saturating_sub(1).checked_div(self.sector).unwrap_or(0);
        for sector in first..=last {
            self.dirty.remove(&sector);
            self.persist_sector(sector);
        }
        Ok(())
    }

    /// Makes every sector written and not yet flushed durable: a flush is the device's, not a
    /// handle's (fdatasync(2): "all modified in-core data of the file referred to by fd"; fcntl(2)
    /// on macOS, F_FULLFSYNC: the drive flushes all buffered data to permanent storage).
    pub fn sync(&mut self) -> Result<(), StorageError> {
        self.stats.syncs = self.stats.syncs.saturating_add(1);
        if self.power_cut() {
            return Err(StorageError::Io { op: "sync_data" });
        }
        let dirty: Vec<u64> = std::mem::take(&mut self.dirty).into_iter().collect();
        self.since_flush.clear();
        self.recorded = 0;
        if let Some(i) = self.faults.iter().position(|f| *f == Fault::SyncError) {
            self.faults.remove(i);
            // The kernel has marked these pages clean; whether they reached the medium is unknown,
            // and a later flush will not write them again.
            for sector in dirty {
                if self.crashes.next_u64() & 1 == 1 {
                    self.persist_sector(sector);
                }
            }
            return Err(StorageError::Io { op: "sync_data" });
        }
        for sector in dirty {
            self.persist_sector(sector);
        }
        if self.durable.len() < self.visible.len() {
            let len = self.visible.len();
            self.durable.resize(len, 0);
        }
        Ok(())
    }

    /// The states a crash within this persistence point may leave (ALICE, Pillai et al. §3.3):
    /// the durable image with each prefix of the writes since the last flush applied, the empty
    /// prefix first and the whole last, and then with each write omitted from the whole. Made one at
    /// a time; none while the device records nothing ([`Self::record`]).
    pub fn states_since_flush(&self) -> States<'_> {
        States {
            device: self,
            next: 0,
        }
    }

    /// Counts one write or flush against an armed power cut; true once the power is off.
    fn power_cut(&mut self) -> bool {
        for fault in &mut self.faults {
            if let Fault::PowerCut { ops } = fault {
                if *ops == 0 {
                    return true;
                }
                *ops = ops.saturating_sub(1);
            }
        }
        false
    }

    /// Copies one sector from the visible image to the durable one, growing it as needed.
    fn persist_sector(&mut self, sector: u64) {
        let Some(start) = sector
            .checked_mul(self.sector)
            .and_then(|s| usize::try_from(s).ok())
        else {
            return;
        };
        let size = usize::try_from(self.sector).unwrap_or(0);
        let end = start.saturating_add(size).min(self.visible.len());
        if start >= end {
            return;
        }
        if self.durable.len() < end {
            self.durable.resize(end, 0);
        }
        if let (Some(dst), Some(src)) = (
            self.durable.get_mut(start..end),
            self.visible.get(start..end),
        ) {
            dst.copy_from_slice(src);
        }
    }
}

/// The crash states within a persistence point, made one at a time ([`Device::states_since_flush`]).
#[derive(Debug)]
pub struct States<'a> {
    device: &'a Device,
    next: usize,
}

impl Iterator for States<'_> {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Vec<u8>> {
        let writes = &self.device.since_flush;
        let count = writes.len();
        let at = self.next;
        if self.device.record_bytes == 0 || at > count.checked_mul(2)? {
            return None;
        }
        self.next = at.checked_add(1)?;
        let mut image = self.device.durable.clone();
        // States 0..=count are the prefixes; count+1.. omit write (state - count - 1) from the whole.
        let applied = |k: usize| {
            if at <= count {
                k < at
            } else {
                at.checked_sub(count)
                    .and_then(|s| s.checked_sub(1))
                    .is_none_or(|omitted| k != omitted)
            }
        };
        for (k, write) in writes.iter().enumerate() {
            if !applied(k) {
                continue;
            }
            let (Ok(start), Some(stop)) = (
                usize::try_from(write.offset),
                usize::try_from(write.offset)
                    .ok()
                    .and_then(|s| s.checked_add(write.bytes.len())),
            ) else {
                return None;
            };
            if image.len() < stop {
                image.resize(stop, 0);
            }
            if let Some(dst) = image.get_mut(start..stop) {
                dst.copy_from_slice(&write.bytes);
            }
        }
        Some(image)
    }
}

/// An offset as an index into the images.
fn index(offset: u64) -> Result<usize, StorageError> {
    usize::try_from(offset).map_err(|_| StorageError::TooLarge)
}

fn overlaps(offset: u64, len: u64, start: u64, span: u64) -> bool {
    let end = offset.saturating_add(len);
    let fault_end = start.saturating_add(span);
    offset < fault_end && start < end
}

/// The bytes of `buf`, read from `offset`, that `[start, start + span)` covers.
fn overlap_mut(
    buf: &mut [u8],
    offset: u64,
    start: u64,
    span: u64,
) -> impl Iterator<Item = &mut u8> {
    let len = u64::try_from(buf.len()).unwrap_or(u64::MAX);
    let from = start.max(offset).saturating_sub(offset);
    let to = start
        .saturating_add(span)
        .min(offset.saturating_add(len))
        .saturating_sub(offset);
    let (from, to) = (
        usize::try_from(from).unwrap_or(usize::MAX),
        usize::try_from(to).unwrap_or(0),
    );
    buf.get_mut(from..to.max(from)).into_iter().flatten()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn device() -> Device {
        Device::new(512, 7).unwrap()
    }

    /// A lost write reads back from the cache until a crash, survives no flush, and leaves what the
    /// medium held.
    #[test]
    fn a_lost_write_reads_back_and_is_gone_after_a_crash_whatever_was_flushed() {
        let mut d = device();
        d.write(&[1; 1024], 0).unwrap();
        d.sync().unwrap();
        d.inject(Fault::LostWrite);
        d.write(&[2; 512], 0).unwrap();
        let mut b = [0u8; 512];
        d.read(&mut b, 0).unwrap();
        assert_eq!(b, [2; 512]);
        d.sync().unwrap();
        d.crash(Crash::KeepAll);
        d.read(&mut b, 0).unwrap();
        assert_eq!(b, [1; 512], "the medium never held the lost write");
        // The next write is an ordinary one again.
        d.write(&[3; 512], 0).unwrap();
        d.sync().unwrap();
        d.crash(Crash::LoseAll);
        d.read(&mut b, 0).unwrap();
        assert_eq!(b, [3; 512]);
    }

    /// A misdirected write lands at its other offset: the target keeps what it held.
    #[test]
    fn a_misdirected_write_leaves_its_target_stale_and_the_other_corrupt() {
        let mut d = device();
        d.write(&[1; 1024], 0).unwrap();
        d.inject(Fault::Misdirected { to: 512 });
        d.write(&[9; 512], 0).unwrap();
        d.sync().unwrap();
        d.crash(Crash::LoseAll);
        let mut b = [0u8; 1024];
        d.read(&mut b, 0).unwrap();
        assert_eq!(&b[..512], &[1; 512]);
        assert_eq!(&b[512..], &[9; 512]);
    }

    /// Zeros and junk replace what a read returns where they lie, and leave the medium as it was.
    #[test]
    fn reads_of_zeros_and_junk_leave_the_stored_bytes() {
        let mut d = device();
        d.write(&[5; 1024], 0).unwrap();
        d.inject(Fault::ReadZeros {
            offset: 100,
            len: 10,
        });
        d.inject(Fault::ReadJunk {
            offset: 600,
            len: 300,
        });
        let mut b = [0u8; 1024];
        d.read(&mut b, 0).unwrap();
        assert_eq!(&b[100..110], &[0; 10]);
        assert_eq!(&b[..100], &[5; 100]);
        assert_eq!(&b[110..600], &[5; 490]);
        assert_ne!(
            &b[600..900],
            &[5; 300],
            "junk is not the stored byte everywhere"
        );
        assert_eq!(&b[900..], &[5; 124]);
        d.clear_faults();
        d.read(&mut b, 0).unwrap();
        assert_eq!(b, [5; 1024]);
    }

    /// Within a persistence point: every prefix, then every omission, each over the durable image.
    #[test]
    fn the_states_within_a_persistence_point_are_its_prefixes_and_omissions() {
        let mut d = device();
        d.write(&[1; 512], 0).unwrap();
        d.sync().unwrap();
        d.record(1 << 20);
        d.write(&[2; 512], 0).unwrap();
        d.write(&[3; 512], 512).unwrap();
        let first = |s: &[u8]| (s.first().copied(), s.get(512).copied());
        let states: Vec<_> = d.states_since_flush().map(|s| first(&s)).collect();
        assert_eq!(
            states,
            vec![
                (Some(1), None),
                (Some(2), None),
                (Some(2), Some(3)),
                (Some(1), Some(3)),
                (Some(2), None),
            ]
        );
        d.sync().unwrap();
        assert_eq!(d.states_since_flush().count(), 1, "the durable image alone");
    }

    /// Recording past its bytes refuses the write, typed, and changes nothing.
    #[test]
    fn a_write_past_what_is_recorded_is_refused() {
        let mut d = device();
        d.record(512);
        d.write(&[1; 512], 0).unwrap();
        assert_eq!(
            d.write(&[2; 512], 512),
            Err(StorageError::RecordFull { bound: 512 })
        );
        assert_eq!(d.len(), 512);
    }
}
