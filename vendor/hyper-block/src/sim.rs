//! A simulated file with power-loss semantics, for testing what a storage engine does when
//! the machine loses power, the disk returns errors, or the bytes come back wrong.
//!
//! The model follows what the literature shows a real stack does. A write lands in a
//! volatile cache and is durable only after a successful flush; at a crash each sector
//! written since the last flush independently survives or not, so a multi-sector write can
//! tear and later writes can persist without earlier ones (Pillai et al., OSDI 2014). A
//! failed flush leaves each of those sectors durable or not while the cache still returns
//! the new bytes, as Linux does after marking the pages clean (Rebello et al., ATC 2020).
//! Reads can fail or return flipped bits, the single-block faults of Ganesan et al.
//! (FAST 2017). Everything random comes from one seed, so a failing run replays exactly.

use std::cell::{RefCell, RefMut};
use std::collections::BTreeSet;

use crate::DiskError;
use crate::block::BlockFile;
use crate::buf::Alignment;

/// The largest simulated file: tests hold two copies of it in memory.
pub const MAX_SIM_LEN: u64 = 1 << 30;

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
    /// Reads of the byte at `offset` return it with bit `bit` inverted; the stored byte is
    /// intact, as with a transient transfer error, unless `stored` is set.
    BitFlip {
        /// The byte's file offset.
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
    /// Writes that would grow the file past `len` fail with ENOSPC.
    Capacity {
        /// The file's capacity.
        len: u64,
    },
    /// Power is cut after `ops` more writes and flushes: that operation and every one after
    /// it fails, until the test crashes the file and clears its faults.
    PowerCut {
        /// Writes and flushes that still succeed.
        ops: u64,
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

#[derive(Debug)]
struct State {
    /// What reads return: the durable image plus writes not yet flushed.
    visible: Vec<u8>,
    /// What survives a crash.
    durable: Vec<u8>,
    /// Sectors written since the last flush.
    dirty: BTreeSet<u64>,
    faults: Vec<Fault>,
    rng: SplitMix64,
    stats: SimStats,
}

/// Operation counts, so a test can assert what an engine did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SimStats {
    /// Reads issued.
    pub reads: u64,
    /// Writes issued.
    pub writes: u64,
    /// Flushes issued.
    pub syncs: u64,
    /// Writes made durable on their own (`BlockFile::write_durable_at`), each also counted a write.
    pub durable_writes: u64,
    /// Crashes the test made.
    pub crashes: u64,
}

/// A simulated file with power-loss semantics. It has one owner, as every [`BlockFile`] does: a
/// log's device thread while the log runs, the test once the log has given it back.
#[derive(Debug)]
pub struct SimFile {
    state: RefCell<State>,
    align: Alignment,
    sector: u64,
    name: std::path::PathBuf,
}

impl SimFile {
    /// An empty simulated file whose transfers must meet `align` and whose writes tear at
    /// `sector` granularity (a power of two no larger than the alignment; 512 models a
    /// 512e drive under 4 KiB I/O).
    pub fn new(align: Alignment, sector: Alignment, seed: u64) -> Result<Self, DiskError> {
        if sector > align {
            return Err(sim_error("sector larger than the alignment"));
        }
        Ok(Self {
            state: RefCell::new(State {
                visible: Vec::new(),
                durable: Vec::new(),
                dirty: BTreeSet::new(),
                faults: Vec::new(),
                rng: SplitMix64::new(seed),
                stats: SimStats::default(),
            }),
            align,
            sector: u64::try_from(sector.get()).unwrap_or(u64::MAX),
            name: std::path::PathBuf::from(format!("sim-{seed:016x}")),
        })
    }

    /// Arms a fault. Read faults persist until cleared; the others fire once.
    pub fn inject(&self, fault: Fault) -> Result<(), DiskError> {
        self.lock()?.faults.push(fault);
        Ok(())
    }

    /// Disarms every fault.
    pub fn clear_faults(&self) -> Result<(), DiskError> {
        self.lock()?.faults.clear();
        Ok(())
    }

    /// Loses power: unflushed sectors survive per `crash`, and what reads return becomes
    /// what survived.
    pub fn crash(&self, crash: Crash) -> Result<(), DiskError> {
        let mut state = self.lock()?;
        let dirty: Vec<u64> = std::mem::take(&mut state.dirty).into_iter().collect();
        for sector in dirty {
            let keep = match crash {
                Crash::Random => state.rng.next_u64() & 1 == 1,
                Crash::LoseAll => false,
                Crash::KeepAll => true,
            };
            if keep {
                persist_sector(&mut state, sector, self.sector);
            }
        }
        state.visible = state.durable.clone();
        state.stats.crashes = state.stats.crashes.saturating_add(1);
        Ok(())
    }

    /// The operations counted so far.
    pub fn stats(&self) -> Result<SimStats, DiskError> {
        Ok(self.lock()?.stats)
    }

    /// A copy of what would survive a crash that kept nothing unflushed.
    pub fn durable_image(&self) -> Result<Vec<u8>, DiskError> {
        Ok(self.lock()?.durable.clone())
    }

    /// The file's state, for one operation: no operation runs inside another, so it is never
    /// borrowed already, and a borrow that fails is refused rather than unwound.
    fn lock(&self) -> Result<RefMut<'_, State>, DiskError> {
        self.state
            .try_borrow_mut()
            .map_err(|_| sim_error("simulated file reentered"))
    }

    /// Refuses what a direct-I/O file would refuse: misaligned offsets, lengths and buffer
    /// addresses, so tests catch an engine that would fail on a real `O_DIRECT` file.
    fn check(&self, addr: usize, offset: u64, len: usize) -> Result<u64, DiskError> {
        // A transfer of no bytes has no first byte to align (`DeviceFile::check_address`).
        if !self.align.is_aligned_u64(offset)
            || !self.align.is_aligned(len)
            || (len != 0 && !self.align.is_aligned(addr))
        {
            return Err(DiskError::Misaligned {
                offset,
                len,
                align: self.align.get(),
            });
        }
        let len = u64::try_from(len).map_err(|_| sim_error("length"))?;
        let end = offset.checked_add(len).ok_or_else(|| sim_error("offset"))?;
        if end > MAX_SIM_LEN {
            return Err(sim_error("simulated file larger than MAX_SIM_LEN"));
        }
        Ok(end)
    }

    fn io(&self, op: &'static str, kind: std::io::ErrorKind) -> DiskError {
        DiskError::Io {
            op,
            path: self.name.clone(),
            source: std::io::Error::from(kind),
        }
    }
}

/// Copies one sector from the visible image to the durable one, growing it as needed.
fn persist_sector(state: &mut State, sector: u64, size: u64) {
    let Some(start) = sector
        .checked_mul(size)
        .and_then(|s| usize::try_from(s).ok())
    else {
        return;
    };
    let size = usize::try_from(size).unwrap_or(0);
    let end = start.saturating_add(size).min(state.visible.len());
    if start >= end {
        return;
    }
    if state.durable.len() < end {
        state.durable.resize(end, 0);
    }
    if let (Some(dst), Some(src)) = (
        state.durable.get_mut(start..end),
        state.visible.get(start..end),
    ) {
        dst.copy_from_slice(src);
    }
}

/// Counts one write or flush against an armed power cut; true once the power is off.
fn power_cut(state: &mut State) -> bool {
    for fault in &mut state.faults {
        if let Fault::PowerCut { ops } = fault {
            if *ops == 0 {
                return true;
            }
            *ops = ops.saturating_sub(1);
        }
    }
    false
}

fn overlaps(offset: u64, len: u64, start: u64, span: u64) -> bool {
    let end = offset.saturating_add(len);
    let fault_end = start.saturating_add(span);
    offset < fault_end && start < end
}

fn sim_error(what: &str) -> DiskError {
    DiskError::Io {
        op: "sim",
        path: std::path::PathBuf::from("sim"),
        source: std::io::Error::new(std::io::ErrorKind::InvalidInput, what.to_owned()),
    }
}

/// SplitMix64 (Steele, Lea and Flood, "Fast Splittable Pseudorandom Number Generators",
/// OOPSLA 2014): one word of state, so that every crash and fault replays from its seed.
#[derive(Debug, Clone)]
struct SplitMix64(u64);

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

impl BlockFile for SimFile {
    fn alignment(&self) -> Alignment {
        self.align
    }

    fn len(&self) -> Result<u64, DiskError> {
        let state = self.lock()?;
        u64::try_from(state.visible.len()).map_err(|_| sim_error("length"))
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), DiskError> {
        let end = self.check(buf.as_ptr().addr(), offset, buf.len())?;
        let mut state = self.lock()?;
        state.stats.reads = state.stats.reads.saturating_add(1);
        let len = u64::try_from(buf.len()).map_err(|_| sim_error("length"))?;
        let failed = state.faults.iter().any(|f| {
            matches!(f, Fault::ReadError { offset: o, len: l } if overlaps(offset, len, *o, *l))
        });
        if failed {
            return Err(self.io("read", std::io::ErrorKind::Other));
        }
        let (start, stop) = (
            usize::try_from(offset).map_err(|_| sim_error("offset"))?,
            usize::try_from(end).map_err(|_| sim_error("offset"))?,
        );
        let Some(src) = state.visible.get(start..stop) else {
            return Err(DiskError::ShortRead {
                path: self.name.clone(),
                offset,
                missing: stop.saturating_sub(state.visible.len().max(start)),
            });
        };
        buf.copy_from_slice(src);
        for fault in &state.faults {
            if let Fault::BitFlip {
                offset: at, bit, ..
            } = fault
                && (offset..end).contains(at)
            {
                let index = usize::try_from(at.saturating_sub(offset)).unwrap_or(usize::MAX);
                if let Some(byte) = buf.get_mut(index) {
                    *byte ^= 1u8.checked_shl(u32::from(*bit % 8)).unwrap_or(0);
                }
            }
        }
        Ok(())
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), DiskError> {
        let end = self.check(buf.as_ptr().addr(), offset, buf.len())?;
        let mut state = self.lock()?;
        state.stats.writes = state.stats.writes.saturating_add(1);
        if power_cut(&mut state) {
            return Err(self.io("write", std::io::ErrorKind::Other));
        }
        if let Some(i) = state.faults.iter().position(|f| *f == Fault::WriteError) {
            state.faults.remove(i);
            return Err(self.io("write", std::io::ErrorKind::Other));
        }
        let capacity = state.faults.iter().find_map(|f| match f {
            Fault::Capacity { len } => Some(*len),
            _ => None,
        });
        if capacity.is_some_and(|cap| end > cap) {
            return Err(self.io("write", std::io::ErrorKind::StorageFull));
        }
        let (start, stop) = (
            usize::try_from(offset).map_err(|_| sim_error("offset"))?,
            usize::try_from(end).map_err(|_| sim_error("offset"))?,
        );
        if state.visible.len() < stop {
            state.visible.resize(stop, 0);
        }
        if let Some(dst) = state.visible.get_mut(start..stop) {
            dst.copy_from_slice(buf);
        }
        let first = offset.checked_div(self.sector).unwrap_or(0);
        let last = end.saturating_sub(1).checked_div(self.sector).unwrap_or(0);
        if !buf.is_empty() {
            state.dirty.extend(first..=last);
        }
        // A stored bit flip is re-applied on every read of its byte; a write over it heals it.
        state.faults.retain(|f| {
            !matches!(f, Fault::BitFlip { offset: at, stored: true, .. } if (offset..end).contains(at))
        });
        Ok(())
    }

    /// The sim stands for a direct file on Linux, so that one run's bytes are every platform's.
    fn fills_new_space(&self) -> bool {
        true
    }

    /// The write, durable on its own as a FUA write is: its sectors reach the medium before it
    /// returns, and every other sector not yet flushed stays as it was, durable at the next flush
    /// or not at a crash. One operation, as a power cut counts it.
    fn write_durable_at(
        &self,
        buf: &[u8],
        offset: u64,
    ) -> Result<crate::block::Durable, DiskError> {
        self.write_all_at(buf, offset)?;
        let mut state = self.lock()?;
        state.stats.durable_writes = state.stats.durable_writes.saturating_add(1);
        if buf.is_empty() {
            return Ok(crate::block::Durable::Written);
        }
        let end = offset.saturating_add(u64::try_from(buf.len()).unwrap_or(u64::MAX));
        let first = offset.checked_div(self.sector).unwrap_or(0);
        let last = end.saturating_sub(1).checked_div(self.sector).unwrap_or(0);
        for sector in first..=last {
            state.dirty.remove(&sector);
            persist_sector(&mut state, sector, self.sector);
        }
        Ok(crate::block::Durable::Written)
    }

    fn sync_data(&self) -> Result<(), DiskError> {
        let mut state = self.lock()?;
        state.stats.syncs = state.stats.syncs.saturating_add(1);
        if power_cut(&mut state) {
            return Err(self.io("sync_data", std::io::ErrorKind::Other));
        }
        let dirty: Vec<u64> = std::mem::take(&mut state.dirty).into_iter().collect();
        if let Some(i) = state.faults.iter().position(|f| *f == Fault::SyncError) {
            state.faults.remove(i);
            // The kernel has marked these pages clean; whether they reached the medium is
            // unknown, and a later flush will not write them again.
            for sector in dirty {
                if state.rng.next_u64() & 1 == 1 {
                    persist_sector(&mut state, sector, self.sector);
                }
            }
            return Err(self.io("sync_data", std::io::ErrorKind::Other));
        }
        for sector in dirty {
            persist_sector(&mut state, sector, self.sector);
        }
        if state.durable.len() < state.visible.len() {
            let len = state.visible.len();
            state.durable.resize(len, 0);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buf::AlignedBuf;

    fn file(seed: u64) -> SimFile {
        SimFile::new(
            Alignment::new(4096).unwrap(),
            Alignment::new(512).unwrap(),
            seed,
        )
        .unwrap()
    }

    fn aligned(len: usize, byte: u8) -> AlignedBuf {
        let mut b = AlignedBuf::zeroed(len, Alignment::new(4096).unwrap()).unwrap();
        b.as_mut_capacity().fill(byte);
        b.set_len(len).unwrap();
        b
    }

    fn block(byte: u8) -> AlignedBuf {
        aligned(4096, byte)
    }

    fn bytes(b: &AlignedBuf) -> Vec<u8> {
        b.as_slice().to_vec()
    }

    /// A write made durable on its own survives a power cut; a write before it that no flush has
    /// made durable does not, as with a FUA write; both survive the next flush; and the durable
    /// write is one operation, counted once.
    #[test]
    fn a_durable_write_persists_itself_and_nothing_else() {
        let f = file(11);
        let (one, two, three) = (block(1), block(2), block(3));
        f.write_all_at(one.as_slice(), 0).unwrap();
        f.sync_data().unwrap();
        f.write_all_at(two.as_slice(), 4096).unwrap();
        assert_eq!(
            f.write_durable_at(three.as_slice(), 8192).unwrap(),
            crate::block::Durable::Written
        );
        let stats = f.stats().unwrap();
        assert_eq!((stats.writes, stats.syncs, stats.durable_writes), (3, 1, 1));
        let image = f.durable_image().unwrap();
        assert_eq!(&image[..4096], bytes(&block(1)).as_slice());
        assert!(
            image[4096..8192].iter().all(|&b| b == 0),
            "the unflushed write was made durable"
        );
        assert_eq!(&image[8192..12288], bytes(&block(3)).as_slice());
        f.crash(Crash::LoseAll).unwrap();
        let mut back = block(0);
        f.read_exact_at(back.as_mut_slice(), 8192).unwrap();
        assert_eq!(bytes(&back), bytes(&block(3)));
        f.read_exact_at(back.as_mut_slice(), 4096).unwrap();
        assert_eq!(bytes(&back), vec![0; 4096]);
    }

    #[test]
    fn flushed_writes_survive_any_crash() {
        let f = file(1);
        f.write_all_at(block(7).as_slice(), 0).unwrap();
        f.sync_data().unwrap();
        f.crash(Crash::LoseAll).unwrap();
        let mut buf = block(0);
        f.read_exact_at(buf.as_mut_slice(), 0).unwrap();
        assert_eq!(bytes(&buf), bytes(&block(7)));
    }

    #[test]
    fn unflushed_writes_are_lost_or_kept_as_the_crash_says() {
        let f = file(2);
        f.write_all_at(block(1).as_slice(), 0).unwrap();
        f.sync_data().unwrap();
        f.write_all_at(block(2).as_slice(), 0).unwrap();
        f.crash(Crash::LoseAll).unwrap();
        let mut buf = block(0);
        f.read_exact_at(buf.as_mut_slice(), 0).unwrap();
        assert_eq!(bytes(&buf), bytes(&block(1)));
        f.write_all_at(block(3).as_slice(), 0).unwrap();
        f.crash(Crash::KeepAll).unwrap();
        f.read_exact_at(buf.as_mut_slice(), 0).unwrap();
        assert_eq!(bytes(&buf), bytes(&block(3)));
    }

    #[test]
    fn random_crashes_tear_writes_at_sector_granularity() {
        let mut torn = false;
        for seed in 0..64 {
            let f = file(seed);
            f.write_all_at(block(1).as_slice(), 0).unwrap();
            f.sync_data().unwrap();
            f.write_all_at(block(2).as_slice(), 0).unwrap();
            f.crash(Crash::Random).unwrap();
            let mut buf = block(0);
            f.read_exact_at(buf.as_mut_slice(), 0).unwrap();
            let got = bytes(&buf);
            for sector in got.chunks(512) {
                assert!(sector.iter().all(|&b| b == sector[0]), "a sector tore");
                assert!(sector[0] == 1 || sector[0] == 2);
            }
            torn |= got.chunks(512).any(|s| s[0] == 1) && got.chunks(512).any(|s| s[0] == 2);
        }
        assert!(torn, "no seed produced a torn write");
    }

    /// The simulated file takes a transfer of no bytes as the real one does: an empty buffer
    /// moves nothing at every alignment (`file`'s `an_empty_buffer_transfers_nothing_at_every_alignment`).
    #[test]
    fn an_empty_buffer_transfers_nothing() {
        let f = file(0);
        for shift in 0..=crate::buf::MAX_ALIGNMENT.trailing_zeros() {
            let align = Alignment::new(1 << shift).unwrap();
            let mut empty = crate::buf::AlignedBuf::zeroed(0, align).unwrap();
            f.write_all_at(empty.as_slice(), 0).unwrap();
            f.read_exact_at(empty.as_mut_capacity(), 0).unwrap();
        }
    }

    #[test]
    fn a_failed_flush_leaves_the_cache_lying_about_durability() {
        let mut lost = false;
        for seed in 0..32 {
            let f = file(seed);
            f.write_all_at(aligned(8192, 9).as_slice(), 0).unwrap();
            f.inject(Fault::SyncError).unwrap();
            assert!(f.sync_data().is_err());
            // Until the crash, the cache still returns the new bytes.
            let mut buf = aligned(8192, 0);
            f.read_exact_at(buf.as_mut_slice(), 0).unwrap();
            assert!(buf.as_slice().iter().all(|&b| b == 9));
            // A second flush "succeeds" without writing anything.
            f.sync_data().unwrap();
            f.crash(Crash::LoseAll).unwrap();
            let image = f.durable_image().unwrap();
            lost |= image.len() < 8192 || image.iter().any(|&b| b != 9);
        }
        assert!(lost, "no seed lost data after a failed flush");
    }

    #[test]
    fn faults_fire_as_armed() {
        let f = file(3);
        f.write_all_at(block(5).as_slice(), 0).unwrap();
        f.inject(Fault::BitFlip {
            offset: 10,
            bit: 3,
            stored: false,
        })
        .unwrap();
        let mut buf = block(0);
        f.read_exact_at(buf.as_mut_slice(), 0).unwrap();
        assert_eq!(buf.as_slice()[10], 5 ^ 8);
        f.clear_faults().unwrap();
        f.inject(Fault::ReadError {
            offset: 4000,
            len: 1,
        })
        .unwrap();
        assert!(f.read_exact_at(buf.as_mut_slice(), 0).is_err());
        f.clear_faults().unwrap();
        f.inject(Fault::WriteError).unwrap();
        assert!(f.write_all_at(block(6).as_slice(), 0).is_err());
        f.write_all_at(block(6).as_slice(), 0).unwrap();
        f.inject(Fault::Capacity { len: 8192 }).unwrap();
        assert!(f.write_all_at(block(6).as_slice(), 8192).is_err());
        f.write_all_at(block(6).as_slice(), 4096).unwrap();
    }

    #[test]
    fn a_power_cut_fails_everything_after_its_count() {
        let f = file(5);
        f.inject(Fault::PowerCut { ops: 2 }).unwrap();
        f.write_all_at(block(1).as_slice(), 0).unwrap();
        f.sync_data().unwrap();
        assert!(f.write_all_at(block(2).as_slice(), 0).is_err());
        assert!(f.sync_data().is_err());
        f.crash(Crash::LoseAll).unwrap();
        f.clear_faults().unwrap();
        let mut buf = block(0);
        f.read_exact_at(buf.as_mut_slice(), 0).unwrap();
        assert_eq!(bytes(&buf), bytes(&block(1)));
    }

    #[test]
    fn misaligned_transfers_are_refused() {
        let f = file(4);
        assert!(matches!(
            f.write_all_at(&[0u8; 100], 0),
            Err(DiskError::Misaligned { .. })
        ));
        // An aligned length at an unaligned address is refused too, as O_DIRECT would.
        let buf = aligned(8192, 1);
        assert!(matches!(
            f.write_all_at(&buf.as_slice()[1..4097], 0),
            Err(DiskError::Misaligned { .. })
        ));
    }
}
