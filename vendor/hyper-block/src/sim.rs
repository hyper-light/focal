//! A simulated file with power-loss semantics, for testing what a storage engine does when the
//! machine loses power, the disk returns errors, or the bytes come back wrong: a [`BlockFile`] over
//! hyper-sim's block device (`hyper_sim::storage`, `docs/sim.md` §3.5), which holds the model, its
//! faults and its crash states. What this layer adds is a direct-I/O file's refusals (alignment, the
//! largest file) and hyper-block's errors. Everything random comes from one seed, so a failing run
//! replays exactly.

use std::cell::{RefCell, RefMut};

use hyper_sim::storage::{Device, StorageError};

use crate::DiskError;
use crate::block::BlockFile;
use crate::buf::Alignment;

mod shared;
pub use shared::{SimDevice, SimHandle};

pub use hyper_sim::storage::{Crash, Fault, Stats as SimStats};

/// The largest simulated file: tests hold two copies of it in memory.
pub const MAX_SIM_LEN: u64 = hyper_sim::storage::MAX_LEN;

/// A simulated file with power-loss semantics. It has one owner, as every [`BlockFile`] does: a
/// log's device thread while the log runs, the test once the log has given it back. Several threads
/// that each need a handle onto one simulated device use [`SimDevice`] instead.
#[derive(Debug)]
pub struct SimFile {
    disk: RefCell<Disk>,
    align: Alignment,
}

impl SimFile {
    /// An empty simulated file whose transfers must meet `align` and whose writes tear at
    /// `sector` granularity (a power of two no larger than the alignment; 512 models a
    /// 512e drive under 4 KiB I/O).
    pub fn new(align: Alignment, sector: Alignment, seed: u64) -> Result<Self, DiskError> {
        Ok(Self {
            disk: RefCell::new(Disk::new(align, sector, seed)?),
            align,
        })
    }

    /// Arms a fault. Read faults persist until cleared; the others fire once.
    pub fn inject(&self, fault: Fault) -> Result<(), DiskError> {
        self.lock()?.inject(fault);
        Ok(())
    }

    /// Disarms every fault.
    pub fn clear_faults(&self) -> Result<(), DiskError> {
        self.lock()?.clear_faults();
        Ok(())
    }

    /// Loses power: unflushed sectors survive per `crash`, and what reads return becomes
    /// what survived.
    pub fn crash(&self, crash: Crash) -> Result<(), DiskError> {
        self.lock()?.crash(crash);
        Ok(())
    }

    /// The operations counted so far.
    pub fn stats(&self) -> Result<SimStats, DiskError> {
        Ok(self.lock()?.stats())
    }

    /// A copy of what would survive a crash that kept nothing unflushed.
    pub fn durable_image(&self) -> Result<Vec<u8>, DiskError> {
        Ok(self.lock()?.durable_image())
    }

    /// The file's state, for one operation: no operation runs inside another, so it is never
    /// borrowed already, and a borrow that fails is refused rather than unwound.
    fn lock(&self) -> Result<RefMut<'_, Disk>, DiskError> {
        self.disk
            .try_borrow_mut()
            .map_err(|_| sim_error("simulated file reentered"))
    }
}

/// The simulated device itself (hyper-sim's) and the name its errors carry. [`SimFile`] holds one in
/// a cell for its single owner; [`SimDevice`]'s device thread owns one and serves every handle from
/// it, so the two behave alike by construction. Every transfer reaching it is one [`check`] has
/// already accepted.
#[derive(Debug)]
pub(crate) struct Disk {
    device: Device,
    name: std::path::PathBuf,
}

impl Disk {
    pub(crate) fn new(align: Alignment, sector: Alignment, seed: u64) -> Result<Self, DiskError> {
        if sector > align {
            return Err(sim_error("sector larger than the alignment"));
        }
        let sector = u64::try_from(sector.get()).map_err(|_| sim_error("sector"))?;
        Ok(Self {
            device: Device::new(sector, seed).map_err(|e| sim_error(&e.to_string()))?,
            name: std::path::PathBuf::from(format!("sim-{seed:016x}")),
        })
    }

    pub(crate) fn inject(&mut self, fault: Fault) {
        self.device.inject(fault);
    }

    pub(crate) fn clear_faults(&mut self) {
        self.device.clear_faults();
    }

    pub(crate) fn crash(&mut self, crash: Crash) {
        self.device.crash(crash);
    }

    pub(crate) fn stats(&self) -> SimStats {
        self.device.stats()
    }

    pub(crate) fn durable_image(&self) -> Vec<u8> {
        self.device.durable_image().to_vec()
    }

    pub(crate) fn len(&self) -> Result<u64, DiskError> {
        Ok(self.device.len())
    }

    /// The device's refusal as the file's: an EIO or ENOSPC as the OS would give it, a read past
    /// the end as a short read, the rest as the simulation's own.
    fn error(&self, error: StorageError) -> DiskError {
        let io = |op, kind| DiskError::Io {
            op,
            path: self.name.clone(),
            source: std::io::Error::from(kind),
        };
        match error {
            StorageError::Io { op } => io(op, std::io::ErrorKind::Other),
            StorageError::NoSpace => io("write", std::io::ErrorKind::StorageFull),
            StorageError::ShortRead { offset, missing } => DiskError::ShortRead {
                path: self.name.clone(),
                offset,
                missing,
            },
            other => sim_error(&other.to_string()),
        }
    }

    /// Fills `buf` from `offset`.
    pub(crate) fn read(&mut self, buf: &mut [u8], offset: u64) -> Result<(), DiskError> {
        self.device.read(buf, offset).map_err(|e| self.error(e))
    }

    /// Writes `buf` at `offset` into the volatile cache.
    pub(crate) fn write(&mut self, buf: &[u8], offset: u64) -> Result<(), DiskError> {
        self.device.write(buf, offset).map_err(|e| self.error(e))
    }

    /// The write, durable on its own as a FUA write is: its sectors reach the medium before it
    /// returns, and every other sector not yet flushed stays as it was. One operation, as a power
    /// cut counts it.
    pub(crate) fn write_durable(
        &mut self,
        buf: &[u8],
        offset: u64,
    ) -> Result<crate::block::Durable, DiskError> {
        self.device
            .write_durable(buf, offset)
            .map(|()| crate::block::Durable::Written)
            .map_err(|e| self.error(e))
    }

    /// Makes every sector the device holds unflushed durable, whichever handle wrote it: a flush
    /// is the file's and the device's, not the descriptor's.
    pub(crate) fn sync(&mut self) -> Result<(), DiskError> {
        self.device.sync().map_err(|e| self.error(e))
    }
}

/// Refuses what a direct-I/O file would refuse: misaligned offsets, lengths and buffer
/// addresses, so tests catch an engine that would fail on a real `O_DIRECT` file, and transfers
/// past the largest simulated file.
pub(crate) fn check(
    align: Alignment,
    addr: usize,
    offset: u64,
    len: usize,
) -> Result<(), DiskError> {
    // A transfer of no bytes has no first byte to align (`DeviceFile::check_address`).
    if !align.is_aligned_u64(offset)
        || !align.is_aligned(len)
        || (len != 0 && !align.is_aligned(addr))
    {
        return Err(DiskError::Misaligned {
            offset,
            len,
            align: align.get(),
        });
    }
    let len = u64::try_from(len).map_err(|_| sim_error("length"))?;
    let end = offset.checked_add(len).ok_or_else(|| sim_error("offset"))?;
    if end > MAX_SIM_LEN {
        return Err(sim_error("simulated file larger than MAX_SIM_LEN"));
    }
    Ok(())
}

pub(crate) fn sim_error(what: &str) -> DiskError {
    DiskError::Io {
        op: "sim",
        path: std::path::PathBuf::from("sim"),
        source: std::io::Error::new(std::io::ErrorKind::InvalidInput, what.to_owned()),
    }
}

impl BlockFile for SimFile {
    fn alignment(&self) -> Alignment {
        self.align
    }

    fn len(&self) -> Result<u64, DiskError> {
        self.lock()?.len()
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), DiskError> {
        check(self.align, buf.as_ptr().addr(), offset, buf.len())?;
        self.lock()?.read(buf, offset)
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), DiskError> {
        check(self.align, buf.as_ptr().addr(), offset, buf.len())?;
        self.lock()?.write(buf, offset)
    }

    /// The sim stands for a direct file on Linux, so that one run's bytes are every platform's.
    fn fills_new_space(&self) -> bool {
        true
    }

    /// The write, durable on its own as a FUA write is (`Disk::write_durable`).
    fn write_durable_at(
        &self,
        buf: &[u8],
        offset: u64,
    ) -> Result<crate::block::Durable, DiskError> {
        check(self.align, buf.as_ptr().addr(), offset, buf.len())?;
        self.lock()?.write_durable(buf, offset)
    }

    fn sync_data(&self) -> Result<(), DiskError> {
        self.lock()?.sync()
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
