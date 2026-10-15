//! macOS: a disk node's capacity and full flush, from the disk ioctls of <sys/disk.h>.
//!
//! The capacity is DKIOCGETBLOCKCOUNT blocks of DKIOCGETBLOCKSIZE bytes. F_FULLFSYNC, the
//! full flush of a file (fcntl(2)), fails on a disk node with ENOTTY (measured on a disk
//! image's node), and plain fsync(2) leaves data in the drive's volatile cache (fsync(2)).
//! So a node's flush is fsync(2), which writes the node's cached blocks to the device and
//! waits for them, then DKIOCSYNCHRONIZE over the whole media, which asks the device to
//! empty its cache: the request the file systems send their device for F_FULLFSYNC.
#![allow(unsafe_code)]

use std::fs::File;
use std::io;

use rustix::ioctl::{Getter, Opcode, Setter, ioctl, opcode};

/// `dk_synchronize_t` of <sys/disk.h>.
#[repr(C)]
struct Synchronize {
    offset: u64,
    length: u64,
    options: u32,
    reserved: [u8; 4],
}

/// DKIOCGETBLOCKSIZE: `_IOR('d', 24, uint32_t)`.
const BLOCK_SIZE: Opcode = opcode::read::<u32>(b'd', 24);
/// DKIOCGETBLOCKCOUNT: `_IOR('d', 25, uint64_t)`.
const BLOCK_COUNT: Opcode = opcode::read::<u64>(b'd', 25);
/// DKIOCSYNCHRONIZE: `_IOW('d', 22, dk_synchronize_t)`.
const SYNCHRONIZE: Opcode = opcode::write::<Synchronize>(b'd', 22);

pub(crate) fn len(file: &File) -> io::Result<u64> {
    // SAFETY: DKIOCGETBLOCKSIZE writes one uint32_t, the getter's output type.
    let size = unsafe { ioctl(file, Getter::<BLOCK_SIZE, u32>::new()) }?;
    // SAFETY: DKIOCGETBLOCKCOUNT writes one uint64_t, the getter's output type.
    let count = unsafe { ioctl(file, Getter::<BLOCK_COUNT, u64>::new()) }?;
    count.checked_mul(u64::from(size)).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "the device reports more bytes than a u64 holds",
        )
    })
}

pub(crate) fn sync(file: &File) -> io::Result<()> {
    rustix::fs::fsync(file)?;
    // Offset and length zero name the whole media; options zero asks for a flush, where
    // DK_SYNCHRONIZE_OPTION_BARRIER would ask only for ordering.
    let whole = Synchronize {
        offset: 0,
        length: 0,
        options: 0,
        reserved: [0; 4],
    };
    // SAFETY: DKIOCSYNCHRONIZE reads one dk_synchronize_t, whose layout `Synchronize`
    // repeats field for field; the setter owns it for the call.
    unsafe { ioctl(file, Setter::<SYNCHRONIZE, Synchronize>::new(whole)) }?;
    Ok(())
}
