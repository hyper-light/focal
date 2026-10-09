//! Keys in memory (`docs/seal.md` §8): one region of locked pages for the whole process, sized once
//! by the consumer's stated count of keys held at once, each key a 32-byte slot in it.
//!
//! - **Locked** against swap: `mlock(2)` on Linux and macOS, `VirtualLock` on Windows. A region the
//!   OS will not lock is refused at [`lock_keys`], never used unlocked.
//! - **Out of core dumps** where the OS offers it: `MADV_DONTDUMP` on Linux (since 3.4).
//! - **Wiped** when a key is dropped, with volatile writes and a fence, so the wipe is never elided.
//! - **Bounded**: a slot is claimed through an atomic bitmap; past the stated count a key is refused
//!   ([`SealError::Capacity`]), and pages are locked once, not a page a key, so the process stays
//!   inside its locked-memory limit.
//!
//! Each slot has one owner at a time, the [`Secret32`] that claimed it, which is what makes reading
//! and writing through the region's pointer sound. The region lives as long as the process.
#![allow(unsafe_code)]

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering, compiler_fence};

use crate::SealError;

/// Bytes of a key slot.
const SLOT: usize = 32;

/// The process's locked region: made once, by the first [`lock_keys`], or the reason it could not
/// be. Every other caller waits for that first one to finish, so no key is asked of a region still
/// being made.
static REGION: OnceLock<Result<Region, SealError>> = OnceLock::new();

struct Region {
    /// The first byte, page-aligned; `len` bytes from here are locked and ours.
    base: usize,
    slots: usize,
    /// One bit a slot, set while a [`Secret32`] owns it.
    claimed: Box<[AtomicU64]>,
    held: AtomicUsize,
}

/// The OS's page size: `sysconf(_SC_PAGESIZE)` through rustix on Unix; 4 KiB on Windows, on both its
/// targets (x86_64 and ARM64), per Microsoft's "Memory management: page size" reference.
fn page_size() -> usize {
    #[cfg(unix)]
    {
        rustix::param::page_size()
    }
    #[cfg(windows)]
    {
        4096
    }
}

/// Locks a region for `count` keys held at once, for the life of the process, before the first key
/// is made. The first call makes it, and a region the OS will not lock is refused (the process's
/// locked-memory limit: `RLIMIT_MEMLOCK` on Unix, the minimum working set on Windows). A later call
/// is answered by the region made: `Ok` if it holds `count` keys, [`SealError::AlreadyLocked`] with
/// its count if it holds fewer; so a library and its host, or the tests of one process, may each
/// ask for what they need.
pub fn lock_keys(count: usize) -> Result<(), SealError> {
    let mut made = false;
    let region = REGION.get_or_init(|| {
        made = true;
        make_region(count)
    });
    match (made, region) {
        (true, Ok(_)) => Ok(()),
        (_, Err(e)) => Err(*e),
        // A region already made serves any later caller it holds enough for: tests in one process,
        // a library and its host. One that holds fewer says how many it holds.
        (false, Ok(r)) if r.slots >= count => Ok(()),
        (false, Ok(r)) => Err(SealError::AlreadyLocked { slots: r.slots }),
    }
}

/// The region of `count` keys: allocated, locked and out of core dumps, or why not.
fn make_region(count: usize) -> Result<Region, SealError> {
    if count == 0 {
        return Err(SealError::Capacity);
    }
    let page = page_size();
    let bytes = count.checked_mul(SLOT).ok_or(SealError::Capacity)?;
    let len = bytes
        .div_ceil(page)
        .checked_mul(page)
        .ok_or(SealError::Capacity)?;
    let slots = len / SLOT;
    let region = allocate(len, page)?;
    let words = slots.div_ceil(64);
    let claimed: Box<[AtomicU64]> = (0..words).map(|_| AtomicU64::new(0)).collect();
    Ok(Region {
        base: region,
        slots,
        claimed,
        held: AtomicUsize::new(0),
    })
}

/// The region, once made.
fn region() -> Option<&'static Region> {
    REGION.get().and_then(|r| r.as_ref().ok())
}

/// The slots the region has, and how many keys hold one now: what a consumer checks after dropping
/// every key on a suspend notification (§8), and what it sizes against.
pub fn keys_held() -> Option<(usize, usize)> {
    region().map(|r| (r.slots, r.held.load(Ordering::Acquire)))
}

/// Allocates `len` bytes aligned to `page`, zeroed, locked, and out of core dumps where the OS
/// offers it. The allocation is never freed: the region lives as long as the process.
fn allocate(len: usize, page: usize) -> Result<usize, SealError> {
    let layout = std::alloc::Layout::from_size_align(len, page).map_err(|_| SealError::Capacity)?;
    // SAFETY: `layout` has a non-zero size (count ≥ 1 slot rounded up to a page) and a power-of-two
    // alignment (the OS's page size), as `alloc_zeroed` requires.
    let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
    if ptr.is_null() {
        return Err(SealError::Capacity);
    }
    lock(ptr, len)?;
    Ok(ptr as usize)
}

#[cfg(unix)]
fn lock(ptr: *mut u8, len: usize) -> Result<(), SealError> {
    // SAFETY: `ptr..ptr+len` is one live allocation of ours, page-aligned and a whole number of
    // pages, which is what mlock(2) and madvise(2) take.
    unsafe { rustix::mm::mlock(ptr.cast(), len) }.map_err(|_| SealError::Lock)?;
    #[cfg(any(target_os = "linux", target_os = "android"))]
    // SAFETY: as above; MADV_DONTDUMP changes only whether the range is written to a core dump.
    unsafe { rustix::mm::madvise(ptr.cast(), len, rustix::mm::Advice::LinuxDontDump) }
        .map_err(|_| SealError::Lock)?;
    Ok(())
}

#[cfg(windows)]
fn lock(ptr: *mut u8, len: usize) -> Result<(), SealError> {
    // SAFETY: `ptr..ptr+len` is one live, committed allocation of ours; VirtualLock reads only the
    // range's address and length.
    let locked = unsafe { windows_sys::Win32::System::Memory::VirtualLock(ptr.cast(), len) };
    if locked == 0 {
        return Err(SealError::Lock);
    }
    Ok(())
}

/// 32 secret bytes in a slot of the locked region: a key at any level. Never `Clone`, never printed,
/// wiped and released when dropped.
pub struct Secret32 {
    slot: usize,
}

impl Secret32 {
    /// A zeroed slot, to be filled. Refused before [`lock_keys`] or past its count.
    pub(crate) fn zeroed() -> Result<Self, SealError> {
        #[cfg(test)]
        test_region();
        let region = region().ok_or(SealError::Capacity)?;
        for (word_index, word) in region.claimed.iter().enumerate() {
            let mut bits = word.load(Ordering::Acquire);
            while bits != u64::MAX {
                let free = bits.trailing_ones();
                let slot = word_index
                    .checked_mul(64)
                    .and_then(|s| s.checked_add(usize::try_from(free).ok()?))
                    .ok_or(SealError::Capacity)?;
                if slot >= region.slots {
                    break;
                }
                let bit = 1u64 << free;
                match word.compare_exchange_weak(
                    bits,
                    bits | bit,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        region.held.fetch_add(1, Ordering::AcqRel);
                        wipe_slot(region, slot);
                        return Ok(Self { slot });
                    }
                    Err(now) => bits = now,
                }
            }
        }
        Err(SealError::Capacity)
    }

    /// A secret from bytes the caller holds; the caller wipes its own copy.
    pub fn from_bytes(bytes: &[u8; 32]) -> Result<Self, SealError> {
        let mut secret = Self::zeroed()?;
        *secret.bytes_mut() = *bytes;
        Ok(secret)
    }

    /// The bytes, for a call into the library.
    pub fn bytes(&self) -> &[u8; 32] {
        let ptr = self.ptr();
        // SAFETY: `ptr` is this secret's slot: in the locked region, which lives for the process,
        // aligned for `u8`, 32 bytes, and owned by `self` alone (its bit was claimed for it), so a
        // shared borrow of `self` is the only access while the reference lives.
        unsafe { &*ptr.cast::<[u8; 32]>() }
    }

    pub(crate) fn bytes_mut(&mut self) -> &mut [u8; 32] {
        let ptr = self.ptr();
        // SAFETY: as `bytes`, and `&mut self` makes this the only access while it lives.
        unsafe { &mut *ptr.cast::<[u8; 32]>() }
    }

    fn ptr(&self) -> *mut u8 {
        let base = region().map_or(0, |r| r.base);
        base.wrapping_add(self.slot.wrapping_mul(SLOT)) as *mut u8
    }
}

impl Drop for Secret32 {
    fn drop(&mut self) {
        if let Some(region) = region() {
            wipe_slot(region, self.slot);
            let word = self.slot / 64;
            let bit = 1u64 << (self.slot % 64);
            if let Some(word) = region.claimed.get(word) {
                word.fetch_and(!bit, Ordering::AcqRel);
                region.held.fetch_sub(1, Ordering::AcqRel);
            }
        }
    }
}

impl std::fmt::Debug for Secret32 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Secret32(..)")
    }
}

/// Wipes slot `slot` of `region`: by its claimer, before the secret is handed out, or by its owner
/// as it is dropped, when nothing else refers to the slot.
fn wipe_slot(region: &Region, slot: usize) {
    if slot >= region.slots {
        return;
    }
    let ptr = region.base.wrapping_add(slot.wrapping_mul(SLOT)) as *mut u8;
    // SAFETY: `slot < slots`, so `ptr..ptr+32` lies in the locked region, which lives for the
    // process; the caller holds the slot's claim and no reference to it, so this slice is the only
    // access while it lives.
    let bytes = unsafe { std::slice::from_raw_parts_mut(ptr, SLOT) };
    wipe(bytes);
}

/// Zeroes `bytes` with volatile writes, then fences, so the compiler keeps every write.
pub(crate) fn wipe(bytes: &mut [u8]) {
    for byte in bytes.iter_mut() {
        let at: *mut u8 = byte;
        // SAFETY: `at` comes from a `&mut u8` borrowed for this statement, so it is valid, aligned
        // and exclusively ours for the write.
        unsafe { std::ptr::write_volatile(at, 0) };
    }
    compiler_fence(Ordering::SeqCst);
}

/// The region every test in this crate shares: one per process, large enough for the tests run at
/// once.
#[cfg(test)]
pub(crate) fn test_region() {
    let _ = lock_keys(4096);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_wipe_zeroes_every_byte() {
        let mut bytes = [0xA5u8; 47];
        wipe(&mut bytes);
        assert_eq!(bytes, [0u8; 47]);
    }

    #[test]
    fn a_secret_never_prints_its_bytes() {
        test_region();
        let secret = Secret32::from_bytes(&[7; 32]).unwrap();
        assert_eq!(format!("{secret:?}"), "Secret32(..)");
    }

    #[test]
    fn a_later_call_is_answered_by_the_region_made() {
        test_region();
        let (slots, _) = keys_held().unwrap();
        assert_eq!(lock_keys(8), Ok(()));
        assert_eq!(lock_keys(slots), Ok(()));
        assert_eq!(
            lock_keys(slots + 1),
            Err(SealError::AlreadyLocked { slots })
        );
    }
}
