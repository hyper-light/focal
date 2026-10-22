//! A thread's work on a heap of its own: what that work holds, read where it is held.
//!
//! mimalloc keeps first-class heaps (Leijen, Zorn and de Moura, "Mimalloc: Free List Sharding in
//! Action", APLAS 2019): a heap made on a thread (`mi_heap_new`) takes that thread's allocations
//! while it is the thread's default (`mi_heap_set_default`), and a block freed anywhere goes back to
//! the page it came from, which is its heap's, whichever thread frees it. Work run on a heap of its
//! own leaves on that heap's pages exactly what it holds — the allocator's rounding of each block
//! included, nothing another piece of work holds — and the pages say so at any time
//! (`mi_heap_visit_blocks`), with nothing counted per allocation: switching the default is all a
//! piece of work pays to run there.
//!
//! What it is for: an owner that must charge what its work holds, where no constant measured on
//! one machine says it for another (hyper-transport's connection state, docs/transport.md §4x).
//! Measured on Linux and macOS under churn of other allocations on the same thread, a heap's bytes
//! in use per connection held reproduced the clean run's within 6% at 128 connections and 1% from
//! 512 on, where the process's footprint erred by up to +392% and the bytes the program counted it
//! asked for fell 5–27% short (docs/benchmarks.md, "What a connection holds, read from a heap of
//! its own").
//!
//! It needs the process to allocate through mimalloc: [`Heap::new`] gives none otherwise, and a
//! future polled [`OnHeap`] runs as it would without one.
//!
//! The `unsafe` here binds mimalloc's heap API (`scripts/check-contracts.py`).

#![allow(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::disallowed_macros
    )
)]

use std::ffi::c_void;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::ptr::NonNull;
use std::task::{Context, Poll};

use libmimalloc_sys as mi;

/// What a heap holds.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Held {
    /// The bytes of its blocks in use, each block's usable size: what its work holds, the
    /// allocator's rounding of each block included.
    pub in_use: u64,
    /// The bytes of the blocks its pages have made, in use or free: what its pages hold.
    pub pages: u64,
}

/// A heap of its own for one thread's work. Neither `Send` nor `Sync`: a heap allocates on the
/// thread that made it, and is let go there.
pub struct Heap {
    heap: NonNull<mi::mi_heap_t>,
    _thread: PhantomData<*mut ()>,
}

impl Heap {
    /// A heap of its own on this thread; none where this process does not allocate through
    /// mimalloc (a block its global allocator gives is outside mimalloc's region), or where
    /// mimalloc makes none.
    pub fn new() -> Option<Self> {
        if !allocates_through_mimalloc() {
            return None;
        }
        // SAFETY: makes a heap of this thread's, which this value keeps and lets go on this thread
        // (it is neither Send nor Sync).
        let heap = unsafe { mi::mi_heap_new() };
        NonNull::new(heap).map(|heap| Self {
            heap,
            _thread: PhantomData,
        })
    }

    /// `work`, run with this heap the thread's default: what it allocates is on this heap, and the
    /// default the thread had is its default again as `work` returns or unwinds.
    pub fn run<R>(&self, work: impl FnOnce() -> R) -> R {
        let _entered = Entered::on(self.heap.as_ptr());
        work()
    }

    /// What this heap holds now, the blocks other threads freed of it taken back first.
    pub fn held(&self) -> Held {
        held_by(self.heap.as_ptr())
    }
}

impl Drop for Heap {
    fn drop(&mut self) {
        // SAFETY: the heap is this thread's (the value never left it) and is used no more: what
        // its blocks still hold moves to the thread's backing heap, and a default it was is the
        // backing heap again (`mi_heap_delete`).
        unsafe { mi::mi_heap_delete(self.heap.as_ptr()) };
    }
}

/// Whether this process allocates through mimalloc: a block its global allocator gives lies in
/// mimalloc's region.
fn allocates_through_mimalloc() -> bool {
    let probe = Box::new(0u8);
    let at: *const u8 = &*probe;
    // SAFETY: a lookup of the address in mimalloc's segment map, which reads nothing at it.
    unsafe { mi::mi_is_in_heap_region(at.cast::<c_void>()) }
}

/// What `heap` holds now. On `heap`'s thread.
fn held_by(heap: *mut mi::mi_heap_t) -> Held {
    let mut held = Held::default();
    // SAFETY: `heap` is this thread's; the visitor writes only the `Held` it is given, which
    // outlives the walk.
    unsafe {
        mi::mi_heap_collect(heap, false);
        mi::mi_heap_visit_blocks(heap, false, Some(visit), (&raw mut held).cast::<c_void>());
    }
    held
}

/// What the heap that is this thread's default holds, when it is a heap of its own (one a
/// [`Heap`] made, being run on): the account a piece of work run [`OnHeap`] reads of itself.
/// None on the thread's backing heap.
pub fn held_here() -> Option<Held> {
    // SAFETY: the thread's own default and backing heaps, which mimalloc keeps for the thread.
    let (default, backing) = unsafe { (mi::mi_heap_get_default(), mi::mi_heap_get_backing()) };
    (!default.is_null() && default != backing).then(|| held_by(default))
}

/// What the heap that is this thread's default holds, by the size of its blocks: each size the
/// heap's pages hold blocks of, with the blocks of it in use, smallest first. The account of
/// [`held_here`] taken apart, to say what a piece of work holds; none on the thread's backing heap.
/// At most one entry for each of mimalloc's size classes, which are fixed (its bins).
pub fn sizes_here() -> Option<Vec<(u64, u64)>> {
    // SAFETY: the thread's own default and backing heaps, which mimalloc keeps for the thread.
    let (default, backing) = unsafe { (mi::mi_heap_get_default(), mi::mi_heap_get_backing()) };
    if default.is_null() || default == backing {
        return None;
    }
    // Room for every size first, off the heap being walked: the walk allocates nothing. mimalloc
    // keeps a page queue for each of its bins (75 in v2, the huge pages' among them); a heap of
    // huge blocks of more sizes than the room has its rest under the last size the room took.
    let mut sizes: Vec<(u64, u64)> = Vec::new();
    outside(|| sizes.try_reserve_exact(SIZES)).ok()?;
    // SAFETY: `default` is this thread's; the visitor writes only the vector it is given, which
    // outlives the walk and which no one else touches during it.
    unsafe {
        mi::mi_heap_collect(default, false);
        mi::mi_heap_visit_blocks(
            default,
            false,
            Some(visit_sizes),
            (&raw mut sizes).cast::<c_void>(),
        );
    }
    sizes.sort_unstable();
    Some(sizes)
}

/// Adds one page of a heap to the sizes `arg` points to: its blocks in use, under their size.
unsafe extern "C" fn visit_sizes(
    _heap: *const mi::mi_heap_t,
    area: *const mi::mi_heap_area_t,
    _block: *mut c_void,
    _block_size: usize,
    arg: *mut c_void,
) -> bool {
    // SAFETY: mimalloc passes an area valid for the call, and `arg` is the vector `sizes_here`
    // passed, which nothing else touches during the walk.
    let (Some(area), Some(sizes)) = (unsafe { area.as_ref() }, unsafe {
        arg.cast::<Vec<(u64, u64)>>().as_mut()
    }) else {
        return true;
    };
    let size = u64::try_from(area.block_size).unwrap_or(u64::MAX);
    let used = u64::try_from(area.used).unwrap_or(u64::MAX);
    if used == 0 {
        return true;
    }
    let room = sizes.len() < sizes.capacity();
    match sizes.iter_mut().find(|(held, _)| *held == size) {
        Some((_, blocks)) => *blocks = blocks.saturating_add(used),
        None if room => sizes.push((size, used)),
        None => {
            if let Some((_, blocks)) = sizes.last_mut() {
                *blocks = blocks.saturating_add(used);
            }
        }
    }
    true
}

/// The sizes [`sizes_here`] has room for: twice mimalloc v2's page queues (its 73 bins, the huge
/// one and the full one), so a heap holding huge blocks of many sizes has room for them too.
const SIZES: usize = 150;

/// `work`, run on the thread's backing heap whatever heap is the default: what it allocates is
/// none of the current heap's (a buffer charged apart from the work, a task's frame that other
/// tasks reuse).
pub fn outside<R>(work: impl FnOnce() -> R) -> R {
    // SAFETY: the thread's own backing heap, which mimalloc keeps for the thread's life.
    let backing = unsafe { mi::mi_heap_get_backing() };
    let _entered = Entered::on(backing);
    work()
}

/// Adds one page of a heap to the [`Held`] `arg` points to.
unsafe extern "C" fn visit(
    _heap: *const mi::mi_heap_t,
    area: *const mi::mi_heap_area_t,
    _block: *mut c_void,
    _block_size: usize,
    arg: *mut c_void,
) -> bool {
    // SAFETY: mimalloc passes an area valid for the call, and `arg` is the `Held` `held_by` passed,
    // which nothing else touches during the walk.
    let (Some(area), Some(held)) = (unsafe { area.as_ref() }, unsafe {
        arg.cast::<Held>().as_mut()
    }) else {
        return true;
    };
    let in_use = u64::try_from(area.used.saturating_mul(area.block_size)).unwrap_or(u64::MAX);
    let pages = u64::try_from(area.committed).unwrap_or(u64::MAX);
    held.in_use = held.in_use.saturating_add(in_use);
    held.pages = held.pages.saturating_add(pages);
    true
}

/// The thread's default heap as it was, made the default again as this is dropped.
struct Entered {
    previous: *mut mi::mi_heap_t,
}

impl Entered {
    /// `heap` made the thread's default.
    fn on(heap: *mut mi::mi_heap_t) -> Self {
        // SAFETY: `heap` is a heap of this thread's: a `Heap`'s, borrowed for the guard's life, or
        // the thread's backing heap. mimalloc refuses (null, the default unchanged) one it does not
        // know.
        let previous = unsafe { mi::mi_heap_set_default(heap) };
        Self { previous }
    }
}

impl Drop for Entered {
    fn drop(&mut self) {
        if !self.previous.is_null() {
            // SAFETY: the heap that was this thread's default as the guard was made, alive still:
            // a `Heap` is borrowed for as long as work runs on it, and guards end in the reverse
            // order they were made.
            unsafe { mi::mi_heap_set_default(self.previous) };
        }
    }
}

pin_project_lite::pin_project! {
    /// A future whose every poll runs on a heap of its own, made on the thread of its first poll
    /// (a shard's task is polled on its shard alone): what it allocates as it runs is on that heap,
    /// and [`held_here`] reads it from within. Where the process does not allocate through
    /// mimalloc the future runs as it would without one.
    pub struct OnHeap<F> {
        // Declared first, so dropped before its heap: what its drop frees goes back to pages the
        // heap still holds.
        #[pin]
        future: F,
        heap: Option<Heap>,
        made: bool,
    }
}

impl<F> OnHeap<F> {
    /// `future`, to be polled on a heap of its own.
    pub fn new(future: F) -> Self {
        Self {
            future,
            heap: None,
            made: false,
        }
    }
}

impl<F: Future> Future for OnHeap<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<F::Output> {
        let this = self.project();
        if !*this.made {
            *this.made = true;
            *this.heap = Heap::new();
        }
        let future = this.future;
        match this.heap.as_ref() {
            Some(heap) => heap.run(|| future.poll(context)),
            None => future.poll(context),
        }
    }
}

#[cfg(test)]
mod tests;
