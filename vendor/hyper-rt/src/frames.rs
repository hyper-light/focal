//! A shard's task frames: the memory a task spawned on its own shard lives in, recycled by that shard
//! (docs/runtime.md §3.1, "one allocation per spawn": the measured follow-up it names). A spawn wrote its
//! future into a `Box` and the task's end gave the box back: a `malloc` and a `free` a task, 10.6 ns of a
//! spawn and join that cost 43.4 ns in all on the comparison's host (docs/benchmarks.md, rt-2x). Here the
//! future is written into a frame of its size class, polled pinned where it lies, and dropped in place at
//! the task's end, and the frame goes onto its class's free list for the next spawn of that size.
//!
//! **Classes.** A frame's size is a power of two from [`SMALLEST`] bytes, aligned to [`FRAME_ALIGN`], what
//! the platform allocators give every block on the 64-bit targets. A future aligned past that (no type of
//! Rust's own is) has a frame of its exact layout, given back at its end; a zero-sized future has none.
//!
//! **Bound.** A class keeps at most `limit` frames free, the shard's task slots: no more of its frames can
//! be live at once, so the store holds no more than the shard once held live, and a frame freed past that
//! goes back to the allocator.
//!
//! **Ownership.** A [`Framed`] owns its future and its frame, as a `Box` does: it is not `Send` (the shard
//! that made it polls and drops it), and dropped without [`Frames::release`] it drops its future and gives
//! its frame back to the allocator. A frame never moves while its future lives, which is the pin.
#![allow(unsafe_code)]

use std::alloc::Layout;
use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::ptr::NonNull;
use std::task::{Context, Poll};

use crate::error::RtError;
use crate::mem::MemError;

/// Format: the alignment of every recycled frame: two pointers, what `malloc` gives every block on the
/// 64-bit targets (glibc's `MALLOC_ALIGNMENT`, macOS's 16-byte quantum, Windows' `MEMORY_ALLOCATION_ALIGNMENT`
/// on x64 and arm64), and C11's `max_align_t` there.
const FRAME_ALIGN: usize = 16;

/// Derived: the smallest frame, bytes: the alignment, which also holds a free frame's link to the next.
const SMALLEST: usize = FRAME_ALIGN;

/// Derived: `log2(SMALLEST)`.
const SMALLEST_SHIFT: u32 = SMALLEST.trailing_zeros();

/// Derived: the classes, one a power of two from [`SMALLEST`] (2⁴) to 2⁶³, the largest a 64-bit layout allows
/// (`64 - 4`); on a 32-bit target the classes past 2³¹ are never taken.
const CLASSES: usize = 60;

/// Where a frame came from, so its release knows where it goes back.
#[derive(Clone, Copy, Debug)]
enum Home {
    /// A zero-sized future: no memory.
    Nothing,
    /// A frame of class `n`, recycled.
    Class(u8),
    /// A frame of its future's exact layout (a future aligned past [`FRAME_ALIGN`]), given back at once.
    Exact(Layout),
}

/// A task's future in a frame of its shard's: owned, polled pinned in place. The pointer alone, as a `Box`
/// is: where its frame goes back is the future's layout, which its vtable gives ([`home_of`]), so a task's
/// slot moves no more for a frame than for a box.
pub(crate) struct Framed {
    future: NonNull<dyn Future<Output = ()>>,
}

impl std::fmt::Debug for Framed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Framed").finish_non_exhaustive()
    }
}

/// Where the frame of the live future `future` goes back: its layout, read from its vtable, names the class
/// [`Frames::put`] gave it.
fn home_of(future: NonNull<dyn Future<Output = ()>>) -> Home {
    // SAFETY: called while the future is alive (before its drop), so a shared reference to it for the read of
    // its size and alignment is valid; nothing holds a `&mut` to it then.
    let layout = Layout::for_value(unsafe { future.as_ref() });
    if layout.size() == 0 {
        Home::Nothing
    } else if layout.align() > FRAME_ALIGN {
        Home::Exact(layout)
    } else {
        class_of(layout.size()).map_or(Home::Exact(layout), Home::Class)
    }
}

impl Framed {
    /// Polls the future where it lies.
    pub(crate) fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        // SAFETY: `future` points at a future this `Framed` owns alone (written by `Frames::put`, dropped only
        // by this value's release or drop), and its frame never moves or is freed while this value lives: so a
        // `&mut` to it for the call aliases nothing, and pinning it is the frame's own promise.
        unsafe { Pin::new_unchecked(&mut *self.future.as_ptr()) }.poll(cx)
    }

    /// Takes the frame apart without running this value's drop: the future's pointer and its home.
    fn into_parts(self) -> (NonNull<dyn Future<Output = ()>>, Home) {
        let this = std::mem::ManuallyDrop::new(self);
        (this.future, home_of(this.future))
    }
}

impl Drop for Framed {
    /// Dropped without its shard's release (a shard torn down, a future dropped off the loop's path): the
    /// future drops in place and the frame goes back to the allocator, an unwinding drop included.
    fn drop(&mut self) {
        let block = self.future.cast::<u8>();
        let _give = GiveBack {
            block,
            home: home_of(self.future),
            frames: None,
        };
        // SAFETY: the future was written by `Frames::put` and not dropped since (release consumes the value
        // without running this), so it is initialized and dropped once, here.
        unsafe { std::ptr::drop_in_place(self.future.as_ptr()) };
    }
}

/// Gives a frame back when it drops: to its store's free list when it has one, else to the allocator, so an
/// unwinding future's drop still returns its frame.
struct GiveBack<'a> {
    block: NonNull<u8>,
    home: Home,
    frames: Option<&'a Frames>,
}

impl Drop for GiveBack<'_> {
    fn drop(&mut self) {
        match (self.home, self.frames) {
            (Home::Nothing, _) => {}
            (Home::Class(class), Some(frames)) => frames.recycle(class, self.block),
            (Home::Class(class), None) => {
                if let Some(layout) = class_layout(class) {
                    // SAFETY: the block was allocated with exactly this class's layout (`Frames::put`), its
                    // future dropped above, and it is freed once, here.
                    unsafe { std::alloc::dealloc(self.block.as_ptr(), layout) };
                }
            }
            (Home::Exact(layout), _) => {
                // SAFETY: the block was allocated with exactly `layout` (`Frames::put`), its future dropped
                // above, and it is freed once, here.
                unsafe { std::alloc::dealloc(self.block.as_ptr(), layout) };
            }
        }
    }
}

/// The refusal of a frame of `len` bytes: the allocator returned no memory (or no layout holds that many).
fn refused(len: usize) -> RtError {
    RtError::Mem(MemError::RegionRefused { len, code: None })
}

/// A free frame's first word: the next free frame of its class.
type Link = Option<NonNull<u8>>;

/// A shard's frames: per class, the free frames, linked through their first words, and how many there are.
pub(crate) struct Frames {
    free: [Cell<Link>; CLASSES],
    held: [Cell<u32>; CLASSES],
    /// The most frames a class keeps free: the shard's task slots.
    limit: u32,
}

impl std::fmt::Debug for Frames {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Frames")
            .field("limit", &self.limit)
            .finish()
    }
}

/// The layout of class `class`'s frames: `SMALLEST << class` bytes at [`FRAME_ALIGN`].
fn class_layout(class: u8) -> Option<Layout> {
    let size = SMALLEST.checked_shl(u32::from(class))?;
    Layout::from_size_align(size, FRAME_ALIGN).ok()
}

/// The class a future of `size` bytes takes: the least whose frames hold it.
fn class_of(size: usize) -> Option<u8> {
    let bits = size
        .max(SMALLEST)
        .checked_next_power_of_two()?
        .trailing_zeros();
    u8::try_from(bits.checked_sub(SMALLEST_SHIFT)?).ok()
}

impl Frames {
    /// A shard's store, keeping at most `tasks` frames free a class.
    pub(crate) fn new(tasks: usize) -> Frames {
        Frames {
            free: [const { Cell::new(None) }; CLASSES],
            held: [const { Cell::new(0) }; CLASSES],
            limit: u32::try_from(tasks).unwrap_or(u32::MAX),
        }
    }

    /// Writes `future` into a frame: one of its class's free frames, or a new one. Refused `Mem` when the
    /// allocator has no memory for a new one.
    pub(crate) fn put<F: Future<Output = ()> + 'static>(
        &self,
        future: F,
    ) -> Result<Framed, RtError> {
        let layout = Layout::new::<F>();
        // The same choice `home_of` makes from the layout at the release.
        let block = if layout.size() == 0 {
            NonNull::<F>::dangling().cast::<u8>()
        } else if let Some(class) =
            class_of(layout.size()).filter(|_| layout.align() <= FRAME_ALIGN)
        {
            self.take(class)?
        } else {
            // SAFETY: `layout` has a non-zero size (checked above).
            NonNull::new(unsafe { std::alloc::alloc(layout) }).ok_or(refused(layout.size()))?
        };
        let typed = block.cast::<F>();
        // SAFETY: `block` is a fresh or freed frame of at least `F`'s size at an alignment `F` accepts (its class
        // is the least holding `F`, every class aligned to `FRAME_ALIGN` and `F` not past it; or `F`'s exact
        // layout; or a dangling pointer for a zero-sized `F`), owned by nothing else: writing `F` there is the
        // only initialization it gets before its `Framed` owns it.
        unsafe { typed.as_ptr().write(future) };
        let future: NonNull<dyn Future<Output = ()>> = typed;
        Ok(Framed { future })
    }

    /// Drops a frame's future in place and keeps its frame for the next spawn of its class (or gives it back
    /// to the allocator past the class's bound): a task's end on the shard that spawned it. The frame comes
    /// back whether the future's drop returns or unwinds.
    pub(crate) fn release(&self, framed: Framed) {
        let (future, home) = framed.into_parts();
        let _give = GiveBack {
            block: future.cast::<u8>(),
            home,
            frames: Some(self),
        };
        // SAFETY: `into_parts` took the future out of its `Framed` without dropping it, so it is initialized and
        // dropped once, here.
        unsafe { std::ptr::drop_in_place(future.as_ptr()) };
    }

    /// A frame of `class`: the most recently freed, or a new one.
    fn take(&self, class: u8) -> Result<NonNull<u8>, RtError> {
        let index = usize::from(class);
        if let (Some(head), Some(held)) = (self.free.get(index), self.held.get(index))
            && let Some(block) = head.get()
        {
            // SAFETY: a frame on the free list holds the link to the next in its first word, written by
            // `recycle` when it went on (every frame is at least `SMALLEST` bytes at `FRAME_ALIGN`, room and
            // alignment for a `Link`), and nothing else touches a free frame.
            let next = unsafe { block.cast::<Link>().as_ptr().read() };
            head.set(next);
            held.set(held.get().saturating_sub(1));
            return Ok(block);
        }
        let layout = class_layout(class).ok_or(refused(usize::MAX))?;
        // SAFETY: a class's layout has a non-zero size (`SMALLEST` and up).
        NonNull::new(unsafe { std::alloc::alloc(layout) }).ok_or(refused(layout.size()))
    }

    /// Puts a frame of `class` whose future was dropped on its free list, or past the bound gives it back.
    fn recycle(&self, class: u8, block: NonNull<u8>) {
        let index = usize::from(class);
        if let (Some(head), Some(held)) = (self.free.get(index), self.held.get(index))
            && held.get() < self.limit
        {
            // SAFETY: the frame is this store's, its future dropped, so nothing reads it; it is at least
            // `SMALLEST` bytes at `FRAME_ALIGN`, room and alignment for a `Link`.
            unsafe { block.cast::<Link>().as_ptr().write(head.get()) };
            head.set(Some(block));
            held.set(held.get().saturating_add(1));
            return;
        }
        if let Some(layout) = class_layout(class) {
            // SAFETY: the block was allocated with exactly this class's layout (`take`), its future dropped,
            // and it is freed once, here.
            unsafe { std::alloc::dealloc(block.as_ptr(), layout) };
        }
    }
}

impl Drop for Frames {
    /// The free frames go back to the allocator: every one is on a list, at most `limit` a class.
    fn drop(&mut self) {
        for (index, head) in self.free.iter().enumerate() {
            let Some(layout) = u8::try_from(index).ok().and_then(class_layout) else {
                continue;
            };
            // Bounded by the class's count: a list holds no more than it counted on.
            let held = self.held.get(index).map_or(0, Cell::get);
            for _ in 0..held {
                let Some(block) = head.get() else {
                    break;
                };
                // SAFETY: as in `take`: the first word of a free frame is its link.
                let next = unsafe { block.cast::<Link>().as_ptr().read() };
                head.set(next);
                // SAFETY: a free frame was allocated with exactly its class's layout and is freed once, here.
                unsafe { std::alloc::dealloc(block.as_ptr(), layout) };
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Waker;

    fn poll_once(framed: &mut Framed) -> Poll<()> {
        framed.poll(&mut Context::from_waker(Waker::noop()))
    }

    /// Counts its drops.
    struct Dropping<'a>(&'a AtomicUsize, [u8; 40]);

    impl Future for Dropping<'_> {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            std::hint::black_box(&self.1);
            Poll::Ready(())
        }
    }

    impl Drop for Dropping<'_> {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn a_released_frame_is_the_next_spawns_of_its_class() {
        static DROPS: AtomicUsize = AtomicUsize::new(0);
        let frames = Frames::new(4);
        let mut first = frames.put(Dropping(&DROPS, [7; 40])).unwrap();
        assert_eq!(poll_once(&mut first), Poll::Ready(()));
        let address = first.future.cast::<u8>();
        frames.release(first);
        assert_eq!(
            DROPS.load(Ordering::Relaxed),
            1,
            "the release dropped the future"
        );
        let second = frames.put(Dropping(&DROPS, [9; 40])).unwrap();
        assert_eq!(second.future.cast::<u8>(), address, "the frame came back");
        drop(second);
        assert_eq!(
            DROPS.load(Ordering::Relaxed),
            2,
            "dropped without release, the future still drops"
        );
    }

    #[test]
    fn a_class_keeps_no_more_frames_than_its_bound() {
        let frames = Frames::new(2);
        let held: Vec<_> = (0..5).map(|_| frames.put(async {}).unwrap()).collect();
        for framed in held {
            frames.release(framed);
        }
        let class = class_of(Layout::for_value(&async {}).size()).unwrap();
        assert_eq!(frames.held[usize::from(class)].get(), 2);
    }

    #[test]
    fn sizes_take_the_least_class_that_holds_them() {
        assert_eq!(class_of(1), Some(0));
        assert_eq!(class_of(16), Some(0));
        assert_eq!(class_of(17), Some(1));
        assert_eq!(class_of(4096), Some(8));
        assert_eq!(class_layout(8).unwrap().size(), 4096);
    }

    /// Over-aligned: a frame of its own layout, polled and given back.
    #[repr(align(64))]
    struct Wide(u8);

    impl Future for Wide {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            assert_eq!(std::ptr::from_ref(&*self).addr() % 64, 0);
            std::hint::black_box(self.0);
            Poll::Ready(())
        }
    }

    #[test]
    fn an_over_aligned_future_has_a_frame_of_its_own_layout() {
        let frames = Frames::new(4);
        let mut wide = frames.put(Wide(1)).unwrap();
        assert!(matches!(home_of(wide.future), Home::Exact(_)));
        assert_eq!(poll_once(&mut wide), Poll::Ready(()));
        frames.release(wide);
    }

    /// Zero-sized: no memory, polled, dropped.
    struct Empty;

    impl Future for Empty {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            Poll::Pending
        }
    }

    #[test]
    fn a_zero_sized_future_takes_no_frame() {
        let frames = Frames::new(4);
        let mut empty = frames.put(Empty).unwrap();
        assert!(matches!(home_of(empty.future), Home::Nothing));
        assert_eq!(poll_once(&mut empty), Poll::Pending);
        frames.release(empty);
        drop(frames.put(Empty).unwrap());
    }

    /// Unwinds in its drop.
    struct Unwinding([u8; 24]);

    impl Future for Unwinding {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            std::hint::black_box(&self.0);
            Poll::Ready(())
        }
    }

    impl Drop for Unwinding {
        fn drop(&mut self) {
            panic!("a future's drop unwinds");
        }
    }

    #[test]
    fn a_frame_comes_back_when_its_futures_drop_unwinds() {
        let frames = Frames::new(4);
        let framed = frames.put(Unwinding([3; 24])).unwrap();
        let address = framed.future.cast::<u8>();
        let unwound =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| frames.release(framed)));
        assert!(unwound.is_err());
        // A future of the same class takes the frame the unwinding one gave back.
        let held = [5u8; 24];
        let next = frames
            .put(async move {
                std::hint::black_box(held);
            })
            .unwrap();
        assert_eq!(
            next.future.cast::<u8>(),
            address,
            "the frame went back to its class"
        );
        frames.release(next);
    }

    #[test]
    fn a_store_dropped_gives_its_free_frames_back() {
        let frames = Frames::new(8);
        let held: Vec<_> = (0..8)
            .map(|i| {
                frames
                    .put(async move {
                        std::hint::black_box(i);
                    })
                    .unwrap()
            })
            .collect();
        for framed in held {
            frames.release(framed);
        }
        drop(frames);
    }
}
