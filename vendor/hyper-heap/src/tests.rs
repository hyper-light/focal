//! A heap of its own holds what the work run on it holds, and nothing else.

use super::*;

// The tests allocate through mimalloc, as a binary using this crate does.
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Blocks of a size that is one of mimalloc's bins, so a block's usable size is its size.
const BLOCK: usize = 1024;
const BLOCKS: usize = 200;

/// Blocks held in an array on the stack: the blocks alone are on a heap.
fn blocks() -> [Vec<u8>; BLOCKS] {
    std::array::from_fn(|_| Vec::with_capacity(BLOCK))
}

/// The bytes of the blocks.
fn their_bytes() -> u64 {
    (BLOCKS * BLOCK) as u64
}

#[test]
fn a_heap_holds_what_its_work_allocated_and_no_more() {
    let heap = Heap::new().expect("the tests allocate through mimalloc");
    let before = heap.held();
    let held = heap.run(blocks);
    let after = heap.held();
    assert_eq!(after.in_use - before.in_use, their_bytes(), "{after:?}");
    assert!(after.pages >= after.in_use, "{after:?}");
    // What the thread allocates off the heap is not its.
    let elsewhere = blocks();
    assert_eq!(heap.held(), after);
    drop(elsewhere);
    // What it held, let go, is not held.
    drop(held);
    assert_eq!(heap.held().in_use, before.in_use);
}

#[test]
fn a_block_freed_on_another_thread_goes_back_to_its_heap() {
    let heap = Heap::new().unwrap();
    let before = heap.held().in_use;
    let held = heap.run(blocks);
    assert_eq!(heap.held().in_use - before, their_bytes());
    std::thread::scope(|scope| {
        scope.spawn(move || drop(held));
    });
    assert_eq!(heap.held().in_use, before);
}

#[test]
fn work_outside_its_heap_is_not_its_heaps() {
    let heap = Heap::new().unwrap();
    let before = heap.held();
    let (outside_it, on_it) = heap.run(|| {
        let outside_it = outside(blocks);
        // The heap is the default again once `outside` returns.
        let on_it = vec![0u8; BLOCK];
        (outside_it, on_it)
    });
    assert_eq!(heap.held().in_use - before.in_use, BLOCK as u64);
    drop((outside_it, on_it));
    assert_eq!(heap.held().in_use, before.in_use);
}

#[test]
fn the_default_is_back_after_work_returns_or_unwinds() {
    let heap = Heap::new().unwrap();
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        heap.run(|| -> () { panic!("the work unwinds") })
    }));
    assert!(unwound.is_err());
    // What the unwinding left on the heap (the panic's own first-use state) is the heap's; what
    // the thread allocates after it is not.
    let after_unwind = heap.held();
    let off_it = blocks();
    assert_eq!(heap.held(), after_unwind);
    let nested = heap.run(|| heap.run(|| vec![0u8; BLOCK]));
    let off_it_again = blocks();
    assert_eq!(heap.held().in_use - after_unwind.in_use, BLOCK as u64);
    drop((off_it, nested, off_it_again));
    assert_eq!(
        held_here(),
        None,
        "the thread's backing heap is its default"
    );
}

#[test]
fn a_future_on_a_heap_reads_its_own_holdings_and_what_it_returns_outlives_its_heap() {
    let mut context = Context::from_waker(std::task::Waker::noop());
    let mut polls = 0;
    // Room for every read and every block a poll keeps, made before the future's heap is: neither
    // list allocates on it.
    let mut read = Vec::with_capacity(3);
    let mut kept = Vec::with_capacity(3);
    let mut future = Box::pin(OnHeap::new(std::future::poll_fn(|_| {
        polls += 1;
        // Each poll holds one more block on the future's heap, and reads what the heap holds.
        let held = vec![0u8; BLOCK];
        read.push(held_here().expect("polled on a heap of its own").in_use);
        kept.push(held);
        if polls < 3 {
            Poll::Pending
        } else {
            Poll::Ready(vec![7u8; BLOCK])
        }
    })));
    assert!(future.as_mut().poll(&mut context).is_pending());
    assert!(future.as_mut().poll(&mut context).is_pending());
    let Poll::Ready(returned) = future.as_mut().poll(&mut context) else {
        panic!("the third poll is its last");
    };
    drop(future);
    // Each read saw the blocks the polls before it held, its own among them.
    assert_eq!(read[1] - read[0], BLOCK as u64, "{read:?}");
    assert_eq!(read[2] - read[1], BLOCK as u64, "{read:?}");
    // The heap is gone; what it returned is the backing heap's, and whole.
    assert!(returned.iter().all(|&byte| byte == 7));
    assert_eq!(held_here(), None);
}

#[test]
fn a_heaps_blocks_are_told_by_their_size() {
    let heap = Heap::new().unwrap();
    let (before, held, after) = heap.run(|| {
        let before = sizes_here().expect("on a heap of its own");
        let held = blocks();
        (before, held, sizes_here().unwrap())
    });
    let of = |sizes: &[(u64, u64)]| {
        sizes
            .iter()
            .find(|(size, _)| *size == BLOCK as u64)
            .map_or(0, |(_, blocks)| *blocks)
    };
    assert_eq!(of(&after) - of(&before), BLOCKS as u64, "{after:?}");
    assert!(
        after.windows(2).all(|pair| pair[0].0 < pair[1].0),
        "{after:?}"
    );
    drop(held);
    assert_eq!(
        sizes_here(),
        None,
        "the thread's backing heap is its default"
    );
}
