//! The map bounds charges are sized by (`map_entry_bytes`, `map_bytes`),
//! checked against what std's `BTreeMap` asks its allocator for: built in
//! order, out of order and from a sorted iterator, and after half its entries
//! were removed again.
//!
//! One test function only: the counters are process-global, so a second
//! concurrent test in the same binary would race them.
#![allow(unsafe_code)]
#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap
)]

use focal_memory::{ALLOCATOR_OVERHEAD, map_bytes};
use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Counting;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static BLOCKS: AtomicUsize = AtomicUsize::new(0);
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY (test allocator): delegates to the system allocator with the
        // same layout; the accounting is ordinary atomic adds.
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            LIVE.fetch_add(layout.size(), Ordering::Relaxed);
            BLOCKS.fetch_add(1, Ordering::Relaxed);
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        BLOCKS.fetch_sub(1, Ordering::Relaxed);
        // SAFETY: `ptr`/`layout` are the pair this allocator returned.
        unsafe { System.dealloc(ptr, layout) };
    }
}
#[global_allocator]
static ALLOC: Counting = Counting;

/// What `build`'s result holds: the bytes its live blocks asked for and each
/// block's bookkeeping, as the budget charges an allocation.
fn held<T>(build: impl FnOnce() -> T) -> (T, usize) {
    let (bytes, blocks) = (LIVE.load(Ordering::Relaxed), BLOCKS.load(Ordering::Relaxed));
    let value = build();
    let grown = LIVE.load(Ordering::Relaxed) - bytes;
    let added = BLOCKS.load(Ordering::Relaxed) - blocks;
    (value, grown + added * ALLOCATOR_OVERHEAD)
}

/// A reproducible permutation of `0..n` (xorshift; no rng dependency).
fn shuffled(n: u64) -> Vec<u64> {
    let mut keys: Vec<u64> = (0..n).collect();
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    for index in (1..keys.len()).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        keys.swap(index, (state % (index as u64 + 1)) as usize);
    }
    keys
}

fn check<K: Ord, V>(name: &str, key: impl Fn(u64) -> K, value: impl Fn(u64) -> V) {
    for n in [
        0u64, 1, 2, 4, 5, 6, 10, 11, 12, 13, 50, 66, 67, 100, 1_000, 10_000,
    ] {
        let bound = map_bytes::<K, V>(n as usize).unwrap();
        let (_, bytes) = held(|| {
            let mut map = BTreeMap::new();
            for index in 0..n {
                map.insert(key(index), value(index));
            }
            map
        });
        assert!(
            bytes <= bound,
            "{name}: {n} in order hold {bytes} > {bound}"
        );
        // The count is real: the entries' own bytes are among what it saw.
        let own = (n as usize) * (size_of::<K>() + size_of::<V>());
        assert!(bytes >= own, "{name}: {n} in order hold {bytes} < {own}");
        // The permutation is made before the measured build.
        let order = shuffled(n);
        let (_, bytes) = held(|| {
            let mut map = BTreeMap::new();
            for index in &order {
                map.insert(key(*index), value(*index));
            }
            map
        });
        assert!(
            bytes <= bound,
            "{name}: {n} shuffled hold {bytes} > {bound}"
        );
        let (_, bytes) = held(|| {
            (0..n)
                .map(|index| (key(index), value(index)))
                .collect::<BTreeMap<_, _>>()
        });
        assert!(
            bytes <= bound,
            "{name}: {n} collected hold {bytes} > {bound}"
        );
        // Removal keeps every node but the root five full: what is left after
        // half is removed is held as a map of the rest.
        let left = n - n / 2;
        let bound = map_bytes::<K, V>(left as usize).unwrap();
        let (_, bytes) = held(|| {
            let mut map = BTreeMap::new();
            for index in 0..n {
                map.insert(key(index), value(index));
            }
            for index in order.iter().take((n / 2) as usize) {
                map.remove(&key(*index));
            }
            map
        });
        assert!(bytes <= bound, "{name}: {left} left hold {bytes} > {bound}");
    }
}

#[test]
fn a_map_holds_no_more_than_its_bound() {
    check("u64 set", |index| index, |_| ());
    check("u64 to u64", |index| index, |index| index);
    check(
        "fingerprint to grant-sized value",
        |index| {
            let mut key = [0u8; 32];
            key[..8].copy_from_slice(&index.to_be_bytes());
            key
        },
        |index| [index; 12],
    );
    check(
        "u64 to 200 bytes",
        |index| index,
        |index| [index as u8; 200],
    );
}
