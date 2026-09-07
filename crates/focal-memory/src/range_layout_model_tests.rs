use super::*;
use std::{
    cell::Cell,
    collections::{BTreeMap, BTreeSet},
};

/// Deliberately not Clone: randomized mutations must use the fallible copier.
#[derive(Debug)]
struct Payload {
    stamp: u64,
    bytes: Vec<u8>,
}
type Oracle = BTreeMap<u64, (u64, Vec<u8>)>;

fn random(seed: &mut u64) -> u64 {
    *seed ^= *seed << 13;
    *seed ^= *seed >> 7;
    *seed ^= *seed << 17;
    *seed
}
fn heap(bytes: &Vec<u8>) -> usize {
    bytes.capacity()
        + if bytes.capacity() == 0 {
            0
        } else {
            ALLOCATOR_OVERHEAD
        }
}
fn entry(key: u64, stamp: u64, length: usize) -> Entry<u64, Payload> {
    let bytes = (0..length)
        .map(|offset| (stamp as u8).wrapping_add((offset % 251) as u8))
        .collect::<Vec<_>>();
    let charge = heap(&bytes);
    Entry::new(key, Payload { stamp, bytes }, charge)
}
fn config() -> RangeConfig {
    RangeConfig {
        page_entries: 5,
        page_bytes: 768,
        max_entry_bytes: size_of::<Entry<u64, Payload>>() + 3072 + ALLOCATOR_OVERHEAD,
        max_batch_entries: 16,
        ..RangeConfig::default()
    }
}
fn copy(value: &Payload) -> Result<Payload, MemoryError> {
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(value.bytes.len())
        .map_err(|_| MemoryError::AllocationFailed)?;
    bytes.extend_from_slice(&value.bytes);
    Ok(Payload {
        stamp: value.stamp,
        bytes,
    })
}
fn assert_root(root: &Root<u64, Payload>, oracle: &Oracle, config: RangeConfig) {
    assert_eq!(root.len, oracle.len());
    let mut expected = oracle.iter();
    let mut previous = None;
    let mut visited = 0;
    for page in root.pages.iter() {
        assert!(!page.entries.is_empty());
        assert!(page.entries.len() <= config.page_entries);
        let charge = page_charge::<u64, Payload>(
            page.entries.len(),
            page.entries.iter().map(|row| row.heap_bytes).sum(),
        )
        .unwrap();
        assert_eq!(page._allocation.bytes(), charge);
        if charge > config.page_bytes {
            assert_eq!(
                page.entries.len(),
                1,
                "oversized leaf contains neighboring rows"
            );
        }
        for row in &page.entries {
            assert!(previous.is_none_or(|key| key < row.key));
            previous = Some(row.key);
            assert_eq!(row.heap_bytes, heap(&row.value.bytes));
            assert!(row.read_bytes().unwrap() <= config.max_entry_bytes);
            let (key, (stamp, bytes)) = expected.next().expect("extra ordered row");
            assert_eq!(row.key, *key);
            assert_eq!(row.value.stamp, *stamp);
            assert_eq!(&row.value.bytes, bytes);
            visited += 1;
        }
    }
    assert!(expected.next().is_none(), "missing ordered row");
    assert_eq!(visited, root.len);
    for key in 0..96 {
        match (root.get(&key), oracle.get(&key)) {
            (None, None) => {}
            (Some(row), Some((stamp, bytes))) => {
                assert_eq!(row.value.stamp, *stamp);
                assert_eq!(&row.value.bytes, bytes);
            }
            _ => panic!("point read differs at key {key}"),
        }
    }
}
fn assert_snapshot(lease: &SnapshotLease<u64, Payload>, prefix: u64, oracle: &Oracle, now: u64) {
    assert_eq!(lease.prefix(), prefix);
    let mut after = 0;
    let mut exclusive = false;
    for (key, (stamp, bytes)) in oracle {
        let next = lease
            .project_next(&after, exclusive, &u64::MAX, now, |row| {
                assert_eq!(row.key, *key);
                assert_eq!(row.value.stamp, *stamp);
                assert_eq!(&row.value.bytes, bytes);
                row.key
            })
            .unwrap();
        assert_eq!(next, Some(*key));
        after = *key;
        exclusive = true;
    }
    assert!(
        lease
            .project_next(&after, exclusive, &u64::MAX, now, |row| row.key)
            .unwrap()
            .is_none()
    );
}

#[test]
fn deterministic_byte_layout_churn_matches_map_and_retained_snapshot_oracles() {
    for initial_seed in [3, 0x1234_5678, 0xcafe_9876] {
        let budget = MemoryBudget::new(16 * 1024 * 1024, 1024 * 1024).unwrap();
        let config = config();
        let mut range =
            RangeStore::new(RangeId(u128::from(initial_seed)), 0, config, budget.clone()).unwrap();
        let mut oracle = Oracle::new();
        let mut seed = initial_seed;
        let mut snapshots = Vec::new();
        let mut inserts = 0;
        let mut replacements = 0;
        let mut deletes = 0;
        let mut small_to_large = 0;
        let mut large_to_small = 0;
        let mut shared_large = 0;
        let mut discarded = 0;
        let sizes = [0, 1, 7, 23, 57, 111, 257, 1024, 3072];
        for round in 1..=360u64 {
            let mut expected = oracle.clone();
            let mut changed = BTreeSet::new();
            let mut changes = Vec::new();
            for _ in 0..random(&mut seed) % 9 {
                let key = random(&mut seed) % 96;
                if !changed.insert(key) {
                    continue;
                }
                if expected.contains_key(&key) && random(&mut seed).is_multiple_of(4) {
                    expected.remove(&key);
                    changes.push(Change::Delete(key));
                    deletes += 1;
                } else {
                    let size = sizes[(random(&mut seed) % sizes.len() as u64) as usize];
                    let stamp = random(&mut seed);
                    let incoming = entry(key, stamp, size);
                    if let Some((_, bytes)) = expected.get(&key) {
                        replacements += 1;
                        if bytes.len() < 1024 && size >= 1024 {
                            small_to_large += 1;
                        }
                        if bytes.len() >= 1024 && size < 1024 {
                            large_to_small += 1;
                        }
                    } else {
                        inserts += 1;
                    }
                    expected.insert(key, (stamp, incoming.value.bytes.clone()));
                    changes.push(Change::Put(incoming));
                }
            }
            let baseline = budget.stats();
            let prefix = range.prefix() + 1;
            let plan = range
                .plan_batch(prefix, changes, BudgetLane::Ordinary, usize::MAX)
                .unwrap();
            let charges = plan.charges();
            assert_eq!(budget.stats(), baseline);
            let candidate = plan
                .build_with(|old| {
                    // Any unchanged oversized singleton must have been shared. A
                    // replacement moves its new payload instead of copying the old.
                    assert!(
                        page_charge::<u64, Payload>(1, heap(&old.bytes)).unwrap()
                            <= config.page_bytes
                    );
                    assert!(budget.stats().used - baseline.used <= charges.additional_peak_bytes());
                    copy(old)
                })
                .unwrap();
            assert_root(&candidate.root, &expected, config);
            assert_root(&range.root, &oracle, config);
            preflight_tests::assert_retained(&baseline, &budget.stats(), charges);
            for old in range.root.pages.iter() {
                if old._allocation.bytes() > config.page_bytes
                    && !changed.contains(&old.entries[0].key)
                {
                    assert!(
                        candidate
                            .root
                            .pages
                            .iter()
                            .any(|page| Arc::ptr_eq(page, old))
                    );
                    shared_large += 1;
                }
            }
            if round.is_multiple_of(17) {
                drop(candidate);
                assert_eq!(budget.stats(), baseline);
                discarded += 1;
            } else {
                range.publish(candidate).unwrap();
                oracle = expected;
                assert_eq!(range.prefix(), prefix);
            }
            assert_root(&range.root, &oracle, config);
            if round.is_multiple_of(29) {
                if snapshots.len() == 3 {
                    let (lease, _, _) = snapshots.remove(0);
                    range.release(&lease).unwrap();
                    drop(lease);
                }
                snapshots.push((
                    range.pin(round, 1000).unwrap(),
                    range.prefix(),
                    oracle.clone(),
                ));
            }
            for (lease, prefix, expected) in &snapshots {
                assert_snapshot(lease, *prefix, expected, round);
            }
        }
        assert!(inserts > 100 && replacements > 100 && deletes > 50);
        assert!(small_to_large > 20 && large_to_small > 20);
        assert!(shared_large > 100 && discarded > 10);
        for (lease, _, _) in snapshots {
            range.release(&lease).unwrap();
            drop(lease);
        }
        drop(range);
        let final_stats = budget.stats();
        assert_eq!(final_stats.used, 0);
        assert_eq!(final_stats.ordinary_used, 0);
        assert!(final_stats.by_kind.iter().all(|bytes| *bytes == 0));
    }
}

#[derive(Debug)]
struct ImportPayload<'a> {
    bytes: Vec<u8>,
    copies: &'a Cell<usize>,
}
impl Clone for ImportPayload<'_> {
    fn clone(&self) -> Self {
        self.copies.set(self.copies.get() + 1);
        Self {
            bytes: self.bytes.clone(),
            copies: self.copies,
        }
    }
}

#[test]
fn byte_bounded_import_fits_under_count_staging_peak_and_never_clones_large_neighbors() {
    const PAYLOAD: usize = 96 * 1024;
    const ROWS: u64 = 4;
    const LIMIT: usize = 1024 * 1024;
    let budget = MemoryBudget::new(LIMIT, 0).unwrap();
    let copies = Cell::new(0);
    let config = RangeConfig {
        page_entries: 16,
        page_bytes: 32 * 1024,
        max_entry_bytes: size_of::<Entry<u64, ImportPayload<'_>>>() + PAYLOAD + ALLOCATOR_OVERHEAD,
        max_batch_entries: 16,
        ..RangeConfig::default()
    };
    // The old count-only chunk holds all payloads under import staging, input
    // Pending and destination pages together: payloads alone exceed this budget.
    assert!(ROWS as usize * PAYLOAD * 3 > LIMIT);
    let unbounded = RangeConfig {
        page_bytes: usize::MAX,
        ..config
    };
    let old = RangeStore::from_entries(
        RangeId(810),
        700,
        unbounded,
        budget.clone(),
        (0..ROWS).map(|key| {
            Entry::new(
                key,
                ImportPayload {
                    bytes: vec![key as u8; PAYLOAD],
                    copies: &copies,
                },
                PAYLOAD + ALLOCATOR_OVERHEAD,
            )
        }),
    );
    assert!(matches!(old, Err(MemoryError::Capacity { .. })));
    assert_eq!(budget.stats().used, 0);
    assert_eq!(copies.get(), 0);

    let pointers: [Cell<*const u8>; ROWS as usize] =
        std::array::from_fn(|_| Cell::new(std::ptr::null()));
    let range = RangeStore::from_entries(
        RangeId(811),
        700,
        config,
        budget.clone(),
        (0..ROWS).map(|key| {
            let bytes = vec![key as u8; PAYLOAD];
            pointers[key as usize].set(bytes.as_ptr());
            Entry::new(
                key,
                ImportPayload {
                    bytes,
                    copies: &copies,
                },
                PAYLOAD + ALLOCATOR_OVERHEAD,
            )
        }),
    )
    .unwrap();
    assert_eq!(range.prefix(), 700);
    assert_eq!(range.len(), ROWS as usize);
    assert_eq!(range.root.pages.len(), ROWS as usize);
    assert_eq!(
        copies.get(),
        0,
        "append after oversized import copied a retained payload"
    );
    for (index, page) in range.root.pages.iter().enumerate() {
        assert_eq!(page.entries.len(), 1);
        assert!(page._allocation.bytes() > config.page_bytes);
        let row = &page.entries[0];
        assert_eq!(row.key, index as u64);
        assert!(row.read_bytes().unwrap() <= config.max_entry_bytes);
        assert_eq!(row.heap_bytes, heap(&row.value.bytes));
        assert_eq!(row.value.bytes.as_ptr(), pointers[index].get());
        assert_eq!(row.value.bytes.len(), PAYLOAD);
        assert!(row.value.bytes.iter().all(|byte| *byte == index as u8));
    }
    assert!(budget.stats().used < LIMIT);
    drop(range);
    let final_stats = budget.stats();
    assert_eq!(final_stats.used, 0);
    assert!(final_stats.by_kind.iter().all(|bytes| *bytes == 0));
}
