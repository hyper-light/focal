//! A publication in place leaves what a rebuild does (doc 30 §9).

use super::*;
use std::collections::BTreeMap as Model;

#[derive(Debug, PartialEq, Eq, Clone)]
struct Value(u64);

fn copy(value: &Value) -> Result<Value, MemoryError> {
    Ok(Value(value.0))
}

/// Small leaves, so batches split, empty and copy pages often.
fn config(page_entries: usize, page_bytes: usize) -> RangeConfig {
    RangeConfig {
        page_entries,
        page_bytes,
        ..RangeConfig::default()
    }
}

/// A deterministic draw (SplitMix64).
struct Draw(u64);
impl Draw {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
}

/// One batch over `model`: puts that insert and replace, deletes of held keys,
/// each key once; `model` is advanced to the batch's result.
fn batch(
    draw: &mut Draw,
    model: &mut Model<u64, u64>,
    keys: u64,
    max: u64,
    oversized: usize,
) -> Vec<Change<u64, Value>> {
    let mut chosen = Model::new();
    for _ in 0..draw.below(max).saturating_add(1) {
        let key = draw.below(keys);
        let delete = model.contains_key(&key) && draw.below(3) == 0;
        let change = if delete {
            Change::Delete(key)
        } else {
            let value = draw.next() % 1_000_000;
            // Some rows carry heap, so the byte bound splits leaves too, and
            // a few more than a leaf holds, each a leaf of its own.
            let heap = match draw.below(40) {
                0 => oversized,
                1..=9 => (draw.below(5) * 64) as usize,
                _ => 0,
            };
            Change::Put(Entry::new(key, Value(value), heap))
        };
        chosen.insert(key, change);
    }
    for (key, change) in &chosen {
        match change {
            Change::Put(entry) => {
                model.insert(*key, entry.value.0);
            }
            Change::Delete(_) => {
                model.remove(key);
            }
        }
    }
    chosen.into_values().collect()
}

fn clone_changes(changes: &[Change<u64, Value>]) -> Vec<Change<u64, Value>> {
    changes
        .iter()
        .map(|change| match change {
            Change::Put(entry) => {
                Change::Put(Entry::new(entry.key, entry.value.clone(), entry.heap_bytes))
            }
            Change::Delete(key) => Change::Delete(*key),
        })
        .collect()
}

/// Each entry as (key, value, heap bytes).
type Rows = Vec<(u64, u64, usize)>;

/// A store's partition classifier, if it has one.
type Classify = Option<fn(&u64) -> u64>;

/// Every entry, then every leaf's length: what the two publications must agree on.
fn shape(store: &RangeStore<u64, Value>) -> (Rows, Vec<usize>) {
    let entries = store
        .entries()
        .map(|entry| (entry.key, entry.value.0, entry.heap_bytes))
        .collect();
    let leaves = store
        .root
        .pages
        .iter()
        .map(|page| page.entries.len())
        .collect();
    (entries, leaves)
}

/// Random batches published by rebuild on one store and in place on another
/// leave identical entries, leaves, lengths and prefixes, under three leaf
/// shapes, with frozen captures held across batches (their pages copied, not
/// changed: a capture reads what it froze to the end).
#[test]
fn a_publication_in_place_leaves_what_a_rebuild_leaves() {
    // Without a classifier, and with one whose partitions each leaf keeps
    // apart (the native store's affinities); leaves bound by their count
    // (four rows well under 4 KiB), by their bytes (512), and by both. A
    // store whose leaves have no byte bound has no sparse plan: its envelope
    // bounds a page's copy by the whole owner limit.
    let partitions: [Classify; 2] = [None, Some(|key: &u64| key / 16)];
    for (seed, leaves, partition) in [
        (1u64, config(4, 4096), partitions[0]),
        (2, config(8, 512), partitions[0]),
        (3, config(128, 4096), partitions[0]),
        (4, config(4, 4096), partitions[1]),
        (5, config(8, 512), partitions[1]),
        (6, config(128, 4096), partitions[1]),
    ] {
        let budget = MemoryBudget::new(64 << 20, 0).unwrap();
        let open = |id| match partition {
            Some(classify) => RangeStore::<u64, Value>::new_partitioned(
                RangeId(id),
                0,
                leaves,
                budget.clone(),
                classify,
            ),
            None => RangeStore::<u64, Value>::new(RangeId(id), 0, leaves, budget.clone()),
        };
        let mut rebuilt = open(1).unwrap();
        let mut in_place = open(2).unwrap();
        // Writes funded by a pool of the store's budget, as a session's
        // completions are: their pages are charged there, and a later write
        // under the store's own budget takes them over.
        let pool = budget.funded_child(BudgetLane::Ordinary, 16 << 20).unwrap();
        let mut draw = Draw(seed);
        let mut model = Model::new();
        let mut frozen: Option<(FrozenRange<u64, Value>, Rows)> = None;
        let oversized = leaves.page_bytes.min(8192) * 3 / 2;
        for prefix in 1..=600u64 {
            let changes = batch(&mut draw, &mut model, 400, 24, oversized);
            let copies = clone_changes(&changes);
            // Writes of either lane, so pages and nodes an earlier write made
            // are charged under another lane than the write that edits them.
            let lane = if draw.below(2) == 0 {
                BudgetLane::Ordinary
            } else {
                BudgetLane::Completion
            };
            rebuilt.apply_batch(prefix, changes, lane).unwrap();
            // Now and then the in-place store takes a rebuild's publication
            // too (a follower's replay), leaving pages and nodes it made.
            if draw.below(7) == 0 {
                in_place.apply_batch(prefix, copies, lane).unwrap();
            } else {
                let source = if draw.below(3) == 0 {
                    &pool
                } else {
                    in_place.budget()
                };
                let sparse = in_place
                    .plan_sparse(
                        source,
                        SparseBase::Committed,
                        prefix,
                        copies,
                        lane,
                        in_place.config.max_batch_entries,
                        |key| in_place.get_entry(key).map(|entry| entry.heap_bytes),
                    )
                    .unwrap_or_else(|error| {
                        panic!("seed {seed}: plan {prefix} refused: {error:?}")
                    });
                assert_eq!(sparse.len(), model.len());
                if let Err(refusal) = in_place.publish_sparse(sparse, copy) {
                    let poisoned = refusal.poisoned();
                    panic!(
                        "seed {seed}: publication {prefix} refused (poisoned {poisoned}): {:?}",
                        refusal.error()
                    );
                }
            }
            assert_eq!(
                shape(&rebuilt),
                shape(&in_place),
                "seed {seed} prefix {prefix}"
            );
            // Every page holds exactly its rows, as a rebuilt leaf does, so the
            // two stores are charged alike; and its charge is what its capacity
            // and rows' heap take, which an in-place merge reads its heap from.
            for page in rebuilt.root.pages.iter().chain(in_place.root.pages.iter()) {
                assert_eq!(
                    page.entries.capacity(),
                    page.entries.len(),
                    "seed {seed} prefix {prefix}: room past a page's rows"
                );
            }
            for page in in_place.root.pages.iter() {
                let heap: usize = page.entries.iter().map(|entry| entry.heap_bytes).sum();
                assert_eq!(
                    page._allocation.bytes(),
                    page_charge::<u64, Value>(page.entries.capacity(), heap).unwrap(),
                    "seed {seed} prefix {prefix}"
                );
            }
            assert_eq!((in_place.len(), in_place.prefix()), (model.len(), prefix));
            // A capture taken now and held across a few batches reads what it froze.
            match draw.below(8) {
                0 if frozen.is_none() => {
                    let image = in_place.freeze().unwrap();
                    let entries = image
                        .entries()
                        .map(|entry| (entry.key, entry.value.0, entry.heap_bytes))
                        .collect();
                    frozen = Some((image, entries));
                }
                1 => {
                    if let Some((image, entries)) = frozen.take() {
                        let read: Vec<_> = image
                            .entries()
                            .map(|entry| (entry.key, entry.value.0, entry.heap_bytes))
                            .collect();
                        assert_eq!(read, entries, "a capture changed under a publication");
                    }
                }
                _ => {}
            }
        }
        drop(frozen);
        drop(rebuilt);
        drop(in_place);
        assert_eq!(pool.stats().used, 0, "every pooled charge given back");
        drop(pool);
        assert_eq!(budget.stats().used, 0, "every charge given back");
    }
}

/// What `visit_retained` visits is what a publication copies where every
/// page it edits is still read elsewhere (a capture taken before each
/// publication): the same rows, under every leaf shape, partitioned or not.
/// A replay prepays the copies by it.
#[test]
fn the_rows_visited_as_retained_are_the_rows_a_publication_copies() {
    let partitions: [Classify; 2] = [None, Some(|key: &u64| key / 16)];
    for (seed, leaves, partition) in [
        (11u64, config(4, 4096), partitions[0]),
        (12, config(8, 512), partitions[0]),
        (13, config(128, 4096), partitions[0]),
        (14, config(4, 4096), partitions[1]),
        (15, config(8, 512), partitions[1]),
        (16, config(128, 4096), partitions[1]),
    ] {
        let budget = MemoryBudget::new(64 << 20, 0).unwrap();
        let mut store = match partition {
            Some(classify) => RangeStore::<u64, Value>::new_partitioned(
                RangeId(1),
                0,
                leaves,
                budget.clone(),
                classify,
            ),
            None => RangeStore::<u64, Value>::new(RangeId(1), 0, leaves, budget.clone()),
        }
        .unwrap();
        let mut draw = Draw(seed);
        let mut model = Model::new();
        let oversized = leaves.page_bytes.min(8192) * 3 / 2;
        let (mut visits, mut copies) = (0usize, 0usize);
        for prefix in 1..=300u64 {
            let changes = batch(&mut draw, &mut model, 400, 24, oversized);
            let sparse = store
                .plan_sparse(
                    store.budget(),
                    SparseBase::Committed,
                    prefix,
                    changes,
                    BudgetLane::Ordinary,
                    store.config.max_batch_entries,
                    |key| store.get_entry(key).map(|entry| entry.heap_bytes),
                )
                .unwrap();
            let mut visited = Vec::new();
            store
                .visit_retained(&sparse, |value| {
                    visited.push(value.0);
                    Ok(())
                })
                .unwrap();
            let capture = store.freeze().unwrap();
            let mut copied = Vec::new();
            if let Err(refusal) = store.publish_sparse(sparse, |value: &Value| {
                copied.push(value.0);
                Ok(Value(value.0))
            }) {
                panic!(
                    "seed {seed}: publication {prefix} refused: {:?}",
                    refusal.error()
                );
            }
            drop(capture);
            visited.sort_unstable();
            copied.sort_unstable();
            assert_eq!(visited, copied, "seed {seed} prefix {prefix}");
            visits += visited.len();
            copies += copied.len();
        }
        // The walk saw real copies, not only empty batches.
        assert!(copies > 300, "seed {seed}: {copies} copies");
        assert_eq!(visits, copies);
        drop(store);
        assert_eq!(budget.stats().used, 0, "every charge given back");
    }
}

/// A funded plan takes its changes' charge from the caller's funding, exactly
/// what a plan reserving its own takes; a funding too small is refused and
/// left as it was, with nothing reserved.
#[test]
fn a_funded_plan_takes_its_input_from_its_funding_and_no_more() {
    let budget = MemoryBudget::new(64 << 20, 0).unwrap();
    let store =
        RangeStore::<u64, Value>::new(RangeId(1), 0, config(8, 4096), budget.clone()).unwrap();
    let changes = || {
        (0..10u64)
            .map(|key| Change::Put(Entry::new(key, Value(key), 64)))
            .collect::<Vec<_>>()
    };
    let lane = BudgetLane::Ordinary;
    // The store's own root.
    let base = budget.stats().used;
    let reserving = store
        .plan_sparse(
            &budget,
            SparseBase::Committed,
            1,
            changes(),
            lane,
            store.config.max_batch_entries,
            |_| None,
        )
        .unwrap();
    let reserved = budget.stats().used;
    let input = reserving.charges().0;
    drop(reserving);
    assert_eq!(budget.stats().used, base);
    // A funding with room to spare gives up the input alone.
    let mut funding = budget
        .reserve(BudgetKind::Pending, lane, input + 1_000)
        .unwrap()
        .commit();
    let funded = store
        .plan_sparse_funded(
            &budget,
            &mut funding,
            SparseBase::Committed,
            1,
            changes(),
            lane,
            store.config.max_batch_entries,
            |_| None,
        )
        .unwrap();
    assert_eq!(funded.charges().0, input);
    assert_eq!(funding.bytes(), 1_000);
    assert_eq!(budget.stats().used, reserved + 1_000);
    drop(funded);
    drop(funding);
    // One byte short: refused, the funding as it was, nothing reserved.
    let mut short = budget
        .reserve(BudgetKind::Pending, lane, input - 1)
        .unwrap()
        .commit();
    let used = budget.stats().used;
    assert!(matches!(
        store.plan_sparse_funded(
            &budget,
            &mut short,
            SparseBase::Committed,
            1,
            changes(),
            lane,
            store.config.max_batch_entries,
            |_| None,
        ),
        Err(MemoryError::Capacity { .. })
    ));
    assert_eq!(short.bytes(), input - 1);
    assert_eq!(budget.stats().used, used);
    drop(short);
    assert_eq!(budget.stats().used, base);
}

/// A chain of candidates, each planned over its unpublished predecessors'
/// changes, publishes in order; one planned on a base already passed is
/// refused whole and leaves the store as it was.
#[test]
fn candidates_publish_in_order_and_a_stale_one_is_refused_whole() {
    let budget = MemoryBudget::new(8 << 20, 0).unwrap();
    let mut store =
        RangeStore::<u64, Value>::new(RangeId(7), 0, config(4, 4096), budget.clone()).unwrap();
    let put = |key: u64| Change::Put(Entry::new(key, Value(key * 10), 0));
    let first = store
        .plan_sparse(
            store.budget(),
            SparseBase::Committed,
            1,
            vec![put(1), put(2)],
            BudgetLane::Ordinary,
            16,
            |_| None,
        )
        .unwrap();
    // The second sees the first's keys as held: it replaces 2 and deletes 1.
    let pending = |key: &u64| first.get(key).flatten().map(|_| 0usize);
    let second = store
        .plan_sparse(
            store.budget(),
            SparseBase::After(&first),
            2,
            vec![Change::Delete(1), put(2), put(3)],
            BudgetLane::Ordinary,
            16,
            pending,
        )
        .unwrap();
    assert_eq!(second.len(), 2);
    assert!(second.follows(&first));
    let sibling = store
        .plan_sparse(
            store.budget(),
            SparseBase::Committed,
            1,
            vec![put(9)],
            BudgetLane::Ordinary,
            16,
            |_| None,
        )
        .unwrap();
    // A child of the sibling: the prefix after the first's, another state.
    let nephew = store
        .plan_sparse(
            store.budget(),
            SparseBase::After(&sibling),
            2,
            vec![put(8)],
            BudgetLane::Ordinary,
            16,
            |_| None,
        )
        .unwrap();
    assert!(!nephew.follows(&first));
    assert!(store.publish_sparse(first, copy).is_ok());
    // The nephew's prefix is the store's next, but its base is the sibling's
    // state, not the one the store holds: refused whole, nothing changed.
    let nephew = match store
        .publish_sparse(nephew, copy)
        .map_err(SparseRefusal::into_parts)
    {
        Err((MemoryError::WrongRange, Some(nephew))) => nephew,
        _ => panic!("a fork's child was not refused whole"),
    };
    drop(nephew);
    assert_eq!(store.len(), 2);
    let refused = match store
        .publish_sparse(sibling, copy)
        .map_err(SparseRefusal::into_parts)
    {
        Err((MemoryError::StalePreparation { .. }, Some(sibling))) => sibling,
        _ => panic!("a sibling planned on a passed base was not refused whole"),
    };
    assert_eq!(refused.changes().len(), 1);
    drop(refused);
    assert!(store.publish_sparse(second, copy).is_ok());
    let held: Vec<_> = store
        .entries()
        .map(|entry| (entry.key, entry.value.0))
        .collect();
    assert_eq!(held, vec![(2, 20), (3, 30)]);
    assert_eq!(store.prefix(), 2);
    drop(store);
    assert_eq!(budget.stats().used, 0);
}

/// A plan refuses what a rebuild's plan refuses: a key deleted that the base
/// does not hold, a key twice, a prefix not the next.
#[test]
fn a_sparse_plan_refuses_what_a_batch_plan_refuses() {
    let budget = MemoryBudget::new(1 << 20, 0).unwrap();
    let store = RangeStore::<u64, Value>::new(RangeId(8), 0, config(4, 4096), budget).unwrap();
    let put = |key: u64| Change::Put(Entry::new(key, Value(key), 0));
    assert!(matches!(
        store.plan_sparse(
            store.budget(),
            SparseBase::Committed,
            1,
            vec![Change::Delete(5)],
            BudgetLane::Ordinary,
            16,
            |_| None
        ),
        Err(MemoryError::MissingKey)
    ));
    assert!(matches!(
        store.plan_sparse(
            store.budget(),
            SparseBase::Committed,
            1,
            vec![put(1), put(1)],
            BudgetLane::Ordinary,
            16,
            |_| None
        ),
        Err(MemoryError::DuplicateKey)
    ));
    assert!(matches!(
        store.plan_sparse(
            store.budget(),
            SparseBase::Committed,
            2,
            vec![put(1)],
            BudgetLane::Ordinary,
            16,
            |_| None
        ),
        Err(MemoryError::PrefixMismatch { .. })
    ));
}

/// A sparse publication spends no more nodes than its plan reserved
/// (`PageDirectory::sparse_bound`, a change's path and the larger of two
/// inserted pages and one removed): in a directory four levels deep whose
/// every node and page a capture shares, each change of a batch under
/// another branch, every put an oversized row splitting its leaf in three and
/// every delete emptying its leaf. A publication that ran short would be
/// refused and poison the store. Measured here, a publication keeps about
/// 1.4 nodes a change of the 32 a change it reserves (doc 30 §9.2).
#[test]
fn a_sparse_publication_spends_within_its_node_bound() {
    // Two rows a leaf: a row too large for a leaf beside another goes in a
    // leaf of its own, so a put of one between a leaf's two rows makes three.
    let leaves = RangeConfig {
        max_batch_entries: 65_536,
        ..config(2, 4096)
    };
    let budget = MemoryBudget::new(1 << 30, 0).unwrap();
    let mut store = RangeStore::<u64, Value>::new(RangeId(9), 0, leaves, budget.clone()).unwrap();
    // 65,536 rows in 32,768 leaves: a directory four levels deep.
    let rows = (0..65_536u64)
        .map(|n| Change::Put(Entry::new(n * 16, Value(n), 0)))
        .collect();
    store.apply_batch(1, rows, BudgetLane::Ordinary).unwrap();
    let mut draw = Draw(9);
    let mut deleted = std::collections::BTreeSet::new();
    for prefix in 2..=40u64 {
        let capture = store.freeze().unwrap();
        // 24 changes, spread across the directory.
        let mut changes = Vec::new();
        for slot in 0..24u64 {
            let leaf = slot * 1365 + draw.below(1365);
            let first = leaf * 32;
            if draw.below(3) == 0 && !deleted.contains(&first) {
                // Every row of the leaf deleted, the leaf removed.
                for key in [first, first + 16] {
                    if store.get_entry(&key).is_some() {
                        changes.push(Change::Delete(key));
                    }
                }
                deleted.insert(first);
            } else {
                changes.push(Change::Put(Entry::new(first + 8, Value(prefix), 6000)));
            }
        }
        changes.sort_by_key(|change| *change.key());
        changes.dedup_by_key(|change| *change.key());
        let sparse = store
            .plan_sparse(
                store.budget(),
                SparseBase::Committed,
                prefix,
                changes,
                BudgetLane::Ordinary,
                store.config.max_batch_entries,
                |key| store.get_entry(key).map(|entry| entry.heap_bytes),
            )
            .unwrap();
        if let Err(refusal) = store.publish_sparse(sparse, copy) {
            panic!("prefix {prefix}: {:?}", refusal.error());
        }
        drop(capture);
    }
}

/// A key whose order is its prefix, as focal's native key orders by its
/// affinity first: a search settles on the cached prefixes and reads a page's
/// rows only where they tie.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Prefixed(u64);
impl crate::RangeKey for Prefixed {
    fn order_prefix(&self) -> u128 {
        u128::from(self.0)
    }
    fn cmp_past_prefix(&self, _other: &Self) -> std::cmp::Ordering {
        std::cmp::Ordering::Equal
    }
}

/// Measurement, not a check: a batch of 18 scattered keys (inserts and
/// replacements, as a claim's rows land in eighteen pages) over a store of
/// `FOCAL_MEASURE_ROWS` rows (100,000 by default), published by rebuild and in
/// place, each timed apart. Run with `--release --ignored --nocapture`.
#[test]
#[ignore = "measurement"]
fn measure_publication_in_place_against_rebuild() {
    let rows: u64 = std::env::var("FOCAL_MEASURE_ROWS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(100_000);
    let batches: u64 = std::env::var("FOCAL_MEASURE_BATCHES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(20_000);
    let leaves = config(128, 4096);
    let budget = MemoryBudget::new(4 << 30, 0).unwrap();
    let mut rebuilt =
        RangeStore::<Prefixed, Value>::new(RangeId(1), 0, leaves, budget.clone()).unwrap();
    let mut in_place =
        RangeStore::<Prefixed, Value>::new(RangeId(2), 0, leaves, budget.clone()).unwrap();
    let put = |key: u64| Change::Put(Entry::new(Prefixed(key), Value(key), 0));
    // Fill both with every other key, so later inserts land between rows.
    let mut prefix = 0u64;
    for chunk in (0..rows).step_by(256) {
        prefix += 1;
        let end = (chunk + 256).min(rows);
        rebuilt
            .apply_batch(
                prefix,
                (chunk..end).map(|i| put(i * 2)).collect(),
                BudgetLane::Ordinary,
            )
            .unwrap();
        in_place
            .apply_batch(
                prefix,
                (chunk..end).map(|i| put(i * 2)).collect(),
                BudgetLane::Ordinary,
            )
            .unwrap();
    }
    let mut draw = Draw(9);
    let (mut rebuild_ns, mut in_place_ns, mut plan_ns) = (0u128, 0u128, 0u128);
    for _ in 0..batches {
        prefix += 1;
        let mut keys = std::collections::BTreeSet::new();
        for _ in 0..18 {
            keys.insert(draw.below(rows * 2));
        }
        let started = std::time::Instant::now();
        rebuilt
            .apply_batch(
                prefix,
                keys.iter().map(|&key| put(key)).collect(),
                BudgetLane::Ordinary,
            )
            .unwrap();
        rebuild_ns += started.elapsed().as_nanos();
        let started = std::time::Instant::now();
        let sparse = in_place
            .plan_sparse(
                in_place.budget(),
                SparseBase::Committed,
                prefix,
                keys.iter().map(|&key| put(key)).collect(),
                BudgetLane::Ordinary,
                in_place.config.max_batch_entries,
                |key| in_place.get_entry(key).map(|entry| entry.heap_bytes),
            )
            .unwrap();
        plan_ns += started.elapsed().as_nanos();
        if in_place.publish_sparse(sparse, copy).is_err() {
            panic!("publication refused");
        }
        in_place_ns += started.elapsed().as_nanos();
    }
    let rows_of = |store: &RangeStore<Prefixed, Value>| -> Vec<(u64, u64)> {
        store
            .entries()
            .map(|entry| (entry.key.0, entry.value.0))
            .collect()
    };
    assert_eq!(rows_of(&rebuilt), rows_of(&in_place));
    println!(
        "{rows} rows, {batches} batches of 18: rebuild {:.2} µs, in place {:.2} µs a batch (its plan {:.2} µs)",
        rebuild_ns as f64 / batches as f64 / 1e3,
        in_place_ns as f64 / batches as f64 / 1e3,
        plan_ns as f64 / batches as f64 / 1e3,
    );
}

/// A row rewritten where it stands keeps its key, its page and the store's prefix and state: a
/// plan made before the rewrite publishes after it, and the page gives back the heap the row no
/// longer holds. While an image still reads the page, nothing is rewritten.
#[test]
fn a_row_rewritten_where_it_stands_gives_back_its_heap_and_leaves_a_pending_plan_valid() {
    let budget = MemoryBudget::new(8 << 20, 0).unwrap();
    let mut store =
        RangeStore::<u64, Value>::new(RangeId(7), 0, config(4, 4096), budget.clone()).unwrap();
    let put = |key: u64, heap: usize| Change::Put(Entry::new(key, Value(key * 10), heap));
    let first = store
        .plan_sparse(
            store.budget(),
            SparseBase::Committed,
            1,
            (0..10).map(|key| put(key, 256)).collect(),
            BudgetLane::Ordinary,
            16,
            |_| None,
        )
        .unwrap();
    assert!(store.publish_sparse(first, copy).is_ok());
    // A plan on the committed state, made before any rewrite.
    let pending = store
        .plan_sparse(
            store.budget(),
            SparseBase::Committed,
            2,
            vec![put(20, 0)],
            BudgetLane::Ordinary,
            16,
            |_| None,
        )
        .unwrap();
    let held = budget.stats().used;
    let rewritten = store.rewrite(&5, |value| Ok(Some((Value(value.0 + 1), 16))));
    assert_eq!(rewritten.unwrap(), Rewrite::Rewritten);
    assert_eq!(budget.stats().used, held - 240);
    let row = |store: &RangeStore<u64, Value>, key| {
        store
            .entries()
            .find(|entry| entry.key == key)
            .map(|entry| (entry.value.0, entry.heap_bytes))
    };
    assert_eq!(row(&store, 5), Some((51, 16)));
    assert_eq!(store.prefix(), 1);
    let absent = store.rewrite(&99, |_| Ok(Some((Value(0), 0))));
    assert_eq!(absent.unwrap(), Rewrite::Absent);
    assert_eq!(store.rewrite(&6, |_| Ok(None)).unwrap(), Rewrite::Unchanged);
    // A row is never grown where it stands: refused, nothing changed.
    assert!(
        store
            .rewrite(&6, |value| Ok(Some((Value(value.0), 512))))
            .is_err()
    );
    assert_eq!(row(&store, 6), Some((60, 256)));
    assert_eq!(budget.stats().used, held - 240);
    // An image of the state reads every page: nothing is rewritten while it is held.
    let frozen = store.freeze().unwrap();
    let shared = store.rewrite(&7, |_| Ok(Some((Value(0), 0))));
    assert_eq!(shared.unwrap(), Rewrite::Shared);
    assert_eq!(row(&store, 7), Some((70, 256)));
    drop(frozen);
    let rewritten = store.rewrite(&7, |_| Ok(Some((Value(0), 0))));
    assert_eq!(rewritten.unwrap(), Rewrite::Rewritten);
    assert_eq!(row(&store, 7), Some((0, 0)));
    // The plan made before the rewrites publishes after them.
    assert!(store.publish_sparse(pending, copy).is_ok());
    assert_eq!((store.prefix(), store.len()), (2, 11));
    drop(store);
    assert_eq!(budget.stats().used, 0);
}
