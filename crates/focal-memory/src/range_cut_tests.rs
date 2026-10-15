//! The cut rule (`layout::Cutter`): the greedy rule's number of leaves, every
//! bound kept, cuts where rows came in; and the fill it leaves (doc 30 §9).

use super::layout::{Cell, Cutter, LeafPartition, LeafSpan, Origin};
use super::*;

#[derive(Debug, PartialEq, Eq, Clone)]
struct Value(u64);

#[derive(Clone, Copy, Debug)]
struct Probe {
    heap: usize,
    origin: Origin,
    part: u8,
}

impl Cell for Probe {
    fn heap(&self) -> usize {
        self.heap
    }
    fn origin(&self) -> Origin {
        self.origin
    }
    fn parts_from(&self, next: &Self) -> bool {
        self.part != next.part
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

fn config(page_entries: usize, page_bytes: usize) -> RangeConfig {
    RangeConfig {
        page_entries,
        page_bytes,
        ..RangeConfig::default()
    }
}

fn cut(cells: &[Probe], config: RangeConfig) -> Vec<(usize, usize)> {
    let mut spans = Vec::new();
    let mut scratch = Vec::with_capacity(cells.len());
    Cutter::new::<u64, Value>(config)
        .unwrap()
        .cut(cells, &mut scratch, &mut |span: LeafSpan| {
            spans.push((span.start, span.len));
            Ok(())
        })
        .unwrap();
    spans
}

/// The greedy rule's leaves, from the left.
fn greedy(cells: &[Probe], config: RangeConfig) -> Vec<(usize, usize)> {
    let mut spans = Vec::new();
    let mut leaves = LeafPartition::new::<u64, Value>(config).unwrap();
    let mut previous: Option<u8> = None;
    for cell in cells {
        let boundary = previous.is_some_and(|part| part != cell.part);
        leaves
            .push(cell.heap, boundary, &mut |span: LeafSpan| {
                spans.push((span.start, span.len));
                Ok(())
            })
            .unwrap();
        previous = Some(cell.part);
    }
    leaves
        .finish(&mut |span: LeafSpan| {
            spans.push((span.start, span.len));
            Ok(())
        })
        .unwrap();
    spans
}

/// The greedy rule's leaves, from the right.
fn greedy_right(cells: &[Probe], config: RangeConfig) -> Vec<(usize, usize)> {
    let reversed: Vec<Probe> = cells.iter().rev().copied().collect();
    let n = cells.len();
    let mut spans: Vec<(usize, usize)> = greedy(&reversed, config)
        .into_iter()
        .map(|(start, len)| (n - start - len, len))
        .collect();
    spans.reverse();
    spans
}

fn check_bounds(cells: &[Probe], spans: &[(usize, usize)], config: RangeConfig) {
    let base = page_charge::<u64, Value>(0, 0).unwrap();
    let inline = size_of::<Entry<u64, Value>>();
    let mut next = 0;
    for &(start, len) in spans {
        assert_eq!(start, next, "leaves are contiguous");
        assert!(len > 0, "no leaf is empty");
        let leaf = &cells[start..start + len];
        assert!(
            len <= config.page_entries,
            "{len} rows past the entry bound"
        );
        assert!(
            leaf.windows(2).all(|pair| pair[0].part == pair[1].part),
            "a leaf mixes partitions"
        );
        let bytes: usize = base + leaf.iter().map(|cell| inline + cell.heap).sum::<usize>();
        assert!(
            len == 1 || bytes <= config.page_bytes,
            "{bytes} bytes past the byte bound"
        );
        next = start + len;
    }
    assert_eq!(next, cells.len(), "the leaves cover every row");
}

/// Random groups under count, byte and partition bounds, oversized rows among
/// them: every leaf keeps its bounds, the cut takes exactly the greedy rule's
/// number of leaves, appended rows pack as the greedy rule from the left and
/// prepended rows as it from the right.
#[test]
fn the_cut_takes_the_greedy_count_and_keeps_every_bound() {
    let base = page_charge::<u64, Value>(0, 0).unwrap();
    let inline = size_of::<Entry<u64, Value>>();
    let mut draw = Draw(11);
    let mut packed = [0usize; 3];
    for round in 0..4000 {
        let config = match round % 5 {
            0 => config(1, 4096),
            1 => config(3, 4096),
            2 => config(8, base + 4 * inline + 64),
            3 => config(128, 4096),
            _ => config(16, usize::MAX),
        };
        let len = 1 + draw.below(300) as usize;
        let parts = 1 + draw.below(4);
        // Old rows, then inserts after them, before them, or among them.
        let shape = draw.below(3);
        let mut cells: Vec<Probe> = (0..len)
            .map(|index| {
                let heap = match draw.below(10) {
                    // Oversized rows in every other round.
                    0 if round % 2 == 0 => 3 * config.page_bytes.min(8192) / 2,
                    1..=3 => draw.below(200) as usize,
                    _ => 0,
                };
                let inserted = match shape {
                    0 => index >= len / 2,
                    1 => index < len / 2,
                    _ => draw.below(2) == 0,
                };
                Probe {
                    heap,
                    origin: if inserted {
                        Origin::Inserted
                    } else if draw.below(5) == 0 {
                        Origin::Replacement
                    } else {
                        Origin::Old
                    },
                    part: 0,
                }
            })
            .collect();
        // Partitions in runs.
        let mut part = 0u8;
        for cell in &mut cells {
            if draw.below(40) == 0 {
                part = (part + 1) % parts as u8;
            }
            cell.part = part;
        }
        let spans = cut(&cells, config);
        check_bounds(&cells, &spans, config);
        let left = greedy(&cells, config);
        assert_eq!(
            spans.len(),
            left.len(),
            "round {round}: the greedy rule's count"
        );
        // One run, appended or prepended past its old rows: packed.
        let one_run = cells.windows(2).all(|pair| pair[0].part == pair[1].part)
            && cells
                .iter()
                .all(|cell| len == 1 || inline + cell.heap <= config.page_bytes - base);
        if one_run && cells.iter().any(|cell| cell.origin == Origin::Old) {
            let olds = cells
                .iter()
                .enumerate()
                .filter(|(_, c)| c.origin == Origin::Old);
            let (first_old, last_old) = (
                olds.clone().map(|(i, _)| i).min().unwrap(),
                olds.map(|(i, _)| i).max().unwrap(),
            );
            let inserts: Vec<usize> = cells
                .iter()
                .enumerate()
                .filter(|(_, c)| c.origin == Origin::Inserted)
                .map(|(i, _)| i)
                .collect();
            if !inserts.is_empty() && inserts.iter().all(|&i| i > last_old) {
                assert_eq!(
                    spans, left,
                    "round {round}: appended rows pack from the left"
                );
                packed[0] += 1;
            } else if !inserts.is_empty() && inserts.iter().all(|&i| i < first_old) {
                assert_eq!(
                    spans,
                    greedy_right(&cells, config),
                    "round {round}: prepended rows pack from the right"
                );
                packed[1] += 1;
            } else {
                packed[2] += 1;
            }
        }
    }
    assert!(
        packed.iter().all(|count| *count > 50),
        "every rule exercised: {packed:?}"
    );
}

/// A full leaf and a row inserted among its rows split in half; rows of
/// unequal bytes split by bytes.
#[test]
fn rows_inserted_among_a_leafs_rows_split_it_evenly() {
    let leaves = config(128, usize::MAX);
    let mut cells = vec![
        Probe {
            heap: 0,
            origin: Origin::Old,
            part: 0,
        };
        129
    ];
    cells[40].origin = Origin::Inserted;
    assert_eq!(cut(&cells, leaves), vec![(0, 64), (64, 65)]);
    // Ten heavy rows first: the cut falls near the bytes' middle.
    let base = page_charge::<u64, Value>(0, 0).unwrap();
    let inline = size_of::<Entry<u64, Value>>();
    let bytes = config(128, base + 64 * (inline + 64));
    let mut cells = vec![
        Probe {
            heap: 64,
            origin: Origin::Old,
            part: 0,
        };
        65
    ];
    cells[30].origin = Origin::Inserted;
    let spans = cut(&cells, bytes);
    assert_eq!(spans.len(), 2);
    assert!(spans[0].1.abs_diff(spans[1].1) <= 1, "{spans:?}");
}

/// Random inserts fill leaves as B-tree halving does (ln 2, Yao 1978), not a
/// row a leaf; ordered inserts, ascending or descending, fill them. Both
/// publications lay rows out alike, so the rebuild's leaves are measured.
#[test]
fn inserts_fill_leaves_as_a_b_tree_does() {
    let leaves = config(128, usize::MAX);
    for (order, least) in [("random", 0.6), ("ascending", 0.95), ("descending", 0.95)] {
        let budget = MemoryBudget::new(1 << 30, 0).unwrap();
        let mut store = RangeStore::<u64, Value>::new(RangeId(1), 0, leaves, budget).unwrap();
        let mut draw = Draw(5);
        let mut prefix = 0u64;
        let mut next = 0u64;
        while store.len() < 20_000 {
            let mut keys: Vec<u64> = (0..18)
                .map(|_| match order {
                    "random" => draw.next(),
                    "ascending" => {
                        next += 1;
                        next
                    }
                    _ => {
                        next += 1;
                        u64::MAX - next
                    }
                })
                .collect();
            keys.sort_unstable();
            keys.dedup();
            prefix += 1;
            store
                .apply_batch(
                    prefix,
                    keys.into_iter()
                        .map(|key| Change::Put(Entry::new(key, Value(key), 0)))
                        .collect(),
                    BudgetLane::Ordinary,
                )
                .unwrap();
        }
        let pages = store.root.pages.len();
        let fill = store.len() as f64 / pages as f64 / leaves.page_entries as f64;
        assert!(
            fill >= least,
            "{order} inserts fill leaves {fill:.2}, at least {least}"
        );
    }
}
