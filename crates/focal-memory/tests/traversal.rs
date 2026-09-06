#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
use focal_memory::{
    BudgetLane, Change, Entry, MemoryBudget, MemoryError, RangeConfig, RangeId, RangeStore,
    ReadBudget, TraversalLimits, TraversalQuery, TraversalStop,
};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

fn graph(edges: &[(u64, &[u64])]) -> (RangeStore<u64, Vec<u64>>, MemoryBudget) {
    let budget = MemoryBudget::new(16 * 1024 * 1024, 4096).unwrap();
    let mut store = RangeStore::new(
        RangeId(1),
        0,
        RangeConfig {
            page_entries: 3,
            ..RangeConfig::default()
        },
        budget.clone(),
    )
    .unwrap();
    store
        .apply_batch(
            1,
            edges
                .iter()
                .map(|(key, neighbors)| {
                    Change::Put(Entry::new(
                        *key,
                        neighbors.to_vec(),
                        std::mem::size_of_val(*neighbors),
                    ))
                })
                .collect(),
            BudgetLane::Ordinary,
        )
        .unwrap();
    (store, budget)
}

fn next(
    _: &u64,
    neighbors: &impl AsRef<[u64]>,
    after: Option<&u64>,
) -> Result<Option<u64>, MemoryError> {
    let neighbors = neighbors.as_ref();
    let offset = after.map_or(0, |key| {
        neighbors.partition_point(|neighbor| neighbor <= key)
    });
    Ok(neighbors.get(offset).copied())
}

fn oracle(edges: &BTreeMap<u64, Vec<u64>>, start: u64) -> Vec<u64> {
    let mut seen = BTreeSet::from([start]);
    let mut queue = VecDeque::from([start]);
    let mut result = Vec::new();
    while let Some(key) = queue.pop_front() {
        result.push(key);
        for neighbor in &edges[&key] {
            if seen.insert(*neighbor) {
                queue.push_back(*neighbor);
            }
        }
    }
    result
}

#[test]
fn breadth_first_cycles_and_diamonds_page_without_duplicates_at_fixed_prefix() {
    let edges: &[(u64, &[u64])] = &[
        (0, &[1, 2]),
        (1, &[0, 3]),
        (2, &[3, 4]),
        (3, &[4]),
        (4, &[1]),
    ];
    let expected = oracle(
        &edges
            .iter()
            .map(|(key, neighbors)| (*key, neighbors.to_vec()))
            .collect(),
        0,
    );
    let (mut store, budget) = graph(edges);
    let lease = store.pin(0, 100).unwrap();
    let query = TraversalQuery {
        root: 0,
        fingerprint: [5; 32],
    };
    let limits = TraversalLimits {
        max_nodes: 16,
        max_state_bytes: 100_000,
        ..TraversalLimits::default()
    };
    let page_budget = ReadBudget {
        max_items: 1,
        max_bytes: 1000,
        max_edge_visits: 2,
    };
    let mut cursor = None;
    let mut visited = Vec::new();
    let mut pages = 0;
    let mut total_probes = 0;
    loop {
        let mut calls = 0;
        let mut page = lease
            .traverse(
                &query,
                limits,
                page_budget,
                cursor,
                1,
                |key, neighbors, after| {
                    calls += 1;
                    next(key, neighbors, after)
                },
            )
            .unwrap();
        assert_eq!(page.prefix, 1);
        assert!(page.len() <= 1);
        assert_eq!(calls, page.edge_visits);
        assert!(calls <= 2);
        total_probes += calls;
        assert_eq!(page.total_edge_visits, total_probes);
        visited.extend(page.items().iter().map(|entry| entry.key));
        cursor = page.continuation.take();
        pages += 1;
        if cursor.is_none() {
            assert_eq!(page.stop, TraversalStop::Complete);
            break;
        }
        assert_eq!(page.stop, TraversalStop::PageLimit);
        // Changing the current graph after every response must not change
        // either the original traversal ordering or its pinned adjacency.
        store
            .apply_batch(
                store.prefix() + 1,
                vec![Change::Put(Entry::new(0, vec![4], 8))],
                BudgetLane::Ordinary,
            )
            .unwrap();
        assert!(pages < 20);
    }
    assert_eq!(visited, expected);
    drop(store);
    drop(lease);
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn random_indexed_graphs_match_breadth_first_oracle() {
    let mut seed = 59u64;
    for _ in 0..30 {
        let mut edges = BTreeMap::new();
        for key in 0..40 {
            let mut neighbors = BTreeSet::new();
            for _ in 0..4 {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                neighbors.insert((seed >> 24) % 40);
            }
            edges.insert(key, neighbors.into_iter().collect::<Vec<_>>());
        }
        let refs: Vec<_> = edges
            .iter()
            .map(|(key, neighbors)| (*key, neighbors.as_slice()))
            .collect();
        let (mut store, _) = graph(&refs);
        let lease = store.pin(0, 100).unwrap();
        let query = TraversalQuery {
            root: 0,
            fingerprint: [1; 32],
        };
        let limits = TraversalLimits {
            max_nodes: 50,
            max_state_bytes: 100_000,
            ..TraversalLimits::default()
        };
        let page_budget = ReadBudget {
            max_items: 3,
            max_bytes: 500,
            max_edge_visits: 5,
        };
        let mut cursor = None;
        let mut result = Vec::new();
        loop {
            let mut page = lease
                .traverse(&query, limits, page_budget, cursor, 0, next)
                .unwrap();
            result.extend(page.items().iter().map(|entry| entry.key));
            cursor = page.continuation.take();
            if cursor.is_none() {
                assert_eq!(page.stop, TraversalStop::Complete);
                break;
            }
        }
        assert_eq!(result, oracle(&edges, 0));
    }
}

#[test]
fn cumulative_bounds_report_truncation_and_release_working_memory() {
    let (mut store, budget) = graph(&[(0, &[1]), (1, &[2]), (2, &[3]), (3, &[])]);
    let lease = store.pin(0, 10).unwrap();
    let baseline = budget.stats().used;
    let query = TraversalQuery {
        root: 0,
        fingerprint: [0; 32],
    };
    let default = TraversalLimits {
        max_nodes: 10,
        max_state_bytes: 100_000,
        ..TraversalLimits::default()
    };
    for (limits, expected) in [
        (
            TraversalLimits {
                max_depth: 1,
                ..default
            },
            TraversalStop::DepthLimit,
        ),
        (
            TraversalLimits {
                max_edges: 1,
                ..default
            },
            TraversalStop::EdgeLimit,
        ),
        (
            TraversalLimits {
                max_nodes: 1,
                ..default
            },
            TraversalStop::NodeLimit,
        ),
    ] {
        let page = lease
            .traverse(&query, limits, ReadBudget::default(), None, 0, next)
            .unwrap();
        assert_eq!(page.stop, expected);
        assert!(page.continuation.is_none());
        drop(page);
        assert_eq!(budget.stats().used, baseline);
    }
    let too_small = TraversalLimits {
        max_state_bytes: 1,
        ..default
    };
    assert!(matches!(
        lease.traverse(&query, too_small, ReadBudget::default(), None, 0, next),
        Err(MemoryError::Capacity { .. })
    ));
    assert_eq!(budget.stats().used, baseline);
}

#[test]
fn continuation_scope_expiry_and_neighbor_order_are_enforced() {
    let (mut store, budget) = graph(&[(0, &[1, 2]), (1, &[]), (2, &[])]);
    let lease = store.pin(0, 10).unwrap();
    let other = store.pin(0, 10).unwrap();
    let query = TraversalQuery {
        root: 0,
        fingerprint: [0; 32],
    };
    let limits = TraversalLimits {
        max_nodes: 10,
        max_state_bytes: 100_000,
        ..TraversalLimits::default()
    };
    let page_budget = ReadBudget {
        max_edge_visits: 1,
        ..ReadBudget::default()
    };
    let make_cursor = || {
        lease
            .traverse(&query, limits, page_budget, None, 0, next)
            .unwrap()
            .continuation
    };
    assert!(matches!(
        other.traverse(&query, limits, page_budget, make_cursor(), 0, next),
        Err(MemoryError::WrongLease)
    ));
    let changed = TraversalQuery {
        root: 0,
        fingerprint: [1; 32],
    };
    assert!(matches!(
        lease.traverse(&changed, limits, page_budget, make_cursor(), 0, next),
        Err(MemoryError::QueryMismatch)
    ));
    assert!(matches!(
        lease.traverse(
            &query,
            limits,
            ReadBudget::default(),
            None,
            0,
            |_, _, _| Ok(Some(1))
        ),
        Err(MemoryError::InvalidNeighbors)
    ));
    let cursor = make_cursor();
    let before = budget.stats().used;
    store.advance_clock(10).unwrap();
    assert!(matches!(
        lease.traverse(&query, limits, page_budget, cursor, 9, next),
        Err(MemoryError::LeaseExpired)
    ));
    assert!(budget.stats().used < before);
}

#[test]
fn referenced_absence_is_not_silently_reported_as_complete_graph() {
    let (mut store, _) = graph(&[(0, &[1])]);
    let lease = store.pin(0, 10).unwrap();
    let query = TraversalQuery {
        root: 0,
        fingerprint: [0; 32],
    };
    let limits = TraversalLimits {
        max_nodes: 10,
        max_state_bytes: 100_000,
        ..TraversalLimits::default()
    };
    assert!(matches!(
        lease.traverse(&query, limits, ReadBudget::default(), None, 0, next),
        Err(MemoryError::MissingKey)
    ));
}
