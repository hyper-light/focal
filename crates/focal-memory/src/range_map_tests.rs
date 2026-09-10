use super::*;

fn limits() -> RangeMapLimits {
    RangeMapLimits { max_ranges: 8 }
}
fn range(
    id: u128,
    generation: u64,
    start: Option<u32>,
    end: Option<u32>,
) -> RangeDescriptor<u32, ()> {
    RangeDescriptor {
        id: RangeId(id),
        generation,
        span: KeySpan { start, end },
        meta: (),
    }
}
fn map() -> RangeMap<u32, ()> {
    RangeMap::new(
        1,
        vec![
            range(1, 1, None, Some(10)),
            range(2, 1, Some(10), Some(20)),
            range(3, 1, Some(20), None),
        ],
        limits(),
    )
    .unwrap()
}

#[test]
fn spans_hold_half_open_key_intervals_and_intersect() {
    let all = KeySpan::<u32>::all();
    assert!(all.contains(&0) && all.contains(&u32::MAX));
    let middle = KeySpan {
        start: Some(10),
        end: Some(20),
    };
    assert!(middle.contains(&10) && middle.contains(&19));
    assert!(!middle.contains(&9) && !middle.contains(&20));
    assert_eq!(
        KeySpan {
            start: Some(5),
            end: Some(15)
        }
        .intersection(&middle),
        Some(KeySpan {
            start: Some(10),
            end: Some(15)
        })
    );
    assert_eq!(
        KeySpan {
            start: Some(20),
            end: None
        }
        .intersection(&middle),
        None
    );
    assert_eq!(all.intersection(&middle), Some(middle));
    assert_eq!(
        KeySpan {
            start: Some(3),
            end: Some(3)
        }
        .validate(),
        Err(RangeMapError::Invalid("empty or reversed span"))
    );
}

#[test]
fn a_valid_map_routes_every_key_to_the_range_holding_it() {
    let map = map();
    assert_eq!(map.route(&0), Some(0));
    assert_eq!(map.route(&9), Some(0));
    assert_eq!(map.route(&10), Some(1));
    assert_eq!(map.route(&19), Some(1));
    assert_eq!(map.route(&20), Some(2));
    assert_eq!(map.route(&u32::MAX), Some(2));
    assert_eq!(map.routed(&15).map(|range| range.id), Some(RangeId(2)));
    assert_eq!(map.position(RangeId(3)), Some(2));
    assert_eq!(map.get(RangeId(9)), None);
    assert_eq!(map.len(), 3);
}

#[test]
fn gaps_overlaps_duplicates_and_bounds_are_refused() {
    let l = limits();
    assert_eq!(
        RangeMap::<u32, ()>::new(0, vec![range(1, 1, None, None)], l).unwrap_err(),
        RangeMapError::Invalid("empty map or zero epoch")
    );
    assert_eq!(
        RangeMap::<u32, ()>::new(1, vec![], l).unwrap_err(),
        RangeMapError::Invalid("empty map or zero epoch")
    );
    assert_eq!(
        RangeMap::new(1, vec![range(1, 1, Some(1), None)], l).unwrap_err(),
        RangeMapError::Gap
    );
    assert_eq!(
        RangeMap::new(1, vec![range(1, 1, None, Some(5))], l).unwrap_err(),
        RangeMapError::Gap
    );
    assert_eq!(
        RangeMap::new(
            1,
            vec![range(1, 1, None, Some(5)), range(2, 1, Some(6), None)],
            l
        )
        .unwrap_err(),
        RangeMapError::Gap
    );
    assert_eq!(
        RangeMap::new(
            1,
            vec![range(1, 1, None, Some(5)), range(2, 1, Some(4), None)],
            l
        )
        .unwrap_err(),
        RangeMapError::Overlap
    );
    assert_eq!(
        RangeMap::new(
            1,
            vec![range(1, 1, None, None), range(2, 1, Some(4), None)],
            l
        )
        .unwrap_err(),
        RangeMapError::Overlap
    );
    assert_eq!(
        RangeMap::new(
            1,
            vec![range(1, 1, None, Some(5)), range(1, 1, Some(5), None)],
            l
        )
        .unwrap_err(),
        RangeMapError::Conflict
    );
    assert_eq!(
        RangeMap::new(1, vec![range(1, 0, None, None)], l).unwrap_err(),
        RangeMapError::Generation
    );
    let many: Vec<_> = (0..9)
        .map(|i| {
            range(
                i,
                1,
                (i > 0).then_some(u32::try_from(i).unwrap()),
                (i < 8).then_some(u32::try_from(i + 1).unwrap()),
            )
        })
        .collect();
    assert_eq!(
        RangeMap::new(1, many, l).unwrap_err(),
        RangeMapError::Capacity
    );
}

#[test]
fn replacement_splits_merges_and_moves_under_generation_rules() {
    let map = map();
    let split = map
        .replace(
            &[RangeId(2)],
            vec![
                range(2, 2, Some(10), Some(15)),
                range(4, 1, Some(15), Some(20)),
            ],
            limits(),
        )
        .unwrap();
    assert_eq!(split.epoch(), 2);
    assert_eq!(split.len(), 4);
    assert_eq!(split.route(&17), Some(2));
    let merged = split
        .replace(
            &[RangeId(2), RangeId(4)],
            vec![range(2, 3, Some(10), Some(20))],
            limits(),
        )
        .unwrap();
    assert_eq!(merged.epoch(), 3);
    assert_eq!(merged.ranges().len(), 3);
    assert_eq!(merged.get(RangeId(2)).unwrap().generation, 3);
    // A move keeps the span and advances the generation.
    let moved = merged
        .replace(&[RangeId(3)], vec![range(3, 2, Some(20), None)], limits())
        .unwrap();
    assert_eq!(moved.get(RangeId(3)).unwrap().generation, 2);
    // Refusals: a stale generation, a reused identity from outside the
    // sources, a noncontiguous source set, a mismatched boundary, an unknown
    // source and an empty replacement.
    assert_eq!(
        map.replace(
            &[RangeId(2)],
            vec![range(2, 1, Some(10), Some(20))],
            limits()
        )
        .unwrap_err(),
        RangeMapError::Generation
    );
    assert_eq!(
        map.replace(
            &[RangeId(2)],
            vec![range(3, 2, Some(10), Some(20))],
            limits()
        )
        .unwrap_err(),
        RangeMapError::Generation
    );
    assert_eq!(
        map.replace(
            &[RangeId(1), RangeId(3)],
            vec![range(5, 1, None, None)],
            limits()
        )
        .unwrap_err(),
        RangeMapError::Gap
    );
    assert_eq!(
        map.replace(
            &[RangeId(2)],
            vec![range(5, 1, Some(10), Some(21))],
            limits()
        )
        .unwrap_err(),
        RangeMapError::Gap
    );
    assert_eq!(
        map.replace(
            &[RangeId(7)],
            vec![range(5, 1, Some(10), Some(20))],
            limits()
        )
        .unwrap_err(),
        RangeMapError::Missing
    );
    assert_eq!(
        map.replace(
            &[RangeId(2), RangeId(2)],
            vec![range(5, 1, Some(10), Some(20))],
            limits()
        )
        .unwrap_err(),
        RangeMapError::Missing
    );
    assert_eq!(
        map.replace(&[RangeId(2)], vec![], limits()).unwrap_err(),
        RangeMapError::Invalid("replacement set")
    );
    let (epoch, ranges) = moved.into_parts();
    assert_eq!((epoch, ranges.len()), (4, 3));
}
