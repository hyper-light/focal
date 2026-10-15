use super::*;

fn position(sequence: u64, ordinal: u32) -> Position {
    Position {
        sequence: SessionSeq(sequence),
        ordinal,
    }
}
fn budget() -> MemoryBudget {
    MemoryBudget::new(1 << 20, 0).unwrap()
}
fn funding(budget: &MemoryBudget, entries: u32) -> Allocation {
    budget
        .reserve(
            BudgetKind::Index,
            BudgetLane::Ordinary,
            entry_charge() * entries as usize,
        )
        .unwrap()
        .commit()
}
fn filed(index: &EventIndex, home: u8) -> Vec<Position> {
    index.positions([home; 16]).collect()
}

/// Events are filed under their homes in the order they were published, each on the charge its
/// publication funded; one unfiled gives its charge back and leaves the others in order, and
/// the index let go gives back everything, its root's charge too.
#[test]
fn events_are_filed_and_unfiled_under_their_homes() {
    let budget = budget();
    let mut index = EventIndex::empty(&budget).unwrap();
    let root = index.charged();
    assert_eq!(budget.stats().used, root);
    index.fund(funding(&budget, 5));
    for (sequence, home) in [(1, 1), (2, 2), (3, 1), (4, 1), (5, 2)] {
        index.file([home; 16], None, position(sequence, 0));
    }
    assert!(index.complete());
    assert_eq!(index.charged(), root + 5 * entry_charge());
    assert_eq!(budget.stats().used, index.charged());
    assert_eq!(
        filed(&index, 1),
        [position(1, 0), position(3, 0), position(4, 0)]
    );
    assert_eq!(filed(&index, 2), [position(2, 0), position(5, 0)]);
    assert!(filed(&index, 3).is_empty());
    index.unfile([1; 16], None, position(3, 0));
    assert_eq!(filed(&index, 1), [position(1, 0), position(4, 0)]);
    assert_eq!(index.charged(), root + 4 * entry_charge());
    assert_eq!(budget.stats().used, index.charged());
    index.drop_all();
    assert!(!index.complete());
    assert_eq!(
        budget.stats().used,
        0,
        "the index let go gives back its charge"
    );
}

/// What the index cannot keep exactly it does not keep at all: unfiling an event it does not
/// hold, or filing one it holds, lets it go, empty and uncharged, for the rows to build again;
/// unbuilt, it files nothing and lets funding go; and its root is charged before it is made.
#[test]
fn what_the_index_cannot_keep_exactly_it_lets_go() {
    let budget = budget();
    let mut index = EventIndex::empty(&budget).unwrap();
    index.fund(funding(&budget, 1));
    index.file([1; 16], None, position(1, 0));
    index.unfile([1; 16], None, position(2, 0));
    assert!(!index.complete());
    assert!(filed(&index, 1).is_empty());
    assert_eq!(budget.stats().used, 0);
    // Unbuilt, it lets funding go and files nothing.
    index.fund(funding(&budget, 1));
    index.file([1; 16], None, position(3, 0));
    assert!(filed(&index, 1).is_empty());
    assert_eq!(budget.stats().used, 0);
    // The same event filed twice.
    let mut index = EventIndex::empty(&budget).unwrap();
    index.fund(funding(&budget, 2));
    index.file([1; 16], None, position(1, 0));
    index.file([1; 16], None, position(1, 0));
    assert!(!index.complete());
    assert_eq!(budget.stats().used, 0);
    // A budget without room for its root refuses to make it.
    let tight = MemoryBudget::new(1, 0).unwrap();
    assert!(EventIndex::empty(&tight).is_err());
}

#[test]
fn inbox_ranges_seek_exclusively_and_retirement_returns_both_charges() {
    let budget = budget();
    let mut index = EventIndex::empty(&budget).unwrap();
    let root = index.charged();
    let first = ParticipantId([1; 16]);
    let other = ParticipantId([2; 16]);
    index.fund(funding(&budget, 7));
    index.file([1; 16], Some(first), position(1, 0));
    index.file([2; 16], Some(other), position(1, 1));
    index.file([3; 16], Some(first), position(2, 0));
    index.file([4; 16], None, position(3, 0));
    assert_eq!(index.charged(), root + 7 * entry_charge());
    assert_eq!(budget.stats().used, index.charged());
    assert_eq!(
        filed(&index, 1),
        [position(1, 0)],
        "an equal object and participant id never mix"
    );
    let deliveries = |index: &EventIndex, after| index.deliveries(first, after).collect::<Vec<_>>();
    assert_eq!(deliveries(&index, None), [position(1, 0), position(2, 0)]);
    assert_eq!(
        deliveries(&index, Some((SessionSeq(1), u32::MAX))),
        [position(2, 0)]
    );
    assert!(deliveries(&index, Some((SessionSeq(u64::MAX), u32::MAX))).is_empty());
    index.unfile([1; 16], Some(first), position(1, 0));
    assert_eq!(deliveries(&index, None), [position(2, 0)]);
    assert_eq!(index.charged(), root + 5 * entry_charge());
    assert_eq!(budget.stats().used, index.charged());
}
