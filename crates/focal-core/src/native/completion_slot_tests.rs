//! Each count's bound keeps a completion share fresh work cannot take: what
//! the book checks, on each lane, against the actual counts alone.
use super::*;

struct Dimension {
    name: &'static str,
    actual: fn(&mut Meta) -> &mut usize,
    ceiling: fn(&mut NativeLimits) -> &mut usize,
}

const DIMENSIONS: [Dimension; 8] = [
    Dimension {
        name: "claims",
        actual: |meta| &mut meta.claims,
        ceiling: |limits| &mut limits.claims,
    },
    Dimension {
        name: "definitions",
        actual: |meta| &mut meta.definitions,
        ceiling: |limits| &mut limits.definitions,
    },
    Dimension {
        name: "evaluations",
        actual: |meta| &mut meta.evaluations,
        ceiling: |limits| &mut limits.evaluations,
    },
    Dimension {
        name: "responses",
        actual: |meta| &mut meta.responses,
        ceiling: |limits| &mut limits.responses,
    },
    Dimension {
        name: "receipts",
        actual: |meta| &mut meta.receipts,
        ceiling: |limits| &mut limits.receipts,
    },
    Dimension {
        name: "result testaments",
        actual: |meta| &mut meta.result_testaments,
        ceiling: |limits| &mut limits.claims,
    },
    Dimension {
        name: "monitors",
        actual: |meta| &mut meta.monitors,
        ceiling: |limits| &mut limits.monitors,
    },
    Dimension {
        name: "monitor links",
        actual: |meta| &mut meta.monitor_links,
        ceiling: |limits| &mut limits.monitor_links,
    },
];

#[test]
fn each_count_keeps_a_completion_share_that_fresh_work_cannot_take() {
    for dimension in &DIMENSIONS {
        let source = source();
        let mut limits = fixture::core().limits;
        // A bound of 32 keeps two (a sixteenth) for completion.
        *(dimension.ceiling)(&mut limits) = 32;
        let book = CompletionBook::new(&source, limits).unwrap();
        let before = source.stats();
        let mut meta = Meta::default();
        for (count, ordinary, completion) in [
            (30, true, true),
            (31, false, true),
            (32, false, true),
            (33, false, false),
        ] {
            *(dimension.actual)(&mut meta) = count;
            assert_eq!(
                book.check_slots(meta, SessionSeq(1), 1, BudgetLane::Ordinary)
                    .is_ok(),
                ordinary,
                "{} at {count}, fresh",
                dimension.name
            );
            assert_eq!(
                book.check_slots(meta, SessionSeq(1), 1, BudgetLane::Completion)
                    .is_ok(),
                completion,
                "{} at {count}, completion",
                dimension.name
            );
        }
        // Nothing is set aside or charged for the share.
        assert_eq!(source.stats(), before);
        assert_eq!(book.totals, Totals::default());
    }
}

#[test]
fn resident_outcomes_keep_no_share_a_seal_makes_their_room() {
    let source = source();
    let mut limits = fixture::core().limits;
    limits.outcomes = 32;
    let book = CompletionBook::new(&source, limits).unwrap();
    let meta = Meta {
        outcomes: 40,
        sealed: 8,
        ..Meta::default()
    };
    for lane in [BudgetLane::Ordinary, BudgetLane::Completion] {
        book.check_slots(meta, SessionSeq(1), 1, lane).unwrap();
        assert!(
            book.check_slots(
                Meta {
                    outcomes: 41,
                    ..meta
                },
                SessionSeq(1),
                1,
                lane
            )
            .is_err()
        );
    }
}

#[test]
fn actual_metadata_overflow_refuses_even_without_live_completion_grants() {
    for dimension in &DIMENSIONS {
        let source = source();
        let mut limits = fixture::core().limits;
        *(dimension.ceiling)(&mut limits) = 1;
        let book = CompletionBook::new(&source, limits).unwrap();
        let before = source.stats();
        let mut meta = Meta::default();
        *(dimension.actual)(&mut meta) = 1;
        book.check_slots(meta, SessionSeq(1), 1, BudgetLane::Completion)
            .unwrap();
        *(dimension.actual)(&mut meta) = 2;
        assert!(
            matches!(
                book.check_slots(meta, SessionSeq(1), 1, BudgetLane::Completion),
                Err(NativeError::Capacity("preparation bytes"))
            ),
            "{} actual capacity",
            dimension.name
        );
        assert_eq!(source.stats(), before);
        assert_eq!(book.totals, Totals::default());
    }
}
