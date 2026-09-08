//! Synthetic slot profiles test the shared book accounting boundary. They do
//! not install a new claimant promise or admit a mismatched lifecycle command.
use super::*;

struct Dimension {
    name: &'static str,
    promised: fn(&mut CompletionSlots) -> &mut usize,
    actual: fn(&mut Meta) -> &mut usize,
    ceiling: fn(&mut NativeLimits) -> &mut usize,
}

const DIMENSIONS: [Dimension; 8] = [
    Dimension {
        name: "claims",
        promised: |slots| &mut slots.claims,
        actual: |meta| &mut meta.claims,
        ceiling: |limits| &mut limits.claims,
    },
    Dimension {
        name: "definitions",
        promised: |slots| &mut slots.definitions,
        actual: |meta| &mut meta.definitions,
        ceiling: |limits| &mut limits.definitions,
    },
    Dimension {
        name: "evaluations",
        promised: |slots| &mut slots.evaluations,
        actual: |meta| &mut meta.evaluations,
        ceiling: |limits| &mut limits.evaluations,
    },
    Dimension {
        name: "responses",
        promised: |slots| &mut slots.responses,
        actual: |meta| &mut meta.responses,
        ceiling: |limits| &mut limits.responses,
    },
    Dimension {
        name: "receipts",
        promised: |slots| &mut slots.receipts,
        actual: |meta| &mut meta.receipts,
        ceiling: |limits| &mut limits.receipts,
    },
    Dimension {
        name: "result testaments",
        promised: |slots| &mut slots.result_testaments,
        actual: |meta| &mut meta.result_testaments,
        ceiling: |limits| &mut limits.claims,
    },
    Dimension {
        name: "monitors",
        promised: |slots| &mut slots.monitors,
        actual: |meta| &mut meta.monitors,
        ceiling: |limits| &mut limits.monitors,
    },
    Dimension {
        name: "monitor links",
        promised: |slots| &mut slots.monitor_links,
        actual: |meta| &mut meta.monitor_links,
        ceiling: |limits| &mut limits.monitor_links,
    },
];

#[test]
fn each_metadata_ceiling_protects_held_slots_and_restores_them_on_rollback() {
    let core = fixture::running(&[(ValidationMode::Observe, false)]);
    let view = View {
        state: &core.state,
        tail: None,
    };
    for dimension in &DIMENSIONS {
        let source = source();
        let (binding, envelope, schemas) = parts(&core, &source, 1);
        let mut per_report = envelope.remaining_slots(1, false).unwrap();
        *(dimension.promised)(&mut per_report) = 1;
        let envelope = envelope
            .with_slot_demand_for_test(per_report, None)
            .unwrap();
        let mut limits = core.limits;
        let mut meta = view.meta();
        let promised = usize::try_from(envelope.reports()).unwrap();
        assert_eq!(promised, 2);
        *(dimension.ceiling)(&mut limits) = *(dimension.actual)(&mut meta) + promised;
        let mut book = CompletionBook::new(&source, limits).unwrap();
        let begin = book
            .install_begin(
                fixture::key(1),
                binding,
                envelope,
                schemas,
                registration(&core, 1),
            )
            .unwrap();
        book.commit(begin).unwrap();
        let before = book.totals;
        let budget = source.stats();
        let pool = book.source().stats();
        let sequence = core.native_sequence();
        let rows = core.state.rows.len();
        book.check_slots(meta, sequence, rows).unwrap();

        let mut stolen = meta;
        *(dimension.actual)(&mut stolen) += 1;
        assert!(
            matches!(
                book.check_slots(stolen, sequence, rows),
                Err(NativeError::Capacity("preparation bytes"))
            ),
            "{} promised capacity",
            dimension.name
        );
        let mut overflow = meta;
        *(dimension.actual)(&mut overflow) = usize::MAX;
        assert!(
            book.check_slots(overflow, sequence, rows).is_err(),
            "{} overflow",
            dimension.name
        );
        assert_eq!(book.totals, before);
        assert_eq!(source.stats(), budget);
        assert_eq!(book.source().stats(), pool);

        // The shared journal retires all future slots after a terminal result.
        // Its rollback must restore every dimension, not just report/event counts.
        let retired = book
            .advance(
                fixture::key(1),
                binding,
                binding.next().unwrap(),
                true,
                CompletionUse::Regular,
            )
            .unwrap();
        assert_eq!(book.totals.slots, CompletionSlots::default());
        book.check_slots(stolen, sequence, rows).unwrap();
        book.rollback(retired).unwrap();
        assert_eq!(book.totals, before);
        assert_eq!(source.stats(), budget);
        assert_eq!(book.source().stats(), pool);
        assert!(
            book.check_slots(stolen, sequence, rows).is_err(),
            "{} rollback protection",
            dimension.name
        );
        book.check_slots(meta, sequence, rows).unwrap();
        drop(book);
        assert_eq!(source.stats().used, 0);
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
        book.check_slots(meta, SessionSeq(1), 1).unwrap();
        *(dimension.actual)(&mut meta) = 2;
        assert!(
            matches!(
                book.check_slots(meta, SessionSeq(1), 1),
                Err(NativeError::Capacity("preparation bytes"))
            ),
            "{} actual capacity",
            dimension.name
        );
        assert_eq!(source.stats(), before);
        assert_eq!(book.totals, Totals::default());
    }
}
