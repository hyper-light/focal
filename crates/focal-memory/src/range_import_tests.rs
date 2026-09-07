use super::*;
use std::cell::Cell;

const FIRST_BYTES: usize = 16 * 1024;

#[derive(Default)]
struct DropProbe {
    calls: Cell<usize>,
    pending_bytes: Cell<usize>,
    payload_capacity: Cell<usize>,
}

#[derive(Clone)]
struct Input<'a> {
    bytes: Vec<u8>,
    budget: &'a MemoryBudget,
    probe: Option<&'a DropProbe>,
}

impl Drop for Input<'_> {
    fn drop(&mut self) {
        if let Some(probe) = self.probe {
            // Vec destruction follows this callback. The actual payload is
            // still allocated while its importer allowance is observed.
            probe.calls.set(probe.calls.get() + 1);
            probe
                .pending_bytes
                .set(self.budget.stats().by_kind[BudgetKind::Pending as usize]);
            probe.payload_capacity.set(self.bytes.capacity());
        }
    }
}

fn config() -> RangeConfig {
    RangeConfig {
        page_entries: 4,
        max_batch_entries: 4,
        page_bytes: 64 * 1024,
        max_entry_bytes: size_of::<Entry<u64, Input<'_>>>() + 32 * 1024 + ALLOCATOR_OVERHEAD,
        ..RangeConfig::default()
    }
}

fn input<'a>(
    key: u64,
    bytes: usize,
    budget: &'a MemoryBudget,
    probe: Option<&'a DropProbe>,
) -> Entry<u64, Input<'a>> {
    let bytes = vec![37; bytes];
    let heap = bytes.capacity() + ALLOCATOR_OVERHEAD;
    Entry::new(
        key,
        Input {
            bytes,
            budget,
            probe,
        },
        heap,
    )
}

fn staging_bytes() -> usize {
    let config = config();
    2 * ALLOCATOR_OVERHEAD
        + config.page_entries.min(config.max_batch_entries)
            * (size_of::<Change<u64, Input<'_>>>() + size_of::<crate::Reservation>())
}

fn assert_retained_until_drop(probe: &DropProbe, heap: usize) {
    assert_eq!(probe.calls.get(), 1, "refusal must not clone staged rows");
    assert_eq!(probe.payload_capacity.get() + ALLOCATOR_OVERHEAD, heap);
    // The two input-vector allowances alone cannot satisfy the observation.
    assert!(staging_bytes() < heap);
    assert_eq!(probe.pending_bytes.get(), staging_bytes() + heap);
}

fn reject_later_input(next_key: u64, next_bytes: usize) -> MemoryError {
    let budget = MemoryBudget::new(1_000_000, 0).unwrap();
    let probe = DropProbe::default();
    let first = input(10, FIRST_BYTES, &budget, Some(&probe));
    let heap = first.heap_bytes;
    let second = input(next_key, next_bytes, &budget, None);
    // Both entries fit in one ordinary leaf by bytes and count. A failure on
    // the second entry therefore happens before the first chunk is published.
    assert!(
        page_charge::<u64, Input<'_>>(2, heap + second.heap_bytes).unwrap() <= config().page_bytes
    );
    let error =
        match RangeStore::from_entries(RangeId(711), 90, config(), budget.clone(), [first, second])
        {
            Err(error) => error,
            Ok(_) => panic!("invalid checkpoint input was accepted"),
        };
    assert_retained_until_drop(&probe, heap);
    let after = budget.stats();
    assert_eq!(after.used, 0);
    assert_eq!(after.ordinary_used, 0);
    assert!(after.by_kind.iter().all(|bytes| *bytes == 0));
    error
}

#[test]
fn duplicate_import_key_keeps_staged_payload_charged_until_its_destructor() {
    assert_eq!(
        reject_later_input(10, FIRST_BYTES),
        MemoryError::InvalidConfiguration("checkpoint entries must be strictly ordered")
    );
}

#[test]
fn descending_import_key_keeps_staged_payload_charged_until_its_destructor() {
    assert_eq!(
        reject_later_input(9, FIRST_BYTES),
        MemoryError::InvalidConfiguration("checkpoint entries must be strictly ordered")
    );
}

#[test]
fn oversized_later_import_keeps_staged_payload_charged_until_its_destructor() {
    assert_eq!(
        reject_later_input(20, 32 * 1024 + 1),
        MemoryError::ItemTooLarge {
            bytes: config().max_entry_bytes + 1,
            limit: config().max_entry_bytes,
        }
    );
}

#[test]
fn later_payload_admission_failure_keeps_prior_import_payload_charged_until_drop() {
    let budget = MemoryBudget::new(1_000_000, 0).unwrap();
    let probe = DropProbe::default();
    let first = input(10, FIRST_BYTES, &budget, Some(&probe));
    let first_heap = first.heap_bytes;
    let second = input(20, FIRST_BYTES, &budget, None);
    let second_heap = second.heap_bytes;
    let available =
        root_charge::<u64, Input<'_>>(0).unwrap() + staging_bytes() + first_heap + second_heap - 1;
    let pressure = budget
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Completion,
            budget.stats().limit - available,
        )
        .unwrap();
    let before = budget.stats();
    let error =
        match RangeStore::from_entries(RangeId(712), 91, config(), budget.clone(), [first, second])
        {
            Err(error) => error,
            Ok(_) => panic!("checkpoint import exceeded its available capacity"),
        };
    assert_eq!(
        error,
        MemoryError::Capacity {
            requested: second_heap,
            available: second_heap - 1,
        }
    );
    assert_retained_until_drop(&probe, first_heap);
    assert_eq!(budget.stats(), before);
    drop(pressure);
    let after = budget.stats();
    assert_eq!(after.used, 0);
    assert_eq!(after.ordinary_used, 0);
    assert!(after.by_kind.iter().all(|bytes| *bytes == 0));
}
