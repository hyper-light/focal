use super::*;
use crate::{Cause, ObjectId, ObjectKind, ObjectRef, ReceiptId, RootCommandId};

fn retained() -> ClaimState {
    let mut definition = crate::lifecycle::claim::tests::definition(4);
    let binding = definition.binding;
    definition.graph = graph::Declaration::new(
        &[
            graph::Obligation {
                kind: graph::Kind::DependsOn,
                target: ClaimId::from_u128(20),
            },
            graph::Obligation {
                kind: graph::Kind::Awaits,
                target: ClaimId::from_u128(21),
            },
        ],
        4,
    )
    .unwrap();
    definition.lineage = succession::Lineage::new(
        binding,
        Cause::Root(RootCommandId::from_u128(22)),
        &[succession::Correction {
            kind: succession::CorrectionKind::Supersedes,
            predecessor: ObjectRef {
                ledger: binding.ledger,
                id: ObjectId::from_u128(23),
                kind: ObjectKind::Claim,
            },
        }],
        4,
    )
    .unwrap();
    definition.acceptance =
        aggregation::AcceptancePolicy::memory_fixture(binding, definition.issuer);
    let mut state = ClaimState::generate(Principal::Actor(definition.issuer), definition).unwrap();
    // A retained-state fixture deliberately includes settled history and spare
    // capacity. Copying must preserve it without attempting current admission.
    state.scopes = scope::Registry::memory_fixture(binding);
    let fence = ReceiptFence {
        receipt: ReceiptId::from_u128(30),
        epoch: 7,
    };
    state.receipt = Some(ReceiptEntitlement {
        holder: state.subject,
        fence,
    });
    state.responses.reserve_exact(8);
    state.responses.push(ResponseRecord {
        link: ResponseLink {
            testament: TestamentId::from_u128(31),
            content: ContentHash([31; 32]),
            receipt: fence,
            cycle: 2,
            prior: Some(TestamentId::from_u128(29)),
        },
        stamp: ReportStamp::fixture(),
        posted: true,
        received: true,
    });
    state.status = ClaimStatus::Satisfied;
    state.local_complete = true;
    state.local_sealed_at = Some(SessionSeq(9));
    state.terminal_cut = Some(ClaimTerminalCut::Explicit(ClaimCut {
        position: SessionSeq(10),
        cause: ContentHash([10; 32]),
    }));
    state
}

#[test]
fn compact_claim_copy_keeps_all_owned_history_after_original_is_dropped() {
    let mut original = retained();
    let copied = original.try_copy(original.copy_charge().unwrap()).unwrap();
    assert_eq!(copied, original);
    assert_ne!(copied.responses.as_ptr(), original.responses.as_ptr());
    assert_ne!(
        copied.graph.obligations().as_ptr(),
        original.graph.obligations().as_ptr()
    );
    assert_ne!(
        copied.lineage.corrections().as_ptr(),
        original.lineage.corrections().as_ptr()
    );
    assert_ne!(
        copied.scopes.iter().next().unwrap().roots().as_ptr(),
        original.scopes.iter().next().unwrap().roots().as_ptr()
    );
    let fingerprint = copied.acceptance.intent_fingerprint();
    original.responses.clear();
    original.graph = graph::Declaration::empty();
    original.receipt = None;
    original.status = ClaimStatus::Generated;
    drop(original);
    assert_eq!(copied.status(), ClaimStatus::Satisfied);
    assert_eq!(copied.responses[0].link.cycle, 2);
    assert!(copied.responses[0].received);
    assert_eq!(copied.graph.obligations().len(), 2);
    assert_eq!(copied.lineage.corrections().len(), 1);
    assert_eq!(copied.scopes.children().len(), 1);
    assert!(copied.scopes.released());
    assert_eq!(copied.acceptance.intent_fingerprint(), fingerprint);
    assert_eq!(copied.receipt.unwrap().fence.epoch, 7);
    assert_eq!(copied.local_sealed_at, Some(SessionSeq(9)));
    assert!(copied.terminal_cut.is_some());
}

#[test]
fn claim_copy_preflights_nested_capacity_and_counts_allocator_buffers() {
    let original = retained();
    let charge = original.copy_charge().unwrap();
    bytes::fail_after(5, || {
        assert_eq!(original.try_copy(charge - 1), Err(ContractError::Capacity));
        assert_eq!(bytes::remaining_allocations(), Some(5));
    });
    // Graph, lineage, three acceptance buffers, three scope buffers, responses.
    assert_eq!(original.copy_heap_allocations().unwrap(), 9);
    assert_eq!(original.heap_allocations().unwrap(), 9);
    assert_eq!(
        charge,
        std::mem::size_of::<ClaimState>() + original.copy_heap_bytes().unwrap()
    );
    let copied = original.try_copy(charge).unwrap();
    assert_eq!(copied.heap_allocations().unwrap(), 9);
    assert_eq!(copied.retained_bytes().unwrap(), charge);
    assert!(copied.retained_heap_bytes().unwrap() < original.retained_heap_bytes().unwrap());
}

#[test]
fn every_nested_copy_allocation_failure_preserves_original_and_allows_retry() {
    let original = retained();
    let before = original.clone();
    let charge = original.copy_charge().unwrap();
    for after in 0..original.copy_heap_allocations().unwrap() {
        assert_eq!(
            bytes::fail_after(after, || original.try_copy(charge)),
            Err(ContractError::Capacity),
            "allocation {after}"
        );
        assert_eq!(original, before);
        assert_eq!(original.try_copy(charge).unwrap(), before);
    }
}
