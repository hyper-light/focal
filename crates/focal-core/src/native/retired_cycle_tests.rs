use super::*;
use crate::native::retired_cycles;

fn adoption(rows: &Rows, receipt: u128) -> NativeCommand {
    NativeCommand::AdoptReceipt {
        expected: rows.claim(),
        previous: rows.parent().receipt,
        receipt: ReceiptId::from_u128(receipt),
        holder: SUBJECT,
    }
}

fn diagnostic(rows: &mut Rows, id: u128) {
    let artifact = rows.artifact(
        id,
        WorkRole::Diagnostic {
            reason: EvidenceFailure::Work,
        },
        SUBJECT,
    );
    rows.send(
        SUBJECT,
        NativeCommand::SubmitDiagnostic {
            claim: rows.claim(),
            reason: EvidenceFailure::Work,
            artifact,
        },
    );
}

#[test]
fn actual_adoptions_preserve_unclosed_work_across_reused_slots_and_closed_lineage() {
    let mut rows = Rows::new(2);
    let first = rows.output(1401, 0);
    let old_receipt = rows.parent().receipt;
    diagnostic(&mut rows, 1402);
    rows.send(ISSUER, adoption(&rows, 1501));
    let second = rows.output(1403, 0);
    assert_eq!(rows.parent().next_cycle, 1);
    assert_eq!(
        rows.collect(),
        vec![
            (second.id, WorkArtifactState::Generated, 1),
            (first.id, WorkArtifactState::Generated, 1),
        ]
    );
    rows.close(
        1601,
        vec![evidence::SlotBinding {
            slot: 0,
            artifact: second,
        }],
        vec![],
    );
    let third = rows.output(1404, 1);
    rows.send(ISSUER, adoption(&rows, 1502));
    assert_eq!(
        rows.collect(),
        vec![
            (third.id, WorkArtifactState::Generated, 2),
            (first.id, WorkArtifactState::Generated, 1),
            (second.id, WorkArtifactState::Attached, 1),
        ]
    );
    let old = rows.core.native_work(first.id).unwrap();
    assert_eq!(old.state.receipt(), old_receipt);
    assert!(old.state.attachment().is_none());
    let head = retired_cycles::head(&rows.view(), ClaimId::from_u128(1), rows.core.limits).unwrap();
    assert_eq!((head.count, head.work_count), (2, 2));
    // An empty entitlement does not manufacture a retired cycle.
    rows.send(ISSUER, adoption(&rows, 1503));
    let after =
        retired_cycles::head(&rows.view(), ClaimId::from_u128(1), rows.core.limits).unwrap();
    assert_eq!(
        (after.head, after.count, after.work_count),
        (head.head, 2, 2)
    );
}

#[test]
fn pending_adoption_exposes_retired_work_and_drop_preserves_original_prefix() {
    let mut rows = Rows::new(2);
    let original = rows.output(1411, 0);
    let before = rows.core.native_sequence();
    let statistics = rows.core.native_budget();
    let candidate = rows.prepare(ISSUER, adoption(&rows, 1511));
    let view = View {
        state: &rows.core.state,
        tail: Some(&candidate),
    };
    let works =
        crate::native::projection_work::works(&view, ClaimId::from_u128(1), rows.core.limits)
            .map(|work| work.unwrap().reference())
            .collect::<Vec<_>>();
    assert_eq!(works, vec![original]);
    assert_eq!(
        retired_cycles::head(&view, ClaimId::from_u128(1), rows.core.limits)
            .unwrap()
            .count,
        1
    );
    assert_eq!(
        retired_cycles::head(&rows.view(), ClaimId::from_u128(1), rows.core.limits)
            .unwrap()
            .count,
        0
    );
    drop(candidate);
    assert_eq!(rows.core.native_sequence(), before);
    assert_eq!(rows.core.native_budget(), statistics);
    assert_eq!(
        rows.collect(),
        vec![(original.id, WorkArtifactState::Generated, 1)]
    );
}

#[test]
fn diagnostic_only_retired_cycles_consume_quote_and_exact_chain_budget() {
    use focal_model::lifecycle::aggregation::ProjectionShape;
    let mut rows = Rows::new(2);
    diagnostic(&mut rows, 1421);
    rows.send(ISSUER, adoption(&rows, 1521));
    diagnostic(&mut rows, 1422);
    rows.send(ISSUER, adoption(&rows, 1522));
    assert!(rows.collect().is_empty());
    let head = retired_cycles::head(&rows.view(), ClaimId::from_u128(1), rows.core.limits).unwrap();
    assert_eq!((head.count, head.work_count), (2, 0));
    let claim = rows.core.native_claim(ClaimId::from_u128(1)).unwrap();
    let shape = ProjectionShape {
        responses: 2,
        works: 4,
        evaluations: 4,
    };
    let limits = NativeLimits {
        plan_edges: 16_384,
        ..rows.core.limits
    };
    let plain = NativeProjectionQuote::derive(limits, claim, shape, 1).unwrap();
    let quoted = NativeProjectionQuote::derive_with_retired(limits, claim, shape, 1, 2).unwrap();
    assert_eq!(quoted.model(), plain.model());
    assert!(quoted.lookup_visits() > plain.lookup_visits());
    assert!(measured_projection(&rows, quoted) <= quoted.lookup_visits());
    rows.replace(
        Key::RetiredCycleHead(ClaimId::from_u128(1)),
        Row::RetiredCycleHead(RetiredCycleHead {
            work_count: 1,
            ..head
        }),
        0,
    );
    let view = rows.view();
    let mut cursor =
        crate::native::projection_work::works(&view, ClaimId::from_u128(1), rows.core.limits);
    assert!(cursor.next().unwrap().is_err());
    assert!(cursor.next().is_none());
}

#[test]
fn repeated_retired_link_refuses_once_without_hiding_the_tail() {
    let mut rows = Rows::new(2);
    rows.output(1431, 0);
    rows.send(ISSUER, adoption(&rows, 1531));
    let head = retired_cycles::head(&rows.view(), ClaimId::from_u128(1), rows.core.limits).unwrap();
    let key = head.head.unwrap();
    rows.replace(
        Key::RetiredCycle(key),
        Row::RetiredCycle(RetiredCycle {
            holder: SUBJECT,
            next: Some(key),
        }),
        0,
    );
    let view = rows.view();
    let mut cursor =
        crate::native::projection_work::works(&view, ClaimId::from_u128(1), rows.core.limits);
    assert!(cursor.next().unwrap().is_err());
    assert!(cursor.next().is_none());
}
