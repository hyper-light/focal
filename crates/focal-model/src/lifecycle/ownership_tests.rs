use super::*;
use crate::lifecycle::{aggregation, claim, graph, succession};
use crate::{
    ClaimStatus, ContentHash, ObjectId, ObjectRevision, ParticipantId, ReceiptFence, ReceiptId,
    RootCommandId,
};

const ISSUER: ParticipantId = ParticipantId::from_u128(1);
const SUBJECT: ParticipantId = ParticipantId::from_u128(2);
struct View<'a> {
    rows: &'a [ClaimState],
    prefix: SessionSeq,
}
impl EffectiveClaims for View<'_> {
    fn ledger(&self) -> LedgerId {
        self.rows[0].binding().ledger
    }
    fn prefix(&self) -> SessionSeq {
        self.prefix
    }
    fn claim(&self, id: ClaimId) -> Option<&ClaimState> {
        self.rows.iter().find(|row| row.binding().object.0 == id.0)
    }
}
fn cut(sequence: u64) -> ClaimCut {
    ClaimCut {
        position: SessionSeq(sequence),
        cause: ContentHash([9; 32]),
    }
}
fn limits() -> Limits {
    Limits {
        nodes: 16,
        edge_visits: 32,
        bytes: 64 * 1024,
    }
}
fn definition(id: u128, issuer: ParticipantId) -> claim::ClaimDefinition {
    let mut value = claim::tests::definition(4);
    value.binding.object = ObjectId::from_u128(id);
    value.issuer = issuer;
    value.lineage = succession::Lineage::root(value.binding, RootCommandId::from_u128(id)).unwrap();
    value.acceptance = aggregation::acceptance_for(value.binding, issuer);
    value
}
fn root(id: u128) -> ClaimState {
    ClaimState::generate(Principal::Actor(ISSUER), definition(id, ISSUER)).unwrap()
}
fn child(parent: &mut ClaimState, id: u128, sequence: u64, issuer: ParticipantId) -> ClaimState {
    let mut value = definition(id, issuer);
    value.created = SessionSeq(sequence);
    value.lineage = succession::Lineage::new(
        value.binding,
        Cause::Claim(ClaimId(parent.binding().object.0)),
        &[],
        0,
    )
    .unwrap();
    parent
        .generate_child(
            &parent.binding(),
            Principal::Actor(issuer),
            parent.receipt().map(|value| value.fence),
            value,
            cut(sequence),
        )
        .unwrap()
}
fn receive(parent: &mut ClaimState) {
    parent
        .apply(
            &parent.binding(),
            Principal::Actor(ISSUER),
            claim::ClaimIntent::Post {
                standing: claim::PostingStanding {
                    binding: parent.binding(),
                    standing: claim::PredicateState::Passed,
                    target: claim::PredicateState::Passed,
                },
            },
        )
        .unwrap();
    let snapshot = graph::Snapshot::capture(
        &[parent],
        graph::Limits {
            nodes: 8,
            edges: 16,
            visits: 128,
        },
    )
    .unwrap();
    let start = snapshot.start(ClaimId(parent.binding().object.0)).unwrap();
    let aggregate = aggregation::ClaimAggregation::new(
        parent,
        aggregation::Limits {
            max_slots: 8,
            max_checks: 8,
            max_results: 16,
            max_updates: 16,
        },
    )
    .unwrap();
    parent
        .acquire_receipt(
            &parent.binding(),
            Principal::Actor(SUBJECT),
            ReceiptFence {
                receipt: ReceiptId::from_u128(11),
                epoch: 1,
            },
            &aggregate.admission(),
            &start,
            &[],
        )
        .unwrap();
}
fn view(rows: &[ClaimState]) -> View<'_> {
    View {
        rows,
        prefix: SessionSeq(10),
    }
}

#[test]
fn issuer_cancels_complete_owned_tree_through_terminal_different_issuer_child() {
    let mut parent = root(1);
    receive(&mut parent);
    let mut owned = child(&mut parent, 2, 3, SUBJECT);
    let grandchild = child(&mut owned, 3, 4, SUBJECT);
    owned
        .apply(
            &owned.binding(),
            Principal::Actor(SUBJECT),
            claim::ClaimIntent::Revoke { cut: cut(5) },
        )
        .unwrap();
    let other = root(9);
    let rows = vec![parent, owned, grandchild, other];
    let original = rows.clone();
    let source = view(&rows);
    let plan = CancellationPlan::prepare(
        &source,
        rows[0].binding(),
        Principal::Actor(ISSUER),
        cut(11),
        limits(),
    )
    .unwrap();
    assert_eq!(
        plan.transitions()
            .iter()
            .map(|token| token.binding().object)
            .collect::<Vec<_>>(),
        vec![
            ObjectId::from_u128(1),
            ObjectId::from_u128(2),
            ObjectId::from_u128(3)
        ]
    );
    assert_eq!(
        plan.transitions()
            .iter()
            .map(Cancellation::changes_state)
            .collect::<Vec<_>>(),
        vec![true, false, true]
    );
    plan.check(&source).unwrap();
    let mut next = rows.clone();
    for token in plan.transitions() {
        next.iter_mut()
            .find(|row| row.binding().object == token.binding().object)
            .unwrap()
            .apply_cancellation(token)
            .unwrap();
    }
    assert_eq!(next[0].status(), ClaimStatus::Cancelled);
    assert_eq!(next[2].status(), ClaimStatus::Cancelled);
    assert_eq!(next[1], original[1]);
    assert_eq!(next[3], original[3]);
    assert_eq!(
        next[2].terminal_cut(),
        Some(ClaimTerminalCut::Explicit(cut(11)))
    );
    for (before, after) in original.iter().zip(&next) {
        assert_eq!(before.scopes(), after.scopes());
    }
    assert_eq!(rows, original);
    assert!(plan.retained_bytes().unwrap() <= limits().bytes);
}

#[test]
fn terminal_root_and_existing_local_seal_are_preserved_while_descendants_close() {
    let mut parent = root(1);
    let mut owned = child(&mut parent, 2, 2, ISSUER);
    claim::tests::local_projection_for_graph(&mut owned);
    let sealed = owned.local_sealed_at();
    parent
        .apply(
            &parent.binding(),
            Principal::Actor(ISSUER),
            claim::ClaimIntent::Revoke { cut: cut(6) },
        )
        .unwrap();
    let rows = vec![parent, owned];
    let source = view(&rows);
    let plan = CancellationPlan::prepare(
        &source,
        rows[0].binding(),
        Principal::Actor(ISSUER),
        cut(11),
        limits(),
    )
    .unwrap();
    let mut next = rows.clone();
    next[0].apply_cancellation(&plan.transitions()[0]).unwrap();
    next[1].apply_cancellation(&plan.transitions()[1]).unwrap();
    assert_eq!(next[0], rows[0]);
    assert_eq!(next[1].local_sealed_at(), sealed);
    assert!(next[1].local_complete());
    assert_eq!(next[1].status(), ClaimStatus::Cancelled);
    assert!(!next[1].released());
}

#[test]
fn direct_parent_cancel_is_refused_and_all_plan_writers_and_cuts_are_checked() {
    let mut parent = root(1);
    let owned = child(&mut parent, 2, 2, ISSUER);
    let before = parent.clone();
    assert_eq!(
        parent.apply(
            &parent.binding(),
            Principal::Actor(ISSUER),
            claim::ClaimIntent::Cancel { cut: cut(11) }
        ),
        Err(ContractError::InvalidTransition)
    );
    assert_eq!(parent, before);
    let rows = vec![parent, owned];
    let source = view(&rows);
    for actor in [Principal::Node(ISSUER), Principal::Actor(SUBJECT)] {
        assert_eq!(
            CancellationPlan::prepare(&source, rows[0].binding(), actor, cut(11), limits())
                .unwrap_err(),
            ContractError::WrongActor
        );
    }
    for invalid in [cut(0), cut(10), cut(12)] {
        assert_eq!(
            CancellationPlan::prepare(
                &source,
                rows[0].binding(),
                Principal::Actor(ISSUER),
                invalid,
                limits()
            )
            .unwrap_err(),
            ContractError::InvalidCut
        );
    }
    assert_eq!(
        CancellationPlan::prepare(
            &source,
            rows[0].binding().next().unwrap(),
            Principal::Actor(ISSUER),
            cut(11),
            limits()
        )
        .unwrap_err(),
        ContractError::StaleRevision
    );
}

#[test]
fn missing_child_and_changed_content_cause_creation_or_registration_are_refused() {
    let mut parent = root(1);
    let owned = child(&mut parent, 2, 2, ISSUER);
    let root_binding = parent.binding();
    for (change, error) in [
        (0, ContractError::InvalidTarget),
        (1, ContractError::ContentConflict),
        (2, ContractError::InvalidTarget),
        (3, ContractError::InvalidCut),
        (4, ContractError::WrongLedger),
    ] {
        let mut rows = vec![parent.clone()];
        if change != 0 {
            let mut def = definition(2, ISSUER);
            def.created = SessionSeq(if change == 3 { 3 } else { 2 });
            if change == 1 {
                def.binding.content = ContentHash([4; 32]);
            }
            if change == 4 {
                def.binding.ledger.session = crate::SessionId::from_u128(99);
            }
            def.lineage = succession::Lineage::new(
                def.binding,
                if change == 2 {
                    Cause::Root(RootCommandId::from_u128(3))
                } else {
                    Cause::Claim(ClaimId::from_u128(1))
                },
                &[],
                0,
            )
            .unwrap();
            def.acceptance = aggregation::acceptance_for(def.binding, ISSUER);
            rows.push(ClaimState::generate_defined(Principal::Actor(ISSUER), def).unwrap());
        }
        let source = view(&rows);
        assert_eq!(
            CancellationPlan::prepare(
                &source,
                root_binding,
                Principal::Actor(ISSUER),
                cut(11),
                limits()
            )
            .unwrap_err(),
            error,
            "case {change}"
        );
        assert_eq!(rows[0], parent);
    }
    assert_eq!(owned.created(), SessionSeq(2));
}

#[test]
fn late_owned_child_and_same_binding_substitution_invalidate_private_tokens() {
    let mut parent = root(1);
    let owned = child(&mut parent, 2, 2, ISSUER);
    let rows = vec![parent, owned];
    let source = view(&rows);
    let plan = CancellationPlan::prepare(
        &source,
        rows[0].binding(),
        Principal::Actor(ISSUER),
        cut(11),
        limits(),
    )
    .unwrap();
    let mut changed = rows.clone();
    let new_child = child(&mut changed[0], 3, 3, ISSUER);
    changed.push(new_child);
    assert_eq!(
        plan.check(&view(&changed)),
        Err(ContractError::StaleRevision)
    );
    assert_eq!(
        plan.check(&View {
            rows: &rows,
            prefix: SessionSeq(11)
        }),
        Err(ContractError::StaleRevision)
    );
    let mut def = definition(1, ISSUER);
    def.binding = rows[0].binding();
    def.subject = ParticipantId::from_u128(88);
    def.lineage = succession::Lineage::root(def.binding, RootCommandId::from_u128(1)).unwrap();
    def.acceptance = aggregation::acceptance_for(def.binding, ISSUER);
    let mut substituted = ClaimState::generate_defined(Principal::Actor(ISSUER), def).unwrap();
    let before = substituted.clone();
    assert_eq!(
        substituted.apply_cancellation(&plan.transitions()[0]),
        Err(ContractError::ContentConflict)
    );
    assert_eq!(substituted, before);
    let mut next = rows[1].clone();
    next.apply_cancellation(&plan.transitions()[1]).unwrap();
    assert_eq!(
        next.apply_cancellation(&plan.transitions()[1]),
        Err(ContractError::StaleRevision)
    );
}

#[test]
fn node_edge_byte_and_revision_capacity_refuse_before_any_state_changes() {
    let mut parent = root(1);
    let owned = child(&mut parent, 2, 2, ISSUER);
    let rows = vec![parent, owned];
    let source = view(&rows);
    for limits in [
        Limits {
            nodes: 0,
            ..limits()
        },
        Limits {
            nodes: 1,
            ..limits()
        },
        Limits {
            edge_visits: 0,
            ..limits()
        },
        Limits {
            bytes: 1,
            ..limits()
        },
    ] {
        assert_eq!(
            CancellationPlan::prepare(
                &source,
                rows[0].binding(),
                Principal::Actor(ISSUER),
                cut(11),
                limits
            )
            .unwrap_err(),
            ContractError::Capacity
        );
    }
    let mut def = definition(8, ISSUER);
    def.binding.revision = ObjectRevision(u64::MAX);
    def.lineage = succession::Lineage::root(def.binding, RootCommandId::from_u128(8)).unwrap();
    def.acceptance = aggregation::acceptance_for(def.binding, ISSUER);
    let rows = vec![ClaimState::generate(Principal::Actor(ISSUER), def).unwrap()];
    let source = view(&rows);
    assert_eq!(
        CancellationPlan::prepare(
            &source,
            rows[0].binding(),
            Principal::Actor(ISSUER),
            cut(11),
            limits()
        )
        .unwrap_err(),
        ContractError::Capacity
    );
    assert_eq!(rows[0].status(), ClaimStatus::Generated);
    assert_eq!(rows[0].binding().revision, ObjectRevision(u64::MAX));
}
