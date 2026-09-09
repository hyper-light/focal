use super::*;
use crate::lifecycle::{aggregation, claim, graph, succession};
use crate::{
    ContentHash, ObjectRef, ObjectRevision, ParticipantId, ReceiptId, RootCommandId, SessionId,
    TenantId,
};

const ISSUER: ParticipantId = ParticipantId::from_u128(1);
const RESPONDENT: ParticipantId = ParticipantId::from_u128(2);

#[derive(Debug, PartialEq, Eq)]
struct View {
    rows: Vec<ClaimState>,
    prefix: SessionSeq,
    /// The authored follow-up policy every parent presents in this fixture.
    escalation: crate::Escalation,
    evaluators: Vec<ParticipantId>,
}
impl EffectiveClaims for View {
    fn ledger(&self) -> LedgerId {
        ledger()
    }
    fn prefix(&self) -> SessionSeq {
        self.prefix
    }
    fn claim(&self, id: ClaimId) -> Option<&ClaimState> {
        self.rows.iter().find(|row| row.binding().object.0 == id.0)
    }
    fn cause_escalation(&self, _: ClaimId) -> crate::Escalation {
        self.escalation
    }
    fn is_designated_evaluator(&self, _: ClaimId, actor: ParticipantId) -> bool {
        self.evaluators.contains(&actor)
    }
}
impl View {
    fn empty() -> Self {
        Self {
            rows: Vec::new(),
            prefix: SessionSeq(0),
            escalation: crate::Escalation::Holder,
            evaluators: Vec::new(),
        }
    }
    fn install(&mut self, plan: CreationPlan) {
        self.prefix = plan.cut().position;
        for row in plan.into_rows() {
            if let Some(old) = self
                .rows
                .iter_mut()
                .find(|old| old.binding().object == row.binding().object)
            {
                *old = row;
            } else {
                self.rows.push(row);
            }
        }
        self.rows.sort_unstable_by_key(|row| row.binding().object);
    }
    fn get(&self, id: u128) -> &ClaimState {
        self.claim(ClaimId::from_u128(id)).unwrap()
    }
}
fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(1),
        session: SessionId::from_u128(2),
    }
}
fn binding(id: u128) -> Binding {
    Binding {
        ledger: ledger(),
        object: ObjectId::from_u128(id),
        content: ContentHash([7; 32]),
        revision: ObjectRevision(1),
    }
}
fn limits() -> Limits {
    Limits {
        nodes: 32,
        edge_visits: 128,
        bytes: 4 * 1024 * 1024,
    }
}
fn cut(sequence: u64) -> ClaimCut {
    ClaimCut {
        position: SessionSeq(sequence),
        cause: ContentHash([8; 32]),
    }
}
fn proposal(id: u128, sequence: u64) -> Proposal {
    let binding = binding(id);
    Proposal {
        definition: ClaimDefinition {
            binding,
            issuer: ISSUER,
            subject: RESPONDENT,
            deadline: None,
            max_responses: 4,
            created: SessionSeq(sequence),
            graph: graph::Declaration::empty(),
            lineage: Lineage::root(binding, RootCommandId::from_u128(1)).unwrap(),
            acceptance: aggregation::acceptance_for(binding, ISSUER),
            scope_limits: scope::ScopeLimits {
                scopes: 4,
                roots: 8,
                children: 8,
            },
        },
        owner: None,
    }
}
fn correction(mut proposal: Proposal, predecessor: u128, kind: CorrectionKind) -> Proposal {
    proposal.definition.lineage = Lineage::new(
        proposal.definition.binding,
        proposal.definition.lineage.cause().clone(),
        &[succession::Correction {
            kind,
            predecessor: ObjectRef::claim(ledger(), ClaimId::from_u128(predecessor)),
        }],
        4,
    )
    .unwrap();
    proposal
}
fn child(
    id: u128,
    sequence: u64,
    parent: Binding,
    receipt: Option<ReceiptFence>,
    issuer: ParticipantId,
) -> Proposal {
    let mut proposal = proposal(id, sequence);
    proposal.definition.issuer = issuer;
    proposal.definition.acceptance =
        aggregation::acceptance_for(proposal.definition.binding, issuer);
    proposal.definition.lineage = Lineage::new(
        proposal.definition.binding,
        Cause::Claim(ClaimId(parent.object.0)),
        &[],
        0,
    )
    .unwrap();
    proposal.owner = Some(Owner {
        expected: parent,
        receipt,
    });
    proposal
}
fn prepare(view: &View, proposals: Vec<Proposal>) -> Result<CreationPlan, ContractError> {
    CreationPlan::prepare(
        Principal::Actor(ISSUER),
        proposals,
        view,
        cut(view.prefix.0 + 1),
        limits(),
    )
}
fn with_root() -> View {
    let mut view = View::empty();
    view.install(prepare(&view, vec![proposal(1, 1)]).unwrap());
    view
}

#[test]
fn root_creation_has_exact_absence_and_no_publication_side_effect() {
    let view = View::empty();
    let plan = prepare(&view, vec![proposal(1, 1)]).unwrap();
    assert!(view.rows.is_empty());
    assert_eq!(plan.absent(), &[ClaimId::from_u128(1)]);
    assert!(plan.reads().is_empty());
    assert_eq!(plan.rows()[0].status(), crate::ClaimStatus::Generated);
    assert_eq!(plan.rows()[0].created(), SessionSeq(1));
}

#[test]
fn duplicate_existing_and_pending_ids_are_refused() {
    let empty = View::empty();
    assert!(matches!(
        prepare(&empty, vec![proposal(1, 1), proposal(1, 1)]),
        Err(ContractError::InvalidTarget)
    ));
    let view = with_root();
    assert!(matches!(
        prepare(&view, vec![proposal(1, 2)]),
        Err(ContractError::InvalidTarget)
    ));
    // The same interface represents an unpublished effective predecessor.
    let pending = prepare(&view, vec![proposal(2, 2)]).unwrap();
    let pending_view = View {
        rows: pending.into_rows(),
        prefix: SessionSeq(2),
        escalation: crate::Escalation::Holder,
        evaluators: Vec::new(),
    };
    assert!(matches!(
        prepare(&pending_view, vec![proposal(2, 3)]),
        Err(ContractError::InvalidTarget)
    ));
}

#[test]
fn every_creation_mode_resolves_direct_endpoints() {
    let view = View::empty();
    for kind in [CorrectionKind::Amends, CorrectionKind::Supersedes] {
        assert!(matches!(
            prepare(&view, vec![correction(proposal(1, 1), 9, kind)]),
            Err(ContractError::InvalidTarget)
        ));
    }
    assert!(matches!(
        prepare(&view, vec![child(1, 1, binding(9), None, ISSUER)]),
        Err(ContractError::InvalidTarget)
    ));
}

#[test]
fn missing_transitive_endpoint_is_not_treated_as_a_root() {
    let old = correction(proposal(1, 1), 9, CorrectionKind::Amends);
    let view = View {
        rows: vec![ClaimState::generate(Principal::Actor(ISSUER), old.definition).unwrap()],
        prefix: SessionSeq(1),
        escalation: crate::Escalation::Holder,
        evaluators: Vec::new(),
    };
    assert!(matches!(
        prepare(
            &view,
            vec![correction(proposal(2, 2), 1, CorrectionKind::Amends)]
        ),
        Err(ContractError::InvalidTarget)
    ));
}

#[test]
fn dfs_checks_disconnected_batch_components_and_rejects_cycles() {
    let view = View::empty();
    let proposals = vec![
        proposal(1, 1),
        correction(proposal(2, 1), 3, CorrectionKind::Amends),
        correction(proposal(3, 1), 2, CorrectionKind::Amends),
    ];
    assert!(matches!(
        prepare(&view, proposals),
        Err(ContractError::InvalidTarget)
    ));
}

#[test]
fn same_batch_ancestry_registers_all_children_even_in_reverse_id_order() {
    let view = View::empty();
    let plan = prepare(
        &view,
        vec![
            child(1, 1, binding(2), None, ISSUER),
            child(2, 1, binding(3), None, ISSUER),
            proposal(3, 1),
        ],
    )
    .unwrap();
    assert!(view.rows.is_empty());
    assert_eq!(plan.rows().len(), 3);
    assert_eq!(
        plan.rows()[1].scopes().children()[0].id(),
        ClaimId::from_u128(1)
    );
    assert_eq!(
        plan.rows()[2].scopes().children()[0].id(),
        ClaimId::from_u128(2)
    );
    assert_eq!(plan.rows()[1].binding().revision, ObjectRevision(2));
    assert_eq!(plan.rows()[2].binding().revision, ObjectRevision(2));
}

#[test]
fn historical_same_publication_ancestry_remains_valid() {
    let mut view = View::empty();
    view.install(
        prepare(
            &view,
            vec![
                proposal(1, 1),
                correction(proposal(2, 1), 1, CorrectionKind::Amends),
            ],
        )
        .unwrap(),
    );
    let plan = prepare(
        &view,
        vec![correction(proposal(3, 2), 2, CorrectionKind::Amends)],
    )
    .unwrap();
    assert_eq!(
        plan.reads(),
        &[view.get(1).binding(), view.get(2).binding()]
    );
}

#[test]
fn historical_edge_chronology_is_checked_at_its_actual_referrer() {
    let one = ClaimState::generate(
        Principal::Actor(ISSUER),
        correction(proposal(1, 1), 2, CorrectionKind::Amends).definition,
    )
    .unwrap();
    let two = ClaimState::generate(Principal::Actor(ISSUER), proposal(2, 2).definition).unwrap();
    let view = View {
        rows: vec![one, two],
        prefix: SessionSeq(2),
        escalation: crate::Escalation::Holder,
        evaluators: Vec::new(),
    };
    assert!(matches!(
        prepare(
            &view,
            vec![correction(proposal(3, 3), 1, CorrectionKind::Amends)]
        ),
        Err(ContractError::InvalidCut)
    ));
}

#[test]
fn unregistered_historical_cause_is_refused() {
    let parent = ClaimState::generate(Principal::Actor(ISSUER), proposal(1, 1).definition).unwrap();
    let orphan = ClaimState::generate_defined(
        Principal::Actor(ISSUER),
        child(2, 2, parent.binding(), None, ISSUER).definition,
    )
    .unwrap();
    let view = View {
        rows: vec![parent, orphan],
        prefix: SessionSeq(2),
        escalation: crate::Escalation::Holder,
        evaluators: Vec::new(),
    };
    assert!(matches!(
        prepare(
            &view,
            vec![correction(proposal(3, 3), 2, CorrectionKind::Amends)]
        ),
        Err(ContractError::InvalidManifest)
    ));
}

#[test]
fn child_fences_and_root_owner_field_are_mandatory() {
    let view = with_root();
    let mut omitted = child(2, 2, view.get(1).binding(), None, ISSUER);
    omitted.owner = None;
    assert!(matches!(
        prepare(&view, vec![omitted]),
        Err(ContractError::InvalidTarget)
    ));
    let mut stale = view.get(1).binding();
    stale.revision.0 += 1;
    assert!(matches!(
        prepare(&view, vec![child(2, 2, stale, None, ISSUER)]),
        Err(ContractError::StaleRevision)
    ));
    let mut root = proposal(2, 2);
    root.owner = Some(Owner {
        expected: view.get(1).binding(),
        receipt: None,
    });
    assert!(matches!(
        prepare(&view, vec![root]),
        Err(ContractError::InvalidTarget)
    ));
}

fn receive(parent: &mut ClaimState) -> ReceiptFence {
    let expected = parent.binding();
    parent
        .apply(
            &expected,
            Principal::Actor(ISSUER),
            claim::ClaimIntent::Post {
                standing: claim::PostingStanding {
                    binding: expected,
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
            edges: 8,
            visits: 32,
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
    let fence = ReceiptFence {
        receipt: ReceiptId::from_u128(9),
        epoch: 1,
    };
    parent
        .acquire_receipt(
            &parent.binding(),
            Principal::Actor(RESPONDENT),
            fence,
            &aggregate.admission(),
            &start,
            &[],
        )
        .unwrap();
    fence
}

#[test]
fn current_receipt_holder_can_create_owned_child_without_issuer_impersonation() {
    let mut view = with_root();
    let fence = receive(&mut view.rows[0]);
    let expected = view.get(1).binding();
    assert!(matches!(
        prepare(&view, vec![child(2, 2, expected, None, ISSUER)]),
        Err(ContractError::StaleReceipt)
    ));
    let old = ReceiptFence { epoch: 2, ..fence };
    assert!(matches!(
        prepare(&view, vec![child(2, 2, expected, Some(old), ISSUER)]),
        Err(ContractError::StaleReceipt)
    ));
    let plan = CreationPlan::prepare(
        Principal::Actor(RESPONDENT),
        vec![child(2, 2, expected, Some(fence), RESPONDENT)],
        &view,
        cut(2),
        limits(),
    )
    .unwrap();
    assert_eq!(plan.rows()[1].issuer(), RESPONDENT);
    assert_eq!(
        plan.rows()[0].scopes().children()[0].id(),
        ClaimId::from_u128(2)
    );
    assert!(view.get(1).scopes().children().is_empty());
}

#[test]
fn all_supersession_consequences_are_prepared_and_terminal_facts_preserved() {
    let mut view = View::empty();
    view.install(prepare(&view, vec![proposal(1, 1), proposal(2, 1)]).unwrap());
    let row = &mut view.rows[1];
    row.apply(
        &row.binding(),
        Principal::Actor(ISSUER),
        claim::ClaimIntent::Cancel { cut: cut(1) },
    )
    .unwrap();
    let original_terminal = view.get(2).clone();
    let mut successor = correction(proposal(3, 2), 1, CorrectionKind::Supersedes);
    successor.definition.lineage = Lineage::new(
        successor.definition.binding,
        Cause::Root(RootCommandId::from_u128(1)),
        &[
            succession::Correction {
                kind: CorrectionKind::Supersedes,
                predecessor: ObjectRef::claim(ledger(), ClaimId::from_u128(1)),
            },
            succession::Correction {
                kind: CorrectionKind::Supersedes,
                predecessor: ObjectRef::claim(ledger(), ClaimId::from_u128(2)),
            },
        ],
        2,
    )
    .unwrap();
    let plan = prepare(&view, vec![successor]).unwrap();
    assert_eq!(plan.rows().len(), 2);
    assert_eq!(plan.rows()[0].status(), crate::ClaimStatus::Superseded);
    assert_eq!(view.get(1).status(), crate::ClaimStatus::Generated);
    assert_eq!(view.get(2), &original_terminal);
    assert_eq!(plan.reads().len(), 2);
}

#[test]
fn invalid_later_supersession_leaves_all_earlier_private_changes_unpublished() {
    let view = with_root();
    let original = view.get(1).clone();
    let mut wrong = correction(proposal(3, 2), 1, CorrectionKind::Supersedes);
    wrong.definition.subject = ParticipantId::from_u128(99);
    let batch = vec![child(2, 2, view.get(1).binding(), None, ISSUER), wrong];
    assert!(matches!(
        prepare(&view, batch),
        Err(ContractError::InvalidTarget)
    ));
    assert_eq!(view.get(1), &original);
}

#[test]
fn generated_cut_revision_and_effective_prefix_are_not_caller_choices() {
    let view = View::empty();
    for (revision, sequence) in [(0, 1), (2, 1), (1, 0), (1, 2)] {
        let mut p = proposal(1, sequence);
        p.definition.binding.revision = ObjectRevision(revision);
        assert!(matches!(
            prepare(&view, vec![p]),
            Err(ContractError::InvalidCut)
        ));
    }
    assert!(matches!(
        CreationPlan::prepare(
            Principal::Actor(ISSUER),
            vec![proposal(1, 1)],
            &view,
            cut(2),
            limits()
        ),
        Err(ContractError::InvalidCut)
    ));
}

#[test]
fn node_edge_byte_and_allocator_failures_leave_effective_rows_unchanged() {
    let view = with_root();
    let original = view.get(1).clone();
    for limited in [
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
        assert!(matches!(
            CreationPlan::prepare(
                Principal::Actor(ISSUER),
                vec![child(2, 2, view.get(1).binding(), None, ISSUER)],
                &view,
                cut(2),
                limited
            ),
            Err(ContractError::Capacity)
        ));
        assert_eq!(view.get(1), &original);
    }
    let p = child(2, 2, view.get(1).binding(), None, ISSUER);
    assert!(matches!(
        bytes::fail_after(0, || prepare(&view, vec![p])),
        Err(ContractError::Capacity)
    ));
    assert_eq!(view.get(1), &original);
}

#[test]
fn fingerprint_covers_semantics_and_owner_fences_but_not_assigned_metadata() {
    let original = intent_fingerprint(&[proposal(1, 1)]).unwrap();
    let mut assigned = proposal(1, 9);
    assigned.definition.binding.revision = ObjectRevision(22);
    assert_eq!(original, intent_fingerprint(&[assigned]).unwrap());
    let mut changes = Vec::new();
    let mut p = proposal(1, 1);
    p.definition.subject = ParticipantId::from_u128(3);
    changes.push(p);
    let mut p = proposal(1, 1);
    p.definition.max_responses = 5;
    changes.push(p);
    let mut p = proposal(1, 1);
    p.definition.scope_limits.children = 7;
    changes.push(p);
    let mut p = proposal(1, 1);
    p.definition.binding.content = ContentHash([8; 32]);
    changes.push(p);
    let mut p = proposal(1, 1);
    p.definition.lineage = Lineage::root(
        Binding {
            content: ContentHash([9; 32]),
            ..p.definition.binding
        },
        RootCommandId::from_u128(1),
    )
    .unwrap();
    changes.push(p);
    let mut p = proposal(1, 1);
    p.definition.graph = graph::Declaration::new(
        &[graph::Obligation {
            kind: graph::Kind::Awaits,
            target: ClaimId::from_u128(2),
        }],
        1,
    )
    .unwrap();
    changes.push(p);
    changes.push(correction(proposal(1, 1), 2, CorrectionKind::Amends));
    for changed in changes {
        assert_ne!(original, intent_fingerprint(&[changed]).unwrap());
    }
    let one = child(2, 2, binding(1), None, ISSUER);
    let mut two = child(2, 2, binding(1), None, ISSUER);
    two.owner.as_mut().unwrap().expected.revision.0 = 2;
    assert_ne!(
        intent_fingerprint(&[one]).unwrap(),
        intent_fingerprint(&[two]).unwrap()
    );
}

fn original_allowance(proposals: &[Proposal], capacity: usize, limits: Limits) -> usize {
    let mut charge = scratch_bytes(limits).unwrap() + vector_charge::<Proposal>(capacity).unwrap();
    for proposal in proposals {
        charge += definition_heap(&proposal.definition).unwrap();
    }
    charge
}

#[test]
fn actual_capacity_overage_in_every_scratch_buffer_is_refused_without_publication() {
    let view = View::empty();
    // Rows, specs, absent IDs, DFS nodes, sorted index, stack and pinned reads.
    // Grow an actual allocation, preserving its contents and logical limit.
    for allocation in 0..7 {
        let proposals = vec![proposal(1, 1)];
        let mut limit = limits();
        limit.bytes = original_allowance(&proposals, proposals.capacity(), limit);
        let result = with_reservation_overage(allocation, 1, || {
            CreationPlan::prepare(Principal::Actor(ISSUER), proposals, &view, cut(1), limit)
        });
        assert!(
            matches!(result, Err(ContractError::Capacity)),
            "allocation {allocation}"
        );
        assert_eq!(view, View::empty());
    }
}

#[test]
fn capacity_overage_can_use_real_precharged_headroom() {
    let view = View::empty();
    let proposals = vec![proposal(1, 1)];
    let mut limit = limits();
    limit.bytes = original_allowance(&proposals, proposals.capacity(), limit)
        + std::mem::size_of::<ClaimState>();
    let plan = with_reservation_overage(0, 1, || {
        CreationPlan::prepare(Principal::Actor(ISSUER), proposals, &view, cut(1), limit)
    })
    .unwrap();
    assert_eq!(plan.rows.len(), 1);
    assert_eq!(plan.rows.capacity(), limit.nodes + 1);
    assert!(view.rows.is_empty());
}

#[test]
fn actual_scope_transition_capacity_is_checked_before_registry_install() {
    let view = with_root();
    let original = view.get(1).clone();
    let proposals = vec![child(2, 2, original.binding(), None, ISSUER)];
    let mut limit = limits();
    limit.bytes = original_allowance(&proposals, proposals.capacity(), limit)
        + original.copy_heap_bytes().unwrap()
        + original.copy_heap_allocations().unwrap() * ALLOCATION
        + scope_copy_charge(&original).unwrap();
    let ordinary = CreationPlan::prepare(
        Principal::Actor(ISSUER),
        vec![child(2, 2, original.binding(), None, ISSUER)],
        &view,
        cut(2),
        limit,
    )
    .unwrap();
    assert_eq!(ordinary.rows()[0].scopes().children().len(), 1);
    drop(ordinary);
    let result = with_reservation_overage(7, 16, || {
        CreationPlan::prepare(Principal::Actor(ISSUER), proposals, &view, cut(2), limit)
    });
    assert!(matches!(result, Err(ContractError::Capacity)));
    assert_eq!(view.get(1), &original);
    assert!(view.get(1).scopes().children().is_empty());
    assert!(view.claim(ClaimId::from_u128(2)).is_none());
}

#[test]
fn authored_escalation_decides_who_besides_the_issuer_may_cite_a_parent() {
    const EVALUATOR: ParticipantId = ParticipantId::from_u128(3);
    let mut view = with_root();
    let fence = receive(&mut view.rows[0]);
    let expected = view.get(1).binding();
    let attempt = |view: &View, actor: ParticipantId| {
        CreationPlan::prepare(
            Principal::Actor(actor),
            vec![child(2, 2, expected, Some(fence), actor)],
            view,
            cut(2),
            limits(),
        )
        .map(|plan| plan.rows()[1].issuer())
    };
    // Follow-ups reserved to the issuer: the current holder is refused.
    view.escalation = crate::Escalation::None;
    assert!(matches!(
        attempt(&view, RESPONDENT),
        Err(ContractError::WrongActor)
    ));
    assert_eq!(attempt(&view, ISSUER).unwrap(), ISSUER);
    // Escalation to evaluators admits a designated evaluator and nobody else.
    view.escalation = crate::Escalation::Evaluator;
    assert!(matches!(
        attempt(&view, EVALUATOR),
        Err(ContractError::WrongActor)
    ));
    view.evaluators.push(EVALUATOR);
    assert_eq!(attempt(&view, EVALUATOR).unwrap(), EVALUATOR);
    assert_eq!(attempt(&view, RESPONDENT).unwrap(), RESPONDENT);
    assert!(matches!(
        attempt(&view, ParticipantId::from_u128(9)),
        Err(ContractError::WrongActor)
    ));
    // A node principal never authors a child, whatever the policy says.
    assert!(matches!(
        CreationPlan::prepare(
            Principal::Node(EVALUATOR),
            vec![child(2, 2, expected, Some(fence), EVALUATOR)],
            &view,
            cut(2),
            limits(),
        ),
        Err(ContractError::WrongActor)
    ));
}
