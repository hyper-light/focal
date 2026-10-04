#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
use super::{
    Guarantee, GuaranteeLevel,
    apply::{Current, Journal, Outcome, Phase, preflight, session_progress},
    plan::{
        Change, ControlProposal, DeploymentIdentity, DeploymentPlan, Observation, ObservedControl,
        ObservedNode, ObservedSession, Proposal, SessionEpochs, compose,
    },
};
use crate::config::{
    Durability, FailureDomain, Placement,
    policy::{CommittedPolicy, PolicyIntent, PolicyRevision},
};

fn intent(survive: FailureDomain, max_failures: u16) -> PolicyIntent {
    PolicyIntent {
        durability: Durability {
            survive,
            max_failures,
        },
        placement: Placement::default(),
    }
}
fn committed(revision: u64, intent: PolicyIntent) -> CommittedPolicy {
    CommittedPolicy {
        revision: PolicyRevision(revision),
        hash: intent.hash().unwrap(),
        intent,
    }
}
fn session(n: u8, achieved: Option<GuaranteeLevel>) -> ObservedSession {
    ObservedSession {
        tenant: [1; 16],
        session: [n; 16],
        epochs: SessionEpochs {
            route: 1,
            membership: 1,
            placement: 1,
        },
        voters: vec![7],
        desired: GuaranteeLevel::NONE,
        achieved,
        pending: None,
        blocked_by: Vec::new(),
    }
}
fn observation(sessions: Vec<ObservedSession>) -> Observation {
    Observation {
        deployment: DeploymentIdentity {
            cluster: [9; 16],
            node: 7,
        },
        committed: committed(1, intent(FailureDomain::Node, 0)),
        observed_at: 1_700_000_000,
        sessions,
        nodes: vec![ObservedNode {
            node: 7,
            generation: 1,
            alive: true,
            eligible: true,
            disk_available: Some(1 << 30),
            region: None,
            zone: None,
        }],
        control: None,
    }
}
const NODE_1: GuaranteeLevel = GuaranteeLevel {
    survive: FailureDomain::Node,
    max_failures: 1,
};

#[test]
fn a_plan_orders_the_policy_commit_first_and_derives_its_identity_from_facts_alone() {
    let observation = observation(vec![
        session(2, Some(GuaranteeLevel::NONE)),
        session(3, Some(GuaranteeLevel::NONE)),
    ]);
    let requested = intent(FailureDomain::Node, 1);
    let proposals = [
        Proposal::Planned {
            operation: [4; 16],
            voters: vec![7, 8, 9],
        },
        Proposal::Satisfied,
    ];
    let plan = compose(
        &observation,
        &requested,
        &proposals,
        &ControlProposal::Unobserved,
        10,
    )
    .unwrap();
    assert_eq!(plan.body.changes.len(), 3);
    assert!(matches!(
        plan.body.changes[0],
        Change::CommitPolicy {
            from_revision: 1,
            to_revision: 2
        }
    ));
    assert!(matches!(
        &plan.body.changes[1],
        Change::PlanSession {
            operation,
            voters,
            pending: false,
            ..
        } if *operation == [4; 16] && voters == &[7, 8, 9]
    ));
    assert!(matches!(plan.body.changes[2], Change::NoChange { .. }));
    assert_eq!(
        plan.body.guarantee,
        Guarantee {
            before: GuaranteeLevel::NONE,
            during: GuaranteeLevel::NONE,
            after: NODE_1
        }
    );
    assert!(plan.body.blocked.is_empty());
    assert!(!plan.is_empty());
    // Time is not identity: the same facts make the same plan.
    let mut later = observation.clone();
    later.observed_at += 60;
    let again = compose(
        &later,
        &requested,
        &proposals,
        &ControlProposal::Unobserved,
        99,
    )
    .unwrap();
    assert_eq!(again.plan_id, plan.plan_id);
    assert_ne!(again.created_ms, plan.created_ms);
    // A different request is a different plan.
    let other = compose(
        &observation,
        &intent(FailureDomain::Zone, 1),
        &proposals,
        &ControlProposal::Unobserved,
        10,
    )
    .unwrap();
    assert_ne!(other.plan_id, plan.plan_id);
    // The same policy with every session satisfied changes nothing.
    let same = compose(
        &observation,
        &intent(FailureDomain::Node, 0),
        &[Proposal::Satisfied, Proposal::Satisfied],
        &ControlProposal::Unobserved,
        10,
    )
    .unwrap();
    assert!(same.is_empty());
    assert_eq!(same.body.changes.len(), 2);
}

#[test]
fn a_refused_session_is_named_as_blocked_and_the_guarantee_after_stays_the_guarantee_before() {
    let observation = observation(vec![
        session(2, Some(GuaranteeLevel::NONE)),
        session(3, None),
    ]);
    let plan = compose(
        &observation,
        &intent(FailureDomain::Node, 1),
        &[
            Proposal::Refused("no placement".into()),
            Proposal::Pending {
                operation: [5; 16],
                voters: vec![7, 8, 9],
            },
        ],
        &ControlProposal::Unobserved,
        10,
    )
    .unwrap();
    assert_eq!(plan.body.blocked.len(), 1);
    assert_eq!(plan.body.blocked[0].session, [2; 16]);
    assert_eq!(plan.body.guarantee.after, GuaranteeLevel::NONE);
    assert!(matches!(
        plan.body.changes[1],
        Change::PlanSession { pending: true, .. }
    ));
    // Without sessions the committed policy is the guarantee before.
    let mut alone = observation.clone();
    alone.sessions.clear();
    alone.committed = committed(3, intent(FailureDomain::Zone, 1));
    let plan = compose(
        &alone,
        &intent(FailureDomain::Zone, 1),
        &[],
        &ControlProposal::Unobserved,
        10,
    )
    .unwrap();
    assert!(plan.body.changes.is_empty());
    assert_eq!(
        plan.body.guarantee.before,
        GuaranteeLevel {
            survive: FailureDomain::Zone,
            max_failures: 1
        }
    );
    assert!(
        compose(
            &alone,
            &intent(FailureDomain::Zone, 1),
            &[Proposal::Satisfied],
            &ControlProposal::Unobserved,
            10,
        )
        .is_err()
    );
}

#[test]
fn the_plan_artifact_round_trips_and_every_tamper_is_refused() {
    let observation = observation(vec![session(2, Some(GuaranteeLevel::NONE))]);
    let plan = compose(
        &observation,
        &intent(FailureDomain::Node, 1),
        &[Proposal::Planned {
            operation: [4; 16],
            voters: vec![7, 8, 9],
        }],
        &ControlProposal::Unobserved,
        10,
    )
    .unwrap();
    let bytes = plan.encode().unwrap();
    assert_eq!(&bytes[..8], b"FCLPLAN1");
    assert_eq!(DeploymentPlan::decode(&bytes).unwrap(), plan);
    for index in [0usize, 8, 20, bytes.len() / 2, bytes.len() - 1] {
        let mut tampered = bytes.clone();
        tampered[index] ^= 0x40;
        assert!(
            matches!(
                DeploymentPlan::decode(&tampered),
                Err(super::DeploymentError::Corrupt(_))
            ),
            "byte {index}"
        );
    }
    assert!(DeploymentPlan::decode(&bytes[..bytes.len() - 1]).is_err());
    assert!(DeploymentPlan::decode(&bytes[..20]).is_err());
    let mut extended = bytes.clone();
    extended.push(0);
    assert!(DeploymentPlan::decode(&extended).is_err());
    // A body whose identity was recomputed for other facts is not this plan.
    let mut other = plan.clone();
    other.body.deployment.node = 8;
    let forged = other.encode().unwrap();
    assert!(matches!(
        DeploymentPlan::decode(&forged),
        Err(super::DeploymentError::Corrupt(_))
    ));
    let view = plan.view();
    assert_eq!(view.kind, "deployment_plan");
    assert_eq!(view.changes.len(), 2);
    assert_eq!(view.guarantee.after.survive, "node");
    assert_eq!(view.guarantee.after.max_failures, 1);
}

#[test]
fn preflight_refuses_a_stale_plan_by_the_fact_that_moved_and_skips_committed_steps() {
    let observation = observation(vec![session(2, Some(GuaranteeLevel::NONE))]);
    let plan = compose(
        &observation,
        &intent(FailureDomain::Node, 1),
        &[Proposal::Planned {
            operation: [4; 16],
            voters: vec![7, 8, 9],
        }],
        &ControlProposal::Unobserved,
        10,
    )
    .unwrap();
    let journal = Journal::new(&plan, 5).unwrap();
    assert!(preflight(&plan, &journal, &Current::of(&observation)).is_ok());
    let mut moved = observation.clone();
    moved.committed.revision = PolicyRevision(2);
    assert!(matches!(
        preflight(&plan, &journal, &Current::of(&moved)),
        Err(super::DeploymentError::Stale { subject, field: "committed_revision" }) if subject == "policy"
    ));
    for (field, mutate) in [
        (
            "route_epoch",
            (|session: &mut ObservedSession| session.epochs.route = 2) as fn(&mut ObservedSession),
        ),
        ("membership_epoch", |session| session.epochs.membership = 3),
        ("placement_epoch", |session| session.epochs.placement = 2),
    ] {
        let mut moved = observation.clone();
        mutate(&mut moved.sessions[0]);
        match preflight(&plan, &journal, &Current::of(&moved)) {
            Err(super::DeploymentError::Stale { field: found, .. }) => assert_eq!(found, field),
            other => panic!("{field}: {other:?}"),
        }
    }
    let mut gone = observation.clone();
    gone.sessions.clear();
    assert!(matches!(
        preflight(&plan, &journal, &Current::of(&gone)),
        Err(super::DeploymentError::Stale {
            field: "presence",
            ..
        })
    ));
    // Steps already committed are not re-checked: the policy moved because
    // this plan moved it, and the session's epochs move as its plan runs.
    let mut resumed = journal.clone();
    resumed.record(0, Phase::Complete, None, 6).unwrap();
    resumed
        .record(1, Phase::Committed, Some([4; 16]), 7)
        .unwrap();
    let mut moved = observation.clone();
    moved.committed.revision = PolicyRevision(2);
    moved.sessions[0].epochs.placement = 2;
    assert!(preflight(&plan, &resumed, &Current::of(&moved)).is_ok());
    // A journal that found the plan stale never resumes it.
    let mut dead = journal.clone();
    dead.outcome = Outcome::Stale {
        step: 1,
        subject: "session".into(),
        field: "operation".into(),
    };
    assert!(matches!(
        preflight(&plan, &dead, &Current::of(&observation)),
        Err(super::DeploymentError::Stale {
            field: "operation",
            ..
        })
    ));
}

#[test]
fn the_journal_round_trips_and_only_advances() {
    let observation = observation(vec![session(2, None)]);
    let plan = compose(
        &observation,
        &intent(FailureDomain::Node, 0),
        &[Proposal::Satisfied],
        &ControlProposal::Unobserved,
        10,
    )
    .unwrap();
    let mut journal = Journal::new(&plan, 5).unwrap();
    journal.record(0, Phase::Prepared, None, 6).unwrap();
    journal
        .record(0, Phase::Committed, Some([4; 16]), 7)
        .unwrap();
    assert!(journal.record(0, Phase::Prepared, None, 8).is_err());
    journal.record(0, Phase::Committed, None, 9).unwrap();
    assert_eq!(journal.operation(0), Some([4; 16]));
    assert_eq!(journal.phase(0), Some(Phase::Committed));
    assert_eq!(journal.phase(1), None);
    let bytes = journal.encode().unwrap();
    assert_eq!(&bytes[..8], b"FCLAPLY1");
    assert_eq!(Journal::decode(&bytes).unwrap(), journal);
    assert!(Journal::decode(&bytes[..bytes.len() - 1]).is_err());
    assert!(Journal::decode(b"FCLPLAN1").is_err());
}

#[test]
fn session_progress_follows_the_directory() {
    let mut observed = session(2, Some(GuaranteeLevel::NONE));
    assert_eq!(session_progress(None, [4; 16], NODE_1, 1), Phase::Committed);
    assert_eq!(
        session_progress(Some(&observed), [4; 16], NODE_1, 1),
        Phase::Committed
    );
    observed.pending = Some([4; 16]);
    assert_eq!(
        session_progress(Some(&observed), [4; 16], NODE_1, 1),
        Phase::Verified
    );
    observed.pending = Some([5; 16]);
    assert_eq!(
        session_progress(Some(&observed), [4; 16], NODE_1, 1),
        Phase::Committed
    );
    observed.pending = None;
    observed.epochs.placement = 2;
    assert_eq!(
        session_progress(Some(&observed), [4; 16], NODE_1, 1),
        Phase::Verified,
        "moved on without the guarantee: under way, not complete"
    );
    observed.achieved = Some(NODE_1);
    assert_eq!(
        session_progress(Some(&observed), [4; 16], NODE_1, 1),
        Phase::Complete
    );
    observed.pending = Some([6; 16]);
    assert_eq!(
        session_progress(Some(&observed), [4; 16], NODE_1, 1),
        Phase::Verified,
        "a later plan under way keeps this one verified, not complete"
    );
    assert!(NODE_1.covers(GuaranteeLevel::NONE));
    assert!(!GuaranteeLevel::NONE.covers(NODE_1));
    let zone_0 = GuaranteeLevel {
        survive: FailureDomain::Zone,
        max_failures: 0,
    };
    assert_eq!(NODE_1.weaker(zone_0), zone_0);
    assert_eq!(zone_0.weaker(NODE_1), zone_0);
}

#[test]
fn the_partition_groups_are_planned_after_the_root_and_weaken_the_control_promise() {
    use super::plan::{ObservedPartition, PartitionProposal, compose_with_partitions};
    let mut observation = observation(vec![session(2, Some(GuaranteeLevel::NONE))]);
    observation.nodes.extend([8, 9].map(|node| ObservedNode {
        node,
        generation: 1,
        alive: true,
        eligible: true,
        disk_available: Some(1 << 30),
        region: None,
        zone: None,
    }));
    let partition = ObservedPartition {
        partition: [5; 16],
        group: [6; 16],
        voters: vec![7],
        learners: Vec::new(),
        tolerates_node: Some(0),
        tolerates_zone: None,
        tolerates_region: None,
        blocked_by: Vec::new(),
    };
    observation.control = Some(ObservedControl {
        voters: vec![7, 8, 9],
        learners: Vec::new(),
        configuration_index: 3,
        tolerates_node: Some(1),
        tolerates_zone: None,
        tolerates_region: None,
        blocked_by: Vec::new(),
        partitions: vec![partition.clone()],
    });
    // The control plane is as weak as its weakest group: a root that
    // tolerates one node and a partition that tolerates none promise none.
    assert_eq!(
        observation
            .control
            .as_ref()
            .unwrap()
            .level(FailureDomain::Node),
        GuaranteeLevel::NONE
    );
    let requested = intent(FailureDomain::Node, 1);
    let proposals = [Proposal::Planned {
        operation: [4; 16],
        voters: vec![7, 8, 9],
    }];
    let plan = compose_with_partitions(
        &observation,
        &requested,
        &proposals,
        &ControlProposal::Satisfied,
        &[PartitionProposal {
            partition: [5; 16],
            group: [6; 16],
            proposal: ControlProposal::Planned {
                voters: vec![7, 8, 9],
                configuration_index: 2,
            },
        }],
        10,
    )
    .unwrap();
    assert!(matches!(plan.body.changes[0], Change::CommitPolicy { .. }));
    assert!(matches!(
        &plan.body.changes[1],
        Change::PlanPartition { partition, group, voters, expected_configuration_index: 2 }
            if *partition == [5; 16] && *group == [6; 16] && voters == &[7, 8, 9]
    ));
    assert!(matches!(plan.body.changes[2], Change::PlanSession { .. }));
    assert_eq!(plan.body.control_guarantee.before, GuaranteeLevel::NONE);
    assert_eq!(plan.body.control_guarantee.after, NODE_1);
    assert!(plan.body.blocked_control.is_none());
    let bytes = plan.encode().unwrap();
    assert_eq!(DeploymentPlan::decode(&bytes).unwrap(), plan);
    // The plan is stale once the partition's group is another, or a
    // planned voter is gone; the group's moving index does not stale it.
    let journal = Journal::new(&plan, 5).unwrap();
    assert!(preflight(&plan, &journal, &Current::of(&observation)).is_ok());
    let mut regrouped = observation.clone();
    regrouped.control.as_mut().unwrap().partitions[0].group = [7; 16];
    assert!(matches!(
        preflight(&plan, &journal, &Current::of(&regrouped)),
        Err(super::DeploymentError::Stale { field, .. }) if field == "group"
    ));
    // A partition no set of nodes seats blocks the control plane's promise
    // as a refused root does, naming the partition.
    let refused = compose_with_partitions(
        &observation,
        &requested,
        &proposals,
        &ControlProposal::Satisfied,
        &[PartitionProposal {
            partition: [5; 16],
            group: [6; 16],
            proposal: ControlProposal::Refused("no nodes".into()),
        }],
        10,
    )
    .unwrap();
    assert_eq!(refused.body.control_guarantee.after, GuaranteeLevel::NONE);
    assert!(
        refused
            .body
            .blocked_control
            .as_deref()
            .is_some_and(|reason| reason.starts_with("partition 05050505")),
        "{:?}",
        refused.body.blocked_control
    );
}

#[test]
fn the_root_group_is_planned_before_the_sessions_and_its_promise_is_stated_apart() {
    let mut observation = observation(vec![session(2, Some(GuaranteeLevel::NONE))]);
    observation.nodes.extend([8, 9].map(|node| ObservedNode {
        node,
        generation: 1,
        alive: true,
        eligible: true,
        disk_available: Some(1 << 30),
        region: None,
        zone: None,
    }));
    observation.control = Some(ObservedControl {
        voters: vec![7],
        learners: vec![8, 9],
        configuration_index: 3,
        tolerates_node: Some(0),
        tolerates_zone: None,
        tolerates_region: None,
        blocked_by: Vec::new(),
        partitions: Vec::new(),
    });
    let requested = intent(FailureDomain::Node, 1);
    let proposals = [Proposal::Planned {
        operation: [4; 16],
        voters: vec![7, 8, 9],
    }];
    let plan = compose(
        &observation,
        &requested,
        &proposals,
        &ControlProposal::Planned {
            voters: vec![7, 8, 9],
            configuration_index: 3,
        },
        10,
    )
    .unwrap();
    assert!(matches!(plan.body.changes[0], Change::CommitPolicy { .. }));
    assert!(matches!(
        &plan.body.changes[1],
        Change::PlanRoot { voters, expected_configuration_index: 3 } if voters == &[7, 8, 9]
    ));
    assert!(matches!(plan.body.changes[2], Change::PlanSession { .. }));
    assert_eq!(plan.body.control_guarantee.before, GuaranteeLevel::NONE);
    assert_eq!(plan.body.control_guarantee.after, NODE_1);
    assert_eq!(plan.body.guarantee.after, NODE_1);
    assert!(plan.body.blocked_control.is_none());
    assert_eq!(plan.view().changes.len(), 3);
    // The artifact carries it, and the planned voters are fenced by the
    // nodes the directory knows, not by the root's moving index.
    let bytes = plan.encode().unwrap();
    assert_eq!(DeploymentPlan::decode(&bytes).unwrap(), plan);
    let journal = Journal::new(&plan, 5).unwrap();
    assert!(preflight(&plan, &journal, &Current::of(&observation)).is_ok());
    let mut moved = observation.clone();
    moved.control.as_mut().unwrap().configuration_index = 4;
    assert!(preflight(&plan, &journal, &Current::of(&moved)).is_ok());
    let mut gone = observation.clone();
    gone.nodes.retain(|node| node.node != 9);
    assert!(matches!(
        preflight(&plan, &journal, &Current::of(&gone)),
        Err(super::DeploymentError::Stale { subject, field: "members" }) if subject == "root"
    ));
    // A root that already tolerates the failures changes nothing of its
    // own; one that cannot be seated blocks the control promise and leaves
    // it where it was, while the data's plan stands.
    let same = compose(
        &observation,
        &requested,
        &proposals,
        &ControlProposal::Satisfied,
        10,
    )
    .unwrap();
    assert_eq!(same.body.changes.len(), 2);
    assert_eq!(same.body.control_guarantee.after, NODE_1);
    let refused = compose(
        &observation,
        &requested,
        &proposals,
        &ControlProposal::Refused("one zone".into()),
        10,
    )
    .unwrap();
    assert_eq!(refused.body.blocked_control.as_deref(), Some("one zone"));
    assert_eq!(refused.body.control_guarantee.after, GuaranteeLevel::NONE);
    assert_eq!(refused.body.guarantee.after, NODE_1);
    assert_ne!(refused.plan_id, plan.plan_id);
    // The root's level in a class a voter has no domain in is none.
    let root = observation.control.as_ref().unwrap();
    assert_eq!(root.level(FailureDomain::Zone).max_failures, 0);
    assert_eq!(root.level(FailureDomain::Node).max_failures, 0);
}
