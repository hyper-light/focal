use super::*;
use crate::lifecycle::claim::{ClaimCut, ClaimTerminalCut};

fn expire(claim: &mut ClaimState) -> ClaimCut {
    let deadline = claim.deadline().unwrap();
    let cut = ClaimCut {
        position: SessionSeq(10),
        cause: ContentHash([97; 32]),
    };
    claim
        .expire(&claim.binding(), deadline, deadline.at, cut)
        .unwrap();
    assert_eq!(claim.terminal_cut(), Some(ClaimTerminalCut::Explicit(cut)));
    cut
}

#[test]
fn actual_expiry_fences_ready_and_retrying_checks_without_result_or_state_replacement() {
    let defs = definitions(ValidationMode::Required, true, false);
    let mut claim = posted(&defs);
    let ready = ready(&claim, &defs[1]);
    let begun = begin(&claim, ready);
    let retry = report(&claim, begun, VerdictValue::Error).next.into_state();
    let ready = ready.into_state();
    let cut = expire(&mut claim);
    for source in [ready, retry] {
        let fenced = source.expire_claim(&defs[1], &claim).unwrap();
        assert_eq!(fenced.binding(), source.binding().next().unwrap());
        assert_eq!(fenced.state(), source.state());
        assert_eq!(fenced.has_begun(), source.has_begun());
        assert_eq!(fenced.last_result(), source.last_result());
        assert_eq!(fenced.target(), source.target());
        assert_eq!(fenced.receipt(), source.receipt());
        assert_eq!(
            fenced.fence(),
            Some(AuthorityFence {
                reason: FenceReason::Expiry,
                cause: cut.cause
            })
        );
        assert!(fenced.bind(&defs[1]).unwrap().audit_finished());
        assert_eq!(fenced.expire_claim(&defs[1], &claim).unwrap(), fenced);
    }
}

#[test]
fn expiry_preserves_quality_phase_proof_and_existing_terminal_or_deadline_history() {
    let defs = definitions(ValidationMode::Required, true, false);
    let mut claim = posted(&defs);
    let quality = report(
        &claim,
        begin(&claim, ready(&claim, &defs[1])),
        VerdictValue::Pass,
    )
    .next;
    assert_eq!(quality.state(), State::ValidatingQualityBar);
    let passed = report(&claim, quality, VerdictValue::Pass)
        .next
        .into_state();
    let prior_fenced = quality
        .fence_deadline(
            &quality.binding(),
            &claim,
            quality.deadline(),
            quality.deadline().at,
            ClaimCut {
                position: SessionSeq(8),
                cause: ContentHash([96; 32]),
            },
        )
        .unwrap();
    let quality = quality.into_state();
    expire(&mut claim);
    let next = quality.expire_claim(&defs[1], &claim).unwrap();
    assert_eq!(next.state(), State::ValidatingQualityBar);
    assert_eq!(next.last_result(), quality.last_result());
    assert_eq!(
        next.last_result().unwrap().programmatic_evidence(),
        quality.last_result().unwrap().programmatic_evidence()
    );
    assert_eq!(passed.expire_claim(&defs[1], &claim).unwrap(), passed);
    assert_eq!(
        prior_fenced.expire_claim(&defs[1], &claim).unwrap(),
        prior_fenced
    );
}

#[test]
fn other_claim_controls_and_substituted_policy_cannot_supply_expiry_authority() {
    let defs = definitions(ValidationMode::Required, false, false);
    let mut claim = posted(&defs);
    let evaluation = begin(&claim, ready(&claim, &defs[1])).into_state();
    assert!(evaluation.expire_claim(&defs[1], &claim).is_err());
    let mut cancelled = posted(&defs);
    cancelled
        .apply(
            &cancelled.binding(),
            Principal::Actor(ISSUER),
            claim::ClaimIntent::Cancel {
                cut: ClaimCut {
                    position: SessionSeq(5),
                    cause: ContentHash([95; 32]),
                },
            },
        )
        .unwrap();
    assert!(evaluation.expire_claim(&defs[1], &cancelled).is_err());
    let other = definitions(ValidationMode::Required, true, false);
    let mut alternate = posted(&other);
    expire(&mut alternate);
    assert!(evaluation.expire_claim(&defs[1], &alternate).is_err());
    expire(&mut claim);
    assert!(evaluation.expire_claim(&other[1], &claim).is_err());
    assert!(evaluation.expire_claim(&defs[1], &claim).is_ok());
}

#[test]
fn checked_monitor_expiry_fences_a_claim_with_no_authored_claim_deadline() {
    let defs = definitions(ValidationMode::Observe, false, false);
    let mut claim = ClaimState::generate(
        Principal::Actor(ISSUER),
        claim::ClaimDefinition {
            binding: binding(200),
            issuer: ISSUER,
            subject: SUBJECT,
            deadline: None,
            max_responses: 4,
            created: SessionSeq(1),
            graph: graph::Declaration::empty(),
            lineage: succession::Lineage::root(binding(200), RootCommandId::from_u128(1)).unwrap(),
            acceptance: aggregation::AcceptancePolicy::new(
                binding(200),
                ISSUER,
                &[],
                &defs,
                limits(),
            )
            .unwrap(),
            scope_limits: scope::ScopeLimits {
                scopes: 2,
                roots: 4,
                children: 4,
            },
        },
    )
    .unwrap();
    claim
        .post_owned(Principal::Actor(ISSUER), claim.binding())
        .unwrap();
    let evaluation = begin(&claim, ready(&claim, &defs[1])).into_state();
    let target_definition = crate::lifecycle::claim::tests::definition(4);
    let target = ClaimState::generate(
        Principal::Actor(target_definition.issuer),
        target_definition,
    )
    .unwrap();
    let graph_limits = graph::Limits {
        nodes: 4,
        edges: 8,
        visits: 4096,
    };
    let captured = graph::Snapshot::capture(&[&target, &claim], graph_limits).unwrap();
    let id = crate::MonitorId::from_u128(4);
    let deadline = Deadline {
        timer: crate::TimerId::from_u128(900),
        generation: 1,
        at: 100,
    };
    let cut = ClaimCut {
        position: SessionSeq(3),
        cause: ContentHash([90; 32]),
    };
    let peers = [&target];
    let transition = scope::Registry::prepare_register_bounded(
        &claim,
        scope::Authority {
            principal: Principal::Actor(ISSUER),
            expected: claim.binding(),
            receipt: None,
            cut,
            now: 3,
        },
        scope::Registration {
            id,
            roots: &[crate::WaitPredicate::Terminal(ClaimId(
                target.binding().object.0,
            ))],
            deadline,
        },
        &captured,
        &peers,
        scope::BuildLimits {
            bytes: usize::MAX,
            visits: 4096,
        },
    )
    .unwrap()
    .build()
    .unwrap();
    claim
        .apply_scope(&claim.binding(), transition, &peers)
        .unwrap();
    let captured = graph::Snapshot::capture(&[&target, &claim], graph_limits).unwrap();
    let expiry_cut = ClaimCut {
        position: SessionSeq(4),
        cause: ContentHash([91; 32]),
    };
    let scope::MonitorDeadlineDecision::Expire(expiry) = scope::Registry::prepare_monitor_deadline(
        &claim,
        scope::MonitorDeadlineRequest {
            id,
            deadline,
            fired_at: 100,
        },
        &captured,
        &peers,
        expiry_cut,
        scope::BuildLimits {
            bytes: usize::MAX,
            visits: 4096,
        },
    )
    .unwrap()
    .resolve()
    .unwrap() else {
        panic!("negative SCC expiry")
    };
    let mut expired = claim.clone();
    expired
        .expire_monitor(&expired.binding(), &expiry, &peers)
        .unwrap();
    let fenced = evaluation.expire_claim(&defs[1], &expired).unwrap();
    assert_eq!(expired.deadline(), None);
    assert_eq!(
        fenced.fence(),
        Some(AuthorityFence {
            reason: FenceReason::Expiry,
            cause: expiry_cut.cause
        })
    );
    assert_eq!(fenced.last_result(), evaluation.last_result());
    assert_eq!(fenced.state(), evaluation.state());
    assert_eq!(fenced.expire_claim(&defs[1], &expired).unwrap(), fenced);
    assert!(evaluation.expire_claim(&defs[1], &claim).is_err());
}
