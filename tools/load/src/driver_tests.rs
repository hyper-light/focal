use super::*;

const MEMORY: &str = "refused: NativeRefusal { kind: Capacity, detail: \"native owner memory: Capacity { requested: 2728, available: 2426 }\" }";
const MEMORY_AGAIN: &str = "refused: NativeRefusal { kind: Capacity, detail: \"native owner memory: Capacity { requested: 360, available: 140 }\" }";

/// Refusals that name their own figures are one reason: a full session's
/// refusals each named their request and its room, and every one took a
/// place of its own until the places ran out.
#[test]
fn refusals_of_one_kind_are_counted_together_whatever_their_figures() {
    let mut refusals = Vec::new();
    note(&mut refusals, MEMORY.into());
    note(&mut refusals, MEMORY_AGAIN.into());
    note(&mut refusals, "access: Capacity".into());
    assert_eq!(
        refusals,
        vec![
            (
                "refused: NativeRefusal { kind: Capacity, detail: \"native owner memory: Capacity { requested: #, available: # }\" }".to_owned(),
                2
            ),
            ("access: Capacity".to_owned(), 1),
        ]
    );
}

/// Past the kept reasons the rest are counted under one, so the reasons
/// always sum to the refusals they name: the comparison's reports at 1,000/s
/// named 5,300 of 24,983.
#[test]
fn reasons_past_the_kept_ones_are_counted_never_dropped() {
    let mut refusals = Vec::new();
    let mut noted = 0u64;
    for kind in ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"] {
        for _ in 0..3 {
            note(&mut refusals, format!("reason {kind}"));
            noted += 1;
        }
    }
    assert_eq!(refusals.len(), MAX_REFUSALS);
    assert_eq!(
        refusals.last().map(|(reason, _)| reason.as_str()),
        Some(OTHER_REASONS)
    );
    assert_eq!(refusals.iter().map(|(_, count)| count).sum::<u64>(), noted);
    // Workers' reasons merged into the run's, past its places as well.
    let mut run = Vec::new();
    for (reason, count) in &refusals {
        note_count(&mut run, reason.clone(), *count);
    }
    for (reason, count) in &refusals {
        note_count(&mut run, reason.clone(), *count);
    }
    assert_eq!(run.len(), MAX_REFUSALS);
    assert_eq!(run.iter().map(|(_, count)| count).sum::<u64>(), 2 * noted);
}

fn claim_read(id: u128) -> ResponseEnvelope {
    let ledger = LedgerId {
        tenant: focal_model::TenantId::from_u128(1),
        session: focal_model::SessionId::from_u128(1),
    };
    let claim = focal_wire::NativeClaim {
        binding: focal_model::native_event::NativeBinding {
            object: focal_model::ObjectId::from_u128(id),
            content: focal_model::ContentHash([1; 32]),
            revision: focal_model::ObjectRevision(1),
        },
        issuer: ParticipantId::from_u128(1),
        subject: ParticipantId::from_u128(2),
        created: SessionSeq(1),
        deadline: None,
        status: focal_model::ClaimStatus::Generated,
        origin: focal_wire::NativeClaimOrigin::Native,
        released: false,
        receipt: None,
        local_complete: false,
        local_sealed_at: None,
        terminal: None,
        latest_response: None,
        response_count: 0,
        max_responses: 1,
        obligations: Vec::new(),
        cause: focal_model::Cause::Root(RootCommandId::from_u128(1)),
        corrections: Vec::new(),
        acceptance: Vec::new(),
        scopes: None,
        content: None,
    };
    ResponseEnvelope {
        protocol: focal_wire::NATIVE_PROTOCOL_VERSION,
        ledger,
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(1),
        result: Response::NativeRead(focal_wire::NativeReadPage {
            token: ReadToken {
                ledger,
                sequence: SessionSeq(1),
                route_epoch: RouteEpoch(1),
            },
            native_sequence: SessionSeq(1),
            logical_time: 0,
            objects: vec![NativeObject::Claim(Box::new(claim))],
            next: None,
            visited: 1,
        }),
    }
}

/// A nonempty page can contain an explicit Missing marker or the wrong object. Neither proves
/// that a committed claim was read, even though the old presence-only hit counter accepted it.
#[test]
fn only_the_requested_claim_counts_as_a_read_hit() {
    assert!(read_hit(&Ok(claim_read(10)), 10));
    assert!(
        !read_hit(&Ok(claim_read(20)), 10),
        "a different claim is not a hit"
    );
    let mut envelope = claim_read(10);
    let Response::NativeRead(page) = &mut envelope.result else {
        panic!("fixture")
    };
    page.objects = vec![NativeObject::Missing(focal_wire::NativeObjectRef::Claim(
        ClaimId::from_u128(10),
    ))];
    assert!(!read_hit(&Ok(envelope.clone()), 10), "Missing is not a hit");
    let Response::NativeRead(page) = &mut envelope.result else {
        panic!("fixture")
    };
    page.objects.clear();
    assert!(!read_hit(&Ok(envelope), 10));
}

/// A run among enrolled agents takes each agent's own client data directory, in the agents'
/// order, and its one context's enrollment; an agent whose enrollment never finished is not one of
/// them, and fewer agents than asked is refused, naming how many were found.
#[test]
fn enrolled_agents_are_each_their_own_client_data_directory() {
    let directory = tempfile::tempdir().unwrap();
    let agents = directory.path().join("agents");
    for agent in [2u16, 0, 1] {
        let name = format!("agent-{agent:04}");
        std::fs::create_dir_all(
            agents
                .join(&name)
                .join("CLIENT.contexts")
                .join(format!("enrollment-{name}")),
        )
        .unwrap();
    }
    // Enrolling, not enrolled: a data directory with no context yet.
    std::fs::create_dir_all(agents.join("agent-0003")).unwrap();
    std::fs::write(agents.join("agent-notes"), b"not a directory").unwrap();
    let found = enrolled_agents(&agents, 3).unwrap();
    assert_eq!(
        found,
        (0..3)
            .map(|agent| {
                let name = format!("agent-{agent:04}");
                agents
                    .join(&name)
                    .join("CLIENT.contexts")
                    .join(format!("enrollment-{name}"))
            })
            .collect::<Vec<_>>()
    );
    let refused = enrolled_agents(&agents, 4).unwrap_err().to_string();
    assert!(
        refused.contains("holds 3 agent enrollments of the 4 asked"),
        "{refused}"
    );
}

/// A run among agents writes through each agent's journal, not the run issuer's: the floors
/// advanced and the requests refused by name are every principal's, summed (an agents' run
/// counted only its issuer's, which none of its writes went through, and reported none).
#[test]
fn every_principals_journal_is_counted() {
    let issuer = Generations::new();
    let agents = [Generations::new(), Generations::new()];
    agents[0]
        .learn(RequestEpoch(5), &[RequestEpoch(5)])
        .unwrap();
    agents[0].advanced(RequestEpoch(6)).unwrap();
    agents[1]
        .learn(RequestEpoch(2), &[RequestEpoch(2), RequestEpoch(3)])
        .unwrap();
    agents[1].learn(RequestEpoch(3), &[]).unwrap();
    assert_eq!(journal_counts([&issuer].into_iter()).unwrap(), (0, 0));
    assert_eq!(
        journal_counts([&issuer].into_iter().chain(agents.iter())).unwrap(),
        (1, 3)
    );
}

/// A lifecycle a refused write ended is counted under the cause its reply named, and every
/// cause's count adds up to the refused, ledger by ledger and in total.
#[test]
fn refused_lifecycles_are_counted_by_their_cause() {
    let mut ledger = LifecycleCounts::default();
    for cause in [
        RefusalCause::OwnerMemory,
        RefusalCause::OwnerMemory,
        RefusalCause::IngressCapacity,
        RefusalCause::Unavailable,
        RefusalCause::Other,
    ] {
        ledger.refused += 1;
        ledger.refused_by_cause.count(cause);
    }
    ledger.refused += 1;
    ledger.refused_by_cause.expired += 1;
    let causes = |counts: &LifecycleCounts| {
        let by = counts.refused_by_cause;
        by.owner_memory
            + by.owner_bound
            + by.ingress_capacity
            + by.unavailable
            + by.expired
            + by.other
    };
    assert_eq!(ledger.refused_by_cause.owner_memory, 2);
    assert_eq!(ledger.refused_by_cause.ingress_capacity, 1);
    assert_eq!(causes(&ledger), ledger.refused);
    let mut total = LifecycleCounts::default();
    total.add(&ledger);
    total.add(&ledger);
    assert_eq!(total.refused, 12);
    assert_eq!(total.refused_by_cause.owner_memory, 4);
    assert_eq!(causes(&total), total.refused);
}
