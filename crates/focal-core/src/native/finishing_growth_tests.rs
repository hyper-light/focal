//! What finishing a claim adds to its checkpoint, path by path (every terminal transition and
//! every release, with the row families each puts, replaces and deletes), against what the claim
//! reserved for it (`finishing`).
use super::*;
use focal_model::Cause;
use focal_model::lifecycle::creation::Owner;
use focal_model::lifecycle::succession::Lineage;
use std::collections::BTreeMap;

type Rows = BTreeMap<String, usize>;

fn snapshot(owner: &NativeOwner) -> Rows {
    let core = owner.committed_core();
    let mut rows = Rows::new();
    for entry in core.state.rows.entries() {
        let bytes = crate::native::record_codec::checkpoint::row_bytes(
            entry.key,
            entry.value,
            core.state.ledger,
            core.state.payloads(),
        )
        .unwrap();
        rows.insert(format!("{:?}", entry.key), bytes);
    }
    rows
}

fn family(key: &str) -> &str {
    key.split('(').next().unwrap_or(key)
}

fn diff(step: &str, before: &Rows, after: &Rows) {
    let rows = after.len() as isize - before.len() as isize;
    let bytes = after.values().sum::<usize>() as isize - before.values().sum::<usize>() as isize;
    eprintln!("{step}: {rows:+} rows, {bytes:+} B");
    for (key, bytes) in after {
        match before.get(key) {
            None => eprintln!("    + {} {bytes}", family(key)),
            Some(old) if old != bytes => eprintln!(
                "    ~ {} {old} -> {bytes} ({:+})",
                family(key),
                *bytes as isize - *old as isize
            ),
            Some(_) => {}
        }
    }
    for (key, bytes) in before {
        if !after.contains_key(key) {
            eprintln!("    - {} {bytes}", family(key));
        }
    }
}

/// The committed root with its reservations, after checking the running total of the
/// reservations against a walk of every row.
fn reserved(owner: &NativeOwner) -> (usize, usize) {
    let state = &owner.committed_core().state;
    assert_eq!(
        state.finishing,
        crate::native::finishing::of_rows(&state.rows).unwrap(),
        "the running reservation is not what the rows reserve"
    );
    owner.checkpoint_projection().unwrap().reserved()
}

/// Publishes `input`, printing what it changed; a step that finishes work must take no more
/// than the reservation it gives back.
fn step(owner: &mut NativeOwner, label: &str, input: NativeInput, time: u64, finishes: bool) {
    let before = snapshot(owner);
    let held = reserved(owner);
    let candidate = stage(owner, input, time);
    owner.publish_after_durable(candidate).unwrap();
    diff(label, &before, &snapshot(owner));
    let after = reserved(owner);
    eprintln!("    with reservations: {held:?} -> {after:?}");
    if finishes {
        assert!(
            after.0 <= held.0 && after.1 <= held.1,
            "{label} took more than it reserved: {held:?} -> {after:?}"
        );
    }
}

fn cancel_input(owner: &NativeOwner, claim: u128, id: u128) -> NativeInput {
    NativeInput {
        request: request(ISSUER, id),
        command: NativeCommand::Cancel {
            expected: owner
                .effective()
                .claim(ClaimId::from_u128(claim))
                .unwrap()
                .binding(),
        },
    }
}

fn release_input(owner: &NativeOwner, claim: u128, id: u128) -> NativeInput {
    NativeInput {
        request: request(ISSUER, id),
        command: NativeCommand::ReleaseScope {
            expected: owner
                .effective()
                .claim(ClaimId::from_u128(claim))
                .unwrap()
                .binding(),
        },
    }
}

/// A monitor on `claim` waiting for `other`'s satisfaction, due at 100.
fn register_satisfied(owner: &mut NativeOwner, claim: u128, monitor: u128, other: u128, time: u64) {
    let source = owner.effective().claim(ClaimId::from_u128(claim)).unwrap();
    let command = NativeCommand::RegisterMonitor {
        expected: source.binding(),
        receipt: source.receipt().map(|receipt| receipt.fence),
        id: MonitorId::from_u128(monitor),
        roots: vec![WaitPredicate::Satisfied(ClaimId::from_u128(other))],
        deadline: timer(claim, monitor, 100).deadline,
    };
    let candidate = stage(
        owner,
        NativeInput {
            request: request(ISSUER, 70_000 + u128::from(time)),
            command,
        },
        time,
    );
    owner.publish_after_durable(candidate).unwrap();
}

/// Fires `input`'s monitor deadline, printing what it changed: what it ends takes no more than
/// the reservation it gives back.
fn fire_and_check(owner: &mut NativeOwner, input: NativeMonitorDeadlineInput, label: &str) {
    let before = snapshot(owner);
    let held = reserved(owner);
    let (candidate, _) = monitor_fire(owner, input, 100);
    owner.publish_after_durable(candidate).unwrap();
    diff(label, &before, &snapshot(owner));
    let after = reserved(owner);
    eprintln!("    with reservations: {held:?} -> {after:?}");
    assert!(
        after.0 <= held.0 && after.1 <= held.1,
        "{label} took more than it reserved: {held:?} -> {after:?}"
    );
}

/// Every transition that ends a claim or releases one takes no more of the checkpoint's bound
/// than the reservation it gives back (rows and bytes alike), on every path: a cancel of a posted
/// claim, of one whose evaluation has begun or never began (fenced), of one others depend on (one
/// of them on two claims, each failed with it) and of an owned tree's root (every descendant ended
/// with it), a claim's deadline and a monitor's passing, a monitor cancelled so its claim can be
/// released, and every release, a tree's bottom up. So none is refused at the bound, whatever
/// order a ledger's clients finish in. The running reservation is, after every step, what a walk
/// of the rows gives.
#[test]
fn finishing_a_claim_takes_no_more_than_it_reserved() {
    eprintln!("== posted claims: plain, with evaluations begun and not, and dependencies");
    let posted = authored_posted(&[
        (1, 500, &[(ValidationMode::Observe, false)], &[]),
        (2, 500, &[], &[]),
        (3, 500, &[], &[(graph::Kind::DependsOn, 2)]),
        (4, 500, &[], &[]),
        (
            5,
            500,
            &[],
            &[(graph::Kind::DependsOn, 2), (graph::Kind::DependsOn, 4)],
        ),
        (6, 500, &[(ValidationMode::Required, false)], &[]),
        (8, 500, &[], &[(graph::Kind::Awaits, 2)]),
        (
            7,
            500,
            &[
                (ValidationMode::Required, false),
                (ValidationMode::Observe, false),
            ],
            &[],
        ),
    ]);
    let mut owner = NativeOwner::new(posted).unwrap();
    // Its evaluation begun first: the steps below move the logical time past the begin's.
    begin_claim(&mut owner, 1, 1, 61_001);
    begin_claim(&mut owner, 7, 2, 61_012);
    let next = cancel_input(&owner, 4, 61_002);
    step(
        &mut owner,
        "cancel a claim one with two dependencies depends on",
        next,
        40,
        true,
    );
    assert!(
        owner
            .effective()
            .claim(ClaimId::from_u128(5))
            .unwrap()
            .is_terminal()
    );
    for (claim, id) in [(4, 61_003), (5, 61_004)] {
        let next = release_input(&owner, claim, id);
        step(
            &mut owner,
            &format!("release claim {claim}"),
            next,
            41,
            true,
        );
    }
    let next = cancel_input(&owner, 1, 61_005);
    step(
        &mut owner,
        "cancel a claim with a begun evaluation",
        next,
        42,
        true,
    );
    let next = release_input(&owner, 1, 61_006);
    step(&mut owner, "release it", next, 43, true);
    let next = cancel_input(&owner, 6, 61_007);
    step(
        &mut owner,
        "cancel a claim whose evaluation never began",
        next,
        44,
        true,
    );
    let next = release_input(&owner, 6, 61_008);
    step(&mut owner, "release it", next, 45, true);
    let next = cancel_input(&owner, 7, 61_013);
    step(
        &mut owner,
        "cancel a claim with two evaluations, one begun",
        next,
        46,
        true,
    );
    let next = release_input(&owner, 7, 61_014);
    step(&mut owner, "release it", next, 46, true);
    let next = cancel_input(&owner, 2, 61_009);
    step(
        &mut owner,
        "cancel a claim another depends on",
        next,
        46,
        true,
    );
    assert!(
        owner
            .effective()
            .claim(ClaimId::from_u128(3))
            .unwrap()
            .is_terminal()
    );
    for (claim, id) in [(2, 61_010), (3, 61_011)] {
        let next = release_input(&owner, claim, id);
        step(
            &mut owner,
            &format!("release claim {claim}"),
            next,
            47,
            true,
        );
    }
    let awaiting = owner.effective().claim(ClaimId::from_u128(8)).unwrap();
    eprintln!("    (the awaiting claim is {:?})", awaiting.status());
    let next = if awaiting.is_terminal() {
        release_input(&owner, 8, 61_015)
    } else {
        cancel_input(&owner, 8, 61_016)
    };
    step(&mut owner, "finish the awaiting claim", next, 48, true);

    eprintln!("== a claim with a monitor");
    let posted = authored_posted(&[(1, 500, &[], &[]), (2, 500, &[], &[])]);
    let mut owner = NativeOwner::new(posted).unwrap();
    let before = snapshot(&owner);
    register_timer(&mut owner, timer(1, 1, 400), 2, 25);
    diff("register a monitor", &before, &snapshot(&owner));
    let next = cancel_input(&owner, 1, 62_001);
    step(&mut owner, "cancel the monitored claim", next, 40, true);
    // A claim with a monitor active is released once the monitor is cancelled (or fires).
    let next = NativeInput {
        request: request(ISSUER, 62_002),
        command: NativeCommand::CancelMonitor {
            expected: owner
                .effective()
                .claim(ClaimId::from_u128(1))
                .unwrap()
                .binding(),
            receipt: None,
            id: MonitorId::from_u128(1),
        },
    };
    step(&mut owner, "cancel its monitor", next, 41, true);
    let next = release_input(&owner, 1, 62_003);
    step(&mut owner, "release it", next, 42, true);

    eprintln!("== a claim whose monitor's deadline passes");
    let posted = authored_posted(&[(1, 500, &[], &[]), (2, 500, &[], &[])]);
    let mut owner = NativeOwner::new(posted).unwrap();
    let deadline = timer(1, 1, 100);
    register_timer(&mut owner, deadline, 2, 25);
    let before = snapshot(&owner);
    let held = reserved(&owner);
    let (candidate, _) = monitor_fire(&mut owner, deadline, 100);
    owner.publish_after_durable(candidate).unwrap();
    diff("its monitor fires", &before, &snapshot(&owner));
    let after = reserved(&owner);
    assert!(
        after.0 <= held.0 && after.1 <= held.1,
        "a monitor's expiry took more than it reserved: {held:?} -> {after:?}"
    );
    let next = NativeInput {
        request: request(ISSUER, 65_001),
        command: NativeCommand::CancelMonitor {
            expected: owner
                .effective()
                .claim(ClaimId::from_u128(1))
                .unwrap()
                .binding(),
            receipt: None,
            id: MonitorId::from_u128(1),
        },
    };
    step(&mut owner, "cancel the fired monitor", next, 101, true);
    let next = release_input(&owner, 1, 65_002);
    step(&mut owner, "release it", next, 102, true);

    eprintln!("== a monitor cycle: the newer claim's monitor fires, the older deadlocks");
    let posted = authored_posted(&[(1, 500, &[], &[]), (2, 500, &[], &[])]);
    let mut owner = NativeOwner::new(posted).unwrap();
    let earlier = timer(1, 1, 100);
    let trigger = timer(2, 2, 100);
    register_timer(&mut owner, earlier, 2, 25);
    register_timer(&mut owner, trigger, 1, 26);
    fire_and_check(&mut owner, trigger, "the trigger's monitor fires");
    assert!(
        owner
            .effective()
            .claim(ClaimId::from_u128(1))
            .unwrap()
            .is_terminal()
    );

    eprintln!("== a monitor cycle on satisfaction: the victim's own monitor fires");
    let posted = authored_posted(&[(1, 500, &[], &[]), (2, 500, &[], &[])]);
    let mut owner = NativeOwner::new(posted).unwrap();
    register_satisfied(&mut owner, 1, 1, 2, 25);
    register_satisfied(&mut owner, 2, 2, 1, 26);
    fire_and_check(
        &mut owner,
        timer(1, 1, 100),
        "the victim's own monitor fires",
    );
    for claim in [1, 2] {
        let state = owner.effective().claim(ClaimId::from_u128(claim)).unwrap();
        eprintln!(
            "    claim {claim}: {:?}, {} monitors active",
            state.status(),
            state.scopes().iter().filter(|scope| scope.active()).count()
        );
    }

    eprintln!("== a monitor cycle of three on satisfaction: the oldest's own monitor fires");
    let posted = authored_posted(&[(1, 500, &[], &[]), (2, 500, &[], &[]), (3, 500, &[], &[])]);
    let mut owner = NativeOwner::new(posted).unwrap();
    register_satisfied(&mut owner, 1, 1, 2, 25);
    register_satisfied(&mut owner, 2, 2, 3, 26);
    register_satisfied(&mut owner, 3, 3, 1, 27);
    fire_and_check(
        &mut owner,
        timer(1, 1, 100),
        "the oldest's own monitor fires",
    );

    eprintln!("== a claim whose deadline passes");
    let posted = authored_posted(&[(1, 50, &[], &[])]);
    let mut owner = NativeOwner::new(posted).unwrap();
    let before = snapshot(&owner);
    let held = reserved(&owner);
    let deadline = input(&owner, 1);
    let (candidate, _) = fire(&mut owner, deadline, 60);
    owner.publish_after_durable(candidate).unwrap();
    diff("expire it", &before, &snapshot(&owner));
    let after = reserved(&owner);
    assert!(
        after.0 <= held.0 && after.1 <= held.1,
        "an expiry took more than it reserved: {held:?} -> {after:?}"
    );
    let next = release_input(&owner, 1, 63_001);
    step(&mut owner, "release it", next, 61, true);

    eprintln!("== an owned tree of three");
    let mut tree = core();
    tree.limits.plan_edges = 65_536;
    let mut claims = Vec::new();
    let mut declarations = Vec::new();
    for id in 1..=3 {
        let NativeCommand::Create {
            claims: mut next,
            declarations: mut definitions,
        } = creation(id, id, &[], None).command
        else {
            panic!("create")
        };
        if id > 1 {
            let row = &mut next[0];
            row.definition.lineage = Lineage::new(
                row.definition.binding,
                Cause::Claim(ClaimId::from_u128(id - 1)),
                &[],
                0,
            )
            .unwrap();
            row.owner = Some(Owner {
                expected: binding(id - 1),
                receipt: None,
            });
        }
        claims.append(&mut next);
        declarations.append(&mut definitions);
    }
    publish(
        &mut tree,
        10,
        NativeInput {
            request: request(ISSUER, 1),
            command: NativeCommand::Create {
                claims,
                declarations,
            },
        },
    );
    let mut owner = NativeOwner::new(tree).unwrap();
    let next = cancel_input(&owner, 1, 64_001);
    step(&mut owner, "cancel the tree's root", next, 40, true);
    for (claim, id) in [(3, 64_002), (2, 64_003), (1, 64_004)] {
        let next = release_input(&owner, claim, id);
        step(
            &mut owner,
            &format!("release claim {claim}"),
            next,
            41,
            true,
        );
    }
}

/// A terminal claim's monitor still waiting is cancelled and the claim's scope released while
/// fresh work's lane is spent: each finishes work already taken, on the completion lane. A
/// monitor's cancel was fresh work's, and a scope releases only once its monitors are disposed
/// of, so with that lane spent a finished claim's family could never be released or retired.
#[test]
fn a_finished_claims_waiting_monitor_is_cancelled_and_its_scope_released_under_fresh_works_pressure()
 {
    let posted = authored_posted(&[(1, 500, &[], &[]), (2, 500, &[], &[])]);
    let mut owner = NativeOwner::new(posted).unwrap();
    // Claim 1 waits for claim 2, which never ends; then claim 1 is cancelled, its monitor active.
    register_timer(&mut owner, timer(1, 1, 400), 2, 25);
    let next = cancel_input(&owner, 1, 66_001);
    let candidate = stage(&mut owner, next, 40);
    owner.publish_after_durable(candidate).unwrap();
    let claim = owner.committed().claim(ClaimId::from_u128(1)).unwrap();
    assert!(claim.is_terminal());
    assert!(claim.scopes().iter().any(|scope| scope.active()));
    // Fresh work's lane spent.
    let source = owner.budget_for_test();
    let pressure = source
        .reserve(
            focal_memory::BudgetKind::Payload,
            focal_memory::BudgetLane::Ordinary,
            source.headroom(focal_memory::BudgetLane::Ordinary),
        )
        .unwrap()
        .commit();
    let expected = owner
        .effective()
        .claim(ClaimId::from_u128(1))
        .unwrap()
        .binding();
    let cancel_monitor = NativeInput {
        request: request(ISSUER, 66_002),
        command: NativeCommand::CancelMonitor {
            expected,
            receipt: None,
            id: MonitorId::from_u128(1),
        },
    };
    let cancelled = owner
        .prepare(context(ISSUER, 41), cancel_monitor, None)
        .unwrap_or_else(|error| panic!("the monitor's cancel was refused: {error:?}"));
    let NativeStaging::Prepared { candidate, .. } = cancelled else {
        panic!("a fresh cancel of the monitor");
    };
    owner.publish_after_durable(candidate).unwrap();
    let next = release_input(&owner, 1, 66_003);
    let released = owner
        .prepare(context(ISSUER, 42), next, None)
        .unwrap_or_else(|error| panic!("the release was refused: {error:?}"));
    let NativeStaging::Prepared { candidate, .. } = released else {
        panic!("a fresh release");
    };
    owner.publish_after_durable(candidate).unwrap();
    assert!(
        owner
            .committed()
            .claim(ClaimId::from_u128(1))
            .unwrap()
            .released()
    );
    drop(pressure);
}
