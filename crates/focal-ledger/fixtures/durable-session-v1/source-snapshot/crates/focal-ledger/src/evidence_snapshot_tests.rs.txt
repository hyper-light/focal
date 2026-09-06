#[test]
fn custody_export_requires_applied_placement_and_pins_only_its_checkpoint_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let mut session =
        Session::open(dir.path(), identity(), config(), SessionLimits::default()).unwrap();
    elect(&mut session);
    assert!(matches!(
        session.checkpoint_evidence(0, 30_000),
        Err(LedgerError::PlacementConflict)
    ));
    let placement = placement_request(&session, SessionFenceKind::Created, 1);
    session.propose_placement(&placement).unwrap();
    assert!(matches!(
        session.checkpoint_evidence(0, 30_000),
        Err(LedgerError::Capacity)
    ));
    session.poll().unwrap();
    let snapshot = session.checkpoint_evidence(0, 30_000).unwrap();
    assert_eq!(snapshot.prefix().sequence, SessionSeq(0));
    assert_eq!(snapshot.prefix().artifacts, 0);
    assert!(snapshot.prefix().index.0 > 0);
    assert_eq!(
        snapshot.prefix().term.0,
        session
            .consensus
            .published_term(snapshot.prefix().index.0)
            .unwrap()
    );
    assert_eq!(
        snapshot.prefix().checkpoint,
        ContentHash(*blake3::hash(snapshot.checkpoint()).as_bytes())
    );
    assert!(
        snapshot
            .artifact_after(None, snapshot.elapsed_clock().unwrap())
            .unwrap()
            .is_none()
    );
    session
        .submit_local(&input(
            1,
            Command::NegotiateEpoch {
                epoch: RequestEpoch(1),
            },
        ))
        .unwrap();
    assert!(session.sequence() > snapshot.prefix().sequence);
    assert!(
        snapshot
            .artifact_after(None, snapshot.elapsed_clock().unwrap())
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        snapshot.artifact_after(None, 30_000),
        Err(LedgerError::Graph(GraphError::Memory(
            MemoryError::LeaseExpired
        )))
    ));
}

#[test]
fn checkpoint_reserves_large_retained_placement_before_cloning_or_persistence() {
    let dir = tempfile::tempdir().unwrap();
    let mut session =
        Session::open(dir.path(), identity(), config(), SessionLimits::default()).unwrap();
    elect(&mut session);
    let mut created = placement_request(&session, SessionFenceKind::Created, 1);
    created.placement.policy.residency = (1u128..=127)
        .map(|id| focal_directory::RegionId(id.to_be_bytes()))
        .collect();
    created.placement.policy.home_regions = created.placement.policy.residency.clone();
    created.placement.placement.materializers = (1..=127).map(|id| (id, 1)).collect();
    created.placement.placement.content_copies = created.placement.placement.materializers.clone();
    session.propose_placement(&created).unwrap();
    session.poll().unwrap();
    let mut cutover = placement_request(&session, SessionFenceKind::Cutover, 2);
    cutover.placement = created.placement;
    session.propose_placement(&cutover).unwrap();
    session.poll().unwrap();
    let prefix = session.placement().unwrap();
    assert_eq!(session.sequence(), SessionSeq(0));
    let used = session.memory_stats().used;
    let pressure = session
        .budget
        .reserve(
            BudgetKind::Control,
            BudgetLane::Completion,
            session.memory_stats().limit - used - 512 * 1024,
        )
        .unwrap()
        .commit();
    // The serial Core is tiny. Its historical scratch estimate fit here, but
    // both complete placement versions must be admitted before constructing V4.
    assert!(matches!(
        session.checkpoint(),
        Err(LedgerError::Memory(MemoryError::Capacity { .. }))
    ));
    assert!(!session.failed);
    assert_eq!(session.placement(), Some(prefix));
    drop(pressure);
    let snapshot = session.checkpoint_evidence(0, 30_000).unwrap();
    assert_eq!(snapshot.prefix().route, RouteEpoch(2));
    assert_eq!(snapshot.prefix().artifacts, 0);
}

#[test]
fn snapshot_reader_clock_is_monotone_and_capture_ttl_cannot_be_restarted() {
    let dir = tempfile::tempdir().unwrap();
    let mut session =
        Session::open(dir.path(), identity(), config(), SessionLimits::default()).unwrap();
    elect(&mut session);
    session
        .propose_placement(&placement_request(&session, SessionFenceKind::Created, 1))
        .unwrap();
    session.poll().unwrap();
    let snapshot = session.checkpoint_evidence(1_000, 30_000).unwrap();
    let advanced = snapshot.elapsed_clock().unwrap() + 5_000;
    snapshot.artifact_after(None, advanced).unwrap();
    assert!(matches!(
        session.checkpoint_evidence(1_000, 30_000),
        Err(LedgerError::Graph(GraphError::Memory(
            MemoryError::ClockRegression {
                supplied: 1_000,
                ..
            }
        )))
    ));
    let mut next = session.checkpoint_evidence(advanced, 30_000).unwrap();
    assert_eq!(next.prefix().sequence, snapshot.prefix().sequence);
    // Deterministically age the private capture anchor instead of assuming a
    // complete checkpoint fsync will finish inside a tiny wall-clock timeout.
    next.captured_at = std::time::Instant::now()
        .checked_sub(std::time::Duration::from_secs(31))
        .unwrap();
    assert!(matches!(
        next.elapsed_clock(),
        Err(LedgerError::Graph(GraphError::Memory(
            MemoryError::LeaseExpired
        )))
    ));
}
