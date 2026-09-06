use super::*;
use focal_consensus::StateRole;

struct Group {
    root: tempfile::TempDir,
    nodes: Vec<Option<Session>>,
    isolated: Option<usize>,
}
impl Group {
    fn config(index: usize) -> NodeConfig {
        let mut config = NodeConfig::single(index as u64 + 1, [11; 16], ledger().session.0);
        config.voters = vec![1, 2, 3];
        config
    }
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let nodes = (0..3)
            .map(|index| {
                Some(
                    Session::open(
                        root.path().join(index.to_string()),
                        ledger(),
                        Self::config(index),
                        SessionLimits::default(),
                    )
                    .unwrap(),
                )
            })
            .collect();
        let mut group = Self {
            root,
            nodes,
            isolated: None,
        };
        group.node(0).campaign().unwrap();
        for _ in 0..30 {
            group.pump();
        }
        assert!(group.node(0).is_authoritative());
        let mut commands = Vec::new();
        populate(
            vec![handler(1, false)],
            None,
            Some(ArtifactPayload::Inline(PASS.to_vec())),
            None,
            |id, actor, command| commands.push(input(id, actor, command)),
        );
        for command in commands {
            group.apply(0, &command);
        }
        group
    }
    fn node(&mut self, index: usize) -> &mut Session {
        self.nodes[index].as_mut().unwrap()
    }
    fn pump(&mut self) {
        let mut messages = Vec::new();
        for node in self.nodes.iter_mut().flatten() {
            messages.extend(node.poll().unwrap().messages);
        }
        for message in messages {
            let from = message.from as usize - 1;
            let to = message.to as usize - 1;
            if self.isolated == Some(from) || self.isolated == Some(to) {
                continue;
            }
            if let Some(node) = self.nodes.get_mut(to).and_then(Option::as_mut) {
                node.step(message).unwrap();
            }
        }
    }
    fn settle(&mut self) {
        for _ in 0..15 {
            for node in self.nodes.iter_mut().flatten() {
                node.tick().unwrap();
            }
            self.pump();
        }
    }
    fn apply(&mut self, index: usize, input: &AuthenticatedInput) -> MutationReceipt {
        assert!(matches!(
            self.node(index).propose(input).unwrap(),
            Submission::Pending(_) | Submission::Committed(_)
        ));
        self.settle();
        self.node(index)
            .receipt(&request_key(input))
            .unwrap()
            .clone()
    }
    fn elect_other(&mut self, old: usize) -> usize {
        self.isolated = Some(old);
        for _ in 0..400 {
            for node in self.nodes.iter_mut().flatten() {
                node.tick().unwrap();
            }
            self.pump();
            for candidate in 0..self.nodes.len() {
                if candidate != old && self.node(candidate).is_authoritative() {
                    self.settle();
                    return candidate;
                }
            }
        }
        panic!("majority did not elect a ready owner");
    }
    fn restart(&mut self, index: usize) {
        drop(self.nodes[index].take());
        self.nodes[index] = Some(
            Session::open(
                self.root.path().join(index.to_string()),
                ledger(),
                Self::config(index),
                SessionLimits::default(),
            )
            .unwrap(),
        );
    }
    fn until(
        &mut self,
        runtime: &mut Runtime,
        index: usize,
        mut done: impl FnMut(&Runtime, &Session) -> bool,
    ) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            runtime.drive(self.node(index)).unwrap();
            if done(runtime, self.node(index)) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "runtime quorum progress stalled: {:?}",
                runtime.pending_input()
            );
            self.pump();
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}
fn counted(clock: Arc<TestClock>, count: Arc<AtomicUsize>) -> Runtime {
    Runtime::new(
        RuntimeConfig {
            workers: 1,
            max_inflight: 1,
            ..Default::default()
        },
        identity(),
        catalog(
            vec![(
                handler(1, false),
                Box::new(CountingValidator(count, VerdictValue::Pass)),
            )],
            RetryContract::ReadOnly,
        ),
        Arc::new(InlineOnly),
        clock,
    )
    .unwrap()
}
fn pending_kind(runtime: &Runtime, kind: &str) -> bool {
    matches!(runtime.pending_input().map(|input| &input.command), Some(Command::RegisterArtifact { artifact }) if artifact.content.kind == kind)
}

#[test]
fn quorum_dispatch_waits_for_commit_and_retries_frozen_input_without_new_memory() {
    let mut group = Group::new();
    let clock = Arc::new(TestClock(AtomicU64::new(1)));
    let count = Arc::new(AtomicUsize::new(0));
    let mut runtime = counted(clock.clone(), count.clone());
    group.until(&mut runtime, 0, |runtime, _| {
        pending_kind(runtime, "runtime-dispatch")
    });
    let pending = runtime.pending_input().unwrap().clone();
    let bytes = postcard::to_stdvec(&pending).unwrap();
    let used = runtime.memory_stats().used;
    let prefix = group.node(0).sequence();
    assert_eq!(count.load(Ordering::SeqCst), 0);
    for now in 2..8 {
        clock.0.store(now, Ordering::SeqCst);
        let report = runtime.drive(group.node(0)).unwrap();
        assert_eq!(report.pending, Some(request_key(&pending)));
        assert_eq!(
            postcard::to_stdvec(runtime.pending_input().unwrap()).unwrap(),
            bytes
        );
        assert_eq!(runtime.memory_stats().used, used);
        assert_eq!(group.node(0).sequence(), prefix);
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }
    group.until(&mut runtime, 0, |_, session| status(session).is_terminal());
    assert_eq!(status(group.node(0)), ClaimStatus::Satisfied);
    assert_eq!(count.load(Ordering::SeqCst), 1);
    group.settle();
    let prefix = group.node(0).sequence();
    for node in group.nodes.iter().flatten() {
        assert_eq!(node.sequence(), prefix);
    }
}

#[test]
fn lost_owner_before_dispatch_cannot_execute_uncommitted_assignment() {
    let mut group = Group::new();
    let clock = Arc::new(TestClock(AtomicU64::new(1)));
    let old_count = Arc::new(AtomicUsize::new(0));
    let mut old = counted(clock.clone(), old_count.clone());
    group.until(&mut old, 0, |runtime, _| {
        pending_kind(runtime, "runtime-dispatch")
    });
    let pending = old.pending_input().unwrap().clone();
    let next = group.elect_other(0);
    assert!(old.drive(group.node(0)).is_err());
    assert_eq!(old.pending_input(), Some(&pending));
    assert_eq!(old_count.load(Ordering::SeqCst), 0);
    clock.0.store(2, Ordering::SeqCst);
    let new_count = Arc::new(AtomicUsize::new(0));
    let mut current = counted(clock.clone(), new_count.clone());
    group.until(&mut current, next, |_, session| {
        status(session).is_terminal()
    });
    assert_eq!(new_count.load(Ordering::SeqCst), 1);
    group.isolated = None;
    group.settle();
    assert!(old.drive(group.node(0)).is_err());
    assert_eq!(old_count.load(Ordering::SeqCst), 0);
    assert_eq!(status(group.node(0)), ClaimStatus::Satisfied);
    let final_owner = group.elect_other(next);
    let report = old.drive(group.node(final_owner)).unwrap();
    assert!(report.stale > 0);
    assert!(old.pending_input().is_none());
    assert_eq!(old_count.load(Ordering::SeqCst), 0);
}

#[test]
fn restarted_owner_requires_new_barrier_and_reuses_committed_diagnostic() {
    let mut group = Group::new();
    let clock = Arc::new(TestClock(AtomicU64::new(1)));
    let count = Arc::new(AtomicUsize::new(0));
    let mut runtime = counted(clock.clone(), count.clone());
    group.until(&mut runtime, 0, |runtime, _| {
        pending_kind(runtime, "validation-result")
    });
    assert_eq!(count.load(Ordering::SeqCst), 1);
    let diagnostic_input = runtime.pending_input().unwrap().clone();
    // Commit the diagnostic while the owner has not advanced to the verdict stage.
    group.settle();
    assert!(
        group
            .node(0)
            .receipt(&request_key(&diagnostic_input))
            .is_some()
    );
    group.node(0).checkpoint().unwrap();
    drop(runtime);
    for index in 0..3 {
        group.restart(index);
    }
    let mut recovered = counted(clock, count.clone());
    assert!(matches!(
        recovered.drive(group.node(0)),
        Err(RuntimeError::Ledger(
            focal_ledger::LedgerError::NotReady { .. }
        ))
    ));
    assert_eq!(count.load(Ordering::SeqCst), 1);
    group.node(0).campaign().unwrap();
    for _ in 0..100 {
        group.pump();
        if group.node(0).is_authoritative() {
            break;
        }
        for node in group.nodes.iter_mut().flatten() {
            node.tick().unwrap();
        }
    }
    assert_eq!(group.node(0).status().role, StateRole::Leader);
    group.until(&mut recovered, 0, |_, session| {
        status(session).is_terminal()
    });
    assert_eq!(status(group.node(0)), ClaimStatus::Satisfied);
    assert_eq!(
        count.load(Ordering::SeqCst),
        1,
        "committed diagnostic must replace external execution"
    );
}

#[test]
fn pending_receipt_adoption_rejects_old_verdict_in_effective_order() {
    let mut group = Group::new();
    let clock = Arc::new(TestClock(AtomicU64::new(1)));
    let count = Arc::new(AtomicUsize::new(0));
    let mut runtime = counted(clock, count);
    group.until(&mut runtime, 0, |runtime, _| {
        pending_kind(runtime, "validation-result")
    });
    group.settle();
    let adoption = input(
        500,
        EVALUATOR,
        Command::AdoptReceipt {
            claim: CLAIM,
            previous: ReceiptFence {
                receipt: ReceiptId::from_u128(30),
                epoch: 1,
            },
            receipt: ReceiptId::from_u128(31),
            holder: WORKER,
            epoch: 2,
        },
    );
    assert!(matches!(
        group.node(0).propose(&adoption).unwrap(),
        Submission::Pending(_)
    ));
    assert!(matches!(
        runtime.drive(group.node(0)),
        Err(RuntimeError::Domain(DomainOutcome::Refuse {
            code: ErrorCode::StaleReceipt,
            ..
        }))
    ));
    group.settle();
    let state = group.node(0).read_at_least(SessionSeq(0)).unwrap();
    assert!(
        state
            .runs
            .values()
            .filter(|run| run.id.validation == ValidationId::from_u128(21))
            .all(|run| run.attempts.is_empty())
    );
    group.until(&mut runtime, 0, |_, session| status(session).is_terminal());
    assert_eq!(status(group.node(0)), ClaimStatus::Satisfied);
}

#[test]
fn leadership_loss_cancels_old_execution_and_discards_its_late_pass() {
    let mut group = Group::new();
    let gate = Gate::new();
    let clock = Arc::new(TestClock(AtomicU64::new(1)));
    let mut old = Runtime::with_executor(
        RuntimeConfig {
            workers: 1,
            max_inflight: 1,
            ..Default::default()
        },
        identity(),
        catalog(
            vec![(handler(1, false), Box::new(TestReportValidator))],
            RetryContract::ReadOnly,
        ),
        gate.clone(),
        clock.clone(),
    )
    .unwrap();
    group.until(&mut old, 0, |runtime, _| runtime.active_count() == 1);
    gate.wait_started();
    let next = group.elect_other(0);
    assert!(old.drive(group.node(0)).is_err());
    gate.release();
    let new_count = Arc::new(AtomicUsize::new(0));
    let mut current = Runtime::new(
        RuntimeConfig {
            workers: 1,
            max_inflight: 1,
            ..Default::default()
        },
        identity(),
        catalog(
            vec![(
                handler(1, false),
                Box::new(CountingValidator(new_count.clone(), VerdictValue::Fail)),
            )],
            RetryContract::ReadOnly,
        ),
        Arc::new(InlineOnly),
        clock,
    )
    .unwrap();
    group.until(&mut current, next, |_, session| {
        status(session).is_terminal()
    });
    assert_eq!(status(group.node(next)), ClaimStatus::ValidationFailed);
    group.isolated = None;
    group.settle();
    let until = Instant::now() + Duration::from_secs(2);
    while old.active_count() != 0 {
        assert!(old.drive(group.node(0)).is_err());
        assert!(Instant::now() < until);
        std::thread::yield_now();
    }
    assert_eq!(gate.started.load(Ordering::SeqCst), 1);
    assert_eq!(new_count.load(Ordering::SeqCst), 1);
    assert_eq!(status(group.node(0)), ClaimStatus::ValidationFailed);
}

#[test]
fn staged_input_capacity_fails_before_consensus_without_retaining_a_charge() {
    let mut group = Group::new();
    let mut runtime = Runtime::new(
        RuntimeConfig {
            max_pending_bytes: 1,
            ..Default::default()
        },
        identity(),
        catalog(
            vec![(handler(1, false), Box::new(TestReportValidator))],
            RetryContract::ReadOnly,
        ),
        Arc::new(InlineOnly),
        Arc::new(TestClock(AtomicU64::new(1))),
    )
    .unwrap();
    let prefix = group.node(0).sequence();
    let used = runtime.memory_stats().used;
    let mut refused = 0;
    for _ in 0..6 {
        match runtime.drive(group.node(0)) {
            Err(RuntimeError::Capacity) => refused += 1,
            Ok(_) => {}
            Err(error) => panic!("unexpected {error}"),
        };
        assert_eq!(runtime.memory_stats().used, used);
        assert!(runtime.pending_input().is_none());
        assert_eq!(group.node(0).pending_count(), 0);
        assert_eq!(group.node(0).sequence(), prefix);
    }
    assert!(refused > 0);
}
