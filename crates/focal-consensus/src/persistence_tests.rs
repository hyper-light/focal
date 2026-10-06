use super::*;
use std::sync::mpsc;

fn shared(dir: &std::path::Path) -> SharedWal {
    SharedWal::open(
        dir,
        WalOptions::new(WalIdentity {
            cluster: [1; 16],
            node: 1,
            stream: 0,
        }),
    )
    .unwrap()
}
fn blocker(wal: &SharedWal) -> WalLease {
    let mut lease = wal.lease(LogicalLogId([250; 16])).unwrap();
    let records: Vec<_> = (1..=3)
        .map(|index| Record {
            log: LogicalLogId([250; 16]),
            kind: RecordKind::Entry,
            index,
            term: 1,
            payload: vec![1],
        })
        .collect();
    lease.append(&records).unwrap();
    lease
}
/// The real bounded replay channel stops the disk owner behind this callback;
/// no test-only disk API, sleeps or timing assumption controls the write batch.
fn pause(lease: WalLease) -> (mpsc::SyncSender<()>, std::thread::JoinHandle<WalLease>) {
    let (entered, entry) = mpsc::sync_channel(1);
    let (resume, resumed) = mpsc::sync_channel(1);
    let worker = std::thread::spawn(move || {
        let mut first = true;
        lease
            .replay(|_| {
                if first {
                    first = false;
                    entered.send(()).unwrap();
                    resumed.recv().unwrap();
                }
                Ok(())
            })
            .unwrap();
        lease
    });
    entry.recv().unwrap();
    (resume, worker)
}
fn node(wal: &SharedWal, group: u8, budget: &MemoryBudget) -> DurableNode {
    let mut node = DurableNode::open_on_wal_in(
        NodeConfig::single(1, [1; 16], [group; 16]),
        wal.clone(),
        budget,
    )
    .unwrap();
    node.campaign().unwrap();
    drop(node.drain().unwrap());
    node
}

#[test]
fn queue_pressure_retains_the_ready_without_clearing_authority() {
    let dir = tempfile::tempdir().unwrap();
    let wal = SharedWal::open_with_budget(
        dir.path(),
        WalOptions::new(WalIdentity {
            cluster: [1; 16],
            node: 1,
            stream: 0,
        }),
        focal_log::WalWriterLimits {
            queue_items: 1,
            ..Default::default()
        },
        MemoryBudget::new(128 * 1024 * 1024, 32 * 1024 * 1024).unwrap(),
    )
    .unwrap();
    let budget = MemoryBudget::new(128 * 1024 * 1024, 32 * 1024 * 1024).unwrap();
    let mut node = node(&wal, 1, &budget);
    let lease = blocker(&wal);
    let mut filler = wal.lease(LogicalLogId([249; 16])).unwrap();
    node.propose(vec![7; 4096]).unwrap();
    let (resume, worker) = pause(lease);
    let mut tickets = Vec::new();
    for index in 1..=8 {
        let record = Record {
            log: LogicalLogId([249; 16]),
            kind: RecordKind::Entry,
            index,
            term: 1,
            payload: vec![1],
        };
        match filler.append_async_in(&[record], BudgetLane::Completion) {
            Ok(ticket) => tickets.push(ticket),
            Err(focal_log::LogError::Capacity) => break,
            Err(error) => panic!("unexpected filler failure: {error}"),
        }
    }
    assert!(!tickets.is_empty());
    assert!(node.try_drain().unwrap().is_none());
    assert!(node.persistence_pending());
    let used = budget.stats().used;
    for _ in 0..8 {
        assert!(node.try_drain().unwrap().is_none());
        assert_eq!(budget.stats().used, used);
    }
    assert!(matches!(
        node.drain(),
        Err(ConsensusError::PersistencePending)
    ));
    assert!(!node.log().failed);
    assert!(matches!(
        node.tick(),
        Err(ConsensusError::PersistencePending)
    ));
    resume.send(()).unwrap();
    drop(worker.join().unwrap());
    for mut ticket in tickets {
        ticket.wait_blocking().unwrap();
    }
    let events = node.drain().unwrap();
    assert_eq!(events.committed.len(), 1);
    assert_eq!(events.committed[0].data, vec![7; 4096]);
    assert!(!node.persistence_pending());
    drop(events);
    drop(node);
    assert_eq!(budget.stats().used, 0);
}
#[test]
fn single_owner_queues_many_groups_into_one_covering_flush() {
    let dir = tempfile::tempdir().unwrap();
    let wal = shared(dir.path());
    let budget = MemoryBudget::new(128 * 1024 * 1024, 32 * 1024 * 1024).unwrap();
    let mut nodes: Vec<_> = (1..=12).map(|group| node(&wal, group, &budget)).collect();
    let lease = blocker(&wal);
    let before = wal.stats().unwrap();
    for (index, node) in nodes.iter_mut().enumerate() {
        node.propose(vec![index as u8; 4096]).unwrap();
    }
    let (resume, worker) = pause(lease);
    for node in &mut nodes {
        assert!(node.try_drain().unwrap().is_none());
    }
    for node in &mut nodes {
        assert!(node.persistence_pending());
        assert!(node.try_drain().unwrap().is_none());
        assert!(!node.has_committed_current_term());
        assert!(matches!(
            node.tick(),
            Err(ConsensusError::PersistencePending)
        ));
        assert!(matches!(
            node.propose(vec![9]),
            Err(ConsensusError::PersistencePending)
        ));
        assert!(matches!(
            node.read_index(vec![1]),
            Err(ConsensusError::PersistencePending)
        ));
        assert!(matches!(
            node.checkpoint(node.status().applied_index, vec![1]),
            Err(ConsensusError::PersistencePending)
        ));
        assert!(matches!(
            node.campaign(),
            Err(ConsensusError::PersistencePending)
        ));
        assert!(matches!(
            node.step(Message::default()),
            Err(ConsensusError::PersistencePending)
        ));
    }
    resume.send(()).unwrap();
    drop(worker.join().unwrap());
    let after_ready = wal.stats().unwrap();
    assert_eq!(after_ready.group_commits - before.group_commits, 1);
    // The one flush is all any of them waits for: a member that alone
    // decides wrote the commit of its entry with it, and gives the entry
    // without asking the disk again.
    for (index, node) in nodes.iter_mut().enumerate() {
        let events = node.try_drain().unwrap().unwrap();
        assert_eq!(events.committed.len(), 1);
        assert_eq!(events.committed[0].data, vec![index as u8; 4096]);
        assert!(!node.persistence_pending());
        assert!(node.has_committed_current_term());
        assert!(node.drain().unwrap().committed.is_empty());
    }
    assert_eq!(
        wal.stats().unwrap().group_commits - before.group_commits,
        1,
        "twelve groups committed an entry each on one flush"
    );
    drop(nodes);
    drop(wal);
    assert_eq!(budget.stats().used, 0);
    // What each gave is what its log holds: the entry, committed, with no
    // election to say so again.
    let wal = shared(dir.path());
    for group in 1..=12u8 {
        let mut node = DurableNode::open_on_wal_in(
            NodeConfig::single(1, [1; 16], [group; 16]),
            wal.clone(),
            &budget,
        )
        .unwrap();
        let recovered = node.drain().unwrap();
        assert_eq!(recovered.committed.len(), 1);
        assert_eq!(recovered.committed[0].data, vec![group - 1; 4096]);
    }
}
#[test]
fn a_failed_write_never_releases_success_and_recovery_uses_its_exact_fence() {
    for fault in [
        FaultPoint::AfterAppend,
        FaultPoint::AfterDataSync,
        FaultPoint::AfterFenceInstall,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let wal = shared(dir.path());
        let budget = MemoryBudget::new(64 * 1024 * 1024, 16 * 1024 * 1024).unwrap();
        let mut node = node(&wal, 1, &budget);
        let published = node.status().applied_index;
        node.propose(vec![42; 8192]).unwrap();
        // The cut is installed for the one write the entry and its commit
        // share.
        node.inject_fault_once(fault).unwrap();
        assert!(node.drain().is_err());
        assert_eq!(node.status().applied_index, published);
        assert!(matches!(node.tick(), Err(ConsensusError::Failed)));
        drop(node);
        drop(wal);
        assert_eq!(budget.stats().used, 0);
        let wal = shared(dir.path());
        let mut node =
            DurableNode::open_on_wal_in(NodeConfig::single(1, [1; 16], [1; 16]), wal, &budget)
                .unwrap();
        let recovered = node.drain().unwrap();
        if fault == FaultPoint::AfterFenceInstall {
            // The fence holds the entry and the commit that names it.
            assert_eq!(recovered.committed[0].data, vec![42; 8192]);
        } else {
            // Neither: the entry is not in the log to be committed again.
            assert!(recovered.committed.is_empty());
            node.campaign().unwrap();
            assert!(node.drain().unwrap().committed.is_empty());
        }
    }
}
#[test]
fn dropping_pending_owner_releases_ram_but_cannot_cancel_an_admitted_write() {
    let dir = tempfile::tempdir().unwrap();
    let wal = shared(dir.path());
    let budget = MemoryBudget::new(64 * 1024 * 1024, 16 * 1024 * 1024).unwrap();
    let mut node = node(&wal, 1, &budget);
    let lease = blocker(&wal);
    let (resume, worker) = pause(lease);
    node.propose(vec![17; 32 * 1024]).unwrap();
    assert!(node.try_drain().unwrap().is_none());
    assert!(budget.stats().used > 32 * 1024);
    drop(node);
    assert_eq!(budget.stats().used, 0);
    resume.send(()).unwrap();
    drop(worker.join().unwrap());
    let mut node =
        DurableNode::open_on_wal_in(NodeConfig::single(1, [1; 16], [1; 16]), wal, &budget).unwrap();
    // The admitted write held the entry and, the member deciding alone, the
    // commit that names it: the log gives it back committed.
    let recovered = node.drain().unwrap();
    assert_eq!(recovered.committed.len(), 1);
    assert_eq!(recovered.committed[0].data, vec![17; 32 * 1024]);
    node.campaign().unwrap();
    assert!(node.drain().unwrap().committed.is_empty());
}

#[test]
fn an_outstanding_ready_keeps_reserved_group_memory_through_publication() {
    let dir = tempfile::tempdir().unwrap();
    let wal = shared(dir.path());
    let budget = MemoryBudget::new(32 * 1024 * 1024, 8 * 1024 * 1024).unwrap();
    let mut node = node(&wal, 1, &budget);
    let lease = blocker(&wal);
    let (resume, worker) = pause(lease);
    node.propose(vec![19; 64 * 1024]).unwrap();
    assert!(node.try_drain().unwrap().is_none());
    let pressure = budget
        .reserve(
            BudgetKind::Pending,
            BudgetLane::Completion,
            budget.stats().limit - budget.stats().used,
        )
        .unwrap()
        .commit();
    assert!(node.try_drain().unwrap().is_none());
    resume.send(()).unwrap();
    drop(worker.join().unwrap());
    // The group reuses its admitted staging permit; its separate physical WAL
    // remains responsible for disk queue admission.
    let mut events = node.drain().unwrap();
    assert_eq!(events.committed[0].data, vec![19; 64 * 1024]);
    let permit = events.take_allocation().unwrap();
    drop(pressure);
    drop(node);
    assert_eq!(budget.stats().used, permit.bytes());
    drop(events);
    drop(permit);
    assert_eq!(budget.stats().used, 0);
}

/// The audit's F17, and what mantle's replica does with this core
/// (`docs/design/replica.md` §3 there): a leader's appends leave while its
/// own write is in flight, since its members persist them for themselves
/// (Ongaro's thesis §10.2.1); a follower's answer leaves only once its write
/// is durable; and the entry commits on the leader's one flush.
#[test]
fn a_leaders_appends_leave_while_it_flushes_and_a_followers_answer_after() {
    use super::tests::Cluster;
    let append = MessageType::MsgAppend;
    let answer = MessageType::MsgAppendResponse;
    let mut cluster = Cluster::new();
    cluster.nodes[0].campaign().unwrap();
    cluster.pump(None);
    cluster.nodes[0].propose(b"settle".to_vec()).unwrap();
    cluster.pump(None);
    for _ in 0..4 {
        for node in &mut cluster.nodes {
            node.tick().unwrap();
        }
        cluster.pump(None);
    }
    let wals: Vec<SharedWal> = cluster
        .nodes
        .iter()
        .map(|node| node.shared_wal().unwrap())
        .collect();
    let flushes = |wal: &SharedWal| wal.stats().unwrap().group_commits;
    // The leader's disk is held: its write cannot finish.
    let blocked = blocker(&wals[0]);
    let before = flushes(&wals[0]);
    let (resume_leader, leader_disk) = pause(blocked);
    cluster.nodes[0].propose(b"overlap".to_vec()).unwrap();
    assert!(cluster.nodes[0].sendable().unwrap().is_none());
    assert!(cluster.nodes[0].try_drain().unwrap().is_none());
    assert!(cluster.nodes[0].persistence_pending());
    let mut early = cluster.nodes[0].sendable().unwrap().unwrap();
    assert!(early.committed.is_empty() && early.read_states.is_empty());
    let appends: Vec<_> = std::mem::take(&mut early.messages);
    assert_eq!(appends.len(), 2);
    assert!(appends.iter().all(|message| message.msg_type == append
        && message.entries.len() == 1
        && message.entries[0].data == b"overlap"));
    // Given once; nothing more is said until the write is durable, and the
    // member takes nothing meanwhile: its owner keeps what comes.
    assert!(cluster.nodes[0].sendable().unwrap().is_none());
    assert!(cluster.nodes[0].try_drain().unwrap().is_none());
    assert!(matches!(
        cluster.nodes[0].tick(),
        Err(ConsensusError::PersistencePending)
    ));
    // One follower's disk is held as well; the other's is not.
    let (resume_follower, follower_disk) = pause(blocker(&wals[1]));
    for message in appends {
        let to = message.to as usize - 1;
        cluster.nodes[to].step(message).unwrap();
    }
    // A follower answers for what it holds: nothing of it may be sent
    // before its write is durable.
    assert!(cluster.nodes[1].try_drain().unwrap().is_none());
    assert!(cluster.nodes[1].persistence_pending());
    assert!(cluster.nodes[1].sendable().unwrap().is_none());
    assert!(cluster.nodes[2].try_drain().unwrap().is_none());
    assert!(cluster.nodes[2].sendable().unwrap().is_none());
    assert!(cluster.nodes[2].wait_persisted().unwrap());
    let answered = cluster.nodes[2].try_drain().unwrap().unwrap();
    assert!(!cluster.nodes[2].persistence_pending());
    let answers: Vec<_> = answered
        .messages
        .into_iter()
        .filter(|message| message.msg_type == answer)
        .collect();
    assert_eq!(answers.len(), 1);
    assert!(!answers[0].reject);
    // The leader's write finishes. It and the follower that answered are a
    // quorum: the entry commits, on the one flush the leader made for it.
    resume_leader.send(()).unwrap();
    drop(leader_disk.join().unwrap());
    assert!(cluster.nodes[0].wait_persisted().unwrap());
    let settled = cluster.nodes[0].try_drain().unwrap().unwrap();
    assert!(!cluster.nodes[0].persistence_pending());
    assert!(settled.committed.is_empty() && settled.messages.is_empty());
    for message in answers {
        cluster.nodes[0].step(message).unwrap();
    }
    let committed = cluster.nodes[0].try_drain().unwrap().unwrap();
    assert_eq!(committed.committed.len(), 1);
    assert_eq!(committed.committed[0].data, b"overlap");
    assert!(!cluster.nodes[0].persistence_pending());
    // The commit waited for no flush: it was given by a drain that does not
    // wait, and is written behind
    // (`a_commit_is_waited_for_by_no_write_and_is_written_behind_what_it_released`
    // holds every disk while it is applied).
    assert!(flushes(&wals[0]) > before);
    // The held follower answers once its disk lets it.
    resume_follower.send(()).unwrap();
    drop(follower_disk.join().unwrap());
    assert!(cluster.nodes[1].wait_persisted().unwrap());
    let late = cluster.nodes[1].try_drain().unwrap().unwrap();
    assert!(
        late.messages
            .iter()
            .any(|message| message.msg_type == answer && !message.reject)
    );
    drop(early);
}

/// What `sendable` gives is charged as every event is, and is no snapshot:
/// a snapshot's owner answers for what became of it, which the member takes
/// only once its write is done.
#[test]
fn what_is_sent_early_is_charged_and_leaves_a_snapshot_for_the_drain() {
    let dir = tempfile::tempdir().unwrap();
    let wal = shared(dir.path());
    let budget = MemoryBudget::new(64 * 1024 * 1024, 16 * 1024 * 1024).unwrap();
    let mut cfg = NodeConfig::single(1, [1; 16], [1; 16]);
    cfg.learners = vec![2, 3];
    let mut node = DurableNode::open_on_wal_in(cfg, wal.clone(), &budget).unwrap();
    node.campaign().unwrap();
    let index = node.drain().unwrap().applied_index;
    node.checkpoint(index, b"prefix".to_vec()).unwrap();
    let term = node.status().term;
    // One learner is behind the checkpoint and is sent it; the other holds
    // the log and is sent the entry.
    node.step(Message {
        from: 3,
        to: 1,
        term,
        index,
        msg_type: MessageType::MsgAppendResponse,
        ..Message::default()
    })
    .unwrap();
    drop(node.drain().unwrap());
    // The other answers a heartbeat a beat after its probe went unanswered:
    // the probe is sent again, and what it needs is the checkpoint.
    for _ in 0..node.heartbeat_tick() {
        node.tick().unwrap();
        drop(node.drain().unwrap());
    }
    node.step(Message {
        from: 2,
        to: 1,
        term,
        msg_type: MessageType::MsgHeartbeatResponse,
        ..Message::default()
    })
    .unwrap();
    let (resume, worker) = pause(blocker(&wal));
    node.propose(b"entry".to_vec()).unwrap();
    assert!(node.try_drain().unwrap().is_none());
    let used = budget.stats().used;
    let mut early = node.sendable().unwrap().unwrap();
    assert!(!early.messages.is_empty());
    assert!(
        early
            .messages
            .iter()
            .all(|message| message.msg_type != MessageType::MsgSnapshot)
    );
    // The charge moved with the messages: nothing was reserved anew.
    assert_eq!(budget.stats().used, used);
    let permit = early.take_allocation().unwrap();
    assert!(permit.bytes() > 0);
    resume.send(()).unwrap();
    drop(worker.join().unwrap());
    let rest = node.drain().unwrap();
    assert_eq!(
        rest.messages
            .iter()
            .filter(|message| message.msg_type == MessageType::MsgSnapshot)
            .count(),
        1
    );
    assert_eq!(rest.committed.len(), 1);
    drop(rest);
    drop(early);
    drop(permit);
    drop(node);
    assert_eq!(budget.stats().used, 0);
}

/// A change of membership is the one thing not applied on a commit the log
/// does not hold. Two voters; the leader removes the other. Whoever is told
/// the removal committed may stop the removed member; a leader that then
/// restarted without the commit would still count it, and could never elect
/// itself. So the removal is applied, and given, only once the write that
/// states its commit is durable — and the leader opened again alone leads.
#[test]
fn a_change_of_membership_is_applied_only_once_the_log_holds_its_commit() {
    let dirs = [tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap()];
    let config = |node: u64| {
        let mut config = NodeConfig::single(node, [1; 16], [1; 16]);
        config.voters = vec![1, 2];
        config
    };
    let mut nodes: Vec<DurableNode> = dirs
        .iter()
        .enumerate()
        .map(|(at, dir)| DurableNode::open(config(at as u64 + 1), dir.path()).unwrap())
        .collect();
    let carry = |nodes: &mut Vec<DurableNode>| {
        for _ in 0..32 {
            let mut messages = Vec::new();
            for node in nodes.iter_mut() {
                messages.extend(node.drain().unwrap().messages);
            }
            if messages.is_empty() {
                return;
            }
            for message in messages {
                let to = message.to as usize - 1;
                nodes[to].step(message).unwrap();
            }
        }
        panic!("the two members did not settle");
    };
    nodes[0].campaign().unwrap();
    carry(&mut nodes);
    assert_eq!(nodes[0].status().role, StateRole::Leader);
    // An entry's commit waits for no write: the leader's disk is held and
    // the entry is given all the same, on its follower's answer.
    nodes[0].propose(b"entry".to_vec()).unwrap();
    let appends = nodes[0].drain().unwrap().messages;
    let mut answers = Vec::new();
    for message in appends {
        nodes[1].step(message).unwrap();
        answers.extend(nodes[1].drain().unwrap().messages);
    }
    let wal = nodes[0].shared_wal().unwrap();
    let (resume, worker) = pause(blocker(&wal));
    for message in answers {
        nodes[0].step(message).unwrap();
    }
    let events = nodes[0].try_drain().unwrap().unwrap();
    assert_eq!(events.committed.len(), 1);
    for message in events.messages {
        nodes[1].step(message).unwrap();
    }
    drop(nodes[1].drain().unwrap());
    resume.send(()).unwrap();
    drop(worker.join().unwrap());
    // The removal: proposed and persisted by both.
    let mut remove = ConfChangeV2::default();
    let mut member = ConfChangeSingle {
        node_id: 2,
        ..Default::default()
    };
    member.change_type = ConfChangeType::RemoveNode;
    remove.changes.push(member);
    nodes[0].propose_conf_change(remove).unwrap();
    let appends = nodes[0].drain().unwrap().messages;
    let mut answers = Vec::new();
    for message in appends {
        nodes[1].step(message).unwrap();
        answers.extend(nodes[1].drain().unwrap().messages);
    }
    // The leader's disk is held. The follower's answer commits the removal
    // in the core; it is not applied, and nothing says it committed, until
    // the commit is in the log.
    let (resume, worker) = pause(blocker(&wal));
    for message in answers {
        nodes[0].step(message).unwrap();
    }
    assert!(nodes[0].try_drain().unwrap().is_none());
    assert!(nodes[0].persistence_pending());
    assert_eq!(nodes[0].status().voters, vec![1, 2]);
    assert!(nodes[0].try_drain().unwrap().is_none());
    resume.send(()).unwrap();
    drop(worker.join().unwrap());
    assert!(nodes[0].wait_persisted().unwrap());
    let events = nodes[0].try_drain().unwrap().unwrap();
    assert_eq!(events.membership.len(), 1);
    assert_eq!(nodes[0].status().voters, vec![1]);
    // The removed member is stopped, and the leader with it. Opened again
    // alone, the leader's log says the removal committed: it is the one
    // voter, and leads.
    drop(events);
    drop(wal);
    drop(nodes);
    let mut alone = DurableNode::open(config(1), dirs[0].path()).unwrap();
    assert_eq!(alone.status().voters, vec![1]);
    alone.campaign().unwrap();
    drop(alone.drain().unwrap());
    assert_eq!(alone.status().role, StateRole::Leader);
    alone.propose(b"alone".to_vec()).unwrap();
    assert_eq!(alone.drain().unwrap().committed[0].data, b"alone");
}

fn image(from: &std::path::Path, to: &std::path::Path) {
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            std::fs::create_dir(&target).unwrap();
            image(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}
/// Two voters commit and apply one entry; then both are cut where they
/// stand — what their disks hold at that moment is opened in their place.
/// Says, for each, whether it then still holds what it had applied.
fn cut_after_applying(written_commit: bool) -> [bool; 2] {
    let dirs = [tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap()];
    let config = |node: u64| {
        let mut config = NodeConfig::single(node, [1; 16], [1; 16]);
        config.voters = vec![1, 2];
        config
    };
    let mut nodes: Vec<DurableNode> = dirs
        .iter()
        .enumerate()
        .map(|(at, dir)| {
            let mut node = DurableNode::open(config(at as u64 + 1), dir.path()).unwrap();
            if written_commit {
                node.apply_on_written_commit();
            }
            node
        })
        .collect();
    nodes[0].campaign().unwrap();
    for _ in 0..32 {
        let mut messages = Vec::new();
        for node in nodes.iter_mut() {
            messages.extend(node.drain().unwrap().messages);
        }
        if messages.is_empty() {
            break;
        }
        for message in messages {
            let to = message.to as usize - 1;
            nodes[to].step(message).unwrap();
        }
    }
    assert_eq!(nodes[0].status().role, StateRole::Leader);
    // The entry is durable at both; the follower's answer commits it.
    nodes[0].propose(b"entry".to_vec()).unwrap();
    let appends = nodes[0].drain().unwrap().messages;
    let mut answers = Vec::new();
    for message in appends {
        nodes[1].step(message).unwrap();
        answers.extend(nodes[1].drain().unwrap().messages);
    }
    // The leader's disk is held while the answer commits the entry.
    let wal = nodes[0].shared_wal().unwrap();
    let (resume, worker) = pause(blocker(&wal));
    for message in answers {
        nodes[0].step(message).unwrap();
    }
    let events = if written_commit {
        // Nothing is applied, and nothing says it committed, on a commit
        // the log does not hold.
        assert!(nodes[0].try_drain().unwrap().is_none());
        assert!(nodes[0].persistence_pending());
        resume.send(()).unwrap();
        drop(worker.join().unwrap());
        assert!(nodes[0].wait_persisted().unwrap());
        nodes[0].try_drain().unwrap().unwrap()
    } else {
        // A commit waits for no write.
        let events = nodes[0].try_drain().unwrap().unwrap();
        resume.send(()).unwrap();
        drop(worker.join().unwrap());
        events
    };
    assert_eq!(events.committed.len(), 1);
    assert_eq!(events.committed[0].data, b"entry");
    // The follower is told, and applies by the same rule.
    let wal = nodes[1].shared_wal().unwrap();
    let (resume, worker) = pause(blocker(&wal));
    for message in events.messages {
        nodes[1].step(message).unwrap();
    }
    let told = if written_commit {
        assert!(nodes[1].try_drain().unwrap().is_none());
        resume.send(()).unwrap();
        drop(worker.join().unwrap());
        assert!(nodes[1].wait_persisted().unwrap());
        nodes[1].try_drain().unwrap().unwrap()
    } else {
        let told = nodes[1].try_drain().unwrap().unwrap();
        resume.send(()).unwrap();
        drop(worker.join().unwrap());
        told
    };
    assert_eq!(told.committed.len(), 1);
    assert_eq!(told.committed[0].data, b"entry");
    // The cut: neither member is let go, so neither writes anything more.
    // What is on each disk now is what a member killed here opens on.
    let images = [tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap()];
    for (dir, to) in dirs.iter().zip(&images) {
        image(dir.path(), to.path());
    }
    let mut holds = [false; 2];
    for (at, dir) in images.iter().enumerate() {
        let mut opened = DurableNode::open(config(at as u64 + 1), dir.path()).unwrap();
        holds[at] = opened
            .drain()
            .unwrap()
            .committed
            .iter()
            .any(|entry| entry.data == b"entry");
    }
    holds
}

/// A group whose members act on what they applied before the group tells
/// them again — a control group: who is enrolled, the fence a binary serves
/// under — applies nothing on a commit its log does not hold. A member of
/// it cut the moment after it applied an entry opens holding that entry; a
/// member of a group that applies on the commit alone, cut there, opens
/// without it and is told again by its group. (A host that had honoured an
/// upgrade fence, killed within its owner's period and started below the
/// fence, served until its group told it of the fence again.)
#[test]
fn a_group_its_members_act_on_at_their_start_applies_only_on_a_commit_its_log_holds() {
    assert_eq!(cut_after_applying(true), [true, true]);
    assert_eq!(cut_after_applying(false), [false, false]);
}
