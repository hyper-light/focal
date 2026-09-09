//! Three disk-backed voters exchanging Raft messages in-process. These cover
//! committed-head matching on the leader, passive follower replay, leader loss
//! with unresolved candidates (barrier and conflict proofs), crash after commit
//! before reply, snapshot catch-up through the enclosing checkpoint, planned
//! handover, correlated read barriers, a follower without evidence custody, a
//! follower under memory pressure, and corrupted record bytes in transit.
use super::tests::{exhaust, ledger, limits, store};
use super::*;
use focal_consensus::{Message, MessageType, NodeConfig, SnapshotStatus};
use focal_core::native::fixtures as fx;
use focal_evidence::BuiltinNativeSchemas;
use focal_model::lifecycle::Binding;
use focal_model::lifecycle::evidence::Parent;
use focal_model::{
    ArtifactId, ClaimId, ClaimStatus, ContentRef, ParticipantId, RequestKey, ValidationMode,
};

const PARTIES: fx::Parties = fx::Parties::numbered(41);
const ELECTION_TICKS: usize = 10;
type Node = NativeSession<BuiltinNativeSchemas>;

fn config(id: u64) -> NodeConfig {
    NodeConfig::joining(id, [21; 16], [22; 16], vec![1, 2, 3], Vec::new())
}
fn open_node(
    dir: &std::path::Path,
    id: u64,
    parent: &MemoryBudget,
) -> Result<Opened<BuiltinNativeSchemas>, NativeSessionError> {
    let content = dir.join(format!("content-{id}"));
    std::fs::create_dir_all(&content).unwrap();
    NativeSession::open(
        dir.join(format!("wal-{id}")),
        config(id),
        ledger(),
        RangeId(u128::from(id)),
        NativeContentProfile::ProjectionOnly,
        limits(),
        parent,
        store(&content),
        BuiltinNativeSchemas,
    )
}
fn slot() -> fx::Slot {
    fx::Slot {
        slot: 0,
        missing_declaration_index: 20,
        mode: ValidationMode::Required,
        checks: vec![],
    }
}
fn creation_for(request: RequestKey, claim: u128) -> NativeInput {
    let declaration =
        fx::delivery_declaration(ledger(), PARTIES, claim, claim * 1000 + 300, 1000).unwrap();
    fx::creation(
        ledger(),
        PARTIES,
        request,
        claim,
        vec![declaration],
        &[slot()],
    )
    .unwrap()
}

/// Deterministic three-node transport: every node has its own parent budget so
/// pressure on one replica never leaks into another.
struct Cluster {
    dir: tempfile::TempDir,
    parents: Vec<MemoryBudget>,
    nodes: Vec<Option<Node>>,
    inbox: Vec<Message>,
    clock: u64,
    serial: u128,
    boundaries: Vec<NativeReadBoundary>,
    /// Nodes whose retryable refusals (memory, custody) are expected.
    retry_ok: Vec<u64>,
    /// Nodes whose fail-closed stop is expected; they are no longer polled.
    fail_ok: Vec<u64>,
    failed: Vec<u64>,
    /// Flip one byte of every committed-record entry delivered to this node.
    corrupt_to: Option<u64>,
}
impl Cluster {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let parents: Vec<MemoryBudget> = (0..3)
            .map(|_| MemoryBudget::new(256 << 20, 64 << 20).unwrap())
            .collect();
        let mut inbox = Vec::new();
        let nodes = (1..=3u64)
            .map(|id| {
                let opened = open_node(dir.path(), id, &parents[id as usize - 1]).unwrap();
                inbox.extend(opened.initial.consensus.messages);
                Some(opened.session)
            })
            .collect();
        Self {
            dir,
            parents,
            nodes,
            inbox,
            clock: 0,
            serial: 0,
            boundaries: Vec::new(),
            retry_ok: Vec::new(),
            fail_ok: Vec::new(),
            failed: Vec::new(),
            corrupt_to: None,
        }
    }
    fn node(&mut self, id: u64) -> &mut Node {
        self.nodes
            .get_mut(id as usize - 1)
            .unwrap()
            .as_mut()
            .unwrap()
    }
    fn live(&self) -> Vec<u64> {
        (1..=3u64)
            .filter(|id| self.nodes[*id as usize - 1].is_some() && !self.failed.contains(id))
            .collect()
    }
    fn stop(&mut self, id: u64) {
        drop(self.nodes.get_mut(id as usize - 1).unwrap().take());
    }
    fn reopen(&mut self, id: u64) -> Result<(), NativeSessionError> {
        assert!(self.nodes.get(id as usize - 1).unwrap().is_none());
        let opened = open_node(self.dir.path(), id, &self.parents[id as usize - 1])?;
        self.inbox.extend(opened.initial.consensus.messages);
        *self.nodes.get_mut(id as usize - 1).unwrap() = Some(opened.session);
        Ok(())
    }
    fn corrupt(&self, message: &mut Message) {
        if self.corrupt_to != Some(message.to) || message.get_msg_type() != MessageType::MsgAppend {
            return;
        }
        for entry in message.mut_entries().iter_mut() {
            if entry.data.is_empty() {
                continue;
            }
            let mut bytes = entry.data.to_vec();
            let middle = bytes.len() / 2;
            bytes[middle] ^= 0x40;
            entry.data = bytes;
        }
    }
    /// Deliver messages until nothing moves. Nodes whose delivery is retained on
    /// a retryable refusal keep it and are polled again later.
    fn pump(&mut self, isolated: &[u64]) {
        for _ in 0..400 {
            let mut messages = std::mem::take(&mut self.inbox);
            let mut progressed = false;
            let live = self.live();
            let boundaries = &mut self.boundaries;
            let retry_ok = &self.retry_ok;
            let fail_ok = &self.fail_ok;
            let failed = &mut self.failed;
            for (index, slot) in self.nodes.iter_mut().enumerate() {
                let id = index as u64 + 1;
                let Some(node) = slot else { continue };
                if !live.contains(&id) {
                    continue;
                }
                match node.poll() {
                    Ok(events) => {
                        progressed |=
                            !events.committed.is_empty() || !events.read_boundaries.is_empty();
                        boundaries.extend(events.read_boundaries);
                        messages.extend(events.consensus.messages);
                    }
                    Err(error) => match error.class() {
                        FailureClass::Retryable if retry_ok.contains(&id) => {}
                        FailureClass::FailClosed if fail_ok.contains(&id) => {
                            assert!(matches!(node.poll(), Err(NativeSessionError::Failed)));
                            failed.push(id);
                            progressed = true;
                        }
                        class => panic!("node {id}: {error:?} ({class:?})"),
                    },
                }
            }
            if messages.is_empty() && !progressed {
                return;
            }
            for mut message in messages {
                if isolated.contains(&message.from) || isolated.contains(&message.to) {
                    continue;
                }
                let (from, to) = (message.from, message.to);
                if !self.live().contains(&to) {
                    continue;
                }
                self.corrupt(&mut message);
                let snapshot = message.get_msg_type() == MessageType::MsgSnapshot;
                let retry_ok = self.retry_ok.contains(&to);
                match self.node(to).step(message) {
                    Ok(()) => {}
                    Err(error) if retry_ok && error.class() == FailureClass::Retryable => continue,
                    Err(error) => panic!("step into {to}: {error:?}"),
                }
                if snapshot && self.live().contains(&from) {
                    self.node(from)
                        .report_snapshot(to, SnapshotStatus::Finish)
                        .unwrap();
                }
            }
        }
        panic!("message delivery failed to quiesce");
    }
    /// Heartbeat rounds: leaders probe followers, stragglers catch up.
    fn settle(&mut self, isolated: &[u64]) {
        for _ in 0..8 {
            for id in self.live() {
                if isolated.contains(&id) {
                    continue;
                }
                let retry_ok = self.retry_ok.contains(&id);
                match self.node(id).tick() {
                    Ok(()) => {}
                    Err(error) if retry_ok && error.class() == FailureClass::Retryable => {}
                    Err(error) => panic!("tick {id}: {error:?}"),
                }
            }
            self.pump(isolated);
        }
    }
    fn authority(&mut self, isolated: &[u64]) -> Option<u64> {
        self.live()
            .into_iter()
            .filter(|id| !isolated.contains(id))
            .find(|id| self.node(*id).is_authoritative())
    }
    /// Make `id` authoritative: a planned handover when a connected authority
    /// exists, otherwise expire every connected lease and campaign.
    fn elect(&mut self, id: u64, isolated: &[u64]) {
        match self.authority(isolated) {
            Some(current) if current == id => return,
            Some(current) => self.node(current).transfer_leader(id).unwrap(),
            None => {
                for peer in self.live() {
                    if peer != id && !isolated.contains(&peer) {
                        self.node(peer)
                            .set_randomized_election_timeout(ELECTION_TICKS * 2 - 1)
                            .unwrap();
                        for _ in 0..ELECTION_TICKS {
                            self.node(peer).tick().unwrap();
                        }
                    }
                }
                self.node(id).campaign().unwrap();
            }
        }
        for _ in 0..4 {
            self.settle(isolated);
            if self.node(id).is_authoritative() {
                return;
            }
        }
        panic!("node {id} did not become authoritative");
    }
    fn next(&mut self, actor: ParticipantId) -> RequestKey {
        self.serial = self.serial.saturating_add(1);
        fx::request(actor, 1, self.serial)
    }
    fn propose(&mut self, id: u64, actor: ParticipantId, input: NativeInput) -> NativeSubmission {
        self.clock = self.clock.saturating_add(1);
        let clock = self.clock;
        self.node(id)
            .propose(fx::context(actor, clock), input)
            .unwrap()
    }
    fn commit(
        &mut self,
        id: u64,
        actor: ParticipantId,
        input: NativeInput,
        isolated: &[u64],
    ) -> NativeOutcome {
        let request = input.request;
        let outcome = match self.propose(id, actor, input) {
            NativeSubmission::Committed(outcome) => return outcome,
            NativeSubmission::Pending { outcome, .. } => outcome,
        };
        for _ in 0..8 {
            self.pump(isolated);
            if self.node(id).outcome(request).unwrap() == Some(outcome) {
                return outcome;
            }
        }
        panic!("candidate never committed on node {id}");
    }
    fn status(&mut self, id: u64, claim: u128) -> Option<ClaimStatus> {
        self.node(id)
            .committed_core()
            .unwrap()
            .native_claim(ClaimId::from_u128(claim))
            .map(|claim| claim.status())
    }
    fn claim(&mut self, id: u64, claim: u128) -> Binding {
        self.node(id)
            .committed_core()
            .unwrap()
            .native_claim(ClaimId::from_u128(claim))
            .unwrap()
            .binding()
    }
    fn sequence(&mut self, id: u64) -> SessionSeq {
        self.node(id).sequence().unwrap()
    }
    fn creation(&mut self, claim: u128) -> NativeInput {
        let request = self.next(PARTIES.issuer);
        creation_for(request, claim)
    }
    fn assert_same_state(&mut self, claims: &[u128]) {
        let live = self.live();
        let sequences: Vec<SessionSeq> = live.iter().map(|id| self.sequence(*id)).collect();
        assert!(
            sequences.windows(2).all(|pair| pair[0] == pair[1]),
            "native prefixes differ: {sequences:?}"
        );
        let ranges: Vec<Option<RangeId>> = live
            .iter()
            .map(|id| self.node(*id).recording_range())
            .collect();
        assert!(
            ranges.windows(2).all(|pair| pair[0] == pair[1]),
            "recording ranges differ: {ranges:?}"
        );
        for &claim in claims {
            let statuses: Vec<Option<ClaimStatus>> =
                live.iter().map(|id| self.status(*id, claim)).collect();
            assert!(
                statuses.windows(2).all(|pair| pair[0] == pair[1]),
                "claim {claim}: {statuses:?}"
            );
        }
    }
    /// Received claim 1 through create, post and receipt on the authority.
    fn received_claim(&mut self, leader: u64) {
        let create = self.creation(1);
        self.commit(leader, PARTIES.issuer, create, &[]);
        let expected = self.claim(leader, 1);
        let post = fx::post(self.next(PARTIES.issuer), expected);
        self.commit(leader, PARTIES.issuer, post, &[]);
        let expected = self.claim(leader, 1);
        let receipt = fx::acquire_receipt(self.next(PARTIES.subject), expected, 701);
        self.commit(leader, PARTIES.subject, receipt, &[]);
        assert_eq!(self.status(leader, 1), Some(ClaimStatus::Received));
    }
}

#[test]
fn three_voters_replicate_native_records_and_followers_match_the_leader() {
    let mut cluster = Cluster::new();
    cluster.elect(1, &[]);
    assert!(cluster.node(1).genesis().is_some());
    for id in 2..=3 {
        assert_eq!(
            cluster.node(id).genesis(),
            cluster.node(1).genesis(),
            "genesis replicated to {id}"
        );
        assert!(!cluster.node(id).is_authoritative());
    }
    let create = cluster.creation(1);
    let created = cluster.commit(1, PARTIES.issuer, create, &[]);
    let expected = cluster.claim(1, 1);
    let post = fx::post(cluster.next(PARTIES.issuer), expected);
    cluster.commit(1, PARTIES.issuer, post, &[]);
    let expected = cluster.claim(1, 1);
    let receipt = fx::acquire_receipt(cluster.next(PARTIES.subject), expected, 701);
    cluster.commit(1, PARTIES.subject, receipt, &[]);
    cluster.assert_same_state(&[1]);
    assert_eq!(cluster.status(3, 1), Some(ClaimStatus::Received));
    assert_eq!(
        cluster.node(3).outcome(created.invocation).unwrap(),
        Some(created)
    );
    assert_eq!(
        cluster.node(2).recording_range(),
        Some(RangeId(1)),
        "records carry the producer's range"
    );
    // Followers refuse admission outright; nothing is queued for a later term.
    let retry = fx::request(PARTIES.issuer, 1, 1);
    assert!(matches!(
        cluster
            .node(2)
            .propose(fx::context(PARTIES.issuer, 99), fx::post(retry, expected)),
        Err(NativeSessionError::NotReady { .. })
    ));
    assert_eq!(cluster.node(2).pending_count(), 0);
}

#[test]
fn a_newer_term_barrier_alone_proves_an_isolated_leaders_candidate_dead() {
    let mut cluster = Cluster::new();
    cluster.elect(1, &[]);
    let first = cluster.creation(1);
    cluster.commit(1, PARTIES.issuer, first, &[]);
    // The leader admits a candidate that never reaches a quorum.
    let orphan = cluster.creation(2);
    let orphan_key = orphan.request;
    let NativeSubmission::Pending { .. } = cluster.propose(1, PARTIES.issuer, orphan) else {
        panic!("fresh candidates are pending");
    };
    cluster.pump(&[1]);
    assert_eq!(cluster.node(1).pending_count(), 1);
    assert_eq!(cluster.node(1).outcome(orphan_key).unwrap(), None);
    // A new term elsewhere commits nothing but its own barrier.
    cluster.elect(2, &[1]);
    assert_eq!(cluster.sequence(2), SessionSeq(1));
    assert_eq!(
        cluster.node(1).pending_count(),
        1,
        "a role change alone must not discard the candidate"
    );
    cluster.settle(&[]);
    assert_eq!(
        cluster.node(1).pending_count(),
        0,
        "resolved by the applied barrier"
    );
    assert!(!cluster.node(1).is_authoritative());
    assert_eq!(cluster.node(1).outcome(orphan_key).unwrap(), None);
    cluster.assert_same_state(&[1, 2]);
    assert_eq!(cluster.status(1, 2), None);
    // The orphan's request key is free again under the new authority.
    let retried = cluster.commit(2, PARTIES.issuer, creation_for(orphan_key, 2), &[]);
    assert_eq!(retried.invocation, NativeInvocation::Request(orphan_key));
    assert_eq!(cluster.node(1).outcome(orphan_key).unwrap(), Some(retried));
    cluster.assert_same_state(&[1, 2]);
    assert_eq!(cluster.node(1).recording_range(), Some(RangeId(2)));
}

#[test]
fn a_conflicting_committed_prefix_discards_the_isolated_leaders_candidates() {
    let mut cluster = Cluster::new();
    cluster.elect(1, &[]);
    let first = cluster.creation(1);
    cluster.commit(1, PARTIES.issuer, first, &[]);
    let orphan = cluster.creation(2);
    let orphan_key = orphan.request;
    let NativeSubmission::Pending { .. } = cluster.propose(1, PARTIES.issuer, orphan) else {
        panic!("fresh candidates are pending");
    };
    cluster.pump(&[1]);
    cluster.elect(2, &[1]);
    let replacement = cluster.creation(3);
    let replaced = cluster.commit(2, PARTIES.issuer, replacement, &[1]);
    assert_eq!(
        cluster.node(1).pending_count(),
        1,
        "still unresolved while isolated"
    );
    cluster.settle(&[]);
    assert_eq!(cluster.node(1).pending_count(), 0);
    assert!(!cluster.node(1).is_authoritative());
    assert_eq!(cluster.node(1).outcome(orphan_key).unwrap(), None);
    assert_eq!(
        cluster.node(1).outcome(replaced.invocation).unwrap(),
        Some(replaced)
    );
    cluster.assert_same_state(&[1, 2, 3]);
    assert_eq!(cluster.status(1, 2), None);
    assert_eq!(cluster.status(1, 3), Some(ClaimStatus::Generated));
    assert_eq!(
        cluster.node(3).recording_range(),
        Some(RangeId(2)),
        "the new term bound its producer range"
    );
}

#[test]
fn a_commit_before_the_reply_survives_the_leader_crash_and_exact_retry_finds_it() {
    let mut cluster = Cluster::new();
    cluster.elect(1, &[]);
    let create = cluster.creation(1);
    let key = create.request;
    let created = cluster.commit(1, PARTIES.issuer, create, &[]);
    assert_eq!(
        cluster.node(2).outcome(key).unwrap(),
        Some(created),
        "quorum committed before the crash"
    );
    cluster.stop(1);
    cluster.reopen(1).unwrap();
    assert_eq!(
        cluster.node(1).outcome(key).unwrap(),
        Some(created),
        "recovered from the durable log"
    );
    assert_eq!(cluster.sequence(1), SessionSeq(1));
    assert!(!cluster.node(1).is_authoritative());
    cluster.elect(1, &[]);
    assert_eq!(
        cluster.propose(1, PARTIES.issuer, creation_for(key, 1)),
        NativeSubmission::Committed(created)
    );
    let second = cluster.creation(2);
    cluster.commit(1, PARTIES.issuer, second, &[]);
    cluster.assert_same_state(&[1, 2]);
}

#[test]
fn a_lagging_follower_installs_the_enclosing_checkpoint_then_replays_the_tail() {
    let mut cluster = Cluster::new();
    cluster.elect(1, &[]);
    let first = cluster.creation(1);
    cluster.commit(1, PARTIES.issuer, first, &[]);
    let second = cluster.creation(2);
    cluster.commit(1, PARTIES.issuer, second, &[3]);
    cluster.node(1).begin_checkpoint().unwrap();
    cluster.pump(&[3]);
    assert!(!cluster.node(1).checkpoint_pending());
    let third = cluster.creation(3);
    cluster.commit(1, PARTIES.issuer, third, &[3]);
    assert_eq!(cluster.sequence(3), SessionSeq(1));
    cluster.settle(&[]);
    assert_eq!(cluster.sequence(3), SessionSeq(3));
    cluster.assert_same_state(&[1, 2, 3]);
    assert_eq!(cluster.node(3).genesis(), cluster.node(1).genesis());
    // Planned handover: the restored follower takes authority in a new term.
    cluster.elect(3, &[]);
    assert!(!cluster.node(1).is_authoritative());
    let fourth = cluster.creation(4);
    cluster.commit(3, PARTIES.issuer, fourth, &[]);
    cluster.assert_same_state(&[1, 2, 3, 4]);
    // A restored replica produces under a derived incarnation, not its configured range.
    let incarnation = cluster.node(3).range();
    assert_ne!(incarnation, RangeId(3));
    assert_eq!(cluster.node(1).recording_range(), Some(incarnation));
    // The restored replica's own checkpoint reproduces the same prefix on restart
    // under a further incarnation; the recorded producer of history is unchanged.
    cluster.node(3).begin_checkpoint().unwrap();
    cluster.pump(&[]);
    cluster.stop(3);
    cluster.reopen(3).unwrap();
    assert_eq!(cluster.sequence(3), SessionSeq(4));
    assert_ne!(cluster.node(3).range(), incarnation);
    assert_eq!(cluster.node(3).recording_range(), Some(incarnation));
    cluster.assert_same_state(&[1, 2, 3, 4]);
}

#[test]
fn concurrent_correlated_read_barriers_return_their_own_correlation_at_an_applied_prefix() {
    let mut cluster = Cluster::new();
    cluster.elect(1, &[]);
    let create = cluster.creation(1);
    cluster.commit(1, PARTIES.issuer, create, &[]);
    cluster
        .node(1)
        .read_index(ReadCorrelation([1; 16]))
        .unwrap();
    cluster
        .node(1)
        .read_index(ReadCorrelation([2; 16]))
        .unwrap();
    cluster.pump(&[]);
    let seen = std::mem::take(&mut cluster.boundaries);
    let mut correlations: Vec<[u8; 16]> =
        seen.iter().map(|boundary| boundary.correlation.0).collect();
    correlations.sort();
    assert_eq!(correlations, vec![[1; 16], [2; 16]]);
    for boundary in seen {
        assert_eq!(boundary.native_sequence, SessionSeq(1));
        assert!(cluster.node(1).read_at_least(boundary).is_ok());
    }
    // Followers cannot serve a barrier; an isolated authority cannot complete one.
    assert!(
        cluster
            .node(2)
            .read_index(ReadCorrelation([3; 16]))
            .is_err()
    );
    cluster
        .node(1)
        .read_index(ReadCorrelation([4; 16]))
        .unwrap();
    cluster.pump(&[2, 3]);
    assert!(cluster.boundaries.is_empty());
}

#[test]
fn followers_retain_a_refused_evidence_record_until_custody_arrives_then_apply_it_once() {
    let mut cluster = Cluster::new();
    cluster.elect(1, &[]);
    cluster.received_claim(1);
    let parent = Parent::from_claim(
        cluster
            .node(1)
            .committed_core()
            .unwrap()
            .native_claim(ClaimId::from_u128(1))
            .unwrap(),
    )
    .unwrap();
    let (artifact, _slot) = fx::work_artifact(ledger(), 801, &parent, 0, fx::PROOF).unwrap();
    let claim = cluster.claim(1, 1);
    let submit = fx::submit_work(cluster.next(PARTIES.subject), claim, 0, artifact);
    let key = submit.request;
    // The leader verified custody locally; followers have no such bytes yet.
    cluster.retry_ok = vec![2, 3];
    let outcome = cluster.commit(1, PARTIES.subject, submit, &[]);
    assert_eq!(cluster.sequence(1), SessionSeq(4));
    assert_eq!(cluster.sequence(2), SessionSeq(3), "retained, not applied");
    assert_eq!(cluster.node(2).outcome(key).unwrap(), None);
    assert!(matches!(
        cluster.node(2).poll(),
        Err(error) if error.class() == FailureClass::Retryable
    ));
    // Custody is a separate replicated fact; once the bytes exist the retained
    // delivery resumes exactly once.
    let pointer = cluster
        .node(1)
        .committed_core()
        .unwrap()
        .native_artifact(ArtifactId::from_u128(801))
        .unwrap()
        .custody()
        .payload();
    let reference = ContentRef {
        domain: pointer.domain,
        root: pointer.root,
        length: pointer.length,
        class: pointer.class,
    };
    let source = cluster.node(1).store_for_test();
    let exported = source.export_manifest(&reference).unwrap();
    let chunks: Vec<Vec<u8>> = (0..exported.chunks())
        .map(|index| source.read_transfer_chunk(&exported, index).unwrap())
        .collect();
    let manifest = exported.encoded().to_vec();
    for follower in [2u64, 3] {
        let target = cluster.node(follower).store_for_test_mut();
        let incoming = target
            .prepare_import(reference.clone(), manifest.clone())
            .unwrap();
        for (index, bytes) in chunks.iter().enumerate() {
            target.import_chunk(&incoming, index, bytes).unwrap();
        }
        assert_eq!(&target.complete_import(&incoming).unwrap(), &reference);
    }
    cluster.retry_ok.clear();
    cluster.settle(&[]);
    cluster.assert_same_state(&[1]);
    assert_eq!(cluster.node(2).outcome(key).unwrap(), Some(outcome));
    assert_eq!(cluster.sequence(3), SessionSeq(4));
    assert!(
        cluster
            .node(3)
            .committed_core()
            .unwrap()
            .native_artifact(ArtifactId::from_u128(801))
            .is_some()
    );
}

#[test]
fn a_follower_under_memory_pressure_keeps_its_delivery_and_finishes_when_memory_returns() {
    let mut cluster = Cluster::new();
    cluster.elect(1, &[]);
    let first = cluster.creation(1);
    cluster.commit(1, PARTIES.issuer, first, &[]);
    let held = exhaust(&cluster.parents[2]);
    assert!(!held.is_empty());
    cluster.retry_ok = vec![3];
    let second = cluster.creation(2);
    let committed = cluster.commit(1, PARTIES.issuer, second, &[]);
    cluster.settle(&[]);
    assert_eq!(cluster.sequence(2), SessionSeq(2));
    assert_eq!(cluster.sequence(3), SessionSeq(1), "refused, not lost");
    assert_eq!(cluster.node(3).outcome(committed.invocation).unwrap(), None);
    drop(held);
    cluster.retry_ok.clear();
    cluster.settle(&[]);
    assert_eq!(cluster.sequence(3), SessionSeq(2));
    assert_eq!(
        cluster.node(3).outcome(committed.invocation).unwrap(),
        Some(committed)
    );
    cluster.assert_same_state(&[1, 2]);
}

#[test]
fn corrupted_record_bytes_stop_only_the_receiving_replica_and_never_apply() {
    let mut cluster = Cluster::new();
    cluster.elect(1, &[]);
    let first = cluster.creation(1);
    cluster.commit(1, PARTIES.issuer, first, &[]);
    cluster.corrupt_to = Some(3);
    cluster.fail_ok = vec![3];
    let second = cluster.creation(2);
    let committed = cluster.commit(1, PARTIES.issuer, second, &[]);
    cluster.settle(&[]);
    assert_eq!(cluster.failed, vec![3], "the damaged replica failed closed");
    assert_eq!(cluster.sequence(2), SessionSeq(2));
    assert_eq!(
        cluster.node(2).outcome(committed.invocation).unwrap(),
        Some(committed)
    );
    assert!(matches!(
        cluster.node(3).outcome(committed.invocation),
        Err(NativeSessionError::Failed)
    ));
    // The healthy majority keeps committing; the damaged log is refused on reopen.
    cluster.corrupt_to = None;
    let third = cluster.creation(3);
    cluster.commit(1, PARTIES.issuer, third, &[]);
    cluster.assert_same_state(&[1, 2, 3]);
    cluster.stop(3);
    cluster.failed.clear();
    match cluster.reopen(3) {
        Err(error) => assert_eq!(error.class(), FailureClass::FailClosed),
        Ok(()) => {
            assert!(matches!(
                cluster.node(3).poll(),
                Err(error) if error.class() == FailureClass::FailClosed
            ));
        }
    }
}

#[test]
fn corrupted_or_foreign_genesis_records_are_refused_fail_closed() {
    let record = genesis::Genesis::derive(
        [21; 16],
        [22; 16],
        ledger(),
        NativeContentProfile::ProjectionOnly,
        crate::native_checkpoint::format_hash(),
    );
    let mut bytes = [0u8; genesis::BYTES];
    record.write_into(&mut bytes);
    assert_eq!(genesis::Genesis::decode(&bytes).unwrap(), record);
    for offset in [0, 9, 12, 30, 60, 76, 77, 110, 150, genesis::BYTES - 1] {
        let mut corrupt = bytes;
        corrupt[offset] ^= 1;
        assert!(
            matches!(
                genesis::Genesis::decode(&corrupt),
                Err(NativeSessionError::Corrupt)
            ),
            "offset {offset}"
        );
    }
    assert!(genesis::Genesis::decode(&bytes[..bytes.len() - 1]).is_err());
    let other = genesis::Genesis::derive(
        [21; 16],
        [22; 16],
        LedgerId {
            tenant: focal_model::TenantId::from_u128(99),
            ..ledger()
        },
        NativeContentProfile::ProjectionOnly,
        crate::native_checkpoint::format_hash(),
    );
    assert_ne!(other.genesis, record.genesis);
}
