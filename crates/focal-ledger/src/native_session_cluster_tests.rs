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
use focal_model::lifecycle::evidence::Parent;
use focal_model::lifecycle::{Binding, ContractError};
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
    open_node_with(dir, id, parent, limits())
}
fn open_node_with(
    dir: &std::path::Path,
    id: u64,
    parent: &MemoryBudget,
    limits: NativeSessionLimits,
) -> Result<Opened<BuiltinNativeSchemas>, NativeSessionError> {
    let content = dir.join(format!("content-{id}"));
    std::fs::create_dir_all(&content).unwrap();
    NativeSession::open(
        dir.join(format!("wal-{id}")),
        config(id),
        ledger(),
        RangeId(u128::from(id)),
        NativeContentProfile::ProjectionOnly,
        limits,
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
        Self::with_limits(|_| limits())
    }
    /// A cluster whose nodes open under per-node limits (the materializer's
    /// worker count differs per follower in the differential tests).
    fn with_limits(limits_for: impl Fn(u64) -> NativeSessionLimits) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let parents: Vec<MemoryBudget> = (1..=3u64)
            .map(|id| {
                let limits = limits_for(id);
                MemoryBudget::new(
                    limits.memory_bytes.max(128 << 20).saturating_mul(2),
                    limits
                        .completion_reserve_bytes
                        .max(16 << 20)
                        .saturating_mul(2),
                )
                .unwrap()
            })
            .collect();
        let mut inbox = Vec::new();
        let nodes = (1..=3u64)
            .map(|id| {
                let opened =
                    open_node_with(dir.path(), id, &parents[id as usize - 1], limits_for(id))
                        .unwrap();
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

/// Doc 25 §2: a follower materializing a delivery's records in parallel
/// waves reaches byte-identical rows to one replaying them one at a time.
fn materializer_limits(workers: usize, assume_independent: bool) -> NativeSessionLimits {
    let mut limits = limits();
    limits.materializer.max_workers = workers;
    limits.materializer.assume_independent = assume_independent;
    // Many claims per session: every completion-class admission funds its
    // future record buffer, so the engine envelope grows with the claims.
    limits.memory_bytes = 768 << 20;
    limits.completion_reserve_bytes = 96 << 20;
    limits
}
fn digests(cluster: &mut Cluster) -> Vec<ContentHash> {
    cluster
        .live()
        .into_iter()
        .map(|id| cluster.node(id).native_state_digest().unwrap())
        .collect()
}
fn assert_same_digest(cluster: &mut Cluster, claims: &[u128]) {
    cluster.assert_same_state(claims);
    let digests = digests(cluster);
    assert!(
        digests.windows(2).all(|pair| pair[0] == pair[1]),
        "native rows differ across nodes: {digests:?}"
    );
}
/// Commit `inputs` on the leader while `isolated` cannot hear, then reconnect
/// it so it receives them as one delivery.
fn commit_isolated(
    cluster: &mut Cluster,
    isolated: u64,
    inputs: Vec<(ParticipantId, NativeInput)>,
) {
    commit_apart(cluster, isolated, inputs);
    cluster.settle(&[]);
    cluster.pump(&[]);
}
/// Commit `inputs` on the leader while `isolated` cannot hear; it stays cut
/// off, so later commits join the same delivery when it reconnects.
fn commit_apart(cluster: &mut Cluster, isolated: u64, inputs: Vec<(ParticipantId, NativeInput)>) {
    commit_apart_on(cluster, 1, isolated, inputs);
}
/// As `commit_apart`, but proposing to an explicit `leader` — used to commit a
/// second batch under a new term after a leadership handover.
fn commit_apart_on(
    cluster: &mut Cluster,
    leader: u64,
    isolated: u64,
    inputs: Vec<(ParticipantId, NativeInput)>,
) {
    let mut outcomes = Vec::new();
    for (position, (actor, input)) in inputs.into_iter().enumerate() {
        // Every pending candidate holds its funded report and record buffer
        // until it commits, so proposals are paced as a client would pace
        // them; the isolated follower still receives them all at once.
        if position % 4 == 0 {
            cluster.pump(&[isolated]);
        }
        let request = input.request;
        match cluster.propose(leader, actor, input) {
            NativeSubmission::Committed(outcome) => outcomes.push((request, outcome)),
            NativeSubmission::Pending { outcome, .. } => outcomes.push((request, outcome)),
        }
    }
    for _ in 0..8 {
        cluster.pump(&[isolated]);
        if outcomes.iter().all(|(request, outcome)| {
            cluster.node(leader).outcome(*request).unwrap() == Some(*outcome)
        }) {
            break;
        }
    }
    for (request, outcome) in &outcomes {
        assert_eq!(
            cluster.node(leader).outcome(*request).unwrap(),
            Some(*outcome)
        );
    }
}

#[test]
fn a_parallel_batch_after_a_leadership_change_crosses_the_term_start_gap() {
    // Node 3 has four workers and receives records from two terms in one
    // catch-up delivery. The new leader's term-start no-op is an empty entry
    // dropped from the committed stream, so the first record of the second term
    // sits at applied_raft + 2, not + 1. The parallel batch path must anchor on
    // the run's own first index (as the single-entry path already does) or it
    // fails closed on this routine leader change and the follower dies on a
    // valid log.
    let mut cluster =
        Cluster::with_limits(|id| materializer_limits(if id == 3 { 4 } else { 1 }, false));
    cluster.elect(1, &[]);
    let first: Vec<_> = (1..=4u128)
        .map(|claim| (PARTIES.issuer, cluster.creation(claim)))
        .collect();
    commit_apart_on(&mut cluster, 1, 3, first);
    // Hand leadership to node 2 (a new term, hence a term-start no-op) while
    // node 3 is still cut off, then commit a second batch under the new term.
    cluster.elect(2, &[3]);
    let second: Vec<_> = (5..=8u128)
        .map(|claim| (PARTIES.issuer, cluster.creation(claim)))
        .collect();
    commit_apart_on(&mut cluster, 2, 3, second);
    // Reconnect node 3: it replays both terms in one delivery, crossing the gap.
    cluster.settle(&[]);
    cluster.pump(&[]);
    let claims: Vec<u128> = (1..=8).collect();
    assert_same_digest(&mut cluster, &claims);
    let stats = cluster.node(3).materializer_stats();
    assert_eq!(stats.violations, 0, "{stats:?}");
    assert!(stats.parallel_batches >= 1, "{stats:?}");
}

#[test]
fn parallel_materialization_matches_serial_replay_on_independent_and_dependent_records() {
    let mut cluster =
        Cluster::with_limits(|id| materializer_limits(if id == 3 { 4 } else { 1 }, false));
    cluster.elect(1, &[]);
    // Six creations of distinct claims: no dependencies, staged in waves.
    let creations: Vec<_> = (1..=6u128)
        .map(|claim| (PARTIES.issuer, cluster.creation(claim)))
        .collect();
    commit_isolated(&mut cluster, 3, creations);
    let claims: Vec<u128> = (1..=8).collect();
    assert_same_digest(&mut cluster, &claims);
    let stats = cluster.node(3).materializer_stats();
    assert!(stats.batches >= 1, "{stats:?}");
    assert!(stats.parallel_batches >= 1, "{stats:?}");
    assert_eq!(stats.violations, 0, "{stats:?}");
    assert_eq!(cluster.node(2).materializer_stats().parallel_batches, 0);
    // Posts and receipts on three of them: a receipt depends on its post and
    // both on the claim, so the planner orders them and no read is stale.
    let mut dependent = Vec::new();
    for claim in 1..=3u128 {
        let binding = cluster.claim(1, claim);
        let post = fx::post(cluster.next(PARTIES.issuer), binding);
        dependent.push((PARTIES.issuer, post));
    }
    commit_isolated(&mut cluster, 3, dependent);
    let mut receipts = Vec::new();
    for claim in 1..=3u128 {
        let binding = cluster.claim(1, claim);
        let receipt = fx::acquire_receipt(cluster.next(PARTIES.subject), binding, 700 + claim);
        receipts.push((PARTIES.subject, receipt));
    }
    commit_isolated(&mut cluster, 3, receipts);
    assert_same_digest(&mut cluster, &claims);
    for claim in 1..=3 {
        assert_eq!(cluster.status(3, claim), Some(ClaimStatus::Received));
    }
    let stats = cluster.node(3).materializer_stats();
    assert_eq!(stats.violations, 0, "{stats:?}");
    assert!(stats.records >= 12, "{stats:?}");
    // A follower catching up after a restart replays the whole log the same way.
    cluster.stop(3);
    cluster.reopen(3).unwrap();
    cluster.settle(&[]);
    assert_same_digest(&mut cluster, &claims);
}

#[test]
fn a_planner_that_omits_every_edge_is_caught_by_the_barrier_and_still_matches() {
    let mut cluster =
        Cluster::with_limits(|id| materializer_limits(if id == 2 { 8 } else { 1 }, id == 2));
    cluster.elect(1, &[]);
    // The creations and the posts of four claims reach node 2 in one
    // delivery, and with eight workers and no edges every post is staged in
    // the same wave as its creation and reads a claim that is not there yet.
    let mut inputs = Vec::new();
    for claim in 1..=4u128 {
        inputs.push((PARTIES.issuer, cluster.creation(claim)));
    }
    commit_apart(&mut cluster, 2, inputs);
    let mut chained = Vec::new();
    for claim in 1..=4u128 {
        let binding = cluster.claim(1, claim);
        chained.push((
            PARTIES.issuer,
            fx::post(cluster.next(PARTIES.issuer), binding),
        ));
    }
    commit_isolated(&mut cluster, 2, chained);
    let mut extra = Vec::new();
    for claim in 5..=8u128 {
        extra.push((PARTIES.issuer, cluster.creation(claim)));
    }
    commit_isolated(&mut cluster, 2, extra);
    let claims: Vec<u128> = (1..=6).collect();
    assert_same_digest(&mut cluster, &claims);
    let stats = cluster.node(2).materializer_stats();
    assert!(stats.batches >= 1, "{stats:?}");
    assert!(
        stats.violations >= 1 && stats.serial_fallbacks >= 1,
        "the omitted edges must surface as stale reads: {stats:?}"
    );
    for claim in 1..=4 {
        assert_eq!(cluster.status(2, claim), Some(ClaimStatus::Posted));
    }
}

/// P10.7: the same log materialized by a one-worker and a four-worker
/// follower, with the time each spent in its batches. Writes
/// `target/measurements/materializer-<unix seconds>.json`; run with
/// `--ignored --nocapture`. Zero-conflict (independent creations) and
/// chained (post and receipt per claim) shapes are timed separately.
#[test]
#[ignore = "measurement, not a correctness gate"]
fn materializer_throughput_at_one_and_four_workers() {
    let mut cluster =
        Cluster::with_limits(|id| materializer_limits(if id == 3 { 4 } else { 1 }, false));
    cluster.elect(1, &[]);
    let claims: Vec<u128> = (1..=8).collect();
    // Every posted claim keeps its completion obligation funded until its
    // response cycle ends, so the chained shape uses four claims per node.
    let chained_claims: Vec<u128> = claims.iter().copied().take(4).collect();
    let mut lines = Vec::new();
    let mut line = |shape: &str,
                    isolated: u64,
                    before: MaterializerStats,
                    after: MaterializerStats| {
        lines.push(format!(
            "{{\"shape\":\"{shape}\",\"node\":{isolated},\"workers\":{},\"records\":{},\"micros\":{},\"waves\":{},\"parallel_batches\":{},\"violations\":{},\"serial_records\":{},\"serial_micros\":{}}}",
            if isolated == 3 { 4 } else { 1 },
            after.records - before.records,
            after.micros - before.micros,
            after.waves - before.waves,
            after.parallel_batches - before.parallel_batches,
            after.violations - before.violations,
            after.serial_records - before.serial_records,
            after.serial_micros - before.serial_micros,
        ));
    };
    for isolated in [2u64, 3u64] {
        let before = cluster.node(isolated).materializer_stats();
        let creations: Vec<_> = claims
            .iter()
            .map(|claim| {
                (
                    PARTIES.issuer,
                    cluster.creation(*claim + u128::from(isolated) * 100),
                )
            })
            .collect();
        commit_isolated(&mut cluster, isolated, creations);
        let after = cluster.node(isolated).materializer_stats();
        line("independent", isolated, before, after);
    }
    for isolated in [2u64, 3u64] {
        let before = cluster.node(isolated).materializer_stats();
        // One post and its receipt at a time: an open completion obligation
        // funds its future record buffers until its cycle ends, so a session
        // bounds how many posted claims are in flight at once.
        for claim in &chained_claims {
            let claim = *claim + u128::from(isolated) * 100;
            let binding = cluster.claim(1, claim);
            let post = fx::post(cluster.next(PARTIES.issuer), binding);
            commit_apart(&mut cluster, isolated, vec![(PARTIES.issuer, post)]);
            let binding = cluster.claim(1, claim);
            let receipt = fx::acquire_receipt(cluster.next(PARTIES.subject), binding, 9000 + claim);
            commit_apart(&mut cluster, isolated, vec![(PARTIES.subject, receipt)]);
        }
        commit_isolated(&mut cluster, isolated, Vec::new());
        let after = cluster.node(isolated).materializer_stats();
        line("chained", isolated, before, after);
    }
    let all: Vec<u128> = claims
        .iter()
        .flat_map(|claim| [*claim + 200, *claim + 300])
        .collect();
    assert_same_digest(&mut cluster, &all);
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/measurements");
    std::fs::create_dir_all(&dir).unwrap();
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let body = format!("[{}]\n", lines.join(",\n"));
    std::fs::write(dir.join(format!("materializer-{stamp}.json")), &body).unwrap();
    println!("{body}");
}

fn layout(cluster: &mut Cluster, id: u64) -> Vec<(RangeId, Option<[u8; 16]>)> {
    cluster.node(id).native_layout().unwrap().collect()
}
fn epoch(cluster: &mut Cluster, id: u64) -> u64 {
    cluster.node(id).native_layout_epoch().unwrap()
}
fn assert_same_layout(cluster: &mut Cluster, expected_epoch: u64) {
    let live = cluster.live();
    let layouts: Vec<_> = live.iter().map(|id| layout(cluster, *id)).collect();
    assert!(
        layouts.windows(2).all(|pair| pair[0] == pair[1]),
        "layouts differ across nodes: {layouts:?}"
    );
    for id in live {
        assert_eq!(epoch(cluster, id), expected_epoch, "node {id}");
    }
}

/// A layout change is a session decision (25 §4): the authority commits a
/// layout record that every replica applies between native records, so
/// the replicas hold one layout under one epoch; native proposals wait for
/// a change in flight, a change waits for pending candidates, a checkpoint
/// carries the layout and its epoch to a lagging follower, and a restart
/// replays the records.
#[test]
fn committed_layout_changes_apply_on_every_replica_and_fence_proposals() {
    let mut cluster = Cluster::new();
    cluster.elect(1, &[]);
    cluster.received_claim(1);
    assert_same_layout(&mut cluster, 0);
    // The origin member is named from the genesis, alike on every replica.
    let origin = super::range::origin_member(&cluster.node(1).genesis().unwrap()).unwrap();
    assert_eq!(layout(&mut cluster, 2), vec![(origin, None)]);
    let split_one = LayoutOperation::Split {
        at: ClaimId::from_u128(1).0,
        id: RangeId(31),
    };
    // Only the authority proposes a change.
    assert!(matches!(
        cluster.node(2).propose_layout(split_one),
        Err(NativeSessionError::NotReady { .. })
    ));
    // A change is checked against the committed layout before it is proposed.
    assert!(matches!(
        cluster
            .node(1)
            .propose_layout(LayoutOperation::Merge { left: RangeId(9) }),
        Err(NativeSessionError::Native(NativeError::Contract(
            ContractError::InvalidManifest
        )))
    ));
    // Pending candidates hold fragments of the current layout: a change
    // waits for them.
    let second = cluster.creation(2);
    let request = second.request;
    let NativeSubmission::Pending { outcome, .. } = cluster.propose(1, PARTIES.issuer, second)
    else {
        panic!("a fresh proposal is pending until the log commits it");
    };
    assert!(matches!(
        cluster.node(1).propose_layout(split_one),
        Err(NativeSessionError::Capacity)
    ));
    for _ in 0..8 {
        cluster.pump(&[]);
        if cluster.node(1).outcome(request).unwrap() == Some(outcome) {
            break;
        }
    }
    assert_eq!(cluster.node(1).outcome(request).unwrap(), Some(outcome));
    // A change in flight fences native proposals and further changes.
    cluster.node(1).propose_layout(split_one).unwrap();
    assert!(matches!(
        cluster.node(1).propose_layout(LayoutOperation::Split {
            at: ClaimId::from_u128(2).0,
            id: RangeId(32),
        }),
        Err(NativeSessionError::LayoutChanging)
    ));
    let third = cluster.creation(3);
    cluster.clock = cluster.clock.saturating_add(1);
    let clock = cluster.clock;
    assert!(matches!(
        cluster
            .node(1)
            .propose(fx::context(PARTIES.issuer, clock), third),
        Err(NativeSessionError::LayoutChanging)
    ));
    cluster.pump(&[]);
    assert_same_layout(&mut cluster, 1);
    assert_eq!(
        layout(&mut cluster, 3),
        vec![(origin, None), (RangeId(31), Some(ClaimId::from_u128(1).0))]
    );
    // Records flow again over the new layout and every replica agrees.
    let third = cluster.creation(3);
    cluster.commit(1, PARTIES.issuer, third, &[]);
    assert_same_digest(&mut cluster, &[1, 2, 3]);
    // A lagging follower installs a checkpoint carrying the layout and its
    // epoch, then replays a later layout record and the records after it.
    let fourth = cluster.creation(4);
    cluster.commit(1, PARTIES.issuer, fourth, &[2]);
    cluster.node(1).begin_checkpoint().unwrap();
    cluster.pump(&[2]);
    assert!(!cluster.node(1).checkpoint_pending());
    cluster
        .node(1)
        .propose_layout(LayoutOperation::Split {
            at: ClaimId::from_u128(3).0,
            id: RangeId(33),
        })
        .unwrap();
    cluster.pump(&[2]);
    assert_eq!(epoch(&mut cluster, 1), 2);
    assert_eq!(epoch(&mut cluster, 2), 1);
    let fifth = cluster.creation(5);
    cluster.commit(1, PARTIES.issuer, fifth, &[2]);
    cluster.settle(&[]);
    assert_same_layout(&mut cluster, 2);
    assert_same_digest(&mut cluster, &[1, 2, 3, 4, 5]);
    assert_ne!(cluster.node(2).range(), RangeId(2));
    // A restart replays the layout records from the log.
    cluster.stop(3);
    cluster.reopen(3).unwrap();
    assert_same_layout(&mut cluster, 2);
    assert_same_digest(&mut cluster, &[1, 2, 3, 4, 5]);
    // A merge joins two members under the left one's identity everywhere.
    cluster
        .node(1)
        .propose_layout(LayoutOperation::Merge { left: RangeId(31) })
        .unwrap();
    cluster.pump(&[]);
    assert_same_layout(&mut cluster, 3);
    assert_eq!(
        layout(&mut cluster, 2),
        vec![(origin, None), (RangeId(31), Some(ClaimId::from_u128(1).0))]
    );
    let sixth = cluster.creation(6);
    cluster.commit(1, PARTIES.issuer, sixth, &[]);
    assert_same_digest(&mut cluster, &[1, 2, 3, 4, 5, 6]);
    // A planned handover: the new authority changes the layout too.
    cluster.elect(3, &[]);
    cluster
        .node(3)
        .propose_layout(LayoutOperation::Split {
            at: ClaimId::from_u128(5).0,
            id: RangeId(35),
        })
        .unwrap();
    cluster.pump(&[]);
    assert_same_layout(&mut cluster, 4);
    let seventh = cluster.creation(7);
    cluster.commit(3, PARTIES.issuer, seventh, &[]);
    assert_same_digest(&mut cluster, &[1, 2, 3, 4, 5, 6, 7]);
}

/// A checkpoint whose Core root exceeds the inline bound travels as seeds
/// (25 §5): the author seals the chunks, the snapshot names them, and a
/// lagging follower installs it only once every chunk is local, pulled from
/// a replica that holds them.
#[test]
fn a_seeded_checkpoint_installs_once_its_chunks_are_pulled() {
    let mut cluster = Cluster::with_limits(|_| {
        let mut limits = limits();
        limits.checkpoint.inline_bytes = 64;
        limits
    });
    cluster.elect(1, &[]);
    cluster.received_claim(1);
    let second = cluster.creation(2);
    cluster.commit(1, PARTIES.issuer, second, &[3]);
    cluster.node(1).begin_checkpoint().unwrap();
    cluster.pump(&[3]);
    assert!(!cluster.node(1).checkpoint_pending());
    let third = cluster.creation(3);
    cluster.commit(1, PARTIES.issuer, third, &[3]);
    assert_eq!(cluster.sequence(3), SessionSeq(3));
    // The follower receives the snapshot but holds none of its chunks: the
    // delivery is retained (a retryable refusal) and the missing chunks are
    // named.
    cluster.retry_ok = vec![3];
    cluster.settle(&[]);
    assert_eq!(cluster.sequence(3), SessionSeq(3));
    let missing = cluster
        .node(3)
        .pending_seed()
        .expect("a seeded checkpoint waits for its chunks")
        .missing_chunks()
        .unwrap();
    assert!(!missing.is_empty());
    assert!(cluster.node(2).pending_seed().is_none());
    // A peer that holds the chunks serves them; each is verified on the way in.
    let reader = cluster.node(1).seed_reader();
    for hash in &missing {
        let bytes = reader
            .read(*hash, focal_evidence::SEED_CHUNK_BYTES)
            .unwrap();
        assert!(matches!(
            cluster.node(3).install_seed_chunk(*hash, b"forged"),
            Err(NativeSessionError::Native(_))
        ));
        cluster.node(3).install_seed_chunk(*hash, &bytes).unwrap();
    }
    cluster.retry_ok.clear();
    cluster.settle(&[]);
    assert!(cluster.node(3).pending_seed().is_none());
    assert_eq!(cluster.sequence(3), SessionSeq(5));
    assert_same_digest(&mut cluster, &[1, 2, 3]);
    assert_ne!(cluster.node(3).range(), RangeId(3));
    // The restored replica's own seeded checkpoint restores it on restart.
    cluster.node(3).begin_checkpoint().unwrap();
    cluster.pump(&[]);
    cluster.stop(3);
    cluster.reopen(3).unwrap();
    assert_eq!(cluster.sequence(3), SessionSeq(5));
    assert_same_digest(&mut cluster, &[1, 2, 3]);
    let fourth = cluster.creation(4);
    cluster.commit(1, PARTIES.issuer, fourth, &[]);
    assert_same_digest(&mut cluster, &[1, 2, 3, 4]);
}

/// The encoded Core root's size, as the checkpoint would seed it.
fn core_bytes(node: &Node) -> usize {
    let limits = crate::native_checkpoint::Limits::default();
    focal_core::native::record_codec::checkpoint::EncodingPlan::prepare(
        node.committed_core().unwrap(),
        focal_core::native::record_codec::EncodingLimits {
            bytes: limits.assembled_bytes,
            visits: limits.visits,
            rows: limits.rows,
        },
    )
    .unwrap()
    .quote()
    .bytes
}

#[test]
fn a_checkpoint_beyond_one_seed_chunk_installs_only_when_every_chunk_is_local() {
    let mut cluster = Cluster::with_limits(|_| {
        let mut limits = limits();
        limits.checkpoint.inline_bytes = 64;
        limits
    });
    cluster.elect(1, &[]);
    // Grow the leader's Core past one seed chunk while follower 3 lags.
    let mut claim = 1u128;
    let bytes = loop {
        for _ in 0..64 {
            let input = cluster.creation(claim);
            cluster.commit(1, PARTIES.issuer, input, &[3]);
            claim += 1;
        }
        let bytes = core_bytes(cluster.node(1));
        if bytes > focal_evidence::SEED_CHUNK_BYTES + 64 * 1024 {
            break bytes;
        }
        assert!(
            claim < 40_000,
            "the Core grew to only {bytes} bytes after {claim} claims"
        );
    };
    cluster.node(1).begin_checkpoint().unwrap();
    cluster.pump(&[3]);
    assert!(!cluster.node(1).checkpoint_pending());
    let after = cluster.creation(claim);
    cluster.commit(1, PARTIES.issuer, after, &[3]);
    // The snapshot names one chunk per MiB of the root; every one is missing.
    cluster.retry_ok = vec![3];
    cluster.settle(&[]);
    let missing = cluster
        .node(3)
        .pending_seed()
        .expect("a seeded checkpoint waits for its chunks")
        .missing_chunks()
        .unwrap();
    assert_eq!(
        missing.len(),
        bytes.div_ceil(focal_evidence::SEED_CHUNK_BYTES)
    );
    assert!(missing.len() >= 2, "{} bytes", bytes);
    // Every chunk but the last is exactly one chunk long, and the delivery
    // stays retained (naming only the last chunk) until that one arrives.
    let reader = cluster.node(1).seed_reader();
    let mut lengths = Vec::new();
    for (index, hash) in missing.iter().enumerate() {
        let chunk = reader
            .read(*hash, focal_evidence::SEED_CHUNK_BYTES)
            .unwrap();
        lengths.push(chunk.len());
        if index + 1 == missing.len() {
            cluster.settle(&[]);
            let remaining = cluster
                .node(3)
                .pending_seed()
                .unwrap()
                .missing_chunks()
                .unwrap();
            assert_eq!(remaining, vec![*hash]);
        }
        cluster.node(3).install_seed_chunk(*hash, &chunk).unwrap();
    }
    assert!(
        lengths[..lengths.len() - 1]
            .iter()
            .all(|length| *length == focal_evidence::SEED_CHUNK_BYTES)
    );
    assert_eq!(lengths.iter().sum::<usize>(), bytes);
    cluster.retry_ok.clear();
    cluster.settle(&[]);
    assert!(cluster.node(3).pending_seed().is_none());
    let leader = cluster.sequence(1);
    assert_eq!(cluster.sequence(3), leader);
    assert_same_digest(&mut cluster, &[1, claim]);
}

/// Movement in the log (25 §6): every replica applies the same steps to the
/// same coordinator state; the fences hold between the barrier and the
/// activation; a lagging follower installs the checkpoint that carries the
/// pending transfer; a restart resumes from the committed step; a forged
/// proof is refused before it is proposed; activation moves the map to the
/// next epoch under the replica holder and reopens admission.
#[test]
fn a_member_moves_under_one_authoritative_decision_and_faults_at_every_barrier_recover() {
    use focal_model::RouteEpoch;
    use focal_ranges::{
        DestinationReady, Holder, KeySpan, Placement, RangeDescriptor, RangeError, RangeIntent,
        RangeOperation, RecoveryProof, ReplicaId, TransferId,
    };
    use std::collections::BTreeSet;
    let mut cluster = Cluster::new();
    cluster.elect(1, &[]);
    cluster.received_claim(1);
    // The genesis map: one member, held by the voters, at range epoch one.
    let map = cluster.node(1).range_map().unwrap().clone();
    assert_eq!(map.epoch(), RouteEpoch(1));
    assert_eq!(map.ranges().len(), 1);
    let member = map.ranges()[0].id;
    assert_eq!(map.ranges()[0].meta.owner, Holder::Voters);
    assert!(cluster.node(1).movement_pending().unwrap().is_none());
    let replica = ReplicaId {
        node: 3,
        generation: 1,
    };
    let target = RangeId::from_u128(77);
    let operation = TransferId::from_u128(1);
    let intent = RangeIntent {
        ledger: ledger(),
        operation,
        old_epoch: RouteEpoch(1),
        sources: BTreeSet::from([member]),
        replacements: vec![RangeDescriptor {
            id: target,
            generation: 1,
            span: KeySpan::all(),
            meta: Placement::replica(replica),
        }],
        seed: cluster.sequence(1),
    };
    // A follower cannot propose; the authority's step is in flight until it
    // applies, and nothing else moves meanwhile.
    assert!(matches!(
        cluster
            .node(2)
            .propose_range(RangeOperation::Begin(intent.clone())),
        Err(NativeSessionError::NotReady { .. })
    ));
    cluster
        .node(1)
        .propose_range(RangeOperation::Begin(intent.clone()))
        .unwrap();
    assert!(cluster.node(1).movement_in_flight());
    assert!(matches!(
        cluster
            .node(1)
            .propose_range(RangeOperation::Barrier { operation }),
        Err(NativeSessionError::RangeMoving)
    ));
    cluster.pump(&[3]);
    assert!(!cluster.node(1).movement_in_flight());
    for id in [1, 2] {
        let pending = cluster.node(id).movement_pending().unwrap().unwrap();
        assert_eq!(pending.intent, intent);
        assert!(pending.barrier.is_none());
        assert_eq!(
            cluster
                .node(id)
                .movement_checkpoint()
                .unwrap()
                .control_ordinal,
            1
        );
    }
    // A layout change waits for the transfer; native records still commit
    // before the barrier.
    assert!(matches!(
        cluster.node(1).propose_layout(LayoutOperation::Split {
            at: [8; 16],
            id: RangeId::from_u128(5),
        }),
        Err(NativeSessionError::RangeMoving)
    ));
    let second = cluster.creation(2);
    cluster.commit(1, PARTIES.issuer, second, &[3]);
    // The replica-held replacement's seed, then the barrier.
    cluster
        .node(1)
        .propose_range(RangeOperation::Snapshot {
            operation,
            range: target,
            hash: ContentHash([5; 32]),
        })
        .unwrap();
    cluster.pump(&[3]);
    cluster
        .node(1)
        .propose_range(RangeOperation::Barrier { operation })
        .unwrap();
    cluster.pump(&[3]);
    let barrier = cluster
        .node(1)
        .movement_pending()
        .unwrap()
        .unwrap()
        .barrier
        .clone()
        .expect("barrier committed");
    assert_eq!(barrier.sequence, cluster.sequence(1));
    assert_eq!(barrier.ordinal, 3);
    // Between the barrier and the activation a mutation on the moving member
    // is refused and retried later, and the transfer cannot be abandoned.
    let fenced = cluster.creation(3);
    cluster.clock = cluster.clock.saturating_add(1);
    let clock = cluster.clock;
    assert!(matches!(
        cluster
            .node(1)
            .propose(fx::context(PARTIES.issuer, clock), fenced),
        Err(NativeSessionError::RangeMoving)
    ));
    assert_eq!(cluster.node(1).pending_count(), 0);
    assert!(matches!(
        cluster
            .node(1)
            .propose_range(RangeOperation::Abort { operation }),
        Err(NativeSessionError::Range(RangeError::Sealed))
    ));
    // The lagging follower installs the checkpoint that carries the pending
    // transfer and resumes from the committed step.
    cluster.node(1).begin_checkpoint().unwrap();
    cluster.pump(&[3]);
    assert!(!cluster.node(1).checkpoint_pending());
    cluster.settle(&[]);
    assert_eq!(cluster.sequence(3), cluster.sequence(1));
    let expected = cluster.node(1).movement_checkpoint().unwrap().clone();
    assert_eq!(*cluster.node(3).movement_checkpoint().unwrap(), expected);
    assert_eq!(*cluster.node(2).movement_checkpoint().unwrap(), expected);
    // A forged readiness proof is refused before it is proposed; the attested
    // one applies on every replica.
    let mut ready = DestinationReady {
        ledger: ledger(),
        operation,
        new_epoch: RouteEpoch(2),
        range: target,
        range_generation: 1,
        replica,
        seed: intent.seed,
        through: barrier.sequence,
        snapshot: ContentHash([5; 32]),
        state: ContentHash([6; 32]),
        checkpoint: ContentHash([7; 32]),
        attestation: ContentHash([9; 32]),
    };
    assert!(matches!(
        cluster
            .node(1)
            .propose_range(RangeOperation::Ready(ready.clone())),
        Err(NativeSessionError::Range(RangeError::Unverified))
    ));
    let verifier = cluster.node(1).range_verifier().unwrap();
    verifier.attest_ready(&mut ready).unwrap();
    cluster
        .node(1)
        .propose_range(RangeOperation::Ready(ready.clone()))
        .unwrap();
    cluster.pump(&[]);
    assert_eq!(
        cluster
            .node(3)
            .movement_pending()
            .unwrap()
            .unwrap()
            .ready
            .get(&target),
        Some(&ready)
    );
    // Activation: the map moves to the next epoch under the replica holder,
    // admission reopens, and every replica agrees.
    let activate = cluster.node(1).range_activation_operation(vec![]).unwrap();
    cluster.node(1).propose_range(activate).unwrap();
    cluster.pump(&[]);
    for id in [1, 2, 3] {
        let map = cluster.node(id).range_map().unwrap();
        assert_eq!(map.epoch(), RouteEpoch(2));
        assert_eq!(map.ranges().len(), 1);
        assert_eq!(map.ranges()[0].id, target);
        assert_eq!(map.ranges()[0].meta.owner, Holder::Replica(replica));
        assert!(cluster.node(id).movement_pending().unwrap().is_none());
        assert!(cluster.node(id).range_activation(operation).is_some());
        assert_eq!(cluster.node(id).movement_refusals(), 0);
    }
    let fourth = cluster.creation(4);
    cluster.commit(1, PARTIES.issuer, fourth, &[]);
    assert_same_digest(&mut cluster, &[1, 2, 4]);
    // Cleanup retires the previous map once the recovery proof is attested.
    let mut recovery = RecoveryProof {
        ledger: ledger(),
        epoch: RouteEpoch(2),
        through: cluster.sequence(1),
        manifest: ContentHash([3; 32]),
        attestation: ContentHash([0; 32]),
    };
    verifier.attest_recovery(&mut recovery).unwrap();
    cluster
        .node(1)
        .propose_range(RangeOperation::Cleanup {
            operation,
            recovery,
        })
        .unwrap();
    cluster.pump(&[]);
    for id in [1, 2, 3] {
        assert!(cluster.node(id).range_activation(operation).is_none());
        assert!(
            cluster
                .node(id)
                .movement_checkpoint()
                .unwrap()
                .history
                .is_empty()
        );
    }
    // The member moves back to the voters: no seed, readiness or seal is
    // demanded of the log itself. A replica restarts in the middle and
    // resumes from the committed step; a new authority finishes the move.
    let back = TransferId::from_u128(2);
    let home = RangeId::from_u128(88);
    let intent = RangeIntent {
        ledger: ledger(),
        operation: back,
        old_epoch: RouteEpoch(2),
        sources: BTreeSet::from([target]),
        replacements: vec![RangeDescriptor {
            id: home,
            generation: 1,
            span: KeySpan::all(),
            meta: Placement::voters(),
        }],
        seed: cluster.sequence(1),
    };
    cluster
        .node(1)
        .propose_range(RangeOperation::Begin(intent.clone()))
        .unwrap();
    cluster.pump(&[]);
    cluster.stop(2);
    cluster.reopen(2).unwrap();
    assert_eq!(
        cluster.node(2).movement_pending().unwrap().unwrap().intent,
        intent
    );
    cluster
        .node(1)
        .propose_range(RangeOperation::Barrier { operation: back })
        .unwrap();
    cluster.pump(&[]);
    // The source is replica-held: its seal is demanded before activation.
    let premature = cluster.node(1).range_activation_operation(vec![]).unwrap();
    assert!(matches!(
        cluster.node(1).propose_range(premature),
        Err(NativeSessionError::Range(RangeError::NotReady))
    ));
    let cut = cluster
        .node(1)
        .movement_pending()
        .unwrap()
        .unwrap()
        .barrier
        .clone()
        .unwrap()
        .sequence;
    let mut seal = focal_ranges::SourceSealProof {
        ledger: ledger(),
        operation: back,
        old_epoch: RouteEpoch(2),
        range: target,
        range_generation: 1,
        replica,
        cut,
        checkpoint: ContentHash([4; 32]),
        attestation: ContentHash([0; 32]),
    };
    verifier.attest_source_seal(&mut seal).unwrap();
    cluster
        .node(1)
        .propose_range(RangeOperation::SourceSealed(seal))
        .unwrap();
    cluster.pump(&[]);
    cluster.elect(2, &[]);
    let activate = cluster.node(2).range_activation_operation(vec![]).unwrap();
    cluster.node(2).propose_range(activate).unwrap();
    cluster.pump(&[]);
    for id in [1, 2, 3] {
        let map = cluster.node(id).range_map().unwrap();
        assert_eq!(map.epoch(), RouteEpoch(3));
        assert_eq!(map.ranges()[0].id, home);
        assert_eq!(map.ranges()[0].meta.owner, Holder::Voters);
        assert_eq!(
            cluster
                .node(id)
                .movement_checkpoint()
                .unwrap()
                .control_ordinal,
            10
        );
    }
    let fifth = cluster.creation(5);
    cluster.commit(2, PARTIES.issuer, fifth, &[]);
    assert_same_digest(&mut cluster, &[1, 2, 4, 5]);
    // A layout change now re-lays the map under the next range epoch, with
    // the new member carrying its parent's placement.
    cluster
        .node(2)
        .propose_layout(LayoutOperation::Split {
            at: [8; 16],
            id: RangeId::from_u128(5),
        })
        .unwrap();
    cluster.pump(&[]);
    assert_same_layout(&mut cluster, 1);
    for id in [1, 2, 3] {
        let map = cluster.node(id).range_map().unwrap();
        assert_eq!(map.epoch(), RouteEpoch(4));
        assert_eq!(map.ranges().len(), 2);
        assert!(
            map.ranges()
                .iter()
                .all(|range| range.meta.owner == Holder::Voters)
        );
    }
}

/// The archive's report bounds the retention floor (26 §3): recorded on a
/// replica, it never regresses, rides that replica's checkpoint to a
/// lagging follower, and survives a restart from it.
#[test]
fn the_archives_report_is_monotone_and_rides_checkpoints() {
    let mut cluster = Cluster::new();
    cluster.elect(1, &[]);
    cluster.received_claim(1);
    assert_eq!(cluster.node(1).archived_through(), SessionSeq(0));
    cluster.node(1).note_archived(SessionSeq(2));
    cluster.node(1).note_archived(SessionSeq(1));
    assert_eq!(cluster.node(1).archived_through(), SessionSeq(2));
    assert_eq!(cluster.node(3).archived_through(), SessionSeq(0));
    let second = cluster.creation(2);
    cluster.commit(1, PARTIES.issuer, second, &[3]);
    cluster.node(1).begin_checkpoint().unwrap();
    cluster.pump(&[3]);
    assert!(!cluster.node(1).checkpoint_pending());
    let third = cluster.creation(3);
    cluster.commit(1, PARTIES.issuer, third, &[3]);
    cluster.settle(&[]);
    assert_eq!(cluster.sequence(3), cluster.sequence(1));
    assert_eq!(cluster.node(3).archived_through(), SessionSeq(2));
    assert_eq!(cluster.node(2).archived_through(), SessionSeq(0));
    cluster.node(3).note_archived(SessionSeq(3));
    cluster.node(3).begin_checkpoint().unwrap();
    cluster.pump(&[]);
    cluster.stop(3);
    cluster.reopen(3).unwrap();
    assert_eq!(cluster.node(3).archived_through(), SessionSeq(3));
    cluster.assert_same_state(&[1, 2, 3]);
}

/// Retirement is a session decision (26 §4): the authority derives a
/// terminal, released family from the committed state, seals its bundle
/// and commits a record naming the root, the bundle and the prefix; every
/// replica derives the same family and retires it alike behind its
/// continuation, keeping the outcome rows so an exact retry of the retired
/// claim's creation still finds its outcome. Only the authority proposes,
/// an ineligible family is refused before proposal, a record in flight
/// fences native proposals, a lagging follower restores the retired state
/// from a checkpoint that carries the count, and a restart keeps it.
#[test]
fn committed_retirements_apply_on_every_replica_and_fence_proposals() {
    use focal_core::native::NativeCommand;
    use focal_core::native::retirement::RetirementRefusal;
    let mut cluster = Cluster::new();
    cluster.elect(1, &[]);
    let create = cluster.creation(1);
    let create_request = create.request;
    let created = cluster.commit(1, PARTIES.issuer, create, &[]);
    let expected = cluster.claim(1, 1);
    let cancel = fx::cancel(cluster.next(PARTIES.issuer), expected);
    cluster.commit(1, PARTIES.issuer, cancel, &[]);
    // A terminal claim whose scope is still held is not a family yet.
    assert!(matches!(
        cluster.node(1).propose_retirement(
            ClaimId::from_u128(1),
            ContentHash([9; 32]),
            1,
            SessionSeq(2)
        ),
        Err(NativeSessionError::Retirement(
            RetirementRefusal::NotReleased(_)
        ))
    ));
    let expected = cluster.claim(1, 1);
    let release = NativeInput {
        request: cluster.next(PARTIES.issuer),
        command: NativeCommand::ReleaseScope { expected },
    };
    cluster.commit(1, PARTIES.issuer, release, &[]);
    let second = cluster.creation(2);
    cluster.commit(1, PARTIES.issuer, second, &[]);
    // The bundle is derived from the committed state.
    let (root, bundle, length, through, rows) = {
        let core = cluster.node(1).committed_core().unwrap();
        let family = core.retirement_family(ClaimId::from_u128(1)).unwrap();
        let through = core.native_sequence();
        let quote = core
            .archive_family_quote(&family, through, limits().encoding)
            .unwrap();
        let mut bytes = vec![0; quote.bytes];
        let bundle = core
            .archive_family_into(&family, through, &mut bytes, quote.visits)
            .unwrap();
        assert!(bytes.starts_with(b"FCNARCHV"));
        (
            family.root,
            bundle,
            bytes.len() as u64,
            through,
            family.rows(),
        )
    };
    assert!(rows > 0);
    // Only the authority proposes; a live family is refused before proposal.
    assert!(matches!(
        cluster
            .node(2)
            .propose_retirement(root, bundle, length, through),
        Err(NativeSessionError::NotReady { .. })
    ));
    assert!(matches!(
        cluster
            .node(1)
            .propose_retirement(ClaimId::from_u128(2), bundle, length, through),
        Err(NativeSessionError::Retirement(
            RetirementRefusal::NotTerminal(_)
        ))
    ));
    // A bundle claiming less than the family's last event is refused.
    assert!(matches!(
        cluster
            .node(1)
            .propose_retirement(root, bundle, length, SessionSeq(through.0 - 2)),
        Err(NativeSessionError::Native(NativeError::Contract(
            ContractError::InvalidManifest
        )))
    ));
    // Pending candidates hold the rows a retirement takes: it waits.
    let third = cluster.creation(3);
    let request = third.request;
    let NativeSubmission::Pending { outcome, .. } = cluster.propose(1, PARTIES.issuer, third)
    else {
        panic!("a fresh proposal is pending until the log commits it");
    };
    assert!(matches!(
        cluster
            .node(1)
            .propose_retirement(root, bundle, length, through),
        Err(NativeSessionError::Capacity)
    ));
    for _ in 0..8 {
        cluster.pump(&[]);
        if cluster.node(1).outcome(request).unwrap() == Some(outcome) {
            break;
        }
    }
    assert_eq!(cluster.node(1).outcome(request).unwrap(), Some(outcome));
    let (root, bundle, length, through) = {
        let core = cluster.node(1).committed_core().unwrap();
        let family = core.retirement_family(ClaimId::from_u128(1)).unwrap();
        let through = core.native_sequence();
        let quote = core
            .archive_family_quote(&family, through, limits().encoding)
            .unwrap();
        (family.root, quote.hash, quote.bytes as u64, through)
    };
    let before = cluster.sequence(1);
    // A record in flight fences native proposals and further decisions.
    cluster
        .node(1)
        .propose_retirement(root, bundle, length, through)
        .unwrap();
    assert!(cluster.node(1).retirement_in_flight().is_some());
    assert!(matches!(
        cluster
            .node(1)
            .propose_retirement(root, bundle, length, through),
        Err(NativeSessionError::Retiring)
    ));
    let fourth = cluster.creation(4);
    cluster.clock = cluster.clock.saturating_add(1);
    let clock = cluster.clock;
    assert!(matches!(
        cluster
            .node(1)
            .propose(fx::context(PARTIES.issuer, clock), fourth),
        Err(NativeSessionError::Retiring)
    ));
    assert!(matches!(
        cluster.node(1).propose_layout(LayoutOperation::Split {
            at: ClaimId::from_u128(2).0,
            id: RangeId(32),
        }),
        Err(NativeSessionError::Retiring)
    ));
    cluster.pump(&[]);
    assert!(cluster.node(1).retirement_in_flight().is_none());
    // The authority applied a record it did not author through its
    // committed core; it is reconstructed at the next readiness barrier.
    cluster.settle(&[]);
    // Every replica retired the same family at the same prefix.
    assert_eq!(cluster.sequence(1), SessionSeq(before.0 + 1));
    assert_same_digest(&mut cluster, &[1, 2, 3]);
    for id in 1..=3u64 {
        assert_eq!(cluster.status(id, 1), None, "node {id}");
        assert_eq!(cluster.node(id).retired_families(), 1, "node {id}");
        let core = cluster.node(id).committed_core().unwrap();
        let continuation = core.native_retired(ClaimId::from_u128(1)).unwrap();
        assert_eq!(continuation.bundle, bundle);
        assert_eq!(continuation.bytes, length);
        assert_eq!(continuation.through, through);
        assert_eq!(continuation.retired_at, SessionSeq(before.0 + 1));
        assert!(
            core.native_outcome(focal_core::native::NativeInvocation::Retirement(root))
                .is_some()
        );
    }
    // The outcome of the retired claim's creation stays: an exact retry of
    // that request is answered from it without a candidate.
    cluster.clock = cluster.clock.saturating_add(1);
    let clock = cluster.clock;
    assert_eq!(
        cluster
            .node(1)
            .propose(
                fx::context(PARTIES.issuer, clock),
                creation_for(create_request, 1)
            )
            .unwrap(),
        NativeSubmission::Committed(created)
    );
    // The same family cannot leave twice.
    assert!(matches!(
        cluster
            .node(1)
            .propose_retirement(root, bundle, length, through),
        Err(NativeSessionError::Retirement(RetirementRefusal::Unknown(
            _
        )))
    ));
    // Records flow again; a lagging follower installs a checkpoint carrying
    // the retired state and the count, then replays the records after it.
    let fifth = cluster.creation(5);
    cluster.commit(1, PARTIES.issuer, fifth, &[3]);
    cluster.node(1).begin_checkpoint().unwrap();
    cluster.pump(&[3]);
    assert!(!cluster.node(1).checkpoint_pending());
    let sixth = cluster.creation(6);
    cluster.commit(1, PARTIES.issuer, sixth, &[3]);
    cluster.settle(&[]);
    assert_eq!(cluster.sequence(3), cluster.sequence(1));
    assert_eq!(cluster.node(3).retired_families(), 1);
    assert_same_digest(&mut cluster, &[2, 3, 5, 6]);
    // A restart replays the record from its own log or checkpoint.
    cluster.stop(2);
    cluster.reopen(2).unwrap();
    cluster.settle(&[]);
    assert_eq!(cluster.node(2).retired_families(), 1);
    assert_eq!(cluster.status(2, 1), None);
    assert_same_digest(&mut cluster, &[2, 3, 5, 6]);
}
