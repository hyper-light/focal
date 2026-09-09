//! The unified Session hosting the native engine: a committed activation over
//! an empty legacy prefix, native work through the same Session, the SS6
//! checkpoint, restart, legacy refusal, and a replicated group where a replica
//! without native hosting refuses native history at ingress until it hosts it.
use super::*;
use crate::native_session::tests::{ledger, limits as native_limits, store};
use focal_consensus::{MessageType, SnapshotStatus};
use focal_core::native::fixtures as fx;
use focal_model::lifecycle::Binding;
use focal_model::{ClaimId, ClaimStatus, Confidence, OutcomeKind, ParticipantId};
use std::path::Path;

const PARTIES: fx::Parties = fx::Parties::numbered(81);
const CLUSTER: [u8; 16] = [7; 16];
const ELECTION_TICKS: usize = 10;

fn content_dir(dir: &Path, id: u64) -> std::path::PathBuf {
    dir.join(format!("content-{id}"))
}
fn hosting(dir: &Path, id: u64) -> NativeHosting {
    NativeHosting {
        limits: native_limits(),
        reader: ContentReader::open(content_dir(dir, id)).unwrap(),
        range: RangeId(u128::from(id)),
    }
}
/// The exclusive content writer of a node; the Session's reader shares its path.
fn writer(dir: &Path, id: u64) -> ContentStore {
    std::fs::create_dir_all(content_dir(dir, id)).unwrap();
    store(&content_dir(dir, id))
}
fn open(dir: &Path, id: u64, voters: Vec<u64>, hosted: bool) -> Session {
    let config = if voters.len() == 1 {
        NodeConfig::single(id, CLUSTER, ledger().session.0)
    } else {
        NodeConfig::joining(id, CLUSTER, ledger().session.0, voters, Vec::new())
    };
    let path = dir.join(format!("wal-{id}"));
    if hosted {
        Session::open_hosted(
            path,
            ledger(),
            config,
            SessionLimits::default(),
            hosting(dir, id),
        )
        .unwrap()
    } else {
        Session::open(path, ledger(), config, SessionLimits::default()).unwrap()
    }
}
fn context(actor: ParticipantId, logical_time: u64) -> NativeContext {
    fx::context(actor, logical_time)
}
fn creation(request: RequestKey, claim: u128) -> NativeInput {
    let declaration =
        fx::delivery_declaration(ledger(), PARTIES, claim, claim * 1000 + 300, 1000).unwrap();
    fx::creation(
        ledger(),
        PARTIES,
        request,
        claim,
        vec![declaration],
        &[fx::Slot {
            slot: 0,
            missing_declaration_index: 20,
            mode: ValidationMode::Required,
            checks: vec![],
        }],
    )
    .unwrap()
}
fn legacy_input(id: u128) -> AuthenticatedInput {
    AuthenticatedInput {
        ledger: ledger(),
        principal: PARTIES.issuer,
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(id),
        expected_revision: None,
        authority: AuthorityContext {
            runtime: true,
            cause: Cause::Root(RootCommandId::from_u128(1)),
            policy_revision: 1,
            logical_time: 0,
            evidence: vec![],
        },
        command: Command::NegotiateEpoch {
            epoch: RequestEpoch(1),
        },
    }
}

/// One node, or several, exchanging Raft messages in-process.
struct Cluster {
    dir: tempfile::TempDir,
    nodes: Vec<Option<Session>>,
    stores: Vec<ContentStore>,
    hosted: Vec<bool>,
    voters: Vec<u64>,
    clock: u64,
    serial: u128,
    /// Nodes expected to refuse or defer packets at ingress.
    refusals: Vec<(u64, String)>,
    /// Replicas whose host sealed legacy payloads for a pending import.
    sealed: Vec<u64>,
    /// Replicas whose host is not sealing yet: their import delivery stays retained.
    no_seal: Vec<u64>,
}
impl Cluster {
    fn new(size: u64, hosted: &[bool]) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let voters: Vec<u64> = (1..=size).collect();
        let stores: Vec<ContentStore> = (1..=size).map(|id| writer(dir.path(), id)).collect();
        let nodes = (1..=size)
            .map(|id| {
                Some(open(
                    dir.path(),
                    id,
                    voters.clone(),
                    hosted[id as usize - 1],
                ))
            })
            .collect();
        Self {
            dir,
            nodes,
            stores,
            hosted: hosted.to_vec(),
            voters,
            clock: 0,
            serial: 0,
            refusals: Vec::new(),
            sealed: Vec::new(),
            no_seal: Vec::new(),
        }
    }
    fn node(&mut self, id: u64) -> &mut Session {
        self.nodes[id as usize - 1].as_mut().unwrap()
    }
    fn live(&self) -> Vec<u64> {
        (1..=self.voters.len() as u64)
            .filter(|id| self.nodes[*id as usize - 1].is_some())
            .collect()
    }
    fn stop(&mut self, id: u64) {
        drop(self.nodes[id as usize - 1].take());
    }
    fn reopen(&mut self, id: u64, hosted: bool) {
        assert!(self.nodes[id as usize - 1].is_none());
        self.hosted[id as usize - 1] = hosted;
        let node = open(self.dir.path(), id, self.voters.clone(), hosted);
        self.nodes[id as usize - 1] = Some(node);
    }
    fn pump(&mut self, isolated: &[u64]) {
        for _ in 0..400 {
            if self.round(isolated) {
                return;
            }
        }
        panic!("message delivery failed to quiesce");
    }
    /// One delivery round: every live node polls once, then every produced
    /// message is delivered. Returns true when nothing happened.
    fn round(&mut self, isolated: &[u64]) -> bool {
        {
            let mut messages = Vec::new();
            let mut progressed = false;
            for id in self.live() {
                let node = self.node(id);
                match node.poll() {
                    Ok(events) => {
                        progressed |= !events.committed.is_empty()
                            || !events.native_committed.is_empty()
                            || !events.native_read_boundaries.is_empty();
                        messages.extend(events.messages);
                    }
                    Err(LedgerError::Retry) => {
                        // A replica whose host is not sealing yet makes no progress.
                        progressed |= !self.no_seal.contains(&id);
                        // A replica applying an import needs its inline legacy
                        // payloads sealed locally first; the host does that.
                        if !self.no_seal.contains(&id)
                            && self.nodes[id as usize - 1]
                                .as_ref()
                                .unwrap()
                                .pending_import()
                                .is_some()
                        {
                            let store = &mut self.stores[id as usize - 1];
                            self.nodes[id as usize - 1]
                                .as_mut()
                                .unwrap()
                                .seal_import_payloads(store)
                                .unwrap();
                            self.sealed.push(id);
                        }
                    }
                    Err(LedgerError::Consensus(ConsensusError::PersistencePending)) => {
                        progressed = true;
                    }
                    Err(error) => panic!("poll {id}: {error:?}"),
                }
            }
            if messages.is_empty() && !progressed {
                return true;
            }
            for message in messages {
                if isolated.contains(&message.from) || isolated.contains(&message.to) {
                    continue;
                }
                let (from, to) = (message.from, message.to);
                if !self.live().contains(&to) {
                    continue;
                }
                let snapshot = message.get_msg_type() == MessageType::MsgSnapshot;
                match self.node(to).step(message) {
                    Ok(()) => {}
                    Err(LedgerError::NativeUnsupported) => {
                        self.refusals.push((to, "unsupported".into()));
                        continue;
                    }
                    Err(LedgerError::Consensus(ConsensusError::PersistencePending)) => {
                        self.refusals.push((to, "pending".into()));
                        continue;
                    }
                    Err(error) => panic!("step into {to}: {error:?}"),
                }
                if snapshot && self.live().contains(&from) {
                    self.node(from)
                        .report_snapshot(to, SnapshotStatus::Finish)
                        .unwrap();
                }
            }
        }
        false
    }
    fn settle(&mut self, isolated: &[u64]) {
        for _ in 0..8 {
            for id in self.live() {
                if isolated.contains(&id) {
                    continue;
                }
                // A staged decoder floor write blocks Raft actions until the
                // next poll persists it; ticks resume afterwards.
                match self.node(id).tick() {
                    Ok(()) | Err(LedgerError::Consensus(ConsensusError::PersistencePending)) => {}
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
    fn elect(&mut self, id: u64, isolated: &[u64]) {
        match self.authority(isolated) {
            Some(current) if current == id => {}
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
        for _ in 0..8 {
            self.settle(isolated);
            if self.node(id).is_authoritative() {
                return;
            }
        }
        panic!("node {id} did not become authoritative");
    }
    /// The node-side support driver, in-process: every hosted replica promises
    /// the successor floor and every replica records what its peers advertise.
    fn exchange_support(&mut self, isolated: &[u64]) {
        let live: Vec<u64> = self
            .live()
            .into_iter()
            .filter(|id| !isolated.contains(id))
            .collect();
        for &id in &live {
            let _ = self.node(id).begin_native_support();
        }
        self.settle(isolated);
        for &peer in &live {
            let Ok(fact) = self.node(peer).native_support() else {
                continue;
            };
            for &recorder in &live {
                if recorder != peer {
                    let _ = self
                        .node(recorder)
                        .record_managed_support(peer, fact.clone());
                }
            }
        }
    }
    /// Activate native history from the authority and wait until it hosts
    /// native admission (activation applied, genesis committed, owner ready).
    fn activate(&mut self, id: u64, isolated: &[u64]) {
        let mut proposed = false;
        for _ in 0..64 {
            match self
                .node(id)
                .propose_native_activation(NativeContentProfile::ProjectionOnly)
            {
                Ok(()) => {
                    proposed = true;
                    break;
                }
                Err(LedgerError::Consensus(ConsensusError::PersistencePending)) => {
                    self.settle(isolated);
                }
                Err(LedgerError::Managed(ManagedError::Unsupported)) => {
                    // A voter has not promised the successor yet.
                    self.exchange_support(isolated);
                }
                Err(error) => panic!("activation: {error:?}"),
            }
        }
        assert!(
            proposed,
            "every voter must promise before activation is proposed"
        );
        for _ in 0..16 {
            self.settle(isolated);
            if self.node(id).native_authoritative() {
                return;
            }
        }
        panic!("node {id} never hosted native admission");
    }
    /// Commit one legacy command through the authority and return its receipt.
    fn legacy(&mut self, id: u64, input: AuthenticatedInput) -> MutationReceipt {
        let key = RequestKey {
            principal: input.principal,
            epoch: input.request_epoch,
            id: input.request_id,
        };
        match self.node(id).propose(&input).unwrap() {
            Submission::Committed(receipt) => return receipt,
            Submission::Pending(_) => {}
            Submission::Domain(outcome) => panic!("legacy command refused: {outcome:?}"),
        }
        for _ in 0..8 {
            self.pump(&[]);
            if let Some(receipt) = self.node(id).receipt(&key) {
                return receipt.clone();
            }
        }
        panic!("legacy command never committed on node {id}");
    }
    /// Propose the import of the populated legacy prefix from the authority
    /// once every voter has promised; the record is in flight on return.
    fn propose_import(&mut self, id: u64, isolated: &[u64]) {
        for _ in 0..64 {
            self.clock += 1;
            let clock = self.clock;
            let store = &mut self.stores[id as usize - 1];
            let (chunk, manifest) = (store.upload_chunk_bytes(), store.max_manifest_bytes());
            match self.nodes[id as usize - 1]
                .as_mut()
                .unwrap()
                .propose_native_import(
                    NativeContentProfile::ProjectionOnly,
                    Some(store),
                    clock,
                    chunk,
                    manifest,
                ) {
                Ok(()) => return,
                Err(LedgerError::Consensus(ConsensusError::PersistencePending)) => {
                    self.settle(isolated);
                }
                Err(LedgerError::Managed(ManagedError::Unsupported)) => {
                    self.exchange_support(isolated);
                }
                Err(error) => panic!("import: {error:?}"),
            }
        }
        panic!("every voter must promise before import is proposed");
    }
    /// Whichever live node holds authority after a restart; elections are
    /// randomized, so the tests follow the authority instead of forcing one.
    fn ensure_authority(&mut self) -> u64 {
        for attempt in 0..24 {
            self.settle(&[]);
            if let Some(id) = self.authority(&[]) {
                return id;
            }
            if attempt % 6 == 5 {
                let lowest = self.live().into_iter().min().unwrap();
                self.elect(lowest, &[]);
            }
        }
        panic!("no authority after restart");
    }
    fn activation_index(&mut self, id: u64) -> Option<u64> {
        match self.node(id).activation() {
            LedgerActivation::Native { index, .. } => Some(index),
            LedgerActivation::V1 => None,
        }
    }
    /// Import the populated legacy prefix from the authority and wait until it
    /// hosts native admission.
    fn activate_import(&mut self, id: u64, isolated: &[u64]) {
        let mut proposed = false;
        for _ in 0..64 {
            self.clock += 1;
            let clock = self.clock;
            let store = &mut self.stores[id as usize - 1];
            let (chunk, manifest) = (store.upload_chunk_bytes(), store.max_manifest_bytes());
            match self.nodes[id as usize - 1]
                .as_mut()
                .unwrap()
                .propose_native_import(
                    NativeContentProfile::ProjectionOnly,
                    Some(store),
                    clock,
                    chunk,
                    manifest,
                ) {
                Ok(()) => {
                    proposed = true;
                    break;
                }
                Err(LedgerError::Consensus(ConsensusError::PersistencePending)) => {
                    self.settle(isolated);
                }
                Err(LedgerError::Managed(ManagedError::Unsupported)) => {
                    self.exchange_support(isolated);
                }
                Err(error) => panic!("import: {error:?}"),
            }
        }
        assert!(
            proposed,
            "every voter must promise before import is proposed"
        );
        // The legacy prefix is sealed while the record is in flight.
        assert!(matches!(
            self.node(id).propose(&legacy_input(999_999)),
            Err(LedgerError::Retry)
        ));
        for _ in 0..16 {
            self.settle(isolated);
            if self.node(id).native_authoritative() {
                return;
            }
        }
        panic!("node {id} never hosted native admission after import");
    }
    fn next(&mut self, actor: ParticipantId) -> RequestKey {
        self.serial += 1;
        fx::request(actor, 1, self.serial)
    }
    fn commit(
        &mut self,
        id: u64,
        actor: ParticipantId,
        input: NativeInput,
        isolated: &[u64],
    ) -> NativeOutcome {
        self.clock += 1;
        let request = input.request;
        let clock = self.clock;
        let store = &mut self.stores[id as usize - 1];
        let submission = self.nodes[id as usize - 1]
            .as_mut()
            .unwrap()
            .propose_native(context(actor, clock), input, NativeCustody::Store(store))
            .unwrap();
        let outcome = match submission {
            NativeSubmission::Committed(outcome) => return outcome,
            NativeSubmission::Pending { outcome, .. } => outcome,
        };
        for _ in 0..8 {
            self.pump(isolated);
            if self.node(id).native_outcome(request).unwrap() == Some(outcome) {
                return outcome;
            }
        }
        panic!("candidate never committed on node {id}");
    }
    fn claim(&mut self, id: u64, claim: u128) -> Binding {
        self.node(id)
            .native_core()
            .unwrap()
            .native_claim(ClaimId::from_u128(claim))
            .unwrap()
            .binding()
    }
    fn status(&mut self, id: u64, claim: u128) -> Option<ClaimStatus> {
        self.node(id)
            .native_core()
            .unwrap()
            .native_claim(ClaimId::from_u128(claim))
            .map(|claim| claim.status())
    }
    fn native_sequences(&mut self) -> Vec<(u64, Option<SessionSeq>)> {
        self.live()
            .into_iter()
            .map(|id| (id, self.node(id).native_sequence().ok()))
            .collect()
    }
}

#[test]
fn an_empty_ledger_activates_native_history_hosts_the_workflow_checkpoints_and_restarts() {
    let mut cluster = Cluster::new(1, &[true]);
    cluster.elect(1, &[]);
    assert_eq!(cluster.node(1).activation(), LedgerActivation::V1);
    assert!(!cluster.node(1).native_authoritative());
    assert!(matches!(
        cluster.node(1).native_core(),
        Err(LedgerError::NativeUnsupported)
    ));
    cluster.activate(1, &[]);
    assert!(matches!(
        cluster.node(1).activation(),
        LedgerActivation::Native {
            kind: ActivationKind::Genesis,
            profile: NativeContentProfile::ProjectionOnly,
            ..
        }
    ));
    assert!(cluster.node(1).native_support_ready());
    // Legacy admission is refused with a typed outcome once native history begins.
    assert!(matches!(
        cluster.node(1).propose(&legacy_input(1)).unwrap(),
        Submission::Domain(DomainOutcome::Refuse {
            code: ErrorCode::UnsupportedSchema,
            ..
        })
    ));
    assert!(matches!(
        cluster
            .node(1)
            .propose_native_activation(NativeContentProfile::ProjectionOnly),
        Err(LedgerError::ActivationConflict)
    ));
    let create = creation(cluster.next(PARTIES.issuer), 1);
    let key = create.request;
    let created = cluster.commit(1, PARTIES.issuer, create, &[]);
    let expected = cluster.claim(1, 1);
    let post = fx::post(cluster.next(PARTIES.issuer), expected);
    cluster.commit(1, PARTIES.issuer, post, &[]);
    let expected = cluster.claim(1, 1);
    let receipt = fx::acquire_receipt(cluster.next(PARTIES.subject), expected, 701);
    cluster.commit(1, PARTIES.subject, receipt, &[]);
    assert_eq!(cluster.status(1, 1), Some(ClaimStatus::Received));
    let parent = focal_model::lifecycle::evidence::Parent::from_claim(
        cluster
            .node(1)
            .native_core()
            .unwrap()
            .native_claim(ClaimId::from_u128(1))
            .unwrap(),
    )
    .unwrap();
    let (work, slot) = fx::work_artifact(ledger(), 801, &parent, 0, fx::PROOF).unwrap();
    let claim = cluster.claim(1, 1);
    let submit = fx::submit_work(cluster.next(PARTIES.subject), claim, 0, work);
    cluster.commit(1, PARTIES.subject, submit, &[]);
    let claim = cluster.claim(1, 1);
    let close = fx::close_response(
        ledger(),
        cluster.next(PARTIES.subject),
        claim,
        10,
        "done",
        Confidence::Committed,
        OutcomeKind::Complete,
        vec![slot],
        vec![],
    );
    cluster.commit(1, PARTIES.subject, close, &[]);
    assert_eq!(cluster.status(1, 1), Some(ClaimStatus::TestamentGenerated));
    let sequence = cluster.node(1).native_sequence().unwrap();
    assert_eq!(sequence, SessionSeq(5));
    // The enclosing checkpoint carries every ancillary section and the native root.
    cluster.node(1).checkpoint().unwrap();
    let applied = cluster.node(1).status().applied_index;
    cluster.stop(1);
    cluster.reopen(1, true);
    assert!(cluster.node(1).activation().is_native());
    assert_eq!(cluster.node(1).native_sequence().unwrap(), sequence);
    assert_eq!(cluster.node(1).status().applied_index, applied);
    assert_eq!(cluster.node(1).native_outcome(key).unwrap(), Some(created));
    assert_eq!(cluster.status(1, 1), Some(ClaimStatus::TestamentGenerated));
    cluster.elect(1, &[]);
    for _ in 0..8 {
        cluster.settle(&[]);
        if cluster.node(1).native_authoritative() {
            break;
        }
    }
    assert!(cluster.node(1).native_authoritative());
    cluster.clock += 1;
    let clock = cluster.clock;
    let store = &mut cluster.stores[0];
    assert_eq!(
        cluster.nodes[0]
            .as_mut()
            .unwrap()
            .propose_native(
                context(PARTIES.issuer, clock),
                creation(key, 1),
                NativeCustody::Store(store)
            )
            .unwrap(),
        NativeSubmission::Committed(created),
        "exact retry across restart"
    );
    let second = creation(cluster.next(PARTIES.issuer), 2);
    cluster.commit(1, PARTIES.issuer, second, &[]);
    assert_eq!(cluster.node(1).native_sequence().unwrap(), SessionSeq(6));
}

#[test]
fn a_voter_without_native_hosting_blocks_activation_and_a_downgraded_replica_cannot_open() {
    let mut cluster = Cluster::new(3, &[true, true, false]);
    cluster.elect(1, &[]);
    // The unhosted voter never promises the successor, so the barrier refuses.
    cluster.exchange_support(&[]);
    for _ in 0..4 {
        match cluster
            .node(1)
            .propose_native_activation(NativeContentProfile::ProjectionOnly)
        {
            Err(LedgerError::Consensus(ConsensusError::PersistencePending)) => cluster.settle(&[]),
            Err(LedgerError::Managed(ManagedError::Unsupported)) => break,
            other => panic!("activation without every voter's promise: {other:?}"),
        }
    }
    assert!(matches!(
        cluster
            .node(1)
            .propose_native_activation(NativeContentProfile::ProjectionOnly),
        Err(LedgerError::Managed(ManagedError::Unsupported))
    ));
    assert_eq!(cluster.node(1).activation(), LedgerActivation::V1);
    assert!(!cluster.node(3).native_hosted());
    // Once the replica hosts the engine it promises, and activation commits.
    cluster.stop(3);
    cluster.reopen(3, true);
    cluster.settle(&[]);
    cluster.activate(1, &[]);
    let create = creation(cluster.next(PARTIES.issuer), 1);
    let created = cluster.commit(1, PARTIES.issuer, create, &[]);
    cluster.settle(&[]);
    for id in [2, 3] {
        assert!(cluster.node(id).activation().is_native(), "replica {id}");
        assert_eq!(
            cluster.node(id).native_outcome(created.invocation).unwrap(),
            Some(created)
        );
    }
    // A downgraded binary cannot open a native ledger: recovery replays the
    // committed activation and refuses before touching any state.
    cluster.stop(3);
    let path = cluster.dir.path().join("wal-3");
    let config = NodeConfig::joining(3, CLUSTER, ledger().session.0, vec![1, 2, 3], Vec::new());
    assert!(matches!(
        Session::open(path, ledger(), config, SessionLimits::default()),
        Err(LedgerError::NativeUnsupported)
    ));
    // Hosting again recovers the same prefix and keeps following the group.
    cluster.reopen(3, true);
    assert!(cluster.node(3).activation().is_native());
    let second = creation(cluster.next(PARTIES.issuer), 2);
    cluster.commit(1, PARTIES.issuer, second, &[]);
    cluster.settle(&[]);
    let sequences = cluster.native_sequences();
    assert!(
        sequences.iter().all(|(_, seq)| *seq == Some(SessionSeq(2))),
        "{sequences:?}"
    );
}

#[test]
fn a_lagging_replica_installs_the_ss6_checkpoint_with_native_state() {
    let mut cluster = Cluster::new(3, &[true, true, true]);
    cluster.elect(1, &[]);
    cluster.activate(1, &[]);
    let first = creation(cluster.next(PARTIES.issuer), 1);
    cluster.commit(1, PARTIES.issuer, first, &[]);
    let second = creation(cluster.next(PARTIES.issuer), 2);
    cluster.commit(1, PARTIES.issuer, second, &[3]);
    cluster.node(1).checkpoint().unwrap();
    let third = creation(cluster.next(PARTIES.issuer), 3);
    cluster.commit(1, PARTIES.issuer, third, &[3]);
    assert_eq!(cluster.node(3).native_sequence().unwrap(), SessionSeq(1));
    cluster.settle(&[]);
    let sequences = cluster.native_sequences();
    assert!(
        sequences.iter().all(|(_, seq)| *seq == Some(SessionSeq(3))),
        "{sequences:?}"
    );
    assert_eq!(cluster.status(3, 3), Some(ClaimStatus::Generated));
    assert!(cluster.node(3).activation().is_native());
    // The restored replica takes authority by planned handover and admits work.
    cluster.elect(3, &[]);
    for _ in 0..8 {
        cluster.settle(&[]);
        if cluster.node(3).native_authoritative() {
            break;
        }
    }
    let fourth = creation(cluster.next(PARTIES.issuer), 4);
    cluster.commit(3, PARTIES.issuer, fourth, &[]);
    let sequences = cluster.native_sequences();
    assert!(
        sequences.iter().all(|(_, seq)| *seq == Some(SessionSeq(4))),
        "{sequences:?}"
    );
}

mod legacy {
    use super::*;
    use focal_model::{
        ArtifactContent, ArtifactPayload, CanonicalContent, ClaimContent, EvidenceAttestation,
        EvidenceSetId, NewArtifact, NewClaim, NewValidation, Relation, RelationKind,
        RelationTarget, RequirementRef, TestamentId, ValidationContent, ValidationKind,
        ValidationPhase,
    };
    use std::collections::BTreeSet;

    pub(super) const ISSUER: ParticipantId = ParticipantId::from_u128(91);
    pub(super) const WORKER: ParticipantId = ParticipantId::from_u128(92);
    pub(super) const EVALUATOR: ParticipantId = ParticipantId::from_u128(93);

    pub(super) fn input(n: u128, actor: ParticipantId, command: Command) -> AuthenticatedInput {
        AuthenticatedInput {
            ledger: ledger(),
            principal: actor,
            request_epoch: RequestEpoch(1),
            request_id: RequestId::from_u128(n),
            expected_revision: None,
            authority: AuthorityContext {
                runtime: true,
                cause: Cause::Root(RootCommandId::from_u128(30)),
                policy_revision: 1,
                logical_time: 1000,
                evidence: Vec::new(),
            },
            command,
        }
    }
    pub(super) fn new_claim(id: u128) -> NewClaim {
        let cid = ClaimId::from_u128(id);
        let vid = focal_model::ValidationId::from_u128(id + 100_000);
        let validation = ValidationContent {
            ledger: ledger(),
            schema: focal_model::SCHEMA_MAJOR,
            claim: cid,
            kind: ValidationKind::Receipt,
            phase: ValidationPhase::WholeWork,
            mode: ValidationMode::Required,
            description: "acknowledged receipt".into(),
            quality_bar: None,
            evaluator: EVALUATOR,
            handlers: Vec::new(),
            evidence_schemas: BTreeSet::new(),
            contributed_by: BTreeSet::from([ISSUER]),
            policy_revision: 1,
        };
        NewClaim {
            id: cid,
            content: ClaimContent {
                ledger: ledger(),
                schema: focal_model::SCHEMA_MAJOR,
                occurrence: focal_model::OccurrenceId::from_u128(id),
                description: "legacy work".into(),
                relations: BTreeSet::from([
                    Relation {
                        kind: RelationKind::Issuer,
                        target: RelationTarget::Participant(ISSUER),
                    },
                    Relation {
                        kind: RelationKind::Subject,
                        target: RelationTarget::Participant(WORKER),
                    },
                    Relation {
                        kind: RelationKind::ClaimAction,
                        target: RelationTarget::Action(focal_model::ActionType::Work),
                    },
                    Relation {
                        kind: RelationKind::CausedBy,
                        target: RelationTarget::Root(RootCommandId::from_u128(30)),
                    },
                ]),
                scopes: BTreeSet::new(),
                requirements: vec![RequirementRef {
                    id: vid,
                    specification: validation.specification_hash().unwrap(),
                }],
                deadline: None,
            },
            validations: vec![NewValidation {
                id: vid,
                content: validation,
            }],
        }
    }
    pub(super) fn artifact(id: u128, receipt: ReceiptFence) -> NewArtifact {
        NewArtifact {
            id: focal_model::ArtifactId::from_u128(id),
            content: ArtifactContent {
                ledger: ledger(),
                schema: focal_model::SCHEMA_MAJOR,
                kind: "test_report".into(),
                schema_hash: focal_evidence::test_report_schema(),
                metadata: Vec::new(),
                payload: ArtifactPayload::Inline(fx::PROOF.to_vec()),
                producer: WORKER,
                receipt: Some(receipt),
                inputs: BTreeSet::new(),
                visibility: BTreeSet::new(),
            },
        }
    }
    pub(super) fn attested(
        mut request: AuthenticatedInput,
        artifact: &NewArtifact,
    ) -> AuthenticatedInput {
        request.authority.evidence.push(EvidenceAttestation {
            descriptor_hash: artifact.content.content_hash().unwrap(),
            custody_revision: 1,
            durable: true,
            schema_valid: true,
        });
        request
    }
    /// A satisfied claim with an inline artifact and a closed, acknowledged
    /// testament, plus a cancelled one: enough to exercise every legacy row.
    pub(super) fn populate(cluster: &mut Cluster, id: u64) {
        for (i, principal) in [ISSUER, WORKER, EVALUATOR].into_iter().enumerate() {
            cluster.legacy(
                id,
                input(
                    900 + i as u128,
                    principal,
                    Command::NegotiateEpoch {
                        epoch: RequestEpoch(1),
                    },
                ),
            );
        }
        let claim = new_claim(1);
        let cid = claim.id;
        cluster.legacy(id, input(1_000, ISSUER, Command::GenerateClaim { claim }));
        cluster.legacy(id, input(1_001, ISSUER, Command::PostClaim { claim: cid }));
        cluster.legacy(
            id,
            input(
                1_002,
                WORKER,
                Command::AcquireReceipt {
                    claim: cid,
                    receipt: ReceiptId::from_u128(11),
                    epoch: 1,
                },
            ),
        );
        let fence = cluster
            .node(id)
            .read_at_least(SessionSeq(0))
            .unwrap()
            .claims[&cid]
            .lifecycle()
            .receipt
            .as_ref()
            .unwrap()
            .fence;
        let set = EvidenceSetId::from_u128(51);
        cluster.legacy(
            id,
            input(
                1_003,
                WORKER,
                Command::BeginEvidenceSet {
                    claim: cid,
                    receipt: fence,
                    evidence_set: set,
                },
            ),
        );
        let report = artifact(61, fence);
        let receipt = cluster.legacy(
            id,
            attested(
                input(
                    1_004,
                    WORKER,
                    Command::AttachArtifact {
                        claim: cid,
                        receipt: fence,
                        evidence_set: set,
                        artifact: report.clone(),
                    },
                ),
                &report,
            ),
        );
        let reference = match receipt.outcome {
            focal_model::CommandResult::Artifact(reference) => reference,
            other => panic!("{other:?}"),
        };
        cluster.legacy(
            id,
            input(
                1_005,
                WORKER,
                Command::CloseTestament {
                    claim: cid,
                    receipt: fence,
                    testament: TestamentId::from_u128(71),
                    evidence_set: set,
                    manifest: vec![reference],
                    summary: "finished".into(),
                    confidence: Confidence::Committed,
                    outcome: OutcomeKind::Complete,
                },
            ),
        );
        cluster.legacy(
            id,
            input(
                1_006,
                ISSUER,
                Command::AcknowledgeTestament {
                    claim: cid,
                    testament: TestamentId::from_u128(71),
                },
            ),
        );
        cluster.legacy(
            id,
            input(
                1_007,
                ISSUER,
                Command::BeginWholeWorkValidation { claim: cid },
            ),
        );
        cluster.legacy(
            id,
            input(1_008, ISSUER, Command::CompleteWholeWork { claim: cid }),
        );
        let other = new_claim(2);
        let oid = other.id;
        cluster.legacy(
            id,
            input(1_010, ISSUER, Command::GenerateClaim { claim: other }),
        );
        cluster.legacy(
            id,
            input(
                1_011,
                ISSUER,
                Command::CancelClaim {
                    claim: oid,
                    reason: "dropped".into(),
                },
            ),
        );
    }
}

#[test]
fn a_populated_legacy_ledger_imports_on_every_replica_keeps_legacy_reads_and_continues_natively() {
    let mut cluster = Cluster::new(3, &[true, true, true]);
    cluster.elect(1, &[]);
    legacy::populate(&mut cluster, 1);
    cluster.settle(&[]);
    let legacy_sequence = cluster
        .node(1)
        .read_at_least(SessionSeq(0))
        .unwrap()
        .sequence;
    assert!(legacy_sequence.0 >= 12);
    let legacy_receipt = cluster
        .node(1)
        .receipt(&RequestKey {
            principal: legacy::ISSUER,
            epoch: RequestEpoch(1),
            id: RequestId::from_u128(1_000),
        })
        .cloned()
        .unwrap();
    // A populated prefix refuses the genesis activation and demands import.
    cluster.exchange_support(&[]);
    for _ in 0..4 {
        match cluster
            .node(1)
            .propose_native_activation(NativeContentProfile::ProjectionOnly)
        {
            Err(LedgerError::Consensus(ConsensusError::PersistencePending))
            | Err(LedgerError::Managed(ManagedError::Unsupported)) => cluster.settle(&[]),
            Err(LedgerError::ActivationConflict) => break,
            other => panic!("genesis over populated history: {other:?}"),
        }
    }
    cluster.activate_import(1, &[]);
    cluster.settle(&[]);
    // Every replica translated its own sealed legacy core to the same root.
    let root = cluster.node(1).native_core().unwrap().native_sequence();
    assert_eq!(root, SessionSeq(1));
    for id in [1, 2, 3] {
        assert!(
            matches!(
                cluster.node(id).activation(),
                LedgerActivation::Native {
                    kind: ActivationKind::Imported,
                    profile: NativeContentProfile::ProjectionOnly,
                    ..
                }
            ),
            "replica {id}"
        );
        assert_eq!(cluster.node(id).native_sequence().unwrap(), SessionSeq(1));
        assert_eq!(cluster.status(id, 1), Some(ClaimStatus::Satisfied));
        assert_eq!(cluster.status(id, 2), Some(ClaimStatus::Cancelled));
        // Legacy exact-retry identities keep resolving from the frozen prefix.
        assert_eq!(
            cluster
                .node(id)
                .receipt(&legacy_receipt.key)
                .map(|receipt| receipt.sequence),
            Some(legacy_receipt.sequence)
        );
        assert_eq!(
            cluster
                .node(id)
                .read_at_least(SessionSeq(0))
                .unwrap()
                .sequence,
            legacy_sequence
        );
    }
    // Followers sealed the inline legacy payload before applying the import.
    assert!(cluster.sealed.contains(&2) && cluster.sealed.contains(&3));
    // Legacy admission is refused; native work continues after the import.
    assert!(matches!(
        cluster
            .node(1)
            .propose(&legacy::input(
                2_000,
                legacy::ISSUER,
                Command::PostClaim {
                    claim: ClaimId::from_u128(2)
                }
            ))
            .unwrap(),
        Submission::Domain(DomainOutcome::Refuse {
            code: ErrorCode::UnsupportedSchema,
            ..
        })
    ));
    let create = creation(cluster.next(PARTIES.issuer), 5);
    let created = cluster.commit(1, PARTIES.issuer, create, &[]);
    cluster.settle(&[]);
    for id in [1, 2, 3] {
        assert_eq!(cluster.node(id).native_sequence().unwrap(), SessionSeq(2));
        assert_eq!(
            cluster.node(id).native_outcome(created.invocation).unwrap(),
            Some(created)
        );
    }
    // The enclosing checkpoint carries the imported prefix; a restart keeps it.
    cluster.node(1).checkpoint().unwrap();
    cluster.stop(1);
    cluster.reopen(1, true);
    assert!(matches!(
        cluster.node(1).activation(),
        LedgerActivation::Native {
            kind: ActivationKind::Imported,
            ..
        }
    ));
    assert_eq!(cluster.node(1).native_sequence().unwrap(), SessionSeq(2));
    assert_eq!(cluster.status(1, 1), Some(ClaimStatus::Satisfied));
    assert_eq!(
        cluster
            .node(1)
            .receipt(&legacy_receipt.key)
            .map(|receipt| receipt.sequence),
        Some(legacy_receipt.sequence)
    );
}

/// Cursor and managed-stream helpers over the harness ledger, mirroring the
/// legacy protocol tests.
mod ancillary {
    use super::*;
    use focal_model::{
        ManagedAuthenticatedInput, ManagedRequestFamily, ManagedRequestKey, RequestStreamCommand,
        RequestStreamControlInput, RequestStreamControlOutcome, RequestStreamIdentity,
        RequestStreamState, managed_command_hash,
    };
    use focal_stream::{ConsumerId, CursorCommand, CursorOperation, DeltaFilter, Position};

    pub(super) fn cursor_input(s: &Session, id: u128, operation: CursorOperation) -> CursorInput {
        CursorInput {
            ledger: ledger(),
            key: RequestKey {
                principal: legacy::ISSUER,
                epoch: RequestEpoch(1),
                id: RequestId::from_u128(id),
            },
            intent_hash: ContentHash(
                *blake3::hash(&postcard::to_stdvec(&operation).unwrap()).as_bytes(),
            ),
            command: CursorCommand {
                expected_revision: s.cursor_revision(),
                now: s.cursor_clock(),
                operation,
            },
        }
    }
    pub(super) fn register(s: &Session, id: u128) -> CursorInput {
        cursor_input(
            s,
            id,
            CursorOperation::RegisterProtected {
                consumer: ConsumerId(legacy::ISSUER.0),
                scope: ContentHash([8; 32]),
                filter: DeltaFilter::All,
                start: Position::origin(ledger()),
            },
        )
    }
    pub(super) fn replay_ids(s: &Session, from: Position) -> (Position, Vec<DeltaId>) {
        let (position, deltas) = replay_deltas(s, from);
        (position, deltas.into_iter().map(|delta| delta.id).collect())
    }
    pub(super) fn replay_deltas(s: &Session, from: Position) -> (Position, Vec<Delta>) {
        let mut deltas = Vec::new();
        let position = s
            .replay(
                from,
                focal_stream::ReplayLimit {
                    max_items: 256,
                    max_bytes: 1 << 20,
                    max_sequences: 1 << 20,
                },
                &mut |delta| {
                    deltas.push(delta.clone());
                    Ok(())
                },
            )
            .unwrap();
        (position, deltas)
    }
    pub(super) fn stream_register(cluster: &mut Cluster, id: u64) -> RequestStreamIdentity {
        let request = RequestStreamControlInput {
            cluster: CLUSTER,
            ledger: ledger(),
            principal: legacy::ISSUER,
            id: RequestId::from_u128(3_000),
            command: RequestStreamCommand::Register {
                slot: 1,
                expected_generation: 0,
                owner: RequestId::from_u128(3_001),
                window: 8,
            },
        };
        for _ in 0..8 {
            match cluster.node(id).propose_request_stream(&request) {
                Ok(RequestStreamSubmission::Pending(_))
                | Ok(RequestStreamSubmission::Committed(_)) => break,
                Err(LedgerError::Consensus(ConsensusError::PersistencePending))
                | Err(LedgerError::Managed(ManagedError::Unsupported)) => {
                    let _ = cluster.node(id).begin_managed_support();
                    cluster.settle(&[]);
                }
                other => panic!("stream register: {other:?}"),
            }
        }
        for _ in 0..8 {
            cluster.pump(&[]);
            if let Some(receipt) = cluster.node(id).request_stream_receipt(&request).unwrap() {
                return match &receipt.outcome {
                    RequestStreamControlOutcome::Registered(RequestStreamState::Active {
                        stream,
                        ..
                    }) => *stream,
                    other => panic!("{other:?}"),
                };
            }
        }
        panic!("stream never registered");
    }
    pub(super) fn managed_claim(
        stream: RequestStreamIdentity,
        ordinal: u64,
    ) -> ManagedAuthenticatedInput {
        let old = legacy::input(
            4_000 + u128::from(ordinal),
            legacy::ISSUER,
            Command::GenerateClaim {
                claim: legacy::new_claim(40 + u128::from(ordinal)),
            },
        );
        ManagedAuthenticatedInput {
            key: ManagedRequestKey {
                stream,
                ordinal,
                id: old.request_id,
            },
            expected_revision: None,
            authority: old.authority,
            command: old.command,
        }
    }
    pub(super) fn managed_hash(input: &ManagedAuthenticatedInput) -> ContentHash {
        managed_command_hash(input).unwrap()
    }
    pub(super) fn domain() -> ManagedRequestFamily {
        ManagedRequestFamily::Domain
    }
}

#[test]
fn the_import_transition_survives_crash_cuts_at_each_durable_boundary() {
    let mut cluster = Cluster::new(3, &[true, true, true]);
    cluster.elect(1, &[]);
    legacy::populate(&mut cluster, 1);
    cluster.settle(&[]);
    let legacy_sequence = cluster
        .node(1)
        .read_at_least(SessionSeq(0))
        .unwrap()
        .sequence;
    cluster.exchange_support(&[]);
    // Cut 1: the authority crashes right after proposing, before anything is
    // replicated. Its log holds the record; on restart it is either committed
    // once or superseded, never applied twice.
    cluster.propose_import(1, &[]);
    cluster.stop(1);
    cluster.reopen(1, true);
    let mut leader = cluster.ensure_authority();
    for _ in 0..16 {
        cluster.settle(&[]);
        if cluster.activation_index(leader).is_some() {
            break;
        }
    }
    if cluster.activation_index(leader).is_none() {
        // The record was lost with the term; the operator proposes again.
        cluster.exchange_support(&[]);
        cluster.propose_import(leader, &[]);
        for _ in 0..16 {
            cluster.settle(&[]);
            if cluster.activation_index(leader).is_some() {
                break;
            }
        }
    }
    let index = cluster
        .activation_index(leader)
        .expect("activation committed once");
    // Cut 2: follower 3 crashes with the record appended but not applied: it
    // is the only other voter reachable, so its append forms the majority, and
    // it stops before learning the commit.
    // (The record already committed above on the reachable majority; a
    // replica that missed it entirely receives it after restart.)
    let follower = if leader == 3 { 2 } else { 3 };
    cluster.stop(follower);
    cluster.reopen(follower, true);
    leader = cluster.ensure_authority();
    for _ in 0..16 {
        cluster.settle(&[]);
        if cluster.activation_index(follower) == Some(index) {
            break;
        }
    }
    assert_eq!(cluster.activation_index(follower), Some(index));
    // Cut 3: a replica whose host has not sealed retains the delivery across a
    // restart; once its host seals, the same record applies exactly once.
    for id in [1, 2, 3] {
        assert_eq!(cluster.activation_index(id), Some(index), "replica {id}");
        assert_eq!(cluster.node(id).native_sequence().unwrap(), SessionSeq(1));
        assert_eq!(cluster.status(id, 1), Some(ClaimStatus::Satisfied));
        assert_eq!(
            cluster
                .node(id)
                .read_at_least(SessionSeq(0))
                .unwrap()
                .sequence,
            legacy_sequence
        );
    }
    // Cut 4: the authority crashes after applying the activation and before
    // the genesis record commits; a shutdown checkpoint is impossible then,
    // and the restarted authority commits genesis and opens native admission.
    cluster.stop(leader);
    cluster.reopen(leader, true);
    leader = cluster.ensure_authority();
    for _ in 0..24 {
        cluster.settle(&[]);
        if cluster.node(leader).native_authoritative() {
            break;
        }
    }
    assert!(cluster.node(leader).native_authoritative());
    let create = creation(cluster.next(PARTIES.issuer), 7);
    let created = cluster.commit(leader, PARTIES.issuer, create, &[]);
    cluster.settle(&[]);
    for id in [1, 2, 3] {
        assert_eq!(cluster.node(id).native_sequence().unwrap(), SessionSeq(2));
        assert_eq!(
            cluster.node(id).native_outcome(created.invocation).unwrap(),
            Some(created)
        );
    }
}

#[test]
fn a_replica_whose_host_seals_late_retains_the_import_across_a_restart_and_applies_it_once() {
    let mut cluster = Cluster::new(3, &[true, true, true]);
    cluster.elect(1, &[]);
    legacy::populate(&mut cluster, 1);
    cluster.settle(&[]);
    cluster.no_seal.push(3);
    cluster.activate_import(1, &[]);
    cluster.settle(&[]);
    // Replica 3 holds the committed record but cannot apply it yet.
    assert!(cluster.node(3).pending_import().is_some());
    assert_eq!(cluster.activation_index(3), None);
    assert!(cluster.node(2).activation().is_native());
    cluster.stop(3);
    cluster.reopen(3, true);
    cluster.settle(&[]);
    assert!(cluster.node(3).pending_import().is_some());
    assert_eq!(cluster.activation_index(3), None);
    // Its host seals; the retained record applies once and matches the leader.
    cluster.no_seal.clear();
    for _ in 0..16 {
        cluster.settle(&[]);
        if cluster.activation_index(3).is_some() {
            break;
        }
    }
    assert_eq!(cluster.activation_index(3), cluster.activation_index(1));
    assert!(cluster.node(3).pending_import().is_none());
    assert_eq!(cluster.node(3).native_sequence().unwrap(), SessionSeq(1));
    assert_eq!(cluster.status(3, 1), Some(ClaimStatus::Satisfied));
}

#[test]
fn watches_and_unresolved_managed_requests_survive_the_transition() {
    let mut cluster = Cluster::new(1, &[true]);
    cluster.elect(1, &[]);
    legacy::populate(&mut cluster, 1);
    cluster.settle(&[]);
    // A protected watch registered before activation replays legacy deltas.
    let registration = ancillary::register(cluster.node(1), 5_000);
    let consumer = focal_stream::ConsumerId(legacy::ISSUER.0);
    match cluster.node(1).propose_cursor(&registration, true).unwrap() {
        CursorSubmission::Committed(_) => {}
        CursorSubmission::Pending(_) => cluster.pump(&[]),
    }
    assert!(cluster.node(1).cursor(consumer).is_some());
    let (position, ids) =
        ancillary::replay_ids(cluster.node(1), focal_stream::Position::origin(ledger()));
    assert!(!ids.is_empty());
    // A managed stream with a committed request whose client never observed it.
    let stream = ancillary::stream_register(&mut cluster, 1);
    let request = ancillary::managed_claim(stream, 1);
    match cluster.node(1).propose_managed(&request).unwrap() {
        ManagedSubmission::Pending(_) => cluster.pump(&[]),
        ManagedSubmission::Committed(_) => {}
        ManagedSubmission::Domain(outcome) => panic!("{outcome:?}"),
    }
    let hash = ancillary::managed_hash(&request);
    let receipt = cluster
        .node(1)
        .managed_receipt(&request.key, hash, ancillary::domain())
        .unwrap()
        .cloned()
        .expect("managed request committed");
    cluster.activate_import(1, &[]);
    // The watch continues from its position without a resync; the cursor and
    // its receipts are intact; the managed receipt still resolves exactly.
    let (again, more) = ancillary::replay_ids(cluster.node(1), position);
    assert!(more.is_empty() || again != position);
    assert!(cluster.node(1).cursor(consumer).is_some());
    assert!(cluster.node(1).cursor_receipt(&registration.key).is_some());
    // The stream line continues past the sealed legacy prefix: the import
    // image (native sequence one) emits no deltas, and every later native
    // record streams as a schema-2 delta at `legacy prefix + native - 1`.
    let legacy_prefix = cluster.node(1).sequence();
    assert_eq!(cluster.node(1).native_sequence().unwrap(), SessionSeq(1));
    assert_eq!(cluster.node(1).stream_published(), legacy_prefix);
    assert_eq!(cluster.node(1).stream_bounds().published, legacy_prefix);
    assert_eq!(
        ancillary::replay_deltas(cluster.node(1), position).0,
        focal_stream::Position::resolved(ledger(), legacy_prefix)
    );
    let create = creation(cluster.next(PARTIES.issuer), 7);
    cluster.commit(1, PARTIES.issuer, create, &[]);
    assert_eq!(cluster.node(1).native_sequence().unwrap(), SessionSeq(2));
    let expected = SessionSeq(legacy_prefix.0 + 1);
    assert_eq!(cluster.node(1).stream_published(), expected);
    // The watch's position predates the last legacy record: one replay
    // returns that retained schema-1 delta first, then the native deltas.
    let (resolved, all) = ancillary::replay_deltas(cluster.node(1), position);
    assert_eq!(
        resolved,
        focal_stream::Position::resolved(ledger(), expected)
    );
    let (legacy, native): (Vec<Delta>, Vec<Delta>) = all
        .into_iter()
        .partition(|delta| delta.id.sequence <= legacy_prefix);
    assert!(
        legacy
            .iter()
            .all(|delta| delta.schema == 1 && delta.id.sequence > position.sequence)
    );
    assert!(!native.is_empty());
    for delta in &native {
        assert_eq!(delta.schema, NATIVE_DELTA_SCHEMA);
        assert_eq!(delta.id.sequence, expected);
        assert_eq!(delta.actor, PARTIES.issuer);
        let DeltaFact::Native(record) = &delta.fact else {
            panic!("native fact after activation: {delta:?}");
        };
        assert_eq!(record.sequence, SessionSeq(2));
        assert_eq!(record.ordinal, delta.id.ordinal);
    }
    assert!(native.iter().any(|delta| {
        delta.action == LifecycleAction::Generated && delta.claim == Some(ClaimId::from_u128(7))
    }));
    // A legacy position inside the sealed prefix still replays the retained
    // legacy tail first, then the native deltas, in one continuous stream.
    let (_, whole) =
        ancillary::replay_ids(cluster.node(1), focal_stream::Position::origin(ledger()));
    assert!(whole.iter().any(|id| id.sequence <= legacy_prefix));
    assert!(whole.iter().any(|id| id.sequence == expected));
    assert!(whole.windows(2).all(|pair| pair[0] < pair[1]));
    assert_eq!(
        cluster
            .node(1)
            .managed_receipt(&request.key, hash, ancillary::domain())
            .unwrap()
            .map(|found| found.raft_index),
        Some(receipt.raft_index)
    );
    // Legacy managed work is refused with a typed outcome after activation.
    let late = ancillary::managed_claim(stream, 2);
    assert!(matches!(
        cluster.node(1).propose_managed(&late).unwrap(),
        ManagedSubmission::Domain(DomainOutcome::Refuse {
            code: ErrorCode::UnsupportedSchema,
            ..
        })
    ));
    // Everything survives the enclosing checkpoint and a restart.
    let (_, before_restart) =
        ancillary::replay_ids(cluster.node(1), focal_stream::Position::origin(ledger()));
    assert!(before_restart.len() > ids.len());
    cluster.node(1).checkpoint().unwrap();
    cluster.stop(1);
    cluster.reopen(1, true);
    assert!(cluster.node(1).activation().is_native());
    assert!(cluster.node(1).cursor(consumer).is_some());
    assert!(cluster.node(1).cursor_receipt(&registration.key).is_some());
    assert_eq!(
        cluster
            .node(1)
            .managed_receipt(&request.key, hash, ancillary::domain())
            .unwrap()
            .map(|found| found.raft_index),
        Some(receipt.raft_index)
    );
    let (_, after_restart) =
        ancillary::replay_ids(cluster.node(1), focal_stream::Position::origin(ledger()));
    assert_eq!(after_restart, before_restart);
}

#[test]
fn native_records_stream_as_schema_two_deltas_on_the_continuous_sequence_line() {
    let mut cluster = Cluster::new(1, &[true]);
    cluster.elect(1, &[]);
    cluster.activate(1, &[]);
    // A watch registered through the managed cursor stream at the origin of
    // an empty native ledger sees nothing. (A raw legacy request key would
    // need an admitted V1 request epoch, which a native ledger never has.)
    let stream = ancillary::stream_register(&mut cluster, 1);
    let consumer = focal_stream::ConsumerId::from_u128(1);
    let registration = ancillary::cursor_input(
        cluster.node(1),
        6_000,
        focal_stream::CursorOperation::Register {
            consumer,
            scope: ContentHash([8; 32]),
            filter: focal_stream::DeltaFilter::All,
            start: focal_stream::Position::origin(ledger()),
            expires_at: 1_000,
        },
    );
    let registration = ManagedCursorInput {
        key: ManagedRequestKey {
            stream,
            ordinal: 1,
            id: RequestId::from_u128(6_000),
        },
        intent_hash: registration.intent_hash,
        command: registration.command,
    };
    match cluster
        .node(1)
        .propose_managed_cursor(&registration, false)
        .unwrap()
    {
        ManagedSubmission::Committed(_) => {}
        ManagedSubmission::Pending(_) => cluster.pump(&[]),
        ManagedSubmission::Domain(outcome) => panic!("{outcome:?}"),
    }
    assert!(cluster.node(1).cursor(consumer).is_some());
    assert_eq!(cluster.node(1).stream_published(), SessionSeq(0));
    let origin = focal_stream::Position::origin(ledger());
    assert_eq!(
        ancillary::replay_deltas(cluster.node(1), origin),
        (
            focal_stream::Position::resolved(ledger(), SessionSeq(0)),
            vec![]
        )
    );
    // Two native records: a creation and a post.
    let create = creation(cluster.next(PARTIES.issuer), 1);
    cluster.commit(1, PARTIES.issuer, create, &[]);
    let expected = cluster.claim(1, 1);
    let post = fx::post(cluster.next(PARTIES.issuer), expected);
    cluster.commit(1, PARTIES.issuer, post, &[]);
    assert_eq!(cluster.node(1).native_sequence().unwrap(), SessionSeq(2));
    // A genesis ledger's stream line is the native sequence itself.
    assert_eq!(cluster.node(1).stream_published(), SessionSeq(2));
    assert_eq!(cluster.node(1).stream_bounds().published, SessionSeq(2));
    assert_eq!(cluster.node(1).stream_bounds().floor, SessionSeq(0));
    let (resolved, deltas) = ancillary::replay_deltas(cluster.node(1), origin);
    assert_eq!(
        resolved,
        focal_stream::Position::resolved(ledger(), SessionSeq(2))
    );
    assert!(deltas.len() >= 2, "{deltas:?}");
    for delta in &deltas {
        assert_eq!(delta.schema, NATIVE_DELTA_SCHEMA);
        assert_eq!(delta.id.ledger, ledger());
        assert_eq!(delta.actor, PARTIES.issuer);
        let DeltaFact::Native(record) = &delta.fact else {
            panic!("native fact: {delta:?}");
        };
        assert_eq!(record.sequence, delta.id.sequence);
        assert_eq!(record.ordinal, delta.id.ordinal);
        assert!(delta.claim.is_none() || delta.claim == Some(ClaimId::from_u128(1)));
    }
    let ids: Vec<DeltaId> = deltas.iter().map(|delta| delta.id).collect();
    assert!(ids.windows(2).all(|pair| pair[0] < pair[1]), "{ids:?}");
    assert!(deltas.iter().any(|delta| {
        delta.id.sequence == SessionSeq(1) && delta.action == LifecycleAction::Generated
    }));
    assert!(deltas.iter().any(|delta| {
        delta.id.sequence == SessionSeq(2) && delta.action == LifecycleAction::Posted
    }));
    // The claim filter selects on the delta's claim; every claim-bound fact
    // of these records belongs to the one claim.
    let filter = focal_stream::DeltaFilter::Claims([ClaimId::from_u128(1)].into());
    assert!(
        deltas
            .iter()
            .filter(|delta| delta.claim.is_some())
            .all(|delta| filter.matches(delta))
    );
    // Resuming after an exact delta continues with the next one; a delta
    // position naming no event is refused; a position ahead is refused.
    let (_, rest) =
        ancillary::replay_deltas(cluster.node(1), focal_stream::Position::after_delta(ids[0]));
    assert_eq!(rest.len(), deltas.len() - 1);
    assert_eq!(rest.first().map(|delta| delta.id), Some(ids[1]));
    let limit = focal_stream::ReplayLimit {
        max_items: 8,
        max_bytes: 1 << 16,
        max_sequences: 8,
    };
    let bogus = focal_stream::Position {
        ledger: ledger(),
        sequence: SessionSeq(1),
        offset: focal_stream::PositionOffset::Delta(999),
    };
    assert!(matches!(
        cluster.node(1).replay(bogus, limit, &mut |_| Ok(())),
        Err(StreamError::Invalid(_))
    ));
    let ahead = focal_stream::Position::resolved(ledger(), SessionSeq(3));
    assert!(matches!(
        cluster.node(1).replay(ahead, limit, &mut |_| Ok(())),
        Err(StreamError::CursorAhead)
    ));
    // Work limits bound one replay call without skipping: one item per call
    // walks the same deltas in order.
    let mut walked = Vec::new();
    let mut from = origin;
    loop {
        let mut seen = None;
        let next = cluster
            .node(1)
            .replay(
                from,
                focal_stream::ReplayLimit {
                    max_items: 1,
                    max_bytes: 1 << 16,
                    max_sequences: 8,
                },
                &mut |delta| {
                    seen = Some(delta.id);
                    Ok(())
                },
            )
            .unwrap();
        match seen {
            Some(id) => walked.push(id),
            None => break,
        }
        from = next;
    }
    assert_eq!(walked, ids);
    // The acknowledgment of a native position is accepted by the registry.
    let token = cluster.node(1).cursor(consumer).unwrap().token;
    let acknowledge = ancillary::cursor_input(
        cluster.node(1),
        6_001,
        focal_stream::CursorOperation::Acknowledge {
            token: focal_stream::CursorToken {
                position: focal_stream::Position::after_delta(ids[0]),
                ..token
            },
        },
    );
    let acknowledge = ManagedCursorInput {
        key: ManagedRequestKey {
            stream,
            ordinal: 2,
            id: RequestId::from_u128(6_001),
        },
        intent_hash: acknowledge.intent_hash,
        command: acknowledge.command,
    };
    match cluster
        .node(1)
        .propose_managed_cursor(&acknowledge, false)
        .unwrap()
    {
        ManagedSubmission::Committed(_) => {}
        ManagedSubmission::Pending(_) => cluster.pump(&[]),
        ManagedSubmission::Domain(outcome) => panic!("{outcome:?}"),
    }
    assert_eq!(
        cluster.node(1).cursor(consumer).unwrap().token.position,
        focal_stream::Position::after_delta(ids[0])
    );
    // Checkpoint and restart derive the same deltas from the retained events.
    cluster.node(1).checkpoint().unwrap();
    cluster.stop(1);
    cluster.reopen(1, true);
    let (_, again) = ancillary::replay_deltas(cluster.node(1), origin);
    assert_eq!(again, deltas);
    assert_eq!(
        cluster.node(1).cursor(consumer).unwrap().token.position,
        focal_stream::Position::after_delta(ids[0])
    );
}
