#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
use focal_consensus::NodeConfig;
use focal_control::*;
use focal_directory::*;
use focal_enrollment::*;
use focal_memory::MemoryBudget;
use focal_model::{
    LedgerId, ParticipantId, RequestEpoch, RequestId, RouteEpoch, SessionId, TenantId,
};
use focal_node::control_host::*;
use focal_wire::*;
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const CLUSTER: [u8; 16] = [61; 16];
const GROUP: [u8; 16] = [62; 16];
const OPERATOR: [u8; 16] = [63; 16];
// This verifier rejects all evidence-bearing directory operations. Tests never
// invent a positive attestation: region registration and partition sealing need
// authenticated operator authority, while enrollment uses genuine CSR/CA checks.
struct RejectUnverifiedEvidence;
impl AuthorityVerifier for RejectUnverifiedEvidence {
    fn verify_enrollment(&self, _: &NodeEnrollment) -> Result<(), DirectoryError> {
        Err(DirectoryError::UnverifiedAuthority)
    }
    fn verify_session_fence(&self, _: &SessionFence) -> Result<(), DirectoryError> {
        Err(DirectoryError::UnverifiedAuthority)
    }
    fn verify_replica_ready(&self, _: &ReplicaReady) -> Result<(), DirectoryError> {
        Err(DirectoryError::UnverifiedAuthority)
    }
    fn verify_custody(&self, _: &CustodyProof) -> Result<(), DirectoryError> {
        Err(DirectoryError::UnverifiedAuthority)
    }
    fn verify_delegation(&self, _: &DelegationFence) -> Result<(), DirectoryError> {
        Err(DirectoryError::UnverifiedAuthority)
    }
}
fn namespace() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(61),
        session: SessionId::from_u128(62),
    }
}
fn peer(role: PeerRole) -> AuthenticatedPeer {
    AuthenticatedPeer::local(PeerGrant {
        principal: ParticipantId(OPERATOR),
        tenants: [namespace().tenant].into_iter().collect(),
        role,
    })
    .unwrap()
}
fn budget() -> MemoryBudget {
    MemoryBudget::new(256 * 1024 * 1024, 64 * 1024 * 1024).unwrap()
}
/// Holds the entire remaining allowance. The host's own tick may take or
/// release bytes between a statistics read and a reservation, so exhaustion is
/// reached by reserving until even one byte is refused rather than by trusting
/// a single snapshot.
fn exhaust(memory: &MemoryBudget) -> Vec<focal_memory::Allocation> {
    let mut held = Vec::new();
    for _ in 0..64 {
        let stats = memory.stats();
        let remaining = stats.limit.saturating_sub(stats.used).max(1);
        match memory.reserve(
            focal_memory::BudgetKind::Control,
            focal_memory::BudgetLane::Completion,
            remaining,
        ) {
            Ok(reservation) => held.push(reservation.commit()),
            Err(_) if remaining == 1 => return held,
            Err(_) => {}
        }
    }
    panic!("the control budget never reached exhaustion")
}
/// How long an owner may run no period before a wait calls it wedged.
const FROZEN: Duration = Duration::from_secs(60);
/// The tick of the owners of a rig.
const RIG_TICK: Duration = Duration::from_millis(25);
fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}
fn root_bootstrap(authority: &BootstrapAuthority) -> ControlBootstrap {
    let root = RootDirectory::new(
        focal_directory::ClusterId(CLUSTER),
        RootConfig::default(),
        budget(),
    )
    .unwrap();
    let enrollment = EnrollmentRegistry::new(
        CLUSTER,
        authority.ca_certificate().to_vec(),
        4,
        EnrollmentLimits::default(),
    )
    .unwrap();
    ControlBootstrap::root(&root, &enrollment).unwrap()
}
fn request(sequence: u64, command: ControlCommand) -> ControlRequest {
    ControlRequest {
        id: ControlRequestId {
            client: OPERATOR,
            sequence,
        },
        acknowledged_through: 0,
        command,
    }
}
fn region(revision: u64, id: u128) -> ControlCommand {
    ControlCommand::Root(RootCommand {
        expected_revision: revision,
        operation: RootOperation::RegisterRegion {
            region: RegionRecord {
                id: RegionId::from_u128(id),
                label: format!("region-{id}"),
                authority_epoch: 1,
            },
            expected_epoch: None,
        },
    })
}
struct Rig {
    directories: Vec<tempfile::TempDir>,
    hosts: Vec<ControlHost>,
    owners: Vec<ControlOwner>,
    routers: Vec<tokio::task::JoinHandle<()>>,
    isolated: Arc<AtomicU64>,
    /// The node the routers send no entries to — appends and snapshots
    /// dropped, everything else carried — so its log falls behind while it
    /// still hears its leader. Zero withholds nothing.
    withheld: Arc<AtomicU64>,
    /// The applied index at which the routers hold every frame, until a
    /// test lets the next one through: a commit at a time, at the test's
    /// pace. `u64::MAX` holds nothing.
    allowed: Arc<AtomicU64>,
    /// The replicas' election timeout, in ticks.
    election_tick: usize,
    /// The owners' request time.
    request_timeout: Duration,
    bootstrap: ControlBootstrap,
    group: [u8; 16],
}
impl Rig {
    fn new(bootstrap: ControlBootstrap, group: [u8; 16]) -> Self {
        Self::with_timing(bootstrap, group, 10, Duration::from_millis(350))
    }
    /// A rig whose replicas campaign only after `election_tick` ticks of
    /// silence and whose owners give a request `request_timeout`: for a
    /// test that holds the group's frames on purpose.
    fn with_timing(
        bootstrap: ControlBootstrap,
        group: [u8; 16],
        election_tick: usize,
        request_timeout: Duration,
    ) -> Self {
        let mut value = Self {
            directories: (0..3).map(|_| tempfile::tempdir().unwrap()).collect(),
            hosts: vec![],
            owners: vec![],
            routers: vec![],
            isolated: Arc::new(AtomicU64::new(0)),
            withheld: Arc::new(AtomicU64::new(0)),
            allowed: Arc::new(AtomicU64::new(u64::MAX)),
            election_tick,
            request_timeout,
            bootstrap,
            group,
        };
        value.start();
        value
    }
    fn start(&mut self) {
        let mut channels = Vec::new();
        for (offset, directory) in self.directories.iter().enumerate() {
            let id = offset as u64 + 1;
            let mut config = NodeConfig::single(id, CLUSTER, self.group);
            config.voters = vec![1, 2, 3];
            config.election_tick = self.election_tick;
            let allowance = budget();
            let replica = ControlReplica::open(
                ControlOptions::new(config),
                self.bootstrap.clone(),
                allowance.clone(),
                directory.path(),
            )
            .unwrap();
            let mut config = ControlHostConfig::new(namespace());
            config.tick = RIG_TICK;
            config.request_timeout = self.request_timeout;
            let (host, owner, channel) =
                ControlHost::spawn(replica, RejectUnverifiedEvidence, config, allowance).unwrap();
            self.hosts.push(host);
            self.owners.push(owner);
            channels.push((id, channel));
        }
        for (from, mut channel) in channels {
            let hosts = self.hosts.clone();
            let isolated = self.isolated.clone();
            let withheld = self.withheld.clone();
            let allowed = self.allowed.clone();
            self.routers.push(tokio::spawn(async move {
                // What the machine takes to wake a task that asked for a
                // millisecond is the path the sender's pace is derived from
                // (27 §3.1 P2), as a node derives its pace from the round
                // trips its probes measure: a loaded machine stretches the
                // owners' periods here as it does there, and every wait
                // charged to them with it. A probe, and not what a message
                // takes to be handled: that is the owner's own period, and a
                // pace fed its own period holds itself wherever it is.
                let mut paths: std::collections::BTreeMap<u64, focal_timing::PathRtt> =
                    std::collections::BTreeMap::new();
                // The path is sampled at most once a tick: a sample after
                // every frame put a wake on the delivery of each, and a
                // backlog held behind a hold drained a wake at a time — on a
                // runner at a load of sixty, tens of the leader's periods
                // before its heartbeats reached a follower (2026-10-04).
                let mut sampled: Option<std::time::Instant> = None;
                while let Some(frame) = channel.recv().await {
                    let excluded = isolated.load(Ordering::SeqCst);
                    if excluded == from || excluded == frame.target {
                        continue;
                    }
                    if withheld.load(Ordering::SeqCst) == frame.target {
                        let (Operation::Raft { message, .. }
                        | Operation::RaftOrdered { message, .. }) = &frame.request.operation
                        else {
                            continue;
                        };
                        let kind = focal_consensus::decode_message(message).unwrap().msg_type;
                        if matches!(
                            kind,
                            focal_consensus::MessageType::MsgAppend
                                | focal_consensus::MessageType::MsgSnapshot
                        ) {
                            continue;
                        }
                    }
                    let Some(target) = hosts.get(frame.target.saturating_sub(1) as usize) else {
                        continue;
                    };
                    // Held while the group has applied what the test
                    // allows: nothing more commits until it allows more.
                    while hosts
                        .iter()
                        .map(|host| host.progress().applied_index)
                        .max()
                        .unwrap_or(0)
                        >= allowed.load(Ordering::SeqCst)
                    {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    let verified = verify_request(
                        peer(PeerRole::Node { node_id: from }),
                        frame.request.clone(),
                        &ControlHost::wire_limits(),
                    )
                    .unwrap();
                    let _ = target.handle(&verified).await;
                    if sampled.is_none_or(|at| at.elapsed() >= RIG_TICK) {
                        let asked = std::time::Instant::now();
                        tokio::time::sleep(Duration::from_millis(1)).await;
                        let taken = u64::try_from(asked.elapsed().as_nanos()).unwrap_or(u64::MAX);
                        paths.entry(frame.target).or_default().on_sample(taken);
                        if let Some(sender) = hosts.get(from.saturating_sub(1) as usize) {
                            sender.pace(paths.values());
                        }
                        sampled = Some(std::time::Instant::now());
                    }
                    drop(frame);
                }
            }));
        }
    }
    fn periods(&self) -> Vec<u64> {
        self.hosts.iter().map(ControlHost::periods).collect()
    }
    /// A wait charged to the owners' own periods (27 §3.1 P8): what ten
    /// seconds hold at the tick they are configured with, however long
    /// that takes on the machine the test runs on.
    fn deadline(&self) -> focal_timing::ProgressDeadline {
        focal_timing::ProgressDeadline::begin(
            &self.periods(),
            focal_timing::ProgressDeadline::periods(Duration::from_secs(10), RIG_TICK),
            FROZEN,
        )
    }
    async fn leader(&self, exclude: u64) -> usize {
        let mut wait = self.deadline();
        // What each host last answered a read with, for the wait's report.
        let mut answered: Vec<Option<ControlFailure>> = vec![None; self.hosts.len()];
        // Every ask is a new one.
        let mut asked = 0u128;
        // Each host's term and leader as they changed during the wait, with
        // the host's periods then: whether a leader stepped down in its
        // term (its quorum check) or a follower's term rose first (its
        // campaign). The last 64 changes.
        let mut seen: Vec<(u64, u64)> = vec![(0, 0); self.hosts.len()];
        let mut changes: std::collections::VecDeque<(u64, u64, u64, u64)> =
            std::collections::VecDeque::with_capacity(64);
        loop {
            for (index, host) in self.hosts.iter().enumerate() {
                let status = host.progress();
                if let Some(last) = seen.get_mut(index)
                    && *last != (status.term, status.leader)
                {
                    *last = (status.term, status.leader);
                    if changes.len() == 64 {
                        changes.pop_front();
                    }
                    changes.push_back((status.node, host.periods(), status.term, status.leader));
                }
                if status.node != exclude && status.leader == status.node {
                    asked += 1;
                    match host
                        .read(
                            peer(PeerRole::Runtime),
                            RequestId::from_u128(900_000 + asked),
                            ControlRead::State,
                        )
                        .await
                    {
                        Ok(_) => return index,
                        Err(error) => {
                            if let Some(slot) = answered.get_mut(index) {
                                *slot = Some(error);
                            }
                        }
                    }
                }
            }
            if let Err(spent) = wait.check(&self.periods()) {
                panic!(
                    "no leader that answers: {spent}; last read answers {answered:?}; (node, period, term, leader) as they changed {changes:?}; periods {:?} refused {:?} longest {:?} pace {:?}; {:?}",
                    self.periods(),
                    self.hosts
                        .iter()
                        .map(ControlHost::refused_periods)
                        .collect::<Vec<_>>(),
                    self.hosts
                        .iter()
                        .map(ControlHost::longest_period)
                        .collect::<Vec<_>>(),
                    self.hosts
                        .iter()
                        .map(ControlHost::current_pace)
                        .collect::<Vec<_>>(),
                    self.hosts.iter().map(|h| h.progress()).collect::<Vec<_>>()
                );
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    async fn state(&self, index: usize) -> ControlSnapshot {
        self.state_on_leader(index).await.1
    }
    /// The definite answer of host `index` to `request`, asked by `role`:
    /// an outcome unknown, a host without the room or one not ready is
    /// asked again, charged to the hosts' periods, and the exact request
    /// finds the same receipt however often it is asked. A host that does
    /// not lead hands the ask to the one that does, `excluded` aside; with
    /// `follow` false its refusal to lead is the answer.
    async fn definite(
        &self,
        index: &mut usize,
        role: PeerRole,
        request: &ControlRequest,
        follow: Option<u64>,
    ) -> Result<ControlReceipt, ControlFailure> {
        let mut wait = self.deadline();
        loop {
            match self.hosts[*index].submit(peer(role), request.clone()).await {
                Err(
                    ControlFailure::OutcomeUnknown
                    | ControlFailure::Unavailable
                    | ControlFailure::NotReady
                    | ControlFailure::Capacity,
                ) => {}
                Err(ControlFailure::NotLeader { .. }) if follow.is_some() => {
                    *index = self.leader(follow.unwrap_or(0)).await;
                }
                answer => {
                    if let Ok(receipt) = &answer {
                        assert_eq!(receipt.request, request.id);
                    }
                    return answer;
                }
            }
            if let Err(spent) = wait.check(&self.periods()) {
                panic!("no definite answer to {:?}: {spent}", request.id);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    async fn commit_on_leader(
        &self,
        mut index: usize,
        request: ControlRequest,
        excluded: u64,
    ) -> (usize, ControlReceipt) {
        // A successful setup read is not a lease on the leader. Preserve the
        // exact command and request ID across short host deadlines or elections;
        // the tests below still exercise minority/refusal boundaries directly.
        match self
            .definite(&mut index, PeerRole::Runtime, &request, Some(excluded))
            .await
        {
            Ok(receipt) => (index, receipt),
            Err(error) => panic!("unexpected setup mutation failure: {error:?}"),
        }
    }
    /// `read` answered by whoever leads: a completed ReadIndex is not an
    /// owner lease, so a host that stopped leading or is not ready hands
    /// the read on, charged to the hosts' periods. Direct minority reads
    /// elsewhere in the tests must still fail immediately.
    async fn read_on_leader(
        &self,
        mut index: usize,
        id: u128,
        read: ControlRead,
    ) -> (usize, ControlReadResult) {
        let mut wait = self.deadline();
        // Every ask is a new one.
        let mut asked = 0u128;
        loop {
            asked += 1;
            match self.hosts[index]
                .read(
                    peer(PeerRole::Runtime),
                    RequestId::from_u128(id * 1_000_000 + asked),
                    read.clone(),
                )
                .await
            {
                Ok(result) => return (index, result),
                Err(
                    ControlFailure::Unavailable
                    | ControlFailure::NotLeader { .. }
                    | ControlFailure::NotReady
                    | ControlFailure::OutcomeUnknown
                    | ControlFailure::Capacity,
                ) => index = self.leader(0).await,
                Err(error) => panic!("unexpected read failure: {error:?}"),
            }
            if let Err(spent) = wait.check(&self.periods()) {
                panic!("no quorum-ready owner answers {read:?}: {spent}");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    async fn state_on_leader(&self, index: usize) -> (usize, ControlSnapshot) {
        match self.read_on_leader(index, 901, ControlRead::State).await {
            (index, ControlReadResult::State(snapshot)) => (index, snapshot),
            _ => panic!("state expected"),
        }
    }
    async fn stop(&mut self) {
        for host in &self.hosts {
            host.stop().await.unwrap();
        }
        for owner in self.owners.drain(..) {
            owner.join().unwrap();
        }
        for router in self.routers.drain(..) {
            router.await.unwrap();
        }
        self.hosts.clear();
    }
}
fn registry(snapshot: &ControlSnapshot) -> EnrollmentRegistry {
    let ControlBootstrap::Root { enrollment, .. } = &snapshot.state else {
        panic!("root expected")
    };
    EnrollmentRegistry::restore(enrollment, CLUSTER, EnrollmentLimits::default()).unwrap()
}

#[tokio::test]
async fn directory_bootstrap_authorization_requires_a_fresh_root_quorum() {
    use focal_node::directory_bootstrap::{DirectoryBootstrapError, FirstDirectoryPlan};
    let directory = tempfile::tempdir().unwrap();
    let authority = BootstrapAuthority::open_or_create(
        directory.path().join("ca"),
        CLUSTER,
        vec!["localhost".into()],
        now(),
    )
    .unwrap();
    let mut rig = Rig::new(root_bootstrap(&authority), GROUP);
    let leader = rig.leader(0).await;
    let plan = FirstDirectoryPlan::derive(CLUSTER, 1).unwrap();
    // The definite answer of the host: one without the room, or not ready,
    // is asked again, charged to the owners' periods.
    // A barrier that the host gave up on in its request's time is asked
    // again too (`Unavailable`), except where the host cannot complete one:
    // a leader that the quorum does not reach.
    async fn definite(
        rig: &Rig,
        index: usize,
        plan: FirstDirectoryPlan,
        quorum: bool,
    ) -> Result<(), DirectoryBootstrapError> {
        let mut wait = rig.deadline();
        let mut last;
        loop {
            match rig.hosts[index].prepare_directory(plan).await {
                Err(
                    error @ (DirectoryBootstrapError::Capacity
                    | DirectoryBootstrapError::NotReady
                    | DirectoryBootstrapError::Control(
                        ControlError::Capacity | ControlError::Busy | ControlError::NotReady,
                    )),
                ) => last = Some(error),
                Err(DirectoryBootstrapError::Unavailable) if quorum => {
                    last = Some(DirectoryBootstrapError::Unavailable);
                }
                answer => return answer.map(|_| ()),
            }
            if let Err(spent) = wait.check(&rig.periods()) {
                panic!(
                    "no definite answer to the directory plan: {spent}; last {last:?}; leads {:?} periods {:?} longest {:?} pace {:?}",
                    rig.hosts[index].progress().leader,
                    rig.periods(),
                    rig.hosts
                        .iter()
                        .map(ControlHost::longest_period)
                        .collect::<Vec<_>>(),
                    rig.hosts[index].current_pace()
                );
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    // The quorum is live, but no real delegation/grant exists in this root.
    let answer = definite(&rig, leader, plan, true).await;
    assert!(
        matches!(answer, Err(DirectoryBootstrapError::Unauthorized)),
        "{answer:?}"
    );
    let old_leader = rig.hosts[leader].progress().node;
    rig.isolated.store(old_leader, Ordering::SeqCst);
    // Even denial must be evaluated behind this request's new barrier. A
    // previously completed read cannot authorize a later startup request:
    // the isolated leader's barrier never completes, and it says so once
    // it has waited a request's time of its own periods.
    let answer = definite(&rig, leader, plan, false).await;
    assert!(
        matches!(answer, Err(DirectoryBootstrapError::Unavailable)),
        "{answer:?}"
    );
    rig.isolated.store(0, Ordering::SeqCst);
    rig.stop().await;
}
fn join_request(
    authority: &BootstrapAuthority,
    invitation: &Invitation,
    key: &JoinKey,
) -> JoinRequest {
    let mut client = rustls::ClientConnection::new(
        Arc::new(invitation.client_config().unwrap()),
        rustls::pki_types::ServerName::try_from(invitation.trust().server_name.clone()).unwrap(),
    )
    .unwrap();
    let mut server = rustls::ServerConnection::new(Arc::new(
        authority.server_identity().server_config().unwrap(),
    ))
    .unwrap();
    for _ in 0..32 {
        if client.wants_write() {
            let mut bytes = Vec::new();
            client.write_tls(&mut bytes).unwrap();
            server.read_tls(&mut std::io::Cursor::new(bytes)).unwrap();
            server.process_new_packets().unwrap();
        }
        if server.wants_write() {
            let mut bytes = Vec::new();
            server.write_tls(&mut bytes).unwrap();
            client.read_tls(&mut std::io::Cursor::new(bytes)).unwrap();
            client.process_new_packets().unwrap();
        }
        if !client.is_handshaking() && !server.is_handshaking() {
            return invitation.request_after_tls(&client, key, now()).unwrap();
        }
    }
    panic!("TLS handshake did not finish")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn eventual_state_rediscovers_majority_after_cached_owner_loses_quorum() {
    let keys = tempfile::tempdir().unwrap();
    let authority = BootstrapAuthority::open_or_create(
        keys.path().join("ca"),
        CLUSTER,
        vec!["localhost".into()],
        now(),
    )
    .unwrap();
    let mut rig = Rig::new(root_bootstrap(&authority), GROUP);
    rig.hosts[0].campaign().await.unwrap();
    let stale = rig.leader(0).await;
    let (stale, _) = rig
        .commit_on_leader(stale, request(1, region(0, 1)), 0)
        .await;
    let excluded = rig.hosts[stale].progress().node;
    rig.isolated.store(excluded, Ordering::SeqCst);
    let majority = rig.leader(excluded).await;
    let (_, committed) = rig
        .commit_on_leader(majority, request(2, region(1, 2)), excluded)
        .await;
    assert_eq!(committed.revisions.root, 2);
    // A cached owner is not a lease: this read must still refuse minority state.
    assert!(
        rig.hosts[stale]
            .read(
                peer(PeerRole::Runtime),
                RequestId::from_u128(903),
                ControlRead::State,
            )
            .await
            .is_err()
    );
    let (owner, snapshot) = rig.state_on_leader(stale).await;
    assert_ne!(owner, stale);
    assert!(snapshot.applied_index >= committed.committed_index);
    rig.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn root_enrollment_majority_commit_exact_retry_and_disk_restart() {
    let keys = tempfile::tempdir().unwrap();
    let authority = BootstrapAuthority::open_or_create(
        keys.path().join("ca"),
        CLUSTER,
        vec!["localhost".into()],
        now(),
    )
    .unwrap();
    let mut rig = Rig::new(root_bootstrap(&authority), GROUP);
    rig.hosts[0].campaign().await.unwrap();
    let mut leader = rig.leader(0).await;
    let first = request(1, region(0, 1));
    let first_receipt = rig
        .definite(&mut leader, PeerRole::Runtime, &first, Some(0))
        .await
        .unwrap();
    assert_eq!(first_receipt.revisions.root, 1);
    let isolated_id = rig.hosts[leader].progress().node;
    rig.isolated.store(isolated_id, Ordering::SeqCst);
    let pending = request(2, region(1, 2));
    let lost = rig.hosts[leader]
        .submit(peer(PeerRole::Runtime), pending.clone())
        .await;
    assert!(matches!(
        lost,
        Err(ControlFailure::OutcomeUnknown
            | ControlFailure::NotLeader { .. }
            | ControlFailure::NotReady)
    ));
    assert!(
        rig.hosts[leader]
            .read(
                peer(PeerRole::Runtime),
                RequestId::from_u128(902),
                ControlRead::State
            )
            .await
            .is_err()
    );
    let mut majority = rig.leader(isolated_id).await;
    let second_receipt = rig
        .definite(
            &mut majority,
            PeerRole::Runtime,
            &pending,
            Some(isolated_id),
        )
        .await
        .unwrap();
    assert_eq!(second_receipt.revisions.root, 2);
    rig.isolated.store(0, Ordering::SeqCst);
    let (majority, snapshot) = rig.state_on_leader(majority).await;
    let draft = registry(&snapshot)
        .prepare_invitation(
            &authority,
            InviteOptions {
                endpoint: "127.0.0.1:8443".into(),
                server_name: "localhost".into(),
                role: EnrollmentRole::Node,
                expires_at: now() + 600,
            },
            now(),
        )
        .unwrap();
    let invitation_request = request(3, ControlCommand::Enrollment(draft.command().clone()));
    let mut majority = majority;
    let invitation_receipt = rig
        .definite(
            &mut majority,
            PeerRole::Runtime,
            &invitation_request,
            Some(0),
        )
        .await
        .unwrap();
    let (majority, snapshot) = rig.state_on_leader(majority).await;
    let invitation = draft.release(&registry(&snapshot)).unwrap();
    let join_key = JoinKey::open_or_create(keys.path().join("join"), CLUSTER).unwrap();
    let join = join_request(&authority, &invitation, &join_key);
    let JoinPreparation::Commit(command) = registry(&snapshot)
        .prepare_join(&authority, &join, now())
        .unwrap()
    else {
        panic!("new enrollment expected")
    };
    let enrollment_request = request(4, ControlCommand::Enrollment(command));
    let mut majority = majority;
    let enrollment_receipt = rig
        .definite(
            &mut majority,
            PeerRole::Runtime,
            &enrollment_request,
            Some(0),
        )
        .await
        .unwrap();
    let enrolled = registry(&rig.state(majority).await)
        .release(&join, now())
        .unwrap();
    assert_eq!(enrolled.identity.node_id, Some(4));
    assert_eq!(enrollment_receipt.revisions.enrollment, 2);
    tokio::time::sleep(Duration::from_millis(150)).await;
    rig.stop().await;
    rig.start();
    rig.hosts[0].campaign().await.unwrap();
    let mut recovered = rig.leader(0).await;
    for (request, receipt) in [
        (first, first_receipt),
        (pending, second_receipt),
        (invitation_request, invitation_receipt),
        (enrollment_request, enrollment_receipt),
    ] {
        assert_eq!(
            rig.definite(&mut recovered, PeerRole::Runtime, &request, Some(0))
                .await
                .unwrap(),
            receipt
        );
    }
    let snapshot = rig.state(recovered).await;
    let registry = registry(&snapshot);
    assert_eq!(registry.release(&join, now()).unwrap(), enrolled);
    assert_eq!(registry.enrollments().count(), 1);
    assert!(matches!(
        registry.prepare_join(&authority, &join, now()).unwrap(),
        JoinPreparation::Existing(_)
    ));
    rig.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partition_owner_replication_and_authorization_are_independent_of_root() {
    let group = [72; 16];
    let directory = DirectoryPartition::new(
        focal_directory::ClusterId(CLUSTER),
        Delegation {
            namespace: NamespaceRange::all(),
            partition: PartitionId::from_u128(1),
            region: RegionId::from_u128(1),
            log_group: LogGroupId(group),
            epoch: 1,
            activation: None,
        },
        PartitionConfig::default(),
        budget(),
    )
    .unwrap();
    let mut rig = Rig::new(ControlBootstrap::partition(&directory), group);
    rig.hosts[0].campaign().await.unwrap();
    let mut leader = rig.leader(0).await;
    assert_eq!(
        rig.definite(
            &mut leader,
            PeerRole::Actor,
            &request(1, region(0, 1)),
            Some(0)
        )
        .await,
        Err(ControlFailure::Unauthorized)
    );
    let forged = ControlRequest {
        id: ControlRequestId {
            client: [99; 16],
            sequence: 1,
        },
        acknowledged_through: 0,
        command: region(0, 1),
    };
    assert_eq!(
        rig.definite(&mut leader, PeerRole::Runtime, &forged, Some(0))
            .await,
        Err(ControlFailure::Unauthorized)
    );
    assert_eq!(
        rig.definite(
            &mut leader,
            PeerRole::Runtime,
            &request(1, region(0, 1)),
            Some(0)
        )
        .await,
        Err(ControlFailure::WrongOwner)
    );
    let request = request(
        1,
        ControlCommand::Partition(PartitionCommand {
            expected_revision: 0,
            delegation_epoch: 1,
            operation: PartitionOperation::SealForTransfer {
                operation: OperationId::from_u128(1),
                destination: PartitionId::from_u128(2),
                next_epoch: 2,
            },
        }),
    );
    let receipt = rig
        .definite(&mut leader, PeerRole::Runtime, &request, Some(0))
        .await
        .unwrap();
    assert_eq!(receipt.revisions.partition, 1);
    let rpc = ControlRpc::Read(ControlRead::State).encode(65536).unwrap();
    for (ledger, group) in [
        (
            LedgerId {
                session: SessionId::from_u128(999),
                ..namespace()
            },
            group,
        ),
        (namespace(), GROUP),
    ] {
        let wire = RequestEnvelope {
            protocol: PROTOCOL_VERSION,
            ledger,
            route_epoch: RouteEpoch(1),
            request_epoch: RequestEpoch(1),
            request_id: RequestId::from_u128(55),
            operation: Operation::Control {
                group,
                request: rpc.clone(),
            },
        };
        let verified =
            verify_request(peer(PeerRole::Runtime), wire, &ControlHost::wire_limits()).unwrap();
        let result = rig.hosts[leader].handle(&verified).await;
        let Response::Control { response } = result.result else {
            panic!("control response expected")
        };
        assert!(matches!(
            ControlReply::decode(&response, 65536).unwrap(),
            ControlReply::Rejected(ControlFailure::Unauthorized | ControlFailure::WrongOwner)
        ));
    }
    let mut message = focal_consensus::Message {
        from: 2,
        to: rig.hosts[leader].progress().node,
        term: 1,
        ..Default::default()
    };
    message.msg_type = focal_consensus::MessageType::MsgHeartbeat;
    let wire = RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger: namespace(),
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(56),
        operation: Operation::Raft {
            group,
            message: focal_consensus::encode_message(&message).unwrap(),
        },
    };
    let verified = verify_request(
        peer(PeerRole::Node { node_id: 999 }),
        wire,
        &ControlHost::wire_limits(),
    )
    .unwrap();
    assert_eq!(
        rig.hosts[leader].handle(&verified).await.result,
        Response::Error(AccessError::Unauthorized)
    );
    tokio::time::sleep(Duration::from_millis(150)).await;
    rig.stop().await;
    rig.start();
    rig.hosts[0].campaign().await.unwrap();
    let mut recovered = rig.leader(0).await;
    assert_eq!(
        rig.definite(&mut recovered, PeerRole::Runtime, &request, Some(0))
            .await
            .unwrap(),
        receipt
    );
    let ControlBootstrap::Partition { directory } = rig.state(recovered).await.state else {
        panic!("partition expected")
    };
    assert_eq!(
        directory.sealed.unwrap().destination,
        PartitionId::from_u128(2)
    );
    rig.stop().await;
}

#[tokio::test]
async fn owned_control_response_retains_input_and_export_budgets_until_delivery_drop() {
    let data = tempfile::tempdir().unwrap();
    let authority = BootstrapAuthority::open_or_create(
        data.path().join("ca"),
        CLUSTER,
        vec!["localhost".into()],
        now(),
    )
    .unwrap();
    let memory = budget();
    let replica = ControlReplica::open(
        ControlOptions::new(NodeConfig::single(1, CLUSTER, GROUP)),
        root_bootstrap(&authority),
        memory.clone(),
        data.path().join("log"),
    )
    .unwrap();
    let (host, owner, _outgoing) = ControlHost::spawn(
        replica,
        RejectUnverifiedEvidence,
        ControlHostConfig::new(namespace()),
        memory.clone(),
    )
    .unwrap();
    host.campaign().await.unwrap();
    let mut wait = focal_timing::ProgressDeadline::begin(&[host.periods()], 100, FROZEN);
    while host
        .read(
            peer(PeerRole::Runtime),
            RequestId::from_u128(1),
            ControlRead::State,
        )
        .await
        .is_err()
    {
        if let Err(spent) = wait.check(&[host.periods()]) {
            panic!("the owner never led: {spent}; {:?}", host.progress());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let before = memory.stats();
    let request = RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger: namespace(),
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(2),
        operation: Operation::Control {
            group: GROUP,
            request: ControlRpc::Read(ControlRead::State).encode(65536).unwrap(),
        },
    };
    let verified = verify_request(
        peer(PeerRole::Runtime),
        request,
        &ControlHost::wire_limits(),
    )
    .unwrap();
    let response = host.handle_accounted(&verified).await;
    assert!(matches!(
        response.envelope().result,
        Response::Control { .. }
    ));
    let retained = memory.stats();
    use focal_memory::BudgetKind;
    assert!(
        retained.by_kind[BudgetKind::Pending as usize]
            > before.by_kind[BudgetKind::Pending as usize]
    );
    assert!(
        retained.by_kind[BudgetKind::Control as usize]
            > before.by_kind[BudgetKind::Control as usize]
    );
    // The transport may hold this value while a slow peer consumes its bytes.
    drop(response);
    // What the answer held is given back the moment it is dropped.
    assert_eq!(
        memory.stats().by_kind[BudgetKind::Control as usize],
        before.by_kind[BudgetKind::Control as usize]
    );
    // And nothing is kept: the budget comes back to where it was. Not at
    // once: the owner goes on leading, and a write of its own in flight (a
    // beat, a commit it settles) holds its `Pending` until written — under
    // sixteen copies at once one held 464 bytes at the moment of the drop.
    // Waited for in the owner's periods.
    let mut wait = focal_timing::ProgressDeadline::begin(&[host.periods()], 100, FROZEN);
    while memory.stats() != before {
        if let Err(spent) = wait.check(&[host.periods()]) {
            panic!(
                "the budget never came back: {spent}; {:?} against {before:?}",
                memory.stats()
            );
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    host.stop().await.unwrap();
    owner.join().unwrap();
}

#[tokio::test]
async fn follower_root_observation_exports_one_durable_prefix_and_retains_delivery_budget_after_stop()
 {
    let data = tempfile::tempdir().unwrap();
    let authority = BootstrapAuthority::open_or_create(
        data.path().join("ca"),
        CLUSTER,
        vec!["localhost".into()],
        now(),
    )
    .unwrap();
    let key = JoinKey::open_or_create(data.path().join("key"), CLUSTER).unwrap();
    let founder = FoundingEnrollmentDraft::open_or_create(
        data.path().join("founder"),
        &authority,
        &key,
        1,
        OPERATOR,
        EnrollmentLimits::default(),
        0,
        now(),
    )
    .unwrap();
    let root = RootDirectory::new(ClusterId(CLUSTER), RootConfig::default(), budget()).unwrap();
    let bootstrap = ControlBootstrap::root(&root, founder.registry()).unwrap();
    let options = ControlOptions::new(NodeConfig::single(1, CLUSTER, GROUP));
    let path = data.path().join("log");
    let mut replica =
        ControlReplica::open(options.clone(), bootstrap.clone(), budget(), &path).unwrap();
    replica.drain(&RejectUnverifiedEvidence).unwrap();
    replica.campaign().unwrap();
    replica.drain(&RejectUnverifiedEvidence).unwrap();
    let commit = |replica: &mut ControlReplica, request: ControlRequest| {
        let id = request.id;
        replica.submit(request, &RejectUnverifiedEvidence).unwrap();
        for _ in 0..8 {
            replica.drain(&RejectUnverifiedEvidence).unwrap();
            if let Some(receipt) = replica.receipt(id).unwrap() {
                return receipt;
            }
        }
        panic!("single-voter control proposal did not durably publish");
    };
    let region = commit(&mut replica, request(1, region(0, 1)));
    let contact = commit(
        &mut replica,
        request(
            2,
            ControlCommand::NodeContact(NodeContactCommand {
                node: 1,
                principal: OPERATOR,
                certificate_fingerprint: certificate_fingerprint(&founder.receipt().certificate),
                advertise: "127.0.0.1:7443".parse().unwrap(),
                expected_generation: 0,
                decided_at: now(),
                region: None,
                zone: None,
                endpoint: None,
            }),
        ),
    );
    let configuration = replica.configuration();
    let membership = commit(
        &mut replica,
        request(
            3,
            ControlCommand::Membership(ControlMembershipCommand {
                expected_configuration_index: configuration.configuration_index,
                expected: configuration.configuration,
                change: MembershipChange::AddLearner { node: 2 },
            }),
        ),
    );
    replica.checkpoint().unwrap();
    drop(replica);
    // Reopen without campaigning. All exported rows are recovered from disk,
    // while this owner has no current leader or quorum-read authority.
    let memory = budget();
    let replica = ControlReplica::open(options, bootstrap, memory.clone(), &path).unwrap();
    let mut config = ControlHostConfig::new(namespace());
    config.tick = Duration::from_secs(1);
    let (host, owner, outgoing) =
        ControlHost::spawn(replica, RejectUnverifiedEvidence, config, memory.clone()).unwrap();
    // What was durable is applied when the owner opens, before its first
    // period: the wait is for the owner's thread to have run at all.
    let mut wait = focal_timing::ProgressDeadline::begin(&[host.periods()], 30, FROZEN);
    while host.progress().applied_index != membership.committed_index {
        if let Err(spent) = wait.check(&[host.periods()]) {
            panic!(
                "what was durable was never applied: {spent}; {:?}",
                host.progress()
            );
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert_eq!(host.progress().leader, 0);
    assert!(matches!(
        host.read(
            peer(PeerRole::Node { node_id: 1 }),
            RequestId::from_u128(90),
            ControlRead::State
        )
        .await,
        Err(ControlFailure::NotLeader { .. })
    ));
    let before = memory.stats();
    // The owner shares the budget: what it reserves for a tick it gives
    // back when the tick is over, and room given back after the budget was
    // filled admits an observation. An observation that was admitted is
    // delivered and dropped, and the budget is filled again, until one is
    // asked with no room: that one is refused.
    let mut refused = false;
    for _ in 0..64 {
        let exhausted = exhaust(&memory);
        let observed = host.observe_root().await;
        drop(exhausted);
        match observed {
            Err(ControlFailure::Capacity) => {
                refused = true;
                break;
            }
            Ok(observation) => drop(observation),
            Err(error) => panic!(
                "an observation failed for no room of its own: {error:?}; the owner: {:?}",
                host.progress().failure
            ),
        }
    }
    assert!(refused, "an observation was never refused for room");
    assert_eq!(
        memory.stats().by_kind[focal_memory::BudgetKind::Control as usize],
        before.by_kind[focal_memory::BudgetKind::Control as usize]
    );
    let mut cancelled = Box::pin(host.observe_root());
    drop(std::future::Future::poll(
        cancelled.as_mut(),
        &mut std::task::Context::from_waker(std::task::Waker::noop()),
    ));
    drop(cancelled);
    // The FIFO barrier also covers an observation whose receiver disappeared
    // before delivery; neither input nor exported state may leak its allowance.
    drop(host.observe_root().await.unwrap());
    assert_eq!(
        memory.stats().by_kind[focal_memory::BudgetKind::Control as usize],
        before.by_kind[focal_memory::BudgetKind::Control as usize]
    );
    let mut pending = Box::pin(host.observe_root());
    let immediate = match std::future::Future::poll(
        pending.as_mut(),
        &mut std::task::Context::from_waker(std::task::Waker::noop()),
    ) {
        std::task::Poll::Ready(result) => Some(result.unwrap()),
        std::task::Poll::Pending => None,
    };
    // A second FIFO observation proves the first was delivered. Keep it in
    // its unpolled oneshot unless the owner already won the first poll race.
    let delivered = host.observe_root().await.unwrap();
    assert_eq!(
        delivered.snapshot().applied_index,
        membership.committed_index
    );
    assert_eq!(
        delivered.contacts().applied_index,
        delivered.snapshot().applied_index
    );
    assert_eq!(
        delivered.configuration().applied_index,
        delivered.snapshot().applied_index
    );
    assert_eq!(delivered.contacts().identity, delivered.snapshot().identity);
    assert_eq!(
        delivered.configuration().identity,
        delivered.snapshot().identity
    );
    assert_eq!(delivered.snapshot().revisions.root, region.revisions.root);
    assert_eq!(delivered.snapshot().revisions.enrollment, 1);
    assert_eq!(registry(delivered.snapshot()).enrollments().count(), 1);
    assert_eq!(delivered.contacts().contacts.records.len(), 1);
    assert_eq!(
        delivered.contacts().contacts.records[0].committed_index,
        contact.committed_index
    );
    assert_eq!(
        delivered.contacts().contacts.records[0].advertise,
        "127.0.0.1:7443".parse().unwrap()
    );
    assert_eq!(
        delivered.configuration().configuration_index,
        membership.committed_index
    );
    assert_eq!(delivered.configuration().configuration.voters, vec![1]);
    assert_eq!(delivered.configuration().configuration.learners, vec![2]);
    let two = memory.stats();
    assert!(
        two.by_kind[focal_memory::BudgetKind::Control as usize]
            > before.by_kind[focal_memory::BudgetKind::Control as usize]
    );
    drop(delivered);
    let queued = memory.stats();
    assert!(queued.used > before.used);
    assert!(queued.used < two.used);
    host.stop().await.unwrap();
    owner.join().unwrap();
    drop(outgoing);
    let stopped = memory.stats();
    assert!(
        stopped.used > 0,
        "unconsumed oneshot lost its exported-state reservation"
    );
    let observation = match immediate {
        Some(value) => value,
        None => pending.await.unwrap(),
    };
    assert_eq!(
        memory.stats(),
        stopped,
        "delivery itself must not release the reservation"
    );
    assert_eq!(
        observation.contacts().contacts.records[0].committed_index,
        contact.committed_index
    );
    assert_eq!(
        observation.configuration().configuration_index,
        membership.committed_index
    );
    drop(observation);
    assert_eq!(memory.stats().used, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn membership_requires_runtime_and_returns_only_committed_configuration_receipts() {
    let keys = tempfile::tempdir().unwrap();
    let authority = BootstrapAuthority::open_or_create(
        keys.path().join("ca"),
        CLUSTER,
        vec!["localhost".into()],
        now(),
    )
    .unwrap();
    let mut rig = Rig::new(root_bootstrap(&authority), GROUP);
    rig.hosts[0].campaign().await.unwrap();
    let leader = rig.leader(0).await;
    let (leader, ControlReadResult::Configuration(before)) = rig
        .read_on_leader(leader, 910, ControlRead::Configuration)
        .await
    else {
        panic!("configuration")
    };
    let add = request(
        1,
        ControlCommand::Membership(ControlMembershipCommand {
            expected_configuration_index: before.configuration_index,
            expected: before.configuration.clone(),
            change: MembershipChange::AddLearner { node: 4 },
        }),
    );
    let mut leader = leader;
    for role in [
        PeerRole::Actor,
        PeerRole::Evaluator,
        PeerRole::Node { node_id: 2 },
    ] {
        assert!(matches!(
            rig.definite(&mut leader, role, &add, Some(0)).await,
            Err(ControlFailure::Unauthorized)
        ));
    }
    let (leader, added) = rig.commit_on_leader(leader, add.clone(), 0).await;
    let (leader, ControlReadResult::Configuration(after)) = rig
        .read_on_leader(leader, 911, ControlRead::Configuration)
        .await
    else {
        panic!("configuration")
    };
    assert_eq!(after.configuration_index, added.committed_index);
    assert_eq!(after.configuration.learners, vec![4]);
    // The entry that changed the configuration is on record, the same on
    // the leader and on a follower: what the group's voters attest to the
    // root when its grant follows the log (F24).
    let (leader, ControlReadResult::MembershipRecord(Some(record))) = rig
        .read_on_leader(leader, 912, ControlRead::MembershipRecord)
        .await
    else {
        panic!("membership record")
    };
    assert_eq!(record.index, added.committed_index);
    assert_eq!(record.term, added.committed_term);
    assert_eq!(record.request_hash, added.request_hash);
    assert_eq!(record.configuration, after.configuration);
    // A follower answers the exact request from what was committed.
    let mut follower = (leader + 1) % 3;
    assert_eq!(
        rig.definite(&mut follower, PeerRole::Runtime, &add, None)
            .await
            .unwrap(),
        added
    );
    // The absent fourth replica cannot be promoted merely because admission committed.
    let promote = request(
        2,
        ControlCommand::Membership(ControlMembershipCommand {
            expected_configuration_index: after.configuration_index,
            expected: after.configuration.clone(),
            change: MembershipChange::Promote { node: 4 },
        }),
    );
    assert!(
        rig.hosts[leader]
            .submit(peer(PeerRole::Runtime), promote)
            .await
            .is_err()
    );
    let remove = request(
        2,
        ControlCommand::Membership(ControlMembershipCommand {
            expected_configuration_index: after.configuration_index,
            expected: after.configuration,
            change: MembershipChange::Remove { node: 4 },
        }),
    );
    let mut leader = leader;
    let removed = rig
        .definite(&mut leader, PeerRole::Runtime, &remove, Some(0))
        .await
        .unwrap();
    let (leader, ControlReadResult::Configuration(current)) = rig
        .read_on_leader(leader, 912, ControlRead::Configuration)
        .await
    else {
        panic!("configuration")
    };
    assert_eq!(current.configuration_index, removed.committed_index);
    assert!(current.configuration.learners.is_empty());
    let target = ((leader + 1) % 3) as u64 + 1;
    let transfer = ControlTransfer {
        expected_configuration_index: current.configuration_index,
        expected: current.configuration,
        target,
    };
    assert!(matches!(
        rig.hosts[leader]
            .transfer(
                peer(PeerRole::Node { node_id: target }),
                RequestId::from_u128(913),
                transfer.clone()
            )
            .await,
        Err(ControlFailure::Unauthorized)
    ));
    // A transfer asks the target to campaign; on a machine that starves
    // its owners another voter may have campaigned first. Leadership is
    // handed on from whoever leads until the target leads.
    let mut leads = leader;
    let mut led = false;
    for attempt in 0..16u128 {
        let asked = rig.hosts[leads]
            .transfer(
                peer(PeerRole::Runtime),
                RequestId::from_u128(914 + attempt),
                transfer.clone(),
            )
            .await;
        assert!(
            matches!(
                asked,
                Ok(_) | Err(ControlFailure::NotLeader { .. } | ControlFailure::Unavailable)
            ),
            "{asked:?}"
        );
        leads = rig.leader(rig.hosts[leads].progress().node).await;
        if rig.hosts[leads].progress().node == target {
            led = true;
            break;
        }
    }
    assert!(led, "the target of a transfer never led");
    rig.stop().await;
}

#[tokio::test]
async fn recovered_control_events_are_forwarded_once_and_keep_frames_charged_after_owner_stop() {
    let data = tempfile::tempdir().unwrap();
    let authority = BootstrapAuthority::open_or_create(
        data.path().join("ca"),
        CLUSTER,
        vec!["localhost".into()],
        now(),
    )
    .unwrap();
    let memory = budget();
    let options = ControlOptions::new(NodeConfig::joining(
        1,
        CLUSTER,
        GROUP,
        vec![1, 2, 3],
        vec![],
    ));
    let mut replica = ControlReplica::open(
        options,
        root_bootstrap(&authority),
        memory.clone(),
        data.path().join("log"),
    )
    .unwrap();
    replica.drain(&RejectUnverifiedEvidence).unwrap();
    replica.campaign().unwrap();
    let initial = replica.drain(&RejectUnverifiedEvidence).unwrap();
    // Use real raft-rs campaign output, already drained by a trusted startup
    // owner. It must survive handoff even though RawNode no longer owns Ready.
    let mut expected: Vec<_> = initial
        .messages
        .iter()
        .map(|message| {
            (
                message.to,
                focal_consensus::encode_message(message).unwrap(),
            )
        })
        .collect();
    expected.sort();
    assert!(!expected.is_empty());
    let mut config = ControlHostConfig::new(namespace());
    config.tick = Duration::from_secs(1);
    let (host, owner, mut outgoing) = ControlHost::spawn_recovered(
        replica,
        RejectUnverifiedEvidence,
        config,
        memory.clone(),
        initial,
    )
    .unwrap();
    let mut frames = Vec::new();
    let mut actual = Vec::new();
    for _ in 0..expected.len() {
        // The owner frames what was recovered when it starts, before its
        // first period.
        let mut wait = focal_timing::ProgressDeadline::begin(&[host.periods()], 30, FROZEN);
        let frame = loop {
            match outgoing.try_recv() {
                Ok(frame) => break frame,
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {}
                Err(error) => panic!("the owner closed what it sends on: {error}"),
            }
            if let Err(spent) = wait.check(&[host.periods()]) {
                panic!("what was recovered was never framed: {spent}");
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        };
        let Operation::Raft { group, message } = &frame.request.operation else {
            panic!("replication frame");
        };
        assert_eq!(*group, GROUP);
        actual.push((frame.target, message.clone()));
        frames.push(frame);
    }
    actual.sort();
    assert_eq!(actual, expected);
    // An observation forces a later drain; it must not replay the initial batch.
    drop(host.observe_root().await.unwrap());
    assert!(matches!(
        outgoing.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
    host.stop().await.unwrap();
    owner.join().unwrap();
    drop(outgoing);
    assert!(memory.stats().by_kind[focal_memory::BudgetKind::Control as usize] > 0);
    drop(frames);
    assert_eq!(memory.stats().used, 0);
}

/// An owner that is refused the room is not ticked and drains nothing, and
/// goes on: the periods pass, it says how many of them passed without a
/// tick, and it leads and answers once the room is back. Before, it stopped for
/// the refusal of one tick or of one drain.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_owner_refused_the_room_waits_and_goes_on() {
    let data = tempfile::tempdir().unwrap();
    let authority = BootstrapAuthority::open_or_create(
        data.path().join("ca"),
        CLUSTER,
        vec!["localhost".into()],
        now(),
    )
    .unwrap();
    let memory = budget();
    let replica = ControlReplica::open(
        ControlOptions::new(NodeConfig::single(1, CLUSTER, GROUP)),
        root_bootstrap(&authority),
        memory.clone(),
        data.path().join("log"),
    )
    .unwrap();
    let mut config = ControlHostConfig::new(namespace());
    config.tick = Duration::from_millis(10);
    let (host, owner, _outgoing) =
        ControlHost::spawn(replica, RejectUnverifiedEvidence, config, memory.clone()).unwrap();
    let answers = async || {
        host.read(
            peer(PeerRole::Runtime),
            RequestId::from_u128(1),
            ControlRead::State,
        )
        .await
    };
    host.campaign().await.unwrap();
    let mut wait = focal_timing::ProgressDeadline::begin(&[host.periods()], 500, FROZEN);
    while answers().await.is_err() {
        if let Err(spent) = wait.check(&[host.periods()]) {
            panic!("the owner never led: {spent}; {:?}", host.progress());
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // No room, for fifty periods of the owner. What it frees is taken
    // again, so that it is refused whatever it gives back.
    let (began, refused) = (host.periods(), host.refused_periods());
    let mut held = Vec::new();
    let mut wait = focal_timing::ProgressDeadline::begin(&[began], 5_000, FROZEN);
    while host.periods() < began + 50 {
        held.extend(exhaust(&memory));
        if let Err(spent) = wait.check(&[host.periods()]) {
            panic!(
                "the owner stopped its periods: {spent}; {:?}",
                host.progress()
            );
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let progress = host.progress();
    assert!(!progress.stopped, "{progress:?}");
    assert_eq!(progress.failure, None);
    assert!(
        host.refused_periods() > refused,
        "fifty periods without room, and none of them refused"
    );
    // The room is back: it leads, or is elected again, and answers.
    drop(held);
    let mut wait = focal_timing::ProgressDeadline::begin(&[host.periods()], 2_000, FROZEN);
    let mut asked = false;
    while answers().await.is_err() {
        if !asked && host.progress().leader == 0 {
            asked = host.campaign().await.is_ok();
        }
        if let Err(spent) = wait.check(&[host.periods()]) {
            panic!(
                "the owner never answered again: {spent}; {:?}",
                host.progress()
            );
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(host.progress().failure, None);
    host.stop().await.unwrap();
    owner.join().unwrap();
}

/// What comes while the owner decides another command waits its turn and
/// is decided after it, in the order it came, each given its request time
/// from its turn. A replica decides one command at a time, and what came
/// meanwhile was refused for capacity: an operator's `membership remove`
/// that met a placement intent of the node's own was told `[capacity]` (the
/// macOS run of 2026-10-01), as four of these five writes are without the
/// turn, and the transfer after them. Then the fifth was given one request
/// time for all five, from when it came, and given up on a slow disk (the
/// macOS run of the day after): here the group commits one write at a time,
/// held apart for more than half the request time each, so that four holds
/// outlast the request time and the fifth write's turn comes only after it;
/// every write is decided. The holds hold heartbeats too, so the replicas
/// are given as many ticks of silence as the request time holds before
/// they campaign; and the request time is four seconds, so that a commit
/// on a starved runner — two seconds after a hold on one macOS run of
/// 2026-10-02, four tests of three owners each sharing two cores — fits
/// beside a hold of half of it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn what_comes_while_the_owner_decides_another_command_waits_its_turn() {
    let keys = tempfile::tempdir().unwrap();
    let authority = BootstrapAuthority::open_or_create(
        keys.path().join("ca"),
        CLUSTER,
        vec!["localhost".into()],
        now(),
    )
    .unwrap();
    const REQUEST: Duration = Duration::from_secs(4);
    let request_periods = focal_timing::ProgressDeadline::periods(REQUEST, RIG_TICK);
    let mut rig = Rig::with_timing(
        root_bootstrap(&authority),
        GROUP,
        usize::try_from(request_periods).unwrap(),
        REQUEST,
    );
    rig.hosts[0].campaign().await.unwrap();
    let leader = rig.leader(0).await;
    let (leader, ControlReadResult::Configuration(configuration)) = rig
        .read_on_leader(leader, 920, ControlRead::Configuration)
        .await
    else {
        panic!("configuration")
    };
    let (leader, state) = rig.state_on_leader(leader).await;
    let revision = state.revisions.root;
    // Five writes at once, each expecting the revision the one before it
    // leaves, and a transfer of leadership behind them.
    let host = rig.hosts[leader].clone();
    // Each write's answer as it arrives, for a failure to name.
    let answered = std::sync::Mutex::new(Vec::new());
    let write = |at: u64| {
        let host = host.clone();
        let answered = &answered;
        async move {
            let answer = host
                .submit(
                    peer(PeerRole::Runtime),
                    request(at, region(revision + at - 1, u128::from(at))),
                )
                .await;
            answered.lock().unwrap().push((at, format!("{answer:?}")));
            answer
        }
    };
    let target = (leader as u64 + 1) % 3 + 1;
    let transfer = host.transfer(
        peer(PeerRole::Runtime),
        RequestId::from_u128(921),
        ControlTransfer {
            expected_configuration_index: configuration.configuration_index,
            expected: configuration.configuration.clone(),
            target,
        },
    );
    // One commit at a time: the routers hold the group at each, for more
    // than half the request time in the leader's periods, before the next
    // is let through; four holds then outlast the request time.
    let hold = request_periods / 2 + 1;
    assert!(hold * 4 > request_periods && hold * 2 < request_periods + request_periods / 2);
    let applied = host.progress().applied_index;
    rig.allowed.store(applied + 1, Ordering::SeqCst);
    let paced = async {
        for written in 1..=5 {
            let mut wait = rig.deadline();
            while host.progress().applied_index < applied + written {
                if let Err(spent) = wait.check(&rig.periods()) {
                    panic!(
                        "write {written} never applied: {spent}; answered {:?}; {:?}",
                        answered.lock().unwrap(),
                        host.progress()
                    );
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            if written == 5 {
                break;
            }
            let from = host.periods();
            let mut wait = focal_timing::ProgressDeadline::begin(&[from], hold + 1, FROZEN);
            while host.periods() < from + hold {
                if let Err(spent) = wait.check(&[host.periods()]) {
                    panic!("the leader's periods stopped: {spent}");
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            rig.allowed.store(applied + written + 1, Ordering::SeqCst);
        }
        rig.allowed.store(u64::MAX, Ordering::SeqCst);
    };
    let (first, second, third, fourth, fifth, transferred, ()) = tokio::join!(
        write(1),
        write(2),
        write(3),
        write(4),
        write(5),
        transfer,
        paced
    );
    let receipts: Vec<ControlReceipt> = [first, second, third, fourth, fifth]
        .into_iter()
        .map(|answer| answer.expect("a write that waited its turn"))
        .collect();
    // Decided in the order they came.
    assert!(
        receipts
            .windows(2)
            .all(|pair| pair[0].committed_index < pair[1].committed_index),
        "{receipts:?}"
    );
    assert_eq!(transferred, Ok(()));
    rig.stop().await;
}

/// A follower's read answered above what it has applied waits for the
/// entries it names (27 §5), and never fails the replica. The follower is
/// sent no entries while the other two commit, so the leader answers its
/// read with a commit the follower has not applied; the barrier was taken
/// for corruption, and a member brought up by snapshot that read before it
/// caught up failed (`a_crowded_partition_splits_survives_a_restart_and_merges_back`,
/// 2026-10-03). Its entries let through, the follower answers the read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_read_answered_ahead_of_what_it_applied_waits_and_never_fails_it() {
    let keys = tempfile::tempdir().unwrap();
    let authority = BootstrapAuthority::open_or_create(
        keys.path().join("ca"),
        CLUSTER,
        vec!["localhost".into()],
        now(),
    )
    .unwrap();
    let mut rig = Rig::new(root_bootstrap(&authority), GROUP);
    rig.hosts[0].campaign().await.unwrap();
    let leader = rig.leader(0).await;
    let follower = (leader + 1) % 3;
    let follower_node = rig.hosts[follower].progress().node;
    rig.withheld.store(follower_node, Ordering::SeqCst);
    // The other two commit what the follower is not sent. Leadership may
    // move between them under load; the follower, behind, cannot take it,
    // and the asks follow whichever of the two leads.
    let mut index = leader;
    for sequence in 1..=3 {
        let revision = rig.hosts[index].progress().revisions.root;
        rig.definite(
            &mut index,
            PeerRole::Runtime,
            &request(sequence, region(revision, u128::from(9_000 + sequence))),
            Some(follower_node),
        )
        .await
        .unwrap();
    }
    assert!(
        rig.hosts[follower].progress().applied_index < rig.hosts[index].progress().applied_index
    );
    // The follower's read is answered by the leader with a commit it has
    // not applied: the read waits, and the replica goes on.
    let asked = rig.hosts[follower]
        .read(
            peer(PeerRole::Runtime),
            RequestId::from_u128(9_100),
            ControlRead::Membership,
        )
        .await;
    assert!(asked.is_err(), "{asked:?}");
    let progress = rig.hosts[follower].progress();
    assert!(
        progress.failure.is_none() && !progress.stopped,
        "the follower failed on a read answered ahead of it: {:?}",
        progress.failure
    );
    // Its entries let through, it catches up and answers.
    rig.withheld.store(0, Ordering::SeqCst);
    let mut wait = rig.deadline();
    let mut asked = 9_100u128;
    let answered = loop {
        asked += 1;
        match rig.hosts[follower]
            .read(
                peer(PeerRole::Runtime),
                RequestId::from_u128(asked),
                ControlRead::Membership,
            )
            .await
        {
            Ok(ControlReadResult::Membership(membership)) => break membership,
            Ok(other) => panic!("{other:?}"),
            Err(error) => {
                if let Err(spent) = wait.check(&rig.periods()) {
                    panic!("the follower never answered: {spent}; last {error:?}");
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert_eq!(answered.node, follower_node);
    let progress = rig.hosts[follower].progress();
    assert!(progress.failure.is_none() && !progress.stopped);
    rig.stop().await;
}

/// A follower answers a read through its leader (27 §5). The root's reads
/// were served by its leader alone, so once a root had three voters a node
/// whose root followed another — the founder restarted under its committed
/// policy, asking its own root for the membership before it reports `Ready`;
/// any operator's `membership show` on a non-leader — was refused
/// `not_leader` (the F24 fleet, 2026-10-02).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_answers_a_read_through_its_leader() {
    let keys = tempfile::tempdir().unwrap();
    let authority = BootstrapAuthority::open_or_create(
        keys.path().join("ca"),
        CLUSTER,
        vec!["localhost".into()],
        now(),
    )
    .unwrap();
    let mut rig = Rig::new(root_bootstrap(&authority), GROUP);
    rig.hosts[0].campaign().await.unwrap();
    let mut wait = rig.deadline();
    let mut asked = 930u128;
    // A read asked of a member that does not lead. Under load leadership
    // may move between finding the leader and the answer, and a member that
    // came to lead answers its own read: that answer is true, and is not the
    // case this test asks for, so it asks a member that follows again.
    let (follower, answered) = loop {
        let leader = rig.leader(0).await;
        let follower = (leader + 1) % 3;
        asked += 1;
        match rig.hosts[follower]
            .read(
                peer(PeerRole::Runtime),
                RequestId::from_u128(asked),
                ControlRead::Membership,
            )
            .await
        {
            Ok(ControlReadResult::Membership(membership))
                if membership.leader != membership.node =>
            {
                break (follower, membership);
            }
            Ok(ControlReadResult::Membership(_)) => {}
            Ok(other) => panic!("{other:?}"),
            Err(error) => {
                if let Err(spent) = wait.check(&rig.periods()) {
                    panic!("the follower never answered: {spent}; last {error:?}");
                }
            }
        }
        if let Err(spent) = wait.check(&rig.periods()) {
            panic!("no member that follows answered: {spent}");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert_eq!(answered.node, rig.hosts[follower].progress().node);
    // Answered through a leader: one of the voters and not the follower.
    assert_ne!(answered.leader, 0, "{answered:?}");
    assert!(answered.voters.contains(&answered.leader), "{answered:?}");
    assert_eq!(answered.voters.len(), 3, "{answered:?}");
    rig.stop().await;
}
