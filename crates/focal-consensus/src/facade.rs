//! The owners' API ([27] §15.7): each method of [`DurableNode`] is the method of the same name on
//! the backend the node opened on, and each constructor opens that backend.
//!
//! [27]: ../../../docs/archictecutre/27-consensus-roadmap-and-slates-port.md
use super::*;

impl DurableNode {
    /// A member over hyper-durable's shell ([27] §15.7): its log one group of the node's
    /// hyper-log log `log`, its records and image under the data directory `root`, its memory
    /// charged within `parent_budget` and its disk within `disk`. `needs` names the decoder an
    /// entry of the owner's needs, where it needs one the group's baseline does not give: a write
    /// holding such an entry waits until the group's records state that decoder durable
    /// (27 §15.5, O2). The conversion and the tests open members so; a start below the upgrade
    /// fence never does (27 §15.8).
    pub fn open_on_shell(
        config: NodeConfig,
        root: &Path,
        log: &hyper_log::Log<hyper_block::file::DeviceFile>,
        parent_budget: &MemoryBudget,
        disk: DiskBudget,
        needs: fn(&[u8]) -> Option<[u8; 32]>,
    ) -> Result<Self, ConsensusError> {
        shell_node::ShellNode::open(config, root, log, parent_budget, disk, needs).map(|node| {
            Self {
                backend: Backend::Shell(node),
            }
        })
    }
    pub fn group_id(&self) -> [u8; 16] {
        dispatch!(inner = &self.backend => inner.group_id())
    }
    pub fn cluster_id(&self) -> [u8; 16] {
        dispatch!(inner = &self.backend => inner.cluster_id())
    }
    pub fn open(config: NodeConfig, data_dir: impl AsRef<Path>) -> Result<Self, ConsensusError> {
        LogNode::open(config, data_dir).map(|node| Self {
            backend: Backend::Log(node),
        })
    }
    pub fn open_in(
        config: NodeConfig,
        data_dir: impl AsRef<Path>,
        parent: &MemoryBudget,
    ) -> Result<Self, ConsensusError> {
        LogNode::open_in(config, data_dir, parent).map(|node| Self {
            backend: Backend::Log(node),
        })
    }
    /// `restore_on_wal_in` over a fresh physical WAL at `data_dir`.
    pub fn restore_in(
        config: NodeConfig,
        data_dir: impl AsRef<Path>,
        parent: &MemoryBudget,
        image: RestoredLog,
    ) -> Result<Self, ConsensusError> {
        LogNode::restore_in(config, data_dir, parent, image).map(|node| Self {
            backend: Backend::Log(node),
        })
    }
    /// Begin a logical group's log from a restored image (26 §6): on an
    /// empty logical log, write the group identity, the decoder floor and
    /// transition the image's log promised, the snapshot at the image's
    /// index and term under the bootstrap membership, and a hard state that
    /// commits it; then open the group exactly as a restart would, so the
    /// snapshot is delivered to the application as a recovered one. A log
    /// that already holds any other record is refused: a restore never
    /// overwrites history. One that holds this image and nothing else is
    /// opened: a restore that is issued again goes on where it was cut.
    pub fn restore_on_wal_in(
        config: NodeConfig,
        shared: SharedWal,
        parent_budget: &MemoryBudget,
        image: RestoredLog,
    ) -> Result<Self, ConsensusError> {
        LogNode::restore_on_wal_in(config, shared, parent_budget, image).map(|node| Self {
            backend: Backend::Log(node),
        })
    }
    /// Host many logical groups on the same node-owned physical WAL. Each group
    /// keeps independent Raft authority while sharing disk flush/segment custody.
    pub fn open_on_wal(config: NodeConfig, shared: SharedWal) -> Result<Self, ConsensusError> {
        LogNode::open_on_wal(config, shared).map(|node| Self {
            backend: Backend::Log(node),
        })
    }
    /// What opening a group under `config` charges before its first drain
    /// prices what it holds (`memory::initial_bytes`).
    pub fn initial_estimate(config: &NodeConfig) -> Result<usize, ConsensusError> {
        LogNode::initial_estimate(config)
    }
    /// Charge this group's retained Raft data and operation staging to a tenant
    /// or node hierarchy. The physical shared WAL has its own node-wide budget.
    pub fn open_on_wal_in(
        config: NodeConfig,
        shared: SharedWal,
        parent_budget: &MemoryBudget,
    ) -> Result<Self, ConsensusError> {
        LogNode::open_on_wal_in(config, shared, parent_budget).map(|node| Self {
            backend: Backend::Log(node),
        })
    }
    pub fn campaign(&mut self) -> Result<(), ConsensusError> {
        dispatch!(inner = &mut self.backend => inner.campaign())
    }
    /// What one transition of this node may stage at most, as its guard
    /// reserves it (`memory::staging_bytes` for an operation that brings
    /// nothing): the bound a heartbeat, a read barrier or a report is held
    /// to, whatever the history.
    pub fn staging_estimate(&self) -> Result<usize, ConsensusError> {
        dispatch!(inner = &self.backend => inner.staging_estimate())
    }
    /// The bound a proposal of `incoming` bytes is held to
    /// (`memory::staging_bytes` as its guard reserves it).
    pub fn staging_estimate_for(&self, incoming: usize) -> Result<usize, ConsensusError> {
        dispatch!(inner = &self.backend => inner.staging_estimate_for(incoming))
    }
    pub fn is_budgeted_within(&self, parent: &MemoryBudget) -> bool {
        dispatch!(inner = &self.backend => inner.is_budgeted_within(parent))
    }
    pub fn propose(&mut self, data: Vec<u8>) -> Result<(), ConsensusError> {
        dispatch!(inner = &mut self.backend => inner.propose(data))
    }
    /// Proposes by the fast track (27 §4), from a member that does not
    /// lead: the entry goes to every voter, for the index after what this
    /// member holds, and is committed when a fast quorum holds it or the
    /// leader's classic quorum does, whichever is first. The index it was
    /// proposed for. Success is admission, as of `propose`: the entry is
    /// committed when `drain` gives it, and is given in
    /// `NodeEvents::displaced` when another took its index.
    ///
    /// A leader proposes as it always did.
    pub fn propose_fast(&mut self, data: Vec<u8>) -> Result<u64, ConsensusError> {
        dispatch!(inner = &mut self.backend => inner.propose_fast(data))
    }
    pub fn propose_fast_in(
        &mut self,
        data: Vec<u8>,
        lane: BudgetLane,
    ) -> Result<u64, ConsensusError> {
        dispatch!(inner = &mut self.backend => inner.propose_fast_in(data, lane))
    }
    /// Whether the group has the fast track.
    pub fn fast(&self) -> bool {
        dispatch!(inner = &self.backend => inner.fast())
    }
    /// What the fast track did at this member since it opened.
    pub fn fast_stats(&self) -> hyper_raft::FastStats {
        dispatch!(inner = &self.backend => inner.fast_stats())
    }
    pub fn propose_in(&mut self, data: Vec<u8>, lane: BudgetLane) -> Result<(), ConsensusError> {
        dispatch!(inner = &mut self.backend => inner.propose_in(data, lane))
    }
    /// Submit a caller-retained record without consuming its retry buffer.
    /// The caller keeps that buffer funded until its own commit/rollback fence.
    /// Raft's independent copy and fanout are admitted before allocation; a
    /// capacity refusal leaves the original bytes available for an exact retry.
    /// Success is proposal admission, never an index assignment or commit proof.
    pub fn propose_borrowed_in(
        &mut self,
        data: &[u8],
        lane: BudgetLane,
    ) -> Result<(), ConsensusError> {
        dispatch!(inner = &mut self.backend => inner.propose_borrowed_in(data, lane))
    }
    /// The authenticated envelope must bind cluster/group identity. Peer input is
    /// validated before Raft; unexpected dependency failures stop this replica.
    pub fn step(&mut self, message: Message) -> Result<(), ConsensusError> {
        dispatch!(inner = &mut self.backend => inner.step(message))
    }
    pub fn tick(&mut self) -> Result<(), ConsensusError> {
        dispatch!(inner = &mut self.backend => inner.tick())
    }
    /// Completion arrives in drain after a quorum read barrier. Publication must
    /// reach that index before serving the read; there is no clock lease.
    pub fn read_index(&mut self, context: Vec<u8>) -> Result<(), ConsensusError> {
        dispatch!(inner = &mut self.backend => inner.read_index(context))
    }
    pub fn propose_conf_change(&mut self, change: ConfChangeV2) -> Result<(), ConsensusError> {
        dispatch!(inner = &mut self.backend => inner.propose_conf_change(change))
    }
    /// This node's election priority (27 §5). A voter refuses its vote, and
    /// its pre-vote, to a candidate of lower priority unless that candidate's
    /// log is strictly longer than the voter's, so among equally current
    /// members the one of highest priority wins and a lower one that times
    /// out first does not take the group. Priority never outranks the log:
    /// the election restriction is unchanged, and a group whose highest
    /// priority member is gone still elects among the rest. It is policy the
    /// owner sets from committed placement, not part of the node's durable
    /// identity.
    ///
    /// It takes effect once this node has a term. A node still at term zero
    /// has no log to defend, and its refusal would bear no term a candidate
    /// could hear: a group's first election is decided by timeouts alone.
    ///
    /// A lower priority is voted for all the same when its log is more
    /// current than the voter's, by its last term and then its length
    /// (`hyper_raft::Precedence::Log`): a voter that refuses for priority
    /// could then have been elected itself, so priority never leaves a group
    /// that can elect without a leader.
    pub fn set_priority(&mut self, priority: i64) -> Result<(), ConsensusError> {
        dispatch!(inner = &mut self.backend => inner.set_priority(priority))
    }
    /// The bounds this member's core holds its queues to, derived from what focal states of it
    /// ([`crate::CoreLimits`]).
    pub fn limits(&self) -> crate::CoreLimits {
        dispatch!(inner = &self.backend => inner.limits())
    }
    /// The fields beyond raft-rs's this member's peers carry, from now on (`Wire`): raised by
    /// focal-node once the upgrade fence opens `RAFT_KEPT_LEVEL`, before a member opened under
    /// that fence sends. Under `Wire::Kept` the member reads a refusal's `kept` and `lost` and
    /// keeps what arrives ahead of a hole (R17).
    pub fn set_raft_wire(&mut self, wire: Wire) -> Result<(), ConsensusError> {
        dispatch!(inner = &mut self.backend => inner.set_raft_wire(wire))
    }
    /// What this member's messages are encoded under (`encode_message_in`).
    pub fn wire(&self) -> Wire {
        dispatch!(inner = &self.backend => inner.wire())
    }
    /// While this member leads, `peer` is sent no more than `bytes` of
    /// entries ahead of its answers: what its owner learned the path to it
    /// carries ([27 §11]). One entry that is larger is still sent, alone,
    /// and a bound of nothing is one byte. Never more than this group's
    /// budget can stage in one transition beside a page for every member:
    /// a window the budget cannot stage would refuse every answer of the
    /// member it was made for. False for a peer the configuration does not
    /// name. The bound is kept until it is said again; a member the
    /// configuration makes anew begins at one page (`page_bytes`).
    ///
    /// [27 §11]: ../../docs/archictecutre/27-consensus-roadmap-and-slates-port.md
    pub fn set_inflight_bytes(&mut self, peer: u64, bytes: u64) -> Result<bool, ConsensusError> {
        dispatch!(inner = &mut self.backend => inner.set_inflight_bytes(peer, bytes))
    }
    /// The bytes of entries one message carries at most, and one entry at
    /// least: what a member is sent ahead of its answers until its owner
    /// says what the path to it carries.
    pub fn page_bytes(&self) -> u64 {
        dispatch!(inner = &self.backend => inner.page_bytes())
    }
    /// The bytes of entries in flight to `peer` and the bound on them, as
    /// this member leads; `None` for a peer the configuration does not name.
    pub fn inflight_bytes(&self, peer: u64) -> Option<(u64, u64)> {
        dispatch!(inner = &self.backend => inner.inflight_bytes(peer))
    }
    /// Ticks without leader contact before this node campaigns.
    /// Ticks between a leader's heartbeats.
    pub fn heartbeat_tick(&self) -> usize {
        dispatch!(inner = &self.backend => inner.heartbeat_tick())
    }
    /// A leader sends its heartbeats now; any other role does nothing. For
    /// an owner whose tick period is stretched (27 §3.1 P2): the election
    /// timeout follows the period, and the heartbeats keep the cadence the
    /// followers were configured to expect.
    pub fn beat(&mut self) -> Result<(), ConsensusError> {
        dispatch!(inner = &mut self.backend => inner.beat())
    }
    pub fn election_tick(&self) -> usize {
        dispatch!(inner = &self.backend => inner.election_tick())
    }
    /// Whether a read asked here waits for a round of heartbeats that has
    /// not left: it leaves with the next drain, carrying every read asked
    /// by then (`hyper_raft::Raft::ask_reads`). An owner with more work
    /// already queued takes it first, so that reads queued together are
    /// confirmed by one round and not by one each.
    pub fn reads_unasked(&self) -> bool {
        dispatch!(inner = &self.backend => inner.reads_unasked())
    }
    /// The reads asked here that wait for a quorum to confirm them.
    pub fn reads_waiting(&self) -> usize {
        dispatch!(inner = &self.backend => inner.reads_waiting())
    }
    /// The reads this member may hold in flight: the core's own bound, which
    /// a follower's parked read barriers share (27 §5, follower reads).
    pub fn pending_reads(&self) -> usize {
        dispatch!(inner = &self.backend => inner.pending_reads())
    }
    /// The messages this member lets one peer have in flight at once
    /// (`NodeConfig::max_inflight_messages`): what an owner admits of a
    /// peer's traffic beside its participants (F56).
    pub fn inflight_window(&self) -> usize {
        dispatch!(inner = &self.backend => inner.inflight_window())
    }
    /// The ticks this member waits beyond its election timeout before it
    /// campaigns (`hyper_raft::Raft::set_patience`): what its owner gives
    /// it for the stalls it has seen in itself.
    pub fn set_patience(&mut self, ticks: usize) -> Result<(), ConsensusError> {
        dispatch!(inner = &mut self.backend => inner.set_patience(ticks))
    }
    /// The priority this node was given; in force once it has a term.
    pub fn priority(&self) -> i64 {
        dispatch!(inner = &self.backend => inner.priority())
    }
    /// The priority votes are judged by now.
    pub fn effective_priority(&self) -> i64 {
        dispatch!(inner = &self.backend => inner.effective_priority())
    }
    pub fn transfer_leader(&mut self, node: u64) -> Result<(), ConsensusError> {
        dispatch!(inner = &mut self.backend => inner.transfer_leader(node))
    }
    /// Deterministic election pacing for harnesses: the follower with the
    /// shortest timeout campaigns first once every lease has expired.
    #[cfg(any(test, feature = "test-support"))]
    pub fn set_randomized_election_timeout(&mut self, ticks: usize) -> Result<(), ConsensusError> {
        dispatch!(inner = &mut self.backend => inner.set_randomized_election_timeout(ticks))
    }
    pub fn report_unreachable(&mut self, node: u64) -> Result<(), ConsensusError> {
        dispatch!(inner = &mut self.backend => inner.report_unreachable(node))
    }
    pub fn report_snapshot(
        &mut self,
        node: u64,
        status: SnapshotStatus,
    ) -> Result<(), ConsensusError> {
        dispatch!(inner = &mut self.backend => inner.report_snapshot(node, status))
    }
    /// Apply delayed transport feedback only to the snapshot still pending for
    /// this peer in this leadership term. Acceptance never proves installation.
    pub fn report_snapshot_at(
        &mut self,
        node: u64,
        term: u64,
        index: u64,
        status: SnapshotStatus,
    ) -> Result<(), ConsensusError> {
        dispatch!(inner = &mut self.backend => inner.report_snapshot_at(node, term, index, status))
    }
    /// Install the application's complete state at its delivered prefix, retaining
    /// the Raft suffix until the new WAL generation and fence are durable.
    pub fn checkpoint(&mut self, index: u64, data: Vec<u8>) -> Result<(), ConsensusError> {
        dispatch!(inner = &mut self.backend => inner.checkpoint(index, data))
    }
    /// Term of a fully published durable prefix, independent of a newer
    /// election term. Snapshot-prefix consumers must not substitute status.term.
    pub fn published_term(&self, index: u64) -> Result<u64, ConsensusError> {
        dispatch!(inner = &self.backend => inner.published_term(index))
    }
    /// Whether the entry the current term began with is at or below `index`, a published entry:
    /// what a newer term proves of a former leader's proposals holds only past it (hyper-raft S-4;
    /// the backends' `term_began_by`).
    pub fn term_began_by(&self, index: u64) -> Result<bool, ConsensusError> {
        dispatch!(inner = &self.backend => inner.term_began_by(index))
    }
    /// Decode through the bounded prost codec and bind the Raft sender to the
    /// authenticated transport principal before any state-machine transition.
    pub fn step_authenticated(
        &mut self,
        peer_node_id: u64,
        encoded: &[u8],
    ) -> Result<(), ConsensusError> {
        dispatch!(inner = &mut self.backend => inner.step_authenticated(peer_node_id, encoded))
    }
    /// Immutable bootstrap membership validated against the durable identity
    /// record on every open. This is independent of the current configuration.
    pub fn bootstrap_membership(&self) -> (&[u64], &[u64]) {
        dispatch!(inner = &self.backend => inner.bootstrap_membership())
    }
    /// Free bytes on the filesystem holding this replica's WAL, sampled now.
    pub fn disk_available_bytes(&self) -> Result<u64, ConsensusError> {
        dispatch!(inner = &self.backend => inner.disk_available_bytes())
    }
    /// The volume envelope this group's WAL promises its writes from; a
    /// session's checkpoint seeds share it (25 §5).
    pub fn disk_budget(&self) -> DiskBudget {
        dispatch!(inner = &self.backend => inner.disk_budget())
    }
    pub fn status(&self) -> NodeStatus {
        dispatch!(inner = &self.backend => inner.status())
    }
    /// The status's scalars, copied: a check that needs no membership
    /// allocates nothing (the audit's F53).
    pub fn scalars(&self) -> NodeScalars {
        dispatch!(inner = &self.backend => inner.scalars())
    }
    /// The membership as this node holds it, borrowed.
    pub fn membership(&self) -> MembershipView<'_> {
        dispatch!(inner = &self.backend => inner.membership())
    }
    /// Per-peer replication progress this node tracks as leader (empty when not
    /// leading). Diagnostic only; it reflects in-memory Raft progress and is
    /// never persisted or replicated.
    pub fn peer_progress(&self) -> Vec<PeerProgress> {
        dispatch!(inner = &self.backend => inner.peer_progress())
    }
    /// What this leader tracks of one member's replication; nothing when
    /// it does not lead or tracks no such member. Allocates nothing, so an
    /// owner may ask on every tick.
    pub fn peer(&self, node: u64) -> Option<PeerProgress> {
        dispatch!(inner = &self.backend => inner.peer(node))
    }
    /// The member this leader is handing leadership to, while it is.
    pub fn transferring(&self) -> Option<u64> {
        dispatch!(inner = &self.backend => inner.transferring())
    }
    /// A configuration change is in the log and not applied yet.
    pub fn configuration_pending(&self) -> bool {
        dispatch!(inner = &self.backend => inner.configuration_pending())
    }
    /// The index of the stored snapshot the log is compacted behind (zero
    /// while the log is complete): a member added after it can only be
    /// seeded by a later snapshot, since Raft discards one whose
    /// configuration does not name the recipient.
    pub fn snapshot_index(&self) -> u64 {
        dispatch!(inner = &self.backend => inner.snapshot_index())
    }
    /// The index of the last entry this node's log holds, durable or not:
    /// an append that names an entry past it is refused.
    pub fn last_index(&self) -> Result<u64, ConsensusError> {
        dispatch!(inner = &self.backend => inner.last_index())
    }
    /// Whether the stored snapshot names every member of the configuration
    /// this node has applied. Raft discards a snapshot that does not name
    /// its recipient, so a member added after the log was compacted is
    /// seeded only by a later snapshot; a log complete from its first entry
    /// seeds anyone. A change that only promotes or removes leaves every
    /// member named.
    pub fn snapshot_names_every_member(&self) -> bool {
        dispatch!(inner = &self.backend => inner.snapshot_names_every_member())
    }
    /// A leader cannot complete a quorum ReadIndex until it has committed an
    /// entry in its current term. Ingress uses this to defer readiness probes.
    pub fn has_committed_current_term(&self) -> bool {
        dispatch!(inner = &self.backend => inner.has_committed_current_term())
    }
    pub fn inject_fault_once(&mut self, point: FaultPoint) -> Result<(), ConsensusError> {
        dispatch!(inner = &mut self.backend => inner.inject_fault_once(point))
    }
    /// Whether this node stopped on a dependency or persistence failure and
    /// must be reopened. A refusal that changed nothing leaves it false, so a
    /// caller can tell "try again" from "this replica is gone".
    pub fn failed(&self) -> bool {
        dispatch!(inner = &self.backend => inner.failed())
    }
    pub fn checkpoint_pending(&self) -> bool {
        dispatch!(inner = &self.backend => inner.checkpoint_pending())
    }
    /// Consume an already funded application checkpoint buffer. The original
    /// allowance remains held through preparation and every refusal, without
    /// another reservation for that caller-owned input. The independent Raft
    /// snapshot and WAL copies still require the usual consensus staging.
    /// This starts the same nonblocking checkpoint; completion requires its
    /// actual durable fence through `try_finish_checkpoint`/`finish_checkpoint`.
    pub fn begin_checkpoint_funded(
        &mut self,
        index: u64,
        data: Vec<u8>,
        allocation: Allocation,
    ) -> Result<(), ConsensusError> {
        dispatch!(inner = &mut self.backend => inner.begin_checkpoint_funded(index, data, allocation))
    }
    /// Prepare an exact published-prefix checkpoint without waiting for disk.
    /// No RawNode mutation is allowed until its ticket completes or this still
    /// unadmitted checkpoint is explicitly canceled.
    pub fn begin_checkpoint(&mut self, index: u64, data: Vec<u8>) -> Result<(), ConsensusError> {
        dispatch!(inner = &mut self.backend => inner.begin_checkpoint(index, data))
    }
    /// Interest may be canceled before admission. An admitted rewrite must
    /// still reach its exact durable fence before this RawNode becomes mutable.
    pub fn cancel_unadmitted_checkpoint(&mut self) -> bool {
        dispatch!(inner = &mut self.backend => inner.cancel_unadmitted_checkpoint())
    }
    pub fn try_finish_checkpoint(&mut self) -> Result<bool, ConsensusError> {
        dispatch!(inner = &mut self.backend => inner.try_finish_checkpoint())
    }
    pub fn finish_checkpoint(&mut self) -> Result<(), ConsensusError> {
        dispatch!(inner = &mut self.backend => inner.finish_checkpoint())
    }
    /// Register the actual compiled application decoder before replay or Raft
    /// participation. This never persists or advertises a capability. A single
    /// hash cannot confirm recovery containing a decoder transition, even when
    /// that hash equals the effective successor requirement.
    pub fn confirm_decoder(&mut self, hash: [u8; 32]) -> Result<(), ConsensusError> {
        dispatch!(inner = &mut self.backend => inner.confirm_decoder(hash))
    }
    /// Trusted application composition registers exactly one compiled ordered
    /// pair. Both decoders must actually exist; this is not a client capability
    /// claim, a hash allowlist, or an application activation decision. A matching
    /// single predecessor registration may be explicitly widened once. Repeated
    /// registration preserves any pending write and its original receipt.
    pub fn confirm_decoder_pair(
        &mut self,
        predecessor: [u8; 32],
        successor: [u8; 32],
    ) -> Result<(), ConsensusError> {
        dispatch!(inner = &mut self.backend => inner.confirm_decoder_pair(predecessor, successor))
    }
    /// Effective minimum decoder. This does not discard the original baseline
    /// promise, which remains required on recovery and in physical checkpoints.
    pub fn required_decoder(&self) -> Option<[u8; 32]> {
        dispatch!(inner = &self.backend => inner.required_decoder())
    }
    /// Only this predicate authorizes advertising the local durable capability.
    /// After a transition, predecessor readiness also requires confirmation of
    /// the full compiled pair. A retained write suppresses publication until its
    /// exact durable fence is observed by this owner.
    pub fn decoder_floor_ready(&self, hash: [u8; 32]) -> bool {
        dispatch!(inner = &self.backend => inner.decoder_floor_ready(hash))
    }
    /// Stage the original immutable requirement between existing Ready/checkpoint
    /// work. It remains idempotent after a transition; passing the successor here
    /// cannot bypass the separate transition record.
    pub fn begin_decoder_floor(&mut self, hash: [u8; 32]) -> Result<(), ConsensusError> {
        dispatch!(inner = &mut self.backend => inner.begin_decoder_floor(hash))
    }
    /// Stage the one registered transition after the predecessor floor is durable.
    /// Registration alone never authorizes successor publication. Call the same
    /// finish/try_finish_decoder_floor or try_drain methods to retain and observe
    /// this write through queue pressure, caller loss, and the actual fsync fence.
    pub fn begin_decoder_transition(&mut self) -> Result<(), ConsensusError> {
        dispatch!(inner = &mut self.backend => inner.begin_decoder_transition())
    }
    pub fn try_finish_decoder_floor(&mut self) -> Result<bool, ConsensusError> {
        dispatch!(inner = &mut self.backend => inner.try_finish_decoder_floor())
    }
    /// Blocking-owner compatibility path over the same exact receipt. Async
    /// hosts use polling; no runtime blocking pool or auxiliary worker is added.
    pub fn finish_decoder_floor(&mut self) -> Result<(), ConsensusError> {
        dispatch!(inner = &mut self.backend => inner.finish_decoder_floor())
    }
    pub fn membership_configuration(&self) -> MembershipConfiguration {
        dispatch!(inner = &self.backend => inner.membership_configuration())
    }
    /// Admission only. Persist an exact request identity in context, then await
    /// the matching AppliedMembership event before releasing a durable receipt.
    pub fn propose_membership(
        &mut self,
        expected: &MembershipConfiguration,
        change: MembershipChange,
        context: Vec<u8>,
    ) -> Result<(), ConsensusError> {
        dispatch!(inner = &mut self.backend => inner.propose_membership(expected, change, context))
    }
    pub fn shared_wal(&self) -> Result<SharedWal, ConsensusError> {
        dispatch!(inner = &self.backend => inner.shared_wal())
    }
    /// True from Ready acquisition until its full output prefix is released.
    /// Mutations return PersistencePending in this state; no Raft input is lost.
    pub fn persistence_pending(&self) -> bool {
        dispatch!(inner = &self.backend => inner.persistence_pending())
    }
    /// Read-only owner wake predicate, including a retained WAL receipt and
    /// newly queued Ready work that has not yet started persistence.
    pub fn has_ready(&self) -> bool {
        dispatch!(inner = &self.backend => inner.has_ready())
    }
    /// Queue/poll one group's durability without waiting for the physical writer.
    /// None retains the exact Ready, preparation, output and accounting permit.
    /// The caller can poll other groups on the same owner before trying again,
    /// and sends what may be sent meanwhile (`sendable`).
    pub fn try_drain(&mut self) -> Result<Option<NodeEvents>, ConsensusError> {
        dispatch!(inner = &mut self.backend => inner.try_drain())
    }
    /// Synchronous compatibility path over the same persistence state machine.
    /// Waits on each exact receipt without a timer, spin loop, or runtime bridge.
    /// An owner that sends for a leader does not wait here: it polls
    /// (`try_drain`), sends what may be sent (`sendable`) and waits for the
    /// write (`wait_persisted`).
    pub fn drain(&mut self) -> Result<NodeEvents, ConsensusError> {
        dispatch!(inner = &mut self.backend => inner.drain())
    }
    /// The messages gathered so far that may be sent while this group's
    /// write is in flight: a leader's, which its members persist for
    /// themselves (Ongaro's thesis §10.2.1) — its own write and theirs then
    /// overlap, and the entry commits when the later of them is durable —
    /// and those of a write this drain already saw durable. None of them
    /// answers for the write in flight: a follower's acknowledgement and a
    /// vote are given once what they answer for is durable, by the drain
    /// that follows. Nothing else is said early: the events of a drain are
    /// given whole, with nothing still to persist, as its owners take them.
    /// A snapshot is left for that drain too: its owner answers for what
    /// became of it (`report_snapshot`), which the member takes only once
    /// its write is done.
    pub fn sendable(&mut self) -> Result<Option<NodeEvents>, ConsensusError> {
        dispatch!(inner = &mut self.backend => inner.sendable())
    }
    /// What this group applies, its members act on when they next start,
    /// before the group has told them anything: so nothing is applied, and
    /// nothing said to have committed, on a commit the log does not hold
    /// (the module's header). A member that stopped then knows, opened
    /// again, all it had acted on. Set once, by the group's owner, before
    /// it drains: what a member replays at opening its log holds already.
    pub fn apply_on_written_commit(&mut self) {
        dispatch!(inner = &mut self.backend => inner.apply_on_written_commit())
    }
    /// An owner of many groups is told when a write of this one is
    /// answered, instead of asking at intervals: `signal` makes, for each
    /// write, what the log's writer calls then, on its own thread — a wake
    /// for the owner, which drains the group after. The wake says a drain
    /// has something to take, nothing more; a write answered without it
    /// (none was set, the wake was lost) is found by the owner's next poll.
    pub fn notify_persisted(&mut self, signal: Option<PersistedSignal>) {
        dispatch!(inner = &mut self.backend => inner.notify_persisted(signal))
    }
    /// Whether the log will tell this group's owner when what the group
    /// waits for is answered: a signal is set (`notify_persisted`), and
    /// every write the group waits for — a `Ready`'s, a commit's, a
    /// checkpoint's, a decoder floor's — was taken by the log, which calls
    /// the signal as it answers. An owner so told need not ask at
    /// intervals: its own period is the bound on a signal that was lost. A
    /// write the log had no room for tells no one, and its owner asks
    /// again.
    pub fn wakes_owner(&self) -> bool {
        dispatch!(inner = &self.backend => inner.wakes_owner())
    }
    /// Waits for the write this group has in flight, when it has one: an
    /// owner on its own thread, with nothing else to do for the group,
    /// waits here and drains after. False when there is no write to wait
    /// for — none is out, or the log had no room to take it and is asked
    /// again by the next drain.
    pub fn wait_persisted(&mut self) -> Result<bool, ConsensusError> {
        dispatch!(inner = &mut self.backend => inner.wait_persisted())
    }
}
