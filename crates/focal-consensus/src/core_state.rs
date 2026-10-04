//! What both backends read of the core and check before they give it an input ([27] §15.7): the
//! core is the same over focal-log and over the shell, so what it reports, and what an owner may
//! ask of it, are the same whichever backend holds it.
//!
//! [27]: ../../../docs/archictecutre/27-consensus-roadmap-and-slates-port.md
use super::*;

/// The core's settings for a member under `config` that applied through `applied`: one set, so
/// that a member opened over either backend elects, replicates and admits as the other does.
pub(crate) fn raft_config(config: &NodeConfig, applied: u64) -> Result<Config, ConsensusError> {
    let page = (config.max_entry_bytes as u64).saturating_add(1024);
    let raft_config = Config {
        election_tick: config.election_tick,
        heartbeat_tick: config.heartbeat_tick,
        applied,
        max_size_per_msg: page,
        max_inflight_msgs: config.max_inflight_messages,
        // Until its owner says what the path to a member carries
        // (`set_inflight_bytes`), a member is sent one page ahead of its
        // answers: the least that always makes progress.
        max_inflight_bytes: page,
        max_uncommitted_size: config.max_uncommitted_bytes,
        max_committed_size_per_ready: COMMITTED_PAGE_BYTES,
        check_quorum: true,
        pre_vote: true,
        fast: config.fast,
        // A member that kept an append ahead of a hole says so in its refusal (`Message::kept`,
        // R17), which raft-rs's encoding, the one focal's peers speak until the upgrade fence opens
        // a successor (18 §1), cannot state. So a member refuses such an append and keeps nothing,
        // raft-rs's rule, and the envelope refuses a `kept` it is ever given.
        ahead: hyper_raft::Ahead::Refused,
        seed: election_seed(config),
        limits: Limits {
            // Reads are admitted against the window (`check_read`); the
            // core's own bound is the same and never the first met.
            pending_reads: config.max_inflight_messages.saturating_add(1),
            ..limits(config, page)?
        },
        ..Config::new(config.node_id, limits(config, page)?)
    };
    raft_config.validate()?;
    Ok(raft_config)
}

/// What a member under `config` states to the core (`hyper_raft::Limits::derive`), each input from
/// focal's own bounds, so that no bound of the core is chosen apart from them:
/// - its largest message is an append. The core takes entries while their encoding fits a page
///   and always at least one, and an entry is at most `max_entry_bytes` with its fixed bytes, inside
///   the record's fixed bytes;
/// - the members a configuration names are [`MAX_MEMBERS`], focal's bound;
/// - each queue holds what one ready of these settings holds, counted resident: a leader's
///   uncommitted bytes or the pages in flight to a member, and one entry past either, as the most
///   entries they carry, each at least its fixed bytes, at an entry's bytes in memory each. So a
///   member always holds a leader's message whole;
/// - one write is out, which the shell raises to its store's depth.
fn limits(config: &NodeConfig, page: u64) -> Result<Limits, ConsensusError> {
    use hyper_raft::wire::{ENTRY_FIXED_BYTES, MESSAGE_RECORD_FIXED_BYTES};
    let unfit =
        || ConsensusError::Configuration("capacity limits past what this machine addresses");
    let entry = config
        .max_entry_bytes
        .checked_add(ENTRY_FIXED_BYTES)
        .ok_or_else(unfit)?;
    let page = usize::try_from(page).map_err(|_| unfit())?;
    let message = page
        .max(entry)
        .checked_add(MESSAGE_RECORD_FIXED_BYTES)
        .ok_or_else(unfit)?;
    let uncommitted = usize::try_from(config.max_uncommitted_bytes).map_err(|_| unfit())?;
    let memory = config
        .max_inflight_messages
        .checked_mul(page)
        .map(|inflight| inflight.max(uncommitted))
        .and_then(|bytes| bytes.checked_add(config.max_entry_bytes))
        .and_then(|bytes| bytes.checked_div(ENTRY_FIXED_BYTES))
        .and_then(|entries| entries.checked_mul(std::mem::size_of::<proto::Entry>()))
        .ok_or_else(unfit)?;
    Ok(Limits::derive(hyper_raft::Stated {
        message,
        members: MAX_MEMBERS,
        memory,
        depth: 1,
    })?)
}

/// Refused while the core's term or its log's last index has nowhere to go.
pub(crate) fn check_core<S: Storage>(raw: &RawNode<S>) -> Result<(), ConsensusError> {
    if raw.raft.term() == u64::MAX || raw.store().last_index()? == u64::MAX {
        Err(ConsensusError::Capacity)
    } else {
        Ok(())
    }
}

/// Refused unless the member leads, naming the leader it knows.
pub(crate) fn check_leader<S: Storage>(raw: &RawNode<S>) -> Result<(), ConsensusError> {
    if raw.raft.state() == StateRole::Leader {
        Ok(())
    } else {
        Err(ConsensusError::NotLeader {
            leader: raw.raft.leader_id(),
        })
    }
}

/// An entry an owner proposes: something, and no more than one entry holds.
pub(crate) fn check_entry(config: &NodeConfig, len: usize) -> Result<(), ConsensusError> {
    if len == 0 || len > config.max_entry_bytes {
        Err(ConsensusError::Capacity)
    } else {
        Ok(())
    }
}

/// Only an applied voter campaigns.
pub(crate) fn check_campaign<S: Storage>(raw: &RawNode<S>) -> Result<(), ConsensusError> {
    if raw.raft.promotable() {
        Ok(())
    } else {
        Err(ConsensusError::Configuration(
            "only an applied voter can campaign",
        ))
    }
}

/// A peer's message, checked before the core is given it. The authenticated transport envelope
/// must bind cluster and group identity; this binds the rest.
pub(crate) fn check_message(config: &NodeConfig, message: &Message) -> Result<(), ConsensusError> {
    if message.to != config.node_id || message.from == 0 {
        return Err(ConsensusError::Configuration(
            "wrong destination or missing sender",
        ));
    }
    // Its length in the encoding a peer's message comes in; a change it
    // carries that does not read has none, and is malformed.
    let encoded = envelope::message_len(message)
        .map_err(|error| ConsensusError::MalformedMessage(error.reason()))?;
    if encoded > 9 * 1024 * 1024
        || message
            .entries
            .iter()
            .any(|entry| entry.data.len() > config.max_entry_bytes)
        || message
            .snapshot
            .as_deref()
            .is_some_and(|snapshot| snapshot.data.len() > IMAGE_BYTES)
    {
        return Err(ConsensusError::Capacity);
    }
    let kind = message.msg_type;
    if kind == MessageType::MsgFastPropose || kind == MessageType::MsgFastVote {
        return check_fast(config, message);
    }
    if kind == MessageType::MsgPropose {
        return Err(ConsensusError::MalformedMessage(
            "proposals must enter through the leader's application admission",
        ));
    }
    if [
        message.term,
        message.index,
        message.commit,
        message.log_term,
        message.commit_term,
        message.request_snapshot,
        message.reject_hint,
    ]
    .contains(&u64::MAX)
    {
        return Err(ConsensusError::Capacity);
    }
    if let Some(snapshot) = message.snapshot.as_deref()
        && !proto::snapshot_is_empty(snapshot)
    {
        let metadata = metadata_of(snapshot);
        if metadata.index == u64::MAX || metadata.term == u64::MAX {
            return Err(ConsensusError::Capacity);
        }
        validate_conf_state(conf_of(metadata))?;
    }
    let mut expected_index = message
        .index
        .checked_add(1)
        .ok_or(ConsensusError::Capacity)?;
    for entry in &message.entries {
        if kind == MessageType::MsgAppend {
            if entry.index != expected_index || entry.term > message.term {
                return Err(ConsensusError::MalformedMessage(
                    "invalid appended log sequence",
                ));
            }
            expected_index = expected_index
                .checked_add(1)
                .ok_or(ConsensusError::Capacity)?;
        }
        if entry.index == u64::MAX || entry.term == u64::MAX {
            return Err(ConsensusError::Capacity);
        }
        // A change is read here as it is where it is applied: one that
        // does not decode, or names a kind or a transition this member
        // does not know, is no entry a peer may send.
        proto::Plan::of_entry(entry)
            .map_err(|_| ConsensusError::MalformedMessage("an entry that cannot be read"))?;
    }
    Ok(())
}

/// A proposal by the fast track, or what a voter holds of one.
fn check_fast(config: &NodeConfig, message: &Message) -> Result<(), ConsensusError> {
    if !config.fast {
        return Err(ConsensusError::MalformedMessage(
            "the fast track in a group that has none",
        ));
    }
    if message.term == u64::MAX || message.commit == u64::MAX {
        return Err(ConsensusError::Capacity);
    }
    if message.entries.is_empty() || message.entries.len() > 256 {
        return Err(ConsensusError::MalformedMessage(
            "a proposal that states nothing, or too much",
        ));
    }
    for entry in &message.entries {
        if entry.entry_type != EntryType::EntryNormal || entry.data.is_empty() || entry.index == 0 {
            return Err(ConsensusError::MalformedMessage(
                "what may not go by the fast track",
            ));
        }
        if entry.data.len() > config.max_entry_bytes
            || entry.index == u64::MAX
            || entry.term == u64::MAX
        {
            return Err(ConsensusError::Capacity);
        }
    }
    Ok(())
}

/// A read asked here, checked against the window: `held` counts the reads a quorum confirmed
/// that wait to be given out.
///
/// A follower asks through its leader: the core forwards the read and the answer names the
/// leader's commit index (27 §5, follower reads). One that knows no leader has no one to ask.
pub(crate) fn check_read<S: Storage>(
    config: &NodeConfig,
    raw: &RawNode<S>,
    context: &[u8],
    held: usize,
) -> Result<(), ConsensusError> {
    if raw.raft.state() != StateRole::Leader && raw.raft.leader_id() == 0 {
        return Err(ConsensusError::NotLeader { leader: 0 });
    }
    if context.is_empty() || context.len() > 1024 {
        return Err(ConsensusError::Capacity);
    }
    if raw.raft.pending_read_count().saturating_add(held) >= config.max_inflight_messages {
        return Err(ConsensusError::Capacity);
    }
    Ok(())
}

/// A change of membership this leader may propose, under the configuration `conf` it applied.
pub(crate) fn check_conf_change<S: Storage>(
    config: &NodeConfig,
    raw: &RawNode<S>,
    conf: &ConfState,
    change: &ConfChangeV2,
) -> Result<(), ConsensusError> {
    check_leader(raw)?;
    // The entry holds the change as the core writes it, and the log and
    // the wire as raft-rs does: each must fit.
    if change
        .encoded_len()
        .max(envelope::conf_change_v2_len(change)?)
        > config.max_entry_bytes
    {
        return Err(ConsensusError::Capacity);
    }
    // A change committed and not yet applied by this member is a
    // moment, not a fault in the request: the one that follows it is
    // asked again once the configuration it builds on is applied (an
    // administrator's promotion right after its admission's receipt).
    if raw.raft.has_pending_conf() {
        return Err(ConsensusError::MembershipPending);
    }
    // One this member could not read where it is applied is not proposed.
    proto::Plan::of(change)
        .map_err(|_| ConsensusError::Configuration("unknown kind of membership change"))?;
    let joint = !conf.voters_outgoing.is_empty();
    if joint != change.changes.is_empty() {
        return Err(ConsensusError::Configuration(
            "joint membership must be entered and left in separate committed changes",
        ));
    }
    if change.changes.len() > 1024 {
        return Err(ConsensusError::Capacity);
    }
    for update in &change.changes {
        if update.node_id == 0 {
            return Err(ConsensusError::Configuration("zero member ID"));
        }
        // Leadership moves first, by the decision of who removes the
        // node; the next leader removes it (27 §5). A leader that
        // applies its own removal all the same, proposed by one that
        // led before it, hands the group to a voter that holds the
        // whole log and follows.
        let leaves = update.change_type == ConfChangeType::RemoveNode
            || update.change_type == ConfChangeType::AddLearnerNode;
        if leaves && update.node_id == raw.raft.id() {
            return Err(ConsensusError::LeaderLeaving);
        }
        if update.change_type == ConfChangeType::AddNode {
            let progress = raw
                .raft
                .tracker()
                .get(update.node_id)
                .ok_or(ConsensusError::LearnerBehind)?;
            if progress.matched < raw.raft.log().committed() {
                return Err(ConsensusError::LearnerBehind);
            }
        }
    }
    Ok(())
}

/// A hand-over of leadership to `node` this member may ask for, under the configuration `conf`
/// it applied. On the leader, to another current voter. On a follower, only leadership for this
/// node itself: the core forwards the request to the leader it knows, which times the
/// transferee out into a campaign; any other target is not a follower's to request.
pub(crate) fn check_transfer<S: Storage>(
    config: &NodeConfig,
    raw: &RawNode<S>,
    conf: &ConfState,
    node: u64,
) -> Result<(), ConsensusError> {
    let voter = conf.voters.contains(&node);
    if raw.raft.state() == StateRole::Leader {
        if !voter || node == config.node_id {
            return Err(ConsensusError::Configuration(
                "transfer target must be another current voter",
            ));
        }
        return Ok(());
    }
    if node != config.node_id || !voter || raw.raft.leader_id() == 0 {
        return Err(ConsensusError::NotLeader {
            leader: raw.raft.leader_id(),
        });
    }
    Ok(())
}

/// Whether delayed transport feedback is about the snapshot still pending for `node` in this
/// leadership term. Acceptance never proves installation.
pub(crate) fn snapshot_still_pending<S: Storage>(
    raw: &RawNode<S>,
    node: u64,
    term: u64,
    index: u64,
) -> bool {
    index != 0
        && raw.raft.term() == term
        && raw
            .raft
            .tracker()
            .get(node)
            .is_some_and(|progress| progress.pending_snapshot == index)
}

/// Whether the commit is of the member's current term: a leader completes a quorum read only once
/// it has committed the entry it began its term with; any other member, once the entry committed
/// is of its term.
pub(crate) fn committed_in_term<S: Storage>(raw: &RawNode<S>) -> bool {
    if raw.raft.state() == StateRole::Leader {
        // A leader of a fast group takes what its voters approved into its log under its own term,
        // below an index a fast quorum may have committed before its term began. An entry of its
        // term committed is then no proof that the commit covers what came before the term, so it
        // waits for the entry it began its term with (Ongaro's thesis §6.4; hyper-raft S-4,
        // `Raft::commit_to_current_term`).
        return raw.raft.commit_to_current_term();
    }
    raw.store()
        .term(raw.raft.log().committed())
        .is_ok_and(|term| term == raw.raft.term())
}

/// Whether the configuration a snapshot states, `stated`, names every member of `current`, the
/// one applied: Raft discards a snapshot that does not name its recipient.
pub(crate) fn names_every_member(stated: &ConfState, current: &ConfState) -> bool {
    let named = |node: &u64| {
        stated.voters.contains(node)
            || stated.learners.contains(node)
            || stated.voters_outgoing.contains(node)
            || stated.learners_next.contains(node)
    };
    current
        .voters
        .iter()
        .chain(&current.learners)
        .chain(&current.voters_outgoing)
        .chain(&current.learners_next)
        .all(named)
}

/// The status's scalars, copied: `applied` is what the owner was handed.
pub(crate) fn scalars<S: Storage>(raw: &RawNode<S>, node_id: u64, applied: u64) -> NodeScalars {
    NodeScalars {
        node_id,
        leader_id: raw.raft.leader_id(),
        term: raw.raft.term(),
        committed_index: raw.raft.log().committed(),
        applied_index: applied,
        role: raw.raft.state(),
    }
}

/// The status, with the membership `conf` the member applied.
pub(crate) fn status<S: Storage>(
    raw: &RawNode<S>,
    node_id: u64,
    applied: u64,
    conf: &ConfState,
) -> NodeStatus {
    let scalars = scalars(raw, node_id, applied);
    NodeStatus {
        node_id: scalars.node_id,
        leader_id: scalars.leader_id,
        term: scalars.term,
        committed_index: scalars.committed_index,
        applied_index: scalars.applied_index,
        role: scalars.role,
        voters: conf.voters.clone(),
        learners: conf.learners.clone(),
    }
}

/// One member's replication as a leader tracks it.
fn progress_of(node: u64, progress: &hyper_raft::progress::Progress) -> PeerProgress {
    PeerProgress {
        node,
        matched: progress.matched,
        next_index: progress.next_index,
        state: match progress.state {
            ProgressState::Probe => PEER_PROBE,
            ProgressState::Replicate => PEER_REPLICATE,
            ProgressState::Snapshot => PEER_SNAPSHOT,
        },
        recent_active: progress.recent_active,
        paused: progress.paused,
        pending_snapshot: progress.pending_snapshot,
    }
}

/// What this member tracks of every other member's replication while it leads; nothing
/// otherwise.
pub(crate) fn peer_progress<S: Storage>(raw: &RawNode<S>, node_id: u64) -> Vec<PeerProgress> {
    if raw.raft.state() != StateRole::Leader {
        return Vec::new();
    }
    raw.raft
        .tracker()
        .iter()
        .filter(|(node, _)| *node != node_id)
        .map(|(node, progress)| progress_of(node, progress))
        .collect()
}

/// What this leader tracks of one member's replication; nothing when it does not lead or tracks
/// no such member. Allocates nothing.
pub(crate) fn peer<S: Storage>(raw: &RawNode<S>, node_id: u64, node: u64) -> Option<PeerProgress> {
    if raw.raft.state() != StateRole::Leader || node == node_id {
        return None;
    }
    raw.raft
        .tracker()
        .get(node)
        .map(|progress| progress_of(node, progress))
}
