//! This core and `raft-rs` on one schedule (27 §6, stage D): the same
//! members, the same messages delivered, lost, repeated and held back in
//! the same order, the same stops and compactions. After every step both
//! say what they persisted, sent, committed and answered, and what each
//! member knows of the others; all of it is equal, or the run fails naming
//! its seed and its step.
//!
//! The two differ by decision in one place a schedule reaches: a leader
//! that applies a change which leaves it no voter steps down here and
//! leads on there. A run that comes to it ends there, and what this core
//! does from there on is tested by itself (`group.rs`).
#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
mod support;

use std::collections::VecDeque;

use focal_raft::proto::MessageType;
use support::{Cluster, Mix, New, Old, Op, Replica, Report, Seeded, Settings};

fn count(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// What the schedules came to: a comparison proves what it reached.
#[derive(Debug, Default)]
struct Reached {
    terms: u64,
    committed: u64,
    changes: u64,
    refused_changes: u64,
    snapshots: u64,
    reads: u64,
    rejections: u64,
    transfers: u64,
    votes_refused: u64,
    restarts: u64,
    refusals: u64,
}
impl Reached {
    fn note(&mut self, op: &Op, reports: &[Report]) {
        if matches!(op, Op::Restart(_)) {
            self.restarts += 1;
        }
        for report in reports {
            let output = &report.output;
            self.committed += output.committed.len() as u64;
            self.changes += output.confs.len() as u64;
            self.refused_changes += output.refused.len() as u64;
            self.snapshots += output.snapshots.len() as u64;
            self.reads += output.reads.len() as u64;
            self.refusals += u64::from(report.accepted == Some(false));
            if output
                .committed
                .iter()
                .any(|said| said.3.is_empty() && said.2 == 0)
            {
                // A leader's first entry: a term that was led.
                self.terms += 1;
            }
            for message in &output.messages {
                match focal_raft::proto::message_type(message) {
                    Some(MessageType::MsgAppendResponse) if message.reject => self.rejections += 1,
                    Some(MessageType::MsgTimeoutNow) => self.transfers += 1,
                    Some(
                        MessageType::MsgRequestVoteResponse
                        | MessageType::MsgRequestPreVoteResponse,
                    ) if message.reject => self.votes_refused += 1,
                    _ => {}
                }
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum End {
    /// Every step of the schedule was taken and compared.
    Ran,
    /// The run came to the place the two differ by decision.
    LeaderLeft(u64),
}

fn brief(message: &focal_raft::proto::Message) -> String {
    let entries: Vec<String> = message
        .entries
        .iter()
        .map(|entry| {
            format!(
                "{}@{}/{}:{}b",
                entry.index,
                entry.term,
                entry.entry_type,
                entry.data.len()
            )
        })
        .collect();
    format!(
        "{:?} {}->{} term {} log {}@{} commit {}@{} reject {} hint {} request {} context {:?} priority {} snapshot {:?} [{}]",
        focal_raft::proto::message_type(message),
        message.from,
        message.to,
        message.term,
        message.index,
        message.log_term,
        message.commit,
        message.commit_term,
        message.reject,
        message.reject_hint,
        message.request_snapshot,
        String::from_utf8_lossy(&message.context),
        message.priority,
        message
            .snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.metadata.as_ref())
            .map(|metadata| (metadata.index, metadata.term)),
        entries.join(" "),
    )
}
/// Where two reports differ, and no more.
fn explain(old: &[Report], new: &[Report]) -> String {
    let mut lines = Vec::new();
    if old.len() != new.len() {
        lines.push(format!("reports: {} and {}", old.len(), new.len()));
    }
    for (old, new) in old.iter().zip(new) {
        let member = new.member;
        macro_rules! differ {
            ($what:literal, $old:expr, $new:expr) => {
                if $old != $new {
                    lines.push(format!(
                        "member {member} {}:\n  raft-rs:    {:?}\n  focal-raft: {:?}",
                        $what, $old, $new
                    ));
                }
            };
        }
        differ!("member", old.member, new.member);
        differ!("accepted", old.accepted, new.accepted);
        differ!(
            "hard states",
            old.output.hard_states,
            new.output.hard_states
        );
        differ!(
            "persisted",
            old.output
                .persisted
                .iter()
                .map(|said| (said.0, said.1, said.2, said.3.len()))
                .collect::<Vec<_>>(),
            new.output
                .persisted
                .iter()
                .map(|said| (said.0, said.1, said.2, said.3.len()))
                .collect::<Vec<_>>()
        );
        differ!("snapshots", old.output.snapshots, new.output.snapshots);
        differ!(
            "committed",
            old.output
                .committed
                .iter()
                .map(|said| (said.0, said.1, said.2, said.3.len()))
                .collect::<Vec<_>>(),
            new.output
                .committed
                .iter()
                .map(|said| (said.0, said.1, said.2, said.3.len()))
                .collect::<Vec<_>>()
        );
        differ!("reads", old.output.reads, new.output.reads);
        differ!("configurations", old.output.confs, new.output.confs);
        differ!("refused changes", old.output.refused, new.output.refused);
        if old.output.messages != new.output.messages {
            lines.push(format!(
                "member {member} messages:\n  raft-rs:\n{}\n  focal-raft:\n{}",
                old.output
                    .messages
                    .iter()
                    .map(|m| format!("    {}", brief(m)))
                    .collect::<Vec<_>>()
                    .join("\n"),
                new.output
                    .messages
                    .iter()
                    .map(|m| format!("    {}", brief(m)))
                    .collect::<Vec<_>>()
                    .join("\n"),
            ));
        }
        differ!("view", old.view, new.view);
    }
    lines.join("\n")
}

fn timeless(reports: &mut [Report]) {
    for report in reports {
        report.view.timeout = 0;
    }
}

fn run(seed: u64, steps: u64, settings: Settings, mix: Mix, reached: &mut Reached) -> End {
    let voters = [1, 2, 3];
    let mut old: Cluster<Old> = Cluster::new(5, &voters, settings, seed);
    let mut new: Cluster<New> = Cluster::new(5, &voters, settings, seed);
    let mut rng = Seeded(seed);
    let mut trace: VecDeque<String> = VecDeque::new();
    let agree = |old: &mut Cluster<Old>, new: &Cluster<New>| {
        for id in new.up() {
            let timeout = new.peek(id).unwrap().view().timeout;
            old.node(id).unwrap().set_timeout(timeout);
        }
    };
    agree(&mut old, &new);
    for id in new.ids() {
        assert_eq!(
            old.peek(id).unwrap().view(),
            new.peek(id).unwrap().view(),
            "seed {seed}: member {id} as opened"
        );
    }
    for step in 0..steps {
        let op = new.choose(&mut rng, &mix);
        trace.push_back(format!("{step}: {op:?}"));
        if trace.len() > 24 {
            trace.pop_front();
        }
        let mut said_old = old.act(&op);
        let mut said_new = new.act(&op);
        timeless(&mut said_old);
        timeless(&mut said_new);
        if said_new.iter().any(|report| report.output.leader_left)
            || said_old.iter().any(|report| report.output.leader_left)
        {
            return End::LeaderLeft(step);
        }
        if said_old != said_new {
            let steps: Vec<&String> = trace.iter().collect();
            panic!(
                "seed {seed}, step {step}: the cores differ\n\
                 the last steps: {steps:#?}\n{}",
                explain(&said_old, &said_new)
            );
        }
        assert_eq!(old.net, new.net, "seed {seed}, step {step}: the networks");
        reached.note(&op, &said_new);
        agree(&mut old, &new);
    }
    End::Ran
}

fn campaign(name: &str, settings: Settings, mix: Mix) -> Reached {
    let mut reached = Reached::default();
    let seeds = count("FOCAL_RAFT_SEEDS", 96);
    let steps = count("FOCAL_RAFT_STEPS", 4_000);
    let first = count("FOCAL_RAFT_SEED", 0);
    let mut ran = 0u64;
    let mut left = 0u64;
    let mut compared = 0u64;
    for seed in first..first + seeds {
        match run(seed, steps, settings, mix, &mut reached) {
            End::Ran => {
                ran += 1;
                compared += steps;
            }
            End::LeaderLeft(step) => {
                left += 1;
                compared += step;
            }
        }
    }
    println!(
        "{name}: {seeds} schedules, {compared} steps compared; {ran} ran to their end, \
         {left} ended where the cores differ by decision"
    );
    assert!(
        ran * 2 >= seeds,
        "{name}: most schedules end where the cores differ"
    );
    println!("{name}: {reached:?}");
    // A schedule that elects and commits nothing compares nothing.
    assert!(reached.terms >= seeds, "{name}: {reached:?}");
    assert!(reached.committed >= seeds * 8, "{name}: {reached:?}");
    assert!(
        reached.reads > 0 && reached.transfers > 0,
        "{name}: {reached:?}"
    );
    assert!(
        reached.votes_refused > 0 && reached.refusals > 0,
        "{name}: {reached:?}"
    );
    reached
}

#[test]
fn the_cores_agree_as_the_shell_sets_them() {
    let reached = campaign("shell", Settings::shell(), Mix::everything());
    assert!(
        reached.changes > 0 && reached.refused_changes > 0,
        "{reached:?}"
    );
    assert!(
        reached.snapshots > 0 && reached.rejections > 0,
        "{reached:?}"
    );
    assert!(reached.restarts > 0, "{reached:?}");
}

#[test]
fn the_cores_agree_without_pre_vote_and_check_quorum() {
    let settings = Settings {
        check_quorum: false,
        pre_vote: false,
        ..Settings::shell()
    };
    let reached = campaign("plain", settings, Mix::everything());
    assert!(reached.committed > 0);
}

#[test]
fn the_cores_agree_with_a_window_of_two_and_a_message_of_one_entry() {
    let settings = Settings {
        max_inflight_msgs: 2,
        max_size_per_msg: 1,
        max_committed_size_per_ready: 64,
        ..Settings::shell()
    };
    let reached = campaign("narrow", settings, Mix::everything());
    assert!(reached.committed > 0);
}

#[test]
fn the_cores_agree_on_what_a_leader_may_hold_uncommitted() {
    // A change that is refused for its size leaves `raft-rs` believing one
    // is pending; this core does not. No change is proposed here.
    let settings = Settings {
        max_uncommitted_size: 96,
        max_size_per_msg: 64,
        ..Settings::shell()
    };
    let mix = Mix {
        changes: false,
        ..Mix::everything()
    };
    let reached = campaign("uncommitted", settings, mix);
    assert!(reached.committed > 0);
}

#[test]
fn the_cores_agree_on_a_network_that_loses_nothing() {
    let mix = Mix {
        restarts: false,
        partitions: false,
        lose: 0,
        repeat: 0,
        ..Mix::everything()
    };
    let reached = campaign("whole", Settings::shell(), mix);
    assert!(reached.committed > 0);
}
