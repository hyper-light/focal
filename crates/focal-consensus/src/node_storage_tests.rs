//! The node's storage handle over both backends: a member opened through it names the handle's
//! writer, two handles on one log are two writers, and the handle restores as it opens.
use super::*;
use crate::LogMetrics;
use focal_log::WalOptions;
use focal_memory::DiskBudgetConfig;
use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_log::{Config as LogConfig, Log, Waits};

const CLUSTER: [u8; 16] = [9; 16];

fn identity() -> WalIdentity {
    WalIdentity {
        cluster: CLUSTER,
        node: 1,
        stream: 0,
    }
}

fn budget() -> MemoryBudget {
    MemoryBudget::new(256 * 1024 * 1024, 64 * 1024 * 1024).unwrap()
}

fn disk() -> DiskBudget {
    DiskBudget::new(DiskBudgetConfig::unbounded()).unwrap()
}

fn group(id: u8) -> NodeConfig {
    NodeConfig::single(1, CLUSTER, [id; 16])
}

fn no_needs(_: &[u8]) -> Option<[u8; 32]> {
    None
}

fn log_in(dir: &Path) -> ShellLog {
    let file = DeviceFile::open(
        &dir.join("raft.log"),
        true,
        CachingRequest::PreferDirect,
        Alignment::new(4096).unwrap(),
    )
    .unwrap();
    let config = LogConfig {
        segment_bytes: 4096 * 4096,
        max_segments: 8,
        max_groups: 8,
        group_entries: 1 << 12,
        group_bytes: 64 << 20,
        group_cache: 1 << 16,
        queue_submissions: 64,
        waits: Waits::Never,
    };
    Log::create(file, config, 7).unwrap()
}

#[test]
fn a_member_names_the_writer_of_the_handle_it_was_opened_through() {
    let dir = tempfile::tempdir().unwrap();
    let wal = SharedWal::open(dir.path().join("wal"), WalOptions::new(identity())).unwrap();
    let on_wal = NodeStorage::Wal(wal);
    let member = on_wal.open_member(group(1), &budget(), no_needs).unwrap();
    assert_eq!(member.storage_writer().unwrap(), on_wal.writer());
    assert_eq!(on_wal.identity().unwrap(), identity());
    drop(member);

    let log = log_in(dir.path());
    let shell = ShellStorage::new(dir.path(), &log, disk(), identity(), budget()).unwrap();
    let on_shell = NodeStorage::Shell(shell.clone());
    let member = on_shell.open_member(group(2), &budget(), no_needs).unwrap();
    assert_eq!(member.storage_writer().unwrap(), on_shell.writer());
    assert_eq!(on_shell.identity().unwrap(), identity());
    assert_ne!(on_shell.writer(), on_wal.writer());
    // A second handle on the same log is another writer: an owner given one admits no member of
    // the other, as one given a WAL admits none of a WAL opened again.
    let again = ShellStorage::new(dir.path(), &log, disk(), identity(), budget()).unwrap();
    assert_ne!(NodeStorage::Shell(again).writer(), on_shell.writer());
    // A clone is the same handle, and the same writer.
    assert_eq!(NodeStorage::Shell(shell).writer(), on_shell.writer());
    drop(member);
    drop(log.close().unwrap());
}

#[test]
fn the_handle_measures_its_volume_and_charges_within_its_budget() {
    let dir = tempfile::tempdir().unwrap();
    let log = log_in(dir.path());
    let parent = budget();
    let child = parent.child(1 << 20, 1 << 20).unwrap();
    let storage =
        NodeStorage::Shell(ShellStorage::new(dir.path(), &log, disk(), identity(), child).unwrap());
    assert!(storage.is_budgeted_within(&parent));
    assert!(!storage.is_budgeted_within(&budget()));
    // The volume is sampled, and with nothing promised the handle gives what the volume has
    // free: on a volume the test can write to, something.
    if focal_platform::available_space(dir.path()).is_some() {
        assert!(storage.available_bytes().unwrap() > 0);
    }
    drop(storage);
    drop(log.close().unwrap());
}

#[test]
fn the_handle_restores_a_group_on_either_backend() {
    let image = || RestoredLog {
        index: 9,
        term: 2,
        data: b"state".to_vec(),
        floor: [3; 32],
        transition: None,
    };
    let dir = tempfile::tempdir().unwrap();
    let wal = NodeStorage::Wal(
        SharedWal::open(dir.path().join("wal"), WalOptions::new(identity())).unwrap(),
    );
    let restored = wal
        .restore_member(group(1), &budget(), no_needs, image())
        .unwrap();
    assert_eq!(restored.snapshot_index(), 9);
    drop(restored);
    let log = log_in(dir.path());
    let shell = NodeStorage::Shell(
        ShellStorage::new(dir.path(), &log, disk(), identity(), budget()).unwrap(),
    );
    let restored = shell
        .restore_member(group(2), &budget(), no_needs, image())
        .unwrap();
    assert_eq!(restored.snapshot_index(), 9);
    assert_eq!(restored.storage_writer().unwrap(), shell.writer());
    drop(restored);
    drop(log.close().unwrap());
}

/// The node log's counts as focal reports them (doc 27 §15.10): what the log measured, its
/// quantiles in order, and no flush in progress once every write was answered.
#[test]
fn the_node_log_reports_what_it_measured() {
    let dir = tempfile::tempdir().unwrap();
    let log = log_in(dir.path());
    let quiet = LogMetrics::of(&log.stats(None).unwrap());
    assert_eq!(quiet.flush.p50_ns, None, "nothing timed reads as nothing");
    for index in 1..=8u64 {
        log.write(
            1,
            hyper_log::Update {
                entries: Some(hyper_log::Entries {
                    first: index,
                    entries: vec![hyper_log::Entry {
                        term: 1,
                        bytes: vec![1; 512],
                    }],
                }),
                ..hyper_log::Update::default()
            },
        )
        .unwrap();
    }
    let stats = log.stats(None).unwrap();
    let metrics = LogMetrics::of(&stats);
    assert_eq!(metrics.frames, stats.frames);
    assert_eq!(metrics.updates, 8);
    assert_eq!(metrics.flushes, stats.flushes);
    assert_eq!(metrics.flush.count, stats.flush.count());
    assert_eq!(metrics.commit_wait.count, 8);
    let (p50, p99, p999) = (
        metrics.commit_wait.p50_ns.unwrap(),
        metrics.commit_wait.p99_ns.unwrap(),
        metrics.commit_wait.p999_ns.unwrap(),
    );
    assert!(p50 <= p99 && p99 <= p999);
    assert_eq!(metrics.flushing_ns, None);
    drop(log.close().unwrap());
}
