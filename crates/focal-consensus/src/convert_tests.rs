use super::*;
use focal_log::{WalIdentity, WalOptions, WalWriterLimits};
use focal_memory::DiskBudgetConfig;
use hyper_block::buf::Alignment;
use hyper_block::file::CachingRequest;
use hyper_log::{Config as LogConfig, Waits};

const HASH: [u8; 32] = [41; 32];
const LOG_ID: u128 = 0x0063_6f6e_7665_7274;

fn budget() -> MemoryBudget {
    MemoryBudget::new(256 * 1024 * 1024, 64 * 1024 * 1024).unwrap()
}

/// The shell's log as its tests size it: a frame holds an entry of a mebibyte, and it never
/// waits for more submitters.
fn log_config() -> LogConfig {
    LogConfig {
        segment_bytes: 1024 * 4096,
        max_segments: 16,
        max_groups: 4,
        group_entries: 1 << 12,
        group_bytes: 8 << 20,
        group_cache: 1 << 16,
        queue_submissions: 64,
        waits: Waits::Never,
    }
}

fn wal(path: &Path, budget: &MemoryBudget) -> SharedWal {
    SharedWal::open_with_budget(
        path,
        WalOptions::new(WalIdentity {
            node: 1,
            cluster: [1; 16],
            stream: 0,
        }),
        WalWriterLimits::default(),
        budget.clone(),
    )
    .unwrap()
}

fn config(group: u8) -> NodeConfig {
    let mut config = NodeConfig::single(1, [1; 16], [group; 16]);
    config.max_entry_bytes = 1 << 20;
    config
}

fn log_file(path: &Path) -> DeviceFile {
    let align = Alignment::new(4096).unwrap();
    DeviceFile::open(
        &path.join("raft.log"),
        true,
        CachingRequest::PreferDirect,
        align,
    )
    .unwrap()
}

fn no_needs(_: &[u8]) -> Option<[u8; 32]> {
    None
}

/// Two groups on one WAL: group 2 with a decoder floor, a checkpoint and a tail; group 3 with
/// entries alone. Returns the WAL's directory and group 2's tail.
fn recorded() -> (tempfile::TempDir, Vec<CommittedEntry>) {
    let old = tempfile::tempdir().unwrap();
    let budget = budget();
    let shared = wal(old.path(), &budget);
    let mut floored = DurableNode::open_on_wal_in(config(2), shared.clone(), &budget).unwrap();
    floored.confirm_decoder(HASH).unwrap();
    floored.drain().unwrap();
    floored.begin_decoder_floor(HASH).unwrap();
    floored.finish_decoder_floor().unwrap();
    floored.campaign().unwrap();
    floored.drain().unwrap();
    floored.propose(b"before".to_vec()).unwrap();
    let index = floored.drain().unwrap().applied_index;
    floored.checkpoint(index, b"image".to_vec()).unwrap();
    floored.propose(b"tail".to_vec()).unwrap();
    let tail = floored.drain().unwrap().committed;
    let mut plain = DurableNode::open_on_wal_in(config(3), shared, &budget).unwrap();
    plain.campaign().unwrap();
    plain.drain().unwrap();
    for data in [b"one".as_slice(), b"two"] {
        plain.propose(data.to_vec()).unwrap();
        plain.drain().unwrap();
    }
    (old, tail)
}

/// 27 §15.8 steps 3 and 5. Do: convert a WAL of two groups (a decoder floor, a checkpoint and a
/// tail in one), verify it, then open both groups on the shell. Expect: the records carry the
/// floor, the shell hands over the image and the tail, and the other group its entries.
#[test]
fn two_groups_with_a_floor_a_checkpoint_and_a_tail_move_whole() {
    let (old, tail) = recorded();
    let new = tempfile::tempdir().unwrap();
    let budget = budget();
    let shared = wal(old.path(), &budget);
    let log = Log::create(log_file(new.path()), log_config(), LOG_ID).unwrap();
    let copied = copy_groups(&shared, new.path(), &log, &budget).unwrap();
    assert_eq!(copied.groups, 2);
    assert_eq!(copied.image_bytes, 5);
    drop(log);
    let (log, _) = Log::open(log_file(new.path()), log_config(), LOG_ID).unwrap();
    verify(&shared, new.path(), &log, &budget).unwrap();
    drop(shared);
    let records =
        group_files::read_records(&FileMedium, &group_files::group_dir(new.path(), [2; 16]))
            .unwrap()
            .unwrap();
    assert_eq!(records.decoder_floor, Some(HASH));
    let disk = || DiskBudget::new(DiskBudgetConfig::unbounded()).unwrap();
    let mut floored =
        DurableNode::open_on_shell(config(2), new.path(), &log, &budget, disk(), no_needs).unwrap();
    assert_eq!(floored.required_decoder(), Some(HASH));
    floored.confirm_decoder(HASH).unwrap();
    let mut snapshot = None;
    let mut committed = Vec::new();
    for _ in 0..100 {
        let events = floored.drain().unwrap();
        snapshot = snapshot.or(events.snapshot);
        committed.extend(events.committed);
        if !floored.has_ready() {
            break;
        }
    }
    assert_eq!(snapshot.unwrap().data, b"image");
    assert_eq!(committed, tail);
    let mut plain =
        DurableNode::open_on_shell(config(3), new.path(), &log, &budget, disk(), no_needs).unwrap();
    let mut data = Vec::new();
    for _ in 0..100 {
        let events = plain.drain().unwrap();
        data.extend(events.committed.into_iter().map(|entry| entry.data));
        if !plain.has_ready() {
            break;
        }
    }
    assert!(
        data.ends_with(&[b"one".to_vec(), b"two".to_vec()]),
        "{data:?}"
    );
}

/// The verification compares, never trusts the copy. Do: convert, then change a group's records,
/// and separately leave a group in the new log that the WAL does not hold. Expect: each is refused.
#[test]
fn verification_refuses_a_copy_that_differs() {
    let (old, _) = recorded();
    let budget = budget();
    let shared = wal(old.path(), &budget);
    // Records that differ.
    let new = tempfile::tempdir().unwrap();
    let log = Log::create(log_file(new.path()), log_config(), LOG_ID).unwrap();
    copy_groups(&shared, new.path(), &log, &budget).unwrap();
    let dir = group_files::group_dir(new.path(), [3; 16]);
    let mut records = group_files::read_records(&FileMedium, &dir)
        .unwrap()
        .unwrap();
    records.decoder_floor = Some(HASH);
    group_files::write_records(&mut FileMedium, &dir, &records).unwrap();
    assert!(matches!(
        verify(&shared, new.path(), &log, &budget),
        Err(ConsensusError::Corruption(_))
    ));
    drop(log);
    // A group the WAL does not hold.
    let new = tempfile::tempdir().unwrap();
    let log = Log::create(log_file(new.path()), log_config(), LOG_ID).unwrap();
    copy_groups(&shared, new.path(), &log, &budget).unwrap();
    let mut stray = claim_empty(&log, [9; 16]).unwrap();
    stray
        .write_now(&Write {
            hard_state: Some(HardState {
                term: 1,
                vote: 1,
                commit: 0,
            }),
            ..Write::default()
        })
        .unwrap();
    assert!(matches!(
        verify(&shared, new.path(), &log, &budget),
        Err(ConsensusError::Corruption(_))
    ));
}

/// A group on the fast track is refused, never converted without its proposals.
#[test]
fn a_group_on_the_fast_track_is_refused() {
    let old = tempfile::tempdir().unwrap();
    let budget = budget();
    let shared = wal(old.path(), &budget);
    let mut fast = config(4);
    fast.fast = true;
    let node = DurableNode::open_on_wal_in(fast, shared.clone(), &budget).unwrap();
    drop(node);
    let new = tempfile::tempdir().unwrap();
    let log = Log::create(log_file(new.path()), log_config(), LOG_ID).unwrap();
    assert!(matches!(
        copy_groups(&shared, new.path(), &log, &budget),
        Err(ConsensusError::Configuration(_))
    ));
}
