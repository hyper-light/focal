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
    let copied = copy_groups(
        &shared,
        new.path(),
        &log,
        &budget,
        &group_files::test_seal(new.path()),
    )
    .unwrap();
    assert_eq!(copied.groups, 2);
    assert_eq!(copied.image_bytes, 5);
    drop(log);
    let (log, _) = Log::open(log_file(new.path()), log_config(), LOG_ID).unwrap();
    verify(
        &shared,
        new.path(),
        &log,
        &budget,
        &group_files::test_seal(new.path()),
    )
    .unwrap();
    drop(shared);
    let records = group_files::read_records(
        &FileMedium,
        &group_files::group_dir(new.path(), [2; 16]),
        &group_files::test_seal(new.path()),
    )
    .unwrap()
    .unwrap();
    assert_eq!(records.decoder_floor, Some(HASH));
    let disk = || DiskBudget::new(DiskBudgetConfig::unbounded()).unwrap();
    let mut floored = DurableNode::open_on_shell(
        config(2),
        &crate::node_storage::test_shell(new.path(), &log, disk()),
        &budget,
        no_needs,
    )
    .unwrap();
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
    let mut plain = DurableNode::open_on_shell(
        config(3),
        &crate::node_storage::test_shell(new.path(), &log, disk()),
        &budget,
        no_needs,
    )
    .unwrap();
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
    copy_groups(
        &shared,
        new.path(),
        &log,
        &budget,
        &group_files::test_seal(new.path()),
    )
    .unwrap();
    let dir = group_files::group_dir(new.path(), [3; 16]);
    let mut records =
        group_files::read_records(&FileMedium, &dir, &group_files::test_seal(new.path()))
            .unwrap()
            .unwrap();
    records.decoder_floor = Some(HASH);
    group_files::write_records(
        &mut FileMedium,
        &dir,
        &records,
        &group_files::test_seal(new.path()),
    )
    .unwrap();
    assert!(matches!(
        verify(
            &shared,
            new.path(),
            &log,
            &budget,
            &group_files::test_seal(new.path())
        ),
        Err(ConsensusError::Corruption(_))
    ));
    drop(log);
    // A group the WAL does not hold.
    let new = tempfile::tempdir().unwrap();
    let log = Log::create(log_file(new.path()), log_config(), LOG_ID).unwrap();
    copy_groups(
        &shared,
        new.path(),
        &log,
        &budget,
        &group_files::test_seal(new.path()),
    )
    .unwrap();
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
        verify(
            &shared,
            new.path(),
            &log,
            &budget,
            &group_files::test_seal(new.path())
        ),
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
        copy_groups(
            &shared,
            new.path(),
            &log,
            &budget,
            &group_files::test_seal(new.path())
        ),
        Err(ConsensusError::Configuration(_))
    ));
}

fn identity() -> WalIdentity {
    WalIdentity {
        node: 1,
        cluster: [1; 16],
        stream: 0,
    }
}

/// The plan of the data directory `root` made by `node_dir`: its root key file beside it, off the
/// data directory (29 §2), removed with the test.
fn plan(root: &Path) -> LogPlan {
    LogPlan {
        config: log_config(),
        align: Alignment::new(4096).unwrap(),
        key_file: root.parent().unwrap().join("keys").join("node.key"),
    }
}

/// A node's data directory, one level inside the temporary directory that holds its key too.
struct NodeDir {
    _outer: tempfile::TempDir,
    data: std::path::PathBuf,
}
impl NodeDir {
    fn path(&self) -> &Path {
        &self.data
    }
}

/// A node's data directory whose `wal/` holds `recorded()`'s two groups, closed.
fn node_dir() -> (NodeDir, Vec<CommittedEntry>) {
    let (old, tail) = recorded();
    let outer = tempfile::tempdir().unwrap();
    let data = outer.path().join("data");
    std::fs::create_dir(&data).unwrap();
    let root = NodeDir {
        _outer: outer,
        data,
    };
    std::fs::create_dir(root.path().join(WAL_DIR)).unwrap();
    for entry in std::fs::read_dir(old.path()).unwrap() {
        let entry = entry.unwrap();
        if entry.file_name() != "LOCK" {
            std::fs::copy(
                entry.path(),
                root.path().join(WAL_DIR).join(entry.file_name()),
            )
            .unwrap();
        }
    }
    (root, tail)
}

fn segments(dir: &Path) -> usize {
    std::fs::read_dir(dir)
        .map(|entries| {
            entries
                .filter(|e| {
                    e.as_ref()
                        .unwrap()
                        .file_name()
                        .to_string_lossy()
                        .ends_with(".seg")
                })
                .count()
        })
        .unwrap_or(0)
}

/// 27 §15.8, steps 1 to 7. Do: convert a node's data directory, open its groups on the shell, and
/// convert it again. Expect: the fence names the log, the segments are aside, no WAL writer opens,
/// the shell hands over what the WAL held, and the repeat copies nothing.
#[test]
fn a_data_directory_converts_whole_and_once() {
    let (root, tail) = node_dir();
    let held = segments(&root.path().join(WAL_DIR));
    let budget = budget();
    let Outcome::Converted { copied, moved } =
        convert_data_dir(root.path(), identity(), &plan(root.path()), &budget).unwrap()
    else {
        panic!("converted now");
    };
    assert_eq!((copied.groups, moved), (2, held));
    assert_eq!(
        focal_log::conversion::storage(&root.path().join(WAL_DIR)).unwrap(),
        focal_log::conversion::Storage::Converted {
            log: log_id(identity())
        }
    );
    assert_eq!(segments(&root.path().join(WAL_DIR)), 0);
    assert_eq!(segments(&root.path().join(CONVERTED_DIR)), held);
    assert!(matches!(
        SharedWal::open(root.path().join(WAL_DIR), WalOptions::new(identity())),
        Err(focal_log::LogError::Converted)
    ));
    let align = Alignment::new(4096).unwrap();
    let file = DeviceFile::open(
        &root.path().join(LOG_FILE),
        true,
        CachingRequest::PreferDirect,
        align,
    )
    .unwrap();
    // The converted log is sealed (29 §5): it opens only under the node's keys.
    let sealing = crate::convert::log_sealing(root.path(), &plan(root.path()).key_file).unwrap();
    let (log, _) = Log::open_sealed(file, log_config(), log_id(identity()), sealing).unwrap();
    let disk = DiskBudget::new(DiskBudgetConfig::unbounded()).unwrap();
    let mut floored = DurableNode::open_on_shell(
        config(2),
        &crate::ShellStorage::new(
            root.path(),
            &log,
            disk,
            identity(),
            budget.clone(),
            &plan(root.path()).key_file,
        )
        .unwrap(),
        &budget,
        no_needs,
    )
    .unwrap();
    floored.confirm_decoder(HASH).unwrap();
    let mut committed = Vec::new();
    for _ in 0..100 {
        committed.extend(floored.drain().unwrap().committed);
        if !floored.has_ready() {
            break;
        }
    }
    assert_eq!(committed, tail);
    drop(floored);
    drop(log);
    assert_eq!(
        convert_data_dir(root.path(), identity(), &plan(root.path()), &budget).unwrap(),
        Outcome::Finished { moved: 0 }
    );
}

/// After a crash. Do: leave an earlier attempt's partial store, convert; and separately commit by
/// hand and stop before the move, then convert. Expect: the partial store is replaced and the
/// conversion completes; past the commit point only the move is finished.
#[test]
fn a_conversion_resumes_where_a_crash_left_it() {
    let budget = budget();
    let (root, _) = node_dir();
    std::fs::create_dir_all(root.path().join(RAFT_DIR).join("groups")).unwrap();
    std::fs::write(root.path().join(LOG_FILE), b"a torn earlier attempt").unwrap();
    assert!(matches!(
        convert_data_dir(root.path(), identity(), &plan(root.path()), &budget).unwrap(),
        Outcome::Converted { .. }
    ));
    let (root, _) = node_dir();
    let held = segments(&root.path().join(WAL_DIR));
    focal_log::conversion::commit(&root.path().join(WAL_DIR), identity(), log_id(identity()))
        .unwrap();
    assert_eq!(
        convert_data_dir(root.path(), identity(), &plan(root.path()), &budget).unwrap(),
        Outcome::Finished { moved: held }
    );
}

/// 27 §15.8, "When it runs". Do: ask what a start opens, below and at the level, for a node's WAL,
/// an empty directory, a converted one, and the stores the fence does not name. Expect: the WAL below
/// the level; a conversion at it; the shell for a store founded there and for a converted one; and a
/// refusal for a `raft/` with no commit point, a commit point with no log, and another node's log.
#[test]
fn a_start_opens_what_the_fence_and_the_directory_state() {
    let (root, _) = node_dir();
    assert_eq!(start(root.path(), identity(), false).unwrap(), Start::Wal);
    assert_eq!(
        start(root.path(), identity(), true).unwrap(),
        Start::Convert
    );
    let empty = tempfile::tempdir().unwrap();
    assert_eq!(start(empty.path(), identity(), false).unwrap(), Start::Wal);
    assert_eq!(start(empty.path(), identity(), true).unwrap(), Start::Shell);
    // A store the fence does not name, below the level.
    std::fs::create_dir_all(root.path().join(RAFT_DIR)).unwrap();
    assert!(start(root.path(), identity(), false).is_err());
    std::fs::remove_dir_all(root.path().join(RAFT_DIR)).unwrap();
    // Converted: the shell, at the level or (never lowered) below it.
    let budget = budget();
    convert_data_dir(root.path(), identity(), &plan(root.path()), &budget).unwrap();
    assert_eq!(start(root.path(), identity(), true).unwrap(), Start::Shell);
    assert_eq!(start(root.path(), identity(), false).unwrap(), Start::Shell);
    // Another node's commit point, and a commit point whose log is gone.
    let other = WalIdentity {
        node: 2,
        ..identity()
    };
    assert!(start(root.path(), other, true).is_err());
    std::fs::remove_file(root.path().join(LOG_FILE)).unwrap();
    assert!(start(root.path(), identity(), true).is_err());
}
