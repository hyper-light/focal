//! A node's storage at its start: focal-log's WAL below the storage level, the shell's log at it
//! (created, then reopened with the same configuration), a WAL converted first, and the log's
//! plan the same at every start.
use super::*;
use crate::StorageBackend;
use crate::convert::Start;
use focal_memory::DiskBudgetConfig;

fn identity() -> focal_log::WalIdentity {
    focal_log::WalIdentity {
        cluster: [5; 16],
        node: 3,
        stream: 0,
    }
}

fn facts() -> StorageFacts {
    StorageFacts {
        max_groups: 64,
        cadence_entries: 1024,
    }
}

fn envelope() -> MemoryBudget {
    MemoryBudget::new(1 << 30, 256 << 20)
        .unwrap()
        .child(256 << 20, 64 << 20)
        .unwrap()
}

fn disk() -> DiskBudget {
    DiskBudget::new(DiskBudgetConfig::unbounded()).unwrap()
}

/// A data directory as a node leaves it before its storage opens: its identity file in place,
/// its root key file beside it, off the data directory (29 §2), both removed with the test.
struct Dir {
    _outer: tempfile::TempDir,
    data: std::path::PathBuf,
}
impl Dir {
    fn path(&self) -> &Path {
        &self.data
    }
}
fn data_dir() -> Dir {
    let outer = tempfile::tempdir().unwrap();
    let data = outer.path().join("data");
    std::fs::create_dir(&data).unwrap();
    std::fs::write(data.join(IDENTITY_FILE), b"identity").unwrap();
    Dir {
        _outer: outer,
        data,
    }
}
/// The root key file of the data directory `root` made by `data_dir`.
fn key_of(root: &Path) -> std::path::PathBuf {
    root.parent().unwrap().join("keys").join("node.key")
}

fn opened(root: &Path, fence_open: bool) -> OpenedStorage {
    open_node_storage(
        root,
        identity(),
        facts(),
        envelope(),
        disk(),
        fence_open,
        &key_of(root),
    )
    .unwrap()
}

/// Below the storage level a node runs on focal-log's WAL, as before, and keeps no log.
#[test]
fn below_the_storage_level_the_node_runs_on_its_wal() {
    let dir = data_dir();
    let opened = opened(dir.path(), false);
    assert!(matches!(opened.storage.backend(), StorageBackend::Wal(_)));
    assert!(opened.log.is_none() && opened.cache.is_none());
    assert!(!dir.path().join(convert::RAFT_DIR).exists());
}

/// At the storage level a directory founded there gets the shell's log, its cache charged to the
/// envelope while it lives; reopened, the same log opens with the configuration it was written
/// with, and the node is not converted (it has no WAL to convert).
#[test]
fn at_the_storage_level_a_founded_directory_opens_and_reopens_its_log() {
    let dir = data_dir();
    assert_eq!(
        convert::start(dir.path(), identity(), true).unwrap(),
        Start::Shell
    );
    let envelope = envelope();
    let opened = open_node_storage(
        dir.path(),
        identity(),
        facts(),
        envelope.clone(),
        disk(),
        true,
        &key_of(dir.path()),
    )
    .unwrap();
    assert!(matches!(opened.storage.backend(), StorageBackend::Shell(_)));
    let log = opened.log.unwrap();
    assert!(envelope.stats().used > 0, "the cache is charged");
    let config = log.config();
    drop(log.close().unwrap());
    drop(opened.cache);
    assert_eq!(envelope.stats().used, 0, "and given back with the log");
    let again = opened_in(dir.path());
    let log = again.log.unwrap();
    assert_eq!(log.config(), config);
    assert!(!dir.path().join(convert::CONVERTED_DIR).exists());
    drop(log.close().unwrap());
}

fn opened_in(root: &Path) -> OpenedStorage {
    opened(root, true)
}

/// At the storage level a directory that holds the WAL is converted first, then opens on the
/// shell: the WAL's fence names the log, and the start then reads it as the shell's.
#[test]
fn at_the_storage_level_a_wal_is_converted_first() {
    let dir = data_dir();
    drop(opened(dir.path(), false));
    assert_eq!(
        convert::start(dir.path(), identity(), true).unwrap(),
        Start::Convert
    );
    let opened = opened_in(dir.path());
    assert!(matches!(opened.storage.backend(), StorageBackend::Shell(_)));
    drop(opened.log.unwrap().close().unwrap());
    assert_eq!(
        convert::start(dir.path(), identity(), true).unwrap(),
        Start::Shell
    );
}

/// The log's plan is the same at every start: what it is made from does not change between them.
#[test]
fn the_log_plan_is_the_same_at_every_start() {
    let dir = data_dir();
    let a = log_plan(dir.path(), facts(), &envelope(), &key_of(dir.path())).unwrap();
    let b = log_plan(dir.path(), facts(), &envelope(), &key_of(dir.path())).unwrap();
    assert_eq!(a.config, b.config);
    assert_eq!(a.align.get(), b.align.get());
}

/// A founding a crash cut short leaves its mark beside a log that never served: empty, as a kill
/// between the file's creation and its first slot leaves it, or half written. The next start
/// removes it and founds the log again, and the mark is gone once the log is durable.
#[test]
fn a_founding_cut_short_is_founded_again() {
    for leftover in [&b""[..], &[0x5a; 6000][..]] {
        let dir = data_dir();
        let raft = dir.path().join(convert::RAFT_DIR);
        std::fs::create_dir_all(&raft).unwrap();
        std::fs::write(dir.path().join(FOUNDING_FILE), b"").unwrap();
        std::fs::write(dir.path().join(convert::LOG_FILE), leftover).unwrap();
        let opened = opened_in(dir.path());
        assert!(matches!(opened.storage.backend(), StorageBackend::Shell(_)));
        assert!(!dir.path().join(FOUNDING_FILE).exists(), "the mark goes");
        drop(opened.log.unwrap().close().unwrap());
        let again = opened_in(dir.path());
        drop(again.log.unwrap().close().unwrap());
    }
}

/// Without the mark, a file where the log should be is the node's log, whatever it holds: an
/// empty one is refused, typed, never removed, so a log a node ran on is never founded over.
#[test]
fn a_log_without_its_founding_mark_is_never_founded_over() {
    let dir = data_dir();
    let raft = dir.path().join(convert::RAFT_DIR);
    std::fs::create_dir_all(&raft).unwrap();
    std::fs::write(dir.path().join(convert::LOG_FILE), b"").unwrap();
    let refused = open_node_storage(
        dir.path(),
        identity(),
        facts(),
        envelope(),
        disk(),
        true,
        &key_of(dir.path()),
    )
    .unwrap_err();
    assert!(
        matches!(refused, OpenError::Log(hyper_log::LogError::Foreign(_))),
        "{refused}"
    );
    assert!(dir.path().join(convert::LOG_FILE).exists());
}
