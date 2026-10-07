//! A node's storage at its start: focal-log's WAL below the storage level, the shell's log at it
//! (created, then reopened with the same configuration), a WAL converted first, and the log's
//! plan the same at every start.
use super::*;
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

/// A data directory as a node leaves it before its storage opens: its identity file in place.
fn data_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join(IDENTITY_FILE), b"identity").unwrap();
    dir
}

fn opened(root: &Path, fence_open: bool) -> OpenedStorage {
    open_node_storage(root, identity(), facts(), envelope(), disk(), fence_open).unwrap()
}

/// Below the storage level a node runs on focal-log's WAL, as before, and keeps no log.
#[test]
fn below_the_storage_level_the_node_runs_on_its_wal() {
    let dir = data_dir();
    let opened = opened(dir.path(), false);
    assert!(matches!(opened.storage, NodeStorage::Wal(_)));
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
    )
    .unwrap();
    assert!(matches!(opened.storage, NodeStorage::Shell(_)));
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
    assert!(matches!(opened.storage, NodeStorage::Shell(_)));
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
    let a = log_plan(dir.path(), facts(), &envelope()).unwrap();
    let b = log_plan(dir.path(), facts(), &envelope()).unwrap();
    assert_eq!(a.config, b.config);
    assert_eq!(a.align.get(), b.align.get());
}
