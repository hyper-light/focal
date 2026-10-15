use super::*;

const LOG: u128 = 0x1234_5678_9abc_def0_1122_3344_5566_7788;

fn identity() -> WalIdentity {
    WalIdentity {
        cluster: [1; 16],
        node: 7,
        stream: 0,
    }
}

/// A WAL directory of version 2 holding one group's records, closed.
fn written() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = Wal::open(dir.path(), WalOptions::new(identity())).unwrap();
    wal.append(&[Record {
        log: LogicalLogId([2; 16]),
        kind: RecordKind::Entry,
        index: 1,
        term: 1,
        payload: b"entry".to_vec(),
    }])
    .unwrap();
    drop(wal);
    dir
}

fn segments(dir: &Path) -> usize {
    fs::read_dir(dir)
        .unwrap()
        .filter(|entry| is_segment(&entry.as_ref().unwrap().file_name()))
        .count()
}

/// The commit point's fence fits the bound every reader of a fence admits.
#[test]
fn a_converted_fence_fits_the_fence_bound() {
    assert!(CONVERTED_FILE_MAX as u64 <= MAX_FENCE_BYTES);
}

/// 27 §15.8, step 6. Do: commit a version 2 WAL to a log, commit again, then to another log. Expect:
/// the directory states the log; the repeat changes nothing; another log is refused.
#[test]
fn the_commit_point_names_the_log_once() {
    let dir = written();
    assert_eq!(storage(dir.path()).unwrap(), Storage::Wal);
    commit(dir.path(), identity(), LOG).unwrap();
    assert_eq!(
        storage(dir.path()).unwrap(),
        Storage::Converted { log: LOG }
    );
    let before = fs::read(dir.path().join("CURRENT")).unwrap();
    commit(dir.path(), identity(), LOG).unwrap();
    assert_eq!(fs::read(dir.path().join("CURRENT")).unwrap(), before);
    assert!(matches!(
        commit(dir.path(), identity(), LOG + 1),
        Err(LogError::Identity)
    ));
    let other = WalIdentity {
        node: 8,
        ..identity()
    };
    assert!(matches!(
        commit(dir.path(), other, LOG),
        Err(LogError::Identity)
    ));
}

/// Past the commit point no WAL writer opens: this binary says why, and the binary before it, which
/// reads a fence as version 2's fields and compares the version first, refuses it as a fence it does
/// not know (`LogError::Identity`) before any replica opens.
#[test]
fn a_converted_wal_opens_for_no_writer_old_or_new() {
    let dir = written();
    commit(dir.path(), identity(), LOG).unwrap();
    assert!(matches!(
        Wal::open(dir.path(), WalOptions::new(identity())),
        Err(LogError::Converted)
    ));
    // The version 2 binary's read: the payload decoded as its `Fence`, trailing bytes ignored.
    let old: Fence =
        postcard::from_bytes(&fence_payload(&dir.path().join("CURRENT")).unwrap()).unwrap();
    assert_eq!(old.version, FENCE_CONVERTED);
    assert_ne!(
        old.version, FENCE_VERSION,
        "the old open refuses it as Identity"
    );
    assert_eq!(old.identity, identity());
}

/// 27 §15.8, step 7. Do: move the segments before and after the commit, then remove them, and try to
/// remove a directory that holds something else. Expect: refused before the commit; after it, every
/// segment moves and the fence stays; removal takes only a directory of segments.
#[test]
fn segments_move_aside_only_past_the_commit_and_are_removed_only_as_segments() {
    let dir = written();
    let converted = dir.path().join("wal-converted");
    let held = segments(dir.path());
    assert!(held > 0);
    assert!(matches!(
        move_segments(dir.path(), &converted),
        Err(LogError::Identity)
    ));
    commit(dir.path(), identity(), LOG).unwrap();
    assert_eq!(move_segments(dir.path(), &converted).unwrap(), held);
    assert_eq!(segments(dir.path()), 0);
    assert_eq!(segments(&converted), held);
    assert!(dir.path().join("CURRENT").exists());
    assert_eq!(move_segments(dir.path(), &converted).unwrap(), 0);
    let wrong = dir.path().join("elsewhere");
    fs::create_dir(&wrong).unwrap();
    fs::write(wrong.join("keep"), b"not a segment").unwrap();
    assert!(remove_segments(dir.path(), &wrong).is_err());
    assert!(wrong.join("keep").exists());
    assert_eq!(remove_segments(dir.path(), &converted).unwrap(), held);
    assert!(!converted.exists());
}
