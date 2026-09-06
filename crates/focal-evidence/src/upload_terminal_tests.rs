use super::*;
use crate::{ContentStore, MAX_TERMINAL_UPLOADS, StoreLimits};
use focal_model::{ContentClass, ContentDomainId};

fn store(path: &Path) -> ContentStore {
    ContentStore::open(
        path,
        StoreLimits {
            max_content_bytes: 64,
            max_staging_bytes: 128,
            max_uploads: 2,
            chunk_bytes: 4,
            max_manifest_bytes: 2048,
        },
    )
    .unwrap()
}
fn begin(store: &mut ContentStore, id: UploadId) -> Result<u64, ContentError> {
    store.begin(
        id,
        ContentDomainId([2; 16]),
        ContentClass::Evidence,
        3,
        None,
    )
}

#[test]
fn terminal_ids_fence_delayed_begin_across_restart_and_preserve_immutable_content() {
    let root = tempfile::tempdir().unwrap();
    let mut current = store(root.path());
    let absent = UploadId([1; 16]);
    current.finish(absent).unwrap();
    assert!(matches!(
        begin(&mut current, absent),
        Err(ContentError::FinishedUpload)
    ));
    let id = UploadId([3; 16]);
    begin(&mut current, id).unwrap();
    current.append(id, 0, b"abc").unwrap();
    let reference = current.seal(id).unwrap();
    current.finish(id).unwrap();
    assert_eq!(current.staged(), (0, 0));
    drop(current);
    let mut current = store(root.path());
    for id in [absent, id] {
        assert!(matches!(
            begin(&mut current, id),
            Err(ContentError::FinishedUpload)
        ));
        current.finish(id).unwrap();
        assert!(!current.part_path(id).exists());
        let bytes = fs::read(current.meta_path(id)).unwrap();
        assert!(postcard::from_bytes::<super::super::UploadMeta>(&bytes).is_err());
    }
    assert_eq!(current.read_bytes(&reference, 3).unwrap(), b"abc");
}

#[test]
fn crash_cuts_recover_old_active_or_new_terminal_without_recreating_an_acknowledged_id() {
    for cut in [Cut::FileSynced, Cut::Renamed] {
        let root = tempfile::tempdir().unwrap();
        let id = UploadId([4; 16]);
        let mut current = store(root.path());
        begin(&mut current, id).unwrap();
        current.append(id, 0, b"abc").unwrap();
        CUT.with(|pending| pending.set(Some(cut)));
        assert!(matches!(current.finish(id), Err(ContentError::Io(_))));
        assert!(matches!(begin(&mut current, id), Err(ContentError::Failed)));
        drop(current);
        let mut current = store(root.path());
        assert!(!current.meta_path(id).with_extension("terminal").exists());
        match cut {
            Cut::FileSynced => assert_eq!(begin(&mut current, id).unwrap(), 3),
            Cut::Renamed => assert!(matches!(
                begin(&mut current, id),
                Err(ContentError::FinishedUpload)
            )),
        }
        current.finish(id).unwrap();
        drop(current);
        assert!(matches!(
            begin(&mut store(root.path()), id),
            Err(ContentError::FinishedUpload)
        ));
    }
}

#[test]
fn malformed_terminal_state_and_trailing_active_bytes_fail_closed() {
    for mode in 0..4 {
        let root = tempfile::tempdir().unwrap();
        let id = UploadId([5; 16]);
        let mut current = store(root.path());
        if mode == 3 {
            begin(&mut current, id).unwrap();
        } else {
            current.finish(id).unwrap();
        }
        let path = current.meta_path(id);
        drop(current);
        let mut bytes = fs::read(&path).unwrap();
        match mode {
            0 => bytes[30] ^= 1,
            1 => {
                bytes.pop();
            }
            2 => {
                fs::rename(
                    &path,
                    path.with_file_name("00000000000000000000000000000009.meta"),
                )
                .unwrap();
            }
            _ => bytes.push(0),
        }
        if mode != 2 {
            fs::write(&path, bytes).unwrap();
        }
        let limits = StoreLimits {
            max_content_bytes: 64,
            max_staging_bytes: 128,
            max_uploads: 2,
            chunk_bytes: 4,
            max_manifest_bytes: 2048,
        };
        assert!(ContentStore::open(root.path(), limits).is_err());
    }
}

#[test]
fn live_upload_reserves_terminal_capacity_against_unknown_cancellations() {
    let root = tempfile::tempdir().unwrap();
    let mut current = store(root.path());
    let live = UploadId([6; 16]);
    begin(&mut current, live).unwrap();
    // Model a full metadata namespace without creating 65,535 fsynced records;
    // the real reservation and publication path below still writes the last slot.
    current.terminal_uploads = MAX_TERMINAL_UPLOADS - 1;
    let unknown = UploadId([7; 16]);
    assert!(matches!(
        current.finish(unknown),
        Err(ContentError::Capacity)
    ));
    assert!(matches!(
        begin(&mut current, unknown),
        Err(ContentError::Capacity)
    ));
    assert!(!current.meta_path(unknown).exists());
    current.finish(live).unwrap();
    assert_eq!(current.terminal_uploads, MAX_TERMINAL_UPLOADS);
    current.finish(live).unwrap();
    assert!(matches!(
        begin(&mut current, live),
        Err(ContentError::FinishedUpload)
    ));
}
