use super::*;

#[test]
fn pending_operation_keeps_live_exclusion_and_releases_before_inherited_descriptor() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("operation");
    let owner = Directory::create(&path).unwrap();
    owner.install(b"original pending request", true).unwrap();
    let inherited = owner._lock.file().try_clone().unwrap();
    for _ in 0..2 {
        assert!(matches!(Directory::open(&path), Err(PendingError::Locked)));
    }
    drop(owner);
    inherited.metadata().unwrap();
    let next = Directory::open(&path).unwrap();
    assert_eq!(next.read().unwrap(), b"original pending request");
    drop(inherited);
    assert!(matches!(Directory::open(&path), Err(PendingError::Locked)));
    drop(next);
    assert_eq!(
        Directory::open(&path).unwrap().read().unwrap(),
        b"original pending request"
    );
}
