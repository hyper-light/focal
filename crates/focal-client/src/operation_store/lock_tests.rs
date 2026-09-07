use super::*;
use std::os::unix::fs::PermissionsExt;

#[test]
fn named_owner_lock_ends_at_scope_even_with_inherited_descriptor() {
    for layout in [Layout::Coordinator, Layout::Watch] {
        let root = tempfile::tempdir().unwrap();
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let (owner, initialized) =
            Directory::named_owner(root.path(), "client", true, layout).unwrap();
        assert!(!initialized);
        owner.finish_coordinator().unwrap();
        let inherited = owner._lock.file().try_clone().unwrap();
        for _ in 0..2 {
            assert!(matches!(
                Directory::named_owner(root.path(), "client", false, layout),
                Err(StoreError::Locked)
            ));
        }
        drop(owner);
        inherited.metadata().unwrap();
        let (next, initialized) =
            Directory::named_owner(root.path(), "client", false, layout).unwrap();
        assert!(initialized);
        drop(inherited);
        assert!(matches!(
            Directory::named_owner(root.path(), "client", false, layout),
            Err(StoreError::Locked)
        ));
        drop(next);
        Directory::named_owner(root.path(), "client", false, layout).unwrap();
    }
}

#[test]
fn managed_and_legacy_directory_unlock_without_waiting_for_inherited_close() {
    for layout in [Layout::Managed, Layout::Legacy] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("operations");
        let owner = Directory::create_layout(&path, layout).unwrap();
        owner.initialize().unwrap();
        let inherited = owner._lock.file().try_clone().unwrap();
        for _ in 0..2 {
            assert!(matches!(
                Directory::open_layout(&path, layout),
                Err(StoreError::Locked)
            ));
        }
        drop(owner);
        inherited.metadata().unwrap();
        let next = Directory::open_layout(&path, layout).unwrap();
        drop(inherited);
        assert!(matches!(
            Directory::open_layout(&path, layout),
            Err(StoreError::Locked)
        ));
        drop(next);
        Directory::open_layout(&path, layout).unwrap();
    }
}
