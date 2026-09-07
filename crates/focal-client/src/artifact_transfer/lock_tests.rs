use super::*;
use std::os::unix::fs::PermissionsExt;

#[test]
fn upload_and_catalogue_lock_end_with_owner_while_duplicate_is_still_open() {
    for layout in [Layout::Upload, Layout::Catalogue] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("transfer");
        let owner = Directory::create_layout(&path, layout).unwrap();
        owner.install(b"original transfer state", true).unwrap();
        let inherited = owner._lock.file().try_clone().unwrap();
        for _ in 0..2 {
            assert!(matches!(
                Directory::lock(&path, false, layout),
                Err(TransferError::Locked)
            ));
        }
        drop(owner);
        inherited.metadata().unwrap();
        let next = Directory::lock(&path, false, layout).unwrap();
        assert_eq!(next.read().unwrap(), b"original transfer state");
        drop(inherited);
        assert!(matches!(
            Directory::lock(&path, false, layout),
            Err(TransferError::Locked)
        ));
        drop(next);
        assert_eq!(
            Directory::lock(&path, false, layout)
                .unwrap()
                .read()
                .unwrap(),
            b"original transfer state"
        );
    }
}

#[test]
fn bootstrap_marker_lock_releases_without_losing_identity_or_live_exclusion() {
    use focal_model::{LedgerId, ParticipantId, SessionId, TenantId};
    let root = tempfile::tempdir().unwrap();
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let context = crate::pending::OperationContext {
        cluster: [1; 16],
        principal: ParticipantId::from_u128(2),
        ledger: LedgerId {
            tenant: TenantId::from_u128(3),
            session: SessionId::from_u128(4),
        },
    };
    let (owner, first) = bootstrap(root.path(), "uploads", context).unwrap();
    assert!(first);
    let original = fs::read(root.path().join("uploads.initialized")).unwrap();
    let inherited = owner.file().try_clone().unwrap();
    for _ in 0..2 {
        assert!(matches!(
            bootstrap(root.path(), "uploads", context),
            Err(TransferError::Locked)
        ));
    }
    drop(owner);
    inherited.metadata().unwrap();
    let (next, first) = bootstrap(root.path(), "uploads", context).unwrap();
    assert!(!first);
    assert_eq!(
        fs::read(root.path().join("uploads.initialized")).unwrap(),
        original
    );
    drop(inherited);
    assert!(matches!(
        bootstrap(root.path(), "uploads", context),
        Err(TransferError::Locked)
    ));
    drop(next);
    let other = crate::pending::OperationContext {
        principal: ParticipantId::from_u128(99),
        ..context
    };
    assert!(matches!(
        bootstrap(root.path(), "uploads", other),
        Err(TransferError::Conflict)
    ));
    let (_, first) = bootstrap(root.path(), "uploads", context).unwrap();
    assert!(!first);
}
