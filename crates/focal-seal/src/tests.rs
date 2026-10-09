//! A node's keys are made once and open again as the same keys; every way the key file and the
//! data directory can disagree is refused, typed, and none is answered by making a new key.
use super::*;

struct Dirs {
    _keep: tempfile::TempDir,
    data: PathBuf,
    key: PathBuf,
}

fn dirs() -> Dirs {
    hyper_seal::lock_keys(key_slots()).unwrap();
    let keep = tempfile::tempdir().unwrap();
    let data = keep.path().join("data");
    std::fs::create_dir(&data).unwrap();
    let key = keep.path().join("keys").join("node.key");
    Dirs {
        data,
        key,
        _keep: keep,
    }
}

/// What a key seals another opens: the two keys are the same key.
fn same(a: &WrappingKey, b: &WrappingKey) -> bool {
    let probe = Secret32::from_bytes(&[9; 32]).unwrap();
    let wrapped = a.wrap(&probe).unwrap();
    a.id() == b.id()
        && b.unwrap(&wrapped)
            .map(|k| *k.bytes() == [9; 32])
            .unwrap_or(false)
}

#[test]
fn keys_are_made_once_and_open_as_the_same_keys() {
    let d = dirs();
    let made = open_or_create(&d.data, &d.key).unwrap();
    assert!(d.key.exists() && d.data.join(SEAL_FILE).exists());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = std::fs::metadata(&d.key).unwrap().permissions().mode();
        assert_eq!(mode & 0o077, 0, "the key file is its owner's alone");
    }
    let again = open_or_create(&d.data, &d.key).unwrap();
    assert_eq!(made.root, again.root);
    for (a, b) in [
        (&made.log_parent, &again.log_parent),
        (&made.group, &again.group),
        (&made.content, &again.content),
        (&made.journal, &again.journal),
    ] {
        assert!(same(a, b));
    }
    assert_eq!(made.log_auth.bytes(), again.log_auth.bytes());
    // Each store's key is its own.
    assert!(!same(&made.group, &made.content));
}

#[test]
fn a_key_file_inside_the_data_directory_is_refused() {
    let d = dirs();
    let inside = d.data.join("keys").join("node.key");
    assert!(matches!(
        open_or_create(&d.data, &inside),
        Err(SealSetupError::KeyInDataDir(_))
    ));
    assert!(!inside.exists(), "nothing is made before the refusal");
    assert!(!d.data.join(SEAL_FILE).exists());
}

#[test]
fn a_missing_key_file_is_refused_never_replaced() {
    let d = dirs();
    open_or_create(&d.data, &d.key).unwrap();
    std::fs::remove_file(&d.key).unwrap();
    assert!(matches!(
        open_or_create(&d.data, &d.key),
        Err(SealSetupError::MissingKey(_))
    ));
    assert!(!d.key.exists(), "a missing key is never made again");
}

#[test]
fn another_nodes_key_file_is_the_wrong_key() {
    let d = dirs();
    open_or_create(&d.data, &d.key).unwrap();
    let other = dirs();
    open_or_create(&other.data, &other.key).unwrap();
    assert!(matches!(
        open_or_create(&d.data, &other.key),
        Err(SealSetupError::WrongKey(_))
    ));
}

#[test]
fn a_damaged_seal_file_is_damage_not_a_wrong_key() {
    let d = dirs();
    open_or_create(&d.data, &d.key).unwrap();
    let path = d.data.join(SEAL_FILE);
    let good = std::fs::read(&path).unwrap();
    for at in [0, 9, 40, 100, good.len() - 1] {
        let mut bad = good.clone();
        bad[at] ^= 1;
        std::fs::write(&path, &bad).unwrap();
        assert!(
            matches!(
                open_or_create(&d.data, &d.key),
                Err(SealSetupError::Damaged(_))
            ),
            "a flip at {at}"
        );
    }
    std::fs::write(&path, &good[..good.len() - 1]).unwrap();
    assert!(matches!(
        open_or_create(&d.data, &d.key),
        Err(SealSetupError::Io(_) | SealSetupError::Damaged(_))
    ));
}

#[cfg(unix)]
#[test]
fn a_key_file_others_may_read_or_of_the_wrong_length_is_refused() {
    use std::os::unix::fs::PermissionsExt as _;
    let d = dirs();
    open_or_create(&d.data, &d.key).unwrap();
    std::fs::set_permissions(&d.key, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert!(matches!(
        open_or_create(&d.data, &d.key),
        Err(SealSetupError::Seal(SealError::Source(_)))
    ));
    std::fs::set_permissions(&d.key, std::fs::Permissions::from_mode(0o600)).unwrap();
    let mut longer = std::fs::read(&d.key).unwrap();
    longer.push(0);
    std::fs::write(&d.key, &longer).unwrap();
    assert!(matches!(
        open_or_create(&d.data, &d.key),
        Err(SealSetupError::Seal(SealError::Source(_)))
    ));
}

#[test]
fn an_operators_existing_key_seals_a_new_data_directory() {
    let d = dirs();
    open_or_create(&d.data, &d.key).unwrap();
    // A second data directory under the same key file (a mounted secret): its own keys.
    let second = d.data.parent().unwrap().join("data-2");
    std::fs::create_dir(&second).unwrap();
    let a = open_or_create(&d.data, &d.key).unwrap();
    let b = open_or_create(&second, &d.key).unwrap();
    assert!(!same(&a.group, &b.group));
}

#[test]
fn the_default_key_file_is_never_the_data_directory() {
    let key = default_key_file("node-1").unwrap();
    assert!(key.ends_with("node-1.key"));
}

#[test]
fn one_store_key_opens_as_the_node_made_it() {
    let d = dirs();
    let made = open_or_create(&d.data, &d.key).unwrap();
    for (store, key) in [
        (Store::Group, &made.group),
        (Store::Content, &made.content),
        (Store::Journal, &made.journal),
    ] {
        assert!(
            same(key, &store_key(&d.data, &d.key, store).unwrap()),
            "{store:?}"
        );
    }
    // A data directory the node never started on has no keys to give.
    let empty = d.data.parent().unwrap().join("empty");
    std::fs::create_dir(&empty).unwrap();
    assert!(matches!(
        store_key(&empty, &d.key, Store::Group),
        Err(SealSetupError::NoKeys(_))
    ));
    assert!(!empty.join(SEAL_FILE).exists());
}
