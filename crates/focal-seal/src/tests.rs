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

/// Nodes of one user starting at once make their root keys in one directory not yet there: each
/// start makes it or finds it made. Do: sixteen threads, released together, each make a root key of
/// its own under one missing directory, over twenty rounds. Expect: every key made; before, a
/// thread that found the directory missing and made it second failed "File exists".
#[test]
fn root_keys_made_at_once_share_the_directory_they_make() {
    for _ in 0..20 {
        let home = tempfile::tempdir().unwrap();
        let dir = home.path().join("config").join("focal").join("keys");
        let start = std::sync::Arc::new(std::sync::Barrier::new(16));
        let makers: Vec<_> = (0..16)
            .map(|node| {
                let path = dir.join(format!("{node:016x}.key"));
                let start = start.clone();
                std::thread::spawn(move || {
                    start.wait();
                    create_root_key(&path)
                })
            })
            .collect();
        for maker in makers {
            maker.join().unwrap().unwrap();
        }
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 16);
    }
}

/// A cache's writer and its readers each make the cache's key and open what the others sealed;
/// another cache's key, in this process or after it, opens nothing of it.
#[test]
fn a_caches_keys_open_what_it_sealed_and_no_other_caches_do() {
    let cache = Cache::new().unwrap();
    let (writer, reader) = (cache.key().unwrap(), cache.key().unwrap());
    let plain = *b"a payload, spilled to the volume";
    let mut sealed = plain;
    let tag = writer.seal(3, 64, false, &mut sealed).unwrap();
    assert_ne!(sealed, plain, "nothing of it is on the volume in the clear");
    let mut opened = sealed;
    reader.open(3, 64, false, &mut opened, &tag).unwrap();
    assert_eq!(opened, plain);
    // At another position, or under another cache's key, it does not open.
    let mut moved = sealed;
    assert!(reader.open(3, 65, false, &mut moved, &tag).is_err());
    let other = Cache::new().unwrap().key().unwrap();
    let mut foreign = sealed;
    assert!(other.open(3, 64, false, &mut foreign, &tag).is_err());
}

/// A data directory's key file is found from the directory: the configured one where its owner
/// names it; else, where `SEAL.node` names a root key, the file named for it or, for keys an
/// earlier binary made under its node's name, that one; and never a fresh key for a directory that
/// has keys.
#[test]
fn a_data_directorys_key_file_is_found_from_the_directory() {
    let d = dirs();
    assert_eq!(recorded_root(&d.data).unwrap(), None, "no keys yet");
    // The owner names one: the keys are made under it, and its ID is recorded.
    let keys = resolve(&d.data, Some(&d.key), None).unwrap();
    assert_eq!(keys.key_file, d.key);
    let id = recorded_root(&d.data).unwrap().unwrap();
    let named = key_file_for(id).unwrap();
    assert!(named.ends_with(format!("{}.key", hex(&id.0))));
    // Unconfigured, the keys an earlier binary made under its node's file are opened by that
    // file while none is named for the root.
    if !named.exists() {
        let legacy = resolve(&d.data, None, Some(d.key.clone())).unwrap();
        assert_eq!(legacy.key_file, d.key);
        legacy.key(Store::Journal).unwrap();
        // Without that file the directory's keys are missing, never made again.
        let elsewhere = d.data.parent().unwrap().join("nowhere.key");
        assert!(matches!(
            resolve(&d.data, None, Some(elsewhere)),
            Err(SealSetupError::MissingKey(_))
        ));
    }
    // A damaged record names no root.
    let seal = d.data.join(SEAL_FILE);
    let mut bytes = std::fs::read(&seal).unwrap();
    bytes[20] ^= 1;
    std::fs::write(&seal, &bytes).unwrap();
    assert!(matches!(
        recorded_root(&d.data),
        Err(SealSetupError::Damaged(_))
    ));
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
