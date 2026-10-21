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
        let start = std::sync::Barrier::new(16);
        std::thread::scope(|scope| {
            let makers: Vec<_> = (0..16)
                .map(|node| {
                    let (path, start) = (dir.join(format!("{node:016x}.key")), &start);
                    scope.spawn(move || {
                        start.wait();
                        create_root_key(&path)
                    })
                })
                .collect();
            for maker in makers {
                maker.join().unwrap().unwrap();
            }
        });
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

/// The names in `dir`, sorted.
fn names(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    names
}

/// Processes making a fresh data directory's keys at once all end with the same keys: one root
/// key, the first `SEAL.node` standing, never one over it, and every other maker opening it. Do:
/// sixteen threads, released together, resolve one directory with no keys, over twenty rounds:
/// odd rounds under one configured key file none has made, even ones under the directory's own
/// (each maker with a random root ID of its own, as a release build makes them). Expect: every
/// one succeeds, all name one key file, a key made by any opens what another sealed, and nothing
/// else is left: no temporary, no key file but the one. Before, a maker read the configured key
/// file another was still writing ("a key file of other than 32 bytes"), and a `SEAL.node`
/// installed by an atomic replace could stand over the one a maker had sealed under.
#[test]
fn keys_made_at_once_for_one_directory_are_one_directorys_keys() {
    for round in 0..20 {
        let home = tempfile::tempdir().unwrap();
        let data = home.path().join("data");
        std::fs::create_dir(&data).unwrap();
        let user = home.path().join("user");
        let configured = home.path().join("configured").join("node.key");
        let configured = (round % 2 == 1).then_some(configured.as_path());
        let start = std::sync::Barrier::new(16);
        let keys: Vec<DataKeys> = std::thread::scope(|scope| {
            let makers: Vec<_> = (0..16)
                .map(|_| {
                    let (data, user, start) = (&data, &user, &start);
                    scope.spawn(move || {
                        start.wait();
                        resolve_in(Some(user), data, configured, None, random_root)
                    })
                })
                .collect();
            makers
                .into_iter()
                .map(|maker| maker.join().unwrap().unwrap())
                .collect()
        });
        assert!(
            keys.iter().all(|k| k.key_file == keys[0].key_file),
            "round {round}"
        );
        let probe = Secret32::from_bytes(&[5; 32]).unwrap();
        let sealed = keys[3].key(Store::Journal).unwrap().wrap(&probe).unwrap();
        let opened = keys[11]
            .key(Store::Journal)
            .unwrap()
            .unwrap(&sealed)
            .unwrap();
        assert_eq!(*opened.bytes(), [5; 32]);
        if let Some(configured) = configured {
            assert_eq!(names(&data), [SEAL_FILE], "round {round}");
            assert_eq!(
                names(configured.parent().unwrap()),
                ["node.key"],
                "round {round}"
            );
        } else {
            assert_eq!(names(&data), [SEAL_FILE, INTENT_FILE], "round {round}");
            assert_eq!(names(&user).len(), 1, "one key file, round {round}");
        }
    }
}

/// What a maker cut short between its write and its link leaves of `name` in `dir`: a temporary of
/// it; and a file of another form beside it, which is no temporary of it.
fn cut_short(dir: &Path, name: &str) {
    std::fs::create_dir_all(dir).unwrap();
    std::fs::write(dir.join(format!("{name}.0123456789abcdef.part")), b"cut").unwrap();
    std::fs::write(dir.join(format!("{name}.kept.part")), b"other").unwrap();
}

/// A start that failed or crashed before the directory's `SEAL.node` leaves the key file its next
/// start makes the keys under, never a key an attempt made that nothing names, and what a maker
/// cut short left mid-publication goes at its name's publication. Do: leave a temporary of the
/// intent, as a start cut short publishing it leaves it, and record the intent; then leave a
/// temporary of the key file and of `SEAL.node`, as a start cut short making them leaves them;
/// resolve the directory. Expect: the intent's temporary goes with its publication; the keys are
/// made under the intent's key file, the one key file there; every temporary gone, every file of
/// another form kept. A damaged intent is refused, never answered by a new root.
#[test]
fn a_start_cut_short_leaves_the_key_its_next_start_makes_the_keys_under() {
    let home = tempfile::tempdir().unwrap();
    let data = home.path().join("data");
    let user = home.path().join("user");
    hyper_seal::lock_keys(key_slots()).unwrap();
    cut_short(&data, INTENT_FILE);
    let id = intended_root(&data, random_root).unwrap();
    assert_eq!(
        names(&data),
        [INTENT_FILE.to_owned(), format!("{INTENT_FILE}.kept.part")]
    );
    let key_file = key_file_in(&user, id);
    let key_name = key_file.file_name().unwrap().to_str().unwrap().to_owned();
    cut_short(&user, &key_name);
    cut_short(&data, SEAL_FILE);
    // A later attempt's intent: the same root, whatever ID it would make.
    assert_eq!(intended_root(&data, random_root).unwrap(), id);
    let keys = resolve_in(Some(&user), &data, None, None, random_root).unwrap();
    assert_eq!(keys.key_file, key_file);
    keys.key(Store::Journal).unwrap();
    assert_eq!(
        names(&user),
        [key_name.clone(), format!("{key_name}.kept.part")]
    );
    assert_eq!(
        names(&data),
        [
            SEAL_FILE.to_owned(),
            format!("{SEAL_FILE}.kept.part"),
            INTENT_FILE.to_owned(),
            format!("{INTENT_FILE}.kept.part"),
        ]
    );
    let other = home.path().join("other");
    std::fs::create_dir(&other).unwrap();
    intended_root(&other, random_root).unwrap();
    let intent = other.join(INTENT_FILE);
    let mut bytes = std::fs::read(&intent).unwrap();
    bytes[9] ^= 1;
    std::fs::write(&intent, &bytes).unwrap();
    assert!(matches!(
        resolve_in(Some(&user), &other, None, None, random_root),
        Err(SealSetupError::IntentDamaged(_))
    ));
}

/// Under a build with test support every data directory made without a key file shares the one
/// test root key file, and each still has keys of its own. Do: resolve two directories; seal under
/// one's journal key. Expect: one key file, named for [`TEST_ROOT`]; the other's journal key opens
/// nothing the first sealed.
#[test]
fn test_directories_share_one_root_key_file_and_keep_keys_of_their_own() {
    let home = tempfile::tempdir().unwrap();
    let (a, b) = (home.path().join("a"), home.path().join("b"));
    std::fs::create_dir(&a).unwrap();
    std::fs::create_dir(&b).unwrap();
    let (a, b) = (test_keys(&a).unwrap(), test_keys(&b).unwrap());
    assert_eq!(a.key_file, b.key_file);
    assert_eq!(Some(a.key_file.clone()), key_file_for(TEST_ROOT));
    let probe = Secret32::from_bytes(&[7; 32]).unwrap();
    let sealed = a.key(Store::Journal).unwrap().wrap(&probe).unwrap();
    assert!(b.key(Store::Journal).unwrap().unwrap(&sealed).is_err());
}
